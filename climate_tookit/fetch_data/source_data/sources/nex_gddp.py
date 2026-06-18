"""
NEX-GDDP-CMIP6 Data Source

Pulls daily downscaled CMIP6 projections from the NASA NEX-GDDP-CMIP6
collection hosted on Google Earth Engine (collection ID `NASA/GDDP-CMIP6`).

Each call fetches a single (model, scenario) combination at the requested
point, sampled at the dataset's native ~0.25° resolution. Long date ranges
are chunked client-side so a 30-year window stays under GEE's
5000-elements-per-collection ceiling; chunks are concatenated before
returning.

Dataset reference:
https://developers.google.com/earth-engine/datasets/catalog/NASA_GDDP-CMIP6

Pre-requisites:
1. Run `earthengine authenticate` once on the machine
2. Set `GCP_PROJECT_ID` in your environment (or in a project-local .env)
"""

import logging
from datetime import date, timedelta
from typing import List, Optional, Tuple

import ee
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

from sources.utils import models
from sources.utils.settings import Settings, set_logging
# Reuse the auth singleton from the shared GEE module so `ee.Authenticate()` runs at most once per process.
from sources.gee import _ensure_gee_initialized

set_logging()
logger = logging.getLogger(__name__)

AVAILABLE_MODELS = [
    'ACCESS-CM2', 'ACCESS-ESM1-5', 'CanESM5', 'CMCC-ESM2',
    'EC-Earth3', 'EC-Earth3-Veg-LR', 'GFDL-ESM4', 'INM-CM4-8',
    'INM-CM5-0', 'KACE-1-0-G', 'MIROC6', 'MPI-ESM1-2-LR',
    'MRI-ESM2-0', 'NorESM2-LM', 'NorESM2-MM', 'TaiESM1'
]

SCENARIO_MAPPING = {
    'SSP1-2.6': 'ssp126', 'SSP2-4.5': 'ssp245', 'SSP5-8.5': 'ssp585',
    'ssp126': 'ssp126', 'ssp245': 'ssp245', 'ssp585': 'ssp585',
    'historical': 'historical'
}

NEX_GDDP_COLLECTION = "NASA/GDDP-CMIP6"

NEX_GDDP_SCALE_M = 27830

NEX_GDDP_CHUNK_DAYS = 4745

# In NEX-GDDP-CMIP6, the 'historical' run covers ~1950-2014 and every SSP
# scenario (ssp126/245/585) covers 2015 onward. A request for an SSP scenario
# spanning the boundary must pull the pre-2015 portion from 'historical', or
# those years come back empty.
NEX_GDDP_HISTORICAL_END = date(2014, 12, 31)
NEX_GDDP_SSP_START      = date(2015, 1, 1)

def _variables_to_bands(variables) -> List[str]:
    """Map requested ClimateVariable enums (or plain strings) to NEX-GDDP band names."""
    bands: List[str] = []
    for v in variables:
        name = str(v).split('.')[-1] if hasattr(v, 'name') else str(v)
        n = name.lower()
        if 'precipitation' in n:
            bands.append('pr')
        elif 'max' in n and 'temp' in n:
            bands.append('tasmax')
        elif 'min' in n and 'temp' in n:
            bands.append('tasmin')
    # De-dupe while preserving order
    seen = set()
    return [b for b in bands if not (b in seen or seen.add(b))]

class DownloadData(models.DataDownloadBase):
    def __init__(self, variables: List[models.ClimateVariable],
                 location_coord: Tuple[float, float],
                 date_from_utc: date, date_to_utc: date,
                 settings: Settings, source: models.ClimateDataset,
                 model: Optional[str] = None,
                 scenario: Optional[str] = None):
        super().__init__(location_coord=location_coord,
                         date_from_utc=date_from_utc,
                         date_to_utc=date_to_utc,
                         variables=variables)
        self.date_from_utc = date_from_utc
        self.date_to_utc = date_to_utc
        self.location_coord = location_coord
        self.variables = variables
        self.settings = settings
        self.source = source

        if model is not None and model not in AVAILABLE_MODELS:
            raise ValueError(
                f"Invalid model '{model}'. Must be one of: {', '.join(AVAILABLE_MODELS)}"
            )
        self.model = model or 'ACCESS-CM2'

        if scenario is not None and scenario not in SCENARIO_MAPPING:
            raise ValueError(
                f"Invalid scenario '{scenario}'. Must be one of: {', '.join(SCENARIO_MAPPING.keys())}"
            )
        self.scenario = SCENARIO_MAPPING.get(scenario, 'ssp245')

        logger.info(
            f"NEX-GDDP using model={self.model}, "
            f"scenario={self.scenario}, "
            f"coord={self.location_coord}"
        )

    def download_variables(self) -> pd.DataFrame:
        """
        Fetch a daily NEX-GDDP DataFrame from GEE for the requested
        (model, scenario, lat, lon, date range, variables).

        Returns a DataFrame with column `date` plus one column per requested
        band (`pr`, `tasmax`, `tasmin`). Units are normalised to the toolkit's
        downstream convention:
          - pr     : mm/day  (raw GEE values are kg/(m²·s); multiplied by 86400)
          - tasmax : °C      (raw GEE values are Kelvin; − 273.15)
          - tasmin : °C      (raw GEE values are Kelvin; − 273.15)

        Long date ranges are split into ~13-year chunks to stay under GEE's
        5000-elements-per-collection ceiling. Chunk failures are logged and
        skipped so a single transient error doesn't kill the whole request.
        """
        _ensure_gee_initialized()

        bands = _variables_to_bands(self.variables)
        if not bands:
            logger.warning("No NEX-GDDP bands requested.")
            return pd.DataFrame()

        lat, lon = self.location_coord
        point = ee.Geometry.Point([lon, lat])

        chunks: List[pd.DataFrame] = []
        # Split the request across the historical/SSP boundary so pre-2015 years
        # are pulled from the 'historical' run rather than coming back empty.
        for seg_scenario, seg_start, seg_end in self._scenario_segments():
            current = seg_start
            while current <= seg_end:
                chunk_end = min(
                    current + timedelta(days=NEX_GDDP_CHUNK_DAYS - 1),
                    seg_end,
                )
                try:
                    df_chunk = self._fetch_chunk(point, current, chunk_end, bands,
                                                 seg_scenario)
                    if not df_chunk.empty:
                        chunks.append(df_chunk)
                except Exception as exc:
                    logger.error(
                        f"NEX-GDDP {self.model}/{seg_scenario} chunk "
                        f"{current}->{chunk_end} failed: {exc}"
                    )
                current = chunk_end + timedelta(days=1)

        if not chunks:
            return pd.DataFrame()

        df = pd.concat(chunks, ignore_index=True)
        df = df.sort_values("date").reset_index(drop=True)

        # Unit normalisation: GEE returns SI units; the toolkit's downstream
        if 'pr' in df.columns:
            df['pr'] = df['pr'].astype(float) * 86400.0
        for tcol in ('tasmax', 'tasmin'):
            if tcol in df.columns:
                df[tcol] = df[tcol].astype(float) - 273.15
        return df

    def _scenario_segments(self) -> List[Tuple[str, date, date]]:
        """Split [date_from, date_to] into (scenario, start, end) segments that
        honour the NEX-GDDP historical/SSP boundary.

        - scenario 'historical': one segment over the whole range.
        - an SSP scenario: the pre-2015 portion is served from 'historical'
          and the 2015+ portion from the SSP, so a range crossing the boundary
          returns continuous data instead of dropping the historical years.
        """
        if self.scenario == 'historical':
            return [('historical', self.date_from_utc, self.date_to_utc)]
        segments: List[Tuple[str, date, date]] = []
        if self.date_from_utc <= NEX_GDDP_HISTORICAL_END:
            segments.append(('historical', self.date_from_utc,
                             min(self.date_to_utc, NEX_GDDP_HISTORICAL_END)))
        if self.date_to_utc >= NEX_GDDP_SSP_START:
            segments.append((self.scenario,
                             max(self.date_from_utc, NEX_GDDP_SSP_START),
                             self.date_to_utc))
        return segments

    def _fetch_chunk(self, point: "ee.Geometry", from_date: date,
                     to_date: date, bands: List[str],
                     scenario: Optional[str] = None) -> pd.DataFrame:
        """Fetch one date-range chunk for a given scenario; per-day DataFrame."""
        scenario = scenario or self.scenario
        start = ee.Date(from_date.strftime("%Y-%m-%d"))
        end = ee.Date(to_date.strftime("%Y-%m-%d")).advance(1, "day")

        col = (
            ee.ImageCollection(NEX_GDDP_COLLECTION)
            .filter(ee.Filter.eq("model", self.model))
            .filter(ee.Filter.eq("scenario", scenario))
            .filterDate(start, end)
            .filterBounds(point)
            .select(bands)
        )
        def extract(image):
            values = image.reduceRegion(
                reducer=ee.Reducer.first(),
                geometry=point,
                scale=NEX_GDDP_SCALE_M,
                tileScale=2,
                bestEffort=True,
            )
            return ee.Feature(None, values).set(
                "date", image.date().format("YYYY-MM-dd")
            )

        fc = col.map(extract)
        result = fc.getInfo()
        records = [f["properties"] for f in result.get("features", [])]
        return pd.DataFrame(records) if records else pd.DataFrame()

    def download_precipitation(self):
        raise NotImplementedError("Use download_variables()")
    def download_temperature(self):
        raise NotImplementedError("Use download_variables()")
    def download_rainfall(self):
        raise NotImplementedError("Use download_variables()")
    def download_windspeed(self):
        raise NotImplementedError("Use download_variables()")
    def download_solar_radiation(self):
        raise NotImplementedError("Use download_variables()")
    def download_humidity(self):
        raise NotImplementedError("Use download_variables()")
    def download_soil_moisture(self):
        raise NotImplementedError("Use download_variables()")