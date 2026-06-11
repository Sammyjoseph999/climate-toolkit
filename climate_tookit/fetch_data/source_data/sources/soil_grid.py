"""Downloads soil data from the ISRIC SoilGrids250m v2.0 dataset on Google Earth Engine.

SoilGrids250m v2.0 publishes each soil property as its own GEE Image (``projects/soilgrids-isric/<prop>_mean``) whose bands are per depth horizon
(e.g. ``clay_0-5cm_mean``). This module fetches the standard root-zone horizons for the requested :class:`SoilVariable` list and returns them already converted
to the conventional units the rest of the toolkit expects:
    sand / silt / clay : percent (0-100)
    organic_carbon     : g/kg
    bulk_density       : kg/m3
    cation_exchange_capacity : cmol(+)/kg
    ph                 : pH(H2O) x 10   (e.g. 65 == pH 6.5)

Two SoilGrids quirks handled here:
* the raw assets store scaled integers (g/kg, dg/kg, cg/cm3, ...), so each property has an explicit conversion factor in ``_SOIL_SPEC``;
* SoilGrids is gridded in an Interrupted Goode Homolosine projection, and an exact-point ``reduceRegion`` of the reprojected raster frequently misses the
  single cell and returns ``None`` -- so we average the handful of 250 m cells inside a small buffer around the point instead.

Pre-requisites: GEE authentication (``earthengine authenticate``) and an authorised GCP project, exactly as for the other GEE-backed sources.
"""

import logging
from datetime import date

import pandas as pd
from dotenv import load_dotenv

from .utils import models
from .utils.settings import Settings, set_logging
from .gee import _ensure_gee_initialized

load_dotenv()
set_logging()
logger = logging.getLogger(__name__)

# ISRIC SoilGrids 250m v2.0 catalogue root on Earth Engine.
_ISRIC_BASE = "projects/soilgrids-isric"

# Root-zone depth horizons (cm) returned, one DataFrame row each.
_ISRIC_DEPTHS = [(0, 5), (5, 15), (15, 30), (30, 60)]

# Buffer (m) around the point before reduceRegion (see module docstring).
_SOIL_SAMPLE_BUFFER_M = 500.0

# SoilVariable.name -> (SoilGrids band-name prefix, raw->standard-unit factor)
_SOIL_SPEC = {
    "sand_content":             ("sand",  0.1),
    "silt_content":             ("silt",  0.1),
    "clay_content":             ("clay",  0.1),
    "organic_carbon":           ("soc",   0.1),
    "bulk_density":             ("bdod",  10.0),
    "cation_exchange_capacity": ("cec",   0.1),
    "ph":                       ("phh2o", 1.0),
}

_DEFAULT_RESOLUTION_M = 250

class DownloadData(models.DataDownloadBase):
    def __init__(
        self,
        variables: list[models.SoilVariable],
        location_coord: tuple[float],
        date_from_utc: date,
        date_to_utc: date,
        settings: Settings,
        source: models.ClimateDataset,
    ):
        super().__init__(
            location_coord=location_coord,
            date_from_utc=date_from_utc,
            date_to_utc=date_to_utc,
            variables=variables,
        )
        self.date_from_utc = date_from_utc
        self.date_to_utc = date_to_utc
        self.location_coord = location_coord
        self.variables = variables
        self.settings = settings
        self.source = source

    def _resolution(self) -> float:
        """Native sampling scale (m); read from settings.soil_grid if present."""
        soil_settings = getattr(self.settings, "soil_grid", None)
        return float(getattr(soil_settings, "resolution", _DEFAULT_RESOLUTION_M))

    def download_variables(self) -> pd.DataFrame:
        """Fetch the requested soil properties for every root-zone depth horizon.
        Returns a DataFrame with one row per depth (``top_cm``/``bottom_cm``) and one column per requested variable, in standard units. Properties not
        backed by SoilGrids (e.g. soil_moisture) are skipped with a warning.
        """
        lat, lon = self.location_coord

        specs = {}
        for variable in self.variables:
            name = variable.name if hasattr(variable, "name") else str(variable)
            if name in _SOIL_SPEC:
                specs[name] = _SOIL_SPEC[name]
            else:
                logger.warning(f"soil_grid: no SoilGrids mapping for '{name}' (skipped)")
        if not specs:
            logger.warning("soil_grid: no supported soil variables requested")
            return pd.DataFrame()

        _ensure_gee_initialized()
        import ee

        scale = self._resolution()
        region = ee.Geometry.Point([lon, lat]).buffer(_SOIL_SAMPLE_BUFFER_M)

        logger.info(
            f"soil_grid: fetching {sorted(specs)} from ISRIC SoilGrids "
            f"at ({lat}, {lon}), {len(_ISRIC_DEPTHS)} depth horizons"
        )

        images = []
        for name, (prefix, _factor) in specs.items():
            bands = [f"{prefix}_{t}-{b}cm_mean" for t, b in _ISRIC_DEPTHS]
            images.append(ee.Image(f"{_ISRIC_BASE}/{prefix}_mean").select(bands))
        composite = ee.Image.cat(images)

        try:
            vals = composite.reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=region,
                scale=scale,
                bestEffort=True,
                maxPixels=int(1e9),
            ).getInfo() or {}
        except Exception as e:
            logger.error(f"soil_grid: GEE reduceRegion failed: {e}")
            return pd.DataFrame()

        rows = []
        for t, b in _ISRIC_DEPTHS:
            row = {"top_cm": float(t), "bottom_cm": float(b)}
            for name, (prefix, factor) in specs.items():
                raw = vals.get(f"{prefix}_{t}-{b}cm_mean")
                row[name] = float(raw) * factor if raw is not None else None
            rows.append(row)

        df = pd.DataFrame(rows)
        logger.info(
            f"soil_grid: retrieved {len(df)} depth horizons, columns={list(df.columns)}"
        )
        return df

    def download_precipitation(self):
        raise NotImplementedError("soil_grid provides static soil properties only")

    def download_rainfall(self):
        raise NotImplementedError("soil_grid provides static soil properties only")

    def download_temperature(self):
        raise NotImplementedError("soil_grid provides static soil properties only")

    def download_windspeed(self):
        raise NotImplementedError("soil_grid provides static soil properties only")

    def download_solar_radiation(self):
        raise NotImplementedError("soil_grid provides static soil properties only")

    def download_humidity(self):
        raise NotImplementedError("soil_grid provides static soil properties only")

    def download_soil_moisture(self):
        raise NotImplementedError("soil_grid provides static soil properties only")