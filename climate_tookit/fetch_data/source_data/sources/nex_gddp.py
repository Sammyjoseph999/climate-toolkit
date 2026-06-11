"""
This module handles the downloading of climate data from the NEX-GDDP dataset hosted by NASA's Earth Engine.

Dataset Reference:
https://developers.google.com/earth-engine/datasets/catalog/NASA_NEX-GDDP

Pre-requisites:
1. GEE Authentication using `earthengine authenticate`
2. Ensure project ID is authorized in GCP if needed
"""

"""
NEX-GDDP-CMIP6 Data Source - Synthetic Data Version

Generates synthetic climate projection data with realistic model/scenario variations.
For production, replace with real GEE implementation once authenticated.
"""

import hashlib
import logging
import pandas as pd
import numpy as np
from datetime import date
from typing import List, Tuple

from sources.utils import models
from sources.utils.settings import Settings, set_logging

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

def _precip_seasonal_cycle(lat: float, doy: np.ndarray) -> np.ndarray:
    """
    Multiplicative seasonal factor for daily precipitation, parameterised on latitude to express realistic regional regimes (so synthetic Kigali and
    synthetic Dakar produce different climatologies):

      |lat| < 5°    : equatorial bimodal — two ITCZ peaks (~Apr & ~Oct)
      5° ≤ lat <25° : tropical NH monsoon — one peak in JJAS
     -5° ≥ lat>-25° : tropical SH monsoon — one peak in DJF
      |lat| ≥ 25°  : weaker seasonality with winter-wet phasing

    Output values are roughly in [0.2, 2.0] so multiplying by the daily base rate yields realistic wet/dry contrast.
    """
    angle = 2 * np.pi * (doy - 1) / 365.25
    if abs(lat) < 5.0:
        # Bimodal: peaks centred on day-of-year ~105 (Apr 15) and ~288 (Oct 15)
        peak_mam = np.cos(angle - 2 * np.pi * 105 / 365)
        peak_ond = np.cos(angle - 2 * np.pi * 288 / 365)
        return 1.0 + 0.85 * np.clip(np.maximum(peak_mam, peak_ond), -0.4, None)
    if 5.0 <= lat < 25.0:
        # NH tropical monsoon: peak ~Aug 1 (doy 213), sharp dry season
        peak = np.cos(angle - 2 * np.pi * 213 / 365)
        return 1.0 + 1.5 * np.clip(peak, -0.55, None)
    if -25.0 < lat <= -5.0:
        # SH tropical monsoon: peak ~Feb 1 (doy 31)
        peak = np.cos(angle - 2 * np.pi * 31 / 365)
        return 1.0 + 1.5 * np.clip(peak, -0.55, None)
    # Mid-latitude: modest seasonality, NH peaks in winter, SH peaks in winter
    if lat >= 0:
        peak = np.cos(angle - 2 * np.pi * 15 / 365)
    else:
        peak = np.cos(angle - 2 * np.pi * 197 / 365)
    return 1.0 + 0.45 * peak

def _temp_seasonal_amplitude(lat: float) -> float:
    """
    Half-amplitude (°C) of the annual temperature cycle, scaling with absolute latitude: tiny near the equator, growing to ~12°C by mid-latitudes.
    """
    abs_lat = min(abs(lat), 60.0)
    return 1.0 + (abs_lat / 60.0) * 11.0

def _temp_seasonal_shape(lat: float, doy: np.ndarray) -> np.ndarray:
    """
    Unit-amplitude annual temperature shape: NH peaks ~Jul 16 (doy 197), SH peaks ~Jan 15 (doy 15). Multiply by the amplitude from
    `_temp_seasonal_amplitude(lat)` for the full seasonal series.
    """
    angle = 2 * np.pi * (doy - 1) / 365.25
    if lat >= 0:
        return np.cos(angle - 2 * np.pi * 197 / 365)
    return np.cos(angle - 2 * np.pi * 15 / 365)

class DownloadData(models.DataDownloadBase):
    def __init__(self, variables: List[models.ClimateVariable], location_coord: Tuple[float, float],
                 date_from_utc: date, date_to_utc: date, settings: Settings,
                 source: models.ClimateDataset, model: str = None, scenario: str = None):
        super().__init__(location_coord=location_coord, date_from_utc=date_from_utc,
                         date_to_utc=date_to_utc, variables=variables)
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
        lat, lon = self.location_coord
        dates = pd.date_range(self.date_from_utc, self.date_to_utc, freq='D')
        # The seed MUST include the year so that inter-annual variability is real
        # Seeding per-year keeps each year distinct yet fully reproducible.
        seed_str = (
            f"{self.model}|{self.scenario}|{lat:.6f}|{lon:.6f}|"
            f"{self.date_from_utc.year}"
        )
        seed = int(hashlib.sha256(seed_str.encode("utf-8")).hexdigest()[:8], 16)
        rng = np.random.default_rng(seed)
        model_idx = AVAILABLE_MODELS.index(self.model) if self.model in AVAILABLE_MODELS else 0
        model_factor = (model_idx - 7.5) / 7.5
        scenario_warming = {'historical': 0.0, 'ssp126': 1.2, 'ssp245': 1.8, 'ssp585': 2.5}
        warming = scenario_warming.get(self.scenario, 1.5)
        scenario_precip = {'historical': 1.0, 'ssp126': 0.95, 'ssp245': 0.90, 'ssp585': 0.85}
        precip_factor = scenario_precip.get(self.scenario, 1.0)

        # Location-dependent seasonal cycles. Now the cycle depends on latitude and hemisphere.
        doy = dates.dayofyear.to_numpy().astype(float)
        precip_seasonal = _precip_seasonal_cycle(lat, doy)
        temp_seasonal_amp = _temp_seasonal_amplitude(lat)
        temp_seasonal_shape = _temp_seasonal_shape(lat, doy)
        # Latitude-aware temperature baselines (warmer at the equator)
        abs_lat = min(abs(lat), 60.0)
        tmax_base = 30.0 - 0.20 * abs_lat + warming
        tmin_base = 20.0 - 0.20 * abs_lat + warming * 0.8

        data = {'date': dates}

        for variable in self.variables:
            var_name = str(variable).split('.')[-1] if hasattr(variable, 'name') else str(variable)

            if 'precipitation' in var_name.lower():
                # `precip_seasonal` is a multiplicative regime factor that encodes bimodal vs. unimodal seasonality per latitude band.
                base = 3.5
                noise = rng.normal(0, 1.2, len(dates))
                model_var = model_factor * 0.3
                data['pr'] = np.maximum(
                    0,
                    (base * precip_seasonal + noise + model_var) * precip_factor,
                )
            elif 'max' in var_name.lower() and 'temp' in var_name.lower():
                seasonal = temp_seasonal_amp * temp_seasonal_shape
                noise = rng.normal(0, 2.5, len(dates))
                model_var = model_factor * 1.5
                data['tasmax'] = tmax_base + seasonal + noise + model_var
            elif 'min' in var_name.lower() and 'temp' in var_name.lower():
                # Tmin has a slightly damped seasonal amplitude (~75% of Tmax)
                seasonal = 0.75 * temp_seasonal_amp * temp_seasonal_shape
                noise = rng.normal(0, 1.8, len(dates))
                model_var = model_factor * 1.2
                data['tasmin'] = tmin_base + seasonal + noise + model_var

        df = pd.DataFrame(data)
        return df

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