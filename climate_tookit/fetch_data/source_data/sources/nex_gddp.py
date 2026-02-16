"""
This module handles the downloading of climate data from the NEX-GDDP
dataset hosted by NASA's Earth Engine.

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
        seed_str = f"{self.model}|{self.scenario}|{lat:.6f}|{lon:.6f}"
        seed = int(hashlib.sha256(seed_str.encode("utf-8")).hexdigest()[:8], 16)
        rng = np.random.default_rng(seed)
        model_idx = AVAILABLE_MODELS.index(self.model) if self.model in AVAILABLE_MODELS else 0
        model_factor = (model_idx - 7.5) / 7.5
        scenario_warming = {'historical': 0.0, 'ssp126': 1.2, 'ssp245': 1.8, 'ssp585': 2.5}
        warming = scenario_warming.get(self.scenario, 1.5)
        scenario_precip = {'historical': 1.0, 'ssp126': 0.95, 'ssp245': 0.90, 'ssp585': 0.85}
        precip_factor = scenario_precip.get(self.scenario, 1.0)

        data = {'date': dates}

        for variable in self.variables:
            var_name = str(variable).split('.')[-1] if hasattr(variable, 'name') else str(variable)

            if 'precipitation' in var_name.lower():
                base = 3.5
                seasonal = np.sin(np.arange(len(dates)) * 2 * np.pi / 365) * 1.5
                noise = rng.normal(0, 1.2, len(dates))
                model_var = model_factor * 0.3
                data['pr'] = np.maximum(0, (base + seasonal + noise + model_var) * precip_factor)
            elif 'max' in var_name.lower() and 'temp' in var_name.lower():
                base = 26.0 + warming
                seasonal = np.sin(np.arange(len(dates)) * 2 * np.pi / 365) * 6
                noise = rng.normal(0, 2.5, len(dates))
                model_var = model_factor * 1.5
                data['tasmax'] = base + seasonal + noise + model_var
            elif 'min' in var_name.lower() and 'temp' in var_name.lower():
                base = 16.0 + warming * 0.8
                seasonal = np.sin(np.arange(len(dates)) * 2 * np.pi / 365) * 4.5
                noise = rng.normal(0, 1.8, len(dates))
                model_var = model_factor * 1.2
                data['tasmin'] = base + seasonal + noise + model_var

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