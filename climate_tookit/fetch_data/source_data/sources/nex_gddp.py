"""
This module handles the downloading of climate data from the NEX-GDDP
dataset hosted by NASA's Earth Engine.

Dataset Reference:
https://developers.google.com/earth-engine/datasets/catalog/NASA_NEX-GDDP

Pre-requisites:
1. GEE Authentication using `earthengine authenticate`
2. Ensure project ID is authorized in GCP if needed
"""

import logging
from datetime import date

from .utils import models
from .utils.settings import Settings, set_logging

set_logging()
logger = logging.getLogger(__name__)


class DownloadData(models.DataDownloadBase):
    def __init__(
        self,
        variables: list[models.ClimateVariable],
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

    def download_precipitation(self):
        raise NotImplementedError

    def download_temperature(self):
        raise NotImplementedError

    def download_rainfall(self):
        raise NotImplementedError

    def download_windspeed(self):
        raise NotImplementedError

    def download_solar_radiation(self):
        raise NotImplementedError

    def download_humidity(self):
        raise NotImplementedError

    def download_soil_moisture(self):
        raise NotImplementedError

    def download_variables(self):
        raise NotImplementedError