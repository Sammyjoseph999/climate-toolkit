"""
This module downloads precipitation data from the CHIRPS dataset using Google Earth Engine.
 
CHIRPS (Climate Hazards Group InfraRed Precipitation with Station data)
blends satellite imagery with station data to provide high-resolution
precipitation estimates globally.
 
Dataset ID (GEE): UCSB-CHG/CHIRPS/DAILY
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
 
    def download_rainfall(self):
        raise NotImplementedError
 
    def download_temperature(self):
        raise NotImplementedError
 
    def download_windspeed(self):
        raise NotImplementedError
 
    def download_solar_radiation(self):
        raise NotImplementedError
 
    def download_humidity(self):
        raise NotImplementedError
 
    def download_soil_moisture(self):
        raise NotImplementedError