"""
This module downloads precipitation data from the CHIRPS dataset using Google Earth Engine.
 
CHIRPS (Climate Hazards Group InfraRed Precipitation with Station data)
blends satellite imagery with station data to provide high-resolution
precipitation estimates globally.
 
Dataset ID (GEE): UCSB-CHG/CHIRPS/DAILY
"""
 
import logging
from datetime import date
import ee
 
from .utils import models
from .utils.settings import Settings, set_logging
 
set_logging()
logger = logging.getLogger(__name__)
 
 
class DownloadData(models.DataDownloadBase):
    def __init__(
        self,
        location_coord: tuple[float],
        aggregation: models.AggregationLevel,
        date_from_utc: date,
        date_to_utc: date,
    ):
        super().__init__(
            location_coord=location_coord,
            aggregation=aggregation,
            date_from_utc=date_from_utc,
            date_to_utc=date_to_utc,
        )
 
        self.settings = Settings.load()
 
        try:
            ee.Initialize(project=self.settings.gee_project_id)
        except Exception as e:
            logger.warning(f"Initial GEE project init failed: {e}. Attempting authentication...")
            ee.Authenticate()
            ee.Initialize(project=self.settings.gee_project_id)
 
        self.location_coord = location_coord
        self.aggregation = aggregation
        self.date_from_utc = date_from_utc
        self.date_to_utc = date_to_utc
 
    def download_precipitation(self, settings: Settings):
        """Returns CHIRPS precipitation values from GEE as a list of daily values for the point."""
        lat, lon = self.location_coord
        point = ee.Geometry.Point(lon, lat)
        
        start = self.date_from_utc.strftime("%Y-%m-%d")
        end = self.date_to_utc.strftime("%Y-%m-%d")
    
        collection = ee.ImageCollection("UCSB-CHG/CHIRPS/DAILY") \
            .filterDate(start, end) \
            .filterBounds(point)
    
        def extract(img):
            value = img.reduceRegion(
                reducer=ee.Reducer.first(),
                geometry=point,
                scale=5000
            )
            date_str = img.date().format("YYYY-MM-dd")
            return ee.Feature(None, value).set("date", date_str)
    
        logger.info(f"Fetching CHIRPS data from GEE for {start} to {end} at {lat}, {lon}")
    
        features = collection.map(extract)
        result = features.aggregate_array("precipitation").getInfo()
    
        return result
 
    def download_rainfall(self, settings: Settings):
        pass
 
    def download_temperature(self, settings: Settings):
        pass
 
    def download_windspeed(self, settings: Settings):
        pass
 
    def download_solar_radiation(self, settings: Settings):
        pass
 
    def download_humidity(self, settings: Settings):
        pass
 
    def download_soil_moisture(self, settings: Settings):
        pass