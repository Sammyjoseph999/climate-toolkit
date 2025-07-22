"""This module handles the downloading of climate data from climate sources
hosted by Google Earth Engine (GEE)."""
 
import logging
import os
from datetime import date, timedelta
from typing import Optional
 
from dotenv import load_dotenv
 
load_dotenv()
 
import ee
import pandas as pd
 
from .utils import models
from .utils.models import Cadence
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
 
    def get_gee_data_daily(
        self,
        image_name: str,
        location_coord: tuple[float],
        from_date: date,
        to_date: date,
        scale: Optional[float] = None,
        crs: Optional[str] = None,
        location_name: Optional[str] = None,
        max_pixels: float = 1e9,
        cadence: Cadence = Cadence.daily,
        tile_scale: float = 1,
    ) -> pd.DataFrame:
        """Uses the Google Earth Engine (GEE) API to retrieve weather information
        from a weather dataset. Used for dataset whose cadence is daily.
 
        API ref: https://developers.google.com/earth-engine/apidocs/
 
        **Pre-requisites:**\n
        - [Register or create](https://www.google.com/url?q=https%3A%2F%2Fcode.earthengine.google.com%2Fregister)
        a Google Cloud Project
        - Register your project for commercial or noncommercial use
        - Enable the "Google Earth Engine API"
 
        Args
        ---
        - collection: The GEE image collection name
        - location_coord: The point geometry (longitude, latitude)
        - from_date, to_date: The date range
        - location_name: The name of the location_coord
        - cadence: cadence type (daily, monthly, etc). Only daily is currently supported
        - scale: A nominal scale in meters of the projection to work in.
        - crs: The projection to work in. If unspecified, the projection of the
        image's first band is used. If specified in addition to scale, rescaled to the specified scale.
        - tile_scale: A scaling factor between 0.1 and 16 used to adjust cadence
        tile size; setting a larger tileScale (e.g., 2 or 4) uses smaller tiles
        and may enable computations that run out of memory with the default.
        - max_pixels: The maximum number of pixels to reduce.
 
        Returns
        ---
        A pandas dataframe containing all the varibles in that climate dataset
        """
 
        logger.info("Authenticating to GEE...")
        ee.Authenticate()
        ee.Initialize(project=os.getenv("GCP_PROJECT_ID"))
 
        # define the location
        location = (
            ee.Geometry.Point(location_coord)
            if location_name is None
            else ee.Geometry.Point(location_coord, {"location": location_name})
        )
 
        date_format = "%Y-%m-%d"
        delta = timedelta(days=1)
        current_date = from_date
        tbl = pd.DataFrame()
 
        logger.info(f"Retrieving information from GEE Image: {image_name}")
 
        dates = []
        while current_date <= to_date:
            dates.append(current_date.strftime(date_format))
            current_date += delta
 
        ee_dates = ee.List(dates)
 
        def get_single_data(date):
            start_date = ee.Date(date)
            end_date = start_date.advance(1, "day")
 
            dataset = ee.ImageCollection(image_name).filterDate(
                start_date, end_date
            )
            image = dataset.filterBounds(location).first()
 
            expression = image.reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=location,
                scale=scale,
                maxPixels=max_pixels,
                crs=crs,
                bestEffort=True,
                tileScale=tile_scale,
            )
 
            return ee.Feature(
                None, {"date": start_date.format("YYYY-MM-dd")}
            ).set(expression)
 
        results = ee_dates.map(get_single_data)
        features = results.getInfo()
 
        data_list = []
        for feature in features:
            data_list.append(feature["properties"])
 
        if not data_list:
            return pd.DataFrame()
 
        tbl = pd.DataFrame(data_list)
        return tbl
 
    def get_gee_data_monthly(
        self,
        image_name: str,
        location_coord: tuple[float],
        from_date: date,
        to_date: date,
        scale: Optional[float] = None,
        crs: Optional[str] = None,
        location_name: Optional[str] = None,
        max_pixels: float = 1e9,
        tile_scale: float = 1,
    ):
        """Uses the Google Earth Engine (GEE) API to retrieve weather information
        from a weather dataset. Used for dataset whose cadence is monthly.
 
        API ref: https://developers.google.com/earth-engine/apidocs/
 
        **Pre-requisites:**\n
        - [Register or create](https://www.google.com/url?q=https%3A%2F%2Fcode.earthengine.google.com%2Fregister)
        a Google Cloud Project
        - Register your project for commercial or noncommercial use
        - Enable the "Google Earth Engine API"
 
        Args
        ---
        - collection: The GEE image collection name
        - location_coord: The point geometry (longitude, latitude)
        - from_date, to_date: The date range
        - location_name: The name of the location_coord
        - cadence: cadence type (daily, monthly, etc). Only daily is currently supported
        - scale: A nominal scale in meters of the projection to work in.
        - crs: The projection to work in. If unspecified, the projection of the
        image's first band is used. If specified in addition to scale, rescaled to the specified scale.
        - tile_scale: A scaling factor between 0.1 and 16 used to adjust cadence
        tile size; setting a larger tileScale (e.g., 2 or 4) uses smaller tiles
        and may enable computations that run out of memory with the default.
        - max_pixels: The maximum number of pixels to reduce.
 
        Returns
        ---
        A pandas dataframe containing all the varibles in that climate dataset
        """
 
        logger.info("Authenticating to GEE...")
        ee.Authenticate()
        ee.Initialize(project=os.environ.get("GCP_PROJECT_ID"))
 
        location = (
            ee.Geometry.Point(location_coord)
            if location_name is None
            else ee.Geometry.Point(location_coord, {"location": location_name})
        )
 
        months = ee.List.sequence(
            0,
            to_date.year * 12
            + to_date.month
            - (from_date.year * 12 + from_date.month),
        )
        start_date = ee.Date.fromYMD(from_date.year, from_date.month, 1)
 
        def get_single_data(month_offset):
            current_month_start = start_date.advance(month_offset, "month")
            current_month_end = current_month_start.advance(1, "month")
 
            dataset = ee.ImageCollection(image_name).filterDate(
                current_month_start, current_month_end
            )
            image = dataset.filterBounds(location).first()
 
            expression = image.reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=location,
                scale=scale,
                maxPixels=max_pixels,
                crs=crs,
                bestEffort=True,
                tileScale=tile_scale,
            )
 
            return ee.Feature(
                None, {"date": current_month_start.format("YYYY-MM-dd")}
            ).set(expression)
 
        logger.info(f"Retrieving information from GEE Image: {image_name}")
 
        results = months.map(get_single_data)
        features = results.getInfo()
 
        if not features:
            return pd.DataFrame()
 
        data_list = [f["properties"] for f in features]
        tbl = pd.DataFrame(data_list)
        return tbl
 
    def download_variables(self) -> pd.DataFrame:
 
        settings = self.settings
        source = self.source
        variables = self.variables
        data_settings = getattr(settings, source.name)
 
        func = (
            self.get_gee_data_monthly
            if data_settings.cadence == models.Cadence.monthly.name
            else self.get_gee_data_daily
        )
 
        climate_data = func(
            image_name=data_settings.gee_image,
            location_coord=self.location_coord,
            from_date=self.date_from_utc,
            to_date=self.date_to_utc,
            scale=data_settings.resolution,
        )
 
        dataset_cols = list(climate_data.columns)
        req_vars = [v.name for v in variables]
 
        available_cols = []
        missing_vars = []
        for v in variables:
            c = getattr(data_settings.variable, v.name)
            if c is not None:
                available_cols.append(c)
            else:
                logger.warning(
                    f"{source.name.upper()} does not have {v.name} data"
                )
                missing_vars.append(v.name)
 
        logger.info(f"Available columns: {dataset_cols}")
        logger.info(f"Requested variables: {req_vars}")
 
        cols = ["date"] + available_cols
        return climate_data[cols]