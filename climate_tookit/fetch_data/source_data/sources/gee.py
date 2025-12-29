"""This module handles the downloading of climate data from climate sources
hosted by Google Earth Engine (GEE)."""

import logging
import os
from datetime import date, timedelta
from typing import Optional, Union

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
        variables: list[Union[models.ClimateVariable, models.SoilVariable]],
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

    def get_gee_data_static(
        self,
        image_name: str,
        location_coord: tuple[float],
        scale: Optional[float] = None,
        crs: Optional[str] = None,
        location_name: Optional[str] = None,
        max_pixels: float = 1e9,
        tile_scale: float = 1,
    ) -> pd.DataFrame:
        """Uses the Google Earth Engine (GEE) API to retrieve static data
        from datasets that don't have temporal components (like SoilGrids).

        API ref: https://developers.google.com/earth-engine/apidocs/

        **Pre-requisites:**\n
        - [Register or create](https://www.google.com/url?q=https%3A%2F%2Fcode.earthengine.google.com%2Fregister)
        a Google Cloud Project
        - Register your project for commercial or noncommercial use
        - Enable the "Google Earth Engine API"

        Args
        ---
        - image_name: The GEE image name
        - location_coord: The point geometry (longitude, latitude)
        - location_name: The name of the location_coord
        - scale: A nominal scale in meters of the projection to work in.
        - crs: The projection to work in. If unspecified, the projection of the
        image's first band is used. If specified in addition to scale, rescaled to the specified scale.
        - tile_scale: A scaling factor between 0.1 and 16 used to adjust cadence
        tile size; setting a larger tileScale (e.g., 2 or 4) uses smaller tiles
        and may enable computations that run out of memory with the default.
        - max_pixels: The maximum number of pixels to reduce.

        Returns
        ---
        A pandas dataframe containing the static variables from the dataset
        """
        logger.info("Authenticating to GEE...")
        ee.Authenticate()
        ee.Initialize(project=os.getenv("GCP_PROJECT_ID"))

        location = (
            ee.Geometry.Point(location_coord)
            if location_name is None
            else ee.Geometry.Point(location_coord, {"location": location_name})
        )

        logger.info(f"Retrieving information from GEE Image: {image_name}")

        try:
            image = ee.Image(image_name)
            expression = image.reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=location,
                scale=scale,
                maxPixels=max_pixels,
                crs=crs,
                bestEffort=True,
                tileScale=tile_scale,
            )

            result = expression.getInfo()
            return pd.DataFrame([result]) if result else pd.DataFrame()

        except Exception as e:
            logger.error(f"Error retrieving static data from GEE: {e}")
            raise

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
        - image_name: The GEE image collection name
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
        A pandas dataframe containing all the variables in that climate dataset
        """

        logger.info("Authenticating to GEE...")
        ee.Authenticate()
        ee.Initialize(project=os.getenv("GCP_PROJECT_ID"))

        location = (
            ee.Geometry.Point(location_coord)
            if location_name is None
            else ee.Geometry.Point(location_coord, {"location": location_name})
        )

        logger.info(f"Retrieving information from GEE Image: {image_name}")

        # Warn if date range is very large
        total_days = (to_date - from_date).days
        if total_days > 3650:  # More than 10 years
            logger.warning(f"Date range is large ({total_days} days). This may cause memory issues or timeouts.")

        dates = []
        current_date = from_date
        while current_date <= to_date:
            dates.append(current_date.strftime("%Y-%m-%d"))
            current_date += timedelta(days=1)

        ee_dates = ee.List(dates)

        def get_single_data(date):
            start_date = ee.Date(date)
            end_date = start_date.advance(1, "day")

            dataset = ee.ImageCollection(image_name).filterDate(start_date, end_date)
            image = dataset.filterBounds(location).first()

            # Check if image exists before trying to reduce
            def compute_if_exists(img):
                expression = img.reduceRegion(
                    reducer=ee.Reducer.mean(),
                    geometry=location,
                    scale=scale,
                    maxPixels=max_pixels,
                    crs=crs,
                    bestEffort=True,
                    tileScale=tile_scale,
                )
                return ee.Feature(None, {"date": start_date.format("YYYY-MM-dd")}).set(expression)
            
            def return_empty():
                return ee.Feature(None, {"date": start_date.format("YYYY-MM-dd")})

            return ee.Algorithms.If(
                dataset.size().gt(0),
                compute_if_exists(image),
                return_empty()
            )

        try:
            results = ee_dates.map(get_single_data)
            features = results.getInfo()

            # Filter out empty features
            data_list = []
            for feature in features:
                props = feature.get("properties", {})
                if props and len(props) > 1:  # More than just 'date'
                    data_list.append(props)
            
            return pd.DataFrame(data_list) if data_list else pd.DataFrame()
        
        except Exception as e:
            logger.error(f"Error in get_gee_data_daily: {e}")
            return pd.DataFrame()

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
        - image_name: The GEE image collection name
        - location_coord: The point geometry (longitude, latitude)
        - from_date, to_date: The date range
        - location_name: The name of the location_coord
        - scale: A nominal scale in meters of the projection to work in.
        - crs: The projection to work in. If unspecified, the projection of the
        image's first band is used. If specified in addition to scale, rescaled to the specified scale.
        - tile_scale: A scaling factor between 0.1 and 16 used to adjust cadence
        tile size; setting a larger tileScale (e.g., 2 or 4) uses smaller tiles
        and may enable computations that run out of memory with the default.
        - max_pixels: The maximum number of pixels to reduce.

        Returns
        ---
        A pandas dataframe containing all the variables in that climate dataset
        """

        logger.info("Authenticating to GEE...")
        ee.Initialize(project=os.environ.get("GCP_PROJECT_ID"))

        location = (
            ee.Geometry.Point(location_coord)
            if location_name is None
            else ee.Geometry.Point(location_coord, {"location": location_name})
        )

        months = ee.List.sequence(
            0,
            to_date.year * 12 + to_date.month - (from_date.year * 12 + from_date.month),
        )
        start_date = ee.Date.fromYMD(from_date.year, from_date.month, 1)

        def get_single_data(month_offset):
            current_month_start = start_date.advance(month_offset, "month")
            current_month_end = current_month_start.advance(1, "month")

            dataset = ee.ImageCollection(image_name).filterDate(
                current_month_start, current_month_end
            )
            image = dataset.filterBounds(location).first()

            # Check if image exists
            def compute_if_exists(img):
                expression = img.reduceRegion(
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
            
            def return_empty():
                return ee.Feature(
                    None, {"date": current_month_start.format("YYYY-MM-dd")}
                )

            return ee.Algorithms.If(
                dataset.size().gt(0),
                compute_if_exists(image),
                return_empty()
            )

        logger.info(f"Retrieving information from GEE Image: {image_name}")

        try:
            results = months.map(get_single_data)
            features = results.getInfo()

            # Filter out empty features
            data_list = []
            for f in features:
                props = f.get("properties", {})
                if props and len(props) > 1:  # More than just 'date'
                    data_list.append(props)
            
            return pd.DataFrame(data_list) if data_list else pd.DataFrame()
        
        except Exception as e:
            logger.error(f"Error in get_gee_data_monthly: {e}")
            return pd.DataFrame()

    def _handle_soil_grid(self, data_settings) -> pd.DataFrame:
        """Handle soil_grid with multiple images.

        Args
        ---
        - data_settings: The settings object containing GEE image configurations

        Returns
        ---
        A pandas dataframe containing soil variables from their respective datasets
        """
        result_data = {}

        for variable in self.variables:
            var_name = variable.name

            gee_image = (
                data_settings.gee_images.get(var_name)
                if isinstance(data_settings.gee_images, dict)
                else getattr(data_settings.gee_images, var_name, None)
            )
            if not gee_image:
                logger.warning(f"No GEE image mapping found for variable '{var_name}'")
                continue

            mapped_col = getattr(data_settings.variable, var_name, None)
            if not mapped_col:
                logger.warning(f"No variable mapping found for '{var_name}'")
                continue

            logger.info(f"Downloading {var_name} from {gee_image}")

            try:
                var_data = self.get_gee_data_static(
                    image_name=gee_image,
                    location_coord=self.location_coord,
                    scale=data_settings.resolution,
                )

                if not var_data.empty and mapped_col in var_data.columns:
                    result_data[var_name] = var_data[mapped_col].iloc[0]
                    logger.info(
                        f"Successfully retrieved {var_name}: {result_data[var_name]}"
                    )
                else:
                    logger.warning(
                        f"No data retrieved for {var_name} - column '{mapped_col}' not found"
                    )

            except Exception as e:
                logger.error(f"Error downloading {var_name}: {e}")

        if result_data:
            logger.info(f"Successfully processed {len(result_data)} soil variables")
            return pd.DataFrame([result_data])
        else:
            logger.warning("No soil data successfully retrieved")
            return pd.DataFrame()

    def download_variables(self) -> pd.DataFrame:
        """Download and process variables from the configured data source.

        This method handles different types of climate datasets including:
        - Static datasets (like soil grids)
        - Daily time series datasets
        - Monthly time series datasets

        Returns
        ---
        A pandas dataframe containing the requested variables, with proper
        column mapping and date handling based on the dataset type.
        """

        try:
            data_settings = getattr(self.settings, self.source.name)
        except AttributeError:
            logger.error(f"Settings for source '{self.source.name}' not found")
            return pd.DataFrame()

        # Handle soil_grid special case with multiple images
        if self.source.name == "soil_grid" and hasattr(data_settings, "gee_images"):
            logger.info(
                "Using enhanced soil variable download with multiple GEE images"
            )
            return self._handle_soil_grid(data_settings)

        # Standard climate data handling
        try:
            if data_settings.cadence == "static":
                climate_data = self.get_gee_data_static(
                    image_name=data_settings.gee_image,
                    location_coord=self.location_coord,
                    scale=data_settings.resolution,
                )
            elif data_settings.cadence == models.Cadence.monthly.name:
                climate_data = self.get_gee_data_monthly(
                    image_name=data_settings.gee_image,
                    location_coord=self.location_coord,
                    from_date=self.date_from_utc,
                    to_date=self.date_to_utc,
                    scale=data_settings.resolution,
                )
            else:
                climate_data = self.get_gee_data_daily(
                    image_name=data_settings.gee_image,
                    location_coord=self.location_coord,
                    from_date=self.date_from_utc,
                    to_date=self.date_to_utc,
                    scale=data_settings.resolution,
                )
        except Exception as e:
            logger.error(f"Error downloading data: {e}")
            return pd.DataFrame()

        if climate_data.empty:
            logger.warning("No data retrieved from GEE")
            return pd.DataFrame()

        # Map columns to variable names and log information
        dataset_cols = list(climate_data.columns)
        req_vars = [v.name for v in self.variables]

        available_cols = []
        missing_vars = []

        for v in self.variables:
            mapped_col = getattr(data_settings.variable, v.name, None)
            if mapped_col and mapped_col in climate_data.columns:
                available_cols.append(mapped_col)
            else:
                logger.warning(
                    f"{self.source.name.upper()} does not have {v.name} data"
                )
                missing_vars.append(v.name)

        logger.info(f"Available columns: {dataset_cols}")
        logger.info(f"Requested variables: {req_vars}")
        logger.info(f"Mapped available columns: {available_cols}")
        if missing_vars:
            logger.info(f"Missing variables: {missing_vars}")

        base_cols = ["date"] if "date" in climate_data.columns else []
        return climate_data[base_cols + available_cols]