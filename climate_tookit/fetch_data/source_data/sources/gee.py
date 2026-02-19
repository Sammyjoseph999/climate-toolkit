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

        lat, lon = location_coord
        location = (
            ee.Geometry.Point([lon, lat])
            if location_name is None
            else ee.Geometry.Point([lon, lat], {"location": location_name})
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

    def _get_gee_data_daily_single_range(
        self,
        image_name: str,
        location_coord: tuple[float, float],
        from_date: date,
        to_date: date,
        scale: Optional[float] = None,
        crs: Optional[str] = None,
        location_name: Optional[str] = None, # This is not used?
        max_pixels: float = 1e9, # Is this needed? only extracting 1 point per pixel
        tile_scale: float = 1, # same here, not sure if needed but I could be wrong
    ) -> pd.DataFrame:
        """Internal method to fetch data for a single date range"""

        logger.info("Authenticating to GEE...")
        ee.Authenticate()
        ee.Initialize(project=os.getenv("GCP_PROJECT_ID"))

        # GEE expects [longitude, latitude], but location_coord is (lat, lon)
        lat, lon = location_coord
        location = ee.Geometry.Point([lon, lat])

        start = ee.Date(from_date.strftime("%Y-%m-%d"))
        end = ee.Date(to_date.strftime("%Y-%m-%d")).advance(1, "day")

        logger.info(f"Fetching data for location: lat={lat}, lon={lon} (GEE Point: [{lon}, {lat}])")
        logger.info(f"Using scale: {scale} meters")

        collection = (
            ee.ImageCollection(image_name)
            .filterDate(start, end)
            .filterBounds(location)
        )

        def extract(image):
            reduce_args = {
                "reducer": ee.Reducer.first(),
                "geometry": location,
                "maxPixels": max_pixels,
                "tileScale": tile_scale,
            }

            #NOTE: I've deviated from old method here, defaulting to native data scale unless specified
            # It should be easy to revert back to old logic if needed
            if scale is not None:
                reduce_args["scale"] = scale

            if crs is not None:
                reduce_args["crs"] = crs

            values = image.reduceRegion(**reduce_args)
            return ee.Feature(None, values).set(
                "date", image.date().format("YYYY-MM-dd")
            )

        feature_collection = collection.map(extract)

        # Single server call for result rather than one per day
        result = feature_collection.getInfo()

        features = result.get("features", [])
        records = [f["properties"] for f in features]

        df = pd.DataFrame(records) if records else pd.DataFrame()

        if not df.empty:
            logger.info(f"=== GEE RETURNED COLUMNS: {list(df.columns)}")
            logger.info(f"=== SAMPLE ROW (first): {df.iloc[0].to_dict()}")

            df = df.sort_values("date").reset_index(drop=True)

            # Ensure full daily index (GEE processing skips missing days, this refills them to match old output)
            full_range = pd.date_range(from_date, to_date, freq="D")
            df["date"] = pd.to_datetime(df["date"], errors="coerce")

            df = (
                df.set_index("date")
                  .reindex(full_range)
                  .rename_axis("date")
                  .reset_index()
            )

            df["date"] = df["date"].dt.strftime("%Y-%m-%d")
        else:
            # df of dates but empty - I think this is same as old method
            full_range = pd.date_range(from_date, to_date, freq="D")
            df = pd.DataFrame({"date": full_range.strftime("%Y-%m-%d")})

        return df

    # NOTE: I did some pretty aggressive chopping here, so test to make sure I didn't break anything downstream,
    # However, from my testing and understanding - if we are just extracting data for a single point/pixel
    # there is no need for chunking and other usual optimizations
    def get_gee_data_daily(
        self,
        image_name: str,
        location_coord: tuple[float, float],
        from_date: date,
        to_date: date,
        scale: Optional[float] = None,
        crs: Optional[str] = None,
        location_name: Optional[str] = None,
        max_pixels: float = 1e9,
        cadence: Cadence = Cadence.daily,
        tile_scale: float = 1,
    ) -> pd.DataFrame:
        """Retrieve daily data from a GEE ImageCollection (optimized single-query version)."""

        logger.info("Authenticating to GEE...")
        logger.info(f"Retrieving information from GEE Image: {image_name}")

        if cadence != Cadence.daily:
            raise NotImplementedError(
                f"Cadence '{cadence}' is not supported. Only daily cadence is currently supported."
            )

        total_days = (to_date - from_date).days

        if total_days < 0:
            logger.warning("from_date is after to_date. Returning empty DataFrame.")
            return pd.DataFrame()

        if total_days > 5000:
            logger.warning(
                f"Large date range requested ({total_days} days). "
                "Single-query extraction will be attempted."
            )

        return self._get_gee_data_daily_single_range(
            image_name=image_name,
            location_coord=location_coord,
            from_date=from_date,
            to_date=to_date,
            scale=scale,
            crs=crs,
            location_name=location_name,
            max_pixels=max_pixels,
            tile_scale=tile_scale,
        )

    def get_gee_data_monthly(
        self,
        image_name: str,
        location_coord: tuple[float, float],
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

        lat, lon = location_coord
        location = (
            ee.Geometry.Point([lon, lat])
            if location_name is None
            else ee.Geometry.Point([lon, lat], {"location": location_name})
        )

        start_date = ee.Date.fromYMD(from_date.year, from_date.month, 1)
        end_date = ee.Date.fromYMD(to_date.year, to_date.month, 1).advance(1, "month")

        nMonths = end_date.difference(start_date, "month");
        months = ee.List.sequence(0, nMonths.subtract(1));

        # Filter full range once
        collection = (
            ee.ImageCollection(image_name)
            .filterDate(start_date, end_date)
            .filterBounds(location)
        )

        def get_single_data(month_offset):
            current_month_start = start_date.advance(month_offset, "month")
            current_month_end = current_month_start.advance(1, "month")

            monthly_images = collection.filterDate(current_month_start, current_month_end)

            #IMPORTANT: I think there was a potential error in old method.
            # I believe dataset....first() pulls first image (so day 1 of the month)
            # therefore the mean value is the value for the first day of the month not monthly mean

            # GEE uses lazy eval, so it will only run this for the point(s) extracted, not all pixels
            monthly_image = monthly_images.mean()
            reduce_args = {
                "reducer": ee.Reducer.first(),
                "geometry": location,
            }
            
            #NOTE: Same behavior as above method, defaulting to native data scale if not specified
            if scale is not None:
                reduce_args["scale"] = scale

            if crs is not None:
                reduce_args["crs"] = crs

            if max_pixels is not None:
                reduce_args["maxPixels"] = max_pixels

            if tile_scale is not None:
                reduce_args["tileScale"] = tile_scale

            values = monthly_image.reduceRegion(**reduce_args)

            return ee.Feature(None, values).set(
                "date", current_month_start.format("YYYY-MM-dd")
            )
        
        logger.info(f"Retrieving information from GEE Image: {image_name}")
        features = months.map(get_single_data)
        result = features.getInfo()

        data_list = [f["properties"] for f in result] if result else []
        df = pd.DataFrame(data_list) if data_list else pd.DataFrame()

        if not df.empty:
            df = df.sort_values("date").reset_index(drop=True)

        return df

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
