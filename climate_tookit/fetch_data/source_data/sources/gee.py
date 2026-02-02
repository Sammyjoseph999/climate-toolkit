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
        location_coord: tuple[float],
        from_date: date,
        to_date: date,
        scale: Optional[float] = None,
        crs: Optional[str] = None,
        location_name: Optional[str] = None,
        max_pixels: float = 1e9,
        tile_scale: float = 1,
    ) -> pd.DataFrame:
        """Internal method to fetch data for a single date range without chunking."""
        ee.Authenticate()
        ee.Initialize(project=os.getenv("GCP_PROJECT_ID"))

        # GEE expects [longitude, latitude], but location_coord is (lat, lon)
        lat, lon = location_coord
        location = ee.Geometry.Point([lon, lat])

        logger.info(f"Fetching data for location: lat={lat}, lon={lon} (GEE Point: [{lon}, {lat}])")
        logger.info(f"Using scale: {scale} meters")

        # Fetch data day by day for maximum reliability
        data_list = []
        current_date = from_date

        while current_date <= to_date:
            try:
                date_str = current_date.strftime("%Y-%m-%d")
                start = ee.Date(date_str)
                end = start.advance(1, "day")

                # Get the image collection for this single day
                collection = ee.ImageCollection(image_name).filterDate(start, end).filterBounds(location)

                # Get the first (and likely only) image
                img = collection.first()

                # Use scale from config, fallback to 5000 if None
                actual_scale = scale if scale is not None else 5000

                # Get all band values using first() reducer
                result = img.reduceRegion(
                    reducer=ee.Reducer.first(),
                    geometry=location,
                    scale=actual_scale,
                    maxPixels=max_pixels,
                    bestEffort=True,
                    tileScale=tile_scale,
                ).getInfo()

                # Add date to result
                result['date'] = date_str

                if result and any(v is not None for k, v in result.items() if k != 'date'):
                    data_list.append(result)
                else:
                    logger.debug(f"No data values for {date_str}, adding empty record")
                    data_list.append({'date': date_str})

            except Exception as e:
                logger.warning(f"Failed to fetch {date_str}: {e}")
                data_list.append({'date': date_str})

            current_date += timedelta(days=1)

        df = pd.DataFrame(data_list) if data_list else pd.DataFrame()

        # DIAGNOSTIC: Log what we actually got from GEE
        if not df.empty:
            logger.info(f"=== GEE RETURNED COLUMNS: {list(df.columns)}")
            if len(df) > 0:
                logger.info(f"=== SAMPLE ROW (first): {df.iloc[0].to_dict()}")

        return df

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

        NOW WITH AUTOMATIC CHUNKING for large date ranges to avoid memory limits!

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
        logger.info(f"Retrieving information from GEE Image: {image_name}")

        total_days = (to_date - from_date).days

        # Warn about large date ranges
        if total_days > 5000:
            logger.warning(f"Date range is large ({total_days} days). This may cause memory issues or timeouts.")

        # Determine if chunking is needed based on dataset and date range
        # High-memory datasets need smaller chunks
        memory_intensive_datasets = [
            'NASA/GPM_L3/IMERG',
            'NASA/GDDP-CMIP6',
        ]

        needs_chunking = False
        chunk_years = 10  

        # Check if this is a memory-intensive dataset
        for dataset_pattern in memory_intensive_datasets:
            if dataset_pattern in image_name:
                needs_chunking = True
                chunk_years = 2 
                break

        # Also chunk if date range is very large (>10 years) regardless of dataset
        if total_days > 3650:  
            needs_chunking = True
            if chunk_years == 10:  
                chunk_years = 5

        # Use chunking if needed
        if needs_chunking:
            logger.info(f"Using chunked download with {chunk_years}-year chunks to avoid memory limits")

            chunks = []
            current_start = from_date
            chunk_num = 1

            while current_start < to_date:
                # Calculate chunk end
                try:
                    chunk_end = date(
                        current_start.year + chunk_years,
                        current_start.month,
                        current_start.day
                    )
                except ValueError:
                    # Handle leap year edge case (Feb 29)
                    chunk_end = date(
                        current_start.year + chunk_years,
                        current_start.month,
                        28
                    )

                # Don't exceed final end date
                if chunk_end > to_date:
                    chunk_end = to_date

                chunk_days = (chunk_end - current_start).days
                logger.info(f"Fetching chunk {chunk_num}: {current_start} to {chunk_end} ({chunk_days} days)")

                try:
                    chunk_df = self._get_gee_data_daily_single_range(
                        image_name=image_name,
                        location_coord=location_coord,
                        from_date=current_start,
                        to_date=chunk_end,
                        scale=scale,
                        crs=crs,
                        location_name=location_name,
                        max_pixels=max_pixels,
                        tile_scale=tile_scale,
                    )

                    if not chunk_df.empty:
                        chunks.append(chunk_df)
                        logger.info(f"Chunk {chunk_num} returned {len(chunk_df)} records")
                    else:
                        logger.warning(f"Chunk {chunk_num} returned no data")

                except Exception as e:
                    logger.error(f"Error fetching chunk {chunk_num}: {e}")

                    # Try splitting this chunk further if it failed
                    if chunk_years > 1:
                        logger.info(f"Retrying chunk {chunk_num} with smaller sub-chunks...")
                        smaller_chunk_years = max(1, chunk_years // 2)

                        sub_start = current_start
                        while sub_start < chunk_end:
                            try:
                                sub_end = date(
                                    sub_start.year + smaller_chunk_years,
                                    sub_start.month,
                                    sub_start.day
                                )
                            except ValueError:
                                sub_end = date(
                                    sub_start.year + smaller_chunk_years,
                                    sub_start.month,
                                    28
                                )

                            if sub_end > chunk_end:
                                sub_end = chunk_end

                            try:
                                sub_df = self._get_gee_data_daily_single_range(
                                    image_name=image_name,
                                    location_coord=location_coord,
                                    from_date=sub_start,
                                    to_date=sub_end,
                                    scale=scale,
                                    crs=crs,
                                    location_name=location_name,
                                    max_pixels=max_pixels,
                                    tile_scale=tile_scale,
                                )

                                if not sub_df.empty:
                                    chunks.append(sub_df)
                                    logger.info(f"Sub-chunk {sub_start} to {sub_end} succeeded")
                            except Exception as sub_e:
                                logger.error(f"Sub-chunk {sub_start} to {sub_end} also failed: {sub_e}")

                            sub_start = sub_end + timedelta(days=1)

                # Move to next chunk
                current_start = chunk_end + timedelta(days=1)
                chunk_num += 1

            # Combine all chunks
            if not chunks:
                logger.warning("No data retrieved from any chunk")
                return pd.DataFrame()

            combined_df = pd.concat(chunks, ignore_index=True)

            # Sort by date and remove duplicates
            if 'date' in combined_df.columns:
                combined_df['date'] = pd.to_datetime(combined_df['date'])
                combined_df = combined_df.sort_values('date').drop_duplicates(subset='date').reset_index(drop=True)

            logger.info(f"Combined {len(chunks)} chunks into {len(combined_df)} total records")
            return combined_df

        else:
            # Small date range - use original single-query method
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

        lat, lon = location_coord
        location = (
            ee.Geometry.Point([lon, lat])
            if location_name is None
            else ee.Geometry.Point([lon, lat], {"location": location_name})
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

            # Don't select bands - reduceRegion will get all bands automatically
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

        # Process results and return DataFrame
        results = months.map(get_single_data)
        features = results.getInfo()

        data_list = [f["properties"] for f in features] if features else []
        return pd.DataFrame(data_list) if data_list else pd.DataFrame()

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