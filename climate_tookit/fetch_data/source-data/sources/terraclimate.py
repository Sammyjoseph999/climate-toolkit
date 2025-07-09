"""This module downloads data from the TerraClimate website"""

import logging
from datetime import date
from typing import Optional

import requests

from .utils import models
from .utils.settings import Settings, set_logging
from .utils.utils import get_gee_data_monthly

set_logging()
logger = logging.getLogger(__name__)


class DownloadData(models.DataDownloadBase):
    """Downloads data from the TerraClimate climate dataset in NetCDF format

    ref: https://www.climatologylab.org/wget-terraclimate.html
    """

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

        self.date_from_utc = date_from_utc
        self.date_to_utc = date_to_utc
        self.location_coord = location_coord

    def _fetch_data(self, variable: str, year: int, base_url: str):
        """Main function for downloading data from the climate database"""

        filename = f"TerraClimate_{variable}_{year}.nc"
        url = f"{base_url}{filename}"
        logger.info(f"Dataset being downloaded: {url}")

        try:
            logger.info(f"Downloading file from: {url}")
            response = requests.get(url, stream=True)
            response.raise_for_status()

            with open(filename, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            logger.info(f"File '{filename}' downloaded successfully.")

        except requests.exceptions.RequestException as e:
            logger.exception(f"Error downloading file: {e}")
        except IOError as e:
            logger.exception(f"Error writing file '{filename}': {e}")
        except Exception as e:
            logger.exception(f"An unexpected error occurred: {e}")

    def _download_from_date_range(
        self, from_date: date, to_date: date, variable: str, url: str
    ):
        """Downloads datasets for a climate variable from TerraClimate given a
        date range."""

        years = range(from_date.year, to_date.year + 1)
        for year in years:
            self._fetch_data(variable=variable, year=year, base_url=url)

    def download_temperature(
        self,
        settings: Settings,
        variable_type: models.VariableType,
    ):

        if variable_type == models.VariableType.min:
            variable = settings.terraclimate.variable.min_temperature
        elif variable_type == models.VariableType.max:
            variable = settings.terraclimate.variable.max_temperature
        else:
            variable = settings.terraclimate.variable.max_temperature

        data_settings = settings.terraclimate

        climate_data = get_gee_data_monthly(
            image_name=data_settings.gee_image,
            location_coord=self.location_coord,
            from_date=self.date_from_utc,
            to_date=self.date_to_utc,
            scale=data_settings.resolution,
        )

        logger.info(f"Available variables: {list(climate_data.columns)}")
        cols = ["date", variable]
        return climate_data[cols]

    def download_precipitation(
        self,
        settings: Settings,
        variable_type: Optional[models.VariableType],
    ):
        variable = settings.terraclimate.variable.precipitation

        data_settings = settings.terraclimate

        climate_data = get_gee_data_monthly(
            image_name=data_settings.gee_image,
            location_coord=self.location_coord,
            from_date=self.date_from_utc,
            to_date=self.date_to_utc,
            scale=data_settings.resolution,
        )

        logger.info(f"Available variables: {list(climate_data.columns)}")
        cols = ["date", variable]
        return climate_data[cols]

    def download_windspeed(
        self,
        settings: Settings,
        variable_type: Optional[models.VariableType],
    ):
        variable = settings.terraclimate.variable.wind_speed

        data_settings = settings.terraclimate

        climate_data = get_gee_data_monthly(
            image_name=data_settings.gee_image,
            location_coord=self.location_coord,
            from_date=self.date_from_utc,
            to_date=self.date_to_utc,
            scale=data_settings.resolution,
        )

        logger.info(f"Available variables: {list(climate_data.columns)}")
        cols = ["date", variable]
        return climate_data[cols]

    def download_solar_radiation(
        self,
        settings: Settings,
        variable_type: Optional[models.VariableType],
    ):
        variable = settings.terraclimate.variable.solar_radiation

        data_settings = settings.terraclimate

        climate_data = get_gee_data_monthly(
            image_name=data_settings.gee_image,
            location_coord=self.location_coord,
            from_date=self.date_from_utc,
            to_date=self.date_to_utc,
            scale=data_settings.resolution,
        )

        logger.info(f"Available variables: {list(climate_data.columns)}")
        cols = ["date", variable]
        return climate_data[cols]

    def download_soil_moisture(
        self,
        settings: Settings,
        variable_type: Optional[models.VariableType],
    ):
        variable = settings.terraclimate.variable.soil_moisture

        data_settings = settings.terraclimate

        climate_data = get_gee_data_monthly(
            image_name=data_settings.gee_image,
            location_coord=self.location_coord,
            from_date=self.date_from_utc,
            to_date=self.date_to_utc,
            scale=data_settings.resolution,
        )

        logger.info(f"Available variables: {list(climate_data.columns)}")
        cols = ["date", variable]
        return climate_data[cols]

    def download_rainfall(
        self,
        settings: Settings,
        variable_type: Optional[models.VariableType],
    ):
        logger.warning("TerraClimate does not have rainfall data.")

    def download_humidity(
        self,
        settings: Settings,
        variable_type: Optional[models.VariableType],
    ):
        logger.warning("TerraClimate does not have humidity data.")
