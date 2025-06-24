"""This module downloads data from the TerraClimate website.

Ref: https://www.climatologylab.org/wget-terraclimate.html

TerraClimate provides high-resolution global monthly climate data.
"""

import logging
from datetime import date
from typing import Optional
import requests

from .utils import models
from .utils.settings import Settings, set_logging

set_logging()
logger = logging.getLogger(__name__)


class DownloadData(models.DataDownloadBase):
    """Downloads data from the TerraClimate climate dataset in NetCDF format."""

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

    def _fetch_data(self, variable: str, year: int, base_url: str):
        """Main function for downloading a specific variable's file for a given year."""
        filename = f"TerraClimate_{variable}_{year}.nc"
        url = f"{base_url}{filename}"
        logger.info(f"Dataset being downloaded: {url}")

        try:
            response = requests.get(url, stream=True, timeout=30)
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
        """Downloads datasets for a climate variable from TerraClimate given a date range."""
        years = range(from_date.year, to_date.year + 1)
        for year in years:
            self._fetch_data(variable=variable, year=year, base_url=url)

    def download_temperature(
        self,
        settings: Settings,
        variable_type: models.VariableType,
    ):
        """Download temperature data (min or max)"""
        if variable_type == models.VariableType.min:
            variable = settings.terraclimate.variable.min_temperature
        else:
            variable = settings.terraclimate.variable.max_temperature

        url = settings.terraclimate.url
        self._download_from_date_range(
            variable=variable,
            url=url,
            from_date=self.date_from_utc,
            to_date=self.date_to_utc,
        )

    def download_precipitation(
        self,
        settings: Settings,
        variable_type: Optional[models.VariableType] = None,
    ):
        """Download precipitation data."""
        variable = settings.terraclimate.variable.precipitation
        url = settings.terraclimate.url
        self._download_from_date_range(
            variable=variable,
            url=url,
            from_date=self.date_from_utc,
            to_date=self.date_to_utc,
        )

    def download_soil_moisture(
        self,
        settings: Settings,
        variable_type: Optional[models.VariableType] = None,
    ):
        """Download soil moisture data."""
        variable = settings.terraclimate.variable.soil_moisture
        url = settings.terraclimate.url
        self._download_from_date_range(
            variable=variable,
            url=url,
            from_date=self.date_from_utc,
            to_date=self.date_to_utc,
        )

    def download_solar_radiation(
        self,
        settings: Settings,
        variable_type: Optional[models.VariableType] = None,
    ):
        """Download solar radiation data."""
        variable = settings.terraclimate.variable.solar_radiation
        url = settings.terraclimate.url
        self._download_from_date_range(
            variable=variable,
            url=url,
            from_date=self.date_from_utc,
            to_date=self.date_to_utc,
        )

    def download_windspeed(
        self,
        settings: Settings,
        variable_type: Optional[models.VariableType] = None,
    ):
        """TerraClimate does not provide windspeed data."""
        logger.warning("TerraClimate does not provide windspeed data.")

    def download_rainfall(
        self,
        settings: Settings,
        variable_type: Optional[models.VariableType] = None,
    ):
        """TerraClimate does not provide rainfall as a distinct variable."""
        logger.warning("TerraClimate does not provide rainfall data separately.")

    def download_humidity(
        self,
        settings: Settings,
        variable_type: Optional[models.VariableType] = None,
    ):
        """TerraClimate does not provide humidity data."""
        logger.warning("TerraClimate does not provide humidity data.")
