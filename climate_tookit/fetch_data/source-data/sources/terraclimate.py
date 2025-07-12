"""This module downloads data from the TerraClimate website"""

import logging
from datetime import date

import requests

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

    def download_temperature(self):
        raise NotImplementedError

    def download_precipitation(self):
        raise NotImplementedError

    def download_windspeed(self):
        raise NotImplementedError

    def download_solar_radiation(self):
        raise NotImplementedError

    def download_soil_moisture(self):
        raise NotImplementedError

    def download_rainfall(self):
        raise NotImplementedError

    def download_humidity(self):
        raise NotImplementedError

    def download_variables(self):
        raise NotImplementedError
