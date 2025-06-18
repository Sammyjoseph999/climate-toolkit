import logging
from datetime import date
from typing import Optional

import models
import requests
from configs.settings import Settings, set_logging

set_logging()
logger = logging.getLogger(__name__)


class DownloadData(models.DataDownloadBase):
    """Downloads data from the TerraClimate climate dataset in NetCDF format"""

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

    def fetch_data(self, variable: str, year: int):
        """Main function for downloading data from the climate database"""

        url = f"https://climate.northwestknowledge.net/TERRACLIMATE-DATA/TerraClimate_{variable}_{year}.nc"
        output_filename = f"TerraClimate_{variable}_{year}.nc"
        logger.info(f"Dataset being downloaded: {url}")

        try:
            logger.info(f"Downloading file from: {url}")
            response = requests.get(url, stream=True)
            response.raise_for_status()

            with open(output_filename, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            logger.info(f"File '{output_filename}' downloaded successfully.")

        except requests.exceptions.RequestException as e:
            logger.exception(f"Error downloading file: {e}")
        except IOError as e:
            logger.exception(f"Error writing file '{output_filename}': {e}")
        except Exception as e:
            logger.exception(f"An unexpected error occurred: {e}")

    def download_rainfall(self, settings: Optional[Settings]):
        variable: str = "ppt"
        years = range(self.date_from_utc.year, self.date_to_utc.year + 1)
        for year in years:
            self.fetch_data(variable=variable, year=year)

    def download_temperature(self, settings: Optional[Settings]):
        variable: str = "tmax"
        years = range(self.date_from_utc.year, self.date_to_utc)
        for year in years:
            self.fetch_data(variable=variable, year=year)
