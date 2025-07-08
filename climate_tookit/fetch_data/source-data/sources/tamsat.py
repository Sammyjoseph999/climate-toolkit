"""This module handles the downloading of rainfall and soil moisture data from the 
Tropical Applications of Meteorology using SATellite data (TAMSAT) dataset.

Ref: https://www.tamsat.org.uk/data/

TAMSAT provides rainfall and soil moisture estimates focused on the African continent.

Note:
- Only rainfall and soil moisture data are available via TAMSAT.
"""

import logging
import os
from datetime import date, timedelta
import requests

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

        self.date_from_utc = date_from_utc
        self.date_to_utc = date_to_utc
        self.location_coord = location_coord
        self.aggregation = aggregation

        self.dates = [
            date_from_utc + timedelta(days=i)
            for i in range((date_to_utc - date_from_utc).days + 1)
        ]

    def _download_file(self, url: str, local_path: str):
        """Download a file from the specified URL to the given local path."""
        try:
            response = requests.get(url, stream=True, timeout=30)
            if response.status_code == 200 and url.endswith(".nc"):
                with open(local_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                logger.info(f"Downloaded: {os.path.basename(local_path)}")
            else:
                logger.warning(f"Failed or invalid content: {url} (Status: {response.status_code})")
        except Exception as e:
            logger.error(f"Error downloading {url}: {e}")

    def download_rainfall(
        self,
        settings: Settings,
        dir_name: str = "tamsat_rainfall",
    ):
        """Download daily rainfall data from the TAMSAT dataset."""
        base_url = settings.tamsat.rainfall_url

        os.makedirs(dir_name, exist_ok=True)
        logger.info(f"Downloading TAMSAT rainfall for {len(self.dates)} days...")

        for dt in self.dates:
            year, month, day = dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")
            filename = f"rfe{year}_{month}_{day}.v3.1.nc"
            url = f"{base_url}{year}/{month}/{filename}"
            local_path = os.path.join(dir_name, filename)

            if os.path.exists(local_path):
                logger.info(f"Already exists, skipping: {filename}")
                continue

            self._download_file(url, local_path)

    def download_soil_moisture(
        self,
        settings: Settings,
        dir_name: str = "tamsat_soil_moisture",
    ):
        """Download daily soil moisture data from the TAMSAT dataset."""
        base_url = settings.tamsat.soil_moisture_url

        os.makedirs(dir_name, exist_ok=True)
        logger.info(f"Downloading TAMSAT soil moisture for {len(self.dates)} days...")

        for dt in self.dates:
            year, month, day = dt.strftime("%Y"), dt.strftime("%m"), dt.strftime("%d")
            filename = f"sm{year}_{month}_{day}.v2.3.1.nc"
            url = f"{base_url}{year}/{month}/{filename}"
            local_path = os.path.join(dir_name, filename)

            if os.path.exists(local_path):
                logger.info(f"Already exists, skipping: {filename}")
                continue

            self._download_file(url, local_path)

    def download_temperature(self):
        pass
    
    def download_precipitation(self):
        pass
    
    def download_windspeed(self):
        pass
    
    def download_solar_radiation(self):
        pass
    
    def download_humidity(self):
        pass