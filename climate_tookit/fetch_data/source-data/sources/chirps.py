"""
This module downloads rainfall data from the CHIRPS dataset.

CHIRPS (Climate Hazards Group InfraRed Precipitation with Station data)
is a global rainfall dataset blending satellite imagery with station data.

Ref: https://data.chc.ucsb.edu/products/CHIRPS-2.0/
"""

import os
import logging
from datetime import date, timedelta

import requests

from .utils import models
from .utils.settings import Settings, set_logging

set_logging()
logger = logging.getLogger(__name__)

CHIRPS_BASE_URL = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/tifs/p05/"


class DownloadData(models.DataDownloadBase):
    def __init__(
        self,
        location_coord: tuple[float],
        aggregation: models.AggregationLevel,
        date_from_utc: date,
        date_to_utc: date,
    ):
        super().__init__(location_coord, aggregation, date_from_utc, date_to_utc)

        self.date_from_utc = date_from_utc
        self.date_to_utc = date_to_utc
        self.dates = self._generate_date_list()

    def _generate_date_list(self):
        current = self.date_from_utc
        return [current + timedelta(days=i) for i in range((self.date_to_utc - current).days + 1)]

    def download_rainfall(self, settings: Settings, download_dir: str = "chirps_data"):
        os.makedirs(download_dir, exist_ok=True)
        logger.info(f"Downloading CHIRPS for {len(self.dates)} days...")

        for dt in self.dates:
            year = dt.strftime("%Y")
            filename = f"chirps-v2.0.{dt.strftime('%Y.%m.%d')}.tif.gz"
            url = f"{CHIRPS_BASE_URL}{year}/{filename}"
            local_path = os.path.join(download_dir, filename)

            if os.path.exists(local_path):
                logger.info(f"Already exists, skipping: {filename}")
                continue

            try:
                response = requests.get(url, stream=True, timeout=30)
                if response.status_code == 200:
                    with open(local_path, "wb") as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            f.write(chunk)
                    logger.info(f"Downloaded: {filename}")
                else:
                    logger.warning(f"Failed: {filename} (Status: {response.status_code})")
            except Exception as e:
                logger.error(f"Error downloading {filename}: {e}")

    def download_temperature(self, settings: Settings):
        pass

    def download_precipitation(self, settings: Settings):
        pass

    def download_windspeed(self, settings: Settings):
        pass

    def download_solar_radiation(self, settings: Settings):
        pass

    def download_humidity(self, settings: Settings):
        pass

    def download_soil_moisture(self, settings: Settings):
        pass
