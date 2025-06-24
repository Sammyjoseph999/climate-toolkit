"""
This module downloads precipitation data from the CHIRPS dataset.

CHIRPS (Climate Hazards Group InfraRed Precipitation with Station data)
blends satellite imagery with station data to provide high-resolution
precipitation estimates globally.

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

        self.dates = self._generate_date_list()

    def _generate_date_list(self):
        """Generates list of dates between start and end date (inclusive)."""
        return [
            self.date_from_utc + timedelta(days=i)
            for i in range((self.date_to_utc - self.date_from_utc).days + 1)
        ]

    def download_precipitation(self, settings: Settings, dir_name: str = "chirps_data"):
        """Download daily CHIRPS precipitation GeoTIFF files."""
        base_url = settings.chirps.base_url

        os.makedirs(dir_name, exist_ok=True)
        logger.info(f"Downloading CHIRPS precipitation for {len(self.dates)} days...")

        for dt in self.dates:
            year = dt.strftime("%Y")
            filename = f"chirps-v2.0.{dt.strftime('%Y.%m.%d')}.tif.gz"
            url = f"{base_url}{year}/{filename}"
            local_path = os.path.join(dir_name, filename)

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
                    logger.warning(f"Failed to download {filename} (Status: {response.status_code})")
            except Exception as e:
                logger.error(f"Error downloading {filename}: {e}")

    
    def download_rainfall():
        pass

    def download_temperature():
        pass

    def download_windspeed():
        pass

    def download_solar_radiation():
        pass

    def download_humidity():
        pass

    def download_soil_moisture():
        pass
