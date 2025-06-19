"""This module handles the downloading of climate data from the Global
Precipitation Measurement (GPM) mission's IMERG dataset hosted by NASA.

ref: https://disc.gsfc.nasa.gov/information/howto?keywords=IMERG&title=How%20to%20Read%20IMERG%20Data%20Using%20Python

Pre-requisites:
1. Create .netrc and .dodsrc files
2. Accept EULA on the NASA website
"""

import logging
import os
import platform
import shutil
from datetime import date

import earthaccess

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

    @staticmethod
    def create_access_files():
        """Creates .urs_cookies and .dodsrc files to access the NASA website"""

        homeDir = os.path.expanduser("~") + os.sep

        # Create .urs_cookies and .dodsrc files
        with open(homeDir + ".urs_cookies", "w") as file:
            file.write("")
            file.close()
        with open(homeDir + ".dodsrc", "w") as file:
            file.write("HTTP.COOKIEJAR={}.urs_cookies\n".format(homeDir))
            file.write("HTTP.NETRC={}.netrc".format(homeDir))
            file.close()

        print("Saved .urs_cookies and .dodsrc to:", homeDir)

        # Copy dodsrc to working directory in Windows
        if platform.system() == "Windows":
            shutil.copy2(homeDir + ".dodsrc", os.getcwd())
            print("Copied .dodsrc to:", os.getcwd())

    def download_rainfall(
        self,
        settings: Settings,
        dir_name: str = ".",
    ):
        auth = earthaccess.login(strategy="environment")

        temporal: tuple[str] = (
            self.date_from_utc.strftime("%Y-%m-%d"),
            self.date_to_utc.strftime("%Y-%m-%d"),
        )
        bounding_box = (
            self.location_coord[1],
            self.location_coord[0],
            self.location_coord[1],
            self.location_coord[0],
        )

        short_name = getattr(settings.imerg.short_name, self.aggregation.name)
        version = settings.imerg.version

        logger.info(
            f"Searching IMERG database with the parameters: {temporal=}, {bounding_box=}, {short_name=}, {version=}"
        )

        results = earthaccess.search_data(
            short_name=short_name,
            version=version,
            temporal=temporal,
            bounding_box=bounding_box,
        )

        downloaded_files = earthaccess.download(
            results,
            local_path=dir_name,
        )

        return downloaded_files

    def download_temperature(settings):
        pass
