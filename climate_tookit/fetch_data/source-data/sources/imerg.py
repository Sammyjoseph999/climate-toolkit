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

    def download_variables(self):
        raise NotImplementedError