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
from typing import Optional

from .utils import models
from .utils.settings import Settings, set_logging
from .utils.utils import get_gee_data_daily

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

    def download_precipitation(
        self,
        settings: Settings,
        variable_type: Optional[models.VariableType],
    ):
        data_settings = settings.imerg

        climate_data = get_gee_data_daily(
            image_name=data_settings.gee_image,
            location_coord=self.location_coord,
            from_date=self.date_from_utc,
            to_date=self.date_to_utc,
            scale=data_settings.resolution,
        )

        logger.info(f"Available variables: {list(climate_data.columns)}")
        cols = ["date", data_settings.variable.precipitation]
        return climate_data[cols]

    def download_temperature(
        self,
        settings: Settings,
        variable_type: Optional[models.VariableType],
    ):
        logger.warning("IMERG does not have temperature data")

    def download_rainfall(
        self,
        settings: Settings,
        variable_type: Optional[models.VariableType],
    ):
        logger.warning("IMERG does not have rainfall data")

    def download_windspeed(
        self,
        settings: Settings,
        variable_type: Optional[models.VariableType],
    ):
        logger.warning("IMERG does not have wind speed data")

    def download_solar_radiation(
        self,
        settings: Settings,
        variable_type: Optional[models.VariableType],
    ):
        logger.warning("IMERG does not have solar radiation data")

    def download_humidity(
        self,
        settings: Settings,
        variable_type: Optional[models.VariableType],
    ):
        logger.warning("IMERG does not have humidity data")

    def download_soil_moisture(
        self,
        settings: Settings,
        variable_type: Optional[models.VariableType],
    ):
        logger.warning("IMERG does not have soil moisture data")

    def download_variables(
        self,
        settings: Settings,
        variables: list[models.ClimateVariable],
        source: models.ClimateDataset,
    ):

        data_settings = getattr(settings, source.name)

        climate_data = get_gee_data_daily(
            image_name=data_settings.gee_image,
            location_coord=self.location_coord,
            from_date=self.date_from_utc,
            to_date=self.date_to_utc,
            scale=data_settings.resolution,
        )

        dataset_cols = list(climate_data.columns)
        req_vars = [v.name for v in variables]

        available_cols = []
        missing_vars = []
        for v in variables:
            try:
                c = getattr(data_settings.variable, v.name)
                available_cols.append(c)
            except:
                logger.warning(
                    f"{source.name.upper()} does not have {v.name} data"
                )
                missing_vars.append(v.name)

        logger.info(f"Available columns: {dataset_cols}")
        logger.info(f"Requested variables: {req_vars}")
        logger.warning(f"Missing variables: {missing_vars}")

        cols = ["date"] + available_cols
        return climate_data[cols]
