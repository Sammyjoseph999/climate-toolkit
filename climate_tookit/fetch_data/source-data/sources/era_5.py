import logging
import os
from datetime import date

import pandas as pd
from cdsapi.api import Client
from dotenv import load_dotenv

from .utils import models
from .utils.settings import Settings, set_logging
from .utils.utils import get_gee_data_daily, get_gee_data_monthly

load_dotenv()

set_logging()
logger = logging.getLogger(__name__)

url = os.environ.get("CDS_URL")
key = os.environ.get("CDS_KEY")
client = Client(url=url, key=key)


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

    def download_precipitation(self):
        raise NotImplementedError

    def download_rainfall(self):
        raise NotImplementedError

    def download_temperature(self):
        raise NotImplementedError

    def download_windspeed(self):
        raise NotImplementedError

    def download_solar_radiation(self):
        raise NotImplementedError

    def download_humidity(self):
        raise NotImplementedError

    def download_soil_moisture(self):
        raise NotImplementedError

    def download_variables(self) -> pd.DataFrame:

        settings = self.settings
        source = self.source
        variables = self.variables
        data_settings = getattr(settings, source.name)

        func = (
            get_gee_data_monthly
            if data_settings.cadence == models.Cadence.monthly.name
            else get_gee_data_daily
        )

        climate_data = func(
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

        cols = ["date"] + available_cols
        return climate_data[cols]
