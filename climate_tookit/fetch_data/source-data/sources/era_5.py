import logging
import os
from datetime import date
from typing import Optional

from cdsapi.api import Client
from dotenv import load_dotenv

from .utils import models
from .utils.settings import Settings, set_logging
from .utils.utils import get_gee_data_daily

load_dotenv()

set_logging()
logger = logging.getLogger(__name__)

url = os.environ.get("CDS_URL")
key = os.environ.get("CDS_KEY")
client = Client(url=url, key=key)


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

    def download_precipitation(
        self,
        settings: Settings,
        variable_type: Optional[models.VariableType],
    ):
        data_settings = settings.era_5

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

    def download_rainfall(
        self,
        settings: Settings,
        variable_type: Optional[models.VariableType],
    ):
        logger.warning("ERA5 does not have rainfall data")

    def download_temperature(
        self,
        settings: Settings,
        variable_type: Optional[models.VariableType],
    ):
        logger.warning("ERA5 does not have temperature data")

    def download_windspeed(
        self,
        settings: Settings,
        variable_type: Optional[models.VariableType],
    ):
        logger.warning("ERA5 does not have wind speed data")

    def download_solar_radiation(
        self,
        settings: Settings,
        variable_type: Optional[models.VariableType],
    ):
        logger.warning("ERA5 does not have solar radiation data")

    def download_humidity(
        self,
        settings: Settings,
        variable_type: Optional[models.VariableType],
    ):
        logger.warning("ERA5 does not have humidity data")

    def download_soil_moisture(
        self,
        settings: Settings,
        variable_type: Optional[models.VariableType],
    ):
        logger.warning("ERA5 does not have soil moisture data")
