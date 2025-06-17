import os
from datetime import date

import models
from cdsapi.api import Client
from configs.settings import Settings
from dotenv import load_dotenv

load_dotenv()

url = os.environ.get("CDS_URL")
key = os.environ.get("CDS_KEY")
client = Client(url=url, key=key)


class DownloadData(models.DownloadDataBase):
    def init(
        self,
        location_coord: tuple[int],
        variable: models.ClimateVariable,
        source: models.ClimateDataset,
        aggregation: models.AggregationLevel,
        date_from_utc: date,
        date_to_utc: date,
    ):
        super().__init__(
            location_coord=location_coord,
            variable=variable,
            source=source,
            aggregation=aggregation,
            date_from_utc=date_from_utc,
            date_to_utc=date_to_utc,
        )

    def download_rainfall():
        pass

    def download_temperature():
        pass

    def download_precipitation():
        pass

    def download_windspeed():
        pass

    def download_solar_radiation():
        pass

    def download_soil_moisture():
        pass

    def download_pressure_levels(
        self,
        settings: Settings,
        pressure_level: list[str] = ["1000"],
        year: list[str] = ["2025"],
        month: list[str] = ["06"],
        day: list[str] = ["01"],
        time: list[str] = ["00:00"],
        file_name: str = "pressure_levels.zip",
    ) -> None:
        """Downloads ERA5 hourly data on pressure levels from 1940 to present

        ref: https://cds.climate.copernicus.eu/datasets/reanalysis-era5-pressure-levels?tab=download
        """

        dataset = "reanalysis-era5-pressure-levels"
        params = {
            "product_type": ["reanalysis"],
            "variable": ["geopotential"],
            "pressure_level": pressure_level,
            "year": year,
            "month": month,
            "day": day,
            "time": time,
        }
        base_config = settings.era_5.request
        request = {**params, **base_config}
        client.retrieve(name=dataset, request=request, target=file_name)
