"""This module downloads data from the AgERA 5 climate dataset. AgERA5 takes
the original ERA5 dataset and pre-processes it so that users can have meaningful
inputs for their analysis and models.

API params: https://cds.climate.copernicus.eu/datasets/reanalysis-era5-pressure-levels?tab=download
"""

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

    def download_rainfall(
        self,
        settings: Settings,
        client: Client = client,
        year: list[str] = ["2025"],
        month: list[str] = ["06"],
        day: list[str] = ["01"],
        file_name: str = "rainfall.zip",
    ) -> None:
        """Downloads rainfall indicators from 1979 to present derived from reanalysis

        Args
        ---
        file_name: file name to save the downloaded data
        year: the year to download the data (1979 - present)
        month: the month to download the data (01-12)
        day: the day of the month to download the data (01-31)
        """

        # TODO: confirm that this is the correct name of the rainfall dataset
        params = {
            "variable": "liquid_precipitation_duration_fraction",
            "year": year,
            "month": month,
            "day": day,
        }

        base_config = settings.agera_5.request
        dataset_name = settings.agera_5.dataset
        request = {**params, **base_config}
        client.retrieve(name=dataset_name, request=request, target=file_name)

    def download_temperature(
        self,
        settings: Settings,
        client: Client = client,
        year: list[str] = ["2025"],
        month: list[str] = ["06"],
        day: list[str] = ["01"],
        file_name: str = "temperature.zip",
        statistic: list[str] = ["24_hour_mean"],
    ) -> None:
        """Downloads rainfall indicators from 1979 to present derived from reanalysis

        Args
        ---
        file_name: file name to save the downloaded data
        year: the year to download the data (1979 - present)
        month: the month to download the data (01-12)
        day: the day of the month to download the data (01-31)
        statistic: aggregation statistic. One of [
            "24_hour_maximum",
            "24_hour_mean",
            "24_hour_minimum",
            "day_time_maximum",
            "day_time_mean",
            "night_time_mean",
            "night_time_minimum"
        ]
        """

        params = {
            "variable": "2m_temperature",
            "statistic": statistic,
            "year": year,
            "month": month,
            "day": day,
        }

        base_config = settings.agera_5.request
        dataset_name = settings.agera_5.dataset
        request = {**params, **base_config}
        client.retrieve(name=dataset_name, request=request, target=file_name)

    def download_precipitation():
        pass

    def download_windspeed():
        pass

    def download_solar_radiation():
        pass

    def download_soil_moisture():
        pass
