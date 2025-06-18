"""Module for applying the DownloadData / SourceData class to download from
different climate databases."""

from datetime import date

import models
from agera_5 import DownloadData as DownloadAgera5
from configs.settings import Settings
from era_5 import DownloadData as DownloadEra5
from terraclimate import DownloadData as DownloadTerra


class SourceData:
    """The main class for retrieving data in a standardised interface."""

    def __init__(
        self,
        location_coord: tuple[int],
        variable: models.ClimateVariable,
        source: models.ClimateDataset,
        aggregation: models.AggregationLevel,
        date_from_utc: date,
        date_to_utc: date,
        settings: Settings,
    ):
        self.location_coord = location_coord
        self.variable = variable
        self.source = source
        self.aggregation = aggregation
        self.date_from_utc = date_from_utc
        self.date_to_utc = date_to_utc
        self.settings = settings

        # determine the client on class instantiation
        if self.source == models.ClimateDataset.agera_5:
            client = DownloadAgera5(
                location_coord=location_coord,
                aggregation=aggregation,
                date_from_utc=self.date_from_utc,
                date_to_utc=self.date_to_utc,
            )

        if self.source == models.ClimateDataset.era_5:
            client = DownloadEra5(
                location_coord=location_coord,
                aggregation=aggregation,
                date_from_utc=date_from_utc,
                date_to_utc=date_to_utc,
            )

        if self.source == models.ClimateDataset.terraclimate:
            client = DownloadTerra(
                location_coord=location_coord,
                aggregation=aggregation,
                date_from_utc=date_from_utc,
                date_to_utc=date_to_utc,
            )

        self.client = client

    def download(self):
        """Performs actual download of the data"""

        # parameters should be handled in the climate dataset module
        if self.variable == models.ClimateVariable.rainfall:
            return self.client.download_rainfall(
                settings=self.settings,
            )

        if self.variable == models.ClimateVariable.temperature:
            return self.client.download_temperature(
                settings=self.settings,
            )


if __name__ == "__main__":
    settings = Settings.load()

    source_data = SourceData(
        location_coord=None,
        variable=models.ClimateVariable.rainfall,
        source=models.ClimateDataset.terraclimate,
        aggregation=None,
        date_from_utc=date(year=2024, month=6, day=1),
        date_to_utc=date(year=2024, month=7, day=2),
        settings=settings,
    )

    source_data.download()
