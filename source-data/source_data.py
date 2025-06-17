"""Module for applying the DownloadData / SourceData class to download from
different climate databases."""

from datetime import date

import models
from agera_5 import DownloadData as DownloadAgera5
from configs.settings import Settings
from era_5 import DownloadData as DownloadEra5


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

        # determine the downloader on class instantiation
        if self.source == models.ClimateDataset.agera_5:
            client = DownloadAgera5(
                location_coord=None,
                variable=None,
                source=None,
                aggregation=None,
                date_from_utc=None,
                date_to_utc=None,
            )

        if self.source == models.ClimateDataset.era_5:
            client = DownloadEra5(
                location_coord=None,
                variable=None,
                source=None,
                aggregation=None,
                date_from_utc=None,
                date_to_utc=None,
            )

        self.downloader = client

    def download(self):
        """Performs actual download of the data"""

        if self.variable == models.ClimateVariable.rainfall:
            return self.downloader.download_rainfall(settings=self.settings)

        if self.variable == models.ClimateVariable.temperature:
            return self.downloader.download_temperature(settings=self.settings)


if __name__ == "__main__":
    settings = Settings.load()

    # download rainfall from AgERA5
    source_data = SourceData(
        location_coord=None,
        variable=models.ClimateVariable.rainfall,
        source=models.ClimateDataset.agera_5,
        aggregation=None,
        date_from_utc=date(year=2025, month=6, day=1),
        date_to_utc=date(year=2025, month=6, day=1),
        settings=settings,
    )

    source_data.download()
