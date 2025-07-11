"""Module for applying the DownloadData / SourceData class to download from
different climate databases."""

from datetime import date
from typing import Optional

from sources.agera_5 import DownloadData as DownloadAgera5
from sources.era_5 import DownloadData as DownloadEra5
from sources.imerg import DownloadData as DownloadImerg
from sources.terraclimate import DownloadData as DownloadTerra
from sources.utils import models
from sources.utils.models import Location
from sources.utils.settings import Settings


class SourceData:
    """The main class for retrieving data via a standardised interface."""

    def __init__(
        self,
        location_coord: tuple[float],
        variables: list[models.ClimateVariable],
        source: models.ClimateDataset,
        date_from_utc: date,
        date_to_utc: date,
        settings: Settings,
        aggregation: Optional[models.AggregationLevel],
    ):
        self.location_coord = location_coord
        self.variables = variables
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

        if self.source == models.ClimateDataset.imerg:
            client = DownloadImerg(
                location_coord=location_coord,
                aggregation=aggregation,
                date_from_utc=date_from_utc,
                date_to_utc=date_to_utc,
            )

        self.client = client

    def download(self):
        """Download climate data from the remote location."""

        return self.client.download_variables(
            settings=self.settings,
            variables=self.variables,
            source=self.source,
        )


if __name__ == "__main__":
    import time

    settings = Settings.load()

    # nairobi = (36.817223, -1.286389)
    location = Location(lon=36.817223, lat=-1.286389)

    source_data = SourceData(
        location_coord=(location.lon, location.lat),
        variables=[
            models.ClimateVariable.precipitation,
            models.ClimateVariable.max_temperature,
            models.ClimateVariable.min_temperature,
        ],
        source=models.ClimateDataset.imerg,
        aggregation=None,
        date_from_utc=date(year=2020, month=1, day=1),
        date_to_utc=date(year=2020, month=3, day=5),
        settings=settings,
    )

    start = time.time()
    climate_data = source_data.download()
    end = time.time()
    elapsed = end - start
    print("time taken (secs):", elapsed)
    print(climate_data)
