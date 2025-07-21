"""Module for applying the DownloadData / SourceData class to download from
different climate databases."""

from datetime import date

from sources.gee import DownloadData as DownloadGEE
from sources.tamsat import DownloadData as DownloadTAMSAT
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
    ):
        self.location_coord = location_coord
        self.variables = variables
        self.source = source
        self.date_from_utc = date_from_utc
        self.date_to_utc = date_to_utc
        self.settings = settings

        # determine the client on class instantiation
        if self.source in (
            models.ClimateDataset.era_5,
            models.ClimateDataset.terraclimate,
            models.ClimateDataset.imerg,
            models.ClimateDataset.chirps,
            models.ClimateDataset.cmip6,
            models.ClimateDataset.nex_gddp,
        ):
            client = DownloadGEE(
                variables=variables,
                location_coord=location_coord,
                date_from_utc=date_from_utc,
                date_to_utc=date_to_utc,
                settings=settings,
                source=source,
            )
        
        elif self.source == models.ClimateDataset.tamsat:
            client = DownloadTAMSAT(
                variables=variables,
                location_coord=location_coord,
                aggregation=None,
                date_from_utc=date_from_utc,
                date_to_utc=date_to_utc,
            )

        self.client = client

    def download(self):
        """Download climate data from the remote location."""

        return self.client.download_variables()


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
            models.ClimateVariable.soil_moisture,
        ],
        source=models.ClimateDataset.tamsat,
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