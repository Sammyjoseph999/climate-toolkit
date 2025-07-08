"""Module for applying the DownloadData / SourceData class to download from
different climate databases."""

from datetime import date
from typing import Optional
from sources.agera_5 import DownloadData as DownloadAgera5
from sources.era_5 import DownloadData as DownloadEra5
from sources.imerg import DownloadData as DownloadImerg
from sources.terraclimate import DownloadData as DownloadTerra
from sources.chirps import DownloadData as DownloadChirps
from sources.tamsat import DownloadData as DownloadTamsat
from sources.utils import models
from sources.utils.settings import Settings


class SourceData:
    """The main class for retrieving data via a standardised interface."""

    def __init__(
        self,
        location_coord: tuple[float],
        variable: models.ClimateVariable,
        source: models.ClimateDataset,
        date_from_utc: date,
        date_to_utc: date,
        settings: Settings,
        variable_type: Optional[models.VariableType],
        aggregation: Optional[models.AggregationLevel],
    ):
        self.location_coord = location_coord
        self.source = source
        self.aggregation = aggregation
        self.date_from_utc = date_from_utc
        self.date_to_utc = date_to_utc
        self.settings = settings
        self.variable_type = variable_type

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

        if self.source == models.ClimateDataset.chirps:
            client = DownloadChirps(
                location_coord=location_coord,
                aggregation=aggregation,
                date_from_utc=self.date_from_utc,
                date_to_utc=self.date_to_utc,
            )
        
        if self.source == models.ClimateDataset.tamsat:
            client = DownloadTamsat(
                location_coord=location_coord,
                aggregation=aggregation,
                date_from_utc=self.date_from_utc,
                date_to_utc=self.date_to_utc,
            )
            
        self.client = client

    def download(self):
        """Download climate data from the remote location."""

        # parameters should be handled in the climate dataset module
        if self.variable == models.ClimateVariable.rainfall:
            return self.client.download_rainfall(
                settings=self.settings, variable_type=self.variable_type
            )

        if self.variable == models.ClimateVariable.temperature:
            return self.client.download_temperature(
                settings=self.settings, variable_type=self.variable_type
            )

        if self.variable == models.ClimateVariable.precipitation:
            return self.client.download_precipitation(
                settings=self.settings, variable_type=self.variable_type
            )

        if self.variable == models.ClimateVariable.wind_speed:
            return self.client.download_windspeed(
                settings=self.settings, variable_type=self.variable_type
            )

        if self.variable == models.ClimateVariable.solar_radiation:
            return self.client.download_solar_radiation(
                settings=self.settings, variable_type=self.variable_type
            )

        if self.variable == models.ClimateVariable.humidity:
            return self.client.download_humidity(
                settings=self.settings, variable_type=self.variable_type
            )

        if self.variable == models.ClimateVariable.soil_moisture:
            return self.client.download_soil_moisture(
                settings=self.settings, variable_type=self.variable_type
            )
            
        if self.variable == models.ClimateVariable.soil_moisture:
            return self.client.download_soil_moisture(
                settings = self.settings,
            )
        
        raise NotImplemented(f"Download not implemented for variable: {self.variable}")
        
        

if __name__ == "__main__":
    settings = Settings.load()

    source_data = SourceData(
        location_coord=(-1.18, 36.343),
        source=models.ClimateDataset.imerg,
        variable_type=None,
        source=models.ClimateDataset.terraclimate,
        aggregation=None,
        date_from_utc=date(year=2024, month=1, day=1),
        date_to_utc=date(year=2024, month=1, day=1),
        settings=settings,
    )

    source_data.download()
