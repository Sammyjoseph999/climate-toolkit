"""Module for applying the DownloadData / SourceData class to download from
different climate databases."""

import sys
import os
sys.path.append(os.path.dirname(__file__))
 
from datetime import date
 
from sources.gee import DownloadData as DownloadGEE
from sources.tamsat import DownloadData as DownloadTAMSAT
from sources.nasa_power import DownloadData as DownloadNASA
from sources.utils.models import ClimateDataset, ClimateVariable, SoilVariable, Location
from sources.utils.settings import Settings
 
 
class SourceData:
    """The main class for retrieving data via a standardised interface."""
 
    def __init__(
        self,
        location_coord: tuple[float],
        variables: list[ClimateVariable|SoilVariable],
        source: ClimateDataset,
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
 
        client = None
 
        if self.source in (
            ClimateDataset.era_5,
            ClimateDataset.terraclimate,
            ClimateDataset.imerg,
            ClimateDataset.chirps,
            ClimateDataset.cmip6,
            ClimateDataset.nex_gddp,
            ClimateDataset.chirts,
            ClimateDataset.agera_5,
            ClimateDataset.soil_grid,
        ):
            client = DownloadGEE(
                variables=variables,
                location_coord=location_coord,
                date_from_utc=date_from_utc,
                date_to_utc=date_to_utc,
                settings=settings,
                source=source,
            )
 
        elif self.source == ClimateDataset.tamsat:
            client = DownloadTAMSAT(
                variables=variables,
                location_coord=location_coord,
                aggregation=None,
                date_from_utc=date_from_utc,
                date_to_utc=date_to_utc,
            )
 
        elif self.source == ClimateDataset.nasa_power:
            client = DownloadNASA(
                variables=variables,
                location_coord=location_coord,
                date_from_utc=date_from_utc,
                date_to_utc=date_to_utc,
                settings=settings,
                source=source,
            )
 
        if client is None:
            raise ValueError(f"No download client defined for source: {self.source}")
 
        self.client = client
 
    def download(self):
        """Download climate data from the remote location."""
 
        return self.client.download_variables()
 
if __name__ == "__main__":
    import time
 
    settings = Settings.load()
 
    location = Location(lon=36.817223, lat=-1.286389)
 
    source_data = SourceData(
        location_coord=(location.lon, location.lat),
        variables=[
            ClimateVariable.precipitation,
            ClimateVariable.max_temperature,
            ClimateVariable.min_temperature,
            ClimateVariable.soil_moisture,
            SoilVariable.ph,
            SoilVariable.bulk_density,
            SoilVariable.clay_content,
            SoilVariable.silt_content,
            SoilVariable.cation_exchange_capacity,
        ],
        source=ClimateDataset.agera_5,
        date_from_utc=date(year=2024, month=1, day=1),
        date_to_utc=date(year=2024, month=3, day=5),
        settings=settings,
    )
 
    start = time.time()
    climate_data = source_data.download()
    end = time.time()
    elapsed = end - start
    print("time taken (secs):", elapsed)
    print(climate_data)
    
# python .\climate_tookit\fetch_data\source_data\source_data.py
