"""Module for defining base classes and enums"""

from abc import ABC, abstractmethod
from datetime import date
from enum import Enum, auto
from typing import NamedTuple

import pandas as pd


class VariableType(Enum):
    max = auto()
    min = auto()
    mean = auto()


class ClimateVariable(Enum):
    """The enum for climate variables"""

    rainfall = auto()
    max_temperature = auto()
    min_temperature = auto()
    precipitation = auto()
    wind_speed = auto()
    solar_radiation = auto()
    humidity = auto()
    soil_moisture = auto()

class SoilVariable(Enum):
    """Soil-specific variables from ISRIC SoilGrids250m v2.0"""
    bulk_density = auto()
    coarse_fragments = auto()
    ph = auto()
    sand_content = auto()
    clay_content = auto()
    organic_carbon = auto()
    organic_carbon_stock = auto()
    soil_moisture = auto()

class ClimateDataset(Enum):
    """The enum to represent climate datasets"""

    agera_5 = auto()
    era_5 = auto()
    terraclimate = auto()
    imerg = auto()
    chirps = auto()
    cmip6 = auto()
    nex_gddp = auto()
    nasa_power = auto()
    tamsat = auto()
    chirts = auto()
    soil_grid = auto()


class Cadence(Enum):
    """The enum for cadence levels"""

    hourly = auto()
    daily = auto()
    monthly = auto()


class Location(NamedTuple):
    lat: float
    lon: float


class DataDownloadBase(ABC):
    """An abstract class for creating astandardised interface for downloading data"""

    def __init__(
        self,
        variables: list[ClimateVariable],
        location_coord: tuple[float],
        date_from_utc: date,
        date_to_utc: date,
    ):
        pass

    @abstractmethod
    def download_rainfall():
        """Retrieves rainfall data from the climate database"""
        # The parameters here can be flexible while reusing the ones initialised
        pass

    @abstractmethod
    def download_temperature():
        """Retrieves temperature data from the climate database"""
        # The parameters here can be flexible while reusing the ones initialised
        pass

    @abstractmethod
    def download_precipitation():
        """Retrieves precipitation data from the climate database"""
        pass

    @abstractmethod
    def download_windspeed():
        """Retrieves wind speed data from the climate database"""
        pass

    @abstractmethod
    def download_solar_radiation():
        """Retrieves solar radiation data from the climate database"""
        pass

    @abstractmethod
    def download_humidity():
        """Retrieves humidity data from the climate database"""
        pass

    @abstractmethod
    def download_soil_moisture():
        """Retrieves soil moisture data from the climate database"""
        pass

    @abstractmethod
    def download_variables() -> pd.DataFrame:
        """Retrieves all variables available in the climate database"""
        pass