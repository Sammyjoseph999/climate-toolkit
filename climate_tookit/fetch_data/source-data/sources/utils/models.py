"""Module for defining base classes and enums"""

from abc import ABC, abstractmethod
from datetime import date
from enum import Enum, auto
from typing import NamedTuple


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


class ClimateDataset(Enum):
    """The enum to represent climate datasets"""

    agera_5 = auto()
    era_5 = auto()
    terraclimate = auto()
    imerg = auto()


class AggregationLevel(Enum):
    """The enum for aggregation levels"""

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
        location_coord: tuple[float],
        aggregation: AggregationLevel,
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
    def download_variables():
        """Retrieves all variables available in the climate database"""
        pass
