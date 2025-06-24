"""Module for defining base classes and enums"""

from abc import ABC, abstractmethod
from datetime import date
from enum import Enum, auto
from typing import Optional

from .settings import Settings


class VariableType(Enum):
    max = auto()
    min = auto()
    mean = auto()


class ClimateVariable(Enum):
    """The enum for climate variables"""

    rainfall = auto()
    temperature = auto()
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
    chirps = auto()
    tamsat = auto()


class AggregationLevel(Enum):
    """The enum for aggregation levels"""

    hourly = auto()
    daily = auto()
    monthly = auto()


class DataDownloadBase(ABC):
    """An abstract class for creating a standardised interface for downloading data"""

    def __init__(
        self,
        location_coord: tuple[float],
        aggregation: AggregationLevel,
        date_from_utc: date,
        date_to_utc: date,
    ):
        self.location_coord = location_coord
        self.aggregation = aggregation
        self.date_from_utc = date_from_utc
        self.date_to_utc = date_to_utc

    @abstractmethod
    def download_rainfall(self, settings: Optional[Settings]):
        """Retrieves rainfall data from the climate database"""
        pass

    @abstractmethod
    def download_temperature(self, settings: Optional[Settings]):
        """Retrieves temperature data from the climate database"""
        pass

    @abstractmethod
    def download_precipitation(self, settings: Optional[Settings]):
        """Retrieves precipitation data from the climate database"""
        pass

    @abstractmethod
    def download_windspeed(self, settings: Optional[Settings]):
        """Retrieves wind speed data from the climate database"""
        pass

    @abstractmethod
    def download_solar_radiation(self, settings: Optional[Settings]):
        """Retrieves solar radiation data from the climate database"""
        pass

    @abstractmethod
    def download_humidity(self, settings: Optional[Settings]):
        """Retrieves humidity data from the climate database"""
        pass

    @abstractmethod
    def download_soil_moisture(self, settings: Optional[Settings]):
        """Retrieves soil moisture data from the climate database"""
        pass
