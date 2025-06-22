"""Module for defining base classes and enums"""

from abc import ABC, abstractmethod
from datetime import date
from enum import Enum, auto
from typing import Optional

from .settings import Settings


class ClimateVariable(Enum):
    """The enum for climate variables"""

    rainfall = auto()
    temperature = auto()


class ClimateDataset(Enum):
    """The enum to represent climate datasets"""

    agera_5 = auto()
    era_5 = auto()
    terraclimate = auto()
    imerg = auto()
    chirps = auto()


class AggregationLevel(Enum):
    """The enum for aggregation levels"""

    hourly = auto()
    daily = auto()
    monthly = auto()


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
    def download_rainfall(settings: Optional[Settings]):
        """Retrieves rainfall data from the climate database"""
        # The parameters here can be flexible while reusing the ones initialised
        pass

    @abstractmethod
    def download_temperature(settings: Optional[Settings]):
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
