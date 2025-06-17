"""Module for defining base classes and enums"""

from abc import ABC, abstractmethod
from datetime import date
from enum import Enum, auto


class ClimateVariable(Enum):
    """The enum for climate variables"""

    rainfall = auto()
    temperature = auto()


class ClimateDataset(Enum):
    """The enum to represent climate datasets"""

    agera_5 = auto()
    era_5 = auto()


class AggregationLevel(Enum):
    """The enum for aggregation levels"""

    hourly = auto()
    daily = auto()
    monthly = auto()


class DownloadDataBase(ABC):
    """An abstract class for creating the downloading data in a standardised interface"""

    def __init__(
        self,
        location_coord: tuple[int],
        variable: ClimateVariable,
        source: ClimateDataset,
        aggregation: AggregationLevel,
        date_from_utc: date,
        date_to_utc: date,
    ):
        pass

    @abstractmethod
    def download_rainfall():
        """The parameters here can be flexible while reusing the ones initialised"""
        pass

    @abstractmethod
    def download_temperature():
        """The parameters here can be flexible while reusing the ones initialised"""
        pass
