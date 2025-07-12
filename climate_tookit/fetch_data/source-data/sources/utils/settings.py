"""This module contains settings and paths for the `source_data` module"""

import logging
from pathlib import Path

import yaml
from pydantic import BaseModel

BASE_DIR = Path(__file__).parent.parent.parent
config_path = "sources/utils/config.yaml"


def set_logging():
    """Configure logging for the application"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(filename)s:%(lineno)d --- %(message)s",
    )


class Cadence(BaseModel):
    monthly: str
    daily: str
    half_hourly: str


class ClimateVariable(BaseModel):
    precipitation: str | None
    max_temperature: str | None
    min_temperature: str | None
    wind_speed: str | None
    solar_radiation: str | None
    soil_moisture: str | None


class Agera5Settings(BaseModel):
    """Corresponds to the 'agera_5' block in YAML."""

    dataset: str
    request: dict


class Era5Settings(BaseModel):
    request: dict
    gee_image: str
    resolution: float
    variable: ClimateVariable
    cadence: str


class ImergSettings(BaseModel):
    version: str
    short_name: Cadence
    gee_image: str
    resolution: float
    variable: ClimateVariable
    cadence: str


class TerraSettings(BaseModel):
    url: str
    variable: ClimateVariable
    gee_image: str
    resolution: float
    cadence: str


class ChirtsSettings(BaseModel):
    gee_image: str
    resolution: float


class Settings(BaseModel):
    """Loads the application's settings."""

    agera_5: Agera5Settings
    era_5: Era5Settings
    imerg: ImergSettings
    terraclimate: TerraSettings
    chirts: ChirtsSettings

    @classmethod
    def load(cls, settings_path: Path = config_path):
        with open(settings_path, mode="r") as f:
            settings = yaml.safe_load(f)

        return cls(**settings)


if __name__ == "__main__":
    print(Settings.load().agera_5)
    print(Settings.load().agera_5.dataset)
    print(Settings.load().agera_5.request)
    print(Settings.load().imerg.short_name.monthly)
