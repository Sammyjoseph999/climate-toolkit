"""This module contains settings and paths for the `source_data` module"""

import logging
from pathlib import Path
import os

import yaml
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).parent.parent.parent
config_path = "sources/utils/config.yaml"


def set_logging():
    """Configure logging for the application"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(filename)s:%(lineno)d --- %(message)s",
    )


class AggregationLevel(BaseModel):
    monthly: str
    daily: str
    half_hourly: str


class TerraClimateVariable(BaseModel):
    precipitation: str
    max_temperature: str
    min_temperature: str
    wind_speed: str
    solar_radiation: str
    soil_moisture: str


class Agera5Settings(BaseModel):
    """Corresponds to the 'agera_5' block in YAML."""

    dataset: str
    request: dict


class Era5Settings(BaseModel):
    request: dict


class ImergSettings(BaseModel):
    version: str
    short_name: AggregationLevel


class TerraSettings(BaseModel):
    url: str
    variable: TerraClimateVariable


class ChirpsSettings(BaseModel):
    base_url: str


class TamsatSettings(BaseModel):
    rainfall_url: str
    soil_moisture_url: str


class TamsatSettings(BaseModel):
    rainfall_url: str
    soil_moisture_url: str


class Settings(BaseModel):
    """Loads the application's settings."""

    agera_5: Agera5Settings
    era_5: Era5Settings
    imerg: ImergSettings
    terraclimate: TerraSettings
    chirps: ChirpsSettings
    tamsat: TamsatSettings

    gee_project_id: str = os.getenv("GEE_PROJECT_ID", "")

    @classmethod
    def load(cls, settings_path: Path = config_path):
        with open(settings_path, mode="r") as f:
            settings = yaml.safe_load(f)

        return cls(**settings)


if __name__ == "__main__":
    s = Settings.load()
    print(s.agera_5)
    print(s.agera_5.dataset)
    print(s.agera_5.request)
    print(s.imerg.short_name.monthly)
    print(s.chirps.base_url)
    print(s.tamsat.rainfall_url)
    print(s.tamsat.soil_moisture_url)
    print(s.gee_project_id)
