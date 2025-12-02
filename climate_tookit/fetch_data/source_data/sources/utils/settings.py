"""This module contains settings and paths for the `source_data` module"""

import logging
from pathlib import Path

import yaml
from pydantic import BaseModel

BASE_DIR = Path(__file__).parent.parent.parent
config_path = Path(__file__).parent / "config.yaml"

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
    precipitation: str | None = None
    max_temperature: str | None = None
    min_temperature: str | None = None
    wind_speed: str | None = None
    solar_radiation: str | None = None
    soil_moisture: str | None = None

class SoilVariable(BaseModel):
    bulk_density: str | None = None
    clay_content: str | None = None
    ph: str | None = None
    sand_content: str | None = None
    silt_content: str | None = None
    organic_carbon: str | None = None
    organic_carbon_stock: str | None = None
    soil_moisture: str | None = None
    cation_exchange_capacity: str | None = None

class Agera5Settings(BaseModel):
    gee_image: str
    cadence: str
    variable: ClimateVariable
    resolution: float = 0.25


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
    cadence: str
    variable: ClimateVariable
    
class ChirpsSettings(BaseModel):
    gee_image: str
    resolution: float
    variable: ClimateVariable
    cadence: str

class Cmip6Settings(BaseModel):
    gee_image: str
    resolution: float
    cadence: str
    variable: ClimateVariable
    
class NexGddpSettings(BaseModel):
    gee_image: str
    resolution: float
    cadence: str
    variable: ClimateVariable
    
class NasaPowerSettings(BaseModel):
    endpoint: str
    parameters: list[str]
    temporal_api: str
    resolution: float
    variable: ClimateVariable
    cadence: str

class TamsatSettings(BaseModel):
    rainfall_url: str
    soil_moisture_url: str
    data_format: str
    download_format: str
    cadence: str
    resolution: float
    variable: ClimateVariable

class SoilGridSettings(BaseModel):
    # Support both single image (backward compatibility) and multiple images
    gee_image: str | None = None
    gee_images: dict[str, str] | None = None
    cadence: str
    resolution: float
    variable: SoilVariable
 
    @property
    def has_multiple_images(self) -> bool:
        """Check if this configuration uses multiple GEE images."""
        return self.gee_images is not None and len(self.gee_images) > 0
 
    def get_image_for_variable(self, variable_name: str) -> str | None:
        """Get the appropriate GEE image for a given variable."""
        if self.has_multiple_images:
            return self.gee_images.get(variable_name)
        return self.gee_image

class Settings(BaseModel):
    """Loads the application's settings."""

    agera_5: Agera5Settings
    era_5: Era5Settings
    imerg: ImergSettings
    terraclimate: TerraSettings
    chirts: ChirtsSettings
    chirps: ChirpsSettings
    cmip_6: Cmip6Settings
    nex_gddp: NexGddpSettings
    nasa_power: NasaPowerSettings
    tamsat: TamsatSettings
    soil_grid: SoilGridSettings

    @classmethod
    def load(cls, settings_path: Path = config_path):
        with open(settings_path, mode="r") as f:
            settings = yaml.safe_load(f)

        return cls(**settings)


if __name__ == "__main__":
    print(Settings.load().agera_5)
    print(Settings.load().imerg.short_name.monthly)
    print(Settings.load().chirps.variable)
    print(Settings.load().cmip_6.variable)
    print(Settings.load().nex_gddp.variable)
    print(Settings.load().nasa_power.variable)
    print(Settings.load().tamsat.rainfall_url)
    print(Settings.load().tamsat.soil_moisture_url)
    print(Settings.load().soil_grid)