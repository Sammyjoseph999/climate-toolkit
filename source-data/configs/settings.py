"""This module contains settings and paths for the `source_data` module"""

import logging
from pathlib import Path

import yaml
from pydantic import BaseModel
from pydantic_settings import BaseSettings

BASE_DIR = Path(__file__).parent.parent


def set_logging():
    """Configure logging for the application"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(filename)s:%(lineno)d --- %(message)s",
    )


class Agera5Settings(BaseModel):
    """Corresponds to the 'agera_5' block in YAML."""

    dataset: str
    request: dict


class Era5Settings(BaseModel):
    request: dict


class Settings(BaseSettings):
    """The main settings class."""

    agera_5: Agera5Settings
    era_5: Era5Settings

    @classmethod
    def load(
        cls, settings_path: Path = Path(BASE_DIR / "configs/config.yaml")
    ):
        with open(settings_path, mode="r") as f:
            settings = yaml.safe_load(f)

        return cls(**settings)


if __name__ == "__main__":
    print(Settings.load().agera_5)
    print(Settings.load().agera_5.dataset)
    print(Settings.load().agera_5.request)
    # print(Settings.load().agera_5.request.data_format)
