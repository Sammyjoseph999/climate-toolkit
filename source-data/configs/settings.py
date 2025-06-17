"""This module contains settings and paths from the `source_data` module of the
climate toolkit."""

from pathlib import Path

import yaml
from pydantic import BaseModel
from pydantic_settings import BaseSettings

BASE_DIR = Path(__file__).parent.parent


class RequestSettings(BaseModel):
    """Corresponds to the 'request' block in YAML."""

    version: str
    data_format: str
    download_format: str


class Agera5Settings(BaseModel):
    """Corresponds to the 'agera_5' block in YAML."""

    dataset: str
    # request: RequestSettings
    request: dict


class Settings(BaseSettings):
    """The main settings class."""

    agera_5: Agera5Settings

    @classmethod
    def load(
        cls, settings_path: Path = Path(BASE_DIR / "configs/config.yaml")
    ) -> "Settings":
        with open(settings_path, mode="r") as f:
            settings = yaml.safe_load(f)

        return cls(**settings)


if __name__ == "__main__":
    print(Settings.load().agera_5)
    print(Settings.load().agera_5.dataset)
    print(Settings.load().agera_5.request)
    # print(Settings.load().agera_5.request.data_format)
