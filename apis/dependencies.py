import os
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.resolve()
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "climate_tookit" / "fetch_data" / "source_data"))

from dotenv import load_dotenv

load_dotenv()


def get_settings():
    from sources.utils.settings import Settings
    return Settings.load()