"""Module for applying the DownloadData / SourceData class to download from
different climate databases."""

from agera_5 import DownloadData as DownloadAgera5
from configs.settings import Settings
from era_5 import DownloadData as DownloadEra5

settings = Settings.load()

download_agera_5 = DownloadAgera5(
    location_coord=None,
    variable=None,
    source=None,
    aggregation=None,
    date_from_utc=None,
    date_to_utc=None,
)

download_era_5 = DownloadEra5(
    location_coord=None,
    variable=None,
    source=None,
    aggregation=None,
    date_from_utc=None,
    date_to_utc=None,
)


if __name__ == "__main__":
    # download_agera_5.download_rainfall(settings=settings)
    # download_agera_5.download_temperature(settings=settings)
    download_era_5.download_pressure_levels(settings=settings)
