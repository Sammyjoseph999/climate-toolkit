"""Module for applying the DownloadData / SourceData class to download from
different climate databases."""

if __name__ == "__main__":
    from agera_5 import DownloadData
    from configs.settings import Settings

    settings = Settings.load()

    download_agera_5 = DownloadData(
        location_coord=None,
        variable=None,
        source=None,
        aggregation=None,
        date_from_utc=None,
        date_to_utc=None,
    )

    download_agera_5.download_rainfall(settings=settings)
    # download_agera_5.download_temperature(settings=settings)
