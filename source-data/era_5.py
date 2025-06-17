import cdsapi

cds_client = cdsapi.Client()


def download_pressure_levels() -> None:
    """Downloads ERA5 hourly data on pressure levels from 1940 to present

    ref: https://cds.climate.copernicus.eu/datasets/reanalysis-era5-pressure-levels?tab=download
    """

    dataset = "reanalysis-era5-pressure-levels"
    request = {
        "product_type": ["reanalysis"],
        "variable": ["geopotential"],
        "year": ["2024"],
        "month": ["03"],
        "day": ["01"],
        "time": ["13:00"],
        "pressure_level": ["1000"],
        "data_format": "grib",
        "download_format": "zip",  # optional
    }

    target = "pressure_levels.zip"

    cds_client.retrieve(dataset=dataset, request=request, target=target)


def download_rainfall():
    pass


def download_temperature():
    pass


def download_precipitation():
    pass


def download_windspeed():
    pass


def download_solar_radiation():
    pass


def download_soil_moisture():
    pass
