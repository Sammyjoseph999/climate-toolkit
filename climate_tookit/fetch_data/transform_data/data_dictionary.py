"""Data dictionary module with source variable mappings."""

import sys
import os
 
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'source_data'))
 
from sources.utils.models import ClimateDataset

SOURCE_VARIABLE_MAPPINGS = {
    ClimateDataset.agera_5: {
        "Precipitation_Flux": "precipitation",
        "Temperature_Air_2m_Max_24h": "max_temperature",
        "Temperature_Air_2m_Min_24h": "min_temperature",
    },
    ClimateDataset.era_5: {
        "total_precipitation": "precipitation",
        "wind_speed": "wind_speed",
        "solar_radiation": "solar_radiation",
        "soil_moisture": "soil_moisture",
    },
    ClimateDataset.terraclimate: {
        "pr": "precipitation",
        "tmmx": "max_temperature",
        "tmmn": "min_temperature",
        "vs": "wind_speed",
        "srad": "solar_radiation",
        "soil": "soil_moisture",
    },
    ClimateDataset.imerg: {
        "precipitation": "precipitation",
    },
    ClimateDataset.chirts: {
        "maximum_temperature": "max_temperature",
        "minimum_temperature": "min_temperature",
        "relative_humidity": "humidity"
    },
    ClimateDataset.chirps: {
        "precipitation": "precipitation",
    },
    ClimateDataset.cmip6: {
        "pr": "precipitation",
        "tasmax": "max_temperature",
        "tasmin": "min_temperature",
    },
    ClimateDataset.nex_gddp: {
        "pr": "precipitation",
        "tasmax": "max_temperature",
        "tasmin": "min_temperature",
    },
    ClimateDataset.nasa_power: {
        "precipitation": "precipitation",
        "max_temperature": "max_temperature",
        "min_temperature": "min_temperature",
    },
    ClimateDataset.tamsat: {
        "rfe": "precipitation",
        "smcl": "soilmoisture",
    },
    ClimateDataset.soil_grid: {
        "ph": "soil_ph",
        "bulk_density": "soil_bulk_density",
        "clay_content": "soil_clay_content",
        "sand_content": "soil_sand_content",
        "organic_carbon": "soil_organic_carbon",
    },
}