import sys
import os
from datetime import date
import yaml

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'source_data'))

from source_data import SourceData
from sources.utils.models import ClimateVariable, ClimateDataset
from sources.utils.settings import Settings


def load_variable_mappings():
    yaml_path = os.path.join(os.path.dirname(__file__), 'data_dictionary.yaml')
    with open(yaml_path, 'r') as file:
        data = yaml.safe_load(file)
    return data['source_mappings']


def transform_data(source: str, location_coord=None, variables=None, date_from=None, date_to=None, settings=None):
    if settings is None:
        settings = Settings.load()
    if location_coord is None:
        location_coord = (0.0, 0.0)
    if variables is None:
        variables = [
            ClimateVariable.precipitation,
            ClimateVariable.max_temperature,
            ClimateVariable.min_temperature,
            ClimateVariable.solar_radiation,
            ClimateVariable.soil_moisture,
            ClimateVariable.wind_speed,
            ClimateVariable.humidity,
        ]
    if date_from is None:
        date_from = date.today()
    if date_to is None:
        date_to = date.today()

    src = SourceData(
        location_coord=location_coord,
        variables=variables,
        source=ClimateDataset[source],
        date_from_utc=date_from,
        date_to_utc=date_to,
        settings=settings,
    )

    raw_df = src.download()

    SOURCE_VARIABLE_MAPPINGS = load_variable_mappings()
    mapping = SOURCE_VARIABLE_MAPPINGS.get(source, {})
    if not mapping:
        raise ValueError(f"No variable mappings found for source '{source}'")

    return raw_df.rename(columns=mapping)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--lon", type=float)
    parser.add_argument("--lat", type=float)
    parser.add_argument("--start", type=str)
    parser.add_argument("--end", type=str)
    args = parser.parse_args()

    location_coord = (args.lon, args.lat) if args.lon and args.lat else None
    date_from = date.fromisoformat(args.start) if args.start else None
    date_to = date.fromisoformat(args.end) if args.end else None

    df = transform_data(
        source=args.source,
        location_coord=location_coord,
        date_from=date_from,
        date_to=date_to,
    )

    print(df)
    
#python climate_tookit/fetch_data/transform_data/transform_data.py --source agera_5 --lon 36.817223 --lat -1.286389 --start 2020-01-01 --end 2020-03-05
