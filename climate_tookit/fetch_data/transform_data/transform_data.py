import sys
import os
from datetime import date
import yaml

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "source_data"))

from source_data import SourceData
from sources.utils.models import ClimateVariable, ClimateDataset, SoilVariable
from sources.utils.settings import Settings


def load_yaml(path: str):
    with open(path, "r") as f:
        return yaml.safe_load(f)


def load_variable_mappings():
    yaml_path = os.path.join(os.path.dirname(__file__), "data_dictionary.yaml")
    return load_yaml(yaml_path)["source_mappings"]


def load_scaling_config(source: str, settings: Settings):
    data_settings = getattr(settings, source, None)
    if data_settings is None:
        return None
    return getattr(data_settings, "variable", None)


def apply_scaling(df, variable_config):
    if variable_config is None:
        return df

    for _, meta in variable_config.model_dump().items():
        if not meta:
            continue

        band = meta.get("band")
        scale = meta.get("scale", 1)

        if band and band in df.columns:
            df[band] = df[band] * scale

    return df


def default_variables():
    return [
        ClimateVariable.precipitation,
        ClimateVariable.max_temperature,
        ClimateVariable.min_temperature,
        ClimateVariable.solar_radiation,
        ClimateVariable.soil_moisture,
        ClimateVariable.wind_speed,
        ClimateVariable.humidity,
        SoilVariable.bulk_density,
        SoilVariable.cation_exchange_capacity,
        SoilVariable.clay_content,
        SoilVariable.coarse_fragments,
        SoilVariable.organic_carbon,
        SoilVariable.organic_carbon_stock,
        SoilVariable.ph,
        SoilVariable.sand_content,
        SoilVariable.silt_content,
        SoilVariable.soil_moisture,
    ]


def transform_data(
    source: str,
    location_coord=None,
    variables=None,
    date_from=None,
    date_to=None,
    settings=None,
    model=None,
    scenario=None,
):
    """Download and transform climate data using SourceData + variable mappings."""

    settings = settings or Settings.load()
    location_coord = location_coord or (0.0, 0.0)
    variables = variables or default_variables()
    date_from = date_from or date.today()
    date_to = date_to or date.today()

    src = SourceData(
        location_coord=location_coord,
        variables=variables,
        source=ClimateDataset[source],
        date_from_utc=date_from,
        date_to_utc=date_to,
        settings=settings,
        model=model,
        scenario=scenario,
    )

    raw_df = src.download()

    scaling_cfg = load_scaling_config(source, settings)
    raw_df = apply_scaling(raw_df, scaling_cfg)

    mappings = load_variable_mappings().get(source, {})
    if not mappings:
        raise ValueError(f"No variable mappings found for source '{source}'")

    return raw_df.rename(columns=mappings)


def save_output(data, output_path, fmt):
    if fmt == "csv":
        data.to_csv(output_path, index=False)
    elif fmt == "json":
        data.to_json(output_path, orient="records", date_format="iso", indent=2)
    else:
        raise ValueError(fmt)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--lon", type=float)
    parser.add_argument("--lat", type=float)
    parser.add_argument("--start", type=str)
    parser.add_argument("--end", type=str)
    parser.add_argument("--model", type=str)
    parser.add_argument("--scenario", type=str)
    parser.add_argument("-o", "--output", default=None)
    parser.add_argument("--format", choices=["csv", "json", "print"], default="print")

    args = parser.parse_args()

    location_coord = (args.lat, args.lon) if args.lon and args.lat else None
    date_from = date.fromisoformat(args.start) if args.start else None
    date_to = date.fromisoformat(args.end) if args.end else None

    df = transform_data(
        source=args.source,
        location_coord=location_coord,
        date_from=date_from,
        date_to=date_to,
        model=args.model,
        scenario=args.scenario,
    )

    if args.format == "print" or not args.output:
        print(df)
    else:
        save_output(df, args.output, args.format)
        print(f"Saved to {args.output}")
    
# python climate_tookit/fetch_data/transform_data/transform_data.py --source era_5 --lon 36.817223 --lat -1.286389 --start 2020-01-01 --end 2020-03-05

# python climate_tookit/fetch_data/transform_data/transform_data.py --source nex_gddp --lon 36.817223 --lat -1.286389 --start 2020-01-01 --end 2020-08-31 --model MRI-ESM2-0 --scenario ssp585

# Download data in csv
# For nex_gddp
# python climate_tookit/fetch_data/transform_data/transform_data.py --source nex_gddp --lon 36.817223 --lat -1.286389 --start 2020-01-01 --end 2020-08-31 --model MRI-ESM2-0 --scenario ssp585 --format csv --output nex_gddp_transformed_jan-aug_2020.csv

# For other sources
# python climate_tookit/fetch_data/transform_data/transform_data.py --source era_5 --lon 36.817223 --lat -1.286389 --start 2020-01-01 --end 2020-03-05 --format csv --output era5_transformed.csv