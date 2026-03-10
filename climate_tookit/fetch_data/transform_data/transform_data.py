import sys
import os
from datetime import date
import yaml

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "source_data"))

from source_data import SourceData
from sources.utils.models import ClimateVariable, ClimateDataset, SoilVariable
from sources.utils.settings import Settings

def validate_coordinates(lat, lon):
    """Validate latitude and longitude ranges."""
    errors = []
    if lat is None or lon is None:
        errors.append("Latitude and Longitude must both be provided.")
        return errors
    if not (-180 <= lon <= 180):
        errors.append(f"Longitude must be between -180 and 180, got {lon}")
    if not (-90 <= lat <= 90):
        errors.append(f"Latitude must be between -90 and 90, got {lat}")
    return errors

def validate_inputs(source, lat, lon, date_from, date_to, model, scenario):
    """Validate all user inputs and return a list of errors."""
    errors = []
    errors.extend(validate_coordinates(lat, lon))
    valid_sources = [s.name for s in ClimateDataset]
    if source not in valid_sources:
        errors.append(
            f"Invalid source '{source}'. Valid sources: {', '.join(valid_sources)}"
        )
    if date_from and date_to and date_from > date_to:
        errors.append("Start date must be before end date")
    if source == "nex_gddp":
        valid_models = ['ACCESS-CM2','ACCESS-ESM1-5','CanESM5','CMCC-ESM2','EC-Earth3','EC-Earth3-Veg-LR','GFDL-ESM4','INM-CM4-8',
                        'INM-CM5-0','KACE-1-0-G','MIROC6','MPI-ESM1-2-LR','MRI-ESM2-0','NorESM2-LM','NorESM2-MM','TaiESM1']
        valid_scenarios = ["ssp126", "ssp245", "ssp585"]
        if model and model not in valid_models:
            errors.append(
                f"Invalid model '{model}'. Valid models: {', '.join(valid_models)}"
            )
        if scenario and scenario not in valid_scenarios:
            errors.append(
                f"Invalid scenario '{scenario}'. Valid scenarios: {', '.join(valid_scenarios)}"
            )
    return errors


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
    if location_coord is None:
        raise ValueError("location_coord must be provided as (lat, lon)")
    
    lat, lon = location_coord
    coord_errors = validate_coordinates(lat, lon)

    if coord_errors:
        raise ValueError(" | ".join(coord_errors))
    variables = variables or default_variables()
    date_from = date_from or date.today()
    date_to = date_to or date.today()

    try:
        dataset = ClimateDataset[source]
    except KeyError:
        raise ValueError(f"Unknown source '{source}'")

    src = SourceData(
        location_coord=location_coord,
        variables=variables,
        source=dataset,
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

    if args.lat is None or args.lon is None:
        print("Error: Both --lat and --lon must be provided")
        sys.exit(1)

    location_coord = (args.lat, args.lon)

    date_from = date.fromisoformat(args.start) if args.start else None
    date_to = date.fromisoformat(args.end) if args.end else None

    errors = validate_inputs(
        args.source,
        args.lat,
        args.lon,
        date_from,
        date_to,
        args.model,
        args.scenario,
    )

    if errors:
        print("\nInput validation failed:\n")
        for err in errors:
            print(f" - {err}")
        sys.exit(1)

    location_coord = (args.lat, args.lon)
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