"""
Climate Data Fetching Orchestrator
Single entry point for the climate data pipeline:
    Source → Transform → Preprocess
Stages:
    raw          - download only (SourceData)
    transformed  - download + standardise column names (transform_data)
    preprocessed - download + standardise + clean/QC (preprocess_data) [default]
"""

import sys
import os
import argparse
from datetime import date

current_dir = os.path.dirname(__file__)
sys.path.append(os.path.join(current_dir, 'source_data'))
sys.path.append(os.path.join(current_dir, 'transform_data'))
sys.path.append(os.path.join(current_dir, 'preprocess_data'))

from source_data.source_data import SourceData
from transform_data.transform_data import (
    transform_data,
    validate_inputs,
    default_variables,
)
from preprocess_data.preprocess_data import preprocess_data
from sources.utils.models import ClimateDataset, ClimateVariable, SoilVariable
from sources.utils.settings import Settings

VALID_STAGES = ("raw", "transformed", "preprocessed")

def fetch_data(
    source,
    location_coord,
    variables=None,
    date_from=None,
    date_to=None,
    settings=None,
    model=None,
    scenario=None,
    stage="preprocessed",
):
    """Fetch climate data through the pipeline.
    Parameters
    ----------
    source : str
        Climate dataset name (e.g. 'era_5', 'chirps', 'nex_gddp').
    location_coord : tuple[float, float]
        (latitude, longitude).
    variables : list, optional
        ClimateVariable / SoilVariable enums. Defaults to a sensible set.
    date_from, date_to : date, optional
        Date range. Defaults to today.
    settings : Settings, optional
        Loaded settings. Auto-loaded if not provided.
    model, scenario : str, optional
        Required only for `nex_gddp`.
    stage : {'raw', 'transformed', 'preprocessed'}
        How far through the pipeline to run. Default 'preprocessed'.
    Returns
    -------
    pandas.DataFrame
    """
    if stage not in VALID_STAGES:
        raise ValueError(
            f"Invalid stage '{stage}'. Must be one of: {', '.join(VALID_STAGES)}"
        )
    settings = settings or Settings.load()
    variables = variables or default_variables()
    date_from = date_from or date.today()
    date_to = date_to or date.today()

    if stage == "raw":
        try:
            dataset = ClimateDataset[source]
        except KeyError:
            raise ValueError(f"Unknown source '{source}'")

        client = SourceData(
            location_coord=location_coord,
            variables=variables,
            source=dataset,
            date_from_utc=date_from,
            date_to_utc=date_to,
            settings=settings,
            model=model,
            scenario=scenario,
        )
        return client.download()

    if stage == "transformed":
        return transform_data(
            source=source,
            location_coord=location_coord,
            variables=variables,
            date_from=date_from,
            date_to=date_to,
            settings=settings,
            model=model,
            scenario=scenario,
        )
    # preprocessed (default)
    return preprocess_data(
        source=source,
        location_coord=location_coord,
        variables=variables,
        date_from=date_from,
        date_to=date_to,
        settings=settings,
        model=model,
        scenario=scenario,
    )

def save_output(data, output_path, fmt):
    if fmt == "csv":
        data.to_csv(output_path, index=False)
    elif fmt == "json":
        data.to_json(output_path, orient="records", date_format="iso", indent=2)
    else:
        raise ValueError(fmt)

def parse_variables(raw):
    """Parse a comma-separated --variables string into enum members."""
    if not raw:
        return None
    variables = []
    for v in raw.split(","):
        v = v.strip()
        if hasattr(ClimateVariable, v):
            variables.append(getattr(ClimateVariable, v))
        elif hasattr(SoilVariable, v):
            variables.append(getattr(SoilVariable, v))
        else:
            raise ValueError(f"Unknown variable '{v}'")
    return variables

def main():
    parser = argparse.ArgumentParser(
        description="Fetch climate data through the source → transform → preprocess pipeline"
    )
    parser.add_argument("--source", required=True)
    parser.add_argument("--lat", type=float, required=True)
    parser.add_argument("--lon", type=float, required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--model", default=None)
    parser.add_argument("--scenario", default=None)
    parser.add_argument(
        "--stage",
        choices=VALID_STAGES,
        default="preprocessed",
        help="Pipeline stage to return (default: preprocessed)",
    )
    parser.add_argument(
        "--variables",
        default=None,
        help="Comma-separated list; defaults to a standard set",
    )
    parser.add_argument("-o", "--output", default=None)
    parser.add_argument(
        "--format",
        choices=["csv", "json", "print"],
        default="print",
    )

    args = parser.parse_args()

    date_from = date.fromisoformat(args.start)
    date_to = date.fromisoformat(args.end)

    errors = validate_inputs(
        args.source, args.lat, args.lon, date_from, date_to,
        args.model, args.scenario,
    )
    if errors:
        print("\nInput validation failed:\n")
        for err in errors:
            print(f" - {err}")
        return 1

    try:
        variables = parse_variables(args.variables)
    except ValueError as e:
        print(f"Error: {e}")
        return 1

    df = fetch_data(
        source=args.source,
        location_coord=(args.lat, args.lon),
        variables=variables,
        date_from=date_from,
        date_to=date_to,
        model=args.model,
        scenario=args.scenario,
        stage=args.stage,
    )

    if args.format == "print" or not args.output:
        print(df)
    else:
        save_output(df, args.output, args.format)
        print(f"Saved to {args.output}")

    return 0

if __name__ == "__main__":
    sys.exit(main())

# Examples:
# Full pipeline (default, preprocessed):
# python climate_tookit/fetch_data/fetch_data.py --source era_5 --lat -1.286 --lon 36.817 --start 2020-01-01 --end 2020-03-05

# Stop at transformed stage:
# python climate_tookit/fetch_data/fetch_data.py --source chirps --lat -1.286 --lon 36.817 --start 2020-01-01 --end 2020-01-10 --stage transformed

# Raw download only:
# python climate_tookit/fetch_data/fetch_data.py --source chirps --lat -1.286 --lon 36.817 --start 2020-01-01 --end 2020-01-10 --stage raw

# NEX-GDDP with model/scenario, saved to CSV:
# python climate_tookit/fetch_data/fetch_data.py --source nex_gddp --lat -1.286 --lon 36.817 --start 2050-01-01 --end 2050-01-10 --model GFDL-ESM4 --scenario ssp245 --format csv --output nex_gddp_2050.csv

# With a custom variable list:
# python climate_tookit/fetch_data/fetch_data.py --source era_5 --lat -1.286 --lon 36.817 --start 2020-01-01 --end 2020-01-10 --variables precipitation,max_temperature,min_temperature