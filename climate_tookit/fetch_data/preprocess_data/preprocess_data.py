"""Climate Data Preprocessing Module

Pre-processes raw source data from climate databases into analysis-ready format.
Receives transformed data from transform_data module and applies cleaning, quality control,
bias correction, and other preprocessing operations.

Pipeline: Receive Transformed Data → Clean → Quality Control → Analysis-Ready Output
"""

import sys
import os
from datetime import date
import pandas as pd
import numpy as np

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'transform_data'))

from transform_data import transform_data
from sources.utils.models import ClimateVariable, ClimateDataset


def clean_climate_data(df: pd.DataFrame) -> pd.DataFrame:
    """Clean climate data: handle missing values, outliers, and data quality issues."""
    if df.empty:
        return df

    cleaned_df = df.copy()

    if 'date' in cleaned_df.columns:
        cleaned_df['date'] = pd.to_datetime(cleaned_df['date'])

    numeric_columns = cleaned_df.select_dtypes(include=[np.number]).columns

    for col in numeric_columns:
        if col == 'precipitation':
            cleaned_df[col] = cleaned_df[col].fillna(0)
        else:
            cleaned_df[col] = cleaned_df[col].ffill().bfill()

    for col in numeric_columns:
        if col != 'date':
            mean = cleaned_df[col].mean()
            std = cleaned_df[col].std()
            outlier_threshold = 3 * std

            cleaned_df[col] = np.where(
                abs(cleaned_df[col] - mean) > outlier_threshold,
                mean,
                cleaned_df[col]
            )

    return cleaned_df


def apply_unit_conversions(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """Apply necessary unit conversions for consistency."""
    if df.empty:
        return df

    converted_df = df.copy()

    if source in ['agera_5', 'era_5', 'nex_gddp']:
        temp_columns = [col for col in converted_df.columns if 'temperature' in col.lower()]
        for col in temp_columns:
            if col in converted_df.columns:
                if converted_df[col].mean() > 200:
                    converted_df[col] = converted_df[col] - 273.15
                    print(f"Converted {col} from Kelvin to Celsius")

    if 'precipitation' in converted_df.columns:
        if converted_df['precipitation'].max() < 1:
            converted_df['precipitation'] = converted_df['precipitation'] * 1000
            print("Converted precipitation from meters to millimeters")

    return converted_df


def quality_control_checks(df: pd.DataFrame) -> pd.DataFrame:
    """Perform quality control checks and flag suspicious data."""
    if df.empty:
        return df

    qc_df = df.copy()

    temp_columns = [col for col in qc_df.columns if 'temperature' in col]
    for col in temp_columns:
        if col in qc_df.columns:
            mask = (qc_df[col] < -50) | (qc_df[col] > 60)
            if mask.any():
                print(f"Warning: {mask.sum()} extreme {col} values detected")
                qc_df.loc[mask, col] = np.nan

    if 'precipitation' in qc_df.columns:
        small_negative = (qc_df['precipitation'] < 0) & (qc_df['precipitation'] > -0.01)
        qc_df.loc[small_negative, 'precipitation'] = 0

        large_negative = qc_df['precipitation'] <= -0.01
        qc_df.loc[large_negative, 'precipitation'] = np.nan

        extreme_precip = qc_df['precipitation'] > 500
        qc_df.loc[extreme_precip, 'precipitation'] = np.nan

    if 'wind_speed' in qc_df.columns:
        qc_df['wind_speed'] = qc_df['wind_speed'].abs()
        extreme_wind = qc_df['wind_speed'] > 50
        qc_df.loc[extreme_wind, 'wind_speed'] = np.nan

    if 'date' in qc_df.columns:
        qc_df = qc_df.sort_values('date').reset_index(drop=True)

    return qc_df


def preprocess_data(
    source: str,
    location_coord=None,
    variables=None,
    date_from=None,
    date_to=None,
    settings=None,
    transformed_data=None,
    model=None,
    scenario=None
) -> pd.DataFrame:
    """Preprocess climate data into analysis-ready format."""
    if transformed_data is not None:
        transformed_df = transformed_data
    else:
        transformed_df = transform_data(
            source=source,
            location_coord=location_coord,
            variables=variables,
            date_from=date_from,
            date_to=date_to,
            settings=settings,
            model=model,
            scenario=scenario
        )

    if transformed_df.empty:
        print("No data retrieved from source")
        return pd.DataFrame()

    data_columns = [col for col in transformed_df.columns if col != 'date']
    if not data_columns:
        print("ERROR: No data columns retrieved")
        return pd.DataFrame()

    cleaned_df = clean_climate_data(transformed_df)
    converted_df = apply_unit_conversions(cleaned_df, source)
    final_df = quality_control_checks(converted_df)

    return final_df


def save_output(data, output_path, fmt):
    if fmt == "csv":
        data.to_csv(output_path, index=False)
    elif fmt == "json":
        data.to_json(output_path, orient="records", date_format="iso", indent=2)
    else:
        raise ValueError(fmt)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Preprocess climate data for analysis")
    parser.add_argument("--source", required=True)
    parser.add_argument("--lon", type=float)
    parser.add_argument("--lat", type=float)
    parser.add_argument("--start", type=str)
    parser.add_argument("--end", type=str)
    parser.add_argument("--model", type=str)
    parser.add_argument("--scenario", type=str)
    parser.add_argument("-o", "--output", default=None)
    parser.add_argument(
        "--format",
        choices=["csv", "json", "print"],
        default="print"
    )

    args = parser.parse_args()

    location_coord = (args.lat, args.lon) if args.lon and args.lat else None
    date_from = date.fromisoformat(args.start) if args.start else None
    date_to = date.fromisoformat(args.end) if args.end else None

    df = preprocess_data(
        source=args.source,
        location_coord=location_coord,
        date_from=date_from,
        date_to=date_to,
        model=args.model,
        scenario=args.scenario
    )

    if args.format == "print" or not args.output:
        print(df)
    else:
        save_output(df, args.output, args.format)
        print(f"Saved to {args.output}")
 
        
# python climate_tookit/fetch_data/preprocess_data/preprocess_data.py --source era_5 --lon 36.8 --lat -1.3 --start 2020-01-01 --end 2020-03-05

# Download data in csv
# For NEX_GDDP
# python climate_tookit/fetch_data/preprocess_data/preprocess_data.py --source nex_gddp --lon 36.8 --lat -1.3 --start 2020-01-01 --end 2020-08-31 --model MRI-ESM2-0 --scenario ssp585 --format csv --output nex_gddp_preprocessed_jan-aug_2020.csv

# For other sources
# python climate_tookit/fetch_data/preprocess_data/preprocess_data.py --source era_5 --lon 36.8 --lat -1.3 --start 2020-01-01 --end 2020-03-05 --format csv --output era5_preprocessed.csv