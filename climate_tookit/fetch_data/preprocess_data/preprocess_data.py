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
        temp_columns = ['max_temperature', 'min_temperature']
        for col in temp_columns:
            if col in converted_df.columns:
                if converted_df[col].mean() > 200:
                    converted_df[col] = converted_df[col] - 273.15
                    print(f"Converted {col} from Kelvin to Celsius")

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

    if 'precipitation' in qc_df.columns:
        negative_precip = qc_df['precipitation'] < 0
        if negative_precip.any():
            print(f"Warning: {negative_precip.sum()} negative precipitation values detected")
            qc_df.loc[negative_precip, 'precipitation'] = 0

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
    """Preprocess climate data into analysis-ready format.

    Receives transformed data and applies cleaning, quality control, and preprocessing.
    Can also handle data fetching if transformed_data is not provided.

    Args:
        source: Data source name
        location_coord: (lat, lon) tuple
        variables: List of ClimateVariable enums
        date_from: Start date
        date_to: End date
        settings: Settings object
        transformed_data: Pre-transformed data (optional)
        model: GCM model name (for NEX-GDDP source)
        scenario: Climate scenario (for NEX-GDDP source)
    """
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
        return pd.DataFrame()

    print("Cleaning data...")
    cleaned_df = clean_climate_data(transformed_df)

    print("Applying unit conversions...")
    converted_df = apply_unit_conversions(cleaned_df, source)

    print("Performing quality control...")
    final_df = quality_control_checks(converted_df)

    print(f"Preprocessing complete: {len(final_df)} analysis-ready records")
    return final_df


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Preprocess climate data for analysis")
    parser.add_argument("--source", required=True, help="Source dataset key")
    parser.add_argument("--lon", type=float, help="Longitude")
    parser.add_argument("--lat", type=float, help="Latitude")
    parser.add_argument("--start", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", type=str, help="End date (YYYY-MM-DD)")
    parser.add_argument("--model", type=str, help="GCM model (for NEX-GDDP)")
    parser.add_argument("--scenario", type=str, help="Climate scenario (for NEX-GDDP)")
    args = parser.parse_args()

    location_coord = (args.lon, args.lat) if args.lon and args.lat else None
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

    if not df.empty:
        print(f"\nAnalysis-ready dataset: {len(df)} records")
        print("\nColumns:", list(df.columns))
        print("\nFirst few rows:")
        print(df)

        if 'max_temperature' in df.columns:
            temp_range = f"{df['max_temperature'].min():.1f}°C to {df['max_temperature'].max():.1f}°C"
            print(f"\nTemperature range: {temp_range}")
        if 'precipitation' in df.columns:
            print(f"Total precipitation: {df['precipitation'].sum():.1f}mm")
    else:
        print("No data was successfully preprocessed")
 
        
# python climate_tookit/fetch_data/preprocess_data/preprocess_data.py --source agera_5 --lon 36.8 --lat -1.3 --start 2023-01-01 --end 2023-01-30


# custom precision(2 decimal places)
# python climate_tookit/fetch_data/preprocess_data/preprocess_data.py --source agera_5 --lon 36.8 --lat -1.3 --start 2023-01-01 --end 2023-01-30 --decimal-places 2