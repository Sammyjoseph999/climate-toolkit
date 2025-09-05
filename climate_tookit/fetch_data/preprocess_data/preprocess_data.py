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
            if std > 0:  
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
    
    # Temperature conversions (Kelvin to Celsius) - auto-detect for all sources
    temp_columns = ['max_temperature', 'min_temperature', 'temperature', 'mean_temperature']
    for col in temp_columns:
        if col in converted_df.columns:
            if converted_df[col].mean() > 200:
                converted_df[col] = converted_df[col] - 273.15
                print(f"Converted {col} from Kelvin to Celsius")

    # Precipitation conversions (to mm total)
    if 'precipitation' in converted_df.columns:
        if source == 'era_5':
            # ERA5: meters to mm
            converted_df['precipitation'] = converted_df['precipitation'] * 1000
            print("Converted precipitation from meters to mm")
        elif source in ['cmip6', 'nex_gddp']:
            # CMIP6/NEX-GDDP: kg/m²/s to mm/day
            converted_df['precipitation'] = converted_df['precipitation'] * 86400
            print(f"Converted precipitation from kg/m²/s to mm/day for {source}")
        elif source == 'imerg':
            # IMERG: mm/hr to mm/day
            converted_df['precipitation'] = converted_df['precipitation'] * 24
            print("Converted precipitation from mm/hr to mm/day for IMERG")
        else:
            # Auto-detect if values are in meters (very small values < 0.1)
            if converted_df['precipitation'].mean() < 0.1 and converted_df['precipitation'].mean() > 0:
                converted_df['precipitation'] = converted_df['precipitation'] * 1000
                print(f"Converted precipitation from meters to mm for {source}")

    # Solar radiation conversions (W/m² to MJ/m²/day)
    if 'solar_radiation' in converted_df.columns:
        if source in ['terraclimate', 'cmip6', 'nex_gddp']:
            # Check if values are in W/m² range (typically 0-1400)
            if converted_df['solar_radiation'].max() > 50:  # Likely W/m²
                converted_df['solar_radiation'] = converted_df['solar_radiation'] * 0.0864
                print("Converted solar radiation from W/m² to MJ/m²/day")

    if 'wind_speed' in converted_df.columns:
        pass

    # Humidity conversions
    if 'humidity' in converted_df.columns:
        if converted_df['humidity'].max() <= 1.0:
            converted_df['humidity'] = converted_df['humidity'] * 100
            print("Converted humidity from fraction to percentage")
    
    return converted_df

def quality_control_checks(df: pd.DataFrame) -> pd.DataFrame:
    """Perform quality control checks and flag suspicious data."""
    if df.empty:
        return df
    
    qc_df = df.copy()
    
    # Temperature validation (-60°C to 60°C for global climate data)
    temp_columns = [col for col in qc_df.columns if 'temperature' in col]
    for col in temp_columns:
        if col in qc_df.columns:
            mask = (qc_df[col] < -60) | (qc_df[col] > 60)
            if mask.any():
                print(f"Warning: {mask.sum()} extreme {col} values detected (outside -60°C to 60°C)")
                # Cap extreme values instead of flagging all as outliers
                qc_df.loc[qc_df[col] < -60, col] = -60
                qc_df.loc[qc_df[col] > 60, col] = 60
    
    # Precipitation validation (non-negative, reasonable upper bound)
    if 'precipitation' in qc_df.columns:
        negative_precip = qc_df['precipitation'] < 0
        if negative_precip.any():
            print(f"Warning: {negative_precip.sum()} negative precipitation values detected - setting to 0")
            qc_df.loc[negative_precip, 'precipitation'] = 0
        
        # Flag extremely high daily precipitation (> 500mm/day)
        extreme_precip = qc_df['precipitation'] > 500
        if extreme_precip.any():
            print(f"Warning: {extreme_precip.sum()} extremely high precipitation values detected (> 500mm)")

    # Solar radiation validation (0 to reasonable maximum)
    if 'solar_radiation' in qc_df.columns:
        negative_solar = qc_df['solar_radiation'] < 0
        if negative_solar.any():
            print(f"Warning: {negative_solar.sum()} negative solar radiation values detected - setting to 0")
            qc_df.loc[negative_solar, 'solar_radiation'] = 0
        
        # Flag extremely high solar radiation (> 50 MJ/m²/day)
        extreme_solar = qc_df['solar_radiation'] > 50
        if extreme_solar.any():
            print(f"Warning: {extreme_solar.sum()} extremely high solar radiation values detected")

    # Wind speed validation (0 to reasonable maximum)
    if 'wind_speed' in qc_df.columns:
        negative_wind = qc_df['wind_speed'] < 0
        if negative_wind.any():
            print(f"Warning: {negative_wind.sum()} negative wind speed values detected - setting to 0")
            qc_df.loc[negative_wind, 'wind_speed'] = 0
        
        extreme_wind = qc_df['wind_speed'] > 100
        if extreme_wind.any():
            print(f"Warning: {extreme_wind.sum()} extremely high wind speed values detected (> 100 m/s)")

    # Humidity validation (0-100%)
    if 'humidity' in qc_df.columns:
        qc_df.loc[qc_df['humidity'] < 0, 'humidity'] = 0
        qc_df.loc[qc_df['humidity'] > 100, 'humidity'] = 100
    
    # Sort by date for proper time series
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
    transformed_data=None
) -> pd.DataFrame:
    """Preprocess climate data into analysis-ready format.
    
    Receives transformed data and applies cleaning, quality control, and preprocessing.
    Can also handle data fetching if transformed_data is not provided.
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
            settings=settings
        )
    
    if transformed_df.empty:
        print("No data available for preprocessing")
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
    args = parser.parse_args()

    location_coord = (args.lon, args.lat) if args.lon and args.lat else None
    date_from = date.fromisoformat(args.start) if args.start else None
    date_to = date.fromisoformat(args.end) if args.end else None

    df = preprocess_data(
        source=args.source,
        location_coord=location_coord,
        date_from=date_from,
        date_to=date_to,
    )

    if not df.empty:
        print(f"\nAnalysis-ready dataset: {len(df)} records")
        print("\nColumns:", list(df.columns))
        print("\nFirst few rows:")
        print(df)
        
    else:
        print("No data was successfully preprocessed")
 
        
# python climate_tookit/fetch_data/preprocess_data/preprocess_data.py --source agera_5 --lon 36.8 --lat -1.3 --start 2023-01-01 --end 2023-01-30