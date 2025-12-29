"""
Climate Statistics Module

Calculates comprehensive climate statistics by season including summary statistics
for all climate variables and daily water balance calculations.

Uses the existing preprocessing pipeline for data consistency.

Dependencies: pandas
"""

import sys
import os
from datetime import datetime, date
import pandas as pd
import math
import json
import argparse
from typing import Tuple, Dict, List, Any

current_dir = os.path.dirname(__file__)
parent_dir = os.path.dirname(current_dir)
sys.path.append(os.path.join(parent_dir, 'fetch_data', 'preprocess_data'))
sys.path.append(os.path.join(parent_dir, 'season_analysis'))

try:
    from preprocess_data import preprocess_data
    from sources.utils.models import ClimateVariable, ClimateDataset
    PREPROCESS_AVAILABLE = True
except ImportError:
    PREPROCESS_AVAILABLE = False
    print("Warning: Preprocessing pipeline not available")

try:
    from seasons import detect_seasons as season_detect_seasons
    SEASON_ANALYSIS_AVAILABLE = True
except ImportError:
    SEASON_ANALYSIS_AVAILABLE = False
    print("Warning: Season analysis module not available")

def get_climate_data(lat: float, lon: float, start_date: str, end_date: str,
                   source: str) -> pd.DataFrame:
    """
    Fetch daily climate data using preprocessing pipeline.

    Args:
        lat (float): Latitude in decimal degrees
        lon (float): Longitude in decimal degrees
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
        source (str): Data source key

    Returns:
        pd.DataFrame: DataFrame with columns [date, tmax, tmin, precip]
    """
    if not PREPROCESS_AVAILABLE:
        raise Exception("Preprocessing pipeline not available - this module requires preprocess_data endpoint")

    date_from = date.fromisoformat(start_date)
    date_to = date.fromisoformat(end_date)

    try:
        variables = [
            ClimateVariable.precipitation,
            ClimateVariable.max_temperature,
            ClimateVariable.min_temperature,
            ClimateVariable.humidity,
            ClimateVariable.soil_moisture,
            ClimateVariable.solar_radiation,
            ClimateVariable.wind_speed,
        ]
    except (NameError, AttributeError):
        variables = ['precipitation', 'max_temperature', 'min_temperature']

    df = preprocess_data(
        source=source,
        location_coord=(lat, lon),
        variables=variables,
        date_from=date_from,
        date_to=date_to
    )

    print(f"Raw columns from preprocessing: {list(df.columns)}")
    print(f"Data shape: {df.shape}")
    print(f"Summary Statistics:\n{df.describe()}")

    # Handle the case where preprocessing returns limited data
    if df.empty or len(df.columns) <= 1:
        raise Exception(f"No usable climate data returned from {source}. Check source configuration.")

    # Map to expected column names with flexible handling
    column_mapping = {
        'max_temperature': 'tmax',
        'min_temperature': 'tmin',
        'temperature_max': 'tmax',
        'temperature_min': 'tmin',
        'tmax': 'tmax',
        'tmin': 'tmin'
    }

    precip_columns = ['precipitation', 'precip', 'rainfall', 'rain']

    # Handle precipitation
    precip_found = False
    for col in precip_columns:
        if col in df.columns:
            df['precip'] = df[col]
            precip_found = True
            break

    if not precip_found:
        print(f"Warning: No precipitation data found in {source}. Setting default values.")
        df['precip'] = 0.0

    # Handle temperature
    temp_found = {'tmax': False, 'tmin': False}
    for old_col, new_col in column_mapping.items():
        if old_col in df.columns:
            df[new_col] = df[old_col]
            temp_found[new_col] = True

    if not (temp_found['tmax'] and temp_found['tmin']):
        # For precipitation-only sources like CHIRPS, use reasonable default temperatures
        if source.lower() == 'chirps' and precip_found:
            print(f"Warning: {source} provides precipitation only. Using default temperature values for ET0 calculation.")
            df['tmax'] = 25.0  # Default max temperature for tropical location
            df['tmin'] = 15.0  # Default min temperature
            temp_found = {'tmax': True, 'tmin': True}
        else:
            # Available columns
            available_cols = [col for col in df.columns if col != 'date']
            raise Exception(f"Temperature data not available from {source}. Available columns: {available_cols}")

    print(f"Final columns after mapping: {list(df.columns)}")
    return df

def calculate_et0(tmin: float, tmax: float, lat: float, date: datetime) -> float:
    """Calculate reference evapotranspiration using Hargreaves method."""
    if tmax is None or tmin is None or tmax < tmin:
        return 0

    J = date.timetuple().tm_yday
    lat_rad = lat * math.pi / 180.0
    sol_decl = 0.409 * math.sin((2 * math.pi / 365) * J - 1.39)
    ird = 1 + 0.033 * math.cos((2 * math.pi / 365) * J)

    val = max(min(-math.tan(lat_rad) * math.tan(sol_decl), 1), -1)
    sha = math.acos(val)

    Gsc = 0.0820
    ra = ((24 * 60) / math.pi) * Gsc * ird * (
        sha * math.sin(lat_rad) * math.sin(sol_decl) +
        math.cos(lat_rad) * math.cos(sol_decl) * math.sin(sha)
    )

    Tmean = (tmax + tmin) / 2
    return 0.0023 * math.sqrt(tmax - tmin) * (Tmean + 17.8) * ra

def detect_seasons(df: pd.DataFrame, gap_days: int = 30, min_season_days: int = 30) -> List[Dict]:
    """Detect growing seasons based on precipitation exceeding ET0 threshold."""
    seasons = []
    df['threshold'] = df['et0'] * 0.5
    df['rainy_day'] = df['precip'] >= df['threshold']

    i, n = 0, len(df)

    while i < n:
        if df.iloc[i]['rainy_day']:
            onset_date = df.iloc[i]['date']
            dry_counter, j, cessation_date = 0, i + 1, None

            while j < n:
                if not df.iloc[j]['rainy_day']:
                    dry_counter += 1
                    if dry_counter >= gap_days:
                        cessation_date = df.iloc[j - gap_days]['date']
                        break
                else:
                    dry_counter = 0
                j += 1

            if cessation_date is None:
                cessation_date = df.iloc[-1]['date']

            season_length = (cessation_date - onset_date).days + 1

            if season_length >= min_season_days:
                seasons.append({
                    'onset_date': onset_date,
                    'cessation_date': cessation_date,
                    'onset_doy': onset_date.timetuple().tm_yday,
                    'cessation_doy': cessation_date.timetuple().tm_yday,
                    'length_days': season_length
                })

            if cessation_date:
                cessation_idx = df[df['date'] == cessation_date].index[0]
                i = cessation_idx + 1
            else:
                break
        else:
            i += 1

    return seasons

def calculate_water_balance(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate daily water balance components.

    Water Balance = Precipitation - ET0
    Cumulative Water Balance = Running sum of daily water balance
    """
    df = df.copy()
    df['water_balance'] = df['precip'] - df['et0']
    df['cumulative_balance'] = df['water_balance'].cumsum()
    df['water_stress'] = df['water_balance'] < 0
    return df

def calculate_season_statistics(df: pd.DataFrame, seasons: List[Dict]) -> Dict[str, Any]:
    """
    Calculate comprehensive statistics for each season and overall period.

    Args:
        df (pd.DataFrame): Climate data with all calculated variables
        seasons (List[Dict]): Detected seasons

    Returns:
        Dict[str, Any]: Complete statistics by season and overall
    """
    overall_stats = {
        'total_days': len(df),
        'precipitation': {
            'total_mm': df['precip'].sum(),
            'mean_daily': df['precip'].mean(),
            'median_daily': df['precip'].median(),
            'std_daily': df['precip'].std(),
            'max_daily': df['precip'].max(),
            'rainy_days': (df['precip'] > 1.0).sum(),
            'dry_days': (df['precip'] <= 1.0).sum()
        },
        'temperature': {
            'mean_tmax': df['tmax'].mean(),
            'mean_tmin': df['tmin'].mean(),
            'mean_tavg': ((df['tmax'] + df['tmin']) / 2).mean(),
            'max_tmax': df['tmax'].max(),
            'min_tmin': df['tmin'].min(),
            'diurnal_range': (df['tmax'] - df['tmin']).mean()
        },
        'et0': {
            'total_mm': df['et0'].sum(),
            'mean_daily': df['et0'].mean(),
            'median_daily': df['et0'].median(),
            'std_daily': df['et0'].std(),
            'max_daily': df['et0'].max(),
            'min_daily': df['et0'].min()
        },
        'water_balance': {
            'total_balance': df['water_balance'].sum(),
            'mean_daily': df['water_balance'].mean(),
            'cumulative_final': df['cumulative_balance'].iloc[-1],
            'deficit_days': (df['water_balance'] < 0).sum(),
            'surplus_days': (df['water_balance'] > 0).sum(),
            'max_deficit': df['water_balance'].min(),
            'max_surplus': df['water_balance'].max()
        }
    }

    season_stats = []

    for i, season in enumerate(seasons, 1):
        season_df = df[
            (df['date'] >= season['onset_date']) &
            (df['date'] <= season['cessation_date'])
        ].copy()

        if len(season_df) == 0:
            continue

        stats = {
            'season_number': i,
            'onset_date': season['onset_date'].strftime('%Y-%m-%d'),
            'cessation_date': season['cessation_date'].strftime('%Y-%m-%d'),
            'length_days': season['length_days'],
            'precipitation': {
                'total_mm': season_df['precip'].sum(),
                'mean_daily': season_df['precip'].mean(),
                'median_daily': season_df['precip'].median(),
                'std_daily': season_df['precip'].std(),
                'max_daily': season_df['precip'].max(),
                'rainy_days': (season_df['precip'] > 1.0).sum(),
                'dry_days': (season_df['precip'] <= 1.0).sum(),
                'intensity': season_df['precip'].sum() / len(season_df)
            },
            'temperature': {
                'mean_tmax': season_df['tmax'].mean(),
                'mean_tmin': season_df['tmin'].mean(),
                'mean_tavg': ((season_df['tmax'] + season_df['tmin']) / 2).mean(),
                'max_tmax': season_df['tmax'].max(),
                'min_tmin': season_df['tmin'].min(),
                'diurnal_range': (season_df['tmax'] - season_df['tmin']).mean()
            },
            'et0': {
                'total_mm': season_df['et0'].sum(),
                'mean_daily': season_df['et0'].mean(),
                'median_daily': season_df['et0'].median(),
                'max_daily': season_df['et0'].max(),
                'min_daily': season_df['et0'].min()
            },
            'water_balance': {
                'total_balance': season_df['water_balance'].sum(),
                'mean_daily': season_df['water_balance'].mean(),
                'deficit_days': (season_df['water_balance'] < 0).sum(),
                'surplus_days': (season_df['water_balance'] > 0).sum(),
                'max_deficit': season_df['water_balance'].min(),
                'max_surplus': season_df['water_balance'].max(),
                'water_stress_ratio': (season_df['water_balance'] < 0).mean()
            }
        }

        season_stats.append(stats)

    return {
        'overall_statistics': overall_stats,
        'season_statistics': season_stats,
        'seasons_detected': len(seasons)
    }

def analyze_climate_statistics(location_coord: Tuple[float, float],
                             date_range: Tuple[str, str],
                             source: str,
                             gap_days: int = 30,
                             min_season_days: int = 30) -> Dict[str, Any]:
    """
    Main function to analyze climate statistics by season using season analysis module.

    Args:
        location_coord (Tuple[float, float]): (latitude, longitude)
        date_range (Tuple[str, str]): (start_date, end_date)
        source (str): Data source key
        gap_days (int): Consecutive dry days to end season
        min_season_days (int): Minimum season length

    Returns:
        Dict[str, Any]: Complete climate statistics analysis by detected seasons
    """
    lat, lon = location_coord
    start_date, end_date = date_range

    try:
        df = get_climate_data(lat, lon, start_date, end_date,source)

        # ET0 for season detection and water balance
        et0_values = [calculate_et0(row['tmin'], row['tmax'], lat, row['date'])
                     for _, row in df.iterrows()]
        df['et0'] = et0_values

        # Daily water balance
        df = calculate_water_balance(df)

        if SEASON_ANALYSIS_AVAILABLE:
            seasons = detect_seasons(df, gap_days, min_season_days)
        else:
            seasons = detect_seasons(df, gap_days, min_season_days)

        # Calculate comprehensive statistics by season
        statistics = calculate_comprehensive_season_statistics(df, seasons)

        result = {
            'location': {'lat': lat, 'lon': lon},
            'date_range': {'start': start_date, 'end': end_date},
            'source': source,
            'analysis_parameters': {
                'gap_days': gap_days,
                'min_season_days': min_season_days
            },
            'season_analysis_method': 'integrated_season_detection' if SEASON_ANALYSIS_AVAILABLE else 'local_detection',
            'statistics': statistics,
            'methodology': 'preprocess_data_with_season_analysis_and_water_balance',
            'analysis_date': datetime.now().isoformat()
        }

        return result

    except Exception as e:
        return {
            'error': str(e),
            'location': {'lat': lat, 'lon': lon},
            'date_range': {'start': start_date, 'end': end_date},
            'source': source
        }

def calculate_comprehensive_season_statistics(df: pd.DataFrame, seasons: List[Dict]) -> Dict[str, Any]:
    """
    Calculate comprehensive statistics by season for all variables with daily water balance.

    Args:
        df (pd.DataFrame): Climate data with all calculated variables
        seasons (List[Dict]): Detected seasons from season analysis

    Returns:
        Dict[str, Any]: Comprehensive statistics by season and overall
    """
    # Overall statistics
    overall_stats = {
        'total_days': len(df),
        'precipitation': {
            'total_mm': df['precip'].sum(),
            'mean_daily': df['precip'].mean(),
            'median_daily': df['precip'].median(),
            'std_daily': df['precip'].std(),
            'max_daily': df['precip'].max(),
            'rainy_days': (df['precip'] > 1.0).sum(),
            'dry_days': (df['precip'] <= 1.0).sum()
        },
        'temperature': {
            'mean_tmax': df['tmax'].mean(),
            'mean_tmin': df['tmin'].mean(),
            'mean_tavg': ((df['tmax'] + df['tmin']) / 2).mean(),
            'max_tmax': df['tmax'].max(),
            'min_tmin': df['tmin'].min(),
            'diurnal_range': (df['tmax'] - df['tmin']).mean()
        },
        'et0': {
            'total_mm': df['et0'].sum(),
            'mean_daily': df['et0'].mean(),
            'median_daily': df['et0'].median(),
            'std_daily': df['et0'].std(),
            'max_daily': df['et0'].max(),
            'min_daily': df['et0'].min()
        },
        'water_balance': {
            'total_balance': df['water_balance'].sum(),
            'mean_daily': df['water_balance'].mean(),
            'cumulative_final': df['cumulative_balance'].iloc[-1],
            'deficit_days': (df['water_balance'] < 0).sum(),
            'surplus_days': (df['water_balance'] > 0).sum(),
            'max_deficit': df['water_balance'].min(),
            'max_surplus': df['water_balance'].max()
        }
    }

    # Enhanced season statistics with daily water balance analysis
    season_stats = []

    for i, season in enumerate(seasons, 1):
        # Filter data for specific detected season
        season_df = df[
            (df['date'] >= season['onset_date']) &
            (df['date'] <= season['cessation_date'])
        ].copy()

        if len(season_df) == 0:
            continue

        # Daily water balance season statistics
        daily_water_balance = []
        for _, row in season_df.iterrows():
            daily_wb = {
                'date': row['date'].strftime('%Y-%m-%d'),
                'precipitation': row['precip'],
                'et0': row['et0'],
                'water_balance': row['water_balance'],
                'cumulative_balance': row['cumulative_balance'],
                'water_stress': row['water_stress']
            }
            daily_water_balance.append(daily_wb)

        # Comprehensive season statistics
        season_stat = {
            'season_number': i,
            'onset_date': season['onset_date'].strftime('%Y-%m-%d'),
            'cessation_date': season['cessation_date'].strftime('%Y-%m-%d'),
            'length_days': season['length_days'],

            # All climate variables statistics
            'precipitation': {
                'total_mm': season_df['precip'].sum(),
                'mean_daily': season_df['precip'].mean(),
                'median_daily': season_df['precip'].median(),
                'std_daily': season_df['precip'].std(),
                'max_daily': season_df['precip'].max(),
                'min_daily': season_df['precip'].min(),
                'rainy_days': (season_df['precip'] > 1.0).sum(),
                'dry_days': (season_df['precip'] <= 1.0).sum(),
                'intensity': season_df['precip'].sum() / len(season_df)
            },
            'temperature': {
                'mean_tmax': season_df['tmax'].mean(),
                'mean_tmin': season_df['tmin'].mean(),
                'mean_tavg': ((season_df['tmax'] + season_df['tmin']) / 2).mean(),
                'max_tmax': season_df['tmax'].max(),
                'min_tmin': season_df['tmin'].min(),
                'std_tmax': season_df['tmax'].std(),
                'std_tmin': season_df['tmin'].std(),
                'diurnal_range': (season_df['tmax'] - season_df['tmin']).mean()
            },
            'et0': {
                'total_mm': season_df['et0'].sum(),
                'mean_daily': season_df['et0'].mean(),
                'median_daily': season_df['et0'].median(),
                'std_daily': season_df['et0'].std(),
                'max_daily': season_df['et0'].max(),
                'min_daily': season_df['et0'].min()
            },
            'water_balance': {
                'total_balance': season_df['water_balance'].sum(),
                'mean_daily': season_df['water_balance'].mean(),
                'deficit_days': (season_df['water_balance'] < 0).sum(),
                'surplus_days': (season_df['water_balance'] > 0).sum(),
                'max_deficit': season_df['water_balance'].min(),
                'max_surplus': season_df['water_balance'].max(),
                'water_stress_ratio': (season_df['water_balance'] < 0).mean(),
                'cumulative_start': season_df['cumulative_balance'].iloc[0],
                'cumulative_end': season_df['cumulative_balance'].iloc[-1]
            },

            # Daily water balance for detailed analysis
            'daily_water_balance': daily_water_balance[:10] if len(daily_water_balance) > 10 else daily_water_balance,
            'daily_records_total': len(daily_water_balance)
        }

        season_stats.append(season_stat)

    return {
        'overall_statistics': overall_stats,
        'season_statistics': season_stats,
        'seasons_detected': len(seasons)
    }

def format_as_dataframes(result: Dict[str, Any]) -> None:
    """
    Format and display results as pandas DataFrames.

    Args:
        result (Dict[str, Any]): Analysis results from analyze_climate_statistics
    """
    if 'error' in result:
        print(f"Error: {result['error']}")
        return

    stats = result['statistics']

    print("=== OVERALL STATISTICS ===")
    overall_data = []
    overall = stats['overall_statistics']

    for key, value in overall['precipitation'].items():
        overall_data.append({'Variable': 'Precipitation', 'Metric': key, 'Value': value, 'Unit': 'mm' if 'mm' in key or key in ['total_mm', 'mean_daily', 'median_daily', 'std_daily', 'max_daily'] else 'days'})

    for key, value in overall['temperature'].items():
        overall_data.append({'Variable': 'Temperature', 'Metric': key, 'Value': value, 'Unit': '°C'})

    for key, value in overall['et0'].items():
        overall_data.append({'Variable': 'ET0', 'Metric': key, 'Value': value, 'Unit': 'mm' if 'mm' in key or key.endswith('daily') else ''})

    for key, value in overall['water_balance'].items():
        unit = 'mm' if key in ['total_balance', 'mean_daily', 'cumulative_final', 'max_deficit', 'max_surplus'] else 'days'
        overall_data.append({'Variable': 'Water Balance', 'Metric': key, 'Value': value, 'Unit': unit})

    overall_df = pd.DataFrame(overall_data)
    print(overall_df.to_string(index=False))
    print()

    print("=== SEASON STATISTICS ===")
    if stats['season_statistics']:
        season_data = []

        for season in stats['season_statistics']:
            base_info = {
                'Season': season['season_number'],
                'Onset': season['onset_date'],
                'Cessation': season['cessation_date'],
                'Length_Days': season['length_days']
            }

            precip_row = base_info.copy()
            precip_row.update({
                'Variable': 'Precipitation',
                'Total_mm': season['precipitation']['total_mm'],
                'Mean_Daily': season['precipitation']['mean_daily'],
                'Max_Daily': season['precipitation']['max_daily'],
                'Rainy_Days': season['precipitation']['rainy_days'],
                'Intensity': season['precipitation']['intensity']
            })
            season_data.append(precip_row)

            temp_row = base_info.copy()
            temp_row.update({
                'Variable': 'Temperature',
                'Mean_Tmax': season['temperature']['mean_tmax'],
                'Mean_Tmin': season['temperature']['mean_tmin'],
                'Mean_Tavg': season['temperature']['mean_tavg'],
                'Max_Tmax': season['temperature']['max_tmax'],
                'Min_Tmin': season['temperature']['min_tmin']
            })
            season_data.append(temp_row)

            et0_row = base_info.copy()
            et0_row.update({
                'Variable': 'ET0',
                'Total_mm': season['et0']['total_mm'],
                'Mean_Daily': season['et0']['mean_daily'],
                'Max_Daily': season['et0']['max_daily'],
                'Min_Daily': season['et0']['min_daily']
            })
            season_data.append(et0_row)

            wb_row = base_info.copy()
            wb_row.update({
                'Variable': 'Water Balance',
                'Total_Balance': season['water_balance']['total_balance'],
                'Mean_Daily': season['water_balance']['mean_daily'],
                'Deficit_Days': season['water_balance']['deficit_days'],
                'Surplus_Days': season['water_balance']['surplus_days'],
                'Stress_Ratio': season['water_balance']['water_stress_ratio']
            })
            season_data.append(wb_row)

        season_df = pd.DataFrame(season_data)
        print(season_df.to_string(index=False))
    else:
        print("No seasons detected")

def main():
    """Command-line interface for climate statistics analysis."""
    parser = argparse.ArgumentParser(description='Climate statistics analysis by season')

    parser.add_argument('--location', required=True,
                       help='Location as "lat,lon" (e.g., "-1.286,36.817")')
    parser.add_argument('--date-from', required=True,
                       help='Start date in YYYY-MM-DD format')
    parser.add_argument('--date-to', required=True,
                       help='End date in YYYY-MM-DD format')
    parser.add_argument('--source', required=True,
                       help='Data source key')
    parser.add_argument('--gap-days', type=int, default=30,
                       help='Consecutive dry days to end season (default: 30)')
    parser.add_argument('--min-season-days', type=int, default=30,
                       help='Minimum season length in days (default: 30)')
    parser.add_argument('--format', choices=['json', 'pandas'], default='json',
                       help='Output format: json or pandas (default: json)')
    parser.add_argument('--output',
                       help='Output JSON file path (only for json format)')

    args = parser.parse_args()

    try:
        lat, lon = map(float, args.location.split(','))
        location = (lat, lon)
    except ValueError:
        print("Error: Invalid location format. Use 'lat,lon' format.")
        sys.exit(1)

    result = analyze_climate_statistics(
        location_coord=location,
        date_range=(args.date_from, args.date_to),
        source=args.source,
        gap_days=args.gap_days,
        min_season_days=args.min_season_days
    )

    if args.format == 'pandas':
        format_as_dataframes(result)
    else:
        output = json.dumps(result, indent=2, default=str)

        if args.output:
            with open(args.output, 'w') as f:
                f.write(output)
            print(f"Climate statistics saved to {args.output}")
        else:
            print(output)

if __name__ == "__main__":
    main()

# python -m climate_tookit.climate_statistics.statistics --source nasa_power --location="-1.286,36.817" --date-from 2012-01-01 --date-to 2012-12-31 --format pandas