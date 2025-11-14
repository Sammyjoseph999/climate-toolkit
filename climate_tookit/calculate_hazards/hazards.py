"""
Calculate Hazards Module

Retrieves crop hazard indices at a specific location by:
1. Using season_analysis to detect growing seasons or accepting season dates
2. Calculating total precipitation and average temperature for the season
3. Evaluating crop-specific hazard thresholds

Dependencies: pandas, season_analysis.seasons module
"""

import sys
import os
from datetime import date, datetime
from typing import Dict, List, Any, Tuple, Optional
import pandas as pd
import json
import argparse

current_dir = os.path.dirname(__file__)
parent_dir = os.path.dirname(current_dir)
sys.path.append(os.path.join(parent_dir, 'season_analysis'))

try:
    from seasons import analyze_season, get_climate_data, calculate_et0
    SEASON_ANALYSIS_AVAILABLE = True
except ImportError:
    SEASON_ANALYSIS_AVAILABLE = False
    print("Warning: Season analysis module not available")

CROP_THRESHOLDS = {
    'Beans': {'Total Precip': {'no_stress': (500, 2000), 'moderate_stress_low': (300, 500), 'moderate_stress_up': (2000, 4300), 'severe_stress_low': (None, 300), 'severe_stress_up': (4300, None)}, 'TAVG': {'no_stress': (18, 30), 'moderate_stress_low': (7, 18), 'moderate_stress_up': (30, 32), 'severe_stress_low': (None, 7), 'severe_stress_up': (32, None)}},
    'Maize': {'Total Precip': {'no_stress': (500, 1200), 'moderate_stress_low': (400, 500), 'moderate_stress_up': (1200, 1800), 'severe_stress_low': (None, 400), 'severe_stress_up': (1800, None)}, 'TAVG': {'no_stress': (18, 32), 'moderate_stress_low': (14, 18), 'moderate_stress_up': (32, 40), 'severe_stress_low': (None, 14), 'severe_stress_up': (40, None)}},
    'Millet': {'Total Precip': {'no_stress': (300, 600), 'moderate_stress_low': (200, 300), 'moderate_stress_up': (600, 1700), 'severe_stress_low': (None, 200), 'severe_stress_up': (1700, None)}, 'TAVG': {'no_stress': (16, 32), 'moderate_stress_low': (12, 16), 'moderate_stress_up': (32, 40), 'severe_stress_low': (None, 12), 'severe_stress_up': (40, None)}},
    'Groundnuts': {'Total Precip': {'no_stress': (400, 1100), 'moderate_stress_low': (200, 400), 'moderate_stress_up': (1100, 1900), 'severe_stress_low': (None, 200), 'severe_stress_up': (1900, None)}, 'TAVG': {'no_stress': (22, 28), 'moderate_stress_low': (18, 22), 'moderate_stress_up': (28, 30), 'severe_stress_low': (None, 18), 'severe_stress_up': (30, None)}},
    'Sorghum': {'Total Precip': {'no_stress': (400, 900), 'moderate_stress_low': (150, 400), 'moderate_stress_up': (900, 1400), 'severe_stress_low': (None, 150), 'severe_stress_up': (1400, None)}, 'TAVG': {'no_stress': (21, 32), 'moderate_stress_low': (8, 21), 'moderate_stress_up': (32, 40), 'severe_stress_low': (None, 8), 'severe_stress_up': (40, None)}},
    'Cassava': {'Total Precip': {'no_stress': (1400, 1800), 'moderate_stress_low': (500, 1400), 'moderate_stress_up': (1800, 5000), 'severe_stress_low': (None, 500), 'severe_stress_up': (5000, None)}, 'TAVG': {'no_stress': (20, 29), 'moderate_stress_low': (10, 20), 'moderate_stress_up': (29, 35), 'severe_stress_low': (None, 10), 'severe_stress_up': (35, None)}},
    'Rice': {'Total Precip': {'no_stress': (1500, 2000), 'moderate_stress_low': (1000, 1500), 'moderate_stress_up': (2000, 4000), 'severe_stress_low': (None, 1000), 'severe_stress_up': (4000, None)}, 'TAVG': {'no_stress': (20, 30), 'moderate_stress_low': (10, 20), 'moderate_stress_up': (30, 36), 'severe_stress_low': (None, 10), 'severe_stress_up': (36, None)}}
}

def get_climate_data_for_season(lat: float, lon: float, start_date: str, end_date: str) -> pd.DataFrame:
    if not SEASON_ANALYSIS_AVAILABLE:
        raise Exception("Season analysis module not available")

    df = get_climate_data(lat, lon, start_date, end_date)

    df['et0'] = df.apply(lambda row: calculate_et0(row['tmin'], row['tmax'], lat, row['date']), axis=1)

    if df.empty:
        raise Exception(f"No climate data returned")
    return df

def calculate_season_statistics(df: pd.DataFrame) -> Dict[str, float]:
    stats = {}

    precip_col = None
    for col in ['precipitation', 'precip', 'total_precipitation']:
        if col in df.columns:
            precip_col = col
            break

    if precip_col:
        precip_data = df[precip_col].copy()
        stats['total_precipitation_mm'] = precip_data.sum()
        stats['mean_daily_precipitation_mm'] = precip_data.mean()
        stats['max_daily_precipitation_mm'] = precip_data.max()
        stats['rainy_days'] = (precip_data > 1.0).sum()
        stats['dry_days'] = (precip_data <= 1.0).sum()

    tmax_col = None
    tmin_col = None
    for col in ['max_temperature', 'tmax', 'maximum_2m_air_temperature']:
        if col in df.columns:
            tmax_col = col
            break
    for col in ['min_temperature', 'tmin', 'minimum_2m_air_temperature']:
        if col in df.columns:
            tmin_col = col
            break

    if tmax_col and tmin_col:
        tmax_data = df[tmax_col].copy()
        tmin_data = df[tmin_col].copy()

        if tmax_data.mean() > 100:
            tmax_data = tmax_data - 273.15
            tmin_data = tmin_data - 273.15

        tavg = (tmax_data + tmin_data) / 2
        stats['mean_temperature_c'] = tavg.mean()
        stats['mean_tmax_c'] = tmax_data.mean()
        stats['mean_tmin_c'] = tmin_data.mean()
        stats['max_temperature_c'] = tmax_data.max()
        stats['min_temperature_c'] = tmin_data.min()

    return stats

def evaluate_threshold(value: float, thresholds: Dict[str, Tuple]) -> str:
    for level, (lower, upper) in thresholds.items():
        if lower is None and value < upper:
            return level
        elif upper is None and value > lower:
            return level
        elif lower is not None and upper is not None and lower <= value <= upper:
            return level
    return 'unknown'

def calculate_hazards(crop_name: str, location_coord: Tuple[float, float], date_from: str, date_to: str, season_start: Optional[str] = None, season_end: Optional[str] = None, custom_thresholds: Optional[Dict] = None, gap_days: int = 30, min_season_days: int = 30) -> Dict[str, Any]:
    lat, lon = location_coord
    crop_name_normalized = crop_name.capitalize()
    if crop_name_normalized not in CROP_THRESHOLDS and not custom_thresholds:
        return {'error': f'Unknown crop: {crop_name}. Available crops: {", ".join(CROP_THRESHOLDS.keys())}', 'available_crops': list(CROP_THRESHOLDS.keys())}
    thresholds = custom_thresholds if custom_thresholds else CROP_THRESHOLDS[crop_name_normalized]

    if season_start and season_end:
        print(f"Using provided season dates: {season_start} to {season_end}")
        df = get_climate_data_for_season(lat, lon, season_start, season_end)
        season_info = {'season_detected': True, 'onset_date': season_start, 'cessation_date': season_end, 'length_days': (datetime.fromisoformat(season_end) - datetime.fromisoformat(season_start)).days, 'method': 'user_provided'}
    elif SEASON_ANALYSIS_AVAILABLE:
        print(f"Detecting growing season for {crop_name} at ({lat}, {lon})")
        season_result = analyze_season(location_coord=(lat, lon), date_range=(date_from, date_to), gap_days=gap_days, min_season_days=min_season_days)
        if 'error' in season_result or not season_result.get('seasons'):
            return {'error': 'No growing season detected. Please provide season_start and season_end dates.', 'season_info': season_result}
        first_season = season_result['seasons'][0]
        season_start = first_season['onset_date']
        season_end = first_season['cessation_date']
        season_info = {'season_detected': True, 'onset_date': season_start, 'cessation_date': season_end, 'length_days': first_season['length_days'], 'method': 'rainfall_based'}
        df = get_climate_data_for_season(lat, lon, season_start, season_end)
    else:
        return {'error': 'Season analysis not available and no season dates provided'}

    stats = calculate_season_statistics(df)
    hazard_assessment = {'crop': crop_name, 'location': {'latitude': lat, 'longitude': lon}, 'season_info': season_info, 'season_statistics': stats, 'hazard_evaluation': {}}

    if 'Total Precip' in thresholds and 'total_precipitation_mm' in stats:
        precip_value = stats['total_precipitation_mm']
        precip_status = evaluate_threshold(precip_value, thresholds['Total Precip'])
        hazard_assessment['hazard_evaluation']['precipitation'] = {'value_mm': round(precip_value, 2), 'status': precip_status}

    if 'TAVG' in thresholds and 'mean_temperature_c' in stats:
        temp_value = stats['mean_temperature_c']
        temp_status = evaluate_threshold(temp_value, thresholds['TAVG'])
        hazard_assessment['hazard_evaluation']['temperature'] = {'value_c': round(temp_value, 2), 'status': temp_status}

    return hazard_assessment

def print_hazard_results(result: Dict[str, Any]):
    if 'error' in result:
        print(f"\nError: {result['error']}")
        if 'available_crops' in result:
            print(f"Available crops: {', '.join(result['available_crops'])}")
        return

    print(f"\n{'='*70}")
    print(f"  CROP HAZARD ASSESSMENT: {result['crop'].upper()}")
    print(f"{'='*70}")
    print(f"  Location: {result['location']['latitude']:.4f}, {result['location']['longitude']:.4f}")

    season = result['season_info']
    print(f"\n  Season Information")
    print(f"  {'─'*66}")
    print(f"  Onset:  {season['onset_date']:<20} End: {season['cessation_date']}")
    print(f"  Length: {season['length_days']} days{' '*15} Method: {season.get('method', 'unknown')}")

    stats = result['season_statistics']
    if 'total_precipitation_mm' in stats:
        print(f"\n  Precipitation Statistics")
        print(f"  {'─'*66}")
        print(f"  {'Metric':<32} {'Value':>15}  {'Unit':<10}")
        print(f"  {'─'*32} {'─'*15}  {'─'*10}")
        print(f"  {'Total':<32} {stats['total_precipitation_mm']:>15.2f}  {'mm':<10}")
        print(f"  {'Daily Mean':<32} {stats['mean_daily_precipitation_mm']:>15.2f}  {'mm':<10}")
        print(f"  {'Daily Maximum':<32} {stats['max_daily_precipitation_mm']:>15.2f}  {'mm':<10}")
        print(f"  {'Rainy Days (>1mm)':<32} {stats['rainy_days']:>15}  {'days':<10}")
        if 'dry_days' in stats:
            print(f"  {'Dry Days (≤1mm)':<32} {stats['dry_days']:>15}  {'days':<10}")

    if 'mean_temperature_c' in stats:
        print(f"\n  Temperature Statistics")
        print(f"  {'─'*66}")
        print(f"  {'Metric':<32} {'Value':>15}  {'Unit':<10}")
        print(f"  {'─'*32} {'─'*15}  {'─'*10}")
        print(f"  {'Mean Temperature':<32} {stats['mean_temperature_c']:>15.2f}  {'°C':<10}")
        print(f"  {'Mean Tmax':<32} {stats['mean_tmax_c']:>15.2f}  {'°C':<10}")
        print(f"  {'Mean Tmin':<32} {stats['mean_tmin_c']:>15.2f}  {'°C':<10}")
        print(f"  {'Maximum Recorded':<32} {stats['max_temperature_c']:>15.2f}  {'°C':<10}")
        print(f"  {'Minimum Recorded':<32} {stats['min_temperature_c']:>15.2f}  {'°C':<10}")

    hazards = result['hazard_evaluation']
    print(f"\n  Hazard Assessment")
    print(f"  {'─'*66}")
    print(f"  {'Indicator':<25} {'Value':>18}  {'Status':<20}")
    print(f"  {'─'*25} {'─'*18}  {'─'*20}")

    if 'precipitation' in hazards:
        precip = hazards['precipitation']
        status_display = precip['status'].replace('_', ' ').upper()
        status_symbol = '✓' if 'no_stress' in precip['status'] else '⚠' if 'moderate' in precip['status'] else '✗'
        print(f"  {'Precipitation':<25} {precip['value_mm']:>16.2f} mm  {status_symbol} {status_display:<18}")

    if 'temperature' in hazards:
        temp = hazards['temperature']
        status_display = temp['status'].replace('_', ' ').upper()
        status_symbol = '✓' if 'no_stress' in temp['status'] else '⚠' if 'moderate' in temp['status'] else '✗'
        print(f"  {'Temperature':<25} {temp['value_c']:>16.2f} °C  {status_symbol} {status_display:<18}")

    print(f"\n{'='*70}\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Calculate crop hazard indices')
    parser.add_argument('crop', type=str, help='Crop name')
    parser.add_argument('--location', type=str, required=True, help='Location as "lat,lon"')
    parser.add_argument('--date-from', type=str, required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--date-to', type=str, required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--season-start', type=str, help='Season start (YYYY-MM-DD)')
    parser.add_argument('--season-end', type=str, help='Season end (YYYY-MM-DD)')
    parser.add_argument('--gap-days', type=int, default=30, help='Dry days to end season (default: 30)')
    parser.add_argument('--min-season-days', type=int, default=30, help='Min season length (default: 30)')
    parser.add_argument('--format', choices=['json', 'text'], default='text', help='Output format')
    parser.add_argument('--output', type=str, help='Output file')
    args = parser.parse_args()
    lat, lon = map(float, args.location.split(','))
    result = calculate_hazards(crop_name=args.crop, location_coord=(lat, lon), date_from=args.date_from, date_to=args.date_to, season_start=args.season_start, season_end=args.season_end, gap_days=args.gap_days, min_season_days=args.min_season_days)
    if args.format == 'json':
        output = json.dumps(result, indent=2, default=str)
        print(output)
        if args.output:
            with open(args.output, 'w') as f:
                f.write(output)
    else:
        print_hazard_results(result)
        if args.output:
            with open(args.output, 'w') as f:
                f.write(json.dumps(result, indent=2, default=str))
                
# python -m climate_tookit.calculate_hazards.hazards maize --location="-1.286,36.817" --date-from 2020-01-01 --date-to 2020-12-31 --season-start 2020-03-01 --season-end 2020-06-30