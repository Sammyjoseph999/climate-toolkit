"""
Season Analysis Module

Detects agricultural growing seasons based on daily precipitation and temperature data.
Uses ET0 calculations and precipitation thresholds to identify planting season onset
and cessation dates.

The module fetches climate data from Open-Meteo API and applies the Hargreaves method
for evapotranspiration calculations to determine when precipitation exceeds 50% of ET0,
indicating favorable conditions for crop growth.

Dependencies: requests, pandas
"""

import requests
import pandas as pd
import math
import json
import argparse
import sys
from datetime import datetime, timedelta
from typing import Tuple, Dict, List, Any

def get_climate_data(lat: float, lon: float, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch daily climate data from Open-Meteo Climate API.

    Args:
        lat (float): Latitude in decimal degrees
        lon (float): Longitude in decimal degrees
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format

    Returns:
        pd.DataFrame: DataFrame with columns [date, tmax, tmin, precip]

    Raises:
        Exception: If API request fails or returns non-200 status code
    """
    url = "https://climate-api.open-meteo.com/v1/climate"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "models": "MRI_AGCM3_2_S",
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
        "temperature_unit": "celsius",
        "precipitation_unit": "mm",
        "timeformat": "iso8601"
    }

    response = requests.get(url, params=params, timeout=30)
    if response.status_code != 200:
        raise Exception(f"API error {response.status_code}: {response.text}")

    data = response.json()
    return pd.DataFrame({
        "date": pd.to_datetime(data['daily']['time']),
        "tmax": data["daily"]["temperature_2m_max"],
        "tmin": data["daily"]["temperature_2m_min"],
        "precip": data["daily"]["precipitation_sum"]
    })

def calculate_et0(tmin: float, tmax: float, lat: float, date: datetime) -> float:
    """
    Calculate reference evapotranspiration using the Hargreaves method.

    The Hargreaves method estimates ET0 based on temperature range and
    extraterrestrial radiation, suitable for areas with limited weather data.

    Args:
        tmin (float): Minimum daily temperature in Celsius
        tmax (float): Maximum daily temperature in Celsius
        lat (float): Latitude in decimal degrees
        date (datetime): Date for solar radiation calculation

    Returns:
        float: Reference evapotranspiration in mm/day, returns 0 if invalid inputs
    """
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
    """
    Detect growing seasons based on precipitation exceeding ET0 threshold.

    A growing season begins when precipitation >= 0.5 * ET0 and ends after
    a specified number of consecutive dry days (precipitation < 0.5 * ET0).

    Args:
        df (pd.DataFrame): DataFrame with columns [date, precip, et0]
        gap_days (int): Number of consecutive dry days to end season. Default 30
        min_season_days (int): Minimum season length in days. Default 30

    Returns:
        List[Dict]: List of detected seasons with onset/cessation dates and lengths
    """
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
                    'onset_date': onset_date.strftime('%Y-%m-%d'),
                    'cessation_date': cessation_date.strftime('%Y-%m-%d'),
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

def monthly_chunks(year: int) -> List[Tuple[str, str]]:
    """
    Generate monthly date ranges for a given year.

    Args:
        year (int): Year to generate chunks for

    Returns:
        List[Tuple[str, str]]: List of (start_date, end_date) tuples in YYYY-MM-DD format
    """
    chunks = []
    for m in range(1, 13):
        start = datetime(year, m, 1)
        end = datetime(year, 12, 31) if m == 12 else datetime(year, m + 1, 1) - timedelta(days=1)
        chunks.append((start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")))
    return chunks

def analyze_season(location_coord: Tuple[float, float],
                  date_range: Tuple[str, str],
                  gap_days: int = 30,
                  min_season_days: int = 30) -> Dict[str, Any]:
    """
    Analyze growing seasons for a specific date range.

    Args:
        location_coord (Tuple[float, float]): (latitude, longitude) in decimal degrees
        date_range (Tuple[str, str]): (start_date, end_date) in YYYY-MM-DD format
        gap_days (int): Consecutive dry days to end season. Default 30
        min_season_days (int): Minimum season length in days. Default 30

    Returns:
        Dict[str, Any]: Analysis results including detected seasons and metadata
    """
    lat, lon = location_coord
    start_date, end_date = date_range

    try:
        df = get_climate_data(lat, lon, start_date, end_date)

        et0_values = [calculate_et0(row['tmin'], row['tmax'], lat, row['date'])
                     for _, row in df.iterrows()]
        df['et0'] = et0_values

        seasons = detect_seasons(df, gap_days, min_season_days)

        return {
            'location': {'lat': lat, 'lon': lon},
            'date_range': {'start': start_date, 'end': end_date},
            'seasons_detected': len(seasons),
            'seasons': seasons,
            'main_season': max(seasons, key=lambda x: x['length_days']) if seasons else None,
            'method': 'ET0_precipitation_threshold',
            'analysis_date': datetime.now().isoformat()
        }

    except Exception as e:
        return {
            'error': str(e),
            'location': {'lat': lat, 'lon': lon},
            'date_range': {'start': start_date, 'end': end_date}
        }

def analyze_full_year(location_coord: Tuple[float, float],
                     year: int,
                     gap_days: int = 30,
                     min_season_days: int = 30) -> Dict[str, Any]:
    """
    Analyze growing seasons for a complete year using monthly data chunks.

    Args:
        location_coord (Tuple[float, float]): (latitude, longitude) in decimal degrees
        year (int): Year to analyze
        gap_days (int): Consecutive dry days to end season. Default 30
        min_season_days (int): Minimum season length in days. Default 30

    Returns:
        Dict[str, Any]: Analysis results including detected seasons and metadata
    """
    lat, lon = location_coord

    try:
        all_dfs = []
        for start_date, end_date in monthly_chunks(year):
            df_month = get_climate_data(lat, lon, start_date, end_date)
            all_dfs.append(df_month)

        df = pd.concat(all_dfs).drop_duplicates(subset="date").reset_index(drop=True)
        df = df.sort_values("date").reset_index(drop=True)

        et0_values = [calculate_et0(row['tmin'], row['tmax'], lat, row['date'])
                     for _, row in df.iterrows()]
        df['et0'] = et0_values

        seasons = detect_seasons(df, gap_days, min_season_days)

        return {
            'location': {'lat': lat, 'lon': lon},
            'year': year,
            'seasons_detected': len(seasons),
            'seasons': seasons,
            'main_season': max(seasons, key=lambda x: x['length_days']) if seasons else None,
            'method': 'ET0_precipitation_threshold',
            'analysis_date': datetime.now().isoformat()
        }

    except Exception as e:
        return {
            'error': str(e),
            'location': {'lat': lat, 'lon': lon},
            'year': year
        }

def main():
    """Command-line interface for season analysis."""
    parser = argparse.ArgumentParser(description='Season analysis using climate data')

    parser.add_argument('--location', required=True,
                       help='Location as "lat,lon" (e.g., "-1.286,36.817")')
    parser.add_argument('--date-from', required=True,
                       help='Start date in YYYY-MM-DD format')
    parser.add_argument('--date-to', required=True,
                       help='End date in YYYY-MM-DD format')
    parser.add_argument('--gap-days', type=int, default=30,
                       help='Consecutive dry days to end season (default: 30)')
    parser.add_argument('--min-season-days', type=int, default=30,
                       help='Minimum season length in days (default: 30)')
    parser.add_argument('--output',
                       help='Output JSON file path')
    parser.add_argument('--show-data', action='store_true',
                       help='Show DataFrame with daily climate data')

    args = parser.parse_args()

    try:
        lat, lon = map(float, args.location.split(','))
        location = (lat, lon)
    except ValueError:
        print("Error: Invalid location format. Use 'lat,lon' format.")
        sys.exit(1)

    result = analyze_season(
        location_coord=location,
        date_range=(args.date_from, args.date_to),
        gap_days=args.gap_days,
        min_season_days=args.min_season_days
    )

    if args.show_data and 'error' not in result:
        print("=== DAILY CLIMATE DATA ===")
        lat, lon = location
        df = get_climate_data(lat, lon, args.date_from, args.date_to)
        et0_values = [calculate_et0(row['tmin'], row['tmax'], lat, row['date'])
                     for _, row in df.iterrows()]
        df['et0'] = et0_values
        df['threshold'] = df['et0'] * 0.5
        df['rainy_day'] = df['precip'] >= df['threshold']
        print(df.head(10))
        print("...")
        print(df.tail(10))
        print(f"Total records: {len(df)}")
        print("=== SEASON ANALYSIS RESULTS ===")

    output = json.dumps(result, indent=2, default=str)

    if args.output:
        with open(args.output, 'w') as f:
            f.write(output)
        print(f"Results saved to {args.output}")
    else:
        print(output)

if __name__ == "__main__":
    main()
    

# python -m climate_tookit.season_analysis.seasons --location="-1.286,36.817" --date-from 2020-01-01 --date-to 2020-12-31 --show-data