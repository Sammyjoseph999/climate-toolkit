"""
Season Analysis Module

Detects agricultural growing seasons based on daily precipitation and temperature data.
Uses ET0 calculations and precipitation thresholds to identify planting season onset
and cessation dates.

The module fetches climate data from Open-Meteo API (historical) and NEX-GDDP-CMIP6
(future projections) and applies the Hargreaves method for evapotransporation
calculations to determine when precipitation exceeds 50% of ET0, indicating favorable
conditions for crop growth.

Dependencies: requests, pandas, ee (Google Earth Engine)
"""

import requests
import pandas as pd
import math
import json
import argparse
import sys
import os
from datetime import datetime, timedelta, date
from typing import Tuple, Dict, List, Any
from dotenv import load_dotenv

load_dotenv()

# Import NEX-GDDP for future projections
try:
    import ee
    NEX_GDDP_AVAILABLE = True
except ImportError:
    NEX_GDDP_AVAILABLE = False
    print("Warning: NEX-GDDP not available. Future projections will use Open-Meteo (limited accuracy).")

def get_climate_data(lat: float, lon: float, start_date: str, end_date: str,
                     use_projections: bool = False, model: str = 'GFDL-ESM4',
                     scenario: str = 'ssp245') -> pd.DataFrame:
    """
    Fetch daily climate data from Open-Meteo (historical) or NEX-GDDP (projections).

    Args:
        lat (float): Latitude in decimal degrees
        lon (float): Longitude in decimal degrees
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
        use_projections (bool): If True, use NEX-GDDP for future projections
        model (str): Climate model for NEX-GDDP (default: GFDL-ESM4)
        scenario (str): SSP scenario for NEX-GDDP (default: ssp245)

    Returns:
        pd.DataFrame: DataFrame with columns [date, tmax, tmin, precip]

    Raises:
        Exception: If API request fails or returns non-200 status code
    """

    # Determine if we should use projections based on year
    start_year = int(start_date.split('-')[0])

    # Use NEX-GDDP for years >= 2015 if available
    if use_projections and start_year >= 2015 and NEX_GDDP_AVAILABLE:
        return _get_nex_gddp_data(lat, lon, start_date, end_date, model, scenario)
    else:
        return _get_open_meteo_data(lat, lon, start_date, end_date)

def _get_open_meteo_data(lat: float, lon: float, start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch data from Open-Meteo Climate API."""
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

def _get_nex_gddp_data(lat: float, lon: float, start_date: str, end_date: str,
                       model: str = 'GFDL-ESM4', scenario: str = 'ssp245') -> pd.DataFrame:
    """Fetch data from NEX-GDDP-CMIP6 via Google Earth Engine."""

    print(f"Fetching NEX-GDDP data: model={model}, scenario={scenario}")

    # Authenticate GEE
    try:
        ee.Initialize(project=os.getenv("GCP_PROJECT_ID"))
    except:
        ee.Authenticate()
        ee.Initialize(project=os.getenv("GCP_PROJECT_ID"))

    location = ee.Geometry.Point([lon, lat])
    collection_name = 'NASA/GDDP-CMIP6'

    # Convert dates
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')

    # Generate date list
    dates = []
    current_date = start_dt
    while current_date <= end_dt:
        dates.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)

    ee_dates = ee.List(dates)

    def get_single_data(date_str):
        start = ee.Date(date_str)
        end = start.advance(1, 'day')

        # Filter collection
        filtered = (ee.ImageCollection(collection_name)
                   .filterDate(start, end)
                   .filter(ee.Filter.eq('model', model))
                   .filter(ee.Filter.eq('scenario', scenario))
                   .filterBounds(location))

        # Get first image and select variables
        image = filtered.first().select(['pr', 'tasmax', 'tasmin'])

        # Reduce region
        result = image.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=location,
            scale=25000,
            maxPixels=1e9,
            bestEffort=True,
            tileScale=1
        )

        return ee.Feature(None, {'date': date_str}).set(result)

    # Map over dates and fetch
    results = ee_dates.map(get_single_data)
    features = results.getInfo()

    data_list = [feature['properties'] for feature in features]
    df = pd.DataFrame(data_list)

    if df.empty:
        raise Exception("No data retrieved from NEX-GDDP")

    # Convert units
    # NEX-GDDP: pr (kg/m2/s) -> mm/day, tasmax/tasmin (K) -> Celsius
    df['precip'] = df['pr'] * 86400  # Convert to mm/day
    df['tmax'] = df['tasmax'] - 273.15  # Convert to Celsius
    df['tmin'] = df['tasmin'] - 273.15  # Convert to Celsius
    df['date'] = pd.to_datetime(df['date'])

    # Select and reorder columns
    return df[['date', 'tmax', 'tmin', 'precip']]

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

def calculate_average_season(seasons: List[Dict]) -> Dict[str, Any]:
    """
    Calculate average season characteristics from multiple seasons.

    Args:
        seasons: List of detected seasons

    Returns:
        Dict with average onset DOY, cessation DOY, and length
    """
    if not seasons:
        return None

    return {
        'avg_onset_doy': sum(s['onset_doy'] for s in seasons) / len(seasons),
        'avg_cessation_doy': sum(s['cessation_doy'] for s in seasons) / len(seasons),
        'avg_length_days': sum(s['length_days'] for s in seasons) / len(seasons),
        'season_count': len(seasons)
    }

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

def calculate_average_season(seasons: List[Dict]) -> Dict[str, Any]:
    """
    Calculate average season characteristics from multiple seasons.

    Args:
        seasons: List of detected seasons

    Returns:
        Dict with average onset DOY, cessation DOY, and length
    """
    if not seasons:
        return None

    return {
        'avg_onset_doy': sum(s['onset_doy'] for s in seasons) / len(seasons),
        'avg_cessation_doy': sum(s['cessation_doy'] for s in seasons) / len(seasons),
        'avg_length_days': sum(s['length_days'] for s in seasons) / len(seasons),
        'season_count': len(seasons)
    }

def analyze_season(location_coord: Tuple[float, float],
                  date_range: Tuple[str, str],
                  gap_days: int = 30,
                  min_season_days: int = 30,
                  baseline_years: Tuple[int, int] = None,
                  future_years: Tuple[int, int] = None,
                  climate_model: str = 'GFDL-ESM4',
                  scenario: str = 'ssp245') -> Dict[str, Any]:
    """
    Analyze growing seasons for a specific date range with optional baseline and future averages.

    Args:
        location_coord (Tuple[float, float]): (latitude, longitude) in decimal degrees
        date_range (Tuple[str, str]): (start_date, end_date) in YYYY-MM-DD format
        gap_days (int): Consecutive dry days to end season. Default 30
        min_season_days (int): Minimum season length in days. Default 30
        baseline_years (Tuple[int, int]): Optional (start_year, end_year) for baseline average
        future_years (Tuple[int, int]): Optional (start_year, end_year) for future average
        climate_model (str): Climate model for projections. Default GFDL-ESM4
        scenario (str): SSP scenario for projections. Default ssp245

    Returns:
        Dict[str, Any]: Analysis results including detected seasons and averages
    """
    lat, lon = location_coord
    start_date, end_date = date_range

    try:
        # Primary period analysis (always uses Open-Meteo)
        df = _get_open_meteo_data(lat, lon, start_date, end_date)

        et0_values = [calculate_et0(row['tmin'], row['tmax'], lat, row['date'])
                     for _, row in df.iterrows()]
        df['et0'] = et0_values

        seasons = detect_seasons(df, gap_days, min_season_days)

        result = {
            'location': {'lat': lat, 'lon': lon},
            'actual_period': {'start': start_date, 'end': end_date},
            'seasons_detected': len(seasons),
            'seasons': seasons,
            'main_season': max(seasons, key=lambda x: x['length_days']) if seasons else None,
            'method': 'ET0_precipitation_threshold',
            'data_source': 'Open-Meteo',
            'analysis_date': datetime.now().isoformat()
        }

        # Calculate baseline average if requested (always use historical data)
        if baseline_years:
            baseline_start, baseline_end = baseline_years
            all_baseline_seasons = []

            print(f"Calculating baseline average for {baseline_start}-{baseline_end}...")
            for year in range(baseline_start, baseline_end + 1):
                try:
                    year_start = f"{year}-01-01"
                    year_end = f"{year}-12-31"
                    # Always use Open-Meteo for baseline (historical data)
                    df_baseline = _get_open_meteo_data(lat, lon, year_start, year_end)
                    et0_baseline = [calculate_et0(row['tmin'], row['tmax'], lat, row['date'])
                                  for _, row in df_baseline.iterrows()]
                    df_baseline['et0'] = et0_baseline
                    year_seasons = detect_seasons(df_baseline, gap_days, min_season_days)
                    all_baseline_seasons.extend(year_seasons)
                    print(f"  {year}: {len(year_seasons)} seasons")
                except Exception as e:
                    print(f"  {year}: Failed - {e}")
                    continue

            result['baseline_average'] = calculate_average_season(all_baseline_seasons)
            result['baseline_period'] = {'start': baseline_start, 'end': baseline_end}
            result['baseline_data_source'] = 'Open-Meteo'

        # Calculate future average if requested (always use NEX-GDDP projections)
        if future_years:
            future_start, future_end = future_years
            all_future_seasons = []

            print(f"Calculating future average for {future_start}-{future_end} using NEX-GDDP...")
            for year in range(future_start, future_end + 1):
                try:
                    year_start = f"{year}-01-01"
                    year_end = f"{year}-12-31"
                    # Always use NEX-GDDP for future projections
                    df_future = _get_nex_gddp_data(lat, lon, year_start, year_end,
                                                   model=climate_model, scenario=scenario)
                    et0_future = [calculate_et0(row['tmin'], row['tmax'], lat, row['date'])
                                for _, row in df_future.iterrows()]
                    df_future['et0'] = et0_future
                    year_seasons = detect_seasons(df_future, gap_days, min_season_days)
                    all_future_seasons.extend(year_seasons)
                    print(f"  {year}: {len(year_seasons)} seasons")
                except Exception as e:
                    print(f"  {year}: Failed - {e}")
                    continue

            result['future_average'] = calculate_average_season(all_future_seasons)
            result['future_period'] = {'start': future_start, 'end': future_end}
            result['future_model'] = climate_model
            result['future_scenario'] = scenario
            result['future_data_source'] = 'NEX-GDDP-CMIP6'

        return result

    except Exception as e:
        return {
            'error': str(e),
            'location': {'lat': lat, 'lon': lon},
            'actual_period': {'start': start_date, 'end': end_date}
        }

def analyze_full_year(location_coord: Tuple[float, float],
                     year: int,
                     gap_days: int = 30,
                     min_season_days: int = 30,
                     climate_model: str = 'GFDL-ESM4',
                     scenario: str = 'ssp245') -> Dict[str, Any]:
    """
    Analyze growing seasons for a complete year using monthly data chunks.

    Args:
        location_coord (Tuple[float, float]): (latitude, longitude) in decimal degrees
        year (int): Year to analyze
        gap_days (int): Consecutive dry days to end season. Default 30
        min_season_days (int): Minimum season length in days. Default 30
        climate_model (str): Climate model for projections. Default GFDL-ESM4
        scenario (str): SSP scenario for projections. Default ssp245

    Returns:
        Dict[str, Any]: Analysis results including detected seasons and metadata
    """
    lat, lon = location_coord
    use_projections = year >= 2015

    try:
        all_dfs = []
        for start_date, end_date in monthly_chunks(year):
            df_month = get_climate_data(lat, lon, start_date, end_date,
                                       use_projections=use_projections,
                                       model=climate_model, scenario=scenario)
            all_dfs.append(df_month)

        df = pd.concat(all_dfs).drop_duplicates(subset="date").reset_index(drop=True)
        df = df.sort_values("date").reset_index(drop=True)

        et0_values = [calculate_et0(row['tmin'], row['tmax'], lat, row['date'])
                     for _, row in df.iterrows()]
        df['et0'] = et0_values

        seasons = detect_seasons(df, gap_days, min_season_days)

        result = {
            'location': {'lat': lat, 'lon': lon},
            'year': year,
            'seasons_detected': len(seasons),
            'seasons': seasons,
            'main_season': max(seasons, key=lambda x: x['length_days']) if seasons else None,
            'method': 'ET0_precipitation_threshold',
            'data_source': 'NEX-GDDP-CMIP6' if use_projections else 'Open-Meteo',
            'analysis_date': datetime.now().isoformat()
        }

        if use_projections:
            result['climate_model'] = climate_model
            result['scenario'] = scenario

        return result

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
    parser.add_argument('--date-from',
                       help='Start date in YYYY-MM-DD format')
    parser.add_argument('--date-to',
                       help='End date in YYYY-MM-DD format')
    parser.add_argument('--baseline-start', type=int,
                       help='Baseline start year (e.g., 1980)')
    parser.add_argument('--baseline-only', action='store_true',
                       help='Calculate only baseline average without primary analysis')
    parser.add_argument('--future-only', action='store_true',
                       help='Calculate only future average using NEX-GDDP projections')
    parser.add_argument('--baseline-end', type=int,
                       help='Baseline end year (e.g., 2010)')
    parser.add_argument('--future-start', type=int,
                       help='Future start year (e.g., 2050)')
    parser.add_argument('--future-end', type=int,
                       help='Future end year (e.g., 2080)')
    parser.add_argument('--climate-model', default='GFDL-ESM4',
                       help='Climate model for projections (default: GFDL-ESM4)')
    parser.add_argument('--scenario', default='ssp245',
                       help='SSP scenario: ssp126, ssp245, ssp370, ssp585 (default: ssp245)')
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

    # Validate arguments
    if args.baseline_only and args.future_only:
        print("Error: Cannot use both --baseline-only and --future-only")
        sys.exit(1)

    if args.baseline_only:
        if not (args.baseline_start and args.baseline_end):
            print("Error: --baseline-only requires --baseline-start and --baseline-end")
            sys.exit(1)
        # Use baseline period as dummy date range
        args.date_from = f"{args.baseline_start}-01-01"
        args.date_to = f"{args.baseline_start}-01-01"
    elif args.future_only:
        if not (args.baseline_start and args.baseline_end):
            print("Error: --future-only requires --baseline-start and --baseline-end (used as future period)")
            sys.exit(1)
        # Use future period as dummy date range
        args.date_from = f"{args.baseline_start}-01-01"
        args.date_to = f"{args.baseline_start}-01-01"
    else:
        if not (args.date_from and args.date_to):
            print("Error: --date-from and --date-to are required (unless using --baseline-only or --future-only)")
            sys.exit(1)

    baseline_years = None
    future_years = None

    if args.baseline_only:
        baseline_years = (args.baseline_start, args.baseline_end)
    elif args.future_only:
        future_years = (args.baseline_start, args.baseline_end)
    else:
        if args.baseline_start and args.baseline_end:
            baseline_years = (args.baseline_start, args.baseline_end)
        if args.future_start and args.future_end:
            future_years = (args.future_start, args.future_end)

    result = analyze_season(
        location_coord=location,
        date_range=(args.date_from, args.date_to),
        gap_days=args.gap_days,
        min_season_days=args.min_season_days,
        baseline_years=baseline_years,
        future_years=future_years,
        climate_model=args.climate_model,
        scenario=args.scenario
    )

    # Filter output if baseline-only or future-only mode
    if args.baseline_only:
        result = {
            'location': result['location'],
            'baseline_average': result.get('baseline_average'),
            'baseline_period': result.get('baseline_period'),
            'baseline_data_source': result.get('baseline_data_source'),
            'method': result['method'],
            'analysis_date': result['analysis_date']
        }
    elif args.future_only:
        result = {
            'location': result['location'],
            'future_average': result.get('future_average'),
            'future_period': result.get('future_period'),
            'future_model': result.get('future_model'),
            'future_scenario': result.get('future_scenario'),
            'future_data_source': result.get('future_data_source'),
            'method': result['method'],
            'analysis_date': result['analysis_date']
        }

    if args.show_data and 'error' not in result and not args.baseline_only and not args.future_only:
        print("=== DAILY CLIMATE DATA ===")
        lat, lon = location
        # Actual analysis always uses Open-Meteo
        df = _get_open_meteo_data(lat, lon, args.date_from, args.date_to)
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

# python -m climate_tookit.season_analysis.seasons --location="-1.286,36.817" --baseline-start 1991 --baseline-end 2020 --baseline-only

# python -m climate_tookit.season_analysis.seasons --location="-1.286,36.817" --baseline-start 2021 --baseline-end 2045 --scenario ssp245 --future-only