"""
Season Analysis Module

Detects agricultural growing seasons based on daily precipitation and temperature data.
Uses ET0 calculations and precipitation thresholds to identify planting season onset
and cessation dates.

The module fetches climate data from the climate toolkit (ERA5, AgERA5, NEX-GDDP, CHIRPS+CHIRTS)
and applies the Hargreaves method for evapotransporation calculations to determine when
precipitation exceeds 50% of ET0, indicating favorable conditions for crop growth.

Priority sources:
- Historical: ERA5 → AgERA5 → CHIRPS+CHIRTS
- Future: NEX-GDDP (climate projections)

Dependencies: pandas, climate_toolkit
"""

import pandas as pd
import math
import json
import argparse
import sys
import os
from datetime import datetime, timedelta, date
from typing import Tuple, Dict, List, Any
from pathlib import Path

current_dir = Path(__file__).parent
toolkit_root = current_dir.parent
sys.path.insert(0, str(toolkit_root))

from fetch_data.preprocess_data.preprocess_data import preprocess_data

# Source priorities
HISTORICAL_SOURCES = ['era_5', 'agera_5']
FUTURE_SOURCE = 'nex_gddp' 
FALLBACK_COMBO = ['chirps', 'chirts']


def get_climate_data(lat: float, lon: float, start_date: str, end_date: str,
                     use_projections: bool = False, model: str = 'GFDL-ESM4',
                     scenario: str = 'ssp245', force_source: str = None) -> pd.DataFrame:
    """
    Fetch daily climate data using climate toolkit.

    Args:
        lat (float): Latitude in decimal degrees
        lon (float): Longitude in decimal degrees
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
        use_projections (bool): If True, use NEX-GDDP for future projections
        model (str): Climate model for NEX-GDDP (default: GFDL-ESM4)
        scenario (str): SSP scenario for NEX-GDDP (default: ssp245)
        force_source (str): Force specific source ('era_5', 'agera_5', 'nex_gddp', 'chirps+chirts')

    Returns:
        pd.DataFrame: DataFrame with columns [date, tmax, tmin, precip]

    Raises:
        Exception: If data retrieval fails
    """
    date_from = date.fromisoformat(start_date)
    date_to = date.fromisoformat(end_date)

    # If source is forced, use only that source
    if force_source:
        if force_source == 'chirps+chirts':
            print("Using CHIRPS (precip) + CHIRTS (temp) combination...")
            df_precip = preprocess_data(
                source='chirps',
                location_coord=(lat, lon),
                date_from=date_from,
                date_to=date_to
            )
            df_temp = preprocess_data(
                source='chirts',
                location_coord=(lat, lon),
                date_from=date_from,
                date_to=date_to
            )
            df = pd.merge(df_precip, df_temp, on='date', how='inner')
        elif force_source == 'nex_gddp':
            print(f"Using NEX-GDDP: model={model}, scenario={scenario}")
            df = preprocess_data(
                source='nex_gddp',
                location_coord=(lat, lon),
                date_from=date_from,
                date_to=date_to,
                model=model,
                scenario=scenario
            )
        else:
            print(f"Using {force_source}...")
            df = preprocess_data(
                source=force_source,
                location_coord=(lat, lon),
                date_from=date_from,
                date_to=date_to
            )
    # For future projections, use NEX-GDDP only
    elif use_projections:
        print(f"Fetching NEX-GDDP data: model={model}, scenario={scenario}")
        df = preprocess_data(
            source=FUTURE_SOURCE,
            location_coord=(lat, lon),
            date_from=date_from,
            date_to=date_to,
            model=model,
            scenario=scenario
        )
    else:
        # For historical data, try sources in priority order
        df = None
        for source in HISTORICAL_SOURCES:
            try:
                print(f"Trying {source}...")
                df = preprocess_data(
                    source=source,
                    location_coord=(lat, lon),
                    date_from=date_from,
                    date_to=date_to
                )
                if not df.empty and 'precipitation' in df.columns:
                    print(f"✓ Using {source}")
                    break
            except Exception as e:
                print(f"✗ {source} failed: {e}")
                continue

        # Fallback to CHIRPS + CHIRTS combination
        if df is None or df.empty:
            print("Using CHIRPS (precip) + CHIRTS (temp) combination...")
            df_precip = preprocess_data(
                source=FALLBACK_COMBO[0],
                location_coord=(lat, lon),
                date_from=date_from,
                date_to=date_to
            )
            df_temp = preprocess_data(
                source=FALLBACK_COMBO[1],
                location_coord=(lat, lon),
                date_from=date_from,
                date_to=date_to
            )
            # Merge on date
            df = pd.merge(df_precip, df_temp, on='date', how='inner')

    if df is None or df.empty:
        raise Exception("Failed to retrieve climate data from all sources")

    # Standardize column names
    result = pd.DataFrame()
    result['date'] = pd.to_datetime(df['date'])
    result['tmax'] = df.get('max_temperature')
    result['tmin'] = df.get('min_temperature')
    result['precip'] = df.get('precipitation')

    return result


def calculate_et0(tmin: float, tmax: float, lat: float, date_val: datetime) -> float:
    """
    Calculate reference evapotranspiration using the Hargreaves method.

    The Hargreaves method estimates ET0 based on temperature range and
    extraterrestrial radiation, suitable for areas with limited weather data.

    Args:
        tmin (float): Minimum daily temperature in Celsius
        tmax (float): Maximum daily temperature in Celsius
        lat (float): Latitude in decimal degrees
        date_val (datetime): Date for solar radiation calculation

    Returns:
        float: Reference evapotranspiration in mm/day, returns 0 if invalid inputs
    """
    if tmax is None or tmin is None or pd.isna(tmax) or pd.isna(tmin) or tmax < tmin:
        return 0

    J = date_val.timetuple().tm_yday
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
    df['rainy_day'] = df['precip'] >= 1

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


def analyze_season(location_coord: Tuple[float, float],
                  date_range: Tuple[str, str],
                  gap_days: int = 30,
                  min_season_days: int = 30,
                  baseline_years: Tuple[int, int] = None,
                  future_years: Tuple[int, int] = None,
                  climate_model: str = 'GFDL-ESM4',
                  scenario: str = 'ssp245',
                  source: str = 'auto') -> Dict[str, Any]:
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
        # Determine source to use
        force_source = None if source == 'auto' else source

        # Primary period analysis (use historical sources or forced source)
        df = get_climate_data(lat, lon, start_date, end_date,
                             use_projections=False,
                             force_source=force_source)

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
            'data_source': 'Climate Toolkit (ERA5/AgERA5/CHIRPS+CHIRTS)',
            'analysis_date': datetime.now().isoformat()
        }

        # Calculate baseline average if requested (always use historical data)
        if baseline_years:
            baseline_start, baseline_end = baseline_years
            all_baseline_seasons = []

            print(f"\nCalculating baseline average for {baseline_start}-{baseline_end}...")
            for year in range(baseline_start, baseline_end + 1):
                try:
                    year_start = f"{year}-01-01"
                    year_end = f"{year}-12-31"
                    df_baseline = get_climate_data(lat, lon, year_start, year_end, use_projections=False)
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
            result['baseline_data_source'] = 'Climate Toolkit (ERA5/AgERA5)'

        # Calculate future average if requested (always use NEX-GDDP projections)
        if future_years:
            future_start, future_end = future_years
            all_future_seasons = []

            print(f"\nCalculating future average for {future_start}-{future_end} using NEX-GDDP...")
            for year in range(future_start, future_end + 1):
                try:
                    year_start = f"{year}-01-01"
                    year_end = f"{year}-12-31"
                    df_future = get_climate_data(lat, lon, year_start, year_end,
                                                 use_projections=True,
                                                 model=climate_model,
                                                 scenario=scenario)
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


def main():
    """Command-line interface for season analysis."""
    parser = argparse.ArgumentParser(description='Season analysis using climate data')

    parser.add_argument('--location', required=True,
                       help='Location as "lat,lon" (e.g., "-1.286,36.817")')
    parser.add_argument('--source',
                       choices=['era_5', 'agera_5', 'nex_gddp', 'chirps+chirts', 'auto'],
                       default='auto',
                       help='Data source: era_5, agera_5, nex_gddp, chirps+chirts, or auto (default: auto)')
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
    parser.add_argument('--download-data',
                       help='Download climate data to CSV file')
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
        args.date_from = f"{args.baseline_start}-01-01"
        args.date_to = f"{args.baseline_start}-01-01"
    elif args.future_only:
        if not (args.future_start and args.future_end):
            print("Error: --future-only requires --future-start and --future-end")
            sys.exit(1)
        args.date_from = f"{args.future_start}-01-01"
        args.date_to = f"{args.future_start}-01-01"
    else:
        if not (args.date_from and args.date_to):
            print("Error: --date-from and --date-to are required (unless using --baseline-only or --future-only)")
            sys.exit(1)

    baseline_years = None
    future_years = None

    if args.baseline_only:
        baseline_years = (args.baseline_start, args.baseline_end)
    elif args.future_only:
        future_years = (args.future_start, args.future_end)
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
        scenario=args.scenario,
        source=args.source
    )

    # Download raw climate data if requested
    if args.download_data and not args.baseline_only and not args.future_only:
        try:
            print(f"\nDownloading climate data to {args.download_data}...")
            lat, lon = location
            force_source = None if args.source == 'auto' else args.source
            df_download = get_climate_data(lat, lon, args.date_from, args.date_to,
                                          use_projections=False,
                                          force_source=force_source)
            # Add ET0 calculations
            df_download['et0'] = [calculate_et0(row['tmin'], row['tmax'], lat, row['date'])
                                 for _, row in df_download.iterrows()]
            df_download['threshold'] = df_download['et0'] * 0.5
            df_download['rainy_day'] = df_download['precip'] >= df_download['threshold']

            # Save to CSV
            df_download.to_csv(args.download_data, index=False)
            print(f"✓ Data saved to {args.download_data}")
        except Exception as e:
            print(f"✗ Failed to download data: {e}")

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
        print("\n=== DAILY CLIMATE DATA ===")
        lat, lon = location
        force_source = None if args.source == 'auto' else args.source
        df = get_climate_data(lat, lon, args.date_from, args.date_to,
                             use_projections=False,
                             force_source=force_source)
        et0_values = [calculate_et0(row['tmin'], row['tmax'], lat, row['date'])
                     for _, row in df.iterrows()]
        df['et0'] = et0_values
        df['threshold'] = df['et0'] * 0.5
        df['rainy_day'] = df['precip'] >= df['threshold']
        print(df.head(10))
        print("...")
        print(df.tail(10))
        print(f"Total records: {len(df)}")
        print("\n=== SEASON ANALYSIS RESULTS ===")

    output = json.dumps(result, indent=2, default=str)

    if args.output:
        with open(args.output, 'w') as f:
            f.write(output)
        print(f"\nResults saved to {args.output}")
    else:
        print(output)


if __name__ == "__main__":
    main()


# Auto-select source (tries ERA5 → AgERA5 → CHIRPS+CHIRTS)
# python climate_tookit/season_analysis/seasons.py --location="-1.286,36.817" --date-from 2020-01-01 --date-to 2020-12-31

# Force specific source
# python climate_tookit/season_analysis/seasons.py --location="-1.286,36.817" --date-from 2020-01-01 --date-to 2020-12-31 --source era_5
# python climate_tookit/season_analysis/seasons.py --location="-1.286,36.817" --date-from 2020-01-01 --date-to 2020-12-31 --source agera_5
# python climate_tookit/season_analysis/seasons.py --location="-1.286,36.817" --date-from 2016-01-01 --date-to 2016-12-31 --source chirps+chirts

# Download climate data to CSV
# python climate_tookit/season_analysis/seasons.py --location="-1.286,36.817" --date-from 2015-01-01 --date-to 2015-12-31 --source chirps+chirts --download-data chirps_chirts_nairobi_2015.csv 

# Baseline analysis
# python climate_tookit/season_analysis/seasons.py --location="-1.286,36.817" --baseline-start 1991 --baseline-end 2020 --baseline-only

# Future projections (uses NEX-GDDP)
# python climate_tookit/season_analysis/seasons.py --location="-1.286,36.817" --future-start 2040 --future-end 2050 --scenario ssp245 --future-only

# Combined analysis with data download
# python climate_tookit/season_analysis/seasons.py --location="-1.286,36.817" --date-from 2020-01-01 --date-to 2020-12-31 --source era_5 --download-data data.csv --output results.json --show-data