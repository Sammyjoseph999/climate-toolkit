"""
Season Analysis Module

Detects agricultural growing seasons from daily precipitation and temperature data.
Applies the Hargreaves ET0 method to identify planting season onset and cessation
based on whether precipitation meets or exceeds 50% of reference evapotranspiration.

Data source priority:
    Historical : ERA5 → AgERA5 → CHIRPS + CHIRTS (fallback)
    Future     : NEX-GDDP-CMIP6 only

Dependencies: pandas, climate_toolkit
"""

import pandas as pd
import math
import json
import argparse
import sys
from datetime import datetime, date
from typing import Tuple, Dict, List, Any, Optional
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fetch_data.preprocess_data.preprocess_data import preprocess_data

HISTORICAL_SOURCES = ['era_5', 'agera_5']
FUTURE_SOURCE = 'nex_gddp'
FALLBACK_COMBO = ('chirps', 'chirts')


def get_climate_data(
    lat: float,
    lon: float,
    start_date: str,
    end_date: str,
    use_projections: bool = False,
    model: str = 'GFDL-ESM4',
    scenario: str = 'ssp245',
    force_source: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch standardised daily climate data from the climate toolkit.

    Args:
        lat: Latitude in decimal degrees.
        lon: Longitude in decimal degrees.
        start_date: Start date as YYYY-MM-DD.
        end_date: End date as YYYY-MM-DD.
        use_projections: When True, retrieve NEX-GDDP future projections.
        model: NEX-GDDP climate model identifier.
        scenario: SSP scenario string (ssp126 / ssp245 / ssp370 / ssp585).
        force_source: Override automatic source selection.

    Returns:
        DataFrame with columns: date, tmax, tmin, precip.

    Raises:
        RuntimeError: When no data source returns a valid result.
    """
    date_from = date.fromisoformat(start_date)
    date_to = date.fromisoformat(end_date)

    df = _fetch_raw(lat, lon, date_from, date_to, use_projections, model, scenario, force_source)

    if df is None or df.empty:
        raise RuntimeError("All data sources exhausted — no climate data retrieved.")

    result = pd.DataFrame()
    result['date'] = pd.to_datetime(df['date'])
    result['tmax'] = df.get('max_temperature')
    result['tmin'] = df.get('min_temperature')
    result['precip'] = df.get('precipitation')
    return result


def _fetch_raw(
    lat: float,
    lon: float,
    date_from: date,
    date_to: date,
    use_projections: bool,
    model: str,
    scenario: str,
    force_source: Optional[str],
) -> Optional[pd.DataFrame]:
    coord = (lat, lon)

    if force_source == 'chirps+chirts':
        return _merge_chirps_chirts(coord, date_from, date_to)

    if force_source == 'nex_gddp' or use_projections:
        return preprocess_data(
            source=FUTURE_SOURCE,
            location_coord=coord,
            date_from=date_from,
            date_to=date_to,
            model=model,
            scenario=scenario,
        )

    if force_source:
        return preprocess_data(
            source=force_source,
            location_coord=coord,
            date_from=date_from,
            date_to=date_to,
        )

    for source in HISTORICAL_SOURCES:
        try:
            df = preprocess_data(source=source, location_coord=coord, date_from=date_from, date_to=date_to)
            if not df.empty and 'precipitation' in df.columns:
                return df
        except Exception:
            continue

    return _merge_chirps_chirts(coord, date_from, date_to)


def _merge_chirps_chirts(coord: Tuple[float, float], date_from: date, date_to: date) -> pd.DataFrame:
    df_precip = preprocess_data(source=FALLBACK_COMBO[0], location_coord=coord, date_from=date_from, date_to=date_to)
    df_temp = preprocess_data(source=FALLBACK_COMBO[1], location_coord=coord, date_from=date_from, date_to=date_to)
    return pd.merge(df_precip, df_temp, on='date', how='inner')


def calculate_et0(tmin: float, tmax: float, lat: float, date_val: datetime) -> float:
    """
    Estimate reference evapotranspiration via the Hargreaves equation.

    Args:
        tmin: Daily minimum temperature in Celsius.
        tmax: Daily maximum temperature in Celsius.
        lat: Latitude in decimal degrees.
        date_val: Calendar date used to derive solar geometry.

    Returns:
        ET0 in mm/day, or 0 when inputs are invalid.
    """
    if tmax is None or tmin is None or pd.isna(tmax) or pd.isna(tmin) or tmax < tmin:
        return 0.0

    J = date_val.timetuple().tm_yday
    lat_rad = math.radians(lat)
    sol_decl = 0.409 * math.sin((2 * math.pi / 365) * J - 1.39)
    ird = 1 + 0.033 * math.cos((2 * math.pi / 365) * J)
    sha = math.acos(max(min(-math.tan(lat_rad) * math.tan(sol_decl), 1), -1))

    ra = (
        (24 * 60 / math.pi)
        * 0.0820
        * ird
        * (
            sha * math.sin(lat_rad) * math.sin(sol_decl)
            + math.cos(lat_rad) * math.cos(sol_decl) * math.sin(sha)
        )
    )

    return 0.0023 * math.sqrt(tmax - tmin) * ((tmax + tmin) / 2 + 17.8) * ra


def detect_seasons(
    df: pd.DataFrame,
    gap_days: int = 30,
    min_season_days: int = 30,
) -> List[Dict]:
    """
    Identify growing seasons where precipitation meets the ET0 threshold.

    A season opens when daily precip >= 0.5 x ET0 and closes after
    `gap_days` consecutive sub-threshold days.

    Args:
        df: DataFrame with columns: date, precip, et0.
        gap_days: Consecutive dry days required to close a season.
        min_season_days: Seasons shorter than this are discarded.

    Returns:
        List of season dicts: onset_date, cessation_date, onset_doy,
        cessation_doy, length_days.
    """
    df = df.copy()
    df['threshold'] = df['et0'] * 0.5
    df['rainy_day'] = df['precip'] >= df['threshold']

    seasons: List[Dict] = []
    i, n = 0, len(df)

    while i < n:
        if not df.iloc[i]['rainy_day']:
            i += 1
            continue

        onset_date = df.iloc[i]['date']
        dry_counter = 0
        cessation_date = None
        j = i + 1

        while j < n:
            if df.iloc[j]['rainy_day']:
                dry_counter = 0
            else:
                dry_counter += 1
                if dry_counter >= gap_days:
                    cessation_date = df.iloc[j - gap_days]['date']
                    break
            j += 1

        if cessation_date is None:
            cessation_date = df.iloc[-1]['date']

        length = (cessation_date - onset_date).days + 1
        if length >= min_season_days:
            seasons.append({
                'onset_date':     onset_date.strftime('%Y-%m-%d'),
                'cessation_date': cessation_date.strftime('%Y-%m-%d'),
                'onset_doy':      onset_date.timetuple().tm_yday,
                'cessation_doy':  cessation_date.timetuple().tm_yday,
                'length_days':    length,
            })

        try:
            i = df[df['date'] == cessation_date].index[0] + 1
        except IndexError:
            break

    return seasons


def calculate_average_season(seasons: List[Dict]) -> Optional[Dict[str, Any]]:
    """
    Compute mean onset DOY, cessation DOY, and season length.

    Args:
        seasons: Detected season records as returned by detect_seasons.

    Returns:
        Dict of averages, or None when the input list is empty.
    """
    if not seasons:
        return None
    n = len(seasons)
    return {
        'avg_onset_doy':     sum(s['onset_doy'] for s in seasons) / n,
        'avg_cessation_doy': sum(s['cessation_doy'] for s in seasons) / n,
        'avg_length_days':   sum(s['length_days'] for s in seasons) / n,
        'season_count':      n,
    }


def analyze_season(
    location_coord: Tuple[float, float],
    date_range: Tuple[str, str],
    gap_days: int = 30,
    min_season_days: int = 30,
    baseline_years: Optional[Tuple[int, int]] = None,
    future_years: Optional[Tuple[int, int]] = None,
    climate_model: str = 'GFDL-ESM4',
    scenario: str = 'ssp245',
    source: str = 'auto',
) -> Dict[str, Any]:
    """
    Full season analysis for a primary period with optional baseline and future averages.

    Args:
        location_coord: (lat, lon) in decimal degrees.
        date_range: (start_date, end_date) as YYYY-MM-DD strings.
        gap_days: Consecutive dry days to close a season.
        min_season_days: Minimum valid season length in days.
        baseline_years: (start_year, end_year) for historical average computation.
        future_years: (start_year, end_year) for NEX-GDDP projection average.
        climate_model: NEX-GDDP model identifier.
        scenario: SSP scenario string.
        source: Data source override; 'auto' applies priority-order selection.

    Returns:
        Analysis result dict, or error dict on failure.
    """
    lat, lon = location_coord
    start_date, end_date = date_range
    force_source = None if source == 'auto' else source

    try:
        df = get_climate_data(lat, lon, start_date, end_date, use_projections=False, force_source=force_source)
        df['et0'] = [calculate_et0(r['tmin'], r['tmax'], lat, r['date']) for _, r in df.iterrows()]
        seasons = detect_seasons(df, gap_days, min_season_days)

        result: Dict[str, Any] = {
            'location':         {'lat': lat, 'lon': lon},
            'actual_period':    {'start': start_date, 'end': end_date},
            'seasons_detected': len(seasons),
            'seasons':          seasons,
            'main_season':      max(seasons, key=lambda x: x['length_days']) if seasons else None,
            'method':           'ET0_precipitation_threshold',
            'data_source':      'Climate Toolkit (ERA5/AgERA5/CHIRPS+CHIRTS)',
            'analysis_date':    datetime.now().isoformat(),
        }

        if baseline_years:
            result.update(_compute_period_average(
                lat, lon, *baseline_years, gap_days, min_season_days,
                use_projections=False,
                prefix='baseline',
                source_label='Climate Toolkit (ERA5/AgERA5)',
            ))

        if future_years:
            result.update(_compute_period_average(
                lat, lon, *future_years, gap_days, min_season_days,
                use_projections=True,
                model=climate_model,
                scenario=scenario,
                prefix='future',
                source_label='NEX-GDDP-CMIP6',
                model_label=climate_model,
                scenario_label=scenario,
            ))

        return result

    except Exception as exc:
        return {
            'error':         str(exc),
            'location':      {'lat': lat, 'lon': lon},
            'actual_period': {'start': start_date, 'end': end_date},
        }


def _compute_period_average(
    lat: float,
    lon: float,
    year_start: int,
    year_end: int,
    gap_days: int,
    min_season_days: int,
    use_projections: bool = False,
    model: str = 'GFDL-ESM4',
    scenario: str = 'ssp245',
    prefix: str = 'baseline',
    source_label: str = '',
    model_label: Optional[str] = None,
    scenario_label: Optional[str] = None,
) -> Dict[str, Any]:
    all_seasons: List[Dict] = []

    for year in range(year_start, year_end + 1):
        try:
            df_year = get_climate_data(
                lat, lon, f"{year}-01-01", f"{year}-12-31",
                use_projections=use_projections, model=model, scenario=scenario,
            )
            df_year['et0'] = [calculate_et0(r['tmin'], r['tmax'], lat, r['date']) for _, r in df_year.iterrows()]
            all_seasons.extend(detect_seasons(df_year, gap_days, min_season_days))
        except Exception:
            continue

    out: Dict[str, Any] = {
        f'{prefix}_average':     calculate_average_season(all_seasons),
        f'{prefix}_period':      {'start': year_start, 'end': year_end},
        f'{prefix}_data_source': source_label,
    }
    if model_label:
        out[f'{prefix}_model'] = model_label
    if scenario_label:
        out[f'{prefix}_scenario'] = scenario_label
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description='Season analysis using climate data')
    parser.add_argument('--location', required=True, help='Coordinates as "lat,lon"')
    parser.add_argument('--source', choices=['era_5', 'agera_5', 'nex_gddp', 'chirps+chirts', 'auto'], default='auto')
    parser.add_argument('--date-from')
    parser.add_argument('--date-to')
    parser.add_argument('--baseline-start', type=int)
    parser.add_argument('--baseline-end', type=int)
    parser.add_argument('--baseline-only', action='store_true')
    parser.add_argument('--future-start', type=int)
    parser.add_argument('--future-end', type=int)
    parser.add_argument('--future-only', action='store_true')
    parser.add_argument('--climate-model', default='GFDL-ESM4')
    parser.add_argument('--scenario', default='ssp245', choices=['ssp126', 'ssp245', 'ssp370', 'ssp585'])
    parser.add_argument('--gap-days', type=int, default=30)
    parser.add_argument('--min-season-days', type=int, default=30)
    parser.add_argument('--output', help='Save JSON result to this path')
    parser.add_argument('--download-data', help='Save daily climate CSV to this path')
    parser.add_argument('--show-data', action='store_true')
    args = parser.parse_args()

    try:
        lat, lon = map(float, args.location.split(','))
    except ValueError:
        print("Error: --location must be in 'lat,lon' format.")
        sys.exit(1)

    if args.baseline_only and args.future_only:
        print("Error: --baseline-only and --future-only are mutually exclusive.")
        sys.exit(1)

    if args.baseline_only:
        if not (args.baseline_start and args.baseline_end):
            print("Error: --baseline-only requires --baseline-start and --baseline-end.")
            sys.exit(1)
        args.date_from = args.date_to = f"{args.baseline_start}-01-01"
    elif args.future_only:
        if not (args.future_start and args.future_end):
            print("Error: --future-only requires --future-start and --future-end.")
            sys.exit(1)
        args.date_from = args.date_to = f"{args.future_start}-01-01"
    else:
        if not (args.date_from and args.date_to):
            print("Error: --date-from and --date-to are required.")
            sys.exit(1)

    baseline_years = (args.baseline_start, args.baseline_end) if (args.baseline_start and args.baseline_end) else None
    future_years = (args.future_start, args.future_end) if (args.future_start and args.future_end) else None

    if args.baseline_only:
        baseline_years, future_years = (args.baseline_start, args.baseline_end), None
    elif args.future_only:
        baseline_years, future_years = None, (args.future_start, args.future_end)

    result = analyze_season(
        location_coord=(lat, lon),
        date_range=(args.date_from, args.date_to),
        gap_days=args.gap_days,
        min_season_days=args.min_season_days,
        baseline_years=baseline_years,
        future_years=future_years,
        climate_model=args.climate_model,
        scenario=args.scenario,
        source=args.source,
    )

    if args.download_data and not args.baseline_only and not args.future_only:
        try:
            force_source = None if args.source == 'auto' else args.source
            df_dl = get_climate_data(lat, lon, args.date_from, args.date_to, force_source=force_source)
            df_dl['et0'] = [calculate_et0(r['tmin'], r['tmax'], lat, r['date']) for _, r in df_dl.iterrows()]
            df_dl['threshold'] = df_dl['et0'] * 0.5
            df_dl['rainy_day'] = df_dl['precip'] >= df_dl['threshold']
            df_dl.to_csv(args.download_data, index=False)
            print(f"Data saved to {args.download_data}")
        except Exception as exc:
            print(f"Failed to save data: {exc}")

    if args.baseline_only:
        result = {k: result[k] for k in ('location', 'baseline_average', 'baseline_period',
                                          'baseline_data_source', 'method', 'analysis_date') if k in result}
    elif args.future_only:
        result = {k: result[k] for k in ('location', 'future_average', 'future_period', 'future_model',
                                          'future_scenario', 'future_data_source', 'method', 'analysis_date') if k in result}

    if args.show_data and 'error' not in result and not args.baseline_only and not args.future_only:
        force_source = None if args.source == 'auto' else args.source
        df_show = get_climate_data(lat, lon, args.date_from, args.date_to, force_source=force_source)
        df_show['et0'] = [calculate_et0(r['tmin'], r['tmax'], lat, r['date']) for _, r in df_show.iterrows()]
        df_show['threshold'] = df_show['et0'] * 0.5
        df_show['rainy_day'] = df_show['precip'] >= df_show['threshold']
        print("\n=== DAILY CLIMATE DATA ===")
        print(df_show.head(10).to_string())
        print(f"... ({len(df_show)} total records)")
        print(df_show.tail(10).to_string())
        print("\n=== SEASON ANALYSIS RESULTS ===")

    output = json.dumps(result, indent=2, default=str)

    if args.output:
        with open(args.output, 'w') as fh:
            fh.write(output)
        print(f"Results saved to {args.output}")
    else:
        print(output)


if __name__ == '__main__':
    main()


# Force specific source
# python climate_tookit/season_analysis/seasons.py --location="-1.286,36.817" --date-from 2020-01-01 --date-to 2020-12-31 --source era_5
# python climate_tookit/season_analysis/seasons.py --location="-1.286,36.817" --date-from 2020-01-01 --date-to 2020-12-31 --source agera_5
# python climate_tookit/season_analysis/seasons.py --location="-1.286,36.817" --date-from 2020-01-01 --date-to 2020-12-31 --source chirps+chirts

# Combined analysis with data download
# python climate_tookit/season_analysis/seasons.py --location="-1.286,36.817" --date-from 2020-01-01 --date-to 2020-12-31 --source era_5 --download-data data.csv --output results.json --show-data