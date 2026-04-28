"""
Calculate Hazards Module

Retrieves crop hazard indices at a specific location by:
1. Using season_analysis to detect growing seasons or accepting season dates
2. Calculating total precipitation and average temperature for the season
3. Evaluating crop-specific hazard thresholds
4. Analyzing dry spell patterns

Dependencies: pandas, season_analysis.seasons module
"""

import sys
import os
from datetime import date, datetime
from typing import Dict, List, Any, Tuple, Optional
import pandas as pd
import json
import argparse

current_dir  = os.path.dirname(os.path.abspath(__file__)) 
parent_dir   = os.path.dirname(current_dir)                 
project_root = os.path.dirname(parent_dir)                   

if project_root not in sys.path:
    sys.path.insert(0, project_root)

SEASON_ANALYSIS_AVAILABLE = False
_IMPORT_ERROR: str = ""

try:
    from climate_tookit.season_analysis.seasons import (
        get_climate_data,
        add_et0,
        detect_onset_cessation,
        fetch_and_analyze_years,
        fetch_and_analyze_years_fixed,
        parse_fixed_seasons,
    )
    SEASON_ANALYSIS_AVAILABLE = True
except Exception as _e:
    _IMPORT_ERROR = str(_e)
    print(f"Warning: Season analysis module not available -- {_e}")

# Crop thresholds
CROP_THRESHOLDS = {
    'Beans':      {
        'Total Precip': {'no_stress': (500, 2000),  'moderate_stress_low': (300, 500),   'moderate_stress_up': (2000, 4300), 'severe_stress_low': (None, 300),   'severe_stress_up': (4300, None)},
        'TAVG':         {'no_stress': (18, 30),     'moderate_stress_low': (7, 18),      'moderate_stress_up': (30, 32),    'severe_stress_low': (None, 7),     'severe_stress_up': (32, None)},
    },
    'Maize':      {
        'Total Precip': {'no_stress': (500, 1200),  'moderate_stress_low': (400, 500),   'moderate_stress_up': (1200, 1800),'severe_stress_low': (None, 400),   'severe_stress_up': (1800, None)},
        'TAVG':         {'no_stress': (18, 32),     'moderate_stress_low': (14, 18),     'moderate_stress_up': (32, 40),    'severe_stress_low': (None, 14),    'severe_stress_up': (40, None)},
    },
    'Millet':     {
        'Total Precip': {'no_stress': (300, 600),   'moderate_stress_low': (200, 300),   'moderate_stress_up': (600, 1700), 'severe_stress_low': (None, 200),   'severe_stress_up': (1700, None)},
        'TAVG':         {'no_stress': (16, 32),     'moderate_stress_low': (12, 16),     'moderate_stress_up': (32, 40),    'severe_stress_low': (None, 12),    'severe_stress_up': (40, None)},
    },
    'Groundnuts': {
        'Total Precip': {'no_stress': (400, 1100),  'moderate_stress_low': (200, 400),   'moderate_stress_up': (1100, 1900),'severe_stress_low': (None, 200),   'severe_stress_up': (1900, None)},
        'TAVG':         {'no_stress': (22, 28),     'moderate_stress_low': (18, 22),     'moderate_stress_up': (28, 30),    'severe_stress_low': (None, 18),    'severe_stress_up': (30, None)},
    },
    'Sorghum':    {
        'Total Precip': {'no_stress': (400, 900),   'moderate_stress_low': (150, 400),   'moderate_stress_up': (900, 1400), 'severe_stress_low': (None, 150),   'severe_stress_up': (1400, None)},
        'TAVG':         {'no_stress': (21, 32),     'moderate_stress_low': (8, 21),      'moderate_stress_up': (32, 40),    'severe_stress_low': (None, 8),     'severe_stress_up': (40, None)},
    },
    'Cassava':    {
        'Total Precip': {'no_stress': (1400, 1800), 'moderate_stress_low': (500, 1400),  'moderate_stress_up': (1800, 5000),'severe_stress_low': (None, 500),   'severe_stress_up': (5000, None)},
        'TAVG':         {'no_stress': (20, 29),     'moderate_stress_low': (10, 20),     'moderate_stress_up': (29, 35),    'severe_stress_low': (None, 10),    'severe_stress_up': (35, None)},
    },
    'Rice':       {
        'Total Precip': {'no_stress': (1500, 2000), 'moderate_stress_low': (1000, 1500), 'moderate_stress_up': (2000, 4000),'severe_stress_low': (None, 1000),  'severe_stress_up': (4000, None)},
        'TAVG':         {'no_stress': (20, 30),     'moderate_stress_low': (10, 20),     'moderate_stress_up': (30, 36),    'severe_stress_low': (None, 10),    'severe_stress_up': (36, None)},
    },
}

# Climate data helpers
def get_climate_data_for_season(
    lat: float, lon: float, start_date: str, end_date: str
) -> pd.DataFrame:
    """Fetch daily climate data for an explicit window and attach ET0."""
    if not SEASON_ANALYSIS_AVAILABLE:
        raise Exception(
            f"Season analysis module not available -- {_IMPORT_ERROR}\n"
            "Ensure seasons.py and its dependencies (fetch_data, etc.) are importable."
        )
    df = get_climate_data(lat, lon, start_date, end_date)
    if df.empty:
        raise Exception(f"No climate data returned for {start_date} -> {end_date}")
    df = add_et0(df, lat) 
    return df

# Dry-spell detection
def detect_dry_spells(
    df: pd.DataFrame,
    min_dry_days: int = 7,
    precip_threshold: float = 1.0,
) -> List[Dict[str, Any]]:
    precip_col = next(
        (c for c in ['precipitation', 'precip', 'total_precipitation'] if c in df.columns),
        None,
    )
    if not precip_col or 'date' not in df.columns:
        return []

    df = df.sort_values('date').copy()
    df['is_dry'] = df[precip_col] < precip_threshold

    dry_spells: List[Dict[str, Any]] = []
    spell_start = None
    spell_days  = 0

    for idx, row in df.iterrows():
        if row['is_dry']:
            spell_start = spell_start or row['date']
            spell_days += 1
        else:
            if spell_start and spell_days >= min_dry_days:
                prev_loc = df.index.get_loc(idx) - 1
                dry_spells.append({
                    'start_date':  spell_start,
                    'end_date':    df.iloc[prev_loc]['date'],
                    'length_days': spell_days,
                })
            spell_start = None
            spell_days  = 0
            
    if spell_start and spell_days >= min_dry_days:
        dry_spells.append({
            'start_date':  spell_start,
            'end_date':    df.iloc[-1]['date'],
            'length_days': spell_days,
        })
    return dry_spells

def calculate_dry_spell_statistics(dry_spells: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not dry_spells:
        return {
            'number_of_dry_spells':       0,
            'max_dry_spell_length_days':  0,
            'mean_dry_spell_length_days': 0.0,
            'dry_spells':                 [],
        }
    lengths = [s['length_days'] for s in dry_spells]
    dist: Dict[str, int] = {}
    for ln in lengths:
        key = f"{(ln // 10) * 10}-{(ln // 10) * 10 + 9}"
        dist[key] = dist.get(key, 0) + 1
    return {
        'number_of_dry_spells':       len(dry_spells),
        'max_dry_spell_length_days':  max(lengths),
        'mean_dry_spell_length_days': round(sum(lengths) / len(lengths), 2),
        'length_distribution':        dist,
        'dry_spells':                 dry_spells,
    }

# Season statistics
def calculate_season_statistics(df: pd.DataFrame) -> Dict[str, Any]:
    stats: Dict[str, Any] = {}

    precip_col = next(
        (c for c in ['precipitation', 'precip', 'total_precipitation'] if c in df.columns),
        None,
    )
    if precip_col:
        p = df[precip_col].copy()
        stats['total_precipitation_mm']      = float(p.sum())
        stats['mean_daily_precipitation_mm'] = float(p.mean())
        stats['max_daily_precipitation_mm']  = float(p.max())
        stats['rainy_days']                  = int((p >= 1.0).sum())
        stats['dry_days']                    = int((p < 1.0).sum())
        stats['dry_spell_statistics']        = calculate_dry_spell_statistics(
            detect_dry_spells(df, min_dry_days=7, precip_threshold=1.0)
        )
    tmax_col = next(
        (c for c in ['max_temperature', 'tmax', 'maximum_2m_air_temperature'] if c in df.columns),
        None,
    )
    tmin_col = next(
        (c for c in ['min_temperature', 'tmin', 'minimum_2m_air_temperature'] if c in df.columns),
        None,
    )
    if tmax_col and tmin_col:
        tmax = df[tmax_col].copy()
        tmin = df[tmin_col].copy()
        if tmax.mean() > 100:     
            tmax -= 273.15
            tmin -= 273.15
        tavg = (tmax + tmin) / 2
        stats['mean_temperature_c'] = float(tavg.mean())
        stats['mean_tmax_c']        = float(tmax.mean())
        stats['mean_tmin_c']        = float(tmin.mean())
        stats['max_temperature_c']  = float(tmax.max())
        stats['min_temperature_c']  = float(tmin.min())
    return stats

def evaluate_threshold(value: float, thresholds: Dict[str, Tuple]) -> str:
    for level, (lower, upper) in thresholds.items():
        if lower is None and value < upper:
            return level
        if upper is None and value > lower:
            return level
        if lower is not None and upper is not None and lower <= value <= upper:
            return level
    return 'unknown'

# Main hazard calculation
def calculate_hazards(
    crop_name:         str,
    location_coord:    Tuple[float, float],
    date_from:         str,
    date_to:           str,
    season_start:      Optional[str]  = None,
    season_end:        Optional[str]  = None,
    fixed_season:      Optional[str]  = None,
    source:            str            = 'auto',
    custom_thresholds: Optional[Dict] = None,
    gap_days:          int            = 30,
    min_season_days:   int            = 30,
) -> Dict[str, Any]:

    lat, lon = location_coord
    crop_normalized = crop_name.capitalize()
    if crop_normalized not in CROP_THRESHOLDS and not custom_thresholds:
        return {
            'error':           f'Unknown crop: {crop_name}. Available: {", ".join(CROP_THRESHOLDS.keys())}',
            'available_crops': list(CROP_THRESHOLDS.keys()),
        }
    thresholds = custom_thresholds or CROP_THRESHOLDS[crop_normalized]

    # Branch A: explicit --season-start / --season-end
    if season_start and season_end:
        print(f"Using provided season dates: {season_start} to {season_end}")
        df = get_climate_data_for_season(lat, lon, season_start, season_end)
        season_info = {
            'season_detected': True,
            'onset_date':      season_start,
            'cessation_date':  season_end,
            'length_days':     (datetime.fromisoformat(season_end) - datetime.fromisoformat(season_start)).days,
            'method':          'user_provided',
        }
        all_results = [{'season_info': season_info, 'df': df}]

    # Branch B: --fixed-season  (mirrors seasons.py fixed-season mode)
    elif fixed_season:
        if not SEASON_ANALYSIS_AVAILABLE:
            return {'error': f'Season analysis module not available -- {_IMPORT_ERROR}'}
        print(f"Fixed-season mode: {fixed_season}")
        try:
            fixed_defs = parse_fixed_seasons(fixed_season)
        except ValueError as exc:
            return {'error': f'Invalid --fixed-season argument: {exc}'}

        start_year = datetime.fromisoformat(date_from).year
        end_year   = datetime.fromisoformat(date_to).year

        seasons_dict, _ = fetch_and_analyze_years_fixed(
            lat, lon,
            fixed_seasons=fixed_defs,
            start_year=start_year,
            end_year=end_year,
            source=source,
        )
        all_results = []
        for year, seasons in sorted(seasons_dict.items()):
            for s in seasons:
                s_start = pd.to_datetime(s['onset']).strftime('%Y-%m-%d')
                s_end   = (
                    pd.to_datetime(s['cessation']).strftime('%Y-%m-%d')
                    if s.get('cessation') else date_to
                )
                season_info = {
                    'season_detected': True,
                    'onset_date':      s_start,
                    'cessation_date':  s_end,
                    'length_days':     s['length_days'],
                    'method':          'fixed_season',
                    'year':            year,
                }
                df = get_climate_data_for_season(lat, lon, s_start, s_end)
                all_results.append({'season_info': season_info, 'df': df})
        if not all_results:
            return {'error': 'No seasons produced by fixed-season mode for the given date range.'}

    # Branch C: auto-detect via fetch_and_analyze_years
    elif SEASON_ANALYSIS_AVAILABLE:
        print(f"Detecting growing season for {crop_name} at ({lat}, {lon})")
        start_year = datetime.fromisoformat(date_from).year
        end_year   = datetime.fromisoformat(date_to).year
        seasons_dict, _ = fetch_and_analyze_years(
            lat, lon, start_year=start_year, end_year=end_year, source=source
        )
        flat = [s for seasons in seasons_dict.values() for s in seasons]
        if not flat:
            return {
                'error': (
                    'No growing season detected. '
                    'Provide --season-start/--season-end or --fixed-season.'
                )
            }
        first   = flat[0]
        s_start = pd.to_datetime(first['onset']).strftime('%Y-%m-%d')
        s_end   = (
            pd.to_datetime(first['cessation']).strftime('%Y-%m-%d')
            if first.get('cessation') else date_to
        )
        season_info = {
            'season_detected': True,
            'onset_date':      s_start,
            'cessation_date':  s_end,
            'length_days':     first['length_days'],
            'method':          'rainfall_based',
        }
        df = get_climate_data_for_season(lat, lon, s_start, s_end)
        all_results = [{'season_info': season_info, 'df': df}]
    else:
        return {
            'error': (
                f'Season analysis not available and no season dates provided -- {_IMPORT_ERROR}'
            )
        }

    # Evaluate hazards for every resolved season
    assessments = []
    for entry in all_results:
        stats      = calculate_season_statistics(entry['df'])
        hazard_eval: Dict[str, Any] = {}

        if 'Total Precip' in thresholds and 'total_precipitation_mm' in stats:
            pv = stats['total_precipitation_mm']
            hazard_eval['precipitation'] = {
                'value_mm': round(pv, 2),
                'status':   evaluate_threshold(pv, thresholds['Total Precip']),
            }
        if 'TAVG' in thresholds and 'mean_temperature_c' in stats:
            tv = stats['mean_temperature_c']
            hazard_eval['temperature'] = {
                'value_c': round(tv, 2),
                'status':  evaluate_threshold(tv, thresholds['TAVG']),
            }
        assessments.append({
            'crop':              crop_name,
            'location':          {'latitude': lat, 'longitude': lon},
            'season_info':       entry['season_info'],
            'season_statistics': stats,
            'hazard_evaluation': hazard_eval,
        })

    # Single season -> flat dict; multiple -> wrapped list
    return assessments[0] if len(assessments) == 1 else {'assessments': assessments}

# Pretty printer
def _fmt_date(d) -> str:
    if isinstance(d, (date, datetime)):
        return d.strftime('%Y-%m-%d')
    return str(d)[:10]

def print_hazard_results(result: Dict[str, Any]) -> None:
    # Multi-season wrapper
    if 'assessments' in result:
        for i, a in enumerate(result['assessments'], 1):
            print(f"\n{'─'*70}")
            print(f"  Assessment {i} of {len(result['assessments'])}")
            print_hazard_results(a)
        return

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
    print(f"  Length: {season['length_days']} days{'':15} Method: {season.get('method', 'unknown')}")

    stats = result['season_statistics']

    if 'total_precipitation_mm' in stats:
        print(f"\n  Precipitation Statistics")
        print(f"  {'─'*66}")
        print(f"  {'Metric':<32} {'Value':>15}  Unit")
        print(f"  {'─'*32} {'─'*15}  {'─'*10}")
        print(f"  {'Total':<32} {stats['total_precipitation_mm']:>15.2f}  mm")
        print(f"  {'Daily Mean':<32} {stats['mean_daily_precipitation_mm']:>15.2f}  mm")
        print(f"  {'Daily Maximum':<32} {stats['max_daily_precipitation_mm']:>15.2f}  mm")
        print(f"  {'Rainy Days (>=1mm)':<32} {stats['rainy_days']:>15}  days")
        print(f"  {'Dry Days (<1mm)':<32} {stats['dry_days']:>15}  days")

    if 'dry_spell_statistics' in stats:
        ds = stats['dry_spell_statistics']
        print(f"\n  Dry Spell Statistics (>=7 consecutive days with <1mm rain)")
        print(f"  {'─'*66}")
        print(f"  {'Number of Dry Spells':<32} {ds['number_of_dry_spells']:>15}  spells")
        print(f"  {'Max Dry Spell Length':<32} {ds['max_dry_spell_length_days']:>15}  days")
        print(f"  {'Mean Dry Spell Length':<32} {ds['mean_dry_spell_length_days']:>15.2f}  days")

        if ds['dry_spells']:
            print(f"\n  Individual Dry Spells:")
            print(f"  {'─'*66}")
            print(f"  {'#':<4} {'Start Date':<13} {'End Date':<13} {'Length (days)':>15}")
            print(f"  {'─'*4} {'─'*13} {'─'*13} {'─'*15}")
            for i, spell in enumerate(ds['dry_spells'], 1):
                print(
                    f"  {i:<4} {_fmt_date(spell['start_date']):<13} "
                    f"{_fmt_date(spell['end_date']):<13} {spell['length_days']:>15}"
                )

        if ds.get('length_distribution'):
            print(f"\n  Length Distribution:")
            print(f"  {'─'*66}")
            for rng, cnt in sorted(ds['length_distribution'].items()):
                print(f"  {rng:<15} days: {cnt:>3} spell(s)")

    if 'mean_temperature_c' in stats:
        print(f"\n  Temperature Statistics")
        print(f"  {'─'*66}")
        print(f"  {'Metric':<32} {'Value':>15}  Unit")
        print(f"  {'─'*32} {'─'*15}  {'─'*10}")
        print(f"  {'Mean Temperature':<32} {stats['mean_temperature_c']:>15.2f}  deg C")
        print(f"  {'Mean Tmax':<32} {stats['mean_tmax_c']:>15.2f}  deg C")
        print(f"  {'Mean Tmin':<32} {stats['mean_tmin_c']:>15.2f}  deg C")
        print(f"  {'Maximum Recorded':<32} {stats['max_temperature_c']:>15.2f}  deg C")
        print(f"  {'Minimum Recorded':<32} {stats['min_temperature_c']:>15.2f}  deg C")

    hazards = result['hazard_evaluation']
    print(f"\n  Hazard Assessment")
    print(f"  {'─'*66}")
    print(f"  {'Indicator':<25} {'Value':>18}  Status")
    print(f"  {'─'*25} {'─'*18}  {'─'*20}")
    if 'precipitation' in hazards:
        p   = hazards['precipitation']
        sym = 'OK' if 'no_stress' in p['status'] else '!!' if 'moderate' in p['status'] else 'XX'
        print(f"  {'Precipitation':<25} {p['value_mm']:>16.2f} mm  [{sym}] {p['status'].replace('_', ' ').upper()}")
    if 'temperature' in hazards:
        t   = hazards['temperature']
        sym = 'OK' if 'no_stress' in t['status'] else '!!' if 'moderate' in t['status'] else 'XX'
        print(f"  {'Temperature':<25} {t['value_c']:>16.2f} degC [{sym}] {t['status'].replace('_', ' ').upper()}")

    print(f"\n{'='*70}\n")

# CLI
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Calculate crop hazard indices',
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        'crop', type=str,
        help='Crop name: Maize | Beans | Rice | Sorghum | Millet | Groundnuts | Cassava',
    )
    parser.add_argument('--location',  type=str, required=True,
                        help='Coordinates as "lat,lon"  e.g. "-1.286,36.817"')
    parser.add_argument('--date-from', type=str, required=True,
                        help='Analysis start date (YYYY-MM-DD)')
    parser.add_argument('--date-to',   type=str, required=True,
                        help='Analysis end date   (YYYY-MM-DD)')

    # season specification (mutually exclusive) 
    season_group = parser.add_mutually_exclusive_group()
    season_group.add_argument(
        '--season-start', type=str, default=None,
        help='Explicit season start (YYYY-MM-DD). Pair with --season-end.',
    )
    season_group.add_argument(
        '--fixed-season',
        type=str, default=None,
        metavar='MM-DD:MM-DD[,MM-DD:MM-DD]',
        help=(
            "Fixed calendar season window(s) applied to every year in the date range.\n"
            "Mirrors --fixed-season in seasons.py.\n\n"
            "Format : one or two 'onset:cessation' tokens as MM-DD:MM-DD,\n"
            "         comma-separated for two seasons per year.\n\n"
            "Examples:\n"
            "  Single season  : --fixed-season '03-01:06-30'\n"
            "  Two seasons    : --fixed-season '03-01:05-31,10-01:12-15'\n"
            "  Year-crossing  : --fixed-season '11-01:02-28'\n"
        ),
    )
    parser.add_argument('--season-end', type=str, default=None,
                        help='Explicit season end (YYYY-MM-DD). Pair with --season-start.')
    parser.add_argument(
        '--source',
        choices=['era_5', 'agera_5', 'chirps+chirts', 'auto'],
        default='auto',
        help=(
            "Climate data source (default: auto).\n"
            "  era_5         -- ERA5 reanalysis\n"
            "  agera_5       -- AgERA5 / ERA5-Land\n"
            "  chirps+chirts -- CHIRPS precipitation + CHIRTS temperature\n"
            "  auto          -- tries era_5 -> agera_5 -> chirps+chirts"
        ),
    )
    parser.add_argument('--gap-days',        type=int, default=30,
                        help='Dry-day gap used to end auto-detected season (default: 30)')
    parser.add_argument('--min-season-days', type=int, default=30,
                        help='Minimum season length for auto-detection (default: 30)')
    parser.add_argument('--format',          choices=['json', 'text'], default='text',
                        help='Output format (default: text)')
    parser.add_argument('--output',          type=str, default=None,
                        help='Save JSON result to this file path')
    args = parser.parse_args()

    # Validate explicit-season pair
    if bool(args.season_start) != bool(args.season_end):
        parser.error('--season-start and --season-end must be supplied together.')

    lat, lon = map(float, args.location.split(','))

    result = calculate_hazards(
        crop_name=args.crop,
        location_coord=(lat, lon),
        date_from=args.date_from,
        date_to=args.date_to,
        season_start=args.season_start,
        season_end=args.season_end,
        fixed_season=args.fixed_season,
        source=args.source,
        gap_days=args.gap_days,
        min_season_days=args.min_season_days,
    )
    if args.format == 'json':
        output_str = json.dumps(result, indent=2, default=str)
        print(output_str)
        if args.output:
            with open(args.output, 'w') as f:
                f.write(output_str)
    else:
        print_hazard_results(result)
        if args.output:
            with open(args.output, 'w') as f:
                f.write(json.dumps(result, indent=2, default=str))

# Explicit season dates (single season):
# python -m climate_tookit.calculate_hazards.hazards maize --location="-1.286,36.817" --date-from 2020-01-01 --date-to 2020-12-31 --season-start 2020-03-01 --season-end 2020-06-30

# Fixed single season:
# python -m climate_tookit.calculate_hazards.hazards maize --location="-1.286,36.817" --date-from 2018-01-01 --date-to 2022-12-31 --fixed-season "03-01:06-30" --source era_5

# Fixed two seasons:
# python -m climate_tookit.calculate_hazards.hazards beans --location="-1.286,36.817" --date-from 2018-01-01 --date-to 2022-12-31 --fixed-season "03-01:05-31,10-01:12-15" --source agera_5

# Fixed year-crossing season:
# python -m climate_tookit.calculate_hazards.hazards sorghum --location="-1.286,36.817" --date-from 2012-01-01 --date-to 2016-12-31 --fixed-season "11-01:02-28" --source chirps+chirts

# Auto-detect season (no season flag supplied):
# python -m climate_tookit.calculate_hazards.hazards maize --location="-1.286,36.817" --date-from 2020-01-01 --date-to 2020-12-31 --source auto

# JSON output to file:
# python -m climate_tookit.calculate_hazards.hazards rice --location="-1.286,36.817" --date-from 2019-01-01 --date-to 2021-12-31 --fixed-season "04-15:07-10" --source auto --format json --output results.json