"""
Climate Statistics Module
Computes agroecology-focused climate statistics by season.
Supports both automatic detection (ETO-based, from seasons.py) and fixed-season calendar windows (--fixed-season), matching the season_analysis interface.

Outputs three views per run, all sliced per detected/fixed season (no full-period summaries):
    1. Raw Climate Summary by Season -- mean / min / max / std per core variable (precip, tmax, tmin, humidity, solar, wind), one block per season
    2. Overall Statistics by Season  -- essential agro metrics, one block per season
    3. Season Statistics             -- compact agro headline per season (plus ETO sub-seasons inside fixed windows)
Detection: delegates to seasons.py building blocks (add_et0,
detect_onset_cessation, reassign_spillover_seasons, remove_duplicate_seasons,
parse_fixed_seasons, check_humid) so behaviour is identical to seasons.py:
    Per reference year, a 1.5-year window is sliced from the master DataFrame so seasons crossing the year boundary are captured. After detection,
    seasons are reassigned to onset year, filtered to MAM/OND windows for equatorial climates, and de-duplicated.

Data sources accepted: era_5, agera_5, chirps+chirts, nasa_power, nex_gddp, terraclimate, auto. NEX-GDDP requires --model and --scenario.

Dependencies: pandas, numpy, climate_toolkit (preprocess_data, seasons.py)
"""

import os
import sys
import math
import json
import argparse
import warnings
from datetime import datetime, date
from pathlib import Path
from typing import Tuple, Dict, List, Any, Optional

import pandas as pd
import numpy as np

warnings.filterwarnings("ignore")

_current_dir = os.path.dirname(__file__)
_parent_dir  = os.path.dirname(_current_dir)
sys.path.append(os.path.join(_parent_dir, 'fetch_data', 'preprocess_data'))
sys.path.append(os.path.join(_parent_dir, 'season_analysis'))

# Pipeline import
try:
    from preprocess_data import preprocess_data
    PREPROCESS_AVAILABLE = True
except ImportError:
    PREPROCESS_AVAILABLE = False
    print("Warning: preprocess_data pipeline not available")

# Variable enum (optional)
try:
    from sources.utils.models import ClimateVariable
    CLIMATE_VARS = [
        ClimateVariable.precipitation,
        ClimateVariable.max_temperature,
        ClimateVariable.min_temperature,
        ClimateVariable.humidity,
        ClimateVariable.soil_moisture,
        ClimateVariable.solar_radiation,
        ClimateVariable.wind_speed,
    ]
except (ImportError, AttributeError):
    CLIMATE_VARS = [
        'precipitation', 'max_temperature', 'min_temperature',
        'humidity', 'soil_moisture', 'solar_radiation', 'wind_speed',
    ]

try:
    from seasons import (
        add_et0,
        parse_fixed_seasons,
        detect_onset_cessation,
        reassign_spillover_seasons,
        remove_duplicate_seasons,
        check_humid,
    )
    SEASONS_AVAILABLE = True
except ImportError as exc:
    SEASONS_AVAILABLE = False
    print(f"Warning: seasons.py not available -- {exc}")

# Constants
RENAME_MAP = {
    'precipitation':    'precip',
    'max_temperature':  'tmax',
    'min_temperature':  'tmin',
}

# Variables shown in the Raw Climate Summary table
SUMMARY_VARS: List[Tuple[str, str]] = [
    ('precip',          'Precipitation (mm/day)'),
    ('tmax',            'Max Temperature (°C)'),
    ('tmin',            'Min Temperature (°C)'),
    ('humidity',        'Humidity (%)'),
    ('solar_radiation', 'Solar Radiation (W/m²)'),
    ('wind_speed',      'Wind Speed (m/s)'),
]

# Data fetching
def _call_preprocess(source, lat, lon, date_from, date_to, model, scenario):
    """Single preprocess_data call -- isolates the kwargs handling."""
    return preprocess_data(
        source=source,
        location_coord=(lat, lon),
        variables=CLIMATE_VARS,
        date_from=date_from,
        date_to=date_to,
        model=model,
        scenario=scenario,
    )

def _fetch_chirps_chirts(lat, lon, date_from, date_to):
    """Merge CHIRPS (precip) + CHIRTS (temp). Other vars unavailable."""
    df_p = _call_preprocess('chirps', lat, lon, date_from, date_to, None, None)
    df_t = _call_preprocess('chirts', lat, lon, date_from, date_to, None, None)
    if df_p is None or df_p.empty:
        raise RuntimeError("CHIRPS returned no data")
    if df_t is None or df_t.empty:
        raise RuntimeError("CHIRTS returned no data")
    return pd.merge(df_p, df_t, on='date', how='inner')

def get_climate_data(
    lat: float, lon: float,
    start_date: str, end_date: str,
    source: str,
    model:    Optional[str] = None,
    scenario: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch all variables for [start_date, end_date] from the given source.
    Source handling
    ---------------
      - 'auto'           : resolves directly to CHIRPS + CHIRTS merge
      - 'chirps+chirts'  : merges CHIRPS precip + CHIRTS temperature
      - any other string : passed straight to preprocess_data (era_5, agera_5,
                           nasa_power, nex_gddp, chirps, chirts, …)
    Renames pipeline columns to canonical names: precip, tmax, tmin (humidity, soil_moisture, solar_radiation, wind_speed pass through when the source provides them).
    """
    if not PREPROCESS_AVAILABLE:
        raise RuntimeError("preprocess_data pipeline not available")

    date_from = date.fromisoformat(start_date)
    date_to   = date.fromisoformat(end_date)
    source_lc = source.lower()

    # Resolve source -> raw DataFrame
    if source_lc in ('chirps+chirts', 'auto'):
        label = "auto -> CHIRPS + CHIRTS" if source_lc == 'auto' else "CHIRPS + CHIRTS"
        print(f"  [source] {label}")
        df = _fetch_chirps_chirts(lat, lon, date_from, date_to)
    else:
        df = _call_preprocess(source, lat, lon, date_from, date_to, model, scenario)

    if df is None or df.empty:
        raise RuntimeError(f"No data returned from source '{source}'")

    df = df.rename(columns=RENAME_MAP).copy()
    df['date'] = pd.to_datetime(df['date'])

    # Minimum required for ET0 + water balance
    if 'precip' not in df.columns:
        print(f"  [WARN] No precipitation column from {source}; defaulting to 0")
        df['precip'] = 0.0
    if 'tmax' not in df.columns or 'tmin' not in df.columns:
        if source_lc == 'chirps':
            print("  [WARN] CHIRPS provides precipitation only -- defaulting tmax=25, tmin=15")
            df['tmax'] = 25.0
            df['tmin'] = 15.0
        else:
            available = [c for c in df.columns if c != 'date']
            raise RuntimeError(
                f"Temperature missing from '{source}'. Got: {available}"
            )
    return df.sort_values('date').reset_index(drop=True)

# Water balance
def calculate_water_balance(df: pd.DataFrame) -> pd.DataFrame:
    """
    Daily water balance:
      water_balance      = precip - ET0
      cumulative_balance = running sum
      water_stress       = water_balance < 0  (boolean)
    Requires column 'ET0_mm_day' (added via seasons.add_et0).
    """
    df = df.copy()
    df['water_balance']      = df['precip'].fillna(0) - df['ET0_mm_day'].fillna(0)
    df['cumulative_balance'] = df['water_balance'].cumsum()
    df['water_stress']       = df['water_balance'] < 0
    return df

# Statistics
def _r(value, n=2):
    """Round but preserve None for missing data."""
    if value is None:
        return None
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return round(float(value), n)

def raw_climate_summary(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Compact summary table -- mean / min / max / std per core variable.
    Missing variables (e.g. humidity not in CHIRPS) appear as None.
    """
    rows: List[Dict[str, Any]] = []
    for col, label in SUMMARY_VARS:
        if col not in df.columns:
            rows.append({'Variable': label,
                         'Mean': None, 'Min': None, 'Max': None, 'Std': None})
            continue
        s = df[col].dropna()
        if s.empty:
            rows.append({'Variable': label,
                         'Mean': None, 'Min': None, 'Max': None, 'Std': None})
            continue
        rows.append({
            'Variable': label,
            'Mean': _r(s.mean(), 3),
            'Min':  _r(s.min(),  3),
            'Max':  _r(s.max(),  3),
            'Std':  _r(s.std(),  3),
        })
    return rows

def overall_statistics(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Essential agro metrics for the full period.
    Filtered to remove noisy daily means/medians/stds and duplicate metrics (per the agroecology-priority spec).
    """
    p   = df['precip'].fillna(0)
    tx  = df['tmax']
    tn  = df['tmin']
    et0 = df['ET0_mm_day'].fillna(0)
    wb  = df['water_balance']

    return {
        'total_days': int(len(df)),
        'precipitation': {
            'total_mm':   _r(p.sum(),  1),
            'rainy_days': int((p >= 1.0).sum()),
            'dry_days':   int((p <  1.0).sum()),
            'max_daily':  _r(p.max(), 2),
        },
        'temperature': {
            'mean_tmax':  _r(tx.mean()),
            'mean_tmin':  _r(tn.mean()),
            'mean_tavg':  _r(((tx + tn) / 2).mean()),
            'max_tmax':   _r(tx.max()),
            'min_tmin':   _r(tn.min()),
        },
        'et0': {
            'total_mm':   _r(et0.sum(), 1),
        },
        'water_balance': {
            'total_balance': _r(wb.sum(), 1),
            'deficit_days':  int((wb < 0).sum()),
            'surplus_days':  int((wb > 0).sum()),
            'max_deficit':   _r(wb.min()),
            'max_surplus':   _r(wb.max()),
        },
    }

def season_statistics(df: pd.DataFrame, season: Dict) -> Dict[str, Any]:
    """
    Essential agro metrics for one season.
    Slices df to [onset, cessation] and computes the trimmed metric set:
      Precip       : Total_mm, Max_Daily, Rainy_Days, Intensity
      Temperature  : Mean_Tmax, Mean_Tmin, Mean_Tavg, Max_Tmax, Min_Tmin
      Water Balance: Total_Balance, Deficit_Days, Surplus_Days, Stress_Ratio
    """
    onset_ts = pd.to_datetime(season['onset'])
    if season.get('cessation') is not None:
        cess_ts = pd.to_datetime(season['cessation'])
    else:
        cess_ts = df['date'].iloc[-1]

    sdf = df[(df['date'] >= onset_ts) & (df['date'] <= cess_ts)].copy()
    if sdf.empty:
        return {}

    p  = sdf['precip'].fillna(0)
    tx = sdf['tmax']
    tn = sdf['tmin']
    wb = sdf['water_balance']

    rainy_days  = int((p >= 1.0).sum())
    length_days = int(season.get('length_days',
                                 (cess_ts - onset_ts).days + 1))
    intensity = _r(p.sum() / rainy_days, 2) if rainy_days else 0.0

    return {
        'onset':       onset_ts.strftime('%Y-%m-%d'),
        'cessation':   cess_ts.strftime('%Y-%m-%d'),
        'length_days': length_days,
        'precipitation': {
            'total_mm':   _r(p.sum(), 1),
            'max_daily':  _r(p.max(), 2),
            'rainy_days': rainy_days,
            'intensity':  intensity,
        },
        'temperature': {
            'mean_tmax':  _r(tx.mean()),
            'mean_tmin':  _r(tn.mean()),
            'mean_tavg':  _r(((tx + tn) / 2).mean()),
            'max_tmax':   _r(tx.max()),
            'min_tmin':   _r(tn.min()),
        },
        'water_balance': {
            'total_balance': _r(wb.sum(), 1),
            'deficit_days':  int((wb < 0).sum()),
            'surplus_days':  int((wb > 0).sum()),
            'stress_ratio':  _r((wb < 0).mean(), 3),
        },
    }

# Season detection on a pre-fetched DataFrame
def detect_seasons_auto(
    df: pd.DataFrame,
    lat: float,
    start_year: int,
    end_year: int,
) -> Tuple[Dict[int, List[Dict]], Dict[int, Dict]]:
    """
    Mirrors seasons.fetch_and_analyze_years() but operates on the *master* DataFrame already in memory (no re-fetching).
    For each ref year, slices a 1.5-year window so onset/cessation crossing the year boundary is captured, then runs ETO detection.
    Final post-processing (reassign + dedup) matches seasons.py.
    """
    if not SEASONS_AVAILABLE:
        raise RuntimeError("seasons.py not importable -- cannot detect seasons")

    seasons_dict: Dict[int, List[Dict]] = {}
    annual_dict:  Dict[int, Dict]       = {}

    for ref_year in range(start_year, end_year + 1):
        print(f"\n  Auto-detecting seasons for {ref_year}")
        win_start = pd.Timestamp(f"{ref_year}-01-01")
        win_end   = pd.Timestamp(f"{ref_year + 1}-06-30")
        win = (df[(df['date'] >= win_start) & (df['date'] <= win_end)]
               .copy().reset_index(drop=True))

        # Annual stats (calendar year only)
        yr_df = df[df['date'].dt.year == ref_year]
        if yr_df.empty:
            seasons_dict[ref_year] = []
            annual_dict[ref_year]  = {}
            continue

        annual_rain = float(yr_df['precip'].fillna(0).sum())
        humid_info  = check_humid(annual_rain, yr_df)
        annual_dict[ref_year] = {
            'annual_rain_mm':  round(annual_rain, 1),
            'is_humid':        humid_info['is_humid'],
            'low_rain_months': humid_info['low_rain_months'],
            'result_str':      humid_info['result_str'],
        }
        print(f"    Annual rainfall={annual_rain:.1f} mm | "
              f"{humid_info['result_str']}")

        if len(win) < 30:
            print(f"    Window too short ({len(win)} days)")
            seasons_dict[ref_year] = []
            continue
        try:
            seasons_dict[ref_year] = detect_onset_cessation(win)
        except ValueError as exc:
            print(f"    Skipped: {exc}")
            seasons_dict[ref_year] = []
        except Exception as exc:
            print(f"    Detection failed: {exc}")
            seasons_dict[ref_year] = []

    # Post-process: reassign spillover & remove duplicates
    cleaned = reassign_spillover_seasons(
        seasons_dict, lat=lat, start_year=start_year, end_year=end_year
    )
    final = remove_duplicate_seasons(cleaned)
    final_annual = {
        y: annual_dict.get(y, {}) for y in range(start_year, end_year + 1)
    }
    return final, final_annual

def detect_seasons_fixed(
    df: pd.DataFrame,
    fixed_defs: List[Dict],
    start_year: int,
    end_year: int,
) -> Tuple[Dict[int, List[Dict]], Dict[int, Dict]]:
    """
    Mirrors seasons.fetch_and_analyze_years_fixed() on the master DataFrame.
    For each year and each fixed window:
      1. Build the [onset, cessation] dates (handles year-crossing).
      2. Slice the master df and run ETO sub-detection inside the window.
    """
    if not SEASONS_AVAILABLE:
        raise RuntimeError("seasons.py not importable -- cannot detect seasons")

    seasons_dict: Dict[int, List[Dict]] = {
        y: [] for y in range(start_year, end_year + 1)
    }
    annual_dict: Dict[int, Dict] = {}

    for year in range(start_year, end_year + 1):
        print(f"\n  Fixed-season analysis for {year}")
        yr_df = df[df['date'].dt.year == year]
        if yr_df.empty:
            annual_dict[year] = {}
            continue

        # Annual stats (calendar year only)
        annual_rain = float(yr_df['precip'].fillna(0).sum())
        humid_info  = check_humid(annual_rain, yr_df)
        annual_dict[year] = {
            'annual_rain_mm':  round(annual_rain, 1),
            'is_humid':        humid_info['is_humid'],
            'low_rain_months': humid_info['low_rain_months'],
            'result_str':      humid_info['result_str'],
        }
        print(f"    Annual rainfall={annual_rain:.1f} mm | "
              f"{humid_info['result_str']}")
        for sd in fixed_defs:
            (o_m, o_d) = sd['onset_md']
            (c_m, c_d) = sd['cessation_md']
            cess_year  = year + 1 if (c_m, c_d) < (o_m, o_d) else year
            try:
                onset_date = date(year, o_m, o_d)
                cess_date  = date(cess_year, c_m, c_d)
            except ValueError as exc:
                print(f"    [WARN] Invalid date: {exc}")
                continue

            length_days = (cess_date - onset_date).days + 1
            cross       = " (year-crossing)" if cess_year != year else ""

            # ETO sub-detection inside the fixed window
            window_df = (df[(df['date'] >= pd.Timestamp(onset_date)) &
                            (df['date'] <= pd.Timestamp(cess_date))]
                         .copy().reset_index(drop=True))
            eto_subs: List[Dict] = []
            if len(window_df) < 14:
                print(f"    [ETO] {onset_date} → {cess_date}: "
                      f"window too short ({len(window_df)} days)")
            else:
                try:
                    eto_subs = detect_onset_cessation(window_df)
                except ValueError as exc:
                    print(f"    [ETO] {onset_date} → {cess_date}: {exc}")
                except Exception as exc:
                    print(f"    [ETO] {onset_date} → {cess_date} failed: {exc}")
            seasons_dict[year].append({
                'onset':       pd.Timestamp(onset_date),
                'cessation':   pd.Timestamp(cess_date),
                'length_days': length_days,
                'regime':      'fixed',
                'eto_seasons': eto_subs,
            })
            print(f"    Fixed window: {onset_date} → {cess_date}{cross} "
                  f"({length_days}d) | ETO sub-seasons={len(eto_subs)}")

    return seasons_dict, annual_dict

# Orchestrator
def analyze_climate_statistics(
    location_coord: Tuple[float, float],
    start_year:     int,
    end_year:       int,
    source:         str,
    fixed_season:   Optional[str] = None,
    model:          Optional[str] = None,
    scenario:       Optional[str] = None,
    extra_months:   int = 6,
) -> Dict[str, Any]:
    """
    Single entrypoint.
      Step 1 -- Fetch all climate variables for [start_year, end_year + tail]
      Step 2 -- Add ET0 (Hargreaves) and water balance
      Step 3 -- Detect seasons (auto or fixed)
      Step 4 -- Compute raw / overall / per-season statistics
    """
    lat, lon = location_coord

    # Decide fetch window (mirror seasons.py's tail logic)
    fixed_defs: Optional[List[Dict]] = None
    tail_extension_years = 0
    if fixed_season:
        fixed_defs = parse_fixed_seasons(fixed_season)
        if any(sd['cessation_md'] < sd['onset_md'] for sd in fixed_defs):
            tail_extension_years = 1
    else:
        # Auto mode: need 6-month tail past final year for late cessations
        tail_extension_years = 1 if extra_months > 0 else 0

    fetch_start = f"{start_year}-01-01"
    if tail_extension_years:
        # Add either 6 months (auto) or full year (fixed year-crossing)
        if fixed_season:
            fetch_end = f"{end_year + 1}-12-31"
        else:
            tail_month = min(12, 6 + 0)  # 6 extra months -> June+1 = July
            tail_end_dt = (date(end_year, 12, 31) +
                           pd.DateOffset(months=extra_months)).date()
            fetch_end = tail_end_dt.strftime('%Y-%m-%d')
    else:
        fetch_end = f"{end_year}-12-31"

    print(f"Fetching climate data: {fetch_start} → {fetch_end} | "
          f"source={source}")
    df = get_climate_data(lat, lon, fetch_start, fetch_end,
                          source, model=model, scenario=scenario)
    print(f"  Retrieved {len(df)} days, columns={list(df.columns)}")

    # ET0 + water balance
    df = add_et0(df, lat)
    df = calculate_water_balance(df)

    # Season detection (uses full df with tail for year-crossing capture)
    if fixed_season:
        seasons_dict, annual_dict = detect_seasons_fixed(
            df, fixed_defs, start_year, end_year
        )
    else:
        seasons_dict, annual_dict = detect_seasons_auto(
            df, lat, start_year, end_year
        )

    # Per-season block (computed against the FULL df so year-crossing seasons have access to days beyond Dec 31). Raw and overall stats are computed per season only; no full-period view is produced.
    season_results: List[Dict] = []
    for year in sorted(seasons_dict.keys()):
        for i, season in enumerate(seasons_dict[year], 1):
            stats = season_statistics(df, season)
            if not stats:
                continue
            stats['year']          = year
            stats['season_number'] = i
            stats['regime']        = season.get('regime', 'auto')

            # Slice once and attach the raw + overall views to this season
            onset_ts = pd.to_datetime(season['onset'])
            cess_ts  = (pd.to_datetime(season['cessation'])
                        if season.get('cessation') is not None
                        else df['date'].iloc[-1])
            sdf = df[(df['date'] >= onset_ts) & (df['date'] <= cess_ts)]
            stats['raw_climate_summary'] = raw_climate_summary(sdf)
            if not sdf.empty:
                stats['overall_statistics'] = overall_statistics(sdf)

            # ETO sub-seasons inside fixed windows
            sub_results: List[Dict] = []
            for es in (season.get('eto_seasons') or []):
                ssub = season_statistics(df, es)
                if ssub:
                    ssub['regime'] = es.get('regime', 'eto')
                    sub_results.append(ssub)
            if sub_results or season.get('eto_seasons') is not None:
                stats['eto_sub_seasons'] = sub_results

            season_results.append(stats)
    annual_summary = {
        str(y): {
            'annual_rain_mm':  info.get('annual_rain_mm'),
            'is_humid':        info.get('is_humid'),
            'low_rain_months': info.get('low_rain_months'),
            'humid_test':      info.get('result_str'),
        }
        for y, info in annual_dict.items()
    }
    return {
        'location':            {'lat': lat, 'lon': lon},
        'period':              {'start_year': start_year, 'end_year': end_year},
        'source':              source,
        'mode':                'fixed' if fixed_season else 'auto',
        'fixed_season':        fixed_season,
        'model':               model,
        'scenario':            scenario,
        'season_statistics':   season_results,
        'annual_summary':      annual_summary,
        'analysis_date':       datetime.now().isoformat(),
        'methodology':         'preprocess_data + seasons.py detection + water balance',
    }

# Display
def _print_indented_table(df: pd.DataFrame, indent: str = "    ") -> None:
    for line in df.to_string(index=False).splitlines():
        print(f"{indent}{line}")

def print_raw_summary_by_season(seasons: List[Dict]) -> None:
    """One raw mean/min/max/std table per season, printed as stacked blocks."""
    print("\n" + "=" * 70)
    print("RAW CLIMATE SUMMARY BY SEASON")
    print("=" * 70)
    if not seasons:
        print("No seasons detected for this period.")
        return
    for s in seasons:
        regime = s.get('regime', 'auto')
        print(f"\n  Year {s['year']} | Season {s['season_number']} ({regime})")
        print(f"    {s['onset']} → {s['cessation']}  ({s['length_days']}d)")
        rows = s.get('raw_climate_summary') or []
        if not rows:
            print("    (no data)")
            continue
        _print_indented_table(pd.DataFrame(rows).fillna("n/a"))

def print_overall_by_season(seasons: List[Dict]) -> None:
    """One overall agro-metric table per season, printed as stacked blocks."""
    print("\n" + "=" * 70)
    print("OVERALL STATISTICS BY SEASON")
    print("=" * 70)
    if not seasons:
        print("No seasons detected for this period.")
        return
    for s in seasons:
        regime = s.get('regime', 'auto')
        print(f"\n  Year {s['year']} | Season {s['season_number']} ({regime})")
        print(f"    {s['onset']} → {s['cessation']}  ({s['length_days']}d)")
        stats = s.get('overall_statistics')
        if not stats:
            print("    (no data)")
            continue
        print(f"    Total days: {stats['total_days']}")
        rows = []
        for var_key, var_label in [
            ('precipitation', 'Precipitation'),
            ('temperature',   'Temperature'),
            ('et0',           'ET0'),
            ('water_balance', 'Water Balance'),
        ]:
            for metric, value in stats[var_key].items():
                rows.append({
                    'Variable': var_label,
                    'Metric':   metric,
                    'Value':    value if value is not None else "n/a",
                })
        _print_indented_table(pd.DataFrame(rows))

def print_seasons(seasons: List[Dict]) -> None:
    print("\n" + "=" * 70)
    print("SEASON STATISTICS")
    print("=" * 70)
    if not seasons:
        print("No seasons detected for this period.")
        return

    for s in seasons:
        regime = s.get('regime', 'auto')
        print(f"\n  Year {s['year']} | Season {s['season_number']} ({regime})")
        print(f"    {s['onset']} → {s['cessation']}  ({s['length_days']}d)")
        p = s['precipitation']
        t = s['temperature']
        w = s['water_balance']
        print(f"    Precipitation : "
              f"total={p['total_mm']} mm | "
              f"max_daily={p['max_daily']} mm | "
              f"rainy_days={p['rainy_days']} | "
              f"intensity={p['intensity']} mm/wet-day")
        print(f"    Temperature   : "
              f"mean_tmax={t['mean_tmax']}°C | "
              f"mean_tmin={t['mean_tmin']}°C | "
              f"mean_tavg={t['mean_tavg']}°C | "
              f"max_tmax={t['max_tmax']}°C | "
              f"min_tmin={t['min_tmin']}°C")
        print(f"    Water balance : "
              f"total={w['total_balance']} mm | "
              f"deficit_days={w['deficit_days']} | "
              f"surplus_days={w['surplus_days']} | "
              f"stress_ratio={w['stress_ratio']}")

        subs = s.get('eto_sub_seasons')
        if subs is not None:
            print(f"    {'─' * 50}")
            print(f"    ETO sub-seasons within fixed window:")
            if not subs:
                print(f"      none detected")
            for j, es in enumerate(subs, 1):
                ep = es['precipitation']
                ew = es['water_balance']
                print(f"      {j}. {es['onset']} → {es['cessation']} "
                      f"({es['length_days']}d) | "
                      f"rain={ep['total_mm']} mm | "
                      f"rainy={ep['rainy_days']}d | "
                      f"stress_ratio={ew['stress_ratio']}")

def print_annual(annual: Dict[str, Dict]) -> None:
    print("\n" + "=" * 70)
    print("ANNUAL SUMMARY (humid test)")
    print("=" * 70)
    if not annual:
        print("(no annual data)")
        return
    rows = []
    for year, info in sorted(annual.items()):
        rows.append({
            'Year':              year,
            'Annual rainfall':   f"{info.get('annual_rain_mm')} mm"
                                 if info.get('annual_rain_mm') is not None
                                 else "n/a",
            'Low-rain months':   info.get('low_rain_months', 'n/a'),
            'Humid test':        info.get('humid_test', 'n/a'),
        })
    print(pd.DataFrame(rows).to_string(index=False))

def print_pandas(result: Dict[str, Any]) -> None:
    if 'error' in result:
        print(f"Error: {result['error']}")
        return
    print(f"\nLocation : {result['location']['lat']:.4f}, "
          f"{result['location']['lon']:.4f}")
    print(f"Period   : {result['period']['start_year']} – "
          f"{result['period']['end_year']}")
    print(f"Source   : {result['source']}  | mode={result['mode']}")
    if result.get('fixed_season'):
        print(f"Fixed    : {result['fixed_season']}")
    if result.get('model'):
        print(f"Model    : {result['model']}")
    if result.get('scenario'):
        print(f"Scenario : {result['scenario']}")

    print_raw_summary_by_season(result['season_statistics'])
    print_overall_by_season(result['season_statistics'])
    print_seasons(result['season_statistics'])
    print_annual(result['annual_summary'])

# CLI
def main() -> None:
    parser = argparse.ArgumentParser(
        description='Climate statistics analysis by season (auto or fixed)',
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument('--location', required=True,
                        help='Coordinates as "lat,lon"  e.g. "-1.286,36.817"')
    parser.add_argument('--start-year', type=int, required=True)
    parser.add_argument('--end-year',   type=int, required=True)
    parser.add_argument('--source',     required=True,
                        help=(
                            "Data source. Examples:\n"
                            "  era_5, agera_5, chirps+chirts, nasa_power, "
                            "nex_gddp, auto"
                        ))
    parser.add_argument(
        '--fixed-season',
        default=None,
        metavar='MM-DD:MM-DD[,MM-DD:MM-DD]',
        help=(
            "Force fixed calendar season windows (matches seasons.py)."
            "Climate data is still fetched via --source for statistics"
            "and ETO-based onset/cessation analysis within each window."
            "Examples:"
            "  Single season : --fixed-season '03-01:05-31'"
            "  Two seasons   : --fixed-season '03-01:05-31,10-01:12-15'"
            "  Year-crossing : --fixed-season '11-01:02-28'"
        ),
    )
    parser.add_argument('--extra-months', type=int, default=6,
                        help='Extra months past Dec for late cessations '
                             '(auto mode, default: 6)')
    parser.add_argument('--model', default=None,
                        help='NEX-GDDP model (e.g. ACCESS-CM2)')
    parser.add_argument('--scenario', default=None,
                        help='NEX-GDDP scenario (e.g. ssp245)')
    parser.add_argument('--format', choices=['json', 'pandas'],
                        default='pandas',
                        help='Output format (default: pandas)')
    parser.add_argument('--output', default=None,
                        help='Output JSON file path (json format only)')
    parser.add_argument('--output-dir', default='.',
                        help='Directory for default JSON output (default: cwd)')
    parser.add_argument('--no-save', action='store_true',
                        help='Skip saving the JSON output')

    args = parser.parse_args()

    try:
        lat, lon = map(float, args.location.split(','))
    except ValueError:
        print("Error: --location must be in 'lat,lon' format.")
        sys.exit(1)

    if args.fixed_season:
        print(f"Fixed-season mode | {lat:.4f}N, {lon:.4f}E | "
              f"{args.start_year}–{args.end_year} | source={args.source}")
    else:
        print(f"Auto-detection mode | {lat:.4f}N, {lon:.4f}E | "
              f"{args.start_year}–{args.end_year} | source={args.source}")

    result = analyze_climate_statistics(
        location_coord=(lat, lon),
        start_year=args.start_year,
        end_year=args.end_year,
        source=args.source,
        fixed_season=args.fixed_season,
        model=args.model,
        scenario=args.scenario,
        extra_months=args.extra_months,
    )

    # Display
    if args.format == 'pandas':
        print_pandas(result)
    else:
        out = json.dumps(result, indent=2, default=str)
        if args.output:
            with open(args.output, 'w') as f:
                f.write(out)
            print(f"Saved to {args.output}")
        else:
            print(out)

    # Auto-save JSON alongside pandas display
    if not args.no_save and args.format == 'pandas':
        mode_tag = 'fixed' if args.fixed_season else args.source
        fname = (f"climate_stats_{lat:.4f}_{lon:.4f}_"
                 f"{args.start_year}_{args.end_year}_{mode_tag}.json")
        path  = Path(args.output_dir) / fname
        with open(path, 'w') as f:
            f.write(json.dumps(result, indent=2, default=str))
        print(f"\n✓ SAVED: {path}")

if __name__ == "__main__":
    main()

# Auto season detection:
# python climate_tookit/climate_statistics/statistics.py --location="-1.286,36.817" --start-year 2018 --end-year 2020 --source era_5 --format pandas
# python climate_tookit/climate_statistics/statistics.py --location="-1.286,36.817" --start-year 2015 --end-year 2020 --source agera_5 --format pandas

# Fixed single season:
# python climate_tookit/climate_statistics/statistics.py --location="-1.286,36.817" --start-year 2018 --end-year 2022 --fixed-season "03-01:05-31" --source era_5 --format pandas

# Fixed two seasons:
# python climate_tookit/climate_statistics/statistics.py --location="-1.286,36.817" --start-year 2018 --end-year 2022 --fixed-season "03-01:05-31,10-01:12-15" --source agera_5 --format pandas

# Fixed year-crossing season:
# python climate_tookit/climate_statistics/statistics.py --location="-1.286,36.817" --start-year 2018 --end-year 2022 --fixed-season "11-01:02-28" --source chirps+chirts --format pandas

# NEX-GDDP with fixed season:
# python climate_tookit/climate_statistics/statistics.py --location="-1.286,36.817" --start-year 2030 --end-year 2032 --fixed-season "03-01:05-31" --source nex_gddp --model ACCESS-CM2 --scenario ssp245 --format pandas