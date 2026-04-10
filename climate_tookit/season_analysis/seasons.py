"""
Season Analysis Module

Detects agricultural growing seasons from daily precipitation and temperature data.
Applies the Hargreaves ET0 method to identify planting season onset and cessation
based on whether precipitation meets or exceeds 50% of reference evapotranspiration.

Data source priority:
    Historical : ERA5 → AgERA5 → CHIRPS + CHIRTS (fallback)
    Future     : NEX-GDDP-CMIP6 only
Perhumid guard (Af climate protection):
    Years where ALL three conditions are met are flagged and skipped:
        1. Annual rainfall  > 1500 mm
        2. Months with total < 40 mm  ≤ 3
        3. Days with ≥ 1 mm precip    > 200
Multi-year overlap fix:
    Detection runs independently per calendar year.
    Seasons that cross a year boundary are attributed to the onset year.
    A final deduplication pass removes any season whose date range overlaps
    an already-accepted season (first-onset wins).
Dependencies: pandas, numpy, climate_toolkit
"""

import pandas as pd
import numpy as np
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
FUTURE_SOURCE      = 'nex_gddp'
FALLBACK_COMBO     = ('chirps', 'chirts')

# Perhumid thresholds 
PERHUMID_ANNUAL_MM       = 1500   
PERHUMID_LOW_MONTH_MM    = 40     
PERHUMID_MAX_LOW_MONTHS  = 3      
PERHUMID_MIN_RAINY_DAYS  = 200    
NEAR_PERHUMID_ANNUAL_MM  = 1200   
NEAR_PERHUMID_RAINY_DAYS = 200 

# Data access  (unchanged public API)
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
    Returns:
        DataFrame with columns: date, tmax, tmin, precip.
    Raises:
        RuntimeError: When no data source returns a valid result.
    """
    date_from = date.fromisoformat(start_date)
    date_to   = date.fromisoformat(end_date)

    df = _fetch_raw(lat, lon, date_from, date_to,
                    use_projections, model, scenario, force_source)

    if df is None or df.empty:
        raise RuntimeError("All data sources exhausted — no climate data retrieved.")

    result = pd.DataFrame()
    result['date']   = pd.to_datetime(df['date'])
    result['tmax']   = df.get('max_temperature')
    result['tmin']   = df.get('min_temperature')
    result['precip'] = df.get('precipitation')
    return result

def _fetch_raw(
    lat, lon, date_from, date_to,
    use_projections, model, scenario, force_source,
) -> Optional[pd.DataFrame]:
    coord = (lat, lon)

    if force_source == 'chirps+chirts':
        return _merge_chirps_chirts(coord, date_from, date_to)
    if force_source == 'nex_gddp' or use_projections:
        return preprocess_data(
            source=FUTURE_SOURCE, location_coord=coord,
            date_from=date_from, date_to=date_to,
            model=model, scenario=scenario,
        )
    if force_source:
        return preprocess_data(
            source=force_source, location_coord=coord,
            date_from=date_from, date_to=date_to,
        )
    for source in HISTORICAL_SOURCES:
        try:
            df = preprocess_data(source=source, location_coord=coord,
                                  date_from=date_from, date_to=date_to)
            if not df.empty and 'precipitation' in df.columns:
                return df
        except Exception:
            continue
    return _merge_chirps_chirts(coord, date_from, date_to)

def _merge_chirps_chirts(coord, date_from, date_to) -> pd.DataFrame:
    df_p = preprocess_data(source=FALLBACK_COMBO[0], location_coord=coord,
                            date_from=date_from, date_to=date_to)
    df_t = preprocess_data(source=FALLBACK_COMBO[1], location_coord=coord,
                            date_from=date_from, date_to=date_to)
    return pd.merge(df_p, df_t, on='date', how='inner')

# ET0 — Hargreaves  (unchanged)
def calculate_et0(tmin: float, tmax: float, lat: float, date_val: datetime) -> float:
    """
    Estimate reference evapotranspiration via the Hargreaves equation.
    Returns ET0 in mm/day, or 0 when inputs are invalid.
    """
    if tmax is None or tmin is None or pd.isna(tmax) or pd.isna(tmin) or tmax < tmin:
        return 0.0
    J        = date_val.timetuple().tm_yday
    lat_rad  = math.radians(lat)
    sol_decl = 0.409 * math.sin((2 * math.pi / 365) * J - 1.39)
    ird      = 1 + 0.033 * math.cos((2 * math.pi / 365) * J)
    sha      = math.acos(max(min(-math.tan(lat_rad) * math.tan(sol_decl), 1), -1))
    ra = (
        (24 * 60 / math.pi) * 0.0820 * ird
        * (sha * math.sin(lat_rad) * math.sin(sol_decl)
           + math.cos(lat_rad) * math.cos(sol_decl) * math.sin(sha))
    )
    return 0.0023 * math.sqrt(tmax - tmin) * ((tmax + tmin) / 2 + 17.8) * ra

# Perhumid guard
def _check_perhumid(df_year: pd.DataFrame, year: int) -> Tuple[bool, Dict]:
    """
    Return (is_perhumid, info_dict) for a single calendar year slice.
    Perhumid (Af) when ALL three hold:
        annual rain  > PERHUMID_ANNUAL_MM       (1500 mm)
        low-rain months ≤ PERHUMID_MAX_LOW_MONTHS (≤ 3 months below 40 mm)
        rainy days   > PERHUMID_MIN_RAINY_DAYS  (200 days ≥ 1 mm)
    """
    yr = df_year[df_year['date'].dt.year == year].copy()
    if yr.empty:
        return False, {}
    p = yr['precip'].fillna(0)
    annual_rain  = float(p.sum())
    rainy_days   = int((p >= 1.0).sum())
    yr['_m']         = yr['date'].dt.month
    monthly_totals   = yr.groupby('_m')['precip'].sum()
    low_rain_months  = int((monthly_totals < PERHUMID_LOW_MONTH_MM).sum())
    is_ph = (
        annual_rain    > PERHUMID_ANNUAL_MM
        and low_rain_months <= PERHUMID_MAX_LOW_MONTHS
        and rainy_days > PERHUMID_MIN_RAINY_DAYS
    )
    is_near_ph = (
        annual_rain    > NEAR_PERHUMID_ANNUAL_MM
        and rainy_days > NEAR_PERHUMID_RAINY_DAYS
    )
    return is_ph, {
        'annual_rain_mm':    round(annual_rain, 1),
        'rainy_days':        int(rainy_days),
        'low_rain_months':   int(low_rain_months),
        'is_perhumid':       bool(is_ph),
        'is_near_perhumid':  bool(is_near_ph),
    }

# Wet-spell confirmation  (anti-false-start)
def _has_wet_confirmation(
    precip_arr: np.ndarray,
    et0_arr: np.ndarray,
    start_idx: int,
    min_wet_days: int,
    annual_rain: float,
) -> bool:
    """
    Look 25 days ahead for `min_wet_days` days where precip ≥ 0.5 × ET0.
    Drier climates tolerate slightly longer dry interruptions.
    """
    window = 25
    if start_idx + window > len(precip_arr):
        return False
    thresh     = 0.5 * et0_arr[start_idx: start_idx + window]
    p_win      = precip_arr[start_idx: start_idx + window]
    max_dry    = 3 if annual_rain < 600 else 2
    wet_streak = 0
    dry_run    = 0
    for p, t in zip(p_win, thresh):
        if p >= t:
            wet_streak += 1
            dry_run     = 0
            if wet_streak >= min_wet_days:
                return True
        else:
            dry_run += 1
            if dry_run > max_dry:
                break
    return False

# Regime detection
def _detect_regime(df_yr: pd.DataFrame) -> str:
    """
    Classify a year as unimodal, bimodal, year_crossing, or erratic based on
    the number and timing of rainfall peaks.
    A peak month must have rainfall > 20% of annual total AND exceed both its
    neighbours by at least 20%.
    """
    df_yr = df_yr.copy()
    df_yr['_m'] = df_yr['date'].dt.month
    monthly = df_yr.groupby('_m')['precip'].sum()
    annual  = monthly.sum()

    if annual <= 0:
        return 'erratic'
    months = sorted(monthly.index.tolist())
    peaks  = []
    for m in months:
        val  = monthly[m]
        prev = monthly.get(m - 1, monthly.get(12 if m == 1 else m - 1, 0))
        nxt  = monthly.get(m + 1, monthly.get(1 if m == 12 else m + 1, 0))
        if val > 0.2 * annual and val > 1.2 * prev and val > 1.2 * nxt:
            peaks.append(m)
    if len(peaks) == 2:
        return 'bimodal'
    elif len(peaks) == 1:
        return 'year_crossing' if peaks[0] > 9 else 'unimodal'
    else:
        return 'erratic'

# Adaptive parameters  (regime-aware)
# Base cessation days by rainfall regime
_REGIME_CESS = {
    'unimodal':     22,
    'bimodal':      18,  
    'year_crossing': 20,
    'erratic':      22,
}

def _adaptive_params(annual_rain: float, regime: str = 'unimodal') -> Dict[str, int]:
    """Return detection parameters tuned to local rainfall and regime."""
    base_cess = _REGIME_CESS.get(regime, 22)
    if annual_rain < 600:       
        return dict(min_season_days=max(25, int(annual_rain * 0.08)),
                    wet_confirm=2, cess_days=base_cess)
    elif annual_rain > 1500:      
        return dict(min_season_days=max(60, int(annual_rain * 0.04)),
                    wet_confirm=3, cess_days=base_cess)
    else:                          
        min_s = 45 if regime != 'bimodal' else 30   
        return dict(min_season_days=min_s, wet_confirm=3, cess_days=base_cess)

# Single-year detector  (works on ONE calendar year's DataFrame)
def _detect_one_year(
    df_yr: pd.DataFrame,
    annual_rain: float,
    gap_days: int,
    min_season_days: int,
) -> List[Dict]:
    """
    Detect seasons within a single year using regime-aware adaptive parameters.
    Steps:
        1. Classify the year's rainfall regime (unimodal / bimodal /
           year_crossing / erratic).
        2. Select cessation threshold and min-season length for that regime.
        3. Run onset/cessation detection with wet-spell confirmation.
    df_yr must have columns: date, precip, et0.  Sorted ascending.
    """
    df_yr = df_yr.copy().reset_index(drop=True)
    p = df_yr['precip'].fillna(0).to_numpy()
    e = df_yr['et0'].fillna(0).to_numpy()
    dts = df_yr['date'].to_numpy()
    regime = _detect_regime(df_yr)
    params = _adaptive_params(annual_rain, regime)
    cess_thresh = max(gap_days, params['cess_days'])
    min_days = max(min_season_days, params['min_season_days'])
    wet_confirm = params['wet_confirm']
    threshold = 0.5 * e
    rainy_flag = p >= threshold
    seasons: List[Dict] = []
    i, n = 0, len(df_yr)

    while i < n:
        if not rainy_flag[i]:
            i += 1
            continue
        # Anti-false-start confirmation
        if not _has_wet_confirmation(p, e, i, wet_confirm, annual_rain):
            i += 1
            continue
        onset_date     = pd.Timestamp(dts[i])
        dry_counter    = 0
        cessation_date = None
        j = i + 1

        while j < n:
            if rainy_flag[j]:
                dry_counter = 0
            else:
                dry_counter += 1
                if dry_counter >= cess_thresh:
                    cessation_date = pd.Timestamp(dts[j - cess_thresh])
                    break
            j += 1

        # No cessation found within this year — cap at last day
        if cessation_date is None:
            cessation_date = pd.Timestamp(dts[-1])
        length = (cessation_date - onset_date).days + 1

        if length >= min_days:
            seasons.append({
                'onset_date':     onset_date.strftime('%Y-%m-%d'),
                'cessation_date': cessation_date.strftime('%Y-%m-%d'),
                'onset_doy':      onset_date.timetuple().tm_yday,
                'cessation_doy':  cessation_date.timetuple().tm_yday,
                'length_days':    length,
                'regime':         regime,
            })
        # Advance past cessation so the next search starts fresh
        try:
            cess_idx = next(
                k for k, d in enumerate(dts)
                if pd.Timestamp(d) == cessation_date
            )
            i = cess_idx + cess_thresh + 1
        except StopIteration:
            break
    return seasons

# Overlap removal
def _remove_overlaps(seasons: List[Dict]) -> List[Dict]:
    """
    Remove seasons whose date range overlaps an already-accepted season.
    Processes in chronological onset order; first onset wins.
    """
    accepted: List[Dict] = []
    for s in sorted(seasons, key=lambda x: x['onset_date']):
        s_on  = pd.Timestamp(s['onset_date'])
        s_off = pd.Timestamp(s['cessation_date'])
        if any(
            s_on  <= pd.Timestamp(a['cessation_date'])
            and s_off >= pd.Timestamp(a['onset_date'])
            for a in accepted
        ):
            continue         
        accepted.append(s)
    return accepted

def _merge_boundary_seasons(
    seasons: List[Dict],
    gap_days: int,
    near_perhumid_years: Optional[set] = None,
) -> List[Dict]:
    """
    Merge consecutive seasons that were artificially split at a Dec 31 / Jan 1
    year boundary but are actually one continuous wet spell.
    Two seasons are merged when the gap between the first cessation and the
    second onset is less than gap_days — UNLESS the onset year or cessation
    year is near-perhumid (>1200mm, >200 rainy days).  Near-perhumid years
    have no genuine dry spell long enough to close a season within the year;
    merging across their boundary would recreate the mega-season problem.
    The merged season spans the earlier onset to the later cessation.
    """
    if len(seasons) < 2:
        return seasons
    near_ph = near_perhumid_years or set()
    seasons = sorted(seasons, key=lambda x: x['onset_date'])
    merged = [seasons[0].copy()]

    for curr in seasons[1:]:
        prev       = merged[-1]
        prev_cess  = pd.Timestamp(prev['cessation_date'])
        curr_on    = pd.Timestamp(curr['onset_date'])
        gap        = (curr_on - prev_cess).days

        # Block merge if either side of the boundary is a near-perhumid year
        prev_yr = prev_cess.year
        curr_yr = curr_on.year
        blocked = (prev_yr in near_ph) or (curr_yr in near_ph)

        if gap < gap_days and not blocked:
            curr_cess = pd.Timestamp(curr['cessation_date'])
            new_cess  = max(prev_cess, curr_cess)
            prev['cessation_date'] = new_cess.strftime('%Y-%m-%d')
            prev['cessation_doy']  = new_cess.timetuple().tm_yday
            prev['length_days']    = (new_cess - pd.Timestamp(prev['onset_date'])).days + 1
        else:
            merged.append(curr.copy())

    return merged

# Multi-year orchestrator  (replaces the old monolithic detect_seasons call)
def detect_seasons_multi_year(
    df: pd.DataFrame,
    lat: float,
    gap_days: int = 30,
    min_season_days: int = 30,
) -> Tuple[List[Dict], Dict[int, Dict]]:
    """
    Detect growing seasons across a multi-year DataFrame.
    Strategy:
        1. Split data by calendar year.
        2. Run perhumid test on each year — skip perhumid years entirely.
        3. Detect seasons independently within each year's data.
           (This prevents wet spells in one year bleeding into the next.)
        4. Pool all detected seasons and run a final overlap-removal pass.
    Args:
        df: DataFrame with columns date, precip, tmax, tmin, et0.
        lat: Latitude in decimal degrees.
        gap_days: Consecutive dry days required to close a season.
        min_season_days: Minimum valid season length in days.
    Returns:
        (seasons, year_flags)
        seasons    — overlap-free list sorted by onset date.
        year_flags — dict[year] with perhumid info for every year processed.
    """
    df = df.copy().sort_values('date').reset_index(drop=True)

    # Ensure ET0 is present
    if 'et0' not in df.columns:
        df['et0'] = [
            calculate_et0(r['tmin'], r['tmax'], lat, r['date'])
            for _, r in df.iterrows()
        ]
    years      = [int(y) for y in sorted(df['date'].dt.year.unique())]
    year_flags: Dict[int, Dict] = {}
    all_raw:    List[Dict]      = []
    near_perhumid_years: set    = set()

    for yr in years:
        yr_df = df[df['date'].dt.year == yr].copy().reset_index(drop=True)

        # Perhumid check
        is_ph, info = _check_perhumid(yr_df, yr)
        year_flags[int(yr)] = info

        if is_ph:
            info['skipped_reason'] = (
                f"perhumid: {info['annual_rain_mm']} mm/yr, "
                f"{info['low_rain_months']} low-rain months, "
                f"{info['rainy_days']} rainy days"
            )
            continue 

        # Track near-perhumid years — their seasons must not merge across boundaries
        if info.get('is_near_perhumid'):
            near_perhumid_years.add(yr)

        # Annual rain for adaptive params 
        annual_rain = float(yr_df['precip'].fillna(0).sum())

        # ── Detect within this year
        yr_seasons = _detect_one_year(yr_df, annual_rain, gap_days, min_season_days)
        all_raw.extend(yr_seasons)

    # Merge year-boundary splits, blocking near-perhumid year boundaries
    seasons = _merge_boundary_seasons(all_raw, gap_days, near_perhumid_years)
    seasons = _remove_overlaps(seasons)

    return seasons, year_flags

# Legacy single-period detector  (kept for backward compatibility / CLI --show-data)
def detect_seasons(
    df: pd.DataFrame,
    gap_days: int = 30,
    min_season_days: int = 30,
) -> List[Dict]:
    """
    Original single-period season detector (no perhumid guard, no year splitting).
    Kept for backward-compatible callers.  New code should use detect_seasons_multi_year.
    Args:
        df: DataFrame with columns: date, precip, et0.
    Returns:
        List of season dicts.
    """
    df = df.copy().reset_index(drop=True)
    df['threshold'] = df['et0'] * 0.5
    df['rainy_day'] = df['precip'] >= df['threshold']

    seasons: List[Dict] = []
    i, n = 0, len(df)

    while i < n:
        if not df.iloc[i]['rainy_day']:
            i += 1
            continue
        onset_date   = df.iloc[i]['date']
        dry_counter  = 0
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

# Season statistics helpers  (unchanged)
def calculate_average_season(seasons: List[Dict]) -> Optional[Dict[str, Any]]:
    """
    Compute mean onset DOY, cessation DOY, and season length.
    Returns None when the input list is empty.
    """
    if not seasons:
        return None
    n = len(seasons)
    return {
        'avg_onset_doy':     sum(s['onset_doy']     for s in seasons) / n,
        'avg_cessation_doy': sum(s['cessation_doy'] for s in seasons) / n,
        'avg_length_days':   sum(s['length_days']   for s in seasons) / n,
        'season_count':      n,
    }

# Main public API  (same signature as before)
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
    Full season analysis for a primary period with optional baseline / future averages.

    Now includes:
      - Per-year perhumid guard  (perhumid years are skipped, not crashed).
      - Per-year independent detection  (no year-crossing mega-seasons).
      - Overlap removal across the full result.
      - perhumid_year_flags in the output for transparency.
    """
    lat, lon         = location_coord
    start_date, end_date = date_range
    force_source     = None if source == 'auto' else source
    try:
        df = get_climate_data(lat, lon, start_date, end_date,
                              use_projections=False, force_source=force_source)
        df['et0'] = [
            calculate_et0(r['tmin'], r['tmax'], lat, r['date'])
            for _, r in df.iterrows()
        ]

        # NEW: multi-year aware detection
        seasons, year_flags = detect_seasons_multi_year(
            df, lat=lat, gap_days=gap_days, min_season_days=min_season_days,
        )
        result: Dict[str, Any] = {
            'location':            {'lat': lat, 'lon': lon},
            'actual_period':       {'start': start_date, 'end': end_date},
            'seasons_detected':    len(seasons),
            'seasons':             seasons,
            'main_season':         max(seasons, key=lambda x: x['length_days']) if seasons else None,
            'perhumid_year_flags': year_flags,
            'method':              'ET0_precipitation_threshold_adaptive',
            'data_source':         'Climate Toolkit (ERA5/AgERA5/CHIRPS+CHIRTS)',
            'analysis_date':       datetime.now().isoformat(),
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
                model=climate_model, scenario=scenario,
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
    """
    Compute average season statistics across a multi-year period.
    Each year is fetched and processed independently.
    Perhumid years are silently excluded from the average.
    """
    all_seasons: List[Dict] = []
    skipped_years: List[int] = []

    for year in range(year_start, year_end + 1):
        try:
            df_year = get_climate_data(
                lat, lon,
                f"{year}-01-01", f"{year}-12-31",
                use_projections=use_projections,
                model=model, scenario=scenario,
            )
            df_year['et0'] = [
                calculate_et0(r['tmin'], r['tmax'], lat, r['date'])
                for _, r in df_year.iterrows()
            ]
            # Perhumid check — skip year if flagged
            is_ph, _ = _check_perhumid(df_year, year)
            if is_ph:
                skipped_years.append(year)
                continue
            annual_rain = float(df_year['precip'].fillna(0).sum())
            yr_seasons  = _detect_one_year(df_year, annual_rain, gap_days, min_season_days)
            all_seasons.extend(yr_seasons)
        except Exception:
            continue

    # Remove any cross-year overlaps that sneak through
    all_seasons = _merge_boundary_seasons(all_seasons, gap_days)
    all_seasons = _remove_overlaps(all_seasons)

    out: Dict[str, Any] = {
        f'{prefix}_average':       calculate_average_season(all_seasons),
        f'{prefix}_period':        {'start': year_start, 'end': year_end},
        f'{prefix}_data_source':   source_label,
        f'{prefix}_skipped_years': skipped_years,
    }
    if model_label:
        out[f'{prefix}_model'] = model_label
    if scenario_label:
        out[f'{prefix}_scenario'] = scenario_label
    return out

# CLI  (identical to original — no arguments changed)
def main() -> None:
    parser = argparse.ArgumentParser(description='Season analysis using climate data')
    parser.add_argument('--location',       required=True, help='Coordinates as "lat,lon"')
    parser.add_argument('--source',         choices=['era_5', 'agera_5', 'nex_gddp', 'chirps+chirts', 'auto'],
                        default='auto')
    parser.add_argument('--date-from')
    parser.add_argument('--date-to')
    parser.add_argument('--baseline-start', type=int)
    parser.add_argument('--baseline-end',   type=int)
    parser.add_argument('--baseline-only',  action='store_true')
    parser.add_argument('--future-start',   type=int)
    parser.add_argument('--future-end',     type=int)
    parser.add_argument('--future-only',    action='store_true')
    parser.add_argument('--climate-model',  default='GFDL-ESM4')
    parser.add_argument('--scenario',       default='ssp245',
                        choices=['ssp126', 'ssp245', 'ssp370', 'ssp585'])
    parser.add_argument('--gap-days',       type=int, default=30)
    parser.add_argument('--min-season-days',type=int, default=30)
    parser.add_argument('--output',         help='Save JSON result to this path')
    parser.add_argument('--download-data',  help='Save daily climate CSV to this path')
    parser.add_argument('--show-data',      action='store_true')
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
    baseline_years = (
        (args.baseline_start, args.baseline_end)
        if args.baseline_start and args.baseline_end else None
    )
    future_years = (
        (args.future_start, args.future_end)
        if args.future_start and args.future_end else None
    )
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
    # Optional data download
    if args.download_data and not args.baseline_only and not args.future_only:
        try:
            force_source = None if args.source == 'auto' else args.source
            df_dl = get_climate_data(lat, lon, args.date_from, args.date_to,
                                      force_source=force_source)
            df_dl['et0']       = [calculate_et0(r['tmin'], r['tmax'], lat, r['date'])
                                   for _, r in df_dl.iterrows()]
            df_dl['threshold'] = df_dl['et0'] * 0.5
            df_dl['rainy_day'] = df_dl['precip'] >= df_dl['threshold']
            df_dl.to_csv(args.download_data, index=False)
            print(f"Data saved to {args.download_data}")
        except Exception as exc:
            print(f"Failed to save data: {exc}")

    # Trim output for --baseline-only / --future-only
    if args.baseline_only:
        keep = ('location', 'baseline_average', 'baseline_period',
                'baseline_data_source', 'baseline_skipped_years',
                'method', 'analysis_date')
        result = {k: result[k] for k in keep if k in result}
    elif args.future_only:
        keep = ('location', 'future_average', 'future_period', 'future_model',
                'future_scenario', 'future_data_source', 'future_skipped_years',
                'method', 'analysis_date')
        result = {k: result[k] for k in keep if k in result}

    # Optional show-data
    if args.show_data and 'error' not in result \
            and not args.baseline_only and not args.future_only:
        force_source = None if args.source == 'auto' else args.source
        df_show = get_climate_data(lat, lon, args.date_from, args.date_to,
                                    force_source=force_source)
        df_show['et0']       = [calculate_et0(r['tmin'], r['tmax'], lat, r['date'])
                                 for _, r in df_show.iterrows()]
        df_show['threshold'] = df_show['et0'] * 0.5
        df_show['rainy_day'] = df_show['precip'] >= df_show['threshold']
        print("\n=== DAILY CLIMATE DATA ===")
        print(df_show.head(10).to_string())
        print(f"... ({len(df_show)} total records)")
        print(df_show.tail(10).to_string())
        print("\n=== SEASON ANALYSIS RESULTS ===")

    # Output
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