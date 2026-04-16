"""
Season Analysis Module

Detects agricultural growing seasons from daily precipitation and temperature data.
Applies the Hargreaves ET0 method to identify planting season onset and cessation
based on whether precipitation meets or exceeds 50% of reference evapotranspiration.

Data source priority:
    Historical : ERA5 -> AgERA5 -> CHIRPS + CHIRTS (fallback)

Detection strategy:
    Per reference year: fetches a 1.5-year window (Jan-Dec + 6 extra months)
    to capture seasons that cross the year boundary.
    Post-processing: reassigns seasons to their onset year, filters to
    MAM/OND onset windows for equatorial climates, removes duplicates.

Perhumid guard (Af climate protection):
    Raises ValueError for years where ALL three hold:
        1. Annual rainfall  > 1500 mm
        2. Months with total < 40 mm  <= 3
        3. Days with >= 1 mm precip    > 200

Fixed-season mode (--fixed-season):
    Bypasses all automatic detection. The user supplies one or two
    season windows as "MM-DD:MM-DD" tokens separated by commas.
    Each token is applied to every year in [start_year, end_year].

    Single season  : --fixed-season "03-01:05-31"
    Two seasons    : --fixed-season "03-01:05-31,10-01:12-15"

    If the cessation month/day is earlier than the onset month/day the
    season is treated as year-crossing (onset in year N, cessation in N+1).

Dependencies: pandas, numpy, climate_toolkit
"""

import pandas as pd
import numpy as np
import math
import json
import argparse
import sys
import warnings
from datetime import datetime, date, timedelta
from typing import Tuple, Dict, List, Any, Optional
from pathlib import Path

warnings.filterwarnings("ignore")

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fetch_data.preprocess_data.preprocess_data import preprocess_data

HISTORICAL_SOURCES = ['era_5', 'agera_5']
FALLBACK_COMBO     = ('chirps', 'chirts')

# Perhumid thresholds
PERHUMID_ANNUAL_MM      = 1500
PERHUMID_LOW_MONTH_MM   = 40
PERHUMID_MAX_LOW_MONTHS = 3
PERHUMID_MIN_RAINY_DAYS = 200

# Fixed-season helpers
def _parse_fixed_season_token(token: str) -> Tuple[Tuple[int, int], Tuple[int, int]]:
    """
    Parse a single "MM-DD:MM-DD" token.
    Returns ((onset_month, onset_day), (cessation_month, cessation_day)).
    Raises ValueError on bad format.
    """
    parts = token.strip().split(":")
    if len(parts) != 2:
        raise ValueError(
            f"Fixed-season token must be 'MM-DD:MM-DD', got: {token!r}"
        )
    onset_str, cess_str = parts

    def _parse_md(s):
        try:
            m, d = s.strip().split("-")
            return int(m), int(d)
        except Exception:
            raise ValueError(f"Expected MM-DD, got: {s!r}")

    return _parse_md(onset_str), _parse_md(cess_str)


def parse_fixed_seasons(fixed_season_arg: str) -> List[Dict]:
    """
    Parse the full --fixed-season argument string.
    Accepts one or two "MM-DD:MM-DD" tokens separated by a comma.

    Returns a list of dicts:
        [{'onset_md': (MM, DD), 'cessation_md': (MM, DD)}, ...]
    """
    tokens  = [t.strip() for t in fixed_season_arg.split(",") if t.strip()]
    seasons = []
    for token in tokens:
        onset_md, cess_md = _parse_fixed_season_token(token)
        seasons.append({"onset_md": onset_md, "cessation_md": cess_md})
    if not seasons:
        raise ValueError("--fixed-season produced no valid tokens.")
    if len(seasons) > 2:
        raise ValueError("At most two fixed seasons are supported.")
    return seasons

def build_fixed_season_results(
    fixed_seasons: List[Dict],
    start_year: int,
    end_year: int,
) -> Dict[int, List[Dict]]:
    """
    Construct a results dict identical in shape to the output of
    fetch_and_analyze_years(), but using user-supplied fixed dates.

    Year-crossing seasons (e.g. onset Nov, cessation Feb) are handled:
    onset is placed in year N, cessation in year N+1.

    Parameters
    ----------
    fixed_seasons : list of dicts from parse_fixed_seasons()
    start_year    : first year
    end_year      : last year (inclusive)

    Returns
    -------
    Dict[int, List[Dict]]  — keyed by onset year
    """
    results: Dict[int, List[Dict]] = {y: [] for y in range(start_year, end_year + 1)}

    for year in range(start_year, end_year + 1):
        for season_def in fixed_seasons:
            (o_m, o_d) = season_def["onset_md"]
            (c_m, c_d) = season_def["cessation_md"]

            try:
                onset_date = date(year, o_m, o_d)
            except ValueError as exc:
                print(f"  [WARNING] Invalid onset date {year}-{o_m:02d}-{o_d:02d}: {exc}")
                continue

            # Year-crossing: cessation earlier in calendar than onset
            cess_year = year + 1 if (c_m, c_d) < (o_m, o_d) else year
            try:
                cessation_date = date(cess_year, c_m, c_d)
            except ValueError as exc:
                print(
                    f"  [WARNING] Invalid cessation date "
                    f"{cess_year}-{c_m:02d}-{c_d:02d}: {exc}"
                )
                continue

            length_days = (cessation_date - onset_date).days + 1

            results[year].append({
                "onset":          pd.Timestamp(onset_date),
                "cessation":      pd.Timestamp(cessation_date),
                "length_days":    length_days,
                "regime":         "fixed",
                "annual_rain_mm": None,
                "params_used":    "fixed-season",
            })
            print(
                f"  Fixed season applied: "
                f"{onset_date.strftime('%Y-%m-%d')} → "
                f"{cessation_date.strftime('%Y-%m-%d')} | {length_days}d"
            )
    return results

# Data access
def get_climate_data(
    lat: float,
    lon: float,
    start_date: str,
    end_date: str,
    force_source: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch standardised daily climate data from the climate toolkit.
    Source priority (unless force_source is set):
        1. era_5
        2. agera_5
        3. chirps + chirts  (merged fallback)
    Returns DataFrame with columns: date, tmax, tmin, precip.
    Raises RuntimeError when all sources are exhausted.
    """
    date_from = date.fromisoformat(start_date)
    date_to   = date.fromisoformat(end_date)
    df = _fetch_raw(lat, lon, date_from, date_to, force_source)
    if df is None or df.empty:
        raise RuntimeError("All data sources exhausted.")
    result = pd.DataFrame()
    result['date']   = pd.to_datetime(df['date'])
    result['tmax']   = df.get('max_temperature')
    result['tmin']   = df.get('min_temperature')
    result['precip'] = df.get('precipitation')
    return result

def _fetch_raw(lat, lon, date_from, date_to, force_source) -> Optional[pd.DataFrame]:
    coord = (lat, lon)
    if force_source == 'chirps+chirts':
        return _merge_chirps_chirts(coord, date_from, date_to)
    if force_source:
        return preprocess_data(source=force_source, location_coord=coord,
                               date_from=date_from, date_to=date_to)
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


# ET0 — Hargreaves
def deg2rad(deg):
    return deg * math.pi / 180.0

def day_of_year(d):
    return d.timetuple().tm_yday

def sol_dec(J):
    return 0.409 * math.sin((2 * math.pi / 365) * J - 1.39)

def inv_rel_dist_earth_sun(J):
    return 1 + 0.033 * math.cos((2 * math.pi / 365) * J)

def sunset_hour_angle(lat, sol_decl):
    val = -math.tan(lat) * math.tan(sol_decl)
    return math.acos(max(min(val, 1), -1))

def et_rad(lat, sol_decl, sha, ird):
    Gsc = 0.0820
    return ((24 * 60) / math.pi) * Gsc * ird * (
        sha * math.sin(lat) * math.sin(sol_decl) +
        math.cos(lat) * math.cos(sol_decl) * math.sin(sha)
    )

def hargreaves(tmin, tmax, Ra):
    if tmax is None or tmin is None or pd.isna(tmax) or pd.isna(tmin) or tmax < tmin:
        return None
    Tmean = (tmax + tmin) / 2
    return 0.0023 * math.sqrt(tmax - tmin) * (Tmean + 17.8) * Ra

def add_et0(df: pd.DataFrame, lat: float) -> pd.DataFrame:
    """Compute Hargreaves ET0 and add column ET0_mm_day."""
    lat_rad   = deg2rad(lat)
    et_values = []
    for _, row in df.iterrows():
        J        = day_of_year(row['date'].to_pydatetime())
        sol_decl = sol_dec(J)
        ird      = inv_rel_dist_earth_sun(J)
        sha      = sunset_hour_angle(lat_rad, sol_decl)
        Ra       = et_rad(lat_rad, sol_decl, sha, ird)
        et_values.append(hargreaves(row['tmin'], row['tmax'], Ra))
    df = df.copy()
    df['ET0_mm_day'] = et_values
    return df

# Perhumid test
def is_perhumid_location(annual_rain, df, threshold_rain=1500,
                         threshold_low_rain_months=40, max_low_rain_months=3,
                         reference_year=None):
    """Test for perhumid Af climates. Returns (is_perhumid, num_low_rain_months, rainy_days_yr, monthly_precip)."""
    if reference_year is None:
        reference_year = df['date'].dt.year.mode()[0]
    ref_year_df   = df[df['date'].dt.year == reference_year]
    rainy_days_yr = int(np.sum(ref_year_df['precip'].fillna(0) >= 1.0))
    df_monthly    = df.copy()
    df_monthly['year']  = df_monthly['date'].dt.year
    df_monthly['month'] = df_monthly['date'].dt.month
    monthly_precip      = df_monthly.groupby(['year', 'month'])['precip'].sum()
    ref_year_months     = monthly_precip[reference_year]
    num_low_rain_months = int(np.sum(ref_year_months < threshold_low_rain_months))
    is_perhumid = (
        annual_rain > threshold_rain and
        num_low_rain_months <= max_low_rain_months and
        rainy_days_yr > 200
    )
    return is_perhumid, num_low_rain_months, rainy_days_yr, monthly_precip

# Regime detection
def detect_regime(df):
    """Enhanced regime detection. Returns a Series aligned to df index."""
    df = df.copy()
    df['year']  = df['date'].dt.year
    df['month'] = df['date'].dt.month
    monthly = df.groupby(['year', 'month'])['precip'].sum().reset_index()

    annual_totals           = monthly.groupby('year')['precip'].sum()
    monthly['annual_total'] = monthly['year'].map(annual_totals)

    monthly['is_peak'] = (
        (monthly['precip'] > 1.2 * monthly.groupby('year')['precip'].shift(1).fillna(0)) &
        (monthly['precip'] > 1.2 * monthly.groupby('year')['precip'].shift(-1).fillna(0)) &
        (monthly['precip'] > 0.2 * monthly['annual_total'])
    )
    yearly_peaks = monthly.groupby('year')['is_peak'].sum()
    peak_months  = monthly[monthly['is_peak']].groupby('year')['month'].apply(list)

    regime_dict = {}
    for year in df['year'].unique():
        if year not in yearly_peaks.index:
            regime_dict[year] = 'unimodal'
            continue
        n_peaks = yearly_peaks[year]
        if n_peaks == 2:
            regime_dict[year] = 'bimodal'
        elif n_peaks == 1:
            peak_month = peak_months[year][0] if year in peak_months.index else 6
            regime_dict[year] = 'year_crossing' if peak_month > 6 else 'unimodal'
        else:
            regime_dict[year] = 'erratic'

    return df['year'].map(regime_dict)

# Wet-spell confirmation
def has_wet_confirmation(precip_data, et0_data, start_idx, min_wet_days=3, annual_rain=800):
    """Adaptive wet confirmation based on annual rainfall."""
    if start_idx + 25 > len(precip_data):
        return False
    threshold     = 0.5 * et0_data[start_idx: start_idx + 25]
    precip_window = precip_data[start_idx: start_idx + 25]
    wet_streak    = 0
    for i, (p, t) in enumerate(zip(precip_window, threshold)):
        if p >= t:
            wet_streak += 1
            if wet_streak >= min_wet_days:
                return True
        else:
            wet_streak = 0
            max_dry_allowed = 3 if annual_rain < 600 else 2
            if i + max_dry_allowed <= len(precip_window):
                if sum(precip_window[i:i + max_dry_allowed] >= threshold[i:i + max_dry_allowed]) >= 1:
                    continue
            break
    return False

# Main adaptive onset/cessation detection
def detect_onset_cessation(df):
    """Fully adaptive onset/cessation detection for all African climates."""
    precip = df['precip'].fillna(0).to_numpy()
    et0    = df['ET0_mm_day'].fillna(0).to_numpy()
    dates  = df['date'].to_numpy()

    main_year   = df['date'].dt.year.mode()[0]
    year_df     = df[df['date'].dt.year == main_year]
    annual_rain = year_df['precip'].sum()

    is_perhumid, num_low_rain_months, rainy_days, monthly_precip = \
        is_perhumid_location(annual_rain, df, reference_year=main_year)
    if is_perhumid:
        raise ValueError(
            f"This is a perhumid location (annual rain={annual_rain:.0f}mm, "
            f"months with <40mm rainfall={num_low_rain_months}, "
            f"rainy days={rainy_days}). "
            "It is very wet year-round -- no real onset/cessation."
        )
    print(f"  ✓ Passed humidity test: {annual_rain:.0f}mm/yr, "
          f"low-rain months={num_low_rain_months}, rainy days={rainy_days}")

    if annual_rain < 600:
        min_rainy_days  = max(25, int(annual_rain * 0.08))
        min_wet_confirm = 2
        base_cess_days  = 18
        regime_multipliers = {'unimodal': 0.9, 'bimodal': 1.1,
                               'year_crossing': 0.8, 'erratic': 1.0}
    elif annual_rain > 1500:
        min_rainy_days  = max(90, int(annual_rain * 0.06))
        min_wet_confirm = 3
        base_cess_days  = 15
        regime_multipliers = {'unimodal': 1.2, 'bimodal': 1.0,
                               'year_crossing': 1.1, 'erratic': 1.3}
    else:
        min_rainy_days  = 45
        min_wet_confirm = 3
        base_cess_days  = 22
        regime_multipliers = {'unimodal': 1.0, 'bimodal': 1.15,
                               'year_crossing': 0.85, 'erratic': 1.0}

    print(f"  ✓ Adaptive params: {annual_rain:.0f}mm/yr | "
          f"min_days={min_rainy_days} | wet_confirm={min_wet_confirm} | "
          f"base_cess={base_cess_days}")

    regime_series = detect_regime(df)
    regime_array  = regime_series.fillna('unimodal').to_numpy()
    threshold     = 0.5 * et0
    rainy_flags   = precip >= threshold

    results = []
    i, n = 0, len(df)

    while i < n:
        if rainy_flags[i]:
            if not has_wet_confirmation(precip, et0, i, min_wet_confirm, annual_rain):
                i += 1
                continue

            onset_date     = dates[i]
            onset_regime   = regime_array[i]
            regime_mult    = regime_multipliers.get(onset_regime, 1.0)
            cess_threshold = int(base_cess_days * regime_mult)

            dry_counter    = 0
            j              = i + 1
            cessation_date = None

            while j < n:
                if not rainy_flags[j]:
                    dry_counter += 1
                    if dry_counter >= cess_threshold:
                        cessation_date = dates[j - cess_threshold]
                        break
                else:
                    dry_counter = 0
                j += 1

            end_for_duration = cessation_date if cessation_date else dates[-1]
            rainy_duration   = (
                pd.to_datetime(end_for_duration) - pd.to_datetime(onset_date)
            ).days + 1

            if rainy_duration >= min_rainy_days:
                results.append({
                    'onset':          onset_date,
                    'cessation':      cessation_date,
                    'length_days':    rainy_duration,
                    'regime':         onset_regime,
                    'annual_rain_mm': annual_rain,
                    'params_used': (
                        f"min_days={min_rainy_days},"
                        f"wet_confirm={min_wet_confirm},"
                        f"cess={cess_threshold}"
                    ),
                })

            if cessation_date:
                try:
                    idx_after_cess = next(
                        k for k, d in enumerate(dates)
                        if pd.to_datetime(d) == pd.to_datetime(cessation_date)
                    )
                    i = idx_after_cess + cess_threshold + 1
                except StopIteration:
                    break
            else:
                break
        else:
            i += 1
    return results

# Reassignment & deduplication
def reassign_spillover_seasons(results_dict, lat=0, start_year=None,
                                end_year=None, hemisphere="NH"):
    """Reassign seasons by onset year; keep only MAM/OND onsets for equatorial."""
    if 10 <= lat <= 20:
        return results_dict.copy()
    allowed_months = set(range(2, 6)) | set(range(10, 13))

    if start_year is None:
        start_year = min(results_dict.keys()) if results_dict else None
    if end_year is None:
        end_year = max(results_dict.keys()) if results_dict else None
    if start_year is None or end_year is None:
        return {}

    cleaned = {year: [] for year in range(start_year, end_year + 1)}

    for raw_year, seasons in sorted(results_dict.items()):
        for season in seasons or []:
            onset       = pd.to_datetime(season['onset'])
            onset_year  = onset.year
            onset_month = onset.month
            if onset_year < start_year or onset_year > end_year:
                continue
            if onset_month in allowed_months:
                cleaned[onset_year].append(season)
            else:
                print(f"  Dropped {onset_year} off-season: "
                      f"{onset.strftime('%Y-%m-%d')} (month {onset_month})")
    for year in cleaned:
        cleaned[year] = sorted(cleaned[year], key=lambda s: pd.to_datetime(s['onset']))
    return cleaned

def remove_duplicate_seasons(refined_results):
    """Remove duplicate seasons based on onset and length similarity."""
    deduped = {}
    for year, seasons in refined_results.items():
        unique = []
        for season in seasons:
            onset  = pd.to_datetime(season['onset'])
            length = season.get('length_days', 0)
            dup    = False
            for kept in unique:
                kept_onset  = pd.to_datetime(kept['onset'])
                kept_length = kept.get('length_days', 0)
                if abs((onset - kept_onset).days) <= 5 and abs(length - kept_length) <= 3:
                    print(f"  Duplicate dropped: {onset.strftime('%Y-%m-%d')} (matches existing)")
                    dup = True
                    break
            if not dup:
                unique.append(season)
        deduped[year] = unique
    return deduped

# 1.5-year window fetcher
def fetch_full_year_plus_cessation(lat, lon, year, source="auto", extra_months=6):
    """Fetch year + extra months into next year to capture late cessations."""
    force      = None if source == "auto" else source
    start_date = f"{year}-01-01"
    end_date   = f"{year + 1}-06-30"
    print(f"  Fetching {start_date} to {end_date} ...")
    df = get_climate_data(lat, lon, start_date, end_date, force_source=force)
    df = add_et0(df, lat)
    return df.sort_values('date').reset_index(drop=True)

# Multi-year orchestrator
def fetch_and_analyze_years(lat, lon, start_year, end_year,
                             extra_months=6, source="auto"):
    """Fetch independent 1.5-year windows per reference year and detect seasons."""
    results = {}

    for ref_year in range(start_year, end_year + 1):
        print(f"\nAnalyzing ref year {ref_year}")
        try:
            df_window = fetch_full_year_plus_cessation(
                lat, lon, ref_year, source=source, extra_months=extra_months
            )
            if df_window is None or df_window.empty:
                print(f"  Retrieved 0 days for {ref_year}")
                results[ref_year] = []
                continue
            print(f"  Retrieved {len(df_window)} days for ref year {ref_year}")
            seasons = detect_onset_cessation(df_window)

            if not seasons:
                print(f"  No seasons detected for {ref_year}")
            else:
                for idx, season in enumerate(seasons, 1):
                    onset = pd.to_datetime(season['onset']).strftime('%Y-%m-%d')
                    cessation = (
                        pd.to_datetime(season['cessation']).strftime('%Y-%m-%d')
                        if season['cessation']
                        else f"to {df_window['date'].iloc[-1].strftime('%Y-%m-%d')}"
                    )
                    print(f"  Season {idx}: {onset} → {cessation} | "
                          f"{season['regime']} | {season['length_days']}d")
            results[ref_year] = seasons

        except ValueError as e:
            print(f"  ⚠ Perhumid error for {ref_year}: {e}")
            results[ref_year] = []
        except Exception as e:
            print(f"  ✗ Error analyzing {ref_year}: {e}")
            results[ref_year] = []
    temp_results  = reassign_spillover_seasons(
        results, lat=lat, start_year=start_year, end_year=end_year
    )
    final_results = remove_duplicate_seasons(temp_results)
    return final_results

# Summary printer
def print_summary(results: dict, save_path: Optional[str] = None):
    """Print the FINAL SEASONS SUMMARY table and optionally save to CSV."""
    print("\n" + "=" * 70)
    print("FINAL SEASONS SUMMARY")
    print("=" * 70)

    rows = []
    for year, seasons in sorted(results.items()):
        print(f"Year {year}: {len(seasons)} season(s)")
        for i, s in enumerate(seasons, 1):
            onset = pd.to_datetime(s['onset']).strftime('%Y-%m-%d')
            cess  = (
                pd.to_datetime(s['cessation']).strftime('%Y-%m-%d')
                if s.get('cessation') else 'open'
            )
            print(f"  Season {i}: {onset} → {cess} | {s['regime']} | {s['length_days']}d")
            rows.append({
                'year':           year,
                'season_number':  i,
                'onset':          onset,
                'cessation':      cess,
                'regime':         s['regime'],
                'length_days':    s['length_days'],
                'annual_rain_mm': s.get('annual_rain_mm', ''),
                'params_used':    s.get('params_used', ''),
            })
    if save_path and rows:
        pd.DataFrame(rows).to_csv(save_path, index=False)
        print(f"\n✓ SAVED: {save_path}")

# CLI
def main() -> None:
    parser = argparse.ArgumentParser(
        description='Season analysis -- ERA5 / AgERA5 / CHIRPS+CHIRTS',
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument('--location',     required=True,
                        help='Coordinates as "lat,lon"  e.g. "-1.286,36.817"')
    parser.add_argument('--source',
                        choices=['era_5', 'agera_5', 'chirps+chirts', 'auto'],
                        default='auto',
                        help=(
                            "Data source.\n"
                            "  era_5         -- ERA5 reanalysis\n"
                            "  agera_5       -- AgERA5 / ERA5-Land\n"
                            "  chirps+chirts -- CHIRPS precipitation + CHIRTS temperature\n"
                            "  auto          -- tries era_5 -> agera_5 -> chirps+chirts  [default]"
                        ))
    parser.add_argument('--start-year',   type=int, required=True,  help='First reference year')
    parser.add_argument('--end-year',     type=int, required=True,  help='Last reference year')
    parser.add_argument('--extra-months', type=int, default=6,
                        help='Extra months beyond Dec for late cessations (default: 6)')
    parser.add_argument('--output-dir',   default='.',
                        help='Directory for CSV output (default: current dir)')
    parser.add_argument('--no-save',      action='store_true',
                        help='Skip saving the seasons CSV')
    parser.add_argument(
        '--fixed-season',
        default=None,
        metavar='MM-DD:MM-DD[,MM-DD:MM-DD]',
        help=(
            "Bypass automatic detection and use fixed calendar dates.\n"
            "Supply one or two 'onset:cessation' windows as MM-DD:MM-DD,\n"
            "separated by a comma for two seasons.\n\n"
            "Examples:\n"
            "  Single season : --fixed-season '03-01:05-31'\n"
            "  Two seasons   : --fixed-season '03-01:05-31,10-01:12-15'\n"
            "  Year-crossing : --fixed-season '11-01:02-28'\n\n"
            "Dates are applied to every year in [start-year, end-year].\n"
            "Year-crossing windows (cessation MM-DD < onset MM-DD) are\n"
            "automatically handled: cessation falls in year+1.\n"
            "No climate data is fetched when this flag is used."
        ),
    )
    args = parser.parse_args()

    try:
        lat, lon = map(float, args.location.split(','))
    except ValueError:
        print("Error: --location must be in 'lat,lon' format.")
        sys.exit(1)

    # Fixed-season path (no data fetching needed) 
    if args.fixed_season:
        print(
            f"Fixed-season mode | {lat:.4f}N, {lon:.4f}E | "
            f"{args.start_year}-{args.end_year}"
        )
        try:
            fixed_defs = parse_fixed_seasons(args.fixed_season)
        except ValueError as exc:
            print(f"Error parsing --fixed-season: {exc}")
            sys.exit(1)

        print(f"  Parsed {len(fixed_defs)} fixed season window(s):")
        for fd in fixed_defs:
            (o_m, o_d), (c_m, c_d) = fd["onset_md"], fd["cessation_md"]
            cross = " (year-crossing)" if (c_m, c_d) < (o_m, o_d) else ""
            print(f"    {o_m:02d}-{o_d:02d} → {c_m:02d}-{c_d:02d}{cross}")
        results = build_fixed_season_results(
            fixed_defs, args.start_year, args.end_year
        )

    # Automatic detection path 
    else:
        print(f"Analyzing {lat:.4f}N, {lon:.4f}E | "
              f"{args.start_year}-{args.end_year} | source={args.source}")
        results = fetch_and_analyze_years(
            lat, lon,
            start_year=args.start_year,
            end_year=args.end_year,
            extra_months=args.extra_months,
            source=args.source,
        )

    # Save & print
    save_path = None
    if not args.no_save:
        mode     = "fixed" if args.fixed_season else args.source
        filename = (f"seasons_{lat:.4f}_{lon:.4f}_"
                    f"{args.start_year}_{args.end_year}_{mode}.csv")
        save_path = str(Path(args.output_dir) / filename)
    print_summary(results, save_path=save_path)
    print("\nAnalysis complete!")

if __name__ == '__main__':
    main()


# Automatic detection (unchanged behaviour):
# python climate_tookit/season_analysis/seasons.py --location="-1.286,36.817" --start-year 2018 --end-year 2020 --source era_5
# python climate_tookit/season_analysis/seasons.py --location="-1.286,36.817" --start-year 2018 --end-year 2020 --source agera_5
# python climate_tookit/season_analysis/seasons.py --location="-1.286,36.817" --start-year 2015 --end-year 2016 --source chirps+chirts
# python climate_tookit/season_analysis/seasons.py --location="-1.286,36.817" --start-year 2015 --end-year 2020 --source agera_5 --output-dir ./results

# Fixed single season (MAM):
# python climate_tookit/season_analysis/seasons.py --location="-1.286,36.817" --start-year 2015 --end-year 2020 --fixed-season "03-01:05-31"

# Fixed two seasons (MAM + OND):
# python climate_tookit/season_analysis/seasons.py --location="-1.286,36.817" --start-year 2015 --end-year 2020 --fixed-season "03-01:05-31,10-01:12-15"

# Fixed year-crossing season:
# python climate_tookit/season_analysis/seasons.py --location="-1.286,36.817" --start-year 2015 --end-year 2020 --fixed-season "11-01:02-28"

# Fixed season, no CSV save:
# python climate_tookit/season_analysis/seasons.py --location="-1.286,36.817" --start-year 2018 --end-year 2022 --fixed-season "04-15:07-10" --no-save