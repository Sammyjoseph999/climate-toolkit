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
Perhumid guard (used internally during ETO detection):
    Raises ValueError for years where ALL three hold:
        1. Annual rainfall  > 1400 mm
        2. Months with total < 40 mm  <= 3
        3. Days with >= 1 mm precip    > 1
Humid test (displayed in output):
    A location/year is classified as "Humid" when BOTH hold:
        1. Annual rainfall > 1400 mm
        2. Months with total rainfall < 40 mm  <=  3
Fixed-season mode (--fixed-season):
    Bypasses automatic onset/cessation detection but:
      • Still fetches climate data via --source for rainfall statistics
      • Runs a full ETO-based onset/cessation analysis WITHIN each fixed window
        and reports the detected sub-season alongside the fixed-window stats
    Supply one or two season windows as "MM-DD:MM-DD" tokens separated by a comma.
    Year-crossing (cessation MM-DD < onset MM-DD) is handled automatically.
Per-season statistics (both modes):
    - Total rainfall (mm)
    - Rainy days  : days with precipitation >= 1 mm
    - Dry days    : days with precipitation <  1 mm
    - Dry spells  : count of distinct runs of 7+ consecutive dry days
Per-year summary (both modes):
    - Annual total rainfall (full calendar year)
    - Humid test result
Dependencies: pandas, numpy, climate_toolkit
"""

import pandas as pd
import numpy as np
import math
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

# Internal perhumid guard thresholds (detection)
PERHUMID_ANNUAL_MM      = 1400
PERHUMID_LOW_MONTH_MM   = 40
PERHUMID_MAX_LOW_MONTHS = 3
PERHUMID_MIN_RAINY_DAYS = 1

# Display humid-test thresholds
HUMID_ANNUAL_MM_THRESHOLD   = 1400
HUMID_LOW_MONTH_MM          = 40
HUMID_MAX_LOW_RAIN_MONTHS   = 3

# Humid test (display)
def check_humid(annual_rain_mm: float, year_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Classify a year as humid or not:
        1. Annual rainfall > 1400 mm
        2. Count of months with total rainfall < 40 mm  <=  3
    Parameters
    ----------
    annual_rain_mm : full-year precipitation total
    year_df        : daily DataFrame (single calendar year) with 'date' & 'precip'
    Returns
    -------
    dict:  is_humid, low_rain_months, result_str
    """
    df_m = year_df.copy()
    df_m['month'] = df_m['date'].dt.month
    monthly_totals  = df_m.groupby('month')['precip'].apply(lambda x: x.fillna(0).sum())
    low_rain_months = int((monthly_totals < HUMID_LOW_MONTH_MM).sum())
    is_humid        = (annual_rain_mm > HUMID_ANNUAL_MM_THRESHOLD) and (low_rain_months <= HUMID_MAX_LOW_RAIN_MONTHS)

    if is_humid:
        result_str = (
            f"Humid  "
            f"(annual={annual_rain_mm:.1f} mm > {HUMID_ANNUAL_MM_THRESHOLD} mm, "
            f"low-rain months={low_rain_months} ≤ {HUMID_MAX_LOW_RAIN_MONTHS})"
        )
    else:
        reasons = []
        if annual_rain_mm <= HUMID_ANNUAL_MM_THRESHOLD:
            reasons.append(f"annual={annual_rain_mm:.1f} mm ≤ {HUMID_ANNUAL_MM_THRESHOLD} mm")
        if low_rain_months > HUMID_MAX_LOW_RAIN_MONTHS:
            reasons.append(f"low-rain months={low_rain_months} > {HUMID_MAX_LOW_RAIN_MONTHS}")
        result_str = f"Not humid  ({', '.join(reasons)})"

    return dict(is_humid=is_humid, low_rain_months=low_rain_months, result_str=result_str)

# Fixed-season helpers
def _parse_fixed_season_token(token: str) -> Tuple[Tuple[int, int], Tuple[int, int]]:
    """Parse a single 'MM-DD:MM-DD' token into two (month, day) tuples."""
    parts = token.strip().split(":")
    if len(parts) != 2:
        raise ValueError(f"Fixed-season token must be 'MM-DD:MM-DD', got: {token!r}")
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
    Returns [{'onset_md': (MM,DD), 'cessation_md': (MM,DD)}, ...].
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

# Per-season statistics
def compute_season_stats(df: pd.DataFrame, onset, cessation) -> Dict[str, Any]:
    """
    Compute rainfall statistics for an onset-cessation window.
    Returns total_rainfall_mm, rainy_days, dry_days, dry_spells.
    A dry spell is counted once it reaches 7 consecutive dry days.
    """
    onset_ts  = pd.Timestamp(onset)
    cess_ts   = pd.Timestamp(cessation)
    season_df = df[(df['date'] >= onset_ts) & (df['date'] <= cess_ts)].copy()

    if season_df.empty:
        return dict(total_rainfall_mm=0.0, rainy_days=0, dry_days=0, dry_spells=0)
    precip         = season_df['precip'].fillna(0).to_numpy()
    total_rainfall = float(np.sum(precip))
    rainy_days     = int(np.sum(precip >= 1.0))
    dry_days       = int(np.sum(precip <  1.0))
    dry_spells  = 0
    consecutive = 0
    for p in precip:
        if p < 1.0:
            consecutive += 1
            if consecutive == 7:
                dry_spells += 1
        else:
            consecutive = 0
    return dict(
        total_rainfall_mm = round(total_rainfall, 1),
        rainy_days        = rainy_days,
        dry_days          = dry_days,
        dry_spells        = dry_spells,
    )
# Season analysis within a fixed window
def run_eto_in_window(df_with_et0: pd.DataFrame, onset, cessation) -> List[Dict]:
    """
    Slice df to [onset, cessation] and run the full ETO-based detection.
    Returns a list of detected seasons (may be empty if the window is too
    short, perhumid, or contains no wet spell).
    Each season dict already includes rainfall statistics via
    detect_onset_cessation -> compute_season_stats.
    """
    onset_ts  = pd.Timestamp(onset)
    cess_ts   = pd.Timestamp(cessation)
    window_df = (
        df_with_et0[(df_with_et0['date'] >= onset_ts) & (df_with_et0['date'] <= cess_ts)]
        .copy()
        .reset_index(drop=True)
    )
    if len(window_df) < 14:
        print("    [ETO] Window too short for detection (<14 days).")
        return []
    try:
        eto_seasons = detect_onset_cessation(window_df)
        return eto_seasons
    except ValueError as exc:
        print(f"    [ETO] Detection skipped: {exc}")
        return []
    except Exception as exc:
        print(f"    [ETO] Detection failed: {exc}")
        return []

# Data access
def get_climate_data(
    lat         : float,
    lon         : float,
    start_date  : str,
    end_date    : str,
    force_source: Optional[str] = None,
) -> pd.DataFrame:
    """
    Fetch standardised daily climate data (date, tmax, tmin, precip).
    Source priority: era_5 → agera_5 → chirps+chirts.
    Raises RuntimeError when all sources are exhausted.
    """
    date_from = date.fromisoformat(start_date)
    date_to   = date.fromisoformat(end_date)
    df = _fetch_raw(lat, lon, date_from, date_to, force_source)
    if df is None or df.empty:
        raise RuntimeError("All data sources exhausted.")
    result           = pd.DataFrame()
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
def deg2rad(deg):        return deg * math.pi / 180.0
def day_of_year(d):      return d.timetuple().tm_yday
def sol_dec(J):          return 0.409 * math.sin((2 * math.pi / 365) * J - 1.39)
def inv_rel_dist(J):     return 1 + 0.033 * math.cos((2 * math.pi / 365) * J)

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
        decl     = sol_dec(J)
        ird      = inv_rel_dist(J)
        sha      = sunset_hour_angle(lat_rad, decl)
        Ra       = et_rad(lat_rad, decl, sha, ird)
        et_values.append(hargreaves(row['tmin'], row['tmax'], Ra))
    df = df.copy()
    df['ET0_mm_day'] = et_values
    return df

# Perhumid guard (internal — used by detect_onset_cessation)
def is_perhumid_location(annual_rain, df, threshold_rain=1400,
                         threshold_low_rain_months=40, max_low_rain_months=3,
                         reference_year=None):
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
        rainy_days_yr > 1
    )
    return is_perhumid, num_low_rain_months, rainy_days_yr, monthly_precip

# Regime detection
def detect_regime(df):
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
    regime_dict  = {}
    for year in df['year'].unique():
        if year not in yearly_peaks.index:
            regime_dict[year] = 'unimodal'; continue
        n_peaks = yearly_peaks[year]
        if n_peaks == 2:
            regime_dict[year] = 'bimodal'
        elif n_peaks == 1:
            pm = peak_months[year][0] if year in peak_months.index else 6
            regime_dict[year] = 'year_crossing' if pm > 6 else 'unimodal'
        else:
            regime_dict[year] = 'erratic'
    return df['year'].map(regime_dict)

# Wet-spell confirmation
def has_wet_confirmation(precip_data, et0_data, start_idx, min_wet_days=3, annual_rain=800):
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

# Onset/cessation detection
def detect_onset_cessation(df):
    """Fully adaptive onset/cessation detection. Returns list of season dicts."""
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
            f"Perhumid location (annual rain={annual_rain:.0f}mm, "
            f"low-rain months={num_low_rain_months}, rainy days={rainy_days}). "
            "No clear onset/cessation."
        )
    print(f"  ✓ Humidity guard passed: {annual_rain:.0f}mm/yr, "
          f"low-rain months={num_low_rain_months}, rainy days={rainy_days}")

    if annual_rain < 600:
        min_rainy_days     = max(25, int(annual_rain * 0.08))
        min_wet_confirm    = 2
        base_cess_days     = 18
        regime_multipliers = {'unimodal': 0.9, 'bimodal': 1.1, 'year_crossing': 0.8, 'erratic': 1.0}
    elif annual_rain > 1400:
        min_rainy_days     = max(90, int(annual_rain * 0.06))
        min_wet_confirm    = 3
        base_cess_days     = 15
        regime_multipliers = {'unimodal': 1.2, 'bimodal': 1.0, 'year_crossing': 1.1, 'erratic': 1.3}
    else:
        min_rainy_days     = 45
        min_wet_confirm    = 3
        base_cess_days     = 15
        regime_multipliers = {'unimodal': 1.0, 'bimodal': 1.15, 'year_crossing': 0.85, 'erratic': 1.0}

    print(f"  ✓ Adaptive params: min_days={min_rainy_days} | "
          f"wet_confirm={min_wet_confirm} | base_cess={base_cess_days}")

    regime_series = detect_regime(df)
    regime_array  = regime_series.fillna('unimodal').to_numpy()
    threshold     = 0.5 * et0
    rainy_flags   = precip >= threshold

    results = []
    i, n    = 0, len(df)

    while i < n:
        if rainy_flags[i]:
            if not has_wet_confirmation(precip, et0, i, min_wet_confirm, annual_rain):
                i += 1; continue

            onset_date     = dates[i]
            onset_regime   = regime_array[i]
            cess_threshold = int(base_cess_days * regime_multipliers.get(onset_regime, 1.0))
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
                stats = compute_season_stats(df, onset_date, end_for_duration)
                results.append({
                    'onset':          onset_date,
                    'cessation':      cessation_date,
                    'length_days':    rainy_duration,
                    'regime':         onset_regime,
                    'annual_rain_mm': annual_rain,
                    'params_used':    (
                        f"min_days={min_rainy_days},"
                        f"wet_confirm={min_wet_confirm},"
                        f"cess={cess_threshold}"
                    ),
                    **stats,
                })
            if cessation_date:
                try:
                    idx_after = next(
                        k for k, d in enumerate(dates)
                        if pd.to_datetime(d) == pd.to_datetime(cessation_date)
                    )
                    i = idx_after + cess_threshold + 1
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
    if 8 <= lat <= 20:
        return results_dict.copy()
    allowed_months = set(range(1, 6)) | set(range(8, 13))
    if start_year is None: start_year = min(results_dict.keys()) if results_dict else None
    if end_year   is None: end_year   = max(results_dict.keys()) if results_dict else None
    if start_year is None or end_year is None: return {}
    cleaned = {year: [] for year in range(start_year, end_year + 1)}
    for raw_year, seasons in sorted(results_dict.items()):
        for season in seasons or []:
            onset       = pd.to_datetime(season['onset'])
            onset_year  = onset.year
            onset_month = onset.month
            if onset_year < start_year or onset_year > end_year: continue
            if onset_month in allowed_months:
                cleaned[onset_year].append(season)
            else:
                print(f"  Dropped {onset_year} off-season: "
                      f"{onset.strftime('%Y-%m-%d')} (month {onset_month})")
    for year in cleaned:
        cleaned[year] = sorted(cleaned[year], key=lambda s: pd.to_datetime(s['onset']))
    return cleaned

def remove_duplicate_seasons(refined_results):
    deduped = {}
    for year, seasons in refined_results.items():
        unique = []
        for season in seasons:
            onset  = pd.to_datetime(season['onset'])
            length = season.get('length_days', 0)
            dup    = False
            for kept in unique:
                if (abs((onset - pd.to_datetime(kept['onset'])).days) <= 5 and
                        abs(length - kept.get('length_days', 0)) <= 3):
                    print(f"  Duplicate dropped: {onset.strftime('%Y-%m-%d')}")
                    dup = True; break
            if not dup:
                unique.append(season)
        deduped[year] = unique
    return deduped

# Annual stats helper
def compute_annual_stats(df: pd.DataFrame, year: int) -> Tuple[float, Dict[str, Any]]:
    """
    Extract full-year precip from df, compute annual total and humid test.
    Returns (annual_rain_mm, humid_info_dict).
    """
    ref_df      = df[df['date'].dt.year == year].copy()
    annual_rain = float(ref_df['precip'].fillna(0).sum())
    humid_info  = check_humid(annual_rain, ref_df)
    return round(annual_rain, 1), humid_info

# 1.5-year window fetcher
def fetch_full_year_plus_cessation(lat, lon, year, source="auto", extra_months=6):
    force      = None if source == "auto" else source
    start_date = f"{year}-01-01"
    end_date   = f"{year + 1}-06-30"
    print(f"  Fetching {start_date} to {end_date} ...")
    df = get_climate_data(lat, lon, start_date, end_date, force_source=force)
    df = add_et0(df, lat)
    return df.sort_values('date').reset_index(drop=True)

# Multi-year orchestrator — automatic detection
def fetch_and_analyze_years(
    lat, lon, start_year, end_year, extra_months=6, source="auto"
) -> Tuple[Dict[int, List[Dict]], Dict[int, Dict]]:
    """
    Returns
    -------
    seasons_dict : {year: [season_dict, ...]}
    annual_dict  : {year: {annual_rain_mm, is_humid, low_rain_months, result_str}}
    """
    seasons_dict : Dict[int, List[Dict]] = {}
    annual_dict  : Dict[int, Dict]       = {}

    for ref_year in range(start_year, end_year + 1):
        print(f"\nAnalyzing ref year {ref_year}")
        try:
            df_window = fetch_full_year_plus_cessation(
                lat, lon, ref_year, source=source, extra_months=extra_months
            )
            if df_window is None or df_window.empty:
                print(f"  Retrieved 0 days for {ref_year}")
                seasons_dict[ref_year] = []
                annual_dict[ref_year]  = {}
                continue
            print(f"  Retrieved {len(df_window)} days")

            # Annual stats (reference year only)
            annual_rain, humid_info = compute_annual_stats(df_window, ref_year)
            annual_dict[ref_year]   = {
                'annual_rain_mm':    annual_rain,
                'is_humid':          humid_info['is_humid'],
                'low_rain_months':   humid_info['low_rain_months'],
                'result_str':        humid_info['result_str'],
            }
            print(f"  Annual rainfall={annual_rain} mm | {humid_info['result_str']}")

            seasons = detect_onset_cessation(df_window)
            if not seasons:
                print(f"  No seasons detected for {ref_year}")
            else:
                for idx, s in enumerate(seasons, 1):
                    onset = pd.to_datetime(s['onset']).strftime('%Y-%m-%d')
                    cess  = (pd.to_datetime(s['cessation']).strftime('%Y-%m-%d')
                             if s['cessation'] else f"→{df_window['date'].iloc[-1].strftime('%Y-%m-%d')}")
                    print(
                        f"  Season {idx}: {onset} → {cess} | "
                        f"{s['regime']} | {s['length_days']}d | "
                        f"rain={s.get('total_rainfall_mm')} mm | "
                        f"rainy={s.get('rainy_days')}d | "
                        f"dry={s.get('dry_days')}d | "
                        f"dry_spells={s.get('dry_spells')}"
                    )
            seasons_dict[ref_year] = seasons
        except ValueError as e:
            print(f"  ⚠ Perhumid error for {ref_year}: {e}")
            seasons_dict[ref_year] = []
            annual_dict[ref_year]  = {}
        except Exception as e:
            print(f"  ✗ Error analyzing {ref_year}: {e}")
            seasons_dict[ref_year] = []
            annual_dict[ref_year]  = {}
    temp    = reassign_spillover_seasons(seasons_dict, lat=lat,
                                         start_year=start_year, end_year=end_year)
    final   = remove_duplicate_seasons(temp)
    # Preserve annual_dict for any years that were filtered
    final_annual = {y: annual_dict.get(y, {}) for y in range(start_year, end_year + 1)}
    return final, final_annual

# Multi-year orchestrator — fixed season
def fetch_and_analyze_years_fixed(
    lat          : float,
    lon          : float,
    fixed_seasons: List[Dict],
    start_year   : int,
    end_year     : int,
    source       : str = "auto",
) -> Tuple[Dict[int, List[Dict]], Dict[int, Dict]]:
    """
    Apply fixed season windows to every year.
    Fetches climate data via --source, computes:
      • Per-season rainfall statistics (fixed window)
      • ETO-based onset/cessation analysis WITHIN the fixed window
      • Annual total rainfall and humid test
    Returns
    -------
    seasons_dict : {year: [season_dict, ...]}
    annual_dict  : {year: {annual_rain_mm, is_humid, low_rain_months, result_str}}
    """
    seasons_dict : Dict[int, List[Dict]] = {y: [] for y in range(start_year, end_year + 1)}
    annual_dict  : Dict[int, Dict]       = {}
    force = None if source == "auto" else source

    for year in range(start_year, end_year + 1):
        print(f"\nFixed-season year {year} | source={source}")

        # Resolve all season dates for this year
        resolved = []
        for sd in fixed_seasons:
            (o_m, o_d) = sd["onset_md"]
            (c_m, c_d) = sd["cessation_md"]
            cess_year   = year + 1 if (c_m, c_d) < (o_m, o_d) else year
            try:
                resolved.append((date(year, o_m, o_d), date(cess_year, c_m, c_d)))
            except ValueError as exc:
                print(f"  [WARNING] Invalid date: {exc}")
        if not resolved:
            annual_dict[year] = {}
            continue

        # Fetch data: full calendar year + any year-crossing tail, in one call
        fetch_start = f"{year}-01-01"
        fetch_end   = max(date(year, 12, 31), max(c for _, c in resolved)).strftime("%Y-%m-%d")
        print(f"  Fetching {fetch_start} to {fetch_end} ...")

        try:
            df = get_climate_data(lat, lon, fetch_start, fetch_end, force_source=force)
            df = add_et0(df, lat)
            print(f"  Retrieved {len(df)} days")
        except Exception as exc:
            print(f"  ✗ Data fetch failed: {exc} — stats will be n/a")
            df = None

        # Annual stats (calendar year only)
        if df is not None and not df.empty:
            annual_rain, humid_info = compute_annual_stats(df, year)
            annual_dict[year] = {
                'annual_rain_mm':  annual_rain,
                'is_humid':        humid_info['is_humid'],
                'low_rain_months': humid_info['low_rain_months'],
                'result_str':      humid_info['result_str'],
            }
            print(f"  Annual rainfall={annual_rain} mm | {humid_info['result_str']}")
        else:
            annual_dict[year] = {}

        # Build each fixed season
        for onset_date, cessation_date in resolved:
            length_days = (cessation_date - onset_date).days + 1
            cross_note  = " (year-crossing)" if cessation_date.year != year else ""

            # Fixed-window rainfall stats
            if df is not None and not df.empty:
                stats = compute_season_stats(df, onset_date, cessation_date)
            else:
                stats = dict(total_rainfall_mm=None, rainy_days=None,
                             dry_days=None, dry_spells=None)

            # Season analysis within the fixed window
            print(f"  Running Season analysis within "
                  f"{onset_date.strftime('%Y-%m-%d')} → "
                  f"{cessation_date.strftime('%Y-%m-%d')}{cross_note} ...")
            if df is not None and not df.empty:
                eto_seasons = run_eto_in_window(df, onset_date, cessation_date)
            else:
                eto_seasons = []

            if eto_seasons:
                for k, es in enumerate(eto_seasons, 1):
                    e_on  = pd.to_datetime(es['onset']).strftime('%Y-%m-%d')
                    e_off = (pd.to_datetime(es['cessation']).strftime('%Y-%m-%d')
                             if es['cessation'] else 'open')
                    print(
                        f"    ETO sub-season {k}: {e_on} → {e_off} | "
                        f"{es['regime']} | {es['length_days']}d | "
                        f"rain={es.get('total_rainfall_mm')} mm | "
                        f"rainy={es.get('rainy_days')}d | "
                        f"dry={es.get('dry_days')}d | "
                        f"dry_spells={es.get('dry_spells')}"
                    )
            else:
                print("    ETO: no sub-season detected within window")

            season_dict = dict(
                onset          = pd.Timestamp(onset_date),
                cessation      = pd.Timestamp(cessation_date),
                length_days    = length_days,
                regime         = "fixed",
                annual_rain_mm = annual_dict[year].get('annual_rain_mm'),
                params_used    = "fixed-season",
                eto_seasons    = eto_seasons,
                **stats,
            )
            seasons_dict[year].append(season_dict)
            print(
                f"  Fixed window: "
                f"{onset_date.strftime('%Y-%m-%d')} → "
                f"{cessation_date.strftime('%Y-%m-%d')}{cross_note} | "
                f"{length_days}d | "
                f"rain={stats['total_rainfall_mm']} mm | "
                f"rainy={stats['rainy_days']}d | "
                f"dry={stats['dry_days']}d | "
                f"dry_spells={stats['dry_spells']}"
            )
    return seasons_dict, annual_dict

# Summary printer
def _fmt(v, suffix=""): return f"{v}{suffix}" if v is not None else "n/a"

def print_summary(
    seasons_dict : Dict[int, List[Dict]],
    annual_dict  : Dict[int, Dict],
    save_path    : Optional[str] = None,
):
    """
    Print FINAL SEASONS SUMMARY.
    For each year:
        - Per-season block: fixed window (or auto-detected) stats
          + ETO sub-season analysis (fixed mode only)
        - Year footer: Annual total rainfall + Humid test
    Optionally saves to CSV.
    """
    print("\n" + "=" * 70)
    print("FINAL SEASONS SUMMARY")
    print("=" * 70)

    rows = []
    for year, seasons in sorted(seasons_dict.items()):
        ann       = annual_dict.get(year, {})
        ann_rain  = ann.get('annual_rain_mm')
        humid_str = ann.get('result_str')

        print(f"\nYear {year}: {len(seasons)} season(s)")

        for i, s in enumerate(seasons, 1):
            onset = pd.to_datetime(s['onset']).strftime('%Y-%m-%d')
            cess  = (pd.to_datetime(s['cessation']).strftime('%Y-%m-%d')
                     if s.get('cessation') else 'open')
            print(
                f"\n  Season {i}: {onset} → {cess} | "
                f"{s['regime']} | {s['length_days']}d"
            )
            print(f"    Total rainfall : {_fmt(s.get('total_rainfall_mm'), ' mm')}")
            print(f"    Rainy days     : {_fmt(s.get('rainy_days'), ' days')}  (precip ≥ 1 mm)")
            print(f"    Dry days       : {_fmt(s.get('dry_days'),   ' days')}  (precip < 1 mm)")
            print(f"    Dry spells     : {_fmt(s.get('dry_spells'))}  (runs of ≥ 7 consecutive dry days)")

            # ETO sub-season analysis (fixed mode only)
            eto_seasons = s.get('eto_seasons')
            if eto_seasons is not None:
                print(f"    {'─' * 50}")
                print(f"    Season analysis within fixed window:")
                if not eto_seasons:
                    print(f"      No ETO-based season detected within window")
                else:
                    for j, es in enumerate(eto_seasons, 1):
                        e_on  = pd.to_datetime(es['onset']).strftime('%Y-%m-%d')
                        e_off = (pd.to_datetime(es['cessation']).strftime('%Y-%m-%d')
                                 if es.get('cessation') else 'open')
                        print(
                            f"      ETO sub-season {j}: {e_on} → {e_off} | "
                            f"{es['regime']} | {es['length_days']}d"
                        )
                        print(f"        Total rainfall : {_fmt(es.get('total_rainfall_mm'), ' mm')}")
                        print(f"        Rainy days     : {_fmt(es.get('rainy_days'), ' days')}  (precip ≥ 1 mm)")
                        print(f"        Dry days       : {_fmt(es.get('dry_days'),   ' days')}  (precip < 1 mm)")
                        print(f"        Dry spells     : {_fmt(es.get('dry_spells'))}  (runs of ≥ 7 consecutive dry days)")
            # CSV row
            eto_summary = "; ".join(
                f"{pd.to_datetime(es['onset']).strftime('%Y-%m-%d')}"
                f"→{pd.to_datetime(es['cessation']).strftime('%Y-%m-%d') if es.get('cessation') else 'open'}"
                f" ({es['length_days']}d,{es['regime']})"
                for es in (eto_seasons or [])
            ) or ("n/a" if eto_seasons is not None else "")
            rows.append({
                'year':                year,
                'season_number':       i,
                'onset':               onset,
                'cessation':           cess,
                'regime':              s['regime'],
                'length_days':         s['length_days'],
                'total_rainfall_mm':   s.get('total_rainfall_mm'),
                'rainy_days':          s.get('rainy_days'),
                'dry_days':            s.get('dry_days'),
                'dry_spells':          s.get('dry_spells'),
                'annual_rain_mm':      ann_rain,
                'humid_result':        humid_str,
                'eto_seasons_summary': eto_summary if eto_summary else "",
                'params_used':         s.get('params_used', ''),
            })
        print(f"\n  {'─' * 48}")
        print(f"  Annual total rainfall : {_fmt(ann_rain, ' mm')}")
        print(f"  Humid test            : {humid_str if humid_str else 'n/a'}")

    if save_path and rows:
        pd.DataFrame(rows).to_csv(save_path, index=False)
        print(f"\n{'=' * 70}")
        print(f"✓ SAVED: {save_path}")

# CLI
def main() -> None:
    parser = argparse.ArgumentParser(
        description='Season analysis — ERA5 / AgERA5 / CHIRPS+CHIRTS',
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument('--location',     required=True,
                        help='Coordinates as "lat,lon"  e.g. "-1.286,36.817"')
    parser.add_argument('--source',
                        choices=['era_5', 'agera_5', 'chirps+chirts', 'auto'],
                        default='auto',
                        help=(
                            "Data source — used in both automatic and fixed-season modes.\n"
                            "  era_5         -- ERA5 reanalysis\n"
                            "  agera_5       -- AgERA5 / ERA5-Land\n"
                            "  chirps+chirts -- CHIRPS precipitation + CHIRTS temperature\n"
                            "  auto          -- tries era_5 -> agera_5 -> chirps+chirts  [default]"
                        ))
    parser.add_argument('--start-year',   type=int, required=True)
    parser.add_argument('--end-year',     type=int, required=True)
    parser.add_argument('--extra-months', type=int, default=6,
                        help='Extra months beyond Dec for late cessations (auto mode, default: 6)')
    parser.add_argument('--output-dir',   default='.',
                        help='Directory for CSV output (default: current dir)')
    parser.add_argument('--no-save',      action='store_true',
                        help='Skip saving the seasons CSV')
    parser.add_argument(
        '--fixed-season',
        default=None,
        metavar='MM-DD:MM-DD[,MM-DD:MM-DD]',
        help=(
            "Force fixed calendar season windows.\n"
            "Climate data is still fetched via --source for statistics\n"
            "and ETO-based onset/cessation analysis within each window.\n\n"
            "Format : one or two 'onset:cessation' tokens as MM-DD:MM-DD,\n"
            "         separated by a comma for two seasons.\n\n"
            "Examples:\n"
            "  Single season : --fixed-season '03-01:05-31'\n"
            "  Two seasons   : --fixed-season '03-01:05-31,10-01:12-15'\n"
            "  Year-crossing : --fixed-season '11-01:02-28'\n\n"
            "Year-crossing windows are handled automatically."
        ),
    )
    args = parser.parse_args()

    try:
        lat, lon = map(float, args.location.split(','))
    except ValueError:
        print("Error: --location must be in 'lat,lon' format.")
        sys.exit(1)
    if args.fixed_season:
        # Fixed-season path 
        print(
            f"Fixed-season mode | {lat:.4f}N, {lon:.4f}E | "
            f"{args.start_year}–{args.end_year} | source={args.source}"
        )
        try:
            fixed_defs = parse_fixed_seasons(args.fixed_season)
        except ValueError as exc:
            print(f"Error parsing --fixed-season: {exc}")
            sys.exit(1)
        print(f"\nParsed {len(fixed_defs)} fixed season window(s):")
        for fd in fixed_defs:
            (o_m, o_d), (c_m, c_d) = fd["onset_md"], fd["cessation_md"]
            cross = " (year-crossing)" if (c_m, c_d) < (o_m, o_d) else ""
            print(f"  {o_m:02d}-{o_d:02d} → {c_m:02d}-{c_d:02d}{cross}")

        seasons_dict, annual_dict = fetch_and_analyze_years_fixed(
            lat, lon,
            fixed_seasons=fixed_defs,
            start_year=args.start_year,
            end_year=args.end_year,
            source=args.source,
        )
    else:
        # Automatic detection path 
        print(f"Analyzing {lat:.4f}N, {lon:.4f}E | "
              f"{args.start_year}–{args.end_year} | source={args.source}")

        seasons_dict, annual_dict = fetch_and_analyze_years(
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
    print_summary(seasons_dict, annual_dict, save_path=save_path)
    print("\nAnalysis complete!")

if __name__ == '__main__':
    main()

# Automatic detection:
# python climate_tookit/season_analysis/seasons.py --location="-1.286,36.817" --start-year 2018 --end-year 2020 --source era_5
# python climate_tookit/season_analysis/seasons.py --location="-1.286,36.817" --start-year 2018 --end-year 2020 --source agera_5
# python climate_tookit/season_analysis/seasons.py --location="-1.286,36.817" --start-year 2015 --end-year 2016 --source chirps+chirts
# python climate_tookit/season_analysis/seasons.py --location="-1.286,36.817" --start-year 2015 --end-year 2020 --source agera_5 --output-dir ./results

# Fixed season:
# python climate_tookit/season_analysis/seasons.py --location="-1.286,36.817" --start-year 2015 --end-year 2022 --fixed-season "03-01:05-31" --source era_5

# Fixed two seasons:
# python climate_tookit/season_analysis/seasons.py --location="-1.286,36.817" --start-year 2015 --end-year 2022 --fixed-season "03-01:05-31,10-01:12-15" --source agera_5

# Fixed year-crossing season:
# python climate_tookit/season_analysis/seasons.py --location="-1.286,36.817" --start-year 2015 --end-year 2022 --fixed-season "11-01:02-28" --source chirps+chirts

# Fixed season, auto source selection, no CSV:
# python climate_tookit/season_analysis/seasons.py --location="-1.286,36.817" --start-year 2018 --end-year 2022 --fixed-season "04-15:07-10" --source auto --no-save