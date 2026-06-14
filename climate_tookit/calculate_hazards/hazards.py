"""
Calculate Hazards Module
Retrieves crop hazard indices at a specific location by:
1. Using season_analysis to detect growing seasons or accepting season dates
2. Calculating total precipitation and average temperature for the season
3. Evaluating crop-specific hazard thresholds
4. Analyzing dry spell patterns
5. Deriving soil-water hazards (NDWS, NDWL0) from a running soil water balance following the Adaptation Atlas method (ERATIO < 0.5 for NDWS;
   LOGGING > 0 for NDWL0)
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

# Soil water balance (Adaptation Atlas algorithm)
DEFAULT_SOILCP  = 100.0
DEFAULT_SOILSAT = 100.0

def calc_water_balance(
    df: pd.DataFrame,
    soilcp:  float = DEFAULT_SOILCP,
    soilsat: float = DEFAULT_SOILSAT,
    kc:      float = 1.0,
    init_avail: float = 0.0,
) -> pd.DataFrame:
    """
    Run the day-by-day soil water balance used by the Adaptation Atlas (CIAT ERA_dev / AdaptationAtlas/hazards). Returns the input frame with
    per-day columns: ERATIO (actual/potential ET ratio), LOGGING (water above field capacity, mm) and RUNOFF (water above saturation, mm).
    PET is taken from the Hargreaves ET0 column ('ET0_mm_day'); actual crop demand is ERATIO * kc * PET.
    """
    precip_col = next(
        (c for c in ['precipitation', 'precip', 'total_precipitation'] if c in df.columns),
        None,
    )
    out = df.sort_values('date').copy() if 'date' in df.columns else df.copy()
    if not precip_col or 'ET0_mm_day' not in out.columns:
        out['ERATIO']  = pd.NA
        out['LOGGING'] = pd.NA
        out['RUNOFF']  = pd.NA
        return out

    rain = out[precip_col].fillna(0).to_numpy()
    pet  = out['ET0_mm_day'].fillna(0).to_numpy()

    eratios, loggings, runoffs = [], [], []
    avail = float(init_avail)
    denom = 97.0 - 3.868 * (soilcp ** 0.5)
    for r, e in zip(rain, pet):
        avail = min(avail, soilcp)
        percwt = min(avail / soilcp * 100.0, 100.0) if soilcp > 0 else 1.0
        percwt = max(percwt, 1.0)
        eratio = min(percwt / denom, 1.0) if denom > 0 else 1.0
        demand = eratio * kc * float(e)

        result  = avail + float(r) - demand
        logging = min(max(result - soilcp, 0.0), soilsat)
        runoff  = max(result - logging - soilcp, 0.0)
        avail   = max(min(soilcp, result), 0.0)

        eratios.append(eratio)
        loggings.append(logging)
        runoffs.append(runoff)

    out['ERATIO']  = eratios
    out['LOGGING'] = loggings
    out['RUNOFF']  = runoffs
    return out

# Season statistics
def calculate_season_statistics(
    df:      pd.DataFrame,
    soilcp:  float = DEFAULT_SOILCP,
    soilsat: float = DEFAULT_SOILSAT,
) -> Dict[str, Any]:
    stats: Dict[str, Any] = {}

    precip_col = next(
        (c for c in ['precipitation', 'precip', 'total_precipitation'] if c in df.columns),
        None,
    )
    p = None
    if precip_col:
        p = df[precip_col].copy()
        stats['total_precipitation_mm']      = float(p.sum())
        stats['mean_daily_precipitation_mm'] = float(p.mean())
        stats['max_daily_precipitation_mm']  = float(p.max())
        stats['rainy_days']                  = int((p >= 1.0).sum())
        stats['dry_days']                    = int((p < 1.0).sum())
        # NDD: Number of Dry Days (precip < 1 mm) -- canonical hazard label
        stats['NDD']                         = int((p < 1.0).sum())
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
        # Canonical hazard labels: Max Tmax and Min Tmin
        stats['max_tmax_c']         = float(tmax.max())
        stats['min_tmin_c']         = float(tmin.min())
        # NTx35 / NTx40: number of days with Tmax above 35C / 40C (heat-stress days)
        stats['NTx35']              = int((tmax > 35).sum())
        stats['NTx40']              = int((tmax > 40).sum())

    # Soil-water hazard counts derived from a running soil water balance (Adaptation Atlas method), NOT a naive daily precip - ET0 comparison.
    #   NDWS  = days the crop cannot meet half its evaporative demand (ERATIO < 0.5)
    #   NDWL0 = days soil water exceeds field capacity (LOGGING > 0)
    if p is not None and 'ET0_mm_day' in df.columns:
        wb = calc_water_balance(df, soilcp=soilcp, soilsat=soilsat)
        eratio  = wb['ERATIO']
        logging = wb['LOGGING']
        stats['NDWS']  = int((eratio < 0.5).sum())
        stats['NDWL0'] = int((logging > 0).sum())
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

# Water-balance hazard severity classes (unit: days), per the Adaptation Atlas
NDWS_SEVERITY  = (15, 20, 25)
NDWL0_SEVERITY = (2, 5, 8)

def classify_water_hazard(value: float, bounds: Tuple[int, int, int]) -> str:
    """Map a day count to a severity class using the Atlas water-balance bands."""
    moderate_floor, severe_floor, extreme_floor = bounds
    if value < moderate_floor:
        return 'no_significant_stress'
    if value <= severe_floor:
        return 'moderate_stress'
    if value <= extreme_floor:
        return 'severe_stress'
    return 'extreme_stress'

def water_balance_hazards(stats: Dict[str, Any]) -> Dict[str, Any]:
    """Build NDWS / NDWL0 severity assessment entries from season statistics."""
    out: Dict[str, Any] = {}
    if stats.get('NDWS') is not None:
        v = stats['NDWS']
        out['water_stress'] = {
            'index':      'NDWS',
            'value_days': round(float(v), 2),
            'status':     classify_water_hazard(v, NDWS_SEVERITY),
        }
    if stats.get('NDWL0') is not None:
        v = stats['NDWL0']
        out['water_logging'] = {
            'index':      'NDWL0',
            'value_days': round(float(v), 2),
            'status':     classify_water_hazard(v, NDWL0_SEVERITY),
        }
    return out

def _severity_symbol(status: str) -> str:
    """Console marker for a hazard status (handles severe/extreme classes too)."""
    if 'no_stress' in status or 'no_significant' in status:
        return 'OK'
    if 'moderate' in status:
        return '!!'
    return 'XX'  # severe or extreme

# Long-Term Mean (Baseline) aggregation
_LTM_SCALAR_KEYS = (
    'total_precipitation_mm', 'mean_daily_precipitation_mm', 'max_daily_precipitation_mm',
    'rainy_days', 'dry_days', 'NDD',
    'mean_temperature_c', 'mean_tmax_c', 'mean_tmin_c',
    'max_temperature_c', 'min_temperature_c', 'max_tmax_c', 'min_tmin_c',
    'NTx35', 'NTx40', 'NDWS', 'NDWL0',
)

def _avg_dry_spell_stats(per_season: List[Dict[str, Any]]) -> Dict[str, Any]:
    counts, max_l, mean_l = [], [], []
    bucket_sums: Dict[str, float] = {}
    n_total = 0
    for stats in per_season:
        ds = stats.get('dry_spell_statistics')
        if not ds:
            continue
        n_total += 1
        counts.append(ds.get('number_of_dry_spells', 0))
        max_l.append(ds.get('max_dry_spell_length_days', 0))
        if ds.get('number_of_dry_spells', 0) > 0:
            mean_l.append(ds.get('mean_dry_spell_length_days', 0))
        for bucket, n in (ds.get('length_distribution') or {}).items():
            bucket_sums[bucket] = bucket_sums.get(bucket, 0) + n
    if not counts:
        return {}
    out = {
        'number_of_dry_spells':       round(sum(counts) / len(counts), 2),
        'max_dry_spell_length_days':  round(sum(max_l)  / len(max_l),  2) if max_l  else 0,
        'mean_dry_spell_length_days': round(sum(mean_l) / len(mean_l), 2) if mean_l else 0,
    }
    if bucket_sums:
        out['length_distribution'] = {
            b: round(total / n_total, 2) for b, total in bucket_sums.items()
        }
    return out

def compute_ltm_baseline(
    assessments: List[Dict[str, Any]],
    crop_name:   str,
    thresholds:  Dict[str, Any],
) -> Dict[str, Any]:
    """
    Long-Term Mean baseline across all evaluated seasons.
    When multiple seasons per year exist (fixed-season two-season mode), produces one LTM entry per season slot ('season_number') so the seasonal signal is
    preserved. Single-season inputs collapse to one overall LTM block.
    """
    # Group by season_number (defaults to 1 for explicit/single-season modes)
    grouped: Dict[int, List[Dict[str, Any]]] = {}
    for a in assessments:
        sn = a.get('season_info', {}).get('season_number', 1) or 1
        grouped.setdefault(sn, []).append(a)

    # Warn when auto-detected season counts differ across years — slot aggregation
    # becomes semantically unstable (season_number=1 in a 1-season year vs a 2-season
    # year refers to different climatological windows).
    yearly_totals = {
        a['season_info']['year']: a['season_info'].get('total_seasons_per_year', 1)
        for a in assessments if 'year' in a.get('season_info', {})
    }
    if len(set(yearly_totals.values())) > 1:
        import warnings as _warnings
        _warnings.warn(
            f"Auto-detected season counts differ across years: "
            f"{yearly_totals}. LTM aggregation by season_number may mix "
            f"incomparable seasonal windows. Use fixed-season mode for reliable "
            f"cross-year comparisons.",
            UserWarning, stacklevel=2,
        )

    ltm_blocks: List[Dict[str, Any]] = []
    for sn in sorted(grouped):
        bucket = grouped[sn]
        stats_list = [a.get('season_statistics', {}) for a in bucket]
        agg: Dict[str, Any] = {}
        for k in _LTM_SCALAR_KEYS:
            vals = [s[k] for s in stats_list if k in s and s[k] is not None]
            if vals:
                agg[k] = round(sum(vals) / len(vals), 2)
        ds_agg = _avg_dry_spell_stats(stats_list)
        if ds_agg:
            agg['dry_spell_statistics'] = ds_agg

        years   = sorted({a['season_info']['year'] for a in bucket if 'year' in a['season_info']})
        lengths = [a['season_info'].get('length_days') for a in bucket
                   if a['season_info'].get('length_days') is not None]
        total   = max((a['season_info'].get('total_seasons_per_year', 1) for a in bucket), default=1)

        hazard_eval: Dict[str, Any] = {}
        if 'Total Precip' in thresholds and 'total_precipitation_mm' in agg:
            pv = agg['total_precipitation_mm']
            hazard_eval['precipitation'] = {
                'value_mm': pv,
                'status':   evaluate_threshold(pv, thresholds['Total Precip']),
            }
        if 'TAVG' in thresholds and 'mean_temperature_c' in agg:
            tv = agg['mean_temperature_c']
            hazard_eval['temperature'] = {
                'value_c': tv,
                'status':  evaluate_threshold(tv, thresholds['TAVG']),
            }
        # NDWS / NDWL0 water-balance severity on the LTM means
        hazard_eval.update(water_balance_hazards(agg))

        ltm_blocks.append({
            'season_number':          sn,
            'total_seasons_per_year': total,
            'n_seasons_averaged':     len(bucket),
            'years_covered':          years,
            'mean_length_days':       round(sum(lengths) / len(lengths), 1) if lengths else None,
            'season_statistics':      agg,
            'hazard_evaluation':      hazard_eval,
        })

    return {
        'crop':            crop_name,
        'n_total_seasons': len(assessments),
        'baseline_method': 'long_term_mean',
        'per_season':      ltm_blocks,
    }
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
    soilcp:            float          = DEFAULT_SOILCP,
    soilsat:           float          = DEFAULT_SOILSAT,
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
        print(f"Climate data source: {source}")
        df = get_climate_data_for_season(lat, lon, season_start, season_end)
        season_info = {
            'season_detected': True,
            'onset_date':      season_start,
            'cessation_date':  season_end,
            'length_days':     (datetime.fromisoformat(season_end) - datetime.fromisoformat(season_start)).days,
            'method':          'user_provided',
            'source':          source,          # record dataset used
        }
        all_results = [{'season_info': season_info, 'df': df}]

    # fixed-season (mirrors seasons.py fixed-season mode)
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
        num_seasons_per_year = len(fixed_defs) 
        all_results = []
        for year, seasons in sorted(seasons_dict.items()):
            for season_idx, s in enumerate(seasons):   
                s_start = pd.to_datetime(s['onset']).strftime('%Y-%m-%d')
                s_end   = (
                    pd.to_datetime(s['cessation']).strftime('%Y-%m-%d')
                    if s.get('cessation') else date_to
                )
                season_info = {
                    'season_detected':        True,
                    'onset_date':             s_start,
                    'cessation_date':         s_end,
                    'length_days':            s['length_days'],
                    'method':                 'fixed_season',
                    'year':                   year,
                    'season_number':          season_idx + 1,           
                    'total_seasons_per_year': num_seasons_per_year,     
                    'source':                 source,                   
                }
                df = get_climate_data_for_season(lat, lon, s_start, s_end)
                all_results.append({'season_info': season_info, 'df': df})
        if not all_results:
            return {'error': 'No seasons produced by fixed-season mode for the given date range.'}

    # auto-detect via fetch_and_analyze_years, always use chirps+chirts for auto-detection
    elif SEASON_ANALYSIS_AVAILABLE:
        auto_source = 'chirps+chirts'
        print(f"Detecting growing season for {crop_name} at ({lat}, {lon})")
        print(f"Climate data source: {auto_source} (fixed for auto-detection accuracy)")
        start_year = datetime.fromisoformat(date_from).year
        end_year   = datetime.fromisoformat(date_to).year
        seasons_dict, _ = fetch_and_analyze_years(
            lat, lon, start_year=start_year, end_year=end_year, source=auto_source
        )
        if not any(seasons_dict.values()):
            return {
                'error': (
                    'No growing season detected. '
                    'Provide --season-start/--season-end or --fixed-season.'
                )
            }
        all_results = []
        for year, seasons in sorted(seasons_dict.items()):
            num_seasons_per_year = len(seasons)
            for season_idx, s in enumerate(seasons):
                s_start = pd.to_datetime(s['onset']).strftime('%Y-%m-%d')
                s_end   = (
                    pd.to_datetime(s['cessation']).strftime('%Y-%m-%d')
                    if s.get('cessation') else date_to
                )
                season_info = {
                    'season_detected':        True,
                    'onset_date':             s_start,
                    'cessation_date':         s_end,
                    'length_days':            s['length_days'],
                    'method':                 'rainfall_based',
                    'year':                   year,
                    'season_number':          season_idx + 1,
                    'total_seasons_per_year': num_seasons_per_year,
                    'source':                 auto_source,
                }
                df = get_climate_data_for_season(lat, lon, s_start, s_end)
                all_results.append({'season_info': season_info, 'df': df})
    else:
        return {
            'error': (
                f'Season analysis not available and no season dates provided -- {_IMPORT_ERROR}'
            )
        }
    # Evaluate hazards for every resolved season
    assessments = []
    for entry in all_results:
        stats      = calculate_season_statistics(entry['df'], soilcp=soilcp, soilsat=soilsat)
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
        # NDWS / NDWL0 water-balance severity (Adaptation Atlas classes)
        hazard_eval.update(water_balance_hazards(stats))
        assessments.append({
            'crop':              crop_name,
            'location':          {'latitude': lat, 'longitude': lon},
            'season_info':       entry['season_info'],
            'season_statistics': stats,
            'hazard_evaluation': hazard_eval,
        })
    # Single season -> flat dict; multiple -> wrapped list with Baseline LTM
    if len(assessments) == 1:
        return assessments[0]
    return {
        'assessments':  assessments,
        'baseline_ltm': compute_ltm_baseline(assessments, crop_name, thresholds),
    }

# Pretty printer
def _fmt_date(d) -> str:
    if isinstance(d, (date, datetime)):
        return d.strftime('%Y-%m-%d')
    return str(d)[:10]

def _print_ltm_block(ltm: Dict[str, Any]) -> None:
    """Pretty-print the Baseline LTM (Long-Term Mean) summary."""
    print(f"\n{'='*70}")
    print(f"  BASELINE LTM (LONG-TERM MEAN): {ltm['crop'].upper()}")
    print(f"  Averaged across {ltm['n_total_seasons']} season(s)  -- method: {ltm['baseline_method']}")
    print(f"{'='*70}")

    for blk in ltm['per_season']:
        sn, total = blk['season_number'], blk['total_seasons_per_year']
        label = f"Season {sn} of {total}" if total and total > 1 else "Overall"
        years = blk.get('years_covered') or []
        year_range = f"{years[0]}-{years[-1]}" if len(years) >= 2 else (str(years[0]) if years else "n/a")

        print(f"\n  {label}  ({blk['n_seasons_averaged']} seasons, years {year_range})")
        print(f"  {'─'*66}")
        if blk.get('mean_length_days') is not None:
            print(f"  Mean season length: {blk['mean_length_days']} days")

        s = blk['season_statistics']
        if 'total_precipitation_mm' in s:
            print(f"\n  Precipitation (LTM means)")
            print(f"  {'─'*66}")
            print(f"  {'Total':<32} {s['total_precipitation_mm']:>15.2f}  mm")
            print(f"  {'Daily Mean':<32} {s.get('mean_daily_precipitation_mm', 0):>15.2f}  mm")
            print(f"  {'Daily Maximum':<32} {s.get('max_daily_precipitation_mm', 0):>15.2f}  mm")
            print(f"  {'Rainy Days (>=1mm)':<32} {s.get('rainy_days', 0):>15.2f}  days")
            print(f"  {'NDD (Dry Days)':<32} {s.get('NDD', s.get('dry_days', 0)):>15.2f}  days")

        if 'dry_spell_statistics' in s:
            ds = s['dry_spell_statistics']
            print(f"\n  Dry Spell Statistics (LTM means; >=7 consecutive days <1mm)")
            print(f"  {'─'*66}")
            print(f"  {'Number of Dry Spells':<32} {ds.get('number_of_dry_spells', 0):>15.2f}  spells")
            print(f"  {'Max Dry Spell Length':<32} {ds.get('max_dry_spell_length_days', 0):>15.2f}  days")
            print(f"  {'Mean Dry Spell Length':<32} {ds.get('mean_dry_spell_length_days', 0):>15.2f}  days")

        if 'mean_temperature_c' in s:
            print(f"\n  Temperature (LTM means)")
            print(f"  {'─'*66}")
            print(f"  {'Mean Temperature':<32} {s['mean_temperature_c']:>15.2f}  deg C")
            print(f"  {'Mean Tmax':<32} {s.get('mean_tmax_c', 0):>15.2f}  deg C")
            print(f"  {'Mean Tmin':<32} {s.get('mean_tmin_c', 0):>15.2f}  deg C")
            print(f"  {'Max Tmax':<32} {s.get('max_tmax_c', s.get('max_temperature_c', 0)):>15.2f}  deg C")
            print(f"  {'Min Tmin':<32} {s.get('min_tmin_c', s.get('min_temperature_c', 0)):>15.2f}  deg C")

        # New hazard counts
        has_counts = any(k in s for k in ('NTx35', 'NTx40', 'NDWS', 'NDWL0'))
        if has_counts:
            print(f"\n  Hazard Indices (LTM means)")
            print(f"  {'─'*66}")
            if 'NTx35' in s:
                print(f"  {'NTx35 (days Tmax > 35C)':<32} {s['NTx35']:>15.2f}  days")
            if 'NTx40' in s:
                print(f"  {'NTx40 (days Tmax > 40C)':<32} {s['NTx40']:>15.2f}  days")
            if 'NDWS' in s:
                print(f"  {'NDWS (water-stress days)':<32} {s['NDWS']:>15.2f}  days")
            if 'NDWL0' in s:
                print(f"  {'NDWL0 (water-logging days)':<32} {s['NDWL0']:>15.2f}  days")

        h = blk.get('hazard_evaluation', {})
        if h:
            print(f"\n  Hazard Assessment (vs crop thresholds, LTM-based)")
            print(f"  {'─'*66}")
            if 'precipitation' in h:
                pp  = h['precipitation']
                sym = 'OK' if 'no_stress' in pp['status'] else '!!' if 'moderate' in pp['status'] else 'XX'
                print(f"  {'Precipitation':<25} {pp['value_mm']:>16.2f} mm  [{sym}] {pp['status'].replace('_', ' ').upper()}")
            if 'temperature' in h:
                tt  = h['temperature']
                sym = 'OK' if 'no_stress' in tt['status'] else '!!' if 'moderate' in tt['status'] else 'XX'
                print(f"  {'Temperature':<25} {tt['value_c']:>16.2f} degC [{sym}] {tt['status'].replace('_', ' ').upper()}")
            if 'water_stress' in h:
                ws  = h['water_stress']
                sym = _severity_symbol(ws['status'])
                print(f"  {'Water Stress (NDWS)':<25} {ws['value_days']:>16.2f} d   [{sym}] {ws['status'].replace('_', ' ').upper()}")
            if 'water_logging' in h:
                wl  = h['water_logging']
                sym = _severity_symbol(wl['status'])
                print(f"  {'Water Logging (NDWL0)':<25} {wl['value_days']:>16.2f} d   [{sym}] {wl['status'].replace('_', ' ').upper()}")
    print(f"\n{'='*70}\n")

def print_hazard_results(result: Dict[str, Any]) -> None:
    # Multi-season wrapper, label each block as "Year YYYY – Season X of Y" when available
    if 'assessments' in result:
        total = len(result['assessments'])
        for i, a in enumerate(result['assessments'], 1):
            print(f"\n{'─'*70}")
            season = a.get('season_info', {})
            year   = season.get('year', '')
            snum   = season.get('season_number', i)
            spyr   = season.get('total_seasons_per_year', '')
            if year and spyr and spyr > 1:
                label = f"Year {year}  –  Season {snum} of {spyr}"
            elif year:
                label = f"Year {year}"
            else:
                label = f"Assessment {i} of {total}"
            print(f"  {label}")
            print_hazard_results(a)
        if result.get('baseline_ltm'):
            _print_ltm_block(result['baseline_ltm'])
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
    # always display the dataset that was used
    if season.get('source'):
        print(f"  Source: {season['source']}")

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
        print(f"  {'Max Tmax (Maximum Recorded)':<32} {stats['max_temperature_c']:>15.2f}  deg C")
        print(f"  {'Min Tmin (Minimum Recorded)':<32} {stats['min_temperature_c']:>15.2f}  deg C")

    # Hazard index counts (NTx35, NTx40, NDD, NDWS, NDWL0)
    has_counts = any(k in stats for k in ('NTx35', 'NTx40', 'NDWS', 'NDWL0'))
    if has_counts:
        print(f"\n  Hazard Index Counts")
        print(f"  {'─'*66}")
        print(f"  {'Index':<32} {'Value':>15}  Unit")
        print(f"  {'─'*32} {'─'*15}  {'─'*10}")
        if 'NTx35' in stats:
            print(f"  {'NTx35 (days Tmax > 35C)':<32} {stats['NTx35']:>15}  days")
        if 'NTx40' in stats:
            print(f"  {'NTx40 (days Tmax > 40C)':<32} {stats['NTx40']:>15}  days")
        if 'NDD' in stats:
            print(f"  {'NDD (dry days, <1mm)':<32} {stats['NDD']:>15}  days")
        if 'NDWS' in stats:
            print(f"  {'NDWS (water-stress days)':<32} {stats['NDWS']:>15}  days")
        if 'NDWL0' in stats:
            print(f"  {'NDWL0 (water-logging days)':<32} {stats['NDWL0']:>15}  days")

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
    if 'water_stress' in hazards:
        ws  = hazards['water_stress']
        sym = _severity_symbol(ws['status'])
        print(f"  {'Water Stress (NDWS)':<25} {ws['value_days']:>16.2f} d   [{sym}] {ws['status'].replace('_', ' ').upper()}")
    if 'water_logging' in hazards:
        wl  = hazards['water_logging']
        sym = _severity_symbol(wl['status'])
        print(f"  {'Water Logging (NDWL0)':<25} {wl['value_days']:>16.2f} d   [{sym}] {wl['status'].replace('_', ' ').upper()}")

    print(f"\n{'='*70}\n")

# CLI
if __name__ == "__main__":
    # Ensure Unicode output (box-drawing chars, degree sign, etc.)
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except (AttributeError, ValueError):
        pass

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
            "  auto          -- tries era_5 -> agera_5 -> chirps+chirts\n"
            "Note: auto-detection (no --season-* flag) always uses chirps+chirts\n"
            "      regardless of this setting, as thresholds are calibrated for it."
        ),
    )
    parser.add_argument('--gap-days',        type=int, default=30,
                        help='Dry-day gap used to end auto-detected season (default: 30)')
    parser.add_argument('--min-season-days', type=int, default=30,
                        help='Minimum season length for auto-detection (default: 30)')
    parser.add_argument('--soil-source', choices=['constant', 'auto'], default='auto',
                        help=("Soil water capacity source for NDWS/NDWL0 (default: auto).\n"
                              "  auto     -- derive per-location soilcp/soilsat from\n"
                              "              SoilGrids via the Adaptation Atlas pedotransfer\n"
                              "              (needs GEE credentials; falls back to constants)\n"
                              "  constant -- use the fixed --soilcp/--soilsat values"))
    parser.add_argument('--soilcp',  type=float, default=DEFAULT_SOILCP,
                        help=f'Soil available water capacity at field capacity, mm '
                             f'(water-balance NDWS/NDWL0; default: {DEFAULT_SOILCP})')
    parser.add_argument('--soilsat', type=float, default=DEFAULT_SOILSAT,
                        help=f'Extra soil water from field capacity to saturation, mm '
                             f'(water-balance NDWL0; default: {DEFAULT_SOILSAT})')
    parser.add_argument('--format',          choices=['json', 'text'], default='text',
                        help='Output format (default: text)')
    parser.add_argument('--output',          type=str, default=None,
                        help='Save JSON result to this file path')
    args = parser.parse_args()

    # Validate explicit-season pair
    if bool(args.season_start) != bool(args.season_end):
        parser.error('--season-start and --season-end must be supplied together.')

    lat, lon = map(float, args.location.split(','))

    # Resolve soil water capacities. 'auto' derives them per location from SoilGrids; on any failure it returns the constants, so the run never breaks for users without GEE credentials.
    soilcp, soilsat = args.soilcp, args.soilsat
    if args.soil_source == 'auto':
        try:
            from soil_capacity import fetch_soil_capacity
        except ImportError:
            from climate_tookit.calculate_hazards.soil_capacity import fetch_soil_capacity
        print("Deriving per-location soil capacity from SoilGrids...")
        soilcp, soilsat = fetch_soil_capacity(lat, lon)

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
        soilcp=soilcp,
        soilsat=soilsat,
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

# Auto-detect season (no season flag supplied -- always uses chirps+chirts internally):
# python -m climate_tookit.calculate_hazards.hazards maize --location="-1.286,36.817" --date-from 2016-01-01 --date-to 2016-12-31 --season-start 2016-03-01 --season-end 2016-06-30

# Fixed single season:
# python -m climate_tookit.calculate_hazards.hazards maize --location="-1.286,36.817" --date-from 2018-01-01 --date-to 2022-12-31 --fixed-season "03-01:06-30" --source era_5

# Fixed two seasons:
# python -m climate_tookit.calculate_hazards.hazards beans --location="-1.286,36.817" --date-from 2018-01-01 --date-to 2022-12-31 --fixed-season "03-01:05-31,10-01:12-15" --source agera_5

# Fixed year-crossing season:
# python -m climate_tookit.calculate_hazards.hazards sorghum --location="-1.286,36.817" --date-from 2012-01-01 --date-to 2016-12-31 --fixed-season "11-01:02-28" --source chirps+chirts

# Explicit season dates (single season):
# python -m climate_tookit.calculate_hazards.hazards maize --location="-1.286,36.817" --date-from 2020-01-01 --date-to 2020-12-31 --season-start 2020-03-01 --season-end 2020-06-30

# JSON output to file:
# python -m climate_tookit.calculate_hazards.hazards rice --location="-1.286,36.817" --date-from 2019-01-01 --date-to 2021-12-31 --fixed-season "04-15:07-10" --source auto --format json --output results.json