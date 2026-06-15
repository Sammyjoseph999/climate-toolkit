"""
Ensemble of hazards.py across NEX-GDDP models x scenarios.
For each (model, scenario) combination, runs the same hazard assessment that hazards.py produces, but with NEX-GDDP daily data. 
Results are bucketed by (year, season_number) and averaged. No baseline. No CHIRPS. No CHIRTS.
Modes:
  - default        : auto-detect seasons per (model, scenario) using NEX-GDDP.
  - --fixed-season : single, two, or year-crossing windows applied to each year.
"""

import os
import sys
import json
import argparse
from collections import defaultdict
from datetime import datetime, date
from statistics import mean
from typing import Dict, List, Tuple, Optional

import pandas as pd

HERE   = os.path.dirname(os.path.abspath(__file__))
PARENT = os.path.dirname(HERE)
ROOT   = os.path.dirname(PARENT)
for p in (HERE, ROOT):
    if p not in sys.path:
        sys.path.insert(0, p)

from hazards import (
    CROP_THRESHOLDS,
    evaluate_threshold,
    calculate_season_statistics,
    add_et0,
    water_balance_hazards,
    _severity_symbol,
    DEFAULT_SOILCP,
    DEFAULT_SOILSAT,
)

sys.path.insert(0, os.path.join(PARENT, 'fetch_data', 'preprocess_data'))
sys.path.insert(0, os.path.join(PARENT, 'fetch_data', 'source_data', 'sources'))
from preprocess_data import preprocess_data
from utils.models      import ClimateVariable

# Optional: only used by auto-detect
try:
    from climate_tookit.season_analysis.seasons import fetch_and_analyze_years
    HAS_FAY = True
except Exception as _e:
    HAS_FAY = False
    _FAY_ERR = str(_e)

SCENARIOS = ['ssp126', 'ssp245', 'ssp585']
MODELS = [
    'ACCESS-CM2', 'ACCESS-ESM1-5', 'CanESM5', 'CMCC-ESM2',
    'EC-Earth3', 'EC-Earth3-Veg-LR', 'GFDL-ESM4', 'INM-CM4-8',
    'INM-CM5-0', 'KACE-1-0-G', 'MIROC6', 'MPI-ESM1-2-LR',
    'MRI-ESM2-0', 'NorESM2-LM', 'NorESM2-MM', 'TaiESM1',
]

_VARS   = [ClimateVariable.precipitation,
           ClimateVariable.max_temperature,
           ClimateVariable.min_temperature]
_COLMAP = {'pr': 'precipitation', 'prcp': 'precipitation',
           'tasmax': 'max_temperature', 'tasmin': 'min_temperature'}

# Fixed-season parsing & expansion
def _parse_fixed(spec: str) -> List[Tuple[str, str]]:
    """'MM-DD:MM-DD[,MM-DD:MM-DD]' -> [(onset, cessation), ...]"""
    out = []
    for token in spec.split(','):
        token = token.strip()
        if not token:
            continue
        if ':' not in token:
            raise ValueError(f"fixed-season token missing ':' -- {token!r}")
        onset, cessation = (s.strip() for s in token.split(':', 1))
        datetime.strptime(onset,     '%m-%d')   # validate
        datetime.strptime(cessation, '%m-%d')
        out.append((onset, cessation))
    if not out:
        raise ValueError("empty fixed-season specification")
    return out

def _yearcross(o: str, c: str) -> bool:
    return (datetime.strptime(c, '%m-%d').replace(year=2001)
          < datetime.strptime(o, '%m-%d').replace(year=2001))

def _isleap(y: int) -> bool:
    return y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)

def _iso(year: int, mmdd: str) -> str:
    m, d = (int(x) for x in mmdd.split('-'))
    if m == 2 and d == 29 and not _isleap(year):
        d = 28
    return f"{year:04d}-{m:02d}-{d:02d}"

def _expand_windows(sy: int, ey: int,
                    defs: List[Tuple[str, str]]) -> List[Dict]:
    out = []
    for y in range(sy, ey + 1):
        for i, (o, c) in enumerate(defs, 1):
            out.append({
                'start':         _iso(y, o),
                'end':           _iso(y + 1 if _yearcross(o, c) else y, c),
                'season_number': i,
                'year':          y,
                'total':         len(defs),
            })
    return out

# NEX-GDDP fetching & per-window assessment
def _fetch(lat: float, lon: float, start: str, end: str,
           model: str, scenario: str) -> pd.DataFrame:
    df = preprocess_data(
        source='nex_gddp',
        location_coord=(lat, lon),
        variables=_VARS,
        date_from=date.fromisoformat(start),
        date_to=date.fromisoformat(end),
        model=model, scenario=scenario,
    )
    if df is None or df.empty:
        raise RuntimeError(f"no data for {model}/{scenario} {start}->{end}")
    rename = {c: _COLMAP[c] for c in df.columns if c in _COLMAP}
    if rename:
        df = df.rename(columns=rename)

    # Attach Hargreaves ET0 so calculate_season_statistics can derive NDWS / NDWL0.
    if {'min_temperature', 'max_temperature', 'date'}.issubset(df.columns):
        view = df.rename(columns={'min_temperature': 'tmin',
                                  'max_temperature': 'tmax'})
        df['ET0_mm_day'] = add_et0(view, lat)['ET0_mm_day'].values
    return df

def _detect_windows(lat: float, lon: float, sy: int, ey: int,
                    model: str, scenario: str) -> List[Dict]:
    """Auto-detect via fetch_and_analyze_years with NEX-GDDP source."""
    if not HAS_FAY:
        raise RuntimeError(f"auto-detect needs seasons.py -- {_FAY_ERR}")
    try:
        seasons_dict, _ = fetch_and_analyze_years(
            lat, lon, start_year=sy, end_year=ey,
            source='nex_gddp', model=model, scenario=scenario,
        )
    except TypeError as e:
        raise RuntimeError(
            "fetch_and_analyze_years did not accept NEX-GDDP arguments. "
            "Either add a NEX-GDDP branch to seasons.py or use --fixed-season. "
            f"({e})"
        )
    out = []
    for y, seasons in sorted(seasons_dict.items()):
        for i, s in enumerate(seasons, 1):
            if not s.get('cessation'):
                continue
            out.append({
                'start':         pd.to_datetime(s['onset']).strftime('%Y-%m-%d'),
                'end':           pd.to_datetime(s['cessation']).strftime('%Y-%m-%d'),
                'season_number': i,
                'year':          y,
                'total':         len(seasons),
            })
    return out

def _evaluate(crop: str, lat: float, lon: float,
              w: Dict, model: str, scenario: str,
              soilcp: float = DEFAULT_SOILCP,
              soilsat: float = DEFAULT_SOILSAT) -> Dict:
    """hazards.py-style assessment for a single window using NEX-GDDP."""
    df    = _fetch(lat, lon, w['start'], w['end'], model, scenario)
    stats = calculate_season_statistics(df, soilcp=soilcp, soilsat=soilsat)
    th    = CROP_THRESHOLDS.get(crop.capitalize(), {})

    hazards = {}
    if 'Total Precip' in th and 'total_precipitation_mm' in stats:
        v = stats['total_precipitation_mm']
        hazards['precipitation'] = {'value_mm': round(v, 2),
                                    'status':   evaluate_threshold(v, th['Total Precip'])}
    if 'TAVG' in th and 'mean_temperature_c' in stats:
        v = stats['mean_temperature_c']
        hazards['temperature'] = {'value_c': round(v, 2),
                                  'status':  evaluate_threshold(v, th['TAVG'])}
    # NDWS / NDWL0 water-balance severity (Adaptation Atlas classes)
    hazards.update(water_balance_hazards(stats))
    length = (datetime.fromisoformat(w['end'])
              - datetime.fromisoformat(w['start'])).days
    return {
        'season_info': {**w, 'length_days': length},
        'season_statistics': stats,
        'hazard_evaluation': hazards,
        'projection': {'model': model, 'scenario': scenario},
    }

# Aggregation -- ensemble means
_SCALAR_KEYS = ['total_precipitation_mm', 'mean_daily_precipitation_mm',
                'max_daily_precipitation_mm', 'rainy_days', 'dry_days',
                'mean_temperature_c', 'mean_tmax_c', 'mean_tmin_c',
                'max_temperature_c', 'min_temperature_c',
                # Canonical hazard variables added in hazards.py
                'max_tmax_c', 'min_tmin_c',
                'NDD', 'NTx35', 'NTx40', 'NDWS', 'NDWL0']

def _avg_stats(rs: List[Dict]) -> Dict:
    out = {}
    if not rs:
        return out
    for k in _SCALAR_KEYS:
        vs = [r['season_statistics'][k]
              for r in rs if k in r.get('season_statistics', {})]
        if vs:
            out[k] = round(mean(vs), 2)

    counts, max_l, mean_l = [], [], []
    bucket_sums: Dict[str, float] = defaultdict(float)
    for r in rs:
        ds = r.get('season_statistics', {}).get('dry_spell_statistics')
        if not ds:
            continue
        counts.append(ds['number_of_dry_spells'])
        max_l.append(ds['max_dry_spell_length_days'])
        if ds['number_of_dry_spells'] > 0:
            mean_l.append(ds['mean_dry_spell_length_days'])
        for bucket, n in (ds.get('length_distribution') or {}).items():
            bucket_sums[bucket] += n

    if counts:
        ds_out = {
            'number_of_dry_spells':       round(mean(counts), 2),
            'max_dry_spell_length_days':  round(mean(max_l), 2)  if max_l  else 0,
            'mean_dry_spell_length_days': round(mean(mean_l), 2) if mean_l else 0,
        }
        if bucket_sums:
            n_total = len(rs)
            ds_out['length_distribution'] = {
                b: round(total / n_total, 2) for b, total in bucket_sums.items()
            }
        out['dry_spell_statistics'] = ds_out
    return out

def _avg_hazards(crop: str, agg: Dict) -> Dict:
    th  = CROP_THRESHOLDS.get(crop.capitalize(), {})
    out = {}
    if 'Total Precip' in th and 'total_precipitation_mm' in agg:
        v = agg['total_precipitation_mm']
        out['precipitation'] = {'value_mm': v,
                                'status': evaluate_threshold(v, th['Total Precip'])}
    if 'TAVG' in th and 'mean_temperature_c' in agg:
        v = agg['mean_temperature_c']
        out['temperature'] = {'value_c': v,
                              'status': evaluate_threshold(v, th['TAVG'])}
    # NDWS / NDWL0 severity on the ensemble-mean day counts
    out.update(water_balance_hazards(agg))
    return out

def _agg_hazard_statuses(bucket: List[Dict]) -> Dict:
    """
    Aggregate pre-computed per-projection hazard statuses by majority vote.
    Returns the modal status for each hazard indicator plus a counts breakdown.
    """
    from collections import Counter
    indicators = ['precipitation', 'temperature', 'water_stress', 'water_logging']
    out = {}
    for ind in indicators:
        statuses = [
            r['hazard_evaluation'][ind]['status']
            for r in bucket
            if ind in r.get('hazard_evaluation', {})
        ]
        if not statuses:
            continue
        counts = Counter(statuses)
        majority = counts.most_common(1)[0][0]
        out[ind] = {'status': majority, 'status_counts': dict(counts)}
    return out

# Driver
def calculate_ensemble(crop: str, lat: float, lon: float,
                       start_year: int, end_year: int,
                       models: List[str], scenarios: List[str],
                       fixed_season: Optional[str] = None,
                       soilcp: float = DEFAULT_SOILCP,
                       soilsat: float = DEFAULT_SOILSAT) -> Dict:
    mode = 'fixed_season' if fixed_season else 'auto_detect'
    fixed_w = (_expand_windows(start_year, end_year, _parse_fixed(fixed_season))
               if fixed_season else None)

    print(f"\nNEX-GDDP ensemble: {crop} at ({lat:.4f}, {lon:.4f})  "
          f"{start_year}-{end_year}")
    print(f"  Mode: {mode}" + (f"  ({fixed_season})" if fixed_season else ""))
    print(f"  Models: {len(models)}   Scenarios: {len(scenarios)}\n")

    results: List[Dict] = []
    for sc in scenarios:
        for i, m in enumerate(models, 1):
            print(f"  [{sc}] [{i}/{len(models)}] {m}")
            try:
                windows = (fixed_w if fixed_w is not None
                           else _detect_windows(lat, lon, start_year, end_year, m, sc))
            except Exception as e:
                print(f"      ! detection failed: {e}")
                continue
            for w in windows:
                tag = f"y{w['year']} s{w['season_number']}/{w['total']} {w['start']}->{w['end']}"
                try:
                    results.append(_evaluate(crop, lat, lon, w, m, sc,
                                             soilcp=soilcp, soilsat=soilsat))
                    print(f"      {tag}  ✓")
                except Exception as e:
                    print(f"      {tag}  ✗ {e}")
    if not results:
        return {'error': 'No projections succeeded.'}

    # Bucket by (scenario, year, season_number) to keep scenarios separate
    buckets: Dict[Tuple[str, int, int], List[Dict]] = defaultdict(list)
    for r in results:
        si = r['season_info']
        buckets[(r['projection']['scenario'], si['year'], si['season_number'])].append(r)

    assessments = []
    for (sc, y, sn), bucket in sorted(buckets.items()):
        agg     = _avg_stats(bucket)
        onsets  = sorted(pd.to_datetime(r['season_info']['start']) for r in bucket)
        ends    = sorted(pd.to_datetime(r['season_info']['end'])   for r in bucket)
        lengths = [r['season_info']['length_days'] for r in bucket]
        total   = max(r['season_info']['total'] for r in bucket)
        projections = []
        for r in bucket:
            st = r.get('season_statistics', {})
            projections.append({
                'model':                  r['projection']['model'],
                'scenario':               r['projection']['scenario'],
                'total_precipitation_mm': st.get('total_precipitation_mm'),
                'rainy_days':             st.get('rainy_days'),
                'mean_temperature_c':     st.get('mean_temperature_c'),
                'mean_tmax_c':            st.get('mean_tmax_c'),
                'mean_tmin_c':            st.get('mean_tmin_c'),
                'max_tmax_c':             st.get('max_tmax_c'),
                'min_tmin_c':             st.get('min_tmin_c'),
                'NDD':                    st.get('NDD'),
                'NTx35':                  st.get('NTx35'),
                'NTx40':                  st.get('NTx40'),
                'NDWS':                   st.get('NDWS'),
                'NDWL0':                  st.get('NDWL0'),
            })
        assessments.append({
            'scenario':               sc,
            'year':                   y,
            'season_number':          sn,
            'total_seasons_per_year': total,
            'n_projections':          len(bucket),
            'window': {
                'start_median':     onsets[len(onsets) // 2].strftime('%Y-%m-%d'),
                'end_median':       ends[len(ends)     // 2].strftime('%Y-%m-%d'),
                'length_days_mean': round(mean(lengths), 1),
            },
            'projections':       projections,
            'season_statistics': agg,
            'hazard_evaluation': _agg_hazard_statuses(bucket),
        })

    overall = _avg_stats(results)
    return {
        'crop':              crop,
        'location':          {'latitude': lat, 'longitude': lon},
        'data_source':       'nex_gddp',
        'period':            {'start_year': start_year, 'end_year': end_year},
        'season_mode':       mode,
        'season_definition': fixed_season,
        'soil_water_balance': {'soilcp': soilcp, 'soilsat': soilsat},
        'models':            models,
        'scenarios':         scenarios,
        'n_total_projections': len(results),
        'assessments':       assessments,
        'overall_ensemble': {
            'n_projections':     len(results),
            'season_statistics': overall,
            'hazard_evaluation': _agg_hazard_statuses(results),
        },
    }

# Pretty printer (mirrors hazards.py year/season blocks)
def _sym(status: str) -> str:
    return 'OK' if 'no_stress' in status else '!!' if 'moderate' in status else 'XX'

def _bucket_key(b: str) -> int:
    try:
        return int(b.split('-')[0])
    except (ValueError, IndexError):
        return 999

def _fmt(v, nd=2):
    """Format a numeric value, or 'n/a' for None — used in per-projection tables."""
    return f"{v:.{nd}f}" if isinstance(v, (int, float)) else "n/a"

def _print_projection_breakdown(a: Dict) -> None:
    """
    Show each contributing (model, scenario) projection before the ensemble means,
    so the reader can see what feeds the averages for this year/season.
    """
    projections = a.get('projections') or []
    if not projections:
        return
    print(f"\n  Per-Projection Breakdown  ({len(projections)} projection(s) → ensemble mean)")
    print(f"  {'─'*66}")
    rows = []
    for p in projections:
        rows.append({
            'Model':    p.get('model'),
            'Scenario': p.get('scenario'),
            'Precip_mm': _fmt(p.get('total_precipitation_mm')),
            'Rainy_d':   _fmt(p.get('rainy_days')),
            'Tmean_c':   _fmt(p.get('mean_temperature_c')),
            'Tmax_c':    _fmt(p.get('mean_tmax_c')),
            'Tmin_c':    _fmt(p.get('mean_tmin_c')),
            'NTx35':     _fmt(p.get('NTx35'), 0),
            'NTx40':     _fmt(p.get('NTx40'), 0),
            'NDD':       _fmt(p.get('NDD'), 0),
            'NDWS':      _fmt(p.get('NDWS'), 0),
            'NDWL0':     _fmt(p.get('NDWL0'), 0),
        })
    s = a['season_statistics']
    rows.append({
        'Model':     f"ENSEMBLE (mean of {len(projections)})",
        'Scenario':  '',
        'Precip_mm': _fmt(s.get('total_precipitation_mm')),
        'Rainy_d':   _fmt(s.get('rainy_days')),
        'Tmean_c':   _fmt(s.get('mean_temperature_c')),
        'Tmax_c':    _fmt(s.get('mean_tmax_c')),
        'Tmin_c':    _fmt(s.get('mean_tmin_c')),
        'NTx35':     _fmt(s.get('NTx35')),
        'NTx40':     _fmt(s.get('NTx40')),
        'NDD':       _fmt(s.get('NDD')),
        'NDWS':      _fmt(s.get('NDWS')),
        'NDWL0':     _fmt(s.get('NDWL0')),
    })
    for line in pd.DataFrame(rows).to_string(index=False).splitlines():
        print(f"  {line}")

def _print_block(a: Dict, crop: str, lat: float, lon: float,
                 mode: str, n_models: int, n_scenarios: int) -> None:
    y, sn, t = a['year'], a['season_number'], a['total_seasons_per_year']
    label = f"Year {y}  -  Season {sn} of {t}" if t > 1 else f"Year {y}"

    print(f"\n{'─'*70}")
    print(f"  {label}   (ensemble of {a['n_projections']} projections)")
    print(f"\n{'='*70}")
    print(f"  CROP HAZARD ASSESSMENT (ENSEMBLE): {crop.upper()}")
    print(f"{'='*70}")
    print(f"  Location: {lat:.4f}, {lon:.4f}")

    w = a['window']
    print(f"\n  Season Information")
    print(f"  {'─'*66}")
    print(f"  Onset (median):  {w['start_median']:<20} End (median): {w['end_median']}")
    print(f"  Length (mean):   {w['length_days_mean']} days{'':10} Method: {mode}")
    print(f"  Source:          nex_gddp ({n_models} models × {n_scenarios} scenarios)")

    _print_projection_breakdown(a)

    s = a['season_statistics']
    if 'total_precipitation_mm' in s:
        print(f"\n  Precipitation Statistics  (ensemble means)")
        print(f"  {'─'*66}")
        print(f"  {'Metric':<32} {'Value':>15}  Unit")
        print(f"  {'─'*32} {'─'*15}  {'─'*10}")
        print(f"  {'Total':<32} {s['total_precipitation_mm']:>15.2f}  mm")
        print(f"  {'Daily Mean':<32} {s.get('mean_daily_precipitation_mm', 0):>15.2f}  mm")
        print(f"  {'Daily Maximum':<32} {s.get('max_daily_precipitation_mm', 0):>15.2f}  mm")
        print(f"  {'Rainy Days (>=1mm)':<32} {s.get('rainy_days', 0):>15.2f}  days")
        print(f"  {'Dry Days (<1mm)':<32} {s.get('dry_days', 0):>15.2f}  days")

    if 'dry_spell_statistics' in s:
        ds = s['dry_spell_statistics']
        print(f"\n  Dry Spell Statistics  (ensemble means; >=7 consecutive days <1mm)")
        print(f"  {'─'*66}")
        print(f"  {'Number of Dry Spells':<32} {ds['number_of_dry_spells']:>15.2f}  spells")
        print(f"  {'Max Dry Spell Length':<32} {ds['max_dry_spell_length_days']:>15.2f}  days")
        print(f"  {'Mean Dry Spell Length':<32} {ds['mean_dry_spell_length_days']:>15.2f}  days")

        if ds.get('length_distribution'):
            print(f"\n  Length Distribution  (mean spell count per bucket)")
            print(f"  {'─'*66}")
            for rng in sorted(ds['length_distribution'].keys(), key=_bucket_key):
                cnt = ds['length_distribution'][rng]
                print(f"  {rng:<15} days: {cnt:>6.2f} spell(s)")

    if 'mean_temperature_c' in s:
        print(f"\n  Temperature Statistics  (ensemble means)")
        print(f"  {'─'*66}")
        print(f"  {'Metric':<32} {'Value':>15}  Unit")
        print(f"  {'─'*32} {'─'*15}  {'─'*10}")
        print(f"  {'Mean Temperature':<32} {s['mean_temperature_c']:>15.2f}  deg C")
        print(f"  {'Mean Tmax':<32} {s.get('mean_tmax_c', 0):>15.2f}  deg C")
        print(f"  {'Mean Tmin':<32} {s.get('mean_tmin_c', 0):>15.2f}  deg C")
        print(f"  {'Max Tmax':<32} {s.get('max_tmax_c', s.get('max_temperature_c', 0)):>15.2f}  deg C")
        print(f"  {'Min Tmin':<32} {s.get('min_tmin_c', s.get('min_temperature_c', 0)):>15.2f}  deg C")

    # Hazard index counts (NTx35, NTx40, NDD, NDWS, NDWL0) -- ensemble means
    has_counts = any(k in s for k in ('NTx35', 'NTx40', 'NDD', 'NDWS', 'NDWL0'))
    if has_counts:
        print(f"\n  Hazard Index Counts  (ensemble means)")
        print(f"  {'─'*66}")
        print(f"  {'Index':<32} {'Value':>15}  Unit")
        print(f"  {'─'*32} {'─'*15}  {'─'*10}")
        if 'NTx35' in s:
            print(f"  {'NTx35 (days Tmax > 35C)':<32} {s['NTx35']:>15.2f}  days")
        if 'NTx40' in s:
            print(f"  {'NTx40 (days Tmax > 40C)':<32} {s['NTx40']:>15.2f}  days")
        if 'NDD' in s:
            print(f"  {'NDD (dry days, <1mm)':<32} {s['NDD']:>15.2f}  days")
        if 'NDWS' in s:
            print(f"  {'NDWS (water-stress days)':<32} {s['NDWS']:>15.2f}  days")
        if 'NDWL0' in s:
            print(f"  {'NDWL0 (water-logging days)':<32} {s['NDWL0']:>15.2f}  days")

    h = a['hazard_evaluation']
    print(f"\n  Hazard Assessment  (based on ensemble means)")
    print(f"  {'─'*66}")
    print(f"  {'Indicator':<25} {'Value':>18}  Status")
    print(f"  {'─'*25} {'─'*18}  {'─'*20}")
    if 'precipitation' in h:
        p = h['precipitation']
        print(f"  {'Precipitation':<25} {s.get('total_precipitation_mm', 0):>16.2f} mm  "
              f"[{_sym(p['status'])}] {p['status'].replace('_', ' ').upper()}")
    if 'temperature' in h:
        t_ = h['temperature']
        print(f"  {'Temperature':<25} {s.get('mean_temperature_c', 0):>16.2f} degC "
              f"[{_sym(t_['status'])}] {t_['status'].replace('_', ' ').upper()}")
    if 'water_stress' in h:
        ws = h['water_stress']
        print(f"  {'Water Stress (NDWS)':<25} {s.get('NDWS', 0):>16.2f} d   "
              f"[{_severity_symbol(ws['status'])}] {ws['status'].replace('_', ' ').upper()}")
    if 'water_logging' in h:
        wl = h['water_logging']
        print(f"  {'Water Logging (NDWL0)':<25} {s.get('NDWL0', 0):>16.2f} d   "
              f"[{_severity_symbol(wl['status'])}] {wl['status'].replace('_', ' ').upper()}")
    print(f"\n{'='*70}")

def print_results(r: Dict) -> None:
    if 'error' in r:
        print(f"\nError: {r['error']}")
        return

    crop = r['crop']
    lat, lon = r['location']['latitude'], r['location']['longitude']
    mode = r['season_mode'] + (f" ({r['season_definition']})" if r.get('season_definition') else '')
    nm, ns = len(r['models']), len(r['scenarios'])

    print(f"\n{'='*70}")
    print(f"  ENSEMBLE HAZARD ASSESSMENT (NEX-GDDP)")
    print(f"{'='*70}")
    print(f"  Crop:              {crop}")
    print(f"  Location:          {lat:.4f}, {lon:.4f}")
    print(f"  Period:            {r['period']['start_year']} -> {r['period']['end_year']}")
    print(f"  Mode:              {mode}")
    print(f"  Models:            {nm}")
    print(f"  Scenarios:         {', '.join(r['scenarios'])}")
    print(f"  Total projections: {r['n_total_projections']}")

    for a in r['assessments']:
        _print_block(a, crop, lat, lon, mode, nm, ns)

    o = r['overall_ensemble']
    s, h = o['season_statistics'], o['hazard_evaluation']
    print(f"\n{'─'*70}")
    print(f"  OVERALL ENSEMBLE  (across all year × season × model × scenario)")
    print(f"  n = {o['n_projections']} projections")
    print(f"  {'─'*66}")
    print(f"  Precipitation (mean): {s.get('total_precipitation_mm', 0):.2f} mm per season")
    print(f"  Temperature   (mean): {s.get('mean_temperature_c', 0):.2f} deg C")
    if 'max_tmax_c' in s or 'min_tmin_c' in s:
        print(f"  Max Tmax / Min Tmin : {s.get('max_tmax_c', 0):.2f} / "
              f"{s.get('min_tmin_c', 0):.2f} deg C")
    if any(k in s for k in ('NTx35', 'NTx40', 'NDD', 'NDWS', 'NDWL0')):
        parts = []
        if 'NTx35' in s: parts.append(f"NTx35={s['NTx35']:.2f}")
        if 'NTx40' in s: parts.append(f"NTx40={s['NTx40']:.2f}")
        if 'NDD'   in s: parts.append(f"NDD={s['NDD']:.2f}")
        if 'NDWS'  in s: parts.append(f"NDWS={s['NDWS']:.2f}")
        if 'NDWL0' in s: parts.append(f"NDWL0={s['NDWL0']:.2f}")
        print(f"  Hazard indices      : {'  '.join(parts)}  (mean days per season)")
    if 'dry_spell_statistics' in s:
        ds = s['dry_spell_statistics']
        print(f"  Dry spells    (mean): {ds['number_of_dry_spells']:.2f} per season  "
              f"(max length {ds['max_dry_spell_length_days']:.2f} days)")
    if 'precipitation' in h:
        print(f"  Precip status:        [{_sym(h['precipitation']['status'])}] "
              f"{h['precipitation']['status'].replace('_', ' ').upper()}")
    if 'temperature' in h:
        print(f"  Temp   status:        [{_sym(h['temperature']['status'])}] "
              f"{h['temperature']['status'].replace('_', ' ').upper()}")
    if 'water_stress' in h:
        print(f"  NDWS   status:        [{_severity_symbol(h['water_stress']['status'])}] "
              f"{h['water_stress']['status'].replace('_', ' ').upper()}")
    if 'water_logging' in h:
        print(f"  NDWL0  status:        [{_severity_symbol(h['water_logging']['status'])}] "
              f"{h['water_logging']['status'].replace('_', ' ').upper()}")
    print(f"\n{'='*70}\n")

# CLI
if __name__ == "__main__":
    # Ensure Unicode output works on Windows consoles that default to cp1252.
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except (AttributeError, ValueError):
        pass

    p = argparse.ArgumentParser(
        description='Ensemble of hazards.py across NEX-GDDP models x scenarios.',
    )
    p.add_argument('crop', nargs='?', default='maize',
                   help='Crop name (default: maize). Same options as hazards.py.')
    p.add_argument('--list-models', action='store_true',
                   help='Print available models & scenarios, then exit.')
    p.add_argument('--location',     type=str, help='"lat,lon"')
    p.add_argument('--start-year',   type=int)
    p.add_argument('--end-year',     type=int)
    p.add_argument('--fixed-season', type=str, default=None,
                   metavar='MM-DD:MM-DD[,MM-DD:MM-DD]',
                   help="omit for auto-detect; otherwise single, two, or year-crossing windows")
    p.add_argument('--models',       type=str, default=','.join(MODELS),
                   help='comma-separated GCMs (default: all 16)')
    p.add_argument('--scenarios',    type=str, default=','.join(SCENARIOS),
                   help=f"comma-separated scenarios (default: {','.join(SCENARIOS)})")
    p.add_argument('--soil-source', choices=['constant', 'auto'], default='auto',
                   help="Soil capacity source for NDWS/NDWL0 (default: auto): 'auto' "
                        "derives per-location values from SoilGrids (needs GEE; falls "
                        "back to constants); 'constant' uses the fixed --soilcp/--soilsat.")
    p.add_argument('--soilcp',  type=float, default=DEFAULT_SOILCP,
                   help=f'Soil available water capacity at field capacity, mm '
                        f'(water-balance NDWS/NDWL0; default: {DEFAULT_SOILCP})')
    p.add_argument('--soilsat', type=float, default=DEFAULT_SOILSAT,
                   help=f'Extra soil water from field capacity to saturation, mm '
                        f'(water-balance NDWL0; default: {DEFAULT_SOILSAT})')
    p.add_argument('--format',       choices=['json', 'text'], default='text')
    p.add_argument('--output',       type=str, default=None,
                   help='write full result as JSON to this path')
    args = p.parse_args()

    if args.list_models:
        print("Models:");    [print(f"  {m}") for m in MODELS]
        print("\nScenarios:"); [print(f"  {s}") for s in SCENARIOS]
        sys.exit(0)

    missing = [n for n, v in (('--location',  args.location),
                              ('--start-year', args.start_year),
                              ('--end-year',   args.end_year)) if v in (None, '')]
    if missing:
        p.error(f"missing required arguments: {', '.join(missing)}")

    lat, lon  = map(float, args.location.split(','))
    models    = [s.strip() for s in args.models.split(',')    if s.strip()]
    scenarios = [s.strip() for s in args.scenarios.split(',') if s.strip()]

    # Derive per-location soil capacity once (shared across all model/scenario projections); falls back to constants if SoilGrids/GEE is unavailable.
    soilcp, soilsat = args.soilcp, args.soilsat
    if args.soil_source == 'auto':
        try:
            from soil_capacity import fetch_soil_capacity
        except ImportError:
            from climate_tookit.calculate_hazards.soil_capacity import fetch_soil_capacity
        print("Deriving per-location soil capacity from SoilGrids...")
        soilcp, soilsat = fetch_soil_capacity(lat, lon)

    result = calculate_ensemble(
        crop=args.crop,
        lat=lat, lon=lon,
        start_year=args.start_year, end_year=args.end_year,
        models=models, scenarios=scenarios,
        fixed_season=args.fixed_season,
        soilcp=soilcp, soilsat=soilsat,
    )

    if args.format == 'json':
        print(json.dumps(result, indent=2, default=str))
    else:
        print_results(result)

    if args.output:
        os.makedirs(os.path.dirname(os.path.abspath(args.output)) or '.', exist_ok=True)
        with open(args.output, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        print(f"Saved to: {args.output}")
        
# NOTE: the 1st command in a section includes all models/scenarios while the 2nd allows selection  
   
# Fixed single season:
# python -m climate_tookit.calculate_hazards.ensemble_hazards millet --location="-1.286,36.817" --start-year 2040 --end-year 2060 --fixed-season "03-01:05-31" --scenarios ssp245,ssp585 --output ensemble_mam_all.json
# python -m climate_tookit.calculate_hazards.ensemble_hazards maize --location="-1.286,36.817" --start-year 2040 --end-year 2060 --fixed-season "03-01:05-31" --models "ACCESS-CM2,EC-Earth3,MRI-ESM2-0" --scenarios ssp585 --output ensemble_mam.json

# Fixed two seasons:
# python -m climate_tookit.calculate_hazards.ensemble_hazards rice --location="-1.286,36.817" --start-year 2040 --end-year 2060 --fixed-season "03-01:05-31,10-01:12-15" --scenarios ssp245,ssp585 --output ensemble_mam_ond_all.json
# python -m climate_tookit.calculate_hazards.ensemble_hazards beans --location="-1.286,36.817" --start-year 2040 --end-year 2060 --fixed-season "03-01:05-31,10-01:12-15" --models "ACCESS-CM2,EC-Earth3,MRI-ESM2-0" --scenarios ssp245,ssp585 --output ensemble_mam_ond.json

# Fixed year-crossing season:
# python -m climate_tookit.calculate_hazards.ensemble_hazards sorghum --location="-1.286,36.817" --start-year 2040 --end-year 2060 --fixed-season "11-01:02-28" --scenarios ssp245,ssp585 --output ensemble_njf_all.json
# python -m climate_tookit.calculate_hazards.ensemble_hazards cassava --location="-1.286,36.817" --start-year 2040 --end-year 2060 --fixed-season "11-01:02-28" --models "ACCESS-CM2,EC-Earth3,MRI-ESM2-0" --scenarios ssp585 --output ensemble_njf.json

# List available NEX-GDDP models and scenarios:
# python -m climate_tookit.calculate_hazards.ensemble_hazards --list-models

# Auto-detect season (NEX-GDDP per (model, scenario), no flag) -- all models, pick scenarios:
# python -m climate_tookit.calculate_hazards.ensemble_hazards maize --location="-1.286,36.817" --start-year 2040 --end-year 2060 --scenarios ssp245,ssp585 --output ensemble_auto_all.json

# Auto-detect season -- custom subset:
# python -m climate_tookit.calculate_hazards.ensemble_hazards maize --location="-1.286,36.817" --start-year 2040 --end-year 2060 --models "ACCESS-CM2,EC-Earth3,MRI-ESM2-0" --scenarios ssp245,ssp585 --output ensemble_auto.json