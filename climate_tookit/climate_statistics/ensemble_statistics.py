"""
NEX-GDDP CMIP6 Ensemble Climate Statistics
- Runs statistics.analyze_climate_statistics() once per NEX-GDDP CMIP6 model under a given SSP scenario, then ensemble-averages each model's output
into one cross-model result. Use for Future LTM season summaries that draw from the CMIP6 ensemble; for non-NEX-GDDP single-source LTMs, run
statistics.py directly.
- All climate fetch / season detection / per-season + LTM computation is delegated to statistics.py -- this module only loops over the 16 CMIP6 models
and ensemble-averages the per-model results, then renders them with statistics.py's display functions so the output mirrors statistics.py.

FUTURE LTM SEASON SUMMARY -- two-step pipeline (NOT annual ensemble / N years):
    Step 1 (per model, inside statistics.py):
        For each of the 16 CMIP6 models M:
            seasons_M  = analyze_climate_statistics(source='nex_gddp', model=M, ...)
                         -> per-year per-season metric values for the focal period.
            LTM_M      = statistics.ltm_season_summary(seasons_M, fixed_season)
                         -> for each season window, the long-term mean of each
                            metric averaged across the focal years (the same LTM statistics.py prints for a single source).
    Step 2 (cross-model, in this module):
        ensemble_LTM   = _ensemble_average_per_model_ltms([LTM_1, ..., LTM_16])
                         -> for each season window, the simple mean across theper-model LTMs (each model weighted equally).
The per-model LTMs feeding step 2 are exposed on the result dict under `per_model_ltm` so callers can verify the ensemble = mean of per-model values.

Output shape mirrors statistics.analyze_climate_statistics(): location, period, source='nex_gddp', mode, fixed_season, scenario, season_statistics,
ltm_season_summary, annual_summary, coverage_warning, analysis_date, methodology. Adds: ensemble=True, models_used, models_failed, n_models_ok,
per_model_ltm.
"""
import os
import sys
import json
import argparse
from datetime import datetime
from pathlib import Path
from typing import Tuple, Dict, List, Any, Optional

current_dir  = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.insert(0, project_root)

from climate_tookit.climate_statistics.statistics import (
    analyze_climate_statistics,
    print_ltm_by_season,
    print_annual,
    BASELINE_DEFAULT_PERIOD,
    MIN_LTM_YEARS,
    _is_num,
    _avg,
)

import pandas as pd

# NEX-GDDP CMIP6 ensemble -- 16 models + canonical SSP labels
NEX_GDDP_MODELS: List[str] = [
    "ACCESS-CM2", "ACCESS-ESM1-5", "CanESM5", "CMCC-ESM2", "EC-Earth3",
    "EC-Earth3-Veg-LR", "GFDL-ESM4", "INM-CM4-8", "INM-CM5-0", "KACE-1-0-G",
    "MIROC6", "MPI-ESM1-2-LR", "MRI-ESM2-0", "NorESM2-LM", "NorESM2-MM",
    "TaiESM1",
]
SSP_SCENARIOS: List[str] = ["ssp126", "ssp245", "ssp585", "historical"]
SCENARIO_ALIASES: Dict[str, str] = {
    "SSP1-2.6": "ssp126", "SSP2-4.5": "ssp245", "SSP5-8.5": "ssp585",
    "ssp126":   "ssp126", "ssp245":   "ssp245", "ssp585":   "ssp585",
    "historical": "historical",
}

def _normalize_scenario(s: str) -> Optional[str]:
    """Map any accepted SSP alias to the canonical scenario string, else None."""
    return SCENARIO_ALIASES.get(s.strip()) if isinstance(s, str) else None

# Cross-model aggregation helpers
def _ensemble_seasons(per_model_seasons: List[List[Dict[str, Any]]]
                      ) -> List[Dict[str, Any]]:
    """
    Match per-model season entries by (year, season_number) and ensemble-average each numeric metric. Onset/cessation/regime are copied from the
    first model (they match exactly for fixed seasons; for auto-detected runs they may vary slightly so this is a representative pick).
    Returns a season_statistics list shaped like statistics.py's output.
    """
    pool: Dict[Tuple[Any, Any], List[Dict[str, Any]]] = {}
    for ms in per_model_seasons:
        for s in (ms or []):
            key = (s.get('year'), s.get('season_number'))
            pool.setdefault(key, []).append(s)

    out: List[Dict[str, Any]] = []
    for key in sorted(pool, key=lambda k: (k[0] or 0, k[1] or 0)):
        bucket = pool[key]
        first  = bucket[0]
        length_avg = _avg([b.get('length_days') for b in bucket], 1)
        entry: Dict[str, Any] = {
            'year':          key[0],
            'season_number': key[1],
            'regime':        first.get('regime'),
            'onset':         first.get('onset'),
            'cessation':     first.get('cessation'),
            'length_days':   int(round(length_avg)) if length_avg is not None else 0,
            'n_models':      len(bucket),
        }
        for cat in ('precipitation', 'temperature', 'water_balance'):
            cat_pool: Dict[str, List[float]] = {}
            for b in bucket:
                for k, v in (b.get(cat) or {}).items():
                    if _is_num(v):
                        cat_pool.setdefault(k, []).append(float(v))
            if cat_pool:
                entry[cat] = {k: _avg(vs, 2) for k, vs in cat_pool.items()}

        # overall_statistics: top-level total_days is an int, the rest are nested dicts
        ov_nested: Dict[str, Dict[str, List[float]]] = {}
        td_pool:   List[float]                       = []
        for b in bucket:
            ov = b.get('overall_statistics') or {}
            if _is_num(ov.get('total_days')):
                td_pool.append(float(ov['total_days']))
            for cat, mets in ov.items():
                if not isinstance(mets, dict):
                    continue
                for k, v in mets.items():
                    if _is_num(v):
                        ov_nested.setdefault(cat, {}).setdefault(k, []).append(float(v))
        if ov_nested or td_pool:
            ov_entry: Dict[str, Any] = {}
            if td_pool:
                td_mean = _avg(td_pool, 0)
                ov_entry['total_days'] = int(round(td_mean)) if td_mean is not None else 0
            for cat, mets in ov_nested.items():
                ov_entry[cat] = {k: _avg(vs, 2) for k, vs in mets.items()}
            entry['overall_statistics'] = ov_entry

        # raw_climate_summary
        raw_pool: Dict[str, Dict[str, List[float]]] = {}
        for b in bucket:
            for row in (b.get('raw_climate_summary') or []):
                var = row.get('Variable')
                if not var:
                    continue
                for stat in ('Mean', 'Min', 'Max', 'Std'):
                    v = row.get(stat)
                    if _is_num(v):
                        raw_pool.setdefault(var, {}).setdefault(stat, []).append(float(v))
        if raw_pool:
            entry['raw_climate_summary'] = [
                {'Variable': var,
                 'Mean':     _avg(mets.get('Mean', []), 3),
                 'Min':      _avg(mets.get('Min',  []), 3),
                 'Max':      _avg(mets.get('Max',  []), 3),
                 'Std':      _avg(mets.get('Std',  []), 3)}
                for var, mets in raw_pool.items()
            ]
        out.append(entry)
    return out

def _ensemble_annual(per_model_annual: List[Dict[str, Dict]]) -> Dict[str, Dict]:
    """
    Ensemble-average annual_summary across models. Averages annual_rain_mm; reports humid-model count as 'X/N models humid'.
    """
    year_pool: Dict[str, List[Dict]] = {}
    for m in per_model_annual:
        for year, info in (m or {}).items():
            if info:
                year_pool.setdefault(str(year), []).append(info)
    out: Dict[str, Dict] = {}
    for year in sorted(year_pool):
        bucket = year_pool[year]
        n = len(bucket)
        humid_count = sum(1 for v in bucket if v.get('is_humid'))
        out[year] = {
            'annual_rain_mm':  _avg([v.get('annual_rain_mm') for v in bucket], 1),
            'is_humid':        f"{humid_count}/{n} models",
            'low_rain_months': 'n/a',
            'humid_test':      f"{humid_count}/{n} humid",
        }
    return out

def _ensemble_average_per_model_ltms(per_model_ltms: List[Dict[str, Any]]
                                     ) -> Dict[str, Any]:
    """
    STEP 2 of the FUTURE LTM SEASON SUMMARY pipeline.
    Average per-model LTM windows across models. This function does NOT touch year-level data -- each input element is one model's
    `ltm_season_summary` output from statistics.py (already collapsed over the focal years for that model). Each model is weighted equally
    (1 / n_models); models are paired by season_number so fixed-season windows stay aligned. Returns the cross-model ensemble LTM with the same
    window shape statistics.py uses.
    """
    win_pool: Dict[int, List[Dict[str, Any]]] = {}
    mode = 'fixed'
    for ltm in per_model_ltms:
        mode = ltm.get('mode', mode) if isinstance(ltm, dict) else mode
        for w in (ltm or {}).get('windows', []) or []:
            win_pool.setdefault(w.get('season_number', 1), []).append(w)

    windows: List[Dict[str, Any]] = []
    for sn in sorted(win_pool):
        bucket = win_pool[sn]
        first  = bucket[0]
        agg: Dict[str, Any] = {
            'window':            first.get('window'),
            'season_number':     sn,
            'n_models':          len(bucket),
            'n_years_per_model': first.get('n_years'),
            'length_days_avg':   _avg([w.get('length_days_avg') for w in bucket], 1),
        }
        for cat in ('precipitation', 'temperature', 'water_balance'):
            pool: Dict[str, List[float]] = {}
            for w in bucket:
                for k, v in (w.get(cat) or {}).items():
                    if _is_num(v):
                        pool.setdefault(k, []).append(float(v))
            if pool:
                agg[cat] = {k: _avg(vs, 2) for k, vs in pool.items()}

        ov_pool: Dict[str, Dict[str, List[float]]] = {}
        for w in bucket:
            for cat, metrics in (w.get('overall_statistics') or {}).items():
                if not isinstance(metrics, dict):
                    continue
                for k, v in metrics.items():
                    if _is_num(v):
                        ov_pool.setdefault(cat, {}).setdefault(k, []).append(float(v))
        if ov_pool:
            agg['overall_statistics'] = {
                cat: {k: _avg(vs, 2) for k, vs in mets.items()}
                for cat, mets in ov_pool.items()
            }

        raw_pool: Dict[str, Dict[str, List[float]]] = {}
        for w in bucket:
            for row in (w.get('raw_climate_summary') or []):
                var = row.get('Variable')
                if not var:
                    continue
                for stat in ('Mean', 'Min', 'Max', 'Std'):
                    v = row.get(stat)
                    if _is_num(v):
                        raw_pool.setdefault(var, {}).setdefault(stat, []).append(float(v))
        if raw_pool:
            agg['raw_climate_summary'] = [
                {'Variable': var,
                 'Mean':     _avg(mets.get('Mean', []), 3),
                 'Min':      _avg(mets.get('Min',  []), 3),
                 'Max':      _avg(mets.get('Max',  []), 3),
                 'Std':      _avg(mets.get('Std',  []), 3)}
                for var, mets in raw_pool.items()
            ]
        windows.append(agg)

    return {'mode': mode, 'windows': windows}

# Main API
def analyze_ensemble_nex_gddp(
    location_coord: Tuple[float, float],
    start_year:     int,
    end_year:       int,
    scenario:       str,
    fixed_season:   Optional[str] = None,
    models:         Optional[List[str]] = None,
    exclude_models: Optional[List[str]] = None,
    extra_months:   int = 6,
    verbose:        bool = True,
) -> Dict[str, Any]:
    """
    Future LTM via NEX-GDDP CMIP6 ensemble.
    Delegates each per-model run to statistics.analyze_climate_statistics() with source='nex_gddp', then ensemble-averages the per-model season /
    LTM / annual outputs. Default model set is the 16 CMIP6 models in NEX_GDDP_MODELS; pass `models` to subset or `exclude_models` to drop.
    """
    canon = _normalize_scenario(scenario)
    if not canon:
        return {'error': (f"scenario '{scenario}' not recognised. "
                          f"Accepted: {sorted(SCENARIO_ALIASES)}")}
    scenario = canon

    active = list(models) if models else list(NEX_GDDP_MODELS)
    if exclude_models:
        excl = {m.upper() for m in exclude_models}
        active = [m for m in active if m.upper() not in excl]
    if not active:
        return {'error': 'No models selected after filtering.'}

    if verbose:
        print(f"\n{'=' * 60}")
        print("NEX-GDDP CMIP6 Ensemble Climate Statistics")
        print(f"  Location : {location_coord[0]}, {location_coord[1]}")
        print(f"  Period   : {start_year}-{end_year}")
        print(f"  Scenario : {scenario}")
        if fixed_season:
            print(f"  Seasons  : {fixed_season}")
        print(f"  Models   : {len(active)}")
        print(f"{'=' * 60}")

    # Per-model collection. The LTM list is keyed by model name so callers can verify the ensemble = mean of per-model values.
    per_model_seasons: List[List[Dict[str, Any]]] = []
    per_model_ltm_list: List[Dict[str, Any]]     = []  # ordered, used for step 2 below
    per_model_ltm_by_name: Dict[str, Dict[str, Any]] = {}
    per_model_annual:  List[Dict[str, Dict]]     = []
    models_ok: List[str]            = []
    failed:    List[Dict[str, str]] = []
    for i, model in enumerate(active, 1):
        if verbose:
            print(f"\n  [{i:02d}/{len(active):02d}] {model}", flush=True)
        try:
            # STEP 1 of the FUTURE LTM pipeline (per-model):
            r = analyze_climate_statistics(
                location_coord=location_coord,
                start_year=start_year, end_year=end_year,
                source='nex_gddp', fixed_season=fixed_season,
                model=model, scenario=scenario,
                extra_months=extra_months,
            )
            seasons    = r.get('season_statistics') or []
            ltm_model  = r.get('ltm_season_summary') or {}  
            annual     = r.get('annual_summary') or {}
            if not seasons and not ltm_model.get('windows'):
                failed.append({'model': model,
                               'error': 'no seasons or LTM produced'})
                if verbose:
                    print("    x  no seasons or LTM produced")
                continue
            per_model_seasons.append(seasons)
            per_model_ltm_list.append(ltm_model)
            per_model_ltm_by_name[model] = ltm_model
            per_model_annual.append(annual)
            models_ok.append(model)
            if verbose:
                print("    ok")
        except Exception as exc:
            failed.append({'model': model, 'error': str(exc)})
            if verbose:
                print(f"    x  {exc}")

    if not models_ok:
        return {'error': 'All models failed.', 'failed_models': failed}

    # STEP 2 of the FUTURE LTM pipeline (cross-model):
    ltm_summary = _ensemble_average_per_model_ltms(per_model_ltm_list)

    season_statistics = _ensemble_seasons(per_model_seasons)
    annual_summary    = _ensemble_annual(per_model_annual)

    years_span = end_year - start_year + 1
    coverage_warning = (
        f"LTM coverage is {years_span} year(s); recommended ≥ {MIN_LTM_YEARS}."
        if years_span < MIN_LTM_YEARS else None
    )

    return {
        'location':           {'lat': location_coord[0],
                               'lon': location_coord[1]},
        'period':             {'start_year': start_year,
                               'end_year':   end_year},
        'source':             'nex_gddp',
        'mode':               'fixed' if fixed_season else 'auto',
        'fixed_season':       fixed_season,
        'scenario':           scenario,
        'ensemble':           True,
        'models_used':        models_ok,
        'models_failed':      failed,
        'n_models_ok':        len(models_ok),
        'season_statistics':  season_statistics,
        'ltm_season_summary': ltm_summary,
        'per_model_ltm':      per_model_ltm_by_name,
        'annual_summary':     annual_summary,
        'coverage_warning':   coverage_warning,
        'analysis_date':      datetime.now().isoformat(),
        'methodology':        ('FUTURE LTM SEASON SUMMARY computed as: '
                               'Step 1 -- statistics.ltm_season_summary per model '
                               '(per-window mean across focal years); '
                               'Step 2 -- simple mean across models of those per-model '
                               'LTMs. No annual / N-years shortcut is used. '
                               'SEASON STATISTICS / RAW / OVERALL sections are '
                               'per-(year, season_number) ensemble means across models.'),
    }

# Display -- mirrors statistics.py's print_pandas with an ensemble preamble
def _ltm_header_ensemble(result: Dict[str, Any]) -> str:
    """Pick FUTURE / BASELINE / generic ensemble LTM header by run window."""
    end          = (result.get('period') or {}).get('end_year',   0)
    start        = (result.get('period') or {}).get('start_year', 0)
    baseline_end = BASELINE_DEFAULT_PERIOD[1]
    if start > baseline_end:
        return "FUTURE LTM SEASON SUMMARY (NEX-GDDP CMIP6 ensemble)"
    if end <= baseline_end:
        return "BASELINE LTM SEASON SUMMARY (NEX-GDDP CMIP6 ensemble)"
    return "LTM SEASON SUMMARY (NEX-GDDP CMIP6 ensemble)"

def _print_indented_table(df: pd.DataFrame, indent: str = "    ") -> None:
    for line in df.to_string(index=False).splitlines():
        print(f"{indent}{line}")

def _window_header(w: Dict[str, Any]) -> str:
    """One-line header for an LTM window, e.g. 'Window 03-01:05-31 (season #1, n_models=16, n_years_per_model=21)'."""
    bits = [f"season #{w.get('season_number')}"]
    if 'n_models' in w:
        bits.append(f"n_models={w['n_models']}")
    if w.get('n_years_per_model') is not None:
        bits.append(f"n_years_per_model={w['n_years_per_model']}")
    elif w.get('n_years') is not None:
        bits.append(f"n_years={w['n_years']}")
    return f"Window {w.get('window')} ({', '.join(bits)})"

def _print_overall_raw_summary(ensemble_ltm: Dict[str, Any]) -> None:
    """One mean/min/max/std table per LTM window (overall, no per-year repetition)."""
    print("\n" + "=" * 70)
    print("RAW CLIMATE SUMMARY (overall, ensemble of models × years)")
    print("=" * 70)
    windows = (ensemble_ltm or {}).get('windows') or []
    if not windows:
        print("(no LTM windows)")
        return
    for w in windows:
        print(f"\n  {_window_header(w)}")
        rows = w.get('raw_climate_summary') or []
        if not rows:
            print("    (no data)")
            continue
        _print_indented_table(pd.DataFrame(rows).fillna("n/a"))

def _print_overall_statistics(ensemble_ltm: Dict[str, Any]) -> None:
    """One category/metric table per LTM window (overall, no per-year repetition)."""
    print("\n" + "=" * 70)
    print("OVERALL STATISTICS (overall, ensemble of models × years)")
    print("=" * 70)
    windows = (ensemble_ltm or {}).get('windows') or []
    if not windows:
        print("(no LTM windows)")
        return
    for w in windows:
        print(f"\n  {_window_header(w)}")
        stats = w.get('overall_statistics') or {}
        if not stats:
            print("    (no data)")
            continue
        if 'total_days' in stats:
            print(f"    Total days: {stats['total_days']}")
        rows = []
        for var_key, var_label in [
            ('precipitation', 'Precipitation'),
            ('temperature',   'Temperature'),
            ('et0',           'ET0'),
            ('water_balance', 'Water Balance'),
        ]:
            block = stats.get(var_key)
            if not isinstance(block, dict):
                continue
            for metric, value in block.items():
                rows.append({
                    'Variable': var_label,
                    'Metric':   metric,
                    'Value':    value if value is not None else "n/a",
                })
        if rows:
            _print_indented_table(pd.DataFrame(rows))

def _print_per_model_ltm_breakdown(per_model_ltm: Dict[str, Dict[str, Any]],
                                   ensemble_ltm:  Dict[str, Any]) -> None:
    """
    For each season window, print one row per model showing that model's LTM
    metrics, then the ensemble mean at the bottom. This makes the 2-step pipeline
    inspectable: the reader can verify the ensemble row equals the column means
    of the per-model rows.
    """
    if not per_model_ltm or not ensemble_ltm.get('windows'):
        return
    print("\n" + "─" * 70)
    print("PER-MODEL LTM BREAKDOWN (Step 1) feeding the ensemble (Step 2)")
    print("─" * 70)
    for ens_w in ensemble_ltm.get('windows') or []:
        sn = ens_w.get('season_number')
        print(f"\n  Window {ens_w.get('window')} (season #{sn})")
        rows = []
        for model_name, m_ltm in per_model_ltm.items():
            target_w = next((w for w in (m_ltm or {}).get('windows', []) or []
                             if w.get('season_number') == sn), None)
            if target_w is None:
                continue
            p  = target_w.get('precipitation') or {}
            t  = target_w.get('temperature')   or {}
            wb = target_w.get('water_balance') or {}
            rows.append({
                'Model':       model_name,
                'n_years':     target_w.get('n_years'),
                'precip_mm':   p.get('total_mm'),
                'rainy_days':  p.get('rainy_days'),
                'mean_tmax_c': t.get('mean_tmax'),
                'mean_tmin_c': t.get('mean_tmin'),
                'wb_total':    wb.get('total_balance'),
            })
        # ensemble row
        ep  = ens_w.get('precipitation') or {}
        et_ = ens_w.get('temperature')   or {}
        ewb = ens_w.get('water_balance') or {}
        rows.append({
            'Model':       f"ENSEMBLE (mean of {len(per_model_ltm)})",
            'n_years':     ens_w.get('n_years_per_model'),
            'precip_mm':   ep.get('total_mm'),
            'rainy_days':  ep.get('rainy_days'),
            'mean_tmax_c': et_.get('mean_tmax'),
            'mean_tmin_c': et_.get('mean_tmin'),
            'wb_total':    ewb.get('total_balance'),
        })
        _print_indented_table(pd.DataFrame(rows).fillna('n/a'))

def print_report(result: Dict[str, Any]) -> None:
    """Render the ensemble result with the same section layout as statistics.print_pandas."""
    if 'error' in result:
        print(f"\nError: {result['error']}")
        for f in result.get('failed_models', []) or []:
            print(f"  - {f.get('model')}: {f.get('error')}")
        return

    n_ok   = result.get('n_models_ok', 0)
    n_fail = len(result.get('models_failed') or [])
    print(f"\n{'=' * 70}")
    print(f"ENSEMBLE: NEX-GDDP CMIP6 "
          f"({result['period']['start_year']}-{result['period']['end_year']})  "
          f"| scenario={result['scenario']}  | {n_ok}/{n_ok + n_fail} models ok")
    print(f"{'=' * 70}")
    print(f"  Location  : {result['location']['lat']:.4f}, "
          f"{result['location']['lon']:.4f}")
    print(f"  Source    : {result['source']}  | mode={result['mode']}")
    if result.get('fixed_season'):
        print(f"  Fixed     : {result['fixed_season']}")
    if result.get('models_failed'):
        failed_names = ', '.join(f['model']
                                 for f in result['models_failed'])
        print(f"  Failed    : {failed_names}")
    if result.get('coverage_warning'):
        print(f"  Coverage  : [WARN] {result['coverage_warning']}")
    print(f"  Note      : per-season tables below are OVERALL values "
          f"(ensemble means across {n_ok} models × focal years), "
          f"not per-year repetitions.")
    print(f"              FUTURE LTM SEASON SUMMARY is the 2-step pipeline -- "
          f"per-model LTM (over focal years) then mean across {n_ok} models.")

    ltm = result.get('ltm_season_summary') or {}
    _print_overall_raw_summary(ltm)
    _print_overall_statistics(ltm)
    print_ltm_by_season(ltm, header=_ltm_header_ensemble(result))
    _print_per_model_ltm_breakdown(result.get('per_model_ltm') or {}, ltm)
    print_annual(result.get('annual_summary', {}))

# CLI
def main() -> None:
    if "--list-models" in sys.argv:
        print("Available NEX-GDDP CMIP6 models:")
        for i, m in enumerate(NEX_GDDP_MODELS, 1):
            print(f"  {i:02d}. {m}")
        print("\nAvailable scenarios (canonical -> accepted aliases):")
        for canon in SSP_SCENARIOS:
            aliases = sorted(a for a, c in SCENARIO_ALIASES.items()
                             if c == canon and a != canon)
            extras = f"  (also: {', '.join(aliases)})" if aliases else ""
            print(f"  - {canon}{extras}")
        sys.exit(0)

    p = argparse.ArgumentParser(
        description=("NEX-GDDP CMIP6 ensemble climate statistics. "
                     "Delegates per-model runs to climate_statistics.statistics "
                     "and averages each model's season / LTM / annual output "
                     "across the ensemble."),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p.add_argument("--location",   required=True,
                   help='Coordinates as "lat,lon" e.g. "-1.286,36.817"')
    p.add_argument("--start-year", type=int, required=True)
    p.add_argument("--end-year",   type=int, required=True)
    p.add_argument("--scenarios",  default="ssp245",
                   metavar="ssp245[,ssp585]",
                   help=("Comma-separated SSP scenarios. Canonical: "
                         f"{', '.join(SSP_SCENARIOS)}.\n"
                         "Aliases also accepted: SSP1-2.6, SSP2-4.5, SSP5-8.5."))
    p.add_argument("--fixed-season", default=None,
                   metavar="MM-DD:MM-DD[,MM-DD:MM-DD]",
                   help=("Optional. Same syntax as statistics.py.\n"
                         "  Single        : '03-01:05-31'\n"
                         "  Two seasons   : '03-01:05-31,10-01:12-15'\n"
                         "  Year-crossing : '11-01:02-28'"))
    p.add_argument("--extra-months", type=int, default=6,
                   help='Extra months past Dec for late cessations '
                        '(auto mode, default: 6)')
    p.add_argument("--models",         default=None,
                   help="Comma-separated subset of CMIP6 models "
                        "(default: all 16)")
    p.add_argument("--exclude-models", default=None,
                   help="Comma-separated CMIP6 models to drop from the ensemble")
    p.add_argument("--list-models",    action="store_true",
                   help="Print available CMIP6 models + scenarios and exit")
    p.add_argument("--format", choices=['json', 'pandas'],
                   default='pandas',
                   help='Output format (default: pandas)')
    p.add_argument("--output",     default=None,
                   help='Output JSON file path (overrides auto-save)')
    p.add_argument("--output-dir", default='.',
                   help='Directory for default JSON output (default: cwd)')
    p.add_argument("--no-save",    action='store_true',
                   help='Skip saving the JSON output')
    p.add_argument("--quiet",      action='store_true',
                   help='Suppress per-model progress prints')
    args = p.parse_args()

    try:
        lat, lon = (float(x) for x in
                    args.location.replace(' ', ',').split(','))
    except ValueError:
        print("Error: --location must be 'lat,lon'.")
        sys.exit(1)

    sub_models = ([m.strip() for m in args.models.split(',')]
                  if args.models else None)
    excl       = ([m.strip() for m in args.exclude_models.split(',')]
                  if args.exclude_models else None)

    # Parse + validate scenarios up front
    raw_scenarios = [s.strip() for s in args.scenarios.split(',') if s.strip()]
    scenarios: List[str] = []
    invalid:   List[str] = []
    for s in raw_scenarios:
        canon = _normalize_scenario(s)
        if canon and canon not in scenarios:
            scenarios.append(canon)
        elif not canon:
            invalid.append(s)
    if invalid:
        print(f"Error: invalid scenario(s) {invalid}. "
              f"Accepted: {sorted(SCENARIO_ALIASES)}")
        sys.exit(1)
    if not scenarios:
        print("Error: no scenarios provided.")
        sys.exit(1)

    all_results: Dict[str, Any] = {}
    any_ok = False
    for scenario in scenarios:
        result = analyze_ensemble_nex_gddp(
            location_coord=(lat, lon),
            start_year=args.start_year,
            end_year=args.end_year,
            scenario=scenario,
            fixed_season=args.fixed_season,
            models=sub_models,
            exclude_models=excl,
            extra_months=args.extra_months,
            verbose=not args.quiet,
        )
        all_results[scenario] = result

        if args.format == 'pandas':
            print_report(result)
        if 'error' not in result:
            any_ok = True

    # JSON dump (single -> bare result, multiple -> {scenario: result})
    payload = (all_results[scenarios[0]] if len(scenarios) == 1
               else all_results)
    if args.format == 'json':
        out = json.dumps(payload, indent=2, default=str)
        if args.output:
            with open(args.output, 'w') as f:
                f.write(out)
            print(f"Saved to {args.output}")
        else:
            print(out)
    elif args.output:
        with open(args.output, 'w') as f:
            json.dump(payload, f, indent=2, default=str)
        print(f"\n✓ SAVED: {args.output}")
    elif not args.no_save:
        sc_tag = scenarios[0] if len(scenarios) == 1 else 'multi'
        fname = (f"ensemble_stats_{lat:.4f}_{lon:.4f}_"
                 f"{args.start_year}_{args.end_year}_{sc_tag}.json")
        path = Path(args.output_dir) / fname
        with open(path, 'w') as f:
            json.dump(payload, f, indent=2, default=str)
        print(f"\n✓ SAVED: {path}")

    if not any_ok:
        sys.exit(1)

if __name__ == "__main__":
    main()

# NOTE: the 1st command in a section includes all models/scenarios while the 2nd allows selection

# List available NEX-GDDP models and scenarios:
# python -m climate_tookit.climate_statistics.ensemble_statistics --list-models

# Fixed single season:
# python -m climate_tookit.climate_statistics.ensemble_statistics --location="-1.286,36.817" --start-year 2040 --end-year 2060 --fixed-season "03-01:05-31" --scenarios ssp245,ssp585 --output ensemble_mam_all.json
# python -m climate_tookit.climate_statistics.ensemble_statistics --location="-1.286,36.817" --start-year 2040 --end-year 2060 --fixed-season "03-01:05-31" --models "ACCESS-CM2,EC-Earth3,MRI-ESM2-0" --scenarios ssp585 --output ensemble_mam.json

# Fixed two seasons:
# python -m climate_tookit.climate_statistics.ensemble_statistics --location="-1.286,36.817" --start-year 2040 --end-year 2060 --fixed-season "03-01:05-31,10-01:12-15" --scenarios ssp245,ssp585 --output ensemble_mam_ond_all.json
# python -m climate_tookit.climate_statistics.ensemble_statistics --location="-1.286,36.817" --start-year 2040 --end-year 2060 --fixed-season "03-01:05-31,10-01:12-15" --models "ACCESS-CM2,EC-Earth3,MRI-ESM2-0" --scenarios ssp245,ssp585 --output ensemble_mam_ond.json

# Fixed year-crossing season:
# python -m climate_tookit.climate_statistics.ensemble_statistics --location="-1.286,36.817" --start-year 2040 --end-year 2060 --fixed-season "11-01:02-28" --scenarios ssp245,ssp585 --output ensemble_njf_all.json
# python -m climate_tookit.climate_statistics.ensemble_statistics --location="-1.286,36.817" --start-year 2040 --end-year 2060 --fixed-season "11-01:02-28" --models "ACCESS-CM2,EC-Earth3,MRI-ESM2-0" --scenarios ssp585 --output ensemble_njf.json

# Auto-detected season (no --fixed-season) -- all models, pick scenarios:
# python -m climate_tookit.climate_statistics.ensemble_statistics --location="-1.286,36.817" --start-year 2040 --end-year 2060 --scenarios ssp245,ssp585 --output ensemble_auto_all.json
# python -m climate_tookit.climate_statistics.ensemble_statistics --location="-1.286,36.817" --start-year 2040 --end-year 2060 --models "ACCESS-CM2,EC-Earth3,MRI-ESM2-0" --scenarios ssp245,ssp585 --output ensemble_auto.json