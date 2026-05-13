"""
NEX-GDDP Ensemble Period Comparison
Runs the same focal-vs-baseline comparison shape as periods.compare(), once per NEX-GDDP CMIP6 model, then averages the per-model results into a single
ensemble comparison.
Difference from periods.compare(): focal is a multi-year *period* (e.g. 2040-2060) rather than a single year. Both sides are annualised before the
diff, so 'focal_avg' and 'baseline_avg' are per-year averages over their respective periods.

Both baseline and focal data come from NEX-GDDP, so each model is compared against its own historical run (the standard model-bias-removing convention).

Why we don't just call periods.compare() in the loop:
    periods.compare()'s signature doesn't accept arbitrary source kwargs and its focal side is a single year. Rather than modify periods.py, this
    module reuses periods.py's diff helpers (_diff_raw, _diff_block, _annualize, _agg_seasons, _round) inside a local _compare_one_model()
    that calls analyze_climate_statistics directly with model+scenario, and uses a local _diff_annual_period() for the annual-summary diff (since
    periods._diff_annual is single-year only).
Output mirrors periods.compare()'s four-section shape, with each leaf replaced by ensemble means + model spread:
    {
        focal_period, focal_years, baseline_period, scenario, fixed_season,
        models_used, models_failed, n_models_ok,
        raw_climate_summary: {var: {stat: {<m>_ensemble_mean, model_spread}}},
        overall_statistics : {cat: {metric: {<m>_ensemble_mean, model_spread}}},
        season_statistics  : {windows: [...]} | {n_models, diff: {...}},
        annual_summary     : {annual_rain_mm_*: spread, humid_focal, humid_baseline},
        metadata           : {...},
    }
"""
import sys
import os
import math
import json
import logging
import argparse
import statistics as pystat
from datetime import datetime
from typing import Dict, Any, Tuple, List, Optional

import pandas as pd

logging.disable(logging.INFO)

current_dir  = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.insert(0, project_root)

from climate_tookit.climate_statistics.statistics import analyze_climate_statistics
from climate_tookit.compare_periods.periods import (
    _annualize, _agg_seasons, _round,
    _diff_raw, _diff_block,
    PRECIP_ONLY,
)

NEX_GDDP_MODELS: List[str] = [
    "ACCESS-CM2", "ACCESS-ESM1-5", "CanESM5", "CMCC-ESM2", "EC-Earth3",
    "EC-Earth3-Veg-LR", "GFDL-ESM4", "INM-CM4-8", "INM-CM5-0", "KACE-1-0-G",
    "MIROC6", "MPI-ESM1-2-LR", "MRI-ESM2-0", "NorESM2-LM", "NorESM2-MM", "TaiESM1",
]
# Canonical scenarios accepted by analyze_climate_statistics for NEX-GDDP.
SSP_SCENARIOS: List[str] = ["ssp126", "ssp245", "ssp585", "historical"]
# Accept both the dotted/dashed CMIP6 labels and the lowercase canonical forms.
SCENARIO_ALIASES: Dict[str, str] = {
    "SSP1-2.6": "ssp126", "SSP2-4.5": "ssp245", "SSP5-8.5": "ssp585",
    "ssp126":   "ssp126", "ssp245":   "ssp245", "ssp585":   "ssp585",
    "historical": "historical",
}

def _normalize_scenario(s: str) -> Optional[str]:
    """Map any accepted alias to the canonical scenario string, else None."""
    return SCENARIO_ALIASES.get(s.strip()) if isinstance(s, str) else None

# helpers
def _is_num(x: Any) -> bool:
    return (isinstance(x, (int, float))
            and not isinstance(x, bool)
            and not (isinstance(x, float) and math.isnan(x)))

def _percentile(data: List[float], p: float) -> Optional[float]:
    if not data:
        return None
    s = sorted(data)
    idx = (p / 100) * (len(s) - 1)
    lo, hi = int(math.floor(idx)), int(math.ceil(idx))
    if lo == hi:
        return round(s[lo], 2)
    return round(s[lo] + (idx - lo) * (s[hi] - s[lo]), 2)

def _spread(values: List[float]) -> Dict[str, Any]:
    """Cross-model spread for one numeric vector."""
    clean = [float(v) for v in values if _is_num(v)]
    if not clean:
        return {"n": 0, "mean": None, "std": None, "min": None, "max": None,
                "p10": None, "p90": None}
    return {
        "n":    len(clean),
        "mean": round(pystat.mean(clean), 2),
        "std":  round(pystat.stdev(clean), 2) if len(clean) > 1 else 0.0,
        "min":  round(min(clean), 2),
        "max":  round(max(clean), 2),
        "p10":  _percentile(clean, 10),
        "p90":  _percentile(clean, 90),
    }

def _filter_models(models: Optional[List[str]],
                   exclude_models: Optional[List[str]]) -> List[str]:
    active = list(models) if models else list(NEX_GDDP_MODELS)
    if exclude_models:
        excl = {m.upper() for m in exclude_models}
        active = [m for m in active if m.upper() not in excl]
    return active

# Local replacement for periods._diff_annual: focal is now a period, not a year.
def _diff_annual_period(focal_ann:    Dict[str, Dict],
                        baseline_ann: Dict[str, Dict]) -> Dict[str, Any]:
    """
    Diff annual_summary as period-vs-period (rather than year-vs-period).
    Returns the same 'annual_rain_mm' shape periods._diff_annual produces ({focal, baseline_avg, diff, pct}) so the cross-model aggregator can stay
    unchanged. For humid status, single-year True/False doesn't apply, so we return raw counts of humid years and total years on each side; the
    aggregator sums them across models.
    """
    def _agg(ann_map: Dict[str, Dict]) -> Tuple[Optional[float], int, int]:
        rains = [v["annual_rain_mm"] for v in ann_map.values()
                 if v and _is_num(v.get("annual_rain_mm"))]
        humid = sum(1 for v in ann_map.values() if v and v.get("is_humid"))
        total = sum(1 for v in ann_map.values() if v)
        avg   = (sum(rains) / len(rains)) if rains else None
        return avg, humid, total

    f_avg, fhy, fht = _agg(focal_ann)
    b_avg, bhy, bht = _agg(baseline_ann)

    out: Dict[str, Any] = {}
    if _is_num(f_avg) and _is_num(b_avg):
        d = f_avg - b_avg
        p = (d / b_avg * 100.0) if b_avg else 0.0
        out["annual_rain_mm"] = {
            "focal":        round(f_avg, 1),
            "baseline_avg": round(b_avg, 1),
            "diff":         round(d, 1),
            "pct":          round(p, 2),
        }
    out["humid_status"] = {
        "focal_humid_count":    fhy,
        "focal_humid_total":    fht,
        "baseline_humid_count": bhy,
        "baseline_humid_total": bht,
    }
    return out

# per-model comparison (replicates periods.compare with focal as a period)
def _compare_one_model(
    location:       Tuple[float, float],
    baseline_start: int,
    baseline_end:   int,
    focal_start:    int,
    focal_end:      int,
    fixed_season:   Optional[str],
    model:          str,
    scenario:       str,
) -> Dict[str, Any]:
    """
    Same logic as periods.compare(), but pinned to source='nex_gddp', forwarding model+scenario to analyze_climate_statistics, and treating focal as a
    multi-year period. Both sides are annualised so overall totals are comparable on a per-year basis.
    """
    if baseline_end < baseline_start:
        return {"error": "baseline_end must be >= baseline_start"}
    if focal_end < focal_start:
        return {"error": "focal_end must be >= focal_start"}

    n_base    = baseline_end - baseline_start + 1
    n_focal   = focal_end    - focal_start    + 1
    drop_temp = "nex_gddp" in PRECIP_ONLY  # NEX-GDDP carries tas, so False
    fs_kw     = {"fixed_season": fixed_season} if fixed_season else {}

    base = analyze_climate_statistics(
        location_coord=location,
        start_year=baseline_start, end_year=baseline_end,
        source="nex_gddp",
        model=model, scenario=scenario,
        **fs_kw,
    )
    focal = analyze_climate_statistics(
        location_coord=location,
        start_year=focal_start, end_year=focal_end,
        source="nex_gddp",
        model=model, scenario=scenario,
        **fs_kw,
    )
    # 1. raw_climate_summary -- already period-wide means/min/max/std
    raw_diff = _diff_raw(focal.get("raw_climate_summary", []),
                         base.get("raw_climate_summary",  []),
                         drop_temp)
    # 2. overall_statistics -- annualise BOTH sides
    base_overall  = _annualize(_round(base.get("overall_statistics",  {}), 2), n_base)
    focal_overall = _annualize(_round(focal.get("overall_statistics", {}), 2), n_focal)
    overall_diff  = _diff_block(focal_overall, base_overall,
                                "focal_avg", "baseline_avg", drop_temp)
    # 3. season_statistics
    base_seasons  = _round(base.get("season_statistics",  []), 2)
    focal_seasons = _round(focal.get("season_statistics", []), 2)
    season_diff: Optional[Dict[str, Any]] = None
    if base_seasons or focal_seasons:
        if fixed_season:
            labels = [w.strip() for w in fixed_season.split(",")]
            base_grp:  Dict[int, List[Dict]] = {}
            focal_grp: Dict[int, List[Dict]] = {}
            for s in base_seasons:
                base_grp.setdefault(s.get("season_number", 1), []).append(s)
            for s in focal_seasons:
                focal_grp.setdefault(s.get("season_number", 1), []).append(s)
            windows = []
            for sn in sorted(set(base_grp) | set(focal_grp)):
                label = labels[sn - 1] if 0 < sn <= len(labels) else f"window_{sn}"
                fb = _agg_seasons(focal_grp.get(sn, []))
                bb = _agg_seasons(base_grp.get(sn, []))
                windows.append({
                    "window":        label,
                    "season_number": sn,
                    "n_baseline":    bb["_n"],
                    "n_focal":       fb["_n"],
                    "diff":          _diff_block(fb, bb, "focal_avg", "baseline_avg",
                                                 drop_temp),
                })
            season_diff = {"windows": windows}
        else:
            fb = _agg_seasons(focal_seasons)
            bb = _agg_seasons(base_seasons)
            season_diff = {
                "n_baseline": bb["_n"],
                "n_focal":    fb["_n"],
                "diff":       _diff_block(fb, bb, "focal_avg", "baseline_avg",
                                          drop_temp),
            }
    # 4. annual_summary -- focal is now a period
    annual_diff = _diff_annual_period(focal.get("annual_summary", {}),
                                      base.get("annual_summary",  {}))
    return {
        "focal_period":         f"{focal_start}-{focal_end}",
        "focal_years":          n_focal,
        "baseline_period":      f"{baseline_start}-{baseline_end}",
        "baseline_years":       n_base,
        "source":               "nex_gddp",
        "model":                model,
        "scenario":             scenario,
        "fixed_season":         fixed_season,
        "temperature_excluded": drop_temp,
        "raw_climate_summary":  raw_diff,
        "overall_statistics":   overall_diff,
        "season_statistics":    season_diff,
        "annual_summary":       annual_diff,
    }

# cross-model aggregation
def _aggregate_2level(per_model: List[Dict[str, Dict[str, Dict[str, Any]]]],
                      round_n: int = 2) -> Dict[str, Any]:
    """
    Pool {outer: {inner: {metric_name: number, ...}}} across models.

    Returns: {outer: {inner: {<metric>_ensemble_mean, model_spread}}}.
    Used for raw_climate_summary, overall_statistics, and each season window.
    """
    pool: Dict[str, Dict[str, Dict[str, List[float]]]] = {}
    for d in per_model:
        for outer, inner_dict in (d or {}).items():
            if not isinstance(inner_dict, dict):
                continue
            for inner, vals in inner_dict.items():
                if not isinstance(vals, dict):
                    continue
                slot = pool.setdefault(outer, {}).setdefault(inner, {})
                for k, v in vals.items():
                    if _is_num(v):
                        slot.setdefault(k, []).append(float(v))
    out: Dict[str, Any] = {}
    for outer, inner_dict in pool.items():
        out[outer] = {}
        for inner, vecs in inner_dict.items():
            entry: Dict[str, Any] = {
                f"{k}_ensemble_mean": round(pystat.mean(vs), round_n)
                for k, vs in vecs.items() if vs
            }
            entry["model_spread"] = {
                "diff": _spread(vecs.get("diff", [])),
                "pct":  _spread(vecs.get("pct", [])),
            }
            out[outer][inner] = entry
    return out

def _aggregate_seasons(per_model: List[Optional[Dict[str, Any]]]) -> Optional[Dict[str, Any]]:
    """Aggregate season_statistics across models (handles both lumped and windowed)."""
    samples = [s for s in per_model if isinstance(s, dict) and s]
    if not samples:
        return None

    if "windows" in samples[0]:
        win_pool: Dict[int, Dict[str, Any]] = {}
        for s in per_model:
            for w in (s or {}).get("windows", []) or []:
                sn = w.get("season_number", 1)
                bucket = win_pool.setdefault(sn, {"label": w.get("window"), "diffs": []})
                bucket["diffs"].append(w.get("diff", {}))
        windows = []
        for sn in sorted(win_pool):
            windows.append({
                "window":        win_pool[sn]["label"],
                "season_number": sn,
                "n_models":      len(win_pool[sn]["diffs"]),
                "diff":          _aggregate_2level(win_pool[sn]["diffs"]),
            })
        return {"windows": windows}

    diffs = [(s or {}).get("diff", {}) for s in per_model if s]
    return {"n_models": len(diffs), "diff": _aggregate_2level(diffs)}

def _aggregate_annual(per_model: List[Optional[Dict[str, Any]]]) -> Dict[str, Any]:
    """Aggregate annual_summary across models (period focal version)."""
    rains_focal:    List[float] = []
    rains_baseline: List[float] = []
    rains_diff:     List[float] = []
    rains_pct:      List[float] = []
    fhy = fht = bhy = bht = 0  
    for ann in per_model:
        ann = ann or {}
        arm = ann.get("annual_rain_mm") or {}
        for vec, key in [(rains_focal,    "focal"),
                         (rains_baseline, "baseline_avg"),
                         (rains_diff,     "diff"),
                         (rains_pct,      "pct")]:
            v = arm.get(key)
            if _is_num(v):
                vec.append(float(v))
        hs = ann.get("humid_status") or {}
        if _is_num(hs.get("focal_humid_count")):    fhy += int(hs["focal_humid_count"])
        if _is_num(hs.get("focal_humid_total")):    fht += int(hs["focal_humid_total"])
        if _is_num(hs.get("baseline_humid_count")): bhy += int(hs["baseline_humid_count"])
        if _is_num(hs.get("baseline_humid_total")): bht += int(hs["baseline_humid_total"])

    out: Dict[str, Any] = {}
    if rains_focal:    out["annual_rain_mm_focal"]    = _spread(rains_focal)
    if rains_baseline: out["annual_rain_mm_baseline"] = _spread(rains_baseline)
    if rains_diff:     out["annual_rain_mm_diff"]     = _spread(rains_diff)
    if rains_pct:      out["annual_rain_mm_pct"]      = _spread(rains_pct)
    out["humid_focal"]    = (f"{fhy}/{fht} ({fhy / fht * 100:.1f}%)"
                             if fht else "n/a")
    out["humid_baseline"] = (f"{bhy}/{bht} ({bhy / bht * 100:.1f}%)"
                             if bht else "n/a")
    return out

# main API
def ensemble_compare(
    location:       Tuple[float, float],
    baseline_start: int,
    baseline_end:   int,
    focal_start:    int,
    focal_end:      int,
    scenario:       str = "ssp245",
    fixed_season:   Optional[str] = None,
    models:         Optional[List[str]] = None,
    exclude_models: Optional[List[str]] = None,
    verbose:        bool = True,
) -> Dict[str, Any]:
    """
    Run the focal-period-vs-baseline-period comparison once per NEX-GDDP model, then average across models.
    All data (baseline + focal) comes from NEX-GDDP, so each model is compared against its own historical run. Returns one ensemble-shaped result.
    """
    if baseline_end < baseline_start:
        return {"error": "baseline_end must be >= baseline_start"}
    if focal_end < focal_start:
        return {"error": "focal_end must be >= focal_start"}

    canon = _normalize_scenario(scenario)
    if not canon:
        return {"error": (f"scenario '{scenario}' not recognised. "
                          f"Accepted: {sorted(SCENARIO_ALIASES)}")}
    scenario = canon

    active = _filter_models(models, exclude_models)
    if not active:
        return {"error": "No models selected after filtering."}

    if verbose:
        print(f"\n{'=' * 60}")
        print("NEX-GDDP Ensemble Comparison")
        print(f"  Location  : {location[0]}, {location[1]}")
        print(f"  Baseline  : {baseline_start}-{baseline_end}")
        print(f"  Focal     : {focal_start}-{focal_end}")
        print(f"  Scenario  : {scenario}")
        if fixed_season:
            print(f"  Seasons   : {fixed_season}")
        print(f"  Models    : {len(active)}")
        print(f"{'=' * 60}")

    per_model: List[Dict[str, Any]] = []
    failed:    List[Dict[str, str]] = []

    for i, model in enumerate(active, 1):
        if verbose:
            print(f"\n  [{i:02d}/{len(active):02d}] {model}", flush=True)
        try:
            r = _compare_one_model(
                location=location,
                baseline_start=baseline_start,
                baseline_end=baseline_end,
                focal_start=focal_start,
                focal_end=focal_end,
                fixed_season=fixed_season,
                model=model,
                scenario=scenario,
            )
            if "error" in r:
                if verbose: print(f"    x  {r['error']}")
                failed.append({"model": model, "error": r["error"]})
                continue
            r["_model"] = model
            per_model.append(r)
            if verbose: print("    ok")
        except Exception as exc:
            if verbose: print(f"    x  {exc}")
            failed.append({"model": model, "error": str(exc)})

    if not per_model:
        return {"error": "All models failed.", "failed_models": failed}

    return {
        "focal_period":    f"{focal_start}-{focal_end}",
        "focal_years":     focal_end - focal_start + 1,
        "baseline_period": f"{baseline_start}-{baseline_end}",
        "baseline_years":  baseline_end - baseline_start + 1,
        "scenario":        scenario,
        "fixed_season":    fixed_season,
        "models_used":     [r["_model"] for r in per_model],
        "models_failed":   failed,
        "n_models_ok":     len(per_model),
        "raw_climate_summary": _aggregate_2level(
            [r.get("raw_climate_summary", {}) for r in per_model], round_n=3),
        "overall_statistics":  _aggregate_2level(
            [r.get("overall_statistics", {}) for r in per_model]),
        "season_statistics":   _aggregate_seasons(
            [r.get("season_statistics") for r in per_model]),
        "annual_summary":      _aggregate_annual(
            [r.get("annual_summary") for r in per_model]),
        "metadata": {
            "location":      {"lat": location[0], "lon": location[1]},
            "source":        "NEX-GDDP-CMIP6",
            "analysis_date": datetime.now().isoformat(),
        },
    }

# printing
def _print_2level(agg: Dict[str, Any],
                  outer_label: str = "Category",
                  inner_label: str = "Metric",
                  precision:   int = 2) -> None:
    """
    Print ensemble means in the same table shape periods.py prints, with columns matching the underlying diff keys (focal_avg/baseline_avg/diff/pct
    -> focal_avg/baseline_avg/Δ/Δ%). Model spread is kept in the JSON but suppressed here so the table mirrors periods.py.
    """
    if not agg:
        print("  (no comparable metrics)")
        return
    rows = []
    for outer, inner_dict in agg.items():
        for inner, vals in inner_dict.items():
            row = {outer_label: outer, inner_label: inner}
            for k, v in vals.items():
                if k == "model_spread" or not _is_num(v):
                    continue
                short = k.replace("_ensemble_mean", "")
                if short == "diff":
                    row["Δ"]  = f"{v:+.{precision}f}"
                elif short == "pct":
                    row["Δ%"] = f"{v:+.2f}%"
                else:
                    row[short] = f"{v:.{precision}f}"
            rows.append(row)
    print(pd.DataFrame(rows).to_string(index=False))

def print_report(r: Dict[str, Any]) -> None:
    if "error" in r:
        print(f"\nError: {r['error']}")
        for f in r.get("failed_models", []):
            print(f"  - {f['model']}: {f['error']}")
        return

    n_total = r["n_models_ok"] + len(r["models_failed"])
    print(f"\n{'=' * 60}")
    print(f"ENSEMBLE: focal {r['focal_period']} vs baseline {r['baseline_period']}")
    print(f"{'=' * 60}")
    print(f"  Scenario : {r['scenario']}")
    print(f"  Models ok: {r['n_models_ok']}/{n_total}")
    if r["models_failed"]:
        print(f"  Failed   : {', '.join(f['model'] for f in r['models_failed'])}")

    print(f"\n--- 1. RAW CLIMATE SUMMARY (ensemble) ---")
    _print_2level(r.get("raw_climate_summary", {}),
                  outer_label="Variable", inner_label="Stat", precision=3)

    print(f"\n--- 2. OVERALL STATISTICS (ensemble, both periods annualised) ---")
    _print_2level(r.get("overall_statistics", {}))

    season = r.get("season_statistics")
    if season:
        print(f"\n--- 3. SEASON STATISTICS (ensemble) ---")
        if "windows" in season:
            for w in season["windows"]:
                print(f"\n  Window {w['window']} (season #{w['season_number']}, n_models={w['n_models']})")
                _print_2level(w["diff"])
        else:
            print(f"  (n_models={season['n_models']})")
            _print_2level(season["diff"])

    print(f"\n--- 4. ANNUAL SUMMARY (ensemble) ---")
    ann = r.get("annual_summary", {})
    foc = ann.get("annual_rain_mm_focal")    or {}
    bas = ann.get("annual_rain_mm_baseline") or {}
    dif = ann.get("annual_rain_mm_diff")     or {}
    pct = ann.get("annual_rain_mm_pct")      or {}
    if _is_num(foc.get("mean")):
        parts = [f"focal_avg={foc['mean']:.1f} mm"]
        if _is_num(bas.get("mean")):
            parts.append(f"baseline_avg={bas['mean']:.1f} mm")
        if _is_num(dif.get("mean")):
            tail = (f" ({pct['mean']:+.2f}%)"
                    if _is_num(pct.get("mean")) else "")
            parts.append(f"Δ={dif['mean']:+.1f}{tail}")
        print(f"  Annual rainfall  : {' | '.join(parts)}")
    print(f"  Humid (focal)    : {ann.get('humid_focal', 'n/a')}")
    print(f"  Humid (baseline) : {ann.get('humid_baseline', 'n/a')}")
    print()

# CLI
def main() -> None:
    if "--list-models" in sys.argv:
        print("Available NEX-GDDP-CMIP6 models:")
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
        description="Ensemble focal-period-vs-baseline-period comparison across NEX-GDDP models.",
        formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--location",       required=True, help="lat,lon (e.g. -1.286,36.817)")
    p.add_argument("--baseline-start", type=int, required=True,
                   help="First year of the baseline period (inclusive)")
    p.add_argument("--baseline-end",   type=int, required=True,
                   help="Last year of the baseline period (inclusive)")
    p.add_argument("--focal-start",    type=int, required=True,
                   help="First year of the focal period (inclusive)")
    p.add_argument("--focal-end",      type=int, required=True,
                   help="Last year of the focal period (inclusive). "
                        "For a single year, set --focal-start == --focal-end.")
    p.add_argument("--scenarios",      default="ssp245",
                   metavar="ssp245[,ssp585]",
                   help=("Comma-separated. Canonical: "
                         f"{', '.join(SSP_SCENARIOS)}.\n"
                         "Aliases also accepted: SSP1-2.6, SSP2-4.5, SSP5-8.5."))
    p.add_argument("--fixed-season",   default=None,
                   metavar="MM-DD:MM-DD[,MM-DD:MM-DD]",
                   help=("Optional. Same syntax as periods.py.\n"
                         "  Single        : '03-01:05-31'\n"
                         "  Two seasons   : '03-01:05-31,10-01:12-15'\n"
                         "  Year-crossing : '11-01:02-28'"))
    p.add_argument("--models",         help="Comma-separated subset of models")
    p.add_argument("--exclude-models", help="Comma-separated models to exclude")
    p.add_argument("--list-models",    action="store_true")
    p.add_argument("--output",         default=None, help="Save JSON result to this path")
    p.add_argument("--quiet",          action="store_true")
    args = p.parse_args()

    try:
        lat, lon = (float(x) for x in args.location.replace(" ", ",").split(","))
    except ValueError:
        print("Error: --location must be 'lat,lon'"); sys.exit(1)

    models  = [m.strip() for m in args.models.split(",")]         if args.models         else None
    exclude = [m.strip() for m in args.exclude_models.split(",")] if args.exclude_models else None

    raw_scenarios = [s.strip() for s in args.scenarios.split(",") if s.strip()]
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
              f"Accepted: {sorted(SCENARIO_ALIASES)}"); sys.exit(1)
    if not scenarios:
        print("Error: no scenarios provided."); sys.exit(1)

    all_results: Dict[str, Any] = {}
    any_ok = False
    for scenario in scenarios:
        result = ensemble_compare(
            location=(lat, lon),
            baseline_start=args.baseline_start,
            baseline_end=args.baseline_end,
            focal_start=args.focal_start,
            focal_end=args.focal_end,
            scenario=scenario,
            fixed_season=args.fixed_season,
            models=models,
            exclude_models=exclude,
            verbose=not args.quiet,
        )
        all_results[scenario] = result
        print_report(result)
        if "error" not in result:
            any_ok = True

    if not any_ok:
        sys.exit(1)

    if args.output:
        os.makedirs(os.path.dirname(os.path.abspath(args.output)) or ".", exist_ok=True)
        # Single scenario -> bare result; multiple -> {scenario: result} map
        payload = all_results[scenarios[0]] if len(scenarios) == 1 else all_results
        with open(args.output, "w") as f:
            json.dump(payload, f, indent=2, default=str)
        print(f"✓ Saved: {args.output}")

if __name__ == "__main__":
    main()

# NOTE: the 1st command in a section runs all 16 models, the 2nd allows model selection.

# List available NEX-GDDP models and scenarios:
# python -m climate_tookit.compare_periods.ensemble_periods --list-models

# Auto-detected season (no --fixed-season) -- all models, pick scenarios:
# python -m climate_tookit.compare_periods.ensemble_periods --location="-1.286,36.817" --baseline-start 1991 --baseline-end 2020 --focal-start 2040 --focal-end 2060 --scenarios ssp245,ssp585 --output ensemble_auto_all.json
# python -m climate_tookit.compare_periods.ensemble_periods --location="-1.286,36.817" --baseline-start 1991 --baseline-end 2020 --focal-start 2040 --focal-end 2060 --models "ACCESS-CM2,EC-Earth3,MRI-ESM2-0" --scenarios ssp245,ssp585 --output ensemble_auto.json

# Fixed single season:
# python -m climate_tookit.compare_periods.ensemble_periods --location="-1.286,36.817" --baseline-start 1991 --baseline-end 2020 --focal-start 2040 --focal-end 2060 --fixed-season "03-01:05-31" --scenarios ssp245,ssp585 --output ensemble_mam_all.json
# python -m climate_tookit.compare_periods.ensemble_periods --location="-1.286,36.817" --baseline-start 1991 --baseline-end 2020 --focal-start 2040 --focal-end 2060 --fixed-season "03-01:05-31" --models "ACCESS-CM2,EC-Earth3,MRI-ESM2-0" --scenarios ssp585 --output ensemble_mam.json

# Fixed two seasons:
# python -m climate_tookit.compare_periods.ensemble_periods --location="-1.286,36.817" --baseline-start 1991 --baseline-end 2020 --focal-start 2040 --focal-end 2060 --fixed-season "03-01:05-31,10-01:12-15" --scenarios ssp245,ssp585 --output ensemble_mam_ond_all.json
# python -m climate_tookit.compare_periods.ensemble_periods --location="-1.286,36.817" --baseline-start 1991 --baseline-end 2020 --focal-start 2040 --focal-end 2060 --fixed-season "03-01:05-31,10-01:12-15" --models "ACCESS-CM2,EC-Earth3,MRI-ESM2-0" --scenarios ssp245,ssp585 --output ensemble_mam_ond.json

# Fixed year-crossing season:
# python -m climate_tookit.compare_periods.ensemble_periods --location="-1.286,36.817" --baseline-start 1991 --baseline-end 2020 --focal-start 2040 --focal-end 2060 --fixed-season "11-01:02-28" --scenarios ssp245,ssp585 --output ensemble_njf_all.json
# python -m climate_tookit.compare_periods.ensemble_periods --location="-1.286,36.817" --baseline-start 1991 --baseline-end 2020 --focal-start 2040 --focal-end 2060 --fixed-season "11-01:02-28" --models "ACCESS-CM2,EC-Earth3,MRI-ESM2-0" --scenarios ssp585 --output ensemble_njf.json