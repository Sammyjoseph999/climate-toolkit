"""
NEX-GDDP Ensemble Period Comparison
Runs the same future-vs-baseline comparison shape as periods.compare(), once per NEX-GDDP CMIP6 model, then averages the per-model results into a single
ensemble comparison.
Difference from periods.compare(): future is a multi-year *period* (e.g. 2040-2060) rather than a single year. Both sides are annualised before the
diff, so 'future_avg' and 'baseline_avg' are per-year averages over their respective periods.

Both baseline and future data come from NEX-GDDP, so each model is compared against its own historical run (the standard model-bias-removing convention).

Output mirrors periods.compare()'s four-section shape, with each leaf replaced by ensemble means + model spread.
    
The ensemble result IS the architecture's "Baseline LTM vs Future LTM" comparison
(Δ = future_avg − baseline_avg = future − baseline). 
When --focal-year/--focal-source are supplied, the observed year is diffed against both ensemble LTMs, completing the
three season-summary comparisons (period concepts: historical=baseline LTM, future=projected LTM, focal=observed single year):
    baseline LTM vs future LTM  -> the ensemble result  (Baseline LTM vs Future LTM)
    focal vs baseline LTM        -> focal_vs_baseline    (Δ = focal − baseline_ltm)
    focal vs future LTM          -> focal_vs_future      (Δ = focal − future_ltm)
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
    PRECIP_ONLY, SUPPORTED,
)

NEX_GDDP_MODELS: List[str] = [
    "ACCESS-CM2", "ACCESS-ESM1-5", "CanESM5", "CMCC-ESM2", "EC-Earth3",
    "EC-Earth3-Veg-LR", "GFDL-ESM4", "INM-CM4-8", "INM-CM5-0", "KACE-1-0-G",
    "MIROC6", "MPI-ESM1-2-LR", "MRI-ESM2-0", "NorESM2-LM", "NorESM2-MM", "TaiESM1",
]
SSP_SCENARIOS: List[str] = ["ssp126", "ssp245", "ssp585", "historical"]
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

# Local replacement for periods._diff_annual: future is now a period, not a year.
def _diff_annual_period(future_ann:    Dict[str, Dict],
                        baseline_ann: Dict[str, Dict]) -> Dict[str, Any]:
    """
    Diff annual_summary as period-vs-period (rather than year-vs-period).
    Returns the same 'annual_rain_mm' shape periods._diff_annual produces ({future, baseline_avg, diff, pct}) so the cross-model aggregator can stay
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

    f_avg, fhy, fht = _agg(future_ann)
    b_avg, bhy, bht = _agg(baseline_ann)

    out: Dict[str, Any] = {}
    if _is_num(f_avg) and _is_num(b_avg):
        d = f_avg - b_avg
        p = (d / b_avg * 100.0) if b_avg else 0.0
        out["annual_rain_mm"] = {
            "future":        round(f_avg, 1),
            "baseline_avg": round(b_avg, 1),
            "diff":         round(d, 1),
            "pct":          round(p, 2),
        }
    out["humid_status"] = {
        "future_humid_count":    fhy,
        "future_humid_total":    fht,
        "baseline_humid_count": bhy,
        "baseline_humid_total": bht,
    }
    return out

# The architecture asks for three season-summary comparisons:
#   Baseline LTM vs Future LTM, Focal vs Baseline LTM, Focal vs Future LTM
def _diff_value_2level(a: Dict[str, Dict[str, Any]],
                       b: Dict[str, Dict[str, Any]],
                       a_lbl: str, b_lbl: str,
                       round_n: int = 2) -> Dict[str, Any]:
    """Diff two {outer: {inner: number}} blocks into {outer: {inner: {a_lbl, b_lbl, diff, pct}}}."""
    out: Dict[str, Any] = {}
    for outer, a_inner in (a or {}).items():
        b_inner = (b or {}).get(outer)
        if not (isinstance(a_inner, dict) and isinstance(b_inner, dict)):
            continue
        block: Dict[str, Any] = {}
        for inner, av in a_inner.items():
            bv = b_inner.get(inner)
            if not (_is_num(av) and _is_num(bv)):
                continue
            d = av - bv
            p = (d / bv * 100.0) if bv != 0 else 0.0
            block[inner] = {a_lbl: round(av, round_n), b_lbl: round(bv, round_n),
                            "diff": round(d, round_n), "pct": round(p, 2)}
        if block:
            out[outer] = block
    return out

def _future_ltm_from_agg(agg: Dict[str, Any],
                         mean_key: str = "future_avg_ensemble_mean") -> Dict[str, Dict[str, float]]:
    """Pull the future-LTM ensemble means out of an aggregated 2-level block."""
    out: Dict[str, Dict[str, float]] = {}
    for outer, inner_dict in (agg or {}).items():
        if not isinstance(inner_dict, dict):
            continue
        block: Dict[str, float] = {}
        for inner, vals in inner_dict.items():
            if isinstance(vals, dict) and _is_num(vals.get(mean_key)):
                block[inner] = float(vals[mean_key])
        if block:
            out[outer] = block
    return out

def _build_focal_summary(location:     Tuple[float, float],
                          focal_year:  int,
                          focal_source: str,
                          fixed_season: Optional[str]) -> Dict[str, Any]:
    """Fetch one observed year and reduce it to comparable season-summary values."""
    fs_kw = {"fixed_season": fixed_season} if fixed_season else {}
    stats = analyze_climate_statistics(
        location_coord=location,
        start_year=focal_year, end_year=focal_year,
        source=focal_source, **fs_kw,
    )

    overall = _round(stats.get("overall_statistics", {}), 2)

    seasons = _round(stats.get("season_statistics", []), 2)
    if fixed_season:
        grp: Dict[int, List[Dict]] = {}
        for s in seasons:
            grp.setdefault(s.get("season_number", 1), []).append(s)
        windows = []
        for sn in sorted(grp):
            agg = _agg_seasons(grp[sn])
            block = {c: agg[c] for c in ("precipitation", "temperature", "water_balance")
                     if isinstance(agg.get(c), dict)}
            windows.append({"season_number": sn, "block": block})
        season_summary: Dict[str, Any] = {"windows": windows}
    else:
        agg = _agg_seasons(seasons)
        block = {c: agg[c] for c in ("precipitation", "temperature", "water_balance")
                 if isinstance(agg.get(c), dict)}
        season_summary = {"block": block}

    ann = (stats.get("annual_summary", {}) or {}).get(str(focal_year), {}) or {}
    return {
        "focal_year":  focal_year,
        "source":       focal_source,
        "overall":      overall,
        "seasons":      season_summary,
        "annual_rain":  ann.get("annual_rain_mm"),
        "is_humid":     ann.get("is_humid"),
        "humid_test":   ann.get("humid_test"),
    }

def _season_block(seasons: List[Dict]) -> Dict[str, Any]:
    """Reduce a list of season rows to {cat: {metric: number}} for the comparable cats."""
    agg = _agg_seasons(seasons)
    return {c: agg[c] for c in ("precipitation", "temperature", "water_balance")
            if isinstance(agg.get(c), dict)}

def _mean_2level(maps: List[Dict[str, Dict[str, Any]]], round_n: int = 2) -> Dict[str, Any]:
    """Mean a list of {outer: {inner: number}} maps into {outer: {inner: mean}}."""
    pool: Dict[str, Dict[str, List[float]]] = {}
    for m in maps:
        for outer, inner in (m or {}).items():
            if not isinstance(inner, dict):
                continue
            for k, v in inner.items():
                if _is_num(v):
                    pool.setdefault(outer, {}).setdefault(k, []).append(float(v))
    return {o: {k: round(sum(vs) / len(vs), round_n) for k, vs in inner.items() if vs}
            for o, inner in pool.items()}

def _build_focal_summary_nexgddp(location:     Tuple[float, float],
                                 focal_year:   int,
                                 fixed_season: Optional[str],
                                 scenario:     str,
                                 models:         Optional[List[str]] = None,
                                 exclude_models: Optional[List[str]] = None,
                                 verbose:        bool = True) -> Optional[Dict[str, Any]]:
    """
    Build a single-year focal summary from the NEX-GDDP ensemble itself (mean across models), so the focal/baseline/future comparison is entirely NEX-GDDP-sourced.
    Scenario-dependent (the focal year inherits the same scenario as the LTMs).
    """
    canon  = _normalize_scenario(scenario) or scenario
    active = _filter_models(models, exclude_models)
    fs_kw  = {"fixed_season": fixed_season} if fixed_season else {}

    overalls:   List[Dict[str, Any]] = []
    win_blocks: Dict[int, List[Dict[str, Any]]] = {}
    lump_blocks: List[Dict[str, Any]] = []
    rains:      List[float] = []
    humid_count = humid_total = 0

    for model in active:
        try:
            stats = analyze_climate_statistics(
                location_coord=location,
                start_year=focal_year, end_year=focal_year,
                source="nex_gddp", model=model, scenario=canon, **fs_kw,
            )
        except Exception as exc:
            if verbose:
                print(f"    x  focal {model}: {exc}")
            continue

        overalls.append(_round(stats.get("overall_statistics", {}), 2))
        seasons = _round(stats.get("season_statistics", []), 2)
        if fixed_season:
            grp: Dict[int, List[Dict]] = {}
            for s in seasons:
                grp.setdefault(s.get("season_number", 1), []).append(s)
            for sn, rows in grp.items():
                win_blocks.setdefault(sn, []).append(_season_block(rows))
        else:
            lump_blocks.append(_season_block(seasons))

        ann = (stats.get("annual_summary", {}) or {}).get(str(focal_year), {}) or {}
        if _is_num(ann.get("annual_rain_mm")):
            rains.append(float(ann["annual_rain_mm"]))
        if ann.get("is_humid") is not None:
            humid_total += 1
            if ann.get("is_humid"):
                humid_count += 1

    if not overalls:
        return None

    if fixed_season:
        season_summary: Dict[str, Any] = {
            "windows": [{"season_number": sn, "block": _mean_2level(win_blocks[sn])}
                        for sn in sorted(win_blocks)]
        }
    else:
        season_summary = {"block": _mean_2level(lump_blocks)}

    return {
        "focal_year":  focal_year,
        "source":      f"nex_gddp ensemble ({len(overalls)} models, {canon})",
        "overall":     _mean_2level(overalls),
        "seasons":     season_summary,
        "annual_rain": round(sum(rains) / len(rains), 1) if rains else None,
        "is_humid":    (humid_count > humid_total / 2) if humid_total else None,
        "humid_test":  f"{humid_count}/{humid_total} models humid" if humid_total else None,
    }

def _diff_focal_vs_ltm(focal:   Dict[str, Any],
                        ensemble: Dict[str, Any],
                        mean_key: str,
                        ltm_label: str,
                        annual_rain_key: str,
                        humid_key: str) -> Dict[str, Any]:
    """
    Diff the observed focal year against one set of ensemble LTM means (focal - ltm). mean_key selects which LTM is pulled from the aggregated blocks:
        'future_avg_ensemble_mean'   -> Future LTM   (focal vs future)
        'baseline_avg_ensemble_mean' -> Baseline LTM (focal vs historical)
    """
    a_lbl, b_lbl = "focal", ltm_label

    ltm_overall  = _future_ltm_from_agg(ensemble.get("overall_statistics", {}), mean_key)
    overall_diff = _diff_value_2level(focal.get("overall", {}), ltm_overall, a_lbl, b_lbl)

    ens_season = ensemble.get("season_statistics") or {}
    act_season = focal.get("seasons") or {}
    season_diff: Optional[Dict[str, Any]] = None
    if "windows" in ens_season:
        act_by_sn = {w["season_number"]: w["block"]
                     for w in act_season.get("windows", [])}
        windows = []
        for w in ens_season.get("windows", []):
            sn         = w.get("season_number", 1)
            ltm_blk    = _future_ltm_from_agg(w.get("diff", {}), mean_key)
            focal_blk = act_by_sn.get(sn, {})
            windows.append({
                "window":        w.get("window"),
                "season_number": sn,
                "diff":          _diff_value_2level(focal_blk, ltm_blk, a_lbl, b_lbl),
            })
        season_diff = {"windows": windows}
    elif ens_season.get("diff"):
        ltm_blk = _future_ltm_from_agg(ens_season["diff"], mean_key)
        season_diff = {"diff": _diff_value_2level(act_season.get("block", {}),
                                                  ltm_blk, a_lbl, b_lbl)}

    annual: Dict[str, Any] = {}
    ra = (ensemble.get("annual_summary", {}) or {}).get(annual_rain_key, {}) or {}
    ltm_rain    = ra.get("mean")
    focal_rain = focal.get("annual_rain")
    if _is_num(focal_rain) and _is_num(ltm_rain):
        d = focal_rain - ltm_rain
        p = (d / ltm_rain * 100.0) if ltm_rain else 0.0
        annual["annual_rain_mm"] = {
            a_lbl: round(float(focal_rain), 1), b_lbl: round(float(ltm_rain), 1),
            "diff": round(d, 1), "pct": round(p, 2),
        }
    annual["humid_status"] = {
        "focal_is_humid":   focal.get("is_humid"),
        "focal_humid_test": focal.get("humid_test"),
        "ltm_humid":         ensemble.get("annual_summary", {}).get(humid_key, "n/a"),
    }

    return {
        "focal_year":        focal["focal_year"],
        "focal_source":      focal["source"],
        "ltm_label":          ltm_label,
        "overall_statistics": overall_diff,
        "season_statistics":  season_diff,
        "annual_summary":     annual,
    }

def _diff_focal_vs_future(focal: Dict[str, Any],
                           ensemble: Dict[str, Any]) -> Dict[str, Any]:
    """Focal observed year vs Future LTM (Δ = focal - future_ltm)."""
    return _diff_focal_vs_ltm(focal, ensemble,
                               mean_key="future_avg_ensemble_mean",
                               ltm_label="future_ltm",
                               annual_rain_key="annual_rain_mm_future",
                               humid_key="humid_future")

def _diff_focal_vs_baseline(focal: Dict[str, Any],
                             ensemble: Dict[str, Any]) -> Dict[str, Any]:
    """Focal observed year vs Baseline (historical) LTM (Δ = focal - baseline_ltm)."""
    return _diff_focal_vs_ltm(focal, ensemble,
                               mean_key="baseline_avg_ensemble_mean",
                               ltm_label="baseline_ltm",
                               annual_rain_key="annual_rain_mm_baseline",
                               humid_key="humid_baseline")

# per-model comparison (replicates periods.compare with future as a period)
def _compare_one_model(
    location:       Tuple[float, float],
    baseline_start: int,
    baseline_end:   int,
    future_start:    int,
    future_end:      int,
    fixed_season:   Optional[str],
    model:          str,
    scenario:       str,
) -> Dict[str, Any]:
    """
    Same logic as periods.compare(), but pinned to source='nex_gddp', forwarding model+scenario to analyze_climate_statistics, and treating future as a
    multi-year period. Both sides are annualised so overall totals are comparable on a per-year basis.
    """
    if baseline_end < baseline_start:
        return {"error": "baseline_end must be >= baseline_start"}
    if future_end < future_start:
        return {"error": "future_end must be >= future_start"}

    n_base    = baseline_end - baseline_start + 1
    n_future   = future_end    - future_start    + 1
    drop_temp = "nex_gddp" in PRECIP_ONLY  # NEX-GDDP carries tas, so False
    fs_kw     = {"fixed_season": fixed_season} if fixed_season else {}

    base = analyze_climate_statistics(
        location_coord=location,
        start_year=baseline_start, end_year=baseline_end,
        source="nex_gddp",
        model=model, scenario=scenario,
        **fs_kw,
    )
    future = analyze_climate_statistics(
        location_coord=location,
        start_year=future_start, end_year=future_end,
        source="nex_gddp",
        model=model, scenario=scenario,
        **fs_kw,
    )
    # 1. raw_climate_summary -- already period-wide means/min/max/std
    raw_diff = _diff_raw(future.get("raw_climate_summary", []),
                         base.get("raw_climate_summary",  []),
                         drop_temp)
    # 2. overall_statistics -- annualise BOTH sides
    base_overall  = _annualize(_round(base.get("overall_statistics",  {}), 2), n_base)
    future_overall = _annualize(_round(future.get("overall_statistics", {}), 2), n_future)
    overall_diff  = _diff_block(future_overall, base_overall,
                                "future_avg", "baseline_avg", drop_temp)
    # 3. season_statistics
    base_seasons  = _round(base.get("season_statistics",  []), 2)
    future_seasons = _round(future.get("season_statistics", []), 2)
    season_diff: Optional[Dict[str, Any]] = None
    if base_seasons or future_seasons:
        if fixed_season:
            labels = [w.strip() for w in fixed_season.split(",")]
            base_grp:  Dict[int, List[Dict]] = {}
            future_grp: Dict[int, List[Dict]] = {}
            for s in base_seasons:
                base_grp.setdefault(s.get("season_number", 1), []).append(s)
            for s in future_seasons:
                future_grp.setdefault(s.get("season_number", 1), []).append(s)
            windows = []
            for sn in sorted(set(base_grp) | set(future_grp)):
                label = labels[sn - 1] if 0 < sn <= len(labels) else f"window_{sn}"
                fb = _agg_seasons(future_grp.get(sn, []))
                bb = _agg_seasons(base_grp.get(sn, []))
                windows.append({
                    "window":        label,
                    "season_number": sn,
                    "n_baseline":    bb["_n"],
                    "n_future":       fb["_n"],
                    "diff":          _diff_block(fb, bb, "future_avg", "baseline_avg",
                                                 drop_temp),
                })
            season_diff = {"windows": windows}
        else:
            fb = _agg_seasons(future_seasons)
            bb = _agg_seasons(base_seasons)
            season_diff = {
                "n_baseline": bb["_n"],
                "n_future":    fb["_n"],
                "diff":       _diff_block(fb, bb, "future_avg", "baseline_avg",
                                          drop_temp),
            }
    # 4. annual_summary -- future is now a period
    annual_diff = _diff_annual_period(future.get("annual_summary", {}),
                                      base.get("annual_summary",  {}))
    return {
        "future_period":         f"{future_start}-{future_end}",
        "future_years":          n_future,
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
    """Aggregate annual_summary across models (period future version)."""
    rains_future:    List[float] = []
    rains_baseline: List[float] = []
    rains_diff:     List[float] = []
    rains_pct:      List[float] = []
    fhy = fht = bhy = bht = 0  
    for ann in per_model:
        ann = ann or {}
        arm = ann.get("annual_rain_mm") or {}
        for vec, key in [(rains_future,    "future"),
                         (rains_baseline, "baseline_avg"),
                         (rains_diff,     "diff"),
                         (rains_pct,      "pct")]:
            v = arm.get(key)
            if _is_num(v):
                vec.append(float(v))
        hs = ann.get("humid_status") or {}
        if _is_num(hs.get("future_humid_count")):    fhy += int(hs["future_humid_count"])
        if _is_num(hs.get("future_humid_total")):    fht += int(hs["future_humid_total"])
        if _is_num(hs.get("baseline_humid_count")): bhy += int(hs["baseline_humid_count"])
        if _is_num(hs.get("baseline_humid_total")): bht += int(hs["baseline_humid_total"])

    out: Dict[str, Any] = {}
    if rains_future:    out["annual_rain_mm_future"]    = _spread(rains_future)
    if rains_baseline: out["annual_rain_mm_baseline"] = _spread(rains_baseline)
    if rains_diff:     out["annual_rain_mm_diff"]     = _spread(rains_diff)
    if rains_pct:      out["annual_rain_mm_pct"]      = _spread(rains_pct)
    out["humid_future"]    = (f"{fhy}/{fht} ({fhy / fht * 100:.1f}%)"
                             if fht else "n/a")
    out["humid_baseline"] = (f"{bhy}/{bht} ({bhy / bht * 100:.1f}%)"
                             if bht else "n/a")
    return out

# main API
def ensemble_compare(
    location:       Tuple[float, float],
    baseline_start: int,
    baseline_end:   int,
    future_start:    int,
    future_end:      int,
    scenario:       str = "ssp245",
    fixed_season:   Optional[str] = None,
    models:         Optional[List[str]] = None,
    exclude_models: Optional[List[str]] = None,
    focal_summary: Optional[Dict[str, Any]] = None,
    verbose:        bool = True,
) -> Dict[str, Any]:
    """
    Run the future-period-vs-baseline-period comparison once per NEX-GDDP model, then average across models.
    All data (baseline + future) comes from NEX-GDDP, so each model is compared against its own historical run. Returns one ensemble-shaped result.
    """
    if baseline_end < baseline_start:
        return {"error": "baseline_end must be >= baseline_start"}
    if future_end < future_start:
        return {"error": "future_end must be >= future_start"}

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
        print(f"  Future     : {future_start}-{future_end}")
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
                future_start=future_start,
                future_end=future_end,
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

    result = {
        "future_period":    f"{future_start}-{future_end}",
        "future_years":     future_end - future_start + 1,
        "baseline_period": f"{baseline_start}-{baseline_end}",
        "baseline_years":  baseline_end - baseline_start + 1,
        "scenario":        scenario,
        "fixed_season":    fixed_season,
        "models_used":     [r["_model"] for r in per_model],
        "models_failed":   failed,
        "n_models_ok":     len(per_model),
        "per_model_results": per_model,
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

    if focal_summary:
        # Completes the architecture's three season-summary comparisons:
        result["focal_vs_baseline"] = _diff_focal_vs_baseline(focal_summary, result)
        result["focal_vs_future"]   = _diff_focal_vs_future(focal_summary, result)

    return result

# printing
def _print_2level(agg: Dict[str, Any],
                  outer_label: str = "Category",
                  inner_label: str = "Metric",
                  precision:   int = 2) -> None:
    """
    Print ensemble means in the same table shape periods.py prints, with columns matching the underlying diff keys (future_avg/baseline_avg/diff/pct
    -> future_avg/baseline_avg/Δ/Δ%). Model spread is kept in the JSON but suppressed here so the table mirrors periods.py.
    """
    if not agg:
        print("  (no comparable metrics)")
        return
    
    relabel = {"future_avg": "future_ltm", "baseline_avg": "baseline_ltm",
               "focal": "future_ltm", "baseline": "baseline_ltm"}
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
                    row[relabel.get(short, short)] = f"{v:.{precision}f}"
            rows.append(row)
    print(pd.DataFrame(rows).to_string(index=False))

def _print_diff_block(diff: Dict[str, Any],
                      outer_label: str = "Category",
                      inner_label: str = "Metric",
                      precision:   int = 2) -> None:
    """Print a plain {outer: {inner: {a_lbl, b_lbl, diff, pct}}} diff (Focal vs Future LTM)."""
    if not diff:
        print("  (no comparable metrics)")
        return
    rows = []
    for outer, inner_dict in diff.items():
        for inner, vals in inner_dict.items():
            row = {outer_label: outer, inner_label: inner}
            for k, v in vals.items():
                if not _is_num(v):
                    continue
                if k == "diff":
                    row["Δ"]  = f"{v:+.{precision}f}"
                elif k == "pct":
                    row["Δ%"] = f"{v:+.2f}%"
                else:
                    row[k] = f"{v:.{precision}f}"
            rows.append(row)
    print(pd.DataFrame(rows).to_string(index=False))

def _print_focal_vs_ltm(avl: Dict[str, Any]) -> None:
    yr       = avl.get("focal_year")
    src      = avl.get("focal_source")
    ltm_lbl  = avl.get("ltm_label", "ltm")
    title    = "FUTURE LTM" if ltm_lbl == "future_ltm" else "BASELINE LTM"
    print(f"\n{'=' * 60}")
    print(f"FOCAL {yr} ({src}) vs {title}   [Δ = focal - {ltm_lbl}]")
    print(f"{'=' * 60}")

    print(f"\n--- OVERALL STATISTICS (annualised) ---")
    _print_diff_block(avl.get("overall_statistics", {}))

    season = avl.get("season_statistics")
    if season:
        print(f"\n--- SEASON STATISTICS ---")
        if "windows" in season:
            for w in season["windows"]:
                print(f"\n  Window {w['window']} (season #{w['season_number']})")
                _print_diff_block(w["diff"])
        else:
            _print_diff_block(season["diff"])

    ann = avl.get("annual_summary", {}) or {}
    arm = ann.get("annual_rain_mm")
    print(f"\n--- ANNUAL SUMMARY ---")
    if arm:
        print(f"  Annual rainfall : focal={arm['focal']} mm | "
              f"{ltm_lbl}={arm[ltm_lbl]} mm | "
              f"Δ={arm['diff']:+.1f} ({arm['pct']:+.2f}%)")
    hs = ann.get("humid_status") or {}
    if hs:
        focal_state = ("humid" if hs.get("focal_is_humid") else
                        "not humid" if hs.get("focal_is_humid") is False else "n/a")
        print(f"  Humid status    : focal={focal_state} | "
              f"{ltm_lbl}={hs.get('ltm_humid', 'n/a')}")
        if hs.get("focal_humid_test"):
            print(f"                    test: {hs['focal_humid_test']}")

def _print_per_model_breakdown(per_model: List[Dict[str, Any]]) -> None:
    """
    Show each model's own future-vs-baseline diff before the ensemble means.
    Mirrors the ensemble sections (overall statistics + season statistics + annual rainfall) so the reader can see what each model contributes to the averages
    printed below — including what each model says about every fixed window/season.
    """
    if not per_model:
        return
    print(f"\n{'=' * 60}")
    print(f"PER-MODEL BREAKDOWN ({len(per_model)} model(s)) — feeds the ensemble means below")
    print(f"{'=' * 60}")
    for r in per_model:
        print(f"\n  Model: {r.get('_model')}")
        print(f"  --- Overall statistics (annualised) ---")
        _print_diff_block(r.get("overall_statistics", {}))

        season = r.get("season_statistics")
        if season:
            print(f"  --- Season statistics ---")
            if "windows" in season:
                for w in season.get("windows", []):
                    print(f"    Window {w.get('window')} (season #{w.get('season_number')}, "
                          f"n_baseline={w.get('n_baseline')}, n_future={w.get('n_future')})")
                    _print_diff_block(w.get("diff", {}))
            elif season.get("diff"):
                print(f"    (n_baseline={season.get('n_baseline')}, "
                      f"n_future={season.get('n_future')})")
                _print_diff_block(season["diff"])

        arm = (r.get("annual_summary") or {}).get("annual_rain_mm") or {}
        if _is_num(arm.get("future")) and _is_num(arm.get("baseline_avg")):
            print(f"  Annual rainfall : future={arm['future']:.1f} mm | "
                  f"baseline={arm['baseline_avg']:.1f} mm | "
                  f"Δ={arm.get('diff', 0):+.1f} ({arm.get('pct', 0):+.2f}%)")

def print_report(r: Dict[str, Any]) -> None:
    if "error" in r:
        print(f"\nError: {r['error']}")
        for f in r.get("failed_models", []):
            print(f"  - {f['model']}: {f['error']}")
        return

    n_total = r["n_models_ok"] + len(r["models_failed"])
    print(f"\n{'=' * 60}")
    print(f"ENSEMBLE: Baseline LTM {r['baseline_period']} vs Future LTM {r['future_period']}")
    print(f"{'=' * 60}")
    print(f"  Scenario : {r['scenario']}")
    print(f"  Models ok: {r['n_models_ok']}/{n_total}")
    print(f"  Δ        : future_ltm - baseline_ltm")
    if r["models_failed"]:
        print(f"  Failed   : {', '.join(f['model'] for f in r['models_failed'])}")

    _print_per_model_breakdown(r.get("per_model_results", []))

    print(f"\n--- 1. RAW CLIMATE SUMMARY (ensemble) ---")
    _print_2level(r.get("raw_climate_summary", {}),
                  outer_label="Variable", inner_label="Stat", precision=3)

    print(f"\n--- 2. OVERALL STATISTICS (ensemble, both periods annualised) ---")
    _print_2level(r.get("overall_statistics", {}))

    season = r.get("season_statistics")
    if season:
        print(f"\n--- 3. SEASON STATISTICS  (Baseline LTM vs Future LTM) ---")
        if "windows" in season:
            for w in season["windows"]:
                print(f"\n  Window {w['window']} (season #{w['season_number']}, n_models={w['n_models']})")
                _print_2level(w["diff"])
        else:
            print(f"  (n_models={season['n_models']})")
            _print_2level(season["diff"])

    print(f"\n--- 4. ANNUAL SUMMARY (ensemble) ---")
    ann = r.get("annual_summary", {})
    foc = ann.get("annual_rain_mm_future")    or {}
    bas = ann.get("annual_rain_mm_baseline") or {}
    dif = ann.get("annual_rain_mm_diff")     or {}
    pct = ann.get("annual_rain_mm_pct")      or {}
    if _is_num(foc.get("mean")):
        parts = [f"future_ltm={foc['mean']:.1f} mm"]
        if _is_num(bas.get("mean")):
            parts.append(f"baseline_ltm={bas['mean']:.1f} mm")
        if _is_num(dif.get("mean")):
            tail = (f" ({pct['mean']:+.2f}%)"
                    if _is_num(pct.get("mean")) else "")
            parts.append(f"Δ={dif['mean']:+.1f}{tail}")
        print(f"  Annual rainfall  : {' | '.join(parts)}")
    print(f"  Humid (future)   : {ann.get('humid_future', 'n/a')}")
    print(f"  Humid (baseline) : {ann.get('humid_baseline', 'n/a')}")
    print()

    avb = r.get("focal_vs_baseline")
    if avb:
        _print_focal_vs_ltm(avb)
        print()
    avf = r.get("focal_vs_future")
    if avf:
        _print_focal_vs_ltm(avf)
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
        description="Ensemble future-period-vs-baseline-period comparison across NEX-GDDP models.",
        formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--location",       required=True, help="lat,lon (e.g. -1.286,36.817)")
    p.add_argument("--baseline-start", type=int, required=True,
                   help="First year of the baseline period (inclusive)")
    p.add_argument("--baseline-end",   type=int, required=True,
                   help="Last year of the baseline period (inclusive)")
    p.add_argument("--future-start",    type=int, required=True,
                   help="First year of the future period (inclusive)")
    p.add_argument("--future-end",      type=int, required=True,
                   help="Last year of the future period (inclusive). "
                        "For a single year, set --future-start == --future-end.")
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
    p.add_argument("--focal-year",    type=int, default=None,
                   help="Optional. Single observed year to also compare against both the "
                        "baseline and future LTMs ('Focal vs Baseline/Future LTM').")
    p.add_argument("--focal-source",  default=None,
                   help=f"Source for --focal-year. Defaults to 'nex_gddp' (focal year "
                        f"drawn from the ensemble itself). External: "
                        f"{', '.join(sorted(SUPPORTED))}")
    p.add_argument("--output",         default=None, help="Save JSON result to this path")
    p.add_argument("--quiet",          action="store_true")
    args = p.parse_args()

    try:
        lat, lon = (float(x) for x in args.location.replace(" ", ",").split(","))
    except ValueError:
        print("Error: --location must be 'lat,lon'"); sys.exit(1)

    models  = [m.strip() for m in args.models.split(",")]         if args.models         else None
    exclude = [m.strip() for m in args.exclude_models.split(",")] if args.exclude_models else None

    # --focal-year with no --focal-source defaults to the NEX-GDDP ensemble itself, giving an all-NEX-GDDP focal/baseline/future comparison.
    if args.focal_year is not None and not args.focal_source:
        args.focal_source = "nex_gddp"
    focal_is_nexgddp = (args.focal_year is not None
                        and args.focal_source.lower() == "nex_gddp")
    if (args.focal_source and not focal_is_nexgddp
            and args.focal_source.lower() not in SUPPORTED):
        print(f"Error: --focal-source '{args.focal_source}' not supported. "
              f"Use 'nex_gddp' or one of: {', '.join(sorted(SUPPORTED))}"); sys.exit(1)

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

    # An external observed source (era_5, etc.) is scenario-independent, so the focal year is fetched once. A NEX-GDDP focal year inherits each scenario's run, so it is built per-scenario inside the loop below.
    focal_summary: Optional[Dict[str, Any]] = None
    if args.focal_year is not None and not focal_is_nexgddp:
        if not args.quiet:
            print(f"\nFetching focal year {args.focal_year} | "
                  f"source={args.focal_source}")
        focal_summary = _build_focal_summary(
            location=(lat, lon),
            focal_year=args.focal_year,
            focal_source=args.focal_source,
            fixed_season=args.fixed_season,
        )

    all_results: Dict[str, Any] = {}
    any_ok = False
    for scenario in scenarios:
        scenario_focal = focal_summary
        if focal_is_nexgddp:
            if not args.quiet:
                print(f"\nFetching focal year {args.focal_year} from NEX-GDDP "
                      f"ensemble | scenario={scenario}")
            scenario_focal = _build_focal_summary_nexgddp(
                location=(lat, lon),
                focal_year=args.focal_year,
                fixed_season=args.fixed_season,
                scenario=scenario,
                models=models,
                exclude_models=exclude,
                verbose=not args.quiet,
            )
        result = ensemble_compare(
            location=(lat, lon),
            baseline_start=args.baseline_start,
            baseline_end=args.baseline_end,
            future_start=args.future_start,
            future_end=args.future_end,
            scenario=scenario,
            fixed_season=args.fixed_season,
            models=models,
            exclude_models=exclude,
            focal_summary=scenario_focal,
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

# focal year from external observed source:
# python -m climate_tookit.compare_periods.ensemble_periods --location="-1.286,36.817" --baseline-start 1991 --baseline-end 2020 --future-start 2040 --future-end 2060 --fixed-season "03-01:05-31" --scenarios ssp245 --focal-year 2019 --focal-source era_5 --output ensemble_mam_focal2019.json
# focal year from NEX-GDDP:
# python -m climate_tookit.compare_periods.ensemble_periods --location="-1.286,36.817" --baseline-start 1991 --baseline-end 2020 --future-start 2040 --future-end 2060 --fixed-season "03-01:05-31" --scenarios ssp245 --focal-year 2019 --output ensemble_mam_focal2019.json

# Auto-detected season (no --fixed-season) -- all models, pick scenarios:
# python -m climate_tookit.compare_periods.ensemble_periods --location="-1.286,36.817" --baseline-start 1991 --baseline-end 2020 --future-start 2040 --future-end 2060 --scenarios ssp245,ssp585 --output ensemble_auto_all.json
# python -m climate_tookit.compare_periods.ensemble_periods --location="-1.286,36.817" --baseline-start 1991 --baseline-end 2020 --future-start 2040 --future-end 2060 --models "ACCESS-CM2,EC-Earth3,MRI-ESM2-0" --scenarios ssp245,ssp585 --output ensemble_auto.json

# Fixed single season:
# python -m climate_tookit.compare_periods.ensemble_periods --location="-1.286,36.817" --baseline-start 1991 --baseline-end 2020 --future-start 2040 --future-end 2060 --fixed-season "03-01:05-31" --scenarios ssp245,ssp585 --output ensemble_mam_all.json
# python -m climate_tookit.compare_periods.ensemble_periods --location="-1.286,36.817" --baseline-start 1991 --baseline-end 2020 --future-start 2040 --future-end 2060 --fixed-season "03-01:05-31" --models "ACCESS-CM2,EC-Earth3,MRI-ESM2-0" --scenarios ssp585 --output ensemble_mam.json

# Fixed two seasons:
# python -m climate_tookit.compare_periods.ensemble_periods --location="-1.286,36.817" --baseline-start 1991 --baseline-end 2020 --future-start 2040 --future-end 2060 --fixed-season "03-01:05-31,10-01:12-15" --scenarios ssp245,ssp585 --output ensemble_mam_ond_all.json
# python -m climate_tookit.compare_periods.ensemble_periods --location="-1.286,36.817" --baseline-start 1991 --baseline-end 2020 --future-start 2040 --future-end 2060 --fixed-season "03-01:05-31,10-01:12-15" --models "ACCESS-CM2,EC-Earth3,MRI-ESM2-0" --scenarios ssp245,ssp585 --output ensemble_mam_ond.json

# Fixed year-crossing season:
# python -m climate_tookit.compare_periods.ensemble_periods --location="-1.286,36.817" --baseline-start 1991 --baseline-end 2020 --future-start 2040 --future-end 2060 --fixed-season "11-01:02-28" --scenarios ssp245,ssp585 --output ensemble_njf_all.json
# python -m climate_tookit.compare_periods.ensemble_periods --location="-1.286,36.817" --baseline-start 1991 --baseline-end 2020 --future-start 2040 --future-end 2060 --fixed-season "11-01:02-28" --models "ACCESS-CM2,EC-Earth3,MRI-ESM2-0" --scenarios ssp585 --output ensemble_njf.json