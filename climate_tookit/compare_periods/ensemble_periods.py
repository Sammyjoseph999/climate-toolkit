"""
Ensemble Period Comparison Module

Runs future-vs-baseline climate statistics comparison across all 16 NEX-GDDP
CMIP6 models, then computes an ensemble-mean future and compares that ensemble
future against one shared historical baseline.

This is NOT model-vs-model comparison.
Each model contributes one future-period result. Those 16 future values are
averaged into a single ensemble mean per metric, which is then compared against
the shared historical baseline. The output contains ONE result — the average.

Usage (CLI):
    python ensemble_periods.py --location="-1.286,36.817" \
        --baseline-start 1991 --baseline-end 2020 --baseline-source nasa_power \
        --future-start 2040 --future-end 2060 --scenario ssp245

    python ensemble_periods.py --location="-1.286,36.817" \
        --baseline-start 1991 --baseline-end 2020 --baseline-source nasa_power \
        --future-start 2040 --future-end 2060 --scenario ssp245 \
        --exclude-models "CanESM5,KACE-1-0-G" --output results.json

    python ensemble_periods.py --list-models
"""

import sys
import os
import math
import statistics
import logging
import json
import argparse
from datetime import datetime
from typing import Dict, Any, Tuple, List, Optional

import pandas as pd

logging.disable(logging.INFO)

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.insert(0, project_root)

from climate_tookit.compare_periods.periods import (
    calculate_baseline_statistics,
    compare_future_vs_baseline,
)

pd.set_option("display.float_format", lambda x: f"{x:.2f}")


# Corrected model list — aligned with what compare_future_vs_baseline() accepts.
NEX_GDDP_MODELS: List[str] = [
    "ACCESS-CM2",
    "ACCESS-ESM1-5",
    "CanESM5",
    "CMCC-ESM2",
    "EC-Earth3",
    "EC-Earth3-Veg-LR",
    "GFDL-ESM4",
    "INM-CM4-8",
    "INM-CM5-0",
    "KACE-1-0-G",
    "MIROC6",
    "MPI-ESM1-2-LR",
    "MRI-ESM2-0",
    "NorESM2-LM",
    "NorESM2-MM",
    "TaiESM1",
]

SSP_SCENARIOS: List[str] = ["ssp126", "ssp245", "ssp370", "ssp585"]

COMPARISON_CATEGORIES = ["precipitation", "temperature", "et0", "water_balance"]


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _percentile(data: List[float], p: float) -> Optional[float]:
    """Compute a simple interpolated percentile."""
    if not data:
        return None
    s = sorted(data)
    idx = (p / 100) * (len(s) - 1)
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return round(s[lo], 2)
    return round(s[lo] + (idx - lo) * (s[hi] - s[lo]), 2)


def _clean_numeric(values: List[Any]) -> List[float]:
    """Keep only numeric non-NaN values."""
    clean: List[float] = []
    for v in values:
        if isinstance(v, bool):
            continue
        if isinstance(v, (int, float)):
            if isinstance(v, float) and math.isnan(v):
                continue
            clean.append(float(v))
    return clean


def _ensemble_stats(values: List[Any]) -> Dict[str, Any]:
    """
    Compute descriptive statistics across all models' future values for one
    metric. The 'mean' is the ensemble average used in all comparisons.
    """
    clean = _clean_numeric(values)
    if not clean:
        return {
            "mean": None, "median": None, "std": None,
            "min": None, "max": None,
            "p10": None, "p25": None, "p75": None, "p90": None,
            "n": 0,
        }
    return {
        "mean":   round(statistics.mean(clean), 2),
        "median": round(statistics.median(clean), 2),
        "std":    round(statistics.stdev(clean), 2) if len(clean) > 1 else 0.0,
        "min":    round(min(clean), 2),
        "max":    round(max(clean), 2),
        "p10":    _percentile(clean, 10),
        "p25":    _percentile(clean, 25),
        "p75":    _percentile(clean, 75),
        "p90":    _percentile(clean, 90),
        "n":      len(clean),
    }


def _filter_models(
    models: Optional[List[str]],
    exclude_models: Optional[List[str]],
) -> List[str]:
    active = list(models) if models else list(NEX_GDDP_MODELS)
    if exclude_models:
        excluded = {m.upper() for m in exclude_models}
        active = [m for m in active if m.upper() not in excluded]
    return active


def _safe_number(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        if isinstance(value, float) and math.isnan(value):
            return None
        return float(value)
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Per-model run
# ──────────────────────────────────────────────────────────────────────────────

def _run_single_model(
    model: str,
    location_coord: Tuple[float, float],
    future_years: Tuple[int, int],
    baseline_stats: Dict[str, Any],
    scenario: str,
) -> Dict[str, Any]:
    """Run future-vs-baseline for one model. Returns {model, error, comparison}."""
    try:
        comparison = compare_future_vs_baseline(
            location_coord=location_coord,
            future_years=future_years,
            baseline_stats=baseline_stats,
            source="nex_gddp",
            model=model,
            scenario=scenario,
        )
        if "error" in comparison:
            return {"model": model, "error": comparison["error"], "comparison": None}
        return {"model": model, "error": None, "comparison": comparison}
    except Exception as exc:
        return {"model": model, "error": str(exc), "comparison": None}


# ──────────────────────────────────────────────────────────────────────────────
# Aggregation  →  single ensemble average
# ──────────────────────────────────────────────────────────────────────────────

def _aggregate_ensemble(
    model_results: List[Dict[str, Any]],
    baseline_stats: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Pool all 16 models' future values per metric and compute a single average.

    Returns one flat result per category/metric:
        {
            category: {
                metric: {
                    baseline:       float,
                    future:         float,   <- ensemble mean (average of all models)
                    difference:     float,
                    percent_change: float,
                    model_spread: {          <- uncertainty info only, no per-model data
                        std, min, max, p10, p90, n_models
                    }
                }
            }
        }
    """
    successful = [r for r in model_results if r.get("comparison") is not None]
    if not successful:
        return {}

    ensemble: Dict[str, Any] = {}

    for category in COMPARISON_CATEGORIES:
        metric_values: Dict[str, List[float]] = {}

        for result in successful:
            category_data = result["comparison"].get("variables", {}).get(category, {})
            for metric, values in category_data.items():
                future_value = _safe_number(values.get("future"))
                if future_value is None:
                    continue
                metric_values.setdefault(metric, []).append(future_value)

        if not metric_values:
            continue

        ensemble[category] = {}

        for metric, futures in metric_values.items():
            stats = _ensemble_stats(futures)
            baseline_value = _safe_number(baseline_stats.get(category, {}).get(metric))
            ensemble_mean = stats["mean"]

            if baseline_value is not None and ensemble_mean is not None:
                diff = round(ensemble_mean - baseline_value, 2)
                pct  = round((diff / baseline_value) * 100, 2) if baseline_value != 0 else 0.0
            else:
                diff = pct = None

            ensemble[category][metric] = {
                "baseline":       round(baseline_value, 2) if baseline_value is not None else None,
                "future":         ensemble_mean,   # single averaged value across all models
                "difference":     diff,
                "percent_change": pct,
                "model_spread": {                  # spread info, not per-model data
                    "std":      stats["std"],
                    "min":      stats["min"],
                    "max":      stats["max"],
                    "p10":      stats["p10"],
                    "p90":      stats["p90"],
                    "n_models": stats["n"],
                },
            }

    return ensemble


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────

def ensemble_compare_periods(
    location_coord: Tuple[float, float],
    baseline_years: Tuple[int, int],
    future_years: Tuple[int, int],
    baseline_source: str,
    scenario: str = "ssp245",
    models: Optional[List[str]] = None,
    exclude_models: Optional[List[str]] = None,
    verbose: bool = True,
) -> Dict[str, Any]:
    """
    Compare baseline vs future climate across all 16 NEX-GDDP models and
    return a SINGLE ensemble-averaged result per metric.

    Process:
      1. Compute one shared historical baseline.
      2. Run each model independently to get its future values.
      3. Average all 16 models' future values per metric.
      4. Compare the ensemble average against the baseline.

    Individual model outputs are NOT included in the return value.

    Returns:
        {
            "ensemble_results": {
                category: {
                    metric: {
                        baseline, future, difference, percent_change, model_spread
                    }
                }
            },
            "baseline": { ... },
            "metadata": { ... }
        }
    """
    lat, lon = location_coord
    baseline_start, baseline_end = baseline_years
    future_start, future_end = future_years

    active_models = _filter_models(models, exclude_models)

    if verbose:
        print(f"\n{'=' * 60}")
        print("NEX-GDDP Ensemble Period Comparison")
        print(f"  Location  : {lat}, {lon}")
        print(f"  Baseline  : {baseline_start}-{baseline_end}  ({baseline_source})")
        print(f"  Future    : {future_start}-{future_end}  (NEX-GDDP / {scenario})")
        print(f"  Models    : {len(active_models)}")
        print(f"{'=' * 60}")

    # ── Step 1: one shared historical baseline ────────────────────────────────
    baseline_stats = calculate_baseline_statistics(
        location_coord=location_coord,
        baseline_years=baseline_years,
        source=baseline_source,
    )
    if "error" in baseline_stats:
        return {
            "error": f"Baseline calculation failed: {baseline_stats['error']}",
            "metadata": {"analysis_date": datetime.now().isoformat()},
        }

    # ── Step 2: run each model independently ─────────────────────────────────
    if verbose:
        print(f"\n  Running {len(active_models)} future model calculations...\n")

    model_results: List[Dict[str, Any]] = []
    failed_models: List[str] = []

    for i, model in enumerate(active_models, 1):
        if verbose:
            print(f"  [{i:02d}/{len(active_models):02d}] {model:<25}", end=" ", flush=True)

        result = _run_single_model(
            model=model,
            location_coord=location_coord,
            future_years=future_years,
            baseline_stats=baseline_stats,
            scenario=scenario,
        )
        model_results.append(result)

        if verbose:
            if result["error"]:
                print(f"x  {result['error']}")
                failed_models.append(model)
            else:
                print("ok")

    # ── Step 3: average all models into one ensemble result ───────────────────
    successful = [r for r in model_results if r["comparison"] is not None]
    ensemble_results = _aggregate_ensemble(model_results, baseline_stats)

    if verbose:
        print(f"\n{'-' * 60}")
        print(f"Ensemble complete ({len(successful)}/{len(active_models)} models succeeded)")
        if failed_models:
            print(f"Failed models : {', '.join(failed_models)}")
        _print_ensemble_summary(ensemble_results)
        print("=" * 60)

    # Only the averaged ensemble result is returned — no per-model data.
    return {
        "ensemble_results": ensemble_results,
        "baseline": baseline_stats,
        "metadata": {
            "location": {"lat": lat, "lon": lon},
            "baseline_period": {"start": baseline_start, "end": baseline_end},
            "future_period": {"start": future_start, "end": future_end},
            "baseline_source": baseline_source,
            "future_source": "NEX-GDDP-CMIP6",
            "scenario": scenario,
            "models_used": active_models,
            "models_ok": len(successful),
            "models_failed": len(active_models) - len(successful),
            "failed_models": failed_models,
            "analysis_date": datetime.now().isoformat(),
        },
    }


# ──────────────────────────────────────────────────────────────────────────────
# Reporting
# ──────────────────────────────────────────────────────────────────────────────

def _print_ensemble_summary(ensemble_results: Dict[str, Any]) -> None:
    """Print a clean summary table of ensemble-averaged results."""
    key_metrics = [
        "total_mm",
        "mean_daily",
        "mean_tmax",
        "mean_tmin",
        "mean_tavg",
        "total_balance",
    ]

    rows = []
    for category, metrics in ensemble_results.items():
        for metric, data in metrics.items():
            if metric not in key_metrics:
                continue
            spread = data.get("model_spread", {})
            if spread.get("n_models", 0) == 0:
                continue
            rows.append({
                "Category":     category.title(),
                "Metric":       metric,
                "Baseline":     f"{data['baseline']:.2f}"         if data.get("baseline")       is not None else "N/A",
                "Ens. Average": f"{data['future']:.2f}"           if data.get("future")         is not None else "N/A",
                "Difference":   f"{data['difference']:+.2f}"      if data.get("difference")     is not None else "N/A",
                "Change %":     f"{data['percent_change']:+.2f}%" if data.get("percent_change") is not None else "N/A",
                "Spread (std)": f"{spread['std']:.2f}"            if spread.get("std")          is not None else "N/A",
                "p10–p90":      (
                    f"[{spread['p10']:.2f} – {spread['p90']:.2f}]"
                    if spread.get("p10") is not None and spread.get("p90") is not None
                    else "N/A"
                ),
                "n": spread.get("n_models", 0),
            })

    if rows:
        print()
        print(pd.DataFrame(rows).to_string(index=False))
        print()


def print_ensemble_report(result: Dict[str, Any]) -> None:
    """Print a formatted ensemble comparison report to stdout."""
    print(f"\n{'=' * 60}")
    print("ENSEMBLE COMPARISON REPORT")
    print(f"{'=' * 60}")

    if "error" in result:
        print(f"Error: {result['error']}")
        return

    meta = result.get("metadata", {})
    bp   = meta.get("baseline_period", {})
    fp   = meta.get("future_period", {})
    loc  = meta.get("location", {})

    print(f"Location : {loc.get('lat')}, {loc.get('lon')}")
    print(f"Baseline : {bp.get('start')}-{bp.get('end')}  ({meta.get('baseline_source', 'N/A')})")
    print(f"Future   : {fp.get('start')}-{fp.get('end')}  (NEX-GDDP / {meta.get('scenario', 'N/A')})")
    print(f"Models   : {meta.get('models_ok')}/{len(meta.get('models_used', []))} succeeded")

    if meta.get("failed_models"):
        print(f"Failed   : {', '.join(meta['failed_models'])}")

    _print_ensemble_summary(result.get("ensemble_results", {}))


# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────

def main() -> None:
    if "--list-models" in sys.argv:
        print("Available NEX-GDDP-CMIP6 models:")
        for i, model in enumerate(NEX_GDDP_MODELS, 1):
            print(f"  {i:02d}. {model}")
        sys.exit(0)

    parser = argparse.ArgumentParser(
        description=(
            "Ensemble future-vs-baseline climate comparison across NEX-GDDP models.\n"
            "Outputs a single averaged result — not per-model data."
        )
    )
    parser.add_argument("--location",        required=True, help='Coordinates as "lat,lon"')
    parser.add_argument("--baseline-start",  type=int, required=True)
    parser.add_argument("--baseline-end",    type=int, required=True)
    parser.add_argument("--baseline-source", required=True,
                        help="Historical data source (e.g. nasa_power, era5)")
    parser.add_argument("--future-start",    type=int, required=True)
    parser.add_argument("--future-end",      type=int, required=True)
    parser.add_argument("--scenario",        default="ssp245", choices=SSP_SCENARIOS)
    parser.add_argument("--models",          help="Comma-separated subset of models")
    parser.add_argument("--exclude-models",  help="Comma-separated models to exclude")
    parser.add_argument("--list-models",     action="store_true")
    parser.add_argument("--output",          help="Save JSON result to this file path")
    parser.add_argument("--quiet",           action="store_true")
    args = parser.parse_args()

    try:
        parts = [p.strip() for p in args.location.replace(" ", ",").split(",") if p.strip()]
        if len(parts) != 2:
            raise ValueError
        lat, lon = map(float, parts)
    except ValueError:
        print('Error: --location must be in "lat,lon" format, e.g. "-1.286,36.817"')
        sys.exit(1)

    models  = [m.strip() for m in args.models.split(",")]         if args.models         else None
    exclude = [m.strip() for m in args.exclude_models.split(",")]  if args.exclude_models else None

    result = ensemble_compare_periods(
        location_coord=(lat, lon),
        baseline_years=(args.baseline_start, args.baseline_end),
        future_years=(args.future_start, args.future_end),
        baseline_source=args.baseline_source,
        scenario=args.scenario,
        models=models,
        exclude_models=exclude,
        verbose=not args.quiet,
    )

    if not args.quiet:
        print_ensemble_report(result)

    output = json.dumps(result, indent=2, default=str)

    if args.output:
        with open(args.output, "w") as fh:
            fh.write(output)
        print(f"\nResults saved to {args.output}")
    else:
        print(output)


if __name__ == "__main__":
    main()

# Example commands
# List available models:
# python climate_tookit/compare_periods/ensemble_periods.py --list-models

# Basic run:
# python climate_tookit/compare_periods/ensemble_periods.py --location="-1.286,36.817" --baseline-start 1991 --baseline-end 2020 --baseline-source nasa_power --future-start 2040 --future-end 2060 --scenario ssp245

# Save output:
# python climate_tookit/compare_periods/ensemble_periods.py --location="-1.286,36.817" --baseline-start 1991 --baseline-end 2020 --baseline-source nasa_power --future-start 2040 --future-end 2060 --scenario ssp245 --output results.json

# Exclude models:
# python climate_tookit/compare_periods/ensemble_periods.py --location="-1.286,36.817" --baseline-start 1991 --baseline-end 2020 --baseline-source nasa_power --future-start 2040 --future-end 2060 --scenario ssp245 --exclude-models "CESM2,GFDL-CM4,BCC-CSM2-MR"

# Subset of models only:
# python climate_tookit/compare_periods/ensemble_periods.py --location="-1.286,36.817" --baseline-start 1991 --baseline-end 2020 --baseline-source nasa_power --future-start 2040 --future-end 2060 --scenario ssp245 --models "GFDL-ESM4,MRI-ESM2-0,ACCESS-CM2"