"""
NEX-GDDP Ensemble Season Analysis Submodule

Runs season analysis across all 16 NEX-GDDP CMIP6 models and computes ensemble
statistics (mean, median, std, percentiles) for onset DOY, cessation DOY, and
season length. Supports per-scenario ensembling with optional model exclusion.

Dependencies: pandas, climate_toolkit, season_analysis.seasons
"""

import json
import argparse
import sys
import math
import statistics

import logging
logging.disable(logging.INFO)

from datetime import datetime
from typing import Tuple, Dict, List, Any, Optional
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from season_analysis.seasons import get_climate_data, calculate_et0, detect_seasons

NEX_GDDP_MODELS: List[str] = [
    'ACCESS-CM2',
    'ACCESS-ESM1-5',
    'BCC-CSM2-MR',
    'CESM2',
    'CMCC-CM2-SR5',
    'CMCC-ESM2',
    'EC-Earth3',
    'EC-Earth3-Veg-LR',
    'GFDL-CM4',
    'GFDL-ESM4',
    'INM-CM4-8',
    'INM-CM5-0',
    'IPSL-CM6A-LR',
    'MPI-ESM1-2-HR',
    'MRI-ESM2-0',
    'NorESM2-MM',
]

SSP_SCENARIOS: List[str] = ['ssp126', 'ssp245', 'ssp370', 'ssp585']


def _percentile(data: List[float], p: float) -> float:
    if not data:
        return float('nan')
    s = sorted(data)
    idx = (p / 100) * (len(s) - 1)
    lo, hi = int(idx), min(int(idx) + 1, len(s) - 1)
    return s[lo] + (idx - lo) * (s[hi] - s[lo])


def compute_ensemble_stats(values: List[float]) -> Dict[str, Any]:
    """
    Compute descriptive statistics over a list of per-model values.

    Args:
        values: Numeric values, one per model. None and NaN are excluded.

    Returns:
        Dict with keys: mean, median, std, min, max, p10, p25, p75, p90, n.
        All values are None when the cleaned list is empty.
    """
    clean = [v for v in values if v is not None and not (isinstance(v, float) and math.isnan(v))]
    if not clean:
        return {k: None for k in ('mean', 'median', 'std', 'min', 'max', 'p10', 'p25', 'p75', 'p90', 'n')}
    return {
        'mean':   round(statistics.mean(clean), 2),
        'median': round(statistics.median(clean), 2),
        'std':    round(statistics.stdev(clean), 2) if len(clean) > 1 else 0.0,
        'min':    round(min(clean), 2),
        'max':    round(max(clean), 2),
        'p10':    round(_percentile(clean, 10), 2),
        'p25':    round(_percentile(clean, 25), 2),
        'p75':    round(_percentile(clean, 75), 2),
        'p90':    round(_percentile(clean, 90), 2),
        'n':      len(clean),
    }


def _run_single_model(
    model: str,
    lat: float,
    lon: float,
    future_start: int,
    future_end: int,
    scenario: str,
    gap_days: int,
    min_season_days: int,
) -> Dict[str, Any]:
    all_seasons: List[Dict] = []
    years_ok = years_fail = 0

    for year in range(future_start, future_end + 1):
        try:
            df = get_climate_data(
                lat, lon,
                f"{year}-01-01", f"{year}-12-31",
                use_projections=True,
                model=model,
                scenario=scenario,
            )
            df['et0'] = [calculate_et0(r['tmin'], r['tmax'], lat, r['date']) for _, r in df.iterrows()]
            all_seasons.extend(detect_seasons(df, gap_days, min_season_days))
            years_ok += 1
        except Exception:
            years_fail += 1

    if not all_seasons:
        return {
            'model': model, 'scenario': scenario,
            'avg_onset_doy': None, 'avg_cessation_doy': None, 'avg_length_days': None,
            'season_count': 0, 'years_with_data': years_ok, 'years_failed': years_fail,
            'error': 'No seasons detected across all years',
        }

    n = len(all_seasons)
    return {
        'model':             model,
        'scenario':          scenario,
        'avg_onset_doy':     round(sum(s['onset_doy'] for s in all_seasons) / n, 1),
        'avg_cessation_doy': round(sum(s['cessation_doy'] for s in all_seasons) / n, 1),
        'avg_length_days':   round(sum(s['length_days'] for s in all_seasons) / n, 1),
        'season_count':      n,
        'years_with_data':   years_ok,
        'years_failed':      years_fail,
    }


def ensemble_season_analysis(
    location_coord: Tuple[float, float],
    future_years: Tuple[int, int],
    scenario: str = 'ssp245',
    models: Optional[List[str]] = None,
    exclude_models: Optional[List[str]] = None,
    gap_days: int = 30,
    min_season_days: int = 30,
    verbose: bool = True,
) -> Dict[str, Any]:
    """
    Run season analysis across all (or selected) NEX-GDDP models and return
    ensemble-aggregated statistics.

    Args:
        location_coord: (lat, lon) in decimal degrees.
        future_years: (start_year, end_year) inclusive.
        scenario: SSP scenario — ssp126, ssp245, ssp370, or ssp585.
        models: Explicit model list; defaults to all 16 NEX_GDDP_MODELS.
        exclude_models: Model names to skip (case-insensitive).
        gap_days: Consecutive dry days to close a season.
        min_season_days: Minimum valid season length in days.
        verbose: Print per-model progress to stdout.

    Returns:
        Dict with keys:
            ensemble_stats — {onset_doy, cessation_doy, length_days} x statistics.
            model_results  — per-model averages.
            metadata       — location, scenario, period, models used, analysis_date.
    """
    lat, lon = location_coord
    future_start, future_end = future_years

    active_models = list(models) if models else list(NEX_GDDP_MODELS)
    if exclude_models:
        excluded = {m.upper() for m in exclude_models}
        active_models = [m for m in active_models if m.upper() not in excluded]

    if verbose:
        print(f"\n{'='*60}")
        print(f"NEX-GDDP Ensemble  |  {lat}, {lon}  |  {future_start}-{future_end}  |  {scenario}  |  {len(active_models)} models")
        print('='*60)

    model_results: List[Dict] = []

    for i, model in enumerate(active_models, 1):
        if verbose:
            print(f"  [{i:02d}/{len(active_models):02d}] {model:<25}", end=' ', flush=True)
        r = _run_single_model(model, lat, lon, future_start, future_end, scenario, gap_days, min_season_days)
        model_results.append(r)

        if verbose:
            if r.get('error'):
                print(f"✗  {r['error']}")
            else:
                print(f"✓  onset={r['avg_onset_doy']} DOY  cessation={r['avg_cessation_doy']} DOY  length={r['avg_length_days']} d  ({r['season_count']} seasons)")

    successful = [r for r in model_results if not r.get('error') and r['avg_onset_doy'] is not None]

    ensemble_stats = {
        'onset_doy':     compute_ensemble_stats([r['avg_onset_doy'] for r in successful]),
        'cessation_doy': compute_ensemble_stats([r['avg_cessation_doy'] for r in successful]),
        'length_days':   compute_ensemble_stats([r['avg_length_days'] for r in successful]),
    }

    if verbose:
        print(f"\n{'─'*60}")
        print(f"Ensemble complete  ({len(successful)}/{len(active_models)} models succeeded)")
        for var, stats in ensemble_stats.items():
            if stats['n']:
                print(f"  {var:<16} mean={stats['mean']}  median={stats['median']}  std={stats['std']}  p10-p90=[{stats['p10']}-{stats['p90']}]")
        print('='*60)

    return {
        'ensemble_stats': ensemble_stats,
        'model_results':  model_results,
        'metadata': {
            'location':        {'lat': lat, 'lon': lon},
            'period':          {'start': future_start, 'end': future_end},
            'scenario':        scenario,
            'models_used':     active_models,
            'models_ok':       len(successful),
            'models_failed':   len(active_models) - len(successful),
            'gap_days':        gap_days,
            'min_season_days': min_season_days,
            'data_source':     'NEX-GDDP-CMIP6',
            'method':          'ET0_precipitation_threshold',
            'analysis_date':   datetime.now().isoformat(),
        },
    }


def ensemble_all_scenarios(
    location_coord: Tuple[float, float],
    future_years: Tuple[int, int],
    models: Optional[List[str]] = None,
    exclude_models: Optional[List[str]] = None,
    gap_days: int = 30,
    min_season_days: int = 30,
    verbose: bool = True,
) -> Dict[str, Any]:
    """
    Run ensemble_season_analysis for all four SSP scenarios.

    Returns:
        Dict keyed by scenario (ssp126, ssp245, ssp370, ssp585), each value
        being the full result from ensemble_season_analysis.
    """
    return {
        scenario: ensemble_season_analysis(
            location_coord=location_coord,
            future_years=future_years,
            scenario=scenario,
            models=models,
            exclude_models=exclude_models,
            gap_days=gap_days,
            min_season_days=min_season_days,
            verbose=verbose,
        )
        for scenario in SSP_SCENARIOS
    }


def main() -> None:
    
    if '--list-models' in sys.argv:
        print("Available NEX-GDDP-CMIP6 models:")
        for i, m in enumerate(NEX_GDDP_MODELS, 1):
            print(f"  {i:02d}. {m}")
        sys.exit(0)
        
    parser = argparse.ArgumentParser(description='NEX-GDDP ensemble season analysis across all 16 CMIP6 models')
    parser.add_argument('--location', required=True, help='Coordinates as "lat,lon"')
    parser.add_argument('--future-start', type=int, required=True)
    parser.add_argument('--future-end', type=int, required=True)
    parser.add_argument('--scenario', default='ssp245', choices=SSP_SCENARIOS)
    parser.add_argument('--all-scenarios', action='store_true', help='Run all four SSP scenarios')
    parser.add_argument('--models', help='Comma-separated subset of models')
    parser.add_argument('--exclude-models', help='Comma-separated models to exclude')
    parser.add_argument('--gap-days', type=int, default=30)
    parser.add_argument('--min-season-days', type=int, default=30)
    parser.add_argument('--list-models', action='store_true', help='Print available models and exit')
    parser.add_argument('--output', help='Save JSON result to this path')
    parser.add_argument('--quiet', action='store_true')
    args = parser.parse_args()

    try:
        lat, lon = map(float, args.location.split(','))
    except ValueError:
        print("Error: --location must be in 'lat,lon' format.")
        sys.exit(1)

    models = [m.strip() for m in args.models.split(',')] if args.models else None
    exclude = [m.strip() for m in args.exclude_models.split(',')] if args.exclude_models else None

    if args.all_scenarios:
        result = ensemble_all_scenarios(
            location_coord=(lat, lon),
            future_years=(args.future_start, args.future_end),
            models=models,
            exclude_models=exclude,
            gap_days=args.gap_days,
            min_season_days=args.min_season_days,
            verbose=not args.quiet,
        )
    else:
        result = ensemble_season_analysis(
            location_coord=(lat, lon),
            future_years=(args.future_start, args.future_end),
            scenario=args.scenario,
            models=models,
            exclude_models=exclude,
            gap_days=args.gap_days,
            min_season_days=args.min_season_days,
            verbose=not args.quiet,
        )

    output = json.dumps(result, indent=2, default=str)

    if args.output:
        with open(args.output, 'w') as fh:
            fh.write(output)
        print(f"Results saved to {args.output}")
    else:
        print(output)


if __name__ == '__main__':
    main()
    
# python climate_tookit/season_analysis/ensemble.py --location="-1.286,36.817" --future-start 2040 --future-end 2060 --scenario ssp245
# python climate_tookit/season_analysis/ensemble.py --list-models
# python climate_tookit/season_analysis/ensemble.py --location="-1.286,36.817" --future-start 2040 --future-end 2060 --scenario ssp245 --output results.json
# python climate_tookit/season_analysis/ensemble.py --location="-1.286,36.817" --future-start 2040 --future-end 2060 --all-scenarios --output all_scenarios.json
# python climate_tookit/season_analysis/ensemble.py --location="-1.286,36.817" --future-start 2040 --future-end 2060 --scenario ssp245 --exclude-models "CESM2,INM-CM4-8"