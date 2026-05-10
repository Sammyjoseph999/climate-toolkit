"""
NEX-GDDP Ensemble Season Analysis

Loops over (scenario × model), calls seasons.py's existing analysis functions for each combination (after monkey-patching get_climate_data to read NEX-GDDP),
then averages results across models.

Default: ALL 16 NEX-GDDP-CMIP6 models × ALL 4 SSP scenarios.
Use --models / --scenarios / --exclude-models to narrow.
Use --fixed-season for fixed calendar windows (single, two-season, year-crossing).
"""

import argparse, io, json, math, statistics, sys, warnings
from contextlib import contextmanager, redirect_stdout
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

warnings.filterwarnings("ignore")

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import season_analysis.seasons as seasons
from fetch_data.preprocess_data.preprocess_data import preprocess_data

NEX_GDDP_MODELS = [
    'ACCESS-CM2',  'ACCESS-ESM1-5',    'CanESM5',       'CMCC-ESM2',
    'EC-Earth3',   'EC-Earth3-Veg-LR', 'GFDL-ESM4',     'INM-CM4-8',
    'INM-CM5-0',   'KACE-1-0-G',       'MIROC6',        'MPI-ESM1-2-LR',
    'MRI-ESM2-0',  'NorESM2-LM',       'NorESM2-MM',    'TaiESM1',
]
SSP_SCENARIOS   = ['ssp126', 'ssp245', 'ssp585']
NEX_GDDP_SOURCE = 'nex_gddp'

# Route seasons.get_climate_data through NEX-GDDP 
@contextmanager
def use_nex_gddp(model: str, scenario: str):
    """
    Temporarily replace seasons.get_climate_data so any call inside seasons.py reads NEX-GDDP for the given (model, scenario).
    Tracks success/failure so we can surface real fetch errors even though seasons.py's orchestrators catch all exceptions internally.
    """
    state    = {'success': 0, 'fail': 0, 'last_error': None}
    original = seasons.get_climate_data

    def patched(lat, lon, start_date, end_date, force_source=None):
        try:
            df = preprocess_data(
                source         = NEX_GDDP_SOURCE,
                location_coord = (lat, lon),
                date_from      = date.fromisoformat(start_date),
                date_to        = date.fromisoformat(end_date),
                model          = model,
                scenario       = scenario,
            )
            if df is None or df.empty:
                raise RuntimeError("preprocess_data returned empty DataFrame")
            out = pd.DataFrame({
                'date':   pd.to_datetime(df['date']),
                'tmax':   df.get('max_temperature'),
                'tmin':   df.get('min_temperature'),
                'precip': df.get('precipitation'),
            })
            state['success'] += 1
            return out
        except Exception as exc:
            state['fail']      += 1
            state['last_error'] = exc
            raise

    seasons.get_climate_data = patched
    try:
        yield state
    finally:
        seasons.get_climate_data = original

def analyze_one_model(lat, lon, start_year, end_year, model, scenario, fixed_arg):
    """Run seasons.py for one (model, scenario). Returns (seasons_dict, annual_dict)."""
    sink = io.StringIO()                       # swallow seasons.py's chatter
    with redirect_stdout(sink), use_nex_gddp(model, scenario) as state:
        if fixed_arg:
            fixed_defs = seasons.parse_fixed_seasons(fixed_arg)
            s_dict, a_dict = seasons.fetch_and_analyze_years_fixed(
                lat, lon,
                fixed_seasons = fixed_defs,
                start_year    = start_year,
                end_year      = end_year,
                source        = 'nex_gddp',  
            )
        else:
            s_dict, a_dict = seasons.fetch_and_analyze_years(
                lat, lon,
                start_year = start_year,
                end_year   = end_year,
                source     = 'nex_gddp',
            )

    # If every fetch failed, surface the real error
    if state['success'] == 0 and state['last_error'] is not None:
        raise state['last_error']
    return s_dict, a_dict

# Ensemble statistics
def _avg(values):
    """Mean of non-null numeric values, or None."""
    clean = [v for v in values if v is not None and not (isinstance(v, float) and math.isnan(v))]
    if not clean:
        return None
    return statistics.mean(clean)

def _avg_ts_iso(ts_list):
    """Average a list of date-like values as timestamps; return ISO date or None."""
    valid = []
    for t in ts_list:
        if t is None:
            continue
        try:
            ts = pd.Timestamp(t)
            if not pd.isna(ts):
                valid.append(ts.value)   # int64 nanoseconds
        except Exception:
            pass
    if not valid:
        return None
    return pd.Timestamp(sum(valid) // len(valid)).strftime('%Y-%m-%d')

def _aggregate_eto_subseasons(eto_per_model: List[List[Dict]]) -> Dict:
    """
    Aggregate ETO sub-seasons across models for one fixed-season slot.
    Sub-seasons are aligned by their position in each model's list.
    """
    metrics = ['total_rainfall_mm', 'rainy_days', 'dry_days', 'dry_spells', 'length_days']
    n_total    = len(eto_per_model)
    n_with_any = sum(1 for lst in eto_per_model if lst)
    max_subs   = max((len(lst) for lst in eto_per_model), default=0)

    sub_slots = []
    for sub_idx in range(max_subs):
        buckets = {k: [] for k in metrics}
        onsets, cessations, regimes = [], [], []
        n = 0
        for lst in eto_per_model:
            if sub_idx < len(lst):
                s = lst[sub_idx]; n += 1
                for k in metrics: buckets[k].append(s.get(k))
                onsets.append(s.get('onset'))
                cessations.append(s.get('cessation'))
                regimes.append(s.get('regime'))
        regime_counts = {}
        for r_ in regimes:
            if r_: regime_counts[r_] = regime_counts.get(r_, 0) + 1
        n_open = sum(1 for c in cessations if c is None)

        sub_slots.append({
            'subseason_index':  sub_idx + 1,
            'n_models':         n,
            'regime_counts':    regime_counts,
            'avg_onset':        _avg_ts_iso(onsets),
            'avg_cessation':    _avg_ts_iso(cessations),
            'n_open_cessation': n_open,
            **{k: _avg(buckets[k]) for k in metrics},
        })
    return {
        'n_models_with_any': n_with_any,
        'n_models_total':    n_total,
        'subseasons':        sub_slots,
    }

def aggregate(model_results: List[Dict]) -> Dict[int, Dict]:
    """Average per-model {seasons_dict, annual_dict} into ensemble stats by year."""
    years = sorted({y for r in model_results for y in r.get('seasons_dict', {})})
    out   = {}
    metrics = ['total_rainfall_mm', 'rainy_days', 'dry_days', 'dry_spells', 'length_days']

    for year in years:
        max_n = max((len(r['seasons_dict'].get(year, [])) for r in model_results), default=0)
        seasons_agg = []
        for idx in range(max_n):
            buckets = {k: [] for k in metrics}
            onsets, cessations, regimes = [], [], []
            eto_per_model = []
            n = 0
            for r in model_results:
                slist = r['seasons_dict'].get(year, [])
                if idx < len(slist):
                    s = slist[idx]; n += 1
                    for k in metrics: buckets[k].append(s.get(k))
                    onsets.append(s.get('onset'))
                    cessations.append(s.get('cessation'))
                    regimes.append(s.get('regime'))
                    eto_per_model.append(s.get('eto_seasons') or [])
            regime_counts = {}
            for r_ in regimes:
                if r_: regime_counts[r_] = regime_counts.get(r_, 0) + 1
            n_open = sum(1 for c in cessations if c is None)

            seasons_agg.append({
                'season_index':     idx + 1,
                'n_models':         n,
                'avg_onset':        _avg_ts_iso(onsets),
                'avg_cessation':    _avg_ts_iso(cessations),
                'n_open_cessation': n_open,
                'regime_counts':    regime_counts,
                'eto_subseasons':   _aggregate_eto_subseasons(eto_per_model),
                **{k: _avg(buckets[k]) for k in metrics},
            })

        annual_rain, low_rain_months = [], []
        humid_n = humid_total = 0
        for r in model_results:
            ann = r['annual_dict'].get(year, {})
            if ann.get('annual_rain_mm') is not None:
                annual_rain.append(ann['annual_rain_mm'])
                humid_total += 1
                if ann.get('low_rain_months') is not None:
                    low_rain_months.append(ann['low_rain_months'])
                if ann.get('is_humid'):
                    humid_n += 1

        out[year] = {
            'seasons':         seasons_agg,
            'annual_rain_mm':  _avg(annual_rain),
            'low_rain_months': _avg(low_rain_months),
            'humid_n':         humid_n,
            'humid_total':     humid_total,
        }
    return out

# Top-level orchestrator
def run_ensemble(lat, lon, start_year, end_year, scenarios, models, fixed_arg=None, verbose=True):
    results = {}
    mode    = 'fixed' if fixed_arg else 'auto'

    for scenario in scenarios:
        if verbose:
            print(f"\n{'=' * 70}")
            print(f"Scenario: {scenario}  |  {len(models)} model(s)  |  "
                  f"{start_year}–{end_year}  |  mode={mode}")
            print('=' * 70)

        per_model = []
        for i, model in enumerate(models, 1):
            if verbose:
                print(f"  [{i:02d}/{len(models):02d}] {model:<22}", end=' ', flush=True)
            try:
                s_dict, a_dict = analyze_one_model(
                    lat, lon, start_year, end_year, model, scenario, fixed_arg)
                per_model.append({'model': model, 'seasons_dict': s_dict, 'annual_dict': a_dict})
                if verbose:
                    n_seasons = sum(len(v) for v in s_dict.values())
                    n_years   = sum(1 for v in s_dict.values() if v)
                    print(f"✓  {n_seasons} season(s) over {n_years} year(s)")
            except Exception as exc:
                per_model.append({
                    'model': model, 'seasons_dict': {}, 'annual_dict': {},
                    'error': f"{type(exc).__name__}: {exc}",
                })
                if verbose:
                    print(f"✗  {type(exc).__name__}: {exc}")

        ok = sum(1 for r in per_model if not r.get('error'))
        results[scenario] = {
            'by_year':       aggregate(per_model),
            'model_results': per_model,
            'metadata': {
                'lat': lat, 'lon': lon,
                'period':         [start_year, end_year],
                'scenario':       scenario,
                'models':         models,
                'models_ok':      ok,
                'models_failed':  len(models) - ok,
                'mode':           mode,
                'fixed_seasons':  fixed_arg,
                'data_source':    'NEX-GDDP-CMIP6',
                'source_key':     NEX_GDDP_SOURCE,
                'analysis_date':  datetime.now().isoformat(),
            },
        }
        if verbose:
            print(f"\n  Ensemble: {ok}/{len(models)} models succeeded")
    return results

# Pretty printer (mirrors seasons.py print_summary exactly, averaged)
def _mm(v):  return f"{v:.1f} mm" if v is not None else "n/a"
def _d(v):   return f"{int(round(v))} days" if v is not None else "n/a"
def _ct(v):  return f"{int(round(v))}" if v is not None else "n/a"
def _len(v): return f"{int(round(v))}d" if v is not None else "?d"

def _humid_line(annual, low_months):
    """Apply the humid test (annual > 1400 AND low-rain months ≤ 3) to ensemble means."""
    HUMID_RAIN, HUMID_LRM = 1400, 3
    if annual is None or low_months is None:
        return "n/a"
    is_humid = annual > HUMID_RAIN and low_months <= HUMID_LRM
    rain_op  = ">"  if annual > HUMID_RAIN else "≤"
    lrm_op   = "≤"  if low_months <= HUMID_LRM else ">"
    label    = "Humid" if is_humid else "Not humid"
    return (f"{label}  (annual={annual:.1f} mm {rain_op} {HUMID_RAIN} mm, "
            f"low-rain months={int(round(low_months))} {lrm_op} {HUMID_LRM})")

def print_summary(results):
    print("\n" + "=" * 70)
    print("FINAL SEASONS SUMMARY  (ENSEMBLE)")
    print("=" * 70)

    for scenario, payload in results.items():
        n_models = len(payload['metadata']['models'])
        mode     = payload['metadata']['mode']
        print(f"\n{'━' * 70}")
        print(f"Scenario: {scenario}  (mode={mode}, {n_models} model(s), values are averages)")
        print('━' * 70)

        for year in sorted(payload['by_year']):
            yr = payload['by_year'][year]
            print(f"\nYear {year}: {len(yr['seasons'])} season(s)")

            for s in yr['seasons']:
                idx    = s['season_index']
                regime = max(s['regime_counts'], key=s['regime_counts'].get) if s['regime_counts'] else "?"
                onset  = s['avg_onset'] or "?"
                cess   = s['avg_cessation'] or "open"
                if s['n_open_cessation'] and s['n_open_cessation'] > s['n_models'] / 2:
                    cess = "open"
                print(f"  Season {idx}: {onset} → {cess} | {regime} | {_len(s['length_days'])}")
                print(f"    Total rainfall : {_mm(s['total_rainfall_mm'])}")
                print(f"    Rainy days     : {_d(s['rainy_days'])}  (precip ≥ 1 mm)")
                print(f"    Dry days       : {_d(s['dry_days'])}  (precip < 1 mm)")
                print(f"    Dry spells     : {_ct(s['dry_spells'])}  (runs of ≥ 7 consecutive dry days)")

                # ETO sub-season block (fixed mode)
                eto = s.get('eto_subseasons') or {}
                if eto.get('n_models_total'):
                    n_any, n_tot = eto['n_models_with_any'], eto['n_models_total']
                    print(f"    {'─' * 50}")
                    print(f"    Season analysis within fixed window:")
                    if n_any == 0 or not eto['subseasons']:
                        print(f"      No ETO-based season detected within window")
                    else:
                        for sub in eto['subseasons']:
                            sn, sn_n = sub['subseason_index'], sub['n_models']
                            if sn_n == 0:
                                continue
                            sub_regime = (max(sub['regime_counts'], key=sub['regime_counts'].get)
                                          if sub['regime_counts'] else "?")
                            sub_onset = sub['avg_onset'] or "?"
                            sub_cess  = sub['avg_cessation'] or "open"
                            if sub.get('n_open_cessation', 0) > sn_n / 2:
                                sub_cess = "open"
                            print(f"      ETO sub-season {sn}: {sub_onset} → {sub_cess} | "
                                  f"{sub_regime} | {_len(sub['length_days'])}  "
                                  f"(detected by {sn_n}/{n_tot} models)")
                            print(f"        Total rainfall : {_mm(sub['total_rainfall_mm'])}")
                            print(f"        Rainy days     : {_d(sub['rainy_days'])}  (precip ≥ 1 mm)")
                            print(f"        Dry days       : {_d(sub['dry_days'])}  (precip < 1 mm)")
                            print(f"        Dry spells     : {_ct(sub['dry_spells'])}  (runs of ≥ 7 consecutive dry days)")

            # year footer (matches seasons.py format)
            print(f"  {'─' * 48}")
            print(f"  Annual total rainfall : {_mm(yr['annual_rain_mm'])}")
            print(f"  Humid test            : {_humid_line(yr['annual_rain_mm'], yr['low_rain_months'])}")

# CLI 
def main():
    global NEX_GDDP_SOURCE

    if '--list-models' in sys.argv:
        print("Available NEX-GDDP-CMIP6 models:")
        for i, m in enumerate(NEX_GDDP_MODELS, 1):
            print(f"  {i:02d}. {m}")
        sys.exit(0)

    p = argparse.ArgumentParser(
        description='NEX-GDDP-CMIP6 ensemble — wraps seasons.py per model and averages',
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p.add_argument('--location',     required=True, help='lat,lon  e.g. "-1.286,36.817"')
    p.add_argument('--start-year',   type=int, required=True)
    p.add_argument('--end-year',     type=int, required=True)
    p.add_argument('--scenarios',    help=f"Comma-separated. Default: ALL ({','.join(SSP_SCENARIOS)})")
    p.add_argument('--models',       help=f"Comma-separated. Default: ALL {len(NEX_GDDP_MODELS)} models")
    p.add_argument('--exclude-models', help='Comma-separated models to drop')
    p.add_argument('--fixed-season', metavar='MM-DD:MM-DD[,MM-DD:MM-DD]',
        help="Fixed windows: '03-01:05-31', '03-01:05-31,10-01:12-15', '11-01:02-28' (year-crossing)")
    p.add_argument('--source-key',
        help=f"Override preprocess_data source key (default: {NEX_GDDP_SOURCE!r})")
    p.add_argument('--list-models',  action='store_true', help='Print models and exit')
    p.add_argument('--output',       help='Save JSON result here')
    p.add_argument('--quiet',        action='store_true')
    args = p.parse_args()

    if args.source_key:
        NEX_GDDP_SOURCE = args.source_key

    try:
        lat, lon = map(float, args.location.split(','))
    except ValueError:
        print("Error: --location must be in 'lat,lon' format."); sys.exit(1)

    scenarios = ([s.strip() for s in args.scenarios.split(',') if s.strip()]
                 if args.scenarios else list(SSP_SCENARIOS))
    invalid = [s for s in scenarios if s not in SSP_SCENARIOS]
    if invalid:
        print(f"Error: unknown scenario(s) {invalid}. Valid: {SSP_SCENARIOS}"); sys.exit(1)

    models = ([m.strip() for m in args.models.split(',') if m.strip()]
              if args.models else list(NEX_GDDP_MODELS))
    if args.exclude_models:
        excl   = {m.strip().upper() for m in args.exclude_models.split(',') if m.strip()}
        models = [m for m in models if m.upper() not in excl]
    if not models:
        print("Error: model list is empty."); sys.exit(1)

    if not args.quiet:
        print(f"NEX-GDDP Ensemble | {lat:.4f},{lon:.4f} | {args.start_year}–{args.end_year}")
        print(f"  Mode       : {'fixed-season' if args.fixed_season else 'auto'}")
        print(f"  Source key : {NEX_GDDP_SOURCE!r}  (override with --source-key)")
        print(f"  Scenarios  : {', '.join(scenarios)}  ({len(scenarios)})")
        print(f"  Models     : {len(models)} / {len(NEX_GDDP_MODELS)}")

    results = run_ensemble(
        lat, lon, args.start_year, args.end_year,
        scenarios = scenarios, models = models,
        fixed_arg = args.fixed_season,
        verbose   = not args.quiet,
    )

    if not args.quiet:
        print_summary(results)
    if args.output:
        with open(args.output, 'w') as fh:
            fh.write(json.dumps(results, indent=2, default=str))
        print(f"\n✓ Saved to {args.output}")

if __name__ == '__main__':
    main()

# NOTE: the 1st command in a section includes all models/scenarios while the 2nd allows selection

# Automatic detection
# python climate_tookit/season_analysis/ensemble.py --location="-1.286,36.817" --start-year 2040 --end-year 2060 --output ensemble_auto_all.json
# python climate_tookit/season_analysis/ensemble.py --location="-1.286,36.817" --start-year 2040 --end-year 2060 --models "ACCESS-CM2,EC-Earth3,MRI-ESM2-0" --scenarios ssp245,ssp585 --output ensemble_auto.json

# Fixed single season
# python climate_tookit/season_analysis/ensemble.py --location="-1.286,36.817" --start-year 2040 --end-year 2060 --fixed-season "03-01:05-31" --output ensemble_mam_all.json
# python climate_tookit/season_analysis/ensemble.py --location="-1.286,36.817" --start-year 2040 --end-year 2060 --fixed-season "03-01:05-31" --models "ACCESS-CM2,EC-Earth3,MRI-ESM2-0" --scenarios ssp585 --output ensemble_mam.json

# Fixed two seasons
# python climate_tookit/season_analysis/ensemble.py --location="-1.286,36.817" --start-year 2040 --end-year 2060 --fixed-season "03-01:05-31,10-01:12-15" --output ensemble_mam_ond_all.json
# python climate_tookit/season_analysis/ensemble.py --location="-1.286,36.817" --start-year 2040 --end-year 2060 --fixed-season "03-01:05-31,10-01:12-15" --models "ACCESS-CM2,EC-Earth3,MRI-ESM2-0" --scenarios ssp245,ssp585 --output ensemble_mam_ond.json

# Fixed year-crossing season
# python climate_tookit/season_analysis/ensemble.py --location="-1.286,36.817" --start-year 2040 --end-year 2060 --fixed-season "11-01:02-28" --output ensemble_njf_all.json
# python climate_tookit/season_analysis/ensemble.py --location="-1.286,36.817" --start-year 2040 --end-year 2060 --fixed-season "11-01:02-28" --models "ACCESS-CM2,EC-Earth3,MRI-ESM2-0" --scenarios ssp585 --output ensemble_njf.json

# python climate_tookit/season_analysis/ensemble.py --list-models