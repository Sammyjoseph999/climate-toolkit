"""
Ensemble Hazards Module with NEX-GDDP Integration
"""
import sys
import os
from datetime import datetime, date
from typing import Dict, List, Any, Tuple, Optional
import json
import argparse
from statistics import mean, stdev

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)

sys.path.insert(0, current_dir)

from hazards import calculate_hazards, CROP_THRESHOLDS, calculate_season_statistics, evaluate_threshold
HAZARDS_AVAILABLE = True

PREPROCESS_AVAILABLE = False
try:
    preprocess_path = os.path.join(parent_dir, 'fetch_data', 'preprocess_data')
    sys.path.insert(0, preprocess_path)
    from preprocess_data import preprocess_data

    sources_path = os.path.join(parent_dir, 'fetch_data', 'source_data', 'sources')
    sys.path.insert(0, sources_path)
    from utils.models import ClimateVariable

    PREPROCESS_AVAILABLE = True
    print("✓ NEX-GDDP pipeline available")
except Exception as e:
    print(f"✗ NEX-GDDP pipeline not available: {e}")

AVAILABLE_SCENARIOS = ['SSP1-2.6', 'SSP2-4.5', 'SSP5-8.5']
AVAILABLE_GCMS = [
    'ACCESS-CM2', 'ACCESS-ESM1-5', 'CanESM5', 'CMCC-ESM2',
    'EC-Earth3', 'EC-Earth3-Veg-LR', 'GFDL-ESM4', 'INM-CM4-8',
    'INM-CM5-0', 'KACE-1-0-G', 'MIROC6', 'MPI-ESM1-2-LR',
    'MRI-ESM2-0', 'NorESM2-LM', 'NorESM2-MM', 'TaiESM1'
]

def calculate_single_projection(
    crop_name: str,
    location_coord: Tuple[float, float],
    season_start: str,
    season_end: str,
    scenario: str,
    model: str
) -> Optional[Dict[str, Any]]:

    try:
        lat, lon = location_coord

        # Using NEX-GDDP for future scenarios
        if scenario != 'Historical' and PREPROCESS_AVAILABLE:
            try:
                df = preprocess_data(
                    source='nex_gddp',
                    location_coord=(lat, lon),
                    variables=[ClimateVariable.precipitation, ClimateVariable.max_temperature, ClimateVariable.min_temperature],
                    date_from=date.fromisoformat(season_start),
                    date_to=date.fromisoformat(season_end),
                    model=model,
                    scenario=scenario
                )

                if not df.empty and len(df.columns) > 1:
                    stats = calculate_season_statistics(df)

                    if stats:  # If we got stats
                        crop_name_normalized = crop_name.capitalize()
                        thresholds = CROP_THRESHOLDS.get(crop_name_normalized, {})

                        hazard_eval = {}
                        if 'Total Precip' in thresholds and 'total_precipitation_mm' in stats:
                            precip_value = stats['total_precipitation_mm']
                            precip_status = evaluate_threshold(precip_value, thresholds['Total Precip'])
                            hazard_eval['precipitation'] = {'value_mm': round(precip_value, 2), 'status': precip_status}

                        if 'TAVG' in thresholds and 'mean_temperature_c' in stats:
                            temp_value = stats['mean_temperature_c']
                            temp_status = evaluate_threshold(temp_value, thresholds['TAVG'])
                            hazard_eval['temperature'] = {'value_c': round(temp_value, 2), 'status': temp_status}

                        result = {
                            'crop': crop_name,
                            'location': {'latitude': lat, 'longitude': lon},
                            'season_info': {
                                'season_detected': True,
                                'onset_date': season_start,
                                'cessation_date': season_end,
                                'length_days': (datetime.strptime(season_end, '%Y-%m-%d') - datetime.strptime(season_start, '%Y-%m-%d')).days,
                                'method': 'nex_gddp'
                            },
                            'season_statistics': stats,
                            'hazard_evaluation': hazard_eval,
                            'projection': {'scenario': scenario, 'model': model}
                        }
                        return result
            except Exception as e:
                print(f"NEX-GDDP error: {e}")

        # Fallback to regular hazards calculation
        result = calculate_hazards(
            crop_name=crop_name,
            location_coord=location_coord,
            date_from=season_start,
            date_to=season_end,
            season_start=season_start,
            season_end=season_end
        )

        if 'error' not in result:
            result['projection'] = {'scenario': scenario, 'model': model}

        return result

    except Exception as e:
        print(f"Error: {e}")
        return None

def aggregate_ensemble_results(results: List[Dict[str, Any]], scenario: str) -> Dict[str, Any]:
    if not results:
        return {'error': f'No valid projections for {scenario}'}

    precip_values = [r['season_statistics']['total_precipitation_mm'] for r in results if 'season_statistics' in r]
    temp_values = [r['season_statistics']['mean_temperature_c'] for r in results if 'season_statistics' in r]
    precip_statuses = [r['hazard_evaluation']['precipitation']['status'] for r in results if 'hazard_evaluation' in r and 'precipitation' in r['hazard_evaluation']]
    temp_statuses = [r['hazard_evaluation']['temperature']['status'] for r in results if 'hazard_evaluation' in r and 'temperature' in r['hazard_evaluation']]

    ensemble = {
        'scenario': scenario,
        'n_models': len(results),
        'ensemble_statistics': {
            'precipitation': {
                'mean_mm': mean(precip_values) if precip_values else 0,
                'min_mm': min(precip_values) if precip_values else 0,
                'max_mm': max(precip_values) if precip_values else 0,
                'std_mm': stdev(precip_values) if len(precip_values) > 1 else 0,
            },
            'temperature': {
                'mean_c': mean(temp_values) if temp_values else 0,
                'min_c': min(temp_values) if temp_values else 0,
                'max_c': max(temp_values) if temp_values else 0,
                'std_c': stdev(temp_values) if len(temp_values) > 1 else 0,
            }
        },
        'consensus': {
            'precipitation': {
                'status_distribution': {status: precip_statuses.count(status) for status in set(precip_statuses)},
                'most_common': max(set(precip_statuses), key=precip_statuses.count) if precip_statuses else 'unknown',
                'agreement_pct': (precip_statuses.count(max(set(precip_statuses), key=precip_statuses.count)) / len(precip_statuses) * 100) if precip_statuses else 0
            },
            'temperature': {
                'status_distribution': {status: temp_statuses.count(status) for status in set(temp_statuses)},
                'most_common': max(set(temp_statuses), key=temp_statuses.count) if temp_statuses else 'unknown',
                'agreement_pct': (temp_statuses.count(max(set(temp_statuses), key=temp_statuses.count)) / len(temp_statuses) * 100) if temp_statuses else 0
            }
        },
        'model_projections': results
    }
    return ensemble

def calculate_ensemble_hazards(
    crop_name: str,
    location_coord: Tuple[float, float],
    baseline_start: str,
    baseline_end: str,
    future_start: str,
    future_end: str,
    scenarios: List[str],
    models: List[str]
) -> Dict[str, Any]:

    lat, lon = location_coord

    print(f"\nCalculating ensemble for {crop_name}")
    print(f"Location: ({lat:.4f}, {lon:.4f})")
    print(f"Baseline: {baseline_start} to {baseline_end}")
    print(f"Future:   {future_start} to {future_end}")
    print(f"Scenarios: {len(scenarios)} SSPs")
    print(f"Models: {len(models)} GCMs per scenario")
    print(f"{'='*70}\n")

    print(f"{'='*70}")
    print(f"  BASELINE PERIOD (Historical)")
    print(f"{'='*70}\n")

    print(f"  Calculating baseline...", end=' ')
    baseline_result = calculate_single_projection(
        crop_name=crop_name,
        location_coord=location_coord,
        season_start=baseline_start,
        season_end=baseline_end,
        scenario='Historical',
        model='Observed'
    )

    if baseline_result and 'error' not in baseline_result:
        print("✓\n")
    else:
        print("✗\n")
        return {'error': 'Baseline calculation failed'}

    print(f"{'='*70}")
    print(f"  FUTURE PERIOD (2031-2060)")
    print(f"{'='*70}\n")

    scenario_ensembles = {}

    for scenario in scenarios:
        print(f"\n  {'─'*66}")
        print(f"  Scenario: {scenario}")
        print(f"  {'─'*66}\n")

        results = []
        for i, model in enumerate(models, 1):
            print(f"  [{i}/{len(models)}] {model}...", end=' ')

            result = calculate_single_projection(
                crop_name=crop_name,
                location_coord=location_coord,
                season_start=future_start,
                season_end=future_end,
                scenario=scenario,
                model=model
            )

            if result and 'error' not in result:
                results.append(result)
                print("✓")
            else:
                print("✗")

        print(f"\n  Complete: {len(results)}/{len(models)} models\n")

        ensemble = aggregate_ensemble_results(results, scenario)

        if baseline_result:
            baseline_precip = baseline_result['season_statistics']['total_precipitation_mm']
            baseline_temp = baseline_result['season_statistics']['mean_temperature_c']
            future_precip = ensemble['ensemble_statistics']['precipitation']['mean_mm']
            future_temp = ensemble['ensemble_statistics']['temperature']['mean_c']

            ensemble['change_from_baseline'] = {
                'precipitation': {
                    'absolute_mm': future_precip - baseline_precip,
                    'percent': ((future_precip - baseline_precip) / baseline_precip * 100) if baseline_precip > 0 else 0
                },
                'temperature': {
                    'absolute_c': future_temp - baseline_temp,
                    'percent': ((future_temp - baseline_temp) / baseline_temp * 100) if baseline_temp > 0 else 0
                }
            }

        scenario_ensembles[scenario] = ensemble

    return {
        'crop': crop_name,
        'location': {'latitude': lat, 'longitude': lon},
        'baseline': {
            'period': {'start': baseline_start, 'end': baseline_end},
            'results': baseline_result
        },
        'future': {
            'period': {'start': future_start, 'end': future_end},
            'ensembles': scenario_ensembles
        },
        'scenarios': scenarios,
        'models': models
    }

def print_ensemble_results(result: Dict[str, Any]):
    if 'error' in result:
        print(f"\nError: {result['error']}")
        return

    print(f"\n{'='*70}")
    print(f"  ENSEMBLE HAZARD ASSESSMENT: {result['crop'].upper()}")
    print(f"{'='*70}")
    print(f"  Location: {result['location']['latitude']:.4f}, {result['location']['longitude']:.4f}")
    print(f"  Models: {len(result['models'])} GCMs per scenario")
    print(f"{'='*70}\n")

    baseline = result['baseline']['results']
    baseline_period = result['baseline']['period']
    future_period = result['future']['period']

    print(f"  {'─'*66}")
    print(f"  BASELINE ({baseline_period['start']} to {baseline_period['end']})")
    print(f"  {'─'*66}")

    if baseline:
        b_stats = baseline['season_statistics']
        b_hazards = baseline['hazard_evaluation']

        print(f"    Precipitation: {b_stats['total_precipitation_mm']:.2f} mm")
        precip_status = b_hazards['precipitation']['status'].replace('_', ' ').upper()
        precip_sym = '✓' if 'no_stress' in b_hazards['precipitation']['status'] else '⚠' if 'moderate' in b_hazards['precipitation']['status'] else '✗'
        print(f"      Status: {precip_sym} {precip_status}")

        print(f"    Temperature: {b_stats['mean_temperature_c']:.2f} °C")
        temp_status = b_hazards['temperature']['status'].replace('_', ' ').upper()
        temp_sym = '✓' if 'no_stress' in b_hazards['temperature']['status'] else '⚠' if 'moderate' in b_hazards['temperature']['status'] else '✗'
        print(f"      Status: {temp_sym} {temp_status}")

    print(f"\n  {'─'*66}")
    print(f"  FUTURE ({future_period['start']} to {future_period['end']})")
    print(f"  {'─'*66}\n")

    for scenario, ensemble in result['future']['ensembles'].items():
        if 'error' in ensemble:
            print(f"\n  {scenario}: {ensemble['error']}")
            continue

        stats = ensemble['ensemble_statistics']
        consensus = ensemble['consensus']
        changes = ensemble.get('change_from_baseline', {})

        print(f"\n  {'─'*66}")
        print(f"  SCENARIO: {scenario}")
        print(f"  {'─'*66}")
        print(f"    Models: {ensemble['n_models']}")

        print(f"\n    Precipitation Ensemble")
        print(f"    {'-'*62}")
        print(f"      {'Metric':<28} {'Value':>12}  {'Change':<12}")
        print(f"      {'-'*28} {'-'*12}  {'-'*12}")

        precip_change = changes.get('precipitation', {})
        change_str = f"+{precip_change.get('absolute_mm', 0):.1f}mm" if precip_change.get('absolute_mm', 0) >= 0 else f"{precip_change.get('absolute_mm', 0):.1f}mm"
        pct_str = f"({precip_change.get('percent', 0):+.1f}%)" if precip_change else ""

        print(f"      {'Mean':<28} {stats['precipitation']['mean_mm']:>12.2f}  {change_str} {pct_str}")
        print(f"      {'Range':<28} {stats['precipitation']['min_mm']:>12.2f}  ")
        print(f"      {'':<28} {stats['precipitation']['max_mm']:>12.2f}  ")
        print(f"      {'Std Deviation':<28} {stats['precipitation']['std_mm']:>12.2f}  ")

        print(f"\n    Temperature Ensemble")
        print(f"    {'-'*62}")
        print(f"      {'Metric':<28} {'Value':>12}  {'Change':<12}")
        print(f"      {'-'*28} {'-'*12}  {'-'*12}")

        temp_change = changes.get('temperature', {})
        change_str = f"+{temp_change.get('absolute_c', 0):.2f}°C" if temp_change.get('absolute_c', 0) >= 0 else f"{temp_change.get('absolute_c', 0):.2f}°C"
        pct_str = f"({temp_change.get('percent', 0):+.1f}%)" if temp_change else ""

        print(f"      {'Mean':<28} {stats['temperature']['mean_c']:>12.2f}  {change_str} {pct_str}")
        print(f"      {'Range':<28} {stats['temperature']['min_c']:>12.2f}  ")
        print(f"      {'':<28} {stats['temperature']['max_c']:>12.2f}  ")
        print(f"      {'Std Deviation':<28} {stats['temperature']['std_c']:>12.2f}  ")

        print(f"\n    Consensus")
        print(f"    {'-'*62}")
        precip_status = consensus['precipitation']['most_common'].replace('_', ' ').upper()
        precip_agree = consensus['precipitation']['agreement_pct']
        precip_sym = '✓' if 'no_stress' in consensus['precipitation']['most_common'] else '⚠' if 'moderate' in consensus['precipitation']['most_common'] else '✗'

        temp_status = consensus['temperature']['most_common'].replace('_', ' ').upper()
        temp_agree = consensus['temperature']['agreement_pct']
        temp_sym = '✓' if 'no_stress' in consensus['temperature']['most_common'] else '⚠' if 'moderate' in consensus['temperature']['most_common'] else '✗'

        print(f"      Precipitation: {precip_sym} {precip_status} ({precip_agree:.0f}% agreement)")
        print(f"      Temperature:   {temp_sym} {temp_status} ({temp_agree:.0f}% agreement)")

    print(f"\n{'='*70}\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Calculate ensemble crop hazard indices with baseline comparison')
    parser.add_argument('crop', type=str, help='Crop name')
    parser.add_argument('--location', type=str, required=True, help='Location as "lat,lon"')
    parser.add_argument('--baseline-start', type=str, required=True, help='Baseline start (YYYY-MM-DD)')
    parser.add_argument('--baseline-end', type=str, required=True, help='Baseline end (YYYY-MM-DD)')
    parser.add_argument('--future-start', type=str, required=True, help='Future start (YYYY-MM-DD)')
    parser.add_argument('--future-end', type=str, required=True, help='Future end (YYYY-MM-DD)')
    parser.add_argument('--scenarios', type=str, default='SSP1-2.6,SSP2-4.5,SSP5-8.5',
                       help='Comma-separated SSP scenarios')
    parser.add_argument('--models', type=str,
                       help='Comma-separated GCM models (default: 5 models)')
    parser.add_argument('--format', choices=['json', 'text'], default='text', help='Output format')
    parser.add_argument('--output', type=str, help='Output file')

    args = parser.parse_args()

    lat, lon = map(float, args.location.split(','))
    scenarios = args.scenarios.split(',')

    if args.models:
        models = args.models.split(',')
    else:
        models = ['ACCESS-CM2', 'CanESM5', 'GFDL-ESM4', 'MIROC6', 'MRI-ESM2-0']

    result = calculate_ensemble_hazards(
        crop_name=args.crop,
        location_coord=(lat, lon),
        baseline_start=args.baseline_start,
        baseline_end=args.baseline_end,
        future_start=args.future_start,
        future_end=args.future_end,
        scenarios=scenarios,
        models=models
    )

    if args.format == 'json':
        output = json.dumps(result, indent=2, default=str)
        print(output)
        if args.output:
            with open(args.output, 'w') as f:
                f.write(output)
    else:
        print_ensemble_results(result)
        if args.output:
            with open(args.output, 'w') as f:
                f.write(json.dumps(result, indent=2, default=str))
                
# python -m climate_tookit.calculate_hazards.ensemble_hazards maize --location="-1.286,36.817" --baseline-start 1991-03-01 --baseline-end 1991-06-30 --future-start 2045-03-01 --future-end 2045-06-30 --models ACCESS-CM2,CanESM5,GFDL-ESM4,MIROC6,MRI-ESM2-0

# python -m climate_tookit.calculate_hazards.ensemble_hazards maize --location="-1.286,36.817" --baseline-start 1991-03-01 --baseline-end 1991-06-30 --future-start 2045-03-01 --future-end 2045-06-30 --models ACCESS-CM2,ACCESS-ESM1-5,CanESM5,CMCC-ESM2,EC-Earth3,EC-Earth3-Veg-LR,GFDL-ESM4,INM-CM4-8,INM-CM5-0,KACE-1-0-G,MIROC6,MPI-ESM1-2-LR,MRI-ESM2-0,NorESM2-LM,NorESM2-MM,TaiESM1