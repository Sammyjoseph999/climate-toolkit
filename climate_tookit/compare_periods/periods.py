"""
Compare Periods Module

Compares climate statistics between different time periods:
1. Baseline (entire period as one dataset)
2. Actual season vs Baseline
3. Future period vs Baseline
"""

import sys
import os
from datetime import datetime
import pandas as pd
import json
import argparse
from typing import Dict, Any, Tuple

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.insert(0, project_root)

from climate_tookit.climate_statistics.statistics import analyze_climate_statistics

# Set pandas display options for 2 decimal places
pd.set_option('display.float_format', lambda x: f'{x:.2f}')


def round_nested_dict(data: Dict[str, Any], decimals: int = 2) -> Dict[str, Any]:
    """Recursively round all numeric values in a nested dictionary."""
    result = {}
    for key, value in data.items():
        if isinstance(value, dict):
            result[key] = round_nested_dict(value, decimals)
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            result[key] = round(value, decimals)
        else:
            result[key] = value
    return result


def calculate_baseline_statistics(
    location_coord: Tuple[float, float],
    baseline_years: Tuple[int, int],
    source: str
) -> Dict[str, Any]:
    """Calculate baseline statistics for entire period as one dataset."""
    lat, lon = location_coord
    start_year, end_year = baseline_years

    print(f"\n{'='*60}")
    print(f"CALCULATING BASELINE ({start_year}-{end_year})")
    print(f"Source: {source}")
    print(f"{'='*60}\n")

    start_date = f"{start_year}-01-01"
    end_date = f"{end_year}-12-31"

    print(f"Processing entire period {start_year}-{end_year}...")

    try:
        result = analyze_climate_statistics(
            location_coord=(lat, lon),
            date_range=(start_date, end_date),
            source=source
        )

        if 'error' in result:
            return {'error': result['error']}

        baseline = result['statistics']['overall_statistics']
        
        # Round all numeric values to 2 decimal places
        baseline = round_nested_dict(baseline, decimals=2)
        
        baseline['baseline_period'] = f"{start_year}-{end_year}"
        baseline['source'] = source
        baseline['total_days'] = baseline.get('total_days', 0)

        print(f"✓ Baseline calculated ({baseline['total_days']:.0f} days)")
        return baseline

    except Exception as e:
        return {'error': str(e)}


def compare_actual_vs_baseline(
    location_coord: Tuple[float, float],
    actual_year: int,
    baseline_stats: Dict[str, Any],
    source: str
) -> Dict[str, Any]:
    """Compare actual year against baseline."""
    lat, lon = location_coord

    print(f"\n{'='*60}")
    print(f"COMPARING ACTUAL YEAR {actual_year} VS BASELINE")
    print(f"Source: {source}")
    print(f"{'='*60}\n")

    start_date = f"{actual_year}-01-01"
    end_date = f"{actual_year}-12-31"

    print(f"Processing {actual_year}...", end=" ")

    try:
        actual_result = analyze_climate_statistics(
            location_coord=(lat, lon),
            date_range=(start_date, end_date),
            source=source
        )

        if 'error' in actual_result:
            return {'error': actual_result['error']}

        print("✓")
        actual_stats = actual_result['statistics']['overall_statistics']
        
        # Round all numeric values to 2 decimal places
        actual_stats = round_nested_dict(actual_stats, decimals=2)

    except Exception as e:
        return {'error': str(e)}

    comparison = {
        'actual_year': actual_year,
        'actual_source': source,
        'baseline_period': baseline_stats.get('baseline_period', 'Unknown'),
        'baseline_source': baseline_stats.get('source', 'Unknown'),
        'variables': {}
    }

    for category in ['precipitation', 'temperature', 'et0', 'water_balance']:
        if category in actual_stats and category in baseline_stats:
            comparison['variables'][category] = {}

            for metric, actual_value in actual_stats[category].items():
                if metric in baseline_stats[category]:
                    baseline_value = baseline_stats[category][metric]
                    diff = actual_value - baseline_value
                    pct = (diff / baseline_value * 100) if baseline_value != 0 else 0

                    comparison['variables'][category][metric] = {
                        'actual': round(actual_value, 2),
                        'baseline': round(baseline_value, 2),
                        'difference': round(diff, 2),
                        'percent_change': round(pct, 2)
                    }

    return comparison


def compare_future_vs_baseline(
    location_coord: Tuple[float, float],
    future_years: Tuple[int, int],
    baseline_stats: Dict[str, Any],
    source: str
) -> Dict[str, Any]:
    """Compare future period against baseline."""
    lat, lon = location_coord
    start_year, end_year = future_years

    print(f"\n{'='*60}")
    print(f"COMPARING FUTURE ({start_year}-{end_year}) VS BASELINE")
    print(f"Source: {source}")
    print(f"{'='*60}\n")

    start_date = f"{start_year}-01-01"
    end_date = f"{end_year}-12-31"

    print(f"Processing entire period {start_year}-{end_year}...")

    try:
        future_result = analyze_climate_statistics(
            location_coord=(lat, lon),
            date_range=(start_date, end_date),
            source=source
        )

        if 'error' in future_result:
            return {'error': future_result['error']}

        future_stats = future_result['statistics']['overall_statistics']
        
        # Round all numeric values to 2 decimal places
        future_stats = round_nested_dict(future_stats, decimals=2)
        
        total_days = future_stats.get('total_days', 0)
        print(f"✓ Future period calculated ({total_days:.0f} days)")

    except Exception as e:
        return {'error': str(e)}

    comparison = {
        'future_period': f"{start_year}-{end_year}",
        'future_source': source,
        'baseline_period': baseline_stats.get('baseline_period', 'Unknown'),
        'baseline_source': baseline_stats.get('source', 'Unknown'),
        'variables': {}
    }

    for category in ['precipitation', 'temperature', 'et0', 'water_balance']:
        if category in future_stats and category in baseline_stats:
            comparison['variables'][category] = {}

            for metric, future_value in future_stats[category].items():
                if metric in baseline_stats[category]:
                    baseline_value = baseline_stats[category][metric]
                    diff = future_value - baseline_value
                    pct = (diff / baseline_value * 100) if baseline_value != 0 else 0

                    comparison['variables'][category][metric] = {
                        'future': round(future_value, 2),
                        'baseline': round(baseline_value, 2),
                        'difference': round(diff, 2),
                        'percent_change': round(pct, 2)
                    }

    return comparison


def print_comparison_report(comparison: Dict[str, Any], comparison_type: str):
    """Print formatted comparison report."""
    print(f"\n{'='*60}")
    print(f"COMPARISON REPORT")
    print(f"{'='*60}")

    if 'error' in comparison:
        print(f"Error: {comparison['error']}")
        return

    if comparison_type == 'actual_vs_baseline':
        print(f"Actual: {comparison['actual_year']} ({comparison.get('actual_source', 'N/A')})")
        print(f"Baseline: {comparison['baseline_period']} ({comparison.get('baseline_source', 'N/A')})")
    elif comparison_type == 'future_vs_baseline':
        print(f"Future: {comparison['future_period']} ({comparison.get('future_source', 'N/A')})")
        print(f"Baseline: {comparison['baseline_period']} ({comparison.get('baseline_source', 'N/A')})")

    print()

    data = []
    key_metrics = ['total_mm', 'mean_daily', 'mean_tmax', 'mean_tmin', 'mean_tavg', 'total_balance']

    for category, metrics in comparison['variables'].items():
        for metric, values in metrics.items():
            if metric in key_metrics:
                row = {
                    'Category': category.title(),
                    'Metric': metric,
                    'Baseline': f"{values['baseline']:.2f}",
                    'Difference': f"{values['difference']:+.2f}",
                    'Change %': f"{values['percent_change']:+.2f}%"
                }

                if comparison_type == 'actual_vs_baseline':
                    row['Actual'] = f"{values['actual']:.2f}"
                else:
                    row['Future'] = f"{values['future']:.2f}"

                data.append(row)

    if data:
        df = pd.DataFrame(data)
        print(df.to_string(index=False))
        print()


def main():
    parser = argparse.ArgumentParser(description='Compare climate periods')

    parser.add_argument('--location', required=True, type=str,
                       help='Location as lat,lon (e.g., -1.286,36.817)')
    parser.add_argument('--comparison-type', required=True,
                       choices=['baseline-only', 'actual-vs-baseline', 'future-vs-baseline'],
                       help='Type of comparison')

    parser.add_argument('--baseline-start', type=int, required=True,
                       help='Baseline start year')
    parser.add_argument('--baseline-end', type=int, required=True,
                       help='Baseline end year')
    parser.add_argument('--baseline-source', required=True,
                       help='Baseline data source')

    parser.add_argument('--actual-year', type=int,
                       help='Actual year (required for actual-vs-baseline)')
    parser.add_argument('--actual-source',
                       help='Actual year data source (required for actual-vs-baseline)')

    parser.add_argument('--future-start', type=int,
                       help='Future start year (required for future-vs-baseline)')
    parser.add_argument('--future-end', type=int,
                       help='Future end year (required for future-vs-baseline)')
    parser.add_argument('--future-source',
                       help='Future data source (required for future-vs-baseline)')

    parser.add_argument('--output', help='Output JSON file')

    args = parser.parse_args()

    # Parse location
    try:
        parts = [p.strip() for p in args.location.replace(' ', ',').split(',') if p.strip()]
        if len(parts) != 2:
            raise ValueError("Need exactly 2 coordinates")
        lat, lon = map(float, parts)
    except (ValueError, AttributeError):
        print("Error: Invalid location. Use format: lat,lon")
        sys.exit(1)

    # Validate required arguments per comparison type
    if args.comparison_type == 'actual-vs-baseline':
        if not args.actual_year or not args.actual_source:
            print("Error: --actual-year and --actual-source required")
            sys.exit(1)
    elif args.comparison_type == 'future-vs-baseline':
        if not args.future_start or not args.future_end or not args.future_source:
            print("Error: --future-start, --future-end, and --future-source required")
            sys.exit(1)

    results = {}

    print(f"\nLocation: {lat}, {lon}")

    # Calculate baseline
    baseline = calculate_baseline_statistics(
        location_coord=(lat, lon),
        baseline_years=(args.baseline_start, args.baseline_end),
        source=args.baseline_source
    )

    if 'error' in baseline:
        print(f"\nError: {baseline['error']}")
        sys.exit(1)

    results['baseline'] = baseline

    if args.comparison_type == 'baseline-only':
        print(f"\n✓ Baseline complete")
        print(f"Period: {baseline['baseline_period']}")
        print(f"Days: {baseline.get('total_days', 0):.0f}")
        print(f"Source: {baseline['source']}")

    elif args.comparison_type == 'actual-vs-baseline':
        comparison = compare_actual_vs_baseline(
            location_coord=(lat, lon),
            actual_year=args.actual_year,
            baseline_stats=baseline,
            source=args.actual_source
        )
        results['comparison'] = comparison
        print_comparison_report(comparison, 'actual_vs_baseline')

    elif args.comparison_type == 'future-vs-baseline':
        comparison = compare_future_vs_baseline(
            location_coord=(lat, lon),
            future_years=(args.future_start, args.future_end),
            baseline_stats=baseline,
            source=args.future_source
        )
        results['comparison'] = comparison
        print_comparison_report(comparison, 'future_vs_baseline')

    if args.output:
        with open(args.output, 'w') as f:
            json.dump(results, indent=2, fp=f, default=str)
        print(f"\n✓ Saved: {args.output}")


if __name__ == "__main__":
    main()

# # Baseline only
# python -m climate_tookit.compare_periods.periods --location=-1.286,36.817 --comparison-type=baseline-only --baseline-start=1991 --baseline-end=2020 --baseline-source=nasa_power

# # Actual vs baseline
# python -m climate_tookit.compare_periods.periods --location=-1.286,36.817 --comparison-type=actual-vs-baseline --baseline-start=1991 --baseline-end=2020 --baseline-source=nasa_power --actual-year=2019 --actual-source=nasa_power

# # Future vs baseline
# python -m climate_tookit.compare_periods.periods --location=-1.286,36.817 --comparison-type=future-vs-baseline --baseline-start=1991 --baseline-end=2020 --baseline-source=nasa_power --future-start=2030 --future-end=2060 --future-source=nex_gddp