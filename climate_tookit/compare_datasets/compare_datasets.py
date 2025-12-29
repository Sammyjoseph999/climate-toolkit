"""
compare_datasets.py

Extended dataset comparison module with:
- inter-annual statistics per dataset
- annual time series plots per dataset (PNG)
- multi-source annual time series comparison plots (PNG)
- monthly climatology per dataset
- monthly climatology plots per dataset (PNG)
- pairwise climatology correlations (monthly means: correlation, RMSE, bias)
- overall period statistics (mean, max, min, std, CV) per dataset

Usage (example):
python -m climate_tookit.compare_datasets.compare_datasets \
    --sources era_5 terraclimate chirps \
    --lat -1.286 --lon 36.817 \
    --start 1991-01-01 --end 2020-12-31 \
    --format report \
    --output-dir /path/to/outputs
"""
import sys
import os
from datetime import date
import pandas as pd
import numpy as np
import json
import argparse
from typing import Dict, List, Tuple

import matplotlib
matplotlib.use("Agg")  # headless backend
import matplotlib.pyplot as plt

# Ensure project root is on path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.insert(0, project_root)

from climate_tookit.fetch_data.preprocess_data.preprocess_data import preprocess_data
from climate_tookit.fetch_data.source_data.sources.utils.models import ClimateVariable


# ----------------------
# Utilities / processing
# ----------------------

DEFAULT_VARS = [
    "precipitation",
    "max_temperature",
    "min_temperature",
    "solar_radiation",
    "wind_speed",
    "humidity",
]

def ensure_datetime(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure dataframe has a 'date' column of dtype datetime64[ns].
    """
    if 'date' not in df.columns:
        # try to find a time-like column
        for c in df.columns:
            if 'date' in c.lower() or 'time' in c.lower() or 'timestamp' in c.lower():
                df = df.rename(columns={c: 'date'})
                break
    if 'date' not in df.columns:
        raise RuntimeError("DataFrame has no 'date' column and none could be inferred.")
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    return df

# ----------------------
# Analysis functions
# ----------------------

def compute_overall_statistics(df: pd.DataFrame, variables: List[str] = None) -> Dict:
    """
    Compute overall statistics for the entire period for each variable:
      - mean, max, min, std, cv (std/mean*100)
    Returns: { var: {mean, max, min, std, cv} }
    """
    df = ensure_datetime(df).copy()
    if variables is None:
        variables = [c for c in df.columns if c != 'date']
    
    stats = {}
    for var in variables:
        if var not in df.columns:
            continue
        
        var_data = df[var].dropna()
        if len(var_data) == 0:
            continue
            
        mean_val = float(var_data.mean())
        max_val = float(var_data.max())
        min_val = float(var_data.min())
        std_val = float(var_data.std())
        cv_val = (std_val / mean_val * 100) if mean_val != 0 else None
        
        stats[var] = {
            'mean': round(mean_val, 2),
            'max': round(max_val, 2),
            'min': round(min_val, 2),
            'std': round(std_val, 2),
            'cv': round(cv_val, 2) if cv_val is not None else None
        }
    
    return stats


def compute_interannual_statistics(df: pd.DataFrame, variables: List[str] = None, start_year: int = None, end_year: int = None) -> Dict:
    """
    Compute yearly stats for each variable:
      - mean, max, min, std, cv (std/mean*100)
    Optionally restrict to start_year..end_year (inclusive).
    Returns: { var: { year: {mean, max, min, std, cv} } }
    """
    df = ensure_datetime(df).copy()
    if variables is None:
        variables = [c for c in df.columns if c != 'date']
    if start_year is not None:
        df = df[df['date'].dt.year >= int(start_year)]
    if end_year is not None:
        df = df[df['date'].dt.year <= int(end_year)]

    stats = {}
    df['year'] = df['date'].dt.year
    for var in variables:
        if var not in df.columns:
            continue
        if var == "precipitation":
            # annual totals for precipitation
            grouped_sum = df.groupby('year')[var].sum()
            grouped_max = df.groupby('year')[var].max()
            grouped_min = df.groupby('year')[var].min()
            grouped_std = df.groupby('year')[var].std()
            summary = pd.DataFrame({
                'mean': grouped_sum,
                'max': grouped_max,
                'min': grouped_min,
                'std': grouped_std
            })
        else:
            grouped_mean = df.groupby('year')[var].mean()
            grouped_max = df.groupby('year')[var].max()
            grouped_min = df.groupby('year')[var].min()
            grouped_std = df.groupby('year')[var].std()
            summary = pd.DataFrame({
                'mean': grouped_mean,
                'max': grouped_max,
                'min': grouped_min,
                'std': grouped_std
            })

        summary['cv'] = (summary['std'] / summary['mean']).replace([np.inf, -np.inf], np.nan) * 100
        stats[var] = {int(y): {k: (None if pd.isna(vk) else float(vk)) for k, vk in row.items()} for y, row in summary.fillna(np.nan).iterrows()}

    if 'year' in df.columns:
        df.drop(columns=['year'], inplace=True)
    return stats


def compute_monthly_climatology(df: pd.DataFrame, variables: List[str] = None) -> Dict:
    """
    Compute 12-month climatology (monthly means) for each variable.
    Returns dict: { variable: {1: mean, 2: mean, ..., 12: mean} }
    """
    df = ensure_datetime(df).copy()
    if variables is None:
        variables = [c for c in df.columns if c != 'date']
    df['month'] = df['date'].dt.month
    climatology = {}
    for var in variables:
        if var not in df.columns:
            continue
        grouped = df.groupby('month')[var].mean().reindex(range(1, 13))
        climatology[var] = {int(m): (None if pd.isna(v) else float(v)) for m, v in grouped.items()}
    return climatology


def compute_climatology_correlations(monthly_clim_stats: Dict[str, Dict], pairs: List[Tuple[str, str]]) -> Dict:
    """
    Given per-source monthly climatology dicts and list of pairs (tuples of two source names),
    compute correlation, RMSE, and bias for each variable between each pair.

    monthly_clim_stats: { source: { var: {1: v, ... 12: v} } }
    pairs: list of tuples (source_a, source_b)
    """
    results = []
    for (a, b) in pairs:
        stats_a = monthly_clim_stats.get(a, {})
        stats_b = monthly_clim_stats.get(b, {})
        pair_res = {'pair': f"{a} vs {b}", 'variables': {}}
        common_vars = set(stats_a.keys()) & set(stats_b.keys())
        for var in common_vars:
            arr_a = np.array([stats_a[var].get(m, np.nan) for m in range(1, 13)], dtype=float)
            arr_b = np.array([stats_b[var].get(m, np.nan) for m in range(1, 13)], dtype=float)
            mask = ~np.isnan(arr_a) & ~np.isnan(arr_b)
            if mask.sum() < 2:
                continue
            a_vals = arr_a[mask]
            b_vals = arr_b[mask]
            corr = float(np.corrcoef(a_vals, b_vals)[0, 1]) if len(a_vals) > 1 else None
            rmse = float(np.sqrt(np.mean((a_vals - b_vals) ** 2)))
            bias = float((a_vals - b_vals).mean())
            pair_res['variables'][var] = {
                'correlation': None if corr is None or np.isnan(corr) else corr,
                'rmse': rmse,
                'bias': bias
            }
        results.append(pair_res)
    return results


# ----------------------
# Plotting functions
# ----------------------

def plot_annual_timeseries(df: pd.DataFrame, source: str, out_dir: str, start_year: int, end_year: int, variables: List[str] = None):
    """
    Save per-source annual time series plots for each variable.
    Uses the provided start_year..end_year inclusive to clip the series.
    Files saved as: {out_dir}/annual_timeseries/{source}_{var}_annual.png
    """
    df = ensure_datetime(df).copy()
    if variables is None:
        variables = [c for c in df.columns if c != 'date']

    save_dir = os.path.join(out_dir, 'annual_timeseries')
    os.makedirs(save_dir, exist_ok=True)

    df = df.set_index('date')
    years = list(range(start_year, end_year + 1))

    for var in variables:
        if var not in df.columns:
            continue
        if var == 'precipitation':
            series = df[var].resample('YE').sum()
            ylabel = 'Annual precipitation (mm)'
            title_agg = 'Annual sum'
        else:
            series = df[var].resample('YE').mean()
            ylabel = var
            title_agg = 'Annual mean'

        series_index_years = [ts.year for ts in series.index]
        series_by_year = pd.Series(data=series.values, index=series_index_years)
        series_by_year = series_by_year.reindex(years)

        plt.figure(figsize=(10, 4.5))
        plt.plot(years, series_by_year.values, marker='o', linewidth=1)
        plt.title(f"{source} — {var} ({title_agg}) [{start_year}–{end_year}]")
        plt.xlabel("Year")
        plt.ylabel(ylabel)
        plt.xticks(years[::max(1, len(years)//10)])
        plt.grid(True)
        plt.tight_layout()

        out_path = os.path.join(save_dir, f"{source}_{var}_annual_{start_year}_{end_year}.png")
        plt.savefig(out_path)
        plt.close()


def plot_monthly_climatology(climatology: Dict, source: str, out_dir: str):
    """
    Save 12-month climatology plots for each variable.
    climatology: { var: {1: v, 2: v, ...} }
    Files saved as: {out_dir}/monthly_climatology/{source}_{var}_monthly_climatology.png
    """
    save_dir = os.path.join(out_dir, 'monthly_climatology')
    os.makedirs(save_dir, exist_ok=True)

    months = list(range(1, 13))
    for var, month_map in climatology.items():
        values = [month_map.get(m, np.nan) for m in months]

        plt.figure(figsize=(8, 4.5))
        plt.plot(months, values, marker='o', linewidth=1)
        plt.title(f"{source} — {var} (Monthly climatology)")
        plt.xlabel("Month")
        plt.xticks(months)
        plt.ylabel(var)
        plt.grid(True)
        plt.tight_layout()

        out_path = os.path.join(save_dir, f"{source}_{var}_monthly_climatology.png")
        plt.savefig(out_path)
        plt.close()


def plot_annual_timeseries_multi(dataframes: Dict[str, pd.DataFrame], sources: List[str], variables: List[str], out_dir: str, start_year: int, end_year: int):
    """
    Create a multi-source comparison plot for each variable across sources.
    Saves files to {out_dir}/annual_timeseries_multi/{var}_comparison_{start}_{end}.png
    """
    save_dir = os.path.join(out_dir, 'annual_timeseries_multi')
    os.makedirs(save_dir, exist_ok=True)

    years = list(range(start_year, end_year + 1))
    for var in variables:
        plt.figure(figsize=(10, 5))
        plotted_any = False
        for src in sources:
            df = dataframes.get(src)
            if df is None:
                continue
            try:
                df = ensure_datetime(df).copy()
            except RuntimeError:
                continue
            if var not in df.columns:
                continue

            df = df.set_index('date')
            if var == 'precipitation':
                series = df[var].resample('YE').sum()
            else:
                series = df[var].resample('YE').mean()
            series_index_years = [ts.year for ts in series.index]
            series_by_year = pd.Series(data=series.values, index=series_index_years).reindex(years)

            plt.plot(years, series_by_year.values, marker='o', linewidth=1.5, label=src)
            plotted_any = True

        if not plotted_any:
            plt.close()
            continue

        plt.title(f"Annual {var} comparison ({start_year}–{end_year})")
        plt.xlabel("Year")
        plt.ylabel(var if var != 'precipitation' else 'Annual precipitation (mm)')
        plt.xticks(years[::max(1, len(years)//10)])
        plt.grid(True)
        plt.legend()
        plt.tight_layout()

        out_path = os.path.join(save_dir, f"{var}_comparison_{start_year}_{end_year}.png")
        plt.savefig(out_path)
        plt.close()


# ----------------------
# Integration with fetch/compare
# ----------------------

def fetch_source(source, lat, lon, start, end, variables=None, output_dir: str = './outputs'):
    """
    Fetch preprocessed data from one source and compute per-source outputs.
    Returns a dict with results and metadata.
    """
    try:
        source_variables = [
            ClimateVariable.precipitation,
            ClimateVariable.max_temperature,
            ClimateVariable.min_temperature
        ] if variables is None else variables

        df = preprocess_data(
            source=source,
            location_coord=(lon, lat),
            variables=source_variables,
            date_from=date.fromisoformat(start),
            date_to=date.fromisoformat(end)
        )

        df = ensure_datetime(df)

        if 'date' not in df.columns:
            raise RuntimeError("Fetched dataframe lacks a 'date' column.")

        var_names = [c for c in df.columns if c != 'date']
        if not var_names:
            raise RuntimeError("Fetched dataframe contains no variable columns.")

        start_year = date.fromisoformat(start).year
        end_year = date.fromisoformat(end).year

        # Compute overall statistics
        overall_stats = compute_overall_statistics(df, variables=var_names)
        
        # Compute interannual stats & monthly climatology
        interannual = compute_interannual_statistics(df, variables=var_names, start_year=start_year, end_year=end_year)
        monthly_clim = compute_monthly_climatology(df, variables=var_names)

        # Produce per-source plots
        plot_annual_timeseries(df, source, output_dir, start_year=start_year, end_year=end_year, variables=var_names)
        plot_monthly_climatology(monthly_clim, source, output_dir)

        print(f"\n{source}:")
        print(f"  Shape: {df.shape}")
        df.info()

        return {
            'source': source,
            'data': df,
            'success': True,
            'overall_statistics': overall_stats,
            'interannual': interannual,
            'monthly_climatology': monthly_clim,
            'plots': {
                'annual_timeseries_dir': os.path.join(output_dir, 'annual_timeseries'),
                'monthly_climatology_dir': os.path.join(output_dir, 'monthly_climatology'),
            }
        }

    except Exception as e:
        print(f"\n{source}: Failed - {str(e)}")
        return {'source': source, 'success': False, 'error': str(e)}


def compare_sources(sources, lat, lon, start, end, output_dir: str = './outputs'):
    """
    Compare multiple sources and compute various statistics and correlations.
    """
    os.makedirs(output_dir, exist_ok=True)

    results = []
    for source in sources:
        result = fetch_source(source, lat, lon, start, end, output_dir=output_dir)
        results.append(result)

    successful = [r for r in results if r.get('success')]

    if len(successful) < 1:
        return {
            'results': results,
            'comparisons': [],
            'overall_statistics': {},
            'interannual': {},
            'monthly_climatology': {},
            'climatology_correlations': []
        }

    # Aggregate overall statistics
    overall_stats_agg = {r['source']: r.get('overall_statistics', {}) for r in successful}

    # Aggregate monthly climatologies per source for pairwise comparison
    monthly_clim_stats = {r['source']: r.get('monthly_climatology', {}) for r in successful}

    # Prepare pairs for climatology correlations
    pairs = []
    for i, a in enumerate(successful):
        for b in successful[i+1:]:
            pairs.append((a['source'], b['source']))

    climatology_correlations = compute_climatology_correlations(monthly_clim_stats, pairs)

    # Compute daily comparisons on merged timeseries
    comparisons = []
    for i, r1 in enumerate(successful):
        for r2 in successful[i+1:]:
            df1 = r1['data']
            df2 = r2['data']

            common_cols = set(df1.columns) & set(df2.columns)
            common_cols.discard('date')

            if not common_cols:
                continue

            merged = pd.merge(df1, df2, on='date', suffixes=('_1', '_2'))

            comp = {'pair': f"{r1['source']} vs {r2['source']}"}

            for col in common_cols:
                col1 = f"{col}_1"
                col2 = f"{col}_2"

                if col1 in merged.columns and col2 in merged.columns:
                    v1 = merged[col1].dropna()
                    v2 = merged[col2].dropna()

                    if len(v1) > 1 and len(v2) > 1:
                        corr = v1.corr(v2)
                        rmse = np.sqrt(np.mean((v1 - v2) ** 2))
                        bias = (v1 - v2).mean()

                        comp[col] = {
                            'correlation': float(corr) if not np.isnan(corr) else None,
                            'rmse': float(rmse),
                            'bias': float(bias)
                        }

            comparisons.append(comp)

    # Collect interannual and monthly climatology aggregated outputs
    interannual_agg = {r['source']: r.get('interannual', {}) for r in successful}
    monthly_clim_agg = {r['source']: r.get('monthly_climatology', {}) for r in successful}

    # Produce multi-source annual comparison plots
    union_vars = set()
    dataframes = {}
    for r in successful:
        src = r['source']
        dataframes[src] = r['data']
        union_vars.update([c for c in r['data'].columns if c != 'date'])
    union_vars = sorted(list(union_vars))

    start_year = date.fromisoformat(start).year
    end_year = date.fromisoformat(end).year

    if union_vars:
        plot_annual_timeseries_multi(dataframes, [r['source'] for r in successful], union_vars, output_dir, start_year, end_year)

    return {
        'results': results,
        'comparisons': comparisons,
        'overall_statistics': overall_stats_agg,
        'interannual': interannual_agg,
        'monthly_climatology': monthly_clim_agg,
        'climatology_correlations': climatology_correlations
    }


# ----------------------
# Reporting / CLI
# ----------------------

def print_report(data):
    """
    Print a human-readable report summarizing all statistics and comparisons.
    """
    print("\n" + "=" * 60)
    print("DATASET COMPARISON")
    print("=" * 60)

    print("\nSOURCES")
    print("-" * 40)
    for r in data['results']:
        if r.get('success'):
            df = r['data']
            print(f"{r['source']:15s} {df.shape[0]:4d} records")
        else:
            print(f"{r['source']:15s} FAILED")

    # Print overall statistics
    if data.get('overall_statistics'):
        print("\n" + "=" * 60)
        print("OVERALL STATISTICS (ENTIRE PERIOD)")
        print("=" * 60)
        for source, stats in data['overall_statistics'].items():
            print(f"\n{source}:")
            for var, values in stats.items():
                print(f"  {var}:")
                print(f"    Mean: {values.get('mean')}  Max: {values.get('max')}  Min: {values.get('min')}")
                print(f"    Std: {values.get('std')}  CV: {values.get('cv')}%")

    if data.get('comparisons'):
        print("\n" + "=" * 60)
        print("DAILY TIMESERIES COMPARISONS")
        print("=" * 60)
        for comp in data['comparisons']:
            print(f"\n{comp.get('pair')}:")
            for key, val in comp.items():
                if key != 'pair' and isinstance(val, dict):
                    r = val.get('correlation')
                    rmse = val.get('rmse')
                    bias = val.get('bias')
                    r_str = f"{r:.3f}" if r is not None else "nan"
                    rmse_str = f"{rmse:.2f}" if rmse is not None else "nan"
                    bias_str = f"{bias:.2f}" if bias is not None else "nan"
                    print(f"  {key:20s} r={r_str}  RMSE={rmse_str}  bias={bias_str}")

    if data.get('climatology_correlations'):
        print("\n" + "=" * 60)
        print("CLIMATOLOGY (MONTHLY) CORRELATIONS")
        print("=" * 60)
        for pair in data['climatology_correlations']:
            print(f"\n{pair['pair']}:")
            for var, metrics in pair.get('variables', {}).items():
                corr = metrics.get('correlation')
                rmse = metrics.get('rmse')
                bias = metrics.get('bias')
                corr_str = f"{corr:.3f}" if corr is not None else "nan"
                rmse_str = f"{rmse:.2f}" if rmse is not None else "nan"
                bias_str = f"{bias:.2f}" if bias is not None else "nan"
                print(f"  {var:20s} r={corr_str}  RMSE={rmse_str}  bias={bias_str}")

    print("\nOutputs (plots) saved to 'annual_timeseries', 'annual_timeseries_multi', and 'monthly_climatology' subfolders in the output directory.")


def main():
    parser = argparse.ArgumentParser(description='Compare climate datasets with extended analysis and plots')
    parser.add_argument('--sources', required=True, nargs='+')
    parser.add_argument('--lat', required=True, type=float)
    parser.add_argument('--lon', required=True, type=float)
    parser.add_argument('--start', required=True)
    parser.add_argument('--end', required=True)
    parser.add_argument('--format', choices=['json', 'report'], default='report')
    parser.add_argument('--output-dir', default='./outputs', help='Directory to save plots and outputs')

    args = parser.parse_args()

    print(f"Fetching data from {len(args.sources)} sources...")

    result = compare_sources(args.sources, args.lat, args.lon, args.start, args.end, output_dir=args.output_dir)

    if args.format == 'report':
        print_report(result)
    else:
        def default(o):
            if isinstance(o, (np.integer, np.floating)):
                return float(o)
            if isinstance(o, np.ndarray):
                return o.tolist()
            if hasattr(o, 'isoformat'):
                return str(o)
            return str(o)

        print(json.dumps(result, indent=2, default=default))


if __name__ == "__main__":
    main()

   
# python -m climate_tookit.compare_datasets.compare_datasets --sources era_5 chirps nasa_power imerg nex_gddp agera_5 cmip_6 --lat -1.286 --lon 36.817 --start 1990-01-01 --end 2020-12-31 --format report