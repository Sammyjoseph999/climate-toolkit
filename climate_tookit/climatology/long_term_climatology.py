"""
Long-term Climatology Module

Calculates climate normals (30-year means) for precipitation and temperature.
Works with datasets that have either precipitation OR temperature OR both.
Follows WMO standards for climatological normal periods.

Standard WMO periods:
- 1961-1990
- 1971-2000
- 1981-2010
- 1991-2020

Outputs (resolves #81):
- Annual statistics per year
- 30-year annual climatology (means / std / min / max / trends)
- Monthly climatology (per calendar month) for precipitation and temperature
- Annual time-series tables (year × variable)
- Monthly climatology tables (month × variable)
- Annual time-series plots (PNG)
- Monthly climatology plots (PNG)

Dependencies: pandas, matplotlib (optional, for plots), preprocess_data pipeline
"""

import sys
import os
from datetime import date
from typing import Dict, List, Any, Tuple, Optional
import pandas as pd
import json
import argparse
from statistics import mean, stdev, median

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, os.path.join(parent_dir, 'fetch_data', 'preprocess_data'))
sys.path.insert(0, os.path.join(parent_dir, 'fetch_data', 'source_data', 'sources'))

try:
    from preprocess_data import preprocess_data
    from utils.models import ClimateVariable
    PREPROCESS_AVAILABLE = True
except ImportError:
    PREPROCESS_AVAILABLE = False
    print("Warning: Preprocessing pipeline not available")

try:
    import matplotlib.pyplot as plt
    from matplotlib.ticker import MaxNLocator
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False

PRECIP_COL_CANDIDATES = ['precipitation', 'precip', 'pr', 'rainfall']
TMAX_COL_CANDIDATES = ['max_temperature', 'tmax', 'tasmax', 'temperature_max']
TMIN_COL_CANDIDATES = ['min_temperature', 'tmin', 'tasmin', 'temperature_min']
MONTH_LABELS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
PALETTE = {
    'precip': '#2E86AB',
    'tmax': '#E84855',
    'tavg': '#F4A261',
    'tmin': '#3BB273',
}

def _detect_columns(df: pd.DataFrame) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Return (precip_col, tmax_col, tmin_col) found in df, or None for each missing."""
    precip = next((c for c in PRECIP_COL_CANDIDATES if c in df.columns), None)
    tmax = next((c for c in TMAX_COL_CANDIDATES if c in df.columns), None)
    tmin = next((c for c in TMIN_COL_CANDIDATES if c in df.columns), None)
    return precip, tmax, tmin

def _normalize_units(df: pd.DataFrame, tmax_col: Optional[str], tmin_col: Optional[str]) -> pd.DataFrame:
    """Convert temperatures from Kelvin to Celsius when values look like K. Returns a copy."""
    df = df.copy()
    if tmax_col and df[tmax_col].notna().any() and df[tmax_col].mean() > 100:
        df[tmax_col] = df[tmax_col] - 273.15
        if tmin_col and tmin_col in df.columns:
            df[tmin_col] = df[tmin_col] - 273.15
    return df

def calculate_annual_statistics(
    lat: float,
    lon: float,
    year: int,
    source: str,
    variables: Optional[List] = None
) -> Optional[Dict[str, Any]]:
    """
    Calculate annual statistics for a single year.
    Works with partial data - accepts datasets with only precipitation, only temperature, or both.
    Args:
        lat: Latitude
        lon: Longitude
        year: Year to analyze
        source: Data source
        variables: List of ClimateVariable enums to fetch
    Returns:
        Dictionary with annual statistics or None if failed
    """
    if not PREPROCESS_AVAILABLE:
        raise Exception("Preprocessing pipeline required")
    
    if variables is None:
        variables = [
            ClimateVariable.precipitation,
            ClimateVariable.max_temperature,
            ClimateVariable.min_temperature
        ]
    
    try:
        date_from = date(year, 1, 1)
        date_to = date(year, 12, 31)
        
        df = preprocess_data(
            source=source,
            location_coord=(lat, lon),
            variables=variables,
            date_from=date_from,
            date_to=date_to
        )
        
        if df.empty or len(df) < 300:  # At least 300 days for valid annual stats
            print(f"  ✗ {year}: Insufficient data ({len(df)} days)")
            return None

        precip_col, tmax_col, tmin_col = _detect_columns(df)
        df = _normalize_units(df, tmax_col, tmin_col)

        stats: Dict[str, Any] = {}
        has_data = False

        if precip_col:
            precip_data = df[precip_col]
            if precip_data.notna().sum() > 0:
                stats['precipitation'] = {
                    'annual_total_mm': float(precip_data.sum()),
                    'annual_mean_daily_mm': float(precip_data.mean()),
                    'annual_median_daily_mm': float(precip_data.median()),
                    'annual_max_daily_mm': float(precip_data.max()),
                    'annual_std_daily_mm': float(precip_data.std()),
                    'rainy_days': int((precip_data > 1.0).sum()),
                    'dry_days': int((precip_data <= 1.0).sum()),
                    'days_with_data': int(precip_data.notna().sum())
                }
                has_data = True

        if tmax_col and tmin_col:
            tmax_data = df[tmax_col]
            tmin_data = df[tmin_col]
            if tmax_data.notna().sum() > 0 and tmin_data.notna().sum() > 0:
                tavg = (tmax_data + tmin_data) / 2
                stats['temperature'] = {
                    'annual_mean_tmax_c': float(tmax_data.mean()),
                    'annual_mean_tmin_c': float(tmin_data.mean()),
                    'annual_mean_tavg_c': float(tavg.mean()),
                    'annual_max_tmax_c': float(tmax_data.max()),
                    'annual_min_tmin_c': float(tmin_data.min()),
                    'annual_std_tmax_c': float(tmax_data.std()),
                    'annual_std_tmin_c': float(tmin_data.std()),
                    'annual_diurnal_range_c': float((tmax_data - tmin_data).mean()),
                    'days_with_data': int(tmax_data.notna().sum())
                }
                has_data = True

        if not has_data:
            print(f"  ✗ {year}: No valid precipitation or temperature data")
            return None

        stats['year'] = year
        stats['data_completeness'] = len(df) / 365.0 * 100
        stats['_daily_df'] = df
        stats['_columns'] = {'precip': precip_col, 'tmax': tmax_col, 'tmin': tmin_col}

        return stats
        
    except Exception as e:
        print(f"  ✗ {year}: {str(e)}")
        return None

def compute_monthly_climatology(
    combined_df: pd.DataFrame,
    precip_col: Optional[str],
    tmax_col: Optional[str],
    tmin_col: Optional[str]
) -> Dict[str, Any]:
    """
    Compute per-calendar-month climatology over the full multi-year window.
    For precipitation: monthly totals are computed per (year, month), then averaged across years to give the mean monthly total — the same convention
    used in compare_datasets.
    For temperature: daily values are averaged within each calendar month.
    Returns a dict keyed by 'precipitation' / 'temperature' with month numbers (1-12) as inner keys.
    """
    monthly: Dict[str, Any] = {}
    if combined_df.empty:
        return monthly

    df = combined_df.copy()
    df['date'] = pd.to_datetime(df['date'])
    df['_yr'] = df['date'].dt.year
    df['_mo'] = df['date'].dt.month

    if precip_col and df[precip_col].notna().any():
        monthly_totals = df.groupby(['_yr', '_mo'])[precip_col].sum()
        mean_total = monthly_totals.groupby(level='_mo').mean()
        std_total = monthly_totals.groupby(level='_mo').std()
        min_total = monthly_totals.groupby(level='_mo').min()
        max_total = monthly_totals.groupby(level='_mo').max()
        mean_daily = df.groupby('_mo')[precip_col].mean()
        rainy_per_month = (
            df.assign(_rd=(df[precip_col] > 1.0).astype(int))
              .groupby(['_yr', '_mo'])['_rd'].sum()
              .groupby(level='_mo').mean()
        )
        monthly['precipitation'] = {
            int(m): {
                'mean_monthly_total_mm': round(float(mean_total.get(m, float('nan'))), 2),
                'std_monthly_total_mm': round(float(std_total.get(m, float('nan'))), 2) if pd.notna(std_total.get(m, float('nan'))) else 0.0,
                'min_monthly_total_mm': round(float(min_total.get(m, float('nan'))), 2),
                'max_monthly_total_mm': round(float(max_total.get(m, float('nan'))), 2),
                'mean_daily_mm': round(float(mean_daily.get(m, float('nan'))), 3),
                'mean_rainy_days': round(float(rainy_per_month.get(m, float('nan'))), 2),
            }
            for m in range(1, 13) if m in mean_total.index
        }
    if tmax_col and tmin_col and df[tmax_col].notna().any() and df[tmin_col].notna().any():
        df['_tavg'] = (df[tmax_col] + df[tmin_col]) / 2
        mean_tmax = df.groupby('_mo')[tmax_col].mean()
        mean_tmin = df.groupby('_mo')[tmin_col].mean()
        mean_tavg = df.groupby('_mo')['_tavg'].mean()
        std_tavg = df.groupby('_mo')['_tavg'].std()
        max_tmax = df.groupby('_mo')[tmax_col].max()
        min_tmin = df.groupby('_mo')[tmin_col].min()

        monthly['temperature'] = {
            int(m): {
                'mean_monthly_tmax_c': round(float(mean_tmax.get(m, float('nan'))), 2),
                'mean_monthly_tmin_c': round(float(mean_tmin.get(m, float('nan'))), 2),
                'mean_monthly_tavg_c': round(float(mean_tavg.get(m, float('nan'))), 2),
                'std_monthly_tavg_c': round(float(std_tavg.get(m, float('nan'))), 2) if pd.notna(std_tavg.get(m, float('nan'))) else 0.0,
                'max_monthly_tmax_c': round(float(max_tmax.get(m, float('nan'))), 2),
                'min_monthly_tmin_c': round(float(min_tmin.get(m, float('nan'))), 2),
            }
            for m in range(1, 13) if m in mean_tavg.index
        }
    return monthly

def _style_ax(ax, title="", xlabel="", ylabel=""):
    ax.set_facecolor("#F7F9FC")
    ax.spines[["top", "right"]].set_visible(False)
    ax.spines[["left", "bottom"]].set_color("#CCCCCC")
    ax.tick_params(colors="#555555", labelsize=8)
    if title:
        ax.set_title(title, fontsize=10, fontweight="bold", pad=8, color="#222222")
    if xlabel:
        ax.set_xlabel(xlabel, fontsize=8, color="#555555")
    if ylabel:
        ax.set_ylabel(ylabel, fontsize=8, color="#555555")
    ax.grid(axis="y", color="#E0E0E0", linewidth=0.6, linestyle="--")

def plot_annual_timeseries(
    annual_stats: List[Dict[str, Any]],
    source: str,
    period_label: str,
    output_path: str
) -> bool:
    """Plot annual precipitation totals and annual mean temperature over the period."""
    if not MATPLOTLIB_AVAILABLE:
        print("  (matplotlib not available — skipping annual time-series plot)")
        return False
    years = [s['year'] for s in annual_stats]
    has_precip = any('precipitation' in s for s in annual_stats)
    has_temp = any('temperature' in s for s in annual_stats)
    n_panels = int(has_precip) + int(has_temp)
    if n_panels == 0:
        return False

    fig, axes = plt.subplots(n_panels, 1, figsize=(10, 3.2 * n_panels), squeeze=False)
    fig.suptitle(f"Annual Time Series — {source} ({period_label})",
                 fontsize=12, fontweight="bold", color="#111111", y=1.01)

    idx = 0
    if has_precip:
        ax = axes[idx][0]
        vals = [s['precipitation']['annual_total_mm'] if 'precipitation' in s else None
                for s in annual_stats]
        ax.plot(years, vals, marker="o", linewidth=1.8, markersize=4,
                color=PALETTE['precip'])
        ax.fill_between(years, vals, alpha=0.12, color=PALETTE['precip'])
        ax.xaxis.set_major_locator(MaxNLocator(integer=True))
        _style_ax(ax, title="Precipitation",
                  xlabel="Year", ylabel="Annual total (mm)")
        idx += 1

    if has_temp:
        ax = axes[idx][0]
        tavg = [s['temperature']['annual_mean_tavg_c'] if 'temperature' in s else None
                for s in annual_stats]
        tmax = [s['temperature']['annual_mean_tmax_c'] if 'temperature' in s else None
                for s in annual_stats]
        tmin = [s['temperature']['annual_mean_tmin_c'] if 'temperature' in s else None
                for s in annual_stats]
        ax.plot(years, tmax, marker="o", linewidth=1.5, markersize=3,
                color=PALETTE['tmax'], label='Tmax')
        ax.plot(years, tavg, marker="o", linewidth=1.8, markersize=4,
                color=PALETTE['tavg'], label='Tavg')
        ax.plot(years, tmin, marker="o", linewidth=1.5, markersize=3,
                color=PALETTE['tmin'], label='Tmin')
        ax.xaxis.set_major_locator(MaxNLocator(integer=True))
        ax.legend(fontsize=8, framealpha=0.7)
        _style_ax(ax, title="Temperature",
                  xlabel="Year", ylabel="Annual mean (°C)")

    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  📊  Saved → {output_path}")
    return True

def plot_monthly_climatology(
    monthly: Dict[str, Any],
    source: str,
    period_label: str,
    output_path: str
) -> bool:
    """Plot monthly climatology: bar chart for precipitation, line plot for temperature."""
    if not MATPLOTLIB_AVAILABLE:
        print("  (matplotlib not available — skipping monthly climatology plot)")
        return False

    has_precip = bool(monthly.get('precipitation'))
    has_temp = bool(monthly.get('temperature'))
    n_panels = int(has_precip) + int(has_temp)
    if n_panels == 0:
        return False

    fig, axes = plt.subplots(n_panels, 1, figsize=(10, 3.2 * n_panels), squeeze=False)
    fig.suptitle(f"Monthly Climatology — {source} ({period_label})",
                 fontsize=12, fontweight="bold", color="#111111", y=1.01)

    idx = 0
    if has_precip:
        ax = axes[idx][0]
        months = sorted(monthly['precipitation'].keys())
        totals = [monthly['precipitation'][m]['mean_monthly_total_mm'] for m in months]
        ax.bar(months, totals, color=PALETTE['precip'], alpha=0.82,
               edgecolor="white", linewidth=0.6)
        ax.set_xticks(range(1, 13))
        ax.set_xticklabels(MONTH_LABELS, fontsize=8)
        _style_ax(ax, title="Precipitation (mean monthly total)",
                  xlabel="Month", ylabel="Total (mm)")
        idx += 1

    if has_temp:
        ax = axes[idx][0]
        months = sorted(monthly['temperature'].keys())
        tmax = [monthly['temperature'][m]['mean_monthly_tmax_c'] for m in months]
        tavg = [monthly['temperature'][m]['mean_monthly_tavg_c'] for m in months]
        tmin = [monthly['temperature'][m]['mean_monthly_tmin_c'] for m in months]
        ax.plot(months, tmax, marker="o", linewidth=1.8, markersize=4,
                color=PALETTE['tmax'], label='Tmax')
        ax.plot(months, tavg, marker="o", linewidth=1.8, markersize=4,
                color=PALETTE['tavg'], label='Tavg')
        ax.plot(months, tmin, marker="o", linewidth=1.8, markersize=4,
                color=PALETTE['tmin'], label='Tmin')
        ax.set_xticks(range(1, 13))
        ax.set_xticklabels(MONTH_LABELS, fontsize=8)
        ax.legend(fontsize=8, framealpha=0.7)
        _style_ax(ax, title="Temperature (mean monthly)",
                  xlabel="Month", ylabel="°C")

    fig.tight_layout()
    fig.savefig(output_path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  📊  Saved → {output_path}")
    return True

def calculate_climatology(
    location_coord: Tuple[float, float],
    start_year: int,
    end_year: int,
    source: str,
    variables: Optional[List] = None,
    output_dir: Optional[str] = None
) -> Dict[str, Any]:
    """
    Calculate long-term climatology (multi-year normals).
    Works with partial data - accepts datasets with only precipitation, only temperature, or both.
    Args:
        location_coord: (latitude, longitude)
        start_year: Start year of climatology period
        end_year: End year of climatology period (inclusive)
        source: Data source identifier
        variables: Optional list of ClimateVariable enums
    Returns:
        Dictionary containing:
        - annual_statistics: List of annual stats for each year
        - climatology: 30-year mean values (for available variables)
        - trends: Linear trends if applicable
    """
    lat, lon = location_coord
    n_years = end_year - start_year + 1
    
    print(f"\n{'='*70}")
    print(f"  CALCULATING {n_years}-YEAR CLIMATOLOGY")
    print(f"{'='*70}")
    print(f"  Location: ({lat:.4f}, {lon:.4f})")
    print(f"  Period: {start_year}-{end_year}")
    print(f"  Source: {source}")
    print(f"{'='*70}\n")
    
    print(f"  Processing {n_years} years...\n")
    
    annual_stats = []
    
    for year in range(start_year, end_year + 1):
        print(f"  [{year - start_year + 1}/{n_years}] {year}...", end=' ')
        
        stats = calculate_annual_statistics(lat, lon, year, source, variables)
        
        if stats:
            annual_stats.append(stats)
            print("✓")
    
    print(f"\n  Complete: {len(annual_stats)}/{n_years} years with valid data\n")
    
    if len(annual_stats) < n_years * 0.8:  # Need at least 80% of years
        return {
            'error': f'Insufficient data: only {len(annual_stats)}/{n_years} years available',
            'location': {'latitude': lat, 'longitude': lon},
            'period': {'start_year': start_year, 'end_year': end_year},
            'source': source
        }
    
    # Calculate climatology (long-term means)
    climatology = {}
    
    # Precipitation climatology (if available)
    precip_annual_totals = [s['precipitation']['annual_total_mm'] for s in annual_stats if 'precipitation' in s]
    precip_annual_means = [s['precipitation']['annual_mean_daily_mm'] for s in annual_stats if 'precipitation' in s]
    
    if precip_annual_totals:
        climatology['precipitation'] = {
            'mean_annual_total_mm': round(mean(precip_annual_totals), 2),
            'median_annual_total_mm': round(median(precip_annual_totals), 2),
            'std_annual_total_mm': round(stdev(precip_annual_totals), 2) if len(precip_annual_totals) > 1 else 0,
            'min_annual_total_mm': round(min(precip_annual_totals), 2),
            'max_annual_total_mm': round(max(precip_annual_totals), 2),
            'mean_daily_mm': round(mean(precip_annual_means), 2),
            'years_used': len(precip_annual_totals)
        }
    # Temperature climatology (if available)
    temp_annual_tavg = [s['temperature']['annual_mean_tavg_c'] for s in annual_stats if 'temperature' in s]
    temp_annual_tmax = [s['temperature']['annual_mean_tmax_c'] for s in annual_stats if 'temperature' in s]
    temp_annual_tmin = [s['temperature']['annual_mean_tmin_c'] for s in annual_stats if 'temperature' in s]
    
    if temp_annual_tavg:
        climatology['temperature'] = {
            'mean_annual_tavg_c': round(mean(temp_annual_tavg), 2),
            'mean_annual_tmax_c': round(mean(temp_annual_tmax), 2),
            'mean_annual_tmin_c': round(mean(temp_annual_tmin), 2),
            'std_annual_tavg_c': round(stdev(temp_annual_tavg), 2) if len(temp_annual_tavg) > 1 else 0,
            'min_annual_tavg_c': round(min(temp_annual_tavg), 2),
            'max_annual_tavg_c': round(max(temp_annual_tavg), 2),
            'years_used': len(temp_annual_tavg)
        }
    # Warn if we have neither
    if not climatology:
        return {
            'error': 'No valid precipitation or temperature data found in any year',
            'location': {'latitude': lat, 'longitude': lon},
            'period': {'start_year': start_year, 'end_year': end_year},
            'source': source
        }
    # Calculate trends if we have enough years
    trends = {}
    if len(annual_stats) >= 10:
        years = [s['year'] for s in annual_stats]
        
        if precip_annual_totals and len(precip_annual_totals) == len(years):
            precip_trend = calculate_linear_trend(years, precip_annual_totals)
            trends['precipitation_trend_mm_per_year'] = round(precip_trend, 3)
        
        if temp_annual_tavg and len(temp_annual_tavg) == len(years):
            temp_trend = calculate_linear_trend(years, temp_annual_tavg)
            trends['temperature_trend_c_per_year'] = round(temp_trend, 4)
    
    # Determine available variables
    available_vars = list(climatology.keys())

    # Build combined daily DataFrame for monthly climatology and time-series tables
    daily_frames = [s.get('_daily_df') for s in annual_stats if s.get('_daily_df') is not None]
    combined_df = pd.concat(daily_frames, ignore_index=True) if daily_frames else pd.DataFrame()

    # Detect canonical columns from the combined frame (any year's columns work)
    if not combined_df.empty:
        precip_col, tmax_col, tmin_col = _detect_columns(combined_df)
    else:
        precip_col = tmax_col = tmin_col = None

    monthly_climatology = compute_monthly_climatology(
        combined_df, precip_col, tmax_col, tmin_col
    )
    # Annual time-series tables (year → value), convenient for downstream consumers
    annual_time_series: Dict[str, Any] = {}
    if precip_annual_totals:
        annual_time_series['precipitation_total_mm'] = {
            s['year']: round(s['precipitation']['annual_total_mm'], 2)
            for s in annual_stats if 'precipitation' in s
        }
    if temp_annual_tavg:
        annual_time_series['tavg_c'] = {
            s['year']: round(s['temperature']['annual_mean_tavg_c'], 2)
            for s in annual_stats if 'temperature' in s
        }
        annual_time_series['tmax_c'] = {
            s['year']: round(s['temperature']['annual_mean_tmax_c'], 2)
            for s in annual_stats if 'temperature' in s
        }
        annual_time_series['tmin_c'] = {
            s['year']: round(s['temperature']['annual_mean_tmin_c'], 2)
            for s in annual_stats if 'temperature' in s
        }
    # Emit plots if requested
    plots_written: List[str] = []
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        period_label = f"{start_year}-{end_year}"

        annual_plot_path = os.path.join(
            output_dir, f"{source}_{period_label}_annual_timeseries.png"
        )
        if plot_annual_timeseries(annual_stats, source, period_label, annual_plot_path):
            plots_written.append(annual_plot_path)

        monthly_plot_path = os.path.join(
            output_dir, f"{source}_{period_label}_monthly_climatology.png"
        )
        if plot_monthly_climatology(monthly_climatology, source, period_label, monthly_plot_path):
            plots_written.append(monthly_plot_path)

    # Strip the private daily-data attachments before returning a serializable result
    for s in annual_stats:
        s.pop('_daily_df', None)
        s.pop('_columns', None)

    result = {
        'location': {'latitude': lat, 'longitude': lon},
        'period': {
            'start_year': start_year,
            'end_year': end_year,
            'n_years': n_years,
            'years_with_data': len(annual_stats)
        },
        'source': source,
        'available_variables': available_vars,
        'climatology': climatology,
        'monthly_climatology': monthly_climatology if monthly_climatology else None,
        'trends': trends if trends else None,
        'annual_statistics': annual_stats,
        'annual_time_series': annual_time_series if annual_time_series else None,
        'plots': plots_written if plots_written else None,
        'metadata': {
            'wmo_standard': n_years == 30,
            'data_completeness_pct': round(len(annual_stats) / n_years * 100, 1),
            'variables': ', '.join(available_vars)
        }
    }
    return result

def calculate_linear_trend(years: List[int], values: List[float]) -> float:
    """
    Calculate linear trend using least squares regression.
    Returns:
        Slope (change per year)
    """
    n = len(years)
    if n < 2:
        return 0.0
    
    mean_year = mean(years)
    mean_value = mean(values)
    
    numerator = sum((years[i] - mean_year) * (values[i] - mean_value) for i in range(n))
    denominator = sum((years[i] - mean_year) ** 2 for i in range(n))
    
    if denominator == 0:
        return 0.0
    
    return numerator / denominator

def print_climatology_report(result: Dict[str, Any]):
    """Print formatted climatology report."""
    
    if 'error' in result:
        print(f"\nError: {result['error']}")
        return
    
    print(f"\n{'='*70}")
    print(f"  CLIMATOLOGY REPORT")
    print(f"{'='*70}")
    print(f"  Location: {result['location']['latitude']:.4f}, {result['location']['longitude']:.4f}")
    print(f"  Period: {result['period']['start_year']}-{result['period']['end_year']} ({result['period']['n_years']} years)")
    print(f"  Source: {result['source']}")
    print(f"  Data Completeness: {result['metadata']['data_completeness_pct']:.1f}%")
    print(f"  Available Variables: {result['metadata']['variables']}")
    
    if result['metadata']['wmo_standard']:
        print(f"  WMO Standard: ✓ (30-year normal)")
    
    print(f"{'='*70}\n")
    
    clim = result['climatology']
    
    # Precipitation
    if 'precipitation' in clim:
        print(f"  {'─'*66}")
        print(f"  PRECIPITATION CLIMATOLOGY")
        print(f"  {'─'*66}")
        p = clim['precipitation']
        print(f"    Mean Annual Total:      {p['mean_annual_total_mm']:>10.2f} mm")
        print(f"    Median Annual Total:    {p['median_annual_total_mm']:>10.2f} mm")
        print(f"    Std Deviation:          {p['std_annual_total_mm']:>10.2f} mm")
        print(f"    Range:                  {p['min_annual_total_mm']:>10.2f} - {p['max_annual_total_mm']:.2f} mm")
        print(f"    Mean Daily:             {p['mean_daily_mm']:>10.2f} mm/day")
        print(f"    Years Used:             {p['years_used']:>10}")
        print()
    
    # Temperature
    if 'temperature' in clim:
        print(f"  {'─'*66}")
        print(f"  TEMPERATURE CLIMATOLOGY")
        print(f"  {'─'*66}")
        t = clim['temperature']
        print(f"    Mean Annual Average:    {t['mean_annual_tavg_c']:>10.2f} °C")
        print(f"    Mean Annual Maximum:    {t['mean_annual_tmax_c']:>10.2f} °C")
        print(f"    Mean Annual Minimum:    {t['mean_annual_tmin_c']:>10.2f} °C")
        print(f"    Std Deviation:          {t['std_annual_tavg_c']:>10.2f} °C")
        print(f"    Range:                  {t['min_annual_tavg_c']:>10.2f} - {t['max_annual_tavg_c']:.2f} °C")
        print(f"    Years Used:             {t['years_used']:>10}")
        print()
    
    # Monthly climatology
    monthly = result.get('monthly_climatology') or {}
    if monthly.get('precipitation'):
        print(f"  {'─'*66}")
        print(f"  MONTHLY PRECIPITATION CLIMATOLOGY")
        print(f"  {'─'*66}")
        print(f"    {'Month':<6}{'Mean(mm)':>12}{'Std(mm)':>12}"
              f"{'Min(mm)':>12}{'Max(mm)':>12}{'Rainy d':>10}")
        for m in sorted(monthly['precipitation'].keys()):
            row = monthly['precipitation'][m]
            print(f"    {MONTH_LABELS[m-1]:<6}"
                  f"{row['mean_monthly_total_mm']:>12.2f}"
                  f"{row['std_monthly_total_mm']:>12.2f}"
                  f"{row['min_monthly_total_mm']:>12.2f}"
                  f"{row['max_monthly_total_mm']:>12.2f}"
                  f"{row['mean_rainy_days']:>10.2f}")
        print()

    if monthly.get('temperature'):
        print(f"  {'─'*66}")
        print(f"  MONTHLY TEMPERATURE CLIMATOLOGY")
        print(f"  {'─'*66}")
        print(f"    {'Month':<6}{'Tavg(°C)':>12}{'Tmax(°C)':>12}"
              f"{'Tmin(°C)':>12}{'Std Tavg':>12}")
        for m in sorted(monthly['temperature'].keys()):
            row = monthly['temperature'][m]
            print(f"    {MONTH_LABELS[m-1]:<6}"
                  f"{row['mean_monthly_tavg_c']:>12.2f}"
                  f"{row['mean_monthly_tmax_c']:>12.2f}"
                  f"{row['mean_monthly_tmin_c']:>12.2f}"
                  f"{row['std_monthly_tavg_c']:>12.2f}")
        print()

    # Annual time series table
    ats = result.get('annual_time_series') or {}
    if ats:
        print(f"  {'─'*66}")
        print(f"  ANNUAL TIME SERIES")
        print(f"  {'─'*66}")
        all_years = sorted({y for series in ats.values() for y in series.keys()})
        col_keys = list(ats.keys())
        header = f"    {'Year':<8}" + "".join(f"{k:>22}" for k in col_keys)
        print(header)
        for y in all_years:
            line = f"    {y:<8}"
            for k in col_keys:
                v = ats[k].get(y)
                line += f"{(f'{v:.2f}' if v is not None else '-'):>22}"
            print(line)
        print()

    # Trends
    if result['trends']:
        print(f"  {'─'*66}")
        print(f"  TRENDS")
        print(f"  {'─'*66}")
        trends = result['trends']

        if 'precipitation_trend_mm_per_year' in trends:
            p_trend = trends['precipitation_trend_mm_per_year']
            direction = "↑" if p_trend > 0 else "↓" if p_trend < 0 else "→"
            print(f"    Precipitation:          {direction} {abs(p_trend):.3f} mm/year")

        if 'temperature_trend_c_per_year' in trends:
            t_trend = trends['temperature_trend_c_per_year']
            direction = "↑" if t_trend > 0 else "↓" if t_trend < 0 else "→"
            print(f"    Temperature:            {direction} {abs(t_trend):.4f} °C/year")
        print()

    # Plot paths
    if result.get('plots'):
        print(f"  {'─'*66}")
        print(f"  PLOTS")
        print(f"  {'─'*66}")
        for p in result['plots']:
            print(f"    📊 {p}")
        print()

    print(f"{'='*70}\n")

def main():
    """Command-line interface for climatology analysis."""
    parser = argparse.ArgumentParser(
        description='Calculate long-term climate normals (WMO 30-year standards)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Calculate 1991-2020 climatology (current WMO standard)
  python3 -m climate_tookit.climatology.long_term_climatology --location="-1.286,36.817" --start-year 1991 --end-year 2020 --source nasa_power
  
  # Calculate with JSON output
  python3 -m climate_tookit.climatology.long_term_climatology --location="-1.286,36.817" --start-year 1991 --end-year 2020 --source nasa_power --format json --output climatology_1991-2020.json
        """
    )
    
    parser.add_argument('--location', required=True, type=str,
                       help='Location as "lat,lon" (e.g., "-1.286,36.817")')
    parser.add_argument('--start-year', required=True, type=int,
                       help='Start year of climatology period')
    parser.add_argument('--end-year', required=True, type=int,
                       help='End year of climatology period (inclusive)')
    parser.add_argument('--source', required=True, type=str,
                       help='Data source (e.g., nasa_power, chirps, chirts)')
    parser.add_argument('--format', choices=['text', 'json'], default='text',
                       help='Output format (default: text)')
    parser.add_argument('--output', type=str,
                       help='Output file path (for JSON format)')
    parser.add_argument('--output-dir', type=str, default='./outputs',
                       help='Directory for plot PNGs (default: ./outputs). '
                            'Pass empty string to disable plotting.')
    
    args = parser.parse_args()
    
    # Parse location
    try:
        lat, lon = map(float, args.location.split(','))
    except ValueError:
        print("Error: Invalid location format. Use 'lat,lon' format.")
        sys.exit(1)
    
    # Validate years
    if args.end_year < args.start_year:
        print("Error: End year must be >= start year")
        sys.exit(1)
    
    n_years = args.end_year - args.start_year + 1
    if n_years < 10:
        print(f"Warning: {n_years} years may be insufficient for robust climatology")
        print("         WMO recommends 30-year periods for climate normals")
    
    # Calculate climatology
    plot_dir = args.output_dir if args.output_dir else None
    result = calculate_climatology(
        location_coord=(lat, lon),
        start_year=args.start_year,
        end_year=args.end_year,
        source=args.source,
        output_dir=plot_dir,
    )
    # Output
    if args.format == 'json':
        output = json.dumps(result, indent=2, default=str)
        
        if args.output:
            with open(args.output, 'w') as f:
                f.write(output)
            print(f"\n✓ Climatology saved to {args.output}")
        else:
            print(output)
    else:
        print_climatology_report(result)
        
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(result, f, indent=2, default=str)
            print(f"✓ JSON data saved to {args.output}")

if __name__ == "__main__":
    main()
    
# Calculate 1991-2020 climatology (current WMO standard)
# python -m climate_tookit.climatology.long_term_climatology --location="-1.286,36.817" --start-year 1991 --end-year 2020 --source era_5

# With JSON output
# python -m climate_tookit.climatology.long_term_climatology --location="-1.286,36.817" --start-year 1991 --end-year 2020 --source nasa_power --format json --output climatology_1991-2020.json