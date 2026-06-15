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
import math
import logging
import contextlib
from datetime import date
from typing import Dict, List, Any, Tuple, Optional
import pandas as pd
import json
import argparse
from statistics import mean, stdev, median

@contextlib.contextmanager
def _quiet_fetch_logs():
    """
    Silence the per-year INFO logs that the preprocess_data → nex_gddp pipeline emits during each year fetch 
    """
    target_names = [
        "nex_gddp", "preprocess_data", "fetch_data",
        "climate_tookit", "sources", "sources.nex_gddp",
    ]
    prev_root_level = logging.root.level
    prev_levels: Dict[str, int] = {}
    for name in target_names:
        lg = logging.getLogger(name)
        prev_levels[name] = lg.level
        lg.setLevel(logging.WARNING)
    logging.root.setLevel(logging.WARNING)
    try:
        yield
    finally:
        logging.root.setLevel(prev_root_level)
        for name, lvl in prev_levels.items():
            logging.getLogger(name).setLevel(lvl)

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, os.path.join(parent_dir, 'fetch_data', 'preprocess_data'))
sys.path.insert(0, os.path.join(parent_dir, 'fetch_data', 'source_data', 'sources'))

from preprocess_data import preprocess_data
from utils.models import ClimateVariable
PREPROCESS_AVAILABLE = True

try:
    import matplotlib
    # Force the non-interactive Agg backend: plots are only saved to PNG, never
    # shown. This avoids the Tk/Tcl backend, which crashes ("main thread is not
    # in main loop" / "Tcl_AsyncDelete") when figures are created while worker
    # threads are alive during the parallel ensemble fetch.
    matplotlib.use('Agg')
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

# NEX-GDDP-CMIP6 ensemble — 16 models + canonical SSP scenario labels.
NEX_GDDP_MODELS: List[str] = [
    'ACCESS-CM2', 'ACCESS-ESM1-5', 'CanESM5', 'CMCC-ESM2',
    'EC-Earth3', 'EC-Earth3-Veg-LR', 'GFDL-ESM4', 'INM-CM4-8',
    'INM-CM5-0', 'KACE-1-0-G', 'MIROC6', 'MPI-ESM1-2-LR',
    'MRI-ESM2-0', 'NorESM2-LM', 'NorESM2-MM', 'TaiESM1',
]
SSP_SCENARIOS: List[str] = ['ssp126', 'ssp245', 'ssp585', 'historical']
SCENARIO_ALIASES: Dict[str, str] = {
    'SSP1-2.6': 'ssp126', 'SSP2-4.5': 'ssp245', 'SSP5-8.5': 'ssp585',
    'ssp126': 'ssp126', 'ssp245': 'ssp245', 'ssp585': 'ssp585',
    'historical': 'historical',
}

def _normalize_scenario(s: str) -> Optional[str]:
    """Map any accepted SSP alias to the canonical scenario string, else None."""
    return SCENARIO_ALIASES.get(s.strip()) if isinstance(s, str) else None

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

def _default_variables() -> List:
    return [
        ClimateVariable.precipitation,
        ClimateVariable.max_temperature,
        ClimateVariable.min_temperature,
    ]

def _fetch_climatology_span(
    lat: float, lon: float, start_year: int, end_year: int,
    source: str, variables: Optional[List] = None,
    model: Optional[str] = None, scenario: Optional[str] = None,
) -> pd.DataFrame:
    """Fetch the entire start_year..end_year daily series in ONE call.

    Replaces one fetch per year with one per (model, scenario). The frame
    carries a datetime 'date' column so callers can slice per year in memory.
    """
    if not PREPROCESS_AVAILABLE:
        raise Exception("Preprocessing pipeline required")
    if variables is None:
        variables = _default_variables()
    fetch_kwargs: Dict[str, Any] = {}
    if model is not None:
        fetch_kwargs['model'] = model
    if scenario is not None:
        fetch_kwargs['scenario'] = scenario
    df = preprocess_data(
        source=source,
        location_coord=(lat, lon),
        variables=variables,
        date_from=date(start_year, 1, 1),
        date_to=date(end_year, 12, 31),
        **fetch_kwargs,
    )
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'])
    return df

def calculate_annual_statistics(
    lat: float,
    lon: float,
    year: int,
    source: str,
    variables: Optional[List] = None,
    model: Optional[str] = None,
    scenario: Optional[str] = None,
    verbose: bool = True,
    df: Optional[pd.DataFrame] = None,
) -> Optional[Dict[str, Any]]:
    """
    Calculate annual statistics for a single year.
    Works with partial data - accepts datasets with only precipitation, only temperature, or both.

    If ``df`` is provided it is used directly (an in-memory slice of a
    pre-fetched multi-year span); otherwise the year is fetched on demand.
    """
    if not PREPROCESS_AVAILABLE:
        raise Exception("Preprocessing pipeline required")

    if variables is None:
        variables = _default_variables()
    try:
        if df is None:
            date_from = date(year, 1, 1)
            date_to = date(year, 12, 31)

            fetch_kwargs: Dict[str, Any] = {}
            if model is not None:
                fetch_kwargs['model'] = model
            if scenario is not None:
                fetch_kwargs['scenario'] = scenario
            df = preprocess_data(
                source=source,
                location_coord=(lat, lon),
                variables=variables,
                date_from=date_from,
                date_to=date_to,
                **fetch_kwargs
            )

        if df is None or df.empty or len(df) < 300:
            if verbose:
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
            if verbose:
                print(f"  ✗ {year}: No valid precipitation or temperature data")
            return None

        stats['year'] = year
        stats['data_completeness'] = len(df) / 365.0 * 100
        stats['_daily_df'] = df
        stats['_columns'] = {'precip': precip_col, 'tmax': tmax_col, 'tmin': tmin_col}

        return stats
        
    except Exception as e:
        if verbose:
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

def _tight_ylim(ax, values: List[Any], pad_pct: float = 0.15,
                min_span_pct: float = 0.02) -> Optional[Tuple[float, float]]:
    """
    Set y-limits to bracket actual data with `pad_pct` padding above/below.
    Guarantees a minimum visible span (`min_span_pct` of the mid value) so a near-flat series still shows year-to-year wobble. Returns (lo, hi).
    """
    vals = [v for v in values if v is not None and not (isinstance(v, float) and math.isnan(v))]
    if not vals:
        return None
    lo, hi = float(min(vals)), float(max(vals))
    span = hi - lo
    mid = (lo + hi) / 2
    floor_span = abs(mid) * min_span_pct if mid != 0 else max(abs(lo), 1.0) * min_span_pct
    if span < floor_span:
        # Series is essentially flat — expand around the mean
        half = floor_span / 2
        lo, hi = mid - half, mid + half
        span = hi - lo
    pad = span * pad_pct
    y_lo, y_hi = lo - pad, hi + pad
    ax.set_ylim(y_lo, y_hi)
    return (y_lo, y_hi)

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
        # Tight y-limits first, then fill from the lower y-limit (not from 0, which would crush the year-to-year signal into a thin band at the top).
        ylim = _tight_ylim(ax, vals)
        if ylim is not None:
            ax.fill_between(years, vals, y2=ylim[0], alpha=0.12,
                            color=PALETTE['precip'])
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
        # Tight y-limits across all three series so the trend is visible.
        _tight_ylim(ax, (tmax or []) + (tavg or []) + (tmin or []), pad_pct=0.10)
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
    output_dir: Optional[str] = None,
    model: Optional[str] = None,
    scenario: Optional[str] = None,
    verbose: bool = True
) -> Dict[str, Any]:
    """
    Calculate long-term climatology (multi-year normals).
    Works with partial data - accepts datasets with only precipitation, only temperature, or both.
    """
    lat, lon = location_coord
    n_years = end_year - start_year + 1

    if verbose:
        print(f"\n{'='*70}")
        print(f"  CALCULATING {n_years}-YEAR CLIMATOLOGY")
        print(f"{'='*70}")
        print(f"  Location: ({lat:.4f}, {lon:.4f})")
        print(f"  Period: {start_year}-{end_year}")
        print(f"  Source: {source}")
        print(f"{'='*70}\n")
        print(f"  Processing {n_years} years...\n")

    annual_stats = []

    # Fetch the whole period in one call, then slice per year in memory.
    # Falls back to per-year fetching if the span fetch fails.
    full_span: Optional[pd.DataFrame] = None
    try:
        full_span = _fetch_climatology_span(
            lat, lon, start_year, end_year, source, variables,
            model=model, scenario=scenario,
        )
    except Exception as e:
        if verbose:
            print(f"  (span fetch unavailable, falling back to per-year: {e})")
        full_span = None

    have_span = full_span is not None and not full_span.empty and 'date' in full_span.columns

    for year in range(start_year, end_year + 1):
        if verbose:
            print(f"  [{year - start_year + 1}/{n_years}] {year}...", end=' ')

        year_df = None
        if have_span:
            year_df = full_span[full_span['date'].dt.year == year].copy()

        stats = calculate_annual_statistics(lat, lon, year, source, variables,
                                            model=model, scenario=scenario,
                                            verbose=verbose, df=year_df)
        if stats:
            annual_stats.append(stats)
            if verbose:
                print("✓")

    if verbose:
        print(f"\n  Complete: {len(annual_stats)}/{n_years} years with valid data\n")
    
    if len(annual_stats) < n_years * 0.8:  
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

# NEX-GDDP ensemble: average per-model climatologies and surface the per-model spread
def _is_number(v: Any) -> bool:
    return (isinstance(v, (int, float)) and not isinstance(v, bool)
            and not (isinstance(v, float) and math.isnan(v)))

def _fmt_num(v: Any, nd: int = 2) -> str:
    """Compact numeric formatter used in per-model and multi-scenario tables."""
    return f"{v:.{nd}f}" if _is_number(v) else "n/a"

def _avg_numeric(vals: List[Any], nd: int = 2) -> Optional[float]:
    nums = [float(v) for v in vals if _is_number(v)]
    return round(sum(nums) / len(nums), nd) if nums else None

def _avg_flat(dicts: List[Optional[Dict[str, Any]]], nd: int = 2) -> Dict[str, Any]:
    """Average matching numeric keys across a list of flat {key: number} dicts."""
    keys: List[str] = []
    for d in dicts:
        for k in (d or {}):
            if k not in keys:
                keys.append(k)
    out: Dict[str, Any] = {}
    for k in keys:
        avg = _avg_numeric([(d or {}).get(k) for d in dicts], nd)
        if avg is not None:
            out[k] = avg
    return out

def _avg_monthly(dicts: List[Optional[Dict[int, Dict]]], nd: int = 2) -> Dict[int, Dict]:
    """Average a {month: {metric: value}} structure across models."""
    months = sorted({m for d in dicts for m in (d or {})})
    return {m: _avg_flat([(d or {}).get(m, {}) for d in dicts], nd) for m in months}

def _avg_series(dicts: List[Optional[Dict[Any, Any]]], nd: int = 2) -> Dict[Any, float]:
    """Average a {year: value} series across models."""
    years = sorted({y for d in dicts for y in (d or {})})
    out: Dict[Any, float] = {}
    for y in years:
        avg = _avg_numeric([(d or {}).get(y) for d in dicts], nd)
        if avg is not None:
            out[y] = avg
    return out

def calculate_climatology_ensemble(
    location_coord: Tuple[float, float],
    start_year: int,
    end_year: int,
    scenario: str,
    variables: Optional[List] = None,
    models: Optional[List[str]] = None,
    exclude_models: Optional[List[str]] = None,
    output_dir: Optional[str] = None,
    verbose: bool = True,
    max_workers: int = 8,
) -> Dict[str, Any]:
    """
    NEX-GDDP-CMIP6 ensemble climatology.
    The per-model results are retained under `per_model_climatology` so the ensemble = mean of per-model values is inspectable.
    """
    lat, lon = location_coord
    canon = _normalize_scenario(scenario) or scenario
    active = list(models) if models else list(NEX_GDDP_MODELS)
    if exclude_models:
        excl = {m.upper() for m in exclude_models}
        active = [m for m in active if m.upper() not in excl]
    if not active:
        return {'error': 'No models selected after filtering.'}

    n_years = end_year - start_year + 1
    if verbose:
        print(f"\n{'='*70}")
        print(f"  NEX-GDDP CMIP6 ENSEMBLE CLIMATOLOGY ({n_years}-year)")
        print(f"{'='*70}")
        print(f"  Location : ({lat:.4f}, {lon:.4f})")
        print(f"  Period   : {start_year}-{end_year}")
        print(f"  Scenario : {canon}")
        print(f"  Models   : {len(active)}")
        print(f"{'='*70}\n")

    def _run_model(model: str) -> Tuple[str, Optional[Dict[str, Any]], Optional[str]]:
        """Compute one model's climatology. Returns (model, result, error)."""
        try:
            r = calculate_climatology(
                location_coord=(lat, lon),
                start_year=start_year, end_year=end_year,
                source='nex_gddp', variables=variables,
                output_dir=None, model=model, scenario=canon,
                verbose=False,
            )
            if 'error' in r:
                return model, None, r['error']
            return model, r, None
        except Exception as exc:
            return model, None, str(exc)

    per_model_results: Dict[str, Dict[str, Any]] = {}
    failed: List[Dict[str, str]] = []
    workers = max(1, min(max_workers, len(active)))

    def _record(model: str, r: Optional[Dict[str, Any]], err: Optional[str], idx: int) -> None:
        if err is not None or r is None:
            failed.append({'model': model, 'error': err or 'unknown error'})
            if verbose:
                print(f"  [{idx:02d}/{len(active):02d}] {model:<22} ✗  {err}")
        else:
            per_model_results[model] = r
            if verbose:
                yrs = r['period']['years_with_data']
                print(f"  [{idx:02d}/{len(active):02d}] {model:<22} ✓  {yrs}/{n_years} years")

    if workers == 1:
        for i, model in enumerate(active, 1):
            m, r, err = _run_model(model)
            _record(m, r, err, i)
    else:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        order = {m: i for i, m in enumerate(active, 1)}
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(_run_model, m): m for m in active}
            for fut in as_completed(futs):
                m, r, err = fut.result()
                _record(m, r, err, order[m])

    # Restore deterministic (input) model order regardless of completion order.
    per_model_results = {m: per_model_results[m] for m in active if m in per_model_results}

    if not per_model_results:
        return {'error': 'All models failed.', 'failed_models': failed,
                'location': {'latitude': lat, 'longitude': lon},
                'period': {'start_year': start_year, 'end_year': end_year},
                'source': 'nex_gddp', 'scenario': canon}

    models_ok = list(per_model_results)
    results_list = list(per_model_results.values())

    # Ensemble-average the climatology blocks
    climatology: Dict[str, Any] = {}
    precip_blocks = [r['climatology'].get('precipitation') for r in results_list
                     if r.get('climatology', {}).get('precipitation')]
    if precip_blocks:
        climatology['precipitation'] = _avg_flat(precip_blocks)
    temp_blocks = [r['climatology'].get('temperature') for r in results_list
                   if r.get('climatology', {}).get('temperature')]
    if temp_blocks:
        climatology['temperature'] = _avg_flat(temp_blocks)
    # years_used is a count, not a mean — report it as an int
    for _blk in climatology.values():
        if _is_number(_blk.get('years_used')):
            _blk['years_used'] = int(round(_blk['years_used']))

    # Monthly climatology
    monthly: Dict[str, Any] = {}
    mp = [(r.get('monthly_climatology') or {}).get('precipitation') for r in results_list]
    mp = [x for x in mp if x]
    if mp:
        monthly['precipitation'] = _avg_monthly(mp)
    mt = [(r.get('monthly_climatology') or {}).get('temperature') for r in results_list]
    mt = [x for x in mt if x]
    if mt:
        monthly['temperature'] = _avg_monthly(mt)

    # Trends
    trend_dicts = [r.get('trends') for r in results_list if r.get('trends')]
    trends = _avg_flat(trend_dicts, 4) if trend_dicts else {}

    # Annual time series (per year, averaged across models)
    series_keys: List[str] = []
    for r in results_list:
        for k in (r.get('annual_time_series') or {}):
            if k not in series_keys:
                series_keys.append(k)
    annual_time_series = {
        k: _avg_series([(r.get('annual_time_series') or {}).get(k, {})
                        for r in results_list])
        for k in series_keys
    }

    available_vars = list(climatology.keys())
    source_label = f"nex_gddp ensemble ({len(models_ok)} models, {canon})"
    years_with_data = int(round(mean(r['period']['years_with_data']
                                     for r in results_list)))

    # Plots from the ensemble-mean series (reconstruct an annual_stats shape)
    plots_written: List[str] = []
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        period_label = f"{start_year}-{end_year}"
        pt   = annual_time_series.get('precipitation_total_mm', {})
        tavg = annual_time_series.get('tavg_c', {})
        tmax = annual_time_series.get('tmax_c', {})
        tmin = annual_time_series.get('tmin_c', {})
        synth: List[Dict[str, Any]] = []
        for y in sorted({*pt, *tavg, *tmax, *tmin}):
            s: Dict[str, Any] = {'year': y}
            if y in pt:
                s['precipitation'] = {'annual_total_mm': pt[y]}
            if y in tavg:
                s['temperature'] = {
                    'annual_mean_tavg_c': tavg.get(y),
                    'annual_mean_tmax_c': tmax.get(y),
                    'annual_mean_tmin_c': tmin.get(y),
                }
            synth.append(s)
        tag = f"nex_gddp_ensemble_{canon}"
        annual_path = os.path.join(output_dir, f"{tag}_{period_label}_annual_timeseries.png")
        if synth and plot_annual_timeseries(synth, source_label, period_label, annual_path):
            plots_written.append(annual_path)
        monthly_path = os.path.join(output_dir, f"{tag}_{period_label}_monthly_climatology.png")
        if monthly and plot_monthly_climatology(monthly, source_label, period_label, monthly_path):
            plots_written.append(monthly_path)

    return {
        'location': {'latitude': lat, 'longitude': lon},
        'period': {
            'start_year': start_year, 'end_year': end_year,
            'n_years': n_years, 'years_with_data': years_with_data,
        },
        'source': source_label,
        'scenario': canon,
        'ensemble': True,
        'models_used': models_ok,
        'models_failed': failed,
        'n_models_ok': len(models_ok),
        'available_variables': available_vars,
        'climatology': climatology,
        'monthly_climatology': monthly if monthly else None,
        'trends': trends if trends else None,
        'annual_time_series': annual_time_series if annual_time_series else None,
        'per_model_climatology': {m: r['climatology'] for m, r in per_model_results.items()},
        'per_model_monthly_climatology': {
            m: (r.get('monthly_climatology') or {})
            for m, r in per_model_results.items()
        },
        'plots': plots_written if plots_written else None,
        'metadata': {
            'wmo_standard': n_years == 30,
            'data_completeness_pct': round(years_with_data / n_years * 100, 1),
            'variables': ', '.join(available_vars),
            'ensemble_models': models_ok,
            'scenario': canon,
        },
    }

def _print_per_model_climatology_breakdown(result: Dict[str, Any]) -> None:
    """
    Show each model's annual climatology before the ensemble mean, so the reader can verify the ENSEMBLE row equals the column means of the per-model rows.
    """
    per_model = result.get('per_model_climatology') or {}
    if not per_model:
        return
    clim = result.get('climatology') or {}

    print(f"\n  {'─'*66}")
    print(f"  PER-MODEL ANNUAL BREAKDOWN ({len(per_model)} model(s)) "
          f"→ feeds the ensemble annual means")
    print(f"  {'─'*66}")

    if any((c or {}).get('precipitation') for c in per_model.values()):
        print(f"\n  Precipitation (annual):")
        rows = []
        for name, c in per_model.items():
            p = (c or {}).get('precipitation') or {}
            rows.append({
                'Model':         name,
                'AnnTotal_mm':   _fmt_num(p.get('mean_annual_total_mm')),
                'MedTotal_mm':   _fmt_num(p.get('median_annual_total_mm')),
                'Std_mm':        _fmt_num(p.get('std_annual_total_mm')),
                'MeanDaily_mm':  _fmt_num(p.get('mean_daily_mm')),
            })
        ep = clim.get('precipitation') or {}
        rows.append({
            'Model':         f"ENSEMBLE (mean of {len(per_model)})",
            'AnnTotal_mm':   _fmt_num(ep.get('mean_annual_total_mm')),
            'MedTotal_mm':   _fmt_num(ep.get('median_annual_total_mm')),
            'Std_mm':        _fmt_num(ep.get('std_annual_total_mm')),
            'MeanDaily_mm':  _fmt_num(ep.get('mean_daily_mm')),
        })
        for line in pd.DataFrame(rows).to_string(index=False).splitlines():
            print(f"    {line}")

    if any((c or {}).get('temperature') for c in per_model.values()):
        print(f"\n  Temperature (annual):")
        rows = []
        for name, c in per_model.items():
            t = (c or {}).get('temperature') or {}
            rows.append({
                'Model':      name,
                'Tavg_c':     _fmt_num(t.get('mean_annual_tavg_c')),
                'Tmax_c':     _fmt_num(t.get('mean_annual_tmax_c')),
                'Tmin_c':     _fmt_num(t.get('mean_annual_tmin_c')),
                'StdTavg_c':  _fmt_num(t.get('std_annual_tavg_c')),
            })
        et = clim.get('temperature') or {}
        rows.append({
            'Model':      f"ENSEMBLE (mean of {len(per_model)})",
            'Tavg_c':     _fmt_num(et.get('mean_annual_tavg_c')),
            'Tmax_c':     _fmt_num(et.get('mean_annual_tmax_c')),
            'Tmin_c':     _fmt_num(et.get('mean_annual_tmin_c')),
            'StdTavg_c':  _fmt_num(et.get('std_annual_tavg_c')),
        })
        for line in pd.DataFrame(rows).to_string(index=False).splitlines():
            print(f"    {line}")

def _print_per_model_monthly_breakdown(result: Dict[str, Any]) -> None:
    """
    Show each model's MONTHLY long-term average, followed by the ENSEMBLE row (column-wise mean of the per-model rows). 
    """
    per_model = result.get('per_model_monthly_climatology') or {}
    if not per_model:
        return
    monthly_ensemble = result.get('monthly_climatology') or {}
    n = len(per_model)

    print(f"\n  {'─'*66}")
    print(f"  PER-MODEL MONTHLY BREAKDOWN ({n} model(s)) "
          f"→ feeds the ensemble monthly means")
    print(f"  {'─'*66}")

    def _build(metric_key: str, var_key: str) -> Optional[pd.DataFrame]:
        if not any((mc or {}).get(var_key) for mc in per_model.values()):
            return None
        rows: List[Dict[str, Any]] = []
        for name, mc in per_model.items():
            var_block = (mc or {}).get(var_key) or {}
            row: Dict[str, Any] = {'Model': name}
            for mo in range(1, 13):
                row[MONTH_LABELS[mo - 1]] = _fmt_num(
                    (var_block.get(mo) or {}).get(metric_key)
                )
            rows.append(row)
        ens_var = monthly_ensemble.get(var_key) or {}
        ens_row: Dict[str, Any] = {'Model': f"ENSEMBLE (mean of {n})"}
        for mo in range(1, 13):
            ens_row[MONTH_LABELS[mo - 1]] = _fmt_num(
                (ens_var.get(mo) or {}).get(metric_key)
            )
        rows.append(ens_row)
        return pd.DataFrame(rows)

    # Precipitation: one model × 12-month table (mean monthly total)
    df_p = _build('mean_monthly_total_mm', 'precipitation')
    if df_p is not None:
        print(f"\n  Precipitation — mean monthly total (mm):")
        for line in df_p.to_string(index=False).splitlines():
            print(f"    {line}")

    # Temperature: separate Tavg / Tmax / Tmin tables
    for metric_key, label in [
        ('mean_monthly_tavg_c', 'mean monthly Tavg (°C)'),
        ('mean_monthly_tmax_c', 'mean monthly Tmax (°C)'),
        ('mean_monthly_tmin_c', 'mean monthly Tmin (°C)'),
    ]:
        df_t = _build(metric_key, 'temperature')
        if df_t is not None:
            print(f"\n  Temperature — {label}:")
            for line in df_t.to_string(index=False).splitlines():
                print(f"    {line}")

def print_ensemble_climatology_report(result: Dict[str, Any]) -> None:
    """Print the per-model breakdowns (annual + monthly), then the ensemble report."""
    if 'error' in result:
        print(f"\nError: {result['error']}")
        for f in result.get('failed_models', []) or []:
            print(f"  - {f.get('model')}: {f.get('error')}")
        return

    n_ok = result.get('n_models_ok', 0)
    n_fail = len(result.get('models_failed') or [])
    print(f"\n{'='*70}")
    print(f"  ENSEMBLE CLIMATOLOGY: NEX-GDDP CMIP6  "
          f"| scenario={result.get('scenario')}  | {n_ok}/{n_ok + n_fail} models ok")
    print(f"{'='*70}")
    if result.get('models_failed'):
        failed_names = ', '.join(f['model'] for f in result['models_failed'])
        print(f"  Failed: {failed_names}")

    _print_per_model_climatology_breakdown(result)
    _print_per_model_monthly_breakdown(result)
    print_climatology_report(result)

def main():
    """Command-line interface for climatology analysis."""
    # Ensure Unicode output (✓, ✗, 📊) works on Windows consoles that default to cp1252.
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except (AttributeError, ValueError):
        pass

    parser = argparse.ArgumentParser(
        description='Calculate long-term climate normals (WMO 30-year standards)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Calculate 1991-2020 climatology (current WMO standard)
  # Calculate with JSON output
  # NEX-GDDP runs the 16-model ensemble (averaged); pick scenario(s) and models
        """
    )
    parser.add_argument('--location', required=True, type=str,
                       help='Location as "lat,lon" (e.g., "-1.286,36.817")')
    parser.add_argument('--start-year', required=True, type=int,
                       help='Start year of climatology period')
    parser.add_argument('--end-year', required=True, type=int,
                       help='End year of climatology period (inclusive)')
    parser.add_argument('--source', required=True, type=str,
                       help='Data source (e.g., nasa_power, chirps, chirts). '
                            "'nex_gddp' runs the CMIP6 ensemble (averaged across models).")
    parser.add_argument('--scenarios', type=str, default='ssp245',
                       metavar='ssp245[,ssp585]',
                       help='NEX-GDDP only. Comma-separated SSP scenarios. Canonical: '
                            f"{', '.join(SSP_SCENARIOS)} (default: ssp245). "
                            'Aliases also accepted: SSP1-2.6, SSP2-4.5, SSP5-8.5.')
    parser.add_argument('--models', type=str, default=None,
                       help='NEX-GDDP only. Comma-separated subset of CMIP6 models '
                            '(default: all 16).')
    parser.add_argument('--exclude-models', type=str, default=None,
                       help='NEX-GDDP only. Comma-separated CMIP6 models to drop.')
    parser.add_argument('--workers', type=int, default=8,
                       help='NEX-GDDP only. Parallel fetch workers across models '
                            '(default: 8; use 1 to disable parallelism).')
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

    plot_dir = args.output_dir if args.output_dir else None

    # NEX-GDDP -> ensemble across models, one result per requested scenario.
    if args.source == 'nex_gddp':
        sub_models = [m.strip() for m in args.models.split(',')] if args.models else None
        excl = [m.strip() for m in args.exclude_models.split(',')] if args.exclude_models else None

        raw_scenarios = [s.strip() for s in args.scenarios.split(',') if s.strip()]
        scenarios: List[str] = []
        invalid: List[str] = []
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
        multi = len(scenarios) > 1
        verbose_text = (args.format != 'json')

        if multi and verbose_text:
            print(f"\n{'#'*70}")
            print(f"  MULTI-SCENARIO RUN ({len(scenarios)} scenarios): "
                  f"{', '.join(scenarios)}")
            print(f"  Location: ({lat:.4f}, {lon:.4f})   "
                  f"Period: {args.start_year}-{args.end_year}")
            print(f"  Reports will be printed together below, after all "
                  f"fetches finish.")
            print(f"{'#'*70}")

        # --- Phase 1: fetch each scenario, defer report printing ---
        for idx, scenario in enumerate(scenarios, 1):
            if multi and verbose_text:
                print(f"\n{'>'*70}")
                print(f"  >>>  FETCHING SCENARIO {idx}/{len(scenarios)}: "
                      f"{scenario}  <<<")
                print(f"{'>'*70}")
            with _quiet_fetch_logs():
                result = calculate_climatology_ensemble(
                    location_coord=(lat, lon),
                    start_year=args.start_year,
                    end_year=args.end_year,
                    scenario=scenario,
                    models=sub_models,
                    exclude_models=excl,
                    output_dir=plot_dir,
                    verbose=verbose_text,
                    max_workers=args.workers,
                )
            all_results[scenario] = result
            if 'error' not in result:
                any_ok = True

        # --- Phase 2: print every scenario's report, back-to-back ---
        if verbose_text:
            if multi:
                print(f"\n{'#'*70}")
                print(f"  RESULTS — ALL {len(scenarios)} SCENARIOS BELOW, IN ORDER")
                print(f"{'#'*70}")
            for idx, scenario in enumerate(scenarios, 1):
                if multi:
                    print(f"\n{'='*70}")
                    print(f"  ===  RESULTS FOR SCENARIO {idx}/{len(scenarios)}: "
                          f"{scenario}  ===")
                    print(f"{'='*70}")
                print_ensemble_climatology_report(all_results[scenario])

        # --- Phase 3: cross-scenario summary ---
        if multi and verbose_text:
            print(f"\n{'#'*70}")
            print(f"  CROSS-SCENARIO SUMMARY ({len(scenarios)} scenarios)")
            print(f"{'#'*70}")
            rows: List[Dict[str, Any]] = []
            for sc, r in all_results.items():
                if 'error' in r:
                    rows.append({
                        'Scenario': sc,
                        'Status': f"FAILED ({r['error']})",
                        'AnnPrecip_mm': 'n/a',
                        'Tavg_c': 'n/a',
                        'Tmax_c': 'n/a',
                        'Tmin_c': 'n/a',
                    })
                    continue
                cl = r.get('climatology') or {}
                p = cl.get('precipitation') or {}
                t = cl.get('temperature') or {}
                n_ok = r.get('n_models_ok', 0)
                n_fail = len(r.get('models_failed') or [])
                rows.append({
                    'Scenario':     sc,
                    'Status':       f"{n_ok}/{n_ok + n_fail} models ok",
                    'AnnPrecip_mm': _fmt_num(p.get('mean_annual_total_mm')),
                    'Tavg_c':       _fmt_num(t.get('mean_annual_tavg_c')),
                    'Tmax_c':       _fmt_num(t.get('mean_annual_tmax_c')),
                    'Tmin_c':       _fmt_num(t.get('mean_annual_tmin_c')),
                })
            for line in pd.DataFrame(rows).to_string(index=False).splitlines():
                print(f"  {line}")
            print(f"{'#'*70}\n")

        payload = (all_results[scenarios[0]] if len(scenarios) == 1 else all_results)
        if args.format == 'json':
            output = json.dumps(payload, indent=2, default=str)
            if args.output:
                with open(args.output, 'w') as f:
                    f.write(output)
                print(f"\n✓ Climatology saved to {args.output}")
            else:
                print(output)
        elif args.output:
            with open(args.output, 'w') as f:
                json.dump(payload, f, indent=2, default=str)
            print(f"✓ JSON data saved to {args.output}")

        if not any_ok:
            sys.exit(1)
        return

    # Single-source climatology (non-NEX-GDDP)
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
# python -m climate_tookit.climatology.long_term_climatology --location="-1.286,36.817" --start-year 1991 --end-year 2020 --source nasa_power

# NEX-GDDP runs the 16-model CMIP6 ensemble (averaged); the 1st picks scenarios, the 2nd also subsets models:
# python -m climate_tookit.climatology.long_term_climatology --location="-1.286,36.817" --start-year 2040 --end-year 2069 --source nex_gddp --scenarios ssp245,ssp585
# python -m climate_tookit.climatology.long_term_climatology --location="-1.286,36.817" --start-year 2040 --end-year 2069 --source nex_gddp --scenarios ssp585 --models "ACCESS-CM2,EC-Earth3,MRI-ESM2-0"

# With JSON output
# python -m climate_tookit.climatology.long_term_climatology --location="-1.286,36.817" --start-year 1991 --end-year 2020 --source nasa_power --format json --output climatology_1991-2020.json