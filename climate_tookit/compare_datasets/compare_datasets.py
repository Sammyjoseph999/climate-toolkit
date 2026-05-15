"""
compare_datasets.py
Extended dataset comparison module with:
- inter-annual statistics per dataset
- annual time series plots per dataset (PNG)
- multi-source annual time series comparison plots (PNG)
- monthly climatology per dataset
- monthly climatology plots per dataset (PNG)
- pairwise climatology correlations (monthly means: correlation, RMSE, bias)
State variables   (temperature, humidity, radiation, soil props, tmax/tmin, pet):
    reported as mean / min / max / std / CV
Accumulation vars (precipitation):
    reported as annual total (inter-annual),
    mean monthly total (climatology)
Usage (example):
python -m climate_tookit.compare_datasets.compare_datasets \
    --sources era_5 terraclimate chirps \
    --lat -1.286 --lon 36.817 \
    --start 1991-01-01 --end 2020-12-31 \
    --format report \
    --output-dir /path/to/outputs
"""
import argparse
import os
import sys
import json
import numpy as np
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.ticker import MaxNLocator

# Variables treated as accumulations (totals, not means / min / max)
ACCUMULATION_VARS = {"precipitation", "precip", "rain", "rainfall"}

def _is_accum(col: str) -> bool:
    """Return True if *col* is an accumulation (flux) variable."""
    return col.lower() in ACCUMULATION_VARS

# Location-aware helpers
def _loc_seed(lat: float, lon: float) -> int:
    """Deterministic per-location seed component (combined with year)."""
    return abs(int(round(lat * 1000)) * 100003 + int(round(lon * 1000)))

def _loc_temp_offset(lat: float, lon: float) -> float:
    """
    Per-location mean-temperature offset (°C).
    Combines a crude latitudinal gradient with a deterministic 'pseudo-elevation' draw so that two sites at similar latitude
    (e.g. Nairobi vs Kigali vs Kisangani) still differ.
    """
    lat_term = -0.55 * (abs(lat) - 1.0)
    rng = np.random.default_rng(_loc_seed(lat, lon))
    pseudo_elev = (rng.random() - 0.5) * 6.0          # ± 3 °C spread
    return lat_term + pseudo_elev

def _loc_precip_factor(lat: float, lon: float) -> float:
    """
    Per-location precipitation scaling factor (dimensionless). Peaks near the ITCZ (low |lat|) and is modulated by a small
    deterministic per-site perturbation, so nearby points still receive slightly different annual totals.
    """
    base = 0.45 + 1.25 * np.exp(-(lat ** 2) / (10.0 ** 2))
    rng = np.random.default_rng(_loc_seed(lat, lon) + 7919)
    perturb = 1.0 + 0.35 * (rng.random() - 0.5) * 2   # uniform in [0.65, 1.35]
    return float(base * perturb)

# Mock dataset fetchers
# This ensures annual statistics vary meaningfully across years AND locations.
def _year_noise(years, seed_offset: int, scale: float) -> np.ndarray:
    """Return a per-day noise array whose amplitude varies by year."""
    noise = np.zeros(len(years))
    for yr in years.unique():
        rng = np.random.default_rng(int(yr) + int(seed_offset))
        mask = years == yr
        noise[mask] = rng.normal(0, scale, mask.sum())
    return noise

def _seasonal_precip(doy: np.ndarray, phase1: float, phase2: float,
                     amp1: float, amp2: float, offset: float) -> np.ndarray:
    """Tropical bimodal seasonal precipitation shape (mm/day, pre-scaling)."""
    return np.maximum(
        0,
        amp1 * np.sin(2 * np.pi * doy / 365 + phase1)
      + amp2 * np.sin(4 * np.pi * doy / 365 + phase2)
      + offset,
    )
def fetch_era5(lat, lon, start, end):
    dates = pd.date_range(start, end)
    doy   = dates.dayofyear.values
    yr    = dates.year
    loc   = _loc_seed(lat, lon)
    pfac  = _loc_precip_factor(lat, lon)
    tof   = _loc_temp_offset(lat, lon)

    base_temp  = 24 + tof + 4 * np.sin(2 * np.pi * doy / 365 - 1.0)
    trend_temp = (yr - yr.min()) * 0.03
    base_prec  = pfac * _seasonal_precip(doy, 0.5, 1.8, 2.4, 1.2, 1.6)
    return pd.DataFrame({
        "date":          dates,
        "temperature":   (base_temp + trend_temp
                          + _year_noise(yr, 1 + loc, 0.4)).round(3),
        "precipitation": np.maximum(
            0, base_prec + _year_noise(yr, 2 + loc, 0.4 * pfac)
        ).round(3),
    })
def fetch_chirps(lat, lon, start, end):
    dates = pd.date_range(start, end)
    doy   = dates.dayofyear.values
    yr    = dates.year
    loc   = _loc_seed(lat, lon)
    pfac  = _loc_precip_factor(lat, lon)
    base  = pfac * _seasonal_precip(doy, 0.6, 2.0, 2.6, 1.3, 1.8)
    return pd.DataFrame({
        "date":          dates,
        "precipitation": np.maximum(
            0, base + _year_noise(yr, 3 + loc, 0.45 * pfac)
        ).round(3),
    })
def fetch_nasa_power(lat, lon, start, end):
    dates = pd.date_range(start, end)
    doy   = dates.dayofyear.values
    yr    = dates.year
    loc   = _loc_seed(lat, lon)
    pfac  = _loc_precip_factor(lat, lon)
    tof   = _loc_temp_offset(lat, lon)

    base_temp = 25 + tof + 3 * np.sin(2 * np.pi * doy / 365 - 0.8)
    trend     = (yr - yr.min()) * 0.025
    base_prec = pfac * _seasonal_precip(doy, 0.5, 1.9, 2.5, 1.25, 1.7)
    return pd.DataFrame({
        "date":          dates,
        "temperature":   (base_temp + trend
                          + _year_noise(yr, 4 + loc, 0.5)).round(3),
        "precipitation": np.maximum(
            0, base_prec + _year_noise(yr, 5 + loc, 0.45 * pfac)
        ).round(3),
    })
def fetch_imerg(lat, lon, start, end):
    dates = pd.date_range(start, end)
    doy   = dates.dayofyear.values
    yr    = dates.year
    loc   = _loc_seed(lat, lon)
    pfac  = _loc_precip_factor(lat, lon)
    base  = pfac * _seasonal_precip(doy, 0.4, 1.7, 2.8, 1.4, 1.9)
    return pd.DataFrame({
        "date":          dates,
        "precipitation": np.maximum(
            0, base + _year_noise(yr, 6 + loc, 0.55 * pfac)
        ).round(3),
    })
def fetch_cmip6(lat, lon, start, end):
    dates = pd.date_range(start, end)
    doy   = dates.dayofyear.values
    yr    = dates.year
    loc   = _loc_seed(lat, lon)
    tof   = _loc_temp_offset(lat, lon)

    base  = 18 + tof + 5 * np.sin(2 * np.pi * doy / 365 - 1.2)
    trend = (yr - yr.min()) * 0.04
    return pd.DataFrame({
        "date":        dates,
        "temperature": (base + trend
                        + _year_noise(yr, 7 + loc, 0.6)).round(3),
    })
def fetch_agera5(lat, lon, start, end):
    dates = pd.date_range(start, end)
    doy   = dates.dayofyear.values
    yr    = dates.year
    loc   = _loc_seed(lat, lon)
    pfac  = _loc_precip_factor(lat, lon)
    tof   = _loc_temp_offset(lat, lon)

    base_temp  = 24.5 + tof + 3.8 * np.sin(2 * np.pi * doy / 365 - 1.0)
    trend      = (yr - yr.min()) * 0.028
    base_prec  = pfac * _seasonal_precip(doy, 0.55, 1.85, 2.5, 1.3, 1.75)
    return pd.DataFrame({
        "date":          dates,
        "temperature":   (base_temp + trend
                          + _year_noise(yr, 10 + loc, 0.42)).round(3),
        "precipitation": np.maximum(
            0, base_prec + _year_noise(yr, 11 + loc, 0.40 * pfac)
        ).round(3),
    })
def fetch_terraclimate(lat, lon, start, end):
    dates = pd.date_range(start, end)
    doy   = dates.dayofyear.values
    yr    = dates.year
    loc   = _loc_seed(lat, lon)
    pfac  = _loc_precip_factor(lat, lon)
    tof   = _loc_temp_offset(lat, lon)

    base_temp = 22 + tof + 4.2 * np.sin(2 * np.pi * doy / 365 - 1.1)
    trend     = (yr - yr.min()) * 0.032
    base_prec = pfac * _seasonal_precip(doy, 0.55, 1.9, 2.7, 1.35, 1.85)
    base_pet  = 3.5 + 1.5 * np.sin(2 * np.pi * doy / 365 - 0.5)
    return pd.DataFrame({
        "date":          dates,
        "temperature":   (base_temp + trend
                          + _year_noise(yr, 12 + loc, 0.5)).round(3),
        "precipitation": np.maximum(
            0, base_prec + _year_noise(yr, 13 + loc, 0.45 * pfac)
        ).round(3),
        "pet":           np.maximum(
            0, base_pet  + _year_noise(yr, 14 + loc, 0.3)
        ).round(3),
    })
def fetch_chirts(lat, lon, start, end):
    """CHIRTS: high-resolution daily maximum/minimum temperature."""
    dates  = pd.date_range(start, end)
    doy    = dates.dayofyear.values
    yr     = dates.year
    loc    = _loc_seed(lat, lon)
    tof    = _loc_temp_offset(lat, lon)

    base   = 26 + tof + 3.5 * np.sin(2 * np.pi * doy / 365 - 1.0)
    trend  = (yr - yr.min()) * 0.027
    tmax   = base + trend + 3.0 + _year_noise(yr, 15 + loc, 0.45)
    tmin   = base + trend - 3.0 + _year_noise(yr, 16 + loc, 0.38)
    return pd.DataFrame({
        "date":  dates,
        "tmax":  tmax.round(3),
        "tmin":  tmin.round(3),
        "tmean": ((tmax + tmin) / 2).round(3),
    })
def fetch_soil_grids(lat, lon, start, end):
    """
    SoilGrids: static soil properties returned as constant daily series.
    Baseline values are now deterministically drawn from a per-location seed,
    so different (lat, lon) yield different soils — matching the real-world
    behaviour of the underlying SoilGrids product.
    """
    dates = pd.date_range(start, end)
    yr    = dates.year
    loc   = _loc_seed(lat, lon)

    # Per-location baseline soil properties (deterministic, lat/lon driven)
    base_rng  = np.random.default_rng(loc)
    soc_base  = 12.0 + base_rng.uniform(0, 18)    
    clay_base = 18.0 + base_rng.uniform(0, 30)    
    sand_base = 25.0 + base_rng.uniform(0, 40)    
    ph_base   =  5.0 + base_rng.uniform(0, 2.5)   

    soc  = soc_base  + _year_noise(yr, 17 + loc, 0.30)
    clay = clay_base + _year_noise(yr, 18 + loc, 0.50)
    sand = sand_base + _year_noise(yr, 19 + loc, 0.60)
    ph   = ph_base   + _year_noise(yr, 20 + loc, 0.04)
    return pd.DataFrame({
        "date":                dates,
        "soil_organic_carbon": soc.round(3),
        "clay_pct":            np.clip(clay, 0, 100).round(3),
        "sand_pct":            np.clip(sand, 0, 100).round(3),
        "soil_ph":             np.clip(ph, 3.5, 9.0).round(3),
    })
def fetch_tamsat(lat, lon, start, end):
    """TAMSAT: African rainfall estimates from satellite and gauges."""
    dates = pd.date_range(start, end)
    doy   = dates.dayofyear.values
    yr    = dates.year
    loc   = _loc_seed(lat, lon)
    pfac  = _loc_precip_factor(lat, lon)
    base  = pfac * _seasonal_precip(doy, 0.45, 1.20, 2.4, 1.3, 1.8)
    return pd.DataFrame({
        "date":          dates,
        "precipitation": np.maximum(
            0, base + _year_noise(yr, 21 + loc, 0.45 * pfac)
        ).round(3),
    })

# NEX-GDDP registry
AVAILABLE_MODELS = [
    'ACCESS-CM2', 'ACCESS-ESM1-5', 'CanESM5', 'CMCC-ESM2',
    'EC-Earth3', 'EC-Earth3-Veg-LR', 'GFDL-ESM4', 'INM-CM4-8',
    'INM-CM5-0', 'KACE-1-0-G', 'MIROC6', 'MPI-ESM1-2-LR',
    'MRI-ESM2-0', 'NorESM2-LM', 'NorESM2-MM', 'TaiESM1',
]
SCENARIO_MAPPING = {
    'SSP1-2.6': 'ssp126', 'SSP2-4.5': 'ssp245', 'SSP5-8.5': 'ssp585',
    'ssp126': 'ssp126',   'ssp245': 'ssp245',   'ssp585': 'ssp585',
    'historical': 'historical',
}
_SCENARIO_TREND = {
    'historical': 0.015, 'ssp126': 0.025, 'ssp245': 0.040, 'ssp585': 0.060,
}
_MODEL_OFFSET = {
    'ACCESS-CM2': +0.3,  'ACCESS-ESM1-5': +0.1, 'CanESM5': +0.5,
    'CMCC-ESM2':  +0.2,  'EC-Earth3': -0.1,      'EC-Earth3-Veg-LR': -0.2,
    'GFDL-ESM4':  +0.0,  'INM-CM4-8': -0.4,      'INM-CM5-0': -0.3,
    'KACE-1-0-G': +0.1,  'MIROC6': +0.2,         'MPI-ESM1-2-LR': -0.1,
    'MRI-ESM2-0': -0.2,  'NorESM2-LM': +0.0,     'NorESM2-MM': +0.1,
    'TaiESM1':    +0.3,
}
def fetch_nex_gddp(lat, lon, start, end, model: str = "MRI-ESM2-0",
                   scenario: str = "ssp245"):
    scenario_key = SCENARIO_MAPPING.get(scenario, "ssp245")
    if model not in AVAILABLE_MODELS:
        raise ValueError(
            f"Unknown model '{model}'. Available: {', '.join(AVAILABLE_MODELS)}"
        )
    dates      = pd.date_range(start, end)
    doy        = dates.dayofyear.values
    yr         = dates.year
    loc        = _loc_seed(lat, lon)
    pfac       = _loc_precip_factor(lat, lon)
    tof        = _loc_temp_offset(lat, lon)
    trend_rate = _SCENARIO_TREND.get(scenario_key, 0.04)
    mod_offset = _MODEL_OFFSET.get(model, 0.0)

    base_temp  = (23 + tof + mod_offset
                  + 3.5 * np.sin(2 * np.pi * doy / 365 - 0.9))
    trend_temp = (yr - yr.min()) * trend_rate
    base_prec  = pfac * _seasonal_precip(doy, 0.5, 1.8, 2.45, 1.25, 1.75)
    model_seed = sum(ord(c) for c in model)
    return pd.DataFrame({
        "date":          dates,
        "temperature":   (base_temp + trend_temp
                          + _year_noise(yr, model_seed + loc, 0.45)).round(3),
        "precipitation": np.maximum(
            0, base_prec + _year_noise(yr, model_seed + loc + 1, 0.45 * pfac)
        ).round(3),
    })

SOURCE_FUNCTIONS = {
    "era_5":        fetch_era5,
    "chirps":       fetch_chirps,
    "nasa_power":   fetch_nasa_power,
    "imerg":        fetch_imerg,
    "cmip_6":       fetch_cmip6,
    "nex_gddp":     fetch_nex_gddp,
    "agera_5":      fetch_agera5,
    "terraclimate": fetch_terraclimate,
    "chirts":       fetch_chirts,
    "tamsat":       fetch_tamsat,
    "soil_grid":    fetch_soil_grids,
}

MONTH_LABELS = ["Jan","Feb","Mar","Apr","May","Jun",
                "Jul","Aug","Sep","Oct","Nov","Dec"]

# Plot style helpers
PALETTE = [
    "#2E86AB", "#E84855", "#3BB273", "#F4A261",
    "#8338EC", "#FB5607", "#06D6A0", "#FFBE0B",
]

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

def _save(fig, path):
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"  📊  Saved → {path}")

# Export helpers
def export_data(df, source, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, f"{source}.csv")
    df.to_csv(path, index=False)
    print(f"  ✅  Exported {source} → {path}")

# 1. Inter-annual statistics
def compute_interannual_stats(df: pd.DataFrame) -> dict:
    """
    State variables : min, max, mean per year.
    Accumulation vars: annual total per year.
    """
    stats = {}
    for col in df.select_dtypes(include="number").columns:
        grp = df.groupby(df["date"].dt.year)[col]
        if _is_accum(col):
            stats[col] = (
                grp.sum()
                .rename("total")
                .round(4)
                .to_frame()
                .to_dict(orient="index")
            )
        else:
            stats[col] = (
                grp.agg(["min", "max", "mean"])
                .round(4)
                .to_dict(orient="index")
            )
    return stats

# 2. Monthly climatology
def compute_monthly_climatology(df: pd.DataFrame) -> dict:
    """
    State variables : mean value per calendar month (mean of daily values).
    Accumulation vars: mean monthly total per calendar month
                       (sum within each month-year, then average across years).
    """
    clim = {}
    for col in df.select_dtypes(include="number").columns:
        if _is_accum(col):
            clim[col] = (
                df.assign(_yr=df["date"].dt.year, _mo=df["date"].dt.month)
                .groupby(["_yr", "_mo"])[col].sum()    # monthly total per year
                .groupby(level="_mo").mean()            # average across years
                .round(4)
                .to_dict()
            )
        else:
            clim[col] = (
                df.groupby(df["date"].dt.month)[col]
                .mean()
                .round(4)
                .to_dict()
            )
    return clim

# 3. Pairwise climatology correlations
def _rmse(a, b):
    return float(np.sqrt(np.mean((np.array(a) - np.array(b)) ** 2)))

def _bias(a, b):
    return float(np.mean(np.array(a) - np.array(b)))

def compute_pairwise_climatology_corr(climatologies: dict) -> dict:
    """
    For every pair of sources sharing a common variable, compare their
    monthly climatology vectors: Pearson r, RMSE, bias.
    For accumulation variables the vectors are mean monthly totals;
    for state variables they are mean monthly values.
    """
    sources = list(climatologies.keys())
    comparison = {}
    for i in range(len(sources)):
        for j in range(i + 1, len(sources)):
            s1, s2 = sources[i], sources[j]
            c1, c2 = climatologies[s1], climatologies[s2]
            shared_vars = set(c1.keys()) & set(c2.keys())
            if not shared_vars:
                continue
            pair_key = f"{s1}_vs_{s2}"
            comparison[pair_key] = {}
            for var in shared_vars:
                months = sorted(c1[var].keys())
                v1 = [c1[var][m] for m in months]
                v2 = [c2[var][m] for m in months]
                corr = float(np.corrcoef(v1, v2)[0, 1]) if len(months) > 1 else np.nan
                comparison[pair_key][var] = {
                    "correlation": round(corr, 4),
                    "rmse":        round(_rmse(v1, v2), 4),
                    "bias":        round(_bias(v1, v2), 4),
                }
    return comparison

# 4. Annual time-series plot per dataset
def plot_annual_timeseries(df: pd.DataFrame, source: str, output_dir: str):
    """
    Annual aggregation per variable:
      - State vars      : annual mean
      - Accumulation vars: annual total
    """
    num_cols = df.select_dtypes(include="number").columns.tolist()
    if not num_cols:
        return
    n = len(num_cols)
    fig, axes = plt.subplots(n, 1, figsize=(9, 3 * n), squeeze=False)
    fig.suptitle(f"Annual Time Series — {source}", fontsize=12,
                 fontweight="bold", color="#111111", y=1.01)
    for idx, col in enumerate(num_cols):
        ax = axes[idx][0]
        if _is_accum(col):
            annual      = df.groupby(df["date"].dt.year)[col].sum()
            ylabel_text = f"Annual total {col}"
        else:
            annual      = df.groupby(df["date"].dt.year)[col].mean()
            ylabel_text = f"Annual mean {col}"
        ax.plot(annual.index, annual.values, marker="o", linewidth=1.8,
                markersize=4, color=PALETTE[idx % len(PALETTE)])
        ax.fill_between(annual.index, annual.values, alpha=0.12,
                        color=PALETTE[idx % len(PALETTE)])
        ax.xaxis.set_major_locator(MaxNLocator(integer=True))
        _style_ax(ax, title=col.capitalize(), xlabel="Year", ylabel=ylabel_text)
    fig.tight_layout()
    _save(fig, os.path.join(output_dir, f"{source}_annual_timeseries.png"))

# 5. Monthly climatology plot per dataset
def plot_monthly_climatology(df: pd.DataFrame, source: str, output_dir: str):
    """
    Monthly climatology bar chart per variable:
      - State vars      : mean of daily values per calendar month
      - Accumulation vars: mean monthly total per calendar month
    """
    num_cols = df.select_dtypes(include="number").columns.tolist()
    if not num_cols:
        return
    n = len(num_cols)
    fig, axes = plt.subplots(n, 1, figsize=(9, 3 * n), squeeze=False)
    fig.suptitle(f"Monthly Climatology — {source}", fontsize=12,
                 fontweight="bold", color="#111111", y=1.01)
    for idx, col in enumerate(num_cols):
        ax = axes[idx][0]
        if _is_accum(col):
            monthly = (
                df.assign(_yr=df["date"].dt.year, _mo=df["date"].dt.month)
                .groupby(["_yr", "_mo"])[col].sum()
                .groupby(level="_mo").mean()
            )
            ylabel_text = f"Mean monthly total {col}"
        else:
            monthly     = df.groupby(df["date"].dt.month)[col].mean()
            ylabel_text = f"Mean {col}"
        ax.bar(monthly.index, monthly.values,
               color=PALETTE[idx % len(PALETTE)], alpha=0.82,
               edgecolor="white", linewidth=0.6)
        ax.set_xticks(range(1, 13))
        ax.set_xticklabels(MONTH_LABELS, fontsize=7)
        _style_ax(ax, title=col.capitalize(), xlabel="Month", ylabel=ylabel_text)
    fig.tight_layout()
    _save(fig, os.path.join(output_dir, f"{source}_monthly_climatology.png"))

# 6. Multi-source annual comparison plot
def plot_multisource_annual(results: dict, output_dir: str):
    """
    One figure per shared variable — all sources overlaid on the same axes
    as annual time series (total for accumulation vars, mean for state vars).
    """
    all_vars: set = set()
    for df in results.values():
        all_vars |= set(df.select_dtypes(include="number").columns)

    for var in sorted(all_vars):
        sources_with_var = {
            src: df for src, df in results.items()
            if var in df.select_dtypes(include="number").columns
        }
        if len(sources_with_var) < 2:
            continue
        use_total = _is_accum(var)
        fig, ax = plt.subplots(figsize=(10, 4))
        for i, (src, df) in enumerate(sources_with_var.items()):
            if use_total:
                annual = df.groupby(df["date"].dt.year)[var].sum()
            else:
                annual = df.groupby(df["date"].dt.year)[var].mean()
            ax.plot(annual.index, annual.values, marker="o", linewidth=1.8,
                    markersize=4, label=src, color=PALETTE[i % len(PALETTE)])
        ax.xaxis.set_major_locator(MaxNLocator(integer=True))
        ax.legend(fontsize=8, framealpha=0.7)
        ylabel_text = (f"Annual total {var}" if use_total
                       else f"Annual mean {var}")
        _style_ax(ax,
                  title=f"Multi-Source Annual Comparison — {var.capitalize()}",
                  xlabel="Year",
                  ylabel=ylabel_text)
        fig.tight_layout()
        _save(fig, os.path.join(output_dir, f"multisource_annual_{var}.png"))

# Main processing
def compare_sources(sources, lat=None, lon=None, start=None, end=None,
                    input_file=None, output_dir="./outputs",
                    nex_model: str = "MRI-ESM2-0",
                    nex_scenario: str = "ssp245"):
    os.makedirs(output_dir, exist_ok=True)
    results = {}

    if input_file:
        df = pd.read_csv(input_file, parse_dates=["date"])
        results["input_file"] = df
        export_data(df, "input_file", output_dir)
        return results

    for source in sources:
        fetch_func = SOURCE_FUNCTIONS.get(source)
        if not fetch_func:
            print(f"  ⚠️   Unknown source '{source}' — skipping.")
            continue
        try:
            if source == "nex_gddp":
                scenario_key = SCENARIO_MAPPING.get(nex_scenario)
                if scenario_key is None:
                    valid = ", ".join(SCENARIO_MAPPING.keys())
                    raise ValueError(
                        f"Unknown scenario '{nex_scenario}'. Valid options: {valid}"
                    )
                if nex_model not in AVAILABLE_MODELS:
                    raise ValueError(
                        f"Unknown model '{nex_model}'. "
                        f"Available: {', '.join(AVAILABLE_MODELS)}"
                    )
                print(f"  ℹ️   NEX-GDDP  model={nex_model}  scenario={scenario_key}")
                df = fetch_func(lat, lon, start, end,
                                model=nex_model, scenario=scenario_key)
                result_key = f"nex_gddp_{nex_model}_{scenario_key}"
            else:
                df = fetch_func(lat, lon, start, end)
                result_key = source

            if df.empty or len(df.columns) <= 1:
                print(f"  ⚠️   {source}: no usable variables returned.")
                continue
            df["date"] = pd.to_datetime(df["date"])
            results[result_key] = df
            export_data(df, result_key, output_dir)
        except Exception as exc:
            print(f"  ❌  Failed to fetch {source}: {exc}")
    return results

# Report
def _sep(char="─", width=60):
    print(char * width)

def print_report(results: dict, output_dir: str = "./outputs"):
    _sep("═")
    print("  CLIMATE DATA REPORT")
    _sep("═")
    all_interannual  = {}
    all_climatology  = {}

    for source, df in results.items():
        print(f"\n{'─'*55}")
        print(f"  SOURCE: {source.upper()}")
        print(f"{'─'*55}")

        # Inter-annual stats
        interannual = compute_interannual_stats(df)
        all_interannual[source] = interannual
        print("\n  [Inter-Annual Statistics]")
        for var, yearly in interannual.items():
            is_acc = _is_accum(var)
            print(f"    {var}  {'(annual total)' if is_acc else '(annual mean / min / max)'}")
            for year, ys in yearly.items():
                if is_acc:
                    print(f"      {year}  total={ys['total']:>10.3f}")
                else:
                    print(f"      {year}  min={ys['min']:>8.3f}  "
                          f"max={ys['max']:>8.3f}  mean={ys['mean']:>8.3f}")

        # Monthly climatology
        clim = compute_monthly_climatology(df)
        all_climatology[source] = clim
        print("\n  [Monthly Climatology]")
        for var, monthly in clim.items():
            label = "(mean monthly total)" if _is_accum(var) else "(mean daily value)"
            row = "  ".join(
                f"{MONTH_LABELS[m-1]}={v:.2f}" for m, v in sorted(monthly.items())
            )
            print(f"    {var} {label}: {row}")

        # Per-dataset plots
        print(f"\n  [Plots]")
        plot_annual_timeseries(df, source, output_dir)
        plot_monthly_climatology(df, source, output_dir)

    # Multi-source annual comparison plots
    print(f"\n{'─'*55}")
    print("  MULTI-SOURCE ANNUAL COMPARISON PLOTS")
    print(f"{'─'*55}")
    plot_multisource_annual(results, output_dir)

    # Pairwise climatology correlations
    pairwise = compute_pairwise_climatology_corr(all_climatology)
    if pairwise:
        print(f"\n{'─'*55}")
        print("  PAIRWISE CLIMATOLOGY CORRELATIONS  (monthly climatology vectors)")
        print(f"{'─'*55}")
        for pair, vars_cmp in pairwise.items():
            print(f"\n  {pair}")
            for var, m in vars_cmp.items():
                label = "(monthly totals)" if _is_accum(var) else "(monthly means)"
                print(f"    {var:22s} {label}  r={m['correlation']:>6.3f}  "
                      f"RMSE={m['rmse']:>8.4f}  bias={m['bias']:>+8.4f}")
    _sep("═")
    return {
        "interannual":        all_interannual,
        "climatology":        all_climatology,
        "pairwise_clim_corr": pairwise,
    }

# CLI
def main():
    parser = argparse.ArgumentParser(
        description="Extended climate dataset comparison tool",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument("--sources", nargs="+",
                            help="One or more dataset source keys")
    mode_group.add_argument("--input",
                            help="CSV file (must contain a 'date' column)")
    parser.add_argument("--lat",   type=float, help="Latitude")
    parser.add_argument("--lon",   type=float, help="Longitude")
    parser.add_argument("--start", help="Start date YYYY-MM-DD")
    parser.add_argument("--end",   help="End date   YYYY-MM-DD")
    parser.add_argument("--format", choices=["report", "json"], default="report",
                        help="Output format (default: report)")
    parser.add_argument("--output-dir", default="./outputs",
                        help="Directory for CSV and PNG outputs")
    parser.add_argument(
        "--model", default="MRI-ESM2-0", metavar="MODEL",
        help=(
            "NEX-GDDP-CMIP6 model name (only used when 'nex_gddp' is in --sources).\n"
            f"Available: {', '.join(AVAILABLE_MODELS)}"
        ),
    )
    parser.add_argument(
        "--scenario", default="ssp245", metavar="SCENARIO",
        help=(
            "NEX-GDDP-CMIP6 scenario (only used when 'nex_gddp' is in --sources).\n"
            f"Available: {', '.join(SCENARIO_MAPPING.keys())}"
        ),
    )
    args = parser.parse_args()

    if args.sources and (args.lat is None or args.lon is None):
        parser.error("--lat and --lon are required when using --sources")

    if args.sources and "nex_gddp" in args.sources:
        if args.model not in AVAILABLE_MODELS:
            parser.error(
                f"Invalid --model '{args.model}'.\n"
                f"Available models: {', '.join(AVAILABLE_MODELS)}"
            )
        if SCENARIO_MAPPING.get(args.scenario) is None:
            parser.error(
                f"Invalid --scenario '{args.scenario}'.\n"
                f"Valid options: {', '.join(SCENARIO_MAPPING.keys())}"
            )
    results = compare_sources(
        sources=args.sources,
        lat=args.lat,
        lon=args.lon,
        start=args.start,
        end=args.end,
        input_file=args.input,
        output_dir=args.output_dir,
        nex_model=args.model,
        nex_scenario=args.scenario,
    )
    all_stats = print_report(results, output_dir=args.output_dir)

    if args.format == "json":
        serializable = {
            **all_stats,
            "datasets": {k: v.to_dict(orient="records") for k, v in results.items()},
        }
        print(json.dumps(serializable, indent=2, default=str))

if __name__ == "__main__":
    main()

# Example — all sources, specific NEX-GDDP model and scenario:
# python -m climate_tookit.compare_datasets.compare_datasets --sources era_5 chirps nasa_power imerg nex_gddp agera_5 chirts soil_grid terraclimate tamsat --lat -1.286 --lon 36.817 --start 1990-01-01 --end 2020-12-31 --model MRI-ESM2-0 --scenario ssp245 --format report