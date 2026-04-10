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

# Mock dataset fetchers
# Each fetcher adds:
#   • a seasonal cycle   (sin-based, tied to day-of-year)
#   • inter-annual trend (slow drift tied to year)
#   • year-level noise   (reproducible via per-year seed)
# This ensures annual statistics vary meaningfully across years.

def _year_noise(years, seed_offset: int, scale: float) -> np.ndarray:
    """Return a per-day noise array whose amplitude varies by year."""
    noise = np.zeros(len(years))
    for yr in years.unique():
        rng = np.random.default_rng(int(yr) + seed_offset)
        mask = years == yr
        noise[mask] = rng.normal(0, scale, mask.sum())
    return noise

def fetch_era5(lat, lon, start, end):
    dates = pd.date_range(start, end)
    doy   = dates.dayofyear.values
    yr    = dates.year
    base_temp  = 24 + 4 * np.sin(2 * np.pi * doy / 365 - 1.0)
    trend_temp = (yr - yr.min()) * 0.03       
    base_prec  = np.maximum(0, 1.2 * np.sin(2 * np.pi * doy / 365 + 0.5) + 0.8)
    return pd.DataFrame({
        "date":          dates,
        "temperature":   (base_temp + trend_temp + _year_noise(yr, 1, 0.4)).round(3),
        "precipitation": np.maximum(0, base_prec + _year_noise(yr, 2, 0.15)).round(3),
    })

def fetch_chirps(lat, lon, start, end):
    dates = pd.date_range(start, end)
    doy   = dates.dayofyear.values
    yr    = dates.year
    base  = np.maximum(0, 1.8 * np.sin(2 * np.pi * doy / 365 + 0.6) + 1.0)
    return pd.DataFrame({
        "date":          dates,
        "precipitation": np.maximum(0, base + _year_noise(yr, 3, 0.25)).round(3),
    })

def fetch_nasa_power(lat, lon, start, end):
    dates = pd.date_range(start, end)
    doy   = dates.dayofyear.values
    yr    = dates.year
    base_temp = 25 + 3 * np.sin(2 * np.pi * doy / 365 - 0.8)
    trend     = (yr - yr.min()) * 0.025
    base_rad  = 110 + 12 * np.sin(2 * np.pi * doy / 365 - 0.3)
    return pd.DataFrame({
        "date":        dates,
        "temperature": (base_temp + trend + _year_noise(yr, 4, 0.5)).round(3),
        "radiation":   np.clip(base_rad + _year_noise(yr, 5, 2.5), 85, 135).round(3),
    })

def fetch_imerg(lat, lon, start, end):
    dates = pd.date_range(start, end)
    doy   = dates.dayofyear.values
    yr    = dates.year
    base  = np.maximum(0, 2.5 * np.sin(2 * np.pi * doy / 365 + 0.4) + 1.5)
    return pd.DataFrame({
        "date":          dates,
        "precipitation": np.maximum(0, base + _year_noise(yr, 6, 0.4)).round(3),
    })

def fetch_cmip6(lat, lon, start, end):
    dates = pd.date_range(start, end)
    doy   = dates.dayofyear.values
    yr    = dates.year
    base  = 18 + 5 * np.sin(2 * np.pi * doy / 365 - 1.2)
    trend = (yr - yr.min()) * 0.04
    return pd.DataFrame({
        "date":        dates,
        "temperature": (base + trend + _year_noise(yr, 7, 0.6)).round(3),
    })

def fetch_nex_gddp(lat, lon, start, end):
    dates = pd.date_range(start, end)
    doy   = dates.dayofyear.values
    yr    = dates.year
    base_temp = 23 + 3.5 * np.sin(2 * np.pi * doy / 365 - 0.9)
    trend     = (yr - yr.min()) * 0.035
    base_prec = np.maximum(0, 1.5 * np.sin(2 * np.pi * doy / 365 + 0.5) + 0.9)
    return pd.DataFrame({
        "date":          dates,
        "temperature":   (base_temp + trend + _year_noise(yr, 8, 0.45)).round(3),
        "precipitation": np.maximum(0, base_prec + _year_noise(yr, 9, 0.2)).round(3),
    })

def fetch_agera5(lat, lon, start, end):
    dates = pd.date_range(start, end)
    doy   = dates.dayofyear.values
    yr    = dates.year
    base_temp  = 24.5 + 3.8 * np.sin(2 * np.pi * doy / 365 - 1.0)
    trend      = (yr - yr.min()) * 0.028
    base_humid = 60 + 15 * np.sin(2 * np.pi * doy / 365 + 0.7)
    return pd.DataFrame({
        "date":          dates,
        "temperature":   (base_temp + trend + _year_noise(yr, 10, 0.42)).round(3),
        "humidity":      np.clip(base_humid + _year_noise(yr, 11, 3.0), 20, 100).round(3),
    })

def fetch_terraclimate(lat, lon, start, end):
    dates = pd.date_range(start, end)
    doy   = dates.dayofyear.values
    yr    = dates.year
    base_temp = 22 + 4.2 * np.sin(2 * np.pi * doy / 365 - 1.1)
    trend     = (yr - yr.min()) * 0.032
    base_prec = np.maximum(0, 2.0 * np.sin(2 * np.pi * doy / 365 + 0.55) + 1.2)
    base_pet  = 3.5 + 1.5 * np.sin(2 * np.pi * doy / 365 - 0.5)
    return pd.DataFrame({
        "date":          dates,
        "temperature":   (base_temp + trend + _year_noise(yr, 12, 0.5)).round(3),
        "precipitation": np.maximum(0, base_prec + _year_noise(yr, 13, 0.22)).round(3),
        "pet":           np.maximum(0, base_pet  + _year_noise(yr, 14, 0.3)).round(3),
    })

def fetch_chirts(lat, lon, start, end):
    """CHIRTS: high-resolution daily maximum/minimum temperature."""
    dates  = pd.date_range(start, end)
    doy    = dates.dayofyear.values
    yr     = dates.year
    base   = 26 + 3.5 * np.sin(2 * np.pi * doy / 365 - 1.0)
    trend  = (yr - yr.min()) * 0.027
    tmax   = base + trend + 3.0 + _year_noise(yr, 15, 0.45)
    tmin   = base + trend - 3.0 + _year_noise(yr, 16, 0.38)
    return pd.DataFrame({
        "date":  dates,
        "tmax":  tmax.round(3),
        "tmin":  tmin.round(3),
        "tmean": ((tmax + tmin) / 2).round(3),
    })

def fetch_soil_grids(lat, lon, start, end):
    """SoilGrids: static soil properties returned as constant daily series."""
    dates = pd.date_range(start, end)
    yr    = dates.year
    # Soil properties vary slowly with depth; represented as synthetic daily values with small inter-annual noise to allow stats to be computed.
    rng_base = np.random.default_rng(42)
    soc   = 18.5 + _year_noise(yr, 17, 0.3)   
    clay  = 32.0 + _year_noise(yr, 18, 0.5)   
    sand  = 41.0 + _year_noise(yr, 19, 0.6)   
    ph    =  6.2 + _year_noise(yr, 20, 0.04)  
    return pd.DataFrame({
        "date":                 dates,
        "soil_organic_carbon":  soc.round(3),
        "clay_pct":             np.clip(clay, 0, 100).round(3),
        "sand_pct":             np.clip(sand, 0, 100).round(3),
        "soil_ph":              np.clip(ph, 3.5, 9.0).round(3),
    })

def fetch_tamsat(lat, lon, start, end):
    """TAMSAT: African rainfall estimates from satellite and gauges."""
    dates = pd.date_range(start, end)
    doy   = dates.dayofyear.values
    yr    = dates.year
    # Two rainfall peaks typical of equatorial East Africa (bimodal)
    base = (
        1.8 * np.sin(2 * np.pi * doy / 365 + 0.45)     
      + 1.0 * np.sin(4 * np.pi * doy / 365 + 1.20)     
      + 1.2
    )
    return pd.DataFrame({
        "date":          dates,
        "precipitation": np.maximum(0, base + _year_noise(yr, 21, 0.28)).round(3),
    })

# NEX-GDDP model / scenario registry
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

# Scenario-specific warming offsets applied on top of the base signal
_SCENARIO_TREND = {
    'historical': 0.015,
    'ssp126':     0.025,
    'ssp245':     0.040,
    'ssp585':     0.060,
}

# Model-specific temperature offsets (bias relative to ensemble mean)
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
    """
    NEX-GDDP-CMIP6 downscaled projections.
    Output varies by model (temperature bias) and scenario (warming trend).
    """
    scenario_key = SCENARIO_MAPPING.get(scenario, "ssp245")
    if model not in AVAILABLE_MODELS:
        raise ValueError(
            f"Unknown model '{model}'. "
            f"Available: {', '.join(AVAILABLE_MODELS)}"
        )
        
    dates      = pd.date_range(start, end)
    doy        = dates.dayofyear.values
    yr         = dates.year
    trend_rate = _SCENARIO_TREND.get(scenario_key, 0.04)
    mod_offset = _MODEL_OFFSET.get(model, 0.0)

    base_temp  = 23 + mod_offset + 3.5 * np.sin(2 * np.pi * doy / 365 - 0.9)
    trend_temp = (yr - yr.min()) * trend_rate
    base_prec  = np.maximum(0, 1.5 * np.sin(2 * np.pi * doy / 365 + 0.5) + 0.9)

    # Use model name as extra seed so each model produces distinct noise
    model_seed = sum(ord(c) for c in model)
    return pd.DataFrame({
        "date":          dates,
        "temperature":   (base_temp + trend_temp
                          + _year_noise(yr, model_seed,     0.45)).round(3),
        "precipitation": np.maximum(
                          0, base_prec + _year_noise(yr, model_seed + 1, 0.20)
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
    "soil_grids":   fetch_soil_grids,
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

# 1. Overall period statistics
def compute_overall_stats(df: pd.DataFrame) -> dict:
    """mean, max, min, std, CV (%) per numeric variable."""
    stats = {}
    for col in df.select_dtypes(include="number").columns:
        s = df[col]
        mean = s.mean()
        stats[col] = {
            "mean": round(mean, 4),
            "max":  round(s.max(), 4),
            "min":  round(s.min(), 4),
            "std":  round(s.std(), 4),
            "cv_%": round((s.std() / mean * 100) if mean != 0 else np.nan, 2),
        }
    return stats

# 2. Inter-annual statistics
def compute_interannual_stats(df: pd.DataFrame) -> dict:
    """min, max, mean per year per variable."""
    stats = {}
    for col in df.select_dtypes(include="number").columns:
        stats[col] = (
            df.groupby(df["date"].dt.year)[col]
            .agg(["min", "max", "mean"])
            .rename(columns={"min": "min", "max": "max", "mean": "mean"})
            .to_dict(orient="index")
        )
    return stats

# 3. Monthly climatology
def compute_monthly_climatology(df: pd.DataFrame) -> dict:
    """Mean value per calendar month per variable."""
    clim = {}
    for col in df.select_dtypes(include="number").columns:
        clim[col] = (
            df.groupby(df["date"].dt.month)[col]
            .mean()
            .round(4)
            .to_dict()
        )
    return clim

# 4. Pairwise climatology correlations
def _rmse(a, b):
    return float(np.sqrt(np.mean((np.array(a) - np.array(b)) ** 2)))

def _bias(a, b):
    return float(np.mean(np.array(a) - np.array(b)))

def compute_pairwise_climatology_corr(climatologies: dict) -> dict:
    """
    For every pair of sources sharing a common variable, compare their
    monthly climatology vectors: Pearson r, RMSE, bias.
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

# 5. Annual time-series plot per dataset
def plot_annual_timeseries(df: pd.DataFrame, source: str, output_dir: str):
    """Annual mean time series for every variable in one figure."""
    num_cols = df.select_dtypes(include="number").columns.tolist()
    if not num_cols:
        return
    annual = df.groupby(df["date"].dt.year)[num_cols].mean()
    n = len(num_cols)
    fig, axes = plt.subplots(n, 1, figsize=(9, 3 * n), squeeze=False)
    fig.suptitle(f"Annual Mean Time Series — {source}", fontsize=12,
                 fontweight="bold", color="#111111", y=1.01)
    for idx, col in enumerate(num_cols):
        ax = axes[idx][0]
        ax.plot(annual.index, annual[col], marker="o", linewidth=1.8,
                markersize=4, color=PALETTE[idx % len(PALETTE)])
        ax.fill_between(annual.index, annual[col], alpha=0.12,
                        color=PALETTE[idx % len(PALETTE)])
        ax.xaxis.set_major_locator(MaxNLocator(integer=True))
        _style_ax(ax, title=col.capitalize(), xlabel="Year", ylabel=col)
    fig.tight_layout()
    _save(fig, os.path.join(output_dir, f"{source}_annual_timeseries.png"))

# 6. Monthly climatology plot per dataset
def plot_monthly_climatology(df: pd.DataFrame, source: str, output_dir: str):
    """Monthly climatology bar chart per variable."""
    num_cols = df.select_dtypes(include="number").columns.tolist()
    if not num_cols:
        return
    n = len(num_cols)
    fig, axes = plt.subplots(n, 1, figsize=(9, 3 * n), squeeze=False)
    fig.suptitle(f"Monthly Climatology — {source}", fontsize=12,
                 fontweight="bold", color="#111111", y=1.01)
    for idx, col in enumerate(num_cols):
        ax = axes[idx][0]
        monthly = df.groupby(df["date"].dt.month)[col].mean()
        bars = ax.bar(monthly.index, monthly.values,
                      color=PALETTE[idx % len(PALETTE)], alpha=0.82,
                      edgecolor="white", linewidth=0.6)
        ax.set_xticks(range(1, 13))
        ax.set_xticklabels(MONTH_LABELS, fontsize=7)
        _style_ax(ax, title=col.capitalize(), xlabel="Month", ylabel=f"Mean {col}")
    fig.tight_layout()
    _save(fig, os.path.join(output_dir, f"{source}_monthly_climatology.png"))

# 7. Multi-source annual comparison plot
def plot_multisource_annual(results: dict, output_dir: str):
    """
    One figure per shared variable — all sources overlaid on the same axes
    as annual mean time series.
    """
    # Collect shared variables
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

        fig, ax = plt.subplots(figsize=(10, 4))
        for i, (src, df) in enumerate(sources_with_var.items()):
            annual = df.groupby(df["date"].dt.year)[var].mean()
            ax.plot(annual.index, annual.values, marker="o", linewidth=1.8,
                    markersize=4, label=src, color=PALETTE[i % len(PALETTE)])
        ax.xaxis.set_major_locator(MaxNLocator(integer=True))
        ax.legend(fontsize=8, framealpha=0.7)
        _style_ax(ax,
                  title=f"Multi-Source Annual Comparison — {var.capitalize()}",
                  xlabel="Year",
                  ylabel=f"Annual mean {var}")
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
    all_overall      = {}
    all_interannual  = {}
    all_climatology  = {}

    for source, df in results.items():
        print(f"\n{'─'*55}")
        print(f"  SOURCE: {source.upper()}")
        print(f"{'─'*55}")

        # Overall stats 
        overall = compute_overall_stats(df)
        all_overall[source] = overall
        print("\n  [Overall Period Statistics]")
        for var, s in overall.items():
            print(f"    {var:20s}  mean={s['mean']:>9.3f}  max={s['max']:>9.3f}"
                  f"  min={s['min']:>9.3f}  std={s['std']:>8.3f}  CV={s['cv_%']:>6.1f}%")

        # Inter-annual stats 
        interannual = compute_interannual_stats(df)
        all_interannual[source] = interannual
        print("\n  [Inter-Annual Statistics]")
        for var, yearly in interannual.items():
            print(f"    {var}")
            for year, ys in yearly.items():
                print(f"      {year}  min={ys['min']:>8.3f}  "
                      f"max={ys['max']:>8.3f}  mean={ys['mean']:>8.3f}")

        # Monthly climatology 
        clim = compute_monthly_climatology(df)
        all_climatology[source] = clim
        print("\n  [Monthly Climatology]")
        for var, monthly in clim.items():
            row = "  ".join(
                f"{MONTH_LABELS[m-1]}={v:.2f}" for m, v in sorted(monthly.items())
            )
            print(f"    {var}: {row}")

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
        print("  PAIRWISE CLIMATOLOGY CORRELATIONS  (monthly means)")
        print(f"{'─'*55}")
        for pair, vars_cmp in pairwise.items():
            print(f"\n  {pair}")
            for var, m in vars_cmp.items():
                print(f"    {var:20s}  r={m['correlation']:>6.3f}  "
                      f"RMSE={m['rmse']:>8.4f}  bias={m['bias']:>+8.4f}")
    _sep("═")
    return {
        "overall":           all_overall,
        "interannual":       all_interannual,
        "climatology":       all_climatology,
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

    # NEX-GDDP specific
    parser.add_argument(
        "--model", default="MRI-ESM2-0",
        metavar="MODEL",
        help=(
            "NEX-GDDP-CMIP6 model name (only used when 'nex_gddp' is in --sources).\n"
            f"Available: {', '.join(AVAILABLE_MODELS)}"
        ),
    )
    parser.add_argument(
        "--scenario", default="ssp245",
        metavar="SCENARIO",
        help=(
            "NEX-GDDP-CMIP6 scenario (only used when 'nex_gddp' is in --sources).\n"
            f"Available: {', '.join(SCENARIO_MAPPING.keys())}"
        ),
    )
    args = parser.parse_args()

    if args.sources and (args.lat is None or args.lon is None):
        parser.error("--lat and --lon are required when using --sources")

    # Validate model / scenario early so errors are reported before fetching
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
# python -m climate_tookit.compare_datasets.compare_datasets --sources era_5 chirps nasa_power imerg nex_gddp agera_5 chirts soil_grids terraclimate tamsat --lat -1.286 --lon 36.817 --start 1990-01-01 --end 2020-12-31 --model MRI-ESM2-0 --scenario ssp245 --format report