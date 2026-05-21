"""
compare_datasets.py
Extended dataset comparison module. All data is sourced from the toolkit's preprocessed-data pipeline (climate_tookit.fetch_data.preprocess_data.preprocess_data),
so this module returns cleaned, QC'd, analysis-ready observations / projections — not synthetic mock data.

Outputs:
- consolidated annual time-series tables (years × sources) per variable
- consolidated monthly climatology tables (months × sources) per variable
- annual statistics tables (sources × mean / min / max / std / CV) per variable
- annual time-series plots per dataset (PNG)
- monthly climatology plots per dataset (PNG)
- multi-source annual time-series comparison plots (PNG)
- multi-source monthly climatology comparison plots (PNG)
- pairwise climatology correlations (monthly means: correlation, RMSE, bias)

State variables   (temperature, humidity, radiation, soil props, tmax/tmin, pet):
    annual mean per year -> across years reported as mean / min / max / std / CV
Accumulation vars (precipitation):
    annual total per year -> across years reported as mean / min / max / std / CV
    monthly climatology  -> mean monthly total per calendar month
"""
import argparse
import os
import sys
import json
from datetime import date
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator

_HERE   = os.path.dirname(os.path.abspath(__file__))
_PARENT = os.path.dirname(_HERE)
for _p in (os.path.join(_PARENT, "fetch_data", "preprocess_data"),
           os.path.join(_PARENT, "fetch_data", "source_data", "sources")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from preprocess_data import preprocess_data            
from utils.models    import ClimateVariable, SoilVariable  

# Variables treated as accumulations (totals, not means / min / max)
ACCUMULATION_VARS = {"precipitation", "precip", "rain", "rainfall"}

def _is_accum(col: str) -> bool:
    """Return True if *col* is an accumulation (flux) variable."""
    return col.lower() in ACCUMULATION_VARS

DEFAULT_CLIMATE_VARIABLES = [
    ClimateVariable.precipitation,
    ClimateVariable.max_temperature,
    ClimateVariable.min_temperature,
]

DEFAULT_SOIL_VARIABLES = [
    SoilVariable.ph,
    SoilVariable.clay_content,
    SoilVariable.sand_content,
    SoilVariable.silt_content,
    SoilVariable.bulk_density,
    SoilVariable.organic_carbon,
    SoilVariable.cation_exchange_capacity,
]

SOURCE_VARIABLES = {
    "soil_grid": DEFAULT_SOIL_VARIABLES,
}

VALID_SOURCES = {
    "era_5", "agera_5", "chirps", "chirts", "cmip_6", "imerg",
    "nasa_power", "nex_gddp", "soil_grid", "tamsat", "terraclimate",
}

# NEX-GDDP registry (used for CLI validation and dispatch)
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

def _fetch_source(source: str, lat: float, lon: float,
                  start, end,
                  model: str | None = None,
                  scenario: str | None = None) -> pd.DataFrame:
    """
    Dispatch a single source to the preprocessed-data pipeline.
    Returns the analysis-ready DataFrame (with a `date` column) that `preprocess_data` produces. Raises if the source key is unknown.

    Notes
    -----
    - Variable list is per-source (climate sources get DEFAULT_CLIMATE_VARIABLES; soil_grid gets DEFAULT_SOIL_VARIABLES).
    - Static sources (e.g. soil_grid) return a single-row DataFrame with no `date` column. We broadcast that row across the requested date
      range so the report (which aggregates by year and month) treats soil properties as a daily-constant series.
    """
    if source not in VALID_SOURCES:
        raise ValueError(
            f"Unknown source '{source}'. Valid keys: {', '.join(sorted(VALID_SOURCES))}"
        )
    date_from = date.fromisoformat(str(start)) if start else None
    date_to   = date.fromisoformat(str(end))   if end   else None
    variables = SOURCE_VARIABLES.get(source, DEFAULT_CLIMATE_VARIABLES)
    kwargs = dict(
        source=source,
        location_coord=(lat, lon),
        variables=variables,
        date_from=date_from,
        date_to=date_to,
    )
    if source == "nex_gddp":
        kwargs["model"]    = model
        kwargs["scenario"] = scenario
    df = preprocess_data(**kwargs)

    if (df is not None
            and not df.empty
            and "date" not in df.columns
            and date_from is not None
            and date_to is not None):
        dates = pd.date_range(start=date_from, end=date_to, freq="D")
        broadcast = pd.concat([df] * len(dates), ignore_index=True)
        broadcast.insert(0, "date", dates)
        df = broadcast
    return df

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

# 1. Per-variable annual series + annual-scale statistics
def compute_annual_series(df: pd.DataFrame, col: str) -> pd.Series:
    """Annual aggregate: total for accumulation vars, mean for state vars."""
    grp = df.groupby(df["date"].dt.year)[col]
    return grp.sum() if _is_accum(col) else grp.mean()

def compute_annual_stats(series: pd.Series) -> dict:
    """Annual-scale metrics across years: mean, min, max, std, CV (%)."""
    mean_val = float(series.mean())
    std_val  = float(series.std())
    cv       = float(std_val / mean_val * 100) if mean_val else float("nan")
    return {
        "mean": round(mean_val, 4),
        "min":  round(float(series.min()), 4),
        "max":  round(float(series.max()), 4),
        "std":  round(std_val, 4),
        "cv":   round(cv, 4),
    }

# 2. Monthly climatology (per variable, single source)
def compute_monthly_climatology_series(df: pd.DataFrame, col: str) -> pd.Series:
    """
    State variables : mean value per calendar month (mean of daily values).
    Accumulation vars: mean monthly total per calendar month
                       (sum within each month-year, then average across years).
    """
    if _is_accum(col):
        return (
            df.assign(_yr=df["date"].dt.year, _mo=df["date"].dt.month)
            .groupby(["_yr", "_mo"])[col].sum()
            .groupby(level="_mo").mean()
        )
    return df.groupby(df["date"].dt.month)[col].mean()

def compute_monthly_climatology(df: pd.DataFrame) -> dict:
    """Per-variable monthly climatology dict for one dataset (used by pairwise)."""
    return {
        col: compute_monthly_climatology_series(df, col).round(4).to_dict()
        for col in df.select_dtypes(include="number").columns
    }

# Consolidated cross-source tables (one DataFrame per variable)
def _sources_with_var(results: dict, var: str) -> dict:
    return {
        src: df for src, df in results.items()
        if var in df.select_dtypes(include="number").columns
    }

def build_annual_timeseries_table(results: dict, var: str) -> pd.DataFrame:
    """Years (rows) × sources (columns) table of annual aggregates for *var*."""
    cols = {src: compute_annual_series(df, var)
            for src, df in _sources_with_var(results, var).items()}
    if not cols:
        return pd.DataFrame()
    table = pd.DataFrame(cols).round(4)
    table.index.name = "year"
    return table

def build_climatology_table(results: dict, var: str) -> pd.DataFrame:
    """Months (rows) × sources (columns) table of monthly climatology for *var*."""
    cols = {src: compute_monthly_climatology_series(df, var)
            for src, df in _sources_with_var(results, var).items()}
    if not cols:
        return pd.DataFrame()
    table = pd.DataFrame(cols).round(4)
    table.index.name = "month"
    return table

def build_annual_stats_table(results: dict, var: str) -> pd.DataFrame:
    """Sources (rows) × {mean, min, max, std, cv} table for annual aggregates of *var*."""
    rows = {src: compute_annual_stats(compute_annual_series(df, var))
            for src, df in _sources_with_var(results, var).items()}
    if not rows:
        return pd.DataFrame()
    table = pd.DataFrame(rows).T[["mean", "min", "max", "std", "cv"]].round(4)
    table.index.name = "source"
    return table

# 3. Pairwise climatology correlations
def _rmse(a, b):
    return float(np.sqrt(np.mean((np.array(a) - np.array(b)) ** 2)))

def _bias(a, b):
    return float(np.mean(np.array(a) - np.array(b)))

def compute_pairwise_climatology_corr(climatologies: dict) -> dict:
    """
    For every pair of sources sharing a common variable, compare their monthly climatology vectors: Pearson r, RMSE, bias.
    For accumulation variables the vectors are mean monthly totals; for state variables they are mean monthly values.
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
    One figure per shared variable — all sources overlaid on the same axes as annual time series (total for accumulation vars, mean for state vars).
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

# 7. Multi-source monthly climatology comparison plot
def plot_multisource_monthly_climatology(results: dict, output_dir: str):
    """
    One figure per shared variable — all sources overlaid on the same axes as monthly climatology (mean monthly total for accumulation vars,
    mean monthly value for state vars).
    """
    all_vars: set = set()
    for df in results.values():
        all_vars |= set(df.select_dtypes(include="number").columns)

    for var in sorted(all_vars):
        sources_with_var = _sources_with_var(results, var)
        if len(sources_with_var) < 2:
            continue
        use_total = _is_accum(var)
        fig, ax = plt.subplots(figsize=(10, 4))
        for i, (src, df) in enumerate(sources_with_var.items()):
            monthly = compute_monthly_climatology_series(df, var)
            ax.plot(monthly.index, monthly.values, marker="o", linewidth=1.8,
                    markersize=4, label=src, color=PALETTE[i % len(PALETTE)])
        ax.set_xticks(range(1, 13))
        ax.set_xticklabels(MONTH_LABELS, fontsize=7)
        ax.legend(fontsize=8, framealpha=0.7)
        ylabel_text = (f"Mean monthly total {var}" if use_total
                       else f"Mean {var}")
        _style_ax(ax,
                  title=f"Multi-Source Monthly Climatology — {var.capitalize()}",
                  xlabel="Month",
                  ylabel=ylabel_text)
        fig.tight_layout()
        _save(fig, os.path.join(output_dir,
                                f"multisource_monthly_climatology_{var}.png"))

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
        if source not in VALID_SOURCES:
            print(f"  ⚠️   Unknown source '{source}' — skipping. "
                  f"Valid keys: {', '.join(sorted(VALID_SOURCES))}")
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
                df = _fetch_source(source, lat, lon, start, end,
                                   model=nex_model, scenario=scenario_key)
                result_key = f"nex_gddp_{nex_model}_{scenario_key}"
            else:
                df = _fetch_source(source, lat, lon, start, end)
                result_key = source

            if df is None or df.empty or len(df.columns) <= 1:
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

    # All numeric variables observed across any source
    all_vars: set = set()
    for df in results.values():
        all_vars |= set(df.select_dtypes(include="number").columns)

    # Per-source plots
    print(f"\n{'─'*60}")
    print("  PER-SOURCE PLOTS")
    print(f"{'─'*60}")
    for source, df in results.items():
        print(f"\n  [{source}]")
        plot_annual_timeseries(df, source, output_dir)
        plot_monthly_climatology(df, source, output_dir)

    # Consolidated tables per variable
    annual_tables: dict = {}
    stats_tables:  dict = {}
    clim_tables:   dict = {}

    for var in sorted(all_vars):
        annual = build_annual_timeseries_table(results, var)
        stats  = build_annual_stats_table(results, var)
        clim   = build_climatology_table(results, var)
        if annual.empty:
            continue
        annual_tables[var] = annual
        stats_tables[var]  = stats
        clim_tables[var]   = clim

        is_acc = _is_accum(var)
        agg_label  = "annual total" if is_acc else "annual mean"
        clim_label = "mean monthly total" if is_acc else "mean daily value"

        print(f"\n{'─'*60}")
        print(f"  VARIABLE: {var.upper()}")
        print(f"{'─'*60}")

        print(f"\n  [Annual Time Series]  ({agg_label} per year × source)")
        print(annual.to_string(float_format=lambda v: f"{v:10.3f}"))

        print(f"\n  [Annual Statistics]  ({agg_label} across years: mean / min / max / std / CV%)")
        print(stats.to_string(float_format=lambda v: f"{v:10.3f}"))

        print(f"\n  [Monthly Climatology]  ({clim_label} × source)")
        clim_display = clim.copy()
        clim_display.index = [MONTH_LABELS[m - 1] for m in clim.index]
        clim_display.index.name = "month"
        print(clim_display.to_string(float_format=lambda v: f"{v:8.3f}"))

    # Multi-source comparison plots
    print(f"\n{'─'*60}")
    print("  MULTI-SOURCE COMPARISON PLOTS")
    print(f"{'─'*60}")
    plot_multisource_annual(results, output_dir)
    plot_multisource_monthly_climatology(results, output_dir)

    # Pairwise climatology correlations
    all_climatology = {src: compute_monthly_climatology(df)
                       for src, df in results.items()}
    pairwise = compute_pairwise_climatology_corr(all_climatology)
    if pairwise:
        print(f"\n{'─'*60}")
        print("  PAIRWISE CLIMATOLOGY CORRELATIONS  (monthly climatology vectors)")
        print(f"{'─'*60}")
        for pair, vars_cmp in pairwise.items():
            print(f"\n  {pair}")
            for var, m in vars_cmp.items():
                label = "(monthly totals)" if _is_accum(var) else "(monthly means)"
                print(f"    {var:22s} {label}  r={m['correlation']:>6.3f}  "
                      f"RMSE={m['rmse']:>8.4f}  bias={m['bias']:>+8.4f}")
    _sep("═")
    return {
        "annual_timeseries":  {v: t.to_dict() for v, t in annual_tables.items()},
        "annual_statistics":  {v: t.to_dict(orient="index") for v, t in stats_tables.items()},
        "climatology":        {v: t.to_dict() for v, t in clim_tables.items()},
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
# python -m climate_tookit.compare_datasets.compare_datasets --sources era_5 chirps nasa_power imerg nex_gddp agera_5 chirts soil_grid terraclimate tamsat --lat -1.286 --lon 36.817 --start 1990-01-01 --end 2016-12-31 --model MRI-ESM2-0 --scenario ssp245 --format report