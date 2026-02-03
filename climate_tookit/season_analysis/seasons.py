"""
Season Analysis Module

Detects agricultural growing seasons based on daily precipitation and temperature data.
Uses ET0 calculations and precipitation thresholds to identify planting season onset
and cessation dates.

Sources supported:
- Historical: ERA5, AgERA5, CHIRPS + ERA5 (temperature fallback)
- Future: NEX-GDDP (CMIP6 climate projections)

Features:
- Source auto-selection or forced selection
- Baseline climatology downloads
- Future projections (single model or ensemble)
- CSV download for current, baseline, and future data
"""

import pandas as pd
import math
import argparse
import sys
import os
from datetime import datetime, date
from pathlib import Path
from typing import List

# DO NOT CHANGE PATH LOGIC
current_dir = Path(__file__).parent
toolkit_root = current_dir.parent
sys.path.insert(0, str(toolkit_root))

from fetch_data.preprocess_data.preprocess_data import preprocess_data
from sources.utils.models import ClimateVariable

SEASON_VARIABLES = [
    ClimateVariable.precipitation,
    ClimateVariable.max_temperature,
    ClimateVariable.min_temperature,
]

HISTORICAL_SOURCES = ['era_5', 'agera_5']
FUTURE_SOURCE = 'nex_gddp'

NEX_GDDP_MODELS = [
    'ACCESS-CM2', 'BCC-CSM2-MR', 'CESM2-WACCM', 'CMCC-CM2-SR5',
    'CNRM-CM6-1', 'CNRM-ESM2-1', 'CanESM5', 'EC-Earth3-Veg-LR',
    'GFDL-ESM4', 'GISS-E2-1-G', 'HadGEM3-GC31-LL', 'IITM-ESM',
    'INM-CM4-8', 'INM-CM5-0', 'IPSL-CM6A-LR', 'MIROC6'
]


def get_climate_data(
    lat: float,
    lon: float,
    start_date: str,
    end_date: str,
    use_projections: bool = False,
    model: str = None,
    scenario: str = 'ssp245',
    force_source: str = None
) -> pd.DataFrame:

    date_from = date.fromisoformat(start_date)
    date_to = date.fromisoformat(end_date)

    if use_projections:
        df = preprocess_data(
            source='nex_gddp',
            location_coord=(lat, lon),
            variables=SEASON_VARIABLES,
            date_from=date_from,
            date_to=date_to,
            model=model,
            scenario=scenario
        )

    elif force_source == 'chirps+chirts':
        df_p = preprocess_data(
            source='chirps',
            location_coord=(lat, lon),
            variables=[ClimateVariable.precipitation],
            date_from=date_from,
            date_to=date_to
        )

        df_t = preprocess_data(
            source='era_5',
            location_coord=(lat, lon),
            variables=[
                ClimateVariable.max_temperature,
                ClimateVariable.min_temperature
            ],
            date_from=date_from,
            date_to=date_to
        )

        if df_p.empty or df_t.empty:
            raise RuntimeError("CHIRPS or ERA5 fallback returned no data")

        df = pd.merge(df_p, df_t, on='date', how='inner')

    elif force_source:
        df = preprocess_data(
            source=force_source,
            location_coord=(lat, lon),
            variables=SEASON_VARIABLES,
            date_from=date_from,
            date_to=date_to
        )

    else:
        df = None
        for src in HISTORICAL_SOURCES:
            df = preprocess_data(
                source=src,
                location_coord=(lat, lon),
                variables=SEASON_VARIABLES,
                date_from=date_from,
                date_to=date_to
            )
            if not df.empty:
                break

        if df is None or df.empty:
            df_p = preprocess_data(
                source='chirps',
                location_coord=(lat, lon),
                variables=[ClimateVariable.precipitation],
                date_from=date_from,
                date_to=date_to
            )
            df_t = preprocess_data(
                source='era_5',
                location_coord=(lat, lon),
                variables=[
                    ClimateVariable.max_temperature,
                    ClimateVariable.min_temperature
                ],
                date_from=date_from,
                date_to=date_to
            )
            df = pd.merge(df_p, df_t, on='date', how='inner')

    if df is None or df.empty:
        raise RuntimeError("No climate data retrieved")

    out = pd.DataFrame()
    out['date'] = pd.to_datetime(df['date'])
    out['tmax'] = df['max_temperature']
    out['tmin'] = df['min_temperature']
    out['precip'] = df['precipitation']
    out['lat'] = lat

    return out


def calculate_et0(tmin: float, tmax: float, lat: float, d: datetime) -> float:
    if pd.isna(tmin) or pd.isna(tmax) or tmax < tmin:
        return 0.0
    J = d.timetuple().tm_yday
    lat_rad = math.radians(lat)
    sol_decl = 0.409 * math.sin((2 * math.pi / 365) * J - 1.39)
    sha = math.acos(max(min(-math.tan(lat_rad) * math.tan(sol_decl), 1), -1))
    ra = (24 * 60 / math.pi) * 0.082 * (
        sha * math.sin(lat_rad) * math.sin(sol_decl) +
        math.cos(lat_rad) * math.cos(sol_decl) * math.sin(sha)
    )
    return 0.0023 * math.sqrt(tmax - tmin) * ((tmax + tmin) / 2 + 17.8) * ra


def detect_seasons(df: pd.DataFrame, gap_days=30, min_days=30) -> List[dict]:
    seasons = []
    df = df.copy()
    df['et0'] = [
        calculate_et0(r.tmin, r.tmax, r.lat, r.date)
        for r in df.itertuples()
    ]
    df['wet'] = df['precip'] >= 0.5 * df['et0']

    i = 0
    while i < len(df):
        if df.iloc[i].wet:
            start = df.iloc[i].date
            dry = 0
            j = i + 1
            while j < len(df):
                if not df.iloc[j].wet:
                    dry += 1
                    if dry >= gap_days:
                        break
                else:
                    dry = 0
                j += 1
            end = df.iloc[j - gap_days].date if dry >= gap_days else df.iloc[-1].date
            if (end - start).days + 1 >= min_days:
                seasons.append({
                    'onset': start.strftime('%Y-%m-%d'),
                    'cessation': end.strftime('%Y-%m-%d'),
                    'length_days': (end - start).days + 1
                })
            i = j
        else:
            i += 1
    return seasons


def main():
    parser = argparse.ArgumentParser("Season analysis")

    parser.add_argument('--lat', type=float, required=True)
    parser.add_argument('--lon', type=float, required=True)
    parser.add_argument('--date-from')
    parser.add_argument('--date-to')
    parser.add_argument('--source', choices=['era_5','agera_5','chirps+chirts','auto'], default='auto')

    parser.add_argument('--baseline-start', type=int)
    parser.add_argument('--baseline-end', type=int)
    parser.add_argument('--baseline-source', choices=['era_5','agera_5','chirps+chirts','auto'], default='auto')

    parser.add_argument('--future-start', type=int)
    parser.add_argument('--future-end', type=int)
    parser.add_argument('--climate-model', default='GFDL-ESM4')
    parser.add_argument('--scenario', default='ssp245')
    parser.add_argument('--use-ensemble', action='store_true')

    parser.add_argument('--download-data')
    parser.add_argument('--download-baseline')
    parser.add_argument('--download-future')

    args = parser.parse_args()

    lat, lon = args.lat, args.lon

    if args.date_from and args.date_to:
        df = get_climate_data(
            lat, lon, args.date_from, args.date_to,
            force_source=None if args.source == 'auto' else args.source
        )
        if args.download_data:
            df.to_csv(args.download_data, index=False)

    if args.baseline_start and args.baseline_end and args.download_baseline:
        frames = []
        for y in range(args.baseline_start, args.baseline_end + 1):
            frames.append(
                get_climate_data(
                    lat, lon, f"{y}-01-01", f"{y}-12-31",
                    force_source=None if args.baseline_source == 'auto' else args.baseline_source
                )
            )
        pd.concat(frames).to_csv(args.download_baseline, index=False)

    if args.future_start and args.future_end and args.download_future:
        models = NEX_GDDP_MODELS if args.use_ensemble else [args.climate_model]
        frames = []
        for m in models:
            for y in range(args.future_start, args.future_end + 1):
                df = get_climate_data(
                    lat, lon, f"{y}-01-01", f"{y}-12-31",
                    use_projections=True, model=m, scenario=args.scenario
                )
                df['model'] = m
                frames.append(df)
        pd.concat(frames).to_csv(args.download_future, index=False)


if __name__ == "__main__":
    main()


# Auto source (ERA5 → AgERA5 → CHIRPS+ERA5 fallback)
# python climate_tookit/season_analysis/seasons.py --lat -1.286 --lon 36.817 --date-from 2020-01-01 --date-to 2020-12-31

# Force CHIRPS precipitation with ERA5 temperature fallback
# python climate_tookit/season_analysis/seasons.py --lat -1.286 --lon 36.817 --date-from 2020-01-01 --date-to 2020-12-31 --source chirps+chirts

# Download baseline data
# python climate_tookit/season_analysis/seasons.py --lat -1.286 --lon 36.817 --baseline-start 1991 --baseline-end 2020 --baseline-source chirps+chirts --download-baseline baseline.csv

# Download future ensemble (NEX-GDDP only)
# python climate_tookit/season_analysis/seasons.py --lat -1.286 --lon 36.817 --future-start 2040 --future-end 2050 --use-ensemble --scenario ssp245 --download-future future_ensemble.csv