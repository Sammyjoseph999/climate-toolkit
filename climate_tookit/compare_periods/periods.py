"""
Compare Periods

Runs statistics.py for a baseline period and a focal year, then diffs the four
sections statistics.py produces:
    raw_climate_summary:    per-variable mean/min/max/std
    overall_statistics :    period totals (baseline annualised before diffing)
    season_statistics  :    per-season metrics
                             - lumped 'typical season' when --fixed-season is omitted
                             - one comparison per window when --fixed-season is given,
                               so a two-season spec doesn't blend MAM with OND
    annual_summary     :   humid test (annual rainfall, humid_year ratio)

--fixed-season is passed straight through to statistics.py, so its three flavors work without periods.py knowing about them:
    Single        : '03-01:05-31'
    Two seasons   : '03-01:05-31,10-01:12-15'
    Year-crossing : '11-01:02-28'
Plain `chirps` source defaults tmax=25/tmin=15 in statistics.py, so temperature is excluded from every diff section when chirps is the source.
"""

import sys
import os
import json
import argparse
from typing import Dict, Any, Tuple, Optional, List

import pandas as pd

current_dir  = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.insert(0, project_root)

from climate_tookit.climate_statistics.statistics import analyze_climate_statistics

CATEGORIES   = ["precipitation", "temperature", "et0", "water_balance"]
ANNUALIZABLE = {
    "precipitation": ["total_mm", "rainy_days", "dry_days"],
    "et0":           ["total_mm"],
    "water_balance": ["total_balance", "deficit_days", "surplus_days"],
}
PRECIP_ONLY  = {"chirps"}
SUPPORTED    = {"era_5", "agera_5", "chirps+chirts", "nasa_power",
                "chirps", "chirts", "terraclimate", "auto"}

# helpers
def _is_num(x: Any) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool)

def _round(d: Any, n: int = 2) -> Any:
    if isinstance(d, dict):  return {k: _round(v, n) for k, v in d.items()}
    if isinstance(d, list):  return [_round(v, n) for v in d]
    return round(d, n) if _is_num(d) else d

def _annualize(stats: Dict[str, Any], n_years: int) -> Dict[str, Any]:
    """Period totals -> per-year averages. Means/maxes/mins untouched."""
    if n_years <= 0:
        return stats
    out: Dict[str, Any] = {}
    for cat, metrics in stats.items():
        if not isinstance(metrics, dict):
            out[cat] = metrics
            continue
        annz = ANNUALIZABLE.get(cat, [])
        out[cat] = {m: round(v / n_years, 2) if (m in annz and _is_num(v)) else v
                    for m, v in metrics.items()}
    return out

def _agg_seasons(seasons: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Average season metrics into a single 'typical season' block."""
    if not seasons:
        return {"_n": 0}
    sums: Dict[str, List[float]] = {}
    for s in seasons:
        for cat in ("precipitation", "temperature", "water_balance"):
            for m, v in (s.get(cat) or {}).items():
                if _is_num(v):
                    sums.setdefault(f"{cat}.{m}", []).append(float(v))
    out: Dict[str, Any] = {"_n": len(seasons)}
    for k, vs in sums.items():
        cat, m = k.split(".", 1)
        out.setdefault(cat, {})[m] = round(sum(vs) / len(vs), 2)
    return out

def _diff_block(a: Dict, b: Dict, a_lbl: str, b_lbl: str,
                drop_temp: bool = False) -> Dict[str, Any]:
    """Diff two category-keyed blocks: {category: {metric: {a_lbl, b_lbl, diff, pct}}}"""
    out: Dict[str, Any] = {}
    for cat in CATEGORIES:
        if drop_temp and cat == "temperature":
            continue
        ab, bb = a.get(cat), b.get(cat)
        if not (isinstance(ab, dict) and isinstance(bb, dict)):
            continue
        cat_out = {}
        for m, av in ab.items():
            bv = bb.get(m)
            if not (_is_num(av) and _is_num(bv)):
                continue
            d = av - bv
            p = (d / bv * 100.0) if bv != 0 else 0.0
            cat_out[m] = {a_lbl:    round(av, 2), b_lbl:    round(bv, 2),
                          "diff":   round(d,  2), "pct":    round(p,  2)}
        if cat_out:
            out[cat] = cat_out
    return out

def _diff_raw(focal_raw: List[Dict], baseline_raw: List[Dict],
              drop_temp: bool = False) -> Dict[str, Any]:
    """Diff raw_climate_summary lists: {variable: {stat: {focal, baseline, diff, pct}}}"""
    fd = {r.get("Variable"): r for r in focal_raw    if r.get("Variable")}
    bd = {r.get("Variable"): r for r in baseline_raw if r.get("Variable")}
    out: Dict[str, Any] = {}
    for var, fr in fd.items():
        if drop_temp and "Temperature" in var:
            continue
        br = bd.get(var)
        if not br:
            continue
        per_stat = {}
        for s in ("Mean", "Min", "Max", "Std"):
            fv, bv = fr.get(s), br.get(s)
            if not (_is_num(fv) and _is_num(bv)):
                continue
            d = fv - bv
            p = (d / bv * 100.0) if bv != 0 else 0.0
            per_stat[s] = {"focal":    round(fv, 3), "baseline": round(bv, 3),
                           "diff":     round(d,  3), "pct":      round(p,  2)}
        if per_stat:
            out[var] = per_stat
    return out

def _diff_annual(focal_ann: Dict[str, Dict], baseline_ann: Dict[str, Dict],
                 focal_year: int) -> Dict[str, Any]:
    """Diff annual_summary: focal year value vs baseline aggregate."""
    fi = focal_ann.get(str(focal_year)) or {}
    rains  = [v["annual_rain_mm"] for v in baseline_ann.values()
              if v and _is_num(v.get("annual_rain_mm"))]
    humid  = sum(1 for v in baseline_ann.values() if v and v.get("is_humid"))
    total  = sum(1 for v in baseline_ann.values() if v)

    out: Dict[str, Any] = {}
    fr = fi.get("annual_rain_mm")
    if _is_num(fr) and rains:
        b_avg = sum(rains) / len(rains)
        d, p = fr - b_avg, ((fr - b_avg) / b_avg * 100.0) if b_avg else 0.0
        out["annual_rain_mm"] = {"focal":    round(float(fr), 1),
                                  "baseline_avg": round(b_avg, 1),
                                  "diff":     round(d, 1),
                                  "pct":      round(p, 2)}
    out["humid_status"] = {
        "focal_is_humid":    fi.get("is_humid"),
        "focal_humid_test":  fi.get("humid_test"),
        "baseline_humid":    f"{humid}/{total}" + (
            f" ({humid/total*100:.1f}%)" if total else ""),
    }
    return out

# main API 
def compare(
    location:       Tuple[float, float],
    baseline_start: int,
    baseline_end:   int,
    focal_year:     int,
    source:         str,
    fixed_season:   Optional[str] = None,
) -> Dict[str, Any]:
    """Run statistics.py for baseline + focal, diff the four sections."""
    if source.lower() not in SUPPORTED:
        return {"error": f"Source '{source}' not supported. "
                         f"Use one of: {', '.join(sorted(SUPPORTED))}"}
    if baseline_end < baseline_start:
        return {"error": "baseline_end must be >= baseline_start"}

    n_years   = baseline_end - baseline_start + 1
    drop_temp = source.lower() in PRECIP_ONLY
    fs_kw     = {"fixed_season": fixed_season} if fixed_season else {}

    print(f"\nFetching baseline {baseline_start}-{baseline_end} | source={source}")
    base = analyze_climate_statistics(
        location_coord=location,
        start_year=baseline_start, end_year=baseline_end,
        source=source, **fs_kw)

    print(f"\nFetching focal year {focal_year} | source={source}")
    focal = analyze_climate_statistics(
        location_coord=location,
        start_year=focal_year, end_year=focal_year,
        source=source, **fs_kw)

    # 1. raw_climate_summary
    raw_diff = _diff_raw(focal.get("raw_climate_summary", []),
                         base.get("raw_climate_summary",  []),
                         drop_temp)

    # 2. overall_statistics (annualise baseline)
    base_overall  = _annualize(_round(base.get("overall_statistics", {}), 2), n_years)
    focal_overall = _round(focal.get("overall_statistics", {}), 2)
    overall_diff  = _diff_block(focal_overall, base_overall,
                                "focal_year", "baseline_avg", drop_temp)

    # 3. season_statistics
    base_seasons  = _round(base.get("season_statistics",  []), 2)
    focal_seasons = _round(focal.get("season_statistics", []), 2)
    season_diff: Optional[Dict[str, Any]] = None
    if base_seasons or focal_seasons:
        if fixed_season:
            # Per-window: group by season_number (statistics.py assigns 1,2,...
            # in the order of windows in --fixed-season, year-crossing included).
            labels = [w.strip() for w in fixed_season.split(",")]
            base_grp:  Dict[int, List[Dict]] = {}
            focal_grp: Dict[int, List[Dict]] = {}
            for s in base_seasons:
                base_grp.setdefault(s.get("season_number", 1), []).append(s)
            for s in focal_seasons:
                focal_grp.setdefault(s.get("season_number", 1), []).append(s)

            windows = []
            for sn in sorted(set(base_grp) | set(focal_grp)):
                label = labels[sn - 1] if 0 < sn <= len(labels) else f"window_{sn}"
                fb    = _agg_seasons(focal_grp.get(sn, []))
                bb    = _agg_seasons(base_grp.get(sn, []))
                windows.append({
                    "window":         label,
                    "season_number":  sn,
                    "n_baseline":     bb["_n"],
                    "n_focal":        fb["_n"],
                    "diff":           _diff_block(fb, bb, "focal", "baseline_avg",
                                                  drop_temp),
                })
            season_diff = {"windows": windows}
        else:
            fb = _agg_seasons(focal_seasons)
            bb = _agg_seasons(base_seasons)
            season_diff = {
                "n_baseline": bb["_n"],
                "n_focal":    fb["_n"],
                "diff":       _diff_block(fb, bb, "focal_avg", "baseline_avg",
                                          drop_temp),
            }
    # 4. annual_summary
    annual_diff = _diff_annual(focal.get("annual_summary", {}),
                               base.get("annual_summary",  {}),
                               focal_year)
    return {
        "focal_year":           focal_year,
        "baseline_period":      f"{baseline_start}-{baseline_end}",
        "baseline_years":       n_years,
        "source":               source,
        "fixed_season":         fixed_season,
        "temperature_excluded": drop_temp,
        "raw_climate_summary":  raw_diff,
        "overall_statistics":   overall_diff,
        "season_statistics":    season_diff,
        "annual_summary":       annual_diff,
    }

#  printing 
def _print_block(diff: Dict[str, Any]) -> None:
    if not diff:
        print("  (no comparable metrics)")
        return
    rows = []
    for cat, metrics in diff.items():
        for metric, vals in metrics.items():
            row = {"Category": cat, "Metric": metric}
            for k, v in vals.items():
                if k == "diff":  row["Δ"]  = f"{v:+.2f}"
                elif k == "pct": row["Δ%"] = f"{v:+.2f}%"
                else:            row[k]    = f"{v:.2f}"
            rows.append(row)
    print(pd.DataFrame(rows).to_string(index=False))

def print_report(r: Dict[str, Any]) -> None:
    if "error" in r:
        print(f"\nError: {r['error']}")
        return

    print(f"\n{'=' * 60}")
    print(f"COMPARISON: focal {r['focal_year']} vs baseline {r['baseline_period']}")
    print(f"{'=' * 60}")
    print(f"  Source        : {r['source']}")
    if r.get("fixed_season"):
        print(f"  Fixed seasons : {r['fixed_season']}")
    if r.get("temperature_excluded"):
        print("  [!] precipitation-only source -- temperature excluded.")

    print(f"\n--- 1. RAW CLIMATE SUMMARY ---")
    raw = r.get("raw_climate_summary", {})
    if raw:
        rows = [{"Variable": var, "Stat": stat,
                 "focal":    f"{v['focal']:.3f}",
                 "baseline": f"{v['baseline']:.3f}",
                 "Δ":        f"{v['diff']:+.3f}",
                 "Δ%":       f"{v['pct']:+.2f}%"}
                for var, stats in raw.items() for stat, v in stats.items()]
        print(pd.DataFrame(rows).to_string(index=False))
    else:
        print("  (no data)")

    print(f"\n--- 2. OVERALL STATISTICS  (baseline annualised) ---")
    _print_block(r.get("overall_statistics", {}))

    season = r.get("season_statistics")
    if season:
        print(f"\n--- 3. SEASON STATISTICS ---")
        if "windows" in season:
            for w in season["windows"]:
                print(f"\n  Window {w['window']} (season #{w['season_number']}, "
                      f"n_baseline={w['n_baseline']}, n_focal={w['n_focal']})")
                _print_block(w["diff"])
        else:
            print(f"  (n_baseline={season['n_baseline']}, n_focal={season['n_focal']})")
            _print_block(season["diff"])

    print(f"\n--- 4. ANNUAL SUMMARY ---")
    ann = r.get("annual_summary", {})
    arm = ann.get("annual_rain_mm")
    if arm:
        print(f"  Annual rainfall : focal={arm['focal']} mm | "
              f"baseline_avg={arm['baseline_avg']} mm | "
              f"Δ={arm['diff']:+.1f} ({arm['pct']:+.2f}%)")
    hs = ann.get("humid_status") or {}
    if hs:
        focal_state = ("humid" if hs.get("focal_is_humid") else
                       "not humid" if hs.get("focal_is_humid") is False else "n/a")
        print(f"  Humid status    : focal={focal_state} | "
              f"baseline={hs.get('baseline_humid', 'n/a')}")
        if hs.get("focal_humid_test"):
            print(f"                    test: {hs['focal_humid_test']}")
    print()

# CLI 
def main() -> None:
    p = argparse.ArgumentParser(
        description="Compare a focal year against a baseline period using statistics.py.",
        formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--location", required=True, help="lat,lon (e.g. -1.286,36.817)")
    p.add_argument("--baseline-start", type=int, required=True)
    p.add_argument("--baseline-end",   type=int, required=True)
    p.add_argument("--focal-year",     type=int, required=True)
    p.add_argument("--source", required=True,
                   help=f"One of: {', '.join(sorted(SUPPORTED))}")
    p.add_argument("--fixed-season", default=None,
                   metavar="MM-DD:MM-DD[,MM-DD:MM-DD]",
                   help=("Optional. Passed through to statistics.py.\n"
                         "  Single        : '03-01:05-31'\n"
                         "  Two seasons   : '03-01:05-31,10-01:12-15'\n"
                         "  Year-crossing : '11-01:02-28'"))
    p.add_argument("--output", default=None, help="Write JSON results to this path")
    args = p.parse_args()

    try:
        lat, lon = (float(x) for x in args.location.replace(" ", ",").split(","))
    except ValueError:
        print("Error: --location must be 'lat,lon'"); sys.exit(1)

    result = compare(
        location=(lat, lon),
        baseline_start=args.baseline_start,
        baseline_end=args.baseline_end,
        focal_year=args.focal_year,
        source=args.source,
        fixed_season=args.fixed_season,
    )
    print_report(result)
    if "error" in result:
        sys.exit(1)

    if args.output:
        with open(args.output, "w") as f:
            json.dump(result, f, indent=2, default=str)
        print(f"✓ Saved: {args.output}")

if __name__ == "__main__":
    main()

# Auto-detected seasons (no --fixed-season):
# python -m climate_tookit.compare_periods.periods --location=-1.286,36.817 --baseline-start=1991 --baseline-end=2016 --focal-year=2015 --source=chirps+chirts --output=results/nairobi_2015_vs_1991-2016_auto.json

# Single fixed season:
# python -m climate_tookit.compare_periods.periods --location=-1.286,36.817 --baseline-start=1991 --baseline-end=2020 --focal-year=2019 --source=terraclimate --fixed-season=03-01:05-31 --output=results/nairobi_2019_MAM.json

# Two fixed seasons:
# python -m climate_tookit.compare_periods.periods --location=-1.286,36.817 --baseline-start=1991 --baseline-end=2020 --focal-year=2019 --source=era_5 --fixed-season='03-01:05-31,10-01:12-15' --output=results/nairobi_2019_MAM_OND.json

# Year-crossing single window:
# python -m climate_tookit.compare_periods.periods --location=-1.286,36.817 --baseline-start=1991 --baseline-end=2016 --focal-year=2016 --source=chirps --fixed-season=11-01:02-28 --output=results/nairobi_2016_NDJF.json