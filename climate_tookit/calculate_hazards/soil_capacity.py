"""
Per-location soil water-holding capacity for the NDWS / NDWL0 water balance.

This reproduces the Adaptation Atlas approach (CIAT ERA_dev water_balance.R):
the Hodnett & Tomasella (2002) van Genuchten pedotransfer function (PTF), known in GSIF as ``AWCPTF``, converts basic soil properties into volumetric
water contents, which are then integrated over the crop root zone to give:
    soilcp  -- plant-available water held at field capacity (mm)
    soilsat -- extra water held between field capacity and saturation (mm)
    
These two numbers are exactly what ``hazards.calc_water_balance`` needs:
``soilcp`` controls ERATIO (-> NDWS) and ``soilsat`` caps LOGGING (-> NDWL0).

Two entry points
----------------
* ``compute_soil_capacity(layers, root_depth_cm)`` -- pure math, no I/O.
  Unit-testable offline; accepts one or many soil layers.
* ``fetch_soil_capacity(lat, lon)`` -- pulls genuine ISRIC SoilGrids 250m v2.0 properties (the same product the Adaptation Atlas uses) for every root-zone
  depth horizon **through the toolkit's ``soil_grid`` source**
  (``SourceData(source=ClimateDataset.soil_grid)`` -> ``sources/soil_grid.py``), then integrates them into ``(soilcp, soilsat)``. Falls back to documented
  defaults on any failure (missing GEE credentials, no soil coverage, etc.) so callers never crash just because soil data is unavailable.

IMPORTANT -- unit assumptions
-----------------------------
``compute_soil_capacity`` expects properties in standard SoilGrids units:
    sand, silt, clay : percent (0-100)
    organic_carbon   : g/kg
    bulk_density     : kg/m3
    cec              : cmol(+)/kg
    ph_x10           : pH(H2O) x 10   (e.g. 65 == pH 6.5)
The ``soil_grid`` source returns properties already in these units; the raw SoilGrids integer->unit conversions live there (``_SOIL_SPEC``). If a live run
produces implausible capacities, that conversion table is the first thing to check.
"""

from __future__ import annotations

import math
from typing import Dict, List, Optional, Sequence, Tuple

# Defaults used when SoilGrids cannot be reached. Mirror hazards.DEFAULT_SOIL*.
FALLBACK_SOILCP  = 100.0
FALLBACK_SOILSAT = 100.0

# Default crop root zone (cm). Atlas soilcap_calc clamps to [min, max].
DEFAULT_ROOT_DEPTH_CM = 60.0
MIN_ROOT_DEPTH_CM     = 45.0
MAX_ROOT_DEPTH_CM     = 100.0

# Hodnett & Tomasella (2002) PTF coefficients (GSIF::AWCPTF defaults).
_PTF = {
    "lnAlfa": [-2.294,  0.0,   -3.526,  0.0,    2.440,   0.0,  -0.076, -11.331, 0.019, 0.0,    0.0,    0.0],
    "lnN":    [62.986,  0.0,    0.0,   -0.833, -0.529,   0.0,   0.0,     0.593,  0.0,   0.007, -0.014,  0.0],
    "tetaS":  [81.799,  0.0,    0.0,    0.099,  0.0,   -31.42,  0.018,   0.451,  0.0,   0.0,    0.0,   -5e-04],
    "tetaR":  [22.733, -0.164,  0.0,    0.0,    0.0,     0.0,   0.235,  -0.831,  0.0,   0.0018, 0.0,    0.0026],
}

# Suctions (kPa). Field capacity at -33 kPa, permanent wilting point at -1585 kPa (the GSIF/Adaptation Atlas AWCPTF default).
H_FIELD_CAPACITY = -33.0
H_WILTING_POINT  = -1585.0

def _predictors(sand: float, silt: float, clay: float, oc: float,
                bld: float, cec: float, ph_x10: float) -> List[float]:
    """
    Build the 11-element predictor vector AWCPTF dots with coef[1:].
    The Hodnett & Tomasella coefficients are calibrated on properties in "natural" units, so SoilGrids-native units are converted here:
        oc  g/kg  -> %      (/10)
        bld kg/m3 -> g/cm3  (*0.001)
        ph  pHx10 -> pH     (/10)   <- e.g. 62 -> 6.2; without this, teta_s
                                       comes out near-saturation (unphysical)
                                       and FC==saturation.
    """
    return [
        sand, silt, clay,
        oc / 10.0,
        bld * 0.001,
        cec,
        ph_x10 / 10.0,
        silt ** 2,
        clay ** 2,
        sand * silt,
        sand * clay,
    ]

def _vg_theta(alfa: float, n: float, teta_r: float, teta_s: float, h: float) -> float:
    """van Genuchten volumetric water content at suction h (kPa)."""
    m = 1.0 - 1.0 / n
    return teta_r + (teta_s - teta_r) / ((1.0 + (alfa * abs(h)) ** n) ** m)

def layer_water_contents(sand: float, silt: float, clay: float, oc: float,
                         bld: float, cec: float, ph_x10: float) -> Dict[str, float]:
    """
    Run the Hodnett-Tomasella PTF for a single soil layer.
    Returns volumetric fractions:
        awc_fc : available water at field capacity (theta_fc - wilting point)
        sat_extra : extra water from field capacity to saturation (theta_s - theta_fc)
        teta_s, wwp, teta_fc : intermediate contents (fractions)
    """
    # Renormalise texture to 100 % (AWCPTF fix.values behaviour).
    tex = sand + silt + clay
    if tex > 0:
        sand, silt, clay = (sand / tex * 100.0, silt / tex * 100.0, clay / tex * 100.0)
    bld = min(max(bld, 100.0), 2650.0)

    x = _predictors(sand, silt, clay, oc, bld, cec, ph_x10)

    def _dot(coef: Sequence[float]) -> float:
        return coef[0] + sum(c * xi for c, xi in zip(coef[1:], x))

    alfa   = math.exp(_dot(_PTF["lnAlfa"]) / 100.0)
    n      = math.exp(_dot(_PTF["lnN"]) / 100.0)
    teta_s = _dot(_PTF["tetaS"]) / 100.0
    teta_r = _dot(_PTF["tetaR"]) / 100.0

    teta_r = max(teta_r, 0.0)
    teta_s = min(teta_s, 1.0)
    # van Genuchten n must exceed 1 for a valid m = 1 - 1/n.
    n = max(n, 1.0001)

    teta_fc = _vg_theta(alfa, n, teta_r, teta_s, H_FIELD_CAPACITY)
    wwp     = _vg_theta(alfa, n, teta_r, teta_s, H_WILTING_POINT)
    wwp     = min(wwp, teta_fc)  # wilting point can't exceed field capacity

    awc_fc    = max(teta_fc - wwp, 0.0)
    sat_extra = max(teta_s - teta_fc, 0.0)
    return {
        "awc_fc":    awc_fc,
        "sat_extra": sat_extra,
        "teta_s":    teta_s,
        "teta_fc":   teta_fc,
        "wwp":       wwp,
    }

def compute_soil_capacity(
    layers: Sequence[Dict[str, float]],
    root_depth_cm: float = DEFAULT_ROOT_DEPTH_CM,
) -> Tuple[float, float]:
    """
    Integrate per-layer water contents over the root zone.
    Each ``layers`` item is a dict of soil properties (standard units, see module docstring) plus optional ``top_cm`` / ``bottom_cm`` horizon bounds.
    If horizon bounds are omitted (single representative profile), the layer's fractions are applied uniformly across the whole root zone.
    Returns ``(soilcp_mm, soilsat_mm)``. Conversion: cm * fraction * 10 = mm.
    """
    if not layers:
        return FALLBACK_SOILCP, FALLBACK_SOILSAT

    rdepth = min(max(root_depth_cm, MIN_ROOT_DEPTH_CM), MAX_ROOT_DEPTH_CM)

    soilcp_mm = 0.0
    soilsat_mm = 0.0

    # Single representative layer -> apply its fractions over the whole zone.
    if len(layers) == 1 and "top_cm" not in layers[0]:
        wc = layer_water_contents(**_only_props(layers[0]))
        soilcp_mm  = wc["awc_fc"]    * rdepth * 10.0
        soilsat_mm = wc["sat_extra"] * rdepth * 10.0
        return round(soilcp_mm, 2), round(soilsat_mm, 2)

    # Multi-horizon: sum thickness-weighted contents, capped at root depth.
    for layer in sorted(layers, key=lambda d: d.get("top_cm", 0.0)):
        top = float(layer.get("top_cm", 0.0))
        bot = float(layer.get("bottom_cm", rdepth))
        if top >= rdepth:
            break
        bot = min(bot, rdepth)
        thickness = max(bot - top, 0.0)
        if thickness <= 0:
            continue
        wc = layer_water_contents(**_only_props(layer))
        soilcp_mm  += thickness * wc["awc_fc"]    * 10.0
        soilsat_mm += thickness * wc["sat_extra"] * 10.0

    return round(soilcp_mm, 2), round(soilsat_mm, 2)

_PROP_KEYS = ("sand", "silt", "clay", "oc", "bld", "cec", "ph_x10")

def _only_props(layer: Dict[str, float]) -> Dict[str, float]:
    """Keep just the PTF property keys (drop horizon bounds etc.)."""
    return {k: float(layer[k]) for k in _PROP_KEYS}

# Soil property fetch + unit normalisation
_SOILVAR_TO_PTF = {
    "sand_content":             "sand",
    "silt_content":             "silt",
    "clay_content":             "clay",
    "organic_carbon":           "oc",
    "bulk_density":             "bld",
    "cation_exchange_capacity": "cec",
    "ph":                       "ph_x10",
}

# Reasonable mid-range fallbacks for any single property the fetch can't get, so one missing band doesn't sink the whole estimate (a loam-ish profile).
_PROP_DEFAULTS = {
    "sand": 40.0, "silt": 40.0, "clay": 20.0,
    "oc": 10.0, "bld": 1300.0, "cec": 15.0, "ph_x10": 65.0,
}

def fetch_soil_capacity(
    lat: float,
    lon: float,
    root_depth_cm: float = DEFAULT_ROOT_DEPTH_CM,
    verbose: bool = True,
) -> Tuple[float, float]:
    """
    Derive (soilcp, soilsat) in mm for a point from ISRIC SoilGrids 250m.
    Fetches every root-zone depth horizon directly from Earth Engine. On ANY error (no GEE auth, no soil coverage, import failure) returns the documented
    fallback capacities instead of raising, so hazard runs stay robust.
    """
    try:
        layers = _download_soil_properties(lat, lon)
    except Exception as exc:  # noqa: BLE001 - fail soft by design
        if verbose:
            print(f"  [soil] SoilGrids fetch failed ({exc}); "
                  f"using defaults soilcp={FALLBACK_SOILCP}, soilsat={FALLBACK_SOILSAT}")
        return FALLBACK_SOILCP, FALLBACK_SOILSAT

    if not layers:
        if verbose:
            print("  [soil] no soil properties returned; using defaults.")
        return FALLBACK_SOILCP, FALLBACK_SOILSAT

    if verbose:
        top = layers[0]
        shown = ", ".join(f"{k}={top[k]:.1f}" for k in _PROP_KEYS)
        print(f"  [soil] ISRIC SoilGrids topsoil ({int(top['top_cm'])}-"
              f"{int(top['bottom_cm'])}cm): {shown}")

    soilcp, soilsat = compute_soil_capacity(layers, root_depth_cm=root_depth_cm)

    # Guard against pathological PTF output (e.g. bad units) -> fall back.
    if not (0 < soilcp < 1000) or not (0 <= soilsat < 1000):
        if verbose:
            print(f"  [soil] implausible result (soilcp={soilcp}, soilsat={soilsat}); "
                  f"using defaults.")
        return FALLBACK_SOILCP, FALLBACK_SOILSAT

    if verbose:
        print(f"  [soil] derived soilcp={soilcp} mm, soilsat={soilsat} mm "
              f"(root depth {root_depth_cm:g} cm)")
    return soilcp, soilsat

def _fill_texture_remainder(std: Dict[str, float]) -> None:
    """If exactly one texture fraction is missing, set it to the 0-100 remainder
    of the other two (in place) rather than dropping to a flat default."""
    if "silt" not in std and "sand" in std and "clay" in std:
        std["silt"] = max(0.0, 100.0 - std["sand"] - std["clay"])
    if "sand" not in std and "silt" in std and "clay" in std:
        std["sand"] = max(0.0, 100.0 - std["silt"] - std["clay"])
    if "clay" not in std and "sand" in std and "silt" in std:
        std["clay"] = max(0.0, 100.0 - std["sand"] - std["silt"])

def _download_soil_properties(lat: float, lon: float) -> List[Dict[str, float]]:
    """
    Fetch soil properties for the point via the toolkit's ``soil_grid`` source and return one layer dict per root-zone horizon, in the standard units that
    ``compute_soil_capacity`` expects. Returns ``[]`` if the source yields no data (caller then falls back to defaults).
    The ISRIC SoilGrids download, unit conversion, and depth handling all live in ``sources/soil_grid.py``; here we only translate the returned soil-variable
    columns into PTF property keys and fill any missing texture fraction.
    """
    import os
    import sys

    here   = os.path.dirname(os.path.abspath(__file__))
    root   = os.path.dirname(os.path.dirname(here))
    srcdir = os.path.join(root, "climate_tookit", "fetch_data", "source_data")
    for p in (srcdir, os.path.join(srcdir, "sources")):
        if p not in sys.path:
            sys.path.insert(0, p)

    from sources.utils.models import ClimateDataset, SoilVariable
    from sources.utils.settings import Settings
    from source_data import SourceData

    wanted = [
        SoilVariable.sand_content,
        SoilVariable.silt_content,
        SoilVariable.clay_content,
        SoilVariable.organic_carbon,
        SoilVariable.bulk_density,
        SoilVariable.cation_exchange_capacity,
        SoilVariable.ph,
    ]
    sd = SourceData(
        location_coord=(lat, lon),
        variables=wanted,
        source=ClimateDataset.soil_grid,
        date_from_utc=None,
        date_to_utc=None,
        settings=Settings.load(),
    )
    df = sd.download()
    if df is None or df.empty:
        return []

    layers: List[Dict[str, float]] = []
    for _, row in df.iterrows():
        props: Dict[str, float] = {}
        for col, ptf_key in _SOILVAR_TO_PTF.items():
            if col in df.columns:
                val = row[col]
                # skip None / NaN (NaN != NaN)
                if val is not None and val == val:
                    props[ptf_key] = float(val)
        _fill_texture_remainder(props)
        if not props:
            continue
        layer = dict(_PROP_DEFAULTS)
        layer.update(props)
        if "top_cm" in df.columns and row["top_cm"] == row["top_cm"]:
            layer["top_cm"] = float(row["top_cm"])
        if "bottom_cm" in df.columns and row["bottom_cm"] == row["bottom_cm"]:
            layer["bottom_cm"] = float(row["bottom_cm"])
        layers.append(layer)
    return layers

if __name__ == "__main__":
    # Quick offline sanity check of the pedotransfer math (no GEE needed):
    # a clayey profile should hold more plant-available water than a sandy one.
    sandy = {"sand": 85, "silt": 10, "clay": 5,  "oc": 5,  "bld": 1500, "cec": 5,  "ph_x10": 65}
    clayey = {"sand": 15, "silt": 25, "clay": 60, "oc": 20, "bld": 1200, "cec": 35, "ph_x10": 65}
    for name, prof in (("sandy", sandy), ("clayey", clayey)):
        cp, sat = compute_soil_capacity([prof])
        wc = layer_water_contents(**prof)
        print(f"{name:7s} soilcp={cp:7.2f} mm  soilsat={sat:7.2f} mm  "
              f"(awc_fc={wc['awc_fc']:.3f}, sat_extra={wc['sat_extra']:.3f})")