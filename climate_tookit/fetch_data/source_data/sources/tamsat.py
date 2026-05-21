"""
TAMSAT Data Downloader
Downloads daily rainfall (RFE) and soil moisture (SMCL) from TAMSAT JASMIN servers. Each monthly NetCDF file contains a per-day `time` dimension; this
module selects the gridcell nearest the requested (lat, lon) and returns one value per day, preserving daily resolution.
Coordinate convention: `location_coord` is `(lat, lon)`, matching the rest of the toolkit.
"""

import logging
import os
import tempfile
import numpy as np
import pandas as pd
import xarray as xr
import requests
from datetime import date, timedelta
from typing import Optional
from collections import defaultdict
from sources.utils.models import DataDownloadBase, ClimateVariable
from sources.utils.settings import Settings

logger = logging.getLogger(__name__)

class DownloadTAMSAT(DataDownloadBase):
    def __init__(
        self,
        location_coord: tuple[float, float],
        date_from_utc: date,
        date_to_utc: date,
        variables: list[ClimateVariable] = None,
        settings: Optional[Settings] = None,
        source=None,
        aggregation: str = "daily"
    ):
        super().__init__(variables=variables or [], location_coord=location_coord,
                         date_from_utc=date_from_utc, date_to_utc=date_to_utc)
        self.location_coord = location_coord
        self.date_from_utc = date_from_utc
        self.date_to_utc = date_to_utc
        self.variables = variables or []
        self.settings = settings or Settings.load()
        self.source = source
        self.aggregation = aggregation
        self.dates = [
            date_from_utc + timedelta(days=i)
            for i in range((date_to_utc - date_from_utc).days + 1)
        ]

    _NETCDF_ENGINES = ("h5netcdf", "netcdf4", "scipy")

    def _open_netcdf_bytes(self, raw: bytes):
        """
        Open NetCDF content from bytes by writing to a temp file and trying explicit engines in order. The temp file is kept alive for the
        lifetime of the returned dataset (caller is responsible for closing the dataset; the temp file is cleaned up by close-handler in
        `_read_nc_variable`). Returns (Dataset, tmp_path) or (None, None).
        We write to disk rather than passing BytesIO because some xarray backends leave lazy references that break when the BytesIO closes.
        """
        fd, tmp_path = tempfile.mkstemp(suffix=".nc", prefix="tamsat_")
        try:
            os.close(fd)
            with open(tmp_path, "wb") as f:
                f.write(raw)
        except Exception:
            try: os.unlink(tmp_path)
            except OSError: pass
            raise

        last_err = None
        for engine in self._NETCDF_ENGINES:
            try:
                ds = xr.open_dataset(tmp_path, engine=engine).load()
                return ds, tmp_path
            except Exception as e:
                last_err = e
                continue
        logger.debug("All NetCDF engines failed; last error: %s", last_err)
        try: os.unlink(tmp_path)
        except OSError: pass
        return None, None

    def _read_nc_variable(self, prefix: str) -> list[float]:
        """
        Read a TAMSAT variable as a daily series at (lat, lon).
        Strategy: group the requested dates by (year, month) so we download each monthly NetCDF file only once. For each file, select the gridcell
        nearest the requested point, then look up each day's value by time index. Days that can't be matched (download error, missing time slice)
        return NaN — never silently replaced with 0.
        """
        cfg = self.settings.tamsat
        if prefix == "rfe":
            base_url     = cfg.rainfall_url
            expected_var = cfg.variable.get_band("precipitation")  
            version      = "v3.1"
            file_template = f"rfe{{year}}{{month:02d}}_{version}.nc"
        elif prefix == "smcl":
            base_url     = cfg.soil_moisture_url
            expected_var = cfg.variable.get_band("soil_moisture")  
            version      = "v2.3.1"
            file_template = f"sm{{year}}{{month:02d}}_{version}.nc"
        else:
            raise ValueError(f"Unknown prefix {prefix}")
        if not expected_var:
            raise ValueError(
                f"TAMSAT settings missing band name for prefix {prefix!r}"
            )

        lat0, lon0 = self.location_coord                   

        grouped = defaultdict(list)
        for dt in self.dates:
            grouped[(dt.year, dt.month)].append(dt)

        values_by_date: dict = {}
        headers = {"User-Agent": "Mozilla/5.0"}

        for (year, month), day_list in grouped.items():
            file_name = file_template.format(year=year, month=month)
            url       = f"{base_url.rstrip('/')}/{year}/{file_name}"
            try:
                resp = requests.get(url, headers=headers, timeout=30)
                resp.raise_for_status()
                ds, tmp_path = self._open_netcdf_bytes(resp.content)
                if ds is None:
                    raise RuntimeError(
                        "no working xarray engine could read the TAMSAT file"
                    )
                try:
                    da = ds[expected_var]

                    # Spatial selection: nearest gridcell to (lat0, lon0).
                    sel_kwargs = {}
                    if "lat" in da.dims:
                        sel_kwargs["lat"] = lat0
                    if "lon" in da.dims:
                        sel_kwargs["lon"] = lon0
                    if sel_kwargs:
                        da = da.sel(method="nearest", **sel_kwargs)

                    # Temporal selection: build a date -> value map.
                    if "time" in da.dims:
                        times = pd.to_datetime(da["time"].values)
                        arr   = np.asarray(da.values, dtype=float).ravel()
                        time_to_val = {
                            ts.date(): float(v) for ts, v in zip(times, arr)
                        }
                        for dt in day_list:
                            values_by_date[dt] = time_to_val.get(dt, np.nan)
                    else:
                        # No time dim — single value applies to the month.
                        scalar = float(np.asarray(da.values).reshape(-1)[0])
                        for dt in day_list:
                            values_by_date[dt] = scalar
                finally:
                    ds.close()
                    try:
                        if tmp_path:
                            os.unlink(tmp_path)
                    except OSError:
                        pass
            except Exception as e:
                logger.warning(
                    f"TAMSAT fetch failed for {year}-{month:02d} ({url}): {e}"
                )
                for dt in day_list:
                    values_by_date[dt] = np.nan

        return [values_by_date.get(dt, np.nan) for dt in self.dates]

    def download_precipitation(self):
        return self._read_nc_variable("rfe")

    def download_rainfall(self):
        return self.download_precipitation()

    def download_soil_moisture(self):
        return self._read_nc_variable("smcl")

    def download_temperature(self):
        raise NotImplementedError("TAMSAT does not provide temperature data")

    def download_windspeed(self):
        raise NotImplementedError("TAMSAT does not provide wind speed data")

    def download_solar_radiation(self):
        raise NotImplementedError("TAMSAT does not provide solar radiation data")

    def download_humidity(self):
        raise NotImplementedError("TAMSAT does not provide humidity data")

    def download_variables(self) -> pd.DataFrame:
        """
        Returns a DataFrame with a `date` column plus one column per requested variable that TAMSAT actually provides. Unsupported variables (e.g.
        temperature, wind, radiation, humidity) are skipped with a warning — we do NOT fabricate zero-filled columns for variables this dataset
        doesn't produce.

        Comparison is by `.name` (string), not by enum identity, because depending on how callers set up `sys.path` (e.g. compare_datasets adds
        both `source_data/` and `source_data/sources/`), `ClimateVariable` can be loaded under two different module paths and yield two distinct
        enum classes whose members do not compare equal.
        """
        data_dict = {}
        for variable in self.variables:
            name = getattr(variable, "name", str(variable))
            if name in ("rainfall", "precipitation"):
                data_dict["precipitation"] = self.download_precipitation()
            elif name == "soil_moisture":
                data_dict["soil_moisture"] = self.download_soil_moisture()
            else:
                logger.warning(
                    "TAMSAT does not provide '%s'; skipping (no column emitted).",
                    name,
                )
        return pd.DataFrame({"date": self.dates, **data_dict})