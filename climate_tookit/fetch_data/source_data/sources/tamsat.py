"""
TAMSAT Data Downloader
Downloads daily rainfall (RFE) and soil moisture (SMCL) from the TAMSAT JASMINpublic tree (`gws-access.jasmin.ac.uk/public/tamsat`). TAMSAT publishes one
NetCDF file per day at this endpoint; this module fetches one file perrequested day, selects the gridcell nearest the requested (lat, lon), and
returns the single-time-step value. Missing/failed days return NaN — never 0.

Note: an earlier revision read from the `/monthly/` tree, but those filescontain a single per-month total rather than per-day samples, which produced
all-NaN (or, before that, all-0) daily output. We now read the `/daily/` tree.

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
        Strategy: TAMSAT's public JASMIN tree publishes one NetCDF file per day
        at `…/{year}/{month:02d}/{prefix}{year}_{month:02d}_{day:02d}.{version}.nc`,
        each containing a single time-step on a (lat, lon) grid. We download
        one file per requested day, select the gridcell nearest the requested
        point, and read the single value. Days that can't be fetched or read
        return NaN — never silently replaced with 0. A single `requests.Session`
        is reused across days for HTTP keep-alive.
        """
        cfg = self.settings.tamsat
        if prefix == "rfe":
            base_url     = cfg.rainfall_url
            expected_var = cfg.variable.get_band("precipitation")
            version      = "v3.1"
            file_prefix  = "rfe"
        elif prefix == "smcl":
            base_url     = cfg.soil_moisture_url
            expected_var = cfg.variable.get_band("soil_moisture")
            version      = "v2.3.1"
            file_prefix  = "sm"
        else:
            raise ValueError(f"Unknown prefix {prefix}")
        if not expected_var:
            raise ValueError(
                f"TAMSAT settings missing band name for prefix {prefix!r}"
            )

        lat0, lon0 = self.location_coord
        values_by_date: dict = {}

        session = requests.Session()
        session.headers.update({"User-Agent": "Mozilla/5.0"})

        try:
            for dt in self.dates:
                file_name = (
                    f"{file_prefix}{dt.year}_{dt.month:02d}_{dt.day:02d}"
                    f".{version}.nc"
                )
                url = (
                    f"{base_url.rstrip('/')}/{dt.year}/{dt.month:02d}/{file_name}"
                )
                try:
                    resp = session.get(url, timeout=30)
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

                        # Each daily file has time-dim 1; just read the scalar.
                        arr = np.asarray(da.values, dtype=float).ravel()
                        values_by_date[dt] = (
                            float(arr[0]) if arr.size else np.nan
                        )
                    finally:
                        ds.close()
                        try:
                            if tmp_path:
                                os.unlink(tmp_path)
                        except OSError:
                            pass
                except Exception as e:
                    logger.warning(
                        f"TAMSAT fetch failed for {dt.isoformat()} ({url}): {e}"
                    )
                    values_by_date[dt] = np.nan
        finally:
            session.close()

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