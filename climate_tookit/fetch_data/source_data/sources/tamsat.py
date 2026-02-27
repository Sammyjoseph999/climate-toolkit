"""
TAMSAT Data Downloader

Provides functionality to download monthly rainfall (RFE) and soil moisture (SMCL)
data from TAMSAT servers. Returns daily aggregated values for a location and time range.
"""

import pandas as pd
import xarray as xr
import requests
from io import BytesIO
from datetime import date, timedelta
from typing import Optional
from collections import defaultdict
from sources.utils.models import DataDownloadBase, ClimateVariable
from sources.utils.settings import Settings

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

    def _read_nc_variable(self, prefix: str) -> list[float]:
        values = []
        cfg = self.settings.tamsat
        if prefix == "rfe":
            base_url = cfg.rainfall_url
            expected_var = cfg.variable.precipitation
            version = "v3.1"
            file_template = f"rfe{{year}}{{month:02d}}_{version}.nc"
        elif prefix == "smcl":
            base_url = cfg.soil_moisture_url
            expected_var = cfg.variable.soil_moisture
            version = "v2.3.1"
            file_template = f"sm{{year}}{{month:02d}}_{version}.nc"
        else:
            raise ValueError(f"Unknown prefix {prefix}")

        grouped = defaultdict(list)
        for dt in self.dates:
            grouped[(dt.year, dt.month)].append(dt)

        headers = {"User-Agent": "Mozilla/5.0"}
        for (year, month), day_list in grouped.items():
            file_name = file_template.format(year=year, month=month)
            url = f"{base_url.rstrip('/')}/{year}/{file_name}"
            try:
                resp = requests.get(url, headers=headers, timeout=30)
                resp.raise_for_status()
                with BytesIO(resp.content) as file_obj:
                    ds = xr.open_dataset(file_obj)
                    mean_val = float(ds[expected_var].mean().values)
                    values.extend([mean_val] * len(day_list))
                    ds.close()
            except Exception:
                values.extend([0.0] * len(day_list))
        return values

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
        data_dict = {}
        for variable in self.variables:
            if variable in [ClimateVariable.rainfall, ClimateVariable.precipitation]:
                data_dict["precipitation"] = self.download_precipitation()
            elif variable == ClimateVariable.soil_moisture:
                data_dict["soil_moisture"] = self.download_soil_moisture()
            else:
                data_dict[variable.name] = [0.0] * len(self.dates)
        return pd.DataFrame({"date": self.dates, **data_dict})