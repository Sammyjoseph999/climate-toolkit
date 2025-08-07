"""
This module provides functionality to download monthly rainfall and soil moisture
data from the TAMSAT remote servers. It uses NetCDF datasets accessed via HTTP,
and extracts mean values for the given geographic location and time range.
"""
 
import pandas as pd, xarray as xr, requests
from io import BytesIO
from datetime import date, timedelta
from typing import Optional
from sources.utils import models
from sources.utils.settings import Settings
from collections import defaultdict
 
 
class DownloadData(models.DataDownloadBase):
    def __init__(
        self,
        location_coord: tuple[float],
        date_from_utc: date,
        date_to_utc: date,
        variables: list[models.ClimateVariable] = None,
        aggregation: Optional[str] = None,
        settings: Settings = None,
        source: models.ClimateDataset = None,
    ):
        super().__init__(
            variables=variables or [],
            location_coord=location_coord,
            date_from_utc=date_from_utc,
            date_to_utc=date_to_utc,
        )
        self.location_coord = location_coord
        self.date_from_utc = date_from_utc
        self.date_to_utc = date_to_utc
        self.variables = variables or []
        self.aggregation = aggregation
        self.settings = settings or Settings.load()
        self.source = source
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
            file_template = f"rfe{{year}}_{{month:02d}}.{version}.nc"
        else:
            base_url = cfg.soil_moisture_url
            expected_var = cfg.variable.soil_moisture
            version = "v2.3.1"
            file_template = f"sm{{year}}_{{month:02d}}.{version}.nc"
        grouped = defaultdict(list)
        for dt in self.dates:
            grouped[(dt.year, dt.month)].append(dt)
        headers = {"User-Agent": "Mozilla/5.0"}
        for (year, month), day_list in grouped.items():
            ym_path = f"{year}/{month:02d}/"
            file_name = file_template.format(year=year, month=month)
            url = f"{base_url.rstrip('/')}/{ym_path}{file_name}"
            try:
                print(f"Downloading from: {url}")
                resp = requests.get(url, headers=headers, timeout=30)
                resp.raise_for_status()
                with BytesIO(resp.content) as file_obj:
                    ds = xr.open_dataset(file_obj)
                    if expected_var not in ds:
                        raise KeyError(
                            f"Expected variable '{expected_var}' not in dataset at {url}."
                        )
                    mean_val = float(ds[expected_var].mean().values)
                    values.extend([mean_val] * len(day_list))
                    ds.close()
            except Exception as e:
                print(f"Error reading from {url}: {e}")
                values.extend([0.0] * len(day_list))
        return values
 
    def download_variables(self, settings: Settings = None) -> pd.DataFrame:
        if not self.variables:
            print("No variables specified for download")
            return pd.DataFrame()

        settings = settings or self.settings
        data_dict = {}
        available_vars = set()

        for variable in self.variables:
            if variable in [models.ClimateVariable.rainfall, models.ClimateVariable.precipitation]:
                print("Reading rainfall/precipitation data...")
                data_dict["precipitation"] = self._read_nc_variable("rfe")
                available_vars.add("precipitation")
            elif variable == models.ClimateVariable.soil_moisture:
                print("Reading soil moisture data...")
                data_dict["soil_moisture"] = self._read_nc_variable("smcl")
                available_vars.add("soil_moisture")
            else:
                print(f"TAMSAT does not support variable: {variable.name}")

        df = pd.DataFrame({"date": self.dates, **data_dict})

        requested_vars = [v.name for v in self.variables]

        for var in requested_vars:
            if var not in df.columns:
                print(f"WARNING: TAMSAT does not have {var} data")

        print(f"Available columns: {df.columns.tolist()}")
        print(f"Requested variables: {requested_vars}")

        final_columns = ["date"] + [col for col in requested_vars if col in df.columns]
        return df[final_columns]

    def download_precipitation(self):
        raise NotImplementedError
 
    def download_temperature(self):
        raise NotImplementedError
 
    def download_windspeed(self):
        raise NotImplementedError
 
    def download_solar_radiation(self):
        raise NotImplementedError
 
    def download_humidity(self):
        raise NotImplementedError
 
    def download_rainfall(self):
        raise NotImplementedError
 
    def download_soil_moisture(self):
        raise NotImplementedError