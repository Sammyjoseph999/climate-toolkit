"""
This module provides functionality to download monthly climate data
from the NASA POWER API.
"""
 
import pandas as pd
import requests
from datetime import date
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
 
    def _get_parameter_codes(self) -> list[str]:
        nasa_cfg = self.settings.nasa_power
        params = []
 
        if models.ClimateVariable.precipitation in self.variables:
            params.append("PRECTOTCORR")
        if models.ClimateVariable.max_temperature in self.variables or models.ClimateVariable.min_temperature in self.variables:
            params.append("T2M")
        if models.ClimateVariable.humidity in self.variables:
            params.append("RH2M")
 
        return params
 
    def _fetch_monthly_data(self) -> dict:
        params = self._get_parameter_codes()
        if not params:
            raise ValueError("No valid parameters to request from NASA POWER.")
 
        lat, lon = self.location_coord
        url = (
            f"{self.settings.nasa_power.endpoint}/point"
            f"?start={self.date_from_utc.year}"
            f"&end={self.date_to_utc.year}"
            f"&latitude={lat}&longitude={lon}"
            f"&community=AG"
            f"&parameters={','.join(params)}"
            f"&format=JSON"
        )
 
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            return resp.json().get("properties", {}).get("parameter", {})
        except Exception as e:
            print(f"Error fetching NASA POWER data: {e}")
            return {}
 
    def download_variables(self, settings: Settings = None) -> pd.DataFrame:
        if not self.variables:
            return pd.DataFrame()
 
        raw_data = self._fetch_monthly_data()
        if not raw_data:
            return pd.DataFrame()
 
        data_by_date = defaultdict(dict)
        for var_code, values in raw_data.items():
            for dt_str, val in values.items():
                if len(dt_str) == 6 and dt_str.isdigit():
                    year, month = dt_str[:4], dt_str[4:6]
                    if 1 <= int(month) <= 12:
                        dt = pd.to_datetime(f"{year}-{month}-01")
 
                        # Map NASA codes to readable names
                        if var_code == "PRECTOTCORR":
                            data_by_date[dt]["precipitation"] = val
                        elif var_code == "T2M":
                            data_by_date[dt]["temperature"] = val
                        elif var_code == "RH2M":
                            data_by_date[dt]["humidity"] = val
 
        df = pd.DataFrame([
            {"date": dt, **vals} for dt, vals in sorted(data_by_date.items())
        ])
        return df
 
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