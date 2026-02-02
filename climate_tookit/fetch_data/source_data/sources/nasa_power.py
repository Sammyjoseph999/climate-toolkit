"""
This module provides functionality to download DAILY climate data
from the NASA POWER API.

FIXED VERSION: Now fetches daily data instead of monthly aggregates.
COORDINATE FIX: Properly handles (lon, lat) tuple order.
"""

import logging
import pandas as pd
import requests
from datetime import date
from typing import Optional
from .utils import models
from .utils.settings import Settings
from collections import defaultdict

logger = logging.getLogger(__name__)

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
        """Map requested variables to NASA POWER parameter codes."""
        params = []

        var_names = [v.name for v in self.variables]

        # NASA POWER has limited precipitation data (only PRECTOTCORR)
        if 'precipitation' in var_names:
            params.append("PRECTOTCORR")

        # T2M covers temperature (we'll use specific max/min if available)
        if 'max_temperature' in var_names or 'min_temperature' in var_names or 'temperature' in var_names:
            params.append("T2M")
            params.append("T2M_MAX") 
            params.append("T2M_MIN") 

        if 'humidity' in var_names:
            params.append("RH2M")

        if 'solar_radiation' in var_names:
            params.append("ALLSKY_SFC_SW_DWN")

        if 'wind_speed' in var_names:
            params.append("WS2M")

        return params

    def _fetch_daily_data(self) -> dict:
        """Fetch DAILY data from NASA POWER API."""
        params = self._get_parameter_codes()
        if not params:
            raise ValueError("No valid parameters to request from NASA POWER.")

        lon, lat = self.location_coord

        # Format dates as YYYYMMDD for daily data
        start_date = self.date_from_utc.strftime("%Y%m%d")
        end_date = self.date_to_utc.strftime("%Y%m%d")

        # CRITICAL: Use temporal-api=daily for daily data
        url = (
            f"{self.settings.nasa_power.endpoint}/point"
            f"?start={start_date}"
            f"&end={end_date}"
            f"&latitude={lat}&longitude={lon}" 
            f"&community=AG"
            f"&parameters={','.join(params)}"
            f"&format=JSON"
            f"&temporal-api=daily"
        )

        logger.info(f"NASA POWER URL: {url}")
        logger.info(f"NASA POWER Coordinates: lat={lat}, lon={lon}")  

        try:
            resp = requests.get(url, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            return data.get("properties", {}).get("parameter", {})
        except Exception as e:
            logger.error(f"Error fetching NASA POWER data: {e}")
            return {}

    def download_variables(self) -> pd.DataFrame:
        if not self.variables:
            return pd.DataFrame()

        try:
            raw_data = self._fetch_daily_data()
        except ValueError as e:
            logger.error(str(e))
            return pd.DataFrame()

        if not raw_data:
            logger.warning("No data returned from NASA POWER")
            return pd.DataFrame()

        data_by_date = defaultdict(dict)
        available_vars = set()

        for var_code, values in raw_data.items():
            for dt_str, val in values.items():
                if len(dt_str) == 8 and dt_str.isdigit():
                    try:
                        dt = pd.to_datetime(dt_str, format="%Y%m%d")

                        if var_code == "PRECTOTCORR":
                            data_by_date[dt]["precipitation"] = val
                            available_vars.add("precipitation")
                        elif var_code == "T2M_MAX":
                            data_by_date[dt]["max_temperature"] = val
                            available_vars.add("max_temperature")
                        elif var_code == "T2M_MIN":
                            data_by_date[dt]["min_temperature"] = val
                            available_vars.add("min_temperature")
                        elif var_code == "T2M":
                            # Only use T2M if we don't have T2M_MAX/MIN
                            if "max_temperature" not in data_by_date[dt]:
                                data_by_date[dt]["max_temperature"] = val
                            if "min_temperature" not in data_by_date[dt]:
                                data_by_date[dt]["min_temperature"] = val
                            available_vars.update(["max_temperature", "min_temperature"])
                        elif var_code == "RH2M":
                            data_by_date[dt]["humidity"] = val
                            available_vars.add("humidity")
                        elif var_code == "ALLSKY_SFC_SW_DWN":
                            data_by_date[dt]["solar_radiation"] = val
                            available_vars.add("solar_radiation")
                        elif var_code == "WS2M":
                            data_by_date[dt]["wind_speed"] = val
                            available_vars.add("wind_speed")
                    except Exception as e:
                        logger.warning(f"Error parsing date {dt_str}: {e}")
                        continue

        df = pd.DataFrame([
            {"date": dt, **vals} for dt, vals in sorted(data_by_date.items())
        ])

        requested_vars = [v.name for v in self.variables]

        for var in requested_vars:
            if var not in available_vars:
                logger.warning(f"NASA POWER does not have {var} data")

        logger.info(f"NASA POWER returned {len(df)} daily records")
        logger.info(f"Date range: {df['date'].min()} to {df['date'].max()}")
        logger.info(f"Available columns: {df.columns.tolist()}")
        logger.info(f"Requested variables: {requested_vars}")

        final_columns = ["date"] + [col for col in requested_vars if col in df.columns]
        return df[final_columns] if final_columns else pd.DataFrame()

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