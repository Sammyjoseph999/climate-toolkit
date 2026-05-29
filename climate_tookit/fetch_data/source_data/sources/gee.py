"""This module handles the downloading of climate data from climate sources
hosted by Google Earth Engine (GEE)."""

import logging
import os
from datetime import date, timedelta
from typing import Optional, Union

from dotenv import load_dotenv

load_dotenv()

import ee
import pandas as pd

from .utils import models
from .utils.models import Cadence
from .utils.settings import Settings, set_logging

set_logging()
logger = logging.getLogger(__name__)


# One-time GEE authentication. `ee.Authenticate()` is interactive and slow
# (1–3 s per call when cached, much longer cold); `ee.Initialize()` is also
# not free. They MUST not run per-chunk — when get_gee_data_daily chunks a
# 26-year range into dozens of sub-queries, repeating auth dominates the
# runtime. Use this module-level guard so every entry point is idempotent.
_GEE_READY = False

def _ensure_gee_initialized() -> None:
    """Authenticate + initialize GEE exactly once per Python process."""
    global _GEE_READY
    if _GEE_READY:
        return
    logger.info("Authenticating to GEE (first call)...")
    ee.Authenticate()
    ee.Initialize(project=os.getenv("GCP_PROJECT_ID"))
    _GEE_READY = True


class DownloadData(models.DataDownloadBase):
    def __init__(
        self,
        variables: list[Union[models.ClimateVariable, models.SoilVariable]],
        location_coord: tuple[float],
        date_from_utc: date,
        date_to_utc: date,
        settings: Settings,
        source: models.ClimateDataset,
    ):
        super().__init__(
            location_coord=location_coord,
            date_from_utc=date_from_utc,
            date_to_utc=date_to_utc,
            variables=variables,
        )

        self.date_from_utc = date_from_utc
        self.date_to_utc = date_to_utc
        self.location_coord = location_coord
        self.variables = variables
        self.settings = settings
        self.source = source

    def download_precipitation(self):
        raise NotImplementedError

    def download_temperature(self):
        raise NotImplementedError

    def download_rainfall(self):
        raise NotImplementedError

    def download_windspeed(self):
        raise NotImplementedError

    def download_solar_radiation(self):
        raise NotImplementedError

    def download_humidity(self):
        raise NotImplementedError

    def download_soil_moisture(self):
        raise NotImplementedError

    def get_gee_data_static(
        self,
        image_name: str,
        location_coord: tuple[float],
        scale: Optional[float] = None,
        crs: Optional[str] = None,
        location_name: Optional[str] = None,
        max_pixels: float = 1e9,
        tile_scale: float = 1,
    ) -> pd.DataFrame:
        """Uses the Google Earth Engine (GEE) API to retrieve static data
        from datasets that don't have temporal components (like SoilGrids)."""
        _ensure_gee_initialized()

        lat, lon = location_coord
        location = (
            ee.Geometry.Point([lon, lat])
            if location_name is None
            else ee.Geometry.Point([lon, lat], {"location": location_name})
        )

        logger.info(f"Retrieving information from GEE Image: {image_name}")

        try:
            image = ee.Image(image_name)
            expression = image.reduceRegion(
                reducer=ee.Reducer.mean(),
                geometry=location,
                scale=scale,
                maxPixels=max_pixels,
                crs=crs,
                bestEffort=True,
                tileScale=tile_scale,
            )

            result = expression.getInfo()
            return pd.DataFrame([result]) if result else pd.DataFrame()

        except Exception as e:
            logger.error(f"Error retrieving static data from GEE: {e}")
            raise

    def _get_gee_data_daily_single_range(
        self,
        image_name: str,
        location_coord: tuple[float, float],
        from_date: date,
        to_date: date,
        scale: Optional[float] = None,
        crs: Optional[str] = None,
        location_name: Optional[str] = None,
        max_pixels: float = 1e9,
        tile_scale: float = 1,
        bands: Optional[list[str]] = None,
    ) -> pd.DataFrame:
        """Internal method to fetch data for a single date range."""

        _ensure_gee_initialized()

        # GEE expects [longitude, latitude], but location_coord is (lat, lon)
        lat, lon = location_coord
        location = ee.Geometry.Point([lon, lat])

        start = ee.Date(from_date.strftime("%Y-%m-%d"))
        end = ee.Date(to_date.strftime("%Y-%m-%d")).advance(1, "day")

        logger.info(f"Fetching data for location: lat={lat}, lon={lon} (GEE Point: [{lon}, {lat}])")
        logger.info(f"Using scale: {scale} meters")

        # For sub-daily collections, pre-aggregate to daily on the server.
        # This is the difference between 3 round-trips and ~100 for a long
        # IMERG fetch. Daily-cadence sources skip this and use the raw
        # collection unchanged.
        if image_name in self._GEE_DAILY_AGG_REDUCER:
            logger.info(
                f"Server-side daily aggregation enabled for {image_name} "
                f"(reducer={self._GEE_DAILY_AGG_REDUCER[image_name]!r})"
            )
            collection = self._daily_aggregated_collection(
                image_name, start, end, location, bands=bands,
            )
        else:
            collection = (
                ee.ImageCollection(image_name)
                .filterDate(start, end)
                .filterBounds(location)
            )

        def extract(image):
            reduce_args = {
                "reducer": ee.Reducer.first(),
                "geometry": location,
                "maxPixels": max_pixels,
                "tileScale": tile_scale,
            }

            if scale is not None:
                reduce_args["scale"] = scale

            if crs is not None:
                reduce_args["crs"] = crs

            values = image.reduceRegion(**reduce_args)
            return ee.Feature(None, values).set(
                "date", image.date().format("YYYY-MM-dd")
            )

        feature_collection = collection.map(extract)

        # Single server call for result rather than one per day
        result = feature_collection.getInfo()

        features = result.get("features", [])
        records = [f["properties"] for f in features]

        df = pd.DataFrame(records) if records else pd.DataFrame()

        if not df.empty:
            logger.info(f"=== GEE RETURNED COLUMNS: {list(df.columns)}")
            logger.info(f"=== SAMPLE ROW (first): {df.iloc[0].to_dict()}")

            df = df.sort_values("date").reset_index(drop=True)

            # Defensive: with server-side daily aggregation there should be
            # no duplicates, but keep the dedup path for any sub-daily source
            # not yet in `_GEE_DAILY_AGG_REDUCER`.
            if df.duplicated("date").any():
                logger.info("Duplicate dates detected — aggregating sub-daily data to daily totals/means.")
                numeric_cols = df.select_dtypes(include="number").columns.tolist()
                agg_dict = {col: 'sum' if 'precipitation' in col.lower() else 'mean' for col in numeric_cols}
                df = df.groupby("date", as_index=False).agg(agg_dict)

            # Ensure full daily index (GEE skips missing days)
            full_range = pd.date_range(from_date, to_date, freq="D")
            df["date"] = pd.to_datetime(df["date"], errors="coerce")

            df = (
                df.set_index("date")
                  .reindex(full_range)
                  .rename_axis("date")
                  .reset_index()
            )

            df["date"] = df["date"].dt.strftime("%Y-%m-%d")
        else:
            full_range = pd.date_range(from_date, to_date, freq="D")
            df = pd.DataFrame({"date": full_range.strftime("%Y-%m-%d")})

        return df

    # Sub-daily collections that should be aggregated to daily on the SERVER
    # before any download. Without this, IMERG (half-hourly, 48 imgs/day) and
    # ERA5-Land hourly (24 imgs/day) require tiny client chunks (~90 d) which
    # costs ~106 round-trips for a 26-year IMERG fetch. Aggregating to daily
    # server-side collapses 48x or 24x elements per day to 1x, so the default
    # 4380-day chunk works for them too -> ~35x fewer round-trips.
    #
    # The reducer matters: precipitation must sum across the day; everything
    # else uses mean. Default is mean if the source isn't listed.
    _GEE_DAILY_AGG_REDUCER = {
        "NASA/GPM_L3/IMERG_V07": "sum",      
        "NASA/GPM_L3/IMERG_V06": "sum",
        "ECMWF/ERA5_LAND/HOURLY": "mean",    
    }
    # All other GEE collections are daily-cadence (ERA5, CHIRPS, CHIRTS, AgERA5, TerraClimate). 1 image/day, so a 12-year chunk = ~4380 elements,
    # comfortably under GEE's 5000 ceiling. 26 years -> 3 chunks.
    _GEE_DEFAULT_CHUNK_DAYS = 4380
    _GEE_MIN_CHUNK_DAYS = 7                  # don't bisect smaller than this

    def _daily_aggregated_collection(self, image_name, start, end, location,
                                     bands: Optional[list[str]] = None):
        """
        Build a server-side daily ImageCollection from a sub-daily source.

        For each calendar day in [start, end), filters the raw sub-daily
        collection to that day and reduces it with the source-appropriate
        reducer (`sum` for precipitation accumulators like IMERG, `mean`
        otherwise). The result is an ImageCollection with one image per
        day, so the downstream `collection.map(extract)` returns one
        Feature per day instead of one per sub-daily frame.

        `bands` restricts the raw collection to the bands we actually need
        before reducing. This is required for IMERG V07, whose half-hourly
        frames carry a varying band set (some 5-band, some 9-band): the
        retrieval-only bands (MW*/IR* etc.) make the collection heterogeneous,
        and `ImageCollection.sum()`/`.mean()` reject heterogeneous collections
        ("Expected a homogeneous image collection"). Selecting just the
        requested band(s) — which are present in every frame — makes the
        collection homogeneous and also lighter to compute.
        """
        reducer_name = self._GEE_DAILY_AGG_REDUCER.get(image_name, "mean")
        n_days = end.difference(start, "day").floor()
        days_seq = ee.List.sequence(0, n_days.subtract(1))
        raw = (
            ee.ImageCollection(image_name)
            .filterDate(start, end)
            .filterBounds(location)
        )
        if bands:
            raw = raw.select(bands)

        def daily_image(day_offset):
            day_offset = ee.Number(day_offset)
            d      = start.advance(day_offset, "day")
            d_next = d.advance(1, "day")
            slice_ = raw.filterDate(d, d_next)
            agg = slice_.sum() if reducer_name == "sum" else slice_.mean()
            return agg.set("system:time_start", d.millis())

        return ee.ImageCollection.fromImages(days_seq.map(daily_image))

    @staticmethod
    def _is_collection_overflow(err: Exception) -> bool:
        """Detect GEE's '5000 elements' / memory ceiling errors for retry."""
        msg = str(err).lower()
        return ("5000 elements" in msg
                or "user memory limit exceeded" in msg
                or "computation timed out" in msg)

    def get_gee_data_daily(
        self,
        image_name: str,
        location_coord: tuple[float, float],
        from_date: date,
        to_date: date,
        scale: Optional[float] = None,
        crs: Optional[str] = None,
        location_name: Optional[str] = None,
        max_pixels: float = 1e9,
        cadence: Cadence = Cadence.daily,
        tile_scale: float = 1,
        bands: Optional[list[str]] = None,
    ) -> pd.DataFrame:
        """
        Retrieve daily data from a GEE ImageCollection with adaptive chunking.
        - All sources use the same large initial chunk (4380 days); sub-daily sources are pre-aggregated to daily server-side so element counts
          match daily.
        - On a 5000-element / memory error the offending chunk is bisected and retried recursively, down to `_GEE_MIN_CHUNK_DAYS`.
        - Failed chunks return empty DataFrames (logged); successful chunks are concatenated.
        """
        _ensure_gee_initialized()
        logger.info(f"Retrieving information from GEE Image: {image_name}")

        if cadence != Cadence.daily:
            raise NotImplementedError(
                f"Cadence '{cadence}' is not supported. Only daily cadence is currently supported."
            )

        total_days = (to_date - from_date).days
        if total_days < 0:
            logger.warning("from_date is after to_date. Returning empty DataFrame.")
            return pd.DataFrame()

        # All sources use the same large chunk: sub-daily sources are collapsed to daily server-side (see `_GEE_DAILY_AGG_REDUCER` and
        # `_daily_aggregated_collection`), so element counts match daily.
        chunk_size = self._GEE_DEFAULT_CHUNK_DAYS
        logger.info(f"GEE chunking: image={image_name} initial chunk_size={chunk_size}d "
                    f"over {total_days+1}d total")

        chunks: list[pd.DataFrame] = []
        chunk_start = from_date
        while chunk_start <= to_date:
            chunk_end = min(chunk_start + timedelta(days=chunk_size - 1), to_date)
            df_chunk = self._fetch_chunk_with_bisect(
                image_name=image_name,
                location_coord=location_coord,
                from_date=chunk_start,
                to_date=chunk_end,
                scale=scale,
                crs=crs,
                location_name=location_name,
                max_pixels=max_pixels,
                tile_scale=tile_scale,
                bands=bands,
            )
            if not df_chunk.empty:
                chunks.append(df_chunk)
            chunk_start = chunk_end + timedelta(days=1)

        if not chunks:
            return pd.DataFrame()
        return pd.concat(chunks, ignore_index=True)

    def _fetch_chunk_with_bisect(
        self,
        image_name: str,
        location_coord: tuple[float, float],
        from_date: date,
        to_date: date,
        scale: Optional[float],
        crs: Optional[str],
        location_name: Optional[str],
        max_pixels: float,
        tile_scale: float,
        bands: Optional[list[str]] = None,
    ) -> pd.DataFrame:
        """
        Fetch one chunk. On 5000-element / memory error, bisect the range and retry each half, down to `_GEE_MIN_CHUNK_DAYS`. Below that floor
        the chunk is given up on (logged + empty DataFrame returned) — much better than failing the entire request.
        """
        span_days = (to_date - from_date).days + 1
        logger.info(f"Fetching chunk: {from_date} -> {to_date} ({span_days}d)")
        try:
            return self._get_gee_data_daily_single_range(
                image_name=image_name,
                location_coord=location_coord,
                from_date=from_date,
                to_date=to_date,
                scale=scale,
                crs=crs,
                location_name=location_name,
                max_pixels=max_pixels,
                tile_scale=tile_scale,
                bands=bands,
            )
        except Exception as e:
            if not self._is_collection_overflow(e):
                logger.error(f"GEE chunk {from_date}->{to_date} failed: {e}")
                return pd.DataFrame()

            if span_days <= self._GEE_MIN_CHUNK_DAYS:
                logger.error(
                    f"GEE chunk {from_date}->{to_date} ({span_days}d) "
                    f"still overflows at minimum chunk size; giving up on this window."
                )
                return pd.DataFrame()

            mid = from_date + timedelta(days=span_days // 2 - 1)
            logger.warning(
                f"GEE overflow on {from_date}->{to_date}; bisecting at {mid}."
            )
            left = self._fetch_chunk_with_bisect(
                image_name, location_coord, from_date, mid,
                scale, crs, location_name, max_pixels, tile_scale, bands,
            )
            right = self._fetch_chunk_with_bisect(
                image_name, location_coord, mid + timedelta(days=1), to_date,
                scale, crs, location_name, max_pixels, tile_scale, bands,
            )
            parts = [d for d in (left, right) if not d.empty]
            if not parts:
                return pd.DataFrame()
            return pd.concat(parts, ignore_index=True)

    def get_gee_data_monthly(
        self,
        image_name: str,
        location_coord: tuple[float, float],
        from_date: date,
        to_date: date,
        scale: Optional[float] = None,
        crs: Optional[str] = None,
        location_name: Optional[str] = None,
        max_pixels: float = 1e9,
        tile_scale: float = 1,
    ):
        """Uses the GEE API to retrieve weather information for monthly-cadence datasets."""

        _ensure_gee_initialized()

        lat, lon = location_coord
        location = (
            ee.Geometry.Point([lon, lat])
            if location_name is None
            else ee.Geometry.Point([lon, lat], {"location": location_name})
        )

        start_date = ee.Date.fromYMD(from_date.year, from_date.month, 1)
        end_date = ee.Date.fromYMD(to_date.year, to_date.month, 1).advance(1, "month")

        nMonths = end_date.difference(start_date, "month").ceil()
        months = ee.List.sequence(0, nMonths.subtract(1))

        # Filter full range once
        collection = (
            ee.ImageCollection(image_name)
            .filterDate(start_date, end_date)
            .filterBounds(location)
        )

        def get_single_data(month_offset):
            current_month_start = start_date.advance(month_offset, "month")
            current_month_end = current_month_start.advance(1, "month")

            monthly_images = collection.filterDate(current_month_start, current_month_end)
            monthly_image = monthly_images.mean()
            reduce_args = {
                "reducer": ee.Reducer.first(),
                "geometry": location,
            }

            if scale is not None:
                reduce_args["scale"] = scale

            if crs is not None:
                reduce_args["crs"] = crs

            if max_pixels is not None:
                reduce_args["maxPixels"] = max_pixels

            if tile_scale is not None:
                reduce_args["tileScale"] = tile_scale

            values = monthly_image.reduceRegion(**reduce_args)

            return ee.Feature(None, values).set(
                "date", current_month_start.format("YYYY-MM-dd")
            )

        logger.info(f"Retrieving information from GEE Image: {image_name}")
        features = months.map(get_single_data)
        result = features.getInfo()

        data_list = [f["properties"] for f in result] if result else []
        df = pd.DataFrame(data_list) if data_list else pd.DataFrame()

        if not df.empty:
            df = df.sort_values("date").reset_index(drop=True)

        return df

    def _handle_soil_grid(self, data_settings) -> pd.DataFrame:
        """Handle soil_grid with multiple images."""
        result_data = {}

        for variable in self.variables:
            var_name = variable.name

            gee_image = (
                data_settings.gee_images.get(var_name)
                if isinstance(data_settings.gee_images, dict)
                else getattr(data_settings.gee_images, var_name, None)
            )
            if not gee_image:
                logger.warning(f"No GEE image mapping found for variable '{var_name}'")
                continue

            mapped_col = getattr(data_settings.variable, var_name, None)
            if not mapped_col:
                logger.warning(f"No variable mapping found for '{var_name}'")
                continue

            logger.info(f"Downloading {var_name} from {gee_image}")

            try:
                var_data = self.get_gee_data_static(
                    image_name=gee_image,
                    location_coord=self.location_coord,
                    scale=data_settings.resolution,
                )

                if not var_data.empty and mapped_col in var_data.columns:
                    result_data[var_name] = var_data[mapped_col].iloc[0]
                    logger.info(
                        f"Successfully retrieved {var_name}: {result_data[var_name]}"
                    )
                else:
                    logger.warning(
                        f"No data retrieved for {var_name} - column '{mapped_col}' not found"
                    )

            except Exception as e:
                logger.error(f"Error downloading {var_name}: {e}")

        if result_data:
            logger.info(f"Successfully processed {len(result_data)} soil variables")
            return pd.DataFrame([result_data])
        else:
            logger.warning("No soil data successfully retrieved")
            return pd.DataFrame()

    def download_variables(self) -> pd.DataFrame:
        """Download and process variables from the configured data source.

        Handles:
        - Static datasets (like soil grids)
        - Daily time series datasets
        - Monthly time series datasets
        """

        try:
            data_settings = getattr(self.settings, self.source.name)
        except AttributeError:
            logger.error(f"Settings for source '{self.source.name}' not found")
            return pd.DataFrame()

        # Handle soil_grid special case with multiple images
        if self.source.name == "soil_grid" and hasattr(data_settings, "gee_images"):
            logger.info("Using enhanced soil variable download with multiple GEE images")
            df_soil = self._handle_soil_grid(data_settings)
            for v in self.variables:
                mapped_col = data_settings.variable.get_band(v.name)
                var_meta = getattr(data_settings.variable, v.name, None)
                if mapped_col in df_soil.columns and var_meta is not None:
                    scale = getattr(var_meta, "scale", 1.0)
                    df_soil[mapped_col] = df_soil[mapped_col] * scale
                    logger.info(f"Applied scaling to {mapped_col} (scale={scale})")
            return df_soil

        # Standard climate data handling
        try:
            if data_settings.cadence == "static":
                climate_data = self.get_gee_data_static(
                    image_name=data_settings.gee_image,
                    location_coord=self.location_coord,
                    scale=data_settings.resolution,
                )
            elif data_settings.cadence == models.Cadence.monthly.name:
                climate_data = self.get_gee_data_monthly(
                    image_name=data_settings.gee_image,
                    location_coord=self.location_coord,
                    from_date=self.date_from_utc,
                    to_date=self.date_to_utc,
                    scale=data_settings.resolution,
                )
            else:
                # Bands we actually need (drops null-mapped variables). For sub-daily sources these restrict the server-side daily
                # aggregation to a homogeneous, lighter band set; ignored by daily-cadence sources.
                wanted_bands = [
                    b for b in (data_settings.variable.get_band(v.name)
                                for v in self.variables)
                    if b
                ]
                climate_data = self.get_gee_data_daily(
                    image_name=data_settings.gee_image,
                    location_coord=self.location_coord,
                    from_date=self.date_from_utc,
                    to_date=self.date_to_utc,
                    scale=data_settings.resolution,
                    bands=wanted_bands or None,
                )
        except Exception as e:
            logger.error(f"Error downloading data: {e}")
            return pd.DataFrame()

        if climate_data.empty:
            logger.warning("No data retrieved from GEE")
            return pd.DataFrame()

        # Map columns to variable names
        dataset_cols = list(climate_data.columns)
        req_vars = [v.name for v in self.variables]

        available_cols = []
        missing_vars = []

        for v in self.variables:
            mapped_col = data_settings.variable.get_band(v.name)
            if mapped_col and mapped_col in climate_data.columns:
                available_cols.append(mapped_col)
            else:
                logger.warning(f"{self.source.name.upper()} does not have {v.name} data")
                missing_vars.append(v.name)

        logger.info(f"Available columns: {dataset_cols}")
        logger.info(f"Requested variables: {req_vars}")
        logger.info(f"Mapped available columns: {available_cols}")
        if missing_vars:
            logger.info(f"Missing variables: {missing_vars}")

        # Apply scaling for each variable that has a scale factor
        for v in self.variables:
            mapped_col = data_settings.variable.get_band(v.name)
            var_meta = getattr(data_settings.variable, v.name, None)
            if mapped_col in climate_data.columns and var_meta is not None:
                scale = getattr(var_meta, "scale", 1.0)
                climate_data[mapped_col] = climate_data[mapped_col] * scale
                logger.info(f"Applied scaling to {mapped_col} (scale={scale})")

        base_cols = ["date"] if "date" in climate_data.columns else []
        return climate_data[base_cols + available_cols]