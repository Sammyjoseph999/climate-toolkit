import logging
import os
from datetime import date, datetime, timedelta
from typing import Optional

import ee
import pandas as pd
from dotenv import load_dotenv

from .models import Cadence
from .settings import set_logging

load_dotenv()

set_logging()
logger = logging.getLogger(__name__)


def get_gee_data_daily_old(
    image_name: str,
    location_coord: tuple[float],
    from_date: date,
    to_date: date,
    scale: Optional[float] = None,
    crs: Optional[str] = None,
    location_name: Optional[str] = None,
    max_pixels: float = 1e9,
    cadence: Cadence = Cadence.daily,
    tile_scale: float = 1,
) -> pd.DataFrame:
    """Uses the Google Earth Engine (GEE) API to retrieve weather information
    from a weather dataset. Used for dataset whose cadence is daily.

    API ref: https://developers.google.com/earth-engine/apidocs/

    **Pre-requisites:**\n
    - [Register or create](https://www.google.com/url?q=https%3A%2F%2Fcode.earthengine.google.com%2Fregister)
    a Google Cloud Project
    - Register your project for commercial or noncommercial use
    - Enable the "Google Earth Engine API"

    Args
    ---
    - collection: The GEE image collection name
    - location_coord: The point geometry (longitude, latitude)
    - from_date, to_date: The date range
    - location_name: The name of the location_coord
    - cadence: cadence type (daily, monthly, etc). Only daily is currently supported
    - scale: A nominal scale in meters of the projection to work in.
    - crs: The projection to work in. If unspecified, the projection of the
    image's first band is used. If specified in addition to scale, rescaled to the specified scale.
    - tile_scale: A scaling factor between 0.1 and 16 used to adjust cadence
    tile size; setting a larger tileScale (e.g., 2 or 4) uses smaller tiles
    and may enable computations that run out of memory with the default.
    - max_pixels: The maximum number of pixels to reduce.

    Returns
    ---
    A pandas dataframe containing all the varibles in that climate dataset
    """

    logger.info("Authenticating to GEE...")
    ee.Authenticate()
    ee.Initialize(project=os.environ.get("GCP_PROJECT_ID"))

    if cadence != Cadence.daily:
        logger.warning("Only daily cadence is supported.")

    # define the location
    location = (
        ee.Geometry.Point(location_coord)
        if location_name is None
        else ee.Geometry.Point(location_coord, {"location": location_name})
    )

    date_format = "%Y-%m-%d"
    delta = timedelta(days=1)
    current_date = from_date
    tbl = pd.DataFrame()

    logger.info(f"Retrieving information from GEE Image: {image_name}")

    while current_date <= to_date:
        next_date = current_date + delta

        # get an image
        dataset = ee.ImageCollection(image_name).filterDate(
            current_date.strftime(date_format),
            next_date.strftime(date_format),
        )

        image = dataset.filterBounds(location).first()

        # reduce the image
        expression = image.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=location,
            scale=scale,
            maxPixels=max_pixels,
            crs=crs,
            bestEffort=True,
            tileScale=tile_scale,
        )

        data = {"date": current_date, **expression.getInfo()}
        temp = pd.DataFrame(data, index=[0])
        tbl = pd.concat(objs=[tbl, temp], axis=0)

        current_date = next_date

    return tbl.reset_index(drop=True)


def get_gee_data_monthly_old(
    image_name: str,
    location_coord: tuple[float],
    from_date: date,
    to_date: date,
    scale: Optional[float] = None,
    crs: Optional[str] = None,
    location_name: Optional[str] = None,
    max_pixels: float = 1e9,
    tile_scale: float = 1,
):
    """Uses the Google Earth Engine (GEE) API to retrieve weather information
    from a weather dataset. Used for dataset whose cadence is monthly.

    API ref: https://developers.google.com/earth-engine/apidocs/

    **Pre-requisites:**\n
    - [Register or create](https://www.google.com/url?q=https%3A%2F%2Fcode.earthengine.google.com%2Fregister)
    a Google Cloud Project
    - Register your project for commercial or noncommercial use
    - Enable the "Google Earth Engine API"

    Args
    ---
    - collection: The GEE image collection name
    - location_coord: The point geometry (longitude, latitude)
    - from_date, to_date: The date range
    - location_name: The name of the location_coord
    - cadence: cadence type (daily, monthly, etc). Only daily is currently supported
    - scale: A nominal scale in meters of the projection to work in.
    - crs: The projection to work in. If unspecified, the projection of the
    image's first band is used. If specified in addition to scale, rescaled to the specified scale.
    - tile_scale: A scaling factor between 0.1 and 16 used to adjust cadence
    tile size; setting a larger tileScale (e.g., 2 or 4) uses smaller tiles
    and may enable computations that run out of memory with the default.
    - max_pixels: The maximum number of pixels to reduce.

    Returns
    ---
    A pandas dataframe containing all the varibles in that climate dataset
    """

    logger.info("Authenticating to GEE...")
    ee.Authenticate()
    ee.Initialize(project=os.environ.get("GCP_PROJECT_ID"))

    location = (
        ee.Geometry.Point(location_coord)
        if location_name is None
        else ee.Geometry.Point(location_coord, {"location": location_name})
    )

    date_format = "%Y-%m"
    current_date_str = from_date.strftime(date_format) + "-01"
    current_date = datetime.strptime(current_date_str, "%Y-%m-%d")
    final_date_str = to_date.strftime(date_format) + "-01"
    final_date = datetime.strptime(final_date_str, "%Y-%m-%d")
    delta = timedelta(days=31)
    tbl = pd.DataFrame()

    logger.info(f"Retrieving information from GEE Image: {image_name}")

    while current_date <= final_date:
        next_date = current_date + delta

        dataset = ee.ImageCollection(image_name).filterDate(
            start=current_date.strftime(date_format) + "-01",
            end=next_date.strftime(date_format) + "-01",
        )

        image = dataset.filterBounds(location).first()

        expression = image.reduceRegion(
            reducer=ee.Reducer.mean(),
            geometry=location,
            scale=scale,
            maxPixels=max_pixels,
            crs=crs,
            bestEffort=True,
            tileScale=tile_scale,
        )

        data = {
            "date": current_date.strftime(date_format) + "-01",
            **expression.getInfo(),
        }
        temp = pd.DataFrame(data, index=[0])
        tbl = pd.concat(objs=[tbl, temp], axis=0)

        current_date = next_date

    return tbl.reset_index(drop=True)
