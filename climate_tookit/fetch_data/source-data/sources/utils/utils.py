import logging
import os
from datetime import date, timedelta
from typing import Optional

import ee
import pandas as pd
from dotenv import load_dotenv
from models import GEE_IMAGE, AggregationLevel
from settings import set_logging

load_dotenv()

set_logging()
logger = logging.getLogger(__name__)


def query_gee_daily(
    image_name: GEE_IMAGE,
    location_coord: tuple[float],
    from_date: date,
    to_date: date,
    scale: Optional[float] = None,
    crs: Optional[str] = None,
    location_name: Optional[str] = None,
    max_pixels: float = 10_000_000,
    aggregation: AggregationLevel = AggregationLevel.daily,
    tile_scale: float = 1,
) -> pd.DataFrame:
    """Uses the Google Earth Engine (GEE) API to retrieve weather information
    from a weather dataset.

    API ref: https://developers.google.com/earth-engine/apidocs/

    Pre-requisites:\n
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
    - aggregation: Aggregation type (daily, monthly, etc). Only daily is currently supported
    - scale: A nominal scale in meters of the projection to work in.
    - crs: The projection to work in. If unspecified, the projection of the
    image's first band is used. If specified in addition to scale, rescaled to the specified scale.
    - tile_scale: A scaling factor between 0.1 and 16 used to adjust aggregation
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

    if aggregation != AggregationLevel.daily:
        logger.warning("Only daily aggregation is supported.")

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

    logger.info(f"Retrieving information from {image_name.value}...")

    while current_date <= to_date:
        next_date = current_date + delta

        # get an image
        image = (
            ee.ImageCollection(image_name.value)
            .filterDate(
                current_date.strftime(date_format),
                next_date.strftime(date_format),
            )
            .first()
        )

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

        current_date = next_date

        data = {"date": current_date, **expression.getInfo()}
        temp = pd.DataFrame(data, index=[0])
        tbl = pd.concat(objs=[tbl, temp], axis=0)

    return tbl.reset_index(drop=True)


if __name__ == "__main__":

    nairobi = (36.817223, -1.286389)

    climate_data = query_gee_daily(
        image_name=GEE_IMAGE.imerg,
        location_coord=nairobi,
        from_date=date(2020, 1, 12),
        to_date=date(2020, 1, 14),
        scale=5566,
    )

    print(climate_data.columns)
    print(climate_data)
