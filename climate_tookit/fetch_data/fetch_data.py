import sys
import os
 
current_dir = os.path.dirname(__file__)
sys.path.append(os.path.join(current_dir, 'transform_data'))
sys.path.append(os.path.join(current_dir, 'source_data'))
 
from transform_data import DataTransformer
from source_data import SourceData
 
def fetch_data(location_coord, variables, source, date_from, date_to, settings):
    source_data = SourceData(
        location_coord=location_coord,
        variables=variables,
        source=source,
        date_from_utc=date_from,
        date_to_utc=date_to,
        settings=settings
    )
    raw_data = source_data.download()
 
    transformer = DataTransformer()
    standardized_data = transformer.transform_data(raw_data, source, variables)
 
    return standardized_data