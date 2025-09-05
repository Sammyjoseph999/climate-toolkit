import sys
import os
import pandas as pd
import numpy as np
import argparse
import json
import importlib.util
from datetime import datetime, timedelta, date
from typing import Dict, List, Tuple, Optional, Any

current_dir = os.path.dirname(__file__)
parent_dir = os.path.dirname(current_dir)
sys.path.append(os.path.join(parent_dir, 'fetch_data'))
sys.path.append(os.path.join(parent_dir, 'fetch_data', 'transform_data'))
sys.path.append(os.path.join(parent_dir, 'fetch_data', 'source_data'))
sys.path.append(os.path.join(parent_dir, 'fetch_data', 'preprocess_data'))

from fetch_data import fetch_data
from sources.utils.models import ClimateDataset, ClimateVariable

preprocess_path = os.path.join(parent_dir, 'fetch_data', 'preprocess_data', 'preprocess_data.py')
spec = importlib.util.spec_from_file_location("preprocess_module", preprocess_path)
preprocess_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(preprocess_module)
preprocess_climate_data = preprocess_module.preprocess_data

class JagermeyerCropCalendar:

    CROP_MAPPING = {
        'maize': 'maize',
        'wheat': 'wheat',
        'rice': 'rice',
        'soybean': 'soybean',
        'barley': 'barley',
        'millet': 'millet',
        'rapeseed': 'rapeseed',
        'rye': 'rye',
        'sorghum': 'sorghum',
        'sugarbeet': 'sugar_beet',
        'sugarcane': 'sugar_cane',
        'cotton': 'cotton',
        'cassava': 'cassava',
        'groundnut': 'groundnut',
        'fieldpea': 'field_pea',
        'sunflower': 'sunflower',
        'drybean': 'dry_bean',
        'potato': 'potato'
    }

    def get_crop_calendar(self, crop: str, location_coord: Tuple[float, float],
                         irrigation_type: str = 'rainfed') -> Optional[Dict]:
        crop_key = self.CROP_MAPPING.get(crop.lower())
        if not crop_key:
            return None

        try:
            calendar_data = fetch_data(
                location_coord=location_coord,
                variables=['planting_day', 'maturity_day'],
                source='jagermeyr_calendar',
                date_from='2000-01-01',
                date_to='2000-12-31',
                settings={'crop': crop_key, 'irrigation': irrigation_type}
            )

            if calendar_data and len(calendar_data) > 0:
                return {
                    'planting_doy': int(calendar_data.iloc[0]['planting_day']),
                    'maturity_doy': int(calendar_data.iloc[0]['maturity_day']),
                    'source': 'jagermeyr'
                }
        except:
            pass

        return None

class MIRCACropCalendar:

    CROP_MAPPING = {
        'maize': 'maize',
        'wheat': 'wheat',
        'rice': 'rice',
        'barley': 'barley',
        'cassava': 'cassava',
        'cotton': 'cotton',
        'groundnut': 'groundnuts',
        'millet': 'millet',
        'potato': 'potatoes',
        'rapeseed': 'rapeseed',
        'rye': 'rye',
        'sugarbeet': 'sugar_beet',
        'sugarcane': 'sugar_cane',
        'sorghum': 'sorghum',
        'soybean': 'soybeans',
        'sunflower': 'sunflower'
    }

    def get_crop_calendar(self, crop: str, location_coord: Tuple[float, float],
                         irrigation_type: str = 'rainfed') -> Optional[Dict]:
        crop_key = self.CROP_MAPPING.get(crop.lower())
        if not crop_key:
            return None

        try:
            calendar_data = fetch_data(
                location_coord=location_coord,
                variables=['planting_month', 'maturity_month'],
                source='mirca_calendar',
                date_from='2015-01-01',
                date_to='2015-12-31',
                settings={'crop': crop_key, 'irrigation': irrigation_type}
            )

            if calendar_data and len(calendar_data) > 0:
                planting_month = int(calendar_data.iloc[0]['planting_month'])
                maturity_month = int(calendar_data.iloc[0]['maturity_month'])

                planting_doy = (planting_month - 1) * 30 + 15
                maturity_doy = (maturity_month - 1) * 30 + 15

                return {
                    'planting_doy': planting_doy,
                    'maturity_doy': maturity_doy,
                    'source': 'mirca'
                }
        except:
            pass

        return None

class SeasonAnalysis:

    def __init__(self):
        self.jagermeyr = JagermeyerCropCalendar()
        self.mirca = MIRCACropCalendar()

    def analyze_season(self, crop: str, location_coord: Tuple[float, float],
                      crop_calendar: Optional[Dict] = None,
                      date_range: Optional[Tuple[str, str]] = None,
                      settings: Optional[Dict] = None) -> Dict[str, Any]:

        input_params = {
            'crop': crop,
            'location_coord': location_coord,
            'crop_calendar': crop_calendar,
            'date_range': date_range,
            'settings': settings or {}
        }

        if crop_calendar and self._is_crop_calendar_available(crop_calendar):
            season_info = self._extract_season_from_calendar(crop_calendar)
        else:
            calendar_data = self._fetch_crop_calendar(crop, location_coord)
            if calendar_data:
                season_info = self._extract_season_from_calendar(calendar_data)
            else:
                season_info = self._calculate_season_from_rainfall(
                    location_coord, date_range, settings
                )

        climate_data = self._get_climate_indicators(
            location_coord, season_info, settings
        )

        aligned_data = self._align_climate_with_season(climate_data, season_info)

        results = {
            'input_parameters': input_params,
            'season_information': season_info,
            'climate_indicators': aligned_data,
            'analysis_metadata': self._generate_metadata()
        }

        return results

    def _fetch_crop_calendar(self, crop: str, location_coord: Tuple[float, float]) -> Optional[Dict]:
        calendar_data = self.jagermeyr.get_crop_calendar(crop, location_coord)
        if not calendar_data:
            calendar_data = self.mirca.get_crop_calendar(crop, location_coord)
        return calendar_data

    def _is_crop_calendar_available(self, crop_calendar: Dict) -> bool:
        required_fields = ['planting_doy', 'maturity_doy']
        return all(field in crop_calendar and crop_calendar[field] for field in required_fields)

    def _convert_to_proper_types(self, variables: List[str], source: str,
                               date_from: str, date_to: str, settings: Dict) -> Tuple:

        source_mapping = {
            'chirps': ClimateDataset.chirps,
            'era_5': ClimateDataset.era_5,
            'terraclimate': ClimateDataset.terraclimate,
            'imerg': ClimateDataset.imerg,
            'cmip6': ClimateDataset.cmip6,
            'nex_gddp': ClimateDataset.nex_gddp,
            'chirts': ClimateDataset.chirts,
            'agera_5': ClimateDataset.agera_5,
            'soil_grid': ClimateDataset.soil_grid,
            'tamsat': ClimateDataset.tamsat,
            'nasa_power': ClimateDataset.nasa_power
        }
        climate_source = source_mapping.get(source.lower(), ClimateDataset.chirps)

        var_mapping = {
            'precipitation': ClimateVariable.precipitation,
            'temperature': ClimateVariable.max_temperature,
            'max_temperature': ClimateVariable.max_temperature,
            'min_temperature': ClimateVariable.min_temperature,
            'humidity': ClimateVariable.humidity,
            'solar_radiation': ClimateVariable.solar_radiation,
            'soil_moisture': ClimateVariable.soil_moisture
        }
        climate_variables = [var_mapping.get(var.lower(), ClimateVariable.precipitation) for var in variables]

        date_from_obj = date.fromisoformat(date_from)
        date_to_obj = date.fromisoformat(date_to)

        from sources.utils.settings import Settings
        settings_obj = Settings.load()

        return climate_variables, climate_source, date_from_obj, date_to_obj, settings_obj

    def _extract_season_from_calendar(self, crop_calendar: Dict) -> Dict[str, Any]:
        planting_doy = crop_calendar.get('planting_doy')
        maturity_doy = crop_calendar.get('maturity_doy')

        season_length = maturity_doy - planting_doy
        if season_length < 0:
            season_length += 365

        return {
            'method': 'crop_calendar',
            'planting_doy': planting_doy,
            'maturity_doy': maturity_doy,
            'season_length_days': season_length,
            'source': crop_calendar.get('source', 'user_provided')
        }

    def _calculate_season_from_rainfall(self, location_coord: Tuple[float, float],
                                      date_range: Optional[Tuple[str, str]],
                                      settings: Optional[Dict]) -> Dict[str, Any]:

        if not date_range:
            current_year = datetime.now().year
            date_range = (f"{current_year-2}-01-01", f"{current_year}-12-31")

        try:
            rainfall_data = fetch_data(
                location_coord=location_coord,
                variables=['precipitation'],
                source='nasa_power',
                date_from=date_range[0],
                date_to=date_range[1],
                settings=settings or {}
            )

            processed_data = preprocess_climate_data(
                source='nasa_power',
                location_coord=location_coord,
                variables=['precipitation'],
                date_from=date_range[0],
                date_to=date_range[1],
                settings=settings or {},
                transformed_data=rainfall_data
            )
            season_info = self._analyze_rainfall_patterns(processed_data)
            season_info['method'] = 'rainfall_analysis'

            return season_info

        except Exception as e:
            print(f"Rainfall analysis failed: {e}")
            return self._get_default_season_info()

    def _analyze_rainfall_patterns(self, rainfall_data: pd.DataFrame) -> Dict[str, Any]:

        if rainfall_data.empty:
            return {
                'planting_doy': 75,
                'maturity_doy': 285,
                'season_start_month': 3,
                'season_end_month': 10,
                'onset_threshold': 0,
                'end_threshold': 0,
                'monthly_rainfall': {}
            }

        if 'date' in rainfall_data.columns:
            rainfall_data = rainfall_data.copy()
            rainfall_data['month'] = pd.to_datetime(rainfall_data['date']).dt.month
            monthly_rain = rainfall_data.groupby('month')['precipitation'].mean()
        else:
            monthly_rain = rainfall_data.groupby('month')['precipitation'].mean()

        if monthly_rain.empty:
            return {
                'planting_doy': 75,
                'maturity_doy': 285,
                'season_start_month': 3,
                'season_end_month': 10,
                'onset_threshold': 0,
                'end_threshold': 0,
                'monthly_rainfall': {}
            }

        onset_threshold = monthly_rain.mean() * 0.5
        end_threshold = monthly_rain.mean() * 0.3

        season_start_month = None
        for month in range(1, 13):
            if month in monthly_rain.index and monthly_rain[month] > onset_threshold:
                season_start_month = month
                break

        season_end_month = None
        if season_start_month:
            for month in range(season_start_month + 1, 13):
                if month in monthly_rain.index and monthly_rain[month] < end_threshold:
                    season_end_month = month
                    break

        planting_doy = (season_start_month - 1) * 30 + 15 if season_start_month else 75
        maturity_doy = (season_end_month - 1) * 30 + 15 if season_end_month else 285

        return {
            'planting_doy': planting_doy,
            'maturity_doy': maturity_doy,
            'season_start_month': season_start_month,
            'season_end_month': season_end_month,
            'onset_threshold': onset_threshold,
            'end_threshold': end_threshold,
            'monthly_rainfall': monthly_rain.to_dict()
        }

    def _get_climate_indicators(self, location_coord: Tuple[float, float],
                              season_info: Dict, settings: Optional[Dict]) -> Dict[str, Any]:

        return {
            'season_method': season_info.get('method'),
            'season_alignment': 'completed',
            'planting_doy': season_info.get('planting_doy'),
            'maturity_doy': season_info.get('maturity_doy'),
            'note': 'Climate indicators temporarily disabled - season analysis complete'
        }

    def _align_climate_with_season(self, climate_data: Dict, season_info: Dict) -> Dict[str, Any]:
        if climate_data is None or (isinstance(climate_data, pd.DataFrame) and climate_data.empty) or (isinstance(climate_data, dict) and len(climate_data) == 0):
            return {'error': 'No climate data available'}

        aligned_data = {
            'season_method': season_info.get('method'),
            'climate_summary': self._summarize_climate_data(climate_data),
            'season_alignment': 'completed',
            'planting_doy': season_info.get('planting_doy'),
            'maturity_doy': season_info.get('maturity_doy')
        }

        return aligned_data

    def _summarize_climate_data(self, climate_data: Dict) -> Dict[str, Any]:
        summary = {}

        if isinstance(climate_data, pd.DataFrame):
            for column in climate_data.select_dtypes(include=[np.number]).columns:
                summary[column] = {
                    'mean': float(climate_data[column].mean()),
                    'min': float(climate_data[column].min()),
                    'max': float(climate_data[column].max()),
                    'std': float(climate_data[column].std())
                }

        return summary

    def _get_default_season_info(self) -> Dict[str, Any]:
        return {
            'method': 'default',
            'planting_doy': 75,
            'maturity_doy': 285,
            'season_length_days': 210,
            'note': 'Using default growing season due to data unavailability'
        }

    def _generate_metadata(self) -> Dict[str, Any]:
        return {
            'analysis_date': datetime.now().isoformat(),
            'version': '1.0',
            'methodology': 'season_detection_with_crop_calendars'
        }

def perform_season_analysis(crop: str, location_coord: Tuple[float, float],
                          crop_calendar: Optional[Dict] = None,
                          date_range: Optional[Tuple[str, str]] = None,
                          settings: Optional[Dict] = None) -> Dict[str, Any]:

    analyzer = SeasonAnalysis()
    return analyzer.analyze_season(crop, location_coord, crop_calendar, date_range, settings)

def main():
    parser = argparse.ArgumentParser(description='Perform season analysis for crops')
    parser.add_argument('crop', help='Crop name (e.g., maize, wheat, rice)')
    parser.add_argument('--location', required=True,
                       help='Location coordinates as "lat,lon" (e.g., "-1.286,36.817")')
    parser.add_argument('--date-from', required=True,
                       help='Start date in YYYY-MM-DD format')
    parser.add_argument('--date-to', required=True,
                       help='End date in YYYY-MM-DD format')
    parser.add_argument('--crop-calendar',
                       help='Crop calendar as JSON string with planting_doy and maturity_doy')
    parser.add_argument('--irrigation', default='rainfed',
                       choices=['rainfed', 'irrigated'],
                       help='Irrigation type (default: rainfed)')

    args = parser.parse_args()

    try:
        lat, lon = map(float, args.location.split(','))
        location_coord = (lat, lon)
    except ValueError:
        print("Error: Invalid location format. Use 'lat,lon' format.")
        return

    date_range = (args.date_from, args.date_to)

    crop_calendar = None
    if args.crop_calendar:
        try:
            crop_calendar = json.loads(args.crop_calendar)
        except json.JSONDecodeError:
            print("Error: Invalid crop calendar JSON format.")
            return

    settings = {'irrigation_type': args.irrigation}

    try:
        results = perform_season_analysis(
            crop=args.crop,
            location_coord=location_coord,
            crop_calendar=crop_calendar,
            date_range=date_range,
            settings=settings
        )

        print(json.dumps(results, indent=2, default=str))

    except Exception as e:
        print(f"Error during analysis: {e}")

if __name__ == "__main__":
    main()

# python -m climate_tookit.season_analysis.seasons maize --location="-1.286,36.817" --date-from 2020-01-01 --date-to 2020-12-31

# python -m climate_tookit.season_analysis.seasons maize --location="-1.286,36.817" --date-from 2020-01-01 --date-to 2020-12-31 --crop-calendar '{"planting_doy": 70, "maturity_doy": 160}'