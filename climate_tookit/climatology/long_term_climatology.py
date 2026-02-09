"""
Long-term Climatology Module

Calculates climate normals (30-year means) for precipitation and temperature.
Works with datasets that have either precipitation OR temperature OR both.
Follows WMO standards for climatological normal periods.

Standard WMO periods:
- 1961-1990
- 1971-2000
- 1981-2010
- 1991-2020

Dependencies: pandas, preprocess_data pipeline
"""

import sys
import os
from datetime import date
from typing import Dict, List, Any, Tuple, Optional
import pandas as pd
import json
import argparse
from statistics import mean, stdev, median

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, os.path.join(parent_dir, 'fetch_data', 'preprocess_data'))
sys.path.insert(0, os.path.join(parent_dir, 'fetch_data', 'source_data', 'sources'))

try:
    from preprocess_data import preprocess_data
    from utils.models import ClimateVariable
    PREPROCESS_AVAILABLE = True
except ImportError:
    PREPROCESS_AVAILABLE = False
    print("Warning: Preprocessing pipeline not available")

def calculate_annual_statistics(
    lat: float,
    lon: float,
    year: int,
    source: str,
    variables: Optional[List] = None
) -> Optional[Dict[str, Any]]:
    """
    Calculate annual statistics for a single year.
    
    Works with partial data - accepts datasets with only precipitation,
    only temperature, or both.
    
    Args:
        lat: Latitude
        lon: Longitude
        year: Year to analyze
        source: Data source
        variables: List of ClimateVariable enums to fetch
        
    Returns:
        Dictionary with annual statistics or None if failed
    """
    if not PREPROCESS_AVAILABLE:
        raise Exception("Preprocessing pipeline required")
    
    if variables is None:
        variables = [
            ClimateVariable.precipitation,
            ClimateVariable.max_temperature,
            ClimateVariable.min_temperature
        ]
    
    try:
        date_from = date(year, 1, 1)
        date_to = date(year, 12, 31)
        
        df = preprocess_data(
            source=source,
            location_coord=(lat, lon),
            variables=variables,
            date_from=date_from,
            date_to=date_to
        )
        
        if df.empty or len(df) < 300:  # At least 300 days for valid annual stats
            print(f"  ✗ {year}: Insufficient data ({len(df)} days)")
            return None
        
        stats = {}
        has_data = False
        
        # Precipitation statistics
        precip_cols = ['precipitation', 'precip', 'pr', 'rainfall']
        precip_col = None
        for col in precip_cols:
            if col in df.columns:
                precip_col = col
                break
        
        if precip_col:
            precip_data = df[precip_col].copy()
            # Check if we have actual precipitation data (not all nulls/zeros)
            if precip_data.notna().sum() > 0:
                stats['precipitation'] = {
                    'annual_total_mm': float(precip_data.sum()),
                    'annual_mean_daily_mm': float(precip_data.mean()),
                    'annual_median_daily_mm': float(precip_data.median()),
                    'annual_max_daily_mm': float(precip_data.max()),
                    'annual_std_daily_mm': float(precip_data.std()),
                    'rainy_days': int((precip_data > 1.0).sum()),
                    'dry_days': int((precip_data <= 1.0).sum()),
                    'days_with_data': int(precip_data.notna().sum())
                }
                has_data = True
        
        # Temperature statistics
        tmax_cols = ['max_temperature', 'tmax', 'tasmax', 'temperature_max']
        tmin_cols = ['min_temperature', 'tmin', 'tasmin', 'temperature_min']
        
        tmax_col = None
        tmin_col = None
        
        for col in tmax_cols:
            if col in df.columns:
                tmax_col = col
                break
        
        for col in tmin_cols:
            if col in df.columns:
                tmin_col = col
                break
        
        if tmax_col and tmin_col:
            tmax_data = df[tmax_col].copy()
            tmin_data = df[tmin_col].copy()
            
            # Check if we have actual temperature data
            if tmax_data.notna().sum() > 0 and tmin_data.notna().sum() > 0:
                # Convert from Kelvin if needed
                if tmax_data.mean() > 100:
                    tmax_data = tmax_data - 273.15
                    tmin_data = tmin_data - 273.15
                
                tavg = (tmax_data + tmin_data) / 2
                
                stats['temperature'] = {
                    'annual_mean_tmax_c': float(tmax_data.mean()),
                    'annual_mean_tmin_c': float(tmin_data.mean()),
                    'annual_mean_tavg_c': float(tavg.mean()),
                    'annual_max_tmax_c': float(tmax_data.max()),
                    'annual_min_tmin_c': float(tmin_data.min()),
                    'annual_std_tmax_c': float(tmax_data.std()),
                    'annual_std_tmin_c': float(tmin_data.std()),
                    'annual_diurnal_range_c': float((tmax_data - tmin_data).mean()),
                    'days_with_data': int(tmax_data.notna().sum())
                }
                has_data = True
        
        # Only return stats if we have at least one valid variable
        if not has_data:
            print(f"  ✗ {year}: No valid precipitation or temperature data")
            return None
        
        stats['year'] = year
        stats['data_completeness'] = len(df) / 365.0 * 100
        
        return stats
        
    except Exception as e:
        print(f"  ✗ {year}: {str(e)}")
        return None

def calculate_climatology(
    location_coord: Tuple[float, float],
    start_year: int,
    end_year: int,
    source: str,
    variables: Optional[List] = None
) -> Dict[str, Any]:
    """
    Calculate long-term climatology (multi-year normals).
    
    Works with partial data - accepts datasets with only precipitation,
    only temperature, or both.
    
    Args:
        location_coord: (latitude, longitude)
        start_year: Start year of climatology period
        end_year: End year of climatology period (inclusive)
        source: Data source identifier
        variables: Optional list of ClimateVariable enums
        
    Returns:
        Dictionary containing:
        - annual_statistics: List of annual stats for each year
        - climatology: 30-year mean values (for available variables)
        - trends: Linear trends if applicable
    """
    lat, lon = location_coord
    n_years = end_year - start_year + 1
    
    print(f"\n{'='*70}")
    print(f"  CALCULATING {n_years}-YEAR CLIMATOLOGY")
    print(f"{'='*70}")
    print(f"  Location: ({lat:.4f}, {lon:.4f})")
    print(f"  Period: {start_year}-{end_year}")
    print(f"  Source: {source}")
    print(f"{'='*70}\n")
    
    print(f"  Processing {n_years} years...\n")
    
    annual_stats = []
    
    for year in range(start_year, end_year + 1):
        print(f"  [{year - start_year + 1}/{n_years}] {year}...", end=' ')
        
        stats = calculate_annual_statistics(lat, lon, year, source, variables)
        
        if stats:
            annual_stats.append(stats)
            print("✓")
    
    print(f"\n  Complete: {len(annual_stats)}/{n_years} years with valid data\n")
    
    if len(annual_stats) < n_years * 0.8:  # Need at least 80% of years
        return {
            'error': f'Insufficient data: only {len(annual_stats)}/{n_years} years available',
            'location': {'latitude': lat, 'longitude': lon},
            'period': {'start_year': start_year, 'end_year': end_year},
            'source': source
        }
    
    # Calculate climatology (long-term means)
    climatology = {}
    
    # Precipitation climatology (if available)
    precip_annual_totals = [s['precipitation']['annual_total_mm'] for s in annual_stats if 'precipitation' in s]
    precip_annual_means = [s['precipitation']['annual_mean_daily_mm'] for s in annual_stats if 'precipitation' in s]
    
    if precip_annual_totals:
        climatology['precipitation'] = {
            'mean_annual_total_mm': round(mean(precip_annual_totals), 2),
            'median_annual_total_mm': round(median(precip_annual_totals), 2),
            'std_annual_total_mm': round(stdev(precip_annual_totals), 2) if len(precip_annual_totals) > 1 else 0,
            'min_annual_total_mm': round(min(precip_annual_totals), 2),
            'max_annual_total_mm': round(max(precip_annual_totals), 2),
            'mean_daily_mm': round(mean(precip_annual_means), 2),
            'years_used': len(precip_annual_totals)
        }
    
    # Temperature climatology (if available)
    temp_annual_tavg = [s['temperature']['annual_mean_tavg_c'] for s in annual_stats if 'temperature' in s]
    temp_annual_tmax = [s['temperature']['annual_mean_tmax_c'] for s in annual_stats if 'temperature' in s]
    temp_annual_tmin = [s['temperature']['annual_mean_tmin_c'] for s in annual_stats if 'temperature' in s]
    
    if temp_annual_tavg:
        climatology['temperature'] = {
            'mean_annual_tavg_c': round(mean(temp_annual_tavg), 2),
            'mean_annual_tmax_c': round(mean(temp_annual_tmax), 2),
            'mean_annual_tmin_c': round(mean(temp_annual_tmin), 2),
            'std_annual_tavg_c': round(stdev(temp_annual_tavg), 2) if len(temp_annual_tavg) > 1 else 0,
            'min_annual_tavg_c': round(min(temp_annual_tavg), 2),
            'max_annual_tavg_c': round(max(temp_annual_tavg), 2),
            'years_used': len(temp_annual_tavg)
        }
    
    # Warn if we have neither
    if not climatology:
        return {
            'error': 'No valid precipitation or temperature data found in any year',
            'location': {'latitude': lat, 'longitude': lon},
            'period': {'start_year': start_year, 'end_year': end_year},
            'source': source
        }
    
    # Calculate trends if we have enough years
    trends = {}
    if len(annual_stats) >= 10:
        years = [s['year'] for s in annual_stats]
        
        if precip_annual_totals and len(precip_annual_totals) == len(years):
            precip_trend = calculate_linear_trend(years, precip_annual_totals)
            trends['precipitation_trend_mm_per_year'] = round(precip_trend, 3)
        
        if temp_annual_tavg and len(temp_annual_tavg) == len(years):
            temp_trend = calculate_linear_trend(years, temp_annual_tavg)
            trends['temperature_trend_c_per_year'] = round(temp_trend, 4)
    
    # Determine available variables
    available_vars = list(climatology.keys())
    
    result = {
        'location': {'latitude': lat, 'longitude': lon},
        'period': {
            'start_year': start_year,
            'end_year': end_year,
            'n_years': n_years,
            'years_with_data': len(annual_stats)
        },
        'source': source,
        'available_variables': available_vars,
        'climatology': climatology,
        'trends': trends if trends else None,
        'annual_statistics': annual_stats,
        'metadata': {
            'wmo_standard': n_years == 30,
            'data_completeness_pct': round(len(annual_stats) / n_years * 100, 1),
            'variables': ', '.join(available_vars)
        }
    }
    
    return result

def calculate_linear_trend(years: List[int], values: List[float]) -> float:
    """
    Calculate linear trend using least squares regression.
    
    Returns:
        Slope (change per year)
    """
    n = len(years)
    if n < 2:
        return 0.0
    
    mean_year = mean(years)
    mean_value = mean(values)
    
    numerator = sum((years[i] - mean_year) * (values[i] - mean_value) for i in range(n))
    denominator = sum((years[i] - mean_year) ** 2 for i in range(n))
    
    if denominator == 0:
        return 0.0
    
    return numerator / denominator

def print_climatology_report(result: Dict[str, Any]):
    """Print formatted climatology report."""
    
    if 'error' in result:
        print(f"\nError: {result['error']}")
        return
    
    print(f"\n{'='*70}")
    print(f"  CLIMATOLOGY REPORT")
    print(f"{'='*70}")
    print(f"  Location: {result['location']['latitude']:.4f}, {result['location']['longitude']:.4f}")
    print(f"  Period: {result['period']['start_year']}-{result['period']['end_year']} ({result['period']['n_years']} years)")
    print(f"  Source: {result['source']}")
    print(f"  Data Completeness: {result['metadata']['data_completeness_pct']:.1f}%")
    print(f"  Available Variables: {result['metadata']['variables']}")
    
    if result['metadata']['wmo_standard']:
        print(f"  WMO Standard: ✓ (30-year normal)")
    
    print(f"{'='*70}\n")
    
    clim = result['climatology']
    
    # Precipitation
    if 'precipitation' in clim:
        print(f"  {'─'*66}")
        print(f"  PRECIPITATION CLIMATOLOGY")
        print(f"  {'─'*66}")
        p = clim['precipitation']
        print(f"    Mean Annual Total:      {p['mean_annual_total_mm']:>10.2f} mm")
        print(f"    Median Annual Total:    {p['median_annual_total_mm']:>10.2f} mm")
        print(f"    Std Deviation:          {p['std_annual_total_mm']:>10.2f} mm")
        print(f"    Range:                  {p['min_annual_total_mm']:>10.2f} - {p['max_annual_total_mm']:.2f} mm")
        print(f"    Mean Daily:             {p['mean_daily_mm']:>10.2f} mm/day")
        print(f"    Years Used:             {p['years_used']:>10}")
        print()
    
    # Temperature
    if 'temperature' in clim:
        print(f"  {'─'*66}")
        print(f"  TEMPERATURE CLIMATOLOGY")
        print(f"  {'─'*66}")
        t = clim['temperature']
        print(f"    Mean Annual Average:    {t['mean_annual_tavg_c']:>10.2f} °C")
        print(f"    Mean Annual Maximum:    {t['mean_annual_tmax_c']:>10.2f} °C")
        print(f"    Mean Annual Minimum:    {t['mean_annual_tmin_c']:>10.2f} °C")
        print(f"    Std Deviation:          {t['std_annual_tavg_c']:>10.2f} °C")
        print(f"    Range:                  {t['min_annual_tavg_c']:>10.2f} - {t['max_annual_tavg_c']:.2f} °C")
        print(f"    Years Used:             {t['years_used']:>10}")
        print()
    
    # Trends
    if result['trends']:
        print(f"  {'─'*66}")
        print(f"  TRENDS")
        print(f"  {'─'*66}")
        trends = result['trends']
        
        if 'precipitation_trend_mm_per_year' in trends:
            p_trend = trends['precipitation_trend_mm_per_year']
            direction = "↑" if p_trend > 0 else "↓" if p_trend < 0 else "→"
            print(f"    Precipitation:          {direction} {abs(p_trend):.3f} mm/year")
        
        if 'temperature_trend_c_per_year' in trends:
            t_trend = trends['temperature_trend_c_per_year']
            direction = "↑" if t_trend > 0 else "↓" if t_trend < 0 else "→"
            print(f"    Temperature:            {direction} {abs(t_trend):.4f} °C/year")
        print()
    
    print(f"{'='*70}\n")

def main():
    """Command-line interface for climatology analysis."""
    parser = argparse.ArgumentParser(
        description='Calculate long-term climate normals (WMO 30-year standards)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Calculate 1991-2020 climatology (current WMO standard)
  python3 -m climate_tookit.climatology.long_term_climatology \\
    --location="-1.286,36.817" \\
    --start-year 1991 \\
    --end-year 2020 \\
    --source nasa_power
  
  # Calculate with JSON output
  python3 -m climate_tookit.climatology.long_term_climatology \\
    --location="-1.286,36.817" \\
    --start-year 1991 \\
    --end-year 2020 \\
    --source nasa_power \\
    --format json \\
    --output climatology_1991-2020.json

Standard WMO Periods:
  1961-1990  (older standard)
  1971-2000  (older standard)
  1981-2010  (previous standard)
  1991-2020  (current standard)
  
Note: Works with partial datasets (precipitation-only or temperature-only sources)
        """
    )
    
    parser.add_argument('--location', required=True, type=str,
                       help='Location as "lat,lon" (e.g., "-1.286,36.817")')
    parser.add_argument('--start-year', required=True, type=int,
                       help='Start year of climatology period')
    parser.add_argument('--end-year', required=True, type=int,
                       help='End year of climatology period (inclusive)')
    parser.add_argument('--source', required=True, type=str,
                       help='Data source (e.g., nasa_power, chirps, chirts)')
    parser.add_argument('--format', choices=['text', 'json'], default='text',
                       help='Output format (default: text)')
    parser.add_argument('--output', type=str,
                       help='Output file path (for JSON format)')
    
    args = parser.parse_args()
    
    # Parse location
    try:
        lat, lon = map(float, args.location.split(','))
    except ValueError:
        print("Error: Invalid location format. Use 'lat,lon' format.")
        sys.exit(1)
    
    # Validate years
    if args.end_year < args.start_year:
        print("Error: End year must be >= start year")
        sys.exit(1)
    
    n_years = args.end_year - args.start_year + 1
    if n_years < 10:
        print(f"Warning: {n_years} years may be insufficient for robust climatology")
        print("         WMO recommends 30-year periods for climate normals")
    
    # Calculate climatology
    result = calculate_climatology(
        location_coord=(lat, lon),
        start_year=args.start_year,
        end_year=args.end_year,
        source=args.source
    )
    
    # Output
    if args.format == 'json':
        output = json.dumps(result, indent=2, default=str)
        
        if args.output:
            with open(args.output, 'w') as f:
                f.write(output)
            print(f"\n✓ Climatology saved to {args.output}")
        else:
            print(output)
    else:
        print_climatology_report(result)
        
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(result, f, indent=2, default=str)
            print(f"✓ JSON data saved to {args.output}")

if __name__ == "__main__":
    main()
    
# Calculate 1991-2020 climatology (current WMO standard)
# python -m climate_tookit.climatology.long_term_climatology --location="-1.286,36.817" --start-year 1991 --end-year 2020 --source era_5

# With JSON output
# python -m climate_tookit.climatology.long_term_climatology --location="-1.286,36.817" --start-year 1991 --end-year 2020 --source nasa_power --format json --output climatology_1991-2020.json