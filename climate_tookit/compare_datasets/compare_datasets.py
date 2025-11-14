"""
Dataset Comparison Module
"""

import sys
import os
from datetime import date
import pandas as pd
import numpy as np
import json
import argparse

# Set up project path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
sys.path.insert(0, project_root)

# Import preprocessing function
from climate_tookit.fetch_data.preprocess_data.preprocess_data import preprocess_data

def fetch_source(source, lat, lon, start, end):
    """
    Fetch preprocessed data from one source and display basic info.
    """
    try:
        df = preprocess_data(
            source=source,
            location_coord=(lat, lon),
            date_from=date.fromisoformat(start),
            date_to=date.fromisoformat(end)
        )
        
        print(f"\n{source}:")
        print(f"  Shape: {df.shape}")
        # df.info()
        print(df['date'].min())
        print(df['date'].max())
        
        return {'source': source, 'data': df, 'success': True}
        
    except Exception as e:
        print(f"\n{source}: Failed - {str(e)}")
        return {'source': source, 'success': False, 'error': str(e)}

def compare_sources(sources, lat, lon, start, end):
    """
    Fetch data from multiple sources and report basic info.
    """
    results = []
    
    for source in sources:
        result = fetch_source(source, lat, lon, start, end)
        results.append(result)
    
    return {'results': results}

def print_report(data):
    """
    Print dataset fetch report.
    """
    print("\n" + "=" * 60)
    print("DATASET FETCH REPORT")
    print("=" * 60)
    
    print("\nSOURCES")
    print("-" * 40)
    for r in data['results']:
        if r['success']:
            df = r['data']
            print(f"{r['source']:15s} {df.shape[0]:4d} records")
        else:
            print(f"{r['source']:15s} FAILED")

def main():
    parser = argparse.ArgumentParser(description='Fetch climate datasets')
    parser.add_argument('--sources', required=True, nargs='+')
    parser.add_argument('--lat', required=True, type=float)
    parser.add_argument('--lon', required=True, type=float)
    parser.add_argument('--start', required=True)
    parser.add_argument('--end', required=True)
    parser.add_argument('--format', choices=['json', 'report'], default='report')
    
    args = parser.parse_args()
    
    print(f"Fetching data from {len(args.sources)} sources...")
    
    result = compare_sources(args.sources, args.lat, args.lon, args.start, args.end)
    
    if args.format == 'report':
        print_report(result)
    else:
        print(json.dumps(result, indent=2, default=str))

if __name__ == "__main__":
    main()

    
# python -m climate_tookit.compare_datasets.compare_datasets --sources era_5 terraclimate chirps --lat -1.286 --lon 36.817 --start 2012-01-01 --end 2012-12-31 --format report