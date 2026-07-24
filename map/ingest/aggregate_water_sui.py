#!/usr/bin/env python3
"""
Aggregate water budget SUI data to mean SUI per HUC12.
Output: JSON lookup { huc12_id: { sui: float, sui_class: str } }
"""

import pandas as pd
import json
import os

# Paths
PARQUET_PATH = '/Users/devanagrawal/Desktop/project-seer/DataCenterMap-Scraper/more.data/water_budget_sui_ensemble.parquet'
OUTPUT_PATH = '/Users/devanagrawal/Desktop/project-seer/map/web/public/data/water_sui_by_huc12.json'


def get_sui_class(sui):
    """Convert SUI value to categorical class."""
    if sui is None or pd.isna(sui):
        return 'Unknown'
    if sui < 0.1:
        return 'Very low/none'
    if sui < 0.2:
        return 'Low'
    if sui < 0.4:
        return 'Moderate'
    if sui < 0.7:
        return 'High'
    return 'Severe'


def main():
    print("Reading parquet file...")
    df = pd.read_parquet(PARQUET_PATH, columns=['huc', 'SUI'])
    print(f"  Loaded {len(df):,} rows")

    print("Aggregating mean SUI per HUC12...")
    agg = df.groupby('huc')['SUI'].mean().reset_index()
    agg.columns = ['huc12', 'sui']
    print(f"  {len(agg):,} unique HUC12s")

    print("Building lookup dictionary...")
    lookup = {}
    for _, row in agg.iterrows():
        huc12 = str(row['huc12']).zfill(12)  # Ensure 12-digit string
        sui = round(row['sui'], 4)
        lookup[huc12] = {
            'sui': sui,
            'sui_class': get_sui_class(sui)
        }

    output = {
        'metadata': {
            'total_huc12s': len(lookup),
            'source': 'USGS Water Budget SUI Ensemble',
            'sui_classes': {
                'Very low/none': 'SUI < 0.1',
                'Low': '0.1 <= SUI < 0.2',
                'Moderate': '0.2 <= SUI < 0.4',
                'High': '0.4 <= SUI < 0.7',
                'Severe': 'SUI >= 0.7'
            }
        },
        'data': lookup
    }

    print(f"Saving to {OUTPUT_PATH}...")
    with open(OUTPUT_PATH, 'w') as f:
        json.dump(output, f)

    size_mb = os.path.getsize(OUTPUT_PATH) / (1024 * 1024)
    print(f"Done! File size: {size_mb:.2f} MB")


if __name__ == '__main__':
    main()
