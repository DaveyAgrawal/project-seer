#!/usr/bin/env python3
"""
Convert Class VI primacy CSV to JSON lookup by state abbreviation.
"""

import csv
import json
import os

# Paths
CSV_PATH = '/Users/devanagrawal/Desktop/project-seer/DataCenterMap-Scraper/more.data/class_vi_primacy_status.csv'
OUTPUT_PATH = '/Users/devanagrawal/Desktop/project-seer/map/web/public/data/class_vi_primacy.json'


def main():
    print("Reading CSV...")
    lookup = {}
    
    with open(CSV_PATH, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            state_abbr = row['state_abbr'].strip()
            lookup[state_abbr] = {
                'state': row['state'].strip(),
                'primacy_status': row['primacy_status'].strip(),
                'primacy_score': int(row['primacy_score']),
                'note': row.get('note', '').strip()
            }
    
    print(f"  Loaded {len(lookup)} states")

    output = {
        'metadata': {
            'total_states': len(lookup),
            'source': 'EPA Class VI Primacy Status',
            'primacy_scores': {
                2: 'Full primacy (state regulates Class VI wells)',
                1: 'Application pending',
                0: 'Federal regulation (EPA regulates)'
            }
        },
        'data': lookup
    }

    print(f"Saving to {OUTPUT_PATH}...")
    with open(OUTPUT_PATH, 'w') as f:
        json.dump(output, f, indent=2)

    size_kb = os.path.getsize(OUTPUT_PATH) / 1024
    print(f"Done! File size: {size_kb:.2f} KB")


if __name__ == '__main__':
    main()
