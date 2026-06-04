#!/usr/bin/env python3
"""
Run this script AFTER downloading thermal_model_inputs_outputs_Ver3.csv
to see what columns are available and verify the data format.

Usage: python3 inspect_csv.py
"""

import pandas as pd
import sys

csv_file = '/Users/devanagrawal/Desktop/project-seer/DataCenterMap-Scraper/more.data/stanford_thermal_model_inputs_outputs_COMPLETE_VERSION2.csv'

print(f"📂 Inspecting: {csv_file}")
print("=" * 60)

try:
    # Read just the first few rows to see columns
    df = pd.read_csv(csv_file, nrows=5)
    
    print("\n📋 COLUMNS FOUND:")
    for i, col in enumerate(df.columns):
        print(f"  {i+1}. {col}")
    
    print("\n📊 FIRST 3 ROWS:")
    print(df.head(3).to_string())
    
    print("\n📈 COLUMN TYPES:")
    print(df.dtypes)
    
    # Check for coordinate columns
    print("\n🗺️ LOOKING FOR COORDINATE COLUMNS:")
    coord_keywords = ['lat', 'lon', 'lng', 'north', 'east', 'x', 'y', 'coord']
    for col in df.columns:
        if any(kw in col.lower() for kw in coord_keywords):
            print(f"  ✓ Found: {col}")
    
    # Check for temperature columns
    print("\n🌡️ LOOKING FOR TEMPERATURE COLUMNS:")
    temp_keywords = ['temp', 't_', '_t', 'celsius', 'fahrenheit']
    for col in df.columns:
        if any(kw in col.lower() for kw in temp_keywords):
            print(f"  ✓ Found: {col}")
    
    # Check for depth columns
    print("\n📏 LOOKING FOR DEPTH COLUMNS:")
    depth_keywords = ['depth', 'z', 'km', 'meter']
    for col in df.columns:
        if any(kw in col.lower() for kw in depth_keywords):
            print(f"  ✓ Found: {col}")

except FileNotFoundError:
    print(f"❌ File not found: {csv_file}")
    print("\nPlease download thermal_model_inputs_outputs_Ver3.csv from:")
    print("https://gdr.openei.org/submissions/1592")
    print(f"\nThen save it to: {csv_file}")
    sys.exit(1)
except Exception as e:
    print(f"❌ Error: {e}")
    sys.exit(1)
