#!/usr/bin/env python3

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import numpy as np
from tqdm import tqdm

def web_mercator_to_wgs84(x, y):
    """Convert Web Mercator (EPSG:3857) to WGS84 (EPSG:4326)"""
    lng = (x / 20037508.34) * 180
    lat = (y / 20037508.34) * 180
    lat = 180 / np.pi * (2 * np.arctan(np.exp(lat * np.pi / 180)) - np.pi / 2)
    return lat, lng

def import_stanford_data():
    print("🔄 Starting Stanford geothermal data import...")
    
    # Database connection - try connecting as current user first
    try:
        conn = psycopg2.connect(
            host='localhost',
            port=5432,
            database='geospatial',
            user='austin'  # Current system user
        )
    except:
        # Fallback to default credentials
        try:
            conn = psycopg2.connect(
                host='localhost',
                port=5432,
                database='geospatial',
                user='geouser',
                password='geopass'
            )
        except:
            print("❌ Could not connect to database")
            return

    cur = conn.cursor()
    
    try:
        # Drop existing table and create new one
        print("📋 Dropping existing geothermal_points table...")
        cur.execute("DROP TABLE IF EXISTS geothermal_points")
        
        print("🏗️ Creating new geothermal_points table...")
        cur.execute("""
            CREATE TABLE geothermal_points (
                gid SERIAL PRIMARY KEY,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                depth_m DOUBLE PRECISION,
                temperature_c DOUBLE PRECISION,
                temperature_f DOUBLE PRECISION,
                geom GEOMETRY(POINT, 4326)
            )
        """)
        
        # Create indices
        print("📍 Creating spatial index...")
        cur.execute("CREATE INDEX geothermal_points_geom_idx ON geothermal_points USING GIST (geom)")
        cur.execute("CREATE INDEX geothermal_points_depth_idx ON geothermal_points (depth_m)")
        
        conn.commit()
        
        # Read and process CSV in chunks
        csv_file = '/Users/austin/Downloads/stanford_thermal_model_inputs_outputs_COMPLETE_VERSION2.csv'
        chunk_size = 10000
        
        print("📊 Processing CSV data...")
        total_inserted = 0
        
        # Read CSV in chunks to handle large file
        for chunk in tqdm(pd.read_csv(csv_file, chunksize=chunk_size), desc="Processing chunks"):
            # Extract required columns
            data = chunk[['Northing', 'Easting', 'Depth', 'T']].copy()
            
            # Remove invalid rows
            data = data.dropna()
            
            if len(data) == 0:
                continue
                
            # Convert coordinates
            coords = data[['Easting', 'Northing']].apply(
                lambda row: web_mercator_to_wgs84(row['Easting'], row['Northing']), 
                axis=1
            )
            
            # Prepare data for insertion
            insert_data = []
            for i, row in data.iterrows():
                lat, lng = coords.iloc[i - data.index[0]]  # Account for chunk indexing
                temp_c = row['T']
                temp_f = (temp_c * 9/5) + 32
                depth = row['Depth']
                
                # Skip invalid coordinates or temperatures
                if not (-90 <= lat <= 90 and -180 <= lng <= 180):
                    continue
                if np.isnan(temp_c) or np.isnan(depth):
                    continue
                    
                insert_data.append((lat, lng, depth, temp_c, temp_f))
            
            # Batch insert
            if insert_data:
                execute_values(
                    cur,
                    """INSERT INTO geothermal_points 
                       (latitude, longitude, depth_m, temperature_c, temperature_f, geom) 
                       VALUES %s""",
                    [(lat, lng, depth, temp_c, temp_f, f"ST_SetSRID(ST_MakePoint({lng}, {lat}), 4326)")
                     for lat, lng, depth, temp_c, temp_f in insert_data],
                    template="(%s, %s, %s, %s, %s, %s)"
                )
                
                total_inserted += len(insert_data)
                conn.commit()
                
                # Progress update
                if total_inserted % 50000 == 0:
                    print(f"📈 Processed {total_inserted:,} rows...")
        
        # Update geometry column properly
        print("🔧 Updating geometry column...")
        cur.execute("""
            UPDATE geothermal_points 
            SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
        """)
        
        # Analyze table
        print("📊 Analyzing table...")
        cur.execute("ANALYZE geothermal_points")
        
        conn.commit()
        
        print(f"✅ Import complete! Total rows inserted: {total_inserted:,}")
        
        # Verify data
        cur.execute("SELECT COUNT(*) FROM geothermal_points WHERE depth_m = 3000")
        count_3000m = cur.fetchone()[0]
        print(f"📋 Rows at 3000m depth: {count_3000m:,}")
        
        cur.execute("SELECT MIN(temperature_c), MAX(temperature_c) FROM geothermal_points WHERE depth_m = 3000")
        min_temp, max_temp = cur.fetchone()
        print(f"🌡️ Temperature range at 3000m: {min_temp:.1f}°C to {max_temp:.1f}°C")
        
    except Exception as e:
        print(f"❌ Import failed: {e}")
        conn.rollback()
        
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    import_stanford_data()