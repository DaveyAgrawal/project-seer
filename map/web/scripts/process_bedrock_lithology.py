#!/usr/bin/env python3
"""
Process USGS SGMC Geodatabase to extract bedrock lithology data.
Simplifies geometries and classifies rock types for map display.
"""

import fiona
import json
from shapely.geometry import shape, mapping
from shapely.ops import transform
from pyproj import Transformer
from collections import defaultdict
import sys

# Input/Output paths
GDB_PATH = '/Users/devanagrawal/Desktop/project-seer/DataCenterMap-Scraper/more.data/USGS_SGMC_Geodatabase/USGS_StateGeologicMapCompilation_ver1.1.gdb'
OUTPUT_PATH = '/Users/devanagrawal/Desktop/project-seer/map/web/public/data/bedrock_lithology.geojson'

# Lithology classification mapping
# Maps MAJOR1 values to simplified rock types
LITHOLOGY_MAP = {
    # Sedimentary
    'Shale': 'Shale',
    'Mudstone': 'Shale',
    'Claystone': 'Shale',
    'Siltstone': 'Shale',
    'Argillite': 'Shale',
    'Sandstone': 'Sandstone',
    'Quartzite': 'Sandstone',
    'Arenite': 'Sandstone',
    'Arkose': 'Sandstone',
    'Graywacke': 'Sandstone',
    'Greywacke': 'Sandstone',
    'Limestone': 'Limestone',
    'Chalk': 'Limestone',
    'Marble': 'Limestone',
    'Doloite': 'Dolostone',
    'Dolomite': 'Dolostone',
    'Dolostone': 'Dolostone',
    'Conglomerate': 'Conglomerate',
    'Breccia': 'Conglomerate',
    'Diamictite': 'Conglomerate',
    'Evaporite': 'Evaporite',
    'Gypite': 'Evaporite',
    'Halite': 'Evaporite',
    'Chert': 'Chert',
    'Coal': 'Coal',
    'Lignite': 'Coal',
    
    # Igneous - Volcanic
    'Basalt': 'Basalt',
    'Diabase': 'Basalt',
    'Gabbro': 'Basalt',
    'Andesite': 'Andesite',
    'Dacite': 'Andesite',
    'Diorite': 'Andesite',
    'Rhyolite': 'Rhyolite',
    'Trachyte': 'Rhyolite',
    'Felsite': 'Rhyolite',
    'Obsidian': 'Rhyolite',
    'Tuff': 'Volcanic',
    'Ash': 'Volcanic',
    'Pyroclastic': 'Volcanic',
    'Volcanic': 'Volcanic',
    'Lava': 'Volcanic',
    
    # Igneous - Intrusive
    'Granite': 'Granite',
    'Granodiorite': 'Granite',
    'Monzonite': 'Granite',
    'Syenite': 'Granite',
    'Tonalite': 'Granite',
    'Trondhjemite': 'Granite',
    'Pegmatite': 'Granite',
    'Aplite': 'Granite',
    'Quartz monzonite': 'Granite',
    'Intrusive': 'Intrusive',
    'Plutonic': 'Intrusive',
    'Porphyry': 'Intrusive',
    
    # Metamorphic
    'Gneiss': 'Gneiss',
    'Migmatite': 'Gneiss',
    'Granulite': 'Gneiss',
    'Schist': 'Schist',
    'Phyllite': 'Schist',
    'Slate': 'Schist',
    'Greenstone': 'Schist',
    'Amphibolite': 'Metamorphic',
    'Serpentinite': 'Metamorphic',
    'Hornfels': 'Metamorphic',
    'Mylonite': 'Metamorphic',
    'Metacite': 'Metamorphic',
    'Metite': 'Metamorphic',
    
    # Unconsolidated
    'Alluvium': 'Unconsolidated',
    'Colluvium': 'Unconsolidated',
    'Gravel': 'Unconsolidated',
    'Sand': 'Unconsolidated',
    'Silt': 'Unconsolidated',
    'Clay': 'Unconsolidated',
    'Till': 'Unconsolidated',
    'Glacial': 'Unconsolidated',
    'Loess': 'Unconsolidated',
    'Terrace': 'Unconsolidated',
    'Eolian': 'Unconsolidated',
    'Lacustrine': 'Unconsolidated',
    'Marine': 'Unconsolidated',
    'Fluvial': 'Unconsolidated',
    'Deltaic': 'Unconsolidated',
    'Beach': 'Unconsolidated',
    'Dune': 'Unconsolidated',
    'Residuum': 'Unconsolidated',
    'Regolith': 'Unconsolidated',
    'Soil': 'Unconsolidated',
    'Fill': 'Unconsolidated',
    'Artificial': 'Unconsolidated',
    'Water': 'Water',
    'Ice': 'Water',
}

# Rock category mapping
ROCK_CATEGORIES = {
    'Shale': 'Sedimentary',
    'Sandstone': 'Sedimentary',
    'Limestone': 'Sedimentary',
    'Dolostone': 'Sedimentary',
    'Conglomerate': 'Sedimentary',
    'Evaporite': 'Sedimentary',
    'Chert': 'Sedimentary',
    'Coal': 'Sedimentary',
    'Basalt': 'Igneous',
    'Andesite': 'Igneous',
    'Rhyolite': 'Igneous',
    'Volcanic': 'Igneous',
    'Granite': 'Igneous',
    'Intrusive': 'Igneous',
    'Gneiss': 'Metamorphic',
    'Schist': 'Metamorphic',
    'Metamorphic': 'Metamorphic',
    'Unconsolidated': 'Unconsolidated',
    'Water': 'Other',
    'Unknown': 'Unknown',
}

def classify_lithology(major1, generalized_lith):
    """Classify rock type from MAJOR1 and GENERALIZED_LITH fields."""
    if not major1:
        major1 = ''
    if not generalized_lith:
        generalized_lith = ''
    
    major1_clean = major1.strip()
    
    # Direct match
    if major1_clean in LITHOLOGY_MAP:
        return LITHOLOGY_MAP[major1_clean]
    
    # Partial match in MAJOR1
    for key, value in LITHOLOGY_MAP.items():
        if key.lower() in major1_clean.lower():
            return value
    
    # Check GENERALIZED_LITH
    gen_lower = generalized_lith.lower()
    if 'sedimentary' in gen_lower:
        if 'ite' in major1_clean.lower():
            return 'Sedimentary'
        return 'Sedimentary'
    elif 'igneous' in gen_lower:
        if 'volcanic' in gen_lower:
            return 'Volcanic'
        elif 'intrusive' in gen_lower:
            return 'Intrusive'
        return 'Igneous'
    elif 'metamorphic' in gen_lower:
        return 'Metamorphic'
    elif 'unconsolidated' in gen_lower or 'surficial' in gen_lower:
        return 'Unconsolidated'
    elif 'water' in gen_lower:
        return 'Water'
    
    return 'Unknown'

def main():
    print("=" * 60)
    print("BEDROCK LITHOLOGY PROCESSING")
    print("=" * 60)
    print(f"Input: {GDB_PATH}")
    print(f"Output: {OUTPUT_PATH}")
    
    # Set up coordinate transformer (ESRI:102039 to WGS84)
    transformer = Transformer.from_crs("ESRI:102039", "EPSG:4326", always_xy=True)
    
    def transform_coords(geom):
        return transform(transformer.transform, geom)
    
    features = []
    lithology_counts = defaultdict(int)
    category_counts = defaultdict(int)
    
    print("\n📍 Reading SGMC_Geology layer...")
    
    with fiona.open(GDB_PATH, layer='SGMC_Geology') as src:
        total = len(src)
        print(f"   Total features: {total:,}")
        
        for i, feat in enumerate(src):
            if i % 50000 == 0:
                print(f"   Processing {i:,}/{total:,} ({100*i/total:.1f}%)...")
            
            props = feat['properties']
            
            # Classify lithology
            rock_type = classify_lithology(props.get('MAJOR1', ''), props.get('GENERALIZED_LITH', ''))
            category = ROCK_CATEGORIES.get(rock_type, 'Unknown')
            
            lithology_counts[rock_type] += 1
            category_counts[category] += 1
            
            # Get geometry and transform to WGS84
            try:
                geom = shape(feat['geometry'])
                
                # Simplify geometry more aggressively (tolerance in meters, ~3000m)
                geom_simplified = geom.simplify(3000, preserve_topology=True)
                
                # Skip small polygons (< 25 km²)
                if geom_simplified.area < 25000000:
                    continue
                
                # Transform to WGS84
                geom_wgs84 = transform_coords(geom_simplified)
                
                # Create feature
                feature = {
                    'type': 'Feature',
                    'geometry': mapping(geom_wgs84),
                    'properties': {
                        'rock_type': rock_type,
                        'category': category,
                        'major1': props.get('MAJOR1', ''),
                        'major2': props.get('MAJOR2', ''),
                        'major3': props.get('MAJOR3', ''),
                        'minor1': props.get('MINOR1', ''),
                        'minor2': props.get('MINOR2', ''),
                        'generalized_lith': props.get('GENERALIZED_LITH', ''),
                        'unit_name': props.get('UNIT_NAME', ''),
                        'orig_label': props.get('ORIG_LABEL', ''),
                        'age_min': props.get('AGE_MIN', ''),
                        'age_max': props.get('AGE_MAX', ''),
                        'state': props.get('STATE', ''),
                    }
                }
                features.append(feature)
                
            except Exception as e:
                # Skip problematic geometries
                continue
    
    print(f"\n✅ Processed {len(features):,} features")
    
    print("\n📊 Lithology distribution:")
    for rock_type, count in sorted(lithology_counts.items(), key=lambda x: -x[1]):
        print(f"   {rock_type}: {count:,}")
    
    print("\n📊 Category distribution:")
    for category, count in sorted(category_counts.items(), key=lambda x: -x[1]):
        print(f"   {category}: {count:,}")
    
    # Write GeoJSON
    print(f"\n💾 Writing GeoJSON to {OUTPUT_PATH}...")
    geojson = {
        'type': 'FeatureCollection',
        'features': features
    }
    
    with open(OUTPUT_PATH, 'w') as f:
        json.dump(geojson, f)
    
    # Get file size
    import os
    size_mb = os.path.getsize(OUTPUT_PATH) / (1024 * 1024)
    print(f"   File size: {size_mb:.1f} MB")
    
    print("\n✅ DONE!")

if __name__ == '__main__':
    main()
