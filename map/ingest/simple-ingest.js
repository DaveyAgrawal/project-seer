#!/usr/bin/env node

const fs = require('fs');
const { Pool } = require('pg');

// Database configuration
const pool = new Pool({
  host: 'localhost',
  port: 5434,
  database: 'geospatial',
  user: 'geouser',
  password: 'geopass'
});

async function ingestTransmissionLines(filePath) {
  console.log('🚀 Starting transmission lines ingestion...');
  
  try {
    // Read and parse the GeoJSON file
    const rawData = fs.readFileSync(filePath, 'utf8');
    const geojsonData = JSON.parse(rawData);
    
    console.log(`📊 Found ${geojsonData.features.length} transmission line features`);
    
    // Connect to database
    const client = await pool.connect();
    
    try {
      // Clear existing data
      await client.query('DELETE FROM transmission_lines');
      console.log('🧹 Cleared existing transmission lines data');
      
      // Process features in batches
      const batchSize = 1000;
      let processed = 0;
      
      for (let i = 0; i < geojsonData.features.length; i += batchSize) {
        const batch = geojsonData.features.slice(i, i + batchSize);
        
        // Insert batch
        for (const feature of batch) {
          // Convert LineString to MultiLineString if needed
          let geometry = feature.geometry;
          if (geometry.type === 'LineString') {
            geometry = {
              type: 'MultiLineString',
              coordinates: [geometry.coordinates]
            };
          }
          
          // Extract properties
          const props = feature.properties || {};
          
          // Insert into database
          await client.query(`
            INSERT INTO transmission_lines (
              id_text, 
              owner, 
              status, 
              kv, 
              volt_class,
              props,
              geom
            ) VALUES ($1, $2, $3, $4, $5, $6, ST_SetSRID(ST_GeomFromGeoJSON($7), 4326))
          `, [
            props.id || props.objectid?.toString() || `line_${i}`,
            props.owner || 'Unknown',
            props.status || 'Unknown',
            props.voltage || null,
            props.volt_class || 'Unknown',
            JSON.stringify(props),
            JSON.stringify(geometry)
          ]);
        }
        
        processed += batch.length;
        console.log(`📈 Processed ${processed}/${geojsonData.features.length} features (${Math.round(processed/geojsonData.features.length*100)}%)`);
      }
      
      // Create indexes
      console.log('🔧 Creating spatial indexes...');
      await client.query('CREATE INDEX IF NOT EXISTS idx_transmission_lines_geom ON transmission_lines USING GIST(geom)');
      await client.query('CREATE INDEX IF NOT EXISTS idx_transmission_lines_kv ON transmission_lines(kv)');
      
      // Update dataset registry
      await client.query(`
        INSERT INTO dataset_registry (
          layer_name, 
          table_name, 
          geometry_type, 
          minzoom, 
          maxzoom,
          attributes,
          style
        ) VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (table_name) DO UPDATE SET
          layer_name = EXCLUDED.layer_name,
          geometry_type = EXCLUDED.geometry_type,
          attributes = EXCLUDED.attributes,
          style = EXCLUDED.style
      `, [
        'transmission_lines',
        'transmission_lines', 
        'MULTILINESTRING',
        3,
        14,
        JSON.stringify(['id_text', 'owner', 'status', 'kv', 'volt_class']),
        JSON.stringify({
          type: 'line',
          paint: {
            'line-color': ['case', ['==', ['get', 'kv'], null], '#999999', '#2196F3'],
            'line-width': 2,
            'line-opacity': 0.8
          }
        })
      ]);
      
      console.log('✅ Transmission lines ingestion completed successfully!');
      console.log(`📊 Total features processed: ${processed}`);
      
    } finally {
      client.release();
    }
    
  } catch (error) {
    console.error('❌ Error during ingestion:', error);
    throw error;
  } finally {
    await pool.end();
  }
}

// Run if called directly
if (require.main === module) {
  const filePath = process.argv[2];
  if (!filePath) {
    console.error('Usage: node simple-ingest.js <path-to-geojson>');
    process.exit(1);
  }
  
  ingestTransmissionLines(filePath)
    .then(() => {
      console.log('🎉 Ingestion completed!');
      process.exit(0);
    })
    .catch(error => {
      console.error('💥 Ingestion failed:', error);
      process.exit(1);
    });
}