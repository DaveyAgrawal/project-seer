#!/usr/bin/env node

const fs = require('fs');
const { Pool } = require('pg');
const csv = require('csv-parser');

// Database configuration
const pool = new Pool({
  host: 'localhost',
  port: 5434,
  database: 'geospatial',
  user: 'geouser',
  password: 'geopass'
});

async function ingestGeothermalData(filePath) {
  console.log('🚀 Starting geothermal data ingestion...');
  console.log('📁 File:', filePath);
  
  try {
    // Connect to database
    const client = await pool.connect();
    
    try {
      // Clear existing data
      await client.query('DELETE FROM geothermal_points');
      console.log('🧹 Cleared existing geothermal data');
      
      // Process CSV in batches
      const batchSize = 10000;
      let processed = 0;
      let batch = [];
      
      return new Promise((resolve, reject) => {
        fs.createReadStream(filePath)
          .pipe(csv())
          .on('data', async (row) => {
            try {
              // Extract data from CSV row
              const geometry = row.geometry; // WKT POINT format
              const depth = parseFloat(row.Depth) || 0;
              const temperature = parseFloat(row.BHT) || null;
              const northing = parseFloat(row.Northing) || null;
              const easting = parseFloat(row.Easting) || null;
              
              // Skip rows without essential data
              if (!geometry || !temperature || temperature <= 0) {
                return;
              }
              
              // Add to batch
              batch.push({
                geometry,
                depth,
                temperature,
                northing,
                easting,
                props: {
                  elevation: parseFloat(row.Elevation) || 0,
                  heat_flow: parseFloat(row.heat_flow) || null,
                  thermal_conductivity: parseFloat(row.thermal_conductivity) || null
                }
              });
              
              // Process batch when full
              if (batch.length >= batchSize) {
                await processBatch(client, batch);
                processed += batch.length;
                batch = [];
                
                console.log(`📈 Processed ${processed.toLocaleString()} geothermal points...`);
              }
            } catch (error) {
              console.error('Error processing row:', error);
            }
          })
          .on('end', async () => {
            try {
              // Process final batch
              if (batch.length > 0) {
                await processBatch(client, batch);
                processed += batch.length;
              }
              
              // Create indexes
              console.log('🔧 Creating spatial indexes...');
              await client.query('CREATE INDEX IF NOT EXISTS idx_geothermal_points_geom ON geothermal_points USING GIST(geom)');
              await client.query('CREATE INDEX IF NOT EXISTS idx_geothermal_points_temp ON geothermal_points(temperature_f)');
              await client.query('CREATE INDEX IF NOT EXISTS idx_geothermal_points_depth ON geothermal_points(depth_m)');
              
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
                'geothermal_points',
                'geothermal_points', 
                'POINT',
                3,
                14,
                JSON.stringify(['latitude', 'longitude', 'temperature_f', 'depth_m']),
                JSON.stringify({
                  type: 'circle',
                  paint: {
                    'circle-color': ['case', ['==', ['get', 'temperature_f'], null], '#999999', '#FF5722'],
                    'circle-radius': 4,
                    'circle-opacity': 0.7
                  }
                })
              ]);
              
              console.log('✅ Geothermal data ingestion completed successfully!');
              console.log(`📊 Total points processed: ${processed.toLocaleString()}`);
              
              resolve();
            } catch (error) {
              reject(error);
            }
          })
          .on('error', reject);
      });
      
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

async function processBatch(client, batch) {
  for (const point of batch) {
    try {
      // Convert WKT to lat/lng for easier handling
      // The geometry is in projected coordinates, we need to transform it
      await client.query(`
        INSERT INTO geothermal_points (
          latitude, 
          longitude, 
          temperature_f, 
          depth_m,
          props,
          geom
        ) VALUES (
          ST_Y(ST_Transform(ST_GeomFromText($1, 3857), 4326)),
          ST_X(ST_Transform(ST_GeomFromText($1, 3857), 4326)),
          $2, 
          $3, 
          $4,
          ST_Transform(ST_GeomFromText($1, 3857), 4326)
        )
      `, [
        point.geometry,
        point.temperature,
        point.depth,
        JSON.stringify(point.props)
      ]);
    } catch (error) {
      // Skip invalid geometries
      console.warn('Skipping invalid geometry:', error.message);
    }
  }
}

// Run if called directly
if (require.main === module) {
  const filePath = process.argv[2];
  if (!filePath) {
    console.error('Usage: node simple-geothermal-ingest.js <path-to-csv>');
    process.exit(1);
  }
  
  ingestGeothermalData(filePath)
    .then(() => {
      console.log('🎉 Geothermal ingestion completed!');
      process.exit(0);
    })
    .catch(error => {
      console.error('💥 Geothermal ingestion failed:', error);
      process.exit(1);
    });
}