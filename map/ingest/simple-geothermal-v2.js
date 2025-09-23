#!/usr/bin/env node

const fs = require('fs');
const { Pool } = require('pg');
const readline = require('readline');

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
  
  const client = await pool.connect();
  
  try {
    // Clear existing data
    await client.query('DELETE FROM geothermal_points');
    console.log('🧹 Cleared existing geothermal data');
    
    // Create file stream
    const fileStream = fs.createReadStream(filePath);
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity
    });
    
    let lineNumber = 0;
    let processed = 0;
    let headers = [];
    const batchSize = 5000;
    let batch = [];
    
    for await (const line of rl) {
      lineNumber++;
      
      if (lineNumber === 1) {
        // Parse headers
        headers = line.split(',');
        console.log('📋 Headers found:', headers.length, 'columns');
        continue;
      }
      
      // Parse CSV line
      const values = line.split(',');
      if (values.length !== headers.length) {
        continue; // Skip malformed lines
      }
      
      // Create row object
      const row = {};
      headers.forEach((header, index) => {
        row[header] = values[index];
      });
      
      // Extract key data
      const geometry = row.geometry;
      const depth = parseFloat(row.Depth) || 0;
      const temperature = parseFloat(row.BHT) || null;
      
      // Skip rows without essential data
      if (!geometry || !temperature || temperature <= 0) {
        continue;
      }
      
      batch.push({ geometry, depth, temperature, row });
      
      // Process batch when full
      if (batch.length >= batchSize) {
        await processBatch(client, batch);
        processed += batch.length;
        batch = [];
        
        if (processed % 50000 === 0) {
          console.log(`📈 Processed ${processed.toLocaleString()} geothermal points...`);
        }
      }
    }
    
    // Process final batch
    if (batch.length > 0) {
      await processBatch(client, batch);
      processed += batch.length;
    }
    
    // Create indexes
    console.log('🔧 Creating spatial indexes...');
    await client.query('CREATE INDEX IF NOT EXISTS idx_geothermal_points_geom ON geothermal_points USING GIST(geom)');
    await client.query('CREATE INDEX IF NOT EXISTS idx_geothermal_points_temp ON geothermal_points(temperature_f)');
    
    // Update dataset registry
    try {
      await client.query(`
        INSERT INTO dataset_registry (
          layer_name, 
          table_name, 
          geometry_type, 
          minzoom, 
          maxzoom,
          attributes,
          style
        ) VALUES (
          'geothermal_points',
          'geothermal_points', 
          'POINT',
          3,
          14,
          '[\"latitude\", \"longitude\", \"temperature_f\", \"depth_m\"]',
          '{"type": "circle", "paint": {"circle-color": "#FF5722", "circle-radius": 3, "circle-opacity": 0.7}}'
        );
      `);
    } catch (error) {
      // Registry entry might already exist
      console.log('Registry entry already exists or error:', error.message);
    }
    
    console.log('✅ Geothermal data ingestion completed successfully!');
    console.log(`📊 Total points processed: ${processed.toLocaleString()}`);
    
  } catch (error) {
    console.error('❌ Error during ingestion:', error);
    throw error;
  } finally {
    client.release();
    await pool.end();
  }
}

async function processBatch(client, batch) {
  const insertPromises = batch.map(async (point) => {
    try {
      // Simple lat/lng extraction from WKT POINT
      const match = point.geometry.match(/POINT \(([^)]+)\)/);
      if (!match) return null;
      
      const coords = match[1].split(' ');
      const easting = parseFloat(coords[0]);
      const northing = parseFloat(coords[1]);
      
      // Simple transformation from Web Mercator (3857) to WGS84 (4326)
      // This is an approximation - for production use proper transformation
      const longitude = easting / 111320;
      const latitude = Math.atan(Math.exp(northing / 6378137)) * 360 / Math.PI - 90;
      
      // Skip points outside reasonable bounds
      if (longitude < -180 || longitude > 180 || latitude < -90 || latitude > 90) {
        return null;
      }
      
      await client.query(`
        INSERT INTO geothermal_points (
          latitude, 
          longitude, 
          temperature_f, 
          depth_m,
          props,
          geom
        ) VALUES ($1, $2, $3, $4, $5, ST_SetSRID(ST_MakePoint($6, $7), 4326))
      `, [
        latitude,
        longitude,
        point.temperature,
        point.depth,
        JSON.stringify({ heat_flow: point.row.heat_flow }),
        longitude,
        latitude
      ]);
      
      return true;
    } catch (error) {
      // Skip problematic points
      return null;
    }
  });
  
  await Promise.all(insertPromises);
}

// Run if called directly
if (require.main === module) {
  const filePath = process.argv[2];
  if (!filePath) {
    console.error('Usage: node simple-geothermal-v2.js <path-to-csv>');
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