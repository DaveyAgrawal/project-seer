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
    const batchSize = 5000; // Larger batches for efficiency
    let batch = [];
    
    for await (const line of rl) {
      lineNumber++;
      
      if (lineNumber === 1) {
        // Parse headers
        headers = line.split(',');
        console.log('📋 Headers found:', headers.length, 'columns');
        continue;
      }
      
      // Processing all 4.2M+ rows for full national coverage
      
      // Parse CSV line
      const values = parseCSVLine(line);
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
      const easting = parseFloat(row.Easting);
      const northing = parseFloat(row.Northing);
      
      // Skip rows without essential data
      if (!geometry || !temperature || temperature <= 0 || !easting || !northing) {
        continue;
      }
      
      batch.push({ geometry, depth, temperature, easting, northing });
      
      // Process batch when full
      if (batch.length >= batchSize) {
        await processBatch(client, batch);
        processed += batch.length;
        batch = [];
        
        if (processed % 50000 === 0) {
          console.log(`📈 Processed ${processed.toLocaleString()} geothermal points... (${Math.round(processed/4279537*100)}%)`);
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
      console.log('Registry entry already exists');
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

function parseCSVLine(line) {
  const result = [];
  let current = '';
  let inQuotes = false;
  
  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    
    if (char === '"') {
      inQuotes = !inQuotes;
    } else if (char === ',' && !inQuotes) {
      result.push(current);
      current = '';
    } else {
      current += char;
    }
  }
  
  result.push(current);
  return result;
}

async function processBatch(client, batch) {
  for (const point of batch) {
    try {
      // Use PostGIS to transform from Web Mercator (3857) to WGS84 (4326)
      // Note: geom is auto-generated from lat/lng, so we don't insert it directly
      const result = await client.query(`
        SELECT 
          ST_Y(ST_Transform(ST_SetSRID(ST_MakePoint($1, $2), 3857), 4326)) as latitude,
          ST_X(ST_Transform(ST_SetSRID(ST_MakePoint($1, $2), 3857), 4326)) as longitude
      `, [point.easting, point.northing]);
      
      const { latitude, longitude } = result.rows[0];
      
      // Skip points outside US bounds
      if (latitude < 15 || latitude > 75 || longitude < -170 || longitude > -60) {
        return;
      }
      
      await client.query(`
        INSERT INTO geothermal_points (
          latitude, 
          longitude, 
          temperature_f, 
          depth_m
        ) VALUES ($1, $2, $3, $4)
      `, [
        latitude,
        longitude,
        point.temperature,
        point.depth
      ]);
    } catch (error) {
      // Skip problematic points
      continue;
    }
  }
}

// Run if called directly
if (require.main === module) {
  const filePath = process.argv[2];
  if (!filePath) {
    console.error('Usage: node simple-geothermal-v3.js <path-to-csv>');
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