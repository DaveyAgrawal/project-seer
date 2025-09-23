#!/usr/bin/env node

const fs = require('fs');
const { Pool } = require('pg');
const csv = require('csv-parser');

// Database configuration (matching existing ingest scripts)
const pool = new Pool({
  host: 'localhost',
  port: 5434,
  database: 'geospatial',
  user: 'geouser',
  password: 'geopass'
});

// Convert Web Mercator (EPSG:3857) to WGS84 (EPSG:4326)  
function webMercatorToWGS84(x, y) {
  const lng = (x / 20037508.34) * 180;
  let lat = (y / 20037508.34) * 180;
  lat = 180 / Math.PI * (2 * Math.atan(Math.exp(lat * Math.PI / 180)) - Math.PI / 2);
  return { lat, lng };
}

async function ingestStanfordData(csvFilePath) {
  console.log('🚀 Starting Stanford geothermal data ingestion...');
  console.log('📁 File:', csvFilePath);
  
  const client = await pool.connect();
  
  try {
    console.log('✅ Connected to database');
    
    // Drop existing table and recreate with correct schema
    console.log('📋 Dropping existing geothermal_points table...');
    await client.query('DROP TABLE IF EXISTS geothermal_points CASCADE');
    
    console.log('🏗️ Creating new geothermal_points table...');
    await client.query(`
      CREATE TABLE geothermal_points (
        gid SERIAL PRIMARY KEY,
        latitude DOUBLE PRECISION NOT NULL,
        longitude DOUBLE PRECISION NOT NULL,
        depth_m DOUBLE PRECISION NOT NULL,
        temperature_c DOUBLE PRECISION NOT NULL,
        temperature_f DOUBLE PRECISION NOT NULL,
        geom GEOMETRY(POINT, 4326) NOT NULL
      )
    `);
    
    // Create indices for performance
    console.log('📍 Creating spatial and depth indices...');
    await client.query('CREATE INDEX geothermal_points_geom_idx ON geothermal_points USING GIST (geom)');
    await client.query('CREATE INDEX geothermal_points_depth_idx ON geothermal_points (depth_m)');
    await client.query('CREATE INDEX geothermal_points_temp_idx ON geothermal_points (temperature_c)');
    
    // Prepare batch insert
    let totalRows = 0;
    let insertedRows = 0;
    let skippedRows = 0;
    const batchSize = 1000;
    let batch = [];
    
    console.log('📊 Processing CSV data in batches...');
    
    const insertBatch = async (batchData) => {
      if (batchData.length === 0) return;
      
      const values = batchData.map(row => [
        row.lat, row.lng, row.depth_m, row.temperature_c, row.temperature_f
      ]);
      
      const query = `
        INSERT INTO geothermal_points (latitude, longitude, depth_m, temperature_c, temperature_f, geom)
        SELECT unnest($1::double precision[]), unnest($2::double precision[]), unnest($3::double precision[]), 
               unnest($4::double precision[]), unnest($5::double precision[]),
               ST_SetSRID(ST_MakePoint(unnest($2::double precision[]), unnest($1::double precision[])), 4326)
      `;
      
      const params = [
        values.map(v => v[0]), // lats
        values.map(v => v[1]), // lngs  
        values.map(v => v[2]), // depths
        values.map(v => v[3]), // temps_c
        values.map(v => v[4])  // temps_f
      ];
      
      await client.query(query, params);
      insertedRows += batchData.length;
    };
    
    return new Promise((resolve, reject) => {
      fs.createReadStream(csvFilePath)
        .pipe(csv())
        .on('data', async (row) => {
          totalRows++;
          
          // Extract required columns from Stanford dataset
          const northing = parseFloat(row['Northing']);
          const easting = parseFloat(row['Easting']);   
          const depth = parseFloat(row['Depth']);       
          const tempCelsius = parseFloat(row['T']);  // Column 82 - correct subsurface temperature
          
          // Skip invalid rows
          if (isNaN(northing) || isNaN(easting) || isNaN(depth) || isNaN(tempCelsius)) {
            skippedRows++;
            return;
          }
          
          // Convert coordinates from Web Mercator to WGS84
          const coords = webMercatorToWGS84(easting, northing);
          
          // Validate coordinate ranges
          if (coords.lat < -90 || coords.lat > 90 || coords.lng < -180 || coords.lng > 180) {
            skippedRows++;
            return;
          }
          
          // Convert Celsius to Fahrenheit
          const tempFahrenheit = (tempCelsius * 9/5) + 32;
          
          // Add to batch
          batch.push({
            lat: coords.lat,
            lng: coords.lng,
            depth_m: depth,
            temperature_c: tempCelsius,
            temperature_f: tempFahrenheit
          });
          
          // Process batch when full
          if (batch.length >= batchSize) {
            try {
              await insertBatch(batch);
              batch = [];
              
              // Progress update
              if (insertedRows % 50000 === 0) {
                console.log(`📈 Inserted ${insertedRows.toLocaleString()} rows (processed ${totalRows.toLocaleString()})...`);
              }
            } catch (error) {
              console.error('❌ Batch insert error:', error);
              reject(error);
              return;
            }
          }
        })
        .on('end', async () => {
          try {
            // Insert remaining batch
            if (batch.length > 0) {
              await insertBatch(batch);
            }
            
            console.log('📊 Analyzing table for query optimization...');
            await client.query('ANALYZE geothermal_points');
            
            // Final statistics
            console.log('✅ Stanford geothermal data ingestion complete!');
            console.log(`📊 Total rows processed: ${totalRows.toLocaleString()}`);
            console.log(`📈 Total rows inserted: ${insertedRows.toLocaleString()}`);
            console.log(`⚠️ Total rows skipped: ${skippedRows.toLocaleString()}`);
            
            // Verification queries
            const countResult = await client.query('SELECT COUNT(*) FROM geothermal_points');
            console.log(`🔢 Final database count: ${parseInt(countResult.rows[0].count).toLocaleString()}`);
            
            const depth3000Result = await client.query('SELECT COUNT(*) FROM geothermal_points WHERE depth_m = 3000');
            console.log(`🎯 Records at 3000m depth: ${parseInt(depth3000Result.rows[0].count).toLocaleString()}`);
            
            const tempRangeResult = await client.query(`
              SELECT MIN(temperature_c) as min_temp, MAX(temperature_c) as max_temp, AVG(temperature_c) as avg_temp 
              FROM geothermal_points WHERE depth_m = 3000
            `);
            const { min_temp, max_temp, avg_temp } = tempRangeResult.rows[0];
            console.log(`🌡️ Temperature range at 3000m: ${parseFloat(min_temp).toFixed(1)}°C to ${parseFloat(max_temp).toFixed(1)}°C (avg: ${parseFloat(avg_temp).toFixed(1)}°C)`);
            
            resolve();
          } catch (error) {
            console.error('❌ Final processing error:', error);
            reject(error);
          }
        })
        .on('error', (error) => {
          console.error('❌ CSV processing error:', error);
          reject(error);
        });
    });
    
  } catch (error) {
    console.error('❌ Database error:', error);
    throw error;
  } finally {
    client.release();
  }
}

async function main() {
  const csvFilePath = '/Users/austin/Downloads/stanford_thermal_model_inputs_outputs_COMPLETE_VERSION2.csv';
  
  if (!fs.existsSync(csvFilePath)) {
    console.error('❌ Stanford CSV file not found at:', csvFilePath);
    process.exit(1);
  }
  
  try {
    await ingestStanfordData(csvFilePath);
    console.log('🎉 Stanford geothermal data ingestion completed successfully!');
  } catch (error) {
    console.error('💥 Ingestion failed:', error);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

// Run the ingestion
if (require.main === module) {
  main();
}

module.exports = { ingestStanfordData };