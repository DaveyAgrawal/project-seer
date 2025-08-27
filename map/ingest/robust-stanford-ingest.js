#!/usr/bin/env node

const fs = require('fs');
const { Pool } = require('pg');
const csv = require('csv-parser');

// Database configuration with connection pooling optimized for large imports
const pool = new Pool({
  host: 'localhost',
  port: 5434,
  database: 'geospatial',
  user: 'geouser',
  password: 'geopass',
  max: 10,
  idleTimeoutMillis: 60000,
  connectionTimeoutMillis: 10000,
  keepAlive: true,
  keepAliveInitialDelayMillis: 10000,
});

// Convert Web Mercator (EPSG:3857) to WGS84 (EPSG:4326)  
function webMercatorToWGS84(x, y) {
  const lng = (x / 20037508.34) * 180;
  let lat = (y / 20037508.34) * 180;
  lat = 180 / Math.PI * (2 * Math.atan(Math.exp(lat * Math.PI / 180)) - Math.PI / 2);
  return { lat, lng };
}

async function setupDatabase() {
  console.log('🏗️ Setting up database schema...');
  const client = await pool.connect();
  
  try {
    // Drop existing table and recreate
    console.log('📋 Dropping existing geothermal_points table...');
    await client.query('DROP TABLE IF EXISTS geothermal_points CASCADE');
    
    console.log('🔨 Creating new geothermal_points table...');
    await client.query(`
      CREATE TABLE geothermal_points (
        gid SERIAL PRIMARY KEY,
        latitude DOUBLE PRECISION NOT NULL,
        longitude DOUBLE PRECISION NOT NULL,
        depth_m DOUBLE PRECISION NOT NULL,
        temperature_c DOUBLE PRECISION NOT NULL,
        temperature_f DOUBLE PRECISION NOT NULL,
        geom GEOMETRY(POINT, 4326)
      )
    `);
    
    console.log('✅ Database schema ready');
  } finally {
    client.release();
  }
}

async function insertBatch(batch, batchNumber) {
  const client = await pool.connect();
  
  try {
    const values = batch.map(row => 
      `(${row.lat}, ${row.lng}, ${row.depth_m}, ${row.temperature_c}, ${row.temperature_f}, ST_SetSRID(ST_MakePoint(${row.lng}, ${row.lat}), 4326))`
    ).join(',');
    
    const query = `
      INSERT INTO geothermal_points (latitude, longitude, depth_m, temperature_c, temperature_f, geom)
      VALUES ${values}
    `;
    
    await client.query(query);
    return batch.length;
    
  } catch (error) {
    console.error(`❌ Batch ${batchNumber} failed:`, error.message);
    throw error;
  } finally {
    client.release();
  }
}

async function createIndices() {
  console.log('📍 Creating indices for optimal performance...');
  const client = await pool.connect();
  
  try {
    await client.query('CREATE INDEX CONCURRENTLY geothermal_points_geom_idx ON geothermal_points USING GIST (geom)');
    await client.query('CREATE INDEX CONCURRENTLY geothermal_points_depth_idx ON geothermal_points (depth_m)');
    await client.query('CREATE INDEX CONCURRENTLY geothermal_points_temp_idx ON geothermal_points (temperature_c)');
    console.log('✅ Indices created');
  } finally {
    client.release();
  }
}

async function ingestStanfordData(csvFilePath) {
  console.log('🚀 Starting Stanford geothermal data ingestion...');
  console.log('📁 File:', csvFilePath);
  
  // Setup database schema
  await setupDatabase();
  
  let totalRows = 0;
  let insertedRows = 0;
  let skippedRows = 0;
  const batchSize = 500; // Smaller batches for stability
  let batch = [];
  let batchNumber = 0;
  
  console.log('📊 Processing CSV data with robust error handling...');
  
  return new Promise((resolve, reject) => {
    fs.createReadStream(csvFilePath)
      .pipe(csv())
      .on('data', async (row) => {
        totalRows++;
        
        try {
          // Extract required columns from Stanford dataset
          const northing = parseFloat(row['Northing']);
          const easting = parseFloat(row['Easting']);   
          const depth = parseFloat(row['Depth']);       
          const tempCelsius = parseFloat(row['T']);
          
          // Skip invalid rows
          if (isNaN(northing) || isNaN(easting) || isNaN(depth) || isNaN(tempCelsius)) {
            skippedRows++;
            return;
          }
          
          // Convert coordinates
          const coords = webMercatorToWGS84(easting, northing);
          
          // Validate coordinate ranges
          if (coords.lat < -90 || coords.lat > 90 || coords.lng < -180 || coords.lng > 180) {
            skippedRows++;
            return;
          }
          
          // Convert temperature
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
            batchNumber++;
            
            try {
              const inserted = await insertBatch([...batch], batchNumber);
              insertedRows += inserted;
              batch = [];
              
              // Progress update
              if (batchNumber % 100 === 0) {
                console.log(`📈 Batch ${batchNumber}: ${insertedRows.toLocaleString()} rows inserted (${totalRows.toLocaleString()} processed)`);
              }
            } catch (error) {
              console.error(`❌ Failed to insert batch ${batchNumber}:`, error.message);
              // Continue with next batch instead of failing completely
              batch = [];
            }
          }
        } catch (error) {
          console.error('❌ Row processing error:', error);
          skippedRows++;
        }
      })
      .on('end', async () => {
        try {
          // Insert final batch
          if (batch.length > 0) {
            batchNumber++;
            try {
              const inserted = await insertBatch(batch, batchNumber);
              insertedRows += inserted;
            } catch (error) {
              console.error(`❌ Final batch ${batchNumber} failed:`, error.message);
            }
          }
          
          // Create indices
          await createIndices();
          
          // Analyze table
          console.log('📊 Analyzing table...');
          const client = await pool.connect();
          try {
            await client.query('ANALYZE geothermal_points');
          } finally {
            client.release();
          }
          
          // Final statistics
          console.log('✅ Stanford geothermal data ingestion complete!');
          console.log(`📊 Total rows processed: ${totalRows.toLocaleString()}`);
          console.log(`📈 Total rows inserted: ${insertedRows.toLocaleString()}`);
          console.log(`⚠️ Total rows skipped: ${skippedRows.toLocaleString()}`);
          
          // Verification
          const verifyClient = await pool.connect();
          try {
            const countResult = await verifyClient.query('SELECT COUNT(*) FROM geothermal_points');
            console.log(`🔢 Database count verification: ${parseInt(countResult.rows[0].count).toLocaleString()}`);
            
            const depth3000Result = await verifyClient.query('SELECT COUNT(*) FROM geothermal_points WHERE depth_m = 3000');
            console.log(`🎯 Records at 3000m depth: ${parseInt(depth3000Result.rows[0].count).toLocaleString()}`);
            
            const tempRangeResult = await verifyClient.query(`
              SELECT MIN(temperature_c) as min_temp, MAX(temperature_c) as max_temp, AVG(temperature_c) as avg_temp 
              FROM geothermal_points WHERE depth_m = 3000
            `);
            
            if (tempRangeResult.rows.length > 0) {
              const { min_temp, max_temp, avg_temp } = tempRangeResult.rows[0];
              console.log(`🌡️ Temperature range at 3000m: ${parseFloat(min_temp).toFixed(1)}°C to ${parseFloat(max_temp).toFixed(1)}°C (avg: ${parseFloat(avg_temp).toFixed(1)}°C)`);
            }
          } finally {
            verifyClient.release();
          }
          
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
}

async function main() {
  const csvFilePath = '/Users/austin/Downloads/stanford_thermal_model_inputs_outputs_COMPLETE_VERSION2.csv';
  
  if (!fs.existsSync(csvFilePath)) {
    console.error('❌ Stanford CSV file not found at:', csvFilePath);
    process.exit(1);
  }
  
  try {
    // Test connection first
    const testClient = await pool.connect();
    console.log('✅ Database connection verified');
    testClient.release();
    
    await ingestStanfordData(csvFilePath);
    console.log('🎉 Stanford geothermal data ingestion completed successfully!');
  } catch (error) {
    console.error('💥 Ingestion failed:', error);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

// Handle graceful shutdown
process.on('SIGINT', async () => {
  console.log('\n⏹️ Graceful shutdown requested...');
  await pool.end();
  process.exit(0);
});

// Run the ingestion
if (require.main === module) {
  main();
}