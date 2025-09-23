const fs = require('fs');
const { Pool } = require('pg');
const csv = require('csv-parser');

// Use the working server's database connection approach
const pool = new Pool({
  host: 'localhost',
  port: 5434,
  database: 'geospatial',
  user: 'geouser',
  password: 'geopass',
  max: 5,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
});

// Convert Web Mercator (EPSG:3857) to WGS84 (EPSG:4326)
function webMercatorToWGS84(x, y) {
  const lng = (x / 20037508.34) * 180;
  let lat = (y / 20037508.34) * 180;
  lat = 180 / Math.PI * (2 * Math.atan(Math.exp(lat * Math.PI / 180)) - Math.PI / 2);
  return { lat, lng };
}

async function importStanfordData() {
  const csvFile = '/Users/austin/Downloads/stanford_thermal_model_inputs_outputs_COMPLETE_VERSION2.csv';
  
  console.log('🔄 Starting Stanford geothermal data import...');
  
  try {
    // Test connection
    await pool.query('SELECT 1');
    console.log('✅ Database connected');
    
    // Drop existing table and create new one
    console.log('📋 Dropping existing geothermal_points table...');
    await pool.query('DROP TABLE IF EXISTS geothermal_points');
    
    console.log('🏗️ Creating new geothermal_points table...');
    await pool.query(`
      CREATE TABLE geothermal_points (
        gid SERIAL PRIMARY KEY,
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        depth_m DOUBLE PRECISION,
        temperature_c DOUBLE PRECISION,
        temperature_f DOUBLE PRECISION,
        geom GEOMETRY(POINT, 4326)
      )
    `);
    
    // Create spatial index
    console.log('📍 Creating spatial index...');
    await pool.query('CREATE INDEX geothermal_points_geom_idx ON geothermal_points USING GIST (geom)');
    await pool.query('CREATE INDEX geothermal_points_depth_idx ON geothermal_points (depth_m)');
    
    let totalRows = 0;
    let insertedRows = 0;
    const batchSize = 1000;
    let batch = [];
    
    console.log('📊 Processing CSV data...');
    
    return new Promise((resolve, reject) => {
      fs.createReadStream(csvFile)
        .pipe(csv())
        .on('data', async (row) => {
          totalRows++;
          
          // Extract the required columns (0-indexed)
          const northing = parseFloat(row['Northing']); // Column 1
          const easting = parseFloat(row['Easting']);   // Column 2  
          const depth = parseFloat(row['Depth']);       // Column 49
          const tempCelsius = parseFloat(row['T']);     // Column 82
          
          // Skip invalid rows
          if (isNaN(northing) || isNaN(easting) || isNaN(depth) || isNaN(tempCelsius)) {
            return;
          }
          
          // Convert coordinates from Web Mercator to WGS84
          const coords = webMercatorToWGS84(easting, northing);
          
          // Convert Celsius to Fahrenheit for consistency with existing API
          const tempFahrenheit = (tempCelsius * 9/5) + 32;
          
          // Add to batch
          batch.push({
            lat: coords.lat,
            lng: coords.lng,
            depth_m: depth,
            temperature_c: tempCelsius,
            temperature_f: tempFahrenheit
          });
          
          // Insert batch when full
          if (batch.length >= batchSize) {
            await insertBatch(batch);
            insertedRows += batch.length;
            batch = [];
            
            // Progress update
            if (insertedRows % 10000 === 0) {
              console.log(`📈 Processed ${insertedRows.toLocaleString()} rows...`);
            }
          }
        })
        .on('end', async () => {
          // Insert remaining batch
          if (batch.length > 0) {
            await insertBatch(batch);
            insertedRows += batch.length;
          }
          
          console.log(`✅ Import complete!`);
          console.log(`📊 Total rows processed: ${totalRows.toLocaleString()}`);
          console.log(`📈 Total rows inserted: ${insertedRows.toLocaleString()}`);
          
          // Update table statistics
          console.log('📊 Analyzing table...');
          await pool.query('ANALYZE geothermal_points');
          
          await pool.end();
          resolve();
        })
        .on('error', reject);
    });
    
  } catch (error) {
    console.error('❌ Import failed:', error);
    await pool.end();
    throw error;
  }
}

async function insertBatch(batch) {
  const values = batch.map(row => 
    `(${row.lat}, ${row.lng}, ${row.depth_m}, ${row.temperature_c}, ${row.temperature_f}, ST_SetSRID(ST_MakePoint(${row.lng}, ${row.lat}), 4326))`
  ).join(',');
  
  const query = `
    INSERT INTO geothermal_points (latitude, longitude, depth_m, temperature_c, temperature_f, geom)
    VALUES ${values}
  `;
  
  await pool.query(query);
}

// Run the import
if (require.main === module) {
  importStanfordData()
    .then(() => {
      console.log('🎉 Stanford data import completed successfully!');
      process.exit(0);
    })
    .catch(error => {
      console.error('💥 Import failed:', error);
      process.exit(1);
    });
}

module.exports = { importStanfordData };