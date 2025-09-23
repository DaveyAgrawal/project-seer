const fs = require('fs');
const { Pool } = require('pg');
const csv = require('csv-parser');

// Convert Web Mercator (EPSG:3857) to WGS84 (EPSG:4326)
function webMercatorToWGS84(x, y) {
  const lng = (x / 20037508.34) * 180;
  let lat = (y / 20037508.34) * 180;
  lat = 180 / Math.PI * (2 * Math.atan(Math.exp(lat * Math.PI / 180)) - Math.PI / 2);
  return { lat, lng };
}

async function importStanfordData() {
  console.log('🔄 Starting Stanford geothermal data import...');
  
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
    
    const csvFile = '/Users/austin/Downloads/stanford_thermal_model_inputs_outputs_COMPLETE_VERSION2.csv';
    
    console.log('📊 Processing CSV data...');
    
    return new Promise((resolve, reject) => {
      fs.createReadStream(csvFile)
        .pipe(csv())
        .on('data', async (row) => {
          totalRows++;
          
          const northing = parseFloat(row['Northing']);
          const easting = parseFloat(row['Easting']);   
          const depth = parseFloat(row['Depth']);       
          const tempCelsius = parseFloat(row['T']);     
          
          if (isNaN(northing) || isNaN(easting) || isNaN(depth) || isNaN(tempCelsius)) {
            return;
          }
          
          const coords = webMercatorToWGS84(easting, northing);
          const tempFahrenheit = (tempCelsius * 9/5) + 32;
          
          batch.push({
            lat: coords.lat,
            lng: coords.lng,
            depth_m: depth,
            temperature_c: tempCelsius,
            temperature_f: tempFahrenheit
          });
          
          if (batch.length >= batchSize) {
            try {
              const values = batch.map(item => 
                `(${item.lat}, ${item.lng}, ${item.depth_m}, ${item.temperature_c}, ${item.temperature_f}, ST_SetSRID(ST_MakePoint(${item.lng}, ${item.lat}), 4326))`
              ).join(',');
              
              const query = `
                INSERT INTO geothermal_points (latitude, longitude, depth_m, temperature_c, temperature_f, geom)
                VALUES ${values}
              `;
              
              await pool.query(query);
              insertedRows += batch.length;
              batch = [];
              
              if (insertedRows % 10000 === 0) {
                console.log(`📈 Processed ${insertedRows.toLocaleString()} rows...`);
              }
            } catch (error) {
              console.error('Error inserting batch:', error);
            }
          }
        })
        .on('end', async () => {
          // Insert remaining batch
          if (batch.length > 0) {
            try {
              const values = batch.map(item => 
                `(${item.lat}, ${item.lng}, ${item.depth_m}, ${item.temperature_c}, ${item.temperature_f}, ST_SetSRID(ST_MakePoint(${item.lng}, ${item.lat}), 4326))`
              ).join(',');
              
              const query = `
                INSERT INTO geothermal_points (latitude, longitude, depth_m, temperature_c, temperature_f, geom)
                VALUES ${values}
              `;
              
              await pool.query(query);
              insertedRows += batch.length;
            } catch (error) {
              console.error('Error inserting final batch:', error);
            }
          }
          
          console.log(`✅ Import complete!`);
          console.log(`📊 Total rows processed: ${totalRows.toLocaleString()}`);
          console.log(`📈 Total rows inserted: ${insertedRows.toLocaleString()}`);
          
          // Update table statistics
          console.log('📊 Analyzing table...');
          await pool.query('ANALYZE geothermal_points');
          
          // Verify data
          const result = await pool.query('SELECT COUNT(*) FROM geothermal_points WHERE depth_m = 3000');
          const count3000 = result.rows[0].count;
          console.log(`📋 Rows at 3000m depth: ${parseInt(count3000).toLocaleString()}`);
          
          const tempResult = await pool.query('SELECT MIN(temperature_c), MAX(temperature_c) FROM geothermal_points WHERE depth_m = 3000');
          const minTemp = tempResult.rows[0].min;
          const maxTemp = tempResult.rows[0].max;
          console.log(`🌡️ Temperature range at 3000m: ${parseFloat(minTemp).toFixed(1)}°C to ${parseFloat(maxTemp).toFixed(1)}°C`);
          
          await pool.end();
          resolve();
        })
        .on('error', (error) => {
          console.error('❌ CSV processing error:', error);
          reject(error);
        });
    });
    
  } catch (error) {
    console.error('❌ Import failed:', error);
    await pool.end();
    throw error;
  }
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