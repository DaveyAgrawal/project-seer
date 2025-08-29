const { Pool } = require('pg');

const pool = new Pool({
  host: 'localhost',
  port: 5434,
  database: 'geospatial',
  user: 'geouser',
  password: 'geopass'
});

async function checkDuplicateCoordinates() {
  try {
    console.log('Checking for duplicate coordinates...');
    
    // Check how many unique longitude values we have
    const uniqueLngs = await pool.query('SELECT COUNT(DISTINCT longitude) as unique_lngs FROM geothermal_points WHERE depth_m = 3000');
    console.log('Unique longitude values at 3000m:', uniqueLngs.rows[0].unique_lngs);
    
    // Check how many unique latitude values we have  
    const uniqueLats = await pool.query('SELECT COUNT(DISTINCT latitude) as unique_lats FROM geothermal_points WHERE depth_m = 3000');
    console.log('Unique latitude values at 3000m:', uniqueLats.rows[0].unique_lats);
    
    // Check total records at 3000m
    const totalCount = await pool.query('SELECT COUNT(*) as total FROM geothermal_points WHERE depth_m = 3000');
    console.log('Total records at 3000m:', totalCount.rows[0].total);
    
    // Check for the most common longitude values
    const commonLngs = await pool.query(`
      SELECT longitude, COUNT(*) as count 
      FROM geothermal_points 
      WHERE depth_m = 3000 
      GROUP BY longitude 
      ORDER BY count DESC 
      LIMIT 10
    `);
    console.log('\nMost common longitude values:');
    commonLngs.rows.forEach(row => {
      console.log(`  ${row.longitude}: ${row.count} occurrences`);
    });
    
    // Sample some coordinate ranges
    const sampleCoords = await pool.query(`
      SELECT longitude, latitude, COUNT(*) as count
      FROM geothermal_points 
      WHERE depth_m = 3000 
      GROUP BY longitude, latitude
      HAVING COUNT(*) > 1
      ORDER BY count DESC 
      LIMIT 5
    `);
    console.log('\nMost duplicated coordinate pairs:');
    sampleCoords.rows.forEach(row => {
      console.log(`  (${row.longitude}, ${row.latitude}): ${row.count} duplicates`);
    });
    
  } finally {
    await pool.end();
  }
}

checkDuplicateCoordinates().catch(console.error);