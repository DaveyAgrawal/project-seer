const XLSX = require('xlsx');
const { Pool } = require('pg');

const pool = new Pool({
  host: 'localhost',
  port: 5434,
  database: 'geospatial',
  user: 'geouser',
  password: 'geopass'
});

async function loadEmitters() {
  const filePath = '/Users/devanagrawal/Desktop/project-seer/DataCenterMap-Scraper/more.data/ghgp_data_by_year_2023.xlsx';
  
  console.log('Reading Point Source Emitters Excel file...');
  const workbook = XLSX.readFile(filePath);
  
  // Get sheet names
  const sheetNames = workbook.SheetNames;
  console.log('Available sheets:', sheetNames);
  
  // Get the "Direct Point Emitters" sheet (first tab)
  const sheet = workbook.Sheets['Direct Point Emitters'];
  if (!sheet) {
    console.error('Sheet not found!');
    return;
  }
  
  // Convert to JSON - skip first 3 rows (header info), row 4 has column names
  const data = XLSX.utils.sheet_to_json(sheet, { range: 3 });
  console.log(`Found ${data.length} point source emitters`);
  
  if (data.length > 0) {
    console.log('Sample row keys:', Object.keys(data[0]));
    console.log('Sample row:', data[0]);
  }
  
  // Create table
  console.log('\nCreating point_source_emitters table...');
  await pool.query(`
    DROP TABLE IF EXISTS point_source_emitters CASCADE;
    CREATE TABLE point_source_emitters (
      id SERIAL PRIMARY KEY,
      facility_id TEXT,
      frs_id TEXT,
      facility_name TEXT,
      city TEXT,
      state TEXT,
      zip_code TEXT,
      address TEXT,
      county TEXT,
      latitude NUMERIC,
      longitude NUMERIC,
      primary_naics_code TEXT,
      industry_type_subparts TEXT,
      industry_type_sectors TEXT,
      total_emissions_2023 NUMERIC,
      geom geometry(Point, 4326),
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
    
    CREATE INDEX idx_emitters_geom ON point_source_emitters USING GIST(geom);
    CREATE INDEX idx_emitters_industry ON point_source_emitters(industry_type_sectors);
    CREATE INDEX idx_emitters_emissions ON point_source_emitters(total_emissions_2023);
  `);
  
  console.log('Table created. Inserting data...');
  
  let inserted = 0;
  let skipped = 0;
  
  for (const row of data) {
    let lat = row['Latitude'];
    let lng = row['Longitude'];
    
    // Convert to number
    lat = typeof lat === 'number' ? lat : parseFloat(lat);
    lng = typeof lng === 'number' ? lng : parseFloat(lng);
    
    if (!lat || !lng || isNaN(lat) || isNaN(lng)) {
      skipped++;
      continue;
    }
    
    // Parse emissions
    let emissions = row['2023 Total reported direct emissions'];
    emissions = typeof emissions === 'number' ? emissions : (parseFloat(emissions) || null);
    
    try {
      await pool.query(`
        INSERT INTO point_source_emitters (
          facility_id, frs_id, facility_name, city, state, zip_code, address, county,
          latitude, longitude, primary_naics_code, industry_type_subparts, industry_type_sectors,
          total_emissions_2023, geom
        ) VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8,
          $9::numeric, $10::numeric, $11, $12, $13,
          $14::numeric, ST_SetSRID(ST_MakePoint($10::numeric, $9::numeric), 4326)
        )
      `, [
        row['Facility Id'] ? String(row['Facility Id']) : null,
        row['FRS Id'] ? String(row['FRS Id']) : null,
        row['Facility Name'] || null,
        row['City'] || null,
        row['State'] || null,
        row['Zip Code'] ? String(row['Zip Code']) : null,
        row['Address'] || null,
        row['County'] || null,
        lat,
        lng,
        row['Primary NAICS Code'] ? String(row['Primary NAICS Code']) : null,
        row['Latest Reported Industry Type (subparts)'] || null,
        row['Latest Reported Industry Type (sectors)'] || null,
        emissions
      ]);
      inserted++;
    } catch (err) {
      console.error('Error inserting row:', row['Facility Name'], err.message);
      skipped++;
    }
  }
  
  console.log(`\n✅ Loaded ${inserted} point source emitters (${skipped} skipped)`);
  
  // Get industry type categories
  const categories = await pool.query(`
    SELECT industry_type_sectors, COUNT(*) as count 
    FROM point_source_emitters 
    WHERE industry_type_sectors IS NOT NULL
    GROUP BY industry_type_sectors 
    ORDER BY count DESC
  `);
  console.log('\nIndustry Type Categories:');
  categories.rows.forEach(r => console.log(`  - ${r.industry_type_sectors}: ${r.count}`));
  
  // Get emissions range
  const stats = await pool.query(`
    SELECT 
      MIN(total_emissions_2023) as min_emissions,
      MAX(total_emissions_2023) as max_emissions,
      AVG(total_emissions_2023) as avg_emissions
    FROM point_source_emitters
  `);
  console.log('\nEmissions Stats:');
  console.log(`  Min: ${stats.rows[0].min_emissions}`);
  console.log(`  Max: ${stats.rows[0].max_emissions}`);
  console.log(`  Avg: ${Math.round(stats.rows[0].avg_emissions)}`);
  
  await pool.end();
}

loadEmitters().catch(console.error);
