const XLSX = require('xlsx');
const { Pool } = require('pg');
const path = require('path');

const pool = new Pool({
  host: 'localhost',
  port: 5434,
  database: 'geospatial',
  user: 'geouser',
  password: 'geopass'
});

async function loadCCUS() {
  const filePath = '/Users/devanagrawal/Desktop/project-seer/DataCenterMap-Scraper/more.data/CATF_CCUS_Database.xlsx';
  
  console.log('Reading CCUS Excel file...');
  const workbook = XLSX.readFile(filePath);
  
  // Get the "US" sheet (third tab)
  const sheetNames = workbook.SheetNames;
  console.log('Available sheets:', sheetNames);
  
  const usSheet = workbook.Sheets['US'];
  if (!usSheet) {
    console.error('US sheet not found!');
    return;
  }
  
  // Convert to JSON
  const data = XLSX.utils.sheet_to_json(usSheet);
  console.log(`Found ${data.length} CCUS sites in US sheet`);
  
  if (data.length > 0) {
    console.log('Sample row keys:', Object.keys(data[0]));
    console.log('Sample row:', data[0]);
  }
  
  // Create table
  console.log('\nCreating ccus_sites table...');
  await pool.query(`
    DROP TABLE IF EXISTS ccus_sites CASCADE;
    CREATE TABLE ccus_sites (
      id SERIAL PRIMARY KEY,
      project_name TEXT,
      entities TEXT,
      capture_storage_details TEXT,
      country TEXT,
      location TEXT,
      state TEXT,
      sector_classification TEXT,
      sector_description TEXT,
      subsector_classification TEXT,
      subsector_description TEXT,
      latitude NUMERIC,
      longitude NUMERIC,
      visualized_capacity NUMERIC,
      capacity NUMERIC,
      storage_classification TEXT,
      storage_description TEXT,
      year_announced INTEGER,
      year_operational INTEGER,
      status TEXT,
      notes TEXT,
      month_announced TEXT,
      reference TEXT,
      geom geometry(Point, 4326),
      created_at TIMESTAMPTZ DEFAULT NOW()
    );
    
    CREATE INDEX idx_ccus_sites_geom ON ccus_sites USING GIST(geom);
    CREATE INDEX idx_ccus_sites_status ON ccus_sites(status);
  `);
  
  console.log('Table created. Inserting data...');
  
  let inserted = 0;
  let skipped = 0;
  
  for (const row of data) {
    // Map Excel columns to database columns - check various possible column names
    let lat = row['Approx. Latitude'] || row['Approx Latitude'] || row['Latitude'];
    let lng = row['Approx. Longitude'] || row['Approx Longitude'] || row['Longitude'];
    
    // Convert to number
    lat = typeof lat === 'number' ? lat : parseFloat(lat);
    lng = typeof lng === 'number' ? lng : parseFloat(lng);
    
    if (!lat || !lng || isNaN(lat) || isNaN(lng)) {
      skipped++;
      continue;
    }
    
    // Parse capacity values
    let vizCap = row[' Visualized Capacity (Metric Tons Per Annum) '] || row['Visualized Capacity (Metric Tons Per Annum)'];
    let cap = row[' Capacity (Metric Tons Per Annum) '] || row['Capacity (Metric Tons Per Annum)'];
    vizCap = typeof vizCap === 'number' ? vizCap : (parseFloat(vizCap) || null);
    cap = typeof cap === 'number' ? cap : (parseFloat(cap) || null);
    
    // Parse year values
    let yearAnn = row['Year Announced'];
    let yearOp = row['Year Operational'];
    yearAnn = typeof yearAnn === 'number' ? yearAnn : (parseInt(yearAnn) || null);
    yearOp = typeof yearOp === 'number' ? yearOp : (parseInt(yearOp) || null);
    
    try {
      await pool.query(`
        INSERT INTO ccus_sites (
          project_name, entities, capture_storage_details, country, location, state,
          sector_classification, sector_description, subsector_classification, subsector_description,
          latitude, longitude, visualized_capacity, capacity,
          storage_classification, storage_description, year_announced, year_operational,
          status, notes, month_announced, reference, geom
        ) VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 
          $11::numeric, $12::numeric, $13::numeric, $14::numeric, 
          $15, $16, $17::integer, $18::integer, $19, $20, $21, $22,
          ST_SetSRID(ST_MakePoint($12::numeric, $11::numeric), 4326)
        )
      `, [
        row['Project Name'] || null,
        row['Entities'] || null,
        row['Capture or Storage Details'] || null,
        row['Country'] || 'USA',
        row['Location'] || null,
        row['State'] || null,
        row['Sector Classification'] || null,
        row['Sector Description'] || null,
        row['Subsector Classification'] || null,
        row['Subsector Description'] || null,
        lat,
        lng,
        vizCap,
        cap,
        row['Storage Classification'] || null,
        row['Storage Description'] || null,
        yearAnn,
        yearOp,
        row['Status'] || null,
        row['Notes'] || null,
        row['Month Announced'] ? String(row['Month Announced']) : null,
        row['Reference'] || null
      ]);
      inserted++;
    } catch (err) {
      console.error('Error inserting row:', row['Project Name'], err.message);
      skipped++;
    }
  }
  
  console.log(`\n✅ Loaded ${inserted} CCUS sites (${skipped} skipped due to missing coordinates)`);
  
  // Verify
  const result = await pool.query('SELECT COUNT(*) FROM ccus_sites');
  console.log(`Total in database: ${result.rows[0].count}`);
  
  await pool.end();
}

loadCCUS().catch(console.error);
