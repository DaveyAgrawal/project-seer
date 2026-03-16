const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');

const pool = new Pool({
  host: 'localhost',
  port: 5434,
  database: 'geospatial',
  user: 'geouser',
  password: 'geopass'
});

async function loadParcels() {
  const processedDir = path.join(__dirname, 'downloads/energynet/processed');
  const files = fs.readdirSync(processedDir).filter(f => f.endsWith('.geojson'));
  
  console.log(`Found ${files.length} GeoJSON files to load`);
  
  let totalParcels = 0;
  
  for (const file of files) {
    const filePath = path.join(processedDir, file);
    const data = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
    const listingId = file.replace('_parcels.geojson', '');
    
    console.log(`Loading ${file}: ${data.features.length} parcels`);
    
    for (const feature of data.features) {
      const props = feature.properties;
      const geom = JSON.stringify(feature.geometry);
      
      try {
        await pool.query(`
          INSERT INTO energynet_parcels (listing_id, parcel_id, state, acres, description, geom, centroid, is_active)
          VALUES ($1, $2, $3, $4, $5, ST_SetSRID(ST_GeomFromGeoJSON($6), 4326), ST_Centroid(ST_SetSRID(ST_GeomFromGeoJSON($6), 4326)), true)
          ON CONFLICT DO NOTHING
        `, [
          props.listing_id || listingId,
          props.parcel_id || props.tract_id || 'unknown',
          props.state || 'Unknown',
          props.acres || null,
          props.description || 'Government Land Parcel',
          geom
        ]);
        totalParcels++;
      } catch (err) {
        console.error(`Error inserting parcel ${props.parcel_id}:`, err.message);
      }
    }
  }
  
  console.log(`\n✅ Loaded ${totalParcels} parcels total`);
  await pool.end();
}

loadParcels().catch(console.error);
