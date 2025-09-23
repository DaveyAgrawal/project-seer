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

async function debugGeothermalData(filePath) {
  console.log('🔍 Debugging geothermal data...');
  
  const fileStream = fs.createReadStream(filePath);
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity
  });
  
  let lineNumber = 0;
  let headers = [];
  let validCount = 0;
  let invalidCount = 0;
  let debugSamples = [];
  
  for await (const line of rl) {
    lineNumber++;
    
    if (lineNumber === 1) {
      headers = line.split(',');
      console.log('📋 Headers found:', headers.length, 'columns');
      continue;
    }
    
    if (lineNumber > 1000) break; // Only check first 1000 rows
    
    const values = parseCSVLine(line);
    if (values.length !== headers.length) {
      continue;
    }
    
    const row = {};
    headers.forEach((header, index) => {
      row[header] = values[index];
    });
    
    const geometry = row.geometry;
    const depth = parseFloat(row.Depth) || 0;
    const temperature = parseFloat(row.BHT) || null;
    const easting = parseFloat(row.Easting);
    const northing = parseFloat(row.Northing);
    
    console.log(`Row ${lineNumber}: geometry=${geometry ? 'OK' : 'MISSING'}, temp=${temperature}, easting=${easting}, northing=${northing}`);
    
    // Check filtering logic
    if (!geometry || !temperature || temperature <= 0 || !easting || !northing) {
      invalidCount++;
      console.log(`❌ Row ${lineNumber} filtered out: geometry=${!!geometry}, temp=${temperature}, easting=${easting}, northing=${northing}`);
    } else {
      validCount++;
      if (debugSamples.length < 5) {
        debugSamples.push({ lineNumber, geometry, temperature, easting, northing });
      }
    }
  }
  
  console.log(`\n📊 Summary:`);
  console.log(`Valid rows: ${validCount}`);
  console.log(`Invalid rows: ${invalidCount}`);
  console.log(`\n🔍 Valid samples:`, debugSamples);
  
  await pool.end();
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

// Run if called directly
if (require.main === module) {
  const filePath = process.argv[2];
  if (!filePath) {
    console.error('Usage: node debug-geothermal.js <path-to-csv>');
    process.exit(1);
  }
  
  debugGeothermalData(filePath)
    .then(() => {
      console.log('🎉 Debug completed!');
      process.exit(0);
    })
    .catch(error => {
      console.error('💥 Debug failed:', error);
      process.exit(1);
    });
}