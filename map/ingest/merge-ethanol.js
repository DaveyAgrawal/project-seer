const fs = require('fs');
const path = require('path');

// Read the CSV file with emissions data
const csvPath = '/Users/devanagrawal/Desktop/project-seer/DataCenterMap-Scraper/more.data/Ethanol Facilities (EPA Flight - emissions included) - Sheet1.csv';
const geojsonPath = '/Users/devanagrawal/Desktop/project-seer/DataCenterMap-Scraper/more.data/Ethanol Plants_20250824_024934_chunk0000.geojson';
const outputPath = '/Users/devanagrawal/Desktop/project-seer/DataCenterMap-Scraper/more.data/ethanol_plants_with_emissions.geojson';

// Parse CSV
const csvContent = fs.readFileSync(csvPath, 'utf-8');
const csvLines = csvContent.split('\n').slice(1); // Skip header

const emissionsData = [];
for (const line of csvLines) {
    if (!line.trim()) continue;
    
    // Parse CSV line (handle quoted fields with commas)
    const parts = [];
    let current = '';
    let inQuotes = false;
    
    for (const char of line) {
        if (char === '"') {
            inQuotes = !inQuotes;
        } else if (char === ',' && !inQuotes) {
            parts.push(current.trim());
            current = '';
        } else {
            current += char;
        }
    }
    parts.push(current.trim());
    
    const [facility, city, state, emissions] = parts;
    
    // Parse emissions (remove commas, handle ---)
    let emissionsValue = null;
    if (emissions && emissions !== '---' && emissions !== '') {
        emissionsValue = parseInt(emissions.replace(/,/g, ''), 10);
        if (isNaN(emissionsValue)) emissionsValue = null;
    }
    
    emissionsData.push({
        facility: facility || '',
        city: city || '',
        state: state || '',
        emissions: emissionsValue
    });
}

console.log(`Loaded ${emissionsData.length} facilities from CSV`);

// Read GeoJSON (newline-delimited JSON)
const geojsonContent = fs.readFileSync(geojsonPath, 'utf-8');
const geojsonLines = geojsonContent.split('\n').filter(l => l.trim());

const features = geojsonLines.map(line => JSON.parse(line));
console.log(`Loaded ${features.length} facilities from GeoJSON`);

// Normalize name for matching
function normalizeName(name) {
    if (!name) return '';
    return name
        .toUpperCase()
        .replace(/[^A-Z0-9\s]/g, '')  // Remove special chars
        .replace(/\s+/g, ' ')          // Normalize whitespace
        .replace(/\bLLC\b/g, '')
        .replace(/\bINC\b/g, '')
        .replace(/\bCORP\b/g, '')
        .replace(/\bCORPORATION\b/g, '')
        .replace(/\bCOMPANY\b/g, '')
        .replace(/\bCO\b/g, '')
        .replace(/\bLP\b/g, '')
        .replace(/\bLTD\b/g, '')
        .replace(/\bETHANOL\b/g, '')
        .replace(/\bENERGY\b/g, '')
        .replace(/\bBIOFUELS\b/g, '')
        .replace(/\bBIOENERGY\b/g, '')
        .replace(/\bRENEWABLE\b/g, '')
        .replace(/\bFUELS\b/g, '')
        .replace(/\bPLANT\b/g, '')
        .replace(/\bBIOREFINING\b/g, '')
        .replace(/\bBIOREFINERY\b/g, '')
        .replace(/\s+/g, ' ')
        .trim();
}

// Match facilities
let matched = 0;
let unmatched = 0;

for (const feature of features) {
    const props = feature.properties;
    const company = props.Company || '';
    const site = props.Site || '';
    const state = props.State || '';
    
    // Try to find a match in emissions data
    const normalizedCompany = normalizeName(company);
    const normalizedSite = normalizeName(site);
    
    let bestMatch = null;
    let bestScore = 0;
    
    for (const emData of emissionsData) {
        if (emData.emissions === null) continue; // Skip entries without emissions
        
        const normalizedFacility = normalizeName(emData.facility);
        const normalizedCity = normalizeName(emData.city);
        
        // Check state match first
        const stateMatch = state.toUpperCase().includes(emData.state.toUpperCase()) || 
                          emData.state.toUpperCase().includes(state.toUpperCase().substring(0, 2));
        
        if (!stateMatch) continue;
        
        // Calculate similarity score
        let score = 0;
        
        // Check if company name contains facility name or vice versa
        if (normalizedCompany.includes(normalizedFacility) || normalizedFacility.includes(normalizedCompany)) {
            score += 50;
        }
        
        // Check if site matches city
        if (normalizedSite.includes(normalizedCity) || normalizedCity.includes(normalizedSite)) {
            score += 30;
        }
        
        // Check for partial word matches
        const companyWords = normalizedCompany.split(' ').filter(w => w.length > 2);
        const facilityWords = normalizedFacility.split(' ').filter(w => w.length > 2);
        
        for (const cw of companyWords) {
            for (const fw of facilityWords) {
                if (cw === fw) score += 10;
                else if (cw.includes(fw) || fw.includes(cw)) score += 5;
            }
        }
        
        // Check site/city match
        const siteWords = normalizedSite.split(' ').filter(w => w.length > 2);
        const cityWords = normalizedCity.split(' ').filter(w => w.length > 2);
        
        for (const sw of siteWords) {
            for (const cw of cityWords) {
                if (sw === cw) score += 15;
            }
        }
        
        if (score > bestScore) {
            bestScore = score;
            bestMatch = emData;
        }
    }
    
    // Only accept matches with score >= 25
    if (bestMatch && bestScore >= 25) {
        props.emissions_mt_co2e = bestMatch.emissions;
        props.emissions_source = 'EPA FLIGHT';
        matched++;
        console.log(`✓ Matched: "${company}" (${site}) -> "${bestMatch.facility}" (${bestMatch.city}) [score: ${bestScore}] = ${bestMatch.emissions} MT CO2e`);
    } else {
        props.emissions_mt_co2e = null;
        props.emissions_source = null;
        unmatched++;
    }
}

console.log(`\n=== Summary ===`);
console.log(`Matched: ${matched}`);
console.log(`Unmatched: ${unmatched}`);

// Write output as proper GeoJSON FeatureCollection
const outputGeoJSON = {
    type: 'FeatureCollection',
    features: features
};

fs.writeFileSync(outputPath, JSON.stringify(outputGeoJSON, null, 2));
console.log(`\nWritten to: ${outputPath}`);
