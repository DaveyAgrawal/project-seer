#!/usr/bin/env node

import { DatacenterScraper } from './datacenter-scraper';
import { DatacenterDatabase } from './datacenter-db';
import * as dotenv from 'dotenv';

// Load environment variables
dotenv.config();

interface CLIOptions {
  testMode?: boolean;
  testLimit?: number;
  statsOnly?: boolean;
}

async function parseArgs(): Promise<CLIOptions> {
  const args = process.argv.slice(2);
  const options: CLIOptions = {};

  for (let i = 0; i < args.length; i++) {
    switch (args[i]) {
      case '--test':
        options.testMode = true;
        // Get next arg as limit if it's a number
        if (args[i + 1] && !isNaN(parseInt(args[i + 1]))) {
          options.testLimit = parseInt(args[i + 1]);
          i++;
        } else {
          options.testLimit = 5; // Default test limit
        }
        break;
      case '--stats':
        options.statsOnly = true;
        break;
      case '--help':
        printHelp();
        process.exit(0);
        break;
    }
  }

  return options;
}

function printHelp(): void {
  console.log(`
Datacenter Scraper - CLI Tool

Usage: npm run scrape:datacenters [options]

Options:
  --test [N]    Run in test mode, scraping only first N facilities (default: 5)
  --stats       Display database statistics only (no scraping)
  --help        Show this help message

Examples:
  npm run scrape:datacenters              # Scrape all 8,000+ datacenters
  npm run scrape:datacenters -- --test    # Test mode: scrape first 5 facilities
  npm run scrape:datacenters -- --test 10 # Test mode: scrape first 10 facilities
  npm run scrape:datacenters -- --stats   # Show statistics only
  `);
}

async function showStats(): Promise<void> {
  console.log('\n📊 Datacenter Database Statistics\n');

  const db = new DatacenterDatabase();
  await db.initialize();

  try {
    const stats = await db.getDatacenterStats();

    console.log('Overall Statistics:');
    console.log('─'.repeat(50));
    console.log(`  Total Datacenters:       ${stats.total_datacenters}`);
    console.log(`  US Datacenters:          ${stats.us_datacenters}`);
    console.log(`  International:           ${stats.international_datacenters}`);
    console.log(`  Countries Covered:       ${stats.countries_covered}`);
    console.log(`  States Covered:          ${stats.states_covered}`);
    console.log(`  Unique Operators:        ${stats.unique_operators}`);
    console.log(`  Total Power Capacity:    ${stats.total_power_capacity_mw.toFixed(2)} MW`);
    console.log(`  Average Power Capacity:  ${stats.avg_power_capacity_mw.toFixed(2)} MW`);
    console.log(`  Geocoded Count:          ${stats.geocoded_count}`);
    console.log(`  Low Quality Records:     ${stats.low_quality_count}`);

    console.log('\n\nTop Countries:');
    console.log('─'.repeat(50));
    const countryStats = await db.getCountryStats();
    countryStats.slice(0, 10).forEach(country => {
      console.log(`  ${country.country.padEnd(25)} ${country.datacenter_count} datacenters`);
    });

    console.log('\n\nTop Operators:');
    console.log('─'.repeat(50));
    const operatorStats = await db.getOperatorStats();
    operatorStats.slice(0, 10).forEach(operator => {
      console.log(`  ${operator.operator.padEnd(25)} ${operator.facility_count} facilities`);
    });

  } catch (error) {
    console.error('❌ Error fetching statistics:', error);
  } finally {
    await db.close();
  }
}

async function runScraper(options: CLIOptions): Promise<void> {
  const scraper = new DatacenterScraper(true);

  try {
    console.log('🚀 Starting Datacenter Scraper...\n');

    if (options.testMode) {
      console.log(`🧪 Running in TEST MODE (limit: ${options.testLimit} facilities)\n`);
    } else {
      console.log('⚠️  Running in PRODUCTION MODE (will scrape all 8,000+ datacenters)');
      console.log('⏱️  This will take several hours to complete.\n');

      // Confirm before proceeding
      console.log('Press Ctrl+C to cancel, or wait 5 seconds to proceed...');
      await new Promise(resolve => setTimeout(resolve, 5000));
      console.log('▶️  Starting scrape...\n');
    }

    // Initialize scraper
    await scraper.initialize();

    // Set up progress callback
    scraper.setProgressCallback((status, data) => {
      if (data?.message) {
        console.log(`[${status.toUpperCase()}] ${data.message}`);
      }
    });

    if (options.testMode) {
      // Test mode: Discover first page and scrape limited facilities
      console.log('🔍 Discovering facilities (first page only)...\n');

      const facilityUrls = await scraper.discoverAllDatacenters();
      const limitedUrls = facilityUrls.slice(0, options.testLimit);

      console.log(`\n📋 Found ${facilityUrls.length} total facilities`);
      console.log(`🧪 Testing with first ${limitedUrls.length} facilities\n`);

      let processed = 0;
      let failed = 0;

      for (const [index, url] of limitedUrls.entries()) {
        try {
          console.log(`\n[${index + 1}/${limitedUrls.length}] Scraping: ${url}`);

          const datacenter = await scraper.scrapeDatacenterDetail(url);

          if (datacenter) {
            console.log(`  ✓ Name: ${datacenter.name}`);
            console.log(`  ✓ Country: ${datacenter.country}`);
            console.log(`  ✓ Operator: ${datacenter.operator || 'N/A'}`);
            console.log(`  ✓ Power: ${datacenter.powerCapacityMw || 'N/A'} MW`);
            console.log(`  ✓ Coordinates: ${datacenter.latitude || 'N/A'}, ${datacenter.longitude || 'N/A'}`);

            // Store in database
            const db = new DatacenterDatabase();
            await db.initialize();
            await db.storeDatacenter(datacenter);
            await db.close();

            processed++;
          } else {
            console.log(`  ✗ Failed to extract data`);
            failed++;
          }

          // Respectful delay
          await new Promise(resolve => setTimeout(resolve, 2000));

        } catch (error) {
          console.error(`  ✗ Error: ${error.message}`);
          failed++;
        }
      }

      console.log('\n\n🎉 Test scrape complete!');
      console.log(`✅ Processed: ${processed}`);
      console.log(`❌ Failed: ${failed}`);

    } else {
      // Production mode: Full scrape
      const stats = await scraper.scrapeAll();

      console.log('\n\n🎉 Full scrape complete!');
      console.log('─'.repeat(50));
      console.log(`📊 Statistics:`);
      console.log(`  Discovered:     ${stats.discovered}`);
      console.log(`  Processed:      ${stats.processed}`);
      console.log(`  Failed:         ${stats.failed}`);
      console.log(`  Skipped:        ${stats.skipped}`);
      console.log(`  US Total:       ${stats.totalUs}`);
      console.log(`  International:  ${stats.totalInternational}`);
    }

  } catch (error) {
    console.error('\n❌ Fatal error:', error);
    process.exit(1);
  } finally {
    await scraper.close();
  }
}

async function main(): Promise<void> {
  const options = await parseArgs();

  console.log('🏢 Datacenter Scraper - datacenters.com');
  console.log('═'.repeat(50));

  if (options.statsOnly) {
    await showStats();
  } else {
    await runScraper(options);
  }

  console.log('\n✅ Done!\n');
  process.exit(0);
}

// Handle graceful shutdown
process.on('SIGINT', () => {
  console.log('\n\n⚠️  Interrupted by user. Shutting down gracefully...');
  process.exit(0);
});

process.on('SIGTERM', () => {
  console.log('\n\n⚠️  Terminated. Shutting down gracefully...');
  process.exit(0);
});

// Run main function
main().catch(error => {
  console.error('❌ Unhandled error:', error);
  process.exit(1);
});
