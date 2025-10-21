#!/usr/bin/env ts-node

import { MultiListingScraper } from './multi-listing-scraper';
import { MultiListingDatabase } from './multi-listing-db';

// CLI interface for the multi-listing scraper
async function main() {
    const args = process.argv.slice(2);
    const command = args[0];

    switch (command) {
        case 'scrape':
            await runFullScraper();
            break;
        case 'test':
            const saleGroup = args[1];
            if (!saleGroup) {
                console.error('❌ Usage: npm run scrape test <sale_group>');
                process.exit(1);
            }
            await testSingleListing(saleGroup);
            break;
        case 'stats':
            await showStats();
            break;
        case 'init':
            await initializeDatabase();
            break;
        default:
            showHelp();
            process.exit(1);
    }
}

async function runFullScraper(): Promise<void> {
    console.log('🚀 Starting Multi-Listing EnergyNet Scraper...');
    console.log('📅 Target: All 12 active government land auction listings\n');
    
    const scraper = new MultiListingScraper();
    
    try {
        await scraper.initialize();
        
        const stats = await scraper.scrapeAllListings();
        
        console.log('\n🎉 Multi-Listing Scraping Complete!');
        console.log('=' .repeat(50));
        console.log(`📋 Discovered Listings: ${stats.discovered}`);
        console.log(`✅ Successfully Processed: ${stats.processed}`);
        console.log(`⏭️  Skipped (Already Exist): ${stats.skipped}`);
        console.log(`❌ Failed: ${stats.failed}`);
        console.log(`🆕 New Listings Added: ${stats.newListings}`);
        console.log(`📍 Total Land Parcels: ${stats.totalParcels}`);
        console.log(`🔄 Expired Listings: ${stats.expiredListings}`);
        
        if (stats.totalParcels > 0) {
            console.log('\n🗺️  Map Update: Land parcels are now available on the map!');
            console.log('   Navigate to the western states to see the new bright pink parcels.');
        }
        
        await scraper.close();
        
    } catch (error) {
        console.error('❌ Critical scraper error:', error);
        await scraper.close();
        process.exit(1);
    }
}

async function testSingleListing(saleGroup: string): Promise<void> {
    console.log(`🧪 Testing Single Listing: ${saleGroup}`);
    console.log('-'.repeat(40));
    
    const scraper = new MultiListingScraper();
    
    try {
        await scraper.initialize();
        await scraper.testSingleListing(saleGroup);
        await scraper.close();
        
        console.log('✅ Single listing test completed');
        
    } catch (error) {
        console.error('❌ Test failed:', error);
        await scraper.close();
        process.exit(1);
    }
}

async function showStats(): Promise<void> {
    console.log('📊 Multi-Listing Database Statistics');
    console.log('=' .repeat(50));
    
    const db = new MultiListingDatabase();
    
    try {
        await db.initialize();
        
        // Overall stats
        const overallStats = await db.getMultiListingStats();
        console.log('\n🌍 Overall Statistics:');
        console.log(`   Total Sale Groups: ${overallStats.total_sale_groups}`);
        console.log(`   Total Listings: ${overallStats.total_listings}`);
        console.log(`   Regions Covered: ${overallStats.regions_covered}`);
        console.log(`   Total Parcels: ${overallStats.total_parcels}`);
        console.log(`   Total Acres: ${overallStats.total_acres ? Number(overallStats.total_acres).toLocaleString() : '0'}`);
        console.log(`   Active Listings: ${overallStats.active_listings}`);
        console.log(`   Expired Listings: ${overallStats.expired_listings}`);
        console.log(`   Processed Listings: ${overallStats.processed_listings}`);
        
        // Regional breakdown
        const regionalStats = await db.getRegionalStats();
        if (regionalStats.length > 0) {
            console.log('\n🗺️  Regional Breakdown:');
            for (const region of regionalStats) {
                console.log(`   ${region.region} (${region.listing_type}):`);
                console.log(`     Sale Groups: ${region.sale_groups}, Parcels: ${region.parcels}, Acres: ${region.total_acres ? Number(region.total_acres).toLocaleString() : '0'}`);
                console.log(`     Active: ${region.active_count}, Expired: ${region.expired_count}, Processed: ${region.processed_count}`);
            }
        }
        
        // Active listings
        const activeListings = await db.getActiveListings();
        if (activeListings.length > 0) {
            console.log('\n📋 Active Listings:');
            for (const listing of activeListings) {
                const processedStatus = listing.is_processed ? '✅' : '⏳';
                console.log(`   ${processedStatus} ${listing.sale_group} (${listing.region}) - ${listing.parcel_count || 0} parcels`);
            }
        }
        
        await db.close();
        
    } catch (error) {
        console.error('❌ Error retrieving statistics:', error);
        await db.close();
        process.exit(1);
    }
}

async function initializeDatabase(): Promise<void> {
    console.log('🔧 Initializing Multi-Listing Database Schema...');
    
    const db = new MultiListingDatabase();
    
    try {
        await db.initialize();
        console.log('✅ Database initialization completed');
        
        // Show initial stats
        await showStats();
        
        await db.close();
        
    } catch (error) {
        console.error('❌ Database initialization failed:', error);
        await db.close();
        process.exit(1);
    }
}

function showHelp(): void {
    console.log('🤖 Multi-Listing EnergyNet Scraper');
    console.log('=' .repeat(50));
    console.log('Usage: npm run scrape <command> [options]');
    console.log('');
    console.log('Commands:');
    console.log('  scrape              Run full multi-listing scraper for all 12 active listings');
    console.log('  test <sale_group>   Test scraper with a single listing (e.g., GEONV-2025-Q4)');
    console.log('  stats               Show current database statistics');
    console.log('  init                Initialize database schema');
    console.log('');
    console.log('Examples:');
    console.log('  npm run scrape scrape     # Scrape all active listings');
    console.log('  npm run scrape test 6391  # Test with Nevada geothermal listing');
    console.log('  npm run scrape stats      # Show current statistics');
    console.log('  npm run scrape init       # Initialize database');
    console.log('');
    console.log('Anti-Bot Features:');
    console.log('  • 2-5 second random delays between requests');
    console.log('  • Rotating user agents');
    console.log('  • Respectful crawling patterns');
    console.log('  • Session management');
}

// Handle graceful shutdown
process.on('SIGINT', () => {
    console.log('\n🛑 Received SIGINT, shutting down gracefully...');
    process.exit(0);
});

process.on('SIGTERM', () => {
    console.log('\n🛑 Received SIGTERM, shutting down gracefully...');
    process.exit(0);
});

// Run the CLI
if (require.main === module) {
    main().catch(error => {
        console.error('❌ Unhandled error:', error);
        process.exit(1);
    });
}

export { main };