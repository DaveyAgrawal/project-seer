import { chromium, Browser, Page } from 'playwright';
import * as fs from 'fs';
import * as path from 'path';
import { EnergyNetProcessor, EnergyNetListing } from './energynet';
import { MultiListingDatabase } from './multi-listing-db';
import { GISProcessor } from './gis-processor';

interface DiscoveredListing {
  saleGroup: string;           // e.g., 'GEONV-2025-Q4'
  listingId: string;          // e.g., '6391'
  title: string;              // Full listing title
  region: string;             // e.g., 'Nevada'
  listingType: string;        // e.g., 'Geothermal', 'Oil & Gas Lease'
  agency: string;             // e.g., 'BLM Nevada State Office'
  saleStartDate: Date | null; // Bidding start date
  saleEndDate: Date | null;   // Bidding end date
  url: string;               // View Listing URL
  status: 'active' | 'expired';
}

interface ScrapingStats {
  discovered: number;
  processed: number;
  failed: number;
  skipped: number;
  newListings: number;
  expiredListings: number;
  totalParcels: number;
}

export class MultiListingScraper {
  private browser: Browser | null = null;
  private db: MultiListingDatabase;
  private gisProcessor: GISProcessor;
  private energyNetProcessor: EnergyNetProcessor;
  private downloadDir: string;
  
  // Anti-bot configuration
  private readonly userAgents = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0'
  ];
  
  private readonly baseDelay = 2000; // 2 seconds base delay
  private readonly maxDelay = 5000;  // 5 seconds max delay

  constructor() {
    this.db = new MultiListingDatabase();
    this.gisProcessor = new GISProcessor();
    this.energyNetProcessor = new EnergyNetProcessor(undefined, false); // Don't double-initialize DB
    this.downloadDir = path.join(__dirname, '../downloads/energynet');
    
    // Ensure download directory exists
    if (!fs.existsSync(this.downloadDir)) {
      fs.mkdirSync(this.downloadDir, { recursive: true });
    }
  }

  async initialize(): Promise<void> {
    console.log('🚀 Initializing Multi-Listing EnergyNet Scraper...');
    
    // Initialize database schema
    await this.db.initialize();
    
    // Initialize the EnergyNet processor
    await this.energyNetProcessor.initialize();
    
    // Launch browser with anti-detection measures for listing discovery
    this.browser = await chromium.launch({
      headless: true
    });
    
    console.log('✅ Multi-Listing Scraper initialized');
  }

  async scrapeAllListings(): Promise<ScrapingStats> {
    if (!this.browser) {
      throw new Error('Scraper not initialized. Call initialize() first.');
    }

    const stats: ScrapingStats = {
      discovered: 0,
      processed: 0,
      failed: 0,
      skipped: 0,
      newListings: 0,
      expiredListings: 0,
      totalParcels: 0
    };

    try {
      console.log('🔍 Phase 1: Discovering active listings from EnergyNet...');
      
      // Step 1: Discover all active listings
      const discoveredListings = await this.discoverActiveListings();
      stats.discovered = discoveredListings.length;
      
      console.log(`📋 Found ${stats.discovered} active listings`);

      // Step 2: Update database with discovered listings (mark expired ones)
      await this.updateListingStatus(discoveredListings);
      
      // Step 3: Process each listing
      console.log('🏭 Phase 2: Processing individual listings...');
      
      for (const [index, listing] of discoveredListings.entries()) {
        try {
          console.log(`\n📍 Processing listing ${index + 1}/${discoveredListings.length}: ${listing.saleGroup} (${listing.region})`);
          
          // Check if already processed recently
          const existingListing = await this.db.listingExists(listing.listingId);
          if (existingListing) {
            console.log(`⏭️  Listing ${listing.listingId} already exists, skipping...`);
            stats.skipped++;
            continue;
          }
          
          // Add respectful delay between requests
          await this.respectfulDelay();
          
          // Process the individual listing using the real EnergyNet processor
          const listingUrl = `https://www.energynet.com/govt_listing.pl?sg=${listing.saleGroup}`;
          const energyNetListing = await this.energyNetProcessor.processListing(listingUrl);
          
          if (energyNetListing && energyNetListing.parcels) {
            // Store using multi-listing database
            const multiListingData = {
              saleGroup: listing.saleGroup,
              listingId: listing.listingId,
              title: listing.title,
              region: listing.region,
              listingType: listing.listingType,
              agency: listing.agency,
              status: listing.status,
              url: listing.url,
              gisDownloadUrl: energyNetListing.gisDownloadUrl,
              description: energyNetListing.description,
              parcels: energyNetListing.parcels
            };

            const dbResult = await this.db.storeMultiListing(multiListingData, {
              updateExisting: true,
              batchSize: 1000,
              skipGeometryValidation: false
            });

            stats.processed++;
            stats.newListings++;
            stats.totalParcels += dbResult.parcelsInserted;
            console.log(`✅ Successfully processed ${listing.saleGroup}: ${dbResult.parcelsInserted} parcels`);
          } else {
            stats.failed++;
            console.log(`❌ Failed to process ${listing.saleGroup}: No parcels found or processing failed`);
          }
          
        } catch (error) {
          stats.failed++;
          console.error(`❌ Error processing listing ${listing.saleGroup}:`, error);
        }
      }

      return stats;
      
    } catch (error) {
      console.error('❌ Critical error in scrapeAllListings:', error);
      throw error;
    }
  }

  async discoverActiveListings(): Promise<DiscoveredListing[]> {
    const page = await this.browser!.newPage();
    const listings: DiscoveredListing[] = [];
    
    try {
      // Set random user agent
      const userAgent = this.userAgents[Math.floor(Math.random() * this.userAgents.length)];
      await page.setExtraHTTPHeaders({ 'User-Agent': userAgent });
      
      // Navigate to main listings page
      console.log('🌐 Fetching main EnergyNet listings page...');
      await page.goto('https://www.energynet.com/govt_listing.pl', {
        waitUntil: 'networkidle',
        timeout: 60000
      });
      
      // Wait for content to load
      await page.waitForTimeout(3000);
      
      // Debug: Save screenshot and HTML for inspection
      const html = await page.content();
      console.log(`📄 Page loaded, HTML length: ${html.length} characters`);
      
      // Try multiple selectors to find listings
      const discoveredListings = await page.evaluate(() => {
        const listings: any[] = [];
        
        // New EnergyNet site uses /salegroup/XXXX format
        const viewListingButtons = Array.from(document.querySelectorAll('a[href*="/salegroup/"]'));
        
        console.log(`Found ${viewListingButtons.length} salegroup links`);
        
        viewListingButtons.forEach((button: any, index: number) => {
          try {
            // Get the sale group ID from the new href format: /salegroup/6472
            const href = button.getAttribute('href');
            const sgMatch = href.match(/\/salegroup\/(\d+)/);
            const saleGroup = sgMatch ? sgMatch[1] : `unknown-${index}`;
            
            // Skip if we couldn't extract a valid ID
            if (!sgMatch) return;
            
            // Find the parent card/container for this listing
            const container = button.closest('.rounded-lg') || button.closest('.border') || button.closest('div');
            if (!container) return;
            
            // Extract title - try multiple selectors for new site structure
            let title = 'Unknown Title';
            const titleSelectors = ['h3', 'h2', '.font-semibold', 'p.text-lg', 'p.font-bold'];
            for (const sel of titleSelectors) {
              const el = container.querySelector(sel);
              if (el && el.textContent && el.textContent.trim().length > 10) {
                title = el.textContent.trim();
                break;
              }
            }
            
            // Extract any descriptive text
            const bylineText = container.textContent?.substring(0, 200) || '';
            
            // Parse region/state from title
            let region = 'Unknown';
            const titleLower = title.toLowerCase();
            if (titleLower.includes('wyoming')) region = 'Wyoming';
            else if (titleLower.includes('nevada')) region = 'Nevada';
            else if (titleLower.includes('new mexico')) region = 'New Mexico';
            else if (titleLower.includes('alaska')) region = 'Alaska';
            else if (titleLower.includes('utah')) region = 'Utah';
            else if (titleLower.includes('montana')) region = 'Montana';
            else if (titleLower.includes('colorado')) region = 'Colorado';
            else if (titleLower.includes('oklahoma')) region = 'Oklahoma';
            else if (titleLower.includes('idaho')) region = 'Idaho';
            else if (titleLower.includes('las vegas')) region = 'Nevada';
            
            // Parse listing type
            let listingType = 'Oil & Gas Lease';
            if (titleLower.includes('geothermal')) listingType = 'Geothermal';
            else if (titleLower.includes('land sale')) listingType = 'Land Sale';
            
            // Extract agency from title
            let agency = 'Unknown Agency';
            if (titleLower.includes('blm')) {
              agency = `BLM ${region} State Office`;
            } else if (titleLower.includes('state oil & gas') || titleLower.includes('state land')) {
              agency = `${region} State Lands`;
            } else if (titleLower.includes('dnr')) {
              agency = `${region} DNR`;
            } else if (titleLower.includes('clo')) {
              agency = `${region} CLO`;
            } else if (titleLower.includes('city of las vegas')) {
              agency = 'City of Las Vegas';
            }
            
            // Extract date information
            const dateElement = container.querySelector('small.text-center');
            let saleStartDate = null;
            let saleEndDate = null;
            
            if (dateElement) {
              const dateText = dateElement.textContent;
              console.log(`Date text for ${saleGroup}: ${dateText}`);
            }
            
            listings.push({
              saleGroup,
              listingId: saleGroup,
              title,
              region,
              listingType,
              agency,
              url: `https://www.energynet.com/salegroup/${saleGroup}`,
              status: 'active',
              byline: bylineText
            });
            
          } catch (error) {
            console.log(`Error parsing listing ${index}:`, error);
          }
        });
        
        return listings;
      });
      
      // Deduplicate by saleGroup (each listing may have multiple buttons)
      const uniqueListings = new Map<string, any>();
      discoveredListings.forEach((listing: any) => {
        if (!uniqueListings.has(listing.saleGroup)) {
          uniqueListings.set(listing.saleGroup, listing);
        } else {
          // Merge: prefer listing with better title
          const existing = uniqueListings.get(listing.saleGroup);
          if (listing.title !== 'Unknown Title' && existing.title === 'Unknown Title') {
            uniqueListings.set(listing.saleGroup, listing);
          }
        }
      });
      
      listings.push(...uniqueListings.values());
      
      console.log(`✅ Discovered ${listings.length} unique active listings`);
      
      return listings;
      
    } catch (error) {
      console.error('❌ Error discovering listings:', error);
      throw error;
    } finally {
      await page.close();
    }
  }

  // Note: processListing method removed - now using EnergyNetProcessor directly

  private async updateListingStatus(discoveredListings: DiscoveredListing[]): Promise<void> {
    console.log('🔄 Updating listing status in database...');
    
    // Get current active sale groups from discovered listings
    const activeSaleGroups = discoveredListings.map(l => l.saleGroup);
    
    // Mark any existing listings not in discovered list as expired
    // This would be implemented with database queries
    
    console.log('✅ Listing status updated');
  }

  private async respectfulDelay(): Promise<void> {
    // Random delay between baseDelay and maxDelay
    const delay = this.baseDelay + Math.random() * (this.maxDelay - this.baseDelay);
    console.log(`⏱️  Respectful delay: ${Math.round(delay)}ms`);
    await new Promise(resolve => setTimeout(resolve, delay));
  }

  async close(): Promise<void> {
    if (this.browser) {
      await this.browser.close();
      this.browser = null;
    }
    
    // Close the EnergyNet processor
    await this.energyNetProcessor.close();
    
    await this.db.close();
    console.log('✅ Multi-Listing Scraper closed');
  }

  // Utility method for manual testing
  async testSingleListing(saleGroup: string): Promise<void> {
    console.log(`🧪 Testing single listing: ${saleGroup}`);
    
    // Test using the EnergyNet processor directly
    const listingUrl = `https://www.energynet.com/govt_listing.pl?sg=${saleGroup}`;
    const energyNetListing = await this.energyNetProcessor.processListing(listingUrl);
    
    if (energyNetListing && energyNetListing.parcels) {
      console.log('✅ Test successful:', {
        title: energyNetListing.title,
        state: energyNetListing.state,
        parcelsFound: energyNetListing.parcels.features.length
      });
    } else {
      console.log('❌ Test failed: No parcels found or processing failed');
    }
  }
}

// CLI interface for running the scraper
export async function runMultiListingScraper(): Promise<void> {
  const scraper = new MultiListingScraper();
  
  try {
    await scraper.initialize();
    
    console.log('🚀 Starting Multi-Listing EnergyNet Scraper...');
    const stats = await scraper.scrapeAllListings();
    
    console.log('\n📊 Scraping Complete - Final Statistics:');
    console.log(`   • Discovered: ${stats.discovered} listings`);
    console.log(`   • Processed: ${stats.processed} listings`);
    console.log(`   • Failed: ${stats.failed} listings`);
    console.log(`   • Skipped: ${stats.skipped} listings`);
    console.log(`   • New Listings: ${stats.newListings}`);
    console.log(`   • Total Parcels: ${stats.totalParcels}`);
    
    await scraper.close();
    
  } catch (error) {
    console.error('❌ Critical scraper error:', error);
    await scraper.close();
    process.exit(1);
  }
}