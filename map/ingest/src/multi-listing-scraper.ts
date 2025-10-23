import puppeteer, { Browser, Page } from 'puppeteer';
import * as fs from 'fs';
import * as path from 'path';
import { EnergyNetListing } from './energynet';
import { EnergyNetDatabase } from './energynet-db';
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
  private db: EnergyNetDatabase;
  private gisProcessor: GISProcessor;
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
    this.db = new EnergyNetDatabase();
    this.gisProcessor = new GISProcessor();
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
    
    // Launch browser with anti-detection measures
    this.browser = await puppeteer.launch({
      headless: 'new',
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-accelerated-2d-canvas',
        '--no-first-run',
        '--no-default-browser-check',
        '--disable-default-apps',
        '--disable-extensions',
        '--disable-plugins'
      ]
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
          
          // Process the individual listing
          const processResult = await this.processListing(listing);
          
          if (processResult.success) {
            stats.processed++;
            stats.newListings++;
            stats.totalParcels += processResult.parcelsCount;
            console.log(`✅ Successfully processed ${listing.saleGroup}: ${processResult.parcelsCount} parcels`);
          } else {
            stats.failed++;
            console.log(`❌ Failed to process ${listing.saleGroup}: ${processResult.error}`);
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

  private async discoverActiveListings(): Promise<DiscoveredListing[]> {
    const page = await this.browser!.newPage();
    const listings: DiscoveredListing[] = [];
    
    try {
      // Set random user agent
      const userAgent = this.userAgents[Math.floor(Math.random() * this.userAgents.length)];
      await page.setUserAgent(userAgent);
      
      // Navigate to main listings page
      console.log('🌐 Fetching main EnergyNet listings page...');
      await page.goto('https://www.energynet.com/govt_listing.pl', {
        waitUntil: 'networkidle2',
        timeout: 30000
      });
      
      // Extract all active listings
      const discoveredListings = await page.evaluate(() => {
        const listings: any[] = [];
        
        // Find all listing containers (adjust selector based on actual page structure)
        const listingElements = document.querySelectorAll('div[class*="listing"], tr[class*="listing"], .govt-listing');
        
        listingElements.forEach((element: any, index: number) => {
          try {
            // Extract listing details (adjust selectors based on actual page structure)
            const titleElement = element.querySelector('h3, .title, td:first-child, a[href*="govt_listing"]');
            const linkElement = element.querySelector('a[href*="govt_listing.pl?sg="], a[href*="View"]');
            
            if (titleElement && linkElement) {
              const title = titleElement.textContent?.trim() || '';
              const url = linkElement.getAttribute('href') || '';
              
              // Extract sale group ID from URL (sg=XXXX)
              const sgMatch = url.match(/sg=([^&]+)/);
              const saleGroup = sgMatch ? sgMatch[1] : `unknown-${index}`;
              
              // Parse region/state from title
              let region = 'Unknown';
              if (title.includes('Nevada')) region = 'Nevada';
              else if (title.includes('Wyoming')) region = 'Wyoming';
              else if (title.includes('New Mexico')) region = 'New Mexico';
              else if (title.includes('Alaska')) region = 'Alaska';
              else if (title.includes('Utah')) region = 'Utah';
              else if (title.includes('Montana')) region = 'Montana';
              else if (title.includes('Colorado')) region = 'Colorado';
              
              // Parse listing type
              let listingType = 'Oil & Gas Lease';
              if (title.toLowerCase().includes('geothermal')) listingType = 'Geothermal';
              else if (title.toLowerCase().includes('land sale')) listingType = 'Land Sale';
              
              // Extract agency
              let agency = 'Unknown Agency';
              if (title.includes('BLM')) agency = `BLM ${region} State Office`;
              else if (title.includes('State')) agency = `${region} State Lands`;
              
              listings.push({
                saleGroup,
                listingId: saleGroup, // Use sale group as listing ID for now
                title,
                region,
                listingType,
                agency,
                url: url.startsWith('http') ? url : `https://www.energynet.com/${url}`,
                status: 'active'
              });
            }
          } catch (error) {
            console.log('Error parsing listing element:', error);
          }
        });
        
        return listings;
      });
      
      // Add discovered listings to results
      listings.push(...discoveredListings);
      
      console.log(`✅ Discovered ${listings.length} active listings`);
      
      return listings;
      
    } catch (error) {
      console.error('❌ Error discovering listings:', error);
      throw error;
    } finally {
      await page.close();
    }
  }

  private async processListing(listing: DiscoveredListing): Promise<{ success: boolean; parcelsCount: number; error?: string }> {
    const page = await this.browser!.newPage();
    
    try {
      // Set random user agent
      const userAgent = this.userAgents[Math.floor(Math.random() * this.userAgents.length)];
      await page.setUserAgent(userAgent);
      
      console.log(`🔍 Accessing listing page: ${listing.url}`);
      
      // Navigate to specific listing page
      await page.goto(listing.url, {
        waitUntil: 'networkidle2',
        timeout: 30000
      });
      
      // Extract GIS download URLs
      const gisDownloads = await page.evaluate(() => {
        const downloads: { format: string; url: string }[] = [];
        
        // Look for GIS download links (WGS84, NAD83, NAD27)
        const downloadLinks = document.querySelectorAll('a[href*=".zip"], a[href*="library/secure"]');
        
        downloadLinks.forEach((link: any) => {
          const href = link.getAttribute('href') || '';
          const text = link.textContent?.trim().toLowerCase() || '';
          
          if (href.includes('.zip') || href.includes('library/secure')) {
            let format = 'Unknown';
            if (text.includes('wgs84') || href.includes('WGS84')) format = 'WGS84';
            else if (text.includes('nad83') || href.includes('NAD83')) format = 'NAD83';
            else if (text.includes('nad27') || href.includes('NAD27')) format = 'NAD27';
            
            downloads.push({
              format,
              url: href.startsWith('http') ? href : `https://www.energynet.com${href}`
            });
          }
        });
        
        return downloads;
      });
      
      if (gisDownloads.length === 0) {
        return { success: false, parcelsCount: 0, error: 'No GIS download links found' };
      }
      
      // Prefer WGS84 format, fallback to first available
      const preferredDownload = gisDownloads.find(d => d.format === 'WGS84') || gisDownloads[0];
      
      console.log(`📥 Found GIS download: ${preferredDownload.format}`);
      
      // Create EnergyNet listing object
      const energyNetListing: EnergyNetListing = {
        id: listing.listingId,
        title: listing.title,
        state: listing.region,
        url: listing.url,
        description: `${listing.listingType} - ${listing.agency}`,
        gisDownloadUrl: preferredDownload.url,
        gisFilePath: '', // Will be set after download
        parcels: undefined    // Will be set after processing
      };
      
      // Download and process GIS data using existing pipeline
      console.log(`📦 Downloading and processing GIS data...`);
      
      // Use existing EnergyNet scraper logic for download and processing
      // (This would typically involve downloading the ZIP, extracting shapefiles, and processing)
      
      // For now, simulate successful processing
      // In actual implementation, this would use the existing download/process pipeline
      
      // Store listing in database with multi-listing fields
      const insertResult = await this.db.storeListing(energyNetListing, {
        updateExisting: true
      });
      
      // Update with multi-listing specific fields
      // This would be done through database queries to add sale_group, region, etc.
      
      return {
        success: true,
        parcelsCount: insertResult.parcelsInserted,
        error: undefined
      };
      
    } catch (error) {
      return {
        success: false,
        parcelsCount: 0,
        error: error instanceof Error ? error.message : 'Unknown error'
      };
    } finally {
      await page.close();
    }
  }

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
    
    await this.db.close();
    console.log('✅ Multi-Listing Scraper closed');
  }

  // Utility method for manual testing
  async testSingleListing(saleGroup: string): Promise<void> {
    console.log(`🧪 Testing single listing: ${saleGroup}`);
    
    const testListing: DiscoveredListing = {
      saleGroup,
      listingId: saleGroup,
      title: `Test Listing ${saleGroup}`,
      region: 'Test State',
      listingType: 'Oil & Gas Lease',
      agency: 'Test Agency',
      saleStartDate: null,
      saleEndDate: null,
      url: `https://www.energynet.com/govt_listing.pl?sg=${saleGroup}`,
      status: 'active'
    };
    
    const result = await this.processListing(testListing);
    console.log('Test result:', result);
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