// EnergyNet government land auction processing
import { chromium, Browser, Page } from 'playwright';
import * as fs from 'fs';
import { pipeline } from 'stream/promises';
import fetch from 'node-fetch';
import * as cheerio from 'cheerio';
import * as path from 'path';
import { GISProcessor, LandParcelCollection } from './gis-processor';
import { MultiListingDatabase, MultiListingInsertResult } from './multi-listing-db';

export interface EnergyNetListing {
  id: string;                    // Listing ID (e.g., "6403")
  title: string;                 // Full listing title
  state: string;                 // State/region
  url: string;                   // View listing URL
  description?: string;          // Additional description
  gisDownloadUrl?: string;       // GIS download URL
  gisFilePath?: string;          // Local path to downloaded GIS file
  parcels?: LandParcelCollection; // Processed parcel data
  dbResult?: MultiListingInsertResult;     // Database storage result
}

export interface EnergyNetScrapingOptions {
  downloadDir?: string;
  skipExisting?: boolean;
  validateGeometry?: boolean;
  respectfulDelayMs?: number;
  enableDatabase?: boolean;
  cacheResults?: boolean;
}

export interface EnergyNetScrapingResult {
  success: boolean;
  listingId: string;
  parcelsFound: number;
  errors?: string[];
  downloadPath?: string;
}

export class EnergyNetProcessor {
  private browser: Browser | null = null;
  private page: Page | null = null;
  private database: MultiListingDatabase | null = null;
  private downloadDir: string;
  private gisProcessor: GISProcessor;

  constructor(downloadDir: string = './downloads/energynet', enableDatabase: boolean = false) {
    this.downloadDir = downloadDir;
    this.gisProcessor = new GISProcessor(downloadDir);
    
    if (enableDatabase) {
      this.database = new MultiListingDatabase();
    }

    // Ensure download directory exists
    if (!fs.existsSync(this.downloadDir)) {
      fs.mkdirSync(this.downloadDir, { recursive: true });
    }
  }

  async initialize(): Promise<void> {
    console.log('🌐 Initializing EnergyNet processor...');
    
    // Initialize database if enabled
    if (this.database) {
      await this.database.initialize();
    }

    this.browser = await chromium.launch({
      headless: true,
      timeout: 60000
    });

    this.page = await this.browser.newPage();

    // Set realistic user agent and headers
    await this.page.setExtraHTTPHeaders({
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
      'Accept-Language': 'en-US,en;q=0.5',
      'Accept-Encoding': 'gzip, deflate',
      'DNT': '1',
      'Connection': 'keep-alive'
    });
  }

  async processListing(listingUrl: string): Promise<EnergyNetListing | null> {
    if (!this.page) {
      throw new Error('Processor not initialized. Call initialize() first.');
    }

    try {
      console.log(`📄 Processing listing: ${listingUrl}`);
      
      // Navigate to listing page
      await this.page.goto(listingUrl, {
        waitUntil: 'domcontentloaded',
        timeout: 60000
      });

      // Wait for content to load
      await this.page.waitForTimeout(2000);

      // Get page content for parsing
      const content = await this.page.content();
      const $ = cheerio.load(content);

      // Extract listing information
      const listing = await this.extractListingData($ as any, listingUrl);
      
      if (listing) {
        console.log(`✅ Successfully extracted listing: ${listing.title}`);
        
        // Try to find and download GIS data
        if (listing.gisDownloadUrl) {
          console.log(`🗺️ Found GIS data link: ${listing.gisDownloadUrl}`);
          const downloadPath = await this.downloadGISData(listing);
          
          if (downloadPath) {
            console.log(`💾 GIS data downloaded to: ${downloadPath}`);
            listing.gisFilePath = downloadPath;
            
            // Process GIS data to extract land parcels
            try {
              console.log(`🔄 Processing GIS data...`);
              const parcels = await this.processGISData(listing);
              listing.parcels = parcels || undefined;
              
              if (listing.parcels) {
                console.log(`✅ Extracted ${listing.parcels.features.length} land parcels`);
              }
            } catch (gisError) {
              console.warn(`⚠️ GIS processing failed, continuing without parcel data:`, gisError);
            }
          }
        }

        // Store to database if enabled
        if (this.database && listing && listing.parcels) {
          try {
            console.log(`💾 Storing listing to database...`);
            
            // Convert to MultiListing format
            const multiListingData = {
              saleGroup: listing.id, // Use listing ID as sale group for now
              listingId: listing.id,
              title: listing.title,
              region: listing.state,
              listingType: listing.description || 'Oil & Gas Lease',
              agency: `${listing.state} State Lands`, // Inferred agency
              status: 'active' as const,
              url: listing.url,
              gisDownloadUrl: listing.gisDownloadUrl,
              description: listing.description,
              parcels: listing.parcels
            };

            listing.dbResult = await this.database.storeMultiListing(multiListingData, {
              updateExisting: true,
              batchSize: 1000,
              skipGeometryValidation: false
            });

            console.log(`✅ Database storage completed:`);
            console.log(`   📋 Listing: ${listing.dbResult.listingInserted ? 'Stored' : 'Updated'}`);
            console.log(`   📍 Parcels inserted: ${listing.dbResult.parcelsInserted}`);
            console.log(`   ⏭️ Parcels skipped: ${listing.dbResult.parcelsSkipped}`);
            console.log(`   ❌ Parcels errors: ${listing.dbResult.parcelsErrors}`);
          } catch (dbError) {
            console.warn(`⚠️ Database storage failed:`, dbError);
          }
        }
      }

      return listing;

    } catch (error) {
      console.error(`❌ Error processing listing ${listingUrl}:`, error);
      return null;
    }
  }

  private async extractListingData($: cheerio.CheerioAPI, url: string): Promise<EnergyNetListing | null> {
    try {
      // Extract listing ID from URL - support both old and new formats
      let id = 'unknown';
      const oldMatch = url.match(/sg=(\d+)/);
      const newMatch = url.match(/\/salegroup\/(\d+)/);
      if (oldMatch) id = oldMatch[1];
      else if (newMatch) id = newMatch[1];

      // Look for the main title - try multiple selectors
      let title = '';
      
      // Try various common selectors for the title
      const titleSelectors = [
        'h1',
        'h2', 
        '.listing-title',
        '.title',
        'td[bgcolor="#003366"] font[color="white"]',
        'font[color="white"]',
        'b:contains("State Oil & Gas")'
      ];

      for (const selector of titleSelectors) {
        const element = $(selector);
        if (element.length > 0) {
          title = element.first().text().trim();
          if (title && title.length > 10) break; // Found a substantial title
        }
      }

      // If still no title, look for text containing our expected content
      if (!title) {
        $('*').each((_, el) => {
          const text = $(el).text().trim();
          if (text.includes('State Oil & Gas Lease Sale') && text.includes('New Mexico')) {
            title = text;
            return false; // break
          }
        });
      }

      // Extract state information  
      let state = 'Unknown';
      const titleLower = title.toLowerCase();
      if (titleLower.includes('new mexico')) state = 'New Mexico';
      else if (titleLower.includes('nevada')) state = 'Nevada';
      else if (titleLower.includes('wyoming')) state = 'Wyoming';
      else if (titleLower.includes('alaska')) state = 'Alaska';
      else if (titleLower.includes('utah')) state = 'Utah';
      else if (titleLower.includes('montana')) state = 'Montana';
      else if (titleLower.includes('colorado')) state = 'Colorado';
      else if (titleLower.includes('california')) state = 'California';
      else if (titleLower.includes('eastern states')) state = 'Eastern States';

      // Look for GIS data download link
      let gisDownloadUrl = '';
      
      // Look for links containing "GIS" and "WGS84"
      $('a').each((_, link) => {
        const linkText = $(link).text().toLowerCase();
        const href = $(link).attr('href');
        
        if (href && (linkText.includes('gis') && linkText.includes('wgs84'))) {
          gisDownloadUrl = this.resolveUrl(href, url);
          return false; // break
        }
      });

      // If no WGS84 link found, look for any GIS link
      if (!gisDownloadUrl) {
        $('a').each((_, link) => {
          const linkText = $(link).text().toLowerCase();
          const href = $(link).attr('href');
          
          if (href && linkText.includes('gis')) {
            gisDownloadUrl = this.resolveUrl(href, url);
            return false; // break
          }
        });
      }

      // If still no GIS link, look for ZIP download links
      if (!gisDownloadUrl) {
        $('a').each((_, link) => {
          const href = $(link).attr('href');
          
          if (href && href.includes('.zip')) {
            gisDownloadUrl = this.resolveUrl(href, url);
            return false; // break
          }
        });
      }

      const listing: EnergyNetListing = {
        id,
        title: title || `EnergyNet Listing ${id}`,
        state,
        url,
        description: 'Oil & Gas Lease',
        gisDownloadUrl: gisDownloadUrl || undefined
      };

      return listing;

    } catch (error) {
      console.error('❌ Error extracting listing data:', error);
      return null;
    }
  }

  private resolveUrl(href: string, baseUrl: string): string {
    if (href.startsWith('http')) {
      return href;
    } else if (href.startsWith('/')) {
      const urlObj = new URL(baseUrl);
      return `${urlObj.protocol}//${urlObj.host}${href}`;
    } else {
      const urlObj = new URL(baseUrl);
      return `${urlObj.protocol}//${urlObj.host}/${href}`;
    }
  }

  private async downloadGISData(listing: EnergyNetListing): Promise<string | null> {
    if (!listing.gisDownloadUrl || !this.page) {
      return null;
    }

    try {
      const filename = `${listing.id}_gis_data.zip`;
      const filepath = path.join(this.downloadDir, filename);

      // Skip if already downloaded
      if (fs.existsSync(filepath)) {
        console.log(`📁 Using existing download: ${filepath}`);
        return filepath;
      }

      console.log(`📥 Downloading GIS data from: ${listing.gisDownloadUrl}`);

      // Use the existing browser page to maintain session cookies
      const cookies = await this.page.context().cookies();
      const cookieHeader = cookies.map(cookie => `${cookie.name}=${cookie.value}`).join('; ');

      const response = await fetch(listing.gisDownloadUrl, {
        headers: {
          'Cookie': cookieHeader,
          'User-Agent': await this.page.evaluate(() => navigator.userAgent),
          'Referer': listing.url
        }
      });

      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`);
      }

      if (!response.body) {
        throw new Error('No response body');
      }

      // Download the file
      const writeStream = fs.createWriteStream(filepath);
      await pipeline(response.body, writeStream);

      console.log(`✅ Downloaded GIS data: ${filepath}`);
      return filepath;

    } catch (error) {
      console.error(`❌ Error downloading GIS data for listing ${listing.id}:`, error);
      return null;
    }
  }

  private async processGISData(listing: EnergyNetListing): Promise<LandParcelCollection | null> {
    if (!listing.gisFilePath) {
      return null;
    }

    try {
      const collection = await this.gisProcessor.processZipFile(
        listing.gisFilePath,
        listing.id,
        listing.state,
        {
          validateGeometry: true,
          cacheResults: true
        }
      );

      return collection;

    } catch (error) {
      console.error(`❌ Error processing GIS data for listing ${listing.id}:`, error);
      return null;
    }
  }

  async close(): Promise<void> {
    if (this.browser) {
      await this.browser.close();
      this.browser = null;
      this.page = null;
    }

    if (this.database) {
      await this.database.close();
    }
  }

  // Single listing scraping method for compatibility with multi-listing scraper
  async scrapeListing(listingId: string): Promise<EnergyNetScrapingResult> {
    const listingUrl = `https://www.energynet.com/govt_listing.pl?sg=${listingId}`;
    
    try {
      await this.initialize();
      const listing = await this.processListing(listingUrl);
      
      if (listing && listing.parcels) {
        return {
          success: true,
          listingId,
          parcelsFound: listing.parcels.features.length,
          downloadPath: listing.gisFilePath
        };
      } else {
        return {
          success: false,
          listingId,
          parcelsFound: 0,
          errors: ['Failed to process listing or no parcels found']
        };
      }
    } catch (error) {
      return {
        success: false,
        listingId,
        parcelsFound: 0,
        errors: [error instanceof Error ? error.message : 'Unknown error']
      };
    } finally {
      await this.close();
    }
  }
}

// CLI interface for running the scraper (backward compatibility)
export async function main(): Promise<void> {
  const args = process.argv.slice(2);
  
  if (args.length === 0) {
    console.log('Usage: node energynet.js <listing_url>');
    process.exit(1);
  }

  const listingUrl = args[0];
  const processor = new EnergyNetProcessor('./downloads/energynet', true);

  try {
    await processor.initialize();
    const listing = await processor.processListing(listingUrl);
    
    if (listing) {
      console.log('🎉 Processing completed successfully!');
      console.log(`📋 Listing: ${listing.title}`);
      console.log(`🗺️ State: ${listing.state}`);
      
      if (listing.parcels) {
        console.log(`📍 Parcels: ${listing.parcels.features.length}`);
        console.log(`📏 Total acres: ${listing.parcels.metadata?.total_acres?.toFixed(2)}`);
      }
    } else {
      console.log('❌ Failed to process listing');
      process.exit(1);
    }
  } catch (error) {
    console.error('❌ Error:', error);
    process.exit(1);
  } finally {
    await processor.close();
  }
}

// Run if this file is executed directly
if (require.main === module) {
  main().catch(error => {
    console.error('❌ Fatal error:', error);
    process.exit(1);
  });
}