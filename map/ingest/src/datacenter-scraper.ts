import { chromium, Browser, Page } from 'playwright';
import * as cheerio from 'cheerio';
import { DatacenterDatabase, DatacenterData } from './datacenter-db';

export interface ProgressCallback {
  (status: string, data?: any): void;
}

export interface ScrapingProgress {
  status: 'starting' | 'discovering' | 'discovered' | 'scraping' | 'complete' | 'error';
  message: string;
  currentPage?: number;
  totalPages?: number;
  currentListing?: number;
  totalListings?: number;
  processed?: number;
  failed?: number;
  failedUrls?: string[];
}

export interface ScrapingStats {
  discovered: number;
  processed: number;
  failed: number;
  skipped: number;
  totalUs: number;
  totalInternational: number;
}

export class DatacenterScraper {
  private browser: Browser | null = null;
  private page: Page | null = null;
  private db: DatacenterDatabase;
  private progressCallback: ProgressCallback | null = null;

  // Anti-bot configuration
  private readonly userAgents = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
  ];

  private readonly baseDelay = 2000; // 2 seconds base delay
  private readonly maxDelay = 5000;  // 5 seconds max delay
  private readonly baseUrl = 'https://www.datacenters.com';

  constructor(enableDatabase: boolean = true) {
    if (enableDatabase) {
      this.db = new DatacenterDatabase();
    }
  }

  setProgressCallback(callback: ProgressCallback): void {
    this.progressCallback = callback;
  }

  private sendProgress(status: string, data?: any): void {
    if (this.progressCallback) {
      this.progressCallback(status, data);
    }
  }

  async initialize(): Promise<void> {
    console.log('🚀 Initializing Datacenter Scraper...');

    if (this.db) {
      await this.db.initialize();
    }

    // Launch browser with anti-detection measures
    this.browser = await chromium.launch({
      headless: true,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-dev-shm-usage',
        '--disable-accelerated-2d-canvas',
        '--disable-blink-features=AutomationControlled'
      ]
    });

    // Create a new page with random user agent
    this.page = await this.browser.newPage({
      userAgent: this.userAgents[Math.floor(Math.random() * this.userAgents.length)]
    });

    // Set viewport to common desktop size
    await this.page.setViewportSize({ width: 1920, height: 1080 });

    console.log('✅ Datacenter Scraper initialized');
  }

  /**
   * Respectful delay between requests
   */
  private async respectfulDelay(): Promise<void> {
    const delay = this.baseDelay + Math.random() * (this.maxDelay - this.baseDelay);
    await new Promise(resolve => setTimeout(resolve, delay));
  }

  /**
   * Get a random user agent
   */
  private getRandomUserAgent(): string {
    return this.userAgents[Math.floor(Math.random() * this.userAgents.length)];
  }

  /**
   * Discover all datacenter facility URLs from paginated listings
   */
  async discoverAllDatacenters(): Promise<string[]> {
    if (!this.page) {
      throw new Error('Scraper not initialized. Call initialize() first.');
    }

    const facilityUrls: string[] = [];
    const startUrl = `${this.baseUrl}/locations`;

    console.log(`🔍 Starting discovery from ${startUrl}`);
    this.sendProgress('discovering', { message: 'Starting to discover datacenters...' });

    try {
      // Navigate to the first page to determine total pages
      await this.page.goto(startUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });
      await this.respectfulDelay();

      const content = await this.page.content();
      const $ = cheerio.load(content);

      // Determine total number of pages (assume 203 as specified)
      // Adjust this selector based on actual site structure
      const totalPages = 203;
      console.log(`📄 Total pages to scrape: ${totalPages}`);

      // Iterate through all pages
      for (let currentPage = 1; currentPage <= totalPages; currentPage++) {
        try {
          console.log(`\n📄 Processing page ${currentPage}/${totalPages}...`);
          this.sendProgress('discovering', {
            message: `Discovering facilities on page ${currentPage}/${totalPages}`,
            currentPage,
            totalPages
          });

          const pageUrl = currentPage === 1 ? startUrl : `${startUrl}?page=${currentPage}`;

          // Navigate to the page
          await this.page.goto(pageUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });
          await this.respectfulDelay();

          // Extract facility URLs from the page
          const pageContent = await this.page.content();
          const page$ = cheerio.load(pageContent);

          // Extract all facility links
          // Adjust these selectors based on actual site structure
          const facilityLinks: string[] = [];

          page$('a[href*="/locations/"]').each((_, element) => {
            const href = page$(element).attr('href');
            if (href && href.includes('/locations/') && !href.endsWith('/locations')) {
              // Build full URL
              const fullUrl = href.startsWith('http') ? href : `${this.baseUrl}${href}`;

              // Avoid duplicates and ensure it's a facility page
              if (!facilityUrls.includes(fullUrl) && !facilityLinks.includes(fullUrl)) {
                facilityLinks.push(fullUrl);
              }
            }
          });

          console.log(`   Found ${facilityLinks.length} facilities on page ${currentPage}`);
          facilityUrls.push(...facilityLinks);

          // Respectful delay between pages
          await this.respectfulDelay();

        } catch (error) {
          console.error(`❌ Error processing page ${currentPage}:`, error);
          // Continue with next page
        }
      }

      console.log(`\n✅ Discovery complete! Found ${facilityUrls.length} datacenter facilities`);
      this.sendProgress('discovered', {
        message: `Discovered ${facilityUrls.length} datacenter facilities`,
        totalListings: facilityUrls.length
      });

      return facilityUrls;

    } catch (error) {
      console.error('❌ Error during discovery:', error);
      this.sendProgress('error', { message: `Discovery failed: ${error.message}` });
      throw error;
    }
  }

  /**
   * Scrape detailed information from a single datacenter facility page
   */
  async scrapeDatacenterDetail(facilityUrl: string): Promise<DatacenterData | null> {
    if (!this.page) {
      throw new Error('Scraper not initialized. Call initialize() first.');
    }

    try {
      console.log(`🏢 Scraping facility: ${facilityUrl}`);

      // Navigate to facility page
      await this.page.goto(facilityUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });
      await this.respectfulDelay();

      const content = await this.page.content();
      const $ = cheerio.load(content);

      // Extract all required fields
      const datacenter = this.extractDatacenterFields($, facilityUrl);

      return datacenter;

    } catch (error) {
      console.error(`❌ Error scraping ${facilityUrl}:`, error);
      return null;
    }
  }

  /**
   * Extract all datacenter fields from the page HTML
   */
  private extractDatacenterFields($: cheerio.CheerioAPI, facilityUrl: string): DatacenterData {
    const datacenter: DatacenterData = {
      facilityUrl,
      name: '',
      country: 'Unknown',
      missingFields: []
    };

    // Extract basic information
    // Note: These selectors are placeholders and need to be adjusted based on actual site structure

    // Name (facility name, not parent company)
    datacenter.name = $('h1.facility-name, .datacenter-name, h1').first().text().trim() || 'Unknown Facility';

    // Operator/Company
    datacenter.operator = $('.operator, .company-name, .provider').first().text().trim() || undefined;

    // Address fields
    datacenter.streetAddress = $('.address, .street-address').first().text().trim() || undefined;
    datacenter.city = $('.city, .locality').first().text().trim() || undefined;
    datacenter.state = $('.state, .region').first().text().trim() || undefined;
    datacenter.country = $('.country').first().text().trim() || 'Unknown';

    // Latitude/Longitude
    // Look for lat/lng in meta tags, data attributes, or embedded maps
    const latLng = this.extractLatLng($);
    if (latLng) {
      datacenter.latitude = latLng.lat;
      datacenter.longitude = latLng.lng;
    }

    // Technical specifications
    const sqft = $('.square-footage, .size').first().text().trim();
    if (sqft) {
      datacenter.squareFootage = this.parseNumber(sqft);
    }

    datacenter.facilityType = $('.facility-type, .type').first().text().trim() || undefined;

    const powerMw = $('.power-capacity, .power').first().text().trim();
    if (powerMw) {
      datacenter.powerCapacityMw = this.parseNumber(powerMw);
    }

    // Certifications
    const certifications: string[] = [];
    $('.certification, .cert, .badge').each((_, el) => {
      const cert = $(el).text().trim();
      if (cert && cert.length > 0) {
        certifications.push(cert);
      }
    });
    datacenter.certifications = certifications.length > 0 ? certifications : undefined;

    // Features (Bare Metal, Internet Exchange, Colocation, IaaS)
    const features: Record<string, boolean> = {};
    const featureKeywords = ['bare metal', 'internet exchange', 'colocation', 'iaas', 'cloud', 'managed services'];

    $('.feature-box, .feature, .service').each((_, el) => {
      const featureText = $(el).text().toLowerCase().trim();
      featureKeywords.forEach(keyword => {
        if (featureText.includes(keyword)) {
          features[keyword.replace(' ', '_')] = true;
        }
      });
    });
    datacenter.features = Object.keys(features).length > 0 ? features : undefined;

    // Provider URL
    const providerLink = $('a[href*="/providers/"], a.provider-link').first().attr('href');
    if (providerLink) {
      datacenter.providerUrl = providerLink.startsWith('http') ? providerLink : `${this.baseUrl}${providerLink}`;
    }

    // Breadcrumb hierarchy
    const breadcrumbs: string[] = [];
    $('.breadcrumb a, nav.breadcrumb a, .breadcrumbs a').each((_, el) => {
      const crumb = $(el).text().trim();
      if (crumb && crumb.length > 0 && crumb.toLowerCase() !== 'home') {
        breadcrumbs.push(crumb);
      }
    });
    datacenter.breadcrumbHierarchy = breadcrumbs.length > 0 ? breadcrumbs : undefined;

    // Market/region label
    datacenter.marketRegion = $('.market-label, .region-label, .market').first().text().trim() || undefined;

    // Nearby datacenter count
    const nearbyText = $('.nearby-count, .density').first().text().trim();
    if (nearbyText) {
      const nearbyMatch = nearbyText.match(/(\d+)/);
      if (nearbyMatch) {
        datacenter.nearbyDatacenterCount = parseInt(nearbyMatch[1], 10);
      }
    }

    // Miles to airport
    const airportText = $('.airport-distance, .miles-to-airport').first().text().trim();
    if (airportText) {
      const milesMatch = airportText.match(/(\d+\.?\d*)/);
      if (milesMatch) {
        datacenter.milesToAirport = parseFloat(milesMatch[1]);
      }
    }

    // Phone number
    datacenter.phoneNumber = $('.phone, .contact-phone, [href^="tel:"]').first().text().trim() || undefined;

    // Media availability flags
    datacenter.hasImages = $('img.facility-image, .gallery img, .photo').length > 0;
    datacenter.hasBrochures = $('a[href$=".pdf"], .brochure-download').length > 0;
    datacenter.hasMedia = $('.media-tab, .gallery, .photos').length > 0;

    return datacenter;
  }

  /**
   * Extract latitude and longitude from page
   */
  private extractLatLng($: cheerio.CheerioAPI): { lat: number; lng: number } | null {
    // Try meta tags
    const lat = $('meta[property="place:location:latitude"], meta[name="geo.position"]').attr('content');
    const lng = $('meta[property="place:location:longitude"]').attr('content');

    if (lat && lng) {
      return { lat: parseFloat(lat), lng: parseFloat(lng) };
    }

    // Try data attributes
    const dataLat = $('[data-lat], [data-latitude]').first().attr('data-lat') || $('[data-latitude]').first().attr('data-latitude');
    const dataLng = $('[data-lng], [data-longitude]').first().attr('data-lng') || $('[data-longitude]').first().attr('data-longitude');

    if (dataLat && dataLng) {
      return { lat: parseFloat(dataLat), lng: parseFloat(dataLng) };
    }

    // Try embedded JSON-LD
    const jsonLd = $('script[type="application/ld+json"]').html();
    if (jsonLd) {
      try {
        const data = JSON.parse(jsonLd);
        if (data.geo && data.geo.latitude && data.geo.longitude) {
          return {
            lat: parseFloat(data.geo.latitude),
            lng: parseFloat(data.geo.longitude)
          };
        }
      } catch (error) {
        // Ignore JSON parse errors
      }
    }

    return null;
  }

  /**
   * Parse number from string (handles commas, units, etc.)
   */
  private parseNumber(text: string): number | undefined {
    const cleaned = text.replace(/[^0-9.]/g, '');
    const num = parseFloat(cleaned);
    return isNaN(num) ? undefined : num;
  }

  /**
   * Scrape all datacenters (discovery + detailed scraping)
   */
  async scrapeAll(): Promise<ScrapingStats> {
    if (!this.browser || !this.page) {
      throw new Error('Scraper not initialized. Call initialize() first.');
    }

    const stats: ScrapingStats = {
      discovered: 0,
      processed: 0,
      failed: 0,
      skipped: 0,
      totalUs: 0,
      totalInternational: 0
    };

    const failedUrls: string[] = [];

    try {
      this.sendProgress('starting', { message: 'Starting datacenter scrape...' });

      // Phase 1: Discover all facility URLs
      console.log('\n🔍 Phase 1: Discovering all datacenter facilities...');
      const facilityUrls = await this.discoverAllDatacenters();
      stats.discovered = facilityUrls.length;

      // Phase 2: Scrape each facility
      console.log('\n🏭 Phase 2: Scraping individual facilities...');
      this.sendProgress('scraping', {
        message: 'Starting to scrape facility details...',
        totalListings: facilityUrls.length
      });

      for (const [index, url] of facilityUrls.entries()) {
        try {
          console.log(`\n📍 Processing facility ${index + 1}/${facilityUrls.length}`);
          this.sendProgress('scraping', {
            message: `Scraping facility ${index + 1}/${facilityUrls.length}`,
            currentListing: index + 1,
            totalListings: facilityUrls.length,
            processed: stats.processed,
            failed: stats.failed
          });

          // Check if already exists
          if (this.db) {
            const exists = await this.db.datacenterExists(url);
            if (exists) {
              console.log(`⏭️  Datacenter already exists, skipping...`);
              stats.skipped++;
              continue;
            }
          }

          // Add respectful delay
          await this.respectfulDelay();

          // Scrape facility details
          const datacenter = await this.scrapeDatacenterDetail(url);

          if (datacenter && this.db) {
            // Store in database
            const result = await this.db.storeDatacenter(datacenter);

            if (result.inserted || result.updated) {
              stats.processed++;

              // Track US vs international
              const isUs = ['USA', 'United States', 'US', 'United States of America'].some(v =>
                datacenter.country.toLowerCase() === v.toLowerCase()
              );
              if (isUs) {
                stats.totalUs++;
              } else {
                stats.totalInternational++;
              }
            }
          } else {
            stats.failed++;
            failedUrls.push(url);
          }

        } catch (error) {
          console.error(`❌ Error processing facility ${url}:`, error);
          stats.failed++;
          failedUrls.push(url);
        }
      }

      console.log('\n✅ Scraping complete!');
      console.log(`📊 Stats: ${stats.processed} processed, ${stats.failed} failed, ${stats.skipped} skipped`);
      console.log(`🌎 Geographic: ${stats.totalUs} US, ${stats.totalInternational} International`);

      this.sendProgress('complete', {
        message: 'Scraping complete!',
        processed: stats.processed,
        failed: stats.failed,
        failedUrls
      });

      return stats;

    } catch (error) {
      console.error('❌ Fatal error during scraping:', error);
      this.sendProgress('error', { message: `Scraping failed: ${error.message}` });
      throw error;
    }
  }

  async close(): Promise<void> {
    if (this.page) {
      await this.page.close();
      this.page = null;
    }

    if (this.browser) {
      await this.browser.close();
      this.browser = null;
    }

    if (this.db) {
      await this.db.close();
    }

    console.log('🔒 Datacenter Scraper closed');
  }
}
