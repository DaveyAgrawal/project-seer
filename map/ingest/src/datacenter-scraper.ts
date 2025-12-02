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
  private db: DatacenterDatabase | null = null;
  private progressCallback: ProgressCallback | null = null;

  // Anti-bot configuration
  private readonly userAgents = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
  ];

  private readonly baseDelay = 3000; // 3 seconds base delay (increased for production)
  private readonly maxDelay = 7000;  // 7 seconds max delay (increased for production)
  private readonly breakInterval = 100; // Take a break every 100 datacenters
  private readonly breakDelay = 30000; // 30-60 second break
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
   * Take a longer break every N datacenters (anti-bot measure)
   */
  private async periodicBreak(count: number): Promise<void> {
    if (count > 0 && count % this.breakInterval === 0) {
      const breakTime = this.breakDelay + Math.random() * 30000; // 30-60 seconds
      console.log(`\n☕ Taking a ${Math.round(breakTime / 1000)}s break after ${count} datacenters...`);
      await new Promise(resolve => setTimeout(resolve, breakTime));
      console.log('▶️  Resuming scraping...\n');
    }
  }

  /**
   * Get a random user agent
   */
  private getRandomUserAgent(): string {
    return this.userAgents[Math.floor(Math.random() * this.userAgents.length)];
  }

  /**
   * Discover all datacenter facility URLs from paginated listings
   * Site has 204 pages with 40 results per page (8129 total listings)
   */
  async discoverAllDatacenters(maxPages: number = 204): Promise<string[]> {
    if (!this.page) {
      throw new Error('Scraper not initialized. Call initialize() first.');
    }

    const facilityUrls: string[] = [];
    const startUrl = `${this.baseUrl}/locations`;
    const totalPages = Math.min(maxPages, 204); // Limit to requested pages or max 204
    const resultsPerPage = 40;

    console.log(`🔍 Starting discovery from ${startUrl}`);
    console.log(`📊 Scanning: ${totalPages} pages (max 204 total)`);
    this.sendProgress('discovering', { message: 'Starting to discover datacenters...' });

    try {
      // Iterate through requested pages
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
          await this.page.goto(pageUrl, { waitUntil: 'networkidle', timeout: 30000 });

          // Wait a bit for JavaScript to render
          await this.page.waitForTimeout(1000);
          await this.respectfulDelay();

          // Extract facility URLs from the page
          const pageContent = await this.page.content();
          const page$ = cheerio.load(pageContent);

          // Extract all facility links - they follow pattern: /provider-slug-datacenter-slug
          const facilityLinks: string[] = [];

          // Look for links that match datacenter detail page pattern
          page$('a[href^="/"]').each((_, element) => {
            const href = page$(element).attr('href');
            if (href &&
                !href.startsWith('/locations') &&
                !href.startsWith('/providers') &&
                !href.includes('?') &&
                href !== '/' &&
                href.split('/').length === 2 && // Only one path segment
                href.split('-').length >= 2) { // Has provider-datacenter format

              const fullUrl = `${this.baseUrl}${href}`;

              // Avoid duplicates
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
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      this.sendProgress('error', { message: `Discovery failed: ${errorMessage}` });
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

      // Navigate to facility page and wait for network to be idle (Next.js needs time to hydrate)
      await this.page.goto(facilityUrl, { waitUntil: 'networkidle', timeout: 30000 });

      // Additional wait for React hydration and script execution
      await this.page.waitForTimeout(2000);

      // Try to extract data directly from Next.js window object
      const nextData = await this.page.evaluate(() => {
        // @ts-ignore
        return window.__NEXT_DATA__;
      });

      if (nextData) {
        const datacenter = this.extractFromNextData(nextData, facilityUrl);
        if (datacenter) {
          return datacenter;
        }
      }

      // Fallback to HTML parsing
      const content = await this.page.content();
      const datacenter = this.extractDatacenterFields(content, facilityUrl);

      return datacenter;

    } catch (error) {
      console.error(`❌ Error scraping ${facilityUrl}:`, error);
      return null;
    }
  }

  /**
   * Extract datacenter data from Next.js __NEXT_DATA__ object
   */
  private extractFromNextData(nextData: any, facilityUrl: string): DatacenterData | null {
    try {
      // Navigate through Next.js data structure to find location data
      const pageProps = nextData?.props?.pageProps;
      if (!pageProps) {
        console.log('⚠️ No pageProps found in __NEXT_DATA__');
        return null;
      }

      // Try to find location data in various possible locations
      const location = pageProps.location || pageProps.data?.location || pageProps.initialData?.location;
      if (!location) {
        console.log('⚠️ No location data found in pageProps');
        return null;
      }

      console.log(`✓ Found location data in __NEXT_DATA__: ${location.name}`);

      const datacenter: DatacenterData = {
        facilityUrl,
        name: location.name || 'Unknown Facility',
        country: 'Unknown'
      };

      // Extract provider/operator
      const provider = pageProps.provider || location.provider;
      if (provider) {
        datacenter.operator = provider.name;
        if (provider.url) {
          datacenter.providerUrl = provider.url.startsWith('http')
            ? provider.url
            : `${this.baseUrl}${provider.url}`;
        }
        datacenter.phoneNumber = provider.supportNumber;
      }

      // Extract address components
      if (location.fullAddress) {
        datacenter.streetAddress = location.fullAddress;
      }

      const city = pageProps.city || location.city;
      const countryState = pageProps.countryState || location.countryState;
      const country = pageProps.country || location.country;

      datacenter.city = city?.name;
      datacenter.state = countryState?.name;
      datacenter.country = country?.name || 'Unknown';

      // Geographic coordinates - CRITICAL
      if (location.latitude != null && location.longitude != null) {
        datacenter.latitude = parseFloat(location.latitude);
        datacenter.longitude = parseFloat(location.longitude);
        console.log(`✓ Coordinates: ${datacenter.latitude}, ${datacenter.longitude}`);
      }

      // Technical specifications
      if (location.grossBuildingSize) {
        datacenter.squareFootage = parseFloat(location.grossBuildingSize);
      }
      if (location.totalPowerMw) {
        datacenter.powerCapacityMw = parseFloat(location.totalPowerMw);
      }
      datacenter.facilityType = location.type;

      // Certifications
      if (location.certifications && Array.isArray(location.certifications)) {
        datacenter.certifications = location.certifications
          .filter((cert: any) => cert.active)
          .map((cert: any) => cert.abbreviation);
      }

      // Features
      if (location.facilities && Array.isArray(location.facilities)) {
        const features: Record<string, boolean> = {};
        location.facilities.forEach((facility: any) => {
          const slug = facility.slug || facility.name.toLowerCase().replace(/\s+/g, '_');
          features[slug] = facility.live || false;
        });
        datacenter.features = Object.keys(features).length > 0 ? features : undefined;
      }

      // Additional metadata
      datacenter.milesToAirport = location.nearestAirport ? parseFloat(location.nearestAirport) : undefined;
      datacenter.marketRegion = location.marketLabel;

      // Media flags
      datacenter.hasImages = location.images && location.images.length > 0;
      datacenter.hasBrochures = location.locationBrochures && location.locationBrochures.length > 0;
      datacenter.hasMedia = datacenter.hasImages || datacenter.hasBrochures;

      // Breadcrumb hierarchy
      const breadcrumbs: string[] = [];
      if (datacenter.country && datacenter.country !== 'Unknown') breadcrumbs.push(datacenter.country);
      if (datacenter.state) breadcrumbs.push(datacenter.state);
      if (datacenter.city) breadcrumbs.push(datacenter.city);
      if (datacenter.name) breadcrumbs.push(datacenter.name);
      datacenter.breadcrumbHierarchy = breadcrumbs.length > 0 ? breadcrumbs : undefined;

      return datacenter;

    } catch (error) {
      console.error(`❌ Error extracting from __NEXT_DATA__:`, error);
      return null;
    }
  }

  /**
   * Extract all datacenter fields from the page HTML
   * Data is stored in embedded JSON within script tags
   */
  private extractDatacenterFields(htmlContent: string, facilityUrl: string): DatacenterData {
    const $ = cheerio.load(htmlContent);

    const datacenter: DatacenterData = {
      facilityUrl,
      name: '',
      country: 'Unknown',
      missingFields: []
    };

    try {
      // Find script tags containing latitude/longitude data using regex on raw HTML
      // Cheerio may not properly handle script tag content, so we search the raw HTML
      const scriptTagRegex = /<script[^>]*>([\s\S]*?)<\/script>/gi;
      let rawData: string | null = null;
      let match;

      while ((match = scriptTagRegex.exec(htmlContent)) !== null) {
        const scriptContent = match[1];

        if (scriptContent && scriptContent.includes('latitude') && scriptContent.includes('longitude')) {
          rawData = scriptContent;
          break;
        }
      }

      if (!rawData) {
        console.warn(`⚠️ No embedded data found for ${facilityUrl}, trying HTML extraction`);
        return this.extractFromHTML(htmlContent, facilityUrl);
      }

      // Type assertion: rawData is definitely a string at this point
      // Decode HTML entities that might be in the script content
      let scriptData: string = rawData
        .replace(/&quot;/g, '"')
        .replace(/&amp;/g, '&')
        .replace(/&lt;/g, '<')
        .replace(/&gt;/g, '>')
        .replace(/&#x27;/g, "'")
        .replace(/&#x2F;/g, '/');

      // Extract individual fields using regex patterns
      const extractField = (pattern: RegExp): string | undefined => {
        const match = scriptData.match(pattern);
        return match ? match[1] : undefined;
      };

      const extractNumber = (pattern: RegExp): number | undefined => {
        const match = scriptData.match(pattern);
        return match ? parseFloat(match[1]) : undefined;
      };

      // Extract basic information
      // Note: Field names in the data follow the pattern: fieldName\":value (with escaped quote)
      datacenter.name = extractField(/name\\":\\"([^"\\]+)\\"/) || 'Unknown Facility';

      // Extract operator
      datacenter.operator = extractField(/operatorName\\":\\"([^"\\]+)\\"/);

      // Extract address
      const address = extractField(/streetAddress1\\":\\"([^"\\]+)\\"/);
      if (address) {
        datacenter.streetAddress = address;
      }

      // Extract city, state, country
      datacenter.city = extractField(/city\\":\\"([^"\\]+)\\"/);
      datacenter.state = extractField(/state\\":\\"([^"\\]+)\\"/);
      datacenter.country = extractField(/country\\":\\"([^"\\]+)\\"/) || 'Unknown';

      // Geographic coordinates - CRITICAL for map display
      datacenter.latitude = extractNumber(/latitude\\":([\d\.\-]+)/);
      datacenter.longitude = extractNumber(/longitude\\":([\d\.\-]+)/);

      // Technical specifications
      const squareFootage = extractNumber(/"grossBuildingSize":"?([\d\.]+)"?/);
      if (squareFootage && squareFootage > 0) {
        datacenter.squareFootage = squareFootage;
      }

      datacenter.facilityType = extractField(/"type":"([^"]+)"/);

      const powerMw = extractNumber(/"totalPowerMw":"?([\d\.]+)"?/);
      if (powerMw && powerMw > 0) {
        datacenter.powerCapacityMw = powerMw;
      }

      // Certifications - extract from certifications array
      const certsMatch = scriptData.match(/"certifications":\[([^\]]+)\]/);
      if (certsMatch) {
        const certAbbrs = [...certsMatch[1].matchAll(/"abbreviation":"([^"]+)"/g)];
        if (certAbbrs.length > 0) {
          datacenter.certifications = certAbbrs.map(m => m[1]);
        }
      }

      // Features - extract facility types
      const facilitiesMatch = scriptData.match(/"facilities":\[([^\]]+)\]/);
      if (facilitiesMatch) {
        const features: Record<string, boolean> = {};
        const facilityNames = [...facilitiesMatch[1].matchAll(/"name":"([^"]+)"/g)];
        facilityNames.forEach(match => {
          const slug = match[1].toLowerCase().replace(/\s+/g, '_');
          features[slug] = true;
        });
        if (Object.keys(features).length > 0) {
          datacenter.features = features;
        }
      }

      // Provider URL
      const providerUrlMatch = scriptData.match(/"provider":\{[^}]*"url":"([^"]+)"/);
      if (providerUrlMatch) {
        datacenter.providerUrl = providerUrlMatch[1].startsWith('http')
          ? providerUrlMatch[1]
          : `${this.baseUrl}${providerUrlMatch[1]}`;
      }

      // Breadcrumb hierarchy
      const breadcrumbs: string[] = [];
      if (datacenter.country && datacenter.country !== 'Unknown') breadcrumbs.push(datacenter.country);
      if (datacenter.state) breadcrumbs.push(datacenter.state);
      if (datacenter.city) breadcrumbs.push(datacenter.city);
      if (datacenter.name) breadcrumbs.push(datacenter.name);
      datacenter.breadcrumbHierarchy = breadcrumbs.length > 0 ? breadcrumbs : undefined;

      // Market region
      const marketLabel = extractField(/"marketLabel":"([^"]+)"/);
      datacenter.marketRegion = marketLabel ||
        (datacenter.city && datacenter.state
          ? `${datacenter.city}, ${datacenter.state}`
          : datacenter.city);

      // Miles to airport
      datacenter.milesToAirport = extractNumber(/"nearestAirport":"?([\d\.]+)"?/);

      // Phone number
      const phoneMatch = scriptData.match(/"supportNumber":"([^"]+)"|"phone":"([^"]+)"/);
      datacenter.phoneNumber = phoneMatch ? (phoneMatch[1] || phoneMatch[2]) : undefined;

      // Media availability flags
      datacenter.hasImages = scriptData.includes('"images":[') && !scriptData.includes('"images":[]');
      datacenter.hasBrochures = scriptData.includes('"locationBrochures":[') && !scriptData.includes('"locationBrochures":[]');
      datacenter.hasMedia = datacenter.hasImages || datacenter.hasBrochures;

      // Nearby datacenter count - try to extract from page text
      const bodyText = $('body').text();
      const nearbyMatch = bodyText.match(/(\d+)\s+facilities?\s+within\s+50\s+miles/i);
      if (nearbyMatch) {
        datacenter.nearbyDatacenterCount = parseInt(nearbyMatch[1], 10);
      }

    } catch (error) {
      console.error(`❌ Error extracting JSON data for ${facilityUrl}:`, error);
      return this.extractFromHTML(htmlContent, facilityUrl);
    }

    return datacenter;
  }

  /**
   * Fallback: Extract from HTML when JSON is not available
   */
  private extractFromHTML(htmlContent: string, facilityUrl: string): DatacenterData {
    const $ = cheerio.load(htmlContent);

    const datacenter: DatacenterData = {
      facilityUrl,
      name: $('h1').first().text().trim() || 'Unknown Facility',
      country: 'Unknown'
    };

    // Basic extraction from HTML as fallback
    datacenter.operator = $('.provider-name, .operator').first().text().trim() || undefined;
    datacenter.streetAddress = $('.address').first().text().trim() || undefined;

    // Try to extract lat/lng from meta tags or data attributes
    const latLng = this.extractLatLng($);
    if (latLng) {
      datacenter.latitude = latLng.lat;
      datacenter.longitude = latLng.lng;
    }

    return datacenter;
  }

  /**
   * Extract latitude and longitude from page
   */
  private extractLatLng($: ReturnType<typeof cheerio.load>): { lat: number; lng: number } | null {
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

              // Take periodic breaks to avoid anti-bot measures
              await this.periodicBreak(stats.processed);
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
      const errorMessage = error instanceof Error ? error.message : 'Unknown error';
      this.sendProgress('error', { message: `Scraping failed: ${errorMessage}` });
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
