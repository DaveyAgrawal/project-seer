// Basic EnergyNet types and interfaces for multi-listing scraper compatibility

import { LandParcelCollection } from './gis-processor';

export interface EnergyNetListing {
  id: string;                    // Listing ID (e.g., "6403")
  title: string;                 // Full listing title
  state: string;                 // State/region
  url: string;                   // View listing URL
  description?: string;          // Additional description
  gisDownloadUrl?: string;       // GIS download URL
  gisFilePath?: string;          // Local path to downloaded GIS file
  parcels?: LandParcelCollection; // Processed parcel data
}

export interface EnergyNetScrapingOptions {
  downloadDir?: string;
  skipExisting?: boolean;
  validateGeometry?: boolean;
  respectfulDelayMs?: number;
}

export interface EnergyNetScrapingResult {
  success: boolean;
  listingId: string;
  parcelsFound: number;
  errors?: string[];
  downloadPath?: string;
}

// Placeholder for backward compatibility
// In a full implementation, this would handle individual listing scraping
export class EnergyNetScraper {
  constructor(private options: EnergyNetScrapingOptions = {}) {}

  async scrapeListing(listingId: string): Promise<EnergyNetScrapingResult> {
    // This would be implemented with the original scraping logic
    // For now, return a placeholder result
    return {
      success: false,
      listingId,
      parcelsFound: 0,
      errors: ['Individual listing scraping not implemented - use multi-listing scraper']
    };
  }
}