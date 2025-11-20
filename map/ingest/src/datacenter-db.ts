import { PoolClient } from 'pg';
import { DatabaseManager } from './db';
import * as fs from 'fs';
import * as path from 'path';
import * as crypto from 'crypto';

export interface DatacenterData {
  // Unique identifiers
  facilityUrl: string;                    // Required: Full URL of facility detail page (unique key)
  providerUrl?: string;                   // URL to operator's parent/provider page

  // Basic information
  name: string;                           // Required: Facility name
  operator?: string;                      // Data center operator/company
  streetAddress?: string;                 // Full street address
  city?: string;                          // City name
  state?: string;                         // State/province
  country: string;                        // Required: Country name

  // Geographic data
  latitude?: number;                      // Latitude coordinate
  longitude?: number;                     // Longitude coordinate

  // Technical specifications
  squareFootage?: number;                 // Facility size in square feet
  facilityType?: string;                  // Type: colocation, hyperscale, enterprise, etc.
  powerCapacityMw?: number;               // Power capacity in megawatts

  // Certifications and features
  certifications?: string[];              // Array of certifications
  features?: Record<string, boolean>;     // Object with boolean feature flags

  // Proximity and regional data
  milesToAirport?: number;                // Distance to nearest airport
  breadcrumbHierarchy?: string[];         // Array of location hierarchy
  marketRegion?: string;                  // Labeled market name
  nearbyDatacenterCount?: number;         // Number of facilities within 50 miles

  // Contact information
  phoneNumber?: string;                   // Published phone number

  // Media availability flags
  hasImages?: boolean;                    // Facility page includes images
  hasBrochures?: boolean;                 // Brochure/PDF downloads available
  hasMedia?: boolean;                     // Media tab/gallery available

  // Data quality tracking
  missingFields?: string[];               // Array of field names with missing data
}

export interface DatacenterInsertResult {
  inserted: boolean;
  updated: boolean;
  internalId: string;
  facilityUrl: string;
}

export interface DatacenterDatabaseOptions {
  updateExisting?: boolean;
  batchSize?: number;
}

export class DatacenterDatabase {
  private db: DatabaseManager;

  constructor(db?: DatabaseManager) {
    this.db = db || new DatabaseManager();
  }

  async initialize(): Promise<void> {
    console.log('🔧 Initializing Datacenter database schema...');

    // Check if datacenter table exists
    const hasDatacenterTable = await this.checkDatacenterSchema();

    if (!hasDatacenterTable) {
      console.log('📋 Applying datacenter database schema...');
      await this.applyDatacenterSchema();
    } else {
      console.log('✅ Datacenter schema already applied');
    }
  }

  private async checkDatacenterSchema(): Promise<boolean> {
    try {
      const result = await this.db.query(`
        SELECT table_name
        FROM information_schema.tables
        WHERE table_name = 'datacenter_listings'
      `);
      return result.rows.length > 0;
    } catch (error) {
      return false;
    }
  }

  private async applyDatacenterSchema(): Promise<void> {
    const schemaPath = path.join(__dirname, 'schema', 'datacenter-schema.sql');

    if (!fs.existsSync(schemaPath)) {
      throw new Error(`Datacenter schema file not found: ${schemaPath}`);
    }

    const schemaSql = fs.readFileSync(schemaPath, 'utf-8');

    try {
      await this.db.query(schemaSql);
      console.log('✅ Datacenter schema applied successfully');
    } catch (error: any) {
      console.error('❌ Error applying datacenter schema:', error);
      throw error;
    }
  }

  /**
   * Generate a unique internal ID from the facility URL
   */
  private generateInternalId(facilityUrl: string): string {
    return crypto.createHash('sha256').update(facilityUrl).digest('hex').substring(0, 16);
  }

  /**
   * Determine if a datacenter is in the US based on country
   */
  private isUSDatacenter(country: string): boolean {
    const usVariants = ['USA', 'United States', 'US', 'United States of America', 'U.S.', 'U.S.A.'];
    return usVariants.some(variant =>
      country.toLowerCase() === variant.toLowerCase()
    );
  }

  /**
   * Identify missing fields for data quality tracking
   */
  private identifyMissingFields(data: DatacenterData): string[] {
    const missingFields: string[] = [];

    const optionalFields: (keyof DatacenterData)[] = [
      'operator', 'streetAddress', 'city', 'state',
      'latitude', 'longitude', 'squareFootage', 'facilityType',
      'powerCapacityMw', 'milesToAirport', 'phoneNumber'
    ];

    for (const field of optionalFields) {
      if (data[field] === undefined || data[field] === null || data[field] === '') {
        missingFields.push(field);
      }
    }

    return missingFields;
  }

  async storeDatacenter(
    datacenterData: DatacenterData,
    options: DatacenterDatabaseOptions = {}
  ): Promise<DatacenterInsertResult> {
    const internalId = this.generateInternalId(datacenterData.facilityUrl);
    const isUS = this.isUSDatacenter(datacenterData.country);
    const missingFields = this.identifyMissingFields(datacenterData);

    const result: DatacenterInsertResult = {
      inserted: false,
      updated: false,
      internalId,
      facilityUrl: datacenterData.facilityUrl
    };

    return await this.db.transaction(async (client: PoolClient) => {
      // Check if datacenter exists
      const existing = await this.getDatacenterByUrl(datacenterData.facilityUrl);

      if (existing && !options.updateExisting) {
        console.log(`⏭️  Datacenter ${datacenterData.name} already exists, skipping...`);
        return result;
      }

      // Insert or update datacenter
      try {
        const query = `
          INSERT INTO datacenter_listings (
            internal_id, facility_url, provider_url,
            name, operator, street_address, city, state, country, is_us,
            latitude, longitude,
            square_footage, facility_type, power_capacity_mw,
            certifications, features,
            miles_to_airport, breadcrumb_hierarchy, market_region, nearby_datacenter_count,
            phone_number,
            has_images, has_brochures, has_media,
            missing_fields
          ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26
          )
          ON CONFLICT (facility_url) DO UPDATE SET
            internal_id = EXCLUDED.internal_id,
            provider_url = EXCLUDED.provider_url,
            name = EXCLUDED.name,
            operator = EXCLUDED.operator,
            street_address = EXCLUDED.street_address,
            city = EXCLUDED.city,
            state = EXCLUDED.state,
            country = EXCLUDED.country,
            is_us = EXCLUDED.is_us,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            square_footage = EXCLUDED.square_footage,
            facility_type = EXCLUDED.facility_type,
            power_capacity_mw = EXCLUDED.power_capacity_mw,
            certifications = EXCLUDED.certifications,
            features = EXCLUDED.features,
            miles_to_airport = EXCLUDED.miles_to_airport,
            breadcrumb_hierarchy = EXCLUDED.breadcrumb_hierarchy,
            market_region = EXCLUDED.market_region,
            nearby_datacenter_count = EXCLUDED.nearby_datacenter_count,
            phone_number = EXCLUDED.phone_number,
            has_images = EXCLUDED.has_images,
            has_brochures = EXCLUDED.has_brochures,
            has_media = EXCLUDED.has_media,
            missing_fields = EXCLUDED.missing_fields,
            updated_at = NOW()
          RETURNING id
        `;

        const values = [
          internalId,
          datacenterData.facilityUrl,
          datacenterData.providerUrl || null,
          datacenterData.name,
          datacenterData.operator || null,
          datacenterData.streetAddress || null,
          datacenterData.city || null,
          datacenterData.state || null,
          datacenterData.country,
          isUS,
          datacenterData.latitude || null,
          datacenterData.longitude || null,
          datacenterData.squareFootage || null,
          datacenterData.facilityType || null,
          datacenterData.powerCapacityMw || null,
          JSON.stringify(datacenterData.certifications || []),
          JSON.stringify(datacenterData.features || {}),
          datacenterData.milesToAirport || null,
          JSON.stringify(datacenterData.breadcrumbHierarchy || []),
          datacenterData.marketRegion || null,
          datacenterData.nearbyDatacenterCount || null,
          datacenterData.phoneNumber || null,
          datacenterData.hasImages || false,
          datacenterData.hasBrochures || false,
          datacenterData.hasMedia || false,
          JSON.stringify(missingFields)
        ];

        await client.query(query, values);

        if (existing) {
          result.updated = true;
          console.log(`🔄 Updated datacenter: ${datacenterData.name} (${datacenterData.country})`);
        } else {
          result.inserted = true;
          console.log(`✅ Stored datacenter: ${datacenterData.name} (${datacenterData.country})`);
        }

      } catch (error) {
        console.error(`❌ Error storing datacenter ${datacenterData.name}:`, error);
        throw error;
      }

      return result;
    });
  }

  async getDatacenterByUrl(facilityUrl: string): Promise<any | null> {
    const result = await this.db.query(
      'SELECT * FROM datacenter_listings WHERE facility_url = $1',
      [facilityUrl]
    );
    return result.rows[0] || null;
  }

  async getDatacenterByInternalId(internalId: string): Promise<any | null> {
    const result = await this.db.query(
      'SELECT * FROM datacenter_listings WHERE internal_id = $1',
      [internalId]
    );
    return result.rows[0] || null;
  }

  async datacenterExists(facilityUrl: string): Promise<boolean> {
    const result = await this.db.query(
      'SELECT 1 FROM datacenter_listings WHERE facility_url = $1',
      [facilityUrl]
    );
    return result.rows.length > 0;
  }

  async getActiveDatacenters(usOnly: boolean = false): Promise<any[]> {
    const whereClause = usOnly ? 'WHERE is_us = true' : '';
    const result = await this.db.query(`
      SELECT
        internal_id,
        facility_url,
        name,
        operator,
        city,
        state,
        country,
        latitude,
        longitude,
        power_capacity_mw,
        facility_type,
        certifications,
        features,
        ST_AsGeoJSON(geom) as geometry
      FROM datacenter_listings
      ${whereClause}
      ORDER BY name
    `);
    return result.rows;
  }

  async getDatacenterStats(): Promise<any> {
    const result = await this.db.query(`
      SELECT
        COUNT(*) as total_datacenters,
        COUNT(CASE WHEN is_us = true THEN 1 END) as us_datacenters,
        COUNT(CASE WHEN is_us = false THEN 1 END) as international_datacenters,
        COUNT(DISTINCT country) as countries_covered,
        COUNT(DISTINCT state) as states_covered,
        COUNT(DISTINCT operator) as unique_operators,
        SUM(COALESCE(power_capacity_mw, 0)) as total_power_capacity_mw,
        AVG(COALESCE(power_capacity_mw, 0)) as avg_power_capacity_mw,
        COUNT(CASE WHEN latitude IS NOT NULL AND longitude IS NOT NULL THEN 1 END) as geocoded_count,
        COUNT(CASE WHEN array_length(missing_fields::text[]::text[], 1) > 5 THEN 1 END) as low_quality_count
      FROM datacenter_listings
    `);
    return result.rows[0];
  }

  async getCountryStats(): Promise<any[]> {
    const result = await this.db.query(`
      SELECT
        country,
        is_us,
        COUNT(*) as datacenter_count,
        COUNT(DISTINCT operator) as unique_operators,
        SUM(COALESCE(power_capacity_mw, 0)) as total_power_mw,
        AVG(COALESCE(power_capacity_mw, 0)) as avg_power_mw,
        COUNT(CASE WHEN latitude IS NOT NULL THEN 1 END) as geocoded_count
      FROM datacenter_listings
      GROUP BY country, is_us
      ORDER BY datacenter_count DESC
    `);
    return result.rows;
  }

  async getOperatorStats(): Promise<any[]> {
    const result = await this.db.query(`
      SELECT
        operator,
        COUNT(*) as facility_count,
        COUNT(DISTINCT country) as countries,
        SUM(COALESCE(power_capacity_mw, 0)) as total_power_mw,
        COUNT(CASE WHEN is_us = true THEN 1 END) as us_facilities,
        COUNT(CASE WHEN is_us = false THEN 1 END) as international_facilities
      FROM datacenter_listings
      WHERE operator IS NOT NULL
      GROUP BY operator
      ORDER BY facility_count DESC
      LIMIT 50
    `);
    return result.rows;
  }

  async optimizeDatabase(): Promise<void> {
    console.log('🔧 Optimizing Datacenter database performance...');
    await this.db.analyze('datacenter_listings');
    console.log('✅ Database optimization completed');
  }

  async close(): Promise<void> {
    await this.db.close();
  }
}
