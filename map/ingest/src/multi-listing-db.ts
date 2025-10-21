import { PoolClient } from 'pg';
import { DatabaseManager } from './db';
import { LandParcelCollection, LandParcelFeature } from './gis-processor';
import * as fs from 'fs';
import * as path from 'path';

export interface MultiListingData {
  saleGroup: string;           // e.g., 'GEONV-2025-Q4'
  listingId: string;          // e.g., '6391'
  title: string;              // Full listing title
  region: string;             // e.g., 'Nevada'
  listingType: string;        // e.g., 'Geothermal', 'Oil & Gas Lease'
  agency: string;             // e.g., 'BLM Nevada State Office'
  saleStartDate?: Date;       // Bidding start date
  saleEndDate?: Date;         // Bidding end date
  status: 'active' | 'expired';
  url: string;               // View Listing URL
  gisDownloadUrl?: string;   // GIS download URL
  description?: string;      // Additional description
  parcels?: LandParcelCollection; // Processed parcel data
}

export interface MultiListingInsertResult {
  listingInserted: boolean;
  listingUpdated: boolean;
  parcelsInserted: number;
  parcelsSkipped: number;
  parcelsErrors: number;
  saleGroup: string;
  region: string;
}

export interface MultiListingDatabaseOptions {
  updateExisting?: boolean;
  batchSize?: number;
  skipGeometryValidation?: boolean;
  markExpiredListings?: boolean;
}

export class MultiListingDatabase {
    private db: DatabaseManager;

    constructor(db?: DatabaseManager) {
        this.db = db || new DatabaseManager();
    }

    async initialize(): Promise<void> {
        console.log('🔧 Initializing Multi-Listing EnergyNet database schema...');
        
        // Check if enhanced tables exist
        const hasMultiColumns = await this.checkMultiListingSchema();
        
        if (!hasMultiColumns) {
            console.log('📋 Applying multi-listing database schema...');
            await this.applyMultiListingSchema();
        } else {
            console.log('✅ Multi-listing schema already applied');
        }
    }

    private async checkMultiListingSchema(): Promise<boolean> {
        try {
            const result = await this.db.query(`
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'energynet_listings' 
                AND column_name = 'sale_group'
            `);
            return result.rows.length > 0;
        } catch (error) {
            return false;
        }
    }

    private async applyMultiListingSchema(): Promise<void> {
        const schemaPath = path.join(__dirname, 'schema', 'energynet-multi-listing-schema.sql');
        
        if (!fs.existsSync(schemaPath)) {
            throw new Error(`Multi-listing schema file not found: ${schemaPath}`);
        }

        const schemaSql = fs.readFileSync(schemaPath, 'utf-8');
        
        try {
            await this.db.query(schemaSql);
            console.log('✅ Multi-listing schema applied successfully');
        } catch (error: any) {
            console.error('❌ Error applying multi-listing schema:', error);
            throw error;
        }
    }

    async storeMultiListing(
        listingData: MultiListingData,
        options: MultiListingDatabaseOptions = {}
    ): Promise<MultiListingInsertResult> {
        const result: MultiListingInsertResult = {
            listingInserted: false,
            listingUpdated: false,
            parcelsInserted: 0,
            parcelsSkipped: 0,
            parcelsErrors: 0,
            saleGroup: listingData.saleGroup,
            region: listingData.region
        };

        return await this.db.transaction(async (client: PoolClient) => {
            // Check if listing exists
            const existingListing = await this.getListingBySaleGroup(listingData.saleGroup);
            
            if (existingListing && !options.updateExisting) {
                console.log(`⏭️  Listing ${listingData.saleGroup} already exists, skipping...`);
                return result;
            }

            // Insert or update listing
            if (existingListing) {
                result.listingUpdated = await this.updateListing(client, listingData);
            } else {
                result.listingInserted = await this.insertListing(client, listingData);
            }

            // Insert parcels if available
            if (listingData.parcels && listingData.parcels.features.length > 0) {
                console.log(`📍 Storing ${listingData.parcels.features.length} land parcels for ${listingData.saleGroup}...`);
                
                const parcelResult = await this.insertParcels(
                    client,
                    listingData,
                    listingData.parcels,
                    options
                );
                
                result.parcelsInserted = parcelResult.inserted;
                result.parcelsSkipped = parcelResult.skipped;
                result.parcelsErrors = parcelResult.errors;

                // Update listing with parcel statistics
                await this.updateListingStats(client, listingData.saleGroup, parcelResult.inserted, listingData.parcels);
            }

            return result;
        });
    }

    private async insertListing(
        client: PoolClient,
        listingData: MultiListingData
    ): Promise<boolean> {
        try {
            const query = `
                INSERT INTO energynet_listings (
                    listing_id, title, state, region, sale_group, listing_type, agency,
                    sale_start_date, sale_end_date, status, description, url, gis_download_url,
                    last_scraped_at, props
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, NOW(), $14)
                ON CONFLICT (sale_group) DO UPDATE SET
                    title = EXCLUDED.title,
                    state = EXCLUDED.state,
                    region = EXCLUDED.region,
                    listing_type = EXCLUDED.listing_type,
                    agency = EXCLUDED.agency,
                    sale_start_date = EXCLUDED.sale_start_date,
                    sale_end_date = EXCLUDED.sale_end_date,
                    status = EXCLUDED.status,
                    description = EXCLUDED.description,
                    url = EXCLUDED.url,
                    gis_download_url = EXCLUDED.gis_download_url,
                    last_scraped_at = NOW(),
                    updated_at = NOW()
                RETURNING id
            `;

            const values = [
                listingData.listingId,
                listingData.title,
                listingData.region, // Use region as state for now
                listingData.region,
                listingData.saleGroup,
                listingData.listingType,
                listingData.agency,
                listingData.saleStartDate || null,
                listingData.saleEndDate || null,
                listingData.status,
                listingData.description || `${listingData.listingType} - ${listingData.agency}`,
                listingData.url,
                listingData.gisDownloadUrl || null,
                JSON.stringify({
                    scraped_at: new Date().toISOString(),
                    multi_listing: true,
                    sale_group: listingData.saleGroup
                })
            ];

            const queryResult = await client.query(query, values);
            console.log(`✅ Stored multi-listing: ${listingData.saleGroup} (${listingData.region})`);
            return true;

        } catch (error) {
            console.error(`❌ Error inserting multi-listing ${listingData.saleGroup}:`, error);
            throw error;
        }
    }

    private async updateListing(
        client: PoolClient,
        listingData: MultiListingData
    ): Promise<boolean> {
        try {
            const query = `
                UPDATE energynet_listings 
                SET 
                    title = $2,
                    state = $3,
                    region = $4,
                    listing_type = $5,
                    agency = $6,
                    sale_start_date = $7,
                    sale_end_date = $8,
                    status = $9,
                    description = $10,
                    url = $11,
                    gis_download_url = $12,
                    last_scraped_at = NOW(),
                    updated_at = NOW()
                WHERE sale_group = $1
            `;

            const values = [
                listingData.saleGroup,
                listingData.title,
                listingData.region,
                listingData.region,
                listingData.listingType,
                listingData.agency,
                listingData.saleStartDate || null,
                listingData.saleEndDate || null,
                listingData.status,
                listingData.description || `${listingData.listingType} - ${listingData.agency}`,
                listingData.url,
                listingData.gisDownloadUrl || null
            ];

            await client.query(query, values);
            console.log(`🔄 Updated multi-listing: ${listingData.saleGroup} (${listingData.region})`);
            return true;

        } catch (error) {
            console.error(`❌ Error updating multi-listing ${listingData.saleGroup}:`, error);
            throw error;
        }
    }

    private async insertParcels(
        client: PoolClient,
        listingData: MultiListingData,
        parcels: LandParcelCollection,
        options: MultiListingDatabaseOptions
    ): Promise<{ inserted: number; skipped: number; errors: number }> {
        const batchSize = options.batchSize || 1000;
        let inserted = 0;
        let skipped = 0;
        let errors = 0;

        // Process in batches for better performance
        for (let i = 0; i < parcels.features.length; i += batchSize) {
            const batch = parcels.features.slice(i, i + batchSize);
            console.log(`📦 Processing parcel batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(parcels.features.length / batchSize)} for ${listingData.saleGroup}`);

            const batchResult = await this.insertParcelBatch(client, listingData, batch, options);
            inserted += batchResult.inserted;
            skipped += batchResult.skipped;
            errors += batchResult.errors;
        }

        return { inserted, skipped, errors };
    }

    private async insertParcelBatch(
        client: PoolClient,
        listingData: MultiListingData,
        parcels: LandParcelFeature[],
        options: MultiListingDatabaseOptions
    ): Promise<{ inserted: number; skipped: number; errors: number }> {
        let inserted = 0;
        let skipped = 0;
        let errors = 0;

        for (const parcel of parcels) {
            try {
                // Validate geometry if requested
                if (!options.skipGeometryValidation && !this.isValidGeometry(parcel)) {
                    console.warn(`⚠️ Invalid geometry for parcel ${parcel.properties.parcel_id}, skipping`);
                    skipped++;
                    continue;
                }

                const query = `
                    INSERT INTO energynet_parcels (
                        listing_id, parcel_id, sale_group, state, region, acres, description, props, geom
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, ST_GeomFromGeoJSON($9))
                    ON CONFLICT (listing_id, parcel_id) DO UPDATE SET
                        sale_group = EXCLUDED.sale_group,
                        state = EXCLUDED.state,
                        region = EXCLUDED.region,
                        acres = EXCLUDED.acres,
                        description = EXCLUDED.description,
                        props = EXCLUDED.props,
                        geom = EXCLUDED.geom
                `;

                const values = [
                    listingData.listingId,
                    parcel.properties.parcel_id,
                    listingData.saleGroup,
                    parcel.properties.state || listingData.region,
                    listingData.region,
                    parcel.properties.acres,
                    parcel.properties.description,
                    JSON.stringify({
                        ...parcel.properties,
                        // Remove our standard properties to avoid duplication
                        listing_id: undefined,
                        parcel_id: undefined,
                        sale_group: undefined,
                        state: undefined,
                        region: undefined,
                        acres: undefined,
                        description: undefined
                    }),
                    JSON.stringify(parcel.geometry)
                ];

                await client.query(query, values);
                inserted++;

            } catch (error: any) {
                console.error(`❌ Error inserting parcel ${parcel.properties.parcel_id}:`, error.message);
                errors++;
            }
        }

        return { inserted, skipped, errors };
    }

    private async updateListingStats(
        client: PoolClient,
        saleGroup: string,
        parcelCount: number,
        parcels: LandParcelCollection
    ): Promise<void> {
        try {
            const totalAcres = parcels.features.reduce((sum, feature) => {
                return sum + (feature.properties.acres || 0);
            }, 0);

            await client.query(`
                UPDATE energynet_listings 
                SET 
                    parcel_count = $2,
                    total_acres = $3,
                    is_processed = true
                WHERE sale_group = $1
            `, [saleGroup, parcelCount, totalAcres]);

        } catch (error) {
            console.error(`❌ Error updating listing stats for ${saleGroup}:`, error);
        }
    }

    private isValidGeometry(parcel: LandParcelFeature): boolean {
        try {
            const geom = parcel.geometry;
            
            if (!geom || !geom.type || !geom.coordinates) {
                return false;
            }
            
            if (!['Polygon', 'MultiPolygon'].includes(geom.type)) {
                return false;
            }
            
            if (geom.type === 'Polygon') {
                const coordinates = geom.coordinates as number[][][];
                return coordinates.length > 0 && 
                       coordinates[0].length >= 4 && 
                       Array.isArray(coordinates[0][0]) &&
                       coordinates[0][0].length === 2;
            }
            
            return true;
            
        } catch (error) {
            return false;
        }
    }

    // Multi-listing specific query methods
    async getListingBySaleGroup(saleGroup: string): Promise<any | null> {
        const result = await this.db.query(
            'SELECT * FROM energynet_listings WHERE sale_group = $1',
            [saleGroup]
        );
        return result.rows[0] || null;
    }

    async saleGroupExists(saleGroup: string): Promise<boolean> {
        const result = await this.db.query(
            'SELECT 1 FROM energynet_listings WHERE sale_group = $1',
            [saleGroup]
        );
        return result.rows.length > 0;
    }

    async markExpiredListings(activeSaleGroups: string[]): Promise<number> {
        if (activeSaleGroups.length === 0) {
            return 0;
        }

        const placeholders = activeSaleGroups.map((_, i) => `$${i + 1}`).join(',');
        const query = `
            UPDATE energynet_listings 
            SET 
                status = 'expired',
                updated_at = NOW()
            WHERE 
                status = 'active' 
                AND sale_group NOT IN (${placeholders})
        `;

        const result = await this.db.query(query, activeSaleGroups);
        const expiredCount = result.rowCount || 0;
        
        if (expiredCount > 0) {
            console.log(`🔄 Marked ${expiredCount} listings as expired`);
        }
        
        return expiredCount;
    }

    async getActiveListings(): Promise<any[]> {
        const result = await this.db.query(`
            SELECT 
                sale_group,
                listing_id,
                title,
                region,
                listing_type,
                agency,
                sale_start_date,
                sale_end_date,
                parcel_count,
                total_acres,
                url,
                last_scraped_at,
                is_processed
            FROM energynet_listings 
            WHERE status = 'active'
            ORDER BY last_scraped_at DESC
        `);
        return result.rows;
    }

    async getMultiListingStats(): Promise<any> {
        const result = await this.db.query(`
            SELECT 
                COUNT(DISTINCT l.sale_group) as total_sale_groups,
                COUNT(DISTINCT l.listing_id) as total_listings,
                COUNT(DISTINCT l.region) as regions_covered,
                SUM(COALESCE(l.parcel_count, 0)) as total_parcels,
                SUM(COALESCE(l.total_acres, 0)) as total_acres,
                COUNT(CASE WHEN l.status = 'active' THEN 1 END) as active_listings,
                COUNT(CASE WHEN l.status = 'expired' THEN 1 END) as expired_listings,
                COUNT(CASE WHEN l.is_processed = true THEN 1 END) as processed_listings,
                MIN(l.sale_start_date) as earliest_sale,
                MAX(l.sale_end_date) as latest_sale
            FROM energynet_listings l
        `);
        return result.rows[0];
    }

    async getRegionalStats(): Promise<any[]> {
        const result = await this.db.query(`
            SELECT 
                l.region,
                l.listing_type,
                COUNT(DISTINCT l.sale_group) as sale_groups,
                COUNT(DISTINCT l.listing_id) as listings,
                SUM(COALESCE(l.parcel_count, 0)) as parcels,
                SUM(COALESCE(l.total_acres, 0)) as total_acres,
                COUNT(CASE WHEN l.status = 'active' THEN 1 END) as active_count,
                COUNT(CASE WHEN l.status = 'expired' THEN 1 END) as expired_count,
                COUNT(CASE WHEN l.is_processed = true THEN 1 END) as processed_count,
                MIN(l.sale_start_date) as earliest_sale,
                MAX(l.sale_end_date) as latest_sale
            FROM energynet_listings l
            GROUP BY l.region, l.listing_type
            ORDER BY total_acres DESC NULLS LAST
        `);
        return result.rows;
    }

    async optimizeDatabase(): Promise<void> {
        console.log('🔧 Optimizing Multi-Listing EnergyNet database performance...');
        
        await this.db.analyze('energynet_listings');
        await this.db.analyze('energynet_parcels');
        
        console.log('✅ Database optimization completed');
    }

    async close(): Promise<void> {
        await this.db.close();
    }
}