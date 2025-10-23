// Basic EnergyNet database operations - compatibility layer

import { PoolClient } from 'pg';
import { DatabaseManager } from './db';
import { EnergyNetListing } from './energynet';
import { LandParcelCollection } from './gis-processor';

export interface EnergyNetDatabaseOptions {
  updateExisting?: boolean;
  batchSize?: number;
  skipGeometryValidation?: boolean;
}

export interface InsertionResult {
  listingInserted: boolean;
  parcelsInserted: number;
  parcelsSkipped: number;
  parcelsErrors: number;
}

// Basic database operations for backward compatibility
export class EnergyNetDatabase {
  private db: DatabaseManager;

  constructor(db?: DatabaseManager) {
    this.db = db || new DatabaseManager();
  }

  async initialize(): Promise<void> {
    console.log('🔧 Initializing basic EnergyNet database schema...');
    // Basic initialization - the multi-listing database will handle the full schema
    console.log('✅ Basic EnergyNet database ready');
  }

  async storeListing(
    listing: EnergyNetListing, 
    options: EnergyNetDatabaseOptions = {}
  ): Promise<InsertionResult> {
    // Placeholder - actual storage would be handled by multi-listing database
    return {
      listingInserted: false,
      parcelsInserted: 0,
      parcelsSkipped: 0,
      parcelsErrors: 0
    };
  }

  async listingExists(listingId: string): Promise<boolean> {
    try {
      const result = await this.db.query(
        'SELECT 1 FROM energynet_listings WHERE listing_id = $1',
        [listingId]
      );
      return result.rows.length > 0;
    } catch (error) {
      return false;
    }
  }

  async close(): Promise<void> {
    await this.db.close();
  }
}