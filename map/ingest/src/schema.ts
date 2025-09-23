import { DatabaseManager } from './db';

export class SchemaManager {
  constructor(private db: DatabaseManager) {}

  async ensureGeothermalPointsTable(tableName: string): Promise<void> {
    console.log(`🏗️  Ensuring geothermal points table: ${tableName}`);
    
    const sql = `
      CREATE TABLE IF NOT EXISTS ${tableName} (
        gid BIGSERIAL PRIMARY KEY,
        latitude    DOUBLE PRECISION NOT NULL,
        longitude   DOUBLE PRECISION NOT NULL,
        depth_m     DOUBLE PRECISION,         -- meters below surface (positive)
        temperature_f DOUBLE PRECISION,       -- Fahrenheit
        geom geometry(POINT, 4326) GENERATED ALWAYS AS (
          ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
        ) STORED
      )
    `;
    
    await this.db.query(sql);
    
    // Create indexes
    await this.db.query(`
      CREATE UNIQUE INDEX IF NOT EXISTS ${tableName}_uniq 
      ON ${tableName} (latitude, longitude, COALESCE(depth_m, 0))
    `);
    
    await this.db.query(`
      CREATE INDEX IF NOT EXISTS ${tableName}_gix 
      ON ${tableName} USING GIST (geom)
    `);
    
    await this.db.query(`
      CREATE INDEX IF NOT EXISTS ${tableName}_temp_idx 
      ON ${tableName}(temperature_f) 
      WHERE temperature_f IS NOT NULL
    `);
    
    await this.db.query(`
      CREATE INDEX IF NOT EXISTS ${tableName}_depth_idx 
      ON ${tableName}(depth_m) 
      WHERE depth_m IS NOT NULL
    `);
  }

  async ensureTransmissionLinesTable(tableName: string): Promise<void> {
    console.log(`🏗️  Ensuring transmission lines table: ${tableName}`);
    
    const sql = `
      CREATE TABLE IF NOT EXISTS ${tableName} (
        gid BIGSERIAL PRIMARY KEY,
        props JSONB DEFAULT '{}'::jsonb,
        id_text TEXT, 
        owner TEXT, 
        status TEXT, 
        volt_class TEXT,
        kv NUMERIC,                                   -- parsed numeric voltage
        geom geometry(MULTILINESTRING, 4326)
      )
    `;
    
    await this.db.query(sql);
    
    // Create indexes
    await this.db.query(`
      CREATE INDEX IF NOT EXISTS ${tableName}_gix 
      ON ${tableName} USING GIST (geom)
    `);
    
    await this.db.query(`
      CREATE INDEX IF NOT EXISTS ${tableName}_kv_idx 
      ON ${tableName}(kv) 
      WHERE kv IS NOT NULL
    `);
    
    await this.db.query(`
      CREATE INDEX IF NOT EXISTS ${tableName}_owner_idx 
      ON ${tableName}(owner) 
      WHERE owner IS NOT NULL
    `);
  }

  async createUSFilteredViews(tableName: string, geometryType: 'POINT' | 'MULTILINESTRING'): Promise<void> {
    console.log(`🗺️  Creating US-filtered views for ${tableName}`);
    
    const usViewName = `${tableName}_us`;
    
    if (geometryType === 'POINT') {
      // Geothermal points US view
      await this.db.query(`
        CREATE OR REPLACE VIEW ${usViewName} AS
        SELECT gid, latitude, longitude, depth_m, temperature_f, geom
        FROM ${tableName}
        WHERE ST_Intersects(
          geom,
          ST_Collect(ARRAY[
            ST_MakeEnvelope(-125, 24, -66.5, 49.6, 4326),   -- CONUS
            ST_MakeEnvelope(-170, 49, -130, 72, 4326),      -- Alaska
            ST_MakeEnvelope(-161, 18.9, -154, 22.4, 4326),  -- Hawaii
            ST_MakeEnvelope(-67.5, 17.6, -65, 18.6, 4326)   -- Puerto Rico
          ])
        )
      `);

      // Create aggregated view for low zooms
      await this.createGeothermalAggregatedView(tableName);
      
    } else if (geometryType === 'MULTILINESTRING') {
      // Transmission lines US view
      await this.db.query(`
        CREATE OR REPLACE VIEW ${usViewName} AS
        SELECT gid, id_text, owner, status, volt_class, kv, geom
        FROM ${tableName}
        WHERE ST_Intersects(
          geom,
          ST_Collect(ARRAY[
            ST_MakeEnvelope(-125, 24, -66.5, 49.6, 4326),   -- CONUS
            ST_MakeEnvelope(-170, 49, -130, 72, 4326),      -- Alaska
            ST_MakeEnvelope(-161, 18.9, -154, 22.4, 4326),  -- Hawaii
            ST_MakeEnvelope(-67.5, 17.6, -65, 18.6, 4326)   -- Puerto Rico
          ])
        )
      `);
    }
  }

  private async createGeothermalAggregatedView(tableName: string): Promise<void> {
    console.log(`📊 Creating aggregated geothermal view for low zooms`);
    
    const aggViewName = `${tableName}_us_z0_9`;
    
    await this.db.query(`
      CREATE OR REPLACE VIEW ${aggViewName} AS
      WITH hex_grid AS (
        SELECT 
          ST_SnapToGrid(geom, 0.5) as hex_center,
          COUNT(*) as point_count,
          AVG(temperature_f) as avg_temperature_f,
          AVG(depth_m) as avg_depth_m,
          MIN(temperature_f) as min_temperature_f,
          MAX(temperature_f) as max_temperature_f,
          STDDEV(temperature_f) as stddev_temperature_f
        FROM ${tableName}_us
        WHERE temperature_f IS NOT NULL
        GROUP BY ST_SnapToGrid(geom, 0.5)
        HAVING COUNT(*) > 0
      )
      SELECT 
        row_number() OVER () as gid,
        point_count,
        ROUND(avg_temperature_f::numeric, 1) as avg_temperature_f,
        ROUND(avg_depth_m::numeric, 1) as avg_depth_m,
        min_temperature_f,
        max_temperature_f,
        ROUND(stddev_temperature_f::numeric, 1) as stddev_temperature_f,
        hex_center as geom
      FROM hex_grid
    `);
  }

  async optimizeTableForBulkInsert(tableName: string): Promise<void> {
    console.log(`⚡ Optimizing ${tableName} for bulk insert`);
    
    // Drop indexes temporarily for faster inserts
    const indexes = await this.getTableIndexes(tableName);
    
    for (const index of indexes) {
      if (index !== `${tableName}_pkey`) { // Keep primary key
        await this.db.query(`DROP INDEX IF EXISTS ${index}`);
      }
    }
  }

  async restoreTableIndexes(tableName: string, geometryType: 'POINT' | 'MULTILINESTRING'): Promise<void> {
    console.log(`🔧 Restoring indexes for ${tableName}`);
    
    if (geometryType === 'POINT') {
      await this.ensureGeothermalPointsTable(tableName);
    } else {
      await this.ensureTransmissionLinesTable(tableName);
    }
  }

  private async getTableIndexes(tableName: string): Promise<string[]> {
    const result = await this.db.query(`
      SELECT indexname 
      FROM pg_indexes 
      WHERE tablename = $1 AND schemaname = 'public'
    `, [tableName]);
    
    return result.rows.map((row: any) => row.indexname);
  }

  async getTableStats(tableName: string): Promise<any> {
    const exists = await this.db.tableExists(tableName);
    if (!exists) {
      return { exists: false };
    }

    const count = await this.db.getTableRowCount(tableName);
    
    // Get table size
    const sizeResult = await this.db.query(`
      SELECT pg_size_pretty(pg_total_relation_size($1)) as size
    `, [tableName]);
    
    // Get coordinate stats if it's a spatial table
    let coordinateStats = {};
    try {
      const coordResult = await this.db.query(`
        SELECT 
          COUNT(*) FILTER (WHERE ST_X(geom) IS NOT NULL AND ST_Y(geom) IS NOT NULL) as with_coords,
          ST_Extent(geom) as bbox
        FROM ${tableName}
        WHERE geom IS NOT NULL
      `);
      
      if (coordResult.rows[0]) {
        coordinateStats = {
          withCoordinates: coordResult.rows[0].with_coords,
          bbox: coordResult.rows[0].bbox
        };
      }
    } catch (error) {
      // Table might not have geometry column
    }

    return {
      exists: true,
      rowCount: count,
      size: sizeResult.rows[0]?.size || 'Unknown',
      ...coordinateStats
    };
  }

  async createPartitionedTable(
    tableName: string, 
    geometryType: 'POINT' | 'MULTILINESTRING',
    partitionColumn: string = 'created_at'
  ): Promise<void> {
    console.log(`🗂️  Creating partitioned table: ${tableName}`);
    
    if (geometryType === 'POINT') {
      const sql = `
        CREATE TABLE IF NOT EXISTS ${tableName} (
          gid BIGSERIAL,
          latitude    DOUBLE PRECISION NOT NULL,
          longitude   DOUBLE PRECISION NOT NULL,
          depth_m     DOUBLE PRECISION,
          temperature_f DOUBLE PRECISION,
          created_at  TIMESTAMPTZ DEFAULT NOW(),
          geom geometry(POINT, 4326) GENERATED ALWAYS AS (
            ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
          ) STORED,
          PRIMARY KEY (gid, ${partitionColumn})
        ) PARTITION BY RANGE (${partitionColumn})
      `;
      
      await this.db.query(sql);
      
      // Create monthly partitions for current year
      await this.createMonthlyPartitions(tableName, new Date().getFullYear());
      
    } else {
      // Similar for transmission lines if needed
      throw new Error('Partitioned transmission lines tables not implemented yet');
    }
  }

  private async createMonthlyPartitions(tableName: string, year: number): Promise<void> {
    for (let month = 1; month <= 12; month++) {
      const partitionName = `${tableName}_${year}_${month.toString().padStart(2, '0')}`;
      const startDate = `${year}-${month.toString().padStart(2, '0')}-01`;
      const nextMonth = month === 12 ? `${year + 1}-01-01` : `${year}-${(month + 1).toString().padStart(2, '0')}-01`;
      
      await this.db.query(`
        CREATE TABLE IF NOT EXISTS ${partitionName} 
        PARTITION OF ${tableName}
        FOR VALUES FROM ('${startDate}') TO ('${nextMonth}')
      `);
    }
  }
}