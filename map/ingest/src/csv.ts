import { createReadStream } from 'fs';
import { pipeline } from 'stream/promises';
import csvParser from 'csv-parser';
import { from as copyFrom } from 'pg-copy-streams';
import { PoolClient } from 'pg';
import { DatabaseManager } from './db';
import { ResumeManager } from './resume';
import { 
  HeaderDetector, 
  UnitConverter, 
  DataValidator,
  FieldMapping 
} from './utils/parsing';
import { WKTProcessor } from './utils/wkt';
import { BatchManager, ProgressTracker, MemoryMonitor } from './utils/batching';
import { GeothermalValidator } from './utils/validation';

export interface CSVProcessorOptions {
  filePath: string;
  tableName: string;
  layerName: string;
  batchSize?: number;
  resume?: boolean;
  skipRows?: number;
  maxRows?: number;
}

export interface GeothermalStagingRow {
  latitude_raw: string;
  longitude_raw: string;
  depth_raw: string;
  temperature_raw: string;
  geometry_wkt_raw: string;
}

export class CSVProcessor {
  private db: DatabaseManager;
  private resumeManager: ResumeManager;
  private memoryMonitor: MemoryMonitor;

  constructor(db: DatabaseManager) {
    this.db = db;
    this.resumeManager = new ResumeManager();
    this.memoryMonitor = new MemoryMonitor(1024); // 1GB memory limit
  }

  async processGeothermalCSV(options: CSVProcessorOptions): Promise<void> {
    const { filePath, tableName, layerName, resume = false } = options;
    let { batchSize = 250000 } = options;

    console.log(`🚀 Starting geothermal CSV ingestion: ${filePath}`);
    console.log(`📊 Target table: ${tableName}, Batch size: ${batchSize.toLocaleString()}`);

    // Check if we can resume
    let skipRows = 0;
    if (resume) {
      skipRows = await this.resumeManager.getResumePoint(filePath, tableName);
      if (skipRows > 0) {
        console.log(`📤 Resuming from row ${skipRows.toLocaleString()}`);
      }
    }

    // Create staging table
    await this.createGeothermalStagingTable(`${tableName}_staging`);

    // First pass: detect headers and count rows
    const { fieldMapping, totalRows } = await this.analyzeCSV(filePath);
    console.log(`📋 Detected fields:`, fieldMapping);
    console.log(`📊 Total rows: ${totalRows.toLocaleString()}`);

    if (!fieldMapping.latitude || !fieldMapping.longitude) {
      throw new Error('Could not detect latitude/longitude columns in CSV');
    }

    // Initialize progress tracking
    const progressTracker = new ProgressTracker(totalRows - skipRows);
    let processedRows = skipRows;
    let currentRowIndex = 0;
    let chunkNumber = 0;

    // Setup batch processing
    const stagingBatch: GeothermalStagingRow[] = [];
    
    try {
      // Set bulk operation settings
      await this.db.setBulkOperationSettings();

      // Stream and process CSV
      const self = this;
      await pipeline(
        createReadStream(filePath),
        csvParser(),
        async function* (source) {
          for await (const row of source) {
            currentRowIndex++;
            
            // Skip rows if resuming
            if (currentRowIndex <= skipRows) {
              continue;
            }

            // Convert row to staging format
            const stagingRow: GeothermalStagingRow = {
              latitude_raw: row[fieldMapping.latitude!] || '',
              longitude_raw: row[fieldMapping.longitude!] || '',
              depth_raw: fieldMapping.depth ? (row[fieldMapping.depth] || '') : '',
              temperature_raw: fieldMapping.temperature ? (row[fieldMapping.temperature] || '') : '',
              geometry_wkt_raw: fieldMapping.geometry ? (row[fieldMapping.geometry] || '') : ''
            };

            stagingBatch.push(stagingRow);
            
            // Process batch when full
            if (stagingBatch.length >= batchSize) {
              await self.processStagingBatch(stagingBatch, `${tableName}_staging`, fieldMapping);
              stagingBatch.length = 0; // Clear batch
              
              processedRows += batchSize;
              chunkNumber++;
              
              progressTracker.update(processedRows);
              await self.resumeManager.updateProgress(filePath, tableName, processedRows, chunkNumber, totalRows);
              
              // Memory management
              self.memoryMonitor.checkMemoryUsage();
              batchSize = self.memoryMonitor.suggestBatchSize(batchSize);
            }
          }
        }
      );

      // Process final batch
      if (stagingBatch.length > 0) {
        await this.processStagingBatch(stagingBatch, `${tableName}_staging`, fieldMapping);
        processedRows += stagingBatch.length;
        progressTracker.update(processedRows);
      }

      progressTracker.complete();

      // Transform staging data to final table
      console.log('🔄 Transforming staging data to final table...');
      await this.transformStagingToFinal(`${tableName}_staging`, tableName, fieldMapping);

      // Create indexes and analyze
      await this.finalizeTable(tableName);

      // Register dataset
      await this.db.registerDataset(layerName, tableName, 'POINT');

      // Clean up
      await this.db.resetSettings();
      await this.resumeManager.deleteMetadata(filePath, tableName);
      await this.db.query(`DROP TABLE IF EXISTS ${tableName}_staging`);

      console.log(`✅ Geothermal CSV ingestion completed: ${processedRows.toLocaleString()} rows`);

    } catch (error) {
      await this.db.resetSettings();
      throw error;
    }
  }

  private async analyzeCSV(filePath: string): Promise<{ fieldMapping: FieldMapping; totalRows: number }> {
    console.log('📊 Analyzing CSV structure...');
    
    let headers: string[] = [];
    let totalRows = 0;
    let isFirstRow = true;

    await pipeline(
      createReadStream(filePath),
      csvParser(),
      async function* (source) {
        for await (const row of source) {
          if (isFirstRow) {
            headers = Object.keys(row);
            isFirstRow = false;
          }
          totalRows++;
          
          // Sample first few rows for validation
          if (totalRows % 100000 === 0) {
            console.log(`📊 Counted ${totalRows.toLocaleString()} rows...`);
          }
        }
      }
    );

    const fieldMapping = HeaderDetector.detectFields(headers);
    return { fieldMapping, totalRows };
  }

  private async createGeothermalStagingTable(tableName: string): Promise<void> {
    const sql = `
      CREATE TABLE IF NOT EXISTS ${tableName} (
        id BIGSERIAL PRIMARY KEY,
        latitude_raw TEXT,
        longitude_raw TEXT, 
        depth_raw TEXT,
        temperature_raw TEXT,
        geometry_wkt_raw TEXT
      )
    `;
    await this.db.query(sql);
    await this.db.query(`TRUNCATE TABLE ${tableName}`); // Clean slate
  }

  private async processStagingBatch(
    batch: GeothermalStagingRow[], 
    stagingTable: string,
    fieldMapping: FieldMapping
  ): Promise<void> {
    await this.db.transaction(async (client: PoolClient) => {
      // Use COPY for high-performance bulk insert
      const copyStream = client.query(
        copyFrom(`COPY ${stagingTable} (latitude_raw, longitude_raw, depth_raw, temperature_raw, geometry_wkt_raw) FROM STDIN WITH CSV`)
      );

      // Convert batch to CSV format
      for (const row of batch) {
        const csvLine = [
          this.escapeCsvField(row.latitude_raw),
          this.escapeCsvField(row.longitude_raw),
          this.escapeCsvField(row.depth_raw),
          this.escapeCsvField(row.temperature_raw),
          this.escapeCsvField(row.geometry_wkt_raw)
        ].join(',') + '\n';
        
        copyStream.write(csvLine);
      }

      copyStream.end();
      await new Promise((resolve, reject) => {
        copyStream.on('finish', resolve);
        copyStream.on('error', reject);
      });
    });
  }

  private async transformStagingToFinal(
    stagingTable: string, 
    finalTable: string, 
    fieldMapping: FieldMapping
  ): Promise<void> {
    const isDepthFeet = fieldMapping.depth ? HeaderDetector.isDepthFeet(fieldMapping.depth) : false;
    const isTempCelsius = fieldMapping.temperature ? HeaderDetector.isTemperatureCelsius(fieldMapping.temperature) : false;

    const sql = `
      INSERT INTO ${finalTable} (latitude, longitude, depth_m, temperature_f)
      SELECT 
        COALESCE(
          CASE 
            WHEN geometry_wkt_raw != '' AND geometry_wkt_raw IS NOT NULL THEN
              ST_Y(ST_Transform(ST_GeomFromText(geometry_wkt_raw, 3857), 4326))
            ELSE 
              CASE 
                WHEN latitude_raw ~ '^-?[0-9]+\.?[0-9]*$' THEN latitude_raw::DOUBLE PRECISION
                ELSE NULL
              END
          END
        ) as latitude,
        COALESCE(
          CASE 
            WHEN geometry_wkt_raw != '' AND geometry_wkt_raw IS NOT NULL THEN
              ST_X(ST_Transform(ST_GeomFromText(geometry_wkt_raw, 3857), 4326))
            ELSE 
              CASE 
                WHEN longitude_raw ~ '^-?[0-9]+\.?[0-9]*$' THEN longitude_raw::DOUBLE PRECISION
                ELSE NULL
              END
          END
        ) as longitude,
        CASE 
          WHEN depth_raw ~ '^-?[0-9]+\.?[0-9]*$' THEN 
            ABS(depth_raw::DOUBLE PRECISION${isDepthFeet ? ' * 0.3048' : ''}) 
          ELSE NULL 
        END as depth_m,
        CASE 
          WHEN temperature_raw ~ '^-?[0-9]+\.?[0-9]*$' THEN 
            temperature_raw::DOUBLE PRECISION${isTempCelsius ? ' * 9.0/5.0 + 32' : ''}
          ELSE NULL 
        END as temperature_f
      FROM ${stagingTable}
      WHERE 
        -- Only insert records with valid coordinates
        (
          (geometry_wkt_raw != '' AND geometry_wkt_raw IS NOT NULL) OR
          (latitude_raw ~ '^-?[0-9]+\.?[0-9]*$' AND longitude_raw ~ '^-?[0-9]+\.?[0-9]*$')
        )
        -- Basic coordinate validation  
        AND COALESCE(
          CASE 
            WHEN geometry_wkt_raw != '' THEN
              ST_Y(ST_Transform(ST_GeomFromText(geometry_wkt_raw, 3857), 4326))
            ELSE latitude_raw::DOUBLE PRECISION
          END, -999
        ) BETWEEN -90 AND 90
        AND COALESCE(
          CASE 
            WHEN geometry_wkt_raw != '' THEN
              ST_X(ST_Transform(ST_GeomFromText(geometry_wkt_raw, 3857), 4326))  
            ELSE longitude_raw::DOUBLE PRECISION
          END, -999
        ) BETWEEN -180 AND 180
      ON CONFLICT (latitude, longitude, depth_m) DO NOTHING
    `;

    await this.db.query(sql);
  }

  private async finalizeTable(tableName: string): Promise<void> {
    console.log(`🔧 Creating indexes and optimizing ${tableName}...`);
    
    // Indexes should already exist from schema, but ensure they're there
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

    // Analyze for query planning
    await this.db.analyze(tableName);
  }

  private escapeCsvField(value: string): string {
    if (!value) return '';
    
    // Escape quotes and handle multiline values
    if (value.includes('"') || value.includes(',') || value.includes('\n')) {
      return '"' + value.replace(/"/g, '""') + '"';
    }
    return value;
  }
}