import { createReadStream } from 'fs';
import { pipeline } from 'stream/promises';
import StreamValues from 'stream-json/streamers/StreamValues';
import parser from 'stream-json';
import { PoolClient } from 'pg';
import { DatabaseManager } from './db';
import { ResumeManager } from './resume';
import { DataValidator } from './utils/parsing';
import { BatchManager, ProgressTracker, MemoryMonitor } from './utils/batching';
import { TransmissionLineValidator } from './utils/validation';
import * as turf from '@turf/turf';

export interface GeoJSONProcessorOptions {
  filePath: string;
  tableName: string;
  layerName: string;
  batchSize?: number;
  resume?: boolean;
  maxFeatures?: number;
}

export interface TransmissionLineFeature {
  id_text?: string;
  owner?: string;
  status?: string;
  volt_class?: string;
  kv?: number;
  geometry: any; // GeoJSON MultiLineString
  properties: any; // Original properties as JSONB
}

export class GeoJSONProcessor {
  private db: DatabaseManager;
  private resumeManager: ResumeManager;
  private memoryMonitor: MemoryMonitor;

  constructor(db: DatabaseManager) {
    this.db = db;
    this.resumeManager = new ResumeManager();
    this.memoryMonitor = new MemoryMonitor(1024);
  }

  async processTransmissionLines(options: GeoJSONProcessorOptions): Promise<void> {
    const { filePath, tableName, layerName, resume = false } = options;
    let { batchSize = 10000 } = options;

    console.log(`🚀 Starting transmission lines GeoJSON ingestion: ${filePath}`);
    console.log(`📊 Target table: ${tableName}, Batch size: ${batchSize.toLocaleString()}`);

    // Check if we can resume
    let skipFeatures = 0;
    if (resume) {
      skipFeatures = await this.resumeManager.getResumePoint(filePath, tableName);
      if (skipFeatures > 0) {
        console.log(`📤 Resuming from feature ${skipFeatures.toLocaleString()}`);
      }
    }

    // Initialize progress tracking
    const progressTracker = new ProgressTracker();
    let processedFeatures = skipFeatures;
    let currentFeatureIndex = 0;
    let chunkNumber = 0;

    // Setup batch processing
    const batchProcessor = new BatchProcessor(this.db, tableName);
    const batchManager = new BatchManager<TransmissionLineFeature>(batchSize, batchProcessor);

    try {
      // Set bulk operation settings
      await this.db.setBulkOperationSettings();

      // Stream and process GeoJSON
      await pipeline(
        createReadStream(filePath),
        parser(),
        StreamValues.withParser(),
        async function* (source) {
          for await (const data of source) {
            const feature = data.value;
            
            // Handle different GeoJSON structures
            const features = this.extractFeatures(feature);
            
            for (const f of features) {
              currentFeatureIndex++;
              
              // Skip features if resuming
              if (currentFeatureIndex <= skipFeatures) {
                continue;
              }

              // Validate and normalize feature
              const validationResult = TransmissionLineValidator.validate(f);
              
              if (!validationResult.isValid) {
                console.warn(`⚠️  Skipping invalid feature ${currentFeatureIndex}: ${validationResult.errors.join(', ')}`);
                continue;
              }

              if (validationResult.warnings.length > 0) {
                console.warn(`⚠️  Feature ${currentFeatureIndex}: ${validationResult.warnings.join(', ')}`);
              }

              // Add to batch
              await batchManager.add(validationResult.data!);
              
              processedFeatures++;
              
              if (processedFeatures % 1000 === 0) {
                progressTracker.update(processedFeatures);
                this.memoryMonitor.checkMemoryUsage();
              }

              // Update resume progress periodically
              if (processedFeatures % batchSize === 0) {
                chunkNumber++;
                await this.resumeManager.updateProgress(
                  filePath, 
                  tableName, 
                  processedFeatures, 
                  chunkNumber
                );
              }
            }
          }
        }.bind(this)
      );

      // Process final batch
      await batchManager.flush();
      progressTracker.update(processedFeatures);
      progressTracker.complete();

      // Create indexes and analyze
      await this.finalizeTable(tableName);

      // Register dataset
      await this.db.registerDataset(layerName, tableName, 'MULTILINESTRING');

      // Clean up
      await this.db.resetSettings();
      await this.resumeManager.deleteMetadata(filePath, tableName);

      console.log(`✅ Transmission lines GeoJSON ingestion completed: ${processedFeatures.toLocaleString()} features`);

    } catch (error) {
      await this.db.resetSettings();
      throw error;
    }
  }

  private extractFeatures(data: any): any[] {
    // Handle FeatureCollection
    if (data.type === 'FeatureCollection' && Array.isArray(data.features)) {
      return data.features;
    }
    
    // Handle single Feature
    if (data.type === 'Feature') {
      return [data];
    }
    
    // Handle array of coordinate arrays (convert to LineString features)
    if (Array.isArray(data)) {
      return data.map((coords, index) => ({
        type: 'Feature',
        geometry: {
          type: 'LineString',
          coordinates: coords
        },
        properties: {
          id: index.toString()
        }
      }));
    }
    
    // Handle raw geometry
    if (data.type && data.coordinates) {
      return [{
        type: 'Feature',
        geometry: data,
        properties: {}
      }];
    }
    
    return [];
  }

  private async finalizeTable(tableName: string): Promise<void> {
    console.log(`🔧 Creating indexes and optimizing ${tableName}...`);
    
    // Create spatial index
    await this.db.query(`
      CREATE INDEX IF NOT EXISTS ${tableName}_gix 
      ON ${tableName} USING GIST (geom)
    `);
    
    // Create voltage index for filtering
    await this.db.query(`
      CREATE INDEX IF NOT EXISTS ${tableName}_kv_idx 
      ON ${tableName}(kv) 
      WHERE kv IS NOT NULL
    `);
    
    // Create owner index for queries
    await this.db.query(`
      CREATE INDEX IF NOT EXISTS ${tableName}_owner_idx 
      ON ${tableName}(owner) 
      WHERE owner IS NOT NULL
    `);

    // Analyze for query planning
    await this.db.analyze(tableName);
    
    console.log('📊 Creating zoom-banded views...');
    await this.createZoomBandedViews(tableName);
  }

  private async createZoomBandedViews(tableName: string): Promise<void> {
    // Low zoom (0-6): Only major transmission lines, heavy simplification
    await this.db.query(`
      CREATE OR REPLACE VIEW ${tableName}_z0_6 AS
      SELECT 
        gid,
        id_text,
        owner,
        kv,
        ST_SimplifyVW(geom, 0.01) as geom
      FROM ${tableName}
      WHERE kv > 138 OR kv IS NULL
    `);

    // Medium zoom (7-10): Medium+ voltage lines, moderate simplification  
    await this.db.query(`
      CREATE OR REPLACE VIEW ${tableName}_z7_10 AS
      SELECT 
        gid,
        id_text,
        owner,
        volt_class,
        kv,
        ST_SimplifyVW(geom, 0.001) as geom
      FROM ${tableName}
      WHERE kv > 69 OR kv IS NULL
    `);

    // High zoom (11-14): All lines, minimal simplification
    await this.db.query(`
      CREATE OR REPLACE VIEW ${tableName}_z11_14 AS
      SELECT 
        gid,
        id_text,
        owner,
        status,
        volt_class,
        kv,
        geom
      FROM ${tableName}
    `);

    console.log('✅ Created zoom-banded views for optimal tile performance');
  }
}

class BatchProcessor implements import('./utils/batching').BatchProcessor<TransmissionLineFeature> {
  constructor(private db: DatabaseManager, private tableName: string) {}

  async processBatch(batch: TransmissionLineFeature[]): Promise<void> {
    await this.db.transaction(async (client: PoolClient) => {
      const values: any[][] = [];
      
      for (const feature of batch) {
        // Filter coordinates to US bounds
        const filteredGeometry = this.filterToUSBounds(feature.geometry);
        
        if (filteredGeometry) {
          values.push([
            JSON.stringify(feature.properties),
            feature.id_text || null,
            feature.owner || null,
            feature.status || null,
            feature.volt_class || null,
            feature.kv || null,
            `ST_GeomFromGeoJSON('${JSON.stringify(filteredGeometry)}')`
          ]);
        }
      }

      if (values.length === 0) return;

      // Batch insert using parameterized query
      const placeholders = values.map((_, i) => 
        `($${i * 7 + 1}, $${i * 7 + 2}, $${i * 7 + 3}, $${i * 7 + 4}, $${i * 7 + 5}, $${i * 7 + 6}, ${values[i][6]})`
      ).join(',');

      const flatValues = values.reduce((acc, val) => {
        acc.push(...val.slice(0, 6)); // Exclude the ST_GeomFromGeoJSON part
        return acc;
      }, []);

      const sql = `
        INSERT INTO ${this.tableName} (props, id_text, owner, status, volt_class, kv, geom)
        VALUES ${placeholders}
        ON CONFLICT (gid) DO NOTHING
      `;

      await client.query(sql, flatValues);
    });
  }

  private filterToUSBounds(geometry: any): any | null {
    try {
      // US bounding boxes
      const usBounds = [
        turf.bboxPolygon([-125, 24, -66.5, 49.6]), // CONUS
        turf.bboxPolygon([-170, 49, -130, 72]),    // Alaska
        turf.bboxPolygon([-161, 18.9, -154, 22.4]), // Hawaii
        turf.bboxPolygon([-67.5, 17.6, -65, 18.6])  // Puerto Rico
      ];

      const feature = turf.feature(geometry);
      
      // Check if geometry intersects with any US bounds
      const intersectsUS = usBounds.some(bound => {
        try {
          return turf.booleanIntersects(feature, bound);
        } catch (e) {
          return false;
        }
      });

      return intersectsUS ? geometry : null;
      
    } catch (error) {
      console.warn('Error filtering geometry to US bounds:', error);
      return geometry; // Return original if filtering fails
    }
  }
}