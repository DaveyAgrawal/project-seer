#!/usr/bin/env node

import { Command } from 'commander';
import { DatabaseManager } from './db';
import { CSVProcessor } from './csv';
import { GeoJSONProcessor } from './geojson';
import { SchemaManager } from './schema';
import { FileDetector, FileType } from './detect';
import { ResumeManager } from './resume';

interface IngestOptions {
  file: string[];
  table?: string;
  layerName?: string;
  batchSize?: number;
  resume?: boolean;
  dryRun?: boolean;
  skipRows?: number;
  maxRows?: number;
}

class GeospatialIngestionCLI {
  private db: DatabaseManager;
  private schemaManager: SchemaManager;
  private resumeManager: ResumeManager;

  constructor() {
    this.db = new DatabaseManager();
    this.schemaManager = new SchemaManager(this.db);
    this.resumeManager = new ResumeManager();
  }

  async run(): Promise<void> {
    const program = new Command();

    program
      .name('geoingest')
      .description('High-performance geospatial data ingestion CLI')
      .version('1.0.0');

    program
      .command('ingest')
      .description('Ingest CSV or GeoJSON files into PostGIS')
      .option('-f, --file <paths...>', 'Input file paths (can be repeated)')
      .option('-t, --table <name>', 'Target table name')
      .option('-l, --layer-name <name>', 'Layer name for dataset registry')
      .option('-b, --batch-size <number>', 'Batch size for processing', '250000')
      .option('-r, --resume', 'Resume from last processed chunk', false)
      .option('-d, --dry-run', 'Show what would be processed without executing', false)
      .option('--skip-rows <number>', 'Skip first N rows', '0')
      .option('--max-rows <number>', 'Process maximum N rows')
      .action(async (options: IngestOptions) => {
        await this.handleIngest(options);
      });

    program
      .command('status')
      .description('Show status of active ingestions')
      .action(async () => {
        await this.handleStatus();
      });

    program
      .command('cleanup')
      .description('Clean up completed and old resume files')
      .option('--days <number>', 'Remove resume files older than N days', '7')
      .action(async (options: { days: string }) => {
        await this.handleCleanup(parseInt(options.days));
      });

    program
      .command('analyze <file>')
      .description('Analyze file structure and content without ingesting')
      .action(async (filePath: string) => {
        await this.handleAnalyze(filePath);
      });

    await program.parseAsync();
  }

  private async handleIngest(options: IngestOptions): Promise<void> {
    try {
      if (!options.file || options.file.length === 0) {
        console.error('❌ Error: At least one file path is required');
        process.exit(1);
      }

      console.log('🚀 Starting geospatial ingestion...');
      console.log(`📁 Files: ${options.file.join(', ')}`);
      
      for (const filePath of options.file) {
        await this.processFile(filePath, options);
      }
      
      console.log('✅ All files processed successfully!');

    } catch (error) {
      console.error('❌ Ingestion failed:', error);
      process.exit(1);
    } finally {
      await this.db.close();
    }
  }

  private async processFile(filePath: string, options: IngestOptions): Promise<void> {
    // Resolve placeholder paths
    const resolvedPath = this.resolvePlaceholder(filePath);
    
    console.log(`\n📄 Processing file: ${resolvedPath}`);
    
    // Validate file access
    await FileDetector.validateFileAccess(resolvedPath);
    
    // Detect file type and analyze
    const fileInfo = await FileDetector.detectFileType(resolvedPath);
    console.log(`📊 File info: ${fileInfo.type}, ${FileDetector.formatFileSize(fileInfo.size)}`);
    
    if (fileInfo.estimatedRows) {
      console.log(`📈 Estimated rows/features: ${fileInfo.estimatedRows.toLocaleString()}`);
    }

    if (options.dryRun) {
      console.log('🔍 DRY RUN - Would process this file');
      if (fileInfo.sampleContent) {
        console.log('📝 Sample content:');
        console.log(fileInfo.sampleContent);
      }
      return;
    }

    // Determine table and layer names
    const tableName = options.table || this.generateTableName(resolvedPath, fileInfo.type);
    const layerName = options.layerName || tableName;

    console.log(`🎯 Target table: ${tableName}`);
    console.log(`🏷️  Layer name: ${layerName}`);

    // Process based on file type
    if (fileInfo.type === FileType.CSV) {
      await this.processCSVFile(resolvedPath, tableName, layerName, options);
    } else if (fileInfo.type === FileType.GEOJSON) {
      await this.processGeoJSONFile(resolvedPath, tableName, layerName, options);
    } else {
      throw new Error(`Unsupported file type: ${fileInfo.type}`);
    }
  }

  private async processCSVFile(
    filePath: string, 
    tableName: string, 
    layerName: string, 
    options: IngestOptions
  ): Promise<void> {
    console.log('📊 Processing as geothermal CSV data...');
    
    // Ensure table exists
    await this.schemaManager.ensureGeothermalPointsTable(tableName);
    
    // Create US-filtered views
    await this.schemaManager.createUSFilteredViews(tableName, 'POINT');
    
    const csvProcessor = new CSVProcessor(this.db);
    await csvProcessor.processGeothermalCSV({
      filePath,
      tableName,
      layerName,
      batchSize: parseInt(options.batchSize?.toString() || '250000'),
      resume: options.resume,
      skipRows: options.skipRows ? parseInt(options.skipRows.toString()) : 0,
      maxRows: options.maxRows ? parseInt(options.maxRows.toString()) : undefined
    });
  }

  private async processGeoJSONFile(
    filePath: string, 
    tableName: string, 
    layerName: string, 
    options: IngestOptions
  ): Promise<void> {
    console.log('🗺️  Processing as transmission lines GeoJSON data...');
    
    // Ensure table exists
    await this.schemaManager.ensureTransmissionLinesTable(tableName);
    
    // Create US-filtered views
    await this.schemaManager.createUSFilteredViews(tableName, 'MULTILINESTRING');
    
    const geoJsonProcessor = new GeoJSONProcessor(this.db);
    await geoJsonProcessor.processTransmissionLines({
      filePath,
      tableName,
      layerName,
      batchSize: parseInt(options.batchSize?.toString() || '10000'),
      resume: options.resume,
      maxFeatures: options.maxRows ? parseInt(options.maxRows.toString()) : undefined
    });
  }

  private async handleStatus(): Promise<void> {
    try {
      const active = await this.resumeManager.listActiveIngestions();
      
      if (active.length === 0) {
        console.log('📊 No active ingestions found');
        return;
      }

      console.log('📊 Active Ingestions:');
      console.log('=' .repeat(80));
      
      for (const metadata of active) {
        const progress = metadata.totalRows 
          ? `${metadata.processedRows.toLocaleString()} / ${metadata.totalRows.toLocaleString()} (${Math.round(metadata.processedRows / metadata.totalRows * 100)}%)`
          : `${metadata.processedRows.toLocaleString()} rows`;
        
        const startTime = new Date(metadata.startTime);
        const lastUpdate = new Date(metadata.lastUpdateTime);
        const duration = (lastUpdate.getTime() - startTime.getTime()) / 1000;
        
        console.log(`📄 ${metadata.fileName}`);
        console.log(`   Table: ${metadata.tableName}`);
        console.log(`   Progress: ${progress}`);
        console.log(`   Duration: ${Math.floor(duration / 60)}m ${Math.floor(duration % 60)}s`);
        console.log(`   Last update: ${lastUpdate.toLocaleString()}`);
        console.log('');
      }
    } catch (error) {
      console.error('❌ Error checking status:', error);
      process.exit(1);
    }
  }

  private async handleCleanup(maxAgeDays: number): Promise<void> {
    try {
      console.log(`🧹 Cleaning up resume files older than ${maxAgeDays} days...`);
      
      await this.resumeManager.cleanupCompleted();
      await this.resumeManager.cleanupOld(maxAgeDays);
      
      console.log('✅ Cleanup completed');
    } catch (error) {
      console.error('❌ Cleanup failed:', error);
      process.exit(1);
    }
  }

  private async handleAnalyze(filePath: string): Promise<void> {
    try {
      const resolvedPath = this.resolvePlaceholder(filePath);
      await FileDetector.validateFileAccess(resolvedPath);
      
      console.log(`🔍 Analyzing file: ${resolvedPath}`);
      
      const fileInfo = await FileDetector.detectFileType(resolvedPath);
      
      console.log('📊 File Analysis:');
      console.log('=' .repeat(50));
      console.log(`Type: ${fileInfo.type}`);
      console.log(`Size: ${FileDetector.formatFileSize(fileInfo.size)}`);
      console.log(`Encoding: ${fileInfo.encoding}`);
      
      if (fileInfo.estimatedRows) {
        console.log(`Estimated rows/features: ${fileInfo.estimatedRows.toLocaleString()}`);
      }
      
      if (fileInfo.hasHeader !== undefined) {
        console.log(`Has header: ${fileInfo.hasHeader}`);
      }
      
      if (fileInfo.sampleContent) {
        console.log('\n📝 Sample content:');
        console.log('-'.repeat(30));
        console.log(fileInfo.sampleContent);
      }
      
      // Show table stats if exists
      const tableName = this.generateTableName(resolvedPath, fileInfo.type);
      const stats = await this.schemaManager.getTableStats(tableName);
      
      if (stats.exists) {
        console.log(`\n📈 Existing table stats (${tableName}):`);
        console.log(`Rows: ${stats.rowCount.toLocaleString()}`);
        console.log(`Size: ${stats.size}`);
        
        if (stats.withCoordinates) {
          console.log(`With coordinates: ${stats.withCoordinates.toLocaleString()}`);
        }
      }
      
    } catch (error) {
      console.error('❌ Analysis failed:', error);
      process.exit(1);
    } finally {
      await this.db.close();
    }
  }

  private generateTableName(filePath: string, fileType: FileType): string {
    const basename = filePath.split('/').pop()?.split('.')[0] || 'unknown';
    const cleanName = basename.toLowerCase().replace(/[^a-z0-9]/g, '_');
    
    if (fileType === FileType.CSV) {
      return `geothermal_${cleanName}`;
    } else if (fileType === FileType.GEOJSON) {
      return `transmission_lines_${cleanName}`;
    }
    
    return `data_${cleanName}`;
  }

  private resolvePlaceholder(filePath: string): string {
    // Replace placeholders with environment variables or defaults
    if (filePath === '{{GEOJSON_LINES_PATH}}') {
      return process.env.GEOJSON_LINES_PATH || filePath;
    }
    
    if (filePath === '{{CSV_GEOTHERMAL_PATH}}') {
      return process.env.CSV_GEOTHERMAL_PATH || filePath;
    }
    
    return filePath;
  }
}

// Run the CLI if this file is executed directly
if (require.main === module) {
  const cli = new GeospatialIngestionCLI();
  cli.run().catch(error => {
    console.error('❌ CLI Error:', error);
    process.exit(1);
  });
}