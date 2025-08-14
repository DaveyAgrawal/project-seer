import { promises as fs } from 'fs';
import { dirname } from 'path';

export interface ResumeMetadata {
  fileName: string;
  tableName: string;
  totalRows?: number;
  processedRows: number;
  lastProcessedChunk: number;
  startTime: string;
  lastUpdateTime: string;
  checksum?: string;
}

export class ResumeManager {
  private resumeDir: string;
  
  constructor(resumeDir: string = '.ingest') {
    this.resumeDir = resumeDir;
  }

  private getResumeFilePath(fileName: string, tableName: string): string {
    const safeFileName = fileName.replace(/[^a-zA-Z0-9.-]/g, '_');
    return `${this.resumeDir}/${safeFileName}_${tableName}.resume.json`;
  }

  async ensureResumeDir(): Promise<void> {
    try {
      await fs.mkdir(this.resumeDir, { recursive: true });
    } catch (error) {
      // Directory might already exist
    }
  }

  async saveMetadata(metadata: ResumeMetadata): Promise<void> {
    await this.ensureResumeDir();
    
    const resumeFile = this.getResumeFilePath(metadata.fileName, metadata.tableName);
    const data = {
      ...metadata,
      lastUpdateTime: new Date().toISOString()
    };
    
    await fs.writeFile(resumeFile, JSON.stringify(data, null, 2));
  }

  async loadMetadata(fileName: string, tableName: string): Promise<ResumeMetadata | null> {
    const resumeFile = this.getResumeFilePath(fileName, tableName);
    
    try {
      const data = await fs.readFile(resumeFile, 'utf8');
      return JSON.parse(data);
    } catch (error) {
      // Resume file doesn't exist or is corrupted
      return null;
    }
  }

  async deleteMetadata(fileName: string, tableName: string): Promise<void> {
    const resumeFile = this.getResumeFilePath(fileName, tableName);
    
    try {
      await fs.unlink(resumeFile);
    } catch (error) {
      // File might not exist
    }
  }

  async canResume(fileName: string, tableName: string, currentFileSize?: number): Promise<boolean> {
    const metadata = await this.loadMetadata(fileName, tableName);
    
    if (!metadata) {
      return false;
    }

    // Basic validation - in a production system you might want to check file checksums
    if (currentFileSize && metadata.checksum) {
      // Could implement checksum validation here
    }

    return metadata.processedRows > 0;
  }

  async getResumePoint(fileName: string, tableName: string): Promise<number> {
    const metadata = await this.loadMetadata(fileName, tableName);
    return metadata ? metadata.processedRows : 0;
  }

  async updateProgress(
    fileName: string, 
    tableName: string, 
    processedRows: number, 
    chunkNumber: number,
    totalRows?: number
  ): Promise<void> {
    let metadata = await this.loadMetadata(fileName, tableName);
    
    if (!metadata) {
      metadata = {
        fileName,
        tableName,
        processedRows: 0,
        lastProcessedChunk: 0,
        startTime: new Date().toISOString(),
        lastUpdateTime: new Date().toISOString()
      };
    }

    metadata.processedRows = processedRows;
    metadata.lastProcessedChunk = chunkNumber;
    metadata.totalRows = totalRows;

    await this.saveMetadata(metadata);
  }

  async listActiveIngestions(): Promise<ResumeMetadata[]> {
    await this.ensureResumeDir();
    
    try {
      const files = await fs.readdir(this.resumeDir);
      const resumeFiles = files.filter(f => f.endsWith('.resume.json'));
      
      const metadata: ResumeMetadata[] = [];
      
      for (const file of resumeFiles) {
        try {
          const content = await fs.readFile(`${this.resumeDir}/${file}`, 'utf8');
          metadata.push(JSON.parse(content));
        } catch (error) {
          console.warn(`Failed to read resume file: ${file}`);
        }
      }
      
      return metadata;
    } catch (error) {
      return [];
    }
  }

  async cleanupCompleted(): Promise<void> {
    const active = await this.listActiveIngestions();
    
    for (const metadata of active) {
      if (metadata.totalRows && metadata.processedRows >= metadata.totalRows) {
        await this.deleteMetadata(metadata.fileName, metadata.tableName);
        console.log(`✅ Cleaned up completed resume file for ${metadata.fileName}`);
      }
    }
  }

  async cleanupOld(maxAgeDays: number = 7): Promise<void> {
    const active = await this.listActiveIngestions();
    const cutoffDate = new Date();
    cutoffDate.setDate(cutoffDate.getDate() - maxAgeDays);
    
    for (const metadata of active) {
      const lastUpdate = new Date(metadata.lastUpdateTime);
      if (lastUpdate < cutoffDate) {
        await this.deleteMetadata(metadata.fileName, metadata.tableName);
        console.log(`🗑️  Cleaned up old resume file for ${metadata.fileName} (${maxAgeDays}+ days old)`);
      }
    }
  }
}