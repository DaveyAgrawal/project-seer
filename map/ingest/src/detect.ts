import { promises as fs } from 'fs';
import { extname } from 'path';

export enum FileType {
  CSV = 'csv',
  GEOJSON = 'geojson',
  UNKNOWN = 'unknown'
}

export interface FileInfo {
  type: FileType;
  size: number;
  estimatedRows?: number;
  encoding?: string;
  hasHeader?: boolean;
  sampleContent?: string;
}

export class FileDetector {
  static async detectFileType(filePath: string): Promise<FileInfo> {
    const stats = await fs.stat(filePath);
    const extension = extname(filePath).toLowerCase();
    
    let type = FileType.UNKNOWN;
    
    // First, try to determine by extension
    if (['.csv', '.tsv'].includes(extension)) {
      type = FileType.CSV;
    } else if (['.geojson', '.json'].includes(extension)) {
      // Need to check content to distinguish GeoJSON from regular JSON
      type = await this.detectJSONType(filePath);
    }
    
    // If still unknown, analyze content
    if (type === FileType.UNKNOWN) {
      type = await this.detectByContent(filePath);
    }
    
    const info: FileInfo = {
      type,
      size: stats.size,
      encoding: await this.detectEncoding(filePath)
    };
    
    // Add type-specific information
    if (type === FileType.CSV) {
      const csvInfo = await this.analyzeCSV(filePath);
      info.hasHeader = csvInfo.hasHeader;
      info.estimatedRows = csvInfo.estimatedRows;
      info.sampleContent = csvInfo.sampleContent;
    } else if (type === FileType.GEOJSON) {
      const geoInfo = await this.analyzeGeoJSON(filePath);
      info.estimatedRows = geoInfo.featureCount;
      info.sampleContent = geoInfo.sampleContent;
    }
    
    return info;
  }

  private static async detectJSONType(filePath: string): Promise<FileType> {
    try {
      // Read first few KB to check structure
      const buffer = Buffer.alloc(8192);
      const fd = await fs.open(filePath, 'r');
      
      try {
        const { bytesRead } = await fd.read(buffer, 0, 8192, 0);
        const content = buffer.toString('utf8', 0, bytesRead).trim();
        
        // Check for GeoJSON indicators
        if (this.looksLikeGeoJSON(content)) {
          return FileType.GEOJSON;
        }
        
        return FileType.UNKNOWN;
        
      } finally {
        await fd.close();
      }
    } catch (error) {
      console.warn(`Error detecting JSON type for ${filePath}:`, error);
      return FileType.UNKNOWN;
    }
  }

  private static async detectByContent(filePath: string): Promise<FileType> {
    try {
      // Read first 1KB to analyze content
      const buffer = Buffer.alloc(1024);
      const fd = await fs.open(filePath, 'r');
      
      try {
        const { bytesRead } = await fd.read(buffer, 0, 1024, 0);
        const content = buffer.toString('utf8', 0, bytesRead);
        
        // Check for CSV patterns
        if (this.looksLikeCSV(content)) {
          return FileType.CSV;
        }
        
        // Check for GeoJSON patterns
        if (this.looksLikeGeoJSON(content)) {
          return FileType.GEOJSON;
        }
        
        return FileType.UNKNOWN;
        
      } finally {
        await fd.close();
      }
    } catch (error) {
      console.warn(`Error detecting content type for ${filePath}:`, error);
      return FileType.UNKNOWN;
    }
  }

  private static looksLikeCSV(content: string): boolean {
    const lines = content.split('\n').slice(0, 5); // Check first 5 lines
    
    if (lines.length < 2) return false;
    
    // Check for consistent comma separation
    const commaCount = lines[0].split(',').length;
    if (commaCount < 2) return false;
    
    // Check if subsequent lines have similar structure
    let consistentLines = 0;
    for (let i = 1; i < lines.length; i++) {
      if (Math.abs(lines[i].split(',').length - commaCount) <= 1) {
        consistentLines++;
      }
    }
    
    return consistentLines >= Math.min(2, lines.length - 1);
  }

  private static looksLikeGeoJSON(content: string): boolean {
    const trimmed = content.trim();
    
    // Must start with { or [
    if (!trimmed.startsWith('{') && !trimmed.startsWith('[')) {
      return false;
    }
    
    // Check for GeoJSON keywords
    const geoJsonKeywords = [
      '"type"',
      '"FeatureCollection"',
      '"Feature"',
      '"geometry"',
      '"coordinates"',
      '"properties"',
      '"LineString"',
      '"MultiLineString"',
      '"Point"',
      '"Polygon"'
    ];
    
    const hasGeoKeywords = geoJsonKeywords.some(keyword => content.includes(keyword));
    return hasGeoKeywords;
  }

  private static async detectEncoding(filePath: string): Promise<string> {
    try {
      // Read first 1KB to detect encoding
      const buffer = Buffer.alloc(1024);
      const fd = await fs.open(filePath, 'r');
      
      try {
        const { bytesRead } = await fd.read(buffer, 0, 1024, 0);
        
        // Check for BOM
        if (bytesRead >= 3 && buffer[0] === 0xEF && buffer[1] === 0xBB && buffer[2] === 0xBF) {
          return 'utf8-bom';
        }
        
        // Simple ASCII/UTF-8 detection
        let asciiCount = 0;
        let utf8Count = 0;
        
        for (let i = 0; i < bytesRead; i++) {
          if (buffer[i] < 128) {
            asciiCount++;
          } else if (buffer[i] >= 194 && buffer[i] <= 244) {
            // Potential UTF-8 start byte
            utf8Count++;
          }
        }
        
        if (utf8Count > asciiCount * 0.1) {
          return 'utf8';
        }
        
        return 'ascii';
        
      } finally {
        await fd.close();
      }
    } catch (error) {
      return 'utf8'; // Default fallback
    }
  }

  private static async analyzeCSV(filePath: string): Promise<{
    hasHeader: boolean;
    estimatedRows: number;
    sampleContent: string;
  }> {
    try {
      const buffer = Buffer.alloc(8192);
      const fd = await fs.open(filePath, 'r');
      
      try {
        const { bytesRead } = await fd.read(buffer, 0, 8192, 0);
        const content = buffer.toString('utf8', 0, bytesRead);
        const lines = content.split('\n');
        
        // Detect header by analyzing first two rows
        let hasHeader = false;
        if (lines.length >= 2) {
          const firstRow = lines[0].split(',');
          const secondRow = lines[1].split(',');
          
          // If first row has non-numeric values and second row has more numeric values, likely has header
          const firstRowNumeric = firstRow.filter(cell => !isNaN(parseFloat(cell.trim()))).length;
          const secondRowNumeric = secondRow.filter(cell => !isNaN(parseFloat(cell.trim()))).length;
          
          hasHeader = firstRowNumeric < secondRowNumeric && firstRowNumeric < firstRow.length / 2;
        }
        
        // Estimate total rows based on average line length
        const stats = await fs.stat(filePath);
        const avgLineLength = content.length / lines.length;
        const estimatedRows = Math.floor(stats.size / avgLineLength);
        
        return {
          hasHeader,
          estimatedRows,
          sampleContent: lines.slice(0, 3).join('\n')
        };
        
      } finally {
        await fd.close();
      }
    } catch (error) {
      return {
        hasHeader: true, // Safe default
        estimatedRows: 0,
        sampleContent: ''
      };
    }
  }

  private static async analyzeGeoJSON(filePath: string): Promise<{
    featureCount: number;
    sampleContent: string;
  }> {
    try {
      const buffer = Buffer.alloc(8192);
      const fd = await fs.open(filePath, 'r');
      
      try {
        const { bytesRead } = await fd.read(buffer, 0, 8192, 0);
        const content = buffer.toString('utf8', 0, bytesRead);
        
        // Count approximate features by counting "Feature" occurrences
        const featureMatches = content.match(/"type":\s*"Feature"/g) || [];
        let featureCount = featureMatches.length;
        
        // If this is a FeatureCollection, estimate based on file size
        if (content.includes('"FeatureCollection"')) {
          const stats = await fs.stat(filePath);
          const avgFeatureSize = Math.max(content.length / Math.max(featureCount, 1), 1000);
          featureCount = Math.floor(stats.size / avgFeatureSize);
        }
        
        return {
          featureCount,
          sampleContent: content.substring(0, 500) + (content.length > 500 ? '...' : '')
        };
        
      } finally {
        await fd.close();
      }
    } catch (error) {
      return {
        featureCount: 0,
        sampleContent: ''
      };
    }
  }

  static formatFileSize(bytes: number): string {
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    let size = bytes;
    let unitIndex = 0;
    
    while (size >= 1024 && unitIndex < units.length - 1) {
      size /= 1024;
      unitIndex++;
    }
    
    return `${size.toFixed(1)} ${units[unitIndex]}`;
  }

  static async validateFileAccess(filePath: string): Promise<void> {
    try {
      await fs.access(filePath, fs.constants.F_OK | fs.constants.R_OK);
    } catch (error) {
      throw new Error(`Cannot access file: ${filePath}. Please check the file path and permissions.`);
    }
  }
}