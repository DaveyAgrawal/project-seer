// GIS processing for EnergyNet land parcel data
import * as fs from 'fs';
import * as path from 'path';
import AdmZip from 'adm-zip';
import * as shapefile from 'shapefile';
import * as turf from '@turf/turf';

export interface LandParcelFeature {
  type: 'Feature';
  id?: string | number;
  properties: {
    listing_id: string;
    parcel_id: string;
    sale_group?: string;
    state?: string;
    region?: string;
    acres?: number;
    description?: string;
    [key: string]: any; // Allow additional properties from shapefile
  };
  geometry: {
    type: 'Polygon' | 'MultiPolygon';
    coordinates: number[][][] | number[][][][];
  };
}

export interface LandParcelCollection {
  type: 'FeatureCollection';
  features: LandParcelFeature[];
  metadata?: {
    listing_id?: string;
    sale_group?: string;
    state?: string;
    total_parcels?: number;
    total_acres?: number;
    processed_at?: string;
    source_files?: string[];
    [key: string]: any;
  };
}

export interface GISProcessingOptions {
  outputFormat?: 'geojson' | 'shapefile';
  coordinateSystem?: 'WGS84' | 'NAD83' | 'NAD27';
  simplifyTolerance?: number;
  validateGeometry?: boolean;
  calculateAreas?: boolean;
  cacheResults?: boolean;
}

export interface GISProcessingResult {
  success: boolean;
  inputFile: string;
  outputFile?: string;
  parcelsProcessed: number;
  errors?: string[];
  warnings?: string[];
  processingTimeMs?: number;
}

export class GISProcessor {
  private workingDir: string;
  private outputDir: string;

  constructor(workingDir: string = './downloads/energynet', outputDir?: string) {
    this.workingDir = workingDir;
    this.outputDir = outputDir || path.join(workingDir, 'processed');
    
    // Ensure directories exist
    if (!fs.existsSync(this.outputDir)) {
      fs.mkdirSync(this.outputDir, { recursive: true });
    }
  }

  async processZipFile(
    zipFilePath: string,
    listingId: string,
    state: string,
    options: GISProcessingOptions = {}
  ): Promise<LandParcelCollection> {
    console.log(`📦 Processing ZIP file: ${zipFilePath}`);
    
    if (!fs.existsSync(zipFilePath)) {
      throw new Error(`ZIP file not found: ${zipFilePath}`);
    }

    // Extract ZIP file
    const extractDir = await this.extractZipFile(zipFilePath);
    console.log(`📂 Extracted to: ${extractDir}`);

    try {
      // Find shapefiles in extracted directory
      const shapefiles = this.findShapefiles(extractDir);
      console.log(`🗺️ Found ${shapefiles.length} shapefile(s):`, shapefiles);

      if (shapefiles.length === 0) {
        throw new Error('No shapefiles found in ZIP archive');
      }

      // Process each shapefile and combine results
      const allFeatures: LandParcelFeature[] = [];
      const sourceFiles: string[] = [];

      for (const shapefilePath of shapefiles) {
        console.log(`📍 Processing shapefile: ${path.basename(shapefilePath)}`);
        const features = await this.processShapefile(shapefilePath, listingId, state, options);
        allFeatures.push(...features);
        sourceFiles.push(path.basename(shapefilePath));
      }

      // Create final collection
      const collection: LandParcelCollection = {
        type: 'FeatureCollection',
        features: allFeatures,
        metadata: {
          listing_id: listingId,
          state,
          total_parcels: allFeatures.length,
          total_acres: this.calculateTotalAcres(allFeatures),
          processed_at: new Date().toISOString(),
          source_files: sourceFiles
        }
      };

      console.log(`✅ Processed ${collection.features.length} land parcels`);
      console.log(`📏 Total area: ${collection.metadata?.total_acres?.toFixed(2)} acres`);

      // Cache results if requested
      if (options.cacheResults !== false) {
        await this.cacheResults(collection, listingId);
      }

      return collection;

    } finally {
      // Cleanup extracted directory
      this.cleanupDirectory(extractDir);
    }
  }

  private async extractZipFile(zipFilePath: string): Promise<string> {
    const zip = new AdmZip(zipFilePath);
    const extractDir = path.join(path.dirname(zipFilePath), 'temp_' + Date.now());
    zip.extractAllTo(extractDir, true);
    return extractDir;
  }

  private findShapefiles(directory: string): string[] {
    const shapefiles: string[] = [];
    
    const scanDirectory = (dir: string) => {
      const items = fs.readdirSync(dir);
      for (const item of items) {
        const fullPath = path.join(dir, item);
        const stat = fs.statSync(fullPath);
        
        if (stat.isDirectory()) {
          scanDirectory(fullPath);
        } else if (path.extname(item).toLowerCase() === '.shp') {
          shapefiles.push(fullPath);
        }
      }
    };
    
    scanDirectory(directory);
    return shapefiles;
  }

  private async processShapefile(
    shapefilePath: string,
    listingId: string,
    state: string,
    options: GISProcessingOptions
  ): Promise<LandParcelFeature[]> {
    const features: LandParcelFeature[] = [];
    
    try {
      // Open and read shapefile
      const source = await shapefile.open(shapefilePath);
      let result = await source.read();
      let featureIndex = 0;

      while (!result.done) {
        const feature = result.value;
        if (feature && feature.geometry) {
          // Convert to our land parcel format
          const landParcel = this.convertToLandParcel(feature, listingId, state, featureIndex, options);
          if (landParcel) {
            features.push(landParcel);
          }
        }
        
        result = await source.read();
        featureIndex++;
      }
      
    } catch (error) {
      console.error(`❌ Error processing shapefile ${shapefilePath}:`, error);
      throw error;
    }
    
    return features;
  }

  private convertToLandParcel(
    feature: any,
    listingId: string,
    state: string,
    index: number,
    options: GISProcessingOptions
  ): LandParcelFeature | null {
    try {
      // Transform coordinates if they appear to be in Web Mercator
      feature = this.transformCoordinatesIfNeeded(feature);

      // Validate and clean geometry
      if (options.validateGeometry) {
        try {
          // Clean coordinates to remove duplicate points
          feature = turf.cleanCoords(feature);
        } catch (cleanError) {
          console.warn(`⚠️ Could not clean geometry for feature ${index}, skipping`);
          return null;
        }
      }

      // Simplify geometry if requested
      if (options.simplifyTolerance && options.simplifyTolerance > 0) {
        feature = turf.simplify(feature, { tolerance: options.simplifyTolerance });
      }

      // Extract parcel ID from properties
      const parcelId = this.extractParcelId(feature.properties, index);

      // Calculate area in acres if geometry is valid
      let acres: number | undefined;
      try {
        const area = turf.area(feature); // Returns square meters
        acres = area * 0.000247105; // Convert to acres
      } catch (areaError) {
        console.warn(`⚠️ Could not calculate area for parcel ${parcelId}`);
      }

      // Build properties object
      const properties = {
        listing_id: listingId,
        parcel_id: parcelId,
        state,
        acres,
        description: 'Oil & Gas Lease',
        ...feature.properties // Include all original shapefile attributes
      };

      const landParcel: LandParcelFeature = {
        type: 'Feature',
        id: `${listingId}_${parcelId}`,
        properties,
        geometry: feature.geometry
      };

      return landParcel;

    } catch (error) {
      console.error(`❌ Error converting feature ${index} to land parcel:`, error);
      return null;
    }
  }

  private extractParcelId(properties: any, fallbackIndex: number): string {
    // Try common parcel ID field names from EnergyNet shapefiles
    const possibleIdFields = [
      'PARCEL_ID', 'PARCEL', 'ID', 'TRACT_ID', 'TRACT',
      'SECTION', 'SEC', 'TOWNSHIP', 'RANGE', 'LEGAL',
      'parcel_id', 'parcel', 'id', 'tract_id', 'tract'
    ];

    for (const field of possibleIdFields) {
      if (properties && properties[field]) {
        return String(properties[field]).trim();
      }
    }

    // If multiple fields exist, try to combine them (e.g., Section-Township-Range)
    const section = properties?.SECTION || properties?.SEC;
    const township = properties?.TOWNSHIP || properties?.TWP;
    const range = properties?.RANGE || properties?.RNG;
    
    if (section && township && range) {
      return `${section} ${township}-${range}`;
    }

    // Fallback to index-based ID
    return String(fallbackIndex + 1).padStart(3, '0');
  }

  private calculateTotalAcres(features: LandParcelFeature[]): number {
    return features.reduce((total, feature) => {
      return total + (feature.properties.acres || 0);
    }, 0);
  }

  private async cacheResults(collection: LandParcelCollection, listingId: string): Promise<void> {
    const filename = `${listingId}_parcels.geojson`;
    const filepath = path.join(this.outputDir, filename);
    
    try {
      fs.writeFileSync(filepath, JSON.stringify(collection, null, 2));
      console.log(`💾 Cached results to: ${filepath}`);
    } catch (error) {
      console.warn(`⚠️ Failed to cache results:`, error);
    }
  }

  private cleanupDirectory(directory: string): void {
    try {
      fs.rmSync(directory, { recursive: true, force: true });
      console.log(`🧹 Cleaned up: ${directory}`);
    } catch (error) {
      console.warn(`⚠️ Failed to cleanup directory ${directory}:`, error);
    }
  }

  async loadCachedResults(listingId: string): Promise<LandParcelCollection | null> {
    const filename = `${listingId}_parcels.geojson`;
    const filepath = path.join(this.outputDir, filename);
    
    try {
      if (fs.existsSync(filepath)) {
        const data = fs.readFileSync(filepath, 'utf-8');
        return JSON.parse(data);
      }
    } catch (error) {
      console.warn(`⚠️ Failed to load cached results for ${listingId}:`, error);
    }
    
    return null;
  }

  private transformCoordinatesIfNeeded(feature: any): any {
    if (!feature || !feature.geometry || !feature.geometry.coordinates) {
      return feature;
    }

    // Check if coordinates appear to be Web Mercator (very large numbers)
    if (this.isWebMercatorCoordinates(feature.geometry.coordinates)) {
      console.log(`🔄 Transforming Web Mercator coordinates to WGS84...`);
      feature.geometry.coordinates = this.transformCoordinates(
        feature.geometry.coordinates, 
        feature.geometry.type
      );
    }

    return feature;
  }

  private isWebMercatorCoordinates(coordinates: any): boolean {
    // Web Mercator coordinates are typically very large (±20,037,508 meters max)
    // WGS84 coordinates are small (±180 degrees max)
    const flatCoords = this.flattenCoordinates(coordinates);
    if (flatCoords.length === 0) return false;

    // Check if any coordinate is larger than 1000 (definitely not WGS84 degrees)
    return flatCoords.some(coord => Math.abs(coord[0]) > 1000 || Math.abs(coord[1]) > 1000);
  }

  private flattenCoordinates(coordinates: any): number[][] {
    const result: number[][] = [];
    
    const flatten = (coords: any) => {
      if (Array.isArray(coords)) {
        if (coords.length === 2 && typeof coords[0] === 'number' && typeof coords[1] === 'number') {
          // This is a coordinate pair [x, y]
          result.push(coords);
        } else {
          // This is an array of coordinates, recurse
          coords.forEach(flatten);
        }
      }
    };
    
    flatten(coordinates);
    return result;
  }

  private transformCoordinates(coordinates: any, geometryType: string): any {
    const transformCoordPair = (coord: number[]): number[] => {
      if (coord.length !== 2) return coord;
      return this.webMercatorToWGS84(coord[0], coord[1]);
    };

    const transformCoordArray = (coords: any): any => {
      if (Array.isArray(coords)) {
        if (coords.length === 2 && typeof coords[0] === 'number' && typeof coords[1] === 'number') {
          // This is a coordinate pair
          return transformCoordPair(coords);
        } else {
          // This is an array of coordinates
          return coords.map(transformCoordArray);
        }
      }
      return coords;
    };

    return transformCoordArray(coordinates);
  }

  private webMercatorToWGS84(x: number, y: number): number[] {
    // Web Mercator (EPSG:3857) to WGS84 (EPSG:4326) transformation
    const earthRadius = 6378137; // Earth's radius in meters (WGS84)
    const originShift = Math.PI * earthRadius; // 20037508.342789244

    // Transform X (longitude)
    let lng = (x / originShift) * 180;

    // Transform Y (latitude) 
    let lat = (y / originShift) * 180;
    lat = 180 / Math.PI * (2 * Math.atan(Math.exp(lat * Math.PI / 180)) - Math.PI / 2);

    // Clamp to valid ranges
    lng = Math.max(-180, Math.min(180, lng));
    lat = Math.max(-90, Math.min(90, lat));

    return [lng, lat];
  }

  async validateGeometry(parcel: LandParcelFeature): Promise<boolean> {
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

  async calculateArea(parcel: LandParcelFeature): Promise<number> {
    try {
      const area = turf.area(parcel); // Returns square meters
      return area * 0.000247105; // Convert to acres
    } catch (error) {
      return parcel.properties.acres || 0;
    }
  }
}