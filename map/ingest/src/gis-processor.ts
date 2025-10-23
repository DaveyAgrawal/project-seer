// GIS processing types and utilities for land parcel data

export interface LandParcelFeature {
  type: 'Feature';
  id?: string | number;
  properties: {
    parcel_id: string;
    listing_id?: string;
    sale_group?: string;
    state?: string;
    region?: string;
    acres?: number;
    description?: string;
    [key: string]: any; // Allow additional properties
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

// Placeholder GIS processor for multi-listing scraper compatibility
export class GISProcessor {
  constructor(private options: GISProcessingOptions = {}) {}

  async processShapefile(
    inputPath: string,
    outputPath: string,
    listingMetadata: any = {}
  ): Promise<GISProcessingResult> {
    // This would be implemented with actual GIS processing logic
    // For now, return a placeholder result
    return {
      success: false,
      inputFile: inputPath,
      parcelsProcessed: 0,
      errors: ['GIS processing not implemented - use existing pipeline']
    };
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
    // Basic area calculation placeholder
    // In reality, this would use a proper GIS library like @turf/turf
    return parcel.properties.acres || 0;
  }
}