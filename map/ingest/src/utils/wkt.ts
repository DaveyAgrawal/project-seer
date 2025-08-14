import * as wkt from 'wellknown';

export interface GeometryInfo {
  type: string;
  coordinates: any;
  srid?: number;
}

export class WKTProcessor {
  /**
   * Parse WKT geometry string to GeoJSON-like object
   */
  static parseWKT(wktString: string, defaultSRID: number = 4326): GeometryInfo | null {
    try {
      // Extract SRID if present (e.g., "SRID=3857;POINT(x y)")
      let srid = defaultSRID;
      let cleanWKT = wktString.trim();

      const sridMatch = cleanWKT.match(/^SRID=(\d+);(.+)/i);
      if (sridMatch) {
        srid = parseInt(sridMatch[1]);
        cleanWKT = sridMatch[2];
      }

      const geometry = wkt.parse(cleanWKT);
      if (!geometry) {
        return null;
      }

      return {
        type: geometry.type,
        coordinates: geometry.coordinates,
        srid
      };
    } catch (error) {
      console.warn(`Failed to parse WKT: ${wktString}`, error);
      return null;
    }
  }

  /**
   * Convert coordinates from one SRID to another (simplified for 3857->4326)
   */
  static transformCoordinates(coords: number[], fromSRID: number, toSRID: number): number[] {
    if (fromSRID === toSRID) {
      return coords;
    }

    // Simplified Web Mercator (3857) to WGS84 (4326) transformation
    if (fromSRID === 3857 && toSRID === 4326) {
      const [x, y] = coords;
      const lon = (x / 20037508.34) * 180;
      const lat = (Math.atan(Math.exp((y / 20037508.34) * Math.PI)) / (Math.PI / 4) - 1) * 90;
      return [lon, lat];
    }

    // For other transformations, would need proj4 or similar
    console.warn(`Transformation from SRID ${fromSRID} to ${toSRID} not supported`);
    return coords;
  }

  /**
   * Get longitude/latitude from geometry
   */
  static getLonLat(geometry: GeometryInfo): [number, number] | null {
    let coords = geometry.coordinates;

    // Transform to 4326 if needed
    if (geometry.srid && geometry.srid !== 4326) {
      if (geometry.type === 'Point') {
        coords = this.transformCoordinates(coords, geometry.srid, 4326);
      } else {
        console.warn(`Coordinate transformation for ${geometry.type} not implemented`);
        return null;
      }
    }

    if (geometry.type === 'Point') {
      const [lon, lat] = coords;
      if (typeof lon === 'number' && typeof lat === 'number') {
        return [lon, lat];
      }
    }

    return null;
  }

  /**
   * Validate coordinates are within valid ranges
   */
  static isValidLonLat(lon: number, lat: number): boolean {
    return lon >= -180 && lon <= 180 && lat >= -90 && lat <= 90;
  }
}