/**
 * Data validation utilities for geospatial ingestion
 */

export interface ValidationResult<T> {
  isValid: boolean;
  data?: T;
  errors: string[];
  warnings: string[];
}

export interface GeothermalRecord {
  latitude: number;
  longitude: number;
  depth_m?: number;
  temperature_f?: number;
}

export interface TransmissionLineRecord {
  id_text?: string;
  owner?: string;
  status?: string;
  volt_class?: string;
  kv?: number;
  geometry: any; // GeoJSON geometry
}

export class GeothermalValidator {
  static validate(record: any): ValidationResult<GeothermalRecord> {
    const result: ValidationResult<GeothermalRecord> = {
      isValid: false,
      errors: [],
      warnings: []
    };

    const lat = record.latitude;
    const lon = record.longitude;
    const depth = record.depth_m;
    const temp = record.temperature_f;

    // Required fields validation
    if (typeof lat !== 'number' || isNaN(lat)) {
      result.errors.push('Invalid or missing latitude');
    } else if (lat < -90 || lat > 90) {
      result.errors.push(`Latitude out of range: ${lat}`);
    }

    if (typeof lon !== 'number' || isNaN(lon)) {
      result.errors.push('Invalid or missing longitude');
    } else if (lon < -180 || lon > 180) {
      result.errors.push(`Longitude out of range: ${lon}`);
    }

    // Optional field validation
    if (depth !== null && depth !== undefined) {
      if (typeof depth !== 'number' || isNaN(depth)) {
        result.warnings.push('Invalid depth value, will be set to null');
      } else if (depth < 0) {
        result.warnings.push('Negative depth value, will be converted to positive');
      } else if (depth > 15000) {
        result.warnings.push(`Very deep measurement: ${depth}m`);
      }
    }

    if (temp !== null && temp !== undefined) {
      if (typeof temp !== 'number' || isNaN(temp)) {
        result.warnings.push('Invalid temperature value, will be set to null');
      } else if (temp < -100 || temp > 1000) {
        result.warnings.push(`Temperature seems out of reasonable range: ${temp}°F`);
      }
    }

    // Check if coordinates are within US bounds
    if (typeof lat === 'number' && typeof lon === 'number' && !isNaN(lat) && !isNaN(lon)) {
      if (!this.isWithinUSBounds(lat, lon)) {
        result.warnings.push('Coordinates appear to be outside US bounds');
      }
    }

    result.isValid = result.errors.length === 0;

    if (result.isValid) {
      result.data = {
        latitude: lat,
        longitude: lon,
        depth_m: (depth !== null && depth !== undefined && !isNaN(depth)) ? Math.abs(depth) : undefined,
        temperature_f: (temp !== null && temp !== undefined && !isNaN(temp)) ? temp : undefined
      };
    }

    return result;
  }

  private static isWithinUSBounds(lat: number, lon: number): boolean {
    return (
      // CONUS
      (lat >= 24 && lat <= 49.6 && lon >= -125 && lon <= -66.5) ||
      // Alaska
      (lat >= 49 && lat <= 72 && lon >= -170 && lon <= -130) ||
      // Hawaii
      (lat >= 18.9 && lat <= 22.4 && lon >= -161 && lon <= -154) ||
      // Puerto Rico  
      (lat >= 17.6 && lat <= 18.6 && lon >= -67.5 && lon <= -65)
    );
  }
}

export class TransmissionLineValidator {
  static validate(feature: any): ValidationResult<TransmissionLineRecord> {
    const result: ValidationResult<TransmissionLineRecord> = {
      isValid: false,
      errors: [],
      warnings: []
    };

    // Validate geometry
    const geometry = feature.geometry;
    if (!geometry) {
      result.errors.push('Missing geometry');
      return result;
    }

    if (!['LineString', 'MultiLineString'].includes(geometry.type)) {
      result.errors.push(`Unsupported geometry type: ${geometry.type}`);
      return result;
    }

    if (!geometry.coordinates || !Array.isArray(geometry.coordinates)) {
      result.errors.push('Invalid geometry coordinates');
      return result;
    }

    // Validate properties
    const props = feature.properties || {};
    
    let kv = null;
    if (props.voltage !== undefined && props.voltage !== null) {
      kv = this.parseVoltage(props.voltage);
      if (kv === null && props.voltage !== '') {
        result.warnings.push(`Could not parse voltage: ${props.voltage}`);
      }
    }

    // Normalize geometry to MultiLineString
    let normalizedGeometry;
    if (geometry.type === 'LineString') {
      normalizedGeometry = {
        type: 'MultiLineString',
        coordinates: [geometry.coordinates]
      };
    } else {
      normalizedGeometry = geometry;
    }

    result.isValid = true;
    result.data = {
      id_text: this.cleanString(props.id || props.identifier),
      owner: this.cleanString(props.owner || props.operator),
      status: this.cleanString(props.status),
      volt_class: this.cleanString(props.volt_class || props.voltage_class),
      kv: kv || undefined,
      geometry: normalizedGeometry
    };

    return result;
  }

  private static parseVoltage(value: any): number | null {
    if (value === null || value === undefined || value === '') {
      return null;
    }

    // Check for sentinel values
    const sentinels = [-9999, -99999, -999999];
    if (typeof value === 'number' && sentinels.includes(value)) {
      return null;
    }

    const str = String(value).toLowerCase();
    
    // Remove "kv" and other text, keep numbers, decimal points, and hyphens
    const cleaned = str.replace(/[^\d\-.,]/g, '');
    
    if (!cleaned) return null;
    
    // Handle ranges like "138-230" -> take max (230)
    if (cleaned.includes('-')) {
      const parts = cleaned.split('-');
      const numbers = parts.map(p => parseFloat(p)).filter(n => !isNaN(n));
      return numbers.length > 0 ? Math.max(...numbers) : null;
    }
    
    const num = parseFloat(cleaned);
    return isNaN(num) ? null : num;
  }

  private static cleanString(value: any): string | undefined {
    if (value === null || value === undefined || value === '') {
      return undefined;
    }
    
    const str = String(value).trim();
    return str === '' ? undefined : str;
  }
}