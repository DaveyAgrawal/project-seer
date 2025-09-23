/**
 * Header detection and field parsing utilities
 */

export interface FieldMapping {
  latitude?: string;
  longitude?: string;
  depth?: string;
  temperature?: string;
  geometry?: string;
}

export class HeaderDetector {
  /**
   * Detect field mappings from CSV headers (case-insensitive, flexible matching)
   */
  static detectFields(headers: string[]): FieldMapping {
    const mapping: FieldMapping = {};
    
    const normalizedHeaders = headers.map(h => h.toLowerCase().replace(/[_\s-]/g, ''));
    
    for (let i = 0; i < headers.length; i++) {
      const original = headers[i];
      const normalized = normalizedHeaders[i];
      
      // Latitude detection
      if (!mapping.latitude && this.matchesPattern(normalized, ['lat', 'latitude', 'y'])) {
        mapping.latitude = original;
      }
      
      // Longitude detection  
      else if (!mapping.longitude && this.matchesPattern(normalized, ['lon', 'lng', 'longitude', 'x'])) {
        mapping.longitude = original;
      }
      
      // Depth detection
      else if (!mapping.depth && this.matchesPattern(normalized, ['depth', 'depthm', 'measureddepth'])) {
        mapping.depth = original;
      }
      
      // Temperature detection
      else if (!mapping.temperature && this.matchesPattern(normalized, ['temperature', 'temperaturef', 'tempf', 'temp', 'temperaturec'])) {
        mapping.temperature = original;
      }
      
      // Geometry detection
      else if (!mapping.geometry && this.matchesPattern(normalized, ['geometry', 'geom', 'wkt'])) {
        mapping.geometry = original;
      }
    }
    
    return mapping;
  }

  private static matchesPattern(normalized: string, patterns: string[]): boolean {
    return patterns.some(pattern => normalized === pattern || normalized.includes(pattern));
  }

  /**
   * Determine if temperature field contains Celsius based on field name
   */
  static isTemperatureCelsius(fieldName: string): boolean {
    const normalized = fieldName.toLowerCase();
    return normalized.includes('_c') || normalized.includes('celsius') || normalized.endsWith('c');
  }

  /**
   * Determine if depth field is in feet based on field name
   */
  static isDepthFeet(fieldName: string): boolean {
    const normalized = fieldName.toLowerCase();
    return normalized.includes('_ft') || normalized.includes('feet') || normalized.includes('foot');
  }
}

export class UnitConverter {
  /**
   * Convert Celsius to Fahrenheit
   */
  static celsiusToFahrenheit(celsius: number): number {
    return (celsius * 9/5) + 32;
  }

  /**
   * Convert feet to meters
   */
  static feetToMeters(feet: number): number {
    return feet * 0.3048;
  }

  /**
   * Normalize depth to meters (positive value below surface)
   */
  static normalizeDepth(depth: number, isInFeet: boolean = false): number {
    let meters = isInFeet ? this.feetToMeters(depth) : depth;
    return Math.abs(meters); // Ensure positive (below surface)
  }

  /**
   * Normalize temperature to Fahrenheit
   */
  static normalizeTemperature(temp: number, isCelsius: boolean = false): number {
    return isCelsius ? this.celsiusToFahrenheit(temp) : temp;
  }
}

export class DataValidator {
  /**
   * Validate and clean numeric value
   */
  static parseNumeric(value: any, sentinelValues: number[] = [-9999, -99999, -999999]): number | null {
    if (value === null || value === undefined || value === '') {
      return null;
    }

    let num: number;
    if (typeof value === 'string') {
      // Remove non-numeric characters except decimal point and negative sign
      const cleaned = value.replace(/[^-\d.]/g, '');
      num = parseFloat(cleaned);
    } else {
      num = Number(value);
    }

    if (isNaN(num)) {
      return null;
    }

    // Check for sentinel values (missing data indicators)
    if (sentinelValues.includes(num)) {
      return null;
    }

    return num;
  }

  /**
   * Parse voltage ranges and extract maximum value
   */
  static parseVoltage(value: any): number | null {
    if (!value) return null;
    
    const str = String(value).toLowerCase();
    
    // Remove "kv" and other text
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

  /**
   * Validate coordinates are within reasonable bounds
   */
  static isValidCoordinate(lat: number, lon: number): boolean {
    return lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180;
  }

  /**
   * Check if coordinates are within US bounds (loose check)
   */
  static isWithinUSBounds(lat: number, lon: number): boolean {
    // Loose bounding box covering all US territories
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