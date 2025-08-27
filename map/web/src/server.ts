import express from 'express';
import cors from 'cors';
import compression from 'compression';
import { Pool } from 'pg';
import { config } from 'dotenv';
import path from 'path';

// Load environment variables
config();

interface DatabaseConfig {
  host: string;
  port: number;
  database: string;
  user: string;
  password: string;
}

class GeospatialWebServer {
  private app: express.Application;
  private pool!: Pool;
  private port: number;

  constructor() {
    this.app = express();
    this.port = parseInt(process.env.PORT || '3000');
    this.setupDatabase();
    this.setupMiddleware();
    this.setupRoutes();
  }

  private setupDatabase(): void {
    const config = this.getDatabaseConfig();
    
    this.pool = new Pool({
      host: config.host,
      port: config.port,
      database: config.database,
      user: config.user,
      password: config.password,
      max: 20,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 2000,
    });

    this.pool.on('error', (err) => {
      console.error('Unexpected error on idle client', err);
    });
  }

  private getDatabaseConfig(): DatabaseConfig {
    const dbUrl = process.env.DATABASE_URL;
    
    if (dbUrl) {
      const url = new URL(dbUrl);
      return {
        host: url.hostname,
        port: parseInt(url.port) || 5432,
        database: url.pathname.slice(1),
        user: url.username,
        password: url.password
      };
    }

    return {
      host: process.env.POSTGRES_HOST || 'localhost',
      port: parseInt(process.env.POSTGRES_PORT || '5432'),
      database: process.env.POSTGRES_DB || 'geospatial',
      user: process.env.POSTGRES_USER || 'geouser',
      password: process.env.POSTGRES_PASSWORD || 'geopass'
    };
  }

  private setupMiddleware(): void {
    // Enable CORS for all routes
    this.app.use(cors());
    
    // Enable gzip compression
    this.app.use(compression());
    
    // Parse JSON bodies
    this.app.use(express.json());
    
    // Serve static files from public directory
    this.app.use(express.static(path.join(__dirname, '../public')));
    
    // Logging middleware
    this.app.use((req, res, next) => {
      console.log(`${new Date().toISOString()} - ${req.method} ${req.path}`);
      next();
    });
  }

  private setupRoutes(): void {
    // API Routes
    this.app.get('/api/datasets', this.getDatasets.bind(this));
    this.app.get('/api/datasets/:id/stats', this.getDatasetStats.bind(this));
    this.app.get('/api/config', this.getConfig.bind(this));
    this.app.get('/api/health', this.getHealth.bind(this));
    this.app.post('/api/temperature-lookup', this.temperatureLookup.bind(this));
    this.app.post('/api/geothermal-tile-data', this.getGeothermalTileData.bind(this));
    
    // Serve main application
    this.app.get('/', (req, res) => {
      res.sendFile(path.join(__dirname, '../public/index.html'));
    });
    
    // Handle 404
    this.app.use((req, res) => {
      res.status(404).json({ error: 'Not Found' });
    });
    
    // Error handler
    this.app.use((err: any, req: express.Request, res: express.Response, next: express.NextFunction) => {
      console.error('Server Error:', err);
      res.status(500).json({ error: 'Internal Server Error' });
    });
  }

  private async getDatasets(req: express.Request, res: express.Response): Promise<void> {
    try {
      const result = await this.pool.query(`
        SELECT 
          id,
          layer_name,
          table_name,
          geometry_type,
          minzoom,
          maxzoom,
          attributes,
          style,
          created_at
        FROM dataset_registry 
        ORDER BY created_at DESC
      `);

      // Add table statistics for each dataset
      const datasets = await Promise.all(
        result.rows.map(async (dataset) => {
          try {
            const statsResult = await this.pool.query(
              `SELECT COUNT(*) as row_count FROM ${dataset.table_name}`
            );
            
            return {
              ...dataset,
              row_count: parseInt(statsResult.rows[0]?.row_count || '0')
            };
          } catch (error) {
            console.warn(`Failed to get stats for ${dataset.table_name}:`, error);
            return {
              ...dataset,
              row_count: 0
            };
          }
        })
      );

      res.json(datasets);
    } catch (error) {
      console.error('Error fetching datasets:', error);
      res.status(500).json({ error: 'Failed to fetch datasets' });
    }
  }

  private async getDatasetStats(req: express.Request, res: express.Response): Promise<void> {
    try {
      const { id } = req.params;
      
      // Get dataset info
      const datasetResult = await this.pool.query(
        'SELECT * FROM dataset_registry WHERE id = $1',
        [id]
      );
      
      if (datasetResult.rows.length === 0) {
        res.status(404).json({ error: 'Dataset not found' });
        return;
      }
      
      const dataset = datasetResult.rows[0];
      const tableName = dataset.table_name;
      
      // Get basic stats
      const statsResult = await this.pool.query(`
        SELECT 
          COUNT(*) as total_rows,
          pg_size_pretty(pg_total_relation_size($1)) as table_size
      `, [tableName]);
      
      let geometryStats = {};
      
      // Get geometry-specific stats
      if (dataset.geometry_type === 'POINT') {
        const geoStatsResult = await this.pool.query(`
          SELECT 
            COUNT(*) FILTER (WHERE geom IS NOT NULL) as with_geometry,
            COUNT(*) FILTER (WHERE temperature_f IS NOT NULL) as with_temperature,
            COUNT(*) FILTER (WHERE depth_m IS NOT NULL) as with_depth,
            MIN(temperature_f) as min_temperature,
            MAX(temperature_f) as max_temperature,
            AVG(temperature_f) as avg_temperature,
            MIN(depth_m) as min_depth,
            MAX(depth_m) as max_depth,
            AVG(depth_m) as avg_depth,
            ST_Extent(geom) as bbox
          FROM ${tableName}
        `);
        
        geometryStats = geoStatsResult.rows[0] || {};
      } else if (dataset.geometry_type === 'MULTILINESTRING') {
        const geoStatsResult = await this.pool.query(`
          SELECT 
            COUNT(*) FILTER (WHERE geom IS NOT NULL) as with_geometry,
            COUNT(*) FILTER (WHERE kv IS NOT NULL) as with_voltage,
            MIN(kv) as min_voltage,
            MAX(kv) as max_voltage,
            AVG(kv) as avg_voltage,
            COUNT(*) FILTER (WHERE owner IS NOT NULL) as with_owner,
            ST_Extent(geom) as bbox
          FROM ${tableName}
        `);
        
        geometryStats = geoStatsResult.rows[0] || {};
      }
      
      const stats = {
        dataset,
        ...statsResult.rows[0],
        ...geometryStats
      };
      
      res.json(stats);
    } catch (error) {
      console.error('Error fetching dataset stats:', error);
      res.status(500).json({ error: 'Failed to fetch dataset stats' });
    }
  }

  private async getConfig(req: express.Request, res: express.Response): Promise<void> {
    try {
      const config = {
        tileserver: {
          url: process.env.TILESERVER_URL || 'http://localhost:7800',
          endpoints: {
            metadata: '/index.json',
            tiles: '/{table}/{z}/{x}/{y}.mvt'
          }
        },
        map: {
          center: [-98.5795, 39.8282], // Geographic center of US
          zoom: 4,
          maxBounds: [
            [-170, 15],  // Southwest coordinates
            [-60, 72]    // Northeast coordinates (covers all US territories)
          ],
          style: process.env.MAP_STYLE || 'https://demotiles.maplibre.org/style.json'
        },
        layers: {
          transmission_lines: {
            minzoom: 3,
            maxzoom: 14,
            voltageClasses: {
              unknown: { color: '#999999', width: 1 },
              low: { color: '#4CAF50', width: 1 },        // < 69 kV
              medium_low: { color: '#FF9800', width: 2 }, // 69-138 kV
              medium: { color: '#2196F3', width: 3 },     // 138-230 kV
              high: { color: '#9C27B0', width: 4 },       // 230-345 kV
              extra_high: { color: '#F44336', width: 5 }, // 345-500 kV
              ultra_high: { color: '#000000', width: 6 }  // > 500 kV
            }
          },
          geothermal_points: {
            minzoom: 3,
            maxzoom: 14,
            temperatureScale: {
              cold: { color: '#2196F3', temp: 100 },      // Blue for < 100°F
              warm: { color: '#4CAF50', temp: 150 },      // Green for 100-150°F
              hot: { color: '#FF9800', temp: 200 },       // Orange for 150-200°F
              very_hot: { color: '#F44336', temp: 250 },  // Red for 200-250°F
              extreme: { color: '#9C27B0', temp: 300 }    // Purple for > 250°F
            }
          }
        }
      };

      res.json(config);
    } catch (error) {
      console.error('Error fetching config:', error);
      res.status(500).json({ error: 'Failed to fetch config' });
    }
  }

  private async getHealth(req: express.Request, res: express.Response): Promise<void> {
    try {
      // Test database connection
      await this.pool.query('SELECT 1');
      
      res.json({
        status: 'healthy',
        timestamp: new Date().toISOString(),
        services: {
          database: 'connected',
          tileserver: process.env.TILESERVER_URL || 'http://localhost:7800'
        }
      });
    } catch (error) {
      res.status(503).json({
        status: 'unhealthy',
        timestamp: new Date().toISOString(),
        error: 'Database connection failed'
      });
    }
  }

  public async start(): Promise<void> {
    return new Promise((resolve) => {
      this.app.listen(this.port, () => {
        console.log(`🌐 Geospatial web server running on http://localhost:${this.port}`);
        console.log(`📡 Tile server expected at: ${process.env.TILESERVER_URL || 'http://localhost:7800'}`);
        resolve();
      });
    });
  }

  private async temperatureLookup(req: express.Request, res: express.Response): Promise<void> {
    try {
      const { lat, lng, depth } = req.body;
      
      if (typeof lat !== 'number' || typeof lng !== 'number' || typeof depth !== 'number') {
        res.status(400).json({ error: 'Invalid coordinates or depth' });
        return;
      }
      
      // Find the closest geothermal point within 50km
      const result = await this.pool.query(`
        SELECT 
          temperature_f,
          depth_m,
          latitude,
          longitude,
          ST_Distance(
            ST_MakePoint($2, $1)::geography,
            geom::geography
          ) / 1000 as distance_km
        FROM geothermal_points 
        WHERE ST_DWithin(
          ST_MakePoint($2, $1)::geography,
          geom::geography,
          50000  -- 50km radius
        )
        AND depth_m BETWEEN $3 - 100 AND $3 + 100  -- Within 100m depth range
        ORDER BY ST_Distance(
          ST_MakePoint($2, $1)::geography,
          geom::geography
        )
        LIMIT 1
      `, [lat, lng, depth]);
      
      if (result.rows.length > 0) {
        const point = result.rows[0];
        res.json({
          temperature: point.temperature_f,
          distance: point.distance_km,
          actual_depth: point.depth_m,
          coordinates: [point.latitude, point.longitude]
        });
      } else {
        res.json({
          temperature: null,
          distance: null,
          message: 'No geothermal data found within 50km and ±100m depth'
        });
      }
      
    } catch (error) {
      console.error('Temperature lookup error:', error);
      res.status(500).json({ error: 'Internal server error' });
    }
  }

  private async getGeothermalTileData(req: express.Request, res: express.Response): Promise<void> {
    try {
      const { bounds, depth, depthTolerance } = req.body;
      
      if (!bounds || typeof depth !== 'number' || typeof depthTolerance !== 'number') {
        res.status(400).json({ error: 'Invalid request parameters' });
        return;
      }
      
      const { west, east, south, north } = bounds;
      
      // Query geothermal points within the tile bounds and depth range
      const result = await this.pool.query(`
        SELECT 
          gid,
          latitude,
          longitude, 
          temperature_f,
          depth_m,
          ST_X(geom) as lng,
          ST_Y(geom) as lat
        FROM geothermal_points 
        WHERE geom && ST_MakeEnvelope($1, $2, $3, $4, 4326)
          AND depth_m BETWEEN ($5::numeric - $6::numeric) AND ($5::numeric + $6::numeric)
        ORDER BY gid
        LIMIT 1000
      `, [west, south, east, north, depth, depthTolerance]);
      
      const points = result.rows.map(row => ({
        coordinates: [row.lng, row.lat],
        temperature_f: row.temperature_f,
        depth_m: row.depth_m,
        latitude: row.lat,
        longitude: row.lng,
        gid: row.gid
      }));
      
      console.log(`🌡️ API: Found ${points.length} geothermal points in bounds [${west}, ${south}, ${east}, ${north}] at depth ${depth}±${depthTolerance}m`);
      
      res.json({
        points,
        bounds,
        depth,
        count: points.length
      });
      
    } catch (error) {
      console.error('Geothermal tile data error:', error);
      res.status(500).json({ error: 'Internal server error' });
    }
  }

  public async stop(): Promise<void> {
    await this.pool.end();
  }
}

// Start server if this file is run directly
if (require.main === module) {
  const server = new GeospatialWebServer();
  
  server.start().catch(error => {
    console.error('Failed to start server:', error);
    process.exit(1);
  });
  
  // Graceful shutdown
  process.on('SIGINT', async () => {
    console.log('Shutting down gracefully...');
    await server.stop();
    process.exit(0);
  });
}