import { Pool, PoolClient } from 'pg';
import { config } from 'dotenv';

// Load environment variables
config();

export interface DatabaseConfig {
  host: string;
  port: number;
  database: string;
  user: string;
  password: string;
}

export class DatabaseManager {
  private pool: Pool;
  
  constructor(dbConfig?: DatabaseConfig) {
    const config = dbConfig || this.getDefaultConfig();
    
    this.pool = new Pool({
      host: config.host,
      port: config.port,
      database: config.database,
      user: config.user,
      password: config.password,
      max: 20, // Maximum connections
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 2000,
    });

    // Handle pool errors
    this.pool.on('error', (err) => {
      console.error('Unexpected error on idle client', err);
      process.exit(-1);
    });
  }

  private getDefaultConfig(): DatabaseConfig {
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

  async getClient(): Promise<PoolClient> {
    return this.pool.connect();
  }

  async query(text: string, params?: any[]): Promise<any> {
    const client = await this.getClient();
    try {
      const result = await client.query(text, params);
      return result;
    } finally {
      client.release();
    }
  }

  async transaction<T>(callback: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.getClient();
    try {
      await client.query('BEGIN');
      const result = await callback(client);
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async registerDataset(
    layerName: string,
    tableName: string,
    geometryType: string,
    minzoom: number = 3,
    maxzoom: number = 14,
    attributes: any = {},
    style: any = {}
  ): Promise<void> {
    await this.query(
      `SELECT register_dataset($1, $2, $3, $4, $5, $6, $7)`,
      [layerName, tableName, geometryType, minzoom, maxzoom, JSON.stringify(attributes), JSON.stringify(style)]
    );
  }

  async tableExists(tableName: string): Promise<boolean> {
    const result = await this.query(
      `SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_name = $1
      )`,
      [tableName]
    );
    return result.rows[0].exists;
  }

  async getTableRowCount(tableName: string): Promise<number> {
    const result = await this.query(`SELECT COUNT(*) FROM ${tableName}`);
    return parseInt(result.rows[0].count);
  }

  async close(): Promise<void> {
    await this.pool.end();
  }

  // Helper for bulk operations
  async setBulkOperationSettings(): Promise<void> {
    await this.query('SET synchronous_commit = off');
    await this.query('SET checkpoint_completion_target = 0.9');
    await this.query('SET wal_buffers = 16MB');
    await this.query('SET max_wal_size = 1GB');
  }

  async resetSettings(): Promise<void> {
    await this.query('SET synchronous_commit = on');
  }

  async analyze(tableName: string): Promise<void> {
    console.log(`Analyzing table: ${tableName}`);
    await this.query(`ANALYZE ${tableName}`);
  }

  async vacuum(tableName: string): Promise<void> {
    console.log(`Vacuuming table: ${tableName}`);
    await this.query(`VACUUM ${tableName}`);
  }
}