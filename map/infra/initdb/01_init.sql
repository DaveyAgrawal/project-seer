-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;

-- Dataset registry for dynamic layer management
CREATE TABLE IF NOT EXISTS dataset_registry (
  id BIGSERIAL PRIMARY KEY,
  layer_name   TEXT UNIQUE NOT NULL,
  table_name   TEXT NOT NULL,
  geometry_type TEXT NOT NULL,   -- e.g. POINT, MULTILINESTRING
  minzoom      INT DEFAULT 3,
  maxzoom      INT DEFAULT 14,
  attributes   JSONB DEFAULT '{}'::jsonb,
  style        JSONB DEFAULT '{}'::jsonb,
  created_at   TIMESTAMPTZ DEFAULT NOW()
);

-- Transmission lines table (normalize to MULTILINESTRING)
CREATE TABLE IF NOT EXISTS transmission_lines (
  gid BIGSERIAL PRIMARY KEY,
  props JSONB DEFAULT '{}'::jsonb,
  id_text TEXT, 
  owner TEXT, 
  status TEXT, 
  volt_class TEXT,
  kv NUMERIC,                                   -- parsed numeric voltage
  geom geometry(MULTILINESTRING, 4326)
);

-- Transmission lines indexes
CREATE INDEX IF NOT EXISTS transmission_lines_gix ON transmission_lines USING GIST (geom);
CREATE INDEX IF NOT EXISTS transmission_lines_kv_idx ON transmission_lines(kv);

-- Geothermal points table with generated geometry column
CREATE TABLE IF NOT EXISTS geothermal_points (
  gid BIGSERIAL PRIMARY KEY,
  latitude    DOUBLE PRECISION NOT NULL,
  longitude   DOUBLE PRECISION NOT NULL,
  depth_m     DOUBLE PRECISION,         -- meters below surface (positive)
  temperature_f DOUBLE PRECISION,       -- Fahrenheit
  geom geometry(POINT, 4326) GENERATED ALWAYS AS (
    ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
  ) STORED
);

-- Geothermal points indexes
CREATE UNIQUE INDEX IF NOT EXISTS geothermal_points_uniq ON geothermal_points (latitude, longitude, depth_m);
CREATE INDEX IF NOT EXISTS geothermal_points_gix ON geothermal_points USING GIST (geom);
CREATE INDEX IF NOT EXISTS geothermal_points_temp_idx ON geothermal_points(temperature_f);
CREATE INDEX IF NOT EXISTS geothermal_points_depth_idx ON geothermal_points(depth_m);

-- US-filtered transmission lines view
CREATE OR REPLACE VIEW transmission_lines_us AS
SELECT gid, id_text, owner, status, volt_class, kv, geom
FROM transmission_lines
WHERE ST_Intersects(
  geom,
  ST_Collect(ARRAY[
    ST_MakeEnvelope(-125, 24, -66.5, 49.6, 4326),   -- CONUS
    ST_MakeEnvelope(-170, 49, -130, 72, 4326),      -- Alaska
    ST_MakeEnvelope(-161, 18.9, -154, 22.4, 4326),  -- Hawaii
    ST_MakeEnvelope(-67.5, 17.6, -65, 18.6, 4326)   -- Puerto Rico
  ])
);

-- Zoom-banded transmission lines views for performance
CREATE OR REPLACE VIEW transmission_lines_us_z0_6 AS
SELECT 
  gid,
  id_text,
  owner,
  kv,
  ST_SimplifyVW(geom, 0.01) as geom
FROM transmission_lines_us
WHERE kv > 138 OR kv IS NULL; -- Only show major lines at low zoom

CREATE OR REPLACE VIEW transmission_lines_us_z7_10 AS
SELECT 
  gid,
  id_text,
  owner,
  volt_class,
  kv,
  ST_SimplifyVW(geom, 0.001) as geom
FROM transmission_lines_us
WHERE kv > 69 OR kv IS NULL; -- Show medium+ voltage lines

CREATE OR REPLACE VIEW transmission_lines_us_z11_14 AS
SELECT 
  gid,
  id_text,
  owner,
  status,
  volt_class,
  kv,
  geom
FROM transmission_lines_us; -- Show all lines at high zoom

-- US-filtered geothermal points view
CREATE OR REPLACE VIEW geothermal_points_us AS
SELECT gid, latitude, longitude, depth_m, temperature_f, geom
FROM geothermal_points
WHERE ST_Intersects(
  geom,
  ST_Collect(ARRAY[
    ST_MakeEnvelope(-125, 24, -66.5, 49.6, 4326),   -- CONUS
    ST_MakeEnvelope(-170, 49, -130, 72, 4326),      -- Alaska
    ST_MakeEnvelope(-161, 18.9, -154, 22.4, 4326),  -- Hawaii
    ST_MakeEnvelope(-67.5, 17.6, -65, 18.6, 4326)   -- Puerto Rico
  ])
);

-- Geothermal aggregated view for low zooms (hexagonal binning)
CREATE OR REPLACE VIEW geothermal_points_us_z0_9 AS
WITH hex_grid AS (
  SELECT 
    ST_SnapToGrid(geom, 0.5) as hex_center,
    COUNT(*) as point_count,
    AVG(temperature_f) as avg_temperature_f,
    AVG(depth_m) as avg_depth_m,
    MIN(temperature_f) as min_temperature_f,
    MAX(temperature_f) as max_temperature_f
  FROM geothermal_points_us
  GROUP BY ST_SnapToGrid(geom, 0.5)
  HAVING COUNT(*) > 0
)
SELECT 
  row_number() OVER () as gid,
  point_count,
  ROUND(avg_temperature_f::numeric, 1) as avg_temperature_f,
  ROUND(avg_depth_m::numeric, 1) as avg_depth_m,
  min_temperature_f,
  max_temperature_f,
  hex_center as geom
FROM hex_grid;

-- Helper function to register datasets
CREATE OR REPLACE FUNCTION register_dataset(
  p_layer_name TEXT,
  p_table_name TEXT,
  p_geometry_type TEXT,
  p_minzoom INT DEFAULT 3,
  p_maxzoom INT DEFAULT 14,
  p_attributes JSONB DEFAULT '{}'::jsonb,
  p_style JSONB DEFAULT '{}'::jsonb
) RETURNS VOID AS $$
BEGIN
  INSERT INTO dataset_registry (layer_name, table_name, geometry_type, minzoom, maxzoom, attributes, style)
  VALUES (p_layer_name, p_table_name, p_geometry_type, p_minzoom, p_maxzoom, p_attributes, p_style)
  ON CONFLICT (layer_name) DO UPDATE SET
    table_name = EXCLUDED.table_name,
    geometry_type = EXCLUDED.geometry_type,
    minzoom = EXCLUDED.minzoom,
    maxzoom = EXCLUDED.maxzoom,
    attributes = EXCLUDED.attributes,
    style = EXCLUDED.style,
    created_at = NOW();
END;
$$ LANGUAGE plpgsql;

-- Performance optimization settings
ALTER SYSTEM SET shared_preload_libraries = 'postgis-3';
ALTER SYSTEM SET max_connections = 200;
ALTER SYSTEM SET shared_buffers = '256MB';
ALTER SYSTEM SET effective_cache_size = '1GB';
ALTER SYSTEM SET work_mem = '16MB';
ALTER SYSTEM SET maintenance_work_mem = '256MB';
ALTER SYSTEM SET checkpoint_completion_target = 0.9;
ALTER SYSTEM SET wal_buffers = '16MB';
ALTER SYSTEM SET default_statistics_target = 100;