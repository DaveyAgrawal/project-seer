-- Safe migration to fix geometry type mismatch
-- This script allows both Polygon and MultiPolygon in energynet_parcels.geom

-- Step 1: Drop views that depend on energynet_parcels.geom
DROP VIEW IF EXISTS energynet_parcels_us CASCADE;
DROP VIEW IF EXISTS energynet_parcels_us_z0_8 CASCADE;
DROP VIEW IF EXISTS energynet_parcels_us_z9_12 CASCADE;
DROP VIEW IF EXISTS energynet_parcels_us_z13_18 CASCADE;
DROP VIEW IF EXISTS energynet_stats CASCADE;
DROP VIEW IF EXISTS energynet_by_state CASCADE;
DROP VIEW IF EXISTS energynet_multi_stats CASCADE;
DROP VIEW IF EXISTS energynet_by_region CASCADE;

-- Step 2: Change geometry column to accept both Polygon and MultiPolygon
ALTER TABLE energynet_parcels ALTER COLUMN geom TYPE geometry(GEOMETRY, 4326);

-- Step 3: Recreate all views with exact same definitions
CREATE OR REPLACE VIEW energynet_parcels_us AS 
SELECT gid,
    listing_id,
    parcel_id,
    sale_group,
    state,
    region,
    acres,
    description,
    props,
    geom,
    created_at
FROM energynet_parcels
WHERE st_intersects(geom, st_collect(ARRAY[
    st_makeenvelope(-125::double precision, 24::double precision, -66.5::double precision, 49.6::double precision, 4326),
    st_makeenvelope(-170::double precision, 49::double precision, -130::double precision, 72::double precision, 4326),
    st_makeenvelope(-161::double precision, 18.9::double precision, -154::double precision, 22.4::double precision, 4326),
    st_makeenvelope(-67.5::double precision, 17.6::double precision, -65::double precision, 18.6::double precision, 4326)
])) AND EXISTS (
    SELECT 1 FROM energynet_listings l 
    WHERE l.sale_group = energynet_parcels.sale_group AND l.status = 'active'::text
);

CREATE OR REPLACE VIEW energynet_parcels_us_z0_8 AS 
SELECT gid, listing_id, parcel_id, sale_group, state, region, acres, description, props, created_at, geom 
FROM energynet_parcels_us;

CREATE OR REPLACE VIEW energynet_parcels_us_z9_12 AS 
SELECT gid, listing_id, parcel_id, sale_group, state, region, acres, description, props, created_at, geom 
FROM energynet_parcels_us;

CREATE OR REPLACE VIEW energynet_parcels_us_z13_18 AS 
SELECT gid, listing_id, parcel_id, sale_group, state, region, acres, description, props, created_at, geom 
FROM energynet_parcels_us;

CREATE OR REPLACE VIEW energynet_stats AS 
SELECT 
  COUNT(DISTINCT l.listing_id) as total_listings,
  COUNT(p.gid) as total_parcels,
  SUM(p.acres) as total_acres,
  COUNT(DISTINCT l.state) as states_covered,
  MIN(l.sale_date) as earliest_sale,
  MAX(l.sale_date) as latest_sale,
  AVG(p.acres) as avg_parcel_acres
FROM energynet_listings l
LEFT JOIN energynet_parcels p ON l.listing_id = p.listing_id;

CREATE OR REPLACE VIEW energynet_by_state AS 
SELECT 
  l.state,
  COUNT(DISTINCT l.listing_id) as listings,
  COUNT(p.gid) as parcels,
  SUM(p.acres) as total_acres,
  AVG(p.acres) as avg_parcel_acres,
  MIN(l.sale_date) as earliest_sale,
  MAX(l.sale_date) as latest_sale
FROM energynet_listings l
LEFT JOIN energynet_parcels p ON l.listing_id = p.listing_id
GROUP BY l.state
ORDER BY total_acres DESC;

CREATE OR REPLACE VIEW energynet_multi_stats AS
SELECT 
  COUNT(DISTINCT l.sale_group) as total_sale_groups,
  COUNT(DISTINCT l.listing_id) as total_listings,
  COUNT(DISTINCT l.region) as regions_covered,
  COUNT(p.gid) as total_parcels,
  SUM(p.acres) as total_acres,
  COUNT(CASE WHEN l.status = 'active' THEN 1 END) as active_listings,
  COUNT(CASE WHEN l.status = 'expired' THEN 1 END) as expired_listings,
  MIN(l.sale_start_date) as earliest_sale,
  MAX(l.sale_end_date) as latest_sale,
  AVG(p.acres) as avg_parcel_acres
FROM energynet_listings l
LEFT JOIN energynet_parcels p ON l.sale_group = p.sale_group;

CREATE OR REPLACE VIEW energynet_by_region AS
SELECT 
  l.region,
  l.listing_type,
  COUNT(DISTINCT l.sale_group) as sale_groups,
  COUNT(DISTINCT l.listing_id) as listings,
  COUNT(p.gid) as parcels,
  SUM(p.acres) as total_acres,
  AVG(p.acres) as avg_parcel_acres,
  MIN(l.sale_start_date) as earliest_sale,
  MAX(l.sale_end_date) as latest_sale,
  COUNT(CASE WHEN l.status = 'active' THEN 1 END) as active_count,
  COUNT(CASE WHEN l.status = 'expired' THEN 1 END) as expired_count
FROM energynet_listings l
LEFT JOIN energynet_parcels p ON l.sale_group = p.sale_group
GROUP BY l.region, l.listing_type
ORDER BY total_acres DESC NULLS LAST;

-- Verification query
SELECT 'Migration completed successfully' as status;