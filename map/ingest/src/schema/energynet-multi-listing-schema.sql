-- EnergyNet Multi-Listing Database Schema Enhancement
-- Adds support for multiple listings across states with lifecycle management

-- Add columns to existing energynet_listings table for multi-listing support
ALTER TABLE energynet_listings 
ADD COLUMN IF NOT EXISTS region TEXT,                    -- State/region (e.g., 'Nevada', 'Wyoming')
ADD COLUMN IF NOT EXISTS sale_group TEXT,                -- EnergyNet sale group ID (e.g., 'GEONV-2025-Q4')
ADD COLUMN IF NOT EXISTS listing_type TEXT DEFAULT 'Oil & Gas Lease', -- 'Oil & Gas Lease', 'Geothermal', 'Land Sale'
ADD COLUMN IF NOT EXISTS sale_start_date DATE,           -- Bidding start date
ADD COLUMN IF NOT EXISTS sale_end_date DATE,             -- Bidding end date
ADD COLUMN IF NOT EXISTS status TEXT DEFAULT 'active',   -- 'active', 'expired', 'processed'
ADD COLUMN IF NOT EXISTS agency TEXT,                    -- 'BLM Nevada', 'Wyoming State Lands', etc.
ADD COLUMN IF NOT EXISTS parcel_count INTEGER DEFAULT 0, -- Number of parcels in this listing
ADD COLUMN IF NOT EXISTS total_acres NUMERIC,            -- Sum of all parcel acres
ADD COLUMN IF NOT EXISTS last_scraped_at TIMESTAMPTZ DEFAULT NOW(),
ADD COLUMN IF NOT EXISTS is_processed BOOLEAN DEFAULT false;

-- Add unique constraint on sale_group to prevent duplicates
ALTER TABLE energynet_listings 
ADD CONSTRAINT IF NOT EXISTS energynet_listings_sale_group_unique 
UNIQUE (sale_group);

-- Update existing listing_id constraint to allow multiple listings
ALTER TABLE energynet_listings 
DROP CONSTRAINT IF EXISTS energynet_listings_listing_id_key;

-- Add composite unique constraint
ALTER TABLE energynet_listings 
ADD CONSTRAINT IF NOT EXISTS energynet_listings_composite_unique 
UNIQUE (sale_group, listing_id);

-- Enhanced indexes for multi-listing queries
CREATE INDEX IF NOT EXISTS energynet_listings_region_idx ON energynet_listings(region);
CREATE INDEX IF NOT EXISTS energynet_listings_sale_group_idx ON energynet_listings(sale_group);
CREATE INDEX IF NOT EXISTS energynet_listings_status_idx ON energynet_listings(status);
CREATE INDEX IF NOT EXISTS energynet_listings_listing_type_idx ON energynet_listings(listing_type);
CREATE INDEX IF NOT EXISTS energynet_listings_sale_dates_idx ON energynet_listings(sale_start_date, sale_end_date);

-- Update energynet_parcels to reference sale_group for better organization
ALTER TABLE energynet_parcels 
ADD COLUMN IF NOT EXISTS sale_group TEXT,
ADD COLUMN IF NOT EXISTS region TEXT,
ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT true;

-- Add index for sale_group in parcels
CREATE INDEX IF NOT EXISTS energynet_parcels_sale_group_idx ON energynet_parcels(sale_group);
CREATE INDEX IF NOT EXISTS energynet_parcels_region_idx ON energynet_parcels(region);
CREATE INDEX IF NOT EXISTS energynet_parcels_is_active_idx ON energynet_parcels(is_active);

-- Enhanced statistics view for multi-listing analytics
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

-- Regional summary view  
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

-- Active listings view for current display
CREATE OR REPLACE VIEW energynet_active_listings AS
SELECT 
  l.sale_group,
  l.listing_id,
  l.title,
  l.region,
  l.listing_type,
  l.agency,
  l.sale_start_date,
  l.sale_end_date,
  l.parcel_count,
  l.total_acres,
  l.url,
  l.gis_download_url,
  l.last_scraped_at
FROM energynet_listings l
WHERE l.status = 'active'
ORDER BY l.sale_start_date DESC;

-- Update US-filtered view to work with multiple listings
DROP VIEW IF EXISTS energynet_parcels_us;
CREATE OR REPLACE VIEW energynet_parcels_us AS
SELECT 
  gid, 
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
WHERE ST_Intersects(
  geom,
  ST_Collect(ARRAY[
    ST_MakeEnvelope(-125, 24, -66.5, 49.6, 4326),   -- CONUS
    ST_MakeEnvelope(-170, 49, -130, 72, 4326),      -- Alaska
    ST_MakeEnvelope(-161, 18.9, -154, 22.4, 4326),  -- Hawaii
    ST_MakeEnvelope(-67.5, 17.6, -65, 18.6, 4326)   -- Puerto Rico
  ])
)
AND is_active = true
AND EXISTS (
  SELECT 1 FROM energynet_listings l 
  WHERE l.sale_group = energynet_parcels.sale_group 
  AND l.status = 'active'
);

-- Recreate zoom-optimized views with active listings filter
CREATE OR REPLACE VIEW energynet_parcels_us_z0_8 AS
SELECT 
  gid, listing_id, parcel_id, sale_group, state, region, acres, description, props, created_at,
  geom
FROM energynet_parcels_us;

CREATE OR REPLACE VIEW energynet_parcels_us_z9_12 AS
SELECT 
  gid, listing_id, parcel_id, sale_group, state, region, acres, description, props, created_at,
  geom
FROM energynet_parcels_us;

CREATE OR REPLACE VIEW energynet_parcels_us_z13_18 AS
SELECT 
  gid, listing_id, parcel_id, sale_group, state, region, acres, description, props, created_at,
  geom
FROM energynet_parcels_us;

-- Function to mark expired listings
CREATE OR REPLACE FUNCTION mark_expired_listings()
RETURNS INTEGER AS $$
DECLARE
  expired_count INTEGER;
BEGIN
  -- Mark listings as expired if sale_end_date has passed
  UPDATE energynet_listings 
  SET 
    status = 'expired',
    updated_at = NOW()
  WHERE 
    status = 'active' 
    AND sale_end_date < CURRENT_DATE;
    
  GET DIAGNOSTICS expired_count = ROW_COUNT;
  
  RETURN expired_count;
END;
$$ LANGUAGE plpgsql;

-- Function to cleanup expired listing data (optional)
CREATE OR REPLACE FUNCTION cleanup_expired_listings(days_old INTEGER DEFAULT 30)
RETURNS INTEGER AS $$
DECLARE
  cleaned_count INTEGER;
BEGIN
  -- Delete parcels for listings expired more than specified days ago
  DELETE FROM energynet_parcels 
  WHERE sale_group IN (
    SELECT sale_group 
    FROM energynet_listings 
    WHERE status = 'expired' 
    AND updated_at < (CURRENT_DATE - INTERVAL '%s days', days_old)
  );
  
  -- Delete the expired listings themselves
  DELETE FROM energynet_listings 
  WHERE status = 'expired' 
  AND updated_at < (CURRENT_DATE - INTERVAL '%s days', days_old);
  
  GET DIAGNOSTICS cleaned_count = ROW_COUNT;
  
  RETURN cleaned_count;
END;
$$ LANGUAGE plpgsql;

-- Update dataset registrations for multi-listing support
DELETE FROM dataset_registry WHERE id IN ('energynet-parcels-low', 'energynet-parcels-medium', 'energynet-parcels-high');

SELECT register_dataset(
  'energynet-parcels-low',
  'energynet_parcels_us_z0_8', 
  'POLYGON',
  0,
  8,
  '{
    "parcel_id": "text", 
    "listing_id": "text", 
    "sale_group": "text",
    "state": "text", 
    "region": "text",
    "acres": "numeric", 
    "description": "text"
  }'::jsonb,
  '{
    "fill-color": "#FF1493", 
    "fill-opacity": 0.7, 
    "fill-outline-color": "#0066CC"
  }'::jsonb
);

SELECT register_dataset(
  'energynet-parcels-medium',
  'energynet_parcels_us_z9_12',
  'POLYGON', 
  9,
  12,
  '{
    "parcel_id": "text", 
    "listing_id": "text", 
    "sale_group": "text",
    "state": "text", 
    "region": "text", 
    "acres": "numeric", 
    "description": "text"
  }'::jsonb,
  '{
    "fill-color": "#FF1493", 
    "fill-opacity": 0.7, 
    "fill-outline-color": "#0066CC"
  }'::jsonb
);

SELECT register_dataset(
  'energynet-parcels-high',
  'energynet_parcels_us_z13_18',
  'POLYGON',
  13,
  18, 
  '{
    "parcel_id": "text", 
    "listing_id": "text", 
    "sale_group": "text",
    "state": "text", 
    "region": "text",
    "acres": "numeric", 
    "description": "text"
  }'::jsonb,
  '{
    "fill-color": "#FF1493", 
    "fill-opacity": 0.7, 
    "fill-outline-color": "#0066CC"
  }'::jsonb
);

-- Comments for documentation
COMMENT ON COLUMN energynet_listings.sale_group IS 'EnergyNet sale group identifier (e.g., GEONV-2025-Q4)';
COMMENT ON COLUMN energynet_listings.region IS 'State or region (e.g., Nevada, Wyoming)';
COMMENT ON COLUMN energynet_listings.listing_type IS 'Type of listing (Oil & Gas Lease, Geothermal, Land Sale)';
COMMENT ON COLUMN energynet_listings.status IS 'Listing lifecycle status (active, expired, processed)';
COMMENT ON VIEW energynet_multi_stats IS 'Multi-listing statistics across all regions and sale groups';
COMMENT ON VIEW energynet_by_region IS 'Regional summary of EnergyNet listings and parcels';
COMMENT ON FUNCTION mark_expired_listings IS 'Marks listings as expired based on sale_end_date';