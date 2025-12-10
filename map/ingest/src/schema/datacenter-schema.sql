-- Datacenter listings table
-- Stores comprehensive data center facility information scraped from datacenters.com
-- Includes both US and international facilities, with US-only filtering for map display

CREATE TABLE IF NOT EXISTS datacenter_listings (
  id BIGSERIAL PRIMARY KEY,

  -- Unique identifiers
  internal_id TEXT UNIQUE NOT NULL,              -- Hash-based unique ID for stable referencing
  facility_url TEXT UNIQUE NOT NULL,             -- Full URL of facility detail page (unique key)
  provider_url TEXT,                             -- URL to operator's parent/provider page

  -- Basic information
  name TEXT NOT NULL,                            -- Facility name (not parent company)
  operator TEXT,                                 -- Data center operator/company (e.g., Amazon, Meta, Google)
  street_address TEXT,                           -- Full street address
  city TEXT,                                     -- City name
  state TEXT,                                    -- State/province
  country TEXT NOT NULL,                         -- Country name
  is_us BOOLEAN DEFAULT false,                   -- Flag for US datacenters (for map filtering)

  -- Geographic data
  latitude NUMERIC(10, 7),                       -- Latitude coordinate
  longitude NUMERIC(10, 7),                      -- Longitude coordinate
  geom geometry(POINT, 4326),                    -- PostGIS point geometry (computed from lat/lng)

  -- Technical specifications
  square_footage NUMERIC,                        -- Facility size in square feet
  facility_type TEXT,                            -- Type: colocation, hyperscale, enterprise, etc.
  power_capacity_mw NUMERIC,                     -- Power capacity in megawatts

  -- Certifications (stored as JSONB array)
  certifications JSONB DEFAULT '[]'::jsonb,      -- e.g., ["ISO 22301", "ISO 27001", "SOC 2"]

  -- Features (stored as JSONB object with boolean flags)
  features JSONB DEFAULT '{}'::jsonb,            -- e.g., {"bare_metal": true, "internet_exchange": false}

  -- Proximity and regional data
  miles_to_airport NUMERIC,                      -- Distance to nearest airport
  breadcrumb_hierarchy JSONB,                    -- e.g., ["Ireland", "County Dublin", "Dublin"]
  market_region TEXT,                            -- Labeled market name (e.g., "North Europe-Ireland")
  nearby_datacenter_count INTEGER,               -- Number of facilities within 50 miles

  -- Contact information
  phone_number TEXT,                             -- Published phone number for facility/operator

  -- Media availability flags
  has_images BOOLEAN DEFAULT false,              -- Facility page includes images
  has_brochures BOOLEAN DEFAULT false,           -- Brochure/PDF downloads available
  has_media BOOLEAN DEFAULT false,               -- Media tab/gallery available

  -- Data quality tracking
  missing_fields JSONB DEFAULT '[]'::jsonb,      -- Array of field names with missing data

  -- Metadata
  scraped_at TIMESTAMPTZ DEFAULT NOW(),          -- When record was first scraped
  updated_at TIMESTAMPTZ DEFAULT NOW()           -- When record was last updated
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS datacenter_listings_gix ON datacenter_listings USING GIST (geom);
CREATE INDEX IF NOT EXISTS datacenter_listings_country_idx ON datacenter_listings(country);
CREATE INDEX IF NOT EXISTS datacenter_listings_state_idx ON datacenter_listings(state);
CREATE INDEX IF NOT EXISTS datacenter_listings_is_us_idx ON datacenter_listings(is_us);
CREATE INDEX IF NOT EXISTS datacenter_listings_operator_idx ON datacenter_listings(operator);
CREATE INDEX IF NOT EXISTS datacenter_listings_facility_type_idx ON datacenter_listings(facility_type);
CREATE INDEX IF NOT EXISTS datacenter_listings_power_idx ON datacenter_listings(power_capacity_mw);

-- Auto-update timestamp trigger
CREATE OR REPLACE FUNCTION update_datacenter_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER datacenter_listings_updated_at
BEFORE UPDATE ON datacenter_listings
FOR EACH ROW
EXECUTE FUNCTION update_datacenter_updated_at();

-- Function to compute geometry from lat/lng
CREATE OR REPLACE FUNCTION update_datacenter_geom()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.latitude IS NOT NULL AND NEW.longitude IS NOT NULL THEN
    NEW.geom = ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326);
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER datacenter_listings_geom
BEFORE INSERT OR UPDATE ON datacenter_listings
FOR EACH ROW
EXECUTE FUNCTION update_datacenter_geom();

-- Register dataset with map system (following EnergyNet pattern)
-- This assumes a register_dataset function exists from the main schema
-- If it doesn't exist, this will be a no-op
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_proc WHERE proname = 'register_dataset') THEN
    PERFORM register_dataset(
      'datacenter-locations',
      'datacenter_listings',
      'POINT',
      4,
      18,
      '{"name": "text", "operator": "text", "power_capacity_mw": "numeric", "facility_type": "text"}'::jsonb,
      '{"circle-color": "#00FF00", "circle-radius": 8}'::jsonb
    );
  END IF;
END;
$$;
