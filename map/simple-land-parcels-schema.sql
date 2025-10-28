-- Simple Land Parcels Schema for EnergyNet
-- Creates land_parcels table to match server API expectations

-- Create land_parcels table  
CREATE TABLE IF NOT EXISTS land_parcels (
  gid BIGSERIAL PRIMARY KEY,
  listing_id TEXT NOT NULL,             
  parcel_id TEXT NOT NULL,              
  state TEXT NOT NULL,
  acres NUMERIC,                        
  description TEXT DEFAULT 'Government Land Parcel',
  geometry geometry(POLYGON, 4326),     -- Match server column name
  created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Create listings table for reference
CREATE TABLE IF NOT EXISTS energynet_listings (
  id BIGSERIAL PRIMARY KEY,
  listing_id TEXT UNIQUE NOT NULL,      
  title TEXT NOT NULL,
  state TEXT NOT NULL,
  sale_date DATE,
  description TEXT,
  url TEXT,
  gis_download_url TEXT,
  props JSONB DEFAULT '{}'::jsonb,      
  scraped_at TIMESTAMPTZ DEFAULT NOW(),
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Composite unique constraint to prevent duplicate parcels
ALTER TABLE land_parcels 
ADD CONSTRAINT IF NOT EXISTS land_parcels_unique 
UNIQUE (listing_id, parcel_id);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS land_parcels_gix ON land_parcels USING GIST (geometry);
CREATE INDEX IF NOT EXISTS land_parcels_listing_id_idx ON land_parcels(listing_id);
CREATE INDEX IF NOT EXISTS land_parcels_state_idx ON land_parcels(state);
CREATE INDEX IF NOT EXISTS land_parcels_acres_idx ON land_parcels(acres);

-- Enable PostGIS if not already enabled
CREATE EXTENSION IF NOT EXISTS postgis;