-- Migration to add is_active column to energynet_parcels table
-- This allows us to keep inactive/expired parcels in database while hiding them from map

-- Add is_active column with default true for existing parcels
ALTER TABLE energynet_parcels ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT true;

-- Create index for better performance when filtering active parcels
CREATE INDEX IF NOT EXISTS idx_energynet_parcels_active ON energynet_parcels (is_active);

-- Update existing parcels to be active if their parent listing is active
UPDATE energynet_parcels 
SET is_active = (
    SELECT CASE 
        WHEN el.status = 'active' THEN true 
        ELSE false 
    END
    FROM energynet_listings el 
    WHERE el.sale_group = energynet_parcels.sale_group
);

-- Add a constraint to ensure valid boolean values
ALTER TABLE energynet_parcels ADD CONSTRAINT check_is_active CHECK (is_active IN (true, false));