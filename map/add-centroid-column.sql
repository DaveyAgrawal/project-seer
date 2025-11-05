-- Add centroid column to energynet_parcels for pin display
-- This improves performance by pre-calculating pin positions

-- Step 1: Add centroid column
ALTER TABLE energynet_parcels 
ADD COLUMN centroid geometry(POINT, 4326);

-- Step 2: Populate centroids for existing parcels
UPDATE energynet_parcels 
SET centroid = ST_Centroid(geom) 
WHERE geom IS NOT NULL;

-- Step 3: Create index for centroid queries
CREATE INDEX idx_energynet_parcels_centroid ON energynet_parcels USING GIST (centroid);

-- Step 4: Add trigger to auto-populate centroid on insert/update
CREATE OR REPLACE FUNCTION update_energynet_centroid()
RETURNS TRIGGER AS $$
BEGIN
    -- Only update centroid if geometry exists
    IF NEW.geom IS NOT NULL THEN
        NEW.centroid = ST_Centroid(NEW.geom);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_energynet_centroid
    BEFORE INSERT OR UPDATE OF geom ON energynet_parcels
    FOR EACH ROW
    EXECUTE FUNCTION update_energynet_centroid();

-- Verification query
SELECT 
    COUNT(*) as total_parcels,
    COUNT(centroid) as parcels_with_centroids,
    COUNT(geom) as parcels_with_geometry
FROM energynet_parcels;