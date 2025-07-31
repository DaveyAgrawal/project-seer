"""
Database repository for DataCenterFacility operations.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional

from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.logging import logger
from src.core.models import DataCenterFacility


class FacilityRepository:
    """Repository for DataCenterFacility database operations."""
    
    def __init__(self, session: AsyncSession):
        self.session = session
    
    async def upsert_facility(self, facility_data: Dict) -> DataCenterFacility:
        """
        Insert or update a facility using PostgreSQL's ON CONFLICT.
        
        Args:
            facility_data: Dictionary with facility information
            
        Returns:
            The created or updated DataCenterFacility instance
        """
        now = datetime.utcnow()
        
        # Prepare data for upsert
        upsert_data = {
            "name": facility_data.get("name"),
            "address_full": facility_data.get("address_full"),
            "city": facility_data.get("city"),
            "state": facility_data.get("state"),
            "postal_code": facility_data.get("postal_code"),
            "country": facility_data.get("country", "USA"),
            "latitude": facility_data.get("latitude"),
            "longitude": facility_data.get("longitude"),
            "source": facility_data.get("source", "datacentermap"),
            "source_url": facility_data["source_url"],  # Required field
            "last_seen_at": now,
        }
        
        # Use PostgreSQL's INSERT ... ON CONFLICT for upsert
        stmt = insert(DataCenterFacility).values(**upsert_data)
        
        # On conflict (source, source_url), update all fields except first_seen_at
        update_fields = {key: stmt.excluded[key] for key in upsert_data.keys() if key != "first_seen_at"}
        
        stmt = stmt.on_conflict_do_update(
            constraint="uq_facility_source_url",
            set_=update_fields
        ).returning(DataCenterFacility)
        
        # If it's a new facility, set first_seen_at
        stmt = stmt.values(first_seen_at=now)
        
        result = await self.session.execute(stmt)
        facility = result.scalar_one()
        
        # If first_seen_at wasn't set (existing facility), we need to handle it differently
        # Let's do a proper upsert with conditional first_seen_at
        stmt = insert(DataCenterFacility).values(**upsert_data, first_seen_at=now)
        
        # On conflict, update all fields except first_seen_at (keep original)
        update_fields = {key: stmt.excluded[key] for key in upsert_data.keys()}
        
        stmt = stmt.on_conflict_do_update(
            constraint="uq_facility_source_url",
            set_=update_fields
        ).returning(DataCenterFacility)
        
        result = await self.session.execute(stmt)
        facility = result.scalar_one()
        
        logger.debug(f"Upserted facility: {facility.name} (ID: {facility.id})")
        return facility
    
    async def upsert_facilities_batch(self, facilities_data: List[Dict]) -> List[DataCenterFacility]:
        """
        Batch upsert multiple facilities for better performance.
        
        Args:
            facilities_data: List of facility dictionaries
            
        Returns:
            List of created/updated DataCenterFacility instances
        """
        if not facilities_data:
            return []
        
        now = datetime.utcnow()
        facilities = []
        
        logger.info(f"🔄 Upserting {len(facilities_data)} facilities to database...")
        
        for facility_data in facilities_data:
            try:
                # Prepare data
                upsert_data = {
                    "name": facility_data.get("name"),
                    "address_full": facility_data.get("address_full"),
                    "city": facility_data.get("city"),
                    "state": facility_data.get("state"),
                    "postal_code": facility_data.get("postal_code"),
                    "country": facility_data.get("country", "USA"),
                    "latitude": facility_data.get("latitude"),
                    "longitude": facility_data.get("longitude"),
                    "source": facility_data.get("source", "datacentermap"),
                    "source_url": facility_data["source_url"],
                    "first_seen_at": now,
                    "last_seen_at": now,
                }
                
                # PostgreSQL upsert
                stmt = insert(DataCenterFacility).values(**upsert_data)
                
                # On conflict, update all fields except first_seen_at
                update_fields = {
                    key: stmt.excluded[key] 
                    for key in upsert_data.keys() 
                    if key != "first_seen_at"
                }
                
                stmt = stmt.on_conflict_do_update(
                    constraint="uq_facility_source_url",
                    set_=update_fields
                ).returning(DataCenterFacility)
                
                result = await self.session.execute(stmt)
                facility = result.scalar_one()
                facilities.append(facility)
                
            except Exception as e:
                logger.error(f"❌ Failed to upsert facility {facility_data.get('name', 'Unknown')}: {e}")
                continue
        
        logger.info(f"✅ Successfully upserted {len(facilities)} facilities")
        return facilities
    
    async def get_facility_by_url(self, source_url: str, source: str = "datacentermap") -> Optional[DataCenterFacility]:
        """Get a facility by source URL."""
        stmt = select(DataCenterFacility).where(
            DataCenterFacility.source == source,
            DataCenterFacility.source_url == source_url
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()
    
    async def get_facilities_by_state(self, state: str) -> List[DataCenterFacility]:
        """Get all facilities in a specific state."""
        stmt = select(DataCenterFacility).where(DataCenterFacility.state == state)
        result = await self.session.execute(stmt)
        return result.scalars().all()
    
    async def get_facility_count(self) -> int:
        """Get total count of facilities."""
        stmt = select(func.count(DataCenterFacility.id))
        result = await self.session.execute(stmt)
        return result.scalar()
    
    async def get_facilities_with_coordinates_count(self) -> int:
        """Get count of facilities with coordinates."""
        stmt = select(func.count(DataCenterFacility.id)).where(
            DataCenterFacility.latitude.is_not(None),
            DataCenterFacility.longitude.is_not(None)
        )
        result = await self.session.execute(stmt)
        return result.scalar()
    
    async def get_state_summary(self) -> List[Dict[str, int]]:
        """Get facility count by state."""
        stmt = select(
            DataCenterFacility.state,
            func.count(DataCenterFacility.id).label('count')
        ).group_by(DataCenterFacility.state).order_by(DataCenterFacility.state)
        
        result = await self.session.execute(stmt)
        return [{"state": row.state, "count": row.count} for row in result.all()]
    
    async def delete_old_facilities(self, days_old: int = 30) -> int:
        """
        Delete facilities that haven't been seen in X days.
        Useful for cleanup of facilities that no longer exist.
        
        Args:
            days_old: Delete facilities not seen in this many days
            
        Returns:
            Number of facilities deleted
        """
        cutoff_date = datetime.utcnow() - timedelta(days=days_old)
        
        stmt = select(func.count(DataCenterFacility.id)).where(
            DataCenterFacility.last_seen_at < cutoff_date
        )
        result = await self.session.execute(stmt)
        count = result.scalar()
        
        if count > 0:
            delete_stmt = DataCenterFacility.__table__.delete().where(
                DataCenterFacility.last_seen_at < cutoff_date
            )
            await self.session.execute(delete_stmt)
            logger.info(f"🗑️  Deleted {count} facilities older than {days_old} days")
        
        return count