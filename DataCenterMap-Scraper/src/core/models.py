"""
Database models for DataCenterMap scraper.
"""

from datetime import datetime
from typing import Optional

from sqlalchemy import String, Float, DateTime, Text, UniqueConstraint
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(AsyncAttrs, DeclarativeBase):
    """Base class for all database models."""
    pass


class DataCenterFacility(Base):
    """Data center facility information from datacentermap.com."""
    
    __tablename__ = "data_center_facilities"
    
    # Primary key
    id: Mapped[int] = mapped_column(primary_key=True)
    
    # Facility information
    name: Mapped[str] = mapped_column(String(500), nullable=False)
    address_full: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    city: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    state: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    postal_code: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    country: Mapped[str] = mapped_column(String(100), nullable=False, default="USA")
    
    # Coordinates (nullable if not available)
    latitude: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    longitude: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    
    # Source tracking
    source: Mapped[str] = mapped_column(String(50), nullable=False, default="datacentermap")
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    
    # Timestamps
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        nullable=False,
        default=datetime.utcnow
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), 
        nullable=False,
        default=datetime.utcnow,
        onupdate=datetime.utcnow
    )
    
    # Ensure uniqueness on source + source_url
    __table_args__ = (
        UniqueConstraint('source', 'source_url', name='uq_facility_source_url'),
    )
    
    def __repr__(self) -> str:
        return f"<DataCenterFacility(name='{self.name}', city='{self.city}', state='{self.state}')>"
    
    def to_dict(self) -> dict:
        """Convert facility to dictionary for CSV export."""
        return {
            "id": self.id,
            "name": self.name,
            "address_full": self.address_full,
            "city": self.city,
            "state": self.state,
            "postal_code": self.postal_code,
            "country": self.country,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "source": self.source,
            "source_url": self.source_url,
            "first_seen_at": self.first_seen_at.isoformat() if self.first_seen_at else None,
            "last_seen_at": self.last_seen_at.isoformat() if self.last_seen_at else None,
        }