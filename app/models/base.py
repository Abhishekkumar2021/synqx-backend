from __future__ import annotations
from datetime import datetime, timezone
from typing import Optional
from sqlalchemy import DateTime, String
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.ext.hybrid import hybrid_property

# New SQLAlchemy 2.0 style Base
class Base(DeclarativeBase):
    pass

class TimestampMixin:
    """Timestamp tracking for all entities"""
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
        index=True,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

class UserTrackingMixin:
    """User tracking for audit trail"""
    created_by: Mapped[Optional[str]] = mapped_column(String(255), index=True)
    updated_by: Mapped[Optional[str]] = mapped_column(String(255))

class TenantMixin:
    """Multi-tenancy support"""
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)

class AuditMixin(TimestampMixin, UserTrackingMixin, TenantMixin):
    """Complete audit trail mixin"""
    pass

class SoftDeleteMixin:
    """Soft delete capability"""
    deleted_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), index=True
    )
    deleted_by: Mapped[Optional[str]] = mapped_column(String(255))

    @hybrid_property
    def is_deleted(self) -> bool:
        return self.deleted_at is not None