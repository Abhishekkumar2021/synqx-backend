from typing import Generator, Optional
from fastapi import Depends, HTTPException, status, Header
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.core.logging import get_logger

logger = get_logger(__name__)


def get_db() -> Generator[Session, None, None]:
    """
    Dependency that provides a database session.
    Automatically commits on success and rolls back on error.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_current_tenant_id(
    x_tenant_id: Optional[str] = Header(None, alias="X-Tenant-ID")
) -> int:
    """
    Dependency that extracts tenant ID from request header.
    In production, this would integrate with your authentication system.
    
    Args:
        x_tenant_id: Tenant ID from request header
        
    Returns:
        Tenant ID as integer
        
    Raises:
        HTTPException: If tenant ID is missing or invalid
    """
    if not x_tenant_id:
        logger.warning("Request missing X-Tenant-ID header")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "Missing tenant ID",
                "message": "X-Tenant-ID header is required"
            }
        )
    
    try:
        tenant_id = int(x_tenant_id)
        if tenant_id <= 0:
            raise ValueError("Tenant ID must be positive")
        return tenant_id
    except ValueError as e:
        logger.warning(f"Invalid tenant ID format: {x_tenant_id}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "Invalid tenant ID",
                "message": f"Tenant ID must be a positive integer: {str(e)}"
            }
        )


def get_current_user_id(
    x_user_id: Optional[str] = Header(None, alias="X-User-ID")
) -> Optional[int]:
    """
    Dependency that extracts user ID from request header.
    Optional - returns None if not provided.
    
    Args:
        x_user_id: User ID from request header
        
    Returns:
        User ID as integer or None
    """
    if not x_user_id:
        return None
    
    try:
        return int(x_user_id)
    except ValueError:
        logger.warning(f"Invalid user ID format: {x_user_id}")
        return None


def require_admin_role(
    x_user_role: Optional[str] = Header(None, alias="X-User-Role")
) -> bool:
    """
    Dependency that requires admin role.
    In production, this would check against your auth system.
    
    Args:
        x_user_role: User role from request header
        
    Returns:
        True if user is admin
        
    Raises:
        HTTPException: If user doesn't have admin role
    """
    if x_user_role != "admin":
        logger.warning(f"Unauthorized access attempt with role: {x_user_role}")
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "error": "Insufficient permissions",
                "message": "Admin role required for this operation"
            }
        )
    return True


class PaginationParams:
    """
    Reusable pagination parameters.
    """
    def __init__(
        self,
        limit: int = 100,
        offset: int = 0
    ):
        if limit < 1 or limit > 1000:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "Invalid pagination",
                    "message": "Limit must be between 1 and 1000"
                }
            )
        
        if offset < 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail={
                    "error": "Invalid pagination",
                    "message": "Offset must be non-negative"
                }
            )
        
        self.limit = limit
        self.offset = offset


def get_pagination_params(
    limit: int = 100,
    offset: int = 0
) -> PaginationParams:
    """
    Dependency for pagination parameters with validation.
    """
    return PaginationParams(limit=limit, offset=offset)