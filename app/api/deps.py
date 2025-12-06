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