from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, status, Query, Body
from sqlalchemy.orm import Session

from app.schemas.connection import (
    ConnectionCreate,
    ConnectionUpdate,
    ConnectionRead,
    ConnectionDetailRead,
    ConnectionListResponse,
    ConnectionTestRequest,
    ConnectionTestResponse,
    AssetCreate,
    AssetDetailRead,
    AssetDiscoverRequest,
    AssetDiscoverResponse,
)
from app.services.connection_service import ConnectionService
from app.api.deps import get_db
from app.core.errors import AppError
from app.core.logging import get_logger
from app.models.enums import ConnectorType

router = APIRouter()
logger = get_logger(__name__)


@router.post(
    "/",
    response_model=ConnectionDetailRead,
    status_code=status.HTTP_201_CREATED,
    summary="Create Connection",
    description="Create a new data source/destination connection",
)
def create_connection(
    connection_create: ConnectionCreate,
    db: Session = Depends(get_db),
    
):
    try:
        service = ConnectionService(db)
        connection = service.create_connection(connection_create)

        response = ConnectionDetailRead.model_validate(connection)
        response.asset_count = len(connection.assets) if connection.assets else 0

        return response

    except AppError as e:
        logger.error(f"Error creating connection: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "Bad request", "message": str(e)},
        )
    except Exception as e:
        logger.error(f"Unexpected error creating connection: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "Internal server error",
                "message": "Failed to create connection",
            },
        )


@router.get(
    "/",
    response_model=ConnectionListResponse,
    summary="List Connections",
    description="List all connections for the current tenant",
)
def list_connections(
    connector_type: Optional[ConnectorType] = Query(
        None, description="Filter by connector type"
    ),
    health_status: Optional[str] = Query(None, description="Filter by health status"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    
):
    try:
        service = ConnectionService(db)
        connections, total = service.list_connections(
            connector_type=connector_type,
            health_status=health_status,
            limit=limit,
            offset=offset,
        )

        return ConnectionListResponse(
            connections=[ConnectionRead.model_validate(c) for c in connections],
            total=total,
            limit=limit,
            offset=offset,
        )

    except Exception as e:
        logger.error(f"Error listing connections: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "Internal server error",
                "message": "Failed to list connections",
            },
        )


@router.get(
    "/{connection_id}",
    response_model=ConnectionDetailRead,
    summary="Get Connection",
    description="Get detailed information about a specific connection",
)
def get_connection(
    connection_id: int,
    db: Session = Depends(get_db),
    
):
    service = ConnectionService(db)
    connection = service.get_connection(connection_id)

    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "Not found",
                "message": f"Connection {connection_id} not found",
            },
        )

    response = ConnectionDetailRead.model_validate(connection)
    response.asset_count = len(connection.assets) if connection.assets else 0

    return response


@router.patch(
    "/{connection_id}",
    response_model=ConnectionRead,
    summary="Update Connection",
    description="Update connection details and configuration",
)
def update_connection(
    connection_id: int,
    connection_update: ConnectionUpdate,
    db: Session = Depends(get_db),
    
):
    try:
        service = ConnectionService(db)
        connection = service.update_connection(
            connection_id, connection_update
        )
        return ConnectionRead.model_validate(connection)

    except AppError as e:
        logger.error(f"Error updating connection {connection_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "Bad request", "message": str(e)},
        )
    except Exception as e:
        logger.error(
            f"Unexpected error updating connection {connection_id}: {e}", exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "Internal server error",
                "message": "Failed to update connection",
            },
        )


@router.delete(
    "/{connection_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete Connection",
    description="Delete a connection (soft delete by default)",
)
def delete_connection(
    connection_id: int,
    hard_delete: bool = Query(False, description="Permanently delete from database"),
    db: Session = Depends(get_db),
    
):
    try:
        service = ConnectionService(db)
        service.delete_connection(connection_id, hard_delete=hard_delete)
        return None

    except AppError as e:
        logger.error(f"Error deleting connection {connection_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "Bad request", "message": str(e)},
        )
    except Exception as e:
        logger.error(
            f"Unexpected error deleting connection {connection_id}: {e}", exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "Internal server error",
                "message": "Failed to delete connection",
            },
        )


@router.post(
    "/{connection_id}/test",
    response_model=ConnectionTestResponse,
    summary="Test Connection",
    description="Test connection health and connectivity",
)
def test_connection(
    connection_id: int,
    test_request: ConnectionTestRequest = Body(default=ConnectionTestRequest()),
    db: Session = Depends(get_db),
    
):
    try:
        service = ConnectionService(db)
        result = service.test_connection(
            connection_id, custom_config=test_request.config
        )
        return result

    except AppError as e:
        logger.error(f"Error testing connection {connection_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "Bad request", "message": str(e)},
        )
    except Exception as e:
        logger.error(
            f"Unexpected error testing connection {connection_id}: {e}", exc_info=True
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "Internal server error",
                "message": "Failed to test connection",
            },
        )


@router.post(
    "/{connection_id}/discover",
    response_model=AssetDiscoverResponse,
    summary="Discover Assets",
    description="Discover available assets (tables, files, etc.) in the connection",
)
def discover_assets(
    connection_id: int,
    discover_request: AssetDiscoverRequest = Body(default=AssetDiscoverRequest()),
    db: Session = Depends(get_db),
    
):
    try:
        service = ConnectionService(db)
        result = service.discover_assets(
            connection_id=connection_id,
            pattern=discover_request.pattern,
            asset_types=discover_request.asset_types,
            include_system=discover_request.include_system,
        )
        return result

    except AppError as e:
        logger.error(f"Error discovering assets for connection {connection_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "Bad request", "message": str(e)},
        )
    except Exception as e:
        logger.error(f"Unexpected error discovering assets: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "Internal server error",
                "message": "Failed to discover assets",
            },
        )
