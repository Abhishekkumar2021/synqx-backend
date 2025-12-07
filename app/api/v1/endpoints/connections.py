from typing import Optional, List
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
    AssetUpdate,
    AssetRead,
    AssetDetailRead,
    AssetListResponse,
    AssetDiscoverRequest,
    AssetDiscoverResponse,
    SchemaDiscoveryRequest,
    SchemaDiscoveryResponse,
    AssetSchemaVersionRead,
)
from app.services.connection_service import ConnectionService
from app.api.deps import get_db
from app.core.errors import AppError
from app.core.logging import get_logger
from app.models.enums import ConnectorType

router = APIRouter()
logger = get_logger(__name__)


# ==================== CONNECTION ENDPOINTS ====================


@router.post(
    "",
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
    "",
    response_model=ConnectionListResponse,
    summary="List Connections",
    description="List all connections",
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
        connection = service.update_connection(connection_id, connection_update)
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
            include_metadata=discover_request.include_metadata,
            pattern=discover_request.pattern,
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

@router.get(
    "/{connection_id}/assets",
    response_model=AssetListResponse,
    summary="List Assets for Connection",
    description="List all assets for a specific connection",
)
def list_connection_assets(
    connection_id: int,
    asset_type: Optional[str] = Query(None, description="Filter by asset type"),
    is_source: Optional[bool] = Query(None, description="Filter source assets"),
    is_destination: Optional[bool] = Query(
        None, description="Filter destination assets"
    ),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    try:
        service = ConnectionService(db)
        assets, total = service.list_assets(
            connection_id=connection_id,
            asset_type=asset_type,
            is_source=is_source,
            is_destination=is_destination,
            limit=limit,
            offset=offset,
        )
        return AssetListResponse(
            assets=[AssetRead.model_validate(a) for a in assets],
            total=total,
            limit=limit,
            offset=offset,
        )
    except Exception as e:
        logger.error(f"Error listing assets: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "Internal server error",
                "message": "Failed to list assets",
            },
        )


@router.post(
    "/{connection_id}/assets",
    response_model=AssetDetailRead,
    status_code=status.HTTP_201_CREATED,
    summary="Create Asset",
    description="Create a new asset for this connection",
)
def create_asset(
    connection_id: int,
    asset_create: AssetCreate,
    db: Session = Depends(get_db),
):
    try:
        asset_create.connection_id = connection_id

        service = ConnectionService(db)
        asset = service.create_asset(asset_create)
        response = AssetDetailRead.model_validate(asset)
        response.connection_name = asset.connection.name if asset.connection else None
        response.schema_version_count = (
            len(asset.schema_versions) if asset.schema_versions else 0
        )
        return response
    except AppError as e:
        logger.error(f"Error creating asset: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "Bad request", "message": str(e)},
        )
    except Exception as e:
        logger.error(f"Unexpected error creating asset: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "Internal server error",
                "message": "Failed to create asset",
            },
        )


@router.get(
    "/{connection_id}/assets/{asset_id}",
    response_model=AssetDetailRead,
    summary="Get Asset",
    description="Get detailed information about a specific asset",
)
def get_asset(
    connection_id: int,
    asset_id: int,
    db: Session = Depends(get_db),
):
    service = ConnectionService(db)
    asset = service.get_asset(asset_id)

    if not asset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "Not found", "message": f"Asset {asset_id} not found"},
        )

    # Verify asset belongs to this connection
    if asset.connection_id != connection_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "Not found",
                "message": f"Asset {asset_id} not found in connection {connection_id}",
            },
        )

    response = AssetDetailRead.model_validate(asset)
    response.connection_name = asset.connection.name if asset.connection else None
    response.schema_version_count = (
        len(asset.schema_versions) if asset.schema_versions else 0
    )

    if asset.schema_versions:
        latest_schema = asset.schema_versions[0]
        response.latest_schema = AssetSchemaVersionRead.model_validate(latest_schema)

    return response


@router.patch(
    "/{connection_id}/assets/{asset_id}",
    response_model=AssetRead,
    summary="Update Asset",
    description="Update asset details and metadata",
)
def update_asset(
    connection_id: int,
    asset_id: int,
    asset_update: AssetUpdate,
    db: Session = Depends(get_db),
):
    try:
        service = ConnectionService(db)
        asset = service.get_asset(asset_id)

        if not asset:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"error": "Not found", "message": f"Asset {asset_id} not found"},
            )

        if asset.connection_id != connection_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error": "Not found",
                    "message": f"Asset {asset_id} not found in connection {connection_id}",
                },
            )

        asset = service.update_asset(asset_id, asset_update)
        return AssetRead.model_validate(asset)
    except HTTPException:
        raise
    except AppError as e:
        logger.error(f"Error updating asset {asset_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "Bad request", "message": str(e)},
        )
    except Exception as e:
        logger.error(f"Unexpected error updating asset {asset_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "Internal server error",
                "message": "Failed to update asset",
            },
        )


@router.delete(
    "/{connection_id}/assets/{asset_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete Asset",
    description="Delete an asset (soft delete by default)",
)
def delete_asset(
    connection_id: int,
    asset_id: int,
    hard_delete: bool = Query(False, description="Permanently delete from database"),
    db: Session = Depends(get_db),
):
    try:
        service = ConnectionService(db)
        asset = service.get_asset(asset_id)

        if not asset:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"error": "Not found", "message": f"Asset {asset_id} not found"},
            )

        if asset.connection_id != connection_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error": "Not found",
                    "message": f"Asset {asset_id} not found in connection {connection_id}",
                },
            )

        service.delete_asset(asset_id, hard_delete=hard_delete)
        return None
    except HTTPException:
        raise
    except AppError as e:
        logger.error(f"Error deleting asset {asset_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "Bad request", "message": str(e)},
        )
    except Exception as e:
        logger.error(f"Unexpected error deleting asset {asset_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "Internal server error",
                "message": "Failed to delete asset",
            },
        )


@router.post(
    "/{connection_id}/assets/{asset_id}/discover-schema",
    response_model=SchemaDiscoveryResponse,
    summary="Discover Asset Schema",
    description="Discover and version the schema of an asset",
)
def discover_schema(
    connection_id: int,
    asset_id: int,
    discovery_request: SchemaDiscoveryRequest = Body(default=SchemaDiscoveryRequest()),
    db: Session = Depends(get_db),
):
    try:
        service = ConnectionService(db)
        asset = service.get_asset(asset_id)

        if not asset:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"error": "Not found", "message": f"Asset {asset_id} not found"},
            )

        if asset.connection_id != connection_id:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={
                    "error": "Not found",
                    "message": f"Asset {asset_id} not found in connection {connection_id}",
                },
            )

        result = service.discover_schema(
            asset_id=asset_id,
            sample_size=discovery_request.sample_size,
            force_refresh=discovery_request.force_refresh,
        )
        return result
    except HTTPException:
        raise
    except AppError as e:
        logger.error(f"Error discovering schema for asset {asset_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "Bad request", "message": str(e)},
        )
    except Exception as e:
        logger.error(f"Unexpected error discovering schema: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={
                "error": "Internal server error",
                "message": "Failed to discover schema",
            },
        )


@router.get(
    "/{connection_id}/assets/{asset_id}/schema-versions",
    response_model=List[AssetSchemaVersionRead],
    summary="List Schema Versions",
    description="Get all schema versions for an asset",
)
def list_schema_versions(
    connection_id: int,
    asset_id: int,
    db: Session = Depends(get_db),
):
    service = ConnectionService(db)
    asset = service.get_asset(asset_id)

    if not asset:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "Not found", "message": f"Asset {asset_id} not found"},
        )

    if asset.connection_id != connection_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={
                "error": "Not found",
                "message": f"Asset {asset_id} not found in connection {connection_id}",
            },
        )

    if not asset.schema_versions:
        return []

    return [AssetSchemaVersionRead.model_validate(v) for v in asset.schema_versions]
