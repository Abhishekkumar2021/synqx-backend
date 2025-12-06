from typing import Optional, List
from fastapi import APIRouter, Depends, HTTPException, status, Query, Body
from sqlalchemy.orm import Session

from app.schemas.connection import (
    AssetCreate,
    AssetUpdate,
    AssetRead,
    AssetDetailRead,
    AssetListResponse,
    SchemaDiscoveryRequest,
    SchemaDiscoveryResponse,
    AssetSchemaVersionRead,
)
from app.services.connection_service import ConnectionService
from app.api.deps import get_db
from app.core.errors import AppError
from app.core.logging import get_logger

router = APIRouter()
logger = get_logger(__name__)


@router.post(
    "/",
    response_model=AssetDetailRead,
    status_code=status.HTTP_201_CREATED,
    summary="Create Asset",
    description="Create a new asset (table, file, etc.)",
)
def create_asset(
    asset_create: AssetCreate,
    db: Session = Depends(get_db),
    
):
    try:
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
    "/",
    response_model=AssetListResponse,
    summary="List Assets",
    description="List all assets for the current tenant",
)
def list_assets(
    connection_id: Optional[int] = Query(None, description="Filter by connection"),
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


@router.get(
    "/{asset_id}",
    response_model=AssetDetailRead,
    summary="Get Asset",
    description="Get detailed information about a specific asset",
)
def get_asset(
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
    "/{asset_id}",
    response_model=AssetRead,
    summary="Update Asset",
    description="Update asset details and metadata",
)
def update_asset(
    asset_id: int,
    asset_update: AssetUpdate,
    db: Session = Depends(get_db),
    
):
    try:
        service = ConnectionService(db)
        asset = service.update_asset(asset_id, asset_update)
        return AssetRead.model_validate(asset)

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
    "/{asset_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete Asset",
    description="Delete an asset (soft delete by default)",
)
def delete_asset(
    asset_id: int,
    hard_delete: bool = Query(False, description="Permanently delete from database"),
    db: Session = Depends(get_db),
    
):
    try:
        service = ConnectionService(db)
        service.delete_asset(asset_id, hard_delete=hard_delete)
        return None

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
    "/{asset_id}/discover-schema",
    response_model=SchemaDiscoveryResponse,
    summary="Discover Asset Schema",
    description="Discover and version the schema of an asset",
)
def discover_schema(
    asset_id: int,
    discovery_request: SchemaDiscoveryRequest = Body(default=SchemaDiscoveryRequest()),
    db: Session = Depends(get_db),
    
):
    try:
        service = ConnectionService(db)
        result = service.discover_schema(
            asset_id=asset_id,
            sample_size=discovery_request.sample_size,
            force_refresh=discovery_request.force_refresh,
        )
        return result

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
    "/{asset_id}/schema-versions",
    response_model=List[AssetSchemaVersionRead],
    summary="List Schema Versions",
    description="Get all schema versions for an asset",
)
def list_schema_versions(
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

    if not asset.schema_versions:
        return []

    return [AssetSchemaVersionRead.model_validate(v) for v in asset.schema_versions]
