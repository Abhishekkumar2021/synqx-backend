from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from app.api.deps import get_db, get_current_user
from app.api import deps
from app.services import connection_service
from app.services.vault_service import VaultService
from app.connectors.factory import ConnectorFactory
from pydantic import BaseModel

router = APIRouter()

class QueryRequest(BaseModel):
    query: str
    limit: Optional[int] = 100
    offset: Optional[int] = 0
    params: Optional[Dict[str, Any]] = None

class QueryResponse(BaseModel):
    results: List[Dict[str, Any]]
    count: int
    columns: List[str]

@router.post("/{connection_id}/execute", response_model=QueryResponse)
def execute_connection_query(
    connection_id: int,
    request: QueryRequest,
    db: Session = Depends(get_db),
    current_user: Any = Depends(get_current_user),
):
    """
    Execute a raw query against a connection.
    Supports SQL and NoSQL depending on the connector type.
    """
    service = connection_service.ConnectionService(db)
    connection = service.get_connection(connection_id)
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    try:
        config = VaultService.get_connector_config(connection)
        connector = ConnectorFactory.get_connector(
            connector_type=connection.connector_type.value,
            config=config
        )
        
        results = connector.execute_query(
            query=request.query,
            limit=request.limit,
            offset=request.offset,
            **(request.params or {})
        )
        
        columns = []
        if results and len(results) > 0:
            columns = list(results[0].keys())
            
        return {
            "results": results,
            "count": len(results),
            "columns": columns
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/{connection_id}/schema-metadata")
def get_connection_schema_metadata(
    connection_id: int,
    db: Session = Depends(get_db),
    current_user: Any = Depends(get_current_user),
):
    """
    Get full schema metadata for autocompletion.
    """
    service = connection_service.ConnectionService(db)
    connection = service.get_connection(connection_id)
    if not connection:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    try:
        config = VaultService.get_connector_config(connection)
        connector = ConnectorFactory.get_connector(
            connector_type=connection.connector_type.value,
            config=config
        )
        
        assets = connector.discover_assets(include_metadata=False)
        
        # For each asset, get columns for rich autocompletion
        metadata = {}
        for asset in assets:
            asset_name = asset["name"]
            try:
                schema = connector.infer_schema(asset_name)
                metadata[asset_name] = [col["name"] for col in schema.get("columns", [])]
            except:
                metadata[asset_name] = []
                
        return {
            "connector_type": connection.connector_type,
            "metadata": metadata
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
