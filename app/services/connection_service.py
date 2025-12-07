from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, timezone
import hashlib
import json
import time
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy import and_

from app.models.connections import Connection, Asset, AssetSchemaVersion
from app.models.enums import ConnectorType
from app.schemas.connection import (
    ConnectionCreate,
    ConnectionUpdate,
    AssetCreate,
    AssetUpdate,
    ConnectionTestResponse,
    AssetDiscoverResponse,
    SchemaDiscoveryResponse,
)
from app.core.errors import AppError
from app.services.vault_service import VaultService
from app.connectors.factory import ConnectorFactory
from app.core.logging import get_logger

logger = get_logger(__name__)


class ConnectionService:

    def __init__(self, db_session: Session):
        self.db_session = db_session

    def create_connection(self, connection_create: ConnectionCreate) -> Connection:
        try:
            encrypted_config = VaultService.encrypt_config(connection_create.config)
            connection = Connection(
                name=connection_create.name,
                connector_type=connection_create.connector_type,
                config_encrypted=encrypted_config,
                description=connection_create.description,
                tags=connection_create.tags,
                max_concurrent_connections=connection_create.max_concurrent_connections,
                connection_timeout_seconds=connection_create.connection_timeout_seconds,
                health_status="unknown",
            )
            self.db_session.add(connection)
            self.db_session.flush()
            test_result = self._test_connection_internal(connection)
            connection.health_status = (
                "healthy" if test_result["success"] else "unhealthy"
            )
            connection.last_test_at = datetime.now(timezone.utc)
            connection.error_message = (
                None if test_result["success"] else test_result["message"]
            )
            self.db_session.commit()
            self.db_session.refresh(connection)
            return connection
        except IntegrityError:
            self.db_session.rollback()
            raise AppError(
                f"Connection with name '{connection_create.name}' already exists"
            )
        except Exception as e:
            self.db_session.rollback()
            raise AppError(f"Failed to create connection: {str(e)}")

    def get_connection(self, connection_id: int) -> Optional[Connection]:
        return (
            self.db_session.query(Connection)
            .filter(
                and_(Connection.id == connection_id, Connection.deleted_at.is_(None))
            )
            .first()
        )

    def list_connections(
        self,
        connector_type: Optional[ConnectorType] = None,
        health_status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Tuple[List[Connection], int]:

        query = self.db_session.query(Connection).filter(
            Connection.deleted_at.is_(None)
        )
        if connector_type:
            query = query.filter(Connection.connector_type == connector_type)
        if health_status:
            query = query.filter(Connection.health_status == health_status)
        total = query.count()
        items = (
            query.order_by(Connection.created_at.desc())
            .limit(limit)
            .offset(offset)
            .all()
        )
        return items, total

    def update_connection(
        self, connection_id: int, connection_update: ConnectionUpdate
    ) -> Connection:
        connection = self.get_connection(connection_id)
        if not connection:
            raise AppError(f"Connection {connection_id} not found")

        try:
            if connection_update.name is not None:
                connection.name = connection_update.name
            if connection_update.description is not None:
                connection.description = connection_update.description
            if connection_update.tags is not None:
                connection.tags = connection_update.tags
            if connection_update.max_concurrent_connections is not None:
                connection.max_concurrent_connections = (
                    connection_update.max_concurrent_connections
                )
            if connection_update.connection_timeout_seconds is not None:
                connection.connection_timeout_seconds = (
                    connection_update.connection_timeout_seconds
                )
            if connection_update.config is not None:
                encrypted = VaultService.encrypt_config(connection_update.config)
                connection.config_encrypted = encrypted
                test_result = self._test_connection_internal(connection)
                connection.health_status = (
                    "healthy" if test_result["success"] else "unhealthy"
                )
                connection.last_test_at = datetime.now(timezone.utc)
                connection.error_message = (
                    None if test_result["success"] else test_result["message"]
                )
            connection.updated_at = datetime.now(timezone.utc)
            self.db_session.commit()
            self.db_session.refresh(connection)
            return connection
        except IntegrityError:
            self.db_session.rollback()
            raise AppError("Connection name already exists")
        except Exception as e:
            self.db_session.rollback()
            raise AppError(f"Failed to update connection: {str(e)}")

    def delete_connection(self, connection_id: int, hard_delete: bool = False) -> bool:
        connection = self.get_connection(connection_id)
        if not connection:
            raise AppError(f"Connection {connection_id} not found")

        try:
            if hard_delete:
                self.db_session.delete(connection)
            else:
                connection.deleted_at = datetime.now(timezone.utc)
                connection.health_status = "deleted"
            self.db_session.commit()
            return True
        except Exception as e:
            self.db_session.rollback()
            raise AppError(f"Failed to delete connection: {str(e)}")

    def test_connection(
        self, connection_id: int, custom_config: Optional[Dict[str, Any]] = None
    ) -> ConnectionTestResponse:
        connection = self.get_connection(connection_id)
        if not connection:
            raise AppError(f"Connection {connection_id} not found")

        if custom_config:
            temp = Connection(
                connector_type=connection.connector_type,
                config_encrypted=VaultService.encrypt_config(custom_config),
            )
            result = self._test_connection_internal(temp)
        else:
            result = self._test_connection_internal(connection)
            connection.health_status = "healthy" if result["success"] else "unhealthy"
            connection.last_test_at = datetime.now(timezone.utc)
            connection.error_message = None if result["success"] else result["message"]
            self.db_session.commit()

        return ConnectionTestResponse(**result)

    def _test_connection_internal(self, connection: Connection) -> Dict[str, Any]:
        start = time.time()
        try:
            config = VaultService.get_connector_config(connection)
            connector = ConnectorFactory.get_connector(
                connection.connector_type.value, config
            )
            with connector.session() as session:
                session.test_connection()
            latency = (time.time() - start) * 1000
            return {
                "success": True,
                "message": "Connection successful",
                "latency_ms": round(latency, 2),
                "details": {"connector_type": connection.connector_type.value},
            }
        except Exception as e:
            latency = (time.time() - start) * 1000
            return {
                "success": False,
                "message": str(e),
                "latency_ms": round(latency, 2),
                "details": {"error_type": type(e).__name__},
            }

    def create_asset(self, asset_create: AssetCreate) -> Asset:
        connection = self.get_connection(asset_create.connection_id)
        if not connection:
            raise AppError(f"Connection {asset_create.connection_id} not found")

        try:
            asset = Asset(
                connection_id=asset_create.connection_id,
                name=asset_create.name,
                asset_type=asset_create.asset_type,
                fully_qualified_name=asset_create.fully_qualified_name,
                is_source=asset_create.is_source,
                is_destination=asset_create.is_destination,
                is_incremental_capable=asset_create.is_incremental_capable,
                description=asset_create.description,
                tags=asset_create.tags,
                schema_metadata=asset_create.schema_metadata,
                row_count_estimate=asset_create.row_count_estimate,
                size_bytes_estimate=asset_create.size_bytes_estimate,
            )
            self.db_session.add(asset)
            self.db_session.commit()
            self.db_session.refresh(asset)
            return asset
        except IntegrityError:
            self.db_session.rollback()
            raise AppError(
                f"Asset with name '{asset_create.name}' already exists for this connection"
            )
        except Exception as e:
            self.db_session.rollback()
            raise AppError(f"Failed to create asset: {str(e)}")

    def get_asset(self, asset_id: int) -> Optional[Asset]:
        return (
            self.db_session.query(Asset)
            .filter(and_(Asset.id == asset_id, Asset.deleted_at.is_(None)))
            .first()
        )

    def list_assets(
        self,
        connection_id: Optional[int] = None,
        asset_type: Optional[str] = None,
        is_source: Optional[bool] = None,
        is_destination: Optional[bool] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Tuple[List[Asset], int]:

        query = self.db_session.query(Asset).filter(Asset.deleted_at.is_(None))

        if connection_id:
            query = query.filter(Asset.connection_id == connection_id)
        if asset_type:
            query = query.filter(Asset.asset_type == asset_type)
        if is_source is not None:
            query = query.filter(Asset.is_source == is_source)
        if is_destination is not None:
            query = query.filter(Asset.is_destination == is_destination)

        total = query.count()
        items = (
            query.order_by(Asset.created_at.desc()).limit(limit).offset(offset).all()
        )
        return items, total

    def update_asset(self, asset_id: int, asset_update: AssetUpdate) -> Asset:
        asset = self.get_asset(asset_id)
        if not asset:
            raise AppError(f"Asset {asset_id} not found")

        try:
            if asset_update.name is not None:
                asset.name = asset_update.name
            if asset_update.asset_type is not None:
                asset.asset_type = asset_update.asset_type
            if asset_update.fully_qualified_name is not None:
                asset.fully_qualified_name = asset_update.fully_qualified_name
            if asset_update.is_source is not None:
                asset.is_source = asset_update.is_source
            if asset_update.is_destination is not None:
                asset.is_destination = asset_update.is_destination
            if asset_update.is_incremental_capable is not None:
                asset.is_incremental_capable = asset_update.is_incremental_capable
            if asset_update.description is not None:
                asset.description = asset_update.description
            if asset_update.tags is not None:
                asset.tags = asset_update.tags
            if asset_update.schema_metadata is not None:
                asset.schema_metadata = asset_update.schema_metadata

            asset.updated_at = datetime.now(timezone.utc)
            self.db_session.commit()
            self.db_session.refresh(asset)
            return asset
        except IntegrityError:
            self.db_session.rollback()
            raise AppError("Asset name already exists for this connection")
        except Exception as e:
            self.db_session.rollback()
            raise AppError(f"Failed to update asset: {str(e)}")

    def delete_asset(self, asset_id: int, hard_delete: bool = False) -> bool:
        asset = self.get_asset(asset_id)
        if not asset:
            raise AppError(f"Asset {asset_id} not found")

        try:
            if hard_delete:
                self.db_session.delete(asset)
            else:
                asset.deleted_at = datetime.now(timezone.utc)
            self.db_session.commit()
            return True
        except Exception as e:
            self.db_session.rollback()
            raise AppError(f"Failed to delete asset: {str(e)}")

    def discover_assets(
        self,
        connection_id: int,
        include_metadata: bool = False,
        pattern: Optional[str] = None,
    ) -> AssetDiscoverResponse:
        connection = self.get_connection(connection_id)
        if not connection:
            raise AppError(f"Connection {connection_id} not found")

        try:
            config = VaultService.get_connector_config(connection)
            connector = ConnectorFactory.get_connector(
                connection.connector_type.value, config
            )

            with connector.session() as session:
                discovered = session.discover_assets(
                    pattern=pattern,
                    include_metadata=include_metadata, 
                )

            connection.last_schema_discovery_at = datetime.now(timezone.utc)
            self.db_session.commit()

            return AssetDiscoverResponse(
                discovered_count=len(discovered),
                assets=discovered,
                message=f"Successfully discovered {len(discovered)} assets",
            )
        except Exception as e:
            raise AppError(f"Failed to discover assets: {str(e)}")

    def discover_schema(
        self, asset_id: int, sample_size: int = 1000, force_refresh: bool = False
    ) -> SchemaDiscoveryResponse:
        asset = self.get_asset(asset_id)
        if not asset:
            raise AppError(f"Asset {asset_id} not found")

        try:
            config = VaultService.get_connector_config(asset.connection)
            connector = ConnectorFactory.get_connector(
                asset.connection.connector_type.value, config
            )

            with connector.session() as session:
                schema = session.infer_schema(asset.name, sample_size=sample_size)

            schema_json = json.dumps(schema, sort_keys=True)
            schema_hash = hashlib.sha256(schema_json.encode()).hexdigest()

            latest = (
                self.db_session.query(AssetSchemaVersion)
                .filter(AssetSchemaVersion.asset_id == asset_id)
                .order_by(AssetSchemaVersion.version.desc())
                .first()
            )

            if latest and latest.schema_hash != schema_hash:
                breaking = self._detect_breaking_changes(latest.json_schema, schema)
                next_version = latest.version + 1
            elif not latest:
                breaking = False
                next_version = 1
            else:
                return SchemaDiscoveryResponse(
                    success=True,
                    schema_version=latest.version,
                    is_breaking_change=False,
                    message="Schema unchanged",
                    schema=schema,
                )

            schema_version = AssetSchemaVersion(
                asset_id=asset_id,
                version=next_version,
                json_schema=schema,
                schema_hash=schema_hash,
                is_breaking_change=breaking,
                discovered_at=datetime.now(timezone.utc),
            )

            self.db_session.add(schema_version)
            asset.current_schema_version = next_version
            asset.schema_metadata = schema
            self.db_session.commit()

            return SchemaDiscoveryResponse(
                success=True,
                schema_version=next_version,
                is_breaking_change=breaking,
                message=f"Schema version {next_version} created",
                schema=schema,
            )
        except Exception as e:
            self.db_session.rollback()
            return SchemaDiscoveryResponse(
                success=False, message=f"Failed to discover schema: {str(e)}"
            )

    def _detect_breaking_changes(
        self, old_schema: Dict[str, Any], new_schema: Dict[str, Any]
    ) -> bool:
        old_cols = {c["name"]: c for c in old_schema.get("columns", [])}
        new_cols = {c["name"]: c for c in new_schema.get("columns", [])}

        for name, col in old_cols.items():
            if name not in new_cols:
                return True
            if col.get("type") != new_cols[name].get("type"):
                return True
        return False
