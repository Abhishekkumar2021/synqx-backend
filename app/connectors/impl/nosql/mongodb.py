from typing import Any, Dict, List, Optional, Iterator, Union
import pandas as pd
from pymongo import MongoClient
from app.connectors.base import BaseConnector
from app.core.errors import ConfigurationError, ConnectionFailedError, DataTransferError, SchemaDiscoveryError
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class MongoConfig(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", case_sensitive=False)
    
    connection_string: Optional[str] = Field(None, description="MongoDB Connection String (URI)")
    host: Optional[str] = Field("localhost", description="Host")
    port: int = Field(27017, description="Port")
    username: Optional[str] = Field(None, description="Username")
    password: Optional[str] = Field(None, description="Password")
    database: str = Field(..., description="Database Name")
    auth_source: str = Field("admin", description="Authentication Database")


class MongoDBConnector(BaseConnector):
    """
    Connector for MongoDB.
    """

    def __init__(self, config: Dict[str, Any]):
        self._config_model: Optional[MongoConfig] = None
        self._client: Optional[MongoClient] = None
        self._db = None
        super().__init__(config)

    def validate_config(self) -> None:
        try:
            self._config_model = MongoConfig.model_validate(self.config)
            # Logic: Either connection_string OR (host & port)
            if not self._config_model.connection_string:
                if not self._config_model.host:
                     raise ValueError("Either 'connection_string' or 'host' must be provided.")
        except Exception as e:
            raise ConfigurationError(f"Invalid MongoDB configuration: {e}")

    def connect(self) -> None:
        if self._client:
            return

        try:
            if self._config_model.connection_string:
                self._client = MongoClient(self._config_model.connection_string)
            else:
                self._client = MongoClient(
                    host=self._config_model.host,
                    port=self._config_model.port,
                    username=self._config_model.username,
                    password=self._config_model.password,
                    authSource=self._config_model.auth_source
                )
            
            # Verify connection
            self._client.admin.command('ping')
            self._db = self._client[self._config_model.database]
            
        except Exception as e:
            raise ConnectionFailedError(f"Failed to connect to MongoDB: {e}")

    def disconnect(self) -> None:
        if self._client:
            self._client.close()
            self._client = None
            self._db = None

    def test_connection(self) -> bool:
        try:
            with self.session():
                return True
        except Exception:
            return False

    def discover_assets(
        self, pattern: Optional[str] = None, include_metadata: bool = False, **kwargs
    ) -> List[Dict[str, Any]]:
        self.connect()
        try:
            collections = self._db.list_collection_names()
            
            if pattern:
                collections = [c for c in collections if pattern.lower() in c.lower()]

            if not include_metadata:
                return [{"name": c, "type": "collection"} for c in collections]

            enriched = []
            for col_name in collections:
                stats = self._db.command("collstats", col_name)
                enriched.append({
                    "name": col_name,
                    "type": "collection",
                    "row_count": stats.get('count', 0),
                    "size_bytes": stats.get('size', 0),
                    "avg_obj_size": stats.get('avgObjSize', 0)
                })
            return enriched

        except Exception as e:
            raise ConnectionFailedError(f"Failed to discover assets: {e}")

    def infer_schema(
        self,
        asset: str,
        sample_size: int = 1000,
        mode: str = "auto",
        **kwargs,
    ) -> Dict[str, Any]:
        self.connect()
        try:
            # MongoDB is schemaless, so we infer from a sample
            collection = self._db[asset]
            cursor = collection.find().limit(sample_size)
            records = list(cursor)
            
            if not records:
                 return {"asset": asset, "columns": [], "note": "Empty collection"}

            df = pd.DataFrame(records)
            
            # Flattening could be complex for nested objects, keeping it simple for now
            columns = [
                {"name": col, "type": str(dtype)}
                for col, dtype in df.dtypes.items()
            ]
            return {
                "asset": asset,
                "columns": columns,
                "schema_type": "inferred"
            }

        except Exception as e:
            raise SchemaDiscoveryError(f"Failed to infer schema for {asset}: {e}")

    def read_batch(
        self,
        asset: str,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        **kwargs,
    ) -> Iterator[pd.DataFrame]:
        
        self.connect()
        collection = self._db[asset]
        
        # Build query
        filter_query = kwargs.get("filter", {})
        projection = kwargs.get("projection", None)
        
        cursor = collection.find(filter_query, projection)
        
        if offset:
            cursor = cursor.skip(offset)
        if limit:
            cursor = cursor.limit(limit)

        chunksize = kwargs.get("chunksize", 1000)
        
        current_batch = []
        for doc in cursor:
            # Convert ObjectId to string for better compatibility
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])
            
            current_batch.append(doc)
            
            if len(current_batch) >= chunksize:
                yield pd.DataFrame(current_batch)
                current_batch = []
        
        if current_batch:
            yield pd.DataFrame(current_batch)

    def write_batch(
        self,
        data: Union[pd.DataFrame, Iterator[pd.DataFrame]],
        asset: str,
        mode: str = "append",
        **kwargs,
    ) -> int:
        
        self.connect()
        collection = self._db[asset]
        
        if mode == "replace":
             collection.drop()
             
        total_written = 0
        
        if isinstance(data, pd.DataFrame):
            iterator = [data]
        else:
            iterator = data
            
        for df in iterator:
            records = df.to_dict(orient="records")
            if not records:
                continue
                
            collection.insert_many(records)
            total_written += len(records)
            
        return total_written
