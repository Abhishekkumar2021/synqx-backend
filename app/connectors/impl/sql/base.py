from abc import abstractmethod
from typing import Any, Dict, List, Optional, Iterator, Union
import pandas as pd
import json
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Engine, Connection
from app.connectors.base import BaseConnector
from app.core.errors import (
    ConfigurationError,
    ConnectionFailedError,
    AuthenticationError,
    SchemaDiscoveryError,
    DataTransferError,
)
from app.core.logging import get_logger
from datetime import datetime
import time

logger = get_logger(__name__)

class SQLConnector(BaseConnector):
    """
    Base class for all SQL-based connectors using SQLAlchemy.
    Provides robust implementations for common SQL operations.
    """
    
    def __init__(self, config: Dict[str, Any]):
        self._engine: Optional[Engine] = None
        self._connection: Optional[Connection] = None
        super().__init__(config)

    @abstractmethod
    def _sqlalchemy_url(self) -> str:
        pass

    def _get_engine_options(self) -> Dict[str, Any]:
        return {
            "pool_size": 5,
            "max_overflow": 10,
            "pool_timeout": 30,
            "pool_recycle": 1800,
            "future": True
        }

    def connect(self) -> None:
        if self._connection and not self._connection.closed:
            return
        
        try:
            if not self._engine:
                self._engine = create_engine(
                    self._sqlalchemy_url(), 
                    **self._get_engine_options()
                )
            self._connection = self._engine.connect()
        except Exception as e:
            msg = str(e).lower()
            if "authentication" in msg or "denied" in msg or "password" in msg:
                raise AuthenticationError(f"Authentication failed: {e}")
            raise ConnectionFailedError(f"Connection failed: {e}")

    def disconnect(self) -> None:
        try:
            if self._connection:
                self._connection.close()
            if self._engine:
                self._engine.dispose()
        except Exception as e:
            logger.warning(f"Error during disconnect: {e}")
        finally:
            self._connection = None
            self._engine = None

    def test_connection(self) -> bool:
        try:
            self.connect()
            self._connection.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False

    def discover_assets(
        self, pattern: Optional[str] = None, include_metadata: bool = False, **kwargs
    ) -> List[Dict[str, Any]]:
        self.connect()
        try:
            inspector = inspect(self._engine)
            db_schema = self.config.get("db_schema") or self.config.get("schema")
            
            tables = inspector.get_table_names(schema=db_schema)
            views = inspector.get_view_names(schema=db_schema)
            all_assets = tables + views

            if pattern:
                all_assets = [t for t in all_assets if pattern.lower() in t.lower()]

            if not include_metadata:
                return [{"name": t, "type": "table" if t in tables else "view"} for t in all_assets]

            results = []
            for asset in all_assets:
                results.append({
                    "name": asset,
                    "type": "table" if asset in tables else "view",
                    "schema": db_schema,
                    "row_count": self._get_row_count(asset, db_schema),
                })
            return results
        except Exception as e:
            raise SchemaDiscoveryError(f"Failed to discover assets: {e}")

    def _get_row_count(self, table: str, schema: Optional[str] = None) -> Optional[int]:
        try:
            table_ref = f'"{schema}"."{table}"' if schema else f'"{table}"'
            query = text(f"SELECT COUNT(*) FROM {table_ref}")
            return int(self._connection.execute(query).scalar())
        except Exception:
            return None

    def infer_schema(
        self, asset: str, sample_size: int = 1000, mode: str = "auto", **kwargs
    ) -> Dict[str, Any]:
        self.connect()
        db_schema = self.config.get("db_schema") or self.config.get("schema")
        
        if mode == "metadata":
            return self._schema_from_metadata(asset, db_schema)
        
        try:
            return self._schema_from_metadata(asset, db_schema)
        except Exception:
            if mode == "sample" or mode == "auto":
                return self._schema_from_sample(asset, db_schema, sample_size)
            raise

    def _schema_from_metadata(self, asset: str, schema: Optional[str]) -> Dict[str, Any]:
        try:
            inspector = inspect(self._engine)
            columns = inspector.get_columns(asset, schema=schema)
            return {
                "asset": asset,
                "schema": schema,
                "columns": [
                    {
                        "name": col["name"],
                        "type": str(col["type"]),
                        "nullable": col.get("nullable"),
                        "primary_key": col.get("primary_key", False),
                    }
                    for col in columns
                ],
            }
        except Exception as e:
            raise SchemaDiscoveryError(f"Metadata extraction failed: {e}")

    def _schema_from_sample(self, asset: str, schema: Optional[str], sample_size: int) -> Dict[str, Any]:
        try:
            df = next(self.read_batch(asset, limit=sample_size))
            return {
                "asset": asset,
                "schema": schema,
                "columns": [{"name": col, "type": str(dtype)} for col, dtype in df.dtypes.items()],
            }
        except Exception as e:
            raise SchemaDiscoveryError(f"Sample-based inference failed: {e}")

    def read_batch(
        self, asset: str, limit: Optional[int] = None, offset: Optional[int] = None, **kwargs
    ) -> Iterator[pd.DataFrame]:
        self.connect()
        schema = self.config.get("db_schema") or self.config.get("schema")
        table_ref = f'"{schema}"."{asset}"' if schema else f'"{asset}"'
        
        query = f"SELECT * FROM {table_ref}"
        if limit: query += f" LIMIT {limit}"
        if offset: query += f" OFFSET {offset}"
        
        chunksize = kwargs.pop("chunksize", 10000)
        try:
            for chunk in pd.read_sql_query(text(query), con=self._connection, chunksize=chunksize, **kwargs):
                yield chunk
        except Exception as e:
            raise DataTransferError(f"Read failed for {asset}: {e}")

    def write_batch(
        self, data: Union[pd.DataFrame, Iterator[pd.DataFrame]], asset: str, mode: str = "append", **kwargs
    ) -> int:
        self.connect()
        schema = self.config.get("db_schema") or self.config.get("schema")
        
        if isinstance(data, pd.DataFrame):
            data_iter = [data]
        else:
            data_iter = data
            
        total = 0
        try:
            for df in data_iter:
                if df.empty: continue
                # Robust type conversion for SQL
                for col in df.columns:
                    if df[col].dtype == 'object':
                        # Convert complex types to JSON strings if they are dicts/lists
                        df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)
                
                df.to_sql(
                    name=asset,
                    schema=schema,
                    con=self._connection,
                    if_exists='replace' if mode == 'replace' and total == 0 else 'append',
                    index=False,
                    **kwargs
                )
                total += len(df)
            return total
        except Exception as e:
            raise DataTransferError(f"Write failed for {asset}: {e}")
