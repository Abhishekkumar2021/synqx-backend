from typing import Any, Dict, List, Optional, Iterator, Union
import pandas as pd
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
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = get_logger(__name__)


class PostgresConfig(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", case_sensitive=False)

    username: str = Field(..., description="Database username")
    password: str = Field(..., description="Database password")
    host: str = Field(..., description="Database host")
    port: int = Field(5432, description="Database port")
    database: str = Field(..., description="Database name")
    db_schema: str = Field("public")


class PostgresConnector(BaseConnector):

    def __init__(self, config: Dict[str, Any]):
        self._config_model: Optional[PostgresConfig] = None
        self._engine: Optional[Engine] = None
        self._connection: Optional[Connection] = None
        super().__init__(config)

    def validate_config(self) -> None:
        try:
            self._config_model = PostgresConfig.model_validate(self.config)
        except Exception as e:
            raise ConfigurationError(f"Invalid PostgreSQL configuration: {e}")

    def _sqlalchemy_url(self) -> str:
        return (
            f"postgresql+psycopg2://"
            f"{self._config_model.username}:{self._config_model.password}"
            f"@{self._config_model.host}:{self._config_model.port}/"
            f"{self._config_model.database}"
        )

    def connect(self) -> None:
        if self._connection and not self._connection.closed:
            return
        try:
            if not self._engine:
                self._engine = create_engine(self._sqlalchemy_url(), future=True)
            self._connection = self._engine.connect()
            logger.info("postgres_connected")
        except Exception as e:
            if "authentication" in str(e).lower():
                raise AuthenticationError(f"Authentication failed: {e}")
            raise ConnectionFailedError(f"Connection failed: {e}")

    def disconnect(self) -> None:
        try:
            if self._connection:
                self._connection.close()
            if self._engine:
                self._engine.dispose()
        finally:
            self._connection = None
            self._engine = None

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
        inspector = inspect(self._engine)

        tables = inspector.get_table_names(schema=self._config_model.db_schema)

        if pattern:
            tables = [t for t in tables if pattern.lower() in t.lower()]

        if not include_metadata:
            return [{"name": t} for t in tables]

        enriched = []
        for tbl in tables:
            metadata = {
                "name": tbl,
                "type": "table",
                "schema": self._config_model.db_schema,
                "row_count": self._get_row_count(tbl),
                "size_bytes": None,
                "last_modified": None,
            }

            enriched.append(metadata)

        return enriched

    def _get_row_count(self, table: str) -> Optional[int]:
        try:
            query = text(
                f'SELECT COUNT(*) AS cnt FROM "{self._config_model.db_schema}"."{table}"'
            )
            result = self._connection.execute(query).scalar()
            return int(result)
        except Exception:
            return None  

    def infer_schema(
        self, asset: str, sample_size: int = 1000, mode: str = "auto", **kwargs
    ) -> Dict[str, Any]:

        self.connect()

        if mode == "metadata":
            return self._schema_from_metadata(asset)

        if mode == "sample":
            return self._schema_from_sample(asset, sample_size)

        try:
            return self._schema_from_metadata(asset)
        except Exception:
            return self._schema_from_sample(asset, sample_size)

    def _schema_from_metadata(self, asset: str) -> Dict[str, Any]:
        try:
            inspector = inspect(self._engine)
            columns = inspector.get_columns(asset, schema=self._config_model.db_schema)

            return {
                "asset": asset,
                "schema": self._config_model.db_schema,
                "columns": [
                    {
                        "name": col["name"],
                        "type": str(col["type"]),
                        "nullable": col.get("nullable"),
                        "default": col.get("default"),
                        "primary_key": col.get("primary_key", False),
                    }
                    for col in columns
                ],
            }

        except Exception as e:
            raise SchemaDiscoveryError(f"Schema metadata failed: {e}")

    # Infer schema from sample data
    def _schema_from_sample(self, asset: str, sample_size: int) -> Dict[str, Any]:
        try:
            sample_df = next(self.read_batch(asset=asset, limit=sample_size, offset=0))
            return {
                "asset": asset,
                "schema": self._config_model.db_schema,
                "columns": [
                    {"name": col, "type": str(dtype)}
                    for col, dtype in sample_df.dtypes.items()
                ],
            }
        except Exception as e:
            raise SchemaDiscoveryError(f"Sample-based schema inference failed: {e}")


    def read_batch(
        self,
        asset: str,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        **kwargs,
    ) -> Iterator[pd.DataFrame]:

        self.connect()

        query = f'SELECT * FROM "{self._config_model.db_schema}"."{asset}"'
        if limit is not None:
            query += f" LIMIT {limit}"
        if offset is not None:
            query += f" OFFSET {offset}"

        chunksize = kwargs.pop("chunksize", 10000)

        try:
            iterator = pd.read_sql_query(
                text(query), con=self._connection, chunksize=chunksize, **kwargs
            )
            for chunk in iterator:
                yield chunk
        except Exception as e:
            raise DataTransferError(f"Failed to read from {asset}: {e}")


    def write_batch(
        self,
        data: Union[pd.DataFrame, Iterator[pd.DataFrame]],
        asset: str,
        mode: str = "append",
        **kwargs,
    ) -> int:

        self.connect()

        if isinstance(data, pd.DataFrame):
            data = [data]

        total_written = 0

        try:
            for df in data:
                df.to_sql(
                    name=asset,
                    schema=self._config_model.db_schema,
                    con=self._connection,
                    if_exists=mode,
                    index=False,
                    **kwargs,
                )
                total_written += len(df)

            logger.info(f"{total_written} rows written to {asset}")
            return total_written

        except Exception as e:
            raise DataTransferError(f"Write failed: {e}")
