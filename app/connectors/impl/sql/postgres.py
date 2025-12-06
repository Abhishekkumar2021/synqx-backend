from typing import Any, Dict, Iterator, List, Optional, Union
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Connection, Engine
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from app.connectors.base import BaseConnector
from app.core.errors import (
    ConfigurationError,
    ConnectionFailedError,
    AuthenticationError,
    SchemaDiscoveryError,
    DataTransferError,
)
from app.core.logging import get_logger

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
            raise ConfigurationError(f"Invalid PostgreSQL configuration: {e}") from e

    def _build_sqlalchemy_url(self) -> str:
        if not self._config_model:
            raise ConfigurationError("Configuration not validated.")
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
                self._engine = create_engine(self._build_sqlalchemy_url(), future=True)
            self._connection = self._engine.connect()
            logger.info("postgresql_connected")
        except Exception as e:
            if "authentication" in str(e).lower() or "password" in str(e).lower():
                raise AuthenticationError(f"Authentication failed: {e}") from e
            raise ConnectionFailedError(f"Connection failed: {e}") from e

    def disconnect(self) -> None:
        if self._connection:
            try:
                self._connection.close()
                logger.info("postgresql_disconnected")
            finally:
                self._connection = None
        if self._engine:
            try:
                self._engine.dispose()
                logger.info("postgresql_engine_disposed")
            finally:
                self._engine = None

    def test_connection(self) -> bool:
        try:
            with self.session():
                return True
        except Exception:
            return False

    def list_assets(self) -> List[str]:
        if not self._connection:
            self.connect()
        try:
            inspector = inspect(self._engine)
            return inspector.get_table_names(schema=self._config_model.db_schema)
        except Exception as e:
            raise SchemaDiscoveryError(f"Failed to list tables: {e}") from e

    def get_schema(self, asset: str) -> Dict[str, Any]:
        if not self._connection:
            self.connect()
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
                        "nullable": col["nullable"],
                        "default": col.get("default"),
                        "primary_key": col.get("primary_key", False),
                    }
                    for col in columns
                ],
            }
        except Exception as e:
            raise SchemaDiscoveryError(f"Failed to read schema for {asset}: {e}") from e

    def read_batch(
        self,
        asset: str,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        **kwargs,
    ) -> Iterator[pd.DataFrame]:

        if not self._connection:
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
            raise DataTransferError(f"Failed to read from {asset}: {e}") from e

    def write_batch(
        self,
        data: Union[pd.DataFrame, Iterator[pd.DataFrame]],
        asset: str,
        mode: str = "append",
        **kwargs,
    ) -> int:

        if not self._connection:
            self.connect()

        total = 0
        data_iter = [data] if isinstance(data, pd.DataFrame) else data

        try:
            for df in data_iter:
                df.to_sql(
                    name=asset,
                    schema=self._config_model.db_schema,
                    con=self._connection,
                    if_exists=mode,
                    index=False,
                    **kwargs,
                )
                total += len(df)
            logger.info(f"{total} rows written to {self._config_model.db_schema}.{asset}")
            return total
        except Exception as e:
            raise DataTransferError(f"Write failed: {e}") from e
