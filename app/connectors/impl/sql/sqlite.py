from typing import Any, Dict, Iterator, List, Optional, Union
import os
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Connection, Engine
from pydantic import Field, FilePath
from pydantic_settings import BaseSettings, SettingsConfigDict

from app.connectors.base import BaseConnector
from app.core.errors import (
    ConfigurationError,
    ConnectionFailedError,
    SchemaDiscoveryError,
    DataTransferError,
)
from app.core.logging import get_logger

logger = get_logger(__name__)

class SQLiteConfig(BaseSettings):
    """
    Configuration model for SQLite connector.
    """
    model_config = SettingsConfigDict(extra='ignore', case_sensitive=False)

    database_path: str = Field(..., description="Path to the SQLite database file")

class SQLiteConnector(BaseConnector):
    """
    A concrete implementation of BaseConnector for SQLite databases.
    """

    def __init__(self, config: Dict[str, Any]):
        self._config_model: Optional[SQLiteConfig] = None
        self._engine: Optional[Engine] = None
        self._connection: Optional[Connection] = None
        super().__init__(config) # This calls validate_config

    def validate_config(self) -> None:
        """
        Validates the configuration using Pydantic.
        """
        try:
            self._config_model = SQLiteConfig.model_validate(self.config)
            # Ensure parent directory exists for the database file
            db_dir = os.path.dirname(self._config_model.database_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)
        except Exception as e:
            raise ConfigurationError(f"Invalid SQLite configuration: {e}") from e

    def _get_sqlalchemy_url(self) -> str:
        """Constructs the SQLAlchemy connection URL."""
        if not self._config_model:
            raise ConfigurationError("Configuration not validated.")
        
        # SQLite URL format: sqlite:///path/to/database.db
        return f"sqlite:///{self._config_model.database_path}"

    def connect(self) -> None:
        """
        Establishes a connection to the SQLite database.
        """
        if self._connection and not self._connection.closed:
            logger.debug("SQLite connection already open.")
            return

        try:
            self._engine = create_engine(self._get_sqlalchemy_url())
            self._connection = self._engine.connect()
            logger.info("Successfully connected to SQLite.")
        except Exception as e:
            logger.error(f"Failed to connect to SQLite: {e}")
            raise ConnectionFailedError(f"SQLite connection failed: {e}") from e

    def disconnect(self) -> None:
        """
        Closes the SQLite database connection and disposes the engine.
        """
        if self._connection:
            try:
                self._connection.close()
                logger.info("SQLite connection closed.")
            except Exception as e:
                logger.warning(f"Error closing SQLite connection: {e}")
            finally:
                self._connection = None
        
        if self._engine:
            try:
                self._engine.dispose()
                logger.info("SQLite engine disposed.")
            except Exception as e:
                logger.warning(f"Error disposing SQLite engine: {e}")
            finally:
                self._engine = None


    def test_connection(self) -> bool:
        """
        Tests if the SQLite connection can be established.
        """
        try:
            with self.session(): # Use the context manager for testing
                self._connection.execute(text("SELECT 1"))
                return True
        except (ConnectionFailedError) as e:
            logger.error(f"SQLite connection test failed: {e}")
            return False
        except Exception as e:
            logger.error(f"An unexpected error occurred during SQLite connection test: {e}")
            return False

    def list_assets(self) -> List[str]:
        """
        Lists all tables in the SQLite database.
        """
        if not self._connection:
            self.connect()

        try:
            inspector = inspect(self._engine)
            tables = inspector.get_table_names()
            logger.debug(f"Found tables in SQLite: {tables}")
            return tables
        except Exception as e:
            raise SchemaDiscoveryError(f"Failed to list assets in SQLite: {e}") from e

    def get_schema(self, asset: str) -> Dict[str, Any]:
        """
        Retrieves the schema for a given table (asset) in SQLite.
        """
        if not self._connection:
            self.connect()

        try:
            inspector = inspect(self._engine)
            columns = inspector.get_columns(asset) # SQLite doesn't use schema for get_columns
            
            schema_dict = {
                "asset_name": asset,
                "columns": [
                    {
                        "name": col["name"],
                        "type": str(col["type"]),
                        "nullable": col["nullable"],
                        "default": col.get("default"),
                        "primary_key": col.get("primary_key", False)
                    } for col in columns
                ]
            }
            logger.debug(f"Retrieved schema for '{asset}': {schema_dict}")
            return schema_dict
        except Exception as e:
            raise SchemaDiscoveryError(f"Failed to get schema for asset '{asset}': {e}") from e

    def read_batch(
        self, 
        asset: str, 
        limit: Optional[int] = None, 
        offset: Optional[int] = None,
        **kwargs
    ) -> Iterator[pd.DataFrame]:
        """
        Reads data from a SQLite table in batches.
        """
        if not self._connection:
            self.connect()

        try:
            query = f'SELECT * FROM "{asset}"'
            
            if limit is not None:
                query += f" LIMIT {limit}"
            if offset is not None:
                query += f" OFFSET {offset}"
            
            logger.info(f"Executing query: {query}")

            chunksize = kwargs.pop("chunksize", 10000)
            
            iterator = pd.read_sql_query(
                sql=text(query), 
                con=self._connection, 
                chunksize=chunksize, 
                coerce_float=True, 
                **kwargs
            )
            
            for chunk in iterator:
                logger.info(f"Read chunk with {len(chunk)} rows")
                yield chunk

        except Exception as e:
            raise DataTransferError(f"Failed to read data from '{asset}': {e}") from e

    def write_batch(
        self, 
        data: Union[pd.DataFrame, Iterator[pd.DataFrame]], 
        asset: str, 
        mode: str = "append",
        **kwargs
    ) -> int:
        """
        Writes data to a SQLite table.
        """
        if not self._connection:
            self.connect()

        total_rows_written = 0
        
        if isinstance(data, pd.DataFrame):
            data_iterator = [data]
        else:
            data_iterator = data

        try:
            for df_chunk in data_iterator:
                if df_chunk.empty:
                    continue

                df_chunk.to_sql(
                    name=asset,
                    con=self._connection,
                    if_exists=mode, # 'append', 'replace', 'fail'
                    index=False,
                    **kwargs
                )
                total_rows_written += len(df_chunk)
            
            if mode == "replace":
                logger.info(f"Table '{asset}' replaced and {total_rows_written} rows written.")
            elif mode == "append":
                logger.info(f"{total_rows_written} rows appended to '{asset}'.")
            
            return total_rows_written

        except Exception as e:
            raise DataTransferError(f"Failed to write data to '{asset}' in mode '{mode}': {e}") from e
