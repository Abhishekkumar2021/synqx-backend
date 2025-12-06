from typing import Any, Dict, Iterator, List, Optional, Union
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.engine import Connection, Engine
from pydantic import Field, PostgresDsn
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
    """
    Configuration model for PostgreSQL connector.
    """
    model_config = SettingsConfigDict(extra='ignore', case_sensitive=False)

    dsn: PostgresDsn = Field(..., description="PostgreSQL connection DSN")
    schema: str = Field("public", description="Default schema to use")

class PostgresConnector(BaseConnector):
    """
    A concrete implementation of BaseConnector for PostgreSQL databases.
    """

    def __init__(self, config: Dict[str, Any]):
        self._config_model: Optional[PostgresConfig] = None
        self._engine: Optional[Engine] = None
        self._connection: Optional[Connection] = None
        super().__init__(config) # This calls validate_config

    def validate_config(self) -> None:
        """
        Validates the configuration using Pydantic.
        """
        try:
            self._config_model = PostgresConfig.model_validate(self.config)
        except Exception as e:
            raise ConfigurationError(f"Invalid PostgreSQL configuration: {e}") from e
        
        # Ensure DSN is properly set up for SQLAlchemy
        self.config["dsn"] = str(self._config_model.dsn)


    def _get_sqlalchemy_url(self) -> str:
        """Constructs the SQLAlchemy connection URL."""
        if not self._config_model:
            raise ConfigurationError("Configuration not validated.")
        
        # Pydantic's PostgresDsn automatically handles user, password, host, port, dbname
        # We just need to make sure the driver is psycopg2
        url = str(self._config_model.dsn)
        if not url.startswith("postgresql+psycopg2://"):
            url = url.replace("postgresql://", "postgresql+psycopg2://")
        return url

    def connect(self) -> None:
        """
        Establishes a connection to the PostgreSQL database.
        """
        if self._connection and not self._connection.closed:
            logger.debug("PostgreSQL connection already open.")
            return

        try:
            self._engine = create_engine(self._get_sqlalchemy_url())
            self._connection = self._engine.connect()
            self._connection.execution_options(autocommit=True)
            logger.info("Successfully connected to PostgreSQL.")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            if "authentication" in str(e).lower() or "password" in str(e).lower():
                raise AuthenticationError(f"PostgreSQL authentication failed: {e}") from e
            raise ConnectionFailedError(f"PostgreSQL connection failed: {e}") from e

    def disconnect(self) -> None:
        """
        Closes the PostgreSQL database connection and disposes the engine.
        """
        if self._connection:
            try:
                self._connection.close()
                logger.info("PostgreSQL connection closed.")
            except Exception as e:
                logger.warning(f"Error closing PostgreSQL connection: {e}")
            finally:
                self._connection = None
        
        if self._engine:
            try:
                self._engine.dispose()
                logger.info("PostgreSQL engine disposed.")
            except Exception as e:
                logger.warning(f"Error disposing PostgreSQL engine: {e}")
            finally:
                self._engine = None


    def test_connection(self) -> bool:
        """
        Tests if the PostgreSQL connection can be established.
        """
        try:
            with self.session(): # Use the context manager for testing
                # If connect() and disconnect() work, it's considered successful
                return True
        except (ConnectionFailedError, AuthenticationError) as e:
            logger.error(f"PostgreSQL connection test failed: {e}")
            return False
        except Exception as e:
            logger.error(f"An unexpected error occurred during PostgreSQL connection test: {e}")
            return False

    def list_assets(self) -> List[str]:
        """
        Lists all tables in the configured schema.
        """
        if not self._connection:
            self.connect() # Ensure connection is open

        try:
            inspector = inspect(self._engine)
            schema = self._config_model.schema
            tables = inspector.get_table_names(schema=schema)
            logger.debug(f"Found tables in schema '{schema}': {tables}")
            return tables
        except Exception as e:
            raise SchemaDiscoveryError(f"Failed to list assets in schema '{schema}': {e}") from e

    def get_schema(self, asset: str) -> Dict[str, Any]:
        """
        Retrieves the schema for a given table (asset).
        """
        if not self._connection:
            self.connect() # Ensure connection is open

        try:
            inspector = inspect(self._engine)
            schema = self._config_model.schema
            columns = inspector.get_columns(asset, schema=schema)
            
            schema_dict = {
                "asset_name": asset,
                "schema": schema,
                "columns": [
                    {
                        "name": col["name"],
                        "type": str(col["type"]),  # Convert SQLAlchemy type to string
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
        Reads data from a PostgreSQL table in batches.
        """
        if not self._connection:
            self.connect() # Ensure connection is open

        try:
            schema = self._config_model.schema
            query = f'SELECT * FROM "{schema}"."{asset}"'
            
            if limit is not None:
                query += f" LIMIT {limit}"
            if offset is not None:
                query += f" OFFSET {offset}"
            
            logger.info(f"Executing query: {query}")

            # For large queries, read in chunks using an iterator
            chunksize = kwargs.pop("chunksize", 10000) # Default chunk size
            
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
        Writes data to a PostgreSQL table.
        """
        if not self._connection:
            self.connect() # Ensure connection is open

        total_rows_written = 0
        schema = self._config_model.schema

        if isinstance(data, pd.DataFrame):
            data_iterator = [data]
        else:
            data_iterator = data

        try:
            for df_chunk in data_iterator:
                # Ensure connection is fresh for each chunk if necessary
                # to_sql can take a connection string or an engine/connection object
                # Using the raw connection due to its autocommit=True setup
                rows_before = 0
                if mode == "append":
                    # Check existing rows if needed, though to_sql in append doesn't return count directly
                    pass # df.to_sql handles this

                df_chunk.to_sql(
                    name=asset,
                    con=self._connection,
                    schema=schema,
                    if_exists=mode, # 'append', 'replace', 'fail'
                    index=False,
                    **kwargs
                )
                total_rows_written += len(df_chunk)
            
            if mode == "replace":
                logger.info(f"Table '{schema}.{asset}' replaced and {total_rows_written} rows written.")
            elif mode == "append":
                logger.info(f"{total_rows_written} rows appended to '{schema}.{asset}'.")
            
            return total_rows_written

        except Exception as e:
            raise DataTransferError(f"Failed to write data to '{asset}' in mode '{mode}': {e}") from e

