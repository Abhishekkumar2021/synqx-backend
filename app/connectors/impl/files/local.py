from typing import Any, Dict, Iterator, List, Optional, Union
import os
import glob
import pandas as pd
from pydantic import Field
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

class LocalFileConfig(BaseSettings):
    """
    Configuration model for Local File connector.
    """
    model_config = SettingsConfigDict(extra='ignore', case_sensitive=False)

    base_path: str = Field(..., description="Base directory path for file operations")

class LocalFileConnector(BaseConnector):
    """
    Connector for reading/writing files on the local filesystem.
    Supports CSV, JSON, and Parquet via Pandas.
    """

    def __init__(self, config: Dict[str, Any]):
        self._config_model: Optional[LocalFileConfig] = None
        super().__init__(config)

    def validate_config(self) -> None:
        try:
            self._config_model = LocalFileConfig.model_validate(self.config)
            if not os.path.exists(self._config_model.base_path):
                os.makedirs(self._config_model.base_path, exist_ok=True)
        except Exception as e:
            raise ConfigurationError(f"Invalid Local File configuration: {e}") from e

    def connect(self) -> None:
        if not os.path.exists(self._config_model.base_path):
             raise ConnectionFailedError(f"Base path does not exist: {self._config_model.base_path}")
        logger.info(f"Connected to local filesystem at {self._config_model.base_path}")

    def disconnect(self) -> None:
        pass

    def test_connection(self) -> bool:
        try:
            return os.path.isdir(self._config_model.base_path) and os.access(self._config_model.base_path, os.W_OK)
        except Exception:
            return False

    def list_assets(self, pattern: str = "*") -> List[str]:
        """
        Lists files in the base path matching a glob pattern.
        """
        try:
            full_pattern = os.path.join(self._config_model.base_path, pattern)
            files = [os.path.relpath(f, self._config_model.base_path) for f in glob.glob(full_pattern, recursive=True) if os.path.isfile(f)]
            return files
        except Exception as e:
            raise SchemaDiscoveryError(f"Failed to list files with pattern '{pattern}': {e}") from e

    def get_schema(self, asset: str) -> Dict[str, Any]:
        """
        Infers schema from the file using Pandas. Reads only a sample.
        """
        file_path = os.path.join(self._config_model.base_path, asset)
        if not os.path.exists(file_path):
            raise SchemaDiscoveryError(f"File not found: {asset}")

        try:
            if asset.endswith(".csv"):
                df = pd.read_csv(file_path, nrows=10)
            elif asset.endswith(".json"):
                # For JSON, try lines=True first, then fallback to normal
                try:
                    df = pd.read_json(file_path, lines=True, nrows=10)
                except ValueError: # If it's not JSON lines
                    df = pd.read_json(file_path, nrows=10)
            elif asset.endswith(".parquet"):
                df = pd.read_parquet(file_path) # Parquet usually reads schema fast
            else:
                raise SchemaDiscoveryError(f"Unsupported file extension for schema discovery: {asset}")

            return {
                "asset_name": asset,
                "columns": [
                    {"name": col, "type": str(dtype)}
                    for col, dtype in df.dtypes.items()
                ]
            }
        except Exception as e:
            raise SchemaDiscoveryError(f"Failed to infer schema for {asset}: {e}") from e

    def read_batch(
        self, 
        asset: str, 
        limit: Optional[int] = None, 
        offset: Optional[int] = None,
        **kwargs
    ) -> Iterator[pd.DataFrame]:
        file_path = os.path.join(self._config_model.base_path, asset)
        if not os.path.exists(file_path):
            raise DataTransferError(f"File not found: {file_path}")

        chunksize = kwargs.pop("chunksize", 10000)
        
        # Determine how to handle offset for initial read.
        # Pandas read_csv skiprows handles data rows, not header. So skip header + offset.
        read_kwargs = {**kwargs} # Copy to avoid modifying original kwargs

        try:
            if asset.endswith(".csv"):
                # For CSV, skip initial header row if exists, then handle offset rows
                # Pandas 'skiprows' param for read_csv takes an integer or a list-like.
                # If int, skips that many rows at the beginning. If list-like, skips rows at specified indices.
                # If header is row 0, and we want to skip 'offset' data rows, we skip header (1 row) + offset rows.
                
                # Careful with header handling if no header, or header=False
                header = read_kwargs.get('header', 'infer')
                effective_skip_rows = 0
                if header is not None and header is not False: # Assuming header exists
                    effective_skip_rows += 1 # Account for header row
                
                if offset:
                    effective_skip_rows += offset

                # nrows applies to data rows AFTER skiprows
                if limit:
                    read_kwargs['nrows'] = limit
                
                # Read in chunks
                read_kwargs['chunksize'] = chunksize

                # pandas read_csv (with iterator=True) handles skiprows and nrows.
                # However, skiprows (int) skips rows *from the start*.
                # If header=0, and skiprows=1, the header is skipped. If we also want to skip 1 data row (offset),
                # we need skiprows=2.
                # The 'iterator' mode with chunksize starts yielding dataframes after skipping.
                
                # Best way to handle header and offset with read_csv:
                # Read header separately if needed, then read data. Or let pandas handle header.
                # If skiprows is an int, it skips that many *lines* from the start.
                # If header=0 (default) and skiprows=1, the header is skipped.
                # So if offset means skipping data rows, we need to add 1 for the header.
                
                # Let's read all and then chunk for simplicity if pandas itself doesn't offer easy offset for chunks.
                # For large files, this would not be ideal.
                # Simplification: For now, if offset is used with CSV, read all into memory then chunk.
                # This is a known limitation of direct pandas chunking + offset.
                if offset:
                    logger.warning("Offset used with CSV, reading entire file into memory before chunking. Performance may be affected.")
                    df = pd.read_csv(file_path, **read_kwargs)
                    df = df.iloc[offset:]
                    if limit:
                        df = df.iloc[:limit]
                    
                    for i in range(0, len(df), chunksize):
                        yield df.iloc[i:i+chunksize]
                else:
                    for chunk in pd.read_csv(file_path, **read_kwargs):
                        yield chunk

            elif asset.endswith(".json"):
                # If it's JSON lines, use lines=True
                if read_kwargs.get('lines', False):
                    # For json lines, read_json with chunksize/nrows/skiprows not directly supported.
                    # Load all and chunk manually or use an explicit streaming parser.
                    # For simplicity, load all into DF then chunk.
                    df = pd.read_json(file_path, lines=True, **read_kwargs)
                    if offset:
                        df = df.iloc[offset:]
                    if limit:
                        df = df.iloc[:limit]
                    for i in range(0, len(df), chunksize):
                        yield df.iloc[i:i+chunksize]
                else:
                    # Assume single JSON object/array, load all.
                    logger.warning("Reading entire JSON file into memory as lines=False. Performance may be affected.")
                    df = pd.read_json(file_path, **read_kwargs)
                    if offset:
                        df = df.iloc[offset:]
                    if limit:
                        df = df.iloc[:limit]
                    for i in range(0, len(df), chunksize):
                        yield df.iloc[i:i+chunksize]

            elif asset.endswith(".parquet"):
                # Pandas read_parquet returns full DF usually.
                # For simplicity in this MVP, load all and chunk.
                logger.warning("Reading entire Parquet file into memory before chunking. Performance may be affected.")
                df = pd.read_parquet(file_path, **read_kwargs)
                if offset:
                    df = df.iloc[offset:]
                if limit:
                    df = df.iloc[:limit]
                
                for i in range(0, len(df), chunksize):
                    yield df.iloc[i:i+chunksize]
            else:
                raise DataTransferError(f"Unsupported read format: {asset}")

        except Exception as e:
            raise DataTransferError(f"Failed to read file {asset}: {e}") from e

    def write_batch(
        self, 
        data: Union[pd.DataFrame, Iterator[pd.DataFrame]], 
        asset: str, 
        mode: str = "append",
        **kwargs
    ) -> int:
        file_path = os.path.join(self._config_model.base_path, asset)
        total_rows = 0
        
        # Convert single DF to iterator
        if isinstance(data, pd.DataFrame):
            data_iterator = [data]
        else:
            data_iterator = data

        first_chunk = True
        
        # Determine format from extension
        format_type = "csv"
        if asset.endswith(".json"):
            format_type = "json"
        elif asset.endswith(".parquet"):
            format_type = "parquet"

        # Handle "replace" mode: delete file if exists before writing first chunk
        if mode == "replace" and os.path.exists(file_path):
            try:
                os.remove(file_path)
                logger.info(f"Deleted existing file {file_path} for replace mode.")
            except OSError as e:
                raise DataTransferError(f"Failed to delete existing file {file_path}: {e}")

        try:
            for df_chunk in data_iterator:
                if df_chunk.empty:
                    continue

                # Determine if we write header
                # Write header only if it's the first chunk AND the file doesn't exist (or we just deleted it)
                # If appending to existing file, no header.
                file_exists_before_write = os.path.exists(file_path)
                write_header = not file_exists_before_write or (mode == 'replace' and first_chunk)


                if format_type == "csv":
                    df_chunk.to_csv(
                        file_path, 
                        mode='a', # Always append in loop, logic above handled replace
                        header=write_header, 
                        index=False,
                        **kwargs
                    )
                elif format_type == "json":
                    # JSON requires lines=True for appending.
                    # Pandas to_json(lines=True) writes one JSON object per line.
                    df_chunk.to_json(
                        file_path, 
                        orient='records', 
                        lines=True, 
                        mode='a', 
                        **kwargs
                    )
                elif format_type == "parquet":
                    # Parquet appending is tricky with pandas.to_parquet. 
                    # Use fastparquet's append mode.
                    if not file_exists_before_write or (mode == 'replace' and first_chunk):
                        df_chunk.to_parquet(file_path, index=False, **kwargs)
                    else:
                        # Ensure fastparquet is installed for append=True
                        try:
                            df_chunk.to_parquet(file_path, engine='fastparquet', append=True, index=False, **kwargs)
                        except Exception as e:
                            logger.warning(f"Fastparquet append failed or not supported, falling back to read-concat-write for parquet: {e}")
                            # Fallback: Read existing, concat, write back (inefficient but works)
                            existing_df = pd.read_parquet(file_path)
                            combined_df = pd.concat([existing_df, df_chunk])
                            combined_df.to_parquet(file_path, index=False, **kwargs)

                total_rows += len(df_chunk)
                first_chunk = False
            
            logger.info(f"Wrote {total_rows} rows to {file_path}")
            return total_rows

        except Exception as e:
            raise DataTransferError(f"Failed to write to file {asset}: {e}") from e