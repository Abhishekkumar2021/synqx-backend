from typing import Any, Dict, List, Optional, Iterator, Union
import os
import glob
import pandas as pd
from datetime import datetime
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from app.connectors.base import BaseConnector
from app.core.errors import ConfigurationError, SchemaDiscoveryError, DataTransferError
from app.core.logging import get_logger

logger = get_logger(__name__)

class LocalFileConfig(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", case_sensitive=False)
    base_path: str = Field(..., description="Base directory for files")

class LocalFileConnector(BaseConnector):
    """
    Robust Connector for Local Filesystem.
    """

    def __init__(self, config: Dict[str, Any]):
        self._config_model: Optional[LocalFileConfig] = None
        super().__init__(config)

    def validate_config(self):
        try:
            self._config_model = LocalFileConfig.model_validate(self.config)
            # We don't force create the directory here anymore to avoid Errno 30 on read-only systems
            # during simple validation/discovery. We only check if it's a valid string.
            if not self._config_model.base_path:
                raise ValueError("base_path cannot be empty")
        except Exception as e:
            raise ConfigurationError(f"Invalid LocalFile configuration: {e}")

    def connect(self) -> None:
        pass

    def disconnect(self) -> None:
        pass

    def test_connection(self) -> bool:
        return os.path.isdir(self._config_model.base_path)

    def _get_full_path(self, asset: str) -> str:
        # Prevent path traversal
        clean_asset = os.path.basename(asset) if "/" not in asset else asset
        path = os.path.join(self._config_model.base_path, clean_asset)
        return os.path.abspath(path)

    def discover_assets(
        self, pattern: Optional[str] = None, include_metadata: bool = False, **kwargs
    ) -> List[Dict[str, Any]]:
        base = self._config_model.base_path
        search_pattern = pattern or "*"
        
        # Use glob for discovery
        files = []
        try:
            full_pattern = os.path.join(base, search_pattern)
            for f in glob.glob(full_pattern, recursive=True):
                if os.path.isfile(f):
                    rel_path = os.path.relpath(f, base)
                    if not include_metadata:
                        files.append({
                            "name": os.path.basename(f), 
                            "fully_qualified_name": rel_path,
                            "type": "file"
                        })
                    else:
                        stat = os.stat(f)
                        files.append({
                            "name": os.path.basename(f),
                            "fully_qualified_name": rel_path,
                            "type": "file",
                            "size_bytes": stat.st_size,
                            "last_modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
                            "format": rel_path.split('.')[-1] if '.' in rel_path else 'unknown'
                        })
            return files
        except Exception as e:
            raise DataTransferError(f"Failed to discover local files: {e}")

    def infer_schema(self, asset: str, sample_size: int = 1000, **kwargs) -> Dict[str, Any]:
        try:
            df_iter = self.read_batch(asset, limit=sample_size)
            df = next(df_iter)
            return {
                "asset": asset,
                "columns": [{"name": col, "type": str(dtype)} for col, dtype in df.dtypes.items()],
                "format": asset.split('.')[-1]
            }
        except Exception as e:
            raise SchemaDiscoveryError(f"Failed to infer schema for {asset}: {e}")

    def read_batch(
        self, asset: str, limit: Optional[int] = None, offset: Optional[int] = None, **kwargs
    ) -> Iterator[pd.DataFrame]:
        path = self._get_full_path(asset)
        fmt = asset.split('.')[-1].lower()
        incremental_filter = kwargs.get("incremental_filter")
        
        if not os.path.exists(path):
            raise DataTransferError(f"File not found: {path}")

        try:
            df_iter: Iterator[pd.DataFrame]
            if fmt == 'csv':
                # For CSV, we can't easily filter before reading without scanning. 
                # We read in chunks to manage memory, then filter.
                chunksize = kwargs.get("chunksize", 10000)
                df_iter = pd.read_csv(path, chunksize=chunksize, skiprows=range(1, offset + 1) if offset else None)
            elif fmt == 'parquet':
                df = pd.read_parquet(path)
                df_iter = iter([self.slice_dataframe(df, offset, None)]) # Limit handled after filter
            elif fmt in ('json', 'jsonl'):
                df = pd.read_json(path, lines=(fmt == 'jsonl'))
                df_iter = iter([self.slice_dataframe(df, offset, None)])
            elif fmt in ('xls', 'xlsx'):
                df = pd.read_excel(path)
                df_iter = iter([self.slice_dataframe(df, offset, None)])
            else:
                raise DataTransferError(f"Unsupported local file format: {fmt}")

            # Apply Incremental Filter and Limit
            rows_yielded = 0
            for df in df_iter:
                if incremental_filter and isinstance(incremental_filter, dict):
                    for col, val in incremental_filter.items():
                        if col in df.columns:
                            # Apply generic "greater than" logic for incremental load
                            df = df[df[col] > val]
                
                if df.empty:
                    continue
                
                # Apply limit if it was passed (and not handled by read_csv natively for chunks)
                if limit:
                    remaining = limit - rows_yielded
                    if remaining <= 0:
                        break
                    if len(df) > remaining:
                        df = df.iloc[:remaining]
                
                rows_yielded += len(df)
                yield df

        except Exception as e:
            raise DataTransferError(f"Error reading local file {asset}: {e}")

    def write_batch(
        self, data: Union[pd.DataFrame, Iterator[pd.DataFrame]], asset: str, mode: str = "append", **kwargs
    ) -> int:
        path = self._get_full_path(asset)
        fmt = asset.split('.')[-1].lower()
        
        # Normalize mode
        clean_mode = mode.lower()
        if clean_mode == "replace": clean_mode = "overwrite"
        
        # Ensure parent directory exists before writing
        os.makedirs(os.path.dirname(path), exist_ok=True)

        if isinstance(data, pd.DataFrame):
            data_iter = [data]
        else:
            data_iter = data

        total = 0
        try:
            first = True
            for df in data_iter:
                if df.empty: continue
                
                if clean_mode == "upsert":
                    logger.warning(f"Upsert requested for file {asset} but not supported. Falling back to append.")

                write_mode = 'w' if (first and clean_mode == 'overwrite') or not os.path.exists(path) else 'a'
                header = True if write_mode == 'w' or not os.path.exists(path) else False
                
                if fmt == 'csv':
                    df.to_csv(path, index=False, mode=write_mode, header=header)
                elif fmt == 'parquet':
                    # Parquet doesn't support 'a' mode directly in to_parquet
                    df.to_parquet(path, index=False)
                elif fmt in ('json', 'jsonl'):
                    df.to_json(path, orient='records', lines=(fmt == 'jsonl'), mode=write_mode)
                
                total += len(df)
                first = False
            return total
        except Exception as e:
            raise DataTransferError(f"Error writing to local file {asset}: {e}")

    def execute_query(
        self,
        query: str,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError("Query execution is not supported for Local File connector.")