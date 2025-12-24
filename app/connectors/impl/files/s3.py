from typing import Any, Dict, List, Optional, Iterator, Union
import pandas as pd
import s3fs
from app.connectors.base import BaseConnector
from app.core.errors import ConfigurationError, ConnectionFailedError, DataTransferError
from app.core.logging import get_logger
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = get_logger(__name__)

class S3Config(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", case_sensitive=False)
    
    bucket: str = Field(..., description="S3 Bucket Name")
    region_name: str = Field("us-east-1", description="AWS Region")
    aws_access_key_id: Optional[str] = Field(None, description="AWS Access Key ID")
    aws_secret_access_key: Optional[str] = Field(None, description="AWS Secret Access Key")
    endpoint_url: Optional[str] = Field(None, description="Custom Endpoint URL (e.g. for MinIO)")

class S3Connector(BaseConnector):
    """
    Robust Connector for Amazon S3 (and compatible storages like MinIO).
    """

    def __init__(self, config: Dict[str, Any]):
        self._config_model: Optional[S3Config] = None
        self._fs: Optional[s3fs.S3FileSystem] = None
        super().__init__(config)

    def validate_config(self) -> None:
        try:
            self._config_model = S3Config.model_validate(self.config)
        except Exception as e:
            raise ConfigurationError(f"Invalid S3 configuration: {e}")

    def connect(self) -> None:
        if self._fs:
            return
        
        try:
            client_kwargs = {}
            if self._config_model.endpoint_url:
                client_kwargs['endpoint_url'] = self._config_model.endpoint_url

            self._fs = s3fs.S3FileSystem(
                key=self._config_model.aws_access_key_id,
                secret=self._config_model.aws_secret_access_key,
                client_kwargs=client_kwargs,
                config_kwargs={'retries': {'max_attempts': 5}}
            )
            # Verify bucket accessibility
            if not self._fs.exists(self._config_model.bucket):
                raise ConnectionFailedError(f"Bucket '{self._config_model.bucket}' not found or inaccessible.")
                
        except Exception as e:
            raise ConnectionFailedError(f"Failed to connect to S3: {e}")

    def disconnect(self) -> None:
        self._fs = None

    def test_connection(self) -> bool:
        try:
            self.connect()
            self._fs.ls(self._config_model.bucket, detail=False)
            return True
        except Exception:
            return False

    def discover_assets(
        self, pattern: Optional[str] = None, include_metadata: bool = False, **kwargs
    ) -> List[Dict[str, Any]]:
        self.connect()
        try:
            # Recursive listing with optimized filter
            files = self._fs.glob(f"{self._config_model.bucket}/**")
            valid_extensions = ('.csv', '.parquet', '.json', '.jsonl', '.avro')
            
            assets = []
            for f in files:
                if f.endswith(valid_extensions):
                    name = f.replace(f"{self._config_model.bucket}/", "")
                    if pattern and pattern.lower() not in name.lower():
                        continue
                    
                    if not include_metadata:
                        assets.append({"name": name, "type": "file"})
                    else:
                        info = self._fs.info(f)
                        assets.append({
                            "name": name,
                            "type": "file",
                            "size_bytes": info.get('size'),
                            "last_modified": str(info.get('LastModified')),
                            "format": name.split('.')[-1]
                        })
            return assets
        except Exception as e:
            raise DataTransferError(f"Failed to discover S3 assets: {e}")

    def infer_schema(self, asset: str, sample_size: int = 1000, **kwargs) -> Dict[str, Any]:
        self.connect()
        try:
            df_iter = self.read_batch(asset, limit=sample_size)
            sample_df = next(df_iter)
            return {
                "asset": asset,
                "columns": [{"name": col, "type": str(dtype)} for col, dtype in sample_df.dtypes.items()],
                "format": asset.split('.')[-1]
            }
        except Exception as e:
            raise SchemaDiscoveryError(f"S3 schema inference failed for {asset}: {e}")

    def read_batch(
        self, asset: str, limit: Optional[int] = None, offset: Optional[int] = None, **kwargs
    ) -> Iterator[pd.DataFrame]:
        self.connect()
        path = f"s3://{self._config_model.bucket}/{asset}"
        fmt = asset.split('.')[-1].lower()
        incremental_filter = kwargs.get("incremental_filter")
        
        storage_options = {
            "key": self._config_model.aws_access_key_id,
            "secret": self._config_model.aws_secret_access_key,
            "client_kwargs": {"endpoint_url": self._config_model.endpoint_url} if self._config_model.endpoint_url else {}
        }

        try:
            df_iter: Iterator[pd.DataFrame]
            if fmt == 'csv':
                # Read in chunks to support filtering without OOM
                chunksize = kwargs.get("chunksize", 10000)
                df_iter = pd.read_csv(path, storage_options=storage_options, chunksize=chunksize, skiprows=range(1, offset + 1) if offset else None)
            elif fmt == 'parquet':
                df = pd.read_parquet(path, storage_options=storage_options)
                df_iter = iter([self.slice_dataframe(df, offset, None)])
            elif fmt in ('json', 'jsonl'):
                df = pd.read_json(path, storage_options=storage_options, lines=(fmt == 'jsonl'))
                df_iter = iter([self.slice_dataframe(df, offset, None)])
            else:
                raise DataTransferError(f"Unsupported S3 file format: {fmt}")

            rows_yielded = 0
            for df in df_iter:
                # Apply Incremental Filter
                if incremental_filter and isinstance(incremental_filter, dict):
                    for col, val in incremental_filter.items():
                        if col in df.columns:
                            df = df[df[col] > val]
                
                if df.empty:
                    continue

                # Apply limit
                if limit:
                    remaining = limit - rows_yielded
                    if remaining <= 0:
                        break
                    if len(df) > remaining:
                        df = df.iloc[:remaining]
                
                rows_yielded += len(df)
                yield df

        except Exception as e:
            raise DataTransferError(f"S3 read failed for {asset}: {e}")

    def write_batch(
        self, data: Union[pd.DataFrame, Iterator[pd.DataFrame]], asset: str, mode: str = "append", **kwargs
    ) -> int:
        self.connect()
        path = f"s3://{self._config_model.bucket}/{asset}"
        fmt = asset.split('.')[-1].lower()
        
        storage_options = {
            "key": self._config_model.aws_access_key_id,
            "secret": self._config_model.aws_secret_access_key,
            "client_kwargs": {"endpoint_url": self._config_model.endpoint_url} if self._config_model.endpoint_url else {}
        }

        if isinstance(data, pd.DataFrame):
            data_iter = [data]
        else:
            data_iter = data

        total = 0
        try:
            # Handle append/replace logic
            if mode == 'replace' or not self._fs.exists(path):
                first_df = next(data_iter)
                self._write_df(first_df, path, fmt, storage_options, 'w')
                total += len(first_df)
            
            for df in data_iter:
                self._write_df(df, path, fmt, storage_options, 'a')
                total += len(df)
            return total
        except Exception as e:
            raise DataTransferError(f"S3 write failed for {asset}: {e}")

    def _write_df(self, df: pd.DataFrame, path: str, fmt: str, options: dict, mode: str):
        if fmt == 'csv':
            df.to_csv(path, index=False, storage_options=options, mode=mode, header=(mode == 'w'))
        elif fmt == 'parquet':
            # Parquet append is complex on S3, usually involves multiple files
            df.to_parquet(path, index=False, storage_options=options)
        elif fmt in ('json', 'jsonl'):
            df.to_json(path, orient='records', lines=(fmt == 'jsonl'), storage_options=options)

    def execute_query(
        self,
        query: str,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError("Query execution is not supported for S3 connector.")