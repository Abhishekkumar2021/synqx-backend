from typing import Any, Dict, List, Optional, Iterator, Union
import pandas as pd
import boto3
import s3fs
from app.connectors.base import BaseConnector
from app.core.errors import ConfigurationError, ConnectionFailedError, DataTransferError
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class S3Config(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", case_sensitive=False)
    
    bucket: str = Field(..., description="S3 Bucket Name")
    region_name: str = Field("us-east-1", description="AWS Region")
    aws_access_key_id: Optional[str] = Field(None, description="AWS Access Key ID")
    aws_secret_access_key: Optional[str] = Field(None, description="AWS Secret Access Key")
    endpoint_url: Optional[str] = Field(None, description="Custom Endpoint URL (e.g. for MinIO)")


class S3Connector(BaseConnector):
    """
    Connector for Amazon S3 (and compatible storages like MinIO).
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
            )
            # Verify bucket existence
            if not self._fs.exists(self._config_model.bucket):
                raise ConnectionFailedError(f"Bucket '{self._config_model.bucket}' does not exist or is not accessible.")
                
        except Exception as e:
            raise ConnectionFailedError(f"Failed to connect to S3: {e}")

    def disconnect(self) -> None:
        self._fs = None

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
            # List objects in the bucket
            files = self._fs.glob(f"{self._config_model.bucket}/**")
            # Filter for common data formats
            valid_extensions = ('.csv', '.parquet', '.json', '.xlsx')
            files = [f for f in files if f.endswith(valid_extensions)]
            
            # Remove bucket prefix for cleaner asset names
            assets = [f.replace(f"{self._config_model.bucket}/", "") for f in files]

            if pattern:
                assets = [a for a in assets if pattern.lower() in a.lower()]

            if not include_metadata:
                return [{"name": a, "type": "file"} for a in assets]

            enriched = []
            for asset in assets:
                full_path = f"{self._config_model.bucket}/{asset}"
                info = self._fs.info(full_path)
                enriched.append({
                    "name": asset,
                    "type": "file",
                    "size_bytes": info.get('size'),
                    "last_modified": str(info.get('LastModified')),
                    "path": full_path
                })
            return enriched

        except Exception as e:
            raise ConnectionFailedError(f"Failed to discover assets in S3: {e}")

    def infer_schema(
        self,
        asset: str,
        sample_size: int = 1000,
        mode: str = "auto",
        **kwargs,
    ) -> Dict[str, Any]:
        
        self.connect()
        try:
            # Read a small sample
            df_iter = self.read_batch(asset, limit=sample_size)
            sample_df = next(df_iter)
            
            columns = [
                {"name": col, "type": str(dtype)}
                for col, dtype in sample_df.dtypes.items()
            ]
            return {
                "asset": asset,
                "columns": columns,
                "format": self._get_file_format(asset)
            }
        except Exception as e:
            # Fallback or re-raise
            raise DataTransferError(f"Failed to infer schema for {asset}: {e}")

    def read_batch(
        self,
        asset: str,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        **kwargs,
    ) -> Iterator[pd.DataFrame]:
        
        self.connect()
        full_path = f"s3://{self._config_model.bucket}/{asset}"
        file_format = self._get_file_format(asset)

        try:
            if file_format == 'csv':
                # S3FS handles 's3://' paths automatically if integrated with pandas
                # But we initialized self._fs. For pandas read_*, we can pass storage_options.
                storage_options = {
                    "key": self._config_model.aws_access_key_id,
                    "secret": self._config_model.aws_secret_access_key,
                    "client_kwargs": {"endpoint_url": self._config_model.endpoint_url} if self._config_model.endpoint_url else {}
                }
                
                # Handling limit/offset for CSV is tricky without reading all. 
                # Pandas 'nrows' = limit. 'skiprows' = offset.
                read_kwargs = kwargs.copy()
                if limit:
                    read_kwargs['nrows'] = limit
                if offset:
                    read_kwargs['skiprows'] = range(1, offset + 1) # Skip rows but keep header (row 0)
                
                df = pd.read_csv(full_path, storage_options=storage_options, **read_kwargs)
                yield df

            elif file_format == 'parquet':
                 storage_options = {
                    "key": self._config_model.aws_access_key_id,
                    "secret": self._config_model.aws_secret_access_key,
                    "client_kwargs": {"endpoint_url": self._config_model.endpoint_url} if self._config_model.endpoint_url else {}
                }
                 # Parquet doesn't support skiprows easily efficiently without predicate pushdown or reading all.
                 # We'll read all and slice for now (not efficient for huge files, but standard for 'pandas' engine).
                 df = pd.read_parquet(full_path, storage_options=storage_options)
                 df = self.slice_dataframe(df, offset, limit)
                 yield df
                 
            elif file_format == 'json':
                storage_options = {
                    "key": self._config_model.aws_access_key_id,
                    "secret": self._config_model.aws_secret_access_key,
                    "client_kwargs": {"endpoint_url": self._config_model.endpoint_url} if self._config_model.endpoint_url else {}
                }
                df = pd.read_json(full_path, storage_options=storage_options, **kwargs)
                df = self.slice_dataframe(df, offset, limit)
                yield df

            else:
                raise DataTransferError(f"Unsupported file format: {file_format}")

        except Exception as e:
            raise DataTransferError(f"Error reading {asset}: {e}")

    def write_batch(
        self,
        data: Union[pd.DataFrame, Iterator[pd.DataFrame]],
        asset: str,
        mode: str = "append",
        **kwargs,
    ) -> int:
        
        self.connect()
        full_path = f"s3://{self._config_model.bucket}/{asset}"
        file_format = self._get_file_format(asset)
        
        storage_options = {
            "key": self._config_model.aws_access_key_id,
            "secret": self._config_model.aws_secret_access_key,
            "client_kwargs": {"endpoint_url": self._config_model.endpoint_url} if self._config_model.endpoint_url else {}
        }

        # Handling "append" for files is complex (except CSV). 
        # S3 is object storage (immutable objects). "Append" usually means "read old + concat + write new" or "write new file".
        # For simplicity, we'll assume "append" implies we are writing a *new part* or overwriting if it's a single file.
        # But standard ETL 'append' to a file asset usually means adding rows.
        
        if mode == "append" and self._fs.exists(full_path):
             # Basic implementation: Read existing, concat, write back.
             # WARNING: Not concurrency safe and slow for big files.
             try:
                existing_iter = self.read_batch(asset)
                existing_df = next(existing_iter)
             except Exception:
                 existing_df = pd.DataFrame()
        else:
            existing_df = pd.DataFrame()

        total_written = 0
        
        if isinstance(data, pd.DataFrame):
            iterator = [data]
        else:
            iterator = data
            
        final_df = existing_df
        
        for df in iterator:
            final_df = pd.concat([final_df, df], ignore_index=True)
            total_written += len(df)
            
        try:
            if file_format == 'csv':
                final_df.to_csv(full_path, index=False, storage_options=storage_options)
            elif file_format == 'parquet':
                final_df.to_parquet(full_path, index=False, storage_options=storage_options)
            elif file_format == 'json':
                final_df.to_json(full_path, orient='records', storage_options=storage_options)
            else:
                 raise DataTransferError(f"Unsupported write format: {file_format}")
                 
            return total_written
            
        except Exception as e:
            raise DataTransferError(f"Error writing to {asset}: {e}")

    def _get_file_format(self, asset: str) -> str:
        if asset.endswith('.csv'): return 'csv'
        if asset.endswith('.parquet'): return 'parquet'
        if asset.endswith('.json'): return 'json'
        if asset.endswith('.xlsx'): return 'excel'
        return 'unknown'
