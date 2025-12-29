import os
import io
from typing import Any, Dict, Iterator, List, Optional, Union
import pandas as pd
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from app.connectors.base import BaseConnector
from app.core.errors import ConfigurationError, ConnectionFailedError, SchemaDiscoveryError
from app.core.logging import get_logger

try:
    from google.cloud import storage
    from google.oauth2 import service_account
except ImportError:
    storage = None
    service_account = None

logger = get_logger(__name__)

class GCSConfig(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", case_sensitive=False)
    
    bucket: str = Field(..., description="GCS Bucket Name")
    project_id: Optional[str] = Field(None, description="GCP Project ID")
    credentials_json: Optional[str] = Field(None, description="Service Account JSON")
    credentials_path: Optional[str] = Field(None, description="Path to Service Account JSON key file")
    recursive: bool = Field(True, description="Recursively search for files")
    max_depth: Optional[int] = Field(None, ge=0, description="Maximum depth for recursion")
    exclude_patterns: Optional[str] = Field(None, description="Comma-separated list of folders/files to exclude")

class GCSConnector(BaseConnector):
    def __init__(self, config: Dict[str, Any]):
        if storage is None:
            raise ConfigurationError("Google Cloud Storage client not installed. Run 'pip install google-cloud-storage'.")
        
        self._config_model: Optional[GCSConfig] = None
        self._client: Optional[storage.Client] = None
        super().__init__(config)

    def validate_config(self) -> None:
        try:
            self._config_model = GCSConfig.model_validate(self.config)
        except Exception as e:
            raise ConfigurationError(f"Invalid GCS configuration: {e}")

    def connect(self) -> None:
        if self._client:
            return
        
        try:
            if self._config_model.credentials_json:
                import json
                info = json.loads(self._config_model.credentials_json)
                credentials = service_account.Credentials.from_service_account_info(info)
                self._client = storage.Client(credentials=credentials, project=self._config_model.project_id)
            elif self._config_model.credentials_path:
                self._client = storage.Client.from_service_account_json(
                    self._config_model.credentials_path, 
                    project=self._config_model.project_id
                )
            else:
                # Default credentials (ADC)
                self._client = storage.Client(project=self._config_model.project_id)
            
            # Verify bucket access
            self._client.get_bucket(self._config_model.bucket)
        except Exception as e:
            raise ConnectionFailedError(f"Failed to connect to GCS: {e}")

    def disconnect(self) -> None:
        self._client = None

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
        
        is_recursive = self._config_model.recursive
        max_depth = self._config_model.max_depth

        # delimiter='/' emulates non-recursive behavior
        delimiter = None if is_recursive else "/"
        blobs = self._client.list_blobs(self._config_model.bucket, prefix=kwargs.get("prefix"), delimiter=delimiter)
        
        assets = []
        valid_extensions = {".csv", ".tsv", ".txt", ".xml", ".json", ".parquet", ".jsonl", ".avro", ".xls", ".xlsx"}
        max_assets = 10000
        
        ignored = {'.git', 'node_modules', '__pycache__', '.venv', 'venv'}
        if self._config_model.exclude_patterns:
            ignored.update({p.strip() for p in self._config_model.exclude_patterns.split(',') if p.strip()})

        for blob in blobs:
            if len(assets) >= max_assets:
                logger.warning(f"Reached max discovery limit of {max_assets} assets for bucket {self._config_model.bucket}")
                break

            # Depth check for GCS
            if max_depth is not None:
                # blob.name is relative to bucket root
                depth = len(blob.name.split('/')) - 1
                if depth > max_depth:
                    continue

            if pattern and pattern not in blob.name:
                continue
            
            # Exclude check
            if any(ig in blob.name for ig in ignored):
                continue

            ext = os.path.splitext(blob.name)[1].lower()
            if ext in valid_extensions:
                asset = {
                    "name": blob.name,
                    "fully_qualified_name": f"{self._config_model.bucket}/{blob.name}",
                    "type": "file",
                    "format": ext.replace(".", "")
                }
                if include_metadata:
                    asset["metadata"] = {
                        "size": blob.size,
                        "updated": blob.updated.isoformat(),
                        "content_type": blob.content_type
                    }
                assets.append(asset)
        return assets

    def infer_schema(self, asset: str, sample_size: int = 1000, **kwargs) -> Dict[str, Any]:
        self.connect()
        try:
            df_iter = self.read_batch(asset, limit=sample_size)
            df = next(df_iter)
            
            columns = []
            for col, dtype in df.dtypes.items():
                col_type = "string"
                dtype_str = str(dtype).lower()
                
                if "int" in dtype_str: col_type = "integer"
                elif "float" in dtype_str or "double" in dtype_str: col_type = "float"
                elif "bool" in dtype_str: col_type = "boolean"
                elif "datetime" in dtype_str: col_type = "datetime"
                elif "object" in dtype_str:
                    first_val = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                    if isinstance(first_val, (dict, list)):
                        col_type = "json"
                
                columns.append({
                    "name": col,
                    "type": col_type,
                    "native_type": str(dtype)
                })

            return {
                "asset": asset,
                "columns": columns,
                "format": asset.split('.')[-1].lower() if '.' in asset else 'unknown',
                "row_count_estimate": len(df)
            }
        except Exception as e:
            logger.error(f"Schema inference failed for GCS file {asset}: {e}")
            raise SchemaDiscoveryError(f"GCS schema inference failed for {asset}: {e}")

    def read_batch(
        self,
        asset: str,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        **kwargs,
    ) -> Iterator[pd.DataFrame]:
        self.connect()
        bucket = self._client.get_bucket(self._config_model.bucket)
        blob = bucket.blob(asset)
        
        # For large files, we should use chunked reading if format supports it
        # CSV and JSONL support chunksize
        ext = os.path.splitext(asset)[1].lower()
        chunksize = kwargs.get("chunksize", 10000)
        
        # Note: In a real implementation, we'd use smart streaming
        # For this prototype, we'll download and use pandas chunks
        with io.BytesIO() as bio:
            blob.download_to_file(bio)
            bio.seek(0)
            
            if ext == ".csv":
                reader = pd.read_csv(bio, chunksize=chunksize)
                rows_yielded = 0
                for df in reader:
                    if limit is not None:
                        remaining = limit - rows_yielded
                        if remaining <= 0:
                            break
                        if len(df) > remaining:
                            df = df.iloc[:int(remaining)]
                    
                    yield df
                    rows_yielded += len(df)
                    if limit is not None and rows_yielded >= limit: break
            
            elif ext == ".tsv":
                reader = pd.read_csv(bio, sep='\t', chunksize=chunksize)
                rows_yielded = 0
                for df in reader:
                    if limit is not None:
                        remaining = limit - rows_yielded
                        if remaining <= 0:
                            break
                        if len(df) > remaining:
                            df = df.iloc[:int(remaining)]
                    yield df
                    rows_yielded += len(df)
                    if limit is not None and rows_yielded >= limit: break

            elif ext == ".txt":
                reader = pd.read_csv(bio, sep='\n', header=None, names=['line'], chunksize=chunksize)
                rows_yielded = 0
                for df in reader:
                    if limit is not None:
                        remaining = limit - rows_yielded
                        if remaining <= 0:
                            break
                        if len(df) > remaining:
                            df = df.iloc[:int(remaining)]
                    yield df
                    rows_yielded += len(df)
                    if limit is not None and rows_yielded >= limit: break

            elif ext == ".xml":
                df = pd.read_xml(bio)
                df = self.slice_dataframe(df, offset, limit)
                yield df

            elif ext in (".xls", ".xlsx"):
                df = pd.read_excel(bio)
                df = self.slice_dataframe(df, offset, limit)
                yield df

            elif ext == ".parquet":
                df = pd.read_parquet(bio)
                df = self.slice_dataframe(df, offset, limit)
                yield df
            
            elif ext == ".jsonl":
                reader = pd.read_json(bio, lines=True, chunksize=chunksize)
                rows_yielded = 0
                for df in reader:
                    if limit is not None:
                        remaining = limit - rows_yielded
                        if remaining <= 0:
                            break
                        if len(df) > remaining:
                            df = df.iloc[:int(remaining)]
                    yield df
                    rows_yielded += len(df)
                    if limit is not None and rows_yielded >= limit: break
            
            elif ext == ".json":
                df = pd.read_json(bio)
                df = self.slice_dataframe(df, offset, limit)
                yield df

    def write_batch(
        self,
        data: Union[pd.DataFrame, Iterator[pd.DataFrame]],
        asset: str,
        mode: str = "append",
        **kwargs,
    ) -> int:
        self.connect()
        bucket = self._client.get_bucket(self._config_model.bucket)
        blob = bucket.blob(asset)
        
        # GCS doesn't support "append" to existing blobs easily (requires compose)
        # We'll treat all writes as full replaces for simplicity in this implementation
        
        if isinstance(data, pd.DataFrame):
            df = data
        else:
            df = pd.concat(list(data))

        ext = os.path.splitext(asset)[1].lower()
        with io.BytesIO() as bio:
            if ext == ".csv":
                df.to_csv(bio, index=False)
            elif ext == ".parquet":
                df.to_parquet(bio, index=False)
            elif ext == ".json":
                df.to_json(bio, orient="records")
            elif ext == ".jsonl":
                df.to_json(bio, orient="records", lines=True)
            
            bio.seek(0)
            blob.upload_from_file(bio, content_type=self._get_content_type(ext))
        
        return len(df)

    def _get_content_type(self, ext: str) -> str:
        if ext == ".csv": return "text/csv"
        if ext == ".parquet": return "application/octet-stream"
        if ext == ".json": return "application/json"
        if ext == ".jsonl": return "application/x-ndjson"
        return "application/octet-stream"

    def execute_query(
        self,
        query: str,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError("GCS connector does not support direct queries. Use file paths as assets.")
