import os
import io
from typing import Any, Dict, Iterator, List, Optional, Union
import pandas as pd
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from app.connectors.base import BaseConnector
from app.core.errors import ConfigurationError, ConnectionFailedError
from app.core.logging import get_logger

try:
    from azure.storage.blob import BlobServiceClient
except ImportError:
    BlobServiceClient = None

logger = get_logger(__name__)

class AzureBlobConfig(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", case_sensitive=False)
    
    connection_string: Optional[str] = Field(None, description="Azure Storage Connection String")
    account_name: Optional[str] = Field(None, description="Storage Account Name")
    account_key: Optional[str] = Field(None, description="Storage Account Key")
    container_name: str = Field(..., description="Container Name")

class AzureBlobConnector(BaseConnector):
    def __init__(self, config: Dict[str, Any]):
        if BlobServiceClient is None:
            raise ConfigurationError("Azure Storage Blob client not installed. Run 'pip install azure-storage-blob'.")
        
        self._config_model: Optional[AzureBlobConfig] = None
        self._service_client: Optional[BlobServiceClient] = None
        self._container_client = None
        super().__init__(config)

    def validate_config(self) -> None:
        try:
            self._config_model = AzureBlobConfig.model_validate(self.config)
            if not self._config_model.connection_string and not (self._config_model.account_name and self._config_model.account_key):
                raise ConfigurationError("Either connection_string OR (account_name and account_key) must be provided.")
        except Exception as e:
            raise ConfigurationError(f"Invalid Azure Blob configuration: {e}")

    def connect(self) -> None:
        if self._service_client:
            return
        
        try:
            if self._config_model.connection_string:
                self._service_client = BlobServiceClient.from_connection_string(self._config_model.connection_string)
            else:
                self._service_client = BlobServiceClient(
                    account_url=f"https://{self._config_model.account_name}.blob.core.windows.net",
                    credential=self._config_model.account_key
                )
            
            self._container_client = self._service_client.get_container_client(self._config_model.container_name)
            if not self._container_client.exists():
                raise ConnectionFailedError(f"Container '{self._config_model.container_name}' does not exist.")
                
        except Exception as e:
            raise ConnectionFailedError(f"Failed to connect to Azure Blob Storage: {e}")

    def disconnect(self) -> None:
        if self._service_client:
            self._service_client.close()
            self._service_client = None
            self._container_client = None

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
        assets = []
        blobs = self._container_client.list_blobs(name_starts_with=kwargs.get("prefix"))
        
        for blob in blobs:
            if pattern and pattern not in blob.name:
                continue
            
            ext = os.path.splitext(blob.name)[1].lower()
            if ext in [".csv", ".json", ".parquet", ".jsonl"]:
                asset = {
                    "name": blob.name,
                    "fully_qualified_name": f"{self._config_model.container_name}/{blob.name}",
                    "type": "file",
                    "format": ext.replace(".", "")
                }
                if include_metadata:
                    asset["metadata"] = {
                        "size": blob.size,
                        "updated": blob.last_modified.isoformat() if blob.last_modified else None,
                        "content_type": blob.content_settings.content_type
                    }
                assets.append(asset)
        return assets

    def infer_schema(self, asset: str, **kwargs) -> Dict[str, Any]:
        self.connect()
        try:
            blob_client = self._container_client.get_blob_client(asset)
            download_stream = blob_client.download_blob(offset=0, length=1024*1024) # First 1MB
            data = download_stream.readall()
            
            ext = os.path.splitext(asset)[1].lower()
            bio = io.BytesIO(data)
            
            if ext == ".csv":
                df = pd.read_csv(bio, nrows=100)
            elif ext == ".parquet":
                df = pd.read_parquet(bio)
            elif ext in [".json", ".jsonl"]:
                df = pd.read_json(bio, lines=(ext == ".jsonl"), nrows=100 if ext == ".jsonl" else None)
            else:
                return {"asset": asset, "columns": [], "type": "file"}

            columns = []
            for col in df.columns:
                dtype = str(df[col].dtype)
                col_type = "string"
                if "int" in dtype: col_type = "integer"
                elif "float" in dtype: col_type = "float"
                elif "bool" in dtype: col_type = "boolean"
                elif "datetime" in dtype: col_type = "datetime"
                columns.append({"name": col, "type": col_type, "native_type": dtype})
            
            return {
                "asset": asset,
                "columns": columns,
                "type": "file"
            }
        except Exception as e:
            logger.error(f"Schema inference failed for Azure Blob {asset}: {e}")
            return {"asset": asset, "columns": [], "type": "file"}

    def read_batch(
        self,
        asset: str,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        **kwargs,
    ) -> Iterator[pd.DataFrame]:
        self.connect()
        blob_client = self._container_client.get_blob_client(asset)
        
        # Download fully for now (simplest for prototyping)
        # For production, use smart streaming or Azure's chunked download
        download_stream = blob_client.download_blob()
        data = download_stream.readall()
        bio = io.BytesIO(data)
        
        ext = os.path.splitext(asset)[1].lower()
        chunksize = kwargs.get("chunksize", 10000)
        
        if ext == ".csv":
            reader = pd.read_csv(bio, chunksize=chunksize)
            rows = 0
            for df in reader:
                if limit and rows + len(df) > limit:
                    yield df.iloc[:limit - rows]
                    break
                yield df
                rows += len(df)
                if limit and rows >= limit: break
        
        elif ext == ".parquet":
            df = pd.read_parquet(bio)
            if offset: df = df.iloc[offset:]
            if limit: df = df.iloc[:limit]
            yield df
            
        elif ext == ".jsonl":
            reader = pd.read_json(bio, lines=True, chunksize=chunksize)
            rows = 0
            for df in reader:
                if limit and rows + len(df) > limit:
                    yield df.iloc[:limit - rows]
                    break
                yield df
                rows += len(df)
                if limit and rows >= limit: break
        
        elif ext == ".json":
            df = pd.read_json(bio)
            if offset: df = df.iloc[offset:]
            if limit: df = df.iloc[:limit]
            yield df

    def write_batch(
        self,
        data: Union[pd.DataFrame, Iterator[pd.DataFrame]],
        asset: str,
        mode: str = "append",
        **kwargs,
    ) -> int:
        self.connect()
        blob_client = self._container_client.get_blob_client(asset)
        
        if isinstance(data, pd.DataFrame):
            df = data
        else:
            df = pd.concat(list(data))
            
        ext = os.path.splitext(asset)[1].lower()
        bio = io.BytesIO()
        
        if ext == ".csv": df.to_csv(bio, index=False)
        elif ext == ".parquet": df.to_parquet(bio, index=False)
        elif ext == ".json": df.to_json(bio, orient="records")
        elif ext == ".jsonl": df.to_json(bio, orient="records", lines=True)
        
        bio.seek(0)
        blob_client.upload_blob(bio, overwrite=True) # Azure Blob append is complex, defaulting to overwrite/replace
        return len(df)

    def execute_query(self, query: str, **kwargs) -> List[Dict[str, Any]]:
        raise NotImplementedError("Azure Blob connector does not support direct queries.")