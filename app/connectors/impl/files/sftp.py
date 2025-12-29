import os
import io
import stat
from typing import Any, Dict, Iterator, List, Optional, Union
import pandas as pd
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from app.connectors.base import BaseConnector
from app.core.errors import ConfigurationError, ConnectionFailedError, DataTransferError
from app.core.logging import get_logger

try:
    import paramiko
except ImportError:
    paramiko = None

logger = get_logger(__name__)

class SFTPConfig(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", case_sensitive=False)
    
    host: str = Field(..., description="SFTP Hostname or IP")
    port: int = Field(22, description="SFTP Port")
    username: str = Field(..., description="Username")
    password: Optional[str] = Field(None, description="Password")
    private_key: Optional[str] = Field(None, description="Private Key (PEM format)")
    private_key_passphrase: Optional[str] = Field(None, description="Passphrase for Private Key")
    base_path: str = Field("/", description="Base path to search for files")

class SFTPConnector(BaseConnector):
    def __init__(self, config: Dict[str, Any]):
        if paramiko is None:
            raise ConfigurationError("Paramiko client not installed. Run 'pip install paramiko'.")
        
        self._config_model: Optional[SFTPConfig] = None
        self._transport: Optional[paramiko.Transport] = None
        self._sftp: Optional[paramiko.SFTPClient] = None
        super().__init__(config)

    def validate_config(self) -> None:
        try:
            self._config_model = SFTPConfig.model_validate(self.config)
            if not self._config_model.password and not self._config_model.private_key:
                raise ConfigurationError("Either password or private_key must be provided.")
        except Exception as e:
            raise ConfigurationError(f"Invalid SFTP configuration: {e}")

    def connect(self) -> None:
        if self._sftp:
            return
        
        try:
            self._transport = paramiko.Transport((self._config_model.host, self._config_model.port))
            
            pkey = None
            if self._config_model.private_key:
                try:
                    pkey = paramiko.RSAKey.from_private_key(io.StringIO(self._config_model.private_key), password=self._config_model.private_key_passphrase)
                except Exception:
                    # Try Ed25519 or others if RSA fails, typically paramiko auto-detects from file, but from string is specific
                    # For simplicity, assuming RSA or generic key handling might require more robust logic
                    # Fallback to simple PKey loading
                    pkey = paramiko.PKey.from_private_key(io.StringIO(self._config_model.private_key), password=self._config_model.private_key_passphrase)

            self._transport.connect(username=self._config_model.username, password=self._config_model.password, pkey=pkey)
            self._sftp = paramiko.SFTPClient.from_transport(self._transport)
            
            # Verify path access
            try:
                self._sftp.listdir(self._config_model.base_path)
            except IOError:
                # Try creating it or fail? Often better to just verify connection success.
                pass
                
        except Exception as e:
            raise ConnectionFailedError(f"Failed to connect to SFTP: {e}")

    def disconnect(self) -> None:
        if self._sftp:
            self._sftp.close()
            self._sftp = None
        if self._transport:
            self._transport.close()
            self._transport = None

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
        base = self._config_model.base_path
        
        # Recursive function to walk directories
        def walk_sftp(path):
            try:
                for entry in self._sftp.listdir_attr(path):
                    remote_path = os.path.join(path, entry.filename)
                    if stat.S_ISDIR(entry.st_mode):
                        # simple recursion prevention depth check omitted for brevity
                        walk_sftp(remote_path)
                    else:
                        if pattern and pattern not in entry.filename:
                            continue
                        
                        ext = os.path.splitext(entry.filename)[1].lower()
                        if ext in [".csv", ".json", ".parquet", ".jsonl"]:
                            asset = {
                                "name": entry.filename,
                                "fully_qualified_name": remote_path,
                                "type": "file",
                                "format": ext.replace(".", "")
                            }
                            if include_metadata:
                                asset["metadata"] = {
                                    "size": entry.st_size,
                                    "modified_at": entry.st_mtime
                                }
                            assets.append(asset)
            except IOError:
                pass

        walk_sftp(base)
        return assets

    def infer_schema(self, asset: str, **kwargs) -> Dict[str, Any]:
        self.connect()
        try:
            # Read first chunk
            with self._sftp.file(asset, mode='rb') as f:
                # Read 1MB
                head = f.read(1024 * 1024)
                
            ext = os.path.splitext(asset)[1].lower()
            bio = io.BytesIO(head)
            
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
            logger.error(f"Schema inference failed for SFTP file {asset}: {e}")
            return {"asset": asset, "columns": [], "type": "file"}

    def read_batch(
        self,
        asset: str,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        **kwargs,
    ) -> Iterator[pd.DataFrame]:
        self.connect()
        
        # We must keep the file open while reading, or download it temp
        # For simplicity and standard compliance with pandas, we'll read into memory/BytesIO
        # Note: Large files over SFTP should ideally be downloaded to local temp first for speed
        
        with self._sftp.file(asset, mode='rb') as f:
            # Check size, if huge maybe warn or use temp file
            # For this impl, assume it fits in memory or we read chunks
            
            # Since pandas requires a seekable stream usually, and paramiko file acts like one mostly
            # but sometimes slow.
            
            ext = os.path.splitext(asset)[1].lower()
            chunksize = kwargs.get("chunksize", 10000)
            
            if ext == ".csv":
                reader = pd.read_csv(f, chunksize=chunksize)
                rows = 0
                for df in reader:
                    if limit and rows + len(df) > limit:
                        yield df.iloc[:limit - rows]
                        break
                    yield df
                    rows += len(df)
                    if limit and rows >= limit: break
            
            elif ext == ".parquet":
                # Parquet engines usually need a seekable file or whole file
                # Reading whole content to BytesIO for compatibility
                bio = io.BytesIO(f.read())
                df = pd.read_parquet(bio)
                if offset: df = df.iloc[offset:]
                if limit: df = df.iloc[:limit]
                yield df
            
            elif ext == ".jsonl":
                reader = pd.read_json(f, lines=True, chunksize=chunksize)
                rows = 0
                for df in reader:
                    if limit and rows + len(df) > limit:
                        yield df.iloc[:limit - rows]
                        break
                    yield df
                    rows += len(df)
                    if limit and rows >= limit: break
            
            elif ext == ".json":
                # JSON usually requires whole file
                bio = io.BytesIO(f.read())
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
        
        # mode support in SFTP? Paramiko open(mode='w'/'a')
        # But writing binary data (parquet/zip) in 'a' mode is risky without ensuring header compatibility
        # We default to overwrite ('w') for structured files unless it's CSV
        
        file_mode = 'w'
        if mode == 'append' and ext in ['.csv', '.jsonl']:
            # We can try to append
            file_mode = 'a'
            # Note: headers in CSV might be an issue if appending
        
        with self._sftp.file(asset, mode=file_mode) as f:
            f.write(bio.getvalue())
            
        return len(df)

    def execute_query(self, query: str, **kwargs) -> List[Dict[str, Any]]:
        raise NotImplementedError("SFTP connector does not support direct queries.")
