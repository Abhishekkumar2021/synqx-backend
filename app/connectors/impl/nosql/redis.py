from typing import Any, Dict, List, Optional, Iterator, Union
import pandas as pd
import redis
from app.connectors.base import BaseConnector
from app.core.errors import ConfigurationError, ConnectionFailedError, DataTransferError
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class RedisConfig(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", case_sensitive=False)
    
    host: str = Field("localhost", description="Redis Host")
    port: int = Field(6379, description="Redis Port")
    password: Optional[str] = Field(None, description="Redis Password")
    db: int = Field(0, description="Redis DB Index")
    decode_responses: bool = Field(True, description="Decode responses to strings")

class RedisConnector(BaseConnector):
    def __init__(self, config: Dict[str, Any]):
        self._config_model: Optional[RedisConfig] = None
        self._client: Optional[redis.Redis] = None
        super().__init__(config)

    def validate_config(self) -> None:
        try:
            self._config_model = RedisConfig.model_validate(self.config)
        except Exception as e:
            raise ConfigurationError(f"Invalid Redis configuration: {e}")

    def connect(self) -> None:
        if self._client:
            return
        try:
            self._client = redis.Redis(
                host=self._config_model.host,
                port=self._config_model.port,
                password=self._config_model.password,
                db=self._config_model.db,
                decode_responses=self._config_model.decode_responses
            )
            self._client.ping()
        except Exception as e:
            raise ConnectionFailedError(f"Failed to connect to Redis: {e}")

    def disconnect(self) -> None:
        if self._client:
            self._client.close()
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
        # Redis is K-V, no real "tables". We can treat keyspaces or patterns as assets?
        # For simplicity, we'll return a single "keyspace" asset or maybe specific patterns if requested.
        # But commonly, Redis ETL involves specific key patterns.
        # Let's return a wildcard asset.
        return [{"name": "*", "type": "key_pattern"}]

    def infer_schema(self, asset: str, **kwargs) -> Dict[str, Any]:
        return {
            "asset": asset,
            "columns": [{"name": "key", "type": "string"}, {"name": "value", "type": "string"}],
            "type": "kv"
        }

    def read_batch(
        self,
        asset: str,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        **kwargs,
    ) -> Iterator[pd.DataFrame]:
        self.connect()
        # asset is treated as a key pattern, e.g., "user:*"
        pattern = asset if asset != "*" else "*"
        
        cursor = 0
        count = limit if limit else 1000 # Redis scan count hint
        
        # This is a simplified scan. Full pagination in Redis is cursor based.
        # Implementing efficient offset in Redis SCAN is hard (O(N)).
        # We will use SCAN for iteration.
        
        keys_batch = []
        # SCAN
        # Note: 'offset' is ignored here because Redis SCAN doesn't support it standardly.
        
        for key in self._client.scan_iter(match=pattern, count=count):
            keys_batch.append(key)
            if len(keys_batch) >= count:
                # Fetch values
                values = self._client.mget(keys_batch)
                df = pd.DataFrame({"key": keys_batch, "value": values})
                yield df
                keys_batch = []
                if limit and limit <= 0: break # Crude limit handling
        
        if keys_batch:
            values = self._client.mget(keys_batch)
            yield pd.DataFrame({"key": keys_batch, "value": values})


    def write_batch(
        self,
        data: Union[pd.DataFrame, Iterator[pd.DataFrame]],
        asset: str, # unused mostly, maybe prefix?
        mode: str = "append",
        **kwargs,
    ) -> int:
        self.connect()
        total = 0
        
        if isinstance(data, pd.DataFrame):
            iterator = [data]
        else:
            iterator = data
            
        for df in iterator:
            if "key" not in df.columns or "value" not in df.columns:
                continue # Skip invalid
            
            # Use pipeline
            pipe = self._client.pipeline()
            mapping = dict(zip(df['key'], df['value']))
            if mapping:
                try:
                    self._client.mset(mapping)
                    total += len(mapping)
                except Exception as e:
                    raise DataTransferError(f"Redis write failed: {e}")
        return total
