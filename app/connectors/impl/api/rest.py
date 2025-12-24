from typing import Any, Dict, Iterator, List, Optional, Union
import httpx
import pandas as pd
from app.connectors.base import BaseConnector
from app.core.errors import ConfigurationError, DataTransferError
from app.core.logging import get_logger
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = get_logger(__name__)

class RestApiConfig(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", case_sensitive=False)
    
    base_url: str = Field(..., description="Base URL of the API")
    auth_type: str = Field("none", description="Auth Type: none, basic, bearer, api_key")
    auth_username: Optional[str] = Field(None, description="Username for Basic Auth")
    auth_password: Optional[str] = Field(None, description="Password for Basic Auth")
    auth_token: Optional[str] = Field(None, description="Bearer Token")
    api_key_name: Optional[str] = Field("X-API-Key", description="API Key Name")
    api_key_value: Optional[str] = Field(None, description="API Key Value")
    api_key_in: str = Field("header", description="API Key Location: header or query")
    timeout: float = Field(30.0, description="Request timeout in seconds")

class RestApiConnector(BaseConnector):
    """
    Robust Connector for Generic REST APIs.
    """

    def __init__(self, config: Dict[str, Any]):
        self._config_model: Optional[RestApiConfig] = None
        self.client: Optional[httpx.Client] = None
        super().__init__(config)

    def validate_config(self) -> None:
        try:
            self._config_model = RestApiConfig.model_validate(self.config)
        except Exception as e:
            raise ConfigurationError(f"Invalid REST API configuration: {e}")

    def connect(self) -> None:
        if self.client:
            return

        headers = {"Accept": "application/json"}
        auth = None
        params = {}

        if self._config_model.auth_type == "basic":
            if self._config_model.auth_username and self._config_model.auth_password:
                auth = (self._config_model.auth_username, self._config_model.auth_password)
        elif self._config_model.auth_type == "bearer":
             if self._config_model.auth_token:
                headers["Authorization"] = f"Bearer {self._config_model.auth_token}"
        elif self._config_model.auth_type == "api_key":
            if self._config_model.api_key_value:
                if self._config_model.api_key_in == "header":
                    headers[self._config_model.api_key_name] = self._config_model.api_key_value
                else:
                    params[self._config_model.api_key_name] = self._config_model.api_key_value

        self.client = httpx.Client(
            base_url=self._config_model.base_url,
            headers=headers,
            params=params,
            auth=auth,
            timeout=self._config_model.timeout,
            follow_redirects=True
        )

    def disconnect(self) -> None:
        if self.client:
            self.client.close()
            self.client = None

    def test_connection(self) -> bool:
        try:
            self.connect()
            # Try a simple GET to base_url
            res = self.client.get("/")
            return res.status_code < 500
        except Exception:
            return False

    def discover_assets(
        self, pattern: Optional[str] = None, include_metadata: bool = False, **kwargs
    ) -> List[Dict[str, Any]]:
        # For REST, assets are typically endpoints. 
        # In a real system, these would be configured or discovered via OpenAPI/Swagger.
        return [{"name": "root", "type": "endpoint", "path": "/"}]

    def infer_schema(self, asset: str, **kwargs) -> Dict[str, Any]:
        self.connect()
        try:
            res = self.client.get(asset)
            res.raise_for_status()
            data = res.json()
            records = self._normalize(data)
            if not records: return {"asset": asset, "columns": []}
            
            df = pd.DataFrame(records[:10])
            return {
                "asset": asset,
                "columns": [{"name": col, "type": str(dtype)} for col, dtype in df.dtypes.items()]
            }
        except Exception as e:
            raise DataTransferError(f"REST schema inference failed: {e}")

    def read_batch(
        self, asset: str, limit: Optional[int] = None, offset: Optional[int] = None, **kwargs
    ) -> Iterator[pd.DataFrame]:
        self.connect()
        params = kwargs.get("params", {}).copy()
        if limit: params["limit"] = limit
        if offset: params["offset"] = offset
        
        incremental_filter = kwargs.get("incremental_filter")

        try:
            res = self.client.get(asset, params=params)
            res.raise_for_status()
            data = res.json()
            records = self._normalize(data)
            
            if records:
                df = pd.DataFrame(records)
                
                # Apply Incremental Filter
                if incremental_filter and isinstance(incremental_filter, dict):
                    for col, val in incremental_filter.items():
                        if col in df.columns:
                            df = df[df[col] > val]
                
                if not df.empty:
                    yield df
        except Exception as e:
            raise DataTransferError(f"REST API read failed: {e}")

    def _normalize(self, data: Any) -> List[Dict[str, Any]]:
        if isinstance(data, list): return data
        if isinstance(data, dict):
            for key in ['data', 'items', 'results', 'records']:
                if key in data and isinstance(data[key], list):
                    return data[key]
            return [data]
        return []

    def write_batch(
        self, data: Union[pd.DataFrame, Iterator[pd.DataFrame]], asset: str, mode: str = "append", **kwargs
    ) -> int:
        self.connect()
        total = 0
        if isinstance(data, pd.DataFrame): data_iter = [data]
        else: data_iter = data

        for df in data_iter:
            for record in df.to_dict(orient="records"):
                try:
                    res = self.client.post(asset, json=record)
                    res.raise_for_status()
                    total += 1
                except Exception as e:
                    logger.error(f"Failed to write record: {e}")
        return total

    def execute_query(
        self,
        query: str,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError("Direct query execution is not supported for REST API connector. Use endpoints as assets.")
