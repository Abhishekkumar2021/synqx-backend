from typing import Any, Dict, Iterator, List, Optional, Union
import httpx
import pandas as pd
from app.connectors.base import BaseConnector
from app.core.errors import ConfigurationError, ConnectionFailedError, DataTransferError
from pydantic import Field, AnyHttpUrl
from pydantic_settings import BaseSettings, SettingsConfigDict

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
    Connector for Generic REST APIs.
    Supports reading from GET endpoints and writing to POST/PUT endpoints.
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

        headers = {}
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
            timeout=self._config_model.timeout
        )

    def disconnect(self) -> None:
        if self.client:
            self.client.close()
            self.client = None

    def test_connection(self) -> bool:
        try:
            with self.session():
                # Try root or a health endpoint if we could configure it. 
                # For generic, we just check if we can make a request to root or just connect.
                # Since we can't know a valid endpoint for sure, we'll try root '/'
                # A 404 is technically a successful connection to the server.
                response = self.client.get("/")
                return True
        except Exception:
            # If root fails, maybe it doesn't exist. We can't be 100% sure without a known endpoint.
            # But usually 'connect' passing means we are good enough config-wise.
            return False

    def discover_assets(
        self, pattern: Optional[str] = None, include_metadata: bool = False, **kwargs
    ) -> List[Dict[str, Any]]:
        # REST APIs don't typically expose "tables", but we can list configured endpoints if we had a schema.
        # For now, we return a dummy "endpoint" asset or user provided endpoints.
        return [{"name": "endpoint", "type": "api"}]

    def infer_schema(
        self,
        asset: str,
        sample_size: int = 1000,
        mode: str = "auto",
        **kwargs,
    ) -> Dict[str, Any]:
        
        self.connect()
        try:
            # Asset is the endpoint path, e.g., "/users"
            # We assume it returns a JSON list or object.
            response = self.client.get(asset)
            response.raise_for_status()
            data = response.json()
            
            records = self._normalize_response(data)
            
            if not records:
                 return {"asset": asset, "columns": [], "format": "json"}

            df = pd.DataFrame(records[:sample_size])
             
            columns = [
                {"name": col, "type": str(dtype)}
                for col, dtype in df.dtypes.items()
            ]
            return {
                "asset": asset,
                "columns": columns,
                "format": "json"
            }
        except Exception as e:
            # Fallback
             return {"asset": asset, "columns": [], "error": str(e)}

    def read_batch(
        self,
        asset: str,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        **kwargs,
    ) -> Iterator[pd.DataFrame]:
        
        self.connect()

        # Handle pagination params (generic)
        params = kwargs.get("params", {}).copy()
        if limit is not None:
            params["limit"] = limit
        if offset is not None:
            params["offset"] = offset

        try:
            # asset is the endpoint, e.g., "/users"
            response = self.client.get(asset, params=params)
            response.raise_for_status()
            data = response.json()

            records = self._normalize_response(data)

            if records:
                df = pd.DataFrame(records)
                yield df
            else:
                yield pd.DataFrame()
                
        except Exception as e:
            raise DataTransferError(f"Failed to read from REST API: {e}")

    def _normalize_response(self, data: Any) -> List[Dict[str, Any]]:
        if isinstance(data, dict):
            # Try to find the list wrapper if it exists (e.g. {"data": [...]})
            if "data" in data and isinstance(data["data"], list):
                return data["data"]
            elif "items" in data and isinstance(data["items"], list):
                return data["items"]
            elif "results" in data and isinstance(data["results"], list):
                return data["results"]
            else:
                return [data] # Single object
        elif isinstance(data, list):
            return data
        else:
            return []

    def write_batch(
        self,
        data: Union[pd.DataFrame, Iterator[pd.DataFrame]],
        asset: str,
        mode: str = "append",
        **kwargs,
    ) -> int:
        
        self.connect()
        count = 0
        method = "PUT" if mode == "replace" else "POST"

        if isinstance(data, pd.DataFrame):
            iterator = [data]
        else:
            iterator = data

        for df in iterator:
            records = df.to_dict(orient="records")
            for record in records:
                try:
                    if method == "POST":
                        response = self.client.post(asset, json=record)
                    else:
                        response = self.client.put(asset, json=record)
                    
                    response.raise_for_status()
                    count += 1
                except Exception as e:
                    # Depending on policy, we might want to stop or log errors
                    raise DataTransferError(f"Failed to write record to {asset}: {e}")
        
        return count