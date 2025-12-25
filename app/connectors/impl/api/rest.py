import json
import time
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
    
    # Advanced Config
    headers: Dict[str, str] = Field(default_factory=dict, description="Custom headers for all requests")
    default_params: Dict[str, str] = Field(default_factory=dict, description="Default query parameters")
    timeout: float = Field(30.0, description="Request timeout in seconds")
    max_retries: int = Field(3, description="Maximum number of retries for failed requests")
    
    # Discovery/Assets
    endpoints: List[Dict[str, str]] = Field(
        default_factory=list, 
        description="List of pre-configured endpoints. Format: [{'name': 'users', 'path': '/users'}]"
    )
    
    # Data Extraction
    data_key: Optional[str] = Field(
        None, description="Dot-notation path to data in response (e.g. 'data.items'). If None, looks for common keys."
    )
    
    # Pagination
    pagination_type: str = Field("none", description="Pagination: none, limit_offset, page_number, cursor")
    limit_param: str = Field("limit", description="Param name for limit")
    offset_param: str = Field("offset", description="Param name for offset")
    page_param: str = Field("page", description="Param name for page number")
    page_size_param: str = Field("page_size", description="Param name for page size")
    page_size: int = Field(100, description="Default page size")

class RestApiConnector(BaseConnector):
    """
    Production-grade Connector for Generic REST APIs.
    Features: Multi-auth support, Automatic Pagination, Retries, Flexible Normalization.
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

        headers = {"Accept": "application/json", "User-Agent": "SynqX-ETL/1.0"}
        headers.update(self._config_model.headers)
        
        auth = None
        params = self._config_model.default_params.copy()

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
            follow_redirects=True,
            transport=httpx.HTTPTransport(retries=self._config_model.max_retries)
        )

    def disconnect(self) -> None:
        if self.client:
            self.client.close()
            self.client = None

    def test_connection(self) -> bool:
        try:
            self.connect()
            # If endpoints are configured, test the first one, otherwise test root
            test_path = self._config_model.endpoints[0]['path'] if self._config_model.endpoints else "/"
            res = self.client.get(test_path)
            return res.status_code < 400
        except Exception as e:
            logger.error(f"REST Connection test failed: {e}")
            return False

    def discover_assets(
        self, pattern: Optional[str] = None, include_metadata: bool = False, **kwargs
    ) -> List[Dict[str, Any]]:
        """
        Returns configured endpoints as assets.
        """
        assets = []
        if self._config_model.endpoints:
            for ep in self._config_model.endpoints:
                if pattern and pattern.lower() not in ep['name'].lower():
                    continue
                assets.append({
                    "name": ep['name'],
                    "asset_type": "endpoint",
                    "path": ep['path'],
                    "metadata": ep if include_metadata else {}
                })
        
        if not assets:
            assets.append({"name": "root", "asset_type": "endpoint", "path": "/"})
            
        return assets

    def infer_schema(self, asset: str, sample_size: int = 10, **kwargs) -> Dict[str, Any]:
        """
        Infers schema by sampling the endpoint.
        """
        self.connect()
        try:
            # We use asset as path directly or lookup from configured endpoints
            path = self._get_path(asset)
            res = self.client.get(path, params={self._config_model.limit_param: sample_size})
            res.raise_for_status()
            
            data = res.json()
            records = self._extract_records(data)
            
            if not records:
                return {"asset": asset, "columns": [], "message": "No records found to infer schema"}
            
            df = pd.DataFrame(records)
            return {
                "asset": asset,
                "columns": [
                    {"name": str(col), "type": self._map_dtype(dtype), "native_type": str(dtype)} 
                    for col, dtype in df.dtypes.items()
                ]
            }
        except Exception as e:
            raise DataTransferError(f"REST schema inference failed for {asset}: {e}")

    def fetch_sample(self, asset: str, limit: int = 10, **kwargs) -> List[Dict[str, Any]]:
        """
        Fetch a sample of data from the endpoint.
        """
        try:
            df_iter = self.read_batch(asset, limit=limit)
            df = next(df_iter)
            return df.to_dict(orient="records")
        except StopIteration:
            return []
        except Exception as e:
            logger.error(f"Sample fetch failed for {asset}: {e}")
            return []

    def read_batch(
        self, asset: str, limit: Optional[int] = None, offset: Optional[int] = None, **kwargs
    ) -> Iterator[pd.DataFrame]:
        """
        Reads data from the REST API with support for pagination.
        """
        self.connect()
        path = self._get_path(asset)
        
        # Initial Params
        base_params = kwargs.get("params", {}).copy()
        
        current_offset = offset or 0
        current_page = 1
        total_fetched = 0
        
        while True:
            params = base_params.copy()
            
            # Apply Pagination
            if self._config_model.pagination_type == "limit_offset":
                params[self._config_model.limit_param] = self._config_model.page_size
                params[self._config_model.offset_param] = current_offset
            elif self._config_model.pagination_type == "page_number":
                params[self._config_model.page_param] = current_page
                params[self._config_model.page_size_param] = self._config_model.page_size

            try:
                res = self.client.get(path, params=params)
                res.raise_for_status()
                data = res.json()
                
                records = self._extract_records(data)
                if not records:
                    break
                
                df = pd.DataFrame(records)
                
                # Apply Incremental filtering if provided
                inc_filter = kwargs.get("incremental_filter")
                if inc_filter and isinstance(inc_filter, dict):
                    for col, val in inc_filter.items():
                        if col in df.columns:
                            # Basic string/numeric comparison
                            df = df[df[col] > val]

                if not df.empty:
                    # Respect global limit if provided
                    if limit and (total_fetched + len(df) > limit):
                        df = df.iloc[:limit - total_fetched]
                        yield df
                        break
                    
                    yield df
                    total_fetched += len(df)

                # Check if we should continue paginating
                if self._config_model.pagination_type == "none" or (limit and total_fetched >= limit):
                    break
                
                # Update counters for next iteration
                if len(records) < self._config_model.page_size:
                    break # Last page reached
                    
                current_offset += len(records)
                current_page += 1
                
            except Exception as e:
                raise DataTransferError(f"REST API read failed at path {path}: {e}")

    def write_batch(
        self, data: Union[pd.DataFrame, Iterator[pd.DataFrame]], asset: str, mode: str = "append", **kwargs
    ) -> int:
        """
        Writes data to the REST API via POST requests.
        """
        self.connect()
        path = self._get_path(asset)
        total = 0
        
        data_iter = [data] if isinstance(data, pd.DataFrame) else data

        for df in data_iter:
            for record in df.to_dict(orient="records"):
                try:
                    # Mode could influence method (append -> POST, overwrite -> PUT/DELETE+POST)
                    # For a generic REST connector, we'll default to POST for each record.
                    res = self.client.post(path, json=record)
                    res.raise_for_status()
                    total += 1
                except Exception as e:
                    logger.error(f"REST write failed for record: {e}")
                    # In production, we might want to continue or fail depending on config
        
        return total

    def execute_query(
        self,
        query: str,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """
        Executes an arbitrary GET request to the path specified in 'query'.
        """
        self.connect()
        try:
            params = kwargs.get("params", {}).copy()
            if limit: params[self._config_model.limit_param] = limit
            if offset: params[self._config_model.offset_param] = offset
            
            res = self.client.get(query, params=params)
            res.raise_for_status()
            return self._extract_records(res.json())
        except Exception as e:
            logger.error(f"REST query failed: {e}")
            return []

    # --- Internal Helpers ---

    def _get_path(self, asset: str) -> str:
        """Resolves asset name to a path."""
        if self._config_model.endpoints:
            for ep in self._config_model.endpoints:
                if ep['name'] == asset:
                    return ep['path']
        return asset if asset.startswith("/") else f"/{asset}"

    def _extract_records(self, data: Any) -> List[Dict[str, Any]]:
        """
        Flexible record extraction from JSON response.
        Supports custom data_key with dot-notation.
        """
        if not data:
            return []

        target = data
        if self._config_model.data_key:
            for part in self._config_model.data_key.split('.'):
                if isinstance(target, dict) and part in target:
                    target = target[part]
                else:
                    return [] # Path not found

        # If it's already a list, great
        if isinstance(target, list):
            return [r for r in target if isinstance(r, dict)]
        
        # If it's a dict, check common keys
        if isinstance(target, dict):
            for key in ['items', 'results', 'data', 'records']:
                if key in target and isinstance(target[key], list):
                    return [r for r in target[key] if isinstance(r, dict)]
            # Single object response, wrap in list
            return [target]

        return []

    def _map_dtype(self, dtype: Any) -> str:
        s = str(dtype).lower()
        if "int" in s: return "integer"
        if "float" in s or "double" in s: return "float"
        if "bool" in s: return "boolean"
        if "datetime" in s: return "datetime"
        return "string"