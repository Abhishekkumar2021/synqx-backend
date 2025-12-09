from typing import Any, Dict, Iterator, List, Optional, Union
import httpx
import pandas as pd
from app.connectors.base import BaseConnector


class RestApiConnector(BaseConnector):
    """
    Connector for Generic REST APIs.
    Supports reading from GET endpoints and writing to POST/PUT endpoints.
    """

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.client: Optional[httpx.Client] = None
        self.base_url = self.config.get("base_url")
        self.auth_config = self.config.get("auth", {})
        self.headers = self.config.get("headers", {})
        self.timeout = self.config.get("timeout", 30.0)

    def validate_config(self) -> None:
        if not self.config.get("base_url"):
            raise ValueError("Configuration 'base_url' is required for RestApiConnector.")

    def connect(self) -> None:
        # Prepare auth
        auth = None
        if self.auth_config.get("type") == "basic":
            auth = (self.auth_config["username"], self.auth_config["password"])
        elif self.auth_config.get("type") == "bearer":
            self.headers["Authorization"] = f"Bearer {self.auth_config['token']}"
        elif self.auth_config.get("type") == "api_key":
            key_name = self.auth_config.get("key_name", "X-API-Key")
            key_value = self.auth_config.get("key_value")
            if self.auth_config.get("in") == "header":
                self.headers[key_name] = key_value
            # 'query' param handling would need to be in request

        self.client = httpx.Client(
            base_url=self.base_url,
            headers=self.headers,
            auth=auth,
            timeout=self.timeout
        )

    def disconnect(self) -> None:
        if self.client:
            self.client.close()
            self.client = None

    def test_connection(self) -> bool:
        try:
            with self.session():
                # Try a health check endpoint if configured, else root
                endpoint = self.config.get("health_endpoint", "/")
                response = self.client.get(endpoint)
                return response.status_code < 500
        except Exception:
            return False

    def discover_assets(
        self, pattern: Optional[str] = None, include_metadata: bool = False, **kwargs
    ) -> List[Dict[str, Any]]:
        # REST APIs don't typically expose "tables", but we can list configured endpoints if we had a schema.
        # For now, we return a dummy "endpoint" asset.
        return [{"name": "endpoint", "type": "api"}]

    def infer_schema(
        self,
        asset: str,
        sample_size: int = 1000,
        mode: str = "auto",
        **kwargs,
    ) -> Dict[str, Any]:
        # Fetch one record to infer schema
        # This is highly dependent on the API structure.
        # Assuming the API returns a list of objects or a single object.
        return {"type": "json", "fields": {}}

    def read_batch(
        self,
        asset: str,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        **kwargs,
    ) -> Iterator[pd.DataFrame]:
        """
        Reads from the API. 'asset' is treated as the endpoint path.
        """
        if not self.client:
            raise ConnectionError("Not connected")

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

            # Normalize data to list of dicts
            if isinstance(data, dict):
                # Try to find the list wrapper if it exists (e.g. {"data": [...]})
                if "data" in data and isinstance(data["data"], list):
                    records = data["data"]
                elif "items" in data and isinstance(data["items"], list):
                    records = data["items"]
                else:
                    records = [data] # Single object
            elif isinstance(data, list):
                records = data
            else:
                records = []

            if records:
                df = pd.DataFrame(records)
                yield df
            else:
                yield pd.DataFrame()
                
        except Exception as e:
            # In a real system, we might log this
            raise RuntimeError(f"Failed to read from REST API: {e}")

    def write_batch(
        self,
        data: Union[pd.DataFrame, Iterator[pd.DataFrame]],
        asset: str,
        mode: str = "append",
        **kwargs,
    ) -> int:
        """
        Writes to the API. 'asset' is treated as the endpoint path.
        Mode 'append' = POST, 'replace' = PUT (generic assumption).
        """
        if not self.client:
            raise ConnectionError("Not connected")

        count = 0
        method = "PUT" if mode == "replace" else "POST"

        iterator = [data] if isinstance(data, pd.DataFrame) else data

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
                except Exception:
                    # Continue or fail depending on strictness. 
                    # For now, we propagate error.
                    raise
        
        return count
