from typing import Any, Dict, List, Optional, Iterator, Union
import pandas as pd
from app.connectors.base import BaseConnector
from app.core.errors import ConfigurationError
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class MongoConfig(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", case_sensitive=False)
    connection_string: Optional[str] = Field(None, description="MongoDB connection string")
    host: Optional[str] = Field("localhost", description="MongoDB host")
    port: int = Field(27017, description="MongoDB port")
    database: str = Field(..., description="MongoDB database name")
    username: Optional[str] = Field(None, description="MongoDB username")
    password: Optional[str] = Field(None, description="MongoDB password")

class MongoDBConnector(BaseConnector):
    def __init__(self, config: Dict[str, Any]):
        self._config_model: Optional[MongoConfig] = None
        super().__init__(config)

    def validate_config(self) -> None:
        try:
            self._config_model = MongoConfig.model_validate(self.config)
            if not self._config_model.connection_string and not (self._config_model.host and self._config_model.database):
                raise ValueError("Either 'connection_string' or 'host' and 'database' must be provided.")
        except Exception as e:
            raise ConfigurationError(f"Invalid MongoDB configuration: {e}")

    def connect(self) -> None:
        # In a real implementation, this would establish a connection
        pass

    def disconnect(self) -> None:
        # In a real implementation, this would close the connection
        pass

    def test_connection(self) -> bool:
        # Simulate connection test
        return True

    def discover_assets(self, pattern: Optional[str] = None, include_metadata: bool = False, **kwargs) -> List[Dict[str, Any]]:
        # Simulate discovering collections
        mock_collections = [
            {"name": "users", "type": "collection"},
            {"name": "products", "type": "collection"},
            {"name": "orders", "type": "collection"},
        ]
        if pattern:
            mock_collections = [c for c in mock_collections if pattern.lower() in c["name"].lower()]
        return mock_collections

    def infer_schema(self, asset: str, sample_size: int = 1000, mode: str = "auto", **kwargs) -> Dict[str, Any]:
        # Simulate schema inference
        if asset.lower() == "users":
            return {"asset": asset, "columns": [{"name": "_id", "type": "ObjectId"}, {"name": "name", "type": "String"}, {"name": "email", "type": "String"}]}
        elif asset.lower() == "products":
            return {"asset": asset, "columns": [{"name": "_id", "type": "ObjectId"}, {"name": "item", "type": "String"}, {"name": "price", "type": "Double"}]}
        return {"asset": asset, "columns": [{"name": "field1", "type": "String"}]}

    def read_batch(self, asset: str, limit: Optional[int] = None, offset: Optional[int] = None, **kwargs) -> Iterator[pd.DataFrame]:
        # Simulate reading data
        if asset.lower() == "users":
            df = pd.DataFrame({"_id": [f"user_{i}" for i in range(1, 4)], "name": ["Alice", "Bob", "Charlie"]})
        elif asset.lower() == "products":
            df = pd.DataFrame({"_id": [f"prod_{i}" for i in range(1, 4)], "item": ["Laptop", "Mouse", "Keyboard"]})
        else:
            df = pd.DataFrame({"field1": [f"data_{i}" for i in range(5)]})
        
        df = self.slice_dataframe(df, offset, limit)
        yield df

    def write_batch(self, data: Union[pd.DataFrame, Iterator[pd.DataFrame]], asset: str, mode: str = "append", **kwargs) -> int:
        total_written = 0
        if isinstance(data, pd.DataFrame):
            total_written += len(data)
        else:
            for df in data:
                total_written += len(df)
        print(f"Simulating write to MongoDB collection '{asset}'. Total rows: {total_written}")
        return total_written