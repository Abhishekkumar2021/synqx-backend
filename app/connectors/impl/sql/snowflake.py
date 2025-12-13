from typing import Any, Dict, Optional
from sqlalchemy import create_engine
from app.connectors.impl.sql.postgres import PostgresConnector
from app.core.errors import ConfigurationError
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class SnowflakeConfig(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", case_sensitive=False)

    user: str = Field(..., description="Snowflake User")
    password: str = Field(..., description="Snowflake Password")
    account: str = Field(..., description="Snowflake Account (e.g. xy12345.us-east-1)")
    warehouse: str = Field(..., description="Warehouse")
    database: str = Field(..., description="Database")
    schema_name: str = Field("PUBLIC", alias="schema", description="Schema")
    role: Optional[str] = Field(None, description="Role")

class SnowflakeConnector(PostgresConnector):
    """
    Connector for Snowflake using SQLAlchemy.
    """

    def __init__(self, config: Dict[str, Any]):
        self._config_model: Optional[SnowflakeConfig] = None
        super().__init__(config)

    def validate_config(self) -> None:
        try:
            self._config_model = SnowflakeConfig.model_validate(self.config)
        except Exception as e:
            raise ConfigurationError(f"Invalid Snowflake configuration: {e}")

    def _sqlalchemy_url(self) -> str:
        url = (
            f"snowflake://{self._config_model.user}:{self._config_model.password}"
            f"@{self._config_model.account}/{self._config_model.database}/{self._config_model.schema_name}"
            f"?warehouse={self._config_model.warehouse}"
        )
        if self._config_model.role:
            url += f"&role={self._config_model.role}"
        return url
