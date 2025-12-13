from typing import Any, Dict, Optional
from sqlalchemy import create_engine
from app.connectors.impl.sql.postgres import PostgresConnector, PostgresConfig
from app.core.errors import ConfigurationError

class MySQLConfig(PostgresConfig):
    # MySQL config is very similar to Postgres, reusing the pydantic model but we might need tweaks if specific params differ.
    # For now, host, port, user, pass, db are standard.
    pass

class MySQLConnector(PostgresConnector):
    """
    Connector for MySQL.
    Inherits from PostgresConnector because the SQLAlchemy logic is 99% identical.
    We just override the connection string.
    """

    def validate_config(self) -> None:
        try:
            self._config_model = MySQLConfig.model_validate(self.config)
        except Exception as e:
            raise ConfigurationError(f"Invalid MySQL configuration: {e}")

    def _sqlalchemy_url(self) -> str:
        return (
            f"mysql+pymysql://"
            f"{self._config_model.username}:{self._config_model.password}"
            f"@{self._config_model.host}:{self._config_model.port}/"
            f"{self._config_model.database}"
        )
