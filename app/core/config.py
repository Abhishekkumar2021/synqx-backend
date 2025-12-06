from typing import List
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    APP_NAME: str = "SynqX ETL Agent"
    ENVIRONMENT: str = "development"
    DATABASE_URL: str
    
    # CORS
    ALLOWED_ORIGINS: List[str] = ["*"]
    
    # Logging
    JSON_LOGS: bool = False
    LOG_LEVEL: str = "INFO"
    
    # Security
    MASTER_PASSWORD: str = "changeme"
    
    # External Services
    REDIS_URL: str = "redis://localhost:6379/0"

    model_config = SettingsConfigDict(
        env_file=".env", 
        env_file_encoding="utf-8", 
        case_sensitive=True,
        extra="ignore"
    )

settings = Settings()