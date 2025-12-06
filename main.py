"""
Main FastAPI Application Entry Point.

Minimal version:
- Setup logging
- Initialize database
- Register built-in middleware
- Provide system + health endpoints
"""

import time
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator
from typing import Dict, Any

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from sqlalchemy import text

from app.core.config import settings
from app.db.session import engine, SessionLocal
from app.core.logging import setup_logging, get_logger
from app.middlewares.correlation import CorrelationMiddleware
from app.models import Base
from app.models.connections import Connection, Asset
from app.models.enums import ConnectorType
from app.services.vault_service import VaultService

# ------------------------------------------------------------
# Initialize logging early
# ------------------------------------------------------------
setup_logging()
logger = get_logger("main")


# ------------------------------------------------------------
# Database initialization
# ------------------------------------------------------------
def init_database() -> None:
    """
    Development-time DB initialization.
    (Production migrations should use Alembic)
    """
    try:
        Base.metadata.create_all(bind=engine)

        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))

        # Seed default connection and assets
        with SessionLocal() as session:
            existing_conn = session.query(Connection).filter_by(name="Default Postgres").first()
            if not existing_conn:
                logger.info("Seeding default PostgreSQL connection...")
                encrypted_config = VaultService.encrypt_config({
                    "dsn": settings.DATABASE_URL,
                    "schema": "public"
                })
                
                default_conn = Connection(
                    name="Default Postgres",
                    connector_type=ConnectorType.POSTGRESQL,
                    config_encrypted=encrypted_config,
                    health_status="healthy",
                    tenant_id="1" # Default tenant
                )
                session.add(default_conn)
                session.flush() # Get ID
                logger.info("Default connection seeded.")
                existing_conn = default_conn
            
            # Seed Assets
            users_asset = session.query(Asset).filter_by(name="users", connection_id=existing_conn.id).first()
            if not users_asset:
                logger.info("Seeding default assets...")
                users_asset = Asset(
                    connection_id=existing_conn.id,
                    name="users",
                    asset_type="table",
                    is_source=True,
                    is_destination=False,
                    tenant_id="1"
                )
                session.add(users_asset)
                
                users_backup_asset = Asset(
                    connection_id=existing_conn.id,
                    name="users_backup",
                    asset_type="table",
                    is_source=False,
                    is_destination=True,
                    tenant_id="1"
                )
                session.add(users_backup_asset)
                session.commit()
                logger.info("Default assets 'users' and 'users_backup' seeded.")

        logger.info("database_ready")
    except Exception as exc:
        logger.critical("database_init_failed", error=str(exc), exc_info=True)
        raise


# ------------------------------------------------------------
# Application lifecycle (startup/shutdown)
# ------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    start = time.perf_counter()
    logger.info(
        "application_starting",
        environment=settings.ENVIRONMENT,
        app=settings.APP_NAME,
    )

    try:
        init_database()
        logger.info(
            "application_ready",
            startup_seconds=round(time.perf_counter() - start, 3),
        )
    except Exception as exc:
        logger.critical("startup_failed", error=str(exc), exc_info=True)
        raise

    yield

    logger.info("application_stopped")


# ------------------------------------------------------------
# FastAPI instance
# ------------------------------------------------------------
app = FastAPI(
    title=settings.APP_NAME,
    version="1.0.0",
    description="Universal ETL Engine",
    lifespan=lifespan,
)


# ------------------------------------------------------------
# Middleware
# ------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(CorrelationMiddleware)


# ------------------------------------------------------------
# Exception handlers
# ------------------------------------------------------------
@app.exception_handler(RequestValidationError)
async def validation_handler(request: Request, exc: RequestValidationError):
    logger.warning("validation_error", errors=exc.errors(), path=request.url.path)
    return JSONResponse(status_code=422, content={"detail": exc.errors()})


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error("unhandled_error", error=str(exc), exc_info=True)
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error"},
    )



from app.api.v1.api import api_router # Import the main API router

# ------------------------------------------------------------
# System endpoints
# ------------------------------------------------------------
@app.get("/", tags=["System"])
async def root():
    return {
        "app": settings.APP_NAME,
        "status": "ok",
        "docs": "/docs",
    }


@app.get("/health", tags=["System"])
async def health() -> Dict[str, Any]:
    result = {"status": "healthy", "database": "unknown"}

    # DB check
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        result["database"] = "up"
    except Exception as exc:
        result["status"] = "degraded"
        result["database"] = f"down: {exc}"
        logger.error("health_db_failed", error=str(exc))

    return result


# ------------------------------------------------------------
# Main API routes
# ------------------------------------------------------------
app.include_router(api_router, prefix=settings.API_V1_STR)

# ------------------------------------------------------------
# Development entrypoint
# ------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=settings.ENVIRONMENT == "development",
    )
