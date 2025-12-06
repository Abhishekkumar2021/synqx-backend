from fastapi import APIRouter

from app.api.v1.endpoints import pipelines
from app.api.v1.endpoints import connections
from app.api.v1.endpoints import assets

api_router = APIRouter()

api_router.include_router(pipelines.router, prefix="/pipelines", tags=["pipelines"])
api_router.include_router(connections.router, prefix="/connections", tags=["connections"])
api_router.include_router(assets.router, prefix="/assets", tags=["assets"])