from fastapi import APIRouter

from app.api.v1.endpoints import pipelines

api_router = APIRouter()

api_router.include_router(pipelines.router, prefix="/pipelines", tags=["pipelines"])