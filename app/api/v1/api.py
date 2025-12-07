from fastapi import APIRouter

from app.api.v1.endpoints import pipelines, connections, jobs

api_router = APIRouter()

api_router.include_router(connections.router, prefix="/connections", tags=["connections & assets"])
api_router.include_router(pipelines.router, prefix="/pipelines", tags=["pipelines"])
api_router.include_router(jobs.router, prefix="", tags=["jobs & runs"])