from fastapi import APIRouter

from app.api.v1.endpoints import pipelines, connections, jobs, websockets, auth, api_keys

api_router = APIRouter()

api_router.include_router(auth.router, prefix="/auth", tags=["authentication"])
api_router.include_router(api_keys.router, prefix="/api-keys", tags=["api-keys"])
api_router.include_router(connections.router, prefix="/connections", tags=["connections & assets"])
api_router.include_router(pipelines.router, prefix="/pipelines", tags=["pipelines"])
api_router.include_router(jobs.router, prefix="", tags=["jobs & runs"])
api_router.include_router(websockets.router, tags=["websockets"])