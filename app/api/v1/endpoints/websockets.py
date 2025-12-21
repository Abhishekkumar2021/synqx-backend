from fastapi import APIRouter, WebSocket
from app.core.websockets import manager

router = APIRouter()

@router.websocket("/ws/jobs/{job_id}")
async def websocket_job_logs(websocket: WebSocket, job_id: int):
    """
    WebSocket endpoint to stream real-time logs for a specific Job.
    """
    await manager.connect_and_stream(websocket, f"job:{job_id}")

@router.websocket("/ws/steps/{step_run_id}")
async def websocket_step_logs(websocket: WebSocket, step_run_id: int):
    """
    WebSocket endpoint to stream real-time logs for a specific Step Run.
    """
    await manager.connect_and_stream(websocket, f"step:{step_run_id}")

@router.websocket("/ws/notifications/{user_id}")
async def websocket_notifications(websocket: WebSocket, user_id: int):
    """
    WebSocket endpoint to stream real-time notifications for a specific User.
    """
    await manager.connect_and_stream(websocket, f"user_notifications:{user_id}")
