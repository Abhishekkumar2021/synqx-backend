import asyncio
import json
from typing import AsyncGenerator
from fastapi import WebSocket, WebSocketDisconnect
from redis import asyncio as aioredis
from app.core.config import settings
from app.core.logging import get_logger

logger = get_logger(__name__)

class ConnectionManager:
    def __init__(self):
        self.redis_url = settings.REDIS_URL

    async def connect_and_stream(self, websocket: WebSocket, channel: str):
        """
        Accepts the WebSocket connection, subscribes to the Redis channel,
        and streams messages to the client.
        """
        await websocket.accept()
        redis = aioredis.from_url(self.redis_url, decode_responses=True)
        pubsub = redis.pubsub()
        
        try:
            await pubsub.subscribe(channel)
            logger.info(f"Subscribed to Redis channel: {channel}")

            async for message in pubsub.listen():
                if message["type"] == "message":
                    # The message data should already be a JSON string from the publisher
                    await websocket.send_text(message["data"])
                    
        except WebSocketDisconnect:
            logger.info(f"Client disconnected from channel {channel}")
        except Exception as e:
            logger.error(f"WebSocket error on channel {channel}: {e}")
        finally:
            await pubsub.unsubscribe(channel)
            await redis.close()

    async def broadcast(self, channel: str, message: dict):
        """
        Publishes a message to a Redis channel.
        """
        redis = aioredis.from_url(self.redis_url, decode_responses=True)
        try:
            await redis.publish(channel, json.dumps(message))
        finally:
            await redis.close()

manager = ConnectionManager()
