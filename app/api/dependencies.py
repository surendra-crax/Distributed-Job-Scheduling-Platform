from collections.abc import AsyncGenerator

import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_db_session
from app.queue.redis_queue import get_redis_client


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Dependency for obtaining an asynchronous database session."""
    async for session in get_db_session():
        yield session


def get_redis() -> aioredis.Redis:
    """Dependency for obtaining the asynchronous Redis client."""
    return get_redis_client()
