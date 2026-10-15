import time

import redis.asyncio as aioredis

from app.core.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)

_redis_client: aioredis.Redis | None = None


def get_redis_client() -> aioredis.Redis:
    """Retrieve or create the global async Redis client."""
    global _redis_client
    if _redis_client is None:
        settings = get_settings()
        _redis_client = aioredis.from_url(
            settings.resolved_redis_url,
            encoding="utf-8",
            decode_responses=True,
            health_check_interval=30,
        )
    return _redis_client


async def close_redis_client() -> None:
    """Close the global Redis client connection."""
    global _redis_client
    if _redis_client is not None:
        if hasattr(_redis_client, "aclose"):
            await _redis_client.aclose()
        else:
            await _redis_client.close()
        _redis_client = None


async def check_redis_connection() -> tuple[bool, float | None, str | None]:
    """Check connectivity to Redis for readiness probe.

    Returns:
        (is_healthy, latency_ms, error_message)
    """
    start_time = time.perf_counter()
    try:
        client = get_redis_client()
        pong = await client.ping()
        latency_ms = (time.perf_counter() - start_time) * 1000.0
        if pong:
            return True, round(latency_ms, 2), None
        return False, None, "Redis ping returned unexpected response"
    except Exception as exc:
        logger.warning("redis_health_check_failed", error=str(exc))
        return False, None, str(exc)
