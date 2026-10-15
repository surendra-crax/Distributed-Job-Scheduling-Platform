from typing import Any

import redis.asyncio as aioredis
from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_db, get_redis
from app.observability.metrics import MetricsCollector

router = APIRouter(prefix="/api/v1/metrics", tags=["Observability & Metrics"])


@router.get(
    "/summary",
    summary="Operational metrics summary",
    description="Returns aggregate statistics on job states, Redis queue depth, and execution durations.",
)
async def get_metrics_summary(
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
) -> dict[str, Any]:
    collector = MetricsCollector(db, redis)
    return await collector.get_summary()
