from datetime import UTC, datetime
from typing import Any

import redis.asyncio as aioredis
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.models.attempt import AttemptOutcome, JobAttempt
from app.models.job import Job, JobStatus


class MetricsCollector:
    """Collects system-wide operational metrics and telemetry across PostgreSQL and Redis."""

    def __init__(self, session: AsyncSession, redis: aioredis.Redis) -> None:
        self.session = session
        self.redis = redis
        self.settings = get_settings()

    async def get_summary(self) -> dict[str, Any]:
        """Aggregate high-level system operational metrics."""
        # 1. Job counts grouped by status
        status_stmt = select(Job.status, func.count(Job.id)).group_by(Job.status)
        status_rows = (await self.session.execute(status_stmt)).all()
        status_counts = {st.value: 0 for st in JobStatus}
        for st_val, count in status_rows:
            status_counts[st_val] = count

        # 2. Redis queue depth
        queue_depth = 0
        try:
            queue_depth = await self.redis.zcard(self.settings.REDIS_JOB_QUEUE_KEY)
        except Exception:
            queue_depth = -1

        # 3. Attempt statistics
        attempt_stmt = select(
            func.count(JobAttempt.id),
            func.avg(JobAttempt.execution_duration_seconds),
        )
        attempt_res = (await self.session.execute(attempt_stmt)).one()
        total_attempts = attempt_res[0] or 0
        avg_duration_s = round(float(attempt_res[1]), 4) if attempt_res[1] is not None else 0.0

        # 4. Out of which succeeded vs failed
        outcome_stmt = select(JobAttempt.outcome, func.count(JobAttempt.id)).group_by(
            JobAttempt.outcome
        )
        outcome_rows = (await self.session.execute(outcome_stmt)).all()
        outcome_counts = {out.value: 0 for out in AttemptOutcome}
        for out_val, count in outcome_rows:
            outcome_counts[out_val] = count

        return {
            "timestamp": datetime.now(UTC).isoformat(),
            "jobs_by_status": status_counts,
            "redis_queue_depth": queue_depth,
            "total_attempts": total_attempts,
            "attempts_by_outcome": outcome_counts,
            "avg_execution_duration_seconds": avg_duration_s,
        }
