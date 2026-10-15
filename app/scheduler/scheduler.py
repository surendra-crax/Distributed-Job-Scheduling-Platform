import asyncio
import signal
from datetime import UTC, datetime
from typing import Any

import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import get_settings
from app.core.logging import configure_logging, get_logger
from app.db.session import close_db_engine, get_session_factory
from app.queue.redis_queue import close_redis_client, get_redis_client
from app.repositories.job_repository import JobRepository

logger = get_logger(__name__)


class SchedulerDaemon:
    """Daemon responsible for finding due scheduled and retry jobs and moving them to the active queue."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
        redis_client: aioredis.Redis | None = None,
        poll_interval: float | None = None,
        batch_size: int | None = None,
    ) -> None:
        self.settings = get_settings()
        self.session_factory = session_factory or get_session_factory()
        self.redis = redis_client or get_redis_client()
        self.poll_interval = (
            poll_interval
            if poll_interval is not None
            else self.settings.SCHEDULER_POLL_INTERVAL_SECONDS
        )
        self.batch_size = (
            batch_size if batch_size is not None else self.settings.SCHEDULER_BATCH_SIZE
        )
        self._running: bool = False
        self._stop_event = asyncio.Event()

    async def tick(self, now: datetime | None = None) -> int:
        """Executes a single scheduler iteration:

        1. Selects due jobs (SCHEDULED or RETRY_WAIT where scheduled_at <= now) using
           `SELECT ... FOR UPDATE SKIP LOCKED` ordered by priority DESC, scheduled_at ASC, created_at ASC.
        2. Atomically transitions them to QUEUED in PostgreSQL.
        3. Pushes job IDs to the Redis priority ready queue.
        4. Commits the transaction.
        """
        current_time = now or datetime.now(UTC)
        dispatched_count = 0

        async with self.session_factory() as session:
            repo = JobRepository(session)
            # Row-level locking ensures concurrent schedulers never select or conflict on the same jobs
            due_jobs = await repo.claim_due_jobs(batch_size=self.batch_size, now=current_time)

            if not due_jobs:
                return 0

            # Commit the DB transaction transitioning status from SCHEDULED/RETRY_WAIT to QUEUED
            await session.commit()

            # Push claimed jobs to Redis ready buffer
            pipeline = self.redis.pipeline()
            for job in due_jobs:
                # Priority score: negative priority so highest priority has lowest score in ZSET
                # Adding micro-timestamp fraction ensures deterministic FIFO tiebreak among identical priorities
                score = -float(job.priority) + (job.created_at.timestamp() / 1e12)
                pipeline.zadd(self.settings.REDIS_JOB_QUEUE_KEY, {str(job.id): score})
                dispatched_count += 1

            try:
                await pipeline.execute()
            except Exception as exc:
                # Even if Redis push fails, PostgreSQL remains authoritative in QUEUED state.
                # The recovery reconciler daemon will detect un-popped QUEUED jobs and re-push.
                logger.warning(
                    "scheduler_redis_pipeline_failed",
                    count=dispatched_count,
                    error=str(exc),
                )

            logger.info(
                "scheduler_tick_dispatched",
                count=dispatched_count,
                timestamp=current_time.isoformat(),
            )

        return dispatched_count

    async def run(self) -> None:
        """Starts the scheduler loop until stopped."""
        self._running = True
        self._stop_event.clear()
        logger.info(
            "scheduler_started",
            poll_interval=self.poll_interval,
            batch_size=self.batch_size,
        )

        while self._running:
            try:
                await self.tick()
            except Exception as exc:
                logger.error("scheduler_tick_error", error=str(exc), exc_info=True)

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.poll_interval)
            except TimeoutError:
                pass

        logger.info("scheduler_stopped")

    def stop(self) -> None:
        """Signals the scheduler loop to gracefully stop."""
        self._running = False
        self._stop_event.set()


async def main() -> None:
    """CLI entrypoint for standalone scheduler process."""
    configure_logging()
    logger.info("scheduler_process_initializing")

    daemon = SchedulerDaemon()

    def handle_signal(sig: int, frame: Any) -> None:
        logger.info("scheduler_termination_signal_received", signal=sig)
        daemon.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        await daemon.run()
    finally:
        await close_db_engine()
        await close_redis_client()
        logger.info("scheduler_process_exited")


if __name__ == "__main__":
    asyncio.run(main())
