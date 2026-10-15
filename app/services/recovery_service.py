import asyncio
import signal
from datetime import UTC, datetime, timedelta
from typing import Any

import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import get_settings
from app.core.logging import configure_logging, get_logger
from app.db.session import close_db_engine, get_session_factory
from app.models.attempt import AttemptOutcome
from app.models.job import JobStatus
from app.queue.redis_queue import close_redis_client, get_redis_client
from app.repositories.job_repository import JobRepository
from app.workers.worker import calculate_backoff_delay

logger = get_logger(__name__)


class RecoveryService:
    """Daemon responsible for crash recovery: sweeping expired leases and reconciling Redis queue desyncs."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
        redis_client: aioredis.Redis | None = None,
        poll_interval: float | None = None,
    ) -> None:
        self.settings = get_settings()
        self.session_factory = session_factory or get_session_factory()
        self.redis = redis_client or get_redis_client()
        self.poll_interval = (
            poll_interval
            if poll_interval is not None
            else self.settings.RECOVERY_POLL_INTERVAL_SECONDS
        )
        self._running: bool = False
        self._stop_event = asyncio.Event()

    async def sweep_expired_leases(self, now: datetime | None = None) -> int:
        """Finds RUNNING jobs whose lease has expired, recovers them by scheduling retry or marking FAILED."""
        current_time = now or datetime.now(UTC)
        recovered_count = 0

        async with self.session_factory() as session:
            repo = JobRepository(session)
            expired_jobs = await repo.get_expired_leases(now=current_time)

            for job in expired_jobs:
                fencing_version = job.version
                attempt_number = job.attempt_count
                max_attempts = job.max_attempts
                prev_worker = job.lease_owner

                error_details = {
                    "error": "WorkerLeaseExpired",
                    "message": f"Worker '{prev_worker}' failed to renew lease or crashed before completion",
                    "lease_expired_at": job.lease_expires_at.isoformat()
                    if job.lease_expires_at
                    else None,
                }

                # Find the latest open attempt to close it as FAILED
                attempts = await repo.get_attempts(job.id)
                if attempts:
                    last_attempt = attempts[-1]
                    if last_attempt.outcome == AttemptOutcome.RUNNING.value:
                        await repo.record_attempt_completion(
                            attempt_id=last_attempt.id,
                            outcome=AttemptOutcome.FAILED,
                            completed_at=current_time,
                            duration_seconds=(
                                (current_time - last_attempt.started_at).total_seconds()
                            ),
                            error_details=error_details,
                        )

                # Check if retry attempts remain
                if attempt_number < max_attempts:
                    backoff_s = calculate_backoff_delay(
                        attempt_count=attempt_number,
                        base_delay=self.settings.RETRY_BASE_DELAY_SECONDS,
                        max_delay=self.settings.RETRY_MAX_DELAY_SECONDS,
                        use_jitter=self.settings.RETRY_JITTER,
                    )
                    retry_at = current_time + timedelta(seconds=backoff_s)
                    success = await repo.atomic_reschedule_job(
                        job_id=job.id,
                        expected_version=fencing_version,
                        next_status=JobStatus.RETRY_WAIT,
                        scheduled_at=retry_at,
                        error=error_details,
                        now=current_time,
                    )
                    logger.info(
                        "recovery_expired_job_rescheduled",
                        job_id=str(job.id),
                        previous_worker=prev_worker,
                        retry_at=retry_at.isoformat(),
                    )
                else:
                    # Max attempts exhausted: mark terminal failure
                    success = await repo.atomic_finish_job(
                        job_id=job.id,
                        expected_version=fencing_version,
                        status=JobStatus.FAILED,
                        error=error_details,
                        now=current_time,
                    )
                    logger.warning(
                        "recovery_expired_job_marked_failed",
                        job_id=str(job.id),
                        previous_worker=prev_worker,
                        attempts_exhausted=attempt_number,
                    )

                if success:
                    recovered_count += 1

            if expired_jobs:
                await session.commit()

        return recovered_count

    async def reconcile_lost_queue_jobs(self, now: datetime | None = None) -> int:
        """Finds jobs in QUEUED state in PostgreSQL that are missing from Redis and re-publishes them."""
        current_time = now or datetime.now(UTC)
        cutoff = current_time - timedelta(seconds=self.settings.RECONCILIATION_INTERVAL_SECONDS)
        reconciled_count = 0

        async with self.session_factory() as session:
            repo = JobRepository(session)
            stuck_jobs = await repo.get_stuck_queued_jobs(cutoff=cutoff)

            if not stuck_jobs:
                return 0

            for job in stuck_jobs:
                # Check if present in Redis
                score = await self.redis.zscore(self.settings.REDIS_JOB_QUEUE_KEY, str(job.id))
                if score is None:
                    # Missing from Redis! Re-publish with appropriate priority score
                    z_score = -float(job.priority) + (job.created_at.timestamp() / 1e12)
                    await self.redis.zadd(self.settings.REDIS_JOB_QUEUE_KEY, {str(job.id): z_score})
                    reconciled_count += 1
                    logger.info("recovery_reconciled_missing_redis_job", job_id=str(job.id))

        return reconciled_count

    async def run(self) -> None:
        """Starts the recovery loop."""
        self._running = True
        self._stop_event.clear()
        logger.info("recovery_daemon_started", poll_interval=self.poll_interval)

        while self._running:
            try:
                await self.sweep_expired_leases()
                await self.reconcile_lost_queue_jobs()
            except Exception as exc:
                logger.error("recovery_daemon_error", error=str(exc), exc_info=True)

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self.poll_interval)
            except TimeoutError:
                pass

        logger.info("recovery_daemon_stopped")

    def stop(self) -> None:
        """Signals the recovery loop to stop gracefully."""
        self._running = False
        self._stop_event.set()


async def main() -> None:
    """CLI entrypoint for standalone recovery daemon."""
    configure_logging()
    logger.info("recovery_process_initializing")

    recovery = RecoveryService()

    def handle_signal(sig: int, frame: Any) -> None:
        logger.info("recovery_termination_signal_received", signal=sig)
        recovery.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        await recovery.run()
    finally:
        await close_db_engine()
        await close_redis_client()
        logger.info("recovery_process_exited")


if __name__ == "__main__":
    asyncio.run(main())
