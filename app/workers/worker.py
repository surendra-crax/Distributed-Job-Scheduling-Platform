import asyncio
import random
import signal
import time
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import get_settings
from app.core.logging import configure_logging, get_logger
from app.db.session import close_db_engine, get_session_factory
from app.exceptions.errors import (
    PermanentJobError,
    RetryableJobError,
)
from app.handlers.base import CancellationToken, JobExecutionContext
from app.handlers.registry import registry
from app.models.attempt import AttemptOutcome
from app.models.job import JobStatus
from app.queue.redis_queue import close_redis_client, get_redis_client
from app.repositories.job_repository import JobRepository

logger = get_logger(__name__)


def calculate_backoff_delay(
    attempt_count: int,
    base_delay: float = 2.0,
    max_delay: float = 300.0,
    use_jitter: bool = True,
) -> float:
    """Calculates exponential backoff delay with optional full jitter.

    Formula: min(max_delay, base_delay * 2^(attempt - 1))
    Full Jitter: uniform_random(0, calculated_delay)
    """
    exponent = max(0, attempt_count - 1)
    delay = min(max_delay, base_delay * (2**exponent))
    if use_jitter:
        return float(random.uniform(0.5, delay))
    return float(delay)


class WorkerProcess:
    """Autonomous worker processing jobs from the queue with atomic lease ownership and fencing verification."""

    def __init__(
        self,
        worker_id: str | None = None,
        session_factory: async_sessionmaker[AsyncSession] | None = None,
        redis_client: aioredis.Redis | None = None,
        concurrency: int | None = None,
    ) -> None:
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self.settings = get_settings()
        self.session_factory = session_factory or get_session_factory()
        self.redis = redis_client or get_redis_client()
        self.concurrency = (
            concurrency if concurrency is not None else self.settings.WORKER_CONCURRENCY
        )
        self._running: bool = False
        self._stop_event = asyncio.Event()

    async def fetch_next_job_id(self) -> uuid.UUID | None:
        """Pops the highest priority job ID from the Redis ZSET buffer."""
        try:
            # ZPOPMIN retrieves member with smallest score (lowest score = highest priority in our indexing)
            results = await self.redis.zpopmin(self.settings.REDIS_JOB_QUEUE_KEY, count=1)
            if results:
                raw_id, _score = results[0]
                return uuid.UUID(raw_id)
        except Exception as exc:
            logger.warning("worker_redis_pop_failed", worker_id=self.worker_id, error=str(exc))
        return None

    async def process_job(self, job_id: uuid.UUID) -> bool:
        """Executes a single job from claim to completion, enforcing leases and fencing."""
        now = datetime.now(UTC)
        start_mono = time.perf_counter()

        async with self.session_factory() as session:
            repo = JobRepository(session)

            # Step 1: Pre-fetch job to inspect timeout configuration
            job = await repo.get_by_id(job_id)
            if not job or job.status != JobStatus.QUEUED.value:
                # Job was cancelled or claimed by another worker; drop cleanly
                return False

            timeout_seconds = job.timeout_seconds
            job_type = job.job_type
            raw_payload = job.payload

            # Step 2: Atomic Claim in PostgreSQL
            claimed_job = await repo.atomic_worker_claim(
                job_id=job_id,
                worker_id=self.worker_id,
                timeout_seconds=timeout_seconds,
                now=now,
            )
            if not claimed_job:
                # Lost race to another worker
                return False

            attempt_number = claimed_job.attempt_count
            fencing_version = claimed_job.version
            max_attempts = claimed_job.max_attempts

            # Step 3: Record attempt start
            attempt = await repo.record_attempt_start(
                job_id=job_id,
                worker_id=self.worker_id,
                attempt_number=attempt_number,
                started_at=now,
            )
            await session.commit()

        logger.info(
            "worker_job_claimed",
            worker_id=self.worker_id,
            job_id=str(job_id),
            attempt=attempt_number,
            fencing_version=fencing_version,
        )

        # Step 4: Execution Harness setup
        cancellation_token = CancellationToken()
        context = JobExecutionContext(
            job_id=job_id,
            attempt_number=attempt_number,
            worker_id=self.worker_id,
            timeout_seconds=timeout_seconds,
            cancellation_token=cancellation_token,
        )

        # Start background heartbeat to renew lease for long tasks
        heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(job_id, fencing_version, timeout_seconds)
        )

        outcome = AttemptOutcome.SUCCEEDED
        error_details: dict[str, Any] | None = None
        result_payload: dict[str, Any] | None = None

        try:
            handler = registry.get(job_type)
            validated_payload = handler.validate_payload(raw_payload)

            # Execute with strict timeout enforcement
            result_payload = await asyncio.wait_for(
                handler.execute(validated_payload, context),
                timeout=float(timeout_seconds),
            )
            logger.info("worker_handler_execution_succeeded", job_id=str(job_id))

        except TimeoutError:
            outcome = AttemptOutcome.TIMED_OUT
            error_details = {
                "error": "ExecutionTimeout",
                "message": f"Execution exceeded limit of {timeout_seconds}s",
            }
            logger.warning("worker_handler_timed_out", job_id=str(job_id), timeout=timeout_seconds)

        except RetryableJobError as exc:
            outcome = AttemptOutcome.FAILED
            error_details = {
                "error": "RetryableJobError",
                "message": exc.message,
                "details": exc.details,
            }
            logger.info("worker_handler_retryable_failure", job_id=str(job_id), error=exc.message)

        except PermanentJobError as exc:
            outcome = AttemptOutcome.FAILED
            error_details = {
                "error": "PermanentJobError",
                "message": exc.message,
                "details": exc.details,
                "permanent": True,
            }
            logger.warning(
                "worker_handler_permanent_failure", job_id=str(job_id), error=exc.message
            )

        except Exception as exc:
            if cancellation_token.is_cancelled:
                outcome = AttemptOutcome.CANCELLED
                error_details = {"error": "Cancelled", "message": "Execution cancelled"}
            else:
                outcome = AttemptOutcome.FAILED
                error_details = {
                    "error": exc.__class__.__name__,
                    "message": str(exc),
                }
                logger.error("worker_handler_unhandled_failure", job_id=str(job_id), error=str(exc))

        finally:
            # Cancel heartbeat background task
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass

        # Step 5: Finalize State in PostgreSQL with Fencing Token Verification
        duration_s = time.perf_counter() - start_mono
        completion_time = datetime.now(UTC)

        async with self.session_factory() as session:
            repo = JobRepository(session)

            # Record attempt outcome
            await repo.record_attempt_completion(
                attempt_id=attempt.id,
                outcome=outcome,
                completed_at=completion_time,
                duration_seconds=round(duration_s, 4),
                error_details=error_details,
            )

            is_permanent = error_details.get("permanent", False) if error_details else False
            has_attempts_remaining = attempt_number < max_attempts

            if outcome == AttemptOutcome.SUCCEEDED:
                # Complete job successfully
                saved = await repo.atomic_finish_job(
                    job_id=job_id,
                    expected_version=fencing_version,
                    status=JobStatus.SUCCEEDED,
                    result=result_payload,
                    now=completion_time,
                )
            elif outcome == AttemptOutcome.CANCELLED:
                saved = await repo.atomic_finish_job(
                    job_id=job_id,
                    expected_version=fencing_version,
                    status=JobStatus.CANCELLED,
                    error=error_details,
                    now=completion_time,
                )
            elif not is_permanent and has_attempts_remaining:
                # Schedule retry with exponential backoff
                backoff_s = calculate_backoff_delay(
                    attempt_count=attempt_number,
                    base_delay=self.settings.RETRY_BASE_DELAY_SECONDS,
                    max_delay=self.settings.RETRY_MAX_DELAY_SECONDS,
                    use_jitter=self.settings.RETRY_JITTER,
                )
                retry_at = completion_time + timedelta(seconds=backoff_s)
                logger.info(
                    "worker_scheduling_retry",
                    job_id=str(job_id),
                    attempt=attempt_number,
                    next_attempt_in_seconds=round(backoff_s, 2),
                )
                saved = await repo.atomic_reschedule_job(
                    job_id=job_id,
                    expected_version=fencing_version,
                    next_status=JobStatus.RETRY_WAIT,
                    scheduled_at=retry_at,
                    error=error_details,
                    now=completion_time,
                )
            else:
                # Permanent failure or max attempts exhausted
                saved = await repo.atomic_finish_job(
                    job_id=job_id,
                    expected_version=fencing_version,
                    status=JobStatus.FAILED,
                    error=error_details,
                    now=completion_time,
                )

            if not saved:
                # Fencing token mismatch! Another worker or the recovery daemon reassigned the lease.
                logger.critical(
                    "stale_worker_fencing_token_rejected",
                    worker_id=self.worker_id,
                    job_id=str(job_id),
                    attempt=attempt_number,
                    fencing_version=fencing_version,
                )
                # Stale write is safely discarded
                await session.rollback()
                return False

            await session.commit()

        return True

    async def _heartbeat_loop(
        self,
        job_id: uuid.UUID,
        fencing_version: int,
        timeout_seconds: int,
    ) -> None:
        """Periodically extends the lease of a running job."""
        interval = min(self.settings.WORKER_HEARTBEAT_INTERVAL_SECONDS, timeout_seconds / 2.0)
        while True:
            try:
                await asyncio.sleep(interval)
                async with self.session_factory() as session:
                    repo = JobRepository(session)
                    renewed = await repo.renew_lease(
                        job_id=job_id,
                        worker_id=self.worker_id,
                        expected_version=fencing_version,
                        extension_seconds=timeout_seconds,
                    )
                    if renewed:
                        await session.commit()
                        logger.debug("worker_lease_renewed", job_id=str(job_id))
                    else:
                        logger.warning("worker_lease_renewal_failed", job_id=str(job_id))
                        break
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("worker_heartbeat_error", job_id=str(job_id), error=str(exc))

    async def _worker_loop(self) -> None:
        """Internal loop for a single concurrent execution slot."""
        while self._running:
            job_id = await self.fetch_next_job_id()
            if job_id:
                try:
                    await self.process_job(job_id)
                except Exception as exc:
                    logger.error(
                        "worker_unexpected_process_error", job_id=str(job_id), error=str(exc)
                    )
            else:
                # Queue empty, wait before polling again
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=self.settings.WORKER_QUEUE_WAIT_TIMEOUT_SECONDS,
                    )
                except TimeoutError:
                    pass

    async def run(self) -> None:
        """Starts the worker process with configured concurrency."""
        self._running = True
        self._stop_event.clear()
        logger.info(
            "worker_process_started",
            worker_id=self.worker_id,
            concurrency=self.concurrency,
        )

        tasks = [asyncio.create_task(self._worker_loop()) for _ in range(self.concurrency)]
        await asyncio.gather(*tasks)
        logger.info("worker_process_stopped", worker_id=self.worker_id)

    def stop(self) -> None:
        """Signals the worker process to stop gracefully."""
        self._running = False
        self._stop_event.set()


async def main() -> None:
    """CLI entrypoint for standalone worker process."""
    configure_logging()
    logger.info("worker_main_initializing")

    worker = WorkerProcess()

    def handle_signal(sig: int, frame: Any) -> None:
        logger.info("worker_termination_signal_received", signal=sig)
        worker.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        await worker.run()
    finally:
        await close_db_engine()
        await close_redis_client()
        logger.info("worker_main_exited")


if __name__ == "__main__":
    asyncio.run(main())
