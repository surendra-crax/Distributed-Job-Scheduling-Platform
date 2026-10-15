import uuid
from datetime import UTC, datetime

import redis.asyncio as aioredis
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.logging import get_logger
from app.exceptions.errors import (
    InvalidStateTransitionError,
    JobNotFoundError,
)
from app.handlers.registry import registry
from app.models.attempt import JobAttempt
from app.models.job import Job, JobStatus
from app.repositories.job_repository import JobRepository
from app.schemas.job import JobCreateRequest

logger = get_logger(__name__)


class JobService:
    """Service orchestrating job business logic, validation, state transitions, and queue publication."""

    def __init__(self, session: AsyncSession, redis: aioredis.Redis) -> None:
        self.session = session
        self.redis = redis
        self.repo = JobRepository(session)
        self.settings = get_settings()

    async def submit_job(self, req: JobCreateRequest) -> Job:
        """Validates payload against registered handler, persists job, and enqueues if immediate."""
        # 1. Validate handler existence and payload schema
        handler = registry.get(req.job_type)
        validated_payload = handler.validate_payload(req.payload)

        # 2. Determine initial state based on scheduling timestamp
        now = datetime.now(UTC)
        is_delayed = req.scheduled_at is not None and req.scheduled_at > now
        initial_status = JobStatus.SCHEDULED if is_delayed else JobStatus.QUEUED
        scheduled_at = req.scheduled_at if is_delayed else now

        # 3. Create durable Job record in PostgreSQL
        job = Job(
            id=uuid.uuid4(),
            job_type=req.job_type,
            payload=validated_payload.model_dump(),
            priority=req.priority,
            status=initial_status.value,
            scheduled_at=scheduled_at,
            max_attempts=req.max_attempts,
            timeout_seconds=req.timeout_seconds,
            attempt_count=0,
            version=1,
        )
        job = await self.repo.create(job)
        await self.session.commit()

        # 4. If immediate execution, push job_id to Redis ready queue buffer
        if initial_status == JobStatus.QUEUED:
            try:
                # Use priority as negative score for max-priority sorting, or push to priority list
                # In Redis ZSET, score = -priority gives highest priority first, with timestamp tiebreak
                score = -float(job.priority) + (job.created_at.timestamp() / 1e12)
                await self.redis.zadd(self.settings.REDIS_JOB_QUEUE_KEY, {str(job.id): score})
            except Exception as exc:
                # PostgreSQL is authoritative. If Redis publication fails, recovery daemon will reconcile.
                logger.warning(
                    "redis_immediate_enqueue_failed_will_reconcile",
                    job_id=str(job.id),
                    error=str(exc),
                )

        logger.info(
            "job_submitted",
            job_id=str(job.id),
            job_type=job.job_type,
            priority=job.priority,
            status=job.status,
            scheduled_at=job.scheduled_at.isoformat(),
        )
        return job

    async def get_job(self, job_id: uuid.UUID) -> Job:
        """Retrieve a job by ID or raise JobNotFoundError."""
        job = await self.repo.get_by_id(job_id)
        if job is None:
            raise JobNotFoundError(f"Job with ID '{job_id}' not found", {"job_id": str(job_id)})
        return job

    async def list_jobs(
        self,
        status: JobStatus | None = None,
        job_type: str | None = None,
        created_after: datetime | None = None,
        created_before: datetime | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> tuple[list[Job], int]:
        """List jobs with filtering and pagination."""
        return await self.repo.list_jobs(
            status=status,
            job_type=job_type,
            created_after=created_after,
            created_before=created_before,
            limit=limit,
            offset=offset,
        )

    async def get_job_attempts(self, job_id: uuid.UUID) -> list[JobAttempt]:
        """Retrieve execution history for a job."""
        # Verify job exists
        await self.get_job(job_id)
        return await self.repo.get_attempts(job_id)

    async def cancel_job(self, job_id: uuid.UUID) -> tuple[JobStatus, JobStatus, str]:
        """Cancels a job if permitted by lifecycle rules. Returns (previous_status, new_status, message)."""
        job = await self.get_job(job_id)
        current_status = JobStatus(job.status)

        if current_status == JobStatus.CANCELLED:
            return current_status, current_status, "Job is already cancelled."

        if current_status == JobStatus.SUCCEEDED:
            raise InvalidStateTransitionError(
                current_status.value,
                JobStatus.CANCELLED.value,
                "Cannot cancel a job that has already completed successfully",
            )

        # Enforce lifecycle transition check
        if not job.can_transition_to(JobStatus.CANCELLED):
            raise InvalidStateTransitionError(
                current_status.value,
                JobStatus.CANCELLED.value,
                f"Cancellation not permitted from status '{current_status.value}'",
            )

        now = datetime.now(UTC)
        prev = current_status

        # If RUNNING, broadcast cancellation token via Redis Pub/Sub for worker cooperative abort
        if current_status == JobStatus.RUNNING:
            channel = f"{self.settings.REDIS_CANCELLATIONS_CHANNEL_PREFIX}{job_id}"
            try:
                await self.redis.publish(channel, "CANCEL")
            except Exception as exc:
                logger.warning(
                    "redis_cancellation_broadcast_failed", job_id=str(job_id), error=str(exc)
                )

        # Update database record
        job.status = JobStatus.CANCELLED.value
        job.completed_at = now
        job.lease_owner = None
        job.lease_expires_at = None
        job.version += 1
        job.updated_at = now
        await self.session.commit()

        # Remove from Redis ready queue if it was buffered
        try:
            await self.redis.zrem(self.settings.REDIS_JOB_QUEUE_KEY, str(job.id))
        except Exception:
            pass

        logger.info("job_cancelled", job_id=str(job_id), previous_status=prev.value)
        return (
            prev,
            JobStatus.CANCELLED,
            f"Job was successfully transitioned from {prev.value} to CANCELLED",
        )

    async def retry_job(self, job_id: uuid.UUID) -> Job:
        """Manually retries a FAILED or CANCELLED job."""
        job = await self.get_job(job_id)
        current_status = JobStatus(job.status)

        if not job.can_transition_to(JobStatus.QUEUED):
            raise InvalidStateTransitionError(
                current_status.value,
                JobStatus.QUEUED.value,
                f"Manual retry is not permitted for job in status '{current_status.value}'",
            )

        now = datetime.now(UTC)
        # If job previously hit max_attempts, increment max_attempts to allow another run
        if job.attempt_count >= job.max_attempts:
            job.max_attempts = job.attempt_count + 1

        job.status = JobStatus.QUEUED.value
        job.scheduled_at = now
        job.started_at = None
        job.completed_at = None
        job.lease_owner = None
        job.lease_expires_at = None
        job.result = None
        job.last_error = None
        job.version += 1
        job.updated_at = now

        await self.session.commit()

        # Enqueue to Redis
        try:
            score = -float(job.priority) + (now.timestamp() / 1e12)
            await self.redis.zadd(self.settings.REDIS_JOB_QUEUE_KEY, {str(job.id): score})
        except Exception as exc:
            logger.warning(
                "redis_retry_enqueue_failed_will_reconcile", job_id=str(job.id), error=str(exc)
            )

        logger.info("job_manual_retry_enqueued", job_id=str(job.id))
        return job
