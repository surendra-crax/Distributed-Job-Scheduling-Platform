import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.attempt import AttemptOutcome, JobAttempt
from app.models.job import Job, JobStatus


class JobRepository:
    """Repository handling database operations for Jobs and JobAttempts with strict concurrency controls."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create(self, job: Job) -> Job:
        """Persists a new job entity."""
        self.session.add(job)
        await self.session.flush()
        await self.session.refresh(job)
        return job

    async def get_by_id(self, job_id: uuid.UUID) -> Job | None:
        """Retrieve a job by its primary key."""
        stmt = select(Job).where(Job.id == job_id)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def list_jobs(
        self,
        status: JobStatus | None = None,
        job_type: str | None = None,
        created_after: datetime | None = None,
        created_before: datetime | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> tuple[list[Job], int]:
        """List jobs with filters and pagination, returning (items, total_count)."""
        base_query = select(Job)
        count_query = select(func.count(Job.id))

        if status:
            base_query = base_query.where(Job.status == status.value)
            count_query = count_query.where(Job.status == status.value)
        if job_type:
            base_query = base_query.where(Job.job_type == job_type)
            count_query = count_query.where(Job.job_type == job_type)
        if created_after:
            base_query = base_query.where(Job.created_at >= created_after)
            count_query = count_query.where(Job.created_at >= created_after)
        if created_before:
            base_query = base_query.where(Job.created_at <= created_before)
            count_query = count_query.where(Job.created_at <= created_before)

        # Count total matching rows
        count_res = await self.session.execute(count_query)
        total = count_res.scalar_one() or 0

        # Query paginated rows ordered by creation descending
        base_query = (
            base_query.order_by(Job.created_at.desc(), Job.id.desc()).limit(limit).offset(offset)
        )
        res = await self.session.execute(base_query)
        items = list(res.scalars().all())

        return items, total

    async def get_attempts(self, job_id: uuid.UUID) -> list[JobAttempt]:
        """Retrieve all execution attempts for a given job."""
        stmt = (
            select(JobAttempt)
            .where(JobAttempt.job_id == job_id)
            .order_by(JobAttempt.attempt_number.asc())
        )
        res = await self.session.execute(stmt)
        return list(res.scalars().all())

    async def claim_due_jobs(
        self,
        batch_size: int,
        now: datetime | None = None,
    ) -> list[Job]:
        """Select due jobs using row-level locking (FOR UPDATE SKIP LOCKED) and transition to QUEUED.

        Deterministic ordering: priority DESC, scheduled_at ASC, created_at ASC, id ASC.
        """
        current_time = now or datetime.now(UTC)

        # Select candidate IDs with SKIP LOCKED to prevent blocking or double-selection
        subquery = (
            select(Job.id)
            .where(
                Job.status.in_([JobStatus.SCHEDULED.value, JobStatus.RETRY_WAIT.value]),
                Job.scheduled_at <= current_time,
            )
            .order_by(
                Job.priority.desc(),
                Job.scheduled_at.asc(),
                Job.created_at.asc(),
                Job.id.asc(),
            )
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        )

        candidate_ids = (await self.session.execute(subquery)).scalars().all()
        if not candidate_ids:
            return []

        # Batch transition candidates to QUEUED
        update_stmt = (
            update(Job)
            .where(Job.id.in_(candidate_ids))
            .values(
                status=JobStatus.QUEUED.value,
                updated_at=current_time,
                version=Job.version + 1,
            )
            .returning(Job)
        )
        result = await self.session.execute(update_stmt)
        return list(result.scalars().all())

    async def atomic_worker_claim(
        self,
        job_id: uuid.UUID,
        worker_id: str,
        timeout_seconds: int,
        now: datetime | None = None,
    ) -> Job | None:
        """Atomically claim a QUEUED job for execution.

        Increments attempt_count, advances fencing version, assigns lease_owner and expiration.
        Returns the updated Job, or None if the job was not in QUEUED state.
        """
        current_time = now or datetime.now(UTC)
        lease_expires = current_time + timedelta(seconds=timeout_seconds)

        stmt = (
            update(Job)
            .where(
                Job.id == job_id,
                Job.status == JobStatus.QUEUED.value,
            )
            .values(
                status=JobStatus.RUNNING.value,
                lease_owner=worker_id,
                lease_expires_at=lease_expires,
                started_at=func.coalesce(Job.started_at, current_time),
                attempt_count=Job.attempt_count + 1,
                version=Job.version + 1,
                updated_at=current_time,
            )
            .returning(Job)
        )
        res = await self.session.execute(stmt)
        return res.scalar_one_or_none()

    async def record_attempt_start(
        self,
        job_id: uuid.UUID,
        worker_id: str,
        attempt_number: int,
        started_at: datetime | None = None,
    ) -> JobAttempt:
        """Record the initiation of a job execution attempt."""
        current_time = started_at or datetime.now(UTC)
        attempt = JobAttempt(
            id=uuid.uuid4(),
            job_id=job_id,
            worker_id=worker_id,
            attempt_number=attempt_number,
            started_at=current_time,
            outcome=AttemptOutcome.RUNNING.value,
        )
        self.session.add(attempt)
        await self.session.flush()
        return attempt

    async def record_attempt_completion(
        self,
        attempt_id: uuid.UUID,
        outcome: AttemptOutcome,
        completed_at: datetime | None = None,
        duration_seconds: float | None = None,
        error_details: dict[str, Any] | None = None,
    ) -> None:
        """Finalize the outcome and duration of an execution attempt."""
        current_time = completed_at or datetime.now(UTC)
        stmt = (
            update(JobAttempt)
            .where(JobAttempt.id == attempt_id)
            .values(
                outcome=outcome.value,
                completed_at=current_time,
                execution_duration_seconds=duration_seconds,
                error_details=error_details,
            )
        )
        await self.session.execute(stmt)

    async def atomic_finish_job(
        self,
        job_id: uuid.UUID,
        expected_version: int,
        status: JobStatus,
        result: dict[str, Any] | None = None,
        error: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> bool:
        """Atomically persist job completion (SUCCEEDED or FAILED) with fencing token check.

        Returns True if updated, or False if version mismatch (stale worker lease).
        """
        current_time = now or datetime.now(UTC)
        stmt = (
            update(Job)
            .where(
                Job.id == job_id,
                Job.version == expected_version,
            )
            .values(
                status=status.value,
                result=result,
                last_error=error,
                completed_at=current_time,
                lease_owner=None,
                lease_expires_at=None,
                version=Job.version + 1,
                updated_at=current_time,
            )
            .returning(Job.id)
        )
        res = await self.session.execute(stmt)
        return res.scalar_one_or_none() is not None

    async def atomic_reschedule_job(
        self,
        job_id: uuid.UUID,
        expected_version: int,
        next_status: JobStatus,
        scheduled_at: datetime,
        error: dict[str, Any] | None = None,
        now: datetime | None = None,
    ) -> bool:
        """Reschedule a job to RETRY_WAIT or QUEUED with fencing token check."""
        current_time = now or datetime.now(UTC)
        stmt = (
            update(Job)
            .where(
                Job.id == job_id,
                Job.version == expected_version,
            )
            .values(
                status=next_status.value,
                scheduled_at=scheduled_at,
                last_error=error,
                lease_owner=None,
                lease_expires_at=None,
                version=Job.version + 1,
                updated_at=current_time,
            )
            .returning(Job.id)
        )
        res = await self.session.execute(stmt)
        return res.scalar_one_or_none() is not None

    async def renew_lease(
        self,
        job_id: uuid.UUID,
        worker_id: str,
        expected_version: int,
        extension_seconds: int,
        now: datetime | None = None,
    ) -> bool:
        """Extend lease duration for a running job (heartbeat mechanism)."""
        current_time = now or datetime.now(UTC)
        new_expiry = current_time + timedelta(seconds=extension_seconds)

        stmt = (
            update(Job)
            .where(
                Job.id == job_id,
                Job.lease_owner == worker_id,
                Job.version == expected_version,
                Job.status == JobStatus.RUNNING.value,
            )
            .values(
                lease_expires_at=new_expiry,
                updated_at=current_time,
            )
            .returning(Job.id)
        )
        res = await self.session.execute(stmt)
        return res.scalar_one_or_none() is not None

    async def get_expired_leases(
        self,
        now: datetime | None = None,
        limit: int = 50,
    ) -> list[Job]:
        """Find RUNNING jobs whose lease has expired using row locking."""
        current_time = now or datetime.now(UTC)
        stmt = (
            select(Job)
            .where(
                Job.status == JobStatus.RUNNING.value,
                Job.lease_expires_at < current_time,
            )
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        res = await self.session.execute(stmt)
        return list(res.scalars().all())

    async def get_stuck_queued_jobs(
        self,
        cutoff: datetime,
        limit: int = 50,
    ) -> list[Job]:
        """Find QUEUED jobs not claimed within the reconciliation window."""
        stmt = (
            select(Job)
            .where(
                Job.status == JobStatus.QUEUED.value,
                Job.updated_at < cutoff,
            )
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        res = await self.session.execute(stmt)
        return list(res.scalars().all())
