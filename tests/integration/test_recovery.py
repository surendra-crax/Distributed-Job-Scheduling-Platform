import uuid
from datetime import UTC, datetime, timedelta

import pytest

from app.db.session import get_session_factory
from app.models.attempt import AttemptOutcome, JobAttempt
from app.models.job import Job, JobStatus
from app.queue.redis_queue import get_redis_client
from app.repositories.job_repository import JobRepository
from app.services.recovery_service import RecoveryService
from app.workers.worker import calculate_backoff_delay


def test_calculate_backoff_delay_exponential_growth():
    """Verify backoff grows exponentially up to maximum cap."""
    b1 = calculate_backoff_delay(attempt_count=1, base_delay=2.0, max_delay=60.0, use_jitter=False)
    b2 = calculate_backoff_delay(attempt_count=2, base_delay=2.0, max_delay=60.0, use_jitter=False)
    b3 = calculate_backoff_delay(attempt_count=3, base_delay=2.0, max_delay=60.0, use_jitter=False)
    b10 = calculate_backoff_delay(
        attempt_count=10, base_delay=2.0, max_delay=60.0, use_jitter=False
    )

    assert b1 == 2.0
    assert b2 == 4.0
    assert b3 == 8.0
    assert b10 == 60.0  # Capped at max_delay

    # With jitter
    bj = calculate_backoff_delay(attempt_count=3, base_delay=2.0, max_delay=60.0, use_jitter=True)
    assert 0.5 <= bj <= 8.0


@pytest.mark.asyncio
async def test_recovery_sweeps_expired_lease_and_reschedules():
    """Worker crashes while executing; lease expires; recovery reschedules to RETRY_WAIT."""
    session_factory = get_session_factory()
    redis = get_redis_client()
    recovery = RecoveryService(session_factory=session_factory, redis_client=redis)

    now = datetime.now(UTC)
    expired_time = now - timedelta(seconds=30)
    job_id = uuid.uuid4()

    async with session_factory() as session:
        repo = JobRepository(session)
        # Create job currently RUNNING with expired lease
        job = Job(
            id=job_id,
            job_type="math.calculate",
            payload={"operation": "sum", "operands": [1, 2]},
            priority=50,
            status=JobStatus.RUNNING.value,
            scheduled_at=now,
            attempt_count=1,
            max_attempts=3,
            lease_owner="crashed-worker-99",
            lease_expires_at=expired_time,
            version=2,
        )
        session.add(job)
        # Open attempt
        attempt = JobAttempt(
            id=uuid.uuid4(),
            job_id=job_id,
            worker_id="crashed-worker-99",
            attempt_number=1,
            started_at=now - timedelta(minutes=1),
            outcome=AttemptOutcome.RUNNING.value,
        )
        session.add(attempt)
        await session.commit()

    # Run recovery sweep
    recovered = await recovery.sweep_expired_leases(now=now)
    assert recovered == 1

    # Verify job status transitioned to RETRY_WAIT and attempt marked FAILED
    async with session_factory() as session:
        repo = JobRepository(session)
        recovered_job = await repo.get_by_id(job_id)
        assert recovered_job.status == JobStatus.RETRY_WAIT.value
        assert recovered_job.lease_owner is None
        assert recovered_job.lease_expires_at is None
        assert recovered_job.version == 3  # Advanced fencing token

        attempts = await repo.get_attempts(job_id)
        assert len(attempts) == 1
        assert attempts[0].outcome == AttemptOutcome.FAILED.value
        assert attempts[0].error_details["error"] == "WorkerLeaseExpired"


@pytest.mark.asyncio
async def test_recovery_sweeps_expired_lease_and_marks_failed_when_max_attempts():
    """Worker crashes on attempt 3 of 3; recovery marks job FAILED."""
    session_factory = get_session_factory()
    redis = get_redis_client()
    recovery = RecoveryService(session_factory=session_factory, redis_client=redis)

    now = datetime.now(UTC)
    expired_time = now - timedelta(seconds=10)
    job_id = uuid.uuid4()

    async with session_factory() as session:
        job = Job(
            id=job_id,
            job_type="math.calculate",
            payload={"operation": "sum", "operands": [1, 2]},
            priority=50,
            status=JobStatus.RUNNING.value,
            scheduled_at=now,
            attempt_count=3,
            max_attempts=3,  # Max reached
            lease_owner="crashed-worker-last",
            lease_expires_at=expired_time,
            version=4,
        )
        session.add(job)
        await session.commit()

    recovered = await recovery.sweep_expired_leases(now=now)
    assert recovered == 1

    async with session_factory() as session:
        repo = JobRepository(session)
        final_job = await repo.get_by_id(job_id)
        assert final_job.status == JobStatus.FAILED.value
        assert final_job.version == 5


@pytest.mark.asyncio
async def test_recovery_reconciles_missing_redis_queued_jobs():
    """Job committed to PostgreSQL as QUEUED but missing from Redis is reconciled and enqueued."""
    session_factory = get_session_factory()
    redis = get_redis_client()
    recovery = RecoveryService(session_factory=session_factory, redis_client=redis)

    now = datetime.now(UTC)
    stuck_time = now - timedelta(minutes=2)
    job_id = uuid.uuid4()

    async with session_factory() as session:
        job = Job(
            id=job_id,
            job_type="math.calculate",
            payload={"operation": "sum", "operands": [5, 5]},
            priority=85,
            status=JobStatus.QUEUED.value,
            scheduled_at=stuck_time,
            updated_at=stuck_time,
            version=1,
        )
        session.add(job)
        await session.commit()

    # Ensure it is NOT in Redis
    await redis.zrem(recovery.settings.REDIS_JOB_QUEUE_KEY, str(job_id))
    score_before = await redis.zscore(recovery.settings.REDIS_JOB_QUEUE_KEY, str(job_id))
    assert score_before is None

    # Run reconciliation
    reconciled = await recovery.reconcile_lost_queue_jobs(now=now)
    assert reconciled >= 1

    # Verify now present in Redis with priority score
    score_after = await redis.zscore(recovery.settings.REDIS_JOB_QUEUE_KEY, str(job_id))
    assert score_after is not None
    # Negative priority (-85)
    assert score_after < -84.0
