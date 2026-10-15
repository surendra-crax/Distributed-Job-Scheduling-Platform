import asyncio
import uuid
from datetime import UTC, datetime

import pytest

from app.db.session import get_session_factory
from app.models.attempt import AttemptOutcome
from app.models.job import Job, JobStatus
from app.queue.redis_queue import get_redis_client
from app.repositories.job_repository import JobRepository
from app.schemas.job import JobCreateRequest
from app.services.job_service import JobService
from app.workers.worker import WorkerProcess


@pytest.mark.asyncio
async def test_worker_end_to_end_job_execution():
    """Verify full lifecycle: submission -> enqueue -> worker claim -> execution -> SUCCEEDED with attempt record."""
    session_factory = get_session_factory()
    redis = get_redis_client()

    # 1. Submit immediate math job
    async with session_factory() as session:
        service = JobService(session, redis)
        job = await service.submit_job(
            JobCreateRequest(
                job_type="math.calculate",
                payload={"operation": "multiply", "operands": [4, 5, 2]},
                priority=60,
            )
        )
        job_id = job.id

    # 2. Worker pops and processes
    worker = WorkerProcess(
        worker_id="test-worker-alpha", session_factory=session_factory, redis_client=redis
    )
    processed = await worker.process_job(job_id)
    assert processed is True

    # 3. Verify final state and attempts
    async with session_factory() as session:
        repo = JobRepository(session)
        finished_job = await repo.get_by_id(job_id)

        assert finished_job.status == JobStatus.SUCCEEDED.value
        assert finished_job.result == {
            "operation": "multiply",
            "result": 40,
            "duration_ms": finished_job.result["duration_ms"],
            "operands_count": 3,
        }
        assert finished_job.completed_at is not None
        assert finished_job.lease_owner is None
        assert finished_job.lease_expires_at is None
        assert finished_job.version == 3  # 1 (create) -> 2 (claim) -> 3 (finish)

        attempts = await repo.get_attempts(job_id)
        assert len(attempts) == 1
        assert attempts[0].outcome == AttemptOutcome.SUCCEEDED.value
        assert attempts[0].worker_id == "test-worker-alpha"
        assert attempts[0].execution_duration_seconds > 0.0


@pytest.mark.asyncio
async def test_concurrent_workers_safe_lease_claiming():
    """Two workers attempt to claim the exact same QUEUED job concurrently.

    Only one must succeed; the other must receive None and drop cleanly.
    """
    session_factory = get_session_factory()
    redis = get_redis_client()

    job_id = uuid.uuid4()
    async with session_factory() as session:
        repo = JobRepository(session)
        job = Job(
            id=job_id,
            job_type="math.calculate",
            payload={"operation": "sum", "operands": [1, 2]},
            priority=50,
            status=JobStatus.QUEUED.value,
            scheduled_at=datetime.now(UTC),
            version=1,
        )
        await repo.create(job)
        await session.commit()

    worker_a = WorkerProcess(
        worker_id="worker-A", session_factory=session_factory, redis_client=redis
    )
    worker_b = WorkerProcess(
        worker_id="worker-B", session_factory=session_factory, redis_client=redis
    )

    # Both process_job calls executed concurrently
    res_a, res_b = await asyncio.gather(
        worker_a.process_job(job_id),
        worker_b.process_job(job_id),
    )

    # Exactly one succeeded in processing the job
    assert (res_a is True and res_b is False) or (res_a is False and res_b is True)

    async with session_factory() as session:
        repo = JobRepository(session)
        final_job = await repo.get_by_id(job_id)
        assert final_job.status == JobStatus.SUCCEEDED.value
        assert final_job.attempt_count == 1  # Executed only once!


@pytest.mark.asyncio
async def test_worker_execution_timeout():
    """Verify job exceeding timeout is aborted and marked TIMED_OUT / RETRY_WAIT."""
    session_factory = get_session_factory()
    redis = get_redis_client()

    job_id = uuid.uuid4()
    async with session_factory() as session:
        repo = JobRepository(session)
        job = Job(
            id=job_id,
            job_type="report.generate",
            payload={"report_type": "sales_summary", "simulated_duration_ms": 2000},
            priority=50,
            status=JobStatus.QUEUED.value,
            scheduled_at=datetime.now(UTC),
            timeout_seconds=1,  # Strict 1-second timeout
            max_attempts=3,
            version=1,
        )
        await repo.create(job)
        await session.commit()

    worker = WorkerProcess(
        worker_id="worker-timeout-tester", session_factory=session_factory, redis_client=redis
    )
    await worker.process_job(job_id)

    async with session_factory() as session:
        repo = JobRepository(session)
        timed_out_job = await repo.get_by_id(job_id)
        # Attempt 1 of 3: transitions to RETRY_WAIT with backoff
        assert timed_out_job.status == JobStatus.RETRY_WAIT.value
        assert timed_out_job.last_error["error"] == "ExecutionTimeout"

        attempts = await repo.get_attempts(job_id)
        assert len(attempts) == 1
        assert attempts[0].outcome == AttemptOutcome.TIMED_OUT.value


@pytest.mark.asyncio
async def test_stale_worker_fencing_token_rejected():
    """Worker A acquires claim (version=2), but before Worker A writes results,

    the version is bumped to 3 (simulating recovery re-lease to Worker B).
    Worker A's finish call must be rejected to prevent stale data overwrite.
    """
    session_factory = get_session_factory()
    now = datetime.now(UTC)
    job_id = uuid.uuid4()

    async with session_factory() as session:
        repo = JobRepository(session)
        job = Job(
            id=job_id,
            job_type="math.calculate",
            payload={"operation": "sum", "operands": [10, 20]},
            priority=50,
            status=JobStatus.QUEUED.value,
            scheduled_at=now,
            version=1,
        )
        await repo.create(job)
        await session.commit()

        # Worker A claims job: status -> RUNNING, version -> 2
        claimed = await repo.atomic_worker_claim(job_id, "worker-A", timeout_seconds=60, now=now)
        assert claimed is not None
        assert claimed.version == 2
        await session.commit()

        # Simulate Recovery bumping version to 3 (releasing or re-leasing)
        job.version = 3
        await session.commit()

        # Worker A tries to persist result with its stale expected_version=2
        saved = await repo.atomic_finish_job(
            job_id=job_id,
            expected_version=2,  # Stale token!
            status=JobStatus.SUCCEEDED,
            result={"result": 30},
            now=now,
        )
        assert saved is False  # Rejected by fencing token!
