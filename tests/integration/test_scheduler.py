import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from app.db.session import get_session_factory
from app.models.job import Job, JobStatus
from app.queue.redis_queue import get_redis_client
from app.repositories.job_repository import JobRepository
from app.scheduler.scheduler import SchedulerDaemon


@pytest.mark.asyncio
async def test_scheduler_due_job_selection_and_timing():
    """Verify scheduler claims due jobs and ignores future scheduled jobs."""
    session_factory = get_session_factory()
    redis = get_redis_client()
    scheduler = SchedulerDaemon(session_factory=session_factory, redis_client=redis)

    now = datetime.now(UTC)
    past_time = now - timedelta(minutes=5)
    future_time = now + timedelta(hours=1)

    async with session_factory() as session:
        # 1. Insert due job
        due_job = Job(
            id=uuid.uuid4(),
            job_type="math.calculate",
            payload={"operation": "sum", "operands": [1, 2]},
            priority=50,
            status=JobStatus.SCHEDULED.value,
            scheduled_at=past_time,
            version=1,
        )
        # 2. Insert future job
        future_job = Job(
            id=uuid.uuid4(),
            job_type="math.calculate",
            payload={"operation": "sum", "operands": [3, 4]},
            priority=80,
            status=JobStatus.SCHEDULED.value,
            scheduled_at=future_time,
            version=1,
        )
        session.add_all([due_job, future_job])
        await session.commit()

    # Run scheduler tick
    dispatched = await scheduler.tick(now=now)
    assert dispatched >= 1

    # Verify states
    async with session_factory() as session:
        repo = JobRepository(session)
        updated_due = await repo.get_by_id(due_job.id)
        assert updated_due.status == JobStatus.QUEUED.value

        updated_future = await repo.get_by_id(future_job.id)
        assert updated_future.status == JobStatus.SCHEDULED.value  # Unchanged!


@pytest.mark.asyncio
async def test_scheduler_priority_ordering():
    """Verify higher priority jobs are selected before lower priority ones."""
    session_factory = get_session_factory()
    redis = get_redis_client()
    scheduler = SchedulerDaemon(session_factory=session_factory, redis_client=redis, batch_size=1)

    now = datetime.now(UTC)
    base_time = now - timedelta(minutes=10)

    # Insert low priority and high priority jobs
    async with session_factory() as session:
        low_prio = Job(
            id=uuid.uuid4(),
            job_type="math.calculate",
            payload={"operation": "sum", "operands": [1]},
            priority=10,
            status=JobStatus.SCHEDULED.value,
            scheduled_at=base_time,
            version=1,
        )
        high_prio = Job(
            id=uuid.uuid4(),
            job_type="math.calculate",
            payload={"operation": "sum", "operands": [2]},
            priority=99,
            status=JobStatus.SCHEDULED.value,
            scheduled_at=base_time,
            version=1,
        )
        session.add_all([low_prio, high_prio])
        await session.commit()

    # Batch size is 1, so the single selected job must be high_prio
    dispatched = await scheduler.tick(now=now)
    assert dispatched == 1

    async with session_factory() as session:
        repo = JobRepository(session)
        hp = await repo.get_by_id(high_prio.id)
        lp = await repo.get_by_id(low_prio.id)

        assert hp.status == JobStatus.QUEUED.value
        assert lp.status == JobStatus.SCHEDULED.value


@pytest.mark.asyncio
async def test_concurrent_schedulers_no_duplicate_claims():
    """Simulate 3 concurrent scheduler daemons contending for the same pool of due jobs."""
    session_factory = get_session_factory()
    redis = get_redis_client()

    now = datetime.now(UTC) - timedelta(minutes=2)
    job_ids = [uuid.uuid4() for _ in range(15)]

    async with session_factory() as session:
        for jid in job_ids:
            job = Job(
                id=jid,
                job_type="math.calculate",
                payload={"operation": "sum", "operands": [1, 2]},
                priority=50,
                status=JobStatus.SCHEDULED.value,
                scheduled_at=now,
                version=1,
            )
            session.add(job)
        await session.commit()

    # Create 3 independent scheduler instances
    schedulers = [
        SchedulerDaemon(session_factory=session_factory, redis_client=redis, batch_size=5)
        for _ in range(3)
    ]

    # Run tick concurrently across all 3 schedulers
    results = await asyncio.gather(
        schedulers[0].tick(),
        schedulers[1].tick(),
        schedulers[2].tick(),
    )

    total_dispatched = sum(results)
    assert total_dispatched >= 15

    # Verify that all 15 jobs are QUEUED and none had version collision or duplicate claim
    async with session_factory() as session:
        repo = JobRepository(session)
        for jid in job_ids:
            job = await repo.get_by_id(jid)
            assert job.status == JobStatus.QUEUED.value
            assert job.version == 2  # Incremented exactly once from 1 to 2!
