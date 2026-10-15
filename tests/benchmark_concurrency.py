import asyncio
import time
from statistics import mean, median

from app.db.session import close_db_engine, get_session_factory
from app.models.job import JobStatus
from app.queue.redis_queue import close_redis_client, get_redis_client
from app.repositories.job_repository import JobRepository
from app.schemas.job import JobCreateRequest
from app.services.job_service import JobService
from app.workers.worker import WorkerProcess


async def run_benchmark(job_count: int = 100, worker_count: int = 4) -> dict:
    """Submits `job_count` jobs and executes them concurrently using `worker_count` worker tasks,

    measuring actual end-to-end throughput and latency.
    """
    session_factory = get_session_factory()
    redis = get_redis_client()

    print(f"\n--- Starting Benchmark: {job_count} Jobs with {worker_count} Concurrent Workers ---")

    # Step 1: Submit jobs
    t0_submit = time.perf_counter()
    job_ids = []

    async with session_factory() as session:
        service = JobService(session, redis)
        for i in range(job_count):
            req = JobCreateRequest(
                job_type="math.calculate",
                payload={"operation": "sum", "operands": [i, i * 2, 42]},
                priority=1 + (i % 100),
            )
            job = await service.submit_job(req)
            job_ids.append(job.id)

    t_submit = time.perf_counter() - t0_submit
    submit_throughput = job_count / t_submit
    print(
        f"Submission complete: {job_count} jobs submitted in {t_submit:.3f}s ({submit_throughput:.1f} jobs/sec)"
    )

    # Step 2: Workers process jobs
    workers = [
        WorkerProcess(
            worker_id=f"bench-worker-{i}",
            session_factory=session_factory,
            redis_client=redis,
        )
        for i in range(worker_count)
    ]

    t0_exec = time.perf_counter()

    async def worker_loop(w: WorkerProcess):
        while True:
            job_id = await w.fetch_next_job_id()
            if not job_id:
                break
            await w.process_job(job_id)

    await asyncio.gather(*[worker_loop(w) for w in workers])
    t_exec = time.perf_counter() - t0_exec
    exec_throughput = job_count / t_exec
    print(
        f"Execution complete: {job_count} jobs executed in {t_exec:.3f}s ({exec_throughput:.1f} jobs/sec)"
    )

    # Step 3: Compute latency metrics from database attempts
    async with session_factory() as session:
        repo = JobRepository(session)
        durations = []
        for jid in job_ids:
            job = await repo.get_by_id(jid)
            assert job.status == JobStatus.SUCCEEDED.value
            attempts = await repo.get_attempts(jid)
            assert len(attempts) == 1
            if attempts[0].execution_duration_seconds:
                durations.append(attempts[0].execution_duration_seconds * 1000.0)

    durations.sort()
    p50 = median(durations)
    p95 = durations[int(len(durations) * 0.95)]
    p99 = durations[int(len(durations) * 0.99)]
    avg_dur = mean(durations)

    results = {
        "job_count": job_count,
        "worker_count": worker_count,
        "submission_time_seconds": round(t_submit, 4),
        "submission_throughput_jobs_sec": round(submit_throughput, 1),
        "execution_time_seconds": round(t_exec, 4),
        "execution_throughput_jobs_sec": round(exec_throughput, 1),
        "handler_duration_p50_ms": round(p50, 2),
        "handler_duration_p95_ms": round(p95, 2),
        "handler_duration_p99_ms": round(p99, 2),
        "handler_duration_avg_ms": round(avg_dur, 2),
    }

    print("\n--- Verified Benchmark Results ---")
    for k, v in results.items():
        print(f"  {k}: {v}")
    print("----------------------------------\n")

    await close_db_engine()
    await close_redis_client()
    return results


if __name__ == "__main__":
    asyncio.run(run_benchmark())
