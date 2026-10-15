# Architecture Specification: Distributed Job Scheduling Platform

## 1. Architectural Philosophy & Single Source of Truth

The Distributed Job Scheduling Platform is built upon the fundamental distributed systems principle of **dual-store decoupled coordination with an authoritative durable source of truth**:

1. **Authoritative Truth (PostgreSQL)**:
   - Stores all job definitions, payloads, scheduling metadata, state transitions, version fencing tokens, execution leases, and attempt audit logs.
   - Guaranteed ACID transactions prevent race conditions during state transitions.
   - Jobs are never considered successfully submitted or transitioned until written and committed to PostgreSQL.

2. **Ephemeral Fast Coordination Buffer (Redis)**:
   - Redis functions as a lightweight, low-latency dispatch and push notification buffer.
   - Prevents worker starvation and eliminates idle database polling by thousands of concurrent workers.
   - Redis data is ephemeral: if Redis crashes, restarts, or evicts keys, the system self-heals by reconciling missing state directly from PostgreSQL.

```mermaid
flowchart TD
    Client([HTTP Client]) -->|REST API Requests| API[FastAPI Web Service]
    API -->|1. Commit Job (status=QUEUED)| PG[(PostgreSQL Authoritative DB)]
    API -->|2. Buffer Job ID| RedisQueue[(Redis Ready Queue ZSET)]

    subgraph Schedulers [Scheduler Instances]
        Sched1[Scheduler Daemon 1]
        Sched2[Scheduler Daemon 2]
    end

    Sched1 & Sched2 -->|Poll Due Jobs with FOR UPDATE SKIP LOCKED| PG
    Sched1 & Sched2 -->|Push Due Job IDs| RedisQueue

    subgraph Workers [Worker Process Pool]
        W1[Worker Process 1]
        W2[Worker Process 2]
        WN[Worker Process N]
    end

    RedisQueue -->|ZPOPMIN (Priority Fetch)| W1 & W2 & WN
    W1 & W2 & WN -->|Atomic Lease Claim with Fencing Token| PG
    W1 & W2 & WN -->|Periodic Lease Renewal Heartbeats| PG
    W1 & W2 & WN -->|Write Final Result or Failure| PG

    subgraph Recovery [Self-Healing Daemons]
        Reaper[Lease Sweeper & Reconciler]
    end

    Reaper -->|Reclaim Expired Leases| PG
    Reaper -->|Reconcile Desynced Jobs| RedisQueue
```

---

## 2. Core Components & Responsibilities

### 2.1 API Service (`app/api/`)
- Accepts HTTP requests adhering to OpenAPI specifications.
- Validates job types and payloads against registered Pydantic schemas before writing to the database.
- Immediately enqueues jobs scheduled for immediate execution to Redis.
- Exposes lifecycle management (cancellation, manual retries, attempt logs) and health probes (`/health/live`, `/health/ready`).

### 2.2 Database Layer (`app/db/` & `app/models/`)
- Implements two primary relational entities:
  - `Job`: Durable state, configuration parameters, timing, leasing information, and fencing token version.
  - `JobAttempt`: Immutable audit log of every execution run, worker identifier, duration, and outcome.
- Utilizes specialized compound indexes to optimize queries for due-job polling, lease recovery, and pagination.

### 2.3 Scheduler Daemon (`app/scheduler/`)
- Executes periodic polling ticks (default: every 1.0 second).
- Scans for jobs in `SCHEDULED` or `RETRY_WAIT` where `scheduled_at <= NOW()`.
- Uses PostgreSQL row-level locking (`SELECT ... FOR UPDATE SKIP LOCKED`) to claim due jobs in batches and atomically transition them to `QUEUED`.
- Dispatches claimed job IDs to Redis priority queues.

### 2.4 Worker Pool (`app/workers/worker.py`)
- Autonomous worker processes running configurable concurrency slots.
- Pops job IDs from the Redis priority ZSET.
- Atomically claims execution leases in PostgreSQL using conditional updates with fencing tokens.
- Executes trusted job handlers under an execution harness that enforces timeouts and cooperative cancellation.
- Emits periodic heartbeats to extend leases for long-running jobs.
- Writes execution outcomes and attempt logs, validating that its fencing token has not been invalidated.

### 2.5 Recovery Daemon (`app/services/recovery_service.py`)
- Sweeps expired worker leases (`status = RUNNING` and `lease_expires_at < NOW()`).
- Evaluates remaining attempt count:
  - If attempts remain: schedules a retry with exponential backoff and jitter.
  - If attempts exhausted: marks the job as `FAILED`.
- Reconciles orphaned queue items: scans for jobs stuck in `QUEUED` in PostgreSQL that are missing from Redis and re-publishes them.

---

## 3. Concurrency Control & Fencing Tokens

### 3.1 The Stale Worker Problem
In distributed systems, a worker process may experience network partitions, garbage collection freezes, or unhandled delays. During this freeze, its lease expires in PostgreSQL, and the recovery daemon reassigns the job to a second worker. If the frozen worker suddenly awakens, it must not overwrite the state produced by the new worker.

### 3.2 Fencing Token Implementation
1. The `jobs` table maintains an integer `version` column initialized to 1.
2. Every state transition atomically increments `version = version + 1`.
3. When Worker 1 claims a job, it captures the current version (e.g., $V = 2$).
4. When writing completion:
   ```sql
   UPDATE jobs
   SET status = 'SUCCEEDED',
       result = :result,
       completed_at = :now,
       version = version + 1
   WHERE id = :job_id
     AND version = 2; -- Fencing check
   ```
5. If the lease expired and Recovery reassigned the job to Worker 2, the version was incremented to $V = 3$. Worker 1's update matches 0 rows. Worker 1 detects the fencing mismatch and cleanly rejects the write.

---

## 4. Scheduling Algorithm & Fairness Trade-offs

### Selection Logic
Due jobs are selected using deterministic ordering:
$$\text{ORDER BY priority DESC, scheduled_at ASC, created_at ASC, id ASC}$$

### Strict Priority vs. Starvation Prevention
- **Strict Priority**: High-priority jobs always take precedence. If a continuous deluge of priority 100 jobs arrives, priority 10 jobs could theoretically be delayed.
- **Fairness Mitigation**: In high-throughput multi-tenant deployments, aging heuristics can be introduced where a job's effective priority is boosted based on wait time:
  $$\text{Effective Priority} = \text{Base Priority} + \left( \frac{\text{Wait Seconds}}{\text{Aging Factor}} \right)$$
- The platform's tie-breaking by `scheduled_at ASC` and `created_at ASC` guarantees deterministic FIFO ordering within any identical priority tier.

---

## 5. Architectural Comparison: Why Not Celery?

| Dimension | Distributed Job Scheduling Platform | Traditional Celery Setup |
|:---|:---|:---|
| **Authoritative State** | PostgreSQL (ACID relational audit log) | Message Broker (Redis/RabbitMQ memory) |
| **Fencing Protection** | Database-enforced version fencing tokens | None; stale workers overwrite newer runs |
| **Scheduled Job Selection** | Row-level locking (`SKIP LOCKED`) across replicas | Celery Beat single-process SPOF |
| **Attempt Audit History** | First-class `JobAttempt` relational history | Ephemeral or requires separate Flower/monitoring |
| **Crash Recovery** | Background recovery daemon sweeps expired leases | Visibility timeout re-queuing (prone to duplicates) |
| **Dependency Footprint** | Pure Python + FastAPI + SQLAlchemy + Redis | Heavy framework abstraction & monkey patching |
