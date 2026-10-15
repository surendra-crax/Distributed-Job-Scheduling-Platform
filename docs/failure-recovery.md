# Failure Modes & Crash Recovery Guide

This document details the recovery mechanisms, reconciliation flows, and failure handling policies implemented in the Distributed Job Scheduling Platform.

---

## 1. Failure Scenarios & Self-Healing Behaviors

### 1.1 PostgreSQL Unavailable
- **Symptoms**: Database connection attempts fail or connection pool timeouts occur.
- **System Behavior**:
  - API Readiness Probe (`/health/ready`) immediately returns HTTP 503 (`Service Unavailable`), prompting load balancers to redirect traffic.
  - API submission endpoints reject incoming write requests with 500/503 errors.
  - Schedulers and Workers back off with retry intervals to prevent connection storms.
- **Recovery**: Once PostgreSQL restarts, connection pool pre-ping restores active connections without process restarts. No committed data is ever lost.

---

### 1.2 Redis Unavailable / Data Loss
- **Symptoms**: Redis connection timeouts or connection refused errors.
- **System Behavior**:
  - API submissions persist the job durably in PostgreSQL with `status = QUEUED`, but catch the Redis publication exception and log a warning.
  - The job is **not lost**.
- **Reconciliation Procedure**:
  - The `RecoveryService` runs every 30 seconds scanning for jobs in PostgreSQL with `status = QUEUED` that are missing from Redis.
  - When Redis becomes available, the reconciler re-publishes all orphaned job IDs into the Redis ZSET ready queue with their respective priority scores.

---

### 1.3 Worker Process Crashes Mid-Execution
- **Symptoms**: A worker process is terminated via `SIGKILL`, hardware failure, or container host crash while executing a job.
- **System Behavior**:
  - The job remains in PostgreSQL with `status = RUNNING`, owned by the crashed worker's ID.
  - Because the crashed worker stops sending heartbeat lease extensions, the job's `lease_expires_at` timestamp lapses.
- **Recovery Procedure**:
  - The `RecoveryService` lease sweeper discovers `status = RUNNING AND lease_expires_at < NOW()`.
  - The sweeper closes the open attempt in `job_attempts` as `FAILED` with error `WorkerLeaseExpired`.
  - If `attempt_count < max_attempts`, the job is transitioned to `RETRY_WAIT` with exponential backoff delay and an advanced version token.
  - If `attempt_count >= max_attempts`, the job is transitioned to `FAILED`.

---

### 1.4 Duplicate Queue Delivery (At-Least-Once Delivery)
- **Symptoms**: Network re-transmissions or broker re-deliveries cause the same `job_id` to be popped by two different workers (Worker 1 and Worker 2) almost simultaneously.
- **System Behavior**:
  - Worker 1 executes:
    ```sql
    UPDATE jobs SET status = 'RUNNING', lease_owner = 'worker-1', version = version + 1
    WHERE id = :job_id AND status = 'QUEUED';
    ```
    This updates 1 row and succeeds.
  - Worker 2 executes the same query moments later. Because `status` is now `RUNNING` (no longer `QUEUED`), the update matches 0 rows.
  - Worker 2 receives `None` and discards the duplicate message cleanly without side effects or errors.

---

### 1.5 Stale Worker Tries to Save Result After Lease Expiration
- **Symptoms**: Worker 1 experiences a 45-second network pause. Its 30-second lease expires. Recovery transitions the job to `RETRY_WAIT` (version increments from 2 to 3). Worker 1 unfreezes and attempts to mark the job as `SUCCEEDED`.
- **System Behavior**:
  - Worker 1 submits an update requiring `version = 2` (its acquired version).
  - Because `version` is now 3, the update matches 0 rows.
  - Worker 1 catches the fencing token mismatch, logs `stale_worker_fencing_token_rejected`, and aborts without overwriting the database.

---

### 1.6 Job Execution Exceeds Configured Timeout
- **Symptoms**: A task hangs or enters an infinite loop.
- **System Behavior**:
  - The worker execution harness wraps handler execution in `asyncio.wait_for(..., timeout=timeout_seconds)`.
  - When the timeout elapses, `asyncio.TimeoutError` is raised.
  - The worker records the attempt outcome as `TIMED_OUT`.
  - If attempts remain, the job transitions to `RETRY_WAIT` with backoff; otherwise, it transitions to `FAILED`.

---

### 1.7 Client Cancels a Job
- **Case A: Job is `QUEUED` or `SCHEDULED`**:
  - Job status is immediately transitioned to `CANCELLED` in PostgreSQL and removed from the Redis buffer.
- **Case B: Job is already `RUNNING`**:
  - The API broadcasts a cancellation message to Redis channel `scheduler:cancellations:{job_id}`.
  - The running worker's background listener triggers `cancellation_token.cancel()`.
  - The handler periodically checks `context.cancellation_token.raise_if_cancelled()` and aborts cleanly.
  - The worker records the attempt outcome as `CANCELLED` and transitions the job to `CANCELLED`.
- **Case C: Job is already `SUCCEEDED`**:
  - The API returns HTTP 409 Conflict: completed jobs cannot be cancelled.

---

## 2. Why Exactly-Once Execution Cannot Be Guaranteed

In any distributed system subject to network unreliability, physical host crashes, and timeouts:

> **The Two Generals' Problem proves that strictly guaranteed exactly-once processing with external side effects is impossible without distributed two-phase consensus on every side effect.**

If a worker executes an external API call (e.g. charging a payment gateway or sending an SMS) and crashes *before* committing the database transaction, the recovery daemon will re-execute the job upon lease expiration.

### Recommended Best Practices:
1. **Idempotency Keys**: For handlers communicating with external APIs, include the deterministic attempt ID (`msg_{job_id}_{attempt_number}`) as an idempotency key.
2. **Read-Modify-Write Transactions**: For internal database operations, rely on unique constraints and optimistic version checks.
3. **Deterministic Handlers**: Keep calculations and report generation pure and reproducible.
