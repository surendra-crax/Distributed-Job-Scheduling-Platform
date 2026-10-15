# REST API Reference & Examples

All endpoints are versioned under `/api/v1` except for health diagnostics. Interactive Swagger documentation is available at `http://localhost:8000/docs`.

---

## 1. Health Diagnostics

### 1.1 Liveness Probe
```bash
curl -X GET http://localhost:8000/health/live
```
**Response (200 OK)**:
```json
{
  "status": "alive",
  "timestamp": "2026-10-15T14:30:00.123456Z",
  "app_name": "Distributed Job Scheduling Platform",
  "environment": "development"
}
```

### 1.2 Readiness Probe
```bash
curl -X GET http://localhost:8000/health/ready
```
**Response (200 OK)**:
```json
{
  "status": "ready",
  "timestamp": "2026-10-15T14:30:01.554321Z",
  "checks": {
    "database": {
      "status": "healthy",
      "latency_ms": 1.25,
      "error": null
    },
    "redis": {
      "status": "healthy",
      "latency_ms": 0.85,
      "error": null
    }
  }
}
```

---

## 2. Job Operations

### 2.1 Submit Immediate Job (Calculation)
```bash
curl -X POST http://localhost:8000/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "job_type": "math.calculate",
    "payload": {
      "operation": "sum",
      "operands": [10, 20, 30, 40]
    },
    "priority": 80,
    "max_attempts": 3,
    "timeout_seconds": 60
  }'
```
**Response (201 Created)**:
```json
{
  "id": "c1f73b64-8488-4c6e-b3f4-30113c41ef0a",
  "job_type": "math.calculate",
  "payload": {
    "operation": "sum",
    "operands": [10, 20, 30, 40]
  },
  "priority": 80,
  "status": "QUEUED",
  "scheduled_at": "2026-10-15T14:35:00.123456Z",
  "created_at": "2026-10-15T14:35:00.123456Z",
  "updated_at": "2026-10-15T14:35:00.123456Z",
  "started_at": null,
  "completed_at": null,
  "attempt_count": 0,
  "max_attempts": 3,
  "timeout_seconds": 60,
  "result": null,
  "last_error": null,
  "lease_owner": null,
  "lease_expires_at": null,
  "version": 1
}
```

### 2.2 Submit Future Scheduled Job
```bash
curl -X POST http://localhost:8000/api/v1/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "job_type": "notification.simulate",
    "payload": {
      "recipient": "user@example.com",
      "channel": "email",
      "template": "reminder_notice"
    },
    "priority": 50,
    "scheduled_at": "2026-10-15T16:00:00Z"
  }'
```
**Response (201 Created)**:
```json
{
  "id": "e4a2d815-5321-4f11-92bb-92718274a10f",
  "job_type": "notification.simulate",
  "payload": {
    "recipient": "user@example.com",
    "channel": "email",
    "template": "reminder_notice"
  },
  "priority": 50,
  "status": "SCHEDULED",
  "scheduled_at": "2026-10-15T16:00:00Z",
  "version": 1
}
```

### 2.3 Retrieve Job Details
```bash
curl -X GET http://localhost:8000/api/v1/jobs/c1f73b64-8488-4c6e-b3f4-30113c41ef0a
```
**Response (200 OK after completion)**:
```json
{
  "id": "c1f73b64-8488-4c6e-b3f4-30113c41ef0a",
  "job_type": "math.calculate",
  "payload": {
    "operation": "sum",
    "operands": [10, 20, 30, 40]
  },
  "priority": 80,
  "status": "SUCCEEDED",
  "scheduled_at": "2026-10-15T14:35:00.123456Z",
  "created_at": "2026-10-15T14:35:00.123456Z",
  "updated_at": "2026-10-15T14:35:00.180000Z",
  "started_at": "2026-10-15T14:35:00.140000Z",
  "completed_at": "2026-10-15T14:35:00.180000Z",
  "attempt_count": 1,
  "max_attempts": 3,
  "timeout_seconds": 60,
  "result": {
    "operation": "sum",
    "result": 100,
    "duration_ms": 1.45,
    "operands_count": 4
  },
  "last_error": null,
  "lease_owner": null,
  "lease_expires_at": null,
  "version": 3
}
```

### 2.4 List Jobs with Pagination and Filters
```bash
curl -X GET "http://localhost:8000/api/v1/jobs?status=SUCCEEDED&limit=10&offset=0"
```
**Response (200 OK)**:
```json
{
  "items": [
    {
      "id": "c1f73b64-8488-4c6e-b3f4-30113c41ef0a",
      "job_type": "math.calculate",
      "status": "SUCCEEDED",
      "priority": 80
    }
  ],
  "total": 1,
  "limit": 10,
  "offset": 0
}
```

### 2.5 Cancel a Job
```bash
curl -X POST http://localhost:8000/api/v1/jobs/e4a2d815-5321-4f11-92bb-92718274a10f/cancel
```
**Response (200 OK)**:
```json
{
  "job_id": "e4a2d815-5321-4f11-92bb-92718274a10f",
  "previous_status": "SCHEDULED",
  "new_status": "CANCELLED",
  "message": "Job was successfully transitioned from SCHEDULED to CANCELLED"
}
```

### 2.6 Retry a Job
```bash
curl -X POST http://localhost:8000/api/v1/jobs/e4a2d815-5321-4f11-92bb-92718274a10f/retry
```
**Response (200 OK)**:
```json
{
  "job_id": "e4a2d815-5321-4f11-92bb-92718274a10f",
  "previous_status": "FAILED",
  "new_status": "QUEUED",
  "scheduled_at": "2026-10-15T14:40:12.345678Z",
  "message": "Job e4a2d815-5321-4f11-92bb-92718274a10f successfully enqueued for retry"
}
```

### 2.7 Get Job Execution Attempt History
```bash
curl -X GET http://localhost:8000/api/v1/jobs/c1f73b64-8488-4c6e-b3f4-30113c41ef0a/attempts
```
**Response (200 OK)**:
```json
[
  {
    "id": "771e8601-529a-4712-a7d0-c3d5268390ba",
    "job_id": "c1f73b64-8488-4c6e-b3f4-30113c41ef0a",
    "worker_id": "worker-4f81a9",
    "attempt_number": 1,
    "started_at": "2026-10-15T14:35:00.140000Z",
    "completed_at": "2026-10-15T14:35:00.180000Z",
    "outcome": "SUCCEEDED",
    "error_details": null,
    "execution_duration_seconds": 0.04
  }
]
```

### 2.8 Operational Metrics Summary
```bash
curl -X GET http://localhost:8000/api/v1/metrics/summary
```
**Response (200 OK)**:
```json
{
  "timestamp": "2026-10-15T14:45:00.000000Z",
  "jobs_by_status": {
    "SCHEDULED": 2,
    "QUEUED": 0,
    "RUNNING": 4,
    "SUCCEEDED": 1240,
    "FAILED": 12,
    "RETRY_WAIT": 3,
    "CANCELLED": 5
  },
  "redis_queue_depth": 0,
  "total_attempts": 1285,
  "attempts_by_outcome": {
    "RUNNING": 4,
    "SUCCEEDED": 1240,
    "FAILED": 25,
    "TIMED_OUT": 11,
    "CANCELLED": 5
  },
  "avg_execution_duration_seconds": 0.0125
}
```
