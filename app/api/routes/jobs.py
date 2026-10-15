import uuid
from datetime import datetime

import redis.asyncio as aioredis
from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.dependencies import get_db, get_redis
from app.exceptions.errors import (
    InvalidPayloadError,
    InvalidStateTransitionError,
    JobNotFoundError,
    UnknownJobTypeError,
)
from app.models.job import JobStatus
from app.schemas.attempt import JobAttemptResponse
from app.schemas.job import (
    JobCancelResponse,
    JobCreateRequest,
    JobListResponse,
    JobResponse,
    JobRetryResponse,
)
from app.services.job_service import JobService

router = APIRouter(prefix="/api/v1/jobs", tags=["Jobs"])


@router.post(
    "",
    response_model=JobResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Submit a job",
    description="Submit a new job for immediate or scheduled execution with priority and payload validation.",
)
async def submit_job(
    request: JobCreateRequest,
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
) -> JobResponse:
    service = JobService(db, redis)
    try:
        job = await service.submit_job(request)
        return JobResponse.model_validate(job)
    except UnknownJobTypeError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={"error": "UnknownJobType", "message": exc.message, "details": exc.details},
        ) from exc
    except InvalidPayloadError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={"error": "InvalidPayload", "message": exc.message, "details": exc.details},
        ) from exc


@router.get(
    "/{job_id}",
    response_model=JobResponse,
    summary="Get job by ID",
    description="Retrieve the current state, lease metadata, results, or error details of a job.",
)
async def get_job(
    job_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
) -> JobResponse:
    service = JobService(db, redis)
    try:
        job = await service.get_job(job_id)
        return JobResponse.model_validate(job)
    except JobNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "JobNotFound", "message": exc.message, "details": exc.details},
        ) from exc


@router.get(
    "",
    response_model=JobListResponse,
    summary="List jobs",
    description="Query jobs with optional status, job_type, and creation date filters with pagination.",
)
async def list_jobs(
    status_filter: JobStatus | None = Query(
        default=None, alias="status", description="Filter by status"
    ),
    job_type: str | None = Query(default=None, description="Filter by job type identifier"),
    created_after: datetime | None = Query(
        default=None, description="Filter jobs created after this timestamp"
    ),
    created_before: datetime | None = Query(
        default=None, description="Filter jobs created before this timestamp"
    ),
    limit: int = Query(default=20, ge=1, le=100, description="Page limit"),
    offset: int = Query(default=0, ge=0, description="Page offset"),
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
) -> JobListResponse:
    service = JobService(db, redis)
    items, total = await service.list_jobs(
        status=status_filter,
        job_type=job_type,
        created_after=created_after,
        created_before=created_before,
        limit=limit,
        offset=offset,
    )
    return JobListResponse(
        items=[JobResponse.model_validate(item) for item in items],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.post(
    "/{job_id}/cancel",
    response_model=JobCancelResponse,
    summary="Cancel a job",
    description="Cancel a job if its current state permits cancellation.",
)
async def cancel_job(
    job_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
) -> JobCancelResponse:
    service = JobService(db, redis)
    try:
        prev, new_st, msg = await service.cancel_job(job_id)
        return JobCancelResponse(
            job_id=job_id,
            previous_status=prev,
            new_status=new_st,
            message=msg,
        )
    except JobNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "JobNotFound", "message": exc.message, "details": exc.details},
        ) from exc
    except InvalidStateTransitionError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "InvalidStateTransition",
                "message": exc.message,
                "details": exc.details,
            },
        ) from exc


@router.post(
    "/{job_id}/retry",
    response_model=JobRetryResponse,
    summary="Retry a job",
    description="Manually retry a failed or cancelled job according to retry policies.",
)
async def retry_job(
    job_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
) -> JobRetryResponse:
    service = JobService(db, redis)
    try:
        job = await service.retry_job(job_id)
        return JobRetryResponse(
            job_id=job.id,
            previous_status=JobStatus.FAILED,  # Indicative
            new_status=JobStatus(job.status),
            scheduled_at=job.scheduled_at,
            message=f"Job {job.id} successfully enqueued for retry",
        )
    except JobNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "JobNotFound", "message": exc.message, "details": exc.details},
        ) from exc
    except InvalidStateTransitionError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "InvalidStateTransition",
                "message": exc.message,
                "details": exc.details,
            },
        ) from exc


@router.get(
    "/{job_id}/attempts",
    response_model=list[JobAttemptResponse],
    summary="Get job execution history",
    description="Retrieve all execution attempts, worker identifiers, durations, and outcomes for a job.",
)
async def get_job_attempts(
    job_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
) -> list[JobAttemptResponse]:
    service = JobService(db, redis)
    try:
        attempts = await service.get_job_attempts(job_id)
        return [JobAttemptResponse.model_validate(att) for att in attempts]
    except JobNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "JobNotFound", "message": exc.message, "details": exc.details},
        ) from exc
