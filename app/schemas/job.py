import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from app.models.job import JobStatus


class JobCreateRequest(BaseModel):
    """Schema for submitting a new job for execution."""

    job_type: str = Field(
        ...,
        min_length=1,
        max_length=64,
        description="Registered job handler identifier (e.g., 'math.calculate')",
        examples=["math.calculate"],
    )
    payload: dict[str, Any] = Field(
        ...,
        description="JSON payload validated against the target job handler schema",
        examples=[{"operation": "sum", "operands": [10, 20, 30]}],
    )
    priority: int = Field(
        default=50,
        ge=1,
        le=100,
        description="Job priority between 1 (lowest) and 100 (highest)",
        examples=[50],
    )
    scheduled_at: datetime | None = Field(
        default=None,
        description="Future timestamp when the job becomes eligible. If omitted, defaults to immediate.",
    )
    max_attempts: int = Field(
        default=3,
        ge=1,
        le=20,
        description="Maximum execution attempts before terminal failure",
        examples=[3],
    )
    timeout_seconds: int = Field(
        default=300,
        ge=1,
        le=86400,
        description="Maximum execution timeout in seconds per attempt",
        examples=[60],
    )


class JobResponse(BaseModel):
    """Full Job representation response."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    job_type: str
    payload: dict[str, Any]
    priority: int
    status: JobStatus
    scheduled_at: datetime
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    attempt_count: int
    max_attempts: int
    timeout_seconds: int
    result: dict[str, Any] | None = None
    last_error: dict[str, Any] | None = None
    lease_owner: str | None = None
    lease_expires_at: datetime | None = None
    version: int


class JobListResponse(BaseModel):
    """Paginated list of jobs."""

    items: list[JobResponse]
    total: int
    limit: int
    offset: int


class JobCancelResponse(BaseModel):
    """Result of a cancellation request."""

    job_id: uuid.UUID
    previous_status: JobStatus
    new_status: JobStatus
    message: str


class JobRetryResponse(BaseModel):
    """Result of a manual retry request."""

    job_id: uuid.UUID
    previous_status: JobStatus
    new_status: JobStatus
    scheduled_at: datetime
    message: str
