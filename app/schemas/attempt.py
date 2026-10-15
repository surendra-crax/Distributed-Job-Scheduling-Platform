import uuid
from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict

from app.models.attempt import AttemptOutcome


class JobAttemptResponse(BaseModel):
    """Execution attempt record response schema."""

    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    job_id: uuid.UUID
    worker_id: str
    attempt_number: int
    started_at: datetime
    completed_at: datetime | None = None
    outcome: AttemptOutcome
    error_details: dict[str, Any] | None = None
    execution_duration_seconds: float | None = None
