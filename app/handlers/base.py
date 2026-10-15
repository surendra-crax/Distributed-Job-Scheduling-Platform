import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, ClassVar

from pydantic import BaseModel, ValidationError

from app.exceptions.errors import InvalidPayloadError


class CancellationToken:
    """Cooperative cancellation token polled by handlers during long executions."""

    def __init__(self) -> None:
        self._cancelled: bool = False

    @property
    def is_cancelled(self) -> bool:
        return self._cancelled

    def cancel(self) -> None:
        self._cancelled = True

    def raise_if_cancelled(self) -> None:
        if self._cancelled:
            from app.exceptions.errors import JobPlatformError

            raise JobPlatformError("Job execution cancelled by client request")


@dataclass(frozen=True)
class JobExecutionContext:
    """Runtime execution context provided to the handler."""

    job_id: uuid.UUID
    attempt_number: int
    worker_id: str
    timeout_seconds: int
    cancellation_token: CancellationToken


class BaseJobHandler(ABC):
    """Abstract base class for all registered job execution handlers."""

    job_type: ClassVar[str]
    payload_schema: ClassVar[type[BaseModel]]

    @classmethod
    def validate_payload(cls, raw_payload: dict[str, Any]) -> BaseModel:
        """Validates raw dictionary payload against the handler's Pydantic schema."""
        try:
            return cls.payload_schema.model_validate(raw_payload)
        except ValidationError as exc:
            raise InvalidPayloadError(
                f"Payload validation failed for job type '{cls.job_type}': {exc.errors()}",
                details={"errors": exc.errors()},
            ) from exc

    @abstractmethod
    async def execute(
        self,
        payload: Any,
        context: JobExecutionContext,
    ) -> dict[str, Any]:
        """Executes the handler's logic.

        Must raise RetryableJobError for transient failures or PermanentJobError for non-retryable failures.
        Returns a JSON-serializable dictionary of execution results.
        """
        pass
