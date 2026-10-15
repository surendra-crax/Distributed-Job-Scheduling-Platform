class JobPlatformError(Exception):
    """Base exception for all domain errors within the Job Scheduling Platform."""

    def __init__(self, message: str, details: dict | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}


class JobNotFoundError(JobPlatformError):
    """Raised when a requested job does not exist."""


class InvalidStateTransitionError(JobPlatformError):
    """Raised when an illegal lifecycle transition is attempted."""

    def __init__(self, current_status: str, target_status: str, reason: str | None = None) -> None:
        message = f"Illegal transition from '{current_status}' to '{target_status}'"
        if reason:
            message += f": {reason}"
        super().__init__(
            message, {"current_status": current_status, "target_status": target_status}
        )
        self.current_status = current_status
        self.target_status = target_status


class UnknownJobTypeError(JobPlatformError):
    """Raised when submitting or executing a job type with no registered handler."""

    def __init__(self, job_type: str) -> None:
        super().__init__(
            f"Job type '{job_type}' is not recognized or registered",
            {"job_type": job_type},
        )
        self.job_type = job_type


class InvalidPayloadError(JobPlatformError):
    """Raised when a job payload does not satisfy the handler schema."""


class ConcurrencyConflictError(JobPlatformError):
    """Raised when an optimistic lock or fencing token mismatch occurs."""


class StaleWorkerLeaseError(JobPlatformError):
    """Raised when a worker attempts to persist results but its lease expired or was preempted."""


class RetryableJobError(JobPlatformError):
    """Handler exception indicating transient failure eligible for retry."""


class PermanentJobError(JobPlatformError):
    """Handler exception indicating fatal failure that must not be retried."""
