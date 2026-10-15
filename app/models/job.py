import enum
import uuid
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Index,
    Integer,
    String,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class JobStatus(enum.StrEnum):
    """Explicit job lifecycle states."""

    SCHEDULED = "SCHEDULED"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    RETRY_WAIT = "RETRY_WAIT"
    CANCELLED = "CANCELLED"


# Allowed state transitions enforced by domain logic
ALLOWED_TRANSITIONS: dict[JobStatus, set[JobStatus]] = {
    JobStatus.SCHEDULED: {JobStatus.QUEUED, JobStatus.CANCELLED},
    JobStatus.QUEUED: {JobStatus.RUNNING, JobStatus.CANCELLED},
    JobStatus.RUNNING: {
        JobStatus.SUCCEEDED,
        JobStatus.FAILED,
        JobStatus.RETRY_WAIT,
        JobStatus.CANCELLED,
    },
    JobStatus.RETRY_WAIT: {JobStatus.QUEUED, JobStatus.CANCELLED},
    # Terminal states can only be revived via explicit manual retry endpoint
    JobStatus.SUCCEEDED: set(),
    JobStatus.FAILED: {JobStatus.QUEUED},
    JobStatus.CANCELLED: {JobStatus.QUEUED},
}


class Job(Base):
    """Durable Job entity representing a unit of asynchronous work."""

    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        nullable=False,
    )
    job_type: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        index=True,
    )
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
    )
    priority: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=50,
    )
    status: Mapped[str] = mapped_column(
        String(32),
        nullable=False,
        default=JobStatus.QUEUED.value,
        index=True,
    )
    scheduled_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(UTC),
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    attempt_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
    )
    max_attempts: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=3,
    )
    timeout_seconds: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=300,
    )
    result: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    last_error: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    lease_owner: Mapped[str | None] = mapped_column(
        String(128),
        nullable=True,
    )
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    version: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        default=1,
    )

    # Relationship to execution attempt history
    attempts = relationship(
        "JobAttempt",
        back_populates="job",
        cascade="all, delete-orphan",
        order_by="JobAttempt.attempt_number",
    )

    __table_args__ = (
        # Range check on priority (1-100)
        CheckConstraint("priority >= 1 AND priority <= 100", name="chk_job_priority_range"),
        # Non-negative attempt count
        CheckConstraint("attempt_count >= 0", name="chk_job_attempt_count_positive"),
        # Compound index for due-job scheduler queries
        Index("ix_jobs_scheduler_due", "status", "scheduled_at", priority.desc()),
        # Compound index for recovery / lease reaper queries
        Index("ix_jobs_recovery_expired", "status", "lease_expires_at"),
        # Compound index for paginated job listings by status
        Index("ix_jobs_status_created", "status", created_at.desc()),
        # Compound index for queue reconciliation queries
        Index("ix_jobs_queue_reconcile", "status", "updated_at"),
    )

    def can_transition_to(self, target: JobStatus) -> bool:
        """Determines whether transitioning from the current status to target is legal."""
        current = JobStatus(self.status)
        return target in ALLOWED_TRANSITIONS.get(current, set())
