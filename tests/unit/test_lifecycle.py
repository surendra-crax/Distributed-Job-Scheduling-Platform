from app.models.job import ALLOWED_TRANSITIONS, Job, JobStatus


def test_allowed_transitions_coverage():
    """Ensure all declared JobStatus values have explicitly defined transition rules."""
    for st in JobStatus:
        assert st in ALLOWED_TRANSITIONS, f"Status {st} missing from ALLOWED_TRANSITIONS table"


def test_job_can_transition_to_valid():
    """Verify legal transitions according to the state machine matrix."""
    # From SCHEDULED
    job = Job(status=JobStatus.SCHEDULED.value)
    assert job.can_transition_to(JobStatus.QUEUED)
    assert job.can_transition_to(JobStatus.CANCELLED)
    assert not job.can_transition_to(JobStatus.RUNNING)
    assert not job.can_transition_to(JobStatus.SUCCEEDED)

    # From QUEUED
    job = Job(status=JobStatus.QUEUED.value)
    assert job.can_transition_to(JobStatus.RUNNING)
    assert job.can_transition_to(JobStatus.CANCELLED)
    assert not job.can_transition_to(JobStatus.SUCCEEDED)

    # From RUNNING
    job = Job(status=JobStatus.RUNNING.value)
    assert job.can_transition_to(JobStatus.SUCCEEDED)
    assert job.can_transition_to(JobStatus.FAILED)
    assert job.can_transition_to(JobStatus.RETRY_WAIT)
    assert job.can_transition_to(JobStatus.CANCELLED)
    assert not job.can_transition_to(JobStatus.QUEUED)

    # From RETRY_WAIT
    job = Job(status=JobStatus.RETRY_WAIT.value)
    assert job.can_transition_to(JobStatus.QUEUED)
    assert job.can_transition_to(JobStatus.CANCELLED)
    assert not job.can_transition_to(JobStatus.RUNNING)


def test_terminal_states_cannot_transition_arbitrarily():
    """Terminal states must strictly forbid non-retry transitions."""
    # SUCCEEDED is terminal: cannot be cancelled, re-queued, or marked failed
    job_succ = Job(status=JobStatus.SUCCEEDED.value)
    for target in JobStatus:
        assert not job_succ.can_transition_to(target)

    # FAILED and CANCELLED can ONLY transition to QUEUED via manual retry
    job_failed = Job(status=JobStatus.FAILED.value)
    assert job_failed.can_transition_to(JobStatus.QUEUED)
    assert not job_failed.can_transition_to(JobStatus.RUNNING)
    assert not job_failed.can_transition_to(JobStatus.SUCCEEDED)

    job_cancelled = Job(status=JobStatus.CANCELLED.value)
    assert job_cancelled.can_transition_to(JobStatus.QUEUED)
    assert not job_cancelled.can_transition_to(JobStatus.RUNNING)
    assert not job_cancelled.can_transition_to(JobStatus.SUCCEEDED)
