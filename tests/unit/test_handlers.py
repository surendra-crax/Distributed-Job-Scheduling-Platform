import uuid

import pytest

from app.exceptions.errors import PermanentJobError, RetryableJobError
from app.handlers.base import CancellationToken, JobExecutionContext
from app.handlers.calculation import CalculationHandler, CalculationPayload
from app.handlers.notification import NotificationPayload, NotificationSimHandler
from app.handlers.registry import registry
from app.handlers.report import ReportSimHandler


def make_context() -> JobExecutionContext:
    return JobExecutionContext(
        job_id=uuid.uuid4(),
        attempt_number=1,
        worker_id="worker_test_1",
        timeout_seconds=60,
        cancellation_token=CancellationToken(),
    )


@pytest.mark.asyncio
async def test_calculation_handler_deterministic():
    handler = registry.get("math.calculate")
    assert isinstance(handler, CalculationHandler)

    # Sum
    payload = handler.validate_payload({"operation": "sum", "operands": [5, 10, 15]})
    res = await handler.execute(payload, make_context())
    assert res["result"] == 30

    # Multiply
    payload = handler.validate_payload({"operation": "multiply", "operands": [2, 3, 4]})
    res = await handler.execute(payload, make_context())
    assert res["result"] == 24

    # Factorial
    payload = handler.validate_payload({"operation": "factorial", "n": 5})
    res = await handler.execute(payload, make_context())
    assert res["result"] == 120

    # Fibonacci
    payload = handler.validate_payload({"operation": "fibonacci", "n": 7})
    res = await handler.execute(payload, make_context())
    assert res["result"] == 13


@pytest.mark.asyncio
async def test_calculation_handler_permanent_error():
    handler = CalculationHandler()
    payload = CalculationPayload(operation="factorial", n=-5)
    with pytest.raises(PermanentJobError, match="non-negative"):
        await handler.execute(payload, make_context())


@pytest.mark.asyncio
async def test_notification_handler_success_and_validation():
    handler = registry.get("notification.simulate")
    assert isinstance(handler, NotificationSimHandler)

    # Valid payload
    payload = handler.validate_payload(
        {
            "recipient": "alice@example.com",
            "channel": "email",
            "template": "welcome_email",
        }
    )
    res = await handler.execute(payload, make_context())
    assert res["status"] == "delivered"
    assert res["channel"] == "email"

    # Bad email should fail permanently
    bad_payload = NotificationPayload(recipient="invalid_email", channel="email", template="test")
    with pytest.raises(PermanentJobError, match="Invalid email"):
        await handler.execute(bad_payload, make_context())


@pytest.mark.asyncio
async def test_notification_handler_simulated_retryable_error():
    handler = NotificationSimHandler()
    payload = NotificationPayload(
        recipient="user@example.com",
        channel="email",
        template="test",
        simulate_failure_rate=1.0,  # 100% failure probability
    )
    with pytest.raises(RetryableJobError, match="Transient network"):
        await handler.execute(payload, make_context())


@pytest.mark.asyncio
async def test_report_handler():
    handler = registry.get("report.generate")
    assert isinstance(handler, ReportSimHandler)

    payload = handler.validate_payload(
        {
            "report_type": "sales_summary",
            "date_range_days": 7,
            "row_count": 500,
            "simulated_duration_ms": 10,
        }
    )
    res = await handler.execute(payload, make_context())
    assert res["report_type"] == "sales_summary"
    assert res["rows_processed"] == 500
    assert "report_checksum" in res
