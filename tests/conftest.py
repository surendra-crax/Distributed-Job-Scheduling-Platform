import os
from collections.abc import AsyncGenerator

import pytest
from httpx import ASGITransport, AsyncClient

# Force testing configuration
os.environ["ENVIRONMENT"] = "testing"
os.environ["DEBUG"] = "true"
os.environ["POSTGRES_DB"] = "job_scheduler_test"
os.environ["DATABASE_URL"] = (
    "postgresql+asyncpg://postgres:postgres@localhost:5432/job_scheduler_test"
)
os.environ["DATABASE_SYNC_URL"] = (
    "postgresql+psycopg://postgres:postgres@localhost:5432/job_scheduler_test"
)
os.environ["REDIS_DB"] = "1"
os.environ["REDIS_URL"] = "redis://localhost:6379/1"

from app.core.config import get_settings  # noqa: E402
from app.main import create_application  # noqa: E402

# Clear lru_cache for settings to pick up test overrides
get_settings.cache_clear()


@pytest.fixture
def app():
    """Create test application instance."""
    return create_application()


@pytest.fixture(autouse=True)
async def cleanup_connections():
    """Ensure clean database and redis state before and after each test."""
    from sqlalchemy import text

    from app.db.session import close_db_engine, get_engine
    from app.queue.redis_queue import close_redis_client, get_redis_client

    # Clean before test
    try:
        engine = get_engine()
        async with engine.begin() as conn:
            await conn.execute(text("TRUNCATE TABLE job_attempts, jobs CASCADE;"))
    except Exception:
        pass

    try:
        redis = get_redis_client()
        await redis.flushdb()
    except Exception:
        pass

    yield

    # Clean and close after test
    await close_db_engine()
    await close_redis_client()


@pytest.fixture
async def client(app) -> AsyncGenerator[AsyncClient, None]:
    """Async HTTP client fixture bound to the FastAPI application."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac
