from app.core.config import Settings


def test_render_database_url_normalization(monkeypatch):
    """Verify that postgres:// and postgresql:// URLs supplied by Render are converted properly."""
    # Clear test env vars that override defaults
    monkeypatch.delenv("DATABASE_SYNC_URL", raising=False)

    # Test Render postgres:// URL
    render_url = "postgres://usr:pwd@dpg-xxx-a.oregon-postgres.render.com/mydb"
    s1 = Settings(DATABASE_URL=render_url, DATABASE_SYNC_URL=None)
    assert s1.async_database_url == "postgresql+asyncpg://usr:pwd@dpg-xxx-a.oregon-postgres.render.com/mydb"
    assert s1.sync_database_url == "postgresql+psycopg://usr:pwd@dpg-xxx-a.oregon-postgres.render.com/mydb"

    # Test standard postgresql:// URL without driver
    std_url = "postgresql://usr:pwd@localhost:5432/mydb"
    s2 = Settings(DATABASE_URL=std_url, DATABASE_SYNC_URL=None)
    assert s2.async_database_url == "postgresql+asyncpg://usr:pwd@localhost:5432/mydb"
    assert s2.sync_database_url == "postgresql+psycopg://usr:pwd@localhost:5432/mydb"

    # Test pre-specified driver URLs are untouched
    asyncpg_url = "postgresql+asyncpg://usr:pwd@localhost:5432/mydb"
    s3 = Settings(DATABASE_URL=asyncpg_url, DATABASE_SYNC_URL=None)
    assert s3.async_database_url == asyncpg_url
    assert s3.sync_database_url == "postgresql+psycopg://usr:pwd@localhost:5432/mydb"


def test_redis_url_resolution(monkeypatch):
    """Verify that REDIS_URL from cloud providers is prioritized."""
    cloud_redis = "redis://red-xxx:6379"
    s = Settings(REDIS_URL=cloud_redis)
    assert s.resolved_redis_url == cloud_redis
