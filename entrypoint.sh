#!/usr/bin/env bash
set -e

echo "=========================================================="
echo "⚡ Starting Distributed Job Scheduling Platform"
echo "Mode: ${MODE:-all-in-one}"
echo "Environment: ${ENVIRONMENT:-production}"
echo "=========================================================="

# 1. Embedded Redis Initialization (Runs 100% on Render without external Redis)
if [ -z "${REDIS_URL}" ] || echo "${REDIS_URL}" | grep -qE "localhost|127\.0\.0\.1"; then
    echo "⚡ No external REDIS_URL provided. Starting high-speed embedded Redis server..."
    redis-server --daemonize yes --bind 127.0.0.1 --port 6379 --dir /tmp --save "" --appendonly no --protected-mode no
    export REDIS_URL="redis://127.0.0.1:6379/0"
    export REDIS_HOST="127.0.0.1"
    export REDIS_PORT="6379"
    echo "✅ Embedded Redis server active on 127.0.0.1:6379 (Zero external setup required)."
else
    echo "🔗 Using external Redis cluster: ${REDIS_URL}"
fi

# 2. Database Detection, Embedded Startup & Readiness Verification
USE_EMBEDDED_PG=false

if [ -z "${DATABASE_URL}" ]; then
    USE_EMBEDDED_PG=true
elif echo "${DATABASE_URL}" | grep -qE "localhost|127\.0\.0\.1"; then
    if ! pg_isready -h 127.0.0.1 -p 5432 -q 2>/dev/null; then
        USE_EMBEDDED_PG=true
    fi
fi

if [ "${USE_EMBEDDED_PG}" = "true" ]; then
    echo "⚡ No external DATABASE_URL provided. Starting embedded PostgreSQL server..."
    export PGDATA="/app/data/pgdata"
    if [ ! -d "${PGDATA}" ] || [ ! -f "${PGDATA}/PG_VERSION" ]; then
        echo "  Initializing embedded PostgreSQL database cluster in ${PGDATA}..."
        mkdir -p "${PGDATA}"
        initdb -D "${PGDATA}" --auth=trust -U postgres --encoding=UTF8
    fi
    echo "  Starting embedded PostgreSQL daemon..."
    pg_ctl -D "${PGDATA}" -l /tmp/postgres.log -o "-p 5432 -h 127.0.0.1" start
    
    echo "  Waiting for PostgreSQL to accept connections..."
    until pg_isready -h 127.0.0.1 -p 5432 -U postgres -q 2>/dev/null; do
        sleep 0.5
    done
    
    # Create job_scheduler database if it does not already exist
    psql -h 127.0.0.1 -p 5432 -U postgres -tc "SELECT 1 FROM pg_database WHERE datname = 'job_scheduler'" | grep -q 1 || \
        psql -h 127.0.0.1 -p 5432 -U postgres -c "CREATE DATABASE job_scheduler;"

    export DATABASE_URL="postgresql+asyncpg://postgres:postgres@127.0.0.1:5432/job_scheduler"
    export DATABASE_SYNC_URL="postgresql+psycopg://postgres:postgres@127.0.0.1:5432/job_scheduler"
    echo "✅ Embedded PostgreSQL server ready on 127.0.0.1:5432 (Zero external setup required)."
else
    echo "🔗 External DATABASE_URL detected: ${DATABASE_URL}"
    echo "  Waiting for PostgreSQL database to be reachable..."
    python3 -c "
import sys, time
from sqlalchemy import create_engine, text
from app.core.config import get_settings

sync_url = get_settings().sync_database_url
for attempt in range(1, 31):
    try:
        engine = create_engine(sync_url, pool_pre_ping=True, connect_args={'connect_timeout': 3})
        with engine.connect() as conn:
            conn.execute(text('SELECT 1'))
        print('  ✅ External PostgreSQL is reachable and ready.')
        sys.exit(0)
    except Exception as e:
        print(f'  [Attempt {attempt}/30] Database not ready yet ({e}). Waiting 2s...')
        time.sleep(2)
print('❌ Could not connect to PostgreSQL within 60 seconds.')
sys.exit(1)
"
fi

# 3. Run Alembic Database Migrations
echo "🔄 Checking and applying database migrations..."
alembic upgrade head
echo "✅ Database schema up to date."

# 2. Execution Dispatch based on MODE / RUN_ALL_SERVICES
PORT="${PORT:-8000}"

if [ "${MODE}" = "api" ]; then
    echo "🚀 Starting FastAPI Web & Mission Control server on port ${PORT}..."
    exec uvicorn app.main:app --host 0.0.0.0 --port "${PORT}"

elif [ "${MODE}" = "scheduler" ]; then
    echo "⏱️ Starting Scheduler Daemon..."
    exec python -m app.scheduler.scheduler

elif [ "${MODE}" = "worker" ]; then
    echo "⚙️ Starting Worker Process Pool..."
    exec python -m app.workers.worker

elif [ "${MODE}" = "recovery" ]; then
    echo "🛡️ Starting Crash Recovery Reaper..."
    exec python -m app.services.recovery_service

else
    # Default: All-in-one unified mode (Ideal for single-container & Render Web Service)
    echo "🌟 Launching in Unified Multi-Process Mode (API + Scheduler + Worker + Reaper)..."

    # Trap termination signals to gracefully stop background processes
    trap 'echo "🛑 Received termination signal. Shutting down..."; kill $(jobs -p); exit 0' SIGTERM SIGINT

    echo "  [1/4] Starting Scheduler Daemon in background..."
    python -m app.scheduler.scheduler &
    SCHED_PID=$!

    echo "  [2/4] Starting Worker Process Pool in background..."
    python -m app.workers.worker &
    WORKER_PID=$!

    echo "  [3/4] Starting Recovery Reaper in background..."
    python -m app.services.recovery_service &
    REAPER_PID=$!

    echo "  [4/4] Starting FastAPI Web & Mission Control server on port ${PORT}..."
    uvicorn app.main:app --host 0.0.0.0 --port "${PORT}" &
    API_PID=$!

    # Wait for the API process; if any process dies, keep others running or exit cleanly
    wait -n ${API_PID} ${WORKER_PID} ${SCHED_PID} ${REAPER_PID}
fi
