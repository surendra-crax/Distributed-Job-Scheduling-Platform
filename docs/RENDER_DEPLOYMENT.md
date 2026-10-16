# 🚀 Deploying to Render — Step-by-Step Guide

This guide walks you through deploying the **Distributed Job Scheduling Platform** and its **Mission Control Dashboard** to [Render](https://render.com).

---

## 🏗️ Architecture on Render

```
                               ┌──────────────────────────────────────────────┐
                               │             Render Web Service               │
                               │  ┌────────────────────────────────────────┐  │
                               │  │   FastAPI API Gateway + Frontend UI    │  │
                               │  │        (Port 8000 or 10000)            │  │
                               │  └────────────────────────────────────────┘  │
                               │  ┌──────────────┐ ┌──────────────┐ ┌──────┐  │
                               │  │  Scheduler   │ │ 4x Worker    │ │Reaper│  │
                               │  │  Daemon (1s) │ │ Process Pool │ │ (5s) │  │
                               │  └──────────────┘ └──────────────┘ └──────┘  │
                               └──────────────┬──────────────────┬────────────┘
                                              │                  │
                         ACID Transactions    │                  │  Queue Buffer & ZSETs
                         FOR UPDATE SKIP LOCKED                  │  (Sub-ms latency)
                                              ▼                  ▼
                                     ┌─────────────────┐ ┌─────────────────┐
                                     │ Render Postgres │ │  Render Redis   │
                                     │ Managed Database│ │   or Upstash    │
                                     └─────────────────┘ └─────────────────┘
```

The container automatically:
1. Runs database schema migrations (`alembic upgrade head`) on startup.
2. Normalizes database URLs (automatically converts `postgres://` to `postgresql+asyncpg://` and `postgresql+psycopg://`).
3. Launches the background **Scheduler daemon**, **Worker process pool**, **Recovery reaper**, and the **FastAPI gateway** in a supervised unified environment.

---

> 💡 **Do you need an external Redis account? NO!**
> The Docker container automatically starts an **embedded, in-memory Redis instance** inside the container on `127.0.0.1:6379` if no external `REDIS_URL` is provided. 
> You only need **Render's Free Web Service + Render's Free PostgreSQL Database**. Zero extra subscriptions or external accounts required!

---

## ⚡ Method 1: 1-Click Blueprint Deployment (Recommended)

Render Blueprints use the included [`render.yaml`](../render.yaml) file to automatically provision the database and web service.

### Step 1: Open Render Blueprints
1. Log in to [dashboard.render.com](https://dashboard.render.com/).
2. In the top right, click **New +** ➔ **Blueprint**.
3. Connect your GitHub account and select your repository:
   **`surendra-crax/Distributed-Job-Scheduling-Platform`**

### Step 2: Apply Blueprint
1. Render will read `render.yaml` and configure:
   - **Web Service**: `job-scheduler-platform` (Docker)
   - **PostgreSQL Database**: `job-scheduler-db` (Free Managed Database)
2. Click **Apply**.
3. Render will provision the PostgreSQL database, build the container, automatically start embedded Redis, apply migrations, and launch the platform!

---

## 🛠️ Method 2: Manual Web Service Setup (Render Only)

If you prefer setting up services manually via the Render dashboard:

### Step 1: Create PostgreSQL Database
1. In Render dashboard, click **New +** ➔ **PostgreSQL**.
2. **Name**: `job-scheduler-db`
3. **Database**: `job_scheduler`
4. **User**: `scheduler_admin`
5. **Plan**: Free (or Starter)
6. Click **Create Database**.
7. Once created, copy the **Internal Database URL** (e.g. `postgres://scheduler_admin:pass@dpg-xxx:5432/job_scheduler`).

### Step 2: Set Up Redis
1. Create a free Redis instance on [Upstash](https://upstash.com/) or Render:
   - On Upstash: Click **Create Database** ➔ Copy the `redis://default:password@endpoint:port` connection URL.

### Step 3: Create the Web Service
1. In Render dashboard, click **New +** ➔ **Web Service**.
2. Select your repository: `surendra-crax/Distributed-Job-Scheduling-Platform`.
3. Fill in the configuration:
   - **Name**: `distributed-job-scheduler`
   - **Region**: Same region as your database (e.g., `Oregon (US West)`)
   - **Branch**: `main`
   - **Runtime**: **Docker**
   - **Dockerfile Path**: `./Dockerfile`
   - **Plan**: Free
4. In the **Environment Variables** section, add:

| Key | Value | Description |
| :--- | :--- | :--- |
| `ENVIRONMENT` | `production` | Enables production pooling & error masking |
| `DATABASE_URL` | *Paste Internal Postgres URL* | Automatically normalized to `postgresql+asyncpg://` |
| `REDIS_URL` | *(Optional) Leave blank!* | Container automatically starts embedded Redis if empty |
| `WORKER_CONCURRENCY` | `4` | Number of worker processes to spawn |
| `SCHEDULER_POLL_INTERVAL_SECONDS` | `1.0` | Frequency of due job polling |
| `RECOVERY_POLL_INTERVAL_SECONDS` | `10.0` | Frequency of zombie lease reaping |

5. Under **Advanced**:
   - **Health Check Path**: `/health/ready`
6. Click **Create Web Service**.

---

## ✅ Verifying Your Deployment

Once Render finishes deploying:

1. **Access Mission Control**:
   - Open `https://<your-service-name>.onrender.com/` in your browser.
   - You will see the **Mission Control Dashboard** with live cluster topology pills:
     - 🐘 **PostgreSQL**: Latency badge (< 5ms)
     - ⚡ **Redis**: Buffer latency badge
     - ⏱️ **Scheduler**: Active (1s tick)
     - ⚙️ **Worker Pool**: 4 Slots Active
     - 🛡️ **Recovery Reaper**: Armed
2. **Interactive Swagger Docs**:
   - Open `https://<your-service-name>.onrender.com/docs` to test raw REST API endpoints.
3. **Health Check Endpoints**:
   - `GET /health/live`: Fast liveness probe.
   - `GET /health/ready`: Deep connectivity check testing both PostgreSQL and Redis.
4. **Run Live Scenarios**:
   - In the dashboard, click **`[Immediate Math]`**, **`[Schedule Future Job (+8s)]`**, or **`[Fire 10 Concurrent Jobs]`** to watch the cluster process distributed tasks in production!
