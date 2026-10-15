// ==========================================================================
// MISSION CONTROL DASHBOARD JAVASCRIPT — DISTRIBUTED JOB SCHEDULER
// ==========================================================================

const API_BASE = "";

// State
let isAutoRefresh = true;
let refreshTimer = null;
let currentJobs = [];
let currentFilter = { search: "", status: "" };
const knownJobStates = new Map(); // id -> { status, version, lease_owner }

// DOM Elements - Metrics
const kpiQueueDepth = document.getElementById("kpi-queue-depth");
const kpiRunning = document.getElementById("kpi-running");
const kpiScheduledRetrying = document.getElementById("kpi-scheduled-retrying");
const kpiSucceeded = document.getElementById("kpi-succeeded");
const kpiFailed = document.getElementById("kpi-failed");
const kpiAvgDuration = document.getElementById("kpi-avg-duration");

// Pipeline Counts
const countScheduled = document.getElementById("count-scheduled");
const countQueued = document.getElementById("count-queued");
const countRunning = document.getElementById("count-running");
const countSucceeded = document.getElementById("count-succeeded");
const countRetry = document.getElementById("count-retry");
const countFailed = document.getElementById("count-failed");
const countCancelled = document.getElementById("count-cancelled");

// Status Bar
const valPostgres = document.getElementById("val-postgres");
const valRedis = document.getElementById("val-redis");

// Table
const tableBody = document.getElementById("jobs-table-body");
const tableTotalCount = document.getElementById("table-total-count");
const filterSearch = document.getElementById("filter-search");
const filterStatus = document.getElementById("filter-status");
const btnResetFilters = document.getElementById("btn-reset-filters");

// Controls
const toggleAutoRefresh = document.getElementById("toggle-autorefresh");
const btnRefresh = document.getElementById("btn-refresh");

// Form
const jobDispatchForm = document.getElementById("job-dispatch-form");
const selectJobType = document.getElementById("select-job-type");
const inputPriority = document.getElementById("input-priority");
const priorityBadge = document.getElementById("priority-badge");
const payloadFieldsContainer = document.getElementById("payload-fields-container");

// Job Modal
const modalBackdrop = document.getElementById("job-modal-backdrop");
const modalClose = document.getElementById("modal-close");
const modalTitle = document.getElementById("modal-title");
const modalSubtitle = document.getElementById("modal-subtitle");
const modalMetaGrid = document.getElementById("modal-meta-grid");
const modalPayload = document.getElementById("modal-payload");
const modalResult = document.getElementById("modal-result");
const modalError = document.getElementById("modal-error");
const modalErrorSection = document.getElementById("modal-error-section");
const modalAttemptsContainer = document.getElementById("modal-attempts-container");
const modalFooterActions = document.getElementById("modal-footer-actions");

// Workflow Guide Modal & Banner
const btnOpenWorkflow = document.getElementById("btn-open-workflow");
const btnBannerFullGuide = document.getElementById("btn-banner-full-guide");
const workflowModalBackdrop = document.getElementById("workflow-modal-backdrop");
const workflowModalClose = document.getElementById("workflow-modal-close");
const btnToggleGuideDetails = document.getElementById("btn-toggle-guide-details");
const guideToggleText = document.getElementById("guide-toggle-text");
const guideDetailsPanel = document.getElementById("guide-details-panel");

// Trace Terminal
const traceTerminal = document.getElementById("trace-terminal");
const btnClearTrace = document.getElementById("btn-clear-trace");

// Toast Container
const toastContainer = document.getElementById("toast-container");

// ==========================================================================
// TOAST NOTIFICATIONS
// ==========================================================================
function showToast(message, type = "info") {
  const toast = document.createElement("div");
  toast.className = `toast toast-${type}`;
  toast.innerHTML = `<span>${type === "error" ? "❌" : type === "success" ? "✅" : "ℹ️"}</span> <span>${message}</span>`;
  toastContainer.appendChild(toast);
  setTimeout(() => {
    toast.style.opacity = "0";
    setTimeout(() => toast.remove(), 300);
  }, 3500);
}

// ==========================================================================
// REAL-TIME SYSTEM TRACE TERMINAL
// ==========================================================================
function addSystemTrace(tag, message, highlight = "") {
  if (!traceTerminal) return;
  const now = new Date();
  const timeStr = now.toTimeString().split(" ")[0] + "." + String(now.getMilliseconds()).padStart(3, "0");

  const row = document.createElement("div");
  row.className = "trace-row";

  let tagClass = "tag-client";
  let tagLabel = tag.toUpperCase();

  if (tag === "postgres" || tag === "db") {
    tagClass = "tag-postgres";
    tagLabel = "POSTGRESQL";
  } else if (tag === "redis") {
    tagClass = "tag-redis";
    tagLabel = "REDIS BUFFER";
  } else if (tag === "scheduler") {
    tagClass = "tag-scheduler";
    tagLabel = "SCHEDULER";
  } else if (tag === "worker") {
    tagClass = "tag-worker";
    tagLabel = "WORKER";
  } else if (tag === "reaper" || tag === "recovery") {
    tagClass = "tag-reaper";
    tagLabel = "REAPER";
  } else if (tag === "client") {
    tagClass = "tag-client";
    tagLabel = "CLIENT API";
  }

  let formattedMsg = escapeHtml(message);
  if (highlight) {
    formattedMsg += ` <span class="trace-highlight">[${escapeHtml(highlight)}]</span>`;
  }

  row.innerHTML = `
    <span class="trace-ts">${timeStr}</span>
    <span class="trace-tag ${tagClass}">${tagLabel}</span>
    <span class="trace-text">${formattedMsg}</span>
  `;

  traceTerminal.appendChild(row);

  // Keep terminal capped at 120 lines
  while (traceTerminal.children.length > 120) {
    traceTerminal.removeChild(traceTerminal.firstChild);
  }

  // Auto-scroll to bottom
  traceTerminal.scrollTop = traceTerminal.scrollHeight;
}

function escapeHtml(str) {
  if (!str) return "";
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

// ==========================================================================
// FETCH METRICS & HEALTH
// ==========================================================================
async function fetchMetricsAndHealth() {
  try {
    const [metricsRes, healthRes] = await Promise.all([
      fetch(`${API_BASE}/api/v1/metrics/summary`),
      fetch(`${API_BASE}/health/ready`)
    ]);

    if (healthRes.ok) {
      const healthData = await healthRes.json();
      if (healthData.checks.database.status === "healthy") {
        valPostgres.textContent = `${healthData.checks.database.latency_ms}ms (ACID)`;
        document.getElementById("pill-postgres").querySelector(".status-dot").classList.remove("error");
      }
      if (healthData.checks.redis.status === "healthy") {
        valRedis.textContent = `${healthData.checks.redis.latency_ms}ms (ZSET)`;
        document.getElementById("pill-redis").querySelector(".status-dot").classList.remove("error");
      }
    }

    if (metricsRes.ok) {
      const data = await metricsRes.json();
      const st = data.jobs_by_status;

      kpiQueueDepth.textContent = data.redis_queue_depth >= 0 ? data.redis_queue_depth : "0";
      kpiRunning.textContent = st.RUNNING || 0;
      kpiScheduledRetrying.textContent = (st.SCHEDULED || 0) + (st.RETRY_WAIT || 0);
      kpiSucceeded.textContent = st.SUCCEEDED || 0;
      kpiFailed.textContent = st.FAILED || 0;
      kpiAvgDuration.textContent = `${(data.avg_execution_duration_seconds * 1000).toFixed(1)} ms`;

      // Update Pipeline Nodes
      countScheduled.textContent = st.SCHEDULED || 0;
      countQueued.textContent = st.QUEUED || 0;
      countRunning.textContent = st.RUNNING || 0;
      countSucceeded.textContent = st.SUCCEEDED || 0;
      countRetry.textContent = st.RETRY_WAIT || 0;
      countFailed.textContent = st.FAILED || 0;
      countCancelled.textContent = st.CANCELLED || 0;
    }
  } catch (err) {
    console.warn("Telemetry fetch error:", err);
  }
}

// ==========================================================================
// FETCH JOBS TABLE & DETECT DISTRIBUTED STATE TRANSITIONS
// ==========================================================================
async function fetchJobs() {
  try {
    const params = new URLSearchParams({
      page: "1",
      page_size: "30"
    });
    if (currentFilter.status) params.set("status", currentFilter.status);

    const res = await fetch(`${API_BASE}/api/v1/jobs?${params.toString()}`);
    if (!res.ok) return;

    const data = await res.json();
    const newJobs = data.items || [];

    // Detect and log state transitions
    newJobs.forEach(job => {
      const shortId = job.id.substring(0, 8);
      const prev = knownJobStates.get(job.id);

      if (prev && prev.status !== job.status) {
        if (job.status === "RUNNING") {
          addSystemTrace(
            "worker",
            `Worker [${job.lease_owner || "pool"}] claimed atomic lease for Job ${shortId}... (fencing version: v${job.version})`,
            "SELECT FOR UPDATE SKIP LOCKED"
          );
        } else if (job.status === "SUCCEEDED") {
          addSystemTrace(
            "postgres",
            `Job ${shortId}... SUCCEEDED. Completed audit record committed to PostgreSQL.`,
            "ACID Commit"
          );
        } else if (job.status === "RETRY_WAIT") {
          addSystemTrace(
            "worker",
            `Job ${shortId}... transient failure on attempt ${job.attempt_count}/${job.max_attempts}. Full-jitter backoff waiting for next window.`,
            "RETRY_WAIT"
          );
        } else if (job.status === "CANCELLED") {
          addSystemTrace(
            "worker",
            `Job ${shortId}... cooperative cancellation confirmed. Worker stopped safely.`,
            "CANCELLED"
          );
        } else if (job.status === "QUEUED" && prev.status === "SCHEDULED") {
          addSystemTrace(
            "scheduler",
            `Scheduler promoted due Job ${shortId}... (scheduled_at <= NOW()) -> pushed to Redis buffer.`,
            "SCHEDULED ➔ QUEUED"
          );
        }
      }

      knownJobStates.set(job.id, {
        status: job.status,
        version: job.version,
        lease_owner: job.lease_owner
      });
    });

    currentJobs = newJobs;
    tableTotalCount.textContent = `${data.total} Jobs in Registry`;
    renderJobsTable();
  } catch (err) {
    console.error("Fetch jobs error:", err);
  }
}

// ==========================================================================
// RENDER JOBS TABLE
// ==========================================================================
function renderJobsTable() {
  const searchTerm = currentFilter.search.toLowerCase().trim();
  const filtered = currentJobs.filter(job => {
    if (!searchTerm) return true;
    return (
      job.id.toLowerCase().includes(searchTerm) ||
      job.job_type.toLowerCase().includes(searchTerm) ||
      (job.lease_owner && job.lease_owner.toLowerCase().includes(searchTerm))
    );
  });

  if (filtered.length === 0) {
    tableBody.innerHTML = `
      <tr>
        <td colspan="8" class="text-center text-muted py-8">
          No matching jobs found. Click any <strong>1-Click Scenario</strong> button above to submit jobs.
        </td>
      </tr>
    `;
    return;
  }

  tableBody.innerHTML = filtered.map(job => {
    const isRunning = job.status === "RUNNING";
    const isRetry = job.status === "RETRY_WAIT";
    const isScheduled = job.status === "SCHEDULED";
    const shortId = job.id.substring(0, 8);
    const dateStr = formatJobTimestamp(job);

    return `
      <tr onclick="openJobModal('${job.id}')" title="Click to inspect payload, logs, and attempts">
        <td class="font-mono">
          <span class="job-id-link">
            <span class="job-id-prefix">${shortId}...</span>
            <span class="view-pill">Inspect ↗</span>
          </span>
        </td>
        <td>
          <span class="handler-tag">${job.job_type}</span>
        </td>
        <td>
          <span class="priority-pill ${job.priority >= 80 ? "p-high" : job.priority >= 50 ? "p-med" : "p-low"}">
            P${job.priority}
          </span>
        </td>
        <td>
          <span class="status-badge badge-${job.status} ${isRunning ? "badge-pulse-running" : ""} ${isRetry ? "badge-pulse-amber" : ""}">
            ${job.status}
          </span>
        </td>
        <td class="font-mono">
          ${job.attempt_count} / ${job.max_attempts}
        </td>
        <td class="text-muted font-sm">
          ${dateStr}
        </td>
        <td class="font-mono">
          <span class="version-tag" title="Fencing token incremented on each lease claim">v${job.version}</span>
        </td>
        <td class="text-right" onclick="event.stopPropagation()">
          ${renderRowActions(job)}
        </td>
      </tr>
    `;
  }).join("");
}

function formatJobTimestamp(job) {
  if (job.status === "SCHEDULED" && job.scheduled_at) {
    const diffMs = new Date(job.scheduled_at) - new Date();
    if (diffMs > 0) {
      return `⏰ in ${(diffMs / 1000).toFixed(0)}s`;
    }
  }
  const date = new Date(job.created_at);
  return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function renderRowActions(job) {
  if (job.status === "QUEUED" || job.status === "RUNNING" || job.status === "SCHEDULED" || job.status === "RETRY_WAIT") {
    return `<button class="btn btn-outline btn-xs btn-danger" onclick="cancelJob('${job.id}')" title="Cancel this in-flight or waiting job">Cancel</button>`;
  }
  if (job.status === "FAILED" || job.status === "CANCELLED") {
    return `<button class="btn btn-outline btn-xs" onclick="retryJob('${job.id}')" title="Re-queue this job">Retry</button>`;
  }
  return `<button class="btn btn-outline btn-xs" onclick="openJobModal('${job.id}')">View</button>`;
}

// ==========================================================================
// JOB ACTIONS (CANCEL / RETRY)
// ==========================================================================
async function cancelJob(jobId) {
  try {
    addSystemTrace("client", `Sending cooperative cancel request for Job ${jobId.substring(0,8)}...`, "POST /cancel");
    const res = await fetch(`${API_BASE}/api/v1/jobs/${jobId}/cancel`, { method: "POST" });
    if (res.ok) {
      showToast("Cancellation broadcasted to worker pool", "success");
      addSystemTrace("worker", `Cancellation token acknowledged for Job ${jobId.substring(0,8)}...`, "Cooperative Stop");
      await fetchJobs();
      await fetchMetricsAndHealth();
    } else {
      const err = await res.json();
      showToast(err.detail || "Cancellation failed", "error");
    }
  } catch (err) {
    console.error("Cancel job error:", err);
    showToast("Network error during cancellation", "error");
  }
}

async function retryJob(jobId) {
  try {
    addSystemTrace("client", `Manual retry requested for Job ${jobId.substring(0,8)}...`, "POST /retry");
    const res = await fetch(`${API_BASE}/api/v1/jobs/${jobId}/retry`, { method: "POST" });
    if (res.ok) {
      showToast("Job reset and re-queued", "success");
      addSystemTrace("redis", `Job ${jobId.substring(0,8)}... re-enqueued to Redis buffer.`, "QUEUED");
      await fetchJobs();
      await fetchMetricsAndHealth();
    } else {
      const err = await res.json();
      showToast(err.detail || "Retry failed", "error");
    }
  } catch (err) {
    console.error("Retry job error:", err);
    showToast("Network error during retry", "error");
  }
}

// ==========================================================================
// 1-CLICK INTERACTIVE SCENARIOS
// ==========================================================================
async function runScenario(scenarioKey) {
  if (scenarioKey === "immediate") {
    showToast("Launching immediate high-priority calculation...", "info");
    addSystemTrace("client", "Dispatched Immediate Math Calculation (Fibonacci n=35, Priority 95)", "POST /api/v1/jobs");
    addSystemTrace("postgres", "Durable ACID transaction committed -> status=QUEUED, version=1", "INSERT INTO jobs");
    addSystemTrace("redis", "Buffer push -> LPUSH queue:default", "Ready for Worker");

    const res = await fetch(`${API_BASE}/api/v1/jobs`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        job_type: "math.calculate",
        payload: { operation: "fibonacci", n: 35 },
        priority: 95
      })
    });
    if (res.ok) {
      const job = await res.json();
      addSystemTrace("worker", `Worker pool acquired Job ${job.id.substring(0,8)}... computing fibonacci(35)`, "RUNNING");
    }
  } else if (scenarioKey === "scheduled") {
    const target = new Date(Date.now() + 8000).toISOString();
    showToast("Scheduling job for +8 seconds into the future...", "info");
    addSystemTrace("client", "Submitted future job with +8s delay", "scheduled_at = NOW() + 8s");
    addSystemTrace("postgres", "Stored in PostgreSQL with status=SCHEDULED. Awaiting scheduler polling.", "Durable State");

    await fetch(`${API_BASE}/api/v1/jobs`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        job_type: "notification.simulate",
        payload: { recipient: "scheduled.demo@example.com", channel: "sms", template: "MeetingReminder" },
        priority: 80,
        scheduled_at: target
      })
    });
  } else if (scenarioKey === "retry") {
    showToast("Dispatching transient fault task with exponential backoff...", "info");
    addSystemTrace("client", "Submitted transient fault simulation job (notification.simulate, fail_rate=1.0)", "Simulated Fault");
    addSystemTrace("postgres", "Job committed to PostgreSQL with max_attempts=3", "status=QUEUED");

    await fetch(`${API_BASE}/api/v1/jobs`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        job_type: "notification.simulate",
        payload: { recipient: "unstable@gateway.net", channel: "email", template: "TestRetryAlert", simulate_failure_rate: 1.0 },
        priority: 60,
        max_attempts: 3
      })
    });
  } else if (scenarioKey === "cancel") {
    showToast("Dispatching 6s task, will broadcast cancellation in 800ms...", "info");
    addSystemTrace("client", "Dispatched 6s multi-chunk task (report.generate) with 50,000 rows...", "chunk_count=6");

    const res = await fetch(`${API_BASE}/api/v1/jobs`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        job_type: "report.generate",
        payload: { report_type: "inventory_audit", simulated_duration_ms: 6000, row_count: 50000 },
        priority: 50
      })
    });
    if (res.ok) {
      const job = await res.json();
      addSystemTrace("worker", `Job ${job.id.substring(0,8)}... RUNNING on worker. Executing chunk 1/6...`, "RUNNING");
      setTimeout(async () => {
        addSystemTrace("client", `Broadcasting cooperative cancellation for Job ${job.id.substring(0,8)}...`, "POST /cancel");
        await cancelJob(job.id);
      }, 800);
    }
  } else if (scenarioKey === "burst") {
    showToast("Firing burst of 10 concurrent jobs across worker pool...", "info");
    addSystemTrace("client", "Burst triggered: submitting 10 parallel jobs with mixed priorities (10-100)...", "10 Concurrent POSTs");
    addSystemTrace("redis", "Buffered 10 items in queue:default. 4 worker processes competing via SKIP LOCKED.", "Concurrent Pool");

    const promises = [];
    for (let i = 1; i <= 10; i++) {
      promises.push(
        fetch(`${API_BASE}/api/v1/jobs`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            job_type: "math.calculate",
            payload: { operation: "sum", operands: [i, i * 10, i * 100] },
            priority: 10 * i
          })
        })
      );
    }
    await Promise.all(promises);
  }

  await fetchJobs();
  await fetchMetricsAndHealth();
}

// ==========================================================================
// JOB INSPECTION MODAL
// ==========================================================================
async function openJobModal(jobId) {
  try {
    const [jobRes, attemptsRes] = await Promise.all([
      fetch(`${API_BASE}/api/v1/jobs/${jobId}`),
      fetch(`${API_BASE}/api/v1/jobs/${jobId}/attempts`)
    ]);

    if (!jobRes.ok) return;
    const job = await jobRes.json();
    const attempts = attemptsRes.ok ? await attemptsRes.json() : [];

    modalTitle.textContent = `Job ${job.id.substring(0, 13)}...`;
    modalSubtitle.textContent = `Handler: ${job.job_type} • Status: ${job.status}`;

    modalMetaGrid.innerHTML = `
      <div class="meta-item"><span class="meta-label">Full UUID</span><span class="meta-value">${job.id}</span></div>
      <div class="meta-item"><span class="meta-label">Priority</span><span class="meta-value">P${job.priority}</span></div>
      <div class="meta-item"><span class="meta-label">Status</span><span class="meta-value status-badge badge-${job.status}">${job.status}</span></div>
      <div class="meta-item"><span class="meta-label">Fencing Token (Version)</span><span class="meta-value">v${job.version}</span></div>
      <div class="meta-item"><span class="meta-label">Attempts</span><span class="meta-value">${job.attempt_count} / ${job.max_attempts}</span></div>
      <div class="meta-item"><span class="meta-label">Timeout</span><span class="meta-value">${job.timeout_seconds}s</span></div>
      <div class="meta-item"><span class="meta-label">Lease Owner</span><span class="meta-value">${job.lease_owner || "None (Released)"}</span></div>
      <div class="meta-item"><span class="meta-label">Scheduled For</span><span class="meta-value">${new Date(job.scheduled_at).toLocaleString()}</span></div>
    `;

    modalPayload.textContent = JSON.stringify(job.payload, null, 2);

    if (job.result) {
      modalResult.textContent = JSON.stringify(job.result, null, 2);
      document.getElementById("modal-result-section").style.display = "block";
    } else {
      document.getElementById("modal-result-section").style.display = "none";
    }

    if (job.last_error) {
      modalError.textContent = JSON.stringify(job.last_error, null, 2);
      modalErrorSection.style.display = "block";
    } else {
      modalErrorSection.style.display = "none";
    }

    // Attempts Timeline
    if (attempts.length === 0) {
      modalAttemptsContainer.innerHTML = `<p class="text-muted">No execution attempts recorded yet.</p>`;
    } else {
      modalAttemptsContainer.innerHTML = attempts.map(att => {
        const dur = att.execution_duration_seconds ? `${(att.execution_duration_seconds * 1000).toFixed(1)} ms` : "in-flight";
        return `
          <div class="attempt-card">
            <div>
              <strong>Attempt #${att.attempt_number}</strong> • <span class="status-badge badge-${att.outcome}">${att.outcome}</span>
              <div style="font-size:11px; color:#64748b; margin-top:2px;">
                Worker: <span class="attempt-worker">${att.worker_id}</span> • Started: ${new Date(att.started_at).toLocaleTimeString()}
              </div>
            </div>
            <div style="text-align:right;">
              <div style="font-family:var(--font-mono); font-weight:600;">${dur}</div>
            </div>
          </div>
        `;
      }).join("");
    }

    // Footer actions
    let footerBtns = `<button class="btn btn-secondary" onclick="closeModal()">Close</button>`;
    if (job.status === "QUEUED" || job.status === "RUNNING" || job.status === "SCHEDULED" || job.status === "RETRY_WAIT") {
      footerBtns += `<button class="btn btn-danger" onclick="cancelJob('${job.id}')">Cancel Job</button>`;
    }
    if (job.status === "FAILED" || job.status === "CANCELLED") {
      footerBtns += `<button class="btn btn-primary" onclick="retryJob('${job.id}')">Retry Job</button>`;
    }
    modalFooterActions.innerHTML = footerBtns;

    modalBackdrop.classList.add("active");
  } catch (err) {
    console.error("Open modal error:", err);
  }
}

function closeModal() {
  modalBackdrop.classList.remove("active");
}

modalClose.addEventListener("click", closeModal);
modalBackdrop.addEventListener("click", (e) => {
  if (e.target === modalBackdrop) closeModal();
});

// ==========================================================================
// WORKFLOW GUIDE MODAL
// ==========================================================================
function openWorkflowModal() {
  workflowModalBackdrop.classList.add("active");
  addSystemTrace("client", "User opened System Architecture & Exact Workflow guide.", "Documentation");
}

function closeWorkflowModal() {
  workflowModalBackdrop.classList.remove("active");
}

if (btnOpenWorkflow) btnOpenWorkflow.addEventListener("click", openWorkflowModal);
if (btnBannerFullGuide) btnBannerFullGuide.addEventListener("click", openWorkflowModal);
if (workflowModalClose) workflowModalClose.addEventListener("click", closeWorkflowModal);
if (workflowModalBackdrop) {
  workflowModalBackdrop.addEventListener("click", (e) => {
    if (e.target === workflowModalBackdrop) closeWorkflowModal();
  });
}

// Workflow modal tabs
document.querySelectorAll(".workflow-nav-btn").forEach(btn => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".workflow-nav-btn").forEach(b => b.classList.remove("active"));
    document.querySelectorAll(".wf-content-view").forEach(v => v.classList.remove("active"));

    btn.classList.add("active");
    const targetId = btn.getAttribute("data-view");
    const targetEl = document.getElementById(targetId);
    if (targetEl) targetEl.classList.add("active");
  });
});

// ==========================================================================
// ON-PAGE WORKFLOW BANNER TOGGLE & TABS
// ==========================================================================
if (btnToggleGuideDetails) {
  btnToggleGuideDetails.addEventListener("click", () => {
    const isHidden = guideDetailsPanel.style.display === "none";
    if (isHidden) {
      guideDetailsPanel.style.display = "block";
      guideToggleText.textContent = "Hide Workflow Instructions ▲";
    } else {
      guideDetailsPanel.style.display = "none";
      guideToggleText.textContent = "Show Workflow Instructions ▼";
    }
  });
}

document.querySelectorAll(".guide-tab-btn").forEach(btn => {
  btn.addEventListener("click", () => {
    document.querySelectorAll(".guide-tab-btn").forEach(b => b.classList.remove("active"));
    document.querySelectorAll(".guide-tab-pane").forEach(p => p.classList.remove("active"));

    btn.classList.add("active");
    const targetId = btn.getAttribute("data-tab");
    const targetEl = document.getElementById(targetId);
    if (targetEl) targetEl.classList.add("active");
  });
});

// ==========================================================================
// CLEAR TRACE & RESET FILTERS
// ==========================================================================
if (btnClearTrace) {
  btnClearTrace.addEventListener("click", () => {
    traceTerminal.innerHTML = "";
    addSystemTrace("client", "Trace feed cleared by user.", "Reset");
  });
}

if (btnResetFilters) {
  btnResetFilters.addEventListener("click", () => {
    filterSearch.value = "";
    filterStatus.value = "";
    currentFilter.search = "";
    currentFilter.status = "";
    fetchJobs();
    showToast("Filters reset", "info");
  });
}

// ==========================================================================
// INTERACTIVE STATE MACHINE PIPELINE NODES
// ==========================================================================
document.querySelectorAll("[data-filter]").forEach(el => {
  el.style.cursor = "pointer";
  el.addEventListener("click", () => {
    const statusVal = el.getAttribute("data-filter");
    filterStatus.value = statusVal;
    currentFilter.status = statusVal;
    fetchJobs();
    showToast(`Filtering jobs by ${statusVal}`, "info");
    addSystemTrace("client", `Filtering registry table by status: ${statusVal}`, "Table Filter");
  });
});

// ==========================================================================
// CUSTOM JOB DISPATCH STUDIO
// ==========================================================================
async function submitCustomJob(e) {
  e.preventDefault();
  const jobType = selectJobType.value;
  const priority = parseInt(inputPriority.value, 10);
  const delaySec = parseInt(document.getElementById("input-delay-seconds").value, 10);
  const maxAttempts = parseInt(document.getElementById("input-max-attempts").value, 10);
  const timeoutSec = parseInt(document.getElementById("input-timeout").value, 10);

  let payload = {};

  if (jobType === "math.calculate") {
    const op = document.getElementById("math-op").value;
    if (op === "sum" || op === "multiply") {
      const opsStr = document.getElementById("math-operands").value;
      const operands = opsStr.split(",").map(s => parseInt(s.trim(), 10)).filter(n => !isNaN(n));
      payload = { operation: op, operands };
    } else {
      const n = parseInt(document.getElementById("math-n").value, 10) || 10;
      payload = { operation: op, n };
    }
  } else if (jobType === "notification.simulate") {
    payload = {
      recipient: document.getElementById("notif-recipient").value,
      channel: document.getElementById("notif-channel").value,
      template: document.getElementById("notif-template").value,
      simulate_failure_rate: parseFloat(document.getElementById("notif-fail-rate").value) || 0.0,
      simulated_delay_seconds: parseFloat(document.getElementById("notif-delay").value) || 0.0
    };
  } else if (jobType === "report.generate") {
    payload = {
      report_type: document.getElementById("report-type").value,
      date_range_days: parseInt(document.getElementById("report-days").value, 10) || 30,
      simulated_duration_ms: parseInt(document.getElementById("report-duration").value, 10) || 1000,
      row_count: parseInt(document.getElementById("report-rows").value, 10) || 10000
    };
  }

  const bodyData = {
    job_type: jobType,
    payload,
    priority,
    max_attempts: maxAttempts,
    timeout_seconds: timeoutSec
  };

  if (delaySec > 0) {
    bodyData.scheduled_at = new Date(Date.now() + delaySec * 1000).toISOString();
  }

  try {
    addSystemTrace("client", `Submitting custom job (${jobType}, Priority: ${priority})`, "POST /api/v1/jobs");
    const res = await fetch(`${API_BASE}/api/v1/jobs`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(bodyData)
    });

    if (res.ok) {
      const created = await res.json();
      showToast(`Job ${created.id.substring(0, 8)}... created (${created.status})`, "success");
      addSystemTrace("postgres", `Job ${created.id.substring(0,8)}... committed with initial status: ${created.status}`, "ACID Write");
      await fetchJobs();
      await fetchMetricsAndHealth();
    } else {
      const err = await res.json();
      showToast(err.detail || "Failed to submit job", "error");
    }
  } catch (err) {
    console.error("Submit custom job error:", err);
    showToast("Network error submitting job", "error");
  }
}

function updatePayloadFields() {
  const type = selectJobType.value;
  if (type === "math.calculate") {
    payloadFieldsContainer.innerHTML = `
      <div class="form-row">
        <div class="form-group flex-1">
          <label>Math Operation</label>
          <select id="math-op" class="form-control" onchange="toggleMathInputs()">
            <option value="sum">Sum</option>
            <option value="multiply">Multiply</option>
            <option value="factorial">Factorial</option>
            <option value="fibonacci">Fibonacci</option>
          </select>
        </div>
        <div class="form-group flex-1" id="group-math-operands">
          <label>Operands (comma separated integers)</label>
          <input type="text" id="math-operands" class="form-control" value="10, 25, 45">
        </div>
        <div class="form-group flex-1" id="group-math-n" style="display:none;">
          <label>N (Number)</label>
          <input type="number" id="math-n" class="form-control" value="20" min="0" max="1000">
        </div>
      </div>
    `;
  } else if (type === "notification.simulate") {
    payloadFieldsContainer.innerHTML = `
      <div class="form-row">
        <div class="form-group flex-1">
          <label>Recipient Address / Phone</label>
          <input type="text" id="notif-recipient" class="form-control" value="alice@company.com">
        </div>
        <div class="form-group flex-1">
          <label>Delivery Channel</label>
          <select id="notif-channel" class="form-control">
            <option value="email">Email</option>
            <option value="sms">SMS</option>
            <option value="push">Push Notification</option>
          </select>
        </div>
        <div class="form-group flex-1">
          <label>Template</label>
          <input type="text" id="notif-template" class="form-control" value="AccountVerification">
        </div>
      </div>
      <div class="form-row">
        <div class="form-group flex-1">
          <label>Simulated Failure Probability (0.0 = Reliable, 1.0 = Guaranteed Error)</label>
          <input type="number" step="0.1" min="0.0" max="1.0" id="notif-fail-rate" class="form-control" value="0.0">
        </div>
        <div class="form-group flex-1">
          <label>Simulated Latency (seconds)</label>
          <input type="number" step="0.5" min="0.0" max="10.0" id="notif-delay" class="form-control" value="0.0">
        </div>
      </div>
    `;
  } else if (type === "report.generate") {
    payloadFieldsContainer.innerHTML = `
      <div class="form-row">
        <div class="form-group flex-1">
          <label>Report Type</label>
          <select id="report-type" class="form-control">
            <option value="sales_summary">Sales Summary</option>
            <option value="user_activity">User Activity</option>
            <option value="inventory_audit">Inventory Audit</option>
          </select>
        </div>
        <div class="form-group flex-1">
          <label>Date Range (Days)</label>
          <input type="number" id="report-days" class="form-control" value="30">
        </div>
        <div class="form-group flex-1">
          <label>Simulated Duration (ms)</label>
          <input type="number" id="report-duration" class="form-control" value="800">
        </div>
        <div class="form-group flex-1">
          <label>Rows Processed</label>
          <input type="number" id="report-rows" class="form-control" value="10000">
        </div>
      </div>
    `;
  }
}

function toggleMathInputs() {
  const op = document.getElementById("math-op").value;
  const groupOperands = document.getElementById("group-math-operands");
  const groupN = document.getElementById("group-math-n");
  if (op === "sum" || op === "multiply") {
    groupOperands.style.display = "block";
    groupN.style.display = "none";
  } else {
    groupOperands.style.display = "none";
    groupN.style.display = "block";
  }
}

// Window attachments for inline event handlers
window.openJobModal = openJobModal;
window.cancelJob = cancelJob;
window.retryJob = retryJob;
window.closeModal = closeModal;
window.toggleMathInputs = toggleMathInputs;

// Event Listeners
selectJobType.addEventListener("change", updatePayloadFields);
inputPriority.addEventListener("input", (e) => {
  priorityBadge.textContent = e.target.value;
});
jobDispatchForm.addEventListener("submit", submitCustomJob);

filterSearch.addEventListener("input", (e) => {
  currentFilter.search = e.target.value;
  renderJobsTable();
});

filterStatus.addEventListener("change", (e) => {
  currentFilter.status = e.target.value;
  fetchJobs();
});

btnRefresh.addEventListener("click", () => {
  fetchMetricsAndHealth();
  fetchJobs();
  showToast("Mission Control refreshed", "info");
  addSystemTrace("client", "Manual refresh requested.", "Refreshed");
});

toggleAutoRefresh.addEventListener("change", (e) => {
  isAutoRefresh = e.target.checked;
  if (isAutoRefresh) {
    startAutoPolling();
    showToast("Live streaming resumed", "info");
    addSystemTrace("client", "Live polling resumed (1.5s interval).", "Streaming");
  } else {
    clearInterval(refreshTimer);
    showToast("Live streaming paused", "info");
    addSystemTrace("client", "Live polling paused.", "Paused");
  }
});

// Scenario Button Handlers
document.getElementById("scenario-immediate").addEventListener("click", () => runScenario("immediate"));
document.getElementById("scenario-scheduled").addEventListener("click", () => runScenario("scheduled"));
document.getElementById("scenario-retry").addEventListener("click", () => runScenario("retry"));
document.getElementById("scenario-cancel").addEventListener("click", () => runScenario("cancel"));
document.getElementById("scenario-burst").addEventListener("click", () => runScenario("burst"));

// ==========================================================================
// POLLING CYCLE
// ==========================================================================
function startAutoPolling() {
  clearInterval(refreshTimer);
  refreshTimer = setInterval(() => {
    if (isAutoRefresh) {
      fetchMetricsAndHealth();
      fetchJobs();
    }
  }, 1500);
}

// Initialization
updatePayloadFields();
fetchMetricsAndHealth();
fetchJobs();
startAutoPolling();

// Initial system traces
addSystemTrace("client", "Mission Control dashboard initialized. Connected to FastAPI gateway.", "Ready");
addSystemTrace("postgres", "Authoritative schema verified: jobs (ACID state), job_attempts (Audit logs).", "Connected");
addSystemTrace("redis", "Ephemeral priority queue buffer active: queue:default", "Connected");
addSystemTrace("scheduler", "Daemon active: polling scheduled_at <= NOW() with SKIP LOCKED every 1.0s", "Armed");
addSystemTrace("worker", "Worker pool active: autonomous processes claiming leases with version fencing", "Ready");
addSystemTrace("reaper", "Crash recovery sweeper armed: checking for expired leases every 5.0s", "Armed");
