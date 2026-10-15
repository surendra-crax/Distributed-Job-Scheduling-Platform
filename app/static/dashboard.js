// ==========================================================================
// MISSION CONTROL DASHBOARD JAVASCRIPT — DISTRIBUTED JOB SCHEDULER
// ==========================================================================

const API_BASE = "";

// State
let isAutoRefresh = true;
let refreshTimer = null;
let currentJobs = [];
let currentFilter = { search: "", status: "" };

// DOM Elements
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

// Controls
const toggleAutoRefresh = document.getElementById("toggle-autorefresh");
const btnRefresh = document.getElementById("btn-refresh");

// Form
const jobDispatchForm = document.getElementById("job-dispatch-form");
const selectJobType = document.getElementById("select-job-type");
const inputPriority = document.getElementById("input-priority");
const priorityBadge = document.getElementById("priority-badge");
const payloadFieldsContainer = document.getElementById("payload-fields-container");

// Modal
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
        valPostgres.textContent = `${healthData.checks.database.latency_ms}ms`;
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
// FETCH JOBS TABLE
// ==========================================================================
async function fetchJobs() {
  try {
    let url = `${API_BASE}/api/v1/jobs?limit=50`;
    if (currentFilter.status) {
      url += `&status=${currentFilter.status}`;
    }

    const res = await fetch(url);
    if (!res.ok) return;

    const data = await res.json();
    currentJobs = data.items;
    renderJobsTable();
  } catch (err) {
    console.warn("Jobs stream error:", err);
  }
}

function renderJobsTable() {
  let filtered = currentJobs;
  if (currentFilter.search) {
    const q = currentFilter.search.toLowerCase();
    filtered = filtered.filter(j => j.id.toLowerCase().includes(q) || j.job_type.toLowerCase().includes(q));
  }

  tableTotalCount.textContent = `${filtered.length} Jobs (${currentJobs.length} loaded)`;

  if (filtered.length === 0) {
    tableBody.innerHTML = `<tr><td colspan="8" class="text-center text-muted py-8">No matching jobs found in cluster.</td></tr>`;
    return;
  }

  tableBody.innerHTML = filtered.map(job => {
    const shortId = job.id.substring(0, 8);
    const prioClass = job.priority >= 80 ? "prio-high" : job.priority >= 40 ? "prio-medium" : "prio-low";
    const isRunning = job.status === "RUNNING";
    const isQueued = job.status === "QUEUED";
    const isFailed = job.status === "FAILED";
    const isCancelled = job.status === "CANCELLED";

    const age = formatTimeAgo(new Date(job.created_at));

    return `
      <tr>
        <td>
          <span class="cell-job-id" onclick="openJobModal('${job.id}')" title="Click to Inspect Details">
            <span>${shortId}...</span>
            <small style="opacity:0.6;">🔍</small>
          </span>
        </td>
        <td><code>${job.job_type}</code></td>
        <td><span class="priority-chip ${prioClass}">P${job.priority}</span></td>
        <td><span class="status-badge badge-${job.status}">${job.status}</span></td>
        <td><code>${job.attempt_count} / ${job.max_attempts}</code></td>
        <td><span title="${job.created_at}">${age}</span></td>
        <td><span class="fencing-version">v${job.version}</span></td>
        <td class="table-actions">
          <button class="btn btn-secondary btn-sm" onclick="openJobModal('${job.id}')">Inspect</button>
          ${(isRunning || isQueued || job.status === "SCHEDULED" || job.status === "RETRY_WAIT") ? 
            `<button class="btn btn-danger btn-sm" onclick="cancelJob('${job.id}')">Cancel</button>` : ''}
          ${(isFailed || isCancelled) ? 
            `<button class="btn btn-primary btn-sm" onclick="retryJob('${job.id}')">Retry</button>` : ''}
        </td>
      </tr>
    `;
  }).join("");
}

function formatTimeAgo(date) {
  const diffSec = Math.floor((new Date() - date) / 1000);
  if (diffSec < 5) return "just now";
  if (diffSec < 60) return `${diffSec}s ago`;
  if (diffSec < 3600) return `${Math.floor(diffSec / 60)}m ago`;
  return `${Math.floor(diffSec / 3600)}h ago`;
}

// ==========================================================================
// JOB ACTIONS: SUBMIT, CANCEL, RETRY
// ==========================================================================
async function submitCustomJob(e) {
  e.preventDefault();
  const jobType = selectJobType.value;
  const priority = parseInt(inputPriority.value, 10);
  const delaySec = parseInt(document.getElementById("input-delay-seconds").value, 10);
  const maxAttempts = parseInt(document.getElementById("input-max-attempts").value, 10);
  const timeoutSeconds = parseInt(document.getElementById("input-timeout").value, 10);

  let payload = {};
  if (jobType === "math.calculate") {
    const op = document.getElementById("math-op").value;
    if (op === "sum" || op === "multiply") {
      const raw = document.getElementById("math-operands").value;
      payload = { operation: op, operands: raw.split(",").map(x => parseInt(x.trim(), 10)).filter(x => !isNaN(x)) };
    } else {
      payload = { operation: op, n: parseInt(document.getElementById("math-n").value, 10) };
    }
  } else if (jobType === "notification.simulate") {
    payload = {
      recipient: document.getElementById("notif-recipient").value,
      channel: document.getElementById("notif-channel").value,
      template: document.getElementById("notif-template").value,
      simulate_failure_rate: parseFloat(document.getElementById("notif-fail-rate").value),
      simulated_delay_seconds: parseFloat(document.getElementById("notif-delay").value)
    };
  } else if (jobType === "report.generate") {
    payload = {
      report_type: document.getElementById("report-type").value,
      date_range_days: parseInt(document.getElementById("report-days").value, 10),
      simulated_duration_ms: parseInt(document.getElementById("report-duration").value, 10),
      row_count: parseInt(document.getElementById("report-rows").value, 10)
    };
  }

  let scheduledAt = null;
  if (delaySec > 0) {
    const d = new Date(Date.now() + delaySec * 1000);
    scheduledAt = d.toISOString();
  }

  const reqBody = {
    job_type: jobType,
    payload: payload,
    priority: priority,
    max_attempts: maxAttempts,
    timeout_seconds: timeoutSeconds,
    scheduled_at: scheduledAt
  };

  try {
    const res = await fetch(`${API_BASE}/api/v1/jobs`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(reqBody)
    });

    if (res.ok) {
      const job = await res.json();
      showToast(`Job ${job.id.substring(0, 8)} submitted (${job.status})!`, "success");
      await fetchJobs();
      await fetchMetricsAndHealth();
    } else {
      const err = await res.json();
      showToast(`Submission rejected: ${JSON.stringify(err.detail)}`, "error");
    }
  } catch (err) {
    showToast(`Network error: ${err.message}`, "error");
  }
}

async function cancelJob(jobId) {
  try {
    const res = await fetch(`${API_BASE}/api/v1/jobs/${jobId}/cancel`, { method: "POST" });
    if (res.ok) {
      const data = await res.json();
      showToast(`Job ${jobId.substring(0, 8)} cancelled!`, "success");
      await fetchJobs();
      await fetchMetricsAndHealth();
      if (modalBackdrop.classList.contains("active")) {
        openJobModal(jobId);
      }
    } else {
      const err = await res.json();
      showToast(`Cancel failed: ${err.detail.message || "Conflict"}`, "error");
    }
  } catch (err) {
    showToast(`Error: ${err.message}`, "error");
  }
}

async function retryJob(jobId) {
  try {
    const res = await fetch(`${API_BASE}/api/v1/jobs/${jobId}/retry`, { method: "POST" });
    if (res.ok) {
      showToast(`Job ${jobId.substring(0, 8)} re-enqueued for retry!`, "success");
      await fetchJobs();
      await fetchMetricsAndHealth();
      if (modalBackdrop.classList.contains("active")) {
        openJobModal(jobId);
      }
    } else {
      const err = await res.json();
      showToast(`Retry failed: ${err.detail.message || "Conflict"}`, "error");
    }
  } catch (err) {
    showToast(`Error: ${err.message}`, "error");
  }
}

// ==========================================================================
// 1-CLICK INTERACTIVE SCENARIOS
// ==========================================================================
async function runScenario(scenarioKey) {
  if (scenarioKey === "immediate") {
    showToast("Launching immediate high-priority calculation...", "info");
    await fetch(`${API_BASE}/api/v1/jobs`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        job_type: "math.calculate",
        payload: { operation: "fibonacci", n: 35 },
        priority: 95
      })
    });
  } else if (scenarioKey === "scheduled") {
    const target = new Date(Date.now() + 8000).toISOString();
    showToast("Scheduling job for +8 seconds into the future...", "info");
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
      setTimeout(async () => {
        await cancelJob(job.id);
      }, 800);
    }
  } else if (scenarioKey === "burst") {
    showToast("Firing burst of 10 concurrent jobs across worker pool...", "info");
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
      <div class="meta-item"><span class="meta-label">Lease Owner</span><span class="meta-value">${job.lease_owner || "None"}</span></div>
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
// DYNAMIC PAYLOAD FORM GENERATOR
// ==========================================================================
function updatePayloadFields() {
  const type = selectJobType.value;
  if (type === "math.calculate") {
    payloadFieldsContainer.innerHTML = `
      <div class="form-row">
        <div class="form-group flex-1">
          <label>Operation</label>
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
});

toggleAutoRefresh.addEventListener("change", (e) => {
  isAutoRefresh = e.target.checked;
  if (isAutoRefresh) {
    startAutoPolling();
    showToast("Live streaming resumed", "info");
  } else {
    clearInterval(refreshTimer);
    showToast("Live streaming paused", "info");
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
