// MineRelay — Frontend Application Logic

const state = {
  token: localStorage.getItem("minerelay_token") || "",
  account: null,
  status: null,
  sse: null,
  pollTimer: null,
  currentFileDir: "",
  fileSearchQuery: "",
  filesRequestSeq: 0,
  fileCache: {},
  editingFilePath: "",
  consoleFilter: "ALL",
  consoleSearch: "",
  consoleLogs: [],
  cmdHistory: [],
  cmdHistoryIdx: -1,
  pendingAction: null,
  pendingActionAt: 0,
  statusSeq: 0,
};

function clearPendingActionIfSettled(localStatus, lastError) {
  if (!state.pendingAction) return;
  if (Date.now() - state.pendingActionAt > 20000) {
    state.pendingAction = null;
    return;
  }
  if (state.pendingAction === "starting" && (localStatus === "running" || (localStatus === "idle" && lastError))) {
    state.pendingAction = null;
  } else if (state.pendingAction === "stopping" && localStatus === "idle") {
    state.pendingAction = null;
  } else if (state.pendingAction === "restarting" && (localStatus === "running" || (localStatus === "idle" && lastError))) {
    state.pendingAction = null;
  }
}

// ============================================================================
// Helpers & API Client
// ============================================================================

function escapeHtml(str) {
  if (str === null || str === undefined) return "";
  return String(str)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function showToast(message, type = "info") {
  const container = document.getElementById("toast-container");
  const el = document.createElement("div");
  el.className = `toast toast-${type}`;
  el.textContent = message;
  container.appendChild(el);
  setTimeout(() => {
    el.remove();
  }, 4200);
}

function showPasskeyModal(title, passkey, subtitle) {
  document.getElementById("passkey-modal-title").textContent = title;
  document.getElementById("passkey-modal-value").textContent = passkey;
  if (subtitle) {
    document.getElementById("passkey-modal-desc").textContent = subtitle;
  }
  document.getElementById("passkey-modal").classList.remove("hidden");
}

function hidePasskeyModal() {
  document.getElementById("passkey-modal").classList.add("hidden");
}

async function apiFetch(path, options = {}) {
  const headers = Object.assign({ "Content-Type": "application/json" }, options.headers || {});
  if (state.token) {
    headers["Authorization"] = `Bearer ${state.token}`;
  }
  const resp = await fetch(path, {
    ...options,
    headers,
  });
  const data = await resp.json().catch(() => ({}));
  if (!resp.ok) {
    const err = new Error(data.error || `Request failed (${resp.status})`);
    err.code = data.code;
    err.status = resp.status;
    err.data = data;
    throw err;
  }
  return data;
}

function formatBytes(bytes) {
  if (!bytes || bytes <= 0) return "0 B";
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
}

function formatDuration(sec) {
  if (!sec || sec <= 0) return "0m 0s";
  const h = Math.floor(sec / 3600);
  const m = Math.floor((sec % 3600) / 60);
  const s = Math.floor(sec % 60);
  if (h > 0) return `${h}h ${m}m ${s}s`;
  return `${m}m ${s}s`;
}

function formatTimeAgo(ts) {
  if (!ts || ts <= 0) return "Never";
  const diff = Math.max(0, Math.floor(Date.now() / 1000 - ts));
  if (diff < 60) return "Just now";
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  return new Date(ts * 1000).toLocaleDateString();
}

// ============================================================================
// Setup & Auth Flow
// ============================================================================

async function bootApp() {
  if (state.token) {
    try {
      const me = await apiFetch("/api/auth/me");
      state.account = me.account;
      state.status = me.status;
      enterDashboard();
      return;
    } catch (err) {
      if (!err || err.status === 401 || err.status === 403) {
        state.token = "";
        localStorage.removeItem("minerelay_token");
      }
    } finally {
      document.documentElement.classList.remove("session-restoring");
    }
  } else {
    document.documentElement.classList.remove("session-restoring");
  }
  await checkInitialSetup();
}

async function checkInitialSetup() {
  try {
    const setup = await apiFetch("/api/setup/status");
    document.getElementById("auth-sync-dir-label").textContent = setup.sharedSyncDir;
    document.getElementById("input-sync-dir").value = setup.sharedSyncDir;
    document.getElementById("login-machine-name").value = setup.machineName || "";
    document.getElementById("setup-machine-name").value = setup.machineName || "";

    // Render detected cloud folder quick chips
    const presetsEl = document.getElementById("cloud-folder-presets");
    presetsEl.innerHTML = "";
    (setup.cloudFolders || []).forEach((folder) => {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "chip-btn";
      btn.textContent = `📁 ${folder.label}`;
      btn.addEventListener("click", () => {
        document.getElementById("input-sync-dir").value = folder.path;
      });
      presetsEl.appendChild(btn);
    });

    const uninitNotice = document.getElementById("cluster-uninit-notice");
    if (!setup.initialized) {
      uninitNotice.classList.remove("hidden");
      switchAuthTab("setup");
    } else {
      uninitNotice.classList.add("hidden");
      switchAuthTab("login");
    }
  } catch (err) {
    showToast(`Failed to connect to local server: ${err.message}`, "error");
  }
}

function switchAuthTab(mode) {
  const loginTab = document.getElementById("tab-btn-login");
  const setupTab = document.getElementById("tab-btn-setup");
  const loginForm = document.getElementById("form-login");
  const setupForm = document.getElementById("form-setup");

  if (mode === "setup") {
    setupTab.classList.add("active");
    loginTab.classList.remove("active");
    setupForm.classList.remove("hidden");
    loginForm.classList.add("hidden");
  } else {
    loginTab.classList.add("active");
    setupTab.classList.remove("active");
    loginForm.classList.remove("hidden");
    setupForm.classList.add("hidden");
  }
}

// ============================================================================
// Dashboard Initialization & Real-Time Updates
// ============================================================================

async function enterDashboard() {
  document.documentElement.classList.remove("session-restoring");
  document.getElementById("auth-view").classList.add("hidden");
  document.getElementById("dashboard-view").classList.remove("hidden");

  state.currentFileDir = "";
  state.fileSearchQuery = "";
  state.fileCache = {};

  const isAdmin = state.account && state.account.role === "admin";

  // Role badge & user info
  const roleBadge = document.getElementById("user-role-badge");
  roleBadge.textContent = isAdmin ? "👑 ADMIN" : "🔑 MEMBER HOST";
  roleBadge.className = isAdmin ? "badge badge-admin" : "badge badge-member";
  document.getElementById("user-name-label").textContent = state.account.username;

  // Show/hide Admin-Only elements
  document.querySelectorAll(".admin-only").forEach((el) => {
    if (isAdmin) {
      el.classList.remove("hidden");
    } else {
      el.classList.add("hidden");
    }
  });

  // Show Member notice & read-only console bar if Member
  document.getElementById("member-role-banner").classList.toggle("hidden", isAdmin);
  document.getElementById("member-console-footer").classList.toggle("hidden", isAdmin);

  // Switch back to overview tab
  switchDashboardTab("tab-overview");

  if (state.status) {
    renderStatusSummary(state.status);
  }

  refreshServerStatus();
  if (isAdmin) {
    loadAdminFiles("");
    loadAdminAccounts();
    loadAdminProperties();
    loadAdminGamerules();
    loadAdminPlayers();
    loadAdminBackups();
    loadAdminPlugins("");
  }

  connectEventStream();
  if (state.pollTimer) clearInterval(state.pollTimer);
  state.pollTimer = setInterval(refreshServerStatus, 5000);
}

function leaveDashboard() {
  if (state.sse) {
    state.sse.close();
    state.sse = null;
  }
  if (state.pollTimer) {
    clearInterval(state.pollTimer);
    state.pollTimer = null;
  }
  state.token = "";
  state.account = null;
  state.currentFileDir = "";
  state.fileSearchQuery = "";
  state.fileCache = {};
  localStorage.removeItem("minerelay_token");
  document.documentElement.classList.remove("session-restoring");
  document.getElementById("dashboard-view").classList.add("hidden");
  document.getElementById("auth-view").classList.remove("hidden");
  checkInitialSetup();
}

function connectEventStream() {
  if (state.sse) {
    state.sse.close();
  }
  const es = new EventSource(`/api/events?token=${encodeURIComponent(state.token)}`);
  es.onmessage = (ev) => {
    try {
      const payload = JSON.parse(ev.data);
      if (payload.event === "console_log") {
        appendConsoleLine(payload.data);
      } else if (payload.event === "status_changed" || payload.event === "connected") {
        const incoming = payload.data;
        if (!incoming) return;
        // Ignore stale intermediate events that contradict an active user button click
        if (state.pendingAction === "starting" && incoming.localStatus === "idle" && !incoming.lastError) {
          return;
        }
        if (state.pendingAction === "stopping" && incoming.localStatus === "running") {
          return;
        }
        if (state.pendingAction === "restarting" && incoming.localStatus === "idle" && !incoming.lastError) {
          return;
        }
        clearPendingActionIfSettled(incoming.localStatus, incoming.lastError);
        const prevStatus = state.status ? state.status.localStatus : "idle";
        state.status = incoming;
        renderStatusSummary(incoming);
        if (
          (prevStatus === "starting" || prevStatus === "syncing_pull" || prevStatus === "restarting") &&
          incoming.localStatus === "running"
        ) {
          showToast("✅ Minecraft Server is ONLINE and ready to join!", "success");
        } else if (
          (prevStatus === "starting" || prevStatus === "syncing_pull" || prevStatus === "restarting") &&
          incoming.localStatus === "idle" &&
          incoming.lastError
        ) {
          showToast(`⚠️ Server startup failed: ${incoming.lastError}`, "error");
        }
      } else if (payload.event === "players_updated") {
        if (payload.data) {
          renderPlayersTab(payload.data);
        }
      } else if (payload.event === "lock_updated") {
        if (state.pendingAction) return;
        if (state.status && payload.data) {
          state.status.lock = payload.data;
          const myMachineId = state.status.environment && state.status.environment.machineId;
          const lockedByMe = Boolean(
            payload.data.isLocked && myMachineId && payload.data.machineId === myMachineId
          );
          state.status.isLocalMachineHosting = lockedByMe;
          if (lockedByMe && state.status.localStatus === "idle" && payload.data.status && payload.data.status !== "idle") {
            state.status.localStatus = payload.data.status;
          }
          renderStatusSummary(state.status);
        }
      } else if (payload.event === "heartbeat") {
        if (state.pendingAction) return;
        if (payload.data && payload.data.lock && state.status) {
          state.status.lock = payload.data.lock;
          const myMachineId = state.status.environment && state.status.environment.machineId;
          if (
            payload.data.lock.isLocked &&
            myMachineId &&
            payload.data.lock.machineId === myMachineId &&
            state.status.localStatus === "idle" &&
            payload.data.lock.status &&
            payload.data.lock.status !== "idle"
          ) {
            state.status.localStatus = payload.data.lock.status;
          }
          renderStatusSummary(state.status);
        }
      } else if (payload.event === "tunnel_updated") {
        if (state.status) {
          state.status.tunnel = payload.data;
          renderStatusSummary(state.status);
        }
      }
    } catch (_) {}
  };
  es.onerror = () => {
    // Reconnect automatically if the browser closes the SSE stream permanently
    if (es.readyState === EventSource.CLOSED && state.token && state.sse === es) {
      setTimeout(() => {
        if (state.token && (!state.sse || state.sse === es)) {
          connectEventStream();
        }
      }, 4000);
    }
  };
  state.sse = es;
}

async function refreshServerStatus() {
  if (!state.token) return;
  if (state.pendingAction && Date.now() - state.pendingActionAt < 15000) return;
  const reqSeq = ++state.statusSeq;
  try {
    const summary = await apiFetch("/api/server/status");
    if (reqSeq !== state.statusSeq || state.pendingAction) return;
    state.status = summary;
    if (summary.currentUser) {
      state.account = summary.currentUser;
    }
    renderStatusSummary(summary);
    if (summary.logs) {
      renderAllConsoleLogs(summary.logs);
    }
    const tabPlayers = document.getElementById("tab-players");
    if (tabPlayers && !tabPlayers.classList.contains("hidden") && state.account && state.account.role === "admin") {
      loadAdminPlayers();
    }
  } catch (err) {
    if (err.status === 401) {
      showToast("Session expired or passkey was rotated/revoked.", "error");
      leaveDashboard();
    }
  }
}

// ============================================================================
// Rendering Server Status & Distributed Host Lock Banner
// ============================================================================

function renderStatusSummary(summary) {
  if (!summary) return;
  const cluster = summary.cluster || {};
  const lock = summary.lock || {};
  const env = summary.environment || {};
  const tunnel = summary.tunnel || {};
  const telemetry = summary.telemetry || {};

  document.getElementById("top-server-name").textContent = cluster.serverName || "Shared Minecraft Server";
  document.getElementById("top-version-badge").textContent =
    `${(cluster.mcSoftware || "paper").toUpperCase()} ${cluster.mcVersion || "1.21.4"} • Synced Folder: ${env.sharedSyncDir || ""}`;
  document.getElementById("user-machine-label").textContent = `@${env.machineName || "PC"}`;

  // Hero Status Banner Logic
  const dot = document.getElementById("hero-status-dot");
  const headline = document.getElementById("hero-status-headline");
  const detail = document.getElementById("hero-status-detail");
  const btnStart = document.getElementById("btn-start-host");
  const btnRestart = document.getElementById("btn-restart-host");
  const btnStop = document.getElementById("btn-stop-host");

  const isLockedByThisMachine = Boolean(lock.isLocked && env.machineId && lock.machineId === env.machineId);
  const isLocalHosting = Boolean(state.pendingAction) || isLockedByThisMachine || summary.localStatus !== "idle";
  const isLockedByOther = Boolean(lock.isActivelyLocked && !isLocalHosting);
  const isStaleLock = Boolean(lock.isLocked && lock.isStale && !isLocalHosting);

  if (isLocalHosting) {
    const rawSt =
      summary.localStatus !== "idle"
        ? summary.localStatus
        : isLockedByThisMachine && lock.status && lock.status !== "idle"
          ? lock.status
          : "idle";
    const st = state.pendingAction || rawSt;

    if (st === "running") {
      dot.className = "status-dot status-running";
      headline.textContent = `Hosting Active on Your PC (${env.machineName})`;
      detail.textContent = `You (${lock.hostUsername || state.account.username}) are currently hosting the server (${(cluster.mcSoftware || "paper").toUpperCase()} ${cluster.mcVersion || "26.3"}). When you finish playing, click "Stop & Sync World" so other members can host.`;
      btnStart.classList.add("hidden");
      btnStop.classList.remove("hidden");
      btnStop.textContent = "⏹ Stop & Sync World";
      btnStop.disabled = false;
      if (btnRestart) {
        btnRestart.classList.remove("hidden");
        btnRestart.disabled = false;
        btnRestart.textContent = "↻ Restart";
      }
    } else if (st === "restarting") {
      dot.className = "status-dot status-busy";
      headline.textContent = "Restarting Minecraft Server...";
      detail.textContent =
        "Saving world changes and relaunching Java server. Your host lock and tunnel remain active.";
      btnStart.classList.add("hidden");
      btnStop.classList.add("hidden");
      if (btnRestart) {
        btnRestart.classList.remove("hidden");
        btnRestart.disabled = true;
        btnRestart.textContent = "⏳ Restarting...";
      }
    } else if (st === "syncing_pull" || st === "starting") {
      dot.className = "status-dot status-busy";
      headline.textContent = "Pulling World Snapshot & Starting Server...";
      detail.textContent =
        "Starting Java Minecraft server and zero-port-forward tunnel. Wait a few seconds for the indicator to turn GREEN before joining.";
      if (btnRestart) btnRestart.classList.add("hidden");
      if (state.pendingAction === "starting") {
        // Keep the start button in place disabled while the initial start request is in flight so buttons don't swap/flash
        btnStart.classList.remove("hidden");
        btnStart.disabled = true;
        btnStart.textContent = "⏳ Claiming Lock & Starting...";
        btnStop.classList.add("hidden");
      } else {
        btnStart.classList.add("hidden");
        btnStop.classList.remove("hidden");
        btnStop.textContent = "⏹ Cancel Startup";
        btnStop.disabled = false;
      }
    } else if (st === "stopping" || st === "syncing_push") {
      dot.className = "status-dot status-busy";
      headline.textContent = "Saving & Syncing World to Shared Folder...";
      detail.textContent = "Packaging world changes into shared storage before releasing the host lock.";
      btnStart.classList.add("hidden");
      if (btnRestart) btnRestart.classList.add("hidden");
      btnStop.classList.remove("hidden");
      btnStop.textContent = "⏳ Stopping & Syncing...";
      btnStop.disabled = true;
    } else {
      dot.className = "status-dot status-busy";
      headline.textContent = `Host Lock Held by ${lock.hostUsername || "Your PC"} (${env.machineName})`;
      detail.textContent =
        "A lock from a previous session on this device is active. Click below to stop any background process, sync the world, and release the lock.";
      btnStart.classList.add("hidden");
      if (btnRestart) btnRestart.classList.add("hidden");
      btnStop.classList.remove("hidden");
      btnStop.textContent = "⏹ Stop & Sync World";
      btnStop.disabled = false;
    }
  } else if (isLockedByOther) {
    dot.className = "status-dot status-locked-other";
    headline.textContent = `Currently Hosted by ${lock.hostUsername} (on ${lock.machineName})`;
    detail.textContent = `Local hosting is locked while ${lock.hostUsername} is hosting (${lock.status.toUpperCase()}). Join using the IP on the right!`;
    btnStart.classList.remove("hidden");
    btnStart.disabled = true;
    btnStart.textContent = `🔒 Hosted by ${lock.hostUsername}`;
    btnStop.classList.add("hidden");
    if (btnRestart) btnRestart.classList.add("hidden");
  } else if (isStaleLock) {
    dot.className = "status-dot status-locked-other";
    headline.textContent = `Host Lock Held by ${lock.hostUsername || "Remote Member"} (Heartbeat Delayed)`;
    detail.textContent = `Last update from ${lock.machineName || "remote PC"} was ${formatTimeAgo(lock.lastHeartbeat)}. Confirm they stopped and cloud sync finished before reclaiming host so progress isn't lost.`;
    btnStart.classList.remove("hidden");
    btnStart.disabled = false;
    btnStart.textContent = "▶ Reclaim Host & Start Server";
    btnStop.classList.add("hidden");
    if (btnRestart) btnRestart.classList.add("hidden");
  } else {
    dot.className = "status-dot status-idle";
    headline.textContent = "Server is Idle — Ready to Host";
    if (summary.lastError) {
      detail.textContent = `⚠️ Last startup issue: ${summary.lastError} — Ready to try again.`;
    } else {
      detail.textContent =
        "No member is currently hosting. Any member with a passkey can click Start to host locally on their device.";
    }
    btnStart.classList.remove("hidden");
    btnStart.disabled = false;
    btnStart.textContent = "▶ Host Server on My PC";
    btnStop.classList.add("hidden");
    if (btnRestart) btnRestart.classList.add("hidden");
  }

  // Join IP & Tunnel Badge
  const joinAddress =
    lock.publicAddress ||
    tunnel.publicAddress ||
    cluster.publicJoinAddress ||
    `${tunnel.lanIp || "127.0.0.1"}:${cluster.serverPort || 25565}`;
  document.getElementById("hero-join-ip").textContent = joinAddress;

  const hostPart = (joinAddress || "").trim().toLowerCase().split(":")[0];
  const isLanAddress =
    !hostPart ||
    hostPart === "localhost" ||
    hostPart.startsWith("127.") ||
    hostPart.startsWith("192.168.") ||
    hostPart.startsWith("10.") ||
    /^172\.(1[6-9]|2\d|3[0-1])\./.test(hostPart);

  const tunnelBadge = document.getElementById("hero-tunnel-badge");
  const tStat = lock.isActivelyLocked ? lock.tunnelStatus : tunnel.status;
  if (!isLanAddress && (tStat === "online" || tStat === "relay_ready")) {
    tunnelBadge.textContent = "PUBLIC TUNNEL ONLINE";
    tunnelBadge.className = "badge badge-member";
  } else if (tStat === "claim_required" || tunnel.claimUrl) {
    tunnelBadge.textContent = "CLAIM LINK READY";
    tunnelBadge.className = "badge badge-admin";
  } else if (isLanAddress && lock.isActivelyLocked) {
    tunnelBadge.textContent = "LAN ONLY (SAME WI-FI)";
    tunnelBadge.className = "badge badge-outline";
  } else if (tStat === "starting" || tStat === "lan_only") {
    tunnelBadge.textContent = "LOCAL / LAN READY";
    tunnelBadge.className = "badge badge-outline";
  } else {
    tunnelBadge.textContent = "TUNNEL STANDBY";
    tunnelBadge.className = "badge badge-outline";
  }

  const heroClaimBanner = document.getElementById("hero-claim-banner");
  if (heroClaimBanner) {
    if (tunnel.claimUrl) {
      heroClaimBanner.classList.remove("hidden");
      document.getElementById("hero-claim-btn").href = tunnel.claimUrl;
    } else if (isLanAddress) {
      heroClaimBanner.classList.remove("hidden");
      document.getElementById("hero-claim-btn").href = "https://playit.gg/account/tunnels";
    } else {
      heroClaimBanner.classList.add("hidden");
    }
  }

  // Telemetry Cards
  document.getElementById("metric-host-user").textContent = lock.isActivelyLocked
    ? `${lock.hostUsername}`
    : lock.isLocked && lock.isStale
      ? `⚠️ ${lock.hostUsername || "Remote Host"} (Stale)`
      : "None (Available)";
  document.getElementById("metric-host-machine").textContent = lock.isActivelyLocked
    ? `Device: ${lock.machineName} (${lock.status})`
    : lock.isLocked && lock.isStale
      ? `Device: ${lock.machineName || "Remote PC"} (${formatTimeAgo(lock.lastHeartbeat)})`
      : "Ready for any member to host";

  const onlineList = summary.onlinePlayers || (lock.isActivelyLocked ? lock.onlinePlayers : []) || lock.onlinePlayers || [];
  const maxP = lock.maxPlayers || 20;
  document.getElementById("metric-players").textContent = `${onlineList.length} / ${maxP}`;
  document.getElementById("metric-players-list").textContent =
    onlineList.length > 0 ? onlineList.join(", ") : "No players currently online";

  // Synchronize Tab 4 dedicated online players list in real time
  renderOnlinePlayersList(onlineList);

  document.getElementById("metric-tps").textContent = lock.isActivelyLocked
    ? `${(telemetry.tps || 20.0).toFixed(1)} TPS`
    : "Offline";
  document.getElementById("metric-cpu").textContent = `CPU Load: ${telemetry.cpuPercent || 0}%`;

  document.getElementById("metric-ram").textContent =
    `${telemetry.memoryUsedMb || 0} / ${telemetry.memoryMaxMb || 4096} MB`;
  document.getElementById("metric-uptime").textContent =
    `Uptime: ${formatDuration(telemetry.uptimeSeconds || 0)}`;

  // Admin Config & Network Tab Status Boxes
  if (state.account && state.account.role === "admin") {
    renderAdminEnvBoxes(summary);
  }
}

function renderAdminEnvBoxes(summary) {
  const env = summary.environment || {};
  const tunnel = summary.tunnel || {};
  const cluster = summary.cluster || {};

  const jarBox = document.getElementById("jar-env-status");
  if (jarBox) {
    jarBox.innerHTML = `
      <div style="display:flex; justify-content:space-between; flex-wrap:wrap; gap:0.5rem; font-size:0.83rem;">
        <div><strong>Java Runtime:</strong> ${env.javaInstalled ? `✅ ${escapeHtml(env.javaVersion)} (Java ${escapeHtml(env.javaMajor || 25)})` : `⚠️ Java ${escapeHtml(env.requiredJavaMajor || 25)}+ will auto-download on Start`}</div>
        <div><strong>Active Server Jar:</strong> ${env.serverJarInstalled ? `✅ ${escapeHtml(env.serverJarName || "server.jar")} (${env.serverJarSizeMb} MB)` : `ℹ️ Will auto-download on Start`}</div>
      </div>
    `;
  }

  const syncDiagBox = document.getElementById("sync-diagnostics-card");
  if (syncDiagBox && env.syncDiagnostics) {
    const d = env.syncDiagnostics;
    const altMirror = Array.isArray(d.mirroredAlternatives) && d.mirroredAlternatives.length > 0 ? d.mirroredAlternatives[0] : "";
    const diagSig = JSON.stringify([
      d.providerDetected,
      d.clusterConfigExists,
      d.latestSnapshotExists,
      d.latestSnapshotSizeBytes,
      d.latestSnapshotModifiedAt,
      d.backupCount,
      d.isShortcutStreamPath,
      altMirror,
      d.cloudTransferActive,
      d.syncManifest && d.syncManifest.snapshotSeq,
    ]);
    if (syncDiagBox.dataset.sig !== diagSig) {
      syncDiagBox.dataset.sig = diagSig;
      const manifestLine = d.syncManifest
        ? `<div><strong>Latest Verified World:</strong> Snapshot #${escapeHtml(d.syncManifest.snapshotSeq)} by ${escapeHtml(d.syncManifest.hostUsername || "Host")} (${formatTimeAgo(d.syncManifest.updatedAt)})</div>`
        : "";
      const transferLine = d.cloudTransferActive
        ? `<div class="alert-box alert-warning" style="margin-top:0.35rem; padding:0.45rem 0.65rem;">⏳ ${escapeHtml(d.cloudTransferActive)}</div>`
        : "";
      const shortcutWarn = d.isShortcutStreamPath
        ? `<div class="alert-box alert-warning" style="margin-top:0.35rem; padding:0.55rem 0.7rem; font-size:0.8rem;">
             ⚠️ <strong>Google Drive Stream Shortcut Detected:</strong> Virtual <code>.shortcut-targets-by-id</code> paths can lag or cache stale lock/world files.
             ${altMirror ? `<div style="margin-top:0.35rem;">Recommended local Google Drive mirror found: <code>${escapeHtml(altMirror)}</code> <button type="button" class="btn btn-primary btn-xs btn-use-mirror-sync" data-path="${escapeHtml(altMirror)}" style="margin-left:0.35rem;">Use Local Mirror</button></div>` : ""}
           </div>`
        : "";
      syncDiagBox.innerHTML = `
        <div style="display:flex; flex-direction:column; gap:0.3rem; font-size:0.83rem;">
          <div><strong>Detected Sync Provider:</strong> <span class="badge badge-member">${escapeHtml(d.providerDetected || "Shared Folder")}</span></div>
          <div><strong>Cluster Config Synced:</strong> ${d.clusterConfigExists ? "✅ cluster-config.json Ready" : "⚠️ Not initialized"}</div>
          <div><strong>Latest World Snapshot:</strong> ${d.latestSnapshotExists ? `✅ latest-server.zip (${formatBytes(d.latestSnapshotSizeBytes)} • ${formatTimeAgo(d.latestSnapshotModifiedAt)})` : "ℹ️ Created on first server stop"}</div>
          ${manifestLine}
          <div><strong>Rolling World Backups:</strong> ${d.backupCount || 0} archives in shared folder</div>
          ${transferLine}
          ${shortcutWarn}
        </div>
      `;
      const mirrorBtn = syncDiagBox.querySelector(".btn-use-mirror-sync");
      if (mirrorBtn) {
        mirrorBtn.addEventListener("click", async () => {
          const targetPath = mirrorBtn.getAttribute("data-path");
          if (!targetPath) return;
          mirrorBtn.disabled = true;
          try {
            await apiFetch("/api/setup/sync-dir", {
              method: "POST",
              body: JSON.stringify({ sharedSyncDir: targetPath }),
            });
            showToast(`Switched Shared Folder to faster local mirror: ${targetPath}`, "success");
            refreshServerStatus();
          } catch (err) {
            showToast(err.message, "error");
            mirrorBtn.disabled = false;
          }
        });
      }
    }
  }

  const dlWrap = document.getElementById("jar-download-progress-wrap");
  if (env.jarDownloading) {
    dlWrap.classList.remove("hidden");
    document.getElementById("jar-download-bar").style.width = `${env.jarDownloadProgress || 0}%`;
    document.getElementById("jar-download-label").textContent = env.jarDownloadStatus || "Downloading...";
  } else if (env.jarDownloadStatus) {
    dlWrap.classList.remove("hidden");
    document.getElementById("jar-download-bar").style.width = `100%`;
    document.getElementById("jar-download-label").textContent = env.jarDownloadStatus;
  }

  const tunnelBox = document.getElementById("tunnel-status-card");
  if (tunnelBox) {
    tunnelBox.innerHTML = `
      <div style="display:flex; flex-direction:column; gap:0.35rem; font-size:0.84rem;">
        <div><strong>Tunnel Status:</strong> <span class="badge badge-outline">${escapeHtml((tunnel.status || "stopped").toUpperCase())}</span></div>
        <div><strong>playit.gg Agent Binary:</strong> ${tunnel.binaryInstalled ? `✅ Installed (${escapeHtml(tunnel.binaryPath)})` : `ℹ️ Using Shared Relay / Click below to auto-download playit.exe`}</div>
        <div><strong>Shared Secret (playit.toml):</strong> ${tunnel.hasSharedSecret ? `✅ Synced in Shared Folder` : `Not set yet`}</div>
        <div><strong>Local LAN Fallback IP:</strong> <code>${escapeHtml(tunnel.lanIp || "127.0.0.1")}:${escapeHtml(cluster.serverPort || 25565)}</code></div>
      </div>
    `;
  }

  const claimAlert = document.getElementById("tunnel-claim-alert");
  if (tunnel.claimUrl) {
    claimAlert.classList.remove("hidden");
    document.getElementById("tunnel-claim-link").href = tunnel.claimUrl;
  } else {
    claimAlert.classList.add("hidden");
  }
}

// ============================================================================
// Console Log Rendering, Filtering & Search
// ============================================================================

let renderedLogIds = new Set();

function matchesConsoleFilter(entry) {
  if (!entry) return false;
  if (state.consoleFilter && state.consoleFilter !== "ALL") {
    const src = (entry.source || "Server").toLowerCase();
    const lvl = (entry.level || "INFO").toUpperCase();
    if (state.consoleFilter === "WARN") {
      if (lvl !== "WARN" && lvl !== "ERROR") return false;
    } else if (src !== state.consoleFilter.toLowerCase()) {
      return false;
    }
  }
  if (state.consoleSearch) {
    const q = state.consoleSearch.toLowerCase();
    const txt = (entry.text || "").toLowerCase();
    const src = (entry.source || "").toLowerCase();
    if (!txt.includes(q) && !src.includes(q)) return false;
  }
  return true;
}

function rebuildConsoleView() {
  const term = document.getElementById("console-output");
  if (!term) return;
  term.innerHTML = "";
  state.consoleLogs.forEach((entry) => {
    if (matchesConsoleFilter(entry)) {
      const div = document.createElement("div");
      div.className = `log-line log-${entry.level || "INFO"} log-${entry.source || "Server"}`;
      div.textContent = entry.text;
      term.appendChild(div);
    }
  });
  const autoScrollEl = document.getElementById("console-autoscroll");
  if (!autoScrollEl || autoScrollEl.checked) {
    term.scrollTop = term.scrollHeight;
  }
}

function renderAllConsoleLogs(logs) {
  const term = document.getElementById("console-output");
  if (!logs || logs.length === 0) return;
  const autoScrollEl = document.getElementById("console-autoscroll");
  const shouldScroll = (!autoScrollEl || autoScrollEl.checked) && (term.scrollTop + term.clientHeight >= term.scrollHeight - 40);
  logs.forEach((entry) => {
    if (!renderedLogIds.has(entry.id)) {
      appendConsoleLine(entry, false);
    }
  });
  if (shouldScroll) {
    term.scrollTop = term.scrollHeight;
  }
}

function appendConsoleLine(entry, autoScroll = true) {
  if (!entry || renderedLogIds.has(entry.id)) return;
  renderedLogIds.add(entry.id);
  state.consoleLogs.push(entry);
  if (state.consoleLogs.length > 600) {
    state.consoleLogs.shift();
  }
  if (!matchesConsoleFilter(entry)) return;
  const term = document.getElementById("console-output");
  const div = document.createElement("div");
  div.className = `log-line log-${entry.level || "INFO"} log-${entry.source || "Server"}`;
  div.textContent = entry.text;
  term.appendChild(div);
  const autoScrollEl = document.getElementById("console-autoscroll");
  if (autoScroll && (!autoScrollEl || autoScrollEl.checked)) {
    term.scrollTop = term.scrollHeight;
  }
}

// ============================================================================
// Admin Tab Loaders (Accounts, Properties, Gamerules, Players, Files, Plugins, Backups)
// ============================================================================

async function loadAdminAccounts() {
  try {
    const data = await apiFetch("/api/admin/accounts");
    const tbody = document.getElementById("accounts-tbody");
    tbody.innerHTML = "";
    (data.accounts || []).forEach((acc) => {
      const tr = document.createElement("tr");
      const isPrimaryAdmin = acc.role === "admin";
      tr.innerHTML = `
        <td><strong>${escapeHtml(acc.username)}</strong></td>
        <td>
          <span class="badge ${isPrimaryAdmin ? "badge-admin" : "badge-member"}">
            ${isPrimaryAdmin ? "👑 Admin" : "Member Host"}
          </span>
        </td>
        <td><code>${escapeHtml(acc.passkeyHint)}</code></td>
        <td>${acc.hostCount || 0}x <span style="color:var(--text-muted);font-size:0.75rem;">(${formatTimeAgo(acc.lastActiveAt)})</span></td>
        <td>
          <div style="display:flex; gap:0.35rem; flex-wrap:wrap;">
            <button class="btn btn-ghost btn-xs btn-rename-acc" title="Rename Account">
              ✎ Rename
            </button>
            <button class="btn btn-secondary btn-xs btn-rotate-pk">
              ↻ New Passkey
            </button>
            ${
              !isPrimaryAdmin
                ? `<button class="btn btn-danger btn-xs btn-delete-acc">Revoke</button>`
                : ""
            }
          </div>
        </td>
      `;
      const renameBtn = tr.querySelector(".btn-rename-acc");
      if (renameBtn) {
        renameBtn.addEventListener("click", async () => {
          const newName = prompt(`Enter new username for '${acc.username}':`, acc.username);
          if (!newName || !newName.trim() || newName.trim() === acc.username) return;
          try {
            const res = await apiFetch("/api/admin/accounts/rename", {
              method: "POST",
              body: JSON.stringify({ userId: acc.id, username: newName.trim() }),
            });
            showToast(`Renamed account to '${res.account.username}'.`, "success");
            if (state.account && state.account.id === acc.id) {
              state.account.username = res.account.username;
              document.getElementById("user-name-label").textContent = res.account.username;
            }
            loadAdminAccounts();
          } catch (err) {
            showToast(err.message, "error");
          }
        });
      }
      const rotBtn = tr.querySelector(".btn-rotate-pk");
      if (rotBtn) {
        rotBtn.addEventListener("click", async () => {
          try {
            const res = await apiFetch("/api/admin/accounts/rotate", {
              method: "POST",
              body: JSON.stringify({ userId: acc.id }),
            });
            showPasskeyModal(
              `New Passkey for ${acc.username}`,
              res.generatedPasskey,
              `The previous passkey for ${acc.username} has been invalidated. Share this new passkey with them:`
            );
            loadAdminAccounts();
          } catch (err) {
            showToast(err.message, "error");
          }
        });
      }
      const delBtn = tr.querySelector(".btn-delete-acc");
      if (delBtn) {
        delBtn.addEventListener("click", async () => {
          try {
            await apiFetch(`/api/admin/accounts/${encodeURIComponent(acc.id)}`, {
              method: "DELETE",
            });
            showToast(`Revoked member account '${acc.username}'.`, "success");
            loadAdminAccounts();
          } catch (err) {
            showToast(err.message, "error");
          }
        });
      }
      tbody.appendChild(tr);
    });
  } catch (err) {
    showToast(err.message, "error");
  }
}

function updateMotdPreview() {
  const motdEl = document.getElementById("prop-motd");
  const prevText = document.getElementById("motd-preview-text");
  const prevTitle = document.getElementById("motd-preview-title");
  const srvNameEl = document.getElementById("cfg-server-name");
  if (prevTitle && srvNameEl) {
    prevTitle.textContent = srvNameEl.value.trim() || "MineRelay Shared Server";
  }
  if (!motdEl || !prevText) return;
  const raw = motdEl.value || "MineRelay Shared Server";
  // Strip Minecraft §x formatting codes for clean preview display
  const clean = raw.replace(/§[0-9a-fk-or]/gi, "").replace(/\\n/g, " — ");
  prevText.textContent = clean;
}

function formatWorldTypeLabel(levelType) {
  const raw = String(levelType || "minecraft:normal").toLowerCase();
  if (raw.includes("flat")) return "Superflat";
  if (raw.includes("large_biomes")) return "Large Biomes";
  if (raw.includes("amplified")) return "Amplified";
  return "Normal";
}

function renderWorldGeneratorInfo(worldInfo, props = {}) {
  const w = worldInfo || {};
  const activeSeed = w.seed !== undefined ? w.seed : (props["level-seed"] || "");
  const levelType = w.levelType || props["level-type"] || "minecraft:normal";
  const normalizedType = levelType.includes(":") ? levelType : `minecraft:${levelType}`;
  const levelName = w.levelName || props["level-name"] || "world";
  const genStructures = w.generateStructures !== undefined
    ? Boolean(w.generateStructures)
    : props["generate-structures"] !== "false";

  const seedDisp = document.getElementById("world-current-seed-display");
  if (seedDisp) {
    seedDisp.textContent = activeSeed ? String(activeSeed) : "Random / Default";
  }
  const typeDisp = document.getElementById("world-current-type-display");
  if (typeDisp) {
    typeDisp.textContent = formatWorldTypeLabel(normalizedType);
  }
  const nameDisp = document.getElementById("world-current-name-display");
  if (nameDisp) {
    nameDisp.textContent = levelName;
  }
  const badgeEl = document.getElementById("world-status-badge");
  if (badgeEl) {
    if (w.worldExists) {
      badgeEl.className = "badge badge-member";
      badgeEl.textContent = `Active Map (${formatBytes(w.worldSizeBytes || 0)})`;
    } else {
      badgeEl.className = "badge badge-outline";
      badgeEl.textContent = "Fresh / Unpacked on Start";
    }
  }

  const typeSelect = document.getElementById("world-level-type");
  if (typeSelect) {
    typeSelect.value = normalizedType;
  }
  const structChk = document.getElementById("world-gen-structures");
  if (structChk) {
    structChk.checked = genStructures;
  }
  const disEndChk = document.getElementById("world-disable-end");
  if (disEndChk) {
    disEndChk.checked = Boolean(w.disableEnd !== undefined ? w.disableEnd : (props["disable-end"] === "true" || props["allow-end"] === "false"));
  }
  const seedInput = document.getElementById("world-seed-input");
  if (seedInput && activeSeed && !seedInput.value.trim()) {
    seedInput.value = String(activeSeed);
  }
  const propSeedInput = document.getElementById("prop-level-seed");
  if (propSeedInput && activeSeed !== undefined) {
    propSeedInput.value = String(activeSeed || "");
  }
}

async function loadAdminProperties() {
  try {
    const [propsRes, statusRes, versRes] = await Promise.all([
      apiFetch("/api/admin/properties"),
      apiFetch("/api/server/status"),
      apiFetch("/api/admin/versions").catch(() => null),
    ]);
    const p = propsRes.properties || {};
    const c = statusRes.cluster || {};
    const env = statusRes.environment || {};

    if (versRes && versRes.versions) {
      const sw = c.mcSoftware || "paper";
      const list = versRes.versions[sw] || versRes.versions.paper || [];
      const latestRel = versRes.versions.latestRelease || "26.3";
      if (list.length > 0) {
        const sel = document.getElementById("cfg-mc-version");
        sel.innerHTML = "";
        list.forEach((v) => {
          const opt = document.createElement("option");
          opt.value = v;
          opt.textContent = v === latestRel ? `${v} (Latest Official Release)` : v;
          sel.appendChild(opt);
        });
      }
    }

    // Cluster & JVM form
    document.getElementById("cfg-server-name").value = c.serverName || "";
    document.getElementById("cfg-mc-software").value = c.mcSoftware || "paper";
    document.getElementById("cfg-mc-version").value = c.mcVersion || "26.3";
    document.getElementById("cfg-mem-min").value = c.memoryMinMb || 2048;
    document.getElementById("cfg-mem-max").value = c.memoryMaxMb || 4096;
    document.getElementById("cfg-java-path").value = c.javaPath || "java";
    document.getElementById("cfg-server-port").value = c.serverPort || 25565;
    const idleSel = document.getElementById("cfg-idle-shutdown");
    if (idleSel) {
      idleSel.value = String(c.idleShutdownMinutes || 0);
    }
    document.getElementById("cfg-jvm-flags").value = c.jvmFlags || "";

    // Network & Sync tab inputs
    document.getElementById("net-public-address").value = c.publicJoinAddress || "";
    document.getElementById("net-sync-dir").value = env.sharedSyncDir || "";
    document.getElementById("net-auto-backup").checked = c.autoBackupOnStop !== false;
    document.getElementById("net-git-sync").checked = !!c.gitSyncEnabled;

    // server.properties visual form
    document.getElementById("prop-motd").value = p["motd"] || "";
    document.getElementById("prop-gamemode").value = p["gamemode"] || "survival";
    document.getElementById("prop-difficulty").value = p["difficulty"] || "normal";
    document.getElementById("prop-max-players").value = p["max-players"] || "20";
    document.getElementById("prop-view-distance").value = p["view-distance"] || "10";
    document.getElementById("prop-sim-distance").value = p["simulation-distance"] || "10";
    document.getElementById("prop-online-mode").checked = p["online-mode"] !== "false";
    document.getElementById("prop-pvp").checked = p["pvp"] !== "false";
    document.getElementById("prop-whitelist").checked = p["white-list"] === "true";
    document.getElementById("prop-allow-flight").checked = p["allow-flight"] === "true";
    document.getElementById("prop-command-block").checked = p["enable-command-block"] !== "false";
    const hcEl = document.getElementById("prop-hardcore");
    if (hcEl) hcEl.checked = p["hardcore"] === "true";
    const netherEl = document.getElementById("prop-allow-nether");
    if (netherEl) netherEl.checked = p["allow-nether"] !== "false";
    const endEl = document.getElementById("prop-disable-end");
    if (endEl) endEl.checked = p["disable-end"] === "true" || p["allow-end"] === "false";
    document.getElementById("prop-level-seed").value = p["level-seed"] || "";
    const spEl = document.getElementById("prop-spawn-protection");
    if (spEl) spEl.value = p["spawn-protection"] || "0";

    renderWorldGeneratorInfo(propsRes.world, p);
    updateMotdPreview();
  } catch (err) {
    showToast(err.message, "error");
  }
}

async function loadAdminGamerules() {
  try {
    const res = await apiFetch("/api/admin/gamerules");
    const g = res.gamerules || {};
    const setChk = (id, val) => {
      const el = document.getElementById(id);
      if (el) el.checked = Boolean(val);
    };
    const setVal = (id, val) => {
      const el = document.getElementById(id);
      if (el && val !== undefined) el.value = String(val);
    };
    setChk("gr-keepInventory", g.keepInventory);
    setChk("gr-mobGriefing", g.mobGriefing !== false);
    setChk("gr-doInsomnia", g.doInsomnia !== false);
    setChk("gr-doFireTick", g.doFireTick !== false);
    setChk("gr-doDaylightCycle", g.doDaylightCycle !== false);
    setChk("gr-doWeatherCycle", g.doWeatherCycle !== false);
    setChk("gr-doImmediateRespawn", g.doImmediateRespawn);
    setChk("gr-forgiveDeadPlayers", g.forgiveDeadPlayers !== false);
    setChk("gr-announceAdvancements", g.announceAdvancements !== false);
    setVal("gr-playersSleepingPercentage", g.playersSleepingPercentage ?? 100);
    setVal("gr-randomTickSpeed", g.randomTickSpeed ?? 3);
    setVal("gr-spawnRadius", g.spawnRadius ?? 10);
  } catch (err) {
    showToast(err.message, "error");
  }
}

async function loadAdminPlayers() {
  try {
    const data = await apiFetch("/api/admin/players");
    renderPlayersTab(data);
  } catch (err) {
    showToast(err.message, "error");
  }
}

function renderOnlinePlayersList(online) {
  const onlineEl = document.getElementById("online-players-container");
  if (!onlineEl) return;
  onlineEl.innerHTML = "";
  if (!online || online.length === 0) {
    onlineEl.innerHTML = `<span style="color:var(--text-muted);font-size:0.84rem;">No players currently online.</span>`;
    return;
  }
  online.forEach((name) => {
    const row = document.createElement("div");
    row.className = "list-row";
    row.style.width = "100%";
    const safeName = escapeHtml(name);
    const avatarUrl = `https://mc-heads.net/avatar/${encodeURIComponent(name)}/24`;
    row.innerHTML = `
      <div style="display:flex;align-items:center;gap:0.55rem;">
        <img class="player-avatar" src="${avatarUrl}" alt="${safeName}" onerror="this.style.display='none'" />
        <strong>🟢 ${safeName}</strong>
      </div>
      <div style="display:flex;gap:0.35rem;flex-wrap:wrap;align-items:center;">
        <select class="select-sm player-gm-select" style="width:auto;padding:0.18rem 0.4rem;font-size:0.75rem;">
          <option value="">Gamemode...</option>
          <option value="survival">Survival</option>
          <option value="creative">Creative</option>
          <option value="adventure">Adventure</option>
          <option value="spectator">Spectator</option>
        </select>
        <button class="btn btn-ghost btn-xs btn-act-heal" title="Heal & Feed Player">❤️ Heal</button>
        <button class="btn btn-ghost btn-xs btn-act-tpspawn" title="Teleport to World Spawn">🧭 Spawn</button>
        <button class="btn btn-secondary btn-xs btn-act-op">OP</button>
        <button class="btn btn-ghost btn-xs btn-act-kick">Kick</button>
        <button class="btn btn-danger btn-xs btn-act-ban">Ban</button>
      </div>
    `;
    const gmSel = row.querySelector(".player-gm-select");
    if (gmSel) {
      gmSel.addEventListener("change", () => {
        if (!gmSel.value) return;
        triggerPlayerAction("gamemode", name, { value: gmSel.value });
        gmSel.value = "";
      });
    }
    const healBtn = row.querySelector(".btn-act-heal");
    if (healBtn) healBtn.addEventListener("click", () => triggerPlayerAction("heal", name));
    const tpBtn = row.querySelector(".btn-act-tpspawn");
    if (tpBtn) tpBtn.addEventListener("click", () => triggerPlayerAction("tp_spawn", name));
    const opBtn = row.querySelector(".btn-act-op");
    if (opBtn) opBtn.addEventListener("click", () => triggerPlayerAction("op", name));
    const kickBtn = row.querySelector(".btn-act-kick");
    if (kickBtn) kickBtn.addEventListener("click", () => triggerPlayerAction("kick", name));
    const banBtn = row.querySelector(".btn-act-ban");
    if (banBtn) banBtn.addEventListener("click", () => triggerPlayerAction("ban", name));
    onlineEl.appendChild(row);
  });
}

function renderPlayersTab(data) {
  if (!data) return;

  // Online players
  if (data.online !== undefined) {
    renderOnlinePlayersList(data.online);
  }

  // OPs
  const opsEl = document.getElementById("ops-list-container");
  if (opsEl && data.ops) {
    opsEl.innerHTML = "";
    data.ops.forEach((op) => {
      const row = document.createElement("div");
      row.className = "list-row";
      const avatarUrl = `https://mc-heads.net/avatar/${encodeURIComponent(op.name)}/24`;
      row.innerHTML = `
        <span style="display:flex;align-items:center;gap:0.45rem;">
          <img class="player-avatar" src="${avatarUrl}" alt="" onerror="this.style.display='none'" />
          ⭐ <strong>${escapeHtml(op.name)}</strong> <small style="color:var(--text-muted)">(Level ${escapeHtml(op.level || 4)})</small>
        </span>
        <button class="btn btn-ghost btn-xs btn-act-deop">Remove OP</button>
      `;
      const deopBtn = row.querySelector(".btn-act-deop");
      if (deopBtn) deopBtn.addEventListener("click", () => triggerPlayerAction("deop", op.name));
      opsEl.appendChild(row);
    });
  }

  // Whitelist
  const wlEl = document.getElementById("whitelist-container");
  if (wlEl && data.whitelist) {
    wlEl.innerHTML = "";
    data.whitelist.forEach((w) => {
      const row = document.createElement("div");
      row.className = "list-row";
      const avatarUrl = `https://mc-heads.net/avatar/${encodeURIComponent(w.name)}/24`;
      row.innerHTML = `
        <span style="display:flex;align-items:center;gap:0.45rem;">
          <img class="player-avatar" src="${avatarUrl}" alt="" onerror="this.style.display='none'" />
          ✓ <strong>${escapeHtml(w.name)}</strong>
        </span>
        <button class="btn btn-ghost btn-xs btn-act-rmwl">Remove</button>
      `;
      const rmBtn = row.querySelector(".btn-act-rmwl");
      if (rmBtn) rmBtn.addEventListener("click", () => triggerPlayerAction("whitelist_remove", w.name));
      wlEl.appendChild(row);
    });
  }

  // Banned
  const banEl = document.getElementById("banned-container");
  if (banEl && data.banned) {
    banEl.innerHTML = "";
    data.banned.forEach((b) => {
      const row = document.createElement("div");
      row.className = "list-row";
      row.innerHTML = `
        <span>🚫 <strong>${escapeHtml(b.name)}</strong> <small style="color:var(--text-muted)">(${escapeHtml(b.reason || "Banned")})</small></span>
        <button class="btn btn-secondary btn-xs btn-act-pardon">Unban</button>
      `;
      const pardonBtn = row.querySelector(".btn-act-pardon");
      if (pardonBtn) pardonBtn.addEventListener("click", () => triggerPlayerAction("pardon", b.name));
      banEl.appendChild(row);
    });
  }
}

window.triggerPlayerAction = async function (action, username, extra = {}) {
  try {
    const res = await apiFetch("/api/admin/players", {
      method: "POST",
      body: JSON.stringify(Object.assign({ action, username }, extra)),
    });
    renderPlayersTab(res);
    showToast(`Executed ${action} for ${username}`, "success");
  } catch (err) {
    showToast(err.message, "error");
  }
};

async function loadAdminPlugins(searchQuery = null) {
  try {
    const url =
      searchQuery !== null
        ? `/api/admin/plugins?search=${encodeURIComponent(searchQuery)}`
        : `/api/admin/plugins?search=`;
    const data = await apiFetch(url);
    renderInstalledPlugins(data.plugins || []);
    if (data.catalog) {
      renderPluginCatalog(data.catalog);
    }
  } catch (err) {
    showToast(err.message, "error");
  }
}

function renderInstalledPlugins(plugins) {
  const container = document.getElementById("installed-plugins-container");
  if (!container) return;
  container.innerHTML = "";
  if (!plugins || plugins.length === 0) {
    container.innerHTML = `<div style="color:var(--text-muted);font-size:0.82rem;">No plugins installed in <code>/plugins</code> yet. Install one from the catalog below or upload a <code>.jar</code> file.</div>`;
    return;
  }
  plugins.forEach((p) => {
    const row = document.createElement("div");
    row.className = "list-row";
    row.innerHTML = `
      <div>
        <strong>🧩 ${escapeHtml(p.name)}</strong>
        <span class="badge ${p.enabled ? "badge-member" : "badge-outline"}" style="margin-left:0.4rem;">
          ${p.enabled ? "ENABLED" : "DISABLED"}
        </span>
        <div style="font-size:0.74rem;color:var(--text-muted);">${escapeHtml(p.filename)} • ${formatBytes(p.sizeBytes)}</div>
      </div>
      <div style="display:flex;gap:0.35rem;">
        <button class="btn btn-secondary btn-xs btn-toggle-plg">${p.enabled ? "Disable" : "Enable"}</button>
        <button class="btn btn-danger btn-xs btn-del-plg">Delete</button>
      </div>
    `;
    row.querySelector(".btn-toggle-plg").addEventListener("click", async () => {
      try {
        const res = await apiFetch("/api/admin/plugins/toggle", {
          method: "POST",
          body: JSON.stringify({ filename: p.filename }),
        });
        renderInstalledPlugins(res.plugins || []);
        showToast(`${p.enabled ? "Disabled" : "Enabled"} plugin '${p.name}' (restart server to apply).`, "info");
      } catch (err) {
        showToast(err.message, "error");
      }
    });
    row.querySelector(".btn-del-plg").addEventListener("click", async () => {
      try {
        const res = await apiFetch(`/api/admin/plugins?filename=${encodeURIComponent(p.filename)}`, {
          method: "DELETE",
        });
        renderInstalledPlugins(res.plugins || []);
        showToast(`Deleted plugin '${p.name}'.`, "info");
      } catch (err) {
        showToast(err.message, "error");
      }
    });
    container.appendChild(row);
  });
}

function renderPluginCatalog(catalog) {
  const container = document.getElementById("plugin-catalog-container");
  if (!container) return;
  container.innerHTML = "";
  if (!catalog || catalog.length === 0) {
    container.innerHTML = `<div style="color:var(--text-muted);font-size:0.82rem;">No matching Paper/Spigot plugins found on Modrinth.</div>`;
    return;
  }
  catalog.forEach((item) => {
    const card = document.createElement("div");
    card.className = "plugin-card";
    card.innerHTML = `
      <div>
        <div class="plugin-card-header">
          <div class="plugin-card-title">${escapeHtml(item.title || item.slug)}</div>
          <span class="badge badge-outline">${escapeHtml(item.author || "Modrinth")}</span>
        </div>
        <div class="plugin-card-desc">${escapeHtml(item.description || "")}</div>
      </div>
      <div class="plugin-card-footer">
        <span style="font-size:0.72rem;color:var(--text-muted);">⬇ ${(item.downloads || 0).toLocaleString()}</span>
        <button class="btn btn-primary btn-xs btn-install-plg">⬇ 1-Click Install</button>
      </div>
    `;
    const btn = card.querySelector(".btn-install-plg");
    btn.addEventListener("click", async () => {
      btn.disabled = true;
      btn.textContent = "⏳ Installing...";
      try {
        const res = await apiFetch("/api/admin/plugins/install", {
          method: "POST",
          body: JSON.stringify({
            projectId: item.projectId || item.slug,
            title: item.title || item.slug,
          }),
        });
        renderInstalledPlugins(res.plugins || []);
        loadAdminFiles(state.currentFileDir);
        showToast(`Installed '${item.title || item.slug}' into /plugins!`, "success");
        btn.textContent = "✓ Installed";
      } catch (err) {
        showToast(err.message, "error");
        btn.disabled = false;
        btn.textContent = "⬇ 1-Click Install";
      }
    });
    container.appendChild(card);
  });
}

async function loadAdminBackups() {
  try {
    const data = await apiFetch("/api/admin/backups");
    renderBackupsList(data.backups || []);
  } catch (err) {
    showToast(err.message, "error");
  }
}

function renderBackupsList(backups) {
  const container = document.getElementById("backups-list-container");
  container.innerHTML = "";
  if (!backups || backups.length === 0) {
    container.innerHTML = `<div style="color:var(--text-muted);font-size:0.84rem;">No backup snapshots yet. Snapshots are created automatically when stopping the server or manually above.</div>`;
    return;
  }
  backups.forEach((b) => {
    const row = document.createElement("div");
    row.className = "list-row";
    const dateStr = new Date(b.createdAt * 1000).toLocaleString();
    const dlUrl = `/api/admin/backups/download?name=${encodeURIComponent(b.name)}&token=${encodeURIComponent(state.token)}`;
    row.innerHTML = `
      <div>
        <strong>📦 ${escapeHtml(b.name)}</strong>
        <div style="font-size:0.75rem;color:var(--text-muted);">${formatBytes(b.sizeBytes)} • ${escapeHtml(dateStr)}</div>
      </div>
      <div style="display:flex;gap:0.35rem;flex-wrap:wrap;">
        <a href="${dlUrl}" class="btn btn-ghost btn-xs" download="${escapeHtml(b.name)}">⬇ Download</a>
        <button class="btn btn-secondary btn-xs btn-restore-bk">Restore</button>
        <button class="btn btn-danger btn-xs btn-del-bk">Delete</button>
      </div>
    `;
    const restoreBtn = row.querySelector(".btn-restore-bk");
    if (restoreBtn) {
      let confirmStep = false;
      let confirmResetTimer = null;
      restoreBtn.addEventListener("click", async () => {
        if (restoreBtn.disabled) return;
        if (!confirmStep) {
          confirmStep = true;
          restoreBtn.textContent = "Confirm Restore?";
          restoreBtn.classList.remove("btn-secondary");
          restoreBtn.classList.add("btn-primary");
          clearTimeout(confirmResetTimer);
          confirmResetTimer = setTimeout(() => {
            confirmStep = false;
            restoreBtn.textContent = "Restore";
            restoreBtn.classList.remove("btn-primary");
            restoreBtn.classList.add("btn-secondary");
          }, 4500);
          return;
        }
        clearTimeout(confirmResetTimer);
        confirmStep = false;
        const allRestoreBtns = Array.from(container.querySelectorAll(".btn-restore-bk"));
        allRestoreBtns.forEach((btn) => { btn.disabled = true; });
        restoreBtn.textContent = "⏳ Restoring...";
        try {
          const res = await apiFetch("/api/admin/backups/restore", {
            method: "POST",
            body: JSON.stringify({ name: b.name }),
          });
          state.fileCache = {};
          state.currentFileDir = "";
          if (res.status) {
            state.status = res.status;
            renderStatusSummary(res.status);
          }
          if (Array.isArray(res.backups)) {
            renderBackupsList(res.backups);
          } else {
            loadAdminBackups();
          }
          loadAdminProperties();
          loadAdminGamerules();
          loadAdminPlayers();
          loadAdminFiles("", "");
          loadAdminPlugins("");
          showToast(`Restored backup '${b.name}'! Server world and config updated.`, "success");
        } catch (err) {
          allRestoreBtns.forEach((btn) => { btn.disabled = false; });
          restoreBtn.textContent = "Restore";
          restoreBtn.classList.remove("btn-primary");
          restoreBtn.classList.add("btn-secondary");
          showToast(err.message, "error");
        }
      });
    }
    const delBtn = row.querySelector(".btn-del-bk");
    if (delBtn) {
      delBtn.addEventListener("click", async () => {
        try {
          const res = await apiFetch(`/api/admin/backups/${encodeURIComponent(b.name)}`, {
            method: "DELETE",
          });
          renderBackupsList(res.backups || []);
          showToast(`Deleted backup '${b.name}'.`, "info");
        } catch (err) {
          showToast(err.message, "error");
        }
      });
    }
    container.appendChild(row);
  });
}

function renderFileBreadcrumbs(currentDir) {
  const nav = document.getElementById("file-breadcrumbs");
  const legacyDir = document.getElementById("file-current-dir");
  if (legacyDir) legacyDir.textContent = "/" + (currentDir || "");
  if (!nav) return;
  nav.innerHTML = "";

  const parts = (currentDir || "").split("/").filter(Boolean);

  // Root folder button / crumb
  if (parts.length === 0) {
    const rootSpan = document.createElement("span");
    rootSpan.className = "breadcrumb-crumb-current";
    rootSpan.textContent = "📁 root";
    nav.appendChild(rootSpan);
  } else {
    const rootBtn = document.createElement("button");
    rootBtn.type = "button";
    rootBtn.className = "breadcrumb-crumb-btn";
    rootBtn.innerHTML = "📁 root";
    rootBtn.title = "Go to root directory";
    rootBtn.addEventListener("click", () => loadAdminFiles("", ""));
    nav.appendChild(rootBtn);
  }

  let accumulated = "";
  parts.forEach((part, idx) => {
    accumulated += (accumulated ? "/" : "") + part;
    const isLast = idx === parts.length - 1;

    const sep = document.createElement("span");
    sep.className = "breadcrumb-divider";
    sep.textContent = "/";
    nav.appendChild(sep);

    if (isLast) {
      const curSpan = document.createElement("span");
      curSpan.className = "breadcrumb-crumb-current";
      curSpan.textContent = part;
      nav.appendChild(curSpan);
    } else {
      const ancestorPath = accumulated;
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "breadcrumb-crumb-btn";
      btn.textContent = part;
      btn.title = `Go back to ${ancestorPath}`;
      btn.addEventListener("click", () => loadAdminFiles(ancestorPath, ""));
      nav.appendChild(btn);
    }
  });
}

function renderFilesList(files, currentDir, isSearching) {
  const container = document.getElementById("files-list-container");
  if (!container) return;
  container.innerHTML = "";

  // Render Parent Directory ("..") row when inside a subfolder and not searching
  if (currentDir && !isSearching) {
    const parentParts = currentDir.split("/").filter(Boolean);
    parentParts.pop();
    const parentPath = parentParts.join("/");
    const parentRow = document.createElement("div");
    parentRow.className = "list-row file-parent-row";
    parentRow.style.cursor = "pointer";
    parentRow.innerHTML = `
      <div style="display:flex; align-items:center; gap:0.55rem;">
        <span style="font-size:1.15rem;">📁</span>
        <strong>..</strong>
        <small style="color:var(--text-muted); font-size:0.75rem;">(Parent Directory)</small>
      </div>
      <button type="button" class="btn btn-ghost btn-xs" style="pointer-events:none;">↑ Up</button>
    `;
    parentRow.addEventListener("click", () => loadAdminFiles(parentPath, ""));
    container.appendChild(parentRow);
  }

  if (!files || files.length === 0) {
    const emptyRow = document.createElement("div");
    emptyRow.style.padding = "1.5rem 1rem";
    emptyRow.style.textAlign = "center";
    emptyRow.style.color = "var(--text-muted)";
    emptyRow.style.fontSize = "0.84rem";
    if (isSearching) {
      emptyRow.innerHTML = `
        No files or folders matching "<strong>${escapeHtml(state.fileSearchQuery)}</strong>".
        <div style="margin-top:0.5rem;">
          <button type="button" class="btn btn-secondary btn-xs" id="btn-clear-empty-search">Clear Search</button>
        </div>
      `;
    } else {
      emptyRow.textContent = "This folder is empty.";
    }
    container.appendChild(emptyRow);
    const clearEmptyBtn = document.getElementById("btn-clear-empty-search");
    if (clearEmptyBtn) {
      clearEmptyBtn.addEventListener("click", () => loadAdminFiles(currentDir, ""));
    }
    return;
  }

  files.forEach((f) => {
    const row = document.createElement("div");
    row.className = "list-row";
    const pathDisplay = isSearching && f.parentDir ? `<div class="file-match-path">in 📁 /${escapeHtml(f.parentDir)}</div>` : "";
    row.innerHTML = `
      <div style="cursor:pointer; display:flex; align-items:center; gap:0.5rem; flex:1; min-width:0;" class="file-item-click">
        <span style="font-size:1.1rem; flex-shrink:0;">${f.isDir ? "📁" : "📄"}</span>
        <div style="overflow:hidden; text-overflow:ellipsis;">
          <strong>${escapeHtml(f.name)}</strong>
          ${!f.isDir ? `<small style="color:var(--text-muted); margin-left:0.35rem;">(${formatBytes(f.sizeBytes)})</small>` : ""}
          ${pathDisplay}
        </div>
      </div>
      <div style="display:flex;gap:0.35rem; align-items:center; flex-shrink:0;">
        ${!f.isDir ? `<button class="btn btn-secondary btn-xs btn-edit-file">Edit</button>` : `<button class="btn btn-ghost btn-xs btn-open-folder">Open</button>`}
        <button class="btn btn-ghost btn-xs btn-del-file" title="Delete">Delete</button>
      </div>
    `;

    const navigateItem = () => {
      if (f.isDir) {
        loadAdminFiles(f.path, "");
      } else {
        openFileInEditor(f.path);
      }
    };

    row.querySelector(".file-item-click").addEventListener("click", navigateItem);
    const openFolderBtn = row.querySelector(".btn-open-folder");
    if (openFolderBtn) openFolderBtn.addEventListener("click", navigateItem);
    const editBtn = row.querySelector(".btn-edit-file");
    if (editBtn) editBtn.addEventListener("click", () => openFileInEditor(f.path));

    row.querySelector(".btn-del-file").addEventListener("click", async () => {
      try {
        await apiFetch(`/api/admin/files?path=${encodeURIComponent(f.path)}`, {
          method: "DELETE",
        });
        loadAdminFiles(state.currentFileDir, state.fileSearchQuery);
        showToast(`Deleted ${f.name}`, "info");
      } catch (err) {
        showToast(err.message, "error");
      }
    });

    container.appendChild(row);
  });
}

async function loadAdminFiles(dirPath = "", searchQuery = "") {
  const reqSeq = ++state.filesRequestSeq;
  const targetDir = (dirPath || "").replace(/^\/+|\/+$/g, "");
  const cleanSearch = (searchQuery || "").trim();

  state.currentFileDir = targetDir;
  state.fileSearchQuery = cleanSearch;

  // Render interactive breadcrumbs with clickable ancestors
  renderFileBreadcrumbs(targetDir);

  // Sync search input and clear button state
  const searchInp = document.getElementById("input-file-search");
  const clearBtn = document.getElementById("btn-clear-file-search");
  if (searchInp && searchInp.value !== cleanSearch) {
    searchInp.value = cleanSearch;
  }
  if (clearBtn) {
    clearBtn.classList.toggle("hidden", !cleanSearch);
  }

  const container = document.getElementById("files-list-container");
  const statusBox = document.getElementById("file-search-status");
  if (!cleanSearch && statusBox) {
    statusBox.classList.add("hidden");
    statusBox.innerHTML = "";
  }
  const hasCached = !cleanSearch && Array.isArray(state.fileCache[targetDir]);
  if (hasCached) {
    renderFilesList(state.fileCache[targetDir], targetDir, false);
  } else if (container) {
    container.innerHTML = `<div class="file-loading-state">⏳ Loading server files...</div>`;
  }

  try {
    const url = `/api/admin/files?dir=${encodeURIComponent(targetDir)}&search=${encodeURIComponent(cleanSearch)}`;
    const data = await apiFetch(url);
    if (reqSeq !== state.filesRequestSeq) {
      return;
    }

    const files = data.files || [];
    if (!cleanSearch) {
      state.fileCache[targetDir] = files;
    }

    // Update search status banner
    if (statusBox) {
      if (cleanSearch) {
        const count = files.length;
        const dirLabel = targetDir ? `/${targetDir}` : "root";
        statusBox.innerHTML = `
          <span>🔍 Found <strong>${count}</strong> match${count === 1 ? "" : "es"} for "<strong>${escapeHtml(cleanSearch)}</strong>" in <code>${escapeHtml(dirLabel)}</code></span>
          <button type="button" class="btn btn-ghost btn-xs" id="btn-clear-search-badge" style="font-size:0.75rem;">Clear Search</button>
        `;
        statusBox.classList.remove("hidden");
        const badgeClearBtn = document.getElementById("btn-clear-search-badge");
        if (badgeClearBtn) {
          badgeClearBtn.addEventListener("click", () => {
            if (searchInp) searchInp.value = "";
            loadAdminFiles(state.currentFileDir, "");
          });
        }
      } else {
        statusBox.classList.add("hidden");
        statusBox.innerHTML = "";
      }
    }

    renderFilesList(files, targetDir, Boolean(cleanSearch));
  } catch (err) {
    if (reqSeq !== state.filesRequestSeq) {
      return;
    }
    if (!hasCached && container) {
      container.innerHTML = `
        <div class="file-loading-state">
          Failed to load files: ${escapeHtml(err.message)}
          <div style="margin-top:0.5rem;">
            <button type="button" class="btn btn-secondary btn-xs" id="btn-retry-files">Retry</button>
          </div>
        </div>
      `;
      document.getElementById("btn-retry-files")?.addEventListener("click", () => {
        loadAdminFiles(targetDir, cleanSearch);
      });
    }
    showToast(err.message, "error");
  }
}

async function openFileInEditor(relPath) {
  try {
    const data = await apiFetch(`/api/admin/files/read?path=${encodeURIComponent(relPath)}`);
    state.editingFilePath = data.path;
    document.getElementById("file-editor-path").textContent = data.path;
    document.getElementById("file-editor-textarea").value = data.content;
    document.getElementById("file-editor-box").classList.remove("hidden");
  } catch (err) {
    showToast(err.message, "error");
  }
}

// ============================================================================
// UI Event Listeners
// ============================================================================

function switchDashboardTab(tabId) {
  document.querySelectorAll(".nav-tab").forEach((btn) => {
    btn.classList.toggle("active", btn.getAttribute("data-tab") === tabId);
  });
  document.querySelectorAll(".tab-panel").forEach((panel) => {
    panel.classList.toggle("hidden", panel.id !== tabId);
    panel.classList.toggle("active", panel.id === tabId);
  });
}

document.addEventListener("DOMContentLoaded", () => {
  // Passkey modal buttons
  document.getElementById("passkey-modal-close").addEventListener("click", hidePasskeyModal);
  document.getElementById("passkey-modal-done").addEventListener("click", hidePasskeyModal);
  document.getElementById("passkey-modal-copy").addEventListener("click", () => {
    const pk = document.getElementById("passkey-modal-value").textContent;
    navigator.clipboard.writeText(pk);
    showToast("Passkey copied to clipboard!", "success");
  });

  // Auth tabs
  document.getElementById("tab-btn-login").addEventListener("click", () => switchAuthTab("login"));
  document.getElementById("tab-btn-setup").addEventListener("click", () => switchAuthTab("setup"));

  // Toggle Shared Sync Folder config in Auth view
  document.getElementById("btn-toggle-sync-config").addEventListener("click", () => {
    document.getElementById("sync-folder-editor").classList.toggle("hidden");
  });

  const browseBtnAuth = document.getElementById("btn-browse-sync-dir");
  if (browseBtnAuth) {
    browseBtnAuth.addEventListener("click", async () => {
      if (browseBtnAuth.disabled) return;
      const origText = browseBtnAuth.textContent;
      browseBtnAuth.disabled = true;
      browseBtnAuth.textContent = "Opening Explorer...";
      try {
        const res = await apiFetch("/api/setup/browse-folder", { method: "POST" });
        if (res.selectedPath) {
          document.getElementById("input-sync-dir").value = res.selectedPath;
          const saveRes = await apiFetch("/api/setup/sync-dir", {
            method: "POST",
            body: JSON.stringify({ sharedSyncDir: res.selectedPath }),
          });
          document.getElementById("auth-sync-dir-label").textContent = saveRes.sharedSyncDir;
          showToast(`Selected Shared Sync Folder: ${saveRes.sharedSyncDir}`, "success");
          checkInitialSetup();
        }
      } catch (err) {
        showToast(err.message, "error");
      } finally {
        browseBtnAuth.disabled = false;
        browseBtnAuth.textContent = origText;
      }
    });
  }

  document.getElementById("btn-save-sync-dir").addEventListener("click", async () => {
    const newDir = document.getElementById("input-sync-dir").value.trim();
    if (!newDir) return;
    try {
      const res = await apiFetch("/api/setup/sync-dir", {
        method: "POST",
        body: JSON.stringify({ sharedSyncDir: newDir }),
      });
      document.getElementById("auth-sync-dir-label").textContent = res.sharedSyncDir;
      document.getElementById("sync-folder-editor").classList.add("hidden");
      showToast("Updated Shared Sync Folder path!", "success");
      checkInitialSetup();
    } catch (err) {
      showToast(err.message, "error");
    }
  });

  // Show/hide passkey input
  document.getElementById("btn-toggle-passkey-vis").addEventListener("click", (e) => {
    const inp = document.getElementById("login-passkey");
    if (inp.type === "password") {
      inp.type = "text";
      e.target.textContent = "Hide Passkey";
    } else {
      inp.type = "password";
      e.target.textContent = "Show Passkey";
    }
  });

  // Login submit
  document.getElementById("form-login").addEventListener("submit", async (e) => {
    e.preventDefault();
    const passkey = document.getElementById("login-passkey").value.trim();
    const machineName = document.getElementById("login-machine-name").value.trim();
    try {
      const res = await apiFetch("/api/auth/login", {
        method: "POST",
        body: JSON.stringify({ passkey, machineName }),
      });
      state.token = res.token;
      state.account = res.account;
      state.status = res.status;
      localStorage.setItem("minerelay_token", res.token);
      showToast(`Signed in as ${res.account.username} (${res.account.role.toUpperCase()})`, "success");
      enterDashboard();
    } catch (err) {
      showToast(err.message, "error");
    }
  });

  // Initial Cluster Setup submit
  document.getElementById("form-setup").addEventListener("submit", async (e) => {
    e.preventDefault();
    const serverName = document.getElementById("setup-server-name").value.trim();
    const adminUsername = document.getElementById("setup-admin-user").value.trim();
    const machineName = document.getElementById("setup-machine-name").value.trim();
    const customPasskey = document.getElementById("setup-custom-passkey").value.trim();
    const sharedSyncDir = document.getElementById("input-sync-dir").value.trim();

    try {
      const res = await apiFetch("/api/setup/init", {
        method: "POST",
        body: JSON.stringify({
          serverName,
          adminUsername,
          machineName,
          customPasskey: customPasskey || null,
          sharedSyncDir: sharedSyncDir || null,
        }),
      });
      state.token = res.token;
      state.account = res.account;
      localStorage.setItem("minerelay_token", res.token);
      enterDashboard();
      showPasskeyModal(
        "👑 Admin Passkey Created",
        res.generatedPasskey,
        "Save your master Admin passkey! You have full control over the server and can create Member passkeys in the Accounts tab."
      );
    } catch (err) {
      showToast(err.message, "error");
    }
  });

  // Logout
  document.getElementById("btn-logout").addEventListener("click", async () => {
    try {
      await apiFetch("/api/auth/logout", { method: "POST" });
    } catch (_) {}
    leaveDashboard();
  });

  // Copy Join IP & Local IP
  document.getElementById("btn-copy-ip").addEventListener("click", () => {
    const ip = document.getElementById("hero-join-ip").textContent;
    navigator.clipboard.writeText(ip);
    showToast(`Copied Server IP: ${ip}`, "success");
  });

  const btnCopyLocalIp = document.getElementById("btn-copy-local-ip");
  if (btnCopyLocalIp) {
    btnCopyLocalIp.addEventListener("click", () => {
      const port = (state.status && state.status.cluster && state.status.cluster.serverPort) || 25565;
      const localIp = `localhost:${port}`;
      navigator.clipboard.writeText(localIp);
      showToast(`Copied Host PC Local Address: ${localIp}`, "success");
    });
  }

  // Start, Restart & Stop Hosting Buttons (Available to both Admin and Member accounts)
  document.getElementById("btn-start-host").addEventListener("click", async () => {
    state.pendingAction = "starting";
    state.pendingActionAt = Date.now();
    state.statusSeq++;
    if (state.status) {
      renderStatusSummary(state.status);
    }
    try {
      const summary = await apiFetch("/api/server/start", { method: "POST" });
      state.pendingAction = null;
      state.status = summary;
      renderStatusSummary(summary);
      if (summary.localStatus === "running") {
        showToast("✅ Minecraft Server is ONLINE and ready to join!", "success");
      } else {
        showToast("Starting Minecraft server... Wait a few seconds for GREEN status before joining!", "info");
      }
    } catch (err) {
      state.pendingAction = null;
      showToast(err.message, "error");
      refreshServerStatus();
    }
  });

  const btnRestartHost = document.getElementById("btn-restart-host");
  if (btnRestartHost) {
    btnRestartHost.addEventListener("click", async () => {
      state.pendingAction = "restarting";
      state.pendingActionAt = Date.now();
      state.statusSeq++;
      if (state.status) {
        renderStatusSummary(state.status);
      }
      try {
        const summary = await apiFetch("/api/server/restart", { method: "POST" });
        state.pendingAction = null;
        state.status = summary;
        renderStatusSummary(summary);
        showToast("Server restarted and world snapshot synced!", "success");
      } catch (err) {
        state.pendingAction = null;
        showToast(err.message, "error");
        refreshServerStatus();
      }
    });
  }

  document.getElementById("btn-stop-host").addEventListener("click", async () => {
    state.pendingAction = "stopping";
    state.pendingActionAt = Date.now();
    state.statusSeq++;
    if (state.status) {
      renderStatusSummary(state.status);
    }
    try {
      const summary = await apiFetch("/api/server/stop", { method: "POST" });
      state.pendingAction = null;
      state.status = summary;
      renderStatusSummary(summary);
      showToast("Server stopped, world synced to Shared Folder, and host lock released!", "success");
      if (state.account && state.account.role === "admin") {
        loadAdminBackups();
      }
    } catch (err) {
      state.pendingAction = null;
      showToast(err.message, "error");
      refreshServerStatus();
    }
  });

  // Navigation Tabs
  document.querySelectorAll(".nav-tab").forEach((btn) => {
    btn.addEventListener("click", () => {
      const target = btn.getAttribute("data-tab");
      switchDashboardTab(target);
      if (state.account && state.account.role === "admin") {
        if (target === "tab-accounts") loadAdminAccounts();
        if (target === "tab-config") {
          loadAdminProperties();
          loadAdminGamerules();
        }
        if (target === "tab-players") loadAdminPlayers();
        if (target === "tab-files") {
          loadAdminFiles(state.currentFileDir);
          loadAdminBackups();
          loadAdminPlugins();
        }
        if (target === "tab-network") loadAdminProperties();
      }
    });
  });

  // Console Toolbar: Filter Pills, Search, Copy & Clear
  document.querySelectorAll(".filter-pill").forEach((pill) => {
    pill.addEventListener("click", () => {
      document.querySelectorAll(".filter-pill").forEach((p) => p.classList.remove("active"));
      pill.classList.add("active");
      state.consoleFilter = pill.getAttribute("data-filter") || "ALL";
      rebuildConsoleView();
    });
  });

  const consoleSearchInp = document.getElementById("console-search");
  if (consoleSearchInp) {
    consoleSearchInp.addEventListener("input", () => {
      state.consoleSearch = consoleSearchInp.value.trim();
      rebuildConsoleView();
    });
  }

  const btnCopyConsole = document.getElementById("btn-copy-console");
  if (btnCopyConsole) {
    btnCopyConsole.addEventListener("click", () => {
      const text = state.consoleLogs
        .filter((e) => matchesConsoleFilter(e))
        .map((e) => e.text)
        .join("\n");
      navigator.clipboard.writeText(text || "");
      showToast("Copied console logs to clipboard!", "success");
    });
  }

  const btnClearConsole = document.getElementById("btn-clear-console");
  if (btnClearConsole) {
    btnClearConsole.addEventListener("click", async () => {
      state.consoleLogs = [];
      renderedLogIds.clear();
      const term = document.getElementById("console-output");
      if (term) term.innerHTML = "";
      if (state.account && state.account.role === "admin") {
        try {
          await apiFetch("/api/admin/console/clear", { method: "POST" });
        } catch (_) {}
      }
      showToast("Cleared console view.", "info");
    });
  }

  // Admin Console Command Submit + Up/Down Arrow History
  const consoleInput = document.getElementById("input-console-cmd");
  if (consoleInput) {
    consoleInput.addEventListener("keydown", (e) => {
      if (e.key === "ArrowUp") {
        if (state.cmdHistory.length === 0) return;
        e.preventDefault();
        if (state.cmdHistoryIdx < state.cmdHistory.length - 1) {
          state.cmdHistoryIdx++;
        }
        consoleInput.value = state.cmdHistory[state.cmdHistory.length - 1 - state.cmdHistoryIdx] || "";
      } else if (e.key === "ArrowDown") {
        if (state.cmdHistoryIdx <= 0) {
          state.cmdHistoryIdx = -1;
          consoleInput.value = "";
          return;
        }
        e.preventDefault();
        state.cmdHistoryIdx--;
        consoleInput.value = state.cmdHistory[state.cmdHistory.length - 1 - state.cmdHistoryIdx] || "";
      }
    });
  }

  document.getElementById("form-console-cmd").addEventListener("submit", async (e) => {
    e.preventDefault();
    const inp = document.getElementById("input-console-cmd");
    const cmd = inp.value.trim();
    if (!cmd) return;
    state.cmdHistory.push(cmd);
    state.cmdHistoryIdx = -1;
    inp.value = "";
    try {
      await apiFetch("/api/admin/console", {
        method: "POST",
        body: JSON.stringify({ command: cmd }),
      });
    } catch (err) {
      showToast(err.message, "error");
    }
  });

  // Quick Console Command Buttons
  document.querySelectorAll(".quick-cmd").forEach((btn) => {
    btn.addEventListener("click", async () => {
      const cmd = btn.getAttribute("data-cmd");
      try {
        await apiFetch("/api/admin/console", {
          method: "POST",
          body: JSON.stringify({ command: cmd }),
        });
      } catch (err) {
        showToast(err.message, "error");
      }
    });
  });

  // Create Member Account Form (Admin Only)
  document.getElementById("form-create-member").addEventListener("submit", async (e) => {
    e.preventDefault();
    const username = document.getElementById("new-member-username").value.trim();
    const customPasskey = document.getElementById("new-member-passkey").value.trim();
    try {
      const res = await apiFetch("/api/admin/accounts", {
        method: "POST",
        body: JSON.stringify({
          username,
          customPasskey: customPasskey || null,
        }),
      });
      document.getElementById("new-member-username").value = "";
      document.getElementById("new-member-passkey").value = "";
      showPasskeyModal(
        `🔑 Member Passkey for ${res.account.username}`,
        res.generatedPasskey,
        `Share this passkey with ${res.account.username}. They will be able to Start, Stop, and Host the server locally whenever nobody else is hosting.`
      );
      loadAdminAccounts();
    } catch (err) {
      showToast(err.message, "error");
    }
  });

  // Download Server Jar Button
  document.getElementById("btn-download-jar").addEventListener("click", async () => {
    const software = document.getElementById("cfg-mc-software").value;
    const version = document.getElementById("cfg-mc-version").value;
    try {
      await apiFetch("/api/admin/install-jar", {
        method: "POST",
        body: JSON.stringify({ software, version }),
      });
      showToast(`Started downloading ${software.toUpperCase()} ${version} server.jar...`, "info");
      refreshServerStatus();
    } catch (err) {
      showToast(err.message, "error");
    }
  });

  // Live MOTD preview listeners
  const motdInput = document.getElementById("prop-motd");
  if (motdInput) motdInput.addEventListener("input", updateMotdPreview);
  const srvNameInput = document.getElementById("cfg-server-name");
  if (srvNameInput) srvNameInput.addEventListener("input", updateMotdPreview);

  // Save JVM & Cluster Settings
  document.getElementById("form-jvm-settings").addEventListener("submit", async (e) => {
    e.preventDefault();
    try {
      const idleSel = document.getElementById("cfg-idle-shutdown");
      await apiFetch("/api/admin/settings", {
        method: "PUT",
        body: JSON.stringify({
          serverName: document.getElementById("cfg-server-name").value.trim(),
          mcSoftware: document.getElementById("cfg-mc-software").value,
          mcVersion: document.getElementById("cfg-mc-version").value,
          memoryMinMb: parseInt(document.getElementById("cfg-mem-min").value, 10),
          memoryMaxMb: parseInt(document.getElementById("cfg-mem-max").value, 10),
          javaPath: document.getElementById("cfg-java-path").value.trim(),
          serverPort: parseInt(document.getElementById("cfg-server-port").value, 10),
          idleShutdownMinutes: idleSel ? parseInt(idleSel.value, 10) : 0,
          jvmFlags: document.getElementById("cfg-jvm-flags").value.trim(),
        }),
      });
      showToast("Saved JVM and cluster configuration!", "success");
      refreshServerStatus();
    } catch (err) {
      showToast(err.message, "error");
    }
  });

  // Save server.properties
  document.getElementById("form-server-properties").addEventListener("submit", async (e) => {
    e.preventDefault();
    const hcEl = document.getElementById("prop-hardcore");
    const netherEl = document.getElementById("prop-allow-nether");
    const endEl = document.getElementById("prop-disable-end");
    const spEl = document.getElementById("prop-spawn-protection");
    const updates = {
      motd: document.getElementById("prop-motd").value.trim(),
      gamemode: document.getElementById("prop-gamemode").value,
      difficulty: document.getElementById("prop-difficulty").value,
      "max-players": document.getElementById("prop-max-players").value,
      "view-distance": document.getElementById("prop-view-distance").value,
      "simulation-distance": document.getElementById("prop-sim-distance").value,
      "online-mode": document.getElementById("prop-online-mode").checked,
      pvp: document.getElementById("prop-pvp").checked,
      "white-list": document.getElementById("prop-whitelist").checked,
      "allow-flight": document.getElementById("prop-allow-flight").checked,
      "enable-command-block": document.getElementById("prop-command-block").checked,
      hardcore: hcEl ? hcEl.checked : false,
      "allow-nether": netherEl ? netherEl.checked : true,
      "disable-end": endEl ? endEl.checked : false,
      "allow-end": endEl ? !endEl.checked : true,
      "level-seed": document.getElementById("prop-level-seed").value.trim(),
      "spawn-protection": spEl ? spEl.value : "0",
    };
    try {
      const res = await apiFetch("/api/admin/properties", {
        method: "PUT",
        body: JSON.stringify({ properties: updates }),
      });
      renderWorldGeneratorInfo(res.world, res.properties || updates);
      showToast("Saved server.properties and synced to Shared Folder!", "success");
    } catch (err) {
      showToast(err.message, "error");
    }
  });

  // World Map & Seed Generator Controls
  const btnCopyWorldSeed = document.getElementById("btn-copy-world-seed");
  if (btnCopyWorldSeed) {
    btnCopyWorldSeed.addEventListener("click", () => {
      const seedDisp = document.getElementById("world-current-seed-display");
      const val = seedDisp ? seedDisp.textContent.trim() : "";
      if (!val || val === "Random / Default") {
        showToast("No explicit seed set yet — generate or roll a seed first!", "info");
        return;
      }
      navigator.clipboard.writeText(val);
      showToast(`Copied world seed '${val}' to clipboard!`, "success");
    });
  }

  function rollRandom64BitSeedString() {
    const arr = new Uint32Array(2);
    if (window.crypto && window.crypto.getRandomValues) {
      window.crypto.getRandomValues(arr);
    } else {
      arr[0] = Math.floor(Math.random() * 0xffffffff);
      arr[1] = Math.floor(Math.random() * 0xffffffff);
    }
    const unsigned = (BigInt(arr[0]) << 32n) | BigInt(arr[1]);
    const signed = BigInt.asIntN(64, unsigned);
    return signed.toString();
  }

  const btnRollSeed = document.getElementById("btn-roll-seed");
  if (btnRollSeed) {
    btnRollSeed.addEventListener("click", () => {
      const rolled = rollRandom64BitSeedString();
      const seedInp = document.getElementById("world-seed-input");
      const propSeedInp = document.getElementById("prop-level-seed");
      if (seedInp) seedInp.value = rolled;
      if (propSeedInp) propSeedInp.value = rolled;
      document.querySelectorAll(".seed-preset-chip").forEach((c) => c.classList.remove("active"));
      showToast(`Rolled random seed: ${rolled}`, "info");
    });
  }

  document.querySelectorAll(".seed-preset-chip").forEach((chip) => {
    chip.addEventListener("click", () => {
      const seedVal = chip.getAttribute("data-seed") || "";
      const typeVal = chip.getAttribute("data-type") || "minecraft:normal";
      const seedInp = document.getElementById("world-seed-input");
      const propSeedInp = document.getElementById("prop-level-seed");
      const typeSel = document.getElementById("world-level-type");
      if (seedInp) seedInp.value = seedVal;
      if (propSeedInp) propSeedInp.value = seedVal;
      if (typeSel) typeSel.value = typeVal;
      document.querySelectorAll(".seed-preset-chip").forEach((c) => c.classList.remove("active"));
      chip.classList.add("active");
    });
  });

  const worldSeedInputEl = document.getElementById("world-seed-input");
  if (worldSeedInputEl) {
    worldSeedInputEl.addEventListener("input", () => {
      const propSeedInp = document.getElementById("prop-level-seed");
      if (propSeedInp) propSeedInp.value = worldSeedInputEl.value.trim();
      document.querySelectorAll(".seed-preset-chip").forEach((c) => {
        c.classList.toggle("active", c.getAttribute("data-seed") === worldSeedInputEl.value.trim());
      });
    });
  }

  let pendingWorldResetMode = null;
  const confirmBox = document.getElementById("world-reset-confirm-box");
  const confirmText = document.getElementById("world-reset-confirm-text");
  const btnConfirmYes = document.getElementById("btn-world-confirm-yes");
  const btnConfirmCancel = document.getElementById("btn-world-confirm-cancel");
  const btnResetRandom = document.getElementById("btn-world-reset-random");
  const btnResetCustom = document.getElementById("btn-world-reset-custom");

  function openWorldResetConfirm(mode) {
    const seedInp = document.getElementById("world-seed-input");
    const customSeed = seedInp ? seedInp.value.trim() : "";
    if (mode === "custom" && !customSeed) {
      if (seedInp) seedInp.focus();
      showToast("Enter a specific seed first (or click a preset / Roll Seed), or click 'Generate Random World Map'.", "error");
      return;
    }
    pendingWorldResetMode = mode;
    const backupChk = document.getElementById("world-backup-existing");
    const willBackup = backupChk ? backupChk.checked : true;
    const backupNote = willBackup
      ? "A safety backup of the current world will be saved to Backups first."
      : "Warning: Auto-backup is unchecked, so the current world will be deleted without a backup.";

    if (confirmText) {
      if (mode === "random") {
        confirmText.textContent = `Generate a brand-new RANDOM world map? ${backupNote}`;
      } else {
        confirmText.textContent = `Generate a brand-new world map using seed '${customSeed}'? ${backupNote}`;
      }
    }
    if (confirmBox) confirmBox.classList.remove("hidden");
  }

  if (btnResetRandom) {
    btnResetRandom.addEventListener("click", () => openWorldResetConfirm("random"));
  }
  if (btnResetCustom) {
    btnResetCustom.addEventListener("click", () => openWorldResetConfirm("custom"));
  }
  if (btnConfirmCancel) {
    btnConfirmCancel.addEventListener("click", () => {
      pendingWorldResetMode = null;
      if (confirmBox) confirmBox.classList.add("hidden");
    });
  }
  if (btnConfirmYes) {
    btnConfirmYes.addEventListener("click", async () => {
      if (!pendingWorldResetMode) return;
      const mode = pendingWorldResetMode;
      const seedInp = document.getElementById("world-seed-input");
      const typeSel = document.getElementById("world-level-type");
      const structChk = document.getElementById("world-gen-structures");
      const disEndChk = document.getElementById("world-disable-end");
      const backupChk = document.getElementById("world-backup-existing");

      const payload = {
        mode,
        seed: mode === "custom" && seedInp ? seedInp.value.trim() : "",
        levelType: typeSel ? typeSel.value : "minecraft:normal",
        generateStructures: structChk ? structChk.checked : true,
        disableEnd: disEndChk ? disEndChk.checked : false,
        backupExisting: backupChk ? backupChk.checked : true,
      };

      const origText = btnConfirmYes.textContent;
      btnConfirmYes.disabled = true;
      btnConfirmYes.textContent = "⏳ Generating New World Map...";
      try {
        const res = await apiFetch("/api/admin/world/reset", {
          method: "POST",
          body: JSON.stringify(payload),
        });
        if (confirmBox) confirmBox.classList.add("hidden");
        pendingWorldResetMode = null;
        if (seedInp && res.seed) {
          seedInp.value = String(res.seed);
        }
        renderWorldGeneratorInfo(res.world, res.properties || {});
        if (res.status) {
          renderStatusSummary(res.status);
        }
        if (res.backupFile) {
          loadAdminBackups().catch(() => {});
        }
        const msg =
          res.mode === "random"
            ? `Generated new random world map (Seed: ${res.seed})!`
            : `Generated new world map with seed '${res.seed}'!`;
        showToast(msg, "success");
      } catch (err) {
        showToast(err.message, "error");
      } finally {
        btnConfirmYes.disabled = false;
        btnConfirmYes.textContent = origText;
      }
    });
  }

  // Save World Gamerules
  const formGamerules = document.getElementById("form-gamerules");
  if (formGamerules) {
    formGamerules.addEventListener("submit", async (e) => {
      e.preventDefault();
      const gamerules = {
        keepInventory: document.getElementById("gr-keepInventory").checked,
        mobGriefing: document.getElementById("gr-mobGriefing").checked,
        doInsomnia: document.getElementById("gr-doInsomnia").checked,
        doFireTick: document.getElementById("gr-doFireTick").checked,
        doDaylightCycle: document.getElementById("gr-doDaylightCycle").checked,
        doWeatherCycle: document.getElementById("gr-doWeatherCycle").checked,
        doImmediateRespawn: document.getElementById("gr-doImmediateRespawn").checked,
        forgiveDeadPlayers: document.getElementById("gr-forgiveDeadPlayers").checked,
        announceAdvancements: document.getElementById("gr-announceAdvancements").checked,
        playersSleepingPercentage: parseInt(document.getElementById("gr-playersSleepingPercentage").value, 10),
        randomTickSpeed: parseInt(document.getElementById("gr-randomTickSpeed").value, 10),
        spawnRadius: parseInt(document.getElementById("gr-spawnRadius").value, 10),
      };
      try {
        await apiFetch("/api/admin/gamerules", {
          method: "PUT",
          body: JSON.stringify({ gamerules }),
        });
        showToast("Saved & applied World Gamerules!", "success");
      } catch (err) {
        showToast(err.message, "error");
      }
    });
  }

  // Plugin Marketplace Search & Refresh
  const formSearchPlugins = document.getElementById("form-search-plugins");
  if (formSearchPlugins) {
    formSearchPlugins.addEventListener("submit", (e) => {
      e.preventDefault();
      const q = document.getElementById("input-plugin-search").value.trim();
      loadAdminPlugins(q);
    });
  }
  const btnRefreshPlugins = document.getElementById("btn-refresh-plugins");
  if (btnRefreshPlugins) {
    btnRefreshPlugins.addEventListener("click", () => {
      const q = document.getElementById("input-plugin-search")
        ? document.getElementById("input-plugin-search").value.trim()
        : "";
      loadAdminPlugins(q);
    });
  }

  // Players Forms
  document.getElementById("form-add-op").addEventListener("submit", async (e) => {
    e.preventDefault();
    const user = document.getElementById("input-op-username").value.trim();
    if (!user) return;
    document.getElementById("input-op-username").value = "";
    await window.triggerPlayerAction("op", user);
  });

  document.getElementById("form-add-whitelist").addEventListener("submit", async (e) => {
    e.preventDefault();
    const user = document.getElementById("input-wl-username").value.trim();
    if (!user) return;
    document.getElementById("input-wl-username").value = "";
    await window.triggerPlayerAction("whitelist_add", user);
  });

  document.getElementById("form-add-ban").addEventListener("submit", async (e) => {
    e.preventDefault();
    const username = document.getElementById("input-ban-username").value.trim();
    const reason = document.getElementById("input-ban-reason").value.trim();
    if (!username) return;
    document.getElementById("input-ban-username").value = "";
    try {
      const res = await apiFetch("/api/admin/players", {
        method: "POST",
        body: JSON.stringify({ action: "ban", username, reason }),
      });
      renderPlayersTab(res);
      showToast(`Banned ${username}`, "success");
    } catch (err) {
      showToast(err.message, "error");
    }
  });

  // Files & Backups Search & Navigation
  document.getElementById("btn-refresh-files").addEventListener("click", () => {
    loadAdminFiles(state.currentFileDir, state.fileSearchQuery);
  });

  document.getElementById("btn-file-up").addEventListener("click", () => {
    if (!state.currentFileDir) {
      if (state.fileSearchQuery) loadAdminFiles("", "");
      return;
    }
    const parts = state.currentFileDir.split("/").filter(Boolean);
    parts.pop();
    loadAdminFiles(parts.join("/"), "");
  });

  let fileSearchTimer = null;
  const fileSearchInput = document.getElementById("input-file-search");
  if (fileSearchInput) {
    fileSearchInput.addEventListener("input", (e) => {
      clearTimeout(fileSearchTimer);
      const query = e.target.value.trim();
      fileSearchTimer = setTimeout(() => {
        loadAdminFiles(state.currentFileDir, query);
      }, 200);
    });
    fileSearchInput.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        clearTimeout(fileSearchTimer);
        loadAdminFiles(state.currentFileDir, e.target.value.trim());
      } else if (e.key === "Escape") {
        e.target.value = "";
        clearTimeout(fileSearchTimer);
        loadAdminFiles(state.currentFileDir, "");
      }
    });
  }

  const btnClearFileSearch = document.getElementById("btn-clear-file-search");
  if (btnClearFileSearch) {
    btnClearFileSearch.addEventListener("click", () => {
      if (fileSearchInput) fileSearchInput.value = "";
      loadAdminFiles(state.currentFileDir, "");
    });
  }

  document.getElementById("btn-close-file").addEventListener("click", () => {
    document.getElementById("file-editor-box").classList.add("hidden");
  });

  document.getElementById("btn-save-file").addEventListener("click", async () => {
    const content = document.getElementById("file-editor-textarea").value;
    try {
      await apiFetch("/api/admin/files/write", {
        method: "POST",
        body: JSON.stringify({ path: state.editingFilePath, content }),
      });
      showToast(`Saved ${state.editingFilePath}!`, "success");
    } catch (err) {
      showToast(err.message, "error");
    }
  });

  document.getElementById("input-file-upload").addEventListener("change", (e) => {
    const file = e.target.files[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = async () => {
      const base64 = reader.result.split(",")[1];
      try {
        await apiFetch("/api/admin/files/upload", {
          method: "POST",
          body: JSON.stringify({
            dir: state.currentFileDir,
            filename: file.name,
            dataBase64: base64,
          }),
        });
        showToast(`Uploaded ${file.name}!`, "success");
        loadAdminFiles(state.currentFileDir);
      } catch (err) {
        showToast(err.message, "error");
      }
    };
    reader.readAsDataURL(file);
  });

  document.getElementById("form-create-backup").addEventListener("submit", async (e) => {
    e.preventDefault();
    const formEl = e.currentTarget;
    const submitBtn = formEl.querySelector("button[type='submit']");
    const origText = submitBtn ? submitBtn.textContent : "";
    if (submitBtn) {
      submitBtn.disabled = true;
      submitBtn.textContent = "⏳ Creating Snapshot...";
    }
    const label = document.getElementById("input-backup-label").value.trim() || "manual";
    try {
      const res = await apiFetch("/api/admin/backups", {
        method: "POST",
        body: JSON.stringify({ label }),
      });
      renderBackupsList(res.backups || []);
      showToast("Created world snapshot backup in Shared Sync Folder!", "success");
    } catch (err) {
      showToast(err.message, "error");
    } finally {
      if (submitBtn) {
        submitBtn.disabled = false;
        submitBtn.textContent = origText;
      }
    }
  });

  // Network & Sync Controls
  document.getElementById("btn-save-public-address").addEventListener("click", async () => {
    const addr = document.getElementById("net-public-address").value.trim();
    try {
      await apiFetch("/api/admin/settings", {
        method: "PUT",
        body: JSON.stringify({ publicJoinAddress: addr }),
      });
      showToast("Updated shared Public Join Address!", "success");
      refreshServerStatus();
    } catch (err) {
      showToast(err.message, "error");
    }
  });

  document.getElementById("btn-save-playit-secret").addEventListener("click", async () => {
    const secretKey = document.getElementById("net-playit-secret").value.trim();
    try {
      await apiFetch("/api/admin/tunnel/secret", {
        method: "POST",
        body: JSON.stringify({ secretKey }),
      });
      document.getElementById("net-playit-secret").value = "";
      showToast("Saved shared playit.gg secret key to Shared Folder!", "success");
      refreshServerStatus();
    } catch (err) {
      showToast(err.message, "error");
    }
  });

  document.getElementById("btn-install-playit").addEventListener("click", async () => {
    try {
      await apiFetch("/api/admin/tunnel/download", { method: "POST" });
      showToast("Started downloading official playit.gg agent binary...", "info");
    } catch (err) {
      showToast(err.message, "error");
    }
  });

  const browseBtnNet = document.getElementById("btn-net-browse-sync");
  if (browseBtnNet) {
    browseBtnNet.addEventListener("click", async () => {
      if (browseBtnNet.disabled) return;
      const origText = browseBtnNet.textContent;
      browseBtnNet.disabled = true;
      browseBtnNet.textContent = "Opening Explorer...";
      try {
        const res = await apiFetch("/api/setup/browse-folder", { method: "POST" });
        if (res.selectedPath) {
          document.getElementById("net-sync-dir").value = res.selectedPath;
          await apiFetch("/api/setup/sync-dir", {
            method: "POST",
            body: JSON.stringify({ sharedSyncDir: res.selectedPath }),
          });
          showToast(`Updated Shared Sync Folder: ${res.selectedPath}`, "success");
          refreshServerStatus();
        }
      } catch (err) {
        showToast(err.message, "error");
      } finally {
        browseBtnNet.disabled = false;
        browseBtnNet.textContent = origText;
      }
    });
  }

  document.getElementById("btn-net-save-sync").addEventListener("click", async () => {
    const sharedSyncDir = document.getElementById("net-sync-dir").value.trim();
    if (!sharedSyncDir) return;
    try {
      await apiFetch("/api/setup/sync-dir", {
        method: "POST",
        body: JSON.stringify({ sharedSyncDir }),
      });
      showToast("Updated local machine's Shared Sync Folder path!", "success");
      refreshServerStatus();
    } catch (err) {
      showToast(err.message, "error");
    }
  });

  document.getElementById("btn-save-sync-toggles").addEventListener("click", async () => {
    try {
      await apiFetch("/api/admin/settings", {
        method: "PUT",
        body: JSON.stringify({
          autoBackupOnStop: document.getElementById("net-auto-backup").checked,
          gitSyncEnabled: document.getElementById("net-git-sync").checked,
        }),
      });
      showToast("Saved synchronization preferences!", "success");
    } catch (err) {
      showToast(err.message, "error");
    }
  });

  document.getElementById("btn-force-unlock").addEventListener("click", async () => {
    try {
      await apiFetch("/api/admin/force-unlock", { method: "POST" });
      showToast("Host lock forcefully released!", "success");
      refreshServerStatus();
    } catch (err) {
      showToast(err.message, "error");
    }
  });

  // Boot app
  bootApp();
});
