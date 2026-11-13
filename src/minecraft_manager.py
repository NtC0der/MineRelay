import os
import re
import sys
import uuid
import json
import time
import socket
import shutil
import base64
import binascii
import hashlib
import secrets
import threading
import subprocess
import urllib.request
from urllib.parse import quote as urlquote
from pathlib import Path
from typing import Optional, List, Dict, Any, Callable, Tuple

try:
    import psutil  # type: ignore
except ImportError:
    psutil = None

from src.models import UserAccount
from src.sync_engine import SyncEngine, HostLockConflictError
from src.tunnel_manager import TunnelManager
from src.bootstrap_runtime import (
    find_portable_java,
    ensure_portable_java,
    ensure_server_jar,
    ensure_paperclip_mojang_cache,
    get_versioned_jar_path,
    get_java_major_version,
    required_java_major_for_mc,
    fetch_live_minecraft_versions,
    list_installed_jars,
)


class MinecraftManager:
    def __init__(
        self,
        sync_engine: SyncEngine,
        tunnel_manager: TunnelManager,
        event_callback: Optional[Callable[[str, Any], None]] = None,
    ):
        self.sync_engine = sync_engine
        self.tunnel_manager = tunnel_manager
        self.event_callback = event_callback

        self._mutex = threading.RLock()
        self._lifecycle_lock = threading.RLock()
        self._stdin_lock = threading.RLock()
        self._metrics_lock = threading.Lock()
        self._jar_dl_lock = threading.Lock()
        self._world_info_lock = threading.Lock()
        self._proc: Optional[subprocess.Popen] = None
        self._psutil_proc: Optional[Any] = None
        self._last_metrics_sample_at: float = 0.0
        self._save_complete_event = threading.Event()
        self._sim_running: bool = False
        self._sim_stop_event = threading.Event()
        self._reached_running: bool = False

        self.local_status: str = "idle"  # 'idle' | 'syncing_pull' | 'starting' | 'running' | 'stopping' | 'syncing_push'
        self.active_host_user: Optional[UserAccount] = None
        self.started_at: float = 0.0
        self.server_ready_at: float = 0.0
        self._last_player_active_at: float = 0.0
        self._last_tps_warn_at: float = 0.0
        self._last_telemetry_publish_at: float = 0.0
        self._telemetry_flush_timer: Optional[threading.Timer] = None
        self._telemetry_lock = threading.Lock()
        self._log_seq: int = 0
        self.online_players: List[str] = []
        self.console_logs: List[Dict[str, Any]] = []
        self.tps: float = 20.0
        self.memory_used_mb: int = 0
        self.cpu_percent: float = 0.0
        self.runtime_mode: str = "none"  # 'java' | 'embedded' | 'none'
        self.last_error: str = ""
        self._active_max_players: int = 20
        self._active_server_port: int = 25565

        self._world_info_cache: Optional[Dict[str, Any]] = None
        self._world_info_cache_ts: float = 0.0
        self._cached_tunnel_state: Optional[Dict[str, Any]] = None
        self._cached_tunnel_state_ts: float = 0.0
        self._cached_tel_cfg: Optional[Any] = None
        self._cached_tel_cfg_ts: float = 0.0

        self._auto_sync_wakeup = threading.Event()
        self._auto_sync_in_progress: bool = False
        self._last_auto_sync_at: float = 0.0
        self._last_auto_sync_status: str = "idle"
        self._last_auto_sync_error: str = ""

        self.jar_downloading: bool = False
        self.jar_download_progress: int = 0
        self.jar_download_status: str = ""
        self._java_cache: Dict[str, Any] = {}
        self._java_cache_key: str = ""
        self._java_cache_ts: float = 0.0

        self.sync_engine.telemetry_provider = self._build_telemetry_snapshot

        if os.environ.get("MINERELAY_TEST_MODE") != "1":
            self._recover_orphaned_session_on_boot()
            threading.Thread(target=self._idle_shutdown_watchdog_loop, daemon=True, name="IdleShutdownWatchdog").start()
            threading.Thread(target=self._auto_sync_loop, daemon=True, name="PeriodicDriveAutoSync").start()

    @staticmethod
    def _safe_int_prop(props: Dict[str, str], key: str, default: int, min_val: Optional[int] = None, max_val: Optional[int] = None) -> int:
        raw = props.get(key, "")
        try:
            val = int(str(raw).strip()) if str(raw).strip() else default
        except (ValueError, TypeError):
            val = default
        if min_val is not None:
            val = max(min_val, val)
        if max_val is not None:
            val = min(max_val, val)
        return val

    @staticmethod
    def _is_proc_alive(proc_ref: Optional[Any]) -> bool:
        if proc_ref is None:
            return False
        poll_fn = getattr(proc_ref, "poll", None)
        if callable(poll_fn):
            return poll_fn() is None
        return True

    def _write_stdin(self, proc_ref: Optional[subprocess.Popen], payload: str) -> bool:
        if proc_ref is None or getattr(proc_ref, "stdin", None) is None:
            return False
        if not self._is_proc_alive(proc_ref):
            return False
        with self._stdin_lock:
            try:
                proc_ref.stdin.write(payload)
                proc_ref.stdin.flush()
                return True
            except (OSError, ValueError):
                return False

    def _idle_shutdown_watchdog_loop(self) -> None:
        while True:
            time.sleep(15.0)
            try:
                if self.local_status != "running" or not self._reached_running:
                    continue
                cfg = self.sync_engine.load_cluster_config()
                if not cfg or cfg.idle_shutdown_minutes <= 0:
                    continue
                with self._mutex:
                    has_players = len(self.online_players) > 0
                if has_players:
                    self._last_player_active_at = time.time()
                    continue
                ref_time = max(self.server_ready_at or self.started_at, self._last_player_active_at)
                if ref_time > 0 and (time.time() - ref_time) >= (cfg.idle_shutdown_minutes * 60):
                    self._log(
                        f"Idle Auto-Shutdown: 0 players online for {cfg.idle_shutdown_minutes} minute(s). Automatically stopping and syncing world...",
                        "INFO",
                        "Server",
                    )
                    self.stop_hosting(self.active_host_user)
            except Exception:
                pass

    def get_auto_sync_status(self, cfg: Optional[ClusterConfig] = None) -> Dict[str, Any]:
        if cfg is None:
            cfg = self.sync_engine.load_cluster_config()
        raw_interval = getattr(cfg, "auto_sync_interval_minutes", 10) if cfg else 10
        try:
            interval_min = max(0, min(1440, int(raw_interval)))
        except (ValueError, TypeError):
            interval_min = 10
        enabled = interval_min > 0
        next_sync_at = 0.0
        if enabled and self.local_status == "running" and self._reached_running:
            base_ts = max(self.server_ready_at or self.started_at or 0.0, self._last_auto_sync_at)
            if base_ts > 0:
                next_sync_at = base_ts + (interval_min * 60.0)
        last_status = "disabled" if not enabled else (self._last_auto_sync_status or "idle")
        return {
            "enabled": enabled,
            "intervalMinutes": interval_min,
            "inProgress": bool(self._auto_sync_in_progress),
            "lastSyncAt": float(self._last_auto_sync_at or 0.0),
            "nextSyncAt": float(next_sync_at or 0.0),
            "lastSyncStatus": last_status,
            "lastSyncError": self._last_auto_sync_error if enabled else "",
        }

    def notify_auto_sync_schedule_changed(self) -> None:
        self._cached_tel_cfg_ts = 0.0
        self._auto_sync_wakeup.set()
        if self.local_status != "idle":
            self._publish_host_telemetry_throttled(force=True)
        self._emit("status_changed", self.get_status_summary())

    def _auto_sync_loop(self) -> None:
        while True:
            wait_sec = 30.0
            try:
                cfg = self.sync_engine.load_cluster_config()
                interval_min = max(0, min(1440, int(getattr(cfg, "auto_sync_interval_minutes", 10) or 0))) if cfg else 10
                if interval_min > 0 and self.local_status == "running" and self._reached_running:
                    base_ts = max(self.server_ready_at or self.started_at, self._last_auto_sync_at)
                    due_in = (base_ts + interval_min * 60.0) - time.time() if base_ts > 0 else interval_min * 60.0
                    if due_in <= 0:
                        self.run_live_auto_sync_once(force=False)
                        wait_sec = min(30.0, float(interval_min * 60.0))
                    else:
                        wait_sec = min(30.0, max(1.0, due_in))
            except Exception:
                wait_sec = 30.0
            self._auto_sync_wakeup.wait(timeout=wait_sec)
            self._auto_sync_wakeup.clear()

    def run_live_auto_sync_once(self, force: bool = False) -> Dict[str, Any]:
        """
        Performs a safe live world snapshot sync to Shared Drive while the server is running:
        save-off -> save-all flush -> stage/push snapshot (without releasing/modifying active lock) -> save-on in finally.
        Serialized with _lifecycle_lock so it never collides with manual stop, restart, world reset, or backup.
        """
        cfg = self.sync_engine.load_cluster_config()
        interval_min = max(0, min(1440, int(getattr(cfg, "auto_sync_interval_minutes", 10) or 0))) if cfg else 10
        if not force and interval_min <= 0:
            self._last_auto_sync_status = "disabled"
            self._last_auto_sync_error = ""
            return {"pushed": False, "skipped": True, "reason": "Periodic Drive auto-sync is disabled."}

        with self._lifecycle_lock:
            if self.local_status != "running" or not self._reached_running:
                return {"pushed": False, "skipped": True, "reason": f"Server is not running (status={self.local_status})."}

            epoch = self.sync_engine._active_lock_epoch
            owns = (
                self.sync_engine.owns_lock(epoch)
                if hasattr(self.sync_engine, "owns_lock")
                else (self.sync_engine._owns_active_lock and self.sync_engine.is_locked_by_current_machine())
            )
            if not owns:
                self._last_auto_sync_status = "skipped"
                self._last_auto_sync_error = "Active host lock is not owned by this machine."
                return {"pushed": False, "skipped": True, "reason": self._last_auto_sync_error}

            if not force and interval_min > 0:
                base_ts = max(self.server_ready_at or self.started_at, self._last_auto_sync_at)
                if base_ts > 0 and (time.time() - base_ts) < (interval_min * 60.0):
                    return {"pushed": False, "skipped": True, "reason": "Auto-sync interval has not elapsed yet."}

            self._auto_sync_in_progress = True
            self._publish_host_telemetry_throttled(force=True)
            self._emit("status_changed", self.get_status_summary())

            proc_ref = self._proc
            paused_autosave = False
            host_uname = self.active_host_user.username if self.active_host_user else "AutoSync"
            try:
                if self._is_proc_alive(proc_ref) and getattr(proc_ref, "stdin", None):
                    with self._stdin_lock:
                        self._save_complete_event.clear()
                        try:
                            proc_ref.stdin.write("save-off\n")
                            proc_ref.stdin.flush()
                            paused_autosave = True
                            proc_ref.stdin.write("save-all flush\n")
                            proc_ref.stdin.flush()
                        except (OSError, ValueError):
                            pass
                    if paused_autosave:
                        saved_ok = self._save_complete_event.wait(timeout=12.0)
                        if not saved_ok:
                            self._log("Auto-sync save-all flush timed out after 12s; proceeding with staged snapshot.", "WARN", "SyncEngine")

                if hasattr(self.sync_engine, "push_live_snapshot"):
                    res = self.sync_engine.push_live_snapshot(
                        lock_epoch=epoch,
                        create_backup=False,
                        host_username=host_uname,
                    )
                else:
                    res = self.sync_engine.push_world_from_runtime(
                        create_backup=False,
                        backup_label="auto-sync",
                        host_username=host_uname,
                    )

                if res.get("pushed"):
                    self._last_auto_sync_at = time.time()
                    self._last_auto_sync_status = "ok"
                    self._last_auto_sync_error = ""
                    self._get_cached_world_info(force_refresh=True)
                    kb = round(float(res.get("sizeBytes", 0) or 0) / 1024.0, 1)
                    self._log(
                        f"Periodic Drive auto-sync complete ({res.get('fileCount', 0)} files, {kb} KB).",
                        "INFO",
                        "SyncEngine",
                    )
                else:
                    self._last_auto_sync_status = "skipped"
                    self._last_auto_sync_error = str(res.get("reason", "") or "")
                return res
            except Exception as e:
                self._last_auto_sync_status = "error"
                self._last_auto_sync_error = str(e)
                self._log(f"Periodic Drive auto-sync failed: {e}", "WARN", "SyncEngine")
                return {"pushed": False, "error": str(e)}
            finally:
                if paused_autosave and self._is_proc_alive(proc_ref) and getattr(proc_ref, "stdin", None):
                    self._write_stdin(proc_ref, "save-on\n")
                self._auto_sync_in_progress = False
                self._publish_host_telemetry_throttled(force=True)
                self._emit("status_changed", self.get_status_summary())

    @property
    def runtime_dir(self) -> Path:
        return self.sync_engine.runtime_dir

    @property
    def bin_dir(self) -> Path:
        return self.sync_engine.base_data_dir / "bin"

    def get_active_jar_path(self) -> Path:
        cfg = self.sync_engine.load_cluster_config()
        sw = cfg.mc_software if cfg else "paper"
        ver = cfg.mc_version if cfg else "26.3"
        v_jar = get_versioned_jar_path(self.bin_dir, sw, ver)
        if v_jar.exists() and v_jar.stat().st_size > 1_000_000:
            return v_jar
        legacy = self.runtime_dir / "server.jar"
        return v_jar if not legacy.exists() else legacy

    def _emit(self, event_type: str, payload: Any) -> None:
        if self.event_callback:
            try:
                self.event_callback(event_type, payload)
            except Exception:
                pass

    def _get_cached_world_info(self, force_refresh: bool = False) -> Optional[Dict[str, Any]]:
        with self._world_info_lock:
            now = time.time()
            if not force_refresh and self._world_info_cache is not None and (now - self._world_info_cache_ts) < 60.0:
                return dict(self._world_info_cache)
        try:
            info = self.get_world_info()
        except Exception:
            with self._world_info_lock:
                return dict(self._world_info_cache) if self._world_info_cache is not None else None
        with self._world_info_lock:
            self._world_info_cache = dict(info)
            self._world_info_cache_ts = time.time()
            return dict(self._world_info_cache)

    def _sample_local_process_metrics(self) -> None:
        if self.local_status in ("idle", "syncing_pull"):
            return
        if psutil is None or self.local_status not in ("running", "starting", "restarting"):
            return
        with self._metrics_lock:
            now = time.time()
            try:
                proc = self._proc
                if proc is not None:
                    if (now - self._last_metrics_sample_at) < 1.2 and self._psutil_proc is not None and self._psutil_proc.pid == proc.pid:
                        return
                    if self._psutil_proc is None or self._psutil_proc.pid != proc.pid:
                        self._psutil_proc = psutil.Process(proc.pid)
                        # Prime baseline CPU counter once on the cached Process instance
                        self._psutil_proc.cpu_percent(interval=None)
                    rss_bytes = self._psutil_proc.memory_info().rss
                    cpu_val = self._psutil_proc.cpu_percent(interval=None)
                    for child in self._psutil_proc.children(recursive=True):
                        try:
                            rss_bytes += child.memory_info().rss
                            cpu_val += child.cpu_percent(interval=None)
                        except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
                            pass
                    self.memory_used_mb = int(rss_bytes / (1024 * 1024))
                    cpu_count = max(1, psutil.cpu_count() or 1)
                    self.cpu_percent = round(min(100.0, float(cpu_val) / cpu_count), 1)
                    self._last_metrics_sample_at = now
                elif self.runtime_mode == "embedded":
                    # Embedded test simulator updates self.memory_used_mb & self.cpu_percent directly
                    pass
                if self.local_status == "running" and self._last_tps_warn_at > 0 and (now - self._last_tps_warn_at) > 20.0:
                    # Recover observed TPS back toward 20.0 once tick-lag warnings subside
                    self.tps = round(min(20.0, self.tps + 0.5), 2)
            except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
                self._psutil_proc = None
            except Exception:
                pass

    def _build_telemetry_snapshot(self) -> Dict[str, Any]:
        self._sample_local_process_metrics()
        now = time.time()
        if self.local_status == "running" and self._cached_tel_cfg is not None and (now - self._cached_tel_cfg_ts) < 1.0:
            cfg = self._cached_tel_cfg
        else:
            cfg = self.sync_engine.load_cluster_config()
            self._cached_tel_cfg = cfg
            self._cached_tel_cfg_ts = now
        with self._mutex:
            players_copy = list(self.online_players)
            logs_copy = list(self.console_logs[-120:])
            max_p = self._active_max_players or 20
        tunnel_state: Dict[str, Any] = {}
        if self.local_status != "idle":
            cur_tm_status = getattr(self.tunnel_manager, "status", None)
            cur_tm_pub = getattr(self.tunnel_manager, "public_address", "") or (cfg.public_join_address if cfg else "")
            cur_tm_claim = getattr(self.tunnel_manager, "claim_url", "")
            if (
                self.local_status == "running"
                and self._cached_tunnel_state is not None
                and (now - self._cached_tunnel_state_ts) < 1.0
                and self._cached_tunnel_state.get("status") == cur_tm_status
                and self._cached_tunnel_state.get("publicAddress") == cur_tm_pub
                and self._cached_tunnel_state.get("claimUrl") == cur_tm_claim
            ):
                tunnel_state = dict(self._cached_tunnel_state)
            else:
                try:
                    tunnel_state = dict(self.tunnel_manager.get_state())
                    self._cached_tunnel_state = dict(tunnel_state)
                    self._cached_tunnel_state_ts = now
                except Exception:
                    tunnel_state = {}
        uptime_sec = (
            int(max(0.0, now - self.server_ready_at))
            if (self.local_status == "running" and self.server_ready_at > 0)
            else 0
        )
        host_lan_ip = str(tunnel_state.get("lanIp") or "")
        world_info = self._get_cached_world_info(force_refresh=False) if self.local_status != "idle" else None
        return {
            "isActive": self.local_status != "idle",
            "status": self.local_status,
            "lastError": self.last_error,
            "tps": self.tps if self.local_status == "running" else 0.0,
            "memoryUsedMb": self.memory_used_mb if self.local_status != "idle" else 0,
            "memoryMaxMb": cfg.memory_max_mb if cfg else 4096,
            "cpuPercent": self.cpu_percent if self.local_status != "idle" else 0.0,
            "uptimeSeconds": uptime_sec,
            "serverReadyAt": self.server_ready_at if self.local_status == "running" else 0.0,
            "hostClockNow": now,
            "onlinePlayers": players_copy,
            "maxPlayers": max_p,
            "mcVersion": f"{cfg.mc_software.capitalize()} {cfg.mc_version}" if cfg else "",
            "mcSoftware": cfg.mc_software if cfg else "paper",
            "rawMcVersion": cfg.mc_version if cfg else "26.3",
            "serverName": cfg.server_name if cfg else "Shared Minecraft Server",
            "publicAddress": str(tunnel_state.get("publicAddress") or ""),
            "tunnelStatus": str(tunnel_state.get("status") or "stopped"),
            "hostLanIp": host_lan_ip,
            "tunnelLanIp": host_lan_ip,
            "claimUrl": str(tunnel_state.get("claimUrl") or ""),
            "tunnelLogs": list(tunnel_state.get("logs") or [])[-40:],
            "worldInfo": world_info,
            "autoSync": self.get_auto_sync_status(cfg=cfg),
            "logs": logs_copy,
        }

    def _publish_host_telemetry_throttled(self, force: bool = False) -> None:
        if self.local_status in ("idle",):
            return
        if os.environ.get("MINERELAY_TEST_MODE") == "1" and not force:
            return
        if force:
            self._cached_tunnel_state = None
            self._cached_tunnel_state_ts = 0.0
            self._cached_tel_cfg = None
            self._cached_tel_cfg_ts = 0.0
        with self._telemetry_lock:
            now = time.time()
            elapsed = now - self._last_telemetry_publish_at
            if not force and elapsed < 1.5:
                if self._telemetry_flush_timer is None:
                    delay = max(0.2, 1.6 - elapsed)
                    t = threading.Timer(delay, self._flush_pending_telemetry)
                    t.daemon = True
                    self._telemetry_flush_timer = t
                    t.start()
                return
            if self._telemetry_flush_timer is not None:
                try:
                    self._telemetry_flush_timer.cancel()
                except Exception:
                    pass
                self._telemetry_flush_timer = None
            self._last_telemetry_publish_at = now
        try:
            if not self.sync_engine._owns_active_lock:
                return
            epoch = self.sync_engine._active_lock_epoch
            lock_username = self.active_host_user.username if self.active_host_user else ""
            max_p = self._active_max_players or 20
            if not epoch:
                lock = self.sync_engine.get_lock()
                if not lock.is_locked or lock.machine_id != self.sync_engine.local_config.machine_id:
                    return
                epoch = lock.lock_epoch
                lock_username = lock.host_username or lock_username
                max_p = lock.max_players or max_p
            snap = self._build_telemetry_snapshot()
            snap["isActive"] = True
            snap["lockEpoch"] = epoch
            snap["machineId"] = self.sync_engine.local_config.machine_id
            snap["machineName"] = self.sync_engine.local_config.machine_name
            snap["hostUsername"] = lock_username
            snap["status"] = self.local_status
            snap["maxPlayers"] = max_p
            self.sync_engine.write_host_telemetry(snap)
        except Exception:
            pass

    def _cancel_telemetry_timer(self) -> None:
        with self._telemetry_lock:
            if self._telemetry_flush_timer is not None:
                try:
                    self._telemetry_flush_timer.cancel()
                except Exception:
                    pass
                self._telemetry_flush_timer = None

    def _flush_pending_telemetry(self) -> None:
        with self._telemetry_lock:
            self._telemetry_flush_timer = None
        self._publish_host_telemetry_throttled(force=True)

    def _log(self, message: str, level: str = "INFO", source: str = "Server") -> None:
        ts = time.strftime("%H:%M:%S")
        formatted = f"[{ts}] [{source}/{level}]: {message}"
        with self._mutex:
            self._log_seq += 1
            entry = {
                "id": f"{int(time.time() * 1000)}-{self._log_seq}",
                "timestamp": ts,
                "level": level,
                "source": source,
                "text": formatted,
            }
            self.console_logs.append(entry)
            if len(self.console_logs) > 500:
                self.console_logs = self.console_logs[-500:]
        self._emit("console_log", entry)
        self._publish_host_telemetry_throttled(force=False)

    def check_java_installed(
        self,
        java_cmd: str = "java",
        mc_version: str = "26.3",
        force_refresh: bool = False,
    ) -> Dict[str, Any]:
        req_major = required_java_major_for_mc(mc_version)
        override = self.sync_engine.local_config.java_override_path.strip()
        portable_java = find_portable_java(self.bin_dir, min_major=req_major)
        custom_cmd = java_cmd if (java_cmd and java_cmd != "java") else ""
        cmd = override or custom_cmd or portable_java or "java"
        cache_key = f"{cmd}|{req_major}"
        now = time.time()
        if not force_refresh and self._java_cache and self._java_cache_key == cache_key and (now - self._java_cache_ts) < 30.0:
            return dict(self._java_cache)
        try:
            creationflags = subprocess.CREATE_NO_WINDOW if sys.platform.startswith("win") else 0
            res = subprocess.run(
                [cmd, "-version"],
                capture_output=True,
                text=True,
                timeout=5,
                creationflags=creationflags,
            )
            output = (res.stderr or res.stdout or "").strip()
            first_line = output.splitlines()[0] if output else "Java installed"
            m = re.search(r'version "(\d+)', output)
            major = int(m.group(1)) if m else 0
            installed = res.returncode == 0 and major >= req_major
            result = {
                "installed": installed,
                "version": first_line,
                "major": major,
                "requiredMajor": req_major,
                "command": cmd,
            }
        except Exception:
            result = {
                "installed": False,
                "version": f"Java {req_major}+ not installed yet",
                "major": 0,
                "requiredMajor": req_major,
                "command": cmd,
            }
        self._java_cache = dict(result)
        self._java_cache_key = cache_key
        self._java_cache_ts = now
        return result

    # -------------------------------------------------------------------------
    # -------------------------------------------------------------------------
    # Process Tree & Port Hygiene (Prevents Orphaned JVM / Port 25565 Collisions)
    # -------------------------------------------------------------------------

    def _is_port_in_use(self, port: int) -> bool:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(0.3)
                return s.connect_ex(("127.0.0.1", port)) == 0
        except Exception:
            return False

    def _wait_for_port_free(self, port: int, timeout: float = 6.0) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if not self._is_port_in_use(port):
                return True
            time.sleep(0.2)
        return not self._is_port_in_use(port)

    def _terminate_process_tree(self, proc: subprocess.Popen, wait_timeout: float = 4.0) -> None:
        if psutil is not None:
            try:
                parent = psutil.Process(proc.pid)
                children = parent.children(recursive=True)
                for c in children:
                    try:
                        c.terminate()
                    except Exception:
                        pass
                parent.terminate()
                _, alive = psutil.wait_procs([parent, *children], timeout=wait_timeout)
                for p in alive:
                    try:
                        p.kill()
                    except Exception:
                        pass
                return
            except Exception:
                pass
        try:
            proc.terminate()
            proc.wait(timeout=wait_timeout)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass

    def _find_runtime_java_processes(self, server_port: int = 25565) -> List[Any]:
        if psutil is None or os.environ.get("MINERELAY_TEST_MODE") == "1":
            return []
        found_pids = set()
        found_procs = []
        runtime_norm = str(self.runtime_dir.resolve()).lower()
        bin_norm = str(self.bin_dir.resolve()).lower()

        # Check listening sockets on server_port first
        port_pids = set()
        try:
            for conn in psutil.net_connections(kind="inet"):
                if (
                    conn.status == "LISTEN"
                    and conn.laddr
                    and conn.laddr.port == server_port
                    and conn.pid
                ):
                    port_pids.add(conn.pid)
        except Exception:
            pass

        for p in psutil.process_iter(["pid", "name", "cmdline", "cwd", "exe"]):
            try:
                pid = p.info.get("pid")
                pname = (p.info.get("name") or "").lower()
                # Only match console JVM (java.exe / java), NEVER javaw.exe (which is the GUI game client!)
                if pname not in ("java.exe", "java"):
                    continue
                cmdline = " ".join(p.info.get("cmdline") or []).lower()
                cwd = (p.info.get("cwd") or "").lower()
                exe = (p.info.get("exe") or "").lower()
                if any(client_kw in cmdline or client_kw in exe for client_kw in ("modrinth", "curseforge", "prismlauncher", ".minecraft")):
                    continue
                is_ours = (
                    runtime_norm in cmdline
                    or runtime_norm in cwd
                    or bin_norm in cmdline
                    or bin_norm in exe
                    or "server-paper-" in cmdline
                    or "server-vanilla-" in cmdline
                    or pid in port_pids
                )
                if is_ours and pid not in found_pids:
                    found_pids.add(pid)
                    found_procs.append(p)
            except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
                pass
        return found_procs

    def _stop_any_runtime_java_processes(self, server_port: int = 25565) -> int:
        if os.environ.get("MINERELAY_TEST_MODE") == "1":
            return 0
        procs = self._find_runtime_java_processes(server_port=server_port)
        if not procs:
            return 0
        all_targets = []
        for p in procs:
            try:
                all_targets.extend(p.children(recursive=True))
            except Exception:
                pass
            all_targets.append(p)
        for t in all_targets:
            try:
                t.terminate()
            except Exception:
                pass
        if psutil is not None:
            try:
                _, alive = psutil.wait_procs(all_targets, timeout=4.0)
                for a in alive:
                    try:
                        a.kill()
                    except Exception:
                        pass
            except Exception:
                pass
        self._wait_for_port_free(server_port, timeout=5.0)
        for lock_file in self.runtime_dir.rglob("session.lock"):
            try:
                lock_file.unlink()
            except OSError:
                pass
        return len(procs)

    def _push_world_compat(
        self,
        create_backup: bool = True,
        backup_label: str = "auto",
        host_username: str = "",
        require_lock_epoch: str = "",
        stage_copy_before_zip: bool = False,
    ) -> Dict[str, Any]:
        """Calls SyncEngine.push_world_from_runtime with enhanced flags if supported."""
        import inspect
        fn = self.sync_engine.push_world_from_runtime
        kwargs: Dict[str, Any] = {
            "create_backup": create_backup,
            "backup_label": backup_label,
            "host_username": host_username,
        }
        try:
            params = inspect.signature(fn).parameters
            if "require_lock_epoch" in params and require_lock_epoch:
                kwargs["require_lock_epoch"] = require_lock_epoch
            if "strict_files" in params:
                kwargs["strict_files"] = True
            if "stage_copy_before_zip" in params and stage_copy_before_zip:
                kwargs["stage_copy_before_zip"] = True
        except Exception:
            pass
        return fn(**kwargs)

    def _recover_orphaned_session_on_boot(self) -> None:
        """
        If MineRelay was closed while a local Java server was still running or while this machine
        held host-lock.json, stop any orphaned JVM, push unsaved world progress from an interrupted session,
        and cleanly release the lock with expected snapshot metadata.
        """
        try:
            cfg = self.sync_engine.load_cluster_config()
            port = cfg.server_port if cfg else 25565
            orphans = self._stop_any_runtime_java_processes(server_port=port)
            self.tunnel_manager.stop_tunnel()
            lock = self.sync_engine.get_lock()
            was_running_session = (
                lock.is_locked
                and lock.machine_id == self.sync_engine.local_config.machine_id
                and lock.status in ("running", "stopping", "syncing_push", "restarting", "sync_failed")
            )
            if orphans > 0 or was_running_session:
                self._log(
                    (
                        f"Recovered {orphans} orphaned Minecraft server process(es); syncing world and unlocking."
                        if orphans > 0
                        else "Detected interrupted hosting session on this machine; pushing latest local world before unlocking."
                    ),
                    "WARN",
                    "Launcher",
                )
                push_res: Dict[str, Any] = {}
                try:
                    push_res = self._push_world_compat(
                        create_backup=True,
                        backup_label="recovery",
                        host_username=lock.host_username or "recovery",
                    )
                except Exception as e:
                    self.last_error = f"Recovery push failed: {e}"
                    self._log(self.last_error, "ERROR", "SyncEngine")
                    if lock.is_locked and lock.machine_id == self.sync_engine.local_config.machine_id:
                        try:
                            self.sync_engine.update_lock_state(status="sync_failed")
                        except Exception:
                            pass
                    return
                self.sync_engine.release_lock(
                    expected_snapshot_seq=int(push_res.get("snapshotSeq", 0) or 0),
                    expected_snapshot_hash=str(push_res.get("sha256", "") or ""),
                )
            elif lock.is_locked and lock.machine_id == self.sync_engine.local_config.machine_id:
                self._log(
                    "Cleared leftover startup host lock from previous session on this machine.",
                    "INFO",
                    "SyncEngine",
                )
                self.sync_engine.release_lock()
        except Exception:
            pass

    # -------------------------------------------------------------------------
    # Start & Stop Distributed Hosting
    # -------------------------------------------------------------------------

    def start_hosting(self, user: UserAccount) -> Dict[str, Any]:
        """
        1. Verifies no other member is currently hosting & acquires the distributed lock.
        2. Cleans up any orphaned local JVM processes and pulls latest world snapshot into ./data/runtime-server.
        3. Starts zero-port-forwarding tunnel (playit.gg).
        4. Ensures matching Java Runtime (Java 25 for MC 26.3+) and versioned server.jar exist, then launches JVM.
        """
        with self._lifecycle_lock:
            if self.local_status not in ("idle",):
                raise RuntimeError(f"Local server is already in state '{self.local_status}'.")

            cfg = self.sync_engine.load_cluster_config()
            if not cfg:
                raise RuntimeError("Cluster configuration is not initialized.")

            self.last_error = ""
            self._reached_running = False
            self.server_ready_at = 0.0
            self._last_tps_warn_at = 0.0
            self._last_auto_sync_at = 0.0
            self._last_auto_sync_status = "idle"
            self._last_auto_sync_error = ""
            self._world_info_cache = None

            # Step 1: Acquire distributed lock (raises HostLockConflictError if another member is hosting)
            self.sync_engine.acquire_lock(
                user=user,
                mc_version=f"{cfg.mc_software.capitalize()} {cfg.mc_version}",
                public_address=cfg.public_join_address,
                initial_status="syncing_pull",
            )
            self.local_status = "syncing_pull"
            self.active_host_user = user
            self._emit("status_changed", self.get_status_summary())
            self._publish_host_telemetry_throttled(force=True)

            try:
                # Ensure no orphaned JVM is holding world/session.lock or the server port before pulling world
                cleaned = self._stop_any_runtime_java_processes(server_port=cfg.server_port or 25565)
                if cleaned > 0:
                    self._log(
                        f"Stopped {cleaned} lingering Java process(es) before starting new session.",
                        "INFO",
                        "Launcher",
                    )

                # Step 2: Pull latest world snapshot from Shared Sync Folder
                self._log(
                    f"Host lock claimed by {user.username} ({user.role.upper()}). Pulling latest world snapshot from shared storage...",
                    "INFO",
                    "SyncEngine",
                )
                pull_res = self.sync_engine.pull_world_to_runtime(strict=True)
                if pull_res.get("synced"):
                    kb = round(pull_res.get("sizeBytes", 0) / 1024, 1)
                    self._log(f"Synced world snapshot ({kb} KB) into local runtime directory.", "INFO", "SyncEngine")
                else:
                    self._log(str(pull_res.get("reason", "Initialized runtime directory.")), "INFO", "SyncEngine")

                # Step 3: Check port availability & Start Zero-Port-Forwarding Tunnel
                props = self.get_server_properties()
                port = self._safe_int_prop(props, "server-port", cfg.server_port or 25565, 1, 65535)
                max_players = self._safe_int_prop(props, "max-players", 20, 1, 10000)
                self._active_max_players = max_players
                self._active_server_port = port
                self._get_cached_world_info(force_refresh=True)

                # Double-check port availability on this machine
                if os.environ.get("MINERELAY_TEST_MODE") != "1" and self._is_port_in_use(port):
                    self._stop_any_runtime_java_processes(server_port=port)
                    if self._is_port_in_use(port):
                        raise RuntimeError(
                            f"Port {port} is still in use by another application on this PC. Close any other Minecraft server or change Server Port in Server Config."
                        )

                tunnel_state = self.tunnel_manager.start_tunnel(server_port=port)

                # Step 4: Ensure Java 25+ & matching versioned server.jar, then start Server
                self.local_status = "starting"
                self.started_at = time.time()
                with self._mutex:
                    self.online_players = []
                self.sync_engine.update_lock_state(
                    status="starting",
                    public_address=tunnel_state.get("publicAddress", ""),
                    tunnel_status=tunnel_state.get("status", "online"),
                    max_players=max_players,
                    online_players=[],
                )
                self._emit("status_changed", self.get_status_summary())
                self._emit("players_updated", self.get_players_data())

                req_java = required_java_major_for_mc(cfg.mc_version)
                java_info = self.check_java_installed(cfg.java_path, cfg.mc_version)
                target_jar = get_versioned_jar_path(self.bin_dir, cfg.mc_software, cfg.mc_version)
                has_jar = target_jar.exists() and target_jar.stat().st_size > 1_000_000

                if (not java_info["installed"] or not has_jar) and os.environ.get("MINERELAY_TEST_MODE") != "1":
                    try:
                        if not java_info["installed"]:
                            self._log(
                                f"Java {req_java}+ required for Minecraft {cfg.mc_version}; auto-downloading Portable Java {req_java} Runtime...",
                                "INFO",
                                "Installer",
                            )
                            ensure_portable_java(
                                self.bin_dir,
                                min_major=req_java,
                                log_fn=lambda msg: self._log(msg, "INFO", "Installer"),
                            )
                            java_info = self.check_java_installed(cfg.java_path, cfg.mc_version, force_refresh=True)
                        if not has_jar:
                            self._log(
                                f"Downloading {cfg.mc_software.upper()} {cfg.mc_version} server.jar for this device...",
                                "INFO",
                                "Installer",
                            )
                            target_jar = ensure_server_jar(
                                self.runtime_dir,
                                cfg.mc_software,
                                cfg.mc_version,
                                log_fn=lambda msg: self._log(msg, "INFO", "Installer"),
                                bin_dir=self.bin_dir,
                            )
                            has_jar = target_jar.exists() and target_jar.stat().st_size > 1_000_000
                    except Exception as boot_err:
                        self._log(f"Auto-download notice: {boot_err}", "WARN", "Installer")

                if not (java_info["installed"] and has_jar) and os.environ.get("MINERELAY_TEST_MODE") != "1":
                    raise RuntimeError(
                        f"Java {req_java}+ and a valid {cfg.mc_software.upper()} {cfg.mc_version} server.jar are required to host (auto-download did not complete)."
                    )

                # Update user host_count in ClusterConfig only after preflight checks pass
                fresh_cfg = self.sync_engine.load_cluster_config() or cfg
                for acc in fresh_cfg.accounts:
                    if acc.id == user.id:
                        acc.host_count += 1
                        acc.last_active_at = time.time()
                        break
                self.sync_engine.save_cluster_config(fresh_cfg)

                if java_info["installed"] and has_jar:
                    ensure_paperclip_mojang_cache(
                        target_jar,
                        self.runtime_dir,
                        log_fn=lambda msg: self._log(msg, "INFO", "Installer"),
                    )
                    self._spawn_java_minecraft(fresh_cfg, java_info["command"], target_jar, max_players, port)
                else:
                    self._spawn_embedded_minecraft_runtime(fresh_cfg, has_jar, java_info["installed"], max_players)

                return self.get_status_summary()

            except Exception as e:
                self._log(f"Start hosting failed: {e}", "ERROR", "Launcher")
                self._publish_host_telemetry_throttled(force=True)
                self._cleanup_after_failed_start(user, reason=str(e))
                raise e

    def _spawn_java_minecraft(
        self,
        cfg: Any,
        java_cmd: str,
        jar_path: Path,
        max_players: int,
        server_port: int = 25565,
    ) -> None:
        self.runtime_mode = "java"
        self.sync_engine._ensure_default_runtime_files()
        flags = [f for f in cfg.jvm_flags.split(" ") if f.strip()]
        for jvm_flag in ("-Dterminal.jline=false", "-Dterminal.ansi=false"):
            if jvm_flag not in flags:
                flags.append(jvm_flag)
        cmd = [
            java_cmd,
            f"-Xms{cfg.memory_min_mb}M",
            f"-Xmx{cfg.memory_max_mb}M",
            *flags,
            "-jar",
            str(jar_path.resolve()),
            "nogui",
        ]
        self._log(
            f"Launching Java Minecraft {cfg.mc_version} ({cfg.mc_software.upper()}): {jar_path.name} (RAM: {cfg.memory_min_mb}M-{cfg.memory_max_mb}M)",
            "INFO",
            "Launcher",
        )
        creationflags = subprocess.CREATE_NO_WINDOW if sys.platform.startswith("win") else 0
        self._proc = subprocess.Popen(
            cmd,
            cwd=str(self.runtime_dir),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            creationflags=creationflags,
        )
        threading.Thread(
            target=self._monitor_java_process,
            args=(self._proc, max_players, server_port),
            daemon=True,
            name="MinecraftProcessMonitor",
        ).start()

    def _monitor_java_process(self, proc: subprocess.Popen, max_players: int, server_port: int = 25565) -> None:
        ansi_regex = re.compile(r"\x1B(?:\[[0-?]*[ -/]*[@-~]|\].*?(?:\x07|\x1b\\)|[@-Z\\-_])")
        done_regex = re.compile(r'Done \([0-9.]+s\)! For help, type "help"')
        prefix = r"^(?:\[[^\]\r\n]+\](?:\s*\[[^\]\r\n]+\])*:\s*)?"
        join_regex = re.compile(prefix + r"(?!<)([a-zA-Z0-9_.*+~-]{2,32})\s+joined the game")
        login_regex = re.compile(prefix + r"(?!<)([a-zA-Z0-9_.*+~-]{2,32})\[.*?\] logged in with entity id")
        leave_regex = re.compile(prefix + r"(?!<)([a-zA-Z0-9_.*+~-]{2,32})\s+(?:left the game|lost connection:)")
        disconnect_regex = re.compile(prefix + r"(?!<)(?:Disconnecting|Kicking)\s+([a-zA-Z0-9_.*+~-]{2,32}):")
        list_regex = re.compile(prefix + r"(?!<)There are \d+ of a max of \d+ players online:(.*)")
        overload_regex = re.compile(r"Running\s+(\d+)ms\s+or\s+\d+\s+ticks behind", re.IGNORECASE)
        tps_cmd_regex = re.compile(r"TPS from last [^:]+:\s*\*?([0-9.]+)", re.IGNORECASE)
        fatal_error = ""

        if proc.stdout:
            try:
                for raw_line in proc.stdout:
                    line = ansi_regex.sub("", raw_line).rstrip("\r\n")
                    if not line:
                        continue
                    lvl = "ERROR" if ("/ERROR]" in line or "FAILED TO BIND" in line or "Exception" in line) else "INFO"
                    self._log(line, lvl, "Minecraft")

                    if "Saved the game" in line or "All dimensions are saved" in line:
                        self._save_complete_event.set()

                    om = overload_regex.search(line)
                    if om:
                        self._last_tps_warn_at = time.time()
                        lag_ms = int(om.group(1))
                        # Estimate effective TPS during server tick overload
                        self.tps = round(max(2.0, min(19.5, 20.0 - (lag_ms / 1000.0) * 1.5)), 2)

                    tm = tps_cmd_regex.search(line)
                    if tm:
                        try:
                            self.tps = round(min(20.0, max(0.0, float(tm.group(1)))), 2)
                        except ValueError:
                            pass

                    if done_regex.search(line):
                        self._reached_running = True
                        self.last_error = ""
                        self.local_status = "running"
                        self.server_ready_at = time.time()
                        self._last_player_active_at = time.time()
                        self.tps = 20.0
                        self._sample_local_process_metrics()
                        self._get_cached_world_info(force_refresh=True)
                        with self._mutex:
                            players_copy = list(self.online_players)
                        self.sync_engine.update_lock_state(status="running", max_players=max_players, online_players=players_copy)
                        self._publish_host_telemetry_throttled(force=True)
                        self._emit("status_changed", self.get_status_summary())
                        self._emit("players_updated", self.get_players_data())
                        self._apply_saved_gamerules_to_live_server(proc)
                        self._auto_sync_wakeup.set()

                    if not self._reached_running:
                        if "FAILED TO BIND TO PORT" in line or "Address already in use" in line:
                            fatal_error = f"Port {server_port} was already in use by another process."
                        elif "DirectoryLock" in line or "locked a portion of the file" in line:
                            fatal_error = "World folder was locked by a lingering Java process."
                        elif "Newer version of world" in line or "Downgrading is not supported" in line:
                            fatal_error = (
                                "World was saved in a newer Minecraft version (e.g. 26.3) and cannot be downgraded to an older version."
                            )
                        elif "Failed to start the minecraft server" in line and not fatal_error:
                            fatal_error = "Minecraft server failed to start (check console logs)."

                        if fatal_error and "Awaiting termination of I/O pool" in line:
                            # Paper's Moonrise worker pool hangs up to 60s after a startup crash; terminate cleanly
                            threading.Timer(1.0, lambda: self._terminate_process_tree(proc, wait_timeout=1.5)).start()

                    player_changed = False
                    players_copy = []

                    jm = join_regex.search(line) or login_regex.search(line)
                    if jm:
                        player = jm.group(1).strip()
                        with self._mutex:
                            if player and not any(p.lower() == player.lower() for p in self.online_players):
                                self.online_players.append(player)
                                player_changed = True
                                players_copy = list(self.online_players)

                    lm = leave_regex.search(line) or disconnect_regex.search(line)
                    if lm:
                        player = lm.group(1).strip()
                        with self._mutex:
                            to_remove = [p for p in self.online_players if p.lower() == player.lower()]
                            if to_remove:
                                for p in to_remove:
                                    self.online_players.remove(p)
                                player_changed = True
                                players_copy = list(self.online_players)

                    list_m = list_regex.search(line)
                    if list_m:
                        raw_names = list_m.group(1).strip()
                        parsed = [p.strip() for p in raw_names.split(",") if p.strip() and p.strip().lower() != "none"]
                        with self._mutex:
                            if set(parsed) != set(self.online_players):
                                self.online_players = parsed
                                player_changed = True
                                players_copy = list(self.online_players)

                    if player_changed:
                        self._last_player_active_at = time.time()
                        self.sync_engine.update_lock_state(online_players=players_copy)
                        self._emit("status_changed", self.get_status_summary())
                        self._emit("players_updated", self.get_players_data())

            except Exception as e:
                self._log(f"Error in Minecraft stdout monitor loop: {e}", "WARN", "Launcher")

        # Close pipes cleanly and transition if process exited unexpectedly
        self._psutil_proc = None
        for pipe in (getattr(proc, "stdin", None), getattr(proc, "stdout", None)):
            if pipe is not None:
                try:
                    pipe.close()
                except Exception:
                    pass
        with self._lifecycle_lock:
            if self._proc is proc:
                self._proc = None
                if self.local_status not in ("stopping", "syncing_push", "restarting", "idle"):
                    if self._reached_running:
                        exit_code = proc.poll()
                        if exit_code not in (0, None):
                            self.last_error = f"Minecraft server crashed or exited unexpectedly (exit code {exit_code})."
                            self._log(
                                f"Java Minecraft process crashed (exit code {exit_code}). Syncing world and releasing lock...",
                                "ERROR",
                                "Launcher",
                            )
                        else:
                            self._log("Java Minecraft process exited. Syncing world and releasing lock...", "WARN", "Launcher")
                        self._publish_host_telemetry_throttled(force=True)
                        self._finalize_stop_and_sync(self.active_host_user)
                    else:
                        err_msg = fatal_error or "Server process exited during startup before reaching Ready state."
                        self._log(
                            f"Startup failed: {err_msg} (World snapshot was preserved and NOT overwritten.)",
                            "ERROR",
                            "Launcher",
                        )
                        self._publish_host_telemetry_throttled(force=True)
                        self._cleanup_after_failed_start(self.active_host_user, reason=err_msg)

    def _spawn_embedded_minecraft_runtime(
        self,
        cfg: Any,
        has_jar: bool,
        has_java: bool,
        max_players: int,
    ) -> None:
        self.runtime_mode = "embedded"
        self._reached_running = True
        self._active_max_players = max_players
        self._sim_stop_event.clear()
        self._sim_running = True

        props = self.get_server_properties()
        level_name = self._sanitize_level_name(props.get("level-name", "world"))
        active_seed = (props.get("level-seed", "") or "849201948102948").strip()

        world_dir = self.runtime_dir / level_name
        world_dir.mkdir(parents=True, exist_ok=True)
        level_meta_path = world_dir / "level-metadata.json"
        if not level_meta_path.exists():
            level_meta_path.write_text(
                json.dumps(
                    {
                        "worldName": level_name,
                        "createdAt": time.time(),
                        "seed": active_seed,
                        "levelType": props.get("level-type", "minecraft:normal") or "minecraft:normal",
                        "ticks": 0,
                        "lastHostedBy": self.active_host_user.username if self.active_host_user else "Admin",
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
        else:
            try:
                meta = json.loads(level_meta_path.read_text(encoding="utf-8"))
                meta["lastHostedBy"] = self.active_host_user.username if self.active_host_user else "Unknown"
                meta["lastStartedAt"] = time.time()
                level_meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
            except Exception:
                pass

        port = props.get("server-port", str(cfg.server_port))
        gamemode = props.get("gamemode", "survival")

        self._log(f"Starting minecraft server version {cfg.mc_version} ({cfg.mc_software.upper()})", "INFO", "ServerMain")
        self._log(f"Loading properties (Default game type: {gamemode.upper()}, Port: {port})", "INFO", "ServerMain")
        self._log(f'Preparing level "{level_name}" (Seed: {active_seed})', "INFO", "ServerMain")
        self._log('Done (1.24s)! For help, type "help"', "INFO", "ServerMain")

        self.local_status = "running"
        now = time.time()
        self.server_ready_at = now
        self._last_player_active_at = now
        self.tps = 20.0
        self.memory_used_mb = min(1024, cfg.memory_min_mb)
        self.cpu_percent = 8.4
        self._get_cached_world_info(force_refresh=True)
        self.sync_engine.update_lock_state(status="running", max_players=max_players)
        self._publish_host_telemetry_throttled(force=True)
        self._emit("status_changed", self.get_status_summary())
        self._auto_sync_wakeup.set()

        def _telemetry_loop() -> None:
            tick = 0
            while not self._sim_stop_event.wait(5.0):
                if not self._sim_running:
                    break
                tick += 100
                self.tps = 20.0
                self.cpu_percent = round(6.5 + (tick % 300) * 0.01, 1)
                self.memory_used_mb = min(cfg.memory_max_mb, int(cfg.memory_min_mb * 0.65) + (tick % 120))
                self._publish_host_telemetry_throttled(force=False)

        threading.Thread(target=_telemetry_loop, daemon=True, name="EmbeddedRuntimeTelemetry").start()

    def _stop_java_process_gracefully(self, proc_ref: subprocess.Popen, wait_timeout: float = 35.0) -> None:
        """Flushes chunks to disk with `save-all flush`, sends `stop`, and waits cleanly before terminating."""
        self._psutil_proc = None
        try:
            if self._is_proc_alive(proc_ref) and getattr(proc_ref, "stdin", None):
                self._save_complete_event.clear()
                if self._write_stdin(proc_ref, "save-all flush\n"):
                    self._save_complete_event.wait(timeout=12.0)
                self._write_stdin(proc_ref, "stop\n")
            proc_ref.wait(timeout=wait_timeout)
        except Exception:
            self._terminate_process_tree(proc_ref, wait_timeout=5.0)
        finally:
            # Close only stdin here; _monitor_java_process closes stdout cleanly after EOF
            if getattr(proc_ref, "stdin", None) is not None:
                try:
                    proc_ref.stdin.close()
                except Exception:
                    pass

    def flush_and_backup_live_world(self, label: str, host_username: str) -> Dict[str, Any]:
        """Safely flushes and pauses chunk writes before packaging a backup while running."""
        with self._lifecycle_lock:
            if self.local_status == "idle":
                lock = self.sync_engine.get_lock()
                if lock.is_actively_locked() and not self.sync_engine.is_locked_by_current_machine(lock):
                    raise HostLockConflictError(
                        f"Cannot create snapshot while '{lock.host_username}' ({lock.machine_name}) holds the host lock."
                    )
            proc_ref = self._proc
            paused_autosave = False
            is_live_running = self.local_status == "running"
            if is_live_running and self._is_proc_alive(proc_ref) and getattr(proc_ref, "stdin", None):
                with self._stdin_lock:
                    self._save_complete_event.clear()
                    try:
                        proc_ref.stdin.write("save-off\n")
                        proc_ref.stdin.flush()
                        paused_autosave = True
                        proc_ref.stdin.write("save-all flush\n")
                        proc_ref.stdin.flush()
                    except (OSError, ValueError):
                        pass
                if paused_autosave:
                    saved_ok = self._save_complete_event.wait(timeout=12.0)
                    if not saved_ok:
                        self._log("Live backup save-all flush timed out after 12s; proceeding with staged snapshot.", "WARN", "SyncEngine")
            try:
                return self._push_world_compat(
                    create_backup=True,
                    backup_label=label,
                    host_username=host_username,
                    require_lock_epoch=self.sync_engine._active_lock_epoch if is_live_running else "",
                    stage_copy_before_zip=is_live_running,
                )
            finally:
                if paused_autosave and self._is_proc_alive(proc_ref) and getattr(proc_ref, "stdin", None):
                    self._write_stdin(proc_ref, "save-on\n")

    def stop_hosting(self, user: Optional[UserAccount] = None) -> Dict[str, Any]:
        """
        Gracefully stops the local Minecraft server, syncs the updated world back to the Shared Sync Folder,
        stops the tunnel, and releases the distributed host lock so another member can host next.
        """
        with self._lifecycle_lock:
            effective_user = user or self.active_host_user
            lock = self.sync_engine.get_lock()
            cfg = self.sync_engine.load_cluster_config()
            port = self._active_server_port or (cfg.server_port if cfg else 25565)

            if self.local_status == "idle":
                if lock.is_locked and lock.machine_id != self.sync_engine.local_config.machine_id and not lock.is_stale():
                    raise HostLockConflictError(
                        f"Cannot stop active remote server hosted by {lock.host_username} on {lock.machine_name} from another PC "
                        "(have them click Stop Server so world saves upload cleanly, or use Admin Force Unlock if their PC crashed)."
                    )
                orphans_stopped = self._stop_any_runtime_java_processes(server_port=port)
                push_res: Dict[str, Any] = {}
                if orphans_stopped > 0:
                    self._log(
                        f"Stopped {orphans_stopped} background Java server process(es) and syncing world...",
                        "INFO",
                        "Launcher",
                    )
                    try:
                        push_res = self._push_world_compat(
                            create_backup=cfg.auto_backup_on_stop if cfg else True,
                            backup_label=effective_user.username if effective_user else "host",
                            host_username=effective_user.username if effective_user else "recovery",
                        )
                    except Exception as e:
                        self._log(f"Warning pushing orphaned world: {e}", "WARN", "SyncEngine")
                if lock.is_locked:
                    self.sync_engine.release_lock(
                        effective_user,
                        expected_snapshot_seq=int(push_res.get("snapshotSeq", 0) or 0) if orphans_stopped > 0 else 0,
                        expected_snapshot_hash=str(push_res.get("sha256", "") or "") if orphans_stopped > 0 else "",
                    )
                    self.tunnel_manager.stop_tunnel()
                    uname = effective_user.username if effective_user else "System"
                    self._log(f"Host lock released by {uname}.", "INFO", "SyncEngine")
                    self._emit("status_changed", self.get_status_summary())
                    return self.get_status_summary()
                self.tunnel_manager.stop_tunnel()
                self._emit("status_changed", self.get_status_summary())
                return self.get_status_summary()

            was_running = (self.local_status == "running") or self._reached_running or self._sim_running
            self.local_status = "stopping"
            self.sync_engine.update_lock_state(status="stopping")
            self._emit("status_changed", self.get_status_summary())
            uname = effective_user.username if effective_user else "System"
            self._log(f"Stopping server requested by {uname}...", "INFO", "Server")
            self._publish_host_telemetry_throttled(force=True)

            if self._proc is not None:
                proc_ref = self._proc
                self._proc = None
                if was_running:
                    self._stop_java_process_gracefully(proc_ref, wait_timeout=35.0)
                else:
                    self._terminate_process_tree(proc_ref, wait_timeout=3.0)

            self._stop_any_runtime_java_processes(server_port=port)

            if self._sim_running:
                self._log("Saving chunks for level 'ServerLevel[world]'...", "INFO", "ServerMain")
                self._sim_running = False
                self._sim_stop_event.set()

            if was_running:
                return self._finalize_stop_and_sync(effective_user)
            else:
                return self._cleanup_after_failed_start(effective_user, reason="")

    def _cleanup_after_failed_start(self, user: Optional[UserAccount], reason: str = "") -> Dict[str, Any]:
        """Releases the lock and stops the tunnel WITHOUT overwriting latest-server.zip when startup fails or is aborted early."""
        self._cancel_telemetry_timer()
        if reason:
            self.last_error = reason
        self.tunnel_manager.stop_tunnel()
        try:
            self.sync_engine.release_lock(user)
        except Exception:
            self.sync_engine.force_unlock()
        self.local_status = "idle"
        self.runtime_mode = "none"
        self._reached_running = False
        self.active_host_user = None
        self._active_max_players = 20
        self._auto_sync_in_progress = False
        with self._mutex:
            self.online_players = []
        self.started_at = 0.0
        self.server_ready_at = 0.0
        self.memory_used_mb = 0
        self.cpu_percent = 0.0
        self.tps = 20.0
        self._psutil_proc = None
        self._emit("status_changed", self.get_status_summary())
        self._emit("players_updated", self.get_players_data())
        return self.get_status_summary()

    def _finalize_stop_and_sync(self, user: Optional[UserAccount]) -> Dict[str, Any]:
        self._cancel_telemetry_timer()
        self.local_status = "syncing_push"
        with self._mutex:
            self.online_players = []
        self.sync_engine.update_lock_state(status="syncing_push", online_players=[])
        self._emit("status_changed", self.get_status_summary())
        self._emit("players_updated", self.get_players_data())

        self.tunnel_manager.stop_tunnel()
        cfg = self.sync_engine.load_cluster_config()
        create_backup = cfg.auto_backup_on_stop if cfg else True

        self._log("Packaging and syncing world files to Shared Sync Folder...", "INFO", "SyncEngine")
        self._publish_host_telemetry_throttled(force=True)
        push_res: Dict[str, Any] = {}
        push_err: Optional[Exception] = None
        for attempt in range(2):
            try:
                push_res = self._push_world_compat(
                    create_backup=create_backup,
                    backup_label=user.username if user else "host",
                    host_username=user.username if user else "",
                    require_lock_epoch=self.sync_engine._active_lock_epoch,
                )
                push_err = None
                break
            except Exception as e:
                push_err = e
                time.sleep(0.5)

        if push_err is None:
            kb = round(push_res.get("sizeBytes", 0) / 1024, 1)
            self._log(
                f"World synced to shared storage ({push_res.get('fileCount', 0)} files, {kb} KB). Releasing host lock.",
                "INFO",
                "SyncEngine",
            )
            self._cancel_telemetry_timer()
            try:
                self.sync_engine.release_lock(
                    user,
                    expected_snapshot_seq=int(push_res.get("snapshotSeq", 0) or 0),
                    expected_snapshot_hash=str(push_res.get("sha256", "") or ""),
                )
            except Exception:
                self.sync_engine.force_unlock()
        else:
            err_msg = f"CRITICAL: World snapshot push failed ({push_err}). Host lock preserved so unsaved progress is NOT overwritten!"
            self.last_error = err_msg
            self._log(err_msg, "ERROR", "SyncEngine")
            self.sync_engine.update_lock_state(status="sync_failed")
            self._publish_host_telemetry_throttled(force=True)

        self.local_status = "idle"
        self.runtime_mode = "none"
        self._reached_running = False
        self.active_host_user = None
        self._active_max_players = 20
        self._auto_sync_in_progress = False
        with self._mutex:
            self.online_players = []
        self.started_at = 0.0
        self.server_ready_at = 0.0
        self.memory_used_mb = 0
        self.cpu_percent = 0.0
        self._psutil_proc = None
        self._emit("status_changed", self.get_status_summary())
        self._emit("players_updated", self.get_players_data())
        return self.get_status_summary()

    def restart_hosting(self, user: Optional[UserAccount] = None) -> Dict[str, Any]:
        """
        Gracefully restarts the local Minecraft server while keeping the host lock and tunnel active,
        pushing a quick world snapshot in between without ever dropping to 'idle'.
        """
        with self._lifecycle_lock:
            effective_user = user or self.active_host_user
            if not effective_user:
                raise RuntimeError("No active host user found for restart.")
            if self.local_status not in ("running", "starting"):
                raise RuntimeError("Server is not currently running on this machine.")

            cfg = self.sync_engine.load_cluster_config()
            if not cfg:
                raise RuntimeError("Cluster configuration is not initialized.")
            props = self.get_server_properties()
            port = self._safe_int_prop(props, "server-port", cfg.server_port or 25565, 1, 65535)
            max_players = self._safe_int_prop(props, "max-players", 20, 1, 10000)
            self._active_server_port = port
            self._active_max_players = max_players

            self.local_status = "restarting"
            self._reached_running = False
            self.server_ready_at = 0.0
            self.memory_used_mb = 0
            self.cpu_percent = 0.0
            self.tps = 0.0
            self._psutil_proc = None
            self._last_auto_sync_at = 0.0
            self._last_auto_sync_status = "idle"
            self._last_auto_sync_error = ""
            with self._mutex:
                self.online_players = []
            self.sync_engine.update_lock_state(status="restarting", online_players=[])
            self._emit("status_changed", self.get_status_summary())
            self._emit("players_updated", self.get_players_data())
            self._log(f"Restarting server requested by {effective_user.username}...", "INFO", "Server")
            self._publish_host_telemetry_throttled(force=True)

            if self._proc is not None:
                proc_ref = self._proc
                self._proc = None
                self._stop_java_process_gracefully(proc_ref, wait_timeout=35.0)

            self._stop_any_runtime_java_processes(server_port=port)

            if self._sim_running:
                self._sim_running = False
                self._sim_stop_event.set()

            push_res: Dict[str, Any] = {}
            try:
                push_res = self._push_world_compat(
                    create_backup=cfg.auto_backup_on_stop if cfg else True,
                    backup_label=f"{effective_user.username}-restart",
                    host_username=effective_user.username,
                    require_lock_epoch=self.sync_engine._active_lock_epoch,
                )
            except Exception as e:
                self._log(f"Warning saving restart backup: {e}", "WARN", "SyncEngine")

            try:
                self.local_status = "starting"
                self.started_at = time.time()
                self.sync_engine.update_lock_state(
                    status="starting",
                    mc_version=f"{cfg.mc_software.capitalize()} {cfg.mc_version}",
                    max_players=max_players,
                    online_players=[],
                )
                self._emit("status_changed", self.get_status_summary())

                req_java = required_java_major_for_mc(cfg.mc_version)
                java_info = self.check_java_installed(cfg.java_path, cfg.mc_version)
                target_jar = get_versioned_jar_path(self.bin_dir, cfg.mc_software, cfg.mc_version)
                has_jar = target_jar.exists() and target_jar.stat().st_size > 1_000_000

                if (not java_info["installed"] or not has_jar) and os.environ.get("MINERELAY_TEST_MODE") != "1":
                    try:
                        if not java_info["installed"]:
                            ensure_portable_java(
                                self.bin_dir,
                                min_major=req_java,
                                log_fn=lambda msg: self._log(msg, "INFO", "Installer"),
                            )
                            java_info = self.check_java_installed(cfg.java_path, cfg.mc_version, force_refresh=True)
                        if not has_jar:
                            target_jar = ensure_server_jar(
                                self.runtime_dir,
                                cfg.mc_software,
                                cfg.mc_version,
                                log_fn=lambda msg: self._log(msg, "INFO", "Installer"),
                                bin_dir=self.bin_dir,
                            )
                            has_jar = target_jar.exists() and target_jar.stat().st_size > 1_000_000
                    except Exception as boot_err:
                        self._log(f"Auto-download notice during restart: {boot_err}", "WARN", "Installer")

                if java_info["installed"] and has_jar and os.environ.get("MINERELAY_TEST_MODE") != "1":
                    ensure_paperclip_mojang_cache(
                        target_jar,
                        self.runtime_dir,
                        log_fn=lambda msg: self._log(msg, "INFO", "Installer"),
                    )
                    self._spawn_java_minecraft(cfg, java_info["command"], target_jar, max_players, port)
                elif os.environ.get("MINERELAY_TEST_MODE") == "1":
                    self._spawn_embedded_minecraft_runtime(cfg, has_jar, java_info["installed"], max_players)
                else:
                    raise RuntimeError(
                        f"Java {req_java}+ and a valid {cfg.mc_software.upper()} {cfg.mc_version} server.jar are required to restart."
                    )

                return self.get_status_summary()
            except Exception as e:
                self._log(f"Restart failed: {e}", "ERROR", "Launcher")
                self._cancel_telemetry_timer()
                self.last_error = str(e)
                self.tunnel_manager.stop_tunnel()
                try:
                    self.sync_engine.release_lock(
                        effective_user,
                        expected_snapshot_seq=int(push_res.get("snapshotSeq", 0) or 0),
                        expected_snapshot_hash=str(push_res.get("sha256", "") or ""),
                    )
                except Exception:
                    self.sync_engine.force_unlock()
                self.local_status = "idle"
                self.runtime_mode = "none"
                self._reached_running = False
                self.active_host_user = None
                self._active_max_players = 20
                self._auto_sync_in_progress = False
                with self._mutex:
                    self.online_players = []
                self.started_at = 0.0
                self.server_ready_at = 0.0
                self.memory_used_mb = 0
                self.cpu_percent = 0.0
                self._psutil_proc = None
                self._emit("status_changed", self.get_status_summary())
                self._emit("players_updated", self.get_players_data())
                raise

    def clear_console_logs(self) -> Dict[str, Any]:
        with self._mutex:
            self.console_logs = []
        self._log("Console log history cleared by Admin.", "INFO", "Server")
        if self.local_status != "idle":
            self._publish_host_telemetry_throttled(force=True)
        return {"ok": True, "cleared": True, "logs": self.console_logs}

    # -------------------------------------------------------------------------
    # Console Commands (Admin-Only)
    # -------------------------------------------------------------------------

    def send_command(self, command: str) -> Dict[str, Any]:
        clean = command.strip()
        if not clean:
            raise ValueError("Command cannot be empty.")
        if clean.startswith("/"):
            clean = clean[1:]

        if self.local_status not in ("running", "starting"):
            raise RuntimeError("Server is not currently running on this machine.")

        self._log(f"> {clean}", "CMD", "AdminConsole")

        parts = clean.split()
        cmd_name = parts[0].lower() if parts else ""
        args = parts[1:] if len(parts) > 1 else []

        if cmd_name == "stop":
            threading.Thread(target=lambda: self.stop_hosting(self.active_host_user), daemon=True).start()
            return {"sent": True, "command": clean}

        if cmd_name == "simjoin" and args:
            p = args[0]
            with self._mutex:
                if p not in self.online_players:
                    self.online_players.append(p)
                self._last_player_active_at = time.time()
                curr_players = list(self.online_players)
            self._log(f"{p} joined the game", "INFO", "Server")
            self.sync_engine.update_lock_state(online_players=curr_players)
            self._emit("status_changed", self.get_status_summary())
            self._emit("players_updated", self.get_players_data())
            return {"sent": True, "command": clean}

        if cmd_name == "simleave" and args:
            p = args[0]
            with self._mutex:
                if p in self.online_players:
                    self.online_players.remove(p)
                self._last_player_active_at = time.time()
                curr_players = list(self.online_players)
            self._log(f"{p} left the game", "INFO", "Server")
            self.sync_engine.update_lock_state(online_players=curr_players)
            self._emit("status_changed", self.get_status_summary())
            self._emit("players_updated", self.get_players_data())
            return {"sent": True, "command": clean}

        if self._proc is not None and self._proc.stdin:
            if not self._write_stdin(self._proc, clean + "\n"):
                self._log("Failed to write to server stdin (process may be stopping)", "ERROR", "Server")
                raise RuntimeError("Failed to send command to server: stdin unavailable")
            return {"sent": True, "command": clean}

        # Embedded interactive command processor
        if cmd_name == "help":
            self._log(
                "Available commands: list, say <msg>, op <player>, deop <player>, whitelist <add|remove|list>, kick <player>, ban <player>, pardon <player>, gamemode <mode>, difficulty <diff>, time set <val>, save-all, stop",
                "INFO",
                "Server",
            )
        elif cmd_name == "list":
            with self._mutex:
                players_copy = list(self.online_players)
                max_p = self._active_max_players or 20
            self._log(
                f"There are {len(players_copy)} of a max of {max_p} players online: {', '.join(players_copy) or 'none'}",
                "INFO",
                "Server",
            )
        elif cmd_name == "say":
            msg = " ".join(args) or "Hello!"
            self._log(f"[Server] {msg}", "INFO", "Server")
        elif cmd_name == "save-all":
            self._log("Saving the game (this may take a moment!)", "INFO", "Server")
            self._log("Saved the game", "INFO", "Server")
            self._save_complete_event.set()
        elif cmd_name == "op" and args:
            self.player_action("op", args[0])
        elif cmd_name == "deop" and args:
            self.player_action("deop", args[0])
        elif cmd_name == "kick" and args:
            self.player_action("kick", args[0])
        elif cmd_name == "ban" and args:
            self.player_action("ban", args[0])
        elif cmd_name == "pardon" and args:
            self.player_action("pardon", args[0])
        else:
            self._log(f"Executed server command: '{clean}'", "INFO", "Server")

        return {"sent": True, "command": clean}

    def _ensure_idle_mutable_sync(self) -> None:
        """Guards idle config/file edits against active remote hosting and transient lifecycle states."""
        if self.local_status in ("syncing_pull", "stopping", "syncing_push", "restarting"):
            raise RuntimeError(
                f"Cannot modify server files while server is '{self.local_status}'. Wait for the transition to complete."
            )
        if self.local_status != "idle":
            return
        lock = self.sync_engine.get_lock()
        if lock.is_actively_locked() and lock.machine_id != self.sync_engine.local_config.machine_id:
            raise HostLockConflictError(
                f"Cannot modify server files while '{lock.host_username}' is hosting on '{lock.machine_name}'. Your edits would be overwritten when they stop."
            )
        self.sync_engine.ensure_runtime_synced_when_idle(strict=True)

    def _push_idle_config_safely(self) -> None:
        """
        Pushes updated config files from runtime_dir to the Shared Sync Folder ONLY if
        no other member holds the lock and local cloud snapshot is synced cleanly
        (so an idle PC never clobbers another member's newer world save).
        """
        if self.local_status != "idle":
            return
        lock = self.sync_engine.get_lock()
        if lock.is_locked and not self.sync_engine.is_locked_by_current_machine(lock):
            return
        if self.sync_engine._has_active_cloud_transfer():
            self._log("Skipped idle snapshot push while cloud sync download/upload is active.", "WARN", "SyncEngine")
            return
        manifest = self.sync_engine._read_sync_manifest()
        if not manifest and self.sync_engine.latest_snapshot_path.exists():
            self._log("Skipped idle snapshot push because shared snapshot manifest is still syncing.", "WARN", "SyncEngine")
            return
        if manifest:
            _, mismatch = self.sync_engine._resolve_verified_snapshot_source(manifest)
            if mismatch:
                self._log(f"Skipped idle snapshot push to protect newer cloud world: {mismatch}", "WARN", "SyncEngine")
                return
        try:
            self.sync_engine.push_world_from_runtime(create_backup=False, host_username="Config")
        except Exception:
            pass

    # -------------------------------------------------------------------------
    # Server Properties Editor (Admin-Only)
    # -------------------------------------------------------------------------

    def _sanitize_level_name(self, raw_name: str) -> str:
        """
        Sanitizes level-name to prevent directory traversal or collision with reserved system folders.
        """
        raw_str = str(raw_name or "").strip()
        if not raw_str or "/" in raw_str or "\\" in raw_str or ".." in raw_str:
            return "world"
        clean = Path(raw_str).name
        clean = "".join(c for c in clean if c.isalnum() or c in ("-", "_", " ")).strip()
        reserved = {"plugins", "cache", "libraries", "versions", "logs", "crash-reports", "tmp-client", "bin", "shared-sync"}
        if not clean or clean.lower() in reserved or clean in (".", ".."):
            return "world"
        return clean

    def _update_bukkit_allow_end(self, allow_end: bool) -> None:
        """
        Updates or creates bukkit.yml to set settings.allow-end to true or false.
        Paper and Bukkit natively check this setting to enable or disable The End dimension.
        """
        bukkit_path = self.runtime_dir / "bukkit.yml"
        val_str = "true" if allow_end else "false"
        if not bukkit_path.exists():
            content = f"# Bukkit/Paper configuration (Managed by MineRelay Dashboard)\nsettings:\n  allow-end: {val_str}\n"
            try:
                bukkit_path.write_text(content, encoding="utf-8")
            except OSError:
                pass
            return

        try:
            lines = bukkit_path.read_text(encoding="utf-8", errors="replace").splitlines()
            new_lines: List[str] = []
            in_settings = False
            found_allow_end = False

            for line in lines:
                stripped = line.strip()
                if stripped.startswith("settings:"):
                    in_settings = True
                    new_lines.append(line)
                    continue

                if in_settings:
                    # Leaving settings section if unindented non-comment/non-empty line
                    if line and not line.startswith(" ") and not line.startswith("\t") and not stripped.startswith("#"):
                        if not found_allow_end:
                            new_lines.append(f"  allow-end: {val_str}")
                            found_allow_end = True
                        in_settings = False
                    elif stripped.startswith("allow-end:"):
                        new_lines.append(f"  allow-end: {val_str}")
                        found_allow_end = True
                        continue

                new_lines.append(line)

            if not found_allow_end:
                if in_settings:
                    new_lines.append(f"  allow-end: {val_str}")
                else:
                    new_lines.append("\nsettings:")
                    new_lines.append(f"  allow-end: {val_str}")

            bukkit_path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
        except Exception:
            pass

    def get_server_properties(self) -> Dict[str, str]:
        if self.local_status == "idle":
            self.sync_engine.ensure_runtime_synced_when_idle()
        self.sync_engine._ensure_default_runtime_files()
        props_path = self.runtime_dir / "server.properties"
        props: Dict[str, str] = {}
        if props_path.exists():
            for line in props_path.read_text(encoding="utf-8", errors="replace").splitlines():
                stripped = line.strip()
                if not stripped or stripped.startswith("#") or "=" not in stripped:
                    continue
                k, v = stripped.split("=", 1)
                props[k.strip()] = v.strip()

        # Check End dimension status across server.properties and bukkit.yml
        if "disable-end" in props or "allow-end" in props:
            is_disabled = props.get("disable-end") == "true" or props.get("allow-end") == "false"
            props["disable-end"] = "true" if is_disabled else "false"
            props["allow-end"] = "false" if is_disabled else "true"
        else:
            bukkit_path = self.runtime_dir / "bukkit.yml"
            bukkit_allow_end = True
            if bukkit_path.exists():
                try:
                    for bline in bukkit_path.read_text(encoding="utf-8", errors="replace").splitlines():
                        if bline.strip().startswith("allow-end:"):
                            bukkit_allow_end = bline.split(":", 1)[1].strip().lower() != "false"
                            break
                except Exception:
                    pass
            props["disable-end"] = "false" if bukkit_allow_end else "true"
            props["allow-end"] = "true" if bukkit_allow_end else "false"

        return props

    def _migrate_playerdata_on_online_mode_change(self, level_name: str, target_online_mode: bool) -> int:
        """
        Preserves player inventories, stats, advancements, OP status, and whitelist entries when
        switching between Mojang Online Auth (online-mode=true) and Cracked Support (online-mode=false)
        by non-destructively mapping known Mojang UUIDs <-> OfflinePlayer v3 UUIDs via usercache/player lists.
        """
        world_dir = self.runtime_dir / self._sanitize_level_name(level_name)
        if not world_dir.exists() or not world_dir.is_dir():
            return 0

        known_players: Dict[str, Tuple[str, List[str]]] = {}
        for json_name in ("usercache.json", "ops.json", "whitelist.json", "banned-players.json"):
            jp = self.runtime_dir / json_name
            if not jp.exists():
                continue
            try:
                entries = json.loads(jp.read_text(encoding="utf-8", errors="replace"))
                if not isinstance(entries, list):
                    continue
                for item in entries:
                    if not isinstance(item, dict):
                        continue
                    name = str(item.get("name", "")).strip()
                    u_val = str(item.get("uuid", "")).strip().lower()
                    if not name or len(u_val) != 36 or "-" not in u_val:
                        continue
                    key = name.lower()
                    canon, ulist = known_players.get(key, (name, []))
                    if u_val not in ulist:
                        ulist.append(u_val)
                    known_players[key] = (canon, ulist)
            except Exception:
                continue

        if not known_players:
            return 0

        dir_groups = [
            (world_dir / "players" / "data", ".dat", True),
            (world_dir / "players" / "stats", ".json", False),
            (world_dir / "players" / "advancements", ".json", False),
            (world_dir / "playerdata", ".dat", True),
            (world_dir / "stats", ".json", False),
            (world_dir / "advancements", ".json", False),
        ]

        migrated_players = 0
        uuid_remaps: Dict[str, str] = {}

        for lower_name, (canon_name, candidate_uuids) in known_players.items():
            offline_uuid = self._compute_offline_uuid(canon_name).lower()
            online_uuids = [u for u in candidate_uuids if u != offline_uuid]
            if target_online_mode and not online_uuids and os.environ.get("MINERELAY_TEST_MODE") != "1":
                try:
                    mojang_url = f"https://api.mojang.com/users/profiles/minecraft/{urlquote(canon_name)}"
                    req = urllib.request.Request(mojang_url, headers={"User-Agent": "MineRelay/2.0"})
                    with urllib.request.urlopen(req, timeout=3.0) as resp:
                        profile = json.loads(resp.read().decode("utf-8"))
                        raw_id = str(profile.get("id", "")).strip().lower()
                        if len(raw_id) == 32:
                            formatted_uuid = f"{raw_id[:8]}-{raw_id[8:12]}-{raw_id[12:16]}-{raw_id[16:20]}-{raw_id[20:]}"
                            if formatted_uuid != offline_uuid:
                                online_uuids = [formatted_uuid]
                except Exception:
                    pass
            if not online_uuids:
                continue

            if not target_online_mode:
                src_uuids = online_uuids
                dst_uuid = offline_uuid
            else:
                src_uuids = [offline_uuid]
                dst_uuid = online_uuids[0]

            player_copied = False
            for src_uuid in src_uuids:
                for base_dir, ext, copy_old in dir_groups:
                    if not base_dir.exists() or not base_dir.is_dir():
                        continue
                    src_file = base_dir / f"{src_uuid}{ext}"
                    dst_file = base_dir / f"{dst_uuid}{ext}"
                    try:
                        if src_file.is_file():
                            dst_exists = dst_file.exists() and dst_file.stat().st_size > 0
                            src_newer = dst_exists and (src_file.stat().st_mtime > dst_file.stat().st_mtime + 1.0)
                            if not dst_exists or src_newer:
                                if dst_exists:
                                    shutil.copy2(dst_file, base_dir / f"{dst_uuid}{ext}.pre_migrate_bak")
                                shutil.copy2(src_file, dst_file)
                                player_copied = True
                    except OSError:
                        pass
                    if copy_old:
                        src_old = base_dir / f"{src_uuid}{ext}_old"
                        dst_old = base_dir / f"{dst_uuid}{ext}_old"
                        try:
                            if src_old.is_file() and (not dst_old.exists() or dst_old.stat().st_size == 0):
                                shutil.copy2(src_old, dst_old)
                        except OSError:
                            pass
            if player_copied:
                migrated_players += 1
            uuid_remaps[lower_name] = dst_uuid

        if uuid_remaps:
            for json_name in ("ops.json", "whitelist.json"):
                jp = self.runtime_dir / json_name
                if not jp.exists():
                    continue
                try:
                    data = json.loads(jp.read_text(encoding="utf-8", errors="replace"))
                    if not isinstance(data, list):
                        continue
                    changed = False
                    for entry in data:
                        if isinstance(entry, dict) and entry.get("name"):
                            k = str(entry["name"]).strip().lower()
                            if k in uuid_remaps and str(entry.get("uuid", "")).lower() != uuid_remaps[k]:
                                entry["uuid"] = uuid_remaps[k]
                                changed = True
                    if changed:
                        jp.write_text(json.dumps(data, indent=2), encoding="utf-8")
                except Exception:
                    pass

        if migrated_players > 0:
            mode_label = "Online Mojang Auth" if target_online_mode else "Cracked/Offline Support"
            self._log(
                f"Preserved inventory/stats/permissions for {migrated_players} player(s) across switch to {mode_label}.",
                "INFO",
                "Server",
            )
        return migrated_players

    def update_server_properties(self, updates: Dict[str, Any], _allow_during_transition: bool = False) -> Dict[str, str]:
        if not _allow_during_transition:
            self._ensure_idle_mutable_sync()
        props = self.get_server_properties()
        old_online_mode = props.get("online-mode", "true").lower() != "false"
        for k, v in updates.items():
            clean_key = str(k).replace("\r", "").replace("\n", "").strip()
            if clean_key:
                if isinstance(v, bool):
                    props[clean_key] = "true" if v else "false"
                else:
                    props[clean_key] = str(v).replace("\r", "").replace("\n", "").strip()

        # Handle disable-end and allow-end synchronization with bukkit.yml
        if "disable-end" in updates or "allow-end" in updates:
            if "disable-end" in updates:
                raw_dis = updates["disable-end"]
                is_disabled = raw_dis is True or str(raw_dis).lower() == "true"
            else:
                raw_ae = updates["allow-end"]
                is_disabled = raw_ae is False or str(raw_ae).lower() == "false"
            props["disable-end"] = "true" if is_disabled else "false"
            props["allow-end"] = "false" if is_disabled else "true"
            self._update_bukkit_allow_end(allow_end=not is_disabled)

        lines = ["# Minecraft server properties (Managed by MineRelay Dashboard)"]
        for k, v in sorted(props.items()):
            lines.append(f"{k}={v}")

        props_path = self.runtime_dir / "server.properties"
        props_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

        new_online_mode = props.get("online-mode", "true").lower() != "false"
        if "online-mode" in updates and (new_online_mode != old_online_mode or not new_online_mode):
            level_name = self._sanitize_level_name(props.get("level-name", "world"))
            self._migrate_playerdata_on_online_mode_change(level_name, new_online_mode)

        self._active_max_players = self._safe_int_prop(props, "max-players", 20, 1, 1000)
        self._active_server_port = self._safe_int_prop(props, "server-port", 25565, 1, 65535)
        self._get_cached_world_info(force_refresh=True)

        # If server is running locally, also apply live-reloadable settings immediately
        if self.local_status == "running" and self._proc is not None:
            cmds: List[str] = []
            if "difficulty" in updates:
                cmds.append(f"difficulty {props.get('difficulty', 'normal')}\n")
            if "white-list" in updates:
                wl_cmd = "whitelist on\n" if props.get("white-list") == "true" else "whitelist off\n"
                cmds.append(wl_cmd)
            if cmds:
                self._write_stdin(self._proc, "".join(cmds))
            self._publish_host_telemetry_throttled(force=True)

        self._push_idle_config_safely()
        return props

    # -------------------------------------------------------------------------
    # World Map & Seed Generator (Admin-Only)
    # -------------------------------------------------------------------------

    def get_world_info(self) -> Dict[str, Any]:
        props = self.get_server_properties()
        level_name = self._sanitize_level_name(props.get("level-name", "world"))
        seed = (props.get("level-seed", "") or "").strip()

        meta_path = self.runtime_dir / level_name / "level-metadata.json"
        if not seed and meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                seed = str(meta.get("seed", "") or "").strip()
            except Exception:
                pass

        raw_type = (props.get("level-type", "minecraft:normal") or "minecraft:normal").strip()
        if ":" not in raw_type:
            raw_type = f"minecraft:{raw_type}"
        generate_structures = props.get("generate-structures", "true").lower() != "false"

        world_dirs = [
            self.runtime_dir / level_name,
            self.runtime_dir / f"{level_name}_nether",
            self.runtime_dir / f"{level_name}_the_end",
        ]
        world_size_bytes = 0
        file_count = 0
        for d in world_dirs:
            if d.exists() and d.is_dir():
                for f in d.rglob("*"):
                    if f.is_file():
                        file_count += 1
                        try:
                            world_size_bytes += f.stat().st_size
                        except OSError:
                            pass

        world_exists = (self.runtime_dir / level_name).exists() and file_count > 0
        disable_end = props.get("disable-end") == "true" or props.get("allow-end") == "false"
        return {
            "levelName": level_name,
            "seed": seed,
            "levelType": raw_type,
            "generateStructures": generate_structures,
            "disableEnd": disable_end,
            "worldExists": world_exists,
            "worldSizeBytes": world_size_bytes,
            "worldFileCount": file_count,
        }

    def generate_new_world(
        self,
        mode: str = "random",
        seed: str = "",
        level_type: Optional[str] = None,
        generate_structures: Optional[bool] = None,
        disable_end: Optional[bool] = None,
        backup_existing: bool = True,
        user: Optional[UserAccount] = None,
    ) -> Dict[str, Any]:
        """
        Generates a brand new Minecraft world map either with a random 64-bit seed
        or a specific seed chosen by the Admin.
        - If a remote machine is currently hosting, raises HostLockConflictError.
        - Optionally creates a safety backup ('pre-world-reset') of the current world first.
        - Stops the local server if running, deletes old world directories, updates server.properties,
          syncs the clean state to the Shared Cloud Folder, and automatically restarts if it was running.
        """
        with self._lifecycle_lock:
            if self.local_status in ("syncing_pull", "stopping", "syncing_push", "restarting"):
                raise RuntimeError(f"Cannot change world map while server is '{self.local_status}'.")
            lock = self.sync_engine.get_lock()
            if lock.is_actively_locked() and not self.sync_engine.is_locked_by_current_machine(lock):
                raise HostLockConflictError(
                    f"Cannot change world map while {lock.host_username} ({lock.machine_name}) is actively hosting the server."
                )

            clean_mode = (mode or "random").strip().lower()
            clean_seed = str(seed or "").strip()
            if clean_mode == "custom" and not clean_seed:
                raise ValueError("Please enter a specific world seed, or select Random World Map.")

            if clean_mode == "random" or not clean_seed:
                # Generate an explicit 64-bit signed integer seed so the user can view/copy the exact seed
                final_seed = str(secrets.randbelow(18446744073709551616) - 9223372036854775808)
                clean_mode = "random"
            else:
                final_seed = clean_seed
                clean_mode = "custom"

            was_running_locally = self.local_status in ("running", "starting")
            if self.local_status == "idle":
                self.sync_engine.ensure_runtime_synced_when_idle()

            props = self.get_server_properties()
            level_name = self._sanitize_level_name(props.get("level-name", "world"))
            cfg = self.sync_engine.load_cluster_config()
            port = self._safe_int_prop(props, "server-port", cfg.server_port if cfg else 25565, 1, 65535)

            if was_running_locally:
                self.local_status = "restarting"
                self._reached_running = False
                self.server_ready_at = 0.0
                self.memory_used_mb = 0
                self.cpu_percent = 0.0
                self.tps = 0.0
                self._last_metrics_sample_at = 0.0
                with self._mutex:
                    self.online_players = []
                    proc_ref = self._proc
                    self._proc = None
                self.sync_engine.update_lock_state(status="restarting", online_players=[])
                self._publish_host_telemetry_throttled(force=True)
                self._emit("status_changed", self.get_status_summary())
                self._emit("players_updated", self.get_players_data())
                self._log("Stopping current world to generate new map...", "INFO", "Server")

                if proc_ref is not None:
                    self._stop_java_process_gracefully(proc_ref, wait_timeout=35.0)

                self._stop_any_runtime_java_processes(server_port=port)

                if self._sim_running:
                    self._sim_running = False
                    self._sim_stop_event.set()

            # Create safety backup of existing world if requested and a world exists
            backup_file: Optional[str] = None
            if backup_existing and (self.runtime_dir / level_name).exists():
                try:
                    bk_res = self._push_world_compat(
                        create_backup=True,
                        backup_label="pre-world-reset",
                        host_username=(user.username if user else ""),
                        stage_copy_before_zip=False,
                    )
                    backup_file = bk_res.get("backupFile")
                    if backup_file:
                        self._log(
                            f"Saved safety backup '{backup_file}' before world map reset.",
                            "INFO",
                            "SyncEngine",
                        )
                except Exception as e:
                    self._log(f"Notice: Could not create pre-reset backup: {e}", "WARN", "SyncEngine")

            # Remove all existing world dimension folders in runtime_dir (preserving system and plugin dirs)
            removed_dirs: List[str] = []
            preserved_dirs = {"plugins", "cache", "libraries", "versions", "logs", "crash-reports", "tmp-client", "bin"}
            target_world_names = {
                level_name,
                f"{level_name}_nether",
                f"{level_name}_the_end",
                "world",
                "world_nether",
                "world_the_end",
            }
            if self.runtime_dir.exists():
                for item in list(self.runtime_dir.iterdir()):
                    if item.is_dir() and item.name not in preserved_dirs:
                        if (
                            item.name in target_world_names
                            or (item / "level.dat").exists()
                            or (item / "level-metadata.json").exists()
                        ):
                            shutil.rmtree(item, ignore_errors=True)
                            removed_dirs.append(item.name)

            # Update server.properties with new seed and world generation options
            prop_updates: Dict[str, Any] = {"level-seed": final_seed}
            if level_type is not None and str(level_type).strip():
                lt = str(level_type).strip()
                if ":" not in lt:
                    lt = f"minecraft:{lt}"
                prop_updates["level-type"] = lt
            if generate_structures is not None:
                prop_updates["generate-structures"] = bool(generate_structures)
            if disable_end is not None:
                prop_updates["disable-end"] = bool(disable_end)
                prop_updates["allow-end"] = not bool(disable_end)

            updated_props = self.update_server_properties(prop_updates, _allow_during_transition=True)
            self._get_cached_world_info(force_refresh=True)

            # Push clean runtime snapshot (with old world removed and new seed written) to Shared Sync Folder
            try:
                self._push_world_compat(
                    create_backup=False,
                    backup_label=(user.username if user else "admin"),
                    host_username=(user.username if user else ""),
                    require_lock_epoch=(lock.lock_epoch if was_running_locally else ""),
                )
            except Exception as e:
                self._log(f"Warning syncing new world configuration: {e}", "WARN", "SyncEngine")

            mode_desc = f"random seed ({final_seed})" if clean_mode == "random" else f"seed '{final_seed}'"
            eff_type = updated_props.get("level-type", "minecraft:normal")
            self._log(
                f"World map reset with {mode_desc} [Type: {eff_type}]. Ready to generate fresh world chunks.",
                "INFO",
                "Server",
            )

            # If server was running locally, start it back up immediately so the new world generates right away
            if was_running_locally and cfg:
                max_players = self._safe_int_prop(updated_props, "max-players", 20, 1, 1000)
                self._active_max_players = max_players
                self._active_server_port = port
                self.local_status = "starting"
                self.started_at = time.time()
                self._last_player_active_at = self.started_at
                self.sync_engine.update_lock_state(status="starting", max_players=max_players, online_players=[])
                self._publish_host_telemetry_throttled(force=True)
                self._emit("status_changed", self.get_status_summary())

                java_info = self.check_java_installed(cfg.java_path, cfg.mc_version)
                target_jar = get_versioned_jar_path(self.bin_dir, cfg.mc_software, cfg.mc_version)
                has_jar = target_jar.exists() and target_jar.stat().st_size > 1_000_000

                try:
                    if os.environ.get("MINERELAY_TEST_MODE") == "1":
                        self._spawn_embedded_minecraft_runtime(cfg, has_jar, java_info["installed"], max_players)
                    else:
                        if not java_info["installed"]:
                            ensure_portable_java(
                                self.bin_dir,
                                min_major=required_java_major_for_mc(cfg.mc_version),
                                log_fn=lambda msg: self._log(msg, "INFO", "Installer"),
                            )
                            self._java_cache.clear()
                            java_info = self.check_java_installed(cfg.java_path, cfg.mc_version)
                        if not has_jar:
                            target_jar = ensure_server_jar(
                                self.runtime_dir,
                                cfg.mc_software,
                                cfg.mc_version,
                                log_fn=lambda msg: self._log(msg, "INFO", "Installer"),
                                bin_dir=self.bin_dir,
                            )
                            has_jar = target_jar.exists() and target_jar.stat().st_size > 1_000_000
                        if not java_info["installed"] or not has_jar:
                            raise RuntimeError(
                                "Java runtime or server.jar missing; cannot restart server after world reset."
                            )
                        ensure_paperclip_mojang_cache(
                            target_jar,
                            self.runtime_dir,
                            log_fn=lambda msg: self._log(msg, "INFO", "Installer"),
                        )
                        self._spawn_java_minecraft(cfg, java_info["command"], target_jar, max_players)
                except Exception as restart_err:
                    self._cleanup_after_failed_start(
                        f"Failed to restart server after world reset: {restart_err}"
                    )
                    raise

            return {
                "reset": True,
                "mode": clean_mode,
                "seed": final_seed,
                "levelType": eff_type,
                "removedDirs": removed_dirs,
                "backupFile": backup_file,
                "restarted": was_running_locally,
                "world": self._get_cached_world_info(force_refresh=True) or self.get_world_info(),
                "properties": updated_props,
                "status": self.get_status_summary(),
            }

    # -------------------------------------------------------------------------
    # World Gamerules Manager (Live & Persistent Across Hosts)
    # -------------------------------------------------------------------------

    DEFAULT_GAMERULES: Dict[str, Any] = {
        "keepInventory": False,
        "mobGriefing": True,
        "doDaylightCycle": True,
        "doWeatherCycle": True,
        "doInsomnia": True,
        "doFireTick": True,
        "forgiveDeadPlayers": True,
        "doImmediateRespawn": False,
        "announceAdvancements": True,
        "playersSleepingPercentage": 100,
        "randomTickSpeed": 3,
        "spawnRadius": 10,
    }

    def get_gamerules(self) -> Dict[str, Any]:
        cfg = self.sync_engine.load_cluster_config()
        merged = dict(self.DEFAULT_GAMERULES)
        if cfg and cfg.gamerules:
            merged.update(cfg.gamerules)
        return merged

    def update_gamerules(self, updates: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(updates, dict):
            raise ValueError("Gamerules payload must be an object.")
        self._ensure_idle_mutable_sync()
        cfg = self.sync_engine.load_cluster_config()
        if not cfg:
            raise ValueError("Cluster not initialized.")
        current = dict(self.DEFAULT_GAMERULES)
        if cfg.gamerules:
            current.update(cfg.gamerules)

        applied_live: List[str] = []
        for rule, raw_val in updates.items():
            clean_rule = re.sub(r"[^a-zA-Z0-9_]", "", str(rule).strip())
            if not clean_rule:
                continue
            if isinstance(raw_val, bool):
                val_norm: Any = raw_val
                cmd_val = "true" if raw_val else "false"
            elif isinstance(raw_val, int) or (isinstance(raw_val, str) and raw_val.strip().lstrip("-").isdigit()):
                val_norm = int(str(raw_val).strip())
                cmd_val = str(val_norm)
            else:
                s_val = str(raw_val).strip().lower()
                if s_val in ("true", "false"):
                    val_norm = s_val == "true"
                    cmd_val = s_val
                else:
                    continue
            current[clean_rule] = val_norm
            if self.local_status == "running" and self._proc is not None:
                if self._write_stdin(self._proc, f"gamerule {clean_rule} {cmd_val}\n"):
                    applied_live.append(f"{clean_rule}={cmd_val}")

        cfg.gamerules = current
        self.sync_engine.save_cluster_config(cfg)
        if applied_live:
            self._log(f"Applied live gamerules: {', '.join(applied_live)}", "INFO", "Server")
        else:
            self._log("Saved gamerules to cluster config (will apply automatically on server start).", "INFO", "Server")
        return current

    def _apply_saved_gamerules_to_live_server(self, proc: subprocess.Popen) -> None:
        try:
            cfg = self.sync_engine.load_cluster_config()
            if not cfg or not cfg.gamerules or not proc.stdin:
                return
            cmds: List[str] = []
            for rule, val in cfg.gamerules.items():
                clean_rule = re.sub(r"[^a-zA-Z0-9_]", "", str(rule).strip())
                if not clean_rule:
                    continue
                cmd_val = ("true" if val else "false") if isinstance(val, bool) else str(val)
                cmds.append(f"gamerule {clean_rule} {cmd_val}\n")
            if cmds:
                self._write_stdin(proc, "".join(cmds))
        except Exception:
            pass

    # -------------------------------------------------------------------------
    # Player Management: Ops, Whitelist, Bans, Online Players (Admin-Only)
    # -------------------------------------------------------------------------

    def _compute_offline_uuid(self, username: str) -> str:
        """Computes standard Minecraft v3 UUID for `OfflinePlayer:<username>`."""
        raw_md5 = bytearray(hashlib.md5(f"OfflinePlayer:{username}".encode("utf-8")).digest())
        raw_md5[6] = (raw_md5[6] & 0x0F) | 0x30
        raw_md5[8] = (raw_md5[8] & 0x3F) | 0x80
        return str(uuid.UUID(bytes=bytes(raw_md5)))

    def _resolve_player_uuid(self, username: str) -> str:
        """
        Resolves a valid 36-char Minecraft UUID from usercache.json, Mojang API (when online),
        or standard Minecraft OfflinePlayer v3 UUID so PaperMC never rejects ops/whitelist/bans JSON.
        """
        clean = username.strip()
        # 1. Check usercache.json in runtime_dir
        usercache_path = self.runtime_dir / "usercache.json"
        if usercache_path.exists():
            try:
                entries = json.loads(usercache_path.read_text(encoding="utf-8", errors="replace"))
                if isinstance(entries, list):
                    for entry in entries:
                        if str(entry.get("name", "")).lower() == clean.lower() and entry.get("uuid"):
                            u_str = str(entry["uuid"])
                            if len(u_str) == 36 and "-" in u_str:
                                return u_str
            except Exception:
                pass

        # 2. If not in test mode, try quick Mojang profile lookup if online-mode is enabled
        if os.environ.get("MINERELAY_TEST_MODE") != "1":
            try:
                props = self.get_server_properties()
                if props.get("online-mode", "true") != "false":
                    req = urllib.request.Request(
                        f"https://api.mojang.com/users/profiles/minecraft/{urlquote(clean)}",
                        headers={"User-Agent": "MineRelay/1.0"},
                    )
                    with urllib.request.urlopen(req, timeout=1.5) as resp:
                        data = json.loads(resp.read().decode("utf-8"))
                        raw_id = str(data.get("id", "")).strip()
                        if len(raw_id) == 32:
                            return str(uuid.UUID(hex=raw_id))
            except Exception:
                pass

        return self._compute_offline_uuid(clean)

    def _read_player_json(self, filename: str) -> List[Dict[str, Any]]:
        if self.local_status == "idle":
            self.sync_engine.ensure_runtime_synced_when_idle()
        self.sync_engine._ensure_default_runtime_files()
        path = self.runtime_dir / filename
        if not path.exists():
            return []
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(data, list):
                return []
            # Auto-repair any legacy non-UUID strings (e.g. 'offline-uuid-...')
            modified = False
            for item in data:
                if isinstance(item, dict) and item.get("name"):
                    u_val = str(item.get("uuid", ""))
                    if u_val.startswith("offline-uuid-") or len(u_val) != 36:
                        item["uuid"] = self._compute_offline_uuid(str(item["name"]))
                        modified = True
            if modified:
                can_write = True
                if self.local_status == "idle":
                    lock = self.sync_engine.get_lock()
                    if lock.is_actively_locked() and not self.sync_engine.is_locked_by_current_machine(lock):
                        can_write = False
                if can_write:
                    try:
                        path.write_text(json.dumps(data, indent=2), encoding="utf-8")
                    except OSError:
                        pass
            return data
        except Exception:
            return []

    def _write_player_json(self, filename: str, data: List[Dict[str, Any]]) -> None:
        self._ensure_idle_mutable_sync()
        path = self.runtime_dir / filename
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        self._push_idle_config_safely()

    def get_players_data(self) -> Dict[str, Any]:
        with self._mutex:
            local_running = self.local_status == "running"
            local_online = list(self.online_players) if local_running else None
            local_max = self._active_max_players
        if local_online is not None:
            online = local_online
            max_p = local_max or 20
        else:
            lock = self.sync_engine.get_lock()
            is_local_host = self.sync_engine.is_locked_by_current_machine(lock)
            if (not is_local_host) and self.local_status == "idle" and lock.is_actively_locked():
                remote_tel = self.sync_engine.read_host_telemetry(expected_epoch=lock.lock_epoch)
                if remote_tel and isinstance(remote_tel.get("onlinePlayers"), list):
                    online = [str(p) for p in remote_tel["onlinePlayers"] if p]
                else:
                    online = list(lock.online_players)
                max_p = int((remote_tel.get("maxPlayers") if remote_tel and remote_tel.get("maxPlayers") else lock.max_players) or 20)
            else:
                online = list(lock.online_players) if lock.is_actively_locked() else []
                max_p = lock.max_players or 20
        return {
            "online": online,
            "max": max_p,
            "ops": self._read_player_json("ops.json"),
            "whitelist": self._read_player_json("whitelist.json"),
            "banned": self._read_player_json("banned-players.json"),
        }

    def player_action(self, action: str, username: str, reason: str = "Banned by Admin") -> Dict[str, Any]:
        clean_user = username.strip().replace("\r", "").replace("\n", "")
        if not clean_user:
            raise ValueError("Player username is required.")
        clean_reason = (reason or "").replace("\r", " ").replace("\n", " ").strip()

        if self.local_status == "idle":
            lock = self.sync_engine.get_lock()
            if lock.is_actively_locked() and not self.sync_engine.is_locked_by_current_machine(lock):
                raise HostLockConflictError(
                    f"Cannot modify or control player '{clean_user}' while '{lock.host_username}' is hosting on '{lock.machine_name}'."
                )

        live_only_actions = ("kick", "gamemode", "heal", "tp_spawn", "xp_level", "clear_inventory", "whisper")
        if action in live_only_actions and self.local_status not in ("running", "starting"):
            raise RuntimeError("Server must be running locally to execute live player commands.")

        player_uuid = self._resolve_player_uuid(clean_user) if action in ("op", "whitelist_add", "ban") else ""

        if action == "op":
            ops = self._read_player_json("ops.json")
            if not any(o.get("name", "").lower() == clean_user.lower() for o in ops):
                ops.append({"uuid": player_uuid, "name": clean_user, "level": 4, "bypassesPlayerLimit": False})
                self._write_player_json("ops.json", ops)
            if self.local_status == "running" and self._proc is not None:
                self._write_stdin(self._proc, f"op {clean_user}\n")
            self._log(f"Made {clean_user} a server operator", "INFO", "Server")
            self._emit("players_updated", self.get_players_data())

        elif action == "deop":
            ops = [o for o in self._read_player_json("ops.json") if o.get("name", "").lower() != clean_user.lower()]
            self._write_player_json("ops.json", ops)
            if self.local_status == "running" and self._proc is not None:
                self._write_stdin(self._proc, f"deop {clean_user}\n")
            self._log(f"Made {clean_user} no longer a server operator", "INFO", "Server")
            self._emit("players_updated", self.get_players_data())

        elif action == "whitelist_add":
            wl = self._read_player_json("whitelist.json")
            if not any(w.get("name", "").lower() == clean_user.lower() for w in wl):
                wl.append({"uuid": player_uuid, "name": clean_user})
                self._write_player_json("whitelist.json", wl)
            if self.local_status == "running" and self._proc is not None:
                self._write_stdin(self._proc, f"whitelist add {clean_user}\nwhitelist reload\n")
            self._log(f"Added {clean_user} to the whitelist", "INFO", "Server")
            self._emit("players_updated", self.get_players_data())

        elif action == "whitelist_remove":
            wl = [w for w in self._read_player_json("whitelist.json") if w.get("name", "").lower() != clean_user.lower()]
            self._write_player_json("whitelist.json", wl)
            if self.local_status == "running" and self._proc is not None:
                self._write_stdin(self._proc, f"whitelist remove {clean_user}\nwhitelist reload\n")
            self._log(f"Removed {clean_user} from the whitelist", "INFO", "Server")
            self._emit("players_updated", self.get_players_data())

        elif action == "ban":
            banned = self._read_player_json("banned-players.json")
            if not any(b.get("name", "").lower() == clean_user.lower() for b in banned):
                banned.append({
                    "uuid": player_uuid,
                    "name": clean_user,
                    "created": time.strftime("%Y-%m-%d %H:%M:%S +0000"),
                    "source": "Admin",
                    "expires": "forever",
                    "reason": clean_reason,
                })
                self._write_player_json("banned-players.json", banned)
            if self.local_status in ("running", "starting"):
                with self._mutex:
                    to_remove = [p for p in self.online_players if p.lower() == clean_user.lower()]
                    for p in to_remove:
                        self.online_players.remove(p)
                    curr_players = list(self.online_players)
                self.sync_engine.update_lock_state(online_players=curr_players)
                if self._proc is not None:
                    ban_cmd = f"ban {clean_user} {clean_reason}\n" if clean_reason else f"ban {clean_user}\n"
                    self._write_stdin(self._proc, ban_cmd)
            self._log(f"Banned player {clean_user}: {clean_reason}", "INFO", "Server")
            self._emit("status_changed", self.get_status_summary())
            self._emit("players_updated", self.get_players_data())

        elif action == "pardon":
            banned = [b for b in self._read_player_json("banned-players.json") if b.get("name", "").lower() != clean_user.lower()]
            self._write_player_json("banned-players.json", banned)
            if self.local_status == "running" and self._proc is not None:
                self._write_stdin(self._proc, f"pardon {clean_user}\n")
            self._log(f"Unbanned player {clean_user}", "INFO", "Server")
            self._emit("players_updated", self.get_players_data())

        elif action == "kick":
            with self._mutex:
                to_remove = [p for p in self.online_players if p.lower() == clean_user.lower()]
                for p in to_remove:
                    self.online_players.remove(p)
                curr_players = list(self.online_players)
            self.sync_engine.update_lock_state(online_players=curr_players)
            if self._proc is not None:
                kick_cmd = f"kick {clean_user} {clean_reason}\n" if clean_reason else f"kick {clean_user}\n"
                self._write_stdin(self._proc, kick_cmd)
            self._log(f"Kicked {clean_user} from the game", "INFO", "Server")
            self._emit("status_changed", self.get_status_summary())
            self._emit("players_updated", self.get_players_data())

        elif action == "gamemode":
            mode = clean_reason.lower() if clean_reason.lower() in ("survival", "creative", "adventure", "spectator") else "survival"
            if self._proc is not None:
                self._write_stdin(self._proc, f"gamemode {mode} {clean_user}\n")
            self._log(f"Set {clean_user}'s gamemode to {mode}", "INFO", "Server")

        elif action == "heal":
            if self._proc is not None:
                self._write_stdin(
                    self._proc,
                    f"effect give {clean_user} minecraft:instant_health 1 10 true\n"
                    f"effect give {clean_user} minecraft:saturation 5 10 true\n",
                )
            self._log(f"Healed and fed {clean_user}", "INFO", "Server")

        elif action == "tp_spawn":
            if self._proc is not None:
                self._write_stdin(
                    self._proc,
                    f"execute in minecraft:overworld run tp {clean_user} 0 100 0\n"
                    f"effect give {clean_user} minecraft:slow_falling 15 1 true\n",
                )
            self._log(f"Teleported {clean_user} to world spawn", "INFO", "Server")

        elif action == "xp_level":
            if self._proc is not None:
                self._write_stdin(self._proc, f"xp add {clean_user} 5 levels\n")
            self._log(f"Gave 5 XP levels to {clean_user}", "INFO", "Server")

        elif action == "clear_inventory":
            if self._proc is not None:
                self._write_stdin(self._proc, f"clear {clean_user}\n")
            self._log(f"Cleared inventory of {clean_user}", "INFO", "Server")

        elif action == "whisper":
            msg = clean_reason or "Hello from Admin!"
            if self._proc is not None:
                self._write_stdin(self._proc, f"tell {clean_user} [Admin] {msg}\n")
            self._log(f"Whispered to {clean_user}: {msg}", "INFO", "Server")

        else:
            raise ValueError(f"Unknown player action: {action}")

        return self.get_players_data()

    # -------------------------------------------------------------------------
    # Server Jar Downloader (PaperMC Fill v3 & Vanilla Mojang APIs)
    # -------------------------------------------------------------------------

    def get_available_versions(self) -> Dict[str, Any]:
        return fetch_live_minecraft_versions()

    def download_server_jar(self, software: str, version: str) -> Dict[str, Any]:
        """Downloads official PaperMC or Vanilla Minecraft server.jar into data/bin/jars/."""
        software = software.lower().strip()
        version = version.strip()
        with self._jar_dl_lock:
            if self.jar_downloading:
                return {"downloading": True, "progress": self.jar_download_progress, "status": self.jar_download_status}
            self.jar_downloading = True
            self.jar_download_progress = 10
            self.jar_download_status = f"Resolving {software.upper()} {version} download URL..."

        def _worker() -> None:
            try:
                req_java = required_java_major_for_mc(version)
                java_info = self.check_java_installed("java", version)
                if not java_info["installed"]:
                    self.jar_download_status = f"Installing Portable Java {req_java} Runtime for Minecraft {version}..."
                    ensure_portable_java(
                        self.bin_dir,
                        min_major=req_java,
                        log_fn=lambda msg: self._log(msg, "INFO", "Installer"),
                    )
                    self._java_cache.clear()

                jar_path = ensure_server_jar(
                    self.runtime_dir,
                    software,
                    version,
                    log_fn=lambda msg: self._log(msg, "INFO", "Installer"),
                    force=True,
                    bin_dir=self.bin_dir,
                )
                self.jar_download_progress = 100
                self.jar_download_status = (
                    f"Installed {software.upper()} {version} ({round(jar_path.stat().st_size / (1024*1024), 1)} MB)"
                )

                with self._jar_dl_lock:
                    cfg = self.sync_engine.load_cluster_config()
                    if cfg:
                        cfg.mc_software = software
                        cfg.mc_version = version
                        self.sync_engine.save_cluster_config(cfg)

            except Exception as e:
                self.jar_download_status = f"Download failed: {e}"
                self._log(self.jar_download_status, "ERROR", "Installer")
            finally:
                with self._jar_dl_lock:
                    self.jar_downloading = False
                self._emit("status_changed", self.get_status_summary())

        threading.Thread(target=_worker, daemon=True, name="JarDownloader").start()
        return {"started": True, "software": software, "version": version}

    # -------------------------------------------------------------------------
    # Server File & Plugin Manager (Admin-Only)
    # -------------------------------------------------------------------------

    def _safe_resolve_runtime_path(self, rel_path: str) -> Path:
        clean = rel_path.strip().lstrip("/\\")
        base = self.runtime_dir.resolve()
        resolved = (base / clean).resolve()
        if not resolved.is_relative_to(base):
            raise PermissionError("Access outside runtime server directory is forbidden.")
        return resolved

    def list_files(self, rel_dir: str = "", search: str = "") -> List[Dict[str, Any]]:
        if self.local_status == "idle":
            self.sync_engine.ensure_runtime_synced_when_idle()
        self.sync_engine._ensure_default_runtime_files()
        target = self._safe_resolve_runtime_path(rel_dir)
        if not target.exists() or not target.is_dir():
            return []

        clean_search = search.strip().lower()
        items: List[Dict[str, Any]] = []

        if not clean_search:
            entries = []
            for entry in target.iterdir():
                try:
                    is_dir = entry.is_dir()
                    st = entry.stat()
                    entries.append((not is_dir, entry.name.lower(), {
                        "name": entry.name,
                        "path": str(entry.relative_to(self.runtime_dir)).replace("\\", "/"),
                        "isDir": is_dir,
                        "sizeBytes": 0 if is_dir else st.st_size,
                        "modifiedAt": st.st_mtime,
                    }))
                except OSError:
                    continue
            entries.sort(key=lambda x: (x[0], x[1]))
            return [x[2] for x in entries]

        # Recursive search within target (or runtime_dir if rel_dir is root)
        excluded_search_dirs = {"cache", "libraries", "versions", ".paper-remapped"}
        match_count = 0
        max_matches = 250

        for root, dirs, files in os.walk(target):
            dirs[:] = [d for d in dirs if d not in excluded_search_dirs]
            rel_root = Path(root).relative_to(self.runtime_dir)

            for d in list(dirs):
                d_path = Path(root) / d
                rel_d = d_path.relative_to(self.runtime_dir)
                rel_d_str = str(rel_d).replace("\\", "/")
                if clean_search in d.lower() or clean_search in rel_d_str.lower():
                    try:
                        st = d_path.stat()
                        items.append({
                            "name": d,
                            "path": rel_d_str,
                            "isDir": True,
                            "sizeBytes": 0,
                            "modifiedAt": st.st_mtime,
                            "parentDir": str(rel_root).replace("\\", "/") if str(rel_root) != "." else "",
                        })
                        match_count += 1
                        if match_count >= max_matches:
                            break
                    except OSError:
                        pass
            if match_count >= max_matches:
                break

            for f in files:
                f_path = Path(root) / f
                rel_f = f_path.relative_to(self.runtime_dir)
                rel_f_str = str(rel_f).replace("\\", "/")
                if clean_search in f.lower() or clean_search in rel_f_str.lower():
                    try:
                        st = f_path.stat()
                        items.append({
                            "name": f,
                            "path": rel_f_str,
                            "isDir": False,
                            "sizeBytes": st.st_size,
                            "modifiedAt": st.st_mtime,
                            "parentDir": str(rel_root).replace("\\", "/") if str(rel_root) != "." else "",
                        })
                        match_count += 1
                        if match_count >= max_matches:
                            break
                    except OSError:
                        pass
            if match_count >= max_matches:
                break

        items.sort(key=lambda x: (not x["isDir"], x["path"].lower()))
        return items

    def read_file_content(self, rel_path: str) -> Dict[str, Any]:
        if self.local_status == "idle":
            self.sync_engine.ensure_runtime_synced_when_idle()
        target = self._safe_resolve_runtime_path(rel_path)
        if not target.exists() or target.is_dir():
            raise FileNotFoundError(f"File '{rel_path}' not found.")
        if target.stat().st_size > 2 * 1024 * 1024:
            raise ValueError("File is too large to edit in browser (> 2MB).")
        content = target.read_text(encoding="utf-8", errors="replace")
        return {"path": rel_path, "content": content}

    def write_file_content(self, rel_path: str, content: str) -> Dict[str, Any]:
        self._ensure_idle_mutable_sync()
        target = self._safe_resolve_runtime_path(rel_path)
        if target == self.runtime_dir.resolve() or target.is_dir():
            raise ValueError("Target path must be a file, not a directory.")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
        self._push_idle_config_safely()
        return {"saved": True, "path": rel_path}

    def upload_file_base64(self, rel_dir: str, filename: str, base64_data: str) -> Dict[str, Any]:
        self._ensure_idle_mutable_sync()
        safe_name = Path(filename).name
        if not safe_name or safe_name in (".", ".."):
            raise ValueError("Invalid filename for upload.")
        target_dir = self._safe_resolve_runtime_path(rel_dir)
        target_dir.mkdir(parents=True, exist_ok=True)
        target_file = target_dir / safe_name
        try:
            raw_bytes = base64.b64decode(base64_data, validate=True)
        except (binascii.Error, ValueError) as e:
            raise ValueError(f"Invalid base64 upload payload: {e}")
        target_file.write_bytes(raw_bytes)
        self._push_idle_config_safely()
        return {
            "uploaded": True,
            "path": str(target_file.relative_to(self.runtime_dir)).replace("\\", "/"),
            "sizeBytes": len(raw_bytes),
        }

    def delete_runtime_file(self, rel_path: str) -> Dict[str, Any]:
        self._ensure_idle_mutable_sync()
        target = self._safe_resolve_runtime_path(rel_path)
        if target == self.runtime_dir.resolve():
            raise ValueError("Cannot delete root runtime directory.")
        if target.exists():
            if target.is_dir():
                shutil.rmtree(target)
            else:
                target.unlink()
        self._push_idle_config_safely()
        return {"deleted": True, "path": rel_path}

    # -------------------------------------------------------------------------
    # Plugin Manager & Modrinth 1-Click Plugin Marketplace (Admin-Only)
    # -------------------------------------------------------------------------

    CURATED_PLUGINS: List[Dict[str, Any]] = [
        {
            "projectId": "fALzjamp",
            "project_id": "fALzjamp",
            "slug": "chunky",
            "title": "Chunky",
            "description": "Pre-generates world chunks quickly and efficiently to eliminate exploration lag.",
            "author": "pop4959",
            "downloads": 2800000,
        },
        {
            "projectId": "P1OZGk5p",
            "project_id": "P1OZGk5p",
            "slug": "viaversion",
            "title": "ViaVersion",
            "description": "Allows players on newer Minecraft client versions to connect to your server.",
            "author": "Mylescomputer",
            "downloads": 6500000,
        },
        {
            "projectId": "Vebnzrzj",
            "project_id": "Vebnzrzj",
            "slug": "luckperms",
            "title": "LuckPerms",
            "description": "A permissions plugin for Minecraft servers with web editor and group ranks.",
            "author": "Luck",
            "downloads": 5100000,
        },
        {
            "projectId": "1u6JkXh5",
            "project_id": "1u6JkXh5",
            "slug": "worldedit",
            "title": "WorldEdit",
            "description": "In-game map editor and building tool for large structures and terrain.",
            "author": "EngineHub",
            "downloads": 8900000,
        },
        {
            "projectId": "hXiIvTyT",
            "project_id": "hXiIvTyT",
            "slug": "essentialsx",
            "title": "EssentialsX",
            "description": "Essential commands (/home, /tpa, /spawn, /warp, economy, kits) for Paper servers.",
            "author": "EssentialsX",
            "downloads": 4200000,
        },
        {
            "projectId": "tsL442db",
            "project_id": "tsL442db",
            "slug": "skinsrestorer",
            "title": "SkinsRestorer",
            "description": "Restores and customizes player skins on both online and offline mode servers.",
            "author": "SRTeam",
            "downloads": 3900000,
        },
    ]

    def list_plugins(self) -> List[Dict[str, Any]]:
        if self.local_status == "idle":
            self.sync_engine.ensure_runtime_synced_when_idle()
        plugins_dir = self.runtime_dir / "plugins"
        plugins_dir.mkdir(parents=True, exist_ok=True)
        items: List[Dict[str, Any]] = []
        for entry in sorted(plugins_dir.iterdir(), key=lambda p: p.name.lower()):
            if not entry.is_file():
                continue
            lname = entry.name.lower()
            if not (lname.endswith(".jar") or lname.endswith(".jar.disabled")):
                continue
            enabled = lname.endswith(".jar")
            base_name = entry.name[:-4] if enabled else entry.name[:-13]
            st = entry.stat()
            items.append({
                "filename": entry.name,
                "name": base_name,
                "enabled": enabled,
                "sizeBytes": st.st_size,
                "modifiedAt": st.st_mtime,
            })
        return items

    def toggle_plugin(self, filename: str) -> List[Dict[str, Any]]:
        self._ensure_idle_mutable_sync()
        safe_name = Path(filename.strip()).name
        if not safe_name:
            raise ValueError("Plugin filename is required.")
        plugins_dir = self.runtime_dir / "plugins"
        target = plugins_dir / safe_name
        if not target.exists() or not target.is_file():
            raise FileNotFoundError(f"Plugin '{safe_name}' not found.")
        if safe_name.lower().endswith(".jar.disabled"):
            new_name = safe_name[:-9]  # strip .disabled
        elif safe_name.lower().endswith(".jar"):
            new_name = safe_name + ".disabled"
        else:
            raise ValueError("Invalid plugin file extension.")
        dest = plugins_dir / new_name
        os.replace(target, dest)
        self._push_idle_config_safely()
        self._log(f"Plugin '{safe_name}' -> '{new_name}' (Restart server to apply).", "INFO", "Plugins")
        return self.list_plugins()

    def delete_plugin(self, filename: str) -> List[Dict[str, Any]]:
        self._ensure_idle_mutable_sync()
        safe_name = Path(filename.strip()).name
        if not safe_name:
            raise ValueError("Plugin filename is required.")
        plugins_dir = self.runtime_dir / "plugins"
        target = plugins_dir / safe_name
        if target.exists() and target.is_file():
            target.unlink()
            self._push_idle_config_safely()
            self._log(f"Deleted plugin '{safe_name}'.", "INFO", "Plugins")
        return self.list_plugins()

    def search_modrinth_plugins(self, query: str = "") -> List[Dict[str, Any]]:
        clean_q = query.strip()
        if os.environ.get("MINERELAY_TEST_MODE") != "1":
            try:
                facets = urlquote('[["project_type:plugin"],["categories:paper","categories:spigot","categories:bukkit"]]')
                q_param = f"query={urlquote(clean_q)}&" if clean_q else ""
                url = f"https://api.modrinth.com/v2/search?{q_param}facets={facets}&limit=10"
                req = urllib.request.Request(url, headers={"User-Agent": "MineRelay/1.0 (github.com/minerelay)"})
                with urllib.request.urlopen(req, timeout=4.0) as resp:
                    payload = json.loads(resp.read().decode("utf-8"))
                    hits = payload.get("hits", [])
                    results = []
                    for h in hits:
                        pid = h.get("project_id") or h.get("slug")
                        results.append({
                            "projectId": pid,
                            "project_id": pid,
                            "slug": h.get("slug", ""),
                            "title": h.get("title", h.get("slug", "Plugin")),
                            "description": h.get("description", ""),
                            "author": h.get("author", "Modrinth"),
                            "downloads": h.get("downloads", 0),
                            "icon_url": h.get("icon_url", ""),
                        })
                    if results:
                        return results
            except Exception:
                pass
        if not clean_q:
            return list(self.CURATED_PLUGINS)
        q_low = clean_q.lower()
        return [
            p for p in self.CURATED_PLUGINS
            if q_low in p["title"].lower() or q_low in p["description"].lower() or q_low in p["slug"].lower()
        ]

    def install_modrinth_plugin(self, project_id: str, title: str = "") -> Dict[str, Any]:
        clean_id = re.sub(r"[^a-zA-Z0-9_-]", "", project_id.strip())
        if not clean_id:
            raise ValueError("Plugin project ID or slug is required.")
        clean_title = re.sub(r"[^a-zA-Z0-9_-]", "", title.strip()) or clean_id
        self._ensure_idle_mutable_sync()
        plugins_dir = self.runtime_dir / "plugins"
        plugins_dir.mkdir(parents=True, exist_ok=True)

        if os.environ.get("MINERELAY_TEST_MODE") == "1":
            dummy_jar = plugins_dir / f"{clean_title}.jar"
            dummy_jar.write_bytes(b"PK\x03\x04test-plugin-jar")
            self._push_idle_config_safely()
            return {"installed": True, "filename": dummy_jar.name, "plugins": self.list_plugins()}

        url = f"https://api.modrinth.com/v2/project/{urlquote(clean_id)}/version"
        req = urllib.request.Request(url, headers={"User-Agent": "MineRelay/1.0 (github.com/minerelay)"})
        try:
            with urllib.request.urlopen(req, timeout=8.0) as resp:
                versions = json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            raise RuntimeError(f"Failed to query Modrinth plugin versions: {e}")

        if not isinstance(versions, list) or not versions:
            raise RuntimeError(f"No downloadable builds found on Modrinth for '{clean_id}'.")

        cfg = self.sync_engine.load_cluster_config()
        mc_ver = cfg.mc_version if cfg else "1.21.4"

        chosen_file = None
        # Prefer version matching mc_ver and paper/spigot/bukkit loader
        for v in versions:
            loaders = [str(l).lower() for l in v.get("loaders", [])]
            gvers = [str(g) for g in v.get("game_versions", [])]
            if any(l in loaders for l in ("paper", "spigot", "bukkit", "purpur")) and mc_ver in gvers:
                for f in v.get("files", []):
                    if str(f.get("filename", "")).endswith(".jar"):
                        chosen_file = f
                        break
            if chosen_file:
                break

        # Fallback to latest paper/spigot/bukkit build regardless of exact MC version tag
        if not chosen_file:
            for v in versions:
                for f in v.get("files", []):
                    if str(f.get("filename", "")).endswith(".jar"):
                        chosen_file = f
                        break
                if chosen_file:
                    break

        if not chosen_file or not chosen_file.get("url"):
            raise RuntimeError(f"No valid .jar file found for plugin '{clean_id}'.")

        filename = Path(str(chosen_file.get("filename", f"{clean_id}.jar"))).name
        dl_url = str(chosen_file["url"])
        dest_path = plugins_dir / filename

        self._log(f"Downloading plugin '{filename}' from Modrinth...", "INFO", "Plugins")
        dl_req = urllib.request.Request(dl_url, headers={"User-Agent": "MineRelay/1.0"})
        with urllib.request.urlopen(dl_req, timeout=30.0) as dl_resp:
            data = dl_resp.read()
            dest_path.write_bytes(data)

        self._push_idle_config_safely()
        self._log(f"Installed plugin '{filename}' ({round(len(data)/1024, 1)} KB) into plugins/!", "INFO", "Plugins")
        return {"installed": True, "filename": filename, "sizeBytes": len(data), "plugins": self.list_plugins()}

    # -------------------------------------------------------------------------
    # Status Summary
    # -------------------------------------------------------------------------

    def get_effective_logs(self, limit: int = 120) -> List[Dict[str, Any]]:
        lock = self.sync_engine.get_lock()
        is_local = self.sync_engine.is_locked_by_current_machine(lock) or self.local_status != "idle"
        if not is_local and lock.is_actively_locked():
            remote_tel = self.sync_engine.read_host_telemetry(expected_epoch=lock.lock_epoch)
            if remote_tel and isinstance(remote_tel.get("logs"), list):
                return list(remote_tel["logs"][-limit:])
            return []
        with self._mutex:
            return list(self.console_logs[-limit:])

    def get_fast_players_snapshot(self) -> Dict[str, Any]:
        """Lightweight online player snapshot without running Java/JAR/disk diagnostics."""
        with self._mutex:
            local_running = self.local_status == "running"
            local_players = list(self.online_players) if local_running else None
            local_max = self._active_max_players
        if local_players is not None:
            return {
                "online": local_players,
                "count": len(local_players),
                "max": local_max or self.sync_engine.get_lock().max_players or 20,
            }
        lock = self.sync_engine.get_lock()
        is_local_host = self.sync_engine.is_locked_by_current_machine(lock)
        if not is_local_host and lock.is_actively_locked():
            remote_tel = self.sync_engine.read_host_telemetry(expected_epoch=lock.lock_epoch)
            if remote_tel and isinstance(remote_tel.get("onlinePlayers"), list):
                online_list = [str(p) for p in remote_tel["onlinePlayers"] if p]
            else:
                online_list = list(lock.online_players)
            max_p = int((remote_tel.get("maxPlayers") if remote_tel and remote_tel.get("maxPlayers") else lock.max_players) or 20)
        else:
            online_list = list(lock.online_players) if lock.is_actively_locked() else []
            max_p = lock.max_players or 20
        return {
            "online": online_list,
            "count": len(online_list),
            "max": max_p,
        }

    def get_status_summary(self) -> Dict[str, Any]:
        cfg = self.sync_engine.load_cluster_config()
        lock = self.sync_engine.get_lock()
        is_local_host = self.sync_engine.is_locked_by_current_machine(lock)
        remote_hosting = (not is_local_host) and (self.local_status == "idle") and lock.is_actively_locked()
        remote_tel: Optional[Dict[str, Any]] = None
        if remote_hosting:
            remote_tel = self.sync_engine.read_host_telemetry(expected_epoch=lock.lock_epoch)

        with self._mutex:
            if is_local_host and self.local_status == "running":
                lock.online_players = list(self.online_players)
            if self.local_status == "running":
                online_list = list(self.online_players)
            elif remote_hosting and remote_tel and isinstance(remote_tel.get("onlinePlayers"), list):
                online_list = [str(p) for p in remote_tel["onlinePlayers"] if p]
            else:
                online_list = list(lock.online_players) if lock.is_actively_locked() else []

        mc_ver = cfg.mc_version if cfg else "26.3"
        java_info = self.check_java_installed(cfg.java_path if cfg else "java", mc_ver)
        active_jar = self.get_active_jar_path()
        has_jar = active_jar.exists() and active_jar.stat().st_size > 1_000_000

        cluster_dict = cfg.to_dict(include_secrets=False) if cfg else None
        lock_dict = lock.to_dict()

        if remote_hosting:
            effective_status = str((remote_tel.get("status") if remote_tel and remote_tel.get("status") else lock.status) or "idle")
            effective_runtime_mode = str((remote_tel.get("runtimeMode") if remote_tel and remote_tel.get("runtimeMode") else self.runtime_mode) or self.runtime_mode)
            effective_last_error = str((remote_tel.get("lastError") if remote_tel and remote_tel.get("lastError") else "") or self.last_error or "")
            tel_max_players = int((remote_tel.get("maxPlayers") if remote_tel and remote_tel.get("maxPlayers") else lock.max_players) or 20)

            uptime_sec = 0
            if effective_status == "running":
                if remote_tel and remote_tel.get("uptimeSeconds") is not None:
                    base_up = float(remote_tel.get("uptimeSeconds") or 0.0)
                    tel_upd = float(remote_tel.get("updatedAt") or 0.0)
                    age = (time.time() - tel_upd) if tel_upd > 0 else 0.0
                    uptime_sec = int(max(0.0, base_up + age)) if 0.0 <= age <= 30.0 else int(max(0.0, base_up))
                else:
                    ref_start = (
                        float(remote_tel.get("serverReadyAt") or 0.0) if remote_tel else 0.0
                    ) or lock.server_ready_at or lock.started_at
                    uptime_sec = int(max(0.0, time.time() - ref_start)) if ref_start else 0

            tel_tps = float((remote_tel.get("tps") if remote_tel and remote_tel.get("tps") is not None else lock.tps) or 0.0)
            if effective_status == "running" and tel_tps <= 0.0:
                tel_tps = 20.0
            elif effective_status != "running":
                tel_tps = 0.0
            tel_mem_used = int((remote_tel.get("memoryUsedMb") if remote_tel and remote_tel.get("memoryUsedMb") is not None else lock.memory_used_mb) or 0)
            tel_mem_max = int((remote_tel.get("memoryMaxMb") if remote_tel and remote_tel.get("memoryMaxMb") else lock.memory_max_mb) or (cfg.memory_max_mb if cfg else 4096))
            tel_cpu = round(float((remote_tel.get("cpuPercent") if remote_tel and remote_tel.get("cpuPercent") is not None else lock.cpu_percent) or 0.0), 1)

            tunnel_state = dict(self.tunnel_manager.get_state())
            tunnel_state["status"] = str(
                (remote_tel.get("tunnelStatus") if remote_tel and remote_tel.get("tunnelStatus") else lock.tunnel_status)
                or "stopped"
            )
            tunnel_state["publicAddress"] = str(
                (remote_tel.get("publicAddress") if remote_tel else "") or lock.public_address or ""
            )
            tunnel_state["claimUrl"] = ""
            tunnel_state["logs"] = (
                list(remote_tel["tunnelLogs"][-40:])
                if remote_tel and isinstance(remote_tel.get("tunnelLogs"), list)
                else []
            )
            remote_lan_ip = (
                str(remote_tel.get("hostLanIp") or remote_tel.get("tunnelLanIp") or "")
                if remote_tel
                else ""
            )
            tunnel_state["lanIp"] = remote_lan_ip
            tunnel_state["hostLanIp"] = remote_lan_ip
            tunnel_state["error"] = str(remote_tel.get("tunnelError") or "") if remote_tel else ""

            world_info = (
                remote_tel.get("worldInfo")
                if remote_tel and isinstance(remote_tel.get("worldInfo"), dict)
                else self._get_cached_world_info()
            )
            auto_sync_info = (
                remote_tel.get("autoSync")
                if remote_tel and isinstance(remote_tel.get("autoSync"), dict)
                else self.get_auto_sync_status(cfg=cfg)
            )
            if cluster_dict and remote_tel:
                if remote_tel.get("rawMcVersion"):
                    cluster_dict["mcVersion"] = str(remote_tel["rawMcVersion"])
                if remote_tel.get("mcSoftware"):
                    cluster_dict["mcSoftware"] = str(remote_tel["mcSoftware"])
                if remote_tel.get("serverName"):
                    cluster_dict["serverName"] = str(remote_tel["serverName"])

            lock_dict["status"] = effective_status
            lock_dict["onlinePlayers"] = online_list
            lock_dict["maxPlayers"] = tel_max_players
            lock_dict["tps"] = tel_tps
            lock_dict["memoryUsedMb"] = tel_mem_used
            lock_dict["memoryMaxMb"] = tel_mem_max
            lock_dict["cpuPercent"] = tel_cpu
            lock_dict["tunnelStatus"] = tunnel_state["status"]
            lock_dict["publicAddress"] = tunnel_state.get("publicAddress", "")
            lock_dict["hostLanIp"] = remote_lan_ip
        else:
            self._sample_local_process_metrics()
            effective_runtime_mode = self.runtime_mode
            effective_last_error = self.last_error
            ref_start = self.server_ready_at or lock.server_ready_at
            uptime_sec = int(max(0.0, time.time() - ref_start)) if (self.local_status == "running" and ref_start) else 0
            tel_tps = self.tps if self.local_status == "running" else 0.0
            tel_mem_used = self.memory_used_mb if self.local_status in ("running", "starting", "stopping", "restarting") else 0
            tel_mem_max = cfg.memory_max_mb if cfg else 4096
            tel_cpu = self.cpu_percent if self.local_status in ("running", "starting", "stopping", "restarting") else 0.0
            tel_max_players = self._active_max_players or (lock.max_players if lock.is_actively_locked() else 20) or 20
            tunnel_state = dict(self.tunnel_manager.get_state())
            local_lan_ip = str(tunnel_state.get("lanIp") or "")
            tunnel_state["hostLanIp"] = local_lan_ip
            world_info = self._get_cached_world_info()
            auto_sync_info = self.get_auto_sync_status(cfg=cfg)

            if self.local_status != "idle":
                lock_dict["isLocked"] = True
                lock_dict["isActivelyLocked"] = True
                lock_dict["machineId"] = self.sync_engine.local_config.machine_id
                lock_dict["machineName"] = self.sync_engine.local_config.machine_name
                lock_dict["status"] = self.local_status
                lock_dict["onlinePlayers"] = online_list
                lock_dict["maxPlayers"] = tel_max_players
                lock_dict["tps"] = tel_tps
                lock_dict["memoryUsedMb"] = tel_mem_used
                lock_dict["memoryMaxMb"] = tel_mem_max
                lock_dict["cpuPercent"] = tel_cpu
                lock_dict["tunnelStatus"] = tunnel_state.get("status", "stopped")
                lock_dict["publicAddress"] = tunnel_state.get("publicAddress", "")
                lock_dict["hostLanIp"] = local_lan_ip
                if self.server_ready_at:
                    lock_dict["serverReadyAt"] = self.server_ready_at
                if self.active_host_user:
                    lock_dict["hostUsername"] = self.active_host_user.username
                    lock_dict["hostUserId"] = self.active_host_user.id

        return {
            "localStatus": self.local_status,
            "runtimeMode": effective_runtime_mode,
            "lastError": effective_last_error,
            "isLocalMachineHosting": is_local_host or (self.local_status != "idle"),
            "onlinePlayers": online_list,
            "maxPlayers": tel_max_players,
            "lock": lock_dict,
            "tunnel": tunnel_state,
            "telemetry": {
                "tps": tel_tps,
                "memoryUsedMb": tel_mem_used,
                "memoryMaxMb": tel_mem_max,
                "cpuPercent": tel_cpu,
                "uptimeSeconds": uptime_sec,
            },
            "world": world_info,
            "autoSync": auto_sync_info,
            "environment": {
                "javaInstalled": java_info["installed"],
                "javaVersion": java_info["version"],
                "javaMajor": java_info["major"],
                "requiredJavaMajor": java_info["requiredMajor"],
                "serverJarInstalled": has_jar,
                "serverJarName": active_jar.name if has_jar else "None",
                "serverJarSizeMb": round(active_jar.stat().st_size / (1024 * 1024), 2) if has_jar else 0,
                "installedJars": list_installed_jars(self.bin_dir),
                "jarDownloading": self.jar_downloading,
                "jarDownloadProgress": self.jar_download_progress,
                "jarDownloadStatus": self.jar_download_status,
                "machineId": self.sync_engine.local_config.machine_id,
                "machineName": self.sync_engine.local_config.machine_name,
                "sharedSyncDir": str(self.sync_engine.shared_dir),
                "syncDiagnostics": self.sync_engine.get_sync_diagnostics(),
            },
            "cluster": cluster_dict,
        }
