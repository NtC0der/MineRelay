import os
import sys
import json
import time
import queue
import inspect
import mimetypes
import threading
import webbrowser
from pathlib import Path
from urllib.parse import urlparse, parse_qs, unquote
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from typing import Optional, Dict, Any, List

# Ensure UTF-8 console output on Windows
try:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

IS_FROZEN = getattr(sys, "frozen", False)
if IS_FROZEN:
    BUNDLE_DIR = Path(getattr(sys, "_MEIPASS", Path(sys.executable).resolve().parent))
    EXE_DIR = Path(sys.executable).resolve().parent
    # If running from dist/ inside the project repo, share the repo's data/ folder if it exists;
    # otherwise store data/ alongside MineRelay.exe (or in %LOCALAPPDATA%/MineRelay/data if read-only)
    if (EXE_DIR / "data").exists():
        ROOT_DIR = EXE_DIR
    elif EXE_DIR.name.lower() == "dist" and (EXE_DIR.parent / "data").exists():
        ROOT_DIR = EXE_DIR.parent
    else:
        ROOT_DIR = EXE_DIR
else:
    BUNDLE_DIR = Path(__file__).resolve().parent.parent
    ROOT_DIR = BUNDLE_DIR

if str(BUNDLE_DIR) not in sys.path:
    sys.path.insert(0, str(BUNDLE_DIR))

from src.models import AUTO_SYNC_DEFAULT_MINUTES, AUTO_SYNC_MAX_MINUTES, _safe_bool
from src.bootstrap_runtime import AutoUpdateManager, check_latest_minerelay_release, is_version_older
from src.sync_engine import SyncEngine, HostLockConflictError
from src.auth_service import AuthService, AuthError, PermissionDeniedError
from src.tunnel_manager import TunnelManager
from src.minecraft_manager import MinecraftManager


def _resolve_default_data_dir() -> str:
    candidate = ROOT_DIR / "data"
    try:
        candidate.mkdir(parents=True, exist_ok=True)
        test_file = candidate / ".write_check"
        test_file.write_text("ok", encoding="utf-8")
        test_file.unlink(missing_ok=True)
        return str(candidate)
    except Exception:
        local_app_data = os.environ.get("LOCALAPPDATA") or str(Path.home() / "AppData" / "Local")
        fallback = Path(local_app_data) / "MineRelay" / "data"
        fallback.mkdir(parents=True, exist_ok=True)
        return str(fallback)


class AppContext:
    def __init__(self, data_dir: Optional[str] = None):
        base_data = data_dir or _resolve_default_data_dir()
        self.sse_clients: List[queue.Queue] = []
        self.sse_mutex = threading.Lock()
        self._cluster_version_recorded: str = ""

        self.sync_engine = SyncEngine(base_data, event_callback=self._on_engine_event)
        self.auth_service = AuthService(self.sync_engine)
        self.tunnel_manager = TunnelManager(self.sync_engine, event_callback=self.broadcast_event)
        self.mc_manager = MinecraftManager(
            self.sync_engine,
            self.tunnel_manager,
            event_callback=self.broadcast_event,
        )
        self.auto_updater = AutoUpdateManager(Path(base_data), event_callback=self._on_updater_event)

        if os.environ.get("MINERELAY_TEST_MODE") != "1":
            threading.Thread(
                target=self._remote_lock_watcher_loop,
                daemon=True,
                name="RemoteLockWatcher",
            ).start()
            threading.Thread(
                target=lambda: self.get_version_check(force=True, trigger_download=True),
                daemon=True,
                name="InitialReleaseVersionCheck",
            ).start()

    def _on_engine_event(self, event_type: str, payload: Any) -> None:
        self.broadcast_event(event_type, payload)
        if event_type == "lock_lost":
            try:
                self.mc_manager.handle_lock_lost(payload if isinstance(payload, dict) else {})
            except Exception:
                pass

    def _enrich_auto_update_state(self, au_state: Dict[str, Any]) -> Dict[str, Any]:
        enriched = dict(au_state or {})
        enriched["canRestartApply"] = bool(IS_FROZEN and sys.platform.startswith("win"))
        enriched["canApplyNow"] = bool(
            self.mc_manager.local_status == "idle" and not self.sync_engine.is_locked_by_current_machine()
        )
        return enriched

    def _on_updater_event(self, event_type: str, payload: Any) -> None:
        if event_type == "update_status" and isinstance(payload, dict):
            payload = self._enrich_auto_update_state(payload)
        self.broadcast_event(event_type, payload)

    def get_version_check(self, force: bool = False, trigger_download: bool = True) -> Dict[str, Any]:
        cfg = None
        try:
            cfg = self.sync_engine.load_cluster_config()
        except Exception:
            cfg = None
        cluster_latest = cfg.latest_app_version if cfg else ""
        vc = check_latest_minerelay_release(force=force, cluster_latest=cluster_latest)
        cur_ver = str(vc.get("currentVersion") or "")
        if (
            cfg is not None
            and cur_ver
            and self._cluster_version_recorded != cur_ver
            and (not cluster_latest or is_version_older(cluster_latest, cur_ver))
            and os.environ.get("MINERELAY_TEST_MODE") != "1"
        ):
            try:
                if not self.sync_engine._has_active_cloud_transfer():
                    with self.auth_service._config_mutex:
                        fresh = self.sync_engine.load_cluster_config()
                        if fresh and (not fresh.latest_app_version or is_version_older(fresh.latest_app_version, cur_ver)):
                            fresh.latest_app_version = cur_ver
                            self.sync_engine.save_cluster_config(fresh)
                    self._cluster_version_recorded = cur_ver
            except Exception:
                pass
        if trigger_download and vc.get("isOutdated") and self.auto_updater.is_auto_download_allowed():
            try:
                self.auto_updater.ensure_update_downloaded_async(vc, force=False)
            except Exception:
                pass
        vc["autoUpdate"] = self._enrich_auto_update_state(self.auto_updater.get_state())
        return vc

    def _remote_lock_watcher_loop(self) -> None:
        """Polls shared host lock & remote telemetry state every 3s and pushes instant SSE updates to dashboards."""
        last_sig: Any = None
        last_prog_sig: Any = None
        poll_sleep: float = 3.0
        last_remote_epoch: str = ""
        seen_remote_log_ids: set = set()
        last_remote_log_count: int = 0
        last_remote_tail_id: str = ""

        def _file_sig(path: Path) -> Any:
            try:
                if path.exists():
                    st = path.stat()
                    return (st.st_mtime_ns, st.st_size)
            except OSError:
                pass
            return None

        def _backups_sig(bdir: Path) -> Any:
            try:
                if not bdir.exists():
                    return ()
                items = []
                with os.scandir(bdir) as it:
                    for entry in it:
                        if entry.name.endswith(".zip"):
                            st = entry.stat()
                            items.append((entry.name, st.st_mtime_ns, st.st_size))
                items.sort()
                return tuple(items)
            except OSError:
                return None

        while True:
            time.sleep(poll_sleep)
            try:
                with self.sse_mutex:
                    has_clients = len(self.sse_clients) > 0
                lock = self.sync_engine.get_lock()
                remote_tel = None
                remote_epoch_changed = False
                logs_reset = False
                unseen_logs: List[Dict[str, Any]] = []
                is_remote_active = lock.is_actively_locked() and not self.sync_engine.is_locked_by_current_machine(lock)

                if is_remote_active:
                    cur_epoch = lock.lock_epoch or f"legacy-{lock.machine_id}-{int(lock.started_at)}"
                    if cur_epoch != last_remote_epoch:
                        seen_remote_log_ids.clear()
                        remote_epoch_changed = True
                        last_remote_epoch = cur_epoch
                        last_remote_log_count = 0
                        last_remote_tail_id = ""
                    remote_tel = self.sync_engine.read_host_telemetry(expected_epoch=lock.lock_epoch)
                    if remote_tel and isinstance(remote_tel.get("logs"), list):
                        tel_logs = remote_tel["logs"][-120:]
                        tel_ids = [
                            str(e.get("id")) for e in tel_logs if isinstance(e, dict) and e.get("id")
                        ]
                        cur_tail_id = tel_ids[-1] if tel_ids else ""
                        if not remote_epoch_changed and last_remote_tail_id and (
                            last_remote_tail_id not in tel_ids or len(tel_logs) < last_remote_log_count
                        ):
                            logs_reset = True
                            seen_remote_log_ids.clear()
                        for entry in tel_logs:
                            eid = entry.get("id") if isinstance(entry, dict) else None
                            if eid and eid not in seen_remote_log_ids:
                                seen_remote_log_ids.add(eid)
                                unseen_logs.append(entry)
                        last_remote_log_count = len(tel_logs)
                        last_remote_tail_id = cur_tail_id
                        if len(seen_remote_log_ids) > 600:
                            seen_remote_log_ids = set(tel_ids)
                elif not lock.is_actively_locked():
                    seen_remote_log_ids.clear()
                    last_remote_epoch = ""
                    last_remote_log_count = 0
                    last_remote_tail_id = ""

                tel_auto = (remote_tel or {}).get("autoSync") if isinstance(remote_tel, dict) else None
                local_prog = self.sync_engine.get_sync_progress()
                tel_prog = (remote_tel or {}).get("syncProgress") if isinstance(remote_tel, dict) else None
                eff_prog = local_prog if local_prog.get("active") else (tel_prog if isinstance(tel_prog, dict) else local_prog)
                prog_sig = (
                    bool(eff_prog.get("active")),
                    str(eff_prog.get("operation") or ""),
                    str(eff_prog.get("phase") or ""),
                    int(int(eff_prog.get("percent", 0) or 0) // 2),
                    bool(eff_prog.get("indeterminate")),
                    int(eff_prog.get("totalFiles") or 0),
                )
                sig = (
                    lock.is_locked,
                    lock.is_stale(),
                    lock.host_username,
                    lock.host_user_id,
                    lock.machine_id,
                    lock.machine_name,
                    lock.status,
                    lock.lock_epoch,
                    lock.started_at,
                    lock.server_ready_at,
                    tuple(lock.online_players),
                    lock.max_players,
                    lock.mc_version,
                    lock.public_address,
                    lock.tunnel_status,
                    lock.memory_used_mb,
                    lock.memory_max_mb,
                    lock.cpu_percent,
                    lock.tps,
                    (remote_tel or {}).get("updatedAt") if remote_tel else None,
                    (remote_tel or {}).get("status") if remote_tel else None,
                    tuple((remote_tel or {}).get("onlinePlayers") or ()) if isinstance(remote_tel, dict) else (),
                    (remote_tel or {}).get("publicAddress") if remote_tel else None,
                    (remote_tel or {}).get("tunnelStatus") if remote_tel else None,
                    (remote_tel or {}).get("lastError") if remote_tel else None,
                    (tel_auto.get("inProgress"), tel_auto.get("lastSyncAt"), tel_auto.get("lastSyncStatus"), tel_auto.get("syncCount"))
                    if isinstance(tel_auto, dict)
                    else None,
                    prog_sig,
                    _file_sig(self.sync_engine.cluster_config_path),
                    _file_sig(self.sync_engine.sync_manifest_path),
                    _backups_sig(self.sync_engine.backups_dir),
                    _backups_sig(self.sync_engine.local_backups_dir),
                    self.mc_manager.local_status,
                    self.mc_manager.jar_downloading,
                    self.mc_manager.jar_download_progress,
                    self.tunnel_manager.status,
                    self.tunnel_manager.public_address,
                    self.tunnel_manager.is_downloading,
                    self.tunnel_manager.download_progress,
                    json.dumps(remote_tel.get("worldInfo"), sort_keys=True, default=str)
                    if isinstance(remote_tel, dict) and isinstance(remote_tel.get("worldInfo"), dict)
                    else None,
                    getattr(self.mc_manager, "_startup_phase", ""),
                    getattr(self.mc_manager, "_startup_detail", ""),
                    getattr(self.mc_manager, "_startup_progress", 0),
                )
                vc_now = self.get_version_check(force=False, trigger_download=True)
                au_now = vc_now.get("autoUpdate") or {}
                sig = sig + (
                    (
                        vc_now.get("latestVersion"),
                        vc_now.get("isOutdated"),
                        au_now.get("status"),
                        au_now.get("targetVersion"),
                    ),
                )

                if has_clients:
                    if last_prog_sig is not None and prog_sig != last_prog_sig:
                        self.broadcast_event("sync_progress", dict(eff_prog))
                    should_emit_logs = (last_sig is not None) and (not remote_epoch_changed) and (not logs_reset)
                    if should_emit_logs and 0 < len(unseen_logs) <= 25:
                        for entry in unseen_logs:
                            self.broadcast_event("console_log", entry)

                    need_status_push = (
                        (last_sig is not None and (sig != last_sig or remote_epoch_changed or logs_reset))
                        or (should_emit_logs and len(unseen_logs) > 25)
                    )
                    if need_status_push:
                        summary = self.mc_manager.get_status_summary()
                        summary["versionCheck"] = vc_now
                        if remote_epoch_changed or logs_reset or len(unseen_logs) > 25:
                            summary["logs"] = self.mc_manager.get_effective_logs(120)
                        self.broadcast_event("status_changed", summary)
                last_sig = sig
                last_prog_sig = prog_sig
                poll_sleep = (
                    1.0
                    if (
                        bool(eff_prog.get("active"))
                        or self.mc_manager.local_status in ("syncing_pull", "preparing", "syncing_push", "stopping")
                    )
                    else 3.0
                )
            except Exception:
                poll_sleep = 3.0

    def broadcast_event(self, event_type: str, payload: Any) -> None:
        if event_type == "status_changed" and isinstance(payload, dict) and "versionCheck" not in payload:
            try:
                payload = dict(payload)
                payload["versionCheck"] = self.get_version_check(force=False)
            except Exception:
                pass
        try:
            msg = json.dumps({"event": event_type, "data": payload}, default=str)
        except Exception:
            return
        with self.sse_mutex:
            stale: List[queue.Queue] = []
            for q in self.sse_clients:
                try:
                    q.put_nowait(msg)
                except queue.Full:
                    stale.append(q)
            for s in stale:
                if s in self.sse_clients:
                    self.sse_clients.remove(s)


def _ensure_no_remote_lock(ctx: AppContext, action_desc: str = "perform this action") -> None:
    lock = ctx.sync_engine.get_lock()
    if lock.is_actively_locked() and not ctx.sync_engine.is_locked_by_current_machine(lock):
        raise HostLockConflictError(
            f"Cannot {action_desc} while '{lock.host_username}' ({lock.machine_name}) is actively hosting."
        )


def _sync_local_lock_machine_name(ctx: AppContext, machine_name: str) -> None:
    if not machine_name:
        return
    try:
        with ctx.sync_engine._lock_mutex:
            lock = ctx.sync_engine.get_lock()
            if lock.is_actively_locked() and ctx.sync_engine.is_locked_by_current_machine(lock):
                if lock.machine_name != machine_name:
                    lock.machine_name = machine_name
                    ctx.sync_engine.save_lock(lock, fsync=True, include_session=True)
    except Exception:
        pass


def create_request_handler(ctx: AppContext, public_dir: Path):
    class DashboardRequestHandler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, format: str, *args: Any) -> None:
            # Keep console output clean
            pass

        def _get_token(self, parsed_url: Any) -> Optional[str]:
            auth_header = self.headers.get("Authorization", "")
            if auth_header.startswith("Bearer "):
                return auth_header[7:].strip()
            qs = parse_qs(parsed_url.query)
            if "token" in qs and qs["token"]:
                return qs["token"][0]
            return None

        def _require_int(self, val: Any, field_name: str) -> int:
            if val is None or isinstance(val, (bool, float)):
                raise ValueError(f"Field '{field_name}' must be an integer, got {type(val).__name__}.")
            if isinstance(val, str):
                s = val.strip()
                if not s or not (s.isdigit() or (s[0] in "+-" and s[1:].isdigit())):
                    raise ValueError(f"Field '{field_name}' must be an integer.")
            try:
                return int(val)
            except (ValueError, TypeError):
                raise ValueError(f"Field '{field_name}' must be an integer.")

        def _read_json_body(self) -> Dict[str, Any]:
            try:
                length = int(self.headers.get("Content-Length", "0"))
            except (ValueError, TypeError):
                raise ValueError("Invalid Content-Length header.")
            if length <= 0:
                return {}
            if length > 50 * 1024 * 1024:
                raise ValueError("Request body exceeds maximum size limit (50 MB).")
            raw = self.rfile.read(length)
            try:
                parsed = json.loads(raw.decode("utf-8"))
            except Exception as e:
                raise ValueError(f"Malformed JSON body: {e}")
            if not isinstance(parsed, dict):
                raise ValueError("JSON body must be an object.")
            return parsed

        def _send_json(self, status_code: int, data: Any) -> None:
            try:
                body = json.dumps(data, default=str).encode("utf-8")
                self.send_response(status_code)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
                self.wfile.write(body)
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
                self.close_connection = True

        def _handle_exception(self, e: Exception) -> None:
            if isinstance(e, AuthError):
                self._send_json(401, {"error": str(e), "code": "UNAUTHORIZED"})
            elif isinstance(e, PermissionDeniedError):
                self._send_json(403, {"error": str(e), "code": "FORBIDDEN"})
            elif isinstance(e, PermissionError):
                if getattr(e, "winerror", None) in (5, 32, 33) or "being used by another process" in str(e).lower():
                    self._send_json(409, {"error": f"File is currently in use by the running server: {e}", "code": "FILE_IN_USE"})
                else:
                    self._send_json(403, {"error": str(e), "code": "FORBIDDEN"})
            elif isinstance(e, HostLockConflictError):
                self._send_json(409, {"error": str(e), "code": "HOST_LOCK_CONFLICT", "lock": ctx.sync_engine.get_lock().to_dict()})
            elif isinstance(e, FileNotFoundError):
                self._send_json(404, {"error": str(e), "code": "NOT_FOUND"})
            elif isinstance(e, (ValueError, RuntimeError, OSError, TypeError)):
                self._send_json(400, {"error": str(e), "code": "BAD_REQUEST"})
            else:
                self._send_json(500, {"error": str(e), "code": "INTERNAL_ERROR"})

        def _validate_local_request(self) -> bool:
            allowed = {"localhost", "127.0.0.1", "::1"}
            host_hdr = (self.headers.get("Host") or "").strip()
            if host_hdr:
                if host_hdr.startswith("["):
                    end_idx = host_hdr.find("]")
                    host_name = host_hdr[1:end_idx].lower() if end_idx != -1 else host_hdr.lower()
                else:
                    host_name = host_hdr.split(":", 1)[0].lower()
                if host_name not in allowed:
                    self._send_json(403, {"error": "Forbidden Host header.", "code": "FORBIDDEN"})
                    return False
            origin_hdr = (self.headers.get("Origin") or "").strip()
            if origin_hdr:
                try:
                    org_host = (urlparse(origin_hdr).hostname or "").lower()
                except Exception:
                    org_host = ""
                if org_host not in allowed:
                    self._send_json(403, {"error": "Forbidden Origin header.", "code": "FORBIDDEN"})
                    return False
            return True

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            path = parsed.path
            qs = parse_qs(parsed.query)

            if not path.startswith("/api/"):
                return self._serve_static(path)

            if not self._validate_local_request():
                return

            try:
                token = self._get_token(parsed)

                if path == "/api/app/update/file":
                    ready = ctx.auto_updater.get_ready_update_path()
                    if ready is None or not ready.exists() or not ready.is_file():
                        raise FileNotFoundError("No verified update installer is ready yet.")
                    au_state = ctx.auto_updater.get_state()
                    ver_tag = str(au_state.get("targetVersion") or "latest").strip()
                    safe_ver = "".join(c for c in ver_tag if c.isalnum() or c in (".", "-", "_")) or "latest"
                    st = ready.stat()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/octet-stream")
                    self.send_header("Content-Disposition", f'attachment; filename="MineRelay-v{safe_ver}.exe"')
                    self.send_header("Content-Length", str(st.st_size))
                    self.send_header("Cache-Control", "no-store")
                    self.end_headers()
                    try:
                        with open(ready, "rb") as uf:
                            while True:
                                chunk = uf.read(1024 * 1024)
                                if not chunk:
                                    break
                                self.wfile.write(chunk)
                    except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
                        self.close_connection = True
                    return

                if path == "/api/setup/status":
                    cfg = ctx.sync_engine.load_cluster_config()
                    vc = ctx.get_version_check(force=False)
                    self._send_json(200, {
                        "initialized": ctx.sync_engine.is_cluster_initialized(),
                        "sharedSyncDir": str(ctx.sync_engine.shared_dir),
                        "machineId": ctx.sync_engine.local_config.machine_id,
                        "machineName": ctx.sync_engine.local_config.machine_name,
                        "cloudFolders": ctx.sync_engine.detect_cloud_folders(),
                        "syncDiagnostics": ctx.sync_engine.get_sync_diagnostics(),
                        "syncProgress": ctx.sync_engine.get_sync_progress(),
                        "serverName": cfg.server_name if cfg else "Shared Minecraft Server",
                        "autoSyncIntervalMinutes": cfg.auto_sync_interval_minutes if cfg else AUTO_SYNC_DEFAULT_MINUTES,
                        "versionCheck": vc,
                    })
                    return

                if path == "/api/auth/me":
                    user = ctx.auth_service.require_auth(token)
                    vc = ctx.get_version_check(force=False)
                    status_summary = ctx.mc_manager.get_status_summary()
                    status_summary["versionCheck"] = vc
                    self._send_json(200, {
                        "account": user.to_public_dict(),
                        "status": status_summary,
                        "versionCheck": vc,
                    })
                    return

                if path == "/api/server/status":
                    user = ctx.auth_service.require_auth(token)
                    summary = ctx.mc_manager.get_status_summary()
                    summary["currentUser"] = user.to_public_dict()
                    summary["logs"] = ctx.mc_manager.get_effective_logs(120)
                    summary["versionCheck"] = ctx.get_version_check(force=False)
                    self._send_json(200, summary)
                    return

                if path == "/api/players":
                    ctx.auth_service.require_auth(token)
                    self._send_json(200, ctx.mc_manager.get_fast_players_snapshot())
                    return

                if path == "/api/events":
                    user = ctx.auth_service.require_auth(token)
                    self._handle_sse(user, token=token)
                    return

                # ---------------- ADMIN ONLY GET ROUTES ----------------
                if path == "/api/admin/settings":
                    ctx.auth_service.require_admin(token)
                    cfg = ctx.sync_engine.load_cluster_config()
                    if not cfg:
                        raise ValueError("Cluster not initialized.")
                    auto_sync_info = None
                    lock = ctx.sync_engine.get_lock()
                    if (
                        ctx.mc_manager.local_status == "idle"
                        and lock.is_actively_locked()
                        and not ctx.sync_engine.is_locked_by_current_machine(lock)
                    ):
                        remote_tel = ctx.sync_engine.read_host_telemetry(expected_epoch=lock.lock_epoch)
                        if remote_tel and isinstance(remote_tel.get("autoSync"), dict):
                            auto_sync_info = remote_tel["autoSync"]
                    if auto_sync_info is None:
                        auto_sync_info = (
                            ctx.mc_manager.get_auto_sync_status()
                            if hasattr(ctx.mc_manager, "get_auto_sync_status")
                            else {
                                "enabled": cfg.auto_sync_interval_minutes > 0,
                                "intervalMinutes": cfg.auto_sync_interval_minutes,
                                "inProgress": False,
                                "lastSyncAt": 0.0,
                                "nextSyncAt": 0.0,
                                "lastSyncStatus": "idle",
                                "lastSyncError": "",
                            }
                        )
                    self._send_json(200, {
                        "cluster": cfg.to_dict(include_secrets=False),
                        "autoSync": auto_sync_info,
                    })
                    return

                if path == "/api/admin/accounts":
                    ctx.auth_service.require_admin(token)
                    cfg = ctx.sync_engine.load_cluster_config()
                    accounts = [a.to_public_dict() for a in (cfg.accounts if cfg else [])]
                    self._send_json(200, {"accounts": accounts})
                    return

                if path == "/api/admin/properties":
                    ctx.auth_service.require_admin(token)
                    props = ctx.mc_manager.get_server_properties()
                    lock = ctx.sync_engine.get_lock()
                    world_info = None
                    if (
                        ctx.mc_manager.local_status == "idle"
                        and lock.is_actively_locked()
                        and not ctx.sync_engine.is_locked_by_current_machine(lock)
                    ):
                        remote_tel = ctx.sync_engine.read_host_telemetry(expected_epoch=lock.lock_epoch)
                        if remote_tel and isinstance(remote_tel.get("worldInfo"), dict):
                            world_info = remote_tel["worldInfo"]
                    if world_info is None:
                        world_info = ctx.mc_manager.get_world_info()
                    self._send_json(200, {
                        "properties": props,
                        "world": world_info,
                    })
                    return

                if path == "/api/admin/world":
                    ctx.auth_service.require_admin(token)
                    lock = ctx.sync_engine.get_lock()
                    world_info = None
                    if (
                        ctx.mc_manager.local_status == "idle"
                        and lock.is_actively_locked()
                        and not ctx.sync_engine.is_locked_by_current_machine(lock)
                    ):
                        remote_tel = ctx.sync_engine.read_host_telemetry(expected_epoch=lock.lock_epoch)
                        if remote_tel and isinstance(remote_tel.get("worldInfo"), dict):
                            world_info = remote_tel["worldInfo"]
                    if world_info is None:
                        world_info = ctx.mc_manager.get_world_info()
                    self._send_json(200, {"world": world_info})
                    return

                if path == "/api/admin/versions":
                    ctx.auth_service.require_admin(token)
                    self._send_json(200, {"versions": ctx.mc_manager.get_available_versions()})
                    return

                if path == "/api/admin/players":
                    ctx.auth_service.require_admin(token)
                    self._send_json(200, ctx.mc_manager.get_players_data())
                    return

                if path == "/api/admin/backups":
                    ctx.auth_service.require_admin(token)
                    self._send_json(200, {"backups": ctx.sync_engine.list_backups()})
                    return

                if path == "/api/admin/backups/download":
                    ctx.auth_service.require_admin(token)
                    req_name = qs.get("name", [""])[0]
                    req_loc = qs.get("location", [""])[0].strip().lower()
                    if req_loc and req_loc not in ("local", "cloud"):
                        raise ValueError("Backup location must be 'local' or 'cloud'.")
                    if not req_name or "/" in req_name or "\\" in req_name or "\x00" in req_name or ".." in req_name:
                        raise ValueError("Invalid backup name.")
                    name = Path(req_name).name
                    if not name or name in (".", "..") or not name.lower().endswith(".zip"):
                        raise ValueError("Invalid backup name.")
                    if not all(c.isalnum() or c in (".", "-", "_", " ", "(", ")") for c in name):
                        raise ValueError("Invalid backup name.")
                    resolved = ctx.sync_engine.resolve_backup_path(name, location=req_loc)
                    if not resolved:
                        raise FileNotFoundError(f"Backup '{name}' not found.")
                    bk_path, _loc = resolved
                    if not bk_path.exists() or not bk_path.is_file():
                        raise FileNotFoundError(f"Backup '{name}' not found.")
                    st = bk_path.stat()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/zip")
                    self.send_header("Content-Disposition", f'attachment; filename="{name}"')
                    self.send_header("Content-Length", str(st.st_size))
                    self.end_headers()
                    try:
                        with open(bk_path, "rb") as bf:
                            while True:
                                chunk = bf.read(1024 * 1024)
                                if not chunk:
                                    break
                                self.wfile.write(chunk)
                    except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
                        self.close_connection = True
                    return

                if path == "/api/admin/gamerules":
                    ctx.auth_service.require_admin(token)
                    self._send_json(200, {"gamerules": ctx.mc_manager.get_gamerules()})
                    return

                if path == "/api/admin/plugins":
                    ctx.auth_service.require_admin(token)
                    search_q = qs.get("search", [""])[0]
                    res_data: Dict[str, Any] = {"plugins": ctx.mc_manager.list_plugins()}
                    if "search" in qs:
                        res_data["catalog"] = ctx.mc_manager.search_modrinth_plugins(search_q)
                    self._send_json(200, res_data)
                    return

                if path == "/api/admin/files":
                    ctx.auth_service.require_admin(token)
                    rel_dir = qs.get("dir", [""])[0]
                    search_q = qs.get("search", [""])[0]
                    self._send_json(200, {
                        "dir": rel_dir,
                        "search": search_q,
                        "files": ctx.mc_manager.list_files(rel_dir, search=search_q),
                    })
                    return

                if path == "/api/admin/files/read":
                    ctx.auth_service.require_admin(token)
                    rel_path = qs.get("path", [""])[0]
                    self._send_json(200, ctx.mc_manager.read_file_content(rel_path))
                    return

                self._send_json(404, {"error": f"Unknown endpoint {path}"})
            except Exception as e:
                self._handle_exception(e)

        def do_POST(self) -> None:
            parsed = urlparse(self.path)
            path = parsed.path
            qs = parse_qs(parsed.query)

            if not self._validate_local_request():
                return

            try:
                token = self._get_token(parsed)
                body = self._read_json_body()

                if path == "/api/app/version-check":
                    vc = ctx.get_version_check(force=True, trigger_download=True)
                    if vc.get("isOutdated"):
                        summary = ctx.mc_manager.get_status_summary()
                        summary["versionCheck"] = vc
                        ctx.broadcast_event("status_changed", summary)
                    self._send_json(200, {"versionCheck": vc})
                    return

                if path == "/api/app/update/download":
                    vc = ctx.get_version_check(force=True, trigger_download=False)
                    au = ctx._enrich_auto_update_state(ctx.auto_updater.ensure_update_downloaded_async(vc, force=True))
                    vc["autoUpdate"] = au
                    summary = ctx.mc_manager.get_status_summary()
                    summary["versionCheck"] = vc
                    ctx.broadcast_event("status_changed", summary)
                    self._send_json(200, {"versionCheck": vc, "autoUpdate": au})
                    return

                if path == "/api/app/update/apply":
                    if ctx.mc_manager.local_status != "idle" or ctx.sync_engine.is_locked_by_current_machine():
                        raise HostLockConflictError(
                            "Stop the Minecraft server and finish world sync before restarting to apply the update."
                        )
                    ready = ctx.auto_updater.get_ready_update_path()
                    if ready is None or not ready.exists() or not ready.is_file():
                        raise FileNotFoundError("No verified update installer is ready yet.")
                    au_state = ctx._enrich_auto_update_state(ctx.auto_updater.get_state())
                    if not (IS_FROZEN and sys.platform.startswith("win")):
                        self._send_json(200, {
                            "applied": False,
                            "restartScheduled": False,
                            "version": au_state.get("targetVersion", ""),
                            "downloadUrl": "/api/app/update/file",
                            "filePath": str(ready),
                            "message": "Update downloaded and verified. Running from source — download or launch the updated executable directly.",
                        })
                        return
                    target_exe = Path(sys.executable).resolve()
                    ctx.auto_updater.spawn_windows_relaunch_helper(
                        target_exe,
                        ready,
                        staged_already=bool(au_state.get("stagedToExe")),
                        extra_args=sys.argv[1:],
                    )
                    self._send_json(200, {
                        "applied": True,
                        "restartScheduled": True,
                        "version": au_state.get("targetVersion", ""),
                    })
                    threading.Timer(0.45, lambda: os._exit(0)).start()
                    return

                if path == "/api/setup/init":
                    admin_username = str(body.get("adminUsername", "Admin"))
                    server_name = str(body.get("serverName", "Shared Minecraft Server"))
                    custom_passkey = body.get("customPasskey")
                    shared_sync_dir = body.get("sharedSyncDir")
                    machine_name = body.get("machineName")

                    cfg, admin_acc, raw_pk, session_tok = ctx.auth_service.initialize_cluster(
                        admin_username=admin_username,
                        server_name=server_name,
                        custom_admin_passkey=custom_passkey,
                        shared_sync_dir=shared_sync_dir,
                    )
                    if machine_name and str(machine_name).strip():
                        ctx.sync_engine.local_config.machine_name = str(machine_name).strip()
                        ctx.sync_engine.save_local_config()
                    self._send_json(200, {
                        "initialized": True,
                        "token": session_tok,
                        "account": admin_acc.to_public_dict(),
                        "generatedPasskey": raw_pk,
                        "cluster": cfg.to_dict(include_secrets=False),
                        "versionCheck": ctx.get_version_check(force=False),
                    })
                    return

                if path == "/api/setup/sync-dir":
                    if ctx.mc_manager.local_status != "idle":
                        raise ValueError("Cannot change Shared Sync Folder while the server is active.")
                    new_dir = str(body.get("sharedSyncDir", "")).strip()
                    machine_name = str(body.get("machineName", "")).strip()
                    if not new_dir or "\x00" in new_dir:
                        raise ValueError("Shared Sync Folder path is required.")
                    if new_dir.startswith("\\\\") or new_dir.startswith("//"):
                        raise ValueError("Network UNC paths are not supported for Shared Sync Folder; use a local or mounted drive path.")
                    transfer_msg = ctx.sync_engine._has_active_cloud_transfer()
                    if transfer_msg:
                        raise ValueError(f"{transfer_msg} Wait for cloud sync to finish before switching folders.")
                    target_dir = Path(new_dir)
                    if target_dir.exists() and not target_dir.is_dir():
                        raise ValueError("Selected Shared Sync path is a file, not a directory.")
                    drive_part = os.path.splitdrive(new_dir)[0]
                    if drive_part and not Path(drive_part + os.sep).exists():
                        raise ValueError(f"Shared drive or root folder is not accessible: '{drive_part}'.")
                    ctx.sync_engine.local_config.shared_sync_dir = new_dir
                    if machine_name:
                        ctx.sync_engine.local_config.machine_name = machine_name
                    ctx.sync_engine.save_local_config()
                    ctx.sync_engine.reset_sync_state()
                    cfg = ctx.sync_engine.load_cluster_config()
                    ctx.broadcast_event("status_changed", ctx.mc_manager.get_status_summary())
                    self._send_json(200, {
                        "sharedSyncDir": str(ctx.sync_engine.shared_dir),
                        "initialized": ctx.sync_engine.is_cluster_initialized(),
                        "syncDiagnostics": ctx.sync_engine.get_sync_diagnostics(),
                        "serverName": cfg.server_name if cfg else None,
                        "versionCheck": ctx.get_version_check(force=False),
                    })
                    return

                if path == "/api/setup/browse-folder":
                    chosen = ctx.sync_engine.browse_folder_dialog()
                    self._send_json(200, {"selectedPath": chosen})
                    return

                if path == "/api/auth/login":
                    passkey = str(body.get("passkey", ""))
                    machine_name = str(body.get("machineName", "")).strip()
                    acc, session_tok = ctx.auth_service.login_with_passkey(passkey)
                    if machine_name:
                        ctx.sync_engine.local_config.machine_name = machine_name
                        ctx.sync_engine.save_local_config()
                        _sync_local_lock_machine_name(ctx, machine_name)
                    vc = ctx.get_version_check(force=False)
                    status_summary = ctx.mc_manager.get_status_summary()
                    status_summary["versionCheck"] = vc
                    self._send_json(200, {
                        "token": session_tok,
                        "account": acc.to_public_dict(),
                        "status": status_summary,
                        "versionCheck": vc,
                    })
                    return

                if path == "/api/auth/logout":
                    if token:
                        ctx.auth_service.logout_session(token)
                    self._send_json(200, {"loggedOut": True})
                    return

                # ---------------- MEMBER + ADMIN HOSTING LIFECYCLE ----------------
                if path == "/api/server/start":
                    user = ctx.auth_service.require_auth(token)
                    sync_param = qs.get("sync", [""])[0].strip().lower()
                    run_sync = (
                        os.environ.get("MINERELAY_TEST_MODE") == "1"
                        or sync_param in ("1", "true", "yes")
                        or bool(body.get("sync"))
                    )
                    fn = ctx.mc_manager.start_hosting
                    try:
                        params = inspect.signature(fn).parameters
                    except Exception:
                        params = {}
                    kwargs: Dict[str, Any] = {}
                    if "sync" in params:
                        kwargs["sync"] = run_sync
                    elif "blocking" in params:
                        kwargs["blocking"] = run_sync
                    elif "async_start" in params:
                        kwargs["async_start"] = not run_sync
                    summary = fn(user, **kwargs)
                    if isinstance(summary, dict):
                        summary.setdefault("started", True)
                        summary["versionCheck"] = ctx.get_version_check(force=False)
                    self._send_json(200, summary)
                    return

                if path == "/api/server/stop":
                    user = ctx.auth_service.require_auth(token)
                    summary = ctx.mc_manager.stop_hosting(user)
                    self._send_json(200, summary)
                    return

                if path == "/api/server/restart":
                    user = ctx.auth_service.require_auth(token)
                    if ctx.mc_manager.local_status == "idle":
                        lock = ctx.sync_engine.get_lock()
                        if lock.is_actively_locked() and not ctx.sync_engine.is_locked_by_current_machine(lock):
                            raise HostLockConflictError(
                                f"Cannot restart active remote server hosted by '{lock.host_username}' ({lock.machine_name})."
                            )
                    summary = ctx.mc_manager.restart_hosting(user)
                    self._send_json(200, summary)
                    return

                if path == "/api/server/sync-now":
                    ctx.auth_service.require_auth(token)
                    lock = ctx.sync_engine.get_lock()
                    if lock.is_actively_locked() and not ctx.sync_engine.is_locked_by_current_machine(lock):
                        raise HostLockConflictError(
                            f"Cannot sync remote server hosted by '{lock.host_username}' ({lock.machine_name})."
                        )
                    if ctx.mc_manager.local_status != "running" or not ctx.mc_manager._reached_running:
                        raise ValueError(
                            f"Server must be running locally to push a live sync (current status: {ctx.mc_manager.local_status})."
                        )
                    if not ctx.sync_engine.owns_lock(ctx.sync_engine._active_lock_epoch):
                        raise HostLockConflictError("This machine does not hold the active host lock.")
                    res = ctx.mc_manager.run_live_auto_sync_once(force=True)
                    self._send_json(200, {
                        "result": res,
                        "autoSync": ctx.mc_manager.get_auto_sync_status(),
                        "status": ctx.mc_manager.get_status_summary(),
                    })
                    return

                # ---------------- ADMIN ONLY POST ROUTES ----------------
                if path == "/api/admin/accounts":
                    ctx.auth_service.require_admin(token)
                    username = str(body.get("username", ""))
                    custom_pk = body.get("customPasskey")
                    member, raw_pk = ctx.auth_service.create_member_account(
                        admin_token=token or "",
                        username=username,
                        custom_passkey=custom_pk,
                    )
                    ctx.broadcast_event("status_changed", ctx.mc_manager.get_status_summary())
                    self._send_json(201, {
                        "account": member.to_public_dict(),
                        "generatedPasskey": raw_pk,
                    })
                    return

                if path == "/api/admin/accounts/rotate":
                    ctx.auth_service.require_admin(token)
                    target_id = str(body.get("userId", ""))
                    acc, new_pk = ctx.auth_service.rotate_passkey(token or "", target_id)
                    ctx.broadcast_event("status_changed", ctx.mc_manager.get_status_summary())
                    self._send_json(200, {
                        "account": acc.to_public_dict(),
                        "generatedPasskey": new_pk,
                    })
                    return

                if path == "/api/admin/accounts/rename":
                    ctx.auth_service.require_admin(token)
                    target_id = str(body.get("userId", ""))
                    new_username = str(body.get("username", ""))
                    acc = ctx.auth_service.rename_account(token or "", target_id, new_username)
                    ctx.broadcast_event("status_changed", ctx.mc_manager.get_status_summary())
                    self._send_json(200, {"account": acc.to_public_dict()})
                    return

                if path == "/api/admin/console":
                    ctx.auth_service.require_admin(token)
                    _ensure_no_remote_lock(ctx, "execute console commands")
                    cmd = str(body.get("command", ""))
                    res = ctx.mc_manager.send_command(cmd)
                    self._send_json(200, res)
                    return

                if path == "/api/admin/console/clear":
                    ctx.auth_service.require_admin(token)
                    self._send_json(200, ctx.mc_manager.clear_console_logs())
                    return

                if path == "/api/admin/install-jar":
                    ctx.auth_service.require_admin(token)
                    _ensure_no_remote_lock(ctx, "install or switch server JAR")
                    if ctx.mc_manager.local_status != "idle":
                        raise ValueError("Cannot install or switch server JAR while this server is starting or running.")
                    software = str(body.get("software", "paper"))
                    version = str(body.get("version", "26.3"))
                    res = ctx.mc_manager.download_server_jar(software, version)
                    self._send_json(200, res)
                    return

                if path == "/api/admin/players":
                    ctx.auth_service.require_admin(token)
                    action = str(body.get("action", ""))
                    username = str(body.get("username", ""))
                    reason = str(body.get("value") or body.get("reason", "Banned by Admin"))
                    res = ctx.mc_manager.player_action(action, username, reason)
                    self._send_json(200, res)
                    return

                if path == "/api/admin/plugins/install":
                    ctx.auth_service.require_admin(token)
                    project_id = str(body.get("projectId", ""))
                    title = str(body.get("title", ""))
                    res = ctx.mc_manager.install_modrinth_plugin(project_id, title=title)
                    self._send_json(200, res)
                    return

                if path == "/api/admin/plugins/toggle":
                    ctx.auth_service.require_admin(token)
                    filename = str(body.get("filename", ""))
                    self._send_json(200, {"plugins": ctx.mc_manager.toggle_plugin(filename)})
                    return

                if path == "/api/admin/backups":
                    admin_user = ctx.auth_service.require_admin(token)
                    lock = ctx.sync_engine.get_lock()
                    if ctx.mc_manager.local_status not in ("idle", "running"):
                        raise ValueError(f"Cannot create backup while server is in '{ctx.mc_manager.local_status}' state.")
                    if ctx.mc_manager.local_status == "idle":
                        if lock.is_actively_locked() and not ctx.sync_engine.is_locked_by_current_machine(lock):
                            raise HostLockConflictError(
                                f"Cannot create snapshot while '{lock.host_username}' ({lock.machine_name}) holds the host lock."
                            )
                        ctx.sync_engine.ensure_runtime_synced_when_idle(strict=True)
                    label = str(body.get("label", "manual"))
                    scope = str(body.get("scope", "cloud") or "cloud").strip().lower()
                    if scope not in ("local", "cloud"):
                        raise ValueError("Backup scope must be 'local' or 'cloud'.")
                    res = ctx.mc_manager.flush_and_backup_live_world(
                        label=label,
                        host_username=admin_user.username,
                        scope=scope,
                    )
                    self._send_json(200, {"result": res, "backups": ctx.sync_engine.list_backups()})
                    return

                if path == "/api/admin/backups/restore":
                    ctx.auth_service.require_admin(token)
                    lock = ctx.sync_engine.get_lock()
                    if ctx.mc_manager.local_status != "idle":
                        raise ValueError("Cannot restore backup while this server is running or starting. Click Stop & Sync first.")
                    if lock.is_actively_locked():
                        raise HostLockConflictError(
                            f"Cannot restore backup while '{lock.host_username}' ({lock.machine_name}) is actively hosting. Stop the server or Force-Unlock in Zero-Port-Forward & Sync first."
                        )
                    name = str(body.get("name", "")).strip()
                    if not name:
                        raise ValueError("Backup name is required.")
                    location = str(body.get("location", "") or "").strip().lower()
                    if location and location not in ("local", "cloud"):
                        raise ValueError("Backup location must be 'local' or 'cloud'.")
                    res = ctx.sync_engine.restore_backup(name, location=location)
                    ctx.mc_manager._world_info_cache = None
                    ctx.mc_manager._world_info_cache_ts = 0.0
                    ctx.mc_manager._runtime_json_cache.clear()
                    ctx.mc_manager._log(f"Restored world backup '{name}' to runtime and shared snapshot.", "INFO", "SyncEngine")
                    ctx.mc_manager._emit("status_changed", ctx.mc_manager.get_status_summary())
                    res["properties"] = ctx.mc_manager.get_server_properties()
                    res["world"] = ctx.mc_manager.get_world_info()
                    res["backups"] = ctx.sync_engine.list_backups()
                    res["status"] = ctx.mc_manager.get_status_summary()
                    self._send_json(200, res)
                    return

                if path == "/api/admin/backups/prune-cloud":
                    ctx.auth_service.require_admin(token)
                    _ensure_no_remote_lock(ctx, "prune cloud backups")
                    if ctx.mc_manager.local_status != "idle":
                        raise HostLockConflictError("Cannot prune cloud backups while the local server is running or transitioning.")
                    keep = 0
                    if "keep" in body:
                        keep = self._require_int(body["keep"], "keep")
                        if keep < 0 or keep > 20:
                            raise ValueError("keep must be between 0 and 20.")
                    res = ctx.sync_engine.prune_cloud_backups(keep=keep)
                    removed_cnt = int(res.get("removedCount", 0) or 0)
                    freed_mb = round(float(res.get("freedBytes", 0) or 0) / (1024.0 * 1024.0), 2)
                    ctx.mc_manager._log(
                        f"Pruned {removed_cnt} cloud backup archive(s) (keep={keep}, freed {freed_mb} MB).",
                        "INFO",
                        "SyncEngine",
                    )
                    summary = ctx.mc_manager.get_status_summary()
                    ctx.broadcast_event("status_changed", summary)
                    self._send_json(200, {
                        "result": res,
                        "backups": ctx.sync_engine.list_backups(),
                        "status": summary,
                    })
                    return

                if path == "/api/admin/backups/push-cloud":
                    ctx.auth_service.require_admin(token)
                    _ensure_no_remote_lock(ctx, "push local backups to Shared Drive")
                    if ctx.mc_manager.local_status not in ("idle", "running"):
                        raise HostLockConflictError(
                            f"Cannot push local backups while the server is transitioning ({ctx.mc_manager.local_status})."
                        )
                    push_all = bool(body.get("all", False))
                    name = str(body.get("name", "") or "").strip()
                    if not push_all and not name:
                        raise ValueError("Backup name is required unless 'all' is true.")
                    res = ctx.sync_engine.push_local_backup_to_cloud(backup_name=name, push_all=push_all)
                    pushed_cnt = int(res.get("pushedCount", 0) or 0)
                    pushed_mb = round(float(res.get("pushedBytes", 0) or 0) / (1024.0 * 1024.0), 2)
                    if pushed_cnt > 0:
                        ctx.mc_manager._log(
                            f"Pushed {pushed_cnt} local backup(s) ({pushed_mb} MB) to Shared Drive folder.",
                            "INFO",
                            "SyncEngine",
                        )
                    summary = ctx.mc_manager.get_status_summary()
                    ctx.broadcast_event("status_changed", summary)
                    self._send_json(200, {
                        "result": res,
                        "backups": ctx.sync_engine.list_backups(),
                        "status": summary,
                    })
                    return

                if path == "/api/admin/files/write":
                    ctx.auth_service.require_admin(token)
                    rel_path = str(body.get("path", "")).strip()
                    if not rel_path:
                        raise ValueError("File path cannot be empty.")
                    content = str(body.get("content", ""))
                    self._send_json(200, ctx.mc_manager.write_file_content(rel_path, content))
                    return

                if path == "/api/admin/files/upload":
                    ctx.auth_service.require_admin(token)
                    rel_dir = str(body.get("dir", ""))
                    filename = str(body.get("filename", "")).strip()
                    if not filename:
                        raise ValueError("Filename cannot be empty.")
                    b64_data = str(body.get("dataBase64", ""))
                    self._send_json(200, ctx.mc_manager.upload_file_base64(rel_dir, filename, b64_data))
                    return

                if path == "/api/admin/tunnel/download":
                    ctx.auth_service.require_admin(token)
                    with ctx.tunnel_manager._mutex:
                        t_running = ctx.tunnel_manager._proc is not None and ctx.tunnel_manager._proc.poll() is None
                    if t_running or ctx.mc_manager.local_status != "idle":
                        raise HostLockConflictError("Stop the server before downloading or updating the playit.gg agent.")
                    threading.Thread(target=ctx.tunnel_manager.download_playit_binary, daemon=True).start()
                    self._send_json(200, {"started": True})
                    return

                if path == "/api/admin/tunnel/secret":
                    ctx.auth_service.require_admin(token)
                    _ensure_no_remote_lock(ctx, "modify tunnel secret key")
                    secret_key = str(body.get("secretKey", ""))
                    ctx.tunnel_manager.save_secret_key(secret_key)
                    t_state = ctx.tunnel_manager.get_state()
                    ctx.broadcast_event("tunnel_updated", t_state)
                    ctx.broadcast_event("status_changed", ctx.mc_manager.get_status_summary())
                    self._send_json(200, t_state)
                    return

                if path == "/api/admin/force-unlock":
                    ctx.auth_service.require_admin(token)
                    if ctx.mc_manager.local_status != "idle":
                        raise HostLockConflictError(
                            "Cannot Force-Unlock while the server is running or transitioning on this PC. Click 'Stop & Sync World' instead."
                        )
                    lock = ctx.sync_engine.force_unlock()
                    ctx.mc_manager._world_info_cache = None
                    if hasattr(ctx.mc_manager, "_java_cache"):
                        ctx.mc_manager._java_cache.clear()
                    if hasattr(ctx.mc_manager, "_remote_log_cutoff_epoch"):
                        ctx.mc_manager._remote_log_cutoff_epoch = ""
                        ctx.mc_manager._remote_log_cutoff_id = ""
                        ctx.mc_manager._remote_log_cutoff_seq = 0
                        ctx.mc_manager._remote_log_cutoff_ms = 0
                    try:
                        ctx.sync_engine.ensure_runtime_synced_when_idle(strict=False)
                    except Exception:
                        pass
                    ctx.mc_manager._log("Emergency Force-Unlock executed by Admin.", "WARN", "SyncEngine")
                    ctx.broadcast_event("status_changed", ctx.mc_manager.get_status_summary())
                    ctx.broadcast_event("players_updated", ctx.mc_manager.get_players_data())
                    self._send_json(200, {"unlocked": True, "lock": lock.to_dict()})
                    return

                if path == "/api/admin/world/reset":
                    admin_user = ctx.auth_service.require_admin(token)
                    mode = str(body.get("mode", "random"))
                    seed = str(body.get("seed", ""))
                    level_type = body.get("levelType")
                    generate_structures = body.get("generateStructures")
                    raw_dis_end = body.get("disableEnd") if body.get("disableEnd") is not None else body.get("disable-end")
                    disable_end = _safe_bool(raw_dis_end) if raw_dis_end is not None else None
                    backup_existing = _safe_bool(body.get("backupExisting"), True)
                    res = ctx.mc_manager.generate_new_world(
                        mode=mode,
                        seed=seed,
                        level_type=str(level_type) if level_type is not None else None,
                        generate_structures=_safe_bool(generate_structures) if generate_structures is not None else None,
                        disable_end=disable_end,
                        backup_existing=backup_existing,
                        user=admin_user,
                    )
                    self._send_json(200, res)
                    return

                self._send_json(404, {"error": f"Unknown endpoint {path}"})
            except Exception as e:
                self._handle_exception(e)

        def do_PUT(self) -> None:
            parsed = urlparse(self.path)
            path = parsed.path

            if not self._validate_local_request():
                return

            try:
                token = self._get_token(parsed)
                body = self._read_json_body()

                if path == "/api/admin/properties":
                    ctx.auth_service.require_admin(token)
                    updates = body.get("properties", body)
                    if not isinstance(updates, dict):
                        raise ValueError("Properties must be an object.")
                    props = ctx.mc_manager.update_server_properties(updates)
                    ctx.broadcast_event("status_changed", ctx.mc_manager.get_status_summary())
                    self._send_json(200, {
                        "properties": props,
                        "world": ctx.mc_manager.get_world_info(),
                    })
                    return

                if path == "/api/admin/gamerules":
                    ctx.auth_service.require_admin(token)
                    _ensure_no_remote_lock(ctx, "modify gamerules")
                    updates = body.get("gamerules", body)
                    if not isinstance(updates, dict):
                        raise ValueError("Gamerules must be an object.")
                    gr = ctx.mc_manager.update_gamerules(updates)
                    ctx.broadcast_event("status_changed", ctx.mc_manager.get_status_summary())
                    self._send_json(200, {"gamerules": gr})
                    return

                if path == "/api/admin/settings":
                    ctx.auth_service.require_admin(token)
                    auto_sync_changed = False
                    with ctx.auth_service._config_mutex:
                        cfg = ctx.sync_engine.load_cluster_config()
                        if not cfg:
                            raise ValueError("Cluster not initialized.")

                        old_port = cfg.server_port
                        old_public_join_address = cfg.public_join_address

                        # Validate all inputs first so malformed payloads always raise 400
                        new_sw = cfg.mc_software
                        if "mcSoftware" in body:
                            new_sw = str(body["mcSoftware"]).strip().lower()
                            if new_sw not in ("paper", "vanilla"):
                                raise ValueError("mcSoftware must be 'paper' or 'vanilla'.")

                        new_ver = cfg.mc_version
                        if "mcVersion" in body:
                            new_ver = str(body["mcVersion"]).strip()
                            if not new_ver:
                                raise ValueError("mcVersion cannot be empty.")

                        new_mem_min = max(512, self._require_int(body["memoryMinMb"], "memoryMinMb")) if "memoryMinMb" in body else cfg.memory_min_mb
                        new_mem_max = max(512, self._require_int(body["memoryMaxMb"], "memoryMaxMb")) if "memoryMaxMb" in body else cfg.memory_max_mb
                        if new_mem_min > new_mem_max:
                            if "memoryMaxMb" in body and "memoryMinMb" not in body:
                                new_mem_min = new_mem_max
                            else:
                                new_mem_max = new_mem_min

                        new_java_path = str(body["javaPath"]).strip() or "java" if "javaPath" in body else cfg.java_path
                        new_jvm_flags = str(body["jvmFlags"]).strip() if "jvmFlags" in body else cfg.jvm_flags

                        new_port = cfg.server_port
                        if "serverPort" in body:
                            new_port = self._require_int(body["serverPort"], "serverPort")
                            if not (1 <= new_port <= 65535):
                                raise ValueError("Server port must be between 1 and 65535.")

                        new_tunnel_enabled = (
                            _safe_bool(body["tunnelEnabled"], cfg.tunnel_enabled)
                            if "tunnelEnabled" in body
                            else cfg.tunnel_enabled
                        )
                        new_tunnel_provider = (
                            str(body["tunnelProvider"]).strip().lower()
                            if "tunnelProvider" in body
                            else cfg.tunnel_provider
                        )
                        if new_tunnel_provider not in ("playit", "custom", "none"):
                            raise ValueError("tunnelProvider must be 'playit', 'custom', or 'none'.")

                        new_idle_min = cfg.idle_shutdown_minutes
                        if "idleShutdownMinutes" in body:
                            new_idle_min = self._require_int(body["idleShutdownMinutes"], "idleShutdownMinutes")
                            if new_idle_min < 0 or new_idle_min > AUTO_SYNC_MAX_MINUTES:
                                raise ValueError(f"idleShutdownMinutes must be between 0 and {AUTO_SYNC_MAX_MINUTES}.")

                        new_sync_min = cfg.auto_sync_interval_minutes
                        if "autoSyncIntervalMinutes" in body:
                            new_sync_min = self._require_int(body["autoSyncIntervalMinutes"], "autoSyncIntervalMinutes")
                            if new_sync_min < 0 or new_sync_min > AUTO_SYNC_MAX_MINUTES:
                                raise ValueError(
                                    f"autoSyncIntervalMinutes must be 0 (disabled) or between 1 and {AUTO_SYNC_MAX_MINUTES}."
                                )

                        new_max_cloud_backups = getattr(cfg, "max_cloud_backups", 2)
                        if "maxCloudBackups" in body:
                            new_max_cloud_backups = self._require_int(body["maxCloudBackups"], "maxCloudBackups")
                            if new_max_cloud_backups < 0 or new_max_cloud_backups > 20:
                                raise ValueError("maxCloudBackups must be between 0 and 20.")

                        runtime_fields_changed = any([
                            new_port != cfg.server_port,
                            new_sw != cfg.mc_software,
                            new_ver != cfg.mc_version,
                            new_mem_min != cfg.memory_min_mb,
                            new_mem_max != cfg.memory_max_mb,
                            new_java_path != cfg.java_path,
                            new_jvm_flags != cfg.jvm_flags,
                            new_tunnel_enabled != cfg.tunnel_enabled,
                            new_tunnel_provider != cfg.tunnel_provider,
                        ])
                        if runtime_fields_changed:
                            if ctx.mc_manager.local_status != "idle":
                                raise HostLockConflictError(
                                    "Stop the local server before changing startup/JVM/port/tunnel-provider settings."
                                )
                            _ensure_no_remote_lock(ctx, "change startup, port, or tunnel-provider settings")

                        if "serverName" in body:
                            cfg.server_name = str(body["serverName"]).strip() or cfg.server_name
                        cfg.mc_software = new_sw
                        cfg.mc_version = new_ver
                        cfg.memory_min_mb = new_mem_min
                        cfg.memory_max_mb = new_mem_max
                        cfg.java_path = new_java_path
                        cfg.jvm_flags = new_jvm_flags
                        if new_port != old_port:
                            ctx.mc_manager.update_server_properties({"server-port": str(new_port)})
                            cfg.server_port = new_port
                        cfg.tunnel_enabled = new_tunnel_enabled
                        cfg.tunnel_provider = new_tunnel_provider
                        if "publicJoinAddress" in body:
                            cfg.public_join_address = str(body["publicJoinAddress"]).strip()
                        if "gitSyncEnabled" in body:
                            cfg.git_sync_enabled = _safe_bool(body["gitSyncEnabled"], cfg.git_sync_enabled)
                        if "autoBackupOnStop" in body:
                            cfg.auto_backup_on_stop = _safe_bool(body["autoBackupOnStop"], cfg.auto_backup_on_stop)
                        if "maxBackups" in body:
                            cfg.max_backups = max(1, self._require_int(body["maxBackups"], "maxBackups"))
                        if "cloudBackupOnStop" in body:
                            cfg.cloud_backup_on_stop = _safe_bool(body["cloudBackupOnStop"], cfg.cloud_backup_on_stop)
                        cfg.max_cloud_backups = new_max_cloud_backups
                        cfg.idle_shutdown_minutes = new_idle_min
                        auto_sync_changed = new_sync_min != cfg.auto_sync_interval_minutes
                        cfg.auto_sync_interval_minutes = new_sync_min

                        ctx.sync_engine.save_cluster_config(cfg)

                    if "machineName" in body and str(body["machineName"]).strip():
                        new_mname = str(body["machineName"]).strip()
                        ctx.sync_engine.local_config.machine_name = new_mname
                        ctx.sync_engine.save_local_config()
                        _sync_local_lock_machine_name(ctx, new_mname)

                    # If active lock is held locally, update or clear publicJoinAddress cleanly
                    if ctx.sync_engine.is_locked_by_current_machine() and "publicJoinAddress" in body:
                        if cfg.public_join_address:
                            with ctx.tunnel_manager._mutex:
                                ctx.tunnel_manager.public_address = cfg.public_join_address
                            ctx.sync_engine.update_lock_state(public_address=cfg.public_join_address)
                        else:
                            with ctx.tunnel_manager._mutex:
                                prev_pub = ctx.tunnel_manager.public_address
                                if prev_pub and prev_pub == old_public_join_address:
                                    port_val = ctx.mc_manager._active_server_port or cfg.server_port or 25565
                                    ctx.tunnel_manager.public_address = (
                                        f"{ctx.tunnel_manager.get_lan_ip()}:{port_val}"
                                        if ctx.tunnel_manager.status != "stopped"
                                        else ""
                                    )
                                effective_pub = ctx.tunnel_manager.public_address
                            ctx.sync_engine.update_lock_state(public_address=effective_pub)

                    if auto_sync_changed and hasattr(ctx.mc_manager, "notify_auto_sync_schedule_changed"):
                        ctx.mc_manager.notify_auto_sync_schedule_changed()

                    summary = ctx.mc_manager.get_status_summary()
                    ctx.broadcast_event("status_changed", summary)
                    self._send_json(200, {
                        "cluster": cfg.to_dict(include_secrets=False),
                        "status": summary,
                    })
                    return

                self._send_json(404, {"error": f"Unknown endpoint {path}"})
            except Exception as e:
                self._handle_exception(e)

        def do_DELETE(self) -> None:
            parsed = urlparse(self.path)
            path = parsed.path
            qs = parse_qs(parsed.query)

            if not self._validate_local_request():
                return

            try:
                token = self._get_token(parsed)

                if path.startswith("/api/admin/accounts/"):
                    target_id = unquote(path.split("/api/admin/accounts/", 1)[1])
                    ctx.auth_service.delete_member_account(token or "", target_id)
                    cfg = ctx.sync_engine.load_cluster_config()
                    ctx.broadcast_event("status_changed", ctx.mc_manager.get_status_summary())
                    self._send_json(200, {
                        "deleted": True,
                        "accounts": [a.to_public_dict() for a in (cfg.accounts if cfg else [])],
                    })
                    return

                if path.startswith("/api/admin/backups/"):
                    ctx.auth_service.require_admin(token)
                    _ensure_no_remote_lock(ctx, "delete backups")
                    if ctx.mc_manager.local_status != "idle":
                        raise HostLockConflictError("Cannot delete backups while the local server is running or transitioning.")
                    name = unquote(path.split("/api/admin/backups/", 1)[1])
                    location = qs.get("location", [""])[0].strip().lower()
                    if location and location not in ("local", "cloud"):
                        raise ValueError("Backup location must be 'local' or 'cloud'.")
                    ctx.sync_engine.delete_backup(name, location=location)
                    self._send_json(200, {"backups": ctx.sync_engine.list_backups()})
                    return

                if path == "/api/admin/plugins":
                    ctx.auth_service.require_admin(token)
                    filename = qs.get("filename", [""])[0]
                    self._send_json(200, {"plugins": ctx.mc_manager.delete_plugin(filename)})
                    return

                if path == "/api/admin/files":
                    ctx.auth_service.require_admin(token)
                    rel_path = qs.get("path", [""])[0]
                    self._send_json(200, ctx.mc_manager.delete_runtime_file(rel_path))
                    return

                self._send_json(404, {"error": f"Unknown endpoint {path}"})
            except Exception as e:
                self._handle_exception(e)

        def _handle_sse(self, user: Any, token: Optional[str] = None) -> None:
            self.close_connection = True
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.end_headers()

            client_q: queue.Queue = queue.Queue(maxsize=100)
            with ctx.sse_mutex:
                ctx.sse_clients.append(client_q)

            try:
                init_summary = ctx.mc_manager.get_status_summary()
                init_summary["logs"] = ctx.mc_manager.get_effective_logs(120)
                init_summary["versionCheck"] = ctx.get_version_check(force=False)
                init_payload = json.dumps({
                    "event": "connected",
                    "data": init_summary,
                }, default=str)
                self.wfile.write(f"data: {init_payload}\n\n".encode("utf-8"))
                self.wfile.flush()

                while True:
                    try:
                        msg = client_q.get(timeout=8)
                        self.wfile.write(f"data: {msg}\n\n".encode("utf-8"))
                        self.wfile.flush()
                    except queue.Empty:
                        if token and ctx.auth_service.get_user_by_token(token) is None:
                            break
                        with ctx.sse_mutex:
                            still_registered = client_q in ctx.sse_clients
                        if not still_registered:
                            break
                        ping_payload = json.dumps({"event": "heartbeat", "data": {"ts": time.time()}}, default=str)
                        self.wfile.write(f"data: {ping_payload}\n\n".encode("utf-8"))
                        self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass
            finally:
                self.close_connection = True
                with ctx.sse_mutex:
                    if client_q in ctx.sse_clients:
                        ctx.sse_clients.remove(client_q)

        def _serve_static(self, req_path: str) -> None:
            clean = unquote(req_path).lstrip("/") or "index.html"
            base_pub = public_dir.resolve()
            target = (base_pub / clean).resolve()
            if not target.is_relative_to(base_pub):
                self._send_json(403, {"error": "Forbidden static path."})
                return
            if not target.exists() or target.is_dir():
                if Path(clean).suffix.lower() in (".js", ".css", ".png", ".svg", ".ico", ".map", ".json", ".woff2", ".wasm"):
                    self._send_json(404, {"error": f"Static asset '{clean}' not found."})
                    return
                target = base_pub / "index.html"
            if not target.exists():
                self._send_json(404, {"error": "Static UI files not found."})
                return
            mimetypes.add_type("image/svg+xml", ".svg")
            mimetypes.add_type("image/x-icon", ".ico")
            mime, _ = mimetypes.guess_type(str(target))
            content = target.read_bytes()
            self.send_response(200)
            self.send_header("Content-Type", mime or "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(content)))
            self.send_header("Cache-Control", "no-cache")
            self.end_headers()
            try:
                self.wfile.write(content)
            except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError, OSError):
                self.close_connection = True

    return DashboardRequestHandler


def run_server(port: int = 3080, open_browser: bool = False, data_dir: Optional[str] = None) -> ThreadingHTTPServer:
    ctx = AppContext(data_dir=data_dir)
    public_dir = BUNDLE_DIR / "public"
    if not public_dir.exists():
        public_dir = ROOT_DIR / "public"
    handler_cls = create_request_handler(ctx, public_dir)
    httpd = ThreadingHTTPServer(("127.0.0.1", port), handler_cls)
    httpd.daemon_threads = True
    httpd.app_context = ctx  # type: ignore
    actual_port = httpd.server_address[1]
    url = f"http://localhost:{actual_port}"
    print(f"============================================================")
    print(f"  MineRelay - Distributed Minecraft Server Host & Dashboard")
    print(f"  Dashboard URL : {url}")
    print(f"  Shared Folder : {ctx.sync_engine.shared_dir}")
    print(f"  Machine Name  : {ctx.sync_engine.local_config.machine_name}")
    print(f"============================================================")
    if open_browser:
        threading.Timer(0.8, lambda: webbrowser.open(url)).start()
    return httpd


if __name__ == "__main__":
    import atexit
    import signal

    if sys.platform.startswith("win"):
        try:
            import ctypes
            ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("MineRelay.ServerHost.App")
            ctypes.windll.kernel32.SetConsoleTitleW("MineRelay — Distributed Minecraft Server Host")
            ico_candidate = BUNDLE_DIR / "public" / "logo.ico"
            if not ico_candidate.exists():
                ico_candidate = ROOT_DIR / "public" / "logo.ico"
            if ico_candidate.exists():
                hwnd = ctypes.windll.kernel32.GetConsoleWindow()
                if hwnd:
                    IMAGE_ICON = 1
                    LR_LOADFROMFILE = 0x0010
                    WM_SETICON = 0x0080
                    hicon_big = ctypes.windll.user32.LoadImageW(0, str(ico_candidate), IMAGE_ICON, 48, 48, LR_LOADFROMFILE)
                    hicon_sm = ctypes.windll.user32.LoadImageW(0, str(ico_candidate), IMAGE_ICON, 16, 16, LR_LOADFROMFILE)
                    if hicon_big:
                        ctypes.windll.user32.SendMessageW(hwnd, WM_SETICON, 1, hicon_big)
                    if hicon_sm:
                        ctypes.windll.user32.SendMessageW(hwnd, WM_SETICON, 0, hicon_sm)
        except Exception:
            pass

    port_arg = int(os.environ.get("MINERELAY_PORT", "3080"))
    auto_open = "--no-browser" not in sys.argv

    # If a previous dashboard instance is still holding port_arg, reuse it unless --replace was passed
    replace_existing = "--replace" in sys.argv
    try:
        import socket
        import urllib.request

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.3)
            port_busy = s.connect_ex(("127.0.0.1", port_arg)) == 0

        if port_busy and not replace_existing:
            try:
                req = urllib.request.Request(
                    f"http://127.0.0.1:{port_arg}/api/setup/status",
                    headers={"Host": f"localhost:{port_arg}"},
                )
                with urllib.request.urlopen(req, timeout=1.5) as resp:
                    if resp.status == 200:
                        probe = json.loads(resp.read(8192).decode("utf-8", errors="replace"))
                        if isinstance(probe, dict) and "initialized" in probe:
                            url = f"http://localhost:{port_arg}"
                            print(f"MineRelay dashboard is already running at {url}.")
                            if auto_open:
                                webbrowser.open(url)
                            sys.exit(0)
            except Exception:
                pass

        if port_busy:
            import psutil  # type: ignore

            my_pid = os.getpid()
            my_ppid = os.getppid()
            protected_pids = {my_pid, my_ppid}
            try:
                curr_p = psutil.Process(my_pid)
                for parent in curr_p.parents():
                    protected_pids.add(parent.pid)
            except Exception:
                pass

            for p in psutil.process_iter(["pid", "name", "cmdline"]):
                try:
                    pid = p.info["pid"]
                    if pid in protected_pids:
                        continue
                    pname = (p.info.get("name") or "").lower()
                    cmdline = " ".join(p.info.get("cmdline") or []).lower()
                    if ("python" in pname and ("server.py" in cmdline or "src.server" in cmdline)) or ("minerelay" in pname):
                        p.terminate()
                        p.wait(timeout=8)
                except Exception:
                    pass
    except SystemExit:
        raise
    except Exception:
        pass

    srv = run_server(port=port_arg, open_browser=auto_open)
    _shutdown_done = False
    _shutdown_lock = threading.Lock()

    def _graceful_shutdown(*_args: Any) -> None:
        global _shutdown_done
        with _shutdown_lock:
            if _shutdown_done:
                return
            _shutdown_done = True
            ctx: AppContext = getattr(srv, "app_context", None)  # type: ignore
            if ctx:
                if ctx.mc_manager.local_status != "idle":
                    print("\nSaving Minecraft world to Shared Folder and releasing host lock before exit...")
                    try:
                        ctx.mc_manager.stop_hosting()
                    except Exception as e:
                        print(f"Shutdown sync notice: {e}")
                else:
                    try:
                        ctx.mc_manager._stop_any_runtime_java_processes()
                        ctx.tunnel_manager.stop_tunnel()
                        if ctx.sync_engine.is_locked_by_current_machine():
                            cur_lk = ctx.sync_engine.get_lock()
                            if cur_lk.status != "sync_failed":
                                ctx.sync_engine.release_lock()
                    except Exception:
                        pass

    atexit.register(_graceful_shutdown)
    for sig_name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        sig = getattr(signal, sig_name, None)
        if sig is not None:
            try:
                signal.signal(sig, lambda s, f: (_graceful_shutdown(), sys.exit(0)))
            except Exception:
                pass

    if sys.platform.startswith("win"):
        try:
            import ctypes
            HandlerRoutine = ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.c_uint)

            def _win_ctrl_handler(ctrl_type: int) -> bool:
                if ctrl_type in (0, 1, 2, 5, 6):  # CTRL_C, CTRL_BREAK, CTRL_CLOSE, LOGOFF, SHUTDOWN
                    _graceful_shutdown()
                return False

            _handler_ref = HandlerRoutine(_win_ctrl_handler)
            ctypes.windll.kernel32.SetConsoleCtrlHandler(_handler_ref, True)
        except Exception:
            pass

    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down MineRelay dashboard...")
        _graceful_shutdown()
        srv.server_close()
