import os
import sys
import json
import time
import queue
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

        self.sync_engine = SyncEngine(base_data, event_callback=self.broadcast_event)
        self.auth_service = AuthService(self.sync_engine)
        self.tunnel_manager = TunnelManager(self.sync_engine, event_callback=self.broadcast_event)
        self.mc_manager = MinecraftManager(
            self.sync_engine,
            self.tunnel_manager,
            event_callback=self.broadcast_event,
        )

    def broadcast_event(self, event_type: str, payload: Any) -> None:
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
            if val is None or isinstance(val, bool):
                raise ValueError(f"Field '{field_name}' must be an integer, got {type(val).__name__}.")
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
                body = json.dumps(data).encode("utf-8")
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
            elif isinstance(e, (PermissionDeniedError, PermissionError)):
                self._send_json(403, {"error": str(e), "code": "FORBIDDEN"})
            elif isinstance(e, HostLockConflictError):
                self._send_json(409, {"error": str(e), "code": "HOST_LOCK_CONFLICT", "lock": ctx.sync_engine.get_lock().to_dict()})
            elif isinstance(e, FileNotFoundError):
                self._send_json(404, {"error": str(e), "code": "NOT_FOUND"})
            elif isinstance(e, (ValueError, RuntimeError, OSError, TypeError)):
                self._send_json(400, {"error": str(e), "code": "BAD_REQUEST"})
            else:
                self._send_json(500, {"error": str(e), "code": "INTERNAL_ERROR"})

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            path = parsed.path
            qs = parse_qs(parsed.query)

            if not path.startswith("/api/"):
                return self._serve_static(path)

            try:
                token = self._get_token(parsed)

                if path == "/api/setup/status":
                    cfg = ctx.sync_engine.load_cluster_config()
                    self._send_json(200, {
                        "initialized": ctx.sync_engine.is_cluster_initialized(),
                        "sharedSyncDir": str(ctx.sync_engine.shared_dir),
                        "machineId": ctx.sync_engine.local_config.machine_id,
                        "machineName": ctx.sync_engine.local_config.machine_name,
                        "cloudFolders": ctx.sync_engine.detect_cloud_folders(),
                        "syncDiagnostics": ctx.sync_engine.get_sync_diagnostics(),
                        "serverName": cfg.server_name if cfg else "Shared Minecraft Server",
                    })
                    return

                if path == "/api/auth/me":
                    user = ctx.auth_service.require_auth(token)
                    self._send_json(200, {
                        "account": user.to_public_dict(),
                        "status": ctx.mc_manager.get_status_summary(),
                    })
                    return

                if path == "/api/server/status":
                    user = ctx.auth_service.require_auth(token)
                    summary = ctx.mc_manager.get_status_summary()
                    summary["currentUser"] = user.to_public_dict()
                    summary["logs"] = ctx.mc_manager.console_logs[-120:]
                    if "onlinePlayers" not in summary:
                        lock = ctx.sync_engine.get_lock()
                        with ctx.mc_manager._mutex:
                            mc_online = list(ctx.mc_manager.online_players)
                        summary["onlinePlayers"] = (
                            mc_online
                            if ctx.mc_manager.local_status == "running"
                            else (list(lock.online_players) if lock.is_actively_locked() else [])
                        )
                    self._send_json(200, summary)
                    return

                if path == "/api/players":
                    user = ctx.auth_service.require_auth(token)
                    lock = ctx.sync_engine.get_lock()
                    with ctx.mc_manager._mutex:
                        mc_online = list(ctx.mc_manager.online_players)
                    online_list = (
                        mc_online
                        if ctx.mc_manager.local_status == "running"
                        else (list(lock.online_players) if lock.is_actively_locked() else [])
                    )
                    max_p = lock.max_players if (lock.is_locked or lock.is_actively_locked()) else 20
                    self._send_json(200, {
                        "online": online_list,
                        "count": len(online_list),
                        "max": max_p,
                    })
                    return

                if path == "/api/events":
                    user = ctx.auth_service.require_auth(token)
                    self._handle_sse(user)
                    return

                # ---------------- ADMIN ONLY GET ROUTES ----------------
                if path == "/api/admin/accounts":
                    ctx.auth_service.require_admin(token)
                    cfg = ctx.sync_engine.load_cluster_config()
                    accounts = [a.to_public_dict() for a in (cfg.accounts if cfg else [])]
                    self._send_json(200, {"accounts": accounts})
                    return

                if path == "/api/admin/properties":
                    ctx.auth_service.require_admin(token)
                    self._send_json(200, {
                        "properties": ctx.mc_manager.get_server_properties(),
                        "world": ctx.mc_manager.get_world_info(),
                    })
                    return

                if path == "/api/admin/world":
                    ctx.auth_service.require_admin(token)
                    self._send_json(200, {"world": ctx.mc_manager.get_world_info()})
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
                    name = Path(qs.get("name", [""])[0]).name
                    if not name:
                        raise ValueError("Backup name is required.")
                    bk_path = ctx.sync_engine.backups_dir / name
                    if not bk_path.exists() or not bk_path.is_file():
                        raise FileNotFoundError(f"Backup '{name}' not found.")
                    data_bytes = bk_path.read_bytes()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/zip")
                    self.send_header("Content-Disposition", f'attachment; filename="{name}"')
                    self.send_header("Content-Length", str(len(data_bytes)))
                    self.end_headers()
                    self.wfile.write(data_bytes)
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

            try:
                token = self._get_token(parsed)
                body = self._read_json_body()

                if path == "/api/setup/init":
                    admin_username = str(body.get("adminUsername", "Admin"))
                    server_name = str(body.get("serverName", "Shared Minecraft Server"))
                    custom_passkey = body.get("customPasskey")
                    shared_sync_dir = body.get("sharedSyncDir")
                    machine_name = body.get("machineName")
                    if machine_name:
                        ctx.sync_engine.local_config.machine_name = str(machine_name).strip()
                        ctx.sync_engine.save_local_config()

                    cfg, admin_acc, raw_pk, session_tok = ctx.auth_service.initialize_cluster(
                        admin_username=admin_username,
                        server_name=server_name,
                        custom_admin_passkey=custom_passkey,
                        shared_sync_dir=shared_sync_dir,
                    )
                    self._send_json(200, {
                        "initialized": True,
                        "token": session_tok,
                        "account": admin_acc.to_public_dict(),
                        "generatedPasskey": raw_pk,
                        "cluster": cfg.to_dict(include_secrets=False),
                    })
                    return

                if path == "/api/setup/sync-dir":
                    if ctx.mc_manager.local_status != "idle":
                        raise ValueError("Cannot change Shared Sync Folder while the server is active.")
                    new_dir = str(body.get("sharedSyncDir", "")).strip()
                    machine_name = str(body.get("machineName", "")).strip()
                    if not new_dir:
                        raise ValueError("Shared Sync Folder path is required.")
                    ctx.sync_engine.local_config.shared_sync_dir = new_dir
                    if machine_name:
                        ctx.sync_engine.local_config.machine_name = machine_name
                    ctx.sync_engine.save_local_config()
                    ctx.sync_engine.reset_sync_state()
                    cfg = ctx.sync_engine.load_cluster_config()
                    self._send_json(200, {
                        "sharedSyncDir": str(ctx.sync_engine.shared_dir),
                        "initialized": ctx.sync_engine.is_cluster_initialized(),
                        "syncDiagnostics": ctx.sync_engine.get_sync_diagnostics(),
                        "serverName": cfg.server_name if cfg else None,
                    })
                    return

                if path == "/api/setup/browse-folder":
                    chosen = ctx.sync_engine.browse_folder_dialog()
                    self._send_json(200, {"selectedPath": chosen})
                    return

                if path == "/api/auth/login":
                    passkey = str(body.get("passkey", ""))
                    machine_name = str(body.get("machineName", "")).strip()
                    if machine_name:
                        ctx.sync_engine.local_config.machine_name = machine_name
                        ctx.sync_engine.save_local_config()
                    acc, session_tok = ctx.auth_service.login_with_passkey(passkey)
                    self._send_json(200, {
                        "token": session_tok,
                        "account": acc.to_public_dict(),
                        "status": ctx.mc_manager.get_status_summary(),
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
                    summary = ctx.mc_manager.start_hosting(user)
                    self._send_json(200, summary)
                    return

                if path == "/api/server/stop":
                    user = ctx.auth_service.require_auth(token)
                    summary = ctx.mc_manager.stop_hosting(user)
                    self._send_json(200, summary)
                    return

                if path == "/api/server/restart":
                    user = ctx.auth_service.require_auth(token)
                    summary = ctx.mc_manager.restart_hosting(user)
                    self._send_json(200, summary)
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
                    self._send_json(201, {
                        "account": member.to_public_dict(),
                        "generatedPasskey": raw_pk,
                    })
                    return

                if path == "/api/admin/accounts/rotate":
                    ctx.auth_service.require_admin(token)
                    target_id = str(body.get("userId", ""))
                    acc, new_pk = ctx.auth_service.rotate_passkey(token or "", target_id)
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
                    self._send_json(200, {"account": acc.to_public_dict()})
                    return

                if path == "/api/admin/console":
                    ctx.auth_service.require_admin(token)
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
                    ctx.auth_service.require_admin(token)
                    label = str(body.get("label", "manual"))
                    res = ctx.sync_engine.push_world_from_runtime(create_backup=True, backup_label=label)
                    self._send_json(200, {"result": res, "backups": ctx.sync_engine.list_backups()})
                    return

                if path == "/api/admin/backups/restore":
                    ctx.auth_service.require_admin(token)
                    if ctx.mc_manager.local_status != "idle" or ctx.sync_engine.get_lock().is_actively_locked():
                        raise ValueError("Cannot restore backup while a server is actively running.")
                    name = str(body.get("name", "")).strip()
                    if not name:
                        raise ValueError("Backup name is required.")
                    res = ctx.sync_engine.restore_backup(name)
                    self._send_json(200, res)
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
                    threading.Thread(target=ctx.tunnel_manager.download_playit_binary, daemon=True).start()
                    self._send_json(200, {"started": True})
                    return

                if path == "/api/admin/tunnel/secret":
                    ctx.auth_service.require_admin(token)
                    secret_key = str(body.get("secretKey", ""))
                    ctx.tunnel_manager.save_secret_key(secret_key)
                    self._send_json(200, ctx.tunnel_manager.get_state())
                    return

                if path == "/api/admin/force-unlock":
                    ctx.auth_service.require_admin(token)
                    lock = ctx.sync_engine.force_unlock()
                    ctx.mc_manager._log("Emergency Force-Unlock executed by Admin.", "WARN", "SyncEngine")
                    self._send_json(200, {"unlocked": True, "lock": lock.to_dict()})
                    return

                if path == "/api/admin/world/reset":
                    admin_user = ctx.auth_service.require_admin(token)
                    mode = str(body.get("mode", "random"))
                    seed = str(body.get("seed", ""))
                    level_type = body.get("levelType")
                    generate_structures = body.get("generateStructures")
                    raw_dis_end = body.get("disableEnd") if body.get("disableEnd") is not None else body.get("disable-end")
                    disable_end = bool(raw_dis_end) if raw_dis_end is not None else None
                    backup_existing = bool(body.get("backupExisting", True))
                    res = ctx.mc_manager.generate_new_world(
                        mode=mode,
                        seed=seed,
                        level_type=str(level_type) if level_type is not None else None,
                        generate_structures=bool(generate_structures) if generate_structures is not None else None,
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

            try:
                token = self._get_token(parsed)
                body = self._read_json_body()

                if path == "/api/admin/properties":
                    ctx.auth_service.require_admin(token)
                    updates = body.get("properties", body)
                    if not isinstance(updates, dict):
                        raise ValueError("Properties must be an object.")
                    props = ctx.mc_manager.update_server_properties(updates)
                    self._send_json(200, {
                        "properties": props,
                        "world": ctx.mc_manager.get_world_info(),
                    })
                    return

                if path == "/api/admin/gamerules":
                    ctx.auth_service.require_admin(token)
                    updates = body.get("gamerules", body)
                    if not isinstance(updates, dict):
                        raise ValueError("Gamerules must be an object.")
                    gr = ctx.mc_manager.update_gamerules(updates)
                    self._send_json(200, {"gamerules": gr})
                    return

                if path == "/api/admin/settings":
                    ctx.auth_service.require_admin(token)
                    cfg = ctx.sync_engine.load_cluster_config()
                    if not cfg:
                        raise ValueError("Cluster not initialized.")

                    if "serverName" in body:
                        cfg.server_name = str(body["serverName"]).strip() or cfg.server_name
                    if "mcSoftware" in body:
                        cfg.mc_software = str(body["mcSoftware"]).strip()
                    if "mcVersion" in body:
                        cfg.mc_version = str(body["mcVersion"]).strip()
                    if "memoryMinMb" in body:
                        cfg.memory_min_mb = max(512, self._require_int(body["memoryMinMb"], "memoryMinMb"))
                    if "memoryMaxMb" in body:
                        cfg.memory_max_mb = max(cfg.memory_min_mb, self._require_int(body["memoryMaxMb"], "memoryMaxMb"))
                    if "javaPath" in body:
                        cfg.java_path = str(body["javaPath"]).strip() or "java"
                    if "jvmFlags" in body:
                        cfg.jvm_flags = str(body["jvmFlags"]).strip()
                    if "serverPort" in body:
                        port = self._require_int(body["serverPort"], "serverPort")
                        if not (1 <= port <= 65535):
                            raise ValueError("Server port must be between 1 and 65535.")
                        cfg.server_port = port
                        if ctx.mc_manager.local_status == "idle":
                            try:
                                ctx.mc_manager.update_server_properties({"server-port": str(port)})
                            except Exception:
                                pass
                    if "tunnelEnabled" in body:
                        cfg.tunnel_enabled = bool(body["tunnelEnabled"])
                    if "tunnelProvider" in body:
                        cfg.tunnel_provider = str(body["tunnelProvider"]).strip()
                    if "publicJoinAddress" in body:
                        cfg.public_join_address = str(body["publicJoinAddress"]).strip()
                    if "gitSyncEnabled" in body:
                        cfg.git_sync_enabled = bool(body["gitSyncEnabled"])
                    if "autoBackupOnStop" in body:
                        cfg.auto_backup_on_stop = bool(body["autoBackupOnStop"])
                    if "maxBackups" in body:
                        cfg.max_backups = max(1, self._require_int(body["maxBackups"], "maxBackups"))
                    if "idleShutdownMinutes" in body:
                        cfg.idle_shutdown_minutes = max(0, self._require_int(body["idleShutdownMinutes"], "idleShutdownMinutes"))

                    ctx.sync_engine.save_cluster_config(cfg)

                    if "machineName" in body and str(body["machineName"]).strip():
                        ctx.sync_engine.local_config.machine_name = str(body["machineName"]).strip()
                        ctx.sync_engine.save_local_config()

                    # If active lock is held locally, update publicJoinAddress immediately
                    if ctx.sync_engine.is_locked_by_current_machine() and "publicJoinAddress" in body:
                        ctx.sync_engine.update_lock_state(public_address=cfg.public_join_address)

                    self._send_json(200, {
                        "cluster": cfg.to_dict(include_secrets=False),
                        "status": ctx.mc_manager.get_status_summary(),
                    })
                    return

                self._send_json(404, {"error": f"Unknown endpoint {path}"})
            except Exception as e:
                self._handle_exception(e)

        def do_DELETE(self) -> None:
            parsed = urlparse(self.path)
            path = parsed.path
            qs = parse_qs(parsed.query)

            try:
                token = self._get_token(parsed)

                if path.startswith("/api/admin/accounts/"):
                    target_id = unquote(path.split("/api/admin/accounts/", 1)[1])
                    ctx.auth_service.delete_member_account(token or "", target_id)
                    cfg = ctx.sync_engine.load_cluster_config()
                    self._send_json(200, {
                        "deleted": True,
                        "accounts": [a.to_public_dict() for a in (cfg.accounts if cfg else [])],
                    })
                    return

                if path.startswith("/api/admin/backups/"):
                    ctx.auth_service.require_admin(token)
                    name = unquote(path.split("/api/admin/backups/", 1)[1])
                    ctx.sync_engine.delete_backup(name)
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

        def _handle_sse(self, user: Any) -> None:
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
                init_payload = json.dumps({
                    "event": "connected",
                    "data": ctx.mc_manager.get_status_summary(),
                })
                self.wfile.write(f"data: {init_payload}\n\n".encode("utf-8"))
                self.wfile.flush()

                while True:
                    try:
                        msg = client_q.get(timeout=15)
                        self.wfile.write(f"data: {msg}\n\n".encode("utf-8"))
                        self.wfile.flush()
                    except queue.Empty:
                        # Send keepalive ping and latest lock state from shared folder
                        ping_payload = json.dumps({
                            "event": "heartbeat",
                            "data": {"lock": ctx.sync_engine.get_lock().to_dict()},
                        })
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
            clean = req_path.lstrip("/") or "index.html"
            base_pub = public_dir.resolve()
            target = (base_pub / clean).resolve()
            if not target.is_relative_to(base_pub) or not target.exists() or target.is_dir():
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
            self.wfile.write(content)

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

    # If a previous dashboard instance is still holding port_arg, terminate it cleanly first
    try:
        import socket
        import psutil  # type: ignore

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(0.3)
            port_busy = s.connect_ex(("127.0.0.1", port_arg)) == 0

        if port_busy:
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
                    if ("python" in pname and "server.py" in cmdline) or ("minerelay" in pname):
                        p.terminate()
                        p.wait(timeout=3)
                except Exception:
                    pass
    except Exception:
        pass

    srv = run_server(port=port_arg, open_browser=auto_open)
    _shutdown_done = False

    def _graceful_shutdown(*_args: Any) -> None:
        global _shutdown_done
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
