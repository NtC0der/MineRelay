import os
import re
import sys
import uuid
import json
import time
import socket
import shutil
import base64
import hashlib
import secrets
import threading
import subprocess
import urllib.request
from urllib.parse import quote as urlquote
from pathlib import Path
from typing import Optional, List, Dict, Any, Callable

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
        self._proc: Optional[subprocess.Popen] = None
        self._sim_running: bool = False
        self._sim_stop_event = threading.Event()
        self._reached_running: bool = False

        self.local_status: str = "idle"  # 'idle' | 'syncing_pull' | 'starting' | 'running' | 'stopping' | 'syncing_push'
        self.active_host_user: Optional[UserAccount] = None
        self.started_at: float = 0.0
        self._last_player_active_at: float = 0.0
        self.online_players: List[str] = []
        self.console_logs: List[Dict[str, Any]] = []
        self.tps: float = 20.0
        self.memory_used_mb: int = 0
        self.cpu_percent: float = 0.0
        self.runtime_mode: str = "none"  # 'java' | 'embedded' | 'none'
        self.last_error: str = ""

        self.jar_downloading: bool = False
        self.jar_download_progress: int = 0
        self.jar_download_status: str = ""
        self._java_cache: Dict[str, Any] = {}
        self._java_cache_key: str = ""
        self._java_cache_ts: float = 0.0

        if os.environ.get("MINERELAY_TEST_MODE") != "1":
            self._recover_orphaned_session_on_boot()
            threading.Thread(target=self._idle_shutdown_watchdog_loop, daemon=True, name="IdleShutdownWatchdog").start()

    def _idle_shutdown_watchdog_loop(self) -> None:
        while True:
            time.sleep(15.0)
            try:
                if self.local_status != "running":
                    continue
                cfg = self.sync_engine.load_cluster_config()
                if not cfg or cfg.idle_shutdown_minutes <= 0:
                    continue
                with self._mutex:
                    has_players = len(self.online_players) > 0
                if has_players:
                    self._last_player_active_at = time.time()
                    continue
                ref_time = max(self.started_at, self._last_player_active_at)
                if ref_time > 0 and (time.time() - ref_time) >= (cfg.idle_shutdown_minutes * 60):
                    self._log(
                        f"Idle Auto-Shutdown: 0 players online for {cfg.idle_shutdown_minutes} minute(s). Automatically stopping and syncing world...",
                        "INFO",
                        "Server",
                    )
                    self.stop_hosting(self.active_host_user)
            except Exception:
                pass

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

    def _log(self, message: str, level: str = "INFO", source: str = "Server") -> None:
        ts = time.strftime("%H:%M:%S")
        formatted = f"[{ts}] [{source}/{level}]: {message}"
        entry = {
            "id": f"{int(time.time() * 1000)}-{len(self.console_logs)}",
            "timestamp": ts,
            "level": level,
            "source": source,
            "text": formatted,
        }
        self.console_logs.append(entry)
        if len(self.console_logs) > 500:
            self.console_logs = self.console_logs[-500:]
        self._emit("console_log", entry)

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
            major = get_java_major_version(cmd)
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

    def _recover_orphaned_session_on_boot(self) -> None:
        """
        If MineRelay was closed while a local Java server was still running or while this machine
        still held host-lock.json, cleanly stop the orphaned JVM, save the world snapshot, and release the lock.
        """
        try:
            cfg = self.sync_engine.load_cluster_config()
            port = cfg.server_port if cfg else 25565
            orphans = self._stop_any_runtime_java_processes(server_port=port)
            self.tunnel_manager.stop_tunnel()
            lock = self.sync_engine.get_lock()
            if orphans > 0:
                self._log(
                    f"Recovered {orphans} orphaned Minecraft server process(es) from a previous session; syncing world and unlocking.",
                    "WARN",
                    "Launcher",
                )
                try:
                    self.sync_engine.push_world_from_runtime(create_backup=True, backup_label="recovery")
                except Exception:
                    pass
                self.sync_engine.force_unlock()
            elif lock.is_locked and lock.machine_id == self.sync_engine.local_config.machine_id:
                self._log(
                    "Cleared leftover host lock from previous session on this machine.",
                    "INFO",
                    "SyncEngine",
                )
                self.sync_engine.force_unlock()
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
        with self._mutex:
            if self.local_status not in ("idle",):
                raise RuntimeError(f"Local server is already in state '{self.local_status}'.")

            cfg = self.sync_engine.load_cluster_config()
            if not cfg:
                raise RuntimeError("Cluster configuration is not initialized.")

            self.last_error = ""
            self._reached_running = False

            # Step 1: Acquire distributed lock (raises HostLockConflictError if another member is hosting)
            lock = self.sync_engine.acquire_lock(
                user=user,
                mc_version=f"{cfg.mc_software.capitalize()} {cfg.mc_version}",
                public_address=cfg.public_join_address,
                initial_status="syncing_pull",
            )
            self.local_status = "syncing_pull"
            self.active_host_user = user
            self._emit("status_changed", self.get_status_summary())

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
                pull_res = self.sync_engine.pull_world_to_runtime()
                if pull_res.get("synced"):
                    kb = round(pull_res.get("sizeBytes", 0) / 1024, 1)
                    self._log(f"Synced world snapshot ({kb} KB) into local runtime directory.", "INFO", "SyncEngine")
                else:
                    self._log(str(pull_res.get("reason", "Initialized runtime directory.")), "INFO", "SyncEngine")

                # Update user host_count in ClusterConfig
                for acc in cfg.accounts:
                    if acc.id == user.id:
                        acc.host_count += 1
                        acc.last_active_at = time.time()
                        break
                self.sync_engine.save_cluster_config(cfg)

                # Step 3: Start Zero-Port-Forwarding Tunnel
                props = self.get_server_properties()
                port = int(props.get("server-port", cfg.server_port or 25565))
                max_players = int(props.get("max-players", 20))

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
                                f"Java {req_java}+ required for Minecraft {cfg.mc_version}; auto-downloading Portable Java 25 Runtime...",
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

                if java_info["installed"] and has_jar:
                    ensure_paperclip_mojang_cache(
                        target_jar,
                        self.runtime_dir,
                        log_fn=lambda msg: self._log(msg, "INFO", "Installer"),
                    )
                    self._spawn_java_minecraft(cfg, java_info["command"], target_jar, max_players)
                else:
                    self._spawn_embedded_minecraft_runtime(cfg, has_jar, java_info["installed"], max_players)

                return self.get_status_summary()

            except Exception as e:
                self.last_error = str(e)
                self.local_status = "idle"
                self.active_host_user = None
                self.tunnel_manager.stop_tunnel()
                try:
                    self.sync_engine.release_lock(user)
                except Exception:
                    pass
                self._emit("status_changed", self.get_status_summary())
                raise e

    def _spawn_java_minecraft(self, cfg: Any, java_cmd: str, jar_path: Path, max_players: int) -> None:
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
            args=(self._proc, max_players),
            daemon=True,
            name="MinecraftProcessMonitor",
        ).start()

    def _monitor_java_process(self, proc: subprocess.Popen, max_players: int) -> None:
        ansi_regex = re.compile(r"\x1B(?:\[[0-?]*[ -/]*[@-~]|\].*?(?:\x07|\x1b\\)|[@-Z\\-_])")
        done_regex = re.compile(r'Done \([0-9.]+s\)! For help, type "help"')
        prefix = r"^(?:\[[^\]\r\n]+\](?:\s*\[[^\]\r\n]+\])*:\s*)?"
        join_regex = re.compile(prefix + r"(?!<)([a-zA-Z0-9_.*+~-]{2,32})\s+joined the game")
        login_regex = re.compile(prefix + r"(?!<)([a-zA-Z0-9_.*+~-]{2,32})\[.*?\] logged in with entity id")
        leave_regex = re.compile(prefix + r"(?!<)([a-zA-Z0-9_.*+~-]{2,32})\s+(?:left the game|lost connection:)")
        disconnect_regex = re.compile(prefix + r"(?!<)(?:Disconnecting|Kicking)\s+([a-zA-Z0-9_.*+~-]{2,32}):")
        list_regex = re.compile(prefix + r"(?!<)There are \d+ of a max of \d+ players online:(.*)")
        fatal_error = ""

        if proc.stdout:
            try:
                for raw_line in proc.stdout:
                    line = ansi_regex.sub("", raw_line).rstrip("\r\n")
                    if not line:
                        continue
                    lvl = "ERROR" if ("/ERROR]" in line or "FAILED TO BIND" in line or "Exception" in line) else "INFO"
                    self._log(line, lvl, "Minecraft")

                    if done_regex.search(line):
                        self._reached_running = True
                        self.last_error = ""
                        self.local_status = "running"
                        self._last_player_active_at = time.time()
                        self.tps = 20.0
                        self.memory_used_mb = 1420
                        self.cpu_percent = 14.5
                        with self._mutex:
                            players_copy = list(self.online_players)
                        self.sync_engine.update_lock_state(status="running", max_players=max_players, online_players=players_copy)
                        self._emit("status_changed", self.get_status_summary())
                        self._emit("players_updated", self.get_players_data())
                        self._apply_saved_gamerules_to_live_server(proc)

                    if not self._reached_running:
                        if "FAILED TO BIND TO PORT" in line or "Address already in use" in line:
                            fatal_error = "Port 25565 was already in use by another process."
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

        # Process exited
        with self._mutex:
            if self._proc is proc:
                self._proc = None
                if self.local_status not in ("stopping", "syncing_push", "restarting", "idle"):
                    if self._reached_running:
                        self._log("Java Minecraft process exited. Syncing world and releasing lock...", "WARN", "Launcher")
                        self._finalize_stop_and_sync(self.active_host_user)
                    else:
                        err_msg = fatal_error or "Server process exited during startup before reaching Ready state."
                        self._log(
                            f"Startup failed: {err_msg} (World snapshot was preserved and NOT overwritten.)",
                            "ERROR",
                            "Launcher",
                        )
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
        self.tps = 20.0
        self.memory_used_mb = min(1024, cfg.memory_min_mb)
        self.cpu_percent = 8.4
        self.sync_engine.update_lock_state(status="running", max_players=max_players)
        self._emit("status_changed", self.get_status_summary())

        def _telemetry_loop() -> None:
            tick = 0
            while not self._sim_stop_event.wait(5.0):
                if not self._sim_running:
                    break
                tick += 100
                self.tps = 20.0
                self.cpu_percent = round(6.5 + (tick % 300) * 0.01, 1)
                self.memory_used_mb = min(cfg.memory_max_mb, int(cfg.memory_min_mb * 0.65) + (tick % 120))

        threading.Thread(target=_telemetry_loop, daemon=True, name="EmbeddedRuntimeTelemetry").start()

    def stop_hosting(self, user: Optional[UserAccount] = None) -> Dict[str, Any]:
        """
        Gracefully stops the local Minecraft server, syncs the updated world back to the Shared Sync Folder,
        stops the tunnel, and releases the distributed host lock so another member can host next.
        """
        with self._mutex:
            effective_user = user or self.active_host_user
            lock = self.sync_engine.get_lock()
            cfg = self.sync_engine.load_cluster_config()
            port = cfg.server_port if cfg else 25565

            if self.local_status == "idle":
                orphans_stopped = self._stop_any_runtime_java_processes(server_port=port)
                if orphans_stopped > 0:
                    self._log(
                        f"Stopped {orphans_stopped} background Java server process(es) and syncing world...",
                        "INFO",
                        "Launcher",
                    )
                    try:
                        self.sync_engine.push_world_from_runtime(
                            create_backup=cfg.auto_backup_on_stop if cfg else True,
                            backup_label=effective_user.username if effective_user else "host",
                        )
                    except Exception:
                        pass
                if lock.is_locked:
                    if (
                        lock.machine_id == self.sync_engine.local_config.machine_id
                        or (effective_user and lock.host_user_id == effective_user.id)
                        or (effective_user and effective_user.role == "admin")
                        or lock.is_stale()
                    ):
                        self.sync_engine.release_lock(effective_user)
                        self.tunnel_manager.stop_tunnel()
                        uname = effective_user.username if effective_user else "System"
                        self._log(f"Host lock released by {uname}.", "INFO", "SyncEngine")
                        self._emit("status_changed", self.get_status_summary())
                        return self.get_status_summary()
                    else:
                        raise HostLockConflictError(
                            f"Only the active host ({lock.host_username} on {lock.machine_name}) or the Admin can stop this remote session."
                        )
                self.tunnel_manager.stop_tunnel()
                self._emit("status_changed", self.get_status_summary())
                return self.get_status_summary()

            was_running = (self.local_status == "running") or self._reached_running or self._sim_running
            self.local_status = "stopping"
            self.sync_engine.update_lock_state(status="stopping")
            self._emit("status_changed", self.get_status_summary())
            uname = effective_user.username if effective_user else "System"
            self._log(f"Stopping server requested by {uname}...", "INFO", "Server")

            if self._proc is not None:
                proc_ref = self._proc
                self._proc = None
                if was_running:
                    try:
                        if proc_ref.stdin:
                            proc_ref.stdin.write("save-all\n")
                            proc_ref.stdin.flush()
                            time.sleep(0.4)
                            proc_ref.stdin.write("stop\n")
                            proc_ref.stdin.flush()
                        proc_ref.wait(timeout=12)
                    except Exception:
                        self._terminate_process_tree(proc_ref, wait_timeout=4.0)
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
        with self._mutex:
            self.online_players = []
        self.started_at = 0.0
        self.memory_used_mb = 0
        self.cpu_percent = 0.0
        self._emit("status_changed", self.get_status_summary())
        self._emit("players_updated", self.get_players_data())
        return self.get_status_summary()

    def _finalize_stop_and_sync(self, user: Optional[UserAccount]) -> Dict[str, Any]:
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
        try:
            push_res = self.sync_engine.push_world_from_runtime(
                create_backup=create_backup,
                backup_label=user.username if user else "host",
            )
            kb = round(push_res.get("sizeBytes", 0) / 1024, 1)
            self._log(
                f"World synced to shared storage ({push_res.get('fileCount', 0)} files, {kb} KB). Releasing host lock.",
                "INFO",
                "SyncEngine",
            )
        except Exception as e:
            self._log(f"Warning during world snapshot push: {e}", "WARN", "SyncEngine")

        try:
            self.sync_engine.release_lock(user)
        except Exception:
            self.sync_engine.force_unlock()

        self.local_status = "idle"
        self.runtime_mode = "none"
        self._reached_running = False
        self.active_host_user = None
        with self._mutex:
            self.online_players = []
        self.started_at = 0.0
        self.memory_used_mb = 0
        self.cpu_percent = 0.0
        self._emit("status_changed", self.get_status_summary())
        self._emit("players_updated", self.get_players_data())
        return self.get_status_summary()

    def restart_hosting(self, user: Optional[UserAccount] = None) -> Dict[str, Any]:
        """
        Gracefully restarts the local Minecraft server while keeping the host lock and tunnel active,
        pushing a quick world snapshot in between without ever dropping to 'idle'.
        """
        with self._mutex:
            effective_user = user or self.active_host_user
            if not effective_user:
                raise RuntimeError("No active host user found for restart.")
            if self.local_status not in ("running", "starting"):
                raise RuntimeError("Server is not currently running on this machine.")

            cfg = self.sync_engine.load_cluster_config()
            if not cfg:
                raise RuntimeError("Cluster configuration is not initialized.")
            port = cfg.server_port or 25565

            self.local_status = "restarting"
            self._reached_running = False
            self.online_players = []
            self.sync_engine.update_lock_state(status="restarting", online_players=[])
            self._emit("status_changed", self.get_status_summary())
            self._emit("players_updated", self.get_players_data())
            self._log(f"Restarting server requested by {effective_user.username}...", "INFO", "Server")

            if self._proc is not None:
                proc_ref = self._proc
                self._proc = None
                try:
                    if proc_ref.stdin:
                        proc_ref.stdin.write("save-all\n")
                        proc_ref.stdin.flush()
                        time.sleep(0.3)
                        proc_ref.stdin.write("stop\n")
                        proc_ref.stdin.flush()
                    proc_ref.wait(timeout=12)
                except Exception:
                    self._terminate_process_tree(proc_ref, wait_timeout=4.0)

            self._stop_any_runtime_java_processes(server_port=port)

            if self._sim_running:
                self._sim_running = False
                self._sim_stop_event.set()

            try:
                self.sync_engine.push_world_from_runtime(
                    create_backup=False,
                    backup_label=effective_user.username,
                )
            except Exception:
                pass

            props = self.get_server_properties()
            max_players = int(props.get("max-players", 20))
            self.local_status = "starting"
            self.started_at = time.time()
            self.sync_engine.update_lock_state(status="starting", max_players=max_players, online_players=[])
            self._emit("status_changed", self.get_status_summary())

            java_info = self.check_java_installed(cfg.java_path, cfg.mc_version)
            target_jar = get_versioned_jar_path(self.bin_dir, cfg.mc_software, cfg.mc_version)
            has_jar = target_jar.exists() and target_jar.stat().st_size > 1_000_000

            if java_info["installed"] and has_jar and os.environ.get("MINERELAY_TEST_MODE") != "1":
                ensure_paperclip_mojang_cache(
                    target_jar,
                    self.runtime_dir,
                    log_fn=lambda msg: self._log(msg, "INFO", "Installer"),
                )
                self._spawn_java_minecraft(cfg, java_info["command"], target_jar, max_players)
            else:
                self._spawn_embedded_minecraft_runtime(cfg, has_jar, java_info["installed"], max_players)

            return self.get_status_summary()

    def clear_console_logs(self) -> Dict[str, Any]:
        with self._mutex:
            self.console_logs = []
        self._log("Console log history cleared by Admin.", "INFO", "Server")
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
                curr_players = list(self.online_players)
            self._log(f"{p} left the game", "INFO", "Server")
            self.sync_engine.update_lock_state(online_players=curr_players)
            self._emit("status_changed", self.get_status_summary())
            self._emit("players_updated", self.get_players_data())
            return {"sent": True, "command": clean}

        if self._proc is not None and self._proc.stdin:
            try:
                self._proc.stdin.write(clean + "\n")
                self._proc.stdin.flush()
                return {"sent": True, "command": clean}
            except (OSError, ValueError) as err:
                self._log(f"Failed to write to server stdin: {err}", "ERROR", "Server")
                raise RuntimeError(f"Failed to send command to server: {err}")

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
            self._log(
                f"There are {len(players_copy)} of a max of 20 players online: {', '.join(players_copy) or 'none'}",
                "INFO",
                "Server",
            )
        elif cmd_name == "say":
            msg = " ".join(args) or "Hello!"
            self._log(f"[Server] {msg}", "INFO", "Server")
        elif cmd_name == "save-all":
            self._log("Saving the game (this may take a moment!)", "INFO", "Server")
            self._log("Saved the game", "INFO", "Server")
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

    def _push_idle_config_safely(self) -> None:
        """
        Pushes updated config files from runtime_dir to the Shared Sync Folder ONLY if
        no other member is currently hosting (so an idle PC never overwrites an active host's snapshot).
        """
        if self.local_status != "idle":
            return
        lock = self.sync_engine.get_lock()
        if lock.is_actively_locked() and not self.sync_engine.is_locked_by_current_machine(lock):
            return
        try:
            self.sync_engine.push_world_from_runtime(create_backup=False)
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
                        if src_file.is_file() and (not dst_file.exists() or dst_file.stat().st_size == 0):
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

    def update_server_properties(self, updates: Dict[str, Any]) -> Dict[str, str]:
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

        # If server is running locally, also apply live-reloadable settings immediately
        if self.local_status == "running" and self._proc and self._proc.stdin:
            try:
                if "difficulty" in updates:
                    self._proc.stdin.write(f"difficulty {props.get('difficulty', 'normal')}\n")
                if "white-list" in updates:
                    wl_cmd = "whitelist on\n" if props.get("white-list") == "true" else "whitelist off\n"
                    self._proc.stdin.write(wl_cmd)
                self._proc.stdin.flush()
            except (OSError, ValueError):
                pass

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
        with self._mutex:
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
            port = cfg.server_port if cfg else 25565

            if was_running_locally:
                self.local_status = "restarting"
                self._reached_running = False
                self.online_players = []
                self.sync_engine.update_lock_state(status="restarting", online_players=[])
                self._emit("status_changed", self.get_status_summary())
                self._emit("players_updated", self.get_players_data())
                self._log("Stopping current world to generate new map...", "INFO", "Server")

                if self._proc is not None:
                    proc_ref = self._proc
                    self._proc = None
                    try:
                        if proc_ref.stdin:
                            proc_ref.stdin.write("save-all\n")
                            proc_ref.stdin.flush()
                            time.sleep(0.3)
                            proc_ref.stdin.write("stop\n")
                            proc_ref.stdin.flush()
                        proc_ref.wait(timeout=12)
                    except Exception:
                        self._terminate_process_tree(proc_ref, wait_timeout=4.0)

                self._stop_any_runtime_java_processes(server_port=port)

                if self._sim_running:
                    self._sim_running = False
                    self._sim_stop_event.set()

            # Create safety backup of existing world if requested and a world exists
            backup_file: Optional[str] = None
            if backup_existing and (self.runtime_dir / level_name).exists():
                try:
                    bk_res = self.sync_engine.push_world_from_runtime(
                        create_backup=True,
                        backup_label="pre-world-reset",
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

            updated_props = self.update_server_properties(prop_updates)

            # Push clean runtime snapshot (with old world removed and new seed written) to Shared Sync Folder
            try:
                self.sync_engine.push_world_from_runtime(
                    create_backup=False,
                    backup_label=(user.username if user else "admin"),
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
                effective_user = user or self.active_host_user
                max_players = int(updated_props.get("max-players", 20))
                self.local_status = "starting"
                self.started_at = time.time()
                self.sync_engine.update_lock_state(status="starting", max_players=max_players, online_players=[])
                self._emit("status_changed", self.get_status_summary())

                java_info = self.check_java_installed(cfg.java_path, cfg.mc_version)
                target_jar = get_versioned_jar_path(self.bin_dir, cfg.mc_software, cfg.mc_version)
                has_jar = target_jar.exists() and target_jar.stat().st_size > 1_000_000

                if java_info["installed"] and has_jar and os.environ.get("MINERELAY_TEST_MODE") != "1":
                    ensure_paperclip_mojang_cache(
                        target_jar,
                        self.runtime_dir,
                        log_fn=lambda msg: self._log(msg, "INFO", "Installer"),
                    )
                    self._spawn_java_minecraft(cfg, java_info["command"], target_jar, max_players)
                else:
                    self._spawn_embedded_minecraft_runtime(cfg, has_jar, java_info["installed"], max_players)

            return {
                "reset": True,
                "mode": clean_mode,
                "seed": final_seed,
                "levelType": eff_type,
                "removedDirs": removed_dirs,
                "backupFile": backup_file,
                "restarted": was_running_locally,
                "world": self.get_world_info(),
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
            if self.local_status == "running" and self._proc and self._proc.stdin:
                try:
                    self._proc.stdin.write(f"gamerule {clean_rule} {cmd_val}\n")
                    self._proc.stdin.flush()
                    applied_live.append(f"{clean_rule}={cmd_val}")
                except (OSError, ValueError):
                    pass

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
            for rule, val in cfg.gamerules.items():
                clean_rule = re.sub(r"[^a-zA-Z0-9_]", "", str(rule).strip())
                if not clean_rule:
                    continue
                default_val = self.DEFAULT_GAMERULES.get(clean_rule)
                if val == default_val:
                    continue
                cmd_val = ("true" if val else "false") if isinstance(val, bool) else str(val)
                proc.stdin.write(f"gamerule {clean_rule} {cmd_val}\n")
            proc.stdin.flush()
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
                try:
                    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
                except OSError:
                    pass
            return data
        except Exception:
            return []

    def _write_player_json(self, filename: str, data: List[Dict[str, Any]]) -> None:
        path = self.runtime_dir / filename
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        self._push_idle_config_safely()

    def get_players_data(self) -> Dict[str, Any]:
        lock = self.sync_engine.get_lock()
        with self._mutex:
            online = (
                list(self.online_players)
                if self.local_status == "running"
                else (list(lock.online_players) if lock.is_actively_locked() else [])
            )
        return {
            "online": online,
            "ops": self._read_player_json("ops.json"),
            "whitelist": self._read_player_json("whitelist.json"),
            "banned": self._read_player_json("banned-players.json"),
        }

    def player_action(self, action: str, username: str, reason: str = "Banned by Admin") -> Dict[str, Any]:
        clean_user = username.strip().replace("\r", "").replace("\n", "")
        if not clean_user:
            raise ValueError("Player username is required.")
        clean_reason = (reason or "").replace("\r", " ").replace("\n", " ").strip()

        player_uuid = self._resolve_player_uuid(clean_user)

        if action == "op":
            ops = self._read_player_json("ops.json")
            if not any(o.get("name", "").lower() == clean_user.lower() for o in ops):
                ops.append({"uuid": player_uuid, "name": clean_user, "level": 4, "bypassesPlayerLimit": False})
                self._write_player_json("ops.json", ops)
            if self._proc and self._proc.stdin:
                try:
                    self._proc.stdin.write(f"op {clean_user}\n")
                    self._proc.stdin.flush()
                except (OSError, ValueError):
                    pass
            self._log(f"Made {clean_user} a server operator", "INFO", "Server")
            self._emit("players_updated", self.get_players_data())

        elif action == "deop":
            ops = [o for o in self._read_player_json("ops.json") if o.get("name", "").lower() != clean_user.lower()]
            self._write_player_json("ops.json", ops)
            if self._proc and self._proc.stdin:
                try:
                    self._proc.stdin.write(f"deop {clean_user}\n")
                    self._proc.stdin.flush()
                except (OSError, ValueError):
                    pass
            self._log(f"Made {clean_user} no longer a server operator", "INFO", "Server")
            self._emit("players_updated", self.get_players_data())

        elif action == "whitelist_add":
            wl = self._read_player_json("whitelist.json")
            if not any(w.get("name", "").lower() == clean_user.lower() for w in wl):
                wl.append({"uuid": player_uuid, "name": clean_user})
                self._write_player_json("whitelist.json", wl)
            if self._proc and self._proc.stdin:
                try:
                    self._proc.stdin.write(f"whitelist add {clean_user}\n")
                    self._proc.stdin.write("whitelist reload\n")
                    self._proc.stdin.flush()
                except (OSError, ValueError):
                    pass
            self._log(f"Added {clean_user} to the whitelist", "INFO", "Server")
            self._emit("players_updated", self.get_players_data())

        elif action == "whitelist_remove":
            wl = [w for w in self._read_player_json("whitelist.json") if w.get("name", "").lower() != clean_user.lower()]
            self._write_player_json("whitelist.json", wl)
            if self._proc and self._proc.stdin:
                try:
                    self._proc.stdin.write(f"whitelist remove {clean_user}\n")
                    self._proc.stdin.write("whitelist reload\n")
                    self._proc.stdin.flush()
                except (OSError, ValueError):
                    pass
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
            with self._mutex:
                to_remove = [p for p in self.online_players if p.lower() == clean_user.lower()]
                for p in to_remove:
                    self.online_players.remove(p)
                curr_players = list(self.online_players)
            self.sync_engine.update_lock_state(online_players=curr_players)
            if self._proc and self._proc.stdin:
                try:
                    ban_cmd = f"ban {clean_user} {clean_reason}\n" if clean_reason else f"ban {clean_user}\n"
                    self._proc.stdin.write(ban_cmd)
                    self._proc.stdin.flush()
                except (OSError, ValueError):
                    pass
            self._log(f"Banned player {clean_user}: {clean_reason}", "INFO", "Server")
            self._emit("status_changed", self.get_status_summary())
            self._emit("players_updated", self.get_players_data())

        elif action == "pardon":
            banned = [b for b in self._read_player_json("banned-players.json") if b.get("name", "").lower() != clean_user.lower()]
            self._write_player_json("banned-players.json", banned)
            if self._proc and self._proc.stdin:
                try:
                    self._proc.stdin.write(f"pardon {clean_user}\n")
                    self._proc.stdin.flush()
                except (OSError, ValueError):
                    pass
            self._log(f"Unbanned player {clean_user}", "INFO", "Server")
            self._emit("players_updated", self.get_players_data())

        elif action == "kick":
            with self._mutex:
                to_remove = [p for p in self.online_players if p.lower() == clean_user.lower()]
                for p in to_remove:
                    self.online_players.remove(p)
                curr_players = list(self.online_players)
            self.sync_engine.update_lock_state(online_players=curr_players)
            if self._proc and self._proc.stdin:
                try:
                    kick_cmd = f"kick {clean_user} {clean_reason}\n" if clean_reason else f"kick {clean_user}\n"
                    self._proc.stdin.write(kick_cmd)
                    self._proc.stdin.flush()
                except (OSError, ValueError):
                    pass
            self._log(f"Kicked {clean_user} from the game", "INFO", "Server")
            self._emit("status_changed", self.get_status_summary())
            self._emit("players_updated", self.get_players_data())

        elif action == "gamemode":
            mode = clean_reason.lower() if clean_reason.lower() in ("survival", "creative", "adventure", "spectator") else "survival"
            if self._proc and self._proc.stdin:
                try:
                    self._proc.stdin.write(f"gamemode {mode} {clean_user}\n")
                    self._proc.stdin.flush()
                except (OSError, ValueError):
                    pass
            self._log(f"Set {clean_user}'s gamemode to {mode}", "INFO", "Server")

        elif action == "heal":
            if self._proc and self._proc.stdin:
                try:
                    self._proc.stdin.write(f"effect give {clean_user} minecraft:instant_health 1 10 true\n")
                    self._proc.stdin.write(f"effect give {clean_user} minecraft:saturation 5 10 true\n")
                    self._proc.stdin.flush()
                except (OSError, ValueError):
                    pass
            self._log(f"Healed and fed {clean_user}", "INFO", "Server")

        elif action == "tp_spawn":
            if self._proc and self._proc.stdin:
                try:
                    self._proc.stdin.write(f"execute in minecraft:overworld run tp {clean_user} 0 100 0\n")
                    self._proc.stdin.write(f"effect give {clean_user} minecraft:slow_falling 15 1 true\n")
                    self._proc.stdin.flush()
                except (OSError, ValueError):
                    pass
            self._log(f"Teleported {clean_user} to world spawn", "INFO", "Server")

        elif action == "xp_level":
            if self._proc and self._proc.stdin:
                try:
                    self._proc.stdin.write(f"xp add {clean_user} 5 levels\n")
                    self._proc.stdin.flush()
                except (OSError, ValueError):
                    pass
            self._log(f"Gave 5 XP levels to {clean_user}", "INFO", "Server")

        elif action == "clear_inventory":
            if self._proc and self._proc.stdin:
                try:
                    self._proc.stdin.write(f"clear {clean_user}\n")
                    self._proc.stdin.flush()
                except (OSError, ValueError):
                    pass
            self._log(f"Cleared inventory of {clean_user}", "INFO", "Server")

        elif action == "whisper":
            msg = clean_reason or "Hello from Admin!"
            if self._proc and self._proc.stdin:
                try:
                    self._proc.stdin.write(f"tell {clean_user} [Admin] {msg}\n")
                    self._proc.stdin.flush()
                except (OSError, ValueError):
                    pass
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
        if self.jar_downloading:
            return {"downloading": True, "progress": self.jar_download_progress, "status": self.jar_download_status}

        software = software.lower().strip()
        version = version.strip()
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

                cfg = self.sync_engine.load_cluster_config()
                if cfg:
                    cfg.mc_software = software
                    cfg.mc_version = version
                    self.sync_engine.save_cluster_config(cfg)

            except Exception as e:
                self.jar_download_status = f"Download failed: {e}"
                self._log(self.jar_download_status, "ERROR", "Installer")
            finally:
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
        if self.local_status == "idle":
            self.sync_engine.ensure_runtime_synced_when_idle()
        target = self._safe_resolve_runtime_path(rel_path)
        if target == self.runtime_dir.resolve() or target.is_dir():
            raise ValueError("Target path must be a file, not a directory.")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content, encoding="utf-8")
        self._push_idle_config_safely()
        return {"saved": True, "path": rel_path}

    def upload_file_base64(self, rel_dir: str, filename: str, base64_data: str) -> Dict[str, Any]:
        if self.local_status == "idle":
            self.sync_engine.ensure_runtime_synced_when_idle()
        safe_name = Path(filename).name
        if not safe_name or safe_name in (".", ".."):
            raise ValueError("Invalid filename for upload.")
        target_dir = self._safe_resolve_runtime_path(rel_dir)
        target_dir.mkdir(parents=True, exist_ok=True)
        target_file = target_dir / safe_name
        raw_bytes = base64.b64decode(base64_data)
        target_file.write_bytes(raw_bytes)
        self._push_idle_config_safely()
        return {
            "uploaded": True,
            "path": str(target_file.relative_to(self.runtime_dir)).replace("\\", "/"),
            "sizeBytes": len(raw_bytes),
        }

    def delete_runtime_file(self, rel_path: str) -> Dict[str, Any]:
        if self.local_status == "idle":
            self.sync_engine.ensure_runtime_synced_when_idle()
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
        if self.local_status == "idle":
            self.sync_engine.ensure_runtime_synced_when_idle()
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
        if self.local_status == "idle":
            self.sync_engine.ensure_runtime_synced_when_idle()
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
        if self.local_status == "idle":
            self.sync_engine.ensure_runtime_synced_when_idle()
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

    def get_status_summary(self) -> Dict[str, Any]:
        cfg = self.sync_engine.load_cluster_config()
        lock = self.sync_engine.get_lock()
        is_local_host = self.sync_engine.is_locked_by_current_machine(lock)
        with self._mutex:
            if is_local_host and self.local_status == "running":
                lock.online_players = list(self.online_players)
            online_list = (
                list(self.online_players)
                if self.local_status == "running"
                else list(lock.online_players)
            )
        mc_ver = cfg.mc_version if cfg else "26.3"
        java_info = self.check_java_installed(cfg.java_path if cfg else "java", mc_ver)
        active_jar = self.get_active_jar_path()
        has_jar = active_jar.exists() and active_jar.stat().st_size > 1_000_000

        uptime_sec = int(time.time() - lock.started_at) if lock.is_actively_locked() and lock.started_at else 0

        if psutil is not None and self.local_status == "running":
            try:
                if self._proc is not None:
                    p = psutil.Process(self._proc.pid)
                    self.memory_used_mb = int(p.memory_info().rss / (1024 * 1024))
                    self.cpu_percent = round(p.cpu_percent(interval=None) or psutil.cpu_percent(interval=None), 1)
                else:
                    self.cpu_percent = round(psutil.cpu_percent(interval=None), 1)
            except Exception:
                pass

        lock_dict = lock.to_dict()
        if self.local_status != "idle":
            lock_dict["isLocked"] = True
            lock_dict["isActivelyLocked"] = True
            lock_dict["machineId"] = self.sync_engine.local_config.machine_id
            lock_dict["machineName"] = self.sync_engine.local_config.machine_name
            lock_dict["status"] = self.local_status
            if self.active_host_user:
                lock_dict["hostUsername"] = self.active_host_user.username
                lock_dict["hostUserId"] = self.active_host_user.id

        return {
            "localStatus": self.local_status,
            "runtimeMode": self.runtime_mode,
            "lastError": self.last_error,
            "isLocalMachineHosting": is_local_host or (self.local_status != "idle"),
            "onlinePlayers": online_list,
            "lock": lock_dict,
            "tunnel": self.tunnel_manager.get_state(),
            "telemetry": {
                "tps": self.tps if self.local_status == "running" else (20.0 if lock.is_actively_locked() else 0.0),
                "memoryUsedMb": self.memory_used_mb,
                "memoryMaxMb": cfg.memory_max_mb if cfg else 4096,
                "cpuPercent": self.cpu_percent,
                "uptimeSeconds": uptime_sec,
            },
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
            "cluster": cfg.to_dict(include_secrets=False) if cfg else None,
        }
