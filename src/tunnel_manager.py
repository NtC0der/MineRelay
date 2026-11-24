import os
import re
import sys
import time
import socket
import threading
import subprocess
import urllib.request
from pathlib import Path
from typing import Optional, Dict, Any, Callable, List

try:
    import psutil  # type: ignore
except ImportError:
    psutil = None

from src.sync_engine import SyncEngine
from src.bootstrap_runtime import _download_to_file_verified, _os_replace_retry


PLAYIT_MIN_VERSION = "0.17.1"
PLAYIT_WINDOWS_URL = f"https://github.com/playit-cloud/playit-agent/releases/download/v{PLAYIT_MIN_VERSION}/playit-windows-x86_64-signed.exe"
PLAYIT_LINUX_URL = f"https://github.com/playit-cloud/playit-agent/releases/download/v{PLAYIT_MIN_VERSION}/playit-linux-amd64"


class TunnelManager:
    """
    Manages Zero-Port-Forwarding connectivity via playit.gg (and local LAN discovery).
    Stores the playit.toml secret inside the Shared Sync Folder so whichever member
    hosts the server broadcasts on the exact same public Minecraft IP/domain!
    """

    def __init__(
        self,
        sync_engine: SyncEngine,
        event_callback: Optional[Callable[[str, Any], None]] = None,
    ):
        self.sync_engine = sync_engine
        self.event_callback = event_callback
        self.bin_dir = self.sync_engine.base_data_dir / "bin"
        self.bin_dir.mkdir(parents=True, exist_ok=True)

        self._proc: Optional[subprocess.Popen] = None
        self._claim_exchange_proc: Optional[subprocess.Popen] = None
        self._claim_init_started: bool = False
        self._mutex = threading.RLock()
        self._download_lock = threading.Lock()
        self._query_lock = threading.Lock()
        self._checked_binary_upgrade: bool = False
        self._last_tunnels_query_at: float = 0.0
        self._last_cached_tunnels_addr: Optional[str] = None
        self._last_rate_limit_log_at: float = 0.0
        self._rate_limit_suppressed: int = 0
        self.status: str = "stopped"  # 'stopped' | 'starting' | 'claim_required' | 'online' | 'relay_ready' | 'error'
        self.public_address: str = ""
        self.claim_url: str = ""
        self.logs: List[str] = []
        self.is_downloading: bool = False
        self.download_progress: int = 0

    @property
    def playit_binary_path(self) -> Path:
        exe_name = "playit.exe" if sys.platform.startswith("win") else "playit"
        return self.bin_dir / exe_name

    @property
    def shared_secret_toml_path(self) -> Path:
        return self.sync_engine.shared_tunnel_dir / "playit.toml"

    def _emit(self, event_type: str, payload: Any) -> None:
        if self.event_callback:
            try:
                self.event_callback(event_type, payload)
            except Exception:
                pass

    def _append_log(self, line: str) -> None:
        clean = line.strip()
        if not clean:
            return
        ts = time.strftime("%H:%M:%S")
        entry = f"[{ts}] [Tunnel] {clean}"
        with self._mutex:
            self.logs.append(entry)
            if len(self.logs) > 200:
                self.logs = self.logs[-200:]
        self._emit("tunnel_log", {"line": entry})

    def get_lan_ip(self) -> str:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(("8.8.8.8", 80))
                return s.getsockname()[0]
        except Exception:
            try:
                return socket.gethostbyname(socket.gethostname())
            except Exception:
                return "127.0.0.1"

    def is_binary_installed(self) -> bool:
        if self.playit_binary_path.exists():
            return True
        # Check system PATH as well
        from shutil import which
        return which("playit") is not None or which("playit.exe") is not None

    def resolve_binary(self) -> Optional[str]:
        if self.playit_binary_path.exists():
            return str(self.playit_binary_path)
        from shutil import which
        return which("playit") or which("playit.exe")

    @staticmethod
    def _parse_version_tuple(version_str: str) -> Optional[tuple]:
        m = re.search(r"(\d+)\.(\d+)\.(\d+)", str(version_str or ""))
        if not m:
            return None
        return (int(m.group(1)), int(m.group(2)), int(m.group(3)))

    def _get_installed_binary_version(self, bin_path: str) -> str:
        try:
            creationflags = subprocess.CREATE_NO_WINDOW if sys.platform.startswith("win") else 0
            res = subprocess.run(
                [bin_path, "version"],
                capture_output=True,
                stdin=subprocess.DEVNULL,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=4,
                creationflags=creationflags,
            )
            out = (res.stdout or res.stderr or "").strip()
            return out.splitlines()[0].strip() if out else ""
        except Exception:
            return ""

    def _ensure_up_to_date_binary(self) -> Optional[str]:
        """Auto-downloads playit.exe if missing, or upgrades cached versions older than PLAYIT_MIN_VERSION."""
        bin_path = self.resolve_binary()
        if os.environ.get("MINERELAY_TEST_MODE") == "1":
            return bin_path

        if not bin_path:
            try:
                self._append_log("playit.gg agent not found locally; auto-downloading for Zero-Port-Forwarding...")
                self.download_playit_binary()
                self._checked_binary_upgrade = True
                return self.resolve_binary()
            except Exception as dl_err:
                self._append_log(f"Auto-download of playit.gg agent skipped: {dl_err}")
                return None

        if (
            not self._checked_binary_upgrade
            and self.playit_binary_path.exists()
            and os.path.normcase(bin_path) == os.path.normcase(str(self.playit_binary_path))
        ):
            self._checked_binary_upgrade = True
            installed_ver = self._get_installed_binary_version(bin_path)
            installed_tuple = self._parse_version_tuple(installed_ver)
            min_tuple = self._parse_version_tuple(PLAYIT_MIN_VERSION) or (0, 17, 1)
            if installed_tuple is not None and installed_tuple < min_tuple:
                try:
                    self._append_log(
                        f"Upgrading outdated playit.gg agent ({installed_ver} -> {PLAYIT_MIN_VERSION}) to fix API rate-limit errors..."
                    )
                    self.download_playit_binary()
                    return self.resolve_binary()
                except Exception as up_err:
                    self._append_log(f"playit.gg agent upgrade skipped ({up_err}); using existing {installed_ver}.")

        return bin_path

    def download_playit_binary(self) -> Dict[str, Any]:
        """Downloads the official playit.gg standalone binary into ./data/bin/."""
        with self._mutex:
            running = self._proc is not None and self._proc.poll() is None
        if running:
            raise RuntimeError("Cannot download or update playit.gg binary while tunnel is running. Stop the server first.")
        with self._download_lock:
            if self.is_downloading:
                return {"status": "downloading", "progress": self.download_progress}
            self.is_downloading = True
            self.download_progress = 0

        url = PLAYIT_WINDOWS_URL if sys.platform.startswith("win") else PLAYIT_LINUX_URL
        self._append_log(f"Downloading official playit.gg agent from {url} ...")

        tmp_target = self.playit_binary_path.with_suffix(f".tmp.{os.getpid()}.{threading.get_ident()}")

        def _on_progress(pct: int, _downloaded: int, _total: int) -> None:
            self.download_progress = pct

        try:
            _download_to_file_verified(
                url,
                tmp_target,
                timeout=60,
                attempts=3,
                progress_cb=_on_progress,
            )
            _os_replace_retry(tmp_target, self.playit_binary_path)
            if not sys.platform.startswith("win"):
                os.chmod(self.playit_binary_path, 0o755)
            self.download_progress = 100
            self._append_log("playit.gg binary downloaded and ready.")
            return {"installed": True, "path": str(self.playit_binary_path)}
        except Exception as e:
            self._append_log(f"Failed to download playit.gg binary: {e}")
            raise RuntimeError(f"Could not download playit binary: {e}")
        finally:
            with self._download_lock:
                self.is_downloading = False
            try:
                if tmp_target.exists():
                    tmp_target.unlink(missing_ok=True)
            except OSError:
                pass
            self._emit("tunnel_updated", self.get_state())

    def save_secret_key(self, secret_key: str) -> None:
        """Saves a playit.gg secret key to the shared folder so all members share the tunnel."""
        clean = str(secret_key or "").strip()
        if any(ord(ch) < 32 and ch not in ("\r", "\n", "\t") for ch in clean):
            raise ValueError("Invalid control characters in tunnel secret key.")
        self.sync_engine._ensure_shared_dirs()
        if not clean:
            if self.shared_secret_toml_path.exists():
                self.shared_secret_toml_path.unlink()
            with self._mutex:
                ex_proc = self._claim_exchange_proc
                self._claim_exchange_proc = None
                self._claim_init_started = False
                self.claim_url = ""
                if self.status not in ("online", "starting", "disabled"):
                    self.status = "stopped"
            if ex_proc is not None:
                try:
                    ex_proc.terminate()
                except Exception:
                    pass
            self._append_log("Cleared shared playit.gg tunnel secret key from Shared Sync Folder.")
            return

        if "secret_key" in clean:
            m = re.match(r'^secret_key\s*=\s*"([A-Za-z0-9_=-]{16,256})"\s*$', clean)
            if not m:
                raise ValueError("Invalid playit.toml secret_key format.")
            toml_text = f'secret_key = "{m.group(1)}"\n'
        else:
            if "\n" in clean or "\r" in clean or not re.fullmatch(r"[A-Za-z0-9_=-]{16,256}", clean):
                raise ValueError("Invalid playit.gg secret key format.")
            toml_text = f'secret_key = "{clean}"\n'

        tmp_toml = self.shared_secret_toml_path.with_suffix(f".tmp.{os.getpid()}.{threading.get_ident()}")
        try:
            tmp_toml.write_text(toml_text, encoding="utf-8")
            _os_replace_retry(tmp_toml, self.shared_secret_toml_path)
        finally:
            try:
                if tmp_toml.exists():
                    tmp_toml.unlink(missing_ok=True)
            except OSError:
                pass

        with self._mutex:
            self.claim_url = ""
            is_running = self._proc is not None
        if is_running:
            self._append_log("Saved shared playit.gg tunnel secret key (restart server to apply to active tunnel).")
        else:
            self._append_log("Saved shared playit.gg tunnel secret key to Shared Sync Folder.")

    def has_shared_secret(self) -> bool:
        try:
            return self.shared_secret_toml_path.exists() and self.shared_secret_toml_path.stat().st_size > 5
        except OSError:
            return False

    def start_tunnel(self, server_port: int = 25565) -> Dict[str, Any]:
        """
        Starts the zero-port-forwarding tunnel when a member starts hosting.
        If a static publicJoinAddress is configured in ClusterConfig, advertises it immediately.
        If playit binary is installed and tunnel_provider is 'playit', launches playit with the shared playit.toml config.
        """
        cfg = self.sync_engine.load_cluster_config()
        disabled = False
        lan = f"{self.get_lan_ip()}:{server_port}"
        with self._mutex:
            self._stop_tunnel_internal(emit_stopped=False)
            if cfg and not cfg.tunnel_enabled:
                self.status = "disabled"
                self.claim_url = ""
                self.public_address = lan
                disabled = True
        if disabled:
            self.sync_engine.update_lock_state(
                public_address=lan,
                tunnel_status="disabled",
            )
            state = self.get_state()
            self._emit("tunnel_updated", state)
            return state

        provider = str(getattr(cfg, "tunnel_provider", "playit") or "playit").strip().lower() if cfg else "playit"
        use_playit = (provider == "playit")

        # Download/upgrade outside self._mutex so download_playit_binary's lock order stays consistent
        bin_path = self._ensure_up_to_date_binary() if use_playit else None

        launch_lock_addr = ""
        launch_lock_status = ""
        need_claim_flow = False
        early_return = False

        with self._mutex:
            cfg = self.sync_engine.load_cluster_config()
            configured_addr = cfg.public_join_address.strip() if cfg and cfg.public_join_address else ""
            lan_addr = f"{self.get_lan_ip()}:{server_port}"
            if use_playit and not bin_path:
                bin_path = self.resolve_binary()

            if use_playit and bin_path and os.environ.get("MINERELAY_TEST_MODE") != "1":
                try:
                    self.sync_engine._ensure_shared_dirs()
                    if not self.has_shared_secret() and sys.platform.startswith("win"):
                        local_appdata = os.environ.get("LOCALAPPDATA", "")
                        if local_appdata:
                            default_local_toml = Path(local_appdata) / "playit_gg" / "playit.toml"
                            if default_local_toml.exists() and default_local_toml.stat().st_size > 5:
                                import shutil
                                shutil.copy2(default_local_toml, self.shared_secret_toml_path)
                                self._append_log("Imported existing playit.toml secret into Shared Sync Folder.")

                    if not self.has_shared_secret():
                        self.status = "claim_required"
                        self.public_address = configured_addr or lan_addr
                        need_claim_flow = True
                        launch_lock_addr = self.public_address
                        launch_lock_status = "online" if configured_addr else "claim_required"
                        early_return = True
                    else:
                        self.status = "starting"
                        self.claim_url = ""
                        self.public_address = configured_addr or lan_addr
                        self._launch_playit_daemon(bin_path, server_port)
                        launch_lock_addr = self.public_address
                        launch_lock_status = "online" if configured_addr else "starting"
                        early_return = True
                except Exception as e:
                    self._append_log(f"Could not spawn playit binary ({e}); falling back to LAN address.")

            if not early_return:
                # Configured Address or Local LAN Fallback
                if configured_addr:
                    self.public_address = configured_addr
                    self.status = "online"
                    self._append_log(f"Tunnel active using shared cluster address: {configured_addr}")
                else:
                    self.public_address = lan_addr
                    self.status = "lan_only"
                    self._append_log(
                        f"Server reachable on Local/LAN IP: {self.public_address} (or localhost:{server_port})."
                    )
                launch_lock_addr = self.public_address
                launch_lock_status = self.status

        if need_claim_flow:
            self._ensure_claim_flow_async(server_port=server_port)
        self.sync_engine.update_lock_state(
            public_address=launch_lock_addr,
            tunnel_status=launch_lock_status,
        )
        state = self.get_state()
        if not early_return or need_claim_flow:
            self._emit("tunnel_updated", state)
        return state

    @staticmethod
    def is_private_lan_address(addr: str) -> bool:
        host = (addr or "").strip().lower().split(":")[0]
        if not host or host in ("localhost", "127.0.0.1", "0.0.0.0"):
            return True
        if host.startswith("192.168.") or host.startswith("10.") or host.startswith("127."):
            return True
        if host.startswith("172."):
            parts = host.split(".")
            if len(parts) >= 2 and parts[1].isdigit() and 16 <= int(parts[1]) <= 31:
                return True
        return False

    def _query_playit_tunnels_list(self, bin_path: Optional[str], min_interval_sec: float = 8.0) -> Optional[str]:
        if not bin_path or not self.shared_secret_toml_path.exists():
            return None
        with self._query_lock:
            now = time.monotonic()
            if min_interval_sec > 0 and (now - self._last_tunnels_query_at) < min_interval_sec:
                return self._last_cached_tunnels_addr
            self._last_tunnels_query_at = now
            try:
                ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
                addr_regex = re.compile(
                    r"([a-zA-Z0-9.-]+\.(?:gl\.joinmc\.link|ply\.gg|joinmc\.link)(?::\d+)?)"
                )
                creationflags = subprocess.CREATE_NO_WINDOW if sys.platform.startswith("win") else 0
                res = subprocess.run(
                    [bin_path, "--secret_path", str(self.shared_secret_toml_path), "-s", "tunnels", "list"],
                    capture_output=True,
                    stdin=subprocess.DEVNULL,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=10,
                    creationflags=creationflags,
                )
                for raw in (res.stdout or "").splitlines():
                    clean = ansi_escape.sub("", raw).strip()
                    if "playit_cli::ui:" in clean:
                        clean = clean.split("playit_cli::ui:", 1)[1].strip()
                    m = addr_regex.search(clean)
                    if m:
                        self._last_cached_tunnels_addr = m.group(1).strip()
                        return self._last_cached_tunnels_addr
                    parts = clean.split()
                    if len(parts) >= 4 and "." in parts[3] and "TooManyRequests" not in clean:
                        self._last_cached_tunnels_addr = parts[3].strip()
                        return self._last_cached_tunnels_addr
            except Exception:
                pass
            return None

    def _launch_playit_daemon(self, bin_path: str, server_port: int) -> None:
        with self._mutex:
            if self._proc is not None and self._proc.poll() is None:
                return
            cmd = [
                bin_path,
                "--secret_path",
                str(self.shared_secret_toml_path),
                "-s",
                "start",
            ]
            self._append_log(f"Starting playit.gg tunnel agent (shared config: {self.shared_secret_toml_path.name})")
            creationflags = subprocess.CREATE_NO_WINDOW if sys.platform.startswith("win") else 0
            self._proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                creationflags=creationflags,
            )
            threading.Thread(
                target=self._monitor_playit_output,
                args=(self._proc, server_port, bin_path),
                daemon=True,
                name="PlayitOutputReader",
            ).start()

    def _ensure_claim_flow_async(self, server_port: int = 25565) -> None:
        if os.environ.get("MINERELAY_TEST_MODE") == "1":
            return
        with self._mutex:
            if self._claim_init_started or self.has_shared_secret() or self.claim_url:
                return
            bin_path = self.resolve_binary()
            if not bin_path:
                return
            self._claim_init_started = True

        def _worker() -> None:
            try:
                self.sync_engine._ensure_shared_dirs()
                if not self.has_shared_secret() and sys.platform.startswith("win"):
                    local_appdata = os.environ.get("LOCALAPPDATA", "")
                    if local_appdata:
                        default_local_toml = Path(local_appdata) / "playit_gg" / "playit.toml"
                        if default_local_toml.exists() and default_local_toml.stat().st_size > 5:
                            import shutil
                            shutil.copy2(default_local_toml, self.shared_secret_toml_path)
                            self._append_log("Imported existing playit.toml secret into Shared Sync Folder.")
                            discovered = self._query_playit_tunnels_list(bin_path)
                            if self.sync_engine.is_locked_by_current_machine():
                                self._launch_playit_daemon(bin_path, server_port)
                            if discovered:
                                self._apply_discovered_public_address(discovered)
                            else:
                                with self._mutex:
                                    self.claim_url = "https://playit.gg/account/tunnels"
                                    self.status = "claim_required"
                                if self.sync_engine.is_locked_by_current_machine():
                                    self.sync_engine.update_lock_state(tunnel_status="claim_required")
                                self._emit("tunnel_updated", self.get_state())
                            return

                ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
                creationflags = subprocess.CREATE_NO_WINDOW if sys.platform.startswith("win") else 0
                gen_res = subprocess.run(
                    [bin_path, "-s", "claim", "generate"],
                    capture_output=True,
                    stdin=subprocess.DEVNULL,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=10,
                    creationflags=creationflags,
                )
                code = ""
                for line in (gen_res.stdout or "").splitlines():
                    clean = ansi_escape.sub("", line).strip()
                    if "playit_cli::ui:" in clean:
                        clean = clean.split("playit_cli::ui:", 1)[1].strip()
                    if re.fullmatch(r"[0-9a-fA-F]{8,16}", clean):
                        code = clean
                        break
                if not code:
                    tokens = ansi_escape.sub("", gen_res.stdout or "").strip().split()
                    if tokens and re.fullmatch(r"[0-9a-fA-F]{8,16}", tokens[-1]):
                        code = tokens[-1]

                if not code:
                    with self._mutex:
                        self._claim_init_started = False
                    return

                with self._mutex:
                    self.claim_url = f"https://playit.gg/claim/{code}"
                    if self.status in ("stopped", "lan_only", "starting"):
                        self.status = "claim_required"
                self._append_log(f"Generated playit.gg claim link: {self.claim_url}")
                if self.sync_engine.is_locked_by_current_machine():
                    self.sync_engine.update_lock_state(tunnel_status="claim_required")
                self._emit("tunnel_updated", self.get_state())

                # Wait for user to click the claim link and exchange it for secret_key (cancellable via stop_tunnel)
                ex_proc = subprocess.Popen(
                    [bin_path, "-s", "claim", "exchange", code, "--wait", "600"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    stdin=subprocess.DEVNULL,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    creationflags=creationflags,
                )
                with self._mutex:
                    self._claim_exchange_proc = ex_proc
                ex_stdout = ""
                try:
                    ex_stdout, _ = ex_proc.communicate(timeout=610)
                except subprocess.TimeoutExpired:
                    ex_proc.kill()
                    try:
                        ex_stdout, _ = ex_proc.communicate(timeout=2)
                    except Exception:
                        pass
                finally:
                    if ex_proc.stdout is not None:
                        try:
                            ex_proc.stdout.close()
                        except OSError:
                            pass
                    with self._mutex:
                        if self._claim_exchange_proc is ex_proc:
                            self._claim_exchange_proc = None

                secret_key = ""
                if ex_proc.returncode == 0 or ex_stdout:
                    for line in (ex_stdout or "").splitlines():
                        clean = ansi_escape.sub("", line).strip()
                        if "playit_cli::ui:" in clean:
                            clean = clean.split("playit_cli::ui:", 1)[1].strip()
                        if re.fullmatch(r"[0-9a-fA-F]{32,128}", clean):
                            secret_key = clean
                            break
                if secret_key:
                    self.save_secret_key(secret_key)
                    try:
                        subprocess.run(
                            [
                                bin_path,
                                "--secret_path",
                                str(self.shared_secret_toml_path),
                                "-s",
                                "tunnels",
                                "prepare",
                                "tcp",
                                "1",
                                "--type",
                                "minecraft-java",
                                "--name",
                                "MineRelay",
                            ],
                            capture_output=True,
                            stdin=subprocess.DEVNULL,
                            text=True,
                            encoding="utf-8",
                            errors="replace",
                            timeout=15,
                            creationflags=creationflags,
                        )
                    except Exception:
                        pass
                    if not self.sync_engine.is_locked_by_current_machine():
                        with self._mutex:
                            self._claim_init_started = False
                        self._emit("tunnel_updated", self.get_state())
                        return
                    with self._mutex:
                        self.status = "starting"
                    self._launch_playit_daemon(bin_path, server_port)
                    discovered = self._query_playit_tunnels_list(bin_path)
                    if discovered:
                        self._apply_discovered_public_address(discovered)
                    else:
                        with self._mutex:
                            self.claim_url = "https://playit.gg/account/tunnels"
                            self.status = "claim_required"
                        if self.sync_engine.is_locked_by_current_machine():
                            self.sync_engine.update_lock_state(tunnel_status="claim_required")
                        self._emit("tunnel_updated", self.get_state())
                else:
                    with self._mutex:
                        self._claim_init_started = False
            except Exception:
                with self._mutex:
                    self._claim_init_started = False

        threading.Thread(target=_worker, daemon=True, name="PlayitClaimFlow").start()

    def _apply_discovered_public_address(self, found_addr: str) -> None:
        if not found_addr:
            return
        with self.sync_engine._config_mutex:
            cfg = self.sync_engine.load_cluster_config()
            if cfg and not cfg.tunnel_enabled:
                return
            custom_addr = cfg.public_join_address.strip() if (cfg and cfg.public_join_address) else ""
            is_custom_override = bool(
                cfg
                and cfg.tunnel_provider in ("custom", "none")
                and (custom_addr or cfg.tunnel_provider == "custom")
            )
            effective_addr = (custom_addr or found_addr) if is_custom_override else found_addr
            if cfg and cfg.tunnel_provider == "playit" and cfg.public_join_address != found_addr:
                cfg.public_join_address = found_addr
                self.sync_engine.save_cluster_config(cfg)
        with self._mutex:
            self.public_address = effective_addr
            self.status = "online"
            self.claim_url = ""
        if self.sync_engine.is_locked_by_current_machine():
            self.sync_engine.update_lock_state(
                public_address=effective_addr,
                tunnel_status="online",
            )
        self._emit("tunnel_updated", self.get_state())

    def _monitor_playit_output(
        self,
        proc: subprocess.Popen,
        server_port: int,
        bin_path: Optional[str] = None,
    ) -> None:
        ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
        claim_regex = re.compile(r"(https://playit\.gg/claim/[a-zA-Z0-9_-]+)")
        addr_regex = re.compile(
            r"([a-zA-Z0-9.-]+\.(?:gl\.joinmc\.link|ply\.gg|joinmc\.link)(?::\d+)?)"
        )
        arrow_addr_regex = re.compile(r"([a-zA-Z0-9.:-]+)\s*=>\s*(?:127\.0\.0\.1|localhost|0\.0\.0\.0)")

        if not proc.stdout:
            return

        effective_bin = bin_path or self.resolve_binary()

        def _poll_until_public() -> None:
            delay = 10.0
            attempts = 0
            while attempts < 12:
                with self._mutex:
                    is_cur = self._proc is proc
                    cur_pub = self.public_address
                if not is_cur or proc.poll() is not None or not self.is_private_lan_address(cur_pub):
                    break
                if self.has_shared_secret():
                    discovered = self._query_playit_tunnels_list(effective_bin, min_interval_sec=8.0)
                    if discovered:
                        self._append_log(f"Public playit.gg tunnel active: {discovered}")
                        self._apply_discovered_public_address(discovered)
                        break
                    attempts += 1
                    delay = min(60.0, delay * 1.5)
                slept = 0.0
                while slept < delay:
                    with self._mutex:
                        if self._proc is not proc:
                            return
                    if proc.poll() is not None:
                        return
                    time.sleep(1.0)
                    slept += 1.0

        threading.Thread(target=_poll_until_public, daemon=True, name="PlayitTunnelListPoller").start()

        try:
            for raw_line in proc.stdout:
                line = ansi_escape.sub("", raw_line).strip()
                if not line:
                    continue
                low = line.lower()
                if "toomanyrequests" in low or "reqpingget" in low:
                    now_mono = time.monotonic()
                    if (now_mono - self._last_rate_limit_log_at) >= 60.0:
                        suffix = (
                            f" (suppressed {self._rate_limit_suppressed} repeated rate-limit messages)"
                            if self._rate_limit_suppressed > 0
                            else ""
                        )
                        self._last_rate_limit_log_at = now_mono
                        self._rate_limit_suppressed = 0
                        self._append_log(f"{line}{suffix}")
                    else:
                        self._rate_limit_suppressed += 1
                    continue
                self._append_log(line)

                claim_match = claim_regex.search(line)
                if claim_match:
                    with self._mutex:
                        self.claim_url = claim_match.group(1)
                        self.status = "claim_required"
                    if self.sync_engine.is_locked_by_current_machine():
                        self.sync_engine.update_lock_state(tunnel_status="claim_required")
                    self._emit("tunnel_updated", self.get_state())

                if "tunnel running" in low or "tunnels registered" in low:
                    with self._mutex:
                        cur_pub = self.public_address
                        cur_status = self.status
                    discovered = (
                        self._query_playit_tunnels_list(effective_bin, min_interval_sec=8.0)
                        if self.is_private_lan_address(cur_pub)
                        else cur_pub
                    )
                    if discovered and not self.is_private_lan_address(discovered):
                        if discovered != cur_pub or cur_status != "online":
                            self._apply_discovered_public_address(discovered)
                    elif "0 tunnels registered" in low:
                        with self._mutex:
                            self.claim_url = "https://playit.gg/account/tunnels"
                            self.status = "claim_required"
                        self._append_log(
                            "Agent claimed, but 0 tunnels are configured on playit.gg! Open https://playit.gg/account/tunnels and add a 'Minecraft Java' tunnel (local port 25565)."
                        )
                        if self.sync_engine.is_locked_by_current_machine():
                            self.sync_engine.update_lock_state(tunnel_status="claim_required")
                        self._emit("tunnel_updated", self.get_state())
                    elif cur_status != "online":
                        with self._mutex:
                            self.status = "online"
                            self.claim_url = ""
                        if self.sync_engine.is_locked_by_current_machine():
                            self.sync_engine.update_lock_state(tunnel_status="online")
                        self._emit("tunnel_updated", self.get_state())

                addr_match = addr_regex.search(line) or arrow_addr_regex.search(line)
                if addr_match:
                    found_addr = addr_match.group(1).strip()
                    self._apply_discovered_public_address(found_addr)
        finally:
            try:
                proc.stdout.close()
            except OSError:
                pass

        should_emit_stopped = False
        with self._mutex:
            if self._proc is proc:
                self._proc = None
                if self.status not in ("stopped", "disabled"):
                    self.status = "stopped"
                    should_emit_stopped = True
        if should_emit_stopped:
            if self.sync_engine.is_locked_by_current_machine():
                self.sync_engine.update_lock_state(tunnel_status="stopped")
            self._emit("tunnel_updated", self.get_state())

    def _kill_orphaned_playit_processes(self) -> None:
        if psutil is None or os.environ.get("MINERELAY_TEST_MODE") == "1":
            return
        bin_norm = str(self.playit_binary_path.resolve()).lower()
        secret_norm = str(self.shared_secret_toml_path.resolve()).lower()
        for p in psutil.process_iter(["pid", "name", "cmdline", "exe"]):
            try:
                pname = (p.info.get("name") or "").lower()
                if "playit" not in pname:
                    continue
                cmdline = " ".join(p.info.get("cmdline") or []).lower()
                if "claim" in cmdline or "tunnels" in cmdline:
                    continue
                exe_path = (p.info.get("exe") or "").lower()
                if bin_norm in exe_path or bin_norm in cmdline or secret_norm in cmdline:
                    p.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
                pass

    def _stop_tunnel_internal(self, emit_stopped: bool = True) -> Dict[str, Any]:
        with self._mutex:
            for proc_attr in ("_proc", "_claim_exchange_proc"):
                proc = getattr(self, proc_attr, None)
                if proc is not None:
                    try:
                        proc.terminate()
                        proc.wait(timeout=3)
                    except Exception:
                        try:
                            proc.kill()
                        except Exception:
                            pass
                    setattr(self, proc_attr, None)
                    if proc_attr == "_claim_exchange_proc":
                        self._claim_init_started = False
            self._kill_orphaned_playit_processes()
            self.public_address = ""
            if self.has_shared_secret():
                self.status = "stopped"
                self.claim_url = ""
            else:
                self.status = "claim_required" if self.claim_url else "stopped"
            state = self.get_state()
        if emit_stopped:
            self._emit("tunnel_updated", state)
        return state

    def stop_tunnel(self) -> Dict[str, Any]:
        return self._stop_tunnel_internal(emit_stopped=True)

    def get_state(self) -> Dict[str, Any]:
        cfg = self.sync_engine.load_cluster_config()
        with self._mutex:
            pub_addr = self.public_address or (cfg.public_join_address if cfg else "")
            logs_copy = list(self.logs[-60:])
            status = self.status
            claim_url = self.claim_url
        return {
            "status": status,
            "publicAddress": pub_addr,
            "claimUrl": claim_url,
            "binaryInstalled": self.is_binary_installed(),
            "binaryPath": self.resolve_binary() or str(self.playit_binary_path),
            "hasSharedSecret": self.has_shared_secret(),
            "sharedSecretPath": str(self.shared_secret_toml_path),
            "lanIp": self.get_lan_ip(),
            "isDownloading": self.is_downloading,
            "downloadProgress": self.download_progress,
            "logs": logs_copy,
        }
