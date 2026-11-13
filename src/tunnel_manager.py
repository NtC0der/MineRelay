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


PLAYIT_WINDOWS_URL = "https://github.com/playit-cloud/playit-agent/releases/download/v0.15.26/playit-windows-x86_64-signed.exe"
PLAYIT_LINUX_URL = "https://github.com/playit-cloud/playit-agent/releases/latest/download/playit-linux-amd64"


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

    def download_playit_binary(self) -> Dict[str, Any]:
        """Downloads the official playit.gg standalone binary into ./data/bin/."""
        with self._download_lock:
            if self.is_downloading:
                return {"status": "downloading", "progress": self.download_progress}
            self.is_downloading = True
            self.download_progress = 0

        url = PLAYIT_WINDOWS_URL if sys.platform.startswith("win") else PLAYIT_LINUX_URL
        self._append_log(f"Downloading official playit.gg agent from {url} ...")

        tmp_target = self.playit_binary_path.with_suffix(f".tmp.{os.getpid()}.{threading.get_ident()}")
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "MineRelay/1.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                total = int(resp.headers.get("Content-Length", 0))
                downloaded = 0
                with open(tmp_target, "wb") as out:
                    while True:
                        chunk = resp.read(65536)
                        if not chunk:
                            break
                        out.write(chunk)
                        downloaded += len(chunk)
                        if total > 0:
                            self.download_progress = int((downloaded / total) * 100)
            os.replace(tmp_target, self.playit_binary_path)
            if not sys.platform.startswith("win"):
                os.chmod(self.playit_binary_path, 0o755)
            self.download_progress = 100
            self._append_log("playit.gg binary downloaded and ready.")
            return {"installed": True, "path": str(self.playit_binary_path)}
        except Exception as e:
            self._append_log(f"Failed to download playit.gg binary: {e}")
            raise RuntimeError(f"Could not download playit binary: {e}")
        finally:
            self.is_downloading = False
            try:
                if tmp_target.exists():
                    tmp_target.unlink(missing_ok=True)
            except OSError:
                pass

    def save_secret_key(self, secret_key: str) -> None:
        """Saves a playit.gg secret key to the shared folder so all members share the tunnel."""
        clean = secret_key.strip()
        self.sync_engine._ensure_shared_dirs()
        if not clean:
            if self.shared_secret_toml_path.exists():
                self.shared_secret_toml_path.unlink()
            return
        if "secret_key" in clean:
            self.shared_secret_toml_path.write_text(clean + "\n", encoding="utf-8")
        else:
            self.shared_secret_toml_path.write_text(f'secret_key = "{clean}"\n', encoding="utf-8")
        self._append_log("Saved shared playit.gg tunnel secret key to Shared Sync Folder.")

    def has_shared_secret(self) -> bool:
        return self.shared_secret_toml_path.exists() and self.shared_secret_toml_path.stat().st_size > 5

    def start_tunnel(self, server_port: int = 25565) -> Dict[str, Any]:
        """
        Starts the zero-port-forwarding tunnel when a member starts hosting.
        If a static publicJoinAddress is configured in ClusterConfig, advertises it immediately.
        If playit binary is installed, launches playit with the shared playit.toml config.
        """
        with self._mutex:
            self._stop_tunnel_internal(emit_stopped=False)
            cfg = self.sync_engine.load_cluster_config()
            if cfg and not cfg.tunnel_enabled:
                self.status = "disabled"
                self.claim_url = ""
                lan = f"{self.get_lan_ip()}:{server_port}"
                self.public_address = lan
                self.sync_engine.update_lock_state(
                    public_address=self.public_address,
                    tunnel_status="disabled",
                )
                state = self.get_state()
                self._emit("tunnel_updated", state)
                return state

            configured_addr = cfg.public_join_address.strip() if cfg and cfg.public_join_address else ""
            lan_addr = f"{self.get_lan_ip()}:{server_port}"
            bin_path = self.resolve_binary()

            if not bin_path and os.environ.get("MINERELAY_TEST_MODE") != "1":
                try:
                    self._append_log("playit.gg agent not found locally; auto-downloading for Zero-Port-Forwarding...")
                    self.download_playit_binary()
                    bin_path = self.resolve_binary()
                except Exception as dl_err:
                    self._append_log(f"Auto-download of playit.gg agent skipped: {dl_err}")

            if bin_path and os.environ.get("MINERELAY_TEST_MODE") != "1":
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

                    self.status = "starting" if self.has_shared_secret() else "claim_required"
                    if self.has_shared_secret():
                        self.claim_url = ""
                    else:
                        self._ensure_claim_flow_async()
                    self.public_address = configured_addr or lan_addr
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
                        bufsize=1,
                        creationflags=creationflags,
                    )
                    threading.Thread(
                        target=self._monitor_playit_output,
                        args=(self._proc, server_port, bin_path),
                        daemon=True,
                        name="PlayitOutputReader",
                    ).start()
                    self.sync_engine.update_lock_state(
                        public_address=self.public_address,
                        tunnel_status="online" if configured_addr else "starting",
                    )
                    return self.get_state()
                except Exception as e:
                    self._append_log(f"Could not spawn playit binary ({e}); falling back to LAN address.")

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

            self.sync_engine.update_lock_state(
                public_address=self.public_address,
                tunnel_status=self.status,
            )
            self._emit("tunnel_updated", self.get_state())
            return self.get_state()

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

    def _query_playit_tunnels_list(self, bin_path: Optional[str]) -> Optional[str]:
        if not bin_path or not self.shared_secret_toml_path.exists():
            return None
        try:
            ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
            addr_regex = re.compile(
                r"([a-zA-Z0-9.-]+\.(?:gl\.joinmc\.link|ply\.gg|joinmc\.link)(?::\d+)?)"
            )
            creationflags = subprocess.CREATE_NO_WINDOW if sys.platform.startswith("win") else 0
            res = subprocess.run(
                [bin_path, "--secret_path", str(self.shared_secret_toml_path), "-s", "tunnels", "list"],
                capture_output=True,
                text=True,
                timeout=10,
                creationflags=creationflags,
            )
            for raw in (res.stdout or "").splitlines():
                clean = ansi_escape.sub("", raw).strip()
                if "playit_cli::ui:" in clean:
                    clean = clean.split("playit_cli::ui:", 1)[1].strip()
                m = addr_regex.search(clean)
                if m:
                    return m.group(1).strip()
                parts = clean.split()
                if len(parts) >= 4 and "." in parts[3]:
                    return parts[3].strip()
        except Exception:
            pass
        return None

    def _ensure_claim_flow_async(self) -> None:
        if os.environ.get("MINERELAY_TEST_MODE") == "1":
            return
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
                            if discovered:
                                self._apply_discovered_public_address(discovered)
                            else:
                                self.claim_url = "https://playit.gg/account/tunnels"
                                self.status = "claim_required"
                                self._emit("tunnel_updated", self.get_state())
                            return

                ansi_escape = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")
                creationflags = subprocess.CREATE_NO_WINDOW if sys.platform.startswith("win") else 0
                gen_res = subprocess.run(
                    [bin_path, "-s", "claim", "generate"],
                    capture_output=True,
                    text=True,
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
                    self._claim_init_started = False
                    return

                self.claim_url = f"https://playit.gg/claim/{code}"
                if self.status in ("stopped", "lan_only"):
                    self.status = "claim_required"
                self._append_log(f"Generated playit.gg claim link: {self.claim_url}")
                self._emit("tunnel_updated", self.get_state())

                # Wait for user to click the claim link and exchange it for secret_key (cancellable via stop_tunnel)
                ex_proc = subprocess.Popen(
                    [bin_path, "-s", "claim", "exchange", code, "--wait", "600"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    creationflags=creationflags,
                )
                self._claim_exchange_proc = ex_proc
                ex_stdout = ""
                try:
                    ex_stdout, _ = ex_proc.communicate(timeout=610)
                except subprocess.TimeoutExpired:
                    ex_proc.kill()
                finally:
                    if self._claim_exchange_proc is ex_proc:
                        self._claim_exchange_proc = None
                secret_key = ""
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
                            text=True,
                            timeout=15,
                            creationflags=creationflags,
                        )
                    except Exception:
                        pass
                    discovered = self._query_playit_tunnels_list(bin_path)
                    if discovered:
                        self._apply_discovered_public_address(discovered)
                    else:
                        self.claim_url = "https://playit.gg/account/tunnels"
                        self.status = "claim_required"
                        self._emit("tunnel_updated", self.get_state())
                else:
                    self._claim_init_started = False
            except Exception:
                self._claim_init_started = False

        threading.Thread(target=_worker, daemon=True, name="PlayitClaimFlow").start()

    def _apply_discovered_public_address(self, found_addr: str) -> None:
        if not found_addr:
            return
        with self._mutex:
            cfg = self.sync_engine.load_cluster_config()
            custom_addr = cfg.public_join_address.strip() if (cfg and cfg.public_join_address) else ""
            is_custom_override = bool(
                cfg
                and cfg.tunnel_provider == "custom"
                and custom_addr
            )
            effective_addr = custom_addr if is_custom_override else found_addr
            self.public_address = effective_addr
            self.status = "online"
            self.claim_url = ""
            if cfg and not is_custom_override and cfg.public_join_address != found_addr:
                cfg.public_join_address = found_addr
                self.sync_engine.save_cluster_config(cfg)
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
            while self._proc is proc and proc.poll() is None:
                if not self.is_private_lan_address(self.public_address):
                    break
                discovered = self._query_playit_tunnels_list(effective_bin)
                if discovered:
                    self._append_log(f"Public playit.gg tunnel active: {discovered}")
                    self._apply_discovered_public_address(discovered)
                    break
                time.sleep(6)

        threading.Thread(target=_poll_until_public, daemon=True, name="PlayitTunnelListPoller").start()

        for raw_line in proc.stdout:
            line = ansi_escape.sub("", raw_line).strip()
            if not line:
                continue
            self._append_log(line)

            claim_match = claim_regex.search(line)
            if claim_match:
                self.claim_url = claim_match.group(1)
                self.status = "claim_required"
                self.sync_engine.update_lock_state(tunnel_status="claim_required")
                self._emit("tunnel_updated", self.get_state())

            if "tunnel running" in line.lower() or "tunnels registered" in line.lower():
                discovered = self._query_playit_tunnels_list(bin_path or self.resolve_binary())
                if discovered:
                    self._apply_discovered_public_address(discovered)
                elif "0 tunnels registered" in line.lower():
                    self.claim_url = "https://playit.gg/account/tunnels"
                    self.status = "claim_required"
                    self._append_log(
                        "Agent claimed, but 0 tunnels are configured on playit.gg! Open https://playit.gg/account/tunnels and add a 'Minecraft Java' tunnel (local port 25565)."
                    )
                    self.sync_engine.update_lock_state(tunnel_status="claim_required")
                    self._emit("tunnel_updated", self.get_state())
                elif self.status != "online":
                    self.status = "online"
                    self.claim_url = ""
                    self.sync_engine.update_lock_state(tunnel_status="online")
                    self._emit("tunnel_updated", self.get_state())

            addr_match = addr_regex.search(line) or arrow_addr_regex.search(line)
            if addr_match:
                found_addr = addr_match.group(1).strip()
                self._apply_discovered_public_address(found_addr)

        if self._proc is proc:
            self._proc = None
            if self.status not in ("stopped", "disabled"):
                self.status = "stopped"
                self._emit("tunnel_updated", self.get_state())

    def _kill_orphaned_playit_processes(self) -> None:
        if psutil is None or os.environ.get("MINERELAY_TEST_MODE") == "1":
            return
        bin_norm = str(self.playit_binary_path.resolve()).lower()
        secret_norm = str(self.shared_secret_toml_path).lower()
        for p in psutil.process_iter(["pid", "name", "cmdline", "exe"]):
            try:
                pname = (p.info.get("name") or "").lower()
                if "playit" not in pname:
                    continue
                cmdline = " ".join(p.info.get("cmdline") or []).lower()
                if "claim" in cmdline or "tunnels" in cmdline:
                    continue
                exe_path = (p.info.get("exe") or "").lower()
                if bin_norm in exe_path or bin_norm in cmdline or secret_norm in cmdline or "playit.toml" in cmdline:
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
