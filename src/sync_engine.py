import os
import re
import sys
import json
import time
import shutil
import zipfile
import threading
import subprocess
from pathlib import Path
from typing import Optional, List, Dict, Any, Callable, Tuple
from src.models import (
    ClusterConfig,
    HostLock,
    LocalMachineConfig,
    UserAccount,
    HEARTBEAT_INTERVAL_SEC,
)


class HostLockConflictError(Exception):
    """Raised when attempting to claim hosting while another member is actively hosting."""
    pass


class SyncEngine:
    def __init__(self, base_data_dir: str, event_callback: Optional[Callable[[str, Any], None]] = None):
        self.base_data_dir = Path(base_data_dir).resolve()
        self.base_data_dir.mkdir(parents=True, exist_ok=True)
        self.local_config_path = self.base_data_dir / "local-config.json"
        self.sync_state_path = self.base_data_dir / ".sync-state.json"
        self.runtime_dir = self.base_data_dir / "runtime-server"
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.default_shared_dir = self.base_data_dir / "shared-sync"

        self.event_callback = event_callback
        self._lock_mutex = threading.RLock()
        self._sync_mutex = threading.RLock()
        self._browse_lock = threading.Lock()
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._heartbeat_stop = threading.Event()
        self._owns_active_lock = False
        self._last_synced_snapshot_mtime: float = 0.0
        self._last_synced_snapshot_sig: Tuple[float, int, str] = (0.0, -1, "")

        self.local_config = self._load_local_config()
        self._ensure_shared_dirs()
        self._init_snapshot_mtime()

    def _save_sync_state(self, mtime: float, size: int, path_str: str) -> None:
        try:
            self._write_json_atomic(
                self.sync_state_path,
                {"mtime": mtime, "size": size, "path": path_str},
            )
        except Exception:
            pass

    def reset_sync_state(self) -> None:
        with self._sync_mutex:
            self._last_synced_snapshot_mtime = 0.0
            self._last_synced_snapshot_sig = (0.0, -1, "")
            try:
                if self.sync_state_path.exists():
                    self.sync_state_path.unlink(missing_ok=True)
            except OSError:
                pass

    def _init_snapshot_mtime(self) -> None:
        try:
            if self.latest_snapshot_path.exists() and (self.runtime_dir / "server.properties").exists():
                st = self.latest_snapshot_path.stat()
                snap_path_str = str(self.latest_snapshot_path)
                if self.sync_state_path.exists():
                    try:
                        saved = json.loads(self.sync_state_path.read_text(encoding="utf-8"))
                        if (
                            saved.get("path") == snap_path_str
                            and int(saved.get("size", -1)) == st.st_size
                            and abs(float(saved.get("mtime", 0.0)) - st.st_mtime) < 1e-3
                        ):
                            self._last_synced_snapshot_mtime = st.st_mtime
                            self._last_synced_snapshot_sig = (st.st_mtime, st.st_size, snap_path_str)
                            return
                    except Exception:
                        pass
                props_mtime = (self.runtime_dir / "server.properties").stat().st_mtime
                if props_mtime >= st.st_mtime - 2.0:
                    self._last_synced_snapshot_mtime = st.st_mtime
                    self._last_synced_snapshot_sig = (st.st_mtime, st.st_size, snap_path_str)
        except OSError:
            pass

    def _emit(self, event_type: str, payload: Any) -> None:
        if self.event_callback:
            try:
                self.event_callback(event_type, payload)
            except Exception:
                pass

    def _load_local_config(self) -> LocalMachineConfig:
        if self.local_config_path.exists():
            try:
                with open(self.local_config_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                cfg = LocalMachineConfig.from_dict(data, str(self.default_shared_dir))
                if not cfg.shared_sync_dir:
                    cfg.shared_sync_dir = str(self.default_shared_dir)
                return cfg
            except Exception:
                pass
        cfg = LocalMachineConfig(shared_sync_dir=str(self.default_shared_dir))
        self.save_local_config(cfg)
        return cfg

    def save_local_config(self, cfg: Optional[LocalMachineConfig] = None) -> None:
        if cfg is not None:
            self.local_config = cfg
        self._write_json_atomic(self.local_config_path, self.local_config.to_dict())
        self._ensure_shared_dirs()

    @property
    def shared_dir(self) -> Path:
        return Path(self.local_config.shared_sync_dir).resolve()

    @property
    def cluster_config_path(self) -> Path:
        return self.shared_dir / "cluster-config.json"

    @property
    def host_lock_path(self) -> Path:
        return self.shared_dir / "host-lock.json"

    @property
    def snapshots_dir(self) -> Path:
        return self.shared_dir / "snapshots"

    @property
    def latest_snapshot_path(self) -> Path:
        return self.snapshots_dir / "latest-server.zip"

    @property
    def backups_dir(self) -> Path:
        return self.snapshots_dir / "backups"

    @property
    def shared_tunnel_dir(self) -> Path:
        return self.shared_dir / "tunnel"

    def _ensure_shared_dirs(self) -> None:
        try:
            self.shared_dir.mkdir(parents=True, exist_ok=True)
            self.snapshots_dir.mkdir(parents=True, exist_ok=True)
            self.backups_dir.mkdir(parents=True, exist_ok=True)
            self.shared_tunnel_dir.mkdir(parents=True, exist_ok=True)
            self._cleanup_cloud_conflict_files()
        except Exception:
            pass

    def _cleanup_cloud_conflict_files(self) -> None:
        """
        Cleans up duplicate conflict files created by Google Drive / OneDrive / Dropbox
        (e.g. 'host-lock (1).json' or leftover '.tmp.*' files).
        """
        conflict_pattern = re.compile(r"^(host-lock|cluster-config|latest-server)\s*\(\d+\)\.(json|zip)$", re.IGNORECASE)
        for folder in (self.shared_dir, self.snapshots_dir):
            if not folder.exists():
                continue
            for item in folder.iterdir():
                if not item.is_file():
                    continue
                if ".tmp." in item.name or conflict_pattern.match(item.name):
                    # Only remove tmp files older than 30 seconds
                    try:
                        if time.time() - item.stat().st_mtime > 30 or conflict_pattern.match(item.name):
                            item.unlink(missing_ok=True)
                    except OSError:
                        pass

    def _write_json_atomic(self, target_path: Path, data: Dict[str, Any]) -> None:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = target_path.with_suffix(f".tmp.{os.getpid()}.{threading.get_ident()}")
        content = json.dumps(data, indent=2)
        try:
            for attempt in range(5):
                try:
                    with open(tmp_path, "w", encoding="utf-8") as f:
                        f.write(content)
                        f.flush()
                        os.fsync(f.fileno())
                    os.replace(tmp_path, target_path)
                    return
                except OSError:
                    time.sleep(0.08 * (attempt + 1))
            # Fallback direct write if cloud sync client locked rename briefly
            with open(target_path, "w", encoding="utf-8") as f:
                f.write(content)
        finally:
            try:
                if tmp_path.exists():
                    tmp_path.unlink(missing_ok=True)
            except OSError:
                pass

    def _read_json_safe(self, target_path: Path) -> Optional[Dict[str, Any]]:
        if not target_path.exists():
            return None
        for attempt in range(4):
            try:
                with open(target_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except (OSError, json.JSONDecodeError):
                time.sleep(0.05 * (attempt + 1))
        return None

    def detect_cloud_folders(self) -> List[Dict[str, str]]:
        """
        Detects installed cloud sync directories (Google Drive in any language/drive letter,
        OneDrive via env vars, Dropbox, and local folders) for easy multi-device setup.
        """
        home = Path.home()
        seen_paths = set()
        results: List[Dict[str, str]] = []

        def _add_candidate(label: str, path_obj: Path) -> None:
            norm = str(path_obj)
            if norm.lower() not in seen_paths:
                seen_paths.add(norm.lower())
                results.append({"label": label, "path": norm})

        # 1. Default local shared folder
        _add_candidate("Default Local Folder", self.default_shared_dir)

        # 2. Scan for Google Drive for Desktop across mounted drive letters (D:..Z:) and User Profile
        gdrive_folder_names = [
            "My Drive",
            "Το Drive μου",  # Greek Google Drive
            "Mon Drive",
            "Meine Ablage",
            "Mi unidad",
            "Il mio Drive",
            "Google Drive",
        ]
        for gd_name in gdrive_folder_names:
            user_gd = home / gd_name
            if user_gd.exists():
                _add_candidate(f"Google Drive ({gd_name})", user_gd / "MineRelay-Shared")

        if sys.platform.startswith("win"):
            for letter in "GHIJKLMNOPQRSTUVWXYZDEF":
                drive_root = Path(f"{letter}:\\")
                try:
                    if not drive_root.exists():
                        continue
                    # Check common localized My Drive folder names on mounted Google Drive
                    for gd_name in gdrive_folder_names:
                        sub = drive_root / gd_name
                        if sub.exists() and sub.is_dir():
                            _add_candidate(f"Google Drive ({letter}:\\{gd_name})", sub / "MineRelay-Shared")
                    # Also check if Google Drive's hidden .shortcut-targets-by-id exists on this drive
                    if (drive_root / ".shortcut-targets-by-id").exists():
                        for sub in drive_root.iterdir():
                            if sub.is_dir() and not sub.name.startswith(".") and sub.name != "$RECYCLE.BIN":
                                _add_candidate(f"Google Drive ({sub})", sub / "MineRelay-Shared")
                except OSError:
                    pass

        # 3. OneDrive (check environment variables + home/OneDrive)
        for env_key in ("OneDriveConsumer", "OneDrive", "OneDriveCommercial"):
            env_val = os.environ.get(env_key, "").strip()
            if env_val and Path(env_val).exists():
                _add_candidate("OneDrive", Path(env_val) / "MineRelay-Shared")
        if (home / "OneDrive").exists():
            _add_candidate("OneDrive", home / "OneDrive" / "MineRelay-Shared")

        # 4. Dropbox
        if (home / "Dropbox").exists():
            _add_candidate("Dropbox", home / "Dropbox" / "MineRelay-Shared")

        return results

    def browse_folder_dialog(self) -> Optional[str]:
        """Opens a native Windows 11 File Explorer Folder Picker (IFileOpenDialog) on top of the browser window."""
        if not sys.platform.startswith("win"):
            return None
        if not self._browse_lock.acquire(blocking=False):
            return None

        result_holder: List[Optional[str]] = [None]
        initial_dir = str(self.shared_dir if self.shared_dir.exists() else Path.home())

        def _sta_picker_worker() -> None:
            import ctypes
            import ctypes.wintypes as wt
            import uuid

            ole32 = ctypes.windll.ole32
            shell32 = ctypes.windll.shell32
            user32 = ctypes.windll.user32

            ole32.CoInitializeEx.argtypes = [ctypes.c_void_p, wt.DWORD]
            ole32.CoInitializeEx.restype = ctypes.c_long
            ole32.CoUninitialize.argtypes = []
            ole32.CoUninitialize.restype = None
            ole32.CoCreateInstance.argtypes = [
                ctypes.c_char_p,
                ctypes.c_void_p,
                wt.DWORD,
                ctypes.c_char_p,
                ctypes.POINTER(ctypes.c_void_p),
            ]
            ole32.CoCreateInstance.restype = ctypes.c_long
            ole32.CoTaskMemFree.argtypes = [ctypes.c_void_p]
            ole32.CoTaskMemFree.restype = None
            shell32.SHCreateItemFromParsingName.argtypes = [
                wt.LPCWSTR,
                ctypes.c_void_p,
                ctypes.c_char_p,
                ctypes.POINTER(ctypes.c_void_p),
            ]
            shell32.SHCreateItemFromParsingName.restype = ctypes.c_long
            user32.CreateWindowExW.argtypes = [
                wt.DWORD,
                wt.LPCWSTR,
                wt.LPCWSTR,
                wt.DWORD,
                ctypes.c_int,
                ctypes.c_int,
                ctypes.c_int,
                ctypes.c_int,
                wt.HWND,
                wt.HMENU,
                wt.HINSTANCE,
                wt.LPVOID,
            ]
            user32.CreateWindowExW.restype = wt.HWND
            user32.DestroyWindow.argtypes = [wt.HWND]
            user32.DestroyWindow.restype = wt.BOOL
            user32.ShowWindow.argtypes = [wt.HWND, ctypes.c_int]
            user32.ShowWindow.restype = wt.BOOL
            user32.SetForegroundWindow.argtypes = [wt.HWND]
            user32.SetForegroundWindow.restype = wt.BOOL

            ole32.CoInitializeEx(None, 2)  # COINIT_APARTMENTTHREADED
            hwnd_owner = None
            p_dlg = ctypes.c_void_p()
            try:
                WS_EX_TOPMOST = 0x00000008
                WS_EX_TOOLWINDOW = 0x00000080
                WS_POPUP = 0x80000000
                hwnd_owner = user32.CreateWindowExW(
                    WS_EX_TOPMOST | WS_EX_TOOLWINDOW,
                    "Static",
                    "MineRelayFolderPickerOwner",
                    WS_POPUP,
                    320,
                    220,
                    1,
                    1,
                    None,
                    None,
                    None,
                    None,
                )
                if hwnd_owner:
                    user32.ShowWindow(hwnd_owner, 5)
                    user32.keybd_event(0, 0, 0, 0)
                    user32.SetForegroundWindow(hwnd_owner)

                clsid_file_open = uuid.UUID("{DC1C5A9C-E88A-4dde-A5A1-60F82A20AEF7}").bytes_le
                iid_file_open = uuid.UUID("{d57c7288-d4ad-4768-be02-9d969532d960}").bytes_le
                iid_shell_item = uuid.UUID("{43826d1e-e718-42ee-bc55-a1e261c37bfe}").bytes_le

                hr = ole32.CoCreateInstance(clsid_file_open, None, 1, iid_file_open, ctypes.byref(p_dlg))
                if hr == 0 and p_dlg.value:
                    vtbl = ctypes.cast(
                        ctypes.cast(p_dlg, ctypes.POINTER(ctypes.c_void_p))[0],
                        ctypes.POINTER(ctypes.c_void_p),
                    )
                    ReleaseDlg = ctypes.WINFUNCTYPE(ctypes.c_ulong, ctypes.c_void_p)(vtbl[2])
                    ShowDlg = ctypes.WINFUNCTYPE(ctypes.c_long, ctypes.c_void_p, wt.HWND)(vtbl[3])
                    SetOptions = ctypes.WINFUNCTYPE(ctypes.c_long, ctypes.c_void_p, ctypes.c_uint32)(vtbl[9])
                    GetOptions = ctypes.WINFUNCTYPE(
                        ctypes.c_long, ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint32)
                    )(vtbl[10])
                    SetFolder = ctypes.WINFUNCTYPE(ctypes.c_long, ctypes.c_void_p, ctypes.c_void_p)(vtbl[12])
                    SetTitle = ctypes.WINFUNCTYPE(ctypes.c_long, ctypes.c_void_p, wt.LPCWSTR)(vtbl[17])
                    SetOkButtonLabel = ctypes.WINFUNCTYPE(ctypes.c_long, ctypes.c_void_p, wt.LPCWSTR)(vtbl[18])
                    GetResult = ctypes.WINFUNCTYPE(
                        ctypes.c_long, ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p)
                    )(vtbl[20])

                    opts = ctypes.c_uint32()
                    GetOptions(p_dlg, ctypes.byref(opts))
                    # FOS_PICKFOLDERS (0x20) | FOS_FORCEFILESYSTEM (0x40) | FOS_PATHMUSTEXIST (0x800)
                    SetOptions(p_dlg, opts.value | 0x20 | 0x40 | 0x800)
                    SetTitle(p_dlg, "Select Shared Cloud Folder (Google Drive / OneDrive / Local) for MineRelay")
                    SetOkButtonLabel(p_dlg, "Select Folder")

                    p_init_item = ctypes.c_void_p()
                    if shell32.SHCreateItemFromParsingName(
                        initial_dir, None, iid_shell_item, ctypes.byref(p_init_item)
                    ) == 0 and p_init_item.value:
                        SetFolder(p_dlg, p_init_item)
                        vtbl_init = ctypes.cast(
                            ctypes.cast(p_init_item, ctypes.POINTER(ctypes.c_void_p))[0],
                            ctypes.POINTER(ctypes.c_void_p),
                        )
                        ctypes.WINFUNCTYPE(ctypes.c_ulong, ctypes.c_void_p)(vtbl_init[2])(p_init_item)

                    if ShowDlg(p_dlg, hwnd_owner) == 0:
                        p_res_item = ctypes.c_void_p()
                        if GetResult(p_dlg, ctypes.byref(p_res_item)) == 0 and p_res_item.value:
                            vtbl_item = ctypes.cast(
                                ctypes.cast(p_res_item, ctypes.POINTER(ctypes.c_void_p))[0],
                                ctypes.POINTER(ctypes.c_void_p),
                            )
                            GetDisplayName = ctypes.WINFUNCTYPE(
                                ctypes.c_long,
                                ctypes.c_void_p,
                                ctypes.c_uint32,
                                ctypes.POINTER(ctypes.c_void_p),
                            )(vtbl_item[5])
                            ReleaseItem = ctypes.WINFUNCTYPE(ctypes.c_ulong, ctypes.c_void_p)(vtbl_item[2])
                            psz_path = ctypes.c_void_p()
                            if GetDisplayName(p_res_item, 0x80058000, ctypes.byref(psz_path)) == 0 and psz_path.value:
                                result_holder[0] = ctypes.wstring_at(psz_path.value)
                                ole32.CoTaskMemFree(psz_path)
                            ReleaseItem(p_res_item)
                    ReleaseDlg(p_dlg)
            except Exception:
                pass
            finally:
                if hwnd_owner:
                    try:
                        user32.DestroyWindow(hwnd_owner)
                    except Exception:
                        pass
                ole32.CoUninitialize()

        try:
            t = threading.Thread(target=_sta_picker_worker, daemon=True)
            t.start()
            t.join(timeout=180)
            return result_holder[0]
        finally:
            self._browse_lock.release()

    def get_sync_diagnostics(self) -> Dict[str, Any]:
        """Returns diagnostics on the current Shared Sync Folder for multi-device troubleshooting."""
        shared = self.shared_dir
        path_lower = str(shared).lower()
        provider = "Local / Custom Folder"
        if "google" in path_lower or "my drive" in path_lower or "drive μου" in path_lower or (shared.drive and Path(shared.drive + "\\.shortcut-targets-by-id").exists()):
            provider = "Google Drive"
        elif "onedrive" in path_lower:
            provider = "OneDrive"
        elif "dropbox" in path_lower:
            provider = "Dropbox"
        elif (shared / ".git").exists():
            provider = "Git Repository"

        snap_exists = self.latest_snapshot_path.exists()
        snap_size = self.latest_snapshot_path.stat().st_size if snap_exists else 0
        snap_mtime = self.latest_snapshot_path.stat().st_mtime if snap_exists else 0.0

        return {
            "sharedSyncDir": str(shared),
            "providerDetected": provider,
            "clusterConfigExists": self.cluster_config_path.exists(),
            "hostLockExists": self.host_lock_path.exists(),
            "latestSnapshotExists": snap_exists,
            "latestSnapshotSizeBytes": snap_size,
            "latestSnapshotModifiedAt": snap_mtime,
            "backupCount": len(list(self.backups_dir.glob("*.zip"))) if self.backups_dir.exists() else 0,
            "hasSharedTunnelSecret": (self.shared_tunnel_dir / "playit.toml").exists(),
        }

    def is_cluster_initialized(self) -> bool:
        cfg = self.load_cluster_config()
        return cfg is not None and len(cfg.accounts) > 0

    def load_cluster_config(self) -> Optional[ClusterConfig]:
        data = self._read_json_safe(self.cluster_config_path)
        if not data:
            return None
        cfg = ClusterConfig.from_dict(data)
        migrated = False
        for acc in cfg.accounts:
            if acc.role == "admin" and acc.username in ("Admin Manager", "AdminManager"):
                acc.username = "Admin"
                migrated = True
        if migrated:
            try:
                self.save_cluster_config(cfg)
            except Exception:
                pass
        return cfg

    def save_cluster_config(self, config: ClusterConfig) -> None:
        self._ensure_shared_dirs()
        self._write_json_atomic(self.cluster_config_path, config.to_dict(include_secrets=True))

    def get_lock(self) -> HostLock:
        data = self._read_json_safe(self.host_lock_path)
        if not data:
            return HostLock(is_locked=False, status="idle")
        return HostLock.from_dict(data)

    def save_lock(self, lock: HostLock) -> None:
        self._ensure_shared_dirs()
        self._write_json_atomic(self.host_lock_path, lock.to_dict())
        self._emit("lock_updated", lock.to_dict())

    def is_locked_by_current_machine(self, lock: Optional[HostLock] = None) -> bool:
        lk = lock or self.get_lock()
        return lk.is_locked and lk.machine_id == self.local_config.machine_id

    def acquire_lock(
        self,
        user: UserAccount,
        mc_version: str = "",
        public_address: str = "",
        initial_status: str = "starting",
    ) -> HostLock:
        """
        Atomically claims the distributed host lock for `user` on this machine.
        Raises HostLockConflictError if another user/machine holds an active lock.
        """
        with self._lock_mutex:
            self.git_pull_if_enabled()
            self._cleanup_cloud_conflict_files()
            current = self.get_lock()
            now = time.time()

            if current.is_actively_locked(now):
                if current.machine_id != self.local_config.machine_id or current.host_user_id != user.id:
                    raise HostLockConflictError(
                        f"Server is already being hosted by '{current.host_username}' "
                        f"on machine '{current.machine_name}' (status: {current.status})."
                    )

            new_lock = HostLock(
                is_locked=True,
                host_user_id=user.id,
                host_username=user.username,
                host_role=user.role,
                machine_id=self.local_config.machine_id,
                machine_name=self.local_config.machine_name,
                started_at=now if not current.is_actively_locked(now) else current.started_at,
                last_heartbeat=now,
                status=initial_status,
                public_address=public_address,
                tunnel_status="starting" if public_address else "stopped",
                mc_version=mc_version,
                online_players=[],
            )
            self.save_lock(new_lock)

            # Re-verify ownership to guard against simultaneous cloud folder writes
            time.sleep(0.15)
            check = self.get_lock()
            if check.is_locked and (
                check.machine_id != self.local_config.machine_id or check.host_user_id != user.id
            ):
                raise HostLockConflictError(
                    f"Lock claim race won by '{check.host_username}' on '{check.machine_name}'."
                )

            self._owns_active_lock = True
            self._start_heartbeat_loop()
            return new_lock

    def update_lock_state(
        self,
        status: Optional[str] = None,
        public_address: Optional[str] = None,
        tunnel_status: Optional[str] = None,
        online_players: Optional[List[str]] = None,
        max_players: Optional[int] = None,
        mc_version: Optional[str] = None,
    ) -> Optional[HostLock]:
        with self._lock_mutex:
            current = self.get_lock()
            if not current.is_locked or current.machine_id != self.local_config.machine_id:
                return None
            current.last_heartbeat = time.time()
            if status is not None:
                current.status = status
            if public_address is not None:
                current.public_address = public_address
            if tunnel_status is not None:
                current.tunnel_status = tunnel_status
            if online_players is not None:
                current.online_players = online_players
            if max_players is not None:
                current.max_players = max_players
            if mc_version is not None:
                current.mc_version = mc_version
            self.save_lock(current)
            return current

    def release_lock(self, user: Optional[UserAccount] = None) -> HostLock:
        with self._lock_mutex:
            self._owns_active_lock = False
            self._stop_heartbeat_loop()
            current = self.get_lock()
            if current.is_locked:
                is_owner = (
                    current.machine_id == self.local_config.machine_id
                    or (user and current.host_user_id == user.id)
                    or (user and user.role == "admin")
                    or current.is_stale()
                )
                if not is_owner:
                    raise HostLockConflictError(
                        f"Cannot release lock owned by '{current.host_username}' on '{current.machine_name}'."
                    )
            unlocked = HostLock(is_locked=False, status="idle")
            self.save_lock(unlocked)
            self.git_push_if_enabled("Release host lock and sync world")
            return unlocked

    def force_unlock(self) -> HostLock:
        """Admin emergency lock override."""
        with self._lock_mutex:
            self._owns_active_lock = False
            self._stop_heartbeat_loop()
            unlocked = HostLock(is_locked=False, status="idle")
            self.save_lock(unlocked)
            return unlocked

    def _start_heartbeat_loop(self) -> None:
        self._stop_heartbeat_loop()
        self._heartbeat_stop.clear()

        def _loop() -> None:
            while not self._heartbeat_stop.wait(HEARTBEAT_INTERVAL_SEC):
                with self._lock_mutex:
                    if not self._owns_active_lock:
                        break
                    current = self.get_lock()
                    if current.is_locked and current.machine_id == self.local_config.machine_id:
                        current.last_heartbeat = time.time()
                        self.save_lock(current)
                    else:
                        self._owns_active_lock = False
                        break

        self._heartbeat_thread = threading.Thread(target=_loop, daemon=True, name="HostLockHeartbeat")
        self._heartbeat_thread.start()

    def _stop_heartbeat_loop(self) -> None:
        self._heartbeat_stop.set()

    # -------------------------------------------------------------------------
    # World & Server Files Snapshot Sync (Shared Cloud Folder <-> Local Runtime)
    # -------------------------------------------------------------------------

    def ensure_runtime_synced_when_idle(self) -> None:
        """
        Ensures that before an idle machine reads or edits config files (like server.properties),
        if a newer or different snapshot exists in the Shared Cloud Folder (from another member's session),
        it pulls that snapshot first so we never clobber a newer world save!
        """
        with self._sync_mutex:
            if not self.latest_snapshot_path.exists():
                self._ensure_default_runtime_files()
                return
            try:
                st = self.latest_snapshot_path.stat()
                last_mtime, last_size, last_path = self._last_synced_snapshot_sig
                if (
                    abs(st.st_mtime - last_mtime) > 1e-3
                    or st.st_size != last_size
                    or str(self.latest_snapshot_path) != last_path
                ):
                    self.pull_world_to_runtime()
            except OSError:
                pass

    def _safe_extract_zip(self, zf: zipfile.ZipFile, dest_dir: Path) -> None:
        """
        Safely extracts all zip members preventing zip-slip path traversal vulnerabilities.
        """
        dest_resolved = dest_dir.resolve()
        for member in zf.infolist():
            target_path = (dest_dir / member.filename).resolve()
            if not (target_path == dest_resolved or target_path.is_relative_to(dest_resolved)):
                raise ValueError(f"Zip path traversal detected: {member.filename}")
            zf.extract(member, dest_dir)

    def pull_world_to_runtime(self) -> Dict[str, Any]:
        """
        Extracts `snapshots/latest-server.zip` from the Shared Sync Folder into `./data/runtime-server`.
        Preserves static JVM/Paper libraries (`server.jar`, `libraries/`, `versions/`, `cache/`, `plugins/.paper-remapped`)
        so version binaries don't need re-unpacking on every start.
        Validates the zip archive BEFORE cleaning runtime files so a cloud sync lock never wipes the local world!
        """
        with self._sync_mutex:
            self._ensure_shared_dirs()
            self.runtime_dir.mkdir(parents=True, exist_ok=True)
            self.git_pull_if_enabled()

            if not self.latest_snapshot_path.exists():
                self._ensure_default_runtime_files()
                return {"synced": False, "reason": "No existing shared snapshot found; initialized fresh runtime."}

            # Verify the snapshot zip can be opened (retry up to 5 times in case OneDrive/Google Drive holds a brief lock)
            last_err = None
            for attempt in range(5):
                try:
                    with zipfile.ZipFile(self.latest_snapshot_path, "r") as zf:
                        _ = zf.namelist()
                    last_err = None
                    break
                except Exception as e:
                    last_err = e
                    time.sleep(0.15 * (attempt + 1))

            if last_err is not None:
                # Keep existing runtime files intact if snapshot is temporarily unreadable
                self._ensure_default_runtime_files()
                return {
                    "synced": False,
                    "reason": f"Using local runtime files (snapshot temporarily busy: {last_err})",
                }

            preserved_entries = {"server.jar", "jar-version.json", "cache", "libraries", "versions", "logs", "crash-reports"}

            # Clean world & config files in runtime_dir before extracting latest snapshot, while preserving plugins/.paper-remapped
            for item in self.runtime_dir.iterdir():
                if item.name in preserved_entries:
                    continue
                if item.name == "plugins" and item.is_dir():
                    for sub in item.iterdir():
                        if sub.name == ".paper-remapped":
                            continue
                        if sub.is_dir():
                            shutil.rmtree(sub, ignore_errors=True)
                        else:
                            try:
                                sub.unlink()
                            except OSError:
                                pass
                    continue
                if item.is_dir():
                    shutil.rmtree(item, ignore_errors=True)
                else:
                    try:
                        item.unlink()
                    except OSError:
                        pass

            with zipfile.ZipFile(self.latest_snapshot_path, "r") as zf:
                self._safe_extract_zip(zf, self.runtime_dir)

            # Remove stale Minecraft session.lock files if any
            for lock_file in self.runtime_dir.rglob("session.lock"):
                try:
                    lock_file.unlink()
                except OSError:
                    pass

            self._ensure_default_runtime_files()
            st = self.latest_snapshot_path.stat()
            self._last_synced_snapshot_mtime = st.st_mtime
            self._last_synced_snapshot_sig = (st.st_mtime, st.st_size, str(self.latest_snapshot_path))
            self._save_sync_state(st.st_mtime, st.st_size, str(self.latest_snapshot_path))
            return {
                "synced": True,
                "snapshotPath": str(self.latest_snapshot_path),
                "sizeBytes": st.st_size,
                "timestamp": time.time(),
            }

    def push_world_from_runtime(self, create_backup: bool = True, backup_label: str = "auto") -> Dict[str, Any]:
        """
        Packages `./data/runtime-server` (worlds, server.properties, ops, whitelist, plugins, configs)
        into `snapshots/latest-server.zip` atomically, and optionally creates a timestamped backup.
        Excludes `server.jar`, `libraries/`, `versions/`, `cache/`, and `plugins/.paper-remapped`
        to keep cloud sync fast (~1.5 MB instead of 30+ MB)!
        """
        with self._sync_mutex:
            self._ensure_shared_dirs()
            self.runtime_dir.mkdir(parents=True, exist_ok=True)

            tmp_zip = self.snapshots_dir / f"latest-server.tmp.{os.getpid()}.{threading.get_ident()}.zip"
            excluded_top = {"cache", "libraries", "versions", "logs", "crash-reports"}
            excluded_files = {"server.jar", "jar-version.json", "session.lock"}

            file_count = 0
            try:
                with zipfile.ZipFile(tmp_zip, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
                    for root, dirs, files in os.walk(self.runtime_dir):
                        rel_root = Path(root).relative_to(self.runtime_dir)
                        if rel_root.parts and rel_root.parts[0] in excluded_top:
                            dirs[:] = []
                            continue
                        if ".paper-remapped" in dirs:
                            dirs.remove(".paper-remapped")
                        if "tmp-client" in dirs:
                            dirs.remove("tmp-client")
                        for file in files:
                            if file in excluded_files or file.endswith(".lck") or file.endswith(".tmp"):
                                continue
                            abs_file = Path(root) / file
                            rel_file = abs_file.relative_to(self.runtime_dir)
                            try:
                                zf.write(abs_file, arcname=str(rel_file))
                                file_count += 1
                            except OSError:
                                pass

                for attempt in range(5):
                    try:
                        os.replace(tmp_zip, self.latest_snapshot_path)
                        break
                    except OSError:
                        if attempt == 4:
                            shutil.copy2(tmp_zip, self.latest_snapshot_path)
                        else:
                            time.sleep(0.1 * (attempt + 1))
            finally:
                try:
                    if tmp_zip.exists():
                        tmp_zip.unlink(missing_ok=True)
                except OSError:
                    pass

            st = self.latest_snapshot_path.stat()
            self._last_synced_snapshot_mtime = st.st_mtime
            self._last_synced_snapshot_sig = (st.st_mtime, st.st_size, str(self.latest_snapshot_path))
            self._save_sync_state(st.st_mtime, st.st_size, str(self.latest_snapshot_path))
            size_bytes = st.st_size

            backup_name = None
            if create_backup:
                ts = time.strftime("%Y%m%d-%H%M%S")
                safe_label = "".join(c for c in backup_label if c.isalnum() or c in ("-", "_")) or "backup"
                backup_name = f"world-{ts}-{safe_label}.zip"
                backup_target = self.backups_dir / backup_name
                shutil.copy2(self.latest_snapshot_path, backup_target)
                self._prune_old_backups()

            self.git_push_if_enabled(f"Sync world snapshot ({file_count} files)")
            return {
                "pushed": True,
                "fileCount": file_count,
                "sizeBytes": size_bytes,
                "backupFile": backup_name,
                "timestamp": time.time(),
            }

    def list_backups(self) -> List[Dict[str, Any]]:
        self._ensure_shared_dirs()
        backups: List[Dict[str, Any]] = []
        for f in sorted(self.backups_dir.glob("*.zip"), key=lambda p: p.stat().st_mtime, reverse=True):
            st = f.stat()
            backups.append({
                "name": f.name,
                "sizeBytes": st.st_size,
                "createdAt": st.st_mtime,
            })
        return backups

    def restore_backup(self, backup_name: str) -> Dict[str, Any]:
        safe_name = Path(backup_name).name
        if not safe_name or safe_name in (".", ".."):
            raise ValueError("Invalid backup name.")
        source = self.backups_dir / safe_name
        if not source.exists() or not source.is_file():
            raise FileNotFoundError(f"Backup '{safe_name}' not found.")
        try:
            with zipfile.ZipFile(source, "r") as zf:
                if zf.testzip() is not None:
                    raise ValueError(f"Backup '{safe_name}' archive is corrupted.")
        except Exception as e:
            raise ValueError(f"Invalid backup archive: {e}")
        shutil.copy2(source, self.latest_snapshot_path)
        pull_res = self.pull_world_to_runtime()
        return {"restored": safe_name, "pull": pull_res}

    def delete_backup(self, backup_name: str) -> bool:
        safe_name = Path(backup_name).name
        if not safe_name or safe_name in (".", ".."):
            return False
        target = self.backups_dir / safe_name
        if target.exists() and target.is_file():
            target.unlink()
            return True
        return False

    def _prune_old_backups(self) -> None:
        cfg = self.load_cluster_config()
        max_b = cfg.max_backups if cfg else 10
        backups = sorted(self.backups_dir.glob("*.zip"), key=lambda p: p.stat().st_mtime, reverse=True)
        for old in backups[max_b:]:
            try:
                old.unlink()
            except OSError:
                pass

    def _ensure_default_runtime_files(self) -> None:
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        eula_path = self.runtime_dir / "eula.txt"
        if not eula_path.exists():
            eula_path.write_text("# Auto-accepted by MineRelay\neula=true\n", encoding="utf-8")

        props_path = self.runtime_dir / "server.properties"
        if not props_path.exists():
            cfg = self.load_cluster_config()
            motd = cfg.server_name if cfg else "MineRelay Shared Minecraft Server"
            port = cfg.server_port if cfg else 25565
            default_props = (
                f"motd={motd}\n"
                f"server-port={port}\n"
                "gamemode=survival\n"
                "difficulty=normal\n"
                "max-players=20\n"
                "online-mode=true\n"
                "pvp=true\n"
                "view-distance=10\n"
                "simulation-distance=10\n"
                "spawn-protection=0\n"
                "allow-flight=false\n"
                "white-list=false\n"
                "enable-command-block=true\n"
            )
            props_path.write_text(default_props, encoding="utf-8")

        for json_file in ("ops.json", "whitelist.json", "banned-players.json"):
            p = self.runtime_dir / json_file
            if not p.exists():
                p.write_text("[]", encoding="utf-8")

        server_icon_target = self.runtime_dir / "server-icon.png"
        if not server_icon_target.exists():
            import sys
            bundle_dir = Path(getattr(sys, "_MEIPASS", Path(__file__).resolve().parent.parent))
            default_icon = bundle_dir / "public" / "server-icon.png"
            if not default_icon.exists():
                default_icon = Path(__file__).resolve().parent.parent / "public" / "server-icon.png"
            if default_icon.exists():
                try:
                    shutil.copy2(default_icon, server_icon_target)
                except OSError:
                    pass

    # -------------------------------------------------------------------------
    # Optional Git Sync Support
    # -------------------------------------------------------------------------

    def git_pull_if_enabled(self) -> None:
        cfg = self.load_cluster_config()
        if not cfg or not cfg.git_sync_enabled:
            return
        if (self.shared_dir / ".git").exists():
            try:
                subprocess.run(
                    ["git", "pull", "--rebase", "--autostash"],
                    cwd=str(self.shared_dir),
                    capture_output=True,
                    timeout=15,
                )
            except Exception:
                pass

    def git_push_if_enabled(self, commit_msg: str) -> None:
        cfg = self.load_cluster_config()
        if not cfg or not cfg.git_sync_enabled:
            return
        if (self.shared_dir / ".git").exists():
            try:
                subprocess.run(["git", "add", "-A"], cwd=str(self.shared_dir), capture_output=True, timeout=10)
                subprocess.run(
                    ["git", "commit", "-m", commit_msg],
                    cwd=str(self.shared_dir),
                    capture_output=True,
                    timeout=10,
                )
                subprocess.run(["git", "push"], cwd=str(self.shared_dir), capture_output=True, timeout=20)
            except Exception:
                pass
