import os
import re
import sys
import json
import time
import zlib
import shutil
import hashlib
import secrets
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
    LOCK_STALE_TIMEOUT_SEC,
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
        self._last_synced_manifest_seq: int = 0
        self._last_known_remote_lock: Optional[HostLock] = None
        self._last_known_remote_lock_at: float = 0.0
        self._remote_hb_tracker: Dict[str, Tuple[float, float]] = {}

        self.local_config = self._load_local_config()
        self._ensure_shared_dirs()
        self._init_snapshot_mtime()

    def _save_sync_state(
        self,
        mtime: float,
        size: int,
        path_str: str,
        manifest_seq: Optional[int] = None,
    ) -> None:
        seq = manifest_seq if manifest_seq is not None else self._last_synced_manifest_seq
        self._last_synced_manifest_seq = seq
        try:
            self._write_json_atomic(
                self.sync_state_path,
                {"mtime": mtime, "size": size, "path": path_str, "manifestSeq": seq},
                in_place_if_exists=True,
            )
        except Exception:
            pass

    def reset_sync_state(self) -> None:
        with self._sync_mutex:
            self._last_synced_snapshot_mtime = 0.0
            self._last_synced_snapshot_sig = (0.0, -1, "")
            self._last_synced_manifest_seq = 0
            self._last_known_remote_lock = None
            self._last_known_remote_lock_at = 0.0
            self._remote_hb_tracker.clear()
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
                            self._last_synced_manifest_seq = int(saved.get("manifestSeq", 0) or 0)
                            return
                    except Exception:
                        pass
                props_mtime = (self.runtime_dir / "server.properties").stat().st_mtime
                if props_mtime >= st.st_mtime - 2.0:
                    self._last_synced_snapshot_mtime = st.st_mtime
                    self._last_synced_snapshot_sig = (st.st_mtime, st.st_size, snap_path_str)
                    manifest = self._read_sync_manifest()
                    if manifest:
                        self._last_synced_manifest_seq = int(manifest.get("snapshotSeq", 0) or 0)
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
    def sync_manifest_path(self) -> Path:
        return self.snapshots_dir / "sync-manifest.json"

    @property
    def locks_dir(self) -> Path:
        return self.shared_dir / "locks"

    @property
    def shared_tunnel_dir(self) -> Path:
        return self.shared_dir / "tunnel"

    def _ensure_shared_dirs(self) -> None:
        try:
            self.shared_dir.mkdir(parents=True, exist_ok=True)
            self.snapshots_dir.mkdir(parents=True, exist_ok=True)
            self.backups_dir.mkdir(parents=True, exist_ok=True)
            self.locks_dir.mkdir(parents=True, exist_ok=True)
            self.shared_tunnel_dir.mkdir(parents=True, exist_ok=True)
            self._cleanup_cloud_conflict_files()
        except Exception:
            pass

    @staticmethod
    def _sha256_file(target_path: Path) -> str:
        h = hashlib.sha256()
        with open(target_path, "rb") as f:
            while True:
                chunk = f.read(1024 * 1024)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest()

    def _has_active_cloud_transfer(self) -> Optional[str]:
        """Returns a human-readable notice if Google Drive / OneDrive is actively syncing a temp file."""
        now = time.time()
        for folder in (self.shared_dir, self.snapshots_dir):
            if not folder.exists():
                continue
            for marker_name in (".tmp.drivedownload", ".tmp.driveupload"):
                mdir = folder / marker_name
                if mdir.exists() and mdir.is_dir():
                    try:
                        for entry in mdir.iterdir():
                            try:
                                if not entry.is_file():
                                    continue
                                st = entry.stat()
                                if st.st_size > 0 and (now - st.st_mtime) < 120:
                                    action = "downloading" if "download" in marker_name else "uploading"
                                    return f"Cloud sync is currently {action} files ({entry.name}, {round(st.st_size / (1024 * 1024), 1)} MB)."
                            except OSError:
                                continue
                    except OSError:
                        pass
        return None

    def _copy_file_preserving_cloud_id(self, src_path: Path, dst_path: Path) -> None:
        """
        Copies `src_path` to `dst_path`, updating `dst_path` in-place when it already exists so
        Google Drive preserves the existing Cloud File ID inside shared folders instead of orphaning duplicates.
        """
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        if dst_path.exists():
            last_err: Optional[Exception] = None
            for attempt in range(6):
                try:
                    with open(src_path, "rb") as fsrc, open(dst_path, "r+b") as fdst:
                        fdst.seek(0)
                        shutil.copyfileobj(fsrc, fdst, length=1024 * 1024)
                        fdst.truncate()
                        fdst.flush()
                        os.fsync(fdst.fileno())
                    return
                except OSError as e:
                    last_err = e
                    time.sleep(0.15 * (attempt + 1))
        tmp_dst = dst_path.parent / f".{dst_path.name}.tmp.{os.getpid()}.{threading.get_ident()}"
        try:
            shutil.copyfile(src_path, tmp_dst)
            for attempt in range(6):
                try:
                    os.replace(tmp_dst, dst_path)
                    return
                except OSError as e:
                    last_err = e
                    time.sleep(0.15 * (attempt + 1))
            shutil.copyfile(tmp_dst, dst_path)
        finally:
            try:
                if tmp_dst.exists():
                    tmp_dst.unlink(missing_ok=True)
            except OSError:
                pass

    def _read_sync_manifest(self) -> Optional[Dict[str, Any]]:
        data = self._read_json_safe(self.sync_manifest_path)
        if not isinstance(data, dict) or not data.get("sha256"):
            return None
        return data

    def _write_sync_manifest(
        self,
        zip_path: Path,
        sha256_hex: str,
        size_bytes: int,
        file_count: int,
        backup_file: Optional[str],
        host_username: str = "",
    ) -> Dict[str, Any]:
        prev = self._read_sync_manifest()
        prev_seq = int(prev.get("snapshotSeq", 0)) if prev else 0
        manifest = {
            "snapshotSeq": prev_seq + 1,
            "sha256": sha256_hex,
            "sizeBytes": int(size_bytes),
            "fileCount": int(file_count),
            "backupFile": backup_file or "",
            "hostUsername": host_username or "",
            "machineId": self.local_config.machine_id,
            "machineName": self.local_config.machine_name,
            "updatedAt": time.time(),
        }
        self._write_json_atomic(self.sync_manifest_path, manifest, in_place_if_exists=True)
        return manifest

    def _load_released_epochs(self) -> set:
        released = set()
        if not self.locks_dir.exists():
            return released
        try:
            for item in self.locks_dir.iterdir():
                if not item.is_file() or not item.name.startswith("released-") or not item.name.endswith(".json"):
                    continue
                epoch = item.name[len("released-"):-len(".json")]
                if epoch:
                    released.add(epoch)
        except OSError:
            pass
        return released

    def _cleanup_cloud_conflict_files(self) -> None:
        """
        Cleans up duplicate conflict files created by Google Drive / OneDrive / Dropbox
        (e.g. 'host-lock (1).json' or leftover '.tmp.*' files) after preserving any fresher active state,
        and prunes expired per-session tombstones in `locks/`.
        """
        conflict_pattern = re.compile(r"^(host-lock|cluster-config|latest-server|sync-manifest)\s*\(\d+\)\.(json|zip)$", re.IGNORECASE)
        manifest = self._read_sync_manifest() if self.sync_manifest_path.exists() else None
        released_epochs = self._load_released_epochs()
        now = time.time()

        for folder in (self.shared_dir, self.snapshots_dir):
            if not folder.exists():
                continue
            for item in folder.iterdir():
                if not item.is_file():
                    continue
                if ".tmp." in item.name:
                    try:
                        if now - item.stat().st_mtime > 30:
                            item.unlink(missing_ok=True)
                    except OSError:
                        pass
                    continue
                m = conflict_pattern.match(item.name)
                if not m:
                    continue
                stem_base = m.group(1).lower()
                # Preserve a newer active lock before deleting a host-lock conflict file
                if stem_base == "host-lock":
                    try:
                        cdata = self._read_json_safe(item)
                        if isinstance(cdata, dict) and cdata:
                            clock = HostLock.from_dict(cdata)
                            main_data = self._read_json_safe(self.host_lock_path)
                            main_lock = HostLock.from_dict(main_data) if isinstance(main_data, dict) else HostLock()
                            if (
                                clock.is_actively_locked(now)
                                and (not clock.lock_epoch or clock.lock_epoch not in released_epochs)
                                and clock.last_heartbeat > main_lock.last_heartbeat
                            ):
                                self._write_json_atomic(self.host_lock_path, clock.to_dict(), in_place_if_exists=True)
                    except Exception:
                        pass
                # Recover a matching snapshot if latest-server.zip doesn't match sync-manifest.json
                elif stem_base == "latest-server" and manifest:
                    try:
                        exp_size = int(manifest.get("sizeBytes", -1))
                        exp_hash = str(manifest.get("sha256", ""))
                        if item.stat().st_size == exp_size and exp_hash:
                            main_ok = (
                                self.latest_snapshot_path.exists()
                                and self.latest_snapshot_path.stat().st_size == exp_size
                                and self._sha256_file(self.latest_snapshot_path) == exp_hash
                            )
                            if not main_ok and self._sha256_file(item) == exp_hash:
                                self._copy_file_preserving_cloud_id(item, self.latest_snapshot_path)
                    except Exception:
                        pass
                try:
                    item.unlink(missing_ok=True)
                except OSError:
                    pass

        if self.locks_dir.exists():
            try:
                for item in self.locks_dir.iterdir():
                    if not item.is_file() or not item.name.endswith(".json"):
                        continue
                    try:
                        age = now - item.stat().st_mtime
                        if item.name.startswith("session-"):
                            epoch = item.name[len("session-"):-len(".json")]
                            if epoch in released_epochs and age > 60:
                                item.unlink(missing_ok=True)
                        elif item.name.startswith("released-") and age > 3600:
                            item.unlink(missing_ok=True)
                    except OSError:
                        pass
            except OSError:
                pass

    def _write_json_atomic(
        self,
        target_path: Path,
        data: Dict[str, Any],
        in_place_if_exists: bool = False,
    ) -> None:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        content = json.dumps(data, indent=2)

        # Updating existing shared JSON in-place keeps Google Drive Cloud File IDs intact
        if in_place_if_exists and target_path.exists():
            for attempt in range(8):
                try:
                    with open(target_path, "r+", encoding="utf-8") as f:
                        f.seek(0)
                        f.write(content)
                        f.truncate()
                        f.flush()
                        os.fsync(f.fileno())
                    return
                except FileNotFoundError:
                    break
                except OSError:
                    time.sleep(0.08 * (attempt + 1))

        tmp_path = target_path.with_suffix(f".tmp.{os.getpid()}.{threading.get_ident()}")
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                f.write(content)
                f.flush()
                os.fsync(f.fileno())
            for attempt in range(8):
                try:
                    os.replace(tmp_path, target_path)
                    return
                except OSError:
                    time.sleep(0.08 * (attempt + 1))
            if not target_path.exists():
                shutil.copyfile(tmp_path, target_path)
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

        # 2. Scan mirrored local Google Drive / shared Minecraft folders first (most reliable real-time sync on Windows)
        mirrored_dirs = []
        for parent_dir in (home / "Documents", home / "Desktop", home):
            try:
                if parent_dir.exists() and parent_dir.is_dir():
                    for sub in parent_dir.iterdir():
                        if not sub.is_dir() or sub.name.startswith("."):
                            continue
                        try:
                            has_drive_markers = (sub / ".tmp.drivedownload").exists() or (sub / ".tmp.driveupload").exists()
                            has_cluster = (sub / "cluster-config.json").exists() and (sub / "snapshots").exists()
                            if has_drive_markers or has_cluster:
                                mirrored_dirs.append(sub)
                        except OSError:
                            continue
            except OSError:
                pass
        for sub in mirrored_dirs:
            _add_candidate(f"Google Drive Mirror ({sub.name})", sub)

        # 3. Scan for Google Drive for Desktop across mounted drive letters (D:..Z:) and User Profile
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

        # 4. OneDrive (check environment variables + home/OneDrive)
        for env_key in ("OneDriveConsumer", "OneDrive", "OneDriveCommercial"):
            env_val = os.environ.get(env_key, "").strip()
            if env_val and Path(env_val).exists():
                _add_candidate("OneDrive", Path(env_val) / "MineRelay-Shared")
        if (home / "OneDrive").exists():
            _add_candidate("OneDrive", home / "OneDrive" / "MineRelay-Shared")

        # 5. Dropbox
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
        manifest = self._read_sync_manifest()
        is_shortcut_stream = ".shortcut-targets-by-id" in path_lower

        mirrored_alternatives: List[str] = []
        if is_shortcut_stream:
            try:
                cur_cfg = self.load_cluster_config()
                cur_cid = cur_cfg.cluster_id if cur_cfg else ""
                for cand in self.detect_cloud_folders():
                    cpath = Path(cand["path"])
                    if cpath.resolve() == shared:
                        continue
                    cc_file = cpath / "cluster-config.json"
                    if ".shortcut-targets-by-id" not in str(cpath).lower() and cc_file.exists():
                        cdata = self._read_json_safe(cc_file)
                        if cdata and (not cur_cid or cdata.get("clusterId") == cur_cid):
                            mirrored_alternatives.append(str(cpath))
            except Exception:
                pass

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
            "isShortcutStreamPath": is_shortcut_stream,
            "mirroredAlternatives": mirrored_alternatives,
            "cloudTransferActive": self._has_active_cloud_transfer(),
            "syncManifest": manifest,
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
        self._write_json_atomic(
            self.cluster_config_path,
            config.to_dict(include_secrets=True),
            in_place_if_exists=True,
        )

    def _write_lock_tombstone(self, lock_epoch: str, released_by: str = "") -> None:
        if not lock_epoch:
            return
        self._ensure_shared_dirs()
        tombstone = self.locks_dir / f"released-{lock_epoch}.json"
        self._write_json_atomic(
            tombstone,
            {
                "lockEpoch": lock_epoch,
                "isLocked": False,
                "releasedAt": time.time(),
                "releasedBy": released_by,
                "machineId": self.local_config.machine_id,
            },
            in_place_if_exists=True,
        )
        session_file = self.locks_dir / f"session-{lock_epoch}.json"
        try:
            if session_file.exists():
                session_file.unlink(missing_ok=True)
        except OSError:
            pass

    def _write_lock_bundle(self, lock: HostLock, include_session: bool = True) -> None:
        self._ensure_shared_dirs()
        payload = lock.to_dict()
        self._write_json_atomic(self.host_lock_path, payload, in_place_if_exists=True)
        if include_session and lock.is_locked and lock.lock_epoch:
            session_file = self.locks_dir / f"session-{lock.lock_epoch}.json"
            self._write_json_atomic(session_file, payload, in_place_if_exists=True)

    def get_lock(self) -> HostLock:
        now = time.time()
        released_epochs = self._load_released_epochs()
        main_data = self._read_json_safe(self.host_lock_path)
        main_read_error = self.host_lock_path.exists() and main_data is None
        any_read_error = main_read_error

        main_lock = HostLock.from_dict(main_data) if isinstance(main_data, dict) else HostLock(is_locked=False, status="idle")
        try:
            main_mtime = self.host_lock_path.stat().st_mtime if self.host_lock_path.exists() else 0.0
        except OSError:
            main_mtime = 0.0

        # Collect locked candidates across host-lock.json, Drive conflict copies, and per-session locks/
        candidates: List[Tuple[str, float, float, HostLock]] = []
        if main_lock.is_locked and (not main_lock.lock_epoch or main_lock.lock_epoch not in released_epochs):
            candidates.append(("main", main_lock.last_heartbeat, main_mtime, main_lock))

        conflict_pattern = re.compile(r"^host-lock\s*\(\d+\)\.json$", re.IGNORECASE)
        try:
            if self.shared_dir.exists():
                for item in self.shared_dir.iterdir():
                    if not item.is_file() or not conflict_pattern.match(item.name):
                        continue
                    cdata = self._read_json_safe(item)
                    if cdata is None:
                        any_read_error = True
                        continue
                    clock = HostLock.from_dict(cdata)
                    if not clock.is_locked:
                        continue
                    if clock.lock_epoch and clock.lock_epoch in released_epochs:
                        continue
                    try:
                        cmtime = item.stat().st_mtime
                    except OSError:
                        cmtime = 0.0
                    # If main_lock explicitly unlocked a legacy session cleanly, ignore older conflict files
                    if not main_lock.is_locked and main_data is not None and not clock.lock_epoch:
                        if max(clock.last_heartbeat, cmtime) <= main_mtime + 2.0:
                            continue
                    candidates.append(("conflict", clock.last_heartbeat, cmtime, clock))
        except OSError:
            pass

        try:
            if self.locks_dir.exists():
                for item in self.locks_dir.iterdir():
                    if not item.is_file() or not item.name.startswith("session-") or not item.name.endswith(".json"):
                        continue
                    epoch = item.name[len("session-"):-len(".json")]
                    if epoch and epoch in released_epochs:
                        continue
                    sdata = self._read_json_safe(item)
                    if sdata is None:
                        any_read_error = True
                        continue
                    slock = HostLock.from_dict(sdata)
                    if not slock.lock_epoch:
                        slock.lock_epoch = epoch
                    if not slock.is_locked or slock.lock_epoch in released_epochs:
                        continue
                    try:
                        smtime = item.stat().st_mtime
                    except OSError:
                        smtime = 0.0
                    candidates.append(("session", slock.last_heartbeat, smtime, slock))
        except OSError:
            pass

        if candidates:
            candidates.sort(key=lambda c: (c[1], c[3].started_at, c[2]), reverse=True)
            chosen = candidates[0][3]

            # Clock-skew protection: if a remote host actively increments last_heartbeat between polls,
            # use our local observation timestamp for stale detection even if PC clocks differ
            if chosen.machine_id and chosen.machine_id != self.local_config.machine_id:
                track_key = f"{chosen.machine_id}:{chosen.lock_epoch or int(chosen.started_at)}"
                prev = self._remote_hb_tracker.get(track_key)
                if prev is None:
                    self._remote_hb_tracker[track_key] = (chosen.last_heartbeat, 0.0)
                else:
                    prev_hb, obs_at = prev
                    if chosen.last_heartbeat > prev_hb + 0.5:
                        obs_at = now
                        self._remote_hb_tracker[track_key] = (chosen.last_heartbeat, obs_at)
                    if (
                        obs_at > 0.0
                        and (now - obs_at) <= LOCK_STALE_TIMEOUT_SEC
                        and (now - chosen.last_heartbeat) > LOCK_STALE_TIMEOUT_SEC
                    ):
                        chosen.last_heartbeat = max(chosen.last_heartbeat, obs_at)

            self._last_known_remote_lock = HostLock.from_dict(chosen.to_dict())
            self._last_known_remote_lock_at = now
            return chosen

        # Do not flicker back to 'idle' during transient Drive read locks if we just saw an active remote host
        if any_read_error and self._last_known_remote_lock and (now - self._last_known_remote_lock_at) < 25.0:
            return HostLock.from_dict(self._last_known_remote_lock.to_dict())

        if not any_read_error:
            self._last_known_remote_lock = None
            self._last_known_remote_lock_at = 0.0
        return HostLock(is_locked=False, status="idle")

    def save_lock(self, lock: HostLock) -> None:
        self._write_lock_bundle(lock, include_session=True)
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

            # Tombstone any dead/stale previous lock epoch before creating ours
            if current.is_locked and current.lock_epoch and (
                current.machine_id != self.local_config.machine_id or current.host_user_id != user.id
            ):
                self._write_lock_tombstone(current.lock_epoch, released_by=user.username)

            reuse_epoch = (
                current.lock_epoch
                if (current.is_actively_locked(now) and current.machine_id == self.local_config.machine_id and current.lock_epoch)
                else ""
            )
            lock_epoch = reuse_epoch or f"{int(now * 1000)}-{secrets.token_hex(4)}"

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
                lock_epoch=lock_epoch,
            )
            self.save_lock(new_lock)

            # Re-verify ownership to guard against simultaneous cloud folder writes
            time.sleep(0.15)
            check = self.get_lock()
            if check.is_locked and (
                check.machine_id != self.local_config.machine_id or check.host_user_id != user.id
            ):
                if not reuse_epoch:
                    self._write_lock_tombstone(lock_epoch, released_by="race_lost")
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
            if current.lock_epoch:
                self._write_lock_tombstone(current.lock_epoch, released_by=user.username if user else "")
            self._last_known_remote_lock = None
            self._last_known_remote_lock_at = 0.0
            unlocked = HostLock(is_locked=False, status="idle")
            self._write_lock_bundle(unlocked, include_session=False)
            self._emit("lock_updated", unlocked.to_dict())
            self.git_push_if_enabled("Release host lock and sync world")
            return unlocked

    def force_unlock(self) -> HostLock:
        """Admin emergency lock override."""
        with self._lock_mutex:
            self._owns_active_lock = False
            self._stop_heartbeat_loop()
            current = self.get_lock()
            if current.lock_epoch:
                self._write_lock_tombstone(current.lock_epoch, released_by="force_unlock")
            try:
                if self.locks_dir.exists():
                    for item in list(self.locks_dir.iterdir()):
                        if item.is_file() and item.name.startswith("session-") and item.name.endswith(".json"):
                            epoch = item.name[len("session-"):-len(".json")]
                            self._write_lock_tombstone(epoch, released_by="force_unlock")
            except OSError:
                pass
            self._last_known_remote_lock = None
            self._last_known_remote_lock_at = 0.0
            unlocked = HostLock(is_locked=False, status="idle")
            self._write_lock_bundle(unlocked, include_session=False)
            self._emit("lock_updated", unlocked.to_dict())
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

    def ensure_runtime_synced_when_idle(self, strict: bool = False) -> None:
        """
        Ensures that before an idle machine reads or edits config files (like server.properties),
        if a newer or different snapshot/manifest exists in the Shared Cloud Folder (from another member's session),
        it pulls that snapshot first so we never clobber a newer world save!
        """
        with self._sync_mutex:
            manifest = self._read_sync_manifest()
            if not self.latest_snapshot_path.exists() and not manifest:
                self._ensure_default_runtime_files()
                return
            try:
                manifest_seq = int(manifest.get("snapshotSeq", 0)) if manifest else 0
                if self.latest_snapshot_path.exists():
                    st = self.latest_snapshot_path.stat()
                    last_mtime, last_size, last_path = self._last_synced_snapshot_sig
                    snap_changed = (
                        abs(st.st_mtime - last_mtime) > 1e-3
                        or st.st_size != last_size
                        or str(self.latest_snapshot_path) != last_path
                    )
                else:
                    snap_changed = bool(manifest)

                manifest_changed = bool(manifest and manifest_seq > 0 and manifest_seq != self._last_synced_manifest_seq)
                if snap_changed or manifest_changed:
                    self.pull_world_to_runtime(strict=strict)
            except OSError:
                if strict:
                    raise

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

    def _remove_path_with_retry(self, target: Path) -> None:
        for attempt in range(4):
            try:
                if target.is_dir():
                    shutil.rmtree(target)
                else:
                    target.unlink(missing_ok=True)
                return
            except OSError:
                if attempt == 3 and target.is_dir():
                    shutil.rmtree(target, ignore_errors=True)
                else:
                    time.sleep(0.1 * (attempt + 1))

    def _extract_zip_to_runtime_safely(self, zip_source: Path) -> Dict[str, Any]:
        """
        Extracts a snapshot or backup zip into a temporary staging folder first so all
        entries & CRCs are verified BEFORE clearing `./data/runtime-server`.
        Preserves static JVM/Paper binaries (`server.jar`, `libraries/`, `versions/`,
        `cache/`, `logs/`, `crash-reports/`, `plugins/.paper-remapped`).
        """
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        staging_dir = self.base_data_dir / f".pull_staging_{os.getpid()}_{threading.get_ident()}"
        preserved_entries = {"server.jar", "jar-version.json", "cache", "libraries", "versions", "logs", "crash-reports"}

        try:
            if staging_dir.exists():
                shutil.rmtree(staging_dir, ignore_errors=True)
            staging_dir.mkdir(parents=True, exist_ok=True)

            file_count = 0
            with zipfile.ZipFile(zip_source, "r") as zf:
                infos = zf.infolist()
                if not infos:
                    raise ValueError(f"Archive '{zip_source.name}' is empty.")
                self._safe_extract_zip(zf, staging_dir)
                file_count = sum(1 for m in infos if not m.is_dir())

            # Remove stale Minecraft session.lock files inside staging
            for lock_file in staging_dir.rglob("session.lock"):
                try:
                    lock_file.unlink()
                except OSError:
                    pass

            # Clean existing dynamic world & config files in runtime_dir only AFTER staging succeeded
            for item in list(self.runtime_dir.iterdir()):
                if item.name in preserved_entries:
                    continue
                if item.name == "plugins" and item.is_dir():
                    for sub in list(item.iterdir()):
                        if sub.name == ".paper-remapped":
                            continue
                        self._remove_path_with_retry(sub)
                    continue
                self._remove_path_with_retry(item)

            # Move verified staged items into runtime_dir without ever nesting dirs on locked paths
            def _place_entry(src: Path, dst: Path) -> None:
                if dst.exists():
                    self._remove_path_with_retry(dst)
                if not dst.exists():
                    shutil.move(str(src), str(dst))
                elif src.is_dir():
                    shutil.copytree(str(src), str(dst), dirs_exist_ok=True, copy_function=shutil.copyfile)
                else:
                    shutil.copyfile(str(src), str(dst))

            for staged_item in list(staging_dir.iterdir()):
                if staged_item.name in preserved_entries:
                    continue
                dest_item = self.runtime_dir / staged_item.name
                if staged_item.name == "plugins" and staged_item.is_dir():
                    dest_item.mkdir(parents=True, exist_ok=True)
                    for sub in list(staged_item.iterdir()):
                        if sub.name == ".paper-remapped":
                            continue
                        _place_entry(sub, dest_item / sub.name)
                    continue
                _place_entry(staged_item, dest_item)

            self._ensure_default_runtime_files()
            return {"fileCount": file_count}
        finally:
            if staging_dir.exists():
                shutil.rmtree(staging_dir, ignore_errors=True)

    def _resolve_verified_snapshot_source(self, manifest: Optional[Dict[str, Any]]) -> Tuple[Optional[Path], str]:
        """
        Resolves the authoritative snapshot zip matching `sync-manifest.json` (checking `latest-server.zip`,
        then its paired backup archive, then Drive conflict copies).
        Returns `(path_or_none, mismatch_reason)`.
        """
        snap_exists = self.latest_snapshot_path.exists()
        if not manifest:
            return (self.latest_snapshot_path if snap_exists else None), ""

        exp_hash = str(manifest.get("sha256", "")).strip().lower()
        exp_size = int(manifest.get("sizeBytes", -1) or -1)
        manifest_ts = float(manifest.get("updatedAt", 0.0) or 0.0)
        host_name = str(manifest.get("hostUsername", "") or "the previous host")
        backup_name = Path(str(manifest.get("backupFile", "") or "")).name

        def _matches(p: Path) -> bool:
            if not p.exists() or not p.is_file():
                return False
            try:
                if exp_size >= 0 and p.stat().st_size != exp_size:
                    return False
                if exp_hash and self._sha256_file(p).lower() != exp_hash:
                    return False
                return True
            except OSError:
                return False

        if snap_exists and _matches(self.latest_snapshot_path):
            return self.latest_snapshot_path, ""

        # If backup file in sync-manifest already arrived cleanly (common when Drive freezes latest-server.zip), use it!
        if backup_name and backup_name not in (".", ".."):
            bk_path = self.backups_dir / backup_name
            if _matches(bk_path):
                return bk_path, ""

        conflict_pattern = re.compile(r"^latest-server\s*\(\d+\)\.zip$", re.IGNORECASE)
        if self.snapshots_dir.exists():
            for item in self.snapshots_dir.iterdir():
                if item.is_file() and conflict_pattern.match(item.name) and _matches(item):
                    return item, ""

        # Backward compatibility: if a legacy v1.0.1 peer pushed a significantly newer latest-server.zip without manifest
        if snap_exists:
            try:
                snap_st = self.latest_snapshot_path.stat()
                if manifest_ts > 0 and snap_st.st_mtime > (manifest_ts + 15.0) and snap_st.st_size > 0:
                    return self.latest_snapshot_path, ""
            except OSError:
                pass

        exp_mb = round(max(0, exp_size) / (1024 * 1024), 1)
        reason = (
            f"Cloud sync is still catching up from {host_name}'s session "
            f"(expected {exp_mb} MB snapshot"
            + (f" / backup '{backup_name}'" if backup_name else "")
            + "). Wait for Google Drive / cloud sync to finish before hosting or modifying files so progress is not overwritten!"
        )
        return None, reason

    def pull_world_to_runtime(self, strict: bool = False) -> Dict[str, Any]:
        """
        Extracts the verified latest world snapshot from the Shared Sync Folder into `./data/runtime-server`.
        Preserves static JVM/Paper libraries (`server.jar`, `libraries/`, `versions/`, `cache/`, `plugins/.paper-remapped`).
        When `strict=True` (used before starting the server or applying host edits), refuses to fall back
        to stale local files if cloud sync is incomplete or mismatched against `sync-manifest.json`.
        """
        with self._sync_mutex:
            self._ensure_shared_dirs()
            self.runtime_dir.mkdir(parents=True, exist_ok=True)
            self.git_pull_if_enabled()

            transfer_msg = self._has_active_cloud_transfer()
            if transfer_msg:
                if strict:
                    raise RuntimeError(f"{transfer_msg} Wait for cloud sync to finish before continuing.")
                return {"synced": False, "reason": transfer_msg}

            manifest = self._read_sync_manifest()
            source_zip, mismatch_reason = self._resolve_verified_snapshot_source(manifest)
            if mismatch_reason:
                if strict:
                    raise RuntimeError(mismatch_reason)
                return {"synced": False, "reason": mismatch_reason}

            if source_zip is None or not source_zip.exists():
                self._ensure_default_runtime_files()
                return {"synced": False, "reason": "No existing shared snapshot found; initialized fresh runtime."}

            # Retry up to 5 times in case Google Drive / OneDrive holds a transient lock while reading/extracting
            last_err: Optional[Exception] = None
            for attempt in range(5):
                try:
                    self._extract_zip_to_runtime_safely(source_zip)
                    last_err = None
                    break
                except (zipfile.BadZipFile, zlib.error, ValueError, OSError) as e:
                    last_err = e
                    time.sleep(0.18 * (attempt + 1))

            if last_err is not None:
                msg = f"World snapshot '{source_zip.name}' could not be read cleanly ({last_err}). Wait for cloud sync to finish."
                if strict:
                    raise RuntimeError(msg)
                self._ensure_default_runtime_files()
                return {"synced": False, "reason": f"Using local runtime files ({msg})"}

            if self.latest_snapshot_path.exists():
                st = self.latest_snapshot_path.stat()
                snap_mtime, snap_size = st.st_mtime, st.st_size
            else:
                st = source_zip.stat()
                snap_mtime, snap_size = st.st_mtime, st.st_size

            manifest_seq = int(manifest.get("snapshotSeq", 0)) if manifest else 0
            self._last_synced_snapshot_mtime = snap_mtime
            self._last_synced_snapshot_sig = (snap_mtime, snap_size, str(self.latest_snapshot_path))
            self._save_sync_state(snap_mtime, snap_size, str(self.latest_snapshot_path), manifest_seq=manifest_seq)
            return {
                "synced": True,
                "snapshotPath": str(source_zip),
                "sizeBytes": snap_size,
                "timestamp": time.time(),
            }

    def push_world_from_runtime(
        self,
        create_backup: bool = True,
        backup_label: str = "auto",
        host_username: str = "",
    ) -> Dict[str, Any]:
        """
        Packages `./data/runtime-server` (worlds, server.properties, ops, whitelist, plugins, configs)
        in local temp storage first, computes its SHA-256 manifest, writes optional backup,
        updates `snapshots/latest-server.zip` in-place (preserving Cloud File IDs), and updates `sync-manifest.json`.
        """
        with self._sync_mutex:
            self._ensure_shared_dirs()
            self.runtime_dir.mkdir(parents=True, exist_ok=True)

            # Build zip inside local non-cloud data_dir so Google Drive never sees half-written temp zips
            tmp_zip = self.base_data_dir / f".push_stage.{os.getpid()}.{threading.get_ident()}.zip"
            excluded_top = {"cache", "libraries", "versions", "logs", "crash-reports"}
            excluded_files = {"server.jar", "jar-version.json", "session.lock"}

            file_count = 0
            backup_name = None
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

                sha256_hex = self._sha256_file(tmp_zip)
                staged_size = tmp_zip.stat().st_size

                if create_backup:
                    ts = time.strftime("%Y%m%d-%H%M%S")
                    safe_label = "".join(c for c in backup_label if c.isalnum() or c in ("-", "_")) or "backup"
                    backup_name = f"world-{ts}-{safe_label}.zip"
                    backup_target = self.backups_dir / backup_name
                    shutil.copyfile(tmp_zip, backup_target)
                    self._prune_old_backups()

                self._copy_file_preserving_cloud_id(tmp_zip, self.latest_snapshot_path)
                try:
                    os.utime(self.latest_snapshot_path, None)
                except OSError:
                    pass

                st = self.latest_snapshot_path.stat()
                size_bytes = st.st_size
                manifest = self._write_sync_manifest(
                    self.latest_snapshot_path,
                    sha256_hex=sha256_hex,
                    size_bytes=staged_size,
                    file_count=file_count,
                    backup_file=backup_name,
                    host_username=host_username or backup_label,
                )
            finally:
                try:
                    if tmp_zip.exists():
                        tmp_zip.unlink(missing_ok=True)
                except OSError:
                    pass

            self._last_synced_snapshot_mtime = st.st_mtime
            self._last_synced_snapshot_sig = (st.st_mtime, st.st_size, str(self.latest_snapshot_path))
            self._save_sync_state(
                st.st_mtime,
                st.st_size,
                str(self.latest_snapshot_path),
                manifest_seq=int(manifest.get("snapshotSeq", 0)),
            )

            self.git_push_if_enabled(f"Sync world snapshot ({file_count} files)")
            return {
                "pushed": True,
                "fileCount": file_count,
                "sizeBytes": size_bytes,
                "sha256": sha256_hex,
                "backupFile": backup_name,
                "timestamp": time.time(),
            }

    def list_backups(self) -> List[Dict[str, Any]]:
        self._ensure_shared_dirs()
        entries = []
        for f in self.backups_dir.glob("*.zip"):
            try:
                st = f.stat()
                entries.append((st.st_mtime, {
                    "name": f.name,
                    "sizeBytes": st.st_size,
                    "createdAt": st.st_mtime,
                }))
            except OSError:
                continue
        entries.sort(key=lambda item: item[0], reverse=True)
        return [item[1] for item in entries]

    def restore_backup(self, backup_name: str) -> Dict[str, Any]:
        safe_name = Path(backup_name).name
        if not safe_name or safe_name in (".", ".."):
            raise ValueError("Invalid backup name.")

        with self._sync_mutex:
            self._ensure_shared_dirs()
            source = self.backups_dir / safe_name
            if not source.exists() or not source.is_file():
                raise FileNotFoundError(f"Backup '{safe_name}' not found.")

            # Stage-extract & validate every entry/CRC directly from backup BEFORE modifying latest-server.zip or runtime-server
            try:
                extract_info = self._extract_zip_to_runtime_safely(source)
            except (zipfile.BadZipFile, zlib.error, ValueError) as e:
                raise ValueError(f"Backup '{safe_name}' is invalid or corrupted: {e}")
            except OSError as e:
                raise RuntimeError(f"Backup file '{safe_name}' is temporarily locked by cloud sync ({e}). Please try again.")

            # Promote backup -> latest-server.zip in-place (preserving Cloud File ID) with current mtime
            try:
                sha256_hex = self._sha256_file(source)
                src_size = source.stat().st_size
                self._copy_file_preserving_cloud_id(source, self.latest_snapshot_path)
                try:
                    os.utime(self.latest_snapshot_path, None)
                except OSError:
                    pass
            except OSError as e:
                raise RuntimeError(
                    f"Could not update '{self.latest_snapshot_path.name}' because cloud sync locked the file ({e}). Try again in a moment."
                )

            st = self.latest_snapshot_path.stat()
            manifest = self._write_sync_manifest(
                self.latest_snapshot_path,
                sha256_hex=sha256_hex,
                size_bytes=src_size,
                file_count=extract_info.get("fileCount", 0),
                backup_file=safe_name,
                host_username="Restore",
            )
            self._last_synced_snapshot_mtime = st.st_mtime
            self._last_synced_snapshot_sig = (st.st_mtime, st.st_size, str(self.latest_snapshot_path))
            self._save_sync_state(
                st.st_mtime,
                st.st_size,
                str(self.latest_snapshot_path),
                manifest_seq=int(manifest.get("snapshotSeq", 0)),
            )

            # Clean up any empty/corrupt unlocked host-lock.json cleanly
            current_lock = self.get_lock()
            if not current_lock.is_actively_locked():
                try:
                    self.force_unlock()
                except Exception:
                    pass

            self.git_push_if_enabled(f"Restore backup {safe_name}")
            return {
                "restored": safe_name,
                "pull": {
                    "synced": True,
                    "snapshotPath": str(self.latest_snapshot_path),
                    "sizeBytes": st.st_size,
                    "fileCount": extract_info.get("fileCount", 0),
                    "timestamp": time.time(),
                },
            }

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
