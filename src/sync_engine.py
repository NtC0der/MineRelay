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
    MAX_DELTA_CHAIN = 25
    MAX_DELTA_RATIO = 0.50
    STORED_EXTENSIONS = frozenset({
        ".mca", ".mcr", ".mcc", ".dat", ".dat_old", ".nbt",
        ".jar", ".zip", ".gz", ".xz", ".bz2", ".zst",
        ".png", ".jpg", ".jpeg", ".webp", ".ogg", ".mp3", ".bin",
    })
    EXCLUDED_TOP_DIRS = frozenset({
        "cache", "libraries", "versions", "bundler", "logs",
        "crash-reports", "timings", "debug",
    })
    EXCLUDED_ROOT_FILES = frozenset({"server.jar", "jar-version.json", "session.lock"})

    def __init__(self, base_data_dir: str, event_callback: Optional[Callable[[str, Any], None]] = None):
        self.base_data_dir = Path(base_data_dir).resolve()
        self.base_data_dir.mkdir(parents=True, exist_ok=True)
        self.local_config_path = self.base_data_dir / "local-config.json"
        self.sync_state_path = self.base_data_dir / ".sync-state.json"
        self.runtime_dir = self.base_data_dir / "runtime-server"
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.local_backups_dir = self.base_data_dir / "backups"
        self.local_backups_dir.mkdir(parents=True, exist_ok=True)
        self.default_shared_dir = self.base_data_dir / "shared-sync"

        self.event_callback = event_callback
        self.telemetry_provider: Optional[Callable[[], Dict[str, Any]]] = None
        self.progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None
        self._lock_mutex = threading.RLock()
        self._sync_mutex = threading.RLock()
        self._cluster_merge_lock = threading.RLock()
        self._browse_lock = threading.Lock()
        self._sync_progress_lock = threading.Lock()
        self._sync_progress: Dict[str, Any] = {
            "active": False,
            "operation": "",
            "phase": "",
            "label": "",
            "detail": "",
            "percent": 0,
            "indeterminate": False,
            "doneFiles": 0,
            "totalFiles": 0,
            "doneBytes": 0,
            "totalBytes": 0,
            "updatedAt": 0.0,
        }
        self._last_progress_emit_at: float = 0.0
        self._last_progress_emit_pct: int = -1
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._heartbeat_stop = threading.Event()
        self._owns_active_lock = False
        self._active_lock_epoch: str = ""
        self._last_synced_snapshot_mtime: float = 0.0
        self._last_synced_snapshot_sig: Tuple[float, int, str] = (0.0, -1, "")
        self._last_synced_snapshot_hash: str = ""
        self._last_synced_manifest_seq: int = 0
        self._file_hash_cache: Dict[str, Tuple[int, int, str]] = {}
        self._archive_hash_cache: Dict[str, Tuple[int, int, str]] = {}
        self._last_sync_stats: Dict[str, Any] = {}
        self._last_verified_base_sig: Tuple[float, int, str] = (0.0, -1, "")
        self._last_known_remote_lock: Optional[HostLock] = None
        self._last_known_remote_lock_at: float = 0.0
        # track_key -> (last_seen_hb, first_seen_local, last_advance_local)
        self._remote_hb_tracker: Dict[str, Tuple[float, float, float]] = {}
        self._known_released_epochs: set = set()
        self._cached_own_lock: Optional[HostLock] = None
        self._cached_own_lock_mtime: float = 0.0
        self._last_session_write_at: float = 0.0
        self._last_telemetry_publish_at: float = 0.0

        self.local_config = self._load_local_config()
        self._ensure_shared_dirs()
        self._init_snapshot_mtime()

    def _save_sync_state(
        self,
        mtime: float,
        size: int,
        path_str: str,
        manifest_seq: Optional[int] = None,
        sha256_hex: str = "",
    ) -> None:
        seq = manifest_seq if manifest_seq is not None else self._last_synced_manifest_seq
        self._last_synced_manifest_seq = seq
        if sha256_hex:
            self._last_synced_snapshot_hash = sha256_hex.lower()
        try:
            self._write_json_atomic(
                self.sync_state_path,
                {
                    "mtime": mtime,
                    "size": size,
                    "path": path_str,
                    "manifestSeq": seq,
                    "sha256": self._last_synced_snapshot_hash,
                    "fileHashes": {
                        k: [int(v[0]), int(v[1]), str(v[2])]
                        for k, v in self._file_hash_cache.items()
                    },
                },
                in_place_if_exists=False,
            )
        except Exception:
            pass

    def reset_sync_state(self) -> None:
        with self._sync_mutex:
            self._last_synced_snapshot_mtime = 0.0
            self._last_synced_snapshot_sig = (0.0, -1, "")
            self._last_synced_snapshot_hash = ""
            self._last_synced_manifest_seq = 0
            self._file_hash_cache.clear()
            self._archive_hash_cache.clear()
            self._last_sync_stats = {}
            self._last_verified_base_sig = (0.0, -1, "")
            with self._lock_mutex:
                self._last_known_remote_lock = None
                self._last_known_remote_lock_at = 0.0
                self._remote_hb_tracker.clear()
                self._known_released_epochs.clear()
                self._cached_own_lock = None
                self._cached_own_lock_mtime = 0.0
                self._last_session_write_at = 0.0
                self._last_telemetry_publish_at = 0.0
            try:
                if self.sync_state_path.exists():
                    self.sync_state_path.unlink(missing_ok=True)
            except OSError:
                pass

    def _init_snapshot_mtime(self) -> None:
        try:
            if (self.runtime_dir / "server.properties").exists() and self.sync_state_path.exists():
                saved = json.loads(self.sync_state_path.read_text(encoding="utf-8"))
                raw_hashes = saved.get("fileHashes")
                if isinstance(raw_hashes, dict):
                    loaded_hashes: Dict[str, Tuple[int, int, str]] = {}
                    for k, v in raw_hashes.items():
                        if isinstance(v, list) and len(v) == 3:
                            try:
                                loaded_hashes[str(k)] = (int(v[0]), int(v[1]), str(v[2]).lower())
                            except (TypeError, ValueError):
                                continue
                    self._file_hash_cache = loaded_hashes
                saved_path_str = str(saved.get("path") or "")
                cand_path = Path(saved_path_str) if saved_path_str else self.latest_snapshot_path
                if cand_path.exists() and cand_path.is_file():
                    st = cand_path.stat()
                    if (
                        saved_path_str == str(cand_path)
                        and int(saved.get("size", -1)) == st.st_size
                        and abs(float(saved.get("mtime", 0.0)) - st.st_mtime) < 1e-3
                    ):
                        self._last_synced_snapshot_mtime = st.st_mtime
                        self._last_synced_snapshot_sig = (st.st_mtime, st.st_size, str(cand_path))
                        self._last_synced_manifest_seq = int(saved.get("manifestSeq", 0) or 0)
                        self._last_synced_snapshot_hash = str(saved.get("sha256", "") or "").lower()
        except Exception:
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
    def file_index_path(self) -> Path:
        return self.snapshots_dir / "file-index.json"

    @property
    def deltas_dir(self) -> Path:
        return self.snapshots_dir / "deltas"

    @property
    def locks_dir(self) -> Path:
        return self.shared_dir / "locks"

    def _iter_locks_dirs(self) -> List[Path]:
        """Returns `locks/` plus any Google Drive conflict `locks (N)/` folders."""
        dirs: List[Path] = []
        if self.locks_dir.exists():
            dirs.append(self.locks_dir)
        pat = re.compile(r"^locks\s*\(\d+\)$", re.IGNORECASE)
        try:
            if self.shared_dir.exists():
                for item in self.shared_dir.iterdir():
                    if item.is_dir() and pat.match(item.name):
                        dirs.append(item)
        except OSError:
            pass
        return dirs

    @property
    def host_telemetry_path(self) -> Path:
        return self.locks_dir / "host-telemetry.json"

    @property
    def shared_tunnel_dir(self) -> Path:
        return self.shared_dir / "tunnel"

    def _ensure_shared_dirs(self, cleanup_conflicts: bool = True) -> None:
        try:
            if (
                not cleanup_conflicts
                and os.path.isdir(self.locks_dir)
                and os.path.isdir(self.shared_tunnel_dir)
                and os.path.isdir(self.snapshots_dir)
                and os.path.isdir(self.deltas_dir)
                and os.path.isdir(self.backups_dir)
            ):
                return
            self.shared_dir.mkdir(parents=True, exist_ok=True)
            self.snapshots_dir.mkdir(parents=True, exist_ok=True)
            self.deltas_dir.mkdir(parents=True, exist_ok=True)
            self.backups_dir.mkdir(parents=True, exist_ok=True)
            self.locks_dir.mkdir(parents=True, exist_ok=True)
            self.shared_tunnel_dir.mkdir(parents=True, exist_ok=True)
            if cleanup_conflicts:
                self._cleanup_cloud_conflict_files()
        except Exception:
            pass

    @classmethod
    def _is_static_runtime_relpath(cls, rel_posix: str) -> bool:
        norm = rel_posix.replace("\\", "/").lstrip("/")
        if not norm:
            return False
        parts = norm.split("/")
        top = parts[0]
        if top in cls.EXCLUDED_TOP_DIRS:
            return True
        if len(parts) == 1:
            if top in cls.EXCLUDED_ROOT_FILES:
                return True
            if top.startswith("server-") and top.endswith(".jar"):
                return True
        fname_lower = parts[-1].lower()
        if fname_lower.endswith((".hprof", ".lck", ".tmp", ".pid")) or fname_lower == "session.lock":
            return True
        if ".paper-remapped" in parts or "tmp-client" in parts:
            return True
        if len(parts) >= 3 and parts[0] == "plugins" and parts[1] == "spark" and parts[2] in ("profiles", "dumps"):
            return True
        return False

    @classmethod
    def _zip_write_file(cls, zf: zipfile.ZipFile, src_file: Path, arcname: str) -> None:
        ext = Path(arcname).suffix.lower()
        if ext in cls.STORED_EXTENSIONS:
            zf.write(src_file, arcname=arcname, compress_type=zipfile.ZIP_STORED)
        else:
            zf.write(src_file, arcname=arcname, compress_type=zipfile.ZIP_DEFLATED, compresslevel=1)

    def _fast_file_sha256(self, abs_path: Path, rel_posix: str, now: Optional[float] = None) -> Tuple[str, int]:
        st = abs_path.stat()
        size = st.st_size
        mtime_ns = getattr(st, "st_mtime_ns", int(st.st_mtime * 1e9))
        t_now = now if now is not None else time.time()
        if size >= 262144 and (t_now - st.st_mtime) >= 2.0:
            cached = self._file_hash_cache.get(rel_posix)
            if cached and cached[0] == size and cached[1] == mtime_ns:
                return cached[2], size
        hex_hash = self._sha256_file(abs_path).lower()
        self._file_hash_cache[rel_posix] = (size, mtime_ns, hex_hash)
        return hex_hash, size

    @staticmethod
    def _strip_conflict_suffix(stem: str) -> str:
        """Strips Google Drive / OneDrive / Dropbox conflict suffixes like ' (1)' from a file stem."""
        return re.sub(r"\s*\(\d+\)$", "", stem).strip()

    @staticmethod
    def _sha256_file(target_path: Path) -> str:
        h = hashlib.sha256()
        with open(target_path, "rb") as f:
            while True:
                chunk = f.read(4 * 1024 * 1024)
                if not chunk:
                    break
                h.update(chunk)
        return h.hexdigest()

    def _cached_archive_sha256(self, target_path: Path) -> str:
        st = target_path.stat()
        mtime_ns = getattr(st, "st_mtime_ns", int(st.st_mtime * 1e9))
        key = os.path.normcase(str(target_path))
        cached = self._archive_hash_cache.get(key)
        if cached and cached[0] == mtime_ns and cached[1] == st.st_size:
            return cached[2]
        digest = self._sha256_file(target_path).lower()
        self._archive_hash_cache[key] = (mtime_ns, st.st_size, digest)
        return digest

    def _set_sync_progress(
        self,
        active: bool,
        operation: str = "",
        phase: str = "",
        label: str = "",
        detail: str = "",
        percent: int = 0,
        done_files: int = 0,
        total_files: int = 0,
        done_bytes: int = 0,
        total_bytes: int = 0,
        indeterminate: bool = False,
        force_emit: bool = False,
    ) -> None:
        pct = max(0, min(100, int(percent))) if active else 0
        now_mono = time.monotonic()
        snapshot: Optional[Dict[str, Any]] = None
        should_emit = False
        with self._sync_progress_lock:
            prev_active = bool(self._sync_progress.get("active"))
            prev_phase = str(self._sync_progress.get("phase") or "")
            self._sync_progress = {
                "active": bool(active),
                "operation": operation if active else "",
                "phase": phase if active else "",
                "label": label if active else "",
                "detail": detail if active else "",
                "percent": pct,
                "indeterminate": bool(indeterminate) if active else False,
                "doneFiles": max(0, int(done_files)) if active else 0,
                "totalFiles": max(0, int(total_files)) if active else 0,
                "doneBytes": max(0, int(done_bytes)) if active else 0,
                "totalBytes": max(0, int(total_bytes)) if active else 0,
                "updatedAt": time.time() if active else 0.0,
            }
            if (
                force_emit
                or bool(active) != prev_active
                or (phase if active else "") != prev_phase
                or abs(pct - self._last_progress_emit_pct) >= 3
                or (now_mono - self._last_progress_emit_at) >= 0.35
            ):
                self._last_progress_emit_at = now_mono
                self._last_progress_emit_pct = pct
                snapshot = dict(self._sync_progress)
                should_emit = True
        if should_emit and snapshot is not None and self.progress_callback:
            try:
                self.progress_callback(snapshot)
            except Exception:
                pass

    def get_cloud_transfer_details(self) -> Dict[str, Any]:
        """Returns structured details on active Google Drive / OneDrive / Dropbox temp transfers."""
        now = time.time()
        marker_names = {".tmp.drivedownload", ".tmp.driveupload", ".dropbox.cache"}
        total_bytes = 0
        file_count = 0
        primary_name = ""
        primary_bytes = 0
        primary_action = ""
        primary_folder: Optional[Path] = None
        for folder in (self.shared_dir, self.snapshots_dir, self.deltas_dir, self.backups_dir, self.locks_dir):
            try:
                with os.scandir(folder) as it:
                    for entry in it:
                        ename = entry.name
                        if ename in marker_names and entry.is_dir(follow_symlinks=False):
                            try:
                                with os.scandir(entry.path) as mit:
                                    for mentry in mit:
                                        try:
                                            if not mentry.is_file(follow_symlinks=False):
                                                continue
                                            st = mentry.stat(follow_symlinks=False)
                                            age = now - st.st_mtime
                                            if (st.st_size > 0 and age < 120) or (st.st_size == 0 and age < 15):
                                                file_count += 1
                                                total_bytes += st.st_size
                                                if not primary_name or st.st_size >= primary_bytes:
                                                    primary_name = mentry.name
                                                    primary_bytes = st.st_size
                                                    primary_action = "uploading" if "upload" in ename else "downloading"
                                                    primary_folder = folder
                                        except OSError:
                                            continue
                            except OSError:
                                pass
                            continue
                        lname = ename.lower()
                        if (
                            lname.endswith((".part", ".partial", ".crdownload"))
                            or (lname.startswith("~") and lname.endswith(".tmp"))
                        ) and entry.is_file(follow_symlinks=False):
                            try:
                                st = entry.stat(follow_symlinks=False)
                                if (now - st.st_mtime) < 60:
                                    file_count += 1
                                    total_bytes += st.st_size
                                    if not primary_name or st.st_size >= primary_bytes:
                                        primary_name = ename
                                        primary_bytes = st.st_size
                                        primary_action = "downloading"
                                        primary_folder = folder
                            except OSError:
                                continue
            except OSError:
                continue

        if file_count == 0:
            return {
                "active": False,
                "action": "",
                "fileCount": 0,
                "currentBytes": 0,
                "expectedBytes": 0,
                "percent": None,
                "message": "",
            }

        expected_bytes = 0
        pct: Optional[int] = None
        manifest = self._read_sync_manifest()
        if isinstance(manifest, dict):
            candidates: List[int] = []
            if primary_folder == self.deltas_dir:
                for d_entry in (manifest.get("deltas") or []):
                    if isinstance(d_entry, dict) and int(d_entry.get("sizeBytes") or 0) > 0:
                        candidates.append(int(d_entry["sizeBytes"]))
                if int(manifest.get("sizeBytes") or 0) > 0:
                    candidates.append(int(manifest["sizeBytes"]))
            elif primary_folder == self.backups_dir:
                for key in ("backupSizeBytes", "baseSizeBytes", "sizeBytes"):
                    val = int(manifest.get(key) or 0)
                    if val > 0:
                        candidates.append(val)
            else:
                for key in ("sizeBytes", "baseSizeBytes", "backupSizeBytes"):
                    val = int(manifest.get(key) or 0)
                    if val > 0:
                        candidates.append(val)
                for d_entry in (manifest.get("deltas") or []):
                    if isinstance(d_entry, dict) and int(d_entry.get("sizeBytes") or 0) > 0:
                        candidates.append(int(d_entry["sizeBytes"]))
            valid_cands = sorted(c for c in set(candidates) if c > 0 and total_bytes <= int(c * 1.05))
            if valid_cands:
                expected_bytes = valid_cands[0]
                pct = max(0, min(99, int((total_bytes * 100) / expected_bytes)))
        msg = (
            f"Cloud sync is currently {primary_action} files ({primary_name}, "
            f"{round(primary_bytes / (1024 * 1024), 1)} MB)."
        )
        return {
            "active": True,
            "action": primary_action,
            "fileCount": file_count,
            "currentBytes": total_bytes,
            "expectedBytes": expected_bytes,
            "percent": pct,
            "message": msg,
        }

    def get_sync_progress(self) -> Dict[str, Any]:
        with self._sync_progress_lock:
            if self._sync_progress.get("active"):
                return dict(self._sync_progress)
        cloud = self.get_cloud_transfer_details()
        if cloud.get("active"):
            pct = cloud.get("percent")
            action = str(cloud.get("action") or "downloading")
            label = (
                "Cloud Drive Downloading Files..."
                if action == "downloading"
                else "Cloud Drive Uploading Files..."
            )
            return {
                "active": True,
                "operation": "cloud_transfer",
                "phase": action,
                "label": label,
                "detail": str(cloud.get("message") or ""),
                "percent": int(pct) if pct is not None else 0,
                "indeterminate": pct is None,
                "doneFiles": 0,
                "totalFiles": int(cloud.get("fileCount") or 0),
                "doneBytes": int(cloud.get("currentBytes") or 0),
                "totalBytes": int(cloud.get("expectedBytes") or 0),
                "updatedAt": time.time(),
            }
        return {
            "active": False,
            "operation": "",
            "phase": "",
            "label": "",
            "detail": "",
            "percent": 0,
            "indeterminate": False,
            "doneFiles": 0,
            "totalFiles": 0,
            "doneBytes": 0,
            "totalBytes": 0,
            "updatedAt": 0.0,
        }

    def _has_active_cloud_transfer(self) -> Optional[str]:
        """Returns a human-readable notice if Google Drive / OneDrive / Dropbox is actively syncing a temp file."""
        details = self.get_cloud_transfer_details()
        return str(details["message"]) if details.get("active") else None

    def _move_or_copy_to_cloud(self, src_path: Path, dst_path: Path, is_immutable: bool = False) -> None:
        """
        Places a completed local temp archive into the cloud sync folder atomically when possible.
        For immutable archives (deltas, backups), prefers instant os.replace / move before copying.
        For latest-server.zip, tries atomic os.replace first (with retry) and falls back to
        in-place preservation copy if cross-device or Drive holds an active handle.
        """
        dst_path.parent.mkdir(parents=True, exist_ok=True)
        for attempt in range(4):
            try:
                os.replace(src_path, dst_path)
                return
            except OSError as e:
                import errno
                if getattr(e, "errno", None) == errno.EXDEV:
                    break
                if attempt < 3:
                    time.sleep(0.12 * (attempt + 1))

        if is_immutable:
            tmp_dst = dst_path.parent / f".{dst_path.name}.tmp.{os.getpid()}.{threading.get_ident()}"
            try:
                shutil.copyfile(src_path, tmp_dst)
                os.replace(tmp_dst, dst_path)
                return
            except OSError:
                pass
            finally:
                try:
                    if tmp_dst.exists():
                        tmp_dst.unlink(missing_ok=True)
                except OSError:
                    pass
        self._copy_file_preserving_cloud_id(src_path, dst_path)

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
        snapshot_seq: Optional[int] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        prev = self._read_sync_manifest()
        prev_seq = int(prev.get("snapshotSeq", 0)) if prev else 0
        exp_seq, _, _ = self.get_expected_snapshot_state()
        base_seq = max(prev_seq, self._last_synced_manifest_seq, exp_seq)
        next_seq = snapshot_seq if snapshot_seq is not None and snapshot_seq > base_seq else (base_seq + 1)
        manifest: Dict[str, Any] = {
            "snapshotSeq": next_seq,
            "sha256": sha256_hex,
            "sizeBytes": int(size_bytes),
            "fileCount": int(file_count),
            "backupFile": backup_file or "",
            "hostUsername": host_username or "",
            "machineId": self.local_config.machine_id,
            "machineName": self.local_config.machine_name,
            "updatedAt": time.time(),
        }
        if extra:
            manifest.update(extra)
        self._write_json_atomic(self.sync_manifest_path, manifest, in_place_if_exists=True)
        return manifest

    def _load_released_epochs(self) -> set:
        with self._lock_mutex:
            released = set(self._known_released_epochs)
        found = set()
        for ldir in self._iter_locks_dirs():
            try:
                for item in ldir.iterdir():
                    if not item.is_file() or not item.name.startswith("released-") or not item.name.endswith(".json"):
                        continue
                    raw_epoch = item.name[len("released-"):-len(".json")]
                    epoch = self._strip_conflict_suffix(raw_epoch)
                    if epoch:
                        found.add(epoch)
                    if raw_epoch != epoch:
                        tdata = self._read_json_safe(item)
                        if isinstance(tdata, dict) and tdata.get("lockEpoch"):
                            ep = str(tdata["lockEpoch"]).strip()
                            if ep:
                                found.add(ep)
            except OSError:
                pass
        if found:
            with self._lock_mutex:
                self._known_released_epochs.update(found)
            released.update(found)
        return released

    def _is_lock_effectively_active(self, lock: HostLock, now: Optional[float] = None) -> bool:
        """Evaluates whether `lock` is active, respecting observed remote heartbeat progression for clock skew."""
        if not lock.is_locked:
            return False
        t_now = now if now is not None else time.time()
        if lock.machine_id and lock.machine_id != self.local_config.machine_id:
            track_key = f"{lock.machine_id}:{lock.lock_epoch or int(lock.started_at)}"
            with self._lock_mutex:
                prev = self._remote_hb_tracker.get(track_key)
            if prev is not None:
                _, first_seen, obs_at = prev
                if obs_at > 0.0 and (t_now - obs_at) <= LOCK_STALE_TIMEOUT_SEC:
                    return True
                ref_obs = obs_at if obs_at > 0.0 else first_seen
                if lock.last_heartbeat > t_now + 30.0 and (t_now - ref_obs) > LOCK_STALE_TIMEOUT_SEC:
                    return False
        return lock.is_actively_locked(t_now)

    def _merge_cluster_config_conflict(self, conflict_path: Path) -> None:
        """Merges accounts, deletion tombstones, and newer settings from a cloud conflict config file before cleanup."""
        try:
            with self._cluster_merge_lock:
                cdata = self._read_json_safe(conflict_path)
                if not isinstance(cdata, dict) or not cdata:
                    return
                ccfg = ClusterConfig.from_dict(cdata)
                mdata = self._read_json_safe(self.cluster_config_path)
                mcfg = self.load_cluster_config(_reconcile_conflicts=False)

                now = time.time()
                merged_deleted: Dict[str, float] = {}
                for raw_map in (
                    getattr(mcfg, "deleted_accounts", None) if mcfg is not None else None,
                    mdata.get("deletedAccounts") if isinstance(mdata, dict) else None,
                    getattr(ccfg, "deleted_accounts", None),
                    cdata.get("deletedAccounts"),
                ):
                    if isinstance(raw_map, dict):
                        for k, v in raw_map.items():
                            acc_id = str(k).strip()
                            if not acc_id:
                                continue
                            try:
                                ts = float(v or 0.0)
                            except (TypeError, ValueError):
                                ts = 0.0
                            if ts > merged_deleted.get(acc_id, -1.0):
                                merged_deleted[acc_id] = ts
                if len(merged_deleted) > 200:
                    cutoff = now - (30 * 86400.0)
                    merged_deleted = {k: v for k, v in merged_deleted.items() if v >= cutoff}

                if mcfg is None:
                    ccfg.accounts = [
                        acc for acc in ccfg.accounts
                        if merged_deleted.get(acc.id, -1.0) < float(acc.created_at or 0.0)
                    ]
                    if hasattr(ccfg, "deleted_accounts"):
                        ccfg.deleted_accounts = merged_deleted
                    if ccfg.accounts:
                        self.save_cluster_config(ccfg)
                    return

                try:
                    c_mtime = conflict_path.stat().st_mtime
                except OSError:
                    c_mtime = 0.0
                try:
                    m_mtime = self.cluster_config_path.stat().st_mtime if self.cluster_config_path.exists() else 0.0
                except OSError:
                    m_mtime = 0.0

                c_is_newer = c_mtime >= m_mtime and c_mtime > 0.0
                changed = False

                # Purge any account in main config that was tombstoned at or after its creation
                kept_accounts: List[UserAccount] = []
                for acc in mcfg.accounts:
                    del_ts = merged_deleted.get(acc.id, -1.0)
                    if del_ts >= float(acc.created_at or 0.0):
                        changed = True
                    else:
                        kept_accounts.append(acc)
                if changed:
                    mcfg.accounts = kept_accounts

                # If the conflict file is newer than or equal to the main file, adopt its non-account cluster settings
                if c_is_newer:
                    for attr in (
                        "server_name",
                        "mc_software",
                        "mc_version",
                        "memory_min_mb",
                        "memory_max_mb",
                        "java_path",
                        "jvm_flags",
                        "server_port",
                        "tunnel_enabled",
                        "tunnel_provider",
                        "public_join_address",
                        "git_sync_enabled",
                        "auto_backup_on_stop",
                        "max_backups",
                        "idle_shutdown_minutes",
                        "auto_sync_interval_minutes",
                        "gamerules",
                    ):
                        if hasattr(ccfg, attr) and hasattr(mcfg, attr):
                            c_val = getattr(ccfg, attr)
                            m_val = getattr(mcfg, attr)
                            if c_val != m_val:
                                setattr(mcfg, attr, c_val)
                                changed = True

                by_id = {a.id: a for a in mcfg.accounts}
                for acc in ccfg.accounts:
                    if merged_deleted.get(acc.id, -1.0) >= float(acc.created_at or 0.0):
                        continue
                    existing = by_id.get(acc.id)
                    if existing is None:
                        mcfg.accounts.append(acc)
                        by_id[acc.id] = acc
                        changed = True
                    else:
                        if acc.last_active_at > existing.last_active_at:
                            existing.last_active_at = acc.last_active_at
                            changed = True
                        if acc.host_count > existing.host_count:
                            existing.host_count = acc.host_count
                            changed = True
                        if c_is_newer:
                            for acc_attr in ("username", "role", "passkey_hash", "passkey_salt", "passkey_hint"):
                                new_v = getattr(acc, acc_attr)
                                if new_v != getattr(existing, acc_attr):
                                    setattr(existing, acc_attr, new_v)
                                    changed = True

                if hasattr(mcfg, "deleted_accounts") and getattr(mcfg, "deleted_accounts", None) != merged_deleted:
                    mcfg.deleted_accounts = merged_deleted
                    changed = True
                if changed:
                    self.save_cluster_config(mcfg)
        except Exception:
            pass

    def _cleanup_cloud_conflict_files(self) -> None:
        """
        Reconciles and cleans up duplicate conflict files and folders created by Google Drive / OneDrive / Dropbox
        (e.g. 'host-lock (1).json', 'cluster-config (1).json', 'locks (1)/', 'snapshots (1)/', leftover '.tmp.*')
        after preserving any fresher active state, and prunes expired per-session tombstones in `locks/`.
        """
        conflict_pattern = re.compile(r"^(host-lock|cluster-config|latest-server|sync-manifest|file-index)\s*\(\d+\)\.(json|zip)$", re.IGNORECASE)
        folder_conflict_pattern = re.compile(r"^(locks|snapshots|tunnel)\s*\(\d+\)$", re.IGNORECASE)
        manifest = self._read_sync_manifest() if self.sync_manifest_path.exists() else None
        released_epochs = self._load_released_epochs()
        now = time.time()
        transfer_active = bool(self._has_active_cloud_transfer())

        # Reconcile duplicate conflict directories (e.g. Google Drive 'locks (1)', 'snapshots (1)', 'tunnel (1)')
        try:
            if self.shared_dir.exists():
                for item in list(self.shared_dir.iterdir()):
                    if not item.is_dir():
                        continue
                    fm = folder_conflict_pattern.match(item.name)
                    if not fm:
                        continue
                    base_dir_name = fm.group(1).lower()
                    if base_dir_name == "locks":
                        self.locks_dir.mkdir(parents=True, exist_ok=True)
                        dir_has_live_remote = False
                        for sub in list(item.rglob("*.json")):
                            norm_name = f"{self._strip_conflict_suffix(sub.stem)}{sub.suffix}"
                            target = self.locks_dir / norm_name
                            try:
                                if norm_name.startswith("released-"):
                                    epoch = norm_name[len("released-"):-len(".json")]
                                    if epoch:
                                        released_epochs.add(epoch)
                                        self._known_released_epochs.add(epoch)
                                    if not target.exists():
                                        shutil.copy2(sub, target)
                                elif norm_name.startswith("session-"):
                                    sdata = self._read_json_safe(sub)
                                    slock = HostLock.from_dict(sdata) if isinstance(sdata, dict) else None
                                    ep = (slock.lock_epoch if slock and slock.lock_epoch else norm_name[len("session-"):-len(".json")])
                                    if ep in released_epochs:
                                        continue
                                    if (
                                        slock
                                        and slock.is_locked
                                        and slock.machine_id != self.local_config.machine_id
                                        and self._is_lock_effectively_active(slock, now)
                                    ):
                                        dir_has_live_remote = True
                                    if not target.exists():
                                        shutil.copy2(sub, target)
                                    elif slock and slock.is_locked:
                                        cur_sdata = self._read_json_safe(target)
                                        cur_hb = float(cur_sdata.get("lastHeartbeat", 0.0) or 0.0) if isinstance(cur_sdata, dict) else 0.0
                                        if slock.last_heartbeat > cur_hb:
                                            shutil.copy2(sub, target)
                                elif norm_name == "host-telemetry.json":
                                    tdata = self._read_json_safe(sub)
                                    if isinstance(tdata, dict):
                                        t_upd = float(tdata.get("updatedAt", 0.0) or 0.0)
                                        t_epoch = str(tdata.get("lockEpoch") or "")
                                        if t_epoch and t_epoch in released_epochs:
                                            continue
                                        if (
                                            tdata.get("isActive")
                                            and str(tdata.get("machineId", "")) != self.local_config.machine_id
                                            and (now - t_upd) < LOCK_STALE_TIMEOUT_SEC
                                        ):
                                            dir_has_live_remote = True
                                        cur_t = self._read_json_safe(target) if target.exists() else None
                                        cur_upd = float(cur_t.get("updatedAt", 0.0) or 0.0) if isinstance(cur_t, dict) else 0.0
                                        if t_upd > cur_upd and (tdata.get("isActive") or cur_t is not None):
                                            shutil.copy2(sub, target)
                            except OSError:
                                pass
                        if not dir_has_live_remote:
                            for leftover in sorted(item.rglob("*"), reverse=True):
                                try:
                                    if leftover.is_file() and (
                                        leftover.name.lower() == "desktop.ini"
                                        or leftover.suffix.lower() == ".json"
                                        or ".tmp." in leftover.name
                                    ):
                                        leftover.unlink(missing_ok=True)
                                    elif leftover.is_dir() and not any(leftover.iterdir()):
                                        leftover.rmdir()
                                except OSError:
                                    pass
                            try:
                                if not any(item.iterdir()):
                                    item.rmdir()
                            except OSError:
                                pass
                    elif base_dir_name == "snapshots":
                        for sub in list(item.glob("backups/*.zip")):
                            target = self.backups_dir / sub.name
                            try:
                                if not target.exists() and sub.stat().st_size > 0:
                                    shutil.copy2(sub, target)
                            except OSError:
                                pass
                        for sub in list(item.glob("deltas/*.zip")):
                            norm_dname = f"{self._strip_conflict_suffix(sub.stem)}{sub.suffix}"
                            target = self.deltas_dir / norm_dname
                            try:
                                self.deltas_dir.mkdir(parents=True, exist_ok=True)
                                if not target.exists() and sub.stat().st_size > 0:
                                    shutil.copy2(sub, target)
                            except OSError:
                                pass
                        c_manifest_path = item / "sync-manifest.json"
                        c_zip_path = item / "latest-server.zip"
                        c_index_path = item / "file-index.json"
                        c_manifest = self._read_json_safe(c_manifest_path) if c_manifest_path.exists() else None
                        if isinstance(c_manifest, dict):
                            cur_seq = int(manifest.get("snapshotSeq", 0)) if manifest else 0
                            c_seq = int(c_manifest.get("snapshotSeq", 0) or 0)
                            c_mode = str(c_manifest.get("mode") or "full").lower()
                            c_hash = str(c_manifest.get("baseSha256") or c_manifest.get("sha256") or "").lower()
                            if c_seq > cur_seq:
                                try:
                                    if c_index_path.exists() and c_manifest.get("fileIndexSha256"):
                                        if self._sha256_file(c_index_path).lower() == str(c_manifest["fileIndexSha256"]).lower():
                                            self._copy_file_preserving_cloud_id(c_index_path, self.file_index_path)
                                    if c_zip_path.exists() and c_hash and self._sha256_file(c_zip_path).lower() == c_hash:
                                        self._copy_file_preserving_cloud_id(c_zip_path, self.latest_snapshot_path)
                                        self._write_json_atomic(self.sync_manifest_path, c_manifest, in_place_if_exists=True)
                                        manifest = c_manifest
                                    elif c_mode == "delta" and (
                                        (self.latest_snapshot_path.exists() and self._sha256_file(self.latest_snapshot_path).lower() == c_hash)
                                        or not c_hash
                                    ):
                                        self._write_json_atomic(self.sync_manifest_path, c_manifest, in_place_if_exists=True)
                                        manifest = c_manifest
                                except OSError:
                                    pass
                        if not transfer_active:
                            try:
                                recent = any((now - p.stat().st_mtime) < 15 for p in item.rglob("*") if p.is_file())
                            except OSError:
                                recent = False
                            if not recent:
                                try:
                                    shutil.rmtree(item, ignore_errors=True)
                                except OSError:
                                    pass
                    elif base_dir_name == "tunnel":
                        self.shared_tunnel_dir.mkdir(parents=True, exist_ok=True)
                        main_toml = self.shared_tunnel_dir / "playit.toml"
                        c_toml = item / "playit.toml"
                        try:
                            if c_toml.exists() and not main_toml.exists() and c_toml.stat().st_size > 0:
                                shutil.copy2(c_toml, main_toml)
                            shutil.rmtree(item, ignore_errors=True)
                        except OSError:
                            pass
        except OSError:
            pass

        released_epochs = self._load_released_epochs()

        for folder in (self.shared_dir, self.snapshots_dir):
            if not folder.exists():
                continue
            for item in sorted(
                folder.iterdir(),
                key=lambda p: (0 if p.name.lower().startswith("sync-manifest") else 1, p.name.lower()),
            ):
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
                should_unlink = True
                if stem_base == "host-lock":
                    try:
                        cdata = self._read_json_safe(item)
                        if isinstance(cdata, dict) and cdata:
                            clock = HostLock.from_dict(cdata)
                            main_data = self._read_json_safe(self.host_lock_path)
                            main_lock = HostLock.from_dict(main_data) if isinstance(main_data, dict) else HostLock()
                            if (
                                self._is_lock_effectively_active(clock, now)
                                and (not clock.lock_epoch or clock.lock_epoch not in released_epochs)
                                and (
                                    not self._is_lock_effectively_active(main_lock, now)
                                    or clock.last_heartbeat > main_lock.last_heartbeat
                                )
                            ):
                                self._write_json_atomic(self.host_lock_path, clock.to_dict(), in_place_if_exists=True)
                            elif (
                                not clock.is_locked
                                and not main_lock.is_locked
                                and clock.expected_snapshot_seq > main_lock.expected_snapshot_seq
                            ):
                                main_lock.expected_snapshot_seq = clock.expected_snapshot_seq
                                main_lock.expected_snapshot_hash = clock.expected_snapshot_hash
                                self._write_json_atomic(self.host_lock_path, main_lock.to_dict(), in_place_if_exists=True)
                    except Exception:
                        pass
                elif stem_base == "cluster-config":
                    self._merge_cluster_config_conflict(item)
                elif stem_base == "sync-manifest":
                    try:
                        cm = self._read_json_safe(item)
                        if isinstance(cm, dict) and cm.get("sha256"):
                            cur_seq = int(manifest.get("snapshotSeq", 0)) if manifest else 0
                            cm_seq = int(cm.get("snapshotSeq", 0) or 0)
                            cm_hash = str(cm.get("sha256", "")).lower()
                            if cm_seq > cur_seq:
                                cand_src = (
                                    self.latest_snapshot_path
                                    if self.latest_snapshot_path.exists() and self._sha256_file(self.latest_snapshot_path).lower() == cm_hash
                                    else None
                                )
                                bk_name = Path(str(cm.get("backupFile", "") or "")).name
                                if cand_src is None and bk_name and (self.backups_dir / bk_name).exists():
                                    if self._sha256_file(self.backups_dir / bk_name).lower() == cm_hash:
                                        cand_src = self.backups_dir / bk_name
                                promoted_zip = False
                                if cand_src is None and cm_hash and self.snapshots_dir.exists():
                                    snap_conflict_re = re.compile(r"^latest-server\s*\(\d+\)\.zip$", re.IGNORECASE)
                                    for zcand in self.snapshots_dir.iterdir():
                                        if zcand.is_file() and snap_conflict_re.match(zcand.name):
                                            try:
                                                if self._sha256_file(zcand).lower() == cm_hash:
                                                    cand_src = zcand
                                                    promoted_zip = True
                                                    break
                                            except OSError:
                                                continue
                                if cand_src is not None:
                                    if promoted_zip:
                                        self._copy_file_preserving_cloud_id(cand_src, self.latest_snapshot_path)
                                        if str(cm.get("mode") or "").lower() == "delta":
                                            cm["mode"] = "full"
                                            cm["baseSeq"] = cm_seq
                                            cm["baseSha256"] = cm_hash
                                            cm["baseSizeBytes"] = int(cm.get("sizeBytes", 0) or 0)
                                            cm["deltaFile"] = ""
                                            cm["deltas"] = []
                                            cm["fileIndexSha256"] = ""
                                    self._write_json_atomic(self.sync_manifest_path, cm, in_place_if_exists=True)
                                    manifest = cm
                                elif transfer_active or (now - item.stat().st_mtime) < 120:
                                    should_unlink = False
                    except Exception:
                        pass
                elif stem_base == "file-index":
                    try:
                        exp_idx_hash = str(manifest.get("fileIndexSha256", "") or "").lower() if manifest else ""
                        if exp_idx_hash and item.stat().st_size > 0:
                            main_ok = (
                                self.file_index_path.exists()
                                and self._sha256_file(self.file_index_path).lower() == exp_idx_hash
                            )
                            if not main_ok and self._sha256_file(item).lower() == exp_idx_hash:
                                self._copy_file_preserving_cloud_id(item, self.file_index_path)
                        elif transfer_active or (now - item.stat().st_mtime) < 120:
                            should_unlink = False
                    except Exception:
                        pass
                elif stem_base == "latest-server":
                    try:
                        if manifest:
                            exp_size = int(manifest.get("sizeBytes", -1))
                            exp_hash = str(manifest.get("sha256", "")).lower()
                            if item.stat().st_size == exp_size and exp_hash:
                                main_ok = (
                                    self.latest_snapshot_path.exists()
                                    and self.latest_snapshot_path.stat().st_size == exp_size
                                    and self._sha256_file(self.latest_snapshot_path).lower() == exp_hash
                                )
                                if not main_ok and self._sha256_file(item).lower() == exp_hash:
                                    self._copy_file_preserving_cloud_id(item, self.latest_snapshot_path)
                            elif transfer_active or (now - item.stat().st_mtime) < 120:
                                should_unlink = False
                        elif transfer_active or (now - item.stat().st_mtime) < 120:
                            should_unlink = False
                    except Exception:
                        pass
                if should_unlink:
                    try:
                        item.unlink(missing_ok=True)
                    except OSError:
                        pass

        # Reconcile and prune conflict copies + expired tombstones in locks/
        if self.locks_dir.exists():
            try:
                tel_conflicts: List[Tuple[float, Path]] = []
                for item in list(self.locks_dir.iterdir()):
                    if not item.is_file():
                        continue
                    if ".tmp." in item.name:
                        try:
                            if now - item.stat().st_mtime > 30:
                                item.unlink(missing_ok=True)
                        except OSError:
                            pass
                        continue
                    if not item.name.endswith(".json"):
                        continue
                    try:
                        age = now - item.stat().st_mtime
                        norm_stem = self._strip_conflict_suffix(item.stem)
                        is_conflict_copy = norm_stem != item.stem
                        if norm_stem.startswith("session-"):
                            epoch = norm_stem[len("session-"):]
                            if epoch in released_epochs and (is_conflict_copy or age > 60):
                                item.unlink(missing_ok=True)
                            elif is_conflict_copy:
                                sdata = self._read_json_safe(item)
                                base_path = self.locks_dir / f"{norm_stem}.json"
                                if isinstance(sdata, dict) and sdata.get("isLocked"):
                                    cur_sdata = self._read_json_safe(base_path)
                                    cur_hb = float(cur_sdata.get("lastHeartbeat", 0.0) or 0.0) if isinstance(cur_sdata, dict) else 0.0
                                    if not base_path.exists() or float(sdata.get("lastHeartbeat", 0.0) or 0.0) > cur_hb:
                                        self._write_json_atomic(base_path, sdata, in_place_if_exists=True)
                                item.unlink(missing_ok=True)
                        elif norm_stem.startswith("released-"):
                            epoch = norm_stem[len("released-"):]
                            if is_conflict_copy:
                                base_path = self.locks_dir / f"{norm_stem}.json"
                                if not base_path.exists():
                                    rdata = self._read_json_safe(item)
                                    if isinstance(rdata, dict):
                                        self._write_json_atomic(base_path, rdata, in_place_if_exists=True)
                                item.unlink(missing_ok=True)
                            elif age > 86400:
                                item.unlink(missing_ok=True)
                        elif norm_stem == "host-telemetry" and is_conflict_copy:
                            tdata = self._read_json_safe(item)
                            t_upd = float(tdata.get("updatedAt", 0.0) or 0.0) if isinstance(tdata, dict) else 0.0
                            tel_conflicts.append((t_upd, item))
                    except OSError:
                        pass
                if tel_conflicts:
                    cur_tel = self._read_json_safe(self.host_telemetry_path)
                    cur_upd = float(cur_tel.get("updatedAt", 0.0) or 0.0) if isinstance(cur_tel, dict) else 0.0
                    for t_upd, cpath in sorted(tel_conflicts, key=lambda x: x[0], reverse=True):
                        cdata = self._read_json_safe(cpath)
                        c_ep = str(cdata.get("lockEpoch") or "") if isinstance(cdata, dict) else ""
                        if isinstance(cdata, dict) and t_upd > cur_upd and (not c_ep or c_ep not in released_epochs):
                            self._write_json_atomic(self.host_telemetry_path, cdata, in_place_if_exists=True)
                            cur_upd = t_upd
                        cpath.unlink(missing_ok=True)
            except OSError:
                pass

        # Reconcile conflict copies inside snapshots/deltas/ (e.g. delta-6-abcd (1).zip)
        if self.deltas_dir.exists():
            try:
                delta_conflict_re = re.compile(r"^(delta-[^()]+?)\s*\(\d+\)\.zip$", re.IGNORECASE)
                for item in list(self.deltas_dir.iterdir()):
                    if not item.is_file():
                        continue
                    if ".tmp." in item.name:
                        try:
                            if now - item.stat().st_mtime > 30:
                                item.unlink(missing_ok=True)
                        except OSError:
                            pass
                        continue
                    dm = delta_conflict_re.match(item.name)
                    if dm:
                        target = self.deltas_dir / f"{dm.group(1)}.zip"
                        try:
                            if not target.exists() and item.stat().st_size > 0:
                                shutil.copy2(item, target)
                            if target.exists() or not transfer_active:
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
        fsync: bool = True,
    ) -> None:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        content_bytes = json.dumps(data, indent=2).encode("utf-8")

        # Updating existing shared JSON in-place keeps Google Drive Cloud File IDs intact.
        # Pad trailing whitespace before truncating in binary mode so concurrent readers never observe
        # leftover tail bytes from a longer previous JSON payload.
        if in_place_if_exists and target_path.exists():
            for attempt in range(8):
                try:
                    with open(target_path, "r+b") as f:
                        f.seek(0, os.SEEK_END)
                        old_size = f.tell()
                        f.seek(0)
                        if old_size > len(content_bytes):
                            f.write(content_bytes + (b" " * (old_size - len(content_bytes))))
                            f.flush()
                            f.seek(len(content_bytes))
                            f.truncate()
                        else:
                            f.write(content_bytes)
                            f.truncate()
                        f.flush()
                        if fsync:
                            os.fsync(f.fileno())
                    return
                except FileNotFoundError:
                    break
                except OSError:
                    time.sleep(0.08 * (attempt + 1))

        tmp_path = target_path.with_suffix(f".tmp.{os.getpid()}.{threading.get_ident()}")
        try:
            with open(tmp_path, "wb") as f:
                f.write(content_bytes)
                f.flush()
                if fsync:
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

        # 2. Scan mirrored local Google Drive / shared Minecraft folders (depth 1 & 2)
        mirrored_dirs = []
        skip_scan = {"appdata", "node_modules", "__pycache__", "windows", "program files", "program files (x86)"}

        def _is_cloud_or_cluster_dir(d: Path) -> bool:
            if (d / ".tmp.drivedownload").exists() or (d / ".tmp.driveupload").exists():
                return True
            return (d / "cluster-config.json").exists() and (d / "snapshots").exists()

        for parent_dir in (home / "Documents", home / "Desktop", home):
            try:
                if parent_dir.exists() and parent_dir.is_dir():
                    for sub in parent_dir.iterdir():
                        if not sub.is_dir() or sub.name.startswith(".") or sub.name.lower() in skip_scan:
                            continue
                        try:
                            if _is_cloud_or_cluster_dir(sub):
                                mirrored_dirs.append(sub)
                                continue
                            for sub2 in sub.iterdir():
                                if not sub2.is_dir() or sub2.name.startswith(".") or sub2.name.lower() in skip_scan:
                                    continue
                                if _is_cloud_or_cluster_dir(sub2):
                                    mirrored_dirs.append(sub2)
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
                    # Check Google Drive Shared drives (1 level inside)
                    for sd_name in ("Shared drives", "Shared Drives", "Κοινόχρηστοι δίσκοι", "Ablagen"):
                        sd_root = drive_root / sd_name
                        if sd_root.exists() and sd_root.is_dir():
                            for sd_sub in sd_root.iterdir():
                                if not sd_sub.is_dir() or sd_sub.name.startswith("."):
                                    continue
                                if (sd_sub / "cluster-config.json").exists() and (sd_sub / "snapshots").exists():
                                    _add_candidate(f"Google Shared Drive ({sd_sub.name})", sd_sub)
                                elif (sd_sub / "MineRelay-Shared" / "cluster-config.json").exists():
                                    _add_candidate(f"Google Shared Drive ({sd_sub.name})", sd_sub / "MineRelay-Shared")
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

        cloud_backup_count = 0
        try:
            with os.scandir(self.backups_dir) as it:
                for entry in it:
                    if entry.name.endswith(".zip") and entry.is_file(follow_symlinks=False):
                        cloud_backup_count += 1
        except OSError:
            cloud_backup_count = 0

        local_backup_count = 0
        try:
            with os.scandir(self.local_backups_dir) as it:
                for entry in it:
                    if entry.name.endswith(".zip") and entry.is_file(follow_symlinks=False):
                        local_backup_count += 1
        except OSError:
            local_backup_count = 0

        backup_count = cloud_backup_count + local_backup_count

        deltas_list = manifest.get("deltas") if isinstance(manifest, dict) and isinstance(manifest.get("deltas"), list) else []
        sync_mode = str(manifest.get("mode") or "full") if isinstance(manifest, dict) else "none"
        last_stats = (
            dict(self._last_sync_stats)
            if self._last_sync_stats
            else (dict(manifest["lastSyncStats"]) if isinstance(manifest, dict) and isinstance(manifest.get("lastSyncStats"), dict) else None)
        )
        transfer_details = self.get_cloud_transfer_details()

        return {
            "sharedSyncDir": str(shared),
            "providerDetected": provider,
            "clusterConfigExists": self.cluster_config_path.exists(),
            "hostLockExists": self.host_lock_path.exists(),
            "latestSnapshotExists": snap_exists,
            "latestSnapshotSizeBytes": snap_size,
            "latestSnapshotModifiedAt": snap_mtime,
            "backupCount": backup_count,
            "cloudBackupCount": cloud_backup_count,
            "localBackupCount": local_backup_count,
            "hasSharedTunnelSecret": (self.shared_tunnel_dir / "playit.toml").exists(),
            "isShortcutStreamPath": is_shortcut_stream,
            "mirroredAlternatives": mirrored_alternatives,
            "cloudTransferActive": str(transfer_details["message"]) if transfer_details.get("active") else None,
            "cloudTransferDetails": transfer_details,
            "syncProgress": self.get_sync_progress(),
            "syncManifest": manifest,
            "syncMode": sync_mode,
            "deltaCount": len(deltas_list),
            "maxDeltaChain": self.MAX_DELTA_CHAIN,
            "lastSyncStats": last_stats,
        }

    def preflight_cloud_ready(self) -> Dict[str, Any]:
        """Non-locking cloud-sync readiness check before claiming the host lock on start."""
        self.git_pull_if_enabled()
        if self._has_snapshot_conflicts():
            self._cleanup_cloud_conflict_files()
        transfer_msg = self._has_active_cloud_transfer()
        if transfer_msg:
            raise RuntimeError(f"{transfer_msg} Wait for cloud sync to finish before continuing.")
        manifest = self._read_sync_manifest()
        if not manifest and self.latest_snapshot_path.exists():
            try:
                if self.latest_snapshot_path.stat().st_size == 0:
                    raise RuntimeError(
                        "Cloud sync is still catching up ('latest-server.zip' is 0 bytes). Wait for Google Drive / cloud sync to finish!"
                    )
            except OSError:
                pass
        source_zip, mismatch_reason = self._resolve_verified_snapshot_source(manifest)
        if mismatch_reason:
            raise RuntimeError(mismatch_reason)
        return {"ready": True, "hasSnapshot": bool(source_zip is not None)}

    def is_cluster_initialized(self) -> bool:
        cfg = self.load_cluster_config()
        return cfg is not None and len(cfg.accounts) > 0

    def load_cluster_config(self, _reconcile_conflicts: bool = True) -> Optional[ClusterConfig]:
        conflict_pattern = re.compile(r"^cluster-config\s*\(\d+\)\.json$", re.IGNORECASE)
        conflict_files: List[Path] = []
        if _reconcile_conflicts:
            try:
                with os.scandir(self.shared_dir) as it:
                    for entry in it:
                        if entry.name.lower().startswith("cluster-config") and conflict_pattern.match(entry.name):
                            if entry.is_file(follow_symlinks=False):
                                conflict_files.append(Path(entry.path))
            except OSError:
                pass

        data = self._read_json_safe(self.cluster_config_path)
        if not data:
            # Fallback to newest valid Google Drive conflict copy if main cluster-config.json is missing/corrupt
            candidates: List[Tuple[float, Dict[str, Any]]] = []
            for item in conflict_files:
                cdata = self._read_json_safe(item)
                if isinstance(cdata, dict) and cdata.get("accounts"):
                    try:
                        cmtime = item.stat().st_mtime
                    except OSError:
                        cmtime = 0.0
                    candidates.append((cmtime, cdata))
            if candidates:
                candidates.sort(key=lambda x: x[0], reverse=True)
                data = candidates[0][1]
                try:
                    self._write_json_atomic(self.cluster_config_path, data, in_place_if_exists=True)
                except Exception:
                    pass
            else:
                return None

        if _reconcile_conflicts and conflict_files and not self._has_active_cloud_transfer():
            with self._cluster_merge_lock:
                for cpath in conflict_files:
                    if cpath.exists():
                        self._merge_cluster_config_conflict(cpath)
                        try:
                            if self.cluster_config_path.exists():
                                cpath.unlink(missing_ok=True)
                        except OSError:
                            pass
            refreshed = self._read_json_safe(self.cluster_config_path)
            if isinstance(refreshed, dict) and refreshed:
                data = refreshed

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
        self._ensure_shared_dirs(cleanup_conflicts=False)
        self._write_json_atomic(
            self.cluster_config_path,
            config.to_dict(include_secrets=True),
            in_place_if_exists=True,
        )

    def _write_lock_tombstone(
        self,
        lock_epoch: str,
        released_by: str = "",
        expected_snapshot_seq: int = 0,
        expected_snapshot_hash: str = "",
    ) -> None:
        if not lock_epoch:
            return
        self._ensure_shared_dirs(cleanup_conflicts=False)
        self._known_released_epochs.add(lock_epoch)
        tombstone = self.locks_dir / f"released-{lock_epoch}.json"
        self._write_json_atomic(
            tombstone,
            {
                "lockEpoch": lock_epoch,
                "isLocked": False,
                "releasedAt": time.time(),
                "releasedBy": released_by,
                "machineId": self.local_config.machine_id,
                "expectedSnapshotSeq": int(expected_snapshot_seq or 0),
                "expectedSnapshotHash": str(expected_snapshot_hash or ""),
            },
            in_place_if_exists=True,
        )
        for ldir in self._iter_locks_dirs():
            try:
                for item in list(ldir.iterdir()):
                    if not item.is_file() or not item.name.endswith(".json"):
                        continue
                    norm_stem = self._strip_conflict_suffix(item.stem)
                    if norm_stem == f"session-{lock_epoch}":
                        item.unlink(missing_ok=True)
            except OSError:
                pass

    def write_host_telemetry(self, payload: Dict[str, Any], fsync: bool = False) -> None:
        """Publishes sanitized live telemetry and console tail for remote cluster observers."""
        try:
            self.locks_dir.mkdir(parents=True, exist_ok=True)
            data = dict(payload)
            data["updatedAt"] = time.time()
            self._write_json_atomic(self.host_telemetry_path, data, in_place_if_exists=True, fsync=fsync)
        except Exception:
            pass

    def read_host_telemetry(self, expected_epoch: str = "") -> Optional[Dict[str, Any]]:
        """Reads the freshest host telemetry payload across `locks/` and Drive conflict folders/files."""
        tel_pattern = re.compile(r"^host-telemetry(?:\s*\(\d+\))?\.json$", re.IGNORECASE)
        candidates: List[Tuple[int, float, int, int, float, Dict[str, Any]]] = []
        for ldir in self._iter_locks_dirs():
            try:
                for item in ldir.iterdir():
                    if not item.is_file() or not tel_pattern.match(item.name):
                        continue
                    data = self._read_json_safe(item)
                    if not isinstance(data, dict):
                        continue
                    d_epoch = str(data.get("lockEpoch") or "")
                    if expected_epoch and d_epoch and d_epoch != expected_epoch:
                        continue
                    if not d_epoch and not data.get("isActive"):
                        continue
                    try:
                        mtime = item.stat().st_mtime
                    except OSError:
                        mtime = 0.0
                    exact_epoch = 1 if (expected_epoch and d_epoch == expected_epoch) else 0
                    is_active = 1 if data.get("isActive") else 0
                    has_logs = 1 if (isinstance(data.get("logs"), list) and len(data["logs"]) > 0) else 0
                    upd = float(data.get("updatedAt") or 0.0)
                    candidates.append((exact_epoch, upd, is_active, has_logs, mtime, data))
            except OSError:
                pass
        if not candidates:
            return None
        candidates.sort(key=lambda c: (c[0], c[1], c[2], c[3], c[4]), reverse=True)
        return candidates[0][5]

    def clear_host_telemetry(self) -> None:
        self.write_host_telemetry({
            "isActive": False,
            "lockEpoch": "",
            "machineId": "",
            "hostUsername": "",
            "logs": [],
        })
        # Prune stale conflict telemetry files belonging to this machine or already-released epochs
        tel_conflict_pat = re.compile(r"^host-telemetry\s*\(\d+\)\.json$", re.IGNORECASE)
        released_epochs = self._load_released_epochs()
        for ldir in self._iter_locks_dirs():
            try:
                for item in list(ldir.iterdir()):
                    if not item.is_file() or not tel_conflict_pat.match(item.name):
                        continue
                    data = self._read_json_safe(item)
                    if not isinstance(data, dict):
                        continue
                    ep = str(data.get("lockEpoch") or "")
                    mid = str(data.get("machineId") or "")
                    if mid == self.local_config.machine_id or (ep and ep in released_epochs) or not data.get("isActive"):
                        item.unlink(missing_ok=True)
            except OSError:
                pass

    def get_expected_snapshot_state(self) -> Tuple[int, str, str]:
        """Returns `(expected_seq, expected_hash, released_by)` from the newest release record."""
        best_seq = 0
        best_hash = ""
        best_by = ""
        try:
            lock_files = [self.host_lock_path]
            conflict_pattern = re.compile(r"^host-lock\s*\(\d+\)\.json$", re.IGNORECASE)
            if self.shared_dir.exists():
                for item in self.shared_dir.iterdir():
                    if item.is_file() and conflict_pattern.match(item.name):
                        lock_files.append(item)
            for lpath in lock_files:
                main_data = self._read_json_safe(lpath)
                if isinstance(main_data, dict):
                    lk = HostLock.from_dict(main_data)
                    if lk.expected_snapshot_seq > best_seq:
                        best_seq = lk.expected_snapshot_seq
                        best_hash = lk.expected_snapshot_hash
                        best_by = lk.host_username or ""
            for ldir in self._iter_locks_dirs():
                for item in ldir.iterdir():
                    if not item.is_file() or not item.name.startswith("released-") or not item.name.endswith(".json"):
                        continue
                    tdata = self._read_json_safe(item)
                    if not isinstance(tdata, dict):
                        continue
                    t_seq = int(tdata.get("expectedSnapshotSeq", 0) or 0)
                    t_hash = str(tdata.get("expectedSnapshotHash", "") or "")
                    t_by = str(tdata.get("releasedBy", "") or "")
                    if t_seq > best_seq:
                        best_seq = t_seq
                        best_hash = t_hash
                        best_by = t_by
                    elif t_seq == best_seq and t_seq > 0:
                        if not best_hash and t_hash:
                            best_hash = t_hash
                        if not best_by and t_by:
                            best_by = t_by
        except OSError:
            pass
        return best_seq, best_hash, best_by

    def _write_lock_bundle(self, lock: HostLock, include_session: bool = True, fsync: bool = True) -> None:
        self._ensure_shared_dirs(cleanup_conflicts=False)
        payload = lock.to_dict()
        self._write_json_atomic(self.host_lock_path, payload, in_place_if_exists=True, fsync=fsync)
        if include_session and lock.is_locked and lock.lock_epoch:
            session_file = self.locks_dir / f"session-{lock.lock_epoch}.json"
            self._write_json_atomic(session_file, payload, in_place_if_exists=True, fsync=fsync)
            self._last_session_write_at = time.time()
        try:
            st_mtime = os.stat(self.host_lock_path).st_mtime
        except OSError:
            st_mtime = 0.0
        with self._lock_mutex:
            if self._owns_active_lock and lock.is_locked and lock.machine_id == self.local_config.machine_id:
                self._cached_own_lock = HostLock.from_dict(payload)
                self._cached_own_lock_mtime = st_mtime
            elif not lock.is_locked:
                self._cached_own_lock = None
                self._cached_own_lock_mtime = 0.0

    def _apply_clock_skew_tracking(self, chosen: HostLock, now: float) -> None:
        if not chosen.machine_id or chosen.machine_id == self.local_config.machine_id:
            return
        track_key = f"{chosen.machine_id}:{chosen.lock_epoch or int(chosen.started_at)}"
        prev = self._remote_hb_tracker.get(track_key)
        if prev is None:
            self._remote_hb_tracker[track_key] = (chosen.last_heartbeat, now, 0.0)
        else:
            prev_hb, first_seen, obs_at = prev
            if chosen.last_heartbeat > prev_hb + 0.5:
                obs_at = now
                self._remote_hb_tracker[track_key] = (chosen.last_heartbeat, first_seen, obs_at)
            # Behind-clock skew protection: active progression keeps lock fresh even if remote PC clock is slow
            if (
                obs_at > 0.0
                and (now - obs_at) <= LOCK_STALE_TIMEOUT_SEC
                and (now - chosen.last_heartbeat) > LOCK_STALE_TIMEOUT_SEC
            ):
                chosen.last_heartbeat = max(chosen.last_heartbeat, obs_at)
            # Ahead-clock skew protection: if remote clock is ahead and heartbeat hasn't moved for > timeout, expire it
            elif chosen.last_heartbeat > now + 30.0:
                ref_obs = obs_at if obs_at > 0.0 else first_seen
                if (now - ref_obs) > LOCK_STALE_TIMEOUT_SEC:
                    chosen.last_heartbeat = min(chosen.last_heartbeat, ref_obs)
        # Prune stale tracker entries
        if len(self._remote_hb_tracker) > 64:
            cutoff = now - 86400.0
            for k in [k for k, v in self._remote_hb_tracker.items() if max(v[1], v[2]) < cutoff]:
                self._remote_hb_tracker.pop(k, None)

    def get_lock(self) -> HostLock:
        now = time.time()
        owns_active = self._owns_active_lock
        active_epoch = self._active_lock_epoch
        released_epochs = set(self._known_released_epochs)

        locks_conflict_re = re.compile(r"^locks\s*\(\d+\)$", re.IGNORECASE)
        host_conflict_re = re.compile(r"^host-lock\s*\(\d+\)\.json$", re.IGNORECASE)
        locks_dirs: List[Path] = []
        host_conflict_files: List[Tuple[Path, float]] = []
        try:
            with os.scandir(self.shared_dir) as it:
                for entry in it:
                    ename = entry.name
                    if ename == "locks" or locks_conflict_re.match(ename):
                        if entry.is_dir(follow_symlinks=False):
                            locks_dirs.append(Path(entry.path))
                    elif ename.lower().startswith("host-lock") and host_conflict_re.match(ename):
                        if entry.is_file(follow_symlinks=False):
                            try:
                                cmtime = entry.stat(follow_symlinks=False).st_mtime
                            except OSError:
                                cmtime = 0.0
                            host_conflict_files.append((Path(entry.path), cmtime))
        except OSError:
            pass

        session_files: List[Tuple[Path, str, float]] = []
        found_released: set = set()
        for ldir in locks_dirs:
            try:
                with os.scandir(ldir) as it:
                    for entry in it:
                        ename = entry.name
                        if not ename.endswith(".json") or not entry.is_file(follow_symlinks=False):
                            continue
                        if ename.startswith("released-"):
                            raw_epoch = ename[len("released-"):-len(".json")]
                            epoch = self._strip_conflict_suffix(raw_epoch)
                            if epoch:
                                found_released.add(epoch)
                            if raw_epoch != epoch:
                                tdata = self._read_json_safe(Path(entry.path))
                                if isinstance(tdata, dict) and tdata.get("lockEpoch"):
                                    ep = str(tdata["lockEpoch"]).strip()
                                    if ep:
                                        found_released.add(ep)
                        elif ename.startswith("session-"):
                            try:
                                smtime = entry.stat(follow_symlinks=False).st_mtime
                            except OSError:
                                smtime = 0.0
                            session_files.append((Path(entry.path), ename, smtime))
            except OSError:
                pass

        if found_released:
            released_epochs.update(found_released)
            if not found_released.issubset(self._known_released_epochs):
                with self._lock_mutex:
                    self._known_released_epochs.update(found_released)

        main_data = self._read_json_safe(self.host_lock_path)
        try:
            main_mtime = os.stat(self.host_lock_path).st_mtime
            main_exists = True
        except OSError:
            main_mtime = 0.0
            main_exists = False
        any_read_error = main_exists and main_data is None
        main_lock = HostLock.from_dict(main_data) if isinstance(main_data, dict) else HostLock(is_locked=False, status="idle")

        # Collect locked candidates across host-lock.json, Drive conflict copies, and per-session locks/
        candidates: List[Tuple[int, int, float, float, HostLock]] = []

        def _add_candidate(lk: HostLock, fmtime: float) -> None:
            is_active = 1 if self._is_lock_effectively_active(lk, now) else 0
            is_own_epoch = 1 if (
                owns_active
                and active_epoch
                and lk.lock_epoch == active_epoch
                and lk.machine_id == self.local_config.machine_id
            ) else 0
            candidates.append((is_active, is_own_epoch, lk.last_heartbeat, fmtime, lk))

        if main_lock.is_locked and (not main_lock.lock_epoch or main_lock.lock_epoch not in released_epochs):
            _add_candidate(main_lock, main_mtime)

        for item, cmtime in host_conflict_files:
            cdata = self._read_json_safe(item)
            if cdata is None:
                any_read_error = True
                continue
            clock = HostLock.from_dict(cdata)
            if not clock.is_locked:
                continue
            if clock.lock_epoch and clock.lock_epoch in released_epochs:
                continue
            # If main_lock explicitly unlocked a legacy session cleanly, ignore older conflict files
            if not main_lock.is_locked and main_data is not None and not clock.lock_epoch:
                if max(clock.last_heartbeat, cmtime) <= main_mtime + 2.0:
                    continue
            _add_candidate(clock, cmtime)

        for item, ename, smtime in session_files:
            raw_epoch = ename[len("session-"):-len(".json")]
            epoch = self._strip_conflict_suffix(raw_epoch)
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
            _add_candidate(slock, smtime)

        if candidates:
            candidates.sort(key=lambda c: (c[0], c[1], c[2], c[4].started_at, c[3]), reverse=True)
            chosen = candidates[0][4]
            if chosen.machine_id and chosen.machine_id != self.local_config.machine_id:
                with self._lock_mutex:
                    self._apply_clock_skew_tracking(chosen, now)
                    if chosen.is_actively_locked(now):
                        self._last_known_remote_lock = HostLock.from_dict(chosen.to_dict())
                        self._last_known_remote_lock_at = now
            return chosen

        if any_read_error or self._last_known_remote_lock is not None:
            with self._lock_mutex:
                # Do not flicker back to 'idle' during transient Drive read locks if we just saw an active remote host
                if any_read_error and self._last_known_remote_lock and (now - self._last_known_remote_lock_at) < 25.0:
                    return HostLock.from_dict(self._last_known_remote_lock.to_dict())

                if not any_read_error:
                    self._last_known_remote_lock = None
                    self._last_known_remote_lock_at = 0.0
        return HostLock(is_locked=False, status="idle")

    def save_lock(self, lock: HostLock, fsync: bool = True, include_session: bool = True) -> None:
        self._write_lock_bundle(lock, include_session=include_session, fsync=fsync)
        self._emit("lock_updated", lock.to_dict())

    def owns_lock(self, lock_epoch: str = "") -> bool:
        """Thread-safe check of whether this machine currently holds the active lock (optionally matching `lock_epoch`)."""
        with self._lock_mutex:
            if not self._owns_active_lock:
                return False
            if lock_epoch and self._active_lock_epoch and lock_epoch != self._active_lock_epoch:
                return False
            return True

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
            carry_seq, carry_hash, _ = self.get_expected_snapshot_state()
            if current.expected_snapshot_seq > carry_seq:
                carry_seq = current.expected_snapshot_seq
                carry_hash = current.expected_snapshot_hash or carry_hash
            manifest = self._read_sync_manifest()
            if manifest:
                m_seq = int(manifest.get("snapshotSeq", 0) or 0)
                m_hash = str(manifest.get("sha256", "") or "")
                if m_seq > carry_seq:
                    carry_seq, carry_hash = m_seq, m_hash

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
                expected_snapshot_seq=carry_seq,
                expected_snapshot_hash=carry_hash,
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
                    try:
                        self._write_json_atomic(self.host_lock_path, check.to_dict(), in_place_if_exists=True)
                    except Exception:
                        pass
                raise HostLockConflictError(
                    f"Lock claim race won by '{check.host_username}' on '{check.machine_name}'."
                )

            self._last_known_remote_lock = None
            self._last_known_remote_lock_at = 0.0
            self._owns_active_lock = True
            self._active_lock_epoch = lock_epoch
            self._cached_own_lock = HostLock.from_dict(new_lock.to_dict())
            try:
                self._cached_own_lock_mtime = os.stat(self.host_lock_path).st_mtime
            except OSError:
                self._cached_own_lock_mtime = 0.0
            self._last_session_write_at = now
            self._last_telemetry_publish_at = now
            self.write_host_telemetry({
                "isActive": True,
                "lockEpoch": lock_epoch,
                "machineId": self.local_config.machine_id,
                "machineName": self.local_config.machine_name,
                "hostUsername": user.username,
                "status": initial_status,
                "publicAddress": public_address,
                "tunnelStatus": new_lock.tunnel_status,
                "mcVersion": mc_version,
                "startedAt": new_lock.started_at,
                "onlinePlayers": [],
                "maxPlayers": new_lock.max_players,
                "logs": [],
            })
            self._start_heartbeat_loop()
            return new_lock

    def _publish_telemetry_from_lock(self, lock: HostLock, snap: Dict[str, Any]) -> None:
        if "logs" not in snap and not lock.is_locked:
            return
        tel_payload = dict(snap)
        if "logs" in tel_payload and isinstance(tel_payload["logs"], list):
            tel_payload["logs"] = list(tel_payload["logs"])[-120:]
        tel_payload["isActive"] = bool(lock.is_locked)
        tel_payload["lockEpoch"] = lock.lock_epoch
        tel_payload["machineId"] = lock.machine_id
        tel_payload["machineName"] = lock.machine_name
        tel_payload["hostUsername"] = lock.host_username
        tel_payload["status"] = lock.status
        tel_payload["startedAt"] = lock.started_at
        tel_payload["tps"] = lock.tps
        tel_payload["memoryUsedMb"] = lock.memory_used_mb
        tel_payload["memoryMaxMb"] = lock.memory_max_mb
        tel_payload["cpuPercent"] = lock.cpu_percent
        tel_payload["serverReadyAt"] = lock.server_ready_at
        tel_payload["onlinePlayers"] = list(lock.online_players)
        tel_payload["maxPlayers"] = lock.max_players
        if not tel_payload.get("publicAddress"):
            tel_payload["publicAddress"] = lock.public_address
        if not tel_payload.get("tunnelStatus"):
            tel_payload["tunnelStatus"] = lock.tunnel_status
        if not tel_payload.get("mcVersion"):
            tel_payload["mcVersion"] = lock.mc_version
        self.write_host_telemetry(tel_payload)
        self._last_telemetry_publish_at = time.time()

    def _apply_telemetry_provider(self, lock: HostLock, publish: bool = True) -> Optional[Dict[str, Any]]:
        if not self.telemetry_provider:
            return None
        try:
            snap = self.telemetry_provider() or {}
            if "memoryUsedMb" in snap:
                lock.memory_used_mb = int(snap["memoryUsedMb"] or 0)
            if "memoryMaxMb" in snap:
                lock.memory_max_mb = max(512, int(snap["memoryMaxMb"] or 4096))
            if "cpuPercent" in snap:
                lock.cpu_percent = round(float(snap["cpuPercent"] or 0.0), 1)
            if "tps" in snap:
                lock.tps = round(float(snap["tps"] or 0.0), 2)
            if "serverReadyAt" in snap:
                lock.server_ready_at = float(snap["serverReadyAt"] or 0.0)
            if "onlinePlayers" in snap and isinstance(snap["onlinePlayers"], list):
                lock.online_players = [str(p) for p in snap["onlinePlayers"] if p]
            if publish:
                self._publish_telemetry_from_lock(lock, snap)
            return snap
        except Exception:
            return None

    def _has_released_tombstone(self, lock_epoch: str) -> bool:
        if not lock_epoch:
            return False
        if lock_epoch in self._known_released_epochs:
            return True
        if os.path.exists(self.locks_dir / f"released-{lock_epoch}.json"):
            self._known_released_epochs.add(lock_epoch)
            return True
        locks_conflict_re = re.compile(r"^locks\s*\(\d+\)$", re.IGNORECASE)
        prefix = f"released-{lock_epoch}"
        try:
            with os.scandir(self.shared_dir) as it:
                for entry in it:
                    if not (entry.name == "locks" or locks_conflict_re.match(entry.name)):
                        continue
                    if not entry.is_dir(follow_symlinks=False):
                        continue
                    try:
                        with os.scandir(entry.path) as lit:
                            for lentry in lit:
                                lname = lentry.name
                                if lname.startswith(prefix) and lname.endswith(".json") and lentry.is_file(follow_symlinks=False):
                                    norm_epoch = self._strip_conflict_suffix(lname[len("released-"):-len(".json")])
                                    if norm_epoch == lock_epoch:
                                        self._known_released_epochs.add(lock_epoch)
                                        return True
                    except OSError:
                        pass
        except OSError:
            pass
        return False

    def update_lock_state(
        self,
        status: Optional[str] = None,
        public_address: Optional[str] = None,
        tunnel_status: Optional[str] = None,
        online_players: Optional[List[str]] = None,
        max_players: Optional[int] = None,
        mc_version: Optional[str] = None,
        memory_used_mb: Optional[int] = None,
        memory_max_mb: Optional[int] = None,
        cpu_percent: Optional[float] = None,
        tps: Optional[float] = None,
        server_ready_at: Optional[float] = None,
    ) -> Optional[HostLock]:
        with self._lock_mutex:
            current: Optional[HostLock] = None
            if (
                status is None
                and self._owns_active_lock
                and self._active_lock_epoch
                and self._cached_own_lock is not None
                and not self._has_released_tombstone(self._active_lock_epoch)
            ):
                try:
                    if os.stat(self.host_lock_path).st_mtime == self._cached_own_lock_mtime:
                        current = HostLock.from_dict(self._cached_own_lock.to_dict())
                except OSError:
                    current = None
            if current is None:
                current = self.get_lock()
            if (
                not current.is_locked
                or current.machine_id != self.local_config.machine_id
                or (self._active_lock_epoch and current.lock_epoch and current.lock_epoch != self._active_lock_epoch)
            ):
                was_owning = self._owns_active_lock
                expected_ep = self._active_lock_epoch
                self._owns_active_lock = False
                self._active_lock_epoch = ""
                self._cached_own_lock = None
                self._cached_own_lock_mtime = 0.0
                self._stop_heartbeat_loop()
                if was_owning:
                    self._emit("lock_lost", {
                        "expectedEpoch": expected_ep,
                        "currentLock": current.to_dict(),
                    })
                return None
            now = time.time()
            current.last_heartbeat = now
            snap = self._apply_telemetry_provider(current, publish=False)
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
            if memory_used_mb is not None:
                current.memory_used_mb = memory_used_mb
            if memory_max_mb is not None:
                current.memory_max_mb = max(512, int(memory_max_mb))
            if cpu_percent is not None:
                current.cpu_percent = round(float(cpu_percent), 1)
            if tps is not None:
                current.tps = round(float(tps), 2)
            if server_ready_at is not None:
                current.server_ready_at = float(server_ready_at)
            is_status_change = status is not None
            include_session = is_status_change or ((now - self._last_session_write_at) >= 2.0)
            self.save_lock(current, fsync=is_status_change, include_session=include_session)
            if snap is not None and (is_status_change or (now - self._last_telemetry_publish_at) >= 0.5):
                self._publish_telemetry_from_lock(current, snap)
            return current

    def release_lock(
        self,
        user: Optional[UserAccount] = None,
        expected_snapshot_seq: int = 0,
        expected_snapshot_hash: str = "",
    ) -> HostLock:
        with self._lock_mutex:
            self._owns_active_lock = False
            self._active_lock_epoch = ""
            self._cached_own_lock = None
            self._cached_own_lock_mtime = 0.0
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
            carry_seq = int(expected_snapshot_seq or 0)
            carry_hash = str(expected_snapshot_hash or "")
            if carry_seq == 0:
                prev_seq, prev_hash, _ = self.get_expected_snapshot_state()
                carry_seq, carry_hash = prev_seq, prev_hash
            if current.lock_epoch:
                self._write_lock_tombstone(
                    current.lock_epoch,
                    released_by=user.username if user else "",
                    expected_snapshot_seq=carry_seq,
                    expected_snapshot_hash=carry_hash,
                )
            self._last_known_remote_lock = None
            self._last_known_remote_lock_at = 0.0
            self.clear_host_telemetry()
            unlocked = HostLock(
                is_locked=False,
                status="idle",
                expected_snapshot_seq=carry_seq,
                expected_snapshot_hash=carry_hash,
            )
            self._write_lock_bundle(unlocked, include_session=False)
            self._emit("lock_updated", unlocked.to_dict())
            self.git_push_if_enabled("Release host lock and sync world")
            return unlocked

    def force_unlock(self) -> HostLock:
        """Admin emergency lock override that preserves expected snapshot verification state."""
        with self._lock_mutex:
            self._owns_active_lock = False
            self._active_lock_epoch = ""
            self._cached_own_lock = None
            self._cached_own_lock_mtime = 0.0
            self._stop_heartbeat_loop()
            current = self.get_lock()

            carry_seq, carry_hash, _ = self.get_expected_snapshot_state()
            manifest = self._read_sync_manifest()
            if manifest:
                m_seq = int(manifest.get("snapshotSeq", 0) or 0)
                m_hash = str(manifest.get("sha256", "") or "")
                if m_seq > carry_seq:
                    carry_seq, carry_hash = m_seq, m_hash

            if current.lock_epoch:
                self._write_lock_tombstone(
                    current.lock_epoch,
                    released_by="force_unlock",
                    expected_snapshot_seq=carry_seq,
                    expected_snapshot_hash=carry_hash,
                )
            for ldir in self._iter_locks_dirs():
                try:
                    for item in list(ldir.iterdir()):
                        if item.is_file() and item.name.startswith("session-") and item.name.endswith(".json"):
                            raw_epoch = item.name[len("session-"):-len(".json")]
                            epoch = self._strip_conflict_suffix(raw_epoch)
                            self._write_lock_tombstone(
                                epoch,
                                released_by="force_unlock",
                                expected_snapshot_seq=carry_seq,
                                expected_snapshot_hash=carry_hash,
                            )
                            item.unlink(missing_ok=True)
                except OSError:
                    pass

            conflict_pattern = re.compile(r"^host-lock\s*\(\d+\)\.json$", re.IGNORECASE)
            try:
                if self.shared_dir.exists():
                    for item in list(self.shared_dir.iterdir()):
                        if item.is_file() and conflict_pattern.match(item.name):
                            item.unlink(missing_ok=True)
            except OSError:
                pass

            self._last_known_remote_lock = None
            self._last_known_remote_lock_at = 0.0
            self.clear_host_telemetry()
            unlocked = HostLock(
                is_locked=False,
                status="idle",
                expected_snapshot_seq=carry_seq,
                expected_snapshot_hash=carry_hash,
            )
            self._write_lock_bundle(unlocked, include_session=False)
            self._emit("lock_updated", unlocked.to_dict())
            self.git_push_if_enabled("Force unlock host lock")
            return unlocked

    def _start_heartbeat_loop(self) -> None:
        self._stop_heartbeat_loop()
        stop_event = threading.Event()
        self._heartbeat_stop = stop_event
        expected_epoch = self._active_lock_epoch

        def _loop() -> None:
            while not stop_event.wait(HEARTBEAT_INTERVAL_SEC):
                with self._lock_mutex:
                    if stop_event.is_set() or not self._owns_active_lock:
                        break
                    if expected_epoch and self._active_lock_epoch and expected_epoch != self._active_lock_epoch:
                        break
                    current = self.get_lock()
                    if (
                        current.is_locked
                        and current.machine_id == self.local_config.machine_id
                        and (not expected_epoch or not current.lock_epoch or current.lock_epoch == expected_epoch)
                    ):
                        current.last_heartbeat = time.time()
                        snap = self._apply_telemetry_provider(current, publish=False)
                        self.save_lock(current, fsync=False)
                        if snap is not None:
                            self._publish_telemetry_from_lock(current, snap)
                    else:
                        self._owns_active_lock = False
                        self._active_lock_epoch = ""
                        self._cached_own_lock = None
                        self._cached_own_lock_mtime = 0.0
                        self._emit("lock_lost", {
                            "expectedEpoch": expected_epoch,
                            "currentLock": current.to_dict(),
                        })
                        break

        self._heartbeat_thread = threading.Thread(target=_loop, daemon=True, name="HostLockHeartbeat")
        self._heartbeat_thread.start()

    def _stop_heartbeat_loop(self) -> None:
        self._heartbeat_stop.set()

    # -------------------------------------------------------------------------
    # World & Server Files Snapshot Sync (Shared Cloud Folder <-> Local Runtime)
    # -------------------------------------------------------------------------

    def _has_snapshot_conflicts(self) -> bool:
        snap_folder_re = re.compile(r"^snapshots\s*\(\d+\)$", re.IGNORECASE)
        snap_file_re = re.compile(r"^(sync-manifest|latest-server|file-index)\s*\(\d+\)\.(json|zip)$", re.IGNORECASE)
        delta_conflict_re = re.compile(r"^delta-.+\s*\(\d+\)\.zip$", re.IGNORECASE)
        try:
            with os.scandir(self.shared_dir) as it:
                for entry in it:
                    ename = entry.name
                    if ename.lower().startswith("snapshots") and snap_folder_re.match(ename):
                        if entry.is_dir(follow_symlinks=False):
                            return True
                    elif snap_file_re.match(ename) and entry.is_file(follow_symlinks=False):
                        return True
        except OSError:
            pass
        try:
            with os.scandir(self.snapshots_dir) as it:
                for entry in it:
                    if snap_file_re.match(entry.name) and entry.is_file(follow_symlinks=False):
                        return True
        except OSError:
            pass
        try:
            with os.scandir(self.deltas_dir) as it:
                for entry in it:
                    if delta_conflict_re.match(entry.name) and entry.is_file(follow_symlinks=False):
                        return True
        except OSError:
            pass
        return False

    def ensure_runtime_synced_when_idle(self, strict: bool = False) -> None:
        """
        Ensures that before an idle machine reads or edits config files (like server.properties),
        if a newer or different snapshot/manifest exists in the Shared Cloud Folder (from another member's session),
        it pulls that snapshot first so we never clobber a newer world save!
        """
        with self._sync_mutex:
            self.git_pull_if_enabled()
            if self._has_snapshot_conflicts():
                self._cleanup_cloud_conflict_files()
            manifest = self._read_sync_manifest()
            expected_seq, expected_hash, _ = self.get_expected_snapshot_state()
            if not self.latest_snapshot_path.exists() and not manifest and expected_seq == 0:
                self._ensure_default_runtime_files()
                return
            try:
                manifest_seq = int(manifest.get("snapshotSeq", 0)) if manifest else 0
                manifest_hash = str(manifest.get("sha256", "") or "").strip().lower() if manifest else ""
                if expected_seq > 0 and (
                    not manifest
                    or manifest_seq < expected_seq
                    or (expected_hash and manifest_seq == expected_seq and manifest_hash != expected_hash.lower())
                ):
                    self.pull_world_to_runtime(strict=strict)
                    return

                missing_runtime = not (self.runtime_dir / "server.properties").exists()
                last_mtime, last_size, last_path = self._last_synced_snapshot_sig
                if self.latest_snapshot_path.exists():
                    st = self.latest_snapshot_path.stat()
                    if last_path == str(self.latest_snapshot_path):
                        snap_changed = abs(st.st_mtime - last_mtime) > 1e-3 or st.st_size != last_size
                    elif manifest_seq > 0 and manifest_seq == self._last_synced_manifest_seq and manifest_hash and manifest_hash == self._last_synced_snapshot_hash:
                        snap_changed = False
                    else:
                        snap_changed = True
                else:
                    snap_changed = bool(manifest)

                manifest_changed = bool(
                    manifest
                    and (
                        (manifest_seq > 0 and manifest_seq != self._last_synced_manifest_seq)
                        or (manifest_hash and manifest_hash != self._last_synced_snapshot_hash)
                    )
                )
                if missing_runtime or snap_changed or manifest_changed:
                    self.pull_world_to_runtime(strict=strict)
            except OSError:
                if strict:
                    raise

    def _safe_extract_zip(self, zf: zipfile.ZipFile, dest_dir: Path) -> None:
        """
        Safely extracts all zip members preventing zip-slip path traversal and symlink vulnerabilities.
        """
        dest_resolved = dest_dir.resolve()
        for member in zf.infolist():
            # Reject POSIX symlink entries inside archives
            if ((member.external_attr >> 16) & 0o170000) == 0o120000:
                raise ValueError(f"Symlink entry not allowed in snapshot: {member.filename}")
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

    def _extract_zip_to_runtime_safely(self, zip_source: Path, operation: str = "pull") -> Dict[str, Any]:
        """
        Extracts a snapshot or backup zip into a temporary staging folder first so all
        entries & CRCs are verified BEFORE clearing `./data/runtime-server`.
        Preserves static JVM/Paper binaries (`server*.jar`, `libraries/`, `versions/`,
        `bundler/`, `cache/`, `logs/`, `crash-reports/`, `timings/`, `debug/`, `plugins/.paper-remapped`).
        """
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        staging_dir = self.base_data_dir / f".pull_staging_{os.getpid()}_{threading.get_ident()}"
        preserved_entries = set(self.EXCLUDED_TOP_DIRS) | {"jar-version.json"}

        def _is_preserved_top(name: str) -> bool:
            if name in preserved_entries:
                return True
            if name == "server.jar" or (name.startswith("server-") and name.endswith(".jar")):
                return True
            if name.lower().endswith(".hprof"):
                return True
            return False

        try:
            if staging_dir.exists():
                shutil.rmtree(staging_dir, ignore_errors=True)
            staging_dir.mkdir(parents=True, exist_ok=True)
            dest_resolved = staging_dir.resolve()

            file_count = 0
            with zipfile.ZipFile(zip_source, "r") as zf:
                infos = zf.infolist()
                if not infos:
                    raise ValueError(f"Archive '{zip_source.name}' is empty.")
                total_files = sum(1 for m in infos if not m.is_dir())
                total_bytes = sum(max(0, m.file_size) for m in infos if not m.is_dir())
                done_files = 0
                done_bytes = 0
                self._set_sync_progress(
                    True,
                    operation=operation,
                    phase="extracting",
                    label="Extracting Server Snapshot..." if operation != "restore" else "Extracting Backup Archive...",
                    detail=f"Extracting {total_files} files from {zip_source.name}...",
                    percent=12,
                    done_files=0,
                    total_files=total_files,
                    done_bytes=0,
                    total_bytes=total_bytes,
                    force_emit=True,
                )
                for member in infos:
                    if ((member.external_attr >> 16) & 0o170000) == 0o120000:
                        raise ValueError(f"Symlink entry not allowed in snapshot: {member.filename}")
                    target_path = (staging_dir / member.filename).resolve()
                    if not (target_path == dest_resolved or target_path.is_relative_to(dest_resolved)):
                        raise ValueError(f"Zip path traversal detected: {member.filename}")
                    zf.extract(member, staging_dir)
                    if not member.is_dir():
                        done_files += 1
                        done_bytes += max(0, member.file_size)
                        pct = 12 + int(70 * (done_bytes / total_bytes if total_bytes > 0 else done_files / max(1, total_files)))
                        self._set_sync_progress(
                            True,
                            operation=operation,
                            phase="extracting",
                            label="Extracting Server Snapshot..." if operation != "restore" else "Extracting Backup Archive...",
                            detail=f"Extracted {done_files}/{total_files} files...",
                            percent=min(82, pct),
                            done_files=done_files,
                            total_files=total_files,
                            done_bytes=done_bytes,
                            total_bytes=total_bytes,
                        )
                file_count = total_files

            self._set_sync_progress(
                True,
                operation=operation,
                phase="committing",
                label="Applying Extracted Server Files...",
                detail=f"Applying {file_count} files cleanly to runtime directory...",
                percent=86,
                done_files=file_count,
                total_files=file_count,
                done_bytes=done_bytes,
                total_bytes=total_bytes,
                force_emit=True,
            )

            # Remove stale Minecraft session.lock files inside staging
            for lock_file in staging_dir.rglob("session.lock"):
                try:
                    lock_file.unlink()
                except OSError:
                    pass

            # Move existing dynamic files into rollback staging before placing new files so mid-pull OSError never wipes world
            rollback_dir = self.base_data_dir / f".pull_rollback_{os.getpid()}_{threading.get_ident()}"
            if rollback_dir.exists():
                shutil.rmtree(rollback_dir, ignore_errors=True)
            rollback_dir.mkdir(parents=True, exist_ok=True)
            moved_to_rollback: List[Tuple[Path, Path]] = []
            try:
                for item in list(self.runtime_dir.iterdir()):
                    if _is_preserved_top(item.name):
                        continue
                    if item.name == "plugins" and item.is_dir():
                        rb_plugins = rollback_dir / "plugins"
                        rb_plugins.mkdir(parents=True, exist_ok=True)
                        for sub in list(item.iterdir()):
                            if sub.name == ".paper-remapped":
                                continue
                            if sub.name == "spark" and sub.is_dir():
                                rb_spark = rb_plugins / "spark"
                                rb_spark.mkdir(parents=True, exist_ok=True)
                                for sp_sub in list(sub.iterdir()):
                                    if sp_sub.name in ("profiles", "dumps"):
                                        continue
                                    rb_dst = rb_spark / sp_sub.name
                                    shutil.move(str(sp_sub), str(rb_dst))
                                    moved_to_rollback.append((rb_dst, sp_sub))
                                continue
                            rb_dst = rb_plugins / sub.name
                            shutil.move(str(sub), str(rb_dst))
                            moved_to_rollback.append((rb_dst, sub))
                        continue
                    rb_dst = rollback_dir / item.name
                    shutil.move(str(item), str(rb_dst))
                    moved_to_rollback.append((rb_dst, item))

                # Move verified staged items into runtime_dir without ever nesting dirs on locked paths
                def _place_entry(src: Path, dst: Path) -> None:
                    if dst.exists():
                        self._remove_path_with_retry(dst)
                    if not dst.exists():
                        shutil.move(str(src), str(dst))
                    elif src.is_dir():
                        for existing_sub in list(dst.iterdir()):
                            if dst.name == "plugins" and existing_sub.name == ".paper-remapped":
                                continue
                            if dst.name == "spark" and dst.parent.name == "plugins" and existing_sub.name in ("profiles", "dumps"):
                                continue
                            self._remove_path_with_retry(existing_sub)
                        shutil.copytree(str(src), str(dst), dirs_exist_ok=True, copy_function=shutil.copyfile)
                    else:
                        shutil.copyfile(str(src), str(dst))

                for staged_item in list(staging_dir.iterdir()):
                    if _is_preserved_top(staged_item.name):
                        continue
                    dest_item = self.runtime_dir / staged_item.name
                    if staged_item.name == "plugins" and staged_item.is_dir():
                        dest_item.mkdir(parents=True, exist_ok=True)
                        for sub in list(staged_item.iterdir()):
                            if sub.name == ".paper-remapped":
                                continue
                            if sub.name == "spark" and sub.is_dir():
                                spark_dest = dest_item / "spark"
                                spark_dest.mkdir(parents=True, exist_ok=True)
                                for sp_sub in list(sub.iterdir()):
                                    if sp_sub.name in ("profiles", "dumps"):
                                        continue
                                    _place_entry(sp_sub, spark_dest / sp_sub.name)
                                continue
                            _place_entry(sub, dest_item / sub.name)
                        continue
                    _place_entry(staged_item, dest_item)
            except Exception:
                # Roll back previous runtime entries if placing new files failed partway
                for rb_src, orig_dst in moved_to_rollback:
                    try:
                        if orig_dst.exists():
                            self._remove_path_with_retry(orig_dst)
                        if rb_src.exists():
                            orig_dst.parent.mkdir(parents=True, exist_ok=True)
                            shutil.move(str(rb_src), str(orig_dst))
                    except OSError:
                        pass
                raise
            finally:
                if rollback_dir.exists():
                    shutil.rmtree(rollback_dir, ignore_errors=True)

            self._ensure_default_runtime_files()
            return {"fileCount": file_count}
        finally:
            if staging_dir.exists():
                shutil.rmtree(staging_dir, ignore_errors=True)

    def _resolve_verified_snapshot_source(self, manifest: Optional[Dict[str, Any]]) -> Tuple[Optional[Path], str]:
        """
        Resolves the authoritative snapshot zip matching `sync-manifest.json` (checking `latest-server.zip`,
        then its paired backup archive, then Drive conflict copies).
        Also enforces any newer snapshot sequence/hash recorded in `host-lock.json` or release tombstones.
        Returns `(path_or_none, mismatch_reason)`.
        """
        expected_seq, expected_hash, released_by = self.get_expected_snapshot_state()
        manifest_seq = int(manifest.get("snapshotSeq", 0) or 0) if manifest else 0
        manifest_hash = str(manifest.get("sha256", "") or "").strip().lower() if manifest else ""

        if expected_seq > 0 and (
            not manifest
            or manifest_seq < expected_seq
            or (expected_hash and manifest_seq == expected_seq and manifest_hash != expected_hash.lower())
        ):
            waiter = released_by or (manifest.get("hostUsername", "") if manifest else "") or "the previous host"
            return (
                None,
                f"Cloud sync is still catching up from {waiter}'s session "
                f"(waiting for snapshot #{expected_seq}). Wait for Google Drive / cloud sync to finish before hosting or editing files!",
            )

        snap_exists = self.latest_snapshot_path.exists()
        if not manifest:
            if snap_exists:
                try:
                    if self.latest_snapshot_path.stat().st_size == 0:
                        return (
                            None,
                            "Cloud sync is still catching up ('latest-server.zip' is 0 bytes). Wait for Google Drive / cloud sync to finish before hosting or modifying files!",
                        )
                except OSError:
                    pass
            return (self.latest_snapshot_path if snap_exists else None), ""

        mode = str(manifest.get("mode") or "full").lower()
        host_name = str(manifest.get("hostUsername", "") or "the previous host")
        backup_name = Path(str(manifest.get("backupFile", "") or "")).name

        def _matches_hash_size(p: Path, target_hash: str, target_size: int = -1) -> bool:
            if not p.exists() or not p.is_file():
                return False
            try:
                if target_size >= 0 and p.stat().st_size != target_size:
                    return False
                if target_hash and self._cached_archive_sha256(p).lower() != target_hash:
                    return False
                return True
            except OSError:
                return False

        if mode == "delta":
            base_hash = str(manifest.get("baseSha256") or "").strip().lower()
            base_size = int(manifest.get("baseSizeBytes", -1) or -1)
            bk_hash = str(manifest.get("backupSha256") or "").strip().lower()
            bk_size = int(manifest.get("backupSizeBytes", -1) or -1)
            idx_hash = str(manifest.get("fileIndexSha256") or "").strip().lower()

            base_ok = snap_exists and _matches_hash_size(self.latest_snapshot_path, base_hash, base_size)
            idx_ok = (not idx_hash) or _matches_hash_size(self.file_index_path, idx_hash, -1)
            deltas_ok = True
            for d_entry in (manifest.get("deltas") or []):
                if not isinstance(d_entry, dict):
                    continue
                d_rel = str(d_entry.get("file") or "")
                d_path = self.snapshots_dir / d_rel if d_rel else None
                d_hash = str(d_entry.get("sha256") or "").strip().lower()
                d_size = int(d_entry.get("sizeBytes", -1) or -1)
                if not d_path or not _matches_hash_size(d_path, d_hash, d_size):
                    deltas_ok = False
                    break

            if base_ok and idx_ok and deltas_ok:
                return self.latest_snapshot_path, ""

            exp_hash = str(manifest.get("sha256") or "").strip().lower()
            exp_size = int(manifest.get("sizeBytes", -1) or -1)
            if snap_exists and exp_hash and _matches_hash_size(self.latest_snapshot_path, exp_hash, exp_size):
                return self.latest_snapshot_path, ""

            if backup_name and backup_name not in (".", ".."):
                bk_path = self.backups_dir / backup_name
                if bk_hash and _matches_hash_size(bk_path, bk_hash, bk_size):
                    return bk_path, ""

            exp_mb = round(max(0, base_size if base_size >= 0 else int(manifest.get("sizeBytes", 0) or 0)) / (1024 * 1024), 1)
            reason = (
                f"Cloud sync is still catching up from {host_name}'s session "
                f"(expected {exp_mb} MB snapshot"
                + (f" / backup '{backup_name}'" if backup_name else "")
                + "). Wait for Google Drive / cloud sync to finish before hosting or modifying files so progress is not overwritten!"
            )
            return None, reason

        exp_hash = str(manifest.get("sha256", "")).strip().lower()
        exp_size = int(manifest.get("sizeBytes", -1) or -1)

        if snap_exists and _matches_hash_size(self.latest_snapshot_path, exp_hash, exp_size):
            return self.latest_snapshot_path, ""

        # If backup file in sync-manifest already arrived cleanly (common when Drive freezes latest-server.zip), use it!
        if backup_name and backup_name not in (".", ".."):
            bk_path = self.backups_dir / backup_name
            bk_hash = str(manifest.get("backupSha256") or exp_hash).strip().lower()
            bk_size = int(manifest.get("backupSizeBytes", exp_size) or exp_size)
            if _matches_hash_size(bk_path, bk_hash, bk_size):
                return bk_path, ""

        conflict_pattern = re.compile(r"^latest-server\s*\(\d+\)\.zip$", re.IGNORECASE)
        if self.snapshots_dir.exists():
            for item in self.snapshots_dir.iterdir():
                if item.is_file() and conflict_pattern.match(item.name) and _matches_hash_size(item, exp_hash, exp_size):
                    return item, ""

        exp_mb = round(max(0, exp_size) / (1024 * 1024), 1)
        reason = (
            f"Cloud sync is still catching up from {host_name}'s session "
            f"(expected {exp_mb} MB snapshot"
            + (f" / backup '{backup_name}'" if backup_name else "")
            + "). Wait for Google Drive / cloud sync to finish before hosting or modifying files so progress is not overwritten!"
        )
        return None, reason

    def _build_runtime_file_index(self, now: Optional[float] = None) -> Tuple[List[Tuple[Path, Path]], Dict[str, Dict[str, Any]], int]:
        """Scans eligible runtime files (excluding regenerable/static dirs) and computes cached SHA-256 index."""
        t_now = now if now is not None else time.time()
        items: List[Tuple[Path, Path]] = []
        file_index: Dict[str, Dict[str, Any]] = {}
        total_bytes = 0
        for root, dirs, files in os.walk(self.runtime_dir):
            rel_root = Path(root).relative_to(self.runtime_dir)
            if rel_root.parts and rel_root.parts[0] in self.EXCLUDED_TOP_DIRS:
                dirs[:] = []
                continue
            if ".paper-remapped" in dirs:
                dirs.remove(".paper-remapped")
            if "tmp-client" in dirs:
                dirs.remove("tmp-client")
            if rel_root.parts == ("plugins", "spark"):
                for sp_excl in ("profiles", "dumps"):
                    if sp_excl in dirs:
                        dirs.remove(sp_excl)
            for file in files:
                abs_file = Path(root) / file
                rel_file = abs_file.relative_to(self.runtime_dir)
                rel_posix = rel_file.as_posix()
                if self._is_static_runtime_relpath(rel_posix):
                    continue
                items.append((abs_file, rel_file))
                try:
                    hex_hash, sz = self._fast_file_sha256(abs_file, rel_posix, now=t_now)
                    file_index[rel_posix] = {"sha256": hex_hash, "size": sz}
                    total_bytes += sz
                except OSError:
                    continue
        return items, file_index, total_bytes

    def _prune_old_deltas(self, keep_files: Optional[set] = None) -> None:
        keep = keep_files or set()
        if not self.deltas_dir.exists():
            return
        try:
            for item in list(self.deltas_dir.iterdir()):
                if item.is_file() and item.name not in keep:
                    try:
                        item.unlink(missing_ok=True)
                    except OSError:
                        pass
        except OSError:
            pass

    def _apply_incremental_pull(
        self,
        source_zip: Path,
        manifest: Optional[Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        """
        Applies an incremental delta/base pull by reusing identical files already in `runtime_dir`,
        extracting only missing or modified files across active deltas + base archive, and removing
        files absent from `file-index.json`. Returns None to fall back to full archive extraction.
        """
        if not manifest or source_zip != self.latest_snapshot_path:
            return None
        if str(manifest.get("mode") or "").lower() == "delta":
            exp_hash = str(manifest.get("sha256") or "").strip().lower()
            base_hash = str(manifest.get("baseSha256") or "").strip().lower()
            if exp_hash and exp_hash != base_hash:
                try:
                    if self._cached_archive_sha256(self.latest_snapshot_path).lower() == exp_hash:
                        return None
                except OSError:
                    pass
        idx_hash = str(manifest.get("fileIndexSha256") or "").strip().lower()
        if not idx_hash or not self.file_index_path.exists():
            return None
        try:
            if self._cached_archive_sha256(self.file_index_path).lower() != idx_hash:
                return None
        except OSError:
            return None
        idx_data = self._read_json_safe(self.file_index_path)
        if not isinstance(idx_data, dict) or not isinstance(idx_data.get("files"), dict):
            return None
        target_files: Dict[str, Dict[str, Any]] = idx_data["files"]

        now = time.time()
        local_entries, local_index, _ = self._build_runtime_file_index(now=now)
        local_rel_set = {rel.as_posix() for _, rel in local_entries}

        needed: Dict[str, str] = {}
        for rel_posix, meta in target_files.items():
            if not isinstance(meta, dict):
                continue
            exp_hash = str(meta.get("sha256") or "").lower()
            exp_size = int(meta.get("size", -1))
            cur = local_index.get(rel_posix)
            if cur and cur.get("size") == exp_size and cur.get("sha256") == exp_hash:
                continue
            needed[rel_posix] = exp_hash

        extra_rels = sorted(rel for rel in local_rel_set if rel not in target_files)
        if not needed and not extra_rels:
            self._ensure_default_runtime_files()
            self._set_sync_progress(
                True,
                operation="pull",
                phase="done",
                label="World Files Already Up to Date",
                detail=f"Reused all {len(target_files)} unchanged server files.",
                percent=100,
                done_files=len(target_files),
                total_files=len(target_files),
                force_emit=True,
            )
            return {
                "fileCount": len(target_files),
                "extractedFiles": 0,
                "reusedFiles": len(target_files),
            }

        total_needed_files = len(needed)
        total_needed_bytes = sum(
            int((target_files.get(rp) or {}).get("size", 0) or 0)
            for rp in needed
        )
        reused_count = max(0, len(target_files) - total_needed_files)
        self._set_sync_progress(
            True,
            operation="pull",
            phase="extracting",
            label="Syncing Changed World Files...",
            detail=f"Updating {total_needed_files} changed files (reusing {reused_count} unchanged)...",
            percent=18,
            done_files=0,
            total_files=total_needed_files,
            done_bytes=0,
            total_bytes=total_needed_bytes,
            force_emit=True,
        )

        staging_dir = self.base_data_dir / f".pull_inc_staging_{os.getpid()}_{threading.get_ident()}"
        rollback_dir = self.base_data_dir / f".pull_inc_rollback_{os.getpid()}_{threading.get_ident()}"
        try:
            if staging_dir.exists():
                shutil.rmtree(staging_dir, ignore_errors=True)
            staging_dir.mkdir(parents=True, exist_ok=True)
            staging_resolved = staging_dir.resolve()

            # Build newest-first list of archives to source needed entries from
            archive_paths: List[Path] = []
            for d_entry in reversed(manifest.get("deltas") or []):
                if isinstance(d_entry, dict) and d_entry.get("file"):
                    d_path = self.snapshots_dir / str(d_entry["file"])
                    if d_path.exists():
                        archive_paths.append(d_path)
            if self.latest_snapshot_path.exists():
                archive_paths.append(self.latest_snapshot_path)

            remaining = set(needed.keys())
            done_needed_files = 0
            done_needed_bytes = 0
            for arc_path in archive_paths:
                if not remaining:
                    break
                with zipfile.ZipFile(arc_path, "r") as zf:
                    by_norm: Dict[str, zipfile.ZipInfo] = {}
                    for member in zf.infolist():
                        if ((member.external_attr >> 16) & 0o170000) == 0o120000:
                            raise ValueError(f"Symlink entry not allowed in snapshot: {member.filename}")
                        norm_m = member.filename.replace("\\", "/").lstrip("/")
                        t_path = (staging_dir / norm_m).resolve()
                        if not (t_path == staging_resolved or t_path.is_relative_to(staging_resolved)):
                            raise ValueError(f"Zip path traversal detected: {member.filename}")
                        if not member.is_dir():
                            by_norm[norm_m] = member
                    for rel_posix in list(remaining):
                        member = by_norm.get(rel_posix)
                        if member is None:
                            continue
                        dst_staged = staging_dir / rel_posix
                        dst_staged.parent.mkdir(parents=True, exist_ok=True)
                        with zf.open(member, "r") as src_f, open(dst_staged, "wb") as out_f:
                            shutil.copyfileobj(src_f, out_f, length=1024 * 1024)
                        remaining.remove(rel_posix)
                        done_needed_files += 1
                        done_needed_bytes += int((target_files.get(rel_posix) or {}).get("size", 0) or member.file_size or 0)
                        frac = (
                            done_needed_bytes / total_needed_bytes
                            if total_needed_bytes > 0
                            else done_needed_files / max(1, total_needed_files)
                        )
                        self._set_sync_progress(
                            True,
                            operation="pull",
                            phase="extracting",
                            label="Syncing Changed World Files...",
                            detail=f"Extracted {done_needed_files}/{total_needed_files} changed files...",
                            percent=18 + int(67 * frac),
                            done_files=done_needed_files,
                            total_files=total_needed_files,
                            done_bytes=done_needed_bytes,
                            total_bytes=total_needed_bytes,
                        )

            if remaining:
                raise ValueError(f"Missing entries from snapshot archive chain: {sorted(remaining)[:5]}")

            self._set_sync_progress(
                True,
                operation="pull",
                phase="committing",
                label="Applying World Updates...",
                detail=f"Applying {total_needed_files} updated files and {len(extra_rels)} removals...",
                percent=88,
                done_files=total_needed_files,
                total_files=total_needed_files,
                done_bytes=total_needed_bytes,
                total_bytes=total_needed_bytes,
                force_emit=True,
            )

            if rollback_dir.exists():
                shutil.rmtree(rollback_dir, ignore_errors=True)
            rollback_dir.mkdir(parents=True, exist_ok=True)
            moved_to_rollback: List[Tuple[Path, Path]] = []
            affected_rels = list(needed.keys()) + extra_rels
            try:
                for rel_posix in affected_rels:
                    src_rt = self.runtime_dir / rel_posix
                    if src_rt.exists() and src_rt.is_file():
                        rb_dst = rollback_dir / rel_posix
                        rb_dst.parent.mkdir(parents=True, exist_ok=True)
                        shutil.move(str(src_rt), str(rb_dst))
                        moved_to_rollback.append((rb_dst, src_rt))
                        self._file_hash_cache.pop(rel_posix, None)

                for rel_posix in needed.keys():
                    staged_src = staging_dir / rel_posix
                    dst_rt = self.runtime_dir / rel_posix
                    dst_rt.parent.mkdir(parents=True, exist_ok=True)
                    if dst_rt.exists():
                        self._remove_path_with_retry(dst_rt)
                    shutil.move(str(staged_src), str(dst_rt))
                    self._fast_file_sha256(dst_rt, rel_posix, now=now)

                # Clean up empty directories left behind by deleted files
                for rel_posix in extra_rels:
                    parent = (self.runtime_dir / rel_posix).parent
                    while parent != self.runtime_dir and parent.exists():
                        try:
                            if any(parent.iterdir()):
                                break
                            parent.rmdir()
                            parent = parent.parent
                        except OSError:
                            break
            except Exception:
                for rb_src, orig_dst in moved_to_rollback:
                    try:
                        if orig_dst.exists():
                            self._remove_path_with_retry(orig_dst)
                        if rb_src.exists():
                            orig_dst.parent.mkdir(parents=True, exist_ok=True)
                            shutil.move(str(rb_src), str(orig_dst))
                    except OSError:
                        pass
                raise
            finally:
                if rollback_dir.exists():
                    shutil.rmtree(rollback_dir, ignore_errors=True)

            self._ensure_default_runtime_files()
            return {
                "fileCount": len(target_files),
                "extractedFiles": len(needed),
                "reusedFiles": max(0, len(target_files) - len(needed)),
            }
        finally:
            if staging_dir.exists():
                shutil.rmtree(staging_dir, ignore_errors=True)

    def pull_world_to_runtime(self, strict: bool = False) -> Dict[str, Any]:
        """
        Extracts the verified latest world snapshot (or only changed delta files when `file-index.json` matches)
        from the Shared Sync Folder into `./data/runtime-server`.
        Preserves static JVM/Paper libraries (`server*.jar`, `libraries/`, `versions/`, `bundler/`, `cache/`, `plugins/.paper-remapped`).
        """
        with self._sync_mutex:
            t_start = time.time()
            self._set_sync_progress(
                True,
                operation="pull",
                phase="verifying",
                label="Checking Cloud World Snapshot...",
                detail="Verifying manifest and snapshot checksums...",
                percent=5,
                force_emit=True,
            )
            try:
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
                pull_info: Optional[Dict[str, Any]] = None
                for attempt in range(5):
                    try:
                        pull_info = self._apply_incremental_pull(source_zip, manifest)
                        if pull_info is None:
                            pull_info = self._extract_zip_to_runtime_safely(source_zip, operation="pull")
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

                st = source_zip.stat()
                snap_mtime, snap_size = st.st_mtime, st.st_size
                snap_hash = (
                    str(manifest.get("sha256", "") or "").lower()
                    if manifest
                    else self._cached_archive_sha256(source_zip).lower()
                )
                manifest_seq = int(manifest.get("snapshotSeq", 0)) if manifest else 0
                self._last_synced_snapshot_mtime = snap_mtime
                self._last_synced_snapshot_sig = (snap_mtime, snap_size, str(source_zip))
                if manifest and source_zip == self.latest_snapshot_path:
                    base_h = str(manifest.get("baseSha256") or manifest.get("sha256") or "").lower()
                    if base_h:
                        self._last_verified_base_sig = (snap_mtime, snap_size, base_h)
                self._save_sync_state(
                    snap_mtime,
                    snap_size,
                    str(source_zip),
                    manifest_seq=manifest_seq,
                    sha256_hex=snap_hash,
                )
                return {
                    "synced": True,
                    "snapshotPath": str(source_zip),
                    "sizeBytes": snap_size,
                    "extractedFiles": (pull_info or {}).get("extractedFiles", (pull_info or {}).get("fileCount", 0)),
                    "reusedFiles": (pull_info or {}).get("reusedFiles", 0),
                    "elapsedSec": round(time.time() - t_start, 3),
                    "timestamp": time.time(),
                }
            finally:
                self._set_sync_progress(False, force_emit=True)

    def _verify_required_lock_epoch(self, require_lock_epoch: str) -> None:
        if not require_lock_epoch:
            return
        if not self.owns_lock(require_lock_epoch):
            raise HostLockConflictError("Host lock lost before snapshot push; aborting to prevent overwrite.")
        if require_lock_epoch in self._load_released_epochs():
            raise HostLockConflictError("Host lock epoch was already released; aborting snapshot push.")
        current = self.get_lock()
        if current.is_locked and (
            current.machine_id != self.local_config.machine_id
            or (current.lock_epoch and current.lock_epoch != require_lock_epoch)
        ):
            raise HostLockConflictError("Host lock was transferred to another machine; aborting snapshot push.")

    def push_world_from_runtime(
        self,
        create_backup: bool = True,
        backup_label: str = "auto",
        host_username: str = "",
        require_lock_epoch: str = "",
        strict_files: bool = True,
        stage_copy_before_zip: bool = False,
        force_full: bool = False,
        backup_scope: str = "cloud",
        on_staging_complete: Optional[Callable[[], None]] = None,
        operation_name: str = "push",
    ) -> Dict[str, Any]:
        """
        Packages `./data/runtime-server` (excluding static/regenerable JVM & Paperclip binaries)
        into either a compact incremental delta zip (`snapshots/deltas/delta-<seq>.zip`) when a valid
        `latest-server.zip` base + `file-index.json` exist within compaction limits, or a full compacted
        `snapshots/latest-server.zip` base archive, using fast per-extension compression (`ZIP_STORED` for
        already-compressed `.mca`/`.jar`/`.dat`/`.gz`/`.png`, `ZIP_DEFLATED level=1` for text/configs).
        `backup_scope` controls where `create_backup=True` stores the backup archive: `"local"` (in
        `./data/backups`, zero Drive upload), `"cloud"` (in `snapshots/backups`), or `"none"`.
        """
        scope = str(backup_scope or "cloud").strip().lower()
        if scope not in ("local", "cloud", "none"):
            scope = "cloud"
        op_name = str(operation_name or "push")
        op_title = (
            "Syncing Live World to Cloud..."
            if op_name == "auto_sync"
            else ("Creating World Backup..." if op_name == "backup" else "Syncing World to Cloud...")
        )
        staging_notified = False

        def _notify_staging_done() -> None:
            nonlocal staging_notified
            if staging_notified:
                return
            staging_notified = True
            if on_staging_complete is not None:
                try:
                    on_staging_complete()
                except Exception:
                    pass

        with self._sync_mutex:
            t_start = time.time()
            self._set_sync_progress(
                True,
                operation=op_name,
                phase="scanning",
                label=op_title,
                detail="Scanning server files and verifying changes...",
                percent=5,
                force_emit=True,
            )
            self._verify_required_lock_epoch(require_lock_epoch)
            self._ensure_shared_dirs()
            self.runtime_dir.mkdir(parents=True, exist_ok=True)
            self.local_backups_dir.mkdir(parents=True, exist_ok=True)

            # Build zips inside local non-cloud data_dir so Google Drive never sees half-written temp zips
            tmp_zip = self.base_data_dir / f".push_stage.{os.getpid()}.{threading.get_ident()}.zip"
            tmp_delta = self.base_data_dir / f".push_delta.{os.getpid()}.{threading.get_ident()}.zip"
            stage_root = self.base_data_dir / f".push_files_{os.getpid()}_{threading.get_ident()}"

            def _stage_entries(entries: List[Tuple[Path, Path]]) -> List[Tuple[Path, Path]]:
                if not stage_copy_before_zip:
                    _notify_staging_done()
                    return entries
                if stage_root.exists():
                    shutil.rmtree(stage_root, ignore_errors=True)
                stage_root.mkdir(parents=True, exist_ok=True)
                total_stage = len(entries)
                self._set_sync_progress(
                    True,
                    operation=op_name,
                    phase="staging",
                    label=op_title,
                    detail=f"Staging {total_stage} modified files...",
                    percent=20,
                    done_files=0,
                    total_files=total_stage,
                    force_emit=True,
                )
                staged: List[Tuple[Path, Path]] = []
                for idx_s, (abs_file, rel_file) in enumerate(entries, start=1):
                    dst_file = stage_root / rel_file
                    dst_file.parent.mkdir(parents=True, exist_ok=True)
                    last_err: Optional[Exception] = None
                    for attempt in range(4):
                        try:
                            shutil.copyfile(abs_file, dst_file)
                            last_err = None
                            break
                        except OSError as e:
                            last_err = e
                            time.sleep(0.08 * (attempt + 1))
                    if last_err is not None:
                        if strict_files:
                            raise RuntimeError(f"Cannot stage '{rel_file}' for snapshot: {last_err}")
                        continue
                    staged.append((dst_file, rel_file))
                    if total_stage > 0 and (idx_s == total_stage or idx_s % 5 == 0):
                        self._set_sync_progress(
                            True,
                            operation=op_name,
                            phase="staging",
                            label=op_title,
                            detail=f"Staged {idx_s}/{total_stage} files...",
                            percent=20 + int(15 * (idx_s / total_stage)),
                            done_files=idx_s,
                            total_files=total_stage,
                        )
                _notify_staging_done()
                return staged

            def _pack_zip(
                out_zip: Path,
                sources: List[Tuple[Path, Path]],
                pct_start: int = 38,
                pct_end: int = 80,
                pack_label: str = "",
            ) -> int:
                count = 0
                total_src = len(sources)
                total_src_bytes = 0
                for sf, _ in sources:
                    try:
                        total_src_bytes += sf.stat().st_size
                    except OSError:
                        pass
                done_src_bytes = 0
                lbl = pack_label or op_title
                self._set_sync_progress(
                    True,
                    operation=op_name,
                    phase="packing",
                    label=lbl,
                    detail=f"Packaging {total_src} files...",
                    percent=pct_start,
                    done_files=0,
                    total_files=total_src,
                    done_bytes=0,
                    total_bytes=total_src_bytes,
                    force_emit=True,
                )
                with zipfile.ZipFile(out_zip, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=1) as zf:
                    for idx_p, (src_file, rel_file) in enumerate(sources, start=1):
                        arc_posix = rel_file.as_posix()
                        f_sz = 0
                        try:
                            f_sz = src_file.stat().st_size
                        except OSError:
                            pass
                        last_err: Optional[Exception] = None
                        for attempt in range(4):
                            try:
                                self._zip_write_file(zf, src_file, arc_posix)
                                count += 1
                                last_err = None
                                break
                            except OSError as e:
                                last_err = e
                                time.sleep(0.08 * (attempt + 1))
                        if last_err is not None and strict_files:
                            raise RuntimeError(f"Cannot archive '{rel_file}' for snapshot: {last_err}")
                        done_src_bytes += f_sz
                        frac = (
                            done_src_bytes / total_src_bytes
                            if total_src_bytes > 0
                            else idx_p / max(1, total_src)
                        )
                        cur_pct = pct_start + int((pct_end - pct_start) * frac)
                        self._set_sync_progress(
                            True,
                            operation=op_name,
                            phase="packing",
                            label=lbl,
                            detail=f"Packaged {idx_p}/{total_src} files...",
                            percent=cur_pct,
                            done_files=idx_p,
                            total_files=total_src,
                            done_bytes=done_src_bytes,
                            total_bytes=total_src_bytes,
                        )
                return count

            backup_name = None
            backup_sha256 = ""
            backup_size = 0
            try:
                eligible, current_index, total_raw_bytes = self._build_runtime_file_index(now=t_start)
                file_count = len(eligible)
                self._set_sync_progress(
                    True,
                    operation=op_name,
                    phase="hashing",
                    label=op_title,
                    detail=f"Checked {file_count} server files ({round(total_raw_bytes / (1024 * 1024), 1)} MB)...",
                    percent=16,
                    done_files=file_count,
                    total_files=file_count,
                    done_bytes=total_raw_bytes,
                    total_bytes=total_raw_bytes,
                )

                prev_manifest = self._read_sync_manifest()
                prev_seq = int(prev_manifest.get("snapshotSeq", 0)) if prev_manifest else 0
                exp_seq, _, _ = self.get_expected_snapshot_state()
                next_seq = max(prev_seq, self._last_synced_manifest_seq, exp_seq) + 1

                base_seq = 0
                base_hash = ""
                prev_deltas: List[Dict[str, Any]] = []
                if prev_manifest:
                    if str(prev_manifest.get("mode") or "full").lower() == "delta":
                        base_seq = int(prev_manifest.get("baseSeq", 0) or 0)
                        base_hash = str(prev_manifest.get("baseSha256") or "").lower()
                        prev_deltas = [d for d in (prev_manifest.get("deltas") or []) if isinstance(d, dict)]
                    else:
                        base_seq = int(prev_manifest.get("snapshotSeq", 0) or 0)
                        base_hash = str(prev_manifest.get("sha256") or "").lower()
                        prev_deltas = []

                base_valid = False
                base_size = 0
                if base_seq > 0 and base_hash and self.latest_snapshot_path.exists():
                    try:
                        bst = self.latest_snapshot_path.stat()
                        base_size = bst.st_size
                        if base_size > 0:
                            if self._last_verified_base_sig == (bst.st_mtime, bst.st_size, base_hash):
                                base_valid = True
                            elif self._cached_archive_sha256(self.latest_snapshot_path).lower() == base_hash:
                                base_valid = True
                                self._last_verified_base_sig = (bst.st_mtime, bst.st_size, base_hash)
                    except OSError:
                        base_valid = False

                prev_files: Dict[str, Any] = {}
                if base_valid and prev_manifest and prev_manifest.get("fileIndexSha256") and self.file_index_path.exists():
                    try:
                        if self._cached_archive_sha256(self.file_index_path).lower() == str(prev_manifest["fileIndexSha256"]).lower():
                            raw_idx = self._read_json_safe(self.file_index_path)
                            if isinstance(raw_idx, dict) and isinstance(raw_idx.get("files"), dict):
                                prev_files = raw_idx["files"]
                    except OSError:
                        prev_files = {}

                changed_entries: List[Tuple[Path, Path]] = []
                changed_raw_bytes = 0
                for abs_file, rel_file in eligible:
                    rp = rel_file.as_posix()
                    cur_meta = current_index.get(rp)
                    prev_meta = prev_files.get(rp) if prev_files else None
                    if not isinstance(prev_meta, dict) or not cur_meta or prev_meta.get("sha256") != cur_meta.get("sha256"):
                        changed_entries.append((abs_file, rel_file))
                        changed_raw_bytes += int((cur_meta or {}).get("size", 0) or 0)

                deleted_files = sorted(set(prev_files.keys()) - set(current_index.keys())) if prev_files else []
                cum_delta_bytes = sum(int(d.get("sizeBytes", 0) or 0) for d in prev_deltas)
                deltas_intact = all(
                    isinstance(d, dict) and d.get("file") and (self.snapshots_dir / str(d["file"])).exists()
                    for d in prev_deltas
                )

                # Fast no-op exit when nothing changed and no backup file was requested
                if (
                    not force_full
                    and base_valid
                    and bool(prev_files)
                    and deltas_intact
                    and len(changed_entries) == 0
                    and len(deleted_files) == 0
                    and (not create_backup or scope == "none")
                ):
                    _notify_staging_done()
                    self._verify_required_lock_epoch(require_lock_epoch)
                    noop_stats = {
                        "mode": "noop",
                        "uploadedBytes": 0,
                        "skippedBytes": total_raw_bytes,
                        "changedFiles": 0,
                        "deletedFiles": 0,
                        "totalFiles": file_count,
                        "deltaCount": len(prev_deltas),
                        "elapsedSec": round(time.time() - t_start, 3),
                    }
                    self._last_sync_stats = noop_stats
                    return {
                        "pushed": True,
                        "unchanged": True,
                        "mode": "noop",
                        "fileCount": file_count,
                        "changedFiles": 0,
                        "deletedFiles": 0,
                        "sizeBytes": 0,
                        "uploadedBytes": 0,
                        "skippedBytes": total_raw_bytes,
                        "sha256": str(prev_manifest.get("sha256", "") if prev_manifest else ""),
                        "snapshotSeq": prev_seq,
                        "backupFile": None,
                        "lastSyncStats": noop_stats,
                        "timestamp": time.time(),
                    }

                huge_rewrite = (
                    total_raw_bytes > 1_048_576
                    and len(eligible) > 0
                    and len(changed_entries) > len(eligible) * 0.60
                    and changed_raw_bytes > total_raw_bytes * 0.60
                )
                can_delta = (
                    (not force_full)
                    and base_valid
                    and bool(prev_files)
                    and deltas_intact
                    and (not huge_rewrite)
                    and (len(prev_deltas) < self.MAX_DELTA_CHAIN)
                    and (base_size > 0 and cum_delta_bytes < int(base_size * self.MAX_DELTA_RATIO))
                )

                want_backup = bool(create_backup and scope != "none")
                full_zip_built = False
                full_sha256 = ""
                full_size = 0
                staged_delta_sources: Optional[List[Tuple[Path, Path]]] = None

                if want_backup or not can_delta:
                    sources = _stage_entries(eligible)
                    if can_delta:
                        changed_rel_set = {rel_f.as_posix() for _, rel_f in changed_entries}
                        staged_delta_sources = [
                            (src_p, rel_f) for src_p, rel_f in sources if rel_f.as_posix() in changed_rel_set
                        ]
                    file_count = _pack_zip(
                        tmp_zip,
                        sources,
                        pct_start=36,
                        pct_end=72 if can_delta else 80,
                        pack_label=op_title,
                    )
                    self._verify_required_lock_epoch(require_lock_epoch)
                    full_sha256 = self._sha256_file(tmp_zip).lower()
                    full_size = tmp_zip.stat().st_size
                    full_zip_built = True

                    if want_backup:
                        ts = time.strftime("%Y%m%d-%H%M%S")
                        safe_label = "".join(c for c in backup_label if c.isalnum() or c in ("-", "_")) or "backup"
                        bk_fname = f"world-{ts}-{safe_label}.zip"
                        target_bk_dir = self.backups_dir if scope == "cloud" else self.local_backups_dir
                        target_bk_dir.mkdir(parents=True, exist_ok=True)
                        backup_target = target_bk_dir / bk_fname
                        if backup_target.exists():
                            bk_fname = f"world-{ts}-{int(time.time() * 1000) % 1000:03d}-{safe_label}.zip"
                            backup_target = target_bk_dir / bk_fname
                        self._set_sync_progress(
                            True,
                            operation=op_name,
                            phase="writing_backup",
                            label=op_title,
                            detail=f"Saving {scope} backup '{bk_fname}'...",
                            percent=75 if can_delta else 82,
                            done_files=file_count,
                            total_files=file_count,
                            force_emit=True,
                        )
                        tmp_bk = target_bk_dir / f".{bk_fname}.tmp.{os.getpid()}.{threading.get_ident()}"
                        try:
                            shutil.copyfile(tmp_zip, tmp_bk)
                            os.replace(tmp_bk, backup_target)
                        except OSError:
                            if tmp_bk.exists():
                                shutil.copyfile(tmp_bk, backup_target)
                        finally:
                            try:
                                if tmp_bk.exists():
                                    tmp_bk.unlink(missing_ok=True)
                            except OSError:
                                pass
                        if scope == "cloud":
                            backup_name = bk_fname
                            backup_sha256 = full_sha256
                            backup_size = full_size
                            self._prune_old_backups(protect_name=bk_fname, scope="cloud")
                        else:
                            self._prune_old_backups(protect_name=bk_fname, scope="local")

                if can_delta:
                    delta_sources = staged_delta_sources if staged_delta_sources is not None else _stage_entries(changed_entries)
                    delta_packed = _pack_zip(
                        tmp_delta,
                        delta_sources,
                        pct_start=45 if not full_zip_built else 78,
                        pct_end=88,
                        pack_label=op_title,
                    )
                    self._verify_required_lock_epoch(require_lock_epoch)
                    delta_sha256 = self._sha256_file(tmp_delta).lower()
                    delta_size = tmp_delta.stat().st_size

                    self.deltas_dir.mkdir(parents=True, exist_ok=True)
                    delta_fname = f"delta-{next_seq}-{delta_sha256[:8]}.zip"
                    delta_rel = f"deltas/{delta_fname}"
                    delta_target = self.deltas_dir / delta_fname
                    self._set_sync_progress(
                        True,
                        operation=op_name,
                        phase="uploading",
                        label=op_title,
                        detail=f"Writing delta snapshot ({delta_packed} changed files, {round(delta_size / 1024, 1)} KB)...",
                        percent=92,
                        done_files=delta_packed,
                        total_files=delta_packed,
                        done_bytes=delta_size,
                        total_bytes=delta_size,
                        force_emit=True,
                    )
                    self._move_or_copy_to_cloud(tmp_delta, delta_target, is_immutable=True)

                    idx_payload = {
                        "snapshotSeq": next_seq,
                        "baseSeq": base_seq,
                        "files": current_index,
                    }
                    self._write_json_atomic(self.file_index_path, idx_payload, in_place_if_exists=True)
                    idx_sha256 = self._cached_archive_sha256(self.file_index_path).lower()

                    deltas_list = prev_deltas + [{
                        "seq": next_seq,
                        "file": delta_rel,
                        "sha256": delta_sha256,
                        "sizeBytes": delta_size,
                        "changedFiles": delta_packed,
                        "deletedFiles": len(deleted_files),
                    }]
                    skipped_bytes = max(0, total_raw_bytes - changed_raw_bytes)
                    sync_stats = {
                        "mode": "delta",
                        "uploadedBytes": delta_size,
                        "skippedBytes": skipped_bytes,
                        "changedFiles": delta_packed,
                        "deletedFiles": len(deleted_files),
                        "totalFiles": file_count,
                        "deltaCount": len(deltas_list),
                        "elapsedSec": round(time.time() - t_start, 3),
                    }
                    self._last_sync_stats = sync_stats

                    manifest = self._write_sync_manifest(
                        delta_target,
                        sha256_hex=delta_sha256,
                        size_bytes=delta_size,
                        file_count=file_count,
                        backup_file=backup_name,
                        host_username=host_username or backup_label,
                        snapshot_seq=next_seq,
                        extra={
                            "mode": "delta",
                            "baseSeq": base_seq,
                            "baseSha256": base_hash,
                            "baseSizeBytes": base_size,
                            "deltaFile": delta_rel,
                            "deltas": deltas_list,
                            "fileIndexSha256": idx_sha256,
                            "backupSha256": backup_sha256,
                            "backupSizeBytes": backup_size,
                            "lastSyncStats": sync_stats,
                        },
                    )
                    st = self.latest_snapshot_path.stat()
                    self._last_synced_snapshot_mtime = st.st_mtime
                    self._last_synced_snapshot_sig = (st.st_mtime, st.st_size, str(self.latest_snapshot_path))
                    self._save_sync_state(
                        st.st_mtime,
                        st.st_size,
                        str(self.latest_snapshot_path),
                        manifest_seq=int(manifest.get("snapshotSeq", 0)),
                        sha256_hex=delta_sha256,
                    )
                    self.git_push_if_enabled(f"Sync delta snapshot ({delta_packed} changed / {file_count} total files)")
                    self._set_sync_progress(
                        True,
                        operation=op_name,
                        phase="done",
                        label=op_title,
                        detail=f"Synced {delta_packed} changed file(s) ({round(delta_size / 1024.0, 1)} KB).",
                        percent=100,
                        done_files=delta_packed,
                        total_files=delta_packed,
                        done_bytes=delta_size,
                        total_bytes=delta_size,
                        force_emit=True,
                    )
                    return {
                        "pushed": True,
                        "mode": "delta",
                        "fileCount": file_count,
                        "changedFiles": delta_packed,
                        "deletedFiles": len(deleted_files),
                        "sizeBytes": delta_size,
                        "uploadedBytes": delta_size,
                        "skippedBytes": skipped_bytes,
                        "sha256": delta_sha256,
                        "snapshotSeq": int(manifest.get("snapshotSeq", 0)),
                        "backupFile": backup_name,
                        "lastSyncStats": sync_stats,
                        "timestamp": time.time(),
                    }

                # Full / compacted base snapshot push
                self._set_sync_progress(
                    True,
                    operation=op_name,
                    phase="uploading",
                    label=op_title,
                    detail=f"Writing compacted snapshot ({file_count} files, {round(full_size / (1024 * 1024), 1)} MB)...",
                    percent=90,
                    done_files=file_count,
                    total_files=file_count,
                    done_bytes=full_size,
                    total_bytes=full_size,
                    force_emit=True,
                )
                self._move_or_copy_to_cloud(tmp_zip, self.latest_snapshot_path, is_immutable=False)
                try:
                    os.utime(self.latest_snapshot_path, None)
                except OSError:
                    pass

                st = self.latest_snapshot_path.stat()
                self._last_verified_base_sig = (st.st_mtime, st.st_size, full_sha256.lower())
                idx_payload = {
                    "snapshotSeq": next_seq,
                    "baseSeq": next_seq,
                    "files": current_index,
                }
                self._write_json_atomic(self.file_index_path, idx_payload, in_place_if_exists=True)
                idx_sha256 = self._cached_archive_sha256(self.file_index_path).lower()
                self._prune_old_deltas(keep_files=set())

                sync_stats = {
                    "mode": "full",
                    "uploadedBytes": full_size,
                    "skippedBytes": 0,
                    "changedFiles": file_count,
                    "deletedFiles": 0,
                    "totalFiles": file_count,
                    "deltaCount": 0,
                    "elapsedSec": round(time.time() - t_start, 3),
                }
                self._last_sync_stats = sync_stats

                manifest = self._write_sync_manifest(
                    self.latest_snapshot_path,
                    sha256_hex=full_sha256,
                    size_bytes=full_size,
                    file_count=file_count,
                    backup_file=backup_name,
                    host_username=host_username or backup_label,
                    snapshot_seq=next_seq,
                    extra={
                        "mode": "full",
                        "baseSeq": next_seq,
                        "baseSha256": full_sha256,
                        "baseSizeBytes": full_size,
                        "deltaFile": "",
                        "deltas": [],
                        "fileIndexSha256": idx_sha256,
                        "backupSha256": backup_sha256 or (full_sha256 if backup_name else ""),
                        "backupSizeBytes": backup_size or (full_size if backup_name else 0),
                        "lastSyncStats": sync_stats,
                    },
                )
                self._set_sync_progress(
                    True,
                    operation=op_name,
                    phase="done",
                    label=op_title,
                    detail=f"Synced snapshot ({file_count} files, {round(full_size / (1024 * 1024), 1)} MB).",
                    percent=100,
                    done_files=file_count,
                    total_files=file_count,
                    done_bytes=full_size,
                    total_bytes=full_size,
                    force_emit=True,
                )
            finally:
                _notify_staging_done()
                for p in (tmp_zip, tmp_delta):
                    try:
                        if p.exists():
                            p.unlink(missing_ok=True)
                    except OSError:
                        pass
                try:
                    if stage_root.exists():
                        shutil.rmtree(stage_root, ignore_errors=True)
                except OSError:
                    pass
                self._set_sync_progress(False, force_emit=True)

            self._last_synced_snapshot_mtime = st.st_mtime
            self._last_synced_snapshot_sig = (st.st_mtime, st.st_size, str(self.latest_snapshot_path))
            self._save_sync_state(
                st.st_mtime,
                st.st_size,
                str(self.latest_snapshot_path),
                manifest_seq=int(manifest.get("snapshotSeq", 0)),
                sha256_hex=full_sha256,
            )

            self.git_push_if_enabled(f"Sync world snapshot ({file_count} files)")
            return {
                "pushed": True,
                "mode": "full",
                "fileCount": file_count,
                "changedFiles": file_count,
                "deletedFiles": 0,
                "sizeBytes": full_size,
                "uploadedBytes": full_size,
                "skippedBytes": 0,
                "sha256": full_sha256,
                "snapshotSeq": int(manifest.get("snapshotSeq", 0)),
                "backupFile": backup_name,
                "lastSyncStats": sync_stats,
                "timestamp": time.time(),
            }

    def push_live_snapshot(
        self,
        lock_epoch: str = "",
        create_backup: bool = False,
        host_username: str = "",
        on_staging_complete: Optional[Callable[[], None]] = None,
    ) -> Dict[str, Any]:
        """
        Pushes a staged world snapshot while the server is actively running without dropping
        or modifying `host-lock.json` / `session-<epoch>.json`.
        Uses a bounded `_sync_mutex` acquire so periodic auto-sync never collides with stop/backup.
        """
        acquired = self._sync_mutex.acquire(timeout=2.0)
        if not acquired:
            if on_staging_complete is not None:
                try:
                    on_staging_complete()
                except Exception:
                    pass
            return {"pushed": False, "skipped": True, "reason": "Another snapshot sync is already in progress."}
        try:
            eff_epoch = lock_epoch or self._active_lock_epoch
            if not eff_epoch or not self.owns_lock(eff_epoch):
                if on_staging_complete is not None:
                    try:
                        on_staging_complete()
                    except Exception:
                        pass
                return {"pushed": False, "skipped": True, "reason": "Host lock is not held by this machine."}
            return self.push_world_from_runtime(
                create_backup=create_backup,
                backup_label=host_username or "autosync",
                host_username=host_username,
                require_lock_epoch=eff_epoch,
                strict_files=True,
                stage_copy_before_zip=True,
                backup_scope="cloud" if create_backup else "none",
                on_staging_complete=on_staging_complete,
                operation_name="auto_sync",
            )
        finally:
            self._sync_mutex.release()

    def list_backups(self) -> List[Dict[str, Any]]:
        self._ensure_shared_dirs(cleanup_conflicts=False)
        self.local_backups_dir.mkdir(parents=True, exist_ok=True)
        entries = []
        seen_keys = set()
        for loc, bdir in (("local", self.local_backups_dir), ("cloud", self.backups_dir)):
            try:
                for f in bdir.glob("*.zip"):
                    try:
                        st = f.stat()
                        seen_keys.add((loc, f.name))
                        entries.append((st.st_mtime, {
                            "name": f.name,
                            "sizeBytes": st.st_size,
                            "createdAt": st.st_mtime,
                            "location": loc,
                        }))
                    except OSError:
                        continue
            except OSError:
                continue
        entries.sort(key=lambda item: item[0], reverse=True)
        return [item[1] for item in entries]

    def resolve_backup_path(self, backup_name: str, location: str = "") -> Optional[Tuple[Path, str]]:
        safe_name = Path(backup_name).name
        if not safe_name or safe_name in (".", ".."):
            return None
        loc = str(location or "").strip().lower()
        search_order: List[Tuple[str, Path]]
        if loc == "local":
            search_order = [("local", self.local_backups_dir)]
        elif loc == "cloud":
            search_order = [("cloud", self.backups_dir)]
        else:
            search_order = [("local", self.local_backups_dir), ("cloud", self.backups_dir)]
        for cur_loc, base_dir in search_order:
            try:
                root_res = base_dir.resolve()
                cand = (root_res / safe_name).resolve()
                if cand.parent == root_res and cand.exists() and cand.is_file():
                    return cand, cur_loc
            except OSError:
                continue
        return None

    def restore_backup(self, backup_name: str, location: str = "") -> Dict[str, Any]:
        safe_name = Path(backup_name).name
        if not safe_name or safe_name in (".", ".."):
            raise ValueError("Invalid backup name.")

        with self._sync_mutex:
            with self._lock_mutex:
                try:
                    self._set_sync_progress(
                        True,
                        operation="restore",
                        phase="verifying",
                        label="Restoring World Backup...",
                        detail=f"Checking lock and verifying '{safe_name}'...",
                        percent=5,
                        force_emit=True,
                    )
                    self.git_pull_if_enabled()
                    current_lock = self.get_lock()
                    if current_lock.is_actively_locked():
                        raise HostLockConflictError(
                            f"Cannot restore backup while '{current_lock.host_username}' is hosting on '{current_lock.machine_name}'. Stop the server first."
                        )

                    self._ensure_shared_dirs()
                    self.local_backups_dir.mkdir(parents=True, exist_ok=True)
                    transfer_msg = self._has_active_cloud_transfer()
                    if transfer_msg:
                        raise RuntimeError(f"{transfer_msg} Wait for cloud sync to finish before restoring a backup.")

                    resolved = self.resolve_backup_path(safe_name, location=location)
                    if resolved is None:
                        raise FileNotFoundError(f"Backup '{safe_name}' not found.")
                    source, resolved_loc = resolved

                    # Stage-extract & validate every entry/CRC directly from backup BEFORE modifying latest-server.zip or runtime-server
                    try:
                        extract_info = self._extract_zip_to_runtime_safely(source, operation="restore")
                    except (zipfile.BadZipFile, zlib.error, ValueError) as e:
                        raise ValueError(f"Backup '{safe_name}' is invalid or corrupted: {e}")
                    except OSError as e:
                        raise RuntimeError(f"Backup file '{safe_name}' is temporarily locked by cloud sync ({e}). Please try again.")

                    # Promote backup -> latest-server.zip
                    self._set_sync_progress(
                        True,
                        operation="restore",
                        phase="uploading",
                        label="Promoting Restored World to Cloud...",
                        detail=f"Updating cloud snapshot from '{safe_name}'...",
                        percent=88,
                        done_files=extract_info.get("fileCount", 0),
                        total_files=extract_info.get("fileCount", 0),
                        force_emit=True,
                    )
                    try:
                        sha256_hex = self._cached_archive_sha256(source)
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
                    self._last_verified_base_sig = (st.st_mtime, st.st_size, sha256_hex.lower())
                    _, restored_index, _ = self._build_runtime_file_index()
                    idx_payload = {
                        "snapshotSeq": 0,
                        "files": restored_index,
                    }
                    self._write_json_atomic(self.file_index_path, idx_payload, in_place_if_exists=True)
                    idx_sha256 = self._cached_archive_sha256(self.file_index_path)
                    self._prune_old_deltas(keep_files=set())

                    sync_stats = {
                        "mode": "full",
                        "uploadedBytes": src_size,
                        "skippedBytes": 0,
                        "changedFiles": extract_info.get("fileCount", 0),
                        "deletedFiles": 0,
                        "totalFiles": len(restored_index),
                        "deltaCount": 0,
                        "elapsedSec": 0.0,
                    }
                    self._last_sync_stats = sync_stats

                    manifest = self._write_sync_manifest(
                        self.latest_snapshot_path,
                        sha256_hex=sha256_hex,
                        size_bytes=src_size,
                        file_count=extract_info.get("fileCount", 0),
                        backup_file=safe_name if resolved_loc == "cloud" else "",
                        host_username="Restore",
                        extra={
                            "mode": "full",
                            "baseSha256": sha256_hex,
                            "baseSizeBytes": src_size,
                            "deltaFile": "",
                            "deltas": [],
                            "fileIndexSha256": idx_sha256,
                            "backupSha256": sha256_hex if resolved_loc == "cloud" else "",
                            "backupSizeBytes": src_size if resolved_loc == "cloud" else 0,
                            "lastSyncStats": sync_stats,
                        },
                    )
                    idx_payload["snapshotSeq"] = int(manifest.get("snapshotSeq", 0))
                    idx_payload["baseSeq"] = idx_payload["snapshotSeq"]
                    self._write_json_atomic(self.file_index_path, idx_payload, in_place_if_exists=True)
                    manifest["fileIndexSha256"] = self._cached_archive_sha256(self.file_index_path)
                    self._write_json_atomic(self.sync_manifest_path, manifest, in_place_if_exists=True)
                    self._last_synced_snapshot_mtime = st.st_mtime
                    self._last_synced_snapshot_sig = (st.st_mtime, st.st_size, str(self.latest_snapshot_path))
                    self._save_sync_state(
                        st.st_mtime,
                        st.st_size,
                        str(self.latest_snapshot_path),
                        manifest_seq=int(manifest.get("snapshotSeq", 0)),
                        sha256_hex=sha256_hex,
                    )

                    # Clean up any stale/empty unlocked host-lock.json while preserving expectedSnapshotSeq/Hash
                    if not self.get_lock().is_actively_locked():
                        try:
                            self.force_unlock()
                        except Exception:
                            pass

                    self.git_push_if_enabled(f"Restore backup {safe_name}")
                    self._set_sync_progress(
                        True,
                        operation="restore",
                        phase="done",
                        label="Backup Restored",
                        detail=f"Restored '{safe_name}' to runtime and shared cloud.",
                        percent=100,
                        done_files=int(extract_info.get("fileCount", 0) or 0),
                        total_files=int(extract_info.get("fileCount", 0) or 0),
                        done_bytes=st.st_size,
                        total_bytes=st.st_size,
                        force_emit=True,
                    )
                    return {
                        "restored": safe_name,
                        "location": resolved_loc,
                        "pull": {
                            "synced": True,
                            "snapshotPath": str(self.latest_snapshot_path),
                            "sizeBytes": st.st_size,
                            "fileCount": extract_info.get("fileCount", 0),
                            "timestamp": time.time(),
                        },
                    }
                finally:
                    self._set_sync_progress(False, force_emit=True)

    def _is_snapshot_chain_intact(self, manifest: Optional[Dict[str, Any]]) -> bool:
        if not isinstance(manifest, dict):
            return False
        exp_hash = str(manifest.get("baseSha256") or manifest.get("sha256") or "").lower()
        snap_ok = (
            self.latest_snapshot_path.exists()
            and bool(exp_hash)
            and self._cached_archive_sha256(self.latest_snapshot_path).lower() == exp_hash
        )
        if snap_ok and str(manifest.get("mode") or "").lower() == "delta":
            for d_entry in (manifest.get("deltas") or []):
                if isinstance(d_entry, dict) and d_entry.get("file") and d_entry.get("sha256"):
                    dp = self.snapshots_dir / str(d_entry["file"])
                    if not dp.exists() or self._cached_archive_sha256(dp).lower() != str(d_entry["sha256"]).lower():
                        return False
        return snap_ok

    def delete_backup(self, backup_name: str, location: str = "") -> bool:
        safe_name = Path(backup_name).name
        if not safe_name or safe_name in (".", ".."):
            return False
        lock = self.get_lock()
        if lock.is_actively_locked() and not self.is_locked_by_current_machine(lock):
            raise HostLockConflictError(
                f"Cannot delete backup '{safe_name}' while '{lock.host_username}' is hosting on '{lock.machine_name}'."
            )
        acquired = self._sync_mutex.acquire(timeout=5.0)
        if not acquired:
            raise RuntimeError("Another snapshot sync or backup operation is in progress. Try again in a moment.")
        try:
            lock = self.get_lock()
            if lock.is_actively_locked() and not self.is_locked_by_current_machine(lock):
                raise HostLockConflictError(
                    f"Cannot delete backup '{safe_name}' while '{lock.host_username}' is hosting on '{lock.machine_name}'."
                )
            resolved = self.resolve_backup_path(safe_name, location=location)
            if resolved is None:
                return False
            target, resolved_loc = resolved
            if resolved_loc == "cloud":
                manifest = self._read_sync_manifest()
                if manifest and Path(str(manifest.get("backupFile", "") or "")).name == safe_name:
                    if not self._is_snapshot_chain_intact(manifest):
                        raise RuntimeError(
                            f"Cannot delete '{safe_name}' while it is the active fallback for the latest cloud snapshot."
                        )
                    manifest["backupFile"] = ""
                    manifest.pop("backupSha256", None)
                    manifest.pop("backupSizeBytes", None)
                    self._write_json_atomic(self.sync_manifest_path, manifest, in_place_if_exists=True)
            target.unlink()
            return True
        finally:
            self._sync_mutex.release()

    def prune_cloud_backups(self, keep: int = 0) -> Dict[str, Any]:
        """Removes older cloud backups in `snapshots/backups` down to `keep` to stop Drive multi-GB downloads."""
        keep_count = max(0, min(20, int(keep)))
        lock = self.get_lock()
        if lock.is_actively_locked() and not self.is_locked_by_current_machine(lock):
            raise HostLockConflictError(
                f"Cannot clean cloud backups while '{lock.host_username}' is hosting on '{lock.machine_name}'."
            )
        acquired = self._sync_mutex.acquire(timeout=5.0)
        if not acquired:
            raise RuntimeError("Another snapshot sync or backup operation is in progress. Try again in a moment.")
        try:
            self._ensure_shared_dirs(cleanup_conflicts=False)
            with_mtime: List[Tuple[float, Path, int]] = []
            for p in self.backups_dir.glob("*.zip"):
                try:
                    st = p.stat()
                    with_mtime.append((st.st_mtime, p, st.st_size))
                except OSError:
                    continue
            with_mtime.sort(key=lambda item: item[0], reverse=True)

            manifest = self._read_sync_manifest()
            manifest_bk = Path(str(manifest.get("backupFile", "") or "")).name if manifest else ""
            chain_intact = self._is_snapshot_chain_intact(manifest)

            kept: List[str] = []
            deleted: List[str] = []
            freed_bytes = 0
            manifest_cleared = False

            for idx, (_, p, sz) in enumerate(with_mtime):
                if idx < keep_count:
                    kept.append(p.name)
                    continue
                if manifest_bk and p.name == manifest_bk and not chain_intact:
                    kept.append(p.name)
                    continue
                try:
                    p.unlink()
                    deleted.append(p.name)
                    freed_bytes += sz
                    if manifest and manifest_bk and p.name == manifest_bk:
                        manifest["backupFile"] = ""
                        manifest.pop("backupSha256", None)
                        manifest.pop("backupSizeBytes", None)
                        manifest_cleared = True
                except OSError:
                    kept.append(p.name)

            if manifest_cleared and manifest:
                self._write_json_atomic(self.sync_manifest_path, manifest, in_place_if_exists=True)

            return {
                "deleted": deleted,
                "removedCount": len(deleted),
                "kept": kept,
                "keptCount": len(kept),
                "freedBytes": freed_bytes,
            }
        finally:
            self._sync_mutex.release()

    def _prune_old_backups(self, protect_name: str = "", scope: str = "cloud") -> None:
        cfg = self.load_cluster_config()
        if scope == "local":
            max_b = max(1, cfg.max_backups if cfg else 10)
            target_dir = self.local_backups_dir
            protected = {n for n in (protect_name,) if n}
        else:
            max_b = max(0, min(20, getattr(cfg, "max_cloud_backups", 2) if cfg else 2))
            target_dir = self.backups_dir
            manifest = self._read_sync_manifest()
            manifest_bk = Path(str(manifest.get("backupFile", "") or "")).name if manifest else ""
            protected = {n for n in (protect_name, manifest_bk) if n}
        with_mtime: List[Tuple[float, Path]] = []
        for p in target_dir.glob("*.zip"):
            try:
                with_mtime.append((p.stat().st_mtime, p))
            except OSError:
                continue
        with_mtime.sort(key=lambda item: item[0], reverse=True)
        for _, old in with_mtime[max_b:]:
            if old.name in protected:
                continue
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
