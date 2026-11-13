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
        self.telemetry_provider: Optional[Callable[[], Dict[str, Any]]] = None
        self._lock_mutex = threading.RLock()
        self._sync_mutex = threading.RLock()
        self._browse_lock = threading.Lock()
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._heartbeat_stop = threading.Event()
        self._owns_active_lock = False
        self._active_lock_epoch: str = ""
        self._last_synced_snapshot_mtime: float = 0.0
        self._last_synced_snapshot_sig: Tuple[float, int, str] = (0.0, -1, "")
        self._last_synced_snapshot_hash: str = ""
        self._last_synced_manifest_seq: int = 0
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
                and os.path.isdir(self.backups_dir)
            ):
                return
            self.shared_dir.mkdir(parents=True, exist_ok=True)
            self.snapshots_dir.mkdir(parents=True, exist_ok=True)
            self.backups_dir.mkdir(parents=True, exist_ok=True)
            self.locks_dir.mkdir(parents=True, exist_ok=True)
            self.shared_tunnel_dir.mkdir(parents=True, exist_ok=True)
            if cleanup_conflicts:
                self._cleanup_cloud_conflict_files()
        except Exception:
            pass

    @staticmethod
    def _strip_conflict_suffix(stem: str) -> str:
        """Strips Google Drive / OneDrive / Dropbox conflict suffixes like ' (1)' from a file stem."""
        return re.sub(r"\s*\(\d+\)$", "", stem).strip()

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
        """Returns a human-readable notice if Google Drive / OneDrive / Dropbox is actively syncing a temp file."""
        now = time.time()
        marker_names = {".tmp.drivedownload", ".tmp.driveupload", ".dropbox.cache"}
        for folder in (self.shared_dir, self.snapshots_dir, self.backups_dir, self.locks_dir):
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
                                                action = "uploading" if "upload" in ename else "downloading"
                                                return f"Cloud sync is currently {action} files ({mentry.name}, {round(st.st_size / (1024 * 1024), 1)} MB)."
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
                                    return f"Cloud sync is currently downloading files ({ename}, {round(st.st_size / (1024 * 1024), 1)} MB)."
                            except OSError:
                                continue
            except OSError:
                continue
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
        exp_seq, _, _ = self.get_expected_snapshot_state()
        base_seq = max(prev_seq, self._last_synced_manifest_seq, exp_seq)
        manifest = {
            "snapshotSeq": base_seq + 1,
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
        """Merges accounts and newer settings from a cloud conflict config file before cleanup."""
        try:
            cdata = self._read_json_safe(conflict_path)
            if not isinstance(cdata, dict) or not cdata:
                return
            ccfg = ClusterConfig.from_dict(cdata)
            mcfg = self.load_cluster_config()
            if mcfg is None:
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

            changed = False
            # If the conflict file is newer than or equal to the main file, adopt its non-account cluster settings
            if c_mtime >= m_mtime and c_mtime > 0.0:
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
                existing = by_id.get(acc.id)
                if existing is None:
                    mcfg.accounts.append(acc)
                    changed = True
                else:
                    if acc.last_active_at > existing.last_active_at:
                        existing.last_active_at = acc.last_active_at
                        changed = True
                    if acc.host_count > existing.host_count:
                        existing.host_count = acc.host_count
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
        conflict_pattern = re.compile(r"^(host-lock|cluster-config|latest-server|sync-manifest)\s*\(\d+\)\.(json|zip)$", re.IGNORECASE)
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
                        c_manifest_path = item / "sync-manifest.json"
                        c_zip_path = item / "latest-server.zip"
                        c_manifest = self._read_json_safe(c_manifest_path) if c_manifest_path.exists() else None
                        if isinstance(c_manifest, dict) and c_zip_path.exists():
                            cur_seq = int(manifest.get("snapshotSeq", 0)) if manifest else 0
                            c_seq = int(c_manifest.get("snapshotSeq", 0) or 0)
                            c_hash = str(c_manifest.get("sha256", "")).lower()
                            if c_seq > cur_seq and c_hash and self._sha256_file(c_zip_path).lower() == c_hash:
                                try:
                                    self._copy_file_preserving_cloud_id(c_zip_path, self.latest_snapshot_path)
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
            for item in list(folder.iterdir()):
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
                                if cand_src is not None:
                                    self._write_json_atomic(self.sync_manifest_path, cm, in_place_if_exists=True)
                                    manifest = cm
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

        backup_count = 0
        try:
            with os.scandir(self.backups_dir) as it:
                for entry in it:
                    if entry.name.endswith(".zip") and entry.is_file(follow_symlinks=False):
                        backup_count += 1
        except OSError:
            backup_count = 0

        return {
            "sharedSyncDir": str(shared),
            "providerDetected": provider,
            "clusterConfigExists": self.cluster_config_path.exists(),
            "hostLockExists": self.host_lock_path.exists(),
            "latestSnapshotExists": snap_exists,
            "latestSnapshotSizeBytes": snap_size,
            "latestSnapshotModifiedAt": snap_mtime,
            "backupCount": backup_count,
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
            # Fallback to newest valid Google Drive conflict copy if main cluster-config.json is missing/corrupt
            conflict_pattern = re.compile(r"^cluster-config\s*\(\d+\)\.json$", re.IGNORECASE)
            candidates: List[Tuple[float, Dict[str, Any]]] = []
            try:
                if self.shared_dir.exists():
                    for item in self.shared_dir.iterdir():
                        if item.is_file() and conflict_pattern.match(item.name):
                            cdata = self._read_json_safe(item)
                            if isinstance(cdata, dict) and cdata.get("accounts"):
                                try:
                                    cmtime = item.stat().st_mtime
                                except OSError:
                                    cmtime = 0.0
                                candidates.append((cmtime, cdata))
            except OSError:
                pass
            if candidates:
                candidates.sort(key=lambda x: x[0], reverse=True)
                data = candidates[0][1]
                try:
                    self._write_json_atomic(self.cluster_config_path, data, in_place_if_exists=True)
                except Exception:
                    pass
            else:
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
                and self._active_lock_epoch not in self._known_released_epochs
                and not os.path.exists(self.locks_dir / f"released-{self._active_lock_epoch}.json")
            ):
                try:
                    if os.stat(self.host_lock_path).st_mtime == self._cached_own_lock_mtime:
                        current = HostLock.from_dict(self._cached_own_lock.to_dict())
                except OSError:
                    current = None
            if current is None:
                current = self.get_lock()
            if not current.is_locked or current.machine_id != self.local_config.machine_id:
                return None
            if self._active_lock_epoch and current.lock_epoch and current.lock_epoch != self._active_lock_epoch:
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

    def ensure_runtime_synced_when_idle(self, strict: bool = False) -> None:
        """
        Ensures that before an idle machine reads or edits config files (like server.properties),
        if a newer or different snapshot/manifest exists in the Shared Cloud Folder (from another member's session),
        it pulls that snapshot first so we never clobber a newer world save!
        """
        with self._sync_mutex:
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

                manifest_changed = bool(manifest and manifest_seq > 0 and manifest_seq != self._last_synced_manifest_seq)
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

            # Move existing dynamic files into rollback staging before placing new files so mid-pull OSError never wipes world
            rollback_dir = self.base_data_dir / f".pull_rollback_{os.getpid()}_{threading.get_ident()}"
            if rollback_dir.exists():
                shutil.rmtree(rollback_dir, ignore_errors=True)
            rollback_dir.mkdir(parents=True, exist_ok=True)
            moved_to_rollback: List[Tuple[Path, Path]] = []
            try:
                for item in list(self.runtime_dir.iterdir()):
                    if item.name in preserved_entries:
                        continue
                    if item.name == "plugins" and item.is_dir():
                        rb_plugins = rollback_dir / "plugins"
                        rb_plugins.mkdir(parents=True, exist_ok=True)
                        for sub in list(item.iterdir()):
                            if sub.name == ".paper-remapped":
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
                            self._remove_path_with_retry(existing_sub)
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
            return (self.latest_snapshot_path if snap_exists else None), ""

        exp_hash = str(manifest.get("sha256", "")).strip().lower()
        exp_size = int(manifest.get("sizeBytes", -1) or -1)
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

            st = source_zip.stat()
            snap_mtime, snap_size = st.st_mtime, st.st_size
            snap_hash = (
                str(manifest.get("sha256", "") or "").lower()
                if manifest
                else self._sha256_file(source_zip).lower()
            )
            manifest_seq = int(manifest.get("snapshotSeq", 0)) if manifest else 0
            self._last_synced_snapshot_mtime = snap_mtime
            self._last_synced_snapshot_sig = (snap_mtime, snap_size, str(source_zip))
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
                "timestamp": time.time(),
            }

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
    ) -> Dict[str, Any]:
        """
        Packages `./data/runtime-server` (worlds, server.properties, ops, whitelist, plugins, configs)
        in local temp storage first, computes its SHA-256 manifest, writes optional backup,
        updates `snapshots/latest-server.zip` in-place (preserving Cloud File IDs), and updates `sync-manifest.json`.
        """
        with self._sync_mutex:
            self._verify_required_lock_epoch(require_lock_epoch)
            self._ensure_shared_dirs()
            self.runtime_dir.mkdir(parents=True, exist_ok=True)

            # Build zip inside local non-cloud data_dir so Google Drive never sees half-written temp zips
            tmp_zip = self.base_data_dir / f".push_stage.{os.getpid()}.{threading.get_ident()}.zip"
            stage_root = self.base_data_dir / f".push_files_{os.getpid()}_{threading.get_ident()}"
            excluded_top = {"cache", "libraries", "versions", "logs", "crash-reports"}
            excluded_files = {"server.jar", "jar-version.json", "session.lock"}

            def _iter_eligible_files() -> List[Tuple[Path, Path]]:
                items: List[Tuple[Path, Path]] = []
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
                        items.append((abs_file, abs_file.relative_to(self.runtime_dir)))
                return items

            file_count = 0
            backup_name = None
            try:
                eligible = _iter_eligible_files()
                zip_sources: List[Tuple[Path, Path]] = []

                if stage_copy_before_zip:
                    if stage_root.exists():
                        shutil.rmtree(stage_root, ignore_errors=True)
                    stage_root.mkdir(parents=True, exist_ok=True)
                    for abs_file, rel_file in eligible:
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
                        zip_sources.append((dst_file, rel_file))
                else:
                    zip_sources = eligible

                with zipfile.ZipFile(tmp_zip, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
                    for src_file, rel_file in zip_sources:
                        last_err = None
                        for attempt in range(4):
                            try:
                                zf.write(src_file, arcname=str(rel_file))
                                file_count += 1
                                last_err = None
                                break
                            except OSError as e:
                                last_err = e
                                time.sleep(0.08 * (attempt + 1))
                        if last_err is not None and strict_files:
                            raise RuntimeError(f"Cannot archive '{rel_file}' for snapshot: {last_err}")

                self._verify_required_lock_epoch(require_lock_epoch)
                sha256_hex = self._sha256_file(tmp_zip)
                staged_size = tmp_zip.stat().st_size

                if create_backup:
                    ts = time.strftime("%Y%m%d-%H%M%S")
                    safe_label = "".join(c for c in backup_label if c.isalnum() or c in ("-", "_")) or "backup"
                    backup_name = f"world-{ts}-{safe_label}.zip"
                    backup_target = self.backups_dir / backup_name
                    if backup_target.exists():
                        backup_name = f"world-{ts}-{int(time.time() * 1000) % 1000:03d}-{safe_label}.zip"
                        backup_target = self.backups_dir / backup_name
                    tmp_bk = self.backups_dir / f".{backup_name}.tmp.{os.getpid()}.{threading.get_ident()}"
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
                    self._prune_old_backups(protect_name=backup_name)

                self._copy_file_preserving_cloud_id(tmp_zip, self.latest_snapshot_path)
                try:
                    os.utime(self.latest_snapshot_path, None)
                except OSError:
                    pass

                st = self.latest_snapshot_path.stat()
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
                try:
                    if stage_root.exists():
                        shutil.rmtree(stage_root, ignore_errors=True)
                except OSError:
                    pass

            self._last_synced_snapshot_mtime = st.st_mtime
            self._last_synced_snapshot_sig = (st.st_mtime, st.st_size, str(self.latest_snapshot_path))
            self._save_sync_state(
                st.st_mtime,
                st.st_size,
                str(self.latest_snapshot_path),
                manifest_seq=int(manifest.get("snapshotSeq", 0)),
                sha256_hex=sha256_hex,
            )

            self.git_push_if_enabled(f"Sync world snapshot ({file_count} files)")
            return {
                "pushed": True,
                "fileCount": file_count,
                "sizeBytes": staged_size,
                "sha256": sha256_hex,
                "snapshotSeq": int(manifest.get("snapshotSeq", 0)),
                "backupFile": backup_name,
                "timestamp": time.time(),
            }

    def push_live_snapshot(
        self,
        lock_epoch: str = "",
        create_backup: bool = False,
        host_username: str = "",
    ) -> Dict[str, Any]:
        """
        Pushes a staged world snapshot while the server is actively running without dropping
        or modifying `host-lock.json` / `session-<epoch>.json`.
        Uses a bounded `_sync_mutex` acquire so periodic auto-sync never collides with stop/backup.
        """
        acquired = self._sync_mutex.acquire(timeout=2.0)
        if not acquired:
            return {"pushed": False, "skipped": True, "reason": "Another snapshot sync is already in progress."}
        try:
            eff_epoch = lock_epoch or self._active_lock_epoch
            if not eff_epoch or not self.owns_lock(eff_epoch):
                return {"pushed": False, "skipped": True, "reason": "Host lock is not held by this machine."}
            return self.push_world_from_runtime(
                create_backup=create_backup,
                backup_label=host_username or "autosync",
                host_username=host_username,
                require_lock_epoch=eff_epoch,
                strict_files=True,
                stage_copy_before_zip=True,
            )
        finally:
            self._sync_mutex.release()

    def list_backups(self) -> List[Dict[str, Any]]:
        self._ensure_shared_dirs(cleanup_conflicts=False)
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
            with self._lock_mutex:
                self.git_pull_if_enabled()
                current_lock = self.get_lock()
                if current_lock.is_actively_locked():
                    raise HostLockConflictError(
                        f"Cannot restore backup while '{current_lock.host_username}' is hosting on '{current_lock.machine_name}'. Stop the server first."
                    )

                self._ensure_shared_dirs()
                transfer_msg = self._has_active_cloud_transfer()
                if transfer_msg:
                    raise RuntimeError(f"{transfer_msg} Wait for cloud sync to finish before restoring a backup.")

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
                    sha256_hex=sha256_hex,
                )

                # Clean up any stale/empty unlocked host-lock.json while preserving expectedSnapshotSeq/Hash
                if not self.get_lock().is_actively_locked():
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
            manifest = self._read_sync_manifest()
            if manifest and Path(str(manifest.get("backupFile", "") or "")).name == safe_name:
                exp_hash = str(manifest.get("sha256", "") or "").lower()
                snap_ok = (
                    self.latest_snapshot_path.exists()
                    and exp_hash
                    and self._sha256_file(self.latest_snapshot_path).lower() == exp_hash
                )
                if not snap_ok:
                    raise RuntimeError(
                        f"Cannot delete '{safe_name}' while it is the active fallback for the latest cloud snapshot."
                    )
                manifest["backupFile"] = ""
                self._write_json_atomic(self.sync_manifest_path, manifest, in_place_if_exists=True)
            target.unlink()
            return True
        return False

    def _prune_old_backups(self, protect_name: str = "") -> None:
        cfg = self.load_cluster_config()
        max_b = cfg.max_backups if cfg else 10
        manifest = self._read_sync_manifest()
        manifest_bk = Path(str(manifest.get("backupFile", "") or "")).name if manifest else ""
        protected = {n for n in (protect_name, manifest_bk) if n}
        with_mtime: List[Tuple[float, Path]] = []
        for p in self.backups_dir.glob("*.zip"):
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
