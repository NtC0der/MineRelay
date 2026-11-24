import os
import re
import sys
import json
import time
import shutil
import hashlib
import tarfile
import zipfile
import platform
import threading
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Optional, Callable, Dict, List, Any, Tuple

from src.models import (
    APP_VERSION,
    GITHUB_RELEASES_API,
    GITHUB_RELEASES_URL,
    GITHUB_DOWNLOAD_URL,
)

ADOPTIUM_JRE_API_TMPL = (
    "https://api.adoptium.net/v3/binary/latest/{major}/ga/{os_name}/{arch}/jre/hotspot/normal/eclipse"
)
USER_AGENT = f"MineRelay/{APP_VERSION} (https://github.com/NtC0der/MineRelay; contact@minerelay.local)"


def _os_replace_retry(src: Path, dst: Path, attempts: int = 6) -> None:
    """Retries os.replace briefly to tolerate Windows Defender / AV file locks on freshly downloaded binaries."""
    last_err: Optional[Exception] = None
    for i in range(max(1, attempts)):
        try:
            os.replace(src, dst)
            return
        except (PermissionError, OSError) as e:
            last_err = e
            time.sleep(0.15 * (i + 1))
    if last_err is not None:
        raise last_err


def _is_valid_zip_archive(path: Path) -> bool:
    try:
        if not path.exists() or not path.is_file() or path.stat().st_size <= 0:
            return False
        with zipfile.ZipFile(path, "r") as zf:
            return len(zf.infolist()) > 0
    except Exception:
        return False


def _file_digest_hex(path: Path, algo: str) -> str:
    h = hashlib.new(algo)
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1048576), b""):
            h.update(chunk)
    return h.hexdigest().lower()


def _download_to_file_verified(
    url: str,
    dest_tmp: Path,
    expected_sha256: str = "",
    expected_sha1: str = "",
    timeout: int = 120,
    attempts: int = 3,
    progress_cb: Optional[Callable[[int, int, int], None]] = None,
) -> int:
    """Downloads URL to dest_tmp with retries, Content-Length completeness check, and optional digest verification."""
    exp256 = (expected_sha256 or "").strip().lower()
    exp1 = (expected_sha1 or "").strip().lower()
    last_err: Optional[Exception] = None

    for attempt in range(max(1, attempts)):
        try:
            try:
                if dest_tmp.exists():
                    dest_tmp.unlink(missing_ok=True)
            except OSError:
                pass
            req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw_cl = resp.headers.get("Content-Length")
                total = int(raw_cl) if raw_cl and str(raw_cl).strip().isdigit() else 0
                downloaded = 0
                with open(dest_tmp, "wb") as out:
                    while True:
                        chunk = resp.read(131072)
                        if not chunk:
                            break
                        out.write(chunk)
                        downloaded += len(chunk)
                        if progress_cb and total > 0:
                            pct = min(100, int((downloaded / total) * 100))
                            progress_cb(pct, downloaded, total)

            if total > 0 and downloaded != total:
                raise RuntimeError(
                    f"Incomplete download ({downloaded} of {total} bytes received)"
                )
            if downloaded <= 0:
                raise RuntimeError("Downloaded file is empty (0 bytes)")
            if len(exp256) == 64:
                actual256 = _file_digest_hex(dest_tmp, "sha256")
                if actual256 != exp256:
                    raise RuntimeError(f"SHA-256 mismatch (expected {exp256[:12]}..., got {actual256[:12]}...)")
            elif len(exp1) == 40:
                actual1 = _file_digest_hex(dest_tmp, "sha1")
                if actual1 != exp1:
                    raise RuntimeError(f"SHA-1 mismatch (expected {exp1[:12]}..., got {actual1[:12]}...)")
            return downloaded
        except Exception as e:
            last_err = e
            try:
                if dest_tmp.exists():
                    dest_tmp.unlink(missing_ok=True)
            except OSError:
                pass
            if attempt + 1 < max(1, attempts):
                time.sleep(0.8 * (2 ** attempt))

    raise RuntimeError(str(last_err or "Download failed"))


def parse_semver(version_str: str) -> Tuple[int, int, int, int, Tuple[Tuple[int, Any], ...]]:
    """
    Parses a version string (e.g. 'v1.10.2', '1.0.6-beta.1') into a comparable tuple:
    (major, minor, patch, is_stable_flag, prerelease_tokens).
    Stable releases compare greater than prereleases of the same core version.
    """
    clean = str(version_str or "").strip()
    if clean.lower().startswith("v"):
        clean = clean[1:].strip()
    clean = clean.split("+", 1)[0]
    if "-" in clean:
        core_part, pre_part = clean.split("-", 1)
        is_stable = 0
        pre_tokens: List[Tuple[int, Any]] = []
        for tok in re.split(r"[.\-_]+", pre_part.strip()):
            if not tok:
                continue
            if tok.isdigit():
                pre_tokens.append((0, int(tok)))
            else:
                pre_tokens.append((1, tok.lower()))
    else:
        core_part = clean
        is_stable = 1
        pre_tokens = []

    nums = [int(x) for x in re.findall(r"\d+", core_part)[:3]]
    while len(nums) < 3:
        nums.append(0)
    return (nums[0], nums[1], nums[2], is_stable, tuple(pre_tokens))


def is_version_older(current: str, latest: str) -> bool:
    c_str = str(current or "").strip()
    l_str = str(latest or "").strip()
    if not c_str or not l_str:
        return False
    if not re.search(r"\d", c_str) or not re.search(r"\d", l_str):
        return False
    return parse_semver(c_str) < parse_semver(l_str)


_RELEASE_CACHE: Optional[Dict[str, Any]] = None
_RELEASE_CACHE_TS: float = 0.0
_RELEASE_CACHE_URL: str = ""
_RELEASE_CACHE_LOCK = threading.Lock()
_RELEASE_CHECK_IN_FLIGHT = False
RELEASE_CACHE_TTL_SEC = 1800.0


def _build_version_check_payload(
    current_ver: str,
    gh_data: Optional[Dict[str, Any]],
    cluster_latest: str = "",
) -> Dict[str, Any]:
    gh_latest = str((gh_data or {}).get("latestVersion") or "").strip()
    c_latest = str(cluster_latest or "").strip()

    effective_latest = gh_latest
    source = "github" if gh_latest else "none"
    if c_latest and re.search(r"\d", c_latest):
        if not effective_latest or is_version_older(effective_latest, c_latest):
            effective_latest = c_latest
            source = "cluster"

    outdated = is_version_older(current_ver, effective_latest) if effective_latest else False
    if source == "cluster":
        release_url = GITHUB_RELEASES_URL
        download_url = GITHUB_DOWNLOAD_URL
        release_name = f"MineRelay v{effective_latest}"
        asset_name = "MineRelay.exe"
        asset_size = 0
        asset_sha256 = ""
    else:
        release_url = str((gh_data or {}).get("releaseUrl") or GITHUB_RELEASES_URL)
        download_url = str((gh_data or {}).get("downloadUrl") or GITHUB_DOWNLOAD_URL)
        release_name = str((gh_data or {}).get("releaseName") or (f"MineRelay v{effective_latest}" if effective_latest else ""))
        asset_name = str((gh_data or {}).get("assetName") or "MineRelay.exe")
        asset_size = int((gh_data or {}).get("assetSize") or 0)
        asset_sha256 = str((gh_data or {}).get("assetSha256") or "").strip().lower()

    return {
        "currentVersion": current_ver,
        "latestVersion": effective_latest,
        "isOutdated": outdated,
        "releaseUrl": release_url,
        "downloadUrl": download_url,
        "releaseName": release_name,
        "assetName": asset_name,
        "assetSize": asset_size,
        "assetSha256": asset_sha256,
        "checkedAt": float((gh_data or {}).get("checkedAt") or 0.0),
        "source": source,
        "error": str((gh_data or {}).get("error") or ""),
    }


def _fetch_latest_github_release_sync(api_url: str, current_ver: str) -> Dict[str, Any]:
    now = time.time()
    try:
        req = urllib.request.Request(
            api_url,
            headers={
                "User-Agent": f"MineRelay/{current_ver}",
                "Accept": "application/vnd.github+json",
            },
        )
        with urllib.request.urlopen(req, timeout=4.0) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("Unexpected GitHub release API payload")
        tag = str(payload.get("tag_name") or "").strip()
        latest_ver = tag[1:].strip() if tag.lower().startswith("v") else tag
        release_url = str(payload.get("html_url") or GITHUB_RELEASES_URL).strip() or GITHUB_RELEASES_URL
        release_name = str(payload.get("name") or tag or "").strip()
        download_url = GITHUB_DOWNLOAD_URL
        asset_name = "MineRelay.exe"
        asset_size = 0
        asset_sha256 = ""
        assets = payload.get("assets")
        if isinstance(assets, list):
            for asset in assets:
                if isinstance(asset, dict):
                    aname_raw = str(asset.get("name") or "").strip()
                    aname = aname_raw.lower()
                    burl = str(asset.get("browser_download_url") or "").strip()
                    if burl and (aname.endswith(".exe") or "minerelay" in aname):
                        download_url = burl
                        asset_name = aname_raw or "MineRelay.exe"
                        try:
                            asset_size = max(0, int(asset.get("size") or 0))
                        except (ValueError, TypeError):
                            asset_size = 0
                        raw_digest = str(asset.get("digest") or "").strip().lower()
                        if raw_digest.startswith("sha256:"):
                            cand_hex = raw_digest.split(":", 1)[1].strip()
                            if len(cand_hex) == 64 and all(c in "0123456789abcdef" for c in cand_hex):
                                asset_sha256 = cand_hex
                        break
        return {
            "latestVersion": latest_ver,
            "releaseUrl": release_url,
            "downloadUrl": download_url,
            "releaseName": release_name,
            "assetName": asset_name,
            "assetSize": asset_size,
            "assetSha256": asset_sha256,
            "checkedAt": now,
            "error": "",
        }
    except Exception as e:
        return {
            "latestVersion": "",
            "releaseUrl": GITHUB_RELEASES_URL,
            "downloadUrl": GITHUB_DOWNLOAD_URL,
            "releaseName": "",
            "assetName": "MineRelay.exe",
            "assetSize": 0,
            "assetSha256": "",
            "checkedAt": now,
            "error": str(e),
        }


def check_latest_minerelay_release(force: bool = False, cluster_latest: str = "") -> Dict[str, Any]:
    """
    Non-blocking cached check against GitHub Releases (latest).
    Respects MINERELAY_APP_VERSION and MINERELAY_RELEASES_API_URL overrides.
    Skips live GitHub calls when MINERELAY_TEST_MODE=1 unless MINERELAY_RELEASES_API_URL is set.
    """
    global _RELEASE_CACHE, _RELEASE_CACHE_TS, _RELEASE_CACHE_URL, _RELEASE_CHECK_IN_FLIGHT

    current_ver = os.environ.get("MINERELAY_APP_VERSION", "").strip() or APP_VERSION
    custom_api = os.environ.get("MINERELAY_RELEASES_API_URL", "").strip()
    api_url = custom_api or GITHUB_RELEASES_API
    skip_network = (os.environ.get("MINERELAY_TEST_MODE") == "1") and not custom_api

    if skip_network:
        return _build_version_check_payload(current_ver, None, cluster_latest=cluster_latest)

    now = time.time()
    with _RELEASE_CACHE_LOCK:
        cache_valid = (
            _RELEASE_CACHE is not None
            and _RELEASE_CACHE_URL == api_url
            and (now - _RELEASE_CACHE_TS) < RELEASE_CACHE_TTL_SEC
        )
        if cache_valid and not force:
            return _build_version_check_payload(current_ver, _RELEASE_CACHE, cluster_latest=cluster_latest)
        cached_snap = dict(_RELEASE_CACHE) if (_RELEASE_CACHE and _RELEASE_CACHE_URL == api_url) else None

    if force or (custom_api and cached_snap is None):
        fresh = _fetch_latest_github_release_sync(api_url, current_ver)
        with _RELEASE_CACHE_LOCK:
            if fresh.get("latestVersion") or _RELEASE_CACHE is None or _RELEASE_CACHE_URL != api_url:
                _RELEASE_CACHE = fresh
                _RELEASE_CACHE_TS = time.time()
                _RELEASE_CACHE_URL = api_url
            elif cached_snap and cached_snap.get("latestVersion"):
                # Keep previous known latestVersion on transient offline refresh failure, attach latest error
                merged = dict(cached_snap)
                merged["checkedAt"] = fresh["checkedAt"]
                merged["error"] = fresh.get("error", "")
                _RELEASE_CACHE = merged
                _RELEASE_CACHE_TS = time.time()
                fresh = merged
        return _build_version_check_payload(current_ver, fresh, cluster_latest=cluster_latest)

    # Trigger background refresh so HTTP endpoints stay non-blocking
    with _RELEASE_CACHE_LOCK:
        should_spawn = not _RELEASE_CHECK_IN_FLIGHT
        if should_spawn:
            _RELEASE_CHECK_IN_FLIGHT = True

    if should_spawn:
        def _bg_refresh() -> None:
            global _RELEASE_CACHE, _RELEASE_CACHE_TS, _RELEASE_CACHE_URL, _RELEASE_CHECK_IN_FLIGHT
            try:
                fresh = _fetch_latest_github_release_sync(api_url, current_ver)
                with _RELEASE_CACHE_LOCK:
                    prev = _RELEASE_CACHE if _RELEASE_CACHE_URL == api_url else None
                    if fresh.get("latestVersion") or prev is None:
                        _RELEASE_CACHE = fresh
                    elif prev and prev.get("latestVersion"):
                        merged = dict(prev)
                        merged["checkedAt"] = fresh["checkedAt"]
                        merged["error"] = fresh.get("error", "")
                        _RELEASE_CACHE = merged
                    _RELEASE_CACHE_TS = time.time()
                    _RELEASE_CACHE_URL = api_url
            finally:
                with _RELEASE_CACHE_LOCK:
                    _RELEASE_CHECK_IN_FLIGHT = False

        threading.Thread(target=_bg_refresh, daemon=True, name="ReleaseVersionCheck").start()

    return _build_version_check_payload(current_ver, cached_snap, cluster_latest=cluster_latest)


class AutoUpdateManager:
    """
    Background GitHub release binary auto-downloader & safe Windows self-staging manager.
    Downloads verified MineRelay-v<ver>.exe binaries into `<data>/updates/` and stages atomic
    executable replacement when running frozen on Windows without interrupting active hosting.
    """

    ERROR_COOLDOWN_SEC = 120.0

    def __init__(self, base_data_dir: Path, event_callback: Optional[Callable[[str, Any], None]] = None):
        self.base_data_dir = Path(base_data_dir).resolve()
        self.updates_dir = self.base_data_dir / "updates"
        self.updates_dir.mkdir(parents=True, exist_ok=True)
        self.event_callback = event_callback
        self._lock = threading.RLock()
        self._last_emit_ts: float = 0.0
        self._last_emit_pct: int = -1
        self._last_error_at: float = 0.0
        self._last_error_ver: str = ""
        self._staged_version: str = ""
        self._state: Dict[str, Any] = {
            "enabled": self.is_auto_download_allowed(),
            "status": "idle",  # idle | downloading | downloaded | ready_to_restart | error
            "targetVersion": "",
            "progress": 0,
            "downloadedBytes": 0,
            "totalBytes": 0,
            "downloadedPath": "",
            "sha256": "",
            "stagedToExe": False,
            "canRestartApply": bool(getattr(sys, "frozen", False) and sys.platform.startswith("win")),
            "error": "",
            "updatedAt": 0.0,
        }
        self._cleanup_old_exe_leftovers()

    @staticmethod
    def _cleanup_old_exe_leftovers() -> None:
        if getattr(sys, "frozen", False) and sys.platform.startswith("win"):
            try:
                cur_exe = Path(sys.executable).resolve()
                old_exe = cur_exe.parent / f"{cur_exe.stem}.old{cur_exe.suffix}"
                if old_exe.exists():
                    old_exe.unlink(missing_ok=True)
            except OSError:
                pass

    @staticmethod
    def is_auto_download_allowed() -> bool:
        if os.environ.get("MINERELAY_AUTO_DOWNLOAD_UPDATES", "").strip() == "0":
            return False
        if os.environ.get("MINERELAY_TEST_MODE") == "1":
            return os.environ.get("MINERELAY_AUTO_UPDATE_TEST", "").strip() == "1"
        return True

    def get_state(self) -> Dict[str, Any]:
        with self._lock:
            snap = dict(self._state)
            snap["enabled"] = self.is_auto_download_allowed()
            return snap

    def _emit_state(self, force: bool = False) -> None:
        with self._lock:
            now = time.monotonic()
            pct = int(self._state.get("progress") or 0)
            if not force and abs(pct - self._last_emit_pct) < 4 and (now - self._last_emit_ts) < 0.45:
                return
            self._last_emit_ts = now
            self._last_emit_pct = pct
            snap = dict(self._state)
            snap["enabled"] = self.is_auto_download_allowed()
        if self.event_callback:
            try:
                self.event_callback("update_status", snap)
            except Exception:
                pass

    @staticmethod
    def _verify_downloaded_exe(path: Path, expected_size: int = 0, expected_sha256: str = "") -> str:
        if not path.exists() or not path.is_file():
            raise RuntimeError("Downloaded update binary is missing.")
        st_size = path.stat().st_size
        min_bytes = 64 if (os.environ.get("MINERELAY_TEST_MODE") == "1" or os.environ.get("MINERELAY_AUTO_UPDATE_TEST") == "1") else 65536
        if st_size < min_bytes:
            raise RuntimeError(f"Downloaded update binary is unexpectedly small ({st_size} bytes).")
        if expected_size > 0 and st_size != expected_size:
            raise RuntimeError(f"Downloaded update size mismatch (expected {expected_size} bytes, got {st_size}).")
        if sys.platform.startswith("win") or path.suffix.lower() == ".exe":
            with open(path, "rb") as f:
                magic = f.read(2)
            if magic != b"MZ":
                raise RuntimeError("Downloaded file is not a valid Windows PE (.exe) binary.")
        actual_sha = _file_digest_hex(path, "sha256")
        exp_sha = (expected_sha256 or "").strip().lower()
        if len(exp_sha) == 64 and actual_sha != exp_sha:
            raise RuntimeError(f"Downloaded update SHA-256 mismatch (expected {exp_sha[:12]}..., got {actual_sha[:12]}...).")
        return actual_sha

    def _try_stage_frozen_exe(self, downloaded_file: Path, target_exe_override: Optional[Path] = None) -> bool:
        if target_exe_override is None:
            if not (getattr(sys, "frozen", False) and sys.platform.startswith("win")):
                return False
            target_exe = Path(sys.executable).resolve()
        else:
            target_exe = Path(target_exe_override).resolve()
        if not sys.platform.startswith("win") or not target_exe.exists():
            return False
        tmp_staged = target_exe.parent / f".{target_exe.name}.update.{os.getpid()}.tmp"
        old_exe = target_exe.parent / f"{target_exe.stem}.old{target_exe.suffix}"
        try:
            shutil.copyfile(downloaded_file, tmp_staged)
            try:
                if old_exe.exists():
                    old_exe.unlink(missing_ok=True)
            except OSError:
                pass
            target_exe.replace(old_exe)
            try:
                tmp_staged.replace(target_exe)
                return True
            except Exception:
                try:
                    if not target_exe.exists() and old_exe.exists():
                        old_exe.replace(target_exe)
                except OSError:
                    pass
                return False
        except OSError:
            return False
        finally:
            try:
                if tmp_staged.exists():
                    tmp_staged.unlink(missing_ok=True)
            except OSError:
                pass

    def _prune_old_updates(self, keep_name: str = "") -> None:
        try:
            candidates = sorted(
                self.updates_dir.glob("MineRelay-v*.exe"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            kept = 0
            for p in candidates:
                if keep_name and p.name == keep_name:
                    kept += 1
                    continue
                if kept < 2:
                    kept += 1
                    continue
                try:
                    p.unlink(missing_ok=True)
                except OSError:
                    pass
        except OSError:
            pass

    def get_ready_update_path(self) -> Optional[Path]:
        with self._lock:
            p_str = str(self._state.get("downloadedPath") or "").strip()
            status = str(self._state.get("status") or "")
        if not p_str or status not in ("downloaded", "ready_to_restart"):
            return None
        p = Path(p_str)
        return p if (p.exists() and p.is_file()) else None

    def ensure_update_downloaded_async(self, version_check: Dict[str, Any], force: bool = False) -> Dict[str, Any]:
        is_outdated = bool(version_check.get("isOutdated"))
        latest_ver = str(version_check.get("latestVersion") or "").strip()
        dl_url = str(version_check.get("downloadUrl") or "").strip()
        exp_size = int(version_check.get("assetSize") or 0)
        exp_sha256 = str(version_check.get("assetSha256") or "").strip().lower()

        if (not is_outdated) or (not latest_ver) or (not re.match(r"^https?://", dl_url, re.IGNORECASE)):
            with self._lock:
                if self._state.get("status") not in ("downloading",):
                    self._state.update({
                        "status": "idle",
                        "targetVersion": "",
                        "progress": 0,
                        "downloadedBytes": 0,
                        "totalBytes": 0,
                        "downloadedPath": "",
                        "sha256": "",
                        "stagedToExe": False,
                        "error": "",
                        "updatedAt": time.time(),
                    })
            return self.get_state()

        if not force and not self.is_auto_download_allowed():
            return self.get_state()

        safe_ver = "".join(c for c in latest_ver if c.isalnum() or c in (".", "-", "_")) or "latest"
        target_file = self.updates_dir / f"MineRelay-v{safe_ver}.exe"

        with self._lock:
            if self._state.get("status") == "downloading" and self._state.get("targetVersion") == latest_ver:
                return self.get_state()
            if (
                not force
                and self._state.get("status") == "error"
                and self._last_error_ver == latest_ver
                and (time.time() - self._last_error_at) < self.ERROR_COOLDOWN_SEC
            ):
                return self.get_state()

        if target_file.exists():
            try:
                verified_sha = self._verify_downloaded_exe(target_file, expected_size=exp_size, expected_sha256=exp_sha256)
                with self._lock:
                    staged = bool(self._state.get("stagedToExe") and self._staged_version == latest_ver)
                if not staged and getattr(sys, "frozen", False) and sys.platform.startswith("win"):
                    staged = self._try_stage_frozen_exe(target_file)
                    if staged:
                        with self._lock:
                            self._staged_version = latest_ver
                sz = target_file.stat().st_size
                with self._lock:
                    self._state.update({
                        "status": "ready_to_restart" if staged else "downloaded",
                        "targetVersion": latest_ver,
                        "progress": 100,
                        "downloadedBytes": sz,
                        "totalBytes": sz,
                        "downloadedPath": str(target_file),
                        "sha256": verified_sha,
                        "stagedToExe": staged,
                        "canRestartApply": bool(getattr(sys, "frozen", False) and sys.platform.startswith("win")),
                        "error": "",
                        "updatedAt": time.time(),
                    })
                self._emit_state(force=True)
                return self.get_state()
            except Exception:
                try:
                    target_file.unlink(missing_ok=True)
                except OSError:
                    pass

        with self._lock:
            self._last_emit_pct = -1
            self._state.update({
                "status": "downloading",
                "targetVersion": latest_ver,
                "progress": 0,
                "downloadedBytes": 0,
                "totalBytes": exp_size,
                "downloadedPath": "",
                "sha256": "",
                "stagedToExe": False,
                "canRestartApply": bool(getattr(sys, "frozen", False) and sys.platform.startswith("win")),
                "error": "",
                "updatedAt": time.time(),
            })
        self._emit_state(force=True)

        def _worker() -> None:
            tmp_part = self.updates_dir / f".MineRelay-v{safe_ver}.part.{os.getpid()}.{threading.get_ident()}"
            try:
                self.updates_dir.mkdir(parents=True, exist_ok=True)

                def _on_dl_prog(pct: int, done_b: int, total_b: int) -> None:
                    with self._lock:
                        self._state.update({
                            "status": "downloading",
                            "progress": max(0, min(99, int(pct))),
                            "downloadedBytes": int(done_b),
                            "totalBytes": int(total_b or exp_size),
                            "updatedAt": time.time(),
                        })
                    self._emit_state(force=False)

                _download_to_file_verified(
                    dl_url,
                    tmp_part,
                    expected_sha256=exp_sha256,
                    timeout=120,
                    attempts=3,
                    progress_cb=_on_dl_prog,
                )
                verified_sha = self._verify_downloaded_exe(
                    tmp_part,
                    expected_size=exp_size,
                    expected_sha256=exp_sha256,
                )
                _os_replace_retry(tmp_part, target_file)
                self._prune_old_updates(keep_name=target_file.name)

                staged = self._try_stage_frozen_exe(target_file)
                if staged:
                    with self._lock:
                        self._staged_version = latest_ver
                final_sz = target_file.stat().st_size
                with self._lock:
                    self._last_error_at = 0.0
                    self._last_error_ver = ""
                    self._state.update({
                        "status": "ready_to_restart" if staged else "downloaded",
                        "targetVersion": latest_ver,
                        "progress": 100,
                        "downloadedBytes": final_sz,
                        "totalBytes": final_sz,
                        "downloadedPath": str(target_file),
                        "sha256": verified_sha,
                        "stagedToExe": staged,
                        "canRestartApply": bool(getattr(sys, "frozen", False) and sys.platform.startswith("win")),
                        "error": "",
                        "updatedAt": time.time(),
                    })
                self._emit_state(force=True)
            except Exception as e:
                with self._lock:
                    self._last_error_at = time.time()
                    self._last_error_ver = latest_ver
                    self._state.update({
                        "status": "error",
                        "targetVersion": latest_ver,
                        "progress": 0,
                        "error": str(e),
                        "updatedAt": time.time(),
                    })
                self._emit_state(force=True)
            finally:
                try:
                    if tmp_part.exists():
                        tmp_part.unlink(missing_ok=True)
                except OSError:
                    pass

        threading.Thread(target=_worker, daemon=True, name="AutoUpdateDownloader").start()
        return self.get_state()

    @staticmethod
    def spawn_windows_relaunch_helper(
        target_exe: Path,
        downloaded_update: Path,
        staged_already: bool = False,
        extra_args: Optional[List[str]] = None,
    ) -> None:
        if not sys.platform.startswith("win"):
            raise RuntimeError("Self-replace relaunch is only supported on Windows executables.")
        cur_pid = os.getpid()
        tgt_esc = str(Path(target_exe).resolve()).replace("'", "''")
        upd_esc = str(Path(downloaded_update).resolve()).replace("'", "''")
        args_list = [str(a).replace("'", "''") for a in (extra_args or [])]
        arg_clause = ""
        if args_list:
            joined = ", ".join(f"'{a}'" for a in args_list)
            arg_clause = f" -ArgumentList @({joined})"
        copy_block = (
            ""
            if staged_already
            else f"Copy-Item -LiteralPath '{upd_esc}' -Destination '{tgt_esc}' -Force; "
        )
        ps_script = (
            f"$p = {cur_pid}; "
            f"for ($i = 0; $i -lt 80; $i++) {{ "
            f"if (-not (Get-Process -Id $p -ErrorAction SilentlyContinue)) {{ break }}; "
            f"Start-Sleep -Milliseconds 250 }}; "
            f"try {{ {copy_block}"
            f"Start-Process -FilePath '{tgt_esc}'{arg_clause} }} catch {{}}"
        )
        flags = (
            getattr(subprocess, "CREATE_NO_WINDOW", 0)
            | getattr(subprocess, "DETACHED_PROCESS", 0)
            | getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        )
        subprocess.Popen(
            ["powershell", "-NoProfile", "-WindowStyle", "Hidden", "-Command", ps_script],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=flags,
            close_fds=True,
        )


def get_java_major_version(java_cmd: str) -> int:
    """Returns the major version of a Java binary (e.g. 25, 21, 17), or 0 if invalid."""
    try:
        creationflags = subprocess.CREATE_NO_WINDOW if sys.platform.startswith("win") else 0
        res = subprocess.run(
            [java_cmd, "-version"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=5,
            creationflags=creationflags,
        )
        output = (res.stderr or res.stdout or "").strip()
        m = re.search(r'version "(\d+)', output)
        if m:
            return int(m.group(1))
    except Exception:
        pass
    return 0


def required_java_major_for_mc(mc_version: str) -> int:
    """
    Minecraft 26.1+ (including 26.3, 27.x, 28.x...) requires Java 25 (class file version 69.0).
    Minecraft 1.20.5 - 1.21.11 requires Java 21.
    Java 25 is backwards-compatible and can run both 26.x and 1.21.x!
    """
    ver = (mc_version or "").strip()
    m = re.match(r"^\s*(\d+)", ver)
    if m and int(m.group(1)) >= 26:
        return 25
    return 21


_JRE_INSTALL_LOCK = threading.Lock()


def find_portable_java(bin_dir: Path, min_major: int = 21) -> Optional[str]:
    """
    Looks for a verified portable java.exe inside data/bin/jre25 or data/bin/jre21
    that satisfies `min_major`. Always prefers Java 25 if present.
    """
    exe_name = "java.exe" if sys.platform.startswith("win") else "java"
    for folder_name, major in [("jre25", 25), ("jre21", 21)]:
        if major < min_major:
            continue
        jre_root = bin_dir / folder_name
        if not jre_root.exists():
            continue
        for candidate in jre_root.rglob(exe_name):
            if candidate.parent.name == "bin":
                resolved = str(candidate.resolve())
                if get_java_major_version(resolved) >= min_major:
                    return resolved
    return None


def ensure_portable_java(
    bin_dir: Path,
    min_major: int = 25,
    log_fn: Optional[Callable[[str], None]] = None,
) -> str:
    """
    Ensures a Portable Java Runtime (>= min_major, defaulting to Java 25 for MC 26.3 support)
    is available inside data/bin/jre25. Serialized via _JRE_INSTALL_LOCK.
    """
    with _JRE_INSTALL_LOCK:
        existing = find_portable_java(bin_dir, min_major=min_major)
        if existing:
            if log_fn:
                log_fn(f"Portable Java Runtime ready at: {existing}")
            return existing

        target_major = 25 if min_major >= 22 else 21
        os_name = "windows" if sys.platform.startswith("win") else ("mac" if sys.platform == "darwin" else "linux")
        machine = platform.machine().lower()
        arch = "aarch64" if machine in ("arm64", "aarch64") else "x64"
        api_url = ADOPTIUM_JRE_API_TMPL.format(major=target_major, os_name=os_name, arch=arch)
        folder_name = f"jre{target_major}"

        bin_dir.mkdir(parents=True, exist_ok=True)
        jre_root = bin_dir / folder_name
        ext = ".zip" if os_name == "windows" else ".tar.gz"
        archive_path = bin_dir / f"temurin{target_major}-jre.{os.getpid()}.{threading.get_ident()}{ext}"
        staging_root = bin_dir / f".jre{target_major}_staging_{os.getpid()}_{threading.get_ident()}"

        if log_fn:
            log_fn(
                f"Downloading Portable Java {target_major} Runtime (Eclipse Temurin {target_major} JRE for Minecraft)... (~53 MB)"
            )

        last_pct = [-1]

        def _on_progress(pct: int, downloaded: int, total: int) -> None:
            if pct // 20 > last_pct[0] // 20 and log_fn:
                last_pct[0] = pct
                log_fn(
                    f"Downloading Portable Java {target_major} JRE: {pct}% ({round(downloaded / (1024*1024), 1)} MB)"
                )

        try:
            _download_to_file_verified(
                api_url,
                archive_path,
                timeout=120,
                attempts=3,
                progress_cb=_on_progress,
            )

            if log_fn:
                log_fn(f"Extracting Portable Java {target_major} JRE...")
            if staging_root.exists():
                shutil.rmtree(staging_root, ignore_errors=True)
            staging_root.mkdir(parents=True, exist_ok=True)
            staging_resolved = staging_root.resolve()
            if os_name == "windows":
                with zipfile.ZipFile(archive_path, "r") as zf:
                    for member in zf.infolist():
                        target_path = (staging_root / member.filename).resolve()
                        if not (target_path == staging_resolved or target_path.is_relative_to(staging_resolved)):
                            raise ValueError(f"Zip path traversal detected in JRE archive: {member.filename}")
                        zf.extract(member, staging_root)
            else:
                with tarfile.open(archive_path, "r:gz") as tf:
                    for member in tf.getmembers():
                        target_path = (staging_root / member.name).resolve()
                        if not (target_path == staging_resolved or target_path.is_relative_to(staging_resolved)):
                            raise ValueError(f"Tar path traversal detected in JRE archive: {member.name}")
                        if member.issym():
                            link_target = (target_path.parent / member.linkname).resolve()
                            if not link_target.is_relative_to(staging_resolved):
                                raise ValueError(f"Unsafe symlink in JRE archive: {member.name}")
                        elif member.islnk():
                            link_target = (staging_root / member.linkname).resolve()
                            if not link_target.is_relative_to(staging_resolved):
                                raise ValueError(f"Unsafe hardlink in JRE archive: {member.name}")
                    tf.extractall(staging_root)

            exe_name = "java.exe" if sys.platform.startswith("win") else "java"
            staged_java = next(
                (c for c in staging_root.rglob(exe_name) if c.parent.name == "bin"),
                None,
            )
            if not staged_java:
                raise RuntimeError(f"Downloaded Java {target_major} archive but could not locate bin/{exe_name} inside.")

            if jre_root.exists():
                shutil.rmtree(jre_root, ignore_errors=True)
            _os_replace_retry(staging_root, jre_root)
        finally:
            try:
                if archive_path.exists():
                    archive_path.unlink(missing_ok=True)
            except OSError:
                pass
            try:
                if staging_root.exists():
                    shutil.rmtree(staging_root, ignore_errors=True)
            except OSError:
                pass

        found = find_portable_java(bin_dir, min_major=min_major)
        if not found:
            raise RuntimeError(f"Downloaded Java {target_major} archive failed verification or could not locate bin/{exe_name}.")
        if log_fn:
            log_fn(f"Portable Java {target_major} JRE ready at: {found}")
        return found


_VERSIONS_CACHE: Optional[Dict[str, Any]] = None
_VERSIONS_CACHE_TS: float = 0.0
_VERSIONS_CACHE_FAIL_TS: float = 0.0
_VERSIONS_CACHE_LOCK = threading.Lock()


def fetch_live_minecraft_versions() -> Dict[str, Any]:
    """
    Queries Mojang's official version_manifest_v2.json and PaperMC's Fill v3 API
    to return the latest release version and supported version lists.
    Caches live results for 10 minutes and offline failures for 45 seconds.
    Falls back to a curated list including 26.3 if offline.
    """
    global _VERSIONS_CACHE, _VERSIONS_CACHE_TS, _VERSIONS_CACHE_FAIL_TS
    import time as _time

    fallback_releases = [
        "26.3",
        "26.2",
        "26.1.2",
        "26.1.1",
        "26.1",
        "1.21.11",
        "1.21.10",
        "1.21.8",
        "1.21.5",
        "1.21.4",
        "1.21.3",
        "1.21.1",
        "1.20.6",
        "1.20.4",
        "1.20.1",
        "1.19.4",
        "1.18.2",
    ]
    result: Dict[str, Any] = {
        "latestRelease": "26.3",
        "vanilla": list(fallback_releases),
        "paper": list(fallback_releases),
    }

    if os.environ.get("MINERELAY_TEST_MODE") == "1":
        return result

    now = _time.time()
    with _VERSIONS_CACHE_LOCK:
        if _VERSIONS_CACHE is not None and (now - _VERSIONS_CACHE_TS) < 600:
            return dict(_VERSIONS_CACHE)
        if _VERSIONS_CACHE_FAIL_TS > 0 and (now - _VERSIONS_CACHE_FAIL_TS) < 45:
            return dict(result)

    mojang_ok = False
    paper_ok = False

    # 1. Query Mojang official manifest
    try:
        req = urllib.request.Request(
            "https://piston-meta.mojang.com/mc/game/version_manifest_v2.json",
            headers={"User-Agent": USER_AGENT},
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            manifest = json.loads(resp.read().decode("utf-8"))
        latest_rel = manifest.get("latest", {}).get("release")
        if latest_rel:
            result["latestRelease"] = latest_rel
        releases = [
            v["id"]
            for v in manifest.get("versions", [])
            if v.get("type") == "release"
        ][:25]
        if releases:
            result["vanilla"] = releases
            mojang_ok = True
    except Exception:
        pass

    # 2. Query PaperMC Fill v3 API
    try:
        req = urllib.request.Request(
            "https://fill.papermc.io/v3/projects/paper",
            headers={"User-Agent": USER_AGENT},
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            paper_data = json.loads(resp.read().decode("utf-8"))
        versions_map = paper_data.get("versions", {})
        paper_list: List[str] = []
        if isinstance(versions_map, dict):
            for group_versions in versions_map.values():
                if isinstance(group_versions, list):
                    for v in group_versions:
                        if isinstance(v, str) and "-" not in v and v not in paper_list:
                            paper_list.append(v)
        elif isinstance(versions_map, list):
            paper_list = [str(v) for v in versions_map if "-" not in str(v)]
        if paper_list:
            # Prefer newest-first ordering using Mojang release order, falling back to descending semver tuple
            order_idx = {v: i for i, v in enumerate(result["vanilla"])} if (mojang_ok and result["vanilla"]) else {}

            def _paper_sort_key(ver_str: str) -> Tuple[Any, ...]:
                if ver_str in order_idx:
                    return (0, order_idx[ver_str], (0, 0, 0))
                sv = parse_semver(ver_str)[:3]
                return (1, 0, (-sv[0], -sv[1], -sv[2]))

            paper_list.sort(key=_paper_sort_key)
            result["paper"] = paper_list[:25]
            paper_ok = True
    except Exception:
        pass

    with _VERSIONS_CACHE_LOCK:
        if mojang_ok or paper_ok:
            _VERSIONS_CACHE = dict(result)
            _VERSIONS_CACHE_TS = now
            _VERSIONS_CACHE_FAIL_TS = 0.0
        else:
            _VERSIONS_CACHE_FAIL_TS = now
    return result


def _resolve_mojang_server_with_sha1(version: str) -> Tuple[str, str, str]:
    """Returns (download_url, resolved_version, sha1_hex) from Mojang's official manifest."""
    manifest_url = "https://piston-meta.mojang.com/mc/game/version_manifest_v2.json"
    req = urllib.request.Request(manifest_url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=20) as resp:
        manifest = json.loads(resp.read().decode("utf-8"))
    versions = manifest.get("versions", [])
    if not versions:
        raise RuntimeError("Mojang version manifest returned no versions.")
    ver_url = next((v["url"] for v in versions if v.get("id") == version), None)
    if not ver_url:
        raise RuntimeError(
            f"Minecraft version '{version}' was not found in Mojang's official release manifest. "
            "Check Server Config and choose an exact supported version."
        )
    with urllib.request.urlopen(urllib.request.Request(ver_url, headers={"User-Agent": USER_AGENT}), timeout=20) as resp:
        ver_data = json.loads(resp.read().decode("utf-8"))
    srv_info = ver_data["downloads"]["server"]
    return srv_info["url"], version, str(srv_info.get("sha1") or "").strip().lower()


def resolve_mojang_server_url(version: str) -> Tuple[str, str]:
    """Returns (download_url, resolved_version) from Mojang's official manifest."""
    url, resolved_ver, _ = _resolve_mojang_server_with_sha1(version)
    return url, resolved_ver


def _resolve_papermc_or_mojang_with_checksum(
    software: str,
    version: str,
    log_fn: Optional[Callable[[str], None]] = None,
) -> Tuple[str, str, str, str, str]:
    """
    Returns (download_url, resolved_software, resolved_version, sha256_hex, sha1_hex).
    Never silently substitutes Vanilla for Paper so existing Paper worlds and plugins cannot be corrupted.
    """
    headers = {"User-Agent": USER_AGENT}
    if software == "paper":
        quoted_ver = urllib.parse.quote(version.strip(), safe="")
        builds_url = f"https://fill.papermc.io/v3/projects/paper/versions/{quoted_ver}/builds"
        try:
            req = urllib.request.Request(builds_url, headers=headers)
            with urllib.request.urlopen(req, timeout=15) as resp:
                builds_data = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 404:
                raise RuntimeError(
                    f"PaperMC does not have a published build for Minecraft '{version}'. "
                    "Choose another Paper version or explicitly switch Server Software to Vanilla in Server Config."
                ) from e
            raise RuntimeError(f"Failed to query PaperMC builds for Minecraft {version} (HTTP {e.code}).") from e
        except Exception as e:
            raise RuntimeError(f"Failed to query PaperMC builds for Minecraft {version}: {e}") from e

        if isinstance(builds_data, list) and len(builds_data) > 0:
            sorted_builds = sorted(builds_data, key=lambda b: int(b.get("id", 0)), reverse=True)
            chosen = next(
                (b for b in sorted_builds if b.get("channel") in ("STABLE", "RECOMMENDED")),
                sorted_builds[0],
            )
            dl = chosen.get("downloads", {})
            for key in ("server:default", "application"):
                if key in dl and "url" in dl[key]:
                    checksums = dl[key].get("checksums") if isinstance(dl[key], dict) else None
                    sha256_hex = str((checksums or {}).get("sha256") or "").strip().lower()
                    return dl[key]["url"], "paper", version, sha256_hex, ""
        raise RuntimeError(
            f"PaperMC returned no downloadable server builds for Minecraft '{version}'. "
            "Select a supported version or explicitly switch Server Software to Vanilla."
        )

    url, resolved_ver, sha1_hex = _resolve_mojang_server_with_sha1(version)
    return url, "vanilla", resolved_ver, "", sha1_hex


def resolve_papermc_or_mojang_url(
    software: str,
    version: str,
    log_fn: Optional[Callable[[str], None]] = None,
) -> Tuple[str, str, str]:
    """
    Returns (download_url, resolved_software, resolved_version).
    """
    url, resolved_sw, resolved_ver, _, _ = _resolve_papermc_or_mojang_with_checksum(
        software, version, log_fn=log_fn
    )
    return url, resolved_sw, resolved_ver


def get_versioned_jar_path(bin_dir: Path, software: str, version: str) -> Path:
    jars_dir = bin_dir / "jars"
    jars_dir.mkdir(parents=True, exist_ok=True)
    safe_sw = "".join(c for c in software.lower().strip() if c.isalnum()) or "paper"
    safe_ver = "".join(c for c in version.strip() if c.isalnum() or c in (".", "-")) or "26.3"
    return jars_dir / f"server-{safe_sw}-{safe_ver}.jar"


def list_installed_jars(bin_dir: Path) -> List[Dict[str, Any]]:
    jars_dir = bin_dir / "jars"
    if not jars_dir.exists():
        return []
    items: List[Tuple[float, Dict[str, Any]]] = []
    for f in jars_dir.glob("server-*.jar"):
        try:
            st = f.stat()
            items.append((
                st.st_mtime,
                {
                    "name": f.name,
                    "path": str(f),
                    "sizeMb": round(st.st_size / (1024 * 1024), 2),
                },
            ))
        except OSError:
            continue
    items.sort(key=lambda pair: pair[0], reverse=True)
    return [entry for _, entry in items]


def ensure_paperclip_mojang_cache(
    jar_path: Path,
    runtime_dir: Path,
    log_fn: Optional[Callable[[str], None]] = None,
) -> None:
    """
    PaperMC jars use Paperclip, which reads META-INF/download-context and downloads
    the official Mojang vanilla server jar into runtime_dir/cache/mojang_<ver>.jar on first boot.
    Pre-caching this file with progress logs prevents a silent 55 MB wait during startup.
    """
    if os.environ.get("MINERELAY_TEST_MODE") == "1" or not jar_path.exists():
        return
    try:
        with zipfile.ZipFile(jar_path, "r") as zf:
            if "META-INF/download-context" not in zf.namelist():
                return
            ctx_line = zf.read("META-INF/download-context").decode("utf-8", errors="replace").strip()
        parts = ctx_line.split("\t")
        if len(parts) < 3:
            return
        exp_sha256, mojang_url, raw_filename = parts[0].strip().lower(), parts[1].strip(), parts[2].strip()
        filename = Path(raw_filename).name
        if not filename or filename in (".", ".."):
            filename = "mojang_vanilla.jar"
        cache_dir = runtime_dir / "cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        target_cache_jar = cache_dir / filename

        def _sha256_matches(path: Path) -> bool:
            if len(exp_sha256) != 64:
                return True
            return _file_digest_hex(path, "sha256") == exp_sha256

        if (
            target_cache_jar.exists()
            and target_cache_jar.stat().st_size > 1_000_000
            and _is_valid_zip_archive(target_cache_jar)
            and _sha256_matches(target_cache_jar)
        ):
            return

        if log_fn:
            log_fn(f"Pre-caching Paperclip base jar ({filename}) for fast startup...")
        tmp_cache = target_cache_jar.parent / f".{target_cache_jar.name}.tmp.{os.getpid()}.{threading.get_ident()}.part"
        last_pct = [-1]

        def _on_progress(pct: int, downloaded: int, total: int) -> None:
            if pct // 25 > last_pct[0] // 25 and log_fn:
                last_pct[0] = pct
                log_fn(
                    f"Downloading base {filename}: {pct}% ({round(downloaded / (1024*1024), 1)} MB)"
                )

        try:
            _download_to_file_verified(
                mojang_url,
                tmp_cache,
                expected_sha256=exp_sha256 if len(exp_sha256) == 64 else "",
                timeout=120,
                attempts=3,
                progress_cb=_on_progress,
            )
            if not _is_valid_zip_archive(tmp_cache):
                raise RuntimeError(f"Downloaded base jar {filename} is not a valid archive")
            _os_replace_retry(tmp_cache, target_cache_jar)
            if log_fn:
                log_fn(f"Cached {filename} ({round(target_cache_jar.stat().st_size / (1024*1024), 1)} MB)")
        finally:
            try:
                if tmp_cache.exists():
                    tmp_cache.unlink(missing_ok=True)
            except OSError:
                pass
    except Exception as e:
        if log_fn:
            log_fn(f"Warning: Paperclip base jar pre-cache skipped ({e}); server will download on boot if needed.")


def ensure_server_jar(
    runtime_dir: Path,
    software: str = "paper",
    version: str = "26.3",
    log_fn: Optional[Callable[[str], None]] = None,
    force: bool = False,
    bin_dir: Optional[Path] = None,
) -> Path:
    """
    Ensures the versioned server jar (e.g. data/bin/jars/server-paper-26.3.jar) exists,
    downloading it if missing, and returns the path to the jar.
    Storing versioned jars in data/bin/jars prevents Windows file lock collisions
    and allows instant switching between Minecraft versions!
    """
    runtime_dir.mkdir(parents=True, exist_ok=True)
    resolved_bin_dir = bin_dir or (runtime_dir.parent / "bin")
    versioned_jar = get_versioned_jar_path(resolved_bin_dir, software, version)

    if not force and versioned_jar.exists() and versioned_jar.stat().st_size > 1_000_000 and _is_valid_zip_archive(versioned_jar):
        if log_fn:
            log_fn(
                f"Minecraft {software.upper()} {version} jar ready ({round(versioned_jar.stat().st_size / (1024*1024), 1)} MB)"
            )
        ensure_paperclip_mojang_cache(versioned_jar, runtime_dir, log_fn=log_fn)
        return versioned_jar

    software = software.lower().strip()
    version = version.strip()
    if log_fn:
        log_fn(f"Resolving {software.upper()} {version} server.jar download URL...")

    download_url, resolved_sw, resolved_ver, exp_sha256, exp_sha1 = _resolve_papermc_or_mojang_with_checksum(
        software, version, log_fn=log_fn
    )
    target_jar = get_versioned_jar_path(resolved_bin_dir, resolved_sw, resolved_ver)

    if not force and target_jar.exists() and target_jar.stat().st_size > 1_000_000 and _is_valid_zip_archive(target_jar):
        ensure_paperclip_mojang_cache(target_jar, runtime_dir, log_fn=log_fn)
        return target_jar

    if log_fn:
        log_fn(f"Downloading Minecraft {resolved_sw.upper()} {resolved_ver} server.jar...")

    tmp_jar = target_jar.parent / f".{target_jar.name}.tmp.{os.getpid()}.{threading.get_ident()}.part"
    last_pct = [-1]

    def _on_progress(pct: int, downloaded: int, total: int) -> None:
        if pct // 20 > last_pct[0] // 20 and log_fn:
            last_pct[0] = pct
            log_fn(
                f"Downloading {resolved_sw.upper()} {resolved_ver} server.jar: {pct}% ({round(downloaded / (1024*1024), 1)} MB)"
            )

    try:
        _download_to_file_verified(
            download_url,
            tmp_jar,
            expected_sha256=exp_sha256,
            expected_sha1=exp_sha1,
            timeout=120,
            attempts=3,
            progress_cb=_on_progress,
        )
        if not _is_valid_zip_archive(tmp_jar):
            raise RuntimeError(f"Downloaded {resolved_sw.upper()} {resolved_ver} server.jar is corrupt or invalid")
        _os_replace_retry(tmp_jar, target_jar)
    finally:
        try:
            if tmp_jar.exists():
                tmp_jar.unlink(missing_ok=True)
        except OSError:
            pass
    ensure_paperclip_mojang_cache(target_jar, runtime_dir, log_fn=log_fn)

    if log_fn:
        log_fn(
            f"Installed {resolved_sw.upper()} {resolved_ver} ({round(target_jar.stat().st_size / (1024*1024), 1)} MB)"
        )
    return target_jar


if __name__ == "__main__":
    root = Path(__file__).resolve().parent.parent
    data_dir = root / "data"
    bin_dir = data_dir / "bin"
    runtime_dir = data_dir / "runtime-server"

    print("=== Checking Latest Minecraft Release & Bootstrapping Runtime ===")
    vers = fetch_live_minecraft_versions()
    latest_ver = vers.get("latestRelease", "26.3")
    print(f"Latest Official Minecraft Release: {latest_ver}")
    java_exe = ensure_portable_java(bin_dir, min_major=25, log_fn=print)
    jar_file = ensure_server_jar(runtime_dir, "paper", latest_ver, log_fn=print, bin_dir=bin_dir)
    print(f"=== All Binaries Ready for Minecraft {latest_ver}! ===")
