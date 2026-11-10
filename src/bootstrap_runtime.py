import os
import re
import sys
import json
import shutil
import zipfile
import subprocess
import urllib.request
from pathlib import Path
from typing import Optional, Callable, Dict, List, Any, Tuple

ADOPTIUM_JRE25_WIN_X64_API = (
    "https://api.adoptium.net/v3/binary/latest/25/ga/windows/x64/jre/hotspot/normal/eclipse"
)
ADOPTIUM_JRE21_WIN_X64_API = (
    "https://api.adoptium.net/v3/binary/latest/21/ga/windows/x64/jre/hotspot/normal/eclipse"
)
PLAYIT_WIN_URL = (
    "https://github.com/playit-cloud/playit-agent/releases/download/v0.15.26/playit-windows-x86_64-signed.exe"
)
USER_AGENT = "MineRelay/2.0 (https://github.com/minerelay; contact@minerelay.local)"


def get_java_major_version(java_cmd: str) -> int:
    """Returns the major version of a Java binary (e.g. 25, 21, 17), or 0 if invalid."""
    try:
        creationflags = subprocess.CREATE_NO_WINDOW if sys.platform.startswith("win") else 0
        res = subprocess.run(
            [java_cmd, "-version"],
            capture_output=True,
            text=True,
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
    Minecraft 26.1+ (including latest 26.3) requires Java 25 (class file version 69.0).
    Minecraft 1.20.5 - 1.21.11 requires Java 21.
    Java 25 is backwards-compatible and can run both 26.x and 1.21.x!
    """
    ver = mc_version.strip()
    if ver.startswith("26.") or ver.startswith("27."):
        return 25
    return 21


def find_portable_java(bin_dir: Path, min_major: int = 21) -> Optional[str]:
    """
    Looks for a portable java.exe inside data/bin/jre25 or data/bin/jre21
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
                return str(candidate.resolve())
    return None


def ensure_portable_java(
    bin_dir: Path,
    min_major: int = 25,
    log_fn: Optional[Callable[[str], None]] = None,
) -> str:
    """
    Ensures a Portable Java Runtime (>= min_major, defaulting to Java 25 for MC 26.3 support)
    is available inside data/bin/jre25.
    """
    existing = find_portable_java(bin_dir, min_major=min_major)
    if existing:
        if log_fn:
            log_fn(f"Portable Java Runtime ready at: {existing}")
        return existing

    target_major = 25
    api_url = ADOPTIUM_JRE25_WIN_X64_API
    folder_name = f"jre{target_major}"

    bin_dir.mkdir(parents=True, exist_ok=True)
    jre_root = bin_dir / folder_name
    jre_root.mkdir(parents=True, exist_ok=True)
    zip_path = bin_dir / f"temurin{target_major}-jre.zip"

    if log_fn:
        log_fn(
            f"Downloading Portable Java {target_major} Runtime (Eclipse Temurin {target_major} JRE for Minecraft 26.3+)... (~53 MB)"
        )

    req = urllib.request.Request(api_url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=120) as resp:
        total = int(resp.headers.get("Content-Length", 0))
        downloaded = 0
        last_pct = -1
        with open(zip_path, "wb") as out:
            while True:
                chunk = resp.read(131072)
                if not chunk:
                    break
                out.write(chunk)
                downloaded += len(chunk)
                if total > 0:
                    pct = int((downloaded / total) * 100)
                    if pct // 20 > last_pct // 20 and log_fn:
                        last_pct = pct
                        log_fn(
                            f"Downloading Portable Java {target_major} JRE: {pct}% ({round(downloaded / (1024*1024), 1)} MB)"
                        )

    if log_fn:
        log_fn(f"Extracting Portable Java {target_major} JRE...")
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(jre_root)
    try:
        zip_path.unlink()
    except OSError:
        pass

    found = find_portable_java(bin_dir, min_major=min_major)
    if not found:
        raise RuntimeError(f"Downloaded Java {target_major} archive but could not locate bin/java.exe inside.")
    if log_fn:
        log_fn(f"Portable Java {target_major} JRE ready at: {found}")
    return found


_VERSIONS_CACHE: Optional[Dict[str, Any]] = None
_VERSIONS_CACHE_TS: float = 0.0


def fetch_live_minecraft_versions() -> Dict[str, Any]:
    """
    Queries Mojang's official version_manifest_v2.json and PaperMC's Fill v3 API
    to return the latest release version and supported version lists.
    Caches the live result for 10 minutes so Admin tab loads are instant.
    Falls back to a curated list including 26.3 if offline.
    """
    global _VERSIONS_CACHE, _VERSIONS_CACHE_TS
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
    if _VERSIONS_CACHE is not None and (now - _VERSIONS_CACHE_TS) < 600:
        return _VERSIONS_CACHE

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
                        if isinstance(v, str) and "-" not in v:
                            paper_list.append(v)
        elif isinstance(versions_map, list):
            paper_list = [str(v) for v in versions_map if "-" not in str(v)]
        if paper_list:
            result["paper"] = paper_list[:25]
    except Exception:
        pass

    _VERSIONS_CACHE = result
    _VERSIONS_CACHE_TS = now
    return result


def resolve_mojang_server_url(version: str) -> Tuple[str, str]:
    """Returns (download_url, resolved_version) from Mojang's official manifest."""
    manifest_url = "https://piston-meta.mojang.com/mc/game/version_manifest_v2.json"
    req = urllib.request.Request(manifest_url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=20) as resp:
        manifest = json.loads(resp.read().decode("utf-8"))
    versions = manifest.get("versions", [])
    resolved_ver = version
    ver_url = next((v["url"] for v in versions if v["id"] == version), None)
    if not ver_url and versions:
        resolved_ver = manifest.get("latest", {}).get("release", versions[0]["id"])
        ver_url = next((v["url"] for v in versions if v["id"] == resolved_ver), versions[0]["url"])
    with urllib.request.urlopen(urllib.request.Request(ver_url, headers={"User-Agent": USER_AGENT}), timeout=20) as resp:
        ver_data = json.loads(resp.read().decode("utf-8"))
    return ver_data["downloads"]["server"]["url"], resolved_ver


def resolve_papermc_or_mojang_url(
    software: str,
    version: str,
    log_fn: Optional[Callable[[str], None]] = None,
) -> Tuple[str, str, str]:
    """
    Returns (download_url, resolved_software, resolved_version).
    Uses PaperMC Fill v3 API (/v3/projects/paper/versions/{version}/builds)
    and automatically falls back to Mojang Official Vanilla if Paper doesn't have a build for that version yet.
    """
    headers = {"User-Agent": USER_AGENT}
    if software == "paper":
        builds_url = f"https://fill.papermc.io/v3/projects/paper/versions/{version}/builds"
        try:
            req = urllib.request.Request(builds_url, headers=headers)
            with urllib.request.urlopen(req, timeout=15) as resp:
                builds_data = json.loads(resp.read().decode("utf-8"))
            if isinstance(builds_data, list) and len(builds_data) > 0:
                sorted_builds = sorted(builds_data, key=lambda b: int(b.get("id", 0)), reverse=True)
                chosen = next(
                    (b for b in sorted_builds if b.get("channel") in ("STABLE", "RECOMMENDED")),
                    sorted_builds[0],
                )
                dl = chosen.get("downloads", {})
                for key in ("server:default", "application"):
                    if key in dl and "url" in dl[key]:
                        return dl[key]["url"], "paper", version
        except Exception:
            pass
        if log_fn:
            log_fn(
                f"Note: PaperMC build not published yet for {version}; using official Mojang Vanilla {version} server.jar."
            )

    url, resolved_ver = resolve_mojang_server_url(version)
    return url, "vanilla", resolved_ver


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
    out: List[Dict[str, Any]] = []
    for f in sorted(jars_dir.glob("server-*.jar"), key=lambda p: p.stat().st_mtime, reverse=True):
        out.append({
            "name": f.name,
            "path": str(f),
            "sizeMb": round(f.stat().st_size / (1024 * 1024), 2),
        })
    return out


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
        _sha256, mojang_url, raw_filename = parts[0].strip(), parts[1].strip(), parts[2].strip()
        filename = Path(raw_filename).name
        if not filename or filename in (".", ".."):
            filename = "mojang_vanilla.jar"
        cache_dir = runtime_dir / "cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        target_cache_jar = cache_dir / filename
        if target_cache_jar.exists() and target_cache_jar.stat().st_size > 1_000_000:
            return

        if log_fn:
            log_fn(f"Pre-caching Paperclip base jar ({filename}) for fast startup...")
        tmp_cache = target_cache_jar.with_suffix(f".tmp.{os.getpid()}.jar")
        req = urllib.request.Request(mojang_url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(req, timeout=120) as resp:
            total = int(resp.headers.get("Content-Length", 0))
            downloaded = 0
            last_pct = -1
            with open(tmp_cache, "wb") as out:
                while True:
                    chunk = resp.read(131072)
                    if not chunk:
                        break
                    out.write(chunk)
                    downloaded += len(chunk)
                    if total > 0:
                        pct = int((downloaded / total) * 100)
                        if pct // 25 > last_pct // 25 and log_fn:
                            last_pct = pct
                            log_fn(
                                f"Downloading base {filename}: {pct}% ({round(downloaded / (1024*1024), 1)} MB)"
                            )
        os.replace(tmp_cache, target_cache_jar)
        if log_fn:
            log_fn(f"Cached {filename} ({round(target_cache_jar.stat().st_size / (1024*1024), 1)} MB)")
    except Exception:
        pass


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

    if not force and versioned_jar.exists() and versioned_jar.stat().st_size > 1_000_000:
        if log_fn:
            log_fn(
                f"Minecraft {software.upper()} {version} jar ready ({round(versioned_jar.stat().st_size / (1024*1024), 1)} MB)"
            )
        ensure_paperclip_mojang_cache(versioned_jar, runtime_dir, log_fn=log_fn)
        # Also mirror to runtime_dir/server.jar if not locked
        try:
            shutil.copy2(versioned_jar, runtime_dir / "server.jar")
        except OSError:
            pass
        return versioned_jar

    software = software.lower().strip()
    version = version.strip()
    if log_fn:
        log_fn(f"Resolving {software.upper()} {version} server.jar download URL...")

    download_url, resolved_sw, resolved_ver = resolve_papermc_or_mojang_url(software, version, log_fn=log_fn)

    if log_fn:
        log_fn(f"Downloading Minecraft {resolved_sw.upper()} {resolved_ver} server.jar...")

    tmp_jar = versioned_jar.with_suffix(f".tmp.{os.getpid()}.jar")
    req = urllib.request.Request(download_url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=120) as resp:
        total = int(resp.headers.get("Content-Length", 0))
        downloaded = 0
        last_pct = -1
        with open(tmp_jar, "wb") as out:
            while True:
                chunk = resp.read(131072)
                if not chunk:
                    break
                out.write(chunk)
                downloaded += len(chunk)
                if total > 0:
                    pct = int((downloaded / total) * 100)
                    if pct // 20 > last_pct // 20 and log_fn:
                        last_pct = pct
                        log_fn(
                            f"Downloading {resolved_sw.upper()} {resolved_ver} server.jar: {pct}% ({round(downloaded / (1024*1024), 1)} MB)"
                        )

    os.replace(tmp_jar, versioned_jar)
    ensure_paperclip_mojang_cache(versioned_jar, runtime_dir, log_fn=log_fn)
    try:
        shutil.copy2(versioned_jar, runtime_dir / "server.jar")
    except OSError:
        pass

    if log_fn:
        log_fn(
            f"Installed {resolved_sw.upper()} {resolved_ver} ({round(versioned_jar.stat().st_size / (1024*1024), 1)} MB)"
        )
    return versioned_jar


def ensure_playit_binary(
    bin_dir: Path,
    log_fn: Optional[Callable[[str], None]] = None,
) -> Path:
    bin_dir.mkdir(parents=True, exist_ok=True)
    target = bin_dir / ("playit.exe" if sys.platform.startswith("win") else "playit")
    if target.exists() and target.stat().st_size > 500_000:
        if log_fn:
            log_fn(f"playit.gg binary already installed at {target}")
        return target

    if log_fn:
        log_fn("Downloading official playit.gg tunnel agent binary...")
    tmp = target.with_suffix(".tmp")
    req = urllib.request.Request(PLAYIT_WIN_URL, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=60) as resp:
        with open(tmp, "wb") as out:
            while True:
                chunk = resp.read(131072)
                if not chunk:
                    break
                out.write(chunk)
    os.replace(tmp, target)
    if log_fn:
        log_fn(f"playit.gg binary installed ({round(target.stat().st_size / (1024*1024), 1)} MB)")
    return target


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
    playit_exe = ensure_playit_binary(bin_dir, log_fn=print)
    print(f"=== All Binaries Ready for Minecraft {latest_ver}! ===")
