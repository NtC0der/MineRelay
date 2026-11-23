"""
Stage 2 E2E & Unit Verification Suite for the 3 User Goals:
  Goal 1: First-Time New-Member / Cold-PC Hosting Reliability
  Goal 2: Fast Optimized Sync (Bloat Exclusion, Smart Compression, Incremental Delta Snapshots)
  Goal 3: Outdated MineRelay Version Check & GitHub Download Prompt

All tests run strictly inside isolated tempfile directories (zero touches to real Drive or dist/).
"""

import io
import os
import sys
import json
import time
import zipfile
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict

os.environ["MINERELAY_TEST_MODE"] = "1"

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.bootstrap_runtime import (
    _download_to_file_verified,
    check_latest_minerelay_release,
    is_version_older,
    parse_semver,
    required_java_major_for_mc,
)
from src.models import APP_VERSION, ClusterConfig
from src.server import AppContext, create_request_handler


class InMemoryHTTPClient:
    """Dispatches HTTP requests directly through DashboardRequestHandler in-memory."""

    def __init__(self, data_dir: str):
        self.ctx = AppContext(data_dir=data_dir)
        self.public_dir = ROOT_DIR / "public"
        self.handler_cls = create_request_handler(self.ctx, self.public_dir)

    def request(self, method: str, path: str, token: str = "", body: Any = None):
        body_bytes = json.dumps(body).encode("utf-8") if body is not None else b""
        headers_lines = [
            f"{method} {path} HTTP/1.1",
            "Host: localhost",
            "Content-Type: application/json",
            f"Content-Length: {len(body_bytes)}",
        ]
        if token:
            headers_lines.append(f"Authorization: Bearer {token}")
        raw_request = ("\r\n".join(headers_lines) + "\r\n\r\n").encode("utf-8") + body_bytes

        rfile = io.BytesIO(raw_request)
        wfile = io.BytesIO()

        class DummySocket:
            def makefile(self, mode, *args, **kwargs):
                return rfile if "r" in mode else wfile

            def sendall(self, data):
                wfile.write(data)

            def close(self):
                pass

        self.handler_cls(DummySocket(), ("127.0.0.1", 50000), None)
        raw_response = wfile.getvalue()
        header_part, _, payload_part = raw_response.partition(b"\r\n\r\n")
        status_line = header_part.splitlines()[0].decode("utf-8")
        status_code = int(status_line.split(" ")[1])
        try:
            parsed = json.loads(payload_part.decode("utf-8"))
        except Exception:
            parsed = {"raw": payload_part.decode("utf-8", errors="replace")}
        return status_code, parsed


def test_goal1_first_time_member_hosting(tmp_root: Path) -> None:
    print("\n[Goal 1] Testing Brand-New Member First-Time / Cold-PC Hosting Reliability...")
    shared_dir = tmp_root / "g1_shared"
    admin_dir = tmp_root / "g1_admin"
    member_dir = tmp_root / "g1_member"
    shared_dir.mkdir(parents=True, exist_ok=True)
    admin_dir.mkdir(parents=True, exist_ok=True)
    member_dir.mkdir(parents=True, exist_ok=True)

    # Admin initializes cluster with a machine-specific javaPath that does NOT exist on Member PC
    pc_admin = InMemoryHTTPClient(str(admin_dir))
    st, init_res = pc_admin.request(
        "POST",
        "/api/setup/init",
        body={
            "serverName": "ColdStartSMP",
            "adminUsername": "Owner",
            "machineName": "PC-Owner",
            "sharedSyncDir": str(shared_dir),
        },
    )
    assert st == 200
    admin_token = init_res["token"]

    st, set_res = pc_admin.request(
        "PUT",
        "/api/admin/settings",
        token=admin_token,
        body={"javaPath": str(tmp_root / "nonexistent-owner-only-path" / "java.exe")},
    )
    assert st == 200

    st, mem_res = pc_admin.request(
        "POST",
        "/api/admin/accounts",
        token=admin_token,
        body={"username": "FreshMember"},
    )
    assert st == 201
    member_pk = mem_res["generatedPasskey"]
    member_id = mem_res["account"]["id"]
    assert mem_res["account"]["hostCount"] == 0

    # Cold Member PC connects to shared Drive folder
    pc_mem = InMemoryHTTPClient(str(member_dir))

    # Reject invalid sync-dir pointing to a regular file or non-existent parent
    bad_file = tmp_root / "not_a_dir.txt"
    bad_file.write_text("x", encoding="utf-8")
    st_bad, _ = pc_mem.request("POST", "/api/setup/sync-dir", body={"sharedSyncDir": str(bad_file)})
    assert st_bad == 400

    st, _ = pc_mem.request(
        "POST",
        "/api/setup/sync-dir",
        body={"sharedSyncDir": str(shared_dir), "machineName": "PC-FreshMember"},
    )
    assert st == 200

    st, login_res = pc_mem.request("POST", "/api/auth/login", body={"passkey": member_pk})
    assert st == 200
    mem_token = login_res["token"]

    # 1.1 check_java_installed gracefully falls back when shared java_path doesn't exist locally
    java_info = pc_mem.ctx.mc_manager.check_java_installed(
        str(tmp_root / "nonexistent-owner-only-path" / "java.exe"),
        "26.3",
    )
    assert isinstance(java_info, dict) and "installed" in java_info

    # 1.2 Corrupt JAR detection & EULA repair
    jar_path = tmp_root / "corrupt-server.jar"
    jar_path.write_bytes(b"TRUNCATED-NOT-A-JAR")
    assert pc_mem.ctx.mc_manager._is_valid_server_jar(jar_path) is False
    valid_jar = tmp_root / "valid-server.jar"
    with zipfile.ZipFile(valid_jar, "w", compression=zipfile.ZIP_STORED) as zf:
        zf.writestr("META-INF/MANIFEST.MF", "Manifest-Version: 1.0\nMain-Class: net.minecraft.bundler.Main\n")
        zf.writestr("payload.bin", b"\x00" * 1_050_000)
    assert pc_mem.ctx.mc_manager._is_valid_server_jar(valid_jar) is True

    eula_path = pc_mem.ctx.mc_manager.runtime_dir / "eula.txt"
    eula_path.parent.mkdir(parents=True, exist_ok=True)
    eula_path.write_text("eula=false\n", encoding="utf-8")
    pc_mem.ctx.mc_manager._ensure_eula_accepted()
    assert "eula=true" in eula_path.read_text(encoding="utf-8").lower()

    # 1.3 preflight_cloud_ready rejects 0-byte snapshot placeholder WITHOUT claiming host lock!
    pc_mem.ctx.sync_engine.snapshots_dir.mkdir(parents=True, exist_ok=True)
    snap_path = pc_mem.ctx.sync_engine.latest_snapshot_path
    snap_path.write_bytes(b"")
    pc_mem.ctx.sync_engine._write_json_atomic(
        pc_mem.ctx.sync_engine.sync_manifest_path,
        {"snapshotSeq": 1, "sha256": "abc", "sizeBytes": 100, "hostUsername": "Owner"},
        in_place_if_exists=True,
    )
    st_pre, res_pre = pc_mem.request("POST", "/api/server/start", token=mem_token)
    assert st_pre == 400 and pc_mem.ctx.sync_engine.get_lock().is_locked is False, (
        f"Preflight failure must reject BEFORE claiming lock! st={st_pre} res={res_pre} lock={pc_mem.ctx.sync_engine.get_lock().to_dict()}"
    )
    snap_path.unlink(missing_ok=True)
    pc_mem.ctx.sync_engine.sync_manifest_path.unlink(missing_ok=True)

    # 1.4 Async start_hosting + non-deadlocking Cancel Startup under slow snapshot pull
    pull_started = threading.Event()
    allow_pull_finish = threading.Event()
    orig_pull = pc_mem.ctx.sync_engine.pull_world_to_runtime

    def _slow_pull(strict: bool = False):
        pull_started.set()
        allow_pull_finish.wait(timeout=5.0)
        return orig_pull(strict=strict)

    pc_mem.ctx.sync_engine.pull_world_to_runtime = _slow_pull  # type: ignore
    try:
        mem_user = pc_mem.ctx.auth_service.get_user_by_token(mem_token)
        assert mem_user is not None
        async_res = pc_mem.ctx.mc_manager.start_hosting(mem_user, sync=False)
        assert async_res["localStatus"] in ("syncing_pull", "preparing", "starting", "running")
        assert pull_started.wait(timeout=3.0), "Async start worker did not begin pull promptly"
        # Cancel Startup while pull is blocked -> must NOT deadlock!
        t_cancel = threading.Thread(target=lambda: pc_mem.ctx.mc_manager.stop_hosting(mem_user), daemon=True)
        t_cancel.start()
        time.sleep(0.05)
        allow_pull_finish.set()
        t_cancel.join(timeout=6.0)
        assert not t_cancel.is_alive(), "Cancel Startup deadlocked against _lifecycle_lock!"
        assert pc_mem.ctx.mc_manager.local_status == "idle"
        assert pc_mem.ctx.sync_engine.get_lock().is_locked is False
    finally:
        pc_mem.ctx.sync_engine.pull_world_to_runtime = orig_pull  # type: ignore

    # 1.5 First successful member host increments hostCount 0 -> 1 and does not double-write on repeat
    st, start_ok = pc_mem.request("POST", "/api/server/start", token=mem_token)
    assert st == 200 and start_ok["localStatus"] == "running"
    cfg_after_first = pc_mem.ctx.sync_engine.load_cluster_config()
    assert cfg_after_first is not None
    mem_acc = next(a for a in cfg_after_first.accounts if a.id == member_id)
    assert mem_acc.host_count == 1
    startup_snap = pc_mem.ctx.mc_manager.get_status_summary()["startup"]
    assert startup_snap["phase"] in ("", "ready", "running", "idle") and isinstance(startup_snap["progress"], int), startup_snap
    st, stop_ok = pc_mem.request("POST", "/api/server/stop", token=mem_token)
    assert st == 200 and stop_ok["localStatus"] == "idle"

    # 1.6 TunnelManager sets claim_required cleanly without spawning competing playit daemon
    tm = pc_mem.ctx.tunnel_manager
    orig_ensure_bin = tm._ensure_up_to_date_binary
    fake_playit = tmp_root / "fake_playit.exe"
    fake_playit.write_bytes(b"MZ")
    tm._ensure_up_to_date_binary = lambda: fake_playit  # type: ignore
    orig_claim = tm._ensure_claim_flow_async
    claim_calls = []

    def _fake_claim(bin_path_arg=None, server_port=25565):
        claim_calls.append(server_port)
        with tm._mutex:
            tm.status = "claim_required"

    tm._ensure_claim_flow_async = _fake_claim  # type: ignore
    old_tmode = os.environ.get("MINERELAY_TEST_MODE")
    try:
        if tm.shared_secret_toml_path.exists():
            tm.shared_secret_toml_path.unlink()
        os.environ["MINERELAY_TEST_MODE"] = "0"
        ts = tm.start_tunnel(server_port=25565)
        assert ts["status"] == "claim_required" and len(claim_calls) == 1, ts
        assert tm._proc is None
    finally:
        if old_tmode is None:
            os.environ.pop("MINERELAY_TEST_MODE", None)
        else:
            os.environ["MINERELAY_TEST_MODE"] = old_tmode
        tm.stop_tunnel()
        tm._ensure_up_to_date_binary = orig_ensure_bin  # type: ignore
        tm._ensure_claim_flow_async = orig_claim  # type: ignore

    print("  [OK] Goal 1 passed: cold-PC Java fallback, corrupt JAR check, EULA repair, preflight, async cancel & claim_required verified.")


def test_goal2_fast_incremental_delta_sync(tmp_root: Path) -> None:
    print("\n[Goal 2] Testing Fast Incremental Delta Sync, Bloat Exclusions & Smart Compression...")
    shared_dir = tmp_root / "g2_shared"
    pc1_dir = tmp_root / "g2_pc1"
    pc2_dir = tmp_root / "g2_pc2"
    for d in (shared_dir, pc1_dir, pc2_dir):
        d.mkdir(parents=True, exist_ok=True)

    pc1 = InMemoryHTTPClient(str(pc1_dir))
    st, init_res = pc1.request(
        "POST",
        "/api/setup/init",
        body={"serverName": "FastSyncSMP", "adminUsername": "Admin", "machineName": "PC-1", "sharedSyncDir": str(shared_dir)},
    )
    assert st == 200
    admin_token = init_res["token"]

    pc2 = InMemoryHTTPClient(str(pc2_dir))
    st, _ = pc2.request("POST", "/api/setup/sync-dir", body={"sharedSyncDir": str(shared_dir), "machineName": "PC-2"})
    assert st == 200

    rt1 = pc1.ctx.sync_engine.runtime_dir
    rt1.mkdir(parents=True, exist_ok=True)

    # Populate valid world/plugin/config files + massive regenerable bloat dirs/files
    (rt1 / "world" / "region").mkdir(parents=True, exist_ok=True)
    mca_data = b"REGION-BINARY-" + (b"\xAB\xCD\xEF\x01" * 20000)  # ~80 KB
    (rt1 / "world" / "region" / "r.0.0.mca").write_bytes(mca_data)
    (rt1 / "world" / "region" / "r.0.1.mca").write_bytes(b"REGION-2-" + (b"\x11\x22" * 20000))
    (rt1 / "plugins").mkdir(parents=True, exist_ok=True)
    (rt1 / "plugins" / "FastPlugin.jar").write_bytes(b"PK\x03\x04plugin-jar-bytes")
    (rt1 / "server.properties").write_text("motd=Fast Sync World\nserver-port=25565\n", encoding="utf-8")
    (rt1 / "eula.txt").write_text("eula=true\n", encoding="utf-8")

    # Regenerable bloat that MUST be excluded from zips
    bloat_files = [
        rt1 / "bundler" / "versions" / "1.21.4.jar",
        rt1 / "cache" / "mojang_1.21.4.jar",
        rt1 / "libraries" / "com" / "google" / "guava.jar",
        rt1 / "versions" / "26.3" / "26.3.jar",
        rt1 / "logs" / "2026-11-17-1.log.gz",
        rt1 / "crash-reports" / "crash.txt",
        rt1 / "timings" / "timings.txt",
        rt1 / "debug" / "profile.txt",
        rt1 / "plugins" / ".paper-remapped" / "FastPlugin.jar",
        rt1 / "plugins" / "spark" / "profiles" / "heap.sparkprofile",
        rt1 / "plugins" / "spark" / "dumps" / "dump.json",
        rt1 / "server.jar",
        rt1 / "server-paper-26.3.jar",
        rt1 / "world" / "session.lock",
        rt1 / "heap.hprof",
    ]
    for bf in bloat_files:
        bf.parent.mkdir(parents=True, exist_ok=True)
        bf.write_bytes(b"BLOAT" * 200)

    # 2.1 First push creates base latest-server.zip + file-index.json with exclusions & smart compression
    push1 = pc1.ctx.sync_engine.push_world_from_runtime(create_backup=True, backup_label="base", host_username="Admin")
    assert push1["mode"] == "full"
    assert pc1.ctx.sync_engine.file_index_path.exists()
    with zipfile.ZipFile(pc1.ctx.sync_engine.latest_snapshot_path, "r") as zf:
        names = {n.replace("\\", "/") for n in zf.namelist()}
        assert "world/region/r.0.0.mca" in names
        assert "plugins/FastPlugin.jar" in names
        assert "server.properties" in names
        for excluded_marker in ("bundler/", "cache/", "libraries/", "versions/", "logs/", "crash-reports/", "timings/", "debug/", ".paper-remapped/", "plugins/spark/", "server.jar", "server-paper-26.3.jar", "session.lock", "heap.hprof"):
            assert not any(excluded_marker in n for n in names), f"Bloat '{excluded_marker}' leaked into base snapshot: {names}"
        assert zf.getinfo("world/region/r.0.0.mca").compress_type == zipfile.ZIP_STORED
        assert zf.getinfo("plugins/FastPlugin.jar").compress_type == zipfile.ZIP_STORED
        assert zf.getinfo("server.properties").compress_type == zipfile.ZIP_DEFLATED

    # Same exclusions must apply to backup zips!
    bk_zip = pc1.ctx.sync_engine.backups_dir / push1["backupFile"]
    with zipfile.ZipFile(bk_zip, "r") as bzf:
        bnames = {n.replace("\\", "/") for n in bzf.namelist()}
        assert not any("bundler/" in n or "server-paper-26.3.jar" in n for n in bnames)

    # 2.2 PC2 pulls base + preserves local static files (libraries/ & .paper-remapped/)
    rt2 = pc2.ctx.sync_engine.runtime_dir
    (rt2 / "libraries" / "local-static.jar").parent.mkdir(parents=True, exist_ok=True)
    (rt2 / "libraries" / "local-static.jar").write_bytes(b"keep-me")
    (rt2 / "plugins" / ".paper-remapped" / "remapped.json").parent.mkdir(parents=True, exist_ok=True)
    (rt2 / "plugins" / ".paper-remapped" / "remapped.json").write_text("{}", encoding="utf-8")

    pull1 = pc2.ctx.sync_engine.pull_world_to_runtime(strict=True)
    assert pull1["synced"] is True
    assert (rt2 / "world" / "region" / "r.0.0.mca").read_bytes() == mca_data
    assert (rt2 / "libraries" / "local-static.jar").exists()
    assert (rt2 / "plugins" / ".paper-remapped" / "remapped.json").exists()

    # 2.3 Edit ONLY 1 small config file + add 1 small player file -> incremental delta push!
    base_mtime_before = pc1.ctx.sync_engine.latest_snapshot_path.stat().st_mtime_ns
    (rt1 / "server.properties").write_text("motd=Delta Updated!\nserver-port=25565\n", encoding="utf-8")
    (rt1 / "world" / "small-edit.txt").write_text("delta-v2", encoding="utf-8")

    push2 = pc1.ctx.sync_engine.push_world_from_runtime(create_backup=False, host_username="Admin")
    assert push2["mode"] == "delta", f"Expected delta push, got: {push2}"
    assert pc1.ctx.sync_engine.latest_snapshot_path.stat().st_mtime_ns == base_mtime_before, "Delta push must NOT rewrite full latest-server.zip!"
    assert push2["lastSyncStats"]["skippedBytes"] > 0
    assert push2["lastSyncStats"]["changedFiles"] == 2
    delta_files = list(pc1.ctx.sync_engine.deltas_dir.glob("delta-*.zip"))
    assert len(delta_files) == 1
    with zipfile.ZipFile(delta_files[0], "r") as dzf:
        dnames = {n.replace("\\", "/") for n in dzf.namelist()}
        assert dnames == {"server.properties", "world/small-edit.txt"}, f"Delta should contain ONLY changed files, got: {dnames}"

    # Verify diagnostics surface delta metrics
    st_diag, diag_res = pc1.request("GET", "/api/setup/status")
    assert st_diag == 200
    sdiag = diag_res["syncDiagnostics"]
    assert sdiag["syncMode"] == "delta" and sdiag["deltaCount"] == 1 and sdiag["lastSyncStats"]["changedFiles"] == 2

    # 2.4 PC2 pulls delta -> unchanged r.0.0.mca is NOT rewritten (inode/mtime untouched), deleted file removed!
    mca2_path = rt2 / "world" / "region" / "r.0.0.mca"
    mca2_mtime_ns = mca2_path.stat().st_mtime_ns
    pull2 = pc2.ctx.sync_engine.pull_world_to_runtime(strict=True)
    assert pull2["synced"] is True
    assert mca2_path.stat().st_mtime_ns == mca2_mtime_ns, "Unchanged region .mca should be reused locally without rewriting!"
    assert (rt2 / "world" / "small-edit.txt").read_text(encoding="utf-8") == "delta-v2"
    assert "Delta Updated!" in (rt2 / "server.properties").read_text(encoding="utf-8")

    # Delete small-edit.txt on PC1 -> delta removes it on PC2 too
    (rt1 / "world" / "small-edit.txt").unlink()
    push3 = pc1.ctx.sync_engine.push_world_from_runtime(create_backup=False, host_username="Admin")
    assert push3["mode"] == "delta"
    pc2.ctx.sync_engine.pull_world_to_runtime(strict=True)
    assert not (rt2 / "world" / "small-edit.txt").exists(), "Deleted file must be pruned during incremental pull!"

    # 2.5 Auto-compaction when MAX_DELTA_CHAIN is exceeded
    for step in range(max(1, pc1.ctx.sync_engine.MAX_DELTA_CHAIN - 1)):
        (rt1 / "world" / f"step_{step}.txt").write_text(f"v{step}", encoding="utf-8")
        p_res = pc1.ctx.sync_engine.push_world_from_runtime(create_backup=False, host_username="Admin")
    assert p_res["mode"] == "full", f"Expected auto-compaction to full after MAX_DELTA_CHAIN deltas, got: {p_res}"
    assert len(list(pc1.ctx.sync_engine.deltas_dir.glob("delta-*.zip"))) == 0, "Old deltas should be pruned after compaction"

    # 2.6 Legacy backward compatibility: delete file-index.json + clear manifest deltas -> pulls full latest-server.zip cleanly
    pc1.ctx.sync_engine.file_index_path.unlink(missing_ok=True)
    man = pc1.ctx.sync_engine._read_sync_manifest() or {}
    man.pop("fileIndexSha256", None)
    man["mode"] = "full"
    man["deltas"] = []
    pc1.ctx.sync_engine._write_json_atomic(pc1.ctx.sync_engine.sync_manifest_path, man, in_place_if_exists=True)
    (rt2 / "world" / "region" / "r.0.0.mca").unlink()
    pull_legacy = pc2.ctx.sync_engine.pull_world_to_runtime(strict=True)
    assert pull_legacy["synced"] is True and (rt2 / "world" / "region" / "r.0.0.mca").read_bytes() == mca_data

    # 2.7 Google Drive conflict copies on file-index (1).json & deltas/delta-* (1).zip get reconciled/cleaned
    pc1.ctx.sync_engine.push_world_from_runtime(create_backup=False, host_username="Admin")
    idx_conflict = pc1.ctx.sync_engine.snapshots_dir / "file-index (1).json"
    idx_conflict.write_text(pc1.ctx.sync_engine.file_index_path.read_text(encoding="utf-8"), encoding="utf-8")
    (pc1.ctx.sync_engine.deltas_dir / "delta-999-abc (1).zip").write_bytes(b"old")
    pc1.ctx.sync_engine._cleanup_cloud_conflict_files()
    assert not idx_conflict.exists()
    assert not (pc1.ctx.sync_engine.deltas_dir / "delta-999-abc (1).zip").exists()

    print("  [OK] Goal 2 passed: bloat excluded, ZIP_STORED/DEFLATED verified, delta chain/pull/delete/compaction & Drive cleanup passed.")


def test_goal3_version_check_and_download_retry(tmp_root: Path) -> None:
    print("\n[Goal 3] Testing Outdated MineRelay Version Detection, GitHub API & Verified Download Retry...")

    # 3.1 Semver & MC Java major parsing checks
    assert is_version_older("1.0.6", "1.0.7") is True
    assert is_version_older("v1.9.0", "v1.10.0") is True
    assert is_version_older("1.0.6-rc1", "1.0.6") is True
    assert is_version_older("1.0.6", "1.0.6") is False
    assert is_version_older("1.0.7", "1.0.6") is False
    assert parse_semver("v1.10.2")[:3] == (1, 10, 2)
    assert required_java_major_for_mc("26.3") == 25
    assert required_java_major_for_mc("28.0.1") == 25

    # 3.2 Mock HTTP server for GitHub Releases API + _download_to_file_verified retry test
    import hashlib
    good_payload = b"PK\x03\x04valid-downloaded-payload-1234567890"
    good_sha256 = hashlib.sha256(good_payload).hexdigest()
    state_srv = {
        "tag": "v9.9.9",
        "mode": "ok",
        "dl_attempts": 0,
    }

    class MockGitHubHandler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            return

        def do_GET(self):
            if self.path == "/releases/latest":
                if state_srv["mode"] == "error":
                    self.send_response(503)
                    self.end_headers()
                    return
                body = json.dumps({
                    "tag_name": state_srv["tag"],
                    "name": f"MineRelay {state_srv['tag']}",
                    "html_url": "https://github.com/NtC0der/MineRelay/releases/tag/" + state_srv["tag"],
                    "assets": [
                        {
                            "name": "MineRelay.exe",
                            "browser_download_url": f"https://github.com/NtC0der/MineRelay/releases/download/{state_srv['tag']}/MineRelay.exe",
                        }
                    ],
                }).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return

            if self.path == "/download/artifact":
                state_srv["dl_attempts"] += 1
                if state_srv["dl_attempts"] == 1:
                    self.send_response(503)
                    self.end_headers()
                    return
                if state_srv["dl_attempts"] == 2:
                    # Truncated response (Content-Length mismatch)
                    self.send_response(200)
                    self.send_header("Content-Length", str(len(good_payload) + 100))
                    self.end_headers()
                    self.wfile.write(good_payload[:10])
                    return
                self.send_response(200)
                self.send_header("Content-Length", str(len(good_payload)))
                self.end_headers()
                self.wfile.write(good_payload)
                return

            self.send_response(404)
            self.end_headers()

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), MockGitHubHandler)
    port = httpd.server_address[1]
    srv_thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    srv_thread.start()

    old_api = os.environ.get("MINERELAY_RELEASES_API_URL")
    old_ver = os.environ.get("MINERELAY_APP_VERSION")
    try:
        # Test _download_to_file_verified retries across 503 + truncated + SHA-256 success
        dl_dest = tmp_root / "retry_artifact.zip"
        _download_to_file_verified(
            f"http://127.0.0.1:{port}/download/artifact",
            dl_dest,
            expected_sha256=good_sha256,
            timeout=3.0,
            attempts=3,
        )
        assert dl_dest.read_bytes() == good_payload and state_srv["dl_attempts"] == 3

        os.environ["MINERELAY_RELEASES_API_URL"] = f"http://127.0.0.1:{port}/releases/latest"
        os.environ["MINERELAY_APP_VERSION"] = "1.0.6"
        vc_outdated = check_latest_minerelay_release(force=True)
        assert vc_outdated["isOutdated"] is True
        assert vc_outdated["currentVersion"] == "1.0.6"
        assert vc_outdated["latestVersion"] == "9.9.9"
        assert "NtC0der/MineRelay/releases" in vc_outdated["downloadUrl"]

        # Wire into HTTP server endpoints (/api/setup/status, /api/app/version-check, /api/auth/login, /api/server/status)
        data_dir = tmp_root / "g3_pc"
        shared_dir = tmp_root / "g3_shared"
        data_dir.mkdir(parents=True, exist_ok=True)
        shared_dir.mkdir(parents=True, exist_ok=True)
        pc = InMemoryHTTPClient(str(data_dir))
        st, setup_res = pc.request("GET", "/api/setup/status")
        assert st == 200 and setup_res["versionCheck"]["isOutdated"] is True
        assert setup_res["versionCheck"]["latestVersion"] == "9.9.9"

        st, init_res = pc.request(
            "POST",
            "/api/setup/init",
            body={"serverName": "VerCheckSMP", "adminUsername": "Admin", "machineName": "PC-Ver", "sharedSyncDir": str(shared_dir)},
        )
        assert st == 200
        tok = init_res["token"]

        st, stat_res = pc.request("GET", "/api/server/status", token=tok)
        assert st == 200 and stat_res["versionCheck"]["isOutdated"] is True

        # Switch GitHub mock to match current version -> POST /api/app/version-check clears isOutdated
        state_srv["tag"] = f"v{APP_VERSION}"
        os.environ["MINERELAY_APP_VERSION"] = APP_VERSION
        st, vc_post = pc.request("POST", "/api/app/version-check", token=tok)
        assert st == 200 and vc_post["versionCheck"]["isOutdated"] is False

        # Offline / 503 error degrades gracefully WITHOUT crashing setup/status
        state_srv["mode"] = "error"
        vc_err = check_latest_minerelay_release(force=True)
        assert vc_err["isOutdated"] is False and vc_err["error"] != ""

        # Cluster latestAppVersion propagates warning even when GitHub API is offline!
        vc_cluster = check_latest_minerelay_release(force=False, cluster_latest="9.9.9")
        assert vc_cluster["isOutdated"] is True and vc_cluster["source"] == "cluster"

        # Frontend invariants for outdated version error prompt + first-time UX + fast sync diagnostics
        idx_html = (ROOT_DIR / "public" / "index.html").read_text(encoding="utf-8")
        app_js = (ROOT_DIR / "public" / "app.js").read_text(encoding="utf-8")
        styles_css = (ROOT_DIR / "public" / "styles.css").read_text(encoding="utf-8")

        for dom_id in (
            "auth-version-error",
            "dashboard-version-banner",
            "version-update-modal",
            "cluster-uninit-notice",
            "auth-sync-warning",
            "hero-startup-error-banner",
            "first-host-hint-banner",
            "app-version-pill",
        ):
            assert dom_id in idx_html, f"Missing DOM element #{dom_id} in public/index.html"
        for sym in ("renderVersionCheck", "POST", "/api/app/version-check", "claim_required", "lastSyncStats", "deltaCount", "maxDeltaChain"):
            assert sym in app_js, f"Missing symbol '{sym}' in public/app.js"
        assert ".version-banner" in styles_css and ".alert-danger" in styles_css

    finally:
        httpd.shutdown()
        httpd.server_close()
        if old_api is None:
            os.environ.pop("MINERELAY_RELEASES_API_URL", None)
        else:
            os.environ["MINERELAY_RELEASES_API_URL"] = old_api
        if old_ver is None:
            os.environ.pop("MINERELAY_APP_VERSION", None)
        else:
            os.environ["MINERELAY_APP_VERSION"] = old_ver
        check_latest_minerelay_release(force=True)

    print("  [OK] Goal 3 passed: semver, mocked GitHub latest release, HTTP retry/checksum, offline cluster check & frontend prompts passed.")


def run_all() -> None:
    print("============================================================")
    print("  MineRelay Fast Sync, First-Time Host & Version Check E2E  ")
    print("============================================================")
    with tempfile.TemporaryDirectory(prefix="minerelay_goals123_") as tmp_dir:
        tmp_root = Path(tmp_dir)
        test_goal1_first_time_member_hosting(tmp_root)
        test_goal2_fast_incremental_delta_sync(tmp_root)
        test_goal3_version_check_and_download_retry(tmp_root)
    print("\n============================================================")
    print("  ALL GOALS 1, 2 & 3 E2E TESTS PASSED SUCCESSFULLY!        ")
    print("============================================================")


if __name__ == "__main__":
    run_all()
