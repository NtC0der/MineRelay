"""
Focused verification suite for fast sync optimizations, local-by-default backups,
hardened periodic auto-sync reliability, and real-time sync progress reporting.

Runs strictly inside isolated temporary directories (MINERELAY_TEST_MODE=1).
"""

import io
import os
import re
import sys
import json
import time
import zipfile
import tempfile
import threading
from pathlib import Path
from typing import Any, Dict, List

os.environ["MINERELAY_TEST_MODE"] = "1"

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.models import ClusterConfig, UserAccount
from src.server import AppContext, create_request_handler
from src.sync_engine import SyncEngine


class InMemoryHTTPClient:
    """Dispatches HTTP requests through DashboardRequestHandler in-memory."""

    def __init__(self, data_dir: str):
        self.ctx = AppContext(data_dir=data_dir)
        self.public_dir = ROOT_DIR / "public"
        self.handler_cls = create_request_handler(self.ctx, self.public_dir)

    def request(self, method: str, path: str, token: str = "", body: Any = None, raw: bool = False):
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
        if raw:
            return status_code, payload_part
        try:
            parsed = json.loads(payload_part.decode("utf-8"))
        except Exception:
            parsed = {"raw": payload_part.decode("utf-8", errors="replace")}
        return status_code, parsed


class _FakeStdin:
    def __init__(self, events: List[str], on_write=None):
        self.commands: List[str] = []
        self.events = events
        self.on_write = on_write

    def write(self, data: str) -> int:
        cmd = data.strip()
        self.commands.append(cmd)
        self.events.append(f"stdin:{cmd}")
        if self.on_write:
            self.on_write(cmd)
        return len(data)

    def flush(self) -> None:
        pass


class _FakeRunningProc:
    def __init__(self, stdin: _FakeStdin):
        self.stdin = stdin
        self.stdout = None
        self.pid = 999999

    def poll(self):
        return None


def _init_cluster_pair(tmp_root: Path):
    shared_dir = tmp_root / "shared"
    admin_dir = tmp_root / "admin_data"
    member_dir = tmp_root / "member_data"
    shared_dir.mkdir(parents=True, exist_ok=True)
    admin_dir.mkdir(parents=True, exist_ok=True)
    member_dir.mkdir(parents=True, exist_ok=True)

    pc_admin = InMemoryHTTPClient(str(admin_dir))
    st, init_res = pc_admin.request(
        "POST",
        "/api/setup/init",
        body={
            "serverName": "FastSyncTestSMP",
            "adminUsername": "AdminOne",
            "machineName": "PC-Admin",
            "sharedSyncDir": str(shared_dir),
        },
    )
    assert st == 200, f"setup/init failed: {init_res}"
    admin_token = init_res["token"]

    st, mem_res = pc_admin.request(
        "POST",
        "/api/admin/accounts",
        token=admin_token,
        body={"username": "MemberTwo"},
    )
    assert st == 201
    member_pk = mem_res["generatedPasskey"]

    pc_member = InMemoryHTTPClient(str(member_dir))
    st, _ = pc_member.request(
        "POST",
        "/api/setup/sync-dir",
        body={"sharedSyncDir": str(shared_dir), "machineName": "PC-Member"},
    )
    assert st == 200
    st, login_res = pc_member.request("POST", "/api/auth/login", body={"passkey": member_pk})
    assert st == 200
    member_token = login_res["token"]
    return pc_admin, admin_token, pc_member, member_token, shared_dir


def test_1_default_local_stop_backups(tmp_root: Path) -> None:
    print("\n[Test 1] Default cloud_backup_on_stop=False & local-only stop backups...")
    pc_admin, admin_token, _pc_mem, _mem_tok, _shared = _init_cluster_pair(tmp_root / "t1")

    cfg = pc_admin.ctx.sync_engine.load_cluster_config()
    assert cfg is not None
    assert cfg.cloud_backup_on_stop is False
    assert cfg.max_cloud_backups == 2

    # Put sample world data in runtime dir before hosting stop
    world_dir = pc_admin.ctx.mc_manager.runtime_dir / "world"
    world_dir.mkdir(parents=True, exist_ok=True)
    (world_dir / "level.dat").write_bytes(b"LEVELDATA-T1")

    st, start_res = pc_admin.request("POST", "/api/server/start", token=admin_token)
    assert st == 200, f"start failed: {start_res}"
    (world_dir / "session_marker.txt").write_text("saved-on-stop", encoding="utf-8")

    st, stop_res = pc_admin.request("POST", "/api/server/stop", token=admin_token)
    assert st == 200, f"stop failed: {stop_res}"

    local_zips = list(pc_admin.ctx.sync_engine.local_backups_dir.glob("*.zip"))
    cloud_zips = list(pc_admin.ctx.sync_engine.backups_dir.glob("*.zip"))
    assert len(local_zips) == 1, f"Expected 1 local stop backup, got {local_zips}"
    assert len(cloud_zips) == 0, f"Expected 0 cloud stop backups by default, got {cloud_zips}"

    st, bk_list_res = pc_admin.request("GET", "/api/admin/backups", token=admin_token)
    assert st == 200
    backups = bk_list_res["backups"]
    assert len(backups) == 1 and backups[0]["location"] == "local"
    diag = stop_res["environment"]["syncDiagnostics"]
    assert diag["localBackupCount"] == 1
    assert diag["cloudBackupCount"] == 0
    assert diag["maxDeltaChain"] == SyncEngine.MAX_DELTA_CHAIN == 25
    print("  [OK] Stop created fast local backup and kept Shared Cloud clean.")


def test_2_local_cloud_backup_api_and_prune(tmp_root: Path) -> None:
    print("\n[Test 2] Local vs Cloud manual backups, download/restore/delete, validation & prune-cloud...")
    pc_admin, admin_token, _pc_mem, _mem_tok, _shared = _init_cluster_pair(tmp_root / "t2")
    world_dir = pc_admin.ctx.mc_manager.runtime_dir / "world"
    world_dir.mkdir(parents=True, exist_ok=True)
    (world_dir / "marker.txt").write_text("v1-world", encoding="utf-8")

    # Create local backup via API
    st, r_loc = pc_admin.request(
        "POST",
        "/api/admin/backups",
        token=admin_token,
        body={"label": "loc_snap", "scope": "local"},
    )
    assert st == 200, r_loc
    assert not r_loc["result"].get("backupFile"), "Local backup must not advertise a cloud fallback backupFile"
    local_bks = [b for b in r_loc["backups"] if b["location"] == "local"]
    assert len(local_bks) == 1
    local_bk_name = local_bks[0]["name"]
    assert (pc_admin.ctx.sync_engine.local_backups_dir / local_bk_name).exists()

    # Modify world, then create cloud backup via API
    (world_dir / "marker.txt").write_text("v2-world", encoding="utf-8")
    st, r_cld = pc_admin.request(
        "POST",
        "/api/admin/backups",
        token=admin_token,
        body={"label": "cld_snap", "scope": "cloud"},
    )
    assert st == 200, r_cld
    cloud_bk_name = r_cld["result"]["backupFile"]
    assert cloud_bk_name
    assert (pc_admin.ctx.sync_engine.backups_dir / cloud_bk_name).exists()
    assert any(b["name"] == cloud_bk_name and b["location"] == "cloud" for b in r_cld["backups"])

    # Reject invalid scope/location & maxCloudBackups bounds
    st_bad, _ = pc_admin.request("POST", "/api/admin/backups", token=admin_token, body={"label": "x", "scope": "invalid"})
    assert st_bad == 400
    st_bad2, _ = pc_admin.request("PUT", "/api/admin/settings", token=admin_token, body={"maxCloudBackups": 25})
    assert st_bad2 == 400
    st_bad3, _ = pc_admin.request("POST", "/api/admin/backups/prune-cloud", token=admin_token, body={"keep": -1})
    assert st_bad3 == 400

    # Download both explicitly by location
    st_dl1, dl_loc_bytes = pc_admin.request(
        "GET", f"/api/admin/backups/download?name={local_bk_name}&location=local", token=admin_token, raw=True
    )
    assert st_dl1 == 200 and dl_loc_bytes[:2] == b"PK"
    st_dl2, dl_cld_bytes = pc_admin.request(
        "GET", f"/api/admin/backups/download?name={cloud_bk_name}&location=cloud", token=admin_token, raw=True
    )
    assert st_dl2 == 200 and dl_cld_bytes[:2] == b"PK"

    # Restore the local v1 backup -> runtime marker.txt reverts to v1-world
    st_rest, res_rest = pc_admin.request(
        "POST",
        "/api/admin/backups/restore",
        token=admin_token,
        body={"name": local_bk_name, "location": "local"},
    )
    assert st_rest == 200, res_rest
    assert (world_dir / "marker.txt").read_text(encoding="utf-8") == "v1-world"

    # Prune cloud backups (keep=0) -> cloud backup removed, local backup preserved
    st_pr, res_pr = pc_admin.request(
        "POST",
        "/api/admin/backups/prune-cloud",
        token=admin_token,
        body={"keep": 0},
    )
    assert st_pr == 200, res_pr
    assert res_pr["result"]["removedCount"] >= 1
    assert list(pc_admin.ctx.sync_engine.backups_dir.glob("*.zip")) == []
    names_left = {(b["name"], b["location"]) for b in res_pr["backups"]}
    assert (local_bk_name, "local") in names_left

    # Delete local backup with ?location=local
    st_del, res_del = pc_admin.request(
        "DELETE", f"/api/admin/backups/{local_bk_name}?location=local", token=admin_token
    )
    assert st_del == 200
    assert (local_bk_name, "local") not in {(b["name"], b["location"]) for b in res_del["backups"]}
    print("  [OK] Local/Cloud backup endpoints, restore, download, validation & prune-cloud verified.")


def test_3_noop_skip_delta_chain_and_hash_cache(tmp_root: Path) -> None:
    print("\n[Test 3] No-op push skip, MAX_DELTA_CHAIN=25 & archive SHA-256 cache...")
    pc_admin, _admin_token, _pc_mem, _mem_tok, _shared = _init_cluster_pair(tmp_root / "t3")
    se = pc_admin.ctx.sync_engine
    assert SyncEngine.MAX_DELTA_CHAIN == 25
    assert SyncEngine.MAX_DELTA_RATIO == 0.50

    world_dir = se.runtime_dir / "world"
    world_dir.mkdir(parents=True, exist_ok=True)
    (world_dir / "region.mca").write_bytes(b"CHUNK-DATA-A" * 200)

    # First push (creates base snapshot, local backup)
    res1 = se.push_world_from_runtime(create_backup=True, backup_label="init", host_username="AdminOne", backup_scope="local")
    assert res1["pushed"] is True
    seq1 = int(res1["snapshotSeq"])
    snap_hash1 = se._cached_archive_sha256(se.latest_snapshot_path)
    assert len(snap_hash1) == 64

    # Immediate second push with create_backup=False & NO file edits -> noop skip!
    res2 = se.push_world_from_runtime(create_backup=False, backup_label="auto-sync", host_username="AdminOne", backup_scope="none")
    assert res2["pushed"] is True
    assert res2.get("unchanged") is True
    assert res2.get("mode") == "noop"
    assert res2.get("uploadedBytes") == 0
    assert int(res2["snapshotSeq"]) == seq1

    # Verify _cached_archive_sha256 hits cache and invalidates on modification
    test_file = tmp_root / "cache_probe.bin"
    test_file.write_bytes(b"alpha" * 100)
    h1 = se._cached_archive_sha256(test_file)
    h2 = se._cached_archive_sha256(test_file)
    assert h1 == h2
    test_file.write_bytes(b"beta!" * 120)
    h3 = se._cached_archive_sha256(test_file)
    assert h3 != h1
    print("  [OK] No-op skip (0 B upload), delta chain limit 25, and archive hash cache verified.")


def test_4_immediate_save_on_sync_now_and_hardened_autosync(tmp_root: Path) -> None:
    print("\n[Test 4] Immediate save-on after staging, POST /api/server/sync-now, retry backoff & log regex...")
    pc_admin, admin_token, _pc_mem, _mem_tok, _shared = _init_cluster_pair(tmp_root / "t4")
    mm = pc_admin.ctx.mc_manager
    se = pc_admin.ctx.sync_engine

    # Put initial runtime world + acquire lock so live auto-sync thinks this PC is running host
    world_dir = se.runtime_dir / "world"
    world_dir.mkdir(parents=True, exist_ok=True)
    (world_dir / "level.dat").write_bytes(b"BASE-LIVE")
    admin_user = pc_admin.ctx.auth_service.require_auth(admin_token)
    se.acquire_lock(user=admin_user, mc_version="Paper 26.3", public_address="localhost:25565", initial_status="running")
    mm.local_status = "running"
    mm._reached_running = True
    mm.active_host_user = admin_user
    mm.server_ready_at = time.time() - 5.0

    # Broadened done_regex + save-complete detection via real _monitor_java_process
    class _FakeMonitorProc:
        def __init__(self, lines_text: str):
            self.stdout = io.StringIO(lines_text)
            self.stdin = None
            self.pid = 888888

        def poll(self):
            return 0

        def wait(self, timeout=None):
            return 0

    mm._save_complete_event.clear()
    mm._reached_running = False
    mm._proc = _FakeMonitorProc('[Server thread/INFO]: Done (2,45s)!\n[Server thread/INFO]: Saved the world\n')
    mm.local_status = "starting"
    mm._monitor_java_process(mm._proc, max_players=20, server_port=25565)
    # Re-assert after monitor sees Done & Saved the world lines: note exit code 0 ends monitor as crash->cleanup,
    # so assert _save_complete_event was set and logs recorded Done/Saved before restoring running test state.
    assert mm._save_complete_event.is_set(), "_monitor_java_process did not trigger _save_complete_event on 'Saved the world'"
    assert any("Done (2,45s)!" in entry.get("text", "") for entry in mm.console_logs)

    # Restore running mock host state for live auto-sync test
    se.acquire_lock(user=admin_user, mc_version="Paper 26.3", public_address="localhost:25565", initial_status="running")
    mm.local_status = "running"
    mm._reached_running = True
    mm.active_host_user = admin_user
    mm.server_ready_at = time.time() - 5.0

    # Trace exact order: save-off -> save-all flush -> save-on (before cloud transfer!)
    events: List[str] = []

    def _on_cmd(cmd: str) -> None:
        if cmd == "save-all flush":
            mm._save_complete_event.set()

    fake_stdin = _FakeStdin(events, on_write=_on_cmd)
    mm._proc = _FakeRunningProc(fake_stdin)

    orig_move = se._move_or_copy_to_cloud

    def tracked_move(src: Path, dst: Path, *args, **kwargs):
        events.append("cloud-write")
        return orig_move(src, dst, *args, **kwargs)

    se._move_or_copy_to_cloud = tracked_move
    try:
        (world_dir / "live_build.txt").write_text("live-chunk-update", encoding="utf-8")
        st_sync, res_sync = pc_admin.request("POST", "/api/server/sync-now", token=admin_token)
        assert st_sync == 200, f"sync-now failed: {res_sync}"
        assert res_sync["result"]["pushed"] is True, f"Expected pushed=True, got: {res_sync}"
        assert res_sync["autoSync"]["syncCount"] == 1
        assert "stdin:save-off" in events
        assert "stdin:save-on" in events
        assert "cloud-write" in events
        assert events.index("stdin:save-on") < events.index("cloud-write"), (
            f"Expected immediate save-on before cloud upload, got order: {events}"
        )

        # Also test flush_and_backup_live_world(scope='local') fires immediate save-on
        events.clear()
        (world_dir / "live_build2.txt").write_text("live-backup-update", encoding="utf-8")
        bk_res = mm.flush_and_backup_live_world(label="live_test", host_username="AdminOne", scope="local")
        assert bk_res.get("pushed") is True and not bk_res.get("backupFile")
        assert len(list(se.local_backups_dir.glob("*live_test*.zip"))) == 1
        assert len(list(se.backups_dir.glob("*live_test*.zip"))) == 0
        assert "stdin:save-on" in events and "cloud-write" in events
        assert events.index("stdin:save-on") < events.index("cloud-write")
    finally:
        se._move_or_copy_to_cloud = orig_move
        mm._proc = None
        mm.local_status = "idle"
        mm._reached_running = False
        se.release_lock(admin_user)

    # Verify exponential retry backoff progression (60s -> 120s -> 240s capped by interval)
    assert mm._auto_sync_retry_backoff_sec(1, 10) == 60.0
    assert mm._auto_sync_retry_backoff_sec(2, 10) == 120.0
    assert mm._auto_sync_retry_backoff_sec(3, 10) == 240.0
    assert mm._auto_sync_retry_backoff_sec(3, 2) == 120.0
    print("  [OK] Immediate save-on before cloud write, POST /api/server/sync-now & backoff verified.")


def test_5_sync_progress_and_passive_drive_mirroring(tmp_root: Path) -> None:
    print("\n[Test 5] Real-time syncProgress callback, passive .tmp.drivedownload & remote mirroring...")
    pc_admin, admin_token, pc_member, member_token, shared_dir = _init_cluster_pair(tmp_root / "t5")
    se_admin = pc_admin.ctx.sync_engine

    # Capture progress events emitted during a push & pull
    seen_progress: List[Dict[str, Any]] = []
    orig_cb = se_admin.progress_callback

    def spy_cb(prog: Dict[str, Any]) -> None:
        seen_progress.append(dict(prog))
        if orig_cb:
            orig_cb(prog)

    se_admin.progress_callback = spy_cb
    world_dir = se_admin.runtime_dir / "world"
    world_dir.mkdir(parents=True, exist_ok=True)
    for idx in range(5):
        (world_dir / f"chunk_{idx}.bin").write_bytes(os.urandom(16 * 1024))

    push_res = se_admin.push_world_from_runtime(create_backup=False, host_username="AdminOne", backup_scope="none")
    assert push_res["pushed"] is True
    active_events = [p for p in seen_progress if p.get("active")]
    assert len(active_events) >= 2, f"Expected progress updates during push, got {seen_progress}"
    pcts = [int(p.get("percent", 0)) for p in active_events]
    assert max(pcts) == 100 and pcts == sorted(pcts), f"Expected monotonic progress reaching 100%, got {pcts}"
    assert se_admin.get_sync_progress()["active"] is False

    # Simulate passive Google Drive transfer in deltas/.tmp.drivedownload/*.part AND delta-mode base download %
    (world_dir / "tiny_delta.txt").write_text("delta-1", encoding="utf-8")
    se_admin.push_world_from_runtime(create_backup=False, host_username="AdminOne", backup_scope="none")
    base_sz = se_admin.latest_snapshot_path.stat().st_size

    delta_dl_tmp = se_admin.deltas_dir / ".tmp.drivedownload"
    delta_dl_tmp.mkdir(parents=True, exist_ok=True)
    (delta_dl_tmp / "delta-part.tmp").write_bytes(b"D" * 256)
    try:
        st_d, d_res = pc_member.request("GET", "/api/setup/status")
        assert st_d == 200 and d_res["syncProgress"]["active"] is True
        assert d_res["syncProgress"]["operation"] == "cloud_transfer"
    finally:
        for f in delta_dl_tmp.iterdir():
            f.unlink(missing_ok=True)
        delta_dl_tmp.rmdir()

    # Base zip download larger than tiny delta must still resolve % against baseSizeBytes!
    dl_tmp = shared_dir / ".tmp.drivedownload"
    dl_tmp.mkdir(parents=True, exist_ok=True)
    (dl_tmp / "latest-server.zip.part").write_bytes(b"Z" * max(8192, base_sz // 2))
    try:
        st_setup, setup_res = pc_member.request("GET", "/api/setup/status")
        assert st_setup == 200
        sp = setup_res["syncProgress"]
        assert sp["active"] is True
        assert sp["operation"] == "cloud_transfer"
        assert sp["phase"] == "downloading"
        assert sp["indeterminate"] is False and 30 <= sp["percent"] <= 70, f"Expected ~50% base download progress, got {sp}"
        assert "downloading" in sp["detail"].lower()

        st_status, status_res = pc_member.request("GET", "/api/server/status", token=member_token)
        assert st_status == 200
        assert status_res["syncProgress"]["active"] is True
    finally:
        for f in dl_tmp.iterdir():
            f.unlink(missing_ok=True)
        dl_tmp.rmdir()

    # After passive transfer clears, verify remote host's active syncProgress mirrors onto Member status summary
    admin_user = pc_admin.ctx.auth_service.require_auth(admin_token)
    se_admin.acquire_lock(user=admin_user, mc_version="Paper 26.3", public_address="", initial_status="syncing_pull")
    try:
        se_admin._set_sync_progress(
            active=True,
            operation="pull",
            phase="extracting",
            label="Pulling Latest World...",
            detail="Extracting world files...",
            percent=64,
            done_files=32,
            total_files=50,
        )
        pc_admin.ctx.mc_manager.local_status = "syncing_pull"
        tel = pc_admin.ctx.mc_manager._build_telemetry_snapshot()
        se_admin.write_host_telemetry(tel)

        st_mem, mem_status = pc_member.request("GET", "/api/server/status", token=member_token)
        assert st_mem == 200
        assert mem_status["syncProgress"]["active"] is True
        assert mem_status["syncProgress"]["percent"] == 64
        assert mem_status["syncProgress"]["phase"] == "extracting"
    finally:
        se_admin._set_sync_progress(active=False)
        pc_admin.ctx.mc_manager.local_status = "idle"
        se_admin.release_lock(admin_user)
    print("  [OK] Real-time syncProgress, passive Drive download progress & remote mirroring verified.")


def test_6_frontend_elements_and_fixes() -> None:
    print("\n[Test 6] Frontend HTML/CSS/JS presence and sm.deltaChain fix checks...")
    html = (ROOT_DIR / "public" / "index.html").read_text(encoding="utf-8")
    css = (ROOT_DIR / "public" / "styles.css").read_text(encoding="utf-8")
    js = (ROOT_DIR / "public" / "app.js").read_text(encoding="utf-8")

    for dom_id in (
        "sync-progress-banner",
        "auth-sync-progress",
        "btn-sync-now",
        "input-backup-scope",
        "btn-prune-cloud-backups",
        "net-cloud-backup-on-stop",
        "net-max-cloud-backups",
    ):
        assert f'id="{dom_id}"' in html, f"Missing id='{dom_id}' in public/index.html"
        assert dom_id in js, f"Missing reference to '{dom_id}' in public/app.js"

    assert ".sync-progress-fill.indeterminate" in css
    assert "syncIndeterminateSlide" in css
    assert 'payload.event === "sync_progress"' in js
    assert "startAuthSyncPolling" in js
    assert "buildEffectiveSyncProgress" in js
    assert "Live console mirrored from" in js
    assert "sm.deltaChain" not in js and "deltaChain" not in js, "Stale sm.deltaChain reference still present in public/app.js!"
    assert "Array.isArray(smRaw.deltas)" in js or "Array.isArray(sm.deltas)" in js
    print("  [OK] All frontend progress bar & backup control checks passed.")


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp_str:
        tmp_root = Path(tmp_str)
        test_1_default_local_stop_backups(tmp_root)
        test_2_local_cloud_backup_api_and_prune(tmp_root)
        test_3_noop_skip_delta_chain_and_hash_cache(tmp_root)
        test_4_immediate_save_on_sync_now_and_hardened_autosync(tmp_root)
        test_5_sync_progress_and_passive_drive_mirroring(tmp_root)
        test_6_frontend_elements_and_fixes()
    print("\nALL FAST-SYNC, PROGRESS BAR & LOCAL BACKUP TESTS PASSED!")


if __name__ == "__main__":
    main()
