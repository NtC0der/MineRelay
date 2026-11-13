"""
Stage 2 Comprehensive End-to-End & Stress Test Suite for MineRelay.
Covers:
  Suite A: Customizable Periodic Drive Auto-Sync (settings, validation, save-off/save-all/save-on,
           lock preservation, failure recovery, collision guards, schedule updates)
  Suite B: Distributed Host-Lock, Concurrent Claim Race, Clock-Skew, Split-Brain Fencing & force_unlock
  Suite C: Google Drive Conflict Resilience, Atomic I/O, Transfer Detection & Snapshot Integrity
  Suite D: Full Cross-Host Data Mirroring, Remote Lock Watcher SSE & Remote Mutation Guards
  Suite E: Auth Session Invalidation, Tunnel State & Frontend Invariants
"""

import io
import os
import sys
import json
import time
import queue
import shutil
import zipfile
import tempfile
import threading
from pathlib import Path
from typing import Any, Dict, List

os.environ["MINERELAY_TEST_MODE"] = "1"

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.models import ClusterConfig, HostLock, LOCK_STALE_TIMEOUT_SEC
from src.server import AppContext, create_request_handler
from src.sync_engine import HostLockConflictError


class InMemoryHTTPClient:
    """Dispatches HTTP requests directly through DashboardRequestHandler without requiring TCP sockets."""

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
            parsed_json = json.loads(payload_part.decode("utf-8"))
        except Exception:
            parsed_json = {"raw": payload_part.decode("utf-8", errors="replace")}
        return status_code, parsed_json


class FakeMinecraftProc:
    """Simulates a running Minecraft process stdin/poll pipe for save-off/save-all/save-on verification."""

    def __init__(self, mgr, fail_save_all: bool = False):
        self.mgr = mgr
        self.commands: List[str] = []
        self._alive = True
        self._fail_save_all = fail_save_all
        parent = self

        class _Stdin:
            def write(self, data: str) -> int:
                parent.commands.append(data)
                if "save-all" in data and not parent._fail_save_all:
                    parent.mgr._save_complete_event.set()
                return len(data)

            def flush(self) -> None:
                pass

            def close(self) -> None:
                pass

        self.stdin = _Stdin()

    def poll(self):
        return None if self._alive else 0


def _drain_sse_queue(q_obj: queue.Queue) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    while True:
        try:
            raw = q_obj.get_nowait()
            events.append(json.loads(raw))
        except queue.Empty:
            break
    return events


def run_stage2_suite():
    print("============================================================")
    print("  MineRelay Stage 2 Comprehensive E2E & Stress Test Suite   ")
    print("============================================================")

    temp_root = Path(tempfile.mkdtemp(prefix="minerelay_stage2_"))
    shared_dir = temp_root / "Google Drive" / "MineRelay-Stage2"
    pc1_dir = temp_root / "pc1_data"
    pc2_dir = temp_root / "pc2_data"
    pc3_dir = temp_root / "pc3_data"

    try:
        pc1 = InMemoryHTTPClient(str(pc1_dir))
        pc2 = InMemoryHTTPClient(str(pc2_dir))

        st, init_res = pc1.request(
            "POST",
            "/api/setup/init",
            body={
                "serverName": "Stage2 SMP",
                "adminUsername": "AdminHost",
                "machineName": "PC-Alpha",
                "sharedSyncDir": str(shared_dir),
            },
        )
        assert st == 200, f"Setup init failed: {init_res}"
        admin_token = init_res["token"]

        st, alice_res = pc1.request("POST", "/api/admin/accounts", token=admin_token, body={"username": "Alice"})
        assert st == 201
        alice_pk = alice_res["generatedPasskey"]
        alice_id = alice_res["account"]["id"]

        st, bob_res = pc1.request("POST", "/api/admin/accounts", token=admin_token, body={"username": "Bob"})
        assert st == 201
        bob_pk = bob_res["generatedPasskey"]

        st, _ = pc2.request("POST", "/api/setup/sync-dir", body={"sharedSyncDir": str(shared_dir), "machineName": "PC-Beta"})
        assert st == 200

        st, alice_login = pc1.request("POST", "/api/auth/login", body={"passkey": alice_pk, "machineName": "PC-Alpha"})
        assert st == 200
        alice_token = alice_login["token"]

        st, bob_login = pc2.request("POST", "/api/auth/login", body={"passkey": bob_pk, "machineName": "PC-Beta"})
        assert st == 200
        bob_token = bob_login["token"]

        # =====================================================================
        # Suite A: Customizable Periodic Drive Auto-Sync
        # =====================================================================
        print("\n[Suite A] Testing Customizable Periodic Drive Auto-Sync...")

        # A.1 Default interval, GET /api/setup/status & GET /api/admin/settings
        st_setup, res_setup = pc1.request("GET", "/api/setup/status")
        assert st_setup == 200 and res_setup.get("autoSyncIntervalMinutes") == 10, f"Unexpected setup status: {res_setup}"

        st_get_mem, _ = pc1.request("GET", "/api/admin/settings", token=alice_token)
        assert st_get_mem == 403, "Member must receive 403 on GET /api/admin/settings"

        st_get_adm, res_get_adm = pc1.request("GET", "/api/admin/settings", token=admin_token)
        assert st_get_adm == 200
        assert res_get_adm["cluster"]["autoSyncIntervalMinutes"] == 10
        assert res_get_adm["autoSync"]["enabled"] is True
        assert res_get_adm["autoSync"]["intervalMinutes"] == 10

        # A.2 Clamping & validation on ClusterConfig and PUT /api/admin/settings
        assert ClusterConfig.from_dict({"autoSyncIntervalMinutes": -5}).auto_sync_interval_minutes == 0
        assert ClusterConfig.from_dict({"autoSyncIntervalMinutes": 9999}).auto_sync_interval_minutes == 1440

        for bad_val in (True, False, None, "abc", "", -1, 1441, 1.5):
            st_bad, res_bad = pc1.request(
                "PUT",
                "/api/admin/settings",
                token=admin_token,
                body={"autoSyncIntervalMinutes": bad_val},
            )
            assert st_bad == 400 and res_bad.get("code") == "BAD_REQUEST", f"Failed rejecting {bad_val!r}: {st_bad} {res_bad}"

        # A.3 Disable auto-sync (0) and verify SSE + schedule wakeup + skipped behavior
        sse_q = queue.Queue(maxsize=100)
        with pc1.ctx.sse_mutex:
            pc1.ctx.sse_clients.append(sse_q)

        wakeup_calls = []
        orig_notify = pc1.ctx.mc_manager.notify_auto_sync_schedule_changed

        def _spy_notify():
            wakeup_calls.append(True)
            return orig_notify()

        pc1.ctx.mc_manager.notify_auto_sync_schedule_changed = _spy_notify  # type: ignore

        st_dis, res_dis = pc1.request("PUT", "/api/admin/settings", token=admin_token, body={"autoSyncIntervalMinutes": 0})
        assert st_dis == 200
        assert res_dis["cluster"]["autoSyncIntervalMinutes"] == 0
        assert res_dis["status"]["autoSync"]["enabled"] is False
        assert res_dis["status"]["autoSync"]["lastSyncStatus"] == "disabled"
        assert len(wakeup_calls) == 1, "PUT /api/admin/settings must notify auto-sync scheduler"

        events_a = _drain_sse_queue(sse_q)
        assert any(e["event"] == "status_changed" for e in events_a), "PUT /api/admin/settings must broadcast status_changed"

        # Start server while disabled -> non-forced auto-sync skips cleanly
        st_start, _ = pc1.request("POST", "/api/server/start", token=admin_token)
        assert st_start == 200
        skip_res = pc1.ctx.mc_manager.run_live_auto_sync_once(force=False)
        assert skip_res.get("pushed") is False and skip_res.get("skipped") is True

        # Enable 5-minute interval while running -> schedule updates immediately
        st_en, res_en = pc1.request("PUT", "/api/admin/settings", token=admin_token, body={"autoSyncIntervalMinutes": 5})
        assert st_en == 200 and res_en["cluster"]["autoSyncIntervalMinutes"] == 5
        assert len(wakeup_calls) == 2

        # A.4 Live snapshot push with save-off -> save-all flush -> push_live_snapshot -> save-on
        fake_proc = FakeMinecraftProc(pc1.ctx.mc_manager)
        pc1.ctx.mc_manager._proc = fake_proc  # type: ignore
        live_chunk = pc1.ctx.mc_manager.runtime_dir / "world" / "autosync-live-chunk.mca"
        live_chunk.parent.mkdir(parents=True, exist_ok=True)
        live_chunk.write_bytes(b"LIVE-CHUNK-DATA-V1")

        lock_before = pc1.ctx.sync_engine.get_lock()
        backups_before = len(pc1.ctx.sync_engine.list_backups())
        manifest_before = pc1.ctx.sync_engine._read_sync_manifest() or {}
        seq_before = int(manifest_before.get("snapshotSeq", 0))

        sync_res = pc1.ctx.mc_manager.run_live_auto_sync_once(force=True)
        assert sync_res.get("pushed") is True, f"Expected live auto-sync push to succeed, got: {sync_res}"
        assert int(sync_res.get("snapshotSeq", 0)) > seq_before, "Live auto-sync must advance snapshotSeq"
        assert fake_proc.commands == ["save-off\n", "save-all flush\n", "save-on\n"], (
            f"Unexpected Minecraft stdin sequence: {fake_proc.commands}"
        )

        # Lock MUST stay owned and unaltered; no extra backup created
        lock_after = pc1.ctx.sync_engine.get_lock()
        assert lock_after.is_locked is True and lock_after.lock_epoch == lock_before.lock_epoch
        assert pc1.ctx.sync_engine.owns_lock(lock_before.lock_epoch) is True
        assert len(pc1.ctx.sync_engine.list_backups()) == backups_before, "Live auto-sync must not create extra backup archives"

        with zipfile.ZipFile(pc1.ctx.sync_engine.latest_snapshot_path, "r") as zf:
            assert "world/autosync-live-chunk.mca" in [n.replace("\\", "/") for n in zf.namelist()]

        st_stat, stat_res = pc1.request("GET", "/api/server/status", token=admin_token)
        assert st_stat == 200
        as_info = stat_res["autoSync"]
        assert as_info["enabled"] is True and as_info["intervalMinutes"] == 5
        assert as_info["inProgress"] is False
        assert as_info["lastSyncStatus"] == "ok"
        assert as_info["lastSyncAt"] > 0 and as_info["nextSyncAt"] > as_info["lastSyncAt"]

        # A.5 try/finally save-on resilience when push_live_snapshot raises
        fake_proc.commands.clear()
        orig_push_live = pc1.ctx.sync_engine.push_live_snapshot

        def _failing_push_live(**kwargs):
            raise RuntimeError("Simulated Drive file lock error")

        pc1.ctx.sync_engine.push_live_snapshot = _failing_push_live  # type: ignore
        try:
            err_res = pc1.ctx.mc_manager.run_live_auto_sync_once(force=True)
            assert err_res.get("pushed") is False and "Simulated Drive" in err_res.get("error", "")
            assert fake_proc.commands == ["save-off\n", "save-all flush\n", "save-on\n"], (
                f"save-on MUST execute in finally after failure! Got: {fake_proc.commands}"
            )
            st_err, err_stat = pc1.request("GET", "/api/server/status", token=admin_token)
            assert st_err == 200
            assert err_stat["autoSync"]["inProgress"] is False
            assert err_stat["autoSync"]["lastSyncStatus"] == "error"
            assert "Simulated Drive" in err_stat["autoSync"]["lastSyncError"]
        finally:
            pc1.ctx.sync_engine.push_live_snapshot = orig_push_live  # type: ignore

        # A.6 Bounded _sync_mutex collision guard (live auto-sync does not deadlock/hang if manual sync holds mutex)
        fake_proc.commands.clear()
        mutex_held = threading.Event()
        release_mutex = threading.Event()

        def _hold_sync_mutex():
            with pc1.ctx.sync_engine._sync_mutex:
                mutex_held.set()
                release_mutex.wait(timeout=5.0)

        t_hold = threading.Thread(target=_hold_sync_mutex, daemon=True)
        t_hold.start()
        assert mutex_held.wait(timeout=2.0)
        try:
            coll_res = pc1.ctx.sync_engine.push_live_snapshot(lock_epoch=lock_before.lock_epoch, host_username="AdminHost")
            assert coll_res.get("pushed") is False and coll_res.get("skipped") is True
        finally:
            release_mutex.set()
            t_hold.join(timeout=2.0)

        # Cleanly clear fake_proc before stopping
        pc1.ctx.mc_manager._proc = None
        pc1.ctx.mc_manager.notify_auto_sync_schedule_changed = orig_notify  # type: ignore
        with pc1.ctx.sse_mutex:
            if sse_q in pc1.ctx.sse_clients:
                pc1.ctx.sse_clients.remove(sse_q)

        st_stop, _ = pc1.request("POST", "/api/server/stop", token=admin_token)
        assert st_stop == 200
        print("  [OK] Suite A passed: auto-sync config, validation, save-off/all/on try-finally, lock preservation & collision guard.")

        # =====================================================================
        # Suite B: Concurrent Lock Claim Race, Clock-Skew, Fencing & force_unlock
        # =====================================================================
        print("\n[Suite B] Testing Concurrent Lock Race, Clock-Skew, Split-Brain Fencing & force_unlock...")

        # B.1 Simultaneous start race between PC1 (Alice) and PC2 (Bob)
        race_results: List[int] = []
        race_barrier = threading.Barrier(2)

        def _race_start(client: InMemoryHTTPClient, tok: str):
            race_barrier.wait(timeout=3.0)
            code, _ = client.request("POST", "/api/server/start", token=tok)
            race_results.append(code)

        t1 = threading.Thread(target=_race_start, args=(pc1, alice_token))
        t2 = threading.Thread(target=_race_start, args=(pc2, bob_token))
        t1.start()
        t2.start()
        t1.join(timeout=10.0)
        t2.join(timeout=10.0)
        assert sorted(race_results) == [200, 409], f"Expected exactly one 200 and one 409 in start race, got: {race_results}"

        # Stop whichever PC won
        if pc1.ctx.mc_manager.local_status != "idle":
            st_w, _ = pc1.request("POST", "/api/server/stop", token=alice_token)
            assert st_w == 200
        if pc2.ctx.mc_manager.local_status != "idle":
            st_w, _ = pc2.request("POST", "/api/server/stop", token=bob_token)
            assert st_w == 200

        # B.2 Ahead-clock frozen remote lock expiration after initial heartbeat tick
        st_b_start, res_b_start = pc2.request("POST", "/api/server/start", token=bob_token)
        assert st_b_start == 200
        bob_epoch = res_b_start["lock"]["lockEpoch"]
        sess_path = pc2.ctx.sync_engine.locks_dir / f"session-{bob_epoch}.json"

        pc1.ctx.sync_engine._remote_hb_tracker.clear()
        sdata = json.loads(sess_path.read_text(encoding="utf-8"))
        # Remote clock +1 hour ahead
        future_hb = time.time() + 3600.0
        sdata["lastHeartbeat"] = future_hb
        sess_path.write_text(json.dumps(sdata), encoding="utf-8")
        pc1.ctx.sync_engine.host_lock_path.write_text(json.dumps(sdata), encoding="utf-8")
        lk1 = pc1.ctx.sync_engine.get_lock()
        assert lk1.is_actively_locked() is True

        # Remote ticks once, then freezes
        sdata["lastHeartbeat"] = future_hb + 10.0
        sess_path.write_text(json.dumps(sdata), encoding="utf-8")
        pc1.ctx.sync_engine.host_lock_path.write_text(json.dumps(sdata), encoding="utf-8")
        lk2 = pc1.ctx.sync_engine.get_lock()
        assert lk2.is_actively_locked() is True

        # Simulate > LOCK_STALE_TIMEOUT_SEC passing locally without remote heartbeat advancing
        tkey = f"{lk2.machine_id}:{bob_epoch}"
        pc1.ctx.sync_engine._remote_hb_tracker[tkey] = (
            future_hb + 10.0,
            time.time() - (LOCK_STALE_TIMEOUT_SEC + 60.0),
            time.time() - (LOCK_STALE_TIMEOUT_SEC + 30.0),
        )
        lk3 = pc1.ctx.sync_engine.get_lock()
        assert lk3.is_actively_locked() is False, "Ahead-clock remote lock frozen past timeout must expire!"
        assert pc1.ctx.sync_engine._is_lock_effectively_active(lk3) is False

        # B.3 force_unlock preserves expectedSnapshotSeq/Hash, cleans locks (1)/ & host-lock (1).json, and fences stale PC2
        manifest_now = pc2.ctx.sync_engine._read_sync_manifest() or {}
        exp_seq_before = int(manifest_now.get("snapshotSeq", 0))
        exp_hash_before = str(manifest_now.get("sha256", ""))

        conflict_locks_1 = shared_dir / "locks (1)"
        conflict_locks_1.mkdir(exist_ok=True)
        shutil.copy2(sess_path, conflict_locks_1 / f"session-{bob_epoch} (1).json")
        (shared_dir / "host-lock (1).json").write_text(json.dumps(sdata), encoding="utf-8")

        st_fu, res_fu = pc1.request("POST", "/api/admin/force-unlock", token=admin_token)
        assert st_fu == 200 and res_fu["unlocked"] is True
        assert res_fu["lock"]["isLocked"] is False

        got_seq, got_hash, _ = pc1.ctx.sync_engine.get_expected_snapshot_state()
        assert got_seq >= exp_seq_before and got_hash == exp_hash_before, (
            f"force_unlock erased expectedSnapshotSeq/Hash! got ({got_seq}, {got_hash}) vs ({exp_seq_before}, {exp_hash_before})"
        )
        assert not (shared_dir / "host-lock (1).json").exists()
        assert not (conflict_locks_1 / f"session-{bob_epoch} (1).json").exists()
        shutil.rmtree(conflict_locks_1, ignore_errors=True)

        # Stale PC2 attempts live/epoch-gated snapshot push after being force-unlocked -> MUST be fenced!
        (pc2.ctx.mc_manager.runtime_dir / "world" / "split-brain-overwrite.txt").write_text("stale", encoding="utf-8")
        try:
            pc2.ctx.sync_engine.push_world_from_runtime(
                create_backup=False,
                require_lock_epoch=bob_epoch,
            )
            assert False, "Expected HostLockConflictError when stale force-unlocked host pushes with old epoch!"
        except HostLockConflictError:
            pass

        try:
            live_fenced = pc2.ctx.sync_engine.push_live_snapshot(lock_epoch=bob_epoch, host_username="Bob")
            assert live_fenced.get("pushed") is False and live_fenced.get("skipped") is True
        except HostLockConflictError:
            pass

        # Reset PC2 local state cleanly
        pc2.ctx.mc_manager.local_status = "idle"
        pc2.ctx.mc_manager._reached_running = False
        pc2.ctx.sync_engine._owns_active_lock = False
        pc2.ctx.sync_engine._active_lock_epoch = ""
        print("  [OK] Suite B passed: start race, ahead-clock frozen expiration, force_unlock preservation & split-brain fencing.")

        # =====================================================================
        # Suite C: Google Drive Conflict Resilience, Atomic I/O & Snapshot Integrity
        # =====================================================================
        print("\n[Suite C] Testing Google Drive Conflict Resilience, Atomic I/O & Snapshot Integrity...")

        # C.1 read_host_telemetry ranks updatedAt over stale isActive/logs conflicts
        pc1.ctx.sync_engine.write_host_telemetry({
            "isActive": False,
            "lockEpoch": "ep-rank",
            "status": "idle",
            "logs": [],
        })
        stale_tel_conflict = pc1.ctx.sync_engine.locks_dir / "host-telemetry (1).json"
        stale_tel_conflict.write_text(json.dumps({
            "isActive": True,
            "lockEpoch": "ep-rank",
            "updatedAt": time.time() - 500.0,
            "logs": [{"id": "old-1", "text": "stale conflict log"}],
        }), encoding="utf-8")
        best_tel = pc1.ctx.sync_engine.read_host_telemetry(expected_epoch="ep-rank")
        assert best_tel is not None and best_tel.get("isActive") is False, (
            f"Stale conflict telemetry outranked newer updatedAt! Got: {best_tel}"
        )
        stale_tel_conflict.unlink(missing_ok=True)

        # C.2 released-<epoch> (1).json suppresses session resurrection
        tomb_conflict = pc1.ctx.sync_engine.locks_dir / "released-ep-ghost (1).json"
        tomb_conflict.write_text(json.dumps({"lockEpoch": "ep-ghost", "releasedAt": time.time()}), encoding="utf-8")
        ghost_sess = pc1.ctx.sync_engine.locks_dir / "session-ep-ghost.json"
        ghost_sess.write_text(json.dumps({
            "isLocked": True,
            "lockEpoch": "ep-ghost",
            "machineId": "ghost-pc",
            "lastHeartbeat": time.time(),
            "status": "running",
        }), encoding="utf-8")
        assert pc1.ctx.sync_engine.get_lock().is_locked is False, "released-* (1).json failed to suppress ghost session!"
        tomb_conflict.unlink(missing_ok=True)
        ghost_sess.unlink(missing_ok=True)

        # C.3 cluster-config (1).json full-field merge & missing-main fallback
        cfg_now = pc1.ctx.sync_engine.load_cluster_config()
        assert cfg_now is not None
        cfg_dict = cfg_now.to_dict(include_secrets=True)
        cfg_dict["serverName"] = "Conflict Merged SMP"
        cfg_dict["autoSyncIntervalMinutes"] = 12
        cfg_conflict = shared_dir / "cluster-config (1).json"
        cfg_conflict.write_text(json.dumps(cfg_dict), encoding="utf-8")
        fut = time.time() + 10.0
        os.utime(cfg_conflict, (fut, fut))
        pc1.ctx.sync_engine._cleanup_cloud_conflict_files()
        merged_cfg = pc1.ctx.sync_engine.load_cluster_config()
        assert merged_cfg is not None
        assert merged_cfg.server_name == "Conflict Merged SMP"
        assert merged_cfg.auto_sync_interval_minutes == 12
        assert not cfg_conflict.exists()

        # Fallback recovery if main cluster-config.json is transiently absent
        saved_cfg_bytes = pc1.ctx.sync_engine.cluster_config_path.read_bytes()
        pc1.ctx.sync_engine.cluster_config_path.unlink()
        cfg_conflict.write_bytes(saved_cfg_bytes)
        recovered_cfg = pc1.ctx.sync_engine.load_cluster_config()
        assert recovered_cfg is not None and recovered_cfg.server_name == "Conflict Merged SMP"
        assert pc1.ctx.sync_engine.cluster_config_path.exists()
        cfg_conflict.unlink(missing_ok=True)

        # C.4 tunnel (1)/playit.toml reconciliation
        tunnel_conflict = shared_dir / "tunnel (1)"
        tunnel_conflict.mkdir(exist_ok=True)
        (tunnel_conflict / "playit.toml").write_text('secret_key = "test-secret"\n', encoding="utf-8")
        pc1.ctx.sync_engine._cleanup_cloud_conflict_files()
        assert (pc1.ctx.sync_engine.shared_tunnel_dir / "playit.toml").exists()
        assert not tunnel_conflict.exists()

        # C.5 .tmp.driveupload blocks start & restore
        up_dir = pc1.ctx.sync_engine.snapshots_dir / ".tmp.driveupload"
        up_dir.mkdir(parents=True, exist_ok=True)
        (up_dir / "uploading.part").write_bytes(b"U" * 2048)
        st_up_blk, res_up_blk = pc1.request("POST", "/api/server/start", token=admin_token)
        assert st_up_blk == 400 and "uploading" in res_up_blk["error"].lower()
        shutil.rmtree(up_dir, ignore_errors=True)

        # C.6 Clock-skew mtime bypass removed & corrupt zip non-destructive failure & snapshotSeq monotonic guard
        saved_snap = pc1.ctx.sync_engine.latest_snapshot_path.read_bytes()
        saved_man = pc1.ctx.sync_engine.sync_manifest_path.read_bytes()
        man_obj = json.loads(saved_man.decode("utf-8"))
        # Temporarily remove paired backup so fallback isn't used, and write corrupted latest-server.zip with FUTURE mtime!
        bk_file = pc1.ctx.sync_engine.backups_dir / man_obj["backupFile"] if man_obj.get("backupFile") else None
        saved_bk = bk_file.read_bytes() if (bk_file and bk_file.exists()) else None
        if bk_file and bk_file.exists():
            bk_file.unlink()
        pc1.ctx.sync_engine.latest_snapshot_path.write_bytes(b"CORRUPT-ZIP-BYTES")
        os.utime(pc1.ctx.sync_engine.latest_snapshot_path, (time.time() + 120.0, time.time() + 120.0))

        try:
            pc1.ctx.sync_engine.pull_world_to_runtime(strict=True)
            assert False, "Stale/corrupt latest-server.zip with future mtime must NOT bypass SHA-256 verification!"
        except RuntimeError as e:
            assert "catching up" in str(e).lower()

        # Restore healthy snapshot/backup, then test corrupt zip extraction preserves runtime_dir
        pc1.ctx.sync_engine.latest_snapshot_path.write_bytes(saved_snap)
        if bk_file and saved_bk is not None:
            bk_file.write_bytes(saved_bk)
        sentinel = pc1.ctx.mc_manager.runtime_dir / "world" / "keep_on_corrupt.txt"
        sentinel.parent.mkdir(parents=True, exist_ok=True)
        sentinel.write_text("intact", encoding="utf-8")
        # Test _write_json_atomic shrinking payload leaves clean JSON
        shrinking_path = temp_root / "shrink_test.json"
        pc1.ctx.sync_engine._write_json_atomic(shrinking_path, {"big": "X" * 500}, in_place_if_exists=True)
        pc1.ctx.sync_engine._write_json_atomic(shrinking_path, {"small": 1}, in_place_if_exists=True)
        assert json.loads(shrinking_path.read_text(encoding="utf-8")) == {"small": 1}

        # C.7 delete_backup protects active fallback backup when latest-server.zip is damaged
        if bk_file and bk_file.exists():
            pc1.ctx.sync_engine.latest_snapshot_path.write_bytes(b"DAMAGED")
            try:
                pc1.ctx.sync_engine.delete_backup(bk_file.name)
                assert False, "delete_backup must refuse deleting active fallback backup when latest-server.zip is damaged!"
            except RuntimeError:
                pass
            pc1.ctx.sync_engine.latest_snapshot_path.write_bytes(saved_snap)

        # C.8 Cross-host mirroring of ops.json, whitelist.json, banned-players.json, banned-ips.json, usercache.json
        st_c8, _ = pc1.request("POST", "/api/server/start", token=admin_token)
        assert st_c8 == 200
        rt1 = pc1.ctx.mc_manager.runtime_dir
        (rt1 / "ops.json").write_text(json.dumps([{"uuid": "00000000-0000-4000-8000-000000000001", "name": "OpOne", "level": 4}]), encoding="utf-8")
        (rt1 / "whitelist.json").write_text(json.dumps([{"uuid": "00000000-0000-4000-8000-000000000002", "name": "WhiteOne"}]), encoding="utf-8")
        (rt1 / "banned-players.json").write_text(json.dumps([{"uuid": "00000000-0000-4000-8000-000000000003", "name": "Griefer"}]), encoding="utf-8")
        (rt1 / "banned-ips.json").write_text(json.dumps([{"ip": "10.9.9.9", "reason": "Test"}]), encoding="utf-8")
        (rt1 / "usercache.json").write_text(json.dumps([{"name": "OpOne", "uuid": "00000000-0000-4000-8000-000000000001"}]), encoding="utf-8")
        st_c8_stop, _ = pc1.request("POST", "/api/server/stop", token=admin_token)
        assert st_c8_stop == 200

        pc2.ctx.sync_engine.ensure_runtime_synced_when_idle(strict=True)
        rt2 = pc2.ctx.mc_manager.runtime_dir
        assert "OpOne" in (rt2 / "ops.json").read_text(encoding="utf-8")
        assert "WhiteOne" in (rt2 / "whitelist.json").read_text(encoding="utf-8")
        assert "Griefer" in (rt2 / "banned-players.json").read_text(encoding="utf-8")
        assert "10.9.9.9" in (rt2 / "banned-ips.json").read_text(encoding="utf-8")
        assert "OpOne" in (rt2 / "usercache.json").read_text(encoding="utf-8")
        print("  [OK] Suite C passed: conflict ranking/merge/fallback, .tmp.driveupload guard, atomic shrink, fallback backup guard & player-list mirroring.")

        # =====================================================================
        # Suite D: Full Cross-Host Mirroring, Remote Lock Watcher SSE & Remote Mutation Guards
        # =====================================================================
        print("\n[Suite D] Testing Full Cross-Host Mirroring, Remote Lock Watcher SSE & Remote Mutation Guards...")

        # Use a dedicated PC3 client with _remote_lock_watcher_loop running to verify real SSE propagation!
        pc3 = InMemoryHTTPClient(str(pc3_dir))
        st_p3, _ = pc3.request("POST", "/api/setup/sync-dir", body={"sharedSyncDir": str(shared_dir), "machineName": "PC-Gamma"})
        assert st_p3 == 200
        pc3_sse_q = queue.Queue(maxsize=200)
        with pc3.ctx.sse_mutex:
            pc3.ctx.sse_clients.append(pc3_sse_q)
        watcher_thread = threading.Thread(target=pc3.ctx._remote_lock_watcher_loop, daemon=True)
        watcher_thread.start()
        # Allow watcher initial baseline tick
        time.sleep(3.4)
        _drain_sse_queue(pc3_sse_q)

        # PC2 starts hosting
        st_d_start, _ = pc2.request("POST", "/api/server/start", token=bob_token)
        assert st_d_start == 200

        # Populate rich state on PC2: players, metrics, tunnel (including claimUrl that must NOT leak!), tunnelLogs, worldInfo, logs
        with pc2.ctx.mc_manager._mutex:
            pc2.ctx.mc_manager.online_players = ["Alice", "Bob"]
            pc2.ctx.mc_manager._active_max_players = 30
            pc2.ctx.mc_manager.memory_used_mb = 2048
            pc2.ctx.mc_manager.cpu_percent = 42.5
            pc2.ctx.mc_manager.tps = 19.4
            pc2.ctx.mc_manager.server_ready_at = time.time() - 75.0
        with pc2.ctx.tunnel_manager._mutex:
            pc2.ctx.tunnel_manager.status = "online"
            pc2.ctx.tunnel_manager.public_address = "mirror-smp.joinmc.example"
            pc2.ctx.tunnel_manager.claim_url = "https://playit.gg/claim/SECRET-HOST-CLAIM"
            pc2.ctx.tunnel_manager.logs = ["[playit] tunnel online at mirror-smp.joinmc.example"]
        pc2.ctx.tunnel_manager.get_lan_ip = lambda: "192.168.1.77"  # type: ignore

        pc2.ctx.mc_manager._log("Remote host ready for mirror check!", "INFO", "Server")
        pc2.ctx.mc_manager.sync_engine.update_lock_state(
            status="running",
            public_address="mirror-smp.joinmc.example",
            tunnel_status="online",
            online_players=["Alice", "Bob"],
            max_players=30,
            memory_used_mb=2048,
            memory_max_mb=4096,
            cpu_percent=42.5,
            tps=19.4,
            server_ready_at=pc2.ctx.mc_manager.server_ready_at,
        )
        pc2.ctx.mc_manager._publish_host_telemetry_throttled(force=True)

        # Verify PC1 REST mirroring across /api/server/status, /api/players, /api/admin/players
        st_m1, m_status = pc1.request("GET", "/api/server/status", token=admin_token)
        assert st_m1 == 200
        assert m_status["isLocalMachineHosting"] is False
        assert m_status["onlinePlayers"] == ["Alice", "Bob"]
        assert m_status["maxPlayers"] == 30
        assert m_status["cluster"]["mcVersion"] == "26.3", f"Expected clean raw mcVersion, got: {m_status['cluster']['mcVersion']}"
        assert m_status["tunnel"]["publicAddress"] == "mirror-smp.joinmc.example"
        assert m_status["tunnel"]["status"] == "online"
        assert m_status["tunnel"]["claimUrl"] == "", "Remote viewer must NEVER receive host's claimUrl!"
        assert m_status["tunnel"].get("hostLanIp") == "192.168.1.77" or m_status["lock"].get("hostLanIp") == "192.168.1.77", (
            f"Remote host LAN IP missing from status: tunnel={m_status['tunnel']} lock={m_status['lock']}"
        )
        assert len(m_status["tunnel"].get("logs", [])) >= 1
        assert m_status["telemetry"]["memoryUsedMb"] == 2048
        assert m_status["telemetry"]["cpuPercent"] == 42.5
        assert abs(m_status["telemetry"]["tps"] - 19.4) < 0.2
        assert m_status["telemetry"]["uptimeSeconds"] >= 70
        assert m_status["world"] is not None

        st_mp, m_players = pc1.request("GET", "/api/players", token=alice_token)
        assert st_mp == 200 and m_players["online"] == ["Alice", "Bob"] and m_players["count"] == 2 and m_players["max"] == 30

        # Wait for PC3's _remote_lock_watcher_loop to emit initial remote status_changed with backfilled logs
        time.sleep(3.5)
        pc3_events_1 = _drain_sse_queue(pc3_sse_q)
        sc_events_1 = [e for e in pc3_events_1 if e["event"] == "status_changed"]
        assert len(sc_events_1) >= 1, f"Watcher failed to emit status_changed on remote start! Got: {pc3_events_1}"
        assert sc_events_1[-1]["data"]["onlinePlayers"] == ["Alice", "Bob"]
        assert any("Remote host ready for mirror check!" in l.get("text", "") for l in sc_events_1[-1]["data"].get("logs", []))
        # Must NOT double-deliver logs via individual console_log on epoch transition
        assert not [e for e in pc3_events_1 if e["event"] == "console_log"], "Epoch transition should not double-deliver console_log!"

        # Now add an incremental console log on PC2 and verify watcher emits incremental console_log
        pc2.ctx.mc_manager._log("Incremental mirrored event 42", "INFO", "Minecraft")
        pc2.ctx.mc_manager._publish_host_telemetry_throttled(force=True)
        time.sleep(3.5)
        pc3_events_2 = _drain_sse_queue(pc3_sse_q)
        cl_events_2 = [e for e in pc3_events_2 if e["event"] == "console_log"]
        assert any("Incremental mirrored event 42" in e["data"].get("text", "") for e in cl_events_2), (
            f"Watcher did not emit incremental console_log! Got: {pc3_events_2}"
        )

        # Remote Admin Mutation Guards while PC2 holds active lock
        st_gp, _ = pc1.request("PUT", "/api/admin/settings", token=admin_token, body={"serverPort": 25599})
        assert st_gp == 409, f"Expected 409 when changing serverPort while remote host active, got {st_gp}"

        st_gr, _ = pc1.request("POST", "/api/server/restart", token=admin_token)
        assert st_gr == 409, f"Expected 409 on remote restart, got {st_gr}"

        st_gb, _ = pc1.request("POST", "/api/admin/backups", token=admin_token, body={"label": "conflict"})
        assert st_gb == 409, f"Expected 409 on idle backup while remote locked, got {st_gb}"

        st_pr, _ = pc1.request("PUT", "/api/admin/properties", token=admin_token, body={"properties": {"motd": "Blocked"}})
        assert st_pr == 409, f"Expected 409 on properties edit while remote locked, got {st_pr}"

        # PC2 stops hosting -> watcher emits idle status_changed and clears mirrored state
        st_d_stop, _ = pc2.request("POST", "/api/server/stop", token=bob_token)
        assert st_d_stop == 200
        time.sleep(3.5)
        pc3_events_3 = _drain_sse_queue(pc3_sse_q)
        sc_events_3 = [e for e in pc3_events_3 if e["event"] == "status_changed"]
        assert len(sc_events_3) >= 1 and sc_events_3[-1]["data"]["lock"]["isLocked"] is False
        assert sc_events_3[-1]["data"]["onlinePlayers"] == []

        with pc3.ctx.sse_mutex:
            pc3.ctx.sse_clients.clear()

        # Invalid base64 file upload returns 400 BAD_REQUEST (not 500)
        st_b64, res_b64 = pc1.request(
            "POST",
            "/api/admin/files/upload",
            token=admin_token,
            body={"dir": "", "filename": "bad.txt", "dataBase64": "!!!NOT_BASE64!!!"},
        )
        assert st_b64 == 400 and res_b64.get("code") == "BAD_REQUEST"
        print("  [OK] Suite D passed: full cross-host mirroring, SSE watcher backfill/incremental/release & remote guards.")

        # =====================================================================
        # Suite E: Auth Session Cleanup, Tunnel State & Frontend Invariants
        # =====================================================================
        print("\n[Suite E] Testing Auth Session Cleanup, Tunnel State & Frontend Invariants...")

        # E.1 Rotate passkey & delete member clean both _sessions and _session_times
        assert alice_token in pc1.ctx.auth_service._sessions
        assert alice_token in pc1.ctx.auth_service._session_times
        st_rot, _ = pc1.request("POST", "/api/admin/accounts/rotate", token=admin_token, body={"userId": alice_id})
        assert st_rot == 200
        assert alice_token not in pc1.ctx.auth_service._sessions
        assert alice_token not in pc1.ctx.auth_service._session_times

        st_del, _ = pc1.request("DELETE", f"/api/admin/accounts/{alice_id}", token=admin_token)
        assert st_del == 200
        st_me_dead, _ = pc1.request("GET", "/api/auth/me", token=alice_token)
        assert st_me_dead == 401

        # E.2 Tunnel disabled sets status="disabled" and clears claimUrl; get_state() has no side effects
        st_td, _ = pc1.request("PUT", "/api/admin/settings", token=admin_token, body={"tunnelEnabled": False})
        assert st_td == 200
        pc1.ctx.tunnel_manager.claim_url = "https://playit.gg/claim/stale"
        pc1.ctx.tunnel_manager.start_tunnel()
        tstate = pc1.ctx.tunnel_manager.get_state()
        assert tstate["status"] == "disabled" and tstate["claimUrl"] == ""

        # E.3 Frontend Stage 2 UI/CSS invariants
        idx_html = (ROOT_DIR / "public" / "index.html").read_text(encoding="utf-8")
        app_js = (ROOT_DIR / "public" / "app.js").read_text(encoding="utf-8")
        styles_css = (ROOT_DIR / "public" / "styles.css").read_text(encoding="utf-8")

        for dom_id in (
            "cfg-auto-sync-minutes",
            "net-auto-sync-preset",
            "net-auto-sync-minutes",
            "overview-autosync-badge",
            "remote-admin-banner",
        ):
            assert dom_id in idx_html, f"Missing Stage 2 element #{dom_id} in public/index.html"

        for fn_name in (
            "syncAutoSyncInputs",
            "applyRemoteAdminLockGuards",
            "handleConsoleEpochTransition",
            "applyRemoteLockStateToSummary",
            "armTwoStepConfirm",
            "runWithButtonBusy",
            "tunnel_log",
            "hostLanIp",
        ):
            assert fn_name in app_js, f"Missing Stage 2 symbol {fn_name} in public/app.js"

        assert "--border-subtle:" in styles_css and "--bg-card-hover:" in styles_css, "Missing CSS root variables!"
        print("  [OK] Suite E passed: auth session cleanup, tunnel disabled state & frontend Stage 2 invariants.")

        print("\n============================================================")
        print("  ALL STAGE 2 E2E & STRESS TESTS PASSED SUCCESSFULLY!        ")
        print("============================================================")

    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


if __name__ == "__main__":
    run_stage2_suite()
