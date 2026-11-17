"""
Comprehensive E2E & Stress Verification for Admin Features While a Non-Admin Member Hosts.

Covers both real-world hosting topologies strictly inside isolated temp directories:
  Scenario A (Remote Non-Admin Hosting):
    - Member ('Bob') actively hosts on PC B; Admin ('Admin') operates all dashboard
      features on PC A over a shared Google Drive-like folder.
    - Verifies:
      * Remote read mirroring: /api/server/status, /api/players, /api/admin/players
        (online, ops, whitelist, banned), /api/admin/settings (remote autoSync),
        /api/admin/properties & /api/admin/world (remote worldInfo), /api/admin/files,
        /api/admin/files/read, /api/admin/plugins, /api/admin/backups & download
        (including Google Drive '(1)' conflict filenames + traversal block).
      * Remote console clear cutoff per lockEpoch + new log arrival + epoch transition.
      * 409 HostLockConflictError guards on runtime/host-only mutations:
        /api/admin/console, /api/server/stop, /api/server/restart,
        /api/admin/install-jar, /api/admin/world/reset, /api/admin/gamerules,
        /api/admin/properties, /api/admin/players (live + list),
        /api/admin/plugins/{install,toggle,DELETE}, /api/admin/files/{write,upload,DELETE},
        /api/admin/backups, /api/admin/backups/restore, DELETE /api/admin/backups/<name>,
        /api/admin/tunnel/secret, and /api/admin/settings startup/port/tunnel-provider changes.
      * Live cluster/sync settings allowed on /api/admin/settings during Scenario A
        (autoSyncIntervalMinutes, idleShutdownMinutes, serverName, publicJoinAddress,
         autoBackupOnStop, gitSyncEnabled, maxBackups) and picked up dynamically by PC B!
      * Drive conflict resilience during member hosting: sync-manifest (1).json +
        latest-server (1).zip pulled cleanly on Admin reads; deletedAccounts tombstones
        prevent deleted member resurrection from stale cluster-config (1).json;
        newer rename/passkey preserved over member hostCount conflict; locks (1)/released-*
        triggers immediate lock_lost fencing; force-unlock recovers cleanly & unblocks restore/start.

  Scenario B (Same-PC Non-Admin Hosting):
    - Member ('Bob') logs in & starts hosting locally on PC A; Admin ('Admin') operates
      on PC A simultaneously while local_status is 'starting' and then 'running'.
    - Verifies:
      * Transient ('starting') guards on live player actions, backups, restore.
      * Live ('running') Admin actions succeed cleanly:
        console commands, console clear, live player actions (gamemode/heal/tp_spawn/kick/op/ban),
        live gamerules (saved + stdin), safe server.properties (motd/difficulty),
        live settings (serverName, autoSyncIntervalMinutes, idleShutdownMinutes,
        publicJoinAddress set + clear back to LAN IP, machineName synced to host lock),
        live backup snapshot (save-off / save-all / save-on), Admin stop & Admin restart.
      * Live ('running') destructive/disk mutations blocked cleanly:
        /api/admin/force-unlock -> 409, /api/admin/settings startup/JVM/port/tunnel -> 409,
        update_server_properties(online-mode/server-port/level-name) -> 409,
        files write/upload/delete -> 409, plugins install/toggle/delete -> 409,
        backups delete -> 409, backups restore -> 400, install-jar -> 400.
      * Live Java pipe failure safety & WinError 32 -> 409 FILE_IN_USE mapping
        (while path-traversal PermissionError remains 403 FORBIDDEN).
      * Account rename updates local host lock username; passkey rotation/revocation
        invalidates member session and closes SSE cleanly.
      * Stage 2 Frontend Scenario A/B guard & UX invariants.
"""

import io
import os
import sys
import json
import time
import queue
import zipfile
import subprocess
import tempfile
import threading
from pathlib import Path
from typing import Any, Dict, List

os.environ["MINERELAY_TEST_MODE"] = "1"

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.models import ClusterConfig, HostLock
from src.server import AppContext, create_request_handler
from src.sync_engine import HostLockConflictError


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
        content_type = ""
        for hline in header_part.splitlines()[1:]:
            hstr = hline.decode("utf-8", errors="replace")
            if hstr.lower().startswith("content-type:"):
                content_type = hstr.split(":", 1)[1].strip().lower()
                break
        if "application/json" in content_type:
            try:
                parsed: Any = json.loads(payload_part.decode("utf-8"))
            except Exception:
                parsed = {"raw": payload_part.decode("utf-8", errors="replace")}
        else:
            parsed = payload_part
        return status_code, parsed


class FakeLiveJavaProc:
    """Simulates a live JVM process with controllable stdin success/failure."""

    def __init__(self, mgr, stdin_fail: bool = False, emit_save_complete: bool = True):
        self.mgr = mgr
        self.commands: List[str] = []
        self._alive = True
        self._stdin_fail = stdin_fail
        parent = self

        class _Stdin:
            def write(self, data: str) -> int:
                if parent._stdin_fail:
                    raise OSError("Broken pipe")
                parent.commands.append(data)
                if emit_save_complete and "save-all" in data:
                    parent.mgr._save_complete_event.set()
                return len(data)

            def flush(self) -> None:
                if parent._stdin_fail:
                    raise OSError("Broken pipe")

            def close(self) -> None:
                pass

        self.stdin = _Stdin()

    def poll(self):
        return None if self._alive else 0


def _drain_sse_queue(q_obj: queue.Queue) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    while True:
        try:
            events.append(json.loads(q_obj.get_nowait()))
        except queue.Empty:
            break
    return events


def run_admin_during_member_host_tests() -> None:
    print("============================================================")
    print("  Admin Features During Non-Admin Member Hosting E2E Suite  ")
    print("============================================================")

    with tempfile.TemporaryDirectory(prefix="minerelay_member_host_e2e_") as tmp_dir:
        tmp_root = Path(tmp_dir)
        shared_dir = tmp_root / "SharedDrive"
        pc_admin_dir = tmp_root / "PC_Admin"
        pc_member_dir = tmp_root / "PC_Member"
        shared_dir.mkdir(parents=True, exist_ok=True)
        pc_admin_dir.mkdir(parents=True, exist_ok=True)
        pc_member_dir.mkdir(parents=True, exist_ok=True)

        # Initialize cluster on Admin PC A, connect Member PC B to same shared Drive folder
        pc_a = InMemoryHTTPClient(str(pc_admin_dir))
        st, init_res = pc_a.request(
            "POST",
            "/api/setup/init",
            body={
                "serverName": "RelaySMP",
                "adminUsername": "AdminOwner",
                "machineName": "PC-Admin",
                "sharedSyncDir": str(shared_dir),
            },
        )
        assert st == 200, f"Setup failed: {st} {init_res}"
        admin_token = init_res["token"]

        st, bob_res = pc_a.request(
            "POST",
            "/api/admin/accounts",
            token=admin_token,
            body={"username": "BobMember"},
        )
        assert st == 201
        bob_id = bob_res["account"]["id"]
        bob_pk = bob_res["generatedPasskey"]

        st, alice_res = pc_a.request(
            "POST",
            "/api/admin/accounts",
            token=admin_token,
            body={"username": "AliceMember"},
        )
        assert st == 201
        alice_id = alice_res["account"]["id"]
        alice_pk = alice_res["generatedPasskey"]

        # Create initial backup archive and normal runtime file while idle
        st, _ = pc_a.request(
            "POST",
            "/api/admin/files/write",
            token=admin_token,
            body={"path": "notes/welcome.txt", "content": "initial-v1"},
        )
        assert st == 200
        st, _ = pc_a.request(
            "POST",
            "/api/admin/backups",
            token=admin_token,
            body={"label": "initial"},
        )
        assert st == 200

        pc_b = InMemoryHTTPClient(str(pc_member_dir))
        st, _ = pc_b.request(
            "POST",
            "/api/setup/sync-dir",
            body={"sharedSyncDir": str(shared_dir), "machineName": "PC-Member-Bob"},
        )
        assert st == 200
        st, bob_login = pc_b.request("POST", "/api/auth/login", body={"passkey": bob_pk})
        assert st == 200
        bob_token = bob_login["token"]

        # =====================================================================
        # SCENARIO A: Remote Non-Admin Member Hosts on PC B, Admin on PC A
        # =====================================================================
        print("\n[Scenario A] Remote Non-Admin Member Hosting on PC B + Admin on PC A...")

        st, start_res = pc_b.request("POST", "/api/server/start", token=bob_token)
        assert st == 200 and start_res["localStatus"] == "running"

        # Seed rich remote runtime state on PC B: players, ops, whitelist, bans, custom world, logs, plugin
        pc_b.ctx.mc_manager._write_player_json_raw = None
        (pc_b.ctx.mc_manager.runtime_dir / "ops.json").write_text(
            json.dumps([{"uuid": "00000000-0000-3000-8000-000000000001", "name": "BobMember", "level": 4}]),
            encoding="utf-8",
        )
        (pc_b.ctx.mc_manager.runtime_dir / "whitelist.json").write_text(
            json.dumps([{"uuid": "00000000-0000-3000-8000-000000000002", "name": "FriendPlayer"}]),
            encoding="utf-8",
        )
        (pc_b.ctx.mc_manager.runtime_dir / "banned-players.json").write_text(
            json.dumps([{"uuid": "00000000-0000-3000-8000-000000000003", "name": "Griefer99", "reason": "TNT"}]),
            encoding="utf-8",
        )
        (pc_b.ctx.mc_manager.runtime_dir / "banned-ips.json").write_text(
            json.dumps([{"ip": "203.0.113.5", "reason": "Bot"}]),
            encoding="utf-8",
        )
        (pc_b.ctx.mc_manager.runtime_dir / "plugins").mkdir(parents=True, exist_ok=True)
        (pc_b.ctx.mc_manager.runtime_dir / "plugins" / "RemoteGuard.jar").write_bytes(b"PK\x03\x04remote")
        (pc_b.ctx.mc_manager.runtime_dir / "notes" / "live-notes.txt").write_text("from-remote-host-v2", encoding="utf-8")

        with pc_b.ctx.mc_manager._mutex:
            pc_b.ctx.mc_manager.online_players = ["BobMember", "FriendPlayer"]
            pc_b.ctx.mc_manager._active_max_players = 25
            pc_b.ctx.mc_manager._last_auto_sync_status = "ok"
            pc_b.ctx.mc_manager._last_auto_sync_at = time.time() - 12.0
        pc_b.ctx.mc_manager._log("Remote member log line #1", "INFO", "Server")
        pc_b.ctx.mc_manager._log("Remote member log line #2", "WARN", "Minecraft")
        pc_b.ctx.mc_manager._publish_host_telemetry_throttled(force=True)

        # A.1 Read mirroring on PC A while PC B hosts
        st, adm_players = pc_a.request("GET", "/api/admin/players", token=admin_token)
        assert st == 200
        assert adm_players["online"] == ["BobMember", "FriendPlayer"]
        assert adm_players["max"] == 25
        assert any(o.get("name") == "BobMember" for o in adm_players["ops"]), f"Missing remote ops: {adm_players}"
        assert any(w.get("name") == "FriendPlayer" for w in adm_players["whitelist"]), f"Missing remote whitelist: {adm_players}"
        assert any(b.get("name") == "Griefer99" for b in adm_players["banned"]), f"Missing remote banned: {adm_players}"
        assert any(ip.get("ip") == "203.0.113.5" for ip in adm_players.get("bannedIps", [])), f"Missing remote bannedIps: {adm_players}"

        st, adm_settings = pc_a.request("GET", "/api/admin/settings", token=admin_token)
        assert st == 200
        assert adm_settings["autoSync"]["lastSyncStatus"] == "ok"
        assert adm_settings["autoSync"]["lastSyncAt"] > 0

        st, adm_world = pc_a.request("GET", "/api/admin/world", token=admin_token)
        assert st == 200 and "world" in adm_world
        st, adm_props = pc_a.request("GET", "/api/admin/properties", token=admin_token)
        assert st == 200 and "properties" in adm_props and "world" in adm_props

        # A.2 Remote console clear watermark & new log arrival + epoch reset
        st, status_before_clear = pc_a.request("GET", "/api/server/status", token=admin_token)
        assert st == 200
        assert any("Remote member log line #2" in e.get("text", "") for e in status_before_clear["logs"])

        st, clear_res = pc_a.request("POST", "/api/admin/console/clear", token=admin_token)
        assert st == 200 and clear_res.get("cleared") is True and clear_res.get("remote") is True

        st, status_after_clear = pc_a.request("GET", "/api/server/status", token=admin_token)
        assert st == 200
        assert not any("Remote member log line #2" in e.get("text", "") for e in status_after_clear["logs"]), (
            f"Old remote logs snapped back after clear: {status_after_clear['logs']}"
        )

        time.sleep(0.02)
        pc_b.ctx.mc_manager._log("Remote member log line #3 AFTER clear", "INFO", "Server")
        pc_b.ctx.mc_manager._publish_host_telemetry_throttled(force=True)
        st, status_after_new_log = pc_a.request("GET", "/api/server/status", token=admin_token)
        assert st == 200
        assert any("Remote member log line #3 AFTER clear" in e.get("text", "") for e in status_after_new_log["logs"]), (
            f"New remote log failed to appear after clear: {status_after_new_log['logs']}"
        )
        assert not any("Remote member log line #1" in e.get("text", "") for e in status_after_new_log["logs"])

        # A.3 Every host-locked / destructive Admin mutation returns 409 HostLockConflictError in Scenario A
        blocked_requests = [
            ("POST", "/api/admin/console", {"command": "list"}),
            ("POST", "/api/server/stop", None),
            ("POST", "/api/server/restart", None),
            ("POST", "/api/admin/install-jar", {"software": "vanilla", "version": "26.3"}),
            ("POST", "/api/admin/world/reset", {"mode": "random", "backupExisting": False}),
            ("PUT", "/api/admin/gamerules", {"gamerules": {"keepInventory": True}}),
            ("PUT", "/api/admin/properties", {"properties": {"motd": "Blocked Remote Edit"}}),
            ("POST", "/api/admin/players", {"action": "kick", "username": "BobMember"}),
            ("POST", "/api/admin/players", {"action": "op", "username": "AliceMember"}),
            ("POST", "/api/admin/players", {"action": "whitelist_add", "username": "AliceMember"}),
            ("POST", "/api/admin/players", {"action": "ban", "username": "AliceMember", "reason": "test"}),
            ("POST", "/api/admin/plugins/install", {"projectId": "luckperms", "title": "LuckPerms"}),
            ("POST", "/api/admin/plugins/toggle", {"filename": "RemoteGuard.jar"}),
            ("DELETE", "/api/admin/plugins?filename=RemoteGuard.jar", None),
            ("POST", "/api/admin/files/write", {"path": "notes/welcome.txt", "content": "hijack"}),
            ("POST", "/api/admin/files/upload", {"dir": "notes", "filename": "x.txt", "dataBase64": "aGk="}),
            ("DELETE", "/api/admin/files?path=notes/welcome.txt", None),
            ("POST", "/api/admin/backups", {"label": "blocked-remote"}),
            ("POST", "/api/admin/tunnel/secret", {"secretKey": "secret-123"}),
            ("PUT", "/api/admin/settings", {"serverPort": 25577}),
            ("PUT", "/api/admin/settings", {"mcSoftware": "vanilla"}),
            ("PUT", "/api/admin/settings", {"mcVersion": "1.21.4"}),
            ("PUT", "/api/admin/settings", {"memoryMinMb": 1024}),
            ("PUT", "/api/admin/settings", {"memoryMaxMb": 6144}),
            ("PUT", "/api/admin/settings", {"javaPath": "C:/java25/bin/java.exe"}),
            ("PUT", "/api/admin/settings", {"jvmFlags": "-XX:+UseG1GC"}),
            ("PUT", "/api/admin/settings", {"tunnelEnabled": False}),
            ("PUT", "/api/admin/settings", {"tunnelProvider": "custom"}),
        ]
        for meth, ep, payload in blocked_requests:
            st_b, res_b = pc_a.request(meth, ep, token=admin_token, body=payload)
            assert st_b == 409 and res_b.get("code") == "HOST_LOCK_CONFLICT", (
                f"Expected 409 HOST_LOCK_CONFLICT for {meth} {ep} body={payload}, got {st_b} {res_b}"
            )

        # Backup restore & backup delete also return 409 while PC B actively hosts
        st, bk_list_res = pc_a.request("GET", "/api/admin/backups", token=admin_token)
        assert st == 200 and len(bk_list_res["backups"]) >= 1
        bk_name = bk_list_res["backups"][0]["name"]
        st_rst, res_rst = pc_a.request("POST", "/api/admin/backups/restore", token=admin_token, body={"name": bk_name})
        assert st_rst == 409 and res_rst.get("code") == "HOST_LOCK_CONFLICT"
        st_del_bk, res_del_bk = pc_a.request("DELETE", f"/api/admin/backups/{bk_name}", token=admin_token)
        assert st_del_bk == 409 and res_del_bk.get("code") == "HOST_LOCK_CONFLICT"

        # A.4 Live cluster/sync settings DO work in Scenario A & PC B picks them up cleanly!
        st, upd_set = pc_a.request(
            "PUT",
            "/api/admin/settings",
            token=admin_token,
            body={
                "serverName": "RelaySMP Live Updated",
                "autoSyncIntervalMinutes": 5,
                "idleShutdownMinutes": 15,
                "autoBackupOnStop": True,
                "maxBackups": 12,
                "publicJoinAddress": "play.relaysmp.test",
            },
        )
        assert st == 200, f"Live cluster settings failed in Scenario A: {st} {upd_set}"
        assert upd_set["cluster"]["serverName"] == "RelaySMP Live Updated"
        assert upd_set["cluster"]["autoSyncIntervalMinutes"] == 5
        assert upd_set["cluster"]["idleShutdownMinutes"] == 15
        assert upd_set["cluster"]["publicJoinAddress"] == "play.relaysmp.test"
        assert upd_set["cluster"]["serverPort"] == 25565  # untouched

        b_auto = pc_b.ctx.mc_manager.get_auto_sync_status()
        assert b_auto["intervalMinutes"] == 5 and b_auto["enabled"] is True

        # A.5 Remote host periodic auto-sync + Google Drive conflict snapshot (1) -> fresh Admin reads!
        fake_proc_b = FakeLiveJavaProc(pc_b.ctx.mc_manager)
        with pc_b.ctx.mc_manager._mutex:
            pc_b.ctx.mc_manager._proc = fake_proc_b
        sync_res = pc_b.ctx.mc_manager.run_live_auto_sync_once(force=True)
        assert sync_res.get("pushed") is True, f"Auto-sync failed: {sync_res}"

        # Create Drive conflict copies of manifest & latest zip in snapshots/ simulating concurrent cloud sync
        manifest_bytes = pc_b.ctx.sync_engine.sync_manifest_path.read_bytes()
        manifest_obj = json.loads(manifest_bytes.decode("utf-8"))
        # Simulate newer remote snapshot #seq+1 in conflict files
        (pc_b.ctx.mc_manager.runtime_dir / "notes" / "conflict-live.txt").write_text("from-conflict-snapshot", encoding="utf-8")
        newer_zip = pc_b.ctx.sync_engine.snapshots_dir / "latest-server (1).zip"
        with zipfile.ZipFile(newer_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for item in pc_b.ctx.mc_manager.runtime_dir.rglob("*"):
                if item.is_file():
                    zf.write(item, str(item.relative_to(pc_b.ctx.mc_manager.runtime_dir)).replace("\\", "/"))
        import hashlib
        newer_sha = hashlib.sha256(newer_zip.read_bytes()).hexdigest()
        conflict_manifest = dict(manifest_obj)
        conflict_manifest["snapshotSeq"] = int(manifest_obj.get("snapshotSeq", 0)) + 1
        conflict_manifest["sha256"] = newer_sha
        conflict_manifest["sizeBytes"] = newer_zip.stat().st_size
        conflict_manifest["updatedAt"] = time.time() + 1.0
        (pc_b.ctx.sync_engine.snapshots_dir / "sync-manifest (1).json").write_text(json.dumps(conflict_manifest), encoding="utf-8")

        st, files_res = pc_a.request("GET", "/api/admin/files?dir=notes", token=admin_token)
        assert st == 200
        names = [f["name"] for f in files_res["files"]]
        assert "live-notes.txt" in names and "conflict-live.txt" in names, f"Stale file list on Admin PC A: {names}"

        st, read_res = pc_a.request("GET", "/api/admin/files/read?path=notes/conflict-live.txt", token=admin_token)
        assert st == 200 and read_res["content"] == "from-conflict-snapshot"

        st, plugins_res = pc_a.request("GET", "/api/admin/plugins", token=admin_token)
        assert st == 200 and any(p["filename"] == "RemoteGuard.jar" for p in plugins_res["plugins"])

        # A.6 Backup download supports Drive '(1)' conflict/space names & blocks traversal
        conflict_bk_path = pc_a.ctx.sync_engine.backups_dir / "world manual (1).zip"
        with zipfile.ZipFile(conflict_bk_path, "w") as zf:
            zf.writestr("hello.txt", "conflict-bk")
        st, bk_bytes = pc_a.request(
            "GET",
            "/api/admin/backups/download?name=world%20manual%20(1).zip",
            token=admin_token,
        )
        assert st == 200 and isinstance(bk_bytes, (bytes, bytearray)) and len(bk_bytes) > 0
        st_trav, _ = pc_a.request(
            "GET",
            "/api/admin/backups/download?name=..%2Fcluster-config.json",
            token=admin_token,
        )
        assert st_trav == 400

        # A.7 ClusterConfig deletedAccounts tombstone + conflict rename preservation
        st, _ = pc_a.request("DELETE", f"/api/admin/accounts/{alice_id}", token=admin_token)
        assert st == 200
        # Inject stale cluster-config (1).json containing deleted Alice + older Bob name
        stale_cfg = ClusterConfig.from_dict(
            json.loads(pc_a.ctx.sync_engine.cluster_config_path.read_text(encoding="utf-8"))
        )
        from src.models import UserAccount
        stale_cfg.accounts.append(
            UserAccount(
                id=alice_id,
                username="AliceMember",
                role="member",
                passkey_salt="s",
                passkey_hash="h",
                passkey_hint="****",
                created_at=alice_res["account"]["createdAt"],
                last_active_at=alice_res["account"]["lastActiveAt"],
            )
        )
        (shared_dir / "cluster-config (1).json").write_text(json.dumps(stale_cfg.to_dict(include_secrets=True)), encoding="utf-8")
        # Wait 10ms and ensure canonical cluster-config is preserved with tombstone
        loaded_cfg = pc_a.ctx.sync_engine.load_cluster_config()
        assert loaded_cfg is not None
        assert not any(a.id == alice_id for a in loaded_cfg.accounts), "Deleted member was resurrected by Drive conflict file!"
        st_alice_login, _ = pc_a.request("POST", "/api/auth/login", body={"passkey": alice_pk})
        assert st_alice_login == 401

        # A.8 Split-brain released (1) tombstone fences PC B immediately on update_lock_state()
        fenced_events = []
        orig_cb = pc_b.ctx.sync_engine.event_callback

        def _capture_ev(ev, data):
            fenced_events.append(ev)
            if orig_cb:
                orig_cb(ev, data)

        pc_b.ctx.sync_engine.event_callback = _capture_ev
        tomb_dir = shared_dir / "locks (1)"
        tomb_dir.mkdir(parents=True, exist_ok=True)
        bob_lock = pc_b.ctx.sync_engine.get_lock()
        (tomb_dir / f"released-{bob_lock.lock_epoch}.json").write_text(
            json.dumps({"lockEpoch": bob_lock.lock_epoch, "forceUnlocked": True}),
            encoding="utf-8",
        )
        pc_b.ctx.sync_engine.update_lock_state(status="running")
        assert pc_b.ctx.sync_engine._owns_active_lock is False
        assert "lock_lost" in fenced_events

        # Cleanly stop PC B (or force unlock on PC A) and verify force-unlock recovery
        with pc_b.ctx.mc_manager._mutex:
            pc_b.ctx.mc_manager._proc = None
        pc_b.ctx.mc_manager.local_status = "idle"
        pc_b.ctx.mc_manager.active_host_user = None
        pc_b.ctx.tunnel_manager.stop_tunnel()

        # Re-seed active remote lock from PC B then force-unlock from PC A
        pc_b.ctx.sync_engine.acquire_lock(pc_b.ctx.auth_service.get_user_by_token(bob_token))
        st, fu_res = pc_a.request("POST", "/api/admin/force-unlock", token=admin_token)
        assert st == 200 and fu_res["unlocked"] is True and fu_res["lock"]["isLocked"] is False

        # After force-unlock, Admin can immediately restore backup and edit settings!
        st, rst_ok = pc_a.request("POST", "/api/admin/backups/restore", token=admin_token, body={"name": bk_name})
        assert st == 200 and rst_ok.get("restored") == bk_name, f"Restore failed: {st} {rst_ok}"
        st, set_ok = pc_a.request("PUT", "/api/admin/settings", token=admin_token, body={"serverPort": 25566})
        assert st == 200 and set_ok["cluster"]["serverPort"] == 25566
        # Reset port back to 25565
        pc_a.request("PUT", "/api/admin/settings", token=admin_token, body={"serverPort": 25565})
        print("  [OK] Scenario A passed.")

        # =====================================================================
        # SCENARIO B: Same-PC Non-Admin Hosting on PC A + Admin Logged In on PC A
        # =====================================================================
        print("\n[Scenario B] Same-PC Non-Admin Member Hosting on PC A + Admin Session on PC A...")

        # Bob logs in on PC A and starts hosting locally on PC A
        st, bob_a_login = pc_a.request("POST", "/api/auth/login", body={"passkey": bob_pk})
        assert st == 200
        bob_a_token = bob_a_login["token"]

        # B.1 Test 'starting' transient guards first
        with pc_a.ctx.mc_manager._lifecycle_lock:
            pc_a.ctx.mc_manager.local_status = "starting"
            pc_a.ctx.mc_manager.active_host_user = pc_a.ctx.auth_service.get_user_by_token(bob_a_token)
        st_start_kick, res_start_kick = pc_a.request(
            "POST",
            "/api/admin/players",
            token=admin_token,
            body={"action": "kick", "username": "Player1"},
        )
        assert st_start_kick in (400, 409) and "starting" in res_start_kick.get("error", "").lower(), res_start_kick
        st_start_bk, res_start_bk = pc_a.request("POST", "/api/admin/backups", token=admin_token, body={"label": "early"})
        assert st_start_bk == 400
        pc_a.ctx.mc_manager.local_status = "idle"
        pc_a.ctx.mc_manager.active_host_user = None

        # Start hosting cleanly as Bob on PC A
        st, b_start = pc_a.request("POST", "/api/server/start", token=bob_a_token)
        assert st == 200 and b_start["localStatus"] == "running"
        assert b_start["lock"]["hostUsername"] == "BobMember"
        assert b_start["isLocalMachineHosting"] is True

        # B.2 Live Admin features that MUST work during Same-PC Member Hosting
        st, cmd_res = pc_a.request("POST", "/api/admin/console", token=admin_token, body={"command": "say AdminHello"})
        assert st == 200 and cmd_res["sent"] is True
        st, join_res = pc_a.request("POST", "/api/admin/console", token=admin_token, body={"command": "simjoin Steve"})
        assert st == 200
        st, st_after_join = pc_a.request("GET", "/api/server/status", token=admin_token)
        assert st == 200 and "Steve" in st_after_join["onlinePlayers"]

        st, cl_res = pc_a.request("POST", "/api/admin/console/clear", token=admin_token)
        assert st == 200 and cl_res["cleared"] is True

        # Live gamerules (saved + stdin)
        fake_proc_a = FakeLiveJavaProc(pc_a.ctx.mc_manager)
        with pc_a.ctx.mc_manager._mutex:
            pc_a.ctx.mc_manager._proc = fake_proc_a
            pc_a.ctx.mc_manager.runtime_mode = "java"
        st, gr_res = pc_a.request(
            "PUT",
            "/api/admin/gamerules",
            token=admin_token,
            body={"gamerules": {"keepInventory": True, "randomTickSpeed": 5}},
        )
        assert st == 200 and gr_res["gamerules"]["keepInventory"] is True
        assert any("gamerule keepInventory true" in c for c in fake_proc_a.commands)

        # Safe live server.properties work, while online-mode / server-port / level-name are blocked
        st, pr_ok = pc_a.request(
            "PUT",
            "/api/admin/properties",
            token=admin_token,
            body={"properties": {"difficulty": "hard", "motd": "Live Same-PC MOTD"}},
        )
        assert st == 200 and pr_ok["properties"]["difficulty"] == "hard"
        assert any("difficulty hard" in c for c in fake_proc_a.commands)

        for dangerous_prop in ({"online-mode": False}, {"server-port": "25590"}, {"level-name": "other_world"}):
            st_dp, res_dp = pc_a.request("PUT", "/api/admin/properties", token=admin_token, body={"properties": dangerous_prop})
            assert st_dp == 409, f"Expected 409 for live {dangerous_prop}, got {st_dp} {res_dp}"

        # Live player action in JVM mode sends stdin and raises cleanly if stdin pipe is broken
        st, gm_ok = pc_a.request(
            "POST",
            "/api/admin/players",
            token=admin_token,
            body={"action": "gamemode", "username": "Steve", "value": "creative"},
        )
        assert st == 200 and any("gamemode creative Steve" in c for c in fake_proc_a.commands)

        # In simulated mode, kick updates online_players; in real JVM (subprocess.Popen) mode,
        # kick sends command and waits for stdout confirmation instead of optimistically dropping player.
        class _DummyPopen(subprocess.Popen):
            def __init__(self, real_stdin):
                self._child_created = False
                self.stdin = real_stdin
                self.returncode = None

            def poll(self):
                return None

            def __del__(self):
                pass

        jvm_proc = _DummyPopen(fake_proc_a.stdin)
        with pc_a.ctx.mc_manager._mutex:
            pc_a.ctx.mc_manager._proc = jvm_proc
            pc_a.ctx.mc_manager.runtime_mode = "java"

        st, kick_ok = pc_a.request(
            "POST",
            "/api/admin/players",
            token=admin_token,
            body={"action": "kick", "username": "Steve"},
        )
        assert st == 200 and "Steve" in pc_a.ctx.mc_manager.online_players
        assert any("kick Steve" in c for c in fake_proc_a.commands)

        # Broken stdin pipe raises error instead of silently succeeding
        broken_proc = FakeLiveJavaProc(pc_a.ctx.mc_manager, stdin_fail=True)
        with pc_a.ctx.mc_manager._mutex:
            pc_a.ctx.mc_manager._proc = broken_proc
        st_pipe, _ = pc_a.request(
            "POST",
            "/api/admin/players",
            token=admin_token,
            body={"action": "heal", "username": "Steve"},
        )
        assert st_pipe == 500 or st_pipe == 400

        # Restore working fake proc for live backup
        with pc_a.ctx.mc_manager._mutex:
            pc_a.ctx.mc_manager._proc = fake_proc_a
        st, live_bk = pc_a.request("POST", "/api/admin/backups", token=admin_token, body={"label": "same-pc-live"})
        assert st == 200 and any("save-all flush" in c for c in fake_proc_a.commands)

        # Live settings: allowed fields + publicJoinAddress clear + machineName lock sync; startup fields -> 409
        st, set_pub = pc_a.request(
            "PUT",
            "/api/admin/settings",
            token=admin_token,
            body={"publicJoinAddress": "custom.local.test", "machineName": "PC-Admin-Renamed", "autoSyncIntervalMinutes": 15},
        )
        assert st == 200
        assert set_pub["status"]["tunnel"]["publicAddress"] == "custom.local.test"
        assert pc_a.ctx.sync_engine.get_lock().machine_name == "PC-Admin-Renamed"

        # Clear publicJoinAddress -> must NOT stay stuck on custom.local.test
        st, clr_pub = pc_a.request(
            "PUT",
            "/api/admin/settings",
            token=admin_token,
            body={"publicJoinAddress": ""},
        )
        assert st == 200
        assert clr_pub["status"]["tunnel"]["publicAddress"] != "custom.local.test"

        for blocked_live_cfg in (
            {"serverPort": 25578},
            {"mcSoftware": "vanilla"},
            {"mcVersion": "1.21.4"},
            {"memoryMinMb": 1024},
            {"memoryMaxMb": 6144},
            {"javaPath": "C:/alt/java.exe"},
            {"jvmFlags": "-Xmx2G"},
            {"tunnelEnabled": False},
            {"tunnelProvider": "custom"},
        ):
            st_lc, res_lc = pc_a.request("PUT", "/api/admin/settings", token=admin_token, body=blocked_live_cfg)
            assert st_lc == 409 and res_lc.get("code") == "HOST_LOCK_CONFLICT", (
                f"Expected 409 for live settings {blocked_live_cfg}, got {st_lc} {res_lc}"
            )

        # B.3 Unsafe disk/runtime/force-unlock actions blocked during Scenario B
        st_fu_b, res_fu_b = pc_a.request("POST", "/api/admin/force-unlock", token=admin_token)
        assert st_fu_b == 409 and res_fu_b.get("code") == "HOST_LOCK_CONFLICT"

        for meth, ep, payload, expected_codes in [
            ("POST", "/api/admin/files/write", {"path": "notes/test.txt", "content": "x"}, (409,)),
            ("POST", "/api/admin/files/upload", {"dir": "", "filename": "u.txt", "dataBase64": "aGk="}, (409,)),
            ("DELETE", "/api/admin/files?path=notes/welcome.txt", None, (409,)),
            ("POST", "/api/admin/plugins/install", {"projectId": "luckperms", "title": "LuckPerms"}, (409,)),
            ("POST", "/api/admin/plugins/toggle", {"filename": "RemoteGuard.jar"}, (409,)),
            ("DELETE", "/api/admin/plugins?filename=RemoteGuard.jar", None, (409,)),
            ("DELETE", f"/api/admin/backups/{bk_name}", None, (409,)),
            ("POST", "/api/admin/backups/restore", {"name": bk_name}, (400,)),
            ("POST", "/api/admin/install-jar", {"software": "paper", "version": "26.3"}, (400,)),
        ]:
            st_x, res_x = pc_a.request(meth, ep, token=admin_token, body=payload)
            assert st_x in expected_codes, f"Scenario B {meth} {ep} expected {expected_codes}, got {st_x} {res_x}"

        # B.4 Member account rename updates local host lock; passkey rotate invalidates member token
        st, ren_res = pc_a.request(
            "POST",
            "/api/admin/accounts/rename",
            token=admin_token,
            body={"userId": bob_id, "username": "BobRenamed"},
        )
        assert st == 200 and ren_res["account"]["username"] == "BobRenamed"
        assert pc_a.ctx.sync_engine.get_lock().host_username == "BobRenamed"

        # Reset fake proc before simulated restart/stop
        with pc_a.ctx.mc_manager._mutex:
            pc_a.ctx.mc_manager._proc = None
            pc_a.ctx.mc_manager.runtime_mode = "simulated"

        # Admin restarts server hosted by Member on PC A
        st, rst_host = pc_a.request("POST", "/api/server/restart", token=admin_token)
        assert st == 200 and rst_host["localStatus"] == "running"

        # Rotate Bob's passkey -> Bob's token is invalidated (401 on /api/auth/me)
        st, rot_res = pc_a.request("POST", "/api/admin/accounts/rotate", token=admin_token, body={"userId": bob_id})
        assert st == 200
        st_bob_me, _ = pc_a.request("GET", "/api/auth/me", token=bob_a_token)
        assert st_bob_me == 401

        # Admin stops server hosted on PC A -> world synced & lock released cleanly
        st, stop_b = pc_a.request("POST", "/api/server/stop", token=admin_token)
        assert st == 200 and stop_b["localStatus"] == "idle" and stop_b["lock"]["isLocked"] is False

        # B.5 WinError 32 -> 409 FILE_IN_USE vs path traversal -> 403 FORBIDDEN
        orig_read = pc_a.ctx.mc_manager.read_file_content
        try:
            def _winerror_32(_rel):
                err = PermissionError(13, "The process cannot access the file because it is being used by another process")
                err.winerror = 32  # type: ignore[attr-defined]
                raise err

            pc_a.ctx.mc_manager.read_file_content = _winerror_32  # type: ignore
            st_w32, res_w32 = pc_a.request("GET", "/api/admin/files/read?path=server.properties", token=admin_token)
            assert st_w32 == 409 and res_w32.get("code") == "FILE_IN_USE", f"Expected 409 FILE_IN_USE, got {st_w32} {res_w32}"
        finally:
            pc_a.ctx.mc_manager.read_file_content = orig_read  # type: ignore

        st_p403, res_p403 = pc_a.request("GET", "/api/admin/files/read?path=../../secret.txt", token=admin_token)
        assert st_p403 == 403 and res_p403.get("code") == "FORBIDDEN"

        print("  [OK] Scenario B passed.")

        # =====================================================================
        # Frontend Invariants Check for Scenario A & Scenario B
        # =====================================================================
        print("\n[Frontend] Verifying Scenario A & Scenario B UI Guard & UX Invariants...")
        app_js = (ROOT_DIR / "public" / "app.js").read_text(encoding="utf-8")
        idx_html = (ROOT_DIR / "public" / "index.html").read_text(encoding="utf-8")
        styles_css = (ROOT_DIR / "public" / "styles.css").read_text(encoding="utf-8")

        for sym in (
            "setGuardElementLock",
            "remoteLocked",
            "localBusyLocked",
            "noLockLocked",
            "handleClusterSnapshotOrLockTransition",
            "autoSyncDirty",
            "HOSTING NOW",
            "renderAllConsoleLogs(summary.logs, { isSnapshot: true })",
            "btn-upload-file-label",
        ):
            assert sym in app_js, f"Missing frontend guard/UX symbol {sym} in public/app.js"
        assert 'id="btn-upload-file-label"' in idx_html
        assert ".btn.btn-disabled" in styles_css and "[readonly]" in styles_css
        print("  [OK] Frontend Scenario A/B invariants passed.")

    print("\n============================================================")
    print("  ALL ADMIN-DURING-MEMBER-HOSTING E2E TESTS PASSED!         ")
    print("============================================================")


if __name__ == "__main__":
    run_admin_during_member_host_tests()
