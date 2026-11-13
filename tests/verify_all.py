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
from typing import Any, Optional

os.environ["MINERELAY_TEST_MODE"] = "1"

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.server import AppContext, create_request_handler
from src.bootstrap_runtime import required_java_major_for_mc


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


def run_verification():
    print("============================================================")
    print("  Running End-to-End Verification Suite for MineRelay")
    print("============================================================")

    assert required_java_major_for_mc("26.3") == 25
    assert required_java_major_for_mc("26.1") == 25
    assert required_java_major_for_mc("1.21.4") == 21
    print("[Test 0A] Verified Minecraft 26.3 Java 25 requirement mapping.")

    temp_root = Path(tempfile.mkdtemp(prefix="minerelay_test_"))
    shared_cloud_dir = temp_root / "Google Drive" / "MineRelay-Shared"
    pc_one_data = temp_root / "pc_alice_data"
    pc_two_data = temp_root / "pc_bob_data"

    try:
        pc1 = InMemoryHTTPClient(data_dir=str(pc_one_data))
        pc2 = InMemoryHTTPClient(data_dir=str(pc_two_data))

        st_html, res_html = pc1.request("GET", "/")
        assert st_html == 200 and "MineRelay" in res_html.get("raw", ""), "Failed serving index.html"
        print("[Test 0B] Static Web UI (index.html, styles.css, app.js) verified.")

        # ---------------------------------------------------------------------
        # 1. Initial Cluster Setup & Single Admin Manager Account Creation
        # ---------------------------------------------------------------------
        print("\n[Test 1] Initializing Cluster with Single Admin Manager Account on PC 1...")
        status, init_res = pc1.request(
            "POST",
            "/api/setup/init",
            body={
                "serverName": "Friends SMP Server",
                "adminUsername": "MasterAdmin",
                "machineName": "Admin-Rig",
                "sharedSyncDir": str(shared_cloud_dir),
            },
        )
        assert status == 200, f"Expected 200, got {status}: {init_res}"
        admin_token = init_res["token"]
        admin_passkey = init_res["generatedPasskey"]
        assert init_res["cluster"]["mcVersion"] == "26.3", f"Expected default version 26.3, got {init_res['cluster']['mcVersion']}"
        print(f"  [OK] Admin Manager initialized (Default MC Version: {init_res['cluster']['mcVersion']}, Passkey: {admin_passkey})")

        # Point PC 2 to the same Shared Google Drive Folder
        status, sync_res = pc2.request(
            "POST",
            "/api/setup/sync-dir",
            body={"sharedSyncDir": str(shared_cloud_dir), "machineName": "Bob-Laptop"},
        )
        assert status == 200 and sync_res["initialized"] is True, f"PC 2 failed to detect cluster: {sync_res}"
        assert sync_res["syncDiagnostics"]["providerDetected"] == "Google Drive"
        print("  [OK] PC 2 connected to Shared Google Drive Folder and verified sync diagnostics.")

        # ---------------------------------------------------------------------
        # 2. Create Member Accounts with Unique Passkeys
        # ---------------------------------------------------------------------
        print("\n[Test 2] Creating Member Accounts (Alice & Bob) with unique passkeys...")
        status, alice_res = pc1.request(
            "POST",
            "/api/admin/accounts",
            token=admin_token,
            body={"username": "Alice"},
        )
        assert status == 201
        alice_passkey = alice_res["generatedPasskey"]
        assert alice_res["account"]["passkeyHint"] == f"{alice_passkey.removeprefix('MC-HOST-')[:2]}••••"

        status, bob_res = pc1.request(
            "POST",
            "/api/admin/accounts",
            token=admin_token,
            body={"username": "Bob"},
        )
        assert status == 201
        bob_passkey = bob_res["generatedPasskey"]
        bob_id = bob_res["account"]["id"]
        print(f"  [OK] Created Alice ({alice_passkey}) & Bob ({bob_passkey})")

        # ---------------------------------------------------------------------
        # 3. Sign In as Members & Verify Strict RBAC (Admin vs Member Perms)
        # ---------------------------------------------------------------------
        print("\n[Test 3] Verifying Member Sign-In and Strict Role-Based Access Control...")
        status, alice_login = pc1.request(
            "POST",
            "/api/auth/login",
            body={"passkey": alice_passkey, "machineName": "Alice-PC"},
        )
        assert status == 200
        alice_token = alice_login["token"]

        status, bob_login = pc2.request(
            "POST",
            "/api/auth/login",
            body={"passkey": bob_passkey, "machineName": "Bob-Laptop"},
        )
        assert status == 200
        bob_token = bob_login["token"]

        for forbidden_call in [
            ("POST", "/api/admin/accounts", {"username": "Hacker"}),
            ("PUT", "/api/admin/properties", {"properties": {"gamemode": "creative"}}),
            ("POST", "/api/admin/console", {"command": "op Alice"}),
            ("POST", "/api/admin/force-unlock", {}),
            ("DELETE", f"/api/admin/accounts/{bob_id}", None),
        ]:
            m, p, b = forbidden_call
            st_code, err_payload = pc1.request(m, p, token=alice_token, body=b)
            assert st_code == 403, f"Expected 403 Forbidden for Member on {m} {p}, got {st_code}: {err_payload}"
        print("  [OK] Confirmed Member accounts receive 403 Forbidden on all Admin Manager endpoints.")

        st_code, _ = pc1.request(
            "PUT",
            "/api/admin/settings",
            token=admin_token,
            body={"publicJoinAddress": "friends-smp.gl.joinmc.link"},
        )
        assert st_code == 200
        st_code, prop_res = pc1.request(
            "PUT",
            "/api/admin/properties",
            token=admin_token,
            body={"properties": {"motd": "Synced Friends Server", "max-players": "25"}},
        )
        assert st_code == 200 and prop_res["properties"]["max-players"] == "25"
        print("  [OK] Confirmed Admin Manager can update server.properties and Zero-Port-Forward public address.")

        # ---------------------------------------------------------------------
        # 4. Distributed Hosting & Mutual Exclusion Lock Test
        # ---------------------------------------------------------------------
        print("\n[Test 4] Testing Distributed Hosting & Mutual Exclusion across PC 1 (Alice) and PC 2 (Bob)...")
        st_code, start_res = pc1.request("POST", "/api/server/start", token=alice_token)
        assert st_code == 200 and start_res["localStatus"] == "running"
        print("  [OK] Alice claimed host lock and started the server on PC 1.")

        st_code, conflict_res = pc2.request("POST", "/api/server/start", token=bob_token)
        assert st_code == 409 and conflict_res["code"] == "HOST_LOCK_CONFLICT"
        print(f"  [OK] Bob's concurrent host attempt was blocked (409 Conflict): {conflict_res['error']}")

        st_code, stop_res = pc1.request("POST", "/api/server/stop", token=alice_token)
        assert st_code == 200 and stop_res["lock"]["isLocked"] is False
        print("  [OK] Alice stopped server, world snapshot saved to Shared Google Drive Folder, and lock released.")

        # ---------------------------------------------------------------------
        # 5. Handoff to Bob on PC 2 & Cross-Device Idle Config Edit Safety
        # ---------------------------------------------------------------------
        print("\n[Test 5] Testing Host Handoff to Bob on PC 2 & Cross-Device World Sync Preservation...")
        st_code, bob_start = pc2.request("POST", "/api/server/start", token=bob_token)
        assert st_code == 200 and bob_start["localStatus"] == "running"

        # Simulate Bob building a new structure in the world on PC 2
        bob_world_marker = pc_two_data / "runtime-server" / "world" / "bob-castle.json"
        bob_world_marker.write_text('{"builtBy": "Bob", "blocks": 5000}', encoding="utf-8")

        # Bob stops hosting on PC 2 -> pushes snapshot with bob-castle.json to Google Drive
        time.sleep(0.05)
        st_code, bob_stop = pc2.request("POST", "/api/server/stop", token=bob_token)
        assert st_code == 200 and bob_stop["lock"]["isLocked"] is False

        # Now Admin on PC 1 (which is idle and has NOT hosted since Alice's session!) edits server.properties
        st_code, prop_res2 = pc1.request(
            "PUT",
            "/api/admin/properties",
            token=admin_token,
            body={"properties": {"difficulty": "hard"}},
        )
        assert st_code == 200 and prop_res2["properties"]["difficulty"] == "hard"

        # Verify PC 1 automatically pulled Bob's snapshot BEFORE pushing the updated server.properties!
        pc1_pulled_marker = pc_one_data / "runtime-server" / "world" / "bob-castle.json"
        assert pc1_pulled_marker.exists(), "PC 1 failed to pull Bob's world snapshot before idle config edit!"
        print("  [OK] PC 1 automatically pulled Bob's world changes before applying idle config edit (zero data loss!).")

        # Verify Google Drive conflict file cleanup
        conflict_file = shared_cloud_dir / "host-lock (1).json"
        conflict_file.write_text("{}", encoding="utf-8")
        pc1.ctx.sync_engine._cleanup_cloud_conflict_files()
        assert not conflict_file.exists(), "Google Drive conflict duplicate file was not cleaned up!"
        print("  [OK] Verified automatic cleanup of Google Drive duplicate conflict files.")

        # ---------------------------------------------------------------------
        # 6. Admin Stop -> Sync -> Re-Host & Failed Startup Preservation Test
        # ---------------------------------------------------------------------
        print("\n[Test 6] Testing Admin Stop -> Sync -> Re-Host & Startup Crash Protection...")
        st_code, adm_start1 = pc1.request("POST", "/api/server/start", token=admin_token)
        assert st_code == 200 and adm_start1["localStatus"] == "running"

        adm_marker = pc_one_data / "runtime-server" / "world" / "admin-spawn-tower.json"
        adm_marker.write_text('{"builtBy": "AdminManager"}', encoding="utf-8")

        st_code, adm_stop1 = pc1.request("POST", "/api/server/stop", token=admin_token)
        assert st_code == 200 and adm_stop1["localStatus"] == "idle" and adm_stop1["lock"]["isLocked"] is False

        # Simulate a corrupted/empty local runtime folder during a failed startup before reaching 'running'
        shutil.rmtree(pc_one_data / "runtime-server" / "world", ignore_errors=True)
        pc1.ctx.mc_manager._cleanup_after_failed_start(None, reason="Simulated port bind failure")
        assert pc1.ctx.mc_manager.last_error == "Simulated port bind failure"

        # Now Admin clicks Start Hosting again -> must pull the intact latest-server.zip with admin-spawn-tower.json!
        st_code, adm_start2 = pc1.request("POST", "/api/server/start", token=admin_token)
        assert st_code == 200 and adm_start2["localStatus"] == "running"
        assert adm_marker.exists(), "World snapshot was lost after failed start cleanup!"
        st_code, adm_stop2 = pc1.request("POST", "/api/server/stop", token=admin_token)
        assert st_code == 200 and adm_stop2["localStatus"] == "idle"
        print("  [OK] Admin Stop -> Sync -> Re-Host cycle and failed startup protection verified.")

        # ---------------------------------------------------------------------
        # 7. Online Player Detection, ANSI Stripping, Prefixes & Mutex Thread Safety
        # ---------------------------------------------------------------------
        print("\n[Test 7] Testing Online Player Detection, ANSI Stripping, Prefixes & Mutex Thread Safety...")
        st_code, start_res = pc1.request("POST", "/api/server/start", token=admin_token)
        assert st_code == 200 and start_res["localStatus"] == "running"

        # Verify initial player state is empty
        assert pc1.ctx.mc_manager.online_players == []

        # Feed stdout lines simulating PaperMC console with ANSI escape sequences
        class MockProc:
            def __init__(self, lines):
                self.stdout = lines
                self.pid = 99999

        sample_lines = [
            # Paper colored log with ANSI escape codes
            "\x1b[32m[12:33:14 INFO]\x1b[0m: \x1b[33mProudHogRider\x1b[0m joined the game\n",
            # Bedrock Floodgate prefix (.)
            "[12:33:15 INFO]: .BedrockPlayer joined the game\n",
            # Canonical login with Bedrock prefix (*)
            "[12:33:16 INFO]: *ConsoleUser[/127.0.0.1:54321] logged in with entity id 125 at (world, 0, 64, 0)\n",
            # Duplicate join message should not create duplicate in list
            "[12:33:17 INFO]: *ConsoleUser joined the game\n",
            # Player leave
            "[12:33:20 INFO]: ProudHogRider lost connection: Disconnected\n",
            # Player kick / disconnect
            "[12:33:22 INFO]: Kicking .BedrockPlayer: Timed out\n",
            # Minecraft list command output format
            "[12:33:25 INFO]: There are 2 of a max of 20 players online: *ConsoleUser, ServerGuest\n",
        ]

        mock_proc = MockProc(sample_lines)
        pc1.ctx.mc_manager._monitor_java_process(mock_proc, max_players=20)

        # Confirm manager parsed lines accurately
        assert pc1.ctx.mc_manager.online_players == ["*ConsoleUser", "ServerGuest"], f"Unexpected online players: {pc1.ctx.mc_manager.online_players}"

        # Test simulator quick commands
        pc1.ctx.mc_manager.send_command("simjoin TestPlayer")
        assert "TestPlayer" in pc1.ctx.mc_manager.online_players
        pc1.ctx.mc_manager.send_command("simleave TestPlayer")
        assert "TestPlayer" not in pc1.ctx.mc_manager.online_players

        # Test concurrent thread-safety of online_players
        def _concurrent_worker(worker_id):
            pname = f"ThreadPlayer_{worker_id}"
            pc1.ctx.mc_manager.send_command(f"simjoin {pname}")
            _ = pc1.ctx.mc_manager.get_players_data()
            _ = pc1.ctx.mc_manager.get_status_summary()
            pc1.ctx.mc_manager.send_command(f"simleave {pname}")

        threads = [threading.Thread(target=_concurrent_worker, args=(i,)) for i in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Stop server and confirm player list is cleanly reset
        st_code, stop_res = pc1.request("POST", "/api/server/stop", token=admin_token)
        assert st_code == 200 and stop_res["localStatus"] == "idle"
        assert pc1.ctx.mc_manager.online_players == []
        lock = pc1.ctx.sync_engine.get_lock()
        assert lock.online_players == []
        print("  [OK] Online player detection, ANSI stripping, Bedrock prefixes, and mutex thread safety verified.")

        # ---------------------------------------------------------------------
        # 8. Real-Time SSE Event Queue Broadcasting
        # ---------------------------------------------------------------------
        print("\n[Test 8] Testing Real-Time SSE Event Queue Broadcasting...")
        sse_client_q = queue.Queue(maxsize=100)
        with pc1.ctx.sse_mutex:
            pc1.ctx.sse_clients.append(sse_client_q)

        st_code, _ = pc1.request("POST", "/api/server/start", token=admin_token)
        assert st_code == 200

        # Drain start events
        while not sse_client_q.empty():
            sse_client_q.get_nowait()

        # Add a simulated player and verify SSE events
        pc1.ctx.mc_manager.send_command("simjoin Alex")

        received_events = {}
        for _ in range(10):
            try:
                raw_msg = sse_client_q.get(timeout=1.0)
                evt = json.loads(raw_msg)
                received_events[evt["event"]] = evt["data"]
            except queue.Empty:
                break

        assert "status_changed" in received_events, "status_changed event was not emitted on player join"
        assert "Alex" in received_events["status_changed"]["onlinePlayers"], "status_changed did not include online player"
        assert "players_updated" in received_events, "players_updated event was not emitted on player join"
        assert "Alex" in received_events["players_updated"]["online"], "players_updated did not include online player"

        # Ban or kick player and check player_action event emission
        pc1.ctx.mc_manager.player_action("kick", "Alex", "Testing kick event")
        kick_events = {}
        for _ in range(10):
            try:
                raw_msg = sse_client_q.get(timeout=1.0)
                evt = json.loads(raw_msg)
                kick_events[evt["event"]] = evt["data"]
            except queue.Empty:
                break

        assert "status_changed" in kick_events
        assert "Alex" not in kick_events["status_changed"]["onlinePlayers"]
        assert "players_updated" in kick_events
        assert "Alex" not in kick_events["players_updated"]["online"]

        # Test resilient broadcasting with non-serializable payload (default=str fallback)
        pc1.ctx.broadcast_event("test_non_serializable", {"path": Path("/test/path")})
        res_msg = sse_client_q.get(timeout=1.0)
        evt = json.loads(res_msg)
        assert evt["event"] == "test_non_serializable"
        assert "path" in evt["data"]

        with pc1.ctx.sse_mutex:
            if sse_client_q in pc1.ctx.sse_clients:
                pc1.ctx.sse_clients.remove(sse_client_q)

        st_code, _ = pc1.request("POST", "/api/server/stop", token=admin_token)
        assert st_code == 200
        print("  [OK] SSE queue dispatching for player updates and resilient broadcasting verified.")

        # ---------------------------------------------------------------------
        # 9. Dedicated Player REST API Endpoints & RBAC Accessibility
        # ---------------------------------------------------------------------
        print("\n[Test 9] Testing Dedicated Player REST Endpoints (/api/players & /api/server/status)...")
        st_code, _ = pc1.request("POST", "/api/server/start", token=admin_token)
        assert st_code == 200
        pc1.ctx.mc_manager.send_command("simjoin Charlie")

        # Admin calls GET /api/players
        st, res_admin = pc1.request("GET", "/api/players", token=admin_token)
        assert st == 200
        assert res_admin["online"] == ["Charlie"]
        assert res_admin["count"] == 1
        assert res_admin["max"] == 25

        # Member (Alice) calls GET /api/players (Must be 200 OK, NOT 403 Forbidden!)
        st, res_alice = pc1.request("GET", "/api/players", token=alice_token)
        assert st == 200
        assert res_alice["online"] == ["Charlie"]
        assert res_alice["count"] == 1
        assert res_alice["max"] == 25

        # Member (Alice) calls GET /api/server/status -> must include top-level onlinePlayers
        st, res_status = pc1.request("GET", "/api/server/status", token=alice_token)
        assert st == 200
        assert "onlinePlayers" in res_status
        assert res_status["onlinePlayers"] == ["Charlie"]

        # Admin calls GET /api/admin/players
        st, res_adm_players = pc1.request("GET", "/api/admin/players", token=admin_token)
        assert st == 200
        assert res_adm_players["online"] == ["Charlie"]

        # Unauthenticated request must return 401
        st, res_unauth = pc1.request("GET", "/api/players")
        assert st == 401

        pc1.ctx.mc_manager.send_command("simleave Charlie")
        st_code, _ = pc1.request("POST", "/api/server/stop", token=admin_token)
        assert st_code == 200
        print("  [OK] Dedicated /api/players endpoint and status onlinePlayers verified for admin and members.")

        # ---------------------------------------------------------------------
        # 10. Server API Input Validation & Exception Mapping
        # ---------------------------------------------------------------------
        print("\n[Test 10] Testing API Input Validation & Exception Status Codes...")
        # 10.1 Non-dict JSON body returns 400 Bad Request
        st, res = pc1.request("POST", "/api/auth/login", body=["not", "a", "dict"])
        assert st == 400
        assert res.get("code") == "BAD_REQUEST"

        # 10.2 Invalid port range in settings returns 400 Bad Request
        st, res = pc1.request("PUT", "/api/admin/settings", token=admin_token, body={"serverPort": 70000})
        assert st == 400
        assert res.get("code") == "BAD_REQUEST"

        st, res = pc1.request("PUT", "/api/admin/settings", token=admin_token, body={"serverPort": 0})
        assert st == 400
        assert res.get("code") == "BAD_REQUEST"

        # 10.3 Non-dict properties update returns 400 Bad Request
        st, res = pc1.request("PUT", "/api/admin/properties", token=admin_token, body={"properties": "invalid"})
        assert st == 400
        assert res.get("code") == "BAD_REQUEST"

        # 10.4 Empty file path in files/write returns 400 Bad Request
        st, res = pc1.request("POST", "/api/admin/files/write", token=admin_token, body={"path": "  ", "content": "xyz"})
        assert st == 400
        assert res.get("code") == "BAD_REQUEST"

        # 10.5 Empty filename in files/upload returns 400 Bad Request
        st, res = pc1.request("POST", "/api/admin/files/upload", token=admin_token, body={"dir": "", "filename": "  ", "dataBase64": "AA=="})
        assert st == 400
        assert res.get("code") == "BAD_REQUEST"

        # 10.6 Backup restore while locked/running returns 400 Bad Request
        st_code, _ = pc1.request("POST", "/api/server/start", token=admin_token)
        assert st_code == 200
        st, res = pc1.request("POST", "/api/admin/backups/restore", token=admin_token, body={"name": "dummy"})
        assert st == 400
        assert "running" in res.get("error", "").lower()

        st_code, _ = pc1.request("POST", "/api/server/stop", token=admin_token)
        assert st_code == 200
        print("  [OK] Input validation and 400 BAD_REQUEST exception handling verified across all endpoints.")

        # ---------------------------------------------------------------------
        # 11. Static UI Assets Serving & XSS Sanitization Audit
        # ---------------------------------------------------------------------
        print("\n[Test 11] Verifying Static UI Assets & Frontend XSS Sanitization...")
        st_css, res_css = pc1.request("GET", "/styles.css")
        assert st_css == 200
        assert len(res_css.get("raw", "")) > 100

        st_js, res_js = pc1.request("GET", "/app.js")
        assert st_js == 200
        js_content = res_js.get("raw", "")
        assert "function escapeHtml" in js_content, "escapeHtml function is missing from app.js"
        assert "escapeHtml(acc.username)" in js_content, "Account username is not escaped in app.js"
        assert "escapeHtml(b.name)" in js_content, "Backup name is not escaped in app.js"
        assert "escapeHtml(f.name)" in js_content, "File name is not escaped in app.js"
        assert "renderOnlinePlayersList" in js_content, "renderOnlinePlayersList function is missing"
        assert "players_updated" in js_content, "players_updated SSE listener is missing"
        assert "heartbeat" in js_content, "heartbeat SSE listener is missing"
        assert "es.onerror" in js_content, "EventSource onerror handler is missing"
        assert "onclick=" not in js_content, "Inline onclick attributes remain in app.js"
        assert "Admin Manager" not in js_content, "'Admin Manager' string still present in app.js"
        assert "Admin Manager" not in res_html.get("raw", ""), "'Admin Manager' string still present in index.html"
        print("  [OK] Static assets served correctly, 'Admin' title verified, and XSS sanitization verified in app.js.")

        # ---------------------------------------------------------------------
        # 12. QoL Updates, Bug Fixes & Pro Hosting Features Verification
        # ---------------------------------------------------------------------
        print("\n[Test 12] Verifying QoL Updates, UUID Bug Fix, Gamerules, Plugins, Restart & Account Rename...")

        # 12.1 Rename Account (Admin & Member)
        admin_acc_id = init_res["account"]["id"]
        st, ren_res = pc1.request(
            "POST",
            "/api/admin/accounts/rename",
            token=admin_token,
            body={"userId": admin_acc_id, "username": "Admin"},
        )
        assert st == 200 and ren_res["account"]["username"] == "Admin"

        # 12.2 Valid 36-character RFC 4122 UUIDs in ops.json, whitelist.json, banned-players.json
        st, op_res = pc1.request(
            "POST",
            "/api/admin/players",
            token=admin_token,
            body={"action": "op", "username": "Notch"},
        )
        assert st == 200
        notch_op = next((o for o in op_res["ops"] if o["name"] == "Notch"), None)
        assert notch_op is not None
        assert len(notch_op["uuid"]) == 36 and not notch_op["uuid"].startswith("offline-uuid-"), (
            f"Invalid UUID generated: {notch_op['uuid']}"
        )

        # Verify legacy offline-uuid-* auto-upgrade
        ops_file = pc_one_data / "runtime-server" / "ops.json"
        ops_file.write_text(
            json.dumps([{"uuid": "offline-uuid-LegacyUser", "name": "LegacyUser", "level": 4}]),
            encoding="utf-8",
        )
        st, players_check = pc1.request("GET", "/api/admin/players", token=admin_token)
        assert st == 200
        legacy_op = next((o for o in players_check["ops"] if o["name"] == "LegacyUser"), None)
        assert legacy_op is not None and len(legacy_op["uuid"]) == 36 and not legacy_op["uuid"].startswith("offline-uuid-")

        # 12.3 World Gamerules GET & PUT
        st, gr_get = pc1.request("GET", "/api/admin/gamerules", token=admin_token)
        assert st == 200 and "keepInventory" in gr_get["gamerules"]
        st, gr_put = pc1.request(
            "PUT",
            "/api/admin/gamerules",
            token=admin_token,
            body={"gamerules": {"keepInventory": True, "playersSleepingPercentage": 1, "mobGriefing": False}},
        )
        assert st == 200
        assert gr_put["gamerules"]["keepInventory"] is True
        assert gr_put["gamerules"]["playersSleepingPercentage"] == 1
        assert gr_put["gamerules"]["mobGriefing"] is False

        # 12.4 Idle Auto-Shutdown Setting & Port Sync to server.properties
        st, set_res = pc1.request(
            "PUT",
            "/api/admin/settings",
            token=admin_token,
            body={"idleShutdownMinutes": 15, "serverPort": 25570},
        )
        assert st == 200 and set_res["cluster"]["idleShutdownMinutes"] == 15
        st, props_after_port = pc1.request("GET", "/api/admin/properties", token=admin_token)
        assert props_after_port["properties"]["server-port"] == "25570"

        # 12.5 PaperMC Plugin Marketplace & Manager (Search, Install, Toggle, Delete)
        st, plg_search = pc1.request("GET", "/api/admin/plugins?search=worldedit", token=admin_token)
        assert st == 200 and len(plg_search.get("catalog", [])) >= 1
        st, plg_inst = pc1.request(
            "POST",
            "/api/admin/plugins/install",
            token=admin_token,
            body={"projectId": "worldedit", "title": "WorldEdit"},
        )
        assert st == 200
        installed_names = [p["filename"] for p in plg_inst["plugins"]]
        assert "WorldEdit.jar" in installed_names

        st, plg_tog = pc1.request(
            "POST",
            "/api/admin/plugins/toggle",
            token=admin_token,
            body={"filename": "WorldEdit.jar"},
        )
        assert st == 200
        we_entry = next((p for p in plg_tog["plugins"] if p["name"] == "WorldEdit"), None)
        assert we_entry is not None and we_entry["enabled"] is False and we_entry["filename"] == "WorldEdit.jar.disabled"

        st, plg_del = pc1.request(
            "DELETE",
            "/api/admin/plugins?filename=WorldEdit.jar.disabled",
            token=admin_token,
        )
        assert st == 200 and len(plg_del["plugins"]) == 0

        # 12.6 Server Restart, Live Player Quick Actions & Console Clear
        st, _ = pc1.request("POST", "/api/server/start", token=admin_token)
        assert st == 200
        pc1.ctx.mc_manager.send_command("simjoin Steve")
        st, _ = pc1.request(
            "POST",
            "/api/admin/players",
            token=admin_token,
            body={"action": "gamemode", "username": "Steve", "value": "creative"},
        )
        assert st == 200
        st, _ = pc1.request(
            "POST",
            "/api/admin/players",
            token=admin_token,
            body={"action": "heal", "username": "Steve"},
        )
        assert st == 200

        st, restart_res = pc1.request("POST", "/api/server/restart", token=admin_token)
        assert st == 200 and restart_res["localStatus"] == "running"

        st, clr_res = pc1.request("POST", "/api/admin/console/clear", token=admin_token)
        assert st == 200 and clr_res["ok"] is True

        st, _ = pc1.request("POST", "/api/server/stop", token=admin_token)
        assert st == 200

        # 12.7 Direct Backup Download
        st, bk_list = pc1.request("GET", "/api/admin/backups", token=admin_token)
        assert st == 200 and len(bk_list["backups"]) >= 1
        bk_name = bk_list["backups"][0]["name"]
        st_dl, _ = pc1.request("GET", f"/api/admin/backups/download?name={bk_name}", token=admin_token)
        assert st_dl == 200

        print("  [OK] Account rename, valid RFC 4122 UUIDs, Gamerules, Plugin Marketplace, Server Restart & Backup Download verified!")

        # ---------------------------------------------------------------------
        # 13. World Map & Seed Generator (Random Map & Specific Seed)
        # ---------------------------------------------------------------------
        print("\n[Test 13] Testing World Map & Seed Generator (Random Map & Specific Seed)...")

        # 13.1 Member cannot reset world map (Admin-only)
        st_mem, _ = pc1.request(
            "POST",
            "/api/admin/world/reset",
            token=alice_token,
            body={"mode": "random"},
        )
        assert st_mem == 403, f"Expected 403 for member world reset, got {st_mem}"

        # 13.2 Create a marker file in the existing world directory to verify old world is wiped
        marker_file = pc1.ctx.mc_manager.runtime_dir / "world" / "old_chunk_marker.txt"
        marker_file.parent.mkdir(parents=True, exist_ok=True)
        marker_file.write_text("old-world-data", encoding="utf-8")
        assert marker_file.exists()

        # 13.3 Reset world map with a SPECIFIC seed while server is idle (with safety backup)
        custom_seed = "1691256543523180978"
        st_rst, rst_res = pc1.request(
            "POST",
            "/api/admin/world/reset",
            token=admin_token,
            body={
                "mode": "custom",
                "seed": custom_seed,
                "levelType": "minecraft:large_biomes",
                "generateStructures": True,
                "backupExisting": True,
            },
        )
        assert st_rst == 200, f"Expected 200 on custom seed reset, got {st_rst}: {rst_res}"
        assert rst_res["reset"] is True
        assert rst_res["mode"] == "custom"
        assert rst_res["seed"] == custom_seed
        assert rst_res["levelType"] == "minecraft:large_biomes"
        assert rst_res["backupFile"] is not None and "pre-world-reset" in rst_res["backupFile"]
        assert not marker_file.exists(), "Old world marker file should have been deleted during world reset"

        # 13.4 Verify PC 2 receives the new seed and world config via Shared Sync Folder
        pc2.ctx.sync_engine.pull_world_to_runtime()
        pc2_world = pc2.ctx.mc_manager.get_world_info()
        assert pc2_world["seed"] == custom_seed
        assert pc2_world["levelType"] == "minecraft:large_biomes"

        # 13.5 Start server on PC 1, then trigger a RANDOM world map generation while server is running
        st_start, _ = pc1.request("POST", "/api/server/start", token=admin_token)
        assert st_start == 200
        assert (pc1.ctx.mc_manager.runtime_dir / "world" / "level-metadata.json").exists()

        st_rand, rand_res = pc1.request(
            "POST",
            "/api/admin/world/reset",
            token=admin_token,
            body={
                "mode": "random",
                "levelType": "minecraft:amplified",
                "generateStructures": False,
                "backupExisting": True,
            },
        )
        assert st_rand == 200, f"Expected 200 on random world reset while running, got {st_rand}: {rand_res}"
        assert rand_res["reset"] is True
        assert rand_res["mode"] == "random"
        assert rand_res["restarted"] is True
        assert rand_res["seed"] != "" and rand_res["seed"] != custom_seed
        assert rand_res["levelType"] == "minecraft:amplified"
        assert rand_res["world"]["generateStructures"] is False
        assert pc1.ctx.mc_manager.local_status == "running"

        # Confirm new world metadata has the newly generated random seed
        new_meta = json.loads(
            (pc1.ctx.mc_manager.runtime_dir / "world" / "level-metadata.json").read_text(encoding="utf-8")
        )
        assert new_meta["seed"] == rand_res["seed"]

        st_stop, _ = pc1.request("POST", "/api/server/stop", token=admin_token)
        assert st_stop == 200
        print(f"  [OK] World Map & Seed Generator verified (Custom Seed: {custom_seed}, Random Seed: {rand_res['seed']}).")

        # ---------------------------------------------------------------------
        # 14. Hardened Security, Edge-Cases & Integrity Verification
        # ---------------------------------------------------------------------
        print("\n[Test 14] Testing Hardened Security, Edge-Cases & Integrity Verification...")

        # 14.1 Zip-Slip path traversal protection in SyncEngine
        slip_zip = temp_root / "slip.zip"
        with zipfile.ZipFile(slip_zip, "w") as zf:
            zf.writestr("../traversal.txt", "malicious payload")
        try:
            with zipfile.ZipFile(slip_zip, "r") as zf:
                pc1.ctx.sync_engine._safe_extract_zip(zf, pc1.ctx.sync_engine.runtime_dir)
            assert False, "Expected ValueError on Zip-Slip path traversal archive"
        except ValueError as e:
            assert "Zip path traversal detected" in str(e)
            print("  [OK] Zip-Slip path traversal extraction blocked successfully.")

        # 14.2 Reserved and malicious level-name sanitization
        assert pc1.ctx.mc_manager._sanitize_level_name("plugins") == "world"
        assert pc1.ctx.mc_manager._sanitize_level_name("cache") == "world"
        assert pc1.ctx.mc_manager._sanitize_level_name("../../etc/passwd") == "world"
        assert pc1.ctx.mc_manager._sanitize_level_name("..") == "world"
        assert pc1.ctx.mc_manager._sanitize_level_name("My Super_World 2") == "My Super_World 2"
        print("  [OK] Level-name sanitization and reserved folder protection verified.")

        # 14.3 server.properties newline injection prevention
        st_inj, _ = pc1.request(
            "PUT",
            "/api/admin/properties",
            token=admin_token,
            body={"properties": {"motd\ninjected=bad": "welcome\nnewline=bad"}},
        )
        assert st_inj == 200
        props_file = (pc1.ctx.mc_manager.runtime_dir / "server.properties").read_text(encoding="utf-8")
        assert "injected=bad" not in props_file.splitlines(), "Newline injected into server.properties lines"
        assert "\nmotdinjected=bad=welcomenewline=bad\n" in props_file or "motdinjected=bad=welcomenewline=bad" in props_file
        print("  [OK] server.properties newline injection protection verified.")

        # 14.4 Boolean values rejected for integer settings fields
        st_bool_port, d_bp = pc1.request("PUT", "/api/admin/settings", token=admin_token, body={"serverPort": True})
        assert st_bool_port == 400 and d_bp.get("code") == "BAD_REQUEST"
        st_bool_mem, d_bm = pc1.request("PUT", "/api/admin/settings", token=admin_token, body={"memoryMinMb": False})
        assert st_bool_mem == 400 and d_bm.get("code") == "BAD_REQUEST"
        st_bool_bk, d_bk = pc1.request("PUT", "/api/admin/settings", token=admin_token, body={"maxBackups": True})
        assert st_bool_bk == 400 and d_bk.get("code") == "BAD_REQUEST"
        print("  [OK] Boolean values safely rejected for integer settings fields (serverPort, memoryMinMb, maxBackups).")

        # 14.5 File manager directory target guards
        (pc1.ctx.mc_manager.runtime_dir / "plugins").mkdir(parents=True, exist_ok=True)
        st_wr_dir, d_wrd = pc1.request(
            "POST",
            "/api/admin/files/write",
            token=admin_token,
            body={"path": "plugins", "content": "not_allowed"},
        )
        assert st_wr_dir == 400, f"Expected 400 when writing to directory path, got {st_wr_dir}: {d_wrd}"

        st_up_dot, d_upd = pc1.request(
            "POST",
            "/api/admin/files/upload",
            token=admin_token,
            body={"dir": "plugins", "filename": "..", "dataBase64": "dGVzdA=="},
        )
        assert st_up_dot == 400, f"Expected 400 when uploading with dot filename, got {st_up_dot}: {d_upd}"
        print("  [OK] File manager directory targets and traversal upload guards verified.")

        # 14.6 Plugin toggle collision resilience on Windows
        plug_jar = pc1.ctx.mc_manager.runtime_dir / "plugins" / "CollisionTest.jar"
        plug_dis = pc1.ctx.mc_manager.runtime_dir / "plugins" / "CollisionTest.jar.disabled"
        plug_jar.write_bytes(b"jar-content")
        plug_dis.write_bytes(b"old-disabled-content")
        toggle_res = pc1.ctx.mc_manager.toggle_plugin("CollisionTest.jar")
        assert plug_dis.read_bytes() == b"jar-content"
        plug_dis.unlink(missing_ok=True)
        print("  [OK] Plugin toggle overwrite collision resilience verified.")

        # 14.7 Reject changing Shared Sync Folder while server is running
        st_s_run, _ = pc1.request("POST", "/api/server/start", token=admin_token)
        assert st_s_run == 200
        new_test_shared = temp_root / "NewSharedSync"
        st_change_run, d_cr = pc1.request(
            "POST",
            "/api/setup/sync-dir",
            body={"sharedSyncDir": str(new_test_shared)},
        )
        assert st_change_run == 400, f"Expected 400 when changing sync folder while active, got {st_change_run}: {d_cr}"

        st_s_stp, _ = pc1.request("POST", "/api/server/stop", token=admin_token)
        assert st_s_stp == 200

        # Change sync folder when idle succeeds and resets sync cache
        st_change_idle, d_ci = pc1.request(
            "POST",
            "/api/setup/sync-dir",
            body={"sharedSyncDir": str(new_test_shared)},
        )
        assert st_change_idle == 200
        assert pc1.ctx.sync_engine._last_synced_snapshot_mtime == 0.0
        assert pc1.ctx.sync_engine._last_synced_snapshot_sig == (0.0, -1, "")

        # Switch back to primary shared_cloud_dir
        st_change_back, _ = pc1.request(
            "POST",
            "/api/setup/sync-dir",
            body={"sharedSyncDir": str(shared_cloud_dir)},
        )
        assert st_change_back == 200
        print("  [OK] Active hosting sync folder protection & sync signature invalidation verified.")

        # ---------------------------------------------------------------------
        # 15. Server Files Explorer Search, Ancestor Navigation & Disable End
        # ---------------------------------------------------------------------
        print("\n[Test 15] Testing File Explorer Search, Ancestor Navigation & Disable End Option...")

        # 15.1 Disable The End Option via Server Properties (persists to server.properties & bukkit.yml)
        st_dis_end, res_dis_end = pc1.request(
            "PUT",
            "/api/admin/properties",
            token=admin_token,
            body={"properties": {"disable-end": True}},
        )
        assert st_dis_end == 200
        assert res_dis_end["properties"]["disable-end"] == "true"
        assert res_dis_end["properties"]["allow-end"] == "false"
        assert res_dis_end["world"]["disableEnd"] is True

        bukkit_content = (pc1.ctx.mc_manager.runtime_dir / "bukkit.yml").read_text(encoding="utf-8")
        assert "allow-end: false" in bukkit_content, "bukkit.yml was not updated with allow-end: false"
        print("  [OK] Disable The End option successfully updated server.properties and bukkit.yml (allow-end: false).")

        # 15.2 Re-enabling The End Option restores allow-end: true in both configs
        st_en_end, res_en_end = pc1.request(
            "PUT",
            "/api/admin/properties",
            token=admin_token,
            body={"properties": {"disable-end": False}},
        )
        assert st_en_end == 200
        assert res_en_end["properties"]["disable-end"] == "false"
        assert res_en_end["properties"]["allow-end"] == "true"
        assert res_en_end["world"]["disableEnd"] is False
        bukkit_content2 = (pc1.ctx.mc_manager.runtime_dir / "bukkit.yml").read_text(encoding="utf-8")
        assert "allow-end: true" in bukkit_content2, "bukkit.yml was not updated with allow-end: true"
        print("  [OK] Re-enabling The End restores allow-end: true across all config files.")

        # 15.3 Disable End during World Map Generation
        mock_end_dir = pc1.ctx.mc_manager.runtime_dir / "world_the_end"
        mock_end_dir.mkdir(parents=True, exist_ok=True)
        (mock_end_dir / "end_data.txt").write_text("old-end-chunk", encoding="utf-8")
        assert mock_end_dir.exists()

        st_rst_end, res_rst_end = pc1.request(
            "POST",
            "/api/admin/world/reset",
            token=admin_token,
            body={"mode": "random", "disableEnd": True},
        )
        assert st_rst_end == 200
        assert res_rst_end["world"]["disableEnd"] is True
        assert res_rst_end["properties"]["disable-end"] == "true"
        assert not mock_end_dir.exists(), "End dimension folder should be cleaned when generating with disableEnd"
        print("  [OK] World reset with disableEnd=True cleanly applied and cleaned End dimension directory.")

        # 15.4 Server File Explorer Recursive Search
        pc1.ctx.mc_manager.write_file_content("plugins/LuckPerms/config.yml", "storage-method: yaml")
        pc1.ctx.mc_manager.write_file_content("plugins/Essentials/motd.txt", "Welcome to server")
        pc1.ctx.mc_manager.write_file_content("config/paper-global.yml", "proxies:\n  velocity: true")

        # Deep search from root directory for 'config'
        st_s_root, res_s_root = pc1.request("GET", "/api/admin/files?search=config", token=admin_token)
        assert st_s_root == 200
        root_matches = {item["path"]: item for item in res_s_root["files"]}
        assert "plugins/LuckPerms/config.yml" in root_matches, "Search should match nested config.yml"
        assert "config/paper-global.yml" in root_matches, "Search should match nested paper-global.yml"
        assert root_matches["plugins/LuckPerms/config.yml"]["parentDir"] == "plugins/LuckPerms"
        assert root_matches["config/paper-global.yml"]["parentDir"] == "config"

        # Targeted search scoped to plugins subfolder
        st_s_sub, res_s_sub = pc1.request("GET", "/api/admin/files?dir=plugins&search=motd", token=admin_token)
        assert st_s_sub == 200
        sub_paths = [item["path"] for item in res_s_sub["files"]]
        assert "plugins/Essentials/motd.txt" in sub_paths
        assert "config/paper-global.yml" not in sub_paths

        # Direct folder listing (non-search) returns immediate children only
        st_direct, res_direct = pc1.request("GET", "/api/admin/files?dir=plugins", token=admin_token)
        assert st_direct == 200
        direct_names = [item["name"] for item in res_direct["files"]]
        assert "LuckPerms" in direct_names and "Essentials" in direct_names
        assert "config.yml" not in direct_names, "Direct listing should not return deep nested files"
        print("  [OK] File Explorer recursive search, scoping, and direct navigation verified.")

        # ---------------------------------------------------------------------
        # 16. Bug Fixes Verification (Fast Sync Cache, Session Veil & Nav Layout)
        # ---------------------------------------------------------------------
        print("\n[Test 16] Verifying Sync-State Persistence, No-Flash Session & Nav Margins...")

        # 16.1 Persistent .sync-state.json avoids redundant pull_world_to_runtime across restarts
        pc1.ctx.sync_engine.push_world_from_runtime(create_backup=False, backup_label="test16")
        assert pc1.ctx.sync_engine.sync_state_path.exists(), ".sync-state.json was not written after push"

        # Simulate server.properties having an older mtime than latest-server.zip (typical Minecraft session)
        props_path = pc1.ctx.sync_engine.runtime_dir / "server.properties"
        snap_st = pc1.ctx.sync_engine.latest_snapshot_path.stat()
        os.utime(props_path, (snap_st.st_mtime - 300.0, snap_st.st_mtime - 300.0))

        # Reboot a fresh AppContext / SyncEngine pointing to the same data_dir
        pc1_rebooted = InMemoryHTTPClient(data_dir=str(pc_one_data))
        assert abs(pc1_rebooted.ctx.sync_engine._last_synced_snapshot_mtime - snap_st.st_mtime) < 1e-3, (
            "Rebooted SyncEngine did not restore last synced snapshot mtime from .sync-state.json"
        )

        pull_called = []
        orig_pull = pc1_rebooted.ctx.sync_engine.pull_world_to_runtime
        def _spy_pull():
            pull_called.append(True)
            return orig_pull()
        pc1_rebooted.ctx.sync_engine.pull_world_to_runtime = _spy_pull  # type: ignore

        st_fast, res_fast = pc1_rebooted.request("GET", "/api/admin/files?dir=plugins", token=admin_token)
        # Note: admin_token is from prior pc1 instance; login again or check list_files directly on rebooted ctx
        fast_files = pc1_rebooted.ctx.mc_manager.list_files("plugins")
        assert len(pull_called) == 0, "list_files unnecessarily triggered slow pull_world_to_runtime on unchanged snapshot!"
        assert any(f["name"] == "LuckPerms" for f in fast_files), "Plugins subdirectory did not load from runtime-server"
        print("  [OK] .sync-state.json prevents slow redundant re-extraction when loading server files after restart.")

        # 16.2 Verify pre-paint session-restoring script & balanced nav-tab CSS rules
        idx_html = (ROOT_DIR / "public" / "index.html").read_text(encoding="utf-8")
        app_js = (ROOT_DIR / "public" / "app.js").read_text(encoding="utf-8")
        styles_css = (ROOT_DIR / "public" / "styles.css").read_text(encoding="utf-8")

        assert "session-restoring" in idx_html and "session-boot-veil" in idx_html
        assert "html.session-restoring #auth-view" in styles_css
        assert "bootApp()" in app_js and "filesRequestSeq" in app_js and "fileCache" in app_js
        assert "flex: 1 0 auto;" in styles_css and "justify-content: space-between;" in styles_css
        print("  [OK] No-flash session boot veil, file request sequencing/cache & symmetrical nav-tab layout verified.")

        # ---------------------------------------------------------------------
        # 17. Atomic Backup Restore & Cracked Support UUID Progress Preservation
        # ---------------------------------------------------------------------
        print("\n[Test 17] Verifying Atomic Backup Restore & Cracked Support UUID Preservation...")

        # 17.1 Set up rich Mojang online-mode player state & create backup
        rt1 = pc1.ctx.mc_manager.runtime_dir
        online_uuid = "ff36425e-4f60-4f1a-9675-82a686d59bd3"
        offline_uuid = pc1.ctx.mc_manager._compute_offline_uuid("ProudHogRider")
        (rt1 / "usercache.json").write_text(
            json.dumps([{"uuid": online_uuid, "name": "ProudHogRider", "expiresOn": "2026-12-09 22:00:00 +0200"}]),
            encoding="utf-8",
        )
        (rt1 / "ops.json").write_text(
            json.dumps([{"uuid": online_uuid, "name": "ProudHogRider", "level": 4, "bypassesPlayerLimit": False}]),
            encoding="utf-8",
        )
        for sub_dir, content in [
            ("world/players/data", b"MC26-PLAYER-NBT-INVENTORY"),
            ("world/players/stats", b'{"stats": {"mined": 42}}'),
            ("world/players/advancements", b'{"minecraft:story/root": {"done": true}}'),
            ("world/playerdata", b"LEGACY-PLAYER-NBT-INVENTORY"),
        ]:
            ext = ".dat" if ("data" in sub_dir) else ".json"
            pdir = rt1 / sub_dir
            pdir.mkdir(parents=True, exist_ok=True)
            (pdir / f"{online_uuid}{ext}").write_bytes(content)

        # Preserve a fake static library & paper-remapped file to ensure restore doesn't delete them
        static_lib = rt1 / "libraries" / "netty-core.jar"
        static_lib.parent.mkdir(parents=True, exist_ok=True)
        static_lib.write_bytes(b"static-lib")
        (rt1 / "plugins" / ".paper-remapped").mkdir(parents=True, exist_ok=True)
        (rt1 / "plugins" / ".paper-remapped" / "index.json").write_text("{}", encoding="utf-8")

        st_bk, res_bk = pc1.request(
            "POST",
            "/api/admin/backups",
            token=admin_token,
            body={"label": "Milioths"},
        )
        assert st_bk == 200 and res_bk["result"]["backupFile"]
        good_bk_name = res_bk["result"]["backupFile"]
        good_bk_path = pc1.ctx.sync_engine.backups_dir / good_bk_name
        # Backdate the backup file to yesterday so we verify restore touches latest-server.zip mtime to NOW
        old_ts = time.time() - 86400.0
        os.utime(good_bk_path, (old_ts, old_ts))

        # 17.2 Enable cracked support (online-mode=false) -> auto-migrates player data without losing progress!
        st_crk, res_crk = pc1.request(
            "PUT",
            "/api/admin/properties",
            token=admin_token,
            body={"properties": {"online-mode": False}},
        )
        assert st_crk == 200
        assert (rt1 / "world" / "players" / "data" / f"{offline_uuid}.dat").read_bytes() == b"MC26-PLAYER-NBT-INVENTORY"
        assert (rt1 / "world" / "players" / "stats" / f"{offline_uuid}.json").exists()
        assert (rt1 / "world" / "players" / "advancements" / f"{offline_uuid}.json").exists()
        assert (rt1 / "world" / "playerdata" / f"{offline_uuid}.dat").read_bytes() == b"LEGACY-PLAYER-NBT-INVENTORY"
        assert (rt1 / "world" / "players" / "data" / f"{online_uuid}.dat").exists(), "Online UUID file must remain intact!"
        migrated_ops = json.loads((rt1 / "ops.json").read_text(encoding="utf-8"))
        assert migrated_ops[0]["uuid"] == offline_uuid
        print("  [OK] Enabling cracked support (online-mode=false) preserved player inventory, stats, advancements & OP permissions.")

        # 17.3 Corrupt world in runtime + push newer broken snapshot, then verify atomic backup restore recovers cleanly
        (rt1 / "world" / "players" / "data" / f"{online_uuid}.dat").unlink()
        (rt1 / "post_disaster_marker.txt").write_text("broken", encoding="utf-8")
        pc1.ctx.sync_engine.push_world_from_runtime(create_backup=False, backup_label="broken")

        # Also test corrupted backup file rejects cleanly WITHOUT wiping runtime_dir
        corrupt_bk = pc1.ctx.sync_engine.backups_dir / "world-corrupted.zip"
        corrupt_bk.write_bytes(b"NOT-A-VALID-ZIP")
        st_bad_rst, res_bad_rst = pc1.request(
            "POST",
            "/api/admin/backups/restore",
            token=admin_token,
            body={"name": "world-corrupted.zip"},
        )
        assert st_bad_rst == 400, f"Expected 400 for corrupt backup restore, got {st_bad_rst}: {res_bad_rst}"
        assert (rt1 / "post_disaster_marker.txt").exists(), "Failed restore must NOT wipe runtime directory!"
        corrupt_bk.unlink(missing_ok=True)

        # Now restore Milioths' healthy backup
        before_restore_ts = time.time() - 2.0
        st_rst, res_rst = pc1.request(
            "POST",
            "/api/admin/backups/restore",
            token=admin_token,
            body={"name": good_bk_name},
        )
        assert st_rst == 200, f"Backup restore failed: {res_rst}"
        assert res_rst["restored"] == good_bk_name
        assert res_rst["properties"]["online-mode"] == "true"
        assert not (rt1 / "post_disaster_marker.txt").exists(), "Stale post-disaster file should be cleaned on restore"
        assert (rt1 / "world" / "players" / "data" / f"{online_uuid}.dat").exists()
        assert static_lib.exists() and (rt1 / "plugins" / ".paper-remapped" / "index.json").exists(), "Static runtime dirs must be preserved"
        # latest-server.zip mtime must be updated to NOW (newer than backup file's yesterday timestamp)
        restored_snap_mtime = pc1.ctx.sync_engine.latest_snapshot_path.stat().st_mtime
        assert restored_snap_mtime >= before_restore_ts, "Restored latest-server.zip mtime was not updated to current time!"

        # Verify PC 2 automatically sees the newly restored snapshot when idle
        pc2.ctx.sync_engine.ensure_runtime_synced_when_idle()
        assert (pc2.ctx.mc_manager.runtime_dir / "world" / "players" / "data" / f"{online_uuid}.dat").exists()
        assert pc2.ctx.mc_manager.get_server_properties().get("online-mode") == "true"
        print("  [OK] Atomic staging restore, fresh mtime promotion, corrupt-backup guard & multi-PC pull verified!")

        # ---------------------------------------------------------------------
        # 18. Cloud Lock Visibility, Clock-Skew Protection & Stale Snapshot Guard
        # ---------------------------------------------------------------------
        print("\n[Test 18] Verifying Google Drive Lock Resilience, Clock Skew & Stale Snapshot Overwrite Guard...")
        # 18.1 Bob starts hosting on PC 2; simulate Drive overwriting host-lock.json back to unlocked idle
        st_b2, res_b2 = pc2.request("POST", "/api/server/start", token=bob_token)
        assert st_b2 == 200
        bob_epoch = res_b2["lock"]["lockEpoch"]
        assert bob_epoch, "Expected non-empty lockEpoch for session claim file"
        assert (pc2.ctx.sync_engine.locks_dir / f"session-{bob_epoch}.json").exists()

        # Simulate stale cached host-lock.json on Drive resetting host-lock.json to idle
        pc1.ctx.sync_engine.host_lock_path.write_text(json.dumps({"isLocked": False, "status": "idle"}), encoding="utf-8")
        st_v1, status_pc1 = pc1.request("GET", "/api/server/status", token=admin_token)
        assert st_v1 == 200
        assert status_pc1["lock"]["isLocked"] is True, "PC 1 lost visibility of Bob's lock when host-lock.json reverted!"
        assert status_pc1["lock"]["hostUsername"] == "Bob"
        assert status_pc1["lock"]["isActivelyLocked"] is True

        # PC 1 must STILL be blocked from starting over Bob!
        st_col, _ = pc1.request("POST", "/api/server/start", token=admin_token)
        assert st_col == 409, f"Expected 409 Conflict while Bob holds session file, got {st_col}"

        # 18.2 Clock-skew protection: simulate Bob's PC clock 10 minutes slow, ticking heartbeat forward
        pc1.ctx.sync_engine._remote_hb_tracker.clear()
        sess_path = pc2.ctx.sync_engine.locks_dir / f"session-{bob_epoch}.json"
        skew_data = json.loads(sess_path.read_text(encoding="utf-8"))
        skew_hb1 = time.time() - 600.0
        skew_data["lastHeartbeat"] = skew_hb1
        sess_path.write_text(json.dumps(skew_data), encoding="utf-8")
        pc1.ctx.sync_engine.host_lock_path.write_text(json.dumps(skew_data), encoding="utf-8")
        first_skew_lock = pc1.ctx.sync_engine.get_lock()
        assert first_skew_lock.is_stale() is True, "Static old timestamp without progression must still report stale"

        # Now Bob bumps his skewed heartbeat +15s -> PC 1 sees forward progression and treats it active!
        skew_data["lastHeartbeat"] = skew_hb1 + 15.0
        sess_path.write_text(json.dumps(skew_data), encoding="utf-8")
        pc1.ctx.sync_engine.host_lock_path.write_text(json.dumps(skew_data), encoding="utf-8")
        second_skew_lock = pc1.ctx.sync_engine.get_lock()
        assert second_skew_lock.is_actively_locked() is True, "Ticking remote heartbeat should bypass PC clock skew!"

        # Restore Bob's current heartbeat, write world progress on PC 2, and stop cleanly
        skew_data["lastHeartbeat"] = time.time()
        sess_path.write_text(json.dumps(skew_data), encoding="utf-8")
        (pc2.ctx.mc_manager.runtime_dir / "world" / "bob_diamond_base.txt").write_text("do-not-lose-me", encoding="utf-8")
        st_b2_stop, res_b2_stop = pc2.request("POST", "/api/server/stop", token=bob_token)
        assert st_b2_stop == 200 and res_b2_stop["lock"]["isLocked"] is False
        assert (pc2.ctx.sync_engine.locks_dir / f"released-{bob_epoch}.json").exists()

        # Simulate stale conflict copy host-lock (1).json with released lockEpoch -> must NOT look locked!
        stale_conflict = pc1.ctx.sync_engine.shared_dir / "host-lock (1).json"
        stale_conflict.write_text(json.dumps(skew_data), encoding="utf-8")
        assert pc1.ctx.sync_engine.get_lock().is_locked is False, "Released epoch tombstone must suppress stale conflict lock!"
        stale_conflict.unlink(missing_ok=True)
        print("  [OK] Session lock resilience, conflict tombstone & clock-skew protection verified!")

        # 18.3 Cloud sync protection: block start if .tmp.drivedownload is active OR snapshot mismatches manifest
        manifest = pc1.ctx.sync_engine._read_sync_manifest()
        assert manifest and manifest["hostUsername"] == "Bob" and manifest["backupFile"]

        dl_dir = pc1.ctx.sync_engine.shared_dir / ".tmp.drivedownload"
        dl_dir.mkdir(parents=True, exist_ok=True)
        active_part = dl_dir / "downloading.part"
        active_part.write_bytes(b"X" * 4096)
        st_dl_blk, res_dl_blk = pc1.request("POST", "/api/server/start", token=admin_token)
        assert st_dl_blk == 400 and "downloading" in res_dl_blk["error"].lower(), f"Expected download block, got {st_dl_blk}: {res_dl_blk}"
        shutil.rmtree(dl_dir, ignore_errors=True)

        # Simulate frozen stale latest-server.zip + temporarily missing backup -> MUST block start AND block idle clobber!
        latest_snap = pc1.ctx.sync_engine.latest_snapshot_path
        good_snap_bytes = latest_snap.read_bytes()
        expected_bk = pc1.ctx.sync_engine.backups_dir / manifest["backupFile"]
        good_bk_bytes = expected_bk.read_bytes()

        latest_snap.write_bytes(b"STALE-OLD-WORLD-BYTES")
        expected_bk.unlink()
        st_stale_blk, res_stale_blk = pc1.request("POST", "/api/server/start", token=admin_token)
        assert st_stale_blk == 400 and "catching up" in res_stale_blk["error"].lower(), f"Expected sync mismatch block, got {st_stale_blk}: {res_stale_blk}"
        assert pc1.ctx.sync_engine.get_lock().is_locked is False, "Blocked start must cleanly release lock!"

        # Restore paired backup only (latest-server.zip still frozen stale) -> verify recovery directly from verified backup!
        expected_bk.write_bytes(good_bk_bytes)
        st_rec_ok, res_rec_ok = pc1.request("POST", "/api/server/start", token=admin_token)
        assert st_rec_ok == 200, f"Expected backup manifest fallback recovery to succeed, got {res_rec_ok}"
        assert (pc1.ctx.mc_manager.runtime_dir / "world" / "bob_diamond_base.txt").exists(), "Bob's diamond base lost!"
        st_rec_stop, _ = pc1.request("POST", "/api/server/stop", token=admin_token)
        assert st_rec_stop == 200
        print("  [OK] Active Drive transfer guard & manifest-verified backup fallback recovery verified!")

        # =====================================================================
        # Test 19: Remote Host Telemetry/Console & Host-Transfer Sync Ordering
        # =====================================================================
        print("\n[Test 19] Verifying Remote Host Telemetry/Console & Host-Transfer Sync Guard...")
        # Bob (PC 2) starts hosting with simulated telemetry + logs
        st_19_start, res_19_start = pc2.request("POST", "/api/server/start", token=bob_token)
        assert st_19_start == 200, f"Expected Bob start to succeed, got {res_19_start}"

        # Populate live metrics and console logs on PC 2 and force-publish host telemetry
        with pc2.ctx.mc_manager._mutex:
            pc2.ctx.mc_manager.memory_used_mb = 1842
            pc2.ctx.mc_manager.cpu_percent = 37.5
            pc2.ctx.mc_manager.tps = 19.6
            pc2.ctx.mc_manager.server_ready_at = time.time() - 95.0
        pc2.ctx.mc_manager._log("Remote console check: Boss raid started!", "INFO", "Minecraft")
        pc2.ctx.mc_manager.sync_engine.update_lock_state(
            status="running",
            memory_used_mb=1842,
            memory_max_mb=4096,
            cpu_percent=37.5,
            tps=19.6,
            server_ready_at=pc2.ctx.mc_manager.server_ready_at,
        )
        pc2.ctx.mc_manager._publish_host_telemetry_throttled(force=True)

        # PC 1 (Admin, idle) queries /api/server/status -> must see Bob's live RAM, CPU, TPS, Uptime & Console Logs!
        st_19_stat, res_19_stat = pc1.request("GET", "/api/server/status", token=admin_token)
        assert st_19_stat == 200
        tel_19 = res_19_stat.get("telemetry", {})
        assert tel_19.get("memoryUsedMb") == 1842, f"Expected remote RAM 1842 MB, got {tel_19}"
        assert tel_19.get("cpuPercent") == 37.5, f"Expected remote CPU 37.5%, got {tel_19}"
        assert abs(float(tel_19.get("tps", 0.0)) - 19.6) < 0.2, f"Expected remote TPS ~19.6, got {tel_19}"
        assert tel_19.get("uptimeSeconds", 0) >= 80, f"Expected remote uptime >=80s, got {tel_19}"
        remote_log_texts = [e.get("text", "") for e in res_19_stat.get("logs", [])]
        assert any("Remote console check: Boss raid started!" in txt for txt in remote_log_texts), (
            f"Remote host console logs missing on PC 1: {remote_log_texts}"
        )

        # Simulate Google Drive splitting `locks/` into `locks (1)/` while `locks/host-telemetry.json` is empty!
        bob_epoch_19 = res_19_stat["lock"]["lockEpoch"]
        live_locks_conflict = pc1.ctx.sync_engine.shared_dir / "locks (1)"
        live_locks_conflict.mkdir(exist_ok=True)
        shutil.copy2(
            pc1.ctx.sync_engine.locks_dir / f"session-{bob_epoch_19}.json",
            live_locks_conflict / f"session-{bob_epoch_19}.json",
        )
        shutil.copy2(
            pc1.ctx.sync_engine.host_telemetry_path,
            live_locks_conflict / "host-telemetry.json",
        )
        # Inject a newer conflict-folder log entry while leaving main locks/host-telemetry.json cleared & stale
        c_tel = json.loads((live_locks_conflict / "host-telemetry.json").read_text(encoding="utf-8"))
        c_tel["logs"].append({
            "id": f"{int(time.time() * 1000)}-conflict19",
            "timestamp": "21:45:00",
            "level": "INFO",
            "source": "Minecraft",
            "text": "[21:45:00] [Minecraft/INFO]: Conflict folder telemetry mirrored!",
        })
        c_tel["updatedAt"] = time.time() + 1.0
        (live_locks_conflict / "host-telemetry.json").write_text(json.dumps(c_tel), encoding="utf-8")
        (pc1.ctx.sync_engine.locks_dir / f"session-{bob_epoch_19}.json").unlink(missing_ok=True)
        pc1.ctx.sync_engine.clear_host_telemetry()
        # Running conflict cleanup must preserve live active remote locks (1) and mirror its console!
        pc1.ctx.sync_engine._cleanup_cloud_conflict_files()
        assert live_locks_conflict.exists(), "Live remote locks (1) folder must not be deleted mid-session!"
        st_19_c, res_19_c = pc1.request("GET", "/api/server/status", token=admin_token)
        assert st_19_c == 200
        c_log_texts = [e.get("text", "") for e in res_19_c.get("logs", [])]
        assert any("Conflict folder telemetry mirrored!" in txt for txt in c_log_texts), (
            f"Expected conflict-folder remote console mirroring on PC 1, got: {c_log_texts}"
        )
        shutil.rmtree(live_locks_conflict, ignore_errors=True)

        # Frontend assertion for remote console mirroring guard
        js_app = (Path(__file__).resolve().parent.parent / "public" / "app.js").read_text(encoding="utf-8")
        assert "Live console mirrored from" in js_app, "Frontend missing remote host console mirror notice!"

        # PC 2 writes critical world progress and stops cleanly
        marker_file_pc2 = pc2.ctx.mc_manager.runtime_dir / "world" / "host_transfer_marker.txt"
        marker_file_pc2.write_text("pc2-session-progress-v19", encoding="utf-8")
        pre_stop_manifest = pc2.ctx.sync_engine._read_sync_manifest() or {}
        st_19_stop, _ = pc2.request("POST", "/api/server/stop", token=bob_token)
        assert st_19_stop == 200

        post_stop_manifest = pc2.ctx.sync_engine._read_sync_manifest() or {}
        expected_seq, expected_hash, released_by = pc1.ctx.sync_engine.get_expected_snapshot_state()
        assert expected_seq == int(post_stop_manifest.get("snapshotSeq", 0))
        assert expected_hash == str(post_stop_manifest.get("sha256", ""))
        assert released_by == "Bob", f"Expected release attribution 'Bob', got '{released_by}'"

        # Simulate Drive lag: released-*.json already reflects expected_seq, while snapshots/ still lags behind!
        saved_manifest_bytes = pc1.ctx.sync_engine.sync_manifest_path.read_bytes()
        saved_snap_bytes = pc1.ctx.sync_engine.latest_snapshot_path.read_bytes()
        newest_bk_path = pc1.ctx.sync_engine.backups_dir / post_stop_manifest["backupFile"]
        saved_bk_bytes = newest_bk_path.read_bytes()

        lagging_manifest = dict(pre_stop_manifest)
        lagging_manifest["snapshotSeq"] = max(1, expected_seq - 1)
        pc1.ctx.sync_engine.sync_manifest_path.write_text(json.dumps(lagging_manifest), encoding="utf-8")
        pc1.ctx.sync_engine.latest_snapshot_path.write_bytes(good_snap_bytes)
        newest_bk_path.unlink(missing_ok=True)

        st_19_lag, res_19_lag = pc1.request("POST", "/api/server/start", token=admin_token)
        assert st_19_lag == 400 and "catching up" in res_19_lag["error"].lower(), (
            f"Expected host-transfer lag block while waiting for snapshotSeq {expected_seq}, got {st_19_lag}: {res_19_lag}"
        )
        assert pc1.ctx.sync_engine.get_lock().is_locked is False

        # Restore synced files AND simulate Drive conflict directory/config duplicates
        pc1.ctx.sync_engine.sync_manifest_path.write_bytes(saved_manifest_bytes)
        pc1.ctx.sync_engine.latest_snapshot_path.write_bytes(saved_snap_bytes)
        newest_bk_path.write_bytes(saved_bk_bytes)

        conflict_locks_dir = pc1.ctx.sync_engine.shared_dir / "locks (1)"
        conflict_locks_dir.mkdir(exist_ok=True)
        (conflict_locks_dir / "released-123456-fake.json").write_text(
            json.dumps({"lockEpoch": "123456-fake", "releasedAt": time.time()}),
            encoding="utf-8",
        )
        conflict_cfg = pc1.ctx.sync_engine.shared_dir / "cluster-config (1).json"
        base_cfg_data = json.loads(pc1.ctx.sync_engine.cluster_config_path.read_text(encoding="utf-8"))
        base_cfg_data["serverName"] = "Synced Conflict Config"
        conflict_cfg.write_text(json.dumps(base_cfg_data), encoding="utf-8")

        st_19_handoff, res_19_handoff = pc1.request("POST", "/api/server/start", token=admin_token)
        assert st_19_handoff == 200, f"Expected clean handoff after Drive catch-up, got {res_19_handoff}"
        pc1_marker = pc1.ctx.mc_manager.runtime_dir / "world" / "host_transfer_marker.txt"
        assert pc1_marker.exists() and pc1_marker.read_text(encoding="utf-8") == "pc2-session-progress-v19"
        assert not conflict_locks_dir.exists(), "Duplicate 'locks (1)' conflict folder should be cleaned up!"
        assert not conflict_cfg.exists(), "Duplicate 'cluster-config (1).json' should be reconciled & cleaned up!"
        st_19_done, _ = pc1.request("POST", "/api/server/stop", token=admin_token)
        assert st_19_done == 200
        print("  [OK] Remote host telemetry, live console mirroring, expectedSnapshotSeq transfer guard & conflict folder cleanup verified!")

        print("\n============================================================")
        print("  ALL END-TO-END VERIFICATION TESTS PASSED SUCCESSFULLY! ")
        print("============================================================")

    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


if __name__ == "__main__":
    run_verification()
