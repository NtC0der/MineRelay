"""
Empirical Concurrency Stress Test Harness for MineRelay Player State.
Tests concurrent requests against GET /api/players and GET /api/server/status
while high-frequency player join/leave/kick/chat events mutate server state.
"""

import io
import os
import sys
import json
import time
import shutil
import tempfile
import threading
from pathlib import Path
from typing import Any, Dict, List, Tuple, Set

os.environ["MINERELAY_TEST_MODE"] = "1"

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.server import AppContext, create_request_handler
from src.minecraft_manager import MinecraftManager
from tests.test_stress_player_monitor import MockStdoutProc


class DirectHTTPClient:
    """Dispatches HTTP requests in-memory via DashboardRequestHandler in a thread-safe manner."""

    def __init__(self, ctx: AppContext):
        self.ctx = ctx
        self.public_dir = ROOT_DIR / "public"
        self.handler_cls = create_request_handler(self.ctx, self.public_dir)

    def request(self, method: str, path: str, token: str = None, body: bytes = b"") -> Tuple[int, Dict[str, Any]]:
        headers = {
            "Host": "localhost",
            "Connection": "close",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        if body:
            headers["Content-Type"] = "application/json"
            headers["Content-Length"] = str(len(body))

        req_lines = [f"{method} {path} HTTP/1.1"]
        for k, v in headers.items():
            req_lines.append(f"{k}: {v}")
        raw_req = ("\r\n".join(req_lines) + "\r\n\r\n").encode("utf-8") + body

        rfile = io.BytesIO(raw_req)
        wfile = io.BytesIO()

        class DummySocket:
            def makefile(self, mode, *args, **kwargs):
                return rfile if "r" in mode else wfile

            def sendall(self, data):
                wfile.write(data)

            def close(self):
                pass

        self.handler_cls(DummySocket(), ("127.0.0.1", 54321), None)
        raw_res = wfile.getvalue()
        header_part, _, payload_part = raw_res.partition(b"\r\n\r\n")
        status_line = header_part.splitlines()[0].decode("utf-8")
        status_code = int(status_line.split(" ")[1])
        try:
            parsed = json.loads(payload_part.decode("utf-8"))
        except Exception:
            parsed = {"raw": payload_part.decode("utf-8", errors="replace")}
        return status_code, parsed


def test_concurrent_player_state_reflection():
    print("============================================================")
    print("  Challenger 2: Concurrent /api/players & /api/server/status")
    print("============================================================")

    temp_root = Path(tempfile.mkdtemp(prefix="stress_concurrent_players_"))
    shared_cloud_dir = temp_root / "SharedDrive"
    data_dir = temp_root / "server_data"

    try:
        ctx = AppContext(data_dir=str(data_dir))
        client = DirectHTTPClient(ctx)

        # 1. Setup cluster and accounts
        init_body = json.dumps({
            "serverName": "Concurrent Stress SMP",
            "adminUsername": "StressAdmin",
            "customPasskey": "MC-ADMIN-PASS-9999",
            "sharedSyncDir": str(shared_cloud_dir),
        }).encode("utf-8")

        st, res = client.request("POST", "/api/setup/init", body=init_body)
        assert st == 200, f"Setup init failed: {res}"
        admin_token = res["token"]

        # Create Member Bob
        bob_body = json.dumps({"username": "Bob", "customPasskey": "MC-HOST-BOB-9999-PASS"}).encode("utf-8")
        st, res = client.request("POST", "/api/admin/accounts", token=admin_token, body=bob_body)
        assert st == 201

        st, res = client.request("POST", "/api/auth/login", body=json.dumps({"passkey": "MC-HOST-BOB-9999-PASS"}).encode("utf-8"))
        assert st == 200
        member_token = res["token"]

        # 2. Start local server
        st, res = client.request("POST", "/api/server/start", token=admin_token, body=b"{}")
        assert st == 200 and res["localStatus"] == "running"
        mgr = ctx.mc_manager

        # Shared test controls
        stop_event = threading.Event()
        errors: List[str] = []
        stats = {
            "api_players_reads": 0,
            "api_status_reads": 0,
            "mutations": 0,
            "invariant_violations": 0,
        }
        stats_mutex = threading.Lock()

        # Invariant checker
        def check_invariants(source: str, data: Dict[str, Any]):
            if source == "/api/players":
                online = data.get("online")
                count = data.get("count")
                max_p = data.get("max")
                if not isinstance(online, list):
                    return f"online is not a list: {type(online)}"
                if not isinstance(count, int):
                    return f"count is not an int: {type(count)}"
                if count != len(online):
                    return f"count ({count}) != len(online) ({len(online)})"
                # Check for duplicate players
                lowered = [p.lower() for p in online if isinstance(p, str)]
                if len(lowered) != len(set(lowered)):
                    return f"Duplicate player detected in online list: {online}"

            elif source == "/api/server/status":
                online_p = data.get("onlinePlayers")
                if not isinstance(online_p, list):
                    return f"onlinePlayers is not a list: {type(online_p)}"
                lowered = [p.lower() for p in online_p if isinstance(p, str)]
                if len(lowered) != len(set(lowered)):
                    return f"Duplicate player detected in onlinePlayers: {online_p}"

            return None

        # Reader Thread: GET /api/players
        def reader_players(t_id: int):
            t_client = DirectHTTPClient(ctx)
            token = member_token if t_id % 2 == 0 else admin_token
            local_reads = 0
            while not stop_event.is_set():
                try:
                    st, d = t_client.request("GET", "/api/players", token=token)
                    if st != 200:
                        with stats_mutex:
                            errors.append(f"GET /api/players returned HTTP {st}: {d}")
                        break
                    err = check_invariants("/api/players", d)
                    if err:
                        with stats_mutex:
                            errors.append(f"Invariant violation in /api/players: {err}")
                            stats["invariant_violations"] += 1
                        break
                    local_reads += 1
                except Exception as e:
                    with stats_mutex:
                        errors.append(f"Exception in reader_players {t_id}: {e}")
                    break

            with stats_mutex:
                stats["api_players_reads"] += local_reads

        # Reader Thread: GET /api/server/status
        def reader_status(t_id: int):
            t_client = DirectHTTPClient(ctx)
            token = member_token if t_id % 2 == 0 else admin_token
            local_reads = 0
            while not stop_event.is_set():
                try:
                    st, d = t_client.request("GET", "/api/server/status", token=token)
                    if st != 200:
                        with stats_mutex:
                            errors.append(f"GET /api/server/status returned HTTP {st}: {d}")
                        break
                    err = check_invariants("/api/server/status", d)
                    if err:
                        with stats_mutex:
                            errors.append(f"Invariant violation in /api/server/status: {err}")
                            stats["invariant_violations"] += 1
                        break
                    local_reads += 1
                except Exception as e:
                    with stats_mutex:
                        errors.append(f"Exception in reader_status {t_id}: {e}")
                    break

            with stats_mutex:
                stats["api_status_reads"] += local_reads

        # Writer Thread 1: Simjoin & Simleave commands
        def mutator_sim_commands():
            local_muts = 0
            roster = [f"SimPlayer_{i}" for i in range(25)]
            while not stop_event.is_set():
                for p in roster:
                    if stop_event.is_set():
                        break
                    mgr.send_command(f"simjoin {p}")
                    local_muts += 1
                    time.sleep(0.002)

                for p in reversed(roster):
                    if stop_event.is_set():
                        break
                    mgr.send_command(f"simleave {p}")
                    local_muts += 1
                    time.sleep(0.002)

            with stats_mutex:
                stats["mutations"] += local_muts

        # Writer Thread 2: Stdout Log Injection (joins, leaves, kicks, bedrock, adversarial chat)
        def mutator_stdout_stream():
            local_muts = 0
            adversarial_and_real_logs = [
                "[12:00:01 INFO]: Alice joined the game\n",
                "[12:00:02 INFO]: .BedrockPlayer joined the game\n",
                "[12:00:03 INFO]: <Attacker> [Server]: Alice left the game\n",  # chat spoof, must be ignored
                "[12:00:04 INFO]: <Griefer> [Notice]: Eve joined the game\n",   # chat spoof, must be ignored
                "[12:00:05 INFO]: Bob[127.0.0.1:54321] logged in with entity id 123 at (0, 64, 0)\n",
                "[12:00:06 INFO]: There are 3 of a max of 20 players online: Alice, .BedrockPlayer, Bob\n",
                "[12:00:07 INFO]: alice joined the game\n",                    # case insensitive duplicate, ignored
                "[12:00:08 INFO]: Kicking Bob: Rule violation\n",
                "[12:00:09 INFO]: ALICE left the game\n",                      # case insensitive leave, removes Alice
                "[12:00:10 INFO]: .BedrockPlayer lost connection: Timed out\n",
            ]
            while not stop_event.is_set():
                proc = MockStdoutProc(adversarial_and_real_logs)
                mgr._monitor_java_process(proc, max_players=20)
                local_muts += len(adversarial_and_real_logs)
                time.sleep(0.01)

            with stats_mutex:
                stats["mutations"] += local_muts

        # Writer Thread 3: REST Player Actions (kick, op, whitelist)
        def mutator_rest_actions():
            t_client = DirectHTTPClient(ctx)
            local_muts = 0
            while not stop_event.is_set():
                # Perform kick or whitelist action
                action_body = json.dumps({"action": "kick", "username": "GhostPlayer"}).encode("utf-8")
                st, _ = t_client.request("POST", "/api/admin/players", token=admin_token, body=action_body)
                local_muts += 1
                time.sleep(0.01)

            with stats_mutex:
                stats["mutations"] += local_muts

        print("\n[Phase 1] Launching 20 concurrent readers and 3 mutator threads...")
        threads = []
        # 10 reader threads for /api/players
        for i in range(10):
            t = threading.Thread(target=reader_players, args=(i,))
            threads.append(t)
        # 10 reader threads for /api/server/status
        for i in range(10):
            t = threading.Thread(target=reader_status, args=(i,))
            threads.append(t)
        # 3 mutator threads
        threads.append(threading.Thread(target=mutator_sim_commands))
        threads.append(threading.Thread(target=mutator_stdout_stream))
        threads.append(threading.Thread(target=mutator_rest_actions))

        for t in threads:
            t.daemon = True
            t.start()

        # Run concurrent stress for 3.5 seconds
        time.sleep(3.5)
        stop_event.set()

        for t in threads:
            t.join(timeout=2.0)

        assert not errors, f"Errors occurred during concurrent read/write stress:\n" + "\n".join(errors[:10])
        assert stats["api_players_reads"] >= 200, f"Too few /api/players reads: {stats['api_players_reads']}"
        assert stats["api_status_reads"] >= 30, f"Too few /api/server/status reads: {stats['api_status_reads']}"
        assert stats["mutations"] >= 10, f"Too few mutations: {stats['mutations']}"
        assert stats["invariant_violations"] == 0, f"Invariant violations: {stats['invariant_violations']}"
        print("  [PASS] Zero invariant violations, zero deadlocks, and zero HTTP errors under high-concurrency read/write.")

        # ---------------------------------------------------------------------
        # Phase 2: Quiescent Ground Truth Exact Match Verification
        # ---------------------------------------------------------------------
        print("\n[Phase 2] Verifying Quiescent Ground Truth Exact Match...")
        # Reset players to a known controlled set
        with mgr._mutex:
            mgr.online_players = ["Alpha", "Bravo", ".CharlieBedrock", "Delta_99"]

        # Simultaneously query both endpoints
        st_p, res_p = client.request("GET", "/api/players", token=member_token)
        st_s, res_s = client.request("GET", "/api/server/status", token=member_token)

        assert st_p == 200 and st_s == 200
        assert res_p["count"] == 4
        assert res_p["online"] == ["Alpha", "Bravo", ".CharlieBedrock", "Delta_99"]
        assert res_s["onlinePlayers"] == ["Alpha", "Bravo", ".CharlieBedrock", "Delta_99"]
        assert res_p["online"] == res_s["onlinePlayers"], "Discrepancy between /api/players and /api/server/status!"
        print("  [PASS] /api/players and /api/server/status reflect exact player state identically.")

        # ---------------------------------------------------------------------
        # Phase 3: Remote Lock / Multi-Device Player State Concurrency
        # ---------------------------------------------------------------------
        print("\n[Phase 3] Testing Remote Machine Active Lock Concurrency...")
        # Stop local server so machine is NOT hosting
        st, _ = client.request("POST", "/api/server/stop", token=admin_token, body=b"{}")
        assert st == 200 and mgr.local_status == "idle"

        # Simulate a remote machine claiming the host lock with active players
        lock = ctx.sync_engine.get_lock()
        lock.is_locked = True
        lock.host_username = "RemotePeer"
        lock.machine_name = "PEER-MACHINE-02"
        lock.started_at = time.time()
        lock.last_heartbeat = time.time()
        lock.status = "running"
        lock.max_players = 40
        lock.online_players = ["Remote_Player1", "Remote_Player2", "Remote_Player3"]
        ctx.sync_engine.save_lock(lock)

        # Fire 20 concurrent requests across both endpoints during remote hosting
        remote_errors = []
        def check_remote(token):
            st1, p1 = client.request("GET", "/api/players", token=token)
            st2, s2 = client.request("GET", "/api/server/status", token=token)
            if st1 != 200 or st2 != 200:
                remote_errors.append(f"Bad status code: {st1}, {st2}")
                return
            if p1["count"] != 3 or p1["max"] != 40 or set(p1["online"]) != {"Remote_Player1", "Remote_Player2", "Remote_Player3"}:
                remote_errors.append(f"Invalid /api/players remote data: {p1}")
            if set(s2["onlinePlayers"]) != {"Remote_Player1", "Remote_Player2", "Remote_Player3"}:
                remote_errors.append(f"Invalid /api/server/status remote data: {s2}")

        remote_threads = [threading.Thread(target=check_remote, args=(admin_token if i % 2 == 0 else member_token,)) for i in range(20)]
        for rt in remote_threads:
            rt.start()
        for rt in remote_threads:
            rt.join()

        assert not remote_errors, f"Remote lock concurrency errors: {remote_errors}"
        print("  [PASS] Remote hosting lock correctly reflects player list (count=3, max=40) across all concurrent requests.")

        # ---------------------------------------------------------------------
        # Phase 4: Idle / Lock Released State
        # ---------------------------------------------------------------------
        print("\n[Phase 4] Verifying Idle & Released Lock State...")
        lock.is_locked = False
        lock.locked_by = None
        lock.online_players = []
        ctx.sync_engine.save_lock(lock)

        st_p, res_p = client.request("GET", "/api/players", token=member_token)
        st_s, res_s = client.request("GET", "/api/server/status", token=member_token)
        assert st_p == 200 and res_p["count"] == 0 and res_p["online"] == []
        assert st_s == 200 and res_s["onlinePlayers"] == []
        print("  [PASS] When idle and unlocked, both endpoints return empty player lists.")

        print("\n============================================================")
        print("  ALL CONCURRENT PLAYER STATE STRESS TESTS PASSED!          ")
        print("============================================================")

    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


if __name__ == "__main__":
    test_concurrent_player_state_reflection()
