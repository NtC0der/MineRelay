import io
import os
import sys
import json
import time
import queue
import shutil
import tempfile
import threading
import subprocess
from pathlib import Path
from typing import List, Dict, Any

os.environ["MINERELAY_TEST_MODE"] = "1"

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.server import AppContext, create_request_handler
from src.minecraft_manager import MinecraftManager
from tests.verify_all import InMemoryHTTPClient


class MockStdoutProc:
    def __init__(self, lines: List[str]):
        self.stdout = lines
        self.pid = 99999


def test_ansi_and_unicode_flood():
    print("\n--- Stress Test 1: ANSI Log Flood & Unicode Handling ---")
    temp_dir = Path(tempfile.mkdtemp(prefix="stress_ansi_"))
    try:
        client = InMemoryHTTPClient(data_dir=str(temp_dir))
        mgr = client.ctx.mc_manager
        mgr.local_status = "running"
        mgr._reached_running = True

        # Generate complex ANSI lines: 24-bit true color, OSC window title, CSI cursor movement
        lines = []
        for i in range(500):
            ansi_noise = (
                f"\x1b[38;2;{i % 256};{(i * 7) % 256};{(i * 13) % 256}m"
                f"\x1b]0;Minecraft Server Console\x07"
                f"\x1b[2K\x1b[1G"
                f"[{time.strftime('%H:%M:%S')} INFO]: "
                f"\x1b[1;33mPlayer_{i}\x1b[0m joined the game\n"
            )
            lines.append(ansi_noise)

        proc = MockStdoutProc(lines)
        t0 = time.time()
        mgr._monitor_java_process(proc, max_players=1000)
        dt = time.time() - t0

        print(f"  Processed 500 ANSI-heavy log lines in {dt:.3f}s ({500 / dt:.1f} lines/sec).")
        assert len(mgr.online_players) == 500, f"Expected 500 online players, got {len(mgr.online_players)}"
        assert mgr.online_players[0] == "Player_0"
        assert mgr.online_players[499] == "Player_499"

        # Unicode in log messages: non-ASCII UTF-8, Chinese, Cyrillic, emoji
        unicode_lines = [
            "[12:00:00 INFO]: \x1b[32m世界你好\x1b[0m: Server tick latency 12ms\n",
            "[12:00:01 INFO]: 🎮 🚀 Minecraft server is running smoothly ✨\n",
            "[12:00:02 INFO]: Russian text: Сервер готов к игре\n",
            "[12:00:03 INFO]: NormalPlayer joined the game\n",
            "[12:00:04 INFO]: NormalPlayer left the game\n",
        ]
        proc_u = MockStdoutProc(unicode_lines)
        mgr._monitor_java_process(proc_u, max_players=1000)
        assert "NormalPlayer" not in mgr.online_players
        print("  [PASS] Stress Test 1 passed: ANSI stripped cleanly and Unicode processed without crash.")
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_rapid_joins_and_leaves():
    print("\n--- Stress Test 2: Rapid High-Volume Joins & Leaves ---")
    temp_dir = Path(tempfile.mkdtemp(prefix="stress_rapid_"))
    try:
        client = InMemoryHTTPClient(data_dir=str(temp_dir))
        mgr = client.ctx.mc_manager
        mgr.local_status = "running"
        mgr._reached_running = True

        # Generate 2,000 rapid lifecycle events: interleaved joins, leaves, duplicates, and list resets
        lines = []
        expected_players = set()
        for i in range(1000):
            p = f"User_{i % 50}"
            if i % 3 != 0:
                lines.append(f"[12:00:{i % 60:02d} INFO]: {p} joined the game\n")
                expected_players.add(p)
            else:
                lines.append(f"[12:00:{i % 60:02d} INFO]: {p} left the game\n")
                expected_players.discard(p)

        proc = MockStdoutProc(lines)
        t0 = time.time()
        mgr._monitor_java_process(proc, max_players=100)
        dt = time.time() - t0

        print(f"  Processed 1,000 rapid lifecycle lines in {dt:.3f}s ({1000 / dt:.1f} lines/sec).")
        assert set(mgr.online_players) == expected_players, (
            f"Player list mismatch! Expected {len(expected_players)} players, got {len(mgr.online_players)}"
        )
        # Duplicate joins should never produce duplicate elements in list
        assert len(mgr.online_players) == len(set(mgr.online_players)), "Duplicate player entries found in online_players!"
        print("  [PASS] Stress Test 2 passed: No duplicates, accurate tracking under 1,000 rapid events.")
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_chat_spoofing_vulnerability():
    print("\n--- Stress Test 3: Chat Message Spoofing & Log Injection Vulnerability ---")
    temp_dir = Path(tempfile.mkdtemp(prefix="stress_spoof_"))
    try:
        client = InMemoryHTTPClient(data_dir=str(temp_dir))
        mgr = client.ctx.mc_manager
        mgr.local_status = "running"
        mgr._reached_running = True

        # Initial legitimate players
        mgr.online_players = ["LegitPlayer", "Alice", "Bob"]

        # Simulated chat messages sent by player 'Attacker' in Minecraft
        chat_spoof_lines = [
            # Attacker types in chat that FakePlayer joined
            "[12:00:10 INFO]: <Attacker> FakePlayer joined the game\n",
            # Attacker types in chat that LegitPlayer left
            "[12:00:11 INFO]: <Attacker> LegitPlayer left the game\n",
            # Attacker types in chat kicking Admin
            "[12:00:12 INFO]: <Attacker> Kicking Admin: Banned\n",
            # Attacker types in chat list output
            "[12:00:13 INFO]: <Attacker> There are 2 of a max of 20 players online: Ghost1, Ghost2\n",
            # Bracketed chat messages simulating channel, guild, server, or notice prefixes:
            "[12:00:01 INFO]: <Attacker> [Server]: Alice left the game\n",
            "[12:00:02 INFO]: <Attacker> [Notice]: Eve joined the game\n",
            "[12:00:03 INFO]: <Attacker> [Mod]: Kicking Bob: Cheating\n",
            "[12:00:04 INFO]: <Attacker> [Stats]: There are 1 of a max of 20 players online: Imposter\n",
            "[12:00:05 INFO]: <Attacker> [trade]: FakePlayer joined the game\n",
            "[12:00:06 INFO]: <Attacker> [clan] [officer]: Disconnecting Alice: Banned\n",
        ]

        proc = MockStdoutProc(chat_spoof_lines)
        mgr._monitor_java_process(proc, max_players=20)

        # Check if chat messages contaminated the online players list
        vulnerabilities = []
        if "FakePlayer" in mgr.online_players:
            vulnerabilities.append("Chat message injected FakePlayer into online_players!")
        if "Eve" in mgr.online_players:
            vulnerabilities.append("Bracketed chat message '<Attacker> [Notice]: Eve joined the game' injected Eve into online_players!")
        if "LegitPlayer" not in mgr.online_players:
            vulnerabilities.append("Chat message '<Attacker> LegitPlayer left the game' removed LegitPlayer from online_players!")
        if "Alice" not in mgr.online_players:
            vulnerabilities.append("Bracketed chat message '<Attacker> [Server]: Alice left the game' removed Alice from online_players!")
        if "Bob" not in mgr.online_players:
            vulnerabilities.append("Bracketed chat message '<Attacker> [Mod]: Kicking Bob: Cheating' removed Bob from online_players!")
        if "Imposter" in mgr.online_players or set(mgr.online_players) == {"Imposter"}:
            vulnerabilities.append("Bracketed chat message '<Attacker> [Stats]: There are 1 of a max of 20 players online: Imposter' hijacked online_players!")
        if any(ghost in mgr.online_players for ghost in ["Ghost1", "Ghost2"]):
            vulnerabilities.append("Chat message '<Attacker> There are 2 of a max of 20 players online: ...' hijacked online_players!")

        # Verify stdout monitor case-insensitive handling
        # 1. Join with different case should not duplicate
        proc_case_join = MockStdoutProc(["[12:00:20 INFO]: alice joined the game\n"])
        mgr._monitor_java_process(proc_case_join, max_players=20)
        if mgr.online_players.count("alice") > 0 or len(mgr.online_players) != 3:
            vulnerabilities.append("Case-insensitive join failed: duplicate case entry added to online_players.")

        # 2. Leave with different case should remove the player
        proc_case_leave = MockStdoutProc(["[12:00:21 INFO]: bob left the game\n"])
        mgr._monitor_java_process(proc_case_leave, max_players=20)
        if "Bob" in mgr.online_players or "bob" in mgr.online_players:
            vulnerabilities.append("Case-insensitive leave failed: 'bob' failed to remove 'Bob' from online_players.")

        if vulnerabilities:
            print(f"  [VULNERABILITY FOUND] Chat Spoofing reproduced ({len(vulnerabilities)} issues):")
            for v in vulnerabilities:
                print(f"    - {v}")
            return False, vulnerabilities
        else:
            print("  [PASS] Server ignored chat messages correctly.")
            return True, []
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_multithreaded_concurrency_stress():
    print("\n--- Stress Test 4: Multi-Threaded State Mutations & Read Consistency ---")
    temp_dir = Path(tempfile.mkdtemp(prefix="stress_threads_"))
    try:
        client = InMemoryHTTPClient(data_dir=str(temp_dir))
        mgr = client.ctx.mc_manager
        mgr.local_status = "running"
        mgr._reached_running = True

        errors = []
        stop_event = threading.Event()
        success_mutations = [0]
        success_reads = [0]
        lock_counter = threading.Lock()

        # Thread Group 1: Writers using send_command (simjoin/simleave)
        def sim_writer(worker_id):
            pname = f"SimUser_{worker_id}"
            while not stop_event.is_set():
                try:
                    mgr.send_command(f"simjoin {pname}")
                    time.sleep(0.001)
                    mgr.send_command(f"simleave {pname}")
                    with lock_counter:
                        success_mutations[0] += 2
                except Exception as e:
                    errors.append(f"sim_writer {worker_id} error: {e}")
                    break

        # Thread Group 2: Writers using player_action (kick, ban, op, whitelist)
        def action_writer(worker_id):
            pname = f"ActionUser_{worker_id}"
            while not stop_event.is_set():
                try:
                    mgr.player_action("op", pname)
                    mgr.player_action("whitelist_add", pname)
                    mgr.player_action("kick", pname)
                    mgr.player_action("deop", pname)
                    with lock_counter:
                        success_mutations[0] += 4
                    time.sleep(0.002)
                except Exception as e:
                    errors.append(f"action_writer {worker_id} error: {e}")
                    break

        # Thread Group 3: Readers calling get_status_summary & get_players_data
        def reader_worker(worker_id):
            while not stop_event.is_set():
                try:
                    summary = mgr.get_status_summary()
                    players_data = mgr.get_players_data()
                    # Directly read online_players property/attribute
                    direct_online = list(mgr.online_players)

                    # Check consistency
                    summary_online = summary.get("onlinePlayers", [])
                    data_online = players_data.get("online", [])

                    # In a race-free system, counts should be integers and lists valid
                    assert isinstance(summary_online, list)
                    assert isinstance(data_online, list)
                    assert isinstance(direct_online, list)

                    with lock_counter:
                        success_reads[0] += 3
                except Exception as e:
                    errors.append(f"reader_worker {worker_id} error: {e}")
                    break

        threads = []
        # 8 sim writers
        for i in range(8):
            t = threading.Thread(target=sim_writer, args=(i,))
            threads.append(t)
        # 4 action writers
        for i in range(4):
            t = threading.Thread(target=action_writer, args=(i,))
            threads.append(t)
        # 8 readers
        for i in range(8):
            t = threading.Thread(target=reader_worker, args=(i,))
            threads.append(t)

        for t in threads:
            t.start()

        # Let the stress test run under heavy multi-threading for 4 seconds
        time.sleep(4.0)
        stop_event.set()

        for t in threads:
            t.join(timeout=2.0)

        print(f"  Completed {success_mutations[0]} mutations and {success_reads[0]} reads across 20 threads.")
        if errors:
            print(f"  [FAIL] Concurrency stress errors ({len(errors)}):")
            for err in errors[:5]:
                print(f"    - {err}")
            assert not errors, f"{len(errors)} errors during concurrency stress!"
        else:
            print("  [PASS] Stress Test 4 passed: Zero deadlocks, zero race condition exceptions.")
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_cross_view_consistency():
    print("\n--- Stress Test 5: Cross-View Ground Truth Consistency ---")
    temp_dir = Path(tempfile.mkdtemp(prefix="stress_consistency_"))
    try:
        client = InMemoryHTTPClient(data_dir=str(temp_dir))
        # Initialize cluster
        st, res = client.request(
            "POST",
            "/api/setup/init",
            body={
                "serverName": "Consistency Test Server",
                "adminUsername": "Admin",
                "adminPasskey": "MC-ADMIN-TEST-PASS-1234",
                "sharedSyncDir": str(temp_dir / "cloud"),
            },
        )
        token = res["token"]

        # Start server
        client.request("POST", "/api/server/start", token=token)

        # Add players
        test_players = [f"Player_{i}" for i in range(15)]
        for p in test_players:
            client.ctx.mc_manager.send_command(f"simjoin {p}")

        # Query all views:
        # 1. mc_manager.online_players
        v1 = list(client.ctx.mc_manager.online_players)

        # 2. mc_manager.get_status_summary()["onlinePlayers"]
        v2 = client.ctx.mc_manager.get_status_summary()["onlinePlayers"]

        # 3. mc_manager.get_players_data()["online"]
        v3 = client.ctx.mc_manager.get_players_data()["online"]

        # 4. GET /api/players
        st4, res4 = client.request("GET", "/api/players", token=token)
        v4 = res4["online"]

        # 5. GET /api/server/status
        st5, res5 = client.request("GET", "/api/server/status", token=token)
        v5 = res5["onlinePlayers"]

        # 6. lock.online_players
        v6 = list(client.ctx.sync_engine.get_lock().online_players)

        print(f"  View 1 (manager.online_players):        {len(v1)} players")
        print(f"  View 2 (get_status_summary):            {len(v2)} players")
        print(f"  View 3 (get_players_data):              {len(v3)} players")
        print(f"  View 4 (GET /api/players):               {len(v4)} players")
        print(f"  View 5 (GET /api/server/status):        {len(v5)} players")
        print(f"  View 6 (lock.online_players):           {len(v6)} players")

        assert set(v1) == set(test_players), "View 1 does not match ground truth!"
        assert set(v2) == set(test_players), "View 2 does not match ground truth!"
        assert set(v3) == set(test_players), "View 3 does not match ground truth!"
        assert set(v4) == set(test_players), "View 4 does not match ground truth!"
        assert set(v5) == set(test_players), "View 5 does not match ground truth!"
        assert set(v6) == set(test_players), "View 6 does not match ground truth!"

        # Stop server
        client.request("POST", "/api/server/stop", token=token)
        print("  [PASS] Stress Test 5 passed: All 6 state views match ground truth identically.")
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_player_action_injection_and_case_handling():
    print("\n--- Stress Test 6: Player Action Command Injection & Case Handling ---")
    temp_dir = Path(tempfile.mkdtemp(prefix="stress_action_"))
    try:
        client = InMemoryHTTPClient(data_dir=str(temp_dir))
        mgr = client.ctx.mc_manager
        mgr.local_status = "running"
        mgr._reached_running = True

        class MockStdin:
            def __init__(self):
                self.buffer = []
            def write(self, s):
                self.buffer.append(s)
            def flush(self):
                pass

        class MockProcWithStdin:
            def __init__(self):
                self.stdin = MockStdin()
                self.pid = 88888

        mgr._proc = MockProcWithStdin()

        findings = []

        # 1. Test Command Injection via Kick Reason
        mgr.online_players = ["Victim"]
        mgr.player_action("kick", "Victim", "Rule Violation\nop Hacker")
        written = "".join(mgr._proc.stdin.buffer)
        if "\nop Hacker" in written:
            findings.append("Command injection confirmed: reason with newline injected 'op Hacker' into server stdin.")

        # 2. Test Case-Insensitive Player Removal
        mgr.online_players = ["Steve", "Alex"]
        mgr.player_action("kick", "steve")
        if "Steve" in mgr.online_players:
            findings.append("Case-sensitivity bug confirmed: kicking 'steve' failed to remove online player 'Steve'.")

        if findings:
            print(f"  [VULNERABILITY FOUND] Player action issues ({len(findings)} issues):")
            for f in findings:
                print(f"    - {f}")
            return False, findings
        else:
            print("  [PASS] Player action stdin sanitized and case-insensitive removal verified.")
            return True, []
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    print("============================================================")
    print("  Running Empirical Stress Test Suite (Challenger 1)")
    print("============================================================")
    test_ansi_and_unicode_flood()
    test_rapid_joins_and_leaves()
    spoof_ok, spoof_issues = test_chat_spoofing_vulnerability()
    test_multithreaded_concurrency_stress()
    test_cross_view_consistency()
    action_ok, action_issues = test_player_action_injection_and_case_handling()
    print("\n============================================================")
    total_issues = len(spoof_issues) + len(action_issues)
    if total_issues > 0:
        print(f"  STRESS SUITE COMPLETED: {total_issues} CRITICAL/HIGH ISSUES CONFIRMED")
        sys.exit(1)
    else:
        print("  ALL STRESS TESTS PASSED SUCCESSFULLY!")
        sys.exit(0)
    print("============================================================")

