"""
Empirical Verification Harness - Challenger 2 (Iteration 2)
Verifies:
1. PUT /api/admin/settings with {"serverPort": null} returns HTTP 400 BAD_REQUEST.
2. SSE client abrupt disconnect terminates cleanly with close_connection = True
   and produces NO ConnectionAbortedError tracebacks in stderr.
3. Type-error resilience across other API inputs.
"""

import io
import os
import sys
import json
import time
import socket
import struct
import tempfile
import threading
import http.client
from pathlib import Path
from typing import Any, Dict, List, Tuple

os.environ["MINERELAY_TEST_MODE"] = "1"

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.server import AppContext, create_request_handler, run_server


class RawHTTPClient:
    def __init__(self, ctx: AppContext):
        self.ctx = ctx
        self.public_dir = ROOT_DIR / "public"
        self.handler_cls = create_request_handler(self.ctx, self.public_dir)

    def send_raw(self, method: str, path: str, headers: Dict[str, str], raw_body: bytes) -> Tuple[int, Dict[str, Any], bytes]:
        req_lines = [f"{method} {path} HTTP/1.1", "Host: localhost", "Connection: close"]
        for k, v in headers.items():
            req_lines.append(f"{k}: {v}")
        if "Content-Length" not in headers:
            req_lines.append(f"Content-Length: {len(raw_body)}")
        
        raw_req = ("\r\n".join(req_lines) + "\r\n\r\n").encode("utf-8") + raw_body
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
        out = wfile.getvalue()
        header_end = out.find(b"\r\n\r\n")
        assert header_end != -1, "No HTTP headers received"
        headers_raw = out[:header_end].decode("utf-8", errors="replace")
        body_raw = out[header_end + 4:]
        status_line = headers_raw.splitlines()[0]
        status_code = int(status_line.split(" ")[1])

        parsed_json = {}
        if body_raw:
            try:
                parsed_json = json.loads(body_raw.decode("utf-8"))
            except Exception:
                pass
        return status_code, parsed_json, body_raw


def test_put_settings_null_and_invalid_types():
    print("\n--- [Empirical Test 1] Testing PUT /api/admin/settings with null & invalid types ---")
    temp_root = Path(tempfile.mkdtemp(prefix="minerelay_challenger2_settings_"))
    shared_cloud_dir = temp_root / "SharedDrive"
    data_dir = temp_root / "server_data"

    ctx = AppContext(data_dir=str(data_dir))
    client = RawHTTPClient(ctx)

    init_body = json.dumps({
        "serverName": "Challenger SMP",
        "adminUsername": "ChallengerAdmin",
        "customPasskey": "MC-ADMIN-PASS-1234",
        "sharedSyncDir": str(shared_cloud_dir),
    }).encode("utf-8")

    st, res, _ = client.send_raw("POST", "/api/setup/init", {"Content-Type": "application/json"}, init_body)
    assert st == 200, f"Init failed: {res}"
    admin_token = res["token"]

    # 1. Test {"serverPort": null}
    payload_null = json.dumps({"serverPort": None}).encode("utf-8")
    status, data, _ = client.send_raw(
        "PUT",
        "/api/admin/settings",
        {"Content-Type": "application/json", "Authorization": f"Bearer {admin_token}"},
        payload_null,
    )
    print(f"  Result for {{'serverPort': null}}: HTTP {status}, payload={data}")
    assert status == 400, f"Expected 400 for null serverPort, got {status} ({data})"
    assert data.get("code") == "BAD_REQUEST", f"Expected BAD_REQUEST code, got {data}"
    assert "error" in data, "Expected error message in response"
    print("  [PASS] PUT /api/admin/settings with {'serverPort': null} cleanly returns 400 BAD_REQUEST.")

    # 2. Test other non-int / invalid type variants
    invalid_cases = [
        ("string", {"serverPort": "not_a_port"}),
        ("empty string", {"serverPort": ""}),
        ("list", {"serverPort": [25565]}),
        ("dict", {"serverPort": {"port": 25565}}),
        ("negative", {"serverPort": -1}),
        ("zero", {"serverPort": 0}),
        ("overflow", {"serverPort": 65536}),
        ("boolean true", {"serverPort": True}),
        ("boolean false", {"serverPort": False}),
        ("boolean true memoryMinMb", {"memoryMinMb": True}),
        ("boolean true maxBackups", {"maxBackups": True}),
    ]

    for label, payload_dict in invalid_cases:
        status, data, _ = client.send_raw(
            "PUT",
            "/api/admin/settings",
            {"Content-Type": "application/json", "Authorization": f"Bearer {admin_token}"},
            json.dumps(payload_dict).encode("utf-8"),
        )
        print(f"  Result for {label} ({payload_dict}): HTTP {status}, payload={data}")
        assert status == 400, f"Expected 400 for {label}, got {status} ({data})"
        assert data.get("code") == "BAD_REQUEST"

    # 3. Test valid ports
    for valid_port in [1, 25565, 65535]:
        status, data, _ = client.send_raw(
            "PUT",
            "/api/admin/settings",
            {"Content-Type": "application/json", "Authorization": f"Bearer {admin_token}"},
            json.dumps({"serverPort": valid_port}).encode("utf-8"),
        )
        assert status == 200, f"Expected 200 for valid port {valid_port}, got {status} ({data})"
        assert data["cluster"]["serverPort"] == valid_port

    print("  [PASS] All port validation boundaries and invalid types verified successfully.")

    # 4. Broader API endpoints TypeError & None handling
    print("  Testing null / unexpected types across other settings fields & API endpoints...")
    settings_null_fields = [
        {"memoryMinMb": None},
        {"memoryMaxMb": None},
        {"maxBackups": None},
        {"serverPort": None},
    ]
    for s_field in settings_null_fields:
        st, d, _ = client.send_raw(
            "PUT",
            "/api/admin/settings",
            {"Content-Type": "application/json", "Authorization": f"Bearer {admin_token}"},
            json.dumps(s_field).encode("utf-8"),
        )
        assert st == 400, f"Expected 400 for {s_field}, got {st} ({d})"
        assert d.get("code") == "BAD_REQUEST"
        print(f"    [OK] PUT /api/admin/settings with {s_field} rejected with 400 BAD_REQUEST: {d.get('error')}")

    endpoints_to_test = [
        ("PUT", "/api/admin/properties", {"properties": None}),
        ("PUT", "/api/admin/properties", {"properties": "not_a_dict"}),
        ("POST", "/api/admin/players", {"action": "invalid_action", "username": "steve"}),
        ("POST", "/api/admin/players", {"action": "kick", "username": ""}),
        ("POST", "/api/admin/files/write", {"path": "", "content": "test"}),
    ]

    for meth, ep, body_obj in endpoints_to_test:
        st, d, _ = client.send_raw(
            meth,
            ep,
            {"Content-Type": "application/json", "Authorization": f"Bearer {admin_token}"},
            json.dumps(body_obj).encode("utf-8"),
        )
        assert st in (400, 200, 201), f"Endpoint {meth} {ep} returned unexpected status {st} ({d}) on {body_obj}"
        if st == 400:
            assert d.get("code") in ("BAD_REQUEST", "INVALID_ARGUMENT", "VALIDATION_ERROR"), f"Unexpected code {d} for {ep}"
            print(f"    [OK] {meth} {ep} safely rejected invalid type {body_obj} with HTTP 400 ({d.get('error')})")
        else:
            print(f"    [OK] {meth} {ep} handled optional field {body_obj} gracefully with HTTP {st}")

    print("  [PASS] All tested API endpoints handle null and malformed types safely without HTTP 500.")


def test_sse_abrupt_disconnect_clean_stderr():
    print("\n--- [Empirical Test 2] Testing SSE Client Abrupt Disconnect & Stderr Cleanliness ---")
    temp_root = Path(tempfile.mkdtemp(prefix="minerelay_challenger2_sse_"))
    shared_cloud_dir = temp_root / "SharedDrive"
    server_data_dir = temp_root / "live_server_data"

    live_srv = run_server(port=0, open_browser=False, data_dir=str(server_data_dir))
    live_port = live_srv.server_address[1]

    srv_thread = threading.Thread(target=live_srv.serve_forever, daemon=True)
    srv_thread.start()
    time.sleep(0.3)

    live_ctx: AppContext = live_srv.app_context  # type: ignore
    _, live_admin, _, live_token = live_ctx.auth_service.initialize_cluster(
        admin_username="LiveAdmin",
        server_name="Live SMP",
        custom_admin_passkey="MC-LIVE-ADMIN-1234",
        shared_sync_dir=str(shared_cloud_dir),
    )

    # Capture stderr during server run to verify no ConnectionAbortedError tracebacks
    class StderrCatcher(io.StringIO):
        def __init__(self, original_stderr):
            super().__init__()
            self.original_stderr = original_stderr
        def write(self, s):
            self.original_stderr.write(s)  # keep echoing to console
            return super().write(s)

    original_stderr = sys.stderr
    captured_stderr = StderrCatcher(original_stderr)

    try:
        sys.stderr = captured_stderr

        # Check physical TCP connect availability
        tcp_permitted = True
        try:
            _probe = socket.socket()
            _probe.settimeout(1.0)
            _probe.connect(("127.0.0.1", live_port))
            _probe.close()
        except (PermissionError, OSError) as _probe_err:
            if isinstance(_probe_err, PermissionError) or getattr(_probe_err, "winerror", None) == 10013:
                tcp_permitted = False
                print(f"  [NOTICE] Physical TCP loopback blocked by OS sandbox ({_probe_err}); testing SSE disconnect handler via Simulated Abrupt Socket.")
            else:
                raise

        if tcp_permitted:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(2.0)
            sock.connect(("127.0.0.1", live_port))

            # Send HTTP/1.1 request (defaults to keep-alive!)
            req_lines = [
                f"GET /api/events?token={live_token} HTTP/1.1",
                f"Host: 127.0.0.1:{live_port}",
                "Accept: text/event-stream",
                "",
                "",
            ]
            sock.sendall("\r\n".join(req_lines).encode("utf-8"))

            # Read until header end
            resp_data = b""
            while b"\r\n\r\n" not in resp_data:
                chunk = sock.recv(1024)
                if not chunk:
                    break
                resp_data += chunk

            assert b"200 OK" in resp_data, f"Expected 200 OK, got: {resp_data[:100]}"
            assert b"text/event-stream" in resp_data, "Expected text/event-stream Content-Type"

            # Verify initial connected event is received
            while b"data: " not in resp_data:
                chunk = sock.recv(1024)
                if not chunk:
                    break
                resp_data += chunk
            assert b"connected" in resp_data

            with live_ctx.sse_mutex:
                assert len(live_ctx.sse_clients) == 1, f"Expected 1 client in live_ctx.sse_clients, got {len(live_ctx.sse_clients)}"
            print("  [OK] SSE client connected and received initial state over HTTP/1.1 keep-alive.")

            # Hard abort the connection with RST packet (SO_LINGER time=0)
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
            except Exception:
                pass
            sock.close()
            print("  [OK] SSE client forcefully aborted socket connection with RST.")

            # Trigger broadcast events to force server write
            for i in range(5):
                live_ctx.broadcast_event("stress_event", {"iteration": i, "timestamp": time.time()})
                time.sleep(0.1)

            # Allow time for server handler thread to complete finally block
            time.sleep(0.5)

            with live_ctx.sse_mutex:
                assert len(live_ctx.sse_clients) == 0, f"Expected 0 clients in live_ctx.sse_clients after disconnect, got {len(live_ctx.sse_clients)}"
            print("  [OK] Client queue successfully removed from live_ctx.sse_clients.")

            # Test 10 rapid concurrent connects and hard aborts
            print("  Testing 10 rapid concurrent connects and hard aborts...")
            for j in range(10):
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(2.0)
                s.connect(("127.0.0.1", live_port))
                s.sendall(f"GET /api/events?token={live_token} HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n".encode("utf-8"))
                time.sleep(0.02)
                try:
                    s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack('ii', 1, 0))
                except Exception:
                    pass
                s.close()

            # Broadcast events
            for i in range(5):
                live_ctx.broadcast_event("flush_event", {"iteration": i})
                time.sleep(0.1)

            time.sleep(0.5)
            with live_ctx.sse_mutex:
                assert len(live_ctx.sse_clients) == 0, f"Expected 0 clients remaining, got {len(live_ctx.sse_clients)}"
        else:
            # Simulated abrupt socket disconnect test inside restricted sandbox
            class MockBrokenSocket:
                def __init__(self, req_bytes: bytes, fail_after_bytes: int = 50):
                    self.rfile = io.BytesIO(req_bytes)
                    self.wfile = io.BytesIO()
                    self.fail_after_bytes = fail_after_bytes
                    self.written = 0

                def makefile(self, mode, *args, **kwargs):
                    if "r" in mode:
                        return self.rfile
                    return self

                def write(self, data):
                    self.written += len(data)
                    if self.written > self.fail_after_bytes:
                        raise ConnectionAbortedError(10053, "An established connection was aborted by the software in your host machine")
                    self.wfile.write(data)

                def flush(self):
                    pass

                def sendall(self, data):
                    self.write(data)

                def close(self):
                    pass

            req_raw = f"GET /api/events?token={live_token} HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n".encode("utf-8")
            handler_cls = create_request_handler(live_ctx, ROOT_DIR / "public")
            threads = []
            for _ in range(5):
                mock_sock = MockBrokenSocket(req_raw, fail_after_bytes=40)
                t = threading.Thread(target=lambda s=mock_sock: handler_cls(s, ("127.0.0.1", 54321), None))
                threads.append(t)
                t.start()

            time.sleep(0.1)
            for i in range(5):
                live_ctx.broadcast_event("sandboxed_stress", {"iteration": i})
                time.sleep(0.05)

            for t in threads:
                t.join(timeout=2.0)

            time.sleep(0.3)
            with live_ctx.sse_mutex:
                assert len(live_ctx.sse_clients) == 0, f"Expected 0 clients after simulated aborts, got {len(live_ctx.sse_clients)}"
            print("  [OK] Simulated abrupt socket disconnects handled cleanly and client queues evicted.")

        # Inspect captured stderr
        stderr_text = captured_stderr.getvalue()
        print(f"  Inspecting stderr output ({len(stderr_text)} chars)...")

        # Must NOT contain tracebacks or ConnectionAbortedError
        forbidden_strings = [
            "ConnectionAbortedError",
            "WinError 10053",
            "Traceback (most recent call last):",
            "Exception happened during processing of request",
        ]
        found_errors = [f for f in forbidden_strings if f in stderr_text]
        assert not found_errors, f"Found unhandled errors in stderr: {found_errors}\nStderr snippet:\n{stderr_text}"
        print("  [PASS] Zero ConnectionAbortedError / unhandled socketserver tracebacks in stderr!")

    finally:
        sys.stderr = original_stderr
        live_srv.shutdown()
        live_srv.server_close()


if __name__ == "__main__":
    try:
        test_put_settings_null_and_invalid_types()
        test_sse_abrupt_disconnect_clean_stderr()
        print("\n============================================================")
        print("  ALL EMPIRICAL CHALLENGER 2 VERIFICATION TESTS PASSED!   ")
        print("============================================================\n")
        sys.exit(0)
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"\n[FAIL] Test failed with: {e}")
        sys.exit(1)
