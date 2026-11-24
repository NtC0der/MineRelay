"""
Focused regression suite for:
  1. Automatic GitHub release checking, background binary auto-downloading,
     PE/SHA-256 verification, error/retry, /api/app/update/file streaming,
     and /api/app/update/apply hosting-lock guards.
  2. Codebase bug-sweep regressions across SyncEngine, AuthService, TunnelManager,
     and server.py HTTP/security/lifecycle paths.

Runs strictly inside isolated temporary directories.
"""

import io
import os
import sys
import time
import json
import shutil
import hashlib
import zipfile
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, Tuple
from unittest.mock import patch

os.environ["MINERELAY_TEST_MODE"] = "1"
os.environ["MINERELAY_AUTO_UPDATE_TEST"] = "1"

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

import src.bootstrap_runtime as bootstrap_rt
from src.models import APP_VERSION, ClusterConfig, UserAccount
from src.sync_engine import SyncEngine, HostLockConflictError
from src.auth_service import AuthService, AuthError
from src.server import AppContext, create_request_handler


class InMemoryHTTPClient:
    """Dispatches raw HTTP/1.1 requests through DashboardRequestHandler in-memory."""

    def __init__(self, data_dir: str):
        self.ctx = AppContext(data_dir=data_dir)
        self.public_dir = ROOT_DIR / "public"
        self.handler_cls = create_request_handler(self.ctx, self.public_dir)

    def request(
        self,
        method: str,
        path: str,
        token: str = "",
        body: Any = None,
        host_header: str = "localhost",
        origin_header: str = "",
        raw_response_bytes: bool = False,
    ) -> Tuple[int, Any]:
        body_bytes = json.dumps(body).encode("utf-8") if body is not None else b""
        headers_lines = [
            f"{method} {path} HTTP/1.1",
            f"Host: {host_header}" if host_header is not None else "",
            "Content-Type: application/json",
            f"Content-Length: {len(body_bytes)}",
        ]
        headers_lines = [ln for ln in headers_lines if ln]
        if origin_header:
            headers_lines.append(f"Origin: {origin_header}")
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
        if raw_response_bytes:
            return status_code, payload_part
        try:
            parsed = json.loads(payload_part.decode("utf-8"))
        except Exception:
            parsed = {"raw": payload_part.decode("utf-8", errors="replace")}
        return status_code, parsed


def _make_fake_pe_bytes(tag: str, size: int = 80 * 1024) -> bytes:
    header = b"MZ" + f"MINERELAY-TEST-PE-{tag}".encode("ascii").ljust(126, b"\x00")
    pad_len = max(0, size - len(header))
    return header + (b"\x90" * pad_len)


def test_auto_update_flow(tmp_root: Path) -> None:
    print("\n[Suite 1] Testing GitHub Auto-Download, Verification, Streaming & Apply Guards...")
    payload_v1 = _make_fake_pe_bytes("9.9.9")
    sha_v1 = hashlib.sha256(payload_v1).hexdigest()
    payload_v2 = _make_fake_pe_bytes("9.9.10")
    sha_v2 = hashlib.sha256(payload_v2).hexdigest()

    mock_state: Dict[str, Any] = {
        "tag": "v9.9.9",
        "digest": f"sha256:{sha_v1}",
        "asset_bytes": payload_v1,
        "size": len(payload_v1),
    }

    srv_port_box = [0]

    class MockReleaseHandler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            return

        def do_GET(self):
            if self.path == "/releases/latest":
                body = json.dumps({
                    "tag_name": mock_state["tag"],
                    "name": f"MineRelay {mock_state['tag']}",
                    "html_url": f"http://127.0.0.1:{srv_port_box[0]}/release/{mock_state['tag']}",
                    "assets": [
                        {
                            "name": "MineRelay.exe",
                            "size": mock_state["size"],
                            "digest": mock_state["digest"],
                            "browser_download_url": f"http://127.0.0.1:{srv_port_box[0]}/assets/MineRelay.exe",
                        }
                    ],
                }).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
            if self.path == "/assets/MineRelay.exe":
                data = mock_state["asset_bytes"]
                self.send_response(200)
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                return
            self.send_response(404)
            self.end_headers()

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), MockReleaseHandler)
    srv_port_box[0] = httpd.server_address[1]
    t_srv = threading.Thread(target=httpd.serve_forever, daemon=True)
    t_srv.start()

    old_api_env = os.environ.get("MINERELAY_RELEASES_API_URL")
    try:
        os.environ["MINERELAY_RELEASES_API_URL"] = f"http://127.0.0.1:{srv_port_box[0]}/releases/latest"
        bootstrap_rt._RELEASE_CACHE = None

        app_dir = tmp_root / "auto_upd_data"
        client = InMemoryHTTPClient(str(app_dir))

        st, res = client.request("GET", "/api/setup/status")
        assert st == 200
        vc = res.get("versionCheck") or {}
        assert vc.get("isOutdated") is True and vc.get("latestVersion") == "9.9.9", f"Unexpected versionCheck: {vc}"
        assert vc.get("assetSha256") == sha_v1

        # Wait for background auto-download to complete & verify
        deadline = time.time() + 10.0
        au: Dict[str, Any] = {}
        while time.time() < deadline:
            st, s_res = client.request("GET", "/api/setup/status")
            au = (s_res.get("versionCheck") or {}).get("autoUpdate") or {}
            if au.get("status") in ("downloaded", "ready_to_restart", "error"):
                break
            time.sleep(0.1)
        assert au.get("status") in ("downloaded", "ready_to_restart"), f"Expected downloaded auto-update, got: {au}"
        assert au.get("targetVersion") == "9.9.9"
        ready_path = client.ctx.auto_updater.get_ready_update_path()
        assert ready_path is not None and ready_path.exists()
        assert hashlib.sha256(ready_path.read_bytes()).hexdigest() == sha_v1

        # Download cached update via /api/app/update/file
        st, dl_bytes = client.request("GET", "/api/app/update/file", raw_response_bytes=True)
        assert st == 200 and dl_bytes == payload_v1

        # Bad asset / SHA mismatch sets error & leaves no partial files
        mock_state.update({
            "tag": "v9.9.10",
            "digest": f"sha256:{sha_v2}",
            "asset_bytes": b"NOT-A-VALID-PE-CORRUPT",
            "size": len(b"NOT-A-VALID-PE-CORRUPT"),
        })
        bootstrap_rt._RELEASE_CACHE = None
        st, _ = client.request("POST", "/api/app/update/download", body={})
        assert st == 200

        deadline = time.time() + 10.0
        while time.time() < deadline:
            au = client.ctx.auto_updater.get_state()
            if au.get("status") in ("error", "downloaded", "ready_to_restart"):
                break
            time.sleep(0.1)
        assert au.get("status") == "error", f"Expected error on corrupt asset, got: {au}"
        assert not list(client.ctx.auto_updater.updates_dir.glob("*.part")), "Partial .part file was not cleaned up"

        # Retry with valid v9.9.10 asset
        mock_state.update({
            "tag": "v9.9.10",
            "digest": f"sha256:{sha_v2}",
            "asset_bytes": payload_v2,
            "size": len(payload_v2),
        })
        st, retry_res = client.request("POST", "/api/app/update/download", body={})
        assert st == 200
        deadline = time.time() + 10.0
        while time.time() < deadline:
            au = client.ctx.auto_updater.get_state()
            if au.get("status") in ("downloaded", "ready_to_restart") and au.get("targetVersion") == "9.9.10":
                break
            time.sleep(0.1)
        assert au.get("status") in ("downloaded", "ready_to_restart") and au.get("targetVersion") == "9.9.10", f"Retry failed: {au}"

        # /api/app/update/apply blocks with 409 while server is hosting
        client.ctx.mc_manager.local_status = "running"
        st, err_apply = client.request("POST", "/api/app/update/apply", body={})
        assert st == 409 and err_apply.get("code") == "HOST_LOCK_CONFLICT", f"Expected 409 when hosting: {st} {err_apply}"
        client.ctx.mc_manager.local_status = "idle"

        # Idle in source mode returns downloadUrl fallback
        st, ok_apply = client.request("POST", "/api/app/update/apply", body={})
        assert st == 200 and ok_apply.get("downloadUrl") == "/api/app/update/file", f"Unexpected apply result: {ok_apply}"
    finally:
        httpd.shutdown()
        httpd.server_close()
        if old_api_env is None:
            os.environ.pop("MINERELAY_RELEASES_API_URL", None)
        else:
            os.environ["MINERELAY_RELEASES_API_URL"] = old_api_env
        bootstrap_rt._RELEASE_CACHE = None

    print("  [OK] Suite 1 passed.")


def test_sync_engine_regressions(tmp_root: Path) -> None:
    print("\n[Suite 2] Testing SyncEngine Concurrency, Conflict, Delta & Backup Regressions...")
    shared = tmp_root / "sync_shared"
    d1 = tmp_root / "sync_pc1"
    d2 = tmp_root / "sync_pc2"

    se1 = SyncEngine(str(d1))
    se1.local_config.shared_sync_dir = str(shared)
    se1.save_local_config()
    se1._ensure_shared_dirs()

    se2 = SyncEngine(str(d2))
    se2.local_config.shared_sync_dir = str(shared)
    se2.save_local_config()

    # 2.1 _write_json_atomic raises instead of silently dropping writes on existing locked file
    locked_json = d1 / "locked_test.json"
    locked_json.write_text('{"a": 1}', encoding="utf-8")
    with patch("src.sync_engine.open", side_effect=PermissionError("locked")), \
         patch("src.sync_engine.os.replace", side_effect=PermissionError("locked")), \
         patch("src.sync_engine.shutil.copyfile", side_effect=PermissionError("locked")):
        raised = False
        try:
            se1._write_json_atomic(locked_json, {"a": 2}, in_place_if_exists=True)
        except OSError:
            raised = True
        assert raised, "_write_json_atomic silently swallowed failure on locked existing file!"

    # 2.2 Zero-change create_backup=True must not emit empty delta zips
    rt1 = se1.runtime_dir
    (rt1 / "world" / "region").mkdir(parents=True, exist_ok=True)
    (rt1 / "world" / "region" / "r.0.0.mca").write_bytes(b"REGIONDATA" * 200)
    (rt1 / "server.properties").write_text("motd=RegressionTest\n", encoding="utf-8")

    push1 = se1.push_world_from_runtime(create_backup=True, backup_label="init", host_username="Admin", backup_scope="cloud")
    assert push1["mode"] == "full" and push1.get("backupFile")
    push2 = se1.push_world_from_runtime(create_backup=True, backup_label="zero", host_username="Admin", backup_scope="cloud")
    assert len(list(se1.deltas_dir.glob("delta-*.zip"))) == 0, f"Empty delta created on zero-change backup: {list(se1.deltas_dir.glob('*.zip'))}"

    # 2.3 delete_backup preserves sync-manifest backupFile if target.unlink() fails
    bk_name = str(push2.get("backupFile") or push1.get("backupFile"))
    assert bk_name
    with patch.object(Path, "unlink", side_effect=PermissionError("drive lock")):
        failed_del = False
        try:
            se1.delete_backup(bk_name, location="cloud")
        except PermissionError:
            failed_del = True
        assert failed_del
    man_after = se1._read_sync_manifest()
    assert man_after and Path(str(man_after.get("backupFile", ""))).name == bk_name, "Manifest mutated before unlink succeeded!"

    # 2.4 _apply_incremental_pull fails closed when file-index.json is missing/corrupt in delta mode
    (rt1 / "world" / "region" / "r.0.0.mca").write_bytes(b"UPDATED" * 200)
    push3 = se1.push_world_from_runtime(create_backup=False, host_username="Admin", backup_scope="none")
    assert push3["mode"] == "delta"
    man3 = se1._read_sync_manifest()
    assert man3 and man3.get("mode") == "delta"
    idx_bak = se1.file_index_path.read_bytes()
    se1.file_index_path.unlink()
    failed_delta = False
    try:
        se2._apply_incremental_pull(se2.latest_snapshot_path, man3)
    except ValueError:
        failed_delta = True
    assert failed_delta, "_apply_incremental_pull returned None instead of failing closed in delta mode!"
    se1.file_index_path.write_bytes(idx_bak)

    # 2.5 release_lock forbids releasing another machine's active lock & preserves local _owns_active_lock if set
    admin = UserAccount(id="adm1", username="Admin", role="admin", passkey_hash="x", passkey_salt="y", passkey_hint="0000", created_at=time.time())
    se1.acquire_lock(admin, mc_version="Paper 26.3")
    se2._owns_active_lock = True  # simulate unrelated flag invariant
    denied = False
    try:
        se2.release_lock(admin)
    except HostLockConflictError:
        denied = True
    assert denied, "PC2 was allowed to release PC1's active lock!"
    assert se2._owns_active_lock is True, "Ownership flag cleared before authorization check!"
    se2._owns_active_lock = False
    se1.release_lock(admin)

    # 2.6 _merge_cluster_config_conflict syncs cloud_backup_on_stop, max_cloud_backups, latest_app_version & prevents duplicate admins
    shared_merge = tmp_root / "sync_shared_merge"
    se_merge = SyncEngine(str(tmp_root / "sync_merge_pc"))
    se_merge.local_config.shared_sync_dir = str(shared_merge)
    se_merge.save_local_config()
    auth1 = AuthService(se_merge)
    cfg1, _, _, _ = auth1.initialize_cluster("Admin", "ClusterTest", shared_sync_dir=str(shared_merge))
    conflict_cfg = dict(cfg1.to_dict(include_secrets=True))
    conflict_cfg["cloudBackupOnStop"] = True
    conflict_cfg["maxCloudBackups"] = 7
    conflict_cfg["latestAppVersion"] = "9.9.10"
    conflict_cfg["accounts"].append({
        "id": "adm-dup",
        "username": "AdminDup",
        "role": "admin",
        "passkeyHash": "aa",
        "passkeySalt": "bb",
        "passkeyHint": "9999",
        "createdAt": time.time(),
        "lastActiveAt": time.time(),
        "hostCount": 3,
    })
    cpath = shared_merge / "cluster-config (1).json"
    cpath.write_text(json.dumps(conflict_cfg), encoding="utf-8")
    os.utime(cpath, (time.time() + 10, time.time() + 10))
    merged = se_merge.load_cluster_config()
    assert merged is not None
    assert merged.cloud_backup_on_stop is True
    assert merged.max_cloud_backups == 7
    assert merged.latest_app_version == "9.9.10"
    assert sum(1 for a in merged.accounts if a.role == "admin") == 1, "Duplicate admin created during merge!"

    print("  [OK] Suite 2 passed.")


def test_auth_and_http_regressions(tmp_root: Path) -> None:
    print("\n[Suite 3] Testing AuthService Persistence & HTTP/API Regressions...")
    data_dir = tmp_root / "http_app_data"
    shared = tmp_root / "http_shared"
    shared.mkdir(parents=True, exist_ok=True)
    client = InMemoryHTTPClient(str(data_dir))

    # 3.1 UNC path rejection on /api/setup/sync-dir
    st, unc_res = client.request("POST", "/api/setup/sync-dir", body={"sharedSyncDir": "\\\\fileserver\\share\\MineRelay"})
    assert st == 400 and "UNC" in unc_res.get("error", ""), f"UNC path should be rejected: {st} {unc_res}"

    # 3.2 Host & Origin validation
    st, _ = client.request("GET", "/api/setup/status", host_header="evil.attacker.com:8080")
    assert st == 403, f"Bad Host should return 403, got {st}"
    st, _ = client.request("POST", "/api/setup/sync-dir", host_header="localhost", origin_header="https://evil.attacker.com", body={"sharedSyncDir": str(shared)})
    assert st == 403, f"Bad Origin should return 403, got {st}"

    # 3.3 Failed initialize_cluster preserves previous shared_sync_dir
    prev_dir = client.ctx.sync_engine.local_config.shared_sync_dir
    failed_init = False
    try:
        client.ctx.auth_service.initialize_cluster("Admin", "BadInit", custom_admin_passkey="123", shared_sync_dir=str(shared))
    except (AuthError, ValueError):
        failed_init = True
    assert failed_init
    assert client.ctx.sync_engine.local_config.shared_sync_dir == prev_dir

    # 3.4 Init cluster, verify session persistence across AuthService reload & rotation salt invalidation
    st, init_res = client.request("POST", "/api/setup/init", body={
        "adminUsername": "Admin",
        "serverName": "HTTPRegTest",
        "sharedSyncDir": str(shared),
        "machineName": "PrimaryPC",
    })
    assert st == 200
    admin_tok = init_res["token"]
    admin_acc_id = init_res["account"]["id"]
    admin_pk = init_res["generatedPasskey"]

    st, mem_res = client.request("POST", "/api/admin/accounts", token=admin_tok, body={"username": "MemberOne"})
    assert st == 201
    mem_id = mem_res["account"]["id"]
    mem_pk = mem_res["generatedPasskey"]
    _, mem_tok = client.ctx.auth_service.login_with_passkey(mem_pk)

    reloaded_auth = AuthService(client.ctx.sync_engine)
    assert reloaded_auth.get_user_by_token(mem_tok) is not None, "Session did not persist in .sessions.json!"

    client.ctx.auth_service.rotate_passkey(admin_tok, mem_id)
    assert reloaded_auth.get_user_by_token(mem_tok) is None, "Rotated passkey salt did not invalidate cross-instance session!"

    # 3.5 Failed login does not overwrite local machine_name
    st, _ = client.request("POST", "/api/auth/login", body={"passkey": "MC-HOST-WRONG-PASS-KEYY", "machineName": "HijackName"})
    assert st == 401
    assert client.ctx.sync_engine.local_config.machine_name == "PrimaryPC"

    # 3.6 Invalid tunnelProvider rejected with 400
    st, bad_tp = client.request("PUT", "/api/admin/settings", token=admin_tok, body={"tunnelProvider": "ngrok"})
    assert st == 400 and "tunnelProvider" in bad_tp.get("error", ""), f"Expected 400 for invalid tunnelProvider: {st} {bad_tp}"

    # 3.7 Custom tunnelProvider starts without playit binary cleanly
    st, _ = client.request("PUT", "/api/admin/settings", token=admin_tok, body={
        "tunnelProvider": "custom",
        "publicJoinAddress": "play.custom.example:25565",
    })
    assert st == 200
    t_state = client.ctx.tunnel_manager.start_tunnel(server_port=25565)
    assert t_state["status"] == "online" and t_state["publicAddress"] == "play.custom.example:25565"
    client.ctx.tunnel_manager.stop_tunnel()

    # 3.8 Missing static asset extension returns 404 (not index.html)
    st, _ = client.request("GET", "/does-not-exist.js")
    assert st == 404, f"Missing .js asset should return 404, got {st}"

    # 3.9 PUT /api/admin/properties handles online-mode=false without spurious failures & backups download streams cleanly
    st, props_res = client.request("PUT", "/api/admin/properties", token=admin_tok, body={"properties": {"online-mode": False}})
    assert st == 200 and props_res["properties"]["online-mode"] == "false"
    st, _ = client.request("POST", "/api/admin/backups", token=admin_tok, body={"label": "stream", "scope": "local"})
    assert st == 200
    bks = client.ctx.sync_engine.list_backups()
    assert bks
    bk_name = bks[0]["name"]
    st, bk_raw = client.request("GET", f"/api/admin/backups/download?name={bk_name}&location=local", token=admin_tok, raw_response_bytes=True)
    assert st == 200 and bk_raw[:2] == b"PK", "Backup download did not stream valid ZIP bytes!"

    # 3.10 Push local backups to Shared Drive folder (single, duplicate skip, push all, auth/lock/404 guards)
    import hashlib
    _, new_mem_pk = client.ctx.auth_service.rotate_passkey(admin_tok, mem_id)
    _, mem_tok2 = client.ctx.auth_service.login_with_passkey(new_mem_pk)

    # Member forbidden
    st, _ = client.request("POST", "/api/admin/backups/push-cloud", token=mem_tok2, body={"name": bk_name})
    assert st == 403, f"Member should get 403 pushing backups to Shared Drive, got {st}"

    # Missing name or missing file
    st, _ = client.request("POST", "/api/admin/backups/push-cloud", token=admin_tok, body={})
    assert st == 400
    st, _ = client.request("POST", "/api/admin/backups/push-cloud", token=admin_tok, body={"name": "missing_backup.zip"})
    assert st == 404, f"Missing backup should return 404, got {st}"

    # Remote active lock returns 409
    from src.models import HostLock
    other_lock = HostLock(
        is_locked=True,
        host_user_id="mem-other",
        host_username="OtherUser",
        host_role="member",
        machine_id="other-machine-id",
        machine_name="OtherPC",
        started_at=time.time(),
        last_heartbeat=time.time(),
        status="running",
        lock_epoch="ep-other-pc",
    )
    client.ctx.sync_engine._write_lock_bundle(other_lock, include_session=True, fsync=True)
    try:
        st, _ = client.request("POST", "/api/admin/backups/push-cloud", token=admin_tok, body={"name": bk_name})
        assert st == 409, f"Remote active lock should block push-cloud with 409, got {st}"
    finally:
        client.ctx.sync_engine.force_unlock()

    # Push single local backup -> copied to snapshots/backups while preserving local copy
    st, push_one = client.request("POST", "/api/admin/backups/push-cloud", token=admin_tok, body={"name": bk_name})
    assert st == 200, f"push-cloud failed: {st} {push_one}"
    res1 = push_one["result"]
    assert res1["pushedCount"] == 1 and res1["pushed"] == [bk_name]
    assert res1["alreadyInCloud"] is False
    local_bk_path = client.ctx.sync_engine.local_backups_dir / bk_name
    cloud_bk_path = client.ctx.sync_engine.backups_dir / bk_name
    assert local_bk_path.exists() and cloud_bk_path.exists(), "Both local and cloud backups must exist after push!"
    assert hashlib.sha256(local_bk_path.read_bytes()).hexdigest() == hashlib.sha256(cloud_bk_path.read_bytes()).hexdigest()
    locs_for_bk = {item["location"] for item in push_one["backups"] if item["name"] == bk_name}
    assert locs_for_bk == {"local", "cloud"}, f"Expected both local and cloud locations, got {locs_for_bk}"

    # Idempotent re-push skips identical archive
    st, push_dup = client.request("POST", "/api/admin/backups/push-cloud", token=admin_tok, body={"name": bk_name})
    assert st == 200
    assert push_dup["result"]["pushedCount"] == 0
    assert push_dup["result"]["skippedCount"] == 1
    assert push_dup["result"]["alreadyInCloud"] is True

    # Create a second local backup & push all -> pushes only the new one and skips the existing one
    time.sleep(0.05)
    st, _ = client.request("POST", "/api/admin/backups", token=admin_tok, body={"label": "second", "scope": "local"})
    assert st == 200
    local_names = [b["name"] for b in client.ctx.sync_engine.list_backups() if b["location"] == "local"]
    second_name = [n for n in local_names if n != bk_name][0]
    st, push_all_res = client.request("POST", "/api/admin/backups/push-cloud", token=admin_tok, body={"all": True})
    assert st == 200, f"push-cloud all failed: {st} {push_all_res}"
    res_all = push_all_res["result"]
    assert res_all["pushedCount"] == 1 and res_all["pushed"] == [second_name]
    assert res_all["skippedCount"] == 1 and res_all["skipped"] == [bk_name]
    assert (client.ctx.sync_engine.local_backups_dir / second_name).exists()
    assert (client.ctx.sync_engine.backups_dir / second_name).exists()

    assert admin_acc_id and admin_pk  # sanity
    print("  [OK] Suite 3 passed.")


def run_all() -> None:
    print("=" * 64)
    print("  MineRelay Auto-Update & Bug Sweep Regression Suite")
    print("=" * 64)
    with tempfile.TemporaryDirectory(prefix="minerelay_autoupdate_sweep_") as td:
        tmp_root = Path(td)
        test_auto_update_flow(tmp_root)
        test_sync_engine_regressions(tmp_root)
        test_auth_and_http_regressions(tmp_root)
    print("\nALL AUTO-UPDATE & BUG SWEEP TESTS PASSED SUCCESSFULLY!")


if __name__ == "__main__":
    run_all()
