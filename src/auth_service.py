import os
import json
import time
import uuid
import hmac
import hashlib
import secrets
import threading
from pathlib import Path
from typing import Optional, Dict, Tuple, List, Any
from src.models import ClusterConfig, UserAccount
from src.sync_engine import SyncEngine


class AuthError(Exception):
    pass


class PermissionDeniedError(Exception):
    pass


class AuthService:
    SESSION_TTL_SEC = 30 * 24 * 3600
    MAX_SESSIONS = 256

    def __init__(self, sync_engine: SyncEngine):
        self.sync_engine = sync_engine
        # Active session token -> user_id & salt mappings in memory + hashed persistence on local disk
        self._sessions: Dict[str, str] = {}
        self._session_times: Dict[str, float] = {}
        self._session_salts: Dict[str, str] = {}
        self._persisted_sessions: Dict[str, Dict[str, Any]] = {}
        self._sessions_path = self.sync_engine.base_data_dir / ".sessions.json"
        self._session_mutex = threading.RLock()
        self._config_mutex = getattr(sync_engine, "_config_mutex", None) or getattr(sync_engine, "_cluster_merge_lock", None) or threading.RLock()
        self._load_persisted_sessions()

    @staticmethod
    def _hash_session_token(token: str) -> str:
        return hashlib.sha256(token.encode("utf-8")).hexdigest()

    def _load_persisted_sessions(self) -> None:
        now = time.time()
        loaded: Dict[str, Dict[str, Any]] = {}
        try:
            if self._sessions_path.exists():
                raw = json.loads(self._sessions_path.read_text(encoding="utf-8"))
                if isinstance(raw, dict):
                    for tok_hash, meta in raw.items():
                        if not isinstance(tok_hash, str) or len(tok_hash) != 64 or not isinstance(meta, dict):
                            continue
                        uid = str(meta.get("userId") or "").strip()
                        ts = float(meta.get("issuedAt") or 0.0)
                        salt = str(meta.get("salt") or "")
                        if uid and ts > 0 and (now - ts) <= self.SESSION_TTL_SEC:
                            loaded[tok_hash] = {"userId": uid, "issuedAt": ts, "salt": salt}
        except Exception:
            loaded = {}
        with self._session_mutex:
            self._persisted_sessions = loaded

    def _save_persisted_sessions(self) -> None:
        try:
            with self._session_mutex:
                payload = dict(self._persisted_sessions)
            self._sessions_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_p = self._sessions_path.with_name(f".sessions.tmp.{os.getpid()}.{threading.get_ident()}")
            tmp_p.write_text(json.dumps(payload), encoding="utf-8")
            os.replace(tmp_p, self._sessions_path)
        except Exception:
            pass

    def clear_local_sessions(self) -> None:
        with self._session_mutex:
            self._sessions.clear()
            self._session_times.clear()
            self._session_salts.clear()
            self._persisted_sessions.clear()
            self._save_persisted_sessions()

    @staticmethod
    def generate_passkey(role: str = "member") -> str:
        prefix = "MC-ADMIN" if role == "admin" else "MC-HOST"
        alphabet = "23456789ABCDEFGHJKLMNPQRSTUVWXYZ"
        blocks = [
            "".join(secrets.choice(alphabet) for _ in range(4))
            for _ in range(3)
        ]
        return f"{prefix}-{'-'.join(blocks)}"

    @staticmethod
    def normalize_passkey(passkey: str) -> str:
        return passkey.strip().upper()

    @staticmethod
    def hash_passkey(passkey: str, salt: Optional[str] = None) -> Tuple[str, str]:
        norm = AuthService.normalize_passkey(passkey)
        use_salt = salt or secrets.token_hex(16)
        digest = hashlib.pbkdf2_hmac(
            "sha256",
            norm.encode("utf-8"),
            use_salt.encode("utf-8"),
            100_000,
        ).hex()
        return digest, use_salt

    @staticmethod
    def make_passkey_hint(passkey: str) -> str:
        norm = AuthService.normalize_passkey(passkey)
        for prefix in ("MC-ADMIN-", "MC-HOST-"):
            if norm.startswith(prefix) and len(norm) > len(prefix):
                norm = norm[len(prefix):]
                break
        return f"{norm[:2]}••••" if norm else "••••"

    def initialize_cluster(
        self,
        admin_username: str,
        server_name: str = "Shared Minecraft Server",
        custom_admin_passkey: Optional[str] = None,
        shared_sync_dir: Optional[str] = None,
    ) -> Tuple[ClusterConfig, UserAccount, str, str]:
        """
        Initializes a new cluster with the single Admin account.
        Validates preconditions before mutating local shared_sync_dir or config.
        Returns (cluster_config, admin_account, raw_admin_passkey, session_token).
        """
        with self._config_mutex:
            admin_username = admin_username.strip() or "Admin"
            if custom_admin_passkey and custom_admin_passkey.strip():
                raw_passkey = self.normalize_passkey(custom_admin_passkey)
                if len(raw_passkey) < 6:
                    raise ValueError("Custom admin passkey must be at least 6 characters.")
            else:
                raw_passkey = self.generate_passkey("admin")

            target_dir = Path(shared_sync_dir).resolve() if (shared_sync_dir and shared_sync_dir.strip()) else self.sync_engine.shared_dir
            if target_dir.exists() and not target_dir.is_dir():
                raise ValueError(f"Shared sync path '{target_dir}' is not a directory.")

            target_cfg_path = target_dir / "cluster-config.json"
            target_snap_path = target_dir / "snapshots" / "latest-server.zip"
            if target_cfg_path.exists():
                existing_target_cfg: Optional[ClusterConfig] = None
                try:
                    existing_target_cfg = self.sync_engine.load_cluster_config() if target_dir == self.sync_engine.shared_dir else ClusterConfig.from_dict(
                        __import__("json").loads(target_cfg_path.read_text(encoding="utf-8"))
                    )
                except Exception:
                    existing_target_cfg = None
                if existing_target_cfg and any(a.role == "admin" for a in existing_target_cfg.accounts):
                    raise AuthError("Cluster is already initialized with an Admin account.")
                if target_cfg_path.stat().st_size > 0 or existing_target_cfg is not None:
                    raise AuthError("Cluster configuration file already exists in this Shared Folder.")

            if target_snap_path.exists():
                raise AuthError("Existing world snapshot detected in this Shared Folder — wait for cloud sync or sign in instead of overwriting it.")

            marker_names = (".tmp.drivedownload", ".tmp.driveupload", ".dropbox.cache")
            for chk_dir in (target_dir, target_dir / "snapshots"):
                if chk_dir.exists():
                    for m in marker_names:
                        mp = chk_dir / m
                        try:
                            if mp.is_dir() and any(True for _ in mp.iterdir()):
                                raise AuthError("Cloud sync is currently downloading or uploading files in this folder; wait for it to finish.")
                        except OSError:
                            pass

            if shared_sync_dir and shared_sync_dir.strip():
                prev_dir = self.sync_engine.local_config.shared_sync_dir
                self.sync_engine.local_config.shared_sync_dir = shared_sync_dir
                self.sync_engine.save_local_config()
                self.sync_engine._ensure_shared_dirs()
                if prev_dir != shared_sync_dir:
                    self.sync_engine.reset_sync_state()
                    self.clear_local_sessions()

            existing = self.sync_engine.load_cluster_config()
            if existing and any(a.role == "admin" for a in existing.accounts):
                raise AuthError("Cluster is already initialized with an Admin account.")

            pk_hash, pk_salt = self.hash_passkey(raw_passkey)
            now = time.time()

            admin_account = UserAccount(
                id=str(uuid.uuid4()),
                username=admin_username,
                role="admin",
                passkey_hash=pk_hash,
                passkey_salt=pk_salt,
                passkey_hint=self.make_passkey_hint(raw_passkey),
                created_at=now,
                last_active_at=now,
            )

            config = ClusterConfig(
                server_name=server_name.strip() or "Shared Minecraft Server",
                accounts=[admin_account],
            )
            self.sync_engine.save_cluster_config(config)
            self.sync_engine._ensure_default_runtime_files()

        token = self.create_session(admin_account.id, salt=pk_salt)
        return config, admin_account, raw_passkey, token

    def login_with_passkey(self, passkey: str) -> Tuple[UserAccount, str]:
        """
        Authenticates any user (Admin or Member) using their unique personal passkey.
        Returns (account, session_token).
        """
        norm = self.normalize_passkey(passkey)
        if not norm:
            raise AuthError("Passkey is required.")

        config = self.sync_engine.load_cluster_config()
        if not config or not config.accounts:
            for _ in range(3):
                time.sleep(0.15)
                config = self.sync_engine.load_cluster_config()
                if config and config.accounts:
                    break
        if not config or not config.accounts:
            raise AuthError("Cluster is not initialized yet. Please complete initial setup or select an existing Shared Sync Folder.")

        matched_account: Optional[UserAccount] = None
        for acc in config.accounts:
            candidate_hash, _ = self.hash_passkey(norm, acc.passkey_salt)
            if hmac.compare_digest(candidate_hash, acc.passkey_hash):
                matched_account = acc
                break

        if not matched_account:
            raise AuthError("Invalid passkey. Please check your passkey and try again.")

        now = time.time()
        # Avoid rewriting shared cluster-config.json on rapid logins to prevent cloud conflict files
        # (Still writes 0.0 -> now on a brand-new account's first login)
        if (now - float(matched_account.last_active_at or 0.0)) >= 300.0:
            with self._config_mutex:
                fresh_cfg = self.sync_engine.load_cluster_config() or config
                for acc in fresh_cfg.accounts:
                    if acc.id == matched_account.id:
                        acc.last_active_at = now
                        matched_account.last_active_at = now
                        self.sync_engine.save_cluster_config(fresh_cfg)
                        break
        token = self.create_session(matched_account.id, salt=matched_account.passkey_salt)
        return matched_account, token

    def create_session(self, user_id: str, salt: str = "") -> str:
        eff_salt = str(salt or "")
        if not eff_salt:
            try:
                cfg = self.sync_engine.load_cluster_config()
                if cfg:
                    for acc in cfg.accounts:
                        if acc.id == user_id:
                            eff_salt = str(acc.passkey_salt or "")
                            break
            except Exception:
                pass
        token = secrets.token_urlsafe(32)
        tok_hash = self._hash_session_token(token)
        now = time.time()
        with self._session_mutex:
            expired = [t for t, ts in self._session_times.items() if (now - ts) > self.SESSION_TTL_SEC]
            for t in expired:
                self._sessions.pop(t, None)
                self._session_times.pop(t, None)
                self._session_salts.pop(t, None)
            expired_persisted = [
                h for h, meta in self._persisted_sessions.items()
                if (now - float(meta.get("issuedAt") or 0.0)) > self.SESSION_TTL_SEC
            ]
            for h in expired_persisted:
                self._persisted_sessions.pop(h, None)

            while len(self._sessions) >= self.MAX_SESSIONS and self._session_times:
                oldest = min(self._session_times, key=self._session_times.get)  # type: ignore[arg-type]
                self._sessions.pop(oldest, None)
                self._session_times.pop(oldest, None)
                self._session_salts.pop(oldest, None)
                self._persisted_sessions.pop(self._hash_session_token(oldest), None)
            while len(self._persisted_sessions) >= self.MAX_SESSIONS and self._persisted_sessions:
                oldest_h = min(
                    self._persisted_sessions,
                    key=lambda h: float(self._persisted_sessions[h].get("issuedAt") or 0.0),
                )
                self._persisted_sessions.pop(oldest_h, None)

            self._sessions[token] = user_id
            self._session_times[token] = now
            self._session_salts[token] = eff_salt
            self._persisted_sessions[tok_hash] = {
                "userId": user_id,
                "issuedAt": now,
                "salt": eff_salt,
            }
            self._save_persisted_sessions()
        return token

    def logout_session(self, token: str) -> None:
        if not token:
            return
        tok_hash = self._hash_session_token(token)
        with self._session_mutex:
            self._sessions.pop(token, None)
            self._session_times.pop(token, None)
            self._session_salts.pop(token, None)
            if tok_hash in self._persisted_sessions:
                self._persisted_sessions.pop(tok_hash, None)
                self._save_persisted_sessions()

    def _revoke_token_locked(self, token: str) -> None:
        tok_hash = self._hash_session_token(token)
        self._sessions.pop(token, None)
        self._session_times.pop(token, None)
        self._session_salts.pop(token, None)
        if tok_hash in self._persisted_sessions:
            self._persisted_sessions.pop(tok_hash, None)
            self._save_persisted_sessions()

    def get_user_by_token(self, token: Optional[str]) -> Optional[UserAccount]:
        if not token:
            return None
        now = time.time()
        tok_hash = self._hash_session_token(token)
        with self._session_mutex:
            user_id = self._sessions.get(token)
            issued_at = self._session_times.get(token, now)
            sess_salt = self._session_salts.get(token, "")
            if not user_id:
                pmeta = self._persisted_sessions.get(tok_hash)
                if pmeta:
                    user_id = str(pmeta.get("userId") or "")
                    issued_at = float(pmeta.get("issuedAt") or 0.0)
                    sess_salt = str(pmeta.get("salt") or "")
                    if user_id and (now - issued_at) <= self.SESSION_TTL_SEC:
                        self._sessions[token] = user_id
                        self._session_times[token] = issued_at
                        self._session_salts[token] = sess_salt
            if user_id and (now - issued_at) > self.SESSION_TTL_SEC:
                self._revoke_token_locked(token)
                user_id = None
        if not user_id:
            return None
        config = self.sync_engine.load_cluster_config()
        if not config:
            # Brief retry in case cloud sync transiently locked cluster-config.json
            time.sleep(0.05)
            config = self.sync_engine.load_cluster_config()
            if not config:
                return None
        for acc in config.accounts:
            if acc.id == user_id:
                # Reject sessions whose passkey was rotated on another PC
                if (sess_salt or acc.passkey_salt) and not hmac.compare_digest(str(sess_salt or ""), str(acc.passkey_salt or "")):
                    with self._session_mutex:
                        self._revoke_token_locked(token)
                    return None
                return acc
        # User was revoked by Admin! Remove session immediately.
        with self._session_mutex:
            self._revoke_token_locked(token)
        return None

    def require_auth(self, token: Optional[str]) -> UserAccount:
        user = self.get_user_by_token(token)
        if not user:
            raise AuthError("Authentication required or passkey was revoked.")
        return user

    def require_admin(self, token: Optional[str]) -> UserAccount:
        user = self.require_auth(token)
        if user.role != "admin":
            raise PermissionDeniedError(
                "Permission denied: Only the Admin account can perform this action. "
                "Member accounts are restricted to starting, stopping, and hosting the server."
            )
        return user

    def create_member_account(
        self,
        admin_token: str,
        username: str,
        custom_passkey: Optional[str] = None,
    ) -> Tuple[UserAccount, str]:
        """
        Admin-only: Creates a new Member account with its own unique passkey.
        Strictly enforces that only 1 Admin account exists; all new accounts are 'member'.
        """
        self.require_admin(admin_token)
        clean_name = username.strip()
        if not clean_name:
            raise ValueError("Username cannot be empty.")

        has_custom = bool(custom_passkey and custom_passkey.strip())
        if has_custom and len(self.normalize_passkey(custom_passkey or "")) < 6:
            raise ValueError("Custom passkey must be at least 6 characters.")

        with self._config_mutex:
            config = self.sync_engine.load_cluster_config()
            if not config:
                raise AuthError("Cluster not initialized.")

            for existing in config.accounts:
                if existing.username.lower() == clean_name.lower():
                    raise ValueError(f"An account with username '{clean_name}' already exists.")

            raw_passkey = ""
            for _ in range(5):
                candidate = self.normalize_passkey(custom_passkey or "") if has_custom else self.generate_passkey("member")
                collision = any(
                    hmac.compare_digest(self.hash_passkey(candidate, existing.passkey_salt)[0], existing.passkey_hash)
                    for existing in config.accounts
                )
                if not collision:
                    raw_passkey = candidate
                    break
                if has_custom:
                    raise ValueError("Passkey collision detected; please choose a different custom passkey.")
            if not raw_passkey:
                raise ValueError("Passkey collision detected; please try again.")

            pk_hash, pk_salt = self.hash_passkey(raw_passkey)
            member = UserAccount(
                id=str(uuid.uuid4()),
                username=clean_name,
                role="member",  # Strictly member; only 1 admin account is allowed
                passkey_hash=pk_hash,
                passkey_salt=pk_salt,
                passkey_hint=self.make_passkey_hint(raw_passkey),
                created_at=time.time(),
                last_active_at=0.0,
            )
            config.accounts.append(member)
            if isinstance(config.deleted_accounts, dict):
                config.deleted_accounts.pop(member.id, None)
            self.sync_engine.save_cluster_config(config)
            return member, raw_passkey

    def rename_account(self, admin_token: str, target_user_id: str, new_username: str) -> UserAccount:
        """
        Admin-only: Renames an account (Admin or Member).
        """
        self.require_admin(admin_token)
        clean_name = new_username.strip()
        if not clean_name:
            raise ValueError("Username cannot be empty.")

        with self._config_mutex:
            config = self.sync_engine.load_cluster_config()
            if not config:
                raise AuthError("Cluster not initialized.")

            for existing in config.accounts:
                if existing.id != target_user_id and existing.username.lower() == clean_name.lower():
                    raise ValueError(f"An account with username '{clean_name}' already exists.")

            target: Optional[UserAccount] = None
            for acc in config.accounts:
                if acc.id == target_user_id:
                    target = acc
                    break

            if not target:
                raise ValueError("Account not found.")

            target.username = clean_name
            self.sync_engine.save_cluster_config(config)
            try:
                with self.sync_engine._lock_mutex:
                    lock = self.sync_engine.get_lock()
                    if (
                        lock.is_actively_locked()
                        and self.sync_engine.is_locked_by_current_machine(lock)
                        and lock.host_user_id == target_user_id
                    ):
                        lock.host_username = clean_name
                        self.sync_engine.save_lock(lock, fsync=True, include_session=True)
            except Exception:
                pass
            return target

    def rotate_passkey(self, admin_token: str, target_user_id: str) -> Tuple[UserAccount, str]:
        """
        Admin-only: Generates a brand-new passkey for an account and invalidates old sessions.
        """
        self.require_admin(admin_token)
        with self._config_mutex:
            config = self.sync_engine.load_cluster_config()
            if not config:
                raise AuthError("Cluster not initialized.")

            target: Optional[UserAccount] = None
            for acc in config.accounts:
                if acc.id == target_user_id:
                    target = acc
                    break

            if not target:
                raise ValueError("Account not found.")

            new_raw_passkey = self.generate_passkey(target.role)
            pk_hash, pk_salt = self.hash_passkey(new_raw_passkey)
            target.passkey_hash = pk_hash
            target.passkey_salt = pk_salt
            target.passkey_hint = self.make_passkey_hint(new_raw_passkey)
            self.sync_engine.save_cluster_config(config)

        # Invalidate sessions for that user (preserving & updating salt on current admin_token if Admin rotated their own key)
        admin_tok_hash = self._hash_session_token(admin_token) if (admin_token and target.role == "admin") else ""
        with self._session_mutex:
            if admin_token and target.role == "admin" and admin_token in self._sessions and self._sessions[admin_token] == target_user_id:
                self._session_salts[admin_token] = pk_salt
                if admin_tok_hash in self._persisted_sessions:
                    self._persisted_sessions[admin_tok_hash]["salt"] = pk_salt
            to_remove = [
                tok
                for tok, uid in self._sessions.items()
                if uid == target_user_id and (target.role != "admin" or tok != admin_token)
            ]
            for tok in to_remove:
                self._sessions.pop(tok, None)
                self._session_times.pop(tok, None)
                self._session_salts.pop(tok, None)
            bad_hashes = [
                h for h, meta in self._persisted_sessions.items()
                if meta.get("userId") == target_user_id and h != admin_tok_hash
            ]
            for h in bad_hashes:
                self._persisted_sessions.pop(h, None)
            self._save_persisted_sessions()

        return target, new_raw_passkey

    def delete_member_account(self, admin_token: str, target_user_id: str) -> bool:
        """
        Admin-only: Deletes/revokes a Member account. Cannot delete the primary Admin account.
        """
        self.require_admin(admin_token)
        with self._config_mutex:
            config = self.sync_engine.load_cluster_config()
            if not config:
                raise AuthError("Cluster not initialized.")

            target: Optional[UserAccount] = None
            for acc in config.accounts:
                if acc.id == target_user_id:
                    target = acc
                    break

            if not target:
                raise ValueError("Account not found.")
            if target.role == "admin":
                raise ValueError("Cannot delete the primary Admin account.")

            config.accounts = [a for a in config.accounts if a.id != target_user_id]
            if not isinstance(config.deleted_accounts, dict):
                config.deleted_accounts = {}
            now = time.time()
            config.deleted_accounts[target_user_id] = max(now, float(target.created_at or 0.0) + 1.0)
            if len(config.deleted_accounts) > 200:
                cutoff = now - (30 * 86400)
                config.deleted_accounts = {
                    k: ts for k, ts in config.deleted_accounts.items() if ts >= cutoff or k == target_user_id
                }
            self.sync_engine.save_cluster_config(config)

        # Invalidate any active sessions for the revoked member
        with self._session_mutex:
            to_remove = [tok for tok, uid in self._sessions.items() if uid == target_user_id]
            for tok in to_remove:
                self._sessions.pop(tok, None)
                self._session_times.pop(tok, None)
                self._session_salts.pop(tok, None)
            bad_hashes = [
                h for h, meta in self._persisted_sessions.items()
                if meta.get("userId") == target_user_id
            ]
            for h in bad_hashes:
                self._persisted_sessions.pop(h, None)
            self._save_persisted_sessions()
        return True
