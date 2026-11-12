import time
import uuid
import hmac
import hashlib
import secrets
import threading
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
        # Active session token -> user_id mapping in memory
        self._sessions: Dict[str, str] = {}
        self._session_times: Dict[str, float] = {}
        self._session_mutex = threading.RLock()

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
        Returns (cluster_config, admin_account, raw_admin_passkey, session_token).
        """
        if shared_sync_dir:
            self.sync_engine.local_config.shared_sync_dir = shared_sync_dir
            self.sync_engine.save_local_config()

        existing = self.sync_engine.load_cluster_config()
        if existing and any(a.role == "admin" for a in existing.accounts):
            raise AuthError("Cluster is already initialized with an Admin account.")

        admin_username = admin_username.strip() or "Admin"
        raw_passkey = (
            self.normalize_passkey(custom_admin_passkey)
            if custom_admin_passkey and custom_admin_passkey.strip()
            else self.generate_passkey("admin")
        )
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

        token = self.create_session(admin_account.id)
        return config, admin_account, raw_passkey, token

    def login_with_passkey(self, passkey: str) -> Tuple[UserAccount, str]:
        """
        Authenticates any user (Admin or Member) using their unique personal passkey.
        Returns (account, session_token).
        """
        config = self.sync_engine.load_cluster_config()
        if not config or not config.accounts:
            raise AuthError("Cluster is not initialized yet. Please complete initial setup or select an existing Shared Sync Folder.")

        norm = self.normalize_passkey(passkey)
        if not norm:
            raise AuthError("Passkey is required.")

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
        if (now - float(matched_account.last_active_at or 0.0)) >= 300.0:
            matched_account.last_active_at = now
            self.sync_engine.save_cluster_config(config)
        token = self.create_session(matched_account.id)
        return matched_account, token

    def create_session(self, user_id: str) -> str:
        token = secrets.token_urlsafe(32)
        now = time.time()
        with self._session_mutex:
            expired = [t for t, ts in self._session_times.items() if (now - ts) > self.SESSION_TTL_SEC]
            for t in expired:
                self._sessions.pop(t, None)
                self._session_times.pop(t, None)
            while len(self._sessions) >= self.MAX_SESSIONS and self._session_times:
                oldest = min(self._session_times, key=self._session_times.get)  # type: ignore[arg-type]
                self._sessions.pop(oldest, None)
                self._session_times.pop(oldest, None)
            self._sessions[token] = user_id
            self._session_times[token] = now
        return token

    def logout_session(self, token: str) -> None:
        with self._session_mutex:
            self._sessions.pop(token, None)
            self._session_times.pop(token, None)

    def get_user_by_token(self, token: Optional[str]) -> Optional[UserAccount]:
        if not token:
            return None
        now = time.time()
        with self._session_mutex:
            user_id = self._sessions.get(token)
            issued_at = self._session_times.get(token, now)
            if user_id and (now - issued_at) > self.SESSION_TTL_SEC:
                self._sessions.pop(token, None)
                self._session_times.pop(token, None)
                user_id = None
        if not user_id:
            return None
        config = self.sync_engine.load_cluster_config()
        if not config:
            return None
        for acc in config.accounts:
            if acc.id == user_id:
                return acc
        # User was revoked by Admin! Remove session immediately.
        with self._session_mutex:
            self._sessions.pop(token, None)
            self._session_times.pop(token, None)
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
        config = self.sync_engine.load_cluster_config()
        if not config:
            raise AuthError("Cluster not initialized.")

        clean_name = username.strip()
        if not clean_name:
            raise ValueError("Username cannot be empty.")

        for existing in config.accounts:
            if existing.username.lower() == clean_name.lower():
                raise ValueError(f"An account with username '{clean_name}' already exists.")

        has_custom = bool(custom_passkey and custom_passkey.strip())
        raw_passkey = ""
        for _ in range(5):
            candidate = self.normalize_passkey(custom_passkey) if has_custom else self.generate_passkey("member")
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
        self.sync_engine.save_cluster_config(config)
        return member, raw_passkey

    def rename_account(self, admin_token: str, target_user_id: str, new_username: str) -> UserAccount:
        """
        Admin-only: Renames an account (Admin or Member).
        """
        self.require_admin(admin_token)
        config = self.sync_engine.load_cluster_config()
        if not config:
            raise AuthError("Cluster not initialized.")

        clean_name = new_username.strip()
        if not clean_name:
            raise ValueError("Username cannot be empty.")

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
        return target

    def rotate_passkey(self, admin_token: str, target_user_id: str) -> Tuple[UserAccount, str]:
        """
        Admin-only: Generates a brand-new passkey for an account and invalidates old sessions.
        """
        self.require_admin(admin_token)
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

        # Invalidate sessions for that user (preserving current admin_token if Admin rotated their own key)
        with self._session_mutex:
            to_remove = [
                tok
                for tok, uid in self._sessions.items()
                if uid == target_user_id and (target.role != "admin" or tok != admin_token)
            ]
            for tok in to_remove:
                self._sessions.pop(tok, None)

        return target, new_raw_passkey

    def delete_member_account(self, admin_token: str, target_user_id: str) -> bool:
        """
        Admin-only: Deletes/revokes a Member account. Cannot delete the primary Admin account.
        """
        self.require_admin(admin_token)
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
        self.sync_engine.save_cluster_config(config)

        # Invalidate any active sessions for the revoked member
        with self._session_mutex:
            to_remove = [tok for tok, uid in self._sessions.items() if uid == target_user_id]
            for tok in to_remove:
                self._sessions.pop(tok, None)
        return True
