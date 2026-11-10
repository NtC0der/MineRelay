import os
import uuid
import socket
import time
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Optional, Any

HEARTBEAT_INTERVAL_SEC = 10
LOCK_STALE_TIMEOUT_SEC = 45


def _safe_str(val: Any, default: str = "") -> str:
    if val is None:
        return default
    return str(val)


def _safe_int(val: Any, default: int = 0) -> int:
    if val is None or isinstance(val, bool):
        return default
    try:
        return int(val)
    except (ValueError, TypeError):
        return default


def _safe_float(val: Any, default: float = 0.0) -> float:
    if val is None or isinstance(val, bool):
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _safe_bool(val: Any, default: bool = False) -> bool:
    if val is None:
        return default
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.strip().lower() in ("true", "1", "yes")
    return bool(val)


@dataclass
class UserAccount:
    id: str
    username: str
    role: str  # 'admin' | 'member'
    passkey_hash: str
    passkey_salt: str
    passkey_hint: str  # e.g. "MC-HOST-8F2A...9C1B"
    created_at: float
    last_active_at: float = 0.0
    host_count: int = 0

    def to_public_dict(self) -> Dict[str, Any]:
        raw_hint = (self.passkey_hint or "").strip().upper()
        for prefix in ("MC-ADMIN-", "MC-HOST-"):
            if raw_hint.startswith(prefix) and len(raw_hint) > len(prefix):
                raw_hint = raw_hint[len(prefix):]
                break
        clean_chars = [c for c in raw_hint if c not in ("•", ".")]
        first_two = "".join(clean_chars[:2])
        short_hint = f"{first_two}••••" if first_two else "••••"
        return {
            "id": self.id,
            "username": self.username,
            "role": self.role,
            "passkeyHint": short_hint,
            "createdAt": self.created_at,
            "lastActiveAt": self.last_active_at,
            "hostCount": self.host_count,
        }


@dataclass
class HostLock:
    is_locked: bool = False
    host_user_id: str = ""
    host_username: str = ""
    host_role: str = ""
    machine_id: str = ""
    machine_name: str = ""
    started_at: float = 0.0
    last_heartbeat: float = 0.0
    status: str = "idle"  # 'idle' | 'syncing_pull' | 'starting' | 'running' | 'stopping' | 'syncing_push'
    public_address: str = ""
    tunnel_status: str = "stopped"
    mc_version: str = ""
    online_players: List[str] = field(default_factory=list)
    max_players: int = 20

    def is_stale(self, now: Optional[float] = None) -> bool:
        if not self.is_locked:
            return False
        current = now if now is not None else time.time()
        return (current - self.last_heartbeat) > LOCK_STALE_TIMEOUT_SEC

    def is_actively_locked(self, now: Optional[float] = None) -> bool:
        return self.is_locked and not self.is_stale(now)

    def to_dict(self) -> Dict[str, Any]:
        now = time.time()
        return {
            "isLocked": self.is_locked,
            "isStale": self.is_stale(now),
            "isActivelyLocked": self.is_actively_locked(now),
            "hostUserId": self.host_user_id,
            "hostUsername": self.host_username,
            "hostRole": self.host_role,
            "machineId": self.machine_id,
            "machineName": self.machine_name,
            "startedAt": self.started_at,
            "lastHeartbeat": self.last_heartbeat,
            "secondsSinceHeartbeat": round(now - self.last_heartbeat, 1) if self.last_heartbeat else None,
            "status": self.status if self.is_locked else "idle",
            "publicAddress": self.public_address,
            "tunnelStatus": self.tunnel_status,
            "mcVersion": self.mc_version,
            "onlinePlayers": self.online_players,
            "maxPlayers": self.max_players,
        }

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "HostLock":
        if not isinstance(data, dict):
            return HostLock()
        raw_players = data.get("online_players", data.get("onlinePlayers"))
        players_list = [str(p) for p in raw_players if p] if isinstance(raw_players, list) else []
        return HostLock(
            is_locked=_safe_bool(data.get("is_locked", data.get("isLocked")), False),
            host_user_id=_safe_str(data.get("host_user_id", data.get("hostUserId")), ""),
            host_username=_safe_str(data.get("host_username", data.get("hostUsername")), ""),
            host_role=_safe_str(data.get("host_role", data.get("hostRole")), ""),
            machine_id=_safe_str(data.get("machine_id", data.get("machineId")), ""),
            machine_name=_safe_str(data.get("machine_name", data.get("machineName")), ""),
            started_at=_safe_float(data.get("started_at", data.get("startedAt")), 0.0),
            last_heartbeat=_safe_float(data.get("last_heartbeat", data.get("lastHeartbeat")), 0.0),
            status=_safe_str(data.get("status"), "idle") or "idle",
            public_address=_safe_str(data.get("public_address", data.get("publicAddress")), ""),
            tunnel_status=_safe_str(data.get("tunnel_status", data.get("tunnelStatus")), "stopped") or "stopped",
            mc_version=_safe_str(data.get("mc_version", data.get("mcVersion")), ""),
            online_players=players_list,
            max_players=_safe_int(data.get("max_players", data.get("maxPlayers")), 20),
        )


@dataclass
class ClusterConfig:
    cluster_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    server_name: str = "Shared Minecraft Server"
    mc_software: str = "paper"  # 'paper' | 'vanilla'
    mc_version: str = "26.3"
    memory_min_mb: int = 2048
    memory_max_mb: int = 4096
    java_path: str = "java"
    jvm_flags: str = "-XX:+UseG1GC -XX:+ParallelRefProcEnabled -XX:MaxGCPauseMillis=200"
    server_port: int = 25565
    tunnel_enabled: bool = True
    tunnel_provider: str = "playit"  # 'playit' | 'custom' | 'none'
    public_join_address: str = ""
    git_sync_enabled: bool = False
    auto_backup_on_stop: bool = True
    max_backups: int = 10
    idle_shutdown_minutes: int = 0
    gamerules: Dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    accounts: List[UserAccount] = field(default_factory=list)

    def to_dict(self, include_secrets: bool = True) -> Dict[str, Any]:
        data = {
            "clusterId": self.cluster_id,
            "serverName": self.server_name,
            "mcSoftware": self.mc_software,
            "mcVersion": self.mc_version,
            "memoryMinMb": self.memory_min_mb,
            "memoryMaxMb": self.memory_max_mb,
            "javaPath": self.java_path,
            "jvmFlags": self.jvm_flags,
            "serverPort": self.server_port,
            "tunnelEnabled": self.tunnel_enabled,
            "tunnelProvider": self.tunnel_provider,
            "publicJoinAddress": self.public_join_address,
            "gitSyncEnabled": self.git_sync_enabled,
            "autoBackupOnStop": self.auto_backup_on_stop,
            "maxBackups": self.max_backups,
            "idleShutdownMinutes": self.idle_shutdown_minutes,
            "gamerules": dict(self.gamerules) if self.gamerules else {},
            "createdAt": self.created_at,
        }
        if include_secrets:
            data["accounts"] = [asdict(a) for a in self.accounts]
        else:
            data["accounts"] = [a.to_public_dict() for a in self.accounts]
        return data

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "ClusterConfig":
        if not isinstance(data, dict):
            return ClusterConfig()
        raw_accounts = data.get("accounts")
        accounts: List[UserAccount] = []
        if isinstance(raw_accounts, list):
            for acc in raw_accounts:
                if not isinstance(acc, dict):
                    continue
                accounts.append(
                    UserAccount(
                        id=_safe_str(acc.get("id"), str(uuid.uuid4())),
                        username=_safe_str(acc.get("username"), "Member"),
                        role=_safe_str(acc.get("role"), "member") or "member",
                        passkey_hash=_safe_str(acc.get("passkey_hash", acc.get("passkeyHash")), ""),
                        passkey_salt=_safe_str(acc.get("passkey_salt", acc.get("passkeySalt")), ""),
                        passkey_hint=_safe_str(acc.get("passkey_hint", acc.get("passkeyHint")), ""),
                        created_at=_safe_float(acc.get("created_at", acc.get("createdAt")), 0.0),
                        last_active_at=_safe_float(acc.get("last_active_at", acc.get("lastActiveAt")), 0.0),
                        host_count=_safe_int(acc.get("host_count", acc.get("hostCount")), 0),
                    )
                )
        return ClusterConfig(
            cluster_id=_safe_str(data.get("clusterId", data.get("cluster_id")), str(uuid.uuid4())) or str(uuid.uuid4()),
            server_name=_safe_str(data.get("serverName", data.get("server_name")), "Shared Minecraft Server") or "Shared Minecraft Server",
            mc_software=_safe_str(data.get("mcSoftware", data.get("mc_software")), "paper") or "paper",
            mc_version=_safe_str(data.get("mcVersion", data.get("mc_version")), "26.3") or "26.3",
            memory_min_mb=_safe_int(data.get("memoryMinMb", data.get("memory_min_mb")), 2048),
            memory_max_mb=_safe_int(data.get("memoryMaxMb", data.get("memory_max_mb")), 4096),
            java_path=_safe_str(data.get("javaPath", data.get("java_path")), "java") or "java",
            jvm_flags=_safe_str(data.get("jvmFlags", data.get("jvm_flags")), "-XX:+UseG1GC"),
            server_port=_safe_int(data.get("serverPort", data.get("server_port")), 25565),
            tunnel_enabled=_safe_bool(data.get("tunnelEnabled", data.get("tunnel_enabled")), True),
            tunnel_provider=_safe_str(data.get("tunnelProvider", data.get("tunnel_provider")), "playit") or "playit",
            public_join_address=_safe_str(data.get("publicJoinAddress", data.get("public_join_address")), ""),
            git_sync_enabled=_safe_bool(data.get("gitSyncEnabled", data.get("git_sync_enabled")), False),
            auto_backup_on_stop=_safe_bool(data.get("autoBackupOnStop", data.get("auto_backup_on_stop")), True),
            max_backups=max(1, _safe_int(data.get("maxBackups", data.get("max_backups")), 10)),
            idle_shutdown_minutes=max(0, _safe_int(data.get("idleShutdownMinutes", data.get("idle_shutdown_minutes")), 0)),
            gamerules=dict(data.get("gamerules", {})) if isinstance(data.get("gamerules"), dict) else {},
            created_at=_safe_float(data.get("createdAt", data.get("created_at")), time.time()),
            accounts=accounts,
        )


@dataclass
class LocalMachineConfig:
    machine_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    machine_name: str = field(default_factory=socket.gethostname)
    shared_sync_dir: str = ""
    java_override_path: str = ""
    dashboard_port: int = 3080

    def to_dict(self) -> Dict[str, Any]:
        return {
            "machineId": self.machine_id,
            "machineName": self.machine_name,
            "sharedSyncDir": self.shared_sync_dir,
            "javaOverridePath": self.java_override_path,
            "dashboardPort": self.dashboard_port,
        }

    @staticmethod
    def from_dict(data: Dict[str, Any], default_sync_dir: str) -> "LocalMachineConfig":
        if not isinstance(data, dict):
            return LocalMachineConfig(shared_sync_dir=default_sync_dir)
        return LocalMachineConfig(
            machine_id=_safe_str(data.get("machineId"), str(uuid.uuid4())) or str(uuid.uuid4()),
            machine_name=_safe_str(data.get("machineName"), socket.gethostname()) or socket.gethostname(),
            shared_sync_dir=_safe_str(data.get("sharedSyncDir"), default_sync_dir) or default_sync_dir,
            java_override_path=_safe_str(data.get("javaOverridePath"), ""),
            dashboard_port=_safe_int(data.get("dashboardPort"), 3080),
        )
