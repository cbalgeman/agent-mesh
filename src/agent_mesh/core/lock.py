"""Owner-aware write lock for the agent-mesh pipeline."""
from __future__ import annotations

import errno
import os
import shutil
import socket
import subprocess
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal

try:
    import fcntl
except ImportError:  # pragma: no cover - POSIX CI covers the lock hardening path.
    fcntl = None  # type: ignore[assignment]

OwnerStatus = Literal["live", "stale_pid_dead", "stale_age_exceeded", "cannot_verify"]

STALE_LOCK_SECONDS = 600


def _read_boot_id() -> str:
    """Return a system identifier that changes at every boot.

    Used to detect PID reuse across reboots: if our recorded boot_id differs from
    the current boot_id, any same-host PID liveness check is unreliable and the
    lock should be treated as stale (boot wiped the prior owner regardless of pid).

    Linux: /proc/sys/kernel/random/boot_id
    macOS: sysctl kern.boottime epoch (best stdlib-only approximation)
    Other: empty string (boot_id check is best-effort)
    """
    proc_path = Path("/proc/sys/kernel/random/boot_id")
    try:
        return proc_path.read_text(encoding="utf-8").strip()
    except OSError:
        pass
    try:
        result = subprocess.run(
            ["sysctl", "-n", "kern.boottime"],
            capture_output=True,
            text=True,
            timeout=2,
            check=False,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except (FileNotFoundError, subprocess.SubprocessError):
        pass
    return ""


class MeshLockError(RuntimeError):
    """Base exception for mail lock failures."""


class MeshLockTimeout(MeshLockError):
    """Raised when a live or unverifiable lock outlives the retry budget."""


@dataclass(frozen=True)
class LockHandle:
    """Acquired lock metadata."""

    lock_dir: Path
    queue_depth: int
    lock_file_fd: int | None = None

    def release(self, last_event_seq: int | None = None) -> None:
        release(self.lock_dir, last_event_seq=last_event_seq, lock_file_fd=self.lock_file_fd)


@dataclass(frozen=True)
class LockOwner:
    """Parsed owner.txt fields."""

    pid: int | None
    hostname: str | None
    start_utc: str | None
    last_event_seq: str | None
    boot_id: str | None


def acquire(lock_dir: str | Path, retries: int = 200, retry_ms: int = 50) -> LockHandle:
    """Acquire the repo-local mail lock using atomic mkdir."""
    path = Path(lock_dir)
    queue_depth = 0

    for _ in range(retries + 1):
        lock_file_fd = _try_acquire_lock_file(path)
        if lock_file_fd is None:
            queue_depth += 1
            time.sleep(retry_ms / 1000)
            continue

        try:
            path.mkdir()
        except FileExistsError:
            queue_depth += 1
            owner_status = check_owner(path)
            if owner_status in {"stale_pid_dead", "stale_age_exceeded"}:
                force_unlock(path, operator_action=True)
                _release_lock_file(lock_file_fd)
                continue
            _release_lock_file(lock_file_fd)
            time.sleep(retry_ms / 1000)
            continue

        try:
            _write_owner(path, last_event_seq=None)
            _fsync_dir(path.parent)
        except BaseException:
            force_unlock(path, operator_action=True)
            _release_lock_file(lock_file_fd)
            raise
        return LockHandle(lock_dir=path, queue_depth=queue_depth, lock_file_fd=lock_file_fd)

    raise MeshLockTimeout(f"could not acquire live mail lock: {path}")


def release(
    lock_dir: str | Path,
    last_event_seq: int | None = None,
    lock_file_fd: int | None = None,
) -> None:
    """Release the lock after recording the latest event sequence for diagnostics."""
    path = Path(lock_dir)
    try:
        if not path.exists():
            return
        _write_owner(path, last_event_seq=last_event_seq)
        owner_path = path / "owner.txt"
        if owner_path.exists():
            owner_path.unlink()
        _fsync_dir(path)
        path.rmdir()
        _fsync_dir(path.parent)
    finally:
        _release_lock_file(lock_file_fd)


def check_owner(lock_dir: str | Path) -> OwnerStatus:
    """Return the lock owner status using the stale-lock rules.

    Detect PID reuse across reboots via boot_id. If the recorded boot_id differs
    from the current boot_id, the prior process cannot exist, so treat it as
    stale_pid_dead regardless of what os.kill reports.
    """
    path = Path(lock_dir)
    owner = _read_owner(path)
    local_hostname = socket.gethostname()
    current_boot_id = _read_boot_id()

    if owner.hostname == local_hostname and owner.pid is not None:
        # Boot-id check first: if known and changed, the lock is provably stale.
        if owner.boot_id and current_boot_id and owner.boot_id != current_boot_id:
            return "stale_pid_dead"
        return "live" if _pid_is_alive(owner.pid) else "stale_pid_dead"

    age = time.time() - _mtime(path)
    if age > STALE_LOCK_SECONDS:
        return "stale_age_exceeded"
    return "cannot_verify"


def force_unlock(lock_dir: str | Path, operator_action: bool = True) -> None:
    """Remove an existing lock directory.

    `operator_action` is retained in the public API because different-host recent locks require
    an explicit operator path. Internal stale releases pass through the same primitive.
    """
    if not operator_action:
        status = check_owner(lock_dir)
        if status == "live" or status == "cannot_verify":
            raise MeshLockError(f"refusing to force-unlock {status} lock: {lock_dir}")

    path = Path(lock_dir)
    if not path.exists():
        return
    shutil.rmtree(path)
    _fsync_dir(path.parent)


def _write_owner(lock_dir: Path, last_event_seq: int | None) -> None:
    lock_dir.mkdir(exist_ok=True)
    owner_path = lock_dir / "owner.txt"
    lines = [
        f"pid={os.getpid()}",
        f"hostname={socket.gethostname()}",
        f"start_utc={datetime.now(UTC).replace(microsecond=0).isoformat().replace('+00:00', 'Z')}",
        f"last_event_seq={'' if last_event_seq is None else last_event_seq}",
        f"boot_id={_read_boot_id()}",
    ]
    with owner_path.open("w", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def _read_owner(lock_dir: Path) -> LockOwner:
    fields: dict[str, str] = {}
    try:
        text = (lock_dir / "owner.txt").read_text(encoding="utf-8")
    except OSError:
        return LockOwner(pid=None, hostname=None, start_utc=None, last_event_seq=None, boot_id=None)

    for line in text.splitlines():
        key, sep, value = line.partition("=")
        if sep:
            fields[key] = value

    pid: int | None
    try:
        pid = int(fields["pid"])
    except (KeyError, ValueError):
        pid = None

    return LockOwner(
        pid=pid,
        hostname=fields.get("hostname"),
        start_utc=fields.get("start_utc"),
        last_event_seq=fields.get("last_event_seq"),
        boot_id=fields.get("boot_id"),
    )


def _pid_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError as exc:
        if exc.errno == errno.ESRCH:
            return False
        if exc.errno == errno.EPERM:
            return True
        raise
    return True


def _try_acquire_lock_file(lock_dir: Path) -> int | None:
    if fcntl is None:
        return -1
    lock_file = lock_dir.parent / f"{lock_dir.name}.fd"
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(lock_file, os.O_RDWR | os.O_CREAT, 0o666)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        return None
    except BaseException:
        os.close(fd)
        raise
    return fd


def _release_lock_file(lock_file_fd: int | None) -> None:
    if lock_file_fd is None or lock_file_fd < 0:
        return
    try:
        if fcntl is not None:
            fcntl.flock(lock_file_fd, fcntl.LOCK_UN)
    finally:
        os.close(lock_file_fd)


def _mtime(path: Path) -> float:
    try:
        return path.stat().st_mtime
    except OSError:
        return 0


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
