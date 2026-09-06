"""Install and manage the Workbench as a native per-user service."""
from __future__ import annotations

import json
import ipaddress
import os
import plistlib
import re
import secrets
import stat
import subprocess
import sys
import time
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Callable, Iterator, Literal, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from xml.sax.saxutils import escape as xml_escape

from agent_mesh.project_registry import registry_dir


SERVICE_LABEL = "dev.agent-mesh.workbench"
LAUNCHD_SOCKET_NAME = "workbench"
SYSTEMD_UNIT = "agent-mesh-workbench.service"
WINDOWS_TASK = "Agent Mesh Workbench"
SERVICE_SCHEMA_VERSION = 1
OWNERSHIP_SCHEMA_VERSION = 1
OWNERSHIP_MODES = {"absent", "activating", "configured", "relinquished"}
SUPPORTED_PLATFORMS = {"darwin", "linux", "win32"}
MAX_MANAGED_HEALTH_RESPONSE_BYTES = 16 * 1024
MAX_MANAGED_BOOKMARK_BYTES = 2 * 1024 * 1024
MAX_SERVICE_METADATA_BYTES = 64 * 1024
MAX_OWNERSHIP_RECORD_BYTES = 64 * 1024
MAX_OWNERSHIP_LEASE_BYTES = 16 * 1024
MAX_OWNERSHIP_LEASES = 128
MAX_INVOCATION_ENVELOPE_BYTES = 8 * 1024
MAX_REGISTERED_POINTERS = 512
REGISTERED_POINTER_DEADLINE_SECONDS = 5.0
OWNERSHIP_DRAIN_SECONDS = 10.0
SERVICE_COMMAND_TIMEOUT_SECONDS = 15.0
WORKBENCH_INVOCATION_ENV = "AGENT_MESH_WORKBENCH_INVOCATION"
_GENERATION_RE = re.compile(r"^[A-Za-z0-9_-]{32,128}$")
_OPEN_SUPPORTS_DIR_FD = os.open in os.supports_dir_fd
_STAT_SUPPORTS_DIR_FD = os.stat in os.supports_dir_fd
_UNLINK_SUPPORTS_DIR_FD = os.unlink in os.supports_dir_fd
_MKDIR_SUPPORTS_DIR_FD = os.mkdir in os.supports_dir_fd

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows is intentionally fail-closed below.
    fcntl = None  # type: ignore[assignment]

OwnershipMode = Literal["absent", "activating", "configured", "relinquished"]


class WorkbenchServiceError(RuntimeError):
    """Raised when the native Workbench service cannot be managed safely."""


class WorkbenchOwnershipError(WorkbenchServiceError):
    """Raised when Workbench ownership cannot be established safely."""


class WorkbenchOwnershipBusy(WorkbenchOwnershipError):
    """Raised when a bounded ownership transition cannot drain active work."""


class WorkbenchDispatchRecoveryRequired(WorkbenchOwnershipError):
    """Raised when relinquishment requires separate D014 recovery."""


class WorkbenchOwnershipPlatformUnsupported(WorkbenchOwnershipError):
    """Raised when the platform cannot provide D015's private-path boundary."""


@dataclass
class _PrivateDirectoryTransaction:
    """Descriptor cache anchoring one locked authority-directory transaction."""

    root: Path
    root_fd: int
    lock_anchor: Path
    lock_anchor_fd: int
    directory_fds: dict[tuple[str, ...], int]

    def close(self) -> None:
        for parts, descriptor in reversed(tuple(self.directory_fds.items())):
            if parts:
                os.close(descriptor)


_PRIVATE_DIRECTORY_TRANSACTION: ContextVar[_PrivateDirectoryTransaction | None] = ContextVar(
    "agent_mesh_private_directory_transaction",
    default=None,
)


@dataclass(frozen=True)
class WorkbenchServiceSpec:
    """The stable launch contract persisted in the native service definition."""

    repo: Path
    host: str
    port: int
    python_executable: Path
    config_home: Path
    ownership_generation: str = ""
    ownership_revision: int = 0

    @property
    def command(self) -> tuple[str, ...]:
        command = [
            str(self.python_executable),
            "-m",
            "agent_mesh.workbench_runner",
            "--repo",
            str(self.repo),
            "--host",
            self.host,
            "--port",
            str(self.port),
            "--managed-service",
            "--config-home",
            str(self.config_home),
        ]
        if self.ownership_generation:
            if self.ownership_revision < 1:
                raise WorkbenchServiceError(
                    "managed Workbench command requires an ownership revision"
                )
            command.extend(("--ownership-generation", self.ownership_generation))
            command.extend(("--ownership-revision", str(self.ownership_revision)))
        return tuple(command)

    def as_dict(self, *, platform_name: str, definition: Path) -> dict[str, Any]:
        package_root, source_checkout = installed_package_identity()
        return {
            "schema_version": SERVICE_SCHEMA_VERSION,
            "platform": platform_name,
            "repo": str(self.repo),
            "package_root": str(package_root),
            "source_checkout": str(source_checkout) if source_checkout is not None else "",
            "host": self.host,
            "port": self.port,
            "python_executable": str(self.python_executable),
            "config_home": str(self.config_home),
            "ownership_generation": self.ownership_generation,
            "ownership_revision": self.ownership_revision,
            "definition": str(definition),
        }


@dataclass(frozen=True)
class WorkbenchServiceStatus:
    platform: str
    installed: bool
    running: bool | None
    state: str
    definition: Path
    metadata: dict[str, Any] | None = None
    api_state: str | None = None
    ownership: ManagedWorkbenchAuthority | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "platform": self.platform,
            "installed": self.installed,
            "running": self.running,
            "state": self.state,
            "definition": str(self.definition),
            "metadata": self.metadata,
            "api_state": self.api_state,
            "ownership": self.ownership.as_public_dict() if self.ownership is not None else None,
        }


@dataclass(frozen=True)
class ManagedWorkbenchAuthority:
    """Private-bookmark-backed authority observed for the managed Workbench."""

    state: str
    mode: str
    verified: bool
    api_base: str
    bookmark_path: Path
    identity: dict[str, str | int]
    generation: str = ""
    ownership_revision: int = 0
    manual_allowed: bool = False

    def as_public_dict(self) -> dict[str, Any]:
        """Return user-facing ownership evidence without the bookmark token."""

        bookmark_path = "" if self.bookmark_path == Path() else str(self.bookmark_path)
        bookmark_url = ""
        if bookmark_path and self.bookmark_path.is_absolute():
            try:
                bookmark_url = self.bookmark_path.as_uri()
            except ValueError:
                bookmark_url = ""
        return {
            "state": self.state,
            "mode": self.mode,
            "verified": self.verified,
            "api_base": self.api_base,
            "bookmark_path": bookmark_path,
            "bookmark_url": bookmark_url,
            "identity": dict(self.identity),
            "generation": self.generation,
            "ownership_revision": self.ownership_revision,
            "manual_allowed": self.manual_allowed,
        }


@dataclass(frozen=True)
class WorkbenchOwnershipRecord:
    """One persisted D015 ownership state."""

    mode: OwnershipMode
    generation: str
    ownership_revision: int
    endpoint: str = ""
    bookmark_path: str = ""
    anchor_repository: str = ""
    package_root: str = ""
    source_checkout: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": OWNERSHIP_SCHEMA_VERSION,
            "mode": self.mode,
            "generation": self.generation,
            "ownership_revision": self.ownership_revision,
            "endpoint": self.endpoint,
            "bookmark_path": self.bookmark_path,
            "anchor_repository": self.anchor_repository,
            "package_root": self.package_root,
            "source_checkout": self.source_checkout,
        }


@dataclass(frozen=True)
class _WorkbenchQuiesceClaim:
    """Nonce-bound ownership-transition claim left visible while work drains."""

    operation: str
    generation: str
    ownership_revision: int
    transition_id: str
    pid: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": OWNERSHIP_SCHEMA_VERSION,
            "operation": self.operation,
            "generation": self.generation,
            "ownership_revision": self.ownership_revision,
            "transition_id": self.transition_id,
            "pid": self.pid,
        }


@dataclass(frozen=True)
class WorkbenchOwnershipBinding:
    """Generation identity bound to one current Workbench server."""

    server_mode: Literal["managed", "manual"]
    generation: str
    ownership_revision: int


@dataclass
class WorkbenchOwnershipLease:
    """Cross-process request admission retained through response flush."""

    authority_root: Path
    lease_path: Path
    binding: WorkbenchOwnershipBinding
    operation: str
    _released: bool = False

    def is_current(self) -> bool:
        if self._released:
            return False
        try:
            with _ownership_lock(self.authority_root):
                record = _require_ownership_record(self.authority_root)
                return (
                    record.generation == self.binding.generation
                    and record.ownership_revision == self.binding.ownership_revision
                    and _mode_allows_server(record.mode, self.binding.server_mode)
                )
        except WorkbenchOwnershipError:
            return False

    def release(self) -> None:
        if self._released:
            return
        self._released = True
        try:
            with _ownership_lock(self.authority_root):
                _unlink_private_entry(
                    _ownership_leases_dir(self.authority_root),
                    self.lease_path.name,
                )
        except (OSError, WorkbenchOwnershipError):
            # A failed cleanup remains fail-closed for the next transition.
            return

    def __enter__(self) -> WorkbenchOwnershipLease:
        return self

    def __exit__(self, *_args: object) -> None:
        self.release()


@dataclass
class WorkbenchInvocation:
    """Long-lived Workbench subprocess marker plus its bounded child envelope."""

    authority_root: Path
    marker_path: Path
    envelope: str
    _released: bool = False

    def bind_dispatch_run(self, binding: WorkbenchOwnershipBinding, run_id: str) -> None:
        """Bind a known run before a multi-append dispatch child starts."""

        try:
            envelope = json.loads(self.envelope)
        except json.JSONDecodeError as exc:  # pragma: no cover - minted above
            raise WorkbenchOwnershipError("Workbench invocation envelope is invalid") from exc
        if not isinstance(envelope, dict):  # pragma: no cover - minted above
            raise WorkbenchOwnershipError("Workbench invocation envelope is invalid")
        _update_dispatch_invocation_marker(
            root=self.authority_root,
            marker=self.marker_path,
            envelope=envelope,
            binding=binding,
            run_id=run_id,
            terminal=False,
        )

    def release(self, *, child_completed: bool = False) -> None:
        if self._released:
            return
        self._released = True
        marker_snapshot: dict[str, Any] | None = None
        try:
            with _ownership_lock(self.authority_root):
                marker = _bounded_json_object(
                    self.marker_path,
                    MAX_INVOCATION_ENVELOPE_BYTES,
                )
                if marker is None:
                    return
                operation = str(marker.get("operation", ""))
                active_run_id = marker.get("active_run_id")
                if (
                    operation.startswith("dispatch")
                    and isinstance(active_run_id, str)
                    and active_run_id
                ):
                    if marker.get("child_started") is not True:
                        _unlink_private_entry(
                            _ownership_invocations_dir(self.authority_root),
                            self.marker_path.name,
                        )
                        return
                    if not child_completed:
                        _mark_invocation_interrupted(
                            self.marker_path,
                            marker,
                            reason="child_failed",
                        )
                        return
                    marker_snapshot = marker
                else:
                    _unlink_private_entry(
                        _ownership_invocations_dir(self.authority_root),
                        self.marker_path.name,
                    )
                    return
            if marker_snapshot is None:
                return
            complete = _dispatch_marker_canonical_complete(marker_snapshot)
            with _ownership_lock(self.authority_root):
                current_marker = _bounded_json_object(
                    self.marker_path,
                    MAX_INVOCATION_ENVELOPE_BYTES,
                )
                if current_marker != marker_snapshot:
                    return
                if not complete:
                    _mark_invocation_interrupted(
                        self.marker_path,
                        current_marker,
                        reason="canonical_suffix_incomplete",
                    )
                    return
                _unlink_private_entry(
                    _ownership_invocations_dir(self.authority_root),
                    self.marker_path.name,
                )
        except (OSError, WorkbenchOwnershipError):
            return

    def __enter__(self) -> WorkbenchInvocation:
        return self

    def __exit__(self, *_args: object) -> None:
        self.release()


@dataclass
class WorkbenchDispatchRecoveryLease:
    """Transition-locked authority to retire one recovered dispatch marker."""

    authority_root: Path
    marker_path: Path
    marker: dict[str, Any]
    config: Any
    run_id: str
    _completed: bool = False

    def complete(self) -> None:
        from agent_mesh.dispatch.execution import verified_dispatch_run_recovery_complete

        if self._completed:
            return
        if not verified_dispatch_run_recovery_complete(
            self.config,
            self.run_id,
        ):
            raise WorkbenchOwnershipError(
                "canonical dispatch recovery is incomplete; marker was retained"
            )
        with _ownership_lock(self.authority_root):
            current = _require_ownership_record(self.authority_root)
            marker = _bounded_json_object(
                self.marker_path,
                MAX_INVOCATION_ENVELOPE_BYTES,
            )
            if (
                marker != self.marker
                or current.generation != self.marker.get("generation")
                or current.ownership_revision != self.marker.get("ownership_revision")
            ):
                raise WorkbenchOwnershipError(
                    "Workbench dispatch marker changed during canonical recovery"
                )
            _unlink_private_entry(
                _ownership_invocations_dir(self.authority_root),
                self.marker_path.name,
            )
        self._completed = True


def current_platform(platform_name: str | None = None) -> str:
    value = platform_name or sys.platform
    if value not in SUPPORTED_PLATFORMS:
        raise WorkbenchServiceError(
            f"automatic Workbench service is not supported on platform {value!r}; "
            "supported platforms are macOS, Linux with systemd, and Windows"
        )
    return value


def _require_d015_activation_platform(platform_name: str) -> None:
    if platform_name == "win32":
        raise WorkbenchOwnershipPlatformUnsupported(
            "exclusive Workbench ownership activation is initially supported on macOS and "
            "Linux only; Windows activation is deferred"
        )


def _os_account_home(platform_name: str) -> Path:
    """Resolve the OS-account home without mutable process environment input."""

    if platform_name not in {"darwin", "linux"}:
        raise WorkbenchOwnershipPlatformUnsupported(
            "stable OS-account home resolution is currently supported on POSIX only"
        )
    try:
        import pwd

        account_home = pwd.getpwuid(os.getuid()).pw_dir
    except (ImportError, KeyError, OSError) as exc:
        raise WorkbenchOwnershipError(
            "could not resolve the OS-account home for Workbench authority"
        ) from exc
    if not account_home or not Path(account_home).is_absolute():
        raise WorkbenchOwnershipError(
            "the OS-account home for Workbench authority is invalid"
        )
    return Path(account_home)


def _stable_registry_dir(
    platform_name: str | None = None,
    *,
    home: Path | None = None,
) -> Path:
    """Return the legacy/default registry location without HOME/XDG overrides."""

    platform_value = current_platform(platform_name)
    if platform_value in {"darwin", "linux"}:
        user_home = home.expanduser().absolute() if home is not None else _os_account_home(platform_value)
        return user_home / ".config" / "agent-mesh"
    root = workbench_authority_root(platform_value, home=home)
    return root.parent


def workbench_authority_root(
    platform_name: str | None = None,
    *,
    home: Path | None = None,
) -> Path:
    """Return the deterministic per-user D015 authority root.

    Production configuration overrides intentionally do not participate: an
    alternate Agent Mesh registry or virtual environment must not silently
    create a second ownership namespace for the same OS user.
    """

    platform_value = current_platform(platform_name)
    if home is not None:
        user_home = home.expanduser().absolute()
    elif platform_value in {"darwin", "linux"}:
        user_home = _os_account_home(platform_value)
    else:
        if sys.platform != "win32":
            raise WorkbenchOwnershipError(
                "Windows Workbench authority requires the Windows Known Folder API"
            )
        try:
            import ctypes

            buffer = ctypes.create_unicode_buffer(32768)
            # CSIDL_LOCAL_APPDATA is resolved for the process token and does not
            # consult HOME, USERPROFILE, or Agent Mesh configuration overrides.
            result = ctypes.windll.shell32.SHGetFolderPathW(None, 28, None, 0, buffer)
        except (AttributeError, OSError) as exc:
            raise WorkbenchOwnershipError(
                "could not resolve the Windows per-user local application folder"
            ) from exc
        if result != 0 or not buffer.value:
            raise WorkbenchOwnershipError(
                "could not resolve the Windows per-user local application folder"
            )
        return Path(buffer.value) / "Agent Mesh" / "workbench"
    if platform_value == "darwin":
        return user_home / "Library" / "Application Support" / "Agent Mesh" / "workbench"
    if platform_value == "linux":
        return user_home / ".local" / "state" / "agent-mesh" / "workbench"
    return user_home / "AppData" / "Local" / "Agent Mesh" / "workbench"


def installed_package_identity(module_file: Path | None = None) -> tuple[Path, Path | None]:
    """Return an always-true package root and an optional proven source checkout."""

    package_root = (module_file or Path(__file__)).resolve().parent
    candidate = package_root.parents[1]
    source_package = candidate / "src" / "agent_mesh"
    try:
        source_checkout = candidate if source_package.resolve() == package_root else None
    except OSError:
        source_checkout = None
    return package_root, source_checkout


def _ownership_record_path(root: Path) -> Path:
    return root / "ownership.json"


def _ownership_marker_path(root: Path) -> Path:
    return root / "activated.json"


def _ownership_marker_backup_path(root: Path) -> Path:
    return root / "activated.backup.json"


def _ownership_lock_path(root: Path) -> Path:
    return root / ".ownership-lock"


def _transition_lock_path(root: Path) -> Path:
    return root / ".transition-lock"


def _ownership_leases_dir(root: Path) -> Path:
    return root / "leases"


def _ownership_invocations_dir(root: Path) -> Path:
    return root / "invocations"


def _quiesce_path(root: Path) -> Path:
    return root / "quiesce.json"


def _ensure_authority_root(root: Path) -> None:
    try:
        with _open_private_directory(root, create=True) as root_fd:
            if hasattr(os, "fchmod"):
                os.fchmod(root_fd, 0o700)
    except (OSError, WorkbenchOwnershipError) as exc:
        raise WorkbenchOwnershipError(
            f"could not initialize Workbench authority root; symlinks and invalid "
            f"components are rejected: {exc}"
        ) from exc


def _require_private_path_primitives() -> None:
    """Fail closed unless descriptor-relative no-follow operations are available."""

    required = (
        os.name == "posix",
        hasattr(os, "O_DIRECTORY"),
        hasattr(os, "O_NOFOLLOW"),
        _OPEN_SUPPORTS_DIR_FD,
        _STAT_SUPPORTS_DIR_FD,
        _UNLINK_SUPPORTS_DIR_FD,
        _MKDIR_SUPPORTS_DIR_FD,
    )
    if not all(required):
        raise WorkbenchOwnershipPlatformUnsupported(
            "exclusive Workbench ownership requires POSIX descriptor-relative no-follow "
            "filesystem operations; Windows activation is deferred"
        )


def _private_path_parts(path: Path) -> tuple[Path, tuple[str, ...]]:
    _require_private_path_primitives()
    absolute = path.expanduser().absolute()
    if not absolute.is_absolute() or absolute.anchor != os.sep:
        raise WorkbenchOwnershipError(f"Workbench private path must be absolute: {path}")
    parts = absolute.parts[1:]
    if any(part in {"", ".", ".."} or Path(part).name != part for part in parts):
        raise WorkbenchOwnershipError(f"Workbench private path is invalid: {path}")
    return absolute, parts


@contextmanager
def _open_private_directory_from_path(path: Path, *, create: bool = False) -> Iterator[int]:
    """Walk one private directory path without consulting an active transaction."""

    absolute, parts = _private_path_parts(path)
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    current_fd = os.open(absolute.anchor, flags)
    try:
        for part in parts:
            try:
                next_fd = os.open(part, flags, dir_fd=current_fd)
            except FileNotFoundError:
                if not create:
                    raise
                os.mkdir(part, 0o700, dir_fd=current_fd)
                os.fsync(current_fd)
                next_fd = os.open(part, flags, dir_fd=current_fd)
            info = os.fstat(next_fd)
            if not stat.S_ISDIR(info.st_mode):
                os.close(next_fd)
                raise WorkbenchOwnershipError(
                    f"Workbench private path component is not a directory: {part}"
                )
            os.close(current_fd)
            current_fd = next_fd
        yield current_fd
    finally:
        os.close(current_fd)


def _transaction_directory_fd(
    transaction: _PrivateDirectoryTransaction,
    parts: tuple[str, ...],
    *,
    create: bool,
) -> int:
    """Open and pin one descendant beneath a locked authority descriptor."""

    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    for index, part in enumerate(parts, start=1):
        prefix = parts[:index]
        existing = transaction.directory_fds.get(prefix)
        if existing is not None:
            continue
        parent_fd = transaction.directory_fds[prefix[:-1]]
        try:
            descriptor = os.open(part, flags, dir_fd=parent_fd)
        except FileNotFoundError:
            if not create:
                raise
            os.mkdir(part, 0o700, dir_fd=parent_fd)
            os.fsync(parent_fd)
            descriptor = os.open(part, flags, dir_fd=parent_fd)
        info = os.fstat(descriptor)
        if not stat.S_ISDIR(info.st_mode):
            os.close(descriptor)
            raise WorkbenchOwnershipError(
                f"Workbench private path component is not a directory: {part}"
            )
        transaction.directory_fds[prefix] = descriptor
    return transaction.directory_fds[parts]


@contextmanager
def _open_private_directory(path: Path, *, create: bool = False) -> Iterator[int]:
    """Open a no-follow directory, pinned to the active locked authority inode."""

    absolute, _ = _private_path_parts(path)
    transaction = _PRIVATE_DIRECTORY_TRANSACTION.get()
    if transaction is not None:
        try:
            relative = absolute.relative_to(transaction.root)
        except ValueError:
            relative = None
        if relative is not None:
            parts = tuple(relative.parts) if relative.parts != (".",) else ()
            descriptor = _transaction_directory_fd(transaction, parts, create=create)
            duplicate = os.dup(descriptor)
            try:
                yield duplicate
            finally:
                os.close(duplicate)
            return
    with _open_private_directory_from_path(absolute, create=create) as descriptor:
        yield descriptor


def _require_retained_directory_path_current(
    path: Path,
    descriptor: int,
    *,
    label: str,
) -> None:
    """Require one absolute directory path to still identify its retained inode."""

    try:
        with _open_private_directory_from_path(path) as current_fd:
            retained = os.fstat(descriptor)
            current = os.fstat(current_fd)
    except (OSError, WorkbenchOwnershipError) as exc:
        raise WorkbenchOwnershipError(
            f"Workbench {label} path changed during the locked transaction"
        ) from exc
    if retained.st_dev != current.st_dev or retained.st_ino != current.st_ino:
        raise WorkbenchOwnershipError(
            f"Workbench {label} path changed during the locked transaction"
        )


def _require_authority_path_current(root: Path, root_fd: int) -> None:
    """Reject success when the deterministic authority path changed mid-transaction."""

    _require_retained_directory_path_current(
        root,
        root_fd,
        label="authority",
    )


def _require_transaction_directories_current(
    transaction: _PrivateDirectoryTransaction,
) -> None:
    """Reject success if any pinned authority child was replaced while locked."""

    try:
        for parts, descriptor in transaction.directory_fds.items():
            if not parts:
                continue
            parent_fd = transaction.directory_fds[parts[:-1]]
            retained = os.fstat(descriptor)
            current = os.stat(parts[-1], dir_fd=parent_fd, follow_symlinks=False)
            if (
                not stat.S_ISDIR(current.st_mode)
                or retained.st_dev != current.st_dev
                or retained.st_ino != current.st_ino
            ):
                raise WorkbenchOwnershipError(
                    "Workbench authority directory changed during the locked transaction"
                )
    except OSError as exc:
        raise WorkbenchOwnershipError(
            "Workbench authority directory changed during the locked transaction"
        ) from exc


def _require_active_private_transaction_current() -> None:
    transaction = _PRIVATE_DIRECTORY_TRANSACTION.get()
    if transaction is None:
        return
    _require_stable_user_lock_ancestry(transaction.lock_anchor)
    _require_retained_directory_path_current(
        transaction.lock_anchor,
        transaction.lock_anchor_fd,
        label="OS-account lock anchor",
    )
    _require_transaction_directories_current(transaction)
    _require_authority_path_current(transaction.root, transaction.root_fd)


def _require_stable_user_lock_ancestry(home: Path) -> None:
    """Reject an anchor if this account can replace any containing directory."""

    current_uid = os.getuid()
    ancestor = home.parent
    while True:
        try:
            with _open_private_directory_from_path(ancestor) as descriptor:
                info = os.fstat(descriptor)
        except (OSError, WorkbenchOwnershipError) as exc:
            raise WorkbenchOwnershipPlatformUnsupported(
                "Workbench ownership lock ancestry is unavailable"
            ) from exc
        if info.st_uid == current_uid or os.access(ancestor, os.W_OK):
            raise WorkbenchOwnershipPlatformUnsupported(
                "Workbench ownership requires an OS-account home whose complete parent "
                "ancestry is not owned or writable by the current account"
            )
        parent = ancestor.parent
        if parent == ancestor:
            return
        ancestor = parent


@contextmanager
def _open_stable_user_lock_anchor() -> Iterator[tuple[Path, int]]:
    """Open the OS-account home inode whose parent the account cannot rename."""

    platform_name = current_platform()
    home = _os_account_home(platform_name).expanduser().absolute()
    try:
        _require_stable_user_lock_ancestry(home)
        with _open_private_directory_from_path(home) as descriptor:
            info = os.fstat(descriptor)
            if info.st_uid != os.getuid():
                raise WorkbenchOwnershipPlatformUnsupported(
                    "Workbench ownership lock anchor is not owned by the current OS account"
                )
            _require_retained_directory_path_current(
                home,
                descriptor,
                label="OS-account lock anchor",
            )
            yield home, descriptor
            _require_stable_user_lock_ancestry(home)
            _require_retained_directory_path_current(
                home,
                descriptor,
                label="OS-account lock anchor",
            )
    except WorkbenchOwnershipError:
        raise
    except OSError as exc:
        raise WorkbenchOwnershipPlatformUnsupported(
            f"Workbench ownership lock anchor is unavailable: {exc}"
        ) from exc


def _assert_no_symlink_components(path: Path) -> None:
    """Compatibility validator backed by the retained descriptor walk."""

    try:
        with _open_private_directory(path):
            return
    except FileNotFoundError:
        return
    except OSError as exc:
        raise WorkbenchOwnershipError(
            f"could not validate Workbench private path {path}: {exc}"
        ) from exc


@contextmanager
def _private_advisory_lock(root: Path, name: str) -> Iterator[int]:
    """Use one stable OS-account lock while retaining the authority-root fd."""

    if fcntl is None:
        raise WorkbenchOwnershipPlatformUnsupported(
            "exclusive Workbench ownership requires POSIX advisory locks"
        )
    if not name or Path(name).name != name:
        raise WorkbenchOwnershipError("Workbench private lock name is invalid")
    absolute_root = root.expanduser().absolute()
    active = _PRIVATE_DIRECTORY_TRANSACTION.get()
    if active is not None:
        if active.root != absolute_root:
            raise WorkbenchOwnershipError(
                "nested Workbench ownership transactions must use one authority root"
            )
        yield active.root_fd
        return
    with _open_stable_user_lock_anchor() as (lock_anchor, lock_fd):
        try:
            for attempt in range(201):
                try:
                    fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if attempt == 200:
                        raise WorkbenchOwnershipBusy(
                            "Workbench ownership lock exceeded its time bound"
                        )
                    time.sleep(0.05)
            try:
                _ensure_authority_root(absolute_root)
                with _open_private_directory(absolute_root) as root_fd:
                    transaction = _PrivateDirectoryTransaction(
                        root=absolute_root,
                        root_fd=root_fd,
                        lock_anchor=lock_anchor,
                        lock_anchor_fd=lock_fd,
                        directory_fds={(): root_fd},
                    )
                    token = _PRIVATE_DIRECTORY_TRANSACTION.set(transaction)
                    try:
                        yield root_fd
                        _require_active_private_transaction_current()
                    finally:
                        _PRIVATE_DIRECTORY_TRANSACTION.reset(token)
                        transaction.close()
            finally:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
        except OSError as exc:
            raise WorkbenchOwnershipError(
                f"could not lock Workbench OS-account anchor: {exc}"
            ) from exc


@contextmanager
def _ownership_lock(root: Path) -> Iterator[int]:
    with _private_advisory_lock(root, ".ownership-lock.fd") as root_fd:
        yield root_fd


@contextmanager
def _transition_lock(
    root: Path,
    *,
    quiesce_claim: _WorkbenchQuiesceClaim | None = None,
) -> Iterator[int]:
    """Serialize one complete native-service/ownership lifecycle operation."""

    with _private_advisory_lock(root, ".transition-lock.fd") as root_fd:
        if quiesce_claim is None:
            if _path_present(_quiesce_path(root)):
                raise WorkbenchOwnershipBusy(
                    "another Workbench ownership transition is active"
                )
        else:
            _require_quiesce_claim(root, quiesce_claim)
        yield root_fd


def _bounded_regular_file_at(
    parent_fd: int,
    name: str,
    maximum: int,
) -> tuple[str, bytes | None]:
    """Read one exact regular child through an already retained parent descriptor."""

    if not name or Path(name).name != name:
        return "invalid", None
    flags = (
        os.O_RDONLY
        | os.O_NOFOLLOW
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    try:
        before = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
            return "invalid", None
        fd = os.open(name, flags, dir_fd=parent_fd)
    except FileNotFoundError:
        return "missing", None
    except (OSError, WorkbenchOwnershipError):
        return "invalid", None
    try:
        opened = os.fstat(fd)
        if not stat.S_ISREG(opened.st_mode) or opened.st_size > maximum:
            return "invalid", None
        chunks: list[bytes] = []
        remaining = maximum + 1
        while remaining > 0:
            chunk = os.read(fd, min(remaining, 64 * 1024))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        after = os.fstat(fd)
    finally:
        os.close(fd)
    stable = (
        before.st_dev == opened.st_dev == after.st_dev
        and before.st_ino == opened.st_ino == after.st_ino
        and opened.st_size == after.st_size == len(payload)
        and getattr(opened, "st_mtime_ns", None) == getattr(after, "st_mtime_ns", None)
    )
    if not stable or len(payload) > maximum:
        return "invalid", None
    return "ok", payload


def _bounded_regular_file(path: Path, maximum: int) -> tuple[str, bytes | None]:
    """Read at most maximum bytes through a retained no-follow parent descriptor."""

    try:
        with _open_private_directory(path.parent) as parent_fd:
            return _bounded_regular_file_at(parent_fd, path.name, maximum)
    except FileNotFoundError:
        return "missing", None
    except (OSError, WorkbenchOwnershipError):
        return "invalid", None


def _path_present(path: Path) -> bool:
    try:
        with _open_private_directory(path.parent) as parent_fd:
            os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return False
    except (OSError, WorkbenchOwnershipError):
        return True
    return True


def _bounded_directory_entries(directory: Path, maximum: int, label: str) -> list[Path]:
    """Return at most ``maximum`` entries without materializing an unbounded directory."""

    entries: list[Path] = []
    try:
        with _open_private_directory(directory) as directory_fd:
            with os.scandir(directory_fd) as iterator:
                for entry in iterator:
                    entries.append(directory / entry.name)
                    if len(entries) > maximum:
                        raise WorkbenchOwnershipBusy(
                            f"Workbench {label} inventory exceeds its bound"
                        )
    except WorkbenchOwnershipBusy:
        raise
    except (OSError, WorkbenchOwnershipError) as exc:
        raise WorkbenchOwnershipBusy(
            f"could not inspect Workbench {label}: {exc}"
        ) from exc
    return entries


def _unlink_private_entry(directory: Path, name: str) -> None:
    """Unlink one exact child without following or re-resolving its directory."""

    _require_active_private_transaction_current()
    if not name or Path(name).name != name:
        raise WorkbenchOwnershipError("Workbench private entry name is invalid")
    try:
        with _open_private_directory(directory) as directory_fd:
            try:
                os.unlink(name, dir_fd=directory_fd)
            except FileNotFoundError:
                pass
    except OSError as exc:
        raise WorkbenchOwnershipError(
            f"could not remove Workbench private entry {name!r}: {exc}"
        ) from exc


def _unlink_private_regular_file(path: Path) -> None:
    """Validate and unlink one private file through the same retained parent fd."""

    _require_active_private_transaction_current()
    try:
        with _open_private_directory(path.parent) as parent_fd:
            state, _ = _bounded_regular_file_at(
                parent_fd,
                path.name,
                MAX_MANAGED_BOOKMARK_BYTES,
            )
            if state == "missing":
                return
            if state != "ok":
                raise WorkbenchOwnershipError(
                    f"Workbench private file is invalid and was not removed: {path}"
                )
            os.unlink(path.name, dir_fd=parent_fd)
            os.fsync(parent_fd)
    except FileNotFoundError:
        return
    except WorkbenchOwnershipError:
        raise
    except OSError as exc:
        raise WorkbenchOwnershipError(
            f"could not remove Workbench private file {path}: {exc}"
        ) from exc


def _parse_ownership_record(payload: bytes) -> WorkbenchOwnershipRecord:
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise WorkbenchOwnershipError("Workbench ownership record is invalid") from exc
    if not isinstance(value, dict) or value.get("schema_version") != OWNERSHIP_SCHEMA_VERSION:
        raise WorkbenchOwnershipError("Workbench ownership record schema is unsupported")
    mode = value.get("mode")
    generation = value.get("generation")
    revision = value.get("ownership_revision")
    if mode not in OWNERSHIP_MODES:
        raise WorkbenchOwnershipError("Workbench ownership mode is invalid")
    if not isinstance(generation, str) or _GENERATION_RE.fullmatch(generation) is None:
        raise WorkbenchOwnershipError("Workbench ownership generation is invalid")
    if isinstance(revision, bool) or not isinstance(revision, int) or not 1 <= revision <= 2**63 - 1:
        raise WorkbenchOwnershipError("Workbench ownership revision is invalid")
    strings: dict[str, str] = {}
    for key in (
        "endpoint",
        "bookmark_path",
        "anchor_repository",
        "package_root",
        "source_checkout",
    ):
        item = value.get(key, "")
        if not isinstance(item, str) or len(item.encode("utf-8")) > 4096:
            raise WorkbenchOwnershipError(f"Workbench ownership {key} is invalid")
        strings[key] = item
    if strings["endpoint"]:
        parsed = urlparse(strings["endpoint"])
        if parsed.scheme != "http" or parsed.username or parsed.password or not parsed.hostname:
            raise WorkbenchOwnershipError("Workbench ownership endpoint is invalid")
        try:
            _validate_loopback_host(parsed.hostname)
            if parsed.port is None:
                raise ValueError
        except (ValueError, WorkbenchServiceError) as exc:
            raise WorkbenchOwnershipError("Workbench ownership endpoint is invalid") from exc
    if strings["bookmark_path"] and not Path(strings["bookmark_path"]).is_absolute():
        raise WorkbenchOwnershipError("Workbench ownership bookmark path is invalid")
    return WorkbenchOwnershipRecord(
        mode=mode,
        generation=generation,
        ownership_revision=revision,
        **strings,
    )


def _read_ownership_record(root: Path) -> tuple[str, WorkbenchOwnershipRecord | None]:
    state, payload = _bounded_regular_file(_ownership_record_path(root), MAX_OWNERSHIP_RECORD_BYTES)
    if state != "ok" or payload is None:
        return state, None
    try:
        record = _parse_ownership_record(payload)
    except WorkbenchOwnershipError:
        return "invalid", None
    marker_state, high_water = _ownership_high_water_observation(root)
    if marker_state != "ok" or high_water != record.ownership_revision:
        return "invalid", None
    return "ok", record


def _require_ownership_record(root: Path) -> WorkbenchOwnershipRecord:
    state, record = _read_ownership_record(root)
    if state != "ok" or record is None:
        raise WorkbenchOwnershipError("Workbench ownership is invalid or unavailable")
    return record


def _write_ownership_record(root: Path, record: WorkbenchOwnershipRecord) -> None:
    _ensure_authority_root(root)
    existing_state, existing_payload = _bounded_regular_file(
        _ownership_record_path(root), MAX_OWNERSHIP_RECORD_BYTES
    )
    if existing_state == "ok" and existing_payload is not None:
        try:
            existing = _parse_ownership_record(existing_payload)
        except WorkbenchOwnershipError:
            existing = None
        if (
            existing is not None
            and record != existing
            and record.ownership_revision <= existing.ownership_revision
        ):
            raise WorkbenchOwnershipError("Workbench ownership revision must advance")
    marker = {
        "schema_version": OWNERSHIP_SCHEMA_VERSION,
        "activated": True,
        "ownership_revision": record.ownership_revision,
    }
    marker_state, high_water = _ownership_high_water_observation(root)
    if marker_state == "invalid":
        raise WorkbenchOwnershipError("Workbench ownership revision high-water is invalid")
    if high_water is not None and record.ownership_revision < high_water:
        raise WorkbenchOwnershipError("Workbench ownership revision would move backwards")
    marker_bytes = (json.dumps(marker, sort_keys=True) + "\n").encode("utf-8")
    _atomic_write(
        _ownership_marker_backup_path(root),
        marker_bytes,
        mode=0o600,
    )
    _atomic_write(
        _ownership_marker_path(root),
        marker_bytes,
        mode=0o600,
    )
    serialized = (json.dumps(record.as_dict(), indent=2, sort_keys=True) + "\n").encode("utf-8")
    if len(serialized) > MAX_OWNERSHIP_RECORD_BYTES:
        raise WorkbenchOwnershipError("Workbench ownership record exceeds its byte bound")
    _atomic_write(_ownership_record_path(root), serialized, mode=0o600)


def _parse_ownership_revision_high_water(payload: bytes | None) -> int | None:
    if payload is None:
        return None
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if (
        not isinstance(value, dict)
        or value.get("schema_version") != OWNERSHIP_SCHEMA_VERSION
        or value.get("activated") is not True
    ):
        return None
    revision = value.get("ownership_revision")
    if isinstance(revision, bool) or not isinstance(revision, int):
        return None
    return revision if 1 <= revision <= 2**63 - 1 else None


def _ownership_revision_high_water(root: Path) -> int | None:
    state, high_water = _ownership_high_water_observation(root)
    if state == "missing":
        return None
    if state != "ok" or high_water is None:
        raise WorkbenchOwnershipError("Workbench ownership revision high-water is invalid")
    return high_water


def _ownership_high_water_observation(root: Path) -> tuple[str, int | None]:
    """Read redundant high-water copies; one intact copy is recoverable evidence."""

    states: list[str] = []
    revisions: list[int] = []
    for path in (_ownership_marker_path(root), _ownership_marker_backup_path(root)):
        state, payload = _bounded_regular_file(path, MAX_OWNERSHIP_RECORD_BYTES)
        states.append(state)
        revision = _parse_ownership_revision_high_water(payload)
        if state == "ok" and revision is not None:
            revisions.append(revision)
    if revisions:
        return "ok", max(revisions)
    if all(state == "missing" for state in states):
        return "missing", None
    return "invalid", None


def _new_ownership_record(
    mode: OwnershipMode,
    *,
    revision: int = 1,
    generation: str | None = None,
    endpoint: str = "",
    bookmark_path: str = "",
    anchor_repository: str = "",
) -> WorkbenchOwnershipRecord:
    package_root, source_checkout = installed_package_identity()
    return WorkbenchOwnershipRecord(
        mode=mode,
        generation=generation or secrets.token_urlsafe(32),
        ownership_revision=revision,
        endpoint=endpoint,
        bookmark_path=bookmark_path,
        anchor_repository=anchor_repository,
        package_root=str(package_root),
        source_checkout=str(source_checkout) if source_checkout is not None else "",
    )


def _mode_allows_server(mode: str, server_mode: str) -> bool:
    if server_mode == "managed":
        return mode == "configured"
    return mode in {"absent", "relinquished"}


def initialize_manual_ownership(
    *,
    platform_name: str | None = None,
    authority_root: Path | None = None,
) -> ManagedWorkbenchAuthority:
    """Perform the one explicit legacy-to-absent transition for a manual launch."""

    platform_value = current_platform(platform_name)
    root = authority_root or workbench_authority_root(platform_value)
    with _ownership_lock(root):
        state, record = _read_ownership_record(root)
        if state == "ok" and record is not None:
            return _authority_from_record(record, state=record.mode)
        marker_state, _ = _ownership_high_water_observation(root)
        if state == "invalid" or marker_state != "missing":
            return _invalid_authority("invalid")
        stable_registry = _stable_registry_dir(platform_value)
        configured_registry = registry_dir().expanduser().absolute()
        legacy_paths = (
            service_definition_path(platform_value),
            service_metadata_path(),
            stable_registry / "workbench.html",
            configured_registry / "workbench-service.json",
            configured_registry / "workbench.html",
        )
        if any(_path_present(path) for path in legacy_paths):
            return _invalid_authority("legacy_uninitialized")
        if configured_registry != stable_registry:
            # An override can name another historical registry that this process
            # cannot prove is the only prior control plane. Explicit managed
            # install/migration is required instead of asserting global absence.
            return _invalid_authority("legacy_uninitialized")
        record = _new_ownership_record("absent")
        _write_ownership_record(root, record)
        return _authority_from_record(record, state="absent")


def _authority_from_record(
    record: WorkbenchOwnershipRecord,
    *,
    state: str,
    verified: bool = False,
    expose_bookmark: bool = False,
) -> ManagedWorkbenchAuthority:
    identity: dict[str, str | int] = {
        "mode": "managed" if record.mode in {"activating", "configured"} else "manual",
    }
    if record.anchor_repository:
        identity["anchor_repository"] = record.anchor_repository
    if record.package_root:
        identity["package_root"] = record.package_root
    if record.source_checkout:
        identity["source_checkout"] = record.source_checkout
    if record.endpoint:
        identity["endpoint"] = record.endpoint
        try:
            port = urlparse(record.endpoint).port
        except ValueError:
            port = None
        if port is not None:
            identity["port"] = port
    return ManagedWorkbenchAuthority(
        state=state,
        mode=record.mode,
        verified=verified,
        api_base=record.endpoint,
        bookmark_path=(
            Path(record.bookmark_path)
            if expose_bookmark and record.bookmark_path
            else Path()
        ),
        identity=identity,
        generation=record.generation,
        ownership_revision=record.ownership_revision,
        manual_allowed=record.mode in {"absent", "relinquished"},
    )


def _invalid_authority(state: str) -> ManagedWorkbenchAuthority:
    return ManagedWorkbenchAuthority(
        state=state,
        mode="",
        verified=False,
        api_base="",
        bookmark_path=Path(),
        identity={},
        manual_allowed=False,
    )


def begin_managed_activation(
    spec: WorkbenchServiceSpec,
    *,
    platform_name: str | None = None,
    authority_root: Path | None = None,
    repair: bool = False,
    direct_human_reactivation: bool = False,
) -> WorkbenchServiceSpec:
    """Linearize managed ownership before native endpoint admission."""

    platform_value = current_platform(platform_name)
    root = authority_root or workbench_authority_root(platform_value)
    with _ownership_lock(root):
        state, existing = _read_ownership_record(root)
        marker_state, _ = _ownership_high_water_observation(root)
        if (state == "invalid" or (state == "missing" and marker_state != "missing")) and not repair:
            raise WorkbenchOwnershipError(
                "Workbench ownership is invalid; run the direct-human service repair action"
            )
        if existing is not None and existing.mode == "relinquished":
            if repair:
                raise WorkbenchOwnershipError(
                    "relinquished Workbench ownership cannot be reactivated by repair; "
                    "use the explicit direct-human service start or service install action"
                )
            if not direct_human_reactivation:
                raise WorkbenchOwnershipError(
                    "relinquished Workbench ownership requires an explicit direct-human "
                    "service start or service install action"
                )
        high_water = _ownership_revision_high_water(root)
        if existing is not None:
            base_revision = max(existing.ownership_revision, high_water or 0)
        elif high_water is not None:
            if not repair:
                raise WorkbenchOwnershipError(
                    "Workbench ownership requires the explicit service repair action"
                )
            base_revision = high_water
        else:
            base_revision = 0
        revision = base_revision + 1
        generation = secrets.token_urlsafe(32)
        endpoint = f"http://{spec.host}:{spec.port}"
        bookmark = str(spec.config_home / "workbench.html")
        record = _new_ownership_record(
            "activating",
            revision=revision,
            generation=generation,
            endpoint=endpoint,
            bookmark_path=bookmark,
            anchor_repository=str(spec.repo),
        )
        _write_ownership_record(root, record)
    return replace(
        spec,
        ownership_generation=generation,
        ownership_revision=revision + 1,
    )


def complete_managed_activation(
    generation: str,
    *,
    expected_revision: int,
    platform_name: str | None = None,
    authority_root: Path | None = None,
) -> WorkbenchOwnershipRecord:
    platform_value = current_platform(platform_name)
    root = authority_root or workbench_authority_root(platform_value)
    with _ownership_lock(root):
        record = _require_ownership_record(root)
        if record.mode != "activating" or record.generation != generation:
            raise WorkbenchOwnershipError("managed activation generation changed before completion")
        configured_revision = record.ownership_revision + 1
        if configured_revision != expected_revision:
            raise WorkbenchOwnershipError(
                "managed activation revision changed before completion"
            )
        configured = replace(
            record,
            mode="configured",
            ownership_revision=configured_revision,
        )
        _write_ownership_record(root, configured)
        return configured


def workbench_server_binding(
    *,
    managed: bool,
    expected_generation: str = "",
    expected_revision: int = 0,
    platform_name: str | None = None,
    authority_root: Path | None = None,
) -> WorkbenchOwnershipBinding | None:
    platform_value = current_platform(platform_name)
    root = authority_root or workbench_authority_root(platform_value)
    authority = (
        managed_workbench_authority(
            platform_name=platform_value,
            authority_root=root,
            include_health=False,
        )
        if managed
        else initialize_manual_ownership(
            platform_name=platform_value,
            authority_root=root,
        )
    )
    if not authority.generation:
        return None
    if managed:
        if (
            not expected_generation
            or expected_revision < 1
            or authority.generation != expected_generation
            or authority.ownership_revision != expected_revision
        ):
            return None
        if authority.mode != "configured":
            return None
    elif not authority.manual_allowed:
        return None
    return WorkbenchOwnershipBinding(
        server_mode="managed" if managed else "manual",
        generation=authority.generation,
        ownership_revision=authority.ownership_revision,
    )


def acquire_workbench_request_lease(
    binding: WorkbenchOwnershipBinding,
    *,
    operation: str,
    platform_name: str | None = None,
    authority_root: Path | None = None,
    _invocation_marker: Path | None = None,
    _invocation_envelope: dict[str, Any] | None = None,
) -> WorkbenchOwnershipLease:
    """Admit one project-backed request against the current ownership revision."""

    if not operation or len(operation.encode("utf-8")) > 256:
        raise WorkbenchOwnershipError("Workbench ownership operation is invalid")
    platform_value = current_platform(platform_name)
    root = authority_root or workbench_authority_root(platform_value)
    with _ownership_lock(root):
        record = _require_ownership_record(root)
        if (
            record.generation != binding.generation
            or record.ownership_revision != binding.ownership_revision
            or not _mode_allows_server(record.mode, binding.server_mode)
        ):
            raise WorkbenchOwnershipError("Workbench server is superseded by current ownership")
        invocation_marker_value: dict[str, Any] | None = None
        if _invocation_marker is not None or _invocation_envelope is not None:
            if _invocation_marker is None or _invocation_envelope is None:
                raise WorkbenchOwnershipError(
                    "Workbench invocation append lease binding is incomplete"
                )
            invocation_id = _invocation_envelope.get("invocation_id")
            expected_marker = _ownership_invocations_dir(root) / f"{invocation_id}.json"
            if (
                not isinstance(invocation_id, str)
                or not invocation_id
                or _invocation_marker.expanduser().absolute()
                != expected_marker.expanduser().absolute()
            ):
                raise WorkbenchOwnershipError(
                    "Workbench invocation append lease marker is invalid"
                )
            invocation_marker_value = _bounded_json_object(
                _invocation_marker,
                MAX_INVOCATION_ENVELOPE_BYTES,
            )
            if (
                invocation_marker_value is None
                or any(
                    invocation_marker_value.get(key) != value
                    for key, value in _invocation_envelope.items()
                )
            ):
                raise WorkbenchOwnershipError(
                    "Workbench invocation append lease marker is unavailable"
                )
        if _path_present(_quiesce_path(root)):
            claim = _read_quiesce_claim(root)
            if (
                invocation_marker_value is None
                or claim is None
                or claim.generation != binding.generation
                or claim.ownership_revision != binding.ownership_revision
            ):
                raise WorkbenchOwnershipBusy(
                    "Workbench ownership transition is in progress"
                )
        leases = _ownership_leases_dir(root)
        _ensure_private_dir(leases)
        entries = _bounded_directory_entries(leases, MAX_OWNERSHIP_LEASES, "ownership leases")
        if len(entries) >= MAX_OWNERSHIP_LEASES:
            raise WorkbenchOwnershipBusy("Workbench ownership lease capacity is busy")
        token = secrets.token_urlsafe(24)
        lease_path = leases / f"{os.getpid()}-{token}.json"
        payload = {
            "schema_version": OWNERSHIP_SCHEMA_VERSION,
            "pid": os.getpid(),
            "generation": record.generation,
            "ownership_revision": record.ownership_revision,
            "server_mode": binding.server_mode,
            "operation": operation,
        }
        serialized = (json.dumps(payload, sort_keys=True) + "\n").encode("utf-8")
        _atomic_write(lease_path, serialized, mode=0o600)
        return WorkbenchOwnershipLease(
            authority_root=root,
            lease_path=lease_path,
            binding=binding,
            operation=operation,
        )


def begin_workbench_invocation(
    lease: WorkbenchOwnershipLease,
    *,
    repo_store_id: str,
    repo_path: Path,
    operation: str,
    ttl_seconds: int,
) -> WorkbenchInvocation:
    """Create one bounded Workbench-originated child invocation envelope."""

    if ttl_seconds < 1 or ttl_seconds > 21_720:
        raise WorkbenchOwnershipError("Workbench invocation expiry is out of range")
    now = int(time.time())
    invocation_id = secrets.token_urlsafe(24)
    payload = {
        "schema_version": OWNERSHIP_SCHEMA_VERSION,
        "invocation_id": invocation_id,
        "generation": lease.binding.generation,
        "ownership_revision": lease.binding.ownership_revision,
        "server_mode": lease.binding.server_mode,
        "repo_store_id": repo_store_id,
        "repo_path": str(repo_path.resolve()),
        "operation": operation,
        "parent_pid": os.getpid(),
        "issued_at": now,
        "expires_at": now + ttl_seconds,
    }
    envelope = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    if len(envelope.encode("utf-8")) > MAX_INVOCATION_ENVELOPE_BYTES:
        raise WorkbenchOwnershipError("Workbench invocation envelope exceeds its byte bound")
    root = lease.authority_root
    with _ownership_lock(root):
        record = _require_ownership_record(root)
        if (
            lease._released
            or record.generation != lease.binding.generation
            or record.ownership_revision != lease.binding.ownership_revision
            or not _mode_allows_server(record.mode, lease.binding.server_mode)
            or _path_present(_quiesce_path(root))
        ):
            raise WorkbenchOwnershipError("Workbench ownership changed before child invocation")
        invocations = _ownership_invocations_dir(root)
        _ensure_private_dir(invocations)
        entries = _bounded_directory_entries(
            invocations,
            MAX_OWNERSHIP_LEASES,
            "invocations",
        )
        if len(entries) >= MAX_OWNERSHIP_LEASES:
            raise WorkbenchOwnershipBusy("Workbench invocation capacity is busy")
        marker = invocations / f"{invocation_id}.json"
        _atomic_write(marker, (envelope + "\n").encode("utf-8"), mode=0o600)
    return WorkbenchInvocation(authority_root=root, marker_path=marker, envelope=envelope)


@contextmanager
def workbench_invocation_append_lease(
    config: Any,
    event: Any | None = None,
    *,
    raw_envelope: str | None = None,
) -> Iterator[Callable[[], None]]:
    """Revalidate a Workbench child envelope around one canonical append."""

    raw = (
        os.environ.get(WORKBENCH_INVOCATION_ENV, "")
        if raw_envelope is None
        else raw_envelope
    )
    if not raw:
        yield lambda: None
        return
    if len(raw.encode("utf-8")) > MAX_INVOCATION_ENVELOPE_BYTES:
        raise WorkbenchOwnershipError("Workbench invocation envelope exceeds its byte bound")
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise WorkbenchOwnershipError("Workbench invocation envelope is invalid") from exc
    if not isinstance(value, dict) or value.get("schema_version") != OWNERSHIP_SCHEMA_VERSION:
        raise WorkbenchOwnershipError("Workbench invocation envelope schema is invalid")
    required_strings = (
        "invocation_id",
        "generation",
        "server_mode",
        "repo_store_id",
        "repo_path",
        "operation",
    )
    if any(not isinstance(value.get(key), str) or not value[key] for key in required_strings):
        raise WorkbenchOwnershipError("Workbench invocation envelope is incomplete")
    revision = value.get("ownership_revision")
    expiry = value.get("expires_at")
    if isinstance(revision, bool) or not isinstance(revision, int):
        raise WorkbenchOwnershipError("Workbench invocation revision is invalid")
    if isinstance(expiry, bool) or not isinstance(expiry, int) or int(time.time()) > expiry:
        raise WorkbenchOwnershipError("Workbench invocation envelope expired")
    if value["repo_store_id"] != config.store_id:
        raise WorkbenchOwnershipError("Workbench invocation repository identity changed")
    if Path(value["repo_path"]).resolve() != config.project_root.resolve():
        raise WorkbenchOwnershipError("Workbench invocation repository path changed")
    server_mode = value["server_mode"]
    if server_mode not in {"managed", "manual"}:
        raise WorkbenchOwnershipError("Workbench invocation server mode is invalid")
    event_kind = str(getattr(event, "kind", ""))
    if event is not None and not _workbench_operation_allows_event(
        value["operation"],
        event_kind,
    ):
        raise WorkbenchOwnershipError(
            f"Workbench invocation {value['operation']!r} does not permit event {event_kind!r}"
        )
    binding = WorkbenchOwnershipBinding(
        server_mode=server_mode,
        generation=value["generation"],
        ownership_revision=revision,
    )
    root = workbench_authority_root()
    marker = _ownership_invocations_dir(root) / f"{value['invocation_id']}.json"
    marker_state, marker_payload = _bounded_regular_file(marker, MAX_INVOCATION_ENVELOPE_BYTES)
    try:
        marker_value = (
            json.loads(marker_payload.decode("utf-8")) if marker_payload is not None else None
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise WorkbenchOwnershipError("Workbench invocation marker is invalid") from exc
    if (
        marker_state != "ok"
        or not isinstance(marker_value, dict)
        or any(marker_value.get(key) != item for key, item in value.items())
    ):
        raise WorkbenchOwnershipError("Workbench invocation marker is unavailable")
    event_run_id: str | None = None
    if value["operation"] == "dispatch.cancel":
        payload = getattr(event, "payload", None)
        event_run_id = payload.get("run_id") if isinstance(payload, dict) else None
        if (
            not isinstance(event_run_id, str)
            or not event_run_id
            or event_run_id != marker_value.get("active_run_id")
        ):
            raise WorkbenchOwnershipError(
                "Workbench dispatch cancellation event does not match its bound run"
            )
    elif value["operation"] == "dispatch.run" and event_kind == "review_assurance_recorded":
        payload = getattr(event, "payload", None)
        event_run_id = payload.get("attempt_id") if isinstance(payload, dict) else None
        if (
            not isinstance(event_run_id, str)
            or not event_run_id
            or event_run_id != marker_value.get("active_run_id")
        ):
            raise WorkbenchOwnershipError(
                "Workbench review assurance event does not match its bound dispatch run"
            )
    lease = acquire_workbench_request_lease(
        binding,
        operation=f"append:{value['operation']}",
        authority_root=root,
        _invocation_marker=marker,
        _invocation_envelope=value,
    )
    if lease.binding.ownership_revision != revision:
        lease.release()
        raise WorkbenchOwnershipError("Workbench invocation ownership revision changed")
    run_id = str(getattr(event, "entity_id", ""))
    append_authorized = False

    def authorize_append() -> None:
        nonlocal append_authorized
        if append_authorized:
            return
        if value["operation"] == "dispatch.cancel":
            assert event_run_id is not None
            _update_dispatch_invocation_marker(
                root=root,
                marker=marker,
                envelope=value,
                binding=binding,
                run_id=event_run_id,
                terminal=False,
                child_started=True,
                expected_active_run_id=event_run_id,
            )
        elif value["operation"] == "dispatch.run" and event_kind == "dispatch_run_planned":
            _update_dispatch_invocation_marker(
                root=root,
                marker=marker,
                envelope=value,
                binding=binding,
                run_id=run_id,
                terminal=False,
                child_started=True,
            )
        append_authorized = True

    try:
        yield authorize_append
        if append_authorized and value["operation"] == "dispatch.run" and event_kind in {
            "dispatch_run_blocked",
            "dispatch_run_completed",
            "dispatch_run_failed",
            "dispatch_run_terminated",
        }:
            try:
                _update_dispatch_invocation_marker(
                    root=root,
                    marker=marker,
                    envelope=value,
                    binding=binding,
                    run_id=run_id,
                    terminal=True,
                )
            except (OSError, WorkbenchOwnershipError):
                # Canonical terminal append already committed. Leaving the
                # marker active is conservative and must not make that append
                # response-indeterminate.
                pass
    finally:
        lease.release()


def _update_dispatch_invocation_marker(
    *,
    root: Path,
    marker: Path,
    envelope: dict[str, Any],
    binding: WorkbenchOwnershipBinding,
    run_id: str,
    terminal: bool,
    child_started: bool = False,
    expected_active_run_id: str | None = None,
) -> None:
    if not run_id or len(run_id.encode("utf-8")) > 256:
        raise WorkbenchOwnershipError("Workbench dispatch run identity is invalid")
    with _ownership_lock(root):
        current = _require_ownership_record(root)
        current_marker = _bounded_json_object(marker, MAX_INVOCATION_ENVELOPE_BYTES)
        if (
            current.generation != binding.generation
            or current.ownership_revision != binding.ownership_revision
            or current_marker is None
            or any(current_marker.get(key) != item for key, item in envelope.items())
            or (
                expected_active_run_id is not None
                and current_marker.get("active_run_id") != expected_active_run_id
            )
        ):
            raise WorkbenchOwnershipError(
                "Workbench ownership changed before dispatch recovery was bound"
            )
        if child_started:
            current_marker["child_started"] = True
        if terminal:
            if current_marker.get("active_run_id") == run_id:
                current_marker["terminal_run_id"] = run_id
        else:
            current_marker["active_run_id"] = run_id
        serialized = (
            json.dumps(current_marker, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode("utf-8")
        if len(serialized) > MAX_INVOCATION_ENVELOPE_BYTES:
            raise WorkbenchOwnershipError("Workbench invocation marker exceeds its byte bound")
        _atomic_write(marker, serialized, mode=0o600)


def _mark_invocation_interrupted(
    marker_path: Path,
    marker: dict[str, Any],
    *,
    reason: str,
) -> None:
    """Persist a closed parent-observed child interruption under ownership lock."""

    if reason not in {"child_failed", "canonical_suffix_incomplete"}:
        raise WorkbenchOwnershipError("Workbench invocation interruption reason is invalid")
    current = _bounded_json_object(marker_path, MAX_INVOCATION_ENVELOPE_BYTES)
    if current != marker:
        raise WorkbenchOwnershipError("Workbench invocation changed before interruption record")
    interrupted = dict(marker)
    interrupted["interruption_state"] = "confirmed"
    interrupted["interruption_reason"] = reason
    interrupted["interrupted_at"] = int(time.time())
    _write_invocation_marker(marker_path, interrupted)


def _write_invocation_marker(marker_path: Path, value: dict[str, Any]) -> None:
    serialized = (json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n").encode(
        "utf-8"
    )
    if len(serialized) > MAX_INVOCATION_ENVELOPE_BYTES:
        raise WorkbenchOwnershipError("Workbench invocation marker exceeds its byte bound")
    _atomic_write(marker_path, serialized, mode=0o600)


def _workbench_operation_allows_event(operation: str, event_kind: str) -> bool:
    """Keep a Workbench child capability within its declared closed operation."""

    from agent_mesh.core.agent_instances import INSTANCE_EVENT_KINDS
    from agent_mesh.core.dispatch_schema import DISPATCH_EVENT_KINDS

    if operation == "dispatch.assure":
        return event_kind == "review_assurance_recorded"
    if operation == "dispatch.assurance.flag":
        return event_kind == "review_assurance_flagged"
    if operation == "dispatch.assurance.retire":
        return event_kind == "review_assurance_retired"
    if operation == "dispatch.cancel":
        return event_kind in {
            "dispatch_run_terminated",
            "dispatch_lease_released",
            "agent_instance_launch_released",
            "agent_instance_terminal",
        }
    if operation == "dispatch.run":
        return (
            event_kind
            in DISPATCH_EVENT_KINDS
            - {
                "review_assurance_flagged",
                "review_assurance_retired",
            }
            or event_kind in INSTANCE_EVENT_KINDS
            or event_kind in {"req_created", "res_posted"}
        )
    return False


def make_service_spec(
    *,
    repo: Path,
    host: str,
    port: int,
    python_executable: Path | None = None,
    config_home: Path | None = None,
    platform_name: str | None = None,
) -> WorkbenchServiceSpec:
    platform_value = current_platform(platform_name)
    host_value = host.strip()
    _validate_loopback_host(host_value)
    requested_executable = (python_executable or Path(sys.executable)).expanduser()
    # Preserve the launcher path instead of resolving symlinks.  A virtualenv's
    # Python executable commonly points at the base interpreter; resolving it
    # would silently discard the virtualenv and make the managed service import
    # a different (potentially stale) agent_mesh installation.
    executable = Path(os.path.abspath(requested_executable))
    if platform_value == "win32" and executable.name.casefold() == "python.exe":
        pythonw = executable.with_name("pythonw.exe")
        if pythonw.exists():
            executable = pythonw
    if not (1 <= int(port) <= 65535):
        raise WorkbenchServiceError("Workbench service port must be between 1 and 65535")
    return WorkbenchServiceSpec(
        repo=repo.expanduser().resolve(),
        host=host_value,
        port=int(port),
        python_executable=executable,
        config_home=(config_home or registry_dir()).expanduser().absolute(),
    )


def service_definition_path(
    platform_name: str | None = None,
    *,
    home: Path | None = None,
) -> Path:
    platform_value = current_platform(platform_name)
    if home is not None:
        user_home = home.expanduser().absolute()
    elif platform_value in {"darwin", "linux"}:
        user_home = _os_account_home(platform_value)
    else:
        user_home = Path()
    if platform_value == "darwin":
        return user_home / "Library" / "LaunchAgents" / f"{SERVICE_LABEL}.plist"
    if platform_value == "linux":
        return user_home / ".config" / "systemd" / "user" / SYSTEMD_UNIT
    return _stable_registry_dir(platform_value, home=home) / "workbench-task.xml"


def service_metadata_path() -> Path:
    return _stable_registry_dir() / "workbench-service.json"


def render_launch_agent(spec: WorkbenchServiceSpec) -> bytes:
    log_dir = spec.config_home / "logs"
    command = [*spec.command, "--launchd-socket-name", LAUNCHD_SOCKET_NAME]
    payload = {
        "Label": SERVICE_LABEL,
        "ProgramArguments": command,
        "RunAtLoad": True,
        "KeepAlive": True,
        "Sockets": {
            LAUNCHD_SOCKET_NAME: {
                "SockFamily": "IPv4",
                "SockNodeName": spec.host,
                "SockProtocol": "TCP",
                "SockServiceName": spec.port,
                "SockType": "stream",
            }
        },
        "ProcessType": "Background",
        "ThrottleInterval": 10,
        "WorkingDirectory": str(spec.repo),
        "StandardOutPath": str(log_dir / "workbench.log"),
        "StandardErrorPath": str(log_dir / "workbench-error.log"),
        "EnvironmentVariables": {"PYTHONUNBUFFERED": "1"},
    }
    return plistlib.dumps(payload, fmt=plistlib.FMT_XML, sort_keys=True)


def _systemd_quote(value: str) -> str:
    escaped = (
        value.replace("%", "%%")
        .replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )
    return f'"{escaped}"'


def render_systemd_unit(spec: WorkbenchServiceSpec) -> str:
    command = " ".join(_systemd_quote(argument) for argument in spec.command)
    return "\n".join(
        [
            "[Unit]",
            "Description=Agent Mesh Workbench",
            "After=default.target",
            "",
            "[Service]",
            "Type=simple",
            f"ExecStart={command}",
            f"WorkingDirectory={_systemd_quote(str(spec.repo))}",
            'Environment="PYTHONUNBUFFERED=1"',
            "Restart=on-failure",
            "RestartSec=2s",
            "TimeoutStopSec=15s",
            "",
            "[Install]",
            "WantedBy=default.target",
            "",
        ]
    )


def _windows_arguments(arguments: Sequence[str]) -> str:
    return subprocess.list2cmdline(list(arguments))


def render_windows_task(spec: WorkbenchServiceSpec, *, user_id: str) -> str:
    command = xml_escape(str(spec.python_executable))
    arguments = xml_escape(_windows_arguments(spec.command[1:]))
    working_directory = xml_escape(str(spec.repo))
    identity = xml_escape(user_id)
    return f'''<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Description>Agent Mesh Workbench per-user service</Description>
  </RegistrationInfo>
  <Triggers>
    <LogonTrigger>
      <Enabled>true</Enabled>
      <UserId>{identity}</UserId>
    </LogonTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <UserId>{identity}</UserId>
      <LogonType>InteractiveToken</LogonType>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
    <RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>
    <AllowStartOnDemand>true</AllowStartOnDemand>
    <Enabled>true</Enabled>
    <Hidden>false</Hidden>
    <RunOnlyIfIdle>false</RunOnlyIfIdle>
    <WakeToRun>false</WakeToRun>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <Priority>7</Priority>
    <RestartOnFailure>
      <Interval>PT1M</Interval>
      <Count>3</Count>
    </RestartOnFailure>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>{command}</Command>
      <Arguments>{arguments}</Arguments>
      <WorkingDirectory>{working_directory}</WorkingDirectory>
    </Exec>
  </Actions>
</Task>
'''


def install_workbench_service(
    spec: WorkbenchServiceSpec,
    *,
    platform_name: str | None = None,
    repair: bool = False,
    authority_root: Path | None = None,
    direct_human_reactivation: bool = False,
) -> WorkbenchServiceStatus:
    platform_value = current_platform(platform_name)
    _require_d015_activation_platform(platform_value)
    root = authority_root or workbench_authority_root(platform_value)
    claim = _quiesce_generation_change(
        root,
        drain_seconds=OWNERSHIP_DRAIN_SECONDS,
    )
    try:
        with _transition_lock(root, quiesce_claim=claim):
            if claim is not None:
                _require_quiesce_drained(root, claim)
            result = _install_workbench_service_locked(
                spec,
                platform_name=platform_value,
                repair=repair,
                authority_root=root,
                direct_human_reactivation=direct_human_reactivation,
            )
            if claim is not None:
                _clear_quiesce(root, claim)
            return result
    except BaseException:
        if claim is not None:
            _clear_quiesce(root, claim, best_effort=True)
        raise


def _install_workbench_service_locked(
    spec: WorkbenchServiceSpec,
    *,
    platform_name: str,
    repair: bool,
    authority_root: Path,
    direct_human_reactivation: bool,
) -> WorkbenchServiceStatus:
    """Install while the caller retains the operation-scoped transition lock."""

    platform_value = platform_name
    spec = begin_managed_activation(
        spec,
        platform_name=platform_value,
        authority_root=authority_root,
        repair=repair,
        direct_human_reactivation=direct_human_reactivation,
    )
    definition = service_definition_path(platform_value)
    if platform_value == "darwin":
        _ensure_private_dir(spec.config_home / "logs")
        _atomic_write(definition, render_launch_agent(spec), mode=0o600)
        domain = _launchd_domain()
        _run(("launchctl", "bootout", domain, str(definition)), check=False)
    elif platform_value == "linux":
        _require_systemd_user()
        _atomic_write(definition, render_systemd_unit(spec).encode("utf-8"), mode=0o600)
        _run(("systemctl", "--user", "daemon-reload"))
        _run(("systemctl", "--user", "enable", SYSTEMD_UNIT))
    else:
        user_id = _windows_user_id()
        _atomic_write(
            definition,
            render_windows_task(spec, user_id=user_id).encode("utf-16"),
            mode=0o600,
        )
        _run(("schtasks.exe", "/End", "/TN", WINDOWS_TASK), check=False)
        _run(("schtasks.exe", "/Create", "/TN", WINDOWS_TASK, "/XML", str(definition), "/F"))

    _write_metadata(spec.as_dict(platform_name=platform_value, definition=definition))
    complete_managed_activation(
        spec.ownership_generation,
        expected_revision=spec.ownership_revision,
        platform_name=platform_value,
        authority_root=authority_root,
    )
    # The runner is started only after configured revision is durable, so its
    # exact generation+revision binding can be admitted without an upgrade.
    if platform_value == "darwin":
        domain = _launchd_domain()
        _run(("launchctl", "bootstrap", domain, str(definition)))
        _run(("launchctl", "enable", f"{domain}/{SERVICE_LABEL}"))
        _run(("launchctl", "kickstart", "-k", f"{domain}/{SERVICE_LABEL}"))
    elif platform_value == "linux":
        # `restart` starts an inactive unit and makes reinstalls apply the
        # current executable, repo, port, generation, and revision.
        _run(("systemctl", "--user", "restart", SYSTEMD_UNIT))
    else:
        _run(("schtasks.exe", "/Run", "/TN", WINDOWS_TASK))
    return workbench_service_status(
        platform_name=platform_value,
        authority_root=authority_root,
    )


def start_workbench_service(
    *,
    platform_name: str | None = None,
    authority_root: Path | None = None,
    direct_human_reactivation: bool = False,
) -> WorkbenchServiceStatus:
    platform_value = current_platform(platform_name)
    _require_d015_activation_platform(platform_value)
    root = authority_root or workbench_authority_root(platform_value)
    with _transition_lock(root):
        return _start_workbench_service_locked(
            platform_name=platform_value,
            authority_root=root,
            direct_human_reactivation=direct_human_reactivation,
        )


def _start_workbench_service_locked(
    *,
    platform_name: str,
    authority_root: Path,
    direct_human_reactivation: bool,
) -> WorkbenchServiceStatus:
    platform_value = platform_name
    authority = managed_workbench_authority(
        platform_name=platform_value,
        authority_root=authority_root,
        include_health=False,
    )
    if authority.mode in {"absent", "relinquished"}:
        if authority.mode == "relinquished" and not direct_human_reactivation:
            raise WorkbenchOwnershipError(
                "relinquished Workbench ownership requires an explicit direct-human "
                "service start or service install action"
            )
        metadata = _read_metadata()
        spec = _service_spec_from_metadata(metadata, platform_name=platform_value)
        if spec is None:
            raise WorkbenchServiceError(
                "Workbench service cannot be started after relinquishment without valid metadata; "
                "run service install"
            )
        return _install_workbench_service_locked(
            spec,
            platform_name=platform_value,
            repair=False,
            authority_root=authority_root,
            direct_human_reactivation=direct_human_reactivation,
        )
    if authority.mode != "configured":
        raise WorkbenchOwnershipError(
            "Workbench ownership is not configured; run service install or service repair"
        )
    definition = service_definition_path(platform_value)
    if not definition.exists() and platform_value != "win32":
        raise WorkbenchServiceError("Workbench service is not installed")
    if platform_value == "darwin":
        domain = _launchd_domain()
        loaded = _run(
            ("launchctl", "print", f"{domain}/{SERVICE_LABEL}"),
            check=False,
        ).returncode == 0
        if not loaded:
            _run(("launchctl", "bootstrap", domain, str(definition)))
        _run(("launchctl", "kickstart", f"{domain}/{SERVICE_LABEL}"))
    elif platform_value == "linux":
        _require_systemd_user()
        _run(("systemctl", "--user", "start", SYSTEMD_UNIT))
    else:
        _run(("schtasks.exe", "/Run", "/TN", WINDOWS_TASK))
    return workbench_service_status(
        platform_name=platform_value,
        authority_root=authority_root,
    )


def restart_workbench_service(
    *,
    platform_name: str | None = None,
    authority_root: Path | None = None,
) -> WorkbenchServiceStatus:
    platform_value = current_platform(platform_name)
    _require_d015_activation_platform(platform_value)
    root = authority_root or workbench_authority_root(platform_value)
    with _transition_lock(root):
        return _restart_workbench_service_locked(
            platform_name=platform_value,
            authority_root=root,
        )


def _restart_workbench_service_locked(
    *,
    platform_name: str,
    authority_root: Path,
) -> WorkbenchServiceStatus:
    platform_value = platform_name
    authority = managed_workbench_authority(
        platform_name=platform_value,
        authority_root=authority_root,
        include_health=False,
    )
    if authority.mode != "configured":
        raise WorkbenchOwnershipError(
            "Workbench ownership is not configured; run service start, install, or repair"
        )
    if platform_value == "darwin":
        domain = _launchd_domain()
        result = _run(
            ("launchctl", "kickstart", "-k", f"{domain}/{SERVICE_LABEL}"),
            check=False,
        )
        if result.returncode != 0:
            return _start_workbench_service_locked(
                platform_name=platform_value,
                authority_root=authority_root,
                direct_human_reactivation=False,
            )
    elif platform_value == "linux":
        _require_systemd_user()
        _run(("systemctl", "--user", "restart", SYSTEMD_UNIT))
    else:
        _run(("schtasks.exe", "/End", "/TN", WINDOWS_TASK), check=False)
        _run(("schtasks.exe", "/Run", "/TN", WINDOWS_TASK))
    return workbench_service_status(
        platform_name=platform_value,
        authority_root=authority_root,
    )


def relinquish_workbench_service(
    *,
    platform_name: str | None = None,
    drain_seconds: float = OWNERSHIP_DRAIN_SECONDS,
    authority_root: Path | None = None,
) -> WorkbenchServiceStatus:
    """Disable automatic relaunch and explicitly restore manual Workbench authority."""

    platform_value = current_platform(platform_name)
    _require_d015_activation_platform(platform_value)
    root = authority_root or workbench_authority_root(platform_value)
    _relinquish_ownership(
        platform_name=platform_value,
        remove_definition=False,
        drain_seconds=drain_seconds,
        authority_root=root,
    )
    return workbench_service_status(
        platform_name=platform_value,
        authority_root=authority_root,
    )


def uninstall_workbench_service(
    *,
    platform_name: str | None = None,
    authority_root: Path | None = None,
) -> WorkbenchServiceStatus:
    platform_value = current_platform(platform_name)
    _require_d015_activation_platform(platform_value)
    root = authority_root or workbench_authority_root(platform_value)
    claim = _quiesce_relinquishment(
        root,
        remove_definition=True,
        drain_seconds=OWNERSHIP_DRAIN_SECONDS,
    )
    try:
        with _transition_lock(root, quiesce_claim=claim):
            definition = service_definition_path(platform_value)
            metadata = _read_metadata()
            authority = managed_workbench_authority(
                platform_name=platform_value,
                authority_root=root,
                include_health=False,
            )
            if authority.mode == "relinquished":
                if claim is not None:
                    raise WorkbenchOwnershipError(
                        "Workbench ownership changed after uninstall quiesced"
                    )
                _disable_native_service(platform_name=platform_value, remove_definition=True)
            else:
                _relinquish_ownership_locked(
                    platform_name=platform_value,
                    remove_definition=True,
                    authority_root=root,
                    quiesce_claim=claim,
                )
            _unlink_private_regular_file(definition)
            bookmark = _managed_bookmark_path(metadata)
            if bookmark != Path() and _managed_bookmark_target(bookmark) is not None:
                _unlink_private_regular_file(bookmark)
            _unlink_private_regular_file(service_metadata_path())
            if platform_value == "linux":
                _run(("systemctl", "--user", "daemon-reload"))
                _run(("systemctl", "--user", "reset-failed", SYSTEMD_UNIT), check=False)
    except BaseException:
        if claim is not None:
            _clear_quiesce(root, claim, best_effort=True)
        raise
    return workbench_service_status(
        platform_name=platform_value,
        authority_root=authority_root,
    )


def _service_spec_from_metadata(
    metadata: dict[str, Any] | None,
    *,
    platform_name: str,
) -> WorkbenchServiceSpec | None:
    if metadata is None:
        return None
    repo = metadata.get("repo")
    host = metadata.get("host")
    port = metadata.get("port")
    executable = metadata.get("python_executable")
    config_home = metadata.get("config_home")
    if (
        not isinstance(repo, str)
        or not repo
        or not isinstance(host, str)
        or not host
        or isinstance(port, bool)
        or not isinstance(port, int)
        or not isinstance(executable, str)
        or not executable
        or not isinstance(config_home, str)
        or not config_home
    ):
        return None
    try:
        return make_service_spec(
            repo=Path(repo),
            host=host,
            port=port,
            python_executable=Path(executable),
            config_home=Path(config_home),
            platform_name=platform_name,
        )
    except (OSError, ValueError, WorkbenchServiceError):
        return None


def _lease_pid_is_active(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return True
    return True


def _bounded_json_object(path: Path, maximum: int) -> dict[str, Any] | None:
    state, payload = _bounded_regular_file(path, maximum)
    if state != "ok" or payload is None:
        return None
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _active_ownership_leases(root: Path) -> list[Path]:
    directory = _ownership_leases_dir(root)
    if not _path_present(directory):
        return []
    entries = _bounded_directory_entries(
        directory,
        MAX_OWNERSHIP_LEASES,
        "ownership leases",
    )
    active: list[Path] = []
    for entry in entries:
        value = _bounded_json_object(entry, MAX_OWNERSHIP_LEASE_BYTES)
        pid = value.get("pid") if value is not None else None
        if isinstance(pid, int) and not isinstance(pid, bool) and _lease_pid_is_active(pid):
            active.append(entry)
        elif value is None:
            active.append(entry)
        else:
            _unlink_private_entry(directory, entry.name)
    return active


def _active_invocations(root: Path) -> list[dict[str, Any]]:
    directory = _ownership_invocations_dir(root)
    if not _path_present(directory):
        return []
    entries = _bounded_directory_entries(
        directory,
        MAX_OWNERSHIP_LEASES,
        "invocations",
    )
    values: list[dict[str, Any]] = []
    now = int(time.time())
    for entry in entries:
        value = _bounded_json_object(entry, MAX_INVOCATION_ENVELOPE_BYTES)
        if value is None:
            values.append({"operation": "invalid", "path": str(entry)})
            continue
        expiry = value.get("expires_at")
        parent_pid = value.get("parent_pid")
        expiry_value: int | None = (
            int(expiry) if isinstance(expiry, int) and not isinstance(expiry, bool) else None
        )
        parent_pid_value: int | None = (
            int(parent_pid)
            if isinstance(parent_pid, int) and not isinstance(parent_pid, bool)
            else None
        )
        parent_active = (
            _lease_pid_is_active(parent_pid_value) if parent_pid_value is not None else False
        )
        expired = expiry_value < now if expiry_value is not None else False
        operation = str(value.get("operation", ""))
        if not operation.startswith("dispatch"):
            if expired and not parent_active:
                _unlink_private_entry(directory, entry.name)
                continue
            values.append(value)
            continue

        active_run_id = value.get("active_run_id")
        if not isinstance(active_run_id, str) or not active_run_id:
            if expired or not parent_active:
                _unlink_private_entry(directory, entry.name)
                continue
            values.append(value)
            continue
        child_started = value.get("child_started") is True
        if not child_started:
            if expired or not parent_active:
                _unlink_private_entry(directory, entry.name)
                continue
            values.append(value)
            continue
        confirmed_interruption = (
            value.get("interruption_state") == "confirmed"
            and value.get("interruption_reason")
            in {"child_failed", "canonical_suffix_incomplete"}
            and isinstance(value.get("interrupted_at"), int)
            and not isinstance(value.get("interrupted_at"), bool)
        )
        value["_interrupted"] = bool(confirmed_interruption or expired or not parent_active)
        value["_marker_name"] = entry.name
        values.append(value)
    return values


def _dispatch_marker_canonical_complete(marker: dict[str, Any]) -> bool:
    from agent_mesh.config import load_config
    from agent_mesh.core.lock import acquire
    from agent_mesh.dispatch.execution import verified_dispatch_run_recovery_complete

    repo_path = marker.get("repo_path")
    run_id = marker.get("active_run_id")
    if not isinstance(repo_path, str) or not repo_path:
        return False
    if not isinstance(run_id, str) or not run_id:
        return False
    try:
        config = load_config(Path(repo_path))
        if config.store_id != marker.get("repo_store_id"):
            return False
        lock = acquire(config.agent_dir / ".mail-lock")
        try:
            return verified_dispatch_run_recovery_complete(config, run_id)
        finally:
            lock.release()
    except Exception:
        return False


@contextmanager
def workbench_dispatch_recovery(
    config: Any,
    run_id: str,
    *,
    authority_root: Path | None = None,
) -> Iterator[WorkbenchDispatchRecoveryLease]:
    """Bind direct canonical recovery to one interrupted Workbench marker."""

    if not run_id or len(run_id.encode("utf-8")) > 256:
        raise WorkbenchOwnershipError("Workbench dispatch recovery run ID is invalid")
    root = authority_root or workbench_authority_root()
    with _transition_lock(root):
        with _ownership_lock(root):
            current = _require_ownership_record(root)
            directory = _ownership_invocations_dir(root)
            matches: list[tuple[Path, dict[str, Any]]] = []
            if _path_present(directory):
                for path in _bounded_directory_entries(
                    directory,
                    MAX_OWNERSHIP_LEASES,
                    "invocations",
                ):
                    marker = _bounded_json_object(path, MAX_INVOCATION_ENVELOPE_BYTES)
                    if (
                        marker is not None
                        and marker.get("active_run_id") == run_id
                        and marker.get("child_started") is True
                    ):
                        matches.append((path, marker))
            if len(matches) > 1:
                raise WorkbenchOwnershipError(
                    "the interrupted Workbench dispatch marker is ambiguous"
                )
            if matches:
                marker_path, marker = matches[0]
                parent_pid = marker.get("parent_pid")
                expires_at = marker.get("expires_at")
                parent_active = (
                    isinstance(parent_pid, int)
                    and not isinstance(parent_pid, bool)
                    and _lease_pid_is_active(parent_pid)
                )
                expired = (
                    isinstance(expires_at, int)
                    and not isinstance(expires_at, bool)
                    and expires_at < int(time.time())
                )
                confirmed_interruption = (
                    marker.get("interruption_state") == "confirmed"
                    and marker.get("interruption_reason")
                    in {"child_failed", "canonical_suffix_incomplete"}
                    and isinstance(marker.get("interrupted_at"), int)
                    and not isinstance(marker.get("interrupted_at"), bool)
                )
                if (
                    not str(marker.get("operation", "")).startswith("dispatch")
                    or marker.get("generation") != current.generation
                    or marker.get("ownership_revision") != current.ownership_revision
                    or marker.get("repo_store_id") != config.store_id
                    or marker.get("repo_path") != str(config.project_root.resolve())
                ):
                    raise WorkbenchOwnershipError(
                        "the interrupted dispatch marker does not match current ownership or repo"
                    )
                if parent_active and not expired and not confirmed_interruption:
                    raise WorkbenchOwnershipBusy(
                        "the Workbench dispatch parent is still active; recovery was not started"
                    )
                lease = WorkbenchDispatchRecoveryLease(
                    authority_root=root,
                    marker_path=marker_path,
                    marker=marker,
                    config=config,
                    run_id=run_id,
                )
            else:
                lease = WorkbenchDispatchRecoveryLease(
                    authority_root=root,
                    marker_path=Path(),
                    marker={},
                    config=config,
                    run_id=run_id,
                    _completed=True,
                )
        if not matches:
            from agent_mesh.core.lock import acquire
            from agent_mesh.dispatch.execution import (
                DISPATCH_RECOVERY_MAX_BYTES,
                DISPATCH_RECOVERY_MAX_EVENTS,
                DISPATCH_RECOVERY_MAX_SECONDS,
                dispatch_run_recovery_complete,
            )
            from agent_mesh.store.read_model import capture_event_snapshot

            lock = acquire(config.agent_dir / ".mail-lock")
            try:
                deadline = time.monotonic() + DISPATCH_RECOVERY_MAX_SECONDS
                snapshot = capture_event_snapshot(
                    config,
                    max_bytes=DISPATCH_RECOVERY_MAX_BYTES,
                    max_events=DISPATCH_RECOVERY_MAX_EVENTS,
                    deadline_monotonic=deadline,
                )
                planned_exists = any(
                    record.get("kind") == "dispatch_run_planned"
                    and record.get("entity_id") == run_id
                    for record in snapshot.records
                )
                complete = dispatch_run_recovery_complete(
                    list(snapshot.records),
                    run_id,
                )
            finally:
                lock.release()
            if not (planned_exists and complete):
                raise WorkbenchOwnershipError(
                    "no interrupted Workbench dispatch marker is available"
                )
        yield lease
        if not lease._completed:
            raise WorkbenchOwnershipError(
                "canonical dispatch recovery did not confirm marker retirement"
            )


def _quiesce_claim_from_value(value: dict[str, Any] | None) -> _WorkbenchQuiesceClaim | None:
    if value is None or set(value) != {
        "schema_version",
        "operation",
        "generation",
        "ownership_revision",
        "transition_id",
        "pid",
    }:
        return None
    operation = value.get("operation")
    generation = value.get("generation")
    revision = value.get("ownership_revision")
    transition_id = value.get("transition_id")
    pid = value.get("pid")
    if (
        value.get("schema_version") != OWNERSHIP_SCHEMA_VERSION
        or operation not in {"install", "relinquish", "uninstall"}
        or not isinstance(generation, str)
        or _GENERATION_RE.fullmatch(generation) is None
        or isinstance(revision, bool)
        or not isinstance(revision, int)
        or revision < 1
        or not isinstance(transition_id, str)
        or _GENERATION_RE.fullmatch(transition_id) is None
        or isinstance(pid, bool)
        or not isinstance(pid, int)
        or pid < 1
    ):
        return None
    return _WorkbenchQuiesceClaim(
        operation=operation,
        generation=generation,
        ownership_revision=revision,
        transition_id=transition_id,
        pid=pid,
    )


def _read_quiesce_claim(root: Path) -> _WorkbenchQuiesceClaim | None:
    return _quiesce_claim_from_value(
        _bounded_json_object(_quiesce_path(root), MAX_OWNERSHIP_LEASE_BYTES)
    )


def _require_quiesce_claim(
    root: Path,
    expected: _WorkbenchQuiesceClaim,
) -> _WorkbenchQuiesceClaim:
    current = _read_quiesce_claim(root)
    if current != expected:
        raise WorkbenchOwnershipError(
            "Workbench ownership quiesce claim changed during the transition"
        )
    return current


def _require_quiesce_drained(
    root: Path,
    expected: _WorkbenchQuiesceClaim,
) -> WorkbenchOwnershipRecord:
    _require_quiesce_claim(root, expected)
    current = _require_ownership_record(root)
    if (
        current.generation != expected.generation
        or current.ownership_revision != expected.ownership_revision
    ):
        raise WorkbenchOwnershipError(
            "Workbench ownership changed after the quiesce drain"
        )
    if _active_ownership_leases(root) or _active_invocations(root):
        raise WorkbenchOwnershipBusy(
            "Workbench ownership became busy after the quiesce drain"
        )
    return current


def _write_quiesce(
    root: Path,
    record: WorkbenchOwnershipRecord,
    operation: str,
) -> _WorkbenchQuiesceClaim:
    claim = _WorkbenchQuiesceClaim(
        operation=operation,
        generation=record.generation,
        ownership_revision=record.ownership_revision,
        transition_id=secrets.token_urlsafe(32),
        pid=os.getpid(),
    )
    _atomic_write(
        _quiesce_path(root),
        (json.dumps(claim.as_dict(), sort_keys=True) + "\n").encode("utf-8"),
        mode=0o600,
    )
    return claim


def _drain_quiesce_claim(
    root: Path,
    record: WorkbenchOwnershipRecord,
    claim: _WorkbenchQuiesceClaim,
    *,
    drain_seconds: float,
    retry_action: str,
) -> None:
    """Poll under short ownership locks so admitted workers can finish between polls."""

    deadline = time.monotonic() + max(0.0, drain_seconds)
    while True:
        with _ownership_lock(root):
            _require_quiesce_claim(root, claim)
            current = _require_ownership_record(root)
            if (
                current.generation != record.generation
                or current.ownership_revision != record.ownership_revision
            ):
                raise WorkbenchOwnershipError(
                    "Workbench ownership changed while admitted work was draining"
                )
            invocations = _active_invocations(root)
            leases = _active_ownership_leases(root)
            interrupted = _interrupted_dispatches(record, invocations)
            if not leases and interrupted:
                run_id, repo_path = sorted(interrupted.items())[0]
                location = f" from {repo_path}" if repo_path else ""
                raise WorkbenchDispatchRecoveryRequired(
                    "dispatch_recovery_required: run "
                    f"`agent-q recover --resolve-dispatch={run_id}`{location}, then retry "
                    f"{retry_action}"
                )
            if not leases and not invocations:
                return
        if time.monotonic() >= deadline:
            raise WorkbenchOwnershipBusy(
                "Workbench ownership drain timed out; ownership was not changed"
            )
        time.sleep(0.05)


def _quiesce_generation_change(
    root: Path,
    *,
    drain_seconds: float,
) -> _WorkbenchQuiesceClaim | None:
    """Close existing managed admission and drain before replacing a generation."""

    with _ownership_lock(root):
        if _path_present(_quiesce_path(root)):
            raise WorkbenchOwnershipBusy("another Workbench ownership transition is active")
        state, record = _read_ownership_record(root)
        if state != "ok" or record is None or record.mode not in {"configured", "activating"}:
            return None
        claim = _write_quiesce(root, record, "install")
    try:
        _drain_quiesce_claim(
            root,
            record,
            claim,
            drain_seconds=drain_seconds,
            retry_action="service install or repair",
        )
        return claim
    except BaseException:
        _clear_quiesce(root, claim, best_effort=True)
        raise


def _quiesce_relinquishment(
    root: Path,
    *,
    remove_definition: bool,
    drain_seconds: float,
) -> _WorkbenchQuiesceClaim | None:
    operation = "uninstall" if remove_definition else "relinquish"
    with _ownership_lock(root):
        if _path_present(_quiesce_path(root)):
            raise WorkbenchOwnershipBusy("another Workbench ownership transition is active")
        record = _require_ownership_record(root)
        if record.mode == "relinquished":
            return None
        if record.mode not in {"configured", "activating"}:
            raise WorkbenchOwnershipError("managed Workbench ownership is not active")
        claim = _write_quiesce(root, record, operation)
    try:
        _drain_quiesce_claim(
            root,
            record,
            claim,
            drain_seconds=drain_seconds,
            retry_action="service relinquish",
        )
        return claim
    except BaseException:
        _clear_quiesce(root, claim, best_effort=True)
        raise


def _interrupted_dispatches(
    record: WorkbenchOwnershipRecord,
    invocations: list[dict[str, Any]],
) -> dict[str, str]:
    interrupted: dict[str, str] = {}
    for invocation in invocations:
        run_id = invocation.get("active_run_id")
        if not (
            str(invocation.get("operation", "")).startswith("dispatch")
            and isinstance(run_id, str)
            and run_id
            and invocation.get("_interrupted") is True
        ):
            continue
        if (
            invocation.get("generation") != record.generation
            or invocation.get("ownership_revision") != record.ownership_revision
        ):
            raise WorkbenchOwnershipError(
                "interrupted dispatch marker does not match current ownership"
            )
        interrupted[run_id] = str(invocation.get("repo_path", "")).strip()
    return interrupted


def _clear_quiesce(
    root: Path,
    expected: _WorkbenchQuiesceClaim,
    *,
    best_effort: bool = False,
) -> None:
    try:
        with _ownership_lock(root):
            _require_quiesce_claim(root, expected)
            _unlink_private_regular_file(_quiesce_path(root))
    except WorkbenchOwnershipError:
        if not best_effort:
            raise


def _disable_native_service(*, platform_name: str, remove_definition: bool) -> None:
    definition = service_definition_path(platform_name)
    if platform_name == "darwin":
        domain = _launchd_domain()
        disabled = _run(("launchctl", "disable", f"{domain}/{SERVICE_LABEL}"), check=False)
        if disabled.returncode != 0:
            raise WorkbenchServiceError("launchd Workbench service could not be disabled")
        result = _run(("launchctl", "bootout", domain, str(definition)), check=False)
        if result.returncode != 0:
            verify = _run(("launchctl", "print", f"{domain}/{SERVICE_LABEL}"), check=False)
            if verify.returncode == 0:
                raise WorkbenchServiceError("launchd Workbench service could not be disabled")
    elif platform_name == "linux":
        _require_systemd_user()
        result = _run(
            ("systemctl", "--user", "disable", "--now", SYSTEMD_UNIT),
            check=False,
        )
        if result.returncode != 0:
            raise WorkbenchServiceError("systemd Workbench service could not be disabled")
    else:
        _run(("schtasks.exe", "/End", "/TN", WINDOWS_TASK), check=False)
        action = "/Delete" if remove_definition else "/Change"
        command = (
            ("schtasks.exe", action, "/TN", WINDOWS_TASK, "/F")
            if remove_definition
            else ("schtasks.exe", action, "/TN", WINDOWS_TASK, "/Disable")
        )
        result = _run(command, check=False)
        if result.returncode != 0:
            raise WorkbenchServiceError("Task Scheduler Workbench service could not be disabled")


def _relinquish_ownership(
    *,
    platform_name: str,
    remove_definition: bool,
    drain_seconds: float,
    authority_root: Path | None = None,
) -> WorkbenchOwnershipRecord:
    root = authority_root or workbench_authority_root(platform_name)
    claim = _quiesce_relinquishment(
        root,
        remove_definition=remove_definition,
        drain_seconds=drain_seconds,
    )
    try:
        with _transition_lock(root, quiesce_claim=claim):
            return _relinquish_ownership_locked(
                platform_name=platform_name,
                remove_definition=remove_definition,
                authority_root=root,
                quiesce_claim=claim,
            )
    except BaseException:
        if claim is not None:
            _clear_quiesce(root, claim, best_effort=True)
        raise


def _relinquish_ownership_locked(
    *,
    platform_name: str,
    remove_definition: bool,
    authority_root: Path,
    quiesce_claim: _WorkbenchQuiesceClaim | None,
) -> WorkbenchOwnershipRecord:
    root = authority_root
    with _ownership_lock(root):
        record = _require_ownership_record(root)
        if record.mode == "relinquished":
            if quiesce_claim is not None:
                raise WorkbenchOwnershipError(
                    "Workbench ownership changed after relinquishment quiesced"
                )
            return record
        if quiesce_claim is None:
            raise WorkbenchOwnershipError(
                "active Workbench ownership requires a completed quiesce drain"
            )
        record = _require_quiesce_drained(root, quiesce_claim)
    _disable_native_service(
        platform_name=platform_name,
        remove_definition=remove_definition,
    )
    with _ownership_lock(root):
        current = _require_quiesce_drained(root, quiesce_claim)
        if current != record:
            raise WorkbenchOwnershipError(
                "Workbench ownership changed before relinquishment committed"
            )
        relinquished = replace(
            current,
            mode="relinquished",
            ownership_revision=current.ownership_revision + 1,
        )
        _write_ownership_record(root, relinquished)
        _clear_quiesce(root, quiesce_claim)
        return relinquished


def workbench_service_status(
    *,
    platform_name: str | None = None,
    authority_root: Path | None = None,
) -> WorkbenchServiceStatus:
    platform_value = current_platform(platform_name)
    definition = service_definition_path(platform_value)
    metadata = _read_metadata()
    authority = managed_workbench_authority(
        platform_name=platform_value,
        authority_root=authority_root,
        include_health=False,
    )
    running: bool | None
    if platform_value == "darwin":
        result = _run(
            ("launchctl", "print", f"{_launchd_domain()}/{SERVICE_LABEL}"),
            check=False,
        )
        loaded = result.returncode == 0
        running = loaded and bool(re.search(r"(?:state\s*=\s*running|pid\s*=\s*\d+)", result.stdout))
        state = "running" if running else "loaded" if loaded else "not loaded"
        installed = definition.exists()
    elif platform_value == "linux":
        if not definition.exists():
            return WorkbenchServiceStatus(
                platform=platform_value,
                installed=False,
                running=False,
                state="not installed",
                definition=definition,
                metadata=metadata,
                ownership=authority,
            )
        _require_systemd_user()
        result = _run(
            ("systemctl", "--user", "is-active", SYSTEMD_UNIT),
            check=False,
        )
        running = result.returncode == 0 and result.stdout.strip() == "active"
        state = result.stdout.strip() or "inactive"
        installed = True
    else:
        result = _run(("schtasks.exe", "/Query", "/TN", WINDOWS_TASK), check=False)
        installed = result.returncode == 0
        running = None if installed else False
        state = "registered with Task Scheduler" if installed else "not installed"
    api_state: str | None = None
    if installed:
        authority = managed_workbench_authority(
            platform_name=platform_value,
            authority_root=authority_root,
        )
        api_state = authority.state
    return WorkbenchServiceStatus(
        platform=platform_value,
        installed=installed,
        running=running,
        state=state,
        definition=definition,
        metadata=metadata,
        api_state=api_state,
        ownership=authority,
    )


def wait_for_managed_workbench(
    bookmark_path: Path,
    *,
    timeout_seconds: float = 10.0,
    poll_interval: float = 0.1,
    platform_name: str | None = None,
    authority_root: Path | None = None,
) -> bool:
    """Wait for the exact configured generation/revision to authenticate."""

    platform_value = current_platform(platform_name)
    root = authority_root or workbench_authority_root(platform_value)
    deadline = time.monotonic() + max(timeout_seconds, 0.0)
    while True:
        target = _managed_bookmark_target(bookmark_path)
        try:
            with _ownership_lock(root):
                state, record = _read_ownership_record(root)
        except WorkbenchOwnershipError:
            state, record = "invalid", None
        if (
            target is not None
            and state == "ok"
            and record is not None
            and record.mode == "configured"
            and record.endpoint.rstrip("/") == target[0]
            and record.bookmark_path
            and Path(record.bookmark_path).absolute() == bookmark_path.absolute()
        ):
            health_state, payload = _managed_health_observation(*target)
            response_identity = payload.get("identity") if isinstance(payload, dict) else None
            identity = _validated_public_identity(
                response_identity,
                expected_api_base=target[0],
                expected_generation=record.generation,
                expected_revision=record.ownership_revision,
            )
            if health_state == "ready" and identity is not None:
                return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(max(poll_interval, 0.01))


def _managed_bookmark_target(bookmark_path: Path) -> tuple[str, str] | None:
    state, raw = _bounded_regular_file(bookmark_path, MAX_MANAGED_BOOKMARK_BYTES)
    if state != "ok" or raw is None:
        return None
    try:
        payload = raw.decode("utf-8")
    except UnicodeDecodeError:
        return None
    if "const MANAGED_SERVICE = true;" not in payload:
        return None
    api_match = None
    token_match = None
    for line in payload.splitlines():
        if api_match is None:
            api_match = re.fullmatch(r"const API_BASE = (.+);", line)
        if token_match is None:
            token_match = re.fullmatch(r"const EMBEDDED_API_TOKEN = (.+);", line)
    if api_match is None or token_match is None:
        return None
    try:
        api_base = json.loads(api_match.group(1))
        token = json.loads(token_match.group(1))
    except json.JSONDecodeError:
        return None
    if not isinstance(api_base, str) or not isinstance(token, str) or not token:
        return None
    parsed = urlparse(api_base)
    if parsed.scheme != "http" or parsed.username or parsed.password or not parsed.hostname:
        return None
    try:
        _validate_loopback_host(parsed.hostname)
        _ = parsed.port
    except (ValueError, WorkbenchServiceError):
        return None
    return api_base.rstrip("/"), token


def _managed_health_ready(api_base: str, token: str) -> bool:
    return _managed_health_state(api_base, token) == "ready"


def _managed_health_state(api_base: str, token: str) -> str:
    state, _payload = _managed_health_observation(api_base, token)
    return state


def _managed_health_observation(api_base: str, token: str) -> tuple[str, dict[str, Any] | None]:
    request = Request(
        f"{api_base}/api/health",
        headers={"X-Agent-Mesh-Token": token},
    )
    try:
        with urlopen(request, timeout=0.5) as response:
            payload = _bounded_health_payload(response)
    except HTTPError as exc:
        try:
            payload = _bounded_health_payload(exc)
        except (OSError, ValueError, UnicodeDecodeError, json.JSONDecodeError):
            return "unavailable", None
        if (
            isinstance(payload, dict)
            and payload.get("code") == "WORKBENCH_RESTART_REQUIRED"
            and payload.get("managed_service") is True
        ):
            return "restart-required", payload
        return "unavailable", payload if isinstance(payload, dict) else None
    except (
        URLError,
        OSError,
        TimeoutError,
        ValueError,
        UnicodeDecodeError,
        json.JSONDecodeError,
    ):
        return "unavailable", None
    if (
        isinstance(payload, dict)
        and payload.get("ok") is True
        and payload.get("managed_service") is True
    ):
        return "ready", payload
    if (
        isinstance(payload, dict)
        and payload.get("code") == "WORKBENCH_RESTART_REQUIRED"
        and payload.get("managed_service") is True
    ):
        return "restart-required", payload
    return "unavailable", payload if isinstance(payload, dict) else None


def managed_workbench_authority(
    *,
    platform_name: str | None = None,
    authority_root: Path | None = None,
    include_health: bool = True,
) -> ManagedWorkbenchAuthority:
    """Resolve persisted ownership and optional authenticated liveness evidence."""

    platform_value = current_platform(platform_name)
    root = authority_root or workbench_authority_root(platform_value)
    if not _path_present(root):
        return _invalid_authority("legacy_uninitialized")
    with _ownership_lock(root):
        state, record = _read_ownership_record(root)
        marker_state, _ = _ownership_high_water_observation(root)
    if state != "ok" or record is None:
        if state == "missing" and marker_state == "missing":
            return _invalid_authority("legacy_uninitialized")
        return _invalid_authority("invalid")
    authority = _authority_from_record(record, state=record.mode)
    if record.mode != "configured":
        return authority
    if not record.bookmark_path or not record.endpoint:
        return _authority_from_record(record, state="configured_unavailable")
    bookmark_path = Path(record.bookmark_path)
    target = _managed_bookmark_target(bookmark_path)
    if target is None or target[0] != record.endpoint.rstrip("/"):
        return _authority_from_record(record, state="configured_unavailable")
    authority = _authority_from_record(
        record,
        state=record.mode,
        expose_bookmark=True,
    )
    if not include_health:
        return authority
    api_base, token = target
    health_state, payload = _managed_health_observation(api_base, token)
    response_identity = payload.get("identity") if isinstance(payload, dict) else None
    identity = _validated_public_identity(
        response_identity,
        expected_api_base=api_base,
        expected_generation=record.generation,
        expected_revision=record.ownership_revision,
    )
    verified = health_state in {"ready", "restart-required"} and identity is not None
    result = _authority_from_record(
        record,
        state=health_state if verified else "configured_unavailable",
        verified=verified,
        expose_bookmark=True,
    )
    if identity is None:
        return result
    return replace(result, identity=identity)


def _managed_metadata_identity(metadata: dict[str, Any]) -> dict[str, str | int]:
    identity: dict[str, str | int] = {"mode": "managed"}
    raw_repo = metadata.get("repo")
    if isinstance(raw_repo, str) and raw_repo.strip():
        identity["anchor_repository"] = raw_repo
    raw_source = metadata.get("source_checkout")
    if isinstance(raw_source, str) and raw_source.strip():
        identity["source_checkout"] = raw_source
    api_base = _metadata_api_base(metadata)
    if api_base:
        identity["endpoint"] = api_base
    raw_port = metadata.get("port")
    if isinstance(raw_port, int) and 1 <= raw_port <= 65535:
        identity["port"] = raw_port
    return identity


def _metadata_api_base(metadata: dict[str, Any]) -> str:
    host = metadata.get("host")
    port = metadata.get("port")
    if not isinstance(host, str) or not host.strip():
        return ""
    if not isinstance(port, int) or not 1 <= port <= 65535:
        return ""
    try:
        _validate_loopback_host(host)
    except WorkbenchServiceError:
        return ""
    return f"http://{host}:{port}"


def _validated_public_identity(
    value: Any,
    *,
    expected_api_base: str,
    expected_generation: str,
    expected_revision: int,
) -> dict[str, str | int] | None:
    if not isinstance(value, dict):
        return None
    mode = value.get("mode")
    endpoint = value.get("endpoint")
    if (
        mode != "managed"
        or endpoint != expected_api_base
        or value.get("ownership_generation") != expected_generation
        or value.get("ownership_revision") != expected_revision
    ):
        return None
    allowed: dict[str, str | int] = {}
    for key in ("mode", "package_root", "source_checkout", "anchor_repository", "endpoint"):
        item = value.get(key)
        if isinstance(item, str) and item.strip() and len(item) <= 4096:
            allowed[key] = item
    port = value.get("port")
    if isinstance(port, int) and 1 <= port <= 65535:
        allowed["port"] = port
    allowed["ownership_generation"] = expected_generation
    allowed["ownership_revision"] = expected_revision
    return allowed if allowed.get("mode") == "managed" else None


def _bounded_health_payload(response: Any) -> Any:
    raw = response.read(MAX_MANAGED_HEALTH_RESPONSE_BYTES + 1)
    if len(raw) > MAX_MANAGED_HEALTH_RESPONSE_BYTES:
        raise ValueError("Workbench health response exceeds its byte bound")
    return json.loads(raw.decode("utf-8"))


def _validate_loopback_host(host: str) -> None:
    normalized = host.strip().casefold()
    if normalized == "localhost":
        return
    try:
        address = ipaddress.ip_address(normalized)
    except ValueError as exc:
        raise WorkbenchServiceError(
            "Workbench services are loopback-only; use 127.0.0.1 or localhost"
        ) from exc
    if address.version != 4 or not address.is_loopback:
        raise WorkbenchServiceError(
            "Workbench services are loopback-only; use 127.0.0.1 or localhost"
        )


def _launchd_domain() -> str:
    if not hasattr(os, "getuid"):
        raise WorkbenchServiceError("launchd user services require a Unix user ID")
    return f"gui/{os.getuid()}"


def _require_systemd_user() -> None:
    result = _run(("systemctl", "--user", "show-environment"), check=False)
    if result.returncode != 0:
        detail = _result_detail(result)
        raise WorkbenchServiceError(
            "systemd user services are unavailable for this login session"
            + (f": {detail}" if detail else "")
        )


def _windows_user_id() -> str:
    result = _run(("whoami.exe",), check=False)
    value = result.stdout.strip()
    if result.returncode == 0 and value:
        return value
    domain = os.environ.get("USERDOMAIN", "").strip()
    username = os.environ.get("USERNAME", "").strip()
    if username:
        return f"{domain}\\{username}" if domain else username
    raise WorkbenchServiceError("could not determine the current Windows user for Task Scheduler")


def _run(command: Sequence[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    _require_active_private_transaction_current()
    try:
        result = subprocess.run(
            list(command),
            capture_output=True,
            text=True,
            check=False,
            timeout=SERVICE_COMMAND_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired as exc:
        raise WorkbenchServiceError(
            f"{command[0]} exceeded the Workbench service command time bound"
        ) from exc
    except OSError as exc:
        raise WorkbenchServiceError(f"could not run {command[0]}: {exc}") from exc
    if check and result.returncode != 0:
        detail = _result_detail(result)
        raise WorkbenchServiceError(
            f"{command[0]} failed with exit code {result.returncode}"
            + (f": {detail}" if detail else "")
        )
    return result


def _result_detail(result: subprocess.CompletedProcess[str]) -> str:
    return (result.stderr.strip() or result.stdout.strip()).splitlines()[-1][:500] if (
        result.stderr.strip() or result.stdout.strip()
    ) else ""


def _atomic_write(path: Path, payload: bytes, *, mode: int) -> None:
    _require_active_private_transaction_current()
    _ensure_private_dir(path.parent)
    with _open_private_directory(path.parent) as parent_fd:
        temporary_name = f".{path.name}.{secrets.token_urlsafe(16)}"
        fd = -1
        try:
            fd = os.open(
                temporary_name,
                os.O_WRONLY
                | os.O_CREAT
                | os.O_EXCL
                | os.O_NOFOLLOW
                | getattr(os, "O_CLOEXEC", 0),
                mode,
                dir_fd=parent_fd,
            )
            with os.fdopen(fd, "wb") as handle:
                fd = -1
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(
                temporary_name,
                path.name,
                src_dir_fd=parent_fd,
                dst_dir_fd=parent_fd,
            )
            os.chmod(path.name, mode, dir_fd=parent_fd, follow_symlinks=False)
            os.fsync(parent_fd)
        finally:
            if fd >= 0:
                os.close(fd)
            try:
                os.unlink(temporary_name, dir_fd=parent_fd)
            except FileNotFoundError:
                pass


def _ensure_private_dir(path: Path) -> None:
    _require_active_private_transaction_current()
    try:
        with _open_private_directory(path, create=True) as directory_fd:
            if hasattr(os, "fchmod"):
                os.fchmod(directory_fd, 0o700)
    except (OSError, WorkbenchOwnershipError) as exc:
        raise WorkbenchOwnershipError(
            f"Workbench private directory is invalid: {path}: {exc}"
        ) from exc


def _write_metadata(payload: dict[str, Any]) -> None:
    serialized = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")
    _atomic_write(service_metadata_path(), serialized, mode=0o600)


def _read_metadata() -> dict[str, Any] | None:
    path = service_metadata_path()
    state, payload = _bounded_regular_file(path, MAX_SERVICE_METADATA_BYTES)
    if state != "ok" or payload is None:
        return None
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(value, dict) or value.get("schema_version") != SERVICE_SCHEMA_VERSION:
        return None
    return value


def _managed_bookmark_path(metadata: dict[str, Any] | None) -> Path:
    raw_config_home = metadata.get("config_home") if metadata else None
    if isinstance(raw_config_home, str) and raw_config_home.strip():
        return Path(raw_config_home).expanduser().absolute() / "workbench.html"
    return registry_dir() / "workbench.html"
