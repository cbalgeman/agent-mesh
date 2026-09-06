"""Machine-local registry of agent-mesh projects."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import tempfile
import time
import tomllib
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterator

from agent_mesh.config import ConfigError, load_config
from agent_mesh.core.lock import acquire


REGISTRY_SCHEMA_VERSION = 1
MAX_PROJECT_REGISTRY_BYTES = 2 * 1024 * 1024
_OPEN_SUPPORTS_DIR_FD = os.open in os.supports_dir_fd
_STAT_SUPPORTS_DIR_FD = os.stat in os.supports_dir_fd


class ProjectRegistryError(RuntimeError):
    """Raised when the machine-local project registry is invalid."""


@dataclass(frozen=True)
class RegisteredProject:
    id: str
    key: str
    name: str
    root: Path

    def as_dict(self) -> dict[str, str]:
        return {
            "id": self.id,
            "key": self.key,
            "name": self.name,
            "root": str(self.root),
        }


@dataclass(frozen=True)
class ProjectUnregisterPlan:
    registry: Path
    root: Path
    registry_sha256: str
    records: tuple[tuple[tuple[str, str], ...], ...]

    @property
    def record_ids(self) -> tuple[str, ...]:
        return tuple(dict(record).get("id", "") for record in self.records)


def registry_dir() -> Path:
    override = os.environ.get("AGENT_MESH_CONFIG_HOME", "").strip()
    if override:
        return Path(override).expanduser().absolute()
    xdg_home = os.environ.get("XDG_CONFIG_HOME", "").strip()
    if xdg_home:
        return (Path(xdg_home).expanduser() / "agent-mesh").absolute()
    return (Path.home() / ".config" / "agent-mesh").absolute()


def registry_path() -> Path:
    return registry_dir() / "projects.toml"


def project_id(root: str | Path) -> str:
    canonical = Path(root).expanduser().resolve()
    try:
        config = load_config(canonical)
    except (ConfigError, OSError, tomllib.TOMLDecodeError):
        config = None
    if config is not None and config.store_id:
        return config.store_id
    return legacy_project_id(canonical)


def legacy_project_id(root: str | Path) -> str:
    """Return the pre-store-identity ID used by legacy registry rows."""
    canonical = Path(root).expanduser().resolve()
    digest = hashlib.sha256(str(canonical).encode("utf-8")).hexdigest()[:16]
    return f"repo-{digest}"


def register_project(repo: str | Path) -> RegisteredProject:
    config = load_config(repo)
    validate_registered_project_storage(config)
    project = RegisteredProject(
        id=config.store_id or project_id(config.project_root),
        key=config.project_key,
        name=config.project_name or config.project_root.name,
        root=config.project_root.resolve(),
    )
    path = registry_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    lock = acquire(path.parent / ".projects-lock")
    try:
        records = _read_records(path)
        for record in records:
            if record.get("key") == project.key and record.get("id") != project.id:
                raise ProjectRegistryError(
                    f"project key {project.key!r} is already registered to another store"
                )
            if record.get("id") != project.id:
                continue
            claimed_root = _record_root(record)
            if claimed_root == project.root:
                continue
            if claimed_root is None or claimed_root.exists():
                claimed_at = (
                    json.dumps(str(claimed_root), ensure_ascii=True)
                    if claimed_root is not None
                    else "an invalid root"
                )
                raise ProjectRegistryError(
                    f"store ID {project.id!r} is already registered at {claimed_at}; "
                    "refusing to reassign a live or unresolved project"
                )
        records = [
            record
            for record in records
            if record.get("id") != project.id and _record_root(record) != project.root
        ]
        records.append(project.as_dict())
        _write_records(path, records)
    finally:
        lock.release()
    return project


def unregister_project(repo: str | Path) -> bool:
    plan = prepare_project_unregistration(repo)
    return apply_project_unregistration(plan)


def prepare_project_unregistration(repo: str | Path) -> ProjectUnregisterPlan:
    root = Path(repo).expanduser().resolve()
    identifiers = {project_id(root), legacy_project_id(root)}
    path = registry_path()
    registry_bytes = path.read_bytes() if path.exists() else b""
    registry_sha256 = hashlib.sha256(registry_bytes).hexdigest()
    records = _records_from_bytes(path, registry_bytes)
    for record in records:
        record_root = _record_root(record)
        record_id = record.get("id")
        if record_id in identifiers and record_root != root:
            claimed_at = (
                json.dumps(str(record_root), ensure_ascii=True)
                if record_root is not None
                else "an invalid root"
            )
            raise ProjectRegistryError(
                f"project registry ID {record_id!r} is also claimed at {claimed_at}; "
                "refusing destructive unregistration"
            )
        if record_root == root and record_id not in identifiers:
            raise ProjectRegistryError(
                f"project registry root {json.dumps(str(root), ensure_ascii=True)} "
                f"has unexpected ID {record_id!r}; "
                "refusing destructive unregistration"
            )
    matched = [
        record
        for record in records
        if record.get("id") in identifiers and _record_root(record) == root
    ]
    frozen_records = tuple(
        tuple(sorted((str(key), str(value)) for key, value in record.items()))
        for record in matched
    )
    return ProjectUnregisterPlan(
        registry=path,
        root=root,
        registry_sha256=registry_sha256,
        records=frozen_records,
    )


def apply_project_unregistration(plan: ProjectUnregisterPlan) -> bool:
    path = plan.registry
    path.parent.mkdir(parents=True, exist_ok=True)
    lock = acquire(path.parent / ".projects-lock")
    try:
        registry_bytes = path.read_bytes() if path.exists() else b""
        if hashlib.sha256(registry_bytes).hexdigest() != plan.registry_sha256:
            raise ProjectRegistryError(
                "project registry changed after confirmation; review the current entry and retry"
            )
        records = _records_from_bytes(path, registry_bytes)
        expected = set(plan.records)
        kept = [
            record
            for record in records
            if tuple(sorted((str(key), str(value)) for key, value in record.items())) not in expected
        ]
        if len(kept) == len(records):
            return False
        _write_records(path, kept)
    finally:
        lock.release()
    return True


def list_registered_projects() -> list[RegisteredProject]:
    """Return valid projects and atomically upgrade same-root legacy rows."""
    path = registry_path()
    if not path.exists():
        return []

    path.parent.mkdir(parents=True, exist_ok=True)
    lock = acquire(path.parent / ".projects-lock")
    try:
        records = _read_records(path)
        projects, migrated_records, changed = _projects_and_migrated_records(records)
        if changed:
            _write_records(path, migrated_records)
    finally:
        lock.release()
    return sorted(projects, key=lambda item: (item.name.casefold(), str(item.root)))


def list_registered_projects_bounded(
    *,
    max_entries: int,
    deadline: float,
    max_bytes: int = MAX_PROJECT_REGISTRY_BYTES,
) -> list[RegisteredProject]:
    """Read valid projects without migration under one byte/entry/time envelope."""

    if max_entries < 0 or max_bytes < 1:
        raise ProjectRegistryError("Invalid bounded project-registry limits")
    path = registry_path()
    payload = _bounded_registry_bytes(path, maximum=max_bytes)
    if time.monotonic() >= deadline:
        raise ProjectRegistryError("Project registry enumeration exceeded its time bound")
    records = _records_from_bytes(path, payload, max_entries=max_entries)
    if time.monotonic() >= deadline:
        raise ProjectRegistryError("Project registry enumeration exceeded its time bound")
    projects: list[RegisteredProject] = []
    seen: set[str] = set()
    for record in records:
        if time.monotonic() >= deadline:
            raise ProjectRegistryError("Project registry enumeration exceeded its time bound")
        root = _record_root(record)
        if root is None:
            continue
        try:
            config = load_config(root)
            validate_registered_project_storage(config)
        except (ConfigError, OSError, ProjectRegistryError, tomllib.TOMLDecodeError):
            continue
        if time.monotonic() >= deadline:
            raise ProjectRegistryError("Project registry enumeration exceeded its time bound")
        identifier = config.store_id or legacy_project_id(config.project_root)
        record_id = record.get("id")
        legacy_id = legacy_project_id(root)
        if record_id not in {identifier, legacy_id} or identifier in seen:
            continue
        if record_id == legacy_id and _record_id_claimed_elsewhere(
            records,
            identifier=identifier,
            root=config.project_root.resolve(),
        ):
            continue
        seen.add(identifier)
        projects.append(
            RegisteredProject(
                id=identifier,
                key=config.project_key,
                name=config.project_name or config.project_root.name,
                root=config.project_root.resolve(),
            )
        )
    return sorted(projects, key=lambda item: (item.name.casefold(), str(item.root)))


def _bounded_registry_bytes(path: Path, *, maximum: int) -> bytes:
    flags = (
        os.O_RDONLY
        | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_CLOEXEC", 0)
    )
    try:
        with _open_registry_directory(path.parent) as parent_fd:
            before = os.stat(path.name, dir_fd=parent_fd, follow_symlinks=False)
            if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
                raise ProjectRegistryError(f"Project registry is not a regular file: {path}")
            fd = os.open(path.name, flags, dir_fd=parent_fd)
    except FileNotFoundError:
        return b""
    except ProjectRegistryError:
        raise
    except OSError as exc:
        raise ProjectRegistryError(f"Cannot read project registry {path}: {exc}") from exc
    try:
        opened = os.fstat(fd)
        if opened.st_size > maximum:
            raise ProjectRegistryError(
                f"Project registry exceeds its byte bound ({maximum})"
            )
        chunks: list[bytes] = []
        remaining = maximum + 1
        while remaining > 0:
            chunk = os.read(fd, min(remaining, 64 * 1024))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        after = os.fstat(fd)
    finally:
        os.close(fd)
    payload = b"".join(chunks)
    if len(payload) > maximum:
        raise ProjectRegistryError(f"Project registry exceeds its byte bound ({maximum})")
    if (
        before.st_dev != opened.st_dev
        or before.st_ino != opened.st_ino
        or opened.st_dev != after.st_dev
        or opened.st_ino != after.st_ino
        or opened.st_size != after.st_size
        or opened.st_mtime_ns != after.st_mtime_ns
    ):
        raise ProjectRegistryError("Project registry changed while it was read")
    return payload


@contextmanager
def _open_registry_directory(path: Path) -> Iterator[int]:
    """Walk a registry parent through retained POSIX no-follow descriptors."""

    if not (
        os.name == "posix"
        and hasattr(os, "O_DIRECTORY")
        and hasattr(os, "O_NOFOLLOW")
        and _OPEN_SUPPORTS_DIR_FD
        and _STAT_SUPPORTS_DIR_FD
    ):
        raise ProjectRegistryError(
            "Bounded managed Workbench project routing requires POSIX no-follow descriptors"
        )
    absolute = path.expanduser().absolute()
    if not absolute.is_absolute() or absolute.anchor != os.sep:
        raise ProjectRegistryError(f"Project registry parent must be absolute: {path}")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    current_fd = os.open(absolute.anchor, flags)
    try:
        for part in absolute.parts[1:]:
            if part in {"", ".", ".."} or Path(part).name != part:
                raise ProjectRegistryError(f"Project registry parent is invalid: {path}")
            next_fd = os.open(part, flags, dir_fd=current_fd)
            info = os.fstat(next_fd)
            if not stat.S_ISDIR(info.st_mode):
                os.close(next_fd)
                raise ProjectRegistryError(
                    f"Project registry parent component is not a directory: {part}"
                )
            os.close(current_fd)
            current_fd = next_fd
        yield current_fd
    finally:
        os.close(current_fd)


def _projects_and_migrated_records(
    records: list[dict[str, Any]],
) -> tuple[list[RegisteredProject], list[dict[str, Any]], bool]:
    projects: list[RegisteredProject] = []
    seen: set[str] = set()
    migrated_records = [dict(record) for record in records]
    changed = False
    for index, record in enumerate(records):
        root = _record_root(record)
        if root is None:
            continue
        try:
            config = load_config(root)
            validate_registered_project_storage(config)
        except (ConfigError, OSError, ProjectRegistryError, tomllib.TOMLDecodeError):
            continue
        project = RegisteredProject(
            id=config.store_id or project_id(config.project_root),
            key=config.project_key,
            name=config.project_name or config.project_root.name,
            root=config.project_root.resolve(),
        )
        record_id = record.get("id")
        legacy_id = legacy_project_id(root)
        if record_id not in {project.id, legacy_id}:
            continue
        if project.id in seen:
            continue
        if record_id == legacy_id and _record_id_claimed_elsewhere(
            records,
            identifier=project.id,
            root=project.root,
        ):
            continue

        canonical_record = project.as_dict()
        if migrated_records[index] != canonical_record:
            migrated_records[index] = canonical_record
            changed = True
        seen.add(project.id)
        projects.append(project)

    return projects, migrated_records, changed


def _record_root(record: dict[str, Any]) -> Path | None:
    raw_root = record.get("root")
    if not isinstance(raw_root, str) or not raw_root.strip():
        return None
    return Path(raw_root).expanduser().resolve()


def _record_id_claimed_elsewhere(
    records: list[dict[str, Any]],
    *,
    identifier: str,
    root: Path,
) -> bool:
    for record in records:
        if record.get("id") != identifier:
            continue
        claimed_root = _record_root(record)
        if claimed_root is None or claimed_root != root:
            return True
    return False


def resolve_registered_project(identifier: str) -> RegisteredProject:
    for project in list_registered_projects():
        if project.id == identifier or project.key == identifier:
            return project
    raise ProjectRegistryError(f"Unknown or unavailable registered repo: {identifier}")


def validate_registered_project_storage(config: Any) -> None:
    """Require registered Workbench state to remain physically inside its repo."""
    paths = {
        ".agent-mesh": config.agent_dir,
        "config": config.config_path,
        "events": config.events_path,
        "database": config.db_path,
        "views": config.views_dir,
        "archive": config.archive_dir,
        "bodies": config.bodies_dir,
    }
    for label, path in paths.items():
        validate_registered_project_path(config.project_root, path, label=label)


def validate_registered_project_path(
    project_root: str | Path,
    path: str | Path,
    *,
    label: str,
) -> Path:
    """Resolve a Workbench path and reject containment or symlink escapes."""
    root = Path(project_root).expanduser().resolve()
    candidate = Path(path).expanduser()
    if not candidate.is_absolute():
        candidate = root / candidate
    try:
        relative = candidate.absolute().relative_to(root)
    except ValueError as exc:
        raise ProjectRegistryError(
            f"Registered repo {label} path must stay under {root}: {candidate}"
        ) from exc

    current = root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise ProjectRegistryError(
                f"Registered repo {label} path must not use symlinks: {current}"
            )

    resolved = candidate.resolve()
    if resolved != root and root not in resolved.parents:
        raise ProjectRegistryError(
            f"Registered repo {label} path must stay under {root}: {resolved}"
        )
    return resolved


def _read_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    try:
        payload = path.read_bytes()
    except OSError as exc:
        raise ProjectRegistryError(f"Cannot read project registry {path}: {exc}") from exc
    return _records_from_bytes(path, payload)


def _records_from_bytes(
    path: Path,
    payload: bytes,
    *,
    max_entries: int | None = None,
) -> list[dict[str, Any]]:
    if not payload:
        return []
    try:
        data = tomllib.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise ProjectRegistryError(f"Cannot read project registry {path}: {exc}") from exc
    if data.get("schema_version") != REGISTRY_SCHEMA_VERSION:
        raise ProjectRegistryError(
            f"Unsupported project registry schema in {path}; expected {REGISTRY_SCHEMA_VERSION}"
        )
    raw_projects = data.get("projects", [])
    if not isinstance(raw_projects, list):
        raise ProjectRegistryError(f"Invalid projects list in {path}")
    if max_entries is not None and len(raw_projects) > max_entries:
        raise ProjectRegistryError(
            f"Project registry exceeds its entry bound ({max_entries})"
        )
    return [dict(item) for item in raw_projects if isinstance(item, dict)]


def _write_records(path: Path, records: list[dict[str, Any]]) -> None:
    normalized = sorted(records, key=lambda item: str(item.get("root", "")))
    lines = [f"schema_version = {REGISTRY_SCHEMA_VERSION}", ""]
    for record in normalized:
        lines.extend(
            [
                "[[projects]]",
                f"id = {json.dumps(str(record.get('id', '')))}",
                f"key = {json.dumps(str(record.get('key', '')))}",
                f"name = {json.dumps(str(record.get('name', '')))}",
                f"root = {json.dumps(str(record.get('root', '')))}",
                "",
            ]
        )
    payload = "\n".join(lines)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix="projects-", suffix=".toml", dir=path.parent)
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()
