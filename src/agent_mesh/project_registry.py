"""Machine-local registry of agent-mesh projects."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from agent_mesh.config import ConfigError, load_config
from agent_mesh.core.lock import acquire


REGISTRY_SCHEMA_VERSION = 1


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


def registry_dir() -> Path:
    override = os.environ.get("AGENT_MESH_CONFIG_HOME", "").strip()
    if override:
        return Path(override).expanduser().resolve()
    xdg_home = os.environ.get("XDG_CONFIG_HOME", "").strip()
    if xdg_home:
        return (Path(xdg_home).expanduser() / "agent-mesh").resolve()
    return (Path.home() / ".config" / "agent-mesh").resolve()


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
                claimed_at = str(claimed_root) if claimed_root is not None else "an invalid root"
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
    root = Path(repo).expanduser().resolve()
    identifiers = {project_id(root), legacy_project_id(root)}
    path = registry_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    lock = acquire(path.parent / ".projects-lock")
    try:
        records = _read_records(path)
        kept = [
            record
            for record in records
            if record.get("id") not in identifiers and _record_root(record) != root
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
        with path.open("rb") as handle:
            data = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ProjectRegistryError(f"Cannot read project registry {path}: {exc}") from exc
    if data.get("schema_version") != REGISTRY_SCHEMA_VERSION:
        raise ProjectRegistryError(
            f"Unsupported project registry schema in {path}; expected {REGISTRY_SCHEMA_VERSION}"
        )
    raw_projects = data.get("projects", [])
    if not isinstance(raw_projects, list):
        raise ProjectRegistryError(f"Invalid projects list in {path}")
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
