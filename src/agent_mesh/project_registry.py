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
    name: str
    root: Path

    def as_dict(self) -> dict[str, str]:
        return {"id": self.id, "name": self.name, "root": str(self.root)}


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
    digest = hashlib.sha256(str(canonical).encode("utf-8")).hexdigest()[:16]
    return f"repo-{digest}"


def register_project(repo: str | Path) -> RegisteredProject:
    config = load_config(repo)
    validate_registered_project_storage(config)
    project = RegisteredProject(
        id=project_id(config.project_root),
        name=config.project_name or config.project_root.name,
        root=config.project_root.resolve(),
    )
    path = registry_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    lock = acquire(path.parent / ".projects-lock")
    try:
        records = _read_records(path)
        records = [record for record in records if record.get("id") != project.id]
        records.append(project.as_dict())
        _write_records(path, records)
    finally:
        lock.release()
    return project


def unregister_project(repo: str | Path) -> bool:
    identifier = project_id(repo)
    path = registry_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    lock = acquire(path.parent / ".projects-lock")
    try:
        records = _read_records(path)
        kept = [record for record in records if record.get("id") != identifier]
        if len(kept) == len(records):
            return False
        _write_records(path, kept)
    finally:
        lock.release()
    return True


def list_registered_projects() -> list[RegisteredProject]:
    """Return valid registered projects; stale or malformed entries are ignored."""
    projects: list[RegisteredProject] = []
    seen: set[str] = set()
    for record in _read_records(registry_path()):
        raw_root = record.get("root")
        if not isinstance(raw_root, str) or not raw_root.strip():
            continue
        root = Path(raw_root).expanduser().resolve()
        identifier = project_id(root)
        if identifier in seen or record.get("id") != identifier:
            continue
        try:
            config = load_config(root)
            validate_registered_project_storage(config)
        except (ConfigError, OSError, ProjectRegistryError, tomllib.TOMLDecodeError):
            continue
        seen.add(identifier)
        projects.append(
            RegisteredProject(
                id=identifier,
                name=config.project_name or config.project_root.name,
                root=config.project_root.resolve(),
            )
        )
    return sorted(projects, key=lambda item: (item.name.casefold(), str(item.root)))


def resolve_registered_project(identifier: str) -> RegisteredProject:
    for project in list_registered_projects():
        if project.id == identifier:
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
            f"Unsupported project registry schema in {path}; "
            f"expected {REGISTRY_SCHEMA_VERSION}"
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
