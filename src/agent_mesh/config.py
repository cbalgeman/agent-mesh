"""Project configuration and discovery for agent-mesh."""

from __future__ import annotations

import fnmatch
import hashlib
import json
import os
import re
import tempfile
import tomllib
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from agent_mesh.core.ids import new_ulid
from agent_mesh.core.lock import acquire


DEFAULT_CONFIG_NAME = "config.toml"
STATE_SHARING_LOCAL_ONLY = "local-only"
STATE_SHARING_GIT_SHARED = "git-shared"
STATE_SHARING_CHOICES = (STATE_SHARING_LOCAL_ONLY, STATE_SHARING_GIT_SHARED)
RUNTIME_CAPABILITY_CHOICES = ("repository", "tools", "network")
PROJECT_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")
STORE_ID_RE = re.compile(r"^store_[0-9A-HJKMNP-TV-Z]{26}$")


class ConfigError(RuntimeError):
    """Raised when project configuration is missing or invalid."""


@dataclass(frozen=True)
class ProjectIdentityUpdate:
    changed: bool
    timezone: str
    project_key: str
    store_id: str


@dataclass(frozen=True)
class CompatibilityViews:
    inbox: Path | None = None
    outbox: dict[str, Path] = field(default_factory=dict)
    message_log: Path | None = None
    archive_dir: Path | None = None


@dataclass(frozen=True)
class RoutingConfig:
    aliases: dict[str, list[str]] = field(default_factory=dict)
    preserve_raw_to: bool = True


@dataclass(frozen=True)
class ChecksConfig:
    exempt_paths: list[str] = field(default_factory=list)

    def is_exempt(self, path: str) -> bool:
        return any(fnmatch.fnmatch(path, pattern) for pattern in self.exempt_paths)


@dataclass(frozen=True)
class WorkbenchConfig:
    decision_date_min_utc: str | None = None
    decision_date_year_corrections: dict[str, str] = field(default_factory=dict)
    decision_date_placeholders: list[str] = field(default_factory=lambda: ["1970-01-01T00:00:00Z"])


@dataclass(frozen=True)
class AdapterDeclaration:
    name: str
    class_path: str
    domain: str
    privacy_class: str
    enabled: bool = True
    options: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RuntimeProfile:
    """One explicitly enabled executable identity for a participant target."""

    name: str
    target: str
    provider: str
    adapter: str
    binary: str
    version: str
    model: str
    role: str
    permission_mode: str
    repository_scope: str
    required_capabilities: tuple[str, ...]
    authentication_mode: str
    billing_mode: str
    credential_denylist: tuple[str, ...]
    enabled: bool = False


@dataclass(frozen=True)
class ProjectPaths:
    events_log: Path = Path(".agent-mesh/events.jsonl")
    db: Path = Path(".agent-mesh/messages.db")
    views_dir: Path = Path(".agent-mesh/views")
    archive_dir: Path = Path(".agent-mesh/archive")
    bodies_dir: Path = Path(".agent-mesh/bodies")


@dataclass(frozen=True)
class AgentMeshConfig:
    project_root: Path
    agent_dir: Path
    schema_version: int = 1
    project_name: str = ""
    project_timezone: str = "UTC"
    project_key: str = "project"
    store_id: str = ""
    participants: list[str] = field(default_factory=lambda: ["user", "agent"])
    default_sender: str = "human"
    default_recipient: str = "agent"
    body_externalization: bool = False
    state_sharing: str = STATE_SHARING_LOCAL_ONLY
    paths: ProjectPaths = field(default_factory=ProjectPaths)
    compatibility_views: CompatibilityViews = field(default_factory=CompatibilityViews)
    routing: RoutingConfig = field(default_factory=RoutingConfig)
    checks: ChecksConfig = field(default_factory=ChecksConfig)
    workbench: WorkbenchConfig = field(default_factory=WorkbenchConfig)
    adapters: dict[str, AdapterDeclaration] = field(default_factory=dict)
    runtime_profiles: dict[str, RuntimeProfile] = field(default_factory=dict)

    @property
    def config_path(self) -> Path:
        return self.agent_dir / DEFAULT_CONFIG_NAME

    @property
    def events_path(self) -> Path:
        return self.resolve_project_path(self.paths.events_log)

    @property
    def db_path(self) -> Path:
        return self.resolve_project_path(self.paths.db)

    @property
    def bodies_dir(self) -> Path:
        return self.resolve_project_path(self.paths.bodies_dir)

    @property
    def views_dir(self) -> Path:
        return self.resolve_project_path(self.paths.views_dir)

    @property
    def archive_dir(self) -> Path:
        return self.resolve_project_path(self.paths.archive_dir)

    def resolve_project_path(self, value: str | Path) -> Path:
        path = Path(value)
        if path.is_absolute():
            return path
        return self.project_root / path

    def canonical_recipients(self, raw_to: str) -> list[str]:
        return self.routing.aliases.get(raw_to, [raw_to])

    def runtime_profile_for_target(self, target: str) -> RuntimeProfile:
        matches = [
            profile
            for profile in self.runtime_profiles.values()
            if profile.enabled and profile.target == target
        ]
        if not matches:
            raise ConfigError(
                f"no enabled dispatch runtime profile for participant {target!r}; "
                "adding a participant does not enable live execution"
            )
        if len(matches) > 1:
            names = ", ".join(sorted(profile.name for profile in matches))
            raise ConfigError(
                f"multiple enabled dispatch runtime profiles for participant {target!r}: {names}"
            )
        return matches[0]


def find_project_root(start: str | Path | None = None) -> Path:
    """Find the nearest ancestor containing `.agent-mesh`."""
    current = Path(start or Path.cwd()).resolve()
    if current.is_file():
        current = current.parent
    for candidate in (current, *current.parents):
        if (candidate / ".agent-mesh").exists():
            return candidate
    raise ConfigError("agent-mesh project not found; run `agent-mesh init` first")


def load_config(start: str | Path | None = None) -> AgentMeshConfig:
    root = find_project_root(start)
    agent_dir = root / ".agent-mesh"
    config_path = agent_dir / DEFAULT_CONFIG_NAME
    if not config_path.exists():
        return AgentMeshConfig(project_root=root, agent_dir=agent_dir)

    with config_path.open("rb") as handle:
        data = tomllib.load(handle)

    schema_version = int(data.get("schema_version", 1))
    if schema_version != 1:
        raise ConfigError(f"unsupported config schema_version={schema_version}")

    project_data = _table(data.get("project", {}), "project")
    agents_data = _table(data.get("agents", {}), "agents")
    features_data = _table(data.get("features", {}), "features")
    paths_data = _table(data.get("paths", {}), "paths")
    version_control_data = _table(data.get("version_control", {}), "version_control")

    project_name = str(project_data.get("name", root.name))
    project_timezone = _project_timezone(
        project_data.get("timezone", "UTC"),
        "project.timezone",
    )
    project_key = _project_key(
        project_data.get("key", project_key_from_name(project_name or root.name)),
        "project.key",
    )
    store_id = _store_id_or_legacy(
        project_data.get("store_id", _legacy_project_id(root)),
        "project.store_id",
    )
    participants = _list_of_strings(
        agents_data.get("participants", data.get("participants", ["user", "agent"])),
        "agents.participants",
    )
    default_sender = str(project_data.get("default_sender", data.get("default_sender", "human")))
    default_recipient = str(
        project_data.get(
            "default_recipient",
            data.get("default_recipient", participants[0] if participants else "agent"),
        )
    )
    body_externalization = bool(
        features_data.get("body_externalization", data.get("body_externalization", False))
    )
    # Configs created before state sharing was explicit used the Git-shared policy.
    # Preserve that behavior so an upgrade never creates a false local-only signal.
    state_sharing = _state_sharing(
        version_control_data.get("state_sharing", STATE_SHARING_GIT_SHARED),
        "version_control.state_sharing",
    )
    paths = ProjectPaths(
        events_log=_config_path(
            paths_data.get("events_log", ".agent-mesh/events.jsonl"), "paths.events_log"
        ),
        db=_config_path(paths_data.get("db", ".agent-mesh/messages.db"), "paths.db"),
        views_dir=_config_path(paths_data.get("views_dir", ".agent-mesh/views"), "paths.views_dir"),
        archive_dir=_config_path(
            paths_data.get("archive_dir", ".agent-mesh/archive"), "paths.archive_dir"
        ),
        bodies_dir=_config_path(
            paths_data.get("bodies_dir", ".agent-mesh/bodies"), "paths.bodies_dir"
        ),
    )

    compat_data = data.get("compatibility_views", {})
    if not isinstance(compat_data, dict):
        raise ConfigError("[compatibility_views] must be a table")
    compat = CompatibilityViews(
        inbox=_optional_compat_path(root, compat_data.get("inbox"), "compatibility_views.inbox"),
        outbox={
            str(key): _required_compat_path(root, value, f"compatibility_views.outbox.{key}")
            for key, value in dict(compat_data.get("outbox", {})).items()
        },
        message_log=_optional_compat_path(
            root, compat_data.get("message_log"), "compatibility_views.message_log"
        ),
        archive_dir=_optional_compat_dir_path(
            root, compat_data.get("archive_dir"), "compatibility_views.archive_dir"
        ),
    )

    routing_data = data.get("routing", {})
    if not isinstance(routing_data, dict):
        raise ConfigError("[routing] must be a table")
    aliases_data = routing_data.get("aliases", {})
    if not isinstance(aliases_data, dict):
        raise ConfigError("[routing.aliases] must be a table")
    aliases = {
        str(key): _list_of_strings(value, f"routing.aliases.{key}")
        for key, value in aliases_data.items()
    }
    routing = RoutingConfig(
        aliases=aliases,
        preserve_raw_to=bool(routing_data.get("preserve_raw_to", True)),
    )
    _validate_routing(participants, aliases)

    checks_data = data.get("checks", {})
    if not isinstance(checks_data, dict):
        raise ConfigError("[checks] must be a table")
    checks = ChecksConfig(
        exempt_paths=_list_of_strings(
            checks_data.get(
                "exempt_paths",
                [
                    ".agent-mesh/**",
                    ".git/**",
                    "**/__pycache__/**",
                    "build/**",
                    "dist/**",
                ],
            ),
            "checks.exempt_paths",
        )
    )

    workbench_data = data.get("workbench", {})
    if not isinstance(workbench_data, dict):
        raise ConfigError("[workbench] must be a table")
    workbench = WorkbenchConfig(
        decision_date_min_utc=_optional_string(
            workbench_data.get("decision_date_min_utc"),
            "workbench.decision_date_min_utc",
        ),
        decision_date_year_corrections=_dict_of_strings(
            workbench_data.get("decision_date_year_corrections", {}),
            "workbench.decision_date_year_corrections",
        ),
        decision_date_placeholders=_list_of_strings(
            workbench_data.get(
                "decision_date_placeholders",
                ["1970-01-01T00:00:00Z"],
            ),
            "workbench.decision_date_placeholders",
        ),
    )

    adapters = _adapter_declarations(data.get("adapters", None))
    dispatch_data = _table(data.get("dispatch", {}), "dispatch")
    runtime_profiles = _runtime_profiles(
        dispatch_data.get("runtime_profiles", {}),
        participants=participants,
    )

    return AgentMeshConfig(
        project_root=root,
        agent_dir=agent_dir,
        schema_version=schema_version,
        project_name=project_name,
        project_timezone=project_timezone,
        project_key=project_key,
        store_id=store_id,
        participants=participants,
        default_sender=default_sender,
        default_recipient=default_recipient,
        body_externalization=body_externalization,
        state_sharing=state_sharing,
        paths=paths,
        compatibility_views=compat,
        routing=routing,
        checks=checks,
        workbench=workbench,
        adapters=adapters,
        runtime_profiles=runtime_profiles,
    )


def default_config_text(
    *,
    participants: list[str] | None = None,
    default_sender: str = "human",
    default_recipient: str | None = None,
    state_sharing: str = STATE_SHARING_LOCAL_ONLY,
    project_name: str = "",
    project_timezone: str | None = None,
    project_key: str | None = None,
    store_id: str | None = None,
) -> str:
    people = participants or ["user", "agent"]
    recipient = default_recipient or ("agent" if "agent" in people else people[0])
    participants_json = json.dumps(people)
    sharing = _state_sharing(state_sharing, "version_control.state_sharing")
    timezone_name = _project_timezone(
        project_timezone or detect_local_timezone(),
        "project.timezone",
    )
    key = _project_key(
        project_key or project_key_from_name(project_name or "project"),
        "project.key",
    )
    identity = _store_id_or_legacy(store_id or new_ulid("store"), "project.store_id")
    return (
        "schema_version = 1\n"
        "\n"
        "[project]\n"
        f"name = {json.dumps(project_name)}\n"
        f"timezone = {json.dumps(timezone_name)}\n"
        f"key = {json.dumps(key)}\n"
        f"store_id = {json.dumps(identity)}\n"
        f"default_sender = {json.dumps(default_sender)}\n"
        f"default_recipient = {json.dumps(recipient)}\n"
        "\n"
        "[agents]\n"
        f"participants = {participants_json}\n"
        "\n"
        "[features]\n"
        "hash_chain = true\n"
        "body_externalization = false\n"
        "\n"
        "[version_control]\n"
        f"state_sharing = {json.dumps(sharing)}\n"
        "\n"
        "[paths]\n"
        'events_log = ".agent-mesh/events.jsonl"\n'
        'db = ".agent-mesh/messages.db"\n'
        'views_dir = ".agent-mesh/views"\n'
        'archive_dir = ".agent-mesh/archive"\n'
        'bodies_dir = ".agent-mesh/bodies"\n'
        "\n"
        "[routing]\n"
        "preserve_raw_to = true\n"
        "\n"
        "[routing.aliases]\n"
        "\n"
        "[checks]\n"
        'exempt_paths = [".agent-mesh/**", ".git/**", "**/__pycache__/**", '
        '"build/**", "dist/**"]\n'
        "\n"
        "[compatibility_views]\n"
        "\n"
        "[compatibility_views.outbox]\n"
        "\n"
        "[workbench]\n"
        "\n"
        "[dispatch]\n"
        "\n"
        "[dispatch.runtime_profiles]\n"
        "\n"
        "[adapters.message_lookup]\n"
        'class = "agent_mesh.adapters.default.DefaultMessageLookupAdapter"\n'
        'domain = "mail"\n'
        'privacy_class = "project_private"\n'
        "enabled = true\n"
        "\n"
        "[adapters.ref_extraction]\n"
        'class = "agent_mesh.adapters.default.DefaultRefExtractionAdapter"\n'
        'domain = "mail"\n'
        'privacy_class = "project_private"\n'
        "enabled = true\n"
    )


def project_identity_status(repo: str | Path) -> dict[str, Any]:
    root = find_project_root(repo)
    config_path = root / ".agent-mesh" / DEFAULT_CONFIG_NAME
    try:
        with config_path.open("rb") as handle:
            data = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        return {"complete": False, "error": str(exc)}
    project = data.get("project")
    if not isinstance(project, dict):
        return {"complete": False, "error": "missing [project] table"}
    missing = [field for field in ("timezone", "key", "store_id") if not project.get(field)]
    if missing:
        return {"complete": False, "missing": missing}
    try:
        timezone_name = _project_timezone(project["timezone"], "project.timezone")
        key = _project_key(project["key"], "project.key")
        store_id = _store_id_or_legacy(project["store_id"], "project.store_id")
    except ConfigError as exc:
        return {"complete": False, "error": str(exc)}
    if not STORE_ID_RE.fullmatch(store_id):
        return {"complete": False, "error": "project.store_id requires migration"}
    return {
        "complete": True,
        "timezone": timezone_name,
        "project_key": key,
        "store_id": store_id,
    }


def ensure_project_identity_config(
    repo: str | Path,
    *,
    timezone_name: str | None = None,
    project_key: str | None = None,
) -> ProjectIdentityUpdate:
    """Lock and fill stable identity fields without rewriting existing choices."""
    root = find_project_root(repo)
    lock = acquire(root / ".agent-mesh" / ".project-identity-lock")
    try:
        return _ensure_project_identity_config_locked(
            root,
            timezone_name=timezone_name,
            project_key=project_key,
        )
    finally:
        lock.release()


def _ensure_project_identity_config_locked(
    repo: str | Path,
    *,
    timezone_name: str | None = None,
    project_key: str | None = None,
) -> ProjectIdentityUpdate:
    """Perform one identity migration while the repository lock is held."""
    root = find_project_root(repo)
    config_path = root / ".agent-mesh" / DEFAULT_CONFIG_NAME
    text = config_path.read_text(encoding="utf-8")
    try:
        data = tomllib.loads(text)
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"cannot migrate invalid config {config_path}: {exc}") from exc
    project = data.get("project")
    if not isinstance(project, dict):
        raise ConfigError(f"cannot migrate config without [project]: {config_path}")

    existing_timezone = project.get("timezone")
    existing_key = project.get("key")
    existing_store_id = project.get("store_id")
    if timezone_name is not None and existing_timezone not in {None, timezone_name}:
        raise ConfigError(
            "--timezone does not rewrite an existing project.timezone; edit config explicitly"
        )
    if project_key is not None and existing_key not in {None, project_key}:
        raise ConfigError(
            "--project-key does not rewrite an existing project.key; edit config explicitly"
        )

    timezone_value = _project_timezone(
        existing_timezone or timezone_name or detect_local_timezone(),
        "project.timezone",
    )
    key_value = _project_key(
        existing_key or project_key or project_key_from_name(str(project.get("name") or root.name)),
        "project.key",
    )
    store_value = _store_id_or_legacy(
        existing_store_id or new_ulid("store"),
        "project.store_id",
    )
    if existing_store_id and not STORE_ID_RE.fullmatch(store_value):
        store_value = new_ulid("store")

    replace_legacy_store_id = existing_store_id is not None and not STORE_ID_RE.fullmatch(
        str(existing_store_id)
    )

    additions: list[str] = []
    if existing_timezone is None:
        additions.append(f"timezone = {json.dumps(timezone_value)}\n")
    if existing_key is None:
        additions.append(f"key = {json.dumps(key_value)}\n")
    if existing_store_id is None:
        additions.append(f"store_id = {json.dumps(store_value)}\n")
    if not additions and not replace_legacy_store_id:
        return ProjectIdentityUpdate(False, timezone_value, key_value, store_value)

    lines = text.splitlines(keepends=True)
    project_start = next(
        (index for index, line in enumerate(lines) if line.strip() == "[project]"),
        None,
    )
    if project_start is None:
        raise ConfigError(f"cannot migrate config without [project]: {config_path}")
    insert_at = len(lines)
    for index in range(project_start + 1, len(lines)):
        if lines[index].lstrip().startswith("["):
            insert_at = index
            break
    if replace_legacy_store_id:
        _replace_project_string_assignment(
            lines,
            start=project_start + 1,
            end=insert_at,
            key="store_id",
            value=store_value,
            config_path=config_path,
        )
    updated = "".join([*lines[:insert_at], *additions, *lines[insert_at:]])
    _atomic_write_text(config_path, updated)
    return ProjectIdentityUpdate(True, timezone_value, key_value, store_value)


def _replace_project_string_assignment(
    lines: list[str],
    *,
    start: int,
    end: int,
    key: str,
    value: str,
    config_path: Path,
) -> None:
    assignment = re.compile(
        rf"^(?P<prefix>[ \t]*{re.escape(key)}[ \t]*=[ \t]*)"
        r"""(?P<value>"(?:\\.|[^"\\])*"|'[^']*')"""
        r"(?P<suffix>[ \t]*(?:#.*)?(?:\r?\n)?)$"
    )
    candidates = [
        index
        for index in range(start, end)
        if re.match(rf"^[ \t]*{re.escape(key)}[ \t]*=", lines[index])
    ]
    if len(candidates) != 1:
        raise ConfigError(
            f"cannot migrate project.{key} assignment in {config_path}; edit config explicitly"
        )
    index = candidates[0]
    match = assignment.fullmatch(lines[index])
    if match is None:
        raise ConfigError(
            f"cannot migrate noncanonical project.{key} assignment in {config_path}; "
            "edit config explicitly"
        )
    lines[index] = f"{match.group('prefix')}{json.dumps(value)}{match.group('suffix')}"


def _atomic_write_text(path: Path, text: str) -> None:
    mode = path.stat().st_mode & 0o777
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}-", dir=path.parent)
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary_path, mode)
        os.replace(temporary_path, path)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()


LOCAL_ONLY_AGENT_DIR_GITIGNORE_TEXT = """# Privacy default: keep all Agent Mesh state local.
# This pattern also ignores this generated policy file, so no .agent-mesh path is
# selected by a normal `git add -A`.
*
"""


GIT_SHARED_AGENT_DIR_GITIGNORE_TEXT = """# Explicit Git-shared mode: deny by default, then allow canonical state only.
*
!.gitignore
!config.toml
!events.jsonl
!bodies/
!bodies/**
"""


# Backward-compatible import name; the package default is privacy-first.
AGENT_DIR_GITIGNORE_TEXT = LOCAL_ONLY_AGENT_DIR_GITIGNORE_TEXT


def ensure_project_dirs(config: AgentMeshConfig) -> None:
    config.agent_dir.mkdir(parents=True, exist_ok=True)
    config.bodies_dir.mkdir(parents=True, exist_ok=True)
    config.views_dir.mkdir(parents=True, exist_ok=True)
    config.archive_dir.mkdir(parents=True, exist_ok=True)
    config.events_path.touch(exist_ok=True)


def write_agent_dir_gitignore(config: AgentMeshConfig) -> None:
    target = config.agent_dir / ".gitignore"
    text = agent_dir_gitignore_text(config.state_sharing)
    if not target.exists() or target.read_text(encoding="utf-8") != text:
        target.write_text(text, encoding="utf-8")


def agent_dir_gitignore_text(state_sharing: str) -> str:
    sharing = _state_sharing(state_sharing, "version_control.state_sharing")
    if sharing == STATE_SHARING_LOCAL_ONLY:
        return LOCAL_ONLY_AGENT_DIR_GITIGNORE_TEXT
    return GIT_SHARED_AGENT_DIR_GITIGNORE_TEXT


def config_from_agent_dir(agent_dir: str | Path) -> AgentMeshConfig:
    directory = Path(agent_dir).resolve()
    if directory.name == ".agent-mesh":
        root = directory.parent
        try:
            return load_config(root)
        except Exception:
            return _ad_hoc_config(root, directory)

    project_agent_dir = directory / ".agent-mesh"
    if project_agent_dir.exists():
        try:
            return load_config(directory)
        except Exception:
            return _ad_hoc_config(directory, project_agent_dir)

    return _ad_hoc_config(directory.parent, directory)


def _ad_hoc_config(root: Path, agent_dir: Path) -> AgentMeshConfig:
    return AgentMeshConfig(
        project_root=root,
        agent_dir=agent_dir,
        paths=ProjectPaths(
            events_log=agent_dir / "events.jsonl",
            db=agent_dir / "messages.db",
            views_dir=agent_dir / "views",
            archive_dir=agent_dir / "archive",
            bodies_dir=agent_dir / "bodies",
        ),
    )


def _table(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigError(f"[{name}] must be a table")
    return value


def _config_path(value: Any, name: str) -> Path:
    if not isinstance(value, str):
        raise ConfigError(f"{name} must be a string path")
    return Path(value)


def _list_of_strings(value: Any, name: str) -> list[str]:
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise ConfigError(f"{name} must be a list of strings")
    return list(value)


def _optional_string(value: Any, name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ConfigError(f"{name} must be a string")
    return value


def detect_local_timezone() -> str:
    """Return the host's IANA timezone when available, otherwise UTC."""
    candidates: list[str] = []
    env_timezone = os.environ.get("TZ", "").strip()
    if env_timezone:
        candidates.append(env_timezone.removeprefix(":"))
    local_tz = datetime.now().astimezone().tzinfo
    local_key = getattr(local_tz, "key", None)
    if isinstance(local_key, str) and local_key:
        candidates.append(local_key)
    for path in (Path("/etc/localtime"), Path("/var/db/timezone/localtime")):
        try:
            resolved = path.resolve(strict=True).as_posix()
        except OSError:
            continue
        marker = "/zoneinfo/"
        if marker in resolved:
            candidates.append(resolved.split(marker, 1)[1])
    for candidate in candidates:
        try:
            ZoneInfo(candidate)
        except (ZoneInfoNotFoundError, ValueError):
            continue
        return candidate
    return "UTC"


def project_key_from_name(value: str) -> str:
    """Return a stable human project key suitable for qualified references."""
    normalized = re.sub(r"[^a-z0-9]+", "-", value.strip().lower()).strip("-")
    return normalized[:63].rstrip("-") or "project"


def _project_timezone(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{name} must be a non-empty IANA timezone")
    normalized = value.strip()
    try:
        ZoneInfo(normalized)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ConfigError(f"{name} is not a valid IANA timezone: {normalized!r}") from exc
    return normalized


def _project_key(value: Any, name: str) -> str:
    if not isinstance(value, str) or not PROJECT_KEY_RE.fullmatch(value.strip()):
        raise ConfigError(
            f"{name} must use lowercase letters, digits, and internal hyphens (max 63 chars)"
        )
    return value.strip()


def _store_id_or_legacy(value: Any, name: str) -> str:
    if not isinstance(value, str):
        raise ConfigError(f"{name} must be a string")
    normalized = value.strip()
    if STORE_ID_RE.fullmatch(normalized) or re.fullmatch(r"repo-[0-9a-f]{16}", normalized):
        return normalized
    raise ConfigError(f"{name} must be a store_<ULID> identifier")


def _legacy_project_id(root: Path) -> str:
    digest = hashlib.sha256(str(root.resolve()).encode("utf-8")).hexdigest()[:16]
    return f"repo-{digest}"


def _dict_of_strings(value: Any, name: str) -> dict[str, str]:
    if not isinstance(value, dict) or not all(
        isinstance(key, str) and isinstance(item, str) for key, item in value.items()
    ):
        raise ConfigError(f"{name} must be a table of string values")
    return dict(value)


def _state_sharing(value: Any, name: str) -> str:
    if not isinstance(value, str) or value not in STATE_SHARING_CHOICES:
        choices = ", ".join(repr(choice) for choice in STATE_SHARING_CHOICES)
        raise ConfigError(f"{name} must be one of: {choices}")
    return value


def _adapter_declarations(value: Any) -> dict[str, AdapterDeclaration]:
    defaults = {
        "message_lookup": AdapterDeclaration(
            name="message_lookup",
            class_path="agent_mesh.adapters.default.DefaultMessageLookupAdapter",
            domain="mail",
            privacy_class="project_private",
        ),
        "ref_extraction": AdapterDeclaration(
            name="ref_extraction",
            class_path="agent_mesh.adapters.default.DefaultRefExtractionAdapter",
            domain="mail",
            privacy_class="project_private",
        ),
    }
    if value is None:
        return defaults
    if not isinstance(value, dict):
        raise ConfigError("[adapters] must be a table")

    declarations = dict(defaults)
    reserved = {"class", "class_path", "domain", "privacy_class", "enabled"}
    for name, item in value.items():
        if not isinstance(item, dict):
            raise ConfigError(f"[adapters.{name}] must be a table")
        class_path = item.get("class") or item.get("class_path")
        if not isinstance(class_path, str) or not class_path:
            raise ConfigError(f"adapters.{name}.class must be a non-empty class path")
        domain = str(item.get("domain", "mail"))
        privacy_class = str(item.get("privacy_class", "project_private"))
        if privacy_class not in {"public_project", "project_private", "sensitive_private"}:
            raise ConfigError(
                f"adapters.{name}.privacy_class must be public_project, "
                "project_private, or sensitive_private"
            )
        declarations[str(name)] = AdapterDeclaration(
            name=str(name),
            class_path=class_path,
            domain=domain,
            privacy_class=privacy_class,
            enabled=bool(item.get("enabled", True)),
            options={str(key): val for key, val in item.items() if key not in reserved},
        )
    return declarations


def _runtime_profiles(
    value: Any,
    *,
    participants: list[str],
) -> dict[str, RuntimeProfile]:
    if not isinstance(value, dict):
        raise ConfigError("[dispatch.runtime_profiles] must be a table")

    profiles: dict[str, RuntimeProfile] = {}
    enabled_targets: dict[str, str] = {}
    required_strings = (
        "target",
        "provider",
        "adapter",
        "binary",
        "version",
        "model",
        "role",
        "permission_mode",
        "repository_scope",
        "authentication_mode",
        "billing_mode",
    )
    for raw_name, raw_profile in value.items():
        name = str(raw_name)
        if not isinstance(raw_profile, dict):
            raise ConfigError(f"[dispatch.runtime_profiles.{name}] must be a table")
        values = {
            field_name: _required_nonempty_string(
                raw_profile.get(field_name),
                f"dispatch.runtime_profiles.{name}.{field_name}",
            )
            for field_name in required_strings
        }
        target = values["target"]
        if target not in participants:
            raise ConfigError(
                f"dispatch.runtime_profiles.{name}.target references unknown participant {target!r}"
            )
        if values["repository_scope"] != "project":
            raise ConfigError(
                f"dispatch.runtime_profiles.{name}.repository_scope must be 'project'"
            )
        capabilities = tuple(
            _list_of_strings(
                raw_profile.get("required_capabilities"),
                f"dispatch.runtime_profiles.{name}.required_capabilities",
            )
        )
        if not capabilities or "repository" not in capabilities:
            raise ConfigError(
                f"dispatch.runtime_profiles.{name}.required_capabilities must include 'repository'"
            )
        if len(set(capabilities)) != len(capabilities):
            raise ConfigError(
                f"dispatch.runtime_profiles.{name}.required_capabilities must not contain "
                "duplicates"
            )
        unknown_capabilities = sorted(set(capabilities) - set(RUNTIME_CAPABILITY_CHOICES))
        if unknown_capabilities:
            joined = ", ".join(unknown_capabilities)
            raise ConfigError(
                f"dispatch.runtime_profiles.{name}.required_capabilities contains unknown "
                f"capability: {joined}"
            )
        denylist = tuple(
            _list_of_strings(
                raw_profile.get("credential_denylist"),
                f"dispatch.runtime_profiles.{name}.credential_denylist",
            )
        )
        if not denylist:
            raise ConfigError(
                f"dispatch.runtime_profiles.{name}.credential_denylist must not be empty"
            )
        if len(set(denylist)) != len(denylist):
            raise ConfigError(
                f"dispatch.runtime_profiles.{name}.credential_denylist must not contain duplicates"
            )
        for variable in denylist:
            if not variable.isidentifier() or variable.upper() != variable:
                raise ConfigError(
                    f"dispatch.runtime_profiles.{name}.credential_denylist contains invalid "
                    f"environment variable name {variable!r}"
                )
        enabled_value = raw_profile.get("enabled", False)
        if not isinstance(enabled_value, bool):
            raise ConfigError(f"dispatch.runtime_profiles.{name}.enabled must be a boolean")
        profile = RuntimeProfile(
            name=name,
            target=target,
            provider=values["provider"],
            adapter=values["adapter"],
            binary=values["binary"],
            version=values["version"],
            model=values["model"],
            role=values["role"],
            permission_mode=values["permission_mode"],
            repository_scope=values["repository_scope"],
            required_capabilities=capabilities,
            authentication_mode=values["authentication_mode"],
            billing_mode=values["billing_mode"],
            credential_denylist=denylist,
            enabled=enabled_value,
        )
        if profile.enabled and target in enabled_targets:
            other = enabled_targets[target]
            raise ConfigError(
                f"dispatch runtime profiles {other!r} and {name!r} are both enabled for "
                f"participant {target!r}"
            )
        if profile.enabled:
            enabled_targets[target] = name
        profiles[name] = profile
    return profiles


def _required_nonempty_string(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{name} must be a non-empty string")
    return value.strip()


def _validate_routing(participants: list[str], aliases: dict[str, list[str]]) -> None:
    known = set(participants)
    for alias, recipients in aliases.items():
        unknown = [recipient for recipient in recipients if recipient not in known]
        if unknown:
            joined = ", ".join(unknown)
            raise ConfigError(
                f"routing alias {alias!r} references unknown participant(s): {joined}"
            )


def _optional_path(root: Path, value: Any) -> Path | None:
    if value is None:
        return None
    return _required_path(root, value, "path")


def _required_path(root: Path, value: Any, name: str) -> Path:
    if not isinstance(value, str):
        raise ConfigError(f"{name} must be a string path")
    path = Path(value)
    return path if path.is_absolute() else root / path


def _optional_compat_path(root: Path, value: Any, name: str) -> Path | None:
    if value is None:
        return None
    return _required_compat_path(root, value, name)


def _optional_compat_dir_path(root: Path, value: Any, name: str) -> Path | None:
    if value is None:
        return None
    return _required_compat_dir_path(root, value, name)


def _required_compat_path(root: Path, value: Any, name: str) -> Path:
    if not isinstance(value, str):
        raise ConfigError(f"{name} must be a string path")
    if not value.strip():
        raise ConfigError(f"{name} must be a non-empty file path under project root")
    path = Path(value)
    if ".." in path.parts:
        raise ConfigError(f"{name} must stay under project root")
    resolved_root = root.resolve()
    resolved_path = (path if path.is_absolute() else root / path).resolve()
    if resolved_path == resolved_root:
        raise ConfigError(f"{name} must be a file path under project root, not the project root")
    if resolved_path.exists() and resolved_path.is_dir():
        raise ConfigError(f"{name} must be a file path, not a directory")
    if resolved_root not in resolved_path.parents:
        raise ConfigError(f"{name} must stay under project root")
    return resolved_path


def _required_compat_dir_path(root: Path, value: Any, name: str) -> Path:
    if not isinstance(value, str):
        raise ConfigError(f"{name} must be a string path")
    if not value.strip():
        raise ConfigError(f"{name} must be a non-empty directory path under project root")
    path = Path(value)
    if ".." in path.parts:
        raise ConfigError(f"{name} must stay under project root")
    resolved_root = root.resolve()
    resolved_path = (path if path.is_absolute() else root / path).resolve()
    if resolved_path == resolved_root:
        raise ConfigError(
            f"{name} must be a directory path under project root, not the project root"
        )
    if resolved_root not in resolved_path.parents:
        raise ConfigError(f"{name} must stay under project root")
    return resolved_path
