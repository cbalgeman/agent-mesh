"""Project configuration and discovery for agent-mesh."""
from __future__ import annotations

import fnmatch
import json
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


DEFAULT_CONFIG_NAME = "config.toml"


class ConfigError(RuntimeError):
    """Raised when project configuration is missing or invalid."""


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
    decision_date_placeholders: list[str] = field(
        default_factory=lambda: ["1970-01-01T00:00:00Z"]
    )


@dataclass(frozen=True)
class AdapterDeclaration:
    name: str
    class_path: str
    domain: str
    privacy_class: str
    enabled: bool = True
    options: dict[str, Any] = field(default_factory=dict)


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
    participants: list[str] = field(default_factory=lambda: ["user", "agent"])
    default_sender: str = "human"
    default_recipient: str = "agent"
    body_externalization: bool = False
    paths: ProjectPaths = field(default_factory=ProjectPaths)
    compatibility_views: CompatibilityViews = field(default_factory=CompatibilityViews)
    routing: RoutingConfig = field(default_factory=RoutingConfig)
    checks: ChecksConfig = field(default_factory=ChecksConfig)
    workbench: WorkbenchConfig = field(default_factory=WorkbenchConfig)
    adapters: dict[str, AdapterDeclaration] = field(default_factory=dict)

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

    project_name = str(project_data.get("name", root.name))
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
    paths = ProjectPaths(
        events_log=_config_path(paths_data.get("events_log", ".agent-mesh/events.jsonl"), "paths.events_log"),
        db=_config_path(paths_data.get("db", ".agent-mesh/messages.db"), "paths.db"),
        views_dir=_config_path(paths_data.get("views_dir", ".agent-mesh/views"), "paths.views_dir"),
        archive_dir=_config_path(
            paths_data.get("archive_dir", ".agent-mesh/archive"), "paths.archive_dir"
        ),
        bodies_dir=_config_path(paths_data.get("bodies_dir", ".agent-mesh/bodies"), "paths.bodies_dir"),
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

    return AgentMeshConfig(
        project_root=root,
        agent_dir=agent_dir,
        schema_version=schema_version,
        project_name=project_name,
        participants=participants,
        default_sender=default_sender,
        default_recipient=default_recipient,
        body_externalization=body_externalization,
        paths=paths,
        compatibility_views=compat,
        routing=routing,
        checks=checks,
        workbench=workbench,
        adapters=adapters,
    )


def default_config_text(
    *,
    participants: list[str] | None = None,
    default_sender: str = "human",
    default_recipient: str | None = None,
) -> str:
    people = participants or ["user", "agent"]
    recipient = default_recipient or ("agent" if "agent" in people else people[0])
    participants_json = json.dumps(people)
    return (
        "schema_version = 1\n"
        "\n"
        "[project]\n"
        f"name = {json.dumps('')}\n"
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
        "[paths]\n"
        "events_log = \".agent-mesh/events.jsonl\"\n"
        "db = \".agent-mesh/messages.db\"\n"
        "views_dir = \".agent-mesh/views\"\n"
        "archive_dir = \".agent-mesh/archive\"\n"
        "bodies_dir = \".agent-mesh/bodies\"\n"
        "\n"
        "[routing]\n"
        "preserve_raw_to = true\n"
        "\n"
        "[routing.aliases]\n"
        "\n"
        "[checks]\n"
        "exempt_paths = [\".agent-mesh/**\", \".git/**\", \"**/__pycache__/**\", "
        "\"build/**\", \"dist/**\"]\n"
        "\n"
        "[compatibility_views]\n"
        "\n"
        "[compatibility_views.outbox]\n"
        "\n"
        "[workbench]\n"
        "\n"
        "[adapters.message_lookup]\n"
        "class = \"agent_mesh.adapters.default.DefaultMessageLookupAdapter\"\n"
        "domain = \"mail\"\n"
        "privacy_class = \"project_private\"\n"
        "enabled = true\n"
        "\n"
        "[adapters.ref_extraction]\n"
        "class = \"agent_mesh.adapters.default.DefaultRefExtractionAdapter\"\n"
        "domain = \"mail\"\n"
        "privacy_class = \"project_private\"\n"
        "enabled = true\n"
    )


AGENT_DIR_GITIGNORE_TEXT = """# Generated agent-mesh runtime artifacts
messages.db
messages.db-shm
messages.db-wal
workbench.html
attachments/
views/
archive/
.mail-lock/
.mail-lock.fd
.events-journal-*
events.jsonl.partial-*

# Canonical tracked state
!config.toml
!.gitignore
!events.jsonl
!bodies/
!bodies/**
"""


def ensure_project_dirs(config: AgentMeshConfig) -> None:
    config.agent_dir.mkdir(parents=True, exist_ok=True)
    config.bodies_dir.mkdir(parents=True, exist_ok=True)
    config.views_dir.mkdir(parents=True, exist_ok=True)
    config.archive_dir.mkdir(parents=True, exist_ok=True)
    config.events_path.touch(exist_ok=True)


def write_agent_dir_gitignore(config: AgentMeshConfig) -> None:
    target = config.agent_dir / ".gitignore"
    if not target.exists() or target.read_text(encoding="utf-8") != AGENT_DIR_GITIGNORE_TEXT:
        target.write_text(AGENT_DIR_GITIGNORE_TEXT, encoding="utf-8")


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


def _dict_of_strings(value: Any, name: str) -> dict[str, str]:
    if not isinstance(value, dict) or not all(
        isinstance(key, str) and isinstance(item, str) for key, item in value.items()
    ):
        raise ConfigError(f"{name} must be a table of string values")
    return dict(value)


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


def _validate_routing(participants: list[str], aliases: dict[str, list[str]]) -> None:
    known = set(participants)
    for alias, recipients in aliases.items():
        unknown = [recipient for recipient in recipients if recipient not in known]
        if unknown:
            joined = ", ".join(unknown)
            raise ConfigError(f"routing alias {alias!r} references unknown participant(s): {joined}")


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
        raise ConfigError(f"{name} must be a directory path under project root, not the project root")
    if resolved_root not in resolved_path.parents:
        raise ConfigError(f"{name} must stay under project root")
    return resolved_path
