"""Install and verify the repo-local Agent Mesh operating contract."""
from __future__ import annotations

import hashlib
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from agent_mesh.config import ConfigError, load_config


CONTRACT_VERSION = "1"
CONTRACT_TARGETS = {
    "agents": Path("AGENTS.md"),
    "claude": Path("CLAUDE.md"),
}
CONTRACT_BODY = """\
## Agent Mesh managed contract

- When `.agent-mesh/` exists, its append-only event log is the canonical
  coordination and decision source. Workbench and the Agent Mesh CLI are the
  supported write surfaces.
- Read decisions with `agent-q decisions list` and
  `agent-q decisions show <decision-id>` before making a related durable choice.
- Create decisions in Workbench's Decisions tab or with
  `agent-mesh decision propose`. Do not allocate an ID from memory.
- A decision remains Proposed until the human explicitly approves it. The
  Workbench Accept control records that human action; an agent may run
  `agent-mesh decision accept` only after explicit human approval and must name
  the approving human identity.
- Edit proposed decisions in Workbench. Editing an accepted or in-force
  decision appends a revision, requires a reason, and returns it to Proposed
  until the human accepts it again.
- Do not hand-edit `.agent-mesh/events.jsonl`, `.agent-mesh/views/`, or a
  repository Markdown decision log. Markdown decision files may exist only as
  generated, read-only compatibility views; they are never a second source of
  truth.
- Record requests, responses, backlog changes, and decision changes through
  Agent Mesh commands or Workbench, then verify the resulting record before
  claiming completion.
"""
START_PREFIX = "<!-- agent-mesh managed contract: start"
END_MARKER = "<!-- agent-mesh managed contract: end -->"
LEGACY_DECISION_WRITE_RE = re.compile(
    r"(?i)(?:"
    r"(?:record|document|append|update|edit|write|authoritative|source[ -]of[ -]truth)"
    r"[^\n]{0,140}decision[_ -]?log\.md"
    r"|decision[_ -]?log\.md[^\n]{0,140}"
    r"(?:record|document|append|update|edit|write|authoritative|source[ -]of[ -]truth)"
    r"|\bdecisions?\b[^\n]{0,80}\|?\s*`?decision[_ -]?log\.md"
    r")"
)
NEGATED_LEGACY_DECISION_WRITE_RE = re.compile(
    r"(?i)(?:"
    r"\b(?:do not|don't|never|must not|may not|should not)\b[^\n]{0,120}"
    r"decision[_ -]?log\.md"
    r"|decision[_ -]?log\.md[^\n]{0,80}\b(?:read[ -]?only|not writable)\b"
    r")"
)


class AdoptionContractError(ConfigError):
    """Raised when a managed instruction block cannot be updated safely."""


@dataclass(frozen=True)
class ContractInstallResult:
    target: str
    path: Path
    changed: bool
    status_before: str
    status_after: str


def contract_digest() -> str:
    return hashlib.sha256(CONTRACT_BODY.encode("utf-8")).hexdigest()[:12]


def managed_contract_block() -> str:
    return (
        f"{START_PREFIX} version={CONTRACT_VERSION} digest={contract_digest()} -->\n"
        f"{CONTRACT_BODY.rstrip()}\n"
        f"{END_MARKER}\n"
    )


def default_contract_targets(repo: Path) -> list[str]:
    root = repo.expanduser().resolve()
    targets = ["agents"]
    if (root / "CLAUDE.md").exists() or (root / ".claude").exists():
        targets.append("claude")
    return targets


def install_contract(
    repo: Path,
    *,
    targets: list[str] | None = None,
) -> list[ContractInstallResult]:
    config = load_config(repo)
    selected = _normalize_targets(targets or default_contract_targets(config.project_root))
    results: list[ContractInstallResult] = []
    for target in selected:
        path = config.project_root / CONTRACT_TARGETS[target]
        status_before = contract_file_status(path)
        updated = _replace_or_append_contract(path)
        status_after = contract_file_status(path)
        if status_after != "current":  # pragma: no cover - atomic write safeguard
            raise AdoptionContractError(f"managed contract verification failed: {path}")
        results.append(
            ContractInstallResult(
                target=target,
                path=path,
                changed=updated,
                status_before=status_before,
                status_after=status_after,
            )
        )
    return results


def contract_status(
    repo: Path,
    *,
    targets: list[str] | None = None,
) -> dict[str, Any]:
    config = load_config(repo)
    selected = _normalize_targets(targets or default_contract_targets(config.project_root))
    files = []
    for target in selected:
        path = config.project_root / CONTRACT_TARGETS[target]
        files.append(
            {
                "target": target,
                "path": path.relative_to(config.project_root).as_posix(),
                "status": contract_file_status(path),
            }
        )
    conflicts = legacy_decision_write_conflicts(config.project_root)
    return {
        "version": CONTRACT_VERSION,
        "digest": contract_digest(),
        "healthy": all(item["status"] == "current" for item in files) and not conflicts,
        "files": files,
        "conflicts": conflicts,
    }


def contract_file_status(path: Path) -> str:
    if not path.exists():
        return "missing"
    if path.is_symlink():
        return "unsafe"
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        return "unreadable"
    start = text.find(START_PREFIX)
    end = text.find(END_MARKER)
    if start < 0 and end < 0:
        return "missing"
    if start < 0 or end < 0 or end < start:
        return "malformed"
    end += len(END_MARKER)
    installed = text[start:end].rstrip() + "\n"
    return "current" if installed == managed_contract_block() else "stale"


def legacy_decision_write_conflicts(repo: Path) -> list[dict[str, Any]]:
    root = repo.expanduser().resolve()
    candidates = [root / "AGENTS.md", root / "CLAUDE.md"]
    claude_root = root / ".claude"
    if claude_root.is_dir() and not claude_root.is_symlink():
        candidates.extend(
            path
            for path in claude_root.rglob("*")
            if path.is_file() and not path.is_symlink() and path.suffix.lower() in {".md", ".py"}
        )

    conflicts: list[dict[str, Any]] = []
    for path in sorted(set(candidates)):
        if not path.is_file() or path.is_symlink():
            continue
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except (OSError, UnicodeError):
            continue
        in_managed_block = False
        for line_number, line in enumerate(lines, start=1):
            if START_PREFIX in line:
                in_managed_block = True
            if (
                not in_managed_block
                and LEGACY_DECISION_WRITE_RE.search(line)
                and not NEGATED_LEGACY_DECISION_WRITE_RE.search(line)
            ):
                conflicts.append(
                    {
                        "path": path.relative_to(root).as_posix(),
                        "line": line_number,
                        "text": line.strip()[:240],
                    }
                )
            if END_MARKER in line:
                in_managed_block = False
    return conflicts


def _normalize_targets(targets: list[str]) -> list[str]:
    normalized: list[str] = []
    for target in targets:
        value = target.strip().lower()
        if value not in CONTRACT_TARGETS:
            raise AdoptionContractError(
                f"unknown adoption target {target!r}; choose from {', '.join(CONTRACT_TARGETS)}"
            )
        if value not in normalized:
            normalized.append(value)
    if not normalized:
        raise AdoptionContractError("at least one adoption target is required")
    return normalized


def _replace_or_append_contract(path: Path) -> bool:
    if path.is_symlink():
        raise AdoptionContractError(f"refusing to write managed contract through symlink: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        existing = path.read_text(encoding="utf-8") if path.exists() else ""
    except (OSError, UnicodeError) as exc:
        raise AdoptionContractError(f"cannot read instruction file: {path}") from exc
    file_mode = (path.stat().st_mode & 0o777) if path.exists() else 0o644

    start = existing.find(START_PREFIX)
    end = existing.find(END_MARKER)
    if (start < 0) != (end < 0) or (start >= 0 and end < start):
        raise AdoptionContractError(
            f"managed contract markers are malformed in {path}; repair them before retrying"
        )
    block = managed_contract_block()
    if start >= 0:
        end += len(END_MARKER)
        updated = existing[:start] + block.rstrip("\n") + existing[end:]
    else:
        separator = "" if not existing else ("\n" if existing.endswith("\n") else "\n\n")
        updated = existing + separator + block
    if updated == existing:
        return False

    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    temporary_path = Path(temporary_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(updated)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary_path, file_mode)
        os.replace(temporary_path, path)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()
    return True
