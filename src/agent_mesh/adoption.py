"""Install and verify the repo-local Agent Mesh operating contract."""

from __future__ import annotations

import hashlib
import os
import re
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from agent_mesh.config import ConfigError, load_config, project_identity_status
from agent_mesh.core.decision_schema import decision_completeness_issues
from agent_mesh.store.read_model import ReadModelUnavailable, open_read_model


CONTRACT_VERSION = "7"
CONTRACT_TARGETS = {
    "agents": Path("AGENTS.md"),
    "claude": Path("CLAUDE.md"),
}
CONTRACT_BODY = """\
## Agent Mesh managed contract

- When `.agent-mesh/` exists, its append-only event log is the canonical
  coordination and decision source. Workbench and the Agent Mesh CLI are the
  supported write surfaces.
- Before code changes, run
  `agent-q decisions preflight --path <repo-relative-path> --json` for the
  initial planned paths, repeating `--path` as needed. A complete result with an
  empty `decisions` list is valid. Unavailable or incomplete context is not an
  empty result; report it before continuing, and rerun preflight if the path set
  materially expands.
- Use `agent-q decisions list` and `agent-q decisions show <decision-id>` for
  decision lifecycle and metadata follow-up.
- Create decisions in Workbench's Decisions tab or with
  `agent-mesh decision propose`. Do not allocate an ID from memory.
- A decision remains Proposed until a human directly approves it. Agents may
  propose or revise decisions, but must never use the Workbench approval control
  or run `agent-mesh decision accept`. The normal approval surface is
  Workbench's Approve and accept control; a human may instead run the explicitly
  interactive CLI command with their identity and an approval note.
- Edit proposed decisions in Workbench. Editing an accepted or in-force
  decision appends a revision, requires a reason, and returns it to Proposed
  until the human accepts it again.
- Do not hand-edit `.agent-mesh/events.jsonl`, `.agent-mesh/views/`, or a
  repository Markdown decision log. Markdown decision files may exist only as
  generated, read-only compatibility views; they are never a second source of
  truth.
- Apply selective chat-to-mesh promotion: ordinary conversation is `chat-only`
  and stays in chat; ambiguous durable coordination is a `promotion-candidate`
  that requires explicit human confirmation; clear durable coordination is a
  `durable-event`.
- A REQ is a durable work contract. A RES is its material outcome or evidence,
  not every assistant reply. Never mirror a complete chat transcript by default.
  Use concise bodies plus source context, body authority/fidelity, causal edges,
  and references to preserve provenance.
- When a configured runtime supports it, route explicit durable delegation
  through Workbench Dispatch or `agent-q dispatches run` so the frozen policy,
  attempt, and material RES remain linked. Do not claim Agent Mesh intercepts
  every harness-native subagent; label addressed-instance, harness-native, and
  external/manual work truthfully instead of representing it as managed launch.
- Keep detailed reviews, specifications, and reports in project-owned files.
  A concise RES or `review.v1` assurance may carry typed, content-bound
  references to those artifacts; the reference does not publish, copy, or grant
  access to the file. Review assurance never approves a decision for the human.
- Treat participant and AI-agent instance as the two identity layers. The sole
  normal instance identity is its public `<participant>-<durable-role>` handle;
  the hidden project-local `AI-...` ID is canonical attribution. Provider,
  runtime profile, role, capabilities, authentication, and billing are separate
  registration facts, not additional identities.
- A trusted configured integration must complete the automatic identity
  handshake before the chat's first canonical write or instance-addressed read.
  It binds the resolved public handle without human registration. For an
  unmanaged compatibility fallback, bind every Agent Mesh command from the chat
  with its public handle through global `--instance` or
  `AGENT_MESH_INSTANCE_ID`; never use a raw `AI-...` ID or borrow another chat's
  handle.
- Read instance-addressed work with `agent-q list --to-instance <handle>` and
  `agent-q backlog list --owner-instance <handle>`. Hand durable work to another
  active instance with request `--to-instance` or backlog `--owner-instance`;
  generic runtime dispatch does not launch work addressed to a specific existing
  chat.
- A public instance handle survives process restarts until retired. It is not
  cryptographic authentication, does not preserve a provider's context window,
  and does not copy ordinary chat.
- Use `agent-mesh promote` for chat-sourced REQ/RES records. Use the native
  backlog and decision commands for those domains; do not squeeze them into
  mail events.
- Record requests, responses, backlog changes, and decision changes through
  Agent Mesh commands or Workbench, then verify the resulting record before
  claiming completion.
"""
START_PREFIX = "<!-- agent-mesh managed contract: start"
END_MARKER = "<!-- agent-mesh managed contract: end -->"
MAX_ADOPTION_DECISION_EVENTS = 100_000
MAX_ADOPTION_DECISION_BYTES = 64 * 1024 * 1024
ADOPTION_DECISION_TIMEOUT_SECONDS = 10.0
LEGACY_DECISION_WRITE_RE = re.compile(
    r"(?i)(?:"
    r"(?:record|document|append|update|edit|write|authoritative|source[ -]of[ -]truth)"
    r"[^\n]{0,140}(?:decision[_ -]?log|decisions?)\.md"
    r"|(?:decision[_ -]?log|decisions?)\.md[^\n]{0,140}"
    r"(?:record|document|append|update|edit|write|authoritative|source[ -]of[ -]truth)"
    r"|\bdecisions?\b[^\n]{0,80}\|?\s*`?(?:decision[_ -]?log|decisions?)\.md"
    r")"
)
NEGATED_LEGACY_DECISION_WRITE_RE = re.compile(
    r"(?i)(?:"
    r"\b(?:do not|don't|never|must not|may not|should not)\b[^\n]{0,120}"
    r"(?:decision[_ -]?log|decisions?)\.md"
    r"|(?:decision[_ -]?log|decisions?)\.md[^\n]{0,80}"
    r"\b(?:read[ -]?only|not writable)\b"
    r")"
)
LEGACY_DECISION_DOCUMENT_WRITE_RE = re.compile(
    r"(?i)^\s*(?:(?:[-*+]\s+)|(?:\d+[.)]\s+))?"
    r"(?:"
    r"(?:add|create)\b[^\n]{0,80}\b(?:new|next|another)\b[^\n]{0,40}\b(?:entry|record)\b"
    r"|(?:append|write|edit|maintain)\b[^\n]{0,120}\bthis (?:document|file|log)\b"
    r"|update\b[^\n]{0,40}\bstatus\b[^\n]{0,80}\b(?:if|when)\b[^\n]{0,40}"
    r"\bdecision\b"
    r")"
)
SUPPORTED_DECISION_WRITE_RE = re.compile(r"(?i)\b(?:agent-mesh decision|Workbench)\b")
DECISION_DOCUMENT_STEM_RE = re.compile(r"(?i)(?:decision|(?:^|[-_])adrs?(?:[-_]|$))")


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
    if ((root / "CLAUDE.md").exists() or (root / ".claude").exists()) and not claude_imports_agents(
        root
    ):
        targets.append("claude")
    return targets


def selected_contract_targets(
    repo: Path,
    targets: list[str] | None = None,
) -> list[str]:
    """Resolve explicit, persisted, or conservatively detected instruction targets."""

    config = load_config(repo)
    if targets is not None:
        return _normalize_targets(targets)
    if config.adoption.contract_targets is not None:
        return _normalize_targets(list(config.adoption.contract_targets))
    return _normalize_targets(default_contract_targets(config.project_root))


def claude_imports_agents(repo: Path) -> bool:
    """Recognize the root-level Claude instruction import without following links."""

    path = repo.expanduser().resolve() / CONTRACT_TARGETS["claude"]
    if not path.is_file() or path.is_symlink():
        return False
    try:
        if path.stat().st_size > 2 * 1024 * 1024:
            return False
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        return False
    return any(
        re.fullmatch(r"\s*@(?:\./)?AGENTS\.md\s*", line) is not None for line in text.splitlines()
    )


def install_contract(
    repo: Path,
    *,
    targets: list[str] | None = None,
) -> list[ContractInstallResult]:
    config = load_config(repo)
    selected = selected_contract_targets(config.project_root, targets)
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


def remove_unselected_contracts(
    repo: Path,
    *,
    selected_targets: list[str],
) -> list[ContractInstallResult]:
    """Remove only Agent Mesh-owned blocks from targets the user did not select."""

    config = load_config(repo)
    selected = set(_normalize_targets(selected_targets))
    results: list[ContractInstallResult] = []
    for target, relative in CONTRACT_TARGETS.items():
        if target in selected:
            continue
        path = config.project_root / relative
        status_before = contract_file_status(path)
        if status_before in {"missing", "unsafe", "unreadable"}:
            continue
        if status_before == "malformed":
            raise AdoptionContractError(
                f"managed contract markers are malformed in {path}; repair them before retrying"
            )
        changed = _remove_contract(path)
        results.append(
            ContractInstallResult(
                target=target,
                path=path,
                changed=changed,
                status_before=status_before,
                status_after=contract_file_status(path),
            )
        )
    return results


def contract_status(
    repo: Path,
    *,
    targets: list[str] | None = None,
) -> dict[str, Any]:
    from agent_mesh.core.context_delivery import default_delivery_report

    config = load_config(repo)
    selected = selected_contract_targets(config.project_root, targets)
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
    identity = project_identity_status(config.project_root)
    decision_health = decision_migration_status(config)
    healthy = (
        all(item["status"] == "current" for item in files)
        and not conflicts
        and bool(identity.get("complete"))
    )
    return {
        "version": CONTRACT_VERSION,
        "digest": contract_digest(),
        "healthy": healthy,
        "adoption_ready": (
            healthy
            and bool(decision_health["complete"])
            and not decision_health["migration_required"]
        ),
        "target_source": (
            "explicit"
            if targets is not None
            else "persisted"
            if config.adoption.contract_targets is not None
            else "detected"
        ),
        "selected_targets": selected,
        "files": files,
        "conflicts": conflicts,
        "project_identity": identity,
        "decision_health": decision_health,
        "context_delivery": default_delivery_report(),
    }


def decision_migration_status(config) -> dict[str, Any]:
    """Report authoritative historical decisions that fail the current contract."""

    try:
        with open_read_model(
            config,
            max_bytes=MAX_ADOPTION_DECISION_BYTES,
            max_events=MAX_ADOPTION_DECISION_EVENTS,
            deadline_monotonic=time.monotonic() + ADOPTION_DECISION_TIMEOUT_SECONDS,
        ) as snapshot:
            conn = snapshot.conn
            migrations: list[dict[str, Any]] = []
            rows = conn.execute(
                "SELECT dec_ulid, human_id, status, tier, owner, "
                "applicability_scope FROM decisions "
                "WHERE status IN ('accepted', 'in_force') ORDER BY human_id"
            ).fetchall()
            for row in rows:
                globs_by_kind = {
                    kind: [
                        item["pattern"]
                        for item in conn.execute(
                            "SELECT pattern FROM decision_globs "
                            "WHERE dec_ulid=? AND kind=? ORDER BY pattern",
                            (row["dec_ulid"], kind),
                        )
                    ]
                    for kind in ("affected", "exempt", "generated")
                }
                verification = [
                    {
                        "command": item["command"],
                        "expected_signal": item["expected_signal"],
                    }
                    for item in conn.execute(
                        "SELECT command, expected_signal FROM decision_verifications "
                        "WHERE dec_ulid=? ORDER BY command",
                        (row["dec_ulid"],),
                    )
                ]
                issues = list(
                    decision_completeness_issues(
                        tier=str(row["tier"]),
                        owner=str(row["owner"] or ""),
                        affected_code_globs=globs_by_kind["affected"],
                        verification=verification,
                        applicability_scope=str(row["applicability_scope"]),
                        exemptions=globs_by_kind["exempt"],
                        generated_artifact_paths=globs_by_kind["generated"],
                    )
                )
                if issues:
                    migrations.append(
                        {
                            "id": str(row["human_id"]),
                            "status": str(row["status"]),
                            "tier": str(row["tier"]),
                            "issues": list(dict.fromkeys(issues)),
                            "remediation": (
                                f"agent-mesh decision amend {row['human_id']} ... --reason <reason>; "
                                "the decision returns to Proposed for human re-approval"
                            ),
                        }
                    )
            return {
                "complete": True,
                "authoritative_decisions_checked": len(rows),
                "migration_required": migrations,
                "diagnostics": [],
            }
    except (ReadModelUnavailable, OSError, ValueError) as exc:
        return {
            "complete": False,
            "authoritative_decisions_checked": 0,
            "migration_required": [],
            "diagnostics": [str(exc)[:1000]],
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
    return contract_text_status(text)


def contract_text_status(text: str) -> str:
    """Classify managed-contract text already captured by a stable reader."""

    start = text.find(START_PREFIX)
    end = text.find(END_MARKER)
    if start < 0 and end < 0:
        return "missing"
    if start < 0 or end < 0 or end < start:
        return "malformed"
    end += len(END_MARKER)
    installed = text[start:end].rstrip() + "\n"
    return "current" if installed == managed_contract_block() else "stale"


def managed_contract_text(text: str) -> str | None:
    """Extract one complete managed block for duplicate-context measurement."""

    start = text.find(START_PREFIX)
    end = text.find(END_MARKER)
    if start < 0 or end < start:
        return None
    return text[start : end + len(END_MARKER)].rstrip() + "\n"


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
    decision_documents = _legacy_decision_document_candidates(root)
    candidates.extend(decision_documents)
    decision_document_set = set(decision_documents)

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
                and (
                    LEGACY_DECISION_WRITE_RE.search(line)
                    or (
                        path in decision_document_set
                        and LEGACY_DECISION_DOCUMENT_WRITE_RE.search(line)
                    )
                )
                and not NEGATED_LEGACY_DECISION_WRITE_RE.search(line)
                and not SUPPORTED_DECISION_WRITE_RE.search(line)
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


def _legacy_decision_document_candidates(root: Path) -> list[Path]:
    """Find likely writable Markdown decision registers without scanning source trees."""

    search_roots = [root]
    for relative in ("docs", "documentation"):
        candidate = root / relative
        if candidate.is_dir() and not candidate.is_symlink():
            search_roots.append(candidate)

    candidates: set[Path] = set()
    for search_root in search_roots:
        iterator = search_root.glob("*.md") if search_root == root else search_root.rglob("*.md")
        for path in iterator:
            if not path.is_file() or path.is_symlink():
                continue
            relative = path.relative_to(root)
            normalized_parts = {part.lower() for part in relative.parts[:-1]}
            if DECISION_DOCUMENT_STEM_RE.search(path.stem) or any(
                DECISION_DOCUMENT_STEM_RE.search(part) for part in normalized_parts
            ):
                candidates.add(path)
    return sorted(candidates)


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


def _remove_contract(path: Path) -> bool:
    if path.is_symlink():
        raise AdoptionContractError(f"refusing to remove managed contract through symlink: {path}")
    try:
        existing = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        raise AdoptionContractError(f"cannot read instruction file: {path}") from exc
    start = existing.find(START_PREFIX)
    end = existing.find(END_MARKER)
    if start < 0 and end < 0:
        return False
    if start < 0 or end < start:
        raise AdoptionContractError(
            f"managed contract markers are malformed in {path}; repair them before retrying"
        )
    end += len(END_MARKER)
    if end < len(existing) and existing[end : end + 1] == "\n":
        end += 1
    updated = existing[:start] + existing[end:]
    if updated == existing:
        return False
    file_mode = path.stat().st_mode & 0o777
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
