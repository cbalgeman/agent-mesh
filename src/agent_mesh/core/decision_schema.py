"""Shared decision-domain policy and projection helpers."""

from __future__ import annotations

import hashlib
import json
import os
import re
import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

from agent_mesh.core.decision_glob import DecisionGlobError, validate_decision_patterns

DECISION_TIERS = (
    "note",
    "implementation_plan",
    "architecture_contract",
    "production_invariant",
    "compliance_security",
)

DECISION_TIER_ENFORCEMENT = {
    "note": "none",
    "implementation_plan": "none",
    "architecture_contract": "advisory",
    "production_invariant": "required",
    "compliance_security": "required",
}

DECISION_IN_FORCE_TIERS = frozenset(
    tier for tier, enforcement in DECISION_TIER_ENFORCEMENT.items() if enforcement != "none"
)

DECISION_APPLICABILITY_SCOPES = ("paths", "repository", "manual")

DECISION_EVIDENCE_KINDS = (
    "source_request",
    "authoring_response",
    "driving_observation",
    "commit",
    "test_artifact",
    "reference_screenshot",
    "prior_art_link",
    "verification_run",
)

_ASSUMPTION_ID_RE = re.compile(r"A[1-9][0-9]*")

VERIFICATION_EXECUTION_MODE_ARGV = "argv"
VERIFICATION_EXECUTION_MODE_LEGACY_ARGV = "legacy_argv"
VERIFICATION_EXECUTION_MODE_LEGACY_SHELL = "legacy_shell"
VERIFICATION_EXECUTABLE_STATUSES = frozenset({"accepted", "in_force"})
_SHELL_CONTROL_CHARS = frozenset("();<>|&")
DECISION_BODY_FORMAT_GENERATED_V1 = "generated_v1"
DECISION_BODY_FORMAT_CUSTOM = "custom"
DECISION_BODY_FORMAT_UNKNOWN = "unknown"


class DecisionVerificationError(ValueError):
    """Raised when a verification definition cannot be executed as explicit argv."""


class DecisionBodyIntegrityError(ValueError):
    """Raised when an approval body does not match its canonical metadata."""


def generated_decision_body(
    human_id: str,
    *,
    title: str,
    context: str,
    decision: str,
) -> str:
    """Render the package-owned Markdown form used when no custom body is supplied."""

    return f"# {human_id} — {title}\n\n## Context\n{context}\n\n## Decision\n{decision}\n"


def parse_generated_decision_body(body: str, *, human_id: str) -> dict[str, str] | None:
    """Parse only the exact package-owned body form, leaving custom Markdown untouched."""

    match = re.fullmatch(
        rf"\# {re.escape(human_id)} — (?P<title>[^\n]*)\n\n"
        r"\#\# Context\n(?P<context>.*?)\n\n"
        r"\#\# Decision\n(?P<decision>.*)\n",
        body,
        flags=re.DOTALL,
    )
    if match is None:
        return None
    return {
        "title": match.group("title"),
        "context": match.group("context"),
        "decision": match.group("decision"),
    }


def decision_body_format_for_proposal(payload: Mapping[str, Any]) -> str:
    """Infer historical proposal provenance from its exact generated-body digest."""

    expected = generated_decision_body(
        str(payload.get("human_id") or ""),
        title=str(payload.get("title") or ""),
        context=str(payload.get("context") or ""),
        decision=str(payload.get("decision") or ""),
    ).encode("utf-8")
    expected_sha = hashlib.sha256(expected).hexdigest()
    return (
        DECISION_BODY_FORMAT_GENERATED_V1
        if str(payload.get("body_sha") or "") == expected_sha
        else DECISION_BODY_FORMAT_CUSTOM
    )


def read_verified_decision_body(
    agent_dir: Path,
    *,
    body_path: str,
    body_sha: str,
    body_bytes: int,
    max_bytes: int | None = None,
) -> bytes:
    """Read a stable repository body only when path, size, and digest all agree."""

    if not body_path:
        raise DecisionBodyIntegrityError("canonical body path is missing")
    relative = Path(body_path)
    if relative.is_absolute() or ".." in relative.parts:
        raise DecisionBodyIntegrityError("canonical body path is not repository-local")
    root = agent_dir.resolve()
    candidate = (agent_dir / relative).resolve()
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise DecisionBodyIntegrityError("canonical body path escapes .agent-mesh") from exc
    if max_bytes is not None and max_bytes < 0:
        raise DecisionBodyIntegrityError("canonical body read limit must not be negative")
    try:
        with candidate.open("rb") as handle:
            before = os.fstat(handle.fileno())
            if max_bytes is not None and before.st_size > max_bytes:
                raise DecisionBodyIntegrityError(
                    f"canonical body exceeds the {max_bytes}-byte read limit"
                )
            data = handle.read(-1 if max_bytes is None else max_bytes + 1)
            after = os.fstat(handle.fileno())
    except OSError as exc:
        raise DecisionBodyIntegrityError(f"canonical body cannot be read: {exc}") from exc
    before_identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    after_identity = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    if before_identity != after_identity:
        raise DecisionBodyIntegrityError("canonical body changed during integrity verification")
    if max_bytes is not None and len(data) > max_bytes:
        raise DecisionBodyIntegrityError(
            f"canonical body exceeds the {max_bytes}-byte read limit"
        )
    if len(data) != int(body_bytes):
        raise DecisionBodyIntegrityError(
            f"canonical body size mismatch: expected {body_bytes}, found {len(data)}"
        )
    observed_sha = hashlib.sha256(data).hexdigest()
    if observed_sha != str(body_sha):
        raise DecisionBodyIntegrityError(
            f"canonical body digest mismatch: expected {body_sha}, found {observed_sha}"
        )
    return data


def is_valid_decision_tier(tier: str) -> bool:
    """Return whether *tier* is one of the five package-defined values."""

    return tier in DECISION_TIER_ENFORCEMENT


def enforcement_for_tier(tier: str) -> str:
    """Return conservative enforcement for both current and historical tiers."""

    return DECISION_TIER_ENFORCEMENT.get(tier, "none")


def normalize_decision_strings(values: Iterable[Any] | None) -> list[str]:
    """Return stable, non-empty decision metadata strings without duplicates."""

    normalized: list[str] = []
    seen: set[str] = set()
    for value in values or ():
        item = str(value).strip()
        if not item or item in seen:
            continue
        normalized.append(item)
        seen.add(item)
    return normalized


def normalize_decision_assumptions(values: Iterable[Any] | None) -> list[dict[str, Any]]:
    """Return canonical authored assumption definitions with stable identities."""

    normalized: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    seen_text: set[str] = set()
    for index, value in enumerate(values or (), start=1):
        if isinstance(value, Mapping):
            assumption_id = str(
                value.get("id") or value.get("assumption_id") or f"A{index}"
            ).strip()
            text = str(value.get("text") or value.get("statement") or "").strip()
            raw_references = value.get("references", value.get("references_decisions", []))
            if isinstance(raw_references, str):
                raw_references = [raw_references]
            if not isinstance(raw_references, list):
                raise ValueError(f"assumption {assumption_id or index} references must be an array")
            references = normalize_decision_strings(raw_references)
        else:
            assumption_id = f"A{index}"
            text = str(value).strip()
            references = []
        if not _ASSUMPTION_ID_RE.fullmatch(assumption_id):
            raise ValueError(f"invalid assumption id: {assumption_id or '[empty]'}")
        if not text:
            raise ValueError(f"assumption {assumption_id} text must not be empty")
        if assumption_id in seen_ids:
            raise ValueError(f"duplicate assumption id: {assumption_id}")
        if text in seen_text:
            raise ValueError(f"duplicate assumption text: {text}")
        normalized.append({"id": assumption_id, "text": text, "references": references})
        seen_ids.add(assumption_id)
        seen_text.add(text)
    return normalized


def normalize_decision_evidence(
    value: Any, *, allow_extensions: bool = False
) -> dict[str, list[str]]:
    """Return a canonical evidence-kind to reference mapping."""

    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError("evidence must be an object")
    normalized: dict[str, list[str]] = {}
    for raw_kind, raw_values in value.items():
        kind = str(raw_kind).strip()
        if kind not in DECISION_EVIDENCE_KINDS and not allow_extensions:
            raise ValueError(
                f"invalid evidence kind {kind!r}; expected one of: "
                + ", ".join(DECISION_EVIDENCE_KINDS)
            )
        if isinstance(raw_values, str):
            values = [raw_values]
        elif isinstance(raw_values, list) and all(isinstance(item, str) for item in raw_values):
            values = raw_values
        elif allow_extensions:
            values = raw_values if isinstance(raw_values, list) else [raw_values]
        else:
            raise ValueError(f"evidence {kind} must be a string or string array")
        references = sorted(normalize_decision_strings(values))
        if not references:
            raise ValueError(f"evidence {kind} requires at least one reference")
        normalized[kind] = references
    return {kind: normalized[kind] for kind in sorted(normalized)}


def parse_decision_evidence_entries(values: Iterable[Any] | None) -> dict[str, list[str]]:
    """Parse repeatable ``KIND=REFERENCE`` authoring values."""

    grouped: dict[str, list[str]] = {}
    for raw_value in values or ():
        entry = str(raw_value).strip()
        kind, separator, reference = entry.partition("=")
        kind = kind.strip()
        reference = reference.strip()
        if not separator or not kind or not reference:
            raise ValueError("evidence must use KIND=REFERENCE with both values non-empty")
        grouped.setdefault(kind, []).append(reference)
    return normalize_decision_evidence(grouped)


def normalize_decision_review_policy(
    value: Any,
    *,
    participants: Iterable[str] | None = None,
    allow_extensions: bool = False,
) -> dict[str, Any]:
    """Validate and canonicalize reviewer identities and quorum."""

    if value is None or value == {}:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError("review_policy must be an object")
    unknown_fields = sorted(set(value) - {"required_reviewers", "approval_quorum"})
    if unknown_fields and not allow_extensions:
        raise ValueError("unsupported review_policy fields: " + ", ".join(unknown_fields))
    raw_reviewers = value.get("required_reviewers", [])
    if not isinstance(raw_reviewers, list) or not all(
        isinstance(item, str) for item in raw_reviewers
    ):
        raise ValueError("review_policy.required_reviewers must be a string array")
    stripped_reviewers = [item.strip() for item in raw_reviewers]
    if any(not item for item in stripped_reviewers):
        raise ValueError("review_policy.required_reviewers must not contain empty identities")
    if len(set(stripped_reviewers)) != len(stripped_reviewers):
        raise ValueError("review_policy.required_reviewers must contain unique identities")
    reviewers = sorted(stripped_reviewers)
    if not reviewers:
        empty_quorum_values = (None, "", 0) if allow_extensions else (None, "")
        if value.get("approval_quorum") not in empty_quorum_values:
            raise ValueError("approval_quorum requires at least one required reviewer")
        return {}
    configured = set(normalize_decision_strings(participants)) if participants is not None else None
    unknown_reviewers = [
        reviewer for reviewer in reviewers if configured is not None and reviewer not in configured
    ]
    if unknown_reviewers:
        raise ValueError(
            "required reviewer(s) are not configured participants: " + ", ".join(unknown_reviewers)
        )
    raw_quorum = value.get("approval_quorum")
    quorum = len(reviewers) if raw_quorum is None else raw_quorum
    if isinstance(quorum, bool) or not isinstance(quorum, int):
        raise ValueError("approval_quorum must be an integer")
    if quorum < 1 or quorum > len(reviewers):
        raise ValueError(
            f"approval_quorum must be between 1 and {len(reviewers)} for this reviewer set"
        )
    return {"required_reviewers": reviewers, "approval_quorum": quorum}


def decision_current_revision_events(meta: Any) -> list[Mapping[str, Any]]:
    """Return lifecycle events after the latest authority-changing amendment."""

    if not isinstance(meta, Mapping):
        return []
    event_log = meta.get("event_log", [])
    if not isinstance(event_log, list):
        return []
    boundary = -1
    for index, item in enumerate(event_log):
        if not isinstance(item, Mapping) or item.get("kind") != "decision_metadata_updated":
            continue
        payload = item.get("payload", {})
        fields = payload.get("fields_changed", {}) if isinstance(payload, Mapping) else {}
        if not isinstance(fields, Mapping):
            continue
        status_change = fields.get("status")
        returns_to_proposed = (
            isinstance(status_change, list)
            and len(status_change) == 2
            and status_change[1] == "proposed"
        )
        if returns_to_proposed or any(field != "status" for field in fields):
            boundary = index
    return [item for item in event_log[boundary + 1 :] if isinstance(item, Mapping)]


def decision_review_progress(meta: Any, revision_sha: str) -> dict[str, Any]:
    """Return deterministic approval progress for the current decision revision."""

    if not isinstance(meta, Mapping):
        meta = {}
    policy = normalize_decision_review_policy(meta.get("review_policy", {}), allow_extensions=True)
    required = list(policy.get("required_reviewers", []))
    quorum = int(policy.get("approval_quorum") or 0)
    approved: set[str] = set()
    revision_events = decision_current_revision_events(meta)
    approval_shas = {
        str(payload.get("approved_revision_sha") or "")
        for item in revision_events
        if item.get("kind") == "decision_accepted"
        for payload in [item.get("payload", {})]
        if isinstance(payload, Mapping) and payload.get("approved_revision_sha")
    }
    approval_revision_sha = revision_sha
    if revision_sha not in approval_shas and len(approval_shas) == 1:
        approval_revision_sha = next(iter(approval_shas))
    for item in revision_events:
        if item.get("kind") != "decision_accepted":
            continue
        payload = item.get("payload", {})
        if not isinstance(payload, Mapping):
            continue
        if str(payload.get("approved_revision_sha") or "") != approval_revision_sha:
            continue
        reviewer = str(payload.get("accepted_by") or "")
        if reviewer in required:
            approved.add(reviewer)
    approved_reviewers = sorted(approved)
    return {
        "required_reviewers": required,
        "approval_quorum": quorum,
        "approved_reviewers": approved_reviewers,
        "remaining_reviewers": [item for item in required if item not in approved],
        "approvals_recorded": len(approved_reviewers),
        "quorum_reached": not required or len(approved_reviewers) >= quorum,
        "current_revision_sha": revision_sha,
        "approval_revision_sha": approval_revision_sha if approval_shas else "",
        "approval_binding": (
            "legacy_pre_authoring_digest"
            if approval_shas and approval_revision_sha != revision_sha
            else "current_revision"
        ),
    }


def _canonical_decision_assumptions_for_digest(
    values: Iterable[Any] | None,
) -> list[dict[str, Any]]:
    """Bind projected legacy assumptions without weakening authoring validation."""

    try:
        return normalize_decision_assumptions(values)
    except ValueError:
        canonical: list[dict[str, Any]] = []
        for index, value in enumerate(values or (), start=1):
            if isinstance(value, Mapping):
                assumption_id = str(value.get("id") or value.get("assumption_id") or f"A{index}")
                text = str(value.get("text") or value.get("statement") or "")
                references = value.get("references", value.get("references_decisions", []))
            else:
                assumption_id = f"A{index}"
                text = str(value)
                references = []
            if isinstance(references, list):
                canonical_references = [str(item) for item in references]
            else:
                canonical_references = [str(references)]
            canonical.append(
                {
                    "id": assumption_id,
                    "text": text,
                    "references": canonical_references,
                }
            )
        return canonical


def _canonical_decision_evidence_for_digest(value: Any) -> Any:
    """Bind extension evidence kinds retained by historical projections."""

    try:
        return normalize_decision_evidence(value, allow_extensions=True)
    except ValueError:
        if not isinstance(value, Mapping):
            return value
        return {
            str(kind): (
                [str(item) for item in references]
                if isinstance(references, list)
                else [str(references)]
            )
            for kind, references in sorted(value.items(), key=lambda item: str(item[0]))
        }


def _canonical_decision_review_policy_for_digest(value: Any) -> Any:
    """Bind known policy fields plus any retained historical extensions."""

    if value is None or value == {}:
        return {}
    if not isinstance(value, Mapping):
        return value
    try:
        normalized = normalize_decision_review_policy(value, allow_extensions=True)
    except ValueError:
        return {str(key): item for key, item in value.items()}
    extensions = {
        str(key): item
        for key, item in value.items()
        if key not in {"required_reviewers", "approval_quorum"}
    }
    return {**extensions, **normalized}


def decision_revision_digest(
    *,
    decision_id: str,
    human_id: str,
    title: str,
    tier: str,
    applicability_scope: str,
    owner: str,
    context: str,
    decision: str,
    body_sha: str,
    affected_code_globs: Iterable[Any] | None,
    exemptions: Iterable[Any] | None,
    generated_artifact_paths: Iterable[Any] | None,
    required_checks: Iterable[Any] | None,
    verification: Iterable[Any] | None,
    tags: Iterable[Any] | None,
    assumptions: Iterable[Any] | None = None,
    evidence: Any = None,
    review_policy: Any = None,
) -> str:
    """Bind human approval to every authority-bearing field of one revision."""

    canonical_verification: list[dict[str, Any]] = []
    for item in normalize_decision_verification(verification):
        canonical_verification.append(
            {
                key: item.get(key)
                for key in (
                    "command",
                    "execution_mode",
                    "argv",
                    "expected_signal",
                    "runtime_cost",
                    "drift_risk",
                )
                if item.get(key) is not None
            }
        )
    canonical_assumptions = _canonical_decision_assumptions_for_digest(assumptions)
    canonical_evidence = _canonical_decision_evidence_for_digest(evidence)
    canonical_review_policy = _canonical_decision_review_policy_for_digest(review_policy)
    payload: dict[str, Any] = {
        "decision_id": str(decision_id),
        "human_id": str(human_id),
        "title": str(title),
        "tier": str(tier),
        "applicability_scope": str(applicability_scope),
        "owner": str(owner),
        "context": str(context),
        "decision": str(decision),
        "body_sha": str(body_sha),
        "affected_code_globs": normalize_decision_strings(affected_code_globs),
        "exemptions": normalize_decision_strings(exemptions),
        "generated_artifact_paths": normalize_decision_strings(generated_artifact_paths),
        "required_checks": normalize_decision_strings(required_checks),
        "verification": canonical_verification,
        "tags": normalize_decision_strings(tags),
    }
    # Preserve the digest of historical revisions whose un-authored metadata was
    # represented by empty required fields. Non-empty values become authority-bearing.
    if canonical_assumptions:
        payload["assumptions"] = canonical_assumptions
    if canonical_evidence:
        payload["evidence"] = canonical_evidence
    if canonical_review_policy:
        payload["review_policy"] = canonical_review_policy
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def applicability_scope_for(
    value: Any | None,
    affected_code_globs: Iterable[Any] | None,
) -> str:
    """Return an explicit scope or the conservative historical inference."""

    if value is None or not str(value).strip():
        return "paths" if normalize_decision_strings(affected_code_globs) else "manual"
    return str(value).strip()


def decision_applicability_issues(
    *,
    applicability_scope: Any | None,
    tier: str,
    affected_code_globs: Iterable[Any] | None,
    exemptions: Iterable[Any] | None = None,
    generated_artifact_paths: Iterable[Any] | None = None,
) -> tuple[str, ...]:
    """Return typed-scope and meshglob-v1 issues without mutating historical state."""

    affected = normalize_decision_strings(affected_code_globs)
    exempt = normalize_decision_strings(exemptions)
    generated = normalize_decision_strings(generated_artifact_paths)
    scope = applicability_scope_for(applicability_scope, affected)
    issues: list[str] = []
    if scope not in DECISION_APPLICABILITY_SCOPES:
        issues.append(
            "applicability_scope must be one of: " + ", ".join(DECISION_APPLICABILITY_SCOPES)
        )
        return tuple(issues)
    if scope == "paths" and not affected:
        issues.append("paths applicability_scope requires at least one affected code glob")
    if scope == "repository" and affected:
        issues.append("repository applicability_scope cannot contain affected code globs")
    if scope == "manual" and (affected or exempt or generated):
        issues.append("manual applicability_scope cannot contain path filters")
    if scope == "manual" and enforcement_for_tier(tier) == "required":
        issues.append(f"{tier} requires paths or repository applicability_scope")
    for label, patterns in (
        ("affected code globs", affected),
        ("exemptions", exempt),
        ("generated artifact paths", generated),
    ):
        try:
            validate_decision_patterns(tuple(patterns))
        except DecisionGlobError as exc:
            issues.append(f"invalid {label}: {exc}")
    return tuple(issues)


def parse_decision_verification_argv(command: str) -> tuple[str, ...]:
    """Parse one verification command into argv without invoking a shell.

    Unquoted shell control operators are rejected so legacy commands that relied
    on pipes, redirection, command grouping, or command chaining fail closed
    instead of silently changing meaning. Quoted punctuation remains a literal
    argument. Environment expansion and glob expansion are intentionally absent.
    """

    command = str(command).strip()
    if not command:
        raise DecisionVerificationError("verification command must not be empty")
    if "\x00" in command or "\n" in command or "\r" in command:
        raise DecisionVerificationError(
            "verification command must be one NUL-free line of argv text"
        )
    lexer = shlex.shlex(command, posix=True, punctuation_chars="();<>|&")
    lexer.whitespace_split = True
    lexer.commenters = ""
    try:
        argv = tuple(lexer)
    except ValueError as exc:
        raise DecisionVerificationError(f"invalid verification argv: {exc}") from exc
    if not argv:
        raise DecisionVerificationError("verification command must contain an executable")
    control = next(
        (token for token in argv if token and set(token).issubset(_SHELL_CONTROL_CHARS)),
        None,
    )
    if control is not None:
        raise DecisionVerificationError(
            f"verification command uses shell control operator {control!r}; "
            "invoke a reviewed repository script as argv instead"
        )
    executable = argv[0]
    if "=" in executable:
        name, _, _value = executable.partition("=")
        if name and name.replace("_", "a").isalnum() and not name[0].isdigit():
            raise DecisionVerificationError(
                "verification command starts with a shell environment assignment; "
                "use `env NAME=value command ...` or a reviewed repository script"
            )
    return argv


def normalize_decision_verification(
    values: Iterable[Any] | None,
    *,
    reject_unsafe: bool = False,
) -> list[dict[str, Any]]:
    """Normalize string or typed verification definitions.

    New authoring surfaces pass ``reject_unsafe=True`` and persist an explicit
    argv definition. Historical events remain replayable: safe strings project
    as ``legacy_argv`` and shell-dependent strings as ``legacy_shell`` so the
    executor can refuse them with an amendment path.
    """

    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for value in values or ():
        refuse_legacy_definition = False
        if isinstance(value, Mapping):
            command = str(value.get("command") or "").strip()
            expected_signal = str(value.get("expected_signal") or "exit 0").strip()
            item = dict(value)
            raw_argv = value.get("argv")
            if raw_argv is not None:
                if not isinstance(raw_argv, (list, tuple)) or not raw_argv:
                    raise DecisionVerificationError(
                        "verification argv must be a non-empty string array"
                    )
                if any(not isinstance(part, str) for part in raw_argv):
                    raise DecisionVerificationError(
                        "verification argv must be a non-empty string array"
                    )
                argv = tuple(raw_argv)
                if any(not part or "\x00" in part for part in argv):
                    raise DecisionVerificationError(
                        "verification argv entries must be non-empty and NUL-free"
                    )
                canonical_command = shlex.join(argv)
                if command and command != canonical_command:
                    if reject_unsafe:
                        raise DecisionVerificationError(
                            "verification command and argv disagree; provide only argv or its "
                            "canonical text"
                        )
                    argv = ()
                    item.pop("argv", None)
                    item["execution_mode"] = VERIFICATION_EXECUTION_MODE_LEGACY_SHELL
                    refuse_legacy_definition = True
                command = command or canonical_command
            else:
                argv = ()
        else:
            command = str(value).strip()
            item = {"command": command, "expected_signal": "exit 0"}
            expected_signal = "exit 0"
            argv = ()
        if not command or command in seen:
            continue
        if refuse_legacy_definition:
            pass
        elif not argv:
            try:
                argv = parse_decision_verification_argv(command)
            except DecisionVerificationError:
                if reject_unsafe:
                    raise
                item.pop("argv", None)
                item["execution_mode"] = VERIFICATION_EXECUTION_MODE_LEGACY_SHELL
            else:
                item["argv"] = list(argv)
                item["execution_mode"] = (
                    VERIFICATION_EXECUTION_MODE_ARGV
                    if reject_unsafe
                    else VERIFICATION_EXECUTION_MODE_LEGACY_ARGV
                )
                if reject_unsafe:
                    command = shlex.join(argv)
        else:
            item["argv"] = list(argv)
            item["execution_mode"] = VERIFICATION_EXECUTION_MODE_ARGV
        item["command"] = command
        item["expected_signal"] = expected_signal
        normalized.append(item)
        seen.add(command)
    return normalized


def decision_completeness_issues(
    *,
    tier: str,
    owner: str | None,
    affected_code_globs: Iterable[Any] | None,
    verification: Iterable[Any] | None,
    applicability_scope: Any | None = None,
    exemptions: Iterable[Any] | None = None,
    generated_artifact_paths: Iterable[Any] | None = None,
) -> tuple[str, ...]:
    """Return acceptance blockers for the documented decision-tier contract.

    Proposal and replay remain permissive so drafts and historical records stay
    readable. Both approval surfaces call this helper immediately before an
    acceptance event is appended.
    """

    issues: list[str] = []
    globs = normalize_decision_strings(affected_code_globs)
    commands = normalize_decision_verification(verification)
    scope = applicability_scope_for(applicability_scope, globs)

    if not is_valid_decision_tier(tier):
        issues.append(
            f"historical tier {tier!r} is not in the canonical tier set; use one of: "
            + ", ".join(DECISION_TIERS)
        )

    issues.extend(
        decision_applicability_issues(
            applicability_scope=scope,
            tier=tier,
            affected_code_globs=globs,
            exemptions=exemptions,
            generated_artifact_paths=generated_artifact_paths,
        )
    )

    if tier in {"architecture_contract", "production_invariant", "compliance_security"}:
        if not str(owner or "").strip():
            issues.append(f"{tier} requires an owner")
        if not commands:
            issues.append(f"{tier} requires at least one verification command")

    comma_globs = [glob for glob in globs if "," in glob]
    if comma_globs:
        issues.append(
            "affected code globs must be separate entries, not comma-separated: "
            + "; ".join(comma_globs)
        )

    return tuple(issues)


@dataclass(frozen=True)
class DecisionLineageSummary:
    content_revisions: int
    revisit_annotations: int


def decision_lineage_summary(meta: Mapping[str, Any]) -> DecisionLineageSummary:
    """Summarize revision and revisit events from projected decision metadata."""

    content_revisions = 0
    revisit_annotations = 0
    event_log = meta.get("event_log", [])
    if not isinstance(event_log, list):
        return DecisionLineageSummary(0, 0)

    for item in event_log:
        if not isinstance(item, dict):
            continue
        kind = item.get("kind")
        if kind == "decision_revisited":
            revisit_annotations += 1
            continue
        if kind != "decision_metadata_updated":
            continue
        payload = item.get("payload", {})
        if not isinstance(payload, dict):
            continue
        if payload.get("change_kind") == "content_revision":
            content_revisions += 1
            continue
        # Compatibility: older Workbench revisions were a revisit annotation
        # followed by a metadata update that returned the decision to Proposed.
        fields = payload.get("fields_changed", {})
        status_change = fields.get("status") if isinstance(fields, dict) else None
        if (
            isinstance(status_change, list)
            and len(status_change) == 2
            and status_change[0] in {"accepted", "in_force"}
            and status_change[1] == "proposed"
        ):
            content_revisions += 1

    return DecisionLineageSummary(content_revisions, revisit_annotations)
