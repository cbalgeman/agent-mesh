"""Shared decision-domain policy and projection helpers."""

from __future__ import annotations

import shlex
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

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

VERIFICATION_EXECUTION_MODE_ARGV = "argv"
VERIFICATION_EXECUTION_MODE_LEGACY_ARGV = "legacy_argv"
VERIFICATION_EXECUTION_MODE_LEGACY_SHELL = "legacy_shell"
VERIFICATION_EXECUTABLE_STATUSES = frozenset({"accepted", "in_force"})
_SHELL_CONTROL_CHARS = frozenset("();<>|&")


class DecisionVerificationError(ValueError):
    """Raised when a verification definition cannot be executed as explicit argv."""


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
) -> tuple[str, ...]:
    """Return acceptance blockers for the documented decision-tier contract.

    Proposal and replay remain permissive so drafts and historical records stay
    readable. Both approval surfaces call this helper immediately before an
    acceptance event is appended.
    """

    issues: list[str] = []
    globs = normalize_decision_strings(affected_code_globs)
    commands = normalize_decision_verification(verification)

    if tier in {"architecture_contract", "production_invariant", "compliance_security"}:
        if not str(owner or "").strip():
            issues.append(f"{tier} requires an owner")
        if not commands:
            issues.append(f"{tier} requires at least one verification command")

    if enforcement_for_tier(tier) == "required" and not globs:
        issues.append(f"{tier} requires at least one affected code glob")

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
