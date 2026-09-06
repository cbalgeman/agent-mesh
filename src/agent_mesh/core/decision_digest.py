"""Bounded human-readable projection of a decision-context envelope."""

from __future__ import annotations

from typing import Any


MAX_DECISION_DIGEST_BYTES = 9_000
MAX_DECISION_DIGEST_RULE_CHARS = 360
MAX_DECISION_DIGEST_TITLE_CHARS = 140
MAX_DECISION_DIGEST_ITEMS = 3

_ENFORCEMENT_PRIORITY = {
    "required": 0,
    "advisory": 1,
    "none": 2,
}


class DecisionDigestOverflow(RuntimeError):
    """Raised when a digest cannot fit its public output bound."""


def _one_line(value: object, *, maximum: int) -> str:
    text = " ".join(
        "".join(character if character.isprintable() else " " for character in str(value or ""))
        .split()
    )
    if len(text) <= maximum:
        return text
    return text[: max(0, maximum - 1)].rstrip() + "…"


def _bounded_join(values: list[str], *, empty: str) -> str:
    normalized = [_one_line(value, maximum=180) for value in values if str(value).strip()]
    if not normalized:
        return empty
    visible = normalized[:MAX_DECISION_DIGEST_ITEMS]
    suffix = f"; +{len(normalized) - len(visible)} more" if len(normalized) > len(visible) else ""
    return "; ".join(visible) + suffix


def _match_text(decision: dict[str, Any]) -> str:
    matches: list[str] = []
    for match in decision.get("matches", []):
        if not isinstance(match, dict):
            continue
        path = match.get("path")
        pattern = match.get("pattern")
        if path is None:
            matches.append("<repository>")
        elif pattern is None:
            matches.append(str(path))
        else:
            matches.append(f"{path} ({pattern})")
    return _bounded_join(matches, empty="no current path match")


def _decision_priority(decision: dict[str, Any]) -> tuple[int, int, str]:
    """Put effective rules first, then configured required rules, deterministically."""

    return (
        _ENFORCEMENT_PRIORITY.get(str(decision.get("effective_enforcement")), 3),
        _ENFORCEMENT_PRIORITY.get(str(decision.get("configured_enforcement")), 3),
        _one_line(decision.get("id"), maximum=80),
    )


def _decision_lines(raw_decision: dict[str, Any]) -> list[str]:
    identifier = _one_line(raw_decision.get("id"), maximum=80)
    title = _one_line(raw_decision.get("title"), maximum=MAX_DECISION_DIGEST_TITLE_CHARS)
    rule = _one_line(raw_decision.get("summary"), maximum=MAX_DECISION_DIGEST_RULE_CHARS)
    if not rule:
        rule = f"No compact rule recorded; inspect with agent-q decisions show {identifier}."
    tier = _one_line(raw_decision.get("tier"), maximum=80)
    tier_valid = raw_decision.get("tier_valid") is True
    tier_label = tier if tier_valid else f"{tier} [INVALID HISTORICAL VALUE]"
    body_sha = _one_line(raw_decision.get("body_sha"), maximum=64)
    lines = [
        "",
        f"{identifier} — {title}",
        f"rule: {rule}",
        (
            f"tier: {tier_label} | status: {raw_decision.get('status', '')} | "
            f"effective: {raw_decision.get('effective_enforcement', '')}"
        ),
        f"applies: {_match_text(raw_decision)}",
        f"pin: body_sha256={body_sha}",
    ]
    commands = [str(item) for item in raw_decision.get("verification_commands", [])]
    checks = [str(item) for item in raw_decision.get("required_checks", [])]
    if commands:
        lines.append(f"verify: {_bounded_join(commands, empty='not recorded')}")
    elif checks:
        lines.append(f"required checks: {_bounded_join(checks, empty='not recorded')}")
    else:
        lines.append("verify: no verification command recorded")
    warnings = [str(item) for item in raw_decision.get("warnings", [])]
    if not tier_valid:
        warnings.append(
            f"normalize with agent-mesh decision amend {identifier} --tier <canonical-tier> "
            "--reason <reason>; accepted or in-force decisions return to Proposed"
        )
    if warnings:
        lines.append(f"warning: {_bounded_join(sorted(set(warnings)), empty='')}")
    return lines


def _omission_line(count: int) -> str:
    return (
        f"+{count} more applicable decision{'s' if count != 1 else ''} omitted to preserve "
        "the digest byte bound; run agent-q decisions preflight --json for the full envelope."
    )


def render_decision_digest(context: dict[str, Any]) -> str:
    """Render compact path-scoped guidance without replacing the JSON contract."""

    if context.get("complete") is not True:
        diagnostic = _bounded_join(
            [str(item) for item in context.get("diagnostics", [])],
            empty="decision context is incomplete",
        )
        return f"Agent Mesh decision context: INCOMPLETE\nreason: {diagnostic}"

    raw_repository = context.get("repository")
    repository: dict[str, Any] = raw_repository if isinstance(raw_repository, dict) else {}
    raw_request = context.get("request")
    request: dict[str, Any] = raw_request if isinstance(raw_request, dict) else {}
    paths = [str(item) for item in request.get("paths", [])]
    lines = [
        "Agent Mesh decision digest",
        (
            f"repository: {_one_line(repository.get('project_key'), maximum=80)} | "
            f"event_seq: {repository.get('event_seq', '')} | "
            f"paths: {_bounded_join(paths, empty='<repository>')}"
        ),
    ]
    decisions = context.get("decisions", [])
    if not isinstance(decisions, list) or not decisions:
        lines.append("No applicable decisions.")
        return "\n".join(lines)

    ordered = sorted(
        (item for item in decisions if isinstance(item, dict)),
        key=_decision_priority,
    )
    visible = 0
    for index, raw_decision in enumerate(ordered):
        candidate = [*lines, *_decision_lines(raw_decision)]
        remaining = len(ordered) - index - 1
        bounded_candidate = (
            [*candidate, "", _omission_line(remaining)] if remaining else candidate
        )
        if len("\n".join(bounded_candidate).encode("utf-8")) > MAX_DECISION_DIGEST_BYTES:
            break
        lines = candidate
        visible += 1

    omitted = len(ordered) - visible
    if omitted:
        lines.extend(["", _omission_line(omitted)])

    rendered = "\n".join(lines)
    if len(rendered.encode("utf-8")) > MAX_DECISION_DIGEST_BYTES:
        raise DecisionDigestOverflow("decision digest exceeds its output byte bound")
    return rendered
