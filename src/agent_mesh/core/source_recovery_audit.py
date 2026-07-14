"""Reducer for source-recovery promotion review manifests.

This module is intentionally pure: it accepts an existing source-recovery
ledger dict or ledger JSON path and returns a compact, JSON-serializable audit
manifest. It does not promote, import, mutate project config, or write files.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping


MANIFEST_SCHEMA_VERSION = 1
MANIFEST_KIND = "source_recovery_audit"
DEFAULT_MAX_ALTERNATIVES = 3
HIGH_CANDIDATE_COUNT = 50
WEAK_SCORE_GAP = 10.0

_CLASS_BASE = {
    "exact_original_context": 100.0,
    "tool_post_body_recovered": 96.0,
    "chat_authoritative_root": 92.0,
    "chat_update_no_mail_closeout": 56.0,
    "response_inferred": 48.0,
    "agent_summary_derived": 34.0,
    "artifact_echo_only": 22.0,
}
_NON_PROMOTABLE_CLASSIFICATIONS = {
    "artifact_echo_only",
    "agent_summary_derived",
    "response_inferred",
    "chat_update_no_mail_closeout",
}
_PREFERRED_CLASSIFICATIONS = {
    "exact_original_context",
    "tool_post_body_recovered",
    "chat_authoritative_root",
}
_CONTEXT_MARKERS = {
    "active_file": (
        "active file",
        "current file",
        "focused file",
        "file currently open",
    ),
    "artifact": (
        "artifact",
        "artifacts",
        "canvas",
        "generated file",
    ),
    "compact": (
        "compact",
        "compaction",
        "conversation compact",
        "context compact",
    ),
    "ide_context": (
        "ide context",
        "workspace context",
        "editor context",
        "environment_context",
        "<environment_context",
        "<ide_context",
        "<ide_opened_file",
        "claude-vscode main",
    ),
    "inventory": (
        "inventory",
        "manifest",
        "ledger",
    ),
    "list": (
        "file list",
        "listing",
        "list of files",
        "list of open",
    ),
    "open_tabs": (
        "open tabs",
        "tabs open",
        "visible tabs",
    ),
    "selected_text": (
        "selected text",
        "selection:",
        "highlighted text",
    ),
    "summary": (
        "agent summary",
        "session summary",
        "conversation summary",
        "summary:",
        "summarized",
        "recap",
    ),
    "tool_output": (
        "tool output",
        "tool result",
        "function output",
        "command output",
    ),
}
_ACTIVE_CREATION_RE = re.compile(
    r"\b(?:created|posted|sent|appended|wrote|recorded|opened|filed)\b"
)
_REFERENTIAL_MARKERS = (
    "previously created",
    "already created",
    "existing request",
    "existing ticket",
    "created previously",
    "was created",
)
_REQUEST_LIKE_RE = re.compile(
    r"\b(?:please|can you|could you|open|create|post|send|file|request|requests|agent-mesh)\b"
)
_HUMAN_ROLE_RE = re.compile(r"(?:user|human|person|operator)", re.IGNORECASE)
_ASSISTANT_ROLE_RE = re.compile(r"(?:assistant|agent|bot|model)", re.IGNORECASE)


class SourceRecoveryAuditError(ValueError):
    """Raised when a source-recovery audit manifest cannot be produced."""


@dataclass(frozen=True)
class _CandidateScore:
    candidate: Mapping[str, Any]
    candidate_id: str
    classification: str
    confidence: float
    score: float
    reasons: tuple[str, ...]
    warnings: tuple[str, ...]
    review_reasons: tuple[str, ...]
    context_markers: tuple[str, ...]

    @property
    def requires_review(self) -> bool:
        return bool(self.review_reasons)


def build_source_recovery_audit_manifest(
    ledger: Mapping[str, Any] | str | Path,
    *,
    max_alternatives: int = DEFAULT_MAX_ALTERNATIVES,
) -> dict[str, Any]:
    """Build a compact promotion-review manifest from a source-recovery ledger.

    ``ledger`` may be an already-loaded mapping or a path to a JSON ledger. When
    a path is provided, the returned manifest includes the input ledger SHA-256.
    """

    if max_alternatives < 0:
        raise SourceRecoveryAuditError("max_alternatives must be >= 0")
    ledger_data, input_sha256 = _load_ledger(ledger)
    results = ledger_data.get("results")
    if not isinstance(results, list):
        raise SourceRecoveryAuditError("ledger must contain a results array")

    manifest_results = [
        _audit_result(result, max_alternatives=max_alternatives) for result in results
    ]
    counts = _counts_for_results(manifest_results)
    manifest: dict[str, Any] = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "manifest_kind": MANIFEST_KIND,
        "generated_utc": _utc_now(),
        "source_ledger_kind": ledger_data.get("ledger_kind"),
        "source_ledger_schema_version": ledger_data.get("schema_version"),
        "counts": counts,
        "results": manifest_results,
    }
    if input_sha256 is not None:
        manifest["input_ledger_sha256"] = input_sha256
    return manifest


def _load_ledger(ledger: Mapping[str, Any] | str | Path) -> tuple[Mapping[str, Any], str | None]:
    if isinstance(ledger, Mapping):
        return ledger, None
    path = Path(ledger)
    try:
        data = path.read_bytes()
    except FileNotFoundError as exc:
        raise SourceRecoveryAuditError(f"ledger not found: {path}") from exc
    try:
        parsed = json.loads(data)
    except json.JSONDecodeError as exc:
        raise SourceRecoveryAuditError(f"{path}: invalid JSON ledger: {exc.msg}") from exc
    if not isinstance(parsed, dict):
        raise SourceRecoveryAuditError("ledger JSON must be an object")
    return parsed, hashlib.sha256(data).hexdigest()


def _audit_result(result: Any, *, max_alternatives: int) -> dict[str, Any]:
    if not isinstance(result, Mapping):
        raise SourceRecoveryAuditError("each ledger result must be an object")
    message_id = str(result.get("id") or "")
    if not message_id:
        raise SourceRecoveryAuditError("each ledger result must include an id")
    raw_candidates = result.get("candidates") or []
    if not isinstance(raw_candidates, list):
        raise SourceRecoveryAuditError(f"{message_id}: candidates must be an array")

    scored = [
        _score_candidate(message_id, candidate, index + 1, len(raw_candidates))
        for index, candidate in enumerate(raw_candidates)
        if isinstance(candidate, Mapping)
    ]
    scored.sort(
        key=lambda item: (
            -item.score,
            -_CLASS_BASE.get(item.classification, 0.0),
            -item.confidence,
            item.candidate_id,
        )
    )
    if not scored:
        return {
            "id": message_id,
            "status": str(result.get("status") or "missing"),
            "candidate_count": len(raw_candidates),
            "decision": {
                "selected_candidate_id": None,
                "classification": "missing",
                "promote_recommended": False,
                "requires_review": True,
                "score": 0.0,
                "reasons": ["no_candidate"],
                "warnings": ["manual_review:no_recoverable_candidate"],
            },
            "selected_candidate": None,
            "alternatives": [],
        }

    selected = scored[0]
    alternatives = scored[1 : 1 + max_alternatives]
    gap_warning = _weak_gap_warning(selected, scored[1] if len(scored) > 1 else None)
    review_reasons = list(selected.review_reasons)
    warnings = list(selected.warnings)
    if gap_warning:
        review_reasons.append("weak_score_gap")
        warnings.append(gap_warning)
    if len(raw_candidates) > HIGH_CANDIDATE_COUNT and "high_candidate_count" not in review_reasons:
        review_reasons.append("high_candidate_count")
        warnings.append(f"manual_review:high_candidate_count:{len(raw_candidates)}")

    requires_review = bool(review_reasons)
    promote_recommended = (
        selected.classification in _PREFERRED_CLASSIFICATIONS
        and not requires_review
        and selected.score >= 80.0
    )
    reasons = list(selected.reasons)
    for reason in review_reasons:
        if reason not in reasons:
            reasons.append(reason)
    if promote_recommended:
        reasons.append("promote_recommended")
    else:
        reasons.append("manual_review" if requires_review else "promotion_not_recommended")

    return {
        "id": message_id,
        "status": str(result.get("status") or "found"),
        "candidate_count": len(raw_candidates),
        "decision": {
            "selected_candidate_id": selected.candidate_id,
            "classification": selected.classification,
            "promote_recommended": promote_recommended,
            "requires_review": requires_review,
            "score": _round_score(selected.score),
            "reasons": _unique(reasons),
            "warnings": _unique(warnings),
        },
        "selected_candidate": _compact_candidate(selected),
        "alternatives": [
            _compact_candidate(
                alternative,
                rejection_reasons=_rejection_reasons(
                    selected,
                    alternative,
                    weak_gap=selected.score - alternative.score < WEAK_SCORE_GAP,
                ),
            )
            for alternative in alternatives
        ],
    }


def _score_candidate(
    message_id: str,
    candidate: Mapping[str, Any],
    index: int,
    candidate_count: int,
) -> _CandidateScore:
    candidate_id = str(candidate.get("candidate_id") or f"{message_id}#candidate-{index:03d}")
    classification = str(candidate.get("classification") or "response_inferred")
    confidence = _as_float(candidate.get("confidence"), default=0.0)
    base = _CLASS_BASE.get(classification, 0.0)
    score = base + (confidence * 12.0)
    reasons = [f"classification:{classification}", f"confidence:{confidence:.3g}"]
    warnings: list[str] = []
    review_reasons: list[str] = []

    context_markers = _context_markers(candidate)
    if context_markers:
        penalty = 12.0 + (8.0 * min(len(context_markers), 5))
        score -= penalty
        reasons.append(f"context_penalty:{_round_score(penalty)}")
        review_reasons.append("context_heavy_candidate")
        warnings.append("manual_review:context_heavy_evidence:" + ",".join(context_markers))

    if candidate_count > 1:
        penalty = min(24.0, (candidate_count - 1) * 2.0)
        score -= penalty
        reasons.append(f"candidate_count_penalty:{_round_score(penalty)}")
    if candidate_count > HIGH_CANDIDATE_COUNT:
        review_reasons.append("high_candidate_count")
        warnings.append(f"manual_review:high_candidate_count:{candidate_count}")

    reward = 0.0
    has_same_session_confirmation = _has_same_session_nearby_assistant_confirmation(candidate)
    has_active_creation = _has_active_creation_confirmation(candidate)
    has_request_like_human_context = _has_request_like_human_context(message_id, candidate)
    if has_same_session_confirmation:
        reward += 12.0
        reasons.append("same_session_nearby_assistant_confirmation")
    if has_active_creation:
        reward += 10.0
        reasons.append("active_creation_confirmation")
    if has_request_like_human_context:
        reward += 8.0
        reasons.append("request_like_human_context")
    score += reward

    if classification in _PREFERRED_CLASSIFICATIONS and not (
        has_same_session_confirmation or has_active_creation or has_request_like_human_context
    ):
        review_reasons.append("weak_context_signal")
        warnings.append("manual_review:weak_context_signal")

    if classification in _NON_PROMOTABLE_CLASSIFICATIONS:
        review_reasons.append(f"non_promotable_classification:{classification}")
        warnings.append(f"manual_review:non_promotable_classification:{classification}")
    if bool(candidate.get("requires_review")):
        review_reasons.append("ledger_candidate_requires_review")
        warnings.append("manual_review:ledger_candidate_requires_review")

    return _CandidateScore(
        candidate=candidate,
        candidate_id=candidate_id,
        classification=classification,
        confidence=confidence,
        score=max(score, 0.0),
        reasons=tuple(_unique(reasons)),
        warnings=tuple(_unique(warnings)),
        review_reasons=tuple(_unique(review_reasons)),
        context_markers=tuple(context_markers),
    )


def _context_markers(candidate: Mapping[str, Any]) -> list[str]:
    excerpt_text = " ".join(
        str(item.get("excerpt") or "")
        for item in _evidence_window(candidate)
        if isinstance(item, Mapping)
    ).lower()
    markers: list[str] = []
    for name, phrases in _CONTEXT_MARKERS.items():
        if any(phrase in excerpt_text for phrase in phrases):
            markers.append(name)
    return sorted(markers)


def _has_same_session_nearby_assistant_confirmation(candidate: Mapping[str, Any]) -> bool:
    window = _evidence_window(candidate)
    previous = _first_window_item(window, "previous_human")
    if previous is None:
        return False
    previous_session = previous.get("session_id")
    previous_line = _as_int(previous.get("line_number"), default=0)
    for item in window:
        if item.get("window_role") not in {"match", "following"}:
            continue
        if not _is_assistantish(item):
            continue
        item_session = item.get("session_id")
        if previous_session and item_session and previous_session != item_session:
            continue
        line = _as_int(item.get("line_number"), default=0)
        if previous_line and line and abs(line - previous_line) > 5:
            continue
        if _excerpt_has_active_creation(item):
            return True
    return False


def _has_active_creation_confirmation(candidate: Mapping[str, Any]) -> bool:
    return any(_excerpt_has_active_creation(item) for item in _evidence_window(candidate))


def _has_request_like_human_context(message_id: str, candidate: Mapping[str, Any]) -> bool:
    message_id_lower = message_id.lower()
    for item in _evidence_window(candidate):
        if not _is_humanish(item):
            continue
        excerpt = str(item.get("excerpt") or "").lower()
        if any(marker in excerpt for marker in _REFERENTIAL_MARKERS):
            continue
        if not _REQUEST_LIKE_RE.search(excerpt):
            continue
        if message_id_lower in excerpt:
            return _request_marker_near_message_id(message_id_lower, excerpt)
        return True
    return False


def _request_marker_near_message_id(message_id: str, excerpt: str) -> bool:
    index = excerpt.find(message_id.lower())
    if index < 0:
        return False
    start = max(0, index - 180)
    end = min(len(excerpt), index + len(message_id) + 180)
    return bool(_REQUEST_LIKE_RE.search(excerpt[start:end]))


def _excerpt_has_active_creation(item: Mapping[str, Any]) -> bool:
    excerpt = str(item.get("excerpt") or "").lower()
    if any(marker in excerpt for marker in _REFERENTIAL_MARKERS):
        return False
    return bool(_ACTIVE_CREATION_RE.search(excerpt))


def _weak_gap_warning(
    selected: _CandidateScore,
    runner_up: _CandidateScore | None,
) -> str | None:
    if runner_up is None:
        return None
    gap = selected.score - runner_up.score
    if gap >= WEAK_SCORE_GAP:
        return None
    return f"manual_review:weak_score_gap:{_round_score(gap)}"


def _rejection_reasons(
    selected: _CandidateScore,
    alternative: _CandidateScore,
    *,
    weak_gap: bool,
) -> list[str]:
    reasons = [f"lower_score:gap={_round_score(selected.score - alternative.score)}"]
    if _CLASS_BASE.get(alternative.classification, 0.0) < _CLASS_BASE.get(
        selected.classification, 0.0
    ):
        reasons.append("lower_classification_rank")
    if alternative.context_markers:
        reasons.append("context_heavy_evidence")
    if alternative.classification in _NON_PROMOTABLE_CLASSIFICATIONS:
        reasons.append(f"non_promotable_classification:{alternative.classification}")
    selected_reason_set = set(selected.reasons)
    alternative_reason_set = set(alternative.reasons)
    if (
        "active_creation_confirmation" in selected_reason_set
        and "active_creation_confirmation" not in alternative_reason_set
    ):
        reasons.append("lacks_active_creation_confirmation")
    if (
        "request_like_human_context" in selected_reason_set
        and "request_like_human_context" not in alternative_reason_set
    ):
        reasons.append("lacks_request_like_human_context")
    if weak_gap:
        reasons.append("weak_score_gap")
    return _unique(reasons)


def _compact_candidate(
    scored: _CandidateScore,
    *,
    rejection_reasons: list[str] | None = None,
) -> dict[str, Any]:
    candidate = scored.candidate
    compact: dict[str, Any] = {
        "candidate_id": scored.candidate_id,
        "classification": scored.classification,
        "confidence": scored.confidence,
        "score": _round_score(scored.score),
        "requires_review": scored.requires_review,
        "reasons": list(scored.reasons),
        "warnings": list(scored.warnings),
        "refs": _compact_refs(candidate),
        "excerpts": _compact_excerpts(candidate),
    }
    if rejection_reasons is not None:
        compact["rejection_reasons"] = rejection_reasons
    return compact


def _compact_refs(candidate: Mapping[str, Any]) -> list[dict[str, Any]]:
    refs: list[dict[str, Any]] = []
    raw_refs = candidate.get("source_context_refs") or []
    if isinstance(raw_refs, list):
        for ref in raw_refs[:3]:
            if not isinstance(ref, Mapping):
                continue
            compact = _without_none(
                {
                    "role": ref.get("role"),
                    "channel": ref.get("channel"),
                    "source_kind": ref.get("source_kind"),
                    "source_id": ref.get("source_id"),
                    "source_event_id": ref.get("source_event_id"),
                    "source_uri": ref.get("source_uri"),
                    "source_file": ref.get("source_file"),
                    "line_number": ref.get("line_number"),
                    "record_index": ref.get("record_index"),
                    "sha256": ref.get("sha256"),
                }
            )
            refs.append(compact)
    if refs:
        return refs
    for item in _evidence_window(candidate)[:3]:
        refs.append(
            _without_none(
                {
                    "role": item.get("window_role"),
                    "channel": item.get("channel"),
                    "source_id": item.get("source_id"),
                    "source_uri": item.get("source_uri"),
                    "source_file": item.get("source_file"),
                    "line_number": item.get("line_number"),
                    "record_index": item.get("record_index"),
                    "sha256": item.get("sha256"),
                }
            )
        )
    return refs


def _compact_excerpts(candidate: Mapping[str, Any]) -> list[dict[str, Any]]:
    excerpts: list[dict[str, Any]] = []
    for item in _evidence_window(candidate)[:3]:
        excerpt = str(item.get("excerpt") or "")
        if not excerpt:
            continue
        excerpts.append(
            _without_none(
                {
                    "window_role": item.get("window_role"),
                    "turn_role": item.get("turn_role"),
                    "line_number": item.get("line_number"),
                    "excerpt": _shorten(excerpt, 280),
                }
            )
        )
    return excerpts


def _counts_for_results(results: list[dict[str, Any]]) -> dict[str, Any]:
    counts: dict[str, Any] = {
        "results": len(results),
        "selected": 0,
        "missing": 0,
        "promote_recommended": 0,
        "requires_review": 0,
        "manual_review": 0,
        "candidate_total": 0,
        "classification_counts": {},
    }
    classifications: dict[str, int] = counts["classification_counts"]
    for result in results:
        counts["candidate_total"] += int(result.get("candidate_count") or 0)
        decision = result.get("decision") or {}
        classification = str(decision.get("classification") or "unknown")
        classifications[classification] = classifications.get(classification, 0) + 1
        if decision.get("selected_candidate_id"):
            counts["selected"] += 1
        if classification == "missing":
            counts["missing"] += 1
        if decision.get("promote_recommended"):
            counts["promote_recommended"] += 1
        if decision.get("requires_review"):
            counts["requires_review"] += 1
            counts["manual_review"] += 1
    return counts


def _evidence_window(candidate: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    window = candidate.get("evidence_window") or []
    if not isinstance(window, list):
        return []
    return [item for item in window if isinstance(item, Mapping)]


def _first_window_item(
    window: list[Mapping[str, Any]],
    window_role: str,
) -> Mapping[str, Any] | None:
    for item in window:
        if item.get("window_role") == window_role:
            return item
    return None


def _is_humanish(item: Mapping[str, Any]) -> bool:
    return bool(_HUMAN_ROLE_RE.search(str(item.get("turn_role") or "")))


def _is_assistantish(item: Mapping[str, Any]) -> bool:
    return bool(_ASSISTANT_ROLE_RE.search(str(item.get("turn_role") or "")))


def _as_float(value: Any, *, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_int(value: Any, *, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _without_none(value: dict[str, Any]) -> dict[str, Any]:
    return {key: item for key, item in value.items() if item is not None}


def _unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _round_score(value: float) -> float:
    return round(float(value), 3)


def _shorten(text: str, limit: int) -> str:
    collapsed = re.sub(r"\s+", " ", text).strip()
    if len(collapsed) <= limit:
        return collapsed
    return collapsed[: limit - 3] + "..."


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
