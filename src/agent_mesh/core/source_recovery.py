"""Recovery-only scanner for Claude/Codex JSONL chat histories.

This module is intentionally pure: it reads source JSONL files and returns a
JSON-serializable recovery ledger. It never loads project config, appends
canonical events, rebuilds projections, or mutates live project data.

Classification is conservative and deterministic:

- ``exact_original_context``: the matching turn itself is a human/operator/user
  turn. The chat turn is treated as the best body context.
- ``chat_authoritative_root``: a REQ id appears in a non-human creation/
  confirmation turn and the immediately preceding human/operator/user turn exists.
- ``tool_post_body_recovered``: a tool/function turn containing the id also has
  a structured body/details payload, or the following tool window does.
- ``artifact_echo_only``: the hit is an artifact/list/manifest/inventory echo.
  Echoes never promote body authority.
- ``agent_summary_derived``: the hit appears in a summary/meta recap rather than
  a direct authoring, post-body, or confirmation turn.
- ``chat_update_no_mail_closeout``: the hit describes an update or closeout but
  says no mail/message was posted.
- ``response_inferred``: no stronger rule applies, usually for RES mentions or
  assistant/tool confirmations without recoverable body text.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

from agent_mesh.core.provenance import BODY_AUTHORITY_VALUES, BODY_FIDELITY_VALUES

REQ_RES_ID_RE = re.compile(
    r"(?<![A-Za-z0-9_])(?:REQ|RES)-\d{8}T\d{6}Z-[A-Z0-9_-]+-\d{5}(?![A-Za-z0-9_])"
)

SOURCE_KINDS = {"claude_code_history", "codex_history"}
RECOVERY_CLASSIFICATIONS = {
    "exact_original_context",
    "chat_authoritative_root",
    "chat_update_no_mail_closeout",
    "tool_post_body_recovered",
    "agent_summary_derived",
    "artifact_echo_only",
    "response_inferred",
}

_CLASS_RANK = {
    "exact_original_context": 100,
    "tool_post_body_recovered": 90,
    "chat_authoritative_root": 80,
    "chat_update_no_mail_closeout": 55,
    "response_inferred": 45,
    "agent_summary_derived": 35,
    "artifact_echo_only": 20,
}

_RECOMMENDATIONS = {
    "exact_original_context": ("human_chat", "full", False, 0.95),
    "chat_authoritative_root": ("human_chat", "full", False, 0.84),
    "chat_update_no_mail_closeout": ("unknown", "inferred", True, 0.50),
    "tool_post_body_recovered": ("tool_payload", "full", False, 0.92),
    "agent_summary_derived": ("agent_summary", "inferred", True, 0.42),
    "artifact_echo_only": ("unknown", "metadata_only", True, 0.34),
    "response_inferred": ("unknown", "inferred", True, 0.38),
}

_HUMAN_MARKERS = ("user", "human", "operator")
_ASSISTANT_MARKERS = ("assistant", "agent", "claude", "codex")
_TOOL_MARKERS = ("tool", "function")
_SUMMARY_MARKERS = (
    "agent summary",
    "session summary",
    "conversation summary",
    "summary:",
    "summarized",
    "recap",
    "compact",
    "compaction",
)
_ARTIFACT_MARKERS = (
    "artifact",
    "manifest",
    "inventory",
    "ledger",
    "file list",
    "listing",
    "archive",
)
_UPDATE_NO_MAIL_MARKERS = (
    "no mail closeout",
    "without mail",
    "without posting",
    "did not post",
    "not posted",
    "no agent-mail",
    "no agent mesh",
    "no agent-mesh",
    "closeout only",
)
_CONFIRMATION_PATTERN = re.compile(
    r"(?:^|\b(?:i|i've|we|we've|agent|codex|claude)\s+)"
    r"(?:successfully\s+)?(?:created|posted|sent|appended|wrote|recorded)\b"
)
_REFERENTIAL_NON_CONFIRMATION_MARKERS = (
    "previously created",
    "already created",
    "existing ticket",
    "existing request",
    "existing agent-mesh ticket",
    "noticed",
    "in the backlog",
)
_BODY_KEYS = {"body", "details", "message_body", "body_markdown", "post_body"}
_TOOL_VALUE_MARKERS = {"tool_result", "tool_use", "function_call", "function_call_output"}
_TOOL_KEY_MARKERS = {
    "tool_name",
    "toolName",
    "tool_call_id",
    "toolUseId",
    "tool_use_id",
    "function",
    "call_id",
    "callId",
    "output",
}
_SESSION_KEYS = (
    "session_id",
    "sessionId",
    "conversation_id",
    "conversationId",
    "chat_id",
    "chatId",
    "thread_id",
    "threadId",
)
_TURN_KEYS = (
    "turn_id",
    "turnId",
    "message_id",
    "messageId",
    "item_id",
    "itemId",
    "id",
)
_EVENT_KEYS = ("event_id", "eventId", "uuid")
_TIMESTAMP_KEYS = (
    "timestamp",
    "created_at",
    "createdAt",
    "created_utc",
    "createdUtc",
    "occurred_utc",
    "occurredUtc",
    "time",
)


class SourceRecoveryError(ValueError):
    """Raised when recovery-only source scanning input is malformed."""


@dataclass(frozen=True)
class SourceSpec:
    """One JSONL source history path and its source-channel kind."""

    path: Path
    source_kind: str | None = None


@dataclass(frozen=True)
class _Turn:
    source_file: Path
    channel: str
    line_number: int
    record_index: int
    raw_line: bytes
    record: Any
    text: str
    role: str | None
    session_id: str | None
    turn_id: str | None
    source_event_id: str | None
    timestamp: str | None

    @property
    def source_id(self) -> str:
        return self.turn_id or f"{self.source_file.as_posix()}:{self.line_number}"

    @property
    def source_ref(self) -> str:
        return f"{self.channel}:{self.source_id}"


def load_requested_ids(path: str | Path) -> list[str]:
    """Load requested REQ/RES ids from JSON or simple delimited text."""

    text = Path(path).read_text(encoding="utf-8")
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return _unique_in_order(REQ_RES_ID_RE.findall(text))
    return _unique_in_order(_ids_from_json_value(data))


def infer_source_kind(path: str | Path, explicit: str | None = None) -> str:
    """Infer a supported source-channel kind from a CLI arg/path."""

    if explicit:
        if explicit not in SOURCE_KINDS:
            raise SourceRecoveryError(f"unsupported source kind: {explicit}")
        return explicit
    lowered = Path(path).as_posix().lower()
    if "claude" in lowered:
        return "claude_code_history"
    return "codex_history"


def build_recovery_ledger(
    requested_ids: Iterable[str],
    sources: Iterable[SourceSpec],
) -> dict[str, Any]:
    """Scan JSONL histories and return a stable recovery ledger."""

    ids = _unique_in_order(str(item) for item in requested_ids if str(item).strip())
    candidates_by_id: dict[str, list[dict[str, Any]]] = {item: [] for item in ids}
    source_entries: list[dict[str, Any]] = []

    id_set = set(ids)
    for source in sources:
        channel = infer_source_kind(source.path, source.source_kind)
        source_files = _source_files(source.path)
        root_entry: dict[str, Any] = {
            "path": str(source.path),
            "source_kind": channel,
            "files": len(source_files),
            "records": 0,
            "sha256": _file_sha256(source.path) if source.path.is_file() else None,
        }
        source_entries.append(root_entry)
        for source_file in source_files:
            turns = _read_turns(source_file, channel)
            root_entry["records"] += len(turns)
            if source.path.is_dir():
                source_entries.append(
                    {
                        "path": str(source_file),
                        "source_kind": channel,
                        "records": len(turns),
                        "sha256": _file_sha256(source_file),
                    }
                )
            for index, turn in enumerate(turns):
                matched_ids = [item for item in id_set if item in turn.text]
                if not matched_ids:
                    continue
                for matched_id in sorted(matched_ids):
                    candidates_by_id[matched_id].append(
                        _candidate_for_hit(matched_id, turns, index)
                    )

    results = [_result_for_id(item, candidates_by_id[item]) for item in ids]
    return {
        "schema_version": 1,
        "ledger_kind": "source_recovery",
        "generated_utc": _utc_now(),
        "requested_ids": ids,
        "sources": source_entries,
        "results": results,
    }


def _source_files(path: Path) -> list[Path]:
    if path.is_dir():
        files = sorted(item for item in path.rglob("*.jsonl") if item.is_file())
        if not files:
            raise SourceRecoveryError(f"source directory contains no JSONL files: {path}")
        return files
    if path.is_file():
        return [path]
    raise SourceRecoveryError(f"source not found: {path}")


def _read_turns(path: Path, channel: str) -> list[_Turn]:
    turns: list[_Turn] = []
    try:
        raw = path.read_bytes()
    except FileNotFoundError as exc:
        raise SourceRecoveryError(f"source not found: {path}") from exc

    record_index = 0
    for line_number, line in enumerate(raw.splitlines(keepends=True), start=1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise SourceRecoveryError(
                f"{path}:{line_number}: invalid JSONL record: {exc.msg}"
            ) from exc
        text = _extract_text(record)
        turns.append(
            _Turn(
                source_file=path,
                channel=channel,
                line_number=line_number,
                record_index=record_index,
                raw_line=line,
                record=record,
                text=text,
                role=_extract_role(record),
                session_id=_first_string_for_keys(record, _SESSION_KEYS),
                turn_id=_first_string_for_keys(record, _TURN_KEYS),
                source_event_id=_first_string_for_keys(record, _EVENT_KEYS),
                timestamp=_first_string_for_keys(record, _TIMESTAMP_KEYS),
            )
        )
        record_index += 1
    return turns


def _candidate_for_hit(message_id: str, turns: list[_Turn], match_index: int) -> dict[str, Any]:
    match = turns[match_index]
    previous = _previous_human_turn(turns, match_index)
    following = _following_confirmation_window(turns, match_index)
    classification = _classify_hit(message_id, match, previous, following)
    authority, fidelity, requires_review, confidence = _recommendation(classification, match)

    window: list[dict[str, Any]] = []
    if previous is not None:
        window.append(_evidence_turn(previous, "previous_human", message_id=None))
    window.append(_evidence_turn(match, "match", message_id=message_id))
    for turn in following:
        window.append(_evidence_turn(turn, "following", message_id=message_id))

    source_refs = _source_context_refs_for_window(
        window,
        classification=classification,
        confidence=confidence,
    )
    causal_edges = _causal_edges_for_candidate(
        message_id,
        classification=classification,
        window=window,
        confidence=confidence,
    )
    return {
        "classification": classification,
        "confidence": confidence,
        "requires_review": requires_review,
        "recommended_body_authority": authority,
        "recommended_body_fidelity": fidelity,
        "source_context_refs": source_refs,
        "causal_edges": causal_edges,
        "evidence_window": window,
    }


def _result_for_id(message_id: str, candidates: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted(
        candidates,
        key=lambda item: (
            -_CLASS_RANK[item["classification"]],
            -float(item["confidence"]),
            _first_window_file(item),
            _first_window_line(item),
        ),
    )
    for index, candidate in enumerate(ordered, start=1):
        candidate["candidate_id"] = f"{message_id}#candidate-{index:03d}"

    if not ordered:
        return {
            "id": message_id,
            "status": "missing",
            "best_classification": "missing",
            "recommended_body_authority": "unknown",
            "recommended_body_fidelity": "missing",
            "source_context_refs": [],
            "causal_edges": [],
            "requires_review": True,
            "confidence": 0.0,
            "candidates": [],
        }

    best = ordered[0]
    return {
        "id": message_id,
        "status": "found",
        "best_classification": best["classification"],
        "recommended_body_authority": best["recommended_body_authority"],
        "recommended_body_fidelity": best["recommended_body_fidelity"],
        "source_context_refs": best["source_context_refs"],
        "causal_edges": best["causal_edges"],
        "requires_review": best["requires_review"],
        "confidence": best["confidence"],
        "candidates": ordered,
    }


def _classify_hit(
    message_id: str,
    match: _Turn,
    previous: _Turn | None,
    following: list[_Turn],
) -> str:
    if _looks_artifact_echo(match):
        return "artifact_echo_only"
    if _turn_has_structured_tool_body(match, message_id) or any(
        _turn_has_structured_tool_body(turn, message_id) for turn in following
    ):
        return "tool_post_body_recovered"
    if _looks_update_no_mail_closeout(match):
        return "chat_update_no_mail_closeout"
    if _is_human_turn(match):
        return "exact_original_context"
    if _looks_agent_summary(match):
        return "agent_summary_derived"
    if message_id.startswith("REQ-") and previous is not None and _looks_confirmation(match):
        return "chat_authoritative_root"
    return "response_inferred"


def _recommendation(classification: str, match: _Turn) -> tuple[str, str, bool, float]:
    authority, fidelity, requires_review, confidence = _RECOMMENDATIONS[classification]
    if classification == "exact_original_context" and not _is_human_turn(match):
        authority = "unknown"
    if authority not in BODY_AUTHORITY_VALUES:
        raise AssertionError(f"invalid recommended body authority: {authority}")
    if fidelity not in BODY_FIDELITY_VALUES:
        raise AssertionError(f"invalid recommended body fidelity: {fidelity}")
    return authority, fidelity, requires_review, confidence


def _previous_human_turn(turns: list[_Turn], match_index: int) -> _Turn | None:
    match = turns[match_index]
    for index in range(match_index - 1, -1, -1):
        candidate = turns[index]
        if not _same_session_or_unknown(match, candidate):
            continue
        if _is_human_turn(candidate):
            return candidate
    return None


def _following_confirmation_window(turns: list[_Turn], match_index: int) -> list[_Turn]:
    match = turns[match_index]
    window: list[_Turn] = []
    for turn in turns[match_index + 1 : match_index + 5]:
        if not _same_session_or_unknown(match, turn):
            continue
        if _is_human_turn(turn):
            break
        if _is_tool_turn(turn) or _is_assistant_turn(turn) or _looks_confirmation(turn):
            window.append(turn)
        if len(window) >= 3:
            break
    return window


def _evidence_turn(turn: _Turn, window_role: str, message_id: str | None) -> dict[str, Any]:
    line_sha = hashlib.sha256(turn.raw_line).hexdigest()
    text_sha = hashlib.sha256(turn.text.encode("utf-8")).hexdigest()
    return {
        "window_role": window_role,
        "channel": turn.channel,
        "source_file": str(turn.source_file),
        "line_number": turn.line_number,
        "record_index": turn.record_index,
        "session_id": turn.session_id,
        "turn_id": turn.turn_id,
        "source_id": turn.source_id,
        "source_event_id": turn.source_event_id,
        "source_uri": _source_uri(turn),
        "timestamp": turn.timestamp,
        "turn_role": turn.role,
        "sha256": line_sha,
        "line_sha256": line_sha,
        "text_sha256": text_sha,
        "excerpt": _excerpt(turn.text, message_id),
    }


def _source_context_refs_for_window(
    window: list[dict[str, Any]],
    *,
    classification: str,
    confidence: float,
) -> list[dict[str, Any]]:
    refs: list[dict[str, Any]] = []
    for item in window:
        role = _semantic_source_role(item["window_role"], classification)
        refs.append(
            {
                "channel": item["channel"],
                "source_kind": _source_kind_for_evidence(item, classification),
                "source_id": item["source_id"],
                "source_event_id": item["source_event_id"],
                "source_uri": item["source_uri"],
                "role": role,
                "observed_utc": item["timestamp"],
                "confidence": (
                    confidence
                    if item["window_role"] != "following"
                    else max(confidence - 0.08, 0.0)
                ),
                "source_file": item["source_file"],
                "line_number": item["line_number"],
                "record_index": item["record_index"],
                "sha256": item["sha256"],
            }
        )
    return refs


def _causal_edges_for_candidate(
    message_id: str,
    *,
    classification: str,
    window: list[dict[str, Any]],
    confidence: float,
) -> list[dict[str, Any]]:
    if not window:
        return []
    match = _first_window_item(window, "match") or window[0]
    previous = _first_window_item(window, "previous_human")
    if classification in {"exact_original_context", "chat_authoritative_root"}:
        source = previous if previous is not None else match
        relation = "caused"
        return [
            {
                "relation": relation,
                "from_ref": _evidence_ref(source),
                "to_ref": message_id,
                "confidence": confidence,
            }
        ]
    if classification == "tool_post_body_recovered":
        tool = _first_toolish_window_item(window) or match
        return [
            {
                "relation": "recovered_from",
                "from_ref": _evidence_ref(tool),
                "to_ref": message_id,
                "confidence": confidence,
            }
        ]
    if classification == "artifact_echo_only":
        return [
            {
                "relation": "artifact_echo_of",
                "from_ref": message_id,
                "to_ref": _evidence_ref(match),
                "confidence": confidence,
            }
        ]
    if classification == "chat_update_no_mail_closeout":
        return [
            {
                "relation": "updated",
                "from_ref": _evidence_ref(match),
                "to_ref": message_id,
                "confidence": confidence,
            }
        ]
    relation = "derived_from" if classification == "agent_summary_derived" else "recovered_from"
    return [
        {
            "relation": relation,
            "from_ref": _evidence_ref(match),
            "to_ref": message_id,
            "confidence": confidence,
        }
    ]


def _looks_artifact_echo(turn: _Turn) -> bool:
    if _is_human_turn(turn):
        return False
    text = turn.text.lower()
    if not any(marker in text for marker in _ARTIFACT_MARKERS):
        return False
    if _record_has_key(turn.record, {"manifest", "inventory", "artifact", "artifacts", "files"}):
        return True
    return bool(REQ_RES_ID_RE.search(turn.text))


def _looks_agent_summary(turn: _Turn) -> bool:
    role = (turn.role or "").lower()
    if "summary" in role:
        return True
    text = turn.text.lower()
    return any(marker in text for marker in _SUMMARY_MARKERS)


def _looks_update_no_mail_closeout(turn: _Turn) -> bool:
    text = turn.text.lower()
    return any(marker in text for marker in _UPDATE_NO_MAIL_MARKERS)


def _looks_confirmation(turn: _Turn) -> bool:
    text = turn.text.lower()
    if any(marker in text for marker in _REFERENTIAL_NON_CONFIRMATION_MARKERS):
        return False
    return bool(_CONFIRMATION_PATTERN.search(text))


def _turn_has_structured_tool_body(turn: _Turn, message_id: str) -> bool:
    if not _is_tool_turn(turn):
        return False
    if message_id not in turn.text:
        return False
    return any(value.strip() for value in _values_for_keys(turn.record, _BODY_KEYS))


def _is_human_turn(turn: _Turn) -> bool:
    if _is_tool_turn(turn):
        return False
    role = (turn.role or "").lower()
    return any(marker in role for marker in _HUMAN_MARKERS)


def _is_assistant_turn(turn: _Turn) -> bool:
    role = (turn.role or "").lower()
    return any(marker in role for marker in _ASSISTANT_MARKERS)


def _is_tool_turn(turn: _Turn) -> bool:
    role = (turn.role or "").lower()
    if any(marker in role for marker in _TOOL_MARKERS):
        return True
    if role in _TOOL_VALUE_MARKERS:
        return True
    if _record_has_key(turn.record, _TOOL_KEY_MARKERS):
        return True
    return _record_has_string_value(turn.record, _TOOL_VALUE_MARKERS)


def _same_session_or_unknown(left: _Turn, right: _Turn) -> bool:
    if left.session_id and right.session_id:
        return left.session_id == right.session_id
    return True


def _semantic_source_role(window_role: str, classification: str) -> str:
    if classification == "exact_original_context":
        return "authoritative_body" if window_role == "match" else "confirmation"
    if classification == "chat_authoritative_root":
        return "authoritative_body" if window_role == "previous_human" else "confirmation"
    if classification == "tool_post_body_recovered":
        return "recovered_body" if window_role in {"match", "following"} else "context"
    if classification == "artifact_echo_only":
        return "artifact_echo"
    if classification == "agent_summary_derived":
        return "summary_context"
    return "context"


def _source_kind_for_evidence(item: dict[str, Any], classification: str) -> str:
    if classification == "tool_post_body_recovered" and item.get("window_role") in {"match", "following"}:
        return "tool_payload"
    role = str(item.get("turn_role") or "").lower()
    if any(marker in role for marker in _HUMAN_MARKERS):
        return "human_chat"
    if any(marker in role for marker in _TOOL_MARKERS):
        return "tool_payload"
    if classification == "agent_summary_derived" or "summary" in role:
        return "agent_summary"
    if classification == "artifact_echo_only":
        return "recovery_artifact"
    return "chat_turn"


def _extract_text(value: Any) -> str:
    parts: list[str] = []

    def visit(item: Any) -> None:
        if isinstance(item, str):
            if item:
                parts.append(item)
        elif isinstance(item, dict):
            for key in sorted(item):
                visit(item[key])
        elif isinstance(item, list):
            for child in item:
                visit(child)
        elif item is not None and not isinstance(item, (bool, int, float)):
            parts.append(str(item))

    visit(value)
    return "\n".join(parts)


def _extract_role(record: Any) -> str | None:
    if not isinstance(record, dict):
        return None
    direct_keys = ("role", "speaker", "author")
    for key in direct_keys:
        value = record.get(key)
        if isinstance(value, str) and value:
            return value
    for key in ("message", "payload", "response_item"):
        nested = record.get(key)
        if isinstance(nested, dict):
            nested_role = _extract_role(nested)
            if nested_role:
                return nested_role
    for key in ("type", "kind"):
        value = record.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _first_string_for_keys(value: Any, keys: Iterable[str]) -> str | None:
    if isinstance(value, dict):
        for key in keys:
            item = value.get(key)
            if isinstance(item, str) and item:
                return item
            if isinstance(item, (int, float)) and not isinstance(item, bool):
                return str(item)
        for child in value.values():
            found = _first_string_for_keys(child, keys)
            if found is not None:
                return found
    elif isinstance(value, list):
        for child in value:
            found = _first_string_for_keys(child, keys)
            if found is not None:
                return found
    return None


def _values_for_keys(value: Any, keys: set[str]) -> list[str]:
    values: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key in keys and isinstance(item, str):
                values.append(item)
            elif key in {"arguments", "args", "input", "output", "content", "result"} and isinstance(item, str):
                values.extend(_json_string_values_for_keys(item, keys))
            values.extend(_values_for_keys(item, keys))
    elif isinstance(value, list):
        for item in value:
            values.extend(_values_for_keys(item, keys))
    return values


def _json_string_values_for_keys(text: str, keys: set[str]) -> list[str]:
    stripped = text.strip()
    if not stripped or stripped[0] not in "[{":
        return []
    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError:
        return []
    return _values_for_keys(parsed, keys)


def _record_has_key(value: Any, keys: set[str]) -> bool:
    if isinstance(value, dict):
        if any(key in value for key in keys):
            return True
        return any(_record_has_key(child, keys) for child in value.values())
    if isinstance(value, list):
        return any(_record_has_key(child, keys) for child in value)
    return False


def _record_has_string_value(value: Any, markers: set[str]) -> bool:
    if isinstance(value, str):
        return value.lower() in markers
    if isinstance(value, dict):
        return any(_record_has_string_value(child, markers) for child in value.values())
    if isinstance(value, list):
        return any(_record_has_string_value(child, markers) for child in value)
    return False


def _ids_from_json_value(value: Any) -> list[str]:
    ids: list[str] = []
    if isinstance(value, str):
        ids.extend(REQ_RES_ID_RE.findall(value))
    elif isinstance(value, dict):
        for child in value.values():
            ids.extend(_ids_from_json_value(child))
    elif isinstance(value, list):
        for child in value:
            ids.extend(_ids_from_json_value(child))
    return ids


def _unique_in_order(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        item = str(value).strip()
        if not item or item in seen:
            continue
        seen.add(item)
        ordered.append(item)
    return ordered


def _source_uri(turn: _Turn) -> str:
    return f"{turn.channel}://{turn.source_file.as_posix()}#L{turn.line_number}"


def _evidence_ref(item: dict[str, Any]) -> str:
    return f"{item['channel']}:{item['source_id']}"


def _first_window_item(window: list[dict[str, Any]], window_role: str) -> dict[str, Any] | None:
    for item in window:
        if item["window_role"] == window_role:
            return item
    return None


def _first_toolish_window_item(window: list[dict[str, Any]]) -> dict[str, Any] | None:
    for item in window:
        role = str(item.get("turn_role") or "").lower()
        if any(marker in role for marker in _TOOL_MARKERS):
            return item
    return None


def _first_window_file(candidate: dict[str, Any]) -> str:
    window = candidate.get("evidence_window") or []
    if not window:
        return ""
    return str(window[0].get("source_file") or "")


def _first_window_line(candidate: dict[str, Any]) -> int:
    window = candidate.get("evidence_window") or []
    if not window:
        return 0
    return int(window[0].get("line_number") or 0)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _excerpt(text: str, message_id: str | None, limit: int = 520) -> str:
    collapsed = re.sub(r"\s+", " ", text).strip()
    if len(collapsed) <= limit:
        return collapsed
    if message_id:
        index = collapsed.find(message_id)
        if index != -1:
            half = limit // 2
            start = max(index - half, 0)
            end = min(start + limit, len(collapsed))
            start = max(end - limit, 0)
            prefix = "..." if start > 0 else ""
            suffix = "..." if end < len(collapsed) else ""
            return prefix + collapsed[start:end] + suffix
    return collapsed[: limit - 3] + "..."
