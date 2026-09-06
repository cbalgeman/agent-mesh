"""Verified, bounded canonical reference resolution shared by CLI surfaces."""

from __future__ import annotations

import json
import sqlite3
from collections import Counter
from typing import Any, Iterable

from agent_mesh.config import AgentMeshConfig
from agent_mesh.core.decision_projection import decision_revision_sha_from_projection
from agent_mesh.core.reference_syntax import ReferenceOccurrence, reference_kind
from agent_mesh.store.read_model import ReadModelSnapshot
from agent_mesh.store.sqlite import (
    json_loads,
    resolve_agent_instance,
    resolve_decision,
    resolve_message,
)

REFERENCE_CONTEXT_SCHEMA = "agent-mesh.reference-context.v1"
MAX_REFERENCE_CONTEXT_JSON_BYTES = 256 * 1024
MAX_REFERENCE_CONTEXT_TEXT_BYTES = 256 * 1024
MAX_REFERENCE_CONTEXT_OCCURRENCES = 5_000
MAX_REFERENCE_CONTEXT_UNIQUE_REFS = 1_000
MAX_REFERENCE_CANONICAL_TEXT_CHARS = 1_000
MAX_REFERENCE_SNAPSHOT_BYTES = 64 * 1024 * 1024
MAX_REFERENCE_SNAPSHOT_EVENTS = 50_000
MAX_REFERENCE_SNAPSHOT_SECONDS = 10.0

TERMINAL_BACKLOG_STATUSES = frozenset(
    {
        "accepted",
        "canceled",
        "cancelled",
        "closed",
        "complete",
        "completed",
        "done",
        "duplicate",
        "rejected",
        "resolved",
        "wontfix",
    }
)


class BoundedReferenceText:
    """Collect stdout and stderr lines under one deterministic UTF-8 budget."""

    def __init__(self, *, max_bytes: int = MAX_REFERENCE_CONTEXT_TEXT_BYTES) -> None:
        self._max_bytes = max_bytes
        self._used_bytes = 0
        self._stdout: list[str] = []
        self._stderr: list[str] = []
        self.complete = True

    def add_stdout(self, line: str) -> bool:
        return self._add(self._stdout, line)

    def add_stderr(self, line: str) -> bool:
        return self._add(self._stderr, line)

    def render(self) -> tuple[str, str, bool]:
        return "".join(self._stdout), "".join(self._stderr), self.complete

    def _add(self, target: list[str], line: str) -> bool:
        if not self.complete:
            return False
        rendered = f"{line}\n"
        size = len(rendered.encode("utf-8"))
        if self._used_bytes + size > self._max_bytes:
            self.complete = False
            self._stdout.clear()
            self._stderr.clear()
            self._used_bytes = 0
            return False
        target.append(rendered)
        self._used_bytes += size
        return True


def build_reference_context(
    config: AgentMeshConfig,
    snapshot: ReadModelSnapshot,
    occurrences: Iterable[ReferenceOccurrence],
    *,
    boundary: str,
    include_diagnostics: bool = False,
) -> dict[str, Any]:
    """Resolve every unique token against one verified canonical snapshot."""

    selected = tuple(occurrences)
    sources = sorted({item.source for item in selected})
    if len(selected) > MAX_REFERENCE_CONTEXT_OCCURRENCES:
        return _incomplete_reference_context(
            repository=_repository_fields(config, snapshot),
            boundary=boundary,
            occurrence_count=len(selected),
            source_count=len(sources),
            diagnostic=("reference context exceeds its occurrence budget; narrow the input set"),
        )

    grouped: dict[str, list[ReferenceOccurrence]] = {}
    for occurrence in selected:
        grouped.setdefault(occurrence.reference, []).append(occurrence)
    if len(grouped) > MAX_REFERENCE_CONTEXT_UNIQUE_REFS:
        return _incomplete_reference_context(
            repository=_repository_fields(config, snapshot),
            boundary=boundary,
            occurrence_count=len(selected),
            source_count=len(sources),
            diagnostic=(
                "reference context exceeds its unique-reference budget; narrow the input set"
            ),
        )

    resolutions: list[dict[str, Any]] = []
    resolution_indexes: dict[str, int] = {}
    for resolution_index, reference in enumerate(grouped):
        resolution_indexes[reference] = resolution_index
        resolved = _resolve_reference(
            snapshot.conn,
            reference,
            include_diagnostics=include_diagnostics,
        )
        resolutions.append(resolved)
    flattened_occurrences = [
        item.as_dict(resolution_index=resolution_indexes[item.reference]) for item in selected
    ]

    counts = Counter(str(item["resolution_status"]) for item in resolutions)
    return {
        "schema": REFERENCE_CONTEXT_SCHEMA,
        "privacy_class": "project_private",
        "context_status": "complete",
        "complete": True,
        "repository": _repository_fields(config, snapshot),
        "request": {
            "boundary": boundary,
            "occurrence_count": len(selected),
            "reference_count": len(resolutions),
            "source_count": len(sources),
            "sources": sources,
        },
        "resolutions": resolutions,
        "occurrences": flattened_occurrences,
        "resolution_counts": {
            "resolved": counts.get("resolved", 0),
            "partial": counts.get("partial", 0),
            "not_found": counts.get("not_found", 0),
            "unsupported": counts.get("unsupported", 0),
        },
        "diagnostics": list(snapshot.warnings),
    }


def build_unavailable_reference_context(
    *,
    boundary: str,
    occurrence_count: int,
    source_count: int,
    diagnostic: str,
) -> dict[str, Any]:
    return {
        "schema": REFERENCE_CONTEXT_SCHEMA,
        "privacy_class": "project_private",
        "context_status": "unavailable",
        "complete": False,
        "repository": None,
        "request": {
            "boundary": boundary,
            "occurrence_count": occurrence_count,
            "reference_count": 0,
            "source_count": source_count,
            "sources": [],
        },
        "resolutions": [],
        "occurrences": [],
        "resolution_counts": {
            "resolved": 0,
            "partial": 0,
            "not_found": 0,
            "unsupported": 0,
        },
        "diagnostics": [_bounded_diagnostic(diagnostic)],
    }


def render_bounded_reference_context_json(
    result: dict[str, Any],
    *,
    max_bytes: int = MAX_REFERENCE_CONTEXT_JSON_BYTES,
) -> tuple[str, bool]:
    rendered = json.dumps(result, sort_keys=True)
    if len(rendered.encode("utf-8")) <= max_bytes:
        complete = result.get("complete") is True and result.get("context_status") == "complete"
        return rendered, complete

    raw_request = result.get("request")
    request: dict[str, Any] = raw_request if isinstance(raw_request, dict) else {}
    compact = _incomplete_reference_context(
        repository=result.get("repository"),
        boundary=str(request.get("boundary") or "resolve"),
        occurrence_count=int(request.get("occurrence_count") or 0),
        source_count=int(request.get("source_count") or 0),
        diagnostic="reference context exceeds the JSON output bound; narrow the input set",
    )
    compact_rendered = json.dumps(compact, sort_keys=True)
    if len(compact_rendered.encode("utf-8")) <= max_bytes:
        return compact_rendered, False
    return (
        json.dumps(
            {
                "complete": False,
                "context_status": "incomplete",
                "privacy_class": "project_private",
            },
            sort_keys=True,
        ),
        False,
    )


def _resolve_reference(
    conn: sqlite3.Connection,
    reference: str,
    *,
    include_diagnostics: bool,
) -> dict[str, Any]:
    kind = reference_kind(reference)
    if kind == "decision":
        return _resolve_decision_reference(conn, reference, include_diagnostics=include_diagnostics)
    if kind in {"request", "response"}:
        return _resolve_message_reference(conn, reference, kind=kind)
    if kind == "backlog":
        return _resolve_backlog_reference(conn, reference)
    if kind == "agent_instance":
        return _resolve_agent_instance_reference(
            conn,
            reference,
            include_diagnostics=include_diagnostics,
        )
    return _base_reference(
        reference,
        kind=kind,
        resolution_status="unsupported",
        warnings=[f"{kind} references do not have a canonical resolver in this release"],
    )


def _resolve_decision_reference(
    conn: sqlite3.Connection,
    reference: str,
    *,
    include_diagnostics: bool,
) -> dict[str, Any]:
    lookup = reference.partition("-§")[0]
    dec_ulid = resolve_decision(conn, lookup)
    if dec_ulid is None:
        return _base_reference(reference, kind="decision", resolution_status="not_found")
    row = conn.execute("SELECT * FROM decisions WHERE dec_ulid=?", (dec_ulid,)).fetchone()
    if row is None:
        return _base_reference(reference, kind="decision", resolution_status="not_found")
    meta = json_loads(row["meta_json"], {})
    if not isinstance(meta, dict):
        meta = {}
    text, field, truncated = _canonical_text(
        meta,
        fields=("decision",),
    )
    status = str(row["status"])
    warnings: list[str] = []
    if status == "proposed":
        warnings.append("decision is proposed; do not treat it as accepted authority")
    elif status in {"rejected", "superseded", "retired"}:
        warnings.append(f"decision lifecycle status is {status}")
    has_fragment = "-§" in reference
    if has_fragment:
        warnings.append("decision fragment is unvalidated; only the base decision resolved")
    result = _base_reference(
        reference,
        kind="decision",
        resolution_status="partial" if has_fragment else "resolved",
        canonical_id=str(row["human_id"]),
        alias_used=lookup != str(row["human_id"]),
        title=str(row["title"]),
        status=status,
        revision_sha256=decision_revision_sha_from_projection(conn, row),
        body_sha256=str(row["body_sha"]),
        state_event_seq=int(row["event_seq"]),
        canonical_text=text,
        canonical_text_field=field,
        canonical_text_truncated=truncated,
        warnings=warnings,
    )
    if include_diagnostics:
        result["internal_id"] = str(row["dec_ulid"])
    return result


def _resolve_message_reference(
    conn: sqlite3.Connection,
    reference: str,
    *,
    kind: str,
) -> dict[str, Any]:
    row = resolve_message(conn, reference)
    if row is None:
        return _base_reference(reference, kind=kind, resolution_status="not_found")
    text, field, truncated = _row_text(row, fields=("summary", "body_preview"))
    return _base_reference(
        reference,
        kind=kind,
        resolution_status="resolved",
        canonical_id=str(row["id"]),
        title=str(row["title"] or ""),
        status=str(row["status"]),
        body_sha256=str(row["body_sha"]),
        state_event_seq=int(row["event_seq"]),
        canonical_text=text,
        canonical_text_field=field,
        canonical_text_truncated=truncated,
    )


def _resolve_backlog_reference(conn: sqlite3.Connection, reference: str) -> dict[str, Any]:
    row = conn.execute("SELECT * FROM backlog_items WHERE id=?", (reference,)).fetchone()
    if row is None:
        return _base_reference(reference, kind="backlog", resolution_status="not_found")
    status = str(row["status"])
    work_item_fields = {
        field: _bounded_row_field(row, field=field)
        for field in ("summary", "root_cause_summary", "disposition", "notes")
    }
    canonical_field_order = (
        ("notes", "root_cause_summary", "disposition", "summary")
        if status.lower() in TERMINAL_BACKLOG_STATUSES
        else ("summary", "root_cause_summary", "notes", "disposition")
    )
    text, field, truncated = _selected_record_text(
        work_item_fields,
        fields=canonical_field_order,
    )
    warnings = ["backlog item is work-item history, not normative authority"]
    if status.lower() in TERMINAL_BACKLOG_STATUSES:
        warnings.append(
            "terminal backlog summary may describe the original filing; inspect disposition, "
            "notes, and root_cause_summary for the outcome"
        )
    result = _base_reference(
        reference,
        kind="backlog",
        resolution_status="resolved",
        canonical_id=str(row["id"]),
        title=str(row["title"]),
        status=status,
        state_event_seq=int(row["event_seq"]),
        canonical_text=text,
        canonical_text_field=field,
        canonical_text_truncated=truncated,
        warnings=warnings,
    )
    result["priority"] = str(row["priority"] or "")
    result["lane"] = str(row["lane"] or "")
    result["work_item_fields"] = work_item_fields
    return result


def _resolve_agent_instance_reference(
    conn: sqlite3.Connection,
    reference: str,
    *,
    include_diagnostics: bool,
) -> dict[str, Any]:
    row = resolve_agent_instance(conn, reference)
    if row is None:
        return _base_reference(reference, kind="agent_instance", resolution_status="not_found")
    handle = str(row["label"])
    result = _base_reference(
        reference,
        kind="agent_instance",
        resolution_status="resolved",
        canonical_id=handle,
        title=handle,
        status=str(row["status"]),
        state_event_seq=int(row["event_seq"]),
    )
    result["participant"] = str(row["participant"])
    if include_diagnostics:
        result["internal_id"] = str(row["id"])
    return result


def _base_reference(
    reference: str,
    *,
    kind: str,
    resolution_status: str,
    canonical_id: str | None = None,
    alias_used: bool = False,
    title: str | None = None,
    status: str | None = None,
    revision_sha256: str | None = None,
    body_sha256: str | None = None,
    state_event_seq: int | None = None,
    canonical_text: str | None = None,
    canonical_text_field: str | None = None,
    canonical_text_truncated: bool = False,
    warnings: list[str] | None = None,
) -> dict[str, Any]:
    base_id, separator, fragment = reference.partition("-§")
    return {
        "requested_id": reference,
        "base_id": base_id,
        "fragment": fragment if separator else None,
        "fragment_validation": "unsupported" if separator else "not_applicable",
        "kind": kind,
        "record_semantics": _record_semantics(kind),
        "authority_class": _authority_class(
            kind=kind,
            status=status,
            resolution_status=resolution_status,
        ),
        "resolution_status": resolution_status,
        "canonical_id": canonical_id,
        "alias_used": alias_used,
        "title": title,
        "status": status,
        "revision_sha256": revision_sha256,
        "body_sha256": body_sha256,
        "state_event_seq": state_event_seq,
        "canonical_text": canonical_text,
        "canonical_text_field": canonical_text_field,
        "canonical_text_truncated": canonical_text_truncated,
        "warnings": list(warnings or []),
    }


def _canonical_text(
    values: dict[str, Any],
    *,
    fields: tuple[str, ...],
) -> tuple[str | None, str | None, bool]:
    for field in fields:
        value = values.get(field)
        if isinstance(value, str) and value.strip():
            return _bounded_text(value, field=field)
    return None, None, False


def _row_text(
    row: sqlite3.Row,
    *,
    fields: tuple[str, ...],
) -> tuple[str | None, str | None, bool]:
    for field in fields:
        value = row[field]
        if value is not None and str(value).strip():
            return _bounded_text(str(value), field=field)
    return None, None, False


def _bounded_row_field(row: sqlite3.Row, *, field: str) -> dict[str, Any]:
    value = row[field]
    if value is None or not str(value).strip():
        return {"text": None, "truncated": False}
    text, _, truncated = _bounded_text(str(value), field=field)
    return {"text": text, "truncated": truncated}


def _selected_record_text(
    values: dict[str, dict[str, Any]],
    *,
    fields: tuple[str, ...],
) -> tuple[str | None, str | None, bool]:
    for field in fields:
        value = values[field]
        text = value.get("text")
        if isinstance(text, str) and text:
            return text, field, value.get("truncated") is True
    return None, None, False


def _bounded_text(value: str, *, field: str) -> tuple[str, str, bool]:
    normalized = value.strip()
    truncated = len(normalized) > MAX_REFERENCE_CANONICAL_TEXT_CHARS
    return normalized[:MAX_REFERENCE_CANONICAL_TEXT_CHARS], field, truncated


def _record_semantics(kind: str) -> str | None:
    return {
        "decision": "decision_lifecycle",
        "request": "coordination_record",
        "response": "coordination_record",
        "backlog": "work_item_history",
        "agent_instance": "identity_record",
    }.get(kind)


def _authority_class(
    *,
    kind: str,
    status: str | None,
    resolution_status: str,
) -> str | None:
    if resolution_status not in {"resolved", "partial"}:
        return None
    if kind == "decision":
        normalized_status = str(status or "").lower()
        if normalized_status in {"accepted", "in_force"}:
            return "human_approved"
        if normalized_status == "proposed":
            return "proposal"
        return "historical"
    if kind == "backlog":
        return "non_normative"
    if kind in {"request", "response"}:
        return "coordination"
    if kind == "agent_instance":
        return "identity"
    return None


def _repository_fields(
    config: AgentMeshConfig,
    snapshot: ReadModelSnapshot,
) -> dict[str, Any]:
    return {
        "project_key": config.project_key,
        "store_id": config.store_id,
        "projection_version": snapshot.projection_version,
        "source_log_sha256": snapshot.source_log_sha256,
        "event_seq": snapshot.event_seq,
        "read_model": snapshot.read_model,
    }


def _incomplete_reference_context(
    *,
    repository: object,
    boundary: str,
    occurrence_count: int,
    source_count: int,
    diagnostic: str,
) -> dict[str, Any]:
    return {
        "schema": REFERENCE_CONTEXT_SCHEMA,
        "privacy_class": "project_private",
        "context_status": "incomplete",
        "complete": False,
        "repository": repository,
        "request": {
            "boundary": boundary,
            "occurrence_count": occurrence_count,
            "reference_count": 0,
            "source_count": source_count,
            "sources": [],
        },
        "resolutions": [],
        "occurrences": [],
        "resolution_counts": {
            "resolved": 0,
            "partial": 0,
            "not_found": 0,
            "unsupported": 0,
        },
        "diagnostics": [_bounded_diagnostic(diagnostic)],
    }


def _bounded_diagnostic(value: object) -> str:
    sanitized = " ".join(
        "".join(character if character.isprintable() else " " for character in str(value)).split()
    )
    return sanitized[:1_000]
