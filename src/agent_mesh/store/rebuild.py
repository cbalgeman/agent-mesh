"""Deterministic projection from events.jsonl into SQLite."""
from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from agent_mesh.config import AgentMeshConfig, config_from_agent_dir, ensure_project_dirs, load_config
from agent_mesh.core.dispatch_schema import (
    DISPATCH_EVENT_KINDS,
    DispatchSchemaError,
    validate_dispatch_payload,
)
from agent_mesh.core.events import Event, utc_now
from agent_mesh.core.provenance import (
    body_authority_for_payload,
    body_fidelity_for_payload,
    confidence_value,
    validate_event_provenance,
)
from agent_mesh.store.sqlite import (
    ALL_TABLES,
    connect,
    get_meta,
    get_last_event_seq,
    initialize_schema,
    json_dumps,
    json_loads,
    reset_schema,
    resolve_decision,
    set_last_event_seq,
    set_meta,
)

# Bump this whenever replay semantics change in a way that requires existing
# SQLite projections to be regenerated. Event-log equality alone cannot detect
# a package upgrade that changes how historical events are interpreted.
PROJECTION_VERSION = "1"

DECISION_PARENT_MISSING = "DECISION_PARENT_MISSING"
DECISION_SUPERSEDE_TARGET_INVALID = "DECISION_SUPERSEDE_TARGET_INVALID"
DECISION_SUPERSEDE_CYCLE = "DECISION_SUPERSEDE_CYCLE"
DECISION_HUMAN_ID_COLLISION = "DECISION_HUMAN_ID_COLLISION"
DECISION_ALIAS_FORK = "DECISION_ALIAS_FORK"

DECISION_STOP_LINE_CODES = {
    DECISION_PARENT_MISSING,
    DECISION_SUPERSEDE_TARGET_INVALID,
    DECISION_SUPERSEDE_CYCLE,
    DECISION_HUMAN_ID_COLLISION,
    DECISION_ALIAS_FORK,
}

DISPATCH_BODY_LEAK = "DISPATCH_BODY_LEAK"
DISPATCH_LEASE_DUPLICATE = "DISPATCH_LEASE_DUPLICATE"
DISPATCH_LEASE_UNKNOWN_RUN = "DISPATCH_LEASE_UNKNOWN_RUN"
DISPATCH_LEASE_RELEASE_INVALID = "DISPATCH_LEASE_RELEASE_INVALID"
DISPATCH_RUN_UNKNOWN_MESSAGE = "DISPATCH_RUN_UNKNOWN_MESSAGE"
DISPATCH_RUN_WITHOUT_LEASE = "DISPATCH_RUN_WITHOUT_LEASE"
DISPATCH_RUN_TERMINAL_WITHOUT_STARTED_OR_LEASE = "DISPATCH_RUN_TERMINAL_WITHOUT_STARTED_OR_LEASE"
DISPATCH_SUPERSEDE_INVALID = "DISPATCH_SUPERSEDE_INVALID"
DISPATCH_OUTPUT_MESSAGE_INVALID = "DISPATCH_OUTPUT_MESSAGE_INVALID"
DISPATCH_OUTPUT_THREAD_MISMATCH = "DISPATCH_OUTPUT_THREAD_MISMATCH"

DISPATCH_STOP_LINE_CODES = {
    DISPATCH_BODY_LEAK,
    DISPATCH_LEASE_DUPLICATE,
    DISPATCH_LEASE_UNKNOWN_RUN,
    DISPATCH_LEASE_RELEASE_INVALID,
    DISPATCH_RUN_UNKNOWN_MESSAGE,
    DISPATCH_RUN_WITHOUT_LEASE,
    DISPATCH_RUN_TERMINAL_WITHOUT_STARTED_OR_LEASE,
    DISPATCH_SUPERSEDE_INVALID,
    DISPATCH_OUTPUT_MESSAGE_INVALID,
    DISPATCH_OUTPUT_THREAD_MISMATCH,
}

# Hierarchical decisions use numbered S/B slices (D038-S1, D067-B2).
# Existing projects also use single-letter variant gates (D076-B, D076-E).
DECISION_SUFFIX_PATTERN = r"(?:[SB]\d+|[A-Z])"
DECISION_ID_RE = re.compile(rf"^D(\d+)(?:-({DECISION_SUFFIX_PATTERN}))?$")
SECTION_REF_RE = re.compile(rf"^D(\d+)(?:-{DECISION_SUFFIX_PATTERN})?-§(.+)$")

DECISION_EVENT_KINDS = {
    "decision_proposed",
    "decision_accepted",
    "decision_revisited",
    "decision_superseded",
    "decision_retired",
    "decision_rejected",
    "decision_metadata_updated",
    "decision_assumption_violated",
    "decision_check_failed",
    "decision_drift_detected",
}


class DecisionStopLine(RuntimeError):
    """Raised when a decision event violates a v1.1 STOP-LINE."""

    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}")


class DispatchStopLine(RuntimeError):
    """Raised when a dispatch event violates a dispatch-domain STOP-LINE at replay time."""

    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}")


@dataclass(frozen=True)
class RebuildResult:
    event_count: int
    last_event_seq: int
    table_hashes: dict[str, str]


def rebuild_all(
    config: AgentMeshConfig | None = None,
    *,
    start: str | Path | None = None,
) -> RebuildResult:
    cfg = config or load_config(start)
    ensure_project_dirs(cfg)
    conn = connect(cfg.db_path)
    try:
        with conn:
            reset_schema(conn)
        event_count = 0
        last_seq = 0
        for record in read_event_records(cfg.events_path):
            apply_record(record, cfg, conn=conn, require_next=True)
            event_count += 1
            last_seq = int(record["event_seq"])
        table_hashes = table_hashes_for(cfg)
        with conn:
            set_meta(conn, "events_jsonl_sha", file_sha256(cfg.events_path))
            set_meta(conn, "projection_version", PROJECTION_VERSION)
        return RebuildResult(event_count=event_count, last_event_seq=last_seq, table_hashes=table_hashes)
    finally:
        conn.close()


def apply_event(event: Event | dict[str, Any], agent_dir: str | Path | None = None) -> None:
    """Project a single event after it has been durably appended."""
    record = event.to_dict() if isinstance(event, Event) else event
    cfg = _config_from_agent_dir(agent_dir)
    ensure_project_dirs(cfg)
    conn = connect(cfg.db_path)
    try:
        apply_record(record, cfg, conn=conn, require_next=True)
        with conn:
            set_meta(conn, "events_jsonl_sha", file_sha256(cfg.events_path))
    finally:
        conn.close()


def apply_record(
    record: dict[str, Any],
    config: AgentMeshConfig,
    *,
    conn,
    require_next: bool,
) -> None:
    initialize_schema(conn)
    event_seq = int(record["event_seq"])
    last_seq = get_last_event_seq(conn)
    if event_seq <= last_seq:
        return
    if require_next and event_seq != last_seq + 1:
        raise RuntimeError(
            f"projection gap: next event_seq must be {last_seq + 1}, got {event_seq}"
        )

    with conn:
        _project_record(conn, record, config)
        set_last_event_seq(conn, event_seq, str(record.get("occurred_utc", utc_now())))


def read_event_records(events_path: str | Path) -> Iterable[dict[str, Any]]:
    path = Path(events_path)
    if not path.exists():
        return []
    records = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                records.append(json.loads(line))
    return sorted(records, key=lambda item: int(item["event_seq"]))


def table_hashes_for(config: AgentMeshConfig) -> dict[str, str]:
    from agent_mesh.store.sqlite import canonical_table_dump

    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        hashes: dict[str, str] = {}
        for table in ALL_TABLES:
            payload = json_dumps(canonical_table_dump(conn, table)).encode("utf-8")
            hashes[table] = hashlib.sha256(payload).hexdigest()
        return hashes
    finally:
        conn.close()


def file_sha256(path: str | Path) -> str:
    file_path = Path(path)
    if not file_path.exists():
        return hashlib.sha256(b"").hexdigest()
    digest = hashlib.sha256()
    with file_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def projection_is_current(config: AgentMeshConfig) -> bool:
    """Return whether SQLite exactly projects the current event log and code contract.

    Callers that use this result to skip a rebuild must hold the project mail
    lock while checking it, so an append cannot race between the SHA comparison
    and the subsequent read.
    """
    if not config.db_path.exists():
        return False
    expected_sha = file_sha256(config.events_path)
    try:
        conn = connect(config.db_path)
        try:
            return (
                get_meta(conn, "projection_version") == PROJECTION_VERSION
                and get_meta(conn, "events_jsonl_sha") == expected_sha
            )
        finally:
            conn.close()
    except sqlite3.DatabaseError:
        return False


def _project_record(conn, record: dict[str, Any], config: AgentMeshConfig) -> None:
    kind = str(record["kind"])
    validate_event_provenance(kind, record.get("payload", {}), str(record.get("entity_id", "")))
    if kind == "req_created":
        _project_req_created(conn, record, config)
    elif kind == "res_posted":
        _project_res_posted(conn, record)
    elif kind == "req_status_changed":
        _project_req_status_changed(conn, record)
    elif kind == "req_claimed":
        conn.execute(
            "UPDATE messages SET claimed_by=?, claimed_utc=?, updated_utc=?, event_seq=? "
            "WHERE id=? AND kind='request'",
            (
                record["payload"].get("claimed_by"),
                record["occurred_utc"],
                record["occurred_utc"],
                record["event_seq"],
                record["entity_id"],
            ),
        )
    elif kind == "req_unclaimed":
        conn.execute(
            "UPDATE messages SET claimed_by=NULL, claimed_utc=NULL, updated_utc=?, event_seq=? "
            "WHERE id=? AND kind='request'",
            (record["occurred_utc"], record["event_seq"], record["entity_id"]),
        )
    elif kind == "message_ref_added":
        payload = record["payload"]
        conn.execute(
            "INSERT OR IGNORE INTO message_refs(message_id, ref_type, ref_value) VALUES (?, ?, ?)",
            (
                payload.get("source_message_id") or record["entity_id"],
                payload["ref_type"],
                payload["ref_value"],
            ),
        )
    elif kind in DECISION_EVENT_KINDS:
        _project_decision_event(conn, record)
    elif kind in DISPATCH_EVENT_KINDS:
        _project_dispatch_event(conn, record)
    elif kind == "backlog_item_upserted":
        _project_backlog_item_upserted(conn, record)
    elif kind == "backlog_link_added":
        _project_backlog_link_added(conn, record)
    elif kind == "backlog_event_recorded":
        _project_backlog_event_recorded(conn, record)
    elif kind in {
        "decision_reference_observed",
        "decision_reference_resolved",
        "decision_scanner_run_completed",
        "projection_regenerated",
        "index_rebuilt",
        "projection_stale_detected",
        "projection_size_exceeded",
        "message_backfilled",
        "backfill_batch_completed",
        "message_body_stored",
    }:
        _project_ops_event(conn, record)


def _project_req_created(conn, record: dict[str, Any], config: AgentMeshConfig) -> None:
    payload = record["payload"]
    body = str(payload.get("body", ""))
    recipients = payload.get("to", [])
    if isinstance(recipients, str):
        recipients = config.canonical_recipients(recipients)
    meta = {"body": body}
    if "original_to" in payload:
        meta["original_to"] = payload["original_to"]
    meta["response_mode"] = str(payload.get("response_mode") or "single")
    _add_provenance_to_meta(meta, payload)
    _insert_message(
        conn,
        record=record,
        kind="request",
        request_id=None,
        parent_id=None,
        sender=str(payload.get("from", record.get("actor", ""))),
        recipients=recipients,
        feature_id=str(payload.get("feature", "")),
        title=str(payload.get("title", "")),
        summary=None,
        body=body,
        body_authority=body_authority_for_payload(payload),
        body_fidelity=body_fidelity_for_payload(payload),
        status="open",
        meta=meta,
    )
    _insert_refs(conn, record["entity_id"], payload.get("refs", []))
    _project_message_provenance(conn, record, synthetic_edges=[])


def _project_res_posted(conn, record: dict[str, Any]) -> None:
    payload = record["payload"]
    body = str(payload.get("body", ""))
    request_id = str(payload.get("request_id") or record["thread_id"])
    parent_id = str(payload.get("parent_id") or request_id)
    _validate_response_parent(conn, record, request_id, parent_id)
    meta = {"body": body}
    if parent_id:
        meta["parent_id"] = parent_id
    if payload.get("parent_kind"):
        meta["parent_kind"] = payload.get("parent_kind")
    if isinstance(payload.get("authorship_policy"), dict):
        meta["authorship_policy"] = payload["authorship_policy"]
    _add_provenance_to_meta(meta, payload)
    _insert_message(
        conn,
        record=record,
        kind="response",
        request_id=request_id,
        parent_id=parent_id,
        sender=str(payload.get("from", record.get("actor", ""))),
        recipients=[],
        feature_id="",
        title=None,
        summary=str(payload.get("summary", "")),
        body=body,
        body_authority=body_authority_for_payload(payload),
        body_fidelity=body_fidelity_for_payload(payload),
        status="posted",
        meta=meta,
    )
    _insert_refs(conn, record["entity_id"], payload.get("refs", []))
    _project_message_provenance(
        conn,
        record,
        synthetic_edges=[
            {
                "relation": "replied_to",
                "from_ref": record["entity_id"],
                "to_ref": parent_id,
                "synthetic": True,
                "source": "res_posted.parent_id",
            }
        ],
    )


def _validate_response_parent(conn, record: dict[str, Any], request_id: str, parent_id: str) -> None:
    request = conn.execute("SELECT kind, thread_id FROM messages WHERE id=?", (request_id,)).fetchone()
    if request is None or request["kind"] != "request":
        raise RuntimeError(f"RES_REQUEST_MISSING: {record['entity_id']} request_id={request_id}")
    if str(record["thread_id"]) != request_id:
        raise RuntimeError(
            f"RES_THREAD_MISMATCH: {record['entity_id']} thread_id={record['thread_id']} request_id={request_id}"
        )
    parent = conn.execute("SELECT kind, thread_id, request_id FROM messages WHERE id=?", (parent_id,)).fetchone()
    if parent is None:
        raise RuntimeError(f"RES_PARENT_MISSING: {record['entity_id']} parent_id={parent_id}")
    declared_parent_kind = record.get("payload", {}).get("parent_kind")
    if declared_parent_kind is not None and str(declared_parent_kind) != str(parent["kind"]):
        raise RuntimeError(
            f"RES_PARENT_KIND_MISMATCH: {record['entity_id']} parent_id={parent_id} "
            f"declared={declared_parent_kind} actual={parent['kind']}"
        )
    if parent["kind"] == "request":
        if parent_id != request_id:
            raise RuntimeError(
                f"RES_PARENT_REQUEST_MISMATCH: {record['entity_id']} parent_id={parent_id} request_id={request_id}"
            )
        return
    if parent["kind"] == "response":
        if str(parent["thread_id"]) != request_id or str(parent["request_id"]) != request_id:
            raise RuntimeError(
                f"RES_PARENT_THREAD_MISMATCH: {record['entity_id']} parent_id={parent_id} request_id={request_id}"
            )
        return
    raise RuntimeError(f"RES_PARENT_INVALID_KIND: {record['entity_id']} parent_id={parent_id}")


def _project_req_status_changed(conn, record: dict[str, Any]) -> None:
    payload = record["payload"]
    to_status = str(payload.get("to_status", "open"))
    conn.execute(
        "UPDATE messages SET status=?, resolution=?, resolved_utc=?, updated_utc=?, event_seq=? "
        "WHERE id=? AND kind='request'",
        (
            to_status,
            payload.get("reason"),
            record["occurred_utc"] if to_status == "closed" else None,
            record["occurred_utc"],
            record["event_seq"],
            record["entity_id"],
        ),
    )


def _insert_message(
    conn,
    *,
    record: dict[str, Any],
    kind: str,
    request_id: str | None,
    parent_id: str | None,
    sender: str,
    recipients: list[str],
    feature_id: str,
    title: str | None,
    summary: str | None,
    body: str,
    body_authority: str,
    body_fidelity: str | None,
    status: str,
    meta: dict[str, Any],
) -> None:
    body_bytes = body.encode("utf-8")
    body_sha = hashlib.sha256(body_bytes).hexdigest()
    conn.execute(
        """
        INSERT INTO messages(
          id, kind, schema_version, thread_id, request_id, parent_id, sender,
          recipients_json, feature_id, title, summary, body_preview, body_sha, body_path,
          body_bytes, body_media_type, body_authority, body_fidelity, status, resolution,
          resolved_utc, claimed_by,
          claimed_utc, has_fenced_json, json_packet_type, created_utc, updated_utc,
          event_seq, source_file, source_line_start, source_line_end, import_batch_id, meta_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, 'text/markdown',
          ?, ?, ?, NULL, NULL, NULL, NULL, ?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, ?)
        ON CONFLICT(id) DO UPDATE SET
          updated_utc=excluded.updated_utc,
          event_seq=excluded.event_seq,
          body_authority=excluded.body_authority,
          body_fidelity=excluded.body_fidelity,
          meta_json=excluded.meta_json
        """,
        (
            record["entity_id"],
            kind,
            int(record.get("schema_version", 1)),
            record["thread_id"],
            request_id,
            parent_id,
            sender,
            json_dumps(recipients),
            feature_id,
            title,
            summary,
            body[:500],
            body_sha,
            len(body_bytes),
            body_authority,
            body_fidelity,
            status,
            1 if "```json" in body else 0,
            _packet_type(body),
            record["occurred_utc"],
            record["occurred_utc"],
            record["event_seq"],
            json_dumps(meta),
        ),
    )


def _insert_refs(conn, message_id: str, refs: list[Any]) -> None:
    for ref in refs:
        if isinstance(ref, dict):
            ref_type = str(ref.get("type", "unknown"))
            ref_value = str(ref.get("value", ""))
        else:
            ref_value = str(ref)
            ref_type = _infer_ref_type(ref_value)
        if ref_value:
            conn.execute(
                "INSERT OR IGNORE INTO message_refs(message_id, ref_type, ref_value) VALUES (?, ?, ?)",
                (message_id, ref_type, ref_value),
            )


def _add_provenance_to_meta(meta: dict[str, Any], payload: dict[str, Any]) -> None:
    for key in (
        "source_context_refs",
        "causal_edges",
        "body_authority",
        "body_fidelity",
        "source_context_status",
        "source_selection",
    ):
        if key in payload:
            meta[key] = payload[key]


def _project_message_provenance(
    conn,
    record: dict[str, Any],
    *,
    synthetic_edges: list[dict[str, Any]],
) -> None:
    payload = record["payload"]
    message_id = str(record["entity_id"])
    event_seq = int(record["event_seq"])
    conn.execute("DELETE FROM message_source_context_refs WHERE message_id=?", (message_id,))
    conn.execute("DELETE FROM message_causal_edges WHERE message_id=?", (message_id,))
    conn.execute("DELETE FROM message_source_selection WHERE message_id=?", (message_id,))

    for index, item in enumerate(payload.get("source_context_refs") or []):
        conn.execute(
            """
            INSERT INTO message_source_context_refs(
              message_id, ref_index, channel, source_kind, source_id, source_event_id,
              source_uri, role, observed_utc, confidence, event_seq, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                message_id,
                index,
                _string_or_none(item.get("channel")),
                _string_or_none(item.get("source_kind") or item.get("source_type")),
                _first_string(item, "source_id", "turn_id", "message_id", "id"),
                _first_string(item, "source_event_id", "event_id"),
                _string_or_none(item.get("source_uri")),
                _string_or_none(item.get("role")),
                _string_or_none(item.get("observed_utc")),
                confidence_value(item.get("confidence")),
                event_seq,
                json_dumps(
                    _extra_fields(
                        item,
                        {
                            "channel",
                            "source_kind",
                            "source_type",
                            "source_id",
                            "turn_id",
                            "message_id",
                            "id",
                            "source_event_id",
                            "event_id",
                            "source_uri",
                            "role",
                            "observed_utc",
                            "confidence",
                        },
                    )
                ),
            ),
        )

    explicit_edges = [_normalized_edge(item, record) for item in payload.get("causal_edges") or []]
    seen = {(edge["relation"], edge.get("from_ref"), edge.get("to_ref")) for edge in explicit_edges}
    edges = list(explicit_edges)
    for item in synthetic_edges:
        edge = _normalized_edge(item, record)
        key = (edge["relation"], edge.get("from_ref"), edge.get("to_ref"))
        if key not in seen:
            edges.append(edge)
            seen.add(key)

    for index, edge in enumerate(edges):
        conn.execute(
            """
            INSERT INTO message_causal_edges(
              message_id, edge_index, relation, from_ref, to_ref, confidence, event_seq, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                message_id,
                index,
                str(edge["relation"]),
                _string_or_none(edge.get("from_ref")),
                _string_or_none(edge.get("to_ref")),
                confidence_value(edge.get("confidence")),
                event_seq,
                json_dumps(
                    _extra_fields(
                        edge,
                        {"relation", "from_ref", "from_id", "to_ref", "to_id", "confidence"},
                    )
                ),
            ),
        )

    source_selection = payload.get("source_selection")
    if isinstance(source_selection, dict):
        conn.execute(
            """
            INSERT INTO message_source_selection(
              message_id, mode, confidence, selected_by, requires_review, event_seq, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                message_id,
                str(source_selection["mode"]),
                float(source_selection["confidence"]),
                str(source_selection["selected_by"]),
                1 if source_selection["requires_review"] else 0,
                event_seq,
                json_dumps(
                    _extra_fields(
                        source_selection,
                        {"mode", "confidence", "selected_by", "requires_review"},
                    )
                ),
            ),
        )


def _normalized_edge(item: dict[str, Any], record: dict[str, Any]) -> dict[str, Any]:
    edge = dict(item)
    if "from_ref" not in edge:
        edge["from_ref"] = edge.get("from_id") or edge.get("source_ref") or edge.get("source_id")
    if "to_ref" not in edge:
        edge["to_ref"] = edge.get("to_id") or edge.get("target_ref") or record["entity_id"]
    return edge


def _first_string(item: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = _string_or_none(item.get(key))
        if value:
            return value
    return None


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _extra_fields(item: dict[str, Any], known: set[str]) -> dict[str, Any]:
    return {key: value for key, value in item.items() if key not in known}


def _project_backlog_item_upserted(conn, record: dict[str, Any]) -> None:
    payload = record["payload"]
    item_id = str(payload.get("id") or record["entity_id"])
    refs = payload.get("refs", [])
    if not isinstance(refs, list):
        refs = []
    meta = {
        key: value
        for key, value in payload.items()
        if key not in {
            "id",
            "title",
            "item_type",
            "summary",
            "root_cause_summary",
            "architectural_category",
            "status",
            "priority",
            "launch_scope",
            "release_phase",
            "production_state",
            "disposition",
            "owner_hint",
            "lane",
            "notes",
            "refs",
        }
    }
    conn.execute(
        """
        INSERT INTO backlog_items(
          id, title, item_type, summary, root_cause_summary, architectural_category,
          status, priority, launch_scope, release_phase, production_state, disposition,
          owner_hint, lane, notes, refs_json, created_utc, updated_utc, event_seq, meta_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
          title=excluded.title,
          item_type=excluded.item_type,
          summary=excluded.summary,
          root_cause_summary=excluded.root_cause_summary,
          architectural_category=excluded.architectural_category,
          status=excluded.status,
          priority=excluded.priority,
          launch_scope=excluded.launch_scope,
          release_phase=excluded.release_phase,
          production_state=excluded.production_state,
          disposition=excluded.disposition,
          owner_hint=excluded.owner_hint,
          lane=excluded.lane,
          notes=excluded.notes,
          refs_json=excluded.refs_json,
          updated_utc=excluded.updated_utc,
          event_seq=excluded.event_seq,
          meta_json=excluded.meta_json
        """,
        (
            item_id,
            str(payload.get("title", "")),
            payload.get("item_type"),
            payload.get("summary"),
            payload.get("root_cause_summary"),
            payload.get("architectural_category"),
            str(payload.get("status", "open")),
            payload.get("priority"),
            payload.get("launch_scope"),
            payload.get("release_phase"),
            payload.get("production_state"),
            payload.get("disposition"),
            payload.get("owner_hint"),
            payload.get("lane"),
            payload.get("notes"),
            json_dumps(refs),
            record["occurred_utc"],
            record["occurred_utc"],
            record["event_seq"],
            json_dumps(meta),
        ),
    )


def _project_backlog_link_added(conn, record: dict[str, Any]) -> None:
    payload = record["payload"]
    item_id = str(payload.get("item_id") or record["entity_id"])
    conn.execute(
        """
        INSERT OR REPLACE INTO backlog_item_links(link_event_id, item_id, ref_type, ref_value, created_utc, event_seq)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            record["event_id"],
            item_id,
            str(payload.get("ref_type", "unknown")),
            str(payload.get("ref_value", "")),
            record["occurred_utc"],
            record["event_seq"],
        ),
    )


def _project_backlog_event_recorded(conn, record: dict[str, Any]) -> None:
    payload = record["payload"]
    item_id = str(payload.get("item_id") or record["entity_id"])
    details = payload.get("details", {})
    if not isinstance(details, dict):
        details = {"value": details}
    conn.execute(
        """
        INSERT OR REPLACE INTO backlog_events(
          event_id, item_id, event_type, actor, created_utc, details_json, event_seq
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            record["event_id"],
            item_id,
            str(payload.get("event_type", record["kind"])),
            str(payload.get("actor") or record.get("actor", "")),
            record["occurred_utc"],
            json_dumps(details),
            record["event_seq"],
        ),
    )


def _project_dispatch_event(conn, record: dict[str, Any]) -> None:
    """Project one dispatch-domain event, folding lifecycle state into dispatch_runs/dispatch_leases.

    Re-enforces the strict allow-list at replay (a hand-edited or imported log must not smuggle a
    body in), then applies the per-kind projection + replay-time STOP-LINE checks (see
    ``docs/domains/dispatch.md`` §6). Raises ``DispatchStopLine`` on any violation; the surrounding
    ``apply_record`` transaction rolls back so no partial state is committed.
    """
    kind = str(record["kind"])
    payload = record.get("payload", {}) or {}
    try:
        validate_dispatch_payload(kind, payload)
    except DispatchSchemaError as exc:
        raise DispatchStopLine(exc.code, exc.detail) from exc
    event_seq = int(record["event_seq"])
    if kind == "dispatch_run_planned":
        _project_dispatch_run_planned(conn, record, payload, event_seq)
    elif kind == "dispatch_run_blocked":
        _project_dispatch_run_blocked(conn, record, payload, event_seq)
    elif kind == "dispatch_lease_acquired":
        _project_dispatch_lease_acquired(conn, payload, event_seq)
    elif kind == "dispatch_lease_released":
        _project_dispatch_lease_released(conn, payload, event_seq)
    elif kind == "dispatch_run_started":
        _project_dispatch_run_started(conn, payload, event_seq)
    elif kind in ("dispatch_run_completed", "dispatch_run_failed"):
        _project_dispatch_run_terminal(conn, kind, payload, event_seq)


def _require_known_request(conn, input_message_id: str, run_id: str) -> None:
    row = conn.execute(
        "SELECT 1 FROM messages WHERE id=? AND kind='request'", (input_message_id,)
    ).fetchone()
    if row is None:
        raise DispatchStopLine(
            DISPATCH_RUN_UNKNOWN_MESSAGE, f"{run_id} input_message_id={input_message_id}"
        )


def _open_lease_for_run(conn, run_id: str):
    return conn.execute(
        "SELECT lease_id FROM dispatch_leases WHERE run_id=? AND status='open'", (run_id,)
    ).fetchone()


def _request_thread_for_run(conn, input_message_id) -> str | None:
    """The canonical thread of a run's input request (the authority for the run's thread, §1)."""
    if not input_message_id:
        return None
    row = conn.execute(
        "SELECT thread_id FROM messages WHERE id=? AND kind='request'", (input_message_id,)
    ).fetchone()
    return str(row["thread_id"]) if row is not None and row["thread_id"] else None


def _project_dispatch_run_planned(conn, record, payload, event_seq: int) -> None:
    run_id = str(payload["run_id"])
    input_message_id = str(payload["input_message_id"])
    _require_known_request(conn, input_message_id, run_id)
    grounding = payload.get("grounding", {}) or {}
    conn.execute(
        """
        INSERT INTO dispatch_runs(
          run_id, run_mode, input_message_id, target_agent, gen_ai_system, model,
          session_key, session_key_source, session_uuid, wave, classification, gate,
          gate_reason_code, requires_gate_json, grounding_complete, grounding_digest,
          plan_artifact_hash, adapter_capabilities_json, target_event_seq, response_mode,
          status, planned, thread_id, planned_utc, event_seq
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?,?)
        ON CONFLICT(run_id) DO UPDATE SET
          run_mode=excluded.run_mode, input_message_id=excluded.input_message_id,
          target_agent=excluded.target_agent, gen_ai_system=excluded.gen_ai_system,
          model=excluded.model, session_key=excluded.session_key,
          session_key_source=excluded.session_key_source, session_uuid=excluded.session_uuid,
          wave=excluded.wave, classification=excluded.classification, gate=excluded.gate,
          gate_reason_code=excluded.gate_reason_code,
          requires_gate_json=excluded.requires_gate_json,
          grounding_complete=excluded.grounding_complete,
          grounding_digest=excluded.grounding_digest,
          plan_artifact_hash=excluded.plan_artifact_hash,
          adapter_capabilities_json=excluded.adapter_capabilities_json,
          target_event_seq=excluded.target_event_seq, response_mode=excluded.response_mode,
          status=excluded.status, planned=1, thread_id=excluded.thread_id,
          planned_utc=excluded.planned_utc, event_seq=excluded.event_seq
        """,
        (
            run_id,
            payload["run_mode"],
            input_message_id,
            payload["target_agent"],
            payload.get("gen_ai_system"),
            payload.get("model"),
            payload.get("session_key"),
            payload.get("session_key_source"),
            payload.get("session_uuid"),
            payload.get("wave"),
            payload.get("classification"),
            payload.get("gate"),
            payload.get("gate_reason_code"),
            json_dumps(payload.get("requires_gate", [])),
            1 if grounding.get("complete") else 0,
            grounding.get("digest"),
            payload.get("plan_artifact_hash"),
            json_dumps(payload.get("adapter_capabilities", {})),
            payload.get("target_event_seq"),
            payload.get("response_mode"),
            payload.get("status"),
            str(record.get("thread_id", "")),
            payload.get("planned_utc"),
            event_seq,
        ),
    )


def _project_dispatch_run_blocked(conn, record, payload, event_seq: int) -> None:
    run_id = str(payload["run_id"])
    input_message_id = str(payload["input_message_id"])
    _require_known_request(conn, input_message_id, run_id)
    conn.execute(
        """
        INSERT INTO dispatch_runs(
          run_id, run_mode, input_message_id, target_agent, gate,
          block_reason_codes_json, missing_count, status, thread_id, planned_utc, event_seq
        ) VALUES (?,?,?,?,?,?,?,'blocked',?,?,?)
        ON CONFLICT(run_id) DO UPDATE SET
          gate=excluded.gate, block_reason_codes_json=excluded.block_reason_codes_json,
          missing_count=excluded.missing_count, status='blocked', event_seq=excluded.event_seq
        """,
        (
            run_id,
            payload["run_mode"],
            input_message_id,
            payload["target_agent"],
            payload.get("gate"),
            json_dumps(payload.get("block_reason_codes", [])),
            payload.get("missing_count"),
            str(record.get("thread_id", "")),
            payload.get("planned_utc"),
            event_seq,
        ),
    )


def _project_dispatch_lease_acquired(conn, payload, event_seq: int) -> None:
    run_id = str(payload["run_id"])
    known_run = conn.execute(
        "SELECT 1 FROM dispatch_runs WHERE run_id=?", (run_id,)
    ).fetchone()
    if known_run is None:
        # The contract §4 FK dispatch_leases.run_id -> dispatch_runs.run_id, enforced at replay
        # (the codebase enforces FKs via stop-lines, not SQL FK clauses on log-rebuilt tables).
        raise DispatchStopLine(
            DISPATCH_LEASE_UNKNOWN_RUN, f"lease={payload.get('lease_id')} run_id={run_id}"
        )
    try:
        conn.execute(
            """
            INSERT INTO dispatch_leases(
              lease_id, run_id, input_message_id, target_agent, session_uuid,
              ttl_seconds, status, created_utc, event_seq
            ) VALUES (?,?,?,?,?,?,'open',?,?)
            """,
            (
                payload["lease_id"],
                payload["run_id"],
                payload["input_message_id"],
                payload["target_agent"],
                payload.get("session_uuid"),
                payload.get("ttl_seconds"),
                payload.get("created_utc"),
                event_seq,
            ),
        )
    except sqlite3.IntegrityError as exc:
        # The lease_id PK or the uq_dispatch_leases_open partial-unique index rejected the row:
        # a second open lease for the same (input_message_id, target_agent), or a duplicate id.
        raise DispatchStopLine(
            DISPATCH_LEASE_DUPLICATE,
            f"{payload.get('input_message_id')}/{payload.get('target_agent')}",
        ) from exc


def _project_dispatch_lease_released(conn, payload, event_seq: int) -> None:
    lease_id = str(payload["lease_id"])
    run_id = str(payload["run_id"])
    reason = payload.get("reason")
    superseded_by = payload.get("superseded_by_run_id")
    # A release must target an EXISTING, OPEN lease that belongs to the named run; otherwise the
    # UPDATE would silently no-op and the release fact would live only in the log, invisible to the
    # projection and to `agent-q dispatches verify`. Fail closed instead.
    lease = conn.execute(
        "SELECT run_id, status FROM dispatch_leases WHERE lease_id=?", (lease_id,)
    ).fetchone()
    if lease is None:
        raise DispatchStopLine(DISPATCH_LEASE_RELEASE_INVALID, f"unknown lease={lease_id}")
    if lease["status"] != "open":
        raise DispatchStopLine(
            DISPATCH_LEASE_RELEASE_INVALID, f"lease not open: {lease_id} status={lease['status']}"
        )
    if str(lease["run_id"]) != run_id:
        raise DispatchStopLine(
            DISPATCH_LEASE_RELEASE_INVALID,
            f"lease={lease_id} run mismatch event_run={run_id} lease_run={lease['run_id']}",
        )
    if reason == "superseded":
        if not superseded_by or superseded_by == run_id:
            raise DispatchStopLine(
                DISPATCH_SUPERSEDE_INVALID, f"lease={lease_id} superseded_by={superseded_by}"
            )
        # The contract requires the successor to be a run with a dispatch_run_planned (§6); a
        # blocked-only run also creates a row, so check the durable `planned` flag, not mere
        # existence (status mutates as the run advances, so it cannot be the discriminator).
        target = conn.execute(
            "SELECT planned FROM dispatch_runs WHERE run_id=?", (superseded_by,)
        ).fetchone()
        if target is None or not target["planned"]:
            raise DispatchStopLine(
                DISPATCH_SUPERSEDE_INVALID,
                f"superseded_by_run_id is not a planned run: {superseded_by}",
            )
    cur = conn.execute(
        """
        UPDATE dispatch_leases SET status='released', reason=?, superseded_by_run_id=?,
          released_utc=?, event_seq=? WHERE lease_id=? AND status='open'
        """,
        (reason, superseded_by, payload.get("released_utc"), event_seq, lease_id),
    )
    if cur.rowcount != 1:
        raise DispatchStopLine(
            DISPATCH_LEASE_RELEASE_INVALID, f"release affected {cur.rowcount} rows: {lease_id}"
        )


def _project_dispatch_run_started(conn, payload, event_seq: int) -> None:
    run_id = str(payload["run_id"])
    if _open_lease_for_run(conn, run_id) is None:
        raise DispatchStopLine(DISPATCH_RUN_WITHOUT_LEASE, run_id)
    conn.execute(
        "UPDATE dispatch_runs SET status='started', started_utc=?, event_seq=? WHERE run_id=?",
        (payload.get("started_utc"), event_seq, run_id),
    )


def _project_dispatch_run_terminal(conn, kind: str, payload, event_seq: int) -> None:
    run_id = str(payload["run_id"])
    row = conn.execute(
        "SELECT started_utc, input_message_id FROM dispatch_runs WHERE run_id=?", (run_id,)
    ).fetchone()
    started = row is not None and row["started_utc"] is not None
    open_lease = _open_lease_for_run(conn, run_id) is not None
    if not (started and open_lease):
        raise DispatchStopLine(
            DISPATCH_RUN_TERMINAL_WITHOUT_STARTED_OR_LEASE,
            f"{run_id} started={started} open_lease={open_lease}",
        )
    if kind == "dispatch_run_completed":
        output_message_id = str(payload["output_message_id"])
        out = conn.execute(
            "SELECT thread_id FROM messages WHERE id=? AND kind='response'", (output_message_id,)
        ).fetchone()
        if out is None:
            raise DispatchStopLine(
                DISPATCH_OUTPUT_MESSAGE_INVALID, f"{run_id} output_message_id={output_message_id}"
            )
        # Bind against the input request's CANONICAL thread (resolved from messages), never the run's
        # self-declared/possibly-empty stored thread. Fail closed if it cannot be established.
        req_thread = _request_thread_for_run(conn, row["input_message_id"] if row is not None else None)
        if not req_thread or str(out["thread_id"]) != str(req_thread):
            raise DispatchStopLine(
                DISPATCH_OUTPUT_THREAD_MISMATCH,
                f"{run_id} output_thread={out['thread_id']} req_thread={req_thread!r}",
            )
        conn.execute(
            """
            UPDATE dispatch_runs SET status='completed', output_message_id=?, input_tokens=?,
              cache_read_input_tokens=?, cache_creation_input_tokens=?, total_input_tokens=?,
              completed_utc=?, event_seq=? WHERE run_id=?
            """,
            (
                output_message_id,
                payload.get("input_tokens"),
                payload.get("cache_read_input_tokens"),
                payload.get("cache_creation_input_tokens"),
                payload.get("total_input_tokens"),
                payload.get("completed_utc"),
                event_seq,
                run_id,
            ),
        )
    else:  # dispatch_run_failed
        conn.execute(
            "UPDATE dispatch_runs SET status='failed', error_class=?, failed_utc=?, event_seq=? "
            "WHERE run_id=?",
            (payload.get("error_class"), payload.get("failed_utc"), event_seq, run_id),
        )


def _project_decision_event(conn, record: dict[str, Any]) -> None:
    kind = record["kind"]
    payload = record["payload"]
    if kind == "decision_proposed":
        _project_decision_proposed(conn, record)
        return

    dec_ulid = _decision_id_for_event(conn, record)
    if dec_ulid is None:
        raise DecisionStopLine(DECISION_PARENT_MISSING, record["entity_id"])

    if kind == "decision_accepted":
        row = conn.execute(
            "SELECT tier, meta_json FROM decisions WHERE dec_ulid=?", (dec_ulid,)
        ).fetchone()
        tier = row["tier"]
        meta = json_loads(row["meta_json"], {})
        _append_decision_log(conn, dec_ulid, record)
        if not _decision_quorum_reached(meta, record):
            return
        status = "in_force" if tier in {
            "architecture_contract",
            "production_invariant",
            "compliance_security",
        } else "accepted"
        conn.execute(
            "UPDATE decisions SET status=?, accepted_utc=?, in_force_utc=?, event_seq=? "
            "WHERE dec_ulid=?",
            (
                status,
                record["occurred_utc"],
                record["occurred_utc"] if status == "in_force" else None,
                record["event_seq"],
                dec_ulid,
            ),
        )
    elif kind == "decision_revisited":
        _append_decision_log(conn, dec_ulid, record)
        new_id = payload.get("new_decision_id")
        if new_id and resolve_decision(conn, str(new_id)) is None:
            raise DecisionStopLine(DECISION_PARENT_MISSING, str(new_id))
    elif kind == "decision_superseded":
        successor = resolve_decision(conn, str(payload.get("superseded_by", "")))
        if successor is None:
            raise DecisionStopLine(DECISION_SUPERSEDE_TARGET_INVALID, str(payload.get("superseded_by")))
        _ensure_supersede_target_valid(conn, successor)
        _ensure_no_supersede_cycle(conn, dec_ulid, successor)
        conn.execute(
            "UPDATE decisions SET status='superseded', superseded_by=?, event_seq=? WHERE dec_ulid=?",
            (successor, record["event_seq"], dec_ulid),
        )
        conn.execute(
            "UPDATE decisions SET supersedes=?, event_seq=? WHERE dec_ulid=?",
            (dec_ulid, record["event_seq"], successor),
        )
    elif kind == "decision_retired":
        conn.execute(
            "UPDATE decisions SET status='retired', retired_utc=?, event_seq=? WHERE dec_ulid=?",
            (record["occurred_utc"], record["event_seq"], dec_ulid),
        )
    elif kind == "decision_rejected":
        conn.execute(
            "UPDATE decisions SET status='rejected', event_seq=? WHERE dec_ulid=?",
            (record["event_seq"], dec_ulid),
        )
    elif kind == "decision_metadata_updated":
        _project_decision_metadata_updated(conn, record, dec_ulid)
    elif kind == "decision_assumption_violated":
        conn.execute(
            "UPDATE decision_assumptions SET status='violated', invalidated_event_id=? "
            "WHERE dec_ulid=? AND assumption_id=?",
            (record["event_id"], dec_ulid, payload.get("assumption_id")),
        )
    elif kind == "decision_check_failed":
        _append_decision_log(conn, dec_ulid, record)
    elif kind == "decision_drift_detected":
        conn.execute(
            "UPDATE decision_verifications SET last_verified_utc=?, last_outcome='fail', "
            "last_event_id=? WHERE dec_ulid=? AND command=?",
            (record["occurred_utc"], record["event_id"], dec_ulid, payload.get("command")),
        )


def _project_decision_proposed(conn, record: dict[str, Any]) -> None:
    payload = record["payload"]
    dec_ulid = record["entity_id"]
    human_id = str(payload["human_id"])
    parent = parent_human_id(human_id)
    if not DECISION_ID_RE.match(human_id):
        raise ValueError(f"invalid decision human_id: {human_id}")
    existing = resolve_decision(conn, human_id)
    if existing is not None and existing != dec_ulid:
        raise DecisionStopLine(DECISION_HUMAN_ID_COLLISION, human_id)

    supersedes = payload.get("supersedes")
    supersedes_ulid = None
    if supersedes:
        supersedes_ulid = resolve_decision(conn, str(supersedes))
        if supersedes_ulid is None:
            raise DecisionStopLine(DECISION_SUPERSEDE_TARGET_INVALID, str(supersedes))
        _ensure_supersede_target_valid(conn, supersedes_ulid)
        _ensure_no_supersede_cycle(conn, supersedes_ulid, dec_ulid)

    meta = {
        "context": payload.get("context", ""),
        "decision": payload.get("decision", ""),
        "rejected_alternatives": payload.get("rejected_alternatives", []),
        "consequences": payload.get("consequences", []),
        "review_policy": payload.get("review_policy", {}),
        "exemptions": payload.get("exemptions", []),
        "generated_artifact_paths": payload.get("generated_artifact_paths", []),
    }
    verification = payload.get("verification", [])
    drift_risk = _max_drift_risk(item.get("drift_risk") for item in verification if isinstance(item, dict))
    conn.execute(
        """
        INSERT INTO decisions(
          dec_ulid, human_id, parent_human_id, title, tier, status, enforcement_mode, owner,
          body_sha, body_path, body_bytes, body_media_type, superseded_by, supersedes,
          proposed_utc, accepted_utc, in_force_utc, retired_utc, last_verified_utc, drift_risk,
          event_seq, meta_json
        ) VALUES (?, ?, ?, ?, ?, 'proposed', ?, ?, ?, ?, ?, 'text/markdown', NULL, ?,
          ?, NULL, NULL, NULL, NULL, ?, ?, ?)
        """,
        (
            dec_ulid,
            human_id,
            parent,
            payload["title"],
            payload["tier"],
            payload.get("enforcement_mode") or enforcement_for_tier(str(payload["tier"])),
            payload.get("owner"),
            payload["body_sha"],
            payload.get("body_path"),
            int(payload["body_bytes"]),
            supersedes_ulid,
            record["occurred_utc"],
            drift_risk,
            record["event_seq"],
            json_dumps(meta),
        ),
    )
    aliases = [human_id, *payload.get("aliases", [])]
    for alias in aliases:
        _insert_alias(conn, str(alias), dec_ulid, is_primary=(alias == human_id))
    for pattern in payload.get("affected_code_globs", []):
        _insert_glob(conn, dec_ulid, str(pattern), "affected")
    for pattern in payload.get("exemptions", []):
        _insert_glob(conn, dec_ulid, str(pattern), "exempt")
    for pattern in payload.get("generated_artifact_paths", []):
        _insert_glob(conn, dec_ulid, str(pattern), "generated")
    for check in payload.get("required_checks", []):
        conn.execute(
            "INSERT OR IGNORE INTO decision_checks(dec_ulid, check_name) VALUES (?, ?)",
            (dec_ulid, str(check)),
        )
    for item in verification:
        if isinstance(item, dict):
            conn.execute(
                "INSERT OR IGNORE INTO decision_verifications("
                "dec_ulid, command, expected_signal, runtime_cost, drift_risk, "
                "last_verified_utc, last_outcome, last_event_id"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, NULL)",
                (
                    dec_ulid,
                    str(item.get("command", "")),
                    str(item.get("expected_signal", "")),
                    item.get("runtime_cost"),
                    item.get("drift_risk"),
                    item.get("last_verified_utc"),
                    item.get("last_outcome"),
                ),
            )
    for index, item in enumerate(payload.get("assumptions", []), start=1):
        if isinstance(item, dict):
            assumption_id = str(item.get("id") or item.get("assumption_id") or f"A{index}")
            text = str(item.get("text", ""))
            refs = item.get("references", [])
        else:
            assumption_id = f"A{index}"
            text = str(item)
            refs = []
        conn.execute(
            "INSERT OR IGNORE INTO decision_assumptions("
            "dec_ulid, assumption_id, text, references_json, status, invalidated_event_id"
            ") VALUES (?, ?, ?, ?, 'active', NULL)",
            (dec_ulid, assumption_id, text, json_dumps(refs)),
        )
    evidence = payload.get("evidence", {})
    if isinstance(evidence, dict):
        for kind, values in evidence.items():
            if not isinstance(values, list):
                values = [values]
            for value in values:
                conn.execute(
                    "INSERT OR IGNORE INTO decision_evidence("
                    "dec_ulid, evidence_kind, ref_value"
                    ") VALUES (?, ?, ?)",
                    (dec_ulid, str(kind), str(value)),
                )
    for tag in payload.get("tags", []):
        conn.execute(
            "INSERT OR IGNORE INTO decision_tags(dec_ulid, tag) VALUES (?, ?)",
            (dec_ulid, str(tag)),
        )


def _project_decision_metadata_updated(conn, record: dict[str, Any], dec_ulid: str) -> None:
    fields = record["payload"].get("fields_changed", {})
    if not isinstance(fields, dict):
        return
    if "human_id" in fields:
        old_new = fields["human_id"]
        if isinstance(old_new, list) and len(old_new) == 2:
            old_id, new_id = str(old_new[0]), str(old_new[1])
            existing = resolve_decision(conn, new_id)
            if existing is not None and existing != dec_ulid:
                raise DecisionStopLine(DECISION_ALIAS_FORK, new_id)
            conn.execute(
                "UPDATE decision_aliases SET is_primary=0 WHERE dec_ulid=?", (dec_ulid,)
            )
            _insert_alias(conn, old_id, dec_ulid, is_primary=False)
            _insert_alias(conn, new_id, dec_ulid, is_primary=True)
            conn.execute(
                "UPDATE decisions SET human_id=?, parent_human_id=?, event_seq=? WHERE dec_ulid=?",
                (new_id, parent_human_id(new_id), record["event_seq"], dec_ulid),
            )
    simple_columns = {
        "title": "title",
        "owner": "owner",
        "body_sha": "body_sha",
        "body_path": "body_path",
        "body_bytes": "body_bytes",
    }
    for field_name, column_name in simple_columns.items():
        if field_name not in fields:
            continue
        new_value = _decision_changed_value(fields[field_name])
        conn.execute(
            f"UPDATE decisions SET {column_name}=?, event_seq=? WHERE dec_ulid=?",
            (new_value, record["event_seq"], dec_ulid),
        )
    if "tier" in fields:
        new_tier = str(_decision_changed_value(fields["tier"]) or "").strip()
        if not new_tier:
            raise ValueError("decision tier must not be empty")
        conn.execute(
            "UPDATE decisions SET tier=?, enforcement_mode=?, event_seq=? WHERE dec_ulid=?",
            (new_tier, enforcement_for_tier(new_tier), record["event_seq"], dec_ulid),
        )
    meta_fields = {
        "context",
        "decision",
        "rejected_alternatives",
        "consequences",
        "review_policy",
        "exemptions",
        "generated_artifact_paths",
    }
    if meta_fields.intersection(fields):
        row = conn.execute(
            "SELECT meta_json FROM decisions WHERE dec_ulid=?", (dec_ulid,)
        ).fetchone()
        meta = json_loads(row["meta_json"] if row else None, {})
        if not isinstance(meta, dict):
            meta = {}
        for field_name in meta_fields.intersection(fields):
            meta[field_name] = _decision_changed_value(fields[field_name])
        conn.execute(
            "UPDATE decisions SET meta_json=?, event_seq=? WHERE dec_ulid=?",
            (json_dumps(meta), record["event_seq"], dec_ulid),
        )
    if "status" in fields:
        old_new = fields["status"]
        new_status = _decision_changed_value(old_new)
        reset_approval = str(new_status) == "proposed"
        conn.execute(
            "UPDATE decisions SET status=?, "
            "accepted_utc=CASE WHEN ? THEN NULL ELSE accepted_utc END, "
            "in_force_utc=CASE WHEN ? THEN NULL ELSE COALESCE(in_force_utc, ?) END, "
            "event_seq=? "
            "WHERE dec_ulid=?",
            (
                str(new_status),
                reset_approval,
                reset_approval,
                record["occurred_utc"] if str(new_status) == "in_force" else None,
                record["event_seq"],
                dec_ulid,
            ),
        )
    _append_decision_log(conn, dec_ulid, record)


def _decision_changed_value(value: Any) -> Any:
    if isinstance(value, list) and len(value) == 2:
        return value[1]
    return value


def _project_ops_event(conn, record: dict[str, Any]) -> None:
    if record["kind"] == "decision_reference_resolved":
        payload = record["payload"]
        dec_ulid = resolve_decision(conn, str(payload.get("dec_ulid") or payload.get("decision_id")))
        if dec_ulid:
            conn.execute(
                "INSERT OR REPLACE INTO decision_references_in_code("
                "dec_ulid, file_path, line_start, line_end, reference_form, commit_sha, scanner_run_id"
                ") VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    dec_ulid,
                    payload["file_path"],
                    int(payload["line_start"]),
                    int(payload["line_end"]),
                    payload["reference_form"],
                    payload.get("commit_sha"),
                    payload["scanner_run_id"],
                ),
            )
    _ = conn


def _decision_id_for_event(conn, record: dict[str, Any]) -> str | None:
    payload = record["payload"]
    candidates = [
        payload.get("decision_id"),
        record.get("entity_id"),
    ]
    for candidate in candidates:
        if candidate:
            resolved = resolve_decision(conn, str(candidate))
            if resolved:
                return resolved
    return None


def _insert_alias(conn, human_id: str, dec_ulid: str, *, is_primary: bool) -> None:
    existing_rows = conn.execute(
        "SELECT dec_ulid FROM decision_aliases WHERE human_id=?", (human_id,)
    ).fetchall()
    existing = {row["dec_ulid"] for row in existing_rows}
    if existing and existing != {dec_ulid}:
        raise DecisionStopLine(DECISION_ALIAS_FORK, human_id)
    conn.execute(
        "INSERT OR REPLACE INTO decision_aliases(human_id, dec_ulid, is_primary) VALUES (?, ?, ?)",
        (human_id, dec_ulid, 1 if is_primary else 0),
    )


def _insert_glob(conn, dec_ulid: str, pattern: str, kind: str) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO decision_globs(dec_ulid, pattern, kind) VALUES (?, ?, ?)",
        (dec_ulid, pattern, kind),
    )


def _ensure_supersede_target_valid(conn, dec_ulid: str) -> None:
    row = conn.execute("SELECT status FROM decisions WHERE dec_ulid=?", (dec_ulid,)).fetchone()
    if row is None or row["status"] not in {"accepted", "in_force"}:
        raise DecisionStopLine(DECISION_SUPERSEDE_TARGET_INVALID, dec_ulid)


def _ensure_no_supersede_cycle(conn, old_dec: str, new_dec: str) -> None:
    current: str | None = new_dec
    seen = {old_dec}
    while current:
        if current in seen:
            raise DecisionStopLine(DECISION_SUPERSEDE_CYCLE, f"{old_dec}->{new_dec}")
        seen.add(current)
        row = conn.execute(
            "SELECT superseded_by FROM decisions WHERE dec_ulid=?", (current,)
        ).fetchone()
        current = row["superseded_by"] if row else None


def _append_decision_log(conn, dec_ulid: str, record: dict[str, Any]) -> None:
    row = conn.execute("SELECT meta_json FROM decisions WHERE dec_ulid=?", (dec_ulid,)).fetchone()
    meta = json_loads(row["meta_json"] if row else None, {})
    log = list(meta.get("event_log", []))
    log.append(
        {
            "event_id": record["event_id"],
            "kind": record["kind"],
            "occurred_utc": record["occurred_utc"],
            "payload": record["payload"],
        }
    )
    meta["event_log"] = log
    conn.execute(
        "UPDATE decisions SET meta_json=?, event_seq=? WHERE dec_ulid=?",
        (json_dumps(meta), record["event_seq"], dec_ulid),
    )


def _decision_quorum_reached(meta: dict[str, Any], current_record: dict[str, Any]) -> bool:
    review_policy = meta.get("review_policy", {})
    if not isinstance(review_policy, dict):
        return True
    required = review_policy.get("required_reviewers", [])
    if not required:
        return True
    quorum = int(review_policy.get("approval_quorum") or len(required))
    accepted_by = {
        str(item.get("payload", {}).get("accepted_by", item.get("actor", "")))
        for item in meta.get("event_log", [])
        if item.get("kind") == "decision_accepted"
    }
    accepted_by.add(str(current_record.get("payload", {}).get("accepted_by", current_record.get("actor", ""))))
    return len(accepted_by.intersection({str(item) for item in required})) >= quorum


def parent_human_id(human_id: str) -> str | None:
    if SECTION_REF_RE.match(human_id):
        return None
    match = DECISION_ID_RE.match(human_id)
    if not match or not match.group(2):
        return None
    return f"D{match.group(1)}"


def enforcement_for_tier(tier: str) -> str:
    return {
        "note": "none",
        "implementation_plan": "none",
        "architecture_contract": "advisory",
        "production_invariant": "required",
        "compliance_security": "required",
    }.get(tier, "none")


def _max_drift_risk(values: Iterable[Any]) -> str | None:
    order = {"low": 1, "medium": 2, "high": 3}
    best: str | None = None
    for value in values:
        text = str(value) if value is not None else ""
        if text in order and (best is None or order[text] > order[best]):
            best = text
    return best


def _packet_type(body: str) -> str | None:
    if "```json" not in body:
        return None
    if "triage" in body.lower():
        return "triage"
    if "plan-ready" in body.lower():
        return "plan-ready"
    return "json"


def _infer_ref_type(ref_value: str) -> str:
    if ref_value.startswith("REQ-"):
        return "req"
    if ref_value.startswith("RES-"):
        return "res"
    if ref_value.startswith("FBK-"):
        return "feedback"
    if ref_value.startswith("BKL-"):
        return "backlog"
    if re.fullmatch(r"[0-9a-f]{7,40}", ref_value):
        return "commit"
    return "unknown"


def _config_from_agent_dir(agent_dir: str | Path | None) -> AgentMeshConfig:
    if agent_dir is None:
        return load_config()
    return config_from_agent_dir(agent_dir)
