"""Small local workbench for agent-mesh projects."""
from __future__ import annotations

import base64
import hashlib
import ipaddress
import json
import os
import re
import secrets
import shlex
import sys
import tempfile
from dataclasses import dataclass
from datetime import datetime, timezone
from html import escape
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, quote, urlparse

from agent_mesh.config import AgentMeshConfig, load_config, write_agent_dir_gitignore
from agent_mesh.core.events import Event, append_event, generate_event_id
from agent_mesh.core.lock import acquire
from agent_mesh.core.recovery import recover
from agent_mesh.message_packet import build_message_packet
from agent_mesh.project_registry import (
    ProjectRegistryError,
    list_registered_projects,
    project_id,
    register_project,
    registry_dir,
    resolve_registered_project,
    validate_registered_project_path,
)
from agent_mesh.store.rebuild import read_event_records, rebuild_all
from agent_mesh.store.sqlite import connect, initialize_schema, json_loads, resolve_decision, resolve_message


class WorkbenchError(RuntimeError):
    """Raised for local workbench request errors."""


MAX_ATTACHMENT_FILES = 20
MAX_ATTACHMENT_BYTES = 40 * 1024 * 1024
MAX_ATTACHMENT_TOTAL_BYTES = 40 * 1024 * 1024
MAX_REQUEST_BYTES = 64 * 1024 * 1024
DONE_STATUSES = {"done", "closed", "complete", "completed", "resolved", "accepted"}
IN_PROGRESS_MARKERS = ("progress", "doing", "active", "started", "implementation")
PENDING_MARKERS = ("review", "verify", "pending", "blocked", "needs")
BACKLOG_SEARCH_FIELDS = (
    "id",
    "title",
    "item_type",
    "summary",
    "status",
    "priority",
    "lane",
    "launch_scope",
    "release_phase",
    "wave",
    "owner_hint",
    "updated_utc",
)
FEEDBACK_REQUEST_PREDICATE_SQL = (
    "(feature_id='feedback' OR title LIKE 'Verify feedback:%' "
    "OR title='Feedback title' OR body_preview LIKE '%Feedback source:%' "
    "OR body_preview LIKE '%# Feedback%')"
)
FEEDBACK_REQUEST_PREAMBLE = (
    "Please review this feedback and update the agent-mesh backlog, "
    "decisions, or request thread as appropriate.\n\n"
)
FEEDBACK_SUBMISSION_ID_RE = re.compile(r"^fb-[A-Za-z0-9-]{8,96}$")


@dataclass(frozen=True)
class WorkbenchContext:
    server_url: str
    start_command: str
    bookmark_path: Path
    default_repo_id: str = ""
    access_token: str = ""
    managed_service: bool = False


def serve_workbench(
    *,
    repo: Path,
    host: str = "127.0.0.1",
    port: int = 8765,
    open_browser: bool = False,
) -> None:
    """Run a local HTTP workbench for registered agent-mesh repos."""
    _validate_workbench_host(host)
    config = load_config(repo)
    registered = register_project(config.project_root)
    url = f"http://{host}:{port}"
    managed_service = os.environ.get("AGENT_MESH_WORKBENCH_SERVICE") == "1"
    context = WorkbenchContext(
        server_url=url,
        start_command=workbench_start_command(config, host=host, port=port),
        bookmark_path=(
            managed_workbench_bookmark_path()
            if managed_service
            else workbench_bookmark_path(config)
        ),
        default_repo_id=registered.id,
        access_token=secrets.token_urlsafe(32),
        managed_service=managed_service,
    )
    handler = _handler_for(config.project_root, context)
    server = ThreadingHTTPServer((host, port), handler)
    try:
        write_agent_dir_gitignore(config)
        write_bookmark_file(config, context)
    except Exception:
        server.server_close()
        raise
    launch_url = workbench_launch_url(context)
    print(f"agent-mesh workbench: {workbench_console_url(context)}")
    print(f"bookmark: file://{context.bookmark_path}")
    print(f"repo: {config.project_root}")
    if open_browser:
        import webbrowser

        webbrowser.open(launch_url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


def workbench_status(repo: Path) -> dict[str, Any]:
    config = _load_and_rebuild(repo)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        last_seq = conn.execute("SELECT last_event_seq FROM events_seen").fetchone()[0]
        counts = {
            "open_requests": conn.execute(
                "SELECT COUNT(*) FROM messages WHERE kind='request' AND status='open'"
            ).fetchone()[0],
            "closed_requests": conn.execute(
                "SELECT COUNT(*) FROM messages WHERE kind='request' AND status='closed'"
            ).fetchone()[0],
            "responses": conn.execute(
                "SELECT COUNT(*) FROM messages WHERE kind='response'"
            ).fetchone()[0],
            "backlog_items": conn.execute("SELECT COUNT(*) FROM backlog_items").fetchone()[0],
            "decisions": conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0],
        }
        counts.update(_feedback_request_counts(conn))
        snapshot = _snapshot_metrics(conn)
    finally:
        conn.close()
    return {
        "ok": True,
        "project": {
            "root": str(config.project_root),
            "name": config.project_name or config.project_root.name,
            "participants": config.participants,
            "default_sender": config.default_sender,
            "default_recipient": config.default_recipient,
        },
        "agent_mesh": {
            "events_file": _rel(config, config.events_path),
            "events_exists": config.events_path.exists(),
            "db_file": _rel(config, config.db_path),
            "db_exists": config.db_path.exists(),
            "last_event_seq": last_seq,
        },
        "counts": counts,
        "snapshot": snapshot,
    }


def lookup_message(repo: Path, message_id: str) -> dict[str, Any] | None:
    config = _load_and_rebuild(repo)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        row = resolve_message(conn, message_id)
        if row is None:
            return None
        packet = build_message_packet(
            conn,
            row,
            max_body_chars=50_000,
            max_thread_body_chars=8_000,
            max_thread_messages=20,
        )
    finally:
        conn.close()
    return {
        "ok": True,
        "id": message_id,
        "source": "agent-mesh",
        "file": _rel(config, config.db_path),
        "block": message_packet_block(packet),
        "packet": packet,
    }


def list_messages(
    repo: Path,
    *,
    status: str = "",
    kind: str = "",
    feature: str = "",
    query: str = "",
) -> list[dict[str, Any]]:
    config = _load_and_rebuild(repo)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        sql = "SELECT * FROM messages WHERE 1=1"
        params: list[str] = []
        if status:
            sql += " AND status=?"
            params.append(status)
        if kind:
            sql += " AND kind=?"
            params.append(kind)
        if feature:
            if feature == "feedback":
                sql += f" AND {FEEDBACK_REQUEST_PREDICATE_SQL}"
            else:
                sql += " AND feature_id=?"
                params.append(feature)
        if query:
            like = f"%{query}%"
            sql += (
                " AND (id LIKE ? OR thread_id LIKE ? OR request_id LIKE ? "
                "OR sender LIKE ? OR feature_id LIKE ? OR title LIKE ? OR summary LIKE ? "
                "OR body_preview LIKE ?)"
            )
            params.extend([like, like, like, like, like, like, like, like])
        sql += " ORDER BY created_utc DESC, event_seq DESC LIMIT 200"
        rows = conn.execute(sql, params).fetchall()
        return [_message_item_from_row(row) for row in rows]
    finally:
        conn.close()


def list_backlog_items(
    repo: Path,
    *,
    status: str = "",
    lane: str = "",
    priority: str = "",
    owner: str = "",
    item_type: str = "",
    launch_scope: str = "",
    wave: str = "",
    query: str = "",
    quick_filter: str = "",
) -> list[dict[str, Any]]:
    config = _load_and_rebuild(repo)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        rows = conn.execute(
            """
            SELECT * FROM backlog_items
            ORDER BY priority ASC, updated_utc DESC, id ASC
            """
        ).fetchall()
        items = [_backlog_item_from_row(row) for row in rows]
        filters = {
            "status": status,
            "lane": lane,
            "priority": priority,
            "owner_hint": owner,
            "item_type": item_type,
            "launch_scope": launch_scope,
            "wave": wave,
        }
        for key, expected in filters.items():
            if expected:
                items = [item for item in items if _contains(item.get(key), expected)]
        if query:
            items = [item for item in items if _backlog_item_matches_query(item, query)]
        if quick_filter:
            items = [
                item
                for item in items
                if _backlog_item_matches_quick_filter(item, quick_filter)
            ]
        return items
    finally:
        conn.close()


def backlog_kanban(repo: Path) -> dict[str, Any]:
    items = list_backlog_items(repo)
    lanes: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        lanes.setdefault(str(item["lane"] or "unassigned"), []).append(item)
    return {"ok": True, "lanes": lanes, "items": items}


def lookup_backlog_item(repo: Path, item_id: str) -> dict[str, Any] | None:
    config = _load_and_rebuild(repo)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        row = conn.execute("SELECT * FROM backlog_items WHERE id=?", (item_id.strip(),)).fetchone()
        if row is None:
            return None
        item = _backlog_detail_from_row(conn, row)
        block = backlog_detail_block(item)
        return {"ok": True, "item": item, "block": block}
    finally:
        conn.close()


def list_decisions(
    repo: Path,
    *,
    query: str = "",
    status: str = "",
    tier: str = "",
) -> list[dict[str, Any]]:
    config = _load_and_rebuild(repo)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        sql = "SELECT * FROM decisions WHERE 1=1"
        params: list[str] = []
        if status:
            sql += " AND status=?"
            params.append(status)
        if tier:
            sql += " AND tier=?"
            params.append(tier)
        if query:
            like = f"%{query}%"
            sql += " AND (human_id LIKE ? OR dec_ulid LIKE ? OR title LIKE ? OR owner LIKE ?)"
            params.extend([like, like, like, like])
        sql += " ORDER BY status ASC, tier ASC, human_id ASC LIMIT 200"
        rows = conn.execute(sql, params).fetchall()
        return [_decision_item_from_row(row) for row in rows]
    finally:
        conn.close()


def lookup_decision(repo: Path, identifier: str) -> dict[str, Any] | None:
    config = _load_and_rebuild(repo)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        dec_ulid = resolve_decision(conn, identifier)
        if dec_ulid is None:
            return None
        row = conn.execute("SELECT * FROM decisions WHERE dec_ulid=?", (dec_ulid,)).fetchone()
        if row is None:
            return None
        meta = json_loads(row["meta_json"], {})
        if not isinstance(meta, dict):
            meta = {}
        body = _decision_body_from_row(config, row)
        block = decision_detail_block(row, meta, body)
        proposed_utc = _decision_proposed_utc(row, meta)
        return {
            "ok": True,
            "decision": {
                "id": row["human_id"],
                "dec_ulid": row["dec_ulid"],
                "title": row["title"],
                "tier": row["tier"],
                "status": row["status"],
                "owner": row["owner"] or "",
                "drift_risk": row["drift_risk"] or "",
                "display_utc": _decision_display_utc(row, meta),
                "proposed_utc": proposed_utc,
                "accepted_utc": row["accepted_utc"],
                "in_force_utc": row["in_force_utc"],
                "retired_utc": row["retired_utc"],
                "last_verified_utc": row["last_verified_utc"],
                "superseded_by": row["superseded_by"] or "",
                "status_utc": _decision_status_utc(row, meta if isinstance(meta, dict) else {}),
                "meta": meta if isinstance(meta, dict) else {},
                "body_path": row["body_path"] or "",
                "body_bytes": row["body_bytes"],
                "body": body,
                "block": block,
            },
            "block": block,
        }
    finally:
        conn.close()


def update_backlog_item(
    repo: Path,
    item_id: str,
    *,
    status: str | None = None,
    lane: str | None = None,
    priority: str | None = None,
    actor: str | None = None,
) -> dict[str, Any]:
    config = load_config(repo)
    item_id = item_id.strip()
    if not item_id:
        raise WorkbenchError("item_id is required")
    updates = {
        key: value.strip()
        for key, value in {
            "status": status,
            "lane": lane,
            "priority": priority,
        }.items()
        if value is not None
    }
    if not updates:
        raise WorkbenchError("status, lane, or priority is required")
    actor = (actor or config.default_sender).strip()
    if actor not in config.participants:
        raise WorkbenchError(f"actor {actor!r} is not in participants")

    lock_handle = acquire(config.agent_dir / ".mail-lock")
    last_event_seq = None
    try:
        rebuild_all(config)
        conn = connect(config.db_path)
        try:
            initialize_schema(conn)
            row = conn.execute("SELECT * FROM backlog_items WHERE id=?", (item_id,)).fetchone()
            if row is None:
                raise WorkbenchError(f"backlog item not found: {item_id}")
            payload = _backlog_payload_from_row(row)
        finally:
            conn.close()
        payload.update(updates)
        event = Event(
            event_id=generate_event_id(),
            actor=actor,
            kind="backlog_item_upserted",
            entity_id=item_id,
            thread_id=item_id,
            payload=payload,
        )
        result = append_event(config.events_path, event, lock_acquired=True)
        last_event_seq = result.event.event_seq
    finally:
        lock_handle.release(last_event_seq=last_event_seq)

    items = [item for item in list_backlog_items(config.project_root) if item["id"] == item_id]
    return {
        "ok": True,
        "item": items[0] if items else {"id": item_id, **updates},
        "event_seq": last_event_seq,
    }


def save_attachment_uploads(repo: Path, files: list[dict[str, Any]]) -> dict[str, Any]:
    config = load_config(repo)
    if not files:
        raise WorkbenchError("No files provided")
    if len(files) > MAX_ATTACHMENT_FILES:
        raise WorkbenchError(f"Too many files; maximum is {MAX_ATTACHMENT_FILES}")

    attachment_dir = validate_registered_project_path(
        config.project_root,
        config.agent_dir / "attachments" / "screenshots",
        label="attachments",
    )
    attachment_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    saved_paths: list[str] = []
    prepared: list[tuple[str, bytes]] = []
    total_bytes = 0

    for idx, item in enumerate(files, start=1):
        name = _sanitize_filename(str(item.get("name", "")))
        data_url = str(item.get("data_url", "")).strip()
        if not data_url.startswith("data:") or ";base64," not in data_url:
            raise WorkbenchError(f"Invalid data payload for {name}")
        encoded = data_url.split(";base64,", 1)[1]
        try:
            data = base64.b64decode(encoded, validate=True)
        except Exception as exc:  # pragma: no cover - defensive decode detail
            raise WorkbenchError(f"Unable to decode {name}: {exc}") from exc
        if len(data) > MAX_ATTACHMENT_BYTES:
            raise WorkbenchError(f"{name} exceeds {MAX_ATTACHMENT_BYTES // (1024 * 1024)} MB")

        stem = Path(name).stem or "upload"
        suffix = Path(name).suffix or ".bin"
        filename = f"{timestamp}-{idx:02d}-{secrets.token_hex(4)}-{stem}{suffix}"
        total_bytes += len(data)
        if total_bytes > MAX_ATTACHMENT_TOTAL_BYTES:
            raise WorkbenchError(
                "Attachment upload exceeds "
                f"{MAX_ATTACHMENT_TOTAL_BYTES // (1024 * 1024)} MB total"
            )
        prepared.append((filename, data))

    created_paths: list[Path] = []
    try:
        for filename, data in prepared:
            destination = validate_registered_project_path(
                config.project_root,
                attachment_dir / filename,
                label="attachment destination",
            )
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            try:
                fd = os.open(destination, flags, 0o600)
            except FileExistsError as exc:
                raise WorkbenchError(f"Attachment destination already exists: {filename}") from exc
            try:
                with os.fdopen(fd, "wb") as handle:
                    handle.write(data)
                    handle.flush()
                    os.fsync(handle.fileno())
            except Exception:
                destination.unlink(missing_ok=True)
                raise
            created_paths.append(destination)
            saved_paths.append(_rel(config, destination))
    except Exception:
        for path in created_paths:
            path.unlink(missing_ok=True)
        raise

    return {
        "ok": True,
        "saved": saved_paths,
        "directory": _rel(config, attachment_dir),
    }


def build_feedback_markdown(payload: dict[str, Any]) -> dict[str, Any]:
    title = _clean(payload.get("title")) or "Untitled feedback"
    status = _clean(payload.get("status")) or "needs-review"
    severity = _clean(payload.get("severity")) or "normal"
    related_id = _clean(payload.get("related_id"))
    target = _clean(payload.get("target"))
    notes = _clean(payload.get("notes"))
    refs = _string_list(payload.get("refs"))
    screenshots = _string_list(payload.get("screenshots"))
    created_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    lines = [
        "# Feedback",
        "",
        f"- title: {title}",
        f"- status: {status}",
        f"- severity: {severity}",
        f"- created_utc: {created_utc}",
    ]
    if related_id:
        lines.append(f"- related_id: {related_id}")
    if target:
        lines.append(f"- target: {target}")
    lines.extend(["", "## Notes", notes or "-"])
    if refs:
        lines.extend(["", "## Refs"])
        lines.extend(f"- {ref}" for ref in refs)
    if screenshots:
        lines.extend(["", "## Screenshots"])
        lines.extend(f"- {screenshot}" for screenshot in screenshots)

    markdown = "\n".join(lines).rstrip() + "\n"
    request_title = f"Verify feedback: {title}"
    request_body = f"{FEEDBACK_REQUEST_PREAMBLE}{markdown}"
    return {
        "ok": True,
        "markdown": markdown,
        "request": {
            "title": request_title,
            "body": request_body,
        },
    }


def submit_feedback_request(repo: Path, payload: dict[str, Any]) -> dict[str, Any]:
    config = load_config(repo)
    _validate_feedback_for_submit(payload)
    draft = build_feedback_markdown(payload)
    sender = _clean(payload.get("sender")) or config.default_sender
    if sender not in config.participants:
        raise WorkbenchError(f"sender {sender!r} is not in participants")
    raw_to = _clean(payload.get("to")) or config.default_recipient
    recipients = config.canonical_recipients(raw_to)
    unknown_recipients = [item for item in recipients if item not in config.participants]
    if unknown_recipients:
        raise WorkbenchError(
            "feedback recipient(s) are not participants: " + ", ".join(unknown_recipients)
        )
    submission_id = _feedback_submission_id(payload)
    submission_digest = _feedback_submission_digest(
        payload,
        sender=sender,
        raw_to=raw_to,
    )

    lock_handle = acquire(config.agent_dir / ".mail-lock")
    last_event_seq = None
    try:
        recover(config.events_path, config.agent_dir)
        existing = _find_feedback_submission(config.events_path, submission_id)
        if existing is not None:
            existing_digest = str(
                existing.get("payload", {}).get("feedback_submission_digest", "")
            )
            if existing_digest != submission_digest:
                raise WorkbenchError(
                    "feedback submission_id was already used for different form content"
                )
            rebuild_all(config)
            return _feedback_response_from_record(existing, reused=True)

        request_id = _new_public_id("REQ", sender)
        event_payload: dict[str, Any] = {
            "from": sender,
            "to": recipients,
            "title": draft["request"]["title"],
            "body": draft["request"]["body"],
            "feature": "feedback",
            "refs": _feedback_refs(payload),
            "response_mode": "single",
            "feedback_submission_id": submission_id,
            "feedback_submission_digest": submission_digest,
        }
        if config.routing.preserve_raw_to:
            event_payload["original_to"] = raw_to
        event = Event(
            event_id=generate_event_id(),
            actor=sender,
            kind="req_created",
            entity_id=request_id,
            thread_id=request_id,
            payload=event_payload,
        )
        result = append_event(config.events_path, event, lock_acquired=True)
        last_event_seq = result.event.event_seq
        rebuild_all(config)
        return _feedback_response_from_record(result.event.to_dict(), reused=False)
    finally:
        lock_handle.release(last_event_seq=last_event_seq)


def feedback_submission_receipt(repo: Path, submission_id: str) -> dict[str, Any]:
    config = load_config(repo)
    normalized = _validate_feedback_submission_id(submission_id)
    lock_handle = acquire(config.agent_dir / ".mail-lock")
    try:
        recover(config.events_path, config.agent_dir)
        record = _find_feedback_submission(config.events_path, normalized)
        if record is None:
            return {
                "ok": True,
                "found": False,
                "submission_id": normalized,
            }
        rebuild_all(config)
        return _feedback_response_from_record(record, reused=True)
    finally:
        lock_handle.release()


def _feedback_submission_id(payload: dict[str, Any]) -> str:
    requested = _clean(payload.get("submission_id"))
    if not requested:
        requested = f"fb-{secrets.token_hex(16)}"
    return _validate_feedback_submission_id(requested)


def _validate_feedback_submission_id(submission_id: str) -> str:
    normalized = submission_id.strip()
    if not FEEDBACK_SUBMISSION_ID_RE.fullmatch(normalized):
        raise WorkbenchError(
            "feedback submission_id must start with fb- and contain 8-96 letters, numbers, or hyphens"
        )
    return normalized


def _feedback_submission_digest(
    payload: dict[str, Any],
    *,
    sender: str,
    raw_to: str,
) -> str:
    normalized = {
        "title": _clean(payload.get("title")),
        "related_id": _clean(payload.get("related_id")),
        "severity": _clean(payload.get("severity")) or "normal",
        "target": _clean(payload.get("target")),
        "notes": _clean(payload.get("notes")),
        "refs": _string_list(payload.get("refs")),
        "screenshots": _string_list(payload.get("screenshots")),
        "sender": sender,
        "to": raw_to,
    }
    encoded = json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _find_feedback_submission(events_path: Path, submission_id: str) -> dict[str, Any] | None:
    for record in reversed(list(read_event_records(events_path))):
        event_payload = record.get("payload", {})
        if (
            record.get("kind") == "req_created"
            and event_payload.get("feature") == "feedback"
            and event_payload.get("feedback_submission_id") == submission_id
        ):
            return record
    return None


def _feedback_response_from_record(
    record: dict[str, Any],
    *,
    reused: bool,
) -> dict[str, Any]:
    event_payload = record.get("payload", {})
    body = str(event_payload.get("body", ""))
    markdown = (
        body[len(FEEDBACK_REQUEST_PREAMBLE):]
        if body.startswith(FEEDBACK_REQUEST_PREAMBLE)
        else body
    )
    return {
        "ok": True,
        "found": True,
        "reused": reused,
        "submission_id": str(event_payload.get("feedback_submission_id", "")),
        "request_id": str(record.get("entity_id", "")),
        "event_seq": int(record.get("event_seq", 0)),
        "to": list(event_payload.get("to", [])),
        "markdown": markdown,
        "request": {
            "title": str(event_payload.get("title", "")),
            "body": body,
        },
    }


def update_request_status(
    repo: Path,
    request_id: str,
    *,
    to_status: str,
    reason: str,
    actor: str | None = None,
) -> dict[str, Any]:
    config = load_config(repo)
    request_id = request_id.strip()
    to_status = to_status.strip()
    reason = reason.strip()
    actor = (actor or config.default_sender).strip()
    if not request_id:
        raise WorkbenchError("request_id is required")
    if to_status not in {"open", "closed"}:
        raise WorkbenchError("to_status must be open or closed")
    if not reason:
        raise WorkbenchError("Reason is required to change request status")
    if actor not in config.participants:
        raise WorkbenchError(f"actor {actor!r} is not in participants")

    lock_handle = acquire(config.agent_dir / ".mail-lock")
    last_event_seq = None
    try:
        rebuild_all(config)
        conn = connect(config.db_path)
        try:
            initialize_schema(conn)
            row = resolve_message(conn, request_id)
            if row is None or row["kind"] != "request":
                raise WorkbenchError(f"request not found: {request_id}")
            from_status = row["status"]
        finally:
            conn.close()
        event = Event(
            event_id=generate_event_id(),
            actor=actor,
            kind="req_status_changed",
            entity_id=request_id,
            thread_id=request_id,
            payload={
                "from_status": from_status,
                "to_status": to_status,
                "reason": reason,
                "actor": actor,
            },
        )
        result = append_event(config.events_path, event, lock_acquired=True)
        last_event_seq = result.event.event_seq
    finally:
        lock_handle.release(last_event_seq=last_event_seq)

    message = lookup_message(config.project_root, request_id)
    return {
        "ok": True,
        "request_id": request_id,
        "status": to_status,
        "event_seq": last_event_seq,
        "message": message,
    }


def _validate_feedback_for_submit(payload: dict[str, Any]) -> None:
    missing: list[str] = []
    if not _clean(payload.get("title")):
        missing.append("Title")
    if not _clean(payload.get("notes")):
        missing.append("Notes")
    if missing:
        if len(missing) == 1:
            raise WorkbenchError(f"{missing[0]} is required to submit feedback.")
        raise WorkbenchError(f"{' and '.join(missing)} are required to submit feedback.")


def message_packet_block(packet: dict[str, Any]) -> str:
    message = packet.get("message", {})
    label = message.get("title") if message.get("kind") == "request" else message.get("summary")
    lines = [
        f"## {message.get('id', '')}",
        f"- kind: {message.get('kind', '')}",
        f"- thread_id: {message.get('thread_id', '')}",
        f"- from: {message.get('sender', '')}",
    ]
    recipients = message.get("recipients") or []
    if recipients:
        lines.append(f"- to: {', '.join(str(item) for item in recipients)}")
    if message.get("request_id"):
        lines.append(f"- request_id: {message.get('request_id')}")
    if label:
        lines.append(f"- title: {label}" if message.get("kind") == "request" else f"- summary: {label}")
    if message.get("status"):
        lines.append(f"- status: {message.get('status')}")
    lines.extend(["", "### Message", str(message.get("body") or "")])
    return "\n".join(lines).rstrip() + "\n"


def decision_detail_block(row, meta: dict[str, Any], body: str) -> str:
    status_utc = _decision_status_utc(row, meta)
    proposed_utc = _decision_proposed_utc(row, meta)
    lines = [
        f"## {row['human_id']}",
        f"- dec_ulid: {row['dec_ulid']}",
        f"- status: {row['status']}",
        f"- status_utc: {status_utc}",
        f"- tier: {row['tier']}",
        f"- owner: {row['owner'] or ''}",
        f"- drift_risk: {row['drift_risk'] or ''}",
        f"- proposed_utc: {proposed_utc}",
    ]
    if row["accepted_utc"]:
        lines.append(f"- accepted_utc: {row['accepted_utc']}")
    if row["in_force_utc"]:
        lines.append(f"- in_force_utc: {row['in_force_utc']}")
    if row["retired_utc"]:
        lines.append(f"- retired_utc: {row['retired_utc']}")
    if row["superseded_by"]:
        lines.append(f"- superseded_by: {row['superseded_by']}")
    lines.extend(["", str(row["title"] or "")])
    if body.strip():
        lines.extend(["", "## Body", body.rstrip()])
    else:
        _append_meta_section(lines, "Context", meta.get("context"))
        _append_meta_section(lines, "Decision", meta.get("decision"))
        _append_meta_section(lines, "Consequences", meta.get("consequences"))
        _append_meta_section(lines, "Rejected Alternatives", meta.get("rejected_alternatives"))
        _append_meta_section(lines, "Generated Artifact Paths", meta.get("generated_artifact_paths"))
    return "\n".join(lines).rstrip() + "\n"


def _decision_body_from_row(config: AgentMeshConfig, row) -> str:
    body_path = row["body_path"]
    if not body_path:
        return ""
    path = config.agent_dir / body_path
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return ""


def _append_meta_section(lines: list[str], title: str, value: Any) -> None:
    text = _meta_section_text(value)
    if text:
        lines.extend(["", f"## {title}", text])


def _meta_section_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        rendered_items = []
        for item in value:
            text = _meta_item_text(item)
            if text:
                rendered_items.append(f"- {_indent_subsequent_lines(text)}")
        return "\n".join(rendered_items)
    if isinstance(value, dict):
        if not value:
            return ""
        return json.dumps(value, indent=2, sort_keys=True)
    return str(value).strip()


def _meta_item_text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, indent=2, sort_keys=True)
    return str(value).strip()


def _indent_subsequent_lines(text: str) -> str:
    return "\n  ".join(text.splitlines())


def workbench_bookmark_path(config: AgentMeshConfig) -> Path:
    return config.agent_dir / "workbench.html"


def managed_workbench_bookmark_path(config_home: Path | None = None) -> Path:
    """Return the stable machine-local bookmark for the per-user service."""

    root = (config_home or registry_dir()).expanduser().resolve()
    return root / "workbench.html"


def workbench_start_command(config: AgentMeshConfig, *, host: str, port: int) -> str:
    return (
        f"agent-mesh workbench --repo {shlex.quote(str(config.project_root))} "
        f"--host {shlex.quote(host)} --port {port}"
    )


def workbench_launch_url(context: WorkbenchContext) -> str:
    """Return an HTTP launch URL without sending the token to the server."""
    return f"{context.server_url}/#token={quote(context.access_token, safe='')}"


def workbench_console_url(context: WorkbenchContext) -> str:
    """Return a log-safe URL while keeping manual launch output convenient."""

    if context.managed_service:
        return context.server_url
    return workbench_launch_url(context)


def _validate_workbench_host(host: str) -> None:
    normalized = host.strip().casefold()
    if normalized == "localhost":
        return
    try:
        address = ipaddress.ip_address(normalized)
    except ValueError as exc:
        raise WorkbenchError(
            "Workbench is loopback-only; use --host 127.0.0.1 or localhost"
        ) from exc
    if not address.is_loopback:
        raise WorkbenchError(
            "Workbench is loopback-only; use --host 127.0.0.1 or localhost"
        )


def write_bookmark_file(config: AgentMeshConfig, context: WorkbenchContext) -> Path:
    payload = render_workbench_html(
        api_base=context.server_url,
        start_command=context.start_command,
        bookmark_path=context.bookmark_path,
        default_repo_id=context.default_repo_id or project_id(config.project_root),
        access_token=context.access_token,
        managed_service=context.managed_service,
    )
    context.bookmark_path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(
        prefix="workbench-",
        suffix=".html",
        dir=context.bookmark_path.parent,
    )
    temporary_path = Path(temporary_name)
    try:
        if os.name != "nt" and hasattr(os, "fchmod"):
            os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, context.bookmark_path)
        if os.name != "nt":
            context.bookmark_path.chmod(0o600)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()
    return context.bookmark_path


def render_server_workbench_html(context: WorkbenchContext) -> str:
    """Render the HTTP page without embedding the bearer token in its body."""
    return render_workbench_html(
        api_base="",
        start_command=context.start_command,
        bookmark_path=context.bookmark_path,
        default_repo_id=context.default_repo_id,
        access_token="",
        managed_service=context.managed_service,
    )


def _request_host_allowed(value: str, context: WorkbenchContext) -> bool:
    return value.strip().casefold() == urlparse(context.server_url).netloc.casefold()


def _request_origin_allowed(value: str, context: WorkbenchContext) -> bool:
    origin = value.strip()
    return not origin or origin in {"null", context.server_url}


def render_workbench_html(
    *,
    api_base: str = "",
    start_command: str = "",
    bookmark_path: Path | None = None,
    default_repo_id: str = "",
    access_token: str = "",
    managed_service: bool = False,
) -> str:
    bookmark_url = bookmark_path.resolve().as_uri() if bookmark_path else ""
    return (
        WORKBENCH_HTML.replace("__AGENT_MESH_API_BASE__", json.dumps(api_base))
        .replace("__AGENT_MESH_START_COMMAND__", escape(start_command))
        .replace("__AGENT_MESH_BOOKMARK_URL__", escape(bookmark_url))
        .replace("__AGENT_MESH_BOOKMARK_PATH__", escape(str(bookmark_path or "")))
        .replace("__AGENT_MESH_DEFAULT_REPO_ID__", json.dumps(default_repo_id))
        .replace("__AGENT_MESH_ACCESS_TOKEN__", json.dumps(access_token))
        .replace("__AGENT_MESH_MANAGED_SERVICE__", json.dumps(managed_service))
        .replace("__AGENT_MESH_MAX_ATTACHMENT_BYTES__", str(MAX_ATTACHMENT_BYTES))
        .replace(
            "__AGENT_MESH_MAX_ATTACHMENT_TOTAL_BYTES__",
            str(MAX_ATTACHMENT_TOTAL_BYTES),
        )
    )


def _load_and_rebuild(repo: Path) -> AgentMeshConfig:
    config = load_config(repo)
    lock_handle = acquire(config.agent_dir / ".mail-lock")
    try:
        rebuild_all(config)
    finally:
        lock_handle.release()
    return config


def _handler_for(repo: Path, context: WorkbenchContext) -> type[BaseHTTPRequestHandler]:
    default_repo = repo.resolve()

    class Handler(BaseHTTPRequestHandler):
        def _allowed_origin(self) -> str:
            origin = self.headers.get("Origin", "").strip()
            if origin and _request_origin_allowed(origin, context):
                return origin
            return ""

        def _request_headers_allowed(self) -> bool:
            if not _request_host_allowed(self.headers.get("Host", ""), context):
                self._json(
                    HTTPStatus.FORBIDDEN,
                    {"ok": False, "error": "Invalid Workbench Host header"},
                )
                return False
            if not _request_origin_allowed(self.headers.get("Origin", ""), context):
                self._json(
                    HTTPStatus.FORBIDDEN,
                    {"ok": False, "error": "Workbench origin is not allowed"},
                )
                return False
            return True

        def _send(self, status: HTTPStatus, body: bytes, content_type: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            allowed_origin = self._allowed_origin()
            if allowed_origin:
                self.send_header("Access-Control-Allow-Origin", allowed_origin)
                self.send_header("Vary", "Origin")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header(
                "Access-Control-Allow-Headers",
                "Content-Type, X-Agent-Mesh-Token",
            )
            self.end_headers()
            self.wfile.write(body)

        def _json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
            self._send(
                status,
                json.dumps(payload, indent=2, sort_keys=True).encode("utf-8"),
                "application/json; charset=utf-8",
            )

        def do_OPTIONS(self) -> None:
            if not self._request_headers_allowed():
                return
            self.send_response(HTTPStatus.NO_CONTENT)
            allowed_origin = self._allowed_origin()
            if allowed_origin:
                self.send_header("Access-Control-Allow-Origin", allowed_origin)
                self.send_header("Vary", "Origin")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header(
                "Access-Control-Allow-Headers",
                "Content-Type, X-Agent-Mesh-Token",
            )
            self.end_headers()

        def _authorized(self) -> bool:
            if not context.access_token:
                return True
            provided = self.headers.get("X-Agent-Mesh-Token", "")
            if secrets.compare_digest(provided, context.access_token):
                return True
            self._json(
                HTTPStatus.FORBIDDEN,
                {"ok": False, "error": "Missing or invalid Workbench access token"},
            )
            return False

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            try:
                if not self._request_headers_allowed():
                    return
                if parsed.path == "/":
                    self._send(
                        HTTPStatus.OK,
                        render_server_workbench_html(context).encode("utf-8"),
                        "text/html; charset=utf-8",
                    )
                    return
                if parsed.path.startswith("/api/") and not self._authorized():
                    return
                if parsed.path == "/api/health":
                    self._json(
                        HTTPStatus.OK,
                        {
                            "ok": True,
                            "server": "online",
                            "managed_service": context.managed_service,
                        },
                    )
                    return
                if parsed.path == "/api/projects":
                    projects = list_registered_projects()
                    self._json(
                        HTTPStatus.OK,
                        {
                            "ok": True,
                            "default_repo_id": context.default_repo_id,
                            "projects": [project.as_dict() for project in projects],
                        },
                    )
                    return
                selected_repo = _registered_repo_from_request(parsed, default_repo)
                if parsed.path == "/api/status":
                    self._json(HTTPStatus.OK, workbench_status(selected_repo))
                    return
                if parsed.path == "/api/feedback/receipt":
                    params = parse_qs(parsed.query)
                    submission_id = params.get("id", [""])[0].strip()
                    if not submission_id:
                        self._json(
                            HTTPStatus.BAD_REQUEST,
                            {"ok": False, "error": "Missing submission id"},
                        )
                        return
                    self._json(
                        HTTPStatus.OK,
                        feedback_submission_receipt(selected_repo, submission_id),
                    )
                    return
                if parsed.path == "/api/message":
                    params = parse_qs(parsed.query)
                    message_id = params.get("id", [""])[0].strip()
                    if not message_id:
                        self._json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "Missing id"})
                        return
                    result = lookup_message(selected_repo, message_id)
                    if result is None:
                        self._json(
                            HTTPStatus.NOT_FOUND,
                            {"ok": False, "error": f"No message found for {message_id}"},
                        )
                        return
                    self._json(HTTPStatus.OK, result)
                    return
                if parsed.path == "/api/messages":
                    params = parse_qs(parsed.query)
                    self._json(
                        HTTPStatus.OK,
                        {
                            "ok": True,
                            "messages": list_messages(
                                selected_repo,
                                status=params.get("status", [""])[0].strip(),
                                kind=params.get("kind", [""])[0].strip(),
                                feature=params.get("feature", [""])[0].strip(),
                                query=params.get("q", [""])[0].strip(),
                            ),
                        },
                    )
                    return
                if parsed.path == "/api/backlog/items":
                    params = parse_qs(parsed.query)
                    self._json(
                        HTTPStatus.OK,
                        {
                            "ok": True,
                            "items": list_backlog_items(
                                selected_repo,
                                status=params.get("status", [""])[0].strip(),
                                lane=params.get("lane", [""])[0].strip(),
                                priority=params.get("priority", [""])[0].strip(),
                                owner=params.get("owner", [""])[0].strip(),
                                item_type=params.get("type", [""])[0].strip(),
                                launch_scope=params.get("scope", [""])[0].strip(),
                                wave=params.get("wave", [""])[0].strip(),
                                query=params.get("q", [""])[0].strip(),
                                quick_filter=params.get("filter", [""])[0].strip(),
                            ),
                        },
                    )
                    return
                if parsed.path == "/api/backlog/item":
                    params = parse_qs(parsed.query)
                    item_id = params.get("id", [""])[0].strip()
                    if not item_id:
                        self._json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "Missing id"})
                        return
                    result = lookup_backlog_item(selected_repo, item_id)
                    if result is None:
                        self._json(
                            HTTPStatus.NOT_FOUND,
                            {"ok": False, "error": f"No backlog item found for {item_id}"},
                        )
                        return
                    self._json(HTTPStatus.OK, result)
                    return
                if parsed.path == "/api/backlog/kanban":
                    self._json(HTTPStatus.OK, backlog_kanban(selected_repo))
                    return
                if parsed.path == "/api/decisions":
                    params = parse_qs(parsed.query)
                    self._json(
                        HTTPStatus.OK,
                        {
                            "ok": True,
                            "decisions": list_decisions(
                                selected_repo,
                                query=params.get("q", [""])[0].strip(),
                                status=params.get("status", [""])[0].strip(),
                                tier=params.get("tier", [""])[0].strip(),
                            ),
                        },
                    )
                    return
                if parsed.path == "/api/decision":
                    params = parse_qs(parsed.query)
                    identifier = params.get("id", [""])[0].strip()
                    if not identifier:
                        self._json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "Missing id"})
                        return
                    result = lookup_decision(selected_repo, identifier)
                    if result is None:
                        self._json(
                            HTTPStatus.NOT_FOUND,
                            {"ok": False, "error": f"No decision found for {identifier}"},
                        )
                        return
                    self._json(HTTPStatus.OK, result)
                    return
                self._json(HTTPStatus.NOT_FOUND, {"ok": False, "error": f"Unknown path: {parsed.path}"})
            except ProjectRegistryError as exc:
                self._json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc)})
            except Exception as exc:  # pragma: no cover - local server safeguard
                self._json(HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": str(exc)})

        def do_POST(self) -> None:
            parsed = urlparse(self.path)
            if not self._request_headers_allowed():
                return
            if not self._authorized():
                return
            try:
                content_length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                self._json(
                    HTTPStatus.BAD_REQUEST,
                    {"ok": False, "error": "Invalid Content-Length"},
                )
                return
            if content_length < 0 or content_length > MAX_REQUEST_BYTES:
                self._json(
                    HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                    {
                        "ok": False,
                        "error": f"Request body exceeds {MAX_REQUEST_BYTES // (1024 * 1024)} MB",
                    },
                )
                return
            raw = self.rfile.read(content_length) if content_length else b"{}"
            try:
                payload = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                self._json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "Invalid JSON body"})
                return
            if not isinstance(payload, dict):
                self._json(
                    HTTPStatus.BAD_REQUEST,
                    {"ok": False, "error": "JSON body must be an object"},
                )
                return
            try:
                selected_repo = _registered_repo_from_request(parsed, default_repo)
                if parsed.path == "/api/feedback/draft":
                    self._json(HTTPStatus.OK, build_feedback_markdown(payload))
                    return
                if parsed.path == "/api/feedback/submit":
                    self._json(HTTPStatus.OK, submit_feedback_request(selected_repo, payload))
                    return
                if parsed.path == "/api/attachments/upload":
                    self._json(
                        HTTPStatus.OK,
                        save_attachment_uploads(selected_repo, payload.get("files", [])),
                    )
                    return
                if parsed.path == "/api/backlog/update":
                    result = update_backlog_item(
                        selected_repo,
                        item_id=_clean(payload.get("id")),
                        status=_optional_clean(payload.get("status")),
                        lane=_optional_clean(payload.get("lane")),
                        priority=_optional_clean(payload.get("priority")),
                        actor=_optional_clean(payload.get("actor")),
                    )
                    self._json(HTTPStatus.OK, result)
                    return
                if parsed.path == "/api/message/status":
                    result = update_request_status(
                        selected_repo,
                        request_id=_clean(payload.get("id")),
                        to_status=_clean(payload.get("status")),
                        reason=_clean(payload.get("reason")),
                        actor=_optional_clean(payload.get("actor")),
                    )
                    self._json(HTTPStatus.OK, result)
                    return
                self._json(HTTPStatus.NOT_FOUND, {"ok": False, "error": f"Unknown path: {parsed.path}"})
            except (ProjectRegistryError, ValueError, WorkbenchError) as exc:
                self._json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc)})
            except Exception as exc:  # pragma: no cover - local server safeguard
                self._json(HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": str(exc)})

        def log_message(self, format: str, *args: Any) -> None:
            print(f"{self.address_string()} - {format % args}", file=sys.stderr)

    return Handler


def _registered_repo_from_request(parsed: Any, default_repo: Path) -> Path:
    params = parse_qs(parsed.query)
    identifier = params.get("repo", [""])[0].strip()
    if not identifier:
        identifier = project_id(default_repo)
    return resolve_registered_project(identifier).root


def _rel(config: AgentMeshConfig, path: Path) -> str:
    try:
        return str(path.relative_to(config.project_root))
    except ValueError:
        return str(path)


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _optional_clean(value: Any) -> str | None:
    if value is None:
        return None
    return str(value).strip()


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        raw = value
    else:
        raw = str(value).splitlines()
    cleaned: list[str] = []
    for item in raw:
        text = str(item).strip()
        if text.startswith(("- ", "* ")):
            text = text[2:].strip()
        if text:
            cleaned.append(text)
    return cleaned


def _feedback_refs(payload: dict[str, Any]) -> list[str]:
    refs: list[str] = []
    related_id = _clean(payload.get("related_id"))
    if related_id:
        refs.append(related_id)
    refs.extend(_string_list(payload.get("refs")))
    refs.extend(_string_list(payload.get("screenshots")))
    seen: set[str] = set()
    unique: list[str] = []
    for ref in refs:
        if ref not in seen:
            seen.add(ref)
            unique.append(ref)
    return unique


def _new_public_id(prefix: str, actor: str) -> str:
    stamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    stamp = stamp.replace("-", "").replace(":", "")
    safe_actor = re.sub(r"[^A-Za-z0-9_-]+", "-", actor).upper()[:20] or "ACTOR"
    return f"{prefix}-{stamp}-{safe_actor}-{secrets.randbelow(100000):05d}"


def _sanitize_filename(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", Path(name).name).strip("-.")
    return cleaned or "upload.bin"


def _backlog_payload_from_row(row) -> dict[str, Any]:
    payload = json_loads(row["meta_json"], {})
    if not isinstance(payload, dict):
        payload = {}
    payload.update({
        "id": row["id"],
        "title": row["title"],
        "item_type": row["item_type"],
        "summary": row["summary"],
        "root_cause_summary": row["root_cause_summary"],
        "architectural_category": row["architectural_category"],
        "status": row["status"],
        "priority": row["priority"],
        "launch_scope": row["launch_scope"],
        "release_phase": row["release_phase"],
        "production_state": row["production_state"],
        "disposition": row["disposition"],
        "owner_hint": row["owner_hint"],
        "lane": row["lane"],
        "notes": row["notes"],
        "refs": json_loads(row["refs_json"], []),
    })
    return {key: value for key, value in payload.items() if value is not None}


def _backlog_item_from_row(row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "title": row["title"],
        "item_type": row["item_type"],
        "summary": row["summary"],
        "status": row["status"],
        "priority": row["priority"],
        "lane": row["lane"] or "unassigned",
        "launch_scope": row["launch_scope"],
        "release_phase": row["release_phase"],
        "wave": row["release_phase"] or "",
        "owner_hint": row["owner_hint"],
        "updated_utc": row["updated_utc"],
        "refs": json_loads(row["refs_json"], []),
    }


def _backlog_detail_from_row(conn, row) -> dict[str, Any]:
    links = conn.execute(
        "SELECT ref_type, ref_value FROM backlog_item_links WHERE item_id=? ORDER BY ref_type, ref_value",
        (row["id"],),
    ).fetchall()
    return {
        "id": row["id"],
        "title": row["title"],
        "item_type": row["item_type"],
        "summary": row["summary"] or "",
        "root_cause_summary": row["root_cause_summary"] or "",
        "architectural_category": row["architectural_category"] or "",
        "status": row["status"],
        "priority": row["priority"] or "",
        "launch_scope": row["launch_scope"] or "",
        "release_phase": row["release_phase"] or "",
        "production_state": row["production_state"] or "",
        "disposition": row["disposition"] or "",
        "owner_hint": row["owner_hint"] or "",
        "lane": row["lane"] or "unassigned",
        "notes": row["notes"] or "",
        "refs": json_loads(row["refs_json"], []),
        "links": [
            {"type": link["ref_type"], "value": link["ref_value"]}
            for link in links
        ],
        "created_utc": row["created_utc"],
        "updated_utc": row["updated_utc"],
        "event_seq": row["event_seq"],
    }


def backlog_detail_block(item: dict[str, Any]) -> str:
    lines = [
        f"## {item.get('id', '')}",
        f"- status: {item.get('status', '')}",
        f"- lane: {item.get('lane', '')}",
        f"- priority: {item.get('priority', '')}",
        f"- type: {item.get('item_type', '')}",
        f"- owner: {item.get('owner_hint', '')}",
        f"- scope: {item.get('launch_scope', '')}",
        f"- wave: {item.get('release_phase') or item.get('wave') or ''}",
        f"- updated_utc: {item.get('updated_utc', '')}",
        "",
        str(item.get("title") or ""),
    ]
    sections = [
        ("Summary", item.get("summary")),
        ("Root Cause Summary", item.get("root_cause_summary")),
        ("Notes", item.get("notes")),
    ]
    for title, value in sections:
        text = str(value or "").strip()
        if text:
            lines.extend(["", f"## {title}", text])
    refs = item.get("refs") or []
    if refs:
        lines.extend(["", "## Refs"])
        for ref in refs:
            if isinstance(ref, dict):
                lines.append(f"- {ref.get('type', 'unknown')}:{ref.get('value', '')}")
            else:
                lines.append(f"- {ref}")
    links = item.get("links") or []
    if links:
        lines.extend(["", "## Links"])
        lines.extend(f"- {link.get('type', 'unknown')}:{link.get('value', '')}" for link in links)
    return "\n".join(lines).rstrip() + "\n"


def _message_item_from_row(row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "kind": row["kind"],
        "thread_id": row["thread_id"],
        "request_id": row["request_id"] or "",
        "sender": row["sender"],
        "recipients": json_loads(row["recipients_json"], []),
        "feature": row["feature_id"] or "",
        "title": row["title"] or row["summary"] or "",
        "status": row["status"],
        "resolution": row["resolution"] or "",
        "created_utc": row["created_utc"],
        "updated_utc": row["updated_utc"],
        "resolved_utc": row["resolved_utc"] or "",
        "event_seq": row["event_seq"],
    }


def _decision_item_from_row(row) -> dict[str, Any]:
    meta = json_loads(row["meta_json"], {})
    if not isinstance(meta, dict):
        meta = {}
    return {
        "id": row["human_id"],
        "dec_ulid": row["dec_ulid"],
        "title": row["title"],
        "tier": row["tier"],
        "status": row["status"],
        "owner": row["owner"] or "",
        "drift_risk": row["drift_risk"] or "",
        "display_utc": _decision_display_utc(row, meta),
        "proposed_utc": _decision_proposed_utc(row, meta),
        "accepted_utc": row["accepted_utc"],
        "in_force_utc": row["in_force_utc"],
        "retired_utc": row["retired_utc"],
        "last_verified_utc": row["last_verified_utc"],
        "superseded_by": row["superseded_by"] or "",
        "status_utc": _decision_status_utc(row, meta),
    }


def _contains(value: Any, expected: str) -> bool:
    return expected.lower() in str(value or "").lower()


def _backlog_item_matches_query(item: dict[str, Any], query: str) -> bool:
    haystack = "\n".join(str(item.get(field) or "") for field in BACKLOG_SEARCH_FIELDS)
    refs = item.get("refs") or []
    if refs:
        haystack += "\n" + "\n".join(str(ref) for ref in refs)
    return query.lower() in haystack.lower()


def _backlog_item_matches_quick_filter(item: dict[str, Any], quick_filter: str) -> bool:
    normalized = quick_filter.strip().lower().replace("-", "_")
    if normalized == "urgent":
        return _is_urgent(item)
    if normalized == "pending_user":
        return _is_pending_user(item)
    if normalized == "done":
        return _is_done(item)
    if normalized == "in_progress":
        return _is_in_progress(item)
    if normalized == "ahead":
        return not _is_done(item) and not _is_in_progress(item)
    return True


def _is_done(item: dict[str, Any]) -> bool:
    return str(item.get("status") or "").lower() in DONE_STATUSES


def _is_in_progress(item: dict[str, Any]) -> bool:
    status = str(item.get("status") or "").lower()
    lane = str(item.get("lane") or "").lower()
    return any(marker in status or marker in lane for marker in IN_PROGRESS_MARKERS)


def _is_pending_user(item: dict[str, Any]) -> bool:
    if _is_done(item):
        return False
    status = str(item.get("status") or "").lower()
    lane = str(item.get("lane") or "").lower()
    owner_hint = str(item.get("owner_hint") or "").lower()
    return any(marker in status or marker in lane for marker in PENDING_MARKERS) or owner_hint in {
        "operator",
        "user",
        "human",
    }


def _is_urgent(item: dict[str, Any]) -> bool:
    if _is_done(item):
        return False
    priority = str(item.get("priority") or "").upper()
    launch_scope = str(item.get("launch_scope") or "").lower()
    status = str(item.get("status") or "").lower()
    return priority == "P0" or "blocking" in launch_scope or "blocked" in status


def _decision_status_utc(row, meta: dict[str, Any]) -> str:
    status = str(row["status"] or "")
    if status == "retired" and row["retired_utc"]:
        return str(row["retired_utc"] or "")
    if status == "in_force" and row["in_force_utc"]:
        return str(row["in_force_utc"] or "")
    if status == "accepted" and row["accepted_utc"]:
        return str(row["accepted_utc"] or "")
    event_kind_by_status = {
        "accepted": "decision_accepted",
        "superseded": "decision_superseded",
        "retired": "decision_retired",
        "rejected": "decision_rejected",
    }
    expected_kind = event_kind_by_status.get(status)
    if expected_kind:
        for event in reversed(meta.get("event_log", [])):
            if isinstance(event, dict) and event.get("kind") == expected_kind:
                return str(event.get("occurred_utc") or row["proposed_utc"] or "")
    return _decision_proposed_utc(row, meta)


def _decision_proposed_utc(row, meta: dict[str, Any]) -> str:
    _ = meta
    return str(row["proposed_utc"] or "")


def _decision_display_utc(row, meta: dict[str, Any]) -> str:
    return _decision_status_utc(row, meta) or _decision_proposed_utc(row, meta)


def _feedback_request_counts(conn) -> dict[str, int]:
    return {
        "feedback_requests": conn.execute(
            f"SELECT COUNT(*) FROM messages WHERE kind='request' AND {FEEDBACK_REQUEST_PREDICATE_SQL}"
        ).fetchone()[0],
        "open_feedback_requests": conn.execute(
            f"""
            SELECT COUNT(*) FROM messages
            WHERE kind='request' AND status='open' AND {FEEDBACK_REQUEST_PREDICATE_SQL}
            """
        ).fetchone()[0],
        "closed_feedback_requests": conn.execute(
            f"""
            SELECT COUNT(*) FROM messages
            WHERE kind='request' AND status='closed' AND {FEEDBACK_REQUEST_PREDICATE_SQL}
            """
        ).fetchone()[0],
    }


def _snapshot_metrics(conn) -> dict[str, Any]:
    rows = conn.execute("SELECT * FROM backlog_items").fetchall()
    by_status = _count_by(rows, "status")
    by_lane = _count_by(rows, "lane", default="unassigned")
    by_priority = _count_by(rows, "priority", default="unprioritized")
    items = [_backlog_item_from_row(row) for row in rows]
    done = 0
    in_progress = 0
    pending_user = 0
    urgent = 0
    for item in items:
        if _is_done(item):
            done += 1
        if _is_in_progress(item):
            in_progress += 1
        if _is_pending_user(item):
            pending_user += 1
        if _is_urgent(item):
            urgent += 1
    ahead = max(len(items) - done - in_progress, 0)
    recent = [
        {
            "id": row["id"],
            "title": row["title"],
            "status": row["status"],
            "lane": row["lane"],
            "priority": row["priority"],
            "updated_utc": row["updated_utc"],
        }
        for row in conn.execute(
            """
            SELECT * FROM backlog_items
            ORDER BY updated_utc DESC, event_seq DESC
            LIMIT 6
            """
        ).fetchall()
    ]
    return {
        "done": done,
        "in_progress": in_progress,
        "ahead": ahead,
        "urgent": urgent,
        "pending_user": pending_user,
        "by_status": by_status,
        "by_lane": by_lane,
        "by_priority": by_priority,
        "recent_backlog": recent,
    }


def _count_by(rows, key: str, *, default: str = "unknown") -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row[key] or default)
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: item[0]))


WORKBENCH_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Agent Mesh Workbench</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f7f9;
      --panel: #ffffff;
      --ink: #17202a;
      --muted: #667085;
      --line: #d8dee8;
      --accent: #176b87;
      --accent-2: #6b5b95;
      --ok: #176b3a;
      --warn: #9a5b13;
      --error: #a33a45;
      --soft: #eef4f8;
      --radius: 8px;
      --mono: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      --sans: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font-family: var(--sans);
    }
    main {
      width: min(1500px, calc(100vw - 28px));
      margin: 18px auto 42px;
      display: grid;
      gap: 14px;
    }
    header, section {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: var(--radius);
    }
    header {
      padding: 16px;
      display: grid;
      gap: 10px;
    }
    h1, h2, h3, p { margin: 0; }
    h1 { font-size: 1.45rem; }
    h2 { font-size: 1rem; }
    h3 { font-size: 0.92rem; }
    p, .muted { color: var(--muted); line-height: 1.45; }
    code, pre { font-family: var(--mono); }
    .workbench-title {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      align-items: end;
      justify-content: space-between;
    }
    .repo-picker {
      display: grid;
      gap: 4px;
      min-width: min(480px, 100%);
    }
    .repo-picker span { color: var(--muted); font-size: 0.78rem; }
    .command-box {
      display: grid;
      gap: 6px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: #f9fbfc;
      padding: 10px;
    }
    .command-line {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
      min-width: 0;
    }
    .command-line code {
      flex: 1;
      min-width: 260px;
      overflow-x: auto;
      white-space: nowrap;
      background: #eef2f6;
      border-radius: 6px;
      padding: 7px 8px;
      font-size: 0.82rem;
    }
    .copy-status {
      min-width: 112px;
      color: var(--ok);
      font-size: 0.82rem;
      font-weight: 600;
    }
    .copy-status.error { color: var(--error); }
    .status {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
      font-family: var(--mono);
      font-size: 0.84rem;
    }
    .badge {
      display: inline-flex;
      gap: 6px;
      align-items: center;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: var(--soft);
      padding: 5px 9px;
      white-space: nowrap;
    }
    .connection {
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 9px 11px;
      font-size: 0.88rem;
      font-weight: 650;
    }
    .connection.checking { background: var(--soft); color: var(--muted); }
    .connection.online { background: #e9f7ef; border-color: #9bc9ab; color: var(--ok); }
    .connection.offline { background: #fff2f3; border-color: #e3abb1; color: var(--error); }
    .metrics {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(130px, 1fr));
      gap: 8px;
    }
    .metric {
      border: 1px solid var(--line);
      border-radius: 6px;
      background: #f9fbfc;
      padding: 10px;
      display: grid;
      gap: 4px;
    }
	    .metric strong { font-size: 1.35rem; line-height: 1; }
	    .metric span { color: var(--muted); font-size: 0.82rem; }
	    .metric.urgent strong { color: var(--error); }
	    .metric[data-backlog-filter],
	    .metric[data-message-kind],
	    .metric[data-message-feature] {
	      cursor: pointer;
	    }
	    .metric[data-backlog-filter]:hover,
	    .metric[data-message-kind]:hover,
	    .metric[data-message-feature]:hover {
	      border-color: var(--accent);
	      background: #edf8fb;
	    }
	    .tabs {
	      display: flex;
	      flex-wrap: wrap;
	      gap: 8px;
	    }
	    .tab-button {
	      background: #e8edf2;
	      color: var(--ink);
	      border: 1px solid var(--line);
	    }
	    .tab-button.active {
	      background: var(--accent);
	      color: white;
	      border-color: var(--accent);
	    }
	    .tab-panel { display: none; }
	    .tab-panel.active { display: grid; }
	    .dashboard-grid {
	      display: grid;
	      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
	      gap: 10px;
	    }
	    .mini-list {
	      border: 1px solid var(--line);
	      border-radius: 6px;
	      background: #f9fbfc;
	      padding: 10px;
	      display: grid;
	      gap: 6px;
	      align-content: start;
	    }
	    .mini-list div {
	      display: flex;
	      justify-content: space-between;
	      gap: 10px;
	      border-bottom: 1px solid #edf1f5;
	      padding-bottom: 5px;
	      font-size: 0.86rem;
	    }
	    .mini-list div:last-child { border-bottom: 0; padding-bottom: 0; }
	    .mini-list div[data-backlog-field] { cursor: pointer; }
	    .mini-list div[data-backlog-field]:hover span {
	      color: var(--accent);
	      text-decoration: underline;
	    }
	    .field-note {
	      color: var(--muted);
	      font-size: 0.72rem;
	      font-weight: 600;
	      margin-left: 4px;
	      text-transform: uppercase;
	    }
	    .required-mark {
	      color: var(--error);
	      font-size: 0.95rem;
	      font-weight: 700;
	      margin-left: 3px;
	    }
	    .grid {
      display: grid;
      grid-template-columns: minmax(320px, 0.9fr) minmax(420px, 1.1fr);
      gap: 14px;
    }
    section {
      padding: 14px;
      display: grid;
      gap: 12px;
      min-width: 0;
    }
    .stack { display: grid; gap: 10px; }
    .row {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
    }
    label {
      display: grid;
      gap: 5px;
      font-size: 0.88rem;
      color: var(--muted);
    }
    input, textarea, select {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 9px 10px;
      font: inherit;
      color: var(--ink);
      background: #fff;
    }
    textarea { min-height: 120px; resize: vertical; }
    button {
      border: 0;
      border-radius: 6px;
      padding: 9px 11px;
      background: var(--accent);
      color: white;
      font: inherit;
      cursor: pointer;
    }
    button.secondary { background: var(--accent-2); }
    button.ghost { background: #e8edf2; color: var(--ink); }
    button:disabled { cursor: not-allowed; opacity: 0.52; }
    .output {
      min-height: 120px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: #111827;
      color: #f9fafb;
      padding: 12px;
      white-space: pre-wrap;
      overflow: visible;
      font: 0.84rem/1.45 var(--mono);
    }
    .table-wrap {
      border: 1px solid var(--line);
      border-radius: 6px;
      overflow: auto;
      max-height: 420px;
    }
    table { width: 100%; border-collapse: collapse; font-size: 0.88rem; }
    th, td {
      border-bottom: 1px solid var(--line);
      padding: 8px;
      text-align: left;
      vertical-align: top;
    }
    th {
      position: sticky;
      top: 0;
      background: #eef2f6;
      z-index: 1;
    }
    th[data-sort-key] {
      cursor: pointer;
      user-select: none;
    }
    th[data-sort-key]:hover {
      color: var(--accent);
      background: #dfeaf2;
    }
    .sort-mark {
      display: inline-block;
      min-width: 10px;
      margin-left: 4px;
      color: var(--accent);
      font-size: 0.75rem;
    }
    tr[data-message-id],
    tr[data-backlog-view-id],
    tr[data-decision-id] {
      cursor: pointer;
    }
    tr[data-message-id]:hover,
    tr[data-backlog-view-id]:hover,
    tr[data-decision-id]:hover {
      background: #edf8fb;
    }
    tr[data-message-id]:hover code,
    tr[data-backlog-view-id]:hover code,
    tr[data-decision-id]:hover code {
      color: var(--accent);
      text-decoration: underline;
    }
    .kanban {
      display: flex;
      gap: 10px;
      overflow-x: auto;
      padding-bottom: 4px;
    }
    .lane {
      flex: 0 0 260px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: #f9fbfc;
      padding: 10px;
      display: grid;
      gap: 8px;
      align-content: start;
    }
    .lane.dragover {
      border-color: var(--accent);
      background: #edf8fb;
    }
    .card {
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 9px;
      background: white;
      display: grid;
      gap: 6px;
    }
    .card strong { line-height: 1.3; }
    .card small { color: var(--muted); font-family: var(--mono); }
    .card[draggable="true"] { cursor: grab; }
    .card[draggable="true"]:active { cursor: grabbing; }
    .mini-field {
      min-width: 92px;
      padding: 5px 6px;
      font-size: 0.82rem;
    }
	    .edit-status {
	      min-height: 18px;
	      color: var(--muted);
	      font-size: 0.84rem;
	    }
	    .dropzone {
	      border: 1px dashed var(--line);
	      border-radius: 6px;
	      background: #f9fbfc;
	      padding: 14px;
	      display: grid;
	      gap: 9px;
	      text-align: center;
	    }
	    .dropzone.dragover {
	      border-color: var(--accent);
	      background: #edf8fb;
	    }
	    .file-list {
	      white-space: pre-wrap;
	      text-align: left;
	      font: 0.82rem/1.4 var(--mono);
	      color: var(--muted);
	    }
	    .search-controls {
	      display: grid;
	      grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
	      gap: 8px;
	      align-items: end;
	    }
	    .search-controls label:first-child {
	      grid-column: span 2;
	    }
	    @media (max-width: 900px) {
	      .grid { grid-template-columns: 1fr; }
	      .search-controls { grid-template-columns: 1fr; }
	      .search-controls label:first-child { grid-column: span 1; }
	    }
  </style>
</head>
<body>
<main>
	  <header>
	    <div class="workbench-title">
	      <h1>Agent Mesh Workbench</h1>
	      <label class="repo-picker">Active repository
	        <select id="repo-selector" data-requires-server disabled>
	          <option value="">Loading registered repos...</option>
	        </select>
	        <span>All feedback, status changes, backlog updates, and decision reads are scoped to this repository.</span>
	      </label>
	    </div>
	    <div class="command-box" id="manual-launch-panel">
      <div class="command-line">
        <span class="muted">Start / restart</span>
        <code id="start-command">__AGENT_MESH_START_COMMAND__</code>
        <button type="button" class="ghost" id="copy-start-command">Copy</button>
        <span id="copy-start-status" class="copy-status" role="status" aria-live="polite"></span>
      </div>
      <div class="command-line">
        <span class="muted">Bookmark</span>
        <code id="bookmark-path">__AGENT_MESH_BOOKMARK_PATH__</code>
        <a id="bookmark-link" href="__AGENT_MESH_BOOKMARK_URL__">Open bookmark file</a>
	      </div>
	    </div>
	    <div class="command-box" id="managed-launch-panel" hidden>
      <div class="command-line">
        <span class="muted">Automatic startup</span>
        <strong>Enabled for this user</strong>
        <button type="button" class="ghost" id="recheck-server">Reconnect</button>
        <span id="recheck-server-status" class="copy-status" role="status" aria-live="polite"></span>
      </div>
      <div class="command-line">
        <span class="muted">Bookmark</span>
        <code id="managed-bookmark-path">__AGENT_MESH_BOOKMARK_PATH__</code>
        <a id="managed-bookmark-link" href="__AGENT_MESH_BOOKMARK_URL__">Open bookmark file</a>
      </div>
	    </div>
	    <div id="server-connection" class="connection checking" role="status" aria-live="polite">Checking Workbench server...</div>
	    <div id="status" class="status"><span class="badge">loading</span></div>
	  </header>

	  <nav class="tabs" aria-label="Workbench views">
	    <button class="tab-button active" data-tab="dashboard">Dashboard</button>
	    <button class="tab-button" data-tab="feedback">Verify / Feedback</button>
	    <button class="tab-button" data-tab="messages">Messages</button>
	    <button class="tab-button" data-tab="backlog">Backlog</button>
	    <button class="tab-button" data-tab="decisions">Decisions</button>
	    <button class="tab-button" data-tab="kanban">Kanban</button>
	  </nav>

	  <section id="tab-dashboard" class="tab-panel active">
	    <h2>Dashboard</h2>
	    <div id="snapshot" class="metrics"></div>
	    <div class="dashboard-grid">
	      <div>
	        <h3>By Status</h3>
	        <div id="dashboard-status" class="mini-list"></div>
	      </div>
	      <div>
	        <h3>By Lane</h3>
	        <div id="dashboard-lane" class="mini-list"></div>
	      </div>
	      <div>
	        <h3>By Priority</h3>
	        <div id="dashboard-priority" class="mini-list"></div>
	      </div>
	      <div>
	        <h3>Recent Backlog</h3>
	        <div id="dashboard-recent" class="mini-list"></div>
	      </div>
	    </div>
	  </section>

	  <section id="tab-feedback" class="tab-panel">
	    <h2>Verify / Feedback</h2>
	    <div class="stack">
	      <label>Title <span class="required-mark">*</span><input id="fb-title" placeholder="Feature review feedback" required aria-required="true"></label>
	      <div class="row">
	        <label style="flex:1">Related REQ/RES <span class="field-note">Optional</span><input id="fb-related" placeholder="REQ-... or RES-..."></label>
	        <label style="width:150px">Severity <span class="field-note">Optional</span>
	          <select id="fb-severity">
	            <option>normal</option>
	            <option>launch-important</option>
	            <option>launch-blocking</option>
	            <option>nit</option>
	          </select>
	        </label>
	      </div>
	      <label>Target or area <span class="field-note">Optional</span><input id="fb-target" placeholder="feature, route, component, or backlog id"></label>
	      <label>Notes <span class="required-mark">*</span><textarea id="fb-notes" placeholder="What you observed, expected behavior, and acceptance criteria." required aria-required="true"></textarea></label>
	      <div id="attachment-dropzone" class="dropzone">
	        <div><strong>Drop screenshots or a short MOV here</strong> or use the picker.</div>
	        <div class="row" style="justify-content:center;">
	          <button type="button" class="ghost" id="pick-attachments" data-requires-server>Choose files</button>
	          <button type="button" class="ghost" id="clear-attachments">Clear attachment paths</button>
	        </div>
	        <input id="attachment-picker" type="file" accept="image/*" multiple hidden>
	        <div id="attachment-upload-status" class="muted">PNG/JPEG screenshots and MOV clips are supported up to 40 MB per file and 40 MB per batch. Existing references survive a failed upload.</div>
	        <div id="attachment-upload-list" class="file-list"></div>
	      </div>
	      <label>Attachment paths <span class="field-note">Optional</span><textarea id="fb-screenshots" placeholder="- .agent-mesh/attachments/screenshots/screenshot.png"></textarea></label>
	      <label>Refs <span class="field-note">Optional</span><textarea id="fb-refs" placeholder="One path, URL, REQ, RES, BKL, or decision id per line."></textarea></label>
	      <div class="row">
	        <button id="submit-feedback" data-requires-server>Submit REQ</button>
	        <button type="button" class="ghost" id="clear-feedback">Clear</button>
	        <button id="draft-feedback" data-requires-server>Draft feedback</button>
	        <button class="ghost" id="copy-feedback" data-requires-server>Copy draft</button>
	        <button class="ghost" id="copy-request" data-requires-server>Copy agent request</button>
	      </div>
	      <div id="feedback-submit-status" class="edit-status"></div>
	      <pre id="feedback-output" class="output"></pre>
	    </div>
	  </section>

	  <section id="tab-messages" class="tab-panel">
	    <div class="row">
	      <h2 style="flex:1">Messages</h2>
	      <button class="ghost" id="reload-messages">Reload</button>
	    </div>
	    <div class="search-controls">
	      <label>Search messages <span class="field-note">Optional</span><input id="message-search" placeholder="REQ, RES, title, sender, text..."></label>
	      <label>Status <span class="field-note">Optional</span>
	        <select id="message-status-filter">
	          <option value="">Any</option>
	          <option value="open">Open</option>
	          <option value="closed">Closed</option>
	        </select>
	      </label>
	      <label>Kind <span class="field-note">Optional</span>
	        <select id="message-kind-filter">
	          <option value="">Any</option>
	          <option value="request">Request</option>
	          <option value="response">Response</option>
	        </select>
	      </label>
	      <label>Feature <span class="field-note">Optional</span>
	        <select id="message-feature-filter">
	          <option value="">Any</option>
	          <option value="feedback">Feedback</option>
	        </select>
	      </label>
	      <button id="search-messages">Search</button>
	    </div>
	    <div class="row">
	      <label style="flex:1">Message ID <span class="required-mark">*</span><input id="message-id" placeholder="REQ-... or RES-..." required aria-required="true"></label>
	      <button id="lookup-message">Lookup</button>
	    </div>
	    <div id="message-status-panel" class="row" style="display:none">
	      <label style="width:150px">Request status
	        <select id="message-new-status">
	          <option value="closed">Closed</option>
	          <option value="open">Open</option>
	        </select>
	      </label>
	      <label style="flex:1">Reason <input id="message-status-reason" placeholder="Triaged into backlog, test request, reopened for follow-up..."></label>
	      <button id="save-message-status">Save status</button>
	    </div>
	    <div id="message-edit-status" class="edit-status"></div>
	    <div class="table-wrap">
	      <table>
	        <thead><tr>
	          <th data-sort-table="messages" data-sort-key="id" aria-sort="none">ID <span class="sort-mark"></span></th>
	          <th data-sort-table="messages" data-sort-key="created_utc" aria-sort="none">Date <span class="sort-mark"></span></th>
	          <th data-sort-table="messages" data-sort-key="status" aria-sort="none">Status <span class="sort-mark"></span></th>
	          <th data-sort-table="messages" data-sort-key="kind" aria-sort="none">Kind <span class="sort-mark"></span></th>
	          <th data-sort-table="messages" data-sort-key="feature" aria-sort="none">Feature <span class="sort-mark"></span></th>
	          <th data-sort-table="messages" data-sort-key="sender" aria-sort="none">From <span class="sort-mark"></span></th>
	          <th data-sort-table="messages" data-sort-key="recipients" aria-sort="none">To <span class="sort-mark"></span></th>
	          <th data-sort-table="messages" data-sort-key="title" aria-sort="none">Title <span class="sort-mark"></span></th>
	        </tr></thead>
	        <tbody id="message-body"></tbody>
	      </table>
	    </div>
	    <pre id="message-output" class="output"></pre>
	  </section>

	  <section id="tab-backlog" class="tab-panel">
	    <div class="row">
	      <h2 style="flex:1">Backlog</h2>
	      <button class="ghost" id="reload-backlog">Reload</button>
	    </div>
	    <div class="search-controls">
	      <label>Search all columns <span class="field-note">Optional</span><input id="backlog-search" placeholder="BKL, title, owner, scope, ref..."></label>
	      <label>Quick filter <span class="field-note">Optional</span>
	        <select id="backlog-quick-filter">
	          <option value="">Any</option>
	          <option value="pending_user">Pending user</option>
	          <option value="urgent">Urgent</option>
	          <option value="done">Done</option>
	          <option value="in_progress">In progress</option>
	          <option value="ahead">Ahead</option>
	        </select>
	      </label>
	      <label>Status <span class="field-note">Optional</span><input id="backlog-status-filter" placeholder="open"></label>
	      <label>Lane <span class="field-note">Optional</span><input id="backlog-lane-filter" placeholder="verify"></label>
	      <label>Priority <span class="field-note">Optional</span><input id="backlog-priority-filter" placeholder="P0"></label>
	      <label>Owner <span class="field-note">Optional</span><input id="backlog-owner-filter" placeholder="human"></label>
	      <label>Type <span class="field-note">Optional</span><input id="backlog-type-filter" placeholder="bug"></label>
	      <label>Scope <span class="field-note">Optional</span><input id="backlog-scope-filter" placeholder="launch"></label>
	      <label>Wave <span class="field-note">Optional</span><input id="backlog-wave-filter" placeholder="pre-launch"></label>
	      <button id="search-backlog">Search</button>
	      <button class="ghost" id="clear-backlog-filters">Clear</button>
	    </div>
	    <div id="backlog-edit-status" class="edit-status"></div>
		    <div class="table-wrap">
		      <table>
        <thead><tr>
          <th data-sort-table="backlog" data-sort-key="id" aria-sort="none">ID <span class="sort-mark"></span></th>
          <th data-sort-table="backlog" data-sort-key="updated_utc" aria-sort="none">Updated <span class="sort-mark"></span></th>
          <th data-sort-table="backlog" data-sort-key="status" aria-sort="none">Status <span class="sort-mark"></span></th>
          <th data-sort-table="backlog" data-sort-key="lane" aria-sort="none">Lane <span class="sort-mark"></span></th>
          <th data-sort-table="backlog" data-sort-key="priority" aria-sort="none">Priority <span class="sort-mark"></span></th>
          <th data-sort-table="backlog" data-sort-key="item_type" aria-sort="none">Type <span class="sort-mark"></span></th>
          <th data-sort-table="backlog" data-sort-key="owner_hint" aria-sort="none">Owner <span class="sort-mark"></span></th>
          <th data-sort-table="backlog" data-sort-key="launch_scope" aria-sort="none">Scope <span class="sort-mark"></span></th>
          <th data-sort-table="backlog" data-sort-key="wave" aria-sort="none">Wave <span class="sort-mark"></span></th>
          <th data-sort-table="backlog" data-sort-key="title" aria-sort="none">Title <span class="sort-mark"></span></th>
        </tr></thead>
	        <tbody id="backlog-body"></tbody>
	      </table>
	    </div>
	    <pre id="backlog-output" class="output"></pre>
	  </section>

	  <section id="tab-decisions" class="tab-panel">
	    <div class="row">
	      <h2 style="flex:1">Decisions</h2>
	      <button class="ghost" id="reload-decisions">Reload</button>
	    </div>
	    <div class="search-controls">
	      <label>Search by ID or text <span class="field-note">Optional</span><input id="decision-search" placeholder="D010 or architecture"></label>
	      <label>Status <span class="field-note">Optional</span>
	        <select id="decision-status-filter">
	          <option value="">Any</option>
	          <option value="proposed">Proposed</option>
	          <option value="accepted">Accepted</option>
	          <option value="in_force">In force</option>
	          <option value="superseded">Superseded</option>
	          <option value="retired">Retired</option>
	          <option value="rejected">Rejected</option>
	        </select>
	      </label>
	      <label>Tier <span class="field-note">Optional</span><input id="decision-tier-filter" placeholder="architecture_contract"></label>
	      <button id="search-decisions">Search</button>
	    </div>
	    <div class="table-wrap">
	      <table>
	        <thead><tr>
	          <th data-sort-table="decisions" data-sort-key="id" aria-sort="none">ID <span class="sort-mark"></span></th>
	          <th data-sort-table="decisions" data-sort-key="decision_date" aria-sort="none">Date <span class="sort-mark"></span></th>
	          <th data-sort-table="decisions" data-sort-key="status" aria-sort="none">Status <span class="sort-mark"></span></th>
	          <th data-sort-table="decisions" data-sort-key="tier" aria-sort="none">Tier <span class="sort-mark"></span></th>
	          <th data-sort-table="decisions" data-sort-key="owner" aria-sort="none">Owner <span class="sort-mark"></span></th>
	          <th data-sort-table="decisions" data-sort-key="title" aria-sort="none">Title <span class="sort-mark"></span></th>
	        </tr></thead>
	        <tbody id="decision-body"></tbody>
	      </table>
	    </div>
	    <pre id="decision-output" class="output"></pre>
	  </section>

	  <section id="tab-kanban" class="tab-panel">
	    <div class="row">
	      <h2 style="flex:1">Kanban</h2>
	      <button class="ghost" id="reload-kanban">Reload</button>
    </div>
    <div id="kanban" class="kanban"></div>
  </section>
</main>
<script>
const API_BASE = __AGENT_MESH_API_BASE__;
const DEFAULT_REPO_ID = __AGENT_MESH_DEFAULT_REPO_ID__;
const EMBEDDED_API_TOKEN = __AGENT_MESH_ACCESS_TOKEN__;
const MANAGED_SERVICE = __AGENT_MESH_MANAGED_SERVICE__;
const FRAGMENT_TOKEN = new URLSearchParams(window.location.hash.slice(1)).get('token') || '';
const API_TOKEN = EMBEDDED_API_TOKEN || FRAGMENT_TOKEN;
if (FRAGMENT_TOKEN) history.replaceState(null, '', window.location.pathname + window.location.search);
const $ = (id) => document.getElementById(id);
const ATTACHMENT_STATUS_DEFAULT = 'PNG/JPEG screenshots and MOV clips are supported up to 40 MB per file and 40 MB per batch. Existing references survive a failed upload.';
const FEEDBACK_PENDING_KEY = 'agent-mesh.feedback.pending.v2';
const FEEDBACK_RECEIPT_KEY = 'agent-mesh.feedback.receipt.v2';
const FEEDBACK_DRAFT_KEY = 'agent-mesh.feedback.draft.v1';
const ACTIVE_REPO_KEY = 'agent-mesh.workbench.active-repo.v1';
const MAX_ATTACHMENT_BYTES = __AGENT_MESH_MAX_ATTACHMENT_BYTES__;
const MAX_ATTACHMENT_TOTAL_BYTES = __AGENT_MESH_MAX_ATTACHMENT_TOTAL_BYTES__;
const FEEDBACK_INPUT_IDS = [
  'fb-title',
  'fb-related',
  'fb-severity',
  'fb-target',
  'fb-notes',
  'fb-refs',
  'fb-screenshots',
];
let lastDraft = null;
let activeRepoId = DEFAULT_REPO_ID;
let projectRegistryLoaded = false;
let feedbackSubmissionId = '';
let recoveringFeedbackSubmission = false;
let lastMessage = null;
let draggedBacklogId = null;
let messageRows = [];
let backlogRows = [];
let decisionRows = [];
const tableSort = {
  messages: { key: 'created_utc', direction: 'desc' },
  backlog: { key: 'updated_utc', direction: 'desc' },
  decisions: { key: 'decision_date', direction: 'desc' },
};

function setServerConnection(state, detail = '') {
  const online = state === 'online';
  const indicator = $('server-connection');
  indicator.className = `connection ${state}`;
  indicator.textContent = online
    ? 'Server online - submit and live data are available.'
    : state === 'offline'
      ? MANAGED_SERVICE
        ? `Server reconnecting - the automatic service will restart it. ${detail}`.trim()
        : `Server offline - start or restart the command above. ${detail}`.trim()
      : 'Checking Workbench server...';
  document.querySelectorAll('[data-requires-server]').forEach((button) => {
    button.disabled = !online || (button.id === 'repo-selector' && !projectRegistryLoaded);
  });
}

async function api(path, options = {}) {
  let requestPath = path;
  if (activeRepoId && path !== '/api/health' && path !== '/api/projects') {
    requestPath += `${path.includes('?') ? '&' : '?'}repo=${encodeURIComponent(activeRepoId)}`;
  }
  let response;
  try {
    response = await fetch(`${API_BASE}${requestPath}`, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        'X-Agent-Mesh-Token': API_TOKEN,
        ...(options.headers || {}),
      },
    });
  } catch (cause) {
    setServerConnection('offline', 'Clear remains available; writes require the server.');
    const error = new Error(`Workbench server is offline at ${API_BASE || 'this page'}.`);
    error.networkFailure = true;
    error.cause = cause;
    throw error;
  }
  setServerConnection('online');
  const payload = await response.json();
  if (!response.ok || payload.ok === false) throw new Error(payload.error || response.statusText);
  return payload;
}

function repoStorageKey(base) {
  return `${base}.${activeRepoId || 'default'}`;
}

async function loadProjects() {
  const result = await api('/api/projects');
  const selector = $('repo-selector');
  selector.innerHTML = result.projects.map((project) => (
    `<option value="${escapeHtml(project.id)}">${escapeHtml(project.name)} - ${escapeHtml(project.root)}</option>`
  )).join('');
  let stored = '';
  try {
    stored = localStorage.getItem(ACTIVE_REPO_KEY) || '';
  } catch (error) {
    stored = '';
  }
  const available = new Set(result.projects.map((project) => project.id));
  activeRepoId = available.has(stored)
    ? stored
    : available.has(DEFAULT_REPO_ID)
      ? DEFAULT_REPO_ID
      : result.default_repo_id || result.projects[0]?.id || '';
  selector.value = activeRepoId;
  projectRegistryLoaded = true;
  selector.disabled = !activeRepoId;
  return result.projects;
}

function resetRepoView() {
  lastDraft = null;
  feedbackSubmissionId = '';
  lastMessage = null;
  draggedBacklogId = null;
  messageRows = [];
  backlogRows = [];
  decisionRows = [];
  FEEDBACK_INPUT_IDS.filter((id) => id !== 'fb-severity').forEach((id) => {
    $(id).value = '';
  });
  $('fb-severity').value = 'normal';
  $('attachment-picker').value = '';
  $('attachment-upload-list').textContent = '';
  $('attachment-upload-status').textContent = ATTACHMENT_STATUS_DEFAULT;
  $('feedback-output').textContent = '';
  $('feedback-submit-status').textContent = 'Repository changed; feedback will be submitted to the active repository.';
  $('message-output').textContent = '';
  $('backlog-output').textContent = '';
  $('decision-output').textContent = '';
}

async function switchRepo(identifier) {
  if (!identifier || identifier === activeRepoId) return;
  activeRepoId = identifier;
  try {
    localStorage.setItem(ACTIVE_REPO_KEY, identifier);
  } catch (error) {
    // The selector still works when file-page storage is unavailable.
  }
  resetRepoView();
  restoreFeedbackDraft();
  await Promise.all([loadStatus(), loadMessages(), loadBacklog(), loadDecisions(), loadKanban()]);
  await recoverPendingFeedbackSubmission();
}

async function checkServerConnection() {
  try {
    await api('/api/health');
    if (projectRegistryLoaded) await recoverPendingFeedbackSubmission();
  } catch (error) {
    if (!error.networkFailure) {
      setServerConnection('offline', error.message);
    }
  }
}

function text(value) {
  return value == null ? '' : String(value);
}

function escapeHtml(value) {
  return text(value).replace(/[&<>"']/g, (ch) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
  }[ch]));
}

function formatLocalDateTime(value) {
  const raw = text(value).trim();
  if (!raw) return '';
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return raw;
  return parsed.toLocaleString(undefined, {
    year: 'numeric',
    month: 'short',
    day: '2-digit',
    hour: 'numeric',
    minute: '2-digit',
  });
}

function sortValue(row, key) {
  if (key === 'recipients') return (row.recipients || []).join(', ').toLowerCase();
  if (key === 'wave') return text(row.wave || row.release_phase).toLowerCase();
  if (key === 'decision_date') {
    return Date.parse(row.display_utc || row.status_utc || row.proposed_utc || '') || 0;
  }
  const value = row[key];
  if (key.endsWith('_utc')) return Date.parse(value || '') || 0;
  return text(value).toLowerCase();
}

function sortedRows(table, rows) {
  const state = tableSort[table];
  const direction = state.direction === 'desc' ? -1 : 1;
  return [...rows].sort((a, b) => {
    const left = sortValue(a, state.key);
    const right = sortValue(b, state.key);
    if (left < right) return -1 * direction;
    if (left > right) return 1 * direction;
    return text(a.id || a.dec_ulid).localeCompare(text(b.id || b.dec_ulid));
  });
}

function renderSortIndicators(table) {
  const state = tableSort[table];
  document.querySelectorAll(`[data-sort-table="${table}"][data-sort-key]`).forEach((header) => {
    const active = header.getAttribute('data-sort-key') === state.key;
    header.setAttribute(
      'aria-sort',
      active ? (state.direction === 'desc' ? 'descending' : 'ascending') : 'none',
    );
    const marker = header.querySelector('.sort-mark');
    if (marker) marker.textContent = active ? (state.direction === 'desc' ? 'v' : '^') : '';
  });
}

function setTableSort(table, key) {
  const state = tableSort[table];
  if (!state || !key) return;
  if (state.key === key) {
    state.direction = state.direction === 'desc' ? 'asc' : 'desc';
  } else {
    state.key = key;
    state.direction = key.endsWith('_utc') || key === 'decision_date' ? 'desc' : 'asc';
  }
  if (table === 'messages') renderMessageRows(messageRows);
  if (table === 'backlog') renderBacklogRows(backlogRows);
  if (table === 'decisions') renderDecisionRows(decisionRows);
}

async function loadStatus() {
  try {
    const status = await api('/api/status');
    const c = status.counts;
	    $('status').innerHTML = [
	      `repo ${status.project.root}`,
	      `sender ${status.project.default_sender}`,
	      `recipient ${status.project.default_recipient}`,
	      `events ${status.agent_mesh.last_event_seq}`,
      `open ${c.open_requests}`,
      `closed ${c.closed_requests}`,
      `open feedback ${c.open_feedback_requests}`,
      `responses ${c.responses}`,
      `backlog ${c.backlog_items}`,
      `decisions ${c.decisions}`,
      `db ${status.agent_mesh.db_file}`,
    ].map((item) => `<span class="badge">${escapeHtml(item)}</span>`).join('');
    const s = status.snapshot;
    $('snapshot').innerHTML = [
      ['Done', s.done, 'backlog items closed or complete', '', 'done'],
      ['In progress', s.in_progress, 'active or doing lanes/statuses', '', 'in_progress'],
      ['Ahead', s.ahead, 'not done or in progress yet', '', 'ahead'],
      ['Pending user', s.pending_user, 'review, verify, pending, or user-owned', '', 'pending_user'],
      ['Urgent', s.urgent, 'P0, blocked, or launch-blocking', 'urgent', 'urgent'],
      ['Open REQs', c.open_requests, 'request threads still open', '', '', 'request', '', 'open'],
      ['Open Feedback', c.open_feedback_requests, 'feedback REQs still open', 'urgent', '', 'request', 'feedback', 'open'],
      ['Closed Feedback', c.closed_feedback_requests, 'feedback REQs already closed', '', '', 'request', 'feedback', 'closed'],
    ].map(([label, value, hint, cls, filter, messageKind, messageFeature, messageStatus]) => `
      <div class="metric ${cls}"
        ${filter ? `data-backlog-filter="${escapeHtml(filter)}"` : ''}
        ${messageKind ? `data-message-kind="${escapeHtml(messageKind)}" data-message-feature="${escapeHtml(messageFeature || '')}" data-message-status="${escapeHtml(messageStatus || '')}"` : ''}
        ${filter || messageKind ? 'role="button" tabindex="0"' : ''}>
        <strong>${escapeHtml(value)}</strong>
        <span>${escapeHtml(label)}</span>
        <span>${escapeHtml(hint)}</span>
      </div>
    `).join('');
    renderBreakdown('dashboard-status', s.by_status, 'status');
    renderBreakdown('dashboard-lane', s.by_lane, 'lane');
    renderBreakdown('dashboard-priority', s.by_priority, 'priority');
    renderRecentBacklog(s.recent_backlog || []);
  } catch (error) {
    $('status').innerHTML = `<span class="badge">error ${escapeHtml(error.message)}</span>`;
    $('snapshot').innerHTML = '';
  }
}

function renderBreakdown(targetId, values, field) {
  const entries = Object.entries(values || {});
  $(targetId).innerHTML = entries.map(([label, count]) => `
    <div data-backlog-field="${escapeHtml(field)}" data-backlog-value="${escapeHtml(label)}" role="button" tabindex="0">
      <span>${escapeHtml(label)}</span><strong>${escapeHtml(count)}</strong>
    </div>
  `).join('') || '<p class="muted">No items.</p>';
}

function renderRecentBacklog(items) {
  $('dashboard-recent').innerHTML = items.map((item) => `
    <div>
      <span><code>${escapeHtml(item.id)}</code> ${escapeHtml(item.title)}</span>
      <strong>${escapeHtml(item.status || item.lane || '')}</strong>
    </div>
  `).join('') || '<p class="muted">No recent backlog.</p>';
}

function feedbackPayload() {
  return {
    title: $('fb-title').value,
    related_id: $('fb-related').value,
    severity: $('fb-severity').value,
    target: $('fb-target').value,
    notes: $('fb-notes').value,
    refs: $('fb-refs').value,
    screenshots: $('fb-screenshots').value,
  };
}

function newFeedbackSubmissionId() {
  if (globalThis.crypto && typeof globalThis.crypto.randomUUID === 'function') {
    return `fb-${globalThis.crypto.randomUUID()}`;
  }
  return `fb-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 14)}`;
}

function storeJson(key, value) {
  try {
    localStorage.setItem(key, JSON.stringify(value));
  } catch (error) {
    // The form remains usable when file-page storage is unavailable.
  }
}

function readJson(key) {
  try {
    const raw = localStorage.getItem(key);
    return raw ? JSON.parse(raw) : null;
  } catch (error) {
    return null;
  }
}

function removeStored(key) {
  try {
    localStorage.removeItem(key);
  } catch (error) {
    // Ignore unavailable file-page storage.
  }
}

function feedbackAttachmentPaths() {
  return $('fb-screenshots').value.split(/\\r?\\n/).map((value) => (
    value.trim().replace(/^[-*]\\s+/, '')
  )).filter(Boolean);
}

function renderAttachmentList() {
  $('attachment-upload-list').textContent = feedbackAttachmentPaths().join('\\n');
}

function rememberFeedbackDraft() {
  storeJson(repoStorageKey(FEEDBACK_DRAFT_KEY), feedbackPayload());
}

function restoreFeedbackDraft() {
  const draft = readJson(repoStorageKey(FEEDBACK_DRAFT_KEY));
  if (draft) restoreFeedbackInputs(draft);
  renderAttachmentList();
  return draft;
}

function rememberPendingFeedback(payload) {
  storeJson(repoStorageKey(FEEDBACK_DRAFT_KEY), payload);
  storeJson(repoStorageKey(FEEDBACK_PENDING_KEY), {
    submission_id: payload.submission_id,
    payload,
    started_utc: new Date().toISOString(),
  });
}

function rememberFeedbackReceipt(receipt) {
  storeJson(repoStorageKey(FEEDBACK_RECEIPT_KEY), {
    submission_id: receipt.submission_id,
    request_id: receipt.request_id,
    event_seq: receipt.event_seq,
    recorded_utc: new Date().toISOString(),
  });
}

function restoreFeedbackInputs(payload) {
  $('fb-title').value = payload.title || '';
  $('fb-related').value = payload.related_id || '';
  $('fb-severity').value = payload.severity || 'normal';
  $('fb-target').value = payload.target || '';
  $('fb-notes').value = payload.notes || '';
  $('fb-refs').value = payload.refs || '';
  $('fb-screenshots').value = payload.screenshots || '';
  renderAttachmentList();
}

function invalidateFeedbackSubmission() {
  feedbackSubmissionId = '';
  removeStored(repoStorageKey(FEEDBACK_PENDING_KEY));
}

function clearFeedbackInputs() {
  FEEDBACK_INPUT_IDS.filter((id) => id !== 'fb-severity').forEach((id) => {
    $(id).value = '';
  });
  $('fb-severity').value = 'normal';
  $('attachment-picker').value = '';
  renderAttachmentList();
  $('attachment-upload-status').textContent = ATTACHMENT_STATUS_DEFAULT;
  lastDraft = null;
  invalidateFeedbackSubmission();
  removeStored(repoStorageKey(FEEDBACK_DRAFT_KEY));
}

async function recoverPendingFeedbackSubmission() {
  if (recoveringFeedbackSubmission) return null;
  const pending = readJson(repoStorageKey(FEEDBACK_PENDING_KEY));
  if (!pending || !pending.submission_id || !pending.payload) return null;
  recoveringFeedbackSubmission = true;
  feedbackSubmissionId = pending.submission_id;
  restoreFeedbackInputs(pending.payload);
  try {
    const receipt = await api(`/api/feedback/receipt?id=${encodeURIComponent(pending.submission_id)}`);
    if (!receipt.found) {
      $('feedback-submit-status').textContent = 'Previous submission was not found; fields were restored and are safe to retry.';
      return receipt;
    }
    lastDraft = receipt;
    $('feedback-output').textContent = receipt.markdown;
    rememberFeedbackReceipt(receipt);
    clearFeedbackInputs();
    $('feedback-submit-status').textContent = `Recovered submitted ${receipt.request_id}; no duplicate was created.`;
    return receipt;
  } catch (error) {
    $('feedback-submit-status').textContent = 'Submission outcome unknown; fields are preserved and will be checked when the server reconnects.';
    return null;
  } finally {
    recoveringFeedbackSubmission = false;
  }
}

async function draftFeedback() {
  const payload = feedbackPayload();
  lastDraft = await api('/api/feedback/draft', { method: 'POST', body: JSON.stringify(payload) });
  $('feedback-output').textContent = lastDraft.markdown;
  $('feedback-submit-status').textContent = 'Draft updated';
  return lastDraft;
}

async function ensureDraft() {
  return await draftFeedback();
}

async function submitFeedback() {
  const missing = [];
  if (!$('fb-title').value.trim()) missing.push('Title');
  if (!$('fb-notes').value.trim()) missing.push('Notes');
  if (missing.length) {
    const requirement = missing.length === 1 ? `${missing[0]} is required` : `${missing.join(' and ')} are required`;
    $('feedback-submit-status').textContent = `${requirement} to submit feedback.`;
    return null;
  }
  $('feedback-submit-status').textContent = 'Submitting REQ...';
  if (!feedbackSubmissionId) feedbackSubmissionId = newFeedbackSubmissionId();
  const payload = { ...feedbackPayload(), submission_id: feedbackSubmissionId };
  rememberPendingFeedback(payload);
  const result = await api('/api/feedback/submit', {
    method: 'POST',
    body: JSON.stringify(payload),
  });
  lastDraft = result;
  $('feedback-output').textContent = result.markdown;
  rememberFeedbackReceipt(result);
  clearFeedbackInputs();
  $('feedback-submit-status').textContent = result.reused
    ? `Recovered submitted ${result.request_id}; no duplicate was created.`
    : `Submitted ${result.request_id}; relay this ID to the agent.`;
  await loadStatus();
  return result;
}

async function lookupMessage() {
  const id = $('message-id').value.trim();
  if (!id) return;
  const result = await api(`/api/message?id=${encodeURIComponent(id)}`);
  renderMessageDetail(result);
  return result;
}

async function loadMessages() {
  const params = new URLSearchParams();
  const query = $('message-search').value.trim();
  const status = $('message-status-filter').value.trim();
  const kind = $('message-kind-filter').value.trim();
  const feature = $('message-feature-filter').value.trim();
  if (query) params.set('q', query);
  if (status) params.set('status', status);
  if (kind) params.set('kind', kind);
  if (feature) params.set('feature', feature);
  const suffix = params.toString() ? `?${params.toString()}` : '';
  const result = await api(`/api/messages${suffix}`);
  messageRows = result.messages;
  renderMessageRows(messageRows);
  $('message-edit-status').textContent = `Showing ${result.messages.length} message(s)${describeMessageFilters(params)}`;
}

function renderMessageRows(messages) {
  const rows = sortedRows('messages', messages);
  renderSortIndicators('messages');
  $('message-body').innerHTML = rows.map((message) => `
    <tr data-message-id="${escapeHtml(message.id)}">
      <td><code>${escapeHtml(message.id)}</code></td>
      <td>${escapeHtml(formatLocalDateTime(message.created_utc))}</td>
      <td>${escapeHtml(message.status)}</td>
      <td>${escapeHtml(message.kind)}</td>
      <td>${escapeHtml(message.feature)}</td>
      <td>${escapeHtml(message.sender)}</td>
      <td>${escapeHtml((message.recipients || []).join(', '))}</td>
      <td>${escapeHtml(message.title)}</td>
    </tr>
  `).join('') || '<tr><td colspan="8" class="muted">No messages.</td></tr>';
}

function renderMessageDetail(result) {
  lastMessage = result;
  const message = (result.packet || {}).message || {};
  $('message-id').value = result.id || message.id || $('message-id').value.trim();
  if (message.id) {
    messageRows = [messageRowFromPacket(message)];
    renderMessageRows(messageRows);
    $('message-edit-status').textContent = `Exact match: ${message.id}`;
  }
  $('message-output').textContent = result.block;
  if (message.kind === 'request') {
    $('message-status-panel').style.display = 'flex';
    $('message-new-status').value = message.status === 'closed' ? 'open' : 'closed';
    $('message-status-reason').value = '';
  } else {
    $('message-status-panel').style.display = 'none';
  }
}

function messageRowFromPacket(message) {
  return {
    id: message.id || '',
    kind: message.kind || '',
    created_utc: message.created_utc || '',
    status: message.status || '',
    feature: message.feature || '',
    sender: message.sender || '',
    recipients: message.recipients || [],
    title: message.title || message.summary || '',
  };
}

function describeMessageFilters(params) {
  const entries = Array.from(params.entries());
  if (!entries.length) return '';
  return ` for ${entries.map(([key, value]) => `${key}=${value}`).join(', ')}`;
}

async function updateMessageStatus() {
  const id = $('message-id').value.trim() || (((lastMessage || {}).packet || {}).message || {}).id || '';
  const status = $('message-new-status').value;
  const reason = $('message-status-reason').value.trim();
  if (!id) return;
  if (!reason) {
    $('message-edit-status').textContent = 'Reason is required to change request status.';
    return;
  }
  $('message-edit-status').textContent = `Saving ${id} status...`;
  const result = await api('/api/message/status', {
    method: 'POST',
    body: JSON.stringify({ id, status, reason }),
  });
  if (result.message) renderMessageDetail(result.message);
  $('message-edit-status').textContent = `Saved ${id} as ${result.status}`;
  await Promise.all([loadStatus(), loadMessages()]);
}

async function loadBacklog() {
  const params = new URLSearchParams();
  const query = $('backlog-search').value.trim();
  const quickFilter = $('backlog-quick-filter').value;
  const status = $('backlog-status-filter').value.trim();
  const lane = $('backlog-lane-filter').value.trim();
  const priority = $('backlog-priority-filter').value.trim();
  const owner = $('backlog-owner-filter').value.trim();
  const itemType = $('backlog-type-filter').value.trim();
  const scope = $('backlog-scope-filter').value.trim();
  const wave = $('backlog-wave-filter').value.trim();
  if (query) params.set('q', query);
  if (quickFilter) params.set('filter', quickFilter);
  if (status) params.set('status', status);
  if (lane) params.set('lane', lane);
  if (priority) params.set('priority', priority);
  if (owner) params.set('owner', owner);
  if (itemType) params.set('type', itemType);
  if (scope) params.set('scope', scope);
  if (wave) params.set('wave', wave);
  const suffix = params.toString() ? `?${params.toString()}` : '';
  const result = await api(`/api/backlog/items${suffix}`);
  backlogRows = result.items;
  renderBacklogRows(backlogRows);
  $('backlog-edit-status').textContent = `Showing ${result.items.length} backlog item(s)${describeBacklogFilters(params)}`;
}

function renderBacklogRows(items) {
  const rows = sortedRows('backlog', items);
  renderSortIndicators('backlog');
  $('backlog-body').innerHTML = rows.map((item) => `
    <tr data-backlog-view-id="${escapeHtml(item.id)}">
      <td><code>${escapeHtml(item.id)}</code></td>
      <td>${escapeHtml(formatLocalDateTime(item.updated_utc))}</td>
      <td><input class="mini-field" data-backlog-id="${escapeHtml(item.id)}" data-backlog-field="status" value="${escapeHtml(item.status)}"></td>
      <td><input class="mini-field" data-backlog-id="${escapeHtml(item.id)}" data-backlog-field="lane" value="${escapeHtml(item.lane)}"></td>
      <td><select class="mini-field" data-backlog-id="${escapeHtml(item.id)}" data-backlog-field="priority">
        ${priorityOptions(item.priority)}
      </select></td>
      <td>${escapeHtml(item.item_type)}</td>
      <td>${escapeHtml(item.owner_hint)}</td>
      <td>${escapeHtml(item.launch_scope)}</td>
      <td>${escapeHtml(item.wave || item.release_phase || '')}</td>
      <td>${escapeHtml(item.title)}</td>
    </tr>
  `).join('') || '<tr><td colspan="10" class="muted">No backlog items.</td></tr>';
}

async function lookupBacklogItem(id) {
  const result = await api(`/api/backlog/item?id=${encodeURIComponent(id)}`);
  $('backlog-output').textContent = result.block;
  $('backlog-edit-status').textContent = `Loaded ${id}`;
  return result;
}

async function loadDecisions() {
  const params = new URLSearchParams();
  const query = $('decision-search').value.trim();
  const status = $('decision-status-filter').value.trim();
  const tier = $('decision-tier-filter').value.trim();
  if (query) params.set('q', query);
  if (status) params.set('status', status);
  if (tier) params.set('tier', tier);
  const suffix = params.toString() ? `?${params.toString()}` : '';
  const result = await api(`/api/decisions${suffix}`);
  decisionRows = result.decisions;
  renderDecisionRows(decisionRows);
}

function renderDecisionRows(decisions) {
  const rows = sortedRows('decisions', decisions);
  renderSortIndicators('decisions');
  $('decision-body').innerHTML = rows.map((decision) => `
    <tr data-decision-id="${escapeHtml(decision.id || decision.dec_ulid)}">
      <td><code>${escapeHtml(decision.id || decision.dec_ulid)}</code></td>
      <td>${escapeHtml(formatLocalDateTime(decision.display_utc || decision.status_utc || decision.proposed_utc))}</td>
      <td>${escapeHtml(decision.status)}</td>
      <td>${escapeHtml(decision.tier)}</td>
      <td>${escapeHtml(decision.owner)}</td>
      <td>${escapeHtml(decision.title)}</td>
    </tr>
  `).join('') || '<tr><td colspan="6" class="muted">No decisions.</td></tr>';
}

async function lookupDecision(id) {
  const result = await api(`/api/decision?id=${encodeURIComponent(id)}`);
  const d = result.decision;
  $('decision-output').textContent = result.block || d.block || [
    `## ${d.id || d.dec_ulid}`,
    `- status: ${d.status || ''}`,
    `- tier: ${d.tier || ''}`,
    `- owner: ${d.owner || ''}`,
    `- drift_risk: ${d.drift_risk || ''}`,
    '',
    d.title || '',
  ].join('\\n').trim() + '\\n';
}

async function loadKanban() {
  const result = await api('/api/backlog/kanban');
  const lanes = Object.entries(result.lanes).sort(([a], [b]) => a.localeCompare(b));
  $('kanban').innerHTML = lanes.map(([lane, items]) => `
    <div class="lane" data-lane="${escapeHtml(lane)}">
      <h3>${escapeHtml(lane)} <span class="muted">${items.length}</span></h3>
      ${items.map((item) => `
        <div class="card" draggable="true" data-backlog-id="${escapeHtml(item.id)}">
          <small>${escapeHtml(item.id)} ${escapeHtml(item.priority || '')}</small>
          <strong>${escapeHtml(item.title)}</strong>
          ${item.wave || item.release_phase ? `<small>Wave ${escapeHtml(item.wave || item.release_phase)}</small>` : ''}
          <small>${escapeHtml(item.status)}</small>
        </div>
      `).join('')}
    </div>
  `).join('') || '<p class="muted">No backlog lanes.</p>';
}

function priorityOptions(value) {
  const current = text(value);
  const options = ['P0', 'P1', 'P2', 'P3', ''];
  if (current && !options.includes(current)) options.unshift(current);
  return options.map((option) => `
    <option value="${escapeHtml(option)}" ${option === current ? 'selected' : ''}>${escapeHtml(option || '-')}</option>
  `).join('');
}

function describeBacklogFilters(params) {
  const entries = Array.from(params.entries());
  if (!entries.length) return '';
  return ` for ${entries.map(([key, value]) => `${key}=${value}`).join(', ')}`;
}

async function updateBacklogItem(id, field, value) {
  $('backlog-edit-status').textContent = `Saving ${id} ${field}...`;
  await api('/api/backlog/update', {
    method: 'POST',
    body: JSON.stringify({ id, [field]: value }),
  });
  $('backlog-edit-status').textContent = `Saved ${id} ${field}`;
  await Promise.all([loadStatus(), loadBacklog(), loadKanban()]);
}

async function copyText(value) {
  await navigator.clipboard.writeText(value || '');
}

let copyStartStatusTimer = null;

async function copyStartCommand() {
  const button = $('copy-start-command');
  const status = $('copy-start-status');
  window.clearTimeout(copyStartStatusTimer);
  try {
    await copyText($('start-command').textContent.trim());
    button.textContent = 'Copied \u2713';
    status.classList.remove('error');
    status.textContent = 'Command copied';
  } catch (error) {
    button.textContent = 'Copy failed';
    status.classList.add('error');
    status.textContent = `Clipboard error: ${error.message || error}`;
  }
  copyStartStatusTimer = window.setTimeout(() => {
    button.textContent = 'Copy';
    status.classList.remove('error');
    status.textContent = '';
  }, 2200);
}

async function recheckServer() {
  const status = $('recheck-server-status');
  status.classList.remove('error');
  status.textContent = 'Checking...';
  await checkServerConnection();
  const online = $('server-connection').classList.contains('online');
  if (!online && MANAGED_SERVICE) {
    status.textContent = 'Loading the latest private bookmark...';
    window.location.replace($('managed-bookmark-link').href);
    return;
  }
  status.classList.toggle('error', !online);
  status.textContent = online ? 'Connected' : 'Still reconnecting';
}

function configureLaunchPanel() {
  $('manual-launch-panel').hidden = MANAGED_SERVICE;
  $('managed-launch-panel').hidden = !MANAGED_SERVICE;
}

function switchTab(tab) {
  document.querySelectorAll('.tab-button').forEach((button) => {
    button.classList.toggle('active', button.getAttribute('data-tab') === tab);
  });
  document.querySelectorAll('.tab-panel').forEach((panel) => {
    panel.classList.toggle('active', panel.id === `tab-${tab}`);
  });
}

function clearBacklogFilterInputs() {
  [
    'backlog-search',
    'backlog-status-filter',
    'backlog-lane-filter',
    'backlog-priority-filter',
    'backlog-owner-filter',
    'backlog-type-filter',
    'backlog-scope-filter',
    'backlog-wave-filter',
  ].forEach((id) => { $(id).value = ''; });
  $('backlog-quick-filter').value = '';
}

function applyBacklogQuickFilter(filter) {
  clearBacklogFilterInputs();
  $('backlog-quick-filter').value = filter;
  switchTab('backlog');
  loadBacklog().catch((error) => {
    $('backlog-edit-status').textContent = error.message;
  });
}

function applyBacklogColumnFilter(field, value) {
  clearBacklogFilterInputs();
  const target = {
    status: 'backlog-status-filter',
    lane: 'backlog-lane-filter',
    priority: 'backlog-priority-filter',
  }[field];
  if (!target) return;
  $(target).value = value;
  switchTab('backlog');
  loadBacklog().catch((error) => {
    $('backlog-edit-status').textContent = error.message;
  });
}

function clearMessageFilterInputs() {
  $('message-search').value = '';
  $('message-status-filter').value = '';
  $('message-kind-filter').value = '';
  $('message-feature-filter').value = '';
}

function applyMessageFilter({ kind = '', feature = '', status = '' } = {}) {
  clearMessageFilterInputs();
  $('message-feature-filter').value = feature || '';
  $('message-status-filter').value = status || '';
  $('message-kind-filter').value = kind || '';
  switchTab('messages');
  loadMessages().catch((error) => {
    $('message-edit-status').textContent = error.message;
  });
}

function addScreenshotPaths(paths) {
  invalidateFeedbackSubmission();
  const current = $('fb-screenshots').value.trim();
  const next = paths.map((path) => `- ${path}`).join('\\n');
  $('fb-screenshots').value = [current, next].filter(Boolean).join('\\n');
  renderAttachmentList();
  rememberFeedbackDraft();
}

function readFileAsDataUrl(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result);
    reader.onerror = () => reject(reader.error || new Error(`Unable to read ${file.name}`));
    reader.readAsDataURL(file);
  });
}

async function uploadAttachmentFiles(fileList) {
  const files = Array.from(fileList || []);
  if (!files.length) return;
  const oversized = files.find((file) => file.size > MAX_ATTACHMENT_BYTES);
  if (oversized) {
    throw new Error(
      `${oversized.name} is ${(oversized.size / (1024 * 1024)).toFixed(1)} MB; `
      + `the per-file limit is ${MAX_ATTACHMENT_BYTES / (1024 * 1024)} MB. `
      + 'Existing attachment references were preserved.',
    );
  }
  const totalBytes = files.reduce((total, file) => total + file.size, 0);
  if (totalBytes > MAX_ATTACHMENT_TOTAL_BYTES) {
    throw new Error(
      `This batch is ${(totalBytes / (1024 * 1024)).toFixed(1)} MB; `
      + `the total limit is ${MAX_ATTACHMENT_TOTAL_BYTES / (1024 * 1024)} MB. `
      + 'Existing attachment references were preserved.',
    );
  }
  $('attachment-upload-status').textContent = `Uploading ${files.length} file(s)...`;
  const payloadFiles = await Promise.all(files.map(async (file) => ({
    name: file.name,
    data_url: await readFileAsDataUrl(file),
  })));
  const result = await api('/api/attachments/upload', {
    method: 'POST',
    body: JSON.stringify({ files: payloadFiles }),
  });
  addScreenshotPaths(result.saved || []);
  $('attachment-upload-status').textContent = `Saved ${result.saved.length} file(s) to ${result.directory}`;
}

document.querySelectorAll('.tab-button').forEach((button) => {
  button.addEventListener('click', () => switchTab(button.getAttribute('data-tab')));
});
FEEDBACK_INPUT_IDS.forEach((id) => {
  $(id).addEventListener('input', () => {
    invalidateFeedbackSubmission();
    rememberFeedbackDraft();
  });
  $(id).addEventListener('change', () => {
    invalidateFeedbackSubmission();
    rememberFeedbackDraft();
  });
});
document.querySelectorAll('[data-sort-table][data-sort-key]').forEach((header) => {
  header.addEventListener('click', () => {
    setTableSort(header.getAttribute('data-sort-table'), header.getAttribute('data-sort-key'));
  });
  header.addEventListener('keydown', (event) => {
    if (event.key !== 'Enter' && event.key !== ' ') return;
    event.preventDefault();
    setTableSort(header.getAttribute('data-sort-table'), header.getAttribute('data-sort-key'));
  });
  header.setAttribute('tabindex', '0');
  header.setAttribute('role', 'button');
});
$('tab-dashboard').addEventListener('click', (event) => {
  const metric = event.target.closest('[data-backlog-filter]');
  if (metric) {
    applyBacklogQuickFilter(metric.getAttribute('data-backlog-filter'));
    return;
  }
  const messageMetric = event.target.closest('[data-message-feature]');
  if (messageMetric) {
    applyMessageFilter({
      kind: messageMetric.getAttribute('data-message-kind') || '',
      feature: messageMetric.getAttribute('data-message-feature') || '',
      status: messageMetric.getAttribute('data-message-status') || '',
    });
    return;
  }
  const breakdown = event.target.closest('[data-backlog-field][data-backlog-value]');
  if (breakdown) {
    applyBacklogColumnFilter(
      breakdown.getAttribute('data-backlog-field'),
      breakdown.getAttribute('data-backlog-value'),
    );
  }
});
$('submit-feedback').addEventListener('click', () => submitFeedback().catch((error) => {
  $('feedback-output').textContent = error.message;
  $('feedback-submit-status').textContent = error.networkFailure
    ? 'Connection lost during submit. Retry when online; the submission receipt prevents duplicates.'
    : 'Submit failed';
}));
$('clear-feedback').addEventListener('click', () => {
  clearFeedbackInputs();
  $('feedback-output').textContent = '';
  $('feedback-submit-status').textContent = 'Feedback form cleared';
});
$('draft-feedback').addEventListener('click', () => draftFeedback().catch((error) => {
  $('feedback-output').textContent = error.message;
  $('feedback-submit-status').textContent = 'Draft failed';
}));
$('copy-feedback').addEventListener('click', () => ensureDraft().then((draft) => copyText(draft.markdown)).then(() => {
  $('feedback-submit-status').textContent = 'Copied draft';
}).catch((error) => {
  $('feedback-output').textContent = error.message;
  $('feedback-submit-status').textContent = 'Copy failed';
}));
$('copy-request').addEventListener('click', () => ensureDraft().then((draft) => copyText(draft.request.body)).then(() => {
  $('feedback-submit-status').textContent = 'Copied agent request';
}).catch((error) => {
  $('feedback-output').textContent = error.message;
  $('feedback-submit-status').textContent = 'Copy failed';
}));
$('lookup-message').addEventListener('click', () => lookupMessage().catch((error) => {
  $('message-output').textContent = error.message;
}));
$('search-messages').addEventListener('click', () => loadMessages().catch((error) => {
  $('message-edit-status').textContent = error.message;
}));
$('reload-messages').addEventListener('click', () => loadMessages().catch((error) => {
  $('message-edit-status').textContent = error.message;
}));
$('message-body').addEventListener('click', (event) => {
  const row = event.target.closest('[data-message-id]');
  if (!row) return;
  $('message-id').value = row.getAttribute('data-message-id');
  lookupMessage().catch((error) => {
    $('message-output').textContent = error.message;
  });
});
$('save-message-status').addEventListener('click', () => updateMessageStatus().catch((error) => {
  $('message-edit-status').textContent = error.message;
}));
$('search-backlog').addEventListener('click', () => loadBacklog().catch((error) => {
  $('backlog-edit-status').textContent = error.message;
}));
$('clear-backlog-filters').addEventListener('click', () => {
  clearBacklogFilterInputs();
  loadBacklog().catch((error) => {
    $('backlog-edit-status').textContent = error.message;
  });
});
$('reload-backlog').addEventListener('click', () => loadBacklog().catch(console.error));
$('search-decisions').addEventListener('click', () => loadDecisions().catch((error) => {
  $('decision-output').textContent = error.message;
}));
$('reload-decisions').addEventListener('click', () => loadDecisions().catch(console.error));
$('decision-body').addEventListener('click', (event) => {
  const row = event.target.closest('[data-decision-id]');
  if (!row) return;
  lookupDecision(row.getAttribute('data-decision-id')).catch((error) => {
    $('decision-output').textContent = error.message;
  });
});
$('reload-kanban').addEventListener('click', () => loadKanban().catch(console.error));
$('copy-start-command').addEventListener('click', copyStartCommand);
$('recheck-server').addEventListener('click', () => {
  recheckServer().catch((error) => {
    $('recheck-server-status').classList.add('error');
    $('recheck-server-status').textContent = error.message || String(error);
  });
});
$('repo-selector').addEventListener('change', (event) => {
  switchRepo(event.target.value).catch((error) => {
    $('status').innerHTML = `<span class="badge">repo switch error ${escapeHtml(error.message)}</span>`;
  });
});
$('pick-attachments').addEventListener('click', () => $('attachment-picker').click());
$('attachment-picker').addEventListener('change', (event) => {
  uploadAttachmentFiles(event.target.files).catch((error) => {
    $('attachment-upload-status').textContent = error.message;
  });
  event.target.value = '';
});
$('clear-attachments').addEventListener('click', () => {
  invalidateFeedbackSubmission();
  $('fb-screenshots').value = '';
  renderAttachmentList();
  rememberFeedbackDraft();
  $('attachment-upload-status').textContent = ATTACHMENT_STATUS_DEFAULT;
});
$('attachment-dropzone').addEventListener('dragover', (event) => {
  event.preventDefault();
  $('attachment-dropzone').classList.add('dragover');
});
$('attachment-dropzone').addEventListener('dragleave', () => {
  $('attachment-dropzone').classList.remove('dragover');
});
$('attachment-dropzone').addEventListener('drop', (event) => {
  event.preventDefault();
  $('attachment-dropzone').classList.remove('dragover');
  uploadAttachmentFiles(event.dataTransfer.files).catch((error) => {
    $('attachment-upload-status').textContent = error.message;
  });
});
$('backlog-body').addEventListener('change', (event) => {
  const target = event.target.closest('[data-backlog-id][data-backlog-field]');
  if (!target) return;
  updateBacklogItem(
    target.getAttribute('data-backlog-id'),
    target.getAttribute('data-backlog-field'),
    target.value,
  ).catch((error) => {
    $('backlog-edit-status').textContent = error.message;
  });
});
$('backlog-body').addEventListener('click', (event) => {
  if (event.target.closest('[data-backlog-id][data-backlog-field]')) return;
  const row = event.target.closest('[data-backlog-view-id]');
  if (!row) return;
  lookupBacklogItem(row.getAttribute('data-backlog-view-id')).catch((error) => {
    $('backlog-output').textContent = error.message;
    $('backlog-edit-status').textContent = 'Lookup failed';
  });
});
$('kanban').addEventListener('dragstart', (event) => {
  const card = event.target.closest('[data-backlog-id]');
  if (!card) return;
  draggedBacklogId = card.getAttribute('data-backlog-id');
  event.dataTransfer.effectAllowed = 'move';
  event.dataTransfer.setData('text/plain', draggedBacklogId);
});
$('kanban').addEventListener('dragend', () => {
  draggedBacklogId = null;
  document.querySelectorAll('.lane.dragover').forEach((lane) => lane.classList.remove('dragover'));
});
$('kanban').addEventListener('dragover', (event) => {
  const lane = event.target.closest('[data-lane]');
  if (!lane) return;
  event.preventDefault();
  lane.classList.add('dragover');
  event.dataTransfer.dropEffect = 'move';
});
$('kanban').addEventListener('dragleave', (event) => {
  const lane = event.target.closest('[data-lane]');
  if (lane && !lane.contains(event.relatedTarget)) lane.classList.remove('dragover');
});
$('kanban').addEventListener('drop', (event) => {
  const lane = event.target.closest('[data-lane]');
  if (!lane) return;
  event.preventDefault();
  lane.classList.remove('dragover');
  const itemId = draggedBacklogId || event.dataTransfer.getData('text/plain');
  const newLane = lane.getAttribute('data-lane');
  if (!itemId || !newLane) return;
  updateBacklogItem(itemId, 'lane', newLane).catch((error) => {
    $('backlog-edit-status').textContent = error.message;
  });
});

configureLaunchPanel();
setServerConnection('checking');
restoreFeedbackDraft();
checkServerConnection();
setInterval(checkServerConnection, 5000);
window.addEventListener('focus', checkServerConnection);
loadProjects().then(() => {
  resetRepoView();
  restoreFeedbackDraft();
  return Promise.all([
    loadStatus(),
    loadMessages(),
    loadBacklog(),
    loadDecisions(),
    loadKanban(),
  ]);
}).then(() => recoverPendingFeedbackSubmission()).catch((error) => {
  $('status').innerHTML = `<span class="badge">startup error ${escapeHtml(error.message)}</span>`;
});
</script>
</body>
</html>
"""
