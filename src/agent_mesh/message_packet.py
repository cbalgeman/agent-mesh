"""Thread-scoped message packets for agent-mesh readers."""

from __future__ import annotations

import json
import sqlite3
from typing import Any

from agent_mesh.store.sqlite import body_from_message_row, json_loads


def build_message_packet(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
    *,
    max_body_chars: int = 20_000,
    max_thread_body_chars: int = 2_000,
    max_thread_messages: int = 12,
    warn_tokens: int = 12_000,
) -> dict[str, Any]:
    """Build a bounded, thread-scoped packet for automated grounding."""
    refs = _message_refs(conn, row["id"])
    body, body_truncated = _bounded_text(body_from_message_row(row), max_body_chars)
    thread_rows = conn.execute(
        "SELECT * FROM messages WHERE thread_id=? ORDER BY created_utc ASC, event_seq ASC",
        (row["thread_id"],),
    ).fetchall()
    thread: list[dict[str, Any]] = []
    truncated_thread = False
    if len(thread_rows) > max_thread_messages:
        truncated_thread = True
        thread_rows = thread_rows[-max_thread_messages:]
    for item in thread_rows:
        item_body, item_truncated = _bounded_text(
            body_from_message_row(item), max_thread_body_chars
        )
        thread.append(
            {
                "id": item["id"],
                "kind": item["kind"],
                "created_utc": item["created_utc"],
                "event_seq": item["event_seq"],
                "sender": public_message_sender(
                    conn,
                    str(item["sender"]),
                    str(item["sender_instance_id"] or ""),
                ),
                "parent_id": item["parent_id"],
                "request_id": item["request_id"],
                "title": item["title"],
                "summary": item["summary"],
                "body": item_body,
                "body_truncated": item_truncated,
                "body_bytes": item["body_bytes"],
                "body_fidelity": item["body_fidelity"],
                "body_authority": item["body_authority"],
                "status": item["status"],
            }
        )
    packet: dict[str, Any] = {
        "packet_version": 1,
        "message": {
            "id": row["id"],
            "kind": row["kind"],
            "thread_id": row["thread_id"],
            "request_id": row["request_id"],
            "parent_id": row["parent_id"],
            "sender": public_message_sender(
                conn,
                str(row["sender"]),
                str(row["sender_instance_id"] or ""),
            ),
            "recipients": public_message_recipients(
                conn,
                json_loads(row["recipients_json"], []),
                json_loads(row["recipient_instance_ids_json"], []),
            ),
            "feature": row["feature_id"],
            "workflow_origin": row["workflow_origin"],
            "workflow_origin_valid": bool(row["workflow_origin_valid"]),
            "workflow_origin_source": row["workflow_origin_source"],
            "title": row["title"],
            "summary": row["summary"],
            "status": row["status"],
            "resolution": row["resolution"],
            "created_utc": row["created_utc"],
            "updated_utc": row["updated_utc"],
            "event_seq": row["event_seq"],
            "body": body,
            "body_truncated": body_truncated,
            "body_bytes": row["body_bytes"],
            "body_fidelity": row["body_fidelity"],
            "body_authority": row["body_authority"],
            "refs": refs,
        },
        "thread": {
            "thread_id": row["thread_id"],
            "message_count": len(thread),
            "truncated": truncated_thread,
            "messages": thread,
        },
        "grounding": {
            "default_scope": "thread",
            "projection_files_required": False,
            "notes": [
                "Use this packet/body/thread data for automated dispatch grounding.",
                "Do not read generated inbox/outbox projections for headless grounding.",
            ],
        },
    }
    payload = json.dumps(packet, sort_keys=True)
    packet["size"] = {
        "chars": len(payload),
        "est_tokens": len(payload) // 4,
        "warn_tokens": warn_tokens,
    }
    return packet


def _bounded_text(value: str, max_chars: int) -> tuple[str, bool]:
    if max_chars < 0:
        max_chars = 0
    if len(value) <= max_chars:
        return value, False
    return value[:max_chars], True


def _message_refs(conn: sqlite3.Connection, message_id: str) -> list[dict[str, str]]:
    rows = conn.execute(
        "SELECT ref_type, ref_value FROM message_refs WHERE message_id=? ORDER BY ref_type, ref_value",
        (message_id,),
    ).fetchall()
    return [{"type": row["ref_type"], "value": row["ref_value"]} for row in rows]


def public_instance_handle_for_id(conn: sqlite3.Connection, identifier: str) -> str:
    """Resolve an internal instance ID to its sole normal public handle."""

    if not identifier:
        return ""
    row = conn.execute(
        "SELECT label FROM agent_instances WHERE id=?", (identifier,)
    ).fetchone()
    return str(row["label"]) if row is not None else "[unknown-instance]"


def public_message_sender(
    conn: sqlite3.Connection, sender: str, sender_instance_id: str
) -> str:
    """Render one sender identity, preferring its instance handle when present."""

    handle = public_instance_handle_for_id(conn, sender_instance_id)
    return handle or sender


def public_message_recipients(
    conn: sqlite3.Connection,
    recipients: list[Any],
    recipient_instance_ids: list[Any],
) -> list[str]:
    """Render generic participants and instance-addressed handles without duplication."""

    instance_rows: list[sqlite3.Row] = []
    for identifier in recipient_instance_ids:
        row = conn.execute(
            "SELECT participant, label FROM agent_instances WHERE id=?",
            (str(identifier),),
        ).fetchone()
        if row is not None:
            instance_rows.append(row)
    targeted_participants = {str(row["participant"]) for row in instance_rows}
    rendered = [
        str(recipient)
        for recipient in recipients
        if str(recipient) not in targeted_participants
    ]
    rendered.extend(str(row["label"]) for row in instance_rows)
    return list(dict.fromkeys(rendered))
