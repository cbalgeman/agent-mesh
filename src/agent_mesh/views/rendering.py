"""Markdown view rendering for projected agent-mesh data."""
from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass
from pathlib import Path

from agent_mesh.config import AgentMeshConfig, ensure_project_dirs
from agent_mesh.store.sqlite import body_from_message_row, connect, initialize_schema, json_loads


@dataclass(frozen=True)
class RenderedView:
    target: Path
    sha256_before: str
    sha256_after: str


def render_all(config: AgentMeshConfig) -> list[RenderedView]:
    ensure_project_dirs(config)
    rendered: list[RenderedView] = []
    rendered.append(render_inbox(config))
    rendered.extend(render_outboxes(config))
    rendered.append(render_log(config))
    rendered.extend(render_archive(config))
    rendered.extend(render_decisions(config))
    return rendered


def render_inbox(config: AgentMeshConfig) -> RenderedView:
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        open_rows = conn.execute(
            "SELECT * FROM messages WHERE kind='request' AND status='open' "
            "ORDER BY created_utc DESC, event_seq DESC"
        ).fetchall()
        closed_rows = conn.execute(
            "SELECT * FROM messages WHERE kind='request' AND status!='open' "
            "ORDER BY COALESCE(resolved_utc, updated_utc) DESC, event_seq DESC LIMIT 100"
        ).fetchall()
        parts = ["# Inbox", "", "## Open Requests", ""]
        if open_rows:
            for row in open_rows:
                parts.extend(_request_block(row))
        else:
            parts.extend(["_No open requests._", ""])
        parts.extend(["## Recent Closed Requests", ""])
        if closed_rows:
            for row in closed_rows:
                parts.extend(_request_block(row))
        else:
            parts.extend(["_No recent closed requests._", ""])
        text = "\n".join(parts).rstrip() + "\n"
        targets = [config.views_dir / "inbox.md"]
        if config.compatibility_views.inbox:
            targets.append(config.compatibility_views.inbox)
        return _write_multi(targets, text)
    finally:
        conn.close()


def render_outboxes(config: AgentMeshConfig) -> list[RenderedView]:
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        historical_senders = [
            row["sender"]
            for row in conn.execute(
                "SELECT DISTINCT sender FROM messages WHERE kind='response' ORDER BY sender"
            )
        ]
        senders = sorted(set(config.participants).union(historical_senders))
        rendered = []
        for sender in senders:
            rows = conn.execute(
                "SELECT * FROM messages WHERE kind='response' AND sender=? "
                "ORDER BY created_utc DESC, event_seq DESC",
                (sender,),
            ).fetchall()
            parts = [f"# Outbox: {sender}", ""]
            for row in rows:
                parts.extend(_response_block(row))
            if not rows:
                parts.extend(["_No responses._", ""])
            text = "\n".join(parts).rstrip() + "\n"
            targets = [config.views_dir / f"outbox-{sender}.md"]
            compat = config.compatibility_views.outbox.get(sender)
            if compat:
                targets.append(compat)
            rendered.append(_write_multi(targets, text))
        return rendered
    finally:
        conn.close()


def render_log(config: AgentMeshConfig) -> RenderedView:
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        rows = conn.execute(
            "SELECT id, kind, sender, thread_id, status, created_utc, event_seq "
            "FROM messages ORDER BY event_seq"
        ).fetchall()
        parts = ["# Message Log", ""]
        for row in rows:
            parts.append(
                f"- {row['event_seq']}: {row['id']} ({row['kind']}) "
                f"from={row['sender']} thread={row['thread_id']} status={row['status']} "
                f"created={row['created_utc']}"
            )
        if not rows:
            parts.append("_No messages._")
        text = "\n".join(parts).rstrip() + "\n"
        targets = [config.views_dir / "message-log.md"]
        if config.compatibility_views.message_log:
            targets.append(config.compatibility_views.message_log)
        return _write_multi(targets, text)
    finally:
        conn.close()


def render_archive(config: AgentMeshConfig) -> list[RenderedView]:
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        rows = conn.execute(
            "SELECT * FROM messages WHERE kind='request' AND status!='open' "
            "ORDER BY created_utc DESC, event_seq DESC"
        ).fetchall()
        archive_dir = config.compatibility_views.archive_dir or config.archive_dir
        target = archive_dir / "inbox-all.md"
        parts = ["# Archive: Inbox", ""]
        for row in rows:
            parts.extend(_request_block(row))
        if not rows:
            parts.extend(["_No archived requests._", ""])
        return [_write_one(target, "\n".join(parts).rstrip() + "\n")]
    finally:
        conn.close()


def render_decisions(config: AgentMeshConfig) -> list[RenderedView]:
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        rows = conn.execute("SELECT * FROM decisions ORDER BY human_id").fetchall()
        rendered = []
        decisions_dir = config.views_dir / "decisions"
        for row in rows:
            meta = json_loads(row["meta_json"], {})
            parts = [
                f"# {row['human_id']} — {row['title']}",
                "",
                f"- dec_ulid: {row['dec_ulid']}",
                f"- tier: {row['tier']}",
                f"- status: {row['status']}",
                f"- enforcement_mode: {row['enforcement_mode']}",
                f"- owner: {row['owner'] or ''}",
                "",
                "## Context",
                str(meta.get("context", "")),
                "",
                "## Decision",
                _decision_body(config, row) or str(meta.get("decision", "")),
                "",
            ]
            rendered.append(_write_one(decisions_dir / f"{row['human_id']}.md", "\n".join(parts)))
        return rendered
    finally:
        conn.close()


def locate_message(config: AgentMeshConfig, message_id: str) -> tuple[Path, int, int] | None:
    candidates = [
        config.views_dir / "inbox.md",
        config.views_dir / "message-log.md",
        config.archive_dir / "inbox-all.md",
    ]
    candidates.extend(config.views_dir.glob("outbox-*.md"))
    if config.compatibility_views.inbox:
        candidates.append(config.compatibility_views.inbox)
    if config.compatibility_views.message_log:
        candidates.append(config.compatibility_views.message_log)
    candidates.extend(config.compatibility_views.outbox.values())
    if config.compatibility_views.archive_dir:
        candidates.append(config.compatibility_views.archive_dir / "inbox-all.md")

    for path in candidates:
        found = _find_heading(path, message_id)
        if found:
            return found
    return None


def _request_block(row) -> list[str]:
    body = body_from_message_row(row)
    meta = json_loads(row["meta_json"], {})
    to_value = meta.get("original_to") or ", ".join(json_loads(row["recipients_json"], []))
    return _projection_marker(row, meta) + [
        f"### {row['id']}",
        f"- created_utc: {row['created_utc']}",
        f"- from: {row['sender']}",
        f"- to: {to_value}",
        f"- feature: {row['feature_id']}",
        f"- status: {row['status']}",
        f"- title: {row['title']}",
        "",
        "#### Message",
        body,
        "",
        "#### Resolution",
        f"- {row['resolution'] or 'pending'}",
        "",
    ]


def _response_block(row) -> list[str]:
    body = body_from_message_row(row)
    meta = json_loads(row["meta_json"], {})
    return _projection_marker(row, meta) + [
        f"### {row['id']}",
        f"- created_utc: {row['created_utc']}",
        f"- from: {row['sender']}",
        f"- request_id: {row['request_id']}",
        f"- summary: {row['summary']}",
        "",
        "#### Details",
        body,
        "",
    ]


def _projection_marker(row, meta: dict) -> list[str]:
    marker: dict[str, object] = {"canonical_event": row["id"]}
    source_refs = meta.get("source_context_refs")
    if isinstance(source_refs, list) and source_refs:
        first_ref = source_refs[0]
        if isinstance(first_ref, dict) and first_ref.get("channel"):
            marker["source_channel"] = first_ref["channel"]
    if meta.get("source_context_status"):
        marker["source_context_status"] = meta["source_context_status"]
    if meta.get("body_authority"):
        marker["body_authority"] = meta["body_authority"]
    if meta.get("body_fidelity"):
        marker["body_fidelity"] = meta["body_fidelity"]

    if len(marker) == 1:
        return []
    if meta.get("parent_id"):
        marker["parent_id"] = meta["parent_id"]
    fields = " ".join(f"{key}={_marker_value(value)}" for key, value in marker.items())
    return [f"<!-- agent-mesh: projection {fields} -->"]


def _marker_value(value: object) -> str:
    text = re.sub(r"\s+", "_", str(value).strip())
    return text.replace("--", "-_").replace(">", "&gt;")


def _decision_body(config: AgentMeshConfig, row) -> str:
    body_path = row["body_path"]
    if not body_path:
        return ""
    path = config.agent_dir / body_path
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return ""


def _write_multi(targets: list[Path], text: str) -> RenderedView:
    first_result: RenderedView | None = None
    for target in targets:
        result = _write_one(target, text)
        if first_result is None:
            first_result = result
    assert first_result is not None
    return first_result


def _write_one(target: Path, text: str) -> RenderedView:
    before = _sha256_path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_name(f"{target.name}.tmp")
    tmp.write_text(text, encoding="utf-8")
    _fsync_file(tmp)
    os.replace(tmp, target)
    _fsync_dir(target.parent)
    after = _sha256_path(target)
    return RenderedView(target=target, sha256_before=before, sha256_after=after)


def _sha256_path(path: Path) -> str:
    if not path.exists():
        return hashlib.sha256(b"").hexdigest()
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _find_heading(path: Path, message_id: str) -> tuple[Path, int, int] | None:
    if not path.exists():
        return None
    lines = path.read_text(encoding="utf-8").splitlines()
    start = None
    for index, line in enumerate(lines, start=1):
        if line.strip() == f"### {message_id}":
            start = index
            break
    if start is None:
        return None
    end = len(lines)
    for index in range(start + 1, len(lines) + 1):
        if lines[index - 1].startswith("### "):
            end = index - 1
            break
    return path, start, end


def _fsync_file(path: Path) -> None:
    with path.open("rb") as handle:
        os.fsync(handle.fileno())


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
