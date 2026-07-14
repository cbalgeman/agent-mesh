"""Default adapter implementations for the mail domain."""
from __future__ import annotations

import re
from pathlib import Path

from agent_mesh.adapters.base import ExtractedRef, LookupAdapter, MessageBlock, RefExtractionAdapter
from agent_mesh.config import config_from_agent_dir
from agent_mesh.store.rebuild import rebuild_all
from agent_mesh.store.sqlite import body_from_message_row, connect, initialize_schema, resolve_message
from agent_mesh.views import locate_message, render_all

REF_RE = re.compile(
    r"(?<![A-Za-z0-9_])("
    r"D\d+(?:-(?:[SB]\d+|[A-Z]))?(?:-§[A-Za-z0-9._-]+)?|"
    r"REQ-\d{8}T\d{6}Z-[A-Z0-9_-]+-\d{5}|"
    r"RES-\d{8}T\d{6}Z-[A-Z0-9_-]+-\d{5}|"
    r"FBK-[A-Za-z0-9][\w-]*|DI-[A-Za-z0-9][\w-]*|J-[A-Za-z0-9][\w-]*|"
    r"BKL-[A-Za-z0-9][\w-]*|IMP-[A-Za-z0-9][\w-]*"
    r")(?![A-Za-z0-9_])"
)


class DefaultMessageLookupAdapter(LookupAdapter):
    """Structured lookup for v1.0 message blocks."""

    def lookup(self, identifier: str) -> MessageBlock | None:
        agent_dir = self.spec.options.get("agent_dir")
        start = Path(str(agent_dir)) if agent_dir else None
        config = config_from_agent_dir(start or Path.cwd())
        rebuild_all(config)
        render_all(config)
        conn = connect(config.db_path)
        try:
            initialize_schema(conn)
            row = resolve_message(conn, identifier)
            if row is None:
                return None
            found = locate_message(config, identifier)
            source_path = str(found[0]) if found else None
            line_start = found[1] if found else None
            line_end = found[2] if found else None
            return MessageBlock(
                message_id=identifier,
                kind=str(row["kind"]),
                thread_id=str(row["thread_id"]),
                body=body_from_message_row(row),
                body_sha=str(row["body_sha"]),
                source_uri=f"agent-mesh://message/{identifier}",
                source_path=source_path,
                line_start=line_start,
                line_end=line_end,
            )
        finally:
            conn.close()


class DefaultRefExtractionAdapter(RefExtractionAdapter):
    """Regex-based extraction for the standard agent-mesh reference forms."""

    def extract(self, text: str, *, source_field: str = "body") -> list[ExtractedRef]:
        refs: list[ExtractedRef] = []
        for match in REF_RE.finditer(text):
            value = match.group(1)
            refs.append(
                ExtractedRef(
                    ref_type=_ref_type(value),
                    ref_value=value,
                    source_field=source_field,
                    privacy_class=self.privacy_class,
                )
            )
        return refs


def _ref_type(value: str) -> str:
    if value.startswith("D"):
        return "decision"
    if value.startswith("REQ-"):
        return "request"
    if value.startswith("RES-"):
        return "response"
    if value.startswith("FBK-"):
        return "feedback"
    if value.startswith("DI-"):
        return "design_intent"
    if value.startswith("J-"):
        return "journey"
    if value.startswith("BKL-"):
        return "workitem"
    if value.startswith("IMP-"):
        return "improvement"
    return "unknown"
