"""Canonical JSON and event-line hashing utilities."""
from __future__ import annotations

import hashlib
import json
from typing import Any

SENTINEL_PREV_HASH = "0" * 64


def canonical_json(obj: Any) -> bytes:
    """Return deterministic UTF-8 JSON bytes for an event envelope."""
    return json.dumps(
        obj,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def hash_event_line(line_bytes: bytes) -> str:
    """Hash the exact newline-terminated bytes stored in events.jsonl."""
    return hashlib.sha256(line_bytes).hexdigest()
