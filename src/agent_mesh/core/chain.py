"""Hash-chain integrity verification for ``events.jsonl`` (live-gate C, slice 1).

Two modes over one walker:

* FULL walk (``anchor=None``): verify the whole log from the sentinel. This is the CLI / periodic
  audit path and the authoritative check (it also catches an arbitrary same-length rewrite of an
  earlier line, which the anchored mode does not).
* ANCHORED (incremental) walk: given a ``ChainAnchor`` captured BEFORE a batch of appends, verify
  only that the appended suffix links onto the known-good tail and continues the chain. Cost is
  O(bytes appended), not O(whole log) -- the right shape for an apply-time / post-append gate.

What anchored verification proves (and does not): it proves suffix continuity from a known-good
tail and catches tail rewrite, truncation, a torn boundary, and a corrupt appended suffix. It does
NOT prove that an arbitrary earlier prefix line was not rewritten in place at the same byte length;
that requires the full walk (kept as the audit backstop). This is acceptable because event-backed
writers are append-only through ``append_event``.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from agent_mesh.core.events import read_tail_line
from agent_mesh.core.hashing import SENTINEL_PREV_HASH, hash_event_line


@dataclass(frozen=True)
class ChainAnchor:
    """A known-good tail of ``events.jsonl`` captured before a batch of appends.

    ``size`` is the exact append boundary (byte length at capture); ``last_seq`` / ``last_hash`` are
    the tail event's seq and line hash (``0`` / ``SENTINEL_PREV_HASH`` for an empty log).
    """

    size: int
    last_seq: int
    last_hash: str


@dataclass(frozen=True)
class ChainResult:
    """Outcome of a chain verification. ``verified`` is the number of events checked (the suffix
    count when anchored; the full count otherwise)."""

    ok: bool
    verified: int
    error: str | None = None
    error_line: int | None = None


def capture_anchor(events_path: str | Path) -> ChainAnchor:
    """Capture the current tail of ``events_path`` as a ``ChainAnchor``.

    A missing or empty log yields the empty anchor (``size=0``, ``last_seq=0``,
    ``last_hash=SENTINEL_PREV_HASH``), so a first-ever append is verified from offset 0.
    """
    path = Path(events_path)
    try:
        size = path.stat().st_size
    except FileNotFoundError:
        return ChainAnchor(size=0, last_seq=0, last_hash=SENTINEL_PREV_HASH)
    if size == 0:
        return ChainAnchor(size=0, last_seq=0, last_hash=SENTINEL_PREV_HASH)
    tail_line, tail_hash = read_tail_line(path)
    last_seq = 0
    if tail_line:
        try:
            last_seq = int(json.loads(tail_line)["event_seq"])
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            # A corrupt tail at capture: leave last_seq=0; the anchored verify still catches a
            # prev_event_hash divergence on the first appended record.
            last_seq = 0
    return ChainAnchor(size=size, last_seq=last_seq, last_hash=tail_hash)


def verify_chain(events_path: str | Path, *, anchor: ChainAnchor | None = None) -> ChainResult:
    """Verify the hash chain of ``events_path``.

    With ``anchor=None`` walks the whole log from the sentinel. With an ``anchor`` verifies only the
    suffix appended since that anchor (and that nothing at/after the tail was rewritten). Returns a
    ``ChainResult``; never raises for an ordinary integrity failure (it is reported in ``error``).
    """
    path = Path(events_path)
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return ChainResult(False, 0, f"events log not found: {path}", None)
    if anchor is None:
        return _verify_suffix(data, 0, SENTINEL_PREV_HASH, 1)
    return _verify_from_anchor(data, anchor)


def _verify_from_anchor(data: bytes, anchor: ChainAnchor) -> ChainResult:
    now_size = len(data)
    if now_size < anchor.size:
        return ChainResult(
            False, 0, f"events log shrank below anchor: {now_size} < {anchor.size}", None
        )
    if now_size == anchor.size:
        # No appends since the anchor.
        if anchor.size == 0:
            return ChainResult(True, 0)  # empty + newline-neutral
        if not data.endswith(b"\n"):
            return ChainResult(False, 0, "events log is not newline-terminated", None)
        tail_hash = hash_event_line(data.splitlines(keepends=True)[-1])
        if tail_hash != anchor.last_hash:
            return ChainResult(
                False, 0, f"tail rewritten since anchor (expected {anchor.last_hash}, got {tail_hash})", None
            )
        return ChainResult(True, 0)
    # now_size > anchor.size: there is an appended suffix.
    if anchor.size == 0:
        return _verify_suffix(data, 0, SENTINEL_PREV_HASH, 1)
    if data[anchor.size - 1 : anchor.size] != b"\n":
        return ChainResult(
            False, 0, f"anchor boundary at byte {anchor.size} is not a line boundary", None
        )
    return _verify_suffix(data, anchor.size, anchor.last_hash, anchor.last_seq + 1)


def _verify_suffix(data: bytes, start_offset: int, prev_hash: str, start_seq: int) -> ChainResult:
    """Walk ``data[start_offset:]`` requiring its first record to chain onto ``prev_hash`` /
    ``start_seq`` and each subsequent record to continue the chain. ``start_offset`` MUST be a line
    boundary in ``data`` (the caller guarantees this)."""
    suffix = data[start_offset:]
    if suffix and not suffix.endswith(b"\n"):
        return ChainResult(False, 0, "events log is not newline-terminated", None)
    lines_before = data[:start_offset].count(b"\n")
    previous_hash = prev_hash
    expected_seq = start_seq
    verified = 0
    for index, line in enumerate(suffix.splitlines(keepends=True)):
        line_number = lines_before + index + 1
        error = _check_line(line, line_number, previous_hash, expected_seq)
        if error is not None:
            return ChainResult(False, verified, error, line_number)
        previous_hash = hash_event_line(line)
        expected_seq += 1
        verified += 1
    return ChainResult(True, verified)


def _check_line(line: bytes, line_number: int, previous_hash: str, expected_seq: int) -> str | None:
    try:
        record = json.loads(line)
    except json.JSONDecodeError as exc:
        return f"line {line_number} invalid JSON: {exc.msg}"
    actual_prev = record.get("prev_event_hash")
    if actual_prev != previous_hash:
        return (
            f"line {line_number} prev_event_hash mismatch "
            f"(expected {previous_hash}, got {actual_prev})"
        )
    actual_seq = record.get("event_seq")
    if actual_seq != expected_seq:
        return (
            f"line {line_number} event_seq mismatch (expected {expected_seq}, got {actual_seq})"
        )
    return None
