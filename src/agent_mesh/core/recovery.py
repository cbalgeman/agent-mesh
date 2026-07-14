"""Crash recovery for the events.jsonl intent/commit journal."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from agent_mesh.core.hashing import SENTINEL_PREV_HASH, hash_event_line

EVENT_LOG_PARTIAL_CORRUPTION = "EVENT_LOG_PARTIAL_CORRUPTION"
EVENT_LOG_TAIL_MISMATCH_AT_COMMIT = "EVENT_LOG_TAIL_MISMATCH_AT_COMMIT"
EVENT_LOG_PRE_APPEND_INTEGRITY_FAILED = "EVENT_LOG_PRE_APPEND_INTEGRITY_FAILED"
EVENT_LOG_FILE_SHRANK = "EVENT_LOG_FILE_SHRANK"
EVENT_LOG_JOURNAL_INVARIANT_VIOLATED = "EVENT_LOG_JOURNAL_INVARIANT_VIOLATED"

STOP_LINE_CODES = {
    EVENT_LOG_PARTIAL_CORRUPTION,
    EVENT_LOG_TAIL_MISMATCH_AT_COMMIT,
    EVENT_LOG_PRE_APPEND_INTEGRITY_FAILED,
    EVENT_LOG_FILE_SHRANK,
    EVENT_LOG_JOURNAL_INVARIANT_VIOLATED,
}


class RecoveryStopLine(RuntimeError):
    """Raised when recovery reaches a stable operator-required STOP-LINE."""

    def __init__(self, code: str, message: str | None = None) -> None:
        self.code = code
        super().__init__(message or code)


@dataclass
class RecoveryReport:
    """Recovery actions taken during one scan."""

    auto_recovered: int = 0
    discarded_intents: int = 0
    promoted_intents: int = 0
    replayed_committed: int = 0
    appended_from_partial: int = 0
    truncated_bytes: int = 0
    stop_lines: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class Journal:
    """Parsed journal metadata."""

    path: Path
    suffix: str
    event_id: str
    event_seq: int
    event_hash: str
    prev_event_hash: str | None
    line_bytes: int | None
    size_before: int | None


def recover(events_path: str | Path, journal_dir: str | Path) -> RecoveryReport:
    """Recover all leftover event journals, or raise a STOP-LINE exception."""
    path = Path(events_path)
    directory = Path(journal_dir)
    report = RecoveryReport()
    if not directory.exists():
        return report

    groups = _journal_groups(directory)
    for event_id, suffixes in sorted(groups.items()):
        if "intent" in suffixes and "committed" in suffixes:
            _stop(report, EVENT_LOG_JOURNAL_INVARIANT_VIOLATED, event_id)

    for event_id, suffixes in sorted(groups.items()):
        if "committed" in suffixes:
            journal = _parse_journal(suffixes["committed"], "committed")
            if journal is None:
                _stop(report, EVENT_LOG_JOURNAL_INVARIANT_VIOLATED, event_id)
                raise AssertionError("unreachable")
            _recover_committed(path, journal, report)
        elif "intent" in suffixes:
            journal = _parse_journal(suffixes["intent"], "intent")
            if journal is None:
                _discard_intent(suffixes["intent"], report)
                continue
            _recover_intent(path, journal, report)
    return report


def _recover_committed(events_path: Path, journal: Journal, report: RecoveryReport) -> None:
    tail_line = _read_tail_fragment(events_path)
    if hash_event_line(tail_line) != journal.event_hash:
        _stop(report, EVENT_LOG_TAIL_MISMATCH_AT_COMMIT, journal.event_id)

    _replay_event_placeholder(events_path, tail_line)
    _cleanup_partial(events_path, journal.event_id)
    journal.path.unlink()
    _fsync_dir(journal.path.parent)
    report.auto_recovered += 1
    report.replayed_committed += 1


def _recover_intent(events_path: Path, journal: Journal, report: RecoveryReport) -> None:
    if journal.line_bytes is None or journal.size_before is None or journal.prev_event_hash is None:
        _discard_intent(journal.path, report)
        return

    current_size = _get_size(events_path)
    tail_line = _read_tail_fragment(events_path)
    tail_hash = hash_event_line(tail_line)
    expected_size = journal.size_before + journal.line_bytes

    if tail_hash == journal.event_hash and current_size == expected_size:
        committed = _promote(journal, report)
        _recover_committed(events_path, committed, report)
        return

    if current_size == journal.size_before:
        partial = _partial_path(events_path, journal.event_id)
        if partial.exists():
            if not _partial_valid(partial, journal):
                _stop(report, EVENT_LOG_PARTIAL_CORRUPTION, journal.event_id)
            _append_from_partial(events_path, partial, journal, report)
            return
        _discard_intent(journal.path, report)
        return

    if current_size > journal.size_before:
        prefix_tail_hash = _tail_hash_for_prefix(events_path, journal.size_before)
        if prefix_tail_hash != journal.prev_event_hash:
            _stop(report, EVENT_LOG_PRE_APPEND_INTEGRITY_FAILED, journal.event_id)

        truncated = current_size - journal.size_before
        _truncate(events_path, journal.size_before)
        report.truncated_bytes += truncated
        if _tail_hash_for_prefix(events_path, journal.size_before) != journal.prev_event_hash:
            _stop(report, EVENT_LOG_PRE_APPEND_INTEGRITY_FAILED, journal.event_id)

        partial = _partial_path(events_path, journal.event_id)
        if not partial.exists() or not _partial_valid(partial, journal):
            _stop(report, EVENT_LOG_PARTIAL_CORRUPTION, journal.event_id)
        _append_from_partial(events_path, partial, journal, report)
        return

    _stop(report, EVENT_LOG_FILE_SHRANK, journal.event_id)


def _append_from_partial(
    events_path: Path, partial: Path, journal: Journal, report: RecoveryReport
) -> None:
    line = partial.read_bytes()
    _append_bytes(events_path, line)
    tail_line = _read_tail_fragment(events_path)
    if hash_event_line(tail_line) != journal.event_hash:
        _stop(report, EVENT_LOG_PARTIAL_CORRUPTION, journal.event_id)
    committed = _promote(journal, report)
    _recover_committed(events_path, committed, report)
    report.appended_from_partial += 1


def _promote(journal: Journal, report: RecoveryReport) -> Journal:
    committed_path = journal.path.with_suffix(".committed")
    os.replace(journal.path, committed_path)
    _fsync_dir(journal.path.parent)
    report.promoted_intents += 1
    return Journal(
        path=committed_path,
        suffix="committed",
        event_id=journal.event_id,
        event_seq=journal.event_seq,
        event_hash=journal.event_hash,
        prev_event_hash=journal.prev_event_hash,
        line_bytes=journal.line_bytes,
        size_before=journal.size_before,
    )


def _discard_intent(path: Path, report: RecoveryReport) -> None:
    if path.exists():
        path.unlink()
        _fsync_dir(path.parent)
    report.auto_recovered += 1
    report.discarded_intents += 1


def _journal_groups(journal_dir: Path) -> dict[str, dict[str, Path]]:
    groups: dict[str, dict[str, Path]] = {}
    for path in journal_dir.glob(".events-journal-*.*"):
        prefix = ".events-journal-"
        if not path.name.startswith(prefix):
            continue
        event_and_suffix = path.name[len(prefix) :]
        event_id, sep, suffix = event_and_suffix.rpartition(".")
        if not sep or suffix not in {"intent", "committed"}:
            groups.setdefault(event_and_suffix, {})["invalid"] = path
            continue
        groups.setdefault(event_id, {})[suffix] = path
    return groups


def _parse_journal(path: Path, suffix: str) -> Journal | None:
    fields: dict[str, str] = {}
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return None
    for line in text.splitlines():
        key, sep, value = line.partition("=")
        if sep:
            fields[key] = value

    if suffix == "intent" and fields.get("state") != "intent":
        return None
    if suffix == "committed" and fields.get("state") not in {"intent", "committed"}:
        return None
    try:
        event_id = fields["event_id"]
        event_seq = int(fields["event_seq"])
        event_hash = fields["event_hash"]
    except (KeyError, ValueError):
        return None

    line_bytes: int | None = None
    size_before: int | None = None
    if suffix == "intent":
        try:
            line_bytes = int(fields["line_bytes"])
            size_before = int(fields["events_jsonl_size_before_append"])
        except (KeyError, ValueError):
            return None

    return Journal(
        path=path,
        suffix=suffix,
        event_id=event_id,
        event_seq=event_seq,
        event_hash=event_hash,
        prev_event_hash=fields.get("prev_event_hash"),
        line_bytes=line_bytes,
        size_before=size_before,
    )


def _partial_valid(partial: Path, journal: Journal) -> bool:
    try:
        line = partial.read_bytes()
    except OSError:
        return False
    return (
        journal.line_bytes is not None
        and len(line) == journal.line_bytes
        and line.endswith(b"\n")
        and hash_event_line(line) == journal.event_hash
    )


def _read_tail_fragment(path: Path) -> bytes:
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return b""
    if not data:
        return b""
    if data.endswith(b"\n"):
        lines = data.splitlines(keepends=True)
        return lines[-1] if lines else b""
    return data.rsplit(b"\n", 1)[-1]


def _tail_hash_for_prefix(path: Path, size: int) -> str:
    if size == 0:
        return SENTINEL_PREV_HASH
    with path.open("rb") as handle:
        data = handle.read(size)
    if not data:
        return SENTINEL_PREV_HASH
    if data.endswith(b"\n"):
        lines = data.splitlines(keepends=True)
        tail = lines[-1] if lines else b""
    else:
        tail = data.rsplit(b"\n", 1)[-1]
    return hash_event_line(tail)


def _append_bytes(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o666)
    try:
        _write_all(fd, data)
        os.fsync(fd)
    finally:
        os.close(fd)


def _truncate(path: Path, size: int) -> None:
    with path.open("r+b") as handle:
        handle.truncate(size)
        handle.flush()
        os.fsync(handle.fileno())


def _cleanup_partial(events_path: Path, event_id: str) -> None:
    partial = _partial_path(events_path, event_id)
    if partial.exists():
        partial.unlink()
        _fsync_dir(partial.parent)


def _partial_path(events_path: Path, event_id: str) -> Path:
    return events_path.with_name(f"{events_path.name}.partial-{event_id}")


def _get_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except FileNotFoundError:
        return 0


def _stop(report: RecoveryReport, code: str, event_id: str) -> None:
    report.stop_lines.append(code)
    raise RecoveryStopLine(code, f"{code}: {event_id}")


def _replay_event_placeholder(events_path: Path, event_line: bytes) -> None:
    import json

    from agent_mesh.config import config_from_agent_dir
    from agent_mesh.store.rebuild import apply_event, rebuild_all
    from agent_mesh.views import render_all

    if not event_line:
        return
    record = json.loads(event_line)
    agent_dir = events_path.parent
    config = config_from_agent_dir(agent_dir)
    try:
        apply_event(record, agent_dir=agent_dir)
    except RuntimeError as exc:
        if "projection gap" not in str(exc):
            raise
        rebuild_all(config)
    render_all(config)


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        written = os.write(fd, view)
        if written == 0:
            raise OSError("short write while recovering event log")
        view = view[written:]
