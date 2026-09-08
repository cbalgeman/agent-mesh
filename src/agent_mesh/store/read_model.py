"""Mutation-free reads over one verified canonical event-log snapshot."""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import stat
import threading
import time
from collections import OrderedDict
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterator

from agent_mesh.config import AgentMeshConfig
from agent_mesh.core.chain import verify_chain_bytes
from agent_mesh.core.hashing import SENTINEL_PREV_HASH, hash_event_line
from agent_mesh.store.rebuild import PROJECTION_VERSION, apply_record
from agent_mesh.store.sqlite import initialize_schema, set_meta


class ReadModelUnavailable(RuntimeError):
    """Raised when a verified canonical query snapshot cannot be established safely."""


_SNAPSHOT_READ_CHUNK_BYTES = 64 * 1024


@dataclass(frozen=True)
class CanonicalEventSnapshot:
    """One stable, hash-chain-verified capture of the canonical event log."""

    records: tuple[dict[str, Any], ...]
    source_log_sha256: str
    source_log_bytes: int
    event_seq: int
    tail_event_sha256: str
    source_identity: tuple[int, int, int, int, int, int]


@dataclass(frozen=True)
class ReadModelSnapshot:
    """One verified event-log snapshot and its in-memory projection."""

    conn: sqlite3.Connection
    records: tuple[dict[str, Any], ...]
    read_model: str
    projection_version: str
    source_log_sha256: str
    source_log_bytes: int
    event_seq: int
    tail_event_sha256: str
    warnings: tuple[str, ...] = ()


@dataclass
class _VerifiedCacheEntry:
    """One bounded projection tied to a retained verified source descriptor."""

    canonical: CanonicalEventSnapshot
    serialized: bytes
    source_descriptor: int

    @property
    def weight(self) -> int:
        return len(self.serialized) + self.canonical.source_log_bytes


class VerifiedReadModelCache:
    """Bounded process-local cache of projections from verified canonical bytes.

    A cold open captures and verifies the full canonical event log before it is
    replayed.  A warm open may reuse that exact snapshot only while a retained
    descriptor and a fresh no-follow descriptor for the configured path have
    the same unchanged regular-file identity.  Unsupported descriptor
    capabilities disable the fast path.  A single lock also prevents
    concurrent cache misses from amplifying CPU-bound replay work in threaded
    local servers.
    """

    def __init__(self, *, max_entries: int, max_serialized_bytes: int) -> None:
        if max_entries < 1:
            raise ValueError("max_entries must be at least 1")
        if max_serialized_bytes < 1:
            raise ValueError("max_serialized_bytes must be at least 1")
        self._max_entries = max_entries
        self._max_serialized_bytes = max_serialized_bytes
        self._entries: OrderedDict[tuple[str, str, str], _VerifiedCacheEntry] = OrderedDict()
        self._serialized_bytes = 0
        self._lock = threading.Lock()

    def clear(self) -> None:
        with self._lock:
            for entry in self._entries.values():
                _close_descriptor(entry.source_descriptor)
            self._entries.clear()
            self._serialized_bytes = 0

    @contextmanager
    def open(
        self,
        config: AgentMeshConfig,
        *,
        max_bytes: int | None = None,
        max_events: int | None = None,
        deadline_monotonic: float | None = None,
    ) -> Iterator[ReadModelSnapshot]:
        if deadline_monotonic is None:
            acquired = self._lock.acquire()
        else:
            remaining = deadline_monotonic - time.monotonic()
            acquired = remaining > 0 and self._lock.acquire(timeout=remaining)
        if not acquired:
            raise ReadModelUnavailable("read-model cache wait exceeded its time budget")
        conn: sqlite3.Connection | None = None
        try:
            config_sha = hashlib.sha256(repr(config).encode("utf-8")).hexdigest()
            key = (
                str(config.project_root.resolve()),
                config_sha,
                PROJECTION_VERSION,
            )
            entry = self._entries.get(key)
            canonical: CanonicalEventSnapshot | None = None
            if entry is not None and _cached_source_is_current(
                config.events_path,
                entry,
                max_bytes=max_bytes,
                max_events=max_events,
                deadline_monotonic=deadline_monotonic,
            ):
                try:
                    conn = _deserialize_read_model(entry.serialized)
                except (sqlite3.Error, MemoryError):
                    self._remove(key)
                else:
                    canonical = entry.canonical
                    self._entries.move_to_end(key)
            elif entry is not None:
                self._remove(key)
            if conn is None:
                canonical = capture_event_snapshot(
                    config,
                    max_bytes=max_bytes,
                    max_events=max_events,
                    deadline_monotonic=deadline_monotonic,
                )
                self._remove_stale_project_entries(key)
                conn = _replay_in_memory(
                    config,
                    canonical.records,
                    source_sha=canonical.source_log_sha256,
                    deadline_monotonic=deadline_monotonic,
                )
                serialized = None
                try:
                    page_count = int(conn.execute("PRAGMA page_count").fetchone()[0])
                    page_size = int(conn.execute("PRAGMA page_size").fetchone()[0])
                    if page_count * page_size <= self._max_serialized_bytes:
                        serialized = conn.serialize()
                except (sqlite3.Error, MemoryError, NotImplementedError, TypeError, ValueError):
                    pass
                if serialized is not None:
                    retained_descriptor = _retain_verified_source(
                        config.events_path,
                        canonical.source_identity,
                    )
                    if retained_descriptor is not None:
                        self._store(
                            key,
                            _VerifiedCacheEntry(
                                canonical=canonical,
                                serialized=serialized,
                                source_descriptor=retained_descriptor,
                            ),
                        )
            if _deadline_expired(deadline_monotonic):
                raise ReadModelUnavailable("read-model cache open exceeded its time budget")
        except Exception:
            if conn is not None:
                conn.close()
            raise
        finally:
            self._lock.release()

        assert conn is not None
        assert canonical is not None
        snapshot = ReadModelSnapshot(
            conn=conn,
            records=canonical.records,
            read_model="in_memory",
            projection_version=PROJECTION_VERSION,
            source_log_sha256=canonical.source_log_sha256,
            source_log_bytes=canonical.source_log_bytes,
            event_seq=canonical.event_seq,
            tail_event_sha256=canonical.tail_event_sha256,
        )
        try:
            yield snapshot
        finally:
            conn.close()

    def _remove(self, key: tuple[str, str, str]) -> None:
        removed = self._entries.pop(key, None)
        if removed is not None:
            self._serialized_bytes -= removed.weight
            _close_descriptor(removed.source_descriptor)

    def _store(self, key: tuple[str, str, str], entry: _VerifiedCacheEntry) -> None:
        self._remove_stale_project_entries(key)
        self._remove(key)
        if entry.weight > self._max_serialized_bytes:
            _close_descriptor(entry.source_descriptor)
            return
        while self._entries and (
            len(self._entries) >= self._max_entries
            or self._serialized_bytes + entry.weight > self._max_serialized_bytes
        ):
            _, removed = self._entries.popitem(last=False)
            self._serialized_bytes -= removed.weight
            _close_descriptor(removed.source_descriptor)
        self._entries[key] = entry
        self._serialized_bytes += entry.weight

    def _remove_stale_project_entries(self, key: tuple[str, str, str]) -> None:
        project_root = key[0]
        for stale_key in tuple(self._entries):
            if stale_key[0] == project_root and stale_key != key:
                self._remove(stale_key)


def _deserialize_read_model(serialized: bytes) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    try:
        conn.deserialize(serialized)
        conn.execute("PRAGMA query_only=ON")
        conn.execute("BEGIN")
        return conn
    except Exception:
        conn.close()
        raise


def _deadline_expired(deadline_monotonic: float | None) -> bool:
    return deadline_monotonic is not None and time.monotonic() >= deadline_monotonic


def _source_identity(value: os.stat_result) -> tuple[int, int, int, int, int, int]:
    return (
        int(value.st_dev),
        int(value.st_ino),
        int(value.st_mode),
        int(value.st_size),
        int(value.st_mtime_ns),
        int(value.st_ctime_ns),
    )


def _descriptor_fast_path_supported() -> bool:
    return all(
        (
            hasattr(os, "O_NOFOLLOW"),
            hasattr(os, "O_CLOEXEC"),
            hasattr(os, "O_DIRECTORY"),
            os.open in getattr(os, "supports_dir_fd", ()),
        )
    )


def _close_descriptor(descriptor: int) -> None:
    try:
        os.close(descriptor)
    except OSError:
        pass


def _retain_verified_source(
    path: Path,
    expected_identity: tuple[int, int, int, int, int, int],
) -> int | None:
    """Retain the exact verified regular file, or disable reuse for this capture."""

    if not _descriptor_fast_path_supported():
        return None
    parent_descriptor: int | None = None
    descriptor: int | None = None
    try:
        parent_descriptor = os.open(
            path.parent,
            os.O_RDONLY | os.O_DIRECTORY | os.O_CLOEXEC | os.O_NOFOLLOW,
        )
        descriptor = os.open(
            path.name,
            os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW,
            dir_fd=parent_descriptor,
        )
        current = os.fstat(descriptor)
        if not stat.S_ISREG(current.st_mode) or _source_identity(current) != expected_identity:
            _close_descriptor(descriptor)
            return None
        return descriptor
    except (AttributeError, OSError):
        if descriptor is not None:
            _close_descriptor(descriptor)
        return None
    finally:
        if parent_descriptor is not None:
            _close_descriptor(parent_descriptor)


def _cached_source_is_current(
    path: Path,
    entry: _VerifiedCacheEntry,
    *,
    max_bytes: int | None,
    max_events: int | None,
    deadline_monotonic: float | None,
) -> bool:
    """Check a warm entry without trusting pathname metadata or cached bytes."""

    canonical = entry.canonical
    if max_bytes is not None and canonical.source_log_bytes > max_bytes:
        return False
    if max_events is not None and len(canonical.records) > max_events:
        return False
    if _deadline_expired(deadline_monotonic) or not _descriptor_fast_path_supported():
        return False
    fresh_descriptor = _retain_verified_source(path, canonical.source_identity)
    if fresh_descriptor is None:
        return False
    try:
        retained_before = os.fstat(entry.source_descriptor)
        retained_after = os.fstat(entry.source_descriptor)
    except OSError:
        return False
    finally:
        _close_descriptor(fresh_descriptor)
    return (
        stat.S_ISREG(retained_before.st_mode)
        and _source_identity(retained_before) == canonical.source_identity
        and _source_identity(retained_after) == canonical.source_identity
        and not _deadline_expired(deadline_monotonic)
    )


def _stable_bytes(
    path: Path,
    *,
    attempts: int = 3,
    max_bytes: int | None = None,
    deadline_monotonic: float | None = None,
) -> bytes:
    """Capture bytes only when the source identity is unchanged around the read."""

    data, _ = _stable_bytes_with_identity(
        path,
        attempts=attempts,
        max_bytes=max_bytes,
        deadline_monotonic=deadline_monotonic,
    )
    return data


def _stable_bytes_with_identity(
    path: Path,
    *,
    attempts: int = 3,
    max_bytes: int | None = None,
    deadline_monotonic: float | None = None,
) -> tuple[bytes, tuple[int, int, int, int, int, int]]:
    """Capture bytes and the stable regular-file identity that produced them."""

    for _ in range(attempts):
        if _deadline_expired(deadline_monotonic):
            raise ReadModelUnavailable("canonical snapshot exceeded its time budget")
        descriptor: int | None = None
        try:
            flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_BINARY", 0)
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            descriptor = os.open(path, flags)
            before = os.fstat(descriptor)
            if not stat.S_ISREG(before.st_mode):
                raise ReadModelUnavailable("canonical event log is not a regular file")
            if max_bytes is not None and before.st_size > max_bytes:
                raise ReadModelUnavailable(
                    f"canonical event log exceeds the {max_bytes}-byte read budget"
                )
            chunks: list[bytes] = []
            captured_bytes = 0
            while True:
                if _deadline_expired(deadline_monotonic):
                    raise ReadModelUnavailable("canonical snapshot exceeded its time budget")
                read_size = _SNAPSHOT_READ_CHUNK_BYTES
                if max_bytes is not None:
                    read_size = min(read_size, max_bytes - captured_bytes + 1)
                chunk = os.read(descriptor, read_size)
                if not chunk:
                    break
                chunks.append(chunk)
                captured_bytes += len(chunk)
                if max_bytes is not None and captured_bytes > max_bytes:
                    raise ReadModelUnavailable(
                        f"canonical event log exceeds the {max_bytes}-byte read budget"
                    )
                if _deadline_expired(deadline_monotonic):
                    raise ReadModelUnavailable("canonical snapshot exceeded its time budget")
            data = b"".join(chunks)
            after = os.fstat(descriptor)
            current = path.stat()
        except ReadModelUnavailable:
            raise
        except OSError as exc:
            raise ReadModelUnavailable(f"cannot read canonical event log: {exc}") from exc
        finally:
            if descriptor is not None:
                _close_descriptor(descriptor)
        if _deadline_expired(deadline_monotonic):
            raise ReadModelUnavailable("canonical snapshot exceeded its time budget")
        before_identity = _source_identity(before)
        after_identity = _source_identity(after)
        current_identity = _source_identity(current)
        if (
            before_identity == after_identity == current_identity
            and len(data) == before.st_size
        ):
            return data, before_identity
    raise ReadModelUnavailable("canonical event log changed during bounded snapshot capture")


def _records_from_snapshot(
    data: bytes,
    *,
    max_events: int | None = None,
    deadline_monotonic: float | None = None,
) -> tuple[dict[str, Any], ...]:
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(data.splitlines(), start=1):
        if _deadline_expired(deadline_monotonic):
            raise ReadModelUnavailable("canonical snapshot parsing exceeded its time budget")
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ReadModelUnavailable(
                f"canonical event log line {line_number} is not valid JSON"
            ) from exc
        if not isinstance(value, dict):
            raise ReadModelUnavailable(f"canonical event log line {line_number} is not an object")
        records.append(value)
        if max_events is not None and len(records) > max_events:
            raise ReadModelUnavailable(
                f"canonical event log exceeds the {max_events}-event replay budget"
            )
    return tuple(records)


def _replay_in_memory(
    config: AgentMeshConfig,
    records: tuple[dict[str, Any], ...],
    *,
    source_sha: str,
    deadline_monotonic: float | None = None,
) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    try:
        initialize_schema(conn)
        for record in records:
            if _deadline_expired(deadline_monotonic):
                raise ReadModelUnavailable("read-model replay exceeded its time budget")
            apply_record(record, config, conn=conn, require_next=True)
        if _deadline_expired(deadline_monotonic):
            raise ReadModelUnavailable("read-model replay exceeded its time budget")
        with conn:
            set_meta(conn, "events_jsonl_sha", source_sha)
            set_meta(conn, "projection_version", PROJECTION_VERSION)
        conn.execute("PRAGMA query_only=ON")
        conn.execute("BEGIN")
        return conn
    except ReadModelUnavailable:
        conn.close()
        raise
    except Exception as exc:
        conn.close()
        raise ReadModelUnavailable(f"cannot replay verified read model: {exc}") from exc


def capture_event_snapshot(
    config: AgentMeshConfig,
    *,
    max_bytes: int | None = None,
    max_events: int | None = None,
    deadline_monotonic: float | None = None,
) -> CanonicalEventSnapshot:
    """Capture canonical bytes once and fail closed unless their hash chain verifies."""

    data, source_identity = _stable_bytes_with_identity(
        config.events_path,
        max_bytes=max_bytes,
        deadline_monotonic=deadline_monotonic,
    )
    chain = verify_chain_bytes(data)
    if _deadline_expired(deadline_monotonic):
        raise ReadModelUnavailable("canonical verification exceeded its time budget")
    if not chain.ok:
        raise ReadModelUnavailable(f"canonical event chain is invalid: {chain.error}")
    records = _records_from_snapshot(
        data,
        max_events=max_events,
        deadline_monotonic=deadline_monotonic,
    )
    source_sha = hashlib.sha256(data).hexdigest()
    try:
        event_seq = int(records[-1]["event_seq"]) if records else 0
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise ReadModelUnavailable("canonical event log has an invalid final event_seq") from exc
    if records:
        previous_newline = data.rfind(b"\n", 0, len(data) - 1)
        tail_event_sha256 = hash_event_line(data[previous_newline + 1 :])
    else:
        tail_event_sha256 = SENTINEL_PREV_HASH
    return CanonicalEventSnapshot(
        records=records,
        source_log_sha256=source_sha,
        source_log_bytes=len(data),
        event_seq=event_seq,
        tail_event_sha256=tail_event_sha256,
        source_identity=source_identity,
    )


def latest_read_model_match(
    config: AgentMeshConfig,
    canonical: CanonicalEventSnapshot | ReadModelSnapshot,
    *,
    predicate: Callable[[sqlite3.Connection, dict[str, Any]], bool],
    capture: Callable[[sqlite3.Connection, dict[str, Any]], Any],
    max_events: int,
    max_source_bytes: int,
    deadline_monotonic: float,
) -> tuple[int, Any] | None:
    """Capture the latest matching state in one bounded verified-snapshot replay."""

    if len(canonical.records) > max_events:
        raise ReadModelUnavailable(
            f"historical comparison exceeds the {max_events}-event replay budget"
        )
    if canonical.source_log_bytes > max_source_bytes:
        raise ReadModelUnavailable(
            f"historical comparison exceeds the {max_source_bytes}-byte replay budget"
        )
    if _deadline_expired(deadline_monotonic):
        raise ReadModelUnavailable("historical comparison exceeded its time budget")

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    latest: tuple[int, Any] | None = None
    try:
        initialize_schema(conn)
        for record in canonical.records:
            if _deadline_expired(deadline_monotonic):
                raise ReadModelUnavailable("historical comparison exceeded its time budget")
            apply_record(record, config, conn=conn, require_next=True)
            if predicate(conn, record):
                try:
                    event_seq = int(record["event_seq"])
                except (KeyError, TypeError, ValueError, OverflowError) as exc:
                    raise ReadModelUnavailable(
                        "matching canonical event has an invalid event_seq"
                    ) from exc
                latest = (event_seq, capture(conn, record))
            if _deadline_expired(deadline_monotonic):
                raise ReadModelUnavailable("historical comparison exceeded its time budget")
        return latest
    except ReadModelUnavailable:
        raise
    except Exception as exc:
        raise ReadModelUnavailable(f"cannot inspect verified historical state: {exc}") from exc
    finally:
        conn.close()


@contextmanager
def open_read_model(
    config: AgentMeshConfig,
    *,
    max_bytes: int | None = None,
    max_events: int | None = None,
    deadline_monotonic: float | None = None,
) -> Iterator[ReadModelSnapshot]:
    """Yield an exact in-memory replay of one verified canonical snapshot.

    The on-disk projection is intentionally ignored. It is WAL-backed and may be
    rebuilt concurrently, so treating it as immutable would disable SQLite's
    locking and change detection without satisfying the immutability contract.
    """

    canonical = capture_event_snapshot(
        config,
        max_bytes=max_bytes,
        max_events=max_events,
        deadline_monotonic=deadline_monotonic,
    )
    conn = _replay_in_memory(
        config,
        canonical.records,
        source_sha=canonical.source_log_sha256,
        deadline_monotonic=deadline_monotonic,
    )
    snapshot = ReadModelSnapshot(
        conn=conn,
        records=canonical.records,
        read_model="in_memory",
        projection_version=PROJECTION_VERSION,
        source_log_sha256=canonical.source_log_sha256,
        source_log_bytes=canonical.source_log_bytes,
        event_seq=canonical.event_seq,
        tail_event_sha256=canonical.tail_event_sha256,
    )
    try:
        yield snapshot
    finally:
        conn.close()
