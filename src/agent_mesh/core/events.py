"""Durable event append protocol for events.jsonl."""

from __future__ import annotations

import json
import os
import warnings
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from agent_mesh.core.agent_instances import (
    INSTANCE_EVENT_KINDS,
    AgentInstanceError,
    reduce_agent_instances,
    selected_agent_instance,
    validate_actor_shadow,
)
from agent_mesh.core.dispatch_schema import DispatchSchemaError, validate_dispatch_payload
from agent_mesh.core.hashing import SENTINEL_PREV_HASH, canonical_json, hash_event_line
from agent_mesh.core.ids import new_ulid
from agent_mesh.core.provenance import ProvenanceValidationError, validate_event_provenance
from agent_mesh.core.workflow_origin import (
    WorkflowOriginValidationError,
    validate_event_workflow_origin,
)

FAULT_ENV_VAR = "AGENT_MESH_FAULT_AFTER"
ALLOW_NOOP_REPLAY_ENV_VAR = "AGENT_MESH_ALLOW_NOOP_REPLAY"
DEPRECATED_ENV_ALIASES = {
    FAULT_ENV_VAR: "AGENT_MAIL_FAULT_AFTER",
    ALLOW_NOOP_REPLAY_ENV_VAR: "AGENT_MAIL_ALLOW_NOOP_REPLAY",
}
_TAIL_READ_BUFFER = 64 * 1024  # 64 KiB — enough for any single event line in v1


class EventProtocolError(RuntimeError):
    """Raised when an event cannot be safely appended."""


@dataclass(frozen=True)
class Event:
    """Schema-versioned event envelope stored as one canonical JSONL line."""

    event_id: str
    schema_version: int = 1
    occurred_utc: str = field(default_factory=lambda: utc_now())
    event_seq: int | None = None
    actor: str = ""
    actor_instance_id: str = ""
    kind: str = ""
    entity_id: str = ""
    thread_id: str = ""
    prev_event_hash: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        if self.event_seq is None:
            raise EventProtocolError("event_seq must be assigned before serialization")
        if self.prev_event_hash is None:
            raise EventProtocolError("prev_event_hash must be assigned before serialization")
        envelope = {
            "actor": self.actor,
            "entity_id": self.entity_id,
            "event_id": self.event_id,
            "event_seq": self.event_seq,
            "kind": self.kind,
            "occurred_utc": self.occurred_utc,
            "payload": self.payload,
            "prev_event_hash": self.prev_event_hash,
            "schema_version": self.schema_version,
            "thread_id": self.thread_id,
        }
        if self.actor_instance_id:
            envelope["actor_instance_id"] = self.actor_instance_id
        return envelope


@dataclass(frozen=True)
class AppendResult:
    """Result of a successful append."""

    event: Event
    event_hash: str
    line_bytes: bytes
    queue_depth: int = 0


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def generate_event_id() -> str:
    """Return an `ev_<ulid>` identifier using a ULID-compatible 26-char body."""
    return new_ulid("ev")


def append_event(
    events_path: str | Path, event: Event, lock_acquired: bool = False
) -> AppendResult:
    """Append one event with the durable intent/commit journal protocol.

    The public default acquires the mesh lock. Pass ``lock_acquired=True`` only
    from a writer that already owns ``.mail-lock`` for the entire transaction.
    """
    event = replace(
        event,
        actor_instance_id=selected_agent_instance(event.actor_instance_id),
    )
    try:
        validate_actor_shadow(event.kind, event.actor, event.payload)
    except AgentInstanceError as exc:
        raise EventProtocolError(str(exc)) from exc
    try:
        validate_event_provenance(event.kind, event.payload, event.entity_id)
    except ProvenanceValidationError as exc:
        raise EventProtocolError(str(exc)) from exc
    try:
        validate_event_workflow_origin(event.kind, event.payload, event.entity_id)
    except WorkflowOriginValidationError as exc:
        raise EventProtocolError(str(exc)) from exc
    try:
        validate_dispatch_payload(event.kind, event.payload)
    except DispatchSchemaError as exc:
        raise EventProtocolError(str(exc)) from exc

    path = Path(events_path)
    journal_dir = path.parent

    if not lock_acquired:
        from agent_mesh.core.lock import acquire

        journal_dir.mkdir(parents=True, exist_ok=True)
        lock_handle = acquire(journal_dir / ".mail-lock")
        try:
            result = append_event(path, event, lock_acquired=True)
        finally:
            last_event_seq = None
            if "result" in locals():
                last_event_seq = result.event.event_seq
            lock_handle.release(last_event_seq=last_event_seq)
        return replace(result, queue_depth=lock_handle.queue_depth)

    journal_dir.mkdir(parents=True, exist_ok=True)
    from agent_mesh.core.recovery import recover

    recover(path, journal_dir)
    event = _validate_agent_instance_before_append(event, path)
    _validate_stateful_event_before_append(event, journal_dir)

    prev_line, prev_hash = read_tail_line(path)
    next_seq = _next_event_seq(prev_line)
    if event.event_seq is not None and event.event_seq != next_seq:
        raise EventProtocolError(f"event_seq {event.event_seq} does not match next seq {next_seq}")
    if event.prev_event_hash is not None and event.prev_event_hash != prev_hash:
        raise EventProtocolError("prev_event_hash does not match events.jsonl tail")

    prepared = replace(event, event_seq=next_seq, prev_event_hash=prev_hash)
    line = canonical_json(prepared.to_dict()) + b"\n"
    event_hash = hash_event_line(line)
    _fault_after("A")

    size_before = get_size(path)
    intent_path = _journal_path(journal_dir, prepared.event_id, "intent")
    committed_path = _journal_path(journal_dir, prepared.event_id, "committed")
    partial_path = _partial_path(path, prepared.event_id)
    _write_intent_journal(
        intent_path=intent_path,
        event=prepared,
        event_hash=event_hash,
        line_bytes=len(line),
        size_before=size_before,
    )
    _fault_after("B")

    if len(line) < 4096:
        _append_line(path, line)
        _fault_after("C.small")
    else:
        _write_partial(partial_path, line)
        _fault_after("C.c1.done")
        _fault_during_large_append(path, line)
        _append_line(path, line)
        _fault_after("C.c2.full")
        if partial_path.exists():
            partial_path.unlink()
            _fsync_dir(path.parent)
    _fault_after("C")

    _fault_after("D.before")
    os.replace(intent_path, committed_path)
    _fsync_dir(journal_dir)
    _fault_after("D")

    _replay_event_placeholder(prepared, path.parent)
    _fault_after("E")
    _fault_after("E.done")

    _fault_before_unlink()
    if committed_path.exists():
        committed_path.unlink()
        _fsync_dir(journal_dir)
    _fault_after("F")

    return AppendResult(event=prepared, event_hash=event_hash, line_bytes=line)


def _validate_agent_instance_before_append(event: Event, events_path: Path) -> Event:
    """Resolve and enforce durable instance attribution while holding the append lock."""

    from agent_mesh.config import config_from_agent_dir

    records: list[dict[str, Any]] = []
    if events_path.exists():
        with events_path.open("r", encoding="utf-8") as handle:
            records = [json.loads(line) for line in handle if line.strip()]
    try:
        instances = reduce_agent_instances(records)
    except AgentInstanceError as exc:
        raise EventProtocolError(str(exc)) from exc

    selected = event.actor_instance_id.strip()
    if selected:
        matches = [
            item
            for item in instances.values()
            if selected == item.id or selected.lower() in item.aliases
        ]
        if len(matches) != 1:
            code = "AGENT_INSTANCE_UNKNOWN" if not matches else "AGENT_INSTANCE_LABEL_AMBIGUOUS"
            raise EventProtocolError(f"{code}: {selected}")
        instance = matches[0]
        if instance.status != "active":
            raise EventProtocolError(f"AGENT_INSTANCE_RETIRED: {instance.id}")
        if instance.participant != event.actor:
            raise EventProtocolError(
                f"AGENT_INSTANCE_ACTOR_MISMATCH: {instance.id} belongs to "
                f"{instance.participant!r}, not {event.actor!r}"
            )
        event = replace(event, actor_instance_id=instance.id)
    elif any(
        item.participant == event.actor and item.status == "active" for item in instances.values()
    ):
        raise EventProtocolError(
            f"AGENT_INSTANCE_REQUIRED: participant {event.actor!r} has active registered instances; "
            "use --instance or AGENT_MESH_INSTANCE_ID"
        )

    if event.kind == "req_created":
        raw_targets = event.payload.get("to_instances", [])
        if not isinstance(raw_targets, list):
            raise EventProtocolError("AGENT_INSTANCE_ADDRESS_INVALID: to_instances must be a list")
        recipients = event.payload.get("to", [])
        if isinstance(recipients, str):
            recipients = [recipients]
        recipient_set = {str(value) for value in recipients}
        seen_targets: set[str] = set()
        for raw_target in raw_targets:
            target_id = str(raw_target)
            if target_id in seen_targets:
                raise EventProtocolError(
                    f"AGENT_INSTANCE_ADDRESS_DUPLICATE: {target_id}"
                )
            seen_targets.add(target_id)
            target = instances.get(target_id)
            if target is None:
                raise EventProtocolError(f"AGENT_INSTANCE_UNKNOWN: {target_id}")
            if target.status != "active":
                raise EventProtocolError(f"AGENT_INSTANCE_RETIRED: {target_id}")
            if target.participant not in recipient_set:
                raise EventProtocolError(
                    f"AGENT_INSTANCE_RECIPIENT_MISMATCH: {target_id} belongs to "
                    f"{target.participant!r}"
                )
    elif event.kind == "res_posted":
        request_id = str(event.payload.get("request_id") or event.thread_id)
        request = next(
            (
                record
                for record in records
                if record.get("kind") == "req_created" and record.get("entity_id") == request_id
            ),
            None,
        )
        target_ids = (
            request.get("payload", {}).get("to_instances", [])
            if isinstance(request, dict) and isinstance(request.get("payload"), dict)
            else []
        )
        addressed_participants = {
            instances[str(target)].participant
            for target in target_ids
            if str(target) in instances
        }
        if event.actor in addressed_participants and event.actor_instance_id not in {
            str(target) for target in target_ids
        }:
            raise EventProtocolError(
                f"AGENT_INSTANCE_NOT_ADDRESSED: "
                f"{event.actor_instance_id or event.actor} is not addressed by {request_id}"
            )
    elif event.kind == "backlog_item_upserted":
        raw_owner_instance_id = event.payload.get("owner_instance_id")
        owner_instance_id = (
            str(raw_owner_instance_id).strip()
            if raw_owner_instance_id is not None
            else ""
        )
        if owner_instance_id:
            owner = instances.get(owner_instance_id)
            if owner is None:
                raise EventProtocolError(f"AGENT_INSTANCE_UNKNOWN: {owner_instance_id}")
            if owner.status != "active":
                raise EventProtocolError(f"AGENT_INSTANCE_RETIRED: {owner_instance_id}")

    if event.kind not in INSTANCE_EVENT_KINDS:
        return event

    config = config_from_agent_dir(events_path.parent)
    payload = event.payload
    identifier = str(payload.get("id") or event.entity_id)
    if identifier != event.entity_id or event.thread_id != event.entity_id:
        raise EventProtocolError(
            "agent instance id, entity_id, and thread_id must identify the same instance"
        )
    if event.kind == "agent_instance_registered":
        participant = str(payload.get("participant", "")).strip()
        if participant not in config.participants:
            raise EventProtocolError(
                f"PARTICIPANT_UNKNOWN: instance participant {participant!r} is not configured"
            )
        runtime_profile = str(payload.get("runtime_profile", "")).strip()
        if runtime_profile:
            profile = config.runtime_profiles.get(runtime_profile)
            if profile is None:
                raise EventProtocolError(f"RUNTIME_PROFILE_UNKNOWN: {runtime_profile}")
            if profile.target != participant:
                raise EventProtocolError(
                    f"RUNTIME_PROFILE_TARGET_MISMATCH: {runtime_profile} targets "
                    f"{profile.target!r}, not {participant!r}"
                )
    elif event.kind == "agent_instance_metadata_updated":
        fields = payload.get("fields_changed", {})
        if isinstance(fields, dict) and "runtime_profile" in fields:
            old_new = fields["runtime_profile"]
            runtime_profile = (
                str(old_new[1]).strip()
                if isinstance(old_new, list) and len(old_new) == 2
                else ""
            )
            if runtime_profile:
                profile = config.runtime_profiles.get(runtime_profile)
                target = instances.get(identifier)
                if profile is None:
                    raise EventProtocolError(f"RUNTIME_PROFILE_UNKNOWN: {runtime_profile}")
                if target is None or profile.target != target.participant:
                    raise EventProtocolError(
                        f"RUNTIME_PROFILE_TARGET_MISMATCH: {runtime_profile}"
                    )
    candidate = {
        "actor": event.actor,
        "actor_instance_id": event.actor_instance_id,
        "entity_id": event.entity_id,
        "event_seq": (int(records[-1]["event_seq"]) + 1) if records else 1,
        "kind": event.kind,
        "occurred_utc": event.occurred_utc,
        "payload": event.payload,
        "thread_id": event.thread_id,
    }
    try:
        reduce_agent_instances([*records, candidate])
    except AgentInstanceError as exc:
        raise EventProtocolError(str(exc)) from exc
    return event


def read_tail_line(path: str | Path) -> tuple[bytes, str]:
    """Return the final line bytes and its sha256 hex hash.

    Streaming variant: reads at most _TAIL_READ_BUFFER bytes from the end of the file
    rather than slurping the entire file. Critical for performance once events.jsonl
    grows beyond a few MB. Empty or missing files return the first-event sentinel.
    """
    file_path = Path(path)
    try:
        size = file_path.stat().st_size
    except FileNotFoundError:
        return b"", SENTINEL_PREV_HASH
    if size == 0:
        return b"", SENTINEL_PREV_HASH

    read_size = min(size, _TAIL_READ_BUFFER)
    with file_path.open("rb") as handle:
        handle.seek(size - read_size)
        chunk = handle.read(read_size)

    # Strip trailing newline if present, then take the bytes after the final newline.
    if chunk.endswith(b"\n"):
        before_final_nl = chunk[:-1]
        nl_index = before_final_nl.rfind(b"\n")
        if nl_index == -1:
            # We may have only read part of the last line; if the file is bigger than
            # our buffer and starts mid-line, fall back to a larger read.
            if size > read_size:
                with file_path.open("rb") as handle:
                    handle.seek(max(0, size - min(size, _TAIL_READ_BUFFER * 16)))
                    chunk = handle.read()
                before_final_nl = chunk[:-1] if chunk.endswith(b"\n") else chunk
                nl_index = before_final_nl.rfind(b"\n")
            line = chunk[nl_index + 1 :] if nl_index != -1 else chunk
        else:
            line = chunk[nl_index + 1 :]
    else:
        nl_index = chunk.rfind(b"\n")
        line = chunk[nl_index + 1 :] if nl_index != -1 else chunk

    return line, hash_event_line(line)


def get_size(path: str | Path) -> int:
    try:
        return Path(path).stat().st_size
    except FileNotFoundError:
        return 0


def _validate_stateful_event_before_append(event: Event, agent_dir: Path) -> None:
    """Run projection-backed invariants after recovery and before journaling."""
    from agent_mesh.config import config_from_agent_dir
    from agent_mesh.store.rebuild import (
        DECISION_EVENT_KINDS,
        rebuild_all,
        validate_backlog_write,
        validate_decision_event,
    )
    from agent_mesh.store.sqlite import connect, initialize_schema

    if event.kind not in DECISION_EVENT_KINDS | {"backlog_item_upserted"}:
        return

    config = config_from_agent_dir(agent_dir)
    rebuild_all(config)
    if event.kind == "backlog_item_upserted":
        item_id = str(event.payload.get("id") or event.entity_id)
        if item_id != event.entity_id or event.thread_id != event.entity_id:
            raise EventProtocolError(
                "backlog id, entity_id, and thread_id must identify the same item"
            )
        intent = str(event.payload.get("write_intent") or "upsert")
        conn = connect(config.db_path)
        try:
            initialize_schema(conn)
            validate_backlog_write(conn, item_id, intent)
        finally:
            conn.close()
        return

    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        validate_decision_event(
            conn,
            kind=event.kind,
            entity_id=event.entity_id,
            thread_id=event.thread_id,
            payload=event.payload,
        )
    finally:
        conn.close()


def _next_event_seq(prev_line: bytes) -> int:
    if not prev_line:
        return 1
    if not prev_line.endswith(b"\n"):
        raise EventProtocolError("events.jsonl tail is not newline-terminated")
    try:
        previous = json.loads(prev_line)
    except json.JSONDecodeError as exc:
        raise EventProtocolError("events.jsonl tail is not valid JSON") from exc
    try:
        return int(previous["event_seq"]) + 1
    except (KeyError, TypeError, ValueError) as exc:
        raise EventProtocolError("events.jsonl tail is missing integer event_seq") from exc


def _write_intent_journal(
    *,
    intent_path: Path,
    event: Event,
    event_hash: str,
    line_bytes: int,
    size_before: int,
) -> None:
    if _fault_matches("B.partial"):
        with intent_path.open("w", encoding="utf-8") as handle:
            handle.write(f"event_id={event.event_id}\nstate=int")
            handle.flush()
            os.fsync(handle.fileno())
        _exit_for_fault()

    lines = [
        f"event_id={event.event_id}",
        f"event_seq={event.event_seq}",
        f"event_hash={event_hash}",
        f"prev_event_hash={event.prev_event_hash}",
        f"line_bytes={line_bytes}",
        f"events_jsonl_size_before_append={size_before}",
        "state=intent",
        f"started_utc={utc_now()}",
    ]
    with intent_path.open("w", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    _fsync_dir(intent_path.parent)


def _write_partial(partial_path: Path, line: bytes) -> None:
    if _fault_matches("C.c1"):
        prefix = line[: max(1, min(32, len(line) - 1))]
        with partial_path.open("wb") as handle:
            handle.write(prefix)
            handle.flush()
            os.fsync(handle.fileno())
        _fsync_dir(partial_path.parent)
        _exit_for_fault()

    with partial_path.open("wb") as handle:
        handle.write(line)
        handle.flush()
        os.fsync(handle.fileno())
    _fsync_dir(partial_path.parent)


def _append_line(path: Path, line: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o666)
    try:
        _write_all(fd, line)
        os.fsync(fd)
    finally:
        os.close(fd)
    # M1 fix: fsync the parent directory so the namespace entry is durable.
    # Without this, a crash between append+fsync and a future directory operation
    # can leave events.jsonl with bytes on disk but no directory entry pointing to them.
    _fsync_dir(path.parent)


def _fault_during_large_append(path: Path, line: bytes) -> None:
    if _fault_matches("C.c2.none"):
        _exit_for_fault()
    if _fault_matches("C.c2"):
        prefix = line[: max(1, min(128, len(line) - 1))]
        _append_line(path, prefix)
        _exit_for_fault()


def _fault_before_unlink() -> None:
    if _fault_matches("F"):
        _exit_for_fault()


def _fault_after(point: str) -> None:
    if _fault_matches(point):
        _exit_for_fault()


def _fault_matches(point: str) -> bool:
    return _env_value(FAULT_ENV_VAR) == point


def _env_value(name: str) -> str | None:
    value = os.environ.get(name)
    if value is not None:
        return value
    deprecated = DEPRECATED_ENV_ALIASES.get(name)
    if deprecated is None:
        return None
    value = os.environ.get(deprecated)
    if value is not None:
        warnings.warn(
            f"{deprecated} is deprecated; use {name} instead",
            DeprecationWarning,
            stacklevel=3,
        )
    return value


def _exit_for_fault() -> None:
    os._exit(137)


def _journal_path(journal_dir: Path, event_id: str, suffix: str) -> Path:
    return journal_dir / f".events-journal-{event_id}.{suffix}"


def _partial_path(events_path: Path, event_id: str) -> Path:
    return events_path.with_name(f"{events_path.name}.partial-{event_id}")


def _replay_event_placeholder(event: Event, agent_dir: Path) -> None:
    """Step [E] of the §6.2 pipeline — DB transaction + view regeneration."""
    from agent_mesh.config import config_from_agent_dir
    from agent_mesh.store.rebuild import apply_event
    from agent_mesh.views import render_all

    config = config_from_agent_dir(agent_dir)
    apply_event(event, agent_dir=agent_dir)
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
            raise EventProtocolError("short write while appending event line")
        view = view[written:]
