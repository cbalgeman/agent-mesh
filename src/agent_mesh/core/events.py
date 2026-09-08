"""Durable event append protocol for events.jsonl."""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import sqlite3
import threading
import warnings
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

from agent_mesh.core.agent_instances import (
    INSTANCE_EVENT_KINDS,
    INSTANCE_ID_RE,
    AgentInstanceError,
    reduce_agent_instances,
    selected_agent_instance,
    validate_actor_shadow,
)
from agent_mesh.core.dispatch_schema import (
    DISPATCH_EVENT_KINDS,
    DispatchSchemaError,
    validate_dispatch_payload,
)
from agent_mesh.core.hashing import SENTINEL_PREV_HASH, canonical_json, hash_event_line
from agent_mesh.core.human_authority import (
    has_current_direct_human_authority,
    has_persisted_direct_human_authority,
    is_direct_human_control_event,
)
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
_DECISION_RECEIPT_LIMIT = 64
_DECISION_RECEIPT_LOCK = threading.Lock()
_CAPABILITY_RECEIPT_LIMIT = 64
_CAPABILITY_RECEIPT_LOCK = threading.Lock()
_WORKBENCH_INVOCATION_ENV = "AGENT_MESH_WORKBENCH_INVOCATION"
# A Workbench-launched CLI receives this one process capability at exec. Consume
# it during module bootstrap so probes, drivers, and provider grandchildren
# cannot inherit it from the ambient environment.
_WORKBENCH_INVOCATION = os.environ.pop(_WORKBENCH_INVOCATION_ENV, "")


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


@dataclass(frozen=True)
class _DecisionAppendReceipt:
    """Opaque, process-local capability for one prepared decision append."""

    token: str


@dataclass(frozen=True)
class _CapabilityAppendReceipt:
    """Opaque, process-local authority for one host-verified planned run append."""

    token: str


@dataclass(frozen=True)
class _CapabilityAppendState:
    pid: int
    thread_id: int
    events_path: str
    receipt_json: str
    profile_digest: str
    drift_inputs_digest: str
    valid_until_utc: str


@dataclass(frozen=True)
class _DecisionAppendState:
    lock_handle: Any
    pid: int
    thread_id: int
    events_path: str
    db_path: str
    config_sha256: str
    projection_version: str
    source_log_sha256: str
    last_event_seq: int
    tail_event_hash: str
    table_hashes: tuple[tuple[str, str], ...]
    schema_sha256: str


_DECISION_RECEIPTS: dict[str, _DecisionAppendState] = {}
_CAPABILITY_RECEIPTS: dict[str, _CapabilityAppendState] = {}


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def generate_event_id() -> str:
    """Return an `ev_<ulid>` identifier using a ULID-compatible 26-char body."""
    return new_ulid("ev")


def append_event(
    events_path: str | Path,
    event: Event,
    lock_acquired: bool = False,
    *,
    _decision_receipt: _DecisionAppendReceipt | None = None,
    _capability_receipt: _CapabilityAppendReceipt | None = None,
    _lock_handle: Any = None,
    _workbench_ownership_checked: bool = False,
    _workbench_append_authorizer: Callable[[], None] | None = None,
) -> AppendResult:
    """Append one event with the durable intent/commit journal protocol.

    The public default acquires the mesh lock. Pass ``lock_acquired=True`` only
    from a writer that already owns ``.mail-lock`` for the entire transaction.
    Prepared decision and capability receipts may only be used under that same
    continuously-held lock.
    """
    if not _workbench_ownership_checked:
        from agent_mesh.workbench_service import (
            WorkbenchOwnershipError,
            workbench_invocation_append_lease,
        )

        if _WORKBENCH_INVOCATION:
            from agent_mesh.config import load_config

            try:
                config = load_config(Path(events_path).parent)
                with workbench_invocation_append_lease(
                    config,
                    event,
                    raw_envelope=_WORKBENCH_INVOCATION,
                ) as authorize_append:
                    return append_event(
                        events_path,
                        event,
                        lock_acquired=lock_acquired,
                        _decision_receipt=_decision_receipt,
                        _capability_receipt=_capability_receipt,
                        _lock_handle=_lock_handle,
                        _workbench_ownership_checked=True,
                        _workbench_append_authorizer=authorize_append,
                    )
            except WorkbenchOwnershipError as exc:
                raise EventProtocolError(str(exc)) from exc
    event = replace(
        event,
        actor_instance_id=selected_agent_instance(event.actor_instance_id),
        payload=dict(event.payload),
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
        if _decision_receipt is not None or _capability_receipt is not None:
            raise EventProtocolError("append authority requires the caller-held mesh lock")
        from agent_mesh.core.lock import acquire

        journal_dir.mkdir(parents=True, exist_ok=True)
        lock_handle = acquire(journal_dir / ".mail-lock")
        try:
            result = append_event(
                path,
                event,
                lock_acquired=True,
                _workbench_ownership_checked=_workbench_ownership_checked,
                _workbench_append_authorizer=_workbench_append_authorizer,
            )
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
    if _decision_receipt is None and _capability_receipt is None:
        projection_conn = _validate_stateful_event_before_append(event, journal_dir)
    else:
        projection_conn = _validate_stateful_event_before_append(
            event,
            journal_dir,
            decision_receipt=_decision_receipt,
            capability_receipt=_capability_receipt,
            lock_handle=_lock_handle,
        )
    try:
        if _workbench_append_authorizer is not None:
            # Only a child that passed every stateless, instance, provenance,
            # dispatch, and stateful validator may mint interrupted-run
            # recovery authority. Keep this immediately before durable append.
            _workbench_append_authorizer()
        return _append_prepared_event(
            path,
            event,
            journal_dir=journal_dir,
            projection_conn=projection_conn,
        )
    finally:
        if projection_conn is not None:
            if projection_conn.in_transaction:
                projection_conn.rollback()
            projection_conn.close()


def _append_prepared_event(
    path: Path,
    event: Event,
    *,
    journal_dir: Path,
    projection_conn: sqlite3.Connection | None,
) -> AppendResult:

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

    _replay_event_placeholder(
        prepared,
        path.parent,
        projection_conn=projection_conn,
    )
    _fault_after("E")
    _fault_after("E.done")

    _fault_before_unlink()
    if committed_path.exists():
        committed_path.unlink()
        _fsync_dir(journal_dir)
    _fault_after("F")

    return AppendResult(event=prepared, event_hash=event_hash, line_bytes=line)


def _prepare_decision_append(config: Any, lock_handle: Any) -> _DecisionAppendReceipt:
    """Recover and rebuild decision authority under one continuously-held mesh lock."""

    from agent_mesh.core.lock import is_active_lock_handle
    from agent_mesh.core.recovery import recover
    from agent_mesh.store.rebuild import PROJECTION_VERSION, file_sha256, rebuild_all

    expected_lock_dir = (config.agent_dir / ".mail-lock").resolve()
    if lock_handle is None or not is_active_lock_handle(lock_handle, expected_lock_dir):
        raise EventProtocolError("decision append preparation requires the acquired mesh lock")

    events_path = config.events_path.resolve()
    recover(events_path, config.agent_dir)
    result = rebuild_all(config)
    _tail_line, tail_hash = read_tail_line(events_path)
    token = secrets.token_urlsafe(32)
    state = _DecisionAppendState(
        lock_handle=lock_handle,
        pid=os.getpid(),
        thread_id=threading.get_ident(),
        events_path=str(events_path),
        db_path=str(config.db_path.resolve()),
        config_sha256=_config_sha256(config),
        projection_version=PROJECTION_VERSION,
        source_log_sha256=file_sha256(events_path),
        last_event_seq=result.last_event_seq,
        tail_event_hash=tail_hash,
        table_hashes=tuple(sorted(result.table_hashes.items())),
        schema_sha256=result.schema_sha256,
    )
    with _DECISION_RECEIPT_LOCK:
        if len(_DECISION_RECEIPTS) >= _DECISION_RECEIPT_LIMIT:
            raise EventProtocolError("decision append preparation capacity is exhausted")
        _DECISION_RECEIPTS[token] = state
    return _DecisionAppendReceipt(token=token)


def _discard_decision_append_receipt(receipt: _DecisionAppendReceipt | None) -> None:
    if receipt is None:
        return
    with _DECISION_RECEIPT_LOCK:
        _DECISION_RECEIPTS.pop(receipt.token, None)


def _prepare_capability_append(
    *,
    events_path: str | Path,
    receipt: dict[str, Any],
) -> _CapabilityAppendReceipt:
    """Mint one non-transferable append authority from completed host preflight."""

    receipt_copy = dict(receipt)
    supplied_digest = str(receipt_copy.pop("receipt_digest", ""))
    encoded_unsigned = json.dumps(receipt_copy, sort_keys=True, separators=(",", ":")).encode(
        "utf-8"
    )
    if not supplied_digest or not secrets.compare_digest(
        supplied_digest, hashlib.sha256(encoded_unsigned).hexdigest()
    ):
        raise EventProtocolError("capability receipt digest is invalid")
    if receipt_copy.get("schema") != "agent-mesh.capability-receipt.v1":
        raise EventProtocolError("capability receipt schema is invalid")
    valid_until = _parse_receipt_time(str(receipt_copy.get("valid_until_utc") or ""))
    if valid_until is None or valid_until <= datetime.now(UTC):
        raise EventProtocolError("capability receipt is expired")
    profile_digest = str(receipt_copy.get("profile_digest") or "")
    drift_digest = str(receipt_copy.get("drift_inputs_digest") or "")
    if not _is_sha256(profile_digest) or not _is_sha256(drift_digest):
        raise EventProtocolError("capability receipt binding is invalid")
    receipt_json = json.dumps(receipt, sort_keys=True, separators=(",", ":"))
    token = secrets.token_urlsafe(32)
    state = _CapabilityAppendState(
        pid=os.getpid(),
        thread_id=threading.get_ident(),
        events_path=str(Path(events_path).resolve()),
        receipt_json=receipt_json,
        profile_digest=profile_digest,
        drift_inputs_digest=drift_digest,
        valid_until_utc=str(receipt_copy["valid_until_utc"]),
    )
    with _CAPABILITY_RECEIPT_LOCK:
        if len(_CAPABILITY_RECEIPTS) >= _CAPABILITY_RECEIPT_LIMIT:
            raise EventProtocolError("capability append preparation capacity is exhausted")
        _CAPABILITY_RECEIPTS[token] = state
    return _CapabilityAppendReceipt(token=token)


def _discard_capability_append_receipt(
    receipt: _CapabilityAppendReceipt | None,
) -> None:
    if receipt is None:
        return
    with _CAPABILITY_RECEIPT_LOCK:
        _CAPABILITY_RECEIPTS.pop(receipt.token, None)


def _parse_receipt_time(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.astimezone(UTC) if parsed.tzinfo is not None else None


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def _config_sha256(config: Any) -> str:
    return hashlib.sha256(repr(config).encode("utf-8")).hexdigest()


def _validate_agent_instance_before_append(event: Event, events_path: Path) -> Event:
    """Resolve and enforce durable instance attribution while holding the append lock."""

    from agent_mesh.config import config_from_agent_dir

    config = config_from_agent_dir(events_path.parent)
    if (
        event.actor in config.identity.integration_registrars
        and event.kind not in INSTANCE_EVENT_KINDS
    ):
        raise EventProtocolError(f"AGENT_INSTANCE_REGISTRAR_SCOPE_VIOLATION: {event.actor}")

    records: list[dict[str, Any]] = []
    if events_path.exists():
        with events_path.open("r", encoding="utf-8") as handle:
            records = [json.loads(line) for line in handle if line.strip()]
    try:
        instances = reduce_agent_instances(records)
    except AgentInstanceError as exc:
        raise EventProtocolError(str(exc)) from exc

    selected = event.actor_instance_id.strip()
    direct_human_control = is_direct_human_control_event(event.kind, event.payload)
    manual_registration_bootstrap = (
        event.kind == "agent_instance_registered"
        and str(event.payload.get("registration_origin", "")).strip().lower() == "manual"
        and str(event.payload.get("registrar", "")).strip() == event.actor
    )
    if selected:
        if direct_human_control:
            raise EventProtocolError(
                "AGENT_INSTANCE_FORBIDDEN_FOR_HUMAN_EVENT: "
                f"{event.kind} must record direct human authority without an AI-agent instance"
            )
        matches = [
            item
            for item in instances.values()
            if selected == item.id or selected.lower() in item.aliases
        ]
        if len(matches) != 1:
            code = "AGENT_INSTANCE_UNKNOWN" if not matches else "AGENT_INSTANCE_LABEL_AMBIGUOUS"
            public_selected = "[internal-id]" if INSTANCE_ID_RE.fullmatch(selected) else selected
            raise EventProtocolError(f"{code}: {public_selected}")
        instance = matches[0]
        if instance.status == "terminal":
            raise EventProtocolError(f"AGENT_INSTANCE_TERMINAL: {instance.label}")
        if instance.status != "active":
            raise EventProtocolError(f"AGENT_INSTANCE_RETIRED: {instance.label}")
        if instance.participant != event.actor:
            raise EventProtocolError(
                f"AGENT_INSTANCE_ACTOR_MISMATCH: {instance.label} belongs to "
                f"{instance.participant!r}, not {event.actor!r}"
            )
        event = replace(event, actor_instance_id=instance.id)
    elif (
        not direct_human_control
        and not manual_registration_bootstrap
        and any(
            item.participant == event.actor and item.status == "active"
            for item in instances.values()
        )
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
            target = instances.get(target_id)
            if target_id in seen_targets:
                raise EventProtocolError(
                    "AGENT_INSTANCE_ADDRESS_DUPLICATE: "
                    f"{target.label if target is not None else '[unknown-instance]'}"
                )
            seen_targets.add(target_id)
            if target is None:
                raise EventProtocolError("AGENT_INSTANCE_UNKNOWN: [unknown-instance]")
            if target.status == "terminal":
                raise EventProtocolError(f"AGENT_INSTANCE_TERMINAL: {target.label}")
            if target.status != "active":
                raise EventProtocolError(f"AGENT_INSTANCE_RETIRED: {target.label}")
            if target.participant not in recipient_set:
                raise EventProtocolError(
                    f"AGENT_INSTANCE_RECIPIENT_MISMATCH: {target.label} belongs to "
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
            instances[str(target)].participant for target in target_ids if str(target) in instances
        }
        if event.actor in addressed_participants and event.actor_instance_id not in {
            str(target) for target in target_ids
        }:
            actor_instance = instances.get(event.actor_instance_id)
            raise EventProtocolError(
                f"AGENT_INSTANCE_NOT_ADDRESSED: "
                f"{actor_instance.label if actor_instance is not None else event.actor} "
                f"is not addressed by {request_id}"
            )
    elif event.kind == "backlog_item_upserted":
        raw_owner_instance_id = event.payload.get("owner_instance_id")
        owner_instance_id = (
            str(raw_owner_instance_id).strip() if raw_owner_instance_id is not None else ""
        )
        if owner_instance_id:
            owner = instances.get(owner_instance_id)
            if owner is None:
                raise EventProtocolError("AGENT_INSTANCE_UNKNOWN: [unknown-instance]")
            if owner.status == "terminal":
                raise EventProtocolError(f"AGENT_INSTANCE_TERMINAL: {owner.label}")
            if owner.status != "active":
                raise EventProtocolError(f"AGENT_INSTANCE_RETIRED: {owner.label}")

    if event.kind not in INSTANCE_EVENT_KINDS:
        return event

    payload = event.payload
    forbidden_identity_fields = {
        "external_session_ref",
        "provider_session_ref",
        "launch_attempt_key",
        "binding_credential",
        "registrar_credential",
    }
    leaked_fields = sorted(forbidden_identity_fields & set(payload))
    if leaked_fields:
        raise EventProtocolError(
            "AGENT_INSTANCE_RAW_REFERENCE_FORBIDDEN: " + ", ".join(leaked_fields)
        )
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
        registration_origin = str(payload.get("registration_origin", "legacy")).strip().lower()
        registrar = str(payload.get("registrar", event.actor)).strip()
        if registration_origin == "manual" and registrar != event.actor:
            raise EventProtocolError("AGENT_INSTANCE_REGISTRAR_MISMATCH")
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
            contract_version = payload.get("instance_contract_version", 1)
            if (
                isinstance(contract_version, bool)
                or not isinstance(contract_version, int)
                or contract_version < 1
            ):
                raise EventProtocolError("AGENT_INSTANCE_CONTRACT_VERSION_INVALID")
            if contract_version >= 2:
                expected = {
                    "provider": profile.provider,
                    "durable_role": profile.durable_role,
                    "role": profile.role,
                    "capabilities": list(profile.required_capabilities),
                    "permission_mode": profile.permission_mode,
                    "authentication_mode": profile.authentication_mode,
                    "billing_mode": profile.billing_mode,
                    "adapter_trust": profile.adapter_trust,
                    "session_identity_mode": profile.session_identity_mode,
                    "resumable": profile.resumable,
                    "concurrent_attachment": profile.concurrent_attachment,
                    "terminal_observation": profile.terminal_observation,
                }
                mismatches = [
                    name for name, value in expected.items() if payload.get(name) != value
                ]
                if mismatches:
                    raise EventProtocolError(
                        "AGENT_INSTANCE_PROFILE_MISMATCH: " + ", ".join(sorted(mismatches))
                    )
    elif event.kind == "agent_instance_metadata_updated":
        fields = payload.get("fields_changed", {})
        if isinstance(fields, dict) and "runtime_profile" in fields:
            old_new = fields["runtime_profile"]
            runtime_profile = (
                str(old_new[1]).strip() if isinstance(old_new, list) and len(old_new) == 2 else ""
            )
            if runtime_profile:
                profile = config.runtime_profiles.get(runtime_profile)
                target = instances.get(identifier)
                if profile is None:
                    raise EventProtocolError(f"RUNTIME_PROFILE_UNKNOWN: {runtime_profile}")
                if target is None or profile.target != target.participant:
                    raise EventProtocolError(f"RUNTIME_PROFILE_TARGET_MISMATCH: {runtime_profile}")
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
    from agent_mesh.store.rebuild import (
        AgentInstanceStopLine,
        validate_agent_instance_projection,
    )

    try:
        validate_agent_instance_projection(config, [*records, candidate])
    except (AgentInstanceStopLine, ValueError) as exc:
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


def _validate_stateful_event_before_append(
    event: Event,
    agent_dir: Path,
    *,
    decision_receipt: _DecisionAppendReceipt | None = None,
    capability_receipt: _CapabilityAppendReceipt | None = None,
    lock_handle: Any = None,
) -> sqlite3.Connection | None:
    """Run projection-backed invariants after recovery and before journaling."""
    from agent_mesh.config import config_from_agent_dir
    from agent_mesh.store.rebuild import (
        DECISION_EVENT_KINDS,
        PROJECTION_VERSION,
        file_sha256,
        rebuild_all,
        read_event_records,
        schema_sha256_for_connection,
        table_hashes_for_connection,
        validate_backlog_write,
        validate_decision_event,
        validate_event_projection,
    )
    from agent_mesh.store.sqlite import connect, initialize_schema

    if decision_receipt is not None and event.kind not in DECISION_EVENT_KINDS:
        _discard_decision_append_receipt(decision_receipt)
        raise EventProtocolError("decision append receipt cannot authorize another event domain")
    if capability_receipt is not None and event.kind != "dispatch_run_planned":
        _discard_capability_append_receipt(capability_receipt)
        raise EventProtocolError("capability append receipt cannot authorize another event domain")
    persisted_capability_receipt = event.payload.get("capability_receipt")
    if event.kind == "dispatch_run_planned" and isinstance(persisted_capability_receipt, dict):
        if capability_receipt is None or not _consume_capability_append_receipt(
            capability_receipt,
            lock_handle=lock_handle,
            events_path=agent_dir / "events.jsonl",
            persisted_receipt=persisted_capability_receipt,
        ):
            raise EventProtocolError("DISPATCH_ATTEMPT_CAPABILITY_AUTHORITY_REQUIRED")
    if event.kind not in DECISION_EVENT_KINDS | DISPATCH_EVENT_KINDS | {
        "backlog_item_upserted",
        "res_posted",
    }:
        return None

    config = config_from_agent_dir(agent_dir)
    if event.kind in DECISION_EVENT_KINDS:
        if (
            event.kind in {"decision_accepted", "decision_rejected"}
            and has_persisted_direct_human_authority(
                kind=event.kind,
                actor=event.actor,
                payload=event.payload,
            )
            and not has_current_direct_human_authority(
                kind=event.kind,
                actor=event.actor,
                payload=event.payload,
                approval_identities=config.decision_approval_identities,
                approval_authority_mode=config.decision_approval_authority_mode,
                approval_authority_revision=config.decision_approval_authority_revision,
            )
        ):
            raise EventProtocolError(
                "DECISION_DIRECT_HUMAN_AUTHORITY_INVALID: "
                f"{event.kind} must bind the current configured human authority"
            )
        if decision_receipt is None:
            rebuild_result = rebuild_all(config)
            expected_table_hashes = tuple(sorted(rebuild_result.table_hashes.items()))
            expected_schema_sha256 = rebuild_result.schema_sha256
        else:
            decision_state = _consume_decision_append_receipt(
                decision_receipt,
                lock_handle=lock_handle,
                config=config,
                projection_version=PROJECTION_VERSION,
                file_sha256=file_sha256,
            )
            if decision_state is None:
                raise EventProtocolError(
                    "prepared decision projection changed; retry the operation"
                )
            expected_table_hashes = decision_state.table_hashes
            expected_schema_sha256 = decision_state.schema_sha256

        conn = connect(config.db_path)
        try:
            initialize_schema(conn)
            conn.commit()
            conn.execute("BEGIN IMMEDIATE")
            current_schema_sha256 = schema_sha256_for_connection(conn)
            current_table_hashes = tuple(sorted(table_hashes_for_connection(conn).items()))
            if (
                current_schema_sha256 != expected_schema_sha256
                or current_table_hashes != expected_table_hashes
            ):
                raise EventProtocolError(
                    "prepared decision projection changed; retry the operation"
                )
            validate_decision_event(
                conn,
                kind=event.kind,
                entity_id=event.entity_id,
                thread_id=event.thread_id,
                actor=event.actor,
                occurred_utc=event.occurred_utc,
                payload=event.payload,
                participants=config.decision_approval_identities,
                approval_authority_mode=config.decision_approval_authority_mode,
                approval_authority_revision=config.decision_approval_authority_revision,
            )
            if event.kind == "decision_accepted":
                from agent_mesh.core.assurance import (
                    assurance_gate_binding,
                    evaluate_transition_assurance,
                )

                decision_row = conn.execute(
                    "SELECT human_id FROM decisions WHERE dec_ulid=?",
                    (event.entity_id,),
                ).fetchone()
                if decision_row is None:  # pragma: no cover - lifecycle validation owns this case.
                    raise EventProtocolError("decision approval target is unavailable")
                gate_evaluated_utc = (
                    datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
                )
                boundary_key = str(decision_row["human_id"])
                gate = evaluate_transition_assurance(
                    config,
                    conn,
                    boundary_kind="decision",
                    boundary_key=boundary_key,
                    now_utc=gate_evaluated_utc,
                )
                if not gate.allows_transition:
                    raise EventProtocolError(
                        "REVIEW_ASSURANCE_TRANSITION_BLOCKED: " + ",".join(gate.reason_codes)
                    )
                if gate.configured:
                    binding = assurance_gate_binding(
                        gate,
                        boundary_kind="decision",
                        boundary_key=boundary_key,
                        gate_evaluated_utc=gate_evaluated_utc,
                    )
                    supplied = event.payload.get("review_gate")
                    if supplied is not None and supplied != binding:
                        raise EventProtocolError("REVIEW_ASSURANCE_GATE_EVIDENCE_MISMATCH")
                    event.payload["review_gate"] = binding
        except BaseException:
            conn.rollback()
            conn.close()
            raise
        return conn

    rebuild_result = rebuild_all(config)
    if event.kind in DISPATCH_EVENT_KINDS or event.kind == "res_posted":
        if event.kind == "review_assurance_retired":
            if event.actor not in config.decision_approval_identities:
                raise EventProtocolError(
                    f"REVIEW_ASSURANCE_HUMAN_AUTHORITY_REQUIRED: {event.actor}"
                )
            if (
                event.payload.get("approval_authority_mode")
                != config.decision_approval_authority_mode
                or event.payload.get("approval_authority_revision")
                != config.decision_approval_authority_revision
            ):
                raise EventProtocolError(f"REVIEW_ASSURANCE_HUMAN_AUTHORITY_STALE: {event.actor}")
        if event.kind == "review_assurance_recorded":
            from agent_mesh.core.assurance import (
                AssuranceResolutionError,
                validate_typed_artifact_refs,
            )

            assurance_conn = connect(config.db_path)
            try:
                policy = assurance_conn.execute(
                    "SELECT artifact_contract_json, subject_json FROM dispatch_policies "
                    "WHERE policy_id=?",
                    (str(event.payload.get("policy_id") or ""),),
                ).fetchone()
                artifact_contract = (
                    json.loads(str(policy["artifact_contract_json"]))
                    if policy is not None
                    else None
                )
                subject = (
                    json.loads(str(policy["subject_json"]))
                    if policy is not None and policy["subject_json"] is not None
                    else None
                )
                validate_typed_artifact_refs(
                    config,
                    event.payload.get("artifact_refs", []),
                    artifact_contract=(
                        artifact_contract if isinstance(artifact_contract, dict) else None
                    ),
                    expected_subject_digest=(
                        str(subject.get("digest") or "") if isinstance(subject, dict) else ""
                    ),
                    expected_provenance=str(event.payload.get("management_level") or ""),
                )
            except AssuranceResolutionError as exc:
                raise EventProtocolError(str(exc)) from exc
            finally:
                assurance_conn.close()
        records = read_event_records(config.events_path)
        candidate = {
            "actor": event.actor,
            "actor_instance_id": event.actor_instance_id,
            "entity_id": event.entity_id,
            "event_id": event.event_id,
            "event_seq": (int(records[-1]["event_seq"]) + 1) if records else 1,
            "kind": event.kind,
            "occurred_utc": event.occurred_utc,
            "payload": event.payload,
            "thread_id": event.thread_id,
        }
        try:
            validate_event_projection(config, [*records, candidate])
        except (RuntimeError, ValueError) as exc:
            raise EventProtocolError(str(exc)) from exc
        return None
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
            conn.commit()
            conn.execute("BEGIN IMMEDIATE")
            current_schema_sha256 = schema_sha256_for_connection(conn)
            current_table_hashes = tuple(sorted(table_hashes_for_connection(conn).items()))
            if (
                current_schema_sha256 != rebuild_result.schema_sha256
                or current_table_hashes != tuple(sorted(rebuild_result.table_hashes.items()))
            ):
                raise EventProtocolError("prepared backlog projection changed; retry the operation")
            validate_backlog_write(conn, item_id, intent)
            current = conn.execute(
                "SELECT status FROM backlog_items WHERE id=?", (item_id,)
            ).fetchone()
            previous_status = str(current["status"] or "") if current is not None else "none"
            next_status = str(event.payload.get("status") or previous_status)
            transition = f"{previous_status}->{next_status}"
            if previous_status != next_status:
                from agent_mesh.core.assurance import (
                    assurance_gate_binding,
                    evaluate_transition_assurance,
                )

                gate_evaluated_utc = (
                    datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
                )
                gate = evaluate_transition_assurance(
                    config,
                    conn,
                    boundary_kind="backlog_transition",
                    boundary_key=transition,
                    now_utc=gate_evaluated_utc,
                )
                if not gate.allows_transition:
                    raise EventProtocolError(
                        "REVIEW_ASSURANCE_TRANSITION_BLOCKED: " + ",".join(gate.reason_codes)
                    )
                if gate.configured:
                    binding = assurance_gate_binding(
                        gate,
                        boundary_kind="backlog_transition",
                        boundary_key=transition,
                        gate_evaluated_utc=gate_evaluated_utc,
                    )
                    supplied = event.payload.get("review_gate")
                    if supplied is not None and supplied != binding:
                        raise EventProtocolError("REVIEW_ASSURANCE_GATE_EVIDENCE_MISMATCH")
                    event.payload["review_gate"] = binding
        except BaseException:
            conn.rollback()
            conn.close()
            raise
        return conn

    return None


def _consume_decision_append_receipt(
    receipt: _DecisionAppendReceipt,
    *,
    lock_handle: Any,
    config: Any,
    projection_version: str,
    file_sha256,
) -> _DecisionAppendState | None:
    from agent_mesh.core.lock import is_active_lock_handle

    with _DECISION_RECEIPT_LOCK:
        state = _DECISION_RECEIPTS.pop(receipt.token, None)
    if state is None:
        return None
    try:
        events_path = config.events_path.resolve()
        expected_lock_dir = (config.agent_dir / ".mail-lock").resolve()
        if (
            state.lock_handle is not lock_handle
            or state.pid != os.getpid()
            or state.thread_id != threading.get_ident()
            or lock_handle is None
            or not is_active_lock_handle(lock_handle, expected_lock_dir)
            or state.events_path != str(events_path)
            or state.db_path != str(config.db_path.resolve())
            or state.config_sha256 != _config_sha256(config)
            or state.projection_version != projection_version
        ):
            return None
        if state.source_log_sha256 != file_sha256(events_path):
            return None
        tail_line, tail_hash = read_tail_line(events_path)
        current_last_seq = _next_event_seq(tail_line) - 1
        if state.last_event_seq != current_last_seq or state.tail_event_hash != tail_hash:
            return None
        return state
    except (json.JSONDecodeError, OSError, RuntimeError, sqlite3.DatabaseError, ValueError):
        return None


def _consume_capability_append_receipt(
    receipt: _CapabilityAppendReceipt,
    *,
    lock_handle: Any,
    events_path: Path,
    persisted_receipt: dict[str, Any],
) -> bool:
    from agent_mesh.core.lock import is_active_lock_handle

    with _CAPABILITY_RECEIPT_LOCK:
        state = _CAPABILITY_RECEIPTS.pop(receipt.token, None)
    if state is None:
        return False
    expected_lock_dir = events_path.parent / ".mail-lock"
    if (
        state.pid != os.getpid()
        or state.thread_id != threading.get_ident()
        or state.events_path != str(events_path.resolve())
        or lock_handle is None
        or not is_active_lock_handle(lock_handle, expected_lock_dir)
    ):
        return False
    valid_until = _parse_receipt_time(state.valid_until_utc)
    if valid_until is None or valid_until <= datetime.now(UTC):
        return False
    if str(persisted_receipt.get("profile_digest") or "") != state.profile_digest:
        return False
    if str(persisted_receipt.get("drift_inputs_digest") or "") != state.drift_inputs_digest:
        return False
    return secrets.compare_digest(
        json.dumps(persisted_receipt, sort_keys=True, separators=(",", ":")),
        state.receipt_json,
    )


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


def _replay_event_placeholder(
    event: Event,
    agent_dir: Path,
    *,
    projection_conn: sqlite3.Connection | None = None,
) -> None:
    """Step [E] of the §6.2 pipeline — DB transaction + view regeneration."""
    from agent_mesh.config import config_from_agent_dir
    from agent_mesh.store.rebuild import apply_event, apply_record, file_sha256
    from agent_mesh.store.sqlite import set_meta
    from agent_mesh.views import render_all

    config = config_from_agent_dir(agent_dir)
    if projection_conn is None:
        apply_event(event, agent_dir=agent_dir)
    else:
        apply_record(
            event.to_dict(),
            config,
            conn=projection_conn,
            require_next=True,
            manage_transaction=False,
        )
        set_meta(projection_conn, "events_jsonl_sha", file_sha256(config.events_path))
        projection_conn.commit()
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
