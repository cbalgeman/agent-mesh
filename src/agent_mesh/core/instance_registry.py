"""Race-safe writers for the D006 runtime identity handshake and launch lifecycle."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Iterable
from zoneinfo import ZoneInfo

from agent_mesh.config import AgentMeshConfig, ConfigError
from agent_mesh.core.agent_instances import (
    INSTANCE_CONTRACT_VERSION,
    AgentInstance,
    AgentInstanceError,
    NormalizedRuntimeIdentityHandshake,
    RuntimeIdentityHandshake,
    bind_agent_instance,
    normalize_runtime_handshake,
    plan_runtime_identity,
    reduce_agent_instances,
    reset_agent_instance,
    selected_agent_instance,
    suspend_agent_instance,
)
from agent_mesh.core.events import Event, append_event, generate_event_id, utc_now
from agent_mesh.core.lock import acquire
from agent_mesh.core.recovery import recover
from agent_mesh.store.rebuild import read_event_records, rebuild_all


@dataclass(frozen=True)
class ResolvedRuntimeIdentity:
    """Internal launch binding plus the sole normal public handle."""

    instance_id: str
    handle: str
    participant: str
    binding_source: str
    action: str
    resumable: bool
    one_shot: bool


def preflight_runtime_identity(
    config: AgentMeshConfig,
    handshake: RuntimeIdentityHandshake,
    *,
    registrar: str,
    registration_origin: str,
    parent_instance_id: str = "",
    originating_run_id: str = "",
    privacy_key: bytes | None = None,
) -> None:
    """Validate a handshake and its exact current-state resolution without appending."""

    normalized = normalize_runtime_handshake(
        handshake,
        project_scope=config.store_id or config.project_key,
        privacy_key=privacy_key,
    )
    _validate_configured_handshake(config, normalized)
    instances = reduce_agent_instances(read_event_records(config.events_path))
    _validate_registrar(
        config,
        instances.values(),
        registrar=registrar,
        registration_origin=registration_origin,
        parent_instance_id=parent_instance_id,
        originating_run_id=originating_run_id,
    )
    plan = plan_runtime_identity(instances, normalized)
    _validate_rebound_lineage(
        plan.instance,
        registration_origin=registration_origin,
        parent_instance_id=parent_instance_id,
        registrar=registrar,
    )


def resolve_or_register_runtime_identity(
    config: AgentMeshConfig,
    handshake: RuntimeIdentityHandshake,
    *,
    registrar: str,
    registration_origin: str,
    parent_instance_id: str = "",
    originating_run_id: str = "",
    privacy_key: bytes | None = None,
    lock_acquired: bool = False,
) -> ResolvedRuntimeIdentity:
    """Resolve or register one trusted runtime while holding the canonical append lock."""

    if not lock_acquired:
        lock_handle = acquire(config.agent_dir / ".mail-lock")
        last_event_seq = None
        try:
            result = resolve_or_register_runtime_identity(
                config,
                handshake,
                registrar=registrar,
                registration_origin=registration_origin,
                parent_instance_id=parent_instance_id,
                originating_run_id=originating_run_id,
                privacy_key=privacy_key,
                lock_acquired=True,
            )
            records = read_event_records(config.events_path)
            last_event_seq = int(records[-1]["event_seq"]) if records else None
            return result
        finally:
            lock_handle.release(last_event_seq=last_event_seq)

    recover(config.events_path, config.agent_dir)
    rebuild_all(config)
    normalized = normalize_runtime_handshake(
        handshake,
        project_scope=config.store_id or config.project_key,
        privacy_key=privacy_key,
    )
    _validate_configured_handshake(config, normalized)
    records = read_event_records(config.events_path)
    instances = reduce_agent_instances(records)
    actor_instance_id = _validate_registrar(
        config,
        instances.values(),
        registrar=registrar,
        registration_origin=registration_origin,
        parent_instance_id=parent_instance_id,
        originating_run_id=originating_run_id,
    )
    plan = plan_runtime_identity(instances, normalized)
    _validate_rebound_lineage(
        plan.instance,
        registration_origin=registration_origin,
        parent_instance_id=parent_instance_id,
        registrar=registrar,
    )
    instance = plan.instance
    if plan.action == "registered":
        occurred_utc = utc_now()
        instance_id = _allocate_instance_id(
            instances.values(),
            occurred_utc=occurred_utc,
            timezone=config.project_timezone,
        )
        registration = Event(
            event_id=generate_event_id(),
            occurred_utc=occurred_utc,
            actor=registrar,
            actor_instance_id=actor_instance_id,
            kind="agent_instance_registered",
            entity_id=instance_id,
            thread_id=instance_id,
            payload={
                "instance_contract_version": INSTANCE_CONTRACT_VERSION,
                "id": instance_id,
                "participant": normalized.participant,
                "provider": normalized.provider,
                "label": plan.handle,
                "durable_role": normalized.durable_role,
                "workstream": normalized.workstream,
                "runtime_profile": normalized.runtime_profile,
                "role": normalized.role,
                "capabilities": list(normalized.capabilities),
                "permission_mode": normalized.permission_mode,
                "authentication_mode": normalized.authentication_mode,
                "billing_mode": normalized.billing_mode,
                "adapter_trust": normalized.adapter_trust,
                "session_identity_mode": normalized.session_identity_mode,
                "resumable": normalized.resumable,
                "concurrent_attachment": normalized.concurrent_attachment,
                "terminal_observation": normalized.terminal_observation,
                "external_session_ref_digest": normalized.external_session_ref_digest,
                "launch_attempt_digest": normalized.launch_attempt_digest,
                "lifecycle_disposition": normalized.lifecycle_disposition,
                "registrar": registrar,
                "registration_origin": registration_origin,
                "parent_instance_id": parent_instance_id,
                "originating_run_id": originating_run_id,
            },
        )
        _append_with_origin_context(
            config,
            registration,
            registration_origin=registration_origin,
        )
        instances = reduce_agent_instances(read_event_records(config.events_path))
        instance = instances[instance_id]
    if instance is None:  # pragma: no cover - pure planner contract safeguard
        raise RuntimeError("runtime identity planner returned no instance")

    if plan.binding_source == "launch-attempt-retry" and _binding_exists(
        read_event_records(config.events_path),
        instance_id=instance.id,
        launch_attempt_digest=normalized.launch_attempt_digest,
    ):
        return _resolved_identity(instance, plan)

    resumed = bool(
        plan.action == "rebound"
        and instance.resumable
        and instance.session_identity_mode == "exact"
        and normalized.external_session_ref_digest
        == instance.external_session_ref_digest
    )
    binding = Event(
        event_id=generate_event_id(),
        actor=registrar,
        actor_instance_id=actor_instance_id,
        kind="agent_instance_bound",
        entity_id=instance.id,
        thread_id=instance.id,
        payload={
            "id": instance.id,
            "binding_source": plan.binding_source,
            "runtime_profile": normalized.runtime_profile,
            "external_session_ref_digest": normalized.external_session_ref_digest,
            "launch_attempt_digest": normalized.launch_attempt_digest,
            "lifecycle_disposition": normalized.lifecycle_disposition,
            "provider_context_resumed": resumed,
        },
    )
    _append_with_origin_context(config, binding, registration_origin=registration_origin)
    return _resolved_identity(instance, plan)


def append_instance_launch_bound(
    config: AgentMeshConfig,
    identity: ResolvedRuntimeIdentity,
    *,
    run_id: str,
    dispatcher: str,
    dispatcher_instance_id: str,
    lock_acquired: bool,
) -> None:
    records = read_event_records(config.events_path)
    existing = _instance_run_event(
        records,
        kind="agent_instance_launch_bound",
        instance_id=identity.instance_id,
        run_id=run_id,
    )
    if existing is not None:
        return
    collision = _run_event_owner(records, run_id=run_id)
    if collision and collision != identity.instance_id:
        raise AgentInstanceError("AGENT_INSTANCE_RUN_BINDING_COLLISION")
    _append_dispatcher_lifecycle(
        config,
        Event(
            event_id=generate_event_id(),
            actor=dispatcher,
            actor_instance_id=dispatcher_instance_id,
            kind="agent_instance_launch_bound",
            entity_id=identity.instance_id,
            thread_id=identity.instance_id,
            payload={"id": identity.instance_id, "run_id": run_id},
        ),
        lock_acquired=lock_acquired,
    )


def append_instance_launch_released(
    config: AgentMeshConfig,
    identity: ResolvedRuntimeIdentity,
    *,
    run_id: str,
    outcome: str,
    dispatcher: str,
    dispatcher_instance_id: str,
    lock_acquired: bool,
) -> None:
    records = read_event_records(config.events_path)
    existing = _instance_run_event(
        records,
        kind="agent_instance_launch_released",
        instance_id=identity.instance_id,
        run_id=run_id,
    )
    if existing is not None:
        existing_outcome = str(existing.get("payload", {}).get("outcome", ""))
        if existing_outcome != outcome:
            raise AgentInstanceError("AGENT_INSTANCE_RELEASE_OUTCOME_CONFLICT")
        return
    if not _dispatch_terminal_suffix_exists(records, run_id):
        raise AgentInstanceError("AGENT_INSTANCE_RELEASE_BEFORE_DISPATCH_TERMINAL")
    _append_dispatcher_lifecycle(
        config,
        Event(
            event_id=generate_event_id(),
            actor=dispatcher,
            actor_instance_id=dispatcher_instance_id,
            kind="agent_instance_launch_released",
            entity_id=identity.instance_id,
            thread_id=identity.instance_id,
            payload={"id": identity.instance_id, "run_id": run_id, "outcome": outcome},
        ),
        lock_acquired=lock_acquired,
    )


def append_instance_terminal(
    config: AgentMeshConfig,
    identity: ResolvedRuntimeIdentity,
    *,
    run_id: str,
    outcome: str,
    dispatcher: str,
    dispatcher_instance_id: str,
    lock_acquired: bool,
) -> None:
    if not identity.one_shot:
        return
    records = read_event_records(config.events_path)
    existing = _instance_run_event(
        records,
        kind="agent_instance_terminal",
        instance_id=identity.instance_id,
        run_id=run_id,
    )
    if existing is not None:
        existing_outcome = str(existing.get("payload", {}).get("outcome", ""))
        if existing_outcome != outcome:
            raise AgentInstanceError("AGENT_INSTANCE_TERMINAL_OUTCOME_CONFLICT")
        return
    _append_dispatcher_lifecycle(
        config,
        Event(
            event_id=generate_event_id(),
            actor=dispatcher,
            actor_instance_id=dispatcher_instance_id,
            kind="agent_instance_terminal",
            entity_id=identity.instance_id,
            thread_id=identity.instance_id,
            payload={"id": identity.instance_id, "run_id": run_id, "outcome": outcome},
        ),
        lock_acquired=lock_acquired,
    )


def reconcile_instance_terminal_suffix(
    config: AgentMeshConfig,
    identity: ResolvedRuntimeIdentity,
    *,
    run_id: str,
    outcome: str,
    dispatcher: str,
    dispatcher_instance_id: str,
    lock_acquired: bool,
) -> None:
    """Idempotently finish the instance suffix after dispatch is terminal and released."""

    append_instance_launch_released(
        config,
        identity,
        run_id=run_id,
        outcome=outcome,
        dispatcher=dispatcher,
        dispatcher_instance_id=dispatcher_instance_id,
        lock_acquired=lock_acquired,
    )
    append_instance_terminal(
        config,
        identity,
        run_id=run_id,
        outcome=outcome,
        dispatcher=dispatcher,
        dispatcher_instance_id=dispatcher_instance_id,
        lock_acquired=lock_acquired,
    )


def _append_dispatcher_lifecycle(
    config: AgentMeshConfig, event: Event, *, lock_acquired: bool
) -> None:
    append_event(config.events_path, event, lock_acquired=lock_acquired)


def _append_with_origin_context(
    config: AgentMeshConfig,
    event: Event,
    *,
    registration_origin: str,
) -> None:
    token = (
        suspend_agent_instance()
        if registration_origin == "integration" or not event.actor_instance_id
        else bind_agent_instance(event.actor_instance_id)
    )
    try:
        append_event(config.events_path, event, lock_acquired=True)
    finally:
        reset_agent_instance(token)


def _validate_configured_handshake(
    config: AgentMeshConfig, handshake: NormalizedRuntimeIdentityHandshake
) -> None:
    if handshake.participant not in config.participants:
        raise ConfigError(f"PARTICIPANT_UNKNOWN: {handshake.participant}")
    profile = config.runtime_profiles.get(handshake.runtime_profile)
    if profile is None or not profile.enabled:
        raise ConfigError(f"RUNTIME_PROFILE_UNKNOWN: {handshake.runtime_profile}")
    expected = {
        "participant": profile.target,
        "provider": profile.provider,
        "durable_role": profile.durable_role,
        "role": profile.role,
        "capabilities": profile.required_capabilities,
        "permission_mode": profile.permission_mode,
        "authentication_mode": profile.authentication_mode,
        "billing_mode": profile.billing_mode,
        "adapter_trust": profile.adapter_trust,
        "session_identity_mode": profile.session_identity_mode,
        "resumable": profile.resumable,
        "concurrent_attachment": profile.concurrent_attachment,
        "terminal_observation": profile.terminal_observation,
    }
    actual = {
        "participant": handshake.participant,
        "provider": handshake.provider,
        "durable_role": handshake.durable_role,
        "role": handshake.role,
        "capabilities": handshake.capabilities,
        "permission_mode": handshake.permission_mode,
        "authentication_mode": handshake.authentication_mode,
        "billing_mode": handshake.billing_mode,
        "adapter_trust": handshake.adapter_trust,
        "session_identity_mode": handshake.session_identity_mode,
        "resumable": handshake.resumable,
        "concurrent_attachment": handshake.concurrent_attachment,
        "terminal_observation": handshake.terminal_observation,
    }
    mismatches = [name for name, value in expected.items() if actual[name] != value]
    if mismatches:
        raise AgentInstanceError(
            "AGENT_INSTANCE_PROFILE_MISMATCH: " + ", ".join(sorted(mismatches))
        )


def _validate_registrar(
    config: AgentMeshConfig,
    instances: Iterable[AgentInstance],
    *,
    registrar: str,
    registration_origin: str,
    parent_instance_id: str,
    originating_run_id: str,
) -> str:
    by_id = {instance.id: instance for instance in instances}
    if registration_origin == "integration":
        if registrar not in config.identity.integration_registrars:
            raise AgentInstanceError("AGENT_INSTANCE_REGISTRAR_UNAUTHORIZED")
        if parent_instance_id or originating_run_id:
            raise AgentInstanceError("AGENT_INSTANCE_INTEGRATION_LINEAGE_INVALID")
        return ""
    if registration_origin != "dispatch":
        raise AgentInstanceError("AGENT_INSTANCE_REGISTRATION_ORIGIN_INVALID")
    if not parent_instance_id:
        if (
            registrar not in config.decision_approval_identities
            or selected_agent_instance()
            or not originating_run_id
        ):
            raise AgentInstanceError("AGENT_INSTANCE_HUMAN_DISPATCH_AUTHORITY_INVALID")
        return ""
    parent = by_id.get(parent_instance_id)
    if parent is None:
        raise AgentInstanceError("AGENT_INSTANCE_PARENT_UNKNOWN")
    if parent.status != "active":
        raise AgentInstanceError("AGENT_INSTANCE_PARENT_NOT_ACTIVE")
    if registrar != parent.participant:
        raise AgentInstanceError("AGENT_INSTANCE_REGISTRAR_MISMATCH")
    if not originating_run_id:
        raise AgentInstanceError("AGENT_INSTANCE_ORIGINATING_RUN_REQUIRED")
    selected = selected_agent_instance()
    if selected not in {parent.id, *parent.aliases}:
        raise AgentInstanceError("AGENT_INSTANCE_REGISTRAR_BINDING_REQUIRED")
    return parent.id


def _validate_rebound_lineage(
    instance: AgentInstance | None,
    *,
    registration_origin: str,
    parent_instance_id: str,
    registrar: str,
) -> None:
    if instance is None:
        return
    if registration_origin == "dispatch" and (
        instance.registration_origin != "dispatch"
        or instance.parent_instance_id != parent_instance_id
        or instance.registrar != registrar
    ):
        raise AgentInstanceError("AGENT_INSTANCE_DISPATCH_LINEAGE_CONFLICT")
    if registration_origin == "integration" and (
        instance.registration_origin != "integration"
        or instance.registrar != registrar
    ):
        raise AgentInstanceError("AGENT_INSTANCE_INTEGRATION_LINEAGE_CONFLICT")


def _allocate_instance_id(
    instances: Iterable[AgentInstance], *, occurred_utc: str, timezone: str
) -> str:
    instant = datetime.fromisoformat(occurred_utc.replace("Z", "+00:00"))
    if instant.tzinfo is None:
        instant = instant.replace(tzinfo=UTC)
    date_stamp = instant.astimezone(ZoneInfo(timezone)).strftime("%Y%m%d")
    pattern = re.compile(rf"^AI-{date_stamp}-(\d+)$")
    sequences = [
        int(match.group(1))
        for instance in instances
        if (match := pattern.fullmatch(instance.id)) is not None
    ]
    return f"AI-{date_stamp}-{max(sequences, default=0) + 1:02d}"


def _dispatch_terminal_suffix_exists(records: list[dict], run_id: str) -> bool:
    terminal = any(
        record.get("entity_id") == run_id
        and record.get("kind")
        in {"dispatch_run_completed", "dispatch_run_failed", "dispatch_run_terminated"}
        for record in records
    )
    released = any(
        record.get("kind") == "dispatch_lease_released"
        and isinstance(record.get("payload"), dict)
        and record["payload"].get("run_id") == run_id
        for record in records
    )
    return terminal and released


def _resolved_identity(instance: AgentInstance, plan) -> ResolvedRuntimeIdentity:
    return ResolvedRuntimeIdentity(
        instance_id=instance.id,
        handle=instance.label,
        participant=instance.participant,
        binding_source=plan.binding_source,
        action=plan.action,
        resumable=instance.resumable,
        one_shot=not instance.resumable,
    )


def _binding_exists(
    records: list[dict], *, instance_id: str, launch_attempt_digest: str
) -> bool:
    return any(
        record.get("kind") == "agent_instance_bound"
        and record.get("entity_id") == instance_id
        and isinstance(record.get("payload"), dict)
        and record["payload"].get("launch_attempt_digest")
        == launch_attempt_digest
        for record in records
    )


def _instance_run_event(
    records: list[dict], *, kind: str, instance_id: str, run_id: str
) -> dict | None:
    return next(
        (
            record
            for record in records
            if record.get("kind") == kind
            and record.get("entity_id") == instance_id
            and isinstance(record.get("payload"), dict)
            and record["payload"].get("run_id") == run_id
        ),
        None,
    )


def _run_event_owner(records: list[dict], *, run_id: str) -> str:
    for record in records:
        if record.get("kind") != "agent_instance_launch_bound":
            continue
        payload = record.get("payload", {})
        if isinstance(payload, dict) and payload.get("run_id") == run_id:
            return str(record.get("entity_id", ""))
    return ""
