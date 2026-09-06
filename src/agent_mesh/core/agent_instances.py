"""Durable, project-local identities for long-lived AI-agent work instances."""

from __future__ import annotations

import contextvars
import hashlib
import hmac
import os
import re
from dataclasses import dataclass, field, replace
from typing import Any, Iterable, Mapping

INSTANCE_ENV_VAR = "AGENT_MESH_INSTANCE_ID"
RESERVED_INSTANCE_ENV_VARS = frozenset(
    {
        INSTANCE_ENV_VAR,
        "AGENT_MESH_INSTANCE_BINDING",
        "AGENT_MESH_INSTANCE_BINDING_CREDENTIAL",
        "AGENT_MESH_LAUNCH_ATTEMPT_KEY",
        "AGENT_MESH_PROVIDER_SESSION_REF",
        "AGENT_MESH_REGISTRAR_CREDENTIAL",
    }
)
INSTANCE_ID_RE = re.compile(r"^AI-\d{8}-\d{2,}$")
INSTANCE_LABEL_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
INSTANCE_HANDLE_COMPONENT_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
SESSION_REF_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
INSTANCE_EVENT_KINDS = frozenset(
    {
        "agent_instance_registered",
        "agent_instance_metadata_updated",
        "agent_instance_bound",
        "agent_instance_launch_bound",
        "agent_instance_launch_released",
        "agent_instance_terminal",
        "agent_instance_retired",
    }
)
INSTANCE_TERMINAL_OUTCOMES = frozenset(
    {
        "completed",
        "failed",
        "timeout",
        "launch_error",
        "cancelled",
        "parent_loss",
        "recovered",
    }
)
INSTANCE_LIFECYCLE_DISPOSITIONS = frozenset({"new", "resumed", "unknown"})
INSTANCE_REGISTRATION_ORIGINS = frozenset({"integration", "dispatch", "manual", "legacy"})
INSTANCE_ADAPTER_TRUST_SOURCES = frozenset(
    {"configured", "project-local", "dispatcher", "unmanaged", "legacy"}
)
INSTANCE_SESSION_IDENTITY_MODES = frozenset({"exact", "none", "legacy"})
INSTANCE_TERMINAL_OBSERVATION_MODES = frozenset({"process", "provider", "none", "legacy"})
INSTANCE_CONTRACT_VERSION = 2
ACTOR_SHADOW_FIELDS = {
    "req_created": "from",
    "res_posted": "from",
    "req_status_changed": "actor",
    "decision_accepted": "accepted_by",
    "backlog_event_recorded": "actor",
}
MUTABLE_INSTANCE_FIELDS = (
    "label",
    "workstream",
    "runtime_profile",
    "external_session_ref_digest",
)

_BOUND_INSTANCE: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "agent_mesh_instance_id", default=None
)


class AgentInstanceError(ValueError):
    """An instance registration, address, or attribution is invalid."""


@dataclass(frozen=True)
class AgentInstance:
    id: str
    participant: str
    provider: str
    label: str
    workstream: str
    runtime_profile: str
    external_session_ref_digest: str
    status: str
    created_utc: str
    updated_utc: str
    retired_utc: str = ""
    aliases: tuple[str, ...] = ()
    durable_role: str = ""
    role: str = ""
    capabilities: tuple[str, ...] = ()
    permission_mode: str = ""
    authentication_mode: str = ""
    billing_mode: str = ""
    adapter_trust: str = "legacy"
    session_identity_mode: str = "legacy"
    resumable: bool = False
    concurrent_attachment: bool = False
    terminal_observation: str = "legacy"
    registrar: str = ""
    registration_origin: str = "legacy"
    parent_instance_id: str = ""
    originating_run_id: str = ""
    launch_attempt_digest: str = ""
    launch_attempt_digests: tuple[str, ...] = ()
    launch_attempt_dispositions: tuple[tuple[str, str], ...] = ()
    last_binding_source: str = ""
    active_launch_run_ids: tuple[str, ...] = ()
    released_launches: tuple[tuple[str, str], ...] = ()
    terminal_outcome: str = ""
    terminal_utc: str = ""


@dataclass(frozen=True)
class RuntimeIdentityHandshake:
    """Trusted, in-memory runtime identity statement.

    Raw provider and launch references are deliberately excluded from repr and comparison so an
    exception or diagnostic cannot accidentally disclose them. Integrations may instead supply an
    already-scoped digest.
    """

    participant: str
    provider: str
    runtime_profile: str
    durable_role: str
    role: str
    capabilities: tuple[str, ...]
    permission_mode: str
    authentication_mode: str
    billing_mode: str
    adapter_trust: str
    session_identity_mode: str
    resumable: bool
    concurrent_attachment: bool
    terminal_observation: str
    lifecycle_disposition: str
    workstream: str = ""
    external_session_ref: str = field(default="", repr=False, compare=False)
    external_session_ref_digest: str = ""
    launch_attempt_key: str = field(default="", repr=False, compare=False)
    launch_attempt_digest: str = ""


@dataclass(frozen=True)
class NormalizedRuntimeIdentityHandshake:
    participant: str
    provider: str
    runtime_profile: str
    durable_role: str
    role: str
    capabilities: tuple[str, ...]
    permission_mode: str
    authentication_mode: str
    billing_mode: str
    adapter_trust: str
    session_identity_mode: str
    resumable: bool
    concurrent_attachment: bool
    terminal_observation: str
    lifecycle_disposition: str
    workstream: str
    external_session_ref_digest: str
    launch_attempt_digest: str


@dataclass(frozen=True)
class RuntimeIdentityPlan:
    action: str
    binding_source: str
    handle: str
    instance: AgentInstance | None = None


def bind_agent_instance(identifier: str | None) -> contextvars.Token[str | None]:
    """Bind a CLI invocation to one instance without mutating process-global environment."""

    value = identifier.strip() if identifier else None
    return _BOUND_INSTANCE.set(value or None)


def reset_agent_instance(token: contextvars.Token[str | None]) -> None:
    _BOUND_INSTANCE.reset(token)


def suspend_agent_instance() -> contextvars.Token[str | None]:
    """Temporarily suppress invocation and environment instance inheritance."""

    return _BOUND_INSTANCE.set("")


def selected_agent_instance(explicit: str = "") -> str:
    """Return explicit, invocation-bound, or environment-provided instance identity."""

    if explicit.strip():
        return explicit.strip()
    bound = _BOUND_INSTANCE.get()
    if bound is not None:
        return bound
    return os.environ.get(INSTANCE_ENV_VAR, "").strip()


def resolve_authoring_actor(
    records: Iterable[Mapping[str, Any]],
    *,
    default_actor: str,
    explicit_actor: str | None = None,
) -> str:
    """Resolve the canonical actor for the currently bound authoring instance."""

    requested = (explicit_actor if explicit_actor is not None else default_actor).strip()
    selected = selected_agent_instance()
    if not selected:
        return requested
    instance = resolve_agent_instance_from_records(records, selected)
    if instance is None:
        raise AgentInstanceError(f"AGENT_INSTANCE_UNKNOWN: {selected}")
    _require_active(instance)
    if explicit_actor is not None and requested != instance.participant:
        raise AgentInstanceError(
            f"AGENT_INSTANCE_ACTOR_MISMATCH: {instance.label} belongs to "
            f"{instance.participant!r}, not {requested!r}"
        )
    return instance.participant


def validate_actor_shadow(kind: str, actor: str, payload: Mapping[str, Any]) -> None:
    """Reject payload authorship fields that disagree with the event envelope."""

    field = ACTOR_SHADOW_FIELDS.get(kind)
    if field is None or field not in payload:
        return
    shadow = str(payload.get(field) or "").strip()
    if shadow != actor:
        raise AgentInstanceError(
            f"EVENT_ACTOR_MISMATCH: {kind} payload.{field}={shadow!r} "
            f"does not match event actor {actor!r}"
        )


def normalize_instance_label(value: str) -> str:
    label = value.strip().lower()
    if not INSTANCE_LABEL_RE.fullmatch(label):
        raise AgentInstanceError(
            "instance handle must use 1-63 lowercase letters, numbers, or interior hyphens"
        )
    return label


def normalize_handle_component(value: str, *, field_name: str) -> str:
    component = value.strip().lower()
    if not INSTANCE_HANDLE_COMPONENT_RE.fullmatch(component):
        raise AgentInstanceError(
            f"{field_name} must use 1-63 lowercase letters, numbers, or interior hyphens"
        )
    return component


def normalize_new_instance_handle(value: str, *, participant: str) -> str:
    """Validate the public ``<participant>-<durable-role>`` handle grammar.

    Historical labels remain replayable through :func:`normalize_instance_label`;
    only newly allocated or renamed handles use this stricter contract.
    """

    handle = normalize_instance_label(value)
    participant_component = normalize_handle_component(
        participant, field_name="instance participant"
    )
    if not handle.startswith(participant_component + "-"):
        raise AgentInstanceError(
            "instance handle must use <participant>-<durable-role>"
        )
    durable_suffix = handle[len(participant_component) + 1 :]
    if not durable_suffix:
        raise AgentInstanceError(
            "instance handle must include a durable role after the participant"
        )
    return handle


def public_instance_handle(instance: AgentInstance, *, project_key: str = "") -> str:
    """Return the only normal user-facing instance identity."""

    return f"{project_key}/{instance.label}" if project_key else instance.label


def allocate_instance_handle(
    instances: Mapping[str, AgentInstance],
    *,
    participant: str,
    durable_role: str,
    workstream: str = "",
) -> str:
    """Allocate a D006 public handle while the caller owns the canonical lock."""

    participant_component = normalize_handle_component(
        participant, field_name="instance participant"
    )
    role_component = normalize_handle_component(durable_role, field_name="durable role")
    workstream_component = (
        normalize_handle_component(workstream, field_name="durable workstream")
        if workstream.strip()
        else ""
    )
    base = f"{participant_component}-{role_component}"
    qualified = f"{base}-{workstream_component}" if workstream_component else base
    reserved = {alias for instance in instances.values() for alias in instance.aliases}
    for candidate in dict.fromkeys((base, qualified)):
        if len(candidate) > 63:
            continue
        if candidate not in reserved:
            return candidate
    ordinal = 2
    while True:
        candidate = f"{qualified}-{ordinal}"
        if len(candidate) > 63:
            raise AgentInstanceError(
                "AGENT_INSTANCE_HANDLE_TOO_LONG: participant, durable role, and workstream "
                "cannot fit the 63-character handle limit"
            )
        if candidate not in reserved:
            return candidate
        ordinal += 1


def digest_external_session_ref(value: str | None) -> str:
    """Hash a provider session reference so canonical state never stores the raw identifier."""

    normalized = (value or "").strip()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest() if normalized else ""


def validate_external_session_ref_digest(value: str) -> str:
    digest = value.strip()
    if digest and not SESSION_REF_DIGEST_RE.fullmatch(digest):
        raise AgentInstanceError("external session reference digest must be SHA-256 hex")
    return digest


def scoped_reference_digest(
    value: str,
    *,
    project_scope: str,
    purpose: str,
    privacy_key: bytes | None = None,
) -> str:
    """Return a project- and purpose-scoped digest without ever formatting the raw value."""

    if not value:
        return ""
    prefix = f"agent-mesh:{project_scope}:{purpose}:".encode("utf-8")
    material = prefix + value.encode("utf-8")
    if privacy_key is not None:
        if len(privacy_key) < 16:
            raise AgentInstanceError("AGENT_INSTANCE_PRIVACY_KEY_WEAK")
        return hmac.new(privacy_key, material, hashlib.sha256).hexdigest()
    return hashlib.sha256(material).hexdigest()


def normalize_runtime_handshake(
    handshake: RuntimeIdentityHandshake,
    *,
    project_scope: str,
    privacy_key: bytes | None = None,
) -> NormalizedRuntimeIdentityHandshake:
    """Validate and privacy-normalize a trusted adapter handshake."""

    participant = normalize_handle_component(
        handshake.participant, field_name="instance participant"
    )
    durable_role = normalize_handle_component(handshake.durable_role, field_name="durable role")
    workstream = handshake.workstream.strip().lower()
    if workstream:
        workstream = normalize_handle_component(workstream, field_name="durable workstream")
    provider = _required_text(handshake.provider, "provider").lower()
    runtime_profile = _required_text(handshake.runtime_profile, "runtime profile")
    role = _required_text(handshake.role, "runtime role")
    permission_mode = _required_text(handshake.permission_mode, "permission mode")
    authentication_mode = _required_text(handshake.authentication_mode, "authentication mode")
    billing_mode = _required_text(handshake.billing_mode, "billing mode")
    capabilities = _normalize_string_tuple(handshake.capabilities, field_name="capabilities")
    if not capabilities:
        raise AgentInstanceError("AGENT_INSTANCE_HANDSHAKE_INVALID: capabilities must not be empty")
    adapter_trust = handshake.adapter_trust.strip().lower()
    if adapter_trust not in INSTANCE_ADAPTER_TRUST_SOURCES - {"legacy", "unmanaged"}:
        raise AgentInstanceError("AGENT_INSTANCE_ADAPTER_UNTRUSTED")
    session_identity_mode = handshake.session_identity_mode.strip().lower()
    if session_identity_mode not in INSTANCE_SESSION_IDENTITY_MODES - {"legacy"}:
        raise AgentInstanceError("AGENT_INSTANCE_SESSION_MODE_INVALID")
    terminal_observation = handshake.terminal_observation.strip().lower()
    if terminal_observation not in INSTANCE_TERMINAL_OBSERVATION_MODES - {"legacy"}:
        raise AgentInstanceError("AGENT_INSTANCE_TERMINAL_OBSERVATION_INVALID")
    disposition = handshake.lifecycle_disposition.strip().lower()
    if disposition not in INSTANCE_LIFECYCLE_DISPOSITIONS:
        raise AgentInstanceError("AGENT_INSTANCE_DISPOSITION_INVALID")
    if handshake.external_session_ref and handshake.external_session_ref_digest:
        raise AgentInstanceError("AGENT_INSTANCE_SESSION_REF_CONFLICT")
    if handshake.launch_attempt_key and handshake.launch_attempt_digest:
        raise AgentInstanceError("AGENT_INSTANCE_LAUNCH_KEY_CONFLICT")
    if handshake.external_session_ref:
        if privacy_key is None and not _looks_high_entropy(handshake.external_session_ref):
            raise AgentInstanceError("AGENT_INSTANCE_SESSION_REF_KEY_REQUIRED")
        session_digest = scoped_reference_digest(
            handshake.external_session_ref,
            project_scope=project_scope,
            purpose="provider-session",
            privacy_key=privacy_key,
        )
    else:
        session_digest = validate_external_session_ref_digest(
            handshake.external_session_ref_digest
        )
    if handshake.launch_attempt_key:
        launch_digest = scoped_reference_digest(
            handshake.launch_attempt_key,
            project_scope=project_scope,
            purpose="launch-attempt",
        )
    else:
        launch_digest = validate_external_session_ref_digest(handshake.launch_attempt_digest)
    if session_identity_mode == "exact" and not session_digest:
        raise AgentInstanceError("AGENT_INSTANCE_EXACT_SESSION_REQUIRED")
    if session_identity_mode == "none" and session_digest:
        raise AgentInstanceError("AGENT_INSTANCE_SESSION_MODE_CONFLICT")
    if handshake.resumable and session_identity_mode != "exact":
        raise AgentInstanceError("AGENT_INSTANCE_RESUME_UNSUPPORTED")
    if disposition == "resumed" and (not handshake.resumable or not session_digest):
        raise AgentInstanceError("AGENT_INSTANCE_RESUME_UNSUPPORTED")
    if terminal_observation == "none" and not handshake.resumable:
        raise AgentInstanceError("AGENT_INSTANCE_TERMINAL_OBSERVATION_REQUIRED")
    return NormalizedRuntimeIdentityHandshake(
        participant=participant,
        provider=provider,
        runtime_profile=runtime_profile,
        durable_role=durable_role,
        role=role,
        capabilities=capabilities,
        permission_mode=permission_mode,
        authentication_mode=authentication_mode,
        billing_mode=billing_mode,
        adapter_trust=adapter_trust,
        session_identity_mode=session_identity_mode,
        resumable=bool(handshake.resumable),
        concurrent_attachment=bool(handshake.concurrent_attachment),
        terminal_observation=terminal_observation,
        lifecycle_disposition=disposition,
        workstream=workstream,
        external_session_ref_digest=session_digest,
        launch_attempt_digest=launch_digest,
    )


def plan_runtime_identity(
    instances: Mapping[str, AgentInstance],
    handshake: NormalizedRuntimeIdentityHandshake,
) -> RuntimeIdentityPlan:
    """Pure D006 new/resumed/unknown resolver. It never mutates canonical state."""

    if handshake.launch_attempt_digest:
        retry_matches = [
            instance
            for instance in instances.values()
            if handshake.launch_attempt_digest
            in (instance.launch_attempt_digests or (instance.launch_attempt_digest,))
        ]
        if len(retry_matches) > 1:
            raise AgentInstanceError("AGENT_INSTANCE_LAUNCH_KEY_COLLISION")
        if retry_matches:
            instance = retry_matches[0]
            _require_compatible_instance(instance, handshake)
            _require_active(instance)
            return RuntimeIdentityPlan(
                action="rebound",
                binding_source="launch-attempt-retry",
                handle=instance.label,
                instance=instance,
            )

    session_matches: list[AgentInstance] = []
    if handshake.external_session_ref_digest:
        session_matches = [
            instance
            for instance in instances.values()
            if instance.participant == handshake.participant
            and instance.provider == handshake.provider
            and instance.external_session_ref_digest
            == handshake.external_session_ref_digest
        ]
        incompatible = [
            instance
            for instance in session_matches
            if not _instance_is_compatible(instance, handshake)
        ]
        if incompatible:
            raise AgentInstanceError("AGENT_INSTANCE_SESSION_METADATA_CONFLICT")
        active_matches = [instance for instance in session_matches if instance.status == "active"]
        if len(active_matches) > 1:
            handles = ", ".join(sorted(instance.label for instance in active_matches))
            raise AgentInstanceError(f"AGENT_INSTANCE_CONTINUITY_AMBIGUOUS: {handles}")
        if session_matches and not active_matches:
            raise AgentInstanceError("AGENT_INSTANCE_SESSION_NOT_ACTIVE")
        if active_matches:
            if handshake.lifecycle_disposition == "new":
                raise AgentInstanceError("AGENT_INSTANCE_NEW_SESSION_CONTRADICTION")
            instance = active_matches[0]
            return RuntimeIdentityPlan(
                action="rebound",
                binding_source="session-digest",
                handle=instance.label,
                instance=instance,
            )

    if handshake.lifecycle_disposition == "resumed":
        raise AgentInstanceError("AGENT_INSTANCE_RESUME_MATCH_REQUIRED")
    handle = allocate_instance_handle(
        instances,
        participant=handshake.participant,
        durable_role=handshake.durable_role,
        workstream=handshake.workstream,
    )
    return RuntimeIdentityPlan(
        action="registered",
        binding_source=(
            "authoritative-new"
            if handshake.lifecycle_disposition == "new"
            else "unknown-no-match"
        ),
        handle=handle,
    )


def reduce_agent_instances(records: Iterable[Mapping[str, Any]]) -> dict[str, AgentInstance]:
    """Fold instance lifecycle events into current state, retaining historical label aliases."""

    instances: dict[str, AgentInstance] = {}
    for record in sorted(records, key=lambda item: int(item.get("event_seq", 0))):
        kind = str(record.get("kind", ""))
        if kind not in INSTANCE_EVENT_KINDS:
            continue
        payload = record.get("payload", {})
        if not isinstance(payload, Mapping):
            raise AgentInstanceError(f"{kind} payload must be an object")
        identifier = str(payload.get("id") or record.get("entity_id") or "")
        if kind == "agent_instance_registered":
            if not INSTANCE_ID_RE.fullmatch(identifier):
                raise AgentInstanceError(f"invalid agent instance ID: {identifier!r}")
            if identifier in instances:
                raise AgentInstanceError(f"AGENT_INSTANCE_ID_COLLISION: {identifier}")
            participant = str(payload.get("participant", "")).strip()
            provider = str(payload.get("provider", "")).strip().lower()
            if not participant or not provider:
                raise AgentInstanceError("instance participant and provider must be non-empty")
            contract_version = _contract_version(payload)
            label = (
                normalize_new_instance_handle(
                    str(payload.get("label", "")), participant=participant
                )
                if contract_version >= INSTANCE_CONTRACT_VERSION
                else normalize_instance_label(str(payload.get("label", "")))
            )
            _ensure_label_available(instances, label)
            capabilities = _payload_string_tuple(
                payload, "capabilities", strict=contract_version >= INSTANCE_CONTRACT_VERSION
            )
            adapter_trust = str(payload.get("adapter_trust", "legacy")).strip().lower()
            session_identity_mode = str(
                payload.get("session_identity_mode", "legacy")
            ).strip().lower()
            terminal_observation = str(
                payload.get("terminal_observation", "legacy")
            ).strip().lower()
            registration_origin = str(
                payload.get("registration_origin", "legacy")
            ).strip().lower()
            registrar = str(payload.get("registrar", record.get("actor", ""))).strip()
            parent_instance_id = str(payload.get("parent_instance_id", "")).strip()
            originating_run_id = str(payload.get("originating_run_id", "")).strip()
            if contract_version >= INSTANCE_CONTRACT_VERSION:
                _validate_registration_contract(
                    payload,
                    record=record,
                    instances=instances,
                    adapter_trust=adapter_trust,
                    session_identity_mode=session_identity_mode,
                    terminal_observation=terminal_observation,
                    registration_origin=registration_origin,
                    registrar=registrar,
                    parent_instance_id=parent_instance_id,
                    originating_run_id=originating_run_id,
                )
            occurred = str(record.get("occurred_utc", ""))
            launch_attempt_digest = validate_external_session_ref_digest(
                str(payload.get("launch_attempt_digest", ""))
            )
            lifecycle_disposition = str(
                payload.get("lifecycle_disposition", "")
            ).strip()
            if contract_version >= INSTANCE_CONTRACT_VERSION and launch_attempt_digest:
                _ensure_launch_attempt_available(
                    instances, launch_attempt_digest, allow_id=identifier
                )
            instances[identifier] = AgentInstance(
                id=identifier,
                participant=participant,
                provider=provider,
                label=label,
                workstream=str(payload.get("workstream", "")).strip(),
                runtime_profile=str(payload.get("runtime_profile", "")).strip(),
                external_session_ref_digest=validate_external_session_ref_digest(
                    str(payload.get("external_session_ref_digest", ""))
                ),
                status="active",
                created_utc=occurred,
                updated_utc=occurred,
                aliases=(label,),
                durable_role=str(payload.get("durable_role", "")).strip().lower(),
                role=str(payload.get("role", "")).strip(),
                capabilities=capabilities,
                permission_mode=str(payload.get("permission_mode", "")).strip(),
                authentication_mode=str(
                    payload.get("authentication_mode", "")
                ).strip(),
                billing_mode=str(payload.get("billing_mode", "")).strip(),
                adapter_trust=adapter_trust,
                session_identity_mode=session_identity_mode,
                resumable=_payload_bool(
                    payload,
                    "resumable",
                    strict=contract_version >= INSTANCE_CONTRACT_VERSION,
                ),
                concurrent_attachment=_payload_bool(
                    payload,
                    "concurrent_attachment",
                    strict=contract_version >= INSTANCE_CONTRACT_VERSION,
                ),
                terminal_observation=terminal_observation,
                registrar=registrar,
                registration_origin=registration_origin,
                parent_instance_id=parent_instance_id,
                originating_run_id=originating_run_id,
                launch_attempt_digest=launch_attempt_digest,
                launch_attempt_digests=(launch_attempt_digest,) if launch_attempt_digest else (),
                launch_attempt_dispositions=(
                    ((launch_attempt_digest, lifecycle_disposition),)
                    if launch_attempt_digest
                    else ()
                ),
            )
            continue

        current = instances.get(identifier)
        if current is None:
            raise AgentInstanceError(f"AGENT_INSTANCE_UNKNOWN: {identifier}")
        if kind == "agent_instance_metadata_updated":
            if current.status != "active":
                raise AgentInstanceError(f"AGENT_INSTANCE_RETIRED: {identifier}")
            if not str(payload.get("reason", "")).strip():
                raise AgentInstanceError("instance metadata update requires a reason")
            fields = payload.get("fields_changed", {})
            if not isinstance(fields, Mapping) or not fields:
                raise AgentInstanceError("instance fields_changed must be a non-empty object")
            unsupported = set(fields) - set(MUTABLE_INSTANCE_FIELDS)
            if unsupported:
                raise AgentInstanceError(
                    "immutable or unknown instance fields: " + ", ".join(sorted(unsupported))
                )
            changes: dict[str, Any] = {}
            aliases = current.aliases
            for name in MUTABLE_INSTANCE_FIELDS:
                if name not in fields:
                    continue
                old_new = fields[name]
                if not isinstance(old_new, list) or len(old_new) != 2:
                    raise AgentInstanceError(f"instance field {name} must be [old, new]")
                old_value = str(old_new[0]).strip()
                current_value = str(getattr(current, name)).strip()
                if old_value != current_value:
                    raise AgentInstanceError(
                        f"instance field {name} old value does not match current state"
                    )
                new_value = str(old_new[1]).strip()
                if name == "label":
                    new_value = normalize_instance_label(new_value)
                    _ensure_label_available(instances, new_value, allow_id=identifier)
                    aliases = tuple(dict.fromkeys((*aliases, new_value)))
                elif name == "external_session_ref_digest":
                    new_value = validate_external_session_ref_digest(new_value)
                if new_value == current_value:
                    raise AgentInstanceError(f"instance field {name} does not change")
                changes[name] = new_value
            instances[identifier] = replace(
                current,
                **changes,
                aliases=aliases,
                updated_utc=str(record.get("occurred_utc", current.updated_utc)),
            )
            continue

        if kind == "agent_instance_bound":
            _require_active(current)
            _validate_lifecycle_actor(current, record)
            binding_source = str(payload.get("binding_source", "")).strip()
            if not binding_source:
                raise AgentInstanceError("instance binding source must be non-empty")
            _validate_binding_payload(current, payload)
            launch_attempt_digest = validate_external_session_ref_digest(
                str(payload.get("launch_attempt_digest", ""))
            )
            if launch_attempt_digest:
                _ensure_launch_attempt_available(
                    instances, launch_attempt_digest, allow_id=identifier
                )
            lifecycle_disposition = str(
                payload.get("lifecycle_disposition", "")
            ).strip()
            launch_attempt_dispositions = dict(current.launch_attempt_dispositions)
            if launch_attempt_digest:
                launch_attempt_dispositions[launch_attempt_digest] = lifecycle_disposition
            instances[identifier] = replace(
                current,
                launch_attempt_digests=tuple(
                    dict.fromkeys(
                        (
                            *current.launch_attempt_digests,
                            *((launch_attempt_digest,) if launch_attempt_digest else ()),
                        )
                    )
                ),
                launch_attempt_dispositions=tuple(
                    launch_attempt_dispositions.items()
                ),
                last_binding_source=binding_source,
                updated_utc=str(record.get("occurred_utc", current.updated_utc)),
            )
            continue

        if kind == "agent_instance_launch_bound":
            _require_active(current)
            _validate_lifecycle_actor(current, record)
            run_id = str(payload.get("run_id", "")).strip()
            if not run_id:
                raise AgentInstanceError("instance launch run_id must be non-empty")
            if (
                current.registration_origin == "dispatch"
                and not current.active_launch_run_ids
                and not current.released_launches
                and run_id != current.originating_run_id
            ):
                raise AgentInstanceError(
                    "AGENT_INSTANCE_ORIGINATING_RUN_MISMATCH"
                )
            for other in instances.values():
                if other.id != identifier and run_id in other.active_launch_run_ids:
                    raise AgentInstanceError("AGENT_INSTANCE_RUN_BINDING_COLLISION")
            if (
                current.active_launch_run_ids
                and run_id not in current.active_launch_run_ids
                and not current.concurrent_attachment
            ):
                raise AgentInstanceError(
                    f"AGENT_INSTANCE_LAUNCH_CONFLICT: {current.label} already has an active launch"
                )
            instances[identifier] = replace(
                current,
                active_launch_run_ids=tuple(
                    dict.fromkeys((*current.active_launch_run_ids, run_id))
                ),
                updated_utc=str(record.get("occurred_utc", current.updated_utc)),
            )
            continue

        if kind == "agent_instance_launch_released":
            _require_active(current)
            _validate_lifecycle_actor(current, record)
            run_id = str(payload.get("run_id", "")).strip()
            if not run_id or run_id not in current.active_launch_run_ids:
                raise AgentInstanceError(
                    f"AGENT_INSTANCE_LAUNCH_RELEASE_INVALID: {identifier}"
                )
            outcome = str(payload.get("outcome", "")).strip()
            if outcome not in INSTANCE_TERMINAL_OUTCOMES:
                raise AgentInstanceError(
                    f"AGENT_INSTANCE_RELEASE_OUTCOME_INVALID: {outcome!r}"
                )
            instances[identifier] = replace(
                current,
                active_launch_run_ids=tuple(
                    active for active in current.active_launch_run_ids if active != run_id
                ),
                released_launches=tuple(
                    (*current.released_launches, (run_id, outcome))
                ),
                updated_utc=str(record.get("occurred_utc", current.updated_utc)),
            )
            continue

        if kind == "agent_instance_terminal":
            _require_active(current)
            _validate_lifecycle_actor(current, record)
            if current.active_launch_run_ids:
                raise AgentInstanceError(
                    f"AGENT_INSTANCE_TERMINAL_WITH_ACTIVE_LAUNCH: {identifier}"
                )
            outcome = str(payload.get("outcome", "")).strip()
            if outcome not in INSTANCE_TERMINAL_OUTCOMES:
                raise AgentInstanceError(
                    f"AGENT_INSTANCE_TERMINAL_INVALID: {outcome!r}"
                )
            run_id = str(payload.get("run_id", "")).strip()
            if (run_id, outcome) not in current.released_launches:
                raise AgentInstanceError(
                    f"AGENT_INSTANCE_TERMINAL_SUFFIX_INVALID: {identifier}"
                )
            occurred = str(record.get("occurred_utc", ""))
            instances[identifier] = replace(
                current,
                status="terminal",
                terminal_outcome=outcome,
                terminal_utc=occurred,
                updated_utc=occurred or current.updated_utc,
            )
            continue

        if current.status != "active":
            raise AgentInstanceError(f"AGENT_INSTANCE_RETIRED: {identifier}")
        if not str(payload.get("reason", "")).strip():
            raise AgentInstanceError("instance retirement requires a reason")
        instances[identifier] = replace(
            current,
            status="retired",
            retired_utc=str(record.get("occurred_utc", "")),
            updated_utc=str(record.get("occurred_utc", current.updated_utc)),
        )
    return instances


def resolve_agent_instance_from_records(
    records: Iterable[Mapping[str, Any]], identifier: str
) -> AgentInstance | None:
    instances = reduce_agent_instances(records)
    token = identifier.strip().lower()
    direct = instances.get(identifier.strip())
    if direct is not None:
        return direct
    matches = [item for item in instances.values() if token in item.aliases]
    if len(matches) > 1:
        raise AgentInstanceError(f"AGENT_INSTANCE_LABEL_AMBIGUOUS: {identifier}")
    return matches[0] if matches else None


def _ensure_label_available(
    instances: Mapping[str, AgentInstance], label: str, *, allow_id: str = ""
) -> None:
    for instance in instances.values():
        if instance.id != allow_id and label in instance.aliases:
            raise AgentInstanceError(f"AGENT_INSTANCE_LABEL_COLLISION: {label}")


def _required_text(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise AgentInstanceError(
            f"AGENT_INSTANCE_HANDSHAKE_INVALID: {field_name} must be non-empty"
        )
    return value.strip()


def _normalize_string_tuple(values: object, *, field_name: str) -> tuple[str, ...]:
    if not isinstance(values, (list, tuple)) or not all(
        isinstance(item, str) and item.strip() for item in values
    ):
        raise AgentInstanceError(
            f"AGENT_INSTANCE_HANDSHAKE_INVALID: {field_name} must be strings"
        )
    normalized = tuple(item.strip() for item in values)
    if len(set(normalized)) != len(normalized):
        raise AgentInstanceError(
            f"AGENT_INSTANCE_HANDSHAKE_INVALID: {field_name} must be unique"
        )
    return normalized


def _looks_high_entropy(value: str) -> bool:
    if len(value) < 24:
        return False
    classes = sum(
        bool(re.search(pattern, value))
        for pattern in (r"[a-z]", r"[A-Z]", r"[0-9]", r"[^A-Za-z0-9]")
    )
    return classes >= 2 and len(set(value)) >= 12


def _instance_is_compatible(
    instance: AgentInstance, handshake: NormalizedRuntimeIdentityHandshake
) -> bool:
    return all(
        (
            instance.participant == handshake.participant,
            instance.provider == handshake.provider,
            instance.runtime_profile == handshake.runtime_profile,
            instance.durable_role == handshake.durable_role,
            instance.role == handshake.role,
            instance.capabilities == handshake.capabilities,
            instance.permission_mode == handshake.permission_mode,
            instance.authentication_mode == handshake.authentication_mode,
            instance.billing_mode == handshake.billing_mode,
            instance.adapter_trust == handshake.adapter_trust,
            instance.session_identity_mode == handshake.session_identity_mode,
            instance.resumable == handshake.resumable,
            instance.concurrent_attachment == handshake.concurrent_attachment,
            instance.terminal_observation == handshake.terminal_observation,
            instance.workstream == handshake.workstream,
        )
    )


def _require_compatible_instance(
    instance: AgentInstance, handshake: NormalizedRuntimeIdentityHandshake
) -> None:
    if not _instance_is_compatible(instance, handshake):
        raise AgentInstanceError("AGENT_INSTANCE_LAUNCH_KEY_METADATA_CONFLICT")
    if (
        instance.external_session_ref_digest
        != handshake.external_session_ref_digest
    ):
        raise AgentInstanceError("AGENT_INSTANCE_LAUNCH_KEY_METADATA_CONFLICT")
    prior_disposition = dict(instance.launch_attempt_dispositions).get(
        handshake.launch_attempt_digest
    )
    if prior_disposition and prior_disposition != handshake.lifecycle_disposition:
        raise AgentInstanceError("AGENT_INSTANCE_LAUNCH_KEY_METADATA_CONFLICT")


def _require_active(instance: AgentInstance) -> None:
    if instance.status == "terminal":
        raise AgentInstanceError(f"AGENT_INSTANCE_TERMINAL: {instance.label}")
    if instance.status != "active":
        raise AgentInstanceError(f"AGENT_INSTANCE_RETIRED: {instance.label}")


def _contract_version(payload: Mapping[str, Any]) -> int:
    value = payload.get("instance_contract_version", 1)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise AgentInstanceError("AGENT_INSTANCE_CONTRACT_VERSION_INVALID")
    return value


def _payload_string_tuple(
    payload: Mapping[str, Any], field_name: str, *, strict: bool
) -> tuple[str, ...]:
    if field_name not in payload and not strict:
        return ()
    values = payload.get(field_name, [])
    try:
        return _normalize_string_tuple(values, field_name=field_name)
    except AgentInstanceError:
        if strict:
            raise
        return ()


def _payload_bool(payload: Mapping[str, Any], field_name: str, *, strict: bool) -> bool:
    if field_name not in payload:
        if strict:
            raise AgentInstanceError(
                f"AGENT_INSTANCE_METADATA_INVALID: {field_name} must be present"
            )
        return False
    value = payload[field_name]
    if not isinstance(value, bool):
        raise AgentInstanceError(
            f"AGENT_INSTANCE_METADATA_INVALID: {field_name} must be a boolean"
        )
    return value


def _validate_registration_contract(
    payload: Mapping[str, Any],
    *,
    record: Mapping[str, Any],
    instances: Mapping[str, AgentInstance],
    adapter_trust: str,
    session_identity_mode: str,
    terminal_observation: str,
    registration_origin: str,
    registrar: str,
    parent_instance_id: str,
    originating_run_id: str,
) -> None:
    required = (
        "durable_role",
        "role",
        "permission_mode",
        "authentication_mode",
        "billing_mode",
    )
    for field_name in required:
        if not str(payload.get(field_name, "")).strip():
            raise AgentInstanceError(
                f"AGENT_INSTANCE_METADATA_INVALID: {field_name} must be non-empty"
            )
    if adapter_trust not in INSTANCE_ADAPTER_TRUST_SOURCES - {"legacy", "unmanaged"}:
        raise AgentInstanceError("AGENT_INSTANCE_ADAPTER_UNTRUSTED")
    if session_identity_mode not in INSTANCE_SESSION_IDENTITY_MODES - {"legacy"}:
        raise AgentInstanceError("AGENT_INSTANCE_SESSION_MODE_INVALID")
    if terminal_observation not in INSTANCE_TERMINAL_OBSERVATION_MODES - {"legacy"}:
        raise AgentInstanceError("AGENT_INSTANCE_TERMINAL_OBSERVATION_INVALID")
    if registration_origin not in INSTANCE_REGISTRATION_ORIGINS - {"legacy", "manual"}:
        raise AgentInstanceError("AGENT_INSTANCE_REGISTRATION_ORIGIN_INVALID")
    if not registrar or registrar != str(record.get("actor", "")).strip():
        raise AgentInstanceError("AGENT_INSTANCE_REGISTRAR_MISMATCH")
    if registration_origin == "dispatch":
        if not parent_instance_id:
            if str(record.get("actor_instance_id", "")).strip() or not originating_run_id:
                raise AgentInstanceError("AGENT_INSTANCE_HUMAN_DISPATCH_LINEAGE_INVALID")
        else:
            parent = instances.get(parent_instance_id)
            if parent is None:
                raise AgentInstanceError("AGENT_INSTANCE_PARENT_UNKNOWN")
            _require_active(parent)
            if str(record.get("actor_instance_id", "")).strip() != parent_instance_id:
                raise AgentInstanceError("AGENT_INSTANCE_REGISTRAR_MISMATCH")
            if parent.participant != registrar:
                raise AgentInstanceError("AGENT_INSTANCE_REGISTRAR_MISMATCH")
            if not originating_run_id:
                raise AgentInstanceError("AGENT_INSTANCE_ORIGINATING_RUN_REQUIRED")
    elif parent_instance_id or originating_run_id or str(
        record.get("actor_instance_id", "")
    ).strip():
        raise AgentInstanceError("AGENT_INSTANCE_INTEGRATION_LINEAGE_INVALID")
    session_digest = validate_external_session_ref_digest(
        str(payload.get("external_session_ref_digest", ""))
    )
    validate_external_session_ref_digest(
        str(payload.get("launch_attempt_digest", ""))
    )
    lifecycle_disposition = str(payload.get("lifecycle_disposition", "")).strip()
    if lifecycle_disposition not in INSTANCE_LIFECYCLE_DISPOSITIONS:
        raise AgentInstanceError("AGENT_INSTANCE_DISPOSITION_INVALID")
    resumable = _payload_bool(payload, "resumable", strict=True)
    _payload_bool(payload, "concurrent_attachment", strict=True)
    if session_identity_mode == "exact" and not session_digest:
        raise AgentInstanceError("AGENT_INSTANCE_EXACT_SESSION_REQUIRED")
    if session_identity_mode == "none" and session_digest:
        raise AgentInstanceError("AGENT_INSTANCE_SESSION_MODE_CONFLICT")
    if resumable and session_identity_mode != "exact":
        raise AgentInstanceError("AGENT_INSTANCE_RESUME_UNSUPPORTED")


def _validate_binding_payload(
    current: AgentInstance, payload: Mapping[str, Any]
) -> None:
    session_digest = validate_external_session_ref_digest(
        str(payload.get("external_session_ref_digest", ""))
    )
    launch_digest = validate_external_session_ref_digest(
        str(payload.get("launch_attempt_digest", ""))
    )
    if session_digest != current.external_session_ref_digest:
        raise AgentInstanceError("AGENT_INSTANCE_BINDING_SESSION_MISMATCH")
    binding_source = str(payload.get("binding_source", "")).strip()
    known_launch_digests = current.launch_attempt_digests or (
        (current.launch_attempt_digest,) if current.launch_attempt_digest else ()
    )
    if binding_source == "launch-attempt-retry":
        if not launch_digest or launch_digest not in known_launch_digests:
            raise AgentInstanceError("AGENT_INSTANCE_BINDING_LAUNCH_MISMATCH")
    prior_disposition = dict(current.launch_attempt_dispositions).get(launch_digest)
    disposition = str(payload.get("lifecycle_disposition", "")).strip()
    if prior_disposition and prior_disposition != disposition:
        raise AgentInstanceError("AGENT_INSTANCE_LAUNCH_KEY_METADATA_CONFLICT")
    resumed = payload.get("provider_context_resumed")
    if not isinstance(resumed, bool):
        raise AgentInstanceError("AGENT_INSTANCE_BINDING_RESUME_INVALID")
    expected_resumed = (
        binding_source in {"session-digest", "launch-attempt-retry"}
        and current.resumable
        and current.session_identity_mode == "exact"
        and session_digest == current.external_session_ref_digest
    )
    if resumed != expected_resumed:
        raise AgentInstanceError("AGENT_INSTANCE_BINDING_RESUME_MISMATCH")
    runtime_profile = str(payload.get("runtime_profile", "")).strip()
    if runtime_profile != current.runtime_profile:
        raise AgentInstanceError("AGENT_INSTANCE_BINDING_PROFILE_MISMATCH")
    if disposition not in INSTANCE_LIFECYCLE_DISPOSITIONS:
        raise AgentInstanceError("AGENT_INSTANCE_DISPOSITION_INVALID")


def _ensure_launch_attempt_available(
    instances: Mapping[str, AgentInstance], digest: str, *, allow_id: str = ""
) -> None:
    if not digest:
        return
    for instance in instances.values():
        known = instance.launch_attempt_digests or (
            (instance.launch_attempt_digest,) if instance.launch_attempt_digest else ()
        )
        if digest in known and instance.id != allow_id:
            raise AgentInstanceError("AGENT_INSTANCE_LAUNCH_KEY_COLLISION")


def _validate_lifecycle_actor(
    current: AgentInstance, record: Mapping[str, Any]
) -> None:
    actor = str(record.get("actor", "")).strip()
    actor_instance_id = str(record.get("actor_instance_id", "")).strip()
    if current.registration_origin == "dispatch":
        if actor != current.registrar or actor_instance_id != current.parent_instance_id:
            raise AgentInstanceError("AGENT_INSTANCE_LAUNCH_PARENT_MISMATCH")
        return
    if current.registration_origin == "integration" and (
        actor != current.registrar or actor_instance_id
    ):
        raise AgentInstanceError("AGENT_INSTANCE_INTEGRATION_LIFECYCLE_MISMATCH")
