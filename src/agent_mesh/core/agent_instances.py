"""Durable, project-local identities for long-lived AI-agent work instances."""

from __future__ import annotations

import contextvars
import hashlib
import os
import re
from dataclasses import dataclass, replace
from typing import Any, Iterable, Mapping

INSTANCE_ENV_VAR = "AGENT_MESH_INSTANCE_ID"
INSTANCE_ID_RE = re.compile(r"^AI-\d{8}-\d{2,}$")
INSTANCE_LABEL_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
SESSION_REF_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
INSTANCE_EVENT_KINDS = frozenset(
    {"agent_instance_registered", "agent_instance_metadata_updated", "agent_instance_retired"}
)
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
    if instance.status != "active":
        raise AgentInstanceError(f"AGENT_INSTANCE_RETIRED: {instance.id}")
    if explicit_actor is not None and requested != instance.participant:
        raise AgentInstanceError(
            f"AGENT_INSTANCE_ACTOR_MISMATCH: {instance.id} belongs to "
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
            "instance label must use 1-63 lowercase letters, numbers, or interior hyphens"
        )
    return label


def digest_external_session_ref(value: str | None) -> str:
    """Hash a provider session reference so canonical state never stores the raw identifier."""

    normalized = (value or "").strip()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest() if normalized else ""


def validate_external_session_ref_digest(value: str) -> str:
    digest = value.strip()
    if digest and not SESSION_REF_DIGEST_RE.fullmatch(digest):
        raise AgentInstanceError("external session reference digest must be SHA-256 hex")
    return digest


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
            label = normalize_instance_label(str(payload.get("label", "")))
            _ensure_label_available(instances, label)
            participant = str(payload.get("participant", "")).strip()
            provider = str(payload.get("provider", "")).strip().lower()
            if not participant or not provider:
                raise AgentInstanceError("instance participant and provider must be non-empty")
            occurred = str(record.get("occurred_utc", ""))
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
            changes: dict[str, str] = {}
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
