"""Frozen dispatch.v1 policy and per-slot outcome semantics."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any

from agent_mesh.config import RuntimeProfile
from agent_mesh.core.assurance import (
    DispatchOutcome as _DispatchOutcome,
    current_dispatch_outcome as _current_dispatch_outcome,
)
from agent_mesh.core.dispatch_schema import dispatch_policy_digest, validate_dispatch_payload
from agent_mesh.core.events import Event, append_event, generate_event_id, utc_now
from agent_mesh.core.ids import new_ulid
from agent_mesh.store.sqlite import json_loads

from .profiles import runtime_profile_revision

DispatchOutcome = _DispatchOutcome
current_dispatch_outcome = _current_dispatch_outcome

class DispatchPolicyError(ValueError):
    """A dispatch.v1 policy or response slot could not be resolved safely."""


@dataclass(frozen=True)
class DispatchPolicySnapshot:
    policy_id: str
    request_id: str
    response_slot_kind: str
    response_slot_key: str
    payload: dict[str, Any]

    @property
    def digest(self) -> str:
        return str(self.payload["policy_digest"])


def build_dispatch_policy(
    conn: sqlite3.Connection,
    *,
    request_id: str,
    profile: RuntimeProfile,
    purpose: str,
    role: str,
    management_level: str,
    response_contract: dict[str, Any] | None = None,
    artifact_contract: dict[str, Any] | None = None,
    provenance_requirements: tuple[str, ...] = (),
    max_attempts: int = 1,
    subject: dict[str, Any] | None = None,
    assurance_policy: dict[str, Any] | None = None,
    instance_id: str = "",
    policy_id: str | None = None,
    frozen_utc: str | None = None,
) -> DispatchPolicySnapshot:
    """Resolve one request response slot and return its immutable policy payload."""

    request = conn.execute(
        "SELECT thread_id, recipients_json, recipient_instance_ids_json, meta_json "
        "FROM messages WHERE id=? AND kind='request'",
        (request_id,),
    ).fetchone()
    if request is None:
        raise DispatchPolicyError(f"DISPATCH_POLICY_UNKNOWN_REQUEST: {request_id}")
    meta = json_loads(request["meta_json"], {})
    response_mode = (
        str(meta.get("response_mode") or "single") if isinstance(meta, dict) else "single"
    )
    recipients = {str(item) for item in json_loads(request["recipients_json"], [])}
    recipient_instances = {
        str(item) for item in json_loads(request["recipient_instance_ids_json"], [])
    }
    target: dict[str, str] = {
        "participant": profile.target,
        "durable_role": profile.durable_role,
        "runtime_profile": profile.name,
    }
    if response_mode == "single":
        if profile.target not in recipients and not instance_id:
            raise DispatchPolicyError(
                f"DISPATCH_POLICY_TARGET_NOT_ADDRESSED: {profile.target}"
            )
        slot_kind = "single"
        slot_key = request_id
    elif response_mode == "multi" and instance_id:
        if instance_id not in recipient_instances:
            raise DispatchPolicyError(
                f"DISPATCH_POLICY_INSTANCE_NOT_ADDRESSED: {instance_id}"
            )
        slot_kind = "instance"
        slot_key = instance_id
        target["instance_id"] = instance_id
    elif response_mode == "multi":
        if profile.target not in recipients:
            raise DispatchPolicyError(
                f"DISPATCH_POLICY_TARGET_NOT_ADDRESSED: {profile.target}"
            )
        slot_kind = "participant"
        slot_key = profile.target
    else:
        raise DispatchPolicyError(f"DISPATCH_POLICY_RESPONSE_MODE_INVALID: {response_mode}")

    identifier = policy_id or new_ulid("dpol")
    payload: dict[str, Any] = {
        "contract_version": "dispatch.v1",
        "policy_id": identifier,
        "request_id": request_id,
        "policy_digest": "0" * 64,
        "purpose": purpose,
        "role": role,
        "required_capabilities": list(profile.required_capabilities),
        "permission_ceiling": profile.permission_mode,
        "response_contract": response_contract
        or {
            "kind": "review_v1" if purpose == "review" else "bounded_res",
            "max_chars": 20_000,
            "requires_fence": True,
        },
        "artifact_contract": artifact_contract
        or {
            "root": "",
            "media_types": [],
            "max_bytes": 0,
            "visibility": "project_private",
            "creation_allowed": False,
        },
        "provenance_requirements": list(provenance_requirements),
        "retry": {"max_attempts": max_attempts},
        "response_slot": {"kind": slot_kind, "key": slot_key},
        "target": target,
        "runtime_profile_revision": runtime_profile_revision(profile),
        "management_level": management_level,
        "frozen_utc": frozen_utc or utc_now(),
    }
    if subject is not None:
        payload["subject"] = subject
    if assurance_policy is not None:
        payload["assurance_policy"] = assurance_policy
    payload["policy_digest"] = dispatch_policy_digest(payload)
    validate_dispatch_payload("dispatch_policy_frozen", payload)
    return DispatchPolicySnapshot(
        policy_id=identifier,
        request_id=request_id,
        response_slot_kind=slot_kind,
        response_slot_key=slot_key,
        payload=payload,
    )


def append_dispatch_policy(
    events_path,
    policy: DispatchPolicySnapshot,
    *,
    actor: str,
    lock_acquired: bool,
) -> None:
    """Append one already-resolved policy without reinterpreting its subject or slot."""

    append_event(
        events_path,
        Event(
            event_id=generate_event_id(),
            actor=actor,
            kind="dispatch_policy_frozen",
            entity_id=policy.policy_id,
            thread_id=policy.request_id,
            payload=policy.payload,
        ),
        lock_acquired=lock_acquired,
    )
