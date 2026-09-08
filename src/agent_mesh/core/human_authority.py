"""Shared classification for instance-free direct-human control events."""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from typing import Any


DIRECT_HUMAN_CONTROL_EVENT_KINDS = frozenset(
    {
        "decision_accepted",
        "decision_rejected",
        "review_assurance_retired",
    }
)


def is_direct_human_control_event(kind: str, payload: Mapping[str, Any]) -> bool:
    """Return whether an event uses the instance-free human-control contract."""

    if kind in {"decision_accepted", "decision_rejected"}:
        return _decision_contract_version(payload) >= 5
    if kind == "review_assurance_retired":
        return str(payload.get("contract_version") or "") == "review-assurance-lifecycle.v1"
    return False


def has_persisted_direct_human_authority(
    *,
    kind: str,
    actor: str,
    payload: Mapping[str, Any],
) -> bool:
    """Recognize an immutable event binding without consulting mutable config."""

    normalized_actor = actor.strip()
    if not normalized_actor or not is_direct_human_control_event(kind, payload):
        return False
    authority_mode = str(
        payload.get(
            "approval_authority_mode" if kind != "decision_rejected" else "rejection_authority_mode"
        )
        or ""
    )
    authority_revision = str(
        payload.get(
            "approval_authority_revision"
            if kind != "decision_rejected"
            else "rejection_authority_revision"
        )
        or ""
    )
    if authority_mode not in {"legacy_participants", "explicit_human_approvers"}:
        return False
    if re.fullmatch(r"[0-9a-f]{64}", authority_revision) is None:
        return False
    if kind == "decision_accepted":
        return str(payload.get("accepted_by") or "").strip() == normalized_actor and str(
            payload.get("approval_source") or ""
        ) in {"workbench", "interactive_cli"}
    if kind == "decision_rejected":
        return (
            str(payload.get("rejected_by") or "").strip() == normalized_actor
            and str(payload.get("rejection_source") or "") == "workbench"
        )
    return kind == "review_assurance_retired"


def has_current_direct_human_authority(
    *,
    kind: str,
    actor: str,
    payload: Mapping[str, Any],
    approval_identities: Iterable[str],
    approval_authority_mode: str,
    approval_authority_revision: str,
) -> bool:
    """Recognize a structurally bound direct-human event under current config.

    This check only decides whether instance attribution is inapplicable. Domain
    validation still verifies the complete event against canonical state.
    """

    normalized_actor = actor.strip()
    if not has_persisted_direct_human_authority(
        kind=kind,
        actor=normalized_actor,
        payload=payload,
    ) or normalized_actor not in set(approval_identities):
        return False

    if kind == "decision_accepted":
        return (
            str(payload.get("approval_authority_mode") or "") == approval_authority_mode
            and str(payload.get("approval_authority_revision") or "") == approval_authority_revision
        )
    if kind == "decision_rejected":
        return (
            str(payload.get("rejection_authority_mode") or "") == approval_authority_mode
            and str(payload.get("rejection_authority_revision") or "")
            == approval_authority_revision
        )
    if kind == "review_assurance_retired":
        return (
            str(payload.get("approval_authority_mode") or "") == approval_authority_mode
            and str(payload.get("approval_authority_revision") or "") == approval_authority_revision
        )
    return False


def _decision_contract_version(payload: Mapping[str, Any]) -> int:
    raw = payload.get("decision_contract_version")
    if isinstance(raw, bool):
        return 0
    if isinstance(raw, int):
        return raw
    if isinstance(raw, str) and raw.isdigit():
        return int(raw)
    return 0
