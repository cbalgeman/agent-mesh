"""Source-channel provenance helpers for mail-shaped events."""
from __future__ import annotations

import math
from typing import Any

BODY_AUTHORITY_VALUES = {
    "human_chat",
    "agent_mail",
    "tool_payload",
    "agent_summary",
    "recovery_artifact",
    "unknown",
}

BODY_FIDELITY_VALUES = {
    "full",
    "metadata_only",
    "reconstructed",
    "inferred",
    "redacted",
    "missing",
}

SOURCE_SELECTION_MODES = {
    "automatic_previous_human",
    "explicit_turn_id",
    "thread_window",
    "tool_result",
    "manual",
    "none",
}

CAUSAL_EDGE_RELATIONS = {
    "caused",
    "replied_to",
    "projected_as",
    "derived_from",
    "updated",
    "closed_out",
    "recovered_from",
    "artifact_echo_of",
}

MESSAGE_PROVENANCE_KINDS = {"req_created", "res_posted"}
NO_SOURCE_CONTEXT_STATUS = "no_source_context_available"


class ProvenanceValidationError(ValueError):
    """Raised when additive provenance metadata is malformed."""


def validate_event_provenance(kind: str, payload: Any, entity_id: str = "") -> None:
    """Validate additive source provenance metadata on supported event payloads.

    Missing provenance fields are valid for backward compatibility.
    """
    if kind not in MESSAGE_PROVENANCE_KINDS:
        return
    if not isinstance(payload, dict):
        raise ProvenanceValidationError(f"{kind}.payload must be an object")

    authority = payload.get("body_authority")
    if authority is not None and authority not in BODY_AUTHORITY_VALUES:
        raise ProvenanceValidationError(
            f"{kind}.payload.body_authority invalid for {entity_id}: {authority!r}"
        )

    fidelity = payload.get("body_fidelity")
    if fidelity is not None and fidelity not in BODY_FIDELITY_VALUES:
        raise ProvenanceValidationError(
            f"{kind}.payload.body_fidelity invalid for {entity_id}: {fidelity!r}"
        )

    source_refs = payload.get("source_context_refs")
    has_source_refs = False
    if source_refs is not None:
        if not isinstance(source_refs, list):
            raise ProvenanceValidationError(
                f"{kind}.payload.source_context_refs must be a list for {entity_id}"
            )
        if not source_refs:
            raise ProvenanceValidationError(
                f"{kind}.payload.source_context_refs must not be empty for {entity_id}; "
                "omit the field or use source_context_status='no_source_context_available'"
            )
        has_source_refs = True
        for index, item in enumerate(source_refs):
            if not isinstance(item, dict):
                raise ProvenanceValidationError(
                    f"{kind}.payload.source_context_refs[{index}] must be an object for {entity_id}"
                )
            channel = item.get("channel")
            if not isinstance(channel, str) or not channel.strip():
                raise ProvenanceValidationError(
                    f"{kind}.payload.source_context_refs[{index}].channel must be a non-empty "
                    f"string for {entity_id}"
                )
            locator_fields = ("source_uri", "source_id", "source_event_id", "source_path")
            if not any(
                isinstance(item.get(field), str) and item.get(field, "").strip()
                for field in locator_fields
            ):
                raise ProvenanceValidationError(
                    f"{kind}.payload.source_context_refs[{index}] must include a non-empty "
                    f"source locator for {entity_id}"
                )
            _validate_optional_confidence(item, f"{kind}.payload.source_context_refs[{index}]")

    source_context_status = payload.get("source_context_status")
    if source_context_status is not None and source_context_status != NO_SOURCE_CONTEXT_STATUS:
        raise ProvenanceValidationError(
            f"{kind}.payload.source_context_status invalid for {entity_id}: "
            f"{source_context_status!r}"
        )
    if has_source_refs and source_context_status == NO_SOURCE_CONTEXT_STATUS:
        raise ProvenanceValidationError(
            f"{kind}.payload.source_context_status conflicts with source_context_refs for {entity_id}"
        )
    if has_source_refs and (authority is None or fidelity is None):
        raise ProvenanceValidationError(
            f"{kind}.payload source trust metadata for {entity_id} must include "
            "body_authority and body_fidelity when source_context_refs are present"
        )
    trust_metadata_requested = any(
        key in payload for key in ("source_context_status", "body_authority", "body_fidelity")
    )
    if not has_source_refs and trust_metadata_requested:
        if (
            source_context_status != NO_SOURCE_CONTEXT_STATUS
            or authority != "unknown"
            or fidelity is None
        ):
            raise ProvenanceValidationError(
                f"{kind}.payload no-source trust metadata for {entity_id} must include "
                "source_context_status='no_source_context_available', "
                "body_authority='unknown', and body_fidelity"
            )

    causal_edges = payload.get("causal_edges")
    if causal_edges is not None:
        if not isinstance(causal_edges, list):
            raise ProvenanceValidationError(
                f"{kind}.payload.causal_edges must be a list for {entity_id}"
            )
        for index, item in enumerate(causal_edges):
            if not isinstance(item, dict):
                raise ProvenanceValidationError(
                    f"{kind}.payload.causal_edges[{index}] must be an object for {entity_id}"
                )
            relation = item.get("relation")
            if relation not in CAUSAL_EDGE_RELATIONS:
                raise ProvenanceValidationError(
                    f"{kind}.payload.causal_edges[{index}].relation invalid for "
                    f"{entity_id}: {relation!r}"
                )
            _validate_optional_confidence(item, f"{kind}.payload.causal_edges[{index}]")

    source_selection = payload.get("source_selection")
    if source_selection is not None:
        if not isinstance(source_selection, dict):
            raise ProvenanceValidationError(
                f"{kind}.payload.source_selection must be an object for {entity_id}"
            )
        mode = source_selection.get("mode")
        if mode not in SOURCE_SELECTION_MODES:
            raise ProvenanceValidationError(
                f"{kind}.payload.source_selection.mode invalid for {entity_id}: {mode!r}"
            )
        if "confidence" not in source_selection:
            raise ProvenanceValidationError(
                f"{kind}.payload.source_selection.confidence is required for {entity_id}"
            )
        _validate_confidence(
            source_selection.get("confidence"),
            f"{kind}.payload.source_selection.confidence",
        )
        selected_by = source_selection.get("selected_by")
        if not isinstance(selected_by, str) or not selected_by:
            raise ProvenanceValidationError(
                f"{kind}.payload.source_selection.selected_by must be a non-empty string "
                f"for {entity_id}"
            )
        requires_review = source_selection.get("requires_review")
        if not isinstance(requires_review, bool):
            raise ProvenanceValidationError(
                f"{kind}.payload.source_selection.requires_review must be a boolean "
                f"for {entity_id}"
            )


def body_authority_for_payload(payload: dict[str, Any]) -> str:
    value = payload.get("body_authority")
    if value in BODY_AUTHORITY_VALUES:
        return str(value)
    # Explicitly degraded bodies must not be promoted to agent_mail authority by
    # default. Import/recovery code should opt into a concrete authority.
    if payload.get("body_fidelity") in BODY_FIDELITY_VALUES - {"full"}:
        return "unknown"
    # Backward-compatible default for pre-provenance mail events: the canonical
    # event body is the agent-mesh/agent-mail body unless explicitly degraded.
    return "agent_mail" if "body" in payload else "unknown"


def body_fidelity_for_payload(payload: dict[str, Any]) -> str | None:
    value = payload.get("body_fidelity")
    if value in BODY_FIDELITY_VALUES:
        return str(value)
    # Existing req/res events with an inline body are full-fidelity canonical mail
    # bodies. Degraded imports must opt in with metadata_only/inferred/etc.
    return "full" if "body" in payload else None


def confidence_value(value: Any) -> float | None:
    if value is None or isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value)


def _validate_optional_confidence(item: dict[str, Any], path: str) -> None:
    if "confidence" in item:
        _validate_confidence(item.get("confidence"), f"{path}.confidence")


def _validate_confidence(value: Any, path: str) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ProvenanceValidationError(f"{path} must be a number from 0.0 to 1.0")
    numeric = float(value)
    if not math.isfinite(numeric) or numeric < 0.0 or numeric > 1.0:
        raise ProvenanceValidationError(f"{path} must be a number from 0.0 to 1.0")
