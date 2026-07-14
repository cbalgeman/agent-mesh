"""Dry-run promotion planner for manually reviewed source-recovery rows.

The planner is intentionally pure. It reads a manual promotion-review manifest
and emits candidate req_created payload patches that validate against the
provenance schema. It never appends events, mutates config, or rebuilds
projections.
"""
from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from agent_mesh.core.provenance import ProvenanceValidationError, validate_event_provenance

MANIFEST_SCHEMA_VERSION = 1
MANIFEST_KIND = "source_recovery_promotion_dry_run_plan"
REVIEW_MANIFEST_KIND = "source_recovery_manual_promotion_review"
APPROVED_DECISION = "approved_for_promotion_manifest"
SELECTED_BY = "phase6-source-recovery-promotion-review"


class SourceRecoveryPromotionError(ValueError):
    """Raised when a source-recovery promotion dry-run plan is unsafe."""


def build_source_recovery_promotion_plan(
    promotion_review: Mapping[str, Any] | str | Path,
) -> dict[str, Any]:
    """Build a dry-run patch plan from approved manual promotion-review rows."""

    review, input_sha256 = _load_review(promotion_review)
    if review.get("manifest_kind") != REVIEW_MANIFEST_KIND:
        raise SourceRecoveryPromotionError(
            f"promotion review manifest_kind must be {REVIEW_MANIFEST_KIND!r}"
        )
    approved = review.get("approved_promotions")
    if not isinstance(approved, list):
        raise SourceRecoveryPromotionError("promotion review must contain approved_promotions array")
    rejected = review.get("rejected_promote_recommendations") or []
    if not isinstance(rejected, list):
        raise SourceRecoveryPromotionError(
            "promotion review rejected_promote_recommendations must be an array"
        )

    operations = [_operation_for_approved_row(row) for row in approved]
    manifest: dict[str, Any] = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "manifest_kind": MANIFEST_KIND,
        "generated_utc": _utc_now(),
        "mode": "dry_run",
        "dry_run_only": True,
        "source_review_kind": review.get("manifest_kind"),
        "source_review_schema_version": review.get("schema_version"),
        "input_audit_sha256": review.get("input_audit_sha256"),
        "input_ledger_sha256": review.get("input_ledger_sha256"),
        "counts": {
            "approved_rows": len(approved),
            "rejected_rows_ignored": len(rejected),
            "planned_operations": len(operations),
        },
        "safety": {
            "applies_changes": False,
            "appends_events": False,
            "requires_followup_importer_review": True,
            "notes": [
                "dry-run plan only; do not apply without separate importer implementation",
                "candidate payloads validated with validate_event_provenance(kind='req_created')",
                "rejected promotion-review rows are intentionally ignored",
            ],
        },
        "operations": operations,
    }
    if input_sha256 is not None:
        manifest["input_promotion_review_sha256"] = input_sha256
    _validate_review_counts(review, manifest)
    return manifest


def _load_review(review: Mapping[str, Any] | str | Path) -> tuple[Mapping[str, Any], str | None]:
    if isinstance(review, Mapping):
        return review, None
    path = Path(review)
    try:
        data = path.read_bytes()
    except FileNotFoundError as exc:
        raise SourceRecoveryPromotionError(f"promotion review not found: {path}") from exc
    try:
        parsed = json.loads(data)
    except json.JSONDecodeError as exc:
        raise SourceRecoveryPromotionError(
            f"{path}: invalid JSON promotion review: {exc.msg}"
        ) from exc
    if not isinstance(parsed, dict):
        raise SourceRecoveryPromotionError("promotion review JSON must be an object")
    return parsed, hashlib.sha256(data).hexdigest()


def _operation_for_approved_row(row: Any) -> dict[str, Any]:
    if not isinstance(row, Mapping):
        raise SourceRecoveryPromotionError("approved promotion row must be an object")
    message_id = str(row.get("id") or "")
    if not message_id:
        raise SourceRecoveryPromotionError("approved promotion row missing id")
    if row.get("manual_review_decision") != APPROVED_DECISION:
        raise SourceRecoveryPromotionError(f"{message_id}: not approved for promotion manifest")

    body = str(row.get("recovered_body") or "")
    if not body.strip():
        raise SourceRecoveryPromotionError(f"{message_id}: empty recovered_body")
    authority = str(row.get("recommended_body_authority") or "")
    fidelity = str(row.get("recommended_body_fidelity") or "")
    patch = row.get("promotion_payload_patch")
    if not isinstance(patch, Mapping):
        raise SourceRecoveryPromotionError(f"{message_id}: missing promotion_payload_patch")
    refs = patch.get("source_context_refs")
    if not isinstance(refs, list) or not refs:
        raise SourceRecoveryPromotionError(f"{message_id}: source_context_refs required")

    payload = {
        "request_id": message_id,
        "body": body,
        "body_authority": authority,
        "body_fidelity": fidelity,
        "source_context_refs": [_source_ref(ref, message_id) for ref in refs],
        "source_selection": {
            "mode": "manual",
            "confidence": 1.0,
            "selected_by": SELECTED_BY,
            "requires_review": False,
        },
        "causal_edges": [_causal_edge(ref, message_id) for ref in refs],
        "refs": [],
    }
    _validate_payload(message_id, payload)
    return {
        "id": message_id,
        "operation": "recover_request_body_payload_patch",
        "would_append_event": False,
        "would_modify_existing_event": False,
        "provenance_validation": "passed",
        "review_decision": row.get("manual_review_decision"),
        "review_note": row.get("review_note"),
        "selected_candidate_id": (row.get("selected_candidate") or {}).get("candidate_id")
        if isinstance(row.get("selected_candidate"), Mapping)
        else None,
        "candidate_req_created_payload": payload,
    }


def _source_ref(ref: Any, message_id: str) -> dict[str, Any]:
    if not isinstance(ref, Mapping):
        raise SourceRecoveryPromotionError(f"{message_id}: source ref must be an object")
    source_uri = str(ref.get("source_uri") or ref.get("source_id") or "")
    if not source_uri:
        raise SourceRecoveryPromotionError(f"{message_id}: source ref missing source_uri/source_id")
    result: dict[str, Any] = {
        "channel": str(ref.get("channel") or ""),
        "source_kind": str(ref.get("source_kind") or ""),
        "source_id": str(ref.get("source_id") or source_uri),
        "source_uri": source_uri,
        "role": str(ref.get("role") or "source"),
        "confidence": 1.0,
    }
    for key in ("sha256", "source_file", "line_number", "record_index"):
        if key in ref:
            result[key] = ref[key]
    return result


def _causal_edge(ref: Any, message_id: str) -> dict[str, Any]:
    if isinstance(ref, Mapping):
        source = str(ref.get("source_uri") or ref.get("source_id") or "")
    else:
        source = ""
    if not source:
        raise SourceRecoveryPromotionError(f"{message_id}: source ref missing edge source")
    return {
        "relation": "recovered_from",
        "from_ref": source,
        "to_ref": message_id,
        "confidence": 1.0,
    }


def _validate_payload(message_id: str, payload: dict[str, Any]) -> None:
    try:
        validate_event_provenance("req_created", payload, message_id)
    except ProvenanceValidationError as exc:
        raise SourceRecoveryPromotionError(f"{message_id}: invalid provenance payload: {exc}") from exc


def _validate_review_counts(review: Mapping[str, Any], plan: Mapping[str, Any]) -> None:
    counts = review.get("counts") or {}
    if not isinstance(counts, Mapping):
        return
    approved_count = counts.get("approved_for_promotion_manifest")
    if approved_count is not None and approved_count != plan["counts"]["approved_rows"]:
        raise SourceRecoveryPromotionError(
            "promotion review approved_for_promotion_manifest count does not match rows"
        )
    rejected_count = counts.get("rejected_after_manual_review")
    if rejected_count is not None and rejected_count != plan["counts"]["rejected_rows_ignored"]:
        raise SourceRecoveryPromotionError(
            "promotion review rejected_after_manual_review count does not match rows"
        )


def _utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
