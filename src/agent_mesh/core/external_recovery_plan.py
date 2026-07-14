"""Verifier-only summaries for external source-recovery dry-run plans.

This module intentionally does not append events, rebuild projections, or apply
candidate request-body patches. It converts a reviewed dry-run promotion plan
into a cutover-verifier section explicitly labeled as external evidence, not
canonical event-log state.
"""
from __future__ import annotations

import hashlib
import json
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

PLAN_MANIFEST_KIND = "source_recovery_promotion_dry_run_plan"
REPORT_MANIFEST_KIND = "external_recovery_plan_verifier_report"
APPROVED_DECISION = "approved_for_promotion_manifest"


class ExternalRecoveryPlanError(ValueError):
    """Raised when an external recovery plan is unsafe for verifier consumption."""


def build_external_recovery_plan_report(plan: Mapping[str, Any] | str | Path) -> dict[str, Any]:
    """Build a non-mutating report section from a P7-S2e dry-run plan.

    The returned report is verifier evidence only. It never represents the plan
    as canonical state and it fails closed if any operation claims it would
    mutate events or existing event payloads.
    """

    manifest, plan_sha256 = _load_plan(plan)
    _validate_plan_header(manifest)
    operations = manifest.get("operations")
    if not isinstance(operations, list):
        raise ExternalRecoveryPlanError("external recovery plan operations must be an array")

    rows = [_row_for_operation(operation) for operation in operations]
    counts = _counts(manifest, rows)
    by_authority = Counter(row["body_authority"] for row in rows)
    by_fidelity = Counter(row["body_fidelity"] for row in rows)

    section = {
        "label": "with_external_recovery_plan",
        "external_only": True,
        "canonical_event_log_state": False,
        "applies_changes": False,
        "appends_events": False,
        "modifies_existing_events": False,
        "plan_manifest_kind": manifest.get("manifest_kind"),
        "plan_sha256": plan_sha256,
        "input_promotion_review_sha256": manifest.get("input_promotion_review_sha256"),
        "input_audit_sha256": manifest.get("input_audit_sha256"),
        "input_ledger_sha256": manifest.get("input_ledger_sha256"),
        "counts": counts,
        "by_authority": dict(sorted(by_authority.items())),
        "by_fidelity": dict(sorted(by_fidelity.items())),
        "externally_recovered_roots": rows,
        "warnings": [
            "This section is verifier evidence only; it is not canonical event-log state.",
            "No req_created events were rewritten or appended by this report.",
        ],
    }
    return {
        "schema_version": 1,
        "manifest_kind": REPORT_MANIFEST_KIND,
        "with_external_recovery_plan": section,
    }


def _load_plan(plan: Mapping[str, Any] | str | Path) -> tuple[Mapping[str, Any], str | None]:
    if isinstance(plan, Mapping):
        return plan, None
    path = Path(plan)
    try:
        data = path.read_bytes()
    except FileNotFoundError as exc:
        raise ExternalRecoveryPlanError(f"external recovery plan not found: {path}") from exc
    try:
        parsed = json.loads(data)
    except json.JSONDecodeError as exc:
        raise ExternalRecoveryPlanError(f"{path}: invalid JSON external recovery plan: {exc.msg}") from exc
    if not isinstance(parsed, dict):
        raise ExternalRecoveryPlanError("external recovery plan JSON must be an object")
    return parsed, hashlib.sha256(data).hexdigest()


def _validate_plan_header(manifest: Mapping[str, Any]) -> None:
    if manifest.get("manifest_kind") != PLAN_MANIFEST_KIND:
        raise ExternalRecoveryPlanError(
            f"external recovery plan manifest_kind must be {PLAN_MANIFEST_KIND!r}"
        )
    if manifest.get("mode") != "dry_run":
        raise ExternalRecoveryPlanError("external recovery plan mode must be dry_run")
    if manifest.get("dry_run_only") is not True:
        raise ExternalRecoveryPlanError("external recovery plan must have dry_run_only=true")


def _row_for_operation(operation: Any) -> dict[str, Any]:
    if not isinstance(operation, Mapping):
        raise ExternalRecoveryPlanError("external recovery operation must be an object")
    message_id = str(operation.get("id") or "")
    if not message_id:
        raise ExternalRecoveryPlanError("external recovery operation missing id")
    if operation.get("would_append_event") is not False:
        raise ExternalRecoveryPlanError(f"{message_id}: external recovery plan would append events")
    if operation.get("would_modify_existing_event") is not False:
        raise ExternalRecoveryPlanError(
            f"{message_id}: external recovery plan would modify existing events"
        )
    if operation.get("provenance_validation") != "passed":
        raise ExternalRecoveryPlanError(f"{message_id}: provenance validation did not pass")
    if operation.get("review_decision") != APPROVED_DECISION:
        raise ExternalRecoveryPlanError(f"{message_id}: operation is not approved for promotion manifest")

    payload = operation.get("candidate_req_created_payload")
    if not isinstance(payload, Mapping):
        raise ExternalRecoveryPlanError(f"{message_id}: missing candidate req_created payload")
    body = str(payload.get("body") or "")
    if not body.strip():
        raise ExternalRecoveryPlanError(f"{message_id}: recovered body is empty")
    refs = payload.get("source_context_refs")
    if not isinstance(refs, list) or not refs:
        raise ExternalRecoveryPlanError(f"{message_id}: source_context_refs required")

    return {
        "id": message_id,
        "canonical_state": "unchanged",
        "external_recovery_available": True,
        "body_authority": str(payload.get("body_authority") or "unknown"),
        "body_fidelity": str(payload.get("body_fidelity") or "unknown"),
        "body_length": len(body),
        "source_context_ref_count": len(refs),
        "operation": operation.get("operation"),
    }


def _counts(manifest: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    source_counts = manifest.get("counts") or {}
    if not isinstance(source_counts, Mapping):
        raise ExternalRecoveryPlanError("external recovery plan counts must be an object")
    planned = int(source_counts.get("planned_operations", len(rows)))
    if planned != len(rows):
        raise ExternalRecoveryPlanError("external recovery plan planned_operations count mismatch")
    return {
        "approved_rows": int(source_counts.get("approved_rows", len(rows))),
        "planned_operations": planned,
        "rejected_rows_ignored": int(source_counts.get("rejected_rows_ignored", 0)),
        "externally_recovered_roots": len(rows),
    }
