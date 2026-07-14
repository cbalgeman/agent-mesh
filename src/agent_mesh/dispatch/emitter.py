"""Canonical dispatch-event emitter: run records as hash-chained ``events.jsonl`` events.

This is the live-gate B emitter. It maps a privacy-safe ``RunPlan`` into the dispatch domain's
canonical event payloads (``docs/domains/dispatch.md`` §1) and appends them to ``events.jsonl`` via
the substrate's durable, hash-chained ``append_event``. It is the bridge from the old host-local
``.dispatch/runs.jsonl`` sink to auditable canonical events.

Two privacy disciplines are enforced here, on top of the substrate's own pre-append guard:
  * The payload is built FRESH from the plan, never copied from ``build_run_record()``'s telemetry:
    the free-text ``gate_reason`` becomes the closed ``gate_reason_code`` enum, and the artifact is
    named ``plan_artifact_hash`` (a sha256[:16] over the planned command + reply, never the strings).
  * Every payload is run through ``validate_dispatch_payload`` BEFORE the append call, so a body-leak
    is rejected at the emitter boundary, not just at the substrate boundary.

Live emission (leases + started/completed/failed) is defined here for completeness and fixtures but
remains NO-GO until human signoff; the audit path (``record_plan``) emits only planned/blocked and
acquires no lease.
"""
from __future__ import annotations

import hashlib
import json

from agent_mesh.core.chain import ChainAnchor, ChainResult, capture_anchor, verify_chain
from agent_mesh.core.dispatch_schema import validate_dispatch_payload
from agent_mesh.core.events import AppendResult, Event, append_event, generate_event_id

from .types import RunPlan

__all__ = [
    "planned_payload",
    "blocked_payload",
    "record_plan",
    "emit_run_planned",
    "emit_run_blocked",
    "emit_lease_acquired",
    "emit_lease_released",
    "emit_run_started",
    "emit_run_completed",
    "emit_run_failed",
    "capture_anchor",
    "verify_appended",
]


def verify_appended(events_path, anchor: ChainAnchor) -> ChainResult:
    """Verify the lifecycle events just appended chain onto ``anchor`` (the tail captured BEFORE the
    batch). This is the live-gate C standalone seam for the dispatch live-launch writer, which writes
    canonical events directly through ``append_event`` (outside ``guarded_apply``): capture an anchor
    with ``capture_anchor`` before a batch of ``emit_*`` calls, then call this after.

    It checks ONLY the appended suffix against the known-good anchor (not the pre-existing log -- the
    CLI / periodic full walk is the audit backstop). A non-ok result means the run's writes broke the
    chain: the caller MUST halt the run and surface it, and MUST NOT mark the run complete. Returns a
    ``ChainResult``; never raises for an ordinary integrity failure. Thin wrapper over core
    ``verify_chain`` so the dispatch writer has one obvious call.
    """
    return verify_chain(events_path, anchor=anchor)


def _digest_of(blob: str) -> str:
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


def _grounding_summary(grounding: dict | None) -> dict:
    """Always ``{complete: bool, digest: 16-hex}``: a privacy-safe view that drops every verbatim
    grounding string (which may be substrate-derived), keeping only the flag and a digest."""
    grounding = grounding or {}
    blob = json.dumps(grounding, sort_keys=True, default=str)
    return {"complete": bool(grounding.get("complete", False)), "digest": _digest_of(blob)}


def _block_reason_codes(plan: RunPlan) -> list[str]:
    """The withhold reasons as enum codes (never free text): the substrate-incomplete code for a hard
    block, or the apply-gate halt categories for a held risky gate."""
    if plan.gate == "blocked-substrate-incomplete":
        return ["substrate-incomplete"]
    return list(plan.requires_gate)


# --- pure payload builders (testable without I/O) ----------------------------------------------

def planned_payload(plan: RunPlan, *, run_mode: str, status: str, route: dict | None = None) -> dict:
    route = route or {}
    return {
        "run_id": plan.run_id,
        "run_mode": run_mode,
        "input_message_id": plan.input_message_id,
        "target_agent": plan.target_agent,
        "gen_ai_system": plan.gen_ai_system,
        "model": plan.model,
        "session_key": plan.session_key,
        "session_key_source": plan.session_key_source,
        "session_uuid": plan.session_uuid,
        "wave": plan.wave,
        "classification": plan.classification,
        "gate": plan.gate,
        "gate_reason_code": plan.gate_reason_code,
        "requires_gate": list(plan.requires_gate),
        "grounding": _grounding_summary(plan.grounding),
        "plan_artifact_hash": _digest_of(plan.would_run + "\n" + plan.post_reply_with),
        "adapter_capabilities": {
            "cache_ttl_control": bool(route.get("cache_ttl_control", False)),
            "cache_prewarm": bool(route.get("cache_prewarm", False)),
        },
        "target_event_seq": plan.target_event_seq,
        "response_mode": plan.response_mode,
        "planned_utc": plan.planned_utc,
        "status": status,
    }


def blocked_payload(plan: RunPlan, *, run_mode: str) -> dict:
    return {
        "run_id": plan.run_id,
        "run_mode": run_mode,
        "input_message_id": plan.input_message_id,
        "target_agent": plan.target_agent,
        "gate": plan.gate,
        "block_reason_codes": _block_reason_codes(plan),
        "missing_count": int(plan.missing_count),
        "planned_utc": plan.planned_utc,
    }


# --- append helpers ----------------------------------------------------------------------------

def _append(
    events_path,
    kind: str,
    *,
    entity_id: str,
    thread_id: str,
    payload: dict,
    actor: str,
    lock_acquired: bool = False,
) -> AppendResult:
    # Fail fast at the emitter boundary; append_event re-validates as the substrate's own guard.
    validate_dispatch_payload(kind, payload)
    event = Event(
        event_id=generate_event_id(),
        kind=kind,
        entity_id=entity_id,
        thread_id=thread_id,
        payload=payload,
        actor=actor,
    )
    return append_event(events_path, event, lock_acquired=lock_acquired)


def emit_run_planned(
    plan: RunPlan,
    *,
    events_path,
    run_mode: str = "dry_run",
    status: str | None = None,
    route: dict | None = None,
    actor: str = "dispatcher",
    lock_acquired: bool = False,
) -> AppendResult:
    status = status or ("planned" if run_mode == "live" else "dry_run")
    payload = planned_payload(plan, run_mode=run_mode, status=status, route=route)
    return _append(
        events_path,
        "dispatch_run_planned",
        entity_id=plan.run_id,
        thread_id=plan.thread_id,
        payload=payload,
        actor=actor,
        lock_acquired=lock_acquired,
    )


def emit_run_blocked(
    plan: RunPlan,
    *,
    events_path,
    run_mode: str = "dry_run",
    actor: str = "dispatcher",
) -> AppendResult:
    payload = blocked_payload(plan, run_mode=run_mode)
    return _append(
        events_path,
        "dispatch_run_blocked",
        entity_id=plan.run_id,
        thread_id=plan.thread_id,
        payload=payload,
        actor=actor,
    )


def record_plan(
    plan: RunPlan,
    *,
    events_path,
    run_mode: str = "dry_run",
    route: dict | None = None,
    actor: str = "dispatcher",
) -> list[AppendResult]:
    """Emit the canonical run record for one plan (the AUDIT path).

    Always emits ``dispatch_run_planned`` (the universal lifecycle entry); additionally emits
    ``dispatch_run_blocked`` when the plan was withheld (a held risky gate or a substrate-incomplete
    hard block). Acquires NO lease: a dry-run preview must never create a stale open lease.
    """
    status = "planned" if run_mode == "live" else "dry_run"
    results = [
        emit_run_planned(
            plan, events_path=events_path, run_mode=run_mode, status=status, route=route, actor=actor
        )
    ]
    if plan.gate in ("hold-for-approval", "blocked-substrate-incomplete"):
        results.append(emit_run_blocked(plan, events_path=events_path, run_mode=run_mode, actor=actor))
    return results


# --- live lifecycle emitters (DORMANT until live dispatch is signed off) ------------------------

def emit_lease_acquired(
    *,
    events_path,
    lease_id: str,
    run_id: str,
    input_message_id: str,
    target_agent: str,
    session_uuid: str,
    thread_id: str,
    ttl_seconds: int,
    created_utc: str,
    actor: str = "dispatcher",
    lock_acquired: bool = False,
) -> AppendResult:
    payload = {
        "lease_id": lease_id,
        "run_id": run_id,
        "input_message_id": input_message_id,
        "target_agent": target_agent,
        "session_uuid": session_uuid,
        "ttl_seconds": int(ttl_seconds),
        "created_utc": created_utc,
    }
    return _append(
        events_path,
        "dispatch_lease_acquired",
        entity_id=lease_id,
        thread_id=thread_id,
        payload=payload,
        actor=actor,
        lock_acquired=lock_acquired,
    )


def emit_lease_released(
    *,
    events_path,
    lease_id: str,
    run_id: str,
    thread_id: str,
    reason: str,
    released_utc: str,
    superseded_by_run_id: str | None = None,
    actor: str = "dispatcher",
    lock_acquired: bool = False,
) -> AppendResult:
    payload = {
        "lease_id": lease_id,
        "run_id": run_id,
        "reason": reason,
        "released_utc": released_utc,
    }
    if reason == "superseded":
        if not superseded_by_run_id:
            # Fail with the meaningful condition, not the body-leak code an empty string would trip.
            raise ValueError("a superseded release requires a superseded_by_run_id")
        payload["superseded_by_run_id"] = superseded_by_run_id
    return _append(
        events_path,
        "dispatch_lease_released",
        entity_id=lease_id,
        thread_id=thread_id,
        payload=payload,
        actor=actor,
        lock_acquired=lock_acquired,
    )


def emit_run_started(
    *,
    events_path,
    run_id: str,
    thread_id: str,
    session_uuid: str,
    started_utc: str,
    actor: str = "dispatcher",
    lock_acquired: bool = False,
) -> AppendResult:
    payload = {"run_id": run_id, "session_uuid": session_uuid, "started_utc": started_utc}
    return _append(
        events_path,
        "dispatch_run_started",
        entity_id=run_id,
        thread_id=thread_id,
        payload=payload,
        actor=actor,
        lock_acquired=lock_acquired,
    )


def emit_run_completed(
    *,
    events_path,
    run_id: str,
    thread_id: str,
    output_message_id: str,
    completed_utc: str,
    input_tokens: int | None = None,
    cache_read_input_tokens: int | None = None,
    cache_creation_input_tokens: int | None = None,
    total_input_tokens: int | None = None,
    actor: str = "dispatcher",
    lock_acquired: bool = False,
) -> AppendResult:
    payload = {
        "run_id": run_id,
        "output_message_id": output_message_id,
        "input_tokens": input_tokens,
        "cache_read_input_tokens": cache_read_input_tokens,
        "cache_creation_input_tokens": cache_creation_input_tokens,
        "total_input_tokens": total_input_tokens,
        "completed_utc": completed_utc,
    }
    return _append(
        events_path,
        "dispatch_run_completed",
        entity_id=run_id,
        thread_id=thread_id,
        payload=payload,
        actor=actor,
        lock_acquired=lock_acquired,
    )


def emit_run_failed(
    *,
    events_path,
    run_id: str,
    thread_id: str,
    error_class: str,
    failed_utc: str,
    actor: str = "dispatcher",
    lock_acquired: bool = False,
    response_candidate_status: str | None = None,
    response_candidate_reason: str | None = None,
) -> AppendResult:
    payload = {"run_id": run_id, "error_class": error_class, "failed_utc": failed_utc}
    if response_candidate_status is not None:
        payload["response_candidate_status"] = response_candidate_status
    if response_candidate_reason is not None:
        payload["response_candidate_reason"] = response_candidate_reason
    return _append(
        events_path,
        "dispatch_run_failed",
        entity_id=run_id,
        thread_id=thread_id,
        payload=payload,
        actor=actor,
        lock_acquired=lock_acquired,
    )
