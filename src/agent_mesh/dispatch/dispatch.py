"""Pure-core dispatch logic for the optional dispatch layer (host-agnostic, no I/O).

This module reasons about the canonical mesh event stream and produces dispatch decisions; it never
reads or writes a host path. A host injects a ``DispatchHost`` (its routes, risk policy, wave
lookup, command + reply templates, and UUID namespace) and a ``RunRecorder`` (where run records
persist). Cursor and lease persistence stay host-side: the engine's lease/cursor helpers are pure
(the host passes the current state in and persists what comes back).

Boundary: the engine carries no risk policy (``DispatchHost.classify`` owns that), no project paths,
and emits no canonical mesh events. ``build_run_record`` returns a privacy-safe value (sizes/hashes
only) that the host's ``RunRecorder`` persists.
"""
from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone

from agent_mesh.core.ids import new_ulid

from .adapters import DispatchHost, RunRecorder
from .eval import GATE_HALTS
from .types import Message, RunPlan, RunRecord


def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ----------------------------- event parsing -----------------------------

def to_message(event: dict, *, aliases: dict[str, list[str]] | None = None) -> Message:
    """Normalize one canonical event into a ``Message``. ``aliases`` (host-supplied) expands a
    recipient group like ``both -> [claude, codex]``; absent, recipients pass through verbatim."""
    payload = event.get("payload", {}) or {}
    raw_to = payload.get("to") or payload.get("original_to") or []
    if isinstance(raw_to, str):
        raw_to = [raw_to]
    aliases = aliases or {}
    recipients: list[str] = []
    for target in raw_to:
        recipients.extend(aliases.get(target, [target]))
    entity_id = event.get("entity_id", "")
    thread_id = event.get("thread_id", "") or entity_id
    if not thread_id:
        # fail loud: an event with neither an entity_id nor a thread_id has no identity, which would
        # yield an empty session key and collide leases/session ids across distinct requests.
        raise ValueError("event must carry an entity_id or thread_id")
    return Message(
        entity_id=entity_id,
        kind=event.get("kind", ""),
        event_seq=int(event.get("event_seq", 0)),
        thread_id=thread_id,
        sender=payload.get("from", ""),
        actor=event.get("actor", ""),
        feature=(payload.get("feature") or "").strip(),
        title=payload.get("title", ""),
        status=payload.get("status", ""),
        response_mode=(payload.get("response_mode") or "single"),
        recipients=tuple(recipients),
        refs=tuple(payload.get("refs", []) or []),
    )


def responders_by_thread(events: list[dict]) -> dict[str, set[str]]:
    """thread_id -> set of agents that have posted a res in that thread."""
    out: dict[str, set[str]] = {}
    for event in events:
        if event.get("kind") != "res_posted":
            continue
        thread = event.get("thread_id", "")
        sender = (event.get("payload", {}) or {}).get("from", "") or event.get("actor", "")
        if not sender:
            continue  # an unattributable res can neither credit an agent nor close a thread
        out.setdefault(thread, set()).add(sender)
    return out


# ----------------------------- session key (S1) -----------------------------

def resolve_session_key(message: Message, host: DispatchHost) -> tuple[str, str]:
    """The S1 fallback chain -> (session_key, source). Never empty. Step 3 (a referenced host row's
    wave) is delegated to ``host.wave_for_refs``; steps 2 (feature) and 4 (thread_id) are generic."""
    if message.feature and message.feature.lower() not in ("", "(none)", "none"):
        return message.feature, "feature"
    wave = host.wave_for_refs(list(message.refs))
    if wave:
        return wave, "wave_ref"
    return message.thread_id, "thread_id"


def session_uuid(source: str, session_key: str, *, namespace: uuid.UUID) -> str:
    """Source-prefixed so a feature name and a thread id can never collide; namespaced per host so
    the id is stable across restarts and unique to the project."""
    return str(uuid.uuid5(namespace, f"{source}:{session_key}"))


# ----------------------------- planning -----------------------------

def _gate_decision(
    classification: str, response_mode: str, recipient_count: int, grounding: dict | None
) -> tuple[str, str, str, int, tuple[str, ...]]:
    """Map (classification, response_mode, grounding) -> (gate, reason, reason_code, missing_count,
    requires_gate). Generic orchestration over the host's risk classification; carries no risk
    policy of its own. ``reason`` is human-readable for dry-run display; ``reason_code`` is the
    closed enum the canonical event records (the free text never enters a payload)."""
    missing_count = 0
    if classification == "routine":
        gate, reason, reason_code = "auto-dispatch", "routine (read-only + RES)", "routine"
    else:
        gate, reason, reason_code = (
            "hold-for-approval",
            "non-routine (default-deny)",
            "non-routine-default-deny",
        )
    if response_mode == "single" and recipient_count > 1:
        gate = "hold-for-approval"
        reason = "single response_mode with multiple recipients: select one or gate"
        reason_code = "single-mode-multi-recipient"
    # a fresh wave-scoped agent that cannot be grounded is a HARD stop, not a human-approvable apply.
    # Fail closed: a grounding dict that omits an explicit complete=True is treated as incomplete.
    # The reason carries only a COUNT, never the verbatim 'missing' text (which the run record
    # persists and which may be substrate-derived).
    if grounding and not grounding.get("complete", False):
        gate = "blocked-substrate-incomplete"
        # prefer the sanitized count from GroundingResult.telemetry(); fall back to a verbatim list's
        # length for a richer non-telemetry summary. Either way the reason exposes only a count.
        missing_count = int(grounding.get("missing_count", len(grounding.get("missing", []) or [])))
        reason = f"substrate-incomplete ({missing_count} missing)"
        reason_code = "substrate-incomplete"
    # only a held (human-approvable) apply carries the eval/gate halt-set; auto-dispatch and a hard
    # substrate block do not. Computed last, after the block override.
    requires_gate = tuple(GATE_HALTS) if gate == "hold-for-approval" else ()
    return gate, reason, reason_code, missing_count, requires_gate


def plan_for(
    message: Message,
    agent: str,
    host: DispatchHost,
    *,
    grounding: dict | None = None,
    now: str | None = None,
) -> RunPlan:
    """Build a dry-run dispatch plan for ``agent`` answering ``message``. Pure: all host specifics
    (routes, risk classification, wave lookup, command/reply templates, namespace) come through
    ``host``; nothing is executed or persisted."""
    session_key, source = resolve_session_key(message, host)
    sess_uuid = session_uuid(source, session_key, namespace=host.session_namespace())
    classification = host.classify(message)
    routes = host.routes()
    route = routes.get(agent, {"gen_ai_system": "unknown", "model": "unknown"})
    # the single-mode "multiple recipients" hold is about genuine ambiguity, so count UNIQUE ROUTED
    # recipients (a duplicate or an unrouted extra is not a second candidate to choose between).
    routed_recipients = len([a for a in dict.fromkeys(message.recipients) if a in routes])
    gate, reason, reason_code, missing_count, requires_gate = _gate_decision(
        classification, message.response_mode, routed_recipients, grounding
    )
    return RunPlan(
        run_id=new_ulid("run"),
        input_message_id=message.entity_id,
        target_agent=agent,
        target_event_seq=message.event_seq,
        thread_id=message.thread_id,
        session_key=session_key,
        session_key_source=source,
        session_uuid=sess_uuid,
        wave=session_key,
        classification=classification,
        response_mode=message.response_mode,
        gate=gate,
        gate_reason=reason,
        gate_reason_code=reason_code,
        missing_count=missing_count,
        requires_gate=requires_gate,
        gen_ai_system=str(route.get("gen_ai_system", "")),
        model=str(route.get("model", "")),
        would_run=host.build_command(agent, message, sess_uuid),
        post_reply_with=host.post_reply_template().format(
            agent=agent, req_id=message.entity_id, summary="<summary>", details="<details>"
        ),
        grounding=grounding or {},
        planned_utc=now or now_utc(),
    )


def find_pending(
    events: list[dict],
    since_seq: int,
    host: DispatchHost,
    *,
    aliases: dict[str, list[str]] | None = None,
    grounding_for=None,
) -> list[RunPlan]:
    """req_created events newer than ``since_seq`` with no res yet from a routed target. Pure: the
    host passes the cursor in. ``grounding_for(entity_id) -> dict`` optionally supplies a grounding
    telemetry dict per message (wired in the grounding slice)."""
    responders = responders_by_thread(events)
    routes = host.routes()
    plans: list[RunPlan] = []
    for event in events:
        if event.get("kind") != "req_created":
            continue
        if int(event.get("event_seq", 0)) <= since_seq:
            continue
        message = to_message(event, aliases=aliases)
        answered = responders.get(message.thread_id, set())
        # response_mode="single" means ONE response closes the thread: if anyone has answered, the
        # request is done. "multi" keeps per-agent remaining-target scheduling.
        if message.response_mode == "single" and answered:
            continue
        for agent in dict.fromkeys(message.recipients):  # dedup, order-preserving: one plan per agent
            if agent not in routes or agent in answered:
                continue
            grounding = grounding_for(message.entity_id) if grounding_for else None
            plans.append(plan_for(message, agent, host, grounding=grounding))
    return plans


# ----------------------------- lease model (pure; host persists) -----------------------------

def lease_key(plan: RunPlan) -> tuple[str, str]:
    """The dedupe key: one in-flight dispatch per (input message, target agent)."""
    return (plan.input_message_id, plan.target_agent)


def is_leased(plan: RunPlan, active_leases: set[tuple[str, str]]) -> bool:
    """Whether an open lease already covers this plan (the host supplies the current lease set)."""
    return lease_key(plan) in active_leases


def build_lease(plan: RunPlan, *, ttl_seconds: int = 3600, now: str | None = None) -> dict:
    """A durable BEFORE-launch lease record (the host appends it to its own store)."""
    return {
        "run_id": plan.run_id,
        "input_message_id": plan.input_message_id,
        "target_agent": plan.target_agent,
        "session_key": plan.session_key,
        "session_uuid": plan.session_uuid,
        "target_event_seq": plan.target_event_seq,
        "status": "open",
        "mode": plan.run_mode,
        "ttl_seconds": ttl_seconds,
        "created_utc": now or now_utc(),
    }


# ----------------------------- run record (built here; host persists) -----------------------------

def _grounding_summary(grounding: dict) -> dict:
    """A privacy-safe view of the grounding for the run record: only the complete flag plus a digest
    of the full dict -- never its verbatim content. A host's grounding may carry substrate-derived
    text (e.g. a free-text 'missing' list), which must not be persisted in telemetry."""
    if not grounding:
        return {}
    blob = json.dumps(grounding, sort_keys=True, default=str)
    return {"complete": bool(grounding.get("complete", False)),
            "digest": hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]}


def build_run_record(plan: RunPlan, *, route: dict | None = None) -> RunRecord:
    """An OTel-GenAI-shaped, privacy-safe run record. NO raw prompt/message bodies and no verbatim
    grounding: only a ``dry_run_artifact_hash`` over the planned command + reply, a grounding
    summary (complete + digest), plus sizes/ids. The host's ``RunRecorder`` decides where it lands;
    the package emits no canonical mesh event."""
    route = route or {}
    artifact = hashlib.sha256(
        (plan.would_run + "\n" + plan.post_reply_with).encode("utf-8")
    ).hexdigest()[:16]
    telemetry: dict[str, object] = {
        "event": "agent_run_planned",
        "run_id": plan.run_id,
        "run_mode": plan.run_mode,
        "target_agent": plan.target_agent,
        "gen_ai.system": plan.gen_ai_system,
        "gen_ai.request.model": plan.model,
        "input_message_id": plan.input_message_id,
        "output_message_id": None,
        "thread_id": plan.thread_id,
        "target_event_seq": plan.target_event_seq,
        "session_key": plan.session_key,
        "session_key_source": plan.session_key_source,
        "wave": plan.wave,
        "response_mode": plan.response_mode,
        "classification": plan.classification,
        "gate": plan.gate,
        "gate_reason": plan.gate_reason,
        "requires_gate": list(plan.requires_gate),
        "grounding": _grounding_summary(plan.grounding),
        "dry_run_artifact_hash": artifact,
        "adapter_capabilities": {
            "cache_ttl_control": bool(route.get("cache_ttl_control", False)),
            "cache_prewarm": bool(route.get("cache_prewarm", False)),
        },
        "status": plan.status,
        "planned_utc": plan.planned_utc,
        "input_tokens": None,
        "cache_read_input_tokens": None,
        "cache_creation_input_tokens": None,
        "total_input_tokens": None,
    }
    return RunRecord(run_id=plan.run_id, run_mode=plan.run_mode, status=plan.status, telemetry=telemetry)


def record_run(plan: RunPlan, recorder: RunRecorder, *, route: dict | None = None) -> RunRecord:
    """Build the run record and hand it to the host's recorder for persistence. Returns the record."""
    record = build_run_record(plan, route=route)
    recorder.record(record)
    return record
