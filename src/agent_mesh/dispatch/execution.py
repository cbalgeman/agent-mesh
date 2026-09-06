"""Identity-bound execution seam for dispatch lifecycle events.

This module bridges a planned dispatch run to a local agent runtime. Every agent launch requires an
automatic durable-instance handshake before lifecycle events are written. It owns the live lifecycle
around a process launch: acquire lease, mark started, bind the child instance, launch, classify the
result, optionally post an accepted response, and release the run and child. Raw prompts and process
output remain outside canonical dispatch events; events carry only ids/category codes and token
counters.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
import time
from typing import Callable, Protocol

from agent_mesh.config import AgentMeshConfig, config_from_agent_dir
from agent_mesh.core.agent_instances import (
    RuntimeIdentityHandshake,
    bind_agent_instance,
    reduce_agent_instances,
    reset_agent_instance,
    resolve_agent_instance_from_records,
    selected_agent_instance,
)
from agent_mesh.core.chain import ChainAnchor, ChainResult
from agent_mesh.core.events import (
    Event,
    _CapabilityAppendReceipt,
    append_event,
    generate_event_id,
)
from agent_mesh.core.instance_registry import (
    ResolvedRuntimeIdentity,
    append_instance_launch_bound,
    preflight_runtime_identity,
    reconcile_instance_terminal_suffix,
    resolve_or_register_runtime_identity,
)
from agent_mesh.core.lock import LockHandle
from agent_mesh.store.read_model import ReadModelUnavailable, capture_event_snapshot
from agent_mesh.store.rebuild import read_event_records
from agent_mesh.core.ids import new_public_message_id, new_ulid

from .adapters import AgentRuntimeAdapter
from .dispatch import now_utc
from .emitter import (
    capture_anchor,
    emit_lease_acquired,
    emit_lease_released,
    emit_run_completed,
    emit_run_failed,
    emit_run_terminated,
    emit_run_planned,
    emit_run_started,
    planned_payload,
    verify_appended,
)
from .output_policy import extract_response_candidate
from .types import AgentLaunchResult, AgentLaunchSpec, AgentRunRequest, RunPlan


class AgentLauncher(Protocol):
    """Minimal process launcher protocol used by the execution seam."""

    def launch(self, spec: AgentLaunchSpec) -> AgentLaunchResult:
        raise NotImplementedError


VerifyFn = Callable[[Path, ChainAnchor], ChainResult]
PrelaunchRevalidateFn = Callable[[], tuple[bool, str]]
LockPhaseFn = Callable[[], None]
ReacquireLockFn = Callable[[], LockHandle | None]
DISPATCH_RECOVERY_MAX_BYTES = 64 * 1024 * 1024
DISPATCH_RECOVERY_MAX_EVENTS = 50_000
DISPATCH_RECOVERY_MAX_SECONDS = 5.0


class DispatchLockReacquireError(RuntimeError):
    """Raised when canonical ownership cannot be restored after child launch."""


def _verified_dispatch_recovery_records(
    config: AgentMeshConfig,
    *,
    deadline_monotonic: float,
) -> tuple[dict, ...]:
    try:
        snapshot = capture_event_snapshot(
            config,
            max_bytes=DISPATCH_RECOVERY_MAX_BYTES,
            max_events=DISPATCH_RECOVERY_MAX_EVENTS,
            deadline_monotonic=deadline_monotonic,
        )
    except ReadModelUnavailable as exc:
        raise ValueError(f"DISPATCH_RECOVERY_SNAPSHOT_UNAVAILABLE: {exc}") from exc
    return snapshot.records


def verified_dispatch_run_recovery_complete(
    config: AgentMeshConfig,
    run_id: str,
    *,
    deadline_monotonic: float | None = None,
) -> bool:
    """Prove lifecycle closure from one bounded hash-chain-verified snapshot."""

    deadline = (
        deadline_monotonic
        if deadline_monotonic is not None
        else time.monotonic() + DISPATCH_RECOVERY_MAX_SECONDS
    )
    return dispatch_run_recovery_complete(
        list(_verified_dispatch_recovery_records(config, deadline_monotonic=deadline)),
        run_id,
    )


def recover_interrupted_managed_run(
    config,
    *,
    run_id: str,
    actor: str,
    lock_acquired: bool,
    now: str | None = None,
    verify: VerifyFn = verify_appended,
) -> bool:
    """Idempotently terminalize one proven-interrupted Workbench dispatch."""

    if not lock_acquired:
        raise ValueError("dispatch recovery requires the canonical lock")
    deadline = time.monotonic() + DISPATCH_RECOVERY_MAX_SECONDS
    records = list(_verified_dispatch_recovery_records(config, deadline_monotonic=deadline))
    planned = _run_event(records, kind="dispatch_run_planned", run_id=run_id)
    if planned is None:
        # The machine marker is written before the first planned append. A
        # missing run therefore means no canonical suffix needs repair.
        return False
    changed = False
    terminal = _terminal_event_for_run(records, run_id)
    stamp = now or now_utc()
    if terminal is None:
        response = _response_for_dispatch_run(records, run_id)
        if response is not None:
            _append_and_verify(
                config.events_path,
                lambda: emit_run_completed(
                    events_path=config.events_path,
                    run_id=run_id,
                    thread_id=str(planned.get("thread_id") or ""),
                    output_message_id=str(response["entity_id"]),
                    completed_utc=stamp,
                    actor=actor,
                    lock_acquired=True,
                ),
                verify=verify,
            )
        else:
            _append_and_verify(
                config.events_path,
                lambda: emit_run_terminated(
                    events_path=config.events_path,
                    run_id=run_id,
                    thread_id=str(planned.get("thread_id") or ""),
                    terminal_state="parent_lost",
                    terminated_utc=stamp,
                    actor=actor,
                    lock_acquired=True,
                ),
                verify=verify,
            )
        changed = True
        records = list(_verified_dispatch_recovery_records(config, deadline_monotonic=deadline))
        terminal = _terminal_event_for_run(records, run_id)
    if terminal is None:  # pragma: no cover - append verification is fail closed
        raise ValueError("DISPATCH_RECOVERY_TERMINAL_MISSING")

    kind = str(terminal.get("kind") or "")
    payload_value = terminal.get("payload")
    payload: dict = payload_value if isinstance(payload_value, dict) else {}
    if kind == "dispatch_run_completed":
        terminal_utc = str(payload.get("completed_utc") or stamp)
        lease_reason = "completed"
        instance_outcome = "completed"
    elif kind == "dispatch_run_failed":
        terminal_utc = str(payload.get("failed_utc") or stamp)
        lease_reason = "failed"
        instance_outcome = "failed"
    else:
        terminal_utc = str(payload.get("terminated_utc") or stamp)
        terminal_state = str(payload.get("terminal_state") or "")
        lease_reason = {
            "cancelled": "cancelled",
            "timed_out": "timeout",
            "parent_lost": "parent_loss",
        }.get(terminal_state, "failed")
        instance_outcome = lease_reason
    changed = _reconcile_terminated_run_suffixes(
        config,
        records=records,
        planned=planned,
        run_id=run_id,
        actor=actor,
        terminal_utc=terminal_utc,
        lease_reason=lease_reason,
        instance_outcome=instance_outcome,
        verify=verify,
    ) or changed
    if not verified_dispatch_run_recovery_complete(
        config,
        run_id,
        deadline_monotonic=deadline,
    ):
        raise ValueError("DISPATCH_RECOVERY_SUFFIX_INCOMPLETE")
    return changed


def dispatch_run_recovery_complete(records: list[dict], run_id: str) -> bool:
    """Prove that an interrupted run has no open canonical lifecycle suffix."""

    run_records = [record for record in records if record.get("entity_id") == run_id]
    planned = [record for record in run_records if record.get("kind") == "dispatch_run_planned"]
    if not planned:
        return not run_records
    terminals = [
        record
        for record in run_records
        if record.get("kind")
        in {"dispatch_run_completed", "dispatch_run_failed", "dispatch_run_terminated"}
    ]
    if len(planned) != 1 or len(terminals) != 1:
        return False
    lease = _run_event(records, kind="dispatch_lease_acquired", run_id=run_id)
    if lease is not None and not _lease_released_for_run(records, run_id):
        return False
    launch_bound = next(
        (
            record
            for record in records
            if record.get("kind") == "agent_instance_launch_bound"
            and isinstance(record.get("payload"), dict)
            and record["payload"].get("run_id") == run_id
        ),
        None,
    )
    if launch_bound is None:
        return True
    identity = _identity_for_run(records, run_id)
    if identity is None:
        return False
    if not _instance_run_suffix_exists(
        records,
        kind="agent_instance_launch_released",
        instance_id=identity.instance_id,
        run_id=run_id,
    ):
        return False
    return not identity.one_shot or _instance_run_suffix_exists(
        records,
        kind="agent_instance_terminal",
        instance_id=identity.instance_id,
        run_id=run_id,
    )


def reconcile_expired_policy_attempt(
    config,
    *,
    policy_id: str,
    actor: str,
    lock_acquired: bool,
    now: str | None = None,
    verify: VerifyFn = verify_appended,
) -> bool:
    """Terminalize an abandoned latest attempt only after its canonical bound expires."""

    if not lock_acquired:
        raise ValueError("dispatch parent-loss reconciliation requires the canonical lock")
    records = read_event_records(config.events_path)
    planned = [
        record
        for record in records
        if record.get("kind") == "dispatch_run_planned"
        and isinstance(record.get("payload"), dict)
        and record["payload"].get("policy_id") == policy_id
    ]
    if not planned:
        return False
    planned.sort(key=lambda record: int(record.get("event_seq") or 0))
    attempt = planned[-1]
    run_id = str(attempt.get("entity_id") or "")
    existing = _terminal_event_for_run(records, run_id)
    if existing is not None:
        if not (
            existing.get("kind") == "dispatch_run_terminated"
            and str(existing.get("payload", {}).get("terminal_state")) == "parent_lost"
        ):
            return False
        terminated_utc = str(existing.get("payload", {}).get("terminated_utc") or now or now_utc())
        return _reconcile_terminated_run_suffixes(
            config,
            records=records,
            planned=attempt,
            run_id=run_id,
            actor=actor,
            terminal_utc=terminated_utc,
            lease_reason="parent_loss",
            instance_outcome="parent_loss",
            verify=verify,
        )
    stamp = now or now_utc()
    current = _parse_dispatch_utc(stamp)
    lease = next(
        (
            record
            for record in records
            if record.get("kind") == "dispatch_lease_acquired"
            and isinstance(record.get("payload"), dict)
            and record["payload"].get("run_id") == run_id
        ),
        None,
    )
    if lease is None:
        planned_utc = _parse_dispatch_utc(str(attempt["payload"]["planned_utc"]))
        if current < planned_utc + timedelta(seconds=600):
            return False
    else:
        lease_payload = lease["payload"]
        created = _parse_dispatch_utc(str(lease_payload["created_utc"]))
        ttl = int(lease_payload["ttl_seconds"])
        if current < created + timedelta(seconds=ttl):
            return False
    thread_id = str(attempt.get("thread_id") or "")
    _append_and_verify(
        config.events_path,
        lambda: emit_run_terminated(
            events_path=config.events_path,
            run_id=run_id,
            thread_id=thread_id,
            terminal_state="parent_lost",
            terminated_utc=stamp,
            actor=actor,
            lock_acquired=True,
        ),
        verify=verify,
    )
    _reconcile_terminated_run_suffixes(
        config,
        records=records,
        planned=attempt,
        run_id=run_id,
        actor=actor,
        terminal_utc=stamp,
        lease_reason="parent_loss",
        instance_outcome="parent_loss",
        verify=verify,
    )
    return True


def cancel_managed_run(
    config,
    *,
    run_id: str,
    actor: str,
    lock_acquired: bool,
    now: str | None = None,
    verify: VerifyFn = verify_appended,
) -> bool:
    """Cancel one active canonical run; any later process result is discarded."""

    if not lock_acquired:
        raise ValueError("dispatch cancellation requires the canonical lock")
    records = read_event_records(config.events_path)
    planned = _run_event(records, kind="dispatch_run_planned", run_id=run_id)
    if planned is None:
        raise ValueError("DISPATCH_CANCEL_RUN_MISSING")
    existing = _terminal_event_for_run(records, run_id)
    if existing is not None:
        if not (
            existing.get("kind") == "dispatch_run_terminated"
            and str(existing.get("payload", {}).get("terminal_state")) == "cancelled"
        ):
            return False
        cancelled_utc = str(existing.get("payload", {}).get("terminated_utc") or now or now_utc())
        _reconcile_terminated_run_suffixes(
            config,
            records=records,
            planned=planned,
            run_id=run_id,
            actor=actor,
            terminal_utc=cancelled_utc,
            lease_reason="cancelled",
            instance_outcome="cancelled",
            verify=verify,
        )
        return True
    stamp = now or now_utc()
    thread_id = str(planned.get("thread_id") or "")
    _append_and_verify(
        config.events_path,
        lambda: emit_run_terminated(
            events_path=config.events_path,
            run_id=run_id,
            thread_id=thread_id,
            terminal_state="cancelled",
            terminated_utc=stamp,
            actor=actor,
            lock_acquired=True,
        ),
        verify=verify,
    )
    _reconcile_terminated_run_suffixes(
        config,
        records=records,
        planned=planned,
        run_id=run_id,
        actor=actor,
        terminal_utc=stamp,
        lease_reason="cancelled",
        instance_outcome="cancelled",
        verify=verify,
    )
    return True


def _reconcile_terminated_run_suffixes(
    config,
    *,
    records: list[dict],
    planned: dict,
    run_id: str,
    actor: str,
    terminal_utc: str,
    lease_reason: str,
    instance_outcome: str,
    verify: VerifyFn,
) -> bool:
    """Idempotently complete the lease and instance suffix of a terminated run."""

    thread_id = str(planned.get("thread_id") or "")
    changed = False
    lease = next(
        (
            record
            for record in records
            if record.get("kind") == "dispatch_lease_acquired"
            and isinstance(record.get("payload"), dict)
            and record["payload"].get("run_id") == run_id
        ),
        None,
    )
    if lease is not None and not _lease_released_for_run(records, run_id):
        _append_and_verify(
            config.events_path,
            lambda: emit_lease_released(
                events_path=config.events_path,
                lease_id=str(lease["payload"]["lease_id"]),
                run_id=run_id,
                thread_id=thread_id,
                reason=lease_reason,
                released_utc=terminal_utc,
                actor=actor,
                lock_acquired=True,
            ),
            verify=verify,
        )
        changed = True
    identity = _identity_for_run(records, run_id)
    if identity is not None:
        instance_suffix_incomplete = not _instance_run_suffix_exists(
            records,
            kind="agent_instance_launch_released",
            instance_id=identity.instance_id,
            run_id=run_id,
        ) or (
            identity.one_shot
            and not _instance_run_suffix_exists(
                records,
                kind="agent_instance_terminal",
                instance_id=identity.instance_id,
                run_id=run_id,
            )
        )
        reconcile_instance_terminal_suffix(
            config,
            identity,
            run_id=run_id,
            outcome=instance_outcome,
            dispatcher=actor,
            dispatcher_instance_id=_dispatcher_instance_id(config.events_path, actor=actor),
            lock_acquired=True,
        )
        changed = changed or instance_suffix_incomplete
    return changed


def _instance_run_suffix_exists(
    records: list[dict],
    *,
    kind: str,
    instance_id: str,
    run_id: str,
) -> bool:
    return any(
        record.get("kind") == kind
        and record.get("entity_id") == instance_id
        and isinstance(record.get("payload"), dict)
        and record["payload"].get("run_id") == run_id
        for record in records
    )


def execute_launch_plan(
    plan: RunPlan,
    *,
    events_path: str | Path,
    runtime_adapter: AgentRuntimeAdapter,
    launcher: AgentLauncher,
    project_root: str | Path,
    prompt: str,
    timeout_seconds: int = 3600,
    ttl_seconds: int | None = None,
    run_mode: str = "live",
    now: str | None = None,
    actor: str = "dispatcher",
    verify: VerifyFn = verify_appended,
    lock_acquired: bool = False,
    post_response: bool = False,
    prelaunch_revalidate: PrelaunchRevalidateFn | None = None,
    release_lock_for_launch: LockPhaseFn | None = None,
    reacquire_lock_after_launch: ReacquireLockFn | None = None,
    capability_append_receipt: _CapabilityAppendReceipt | None = None,
    lock_handle: LockHandle | None = None,
) -> AgentLaunchResult:
    """Execute one planned launch and emit canonical lifecycle events.

    ``prompt`` is handed only to the runtime adapter/launcher as in-memory stdin text. Dispatch
    events record no prompt, stdout, stderr, command, reply template, or raw error text. The runtime
    adapter must provide an automatic identity handshake for every launch, including launch-only
    execution. When ``post_response`` is true, only a candidate accepted through the runtime
    adapter's trusted response boundary is appended via the normal ``res_posted`` event shape
    before ``dispatch_run_completed`` is emitted. Generic process adapters require the explicit
    textual fence; the built-in Codex adapter may instead use its provider-delimited final message.
    """

    if run_mode != "live":
        raise ValueError("execute_launch_plan currently supports only run_mode='live'")
    if bool(release_lock_for_launch) != bool(reacquire_lock_after_launch):
        raise ValueError("dispatch launch lock phase callbacks must be supplied together")
    path = Path(events_path)
    config = config_from_agent_dir(path.parent)
    current_lock_handle = lock_handle
    stamp = now or now_utc()
    lease_ttl_seconds = ttl_seconds if ttl_seconds is not None else timeout_seconds + 300
    if not 1 <= lease_ttl_seconds <= 86_400:
        raise ValueError("dispatch lease ttl must be between 1 and 86400 seconds")
    lease_id = new_ulid("lease")
    request = AgentRunRequest(
        run_id=plan.run_id,
        target_agent=plan.target_agent,
        session_uuid=plan.session_uuid,
        project_root=Path(project_root),
        prompt=prompt,
        workstream=plan.wave or plan.session_key,
        timeout_seconds=timeout_seconds,
    )
    if post_response:
        if not lock_acquired:
            raise ValueError("post_response requires caller-held mail lock")
        if plan.thread_id != plan.input_message_id:
            raise ValueError("post_response plan thread_id must equal input_message_id")

    initial_records = read_event_records(path)
    initial_planned = _run_event(
        initial_records,
        kind="dispatch_run_planned",
        run_id=plan.run_id,
    )
    if initial_planned is not None:
        # A run ID is a canonical idempotency key. Validate the complete plan
        # identity before any recovery helper can append a missing suffix or
        # return an already-terminal result.
        _validate_recovery_plan(initial_planned, plan)

    dispatcher_instance_id = _dispatcher_instance_id(path, actor=actor)
    recovered = _reconcile_existing_managed_run(
        config,
        plan=plan,
        events_path=path,
        actor=actor,
        dispatcher_instance_id=dispatcher_instance_id,
        lock_acquired=lock_acquired,
        verify=verify,
        now=stamp,
    )
    if recovered is not None:
        return recovered

    if not _runtime_identity_provider_configured(runtime_adapter):
        raise ValueError(
            "AGENT_INSTANCE_RUNTIME_UNBOUND: agent dispatch requires an "
            "automatic runtime identity handshake"
        )
    if not lock_acquired:
        raise ValueError("AGENT_INSTANCE_HANDSHAKE_REQUIRES_CANONICAL_LOCK")
    if post_response:
        _ensure_response_candidate_post_allowed(
            path,
            plan.input_message_id,
            allow_superseding=bool(plan.policy_id and plan.attempt_number > 1),
        )

    records = read_event_records(path)
    planned = _run_event(records, kind="dispatch_run_planned", run_id=plan.run_id)
    if planned is None:
        _ensure_no_other_unfinished_run(records, plan)
        provider_inventory_digest = _prepare_runtime_identity_handshake(runtime_adapter, request)
        _append_and_verify(
            path,
            append_fn=lambda: emit_run_planned(
                plan,
                events_path=path,
                run_mode=run_mode,
                status="planned",
                provider_inventory_digest=provider_inventory_digest,
                actor=actor,
                lock_acquired=lock_acquired,
                capability_append_receipt=capability_append_receipt,
                lock_handle=current_lock_handle,
            ),
            verify=verify,
        )
    else:
        _validate_recovery_plan(planned, plan)

    try:
        handshake = _runtime_identity_handshake(runtime_adapter, request)
    except Exception:
        _abort_runtime_identity_handshake(runtime_adapter, request)
        raise
    if handshake is None:
        _abort_runtime_identity_handshake(runtime_adapter, request)
        raise ValueError(
            "AGENT_INSTANCE_RUNTIME_UNBOUND: agent dispatch requires an "
            "automatic runtime identity handshake"
        )

    if handshake.participant.strip().lower() != plan.target_agent:
        _abort_runtime_identity_handshake(runtime_adapter, request)
        raise ValueError("AGENT_INSTANCE_RUNTIME_TARGET_MISMATCH")
    try:
        preflight_runtime_identity(
            config,
            handshake,
            registrar=actor,
            registration_origin="dispatch",
            parent_instance_id=dispatcher_instance_id,
            originating_run_id=plan.run_id,
        )
    except Exception:
        _abort_runtime_identity_handshake(runtime_adapter, request)
        raise

    runtime_identity: ResolvedRuntimeIdentity | None = None
    try:
        runtime_identity = _resolve_managed_runtime_identity(
            config,
            path=path,
            handshake=handshake,
            request=request,
            actor=actor,
            dispatcher_instance_id=dispatcher_instance_id,
            lock_acquired=lock_acquired,
        )
        append_instance_launch_bound(
            config,
            runtime_identity,
            run_id=plan.run_id,
            dispatcher=actor,
            dispatcher_instance_id=dispatcher_instance_id,
            lock_acquired=lock_acquired,
        )
        records = read_event_records(path)
        lease = _run_event(records, kind="dispatch_lease_acquired", run_id=plan.run_id)
        if lease is None:
            _append_and_verify(
                path,
                lambda: emit_lease_acquired(
                    events_path=path,
                    lease_id=lease_id,
                    run_id=plan.run_id,
                    input_message_id=plan.input_message_id,
                    target_agent=plan.target_agent,
                    session_uuid=plan.session_uuid,
                    thread_id=plan.thread_id,
                    ttl_seconds=lease_ttl_seconds,
                    created_utc=stamp,
                    actor=actor,
                    lock_acquired=lock_acquired,
                ),
                verify=verify,
            )
        else:
            payload = lease.get("payload")
            if not isinstance(payload, dict) or not isinstance(payload.get("lease_id"), str):
                raise ValueError("AGENT_INSTANCE_RECOVERY_LEASE_INVALID")
            lease_id = payload["lease_id"]

        records = read_event_records(path)
        if _run_event(records, kind="dispatch_run_started", run_id=plan.run_id) is None:
            _append_and_verify(
                path,
                lambda: emit_run_started(
                    events_path=path,
                    run_id=plan.run_id,
                    thread_id=plan.thread_id,
                    session_uuid=plan.session_uuid,
                    started_utc=stamp,
                    actor=actor,
                    lock_acquired=lock_acquired,
                ),
                verify=verify,
            )
        if release_lock_for_launch is not None:
            release_lock_for_launch()
        try:
            prelaunch_ok = True
            prelaunch_code = ""
            if prelaunch_revalidate is not None:
                try:
                    prelaunch_ok, prelaunch_code = prelaunch_revalidate()
                except Exception:
                    prelaunch_ok, prelaunch_code = False, "UnknownFailure"
            if prelaunch_ok:
                try:
                    result = _launch_runtime_adapter(
                        runtime_adapter,
                        request,
                        launcher=launcher,
                        child_instance_handle=runtime_identity.handle,
                    )
                except KeyboardInterrupt:
                    result = AgentLaunchResult(
                        status="cancelled",
                        exit_code=None,
                        metadata={"phase": "launch"},
                    )
            else:
                result = AgentLaunchResult(
                    status="prelaunch_denied",
                    exit_code=None,
                    metadata={"failure_code": prelaunch_code or "RuntimeConfiguration"},
                )
        finally:
            if reacquire_lock_after_launch is not None:
                try:
                    reacquired = reacquire_lock_after_launch()
                except BaseException as exc:
                    raise DispatchLockReacquireError(
                        "dispatch canonical lock could not be reacquired"
                    ) from exc
                if current_lock_handle is not None:
                    from agent_mesh.core.lock import is_active_lock_handle

                    if reacquired is None or not is_active_lock_handle(
                        reacquired, path.parent / ".mail-lock"
                    ):
                        raise DispatchLockReacquireError(
                            "dispatch canonical lock reacquire returned no active authority"
                        )
                    current_lock_handle = reacquired
    except DispatchLockReacquireError:
        _abort_runtime_identity_handshake(runtime_adapter, request)
        raise
    except Exception:
        # Adapters may retain a bounded handshake transport until managed
        # launch. Always give them a chance to release it; the adapter decides
        # whether a canonically bound provider session must be preserved.
        _abort_runtime_identity_handshake(runtime_adapter, request)
        result = AgentLaunchResult(
            status="launch_error", exit_code=None, metadata={"phase": "launch"}
        )

    if result.status == "observation_pending":
        return result

    intervening_terminal = _reconcile_existing_managed_run(
        config,
        plan=plan,
        events_path=path,
        actor=actor,
        dispatcher_instance_id=dispatcher_instance_id,
        lock_acquired=lock_acquired,
        verify=verify,
        now=stamp,
    )
    if intervening_terminal is not None:
        return intervening_terminal

    try:
        response_candidate_status = None
        response_candidate_reason = None
        output_message_id = None
        if result.status == "completed" and result.exit_code == 0:
            candidate_extractor = getattr(runtime_adapter, "response_candidate", None)
            candidate = (
                candidate_extractor(result)
                if callable(candidate_extractor)
                else extract_response_candidate(result)
            )
            response_candidate_status = "ready" if candidate.status == "accepted" else "rejected"
            response_candidate_reason = candidate.reason
            if post_response and candidate.status == "accepted":
                _ensure_response_candidate_post_allowed(
                    path,
                    plan.input_message_id,
                    allow_superseding=bool(plan.policy_id and plan.attempt_number > 1),
                )
                output_message_id = _append_response_candidate(
                    path,
                    plan=plan,
                    body=candidate.body,
                    summary=candidate.summary,
                    actor=(
                        runtime_identity.participant
                        if runtime_identity is not None
                        else plan.target_agent
                    ),
                    actor_instance_id=(
                        runtime_identity.instance_id if runtime_identity is not None else ""
                    ),
                    lock_acquired=lock_acquired,
                    verify=verify,
                )
                _append_and_verify(
                    path,
                    lambda: emit_run_completed(
                        events_path=path,
                        run_id=plan.run_id,
                        thread_id=plan.thread_id,
                        output_message_id=output_message_id,
                        completed_utc=stamp,
                        actor=actor,
                        lock_acquired=lock_acquired,
                    ),
                    verify=verify,
                )
                release_reason = "completed"
            else:
                error_class = "OutputRejected" if post_response else "OutputNotPosted"
                _append_and_verify(
                    path,
                    lambda: emit_run_failed(
                        events_path=path,
                        run_id=plan.run_id,
                        thread_id=plan.thread_id,
                        error_class=error_class,
                        failed_utc=stamp,
                        actor=actor,
                        lock_acquired=lock_acquired,
                        response_candidate_status=response_candidate_status,
                        response_candidate_reason=response_candidate_reason,
                    ),
                    verify=verify,
                )
                release_reason = "failed"
        elif result.status in {"cancelled", "timeout", "parent_loss"}:
            terminal_state = {
                "cancelled": "cancelled",
                "timeout": "timed_out",
                "parent_loss": "parent_lost",
            }[result.status]
            _append_and_verify(
                path,
                lambda: emit_run_terminated(
                    events_path=path,
                    run_id=plan.run_id,
                    thread_id=plan.thread_id,
                    terminal_state=terminal_state,
                    terminated_utc=stamp,
                    actor=actor,
                    lock_acquired=lock_acquired,
                ),
                verify=verify,
            )
            release_reason = {
                "cancelled": "cancelled",
                "timeout": "timeout",
                "parent_loss": "parent_loss",
            }[result.status]
        else:
            _append_and_verify(
                path,
                lambda: emit_run_failed(
                    events_path=path,
                    run_id=plan.run_id,
                    thread_id=plan.thread_id,
                    error_class=_error_class_for(result),
                    failed_utc=stamp,
                    actor=actor,
                    lock_acquired=lock_acquired,
                ),
                verify=verify,
            )
            release_reason = "failed"

        _release_lease(
            path,
            lease_id=lease_id,
            plan=plan,
            reason=release_reason,
            released_utc=stamp,
            actor=actor,
            lock_acquired=lock_acquired,
            verify=verify,
        )
        if runtime_identity is not None:
            terminal_outcome = _instance_terminal_outcome(result, release_reason=release_reason)
            reconcile_instance_terminal_suffix(
                config,
                runtime_identity,
                run_id=plan.run_id,
                outcome=terminal_outcome,
                dispatcher=actor,
                dispatcher_instance_id=dispatcher_instance_id,
                lock_acquired=lock_acquired,
            )
    except Exception:
        _best_effort_fail_and_release(
            path,
            lease_id=lease_id,
            plan=plan,
            failed_utc=stamp,
            actor=actor,
            lock_acquired=lock_acquired,
            verify=verify,
        )
        if runtime_identity is not None:
            _best_effort_instance_suffix(
                config,
                runtime_identity,
                run_id=plan.run_id,
                dispatcher=actor,
                dispatcher_instance_id=dispatcher_instance_id,
                lock_acquired=lock_acquired,
            )
        raise
    return result


def _release_lease(
    events_path: Path,
    *,
    lease_id: str,
    plan: RunPlan,
    reason: str,
    released_utc: str,
    actor: str,
    lock_acquired: bool,
    verify: VerifyFn,
) -> None:
    _append_and_verify(
        events_path,
        lambda: emit_lease_released(
            events_path=events_path,
            lease_id=lease_id,
            run_id=plan.run_id,
            thread_id=plan.thread_id,
            reason=reason,
            released_utc=released_utc,
            actor=actor,
            lock_acquired=lock_acquired,
        ),
        verify=verify,
    )


def _best_effort_fail_and_release(
    events_path: Path,
    *,
    lease_id: str,
    plan: RunPlan,
    failed_utc: str,
    actor: str,
    lock_acquired: bool,
    verify: VerifyFn,
) -> None:
    try:
        _append_and_verify(
            events_path,
            lambda: emit_run_failed(
                events_path=events_path,
                run_id=plan.run_id,
                thread_id=plan.thread_id,
                error_class="LifecycleError",
                failed_utc=failed_utc,
                actor=actor,
                lock_acquired=lock_acquired,
            ),
            verify=verify,
        )
    except Exception:
        pass
    try:
        _release_lease(
            events_path,
            lease_id=lease_id,
            plan=plan,
            reason="failed",
            released_utc=failed_utc,
            actor=actor,
            lock_acquired=lock_acquired,
            verify=verify,
        )
    except Exception:
        pass


def _ensure_response_candidate_post_allowed(
    events_path: Path,
    request_id: str,
    *,
    allow_superseding: bool = False,
) -> None:
    request_payload: dict | None = None
    request_thread_id: str | None = None
    existing_direct_response: str | None = None
    for record in read_event_records(events_path):
        payload = record.get("payload", {})
        if record.get("kind") == "req_created" and record.get("entity_id") == request_id:
            request_payload = payload if isinstance(payload, dict) else {}
            request_thread_id = str(record.get("thread_id") or "")
            continue
        if record.get("kind") != "res_posted" or not isinstance(payload, dict):
            continue
        if str(payload.get("request_id") or record.get("thread_id")) != request_id:
            continue
        parent_id = str(payload.get("parent_id") or request_id)
        parent_kind = str(payload.get("parent_kind") or "request")
        if parent_id == request_id and parent_kind == "request":
            existing_direct_response = str(record.get("entity_id"))
            break
    if request_payload is None:
        raise ValueError(f"request {request_id} does not exist")
    if request_thread_id != request_id:
        raise ValueError(
            f"request {request_id} thread_id must equal request id for dispatch response posting"
        )
    response_mode = str(request_payload.get("response_mode") or "single")
    if response_mode == "multi":
        return
    if response_mode != "single":
        raise ValueError(f"invalid response_mode for request {request_id}: {response_mode}")
    if existing_direct_response and not allow_superseding:
        raise ValueError(f"request {request_id} already has response {existing_direct_response}")


def _append_response_candidate(
    events_path: Path,
    *,
    plan: RunPlan,
    body: str,
    summary: str,
    actor: str,
    actor_instance_id: str,
    lock_acquired: bool,
    verify: VerifyFn,
) -> str:
    response_id = new_public_message_id("RES", actor)
    payload = {
        "from": actor,
        "request_id": plan.input_message_id,
        "parent_id": plan.input_message_id,
        "parent_kind": "request",
        "summary": summary,
        "body": body,
        "response_id": response_id,
        "refs": [],
        "body_authority": "agent_summary",
        "body_fidelity": "full",
        "source_context_refs": [
            {
                "channel": "agent-mesh-dispatch",
                "source_event_id": plan.run_id,
                "role": "authoritative_body",
                "confidence": 1.0,
            }
        ],
    }

    def append_response() -> None:
        token = bind_agent_instance(actor_instance_id)
        try:
            append_event(
                events_path,
                Event(
                    event_id=generate_event_id(),
                    actor=actor,
                    actor_instance_id=actor_instance_id,
                    kind="res_posted",
                    entity_id=response_id,
                    thread_id=plan.input_message_id,
                    payload=payload,
                ),
                lock_acquired=lock_acquired,
            )
        finally:
            reset_agent_instance(token)

    _append_and_verify(events_path, append_response, verify=verify)
    return response_id


def _append_and_verify(events_path: Path, append_fn, *, verify: VerifyFn) -> None:
    anchor = capture_anchor(events_path)
    append_fn()
    check = verify(events_path, anchor)
    if not check.ok:
        detail = check.error or "unknown chain verification failure"
        raise RuntimeError(f"dispatch append chain verification failed: {detail}")


def _error_class_for(result: AgentLaunchResult) -> str:
    if result.status == "prelaunch_denied":
        code = str(result.metadata.get("failure_code") or "RuntimeConfiguration")
        if code in {
            "ParentDenial",
            "RuntimeConfiguration",
            "AuthenticationDrift",
            "ProviderRejection",
            "OutputPolicyRejection",
            "ParentLoss",
            "UnknownFailure",
        }:
            return code
        return "RuntimeConfiguration"
    if result.status == "timeout":
        return "TimeoutError"
    if result.status == "launch_error":
        return "LaunchError"
    if result.status == "failed":
        return "AgentFailed"
    return "AgentError"


def _resolve_managed_runtime_identity(
    config,
    *,
    path: Path,
    handshake: RuntimeIdentityHandshake,
    request: AgentRunRequest,
    actor: str,
    dispatcher_instance_id: str,
    lock_acquired: bool,
) -> ResolvedRuntimeIdentity:
    if not lock_acquired:
        raise ValueError("AGENT_INSTANCE_HANDSHAKE_REQUIRES_CANONICAL_LOCK")
    parent = resolve_agent_instance_from_records(read_event_records(path), dispatcher_instance_id)
    if not dispatcher_instance_id:
        if actor not in config.decision_approval_identities:
            raise ValueError("AGENT_INSTANCE_HUMAN_DISPATCH_AUTHORITY_REQUIRED")
        return resolve_or_register_runtime_identity(
            config,
            handshake,
            registrar=actor,
            registration_origin="dispatch",
            parent_instance_id="",
            originating_run_id=request.run_id,
            lock_acquired=True,
        )
    if parent is None or parent.status != "active":
        raise ValueError("AGENT_INSTANCE_DISPATCHER_BINDING_REQUIRED")
    if parent.participant != actor:
        raise ValueError("AGENT_INSTANCE_DISPATCHER_ACTOR_MISMATCH")
    return resolve_or_register_runtime_identity(
        config,
        handshake,
        registrar=parent.participant,
        registration_origin="dispatch",
        parent_instance_id=parent.id,
        originating_run_id=request.run_id,
        lock_acquired=True,
    )


def _runtime_identity_handshake(
    runtime_adapter: AgentRuntimeAdapter, request: AgentRunRequest
) -> RuntimeIdentityHandshake | None:
    provider = getattr(runtime_adapter, "identity_handshake", None)
    return provider(request) if callable(provider) else None


def _prepare_runtime_identity_handshake(
    runtime_adapter: AgentRuntimeAdapter, request: AgentRunRequest
) -> str:
    prepare = getattr(runtime_adapter, "prepare_identity_handshake", None)
    if not callable(prepare):
        return ""
    digest = prepare(request)
    if not isinstance(digest, str):
        raise ValueError("AGENT_INSTANCE_PREPARATION_INVALID")
    return digest


def _run_event(records: list[dict], *, kind: str, run_id: str) -> dict | None:
    matches = [
        record
        for record in records
        if record.get("kind") == kind
        and (
            record.get("entity_id") == run_id
            or (
                isinstance(record.get("payload"), dict)
                and record["payload"].get("run_id") == run_id
            )
        )
    ]
    if len(matches) > 1:
        raise ValueError("AGENT_INSTANCE_RECOVERY_EVENT_DUPLICATE")
    return matches[0] if matches else None


def _terminal_event_for_run(records: list[dict], run_id: str) -> dict | None:
    matches = [
        record
        for record in records
        if record.get("entity_id") == run_id
        and record.get("kind")
        in {"dispatch_run_completed", "dispatch_run_failed", "dispatch_run_terminated"}
    ]
    if len(matches) > 1:
        raise ValueError("DISPATCH_RUN_TERMINAL_DUPLICATE")
    return matches[0] if matches else None


def _lease_released_for_run(records: list[dict], run_id: str) -> bool:
    return any(
        record.get("kind") == "dispatch_lease_released"
        and isinstance(record.get("payload"), dict)
        and record["payload"].get("run_id") == run_id
        for record in records
    )


def _identity_for_run(records: list[dict], run_id: str) -> ResolvedRuntimeIdentity | None:
    launch_bound = next(
        (
            record
            for record in records
            if record.get("kind") == "agent_instance_launch_bound"
            and isinstance(record.get("payload"), dict)
            and record["payload"].get("run_id") == run_id
        ),
        None,
    )
    if launch_bound is None:
        return None
    child = reduce_agent_instances(records).get(str(launch_bound.get("entity_id") or ""))
    if child is None:
        raise ValueError("AGENT_INSTANCE_RECOVERY_CHILD_UNKNOWN")
    return ResolvedRuntimeIdentity(
        instance_id=child.id,
        handle=child.label,
        participant=child.participant,
        binding_source=child.last_binding_source,
        action="recovered",
        resumable=child.resumable,
        one_shot=not child.resumable,
    )


def _parse_dispatch_utc(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("DISPATCH_LIFECYCLE_TIME_INVALID") from exc
    if parsed.tzinfo is None:
        raise ValueError("DISPATCH_LIFECYCLE_TIME_INVALID")
    return parsed.astimezone(UTC)


def _validate_recovery_plan(record: dict, plan: RunPlan) -> None:
    payload = record.get("payload")
    expected = planned_payload(plan, run_mode="live", status="planned")
    if (
        record.get("entity_id") != plan.run_id
        or record.get("thread_id") != plan.thread_id
        or not isinstance(payload, dict)
        or any(payload.get(key) != value for key, value in expected.items())
    ):
        raise ValueError("AGENT_INSTANCE_RECOVERY_PLAN_MISMATCH")


def _ensure_no_other_unfinished_run(records: list[dict], plan: RunPlan) -> None:
    terminal_run_ids = {
        str(record.get("entity_id", ""))
        for record in records
        if record.get("kind")
        in {"dispatch_run_completed", "dispatch_run_failed", "dispatch_run_terminated"}
    }
    for record in records:
        payload = record.get("payload")
        if (
            record.get("kind") == "dispatch_run_planned"
            and record.get("entity_id") != plan.run_id
            and isinstance(payload, dict)
            and payload.get("run_mode") == "live"
            and payload.get("status") == "planned"
            and payload.get("target_agent") == plan.target_agent
            and record.get("entity_id") not in terminal_run_ids
        ):
            raise ValueError("AGENT_INSTANCE_PRELAUNCH_RECOVERY_PENDING")


def _launch_runtime_adapter(
    runtime_adapter: AgentRuntimeAdapter,
    request: AgentRunRequest,
    *,
    launcher: AgentLauncher,
    child_instance_handle: str,
) -> AgentLaunchResult:
    def launch_without_workbench_authority(spec: AgentLaunchSpec) -> AgentLaunchResult:
        from agent_mesh.workbench_service import WORKBENCH_INVOCATION_ENV

        environment = dict(spec.environment or {})
        environment.pop(WORKBENCH_INVOCATION_ENV, None)
        return launcher.launch(replace(spec, environment=environment))

    provider_launch = getattr(runtime_adapter, "launch", None)
    if callable(provider_launch):
        return provider_launch(
            request,
            launch_process=launch_without_workbench_authority,
            child_instance_handle=child_instance_handle,
        )
    launch_spec = replace(
        runtime_adapter.build_launch(request),
        child_instance_handle=child_instance_handle,
    )
    return launch_without_workbench_authority(launch_spec)


def _runtime_identity_provider_configured(
    runtime_adapter: AgentRuntimeAdapter,
) -> bool:
    provider = getattr(type(runtime_adapter), "identity_handshake", None)
    return callable(provider) and provider is not AgentRuntimeAdapter.identity_handshake


def _abort_runtime_identity_handshake(
    runtime_adapter: AgentRuntimeAdapter, request: AgentRunRequest
) -> None:
    abort = getattr(runtime_adapter, "abort_identity_handshake", None)
    if not callable(abort):
        return
    try:
        abort(request)
    except Exception:
        pass


def _reconcile_existing_managed_run(
    config,
    *,
    plan: RunPlan,
    events_path: Path,
    actor: str,
    dispatcher_instance_id: str,
    lock_acquired: bool,
    verify: VerifyFn,
    now: str,
) -> AgentLaunchResult | None:
    """Finish an unambiguous crash suffix without relaunching the child process."""

    records = read_event_records(events_path)
    if not any(
        record.get("kind") == "dispatch_run_planned" and record.get("entity_id") == plan.run_id
        for record in records
    ):
        return None
    launch_bound = next(
        (
            record
            for record in records
            if record.get("kind") == "agent_instance_launch_bound"
            and isinstance(record.get("payload"), dict)
            and record["payload"].get("run_id") == plan.run_id
        ),
        None,
    )
    if launch_bound is None:
        return None
    child_id = str(launch_bound.get("entity_id", ""))
    child = reduce_agent_instances(records).get(child_id)
    if child is None:
        raise ValueError("AGENT_INSTANCE_RECOVERY_CHILD_UNKNOWN")
    identity = ResolvedRuntimeIdentity(
        instance_id=child.id,
        handle=child.label,
        participant=child.participant,
        binding_source=child.last_binding_source,
        action="recovered",
        resumable=child.resumable,
        one_shot=not child.resumable,
    )
    response = _response_for_dispatch_run(records, plan.run_id)
    terminal = next(
        (
            record
            for record in records
            if record.get("entity_id") == plan.run_id
            and record.get("kind")
            in {"dispatch_run_completed", "dispatch_run_failed", "dispatch_run_terminated"}
        ),
        None,
    )
    if terminal is None:
        if response is None:
            return None
        _append_and_verify(
            events_path,
            lambda: emit_run_completed(
                events_path=events_path,
                run_id=plan.run_id,
                thread_id=plan.thread_id,
                output_message_id=str(response["entity_id"]),
                completed_utc=now,
                actor=actor,
                lock_acquired=lock_acquired,
            ),
            verify=verify,
        )
        outcome = "completed"
        release_reason = "completed"
    elif terminal["kind"] == "dispatch_run_completed":
        outcome = "completed"
        release_reason = "completed"
    elif terminal["kind"] == "dispatch_run_failed":
        error_class = str(terminal.get("payload", {}).get("error_class", ""))
        outcome = {
            "TimeoutError": "timeout",
            "LaunchError": "launch_error",
        }.get(error_class, "failed")
        release_reason = "failed"
    else:
        terminal_state = str(terminal.get("payload", {}).get("terminal_state", ""))
        outcome = {
            "cancelled": "cancelled",
            "timed_out": "timeout",
            "parent_lost": "parent_loss",
        }.get(terminal_state, "failed")
        release_reason = {
            "cancelled": "cancelled",
            "timed_out": "timeout",
            "parent_lost": "parent_loss",
        }.get(terminal_state, "failed")

    lease = next(
        (
            record
            for record in records
            if record.get("kind") == "dispatch_lease_acquired"
            and isinstance(record.get("payload"), dict)
            and record["payload"].get("run_id") == plan.run_id
        ),
        None,
    )
    if lease is None:
        raise ValueError("AGENT_INSTANCE_RECOVERY_LEASE_UNKNOWN")
    lease_released = any(
        record.get("kind") == "dispatch_lease_released"
        and isinstance(record.get("payload"), dict)
        and record["payload"].get("run_id") == plan.run_id
        for record in records
    )
    if not lease_released:
        _release_lease(
            events_path,
            lease_id=str(lease["payload"]["lease_id"]),
            plan=plan,
            reason=release_reason,
            released_utc=now,
            actor=actor,
            lock_acquired=lock_acquired,
            verify=verify,
        )
    reconcile_instance_terminal_suffix(
        config,
        identity,
        run_id=plan.run_id,
        outcome=outcome,
        dispatcher=actor,
        dispatcher_instance_id=dispatcher_instance_id,
        lock_acquired=lock_acquired,
    )
    return AgentLaunchResult(
        status=outcome,
        exit_code=0 if outcome == "completed" else None,
        metadata={"phase": "recovered", "run_id": plan.run_id},
    )


def _response_for_dispatch_run(records: list[dict], run_id: str) -> dict | None:
    for record in records:
        if record.get("kind") != "res_posted":
            continue
        payload = record.get("payload", {})
        refs = payload.get("source_context_refs", []) if isinstance(payload, dict) else []
        if any(
            isinstance(ref, dict)
            and ref.get("channel") == "agent-mesh-dispatch"
            and ref.get("source_event_id") == run_id
            for ref in refs
        ):
            return record
    return None


def _dispatcher_instance_id(events_path: Path, *, actor: str) -> str:
    selected = selected_agent_instance()
    if not selected:
        return ""
    parent = resolve_agent_instance_from_records(read_event_records(events_path), selected)
    if parent is None or parent.status != "active" or parent.participant != actor:
        raise ValueError("AGENT_INSTANCE_DISPATCHER_BINDING_REQUIRED")
    return parent.id


def _instance_terminal_outcome(result: AgentLaunchResult, *, release_reason: str) -> str:
    if release_reason == "completed":
        return "completed"
    if result.status in {"failed", "timeout", "launch_error", "cancelled", "parent_loss"}:
        return result.status
    return "failed"


def _best_effort_instance_suffix(
    config,
    identity: ResolvedRuntimeIdentity,
    *,
    run_id: str,
    dispatcher: str,
    dispatcher_instance_id: str,
    lock_acquired: bool,
) -> None:
    try:
        reconcile_instance_terminal_suffix(
            config,
            identity,
            run_id=run_id,
            outcome="failed",
            dispatcher=dispatcher,
            dispatcher_instance_id=dispatcher_instance_id,
            lock_acquired=lock_acquired,
        )
    except Exception:
        pass
