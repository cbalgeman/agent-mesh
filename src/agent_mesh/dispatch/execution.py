"""Launch-only execution seam for dispatch lifecycle events.

This module bridges a planned dispatch run to a local agent runtime without posting a RES. It owns the
minimal live lifecycle around a process launch: acquire lease, mark started, launch, classify the
result, emit a failure-class terminal event until RES/output posting policy exists, and release the
lease. Raw prompts and process output remain outside
canonical dispatch events; events carry only ids/category codes and token counters.
"""
from __future__ import annotations

from pathlib import Path
from typing import Callable, Protocol

from agent_mesh.core.chain import ChainAnchor, ChainResult
from agent_mesh.core.events import Event, append_event, generate_event_id
from agent_mesh.store.rebuild import read_event_records
from agent_mesh.core.ids import new_ulid

from .adapters import AgentRuntimeAdapter
from .dispatch import now_utc
from .emitter import (
    capture_anchor,
    emit_lease_acquired,
    emit_lease_released,
    emit_run_completed,
    emit_run_failed,
    emit_run_planned,
    emit_run_started,
    verify_appended,
)
from .output_policy import extract_response_candidate
from .types import AgentLaunchResult, AgentLaunchSpec, AgentRunRequest, RunPlan


class AgentLauncher(Protocol):
    """Minimal process launcher protocol used by the execution seam."""

    def launch(self, spec: AgentLaunchSpec) -> AgentLaunchResult:
        raise NotImplementedError


VerifyFn = Callable[[Path, ChainAnchor], ChainResult]


def execute_launch_plan(
    plan: RunPlan,
    *,
    events_path: str | Path,
    runtime_adapter: AgentRuntimeAdapter,
    launcher: AgentLauncher,
    project_root: str | Path,
    prompt: str,
    timeout_seconds: int = 3600,
    ttl_seconds: int = 3600,
    run_mode: str = "live",
    now: str | None = None,
    actor: str = "dispatcher",
    verify: VerifyFn = verify_appended,
    lock_acquired: bool = False,
    post_response: bool = False,
) -> AgentLaunchResult:
    """Execute one planned launch and emit canonical lifecycle events.

    ``prompt`` is handed only to the runtime adapter/launcher as in-memory stdin text. Dispatch
    events record no prompt, stdout, stderr, command, reply template, or raw error text. When
    ``post_response`` is true, only an accepted explicitly-fenced response candidate is appended via
    the normal ``res_posted`` event shape before ``dispatch_run_completed`` is emitted.
    """

    if run_mode != "live":
        raise ValueError("execute_launch_plan currently supports only run_mode='live'")
    path = Path(events_path)
    stamp = now or now_utc()
    lease_id = new_ulid("lease")
    if post_response:
        if not lock_acquired:
            raise ValueError("post_response requires caller-held mail lock")
        if plan.thread_id != plan.input_message_id:
            raise ValueError("post_response plan thread_id must equal input_message_id")
        _ensure_response_candidate_post_allowed(path, plan.input_message_id)

    _append_and_verify(
        path,
        lambda: emit_run_planned(
            plan,
            events_path=path,
            run_mode=run_mode,
            status="planned",
            actor=actor,
            lock_acquired=lock_acquired,
        ),
        verify=verify,
    )
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
            ttl_seconds=ttl_seconds,
            created_utc=stamp,
            actor=actor,
            lock_acquired=lock_acquired,
        ),
        verify=verify,
    )
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

    request = AgentRunRequest(
        run_id=plan.run_id,
        target_agent=plan.target_agent,
        session_uuid=plan.session_uuid,
        project_root=Path(project_root),
        prompt=prompt,
        timeout_seconds=timeout_seconds,
    )
    try:
        launch_spec = runtime_adapter.build_launch(request)
        result = launcher.launch(launch_spec)
    except Exception:
        result = AgentLaunchResult(status="launch_error", exit_code=None, metadata={"phase": "launch"})

    try:
        response_candidate_status = None
        response_candidate_reason = None
        output_message_id = None
        if result.status == "completed" and result.exit_code == 0:
            candidate = extract_response_candidate(result)
            response_candidate_status = "ready" if candidate.status == "accepted" else "rejected"
            response_candidate_reason = candidate.reason
            if post_response and candidate.status == "accepted":
                _ensure_response_candidate_post_allowed(path, plan.input_message_id)
                output_message_id = _append_response_candidate(
                    path,
                    plan=plan,
                    body=candidate.body,
                    summary=candidate.summary,
                    actor=plan.target_agent,
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


def _ensure_response_candidate_post_allowed(events_path: Path, request_id: str) -> None:
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
        raise ValueError(f"request {request_id} thread_id must equal request id for dispatch response posting")
    response_mode = str(request_payload.get("response_mode") or "single")
    if response_mode == "multi":
        return
    if response_mode != "single":
        raise ValueError(f"invalid response_mode for request {request_id}: {response_mode}")
    if existing_direct_response:
        raise ValueError(f"request {request_id} already has response {existing_direct_response}")


def _append_response_candidate(
    events_path: Path,
    *,
    plan: RunPlan,
    body: str,
    summary: str,
    actor: str,
    lock_acquired: bool,
    verify: VerifyFn,
) -> str:
    response_id = new_ulid("res")
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
        append_event(
            events_path,
            Event(
                event_id=generate_event_id(),
                actor=actor,
                kind="res_posted",
                entity_id=response_id,
                thread_id=plan.input_message_id,
                payload=payload,
            ),
            lock_acquired=lock_acquired,
        )

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
    if result.status == "timeout":
        return "TimeoutError"
    if result.status == "launch_error":
        return "LaunchError"
    if result.status == "failed":
        return "AgentFailed"
    return "AgentError"
