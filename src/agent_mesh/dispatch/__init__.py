"""agent_mesh.dispatch -- OPTIONAL reusable orchestration layer (not core substrate).

This subpackage hosts the host-agnostic engine for autonomous dispatch: a transactional risky-apply
guard, a deterministic eval runner + gate, message/plan/session-key dispatch logic, and a cold-start
grounding assembler. It is OPT-IN: ``agent-mesh`` remains a usable pure message substrate with
``dispatch`` unconfigured and unimported.

Boundary rules:
  * core substrate (``agent_mesh.core`` / ``store`` / ``views``) MUST NOT import ``agent_mesh.dispatch``.
  * ``agent_mesh.dispatch`` MUST NOT import any host's domain code (a backlog, project paths, etc.).
    All host coupling goes through the adapter ABCs below, injected by the host.

The contracts live in ``adapters``/``types``; the host-agnostic engine lives in ``guard`` (the
transactional risky-apply guard), ``eval`` (the deterministic eval runner + gate), and ``dispatch``
(the pure-core dispatch planning + run-record builder). Grounding assembly lands in a later slice.
"""
from __future__ import annotations

from .adapters import (
    AgentRuntimeAdapter,
    DispatchHost,
    GroundingSources,
    ProjectStateProvider,
    RunRecorder,
    StoreAdapter,
)
from .atomic import JOURNAL_NAME, promote_unit, recover_unit
from .dispatch import (
    build_lease,
    build_run_record,
    find_pending,
    is_leased,
    lease_key,
    plan_for,
    record_run,
    resolve_session_key,
    responders_by_thread,
    session_uuid,
    to_message,
)
from .emitter import (
    blocked_payload,
    capture_anchor,
    emit_lease_acquired,
    emit_lease_released,
    emit_run_blocked,
    emit_run_completed,
    emit_run_failed,
    emit_run_planned,
    emit_run_started,
    planned_payload,
    record_plan,
    verify_appended,
)
from .eval import GATE_HALTS, case_set_digest, eval_fingerprint, gate, run_evals
from .execution import AgentLauncher, execute_launch_plan
from .grounding import (
    GroundingResult,
    Section,
    SubstrateIncomplete,
    assemble_grounding,
    build_prompt,
    prior_decisions_text,
)
from .guard import GUARD_VERSION, artifact_hash, guarded_apply
from .output_policy import ResponseCandidateDecision, extract_response_candidate
from .runtime import DEFAULT_CODEX_BINARY, AgentProcessLauncher, CodexCliRuntimeAdapter
from .types import AgentLaunchResult, AgentLaunchSpec, AgentRunRequest, EvalCase, EvalSuite, Message, RunPlan, RunRecord

__all__ = [
    # contracts
    "StoreAdapter",
    "AgentRuntimeAdapter",
    "DispatchHost",
    "RunRecorder",
    "GroundingSources",
    "ProjectStateProvider",
    "Message",
    "RunPlan",
    "RunRecord",
    "AgentRunRequest",
    "AgentLaunchSpec",
    "AgentLaunchResult",
    "EvalCase",
    "EvalSuite",
    # guard engine
    "guarded_apply",
    "artifact_hash",
    "GUARD_VERSION",
    "promote_unit",
    "recover_unit",
    "JOURNAL_NAME",
    # eval engine
    "run_evals",
    "gate",
    "eval_fingerprint",
    "case_set_digest",
    "GATE_HALTS",
    # execution seam
    "execute_launch_plan",
    "AgentLauncher",
    # dispatch engine
    "to_message",
    "responders_by_thread",
    "resolve_session_key",
    "session_uuid",
    "plan_for",
    "find_pending",
    "lease_key",
    "is_leased",
    "build_lease",
    "build_run_record",
    "record_run",
    # canonical event emitter (live-gate B)
    "record_plan",
    "planned_payload",
    "blocked_payload",
    "emit_run_planned",
    "emit_run_blocked",
    "emit_lease_acquired",
    "emit_lease_released",
    "emit_run_started",
    "emit_run_completed",
    "emit_run_failed",
    "capture_anchor",
    "verify_appended",
    # output policy
    "extract_response_candidate",
    "ResponseCandidateDecision",
    # runtime adapters
    "CodexCliRuntimeAdapter",
    "AgentProcessLauncher",
    "DEFAULT_CODEX_BINARY",
    # grounding engine
    "assemble_grounding",
    "build_prompt",
    "prior_decisions_text",
    "GroundingResult",
    "Section",
    "SubstrateIncomplete",
]
