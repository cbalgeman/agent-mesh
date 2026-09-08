"""agent-q CLI — read side."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import time
import uuid
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from agent_mesh import __version__
from agent_mesh.config import AgentMeshConfig, ConfigError, load_config
from agent_mesh.core.agent_instances import (
    INSTANCE_ID_RE,
    AgentInstanceError,
    bind_agent_instance,
    resolve_authoring_actor,
    reset_agent_instance,
    selected_agent_instance,
)
from agent_mesh.core.decision_schema import (
    DecisionBodyIntegrityError,
    VERIFICATION_EXECUTABLE_STATUSES,
    VERIFICATION_EXECUTION_MODE_ARGV,
    VERIFICATION_EXECUTION_MODE_LEGACY_ARGV,
    decision_completeness_issues,
    decision_lineage_summary,
    decision_review_progress,
    normalize_decision_review_policy,
    read_verified_decision_body,
)
from agent_mesh.core.decision_projection import decision_revision_sha_from_projection
from agent_mesh.core.decision_applicability import (
    DecisionPathError,
    evaluate_decision_applicability,
    normalize_candidate_paths,
)
from agent_mesh.core.decision_context import (
    DECISION_CONTEXT_SCHEMA,
    MAX_DECISION_CONTEXT_JSON_BYTES as DEFAULT_MAX_DECISION_CONTEXT_JSON_BYTES,
    build_decision_context,
    render_bounded_decision_context_json,
)
from agent_mesh.core.decision_digest import DecisionDigestOverflow, render_decision_digest
from agent_mesh.core.events import Event, EventProtocolError, append_event, generate_event_id
from agent_mesh.core.ids import new_public_message_id, new_ulid
from agent_mesh.core.git_changes import GitChangeUnavailable
from agent_mesh.core.assurance import (
    AssuranceResolutionError,
    ReviewResponseError,
    append_assurance_lifecycle_transition,
    assurance_lifecycle_for_response,
    parse_review_response,
    resolve_artifact_subject,
    resolve_current_change_set_subject,
    resolve_decision_subject,
    resolve_typed_artifact_ref,
    validate_typed_artifact_refs,
)
from agent_mesh.core.dispatch_schema import assurance_policy_revision
from agent_mesh.core.workflow_origin import WORKFLOW_ORIGINS
from agent_mesh.core.external_recovery_plan import (
    ExternalRecoveryPlanError,
    build_external_recovery_plan_report,
)
from agent_mesh.core.chain import verify_chain
from agent_mesh.core.context_delivery import (
    MAX_CONTEXT_BOOTSTRAP_SECONDS,
    MAX_CONTEXT_SNAPSHOT_BYTES,
    MAX_CONTEXT_SNAPSHOT_EVENTS,
    ContextDeliveryRequestError,
    build_context_bootstrap,
    build_unavailable_context_bootstrap,
    load_builtin_lifecycle_mapping,
    load_delivery_report,
    load_lifecycle_mapping,
    load_prior_cursor,
    render_bounded_context_bootstrap_json,
)
from agent_mesh.core.lock import LockHandle, acquire
from agent_mesh.core.recovery import RecoveryStopLine, recover
from agent_mesh.core.reference_context import (
    BoundedReferenceText,
    MAX_REFERENCE_CONTEXT_OCCURRENCES,
    MAX_REFERENCE_CONTEXT_TEXT_BYTES,
    MAX_REFERENCE_SNAPSHOT_BYTES,
    MAX_REFERENCE_SNAPSHOT_EVENTS,
    MAX_REFERENCE_SNAPSHOT_SECONDS,
    build_reference_context,
    build_unavailable_reference_context,
    render_bounded_reference_context_json,
)
from agent_mesh.core.reference_syntax import (
    ReferenceOccurrence,
    explicit_reference_occurrences,
    extract_reference_occurrences,
)
from agent_mesh.message_packet import (
    build_message_packet,
    public_instance_handle_for_id,
    public_message_recipients,
    public_message_sender,
)
from agent_mesh.core.source_recovery import (
    SourceRecoveryError,
    SourceSpec,
    build_recovery_ledger,
    load_requested_ids,
)
from agent_mesh.core.source_recovery_audit import (
    SourceRecoveryAuditError,
    build_source_recovery_audit_manifest,
)
from agent_mesh.core.source_recovery_promotion import (
    SourceRecoveryPromotionError,
    build_source_recovery_promotion_plan,
)
from agent_mesh.store.rebuild import (
    BacklogStopLine,
    AgentInstanceStopLine,
    DecisionStopLine,
    DispatchStopLine,
    diagnose_decision_replay,
    read_event_records,
    rebuild_all,
)
from agent_mesh.store.sqlite import (
    body_from_message_row,
    connect,
    initialize_schema,
    json_loads,
    resolve_agent_instance,
    resolve_decision,
    resolve_dispatch_run,
    resolve_message,
)
from agent_mesh.store.read_model import (
    ReadModelUnavailable,
    capture_event_snapshot,
    open_read_model,
)
from agent_mesh.views import locate_message, render_all
from agent_mesh.adapters.base import AdapterSpec
from agent_mesh.dispatch import extract_response_candidate, plan_for, to_message
from agent_mesh.dispatch.adapters import AgentRuntimeAdapter, DispatchHost
from agent_mesh.dispatch.execution import (
    DispatchLockReacquireError,
    cancel_managed_run,
    execute_launch_plan,
    recover_interrupted_managed_run,
    reconcile_expired_policy_attempt,
)
from agent_mesh.dispatch.emitter import emit_retry_exhausted
from agent_mesh.core.assurance import (
    evaluate_review_assurance,
    evaluate_transition_assurance,
)
from agent_mesh.dispatch.policy import (
    DispatchPolicySnapshot,
    append_dispatch_policy,
    build_dispatch_policy,
    current_dispatch_outcome,
)
from agent_mesh.dispatch.review_response import review_response_instructions
from agent_mesh.dispatch.profiles import (
    RuntimePreflightResult,
    preflight_runtime_profile,
    runtime_profile_revision,
)
from agent_mesh.dispatch.runtime import AgentProcessLauncher, CodexAppServerError
from agent_mesh.dispatch.runtime_registry import (
    RuntimeAdapterRegistry,
    RuntimeDriverContext,
    built_in_runtime_registry,
    runtime_registry_for_profile,
)
from agent_mesh.dispatch.types import Message

MAX_DECISION_CONTEXT_JSON_BYTES = DEFAULT_MAX_DECISION_CONTEXT_JSON_BYTES
MAX_DECISION_CONTEXT_TEXT_BYTES = 16 * 1024
MAX_DECISION_HOOK_REQUEST_BYTES = 256 * 1024
MAX_REFERENCE_INPUT_BYTES = 2 * 1024 * 1024
MAX_REFERENCE_FILE_BYTES = 1024 * 1024
MAX_REFERENCE_FILES = 1_000
MAX_REFERENCE_PATH_BYTES = 4_096
MAX_DECISION_QUERY_SOURCE_BYTES = 64 * 1024 * 1024
MAX_DECISION_QUERY_EVENTS = 50_000
MAX_DECISION_QUERY_SECONDS = 30.0
MAX_DECISION_LIST_RESULTS = 500
MAX_DECISION_BODY_OUTPUT_BYTES = 512 * 1024
DECISION_HOOK_REQUEST_SCHEMA = "agent-mesh.decision-hook-request.v1"
DECISION_HOOK_BOUNDARIES = frozenset({"edit", "write"})


class DecisionHookRequestError(ValueError):
    """Raised when the provider-neutral hook request is malformed."""


def _decision_list_limit(value: str) -> int:
    try:
        limit = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must be an integer") from exc
    if not 1 <= limit <= MAX_DECISION_LIST_RESULTS:
        raise argparse.ArgumentTypeError(
            f"must be between 1 and {MAX_DECISION_LIST_RESULTS}"
        )
    return limit


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    explicit_instance = getattr(args, "instance", None) or ""
    if INSTANCE_ID_RE.fullmatch(selected_agent_instance(explicit_instance)):
        print(
            "agent-q: raw AI instance IDs are diagnostic-only; use the instance handle",
            file=sys.stderr,
        )
        return 2
    instance_token = bind_agent_instance(getattr(args, "instance", None))
    try:
        return int(args.func(args))
    except (
        AgentInstanceError,
        AssuranceResolutionError,
        ConfigError,
        EventProtocolError,
    ) as exc:
        print(f"agent-q: {exc}", file=sys.stderr)
        return 2
    except DecisionHookRequestError as exc:
        print(f"agent-q decisions hook: invalid request: {exc}", file=sys.stderr)
        return 2
    except GitChangeUnavailable as exc:
        print(f"agent-q: Git change subject unavailable: {exc}", file=sys.stderr)
        return 2
    except ContextDeliveryRequestError as exc:
        print(f"agent-q context bootstrap: invalid request: {exc}", file=sys.stderr)
        return 2
    except DecisionPathError as exc:
        print(f"agent-q: invalid decision path: {exc}", file=sys.stderr)
        return 2
    except ReadModelUnavailable as exc:
        decision_context_json = getattr(args, "func", None) is cmd_decisions_hook or (
            getattr(args, "func", None) is cmd_decisions_preflight and getattr(args, "json", False)
        )
        if getattr(args, "func", None) is cmd_context_bootstrap:
            result = build_unavailable_context_bootstrap(str(exc))
            rendered, _ = render_bounded_context_bootstrap_json(
                result,
                pretty=getattr(args, "pretty", False),
            )
            print(rendered)
        elif decision_context_json:
            paths = list(
                getattr(
                    args,
                    "_decision_context_paths",
                    getattr(args, "path", []),
                )
            )
            print(
                json.dumps(
                    {
                        "schema": DECISION_CONTEXT_SCHEMA,
                        "privacy_class": "project_private",
                        "context_status": "unavailable",
                        "complete": False,
                        "repository": None,
                        "request": {
                            "boundary": getattr(args, "_decision_context_boundary", "task"),
                            "path_count": len(paths),
                            "paths": [],
                        },
                        "decisions": [],
                        "diagnostics": [str(exc)[:1000]],
                    },
                    sort_keys=True,
                )
            )
        else:
            print(f"agent-q: read model unavailable: {exc}", file=sys.stderr)
        return 3
    except (AgentInstanceStopLine, BacklogStopLine, DecisionStopLine, DispatchStopLine) as exc:
        print(f"agent-q: {exc.code}: {exc.detail}", file=sys.stderr)
        return 1
    except RecoveryStopLine as exc:
        print(f"agent-q: {exc.code}", file=sys.stderr)
        return 1
    except (
        SourceRecoveryError,
        SourceRecoveryAuditError,
        SourceRecoveryPromotionError,
        ExternalRecoveryPlanError,
    ) as exc:
        command = getattr(locals().get("args", None), "command", "recover-sources")
        print(f"agent-q {command}: {exc}", file=sys.stderr)
        return 1
    finally:
        reset_agent_instance(instance_token)


def _rebuild_all_locked(config):
    lock_handle = acquire(config.agent_dir / ".mail-lock")
    try:
        return rebuild_all(config)
    finally:
        lock_handle.release()


def _render_all_locked(config):
    lock_handle = acquire(config.agent_dir / ".mail-lock")
    try:
        rebuild_all(config)
        return render_all(config)
    finally:
        lock_handle.release()


def _authoring_actor(config, *, explicit_actor: str | None = None) -> str:
    return resolve_authoring_actor(
        read_event_records(config.events_path),
        default_actor=config.default_sender,
        explicit_actor=explicit_actor,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="agent-q")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument(
        "--instance",
        help="authoring AI instance handle for read commands that append outcomes",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    list_cmd = sub.add_parser("list")
    list_cmd.add_argument("--status")
    list_cmd.add_argument("--to")
    list_cmd.add_argument("--to-instance")
    list_cmd.add_argument("--origin", choices=WORKFLOW_ORIGINS)
    list_cmd.add_argument("--json", action="store_true")
    list_cmd.set_defaults(func=cmd_list)

    locate = sub.add_parser("locate")
    locate.add_argument("message_id")
    locate.set_defaults(func=cmd_locate)

    body = sub.add_parser("body")
    body.add_argument("--id", required=True, dest="message_id")
    body.set_defaults(func=cmd_body)

    packet = sub.add_parser("packet")
    packet.add_argument("--id", required=True, dest="message_id")
    packet.add_argument("--max-body-chars", type=int, default=20_000)
    packet.add_argument("--max-thread-body-chars", type=int, default=2_000)
    packet.add_argument("--max-thread-messages", type=int, default=12)
    packet.add_argument("--warn-tokens", type=int, default=12_000)
    packet.set_defaults(func=cmd_packet)

    thread = sub.add_parser("thread")
    thread.add_argument("message_id")
    thread.set_defaults(func=cmd_thread)

    trace = sub.add_parser("trace")
    trace.add_argument("message_id")
    trace.add_argument("--show-source", action="store_true")
    trace.set_defaults(func=cmd_trace)

    render = sub.add_parser("render")
    render.add_argument("--all", action="store_true")
    render.set_defaults(func=cmd_render)

    rebuild = sub.add_parser("rebuild")
    rebuild.add_argument("--all", action="store_true")
    rebuild.set_defaults(func=cmd_rebuild)

    recover_cmd = sub.add_parser("recover")
    recover_cmd.add_argument("--force-unlock", action="store_true")
    recover_cmd.add_argument("--resolve-event")
    recover_cmd.add_argument("--resolve-decision")
    recover_cmd.add_argument("--resolve-dispatch")
    recover_cmd.set_defaults(func=cmd_recover)

    recover_sources = sub.add_parser("recover-sources")
    recover_sources.add_argument("--ids-file", required=True, type=Path)
    recover_sources.add_argument("--source", required=True, action="append", type=Path)
    recover_sources.add_argument(
        "--source-kind",
        action="append",
        choices=("claude_code_history", "codex_history"),
        help=(
            "source-channel kind override. One value applies to all sources; "
            "one per --source pairs by order; omit to infer per path"
        ),
    )
    recover_sources.add_argument("--output", type=Path)
    recover_sources.add_argument("--pretty", action="store_true")
    recover_sources.set_defaults(func=cmd_recover_sources)

    audit_recovered = sub.add_parser("audit-recovered-sources")
    audit_recovered.add_argument("--ledger", required=True, type=Path)
    audit_recovered.add_argument("--output", required=True, type=Path)
    audit_recovered.add_argument("--pretty", action="store_true")
    audit_recovered.add_argument("--max-alternatives", type=int, default=3)
    audit_recovered.set_defaults(func=cmd_audit_recovered_sources)

    plan_promotions = sub.add_parser("plan-source-promotions")
    plan_promotions.add_argument("--promotion-review", required=True, type=Path)
    plan_promotions.add_argument("--output", required=True, type=Path)
    plan_promotions.add_argument("--pretty", action="store_true")
    plan_promotions.set_defaults(func=cmd_plan_source_promotions)

    external_recovery = sub.add_parser("report-external-recovery-plan")
    external_recovery.add_argument("--plan", required=True, type=Path)
    external_recovery.add_argument("--output", required=True, type=Path)
    external_recovery.add_argument("--pretty", action="store_true")
    external_recovery.set_defaults(func=cmd_report_external_recovery_plan)

    verify = sub.add_parser("verify-chain")
    verify.add_argument("events_path", nargs="?")
    verify.set_defaults(func=cmd_verify_chain)

    status = sub.add_parser("status")
    status.add_argument("--writes", action="store_true")
    status.set_defaults(func=cmd_status)

    events = sub.add_parser("events")
    events.add_argument("--kind")
    events.add_argument("--thread")
    events.add_argument("--actor-instance")
    events.add_argument("--json", action="store_true")
    events.set_defaults(func=cmd_events)

    backlog = sub.add_parser("backlog")
    backlog_sub = backlog.add_subparsers(dest="backlog_command", required=True)
    backlog_list = backlog_sub.add_parser("list")
    backlog_list.add_argument("--status")
    backlog_list.add_argument("--lane")
    backlog_list.add_argument("--owner-instance")
    backlog_list.add_argument("--origin", choices=WORKFLOW_ORIGINS)
    backlog_list.set_defaults(func=cmd_backlog_list)
    backlog_get = backlog_sub.add_parser("get")
    backlog_get.add_argument("item_id")
    backlog_get.set_defaults(func=cmd_backlog_get)
    backlog_events = backlog_sub.add_parser("events")
    backlog_events.add_argument("item_id")
    backlog_events.set_defaults(func=cmd_backlog_events)

    context = sub.add_parser("context")
    context_sub = context.add_subparsers(dest="context_command", required=True)
    context_bootstrap = context_sub.add_parser(
        "bootstrap",
        description=("Emit a bounded mutation-free canonical bootstrap and freshness cursor."),
    )
    context_bootstrap.add_argument("--prior-cursor", type=Path)
    delivery_source = context_bootstrap.add_mutually_exclusive_group()
    delivery_source.add_argument(
        "--mapping",
        type=Path,
        help="optional static lifecycle mapping; schema conformance is not installed support",
    )
    delivery_source.add_argument(
        "--report",
        type=Path,
        help="optional closed harness self-report; self-report cannot claim verified state",
    )
    delivery_source.add_argument(
        "--builtin-mapping",
        choices=("claude", "codex-hermes", "generic-local"),
        help="wheel-available schema-only lifecycle mapping",
    )
    context_bootstrap.add_argument("--pretty", action="store_true")
    context_bootstrap.set_defaults(func=cmd_context_bootstrap)

    refs = sub.add_parser("refs")
    refs_sub = refs.add_subparsers(dest="refs_command", required=True)
    refs_resolve = refs_sub.add_parser(
        "resolve",
        description=(
            "Resolve explicit or extracted references against one verified canonical snapshot."
        ),
    )
    refs_resolve.add_argument("references", nargs="*")
    refs_resolve.add_argument("--stdin", action="store_true", dest="read_stdin")
    refs_resolve.add_argument("--file", action="append", type=Path, default=[])
    refs_resolve.add_argument("--json", action="store_true")
    refs_resolve.add_argument(
        "--diagnostics",
        action="store_true",
        help="include internal storage identifiers for troubleshooting",
    )
    refs_resolve.set_defaults(func=cmd_refs_resolve)

    instances = sub.add_parser("instances")
    instance_sub = instances.add_subparsers(dest="instances_command", required=True)
    instance_list = instance_sub.add_parser("list")
    instance_list.add_argument("--participant")
    instance_list.add_argument("--status", choices=("active", "terminal", "retired"))
    instance_list.add_argument("--json", action="store_true")
    instance_list.add_argument("--diagnostic", action="store_true")
    instance_list.set_defaults(func=cmd_instances_list)
    instance_show = instance_sub.add_parser("show")
    instance_show.add_argument("identifier")
    instance_show.add_argument("--diagnostic", action="store_true")
    instance_show.set_defaults(func=cmd_instances_show)

    decisions = sub.add_parser("decisions")
    decision_sub = decisions.add_subparsers(dest="decision_command", required=True)
    dec_list = decision_sub.add_parser("list")
    dec_list.add_argument("--status")
    tier_filter = dec_list.add_mutually_exclusive_group()
    tier_filter.add_argument("--tier")
    tier_filter.add_argument(
        "--invalid-tier",
        action="store_true",
        help="show preserved historical decisions outside the canonical tier vocabulary",
    )
    dec_list.add_argument("--scope")
    dec_list.add_argument("--owner")
    dec_list.add_argument(
        "--incomplete",
        action="store_true",
        help="show only decisions that are incomplete under the current contract",
    )
    dec_list.add_argument(
        "--limit",
        type=_decision_list_limit,
        default=MAX_DECISION_LIST_RESULTS,
        help=f"maximum rows to print (1-{MAX_DECISION_LIST_RESULTS})",
    )
    dec_list.set_defaults(func=cmd_decisions_list)

    show = decision_sub.add_parser("show")
    show.add_argument("identifier")
    show.add_argument("--references", action="store_true")
    show.add_argument("--evidence", action="store_true")
    show.add_argument("--assumptions", action="store_true")
    show.add_argument(
        "--body",
        action="store_true",
        help="include the integrity-checked canonical Markdown body",
    )
    show.add_argument(
        "--diagnostics",
        action="store_true",
        help="include internal storage identifiers for troubleshooting",
    )
    show.set_defaults(func=cmd_decisions_show)

    log = decision_sub.add_parser("log")
    log.add_argument("identifier")
    log.set_defaults(func=cmd_decisions_log)

    search = decision_sub.add_parser("search")
    search.add_argument("query")
    search.set_defaults(func=cmd_decisions_search)

    diagnose = decision_sub.add_parser(
        "diagnose",
        description=(
            "Replay the canonical log in memory and report the first decision stop-line. "
            "This command never edits or quarantines canonical events."
        ),
    )
    diagnose.set_defaults(func=cmd_decisions_diagnose)

    at = decision_sub.add_parser("at")
    at.add_argument("path")
    at.set_defaults(func=cmd_decisions_at)

    preflight = decision_sub.add_parser(
        "preflight",
        description="Return applicable current decisions without mutating projection state.",
    )
    preflight.add_argument("--path", action="append", default=[])
    preflight_output = preflight.add_mutually_exclusive_group()
    preflight_output.add_argument("--json", action="store_true")
    preflight_output.add_argument(
        "--digest",
        action="store_true",
        help="render compact path-scoped guidance while retaining --json for machines",
    )
    preflight.add_argument("--include-proposed", action="store_true")
    preflight.add_argument(
        "--diagnostics",
        action="store_true",
        help="include internal storage identifiers for troubleshooting",
    )
    preflight.set_defaults(func=cmd_decisions_preflight)

    hook = decision_sub.add_parser(
        "hook",
        description=(
            "Read one provider-neutral edit/write request from stdin and return "
            "applicable decisions as bounded JSON without mutating projection state."
        ),
    )
    hook.set_defaults(func=cmd_decisions_hook)

    verify_dec = decision_sub.add_parser(
        "verify",
        description=(
            "Execute human-approved verification definitions as explicit argv. "
            "Shell interpolation, pipes, redirection, expansion, and command chaining are disabled."
        ),
    )
    verify_dec.add_argument("identifier")
    verify_dec.set_defaults(func=cmd_decisions_verify)

    dispatches = sub.add_parser("dispatches")
    dispatch_sub = dispatches.add_subparsers(dest="dispatch_command", required=True)
    disp_list = dispatch_sub.add_parser("list")
    disp_list.add_argument("--status")
    disp_list.add_argument("--agent")
    disp_list.add_argument("--json", action="store_true")
    disp_list.set_defaults(func=cmd_dispatches_list)
    disp_show = dispatch_sub.add_parser("show")
    disp_show.add_argument("identifier")
    disp_show.set_defaults(func=cmd_dispatches_show)
    disp_log = dispatch_sub.add_parser("log")
    disp_log.add_argument("identifier")
    disp_log.set_defaults(func=cmd_dispatches_log)
    disp_status = dispatch_sub.add_parser("status")
    disp_status.set_defaults(func=cmd_dispatches_status)
    disp_verify = dispatch_sub.add_parser("verify")
    disp_verify.set_defaults(func=cmd_dispatches_verify)
    disp_gate = dispatch_sub.add_parser(
        "gate",
        description=(
            "Evaluate the current response-slot and review.v1 assurance gate for one "
            "frozen dispatch policy without writing canonical state."
        ),
    )
    disp_gate.add_argument("--policy", default="")
    disp_gate.add_argument(
        "--boundary-kind",
        choices=("decision", "path", "backlog_transition", "release"),
    )
    disp_gate.add_argument("--boundary-key")
    disp_gate.add_argument("--json", action="store_true")
    disp_gate.add_argument("--diagnostics", action="store_true")
    disp_gate.set_defaults(func=cmd_dispatches_gate)
    disp_preflight = dispatch_sub.add_parser("preflight")
    disp_preflight.add_argument("--target", required=True)
    disp_preflight.add_argument("--profile")
    disp_preflight.set_defaults(func=cmd_dispatches_preflight)
    disp_run = dispatch_sub.add_parser(
        "run",
        description=(
            "Create or select one REQ, freeze dispatch.v1, prove runtime capabilities, "
            "launch a managed attempt, and post its bounded RES outcome."
        ),
    )
    disp_run.add_argument("--live", action="store_true")
    disp_run.add_argument("--target", required=True)
    disp_run.add_argument("--profile")
    disp_run.add_argument("--actor")
    request_source = disp_run.add_mutually_exclusive_group(required=True)
    request_source.add_argument("--message")
    request_source.add_argument("--title")
    request_source.add_argument(
        "--policy", help="retry the next attempt under an existing frozen dispatch.v1 policy"
    )
    disp_run.add_argument("--body")
    disp_run.add_argument(
        "--workstream",
        help=(
            "stable human-facing workstream for a new REQ; reuse the same value "
            "to resume supported provider context"
        ),
    )
    disp_run.add_argument(
        "--continue-response",
        help=(
            "public RES identifier from a completed managed dispatch; create a linked "
            "follow-up REQ and reuse its managed context"
        ),
    )
    disp_run.add_argument("--purpose", default="review")
    disp_run.add_argument("--role", default="reviewer")
    disp_run.add_argument("--max-attempts", type=int, default=1)
    subject = disp_run.add_mutually_exclusive_group()
    subject.add_argument("--subject-decision")
    subject.add_argument("--subject-artifact")
    subject.add_argument("--subject-change-mode", choices=("pr", "staged", "worktree", "full"))
    disp_run.add_argument("--subject-change-base")
    disp_run.add_argument(
        "--coverage-kind",
        choices=("decision", "path", "backlog_transition", "release"),
    )
    disp_run.add_argument("--coverage-key")
    disp_run.add_argument("--timeout-seconds", type=int, default=3600)
    disp_run.set_defaults(func=cmd_dispatches_run)
    disp_cancel = dispatch_sub.add_parser(
        "cancel",
        description=(
            "Terminalize one active managed run as cancelled. A late process result is discarded."
        ),
    )
    disp_cancel.add_argument("--run", required=True)
    disp_cancel.add_argument("--actor")
    disp_cancel.set_defaults(func=cmd_dispatches_cancel)
    disp_assure = dispatch_sub.add_parser(
        "assure",
        description=(
            "Derive bounded review.v1 evidence from the closed envelope in the current "
            "dispatcher-bound RES. No verdict fields are accepted from the operator."
        ),
    )
    disp_assure.add_argument("--policy", required=True)
    disp_assure.add_argument("--actor")
    disp_assure.set_defaults(func=cmd_dispatches_assure)
    disp_assurance = dispatch_sub.add_parser(
        "assurance",
        description="Show current review reliance state using a public RES reference.",
    )
    disp_assurance.add_argument("--response", required=True)
    disp_assurance.add_argument("--json", action="store_true")
    disp_assurance.add_argument("--diagnostics", action="store_true")
    disp_assurance.set_defaults(func=cmd_dispatches_assurance)
    disp_flag = dispatch_sub.add_parser(
        "flag-assurance",
        description=(
            "Exclude a materially contested review assurance from gate satisfaction. "
            "The immutable RES remains in history."
        ),
    )
    disp_flag.add_argument("--response", required=True)
    disp_flag.add_argument("--expected-version", required=True, type=int)
    disp_flag.add_argument("--reason-code", required=True)
    disp_flag.add_argument("--note", required=True)
    disp_flag.add_argument("--actor")
    disp_flag.set_defaults(func=cmd_dispatches_flag_assurance)
    disp_retire = dispatch_sub.add_parser(
        "retire-assurance",
        description=(
            "Permanently retire one flagged assurance. The actor must belong to the "
            "configured direct-human authority."
        ),
    )
    disp_retire.add_argument("--response", required=True)
    disp_retire.add_argument("--expected-version", required=True, type=int)
    disp_retire.add_argument("--reason-code", required=True)
    disp_retire.add_argument("--note", required=True)
    disp_retire.add_argument("--actor", required=True)
    disp_retire.set_defaults(func=cmd_dispatches_retire_assurance)
    disp_once = dispatch_sub.add_parser("once")
    disp_once.add_argument("--live", action="store_true")
    disp_once.add_argument("--target", required=True)
    disp_once.add_argument("--profile")
    disp_once.add_argument("--message", required=True)
    disp_once.add_argument("--timeout-seconds", type=int, default=3600)
    disp_once.add_argument("--post-response", action="store_true")
    disp_once.set_defaults(func=cmd_dispatches_once)
    disp_worker = dispatch_sub.add_parser("worker")
    disp_worker.add_argument("--live", action="store_true")
    disp_worker.add_argument("--target", required=True)
    disp_worker.add_argument("--profile")
    disp_worker.add_argument("--max-runs", type=int, default=1)
    disp_worker.add_argument("--timeout-seconds", type=int, default=3600)
    disp_worker.add_argument("--message", action="append", default=[])
    disp_worker.add_argument("--after-event-seq", type=int, default=None)
    disp_worker.add_argument("--post-response", action="store_true")
    disp_worker.set_defaults(func=cmd_dispatches_worker)

    drivers = sub.add_parser("drivers", help="inspect configured runtime drivers")
    driver_sub = drivers.add_subparsers(dest="driver_command", required=True)
    driver_check = driver_sub.add_parser(
        "check",
        description=(
            "Run the configured driver's no-model-prompt preflight. Agent Mesh appends no "
            "events, but a project-local driver is unsandboxed project code."
        ),
    )
    driver_check.add_argument("--target", required=True)
    driver_check.add_argument("--profile")
    driver_check.set_defaults(func=cmd_drivers_check)
    return parser


def cmd_list(args: argparse.Namespace) -> int:
    config = load_config()
    with open_read_model(config) as snapshot:
        conn = snapshot.conn
        sql = "SELECT * FROM messages WHERE kind='request'"
        params: list[str] = []
        if args.status:
            sql += " AND status=?"
            params.append(args.status)
        if args.to:
            sql += " AND recipients_json LIKE ?"
            params.append(f"%{args.to}%")
        if args.to_instance:
            if INSTANCE_ID_RE.fullmatch(args.to_instance):
                raise ConfigError(
                    "raw AI instance IDs are diagnostic-only; use the instance handle"
                )
            instance = resolve_agent_instance(conn, args.to_instance)
            if instance is None:
                raise ConfigError(f"AGENT_INSTANCE_UNKNOWN: {args.to_instance}")
            sql += " AND recipient_instance_ids_json LIKE ?"
            params.append(f'%"{instance["id"]}"%')
        if args.origin:
            sql += " AND workflow_origin=?"
            params.append(args.origin)
        sql += " ORDER BY created_utc DESC, event_seq DESC"
        rows = conn.execute(sql, params).fetchall()
        if args.json:
            rendered = []
            for row in rows:
                item = {
                    "id": row["id"],
                    "kind": row["kind"],
                    "thread_id": row["thread_id"],
                    "request_id": row["request_id"],
                    "parent_id": row["parent_id"],
                    "sender": public_message_sender(
                        conn,
                        str(row["sender"]),
                        str(row["sender_instance_id"] or ""),
                    ),
                    "recipients": public_message_recipients(
                        conn,
                        json_loads(row["recipients_json"], []),
                        json_loads(row["recipient_instance_ids_json"], []),
                    ),
                    "feature": row["feature_id"],
                    "workflow_origin": row["workflow_origin"],
                    "title": row["title"],
                    "summary": row["summary"],
                    "body_preview": row["body_preview"],
                    "status": row["status"],
                    "resolution": row["resolution"],
                    "created_utc": row["created_utc"],
                    "updated_utc": row["updated_utc"],
                    "event_seq": row["event_seq"],
                }
                rendered.append(item)
            print(json.dumps(rendered, indent=2))
        else:
            for row in rows:
                sender = public_message_sender(
                    conn,
                    str(row["sender"]),
                    str(row["sender_instance_id"] or ""),
                )
                print(
                    f"{row['id']}\t{row['status']}\t{sender}\t"
                    f"{row['title']}\t"
                    f"{row['workflow_origin'] or ''}"
                )
    return 0


def cmd_locate(args: argparse.Namespace) -> int:
    config = load_config()
    with open_read_model(config) as snapshot:
        row = resolve_message(snapshot.conn, args.message_id)
        if row is None or row["kind"] not in {"request", "response"}:
            print(f"agent-q locate: not found: {args.message_id}", file=sys.stderr)
            return 1
        found = locate_message(config, args.message_id)
        if not found:
            for line_number, record in enumerate(snapshot.records, start=1):
                if (
                    record.get("kind") in {"req_created", "res_posted"}
                    and record.get("entity_id") == args.message_id
                ):
                    found = (config.events_path, line_number, line_number)
                    break
    if not found:
        print(f"agent-q locate: not found: {args.message_id}", file=sys.stderr)
        return 1
    path, start, end = found
    print(f"{path}:{start}-{end}")
    return 0


def cmd_body(args: argparse.Namespace) -> int:
    config = load_config()
    with open_read_model(config) as snapshot:
        conn = snapshot.conn
        row = resolve_message(conn, args.message_id)
        if row is None:
            print(f"agent-q body: not found: {args.message_id}", file=sys.stderr)
            return 1
        print(
            body_from_message_row(row),
            end="" if body_from_message_row(row).endswith("\n") else "\n",
        )
    return 0


def cmd_packet(args: argparse.Namespace) -> int:
    config = load_config()
    with open_read_model(config) as snapshot:
        conn = snapshot.conn
        row = resolve_message(conn, args.message_id)
        if row is None:
            print(f"agent-q packet: not found: {args.message_id}", file=sys.stderr)
            return 1
        packet = build_message_packet(
            conn,
            row,
            max_body_chars=args.max_body_chars,
            max_thread_body_chars=args.max_thread_body_chars,
            max_thread_messages=args.max_thread_messages,
            warn_tokens=args.warn_tokens,
        )
    if packet["size"]["est_tokens"] > args.warn_tokens:
        print(
            f"agent-q packet: warning: estimated packet size {packet['size']['est_tokens']} "
            f"tokens exceeds --warn-tokens={args.warn_tokens}",
            file=sys.stderr,
        )
    print(json.dumps(packet, indent=2, sort_keys=True))
    return 0


def cmd_thread(args: argparse.Namespace) -> int:
    config = load_config()
    with open_read_model(config) as snapshot:
        conn = snapshot.conn
        root = resolve_message(conn, args.message_id)
        if root is None:
            print(f"agent-q thread: not found: {args.message_id}", file=sys.stderr)
            return 1
        thread_id = str(root["thread_id"])
        rows = conn.execute(
            "SELECT * FROM messages WHERE thread_id=? ORDER BY created_utc ASC, event_seq ASC",
            (thread_id,),
        ).fetchall()
        for row in rows:
            parent = row["parent_id"] or "-"
            label = row["title"] if row["kind"] == "request" else row["summary"]
            print(
                f"{row['id']}\t{row['kind']}\tparent={parent}\tfrom={row['sender']}\t{label or ''}"
            )
    return 0


def cmd_trace(args: argparse.Namespace) -> int:
    config = load_config()
    with open_read_model(config) as snapshot:
        conn = snapshot.conn
        row = resolve_message(conn, args.message_id)
        if row is None:
            print(f"agent-q trace: not found: {args.message_id}", file=sys.stderr)
            return 1
        parent = row["parent_id"] or "-"
        request = row["request_id"] or "-"
        label = row["title"] if row["kind"] == "request" else row["summary"]
        print(f"{row['id']}\t{row['kind']}\tthread={row['thread_id']}\tparent={parent}")
        print(f"request_id: {request}")
        print(f"sender: {row['sender']}")
        print(f"created_utc: {row['created_utc']}")
        if label:
            print(f"label: {label}")
        if args.show_source:
            _print_source_trace(conn, row["id"])
    return 0


def _print_source_trace(conn, message_id: str) -> None:
    row = conn.execute("SELECT * FROM messages WHERE id=?", (message_id,)).fetchone()
    print(f"body_authority: {row['body_authority'] or 'unknown'}")
    print(f"body_fidelity: {row['body_fidelity'] or '(unspecified)'}")

    selection = conn.execute(
        "SELECT * FROM message_source_selection WHERE message_id=?", (message_id,)
    ).fetchone()
    if selection is None:
        print("source_selection: none")
    else:
        requires_review = "true" if selection["requires_review"] else "false"
        print(
            "source_selection: "
            f"mode={selection['mode']} confidence={selection['confidence']:.3g} "
            f"selected_by={selection['selected_by']} requires_review={requires_review}"
        )
        if selection["requires_review"]:
            print("warning: source_selection requires_review=true")

    refs = conn.execute(
        "SELECT * FROM message_source_context_refs WHERE message_id=? ORDER BY ref_index",
        (message_id,),
    ).fetchall()
    if not refs:
        print("source_context_refs: none")
    else:
        print("source_context_refs:")
        for ref in refs:
            print(
                f"- [{ref['ref_index']}] channel={ref['channel'] or '-'} "
                f"source_id={ref['source_id'] or '-'} "
                f"source_event_id={ref['source_event_id'] or '-'} "
                f"source_kind={ref['source_kind'] or '-'} role={ref['role'] or '-'} "
                f"confidence={_format_optional_confidence(ref['confidence'])} "
                f"uri={ref['source_uri'] or '-'}"
            )

    edges = conn.execute(
        "SELECT * FROM message_causal_edges WHERE message_id=? ORDER BY edge_index",
        (message_id,),
    ).fetchall()
    if not edges:
        print("causal_edges: none")
    else:
        print("causal_edges:")
        for edge in edges:
            print(
                f"- [{edge['edge_index']}] {edge['relation']}: "
                f"{edge['from_ref'] or '-'} -> {edge['to_ref'] or '-'} "
                f"confidence={_format_optional_confidence(edge['confidence'])}"
            )


def _format_optional_confidence(value) -> str:
    if value is None:
        return "-"
    return f"{float(value):.3g}"


def cmd_render(args: argparse.Namespace) -> int:
    config = load_config()
    rendered = _render_all_locked(config)
    for item in rendered:
        print(f"rendered {item.target}")
    _ = args
    return 0


def cmd_rebuild(args: argparse.Namespace) -> int:
    config = load_config()
    lock = acquire(config.agent_dir / ".mail-lock")
    try:
        result = rebuild_all(config)
        render_all(config)
    finally:
        lock.release()
    print(f"rebuilt {result.event_count} events")
    _ = args
    return 0


def cmd_recover(args: argparse.Namespace) -> int:
    config = load_config()
    if args.resolve_dispatch:
        from agent_mesh.workbench_service import (
            WorkbenchServiceError,
            workbench_dispatch_recovery,
        )

        actor = _authoring_actor(config)
        try:
            with workbench_dispatch_recovery(config, args.resolve_dispatch) as recovery_lease:
                lock = acquire(config.agent_dir / ".mail-lock")
                try:
                    changed = recover_interrupted_managed_run(
                        config,
                        run_id=args.resolve_dispatch,
                        actor=actor,
                        lock_acquired=True,
                    )
                    rebuild_all(config)
                    render_all(config)
                    recovery_lease.complete()
                finally:
                    lock.release()
        except (ValueError, WorkbenchServiceError) as exc:
            print(f"agent-q recover: {exc}", file=sys.stderr)
            return 2
        print(
            f"dispatch recovery: run={args.resolve_dispatch} "
            f"canonical_changed={str(changed).lower()} marker_retired=true"
        )
        return 0
    lock = acquire(config.agent_dir / ".mail-lock")
    try:
        report = recover(config.events_path, config.agent_dir)
        rebuild_all(config)
        render_all(config)
    finally:
        lock.release()
    print(
        "recover: "
        f"auto_recovered={report.auto_recovered} "
        f"discarded_intents={report.discarded_intents} "
        f"replayed_committed={report.replayed_committed}"
    )
    _ = args
    return 0


def cmd_recover_sources(args: argparse.Namespace) -> int:
    ids = load_requested_ids(args.ids_file)
    if not ids:
        print(
            f"agent-q recover-sources: no REQ-/RES- ids found in {args.ids_file}",
            file=sys.stderr,
        )
        return 1
    source_kinds = args.source_kind or []
    if len(source_kinds) > 1 and len(source_kinds) != len(args.source):
        print(
            "agent-q recover-sources: use either one --source-kind for all sources "
            "or one --source-kind per --source",
            file=sys.stderr,
        )
        return 1
    sources = [
        SourceSpec(
            path=path,
            source_kind=(
                source_kinds[index]
                if len(source_kinds) == len(args.source)
                else (source_kinds[0] if source_kinds else None)
            ),
        )
        for index, path in enumerate(args.source)
    ]
    ledger = build_recovery_ledger(ids, sources)
    indent = 2 if args.pretty else None
    payload = json.dumps(ledger, indent=indent, sort_keys=True)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload + "\n", encoding="utf-8")
    else:
        print(payload)
    return 0


def cmd_audit_recovered_sources(args: argparse.Namespace) -> int:
    manifest = build_source_recovery_audit_manifest(
        args.ledger,
        max_alternatives=args.max_alternatives,
    )
    indent = 2 if args.pretty else None
    payload = json.dumps(manifest, indent=indent, sort_keys=True)
    try:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload + "\n", encoding="utf-8")
    except OSError as exc:
        raise SourceRecoveryAuditError(
            f"could not write output {args.output}: {exc.strerror or exc}"
        ) from exc
    return 0


def cmd_plan_source_promotions(args: argparse.Namespace) -> int:
    manifest = build_source_recovery_promotion_plan(args.promotion_review)
    indent = 2 if args.pretty else None
    payload = json.dumps(manifest, indent=indent, sort_keys=True)
    try:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload + "\n", encoding="utf-8")
    except OSError as exc:
        raise SourceRecoveryPromotionError(
            f"could not write output {args.output}: {exc.strerror or exc}"
        ) from exc
    return 0


def cmd_report_external_recovery_plan(args: argparse.Namespace) -> int:
    report = build_external_recovery_plan_report(args.plan)
    indent = 2 if args.pretty else None
    payload = json.dumps(report, indent=indent, sort_keys=True)
    try:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload + "\n", encoding="utf-8")
    except OSError as exc:
        raise ExternalRecoveryPlanError(
            f"could not write output {args.output}: {exc.strerror or exc}"
        ) from exc
    return 0


def cmd_verify_chain(args: argparse.Namespace) -> int:
    events_path = Path(args.events_path) if args.events_path else Path(".agent-mesh/events.jsonl")
    result = verify_chain(events_path)
    if not result.ok:
        print(f"agent-q verify-chain: {result.error}", file=sys.stderr)
        return 1
    print(f"verify-chain: OK ({result.verified} events)")
    return 0


def cmd_context_bootstrap(args: argparse.Namespace) -> int:
    config = load_config()
    prior_cursor = load_prior_cursor(args.prior_cursor) if args.prior_cursor else None
    context_delivery = None
    if args.mapping:
        context_delivery = load_lifecycle_mapping(args.mapping)
    elif args.report:
        context_delivery = load_delivery_report(args.report)
    elif args.builtin_mapping:
        context_delivery = load_builtin_lifecycle_mapping(args.builtin_mapping)
    deadline = time.monotonic() + MAX_CONTEXT_BOOTSTRAP_SECONDS
    snapshot = capture_event_snapshot(
        config,
        max_bytes=MAX_CONTEXT_SNAPSHOT_BYTES,
        max_events=MAX_CONTEXT_SNAPSHOT_EVENTS,
        deadline_monotonic=deadline,
    )
    result = build_context_bootstrap(
        config,
        snapshot,
        deadline_monotonic=deadline,
        prior_cursor=prior_cursor,
        context_delivery=context_delivery,
    )
    rendered, complete = render_bounded_context_bootstrap_json(
        result,
        pretty=args.pretty,
    )
    print(rendered)
    return 0 if complete else 3


def cmd_status(args: argparse.Namespace) -> int:
    config = load_config()
    with open_read_model(config) as snapshot:
        conn = snapshot.conn
        last_seq = snapshot.event_seq
        open_count = conn.execute(
            "SELECT COUNT(*) FROM messages WHERE kind='request' AND status='open'"
        ).fetchone()[0]
    print(f"project: {config.project_root}")
    print(f"last_event_seq: {last_seq}")
    print(f"open_requests: {open_count}")
    if args.writes:
        print(f"lock_present: {(config.agent_dir / '.mail-lock').exists()}")
    return 0


def cmd_instances_list(args: argparse.Namespace) -> int:
    config = load_config()
    with open_read_model(config) as snapshot:
        conn = snapshot.conn
        sql = "SELECT * FROM agent_instances WHERE 1=1"
        params: list[str] = []
        if args.participant:
            sql += " AND participant=?"
            params.append(args.participant)
        if args.status:
            sql += " AND status=?"
            params.append(args.status)
        sql += " ORDER BY created_event_seq, id"
        rows = conn.execute(sql, params).fetchall()
        if args.json:
            if args.diagnostic:
                payload = [{key: row[key] for key in row.keys()} for row in rows]
            else:
                payload = [_public_instance_row(row) for row in rows]
            print(json.dumps(payload, indent=2))
        else:
            for row in rows:
                if args.diagnostic:
                    print(
                        f"{row['id']}\t{row['label']}\t{row['status']}\t"
                        f"{row['participant']}\t{row['provider']}\t{row['workstream'] or ''}"
                    )
                else:
                    print(
                        f"{row['label']}\t{row['status']}\t{row['provider']}\t"
                        f"resumable={str(bool(row['resumable'])).lower()}\t"
                        f"binding={row['last_binding_source'] or ''}"
                    )
    return 0


def _public_instance_row(row) -> dict[str, object]:
    return {
        "handle": row["label"],
        "status": row["status"],
        "provider": row["provider"],
        "workstream": row["workstream"] or "",
        "runtime_profile": row["runtime_profile"] or "",
        "resumable": bool(row["resumable"]),
        "adapter_trust": row["adapter_trust"],
        "binding_source": row["last_binding_source"] or "",
        "terminal_outcome": row["terminal_outcome"] or "",
    }


def cmd_instances_show(args: argparse.Namespace) -> int:
    config = load_config()
    if INSTANCE_ID_RE.fullmatch(args.identifier) and not args.diagnostic:
        print(
            "agent-q instances show: raw AI instance IDs require --diagnostic",
            file=sys.stderr,
        )
        return 2
    with open_read_model(config) as snapshot:
        conn = snapshot.conn
        row = resolve_agent_instance(conn, args.identifier)
        if row is None:
            print(f"agent-q instances show: not found: {args.identifier}", file=sys.stderr)
            return 1
        aliases = conn.execute(
            "SELECT label, is_primary FROM agent_instance_aliases WHERE instance_id=? "
            "ORDER BY is_primary DESC, label",
            (row["id"],),
        ).fetchall()
        if args.diagnostic:
            print(f"id: {row['id']}")
        print(f"handle: {row['label']}")
        print(f"status: {row['status']}")
        print(f"provider: {row['provider']}")
        print(f"workstream: {row['workstream'] or ''}")
        print(f"runtime_profile: {row['runtime_profile'] or ''}")
        print(f"resumable: {str(bool(row['resumable'])).lower()}")
        print(f"adapter_trust: {row['adapter_trust']}")
        print(f"binding_source: {row['last_binding_source'] or ''}")
        print(f"created_utc: {row['created_utc']}")
        print(f"last_seen_utc: {row['last_seen_utc'] or ''}")
        print(f"retired_utc: {row['retired_utc'] or ''}")
        if args.diagnostic:
            print(f"participant: {row['participant']}")
            print(f"external_session_ref_digest: {row['external_session_ref_digest'] or ''}")
            print("aliases: " + ", ".join(str(alias["label"]) for alias in aliases))
    return 0


def cmd_events(args: argparse.Namespace) -> int:
    config = load_config()
    actor_instance_id = ""
    with open_read_model(config) as snapshot:
        if args.actor_instance:
            conn = snapshot.conn
            instance = resolve_agent_instance(conn, args.actor_instance)
            if instance is None:
                print(
                    f"agent-q events: instance not found: {args.actor_instance}",
                    file=sys.stderr,
                )
                return 1
            actor_instance_id = str(instance["id"])
        rows = []
        for record in snapshot.records:
            if args.kind and record.get("kind") != args.kind:
                continue
            if args.thread and record.get("thread_id") != args.thread:
                continue
            if actor_instance_id and record.get("actor_instance_id") != actor_instance_id:
                continue
            rows.append(record)
    if args.json:
        print(json.dumps(rows, indent=2, sort_keys=True))
    else:
        for record in rows:
            print(
                f"{record['event_seq']}\t{record['kind']}\t{record['entity_id']}\t"
                f"actor={record['actor']}\t"
                f"instance={record.get('actor_instance_id', '')}\tthread={record['thread_id']}"
            )
    return 0


def cmd_backlog_list(args: argparse.Namespace) -> int:
    config = load_config()
    with open_read_model(config) as snapshot:
        conn = snapshot.conn
        sql = "SELECT * FROM backlog_items WHERE 1=1"
        params: list[str] = []
        if args.status:
            sql += " AND status=?"
            params.append(args.status)
        if args.lane:
            sql += " AND lane=?"
            params.append(args.lane)
        if args.owner_instance:
            if INSTANCE_ID_RE.fullmatch(args.owner_instance):
                raise ConfigError(
                    "raw AI instance IDs are diagnostic-only; use the instance handle"
                )
            instance = resolve_agent_instance(conn, args.owner_instance)
            if instance is None:
                raise ConfigError(f"AGENT_INSTANCE_UNKNOWN: {args.owner_instance}")
            sql += " AND owner_instance_id=?"
            params.append(str(instance["id"]))
        if args.origin:
            sql += " AND workflow_origin=?"
            params.append(args.origin)
        sql += " ORDER BY priority ASC, updated_utc DESC, id ASC"
        for row in conn.execute(sql, params):
            print(
                f"{row['id']}\t{row['status']}\t{row['lane'] or ''}\t"
                f"{row['priority'] or ''}\t{row['title']}\t{row['workflow_origin'] or ''}"
            )
    return 0


def cmd_backlog_get(args: argparse.Namespace) -> int:
    config = load_config()
    with open_read_model(config) as snapshot:
        conn = snapshot.conn
        row = conn.execute("SELECT * FROM backlog_items WHERE id=?", (args.item_id,)).fetchone()
        if row is None:
            print(f"agent-q backlog get: not found: {args.item_id}", file=sys.stderr)
            return 1
        print(f"id: {row['id']}")
        print(f"title: {row['title']}")
        print(f"status: {row['status']}")
        print(f"lane: {row['lane'] or ''}")
        print(f"priority: {row['priority'] or ''}")
        owner = public_instance_handle_for_id(conn, str(row["owner_instance_id"] or "")) or str(
            row["owner_hint"] or ""
        )
        print(f"owner: {owner}")
        print(f"workflow_origin: {row['workflow_origin'] or ''}")
        print(f"workflow_origin_source: {row['workflow_origin_source']}")
        print(f"workflow_origin_valid: {str(bool(row['workflow_origin_valid'])).lower()}")
        if not row["workflow_origin_valid"]:
            print("workflow_origin_warning: historical invalid or conflicting origin")
        if row["summary"]:
            print(f"summary: {row['summary']}")
        if row["notes"]:
            print(f"notes: {row['notes']}")
        refs = json_loads(row["refs_json"], [])
        if refs:
            print("refs:")
            for ref in refs:
                if isinstance(ref, dict):
                    print(f"- {ref.get('type', 'unknown')}:{ref.get('value', '')}")
                else:
                    print(f"- {ref}")
        links = conn.execute(
            "SELECT ref_type, ref_value FROM backlog_item_links WHERE item_id=? ORDER BY ref_type, ref_value",
            (args.item_id,),
        ).fetchall()
        if links:
            print("links:")
            for link in links:
                print(f"- {link['ref_type']}:{link['ref_value']}")
    return 0


def cmd_backlog_events(args: argparse.Namespace) -> int:
    config = load_config()
    with open_read_model(config) as snapshot:
        conn = snapshot.conn
        rows = conn.execute(
            "SELECT * FROM backlog_events WHERE item_id=? ORDER BY event_seq ASC",
            (args.item_id,),
        ).fetchall()
        if not rows:
            print(f"agent-q backlog events: not found: {args.item_id}", file=sys.stderr)
            return 1
        for row in rows:
            details = json_loads(row["details_json"], {})
            print(f"{row['event_type']}\t{row['actor']}\t{json.dumps(details, sort_keys=True)}")
    return 0


def cmd_refs_resolve(args: argparse.Namespace) -> int:
    config = load_config()
    occurrences, boundary, input_source_count = _reference_inputs(config, args)
    try:
        with open_read_model(
            config,
            max_bytes=MAX_REFERENCE_SNAPSHOT_BYTES,
            max_events=MAX_REFERENCE_SNAPSHOT_EVENTS,
            deadline_monotonic=time.monotonic() + MAX_REFERENCE_SNAPSHOT_SECONDS,
        ) as snapshot:
            result = build_reference_context(
                config,
                snapshot,
                occurrences,
                boundary=boundary,
                include_diagnostics=bool(args.diagnostics),
            )
    except ReadModelUnavailable as exc:
        unavailable = build_unavailable_reference_context(
            boundary=boundary,
            occurrence_count=len(occurrences),
            source_count=input_source_count,
            diagnostic=str(exc),
        )
        if args.json:
            rendered, _ = render_bounded_reference_context_json(unavailable)
            print(rendered)
        else:
            print(f"agent-q refs resolve: canonical state unavailable: {exc}", file=sys.stderr)
        return 3

    if args.json:
        rendered, complete = render_bounded_reference_context_json(result)
        print(rendered)
        if not complete:
            return 3
    else:
        if result.get("complete") is not True:
            for diagnostic in result.get("diagnostics", []):
                print(f"agent-q refs resolve: {diagnostic}", file=sys.stderr)
            return 3
        stdout_text, stderr_text, text_complete = _render_reference_resolutions(result)
        if not text_complete:
            print(
                "agent-q refs resolve: reference context exceeds the "
                f"{MAX_REFERENCE_CONTEXT_TEXT_BYTES}-byte text output bound; "
                "narrow the input set or use --json",
                file=sys.stderr,
            )
            return 3
        sys.stdout.write(stdout_text)
        sys.stderr.write(stderr_text)
    counts = result.get("resolution_counts", {})
    unresolved = (
        int(counts.get("partial", 0))
        + int(counts.get("not_found", 0))
        + int(counts.get("unsupported", 0))
    )
    return 1 if unresolved else 0


def _reference_inputs(
    config: AgentMeshConfig,
    args: argparse.Namespace,
) -> tuple[list[ReferenceOccurrence], str, int]:
    if not args.references and not args.file and not args.read_stdin:
        raise ConfigError("refs resolve requires an explicit ID, --file, or --stdin")
    if len(args.file) > MAX_REFERENCE_FILES:
        raise ConfigError(f"refs resolve accepts at most {MAX_REFERENCE_FILES} files")
    try:
        occurrences = explicit_reference_occurrences(
            args.references,
            max_occurrences=MAX_REFERENCE_CONTEXT_OCCURRENCES + 1,
        )
    except ValueError as exc:
        raise ConfigError(str(exc)) from exc
    input_kinds: list[str] = ["explicit"] if args.references else []
    total_bytes = sum(len(item.encode("utf-8")) for item in args.references)
    for path in args.file:
        if len(str(path).encode("utf-8")) > MAX_REFERENCE_PATH_BYTES:
            raise ConfigError(
                f"reference input path exceeds {MAX_REFERENCE_PATH_BYTES} UTF-8 bytes"
            )
        source, text = _read_reference_file(config, path)
        total_bytes += len(text.encode("utf-8"))
        if total_bytes > MAX_REFERENCE_INPUT_BYTES:
            raise ConfigError(f"reference inputs exceed {MAX_REFERENCE_INPUT_BYTES} UTF-8 bytes")
        occurrences.extend(
            extract_reference_occurrences(
                text,
                source=source,
                source_kind="file",
                max_occurrences=max(
                    0,
                    MAX_REFERENCE_CONTEXT_OCCURRENCES + 1 - len(occurrences),
                ),
            )
        )
        input_kinds.append("file")
    if args.read_stdin:
        text = _read_reference_stdin()
        total_bytes += len(text.encode("utf-8"))
        if total_bytes > MAX_REFERENCE_INPUT_BYTES:
            raise ConfigError(f"reference inputs exceed {MAX_REFERENCE_INPUT_BYTES} UTF-8 bytes")
        occurrences.extend(
            extract_reference_occurrences(
                text,
                source="<stdin>",
                source_kind="stdin",
                max_occurrences=max(
                    0,
                    MAX_REFERENCE_CONTEXT_OCCURRENCES + 1 - len(occurrences),
                ),
            )
        )
        input_kinds.append("stdin")
    boundary = input_kinds[0] if len(set(input_kinds)) == 1 else "mixed"
    return occurrences, boundary, len(args.file) + int(bool(args.references)) + int(args.read_stdin)


def _read_reference_file(config: AgentMeshConfig, path: Path) -> tuple[str, str]:
    root = config.project_root.resolve()
    candidate = path if path.is_absolute() else root / path
    try:
        resolved = candidate.resolve(strict=True)
    except OSError as exc:
        raise ConfigError(f"cannot read reference file {path}: {exc}") from exc
    try:
        relative = resolved.relative_to(root)
    except ValueError as exc:
        raise ConfigError(f"reference file must remain inside the project root: {path}") from exc
    if not resolved.is_file():
        raise ConfigError(f"reference input is not a file: {path}")
    try:
        with resolved.open("rb") as stream:
            raw_bytes = stream.read(MAX_REFERENCE_FILE_BYTES + 1)
    except OSError as exc:
        raise ConfigError(f"cannot read reference file {path}: {exc}") from exc
    if len(raw_bytes) > MAX_REFERENCE_FILE_BYTES:
        raise ConfigError(f"reference file exceeds {MAX_REFERENCE_FILE_BYTES} bytes: {path}")
    try:
        text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ConfigError(f"reference file must be valid UTF-8 text: {path}") from exc
    return relative.as_posix(), text


def _read_reference_stdin() -> str:
    if sys.stdin.isatty():
        raise ConfigError("--stdin requires piped UTF-8 text")
    binary_stream = getattr(sys.stdin, "buffer", None)
    if binary_stream is not None:
        raw_bytes = binary_stream.read(MAX_REFERENCE_INPUT_BYTES + 1)
    else:
        raw_text = sys.stdin.read(MAX_REFERENCE_INPUT_BYTES + 1)
        raw_bytes = raw_text.encode("utf-8")
    if len(raw_bytes) > MAX_REFERENCE_INPUT_BYTES:
        raise ConfigError(f"stdin exceeds {MAX_REFERENCE_INPUT_BYTES} UTF-8 bytes")
    try:
        return raw_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ConfigError("stdin must be valid UTF-8 text") from exc


def _render_reference_resolutions(result: dict[str, Any]) -> tuple[str, str, bool]:
    output = BoundedReferenceText()
    for resolution in result.get("resolutions", []):
        state = str(resolution.get("resolution_status") or "unknown")
        if state not in {"resolved", "partial"}:
            output.add_stdout(f"{_single_line(resolution.get('requested_id'))} [{state}]")
            for warning in resolution.get("warnings", []):
                output.add_stderr(f"warning: {_single_line(warning)}")
            continue
        canonical_id = _single_line(resolution.get("canonical_id"))
        status = _single_line(resolution.get("status") or resolution.get("kind"))
        title = _single_line(resolution.get("title"))
        line = f"{canonical_id} [{status}] {title}".rstrip()
        revision = str(resolution.get("revision_sha256") or "")
        if revision:
            line += f" (revision {revision[:12]})"
        canonical_text = _single_line(resolution.get("canonical_text"))
        if canonical_text:
            line += f" — {canonical_text[:240]}"
            if len(canonical_text) > 240 or resolution.get("canonical_text_truncated"):
                line += "…"
        fragment = _single_line(resolution.get("fragment"))
        if fragment:
            line += f" [fragment {fragment}: unvalidated]"
        output.add_stdout(line)
        for warning in resolution.get("warnings", []):
            output.add_stderr(f"warning: {canonical_id}: {_single_line(warning)}")
    return output.render()


def _single_line(value: object) -> str:
    return " ".join(
        "".join(
            character if character.isprintable() else " " for character in str(value or "")
        ).split()
    )


def cmd_decisions_list(args: argparse.Namespace) -> int:
    config = load_config()
    with open_read_model(
        config,
        max_bytes=MAX_DECISION_QUERY_SOURCE_BYTES,
        max_events=MAX_DECISION_QUERY_EVENTS,
        deadline_monotonic=time.monotonic() + MAX_DECISION_QUERY_SECONDS,
    ) as snapshot:
        conn = snapshot.conn
        scoped_ids: set[str] | None = None
        if args.scope:
            lifecycle_statuses = (
                {args.status}
                if args.status
                else {
                    str(row["status"])
                    for row in conn.execute("SELECT DISTINCT status FROM decisions")
                }
            )
            _, applicable = evaluate_decision_applicability(
                conn,
                [args.scope],
                lifecycle_statuses=lifecycle_statuses,
            )
            scoped_ids = {str(item["id"]) for item in applicable}
        sql = "SELECT * FROM decisions WHERE 1=1"
        params: list[str] = []
        if args.status:
            sql += " AND status=?"
            params.append(args.status)
        if args.tier:
            sql += " AND tier=?"
            params.append(args.tier)
        if args.invalid_tier:
            sql += " AND tier_valid=0"
        if args.owner:
            sql += " AND owner=?"
            params.append(args.owner)
        sql += " ORDER BY human_id"
        emitted = 0
        for row in conn.execute(sql, params):
            if scoped_ids is not None and str(row["human_id"]) not in scoped_ids:
                continue
            tier = row["tier"] if row["tier_valid"] else f"{row['tier']} [INVALID]"
            issues = _decision_completeness_from_projection(conn, row)
            if args.incomplete and not issues:
                continue
            if emitted >= args.limit:
                print(
                    f"warning: decision list truncated after {args.limit} results; "
                    "refine the filters or request a smaller scope",
                    file=sys.stderr,
                )
                break
            completeness = "complete" if not issues else "INCOMPLETE"
            print(f"{row['human_id']}\t{row['status']}\t{tier}\t{completeness}\t{row['title']}")
            emitted += 1
    return 0


def cmd_decisions_show(args: argparse.Namespace) -> int:
    config = load_config()
    with open_read_model(
        config,
        max_bytes=MAX_DECISION_QUERY_SOURCE_BYTES,
        max_events=MAX_DECISION_QUERY_EVENTS,
        deadline_monotonic=time.monotonic() + MAX_DECISION_QUERY_SECONDS,
    ) as snapshot:
        conn = snapshot.conn
        dec_ulid = resolve_decision(conn, args.identifier)
        if dec_ulid is None:
            print(f"agent-q decisions show: not found: {args.identifier}", file=sys.stderr)
            return 1
        row = conn.execute("SELECT * FROM decisions WHERE dec_ulid=?", (dec_ulid,)).fetchone()
        meta = json_loads(row["meta_json"], {})
        if not isinstance(meta, dict):
            meta = {}
        lineage = decision_lineage_summary(meta)
        supersedes = _decision_human_id(conn, row["supersedes"])
        superseded_by = _decision_human_id(conn, row["superseded_by"])
        print(str(row["human_id"]))
        if args.diagnostics:
            print(f"internal_id: {row['dec_ulid']}")
        print(f"title: {row['title']}")
        print(f"tier: {row['tier']}")
        print(f"tier_valid: {str(bool(row['tier_valid'])).lower()}")
        if not row["tier_valid"]:
            print("tier_warning: historical invalid tier; canonical event was not rewritten")
        print(f"status: {row['status']}")
        print(f"applicability_scope: {row['applicability_scope']}")
        print(f"owner: {row['owner'] or ''}")
        print(f"supersedes: {supersedes}")
        print(f"superseded_by: {superseded_by}")
        print(f"content_revisions: {lineage.content_revisions}")
        print(f"revisit_annotations: {lineage.revisit_annotations}")
        review_policy = normalize_decision_review_policy(
            meta.get("review_policy", {}), allow_extensions=True
        )
        revision_sha = decision_revision_sha_from_projection(conn, row)
        review_progress = decision_review_progress(meta, revision_sha)
        print(f"revision_sha: {revision_sha}")
        if review_progress["approval_binding"] == "legacy_pre_authoring_digest":
            print(f"approved_revision_sha: {review_progress['approval_revision_sha']}")
            print("approval_binding: legacy_pre_authoring_digest")
        for reviewer in review_policy.get("required_reviewers", []):
            print(f"required_reviewer: {reviewer}")
        if review_policy:
            print(f"approval_quorum: {review_policy['approval_quorum']}")
        issues = _decision_completeness_from_projection(conn, row)
        print(f"complete_for_acceptance: {str(not issues).lower()}")
        for issue in issues:
            print(f"completeness_issue: {issue}")
        for item in conn.execute(
            "SELECT pattern FROM decision_globs "
            "WHERE dec_ulid=? AND kind='affected' ORDER BY pattern",
            (dec_ulid,),
        ):
            print(f"affected_code_glob: {item['pattern']}")
        for kind, label in (
            ("exempt", "exemption_glob"),
            ("generated", "generated_artifact_glob"),
        ):
            for item in conn.execute(
                "SELECT pattern FROM decision_globs WHERE dec_ulid=? AND kind=? ORDER BY pattern",
                (dec_ulid, kind),
            ):
                print(f"{label}: {item['pattern']}")
        for item in conn.execute(
            "SELECT check_name FROM decision_checks WHERE dec_ulid=? ORDER BY check_name",
            (dec_ulid,),
        ):
            print(f"required_check: {item['check_name']}")
        print(f"last_verified_utc: {row['last_verified_utc'] or ''}")
        for item in conn.execute(
            "SELECT command, execution_mode, expected_signal, last_verified_utc, last_outcome "
            "FROM decision_verifications WHERE dec_ulid=? ORDER BY command",
            (dec_ulid,),
        ):
            print(
                "verification: "
                f"{item['last_outcome'] or 'never'}\t"
                f"{item['last_verified_utc'] or ''}\t"
                f"{item['command']}\texpected={item['expected_signal']}\t"
                f"mode={item['execution_mode']}"
            )
        if args.assumptions:
            for item in conn.execute(
                "SELECT assumption_id, status, text, references_json "
                "FROM decision_assumptions "
                "WHERE dec_ulid=? ORDER BY assumption_id",
                (dec_ulid,),
            ):
                suffix = ""
                references = json_loads(item["references_json"], [])
                if isinstance(references, list) and references:
                    suffix = " (references: " + ", ".join(str(ref) for ref in references) + ")"
                print(
                    f"assumption {item['assumption_id']} [{item['status']}]: {item['text']}{suffix}"
                )
        if args.evidence:
            for item in conn.execute(
                "SELECT evidence_kind, ref_value FROM decision_evidence "
                "WHERE dec_ulid=? ORDER BY evidence_kind, ref_value",
                (dec_ulid,),
            ):
                print(f"evidence {item['evidence_kind']}: {item['ref_value']}")
        if args.references:
            for item in conn.execute(
                "SELECT file_path, line_start, reference_form FROM decision_references_in_code "
                "WHERE dec_ulid=? ORDER BY file_path, line_start",
                (dec_ulid,),
            ):
                print(
                    f"reference {item['file_path']}:{item['line_start']} {item['reference_form']}"
                )
        if args.body:
            body = _decision_body_for_cli(config, row, meta)
            print("body:")
            sys.stdout.write(body)
            if not body.endswith("\n"):
                print()
    return 0


def _decision_body_for_cli(config: AgentMeshConfig, row, meta: dict[str, Any]) -> str:
    """Resolve the current canonical body without trusting a compatibility view."""

    body_path = str(row["body_path"] or "")
    body_sha = str(row["body_sha"] or "")
    body_bytes = int(row["body_bytes"] or 0)
    if body_bytes > MAX_DECISION_BODY_OUTPUT_BYTES:
        raise ReadModelUnavailable(
            f"decision body is {body_bytes} bytes; --body is limited to "
            f"{MAX_DECISION_BODY_OUTPUT_BYTES} bytes"
        )
    if body_path:
        try:
            data = read_verified_decision_body(
                config.agent_dir,
                body_path=body_path,
                body_sha=body_sha,
                body_bytes=body_bytes,
                max_bytes=MAX_DECISION_BODY_OUTPUT_BYTES,
            )
            return data.decode("utf-8")
        except (DecisionBodyIntegrityError, UnicodeDecodeError) as exc:
            raise ReadModelUnavailable(f"decision body integrity check failed: {exc}") from exc

    legacy_body = meta.get("legacy_markdown_body")
    if isinstance(legacy_body, str):
        data = legacy_body.encode("utf-8")
        observed_sha = hashlib.sha256(data).hexdigest()
        if len(data) != body_bytes or observed_sha != body_sha:
            raise ReadModelUnavailable(
                "decision body integrity check failed: embedded legacy body does not "
                "match the current canonical size and digest"
            )
        return legacy_body

    if body_bytes:
        raise ReadModelUnavailable(
            "decision body is referenced by canonical metadata but no readable body is available"
        )

    sections: list[str] = []
    for label, value in (
        ("Context", meta.get("context")),
        ("Decision", meta.get("decision")),
        ("Consequences", meta.get("consequences")),
        ("Rejected Alternatives", meta.get("rejected_alternatives")),
    ):
        if isinstance(value, list):
            rendered = "\n".join(f"- {item}" for item in value if str(item).strip())
        else:
            rendered = str(value or "").strip()
        if rendered:
            sections.append(f"## {label}\n{rendered}")
    body = "\n\n".join(sections) + ("\n" if sections else "")
    if len(body.encode("utf-8")) > MAX_DECISION_BODY_OUTPUT_BYTES:
        raise ReadModelUnavailable(
            f"decision body exceeds the {MAX_DECISION_BODY_OUTPUT_BYTES}-byte --body limit"
        )
    return body


def _decision_completeness_from_projection(conn, row) -> tuple[str, ...]:
    globs_by_kind = {
        kind: [
            item["pattern"]
            for item in conn.execute(
                "SELECT pattern FROM decision_globs WHERE dec_ulid=? AND kind=?",
                (row["dec_ulid"], kind),
            )
        ]
        for kind in ("affected", "exempt", "generated")
    }
    verification = [
        {"command": item["command"], "expected_signal": item["expected_signal"]}
        for item in conn.execute(
            "SELECT command, expected_signal FROM decision_verifications WHERE dec_ulid=?",
            (row["dec_ulid"],),
        )
    ]
    return decision_completeness_issues(
        tier=str(row["tier"]),
        owner=str(row["owner"] or ""),
        affected_code_globs=globs_by_kind["affected"],
        verification=verification,
        applicability_scope=str(row["applicability_scope"]),
        exemptions=globs_by_kind["exempt"],
        generated_artifact_paths=globs_by_kind["generated"],
    )


def cmd_decisions_log(args: argparse.Namespace) -> int:
    config = load_config()
    with open_read_model(config) as snapshot:
        conn = snapshot.conn
        dec_ulid = resolve_decision(conn, args.identifier)
        if dec_ulid is None:
            print(f"agent-q decisions log: not found: {args.identifier}", file=sys.stderr)
            return 1
        for record in snapshot.records:
            if (
                record.get("entity_id") == dec_ulid
                or record.get("payload", {}).get("decision_id") == dec_ulid
            ):
                print(
                    f"{record['event_seq']}\t{record['kind']}\t{record['occurred_utc']}\t{record['event_id']}"
                )
    return 0


def cmd_decisions_diagnose(args: argparse.Namespace) -> int:
    config = load_config()
    snapshot = capture_event_snapshot(config)
    diagnostic = diagnose_decision_replay(config, records=snapshot.records)
    if diagnostic.ok:
        print(f"decision replay: OK ({diagnostic.total_events} events)")
        return 0
    print("decision replay: BLOCKED")
    print(f"event_seq: {diagnostic.event_seq}")
    print(f"event_id: {diagnostic.event_id}")
    print(f"kind: {diagnostic.kind}")
    print(f"code: {diagnostic.code}")
    print(f"detail: {diagnostic.detail}")
    print(f"valid_prefix_events: {diagnostic.valid_events}")
    if diagnostic.code == "DECISION_BODY_PROJECTION_DIVERGENCE":
        print(
            "recovery: canonical events were not changed; amend the named decision through "
            "Agent Mesh with synchronized metadata and --from-file Markdown, then obtain fresh "
            "human approval if the decision had been accepted or in force."
        )
        return 1
    print(
        "recovery: canonical events were not changed; do not hand-edit events.jsonl. "
        "Restore a valid log backup or upgrade/migrate the identified legacy event, "
        "then run agent-q rebuild --all."
    )
    _ = args
    return 1


def cmd_decisions_search(args: argparse.Namespace) -> int:
    config = load_config()
    query = args.query.lower()
    with open_read_model(config) as snapshot:
        conn = snapshot.conn
        rows = conn.execute("SELECT * FROM decisions ORDER BY human_id").fetchall()
        for row in rows:
            meta = json_loads(row["meta_json"], {})
            haystack = " ".join(
                [
                    row["human_id"],
                    row["title"],
                    str(meta.get("context", "")),
                    str(meta.get("decision", "")),
                ]
            ).lower()
            if query in haystack:
                print(f"{row['human_id']}\t{row['status']}\t{row['title']}")
    return 0


def cmd_decisions_at(args: argparse.Namespace) -> int:
    config = load_config()
    with open_read_model(config) as snapshot:
        _, decisions = evaluate_decision_applicability(snapshot.conn, [args.path])
        for decision in decisions:
            for match in decision["matches"]:
                pattern = match["pattern"] or "<repository>"
                print(
                    f"{_decision_text_cell(decision['id'])}\t{decision['status']}\t"
                    f"{_decision_text_cell(pattern)}\t{_decision_text_cell(decision['title'])}"
                )
    return 0


def _decision_text_cell(value: Any) -> str:
    """Keep untrusted path and decision text within one bounded CLI row."""

    return " ".join(str(value).split())[:1000]


class _DecisionContextTextOverflow(RuntimeError):
    pass


class _BoundedDecisionContextText:
    def __init__(self, max_bytes: int) -> None:
        self._max_bytes = max_bytes
        self._size = 0
        self._chunks: list[str] = []

    def append(self, value: str) -> None:
        size = len(value.encode("utf-8"))
        if self._size + size > self._max_bytes:
            raise _DecisionContextTextOverflow
        self._chunks.append(value)
        self._size += size

    def render(self) -> str:
        return "".join(self._chunks)


def _unique_decision_hook_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise DecisionHookRequestError("request contains a duplicate JSON field")
        result[key] = value
    return result


def _read_decision_hook_request() -> tuple[str, list[str]]:
    if sys.stdin.isatty():
        raise DecisionHookRequestError(
            f"expected one {DECISION_HOOK_REQUEST_SCHEMA} JSON object on stdin"
        )
    binary_stream = getattr(sys.stdin, "buffer", None)
    if binary_stream is not None:
        raw_bytes = binary_stream.read(MAX_DECISION_HOOK_REQUEST_BYTES + 1)
    else:
        raw_text = sys.stdin.read(MAX_DECISION_HOOK_REQUEST_BYTES + 1)
        raw_bytes = raw_text.encode("utf-8")
    if len(raw_bytes) > MAX_DECISION_HOOK_REQUEST_BYTES:
        raise DecisionHookRequestError(
            f"stdin exceeds {MAX_DECISION_HOOK_REQUEST_BYTES} UTF-8 bytes"
        )
    try:
        raw_text = raw_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise DecisionHookRequestError("stdin must be valid UTF-8 JSON") from exc
    if not raw_text.strip():
        raise DecisionHookRequestError("stdin must contain one JSON object")
    try:
        request = json.loads(raw_text, object_pairs_hook=_unique_decision_hook_object)
    except DecisionHookRequestError:
        raise
    except (json.JSONDecodeError, ValueError, OverflowError, RecursionError) as exc:
        raise DecisionHookRequestError("stdin must contain one valid JSON object") from exc
    if not isinstance(request, dict):
        raise DecisionHookRequestError("request must be a JSON object")
    allowed_fields = {"schema", "boundary", "paths"}
    unknown_fields = sorted(set(request) - allowed_fields)
    if unknown_fields:
        raise DecisionHookRequestError(
            f"request contains {len(unknown_fields)} unknown JSON field(s)"
        )
    if request.get("schema") != DECISION_HOOK_REQUEST_SCHEMA:
        raise DecisionHookRequestError(f"schema must be {DECISION_HOOK_REQUEST_SCHEMA!r}")
    boundary = request.get("boundary")
    if boundary not in DECISION_HOOK_BOUNDARIES:
        raise DecisionHookRequestError("boundary must be 'edit' or 'write'")
    paths = request.get("paths")
    if not isinstance(paths, list) or not paths:
        raise DecisionHookRequestError("paths must be a non-empty JSON string array")
    if not all(isinstance(path, str) for path in paths):
        raise DecisionHookRequestError("paths must be a non-empty JSON string array")
    return str(boundary), paths


def _emit_decision_context(
    args: argparse.Namespace,
    *,
    candidate_paths: list[str],
    boundary: str,
    include_proposed: bool,
    include_diagnostics: bool,
    json_output: bool,
    digest_output: bool = False,
) -> int:
    candidate_paths = list(normalize_candidate_paths(candidate_paths))
    args._decision_context_boundary = boundary
    args._decision_context_paths = candidate_paths
    config = load_config()
    with open_read_model(config) as snapshot:
        result = build_decision_context(
            config,
            snapshot,
            candidate_paths,
            boundary=boundary,
            include_proposed=include_proposed,
            include_diagnostics=include_diagnostics,
        )
        decisions = result["decisions"]
        if json_output:
            rendered, complete = render_bounded_decision_context_json(
                result,
                max_bytes=MAX_DECISION_CONTEXT_JSON_BYTES,
            )
            print(rendered)
            return 0 if complete else 3
        if digest_output:
            try:
                print(render_decision_digest(result))
            except DecisionDigestOverflow as exc:
                print(f"agent-q: decision context incomplete: {exc}", file=sys.stderr)
                return 3
            return 0 if result.get("complete") is True else 3
        if result.get("complete") is not True:
            diagnostic = next(iter(result.get("diagnostics") or []), "context is incomplete")
            print(
                f"agent-q: decision context incomplete: {diagnostic}; use --json or "
                "narrow the path set",
                file=sys.stderr,
            )
            return 3
        if not decisions:
            print("No applicable decisions.")
            return 0
        writer = _BoundedDecisionContextText(MAX_DECISION_CONTEXT_TEXT_BYTES)
        try:
            for decision_index, decision in enumerate(decisions):
                if decision_index:
                    writer.append("\n")
                writer.append(
                    f"{_decision_text_cell(decision['id'])}\t{decision['status']}\t"
                    f"{decision['effective_enforcement']}\t"
                )
                for match_index, match in enumerate(decision["matches"]):
                    if match_index:
                        writer.append(", ")
                    if match["path"] is None:
                        writer.append("<repository>")
                    elif match["pattern"] is None:
                        writer.append(_decision_text_cell(match["path"]))
                    else:
                        writer.append(
                            f"{_decision_text_cell(match['path'])} "
                            f"({_decision_text_cell(match['pattern'])})"
                        )
                writer.append(f"\t{_decision_text_cell(decision['title'])}")
        except _DecisionContextTextOverflow:
            print(
                "agent-q: decision context incomplete: applicable decisions exceed the text "
                "output bound; use --json or narrow the path set",
                file=sys.stderr,
            )
            return 3
        print(writer.render())
        for warning in snapshot.warnings:
            print(f"warning: {warning}", file=sys.stderr)
    return 0


def cmd_decisions_preflight(args: argparse.Namespace) -> int:
    return _emit_decision_context(
        args,
        candidate_paths=list(args.path),
        boundary="task",
        include_proposed=bool(args.include_proposed),
        include_diagnostics=bool(args.diagnostics),
        json_output=bool(args.json),
        digest_output=bool(args.digest),
    )


def cmd_decisions_hook(args: argparse.Namespace) -> int:
    boundary, paths = _read_decision_hook_request()
    return _emit_decision_context(
        args,
        candidate_paths=paths,
        boundary=boundary,
        include_proposed=False,
        include_diagnostics=False,
        json_output=True,
    )


def cmd_decisions_verify(args: argparse.Namespace) -> int:
    config = load_config()
    actor = _authoring_actor(config)
    _rebuild_all_locked(config)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        dec_ulid = resolve_decision(conn, args.identifier)
        if dec_ulid is None:
            print(f"agent-q decisions verify: not found: {args.identifier}", file=sys.stderr)
            return 1
        decision = conn.execute(
            "SELECT human_id, status FROM decisions WHERE dec_ulid=?", (dec_ulid,)
        ).fetchone()
        rows = conn.execute(
            "SELECT command, execution_mode, argv_json, expected_signal, last_verified_utc "
            "FROM decision_verifications WHERE dec_ulid=? "
            "ORDER BY command",
            (dec_ulid,),
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        print("no verification commands")
        return 0
    status = str(decision["status"] if decision is not None else "")
    if status not in VERIFICATION_EXECUTABLE_STATUSES:
        print(
            "agent-q decisions verify: refused: verification definitions are executable only "
            f"after direct human acceptance; {args.identifier} is {status or 'unknown'}",
            file=sys.stderr,
        )
        return 2

    prepared: list[tuple[Any, list[str]]] = []
    for row in rows:
        argv_value = _projected_verification_argv(row)
        if argv_value is None:
            print(
                "agent-q decisions verify: refused legacy shell-dependent definition: "
                f"{row['command']!r}. Amend {decision['human_id']} to invoke a reviewed "
                "repository script or executable as argv, then obtain fresh human acceptance.",
                file=sys.stderr,
            )
            return 2
        prepared.append((row, argv_value))

    failed = 0
    for row, argv in prepared:
        process, exit_code, actual_signal, refusal = _start_decision_verification(
            config,
            dec_ulid=dec_ulid,
            command=str(row["command"]),
            execution_mode=str(row["execution_mode"]),
            argv=argv,
        )
        if refusal is not None:
            print(f"agent-q decisions verify: refused: {refusal}", file=sys.stderr)
            return 2
        if process is not None:
            process.communicate()
            exit_code = process.returncode
            actual_signal = f"exit {exit_code}"
        assert exit_code is not None
        assert actual_signal is not None
        outcome = "pass" if exit_code == 0 else "fail"
        outcome_event = Event(
            event_id=generate_event_id(),
            actor=actor,
            kind="decision_verification_recorded",
            entity_id=dec_ulid,
            thread_id=dec_ulid,
            payload={
                "decision_id": dec_ulid,
                "command": row["command"],
                "execution_mode": str(row["execution_mode"]),
                "argv": argv,
                "expected_signal": row["expected_signal"],
                "actual_signal": actual_signal,
                "outcome": outcome,
                "exit_code": exit_code,
                "last_verified_utc_before": row["last_verified_utc"],
            },
        )
        append_event(config.events_path, outcome_event, lock_acquired=False)
        if outcome == "pass":
            print(f"pass: {row['command']}")
            continue
        failed += 1
        print(f"fail: {row['command']} ({actual_signal})", file=sys.stderr)
        event = Event(
            event_id=generate_event_id(),
            actor=actor,
            kind="decision_drift_detected",
            entity_id=dec_ulid,
            thread_id=dec_ulid,
            payload={
                "decision_id": dec_ulid,
                "command": row["command"],
                "execution_mode": str(row["execution_mode"]),
                "argv": argv,
                "expected_signal": row["expected_signal"],
                "actual_signal": actual_signal,
                "last_verified_utc_before": row["last_verified_utc"],
            },
        )
        append_event(config.events_path, event, lock_acquired=False)
    return 1 if failed else 0


def _projected_verification_argv(row: Any) -> list[str] | None:
    mode = str(row["execution_mode"] or "")
    try:
        argv_value = json.loads(str(row["argv_json"])) if row["argv_json"] else None
    except (TypeError, ValueError):
        return None
    if (
        mode not in {VERIFICATION_EXECUTION_MODE_ARGV, VERIFICATION_EXECUTION_MODE_LEGACY_ARGV}
        or not isinstance(argv_value, list)
        or not argv_value
        or any(not isinstance(part, str) or not part or "\x00" in part for part in argv_value)
    ):
        return None
    return argv_value


def _start_decision_verification(
    config: AgentMeshConfig,
    *,
    dec_ulid: str,
    command: str,
    execution_mode: str,
    argv: list[str],
) -> tuple[subprocess.Popen[str] | None, int | None, str | None, str | None]:
    """Revalidate authorization and spawn while the append lock prevents a revision race."""

    lock_handle = acquire(config.agent_dir / ".mail-lock")
    try:
        recover(config.events_path, config.agent_dir)
        rebuild_all(config)
        conn = connect(config.db_path)
        try:
            initialize_schema(conn)
            decision = conn.execute(
                "SELECT human_id, status FROM decisions WHERE dec_ulid=?", (dec_ulid,)
            ).fetchone()
            current = conn.execute(
                "SELECT command, execution_mode, argv_json FROM decision_verifications "
                "WHERE dec_ulid=? AND command=?",
                (dec_ulid, command),
            ).fetchone()
        finally:
            conn.close()
        if decision is None or str(decision["status"]) not in VERIFICATION_EXECUTABLE_STATUSES:
            status = str(decision["status"] if decision is not None else "unknown")
            return None, None, None, f"decision is no longer human-approved ({status})"
        if (
            current is None
            or str(current["execution_mode"]) != execution_mode
            or _projected_verification_argv(current) != argv
        ):
            return None, None, None, "verification definition changed before execution"
        try:
            process = subprocess.Popen(
                argv,
                cwd=config.project_root,
                shell=False,
                text=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except FileNotFoundError:
            return None, 127, "exec error: executable not found", None
        except PermissionError:
            return None, 126, "exec error: executable not permitted", None
        return process, None, None, None
    finally:
        lock_handle.release()


def _decision_human_id(conn, dec_ulid: str | None) -> str:
    if not dec_ulid:
        return ""
    row = conn.execute("SELECT human_id FROM decisions WHERE dec_ulid=?", (dec_ulid,)).fetchone()
    return str(row["human_id"]) if row else "[unresolved decision]"


def cmd_dispatches_preflight(args: argparse.Namespace) -> int:
    config = load_config()
    with open_read_model(config):
        profile = config.runtime_profile_for_target(args.target, profile_name=args.profile)
        result = preflight_runtime_profile(profile, project_root=config.project_root)
        _print_runtime_preflight(result)
    return 0 if result.passed else 1


def cmd_drivers_check(args: argparse.Namespace) -> int:
    config = load_config()
    profile = config.runtime_profile_for_target(args.target, profile_name=args.profile)
    result = preflight_runtime_profile(profile, project_root=config.project_root)
    _print_runtime_preflight(result)
    return 0 if result.passed else 1


def _print_runtime_preflight(result: RuntimePreflightResult) -> None:
    status = "PASS" if result.passed else "FAIL"
    print(f"runtime preflight {status}\tprofile={result.profile_name}\ttarget={result.target}")
    for check in result.checks:
        check_status = "PASS" if check.passed else "FAIL"
        print(f"{check_status}\t{check.name}\t{check.detail}")


def _runtime_adapter_for(
    config,
    target: str,
    *,
    profile_name: str | None = None,
    registry: RuntimeAdapterRegistry | None = None,
) -> AgentRuntimeAdapter:
    profile = config.runtime_profile_for_target(target, profile_name=profile_name)
    result = preflight_runtime_profile(
        profile,
        project_root=config.project_root,
        registry=registry,
    )
    if not result.passed or result.binary_path is None:
        raise ConfigError(
            f"runtime preflight failed for profile {profile.name!r}: {result.failure_summary()}"
        )
    return _runtime_adapter_from_preflight(
        config,
        profile,
        result,
        registry=registry,
    )


def _runtime_adapter_from_preflight(
    config,
    profile,
    result: RuntimePreflightResult,
    *,
    registry: RuntimeAdapterRegistry | None = None,
) -> AgentRuntimeAdapter:
    if not result.passed or result.binary_path is None:
        raise ConfigError(
            f"runtime preflight failed for profile {profile.name!r}: {result.failure_summary()}"
        )
    drivers = (
        registry
        if registry is not None
        else runtime_registry_for_profile(
            profile,
            project_root=config.project_root,
            registry=built_in_runtime_registry(),
        )
    )
    driver = drivers.resolve(profile)
    return driver.factory(
        profile,
        binary_path=result.binary_path,
        context=RuntimeDriverContext(
            events_path=config.events_path,
            project_scope=config.store_id or config.project_key,
        ),
    )


def cmd_dispatches_cancel(args: argparse.Namespace) -> int:
    config = load_config()
    actor = _authoring_actor(config, explicit_actor=args.actor)
    lock_handle = acquire(config.agent_dir / ".mail-lock")
    try:
        rebuild_all(config)
        try:
            cancelled = cancel_managed_run(
                config,
                run_id=str(args.run),
                actor=actor,
                lock_acquired=True,
            )
        except ValueError as exc:
            if str(exc) != "DISPATCH_CANCEL_RUN_MISSING":
                raise
            print(
                f"agent-q dispatches cancel: run not found: {args.run}",
                file=sys.stderr,
            )
            return 1
        rebuild_all(config)
    finally:
        lock_handle.release()
    print(f"dispatch.v1 attempt={args.run}\tstatus={'cancelled' if cancelled else 'terminal'}")
    return 0 if cancelled else 1


def _record_retry_exhaustion_if_needed(
    config,
    *,
    policy_id: str,
    request_id: str,
    run_id: str,
    attempt_number: int,
    max_attempts: int,
    actor: str,
) -> bool:
    if attempt_number != max_attempts:
        return False
    if any(
        record.get("kind") == "dispatch_retry_exhausted" and record.get("entity_id") == policy_id
        for record in read_event_records(config.events_path)
    ):
        return False
    stamp = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    emit_retry_exhausted(
        events_path=config.events_path,
        policy_id=policy_id,
        run_id=run_id,
        thread_id=request_id,
        attempt_number=attempt_number,
        max_attempts=max_attempts,
        exhausted_utc=stamp,
        actor=actor,
        lock_acquired=True,
    )
    return True


def cmd_dispatches_run(args: argparse.Namespace) -> int:
    """Execute the high-level dispatch.v1 managed-launch flow."""

    if not args.live:
        print("agent-q dispatches run: --live is required", file=sys.stderr)
        return 2
    if not 1 <= args.max_attempts <= 8:
        print(
            "agent-q dispatches run: --max-attempts must be between 1 and 8",
            file=sys.stderr,
        )
        return 2
    if not 1 <= args.timeout_seconds <= 21_600:
        print(
            "agent-q dispatches run: --timeout-seconds must be between 1 and 21600",
            file=sys.stderr,
        )
        return 2
    if args.title and args.body is None:
        print(
            "agent-q dispatches run: --body is required when creating a REQ",
            file=sys.stderr,
        )
        return 2
    workstream = str(args.workstream or "").strip()
    if workstream and not args.title:
        print(
            "agent-q dispatches run: --workstream is supported only when creating a new REQ",
            file=sys.stderr,
        )
        return 2
    continue_response = str(args.continue_response or "").strip()
    if continue_response and not args.title:
        print(
            "agent-q dispatches run: --continue-response requires a new REQ title",
            file=sys.stderr,
        )
        return 2
    if continue_response and workstream:
        print(
            "agent-q dispatches run: a follow-up inherits its prior managed context; "
            "do not also pass --workstream",
            file=sys.stderr,
        )
        return 2
    if len(workstream) > 128 or any(ord(character) < 32 for character in workstream):
        print(
            "agent-q dispatches run: --workstream must be one line with at most 128 characters",
            file=sys.stderr,
        )
        return 2
    if args.subject_change_base and not args.subject_change_mode:
        print(
            "agent-q dispatches run: --subject-change-base requires --subject-change-mode",
            file=sys.stderr,
        )
        return 2
    if bool(args.coverage_kind) != bool(args.coverage_key):
        print(
            "agent-q dispatches run: --coverage-kind and --coverage-key are required together",
            file=sys.stderr,
        )
        return 2
    if args.policy and any(
        (
            args.subject_decision,
            args.subject_artifact,
            args.subject_change_mode,
            args.coverage_kind,
            args.coverage_key,
        )
    ):
        print(
            "agent-q dispatches run: a retry reuses its frozen subject; do not pass subject options",
            file=sys.stderr,
        )
        return 2
    if (
        not args.policy
        and args.purpose == "review"
        and not any((args.subject_decision, args.subject_artifact, args.subject_change_mode))
    ):
        print(
            "agent-q dispatches run: review requires an exact --subject-decision, "
            "--subject-artifact, or --subject-change-mode",
            file=sys.stderr,
        )
        return 2

    config = load_config()
    if not args.policy and args.purpose == "review":
        if not config.review_assurance.artifact_roots:
            print(
                "agent-q dispatches run: review requires at least one configured artifact root",
                file=sys.stderr,
            )
            return 2
        if (
            config.review_assurance.enforcement == "blocking"
            and args.role not in config.review_assurance.reviewer_roles
        ):
            print(
                "agent-q dispatches run: selected review role is not in the blocking "
                "reviewer role allowlist",
                file=sys.stderr,
            )
            return 2
        if args.coverage_kind == "path":
            from agent_mesh.core.decision_glob import DecisionGlobError, compile_meshglob

            try:
                compile_meshglob(str(args.coverage_key))
            except DecisionGlobError as exc:
                print(
                    f"agent-q dispatches run: invalid path coverage meshglob: {exc}",
                    file=sys.stderr,
                )
                return 2
    actor = _authoring_actor(config, explicit_actor=args.actor)
    retry_policy_payload = None
    profile_name = args.profile
    if args.policy:
        retry_policy_payload = _dispatch_policy_event_payload(config.events_path, args.policy)
        if retry_policy_payload is None:
            print(
                f"agent-q dispatches run: frozen policy not found: {args.policy}",
                file=sys.stderr,
            )
            return 1
        target = retry_policy_payload.get("target")
        if not isinstance(target, dict) or str(target.get("participant") or "") != args.target:
            print(
                "agent-q dispatches run: retry target does not match the frozen policy",
                file=sys.stderr,
            )
            return 2
        profile_name = profile_name or str(target.get("runtime_profile") or "") or None
    profile = config.runtime_profile_for_target(
        args.target,
        profile_name=profile_name,
    )
    preflight = preflight_runtime_profile(profile, project_root=config.project_root)
    if not preflight.passed or preflight.binary_path is None:
        _print_runtime_preflight(preflight)
        print(
            f"agent-q dispatches run: capability preflight failed: {preflight.failure_summary()}",
            file=sys.stderr,
        )
        return 1
    receipt_expiry = _parse_iso_utc(preflight.valid_until_utc)
    if receipt_expiry is None or receipt_expiry <= datetime.now(timezone.utc):
        print(
            "agent-q dispatches run: capability receipt is missing or expired",
            file=sys.stderr,
        )
        return 1
    frozen_assurance = (
        retry_policy_payload.get("assurance_policy")
        if isinstance(retry_policy_payload, dict)
        else None
    )
    blocking_review = (
        isinstance(frozen_assurance, dict) and frozen_assurance.get("enforcement") == "blocking"
    ) or (
        retry_policy_payload is None
        and args.purpose == "review"
        and config.review_assurance.enforcement == "blocking"
    )
    if blocking_review:
        if not preflight.blocking_eligible:
            _print_runtime_preflight(preflight)
            print(
                "agent-q dispatches run: blocking review requires host-verified "
                "effective-capability evidence",
                file=sys.stderr,
            )
            return 1
    runtime = _runtime_adapter_from_preflight(config, profile, preflight)
    receipt = preflight.capability_receipt(profile.required_capabilities)

    lock_slot: list[LockHandle | None] = [acquire(config.agent_dir / ".mail-lock")]
    try:
        rebuild_all(config)
        continuation: dict[str, str] | None = None
        continuation_packet: dict[str, Any] | None = None
        if continue_response:
            continuation_conn = connect(config.db_path)
            try:
                initialize_schema(continuation_conn)
                response_row = resolve_message(continuation_conn, continue_response)
                if response_row is None or str(response_row["kind"]) != "response":
                    raise ConfigError(f"follow-up response not found: {continue_response}")
                prior_runs = continuation_conn.execute(
                    "SELECT policy_id, input_message_id, target_agent, runtime_profile, "
                    "session_key, session_key_source, session_uuid, wave "
                    "FROM dispatch_runs WHERE output_message_id=? AND status='completed' "
                    "ORDER BY event_seq DESC LIMIT 2",
                    (continue_response,),
                ).fetchall()
                if len(prior_runs) != 1:
                    raise ConfigError(
                        "--continue-response must identify the unique result of a completed "
                        "managed dispatch"
                    )
                prior_run = prior_runs[0]
                if str(prior_run["target_agent"] or "") != args.target:
                    raise ConfigError(
                        "follow-up target must match the agent that produced the prior response"
                    )
                if str(prior_run["runtime_profile"] or "") != profile.name:
                    raise ConfigError(
                        "follow-up runtime profile must match the prior managed dispatch"
                    )
                prior_request = resolve_message(
                    continuation_conn,
                    str(prior_run["input_message_id"] or ""),
                )
                if prior_request is None or str(prior_request["kind"]) != "request":
                    raise ConfigError("prior managed request is unavailable for follow-up")
                continuation = {
                    "session_key": str(prior_run["session_key"] or ""),
                    "session_key_source": str(prior_run["session_key_source"] or ""),
                    "session_uuid": str(prior_run["session_uuid"] or ""),
                    "wave": str(prior_run["wave"] or ""),
                    "feature": str(prior_request["feature_id"] or ""),
                }
                if not all(
                    continuation[field]
                    for field in (
                        "session_key",
                        "session_key_source",
                        "session_uuid",
                        "wave",
                    )
                ):
                    raise ConfigError("prior managed context is incomplete and cannot be resumed")
                continuation_packet = build_message_packet(
                    continuation_conn,
                    response_row,
                    max_body_chars=8_000,
                    max_thread_body_chars=4_000,
                    max_thread_messages=6,
                )
            finally:
                continuation_conn.close()
        if retry_policy_payload is not None:
            if reconcile_expired_policy_attempt(
                config,
                policy_id=str(args.policy),
                actor=actor,
                lock_acquired=True,
            ):
                rebuild_all(config)
        prepared_subject = None
        if retry_policy_payload is None and any(
            (args.subject_decision, args.subject_artifact, args.subject_change_mode)
        ):
            subject_conn = connect(config.db_path)
            try:
                initialize_schema(subject_conn)
                prepared_subject = _dispatch_subject_from_args(config, subject_conn, args)
            finally:
                subject_conn.close()
        request_id = str(
            args.message
            or (
                retry_policy_payload.get("request_id")
                if isinstance(retry_policy_payload, dict)
                else ""
            )
            or ""
        )
        if not request_id:
            request_id = new_public_message_id("REQ", actor)
            append_event(
                config.events_path,
                Event(
                    event_id=generate_event_id(),
                    actor=actor,
                    kind="req_created",
                    entity_id=request_id,
                    thread_id=request_id,
                    payload={
                        "from": actor,
                        "to": [args.target],
                        "title": args.title,
                        "body": args.body,
                        "feature": continuation["feature"] if continuation else workstream,
                        "refs": [continue_response] if continue_response else [],
                        "response_mode": "single",
                    },
                ),
                lock_acquired=True,
            )

        request_event = _request_event_for_message(config.events_path, request_id)
        if request_event is None:
            print(
                f"agent-q dispatches run: request not found: {request_id}",
                file=sys.stderr,
            )
            return 1
        message = to_message(request_event, aliases=config.routing.aliases)
        if message.recipient_instances:
            print(
                "agent-q dispatches run: addressed running instances require the "
                "addressed_running_instance management level",
                file=sys.stderr,
            )
            return 1
        if args.target not in message.recipients:
            print(
                f"agent-q dispatches run: target {args.target} is not a recipient of {request_id}",
                file=sys.stderr,
            )
            return 1

        conn = connect(config.db_path)
        try:
            initialize_schema(conn)
            row = resolve_message(conn, request_id)
            if row is None or row["kind"] != "request":
                raise ConfigError(f"request not found after append: {request_id}")
            request_body = body_from_message_row(row)
            if retry_policy_payload is not None:
                projected = conn.execute(
                    "SELECT policy_digest FROM dispatch_policies WHERE policy_id=?",
                    (args.policy,),
                ).fetchone()
                if projected is None or str(projected["policy_digest"]) != str(
                    retry_policy_payload.get("policy_digest") or ""
                ):
                    raise ConfigError("frozen dispatch policy projection is unavailable")
                latest = conn.execute(
                    "SELECT run_id, attempt_number, status FROM dispatch_runs "
                    "WHERE policy_id=? ORDER BY attempt_number DESC, event_seq DESC LIMIT 1",
                    (args.policy,),
                ).fetchone()
                if latest is None:
                    raise ConfigError("retry requires a prior attempt")
                if str(latest["status"]) in {"planned", "started"}:
                    raise ConfigError("retry is blocked while the prior attempt is active")
                attempt_number = int(latest["attempt_number"] or 0) + 1
                maximum = int((retry_policy_payload.get("retry") or {}).get("max_attempts") or 0)
                if attempt_number > maximum:
                    _record_retry_exhaustion_if_needed(
                        config,
                        policy_id=str(args.policy),
                        request_id=request_id,
                        run_id=str(latest["run_id"]),
                        attempt_number=int(latest["attempt_number"] or 0),
                        max_attempts=maximum,
                        actor=actor,
                    )
                    raise ConfigError("frozen dispatch policy retry limit is exhausted")
                slot = retry_policy_payload.get("response_slot") or {}
                policy = DispatchPolicySnapshot(
                    policy_id=str(args.policy),
                    request_id=request_id,
                    response_slot_kind=str(slot.get("kind") or ""),
                    response_slot_key=str(slot.get("key") or ""),
                    payload=retry_policy_payload,
                )
                if retry_policy_payload.get("runtime_profile_revision") != runtime_profile_revision(
                    profile
                ):
                    raise ConfigError(
                        "current runtime profile does not match the frozen dispatch policy"
                    )
                previous_attempt_id = str(latest["run_id"])
            else:
                subject = prepared_subject
                assurance_policy = None
                if args.purpose == "review":
                    coverage = {"kind": "none", "key": ""}
                    if args.coverage_kind and args.coverage_key:
                        coverage = {
                            "kind": str(args.coverage_kind),
                            "key": str(args.coverage_key),
                        }
                    elif (
                        isinstance(subject, dict)
                        and subject.get("type") == "decision_revision"
                        and subject.get("stable_id") in config.review_assurance.covered_decisions
                    ):
                        coverage = {
                            "kind": "decision",
                            "key": str(subject["stable_id"]),
                        }
                    assurance_policy = {
                        "revision": "0" * 64,
                        "reviewer_roles": list(
                            config.review_assurance.reviewer_roles or (args.role,)
                        ),
                        "independence": config.review_assurance.independence,
                        "quorum": config.review_assurance.quorum,
                        "validity_seconds": config.review_assurance.validity_seconds,
                        "enforcement": config.review_assurance.enforcement,
                        "coverage": coverage,
                    }
                    assurance_policy["revision"] = assurance_policy_revision(assurance_policy)
                policy = build_dispatch_policy(
                    conn,
                    request_id=request_id,
                    profile=profile,
                    purpose=args.purpose,
                    role=args.role,
                    management_level="managed_launch",
                    artifact_contract=(
                        {
                            "root": config.review_assurance.artifact_roots[0],
                            "media_types": ["text/markdown"],
                            "max_bytes": 16 * 1024 * 1024,
                            "visibility": "project_private",
                            "creation_allowed": True,
                        }
                        if args.purpose == "review"
                        else None
                    ),
                    provenance_requirements=(
                        "runtime_profile",
                        "instance",
                        "context",
                        "subject_binding",
                    ),
                    max_attempts=args.max_attempts,
                    subject=subject,
                    assurance_policy=assurance_policy,
                )
                attempt_number = 1
                previous_attempt_id = ""
        finally:
            conn.close()

        if retry_policy_payload is None:
            append_dispatch_policy(
                config.events_path,
                policy,
                actor=actor,
                lock_acquired=True,
            )
        host = _CliDispatchHost(config, args.target, profile_name=profile.name)
        base_plan = plan_for(message, args.target, host, grounding={"complete": True})
        if continuation is not None:
            base_plan = replace(
                base_plan,
                session_key=continuation["session_key"],
                session_key_source=continuation["session_key_source"],
                session_uuid=continuation["session_uuid"],
                wave=continuation["wave"],
            )
        plan = replace(
            base_plan,
            run_mode="live",
            status="planned",
            policy_id=policy.policy_id,
            policy_digest=policy.digest,
            attempt_number=attempt_number,
            previous_attempt_id=previous_attempt_id,
            management_level="managed_launch",
            runtime_profile=profile.name,
            effective_capability_evidence_digest=str(receipt["receipt_digest"]),
            capability_receipt=receipt,
        )
        prompt = _launch_prompt(
            message,
            request_body,
            continuation_packet=continuation_packet,
        )
        if policy.payload.get("purpose") == "review":
            policy_subject = policy.payload.get("subject")
            if not isinstance(policy_subject, dict):
                raise ConfigError("review dispatch policy has no exact subject")
            replacement_response_id = ""
            if retry_policy_payload is not None:
                replacement_conn = connect(config.db_path)
                try:
                    flagged = replacement_conn.execute(
                        "SELECT a.originating_response_id FROM review_assurances a "
                        "JOIN review_assurance_lifecycle l ON l.assurance_id=a.assurance_id "
                        "WHERE a.policy_id=? AND l.lifecycle_version=("
                        "SELECT MAX(l2.lifecycle_version) FROM review_assurance_lifecycle l2 "
                        "WHERE l2.assurance_id=a.assurance_id) AND l.state='flagged' "
                        "ORDER BY a.event_seq",
                        (policy.policy_id,),
                    ).fetchall()
                finally:
                    replacement_conn.close()
                if len(flagged) > 1:
                    raise ConfigError(
                        "multiple flagged assurances are bound to this policy; "
                        "select a policy with one exact replacement lineage"
                    )
                if flagged:
                    replacement_response_id = str(flagged[0]["originating_response_id"] or "")
            prompt += "\n\n" + review_response_instructions(
                policy_id=policy.policy_id,
                policy_digest=policy.digest,
                subject_digest=str(policy_subject.get("digest") or ""),
                replaces_response_id=replacement_response_id,
            )

        def revalidate() -> tuple[bool, str]:
            if receipt_expiry is None or receipt_expiry <= datetime.now(timezone.utc):
                return False, "RuntimeConfiguration"
            current = preflight_runtime_profile(profile, project_root=config.project_root)
            if not current.passed or current.binary_path is None:
                return False, "ParentDenial"
            if (
                current.profile_digest != preflight.profile_digest
                or current.drift_inputs_digest != preflight.drift_inputs_digest
            ):
                return False, "RuntimeConfiguration"
            if blocking_review and not current.blocking_eligible:
                return False, "ParentDenial"
            return True, ""

        def release_dispatch_lock_for_launch() -> None:
            handle = lock_slot[0]
            if handle is None:
                raise RuntimeError("dispatch canonical lock is not held")
            handle.release()
            lock_slot[0] = None

        def reacquire_dispatch_lock_after_launch() -> LockHandle:
            if lock_slot[0] is not None:
                raise RuntimeError("dispatch canonical lock is already held")
            handle = acquire(config.agent_dir / ".mail-lock")
            lock_slot[0] = handle
            return handle

        authoritative_receipt, capability_append_receipt = (
            preflight.mint_capability_append_authority(
                events_path=config.events_path,
                required_capabilities=profile.required_capabilities,
            )
        )
        if authoritative_receipt != receipt:
            raise ConfigError("runtime capability receipt changed before append authority mint")
        try:
            result = execute_launch_plan(
                plan,
                events_path=config.events_path,
                runtime_adapter=runtime,
                launcher=AgentProcessLauncher(),
                project_root=config.project_root,
                prompt=prompt,
                timeout_seconds=args.timeout_seconds,
                lock_acquired=True,
                post_response=True,
                actor=actor,
                prelaunch_revalidate=revalidate,
                release_lock_for_launch=release_dispatch_lock_for_launch,
                reacquire_lock_after_launch=reacquire_dispatch_lock_after_launch,
                capability_append_receipt=capability_append_receipt,
                lock_handle=lock_slot[0],
            )
        except DispatchLockReacquireError:
            print(
                "agent-q dispatches run: canonical lock reacquire failed; "
                "the active attempt was left for bounded reconciliation",
                file=sys.stderr,
            )
            return 1
        except CodexAppServerError as exc:
            rebuild_all(config)
            print(
                "agent-q dispatches run: provider preparation failed: "
                f"{exc}; policy={policy.policy_id}; repair the runtime, then retry "
                "this frozen policy",
                file=sys.stderr,
            )
            return 1
        finally:
            preflight.discard_capability_append_authority(capability_append_receipt)
        rebuild_all(config)
        if result.status == "completed" and policy.payload.get("purpose") == "review":
            _derive_review_assurance(
                config,
                policy_id=policy.policy_id,
                actor=actor,
                lock_acquired=True,
            )
            rebuild_all(config)
        if result.status != "observation_pending":
            maximum = int((policy.payload.get("retry") or {}).get("max_attempts") or 0)
            if _record_retry_exhaustion_if_needed(
                config,
                policy_id=policy.policy_id,
                request_id=request_id,
                run_id=plan.run_id,
                attempt_number=attempt_number,
                max_attempts=maximum,
                actor=actor,
            ):
                rebuild_all(config)
    finally:
        if lock_slot[0] is not None:
            lock_slot[0].release()

    terminal, candidate_status, candidate_reason = _dispatch_terminal(result, post_response=True)
    print(
        f"dispatch.v1 policy={policy.policy_id}\tattempt={plan.run_id}\t"
        f"request={request_id}\tstatus={terminal}\t"
        f"response_candidate={candidate_status}:{candidate_reason}"
    )
    return 0 if terminal == "completed" else 1


def _dispatch_subject_from_args(config, conn, args):
    if args.subject_decision:
        return resolve_decision_subject(conn, args.subject_decision)
    if args.subject_artifact:
        return resolve_artifact_subject(config, args.subject_artifact)
    if args.subject_change_mode:
        return resolve_current_change_set_subject(
            config.project_root,
            mode=args.subject_change_mode,
            base=args.subject_change_base,
        )
    return None


def _dispatch_policy_event_payload(events_path: Path, policy_id: str) -> dict | None:
    for record in read_event_records(events_path):
        if record.get("kind") == "dispatch_policy_frozen" and record.get("entity_id") == policy_id:
            payload = record.get("payload")
            return dict(payload) if isinstance(payload, dict) else None
    return None


def cmd_dispatches_assure(args: argparse.Namespace) -> int:
    config = load_config()
    actor = _authoring_actor(config, explicit_actor=args.actor)
    lock_handle = acquire(config.agent_dir / ".mail-lock")
    try:
        rebuild_all(config)
        payload = _derive_review_assurance(
            config,
            policy_id=args.policy,
            actor=actor,
            lock_acquired=True,
        )
        rebuild_all(config)
    finally:
        lock_handle.release()
    print(
        f"review.v1 assurance={payload['assurance_id']}\tpolicy={args.policy}\t"
        f"attempt={payload['attempt_id']}\tdisposition={payload['disposition']}"
    )
    return 0


def cmd_dispatches_assurance(args: argparse.Namespace) -> int:
    config = load_config()
    with open_read_model(config) as snapshot:
        result = assurance_lifecycle_for_response(snapshot.conn, args.response)
    if result is None:
        print(
            f"agent-q dispatches assurance: response not found: {args.response}",
            file=sys.stderr,
        )
        return 1
    public = {
        "schema": "agent-mesh.review-assurance-state.v1",
        "response_id": result["response_id"],
        "state": result["state"],
        "version": result["version"],
        "disposition": result["disposition"],
        "reason_code": result["reason_code"],
        "note": result["note"],
        "actor": result["actor"],
        "occurred_utc": result["occurred_utc"],
        "replacement_response_id": result["replacement_response_id"],
    }
    if args.diagnostics:
        public["audit"] = {
            "assurance_id": result["assurance_id"],
            "policy_id": result["policy_id"],
            "operation_key": result["operation_key"],
        }
    if args.json:
        print(json.dumps(public, sort_keys=True))
    else:
        replacement = (
            f"\treplacement={public['replacement_response_id']}"
            if public["replacement_response_id"]
            else ""
        )
        print(
            f"response={public['response_id']}\tstate={public['state']}\t"
            f"version={public['version']}\tdisposition={public['disposition']}"
            f"{replacement}"
        )
    return 0


def _cmd_dispatches_assurance_transition(args: argparse.Namespace, *, action: str) -> int:
    config = load_config()
    actor = _authoring_actor(config, explicit_actor=args.actor)
    lock_handle = acquire(config.agent_dir / ".mail-lock")
    try:
        recover(config.events_path, config.agent_dir)
        rebuild_all(config)
        result = append_assurance_lifecycle_transition(
            config,
            response_id=args.response,
            actor=actor,
            action=action,
            expected_version=args.expected_version,
            reason_code=args.reason_code,
            note=args.note,
            lock_acquired=True,
        )
        rebuild_all(config)
    finally:
        lock_handle.release()
    reuse = "\treused=true" if result["reused"] else ""
    print(
        f"response={result['response_id']}\tstate={result['state']}\t"
        f"version={result['version']}{reuse}"
    )
    return 0


def cmd_dispatches_flag_assurance(args: argparse.Namespace) -> int:
    return _cmd_dispatches_assurance_transition(args, action="flag")


def cmd_dispatches_retire_assurance(args: argparse.Namespace) -> int:
    return _cmd_dispatches_assurance_transition(args, action="retire")


def _canonical_response_body(events_path: Path, response_id: str) -> str:
    for record in reversed(read_event_records(events_path)):
        if record.get("kind") != "res_posted" or record.get("entity_id") != response_id:
            continue
        payload = record.get("payload")
        if isinstance(payload, dict) and isinstance(payload.get("body"), str):
            return str(payload["body"])
        break
    raise ConfigError("dispatcher-bound RES body is unavailable")


def _derive_review_assurance(
    config: AgentMeshConfig,
    *,
    policy_id: str,
    actor: str,
    lock_acquired: bool,
) -> dict[str, Any]:
    """Derive authoritative review facts only from the exact accepted RES envelope."""

    conn = connect(config.db_path)
    try:
        policy = conn.execute(
            "SELECT * FROM dispatch_policies WHERE policy_id=?", (policy_id,)
        ).fetchone()
        if policy is None:
            raise ConfigError(f"frozen dispatch policy not found: {policy_id}")
        assurance_policy = json_loads(policy["assurance_policy_json"], None)
        subject = json_loads(policy["subject_json"], None)
        artifact_contract = json_loads(policy["artifact_contract_json"], None)
        if (
            not isinstance(assurance_policy, dict)
            or not isinstance(subject, dict)
            or not isinstance(artifact_contract, dict)
        ):
            raise ConfigError("policy does not carry a bound review.v1 assurance contract")
        outcome = current_dispatch_outcome(conn, policy_id)
        if not outcome.gate_satisfying or not outcome.attempt_id or not outcome.output_message_id:
            raise ConfigError("current response slot has no completed dispatcher-bound RES")
        existing = conn.execute(
            "SELECT assurance_id, disposition FROM review_assurances "
            "WHERE policy_id=? AND attempt_id=? AND authoritative=1",
            (policy_id, outcome.attempt_id),
        ).fetchone()
        if existing is not None:
            return {
                "assurance_id": str(existing["assurance_id"]),
                "attempt_id": outcome.attempt_id,
                "disposition": str(existing["disposition"]),
            }
        run = conn.execute(
            "SELECT management_level FROM dispatch_runs WHERE run_id=?",
            (outcome.attempt_id,),
        ).fetchone()
        response = conn.execute(
            "SELECT sender, sender_instance_id FROM messages WHERE id=? AND kind='response'",
            (outcome.output_message_id,),
        ).fetchone()
        if run is None or response is None:
            raise ConfigError("current dispatch outcome projection is incomplete")
        body = _canonical_response_body(config.events_path, outcome.output_message_id)
        try:
            envelope = parse_review_response(body)
        except ReviewResponseError as exc:
            raise ConfigError(str(exc)) from exc
        if (
            envelope.policy_id != policy_id
            or envelope.policy_digest != str(policy["policy_digest"])
            or envelope.subject_digest != str(subject.get("digest") or "")
        ):
            raise ConfigError("REVIEW_RESPONSE_BINDING_MISMATCH")
        supersession: dict[str, Any] = {}
        if envelope.replaces_response_id:
            replaced = conn.execute(
                "SELECT a.assurance_id, a.subject_json, l.lifecycle_version, l.state "
                "FROM review_assurances a JOIN review_assurance_lifecycle l "
                "ON l.assurance_id=a.assurance_id "
                "WHERE a.originating_response_id=? AND a.authoritative=1 "
                "AND l.lifecycle_version=(SELECT MAX(l2.lifecycle_version) "
                "FROM review_assurance_lifecycle l2 WHERE l2.assurance_id=a.assurance_id) "
                "ORDER BY a.event_seq DESC LIMIT 1",
                (envelope.replaces_response_id,),
            ).fetchone()
            if replaced is None:
                raise ConfigError("REVIEW_ASSURANCE_SUPERSESSION_TARGET_MISSING")
            if str(replaced["state"]) != "flagged":
                raise ConfigError("REVIEW_ASSURANCE_SUPERSESSION_TARGET_NOT_FLAGGED")
            if str(replaced["subject_json"]) != json.dumps(
                subject, sort_keys=True, separators=(",", ":")
            ):
                raise ConfigError("REVIEW_ASSURANCE_SUPERSESSION_SUBJECT_MISMATCH")
            supersession = {
                "supersedes_assurance_id": str(replaced["assurance_id"]),
                "supersedes_expected_version": int(replaced["lifecycle_version"]),
            }
        reviewer_instance = str(response["sender_instance_id"] or "")
        instance = conn.execute(
            "SELECT external_session_ref_digest, launch_attempt_digest "
            "FROM agent_instances WHERE id=?",
            (reviewer_instance,),
        ).fetchone()
        reviewer_context = (
            str(instance["external_session_ref_digest"] or "")
            or str(instance["launch_attempt_digest"] or "")
            if instance is not None
            else ""
        )
        if not reviewer_instance or not reviewer_context:
            raise ConfigError("REVIEW_ASSURANCE_CONTEXT_UNPROVEN")
        author_provenance = subject.get("author_provenance")
        if not isinstance(author_provenance, dict):
            author_provenance = {}
        author_instance = str(author_provenance.get("instance_id") or "")
        author_context = str(author_provenance.get("context_digest") or "")
        required_independence = str(assurance_policy.get("independence") or "")
        missing_instance_authority = (
            required_independence
            in {
                "distinct_instance",
                "distinct_instance_and_context",
            }
            and not author_instance
        )
        missing_context_authority = (
            required_independence
            in {
                "distinct_context",
                "distinct_instance_and_context",
            }
            and not author_context
        )
        if missing_instance_authority or missing_context_authority:
            if str(assurance_policy.get("enforcement") or "advisory") == "blocking":
                if missing_instance_authority:
                    raise ConfigError("REVIEW_ASSURANCE_AUTHOR_INSTANCE_UNPROVEN")
                raise ConfigError("REVIEW_ASSURANCE_AUTHOR_CONTEXT_UNPROVEN")
            independence_class = "external_unverified"
        else:
            distinct_instance = reviewer_instance != author_instance
            distinct_context = reviewer_context != author_context
            independence_class = (
                "distinct_instance_and_context"
                if distinct_instance and distinct_context
                else "distinct_instance"
                if distinct_instance
                else "distinct_context"
                if distinct_context
                else "same_context"
            )
        media_types = artifact_contract.get("media_types")
        media_type = (
            str(media_types[0])
            if isinstance(media_types, list) and media_types
            else "text/markdown"
        )
        management_level = str(run["management_level"] or policy["management_level"])
        deadline = time.monotonic() + 5.0
        artifact_refs = [
            resolve_typed_artifact_ref(
                config,
                location_type="repository_path",
                location=location,
                media_type=media_type,
                revision="",
                visibility=str(artifact_contract.get("visibility") or "project_private"),
                provenance=management_level,
                subject_digest=str(subject["digest"]),
                deadline_monotonic=deadline,
            )
            for location in envelope.artifact_paths
        ]
        validate_typed_artifact_refs(
            config,
            artifact_refs,
            artifact_contract=artifact_contract,
            expected_subject_digest=str(subject["digest"]),
            expected_provenance=management_level,
            deadline_monotonic=deadline,
        )
        recorded = datetime.now(timezone.utc).replace(microsecond=0)
        recorded_utc = recorded.isoformat().replace("+00:00", "Z")
        payload = {
            "contract_version": "review.v1",
            "assurance_id": new_ulid("assurance"),
            "request_id": str(policy["request_id"]),
            "attempt_id": outcome.attempt_id,
            "bound": True,
            "subject": subject,
            "reviewer": {
                "participant": str(response["sender"]),
                "instance_id": reviewer_instance,
                "context_digest": reviewer_context,
                "role": str(policy["role"]),
                "independence_class": independence_class,
            },
            "policy_revision": str(assurance_policy["revision"]),
            "disposition": envelope.disposition,
            "finding_counts": envelope.finding_counts,
            "artifact_refs": artifact_refs,
            "management_level": management_level,
            "recorded_utc": recorded_utc,
            "valid_until_utc": (
                recorded + timedelta(seconds=int(assurance_policy["validity_seconds"]))
            )
            .isoformat()
            .replace("+00:00", "Z"),
            "authoritative": True,
            "policy_id": policy_id,
            "policy_digest": str(policy["policy_digest"]),
            "originating_response_id": outcome.output_message_id,
            **supersession,
        }
    finally:
        conn.close()
    append_event(
        config.events_path,
        Event(
            event_id=generate_event_id(),
            occurred_utc=recorded_utc,
            actor=actor,
            kind="review_assurance_recorded",
            entity_id=str(payload["assurance_id"]),
            thread_id=str(payload["request_id"]),
            payload=payload,
        ),
        lock_acquired=lock_acquired,
    )
    return payload


def cmd_dispatches_once(args: argparse.Namespace) -> int:
    if not args.live:
        print("agent-q dispatches once: --live is required", file=sys.stderr)
        return 2
    config = load_config()
    actor = _authoring_actor(config)
    runtime = _runtime_adapter_for(config, args.target, profile_name=args.profile)
    lock_handle = acquire(config.agent_dir / ".mail-lock")
    try:
        rebuild_all(config)
        request_event = _request_event_for_message(config.events_path, args.message)
        if request_event is None:
            print(f"agent-q dispatches once: request not found: {args.message}", file=sys.stderr)
            return 1
        message = to_message(request_event, aliases=config.routing.aliases)
        if message.recipient_instances:
            print(
                "agent-q dispatches once: instance-addressed requests must be retrieved by "
                "the named running instance; generic participant dispatch is disabled",
                file=sys.stderr,
            )
            return 1
        if args.target not in message.recipients:
            print(
                f"agent-q dispatches once: target {args.target} is not a recipient of {args.message}",
                file=sys.stderr,
            )
            return 1
        conn = connect(config.db_path)
        try:
            initialize_schema(conn)
            row = resolve_message(conn, message.entity_id)
            if row is None or row["kind"] != "request":
                print(
                    f"agent-q dispatches once: request not found: {args.message}", file=sys.stderr
                )
                return 1
            if _open_dispatch_lease_exists(conn, message.entity_id, args.target):
                print(
                    f"agent-q dispatches once: open lease already exists for {message.entity_id}/{args.target}",
                    file=sys.stderr,
                )
                return 1
            response_mode = str(request_event.get("payload", {}).get("response_mode") or "single")
            if response_mode not in {"single", "multi"}:
                print(
                    f"agent-q dispatches once: invalid response_mode for {message.entity_id}",
                    file=sys.stderr,
                )
                return 1
            if (
                args.post_response
                and response_mode == "single"
                and _direct_response_exists(conn, message.entity_id)
            ):
                print(
                    f"agent-q dispatches once: request {message.entity_id} already has response",
                    file=sys.stderr,
                )
                return 1
            prompt = _launch_prompt(message, body_from_message_row(row))
        finally:
            conn.close()

        host = _CliDispatchHost(config, args.target, profile_name=args.profile)
        plan = plan_for(message, args.target, host, grounding={"complete": True})
        result = execute_launch_plan(
            plan,
            events_path=config.events_path,
            runtime_adapter=runtime,
            launcher=AgentProcessLauncher(),
            project_root=config.project_root,
            prompt=prompt,
            timeout_seconds=args.timeout_seconds,
            lock_acquired=True,
            post_response=args.post_response,
            actor=actor,
        )
        rebuild_all(config)
    finally:
        lock_handle.release()
    terminal = result.status
    candidate_status = "none"
    candidate_reason = "not_successful"
    if result.status == "completed" and result.exit_code == 0:
        candidate = extract_response_candidate(result)
        candidate_status = "ready" if candidate.status == "accepted" else "rejected"
        candidate_reason = candidate.reason
        if args.post_response and candidate.status == "accepted":
            terminal = "completed"
        elif args.post_response:
            terminal = "OutputRejected"
        else:
            terminal = "OutputNotPosted"
    print(
        f"dispatch launch-only {plan.run_id}\t{result.status}\t{terminal}\t"
        f"response_candidate={candidate_status}:{candidate_reason}"
    )
    if args.post_response and terminal == "OutputRejected":
        return 1
    return 0


def _dispatch_message_locked(
    config,
    *,
    target: str,
    message_id: str,
    timeout_seconds: int,
    post_response: bool,
    runtime: AgentRuntimeAdapter,
    actor: str,
    profile_name: str | None = None,
):
    request_event = _request_event_for_message(config.events_path, message_id)
    if request_event is None:
        raise ValueError(f"request not found: {message_id}")
    message = to_message(request_event, aliases=config.routing.aliases)
    if message.recipient_instances:
        raise ValueError("instance-addressed requests cannot use generic participant dispatch")
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        row = resolve_message(conn, message.entity_id)
        if row is None or row["kind"] != "request":
            raise ValueError(f"request not found: {message_id}")
        prompt = _launch_prompt(message, body_from_message_row(row))
    finally:
        conn.close()
    host = _CliDispatchHost(config, target, profile_name=profile_name)
    plan = plan_for(message, target, host, grounding={"complete": True})
    result = execute_launch_plan(
        plan,
        events_path=config.events_path,
        runtime_adapter=runtime,
        launcher=AgentProcessLauncher(),
        project_root=config.project_root,
        prompt=prompt,
        timeout_seconds=timeout_seconds,
        lock_acquired=True,
        post_response=post_response,
        actor=actor,
    )
    rebuild_all(config)
    return plan, result


def _dispatch_terminal(result, *, post_response: bool) -> tuple[str, str, str]:
    terminal = result.status
    candidate_status = "none"
    candidate_reason = "not_successful"
    if result.status == "completed" and result.exit_code == 0:
        candidate = extract_response_candidate(result)
        candidate_status = "ready" if candidate.status == "accepted" else "rejected"
        candidate_reason = candidate.reason
        if post_response and candidate.status == "accepted":
            terminal = "completed"
        elif post_response:
            terminal = "OutputRejected"
        else:
            terminal = "OutputNotPosted"
    return terminal, candidate_status, candidate_reason


def _eligible_dispatch_request_ids(
    config,
    *,
    target: str,
    post_response: bool,
    message_ids: set[str] | None = None,
    after_event_seq: int | None = None,
    profile_name: str | None = None,
) -> list[str]:
    ids: list[str] = []
    seen: set[str] = set()
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        for record in _event_records(config.events_path):
            if record.get("kind") != "req_created":
                continue
            request_id = str(record.get("entity_id") or "")
            if after_event_seq is not None and int(record.get("event_seq") or 0) <= after_event_seq:
                continue
            if message_ids is not None and request_id not in message_ids:
                continue
            if not request_id or request_id in seen:
                continue
            seen.add(request_id)
            message = to_message(record, aliases=config.routing.aliases)
            if message.recipient_instances:
                continue
            if target not in message.recipients:
                continue
            row = resolve_message(conn, request_id)
            if row is None or row["kind"] != "request":
                continue
            payload = record.get("payload", {}) or {}
            if not _has_dispatchable_request_body(row, payload):
                continue
            if _open_dispatch_lease_exists(conn, request_id, target):
                continue
            if _terminal_dispatch_run_exists(conn, request_id, target):
                continue
            response_mode = str(payload.get("response_mode") or "single")
            if response_mode not in {"single", "multi"}:
                continue
            if response_mode == "single" and len(set(message.recipients)) > 1:
                continue
            if response_mode == "single" and _direct_response_exists(conn, request_id):
                continue
            if response_mode == "multi" and _direct_response_from_exists(conn, request_id, target):
                continue
            plan = plan_for(
                message,
                target,
                _CliDispatchHost(config, target, profile_name=profile_name),
                grounding={"complete": True},
            )
            if plan.gate != "auto-dispatch":
                continue
            ids.append(request_id)
    finally:
        conn.close()
    return ids


def cmd_dispatches_worker(args: argparse.Namespace) -> int:
    if not args.live:
        print("agent-q dispatches worker: --live is required", file=sys.stderr)
        return 2
    if args.max_runs < 1:
        print("agent-q dispatches worker: --max-runs must be >= 1", file=sys.stderr)
        return 2
    if args.after_event_seq is not None and args.after_event_seq < 0:
        print("agent-q dispatches worker: --after-event-seq must be >= 0", file=sys.stderr)
        return 2
    config = load_config()
    actor = _authoring_actor(config)
    message_ids = set(args.message) if args.message else None
    runs = 0
    stopped = "max-runs"
    exit_code = 0
    while runs < args.max_runs:
        # A bounded worker can span several long model runs. Re-run the no-prompt
        # preflight before each possible launch so executable, model, auth/billing,
        # capability, and credential-isolation drift fails before lifecycle writes.
        runtime = _runtime_adapter_for(config, args.target, profile_name=args.profile)
        lock_handle = acquire(config.agent_dir / ".mail-lock")
        try:
            rebuild_all(config)
            eligible = _eligible_dispatch_request_ids(
                config,
                target=args.target,
                post_response=args.post_response,
                message_ids=message_ids,
                after_event_seq=args.after_event_seq,
                profile_name=args.profile,
            )
            if not eligible:
                stopped = "empty"
                break
            message_id = eligible[0]
            plan, result = _dispatch_message_locked(
                config,
                target=args.target,
                message_id=message_id,
                timeout_seconds=args.timeout_seconds,
                post_response=args.post_response,
                runtime=runtime,
                actor=actor,
                profile_name=args.profile,
            )
        finally:
            lock_handle.release()
        runs += 1
        terminal, candidate_status, candidate_reason = _dispatch_terminal(
            result, post_response=args.post_response
        )
        print(
            f"dispatch worker run {plan.run_id}\t{result.status}\t{terminal}\t"
            f"message={message_id}\tresponse_candidate={candidate_status}:{candidate_reason}"
        )
        if args.post_response and terminal == "OutputRejected":
            stopped = "OutputRejected"
            exit_code = 1
            break
    print(f"dispatch worker completed\truns={runs}\tstopped={stopped}")
    return exit_code


class _CliDispatchHost(DispatchHost):
    def __init__(self, config, target: str, *, profile_name: str | None = None) -> None:
        super().__init__(
            AdapterSpec(
                name="cli-dispatch-host", domain="dispatch", privacy_class="project_private"
            )
        )
        self.config = config
        self.target = target
        self.profile_name = profile_name

    def routes(self) -> dict[str, dict[str, object]]:
        profile = self.config.runtime_profile_for_target(
            self.target, profile_name=self.profile_name
        )
        return {
            self.target: {
                "gen_ai_system": profile.provider,
                "model": profile.model,
            }
        }

    def classify(self, message: Message) -> str:
        return "routine"

    def wave_for_refs(self, refs: list[str]) -> str | None:
        return None

    def post_reply_template(self) -> str:
        return "agent-mesh reply placeholder for {agent} {req_id}"

    def build_command(self, agent: str, message: Message, session_uuid: str) -> str:
        profile = f" --profile {self.profile_name}" if self.profile_name else ""
        return (
            f"agent-q dispatches once --live --target {agent}{profile} "
            f"--message {message.entity_id}"
        )

    def session_namespace(self) -> uuid.UUID:
        return uuid.uuid5(uuid.NAMESPACE_URL, f"agent-mesh:{self.config.project_root}")


def _request_event_for_message(events_path: Path, message_id: str) -> dict | None:
    for record in _event_records(events_path):
        if record.get("kind") == "req_created" and record.get("entity_id") == message_id:
            return record
    return None


def _has_dispatchable_request_body(row, payload: dict) -> bool:
    fidelity = str(row["body_fidelity"] or payload.get("body_fidelity") or "").strip()
    if fidelity in {"metadata_only", "missing"}:
        return False
    try:
        body_bytes = int(row["body_bytes"])
    except (TypeError, ValueError):
        body_bytes = 0
    body = payload.get("body")
    if body_bytes <= 0 and (body is None or str(body).strip() == ""):
        return False
    return True


def _direct_response_exists(conn, request_id: str) -> bool:
    row = conn.execute(
        """
        SELECT 1 FROM messages
        WHERE kind='response' AND request_id=? AND parent_id=?
        LIMIT 1
        """,
        (request_id, request_id),
    ).fetchone()
    return row is not None


def _direct_response_from_exists(conn, request_id: str, sender: str) -> bool:
    row = conn.execute(
        """
        SELECT 1 FROM messages
        WHERE kind='response' AND request_id=? AND parent_id=? AND sender=?
        LIMIT 1
        """,
        (request_id, request_id, sender),
    ).fetchone()
    return row is not None


def _terminal_dispatch_run_exists(conn, input_message_id: str, target_agent: str) -> bool:
    row = conn.execute(
        """
        SELECT 1 FROM dispatch_runs
        WHERE input_message_id=? AND target_agent=?
          AND status IN ('completed', 'failed', 'cancelled', 'timed_out', 'parent_lost')
        LIMIT 1
        """,
        (input_message_id, target_agent),
    ).fetchone()
    return row is not None


def _open_dispatch_lease_exists(conn, input_message_id: str, target_agent: str) -> bool:
    row = conn.execute(
        """
        SELECT 1 FROM dispatch_leases
        WHERE input_message_id=? AND target_agent=? AND status='open'
        LIMIT 1
        """,
        (input_message_id, target_agent),
    ).fetchone()
    return row is not None


def _launch_prompt(
    message: Message,
    body: str,
    *,
    continuation_packet: dict[str, Any] | None = None,
) -> str:
    prompt = (
        "You are responding to an agent-mesh request in launch-only mode.\n"
        "Do not post a reply yourself; return your proposed response on stdout.\n"
        "If you have a response candidate, wrap exactly that response body between these marker lines:\n"
        "AGENT_MESH_RESPONSE_BEGIN\n"
        "<response body>\n"
        "AGENT_MESH_RESPONSE_END\n\n"
        f"Request ID: {message.entity_id}\n"
        f"Thread ID: {message.thread_id}\n"
        f"Title: {message.title}\n\n"
        "Request body:\n"
        f"{body}"
    )
    if continuation_packet is None:
        return prompt
    thread = continuation_packet.get("thread", {})
    thread_messages = thread.get("messages", []) if isinstance(thread, dict) else []
    context_lines = [
        "This is a follow-up to a prior managed result. Use the bounded durable thread below as "
        "context, while treating the new request body above as the current instruction."
    ]
    for item in thread_messages if isinstance(thread_messages, list) else []:
        if not isinstance(item, dict):
            continue
        label = str(item.get("title") or item.get("summary") or "").strip()
        header = " ".join(
            part
            for part in (
                str(item.get("kind") or "message"),
                str(item.get("id") or ""),
                f"from={item.get('sender')}" if item.get("sender") else "",
                f"title={label}" if label else "",
            )
            if part
        )
        context_lines.extend((f"-- {header} --", str(item.get("body") or "")))
    if isinstance(thread, dict) and thread.get("truncated"):
        context_lines.append("[Earlier messages were omitted by the bounded context limit.]")
    return prompt + "\n\nPrevious managed request and result:\n" + "\n".join(context_lines)


def cmd_dispatches_list(args: argparse.Namespace) -> int:
    config = load_config()
    with open_read_model(config) as snapshot:
        conn = snapshot.conn
        sql = "SELECT * FROM dispatch_runs WHERE 1=1"
        params: list[str] = []
        if args.status:
            sql += " AND status=?"
            params.append(args.status)
        if args.agent:
            sql += " AND target_agent=?"
            params.append(args.agent)
        sql += " ORDER BY run_id"
        rows = conn.execute(sql, params).fetchall()
        if args.json:
            print(json.dumps([{key: row[key] for key in row.keys()} for row in rows], indent=2))
        else:
            for row in rows:
                print(
                    f"{row['run_id']}\t{row['status']}\t{row['gate']}\t"
                    f"{row['target_agent']}\t{row['input_message_id']}"
                )
    return 0


def cmd_dispatches_show(args: argparse.Namespace) -> int:
    config = load_config()
    with open_read_model(config) as snapshot:
        conn = snapshot.conn
        run_id = resolve_dispatch_run(conn, args.identifier)
        if run_id is None:
            print(f"agent-q dispatches show: not found: {args.identifier}", file=sys.stderr)
            return 1
        row = conn.execute("SELECT * FROM dispatch_runs WHERE run_id=?", (run_id,)).fetchone()
        print(f"{row['run_id']}")
        print(f"status: {row['status']}")
        print(f"run_mode: {row['run_mode']}")
        print(f"gate: {row['gate'] or ''}")
        print(f"gate_reason_code: {row['gate_reason_code'] or ''}")
        print(f"target_agent: {row['target_agent']}")
        print(f"input_message_id: {row['input_message_id']}")
        print(f"session_key: {row['session_key'] or ''}")
        print(f"plan_artifact_hash: {row['plan_artifact_hash'] or ''}")
        if row["block_reason_codes_json"]:
            print(f"block_reason_codes: {row['block_reason_codes_json']}")
            print(f"missing_count: {row['missing_count']}")
        if row["output_message_id"]:
            print(f"output_message_id: {row['output_message_id']}")
        for lease in conn.execute(
            "SELECT lease_id, status, reason FROM dispatch_leases WHERE run_id=? ORDER BY lease_id",
            (run_id,),
        ):
            suffix = f" {lease['reason']}" if lease["reason"] else ""
            print(f"lease {lease['lease_id']} [{lease['status']}]{suffix}")
    return 0


def cmd_dispatches_log(args: argparse.Namespace) -> int:
    config = load_config()
    with open_read_model(config) as snapshot:
        conn = snapshot.conn
        run_id = resolve_dispatch_run(conn, args.identifier)
        if run_id is None:
            print(f"agent-q dispatches log: not found: {args.identifier}", file=sys.stderr)
            return 1
        for record in snapshot.records:
            payload = record.get("payload", {}) or {}
            if record.get("entity_id") == run_id or payload.get("run_id") == run_id:
                print(
                    f"{record['event_seq']}\t{record['kind']}\t"
                    f"{record['occurred_utc']}\t{record['event_id']}"
                )
    return 0


def cmd_dispatches_status(args: argparse.Namespace) -> int:
    config = load_config()
    with open_read_model(config) as snapshot:
        conn = snapshot.conn
        for row in conn.execute(
            "SELECT status, COUNT(*) AS n FROM dispatch_runs GROUP BY status ORDER BY status"
        ):
            print(f"runs {row['status']}\t{row['n']}")
        for row in conn.execute(
            "SELECT status, COUNT(*) AS n FROM dispatch_leases GROUP BY status ORDER BY status"
        ):
            print(f"leases {row['status']}\t{row['n']}")
    return 0


def cmd_dispatches_gate(args: argparse.Namespace) -> int:
    """Return success only when the selected policy currently allows its transition."""

    config = load_config()
    if bool(args.boundary_kind) != bool(args.boundary_key):
        print(
            "agent-q dispatches gate: --boundary-kind and --boundary-key are required together",
            file=sys.stderr,
        )
        return 2
    if not args.policy and not args.boundary_kind:
        print(
            "agent-q dispatches gate: provide --policy or an exact transition boundary",
            file=sys.stderr,
        )
        return 2
    with open_read_model(config) as snapshot:
        result = (
            evaluate_transition_assurance(
                config,
                snapshot.conn,
                boundary_kind=args.boundary_kind,
                boundary_key=args.boundary_key,
                policy_id=args.policy,
            )
            if args.boundary_kind
            else evaluate_review_assurance(config, snapshot.conn, args.policy)
        )
        qualifying_responses = (
            [
                str(row["originating_response_id"])
                for row in snapshot.conn.execute(
                    "SELECT originating_response_id FROM review_assurances "
                    f"WHERE assurance_id IN ({','.join('?' for _ in result.qualifying_assurance_ids)})",
                    result.qualifying_assurance_ids,
                )
                if row["originating_response_id"]
            ]
            if result.qualifying_assurance_ids
            else []
        )
    payload = {
        "schema": "agent-mesh.review-gate.v1",
        "policy_id": result.policy_id,
        "configured": result.configured,
        "enforcement": result.enforcement,
        "satisfied": result.satisfied,
        "allows_transition": result.allows_transition,
        "current_attempt_id": result.current_attempt_id,
        "qualifying_responses": qualifying_responses,
        "reason_codes": list(result.reason_codes),
    }
    if args.diagnostics:
        payload["audit"] = {
            "qualifying_assurance_ids": list(result.qualifying_assurance_ids),
        }
    if args.json:
        print(json.dumps(payload, sort_keys=True))
    else:
        reasons = ",".join(result.reason_codes) or "none"
        print(
            f"policy={result.policy_id}\tenforcement={result.enforcement}\t"
            f"satisfied={str(result.satisfied).lower()}\t"
            f"allows_transition={str(result.allows_transition).lower()}\t"
            f"reasons={reasons}"
        )
    return 0 if result.allows_transition else 1


def _parse_iso_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def cmd_dispatches_verify(args: argparse.Namespace) -> int:
    """Declarative, READ-ONLY integrity check over the dispatch projection (emits no events).

    Re-asserts the cross-table invariants the projection enforces and additionally reports
    expired-but-unreleased open leases (a clock-dependent condition replay cannot check).
    """
    config = load_config()
    issues: list[str] = []
    with open_read_model(config) as snapshot:
        conn = snapshot.conn
        for run in conn.execute(
            "SELECT run_id, input_message_id, output_message_id, status FROM dispatch_runs"
        ):
            req = conn.execute(
                "SELECT thread_id FROM messages WHERE id=? AND kind='request'",
                (run["input_message_id"],),
            ).fetchone()
            if req is None:
                issues.append(f"DISPATCH_RUN_UNKNOWN_MESSAGE: {run['run_id']}")
            if run["status"] == "completed":
                out = conn.execute(
                    "SELECT thread_id FROM messages WHERE id=? AND kind='response'",
                    (run["output_message_id"],),
                ).fetchone()
                req_thread = req["thread_id"] if req is not None else None
                if out is None:
                    issues.append(f"DISPATCH_OUTPUT_MESSAGE_INVALID: {run['run_id']}")
                elif not req_thread or str(out["thread_id"]) != str(req_thread):
                    # bind against the request's canonical thread, not the run's stored thread
                    issues.append(f"DISPATCH_OUTPUT_THREAD_MISMATCH: {run['run_id']}")
        for lease in conn.execute("SELECT lease_id, run_id FROM dispatch_leases"):
            if (
                conn.execute(
                    "SELECT 1 FROM dispatch_runs WHERE run_id=?", (lease["run_id"],)
                ).fetchone()
                is None
            ):
                issues.append(f"DISPATCH_LEASE_UNKNOWN_RUN: {lease['lease_id']}")
        # re-assert the uq_dispatch_leases_open invariant independently of the index
        for dup in conn.execute(
            "SELECT input_message_id, target_agent, COUNT(*) AS n FROM dispatch_leases "
            "WHERE status='open' GROUP BY input_message_id, target_agent HAVING n > 1"
        ):
            issues.append(
                f"DISPATCH_LEASE_DUPLICATE: {dup['input_message_id']}/{dup['target_agent']}"
            )
        now = datetime.now(timezone.utc)
        for lease in conn.execute(
            "SELECT lease_id, created_utc, ttl_seconds FROM dispatch_leases WHERE status='open'"
        ):
            created = _parse_iso_utc(lease["created_utc"])
            ttl = lease["ttl_seconds"]
            if created is None or ttl is None:
                # a malformed-but-schema-valid clock must surface, not silently pass as never-expired
                issues.append(f"DISPATCH_LEASE_UNCHECKABLE_CLOCK: {lease['lease_id']}")
            elif (now - created).total_seconds() > float(ttl):
                issues.append(f"DISPATCH_LEASE_EXPIRED_UNRELEASED: {lease['lease_id']}")
    for issue in issues:
        print(issue, file=sys.stderr)
    if not issues:
        print("verify: OK")
    return 1 if issues else 0


def _event_records(path: Path):
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


if __name__ == "__main__":
    raise SystemExit(main())
