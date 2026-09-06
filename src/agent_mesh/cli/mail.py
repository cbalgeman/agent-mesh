"""agent-mesh CLI — write side."""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import json
import math
import os
import re
import secrets
import shlex
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
import tempfile
from zoneinfo import ZoneInfo

from agent_mesh.adoption import (
    CONTRACT_TARGETS,
    AdoptionContractError,
    contract_status,
    install_contract,
)
from agent_mesh.config import (
    AgentMeshConfig,
    ConfigError,
    STATE_SHARING_CHOICES,
    STATE_SHARING_LOCAL_ONLY,
    default_config_text,
    detect_local_timezone,
    ensure_project_identity_config,
    ensure_project_dirs,
    load_config,
    project_key_from_name,
    write_agent_dir_gitignore,
)
from agent_mesh.core.decision_schema import (
    DECISION_APPLICABILITY_SCOPES,
    DECISION_BODY_FORMAT_CUSTOM,
    DECISION_BODY_FORMAT_GENERATED_V1,
    DECISION_BODY_FORMAT_UNKNOWN,
    DECISION_TIERS,
    DecisionBodyIntegrityError,
    DecisionVerificationError,
    applicability_scope_for,
    decision_applicability_issues,
    decision_completeness_issues,
    generated_decision_body,
    decision_review_progress,
    decision_revision_digest,
    normalize_decision_assumptions,
    normalize_decision_review_policy,
    normalize_decision_strings,
    normalize_decision_verification,
    parse_decision_evidence_entries,
    read_verified_decision_body,
)
from agent_mesh.core.decision_applicability import DecisionPathError
from agent_mesh.core.decision_context import (
    build_decision_context,
    build_unavailable_decision_context,
    render_bounded_decision_context_json,
)
from agent_mesh.core.context_delivery import capability_state_counts
from agent_mesh.core.context_budget import (
    CONTEXT_BUDGET_TIMEOUT_SECONDS,
    build_context_budget_report,
    render_context_budget_text,
)
from agent_mesh.core.git_changes import (
    GitChangeRequestError,
    GitChangeUnavailable,
    collect_git_changes,
    read_bounded_git_diff_paths,
    read_bounded_git_tracked_paths,
)
from agent_mesh.core.agent_instances import (
    INSTANCE_ID_RE,
    AgentInstanceError,
    bind_agent_instance,
    normalize_new_instance_handle,
    resolve_agent_instance_from_records,
    resolve_authoring_actor,
    reset_agent_instance,
    selected_agent_instance,
    validate_external_session_ref_digest,
)
from agent_mesh.core.provenance import BODY_AUTHORITY_VALUES, BODY_FIDELITY_VALUES
from agent_mesh.core.events import (
    Event,
    EventProtocolError,
    append_event,
    generate_event_id,
    utc_now,
)
from agent_mesh.core.lock import acquire
from agent_mesh.core.ids import new_public_message_id
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
    REFERENCE_RE,
    ReferenceOccurrence,
    extract_reference_occurrences,
)
from agent_mesh.core.recovery import recover
from agent_mesh.core.workflow_origin import WORKFLOW_ORIGINS, refs_with_workflow_origin
from agent_mesh.project_registry import (
    ProjectRegistryError,
    RegisteredProject,
    apply_project_unregistration,
    list_registered_projects,
    list_registered_projects_bounded,
    prepare_project_unregistration,
    register_project,
    registry_path,
    resolve_registered_project,
)
from agent_mesh.skill import SUPPORTED_TARGETS, UnknownTargetError, render_skill
from agent_mesh.store.rebuild import (
    BACKLOG_ID_COLLISION,
    DECISION_ID_RE,
    AgentInstanceStopLine,
    BacklogStopLine,
    DecisionStopLine,
    read_event_records,
    rebuild_all,
    validate_decision_proposal_identity,
)
from agent_mesh.store.sqlite import (
    connect,
    initialize_schema,
    json_loads,
    resolve_agent_instance,
    resolve_decision,
    resolve_message,
)
from agent_mesh.store.read_model import ReadModelUnavailable, open_read_model
from agent_mesh.views import locate_message, render_all
from agent_mesh.dispatch.local_driver import scaffold_project_local_driver

MAX_DECISION_CHECK_TEXT_BYTES = 16 * 1024
MAX_REFERENCE_CHECK_FILE_BYTES = 4 * 1024 * 1024
MAX_REFERENCE_CHECK_INPUT_BYTES = 64 * 1024 * 1024
MAX_REFERENCE_CHECK_PATHS = 10_000
MAX_REFERENCE_CHECK_PATH_BYTES = 4_096
MAX_REFERENCE_CHECK_FILESYSTEM_ENTRIES = 50_000
MAX_REFERENCE_CHECK_FILESYSTEM_SECONDS = 5.0
REF_RE = REFERENCE_RE


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    explicit_instance = getattr(args, "instance", None) or ""
    if INSTANCE_ID_RE.fullmatch(selected_agent_instance(explicit_instance)):
        print(
            "agent-mesh: raw AI instance IDs are diagnostic-only; use the instance handle",
            file=sys.stderr,
        )
        return 2
    instance_token = bind_agent_instance(getattr(args, "instance", None))
    try:
        return int(args.func(args))
    except AdoptionContractError as exc:
        print(f"agent-mesh: {exc}", file=sys.stderr)
        return 2
    except (DecisionPathError, GitChangeRequestError) as exc:
        print(f"agent-mesh check decisions: invalid request: {exc}", file=sys.stderr)
        return 2
    except (GitChangeUnavailable, ReadModelUnavailable) as exc:
        if getattr(args, "func", None) is cmd_check_decisions and getattr(args, "json", False):
            mode = str(getattr(args, "mode", "full"))
            base = getattr(args, "base", None)
            if mode in {"pr", "full"} and base is None:
                base = "main"
            print(
                json.dumps(
                    build_unavailable_decision_context(
                        mode=mode,
                        base=base,
                        diagnostic=str(exc),
                    ),
                    sort_keys=True,
                )
            )
        else:
            print(f"agent-mesh check decisions: context unavailable: {exc}", file=sys.stderr)
        return 3
    except (AgentInstanceError, ConfigError, DecisionVerificationError) as exc:
        print(f"agent-mesh: {exc}", file=sys.stderr)
        return 2
    except ProjectRegistryError as exc:
        print(f"agent-mesh: {exc}", file=sys.stderr)
        return 2
    except (AgentInstanceStopLine, DecisionStopLine) as exc:
        print(f"agent-mesh: {exc.code}: {exc.detail}", file=sys.stderr)
        return 1
    except BacklogStopLine as exc:
        print(f"agent-mesh: {exc.code}: {exc.detail}", file=sys.stderr)
        return 1
    except EventProtocolError as exc:
        print(f"agent-mesh: {exc}", file=sys.stderr)
        return 2
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


def _authoring_actor(config: AgentMeshConfig, *, explicit_actor: str | None = None) -> str:
    return resolve_authoring_actor(
        read_event_records(config.events_path),
        default_actor=config.default_sender,
        explicit_actor=explicit_actor,
    )


def _cross_project_authoring_actor(
    source_config: AgentMeshConfig,
    target_config: AgentMeshConfig,
) -> tuple[str, str]:
    """Resolve one bound chat across project-local registries without reusing its local ID."""

    selected = selected_agent_instance()
    if not selected:
        return _authoring_actor(target_config), ""
    if INSTANCE_ID_RE.fullmatch(selected):
        raise AgentInstanceError(
            "CROSS_PROJECT_INSTANCE_ID_FORBIDDEN: AI-agent instance IDs are project-local; "
            "bind a shared instance handle for cross-project writes"
        )

    source_instance = resolve_agent_instance_from_records(
        read_event_records(source_config.events_path), selected
    )
    if source_instance is None or source_instance.status != "active":
        raise AgentInstanceError(f"AGENT_INSTANCE_UNKNOWN: {selected}")
    mapping_key = f"{source_config.project_key}/{source_instance.label}"
    mapped_handle = target_config.identity.cross_project_instance_mappings.get(mapping_key)
    if not mapped_handle or INSTANCE_ID_RE.fullmatch(mapped_handle):
        raise AgentInstanceError(
            f"CROSS_PROJECT_INSTANCE_MAPPING_MISSING: target project must explicitly map "
            f"{mapping_key!r} to an active target-local handle"
        )
    target_instance = resolve_agent_instance_from_records(
        read_event_records(target_config.events_path), mapped_handle
    )
    if target_instance is None or target_instance.status != "active":
        raise AgentInstanceError(
            f"CROSS_PROJECT_INSTANCE_MAPPING_MISSING: mapped target handle "
            f"{mapped_handle!r} must be active in the target project"
        )
    if source_instance.participant != target_instance.participant:
        raise AgentInstanceError(
            f"CROSS_PROJECT_INSTANCE_MAPPING_MISMATCH: {mapping_key!r} maps to a "
            "target handle owned by a different participant"
        )
    return target_instance.participant, target_instance.label


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="agent-mesh")
    parser.add_argument(
        "--instance",
        help="authoring AI instance handle; alternatively set AGENT_MESH_INSTANCE_ID",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    init = sub.add_parser("init")
    init.add_argument("--participants", default="user,agent")
    init.add_argument("--default-recipient")
    init.add_argument("--default-sender")
    init.add_argument(
        "--timezone",
        help="human user's IANA timezone for date-based IDs (default: detected local timezone)",
    )
    init.add_argument(
        "--project-key",
        help="stable human project key for cross-repository references",
    )
    init.add_argument(
        "--state-sharing",
        choices=STATE_SHARING_CHOICES,
        default=None,
        help=(
            "Git policy for .agent-mesh state; new projects default to local-only, "
            "and git-shared must be selected explicitly"
        ),
    )
    init.add_argument(
        "--no-register",
        action="store_true",
        help="do not add this repo to the machine-local Workbench registry",
    )
    init.set_defaults(func=cmd_init)

    instance = sub.add_parser(
        "instance", help="register or manage a durable public AI-agent instance handle"
    )
    instance_sub = instance.add_subparsers(dest="instance_command", required=True)

    instance_register = instance_sub.add_parser("register")
    instance_register.add_argument("--participant", required=True)
    instance_register.add_argument("--provider", required=True)
    register_handle = instance_register.add_mutually_exclusive_group(required=True)
    register_handle.add_argument("--handle", dest="label")
    register_handle.add_argument("--label", dest="label", help=argparse.SUPPRESS)
    instance_register.add_argument("--workstream", default="")
    instance_register.add_argument("--runtime-profile", default="")
    instance_register.add_argument(
        "--external-session-ref-digest",
        default="",
        help="precomputed project-scoped SHA-256 session digest for unmanaged recovery",
    )
    instance_register.add_argument("--actor")
    instance_register.set_defaults(func=cmd_instance_register)

    instance_update = instance_sub.add_parser("update")
    instance_update.add_argument("identifier")
    instance_update.add_argument("--reason", required=True)
    update_handle = instance_update.add_mutually_exclusive_group()
    update_handle.add_argument("--handle", dest="label")
    update_handle.add_argument("--label", dest="label", help=argparse.SUPPRESS)
    instance_update.add_argument("--workstream")
    instance_update.add_argument("--runtime-profile")
    instance_update.add_argument("--external-session-ref-digest")
    instance_update.add_argument("--actor")
    instance_update.set_defaults(func=cmd_instance_update)

    instance_retire = instance_sub.add_parser("retire")
    instance_retire.add_argument("identifier")
    instance_retire.add_argument("--reason", required=True)
    instance_retire.add_argument("--actor")
    instance_retire.set_defaults(func=cmd_instance_retire)

    adopt = sub.add_parser(
        "adopt",
        help="install or verify the managed repo-local Agent Mesh contract",
    )
    adopt.add_argument("--repo", type=Path, default=Path("."))
    adopt.add_argument(
        "--timezone",
        help="human user's IANA timezone when adopting a config that lacks one",
    )
    adopt.add_argument(
        "--project-key",
        help="stable human project key when adopting a config that lacks one",
    )
    adopt.add_argument(
        "--target",
        dest="targets",
        action="append",
        choices=tuple(CONTRACT_TARGETS),
        help="instruction target; repeat to install more than one (default: AGENTS plus detected Claude)",
    )
    adopt.add_argument(
        "--check",
        action="store_true",
        help="verify the contract and conflicting legacy decision-write guidance without writing",
    )
    adopt.set_defaults(func=cmd_adopt)

    doctor = sub.add_parser(
        "doctor",
        help="run bounded local diagnostics without changing canonical state",
    )
    doctor.add_argument(
        "--context-budget",
        action="store_true",
        help="measure declared instruction files and representative decision-digest output",
    )
    doctor.add_argument(
        "--scope",
        choices=("repo", "registered-projects"),
        default="repo",
        help="inspect this repo or the explicit machine-local project registry",
    )
    doctor.add_argument(
        "--path",
        action="append",
        default=[],
        help="override configured representative digest sample paths; may repeat",
    )
    doctor.add_argument("--json", action="store_true")
    doctor.set_defaults(func=cmd_doctor)

    drivers = sub.add_parser("drivers", help="author project-local runtime drivers")
    drivers_sub = drivers.add_subparsers(dest="driver_command", required=True)
    driver_scaffold = drivers_sub.add_parser(
        "scaffold",
        description="Create a new fail-closed project-local one-shot driver skeleton.",
    )
    driver_scaffold.add_argument("--id", required=True, dest="driver_id")
    driver_scaffold.add_argument("--provider", required=True)
    driver_scaffold.add_argument("--output", required=True)
    driver_scaffold.set_defaults(func=cmd_drivers_scaffold)

    projects = sub.add_parser("projects", help="manage registered Workbench repos")
    projects_sub = projects.add_subparsers(dest="projects_command", required=True)
    projects_list = projects_sub.add_parser("list", help="list registered repos")
    projects_list.set_defaults(func=cmd_projects_list)
    projects_register = projects_sub.add_parser("register", help="register an agent-mesh repo")
    projects_register.add_argument("--repo", type=Path, default=Path("."))
    projects_register.set_defaults(func=cmd_projects_register)
    projects_unregister = projects_sub.add_parser(
        "unregister",
        help="interactively remove a repo from the machine-local Workbench registry",
        description=(
            "Interactively remove a repo from the machine-local Workbench registry. "
            "This destructive command requires two direct human confirmations and "
            "has no non-interactive bypass."
        ),
    )
    projects_unregister.add_argument("--repo", type=Path, default=Path("."))
    projects_unregister.set_defaults(func=cmd_projects_unregister)

    promote = sub.add_parser(
        "promote",
        help="selectively promote durable chat coordination into canonical events",
    )
    promote_sub = promote.add_subparsers(dest="promote_command", required=True)

    promote_chat_only = promote_sub.add_parser(
        "chat-only",
        help="classify a turn as conversational and write no Agent Mesh event",
    )
    promote_chat_only.set_defaults(func=cmd_promote_chat_only)

    promote_request = promote_sub.add_parser(
        "request",
        help="promote a durable work contract to a provenance-linked REQ",
    )
    _add_promotion_policy_arguments(promote_request)
    promote_request.add_argument("--from", dest="sender")
    promote_request.add_argument("--to")
    promote_request.add_argument("--to-instance", action="append", default=[])
    promote_request.add_argument("--feature", default="")
    _add_workflow_origin_argument(promote_request)
    promote_request.add_argument("--ref", action="append", default=[])
    promote_request.add_argument("--response-mode", choices=("single", "multi"), default="single")
    _add_promotion_provenance_arguments(promote_request)
    promote_request.add_argument("title")
    promote_request.add_argument("body", nargs="?")
    promote_request.set_defaults(func=cmd_promote_request)

    promote_response = promote_sub.add_parser(
        "response",
        help="promote a material outcome or evidence to a provenance-linked RES",
    )
    _add_promotion_policy_arguments(promote_response)
    promote_response.add_argument("--from", dest="sender")
    _add_workflow_origin_argument(promote_response)
    promote_response.add_argument("--ref", action="append", default=[])
    _add_promotion_provenance_arguments(promote_response)
    promote_response.add_argument("parent_id")
    promote_response.add_argument("summary")
    promote_response.add_argument("details", nargs="?")
    promote_response.set_defaults(func=cmd_promote_response)

    request = sub.add_parser("request")
    request.add_argument("--from", dest="sender")
    request.add_argument("--to")
    request.add_argument("--to-instance", action="append", default=[])
    request.add_argument("--feature", default="")
    _add_workflow_origin_argument(request)
    request.add_argument("--ref", action="append", default=[])
    request.add_argument("--response-mode", choices=("single", "multi"), default="single")
    _add_provenance_arguments(request)
    request.add_argument("title")
    request.add_argument("body", nargs="?")
    request.set_defaults(func=cmd_request)

    reply = sub.add_parser("reply")
    reply.add_argument("--from", dest="sender")
    _add_workflow_origin_argument(reply)
    reply.add_argument("--ref", action="append", default=[])
    _add_provenance_arguments(reply)
    reply.add_argument("parent_id")
    reply.add_argument("summary")
    reply.add_argument("details", nargs="?")
    reply.set_defaults(func=cmd_reply)

    respond = sub.add_parser("respond")
    respond.add_argument("--from", dest="sender")
    _add_workflow_origin_argument(respond)
    respond.add_argument("--ref", action="append", default=[])
    _add_provenance_arguments(respond)
    respond.add_argument("parent_id")
    respond.add_argument("summary")
    respond.add_argument("details", nargs="?")
    respond.set_defaults(func=cmd_reply)

    resolve = sub.add_parser("resolve")
    resolve.add_argument("--actor", default=None)
    resolve.add_argument("request_id")
    resolve.add_argument("reason")
    resolve.set_defaults(func=cmd_resolve)

    reopen = sub.add_parser("reopen")
    reopen.add_argument("--actor", default=None)
    reopen.add_argument("request_id")
    reopen.add_argument("reason")
    reopen.set_defaults(func=cmd_reopen)

    locate = sub.add_parser("locate")
    locate.add_argument("message_id")
    locate.set_defaults(func=cmd_locate)

    status = sub.add_parser("status")
    status.set_defaults(func=cmd_status)

    decision = sub.add_parser("decision")
    decision_sub = decision.add_subparsers(dest="decision_command", required=True)
    propose = decision_sub.add_parser("propose")
    propose.add_argument("--id", required=True, dest="human_id")
    propose.add_argument("--title", required=True)
    propose.add_argument("--tier", required=True, choices=DECISION_TIERS)
    propose.add_argument(
        "--scope",
        dest="applicability_scope",
        choices=DECISION_APPLICABILITY_SCOPES,
        help="applicability authority; defaults to paths with --affects, otherwise manual",
    )
    propose.add_argument("--owner")
    propose.add_argument("--affects", action="append", default=[])
    propose.add_argument("--exempt", action="append", default=[])
    propose.add_argument(
        "--generated-artifact",
        dest="generated_artifact_paths",
        action="append",
        default=[],
    )
    propose.add_argument("--required-check", action="append", default=[])
    propose.add_argument(
        "--verification",
        "--verify-command",
        dest="verify_command",
        action="append",
        default=[],
        help=(
            "add an argv-only verification command; shell operators and leading "
            "environment assignments are rejected"
        ),
    )
    propose.add_argument("--from-file", type=Path)
    propose.add_argument("--context", default="")
    propose.add_argument("--decision", default="")
    propose.add_argument("--tag", action="append", default=[])
    propose.add_argument(
        "--assumption",
        action="append",
        default=[],
        help="add an assumption statement; repeat to assign stable A1, A2, ... identities",
    )
    propose.add_argument(
        "--evidence",
        action="append",
        default=[],
        metavar="KIND=REFERENCE",
        help="add typed evidence; repeat for every reference",
    )
    propose.add_argument(
        "--required-reviewer",
        action="append",
        default=[],
        help="require a configured participant reviewer; repeat for the full reviewer set",
    )
    propose.add_argument(
        "--approval-quorum",
        type=int,
        help="approvals required from the required-reviewer set; defaults to all",
    )
    propose.set_defaults(func=cmd_decision_propose)

    amend = decision_sub.add_parser(
        "amend",
        help="append a metadata revision; accepted decisions return to Proposed",
        description=(
            "Append a metadata revision; accepted decisions return to Proposed. "
            "Title, context, and decision edits keep package-generated canonical Markdown "
            "synchronized; custom Markdown must be replaced in full with --from-file. "
            "Supplying a collection option replaces the whole stored collection, "
            "so repeat the option for every value to retain. Use its clear option "
            "alone to remove the collection."
        ),
    )
    amend.add_argument("identifier")
    amend.add_argument("--reason", required=True)
    amend.add_argument("--title")
    amend.add_argument("--tier", choices=DECISION_TIERS)
    amend.add_argument(
        "--scope",
        dest="applicability_scope",
        choices=DECISION_APPLICABILITY_SCOPES,
        help="replace applicability authority for this revision",
    )
    amend.add_argument("--owner")
    amend.add_argument("--clear-owner", action="store_true")
    amend.add_argument(
        "--affects",
        action="append",
        help="replace affected code globs; repeat for every glob to retain",
    )
    amend.add_argument(
        "--clear-affects",
        action="store_true",
        help="remove all affected code globs; cannot be combined with --affects",
    )
    amend.add_argument(
        "--exempt",
        action="append",
        help="replace exemption globs; repeat for every glob to retain",
    )
    amend.add_argument("--clear-exemptions", action="store_true")
    amend.add_argument(
        "--generated-artifact",
        dest="generated_artifact_paths",
        action="append",
        help="replace generated-artifact globs; repeat for every glob to retain",
    )
    amend.add_argument("--clear-generated-artifact-paths", action="store_true")
    amend.add_argument(
        "--required-check",
        action="append",
        help="replace required checks; repeat for every check to retain",
    )
    amend.add_argument(
        "--clear-required-checks",
        action="store_true",
        help="remove all required checks; cannot be combined with --required-check",
    )
    amend.add_argument(
        "--verification",
        "--verify-command",
        dest="verify_command",
        action="append",
        help=(
            "replace verification commands with argv-only definitions; repeat for every "
            "command to retain; shell operators are rejected"
        ),
    )
    amend.add_argument(
        "--clear-verification",
        action="store_true",
        help="remove all verification commands; cannot be combined with --verification",
    )
    amend.add_argument(
        "--tag",
        action="append",
        help="replace tags; repeat for every tag to retain",
    )
    amend.add_argument(
        "--clear-tags",
        action="store_true",
        help="remove all tags; cannot be combined with --tag",
    )
    amend.add_argument(
        "--assumption",
        action="append",
        help="replace assumptions; repeat for every assumption to retain",
    )
    amend.add_argument("--clear-assumptions", action="store_true")
    amend.add_argument(
        "--evidence",
        action="append",
        metavar="KIND=REFERENCE",
        help="replace evidence; repeat for every reference to retain",
    )
    amend.add_argument("--clear-evidence", action="store_true")
    amend.add_argument(
        "--required-reviewer",
        action="append",
        help="replace the full configured-participant reviewer set; repeat to retain",
    )
    amend.add_argument("--approval-quorum", type=int)
    amend.add_argument("--clear-review-policy", action="store_true")
    amend.add_argument(
        "--from-file",
        type=Path,
        help="replace the complete canonical Markdown body for a custom decision",
    )
    amend.add_argument("--context")
    amend.add_argument("--decision")
    amend.set_defaults(func=cmd_decision_amend)

    accept = decision_sub.add_parser("accept")
    accept.add_argument("identifier")
    accept.add_argument(
        "--by",
        required=True,
        help="human participant operating this interactive approval command",
    )
    accept.add_argument(
        "--notes",
        required=True,
        help="where or why the human approved this exact decision revision",
    )
    accept.set_defaults(func=cmd_decision_accept)

    revisit = decision_sub.add_parser("revisit")
    revisit.add_argument("identifier")
    revisit.add_argument("--reason", required=True)
    revisit.add_argument("--assumption")
    revisit.add_argument("--new-decision-id")
    revisit.set_defaults(func=cmd_decision_revisit)

    supersede = decision_sub.add_parser("supersede")
    supersede.add_argument("old_id")
    supersede.add_argument("--by", required=True, dest="new_id")
    supersede.add_argument("--migration-notes")
    supersede.set_defaults(func=cmd_decision_supersede)

    retire = decision_sub.add_parser("retire")
    retire.add_argument("identifier")
    retire.add_argument("--reason", required=True)
    retire.set_defaults(func=cmd_decision_retire)

    backlog = sub.add_parser("backlog")
    backlog_sub = backlog.add_subparsers(dest="backlog_command", required=True)

    backlog_fields = argparse.ArgumentParser(add_help=False)
    backlog_fields.add_argument("--actor")
    backlog_fields.add_argument("--title")
    backlog_fields.add_argument("--item-type")
    backlog_fields.add_argument("--summary")
    backlog_fields.add_argument("--root-cause-summary")
    backlog_fields.add_argument("--architectural-category")
    backlog_fields.add_argument("--status")
    backlog_fields.add_argument("--priority")
    backlog_fields.add_argument("--launch-scope")
    backlog_fields.add_argument("--release-phase")
    backlog_fields.add_argument("--production-state")
    backlog_fields.add_argument("--disposition")
    backlog_fields.add_argument("--owner-hint")
    backlog_fields.add_argument(
        "--owner-instance", help="active public AI instance handle responsible for this item"
    )
    backlog_fields.add_argument("--lane")
    backlog_fields.add_argument("--notes")
    _add_workflow_origin_argument(backlog_fields)
    backlog_fields.add_argument(
        "--ref",
        action="append",
        default=[],
        help="structured ref as type:value; may be repeated",
    )
    _add_json_payload_arguments(backlog_fields)

    backlog_create = backlog_sub.add_parser(
        "create",
        parents=[backlog_fields],
        help="create a backlog item with an atomically allocated human-readable ID",
    )
    backlog_create.set_defaults(func=cmd_backlog_upsert, backlog_write_mode="create")

    backlog_update = backlog_sub.add_parser(
        "update",
        parents=[backlog_fields],
        help="update an existing backlog item",
    )
    backlog_update.add_argument("item_id", metavar="BKL-ID")
    backlog_update.set_defaults(func=cmd_backlog_upsert, backlog_write_mode="update")

    backlog_upsert = backlog_sub.add_parser(
        "upsert",
        parents=[backlog_fields],
        description=(
            "Compatibility/import surface. An explicit --id may only import a missing "
            "item; collisions fail loudly and existing items require backlog update. "
            "Omit --id to create a normal item with an atomically allocated "
            "BKL-YYYYMMDD-NN identifier."
        ),
    )
    backlog_upsert.add_argument(
        "--id",
        dest="item_id",
        metavar="BKL-ID",
        help="missing imported item ID; existing items require backlog update",
    )
    backlog_upsert.set_defaults(func=cmd_backlog_upsert, backlog_write_mode="upsert")

    backlog_refer = backlog_sub.add_parser(
        "refer",
        help="route a source backlog finding to the registered repo that owns the work",
    )
    backlog_refer.add_argument("source_item_id", metavar="SOURCE-BKL-ID")
    backlog_refer.add_argument("--to", required=True, dest="target_project")
    backlog_refer.add_argument("--reason", required=True, help="why ownership belongs elsewhere")
    backlog_refer.add_argument("--title")
    backlog_refer.add_argument("--summary")
    backlog_refer.add_argument("--priority")
    backlog_refer.add_argument("--source-event", action="append", default=[])
    backlog_refer.add_argument("--ref", action="append", default=[])
    backlog_refer.add_argument(
        "--blocks-current-work",
        choices=("yes", "no", "unknown"),
        default="unknown",
    )
    backlog_refer.add_argument("--risk", default="not stated")
    backlog_refer.add_argument(
        "--use-existing",
        metavar="TARGET-BKL-ID",
        help="promote onto a reviewed duplicate instead of creating another target item",
    )
    backlog_refer.add_argument("--actor")
    referral_write = backlog_refer.add_mutually_exclusive_group()
    referral_write.add_argument(
        "--apply",
        action="store_true",
        help="write both repos; caller attests its runtime has explicit scope for both",
    )
    referral_write.add_argument(
        "--record-only",
        action="store_true",
        help="record a pending source receipt without writing the target repo",
    )
    backlog_refer.set_defaults(func=cmd_backlog_refer)

    backlog_link = backlog_sub.add_parser("link")
    backlog_link.add_argument("--actor")
    backlog_link.add_argument("--allow-missing-item", action="store_true")
    backlog_link.add_argument("item_id")
    backlog_link.add_argument("ref_type")
    backlog_link.add_argument("ref_value")
    backlog_link.set_defaults(func=cmd_backlog_link)

    backlog_record = backlog_sub.add_parser("record")
    backlog_record.add_argument("--actor")
    backlog_record.add_argument(
        "--detail", action="append", default=[], help="audit detail as key=value; may be repeated"
    )
    backlog_record.add_argument("--details-json")
    backlog_record.add_argument("--details-file", type=Path)
    backlog_record.add_argument("item_id")
    backlog_record.add_argument("event_type")
    backlog_record.set_defaults(func=cmd_backlog_record)

    check = sub.add_parser("check")
    check_sub = check.add_subparsers(dest="check_command", required=True)
    refs = check_sub.add_parser("refs")
    refs.add_argument("--paths", default="**/*")
    refs.add_argument("--ci-mode", choices=("pr", "full"), default="full")
    refs.add_argument("--base", default="main")
    refs.add_argument(
        "--file",
        action="append",
        type=Path,
        default=[],
        help="scan an explicit project-local file, including ignored/private files; may repeat",
    )
    refs.add_argument("--stdin", action="store_true", dest="read_stdin")
    refs.add_argument("--json", action="store_true")
    refs.add_argument("--record-scan", action="store_true")
    refs.set_defaults(func=cmd_check_refs)
    decision_check = check_sub.add_parser(
        "decisions",
        description=(
            "Evaluate applicable decisions against one local Git change set. "
            "Version 0.4.0 results are advisory and never execute stored verification."
        ),
    )
    decision_check.add_argument(
        "--mode",
        choices=("pr", "staged", "worktree", "full"),
        default="full",
    )
    decision_check.add_argument(
        "--base",
        help="local Git ref used only by pr/full mode (default: main)",
    )
    decision_check.add_argument("--json", action="store_true")
    decision_check.set_defaults(func=cmd_check_decisions)

    workbench = sub.add_parser(
        "workbench",
        help="run a small local agent-mesh workbench",
        description=(
            "run a small local agent-mesh workbench or manage its automatic per-user service"
        ),
    )
    workbench.add_argument("workbench_mode", nargs="?", choices=("service",))
    workbench.add_argument(
        "service_action",
        nargs="?",
        choices=(
            "install",
            "repair",
            "status",
            "open",
            "start",
            "restart",
            "relinquish",
            "uninstall",
        ),
    )
    workbench.add_argument("--repo", type=Path, default=Path("."))
    workbench.add_argument("--host", default="127.0.0.1")
    workbench.add_argument(
        "--port",
        type=int,
        help="loopback port (default: 8765 manual, 8767 automatic service)",
    )
    workbench.add_argument("--open", action="store_true", help="open the workbench in a browser")
    workbench.add_argument("--managed-service", action="store_true", help=argparse.SUPPRESS)
    workbench.add_argument("--config-home", type=Path, help=argparse.SUPPRESS)
    workbench.set_defaults(func=cmd_workbench)

    skill = sub.add_parser("skill", help="render or install the agent-mesh skill")
    skill_sub = skill.add_subparsers(dest="skill_command", required=True)

    skill_targets = skill_sub.add_parser("targets", help="list supported skill targets")
    skill_targets.set_defaults(func=cmd_skill_targets)

    skill_render = skill_sub.add_parser("render", help="render skill to stdout or a file")
    skill_render.add_argument("--target", required=True)
    render_dest = skill_render.add_mutually_exclusive_group()
    render_dest.add_argument("--stdout", action="store_true", default=False)
    render_dest.add_argument(
        "--output",
        type=Path,
        default=None,
        help="explicit file path; defaults to stdout when omitted",
    )
    skill_render.set_defaults(func=cmd_skill_render)

    skill_install = skill_sub.add_parser(
        "install", help="render skill and write it to an explicit destination"
    )
    skill_install.add_argument("--target", required=True)
    skill_install.add_argument(
        "--dest",
        required=True,
        type=Path,
        help="explicit destination file; no global home discovery, no --all",
    )
    skill_install.set_defaults(func=cmd_skill_install)

    return parser


def _add_json_payload_arguments(parser: argparse.ArgumentParser) -> None:
    payload = parser.add_mutually_exclusive_group()
    payload.add_argument("--json", dest="json_payload")
    payload.add_argument("--file", dest="json_file", type=Path)


def cmd_init(args: argparse.Namespace) -> int:
    root = Path.cwd()
    agent_dir = root / ".agent-mesh"
    agent_dir.mkdir(parents=True, exist_ok=True)
    participants = [item.strip() for item in args.participants.split(",") if item.strip()]
    default_sender = args.default_sender or (participants[0] if participants else "human")
    config_path = agent_dir / "config.toml"
    if not config_path.exists():
        config_path.write_text(
            default_config_text(
                participants=participants,
                default_sender=default_sender,
                default_recipient=args.default_recipient
                or (participants[0] if participants else "codex"),
                state_sharing=args.state_sharing or STATE_SHARING_LOCAL_ONLY,
                project_name=root.name,
                project_timezone=args.timezone or detect_local_timezone(),
                project_key=args.project_key or project_key_from_name(root.name),
            ),
            encoding="utf-8",
        )
    config = load_config(root)
    if args.state_sharing is not None and args.state_sharing != config.state_sharing:
        raise ConfigError(
            "--state-sharing does not rewrite an existing config; edit "
            "[version_control].state_sharing in .agent-mesh/config.toml, then rerun init"
        )
    if args.timezone is not None and args.timezone != config.project_timezone:
        raise ConfigError(
            "--timezone does not rewrite an existing config; edit "
            "[project].timezone in .agent-mesh/config.toml, then rerun init"
        )
    if args.project_key is not None and args.project_key != config.project_key:
        raise ConfigError(
            "--project-key does not rewrite an existing config; edit "
            "[project].key in .agent-mesh/config.toml, then rerun init"
        )
    ensure_project_dirs(config)
    write_agent_dir_gitignore(config)
    rebuild_all(config)
    render_all(config)
    print(f"initialized agent-mesh project at {agent_dir}")
    if config.state_sharing == STATE_SHARING_LOCAL_ONLY:
        print("state sharing: local-only (all .agent-mesh paths are ignored by Git)")
    else:
        print("state sharing: git-shared (canonical config, events, and bodies may be tracked)")
    print(f"project timezone: {config.project_timezone}")
    print(f"project key: {config.project_key}")
    if not args.no_register:
        project = register_project(root)
        print(f"registered Workbench repo {project.id} at {registry_path()}")
    integration = contract_status(root)
    if not integration["healthy"]:
        print(
            "agent integration: incomplete; run `agent-mesh adopt --repo .`, "
            "then `agent-mesh adopt --repo . --check`"
        )
    return 0


def cmd_doctor(args: argparse.Namespace) -> int:
    if not args.context_budget:
        raise ConfigError("doctor currently requires --context-budget")
    deadline = time.monotonic() + CONTEXT_BUDGET_TIMEOUT_SECONDS
    current = load_config()
    configs: dict[Path, AgentMeshConfig] = {
        current.project_root.resolve(): current,
    }
    if args.scope == "registered-projects":
        projects = list_registered_projects_bounded(max_entries=64, deadline=deadline)
        for project in projects:
            if time.monotonic() >= deadline:
                raise ProjectRegistryError(
                    "Context-budget project enumeration exceeded its time bound"
                )
            root = project.root.resolve()
            if root not in configs:
                configs[root] = load_config(root)
    report = build_context_budget_report(
        [configs[root] for root in sorted(configs, key=str)],
        scope=args.scope,
        sample_paths=args.path or None,
        deadline_monotonic=deadline,
    )
    if args.json:
        print(json.dumps(report, sort_keys=True))
    else:
        print(render_context_budget_text(report))
    return 0 if report["complete"] else 3


def cmd_instance_register(args: argparse.Namespace) -> int:
    config = load_config()
    participant = args.participant.strip()
    actor = _authoring_actor(config, explicit_actor=args.actor)
    _ensure_participant(config, participant, role="instance participant")
    _ensure_participant(config, actor, role="actor")
    label = normalize_new_instance_handle(args.label, participant=participant)
    provider = args.provider.strip().lower()
    if not provider:
        raise ConfigError("instance provider must be non-empty")
    _validate_instance_runtime_profile(config, args.runtime_profile, participant)

    lock_handle = acquire(config.agent_dir / ".mail-lock")
    last_event_seq = None
    try:
        recover(config.events_path, config.agent_dir)
        rebuild_all(config)
        occurred_utc = utc_now()
        instance_id = _allocate_agent_instance_id(config, occurred_utc=occurred_utc)
        event = Event(
            event_id=generate_event_id(),
            occurred_utc=occurred_utc,
            actor=actor,
            kind="agent_instance_registered",
            entity_id=instance_id,
            thread_id=instance_id,
            payload={
                "id": instance_id,
                "participant": participant,
                "provider": provider,
                "label": label,
                "workstream": args.workstream.strip(),
                "runtime_profile": args.runtime_profile.strip(),
                "external_session_ref_digest": validate_external_session_ref_digest(
                    args.external_session_ref_digest
                ),
            },
        )
        result = append_event(config.events_path, event, lock_acquired=True)
        last_event_seq = result.event.event_seq
    finally:
        lock_handle.release(last_event_seq=last_event_seq)
    print(f"registered {label}\tactive")
    return 0


def cmd_instance_update(args: argparse.Namespace) -> int:
    config = load_config()
    actor = _authoring_actor(config, explicit_actor=args.actor)
    _ensure_participant(config, actor, role="actor")
    if not args.reason.strip():
        raise ConfigError("instance update requires a non-empty reason")

    lock_handle = acquire(config.agent_dir / ".mail-lock")
    last_event_seq = None
    did_update = False
    instance_id = args.identifier
    try:
        recover(config.events_path, config.agent_dir)
        rebuild_all(config)
        current = _agent_instance_snapshot(config, args.identifier, require_active=True)
        instance_id = current["id"]
        fields_changed: dict[str, list[str]] = {}
        if args.label is not None:
            label = normalize_new_instance_handle(
                args.label, participant=str(current["participant"])
            )
            if label != current["label"]:
                fields_changed["label"] = [current["label"], label]
        if args.workstream is not None:
            workstream = args.workstream.strip()
            if workstream != current["workstream"]:
                fields_changed["workstream"] = [current["workstream"], workstream]
        if args.runtime_profile is not None:
            runtime_profile = args.runtime_profile.strip()
            _validate_instance_runtime_profile(config, runtime_profile, current["participant"])
            if runtime_profile != current["runtime_profile"]:
                fields_changed["runtime_profile"] = [
                    current["runtime_profile"],
                    runtime_profile,
                ]
        if args.external_session_ref_digest is not None:
            digest = validate_external_session_ref_digest(args.external_session_ref_digest)
            if digest != current["external_session_ref_digest"]:
                fields_changed["external_session_ref_digest"] = [
                    current["external_session_ref_digest"],
                    digest,
                ]
        if fields_changed:
            event = Event(
                event_id=generate_event_id(),
                actor=actor,
                kind="agent_instance_metadata_updated",
                entity_id=instance_id,
                thread_id=instance_id,
                payload={
                    "id": instance_id,
                    "reason": args.reason.strip(),
                    "fields_changed": fields_changed,
                },
            )
            result = append_event(config.events_path, event, lock_acquired=True)
            last_event_seq = result.event.event_seq
            did_update = True
        else:
            last_event_seq = int(current["event_seq"])
    finally:
        lock_handle.release(last_event_seq=last_event_seq)
    print(f"{'updated' if did_update else 'unchanged'} {current['label']}")
    return 0


def cmd_instance_retire(args: argparse.Namespace) -> int:
    config = load_config()
    actor = _authoring_actor(config, explicit_actor=args.actor)
    _ensure_participant(config, actor, role="actor")
    reason = args.reason.strip()
    if not reason:
        raise ConfigError("instance retirement requires a non-empty reason")

    lock_handle = acquire(config.agent_dir / ".mail-lock")
    last_event_seq = None
    try:
        recover(config.events_path, config.agent_dir)
        rebuild_all(config)
        current = _agent_instance_snapshot(config, args.identifier, require_active=True)
        instance_id = current["id"]
        event = Event(
            event_id=generate_event_id(),
            actor=actor,
            kind="agent_instance_retired",
            entity_id=instance_id,
            thread_id=instance_id,
            payload={"id": instance_id, "reason": reason},
        )
        result = append_event(config.events_path, event, lock_acquired=True)
        last_event_seq = result.event.event_seq
    finally:
        lock_handle.release(last_event_seq=last_event_seq)
    print(f"retired {current['label']}")
    return 0


def cmd_adopt(args: argparse.Namespace) -> int:
    root = args.repo.expanduser().resolve()
    if args.check:
        status = contract_status(root, targets=args.targets)
        for item in status["files"]:
            print(f"{item['target']}\t{item['status']}\t{item['path']}")
        for conflict in status["conflicts"]:
            print(f"conflict\t{conflict['path']}:{conflict['line']}\t{conflict['text']}")
        identity = status["project_identity"]
        if identity.get("complete"):
            print(
                "project identity: current "
                f"({identity['project_key']} {identity['timezone']} {identity['store_id']})"
            )
        elif identity.get("migration_kind"):
            detail = identity.get("error") or ",".join(identity.get("missing", []))
            print(
                "project identity: migration required "
                f"({detail}; run `agent-mesh adopt --repo {shlex.quote(str(root))}`)"
            )
        else:
            detail = identity.get("error") or ",".join(identity.get("missing", []))
            print(f"project identity: error ({detail}; edit .agent-mesh/config.toml)")
        print(
            "agent contract: "
            + ("healthy" if status["healthy"] else "incomplete or conflicting")
            + f" (v{status['version']} {status['digest']})"
        )
        _print_context_delivery_status(status)
        return 0 if status["healthy"] else 1

    identity_update = ensure_project_identity_config(
        root,
        timezone_name=args.timezone,
        project_key=args.project_key,
    )
    identity_action = "updated" if identity_update.changed else "current"
    print(
        f"{identity_action}\tproject-identity\t.agent-mesh/config.toml\t"
        f"{identity_update.project_key}\t{identity_update.timezone}"
    )
    results = install_contract(root, targets=args.targets)
    for result in results:
        action = "updated" if result.changed else "current"
        print(f"{action}\t{result.target}\t{result.path}")
    status = contract_status(root, targets=args.targets)
    for conflict in status["conflicts"]:
        print(
            f"warning: conflicting legacy decision-write guidance at "
            f"{conflict['path']}:{conflict['line']}: {conflict['text']}"
        )
    if status["conflicts"]:
        print(
            "managed contract installed, but adoption remains incomplete until the "
            "conflicting legacy write guidance is removed"
        )
        return 1
    print(f"agent contract: healthy (v{status['version']} {status['digest']})")
    _print_context_delivery_status(status)
    return 0


def _print_context_delivery_status(status: dict[str, Any]) -> None:
    report = status.get("context_delivery")
    if not isinstance(report, dict):
        print("context delivery: unavailable (does not change agent-contract health)")
        return
    counts = capability_state_counts(report)
    print(
        "context delivery: available "
        f"(unreported={counts['unreported']}, unsupported={counts['unsupported']}, "
        f"reported={counts['reported']}, verified={counts['verified']}; "
        "does not change agent-contract health)"
    )


def cmd_drivers_scaffold(args: argparse.Namespace) -> int:
    config = load_config()
    result = scaffold_project_local_driver(
        project_root=config.project_root,
        output=args.output,
        driver_id=args.driver_id,
        provider=args.provider,
    )
    manifest = result.manifest_path.relative_to(config.project_root)
    print(f"created\t{result.entrypoint_path.relative_to(config.project_root)}")
    print(f"created\t{manifest}")
    print('driver_source = "project-local"')
    print('driver_protocol = "agent-mesh.runtime-driver.v1"')
    print(f'driver_manifest = "{manifest.as_posix()}"')
    print(f'driver_manifest_sha256 = "{result.manifest_sha256}"')
    print("scaffold is fail-closed until probe and launch are implemented")
    return 0


def cmd_projects_list(args: argparse.Namespace) -> int:
    for project in list_registered_projects():
        print(f"{project.key}\t{project.id}\t{project.name}\t{project.root}")
    _ = args
    return 0


def cmd_projects_register(args: argparse.Namespace) -> int:
    project = register_project(args.repo)
    print(f"registered {project.id}\t{project.name}\t{project.root}")
    return 0


def _confirm_destructive_cli_action(
    *,
    title: str,
    scope: str,
    consequences: tuple[str, ...],
    confirmation: str,
) -> bool:
    if not sys.stdin.isatty():
        raise ConfigError(
            f"{title} requires direct human action in an interactive terminal; "
            "automation flags cannot bypass destructive confirmation"
        )
    print(f"Destructive action: {title}")
    print(f"Scope: {scope}")
    for consequence in consequences:
        print(f"Consequence: {consequence}")
    try:
        first_confirmation = input("Continue to the final confirmation? [y/N] ")
    except (EOFError, KeyboardInterrupt):
        print("destructive action cancelled; no information was removed", file=sys.stderr)
        return False
    if first_confirmation.strip().casefold() not in {
        "y",
        "yes",
    }:
        print("destructive action cancelled; no information was removed", file=sys.stderr)
        return False
    try:
        final_confirmation = input(f"Type {confirmation} to confirm this exact scope: ")
    except (EOFError, KeyboardInterrupt):
        print("destructive action cancelled; no information was removed", file=sys.stderr)
        return False
    if final_confirmation.strip() != confirmation:
        print("destructive action cancelled; no information was removed", file=sys.stderr)
        return False
    return True


def cmd_projects_unregister(args: argparse.Namespace) -> int:
    root = args.repo.expanduser().resolve()
    plan = prepare_project_unregistration(root)
    if not plan.records:
        print("not registered")
        return 0
    registered_ids = ", ".join(plan.record_ids)
    escaped_root = json.dumps(str(root), ensure_ascii=True)
    if not _confirm_destructive_cli_action(
        title="unregister an Agent Mesh project",
        scope=(
            f"the machine-local Workbench registry entry IDs {registered_ids} "
            f"for path {escaped_root}"
        ),
        consequences=(
            "the project will disappear from this user's Workbench registry until registered again",
            "no repository file or canonical Agent Mesh event will be deleted",
            "other registries, clones, caches, backups, and external copies are not changed",
        ),
        confirmation=f"UNREGISTER {registered_ids}",
    ):
        return 1
    removed = apply_project_unregistration(plan)
    print("unregistered" if removed else "not registered")
    return 0


def _add_promotion_policy_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--classification",
        choices=("promotion-candidate", "durable-event"),
        default="promotion-candidate",
        help=(
            "ambiguous candidates require --confirmed-by; durable-event is for an "
            "unambiguous work contract or material outcome"
        ),
    )
    parser.add_argument(
        "--confirmed-by",
        help="human participant who explicitly confirmed an ambiguous promotion",
    )


def _add_promotion_provenance_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--source-channel", required=True)
    parser.add_argument("--source-uri", required=True)
    parser.add_argument("--source-confidence", type=float, default=1.0)
    parser.add_argument(
        "--body-fidelity",
        choices=sorted(BODY_FIDELITY_VALUES),
        default="full",
    )


def cmd_promote_chat_only(args: argparse.Namespace) -> int:
    load_config()
    _ = args
    print("chat-only: no Agent Mesh event written")
    return 0


def _prepare_chat_promotion(
    args: argparse.Namespace,
    *,
    body_authority: str,
    causal_relation: str,
) -> bool:
    config = load_config()
    sender = _authoring_actor(config, explicit_actor=args.sender)
    confirmed_by = (args.confirmed_by or "").strip()
    if args.classification == "promotion-candidate" and not confirmed_by:
        print(
            "agent-mesh: promotion-candidate is ambiguous; no event written. "
            "Ask the human, then rerun with --confirmed-by <participant>.",
            file=sys.stderr,
        )
        return False
    if confirmed_by:
        _ensure_participant(config, confirmed_by, role="confirming human")

    args.source_role = "authoritative_body"
    args.body_authority = body_authority
    args._causal_relation = causal_relation
    args._source_selection = {
        "mode": "manual",
        "confidence": args.source_confidence,
        "selected_by": confirmed_by or sender,
        "requires_review": False,
    }
    return True


def cmd_promote_request(args: argparse.Namespace) -> int:
    if not _prepare_chat_promotion(
        args,
        body_authority="human_chat",
        causal_relation="caused",
    ):
        return 1
    return cmd_request(args)


def cmd_promote_response(args: argparse.Namespace) -> int:
    if not _prepare_chat_promotion(
        args,
        body_authority="agent_summary",
        causal_relation="derived_from",
    ):
        return 1
    return cmd_reply(args)


def cmd_request(args: argparse.Namespace) -> int:
    config = load_config()
    body = args.body if args.body is not None else sys.stdin.read()
    sender = _authoring_actor(config, explicit_actor=args.sender)
    _ensure_participant(config, sender, role="sender")
    provenance = _provenance_payload_from_args(args)
    if provenance is None:
        return 2
    raw_to = (args.to or "").strip()
    instance_addresses = list(args.to_instance or [])
    if not raw_to and not instance_addresses:
        raise ConfigError("request requires --to or at least one --to-instance")
    recipients = config.canonical_recipients(raw_to) if raw_to else []
    recipient_instance_ids: list[str] = []
    if instance_addresses:
        _rebuild_all_locked(config)
        for address in instance_addresses:
            instance = _agent_instance_snapshot(config, address, require_active=True)
            recipient_instance_ids.append(str(instance["id"]))
            recipients.append(str(instance["participant"]))
    recipients = list(dict.fromkeys(recipients))
    _ensure_participants(config, recipients, role="recipient")
    request_id = new_public_message_id("REQ", sender)
    refs = refs_with_workflow_origin(args.ref, args.workflow_origin)
    payload: dict[str, Any] = {
        "from": sender,
        "to": recipients,
        "title": args.title,
        "body": body,
        "feature": args.feature,
        "refs": refs,
        "response_mode": args.response_mode,
    }
    if recipient_instance_ids:
        payload["to_instances"] = recipient_instance_ids
        payload["original_to_instances"] = instance_addresses
    if args.workflow_origin:
        payload["workflow_origin"] = args.workflow_origin
    payload.update(provenance)
    if config.routing.preserve_raw_to and raw_to:
        payload["original_to"] = raw_to
    event = Event(
        event_id=generate_event_id(),
        actor=str(payload["from"]),
        kind="req_created",
        entity_id=request_id,
        thread_id=request_id,
        payload=payload,
    )
    result = append_event(config.events_path, event, lock_acquired=False)
    print(result.event.entity_id)
    return 0


def _add_provenance_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--source-channel")
    parser.add_argument("--source-uri")
    parser.add_argument("--source-role", default="authoritative_body")
    parser.add_argument("--source-confidence", type=float, default=1.0)
    parser.add_argument("--body-authority", choices=sorted(BODY_AUTHORITY_VALUES))
    parser.add_argument("--body-fidelity", choices=sorted(BODY_FIDELITY_VALUES))


def _add_workflow_origin_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--origin",
        dest="workflow_origin",
        choices=WORKFLOW_ORIGINS,
        help="workflow provenance, independent of status/lane scheduling fields",
    )


def _provenance_payload_from_args(args: argparse.Namespace) -> dict[str, Any] | None:
    source_channel = args.source_channel.strip() if args.source_channel is not None else None
    source_uri = args.source_uri.strip() if args.source_uri is not None else None
    source_ref_was_provided = args.source_channel is not None or args.source_uri is not None
    if source_ref_was_provided and (not source_channel or not source_uri):
        print(
            "agent-mesh: --source-channel and --source-uri must be provided together and non-empty",
            file=sys.stderr,
        )
        return None
    if not math.isfinite(args.source_confidence) or not 0.0 <= args.source_confidence <= 1.0:
        print(
            "agent-mesh: --source-confidence must be a finite number from 0.0 to 1.0",
            file=sys.stderr,
        )
        return None
    source_role = args.source_role.strip()
    if (source_channel or source_uri) and not source_role:
        print(
            "agent-mesh: --source-role must be non-empty when source refs are provided",
            file=sys.stderr,
        )
        return None
    if (
        source_channel
        and source_uri
        and (args.body_authority is None or args.body_fidelity is None)
    ):
        print(
            "agent-mesh: --body-authority and --body-fidelity are required when source refs are provided",
            file=sys.stderr,
        )
        return None
    no_source_metadata_requested = not source_channel and (
        args.body_authority is not None or args.body_fidelity is not None
    )
    if no_source_metadata_requested and (
        args.body_authority != "unknown" or args.body_fidelity is None
    ):
        print(
            "agent-mesh: --body-authority must be unknown and --body-fidelity must be provided "
            "when no source context refs are provided",
            file=sys.stderr,
        )
        return None

    payload: dict[str, Any] = {}
    if source_channel and source_uri:
        payload["source_context_refs"] = [
            {
                "channel": source_channel,
                "source_uri": source_uri,
                "role": source_role,
                "confidence": args.source_confidence,
            }
        ]
    if args.body_authority:
        payload["body_authority"] = args.body_authority
    if args.body_fidelity:
        payload["body_fidelity"] = args.body_fidelity
    if not source_channel and (args.body_authority or args.body_fidelity):
        payload["source_context_status"] = "no_source_context_available"
    causal_relation = getattr(args, "_causal_relation", None)
    if causal_relation:
        payload["causal_edges"] = [
            {
                "relation": causal_relation,
                "from_ref": source_uri,
                "confidence": args.source_confidence,
            }
        ]
    source_selection = getattr(args, "_source_selection", None)
    if source_selection:
        payload["source_selection"] = source_selection
    return payload


def cmd_reply(args: argparse.Namespace) -> int:
    config = load_config()
    body = args.details if args.details is not None else sys.stdin.read()
    sender = _authoring_actor(config, explicit_actor=args.sender)
    _ensure_participant(config, sender, role="sender")
    provenance = _provenance_payload_from_args(args)
    if provenance is None:
        return 2
    lock_handle = acquire(config.agent_dir / ".mail-lock")
    last_event_seq = None
    try:
        parent = _resolve_reply_parent(config, args.parent_id)
        if parent is None:
            print(f"agent-mesh: parent not found: {args.parent_id}", file=sys.stderr)
            return 1
        if parent["parent_kind"] == "request":
            request_payload = _ensure_request_event_exists(config, parent["request_id"])
            _ensure_response_allowed(
                config, request_id=parent["request_id"], request_payload=request_payload
            )
        response_id = new_public_message_id("RES", sender)
        refs = refs_with_workflow_origin(args.ref, args.workflow_origin)
        event = Event(
            event_id=generate_event_id(),
            actor=sender,
            kind="res_posted",
            entity_id=response_id,
            thread_id=parent["thread_id"],
            payload={
                "from": sender,
                "request_id": parent["request_id"],
                "parent_id": parent["parent_id"],
                "parent_kind": parent["parent_kind"],
                "summary": args.summary,
                "body": body,
                "response_id": response_id,
                "refs": refs,
                **provenance,
            },
        )
        if args.workflow_origin:
            event.payload["workflow_origin"] = args.workflow_origin
        result = append_event(config.events_path, event, lock_acquired=True)
        last_event_seq = result.event.event_seq
    finally:
        lock_handle.release(last_event_seq=last_event_seq)
    print(response_id)
    return 0


def cmd_resolve(args: argparse.Namespace) -> int:
    config = load_config()
    _ensure_request_exists(config, args.request_id)
    _status_event(config, args, to_status="closed")
    print(f"resolved {args.request_id}")
    return 0


def cmd_reopen(args: argparse.Namespace) -> int:
    config = load_config()
    _ensure_request_exists(config, args.request_id)
    _status_event(config, args, to_status="open")
    print(f"reopened {args.request_id}")
    return 0


def cmd_locate(args: argparse.Namespace) -> int:
    config = load_config()
    _render_all_locked(config)
    found = locate_message(config, args.message_id)
    if not found:
        print(f"agent-mesh locate: not found: {args.message_id}", file=sys.stderr)
        return 1
    path, start, end = found
    print(f"{path}:{start}-{end}")
    return 0


def cmd_status(args: argparse.Namespace) -> int:
    config = load_config()
    _rebuild_all_locked(config)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        open_count = conn.execute(
            "SELECT COUNT(*) FROM messages WHERE kind='request' AND status='open'"
        ).fetchone()[0]
        response_count = conn.execute(
            "SELECT COUNT(*) FROM messages WHERE kind='response'"
        ).fetchone()[0]
        decision_count = conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0]
        last_seq = conn.execute("SELECT last_event_seq FROM events_seen").fetchone()[0]
    finally:
        conn.close()
    print(f"project: {config.project_root}")
    print(f"events: {last_seq}")
    print(f"open_requests: {open_count}")
    print(f"responses: {response_count}")
    print(f"decisions: {decision_count}")
    _ = args
    return 0


def cmd_workbench(args: argparse.Namespace) -> int:
    if args.workbench_mode == "service":
        return _cmd_workbench_service(args)
    if args.service_action is not None:
        print("agent-mesh: Workbench service action requires 'workbench service'", file=sys.stderr)
        return 2
    command = [
        sys.executable,
        "-m",
        "agent_mesh.workbench_runner",
        "--repo",
        str(args.repo),
        "--host",
        str(args.host),
        "--port",
        str(args.port if args.port is not None else 8765),
    ]
    if args.open:
        command.append("--open")
    if args.managed_service:
        command.append("--managed-service")
    if args.config_home is not None:
        command.extend(("--config-home", str(args.config_home)))
    os.execv(sys.executable, command)
    return 2  # pragma: no cover - os.execv replaces the process


def _cmd_workbench_service(args: argparse.Namespace) -> int:
    import webbrowser

    from agent_mesh.workbench import (
        WorkbenchError,
        _validate_workbench_host,
        managed_workbench_bookmark_path,
        write_managed_bookmark_pointer,
    )
    from agent_mesh.workbench_service import (
        WorkbenchServiceError,
        install_workbench_service,
        managed_workbench_authority,
        make_service_spec,
        relinquish_workbench_service,
        restart_workbench_service,
        start_workbench_service,
        uninstall_workbench_service,
        wait_for_managed_workbench,
        workbench_service_status,
    )

    action = args.service_action
    if action is None:
        print(
            "agent-mesh: choose a Workbench service action: "
            "install, repair, status, open, start, restart, relinquish, or uninstall",
            file=sys.stderr,
        )
        return 2

    bookmark_path: Path | None = None
    anchor_config: AgentMeshConfig | None = None
    try:
        if action in {"install", "repair"}:
            _validate_workbench_host(args.host)
            config = load_config(args.repo)
            anchor_config = config
            spec = make_service_spec(
                repo=config.project_root,
                host=args.host,
                port=args.port if args.port is not None else 8767,
            )
            authority = managed_workbench_authority(include_health=False)
            reactivating = authority.mode == "relinquished"
            if reactivating and action == "repair":
                raise WorkbenchServiceError(
                    "relinquished ownership must be reactivated with the explicit "
                    "service start or service install action"
                )
            if reactivating and not _confirm_workbench_reactivation(action):
                return 2
            register_project(config.project_root)
            if action == "repair":
                status = install_workbench_service(spec, repair=True)
            elif reactivating:
                status = install_workbench_service(
                    spec,
                    direct_human_reactivation=True,
                )
            else:
                status = install_workbench_service(spec)
            bookmark_path = managed_workbench_bookmark_path(spec.config_home)
        elif action == "status":
            status = workbench_service_status()
        elif action == "open":
            status = workbench_service_status()
            if not status.installed:
                raise WorkbenchServiceError(
                    "Workbench service is not installed; run "
                    "'agent-mesh workbench service install --repo . --open'"
                )
            if status.running is not True:
                status = start_workbench_service()
        elif action == "start":
            authority = managed_workbench_authority(include_health=False)
            reactivating = authority.mode == "relinquished"
            if reactivating and not _confirm_workbench_reactivation(action):
                return 2
            status = start_workbench_service(
                direct_human_reactivation=reactivating,
            )
        elif action == "restart":
            status = restart_workbench_service()
        elif action == "relinquish":
            if not _confirm_workbench_relinquish(action):
                return 2
            status = relinquish_workbench_service()
        else:
            if not _confirm_workbench_relinquish(action):
                return 2
            status = uninstall_workbench_service()
    except (ConfigError, ProjectRegistryError, WorkbenchServiceError, WorkbenchError) as exc:
        print(f"agent-mesh: {exc}", file=sys.stderr)
        return 2

    if bookmark_path is None:
        bookmark_path = _service_bookmark_path(status, managed_workbench_bookmark_path)
    _print_workbench_service_status(status, bookmark_path=bookmark_path)
    if action in {"install", "repair", "open", "start", "restart"}:
        if bookmark_path is not None:
            if not wait_for_managed_workbench(bookmark_path):
                print(
                    "agent-mesh: the automatic Workbench service was registered but did not "
                    "become reachable within 10 seconds; check service status and logs",
                    file=sys.stderr,
                )
                return 2
            if anchor_config is None:
                anchor_config = _service_anchor_config(status)
            _write_registered_managed_bookmark_pointers(
                bookmark_path,
                anchor_config=anchor_config,
                pointer_writer=lambda config, path: write_managed_bookmark_pointer(
                    config,
                    path,
                    _validated=True,
                ),
            )
            if args.open or action == "open":
                webbrowser.open(bookmark_path.resolve().as_uri())
        elif args.open:
            print(
                "agent-mesh: the service is registered, but its project bookmark could not "
                "be resolved from local metadata",
                file=sys.stderr,
            )
            return 2
    return 0


def _confirm_workbench_relinquish(action: str) -> bool:
    prompt = (
        f"{action} disables automatic Workbench ownership and restores manual server access. "
        "Type RELINQUISH to continue: "
    )
    try:
        answer = input(prompt)
    except (EOFError, KeyboardInterrupt):
        print("agent-mesh: Workbench ownership was not changed", file=sys.stderr)
        return False
    if answer.strip() != "RELINQUISH":
        print("agent-mesh: Workbench ownership was not changed", file=sys.stderr)
        return False
    return True


def _confirm_workbench_reactivation(action: str) -> bool:
    prompt = (
        f"{action} will reclaim managed Workbench ownership after explicit relinquishment. "
        "Type ACTIVATE to continue: "
    )
    try:
        answer = input(prompt)
    except (EOFError, KeyboardInterrupt):
        print("agent-mesh: Workbench ownership was not changed", file=sys.stderr)
        return False
    if answer.strip() != "ACTIVATE":
        print("agent-mesh: Workbench ownership was not changed", file=sys.stderr)
        return False
    return True


def _service_bookmark_path(status: Any, resolver: Any) -> Path | None:
    if not status.metadata:
        return None
    raw_config_home = status.metadata.get("config_home")
    if not isinstance(raw_config_home, str) or not raw_config_home:
        return None
    try:
        return resolver(Path(raw_config_home))
    except OSError:
        return None


def _service_anchor_config(status: Any) -> AgentMeshConfig | None:
    if not status.metadata:
        return None
    raw_repo = status.metadata.get("repo")
    if not isinstance(raw_repo, str) or not raw_repo:
        return None
    try:
        return load_config(Path(raw_repo))
    except ConfigError:
        return None


def _write_registered_managed_bookmark_pointers(
    bookmark_path: Path,
    *,
    anchor_config: AgentMeshConfig | None,
    pointer_writer: Any,
) -> None:
    """Route every valid registered repo bookmark to one managed bookmark."""

    from agent_mesh.workbench_service import (
        MAX_REGISTERED_POINTERS,
        REGISTERED_POINTER_DEADLINE_SECONDS,
        _managed_bookmark_target,
    )

    configs: dict[Path, AgentMeshConfig] = {}
    deadline = time.monotonic() + REGISTERED_POINTER_DEADLINE_SECONDS
    if _managed_bookmark_target(bookmark_path) is None:
        print(
            "agent-mesh: warning: managed bookmark validation failed; project pointers were "
            "not changed",
            file=sys.stderr,
        )
        return
    if time.monotonic() >= deadline:
        print(
            "agent-mesh: warning: managed bookmark validation reached the pointer time bound; "
            "project pointers were not changed",
            file=sys.stderr,
        )
        return
    if anchor_config is not None:
        configs[anchor_config.project_root.resolve()] = anchor_config
    try:
        projects = list_registered_projects_bounded(
            max_entries=MAX_REGISTERED_POINTERS,
            deadline=deadline,
        )
    except ProjectRegistryError as exc:
        print(
            f"agent-mesh: warning: could not enumerate registered project bookmarks: {exc}",
            file=sys.stderr,
        )
        projects = []
    for project in projects:
        if time.monotonic() >= deadline:
            print(
                "agent-mesh: warning: registered project bookmark update reached its time bound; "
                "remaining pointers were not changed",
                file=sys.stderr,
            )
            break
        root = project.root.resolve()
        if root in configs:
            continue
        if len(configs) >= MAX_REGISTERED_POINTERS:
            print(
                "agent-mesh: warning: registered project bookmark update reached its entry "
                f"bound ({MAX_REGISTERED_POINTERS}); remaining pointers were not changed",
                file=sys.stderr,
            )
            break
        try:
            configs[root] = load_config(root)
        except ConfigError as exc:
            print(
                f"agent-mesh: warning: could not load registered project {project.key}: {exc}",
                file=sys.stderr,
            )
    for root in sorted(configs, key=str):
        if time.monotonic() >= deadline:
            print(
                "agent-mesh: warning: registered project bookmark update reached its time bound; "
                "remaining pointers were not changed",
                file=sys.stderr,
            )
            break
        try:
            pointer_writer(configs[root], bookmark_path)
        except OSError as exc:
            print(
                f"agent-mesh: warning: could not update project bookmark for {root}: {exc}",
                file=sys.stderr,
            )


def _print_workbench_service_status(
    status: Any,
    *,
    bookmark_path: Path | None = None,
) -> None:
    if not status.installed:
        summary = "not installed"
    elif status.running is True:
        summary = "running"
    elif status.running is False:
        summary = status.state
    else:
        summary = status.state
    print(f"workbench service: {summary}")
    if status.api_state is not None:
        print(f"api: {status.api_state}")
    if status.ownership is not None:
        print(f"ownership: {status.ownership.state}")
        if status.ownership.generation:
            print(f"ownership generation: {status.ownership.generation}")
            print(f"ownership revision: {status.ownership.ownership_revision}")
    print(f"platform: {status.platform}")
    print(f"definition: {status.definition}")
    if status.metadata:
        print(f"repo: {status.metadata.get('repo', '')}")
        print(f"url: http://{status.metadata.get('host', '')}:{status.metadata.get('port', '')}")
    if status.installed and bookmark_path is not None:
        print(f"bookmark: {bookmark_path}")
        print("open: agent-mesh workbench service open")


def cmd_decision_propose(args: argparse.Namespace) -> int:
    config = load_config()
    if not DECISION_ID_RE.fullmatch(args.human_id):
        raise ConfigError("decision ID must look like D001, D038-S1, or D076-E")
    dec_ulid = _new_decision_id()
    body = _decision_body_from_args(args)
    verification = normalize_decision_verification(args.verify_command, reject_unsafe=True)
    try:
        assumptions = normalize_decision_assumptions(args.assumption)
        evidence = parse_decision_evidence_entries(args.evidence)
        review_policy = normalize_decision_review_policy(
            {
                "required_reviewers": args.required_reviewer,
                "approval_quorum": args.approval_quorum,
            },
            participants=config.decision_approval_identities,
        )
    except ValueError as exc:
        raise ConfigError(str(exc)) from exc
    applicability_scope = applicability_scope_for(args.applicability_scope, args.affects)
    applicability_issues = decision_applicability_issues(
        applicability_scope=applicability_scope,
        tier=args.tier,
        affected_code_globs=args.affects,
        exemptions=args.exempt,
        generated_artifact_paths=args.generated_artifact_paths,
    )
    if applicability_issues:
        raise ConfigError("; ".join(applicability_issues))
    lock_handle = acquire(config.agent_dir / ".mail-lock")
    last_event_seq = None
    try:
        recover(config.events_path, config.agent_dir)
        rebuild_all(config)
        conn = connect(config.db_path)
        try:
            initialize_schema(conn)
            validate_decision_proposal_identity(
                conn,
                args.human_id,
                dec_ulid=dec_ulid,
            )
        finally:
            conn.close()

        body_path, body_sha, body_bytes = _write_body(config, body)
        payload = {
            "decision_contract_version": 5,
            "human_id": args.human_id,
            "aliases": [],
            "title": args.title,
            "tier": args.tier,
            "applicability_scope": applicability_scope,
            "context": args.context,
            "decision": args.decision,
            "rejected_alternatives": [],
            "consequences": [],
            "affected_code_globs": normalize_decision_strings(args.affects),
            "exemptions": normalize_decision_strings(args.exempt),
            "generated_artifact_paths": normalize_decision_strings(args.generated_artifact_paths),
            "assumptions": assumptions,
            "evidence": evidence,
            "supersedes": None,
            "owner": args.owner,
            "review_policy": review_policy,
            "required_checks": normalize_decision_strings(args.required_check),
            "verification": verification,
            "tags": normalize_decision_strings(args.tag),
            "body_sha": body_sha,
            "body_path": body_path,
            "body_bytes": body_bytes,
            "body_format": (
                DECISION_BODY_FORMAT_CUSTOM
                if args.from_file is not None
                else DECISION_BODY_FORMAT_GENERATED_V1
            ),
        }
        event = Event(
            event_id=generate_event_id(),
            actor=_authoring_actor(config),
            kind="decision_proposed",
            entity_id=dec_ulid,
            thread_id=dec_ulid,
            payload=payload,
        )
        result = append_event(config.events_path, event, lock_acquired=True)
        last_event_seq = result.event.event_seq
    finally:
        lock_handle.release(last_event_seq=last_event_seq)
    print(f"{args.human_id} {dec_ulid}")
    return 0


def cmd_decision_amend(args: argparse.Namespace) -> int:
    config = load_config()
    reason = args.reason.strip()
    if not reason:
        raise ConfigError("decision amendment requires a non-empty reason")
    _reject_conflicting_clear_flag(args, "owner")
    _reject_conflicting_clear_flag(args, "affects")
    _reject_conflicting_clear_flag(args, "exemptions", value_name="exempt")
    _reject_conflicting_clear_flag(args, "generated_artifact_paths")
    _reject_conflicting_clear_flag(args, "required_checks", value_name="required_check")
    _reject_conflicting_clear_flag(args, "verification", value_name="verify_command")
    _reject_conflicting_clear_flag(args, "tags", value_name="tag")
    _reject_conflicting_clear_flag(args, "assumptions", value_name="assumption")
    _reject_conflicting_clear_flag(args, "evidence")
    if args.clear_review_policy and (
        args.required_reviewer is not None or args.approval_quorum is not None
    ):
        raise ConfigError(
            "--clear-review-policy cannot be combined with --required-reviewer or --approval-quorum"
        )
    if args.approval_quorum is not None and args.required_reviewer is None:
        raise ConfigError(
            "--approval-quorum requires the full replacement reviewer set via --required-reviewer"
        )

    lock_handle = acquire(config.agent_dir / ".mail-lock")
    last_event_seq = None
    human_id = args.identifier
    did_update = False
    try:
        recover(config.events_path, config.agent_dir)
        rebuild_all(config)
        current = _decision_metadata_snapshot(config, args.identifier)
        human_id = current["human_id"]
        status = current["status"]
        if status in {"superseded", "retired", "rejected"}:
            raise ConfigError(
                f"{human_id} is {status}; create a successor decision instead of amending it"
            )

        fields_changed: dict[str, list[Any]] = {}
        for field_name in ("title", "tier", "context", "decision"):
            requested = getattr(args, field_name)
            if requested is None:
                continue
            normalized = requested.strip()
            if field_name in {"title", "tier"} and not normalized:
                raise ConfigError(f"decision {field_name} must not be empty")
            if normalized != current[field_name]:
                fields_changed[field_name] = [current[field_name], normalized]

        requested_owner = "" if args.clear_owner else args.owner
        if requested_owner is not None:
            normalized_owner = requested_owner.strip()
            if normalized_owner != current["owner"]:
                fields_changed["owner"] = [current["owner"], normalized_owner]

        if (
            args.applicability_scope is not None
            and args.applicability_scope != current["applicability_scope"]
        ):
            fields_changed["applicability_scope"] = [
                current["applicability_scope"],
                args.applicability_scope,
            ]

        collection_requests = (
            (
                "affected_code_globs",
                [] if args.clear_affects else args.affects,
                normalize_decision_strings,
            ),
            (
                "required_checks",
                [] if args.clear_required_checks else args.required_check,
                normalize_decision_strings,
            ),
            (
                "exemptions",
                [] if args.clear_exemptions else args.exempt,
                normalize_decision_strings,
            ),
            (
                "generated_artifact_paths",
                ([] if args.clear_generated_artifact_paths else args.generated_artifact_paths),
                normalize_decision_strings,
            ),
            (
                "verification",
                [] if args.clear_verification else args.verify_command,
                lambda values: normalize_decision_verification(values, reject_unsafe=True),
            ),
            (
                "assumptions",
                [] if args.clear_assumptions else args.assumption,
                normalize_decision_assumptions,
            ),
            (
                "evidence",
                [] if args.clear_evidence else args.evidence,
                parse_decision_evidence_entries,
            ),
            ("tags", [] if args.clear_tags else args.tag, normalize_decision_strings),
        )
        for field_name, requested, normalizer in collection_requests:
            if requested is None:
                continue
            try:
                normalized_values = normalizer(requested)
            except ValueError as exc:
                raise ConfigError(str(exc)) from exc
            if normalized_values != current[field_name]:
                fields_changed[field_name] = [current[field_name], normalized_values]

        if args.clear_review_policy:
            requested_review_policy: dict[str, Any] | None = {}
        elif args.required_reviewer is not None:
            try:
                requested_review_policy = normalize_decision_review_policy(
                    {
                        "required_reviewers": args.required_reviewer,
                        "approval_quorum": args.approval_quorum,
                    },
                    participants=config.decision_approval_identities,
                )
            except ValueError as exc:
                raise ConfigError(str(exc)) from exc
        else:
            requested_review_policy = None
        if (
            requested_review_policy is not None
            and requested_review_policy != current["review_policy"]
        ):
            fields_changed["review_policy"] = [
                current["review_policy"],
                requested_review_policy,
            ]

        prospective_scope = fields_changed.get(
            "applicability_scope", [None, current["applicability_scope"]]
        )[1]
        prospective_tier = fields_changed.get("tier", [None, current["tier"]])[1]
        prospective_globs = fields_changed.get(
            "affected_code_globs", [None, current["affected_code_globs"]]
        )[1]
        prospective_exemptions = fields_changed.get("exemptions", [None, current["exemptions"]])[1]
        prospective_generated = fields_changed.get(
            "generated_artifact_paths", [None, current["generated_artifact_paths"]]
        )[1]
        applicability_issues = decision_applicability_issues(
            applicability_scope=prospective_scope,
            tier=str(prospective_tier),
            affected_code_globs=prospective_globs,
            exemptions=prospective_exemptions,
            generated_artifact_paths=prospective_generated,
        )
        if applicability_issues:
            raise ConfigError("; ".join(applicability_issues))

        canonical_fields_requested = any(
            getattr(args, field_name) is not None for field_name in ("title", "context", "decision")
        )
        if args.from_file is not None:
            body = args.from_file.read_text(encoding="utf-8")
        elif canonical_fields_requested:
            body_missing = not current["body_path"] and int(current["body_bytes"] or 0) == 0
            if current["body_format"] != DECISION_BODY_FORMAT_GENERATED_V1 and not body_missing:
                raise ConfigError(
                    f"{human_id} has custom canonical Markdown; amend title, context, or "
                    "decision with --from-file so the metadata and reviewed body change together"
                )
            body = generated_decision_body(
                human_id,
                title=str(fields_changed.get("title", [None, current["title"]])[1]),
                context=str(fields_changed.get("context", [None, current["context"]])[1]),
                decision=str(fields_changed.get("decision", [None, current["decision"]])[1]),
            )
        else:
            body = current["body"]

        body_changed = body != current["body"]
        if body_changed:
            body_path, body_sha, body_bytes = _write_body(config, body)
            body_format = (
                DECISION_BODY_FORMAT_CUSTOM
                if args.from_file is not None
                else DECISION_BODY_FORMAT_GENERATED_V1
            )
            fields_changed.update(
                {
                    "body_path": [current["body_path"], body_path],
                    "body_sha": [current["body_sha"], body_sha],
                    "body_bytes": [current["body_bytes"], body_bytes],
                    "body_format": [current["body_format"], body_format],
                }
            )

        if fields_changed:
            if status in {"accepted", "in_force"}:
                fields_changed["status"] = [status, "proposed"]
            event = Event(
                event_id=generate_event_id(),
                actor=_authoring_actor(config),
                kind="decision_metadata_updated",
                entity_id=current["dec_ulid"],
                thread_id=current["dec_ulid"],
                payload={
                    "decision_contract_version": 5,
                    "decision_id": current["dec_ulid"],
                    "reason": reason,
                    "change_kind": (
                        "content_revision"
                        if body_changed or status in {"accepted", "in_force"}
                        else "content_update"
                    ),
                    "fields_changed": fields_changed,
                },
            )
            result = append_event(config.events_path, event, lock_acquired=True)
            last_event_seq = result.event.event_seq
            did_update = True
            rebuild_all(config)
            render_all(config)
        else:
            last_event_seq = current["event_seq"]
    finally:
        lock_handle.release(last_event_seq=last_event_seq)

    if did_update:
        updated = _decision_metadata_snapshot(config, human_id)
        print(f"amended {human_id}; status={updated['status']}")
    else:
        print(f"unchanged {human_id}")
    return 0


def cmd_decision_accept(args: argparse.Namespace) -> int:
    config = load_config()
    approver = args.by.strip()
    notes = args.notes.strip()
    _ensure_participant(config, approver, role="approving human")
    if approver not in config.decision_approval_identities:
        raise ConfigError(
            f"DECISION_APPROVER_UNAUTHORIZED: {approver!r} is not in the configured "
            "direct-human approval authority"
        )
    if not notes:
        raise ConfigError("decision acceptance requires a non-empty approval note")
    if not sys.stdin.isatty():
        raise ConfigError(
            "decision acceptance requires direct human action in an interactive terminal; "
            "use the Workbench Approve and accept control instead"
        )

    rebuild_all(config)
    preview = _decision_acceptance_snapshot(config, args.identifier)
    if preview["status"] in {"accepted", "in_force"}:
        print(f"already {preview['status']} {preview['human_id']}")
        return 0
    if preview["status"] != "proposed":
        raise ConfigError(
            f"cannot accept {preview['human_id']} while status is {preview['status']}"
        )
    required_reviewers = preview["review_policy"].get("required_reviewers", [])
    if required_reviewers and approver not in required_reviewers:
        raise ConfigError(f"approver {approver!r} is not a required reviewer")
    if approver in preview["review_progress"]["approved_reviewers"]:
        print(f"already approved by {approver} for {preview['human_id']}")
        return 0
    _require_complete_decision(preview)

    confirmation = f"ACCEPT {preview['human_id']}"
    print(f"Decision: {preview['human_id']} - {preview['title']}")
    print(f"Body SHA-256: {preview['body_sha']}")
    print(f"Revision SHA-256: {preview['revision_sha']}")
    print(f"Tier: {preview['tier']}")
    print(f"Owner: {preview['owner']}")
    print(f"Applicability scope: {preview['applicability_scope']}")
    for pattern in preview["affected_code_globs"]:
        print(f"Affected code glob: {pattern}")
    for pattern in preview["exemptions"]:
        print(f"Exemption glob: {pattern}")
    for pattern in preview["generated_artifact_paths"]:
        print(f"Generated-artifact glob: {pattern}")
    for check in preview["required_checks"]:
        print(f"Required check: {check}")
    for verification in preview["verification"]:
        print(f"Verification command: {verification['command']}")
    for assumption in preview["assumptions"]:
        print(f"Assumption {assumption['id']}: {assumption['text']}")
        for reference in assumption.get("references", []):
            print(f"Assumption {assumption['id']} reference: {reference}")
    for kind, references in preview["evidence"].items():
        for reference in references:
            print(f"Evidence {kind}: {reference}")
    if preview["review_policy"]:
        print("Required reviewers: " + ", ".join(preview["review_policy"]["required_reviewers"]))
        print(f"Approval quorum: {preview['review_policy']['approval_quorum']}")
    print(f"Approver: {approver}")
    print(f"Approval note: {notes}")
    if input(f"Type {confirmation} to approve this exact revision: ").strip() != confirmation:
        print("decision acceptance cancelled", file=sys.stderr)
        return 1

    lock_handle = acquire(config.agent_dir / ".mail-lock")
    last_event_seq = None
    try:
        recover(config.events_path, config.agent_dir)
        rebuild_all(config)
        current = _decision_acceptance_snapshot(config, args.identifier)
        if current["status"] != "proposed":
            raise ConfigError(
                f"cannot accept {current['human_id']} while status is {current['status']}"
            )
        _require_complete_decision(current)
        if (
            current["dec_ulid"] != preview["dec_ulid"]
            or current["revision_sha"] != preview["revision_sha"]
        ):
            raise ConfigError(
                "decision changed after it was shown for approval; review the current revision "
                "and try again"
            )
        approved_utc = utc_now()
        event = Event(
            event_id=generate_event_id(),
            occurred_utc=approved_utc,
            actor=approver,
            kind="decision_accepted",
            entity_id=current["dec_ulid"],
            thread_id=current["dec_ulid"],
            payload={
                "decision_contract_version": 5,
                "decision_id": current["dec_ulid"],
                "accepted_by": approver,
                "notes": notes,
                "approval_source": "interactive_cli",
                "approved_utc": approved_utc,
                "approved_body_sha": current["body_sha"],
                "approved_revision_sha": current["revision_sha"],
                "approval_authority_mode": config.decision_approval_authority_mode,
                "approval_authority_revision": (config.decision_approval_authority_revision),
            },
        )
        result = append_event(config.events_path, event, lock_acquired=True)
        last_event_seq = result.event.event_seq
        rebuild_all(config)
        render_all(config)
    finally:
        lock_handle.release(last_event_seq=last_event_seq)
    print(f"accepted {preview['human_id']}")
    return 0


def cmd_decision_revisit(args: argparse.Namespace) -> int:
    config = load_config()
    dec_ulid = _resolve_decision_or_die(config, args.identifier)
    payload = {
        "decision_id": dec_ulid,
        "reason": args.reason,
        "assumption_id": args.assumption,
        "new_decision_id": args.new_decision_id,
    }
    event = Event(
        event_id=generate_event_id(),
        actor=_authoring_actor(config),
        kind="decision_revisited",
        entity_id=dec_ulid,
        thread_id=dec_ulid,
        payload=payload,
    )
    append_event(config.events_path, event, lock_acquired=False)
    print(f"revisited {args.identifier}")
    return 0


def cmd_decision_supersede(args: argparse.Namespace) -> int:
    config = load_config()
    old_ulid = _resolve_decision_or_die(config, args.old_id)
    new_ulid = _resolve_decision_or_die(config, args.new_id)
    event = Event(
        event_id=generate_event_id(),
        actor=_authoring_actor(config),
        kind="decision_superseded",
        entity_id=old_ulid,
        thread_id=old_ulid,
        payload={
            "decision_id": old_ulid,
            "superseded_by": new_ulid,
            "migration_notes": args.migration_notes,
        },
    )
    append_event(config.events_path, event, lock_acquired=False)
    print(f"superseded {args.old_id} by {args.new_id}")
    return 0


def cmd_decision_retire(args: argparse.Namespace) -> int:
    config = load_config()
    dec_ulid = _resolve_decision_or_die(config, args.identifier)
    event = Event(
        event_id=generate_event_id(),
        actor=_authoring_actor(config),
        kind="decision_retired",
        entity_id=dec_ulid,
        thread_id=dec_ulid,
        payload={"decision_id": dec_ulid, "reason": args.reason},
    )
    append_event(config.events_path, event, lock_acquired=False)
    print(f"retired {args.identifier}")
    return 0


BACKLOG_PAYLOAD_FIELDS = {
    "id",
    "title",
    "item_type",
    "summary",
    "root_cause_summary",
    "architectural_category",
    "status",
    "priority",
    "launch_scope",
    "release_phase",
    "production_state",
    "disposition",
    "owner_hint",
    "owner_instance_id",
    "lane",
    "notes",
    "workflow_origin",
    "refs",
}


def cmd_backlog_upsert(args: argparse.Namespace) -> int:
    config = load_config()
    actor = _authoring_actor(config, explicit_actor=args.actor)
    _ensure_participant(config, actor, role="actor")
    json_payload = _read_optional_json_payload(args)
    write_mode = getattr(args, "backlog_write_mode", "upsert")
    requested_item_id = (getattr(args, "item_id", None) or str(json_payload.get("id", ""))).strip()
    mint_item_id = write_mode == "create" or (write_mode == "upsert" and not requested_item_id)
    if write_mode == "create" and json_payload.get("id"):
        raise ConfigError("backlog create allocates its ID; remove JSON field id")
    if write_mode == "update" and not requested_item_id:
        raise ConfigError("backlog update requires an item ID")
    if mint_item_id and not (args.title or str(json_payload.get("title", "")).strip()):
        raise ConfigError("backlog creation requires --title or JSON field title")

    lock_handle = acquire(config.agent_dir / ".mail-lock")
    last_event_seq = None
    try:
        recover(config.events_path, config.agent_dir)
        rebuild_all(config)
        json_owner_instance = json_payload.get("owner_instance_id")
        owner_instance_ref = (getattr(args, "owner_instance", None) or "").strip()
        if not owner_instance_ref and json_owner_instance is not None:
            owner_instance_ref = str(json_owner_instance).strip()
        args.owner_instance_id = None
        if owner_instance_ref:
            owner_instance = _agent_instance_snapshot(
                config, owner_instance_ref, require_active=True
            )
            args.owner_instance_id = str(owner_instance["id"])
            json_payload["owner_instance_id"] = args.owner_instance_id
        occurred_utc = utc_now()
        item_id = (
            _allocate_backlog_id(config, occurred_utc=occurred_utc)
            if mint_item_id
            else requested_item_id
        )
        existing = _backlog_item_payload_from_projection(config, item_id)
        if write_mode == "update" and existing is None:
            raise ConfigError(f"backlog item not found: {item_id}")
        if write_mode == "upsert" and requested_item_id and existing is not None:
            raise ConfigError(
                f"{BACKLOG_ID_COLLISION}: {item_id} already exists; "
                f"use backlog update {item_id} for an intentional change"
            )
        if mint_item_id:
            write_intent = "create"
        elif write_mode == "update":
            write_intent = "update"
        else:
            write_intent = "create"
        payload = _merge_backlog_payload(existing, json_payload, args, item_id=item_id)
        if not payload.get("title"):
            raise ConfigError(
                f"backlog upsert requires --title for new item {item_id}; "
                "partial updates are only allowed after the item exists"
            )
        payload["write_intent"] = write_intent
        event = Event(
            event_id=generate_event_id(),
            occurred_utc=occurred_utc,
            actor=actor,
            kind="backlog_item_upserted",
            entity_id=item_id,
            thread_id=item_id,
            payload=payload,
        )
        result = append_event(config.events_path, event, lock_acquired=True)
        last_event_seq = result.event.event_seq
    finally:
        lock_handle.release(last_event_seq=last_event_seq)
    if write_mode == "upsert":
        action = "upserted"
    else:
        action = "created" if write_intent == "create" else "updated"
    print(f"{action} {item_id}")
    return 0


def cmd_backlog_refer(args: argparse.Namespace) -> int:
    source_config = load_config()
    actor = _authoring_actor(source_config, explicit_actor=args.actor)
    _ensure_participant(source_config, actor, role="actor")
    source_project = _registered_project_for_config(source_config)
    target_project = resolve_registered_project(args.target_project)
    target_config = load_config(target_project.root)
    if source_project.id == target_project.id:
        raise ConfigError(
            "source and target are the same registered repo; update the local backlog item instead"
        )

    source_item = _backlog_item_payload(source_config, args.source_item_id)
    if source_item is None:
        raise ConfigError(f"source backlog item not found: {args.source_item_id}")
    reason = args.reason.strip()
    if not reason:
        raise ConfigError("cross-repository referral requires a non-empty ownership reason")

    referral_id = _backlog_referral_id(
        source_project.id,
        args.source_item_id,
        target_project.id,
    )
    source_qualified = f"{source_project.key}:{args.source_item_id}"
    proposed_title = (args.title or str(source_item.get("title") or "")).strip()
    if not proposed_title:
        raise ConfigError("target backlog title must not be empty")
    existing_target = _find_referral_target(target_config, referral_id)
    duplicate_candidates = _find_backlog_title_duplicates(
        target_config,
        proposed_title,
        exclude=existing_target,
    )
    _print_backlog_referral_plan(
        referral_id=referral_id,
        source_project=source_project,
        source_item=source_item,
        target_project=target_project,
        title=proposed_title,
        reason=reason,
        blocked=args.blocks_current_work,
        risk=args.risk,
        existing_target=existing_target,
        duplicate_candidates=duplicate_candidates,
        apply=args.apply,
        record_only=args.record_only,
    )

    if not args.apply and not args.record_only:
        if duplicate_candidates and not args.use_existing:
            choices = ", ".join(
                f"{target_project.key}:{item_id}" for item_id in duplicate_candidates
            )
            print(
                "requested action: review the possible duplicate(s) "
                f"{choices}, then rerun with --use-existing TARGET-BKL-ID --apply "
                "or change the proposed title"
            )
        else:
            print(
                "requested action: if this agent has explicit write scope for both repos, "
                "rerun with --apply; otherwise rerun with --record-only and ask for target scope"
            )
        return 0

    intent_event_id = _ensure_source_referral_intent(
        source_config,
        source_item_id=args.source_item_id,
        actor=actor,
        referral_id=referral_id,
        source_qualified=source_qualified,
        target_project=target_project,
        reason=reason,
        blocked=args.blocks_current_work,
        risk=args.risk,
    )

    if args.record_only:
        _record_source_referral_status(
            source_config,
            source_item_id=args.source_item_id,
            actor=actor,
            referral_id=referral_id,
            event_type="backlog_referral_pending",
            details={
                "reason_code": "target_write_scope_absent",
                "target_project": target_project.key,
                "target_store_id": target_project.id,
            },
        )
        print(
            "pending source receipt recorded; requested action: grant an agent write scope "
            f"for {target_project.key} and rerun this referral with --apply"
        )
        return 0

    if existing_target and args.use_existing and args.use_existing != existing_target:
        raise ConfigError(
            f"referral already resolves to {target_project.key}:{existing_target}; "
            "do not select a second target item"
        )
    if duplicate_candidates and not existing_target and not args.use_existing:
        _record_source_referral_status(
            source_config,
            source_item_id=args.source_item_id,
            actor=actor,
            referral_id=referral_id,
            event_type="backlog_referral_pending",
            details={
                "reason_code": "duplicate_review_required",
                "target_project": target_project.key,
                "duplicate_candidates": duplicate_candidates,
            },
        )
        choices = ", ".join(f"{target_project.key}:{item_id}" for item_id in duplicate_candidates)
        raise ConfigError(
            f"possible target duplicate(s): {choices}. Requested action: review them and rerun "
            "with --use-existing TARGET-BKL-ID --apply, or change the proposed title"
        )

    if args.use_existing:
        _require_target_backlog_item(target_config, args.use_existing)
        target_item_id = args.use_existing
    else:
        target_item_id = existing_target

    try:
        target_actor, target_instance_handle = _cross_project_authoring_actor(
            source_config, target_config
        )
        target_instance_token = (
            bind_agent_instance(target_instance_handle) if target_instance_handle else None
        )
        try:
            if target_item_id is None:
                target_item_id = _promote_referral_to_target(
                    target_config,
                    actor=target_actor,
                    source_project=source_project,
                    source_item=source_item,
                    source_qualified=source_qualified,
                    target_project=target_project,
                    referral_id=referral_id,
                    intent_event_id=intent_event_id,
                    title=proposed_title,
                    summary=(args.summary or str(source_item.get("summary") or "")).strip(),
                    priority=(args.priority or str(source_item.get("priority") or "")).strip(),
                    reason=reason,
                    source_events=args.source_event,
                    extra_refs=args.ref,
                )
            else:
                _link_existing_target_referral(
                    target_config,
                    actor=target_actor,
                    target_item_id=target_item_id,
                    source_project=source_project,
                    source_qualified=source_qualified,
                    referral_id=referral_id,
                    intent_event_id=intent_event_id,
                )
        finally:
            if target_instance_token is not None:
                reset_agent_instance(target_instance_token)
    except Exception as exc:
        _record_source_referral_status(
            source_config,
            source_item_id=args.source_item_id,
            actor=actor,
            referral_id=referral_id,
            event_type="backlog_referral_pending",
            details={
                "reason_code": "target_write_failed",
                "target_project": target_project.key,
                "error": str(exc),
            },
        )
        raise ConfigError(
            f"target promotion did not complete: {exc}. The source intent is durable. "
            f"Requested action: grant or repair write scope for {target_project.key}, then rerun "
            "the same command with --apply"
        ) from exc

    _complete_source_referral(
        source_config,
        source_item_id=args.source_item_id,
        actor=actor,
        referral_id=referral_id,
        target_project=target_project,
        target_item_id=target_item_id,
    )
    print(f"promoted {source_qualified} -> {target_project.key}:{target_item_id} ({referral_id})")
    return 0


def cmd_backlog_link(args: argparse.Namespace) -> int:
    config = load_config()
    actor = _authoring_actor(config, explicit_actor=args.actor)
    _ensure_participant(config, actor, role="actor")
    lock_handle = acquire(config.agent_dir / ".mail-lock")
    last_event_seq = None
    try:
        if not args.allow_missing_item and _backlog_item_payload(config, args.item_id) is None:
            raise ConfigError(f"backlog item not found: {args.item_id}")
        event = Event(
            event_id=generate_event_id(),
            actor=actor,
            kind="backlog_link_added",
            entity_id=args.item_id,
            thread_id=args.item_id,
            payload={
                "item_id": args.item_id,
                "ref_type": args.ref_type,
                "ref_value": args.ref_value,
            },
        )
        result = append_event(config.events_path, event, lock_acquired=True)
        last_event_seq = result.event.event_seq
    finally:
        lock_handle.release(last_event_seq=last_event_seq)
    print(f"linked {args.item_id} {args.ref_type}:{args.ref_value}")
    return 0


def cmd_backlog_record(args: argparse.Namespace) -> int:
    config = load_config()
    actor = _authoring_actor(config, explicit_actor=args.actor)
    _ensure_participant(config, actor, role="actor")
    details = _details_payload_from_args(args)
    lock_handle = acquire(config.agent_dir / ".mail-lock")
    last_event_seq = None
    try:
        if _backlog_item_payload(config, args.item_id) is None:
            raise ConfigError(f"backlog item not found: {args.item_id}")
        event = Event(
            event_id=generate_event_id(),
            actor=actor,
            kind="backlog_event_recorded",
            entity_id=args.item_id,
            thread_id=args.item_id,
            payload={
                "item_id": args.item_id,
                "event_type": args.event_type,
                "actor": actor,
                "details": details,
            },
        )
        result = append_event(config.events_path, event, lock_acquired=True)
        last_event_seq = result.event.event_seq
    finally:
        lock_handle.release(last_event_seq=last_event_seq)
    print(f"recorded {args.event_type} for {args.item_id}")
    return 0


def cmd_check_refs(args: argparse.Namespace) -> int:
    config = load_config()
    if args.record_scan and args.read_stdin:
        raise ConfigError("--record-scan cannot be combined with --stdin")
    occurrences, paths, input_warnings, input_source_count = _check_reference_inputs(
        config,
        args,
    )
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
                boundary="check_refs",
            )
    except ReadModelUnavailable as exc:
        unavailable = build_unavailable_reference_context(
            boundary="check_refs",
            occurrence_count=len(occurrences),
            source_count=input_source_count,
            diagnostic=str(exc),
        )
        unavailable["scan"] = {
            "paths_scanned": [path.relative_to(config.project_root).as_posix() for path in paths],
            "input_warnings": input_warnings,
        }
        if args.json:
            rendered, _ = render_bounded_reference_context_json(unavailable)
            print(rendered)
        else:
            print(f"agent-mesh check refs: canonical state unavailable: {exc}", file=sys.stderr)
        return 3

    result["scan"] = {
        "paths_scanned": [path.relative_to(config.project_root).as_posix() for path in paths],
        "input_warnings": input_warnings,
    }
    if result.get("complete") is not True:
        rendered, _ = render_bounded_reference_context_json(result)
        if args.json:
            print(rendered)
        else:
            for diagnostic in result.get("diagnostics", []):
                print(f"agent-mesh check refs: {diagnostic}", file=sys.stderr)
        return 3

    dangling, warnings, partial_count = _reference_check_findings(
        result,
        input_warnings=input_warnings,
    )

    if args.record_scan:
        scanner_run_id = "scan_" + secrets.token_hex(8)
        event = Event(
            event_id=generate_event_id(),
            actor=_authoring_actor(config),
            kind="decision_scanner_run_completed",
            entity_id=scanner_run_id,
            thread_id=scanner_run_id,
            payload={
                "scanner_run_id": scanner_run_id,
                "paths_scanned": [
                    path.relative_to(config.project_root).as_posix() for path in paths
                ],
                "dangling_refs": dangling,
                "legacy_warnings": warnings,
                "duration_ms": 0,
            },
        )
        append_event(config.events_path, event, lock_acquired=False)

    if args.json:
        rendered, complete = render_bounded_reference_context_json(result)
        print(rendered)
        if not complete:
            return 3
    else:
        resolution_warning_count = sum(
            1
            for resolution in result.get("resolutions", [])
            for warning in resolution.get("warnings", [])
            if resolution.get("resolution_status") in {"resolved", "partial"}
            and not (
                resolution.get("resolution_status") == "partial"
                and str(warning).startswith("decision fragment is unvalidated")
            )
        )
        summary = (
            f"refs: scanned={len(paths) + int(args.read_stdin)} "
            f"resolved={result['resolution_counts']['resolved']} "
            f"partial={partial_count} "
            f"dangling={len(dangling)} "
            f"warnings={len(warnings) + resolution_warning_count}"
        )
        stdout_text, stderr_text, text_complete = _render_reference_check(
            result,
            dangling=dangling,
            warnings=warnings,
            summary=summary,
        )
        if not text_complete:
            print(
                "agent-mesh check refs: reference context exceeds the "
                f"{MAX_REFERENCE_CONTEXT_TEXT_BYTES}-byte text output bound; "
                "narrow the input set or use --json",
                file=sys.stderr,
            )
            return 3
        sys.stdout.write(stdout_text)
        sys.stderr.write(stderr_text)
    return 1 if dangling or partial_count else 0


def _check_reference_inputs(
    config: AgentMeshConfig,
    args: argparse.Namespace,
) -> tuple[list[ReferenceOccurrence], list[Path], list[dict[str, Any]], int]:
    paths = _explicit_reference_paths(config, args.file) if args.file else []
    if not args.file and not args.read_stdin:
        paths = _tracked_paths(config, args)
    occurrences: list[ReferenceOccurrence] = []
    warnings: list[dict[str, Any]] = []
    total_bytes = 0
    for path in paths:
        relative = path.relative_to(config.project_root).as_posix()
        if not args.file and config.checks.is_exempt(relative):
            continue
        text, warning = _read_reference_check_file(config, path, relative=relative)
        if warning is not None:
            if args.file:
                raise ConfigError(
                    f"cannot scan explicit reference file {relative}: {warning['reason']}"
                )
            warnings.append(warning)
            continue
        if text is None:
            raise ConfigError(f"reference input produced no text or warning: {relative}")
        total_bytes += len(text.encode("utf-8"))
        if total_bytes > MAX_REFERENCE_CHECK_INPUT_BYTES:
            raise ConfigError(
                f"reference scan exceeds {MAX_REFERENCE_CHECK_INPUT_BYTES} UTF-8 bytes; "
                "narrow --paths or --file inputs"
            )
        occurrences.extend(
            extract_reference_occurrences(
                text,
                source=relative,
                source_kind="file",
                max_occurrences=max(
                    0,
                    MAX_REFERENCE_CONTEXT_OCCURRENCES + 1 - len(occurrences),
                ),
            )
        )
    if args.read_stdin:
        text = _read_reference_check_stdin()
        total_bytes += len(text.encode("utf-8"))
        if total_bytes > MAX_REFERENCE_CHECK_INPUT_BYTES:
            raise ConfigError(
                f"reference scan exceeds {MAX_REFERENCE_CHECK_INPUT_BYTES} UTF-8 bytes"
            )
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
    return occurrences, paths, warnings, len(paths) + int(args.read_stdin)


def _explicit_reference_paths(config: AgentMeshConfig, requested: list[Path]) -> list[Path]:
    if len(requested) > MAX_REFERENCE_CHECK_PATHS:
        raise ConfigError(f"check refs accepts at most {MAX_REFERENCE_CHECK_PATHS} explicit files")
    root = config.project_root.resolve()
    paths: list[Path] = []
    for path in requested:
        if len(str(path).encode("utf-8")) > MAX_REFERENCE_CHECK_PATH_BYTES:
            raise ConfigError(
                f"reference input path exceeds {MAX_REFERENCE_CHECK_PATH_BYTES} UTF-8 bytes"
            )
        candidate = path if path.is_absolute() else root / path
        try:
            resolved = candidate.resolve(strict=True)
        except OSError as exc:
            raise ConfigError(f"cannot read reference file {path}: {exc}") from exc
        try:
            relative = resolved.relative_to(root)
        except ValueError as exc:
            raise ConfigError(
                f"reference file must remain inside the project root: {path}"
            ) from exc
        if not resolved.is_file():
            raise ConfigError(f"reference input is not a file: {path}")
        paths.append(config.project_root / relative)
    return sorted(set(paths))


def _read_reference_check_file(
    config: AgentMeshConfig,
    path: Path,
    *,
    relative: str,
) -> tuple[str | None, dict[str, Any] | None]:
    root = config.project_root.resolve()
    try:
        resolved = path.resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError):
        return None, {"file": relative, "line": None, "ref": "", "reason": "path escape"}
    try:
        with resolved.open("rb") as stream:
            raw_bytes = stream.read(MAX_REFERENCE_CHECK_FILE_BYTES + 1)
    except OSError as exc:
        return None, {
            "file": relative,
            "line": None,
            "ref": "",
            "reason": f"cannot read file: {exc}",
        }
    if len(raw_bytes) > MAX_REFERENCE_CHECK_FILE_BYTES:
        return None, {
            "file": relative,
            "line": None,
            "ref": "",
            "reason": f"file exceeds {MAX_REFERENCE_CHECK_FILE_BYTES} bytes",
        }
    try:
        return raw_bytes.decode("utf-8"), None
    except UnicodeDecodeError:
        return None, {
            "file": relative,
            "line": None,
            "ref": "",
            "reason": "file is not valid UTF-8 text",
        }


def _read_reference_check_stdin() -> str:
    if sys.stdin.isatty():
        raise ConfigError("--stdin requires piped UTF-8 text")
    binary_stream = getattr(sys.stdin, "buffer", None)
    if binary_stream is not None:
        raw_bytes = binary_stream.read(MAX_REFERENCE_CHECK_INPUT_BYTES + 1)
    else:
        raw_text = sys.stdin.read(MAX_REFERENCE_CHECK_INPUT_BYTES + 1)
        raw_bytes = raw_text.encode("utf-8")
    if len(raw_bytes) > MAX_REFERENCE_CHECK_INPUT_BYTES:
        raise ConfigError(f"stdin exceeds {MAX_REFERENCE_CHECK_INPUT_BYTES} UTF-8 bytes")
    try:
        return raw_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ConfigError("stdin must be valid UTF-8 text") from exc


def _reference_check_findings(
    result: dict[str, Any],
    *,
    input_warnings: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    dangling: list[dict[str, Any]] = []
    warnings = list(input_warnings)
    partial_count = 0
    occurrences_by_resolution: dict[int, list[dict[str, Any]]] = {}
    for occurrence in result.get("occurrences", []):
        occurrences_by_resolution.setdefault(int(occurrence["resolution_index"]), []).append(
            occurrence
        )
    for index, resolution in enumerate(result.get("resolutions", [])):
        state = str(resolution["resolution_status"])
        if state == "partial":
            for occurrence in occurrences_by_resolution.get(index, []):
                partial_count += 1
                warnings.append(
                    {
                        "ref": str(resolution["requested_id"]),
                        "file": str(occurrence["source"]),
                        "line": occurrence["line"],
                        "column": occurrence["column"],
                        "reason": "decision fragment is unvalidated; only the base resolved",
                    }
                )
            continue
        if state not in {"not_found", "unsupported"}:
            continue
        for occurrence in occurrences_by_resolution.get(index, []):
            finding = {
                "ref": str(resolution["requested_id"]),
                "file": str(occurrence["source"]),
                "line": occurrence["line"],
                "column": occurrence["column"],
            }
            if state == "not_found":
                dangling.append(finding)
            else:
                finding["reason"] = "reference kind has no canonical resolver"
                warnings.append(finding)
    return dangling, warnings, partial_count


def _render_reference_check(
    result: dict[str, Any],
    *,
    dangling: list[dict[str, Any]],
    warnings: list[dict[str, Any]],
    summary: str,
) -> tuple[str, str, bool]:
    output = BoundedReferenceText()
    occurrences_by_resolution: dict[int, list[dict[str, Any]]] = {}
    for occurrence in result.get("occurrences", []):
        occurrences_by_resolution.setdefault(int(occurrence["resolution_index"]), []).append(
            occurrence
        )
    for index, resolution in enumerate(result.get("resolutions", [])):
        if resolution["resolution_status"] not in {"resolved", "partial"}:
            continue
        canonical_id = _reference_check_single_line(resolution["canonical_id"])
        status = _reference_check_single_line(resolution["status"] or resolution["kind"])
        title = _reference_check_single_line(resolution["title"])
        revision = str(resolution.get("revision_sha256") or "")
        suffix = f" revision={revision[:12]}" if revision else ""
        fragment = _reference_check_single_line(resolution.get("fragment"))
        if fragment:
            suffix += f" fragment={fragment}:unvalidated"
        for occurrence in occurrences_by_resolution.get(index, []):
            location = _reference_check_location(occurrence)
            output.add_stdout(
                f"resolved: {location} {canonical_id} [{status}] {title}{suffix}".rstrip()
            )
        for warning in resolution.get("warnings", []):
            if resolution["resolution_status"] == "partial" and str(warning).startswith(
                "decision fragment is unvalidated"
            ):
                continue
            output.add_stderr(f"warning: {canonical_id}: {_reference_check_single_line(warning)}")
    for warning in warnings:
        location = _reference_check_location(warning)
        output.add_stderr(
            f"warning: {location} {_reference_check_single_line(warning.get('ref'))} "
            f"{_reference_check_single_line(warning.get('reason'))}".rstrip()
        )
    for item in dangling:
        output.add_stderr(
            f"dangling: {_reference_check_location(item)} "
            f"{_reference_check_single_line(item['ref'])}"
        )
    output.add_stdout(summary)
    return output.render()


def _reference_check_location(value: dict[str, Any]) -> str:
    location = _reference_check_single_line(value.get("source") or value.get("file"))
    if value.get("line") is not None:
        location += f":{value['line']}"
    if value.get("column") is not None:
        location += f":{value['column']}"
    return location


def _reference_check_single_line(value: object) -> str:
    return " ".join(
        "".join(
            character if character.isprintable() else " " for character in str(value or "")
        ).split()
    )


def cmd_check_decisions(args: argparse.Namespace) -> int:
    """Evaluate the actual local Git change set without mutating Agent Mesh state."""

    config = load_config()
    change_set = collect_git_changes(
        config.project_root,
        mode=args.mode,
        base=args.base,
    )
    with open_read_model(config) as snapshot:
        result = build_decision_context(
            config,
            snapshot,
            change_set.paths,
            boundary="change_review",
            change_set=change_set,
        )
        if args.json:
            rendered, complete = render_bounded_decision_context_json(result)
            print(rendered)
            return 0 if complete else 3

        if result.get("complete") is not True:
            diagnostic = next(iter(result.get("diagnostics") or []), "context is incomplete")
            print(
                f"agent-mesh check decisions: context incomplete: {diagnostic}; use --json",
                file=sys.stderr,
            )
            return 3

        decisions = result["decisions"]
        writer = _BoundedDecisionCheckText(MAX_DECISION_CHECK_TEXT_BYTES)
        try:
            writer.append(
                f"decision-check: mode={change_set.mode} paths={len(change_set.paths)} "
                f"decisions={len(decisions)} policy=advisory"
            )
            for decision in decisions:
                writer.append(
                    f"\n{_decision_check_cell(decision['id'])}\t{decision['status']}\t"
                    f"{decision['configured_enforcement']}->{decision['effective_enforcement']}\t"
                )
                for index, match in enumerate(decision["matches"]):
                    if index:
                        writer.append(", ")
                    if match["path"] is None:
                        writer.append("<repository>")
                    else:
                        writer.append(
                            f"{_decision_check_cell(match.get('comparison') or 'selected')}:"
                            f"{_decision_check_cell(match.get('change_kind') or 'selected')}:"
                            f"{_decision_check_cell(match['path'])}"
                        )
                writer.append(f"\t{_decision_check_cell(decision['title'])}")
            if not decisions:
                writer.append("\nNo applicable decisions for this Git change set.")
        except _DecisionCheckTextOverflow:
            print(
                "agent-mesh check decisions: context incomplete: output exceeds the text "
                "bound; use --json",
                file=sys.stderr,
            )
            return 3
        print(writer.render())
        for warning in snapshot.warnings:
            print(f"warning: {warning}", file=sys.stderr)
    return 0


def _decision_check_cell(value: Any) -> str:
    sanitized = "".join(character if character.isprintable() else " " for character in str(value))
    return " ".join(sanitized.split())[:1000]


class _DecisionCheckTextOverflow(RuntimeError):
    pass


class _BoundedDecisionCheckText:
    def __init__(self, max_bytes: int) -> None:
        self._max_bytes = max_bytes
        self._size = 0
        self._chunks: list[str] = []

    def append(self, value: str) -> None:
        size = len(value.encode("utf-8"))
        if self._size + size > self._max_bytes:
            raise _DecisionCheckTextOverflow
        self._chunks.append(value)
        self._size += size

    def render(self) -> str:
        return "".join(self._chunks)


def _status_event(config: AgentMeshConfig, args: argparse.Namespace, *, to_status: str) -> None:
    actor = _authoring_actor(config, explicit_actor=args.actor)
    _ensure_participant(config, actor, role="actor")
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        row = resolve_message(conn, args.request_id)
        from_status = row["status"] if row is not None else "unknown"
    finally:
        conn.close()
    event = Event(
        event_id=generate_event_id(),
        actor=actor,
        kind="req_status_changed",
        entity_id=args.request_id,
        thread_id=args.request_id,
        payload={
            "from_status": from_status,
            "to_status": to_status,
            "reason": args.reason,
            "actor": actor,
        },
    )
    append_event(config.events_path, event, lock_acquired=False)


def _ensure_request_exists(config: AgentMeshConfig, request_id: str):
    rebuild_all(config)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        row = resolve_message(conn, request_id)
        if row is None or row["kind"] != "request":
            raise ConfigError(f"request not found: {request_id}")
        return row
    finally:
        conn.close()


def _read_optional_json_payload(args: argparse.Namespace) -> dict[str, Any]:
    raw = None
    if getattr(args, "json_payload", None):
        raw = args.json_payload
    elif getattr(args, "json_file", None):
        raw = args.json_file.read_text(encoding="utf-8")
    if raw is None:
        return {}
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ConfigError(f"invalid JSON payload: {exc.msg}") from exc
    if not isinstance(payload, dict):
        raise ConfigError("JSON payload must be an object")
    return payload


def _backlog_item_payload(config: AgentMeshConfig, item_id: str) -> dict[str, Any] | None:
    rebuild_all(config)
    return _backlog_item_payload_from_projection(config, item_id)


def _backlog_item_payload_from_projection(
    config: AgentMeshConfig,
    item_id: str,
) -> dict[str, Any] | None:
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        row = conn.execute("SELECT * FROM backlog_items WHERE id=?", (item_id,)).fetchone()
        if row is None:
            return None
        payload = json_loads(row["meta_json"], {})
        if not isinstance(payload, dict):
            payload = {}
        payload.update(
            {
                "id": row["id"],
                "title": row["title"],
                "item_type": row["item_type"],
                "summary": row["summary"],
                "root_cause_summary": row["root_cause_summary"],
                "architectural_category": row["architectural_category"],
                "status": row["status"],
                "priority": row["priority"],
                "launch_scope": row["launch_scope"],
                "release_phase": row["release_phase"],
                "production_state": row["production_state"],
                "disposition": row["disposition"],
                "owner_hint": row["owner_hint"],
                "owner_instance_id": row["owner_instance_id"],
                "lane": row["lane"],
                "notes": row["notes"],
                "workflow_origin": row["workflow_origin"],
                "refs": json_loads(row["refs_json"], []),
            }
        )
        return payload
    finally:
        conn.close()


def _registered_project_for_config(config: AgentMeshConfig) -> RegisteredProject:
    root = config.project_root.resolve()
    for project in list_registered_projects():
        if project.root == root and (not config.store_id or project.id == config.store_id):
            return project
    raise ConfigError(
        f"source repo {root} is not registered. Requested action: run "
        f"'agent-mesh projects register --repo {root}' and retry"
    )


def _backlog_referral_id(source_store: str, source_item: str, target_store: str) -> str:
    identity = f"{source_store}\0{source_item}\0{target_store}".encode("utf-8")
    return "RFL-" + hashlib.sha256(identity).hexdigest()[:20].upper()


def _ref_value(ref: Any) -> tuple[str, str] | None:
    if not isinstance(ref, dict):
        return None
    ref_type = str(ref.get("type") or "").strip()
    value = str(ref.get("value") or "").strip()
    if not ref_type or not value:
        return None
    return ref_type, value


def _merge_structured_refs(
    existing: list[Any] | None,
    additions: list[dict[str, str]],
) -> list[Any]:
    merged = list(existing or [])
    seen = {value for ref in merged if (value := _ref_value(ref)) is not None}
    for ref in additions:
        value = _ref_value(ref)
        if value is None or value in seen:
            continue
        merged.append(ref)
        seen.add(value)
    return merged


def _find_referral_target(config: AgentMeshConfig, referral_id: str) -> str | None:
    items, links = _backlog_state_from_events(config)
    for item_id, ref_type, ref_value in links:
        if (ref_type, ref_value) == ("referral", referral_id):
            return item_id
    for item_id in sorted(items):
        refs = items[item_id].get("refs", [])
        if isinstance(refs, list) and any(
            _ref_value(ref) == ("referral", referral_id) for ref in refs
        ):
            return item_id
    return None


def _find_backlog_title_duplicates(
    config: AgentMeshConfig,
    title: str,
    *,
    exclude: str | None,
) -> list[str]:
    items, _ = _backlog_state_from_events(config)
    normalized_title = title.strip().casefold()
    return sorted(
        item_id
        for item_id, item in items.items()
        if item_id != exclude
        and str(item.get("title") or "").strip().casefold() == normalized_title
    )


def _backlog_state_from_events(
    config: AgentMeshConfig,
) -> tuple[dict[str, dict[str, Any]], list[tuple[str, str, str]]]:
    """Read target backlog identity data without mutating its projection store."""

    items: dict[str, dict[str, Any]] = {}
    links: list[tuple[str, str, str]] = []
    for record in read_event_records(config.events_path):
        payload = record.get("payload")
        if not isinstance(payload, dict):
            continue
        if record.get("kind") == "backlog_item_upserted":
            item_id = str(payload.get("id") or record.get("entity_id") or "").strip()
            if item_id:
                items[item_id] = {**payload, "id": item_id}
        elif record.get("kind") == "backlog_link_added":
            item_id = str(payload.get("item_id") or record.get("entity_id") or "").strip()
            ref_type = str(payload.get("ref_type") or "").strip()
            ref_value = str(payload.get("ref_value") or "").strip()
            if item_id and ref_type and ref_value:
                links.append((item_id, ref_type, ref_value))
    return items, links


def _source_referral_event(
    config: AgentMeshConfig,
    *,
    source_item_id: str,
    referral_id: str,
    event_type: str,
    detail_match: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        for row in conn.execute(
            "SELECT * FROM backlog_events WHERE item_id=? AND event_type=? ORDER BY event_seq",
            (source_item_id, event_type),
        ):
            details = json_loads(row["details_json"], {})
            if (
                isinstance(details, dict)
                and details.get("referral_id") == referral_id
                and all(details.get(key) == value for key, value in (detail_match or {}).items())
            ):
                return {**dict(row), "details": details}
    finally:
        conn.close()
    return None


def _ensure_source_referral_intent(
    config: AgentMeshConfig,
    *,
    source_item_id: str,
    actor: str,
    referral_id: str,
    source_qualified: str,
    target_project: RegisteredProject,
    reason: str,
    blocked: str,
    risk: str,
) -> str:
    lock_handle = acquire(config.agent_dir / ".mail-lock")
    last_event_seq = None
    try:
        recover(config.events_path, config.agent_dir)
        rebuild_all(config)
        existing = _source_referral_event(
            config,
            source_item_id=source_item_id,
            referral_id=referral_id,
            event_type="backlog_referral_intent",
        )
        if existing is not None:
            last_event_seq = int(existing["event_seq"])
            return str(existing["event_id"])
        event = Event(
            event_id=generate_event_id(),
            actor=actor,
            kind="backlog_event_recorded",
            entity_id=source_item_id,
            thread_id=source_item_id,
            payload={
                "item_id": source_item_id,
                "event_type": "backlog_referral_intent",
                "actor": actor,
                "details": {
                    "referral_id": referral_id,
                    "source_backlog": source_qualified,
                    "target_project": target_project.key,
                    "target_store_id": target_project.id,
                    "ownership_reason": reason,
                    "blocks_current_work": blocked,
                    "risk": risk,
                },
            },
        )
        result = append_event(config.events_path, event, lock_acquired=True)
        last_event_seq = result.event.event_seq
        rebuild_all(config)
        return result.event.event_id
    finally:
        lock_handle.release(last_event_seq=last_event_seq)


def _record_source_referral_status(
    config: AgentMeshConfig,
    *,
    source_item_id: str,
    actor: str,
    referral_id: str,
    event_type: str,
    details: dict[str, Any],
) -> None:
    lock_handle = acquire(config.agent_dir / ".mail-lock")
    last_event_seq = None
    try:
        recover(config.events_path, config.agent_dir)
        rebuild_all(config)
        existing = _source_referral_event(
            config,
            source_item_id=source_item_id,
            referral_id=referral_id,
            event_type=event_type,
            detail_match=details,
        )
        if existing is not None:
            last_event_seq = int(existing["event_seq"])
            return
        event = Event(
            event_id=generate_event_id(),
            actor=actor,
            kind="backlog_event_recorded",
            entity_id=source_item_id,
            thread_id=source_item_id,
            payload={
                "item_id": source_item_id,
                "event_type": event_type,
                "actor": actor,
                "details": {"referral_id": referral_id, **details},
            },
        )
        result = append_event(config.events_path, event, lock_acquired=True)
        last_event_seq = result.event.event_seq
        rebuild_all(config)
    finally:
        lock_handle.release(last_event_seq=last_event_seq)


def _require_target_backlog_item(config: AgentMeshConfig, item_id: str) -> None:
    if _backlog_item_payload(config, item_id) is None:
        raise ConfigError(f"target backlog item not found: {item_id}")


def _target_referral_refs(
    *,
    source_project: RegisteredProject,
    source_qualified: str,
    referral_id: str,
    intent_event_id: str,
) -> list[dict[str, str]]:
    return [
        {"type": "referral", "value": referral_id},
        {"type": "source_project", "value": source_project.key},
        {"type": "source_store", "value": source_project.id},
        {"type": "source_backlog", "value": source_qualified},
        {"type": "source_event", "value": intent_event_id},
    ]


def _promote_referral_to_target(
    config: AgentMeshConfig,
    *,
    actor: str,
    source_project: RegisteredProject,
    source_item: dict[str, Any],
    source_qualified: str,
    target_project: RegisteredProject,
    referral_id: str,
    intent_event_id: str,
    title: str,
    summary: str,
    priority: str,
    reason: str,
    source_events: list[str],
    extra_refs: list[str],
) -> str:
    _ensure_participant(config, actor, role="target actor")
    lock_handle = acquire(config.agent_dir / ".mail-lock")
    last_event_seq = None
    try:
        recover(config.events_path, config.agent_dir)
        rebuild_all(config)
        existing = _find_referral_target(config, referral_id)
        if existing is not None:
            return existing
        occurred_utc = utc_now()
        item_id = _allocate_backlog_id(config, occurred_utc=occurred_utc)
        refs = _target_referral_refs(
            source_project=source_project,
            source_qualified=source_qualified,
            referral_id=referral_id,
            intent_event_id=intent_event_id,
        )
        refs.extend({"type": "source_event", "value": value} for value in source_events)
        refs.extend(_parse_backlog_refs(extra_refs))
        refs = refs_with_workflow_origin(refs, "external-input", replace=True)
        event = Event(
            event_id=generate_event_id(),
            occurred_utc=occurred_utc,
            actor=actor,
            kind="backlog_item_upserted",
            entity_id=item_id,
            thread_id=item_id,
            payload={
                "id": item_id,
                "title": title,
                "item_type": source_item.get("item_type") or "cross-repository-referral",
                "summary": summary or f"Referred from {source_qualified} for target-local triage.",
                "status": "triaged",
                "priority": priority or None,
                "disposition": "needs-investigation",
                "lane": "triage",
                "notes": (
                    f"Cross-repository referral {referral_id}. Found in {source_project.key}; "
                    f"owned by {target_project.key} because: {reason} Source remains evidence; "
                    "this target-local item is authoritative for target work."
                ),
                "workflow_origin": "external-input",
                "refs": refs,
                "write_intent": "create",
                "referral_id": referral_id,
            },
        )
        result = append_event(config.events_path, event, lock_acquired=True)
        last_event_seq = result.event.event_seq
        rebuild_all(config)
        render_all(config)
        return item_id
    finally:
        lock_handle.release(last_event_seq=last_event_seq)


def _link_existing_target_referral(
    config: AgentMeshConfig,
    *,
    actor: str,
    target_item_id: str,
    source_project: RegisteredProject,
    source_qualified: str,
    referral_id: str,
    intent_event_id: str,
) -> None:
    _ensure_participant(config, actor, role="target actor")
    lock_handle = acquire(config.agent_dir / ".mail-lock")
    last_event_seq = None
    try:
        recover(config.events_path, config.agent_dir)
        rebuild_all(config)
        existing_target = _find_referral_target(config, referral_id)
        if existing_target is not None:
            if existing_target != target_item_id:
                raise ConfigError(
                    f"referral already resolves to another target item: {existing_target}"
                )
            return
        payload = _backlog_item_payload_from_projection(config, target_item_id)
        if payload is None:
            raise ConfigError(f"target backlog item not found: {target_item_id}")
        payload["refs"] = _merge_structured_refs(
            payload.get("refs"),
            _target_referral_refs(
                source_project=source_project,
                source_qualified=source_qualified,
                referral_id=referral_id,
                intent_event_id=intent_event_id,
            ),
        )
        payload["write_intent"] = "update"
        event = Event(
            event_id=generate_event_id(),
            actor=actor,
            kind="backlog_item_upserted",
            entity_id=target_item_id,
            thread_id=target_item_id,
            payload=payload,
        )
        result = append_event(config.events_path, event, lock_acquired=True)
        last_event_seq = result.event.event_seq
        rebuild_all(config)
        render_all(config)
    finally:
        lock_handle.release(last_event_seq=last_event_seq)


def _complete_source_referral(
    config: AgentMeshConfig,
    *,
    source_item_id: str,
    actor: str,
    referral_id: str,
    target_project: RegisteredProject,
    target_item_id: str,
) -> None:
    lock_handle = acquire(config.agent_dir / ".mail-lock")
    last_event_seq = None
    try:
        recover(config.events_path, config.agent_dir)
        rebuild_all(config)
        completed = _source_referral_event(
            config,
            source_item_id=source_item_id,
            referral_id=referral_id,
            event_type="backlog_referral_completed",
        )
        if completed is not None:
            last_event_seq = int(completed["event_seq"])
            return
        payload = _backlog_item_payload_from_projection(config, source_item_id)
        if payload is None:
            raise ConfigError(f"source backlog item not found: {source_item_id}")
        target_qualified = f"{target_project.key}:{target_item_id}"
        current_refs = list(payload.get("refs") or [])
        merged_refs = _merge_structured_refs(
            current_refs,
            [
                {"type": "referral", "value": referral_id},
                {"type": "target_project", "value": target_project.key},
                {"type": "target_store", "value": target_project.id},
                {"type": "target_backlog", "value": target_qualified},
            ],
        )
        if merged_refs != current_refs:
            payload["refs"] = merged_refs
            payload["write_intent"] = "update"
            update = Event(
                event_id=generate_event_id(),
                actor=actor,
                kind="backlog_item_upserted",
                entity_id=source_item_id,
                thread_id=source_item_id,
                payload=payload,
            )
            result = append_event(config.events_path, update, lock_acquired=True)
            last_event_seq = result.event.event_seq
        completed_event = Event(
            event_id=generate_event_id(),
            actor=actor,
            kind="backlog_event_recorded",
            entity_id=source_item_id,
            thread_id=source_item_id,
            payload={
                "item_id": source_item_id,
                "event_type": "backlog_referral_completed",
                "actor": actor,
                "details": {
                    "referral_id": referral_id,
                    "target_project": target_project.key,
                    "target_store_id": target_project.id,
                    "target_backlog": target_qualified,
                },
            },
        )
        result = append_event(config.events_path, completed_event, lock_acquired=True)
        last_event_seq = result.event.event_seq
        rebuild_all(config)
        render_all(config)
    finally:
        lock_handle.release(last_event_seq=last_event_seq)


def _print_backlog_referral_plan(
    *,
    referral_id: str,
    source_project: RegisteredProject,
    source_item: dict[str, Any],
    target_project: RegisteredProject,
    title: str,
    reason: str,
    blocked: str,
    risk: str,
    existing_target: str | None,
    duplicate_candidates: list[str],
    apply: bool,
    record_only: bool,
) -> None:
    source_qualified = f"{source_project.key}:{source_item['id']}"
    mode = "apply" if apply else "record-only" if record_only else "preview"
    duplicates = (
        ", ".join(f"{target_project.key}:{item_id}" for item_id in duplicate_candidates) or "none"
    )
    target_record = (
        f"reuse {target_project.key}:{existing_target}"
        if existing_target
        else f"create a target-local backlog ID in {target_project.key}"
    )
    print(f"referral: {referral_id}")
    print(f"mode: {mode}")
    print(f"found: {source_qualified} - {source_item.get('title') or ''}")
    print(f"proposed target: {target_project.key} ({target_project.id}) at {target_project.root}")
    print(f"why ownership differs: {reason}")
    print(f"target record: {target_record}; title={title}")
    print("source records: durable intent, qualified target receipt, and completion event")
    print(f"current work blocked: {blocked}")
    print(f"risk: {risk}")
    print(f"possible duplicates: {duplicates}")


def _merge_backlog_payload(
    existing: dict[str, Any] | None,
    json_payload: dict[str, Any],
    args: argparse.Namespace,
    *,
    item_id: str,
) -> dict[str, Any]:
    if json_payload.get("id") and str(json_payload["id"]) != item_id:
        raise ConfigError(f"JSON id {json_payload['id']!r} does not match --id {item_id!r}")
    payload: dict[str, Any] = dict(existing or {})
    payload.update(json_payload)
    payload["id"] = item_id

    cli_field_map = {
        "title": "title",
        "item_type": "item_type",
        "summary": "summary",
        "root_cause_summary": "root_cause_summary",
        "architectural_category": "architectural_category",
        "status": "status",
        "priority": "priority",
        "launch_scope": "launch_scope",
        "release_phase": "release_phase",
        "production_state": "production_state",
        "disposition": "disposition",
        "owner_hint": "owner_hint",
        "owner_instance_id": "owner_instance_id",
        "lane": "lane",
        "notes": "notes",
        "workflow_origin": "workflow_origin",
    }
    for attr, key in cli_field_map.items():
        value = getattr(args, attr, None)
        if value is not None:
            payload[key] = value

    refs = payload.get("refs", [])
    if refs is None:
        refs = []
    if not isinstance(refs, list):
        raise ConfigError("backlog refs must be a list")
    refs = list(refs)
    refs.extend(_parse_backlog_refs(args.ref))
    payload["refs"] = refs_with_workflow_origin(
        refs,
        payload.get("workflow_origin"),
        replace=bool(payload.get("workflow_origin")),
    )

    for key in BACKLOG_PAYLOAD_FIELDS - {"refs"}:
        if payload.get(key) is None:
            payload.pop(key, None)
    return payload


def _parse_backlog_refs(raw_refs: list[str]) -> list[dict[str, str]]:
    parsed = []
    for raw in raw_refs:
        ref_type, sep, ref_value = raw.partition(":")
        if not sep or not ref_type.strip() or not ref_value.strip():
            raise ConfigError(f"backlog --ref must be type:value, got {raw!r}")
        parsed.append({"type": ref_type.strip(), "value": ref_value.strip()})
    return parsed


def _details_payload_from_args(args: argparse.Namespace) -> dict[str, Any]:
    details: dict[str, Any] = {}
    if args.details_json:
        try:
            parsed = json.loads(args.details_json)
        except json.JSONDecodeError as exc:
            raise ConfigError(f"invalid --details-json: {exc.msg}") from exc
        if not isinstance(parsed, dict):
            raise ConfigError("--details-json must be an object")
        details.update(parsed)
    if args.details_file:
        try:
            parsed = json.loads(args.details_file.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ConfigError(f"invalid --details-file JSON: {exc.msg}") from exc
        if not isinstance(parsed, dict):
            raise ConfigError("--details-file must contain a JSON object")
        details.update(parsed)
    for raw in args.detail:
        key, sep, value = raw.partition("=")
        if not sep or not key.strip():
            raise ConfigError(f"--detail must be key=value, got {raw!r}")
        details[key.strip()] = value
    return details


def _ensure_request_event_exists(config: AgentMeshConfig, request_id: str) -> dict[str, Any]:
    for record in read_event_records(config.events_path):
        if record.get("kind") == "req_created" and record.get("entity_id") == request_id:
            payload = record.get("payload", {})
            if isinstance(payload, dict):
                return payload
            break
    raise ConfigError(f"request not found: {request_id}")


def _resolve_reply_parent(config: AgentMeshConfig, parent_id: str) -> dict[str, str] | None:
    rebuild_all(config)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        row = resolve_message(conn, parent_id)
        if row is None:
            return None
        if row["kind"] == "request":
            return {
                "parent_id": parent_id,
                "parent_kind": "request",
                "thread_id": parent_id,
                "request_id": parent_id,
            }
        if row["kind"] == "response":
            request_id = str(row["request_id"] or row["thread_id"])
            return {
                "parent_id": parent_id,
                "parent_kind": "response",
                "thread_id": str(row["thread_id"]),
                "request_id": request_id,
            }
        return None
    finally:
        conn.close()


def _ensure_response_allowed(
    config: AgentMeshConfig,
    *,
    request_id: str,
    request_payload: dict[str, Any],
) -> None:
    response_mode = str(request_payload.get("response_mode") or "single")
    if response_mode == "multi":
        return
    if response_mode != "single":
        raise ConfigError(f"RESPONSE_MODE_INVALID: {response_mode}")

    existing = _first_direct_response_for_request(config, request_id)
    if existing:
        raise ConfigError(
            f"RES_DUPLICATE_FOR_SINGLE_MODE_REQ: {request_id} already has response {existing}"
        )


def _first_direct_response_for_request(config: AgentMeshConfig, request_id: str) -> str | None:
    for record in read_event_records(config.events_path):
        payload = record.get("payload", {})
        if record.get("kind") != "res_posted" or not isinstance(payload, dict):
            continue
        if str(payload.get("request_id") or record.get("thread_id")) != request_id:
            continue
        parent_id = str(payload.get("parent_id") or request_id)
        parent_kind = str(payload.get("parent_kind") or "request")
        if parent_id == request_id and parent_kind == "request":
            return str(record.get("entity_id"))
    return None


def _ensure_participant(config: AgentMeshConfig, name: str, *, role: str) -> None:
    if name not in config.participants:
        raise ConfigError(f"PARTICIPANT_UNKNOWN: {role} {name!r} is not in participants")


def _ensure_participants(config: AgentMeshConfig, names: list[str], *, role: str) -> None:
    for name in names:
        _ensure_participant(config, name, role=role)


def _resolve_decision_or_die(config: AgentMeshConfig, identifier: str) -> str:
    rebuild_all(config)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        resolved = resolve_decision(conn, identifier)
    finally:
        conn.close()
    if resolved is None:
        raise ConfigError(f"decision not found: {identifier}")
    return resolved


def _decision_acceptance_snapshot(
    config: AgentMeshConfig,
    identifier: str,
) -> dict[str, Any]:
    snapshot = _decision_metadata_snapshot(config, identifier)
    try:
        data = read_verified_decision_body(
            config.agent_dir,
            body_path=str(snapshot["body_path"]),
            body_sha=str(snapshot["body_sha"]),
            body_bytes=int(snapshot["body_bytes"]),
        )
        snapshot["body"] = data.decode("utf-8")
    except (DecisionBodyIntegrityError, UnicodeDecodeError) as exc:
        raise ConfigError(f"decision body integrity check failed: {exc}") from exc
    return snapshot


def _decision_metadata_snapshot(
    config: AgentMeshConfig,
    identifier: str,
) -> dict[str, Any]:
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        dec_ulid = resolve_decision(conn, identifier)
        if dec_ulid is None:
            raise ConfigError(f"decision not found: {identifier}")
        row = conn.execute("SELECT * FROM decisions WHERE dec_ulid=?", (dec_ulid,)).fetchone()
        globs = [
            str(item["pattern"])
            for item in conn.execute(
                "SELECT pattern FROM decision_globs "
                "WHERE dec_ulid=? AND kind='affected' ORDER BY rowid",
                (dec_ulid,),
            )
        ]
        checks = [
            str(item["check_name"])
            for item in conn.execute(
                "SELECT check_name FROM decision_checks WHERE dec_ulid=? ORDER BY rowid",
                (dec_ulid,),
            )
        ]
        verification = [
            {
                "command": str(item["command"]),
                "execution_mode": str(item["execution_mode"]),
                "argv": json_loads(item["argv_json"], None),
                "expected_signal": str(item["expected_signal"]),
                "runtime_cost": item["runtime_cost"],
                "drift_risk": item["drift_risk"],
            }
            for item in conn.execute(
                "SELECT command, execution_mode, argv_json, expected_signal, "
                "runtime_cost, drift_risk "
                "FROM decision_verifications "
                "WHERE dec_ulid=? ORDER BY rowid",
                (dec_ulid,),
            )
        ]
        tags = [
            str(item["tag"])
            for item in conn.execute(
                "SELECT tag FROM decision_tags WHERE dec_ulid=? ORDER BY rowid",
                (dec_ulid,),
            )
        ]
        assumptions = [
            {
                "id": str(item["assumption_id"]),
                "text": str(item["text"]),
                "references": json_loads(item["references_json"], []),
            }
            for item in conn.execute(
                "SELECT assumption_id, text, references_json FROM decision_assumptions "
                "WHERE dec_ulid=? ORDER BY rowid",
                (dec_ulid,),
            )
        ]
        evidence: dict[str, list[str]] = {}
        for item in conn.execute(
            "SELECT evidence_kind, ref_value FROM decision_evidence "
            "WHERE dec_ulid=? ORDER BY evidence_kind, ref_value",
            (dec_ulid,),
        ):
            evidence.setdefault(str(item["evidence_kind"]), []).append(str(item["ref_value"]))
    finally:
        conn.close()
    if row is None:
        raise ConfigError(f"decision not found: {identifier}")
    meta = json_loads(row["meta_json"], {})
    if not isinstance(meta, dict):
        meta = {}
    body_path = str(row["body_path"] or "")
    body = ""
    if body_path:
        candidate = config.agent_dir / body_path
        if candidate.exists():
            body = candidate.read_text(encoding="utf-8")
    snapshot: dict[str, Any] = {
        "dec_ulid": str(row["dec_ulid"]),
        "human_id": str(row["human_id"]),
        "title": str(row["title"]),
        "tier": str(row["tier"]),
        "status": str(row["status"]),
        "contract_version": int(row["contract_version"] or 0),
        "applicability_scope": str(row["applicability_scope"]),
        "owner": str(row["owner"] or ""),
        "context": str(meta.get("context") or ""),
        "decision": str(meta.get("decision") or ""),
        "body_sha": str(row["body_sha"]),
        "body_path": body_path,
        "body_bytes": int(row["body_bytes"] or 0),
        "body": body,
        "body_format": str(meta.get("body_format") or DECISION_BODY_FORMAT_UNKNOWN),
        "affected_code_globs": globs,
        "exemptions": normalize_decision_strings(meta.get("exemptions", [])),
        "generated_artifact_paths": normalize_decision_strings(
            meta.get("generated_artifact_paths", [])
        ),
        "required_checks": checks,
        "verification": verification,
        "assumptions": assumptions,
        "evidence": evidence,
        "review_policy": normalize_decision_review_policy(
            meta.get("review_policy", {}), allow_extensions=True
        ),
        "tags": tags,
        "event_seq": int(row["event_seq"]),
    }
    digest_kwargs: dict[str, Any] = {}
    if snapshot["contract_version"] >= 5:
        digest_kwargs = {
            "assumptions": snapshot["assumptions"],
            "evidence": snapshot["evidence"],
            "review_policy": meta.get("review_policy", {}),
        }
    snapshot["revision_sha"] = decision_revision_digest(
        decision_id=snapshot["dec_ulid"],
        human_id=snapshot["human_id"],
        title=snapshot["title"],
        tier=snapshot["tier"],
        applicability_scope=snapshot["applicability_scope"],
        owner=snapshot["owner"],
        context=snapshot["context"],
        decision=snapshot["decision"],
        body_sha=snapshot["body_sha"],
        affected_code_globs=snapshot["affected_code_globs"],
        exemptions=snapshot["exemptions"],
        generated_artifact_paths=snapshot["generated_artifact_paths"],
        required_checks=snapshot["required_checks"],
        verification=snapshot["verification"],
        tags=snapshot["tags"],
        **digest_kwargs,
    )
    snapshot["review_progress"] = decision_review_progress(meta, snapshot["revision_sha"])
    return snapshot


def _require_complete_decision(snapshot: dict[str, Any]) -> None:
    issues = decision_completeness_issues(
        tier=str(snapshot["tier"]),
        owner=str(snapshot["owner"]),
        affected_code_globs=snapshot["affected_code_globs"],
        verification=snapshot["verification"],
        applicability_scope=snapshot["applicability_scope"],
        exemptions=snapshot["exemptions"],
        generated_artifact_paths=snapshot["generated_artifact_paths"],
    )
    if issues:
        detail = "; ".join(issues)
        raise ConfigError(
            f"cannot accept {snapshot['human_id']}: {detail}. "
            f"Repair it with 'agent-mesh decision amend {snapshot['human_id']} --reason ...' "
            "and then review the new Proposed revision"
        )


def _reject_conflicting_clear_flag(
    args: argparse.Namespace,
    clear_name: str,
    *,
    value_name: str | None = None,
) -> None:
    clear = bool(getattr(args, f"clear_{clear_name}"))
    value = getattr(args, value_name or clear_name)
    if clear and value is not None:
        option = (value_name or clear_name).replace("_", "-")
        clear_option = clear_name.replace("_", "-")
        raise ConfigError(f"--{option} cannot be combined with --clear-{clear_option}")


def _decision_body_from_args(args: argparse.Namespace) -> str:
    if args.from_file:
        return args.from_file.read_text(encoding="utf-8")
    return generated_decision_body(
        args.human_id,
        title=args.title,
        context=args.context,
        decision=args.decision,
    )


def _write_body(config: AgentMeshConfig, body: str) -> tuple[str, str, int]:
    data = body.encode("utf-8")
    body_sha = hashlib.sha256(data).hexdigest()
    relative = Path("bodies") / f"{body_sha}.md"
    target = config.agent_dir / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists() or target.read_bytes() != data:
        tmp = target.with_suffix(".md.tmp")
        tmp.write_bytes(data)
        with tmp.open("rb") as handle:
            os.fsync(handle.fileno())
        os.replace(tmp, target)
    return relative.as_posix(), body_sha, len(data)


def _new_decision_id() -> str:
    return "dec_" + generate_event_id()[3:]


def _allocate_backlog_id(config: AgentMeshConfig, *, occurred_utc: str) -> str:
    instant = datetime.fromisoformat(occurred_utc.replace("Z", "+00:00"))
    if instant.tzinfo is None:
        instant = instant.replace(tzinfo=UTC)
    date_stamp = instant.astimezone(ZoneInfo(config.project_timezone)).strftime("%Y%m%d")
    pattern = re.compile(rf"^BKL-{date_stamp}-(\d+)$")
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        rows = conn.execute(
            "SELECT id FROM backlog_items WHERE id LIKE ?",
            (f"BKL-{date_stamp}-%",),
        ).fetchall()
    finally:
        conn.close()
    sequences = [
        int(match.group(1))
        for row in rows
        if (match := pattern.fullmatch(str(row["id"]))) is not None
    ]
    return f"BKL-{date_stamp}-{max(sequences, default=0) + 1:02d}"


def _allocate_agent_instance_id(config: AgentMeshConfig, *, occurred_utc: str) -> str:
    instant = datetime.fromisoformat(occurred_utc.replace("Z", "+00:00"))
    if instant.tzinfo is None:
        instant = instant.replace(tzinfo=UTC)
    date_stamp = instant.astimezone(ZoneInfo(config.project_timezone)).strftime("%Y%m%d")
    pattern = re.compile(rf"^AI-{date_stamp}-(\d+)$")
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        rows = conn.execute(
            "SELECT id FROM agent_instances WHERE id LIKE ?", (f"AI-{date_stamp}-%",)
        ).fetchall()
    finally:
        conn.close()
    sequences = [
        int(match.group(1))
        for row in rows
        if (match := pattern.fullmatch(str(row["id"]))) is not None
    ]
    return f"AI-{date_stamp}-{max(sequences, default=0) + 1:02d}"


def _agent_instance_snapshot(
    config: AgentMeshConfig, identifier: str, *, require_active: bool = False
) -> dict[str, Any]:
    if INSTANCE_ID_RE.fullmatch(identifier.strip()):
        raise ConfigError("raw AI instance IDs are diagnostic-only; use the instance handle")
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        row = resolve_agent_instance(conn, identifier)
        if row is None:
            raise ConfigError(f"AGENT_INSTANCE_UNKNOWN: {identifier}")
        snapshot = {key: row[key] for key in row.keys()}
    finally:
        conn.close()
    if require_active and snapshot["status"] != "active":
        raise ConfigError(f"AGENT_INSTANCE_RETIRED: {snapshot['label']}")
    return snapshot


def _validate_instance_runtime_profile(
    config: AgentMeshConfig, runtime_profile: str, participant: str
) -> None:
    profile_name = runtime_profile.strip()
    if not profile_name:
        return
    profile = config.runtime_profiles.get(profile_name)
    if profile is None:
        raise ConfigError(f"RUNTIME_PROFILE_UNKNOWN: {profile_name}")
    if profile.target != participant:
        raise ConfigError(
            f"RUNTIME_PROFILE_TARGET_MISMATCH: {profile_name} targets "
            f"{profile.target!r}, not {participant!r}"
        )


def _tracked_paths(config: AgentMeshConfig, args: argparse.Namespace) -> list[Path]:
    if args.ci_mode == "pr":
        names = _pr_diff_names(config, args.base)
        candidates = [config.project_root / name for name in names]
    else:
        try:
            raw_paths = read_bounded_git_tracked_paths(config.project_root)
        except GitChangeUnavailable as exc:
            if (config.project_root / ".git").exists():
                raise ConfigError(f"cannot enumerate tracked reference inputs: {exc}") from exc
            candidates = _bounded_reference_filesystem_paths(config.project_root)
        else:
            candidates = [
                config.project_root / name
                for name in _decode_reference_git_paths(raw_paths, operation="git ls-files")
            ]

    patterns = [item.strip() for item in args.paths.split(",") if item.strip()] or ["**/*"]
    return sorted(
        {
            path
            for path in candidates
            if path.is_file()
            and any(
                pattern == "**/*"
                or fnmatch.fnmatch(path.relative_to(config.project_root).as_posix(), pattern)
                for pattern in patterns
            )
        }
    )


def _pr_diff_names(config: AgentMeshConfig, base: str) -> list[str]:
    failures: list[str] = []
    for ref in (f"origin/{base}", base):
        try:
            raw_paths = read_bounded_git_diff_paths(
                config.project_root,
                base_ref=ref,
            )
        except GitChangeUnavailable as exc:
            failures.append(str(exc))
            continue
        return _decode_reference_git_paths(raw_paths, operation="git diff")
    detail = next((item for item in failures if item), "git diff failed")
    raise ConfigError(f"cannot enumerate PR reference inputs: {detail[:500]}")


def _decode_reference_git_paths(raw_paths: bytes, *, operation: str) -> list[str]:
    if raw_paths and not raw_paths.endswith(b"\0"):
        raise ConfigError(f"{operation} returned an incomplete NUL-delimited path list")
    paths: list[str] = []
    for raw_path in raw_paths.split(b"\0"):
        if not raw_path:
            continue
        if len(paths) >= MAX_REFERENCE_CHECK_PATHS:
            raise ConfigError(
                f"{operation} exceeds {MAX_REFERENCE_CHECK_PATHS} reference input paths"
            )
        if len(raw_path) > MAX_REFERENCE_CHECK_PATH_BYTES:
            raise ConfigError(
                f"{operation} returned a path longer than "
                f"{MAX_REFERENCE_CHECK_PATH_BYTES} UTF-8 bytes"
            )
        try:
            path = raw_path.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ConfigError(f"{operation} returned a non-UTF-8 path") from exc
        candidate = Path(path)
        if candidate.is_absolute() or ".." in candidate.parts:
            raise ConfigError(f"{operation} returned a path outside the project root")
        paths.append(path)
    return paths


def _bounded_reference_filesystem_paths(
    project_root: Path,
    *,
    max_entries: int = MAX_REFERENCE_CHECK_FILESYSTEM_ENTRIES,
    deadline_monotonic: float | None = None,
) -> list[Path]:
    """Enumerate regular files without following symlinks or walking without bounds."""

    root = project_root.resolve()
    deadline = (
        deadline_monotonic
        if deadline_monotonic is not None
        else time.monotonic() + MAX_REFERENCE_CHECK_FILESYSTEM_SECONDS
    )
    paths: list[Path] = []
    pending = [root]
    visited_entries = 0
    while pending:
        if time.monotonic() >= deadline:
            raise ConfigError(
                "filesystem reference scan exceeded its time budget; use explicit --file inputs"
            )
        directory = pending.pop()
        try:
            with os.scandir(directory) as stream:
                entries: list[os.DirEntry[str]] = []
                for entry in stream:
                    if time.monotonic() >= deadline:
                        raise ConfigError(
                            "filesystem reference scan exceeded its time budget; "
                            "use explicit --file inputs"
                        )
                    if visited_entries >= max_entries:
                        raise ConfigError(
                            f"filesystem reference scan exceeds {max_entries} visited entries; "
                            "use explicit --file inputs"
                        )
                    visited_entries += 1
                    entries.append(entry)
                entries.sort(key=lambda entry: entry.name, reverse=True)
        except OSError as exc:
            raise ConfigError(f"cannot enumerate reference inputs in {directory}: {exc}") from exc
        for entry in entries:
            if time.monotonic() >= deadline:
                raise ConfigError(
                    "filesystem reference scan exceeded its time budget; use explicit --file inputs"
                )
            path = Path(entry.path)
            relative = path.relative_to(root)
            try:
                relative_bytes = relative.as_posix().encode("utf-8")
            except UnicodeEncodeError as exc:
                raise ConfigError("filesystem reference scan found a non-UTF-8 path") from exc
            if len(relative_bytes) > MAX_REFERENCE_CHECK_PATH_BYTES:
                raise ConfigError(
                    "reference scan found a path longer than "
                    f"{MAX_REFERENCE_CHECK_PATH_BYTES} UTF-8 bytes"
                )
            try:
                if entry.is_symlink():
                    continue
                if entry.is_dir(follow_symlinks=False):
                    pending.append(path)
                    continue
                if not entry.is_file(follow_symlinks=False):
                    continue
            except OSError as exc:
                raise ConfigError(f"cannot inspect reference input {path}: {exc}") from exc
            if len(paths) >= MAX_REFERENCE_CHECK_PATHS:
                raise ConfigError(
                    f"reference scan exceeds {MAX_REFERENCE_CHECK_PATHS} filesystem paths"
                )
            paths.append(project_root / relative)
    return paths


# ---------------------------------------------------------------------------
# skill subcommand (Phase 5)
# ---------------------------------------------------------------------------


def cmd_skill_targets(args: argparse.Namespace) -> int:
    for name in sorted(SUPPORTED_TARGETS):
        target = SUPPORTED_TARGETS[name]
        print(f"{name}\t{target.description}")
    return 0


def cmd_skill_render(args: argparse.Namespace) -> int:
    try:
        rendered = render_skill(args.target)
    except UnknownTargetError as exc:
        print(f"agent-mesh: {exc}", file=sys.stderr)
        return 2
    if args.output is not None:
        dest = _safe_resolve_dest(args.output)
        _atomic_write_text(dest, rendered)
        print(f"wrote {dest}")
        return 0
    # Default: stdout (--stdout flag is accepted but is the default).
    sys.stdout.write(rendered)
    return 0


def cmd_skill_install(args: argparse.Namespace) -> int:
    try:
        rendered = render_skill(args.target)
    except UnknownTargetError as exc:
        print(f"agent-mesh: {exc}", file=sys.stderr)
        return 2
    dest = _safe_resolve_dest(args.dest)
    _atomic_write_text(dest, rendered)
    print(f"installed agent-mesh skill (target={args.target}) -> {dest}")
    return 0


def _safe_resolve_dest(path: Path) -> Path:
    """Resolve a destination path and reject directory-traversal sentinels.

    The CLI takes an explicit path from the user; we still refuse paths
    that are ambiguous (empty), look like a directory rather than a file,
    or attempt to traverse via embedded `..` after resolution drift. The
    actual filesystem write is bounded by the path the user typed.
    """
    if str(path) == "" or str(path) == "-":
        raise ConfigError("--dest/--output requires an explicit file path")
    resolved = path.expanduser()
    # Reject paths whose final component is empty (trailing slash) — that
    # implies a directory, but we write a single file.
    if resolved.name == "":
        raise ConfigError(f"destination must be a file path, not a directory: {path}")
    # Reject literal `..` segments. We allow absolute and relative paths;
    # we just don't want surprise traversal in scripted usage.
    if any(part == ".." for part in resolved.parts):
        raise ConfigError(f"destination must not contain '..' segments: {path}")
    if resolved.exists() and resolved.is_dir():
        raise ConfigError(f"destination must be a file path, not a directory: {path}")
    return resolved


def _atomic_write_text(dest: Path, content: str) -> None:
    """Atomically write UTF-8 text to an explicit destination path."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=dest.parent,
        prefix=f".{dest.name}.",
        suffix=".tmp",
        delete=False,
    ) as fh:
        tmp = Path(fh.name)
        fh.write(content)
        fh.flush()
        os.fsync(fh.fileno())
    try:
        tmp.replace(dest)
    except Exception:
        try:
            tmp.unlink()
        finally:
            raise


if __name__ == "__main__":
    raise SystemExit(main())
