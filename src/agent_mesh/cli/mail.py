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
import subprocess
import sys
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
    DECISION_TIERS,
    DecisionVerificationError,
    decision_completeness_issues,
    normalize_decision_strings,
    normalize_decision_verification,
)
from agent_mesh.core.agent_instances import (
    INSTANCE_ID_RE,
    AgentInstanceError,
    bind_agent_instance,
    digest_external_session_ref,
    normalize_instance_label,
    resolve_agent_instance_from_records,
    resolve_authoring_actor,
    reset_agent_instance,
    selected_agent_instance,
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
from agent_mesh.core.recovery import recover
from agent_mesh.core.workflow_origin import WORKFLOW_ORIGINS, refs_with_workflow_origin
from agent_mesh.project_registry import (
    ProjectRegistryError,
    RegisteredProject,
    list_registered_projects,
    register_project,
    registry_path,
    resolve_registered_project,
    unregister_project,
)
from agent_mesh.skill import SUPPORTED_TARGETS, UnknownTargetError, render_skill
from agent_mesh.store.rebuild import (
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
from agent_mesh.views import locate_message, render_all

REF_RE = re.compile(
    r"(?<![A-Za-z0-9_])("
    r"D\d+(?:-(?:[SB]\d+|[A-Z]))?(?:-§[A-Za-z0-9._-]+)?|"
    r"REQ-\d{8}T\d{6}Z-[A-Z0-9_-]+-\d{5}|"
    r"RES-\d{8}T\d{6}Z-[A-Z0-9_-]+-\d{5}|"
    r"AI-\d{8}-\d{2,}|"
    r"FBK-[A-Za-z0-9][\w-]*|DI-[A-Za-z0-9][\w-]*|J-[A-Za-z0-9][\w-]*|"
    r"BKL-[A-Za-z0-9][\w-]*|IMP-[A-Za-z0-9][\w-]*"
    r")(?![A-Za-z0-9_])"
)


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    instance_token = bind_agent_instance(getattr(args, "instance", None))
    try:
        return int(args.func(args))
    except AdoptionContractError as exc:
        print(f"agent-mesh: {exc}", file=sys.stderr)
        return 2
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


def _authoring_actor(
    config: AgentMeshConfig, *, explicit_actor: str | None = None
) -> str:
    return resolve_authoring_actor(
        read_event_records(config.events_path),
        default_actor=config.default_sender,
        explicit_actor=explicit_actor,
    )


def _cross_project_authoring_actor(
    source_config: AgentMeshConfig,
    target_config: AgentMeshConfig,
) -> str:
    """Resolve one bound chat across project-local registries without reusing its local ID."""

    selected = selected_agent_instance()
    if not selected:
        return _authoring_actor(target_config)
    if INSTANCE_ID_RE.fullmatch(selected):
        raise AgentInstanceError(
            "CROSS_PROJECT_INSTANCE_ID_FORBIDDEN: AI-agent instance IDs are project-local; "
            "bind a shared instance label for cross-project writes"
        )

    source_instance = resolve_agent_instance_from_records(
        read_event_records(source_config.events_path), selected
    )
    target_instance = resolve_agent_instance_from_records(
        read_event_records(target_config.events_path), selected
    )
    if source_instance is None or source_instance.status != "active":
        raise AgentInstanceError(f"AGENT_INSTANCE_UNKNOWN: {selected}")
    if target_instance is None or target_instance.status != "active":
        raise AgentInstanceError(
            f"CROSS_PROJECT_INSTANCE_MAPPING_MISSING: active label {selected!r} "
            "must be registered in the target project"
        )
    source_identity = (
        source_instance.participant,
        source_instance.provider,
        source_instance.external_session_ref_digest,
    )
    target_identity = (
        target_instance.participant,
        target_instance.provider,
        target_instance.external_session_ref_digest,
    )
    if source_identity != target_identity:
        raise AgentInstanceError(
            f"CROSS_PROJECT_INSTANCE_MAPPING_MISMATCH: label {selected!r} must map to "
            "the same participant, provider, and external session digest in both projects"
        )
    return target_instance.participant


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="agent-mesh")
    parser.add_argument(
        "--instance",
        help="authoring AI instance ID or label; alternatively set AGENT_MESH_INSTANCE_ID",
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
        "instance", help="register or manage a durable addressable AI-agent instance"
    )
    instance_sub = instance.add_subparsers(dest="instance_command", required=True)

    instance_register = instance_sub.add_parser("register")
    instance_register.add_argument("--participant", required=True)
    instance_register.add_argument("--provider", required=True)
    instance_register.add_argument("--label", required=True)
    instance_register.add_argument("--workstream", default="")
    instance_register.add_argument("--runtime-profile", default="")
    instance_register.add_argument(
        "--external-session-ref",
        help="provider chat/session reference; only its SHA-256 digest is recorded",
    )
    instance_register.add_argument("--actor")
    instance_register.set_defaults(func=cmd_instance_register)

    instance_update = instance_sub.add_parser("update")
    instance_update.add_argument("identifier")
    instance_update.add_argument("--reason", required=True)
    instance_update.add_argument("--label")
    instance_update.add_argument("--workstream")
    instance_update.add_argument("--runtime-profile")
    instance_update.add_argument("--external-session-ref")
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

    projects = sub.add_parser("projects", help="manage registered Workbench repos")
    projects_sub = projects.add_subparsers(dest="projects_command", required=True)
    projects_list = projects_sub.add_parser("list", help="list registered repos")
    projects_list.set_defaults(func=cmd_projects_list)
    projects_register = projects_sub.add_parser("register", help="register an agent-mesh repo")
    projects_register.add_argument("--repo", type=Path, default=Path("."))
    projects_register.set_defaults(func=cmd_projects_register)
    projects_unregister = projects_sub.add_parser("unregister", help="unregister a repo")
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
    propose.add_argument("--owner")
    propose.add_argument("--affects", action="append", default=[])
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
    propose.set_defaults(func=cmd_decision_propose)

    amend = decision_sub.add_parser(
        "amend",
        help="append a metadata revision; accepted decisions return to Proposed",
        description=(
            "Append a metadata revision; accepted decisions return to Proposed. "
            "Supplying a collection option replaces the whole stored collection, "
            "so repeat the option for every value to retain. Use its clear option "
            "alone to remove the collection."
        ),
    )
    amend.add_argument("identifier")
    amend.add_argument("--reason", required=True)
    amend.add_argument("--title")
    amend.add_argument("--tier", choices=DECISION_TIERS)
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
    amend.add_argument("--from-file", type=Path)
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
        "--owner-instance", help="active AI instance ID or label responsible for this item"
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
            "Compatibility/import surface. Omit --id to create a normal item with "
            "an atomically allocated BKL-YYYYMMDD-NN identifier."
        ),
    )
    backlog_upsert.add_argument(
        "--id",
        dest="item_id",
        metavar="BKL-ID",
        help="existing or imported item ID; normal creation should use backlog create",
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
    refs.add_argument("--record-scan", action="store_true")
    refs.set_defaults(func=cmd_check_refs)

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
        choices=("install", "status", "open", "start", "restart", "uninstall"),
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


def cmd_instance_register(args: argparse.Namespace) -> int:
    config = load_config()
    participant = args.participant.strip()
    actor = _authoring_actor(config, explicit_actor=args.actor)
    _ensure_participant(config, participant, role="instance participant")
    _ensure_participant(config, actor, role="actor")
    label = normalize_instance_label(args.label)
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
                "external_session_ref_digest": digest_external_session_ref(
                    args.external_session_ref
                ),
            },
        )
        result = append_event(config.events_path, event, lock_acquired=True)
        last_event_seq = result.event.event_seq
    finally:
        lock_handle.release(last_event_seq=last_event_seq)
    print(f"{instance_id}\t{label}\t{participant}\tactive")
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
            label = normalize_instance_label(args.label)
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
        if args.external_session_ref is not None:
            digest = digest_external_session_ref(args.external_session_ref)
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
    print(f"{'updated' if did_update else 'unchanged'} {instance_id}")
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
    print(f"retired {instance_id}")
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
        else:
            detail = identity.get("error") or ",".join(identity.get("missing", []))
            print(f"project identity: incomplete ({detail})")
        print(
            "agent contract: "
            + ("healthy" if status["healthy"] else "incomplete or conflicting")
            + f" (v{status['version']} {status['digest']})"
        )
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


def cmd_projects_unregister(args: argparse.Namespace) -> int:
    removed = unregister_project(args.repo)
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
    instance_addresses = list(dict.fromkeys(args.to_instance or []))
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
    request_id = _new_public_id("REQ", sender)
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
        response_id = _new_public_id("RES", sender)
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
    from agent_mesh.workbench import WorkbenchError, _validate_workbench_host, serve_workbench

    if args.workbench_mode == "service":
        return _cmd_workbench_service(args)
    if args.service_action is not None:
        print("agent-mesh: Workbench service action requires 'workbench service'", file=sys.stderr)
        return 2
    if args.managed_service:
        os.environ["AGENT_MESH_WORKBENCH_SERVICE"] = "1"
        if args.config_home is not None:
            os.environ["AGENT_MESH_CONFIG_HOME"] = str(args.config_home.expanduser().resolve())

    repo = args.repo
    port = args.port if args.port is not None else 8765
    if args.managed_service:
        try:
            load_config(repo)
        except ConfigError:
            projects = list_registered_projects()
            if not projects:
                raise
            repo = projects[0].root

    try:
        _validate_workbench_host(args.host)
        serve_workbench(
            repo=repo,
            host=args.host,
            port=port,
            open_browser=args.open,
        )
    except WorkbenchError as exc:
        print(f"agent-mesh: {exc}", file=sys.stderr)
        return 2
    return 0


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
        make_service_spec,
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
            "install, status, open, start, restart, or uninstall",
            file=sys.stderr,
        )
        return 2

    bookmark_path: Path | None = None
    anchor_config: AgentMeshConfig | None = None
    try:
        if action == "install":
            _validate_workbench_host(args.host)
            config = load_config(args.repo)
            anchor_config = config
            register_project(config.project_root)
            spec = make_service_spec(
                repo=config.project_root,
                host=args.host,
                port=args.port if args.port is not None else 8767,
            )
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
            status = start_workbench_service()
        elif action == "restart":
            status = restart_workbench_service()
        else:
            status = uninstall_workbench_service()
    except (ConfigError, ProjectRegistryError, WorkbenchServiceError, WorkbenchError) as exc:
        print(f"agent-mesh: {exc}", file=sys.stderr)
        return 2

    if bookmark_path is None:
        bookmark_path = _service_bookmark_path(status, managed_workbench_bookmark_path)
    _print_workbench_service_status(status, bookmark_path=bookmark_path)
    if action in {"install", "open", "start", "restart"}:
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
            if anchor_config is not None:
                try:
                    write_managed_bookmark_pointer(anchor_config, bookmark_path)
                except OSError as exc:
                    print(
                        f"agent-mesh: warning: could not update the project bookmark: {exc}",
                        file=sys.stderr,
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
            "human_id": args.human_id,
            "aliases": [],
            "title": args.title,
            "tier": args.tier,
            "context": args.context,
            "decision": args.decision,
            "rejected_alternatives": [],
            "consequences": [],
            "affected_code_globs": normalize_decision_strings(args.affects),
            "exemptions": [],
            "generated_artifact_paths": [],
            "assumptions": [],
            "evidence": {},
            "supersedes": None,
            "owner": args.owner,
            "review_policy": {},
            "required_checks": normalize_decision_strings(args.required_check),
            "verification": verification,
            "tags": normalize_decision_strings(args.tag),
            "body_sha": body_sha,
            "body_path": body_path,
            "body_bytes": body_bytes,
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
    _reject_conflicting_clear_flag(args, "required_checks", value_name="required_check")
    _reject_conflicting_clear_flag(args, "verification", value_name="verify_command")
    _reject_conflicting_clear_flag(args, "tags", value_name="tag")

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
                "verification",
                [] if args.clear_verification else args.verify_command,
                lambda values: normalize_decision_verification(values, reject_unsafe=True),
            ),
            ("tags", [] if args.clear_tags else args.tag, normalize_decision_strings),
        )
        for field_name, requested, normalizer in collection_requests:
            if requested is None:
                continue
            normalized_values = normalizer(requested)
            if normalized_values != current[field_name]:
                fields_changed[field_name] = [current[field_name], normalized_values]

        if args.from_file is not None:
            body = args.from_file.read_text(encoding="utf-8")
            if body != current["body"]:
                body_path, body_sha, body_bytes = _write_body(config, body)
                fields_changed.update(
                    {
                        "body_path": [current["body_path"], body_path],
                        "body_sha": [current["body_sha"], body_sha],
                        "body_bytes": [current["body_bytes"], body_bytes],
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
                    "decision_id": current["dec_ulid"],
                    "reason": reason,
                    "change_kind": (
                        "content_revision" if status in {"accepted", "in_force"} else "content_update"
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
    _require_complete_decision(preview)

    confirmation = f"ACCEPT {preview['human_id']}"
    print(f"Decision: {preview['human_id']} - {preview['title']}")
    print(f"Body SHA-256: {preview['body_sha']}")
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
        if current["dec_ulid"] != preview["dec_ulid"] or current["body_sha"] != preview["body_sha"]:
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
                "decision_id": current["dec_ulid"],
                "accepted_by": approver,
                "notes": notes,
                "approval_source": "interactive_cli",
                "approved_utc": approved_utc,
                "approved_body_sha": current["body_sha"],
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
        if mint_item_id:
            write_intent = "create"
        elif existing is not None:
            write_intent = "update"
        else:
            write_intent = "upsert"
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
        choices = ", ".join(
            f"{target_project.key}:{item_id}" for item_id in duplicate_candidates
        )
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
        target_actor = _cross_project_authoring_actor(source_config, target_config)
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
    print(
        f"promoted {source_qualified} -> {target_project.key}:{target_item_id} "
        f"({referral_id})"
    )
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
    _rebuild_all_locked(config)
    paths = _tracked_paths(config, args)
    conn = connect(config.db_path)
    dangling: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    try:
        initialize_schema(conn)
        for path in paths:
            rel = path.relative_to(config.project_root).as_posix()
            if config.checks.is_exempt(rel):
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                continue
            for line_no, line in enumerate(text.splitlines(), start=1):
                for match in REF_RE.finditer(line):
                    token = match.group(1)
                    kind = _ref_kind(token)
                    if kind == "decision":
                        base = token.split("-§", 1)[0]
                        if resolve_decision(conn, base) is None:
                            dangling.append({"ref": token, "file": rel, "line": line_no})
                    elif kind in {"request", "response"}:
                        if resolve_message(conn, token) is None:
                            dangling.append({"ref": token, "file": rel, "line": line_no})
                    elif kind == "backlog":
                        if (
                            conn.execute(
                                "SELECT 1 FROM backlog_items WHERE id=?", (token,)
                            ).fetchone()
                            is None
                        ):
                            dangling.append({"ref": token, "file": rel, "line": line_no})
                    elif kind == "agent_instance":
                        if resolve_agent_instance(conn, token) is None:
                            dangling.append({"ref": token, "file": rel, "line": line_no})
                    else:
                        warnings.append(
                            {
                                "ref": token,
                                "file": rel,
                                "line": line_no,
                                "reason": "unknown ref kind",
                            }
                        )
    finally:
        conn.close()

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

    for warning in warnings:
        print(
            f"warning: {warning['file']}:{warning['line']} unknown ref kind {warning['ref']}",
            file=sys.stderr,
        )
    for item in dangling:
        print(f"dangling: {item['file']}:{item['line']} {item['ref']}", file=sys.stderr)
    print(f"refs: scanned={len(paths)} dangling={len(dangling)} warnings={len(warnings)}")
    return 1 if dangling else 0


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
        ", ".join(f"{target_project.key}:{item_id}" for item_id in duplicate_candidates)
        or "none"
    )
    target_record = (
        f"reuse {target_project.key}:{existing_target}"
        if existing_target
        else f"create a target-local backlog ID in {target_project.key}"
    )
    print(f"referral: {referral_id}")
    print(f"mode: {mode}")
    print(f"found: {source_qualified} - {source_item.get('title') or ''}")
    print(
        f"proposed target: {target_project.key} ({target_project.id}) at {target_project.root}"
    )
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
    return _decision_metadata_snapshot(config, identifier)


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
            }
            for item in conn.execute(
                "SELECT command, execution_mode, argv_json, expected_signal "
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
    return {
        "dec_ulid": str(row["dec_ulid"]),
        "human_id": str(row["human_id"]),
        "title": str(row["title"]),
        "tier": str(row["tier"]),
        "status": str(row["status"]),
        "owner": str(row["owner"] or ""),
        "context": str(meta.get("context") or ""),
        "decision": str(meta.get("decision") or ""),
        "body_sha": str(row["body_sha"]),
        "body_path": body_path,
        "body_bytes": int(row["body_bytes"] or 0),
        "body": body,
        "affected_code_globs": globs,
        "required_checks": checks,
        "verification": verification,
        "tags": tags,
        "event_seq": int(row["event_seq"]),
    }


def _require_complete_decision(snapshot: dict[str, Any]) -> None:
    issues = decision_completeness_issues(
        tier=str(snapshot["tier"]),
        owner=str(snapshot["owner"]),
        affected_code_globs=snapshot["affected_code_globs"],
        verification=snapshot["verification"],
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
    return (
        f"# {args.human_id} — {args.title}\n\n"
        f"## Context\n{args.context}\n\n"
        f"## Decision\n{args.decision}\n"
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


def _new_public_id(prefix: str, actor: str) -> str:
    stamp = utc_now().replace("-", "").replace(":", "")
    safe_actor = re.sub(r"[^A-Za-z0-9_-]+", "-", actor).upper()[:20] or "ACTOR"
    return f"{prefix}-{stamp}-{safe_actor}-{secrets.randbelow(100000):05d}"


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
        raise ConfigError(f"AGENT_INSTANCE_RETIRED: {snapshot['id']}")
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
        result = subprocess.run(
            ["git", "ls-files"],
            cwd=config.project_root,
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode == 0:
            candidates = [
                config.project_root / line.strip()
                for line in result.stdout.splitlines()
                if line.strip()
            ]
        else:
            candidates = [path for path in config.project_root.rglob("*") if path.is_file()]

    patterns = [item.strip() for item in args.paths.split(",") if item.strip()] or ["**/*"]
    return sorted(
        {
            path
            for path in candidates
            if path.is_file()
            and any(
                fnmatch.fnmatch(path.relative_to(config.project_root).as_posix(), pattern)
                for pattern in patterns
            )
        }
    )


def _pr_diff_names(config: AgentMeshConfig, base: str) -> list[str]:
    for ref in (f"origin/{base}", base):
        result = subprocess.run(
            ["git", "diff", "--name-only", f"{ref}..HEAD"],
            cwd=config.project_root,
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode == 0:
            return [line.strip() for line in result.stdout.splitlines() if line.strip()]
    return []


def _ref_kind(token: str) -> str:
    if token.startswith("D"):
        return "decision"
    if token.startswith("REQ-"):
        return "request"
    if token.startswith("RES-"):
        return "response"
    if token.startswith("BKL-"):
        return "backlog"
    if token.startswith("AI-"):
        return "agent_instance"
    return "unknown"


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
