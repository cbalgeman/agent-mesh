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
from pathlib import Path
from typing import Any
import tempfile

from agent_mesh.config import (
    AgentMeshConfig,
    ConfigError,
    STATE_SHARING_CHOICES,
    STATE_SHARING_LOCAL_ONLY,
    default_config_text,
    ensure_project_dirs,
    load_config,
    write_agent_dir_gitignore,
)
from agent_mesh.core.provenance import BODY_AUTHORITY_VALUES, BODY_FIDELITY_VALUES
from agent_mesh.core.events import Event, append_event, generate_event_id, utc_now
from agent_mesh.core.lock import acquire
from agent_mesh.project_registry import (
    ProjectRegistryError,
    list_registered_projects,
    register_project,
    registry_path,
    unregister_project,
)
from agent_mesh.skill import SUPPORTED_TARGETS, UnknownTargetError, render_skill
from agent_mesh.store.rebuild import DecisionStopLine, read_event_records, rebuild_all
from agent_mesh.store.sqlite import (
    connect,
    initialize_schema,
    json_loads,
    resolve_decision,
    resolve_message,
)
from agent_mesh.views import locate_message, render_all

REF_RE = re.compile(
    r"(?<![A-Za-z0-9_])("
    r"D\d+(?:-(?:[SB]\d+|[A-Z]))?(?:-§[A-Za-z0-9._-]+)?|"
    r"REQ-\d{8}T\d{6}Z-[A-Z0-9_-]+-\d{5}|"
    r"RES-\d{8}T\d{6}Z-[A-Z0-9_-]+-\d{5}|"
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
    try:
        return int(args.func(args))
    except ConfigError as exc:
        print(f"agent-mesh: {exc}", file=sys.stderr)
        return 2
    except ProjectRegistryError as exc:
        print(f"agent-mesh: {exc}", file=sys.stderr)
        return 2
    except DecisionStopLine as exc:
        print(f"agent-mesh: {exc.code}: {exc.detail}", file=sys.stderr)
        return 1


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


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="agent-mesh")
    sub = parser.add_subparsers(dest="command", required=True)

    init = sub.add_parser("init")
    init.add_argument("--participants", default="user,agent")
    init.add_argument("--default-recipient")
    init.add_argument("--default-sender")
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

    request = sub.add_parser("request")
    request.add_argument("--from", dest="sender")
    request.add_argument("--to", required=True)
    request.add_argument("--feature", default="")
    request.add_argument("--ref", action="append", default=[])
    request.add_argument("--response-mode", choices=("single", "multi"), default="single")
    _add_provenance_arguments(request)
    request.add_argument("title")
    request.add_argument("body", nargs="?")
    request.set_defaults(func=cmd_request)

    reply = sub.add_parser("reply")
    reply.add_argument("--from", dest="sender")
    reply.add_argument("--ref", action="append", default=[])
    _add_provenance_arguments(reply)
    reply.add_argument("parent_id")
    reply.add_argument("summary")
    reply.add_argument("details", nargs="?")
    reply.set_defaults(func=cmd_reply)

    respond = sub.add_parser("respond")
    respond.add_argument("--from", dest="sender")
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
    propose.add_argument("--tier", required=True)
    propose.add_argument("--owner")
    propose.add_argument("--affects", action="append", default=[])
    propose.add_argument("--from-file", type=Path)
    propose.add_argument("--context", default="")
    propose.add_argument("--decision", default="")
    propose.add_argument("--tag", action="append", default=[])
    propose.set_defaults(func=cmd_decision_propose)

    accept = decision_sub.add_parser("accept")
    accept.add_argument("identifier")
    accept.add_argument("--by", default=None)
    accept.add_argument("--notes")
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

    backlog_upsert = backlog_sub.add_parser("upsert")
    backlog_upsert.add_argument("--actor")
    backlog_upsert.add_argument("--id", dest="item_id")
    backlog_upsert.add_argument("--title")
    backlog_upsert.add_argument("--item-type")
    backlog_upsert.add_argument("--summary")
    backlog_upsert.add_argument("--root-cause-summary")
    backlog_upsert.add_argument("--architectural-category")
    backlog_upsert.add_argument("--status")
    backlog_upsert.add_argument("--priority")
    backlog_upsert.add_argument("--launch-scope")
    backlog_upsert.add_argument("--release-phase")
    backlog_upsert.add_argument("--production-state")
    backlog_upsert.add_argument("--disposition")
    backlog_upsert.add_argument("--owner-hint")
    backlog_upsert.add_argument("--lane")
    backlog_upsert.add_argument("--notes")
    backlog_upsert.add_argument("--ref", action="append", default=[],
                                help="structured ref as type:value; may be repeated")
    _add_json_payload_arguments(backlog_upsert)
    backlog_upsert.set_defaults(func=cmd_backlog_upsert)

    backlog_link = backlog_sub.add_parser("link")
    backlog_link.add_argument("--actor")
    backlog_link.add_argument("--allow-missing-item", action="store_true")
    backlog_link.add_argument("item_id")
    backlog_link.add_argument("ref_type")
    backlog_link.add_argument("ref_value")
    backlog_link.set_defaults(func=cmd_backlog_link)

    backlog_record = backlog_sub.add_parser("record")
    backlog_record.add_argument("--actor")
    backlog_record.add_argument("--detail", action="append", default=[],
                                help="audit detail as key=value; may be repeated")
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
        choices=("install", "status", "start", "restart", "uninstall"),
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
    render_dest.add_argument("--output", type=Path, default=None,
                             help="explicit file path; defaults to stdout when omitted")
    skill_render.set_defaults(func=cmd_skill_render)

    skill_install = skill_sub.add_parser("install",
                                         help="render skill and write it to an explicit destination")
    skill_install.add_argument("--target", required=True)
    skill_install.add_argument("--dest", required=True, type=Path,
                               help="explicit destination file; no global home discovery, no --all")
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
                default_recipient=args.default_recipient or (participants[0] if participants else "codex"),
                state_sharing=args.state_sharing or STATE_SHARING_LOCAL_ONLY,
            ),
            encoding="utf-8",
        )
    config = load_config(root)
    if args.state_sharing is not None and args.state_sharing != config.state_sharing:
        raise ConfigError(
            "--state-sharing does not rewrite an existing config; edit "
            "[version_control].state_sharing in .agent-mesh/config.toml, then rerun init"
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
    if not args.no_register:
        project = register_project(root)
        print(f"registered Workbench repo {project.id} at {registry_path()}")
    return 0


def cmd_projects_list(args: argparse.Namespace) -> int:
    for project in list_registered_projects():
        print(f"{project.id}\t{project.name}\t{project.root}")
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


def cmd_request(args: argparse.Namespace) -> int:
    config = load_config()
    body = args.body if args.body is not None else sys.stdin.read()
    sender = args.sender or config.default_sender
    _ensure_participant(config, sender, role="sender")
    provenance = _provenance_payload_from_args(args)
    if provenance is None:
        return 2
    raw_to = args.to
    recipients = config.canonical_recipients(raw_to)
    _ensure_participants(config, recipients, role="recipient")
    request_id = _new_public_id("REQ", sender)
    payload: dict[str, Any] = {
        "from": sender,
        "to": recipients,
        "title": args.title,
        "body": body,
        "feature": args.feature,
        "refs": args.ref,
        "response_mode": args.response_mode,
    }
    payload.update(provenance)
    if config.routing.preserve_raw_to:
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
        print("agent-mesh: --source-role must be non-empty when source refs are provided", file=sys.stderr)
        return None
    if source_channel and source_uri and (args.body_authority is None or args.body_fidelity is None):
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
    return payload


def cmd_reply(args: argparse.Namespace) -> int:
    config = load_config()
    body = args.details if args.details is not None else sys.stdin.read()
    sender = args.sender or config.default_sender
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
            _ensure_response_allowed(config, request_id=parent["request_id"], request_payload=request_payload)
        response_id = _new_public_id("RES", sender)
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
                "refs": args.ref,
                **provenance,
            },
        )
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
            "install, status, start, restart, or uninstall",
            file=sys.stderr,
        )
        return 2

    bookmark_path: Path | None = None
    try:
        if action == "install":
            _validate_workbench_host(args.host)
            config = load_config(args.repo)
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
        elif action == "start":
            status = start_workbench_service()
        elif action == "restart":
            status = restart_workbench_service()
        else:
            status = uninstall_workbench_service()
    except (ConfigError, ProjectRegistryError, WorkbenchServiceError, WorkbenchError) as exc:
        print(f"agent-mesh: {exc}", file=sys.stderr)
        return 2

    _print_workbench_service_status(status)
    if action in {"install", "start", "restart"}:
        if bookmark_path is None:
            bookmark_path = _service_bookmark_path(status, managed_workbench_bookmark_path)
        if bookmark_path is not None:
            if not wait_for_managed_workbench(bookmark_path):
                print(
                    "agent-mesh: the automatic Workbench service was registered but did not "
                    "become reachable within 10 seconds; check service status and logs",
                    file=sys.stderr,
                )
                return 2
            print(f"bookmark: {bookmark_path}")
            if args.open:
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


def _print_workbench_service_status(status: Any) -> None:
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


def cmd_decision_propose(args: argparse.Namespace) -> int:
    config = load_config()
    dec_ulid = _new_decision_id()
    body = _decision_body_from_args(args)
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
        "affected_code_globs": args.affects,
        "exemptions": [],
        "generated_artifact_paths": [],
        "assumptions": [],
        "evidence": {},
        "supersedes": None,
        "owner": args.owner,
        "review_policy": {},
        "required_checks": [],
        "verification": [],
        "tags": args.tag,
        "body_sha": body_sha,
        "body_path": body_path,
        "body_bytes": body_bytes,
    }
    event = Event(
        event_id=generate_event_id(),
        actor=args.owner or config.default_sender,
        kind="decision_proposed",
        entity_id=dec_ulid,
        thread_id=dec_ulid,
        payload=payload,
    )
    append_event(config.events_path, event, lock_acquired=False)
    print(f"{args.human_id} {dec_ulid}")
    return 0


def cmd_decision_accept(args: argparse.Namespace) -> int:
    config = load_config()
    dec_ulid = _resolve_decision_or_die(config, args.identifier)
    event = Event(
        event_id=generate_event_id(),
        actor=args.by or config.default_sender,
        kind="decision_accepted",
        entity_id=dec_ulid,
        thread_id=dec_ulid,
        payload={"decision_id": dec_ulid, "accepted_by": args.by or config.default_sender, "notes": args.notes},
    )
    append_event(config.events_path, event, lock_acquired=False)
    print(f"accepted {args.identifier}")
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
        actor=config.default_sender,
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
        actor=config.default_sender,
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
        actor=config.default_sender,
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
    "lane",
    "notes",
    "refs",
}


def cmd_backlog_upsert(args: argparse.Namespace) -> int:
    config = load_config()
    actor = args.actor or config.default_sender
    _ensure_participant(config, actor, role="actor")
    json_payload = _read_optional_json_payload(args)
    item_id = args.item_id or str(json_payload.get("id", "")).strip()
    if not item_id:
        raise ConfigError("backlog upsert requires --id or JSON field id")

    lock_handle = acquire(config.agent_dir / ".mail-lock")
    last_event_seq = None
    try:
        existing = _backlog_item_payload(config, item_id)
        payload = _merge_backlog_payload(existing, json_payload, args, item_id=item_id)
        if not payload.get("title"):
            raise ConfigError(
                f"backlog upsert requires --title for new item {item_id}; "
                "partial updates are only allowed after the item exists"
            )
        event = Event(
            event_id=generate_event_id(),
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
    print(f"upserted {item_id}")
    return 0


def cmd_backlog_link(args: argparse.Namespace) -> int:
    config = load_config()
    actor = args.actor or config.default_sender
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
    actor = args.actor or config.default_sender
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
                        if conn.execute("SELECT 1 FROM backlog_items WHERE id=?", (token,)).fetchone() is None:
                            dangling.append({"ref": token, "file": rel, "line": line_no})
                    else:
                        warnings.append({"ref": token, "file": rel, "line": line_no, "reason": "unknown ref kind"})
    finally:
        conn.close()

    if args.record_scan:
        scanner_run_id = "scan_" + secrets.token_hex(8)
        event = Event(
            event_id=generate_event_id(),
            actor=config.default_sender,
            kind="decision_scanner_run_completed",
            entity_id=scanner_run_id,
            thread_id=scanner_run_id,
            payload={
                "scanner_run_id": scanner_run_id,
                "paths_scanned": [path.relative_to(config.project_root).as_posix() for path in paths],
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
    actor = args.actor or config.default_sender
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
                "lane": row["lane"],
                "notes": row["notes"],
                "refs": json_loads(row["refs_json"], []),
            }
        )
        return payload
    finally:
        conn.close()


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
        "lane": "lane",
        "notes": "notes",
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
    payload["refs"] = refs

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
            "RES_DUPLICATE_FOR_SINGLE_MODE_REQ: "
            f"{request_id} already has response {existing}"
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
            and any(fnmatch.fnmatch(path.relative_to(config.project_root).as_posix(), pattern) for pattern in patterns)
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
