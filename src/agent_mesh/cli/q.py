"""agent-q CLI — read side."""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from agent_mesh.config import ConfigError, load_config
from agent_mesh.core.events import Event, append_event, generate_event_id
from agent_mesh.core.external_recovery_plan import (
    ExternalRecoveryPlanError,
    build_external_recovery_plan_report,
)
from agent_mesh.core.chain import verify_chain
from agent_mesh.core.lock import acquire
from agent_mesh.core.recovery import RecoveryStopLine, recover
from agent_mesh.message_packet import build_message_packet
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
from agent_mesh.store.rebuild import DecisionStopLine, DispatchStopLine, rebuild_all
from agent_mesh.store.sqlite import (
    body_from_message_row,
    connect,
    initialize_schema,
    json_loads,
    resolve_decision,
    resolve_dispatch_run,
    resolve_message,
)
from agent_mesh.views import locate_message, render_all
from agent_mesh.adapters.base import AdapterSpec
from agent_mesh.dispatch import extract_response_candidate, plan_for, to_message
from agent_mesh.dispatch.adapters import DispatchHost
from agent_mesh.dispatch.execution import execute_launch_plan
from agent_mesh.dispatch.runtime import AgentProcessLauncher, CodexCliRuntimeAdapter
from agent_mesh.dispatch.types import Message


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return int(exc.code or 0)
    try:
        return int(args.func(args))
    except ConfigError as exc:
        print(f"agent-q: {exc}", file=sys.stderr)
        return 2
    except (DecisionStopLine, DispatchStopLine) as exc:
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
    parser = argparse.ArgumentParser(prog="agent-q")
    sub = parser.add_subparsers(dest="command", required=True)

    list_cmd = sub.add_parser("list")
    list_cmd.add_argument("--status")
    list_cmd.add_argument("--to")
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
    events.add_argument("--json", action="store_true")
    events.set_defaults(func=cmd_events)

    backlog = sub.add_parser("backlog")
    backlog_sub = backlog.add_subparsers(dest="backlog_command", required=True)
    backlog_list = backlog_sub.add_parser("list")
    backlog_list.add_argument("--status")
    backlog_list.add_argument("--lane")
    backlog_list.set_defaults(func=cmd_backlog_list)
    backlog_get = backlog_sub.add_parser("get")
    backlog_get.add_argument("item_id")
    backlog_get.set_defaults(func=cmd_backlog_get)
    backlog_events = backlog_sub.add_parser("events")
    backlog_events.add_argument("item_id")
    backlog_events.set_defaults(func=cmd_backlog_events)

    decisions = sub.add_parser("decisions")
    decision_sub = decisions.add_subparsers(dest="decision_command", required=True)
    dec_list = decision_sub.add_parser("list")
    dec_list.add_argument("--status")
    dec_list.add_argument("--tier")
    dec_list.add_argument("--scope")
    dec_list.add_argument("--owner")
    dec_list.set_defaults(func=cmd_decisions_list)

    show = decision_sub.add_parser("show")
    show.add_argument("identifier")
    show.add_argument("--references", action="store_true")
    show.add_argument("--evidence", action="store_true")
    show.add_argument("--assumptions", action="store_true")
    show.set_defaults(func=cmd_decisions_show)

    log = decision_sub.add_parser("log")
    log.add_argument("identifier")
    log.set_defaults(func=cmd_decisions_log)

    search = decision_sub.add_parser("search")
    search.add_argument("query")
    search.set_defaults(func=cmd_decisions_search)

    at = decision_sub.add_parser("at")
    at.add_argument("path")
    at.set_defaults(func=cmd_decisions_at)

    verify_dec = decision_sub.add_parser("verify")
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
    disp_once = dispatch_sub.add_parser("once")
    disp_once.add_argument("--live", action="store_true")
    disp_once.add_argument("--target", required=True)
    disp_once.add_argument("--message", required=True)
    disp_once.add_argument("--timeout-seconds", type=int, default=3600)
    disp_once.add_argument("--post-response", action="store_true")
    disp_once.set_defaults(func=cmd_dispatches_once)
    disp_worker = dispatch_sub.add_parser("worker")
    disp_worker.add_argument("--live", action="store_true")
    disp_worker.add_argument("--target", required=True)
    disp_worker.add_argument("--max-runs", type=int, default=1)
    disp_worker.add_argument("--timeout-seconds", type=int, default=3600)
    disp_worker.add_argument("--message", action="append", default=[])
    disp_worker.add_argument("--after-event-seq", type=int, default=None)
    disp_worker.add_argument("--post-response", action="store_true")
    disp_worker.set_defaults(func=cmd_dispatches_worker)
    return parser


def cmd_list(args: argparse.Namespace) -> int:
    config = load_config()
    _rebuild_all_locked(config)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        sql = "SELECT * FROM messages WHERE kind='request'"
        params: list[str] = []
        if args.status:
            sql += " AND status=?"
            params.append(args.status)
        if args.to:
            sql += " AND recipients_json LIKE ?"
            params.append(f"%{args.to}%")
        sql += " ORDER BY created_utc DESC, event_seq DESC"
        rows = conn.execute(sql, params).fetchall()
        if args.json:
            print(json.dumps([{key: row[key] for key in row.keys()} for row in rows], indent=2))
        else:
            for row in rows:
                print(f"{row['id']}\t{row['status']}\t{row['sender']}\t{row['title']}")
    finally:
        conn.close()
    return 0


def cmd_locate(args: argparse.Namespace) -> int:
    config = load_config()
    _render_all_locked(config)
    found = locate_message(config, args.message_id)
    if not found:
        print(f"agent-q locate: not found: {args.message_id}", file=sys.stderr)
        return 1
    path, start, end = found
    print(f"{path}:{start}-{end}")
    return 0


def cmd_body(args: argparse.Namespace) -> int:
    config = load_config()
    _rebuild_all_locked(config)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        row = resolve_message(conn, args.message_id)
        if row is None:
            print(f"agent-q body: not found: {args.message_id}", file=sys.stderr)
            return 1
        print(body_from_message_row(row), end="" if body_from_message_row(row).endswith("\n") else "\n")
    finally:
        conn.close()
    return 0


def cmd_packet(args: argparse.Namespace) -> int:
    config = load_config()
    _rebuild_all_locked(config)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
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
    finally:
        conn.close()
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
    _rebuild_all_locked(config)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
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
                f"{row['id']}\t{row['kind']}\tparent={parent}\t"
                f"from={row['sender']}\t{label or ''}"
            )
    finally:
        conn.close()
    return 0


def cmd_trace(args: argparse.Namespace) -> int:
    config = load_config()
    _rebuild_all_locked(config)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
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
    finally:
        conn.close()
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


def cmd_status(args: argparse.Namespace) -> int:
    config = load_config()
    _rebuild_all_locked(config)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        last_seq = conn.execute("SELECT last_event_seq FROM events_seen").fetchone()[0]
        open_count = conn.execute(
            "SELECT COUNT(*) FROM messages WHERE kind='request' AND status='open'"
        ).fetchone()[0]
    finally:
        conn.close()
    print(f"project: {config.project_root}")
    print(f"last_event_seq: {last_seq}")
    print(f"open_requests: {open_count}")
    if args.writes:
        print(f"lock_present: {(config.agent_dir / '.mail-lock').exists()}")
    return 0


def cmd_events(args: argparse.Namespace) -> int:
    config = load_config()
    rows = []
    for record in _event_records(config.events_path):
        if args.kind and record.get("kind") != args.kind:
            continue
        if args.thread and record.get("thread_id") != args.thread:
            continue
        rows.append(record)
    if args.json:
        print(json.dumps(rows, indent=2, sort_keys=True))
    else:
        for record in rows:
            print(
                f"{record['event_seq']}\t{record['kind']}\t{record['entity_id']}\t"
                f"actor={record['actor']}\tthread={record['thread_id']}"
            )
    return 0


def cmd_backlog_list(args: argparse.Namespace) -> int:
    config = load_config()
    _rebuild_all_locked(config)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        sql = "SELECT * FROM backlog_items WHERE 1=1"
        params: list[str] = []
        if args.status:
            sql += " AND status=?"
            params.append(args.status)
        if args.lane:
            sql += " AND lane=?"
            params.append(args.lane)
        sql += " ORDER BY priority ASC, updated_utc DESC, id ASC"
        for row in conn.execute(sql, params):
            print(f"{row['id']}\t{row['status']}\t{row['lane'] or ''}\t{row['priority'] or ''}\t{row['title']}")
    finally:
        conn.close()
    return 0


def cmd_backlog_get(args: argparse.Namespace) -> int:
    config = load_config()
    _rebuild_all_locked(config)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        row = conn.execute("SELECT * FROM backlog_items WHERE id=?", (args.item_id,)).fetchone()
        if row is None:
            print(f"agent-q backlog get: not found: {args.item_id}", file=sys.stderr)
            return 1
        print(f"id: {row['id']}")
        print(f"title: {row['title']}")
        print(f"status: {row['status']}")
        print(f"lane: {row['lane'] or ''}")
        print(f"priority: {row['priority'] or ''}")
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
    finally:
        conn.close()
    return 0


def cmd_backlog_events(args: argparse.Namespace) -> int:
    config = load_config()
    _rebuild_all_locked(config)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
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
    finally:
        conn.close()
    return 0


def cmd_decisions_list(args: argparse.Namespace) -> int:
    config = load_config()
    _rebuild_all_locked(config)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        sql = "SELECT * FROM decisions WHERE 1=1"
        params: list[str] = []
        if args.status:
            sql += " AND status=?"
            params.append(args.status)
        if args.tier:
            sql += " AND tier=?"
            params.append(args.tier)
        if args.owner:
            sql += " AND owner=?"
            params.append(args.owner)
        if args.scope:
            sql += (
                " AND dec_ulid IN (SELECT dec_ulid FROM decision_globs "
                "WHERE kind='affected' AND ? GLOB pattern)"
            )
            params.append(args.scope)
        sql += " ORDER BY human_id"
        for row in conn.execute(sql, params):
            print(f"{row['human_id']}\t{row['status']}\t{row['tier']}\t{row['title']}")
    finally:
        conn.close()
    return 0


def cmd_decisions_show(args: argparse.Namespace) -> int:
    config = load_config()
    _rebuild_all_locked(config)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        dec_ulid = resolve_decision(conn, args.identifier)
        if dec_ulid is None:
            print(f"agent-q decisions show: not found: {args.identifier}", file=sys.stderr)
            return 1
        row = conn.execute("SELECT * FROM decisions WHERE dec_ulid=?", (dec_ulid,)).fetchone()
        print(f"{row['human_id']} {row['dec_ulid']}")
        print(f"title: {row['title']}")
        print(f"tier: {row['tier']}")
        print(f"status: {row['status']}")
        print(f"owner: {row['owner'] or ''}")
        if args.assumptions:
            for item in conn.execute(
                "SELECT assumption_id, status, text FROM decision_assumptions "
                "WHERE dec_ulid=? ORDER BY assumption_id",
                (dec_ulid,),
            ):
                print(f"assumption {item['assumption_id']} [{item['status']}]: {item['text']}")
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
                print(f"reference {item['file_path']}:{item['line_start']} {item['reference_form']}")
    finally:
        conn.close()
    return 0


def cmd_decisions_log(args: argparse.Namespace) -> int:
    config = load_config()
    _rebuild_all_locked(config)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        dec_ulid = resolve_decision(conn, args.identifier)
        if dec_ulid is None:
            print(f"agent-q decisions log: not found: {args.identifier}", file=sys.stderr)
            return 1
        for record in _event_records(config.events_path):
            if record.get("entity_id") == dec_ulid or record.get("payload", {}).get("decision_id") == dec_ulid:
                print(f"{record['event_seq']}\t{record['kind']}\t{record['occurred_utc']}\t{record['event_id']}")
    finally:
        conn.close()
    return 0


def cmd_decisions_search(args: argparse.Namespace) -> int:
    config = load_config()
    _rebuild_all_locked(config)
    query = args.query.lower()
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        rows = conn.execute("SELECT * FROM decisions ORDER BY human_id").fetchall()
        for row in rows:
            meta = json_loads(row["meta_json"], {})
            haystack = " ".join(
                [row["human_id"], row["title"], str(meta.get("context", "")), str(meta.get("decision", ""))]
            ).lower()
            if query in haystack:
                print(f"{row['human_id']}\t{row['status']}\t{row['title']}")
    finally:
        conn.close()
    return 0


def cmd_decisions_at(args: argparse.Namespace) -> int:
    import fnmatch

    config = load_config()
    _rebuild_all_locked(config)
    rel = Path(args.path).as_posix()
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        rows = conn.execute(
            "SELECT d.human_id, d.status, d.title, g.pattern FROM decisions d "
            "JOIN decision_globs g ON d.dec_ulid=g.dec_ulid WHERE g.kind='affected' "
            "ORDER BY d.human_id"
        ).fetchall()
        for row in rows:
            if fnmatch.fnmatch(rel, row["pattern"]):
                print(f"{row['human_id']}\t{row['status']}\t{row['pattern']}\t{row['title']}")
    finally:
        conn.close()
    return 0


def cmd_decisions_verify(args: argparse.Namespace) -> int:
    config = load_config()
    _rebuild_all_locked(config)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        dec_ulid = resolve_decision(conn, args.identifier)
        if dec_ulid is None:
            print(f"agent-q decisions verify: not found: {args.identifier}", file=sys.stderr)
            return 1
        rows = conn.execute(
            "SELECT command, expected_signal FROM decision_verifications WHERE dec_ulid=? "
            "ORDER BY command",
            (dec_ulid,),
        ).fetchall()
    finally:
        conn.close()

    failed = 0
    for row in rows:
        result = subprocess.run(
            row["command"],
            cwd=config.project_root,
            shell=True,
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode == 0:
            print(f"pass: {row['command']}")
            continue
        failed += 1
        print(f"fail: {row['command']}", file=sys.stderr)
        event = Event(
            event_id=generate_event_id(),
            actor=config.default_sender,
            kind="decision_drift_detected",
            entity_id=dec_ulid,
            thread_id=dec_ulid,
            payload={
                "decision_id": dec_ulid,
                "command": row["command"],
                "expected_signal": row["expected_signal"],
                "actual_signal": f"exit {result.returncode}",
                "last_verified_utc_before": None,
            },
        )
        append_event(config.events_path, event, lock_acquired=False)
    if not rows:
        print("no verification commands")
    return 1 if failed else 0


def cmd_dispatches_once(args: argparse.Namespace) -> int:
    if not args.live:
        print("agent-q dispatches once: --live is required", file=sys.stderr)
        return 2
    if args.target != "codex":
        print("agent-q dispatches once: only --target codex is supported in this slice", file=sys.stderr)
        return 2
    config = load_config()
    lock_handle = acquire(config.agent_dir / ".mail-lock")
    try:
        rebuild_all(config)
        request_event = _request_event_for_message(config.events_path, args.message)
        if request_event is None:
            print(f"agent-q dispatches once: request not found: {args.message}", file=sys.stderr)
            return 1
        message = to_message(request_event, aliases=config.routing.aliases)
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
                print(f"agent-q dispatches once: request not found: {args.message}", file=sys.stderr)
                return 1
            if _open_dispatch_lease_exists(conn, message.entity_id, args.target):
                print(
                    f"agent-q dispatches once: open lease already exists for {message.entity_id}/{args.target}",
                    file=sys.stderr,
                )
                return 1
            response_mode = str(request_event.get("payload", {}).get("response_mode") or "single")
            if response_mode not in {"single", "multi"}:
                print(f"agent-q dispatches once: invalid response_mode for {message.entity_id}", file=sys.stderr)
                return 1
            if args.post_response and response_mode == "single" and _direct_response_exists(conn, message.entity_id):
                print(
                    f"agent-q dispatches once: request {message.entity_id} already has response",
                    file=sys.stderr,
                )
                return 1
            prompt = _launch_prompt(message, body_from_message_row(row))
        finally:
            conn.close()

        host = _CliDispatchHost(config, args.target)
        plan = plan_for(message, args.target, host, grounding={"complete": True})
        runtime = CodexCliRuntimeAdapter(
            AdapterSpec(name="codex-cli", domain="agent_runtime", privacy_class="project_private")
        )
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


def _dispatch_message_locked(config, *, target: str, message_id: str, timeout_seconds: int, post_response: bool):
    request_event = _request_event_for_message(config.events_path, message_id)
    if request_event is None:
        raise ValueError(f"request not found: {message_id}")
    message = to_message(request_event, aliases=config.routing.aliases)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        row = resolve_message(conn, message.entity_id)
        if row is None or row["kind"] != "request":
            raise ValueError(f"request not found: {message_id}")
        prompt = _launch_prompt(message, body_from_message_row(row))
    finally:
        conn.close()
    host = _CliDispatchHost(config, target)
    plan = plan_for(message, target, host, grounding={"complete": True})
    runtime = CodexCliRuntimeAdapter(
        AdapterSpec(name="codex-cli", domain="agent_runtime", privacy_class="project_private")
    )
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
            plan = plan_for(message, target, _CliDispatchHost(config, target), grounding={"complete": True})
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
    if args.target != "codex":
        print("agent-q dispatches worker: only --target codex is supported in this slice", file=sys.stderr)
        return 2
    if args.max_runs < 1:
        print("agent-q dispatches worker: --max-runs must be >= 1", file=sys.stderr)
        return 2
    if args.after_event_seq is not None and args.after_event_seq < 0:
        print("agent-q dispatches worker: --after-event-seq must be >= 0", file=sys.stderr)
        return 2
    config = load_config()
    message_ids = set(args.message) if args.message else None
    runs = 0
    stopped = "max-runs"
    exit_code = 0
    while runs < args.max_runs:
        lock_handle = acquire(config.agent_dir / ".mail-lock")
        try:
            rebuild_all(config)
            eligible = _eligible_dispatch_request_ids(
                config,
                target=args.target,
                post_response=args.post_response,
                message_ids=message_ids,
                after_event_seq=args.after_event_seq,
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
            )
        finally:
            lock_handle.release()
        runs += 1
        terminal, candidate_status, candidate_reason = _dispatch_terminal(result, post_response=args.post_response)
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
    def __init__(self, config, target: str) -> None:
        super().__init__(
            AdapterSpec(name="cli-dispatch-host", domain="dispatch", privacy_class="project_private")
        )
        self.config = config
        self.target = target

    def routes(self) -> dict[str, dict[str, object]]:
        return {self.target: {"gen_ai_system": "openai", "model": "codex-cli"}}

    def classify(self, message: Message) -> str:
        return "routine"

    def wave_for_refs(self, refs: list[str]) -> str | None:
        return None

    def post_reply_template(self) -> str:
        return "agent-mesh reply placeholder for {agent} {req_id}"

    def build_command(self, agent: str, message: Message, session_uuid: str) -> str:
        return f"agent-q dispatches once --live --target {agent} --message {message.entity_id}"

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
        WHERE input_message_id=? AND target_agent=? AND status IN ('completed', 'failed', 'timeout', 'launch_error')
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


def _launch_prompt(message: Message, body: str) -> str:
    return (
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


def cmd_dispatches_list(args: argparse.Namespace) -> int:
    config = load_config()
    _rebuild_all_locked(config)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
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
    finally:
        conn.close()
    return 0


def cmd_dispatches_show(args: argparse.Namespace) -> int:
    config = load_config()
    _rebuild_all_locked(config)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
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
    finally:
        conn.close()
    return 0


def cmd_dispatches_log(args: argparse.Namespace) -> int:
    config = load_config()
    _rebuild_all_locked(config)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        run_id = resolve_dispatch_run(conn, args.identifier)
        if run_id is None:
            print(f"agent-q dispatches log: not found: {args.identifier}", file=sys.stderr)
            return 1
    finally:
        conn.close()
    for record in _event_records(config.events_path):
        payload = record.get("payload", {}) or {}
        if record.get("entity_id") == run_id or payload.get("run_id") == run_id:
            print(
                f"{record['event_seq']}\t{record['kind']}\t"
                f"{record['occurred_utc']}\t{record['event_id']}"
            )
    return 0


def cmd_dispatches_status(args: argparse.Namespace) -> int:
    config = load_config()
    _rebuild_all_locked(config)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        for row in conn.execute(
            "SELECT status, COUNT(*) AS n FROM dispatch_runs GROUP BY status ORDER BY status"
        ):
            print(f"runs {row['status']}\t{row['n']}")
        for row in conn.execute(
            "SELECT status, COUNT(*) AS n FROM dispatch_leases GROUP BY status ORDER BY status"
        ):
            print(f"leases {row['status']}\t{row['n']}")
    finally:
        conn.close()
    return 0


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
    _rebuild_all_locked(config)
    issues: list[str] = []
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
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
            if conn.execute(
                "SELECT 1 FROM dispatch_runs WHERE run_id=?", (lease["run_id"],)
            ).fetchone() is None:
                issues.append(f"DISPATCH_LEASE_UNKNOWN_RUN: {lease['lease_id']}")
        # re-assert the uq_dispatch_leases_open invariant independently of the index
        for dup in conn.execute(
            "SELECT input_message_id, target_agent, COUNT(*) AS n FROM dispatch_leases "
            "WHERE status='open' GROUP BY input_message_id, target_agent HAVING n > 1"
        ):
            issues.append(f"DISPATCH_LEASE_DUPLICATE: {dup['input_message_id']}/{dup['target_agent']}")
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
    finally:
        conn.close()
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
