from __future__ import annotations

import hashlib
import json
import math
import os
import subprocess
import sys
from pathlib import Path

from agent_mesh.config import default_config_text, load_config
from agent_mesh.core.events import Event, append_event, generate_event_id
from agent_mesh.store.rebuild import rebuild_all
from agent_mesh.store.sqlite import connect, initialize_schema, json_loads, resolve_decision
from agent_mesh.views import render_all


def main() -> int:
    n = int(sys.argv[1])
    if n < 1:
        raise SystemExit("N must be >= 1")
    workdir = Path(os.environ["AGENT_MESH_EXAMPLE_WORKDIR"]).resolve()
    workdir.mkdir(parents=True, exist_ok=True)
    os.chdir(workdir)

    participants = [f"agent-{index}" for index in range(1, n + 1)]
    run_mail(
        [
            "init",
            "--participants",
            ",".join(participants),
            "--default-sender",
            participants[0],
            "--no-register",
        ]
    )
    write_config(participants)

    scenario_solo_write(participants)
    scenario_broadcast(participants)
    scenario_response_modes(participants)
    scenario_multi_reviewer_decision(participants)
    scenario_triage_disagreement(participants)
    scenario_outboxes(participants)
    scenario_alias_all(participants)
    scenario_typo_rejection(participants)
    scenario_add_remove_agent(participants)

    print(f"n-agent ok: N={n} workdir={workdir}")
    return 0


def scenario_solo_write(participants: list[str]) -> None:
    req = request(participants[0], participants[0], "Solo write", "Self-addressed request.")
    respond(participants[0], req, "Solo response", "Self-response accepted.")


def scenario_broadcast(participants: list[str]) -> None:
    target = "others" if len(participants) > 1 else participants[0]
    req = request(participants[0], target, "Broadcast request", "Broadcast fixture.")
    config = load_config(Path.cwd())
    rebuild_all(config)
    row = message_row(req)
    recipients = json_loads(row["recipients_json"], [])
    expected = participants[1:] if len(participants) > 1 else [participants[0]]
    assert recipients == expected


def scenario_response_modes(participants: list[str]) -> None:
    req_single = request(participants[0], participants[0], "Single mode", "First response wins.")
    respond(participants[0], req_single, "First", "Accepted.")
    duplicate = run_mail(
        ["respond", "--from", participants[0], req_single, "Duplicate", "Rejected."],
        check=False,
    )
    assert duplicate.returncode != 0
    assert "RES_DUPLICATE_FOR_SINGLE_MODE_REQ" in duplicate.stderr

    req_multi = request(
        participants[0],
        "all",
        "Multi mode",
        "Panel response.",
        response_mode="multi",
    )
    responders = participants[1:3] if len(participants) >= 3 else participants[:1]
    processes = [
        subprocess.Popen(
            [
                sys.executable,
                "-m",
                "agent_mesh.cli.mail",
                "respond",
                "--from",
                responder,
                req_multi,
                f"Response from {responder}",
                "Accepted under multi mode.",
            ],
            cwd=Path.cwd(),
            env=os.environ.copy(),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        for responder in responders
    ]
    for process in processes:
        stdout, stderr = process.communicate(timeout=20)
        assert process.returncode == 0, (stdout, stderr)
    rebuild_all(load_config(Path.cwd()))
    assert response_count(req_multi) == len(responders)


def scenario_multi_reviewer_decision(participants: list[str]) -> None:
    config = load_config(Path.cwd())
    body_path, body_sha, body_bytes = decision_body(config.agent_dir, "N-agent decision body")
    reviewers = participants[1:]
    quorum = math.ceil(len(reviewers) / 2) if reviewers else 0
    dec_ulid = "dec_n_agent"
    append(
        "decision_proposed",
        dec_ulid,
        dec_ulid,
        participants[0],
        {
            "human_id": "D900",
            "aliases": [],
            "title": "N-agent quorum fixture",
            "tier": "architecture_contract",
            "context": "N-agent fixture",
            "decision": "Quorum scales with participant count.",
            "rejected_alternatives": [],
            "consequences": [],
            "affected_code_globs": [],
            "exemptions": [],
            "generated_artifact_paths": [],
            "assumptions": [],
            "evidence": {},
            "supersedes": None,
            "owner": participants[0],
            "review_policy": {
                "required_reviewers": reviewers,
                "approval_quorum": quorum,
            },
            "required_checks": [],
            "verification": [],
            "tags": ["n-agent"],
            "body_sha": body_sha,
            "body_path": body_path,
            "body_bytes": body_bytes,
        },
    )
    rebuild_all(config)
    assert decision_status("D900") == "proposed"
    if not reviewers:
        return
    for reviewer in reviewers[: max(0, quorum - 1)]:
        accept_decision(dec_ulid, reviewer)
    rebuild_all(config)
    assert decision_status("D900") == "proposed"
    accept_decision(dec_ulid, reviewers[quorum - 1])
    rebuild_all(config)
    assert decision_status("D900") == "in_force"


def scenario_triage_disagreement(participants: list[str]) -> None:
    obs_id = "OBS-N-AGENT-1"
    append(
        "ui_observation_raw",
        obs_id,
        obs_id,
        participants[0],
        {"surface": "fixture", "body": "Ambiguous UI signal."},
    )
    enrichers = participants[: min(3, len(participants))]
    for index, agent in enumerate(enrichers, start=1):
        append(
            "ui_observation_enriched",
            f"{obs_id}-E{index}",
            obs_id,
            agent,
            {"observation_id": obs_id, "interpretation": f"interpretation-{index}"},
        )
    result = run_q(["events", "--kind", "ui_observation_enriched", "--thread", obs_id])
    assert result.stdout.count("ui_observation_enriched") == len(enrichers)


def scenario_outboxes(participants: list[str]) -> None:
    config = load_config(Path.cwd())
    rebuild_all(config)
    render_all(config)
    outboxes = sorted((config.views_dir).glob("outbox-*.md"))
    assert {path.name for path in outboxes} == {f"outbox-{name}.md" for name in participants}


def scenario_alias_all(participants: list[str]) -> None:
    req = request(participants[0], "all", "All alias", "Full-cardinality alias.")
    row = message_row(req)
    assert json_loads(row["recipients_json"], []) == participants
    assert json_loads(row["meta_json"], {})["original_to"] == "all"


def scenario_typo_rejection(participants: list[str]) -> None:
    result = run_mail(
        ["request", "--from", participants[0], "--to", "agent-typo", "Bad recipient", "Nope."],
        check=False,
    )
    assert result.returncode != 0
    assert "PARTICIPANT_UNKNOWN" in result.stderr


def scenario_add_remove_agent(participants: list[str]) -> None:
    removed = participants[-1]
    historical_req = request(
        participants[0],
        "all",
        "Historical removed-agent response",
        "Creates a historical outbox.",
        response_mode="multi",
    )
    respond(removed, historical_req, "Historical response", "Keep this outbox after removal.")

    added = [*participants, "agent-new"]
    write_config(added)
    config = load_config(Path.cwd())
    rebuild_all(config)
    render_all(config)
    new_outbox = config.views_dir / "outbox-agent-new.md"
    assert new_outbox.exists()
    assert "_No responses._" in new_outbox.read_text(encoding="utf-8")
    run_q(["verify-chain", ".agent-mesh/events.jsonl"])

    remaining = [name for name in added if name != removed]
    write_config(remaining)
    config = load_config(Path.cwd())
    rebuild_all(config)
    render_all(config)
    assert (config.views_dir / f"outbox-{removed}.md").exists()

    from_removed = run_mail(
        ["request", "--from", removed, "--to", remaining[0], "Removed sender", "Rejected."],
        check=False,
    )
    to_removed = run_mail(
        ["request", "--from", remaining[0], "--to", removed, "Removed recipient", "Rejected."],
        check=False,
    )
    assert from_removed.returncode != 0
    assert to_removed.returncode != 0
    assert "PARTICIPANT_UNKNOWN" in from_removed.stderr
    assert "PARTICIPANT_UNKNOWN" in to_removed.stderr


def request(
    sender: str,
    recipient: str,
    title: str,
    body: str,
    *,
    response_mode: str = "single",
) -> str:
    args = [
        "request",
        "--from",
        sender,
        "--to",
        recipient,
        "--response-mode",
        response_mode,
        title,
        body,
    ]
    return run_mail(args).stdout.strip().splitlines()[-1]


def respond(sender: str, request_id: str, summary: str, body: str) -> str:
    return run_mail(["respond", "--from", sender, request_id, summary, body]).stdout.strip().splitlines()[-1]


def accept_decision(dec_ulid: str, reviewer: str) -> None:
    append(
        "decision_accepted",
        dec_ulid,
        dec_ulid,
        reviewer,
        {"decision_id": dec_ulid, "accepted_by": reviewer, "notes": "accepted"},
    )


def append(kind: str, entity_id: str, thread_id: str, actor: str, payload: dict) -> None:
    config = load_config(Path.cwd())
    append_event(
        config.events_path,
        Event(
            event_id=generate_event_id(),
            actor=actor,
            kind=kind,
            entity_id=entity_id,
            thread_id=thread_id,
            payload=payload,
        ),
        lock_acquired=False,
    )


def write_config(participants: list[str]) -> None:
    text = default_config_text(
        participants=participants,
        default_sender=participants[0] if participants else "",
        default_recipient=participants[0] if participants else "",
    )
    aliases = {
        "all": participants,
        "others": participants[1:] if len(participants) > 1 else participants,
    }
    alias_lines = "\n".join(
        f"{name} = {json.dumps(values)}" for name, values in aliases.items()
    )
    text = text.replace("[routing.aliases]\n", f"[routing.aliases]\n{alias_lines}\n")
    Path(".agent-mesh/config.toml").write_text(text, encoding="utf-8")


def decision_body(agent_dir: Path, text: str) -> tuple[str, str, int]:
    data = text.encode("utf-8")
    body_sha = hashlib.sha256(data).hexdigest()
    relative = Path("bodies") / f"{body_sha}.md"
    target = agent_dir / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)
    return relative.as_posix(), body_sha, len(data)


def message_row(message_id: str):
    config = load_config(Path.cwd())
    rebuild_all(config)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        row = conn.execute("SELECT * FROM messages WHERE id=?", (message_id,)).fetchone()
        assert row is not None
        return row
    finally:
        conn.close()


def response_count(request_id: str) -> int:
    config = load_config(Path.cwd())
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        return int(
            conn.execute(
                "SELECT COUNT(*) FROM messages WHERE kind='response' AND request_id=?",
                (request_id,),
            ).fetchone()[0]
        )
    finally:
        conn.close()


def decision_status(human_id: str) -> str:
    config = load_config(Path.cwd())
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        dec_ulid = resolve_decision(conn, human_id)
        assert dec_ulid is not None
        row = conn.execute("SELECT status FROM decisions WHERE dec_ulid=?", (dec_ulid,)).fetchone()
        assert row is not None
        return str(row["status"])
    finally:
        conn.close()


def run_mail(args: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return run_module("agent_mesh.cli.mail", args, check=check)


def run_q(args: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    return run_module("agent_mesh.cli.q", args, check=check)


def run_module(module: str, args: list[str], *, check: bool) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        [sys.executable, "-m", module, *args],
        cwd=Path.cwd(),
        env=os.environ.copy(),
        text=True,
        capture_output=True,
        check=False,
    )
    if check and result.returncode != 0:
        raise AssertionError((module, args, result.returncode, result.stdout, result.stderr))
    return result


if __name__ == "__main__":
    raise SystemExit(main())
