from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from agent_mesh.cli import mail as mail_cli
from agent_mesh.cli import q as q_cli
from agent_mesh.config import load_config
from agent_mesh.core.chain import verify_chain
from agent_mesh.core.events import Event, append_event, generate_event_id
from agent_mesh.store.rebuild import (
    DECISION_TRANSITION_INVALID,
    DecisionStopLine,
    read_event_records,
)


def _init_project(root: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    root.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(root)
    assert (
        mail_cli.main(
            ["init", "--participants", "human,agent", "--default-sender", "human", "--no-register"]
        )
        == 0
    )
    capsys.readouterr()


def test_smoke_request_response_and_bounded_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _init_project(tmp_path, monkeypatch, capsys)

    assert (
        mail_cli.main(
            ["request", "--from", "human", "--to", "agent", "Review change", "Check the patch."]
        )
        == 0
    )
    request_id = capsys.readouterr().out.strip().splitlines()[-1]
    assert request_id.startswith("REQ-")

    assert mail_cli.main(["respond", "--from", "agent", request_id, "Reviewed", "No blocker."]) == 0
    response_id = capsys.readouterr().out.strip().splitlines()[-1]
    assert response_id.startswith("RES-")

    assert q_cli.main(["packet", "--id", request_id]) == 0
    packet = capsys.readouterr().out
    assert request_id in packet
    assert response_id in packet
    assert "Check the patch." in packet
    assert "No blocker." in packet


def test_hash_chain_detects_tampering(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _init_project(tmp_path, monkeypatch, capsys)
    assert mail_cli.main(["request", "--to", "agent", "Integrity check", "Append one event."]) == 0
    capsys.readouterr()
    events_path = load_config(tmp_path).events_path

    clean = verify_chain(events_path)
    assert clean.ok and clean.verified == 1

    data = events_path.read_text(encoding="utf-8")
    events_path.write_text(
        data.replace('"prev_event_hash":"' + "0" * 64, '"prev_event_hash":"' + "1" * 64, 1),
        encoding="utf-8",
    )
    tampered = verify_chain(events_path)
    assert not tampered.ok
    assert tampered.error is not None
    assert "prev_event_hash mismatch" in tampered.error


def test_invalid_decision_transition_never_reaches_the_log(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _init_project(tmp_path, monkeypatch, capsys)
    assert (
        mail_cli.main(
            [
                "decision",
                "propose",
                "--id",
                "D001",
                "--title",
                "Keep approval explicit",
                "--tier",
                "note",
                "--decision",
                "Only a proposed decision may be rejected.",
            ]
        )
        == 0
    )
    capsys.readouterr()
    config = load_config(tmp_path)
    proposal = list(read_event_records(config.events_path))[0]
    decision_id = str(proposal["entity_id"])
    before = config.events_path.read_bytes()

    with pytest.raises(DecisionStopLine) as stopped:
        append_event(
            config.events_path,
            Event(
                event_id=generate_event_id(),
                actor="human",
                kind="decision_retired",
                entity_id=decision_id,
                thread_id=decision_id,
                payload={"decision_id": decision_id, "reason": "Invalid while proposed"},
            ),
        )

    assert stopped.value.code == DECISION_TRANSITION_INVALID
    assert config.events_path.read_bytes() == before
    assert q_cli.main(["decisions", "diagnose"]) == 0
    assert "decision replay: OK (1 events)" in capsys.readouterr().out


def test_privacy_defaults_and_git_shared_allowlist(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    local_project = tmp_path / "local"
    local_project.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=local_project, check=True)
    _init_project(local_project, monkeypatch, capsys)
    local_policy = (local_project / ".agent-mesh" / ".gitignore").read_text(encoding="utf-8")
    assert local_policy.rstrip().endswith("*")
    assert _untracked(local_project) == []

    shared_project = tmp_path / "shared"
    shared_project.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=shared_project, check=True)
    monkeypatch.chdir(shared_project)
    assert (
        mail_cli.main(
            [
                "init",
                "--participants",
                "human,agent",
                "--state-sharing",
                "git-shared",
                "--no-register",
            ]
        )
        == 0
    )
    capsys.readouterr()
    agent_dir = shared_project / ".agent-mesh"
    (agent_dir / "attachments").mkdir()
    (agent_dir / "attachments" / "private.txt").write_text("local attachment", encoding="utf-8")
    policy = (agent_dir / ".gitignore").read_text(encoding="utf-8")
    assert "!config.toml" in policy
    assert "!events.jsonl" in policy
    assert "!bodies/**" in policy
    assert "attachments" not in policy
    assert _untracked(shared_project) == [
        ".agent-mesh/.gitignore",
        ".agent-mesh/config.toml",
        ".agent-mesh/events.jsonl",
    ]


def _untracked(root: Path) -> list[str]:
    result = subprocess.run(
        ["git", "ls-files", "--others", "--exclude-standard"],
        cwd=root,
        check=True,
        text=True,
        capture_output=True,
    )
    return result.stdout.splitlines()
