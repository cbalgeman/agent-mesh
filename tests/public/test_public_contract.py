from __future__ import annotations

import io
import json
import subprocess
from pathlib import Path

import pytest

from agent_mesh.cli import mail as mail_cli
from agent_mesh.cli import q as q_cli
from agent_mesh.config import load_config
from agent_mesh.core.chain import verify_chain
from agent_mesh.core.events import Event, append_event, generate_event_id
from agent_mesh.core.context_delivery import load_lifecycle_mapping
from agent_mesh.store.rebuild import (
    DECISION_TRANSITION_INVALID,
    DecisionStopLine,
    read_event_records,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class _InteractiveInput(io.StringIO):
    def isatty(self) -> bool:
        return True


def _init_project(
    root: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
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


def test_decision_hook_returns_complete_empty_context(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _init_project(tmp_path, monkeypatch, capsys)
    monkeypatch.setattr(
        q_cli.sys,
        "stdin",
        io.StringIO(
            json.dumps(
                {
                    "schema": "agent-mesh.decision-hook-request.v1",
                    "boundary": "write",
                    "paths": ["src/example.py"],
                }
            )
        ),
    )

    assert q_cli.main(["decisions", "hook"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["schema"] == "agent-mesh.decision-context.v1"
    assert result["privacy_class"] == "project_private"
    assert result["context_status"] == "complete"
    assert result["complete"] is True
    assert result["request"] == {"boundary": "write", "paths": ["src/example.py"]}
    assert result["decisions"] == []

    assert q_cli.main(["decisions", "preflight", "--path", "src/example.py", "--digest"]) == 0
    digest = capsys.readouterr().out
    assert "Agent Mesh decision digest" in digest
    assert "No applicable decisions." in digest


def test_context_budget_is_report_only_and_machine_readable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _init_project(tmp_path, monkeypatch, capsys)
    (tmp_path / "AGENTS.md").write_text("# Instructions\n", encoding="utf-8")
    before = (tmp_path / ".agent-mesh" / "events.jsonl").read_bytes()

    assert mail_cli.main(["doctor", "--context-budget", "--json"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["schema"] == "agent-mesh.context-budget.v1"
    assert report["privacy_class"] == "project_private"
    assert report["projects"][0]["sources"][0]["path"] == "AGENTS.md"
    assert report["projects"][0]["hook_outputs"][0]["kind"] == "decision_digest_sample"
    assert (tmp_path / ".agent-mesh" / "events.jsonl").read_bytes() == before


def test_patch_upgrade_bootstraps_instances_and_persists_import_aware_adoption(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    assert (
        mail_cli.main(
            [
                "init",
                "--participants",
                "human,agent",
                "--default-sender",
                "agent",
                "--default-recipient",
                "human",
                "--no-register",
            ]
        )
        == 0
    )
    capsys.readouterr()
    (tmp_path / "CLAUDE.md").write_text("# Runtime instructions\n\n@AGENTS.md\n", encoding="utf-8")
    assert mail_cli.main(["adopt", "--repo", "."]) == 0
    capsys.readouterr()
    assert load_config(tmp_path).adoption.contract_targets == ("agents",)

    for handle in ("agent-first", "agent-second"):
        assert (
            mail_cli.main(
                [
                    "instance",
                    "register",
                    "--participant",
                    "agent",
                    "--provider",
                    "local",
                    "--handle",
                    handle,
                    "--actor",
                    "agent",
                ]
            )
            == 0
        )
        capsys.readouterr()

    assert mail_cli.main(["doctor"]) == 0
    assert "contract targets: agents (persisted)" in capsys.readouterr().out
    assert (
        mail_cli.main(
            ["request", "--from", "agent", "--to", "human", "Unbound", "must fail"]
        )
        == 2
    )
    assert "AGENT_INSTANCE_REQUIRED" in capsys.readouterr().err


def test_context_bootstrap_zero_hook_baseline_is_complete_and_truthful(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _init_project(tmp_path, monkeypatch, capsys)
    assert mail_cli.main(["adopt", "--repo", ".", "--target", "agents"]) == 0
    capsys.readouterr()

    assert q_cli.main(["context", "bootstrap"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["schema"] == "agent-mesh.context-bootstrap.v1"
    assert result["context_status"] == "complete"
    assert result["complete"] is True
    assert result["freshness"]["cursor"]["event_seq"] == 0
    assert result["managed_contract"]["body_sha256"]
    assert result["context_delivery"]["health_independent"] is True
    assert {
        item["state"]
        for item in result["context_delivery"]["report"]["capabilities"]
    } == {"unreported"}


def test_published_context_delivery_fixtures_remain_schema_only() -> None:
    fixture_root = PROJECT_ROOT / "examples" / "context-delivery"
    for name in ("claude.json", "codex-hermes.json", "generic-local.json"):
        delivery = load_lifecycle_mapping(fixture_root / name)
        assert delivery["verification"]["verification_scope"] == "schema_fixture"
        assert delivery["verification"]["capabilities"] == []
        assert all(
            item["state"] != "verified" for item in delivery["report"]["capabilities"]
        )


def test_builtin_context_mapping_is_available_without_checkout_example_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _init_project(tmp_path, monkeypatch, capsys)
    assert q_cli.main(["context", "bootstrap", "--builtin-mapping", "generic-local"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["context_delivery"]["fixture"]["runtime_family"] == "generic-local"
    assert result["context_delivery"]["verification"]["capabilities"] == []
    assert len(result["protocols"]) == 8


def test_reference_resolution_returns_canonical_title_status_and_revision(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _init_project(tmp_path, monkeypatch, capsys)
    assert (
        mail_cli.main(
            [
                "decision",
                "propose",
                "--id",
                "D010",
                "--title",
                "Resolve canonical references",
                "--tier",
                "note",
                "--decision",
                "Retrieve this exact decision text instead of a static gloss.",
            ]
        )
        == 0
    )
    capsys.readouterr()

    assert q_cli.main(["refs", "resolve", "D010", "--json"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["schema"] == "agent-mesh.reference-context.v1"
    assert result["repository"]["read_model"] == "in_memory"
    assert result["resolutions"][0]["canonical_id"] == "D010"
    assert result["resolutions"][0]["title"] == "Resolve canonical references"
    assert result["resolutions"][0]["status"] == "proposed"
    assert result["resolutions"][0]["revision_sha256"]
    assert result["resolutions"][0]["canonical_text"] == (
        "Retrieve this exact decision text instead of a static gloss."
    )
    assert result["resolutions"][0]["record_semantics"] == "decision_lifecycle"
    assert result["resolutions"][0]["authority_class"] == "proposal"

    assert (
        mail_cli.main(
            [
                "backlog",
                "create",
                "--title",
                "Correct a stale filing",
                "--summary",
                "The original premise.",
                "--status",
                "closed",
                "--disposition",
                "corrected",
                "--notes",
                "The verified outcome supersedes the premise.",
            ]
        )
        == 0
    )
    backlog_id = capsys.readouterr().out.strip().splitlines()[-1].removeprefix("created ")
    assert q_cli.main(["refs", "resolve", backlog_id, "--json"]) == 0
    backlog = json.loads(capsys.readouterr().out)["resolutions"][0]
    assert backlog["record_semantics"] == "work_item_history"
    assert backlog["authority_class"] == "non_normative"
    assert backlog["canonical_text"] == "The verified outcome supersedes the premise."
    assert backlog["canonical_text_field"] == "notes"
    assert backlog["work_item_fields"]["summary"]["text"] == "The original premise."


def test_changed_path_decision_check_is_complete_advisory_and_read_only(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    subprocess.run(["git", "init", "-q", "-b", "main"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.email", "test@example.com"], cwd=tmp_path, check=True)
    subprocess.run(["git", "config", "user.name", "Test User"], cwd=tmp_path, check=True)
    _init_project(tmp_path, monkeypatch, capsys)
    assert (
        mail_cli.main(
            [
                "decision",
                "propose",
                "--id",
                "D010",
                "--title",
                "Repository change context",
                "--tier",
                "note",
                "--scope",
                "repository",
            ]
        )
        == 0
    )
    monkeypatch.setattr(mail_cli.sys, "stdin", _InteractiveInput("ACCEPT D010\n"))
    assert (
        mail_cli.main(
            [
                "decision",
                "accept",
                "D010",
                "--by",
                "human",
                "--notes",
                "Approved for public changed-path coverage.",
            ]
        )
        == 0
    )
    source = tmp_path / "src"
    source.mkdir()
    example = source / "example.py"
    example.write_text("VALUE = 1\n", encoding="utf-8")
    subprocess.run(["git", "add", "src/example.py"], cwd=tmp_path, check=True)
    subprocess.run(["git", "commit", "-qm", "seed"], cwd=tmp_path, check=True)
    example.write_text("VALUE = 2\n", encoding="utf-8")
    capsys.readouterr()
    config = load_config(tmp_path)
    before = (config.events_path.read_bytes(), config.db_path.read_bytes())

    assert mail_cli.main(["check", "decisions", "--mode", "worktree", "--json"]) == 0
    result = json.loads(capsys.readouterr().out)

    assert result["complete"] is True
    assert result["context_status"] == "complete"
    assert result["request"]["paths"] == ["src/example.py"]
    assert result["decisions"][0]["id"] == "D010"
    assert result["decisions"][0]["matches"][0]["change_kind"] == "modified"
    assert result["decisions"][0]["evaluation_status"] == "not_run"
    assert result["decisions"][0]["would_block"] is None
    assert before == (config.events_path.read_bytes(), config.db_path.read_bytes())


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

    lifecycle = (PROJECT_ROOT / "docs/privacy-lifecycle.md").read_text(encoding="utf-8")
    normalized_lifecycle = " ".join(lifecycle.split())
    assert "It does not add a delete, history-rewrite" in normalized_lifecycle
    assert "A tombstone changes ordinary retrieval eligibility" in normalized_lifecycle
    assert "no supported emergency canonical-redaction path" in normalized_lifecycle
    assert "Agent Mesh never uploads or sends the bundle automatically" in normalized_lifecycle
    assert "Development provenance: this contract is tracked by decision `D008`" in (
        normalized_lifecycle
    )
    readme = (PROJECT_ROOT / "README.md").read_text(encoding="utf-8")
    normalized_readme = " ".join(readme.split())
    assert "Development provenance is tracked by decision `D008`" in normalized_readme
    assert "Unregistration removes machine-local Workbench information" in normalized_readme
    assert "projects unregister` CLI has no non-interactive bypass" in normalized_readme
    assert "agents must not invoke lower-level registry writers" in normalized_readme
    assert "The OS account remains the security boundary" in normalized_readme

    runtime_contract = (PROJECT_ROOT / "docs/runtime-adapter-contract.md").read_text(
        encoding="utf-8"
    )
    normalized_runtime = " ".join(runtime_contract.split())
    assert "does not depend on a runtime adapter for ordinary coordination" in normalized_runtime
    assert "Agent Mesh owns, ships, tests, documents, and supports" in normalized_runtime
    assert "Official built-in Claude, Gemini, Cursor, or other launch" in normalized_runtime
    assert "Not implemented" in normalized_runtime
    assert "project-trusted, not official provider support" in normalized_runtime
    assert "does not generalize exact continuity" in normalized_runtime

    local_driver = (PROJECT_ROOT / "docs/local-runtime-drivers.md").read_text(encoding="utf-8")
    normalized_local = " ".join(local_driver.split())
    assert "agent-mesh.runtime-driver.v1" in normalized_local
    assert "does not permit exact provider continuity" in normalized_local
    assert "cannot use or shadow a built-in ID" in normalized_local
    assert "Digest matching is drift detection" in normalized_local
    assert "not a transitive dependency lock" in normalized_local
    assert "direct `build_launch()` surface fails closed" in normalized_local
    assert "does not become an officially supported driver" in normalized_local
    assert "privacy and canonical-content trust boundary" in normalized_local
    assert "drivers check` cannot certify them" in normalized_local
    assert "local code can mutate project files" in normalized_local


def test_supervised_workbench_restart_public_contract_is_bounded_and_truthful() -> None:
    readme = " ".join((PROJECT_ROOT / "README.md").read_text(encoding="utf-8").split())
    adoption = " ".join(
        (PROJECT_ROOT / "docs" / "adoption.md").read_text(encoding="utf-8").split()
    )
    changelog = " ".join((PROJECT_ROOT / "CHANGELOG.md").read_text(encoding="utf-8").split())

    assert "one authenticated, zero-input self-retirement request" in changelog
    assert "runner exit code 75" in readme
    assert "bounded health polling act as an on-demand relaunch signal" in readme
    assert "service repair" in readme
    assert "drains already-admitted POSTs for at most ten seconds" in readme
    assert "A pre-endpoint, offline, broken, uninstalled, manual" in readme
    assert "Verified on 2026-09-01" in readme
    assert "Windows relaunch remains unverified" in readme
    assert "polls health at most 45 times over 90 seconds" in adoption
    assert "does not retry an indeterminate POST" in adoption
    assert "Verified on 2026-09-01" in adoption
    assert "Reopening the unmarked bookmark is a new direct user action" in adoption


def test_durable_dispatch_and_review_assurance_public_contract_is_published() -> None:
    dispatch = " ".join(
        (PROJECT_ROOT / "docs" / "dispatch-contract.md").read_text(encoding="utf-8").split()
    )
    assurance = " ".join(
        (PROJECT_ROOT / "docs" / "review-assurance-contract.md")
        .read_text(encoding="utf-8")
        .split()
    )
    adoption = " ".join(
        (PROJECT_ROOT / "docs" / "adoption.md").read_text(encoding="utf-8").split()
    )
    assert "A REQ remains the durable work request" in dispatch
    assert "does not claim to intercept every subagent" in dispatch
    assert "A retry creates a new attempt against the same policy" in dispatch
    assert "Help text alone is declaration evidence" in dispatch
    assert "does not copy, publish, export, or grant access" in dispatch
    assert "request prose and a digest written by the reviewer are not binding authority" in (
        assurance.lower()
    )
    assert "agent-q dispatches gate --policy dpol_... --json" in assurance
    assert "Keep the REQ as the concise contract and the RES as the concise material outcome" in (
        adoption
    )


def _untracked(root: Path) -> list[str]:
    result = subprocess.run(
        ["git", "ls-files", "--others", "--exclude-standard"],
        cwd=root,
        check=True,
        text=True,
        capture_output=True,
    )
    return result.stdout.splitlines()
