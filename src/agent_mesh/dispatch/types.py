"""Value types for the optional ``agent_mesh.dispatch`` orchestration layer.

These are plain, host-agnostic data carriers. They reference no host domain (no backlog, no
triage, no project paths). ``Message``/``RunPlan``/``RunRecord`` start minimal here in the scaffold
slice and gain fields when the dispatch engine itself is promoted; ``EvalCase``/``EvalSuite`` are
the host-owned eval contract the engine scores against (the host supplies the cases, scorer, and
suite versions -- the package ships no default cases).
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable


@dataclass(frozen=True)
class Message:
    """A normalized view of one mesh event the dispatcher reasons about. Host-agnostic; the engine
    builds these from the canonical event schema."""

    entity_id: str
    kind: str
    event_seq: int
    thread_id: str
    sender: str
    actor: str = ""
    feature: str = ""
    title: str = ""
    status: str = ""
    response_mode: str = "single"
    recipients: tuple[str, ...] = ()
    refs: tuple[str, ...] = ()


@dataclass(frozen=True)
class RunPlan:
    """A dispatch decision the engine builds for one pending message. Host-agnostic: the
    host-specific strings (``would_run``, ``post_reply_with``) are opaque carriers the engine only
    hashes for the run-record artifact; it never interprets or executes them."""

    run_id: str
    input_message_id: str
    target_agent: str
    target_event_seq: int
    thread_id: str
    session_key: str
    session_uuid: str
    classification: str
    gate: str
    session_key_source: str = ""
    wave: str = ""
    response_mode: str = "single"
    gate_reason: str = ""
    gate_reason_code: str = ""
    missing_count: int = 0
    requires_gate: tuple[str, ...] = ()
    gen_ai_system: str = ""
    model: str = ""
    would_run: str = ""
    post_reply_with: str = ""
    run_mode: str = "dry_run"
    status: str = "dry_run"
    planned_utc: str = ""
    grounding: dict = field(default_factory=dict)


@dataclass(frozen=True)
class RunRecord:
    """A privacy-safe, OTel-shaped run record the engine builds. PERSISTENCE is the host's job via
    ``RunRecorder`` (the engine never writes it) until the telemetry domain contract exists. The
    ``telemetry`` dict carries sizes/hashes only -- never raw prompt or message bodies."""

    run_id: str
    run_mode: str
    status: str
    telemetry: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class AgentRunRequest:
    """In-memory request for launching one external agent runtime.

    This is deliberately separate from canonical dispatch events: it may carry the raw prompt needed
    to invoke a local CLI, but event payloads and launch metadata must persist only hashes/ids.
    """

    run_id: str
    target_agent: str
    session_uuid: str
    project_root: Path
    prompt: str
    timeout_seconds: int = 3600


@dataclass(frozen=True)
class AgentLaunchSpec:
    """Provider-neutral launch description for a local agent process.

    ``argv``/``metadata`` are safe to record: they must not contain raw prompt or response bodies.
    The raw prompt remains in ``stdin_text`` as an in-memory handoff to the process launcher.
    """

    argv: list[str]
    cwd: Path
    requires_pty: bool
    timeout_seconds: int
    prompt_sha: str
    stdin_text: str
    metadata: dict[str, object] = field(default_factory=dict)
    stdout_file: Path | None = None


@dataclass(frozen=True)
class AgentLaunchResult:
    """Privacy-safe result from running an ``AgentLaunchSpec``.

    The result may contain agent stdout/stderr, because the launched process controls those streams.
    Machine metadata remains sanitized and must not include the raw prompt body.
    """

    status: str
    exit_code: int | None
    stdout: str = ""
    stderr: str = ""
    duration_seconds: float = 0.0
    metadata: dict[str, object] = field(default_factory=dict)

    def metadata_repr(self) -> str:
        """Stable string representation for log/event assertions without stream bodies."""

        return json.dumps(self.metadata, sort_keys=True, default=str)


@dataclass(frozen=True)
class EvalCase:
    """A host-supplied deterministic check. ``fn()`` runs under ``StoreAdapter.isolated()`` and
    returns ``(passed, detail)``. The package ships no default cases."""

    name: str
    fn: Callable[[], tuple[bool, str]]


@dataclass(frozen=True)
class EvalSuite:
    """The host-owned eval contract: the cases plus the scorer/suite versions. The engine computes
    the case-set digest and the eval fingerprint from THESE host inputs; it never invents cases or
    a scorer version of its own. An empty suite is invalid (fails closed at the engine boundary)."""

    cases: tuple[EvalCase, ...]
    scorer_version: str
    suite_version: str
