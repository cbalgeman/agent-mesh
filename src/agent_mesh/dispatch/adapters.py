"""Host adapter contracts for the optional ``agent_mesh.dispatch`` orchestration layer.

These ABCs are the ONLY way ``agent_mesh.dispatch`` reaches a host's domain: a backlog/store, a
route table, a project's files and git state. The dispatch engine itself carries zero host
knowledge; a host application implements these and injects them. Core substrate
(``agent_mesh.core``/``store``/``views``) must never import this module, and this module must never
import a host's domain code.

All contracts extend ``agent_mesh.adapters.base.Adapter`` so they share ``AdapterSpec`` (name,
domain, privacy_class, options) and ``healthcheck()``.
"""
from __future__ import annotations

import uuid
from abc import abstractmethod
from contextlib import AbstractContextManager
from pathlib import Path

from agent_mesh.adapters.base import Adapter

from .types import AgentLaunchSpec, AgentRunRequest, Message, RunRecord


class StoreAdapter(Adapter):
    """A host's mutable, projection-backed store that the transactional guard applies packets to.

    The guard supplies the generic machinery (atomic snapshot/restore, approval-artifact binding,
    delta-based invariant blocking); the host supplies all domain meaning through these methods.
    """

    @abstractmethod
    def snapshot_paths(self) -> list[Path]:
        """Every file that forms the atomic projection unit (canonical store + human projections +
        any sidecars). The guard snapshots/restores these as one unit. Returns absolute paths."""
        raise NotImplementedError

    @abstractmethod
    def normalize(self, packet: dict, *, response_ref: str = "") -> dict:
        """Validate + normalize a host packet. MUST reject exactly what ``apply`` would reject
        (validate==apply parity), failing closed with a clear error on an invalid packet."""
        raise NotImplementedError

    @abstractmethod
    def plan_transitions(self, normalized: dict) -> list[dict]:
        """Read-only resolution of which rows ``apply`` WOULD touch (explicit id and any matched/
        reopened rows), with NO mutation. Lets the guard bind every would-touch row into approval."""
        raise NotImplementedError

    @abstractmethod
    def touched_row_versions(self, plan: list[dict]) -> dict[str, str | None]:
        """Current version (e.g. ``updated_utc``) of every row the plan would touch. ``None`` means
        the row does not exist yet (a create). Bound into the approval artifact."""
        raise NotImplementedError

    @abstractmethod
    def apply(self, packet: dict, *, actor: str, response_ref: str = "") -> dict:
        """Apply the packet to the live store and return a result summary. Called by the guard
        between snapshot and the post-apply invariant check."""
        raise NotImplementedError

    @abstractmethod
    def rebuild_projections(self) -> None:
        """Rebuild human-readable projections from the store after a successful apply."""
        raise NotImplementedError

    @abstractmethod
    def check_invariants(self) -> list[str]:
        """Read-only post-apply invariants. Returns violation strings (empty == OK). MUST NOT
        mutate the store or its projections."""
        raise NotImplementedError

    @abstractmethod
    def utc_now(self) -> str:
        """An ISO-8601 UTC timestamp string (host-owned so tests can make it deterministic)."""
        raise NotImplementedError

    @abstractmethod
    def isolated(self) -> AbstractContextManager[None]:
        """Context manager that swaps the live store for a fresh, throwaway copy for the duration
        of the block, then restores ALL host state. HARD CONTRACT: it MUST restore on normal exit
        AND on exception, and MUST NOT touch the live store while active (used to run eval cases)."""
        raise NotImplementedError

    def working_copy(self) -> AbstractContextManager:
        """OPTIONAL (override to support guarded_apply(atomic_promote=True)): yield a ``promote``
        callable backed by a SAME-FILESYSTEM copy of the WHOLE candidate set (``snapshot_paths()``).
        The caller applies to the copy (the live store is never touched), checks invariants on it,
        then calls ``promote()`` to commit. ANY exit without ``promote()`` (success-without-promote,
        a raised error, or a failed invariant) discards the copy and leaves the live store exactly as
        it was.

        ``promote()`` must commit the multi-file unit so a reader never observes a MIXED unit. Since
        per-file ``os.replace`` is not atomic as a group, implement it with a roll-forward journal
        (``agent_mesh.dispatch.promote_unit``): journal the moves durably, replace the files, clear
        the journal; a crash mid-promote is completed by ``recover_unit`` (call ``recover()`` at
        startup) from the surviving validated copies. So the unit is committed-as-a-whole or, after
        recovery, completed-as-a-whole -- never left half-promoted. The default raises: an adapter
        that has not implemented this cannot use atomic_promote."""
        raise NotImplementedError("this StoreAdapter does not support atomic_promote")

    def recover(self) -> bool:
        """Roll forward an interrupted atomic promote before any read (no-op if none pending).
        Override alongside ``working_copy``; the default is a no-op."""
        return False

    def events_path(self) -> Path | None:
        """OPTIONAL: the canonical ``events.jsonl`` this store appends to, relative to its CURRENT
        root (so under ``working_copy()``/``isolated()`` it yields the COPY's log). The default
        ``None`` means the store is not canonical-event-backed (e.g. a separate projection DB), and
        the guard skips the post-apply chain check. An event-backed adapter that returns a path MUST
        also include it in ``snapshot_paths()`` and in the ``working_copy()`` unit so a chain failure
        rolls back / is discarded with the rest of the projection unit."""
        return None


class DispatchHost(Adapter):
    """The host's routing + policy for the dispatcher: who the agents are, how risk is classified,
    how a wave is resolved, and how a reply is posted."""

    @abstractmethod
    def routes(self) -> dict[str, dict[str, object]]:
        """Map of agent name -> route config (gen_ai_system, model, cache capabilities)."""
        raise NotImplementedError

    @abstractmethod
    def classify(self, message: Message) -> str:
        """``'routine'`` or ``'risky'`` from host POLICY (default-deny). The package never embeds a
        risk policy of its own."""
        raise NotImplementedError

    @abstractmethod
    def wave_for_refs(self, refs: list[str]) -> str | None:
        """Resolve a wave/session label from host references (e.g. a backlog row), or ``None``."""
        raise NotImplementedError

    @abstractmethod
    def post_reply_template(self) -> str:
        """The host's reply command template (rendered for display in dry-run; never executed by
        the package)."""
        raise NotImplementedError

    @abstractmethod
    def build_command(self, agent: str, message: Message, session_uuid: str) -> str:
        """The host-specific headless command string for a dry-run plan (e.g. the runner invocation
        for this agent). DISPLAY ONLY: the package hashes it for the run-record artifact but never
        parses or executes it."""
        raise NotImplementedError

    @abstractmethod
    def session_namespace(self) -> uuid.UUID:
        """The host's stable UUID namespace, so a wave's session id is reproducible across restarts
        and never collides with another project's."""
        raise NotImplementedError


class AgentRuntimeAdapter(Adapter):
    """Provider-neutral contract for launching an external agent process.

    Dispatch core owns run/lease state; runtime adapters only translate an in-memory run request into
    a sanitized launch spec. Raw prompts/responses must not appear in argv or metadata that may later
    be recorded as dispatch telemetry.
    """

    @abstractmethod
    def build_launch(self, request: AgentRunRequest) -> AgentLaunchSpec:
        """Return a process launch spec for ``request`` without executing it."""
        raise NotImplementedError


class RunRecorder(Adapter):
    """Where run records persist. Kept host-side until the telemetry domain contract exists, so the
    package never writes host paths or emits canonical mesh run events directly."""

    @abstractmethod
    def record(self, run_record: RunRecord) -> None:
        """Persist one privacy-safe run record (the host decides where: a local file, a sink)."""
        raise NotImplementedError


class GroundingSources(Adapter):
    """The host's content sources for cold-start grounding. The grounding engine assembles + bounds
    + privacy-tiers; the host supplies the raw text."""

    @abstractmethod
    def events(self) -> list[dict]:
        """The canonical event log as a list of event dicts."""
        raise NotImplementedError

    @abstractmethod
    def agents_md_text(self) -> str:
        """The project's agent-standards document text (e.g. AGENTS.md)."""
        raise NotImplementedError

    @abstractmethod
    def backlog_rows_text(self, refs: list[str]) -> tuple[str | None, list[str]]:
        """``(rendered_rows_or_None, unresolved_ref_ids)`` for the referenced host rows."""
        raise NotImplementedError

    @abstractmethod
    def wave_doc_text(self, feature: str) -> str | None:
        """The wave/feature design-doc acceptance text, or ``None`` if not found."""
        raise NotImplementedError


class ProjectStateProvider(Adapter):
    """Project location + version state. Parameterized so the package makes no cwd or
    package-root assumptions (used for git-SHA binding and the grounding git-state section)."""

    @abstractmethod
    def project_root(self) -> Path:
        """Absolute path to the host project root."""
        raise NotImplementedError

    @abstractmethod
    def git_sha(self) -> str:
        """Current git HEAD SHA of the host project (bound into the approval artifact)."""
        raise NotImplementedError

    @abstractmethod
    def git_state_text(self) -> str:
        """A short, deterministic git-state summary for the grounding volatile tail."""
        raise NotImplementedError
