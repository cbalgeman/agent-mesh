"""Small local workbench for agent-mesh projects."""

from __future__ import annotations

import base64
import ctypes
import difflib
import hashlib
import ipaddress
import json
import os
import re
import secrets
import shlex
import socket
import sys
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from html import escape
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable, Iterator, cast
from urllib.parse import parse_qs, quote, urlparse

from agent_mesh.adoption import contract_status as adoption_contract_status
from agent_mesh.config import AgentMeshConfig, load_config, write_agent_dir_gitignore
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
    decision_lineage_summary,
    decision_review_progress,
    decision_revision_digest,
    normalize_decision_assumptions,
    normalize_decision_evidence,
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
from agent_mesh.core.git_changes import (
    GitChangeMode,
    GitChangeRequestError,
    GitChangeUnavailable,
    collect_git_changes,
)
from agent_mesh.core.agent_instances import AgentInstanceError, resolve_authoring_actor
from agent_mesh.core.events import (
    Event,
    EventProtocolError,
    _discard_decision_append_receipt,
    _prepare_decision_append,
    append_event,
    generate_event_id,
    utc_now,
)
from agent_mesh.core.lock import acquire
from agent_mesh.core.ids import new_public_message_id
from agent_mesh.core.recovery import recover
from agent_mesh.core.workflow_origin import WORKFLOW_ORIGINS, refs_with_workflow_origin
from agent_mesh.message_packet import (
    build_message_packet,
    public_instance_handle_for_id,
    public_message_recipients,
    public_message_sender,
)
from agent_mesh.project_registry import (
    ProjectRegistryError,
    list_registered_projects,
    project_id,
    register_project,
    registry_dir,
    resolve_registered_project,
    validate_registered_project_path,
)
from agent_mesh.store.rebuild import (
    DECISION_ID_RE,
    DecisionStopLine,
    projection_is_current,
    read_event_records,
    rebuild_all,
    validate_decision_proposal_identity,
)
from agent_mesh.store.sqlite import (
    connect,
    initialize_schema,
    json_loads,
    resolve_decision,
    resolve_message,
)
from agent_mesh.dispatch.process_io import run_bounded_process
from agent_mesh.core.assurance import assurance_lifecycle_for_id, evaluate_review_assurance
from agent_mesh.store.read_model import (
    ReadModelSnapshot,
    ReadModelUnavailable,
    VerifiedReadModelCache,
    latest_read_model_match,
    open_read_model,
)
from agent_mesh.workbench_freshness import (
    WorkbenchCodeError,
    workbench_code_fingerprint as _workbench_code_fingerprint,
)


class WorkbenchError(RuntimeError):
    """Raised for local workbench request errors."""


class WorkbenchBusyError(WorkbenchError):
    """Raised when a bounded Workbench operation has no remaining capacity."""


def _normalize_authored_verification(values: list[Any] | None) -> list[dict[str, Any]]:
    try:
        return normalize_decision_verification(values, reject_unsafe=True)
    except DecisionVerificationError as exc:
        raise WorkbenchError(str(exc)) from exc


def _normalize_authored_assumptions(values: list[Any] | None) -> list[dict[str, Any]]:
    try:
        return normalize_decision_assumptions(values)
    except ValueError as exc:
        raise WorkbenchError(str(exc)) from exc


def _normalize_authored_evidence(value: Any) -> dict[str, list[str]]:
    try:
        return normalize_decision_evidence(value)
    except ValueError as exc:
        raise WorkbenchError(str(exc)) from exc


def _normalize_authored_review_policy(
    value: Any,
    config: AgentMeshConfig,
    *,
    allow_extensions: bool = False,
) -> dict[str, Any]:
    try:
        return normalize_decision_review_policy(
            value,
            participants=config.decision_approval_identities,
            allow_extensions=allow_extensions,
        )
    except ValueError as exc:
        raise WorkbenchError(str(exc)) from exc


MAX_ATTACHMENT_FILES = 20
MAX_ATTACHMENT_BYTES = 40 * 1024 * 1024
MAX_ATTACHMENT_TOTAL_BYTES = 40 * 1024 * 1024
MAX_REQUEST_BYTES = 64 * 1024 * 1024
MAX_DECISION_DIFF_BODY_INPUT_BYTES = 512 * 1024
MAX_DECISION_DIFF_BODY_LINES = 5_000
MAX_DECISION_DIFF_BODY_OUTPUT_BYTES = 96 * 1024
MAX_DECISION_DIFF_METADATA_OUTPUT_BYTES = 64 * 1024
MAX_DECISION_DIFF_VALUE_BYTES = 4 * 1024
MAX_DECISION_DIFF_COLLECTION_ITEMS = 128
MAX_DECISION_HISTORY_VERSIONS = 64
MAX_DECISION_HISTORY_FIELD_BYTES = 32 * 1024
MAX_DECISION_HISTORY_BODY_BYTES = 64 * 1024
MAX_DECISION_HISTORY_TOTAL_BODY_BYTES = 512 * 1024
MAX_DECISION_LOOKUP_SOURCE_BYTES = 64 * 1024 * 1024
MAX_DECISION_LOOKUP_EVENTS = 50_000
# The managed macOS LaunchAgent runs with ProcessType=Background. A first
# verified replay can therefore take several times longer than the same lookup
# in a foreground CLI process. Initial repository loading starts a longer
# bounded warm operation; this request budget lets an immediate click join that
# single flight without surfacing a false unavailable state.
MAX_DECISION_LOOKUP_SECONDS = 30.0
MAX_DECISION_READ_MODEL_CACHE_ENTRIES = 2
MAX_DECISION_READ_MODEL_CACHE_BYTES = 128 * 1024 * 1024
MAX_DECISION_READ_MODEL_WARM_SECONDS = 45.0
MAX_CONCURRENT_DECISION_DETAIL_REQUESTS = 1
MAX_DECISION_REVISION_REPLAY_SOURCE_BYTES = 64 * 1024 * 1024
MAX_DECISION_REVISION_REPLAY_EVENTS = 50_000
# Large canonical histories can exceed two seconds on a verified cold replay
# under a background LaunchAgent. Keep the comparison independently bounded, but give
# it enough of the enclosing 15-second decision-detail budget to complete.
MAX_DECISION_REVISION_REPLAY_SECONDS = 10.0
DECISION_REVISION_AUTHORITY_FIELDS = frozenset(
    {
        "human_id",
        "title",
        "tier",
        "applicability_scope",
        "owner",
        "context",
        "decision",
        "body_sha",
        "affected_code_globs",
        "exemptions",
        "generated_artifact_paths",
        "required_checks",
        "verification",
        "tags",
        "assumptions",
        "evidence",
        "review_policy",
    }
)
WORKBENCH_RESTART_REQUIRED = "WORKBENCH_RESTART_REQUIRED"
WORKBENCH_RESTART_ACCEPTED = "WORKBENCH_RESTART_ACCEPTED"
WORKBENCH_RESTARTING = "WORKBENCH_RESTARTING"
WORKBENCH_RESTART_DRAIN_TIMEOUT = "WORKBENCH_RESTART_DRAIN_TIMEOUT"
WORKBENCH_RESTART_NOT_REQUIRED = "WORKBENCH_RESTART_NOT_REQUIRED"
WORKBENCH_RESTART_UNVERIFIED = "WORKBENCH_RESTART_UNVERIFIED"
WORKBENCH_RESTART_REJECTED = "WORKBENCH_RESTART_REJECTED"
WORKBENCH_RESTART_PATH = "/api/service/restart"
WORKBENCH_RESTART_DRAIN_SECONDS = 10.0
WORKBENCH_MANUAL_SUPERSEDED = "WORKBENCH_MANUAL_SUPERSEDED"
WORKBENCH_MANAGED_AUTHORITY_CACHE_SECONDS = 1.0
WORKBENCH_DECISION_DETAIL_BUSY = "WORKBENCH_DECISION_DETAIL_BUSY"
WORKBENCH_OWNERSHIP_GET_ROUTES = frozenset(
    {
        "/api/projects",
        "/api/snapshot",
        "/api/status",
        "/api/feedback/receipt",
        "/api/message",
        "/api/messages",
        "/api/backlog/items",
        "/api/backlog/item",
        "/api/backlog/kanban",
        "/api/decisions/applicability",
        "/api/decisions",
        "/api/dispatches",
        "/api/dispatch",
        "/api/decision",
        "/api/decision/review",
    }
)
WORKBENCH_OWNERSHIP_POST_ROUTES = frozenset(
    {
        "/api/feedback/draft",
        "/api/feedback/submit",
        "/api/attachments/upload",
        "/api/backlog/update",
        "/api/message/status",
        "/api/dispatch/run",
        "/api/dispatch/assure",
        "/api/dispatch/assurance/flag",
        "/api/dispatch/assurance/retire",
        "/api/dispatch/cancel",
        "/api/decision/create",
        "/api/decision/update",
        "/api/decision/accept",
        "/api/decision/reject",
    }
)
WORKBENCH_SUBPROCESS_ROUTE_OPERATIONS = {
    "/api/dispatch/run": "dispatch.run",
    "/api/dispatch/assure": "dispatch.assure",
    "/api/dispatch/assurance/flag": "dispatch.assurance.flag",
    "/api/dispatch/assurance/retire": "dispatch.assurance.retire",
    "/api/dispatch/cancel": "dispatch.cancel",
}
_DECISION_APPLICABILITY_CAPACITY = threading.BoundedSemaphore(2)
_DECISION_REVISION_DIFF_CAPACITY = threading.BoundedSemaphore(2)
_DECISION_DETAIL_REQUEST_CAPACITY = threading.BoundedSemaphore(
    MAX_CONCURRENT_DECISION_DETAIL_REQUESTS
)
_DISPATCH_ACTION_CAPACITY = threading.BoundedSemaphore(1)
_DISPATCH_CANCEL_CAPACITY = threading.BoundedSemaphore(1)
_DECISION_READ_MODEL_CACHE = VerifiedReadModelCache(
    max_entries=MAX_DECISION_READ_MODEL_CACHE_ENTRIES,
    max_serialized_bytes=MAX_DECISION_READ_MODEL_CACHE_BYTES,
)
_DECISION_READ_MODEL_WARM_LOCK = threading.Lock()
_DECISION_READ_MODEL_WARMING: set[str] = set()
DONE_STATUSES = {"done", "closed", "complete", "completed", "resolved", "accepted"}
IN_PROGRESS_MARKERS = ("progress", "doing", "active", "started", "implementation")
PENDING_MARKERS = ("review", "verify", "pending", "blocked", "needs")


@contextmanager
def _decision_detail_request_slot() -> Iterator[bool]:
    acquired = _DECISION_DETAIL_REQUEST_CAPACITY.acquire(blocking=False)
    try:
        yield acquired
    finally:
        if acquired:
            _DECISION_DETAIL_REQUEST_CAPACITY.release()


def _decision_detail_busy_payload() -> dict[str, Any]:
    return {
        "ok": False,
        "code": WORKBENCH_DECISION_DETAIL_BUSY,
        "error": "Decision detail capacity is busy; wait for the active operation and retry.",
    }


def _ensure_decision_read_model_warm(repo: Path) -> bool:
    """Start one bounded verified cache fill for a repository without blocking the response."""

    key = str(repo.resolve())
    with _DECISION_READ_MODEL_WARM_LOCK:
        if key in _DECISION_READ_MODEL_WARMING:
            return False
        if len(_DECISION_READ_MODEL_WARMING) >= MAX_DECISION_READ_MODEL_CACHE_ENTRIES:
            return False
        _DECISION_READ_MODEL_WARMING.add(key)
    try:
        thread = threading.Thread(
            target=_warm_decision_read_model,
            args=(repo, key),
            name="agent-mesh-decision-read-model-warm",
            daemon=True,
        )
        thread.start()
    except RuntimeError:
        with _DECISION_READ_MODEL_WARM_LOCK:
            _DECISION_READ_MODEL_WARMING.discard(key)
        return False
    return True


def _warm_decision_read_model(repo: Path, key: str) -> None:
    try:
        config = load_config(repo)
        with _DECISION_READ_MODEL_CACHE.open(
            config,
            max_bytes=MAX_DECISION_LOOKUP_SOURCE_BYTES,
            max_events=MAX_DECISION_LOOKUP_EVENTS,
            deadline_monotonic=time.monotonic() + MAX_DECISION_READ_MODEL_WARM_SECONDS,
        ):
            pass
    except Exception:
        # This is an opportunistic read-only warm. The foreground lookup keeps
        # the bounded, user-visible failure path and exact diagnostic.
        pass
    finally:
        with _DECISION_READ_MODEL_WARM_LOCK:
            _DECISION_READ_MODEL_WARMING.discard(key)


BACKLOG_SEARCH_FIELDS = (
    "id",
    "title",
    "item_type",
    "summary",
    "status",
    "priority",
    "lane",
    "launch_scope",
    "release_phase",
    "wave",
    "owner_hint",
    "workflow_origin",
    "updated_utc",
)
FEEDBACK_REQUEST_PREDICATE_SQL = (
    "(feature_id='feedback' OR title LIKE 'Verify feedback:%' "
    "OR title='Feedback title' OR body_preview LIKE '%Feedback source:%' "
    "OR body_preview LIKE '%# Feedback%')"
)
FEEDBACK_REQUEST_PREAMBLE = (
    "Please review this feedback and update the agent-mesh backlog, "
    "decisions, or request thread as appropriate.\n\n"
)
FEEDBACK_SUBMISSION_ID_RE = re.compile(r"^fb-[A-Za-z0-9-]{8,96}$")
DECISION_TERMINAL_STATUSES = {"superseded", "retired", "rejected"}
DECISION_APPROVED_STATUSES = {"accepted", "in_force", "superseded", "retired"}


@dataclass(frozen=True)
class WorkbenchContext:
    server_url: str
    start_command: str
    bookmark_path: Path
    default_repo_id: str = ""
    access_token: str = ""
    managed_service: bool = False
    ownership_generation: str = ""
    ownership_revision: int = 0
    ownership_authority_root: Path | None = None


class WorkbenchServeOutcome(str, Enum):
    """Typed reason that the Workbench serve loop ended."""

    STOPPED = "stopped"
    RESTART_REQUESTED = "restart_requested"


@dataclass
class _RestartAttempt:
    deadline: float
    result: str | None = None


class WorkbenchRestartCoordinator:
    """Quiesce mutating handlers before a supervised process retirement."""

    def __init__(
        self,
        *,
        drain_seconds: float = WORKBENCH_RESTART_DRAIN_SECONDS,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._condition = threading.Condition()
        self._drain_seconds = drain_seconds
        self._monotonic = monotonic
        self._admission_open = True
        self._active_posts = 0
        self._attempt: _RestartAttempt | None = None
        self._restart_accepted = False
        self._serve_loop_signaled = False

    @contextmanager
    def post_lease(self) -> Iterator[bool]:
        with self._condition:
            admitted = self._admission_open and not self._restart_accepted
            if admitted:
                self._active_posts += 1
        try:
            yield admitted
        finally:
            if admitted:
                with self._condition:
                    self._active_posts -= 1
                    self._condition.notify_all()

    def request_restart(self) -> str:
        """Return the shared result for one bounded drain cycle."""
        with self._condition:
            if self._restart_accepted:
                return WORKBENCH_RESTART_ACCEPTED
            attempt = self._attempt
            if attempt is None:
                attempt = _RestartAttempt(
                    deadline=self._monotonic() + self._drain_seconds,
                )
                self._attempt = attempt
                self._admission_open = False

            while attempt.result is None:
                if not self._active_posts:
                    self._restart_accepted = True
                    attempt.result = WORKBENCH_RESTART_ACCEPTED
                    self._condition.notify_all()
                    break
                remaining = attempt.deadline - self._monotonic()
                if remaining <= 0:
                    self._admission_open = True
                    attempt.result = WORKBENCH_RESTART_DRAIN_TIMEOUT
                    if self._attempt is attempt:
                        self._attempt = None
                    self._condition.notify_all()
                    break
                self._condition.wait(remaining)
            assert attempt.result is not None
            return attempt.result

    def claim_serve_loop_signal(self) -> bool:
        """Allow exactly one flushed restart response to stop the serve loop."""
        with self._condition:
            if not self._restart_accepted or self._serve_loop_signaled:
                return False
            self._serve_loop_signaled = True
            return True

    @property
    def restart_accepted(self) -> bool:
        with self._condition:
            return self._restart_accepted

    @property
    def admission_open(self) -> bool:
        with self._condition:
            return self._admission_open

    @property
    def active_posts(self) -> int:
        with self._condition:
            return self._active_posts


def serve_workbench(
    *,
    repo: Path,
    host: str = "127.0.0.1",
    port: int = 8765,
    open_browser: bool = False,
    expected_code_fingerprint: str | None = None,
    ownership_generation: str = "",
    ownership_revision: int = 0,
    ownership_authority_root: Path | None = None,
    launchd_socket_name: str = "",
) -> WorkbenchServeOutcome:
    """Run a local HTTP workbench for registered agent-mesh repos."""
    _validate_workbench_host(host)
    config = load_config(repo)
    _require_expected_workbench_code(expected_code_fingerprint)
    url = f"http://{host}:{port}"
    managed_service = os.environ.get("AGENT_MESH_WORKBENCH_SERVICE") == "1"
    if launchd_socket_name and not managed_service:
        raise WorkbenchError("launchd socket activation requires managed Workbench mode")
    from agent_mesh.workbench_service import workbench_server_binding

    if managed_service and (not ownership_generation or ownership_revision < 1):
        raise WorkbenchError(
            "managed Workbench ownership is uninitialized; run the direct-human service "
            "install or repair action"
        )
    ownership_binding = workbench_server_binding(
        managed=managed_service,
        expected_generation=ownership_generation,
        expected_revision=ownership_revision,
        authority_root=ownership_authority_root,
    )
    if managed_service and ownership_binding is None:
        raise WorkbenchError(
            "managed Workbench ownership generation or revision is invalid or superseded; run service "
            "status, install, or repair"
        )
    default_repo_id = config.store_id
    if ownership_binding is not None:
        default_repo_id = register_project(config.project_root).id
    context = WorkbenchContext(
        server_url=url,
        start_command=(
            workbench_service_restart_command()
            if managed_service
            else workbench_start_command(config, host=host, port=port)
        ),
        bookmark_path=(
            managed_workbench_bookmark_path()
            if managed_service
            else workbench_bookmark_path(config)
        ),
        default_repo_id=default_repo_id,
        access_token=secrets.token_urlsafe(32),
        managed_service=managed_service,
        ownership_generation=(
            ownership_binding.generation if ownership_binding is not None else ownership_generation
        ),
        ownership_revision=(
            ownership_binding.ownership_revision if ownership_binding is not None else 0
        ),
        ownership_authority_root=ownership_authority_root,
    )
    restart_coordinator = WorkbenchRestartCoordinator()
    handler = _handler_for(
        config.project_root,
        context,
        _startup_code_fingerprint=expected_code_fingerprint,
        _restart_coordinator=restart_coordinator,
    )
    _require_expected_workbench_code(expected_code_fingerprint)
    server = _make_workbench_server(
        host=host,
        port=port,
        handler=handler,
        launchd_socket_name=launchd_socket_name,
    )
    try:
        _require_expected_workbench_code(expected_code_fingerprint)
        write_agent_dir_gitignore(config)
        from agent_mesh.workbench_service import managed_workbench_authority

        managed_authority = managed_workbench_authority(
            authority_root=ownership_authority_root,
            include_health=False,
        )
        if context.managed_service or managed_authority.manual_allowed:
            write_bookmark_file(config, context)
        elif managed_authority.bookmark_path != Path():
            write_managed_bookmark_pointer(config, managed_authority.bookmark_path)
    except Exception:
        server.server_close()
        raise
    launch_url = workbench_launch_url(context)
    print(f"agent-mesh workbench: {workbench_console_url(context)}")
    print(f"bookmark: file://{context.bookmark_path}")
    print(f"repo: {config.project_root}")
    if open_browser:
        import webbrowser

        webbrowser.open(launch_url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    if restart_coordinator.restart_accepted:
        return WorkbenchServeOutcome.RESTART_REQUESTED
    return WorkbenchServeOutcome.STOPPED


def _make_workbench_server(
    *,
    host: str,
    port: int,
    handler: type[BaseHTTPRequestHandler],
    launchd_socket_name: str,
) -> ThreadingHTTPServer:
    if not launchd_socket_name:
        return ThreadingHTTPServer((host, port), handler)
    inherited = _activate_launchd_socket(launchd_socket_name)
    try:
        _validate_launchd_socket(inherited, host=host, port=port)
        server = ThreadingHTTPServer((host, port), handler, bind_and_activate=False)
        server.socket.close()
        server.socket = inherited
        server.server_address = (host, port)
        server.server_name = socket.getfqdn(host)
        server.server_port = port
        return server
    except BaseException:
        inherited.close()
        raise


def _activate_launchd_socket(name: str) -> socket.socket:
    """Adopt the one fixed launchd listener rendered for the managed Workbench."""

    from agent_mesh.workbench_service import LAUNCHD_SOCKET_NAME

    if sys.platform != "darwin":
        raise WorkbenchError("launchd socket activation is available on macOS only")
    if name != LAUNCHD_SOCKET_NAME:
        raise WorkbenchError("launchd Workbench socket name is invalid")
    library = ctypes.CDLL(None, use_errno=True)
    try:
        activate = library.launch_activate_socket
    except AttributeError as exc:  # pragma: no cover - guarded by the macOS platform boundary.
        raise WorkbenchError("launchd socket activation API is unavailable") from exc
    activate.argtypes = [
        ctypes.c_char_p,
        ctypes.POINTER(ctypes.POINTER(ctypes.c_int)),
        ctypes.POINTER(ctypes.c_size_t),
    ]
    activate.restype = ctypes.c_int
    descriptors = ctypes.POINTER(ctypes.c_int)()
    count = ctypes.c_size_t()
    result = int(
        activate(
            name.encode("ascii"),
            ctypes.byref(descriptors),
            ctypes.byref(count),
        )
    )
    if result != 0:
        reason = os.strerror(result)
        raise WorkbenchError(f"launchd Workbench socket activation failed: {reason}")
    free = library.free
    free.argtypes = [ctypes.c_void_p]
    free.restype = None
    if count.value > 16 or (count.value and not descriptors):
        free(ctypes.cast(descriptors, ctypes.c_void_p))
        raise WorkbenchError("launchd Workbench socket activation returned an invalid count")
    descriptor_values = [int(descriptors[index]) for index in range(count.value)]
    free(ctypes.cast(descriptors, ctypes.c_void_p))
    if len(descriptor_values) != 1:
        for descriptor in descriptor_values:
            try:
                os.close(descriptor)
            except OSError:
                pass
        raise WorkbenchError("launchd Workbench socket activation returned an invalid count")
    descriptor = descriptor_values[0]
    try:
        return socket.socket(
            family=socket.AF_INET,
            type=socket.SOCK_STREAM,
            fileno=descriptor,
        )
    except OSError as exc:
        try:
            os.close(descriptor)
        except OSError:
            pass
        raise WorkbenchError("launchd Workbench socket could not be adopted") from exc


def _validate_launchd_socket(listener: socket.socket, *, host: str, port: int) -> None:
    # launch_activate_socket() returns listeners from the fixed, named Sockets
    # entry. Darwin returns ENOPROTOOPT for SO_ACCEPTCONN on this activated
    # descriptor, so validate the descriptor's socket type and exact endpoint
    # instead of relying on that unsupported inspection option.
    try:
        address = listener.getsockname()
    except OSError as exc:
        raise WorkbenchError(
            f"launchd Workbench socket address could not be inspected (errno {exc.errno})"
        ) from exc
    try:
        socket_type = listener.getsockopt(socket.SOL_SOCKET, socket.SO_TYPE)
    except OSError as exc:
        raise WorkbenchError(
            f"launchd Workbench socket type could not be inspected (errno {exc.errno})"
        ) from exc
    if listener.family != socket.AF_INET or socket_type != socket.SOCK_STREAM:
        raise WorkbenchError("launchd Workbench socket is not an IPv4 TCP socket")
    if address != (host, port):
        raise WorkbenchError("launchd Workbench socket does not match the configured endpoint")


def _require_expected_workbench_code(expected_code_fingerprint: str | None) -> None:
    if expected_code_fingerprint is None:
        return
    try:
        current = _workbench_code_fingerprint()
    except WorkbenchCodeError as exc:
        raise WorkbenchError(
            "Agent Mesh code could not be verified; restart Workbench before writing state"
        ) from exc
    if current != expected_code_fingerprint:
        raise WorkbenchError(
            "Agent Mesh code changed while Workbench was starting; restart Workbench"
        )


def workbench_status(
    repo: Path,
    *,
    _config: AgentMeshConfig | None = None,
) -> dict[str, Any]:
    config = _config or _load_and_rebuild(repo)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        last_seq = conn.execute("SELECT last_event_seq FROM events_seen").fetchone()[0]
        counts = {
            "open_requests": conn.execute(
                "SELECT COUNT(*) FROM messages WHERE kind='request' AND status='open'"
            ).fetchone()[0],
            "closed_requests": conn.execute(
                "SELECT COUNT(*) FROM messages WHERE kind='request' AND status='closed'"
            ).fetchone()[0],
            "responses": conn.execute(
                "SELECT COUNT(*) FROM messages WHERE kind='response'"
            ).fetchone()[0],
            "backlog_items": conn.execute("SELECT COUNT(*) FROM backlog_items").fetchone()[0],
            "decisions": conn.execute("SELECT COUNT(*) FROM decisions").fetchone()[0],
        }
        counts.update(_feedback_request_counts(conn))
        snapshot = _snapshot_metrics(conn)
        instances = [
            {
                "handle": row["label"],
                "status": row["status"],
                "provider": row["provider"],
                "runtime_profile": row["runtime_profile"] or "",
                "resumable": bool(row["resumable"]),
                "adapter_trust": row["adapter_trust"],
                "binding_source": row["last_binding_source"] or "",
                "terminal_outcome": row["terminal_outcome"] or "",
            }
            for row in conn.execute(
                "SELECT label, status, provider, runtime_profile, resumable, "
                "adapter_trust, last_binding_source, terminal_outcome "
                "FROM agent_instances ORDER BY created_event_seq, label"
            ).fetchall()
        ]
    finally:
        conn.close()
    agent_contract = adoption_contract_status(config.project_root)
    agent_contract["health_message"] = agent_contract_health_message(agent_contract)
    return {
        "ok": True,
        "project": {
            "root": str(config.project_root),
            "name": config.project_name or config.project_root.name,
            "participants": config.participants,
            "decision_approval_identities": list(config.decision_approval_identities),
            "decision_approval_diagnosis": config.decision_approval_diagnosis,
            "default_sender": config.default_sender,
            "default_recipient": config.default_recipient,
        },
        "agent_mesh": {
            "events_file": _rel(config, config.events_path),
            "events_exists": config.events_path.exists(),
            "db_file": _rel(config, config.db_path),
            "db_exists": config.db_path.exists(),
            "last_event_seq": last_seq,
        },
        "agent_contract": agent_contract,
        "instances": instances,
        "runtime_profiles": [
            {
                "name": profile.name,
                "participant": profile.target,
                "provider": profile.provider,
                "model": profile.model,
                "durable_role": profile.durable_role,
                "role": profile.role,
                "permission_mode": profile.permission_mode,
                "required_capabilities": list(profile.required_capabilities),
                "resumable": profile.resumable,
                "enabled": profile.enabled,
                "driver_source": profile.driver_source,
            }
            for profile in sorted(config.runtime_profiles.values(), key=lambda item: item.name)
        ],
        "review_assurance": {
            "enforcement": config.review_assurance.enforcement,
            "reviewer_roles": list(config.review_assurance.reviewer_roles),
            "independence": config.review_assurance.independence,
            "quorum": config.review_assurance.quorum,
            "validity_seconds": config.review_assurance.validity_seconds,
        },
        "counts": counts,
        "snapshot": snapshot,
    }


def agent_contract_health_message(contract: dict[str, Any]) -> str:
    """Return actionable Workbench remediation for contract and identity state."""
    if contract.get("healthy"):
        return (
            "Agent contract healthy - "
            f"v{contract.get('version', 'unknown')} {contract.get('digest', 'unknown')}."
        )

    files = contract.get("files") or []
    incomplete_instructions = [
        item for item in files if isinstance(item, dict) and item.get("status") != "current"
    ]
    identity = contract.get("project_identity") or {
        "complete": False,
        "error": "project identity status is unavailable",
    }
    missing = identity.get("missing") or []
    identity_complete = bool(identity.get("complete"))
    identity_needs_migration = not identity_complete and bool(identity.get("migration_kind"))
    identity_detail = (
        identity.get("error") or ", ".join(str(field) for field in missing) or "incomplete"
    )
    identity_problem = ""
    if identity_needs_migration:
        identity_problem = f"Project identity migration required - {identity_detail}."
    elif not identity_complete:
        identity_problem = f"Project identity error - {identity_detail}."

    conflicts = contract.get("conflicts") or []
    if conflicts:
        first = conflicts[0]
        message = (
            "Agent instruction conflict - "
            f"{len(conflicts)} legacy decision-write instruction(s), starting at "
            f"{first.get('path', 'unknown')}:{first.get('line', 'unknown')}."
        )
        if identity_problem:
            message += f" {identity_problem}"
        message += " Remove or revise the unmanaged conflicting instruction."
        if identity_needs_migration:
            return (
                f"{message} Run agent-mesh adopt --repo . to migrate project identity; "
                "then rerun with --check."
            )
        if not identity_complete:
            return (
                f"{message} Edit .agent-mesh/config.toml to correct project identity; "
                "then rerun agent-mesh adopt --repo . --check."
            )
        return f"{message} Then rerun agent-mesh adopt --repo . --check."

    instruction_detail = ""
    if incomplete_instructions:
        statuses = ", ".join(
            f"{item.get('path', 'unknown')} is {item.get('status', 'unknown')}"
            for item in incomplete_instructions
        )
        instruction_detail = f" Managed instructions also need refresh: {statuses}."
    if identity_needs_migration:
        return (
            f"{identity_problem}{instruction_detail} Run agent-mesh adopt --repo .; "
            "then rerun with --check."
        )
    if not identity_complete:
        return (
            f"{identity_problem}{instruction_detail} Edit .agent-mesh/config.toml to correct "
            "project identity; then rerun agent-mesh adopt --repo . --check."
        )

    drift = ", ".join(
        f"{item.get('path', 'unknown')} is {item.get('status', 'unknown')}"
        for item in incomplete_instructions
    )
    return (
        "Agent instruction drift - "
        f"{drift or 'managed instructions are missing'}. Run agent-mesh adopt --repo ."
    )


def lookup_message(repo: Path, message_id: str) -> dict[str, Any] | None:
    config = _load_and_rebuild(repo)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        row = resolve_message(conn, message_id)
        if row is None:
            return None
        packet = build_message_packet(
            conn,
            row,
            max_body_chars=50_000,
            max_thread_body_chars=8_000,
            max_thread_messages=20,
        )
    finally:
        conn.close()
    return {
        "ok": True,
        "id": message_id,
        "source": "agent-mesh",
        "file": _rel(config, config.db_path),
        "block": message_packet_block(packet),
        "packet": packet,
    }


def list_messages(
    repo: Path,
    *,
    status: str = "",
    kind: str = "",
    feature: str = "",
    workflow_origin: str = "",
    query: str = "",
    _config: AgentMeshConfig | None = None,
) -> list[dict[str, Any]]:
    config = _config or _load_and_rebuild(repo)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        sql = "SELECT * FROM messages WHERE 1=1"
        params: list[str] = []
        if status:
            sql += " AND status=?"
            params.append(status)
        if kind:
            sql += " AND kind=?"
            params.append(kind)
        if feature:
            if feature == "feedback":
                sql += f" AND {FEEDBACK_REQUEST_PREDICATE_SQL}"
            else:
                sql += " AND feature_id=?"
                params.append(feature)
        if workflow_origin:
            sql += " AND workflow_origin=?"
            params.append(workflow_origin)
        if query:
            like = f"%{query}%"
            sql += (
                " AND (id LIKE ? OR thread_id LIKE ? OR request_id LIKE ? "
                "OR sender LIKE ? OR feature_id LIKE ? OR title LIKE ? OR summary LIKE ? "
                "OR body_preview LIKE ?)"
            )
            params.extend([like, like, like, like, like, like, like, like])
        sql += " ORDER BY created_utc DESC, event_seq DESC LIMIT 200"
        rows = conn.execute(sql, params).fetchall()
        return [_message_item_from_row(conn, row) for row in rows]
    finally:
        conn.close()


def list_backlog_items(
    repo: Path,
    *,
    status: str = "",
    lane: str = "",
    priority: str = "",
    owner: str = "",
    item_type: str = "",
    launch_scope: str = "",
    wave: str = "",
    workflow_origin: str = "",
    query: str = "",
    quick_filter: str = "",
    _config: AgentMeshConfig | None = None,
) -> list[dict[str, Any]]:
    config = _config or _load_and_rebuild(repo)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        rows = conn.execute(
            """
            SELECT * FROM backlog_items
            ORDER BY priority ASC, updated_utc DESC, id ASC
            """
        ).fetchall()
        items = [_backlog_item_from_row(conn, row) for row in rows]
        return _filter_backlog_items(
            items,
            status=status,
            lane=lane,
            priority=priority,
            owner=owner,
            item_type=item_type,
            launch_scope=launch_scope,
            wave=wave,
            workflow_origin=workflow_origin,
            query=query,
            quick_filter=quick_filter,
        )
    finally:
        conn.close()


def backlog_kanban(
    repo: Path,
    *,
    _config: AgentMeshConfig | None = None,
    _items: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    items = _items if _items is not None else list_backlog_items(repo, _config=_config)
    lanes: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        lanes.setdefault(str(item["lane"] or "unassigned"), []).append(item)
    return {"ok": True, "lanes": lanes, "items": items}


def lookup_backlog_item(repo: Path, item_id: str) -> dict[str, Any] | None:
    config = _load_and_rebuild(repo)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        row = conn.execute("SELECT * FROM backlog_items WHERE id=?", (item_id.strip(),)).fetchone()
        if row is None:
            return None
        item = _backlog_detail_from_row(conn, row)
        item["events"] = [
            {
                "event_id": event["event_id"],
                "event_type": event["event_type"],
                "actor": event["actor"],
                "created_utc": event["created_utc"],
                "details": json_loads(event["details_json"], {}),
            }
            for event in conn.execute(
                "SELECT * FROM backlog_events WHERE item_id=? ORDER BY event_seq",
                (item_id.strip(),),
            )
        ]
        block = backlog_detail_block(item)
        return {"ok": True, "item": item, "block": block}
    finally:
        conn.close()


def list_decisions(
    repo: Path,
    *,
    query: str = "",
    status: str = "",
    tier: str = "",
    _config: AgentMeshConfig | None = None,
) -> list[dict[str, Any]]:
    config = _config or _load_and_rebuild(repo)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        sql = "SELECT * FROM decisions WHERE 1=1"
        params: list[str] = []
        if status:
            sql += " AND status=?"
            params.append(status)
        if tier:
            sql += " AND tier=?"
            params.append(tier)
        if query:
            like = f"%{query}%"
            sql += " AND (human_id LIKE ? OR title LIKE ? OR owner LIKE ?)"
            params.extend([like, like, like])
        sql += " ORDER BY status ASC, tier ASC, human_id ASC LIMIT 200"
        rows = conn.execute(sql, params).fetchall()
        completeness_collections = _decision_completeness_collections(
            conn,
            {str(row["dec_ulid"]) for row in rows},
        )
        return [
            _decision_item_from_row(
                row,
                completeness_collections.get(str(row["dec_ulid"])),
                superseded_by=_public_decision_id(conn, row["superseded_by"]),
            )
            for row in rows
        ]
    finally:
        conn.close()


def list_dispatches(
    repo: Path,
    *,
    _config: AgentMeshConfig | None = None,
) -> list[dict[str, Any]]:
    """Return bounded dispatch.v1 policy, current-attempt, and assurance summaries."""

    config = _config or _load_and_rebuild(repo)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        policies = conn.execute(
            "SELECT * FROM dispatch_policies ORDER BY event_seq DESC LIMIT 200"
        ).fetchall()
        items: list[dict[str, Any]] = []
        for policy in policies:
            request = resolve_message(conn, str(policy["request_id"]))
            latest = conn.execute(
                "SELECT run_id, attempt_number, status, output_message_id, terminal_code, "
                "runtime_profile, wave, session_key_source, event_seq "
                "FROM dispatch_runs WHERE policy_id=? "
                "ORDER BY attempt_number DESC, event_seq DESC LIMIT 1",
                (policy["policy_id"],),
            ).fetchone()
            assurance_count = int(
                conn.execute(
                    "SELECT COUNT(*) FROM review_assurances WHERE policy_id=? AND authoritative=1",
                    (policy["policy_id"],),
                ).fetchone()[0]
            )
            lifecycle_counts = {
                str(row["state"]): int(row["n"])
                for row in conn.execute(
                    "SELECT l.state, COUNT(*) AS n FROM review_assurances a "
                    "JOIN review_assurance_lifecycle l ON l.assurance_id=a.assurance_id "
                    "WHERE a.policy_id=? AND l.lifecycle_version=(SELECT MAX(l2.lifecycle_version) "
                    "FROM review_assurance_lifecycle l2 WHERE l2.assurance_id=a.assurance_id) "
                    "GROUP BY l.state",
                    (policy["policy_id"],),
                )
            }
            target = json_loads(policy["target_json"], {})
            subject = json_loads(policy["subject_json"], None)
            retry = json_loads(policy["retry_json"], {})
            if not isinstance(retry, dict):
                retry = {}
            maximum_attempts = int(retry.get("max_attempts") or 0)
            retry_exhausted = bool(policy["retry_exhausted_run_id"])
            items.append(
                {
                    "policy_id": str(policy["policy_id"]),
                    "request_id": str(policy["request_id"]),
                    "request_title": (
                        str(request["title"] or request["summary"] or "") if request else ""
                    ),
                    "purpose": str(policy["purpose"]),
                    "role": str(policy["role"]),
                    "participant": str(target.get("participant") or ""),
                    "runtime_profile": str(target.get("runtime_profile") or ""),
                    "workstream": (
                        str(latest["wave"] or "")
                        if latest and str(latest["session_key_source"] or "") != "thread_id"
                        else str(request["feature_id"] or "")
                        if request
                        else ""
                    ),
                    "management_level": str(policy["management_level"]),
                    "response_slot": {
                        "kind": str(policy["response_slot_kind"]),
                        "key": str(policy["response_slot_key"]),
                    },
                    "subject": subject,
                    "attempt_id": str(latest["run_id"]) if latest else "",
                    "attempt_number": int(latest["attempt_number"] or 0) if latest else 0,
                    "status": str(latest["status"]) if latest else "not_started",
                    "output_message_id": str(latest["output_message_id"] or "") if latest else "",
                    "terminal_code": str(latest["terminal_code"] or "") if latest else "",
                    "retry_exhausted": retry_exhausted,
                    "retry_exhausted_utc": str(policy["retry_exhausted_utc"] or ""),
                    "maximum_attempts": maximum_attempts,
                    "assurance_count": assurance_count,
                    "assurance_states": lifecycle_counts,
                    "frozen_utc": str(policy["frozen_utc"]),
                }
            )
        response_refs: dict[str, list[str]] = {}
        request_ids = [str(item["request_id"]) for item in items]
        if request_ids:
            placeholders = ",".join("?" for _ in request_ids)
            for ref in conn.execute(
                "SELECT message_id, ref_value FROM message_refs "
                f"WHERE ref_type='res' AND message_id IN ({placeholders}) "
                "ORDER BY message_id, ref_value",
                request_ids,
            ):
                response_refs.setdefault(str(ref["message_id"]), []).append(str(ref["ref_value"]))
        return _annotate_dispatch_continuations(items, response_refs=response_refs)
    finally:
        conn.close()


def _annotate_dispatch_continuations(
    items: list[dict[str, Any]],
    *,
    response_refs: dict[str, list[str]],
) -> list[dict[str, Any]]:
    """Attach presentation-only continuation lineage without changing canonical records."""

    by_policy = {str(item["policy_id"]): item for item in items}
    response_to_policy = {
        str(item["output_message_id"]): str(item["policy_id"])
        for item in items
        if item.get("output_message_id")
    }
    for item in items:
        candidates = [
            response_id
            for response_id in response_refs.get(str(item["request_id"]), [])
            if response_id in response_to_policy
        ]
        continuation_response_id = candidates[0] if len(candidates) == 1 else ""
        item["continuation_response_id"] = continuation_response_id
        item["parent_policy_id"] = (
            response_to_policy.get(continuation_response_id, "") if continuation_response_id else ""
        )

    for item in items:
        policy_id = str(item["policy_id"])
        current = item
        seen = {policy_id}
        depth = 0
        while current.get("parent_policy_id"):
            parent_policy_id = str(current["parent_policy_id"])
            parent = by_policy.get(parent_policy_id)
            if parent is None or parent_policy_id in seen:
                depth = 0
                current = item
                break
            seen.add(parent_policy_id)
            current = parent
            depth += 1
        item["root_policy_id"] = str(current["policy_id"])
        item["continuation_depth"] = depth

    chains: dict[str, list[dict[str, Any]]] = {}
    for item in items:
        chains.setdefault(str(item["root_policy_id"]), []).append(item)
    for chain in chains.values():
        latest_utc = max(str(item.get("frozen_utc") or "") for item in chain)
        followup_count = max(0, len(chain) - 1)
        for item in chain:
            item["chain_size"] = len(chain)
            item["chain_followup_count"] = followup_count
            item["chain_latest_utc"] = latest_utc
    return items


def lookup_dispatch(repo: Path, policy_id: str) -> dict[str, Any] | None:
    config = _load_and_rebuild(repo)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        policy = conn.execute(
            "SELECT * FROM dispatch_policies WHERE policy_id=?", (policy_id,)
        ).fetchone()
        if policy is None:
            return None
        attempts = [
            {
                "attempt_id": str(row["run_id"]),
                "attempt_number": int(row["attempt_number"] or 0),
                "status": str(row["status"]),
                "output_message_id": str(row["output_message_id"] or ""),
                "terminal_code": str(row["terminal_code"] or ""),
                "runtime_profile": str(row["runtime_profile"] or ""),
                "planned_utc": str(row["planned_utc"] or ""),
                "started_utc": str(row["started_utc"] or ""),
                "completed_utc": str(row["completed_utc"] or row["failed_utc"] or ""),
            }
            for row in conn.execute(
                "SELECT * FROM dispatch_runs WHERE policy_id=? ORDER BY attempt_number, event_seq",
                (policy_id,),
            )
        ]
        assurances = []
        for row in conn.execute(
            "SELECT * FROM review_assurances WHERE policy_id=? ORDER BY event_seq",
            (policy_id,),
        ):
            lifecycle = assurance_lifecycle_for_id(conn, str(row["assurance_id"]))
            artifact_refs = [
                {
                    "location_type": str(ref["location_type"]),
                    "location": str(ref["location"]),
                    "sha256": str(ref["sha256"]),
                    "byte_size": int(ref["byte_size"]),
                    "media_type": str(ref["media_type"]),
                    "revision": str(ref["revision"]),
                    "visibility": str(ref["visibility"]),
                    "provenance": str(ref["provenance"]),
                    "subject_digest": str(ref["subject_digest"]),
                }
                for ref in conn.execute(
                    "SELECT * FROM assurance_artifact_refs WHERE assurance_id=? ORDER BY ref_index",
                    (row["assurance_id"],),
                )
            ]
            assurances.append(
                {
                    "assurance_id": str(row["assurance_id"]),
                    "attempt_id": str(row["attempt_id"]),
                    "disposition": str(row["disposition"]),
                    "authoritative": bool(row["authoritative"]),
                    "reviewer": json_loads(row["reviewer_json"], {}),
                    "valid_until_utc": str(row["valid_until_utc"]),
                    "artifact_refs": artifact_refs,
                    "artifact_navigation": "repository_tooling",
                    "lifecycle": (
                        {
                            "response_id": lifecycle["response_id"],
                            "state": lifecycle["state"],
                            "version": lifecycle["version"],
                            "reason_code": lifecycle["reason_code"],
                            "note": lifecycle["note"],
                            "actor": lifecycle["actor"],
                            "occurred_utc": lifecycle["occurred_utc"],
                            "replacement_response_id": lifecycle["replacement_response_id"],
                        }
                        if lifecycle is not None
                        else None
                    ),
                }
            )
        gate = evaluate_review_assurance(config, conn, policy_id)
        qualifying_response_ids = (
            [
                str(row["originating_response_id"])
                for row in conn.execute(
                    "SELECT originating_response_id FROM review_assurances "
                    f"WHERE assurance_id IN ({','.join('?' for _ in gate.qualifying_assurance_ids)})",
                    gate.qualifying_assurance_ids,
                )
                if row["originating_response_id"]
            ]
            if gate.qualifying_assurance_ids
            else []
        )
        retry = json_loads(policy["retry_json"], {})
        if not isinstance(retry, dict):
            retry = {}
        return {
            "ok": True,
            "policy": {
                "policy_id": policy_id,
                "request_id": str(policy["request_id"]),
                "purpose": str(policy["purpose"]),
                "role": str(policy["role"]),
                "target": json_loads(policy["target_json"], {}),
                "response_slot": {
                    "kind": str(policy["response_slot_kind"]),
                    "key": str(policy["response_slot_key"]),
                },
                "subject": json_loads(policy["subject_json"], None),
                "assurance_policy": json_loads(policy["assurance_policy_json"], None),
                "management_level": str(policy["management_level"]),
                "retry": retry,
                "frozen_utc": str(policy["frozen_utc"]),
            },
            "attempts": attempts,
            "retry_exhausted": bool(policy["retry_exhausted_run_id"]),
            "retry_exhausted_utc": str(policy["retry_exhausted_utc"] or ""),
            "assurances": assurances,
            "gate": {
                "configured": gate.configured,
                "enforcement": gate.enforcement,
                "satisfied": gate.satisfied,
                "allows_transition": gate.allows_transition,
                "current_attempt_id": gate.current_attempt_id,
                "qualifying_responses": qualifying_response_ids,
                "reason_codes": list(gate.reason_codes),
            },
            "audit": {
                "qualifying_assurance_ids": list(gate.qualifying_assurance_ids),
            },
        }
    finally:
        conn.close()


def _workbench_subprocess_environment(ownership_envelope: str) -> dict[str, str]:
    from agent_mesh.workbench_service import WORKBENCH_INVOCATION_ENV

    environment = dict(os.environ)
    if ownership_envelope:
        environment[WORKBENCH_INVOCATION_ENV] = ownership_envelope
    return environment


def run_dispatch_from_workbench(
    repo: Path,
    payload: dict[str, Any],
    *,
    ownership_envelope: str = "",
) -> dict[str, Any]:
    """Invoke the bounded high-level CLI in a child process for one private Workbench action."""

    if not _DISPATCH_ACTION_CAPACITY.acquire(blocking=False):
        raise WorkbenchBusyError(
            "Another Workbench dispatch or assurance action is still running; retry after it finishes"
        )
    try:
        return _run_dispatch_from_workbench(
            repo,
            payload,
            ownership_envelope=ownership_envelope,
        )
    finally:
        _DISPATCH_ACTION_CAPACITY.release()


def _run_dispatch_from_workbench(
    repo: Path,
    payload: dict[str, Any],
    *,
    ownership_envelope: str = "",
) -> dict[str, Any]:
    """Run one already-admitted private Workbench dispatch action."""

    target = _clean(payload.get("target"))
    profile = _clean(payload.get("profile"))
    actor = _clean(payload.get("actor"))
    if not target or not actor:
        raise WorkbenchError("Dispatch target and acting identity are required")
    argv = [
        sys.executable,
        "-m",
        "agent_mesh.cli.q",
        "dispatches",
        "run",
        "--live",
        "--target",
        target,
        "--actor",
        actor,
    ]
    if profile:
        argv.extend(["--profile", profile])
    policy_id = _clean(payload.get("policy_id"))
    message_id = _clean(payload.get("message_id"))
    title = _clean(payload.get("title"))
    body = str(payload.get("body") or "")
    workstream = _clean(payload.get("workstream"))
    continue_response_id = _clean(payload.get("continue_response_id"))
    selected = sum(bool(value) for value in (policy_id, message_id, title))
    if selected != 1:
        if policy_id and title:
            raise WorkbenchError(
                "Prior work is selected for retry. Clear the retry selection to create a new request"
            )
        if policy_id and message_id:
            raise WorkbenchError(
                "Prior work is selected for retry. Clear the retry selection to use an existing request"
            )
        if message_id and title:
            raise WorkbenchError("Choose either an existing request or a new request, not both")
        raise WorkbenchError("Select exactly one frozen policy, existing REQ, or new REQ title")
    if workstream and not title:
        raise WorkbenchError("Workstream can be selected only when creating a new request")
    if continue_response_id and not title:
        raise WorkbenchError("A managed follow-up must create a new request")
    if continue_response_id and workstream:
        raise WorkbenchError(
            "A managed follow-up inherits its prior context; clear the separate Workstream value"
        )
    if policy_id:
        argv.extend(["--policy", policy_id])
    elif message_id:
        argv.extend(["--message", message_id])
    else:
        if not body.strip():
            raise WorkbenchError("A new REQ requires a body")
        argv.extend(["--title", title, "--body", body])
        if continue_response_id:
            argv.extend(["--continue-response", continue_response_id])
        if workstream:
            if len(workstream) > 128 or any(ord(character) < 32 for character in workstream):
                raise WorkbenchError("Workstream must be one line with at most 128 characters")
            argv.extend(["--workstream", workstream])
    if not policy_id:
        purpose = _clean(payload.get("purpose")) or "review"
        role = _clean(payload.get("role")) or "reviewer"
        argv.extend(["--purpose", purpose, "--role", role])
        maximum = payload.get("max_attempts", 1)
        if isinstance(maximum, bool) or not isinstance(maximum, int):
            raise WorkbenchError("Maximum attempts must be an integer")
        argv.extend(["--max-attempts", str(maximum)])
        subjects = [
            ("--subject-decision", _clean(payload.get("subject_decision"))),
            ("--subject-artifact", _clean(payload.get("subject_artifact"))),
            ("--subject-change-mode", _clean(payload.get("subject_change_mode"))),
        ]
        chosen = [(option, value) for option, value in subjects if value]
        if len(chosen) > 1:
            raise WorkbenchError("Select only one exact review subject")
        for option, value in chosen:
            argv.extend([option, value])
        change_base = _clean(payload.get("subject_change_base"))
        if change_base:
            argv.extend(["--subject-change-base", change_base])
    timeout_seconds = payload.get("timeout_seconds", 3600)
    if (
        isinstance(timeout_seconds, bool)
        or not isinstance(timeout_seconds, int)
        or timeout_seconds < 1
        or timeout_seconds > 21_600
    ):
        raise WorkbenchError("Dispatch timeout must be between 1 and 21600 seconds")
    argv.extend(["--timeout-seconds", str(timeout_seconds)])
    result = run_bounded_process(
        argv,
        cwd=repo,
        environment=_workbench_subprocess_environment(ownership_envelope),
        stdin=b"",
        timeout_seconds=float(timeout_seconds + 60),
        stdout_limit=64 * 1024,
        stderr_limit=64 * 1024,
    )
    stdout = result.stdout.decode("utf-8", errors="replace").strip()
    stderr = result.stderr.decode("utf-8", errors="replace").strip()
    if result.status != "completed" or result.returncode != 0:
        detail_parts = [part for part in (stderr, stdout) if part]
        detail = "\n\n".join(detail_parts) or "Dispatch did not complete successfully"
        raise WorkbenchError(detail[:4_000])
    dispatches = list_dispatches(repo)
    policy_match = re.search(r"(?:^|\s)policy=(dpol_[A-Za-z0-9]+)(?:\s|$)", stdout)
    dispatch = (
        next(
            (item for item in dispatches if item["policy_id"] == policy_match.group(1)),
            None,
        )
        if policy_match
        else None
    )
    response = (
        lookup_message(repo, str(dispatch["output_message_id"]))
        if dispatch and dispatch.get("output_message_id")
        else None
    )
    return {
        "ok": True,
        "result": stdout[:4_000],
        "dispatches": dispatches,
        "dispatch": dispatch,
        "response": response,
    }


def record_dispatch_assurance_from_workbench(
    repo: Path,
    payload: dict[str, Any],
    *,
    ownership_envelope: str = "",
) -> dict[str, Any]:
    """Record one bounded review.v1 assurance through the supported CLI surface."""

    if not _DISPATCH_ACTION_CAPACITY.acquire(blocking=False):
        raise WorkbenchBusyError(
            "Another Workbench dispatch or assurance action is still running; retry after it finishes"
        )
    try:
        return _record_dispatch_assurance_from_workbench(
            repo,
            payload,
            ownership_envelope=ownership_envelope,
        )
    finally:
        _DISPATCH_ACTION_CAPACITY.release()


def _record_dispatch_assurance_from_workbench(
    repo: Path,
    payload: dict[str, Any],
    *,
    ownership_envelope: str = "",
) -> dict[str, Any]:
    """Run one already-admitted private Workbench assurance action."""

    operator_verdict_fields = {
        "disposition",
        "reviewer_role",
        "fatal",
        "material",
        "minor",
        "artifacts",
    }
    supplied = sorted(operator_verdict_fields.intersection(payload))
    if supplied:
        raise WorkbenchError(
            "Review verdict, findings, and artifacts are derived from the exact current RES; "
            "remove operator-authored assurance fields: " + ", ".join(supplied)
        )
    policy_id = _clean(payload.get("policy_id"))
    actor = _clean(payload.get("actor"))
    if not policy_id or not actor:
        raise WorkbenchError("Dispatch policy and acting identity are required")
    argv = [
        sys.executable,
        "-m",
        "agent_mesh.cli.q",
        "dispatches",
        "assure",
        "--policy",
        policy_id,
        "--actor",
        actor,
    ]
    result = run_bounded_process(
        argv,
        cwd=repo,
        environment=_workbench_subprocess_environment(ownership_envelope),
        stdin=b"",
        timeout_seconds=60.0,
        stdout_limit=64 * 1024,
        stderr_limit=64 * 1024,
    )
    stdout = result.stdout.decode("utf-8", errors="replace").strip()
    stderr = result.stderr.decode("utf-8", errors="replace").strip()
    if result.status != "completed" or result.returncode != 0:
        detail = stderr or stdout or "Review assurance was not recorded"
        raise WorkbenchError(detail[:4_000])
    dispatch_detail = lookup_dispatch(repo, policy_id)
    return {
        "ok": True,
        "result": stdout[:4_000],
        "dispatch": dispatch_detail,
        "dispatches": list_dispatches(repo),
    }


def transition_dispatch_assurance_from_workbench(
    repo: Path,
    payload: dict[str, Any],
    *,
    action: str,
    ownership_envelope: str = "",
) -> dict[str, Any]:
    """Flag or direct-human-retire one assurance through its public RES reference."""

    if action not in {"flag", "retire"}:
        raise WorkbenchError("Review assurance lifecycle action is invalid")
    if not _DISPATCH_ACTION_CAPACITY.acquire(blocking=False):
        raise WorkbenchBusyError(
            "Another Workbench dispatch or assurance action is still running; retry after it finishes"
        )
    try:
        response_id = _clean(payload.get("response_id"))
        policy_id = _clean(payload.get("policy_id"))
        actor = _clean(payload.get("actor"))
        reason_code = _clean(payload.get("reason_code"))
        note = _clean(payload.get("note"))
        expected_version = payload.get("expected_version")
        if not response_id or not policy_id or not actor or not reason_code or not note:
            raise WorkbenchError(
                "Response, policy, acting identity, reason, and explanatory note are required"
            )
        if (
            isinstance(expected_version, bool)
            or not isinstance(expected_version, int)
            or expected_version < 1
        ):
            raise WorkbenchError("Review assurance lifecycle version is invalid")
        command = "flag-assurance" if action == "flag" else "retire-assurance"
        argv = [
            sys.executable,
            "-m",
            "agent_mesh.cli.q",
            "dispatches",
            command,
            "--response",
            response_id,
            "--expected-version",
            str(expected_version),
            "--reason-code",
            reason_code,
            "--note",
            note,
            "--actor",
            actor,
        ]
        result = run_bounded_process(
            argv,
            cwd=repo,
            environment=_workbench_subprocess_environment(ownership_envelope),
            stdin=b"",
            timeout_seconds=60.0,
            stdout_limit=64 * 1024,
            stderr_limit=64 * 1024,
        )
        stdout = result.stdout.decode("utf-8", errors="replace").strip()
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        if result.status != "completed" or result.returncode != 0:
            detail = stderr or stdout or "Review assurance state was not updated"
            raise WorkbenchError(detail[:4_000])
        return {
            "ok": True,
            "result": stdout[:4_000],
            "dispatch": lookup_dispatch(repo, policy_id),
            "dispatches": list_dispatches(repo),
        }
    finally:
        _DISPATCH_ACTION_CAPACITY.release()


def _validated_dispatch_cancel_request(payload: dict[str, Any]) -> tuple[str, str]:
    run_id = _clean(payload.get("run_id"))
    actor = _clean(payload.get("actor"))
    if not run_id or not actor:
        raise WorkbenchError("Active dispatch run and acting identity are required")
    return run_id, actor


def cancel_dispatch_from_workbench(
    repo: Path,
    payload: dict[str, Any],
    *,
    ownership_envelope: str = "",
) -> dict[str, Any]:
    """Terminalize one active managed dispatch through the supported CLI surface."""

    if not _DISPATCH_CANCEL_CAPACITY.acquire(blocking=False):
        raise WorkbenchBusyError(
            "Another Workbench cancellation is still running; retry after it finishes"
        )
    try:
        run_id, actor = _validated_dispatch_cancel_request(payload)
        argv = [
            sys.executable,
            "-m",
            "agent_mesh.cli.q",
            "dispatches",
            "cancel",
            "--run",
            run_id,
            "--actor",
            actor,
        ]
        result = run_bounded_process(
            argv,
            cwd=repo,
            environment=_workbench_subprocess_environment(ownership_envelope),
            stdin=b"",
            timeout_seconds=60.0,
            stdout_limit=64 * 1024,
            stderr_limit=64 * 1024,
        )
        stdout = result.stdout.decode("utf-8", errors="replace").strip()
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        if result.status != "completed" or result.returncode != 0:
            detail = stderr or stdout or "Dispatch was not cancelled"
            raise WorkbenchError(detail[:4_000])
        return {
            "ok": True,
            "result": stdout[:4_000],
            "dispatches": list_dispatches(repo),
        }
    finally:
        _DISPATCH_CANCEL_CAPACITY.release()


def decision_applicability_for_changes(
    repo: Path,
    *,
    mode: GitChangeMode = "worktree",
    base: str | None = None,
) -> dict[str, Any]:
    """Return the same advisory Git-diff applicability result exposed by the CLI."""

    if not _DECISION_APPLICABILITY_CAPACITY.acquire(blocking=False):
        raise GitChangeUnavailable(
            "Workbench changed-path evaluation capacity is busy; retry after an active check finishes"
        )
    try:
        config = load_config(repo)
        change_set = collect_git_changes(config.project_root, mode=mode, base=base)
        with open_read_model(config) as snapshot:
            context = build_decision_context(
                config,
                snapshot,
                change_set.paths,
                boundary="change_review",
                change_set=change_set,
            )
        rendered, complete = render_bounded_decision_context_json(context)
        bounded_context = json.loads(rendered)
        if not isinstance(bounded_context, dict):  # pragma: no cover - renderer invariant
            raise WorkbenchError("decision applicability renderer returned an invalid envelope")
        result: dict[str, Any] = {"ok": complete, "context": bounded_context}
        if not complete:
            diagnostics = bounded_context.get("diagnostics")
            if isinstance(diagnostics, list) and diagnostics:
                result["error"] = str(diagnostics[0])
        return result
    finally:
        _DECISION_APPLICABILITY_CAPACITY.release()


def workbench_snapshot(
    repo: Path,
    *,
    message_status: str = "",
    message_kind: str = "",
    message_feature: str = "",
    message_workflow_origin: str = "",
    message_query: str = "",
    backlog_status: str = "",
    backlog_lane: str = "",
    backlog_priority: str = "",
    backlog_owner: str = "",
    backlog_item_type: str = "",
    backlog_launch_scope: str = "",
    backlog_wave: str = "",
    backlog_workflow_origin: str = "",
    backlog_query: str = "",
    backlog_quick_filter: str = "",
    decision_query: str = "",
    decision_status: str = "",
    decision_tier: str = "",
) -> dict[str, Any]:
    """Return the repo-switch payload from one projection freshness check."""
    config = load_config(repo)
    lock_handle = acquire(config.agent_dir / ".mail-lock")
    try:
        _rebuild_if_stale(config)
        all_backlog_items = list_backlog_items(config.project_root, _config=config)
        filtered_backlog_items = _filter_backlog_items(
            all_backlog_items,
            status=backlog_status,
            lane=backlog_lane,
            priority=backlog_priority,
            owner=backlog_owner,
            item_type=backlog_item_type,
            launch_scope=backlog_launch_scope,
            wave=backlog_wave,
            workflow_origin=backlog_workflow_origin,
            query=backlog_query,
            quick_filter=backlog_quick_filter,
        )
        return {
            "ok": True,
            "status": workbench_status(config.project_root, _config=config),
            "messages": list_messages(
                config.project_root,
                status=message_status,
                kind=message_kind,
                feature=message_feature,
                workflow_origin=message_workflow_origin,
                query=message_query,
                _config=config,
            ),
            "backlog_items": filtered_backlog_items,
            "decisions": list_decisions(
                config.project_root,
                query=decision_query,
                status=decision_status,
                tier=decision_tier,
                _config=config,
            ),
            "dispatches": list_dispatches(config.project_root, _config=config),
            "kanban": backlog_kanban(
                config.project_root,
                _config=config,
                _items=all_backlog_items,
            ),
        }
    finally:
        lock_handle.release()


def lookup_decision(
    repo: Path,
    identifier: str,
    *,
    include_review_data: bool = True,
) -> dict[str, Any] | None:
    config = load_config(repo)
    lookup_deadline = time.monotonic() + MAX_DECISION_LOOKUP_SECONDS
    with _DECISION_READ_MODEL_CACHE.open(
        config,
        max_bytes=MAX_DECISION_LOOKUP_SOURCE_BYTES,
        max_events=MAX_DECISION_LOOKUP_EVENTS,
        deadline_monotonic=lookup_deadline,
    ) as snapshot:
        conn = snapshot.conn
        dec_ulid = resolve_decision(conn, identifier)
        if dec_ulid is None:
            return None
        row = conn.execute("SELECT * FROM decisions WHERE dec_ulid=?", (dec_ulid,)).fetchone()
        if row is None:
            return None
        meta = json_loads(row["meta_json"], {})
        if not isinstance(meta, dict):
            meta = {}
        body = _decision_body_from_row(config, row)
        collections = _decision_collections(conn, dec_ulid)
        revision_sha = _decision_revision_sha(row, meta, collections)
        review_policy = normalize_decision_review_policy(
            meta.get("review_policy", {}), allow_extensions=True
        )
        review_progress = decision_review_progress(meta, revision_sha)
        superseded_by = _public_decision_id(conn, row["superseded_by"])
        completeness_issues = decision_completeness_issues(
            tier=str(row["tier"]),
            owner=str(row["owner"] or ""),
            affected_code_globs=collections["affected_code_globs"],
            verification=collections["verification"],
            applicability_scope=str(row["applicability_scope"]),
            exemptions=collections["exemptions"],
            generated_artifact_paths=collections["generated_artifact_paths"],
        )
        approval_blockers: list[str] = []
        if not str(row["body_path"] or "") and int(row["body_bytes"] or 0) == 0:
            approval_blockers.append(
                "canonical body object is missing; enter body content and save the revision "
                "before approval"
            )
        block = decision_detail_block(
            row,
            meta,
            body,
            collections=collections,
            completeness_issues=completeness_issues,
            superseded_by=superseded_by,
        )
        proposed_utc = _decision_proposed_utc(row, meta)
        public_meta = _public_decision_value(conn, meta)
        status = str(row["status"])
        revision_count = _decision_revision_count(meta)
        approved_version_count = _decision_approved_version_count(
            meta,
            current_status=status,
        )
        if include_review_data:
            revision_history = _decision_revision_history(
                config,
                snapshot,
                dec_ulid=dec_ulid,
                current_revision_sha=revision_sha,
                current_status=status,
                deadline_monotonic=lookup_deadline,
            )
            revision_diff = _pending_decision_revision_diff(
                config,
                snapshot,
                row=row,
                meta=meta,
                body=body,
                collections=collections,
                revision_sha=revision_sha,
                deadline_monotonic=min(
                    lookup_deadline,
                    time.monotonic() + MAX_DECISION_REVISION_REPLAY_SECONDS,
                ),
            )
            revision_count = max(
                revision_count,
                int(revision_history.get("total_revisions") or 1),
            )
            approved_version_count = max(
                approved_version_count,
                int(revision_history.get("approved_versions") or 0),
            )
        else:
            revision_history = {
                "available": False,
                "loading": True,
                "total_revisions": revision_count,
                "approved_versions": approved_version_count,
                "revisions": [],
            }
            revision_diff = (
                {
                    "pending": True,
                    "available": False,
                    "loading": True,
                    "pending_revision_sha": revision_sha,
                    "pending_body_sha": str(row["body_sha"] or ""),
                }
                if status == "proposed"
                else {"pending": False, "available": True}
            )
        return {
            "ok": True,
            "decision": {
                "id": row["human_id"],
                "title": row["title"],
                "tier": row["tier"],
                "tier_valid": bool(row["tier_valid"]),
                "status": row["status"],
                "applicability_scope": row["applicability_scope"],
                "owner": row["owner"] or "",
                "drift_risk": row["drift_risk"] or "",
                "display_utc": _decision_display_utc(row, meta),
                "proposed_utc": proposed_utc,
                "accepted_utc": row["accepted_utc"],
                "in_force_utc": row["in_force_utc"],
                "retired_utc": row["retired_utc"],
                "last_verified_utc": row["last_verified_utc"],
                "superseded_by": superseded_by,
                "status_utc": _decision_status_utc(row, meta if isinstance(meta, dict) else {}),
                "meta": public_meta,
                "body_sha": row["body_sha"] or "",
                "revision_sha": revision_sha,
                "revision": revision_count,
                "revision_count": revision_count,
                "approved_version": approved_version_count or None,
                "approved_version_count": approved_version_count,
                "version": approved_version_count,
                "version_count": approved_version_count,
                "approval_revision_sha": review_progress["approval_revision_sha"],
                "approval_binding": review_progress["approval_binding"],
                "review_policy": review_policy,
                "review_progress": review_progress,
                "body_path": row["body_path"] or "",
                "body_bytes": row["body_bytes"],
                "body": body,
                **collections,
                "complete_for_acceptance": not completeness_issues,
                "completeness_issues": list(completeness_issues),
                "approval_blockers": approval_blockers,
                "revision_diff": revision_diff,
                "revision_history": revision_history,
                "review_data_required": not include_review_data
                and (status == "proposed" or revision_count > 1),
                "block": block,
            },
            "block": block,
        }


def lookup_decision_review(
    repo: Path,
    identifier: str,
    *,
    expected_revision_sha: str,
) -> dict[str, Any] | None:
    """Return expensive review data separately from the current decision card."""

    result = lookup_decision(repo, identifier)
    if result is None:
        return None
    decision = result["decision"]
    current_revision_sha = str(decision.get("revision_sha") or "")
    if expected_revision_sha and not secrets.compare_digest(
        expected_revision_sha,
        current_revision_sha,
    ):
        return {
            "ok": True,
            "stale": True,
            "decision_id": str(decision.get("id") or identifier),
            "requested_revision_sha": expected_revision_sha,
            "current_revision_sha": current_revision_sha,
        }
    return {
        "ok": True,
        "stale": False,
        "decision_id": str(decision.get("id") or identifier),
        "revision_sha": current_revision_sha,
        "revision": int(decision.get("revision") or 1),
        "revision_count": int(decision.get("revision_count") or 1),
        "approved_version": decision.get("approved_version"),
        "approved_version_count": int(decision.get("approved_version_count") or 0),
        "revision_history": decision.get("revision_history") or {},
        "revision_diff": decision.get("revision_diff") or {},
    }


def _decision_mutation_result(
    config: AgentMeshConfig,
    identifier: str,
    *,
    operation: str,
    event_seq: int,
    committed: bool,
    flags: dict[str, Any],
) -> dict[str, Any]:
    try:
        detail = lookup_decision(config.project_root, identifier)
    except Exception:
        detail = None
    if detail is not None:
        return {
            **detail,
            "operation": operation,
            "event_seq": event_seq,
            "committed": committed,
            "detail_available": True,
            **flags,
        }
    outcome = "committed" if committed else "already reflected in canonical state"
    return {
        "ok": True,
        "operation": operation,
        "event_seq": event_seq,
        "committed": committed,
        "detail_available": False,
        "decision": {"id": identifier},
        "diagnostic": (
            f"{operation.replace('_', ' ').capitalize()} {outcome} at event {event_seq}; "
            "detail refresh is temporarily unavailable. Reload Decisions before editing or "
            "approving again."
        ),
        **flags,
    }


def next_decision_human_id(repo: Path) -> str:
    config = _load_and_rebuild(repo)
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        return _next_decision_human_id(conn)
    finally:
        conn.close()


def create_decision(
    repo: Path,
    *,
    title: str,
    tier: str,
    owner: str = "",
    context: str = "",
    decision: str = "",
    body: str | None = None,
    applicability_scope: str | None = None,
    affected_code_globs: list[str] | None = None,
    exemptions: list[str] | None = None,
    generated_artifact_paths: list[str] | None = None,
    required_checks: list[str] | None = None,
    verification: list[Any] | None = None,
    assumptions: list[Any] | None = None,
    evidence: Any = None,
    review_policy: dict[str, Any] | None = None,
    tags: list[str] | None = None,
    human_id: str = "",
    actor: str | None = None,
) -> dict[str, Any]:
    config = load_config(repo)
    title = title.strip()
    tier = tier.strip()
    owner = owner.strip()
    context = context.strip()
    decision = decision.strip()
    affected_code_globs = normalize_decision_strings(affected_code_globs)
    exemptions = normalize_decision_strings(exemptions)
    generated_artifact_paths = normalize_decision_strings(generated_artifact_paths)
    applicability_scope = applicability_scope_for(applicability_scope, affected_code_globs)
    required_checks = normalize_decision_strings(required_checks)
    verification = _normalize_authored_verification(verification)
    assumptions = _normalize_authored_assumptions(assumptions)
    evidence = _normalize_authored_evidence(evidence)
    review_policy = _normalize_authored_review_policy(review_policy, config)
    tags = normalize_decision_strings(tags)
    actor = _workbench_authoring_actor(config, actor)
    requested_id = human_id.strip().upper()
    if not title:
        raise WorkbenchError("Decision title is required")
    if not tier:
        raise WorkbenchError("Decision tier is required")
    if tier not in DECISION_TIERS:
        raise WorkbenchError(f"Decision tier must be one of: {', '.join(DECISION_TIERS)}")
    if requested_id and not DECISION_ID_RE.fullmatch(requested_id):
        raise WorkbenchError("Decision ID must look like D001, D038-S1, or D076-E")
    applicability_issues = decision_applicability_issues(
        applicability_scope=applicability_scope,
        tier=tier,
        affected_code_globs=affected_code_globs,
        exemptions=exemptions,
        generated_artifact_paths=generated_artifact_paths,
    )
    if applicability_issues:
        raise WorkbenchError("; ".join(applicability_issues))

    lock_handle = acquire(config.agent_dir / ".mail-lock")
    last_event_seq = None
    decision_receipt = None
    uncommitted_body_path = None
    try:
        decision_receipt = _prepare_decision_append(config, lock_handle)
        conn = connect(config.db_path)
        try:
            initialize_schema(conn)
            resolved_id = requested_id or _next_decision_human_id(conn)
            dec_ulid = "dec_" + generate_event_id()[3:]
            try:
                validate_decision_proposal_identity(
                    conn,
                    resolved_id,
                    dec_ulid=dec_ulid,
                )
            except DecisionStopLine:
                raise WorkbenchError(f"decision already exists: {resolved_id}") from None
        finally:
            conn.close()

        canonical_body = body if body is not None else ""
        body_format = DECISION_BODY_FORMAT_CUSTOM
        if not canonical_body.strip():
            canonical_body = _default_decision_body(
                resolved_id,
                title=title,
                context=context,
                decision=decision,
            )
            body_format = DECISION_BODY_FORMAT_GENERATED_V1
        body_path, body_sha, body_bytes, body_created = _write_decision_body(config, canonical_body)
        if body_created:
            uncommitted_body_path = config.agent_dir / body_path
        event = Event(
            event_id=generate_event_id(),
            actor=actor,
            kind="decision_proposed",
            entity_id=dec_ulid,
            thread_id=dec_ulid,
            payload={
                "decision_contract_version": 5,
                "human_id": resolved_id,
                "aliases": [],
                "title": title,
                "tier": tier,
                "applicability_scope": applicability_scope,
                "context": context,
                "decision": decision,
                "rejected_alternatives": [],
                "consequences": [],
                "affected_code_globs": affected_code_globs,
                "exemptions": exemptions,
                "generated_artifact_paths": generated_artifact_paths,
                "assumptions": assumptions,
                "evidence": evidence,
                "supersedes": None,
                "owner": owner or None,
                "review_policy": review_policy,
                "required_checks": required_checks,
                "verification": verification,
                "tags": tags,
                "body_sha": body_sha,
                "body_path": body_path,
                "body_bytes": body_bytes,
                "body_format": body_format,
            },
        )
        result = append_event(
            config.events_path,
            event,
            lock_acquired=True,
            _decision_receipt=decision_receipt,
            _lock_handle=lock_handle,
        )
        last_event_seq = result.event.event_seq
        uncommitted_body_path = None
    except EventProtocolError as exc:
        if str(exc).startswith("prepared decision projection changed"):
            _discard_uncommitted_decision_body(uncommitted_body_path)
            uncommitted_body_path = None
        raise
    finally:
        _discard_decision_append_receipt(decision_receipt)
        lock_handle.release(last_event_seq=last_event_seq)

    assert last_event_seq is not None
    return _decision_mutation_result(
        config,
        resolved_id,
        operation="decision_created",
        event_seq=last_event_seq,
        committed=True,
        flags={"created": True},
    )


def update_decision(
    repo: Path,
    identifier: str,
    *,
    title: str | None = None,
    tier: str | None = None,
    owner: str | None = None,
    context: str | None = None,
    decision: str | None = None,
    body: str | None = None,
    applicability_scope: str | None = None,
    affected_code_globs: list[str] | None = None,
    exemptions: list[str] | None = None,
    generated_artifact_paths: list[str] | None = None,
    required_checks: list[str] | None = None,
    verification: list[Any] | None = None,
    assumptions: list[Any] | None = None,
    evidence: Any = None,
    review_policy: dict[str, Any] | None = None,
    tags: list[str] | None = None,
    revision_reason: str = "",
    actor: str | None = None,
) -> dict[str, Any]:
    config = load_config(repo)
    identifier = identifier.strip()
    actor = _workbench_authoring_actor(config, actor)
    revision_reason = revision_reason.strip()
    if not identifier:
        raise WorkbenchError("Decision ID is required")

    lock_handle = acquire(config.agent_dir / ".mail-lock")
    last_event_seq = None
    did_update = False
    decision_receipt = None
    uncommitted_body_path = None
    try:
        decision_receipt = _prepare_decision_append(config, lock_handle)
        conn = connect(config.db_path)
        try:
            initialize_schema(conn)
            dec_ulid = resolve_decision(conn, identifier)
            if dec_ulid is None:
                raise WorkbenchError(f"decision not found: {identifier}")
            row = conn.execute("SELECT * FROM decisions WHERE dec_ulid=?", (dec_ulid,)).fetchone()
            if row is None:
                raise WorkbenchError(f"decision not found: {identifier}")
            meta = json_loads(row["meta_json"], {})
            if not isinstance(meta, dict):
                meta = {}
            current_body = _decision_body_from_row(config, row)
            current_collections = _decision_collections(conn, dec_ulid)
            status = str(row["status"])
        finally:
            conn.close()

        if status in DECISION_TERMINAL_STATUSES:
            raise WorkbenchError(
                f"{identifier} is {status}; create a successor decision instead of editing it"
            )

        fields_changed: dict[str, list[Any]] = {}
        requested_values = {
            "title": title,
            "tier": tier,
            "owner": owner,
            "context": context,
            "decision": decision,
        }
        current_values = {
            "title": str(row["title"] or ""),
            "tier": str(row["tier"] or ""),
            "owner": str(row["owner"] or ""),
            "context": str(meta.get("context") or ""),
            "decision": str(meta.get("decision") or ""),
        }
        for field_name, requested in requested_values.items():
            if requested is None:
                continue
            normalized = requested.strip()
            if field_name in {"title", "tier"} and not normalized:
                raise WorkbenchError(f"Decision {field_name} is required")
            if field_name == "tier" and normalized not in DECISION_TIERS:
                raise WorkbenchError(f"Decision tier must be one of: {', '.join(DECISION_TIERS)}")
            if normalized != current_values[field_name]:
                fields_changed[field_name] = [current_values[field_name], normalized]

        if applicability_scope is not None:
            normalized_scope = applicability_scope.strip()
            if normalized_scope not in DECISION_APPLICABILITY_SCOPES:
                raise WorkbenchError(
                    "Decision applicability scope must be one of: "
                    + ", ".join(DECISION_APPLICABILITY_SCOPES)
                )
            if normalized_scope != str(row["applicability_scope"]):
                fields_changed["applicability_scope"] = [
                    str(row["applicability_scope"]),
                    normalized_scope,
                ]

        normalized_collection_requests: dict[str, Any] = {
            "affected_code_globs": cast(list[Any], normalize_decision_strings(affected_code_globs))
            if affected_code_globs is not None
            else None,
            "exemptions": cast(list[Any], normalize_decision_strings(exemptions))
            if exemptions is not None
            else None,
            "generated_artifact_paths": cast(
                list[Any], normalize_decision_strings(generated_artifact_paths)
            )
            if generated_artifact_paths is not None
            else None,
            "required_checks": cast(list[Any], normalize_decision_strings(required_checks))
            if required_checks is not None
            else None,
            "verification": _normalize_authored_verification(verification)
            if verification is not None
            else None,
            "assumptions": _normalize_authored_assumptions(assumptions)
            if assumptions is not None
            else None,
            "evidence": _normalize_authored_evidence(evidence) if evidence is not None else None,
            "tags": cast(list[Any], normalize_decision_strings(tags)) if tags is not None else None,
        }
        for field_name, normalized_values in normalized_collection_requests.items():
            if normalized_values is None:
                continue
            if (
                field_name == "verification"
                and verification is not None
                and all(not isinstance(item, dict) for item in verification)
            ):
                current_commands = [
                    str(item.get("command") or "")
                    for item in current_collections[field_name]
                    if isinstance(item, dict)
                ]
                requested_commands = [
                    str(item.get("command") or "")
                    for item in normalized_values
                    if isinstance(item, dict)
                ]
                if requested_commands == current_commands:
                    continue
            if field_name == "verification" and _verification_definitions(
                normalized_values
            ) == _verification_definitions(current_collections[field_name]):
                continue
            current_value = current_collections[field_name]
            if field_name == "assumptions":
                current_value = normalize_decision_assumptions(current_value)
            if normalized_values != current_value:
                fields_changed[field_name] = [
                    current_value,
                    normalized_values,
                ]

        if review_policy is not None:
            normalized_review_policy = _normalize_authored_review_policy(review_policy, config)
            current_review_policy = normalize_decision_review_policy(
                meta.get("review_policy", {}), allow_extensions=True
            )
            if normalized_review_policy != current_review_policy:
                fields_changed["review_policy"] = [
                    current_review_policy,
                    normalized_review_policy,
                ]

        prospective_scope = str(
            fields_changed.get("applicability_scope", [None, str(row["applicability_scope"])])[1]
        )
        prospective_tier = str(fields_changed.get("tier", [None, str(row["tier"])])[1])
        prospective_globs = fields_changed.get(
            "affected_code_globs", [None, current_collections["affected_code_globs"]]
        )[1]
        applicability_issues = decision_applicability_issues(
            applicability_scope=prospective_scope,
            tier=prospective_tier,
            affected_code_globs=prospective_globs,
            exemptions=fields_changed.get("exemptions", [None, current_collections["exemptions"]])[
                1
            ],
            generated_artifact_paths=fields_changed.get(
                "generated_artifact_paths",
                [None, current_collections["generated_artifact_paths"]],
            )[1],
        )
        if applicability_issues:
            raise WorkbenchError("; ".join(applicability_issues))

        canonical_fields_changed = any(
            field_name in fields_changed for field_name in ("title", "context", "decision")
        )
        effective_body = body
        current_body_format = str(meta.get("body_format") or DECISION_BODY_FORMAT_UNKNOWN)
        if canonical_fields_changed and (body is None or body == current_body):
            body_missing = not str(row["body_path"] or "") and int(row["body_bytes"] or 0) == 0
            if current_body_format != DECISION_BODY_FORMAT_GENERATED_V1 and not body_missing:
                raise WorkbenchError(
                    "This decision has custom canonical Markdown. Update the Canonical Markdown "
                    "field together with the title, context, or decision text."
                )
            effective_body = generated_decision_body(
                str(row["human_id"]),
                title=str(fields_changed.get("title", [None, current_values["title"]])[1]),
                context=str(fields_changed.get("context", [None, current_values["context"]])[1]),
                decision=str(fields_changed.get("decision", [None, current_values["decision"]])[1]),
            )
        body_changed = effective_body is not None and effective_body != current_body
        if (
            status in {"accepted", "in_force"}
            and (fields_changed or body_changed)
            and not revision_reason
        ):
            raise WorkbenchError("Revision reason is required when editing an accepted decision")

        if body_changed:
            assert effective_body is not None
            next_body_format = (
                DECISION_BODY_FORMAT_CUSTOM
                if body is not None and body != current_body
                else DECISION_BODY_FORMAT_GENERATED_V1
            )
            body_path, body_sha, body_bytes, body_created = _write_decision_body(
                config, effective_body
            )
            if body_created:
                uncommitted_body_path = config.agent_dir / body_path
            fields_changed.update(
                {
                    "body_path": [str(row["body_path"] or ""), body_path],
                    "body_sha": [str(row["body_sha"] or ""), body_sha],
                    "body_bytes": [int(row["body_bytes"] or 0), body_bytes],
                    "body_format": [current_body_format, next_body_format],
                }
            )

        if fields_changed:
            if status in {"accepted", "in_force"}:
                fields_changed["status"] = [status, "proposed"]

            update = Event(
                event_id=generate_event_id(),
                actor=actor,
                kind="decision_metadata_updated",
                entity_id=dec_ulid,
                thread_id=dec_ulid,
                payload={
                    "decision_contract_version": 5,
                    "decision_id": dec_ulid,
                    "reason": revision_reason or "Updated proposed decision in Workbench",
                    "change_kind": (
                        "content_revision"
                        if body_changed or status in {"accepted", "in_force"}
                        else "content_update"
                    ),
                    "fields_changed": fields_changed,
                },
            )
            result = append_event(
                config.events_path,
                update,
                lock_acquired=True,
                _decision_receipt=decision_receipt,
                _lock_handle=lock_handle,
            )
            last_event_seq = result.event.event_seq
            did_update = True
            uncommitted_body_path = None
        else:
            last_event_seq = int(row["event_seq"])
    except EventProtocolError as exc:
        if str(exc).startswith("prepared decision projection changed"):
            _discard_uncommitted_decision_body(uncommitted_body_path)
            uncommitted_body_path = None
        raise
    finally:
        _discard_decision_append_receipt(decision_receipt)
        lock_handle.release(last_event_seq=last_event_seq)

    assert last_event_seq is not None
    return _decision_mutation_result(
        config,
        identifier,
        operation="decision_updated" if did_update else "decision_unchanged",
        event_seq=last_event_seq,
        committed=did_update,
        flags={"updated": did_update},
    )


def accept_decision(
    repo: Path,
    identifier: str,
    *,
    expected_body_sha: str,
    expected_revision_sha: str,
    notes: str,
    actor: str,
) -> dict[str, Any]:
    config = load_config(repo)
    identifier = identifier.strip()
    expected_body_sha = expected_body_sha.strip()
    expected_revision_sha = expected_revision_sha.strip()
    notes = notes.strip()
    if not actor.strip():
        raise WorkbenchError("Approving human identity is required")
    actor = _human_decision_actor(config, actor)
    if not identifier:
        raise WorkbenchError("Decision ID is required")
    if not expected_body_sha:
        raise WorkbenchError("Reviewed decision body hash is required")
    if not expected_revision_sha:
        raise WorkbenchError("Reviewed decision revision hash is required")
    if not notes:
        raise WorkbenchError("Approval note is required")

    lock_handle = acquire(config.agent_dir / ".mail-lock")
    last_event_seq = None
    reused = False
    decision_receipt = None
    try:
        decision_receipt = _prepare_decision_append(config, lock_handle)
        conn = connect(config.db_path)
        try:
            initialize_schema(conn)
            dec_ulid = resolve_decision(conn, identifier)
            if dec_ulid is None:
                raise WorkbenchError(f"decision not found: {identifier}")
            row = conn.execute("SELECT * FROM decisions WHERE dec_ulid=?", (dec_ulid,)).fetchone()
            if row is None:
                raise WorkbenchError(f"decision not found: {identifier}")
            status = str(row["status"])
            body_sha = str(row["body_sha"])
            last_event_seq = int(row["event_seq"])
            meta = json_loads(row["meta_json"], {})
            if not isinstance(meta, dict):
                meta = {}
            collections = _decision_collections(conn, dec_ulid)
            revision_sha = _decision_revision_sha(row, meta, collections)
            review_policy = _normalize_authored_review_policy(
                meta.get("review_policy", {}), config, allow_extensions=True
            )
            review_progress = decision_review_progress(meta, revision_sha)
            body_path = str(row["body_path"] or "")
            body_bytes = int(row["body_bytes"] or 0)
            if not body_path and body_bytes == 0:
                raise WorkbenchError(
                    "Decision has no verified canonical body object; enter body content and "
                    "save the revision before approval"
                )
            try:
                read_verified_decision_body(
                    config.agent_dir,
                    body_path=body_path,
                    body_sha=body_sha,
                    body_bytes=body_bytes,
                )
            except DecisionBodyIntegrityError as exc:
                raise WorkbenchError(f"Decision body integrity check failed: {exc}") from exc
        finally:
            conn.close()

        if body_sha != expected_body_sha or revision_sha != expected_revision_sha:
            raise WorkbenchError(
                "Decision changed after it was shown for approval; review the current revision "
                "and try again"
            )
        required_reviewers = review_policy.get("required_reviewers", [])
        if required_reviewers and actor not in required_reviewers:
            raise WorkbenchError(f"actor {actor!r} is not a required reviewer")
        if status in {"accepted", "in_force"}:
            reused = True
        elif status != "proposed":
            raise WorkbenchError(f"cannot accept {identifier} while status is {status}")
        elif actor in review_progress["approved_reviewers"]:
            reused = True
        else:
            issues = decision_completeness_issues(
                tier=str(row["tier"]),
                owner=str(row["owner"] or ""),
                affected_code_globs=collections["affected_code_globs"],
                verification=collections["verification"],
                applicability_scope=str(row["applicability_scope"]),
                exemptions=collections["exemptions"],
                generated_artifact_paths=collections["generated_artifact_paths"],
            )
            if issues:
                raise WorkbenchError(
                    f"Cannot accept {identifier}: {'; '.join(issues)}. "
                    "Add the missing metadata, save the Proposed revision, and review it again."
                )
            approved_utc = utc_now()
            event = Event(
                event_id=generate_event_id(),
                occurred_utc=approved_utc,
                actor=actor,
                kind="decision_accepted",
                entity_id=dec_ulid,
                thread_id=dec_ulid,
                payload={
                    "decision_contract_version": 5,
                    "decision_id": dec_ulid,
                    "accepted_by": actor,
                    "notes": notes,
                    "approval_source": "workbench",
                    "approved_utc": approved_utc,
                    "approved_body_sha": body_sha,
                    "approved_revision_sha": revision_sha,
                    "approval_authority_mode": config.decision_approval_authority_mode,
                    "approval_authority_revision": (config.decision_approval_authority_revision),
                },
            )
            result = append_event(
                config.events_path,
                event,
                lock_acquired=True,
                _decision_receipt=decision_receipt,
                _lock_handle=lock_handle,
            )
            last_event_seq = result.event.event_seq
    finally:
        _discard_decision_append_receipt(decision_receipt)
        lock_handle.release(last_event_seq=last_event_seq)

    assert last_event_seq is not None
    return _decision_mutation_result(
        config,
        identifier,
        operation="decision_accepted",
        event_seq=last_event_seq,
        committed=not reused,
        flags={"accepted": True, "reused": reused},
    )


def reject_decision(
    repo: Path,
    identifier: str,
    *,
    expected_body_sha: str,
    expected_revision_sha: str,
    reason: str,
    actor: str,
) -> dict[str, Any]:
    """Record a direct human rejection of the exact reviewed proposal revision."""

    config = load_config(repo)
    identifier = identifier.strip()
    expected_body_sha = expected_body_sha.strip()
    expected_revision_sha = expected_revision_sha.strip()
    reason = reason.strip()
    if not actor.strip():
        raise WorkbenchError("Rejecting human identity is required")
    actor = _human_decision_actor(config, actor)
    if not identifier:
        raise WorkbenchError("Decision ID is required")
    if not expected_revision_sha:
        raise WorkbenchError("Reviewed decision revision hash is required")
    if not reason:
        raise WorkbenchError("Rejection reason is required")

    lock_handle = acquire(config.agent_dir / ".mail-lock")
    last_event_seq = None
    reused = False
    decision_receipt = None
    try:
        decision_receipt = _prepare_decision_append(config, lock_handle)
        conn = connect(config.db_path)
        try:
            initialize_schema(conn)
            dec_ulid = resolve_decision(conn, identifier)
            if dec_ulid is None:
                raise WorkbenchError(f"decision not found: {identifier}")
            row = conn.execute("SELECT * FROM decisions WHERE dec_ulid=?", (dec_ulid,)).fetchone()
            if row is None:
                raise WorkbenchError(f"decision not found: {identifier}")
            status = str(row["status"])
            body_sha = str(row["body_sha"] or "")
            last_event_seq = int(row["event_seq"])
            meta = json_loads(row["meta_json"], {})
            if not isinstance(meta, dict):
                meta = {}
            collections = _decision_collections(conn, dec_ulid)
            revision_sha = _decision_revision_sha(row, meta, collections)
            body_path = str(row["body_path"] or "")
            body_bytes = int(row["body_bytes"] or 0)
            if body_path or body_bytes:
                try:
                    read_verified_decision_body(
                        config.agent_dir,
                        body_path=body_path,
                        body_sha=body_sha,
                        body_bytes=body_bytes,
                    )
                except DecisionBodyIntegrityError as exc:
                    raise WorkbenchError(f"Decision body integrity check failed: {exc}") from exc
        finally:
            conn.close()

        if body_sha != expected_body_sha or revision_sha != expected_revision_sha:
            raise WorkbenchError(
                "Decision changed after it was shown for rejection; review the current "
                "revision and try again"
            )
        if status == "rejected":
            reused = True
        elif status != "proposed":
            raise WorkbenchError(f"cannot reject {identifier} while status is {status}")
        else:
            rejected_utc = utc_now()
            event = Event(
                event_id=generate_event_id(),
                occurred_utc=rejected_utc,
                actor=actor,
                kind="decision_rejected",
                entity_id=dec_ulid,
                thread_id=dec_ulid,
                payload={
                    "decision_contract_version": 5,
                    "decision_id": dec_ulid,
                    "rejected_by": actor,
                    "reason": reason,
                    "rejection_source": "workbench",
                    "rejected_utc": rejected_utc,
                    "rejected_body_sha": body_sha,
                    "rejected_revision_sha": revision_sha,
                    "rejection_authority_mode": config.decision_approval_authority_mode,
                    "rejection_authority_revision": (config.decision_approval_authority_revision),
                },
            )
            result = append_event(
                config.events_path,
                event,
                lock_acquired=True,
                _decision_receipt=decision_receipt,
                _lock_handle=lock_handle,
            )
            last_event_seq = result.event.event_seq
    finally:
        _discard_decision_append_receipt(decision_receipt)
        lock_handle.release(last_event_seq=last_event_seq)

    assert last_event_seq is not None
    return _decision_mutation_result(
        config,
        identifier,
        operation="decision_rejected",
        event_seq=last_event_seq,
        committed=not reused,
        flags={"rejected": True, "reused": reused},
    )


def _human_decision_actor(config: AgentMeshConfig, actor: str | None) -> str:
    normalized = (actor or config.default_sender).strip()
    if normalized not in config.participants:
        raise WorkbenchError(f"actor {normalized!r} is not in participants")
    if normalized not in config.decision_approval_identities:
        raise WorkbenchError(
            f"actor {normalized!r} is not in the configured direct-human approval authority"
        )
    return normalized


def _workbench_authoring_actor(
    config: AgentMeshConfig, actor: str | None = None, *, role: str = "actor"
) -> str:
    try:
        normalized = resolve_authoring_actor(
            read_event_records(config.events_path),
            default_actor=config.default_sender,
            explicit_actor=actor,
        )
    except AgentInstanceError as exc:
        raise WorkbenchError(str(exc)) from exc
    if normalized not in config.participants:
        raise WorkbenchError(f"{role} {normalized!r} is not in participants")
    return normalized


def _next_decision_human_id(conn) -> str:
    highest = 0
    for row in conn.execute("SELECT human_id FROM decisions"):
        match = DECISION_ID_RE.fullmatch(str(row["human_id"]))
        if match and match.group(2) is None:
            highest = max(highest, int(match.group(1)))
    return f"D{highest + 1:03d}"


def _default_decision_body(
    human_id: str,
    *,
    title: str,
    context: str,
    decision: str,
) -> str:
    return generated_decision_body(
        human_id,
        title=title,
        context=context,
        decision=decision,
    )


def _write_decision_body(
    config: AgentMeshConfig,
    body: str,
) -> tuple[str, str, int, bool]:
    data = body.encode("utf-8")
    body_sha = hashlib.sha256(data).hexdigest()
    relative = Path("bodies") / f"{body_sha}.md"
    target = config.agent_dir / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    created = not target.exists()
    if not target.exists() or target.read_bytes() != data:
        tmp = target.with_name(f".{target.name}.{secrets.token_hex(6)}.tmp")
        try:
            with tmp.open("wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, target)
        finally:
            if tmp.exists():
                tmp.unlink()
    return relative.as_posix(), body_sha, len(data), created


def _discard_uncommitted_decision_body(path: Path | None) -> None:
    if path is None:
        return
    try:
        path.unlink()
    except FileNotFoundError:
        return


def update_backlog_item(
    repo: Path,
    item_id: str,
    *,
    status: str | None = None,
    lane: str | None = None,
    priority: str | None = None,
    actor: str | None = None,
) -> dict[str, Any]:
    config = load_config(repo)
    item_id = item_id.strip()
    if not item_id:
        raise WorkbenchError("item_id is required")
    updates = {
        key: value.strip()
        for key, value in {
            "status": status,
            "lane": lane,
            "priority": priority,
        }.items()
        if value is not None
    }
    if not updates:
        raise WorkbenchError("status, lane, or priority is required")
    actor = _workbench_authoring_actor(config, actor)

    lock_handle = acquire(config.agent_dir / ".mail-lock")
    last_event_seq = None
    try:
        rebuild_all(config)
        conn = connect(config.db_path)
        try:
            initialize_schema(conn)
            row = conn.execute("SELECT * FROM backlog_items WHERE id=?", (item_id,)).fetchone()
            if row is None:
                raise WorkbenchError(f"backlog item not found: {item_id}")
            payload = _backlog_payload_from_row(row)
        finally:
            conn.close()
        payload.update(updates)
        payload["write_intent"] = "update"
        payload["refs"] = refs_with_workflow_origin(
            payload.get("refs", []),
            payload.get("workflow_origin"),
            replace=bool(payload.get("workflow_origin")),
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

    items = [item for item in list_backlog_items(config.project_root) if item["id"] == item_id]
    return {
        "ok": True,
        "item": items[0] if items else {"id": item_id, **updates},
        "event_seq": last_event_seq,
    }


def save_attachment_uploads(repo: Path, files: list[dict[str, Any]]) -> dict[str, Any]:
    config = load_config(repo)
    if not files:
        raise WorkbenchError("No files provided")
    if len(files) > MAX_ATTACHMENT_FILES:
        raise WorkbenchError(f"Too many files; maximum is {MAX_ATTACHMENT_FILES}")

    attachment_dir = validate_registered_project_path(
        config.project_root,
        config.agent_dir / "attachments" / "screenshots",
        label="attachments",
    )
    attachment_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    saved_paths: list[str] = []
    prepared: list[tuple[str, bytes]] = []
    total_bytes = 0

    for idx, item in enumerate(files, start=1):
        name = _sanitize_filename(str(item.get("name", "")))
        data_url = str(item.get("data_url", "")).strip()
        if not data_url.startswith("data:") or ";base64," not in data_url:
            raise WorkbenchError(f"Invalid data payload for {name}")
        encoded = data_url.split(";base64,", 1)[1]
        try:
            data = base64.b64decode(encoded, validate=True)
        except Exception as exc:  # pragma: no cover - defensive decode detail
            raise WorkbenchError(f"Unable to decode {name}: {exc}") from exc
        if len(data) > MAX_ATTACHMENT_BYTES:
            raise WorkbenchError(f"{name} exceeds {MAX_ATTACHMENT_BYTES // (1024 * 1024)} MB")

        stem = Path(name).stem or "upload"
        suffix = Path(name).suffix or ".bin"
        filename = f"{timestamp}-{idx:02d}-{secrets.token_hex(4)}-{stem}{suffix}"
        total_bytes += len(data)
        if total_bytes > MAX_ATTACHMENT_TOTAL_BYTES:
            raise WorkbenchError(
                f"Attachment upload exceeds {MAX_ATTACHMENT_TOTAL_BYTES // (1024 * 1024)} MB total"
            )
        prepared.append((filename, data))

    created_paths: list[Path] = []
    try:
        for filename, data in prepared:
            destination = validate_registered_project_path(
                config.project_root,
                attachment_dir / filename,
                label="attachment destination",
            )
            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            try:
                fd = os.open(destination, flags, 0o600)
            except FileExistsError as exc:
                raise WorkbenchError(f"Attachment destination already exists: {filename}") from exc
            try:
                with os.fdopen(fd, "wb") as handle:
                    handle.write(data)
                    handle.flush()
                    os.fsync(handle.fileno())
            except Exception:
                destination.unlink(missing_ok=True)
                raise
            created_paths.append(destination)
            saved_paths.append(_rel(config, destination))
    except Exception:
        for path in created_paths:
            path.unlink(missing_ok=True)
        raise

    return {
        "ok": True,
        "saved": saved_paths,
        "directory": _rel(config, attachment_dir),
    }


def build_feedback_markdown(payload: dict[str, Any]) -> dict[str, Any]:
    title = _clean(payload.get("title")) or "Untitled feedback"
    status = _clean(payload.get("status")) or "needs-review"
    severity = _clean(payload.get("severity")) or "normal"
    related_id = _clean(payload.get("related_id"))
    target = _clean(payload.get("target"))
    notes = _clean(payload.get("notes"))
    refs = _string_list(payload.get("refs"))
    screenshots = _string_list(payload.get("screenshots"))
    created_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    workflow_origin = _clean(payload.get("workflow_origin"))

    lines = [
        "# Feedback",
        "",
        f"- title: {title}",
        f"- status: {status}",
        f"- severity: {severity}",
        f"- created_utc: {created_utc}",
    ]
    if workflow_origin:
        lines.append(f"- workflow_origin: {workflow_origin}")
    if related_id:
        lines.append(f"- related_id: {related_id}")
    if target:
        lines.append(f"- target: {target}")
    lines.extend(["", "## Notes", notes or "-"])
    if refs:
        lines.extend(["", "## Refs"])
        lines.extend(f"- {ref}" for ref in refs)
    if screenshots:
        lines.extend(["", "## Screenshots"])
        lines.extend(f"- {screenshot}" for screenshot in screenshots)

    markdown = "\n".join(lines).rstrip() + "\n"
    request_title = f"Verify feedback: {title}"
    request_body = f"{FEEDBACK_REQUEST_PREAMBLE}{markdown}"
    return {
        "ok": True,
        "markdown": markdown,
        "request": {
            "title": request_title,
            "body": request_body,
        },
    }


def submit_feedback_request(repo: Path, payload: dict[str, Any]) -> dict[str, Any]:
    config = load_config(repo)
    _validate_feedback_for_submit(payload)
    draft = build_feedback_markdown(payload)
    explicit_sender = _clean(payload.get("sender")) or None
    sender = _workbench_authoring_actor(config, explicit_sender, role="sender")
    if sender not in config.participants:
        raise WorkbenchError(f"sender {sender!r} is not in participants")
    raw_to = _clean(payload.get("to")) or config.default_recipient
    recipients = config.canonical_recipients(raw_to)
    unknown_recipients = [item for item in recipients if item not in config.participants]
    if unknown_recipients:
        raise WorkbenchError(
            "feedback recipient(s) are not participants: " + ", ".join(unknown_recipients)
        )
    submission_id = _feedback_submission_id(payload)
    workflow_origin = _clean(payload.get("workflow_origin"))
    submission_digest = _feedback_submission_digest(
        payload,
        sender=sender,
        raw_to=raw_to,
    )

    lock_handle = acquire(config.agent_dir / ".mail-lock")
    last_event_seq = None
    try:
        recover(config.events_path, config.agent_dir)
        existing = _find_feedback_submission(config.events_path, submission_id)
        if existing is not None:
            existing_digest = str(existing.get("payload", {}).get("feedback_submission_digest", ""))
            if existing_digest != submission_digest:
                raise WorkbenchError(
                    "feedback submission_id was already used for different form content"
                )
            rebuild_all(config)
            return _feedback_response_from_record(existing, reused=True)

        request_id = new_public_message_id("REQ", sender)
        event_payload: dict[str, Any] = {
            "from": sender,
            "to": recipients,
            "title": draft["request"]["title"],
            "body": draft["request"]["body"],
            "feature": "feedback",
            "refs": refs_with_workflow_origin(_feedback_refs(payload), workflow_origin),
            "response_mode": "single",
            "feedback_submission_id": submission_id,
            "feedback_submission_digest": submission_digest,
        }
        if workflow_origin:
            event_payload["workflow_origin"] = workflow_origin
        if config.routing.preserve_raw_to:
            event_payload["original_to"] = raw_to
        event = Event(
            event_id=generate_event_id(),
            actor=sender,
            kind="req_created",
            entity_id=request_id,
            thread_id=request_id,
            payload=event_payload,
        )
        result = append_event(config.events_path, event, lock_acquired=True)
        last_event_seq = result.event.event_seq
        rebuild_all(config)
        return _feedback_response_from_record(result.event.to_dict(), reused=False)
    finally:
        lock_handle.release(last_event_seq=last_event_seq)


def feedback_submission_receipt(repo: Path, submission_id: str) -> dict[str, Any]:
    config = load_config(repo)
    normalized = _validate_feedback_submission_id(submission_id)
    lock_handle = acquire(config.agent_dir / ".mail-lock")
    try:
        recover(config.events_path, config.agent_dir)
        record = _find_feedback_submission(config.events_path, normalized)
        if record is None:
            return {
                "ok": True,
                "found": False,
                "submission_id": normalized,
            }
        rebuild_all(config)
        return _feedback_response_from_record(record, reused=True)
    finally:
        lock_handle.release()


def _feedback_submission_id(payload: dict[str, Any]) -> str:
    requested = _clean(payload.get("submission_id"))
    if not requested:
        requested = f"fb-{secrets.token_hex(16)}"
    return _validate_feedback_submission_id(requested)


def _validate_feedback_submission_id(submission_id: str) -> str:
    normalized = submission_id.strip()
    if not FEEDBACK_SUBMISSION_ID_RE.fullmatch(normalized):
        raise WorkbenchError(
            "feedback submission_id must start with fb- and contain 8-96 letters, numbers, or hyphens"
        )
    return normalized


def _feedback_submission_digest(
    payload: dict[str, Any],
    *,
    sender: str,
    raw_to: str,
) -> str:
    normalized = {
        "title": _clean(payload.get("title")),
        "related_id": _clean(payload.get("related_id")),
        "severity": _clean(payload.get("severity")) or "normal",
        "target": _clean(payload.get("target")),
        "notes": _clean(payload.get("notes")),
        "refs": _string_list(payload.get("refs")),
        "screenshots": _string_list(payload.get("screenshots")),
        "workflow_origin": _clean(payload.get("workflow_origin")),
        "sender": sender,
        "to": raw_to,
    }
    encoded = json.dumps(normalized, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _find_feedback_submission(events_path: Path, submission_id: str) -> dict[str, Any] | None:
    for record in reversed(list(read_event_records(events_path))):
        event_payload = record.get("payload", {})
        if (
            record.get("kind") == "req_created"
            and event_payload.get("feature") == "feedback"
            and event_payload.get("feedback_submission_id") == submission_id
        ):
            return record
    return None


def _feedback_response_from_record(
    record: dict[str, Any],
    *,
    reused: bool,
) -> dict[str, Any]:
    event_payload = record.get("payload", {})
    body = str(event_payload.get("body", ""))
    markdown = (
        body[len(FEEDBACK_REQUEST_PREAMBLE) :]
        if body.startswith(FEEDBACK_REQUEST_PREAMBLE)
        else body
    )
    return {
        "ok": True,
        "found": True,
        "reused": reused,
        "submission_id": str(event_payload.get("feedback_submission_id", "")),
        "request_id": str(record.get("entity_id", "")),
        "event_seq": int(record.get("event_seq", 0)),
        "to": list(event_payload.get("to", [])),
        "markdown": markdown,
        "request": {
            "title": str(event_payload.get("title", "")),
            "body": body,
        },
    }


def update_request_status(
    repo: Path,
    request_id: str,
    *,
    to_status: str,
    reason: str,
    actor: str | None = None,
) -> dict[str, Any]:
    config = load_config(repo)
    request_id = request_id.strip()
    to_status = to_status.strip()
    reason = reason.strip()
    actor = _workbench_authoring_actor(config, actor)
    if not request_id:
        raise WorkbenchError("request_id is required")
    if to_status not in {"open", "closed"}:
        raise WorkbenchError("to_status must be open or closed")
    if not reason:
        raise WorkbenchError("Reason is required to change request status")

    lock_handle = acquire(config.agent_dir / ".mail-lock")
    last_event_seq = None
    try:
        rebuild_all(config)
        conn = connect(config.db_path)
        try:
            initialize_schema(conn)
            row = resolve_message(conn, request_id)
            if row is None or row["kind"] != "request":
                raise WorkbenchError(f"request not found: {request_id}")
            from_status = row["status"]
        finally:
            conn.close()
        event = Event(
            event_id=generate_event_id(),
            actor=actor,
            kind="req_status_changed",
            entity_id=request_id,
            thread_id=request_id,
            payload={
                "from_status": from_status,
                "to_status": to_status,
                "reason": reason,
                "actor": actor,
            },
        )
        result = append_event(config.events_path, event, lock_acquired=True)
        last_event_seq = result.event.event_seq
    finally:
        lock_handle.release(last_event_seq=last_event_seq)

    message = lookup_message(config.project_root, request_id)
    return {
        "ok": True,
        "request_id": request_id,
        "status": to_status,
        "event_seq": last_event_seq,
        "message": message,
    }


def _validate_feedback_for_submit(payload: dict[str, Any]) -> None:
    missing: list[str] = []
    if not _clean(payload.get("title")):
        missing.append("Title")
    if not _clean(payload.get("notes")):
        missing.append("Notes")
    if missing:
        if len(missing) == 1:
            raise WorkbenchError(f"{missing[0]} is required to submit feedback.")
        raise WorkbenchError(f"{' and '.join(missing)} are required to submit feedback.")
    workflow_origin = _clean(payload.get("workflow_origin"))
    if workflow_origin and workflow_origin not in WORKFLOW_ORIGINS:
        raise WorkbenchError("Workflow origin must be one of: " + ", ".join(WORKFLOW_ORIGINS))


def message_packet_block(packet: dict[str, Any]) -> str:
    message = packet.get("message", {})
    label = message.get("title") if message.get("kind") == "request" else message.get("summary")
    lines = [
        f"## {message.get('id', '')}",
        f"- kind: {message.get('kind', '')}",
        f"- thread_id: {message.get('thread_id', '')}",
        f"- from: {message.get('sender', '')}",
        f"- workflow_origin: {message.get('workflow_origin') or ''}",
    ]
    recipients = message.get("recipients") or []
    if recipients:
        lines.append(f"- to: {', '.join(str(item) for item in recipients)}")
    if message.get("request_id"):
        lines.append(f"- request_id: {message.get('request_id')}")
    if label:
        lines.append(
            f"- title: {label}" if message.get("kind") == "request" else f"- summary: {label}"
        )
    if message.get("status"):
        lines.append(f"- status: {message.get('status')}")
    lines.extend(["", "### Message", str(message.get("body") or "")])
    return "\n".join(lines).rstrip() + "\n"


def decision_detail_block(
    row,
    meta: dict[str, Any],
    body: str,
    *,
    collections: dict[str, Any] | None = None,
    completeness_issues: tuple[str, ...] = (),
    superseded_by: str = "",
) -> str:
    status_utc = _decision_status_utc(row, meta)
    proposed_utc = _decision_proposed_utc(row, meta)
    lineage = decision_lineage_summary(meta)
    revision_count = _decision_revision_count(meta)
    approved_version_count = _decision_approved_version_count(
        meta, current_status=str(row["status"])
    )
    collections = collections or {}
    revision_sha = _decision_revision_sha(row, meta, collections)
    review_progress = decision_review_progress(meta, revision_sha)
    lines = [
        f"## {row['human_id']}",
        f"- status: {row['status']}",
        f"- status_utc: {status_utc}",
        f"- tier: {row['tier']}",
        f"- tier_valid: {bool(row['tier_valid'])}",
        f"- applicability_scope: {row['applicability_scope']}",
        f"- owner: {row['owner'] or ''}",
        f"- drift_risk: {row['drift_risk'] or ''}",
        f"- proposed_utc: {proposed_utc}",
        f"- revision: {revision_count}",
        f"- approved_version: {approved_version_count or ''}",
        f"- content_revisions: {lineage.content_revisions}",
        f"- revisit_annotations: {lineage.revisit_annotations}",
        f"- complete_for_acceptance: {not completeness_issues}",
        f"- revision_sha: {revision_sha}",
    ]
    if review_progress["approval_binding"] == "legacy_pre_authoring_digest":
        lines.extend(
            [
                f"- approved_revision_sha: {review_progress['approval_revision_sha']}",
                "- approval_binding: legacy_pre_authoring_digest",
            ]
        )
    if review_progress["required_reviewers"]:
        lines.extend(
            [
                "- required_reviewers: " + ", ".join(review_progress["required_reviewers"]),
                f"- approval_quorum: {review_progress['approval_quorum']}",
                "- approved_reviewers: " + ", ".join(review_progress["approved_reviewers"]),
                f"- approvals_recorded: {review_progress['approvals_recorded']}",
            ]
        )
    if completeness_issues:
        lines.append(f"- completeness_issues: {'; '.join(completeness_issues)}")
    for label, values in (
        ("affected_code_globs", collections.get("affected_code_globs", [])),
        ("exemptions", collections.get("exemptions", [])),
        (
            "generated_artifact_paths",
            collections.get("generated_artifact_paths", []),
        ),
        ("required_checks", collections.get("required_checks", [])),
        (
            "verification_commands",
            [
                str(item.get("command") or "")
                for item in collections.get("verification", [])
                if isinstance(item, dict)
            ],
        ),
        ("tags", collections.get("tags", [])),
    ):
        if values:
            lines.append(f"- {label}: {', '.join(str(value) for value in values)}")
    for assumption in collections.get("assumptions", []):
        if isinstance(assumption, dict):
            references = assumption.get("references", [])
            reference_suffix = ""
            if isinstance(references, list) and references:
                reference_suffix = (
                    " (references: " + ", ".join(str(reference) for reference in references) + ")"
                )
            lines.append(
                f"- assumption {assumption.get('id', '')} "
                f"[{assumption.get('status', 'active')}]: "
                f"{assumption.get('text', '')}{reference_suffix}"
            )
    evidence = collections.get("evidence", {})
    if isinstance(evidence, dict):
        for kind, values in evidence.items():
            for value in values if isinstance(values, list) else [values]:
                lines.append(f"- evidence {kind}: {value}")
    if row["accepted_utc"]:
        lines.append(f"- accepted_utc: {row['accepted_utc']}")
        event_log = meta.get("event_log", [])
        latest_acceptance: dict[str, Any] = {}
        if isinstance(event_log, list):
            for item in reversed(event_log):
                if not isinstance(item, dict) or item.get("kind") != "decision_accepted":
                    continue
                payload = item.get("payload", {})
                if isinstance(payload, dict):
                    latest_acceptance = payload
                break
        if latest_acceptance.get("accepted_by"):
            lines.append(f"- accepted_by: {latest_acceptance['accepted_by']}")
        if latest_acceptance.get("approval_source"):
            lines.append(f"- approval_source: {latest_acceptance['approval_source']}")
        if latest_acceptance.get("approved_body_sha"):
            lines.append(f"- approved_body_sha: {latest_acceptance['approved_body_sha']}")
        if latest_acceptance.get("notes"):
            lines.append(f"- approval_note: {latest_acceptance['notes']}")
    if row["in_force_utc"]:
        lines.append(f"- in_force_utc: {row['in_force_utc']}")
    if row["retired_utc"]:
        lines.append(f"- retired_utc: {row['retired_utc']}")
    if superseded_by:
        lines.append(f"- superseded_by: {superseded_by}")
    lines.extend(["", str(row["title"] or "")])
    if body.strip():
        lines.extend(["", "## Body", body.rstrip()])
    else:
        _append_meta_section(lines, "Context", meta.get("context"))
        _append_meta_section(lines, "Decision", meta.get("decision"))
        _append_meta_section(lines, "Consequences", meta.get("consequences"))
        _append_meta_section(lines, "Rejected Alternatives", meta.get("rejected_alternatives"))
        _append_meta_section(
            lines, "Generated Artifact Paths", meta.get("generated_artifact_paths")
        )
    return "\n".join(lines).rstrip() + "\n"


def _decision_body_from_row(config: AgentMeshConfig, row) -> str:
    body_path = str(row["body_path"] or "")
    body_sha = str(row["body_sha"] or "")
    body_bytes = int(row["body_bytes"] or 0)
    # Historical metadata-only decisions can carry a legacy digest placeholder
    # without a body object.  Zero bytes plus no path is the explicit empty-body
    # representation; a non-zero missing body still fails closed below.
    if not body_path and body_bytes == 0:
        return ""
    try:
        data = read_verified_decision_body(
            config.agent_dir,
            body_path=body_path,
            body_sha=body_sha,
            body_bytes=body_bytes,
        )
        return data.decode("utf-8")
    except (DecisionBodyIntegrityError, UnicodeDecodeError) as exc:
        raise ReadModelUnavailable(f"decision body integrity check failed: {exc}") from exc


def _decision_revision_count(meta: dict[str, Any]) -> int:
    event_log = meta.get("event_log", [])
    if not isinstance(event_log, list):
        return 1
    updates = 0
    for item in event_log:
        if not isinstance(item, dict) or item.get("kind") != "decision_metadata_updated":
            continue
        payload = item.get("payload", {})
        fields = payload.get("fields_changed", {}) if isinstance(payload, dict) else {}
        if isinstance(fields, dict) and DECISION_REVISION_AUTHORITY_FIELDS.intersection(fields):
            updates += 1
    return updates + 1


def _decision_approved_version_count(meta: dict[str, Any], *, current_status: str) -> int:
    """Count revisions proven to have completed human approval.

    A later authority-bearing edit records the prior approved lifecycle in its
    status transition. The current projection status proves the final revision.
    This intentionally does not count individual reviewer votes as versions.
    """

    event_log = meta.get("event_log", [])
    if not isinstance(event_log, list):
        return 1 if current_status in DECISION_APPROVED_STATUSES else 0
    approved_versions = 0
    for item in event_log:
        if not isinstance(item, dict) or item.get("kind") != "decision_metadata_updated":
            continue
        payload = item.get("payload", {})
        fields = payload.get("fields_changed", {}) if isinstance(payload, dict) else {}
        if not isinstance(fields, dict):
            continue
        status_change = fields.get("status")
        if (
            isinstance(status_change, list)
            and len(status_change) == 2
            and status_change[0] in {"accepted", "in_force"}
            and status_change[1] == "proposed"
        ):
            approved_versions += 1
    if current_status in DECISION_APPROVED_STATUSES:
        approved_versions += 1
    return approved_versions


def _history_contract_version(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError, OverflowError):
        return 0


def _decision_history_state_from_proposal(
    payload: dict[str, Any], *, dec_ulid: str
) -> dict[str, Any]:
    affected = list(payload.get("affected_code_globs") or [])
    return {
        "decision_id": dec_ulid,
        "contract_version": _history_contract_version(payload.get("decision_contract_version")),
        "human_id": str(payload.get("human_id") or ""),
        "title": str(payload.get("title") or ""),
        "tier": str(payload.get("tier") or ""),
        "applicability_scope": applicability_scope_for(
            payload.get("applicability_scope"), affected
        ),
        "owner": str(payload.get("owner") or ""),
        "context": str(payload.get("context") or ""),
        "decision": str(payload.get("decision") or ""),
        "body_sha": str(payload.get("body_sha") or ""),
        "body_path": str(payload.get("body_path") or ""),
        "body_bytes": int(payload.get("body_bytes") or 0),
        "affected_code_globs": affected,
        "exemptions": list(payload.get("exemptions") or []),
        "generated_artifact_paths": list(payload.get("generated_artifact_paths") or []),
        "required_checks": list(payload.get("required_checks") or []),
        "verification": list(payload.get("verification") or []),
        "tags": list(payload.get("tags") or []),
        "assumptions": list(payload.get("assumptions") or []),
        "evidence": payload.get("evidence") or {},
        "review_policy": payload.get("review_policy") or {},
    }


def _decision_history_revision_sha(state: dict[str, Any]) -> str:
    digest_kwargs: dict[str, Any] = {}
    if int(state.get("contract_version") or 0) >= 5:
        digest_kwargs = {
            "assumptions": state.get("assumptions", []),
            "evidence": state.get("evidence", {}),
            "review_policy": state.get("review_policy", {}),
        }
    return decision_revision_digest(
        decision_id=str(state["decision_id"]),
        human_id=str(state["human_id"]),
        title=str(state["title"]),
        tier=str(state["tier"]),
        applicability_scope=str(state["applicability_scope"]),
        owner=str(state.get("owner") or ""),
        context=str(state.get("context") or ""),
        decision=str(state.get("decision") or ""),
        body_sha=str(state.get("body_sha") or ""),
        affected_code_globs=state.get("affected_code_globs", []),
        exemptions=state.get("exemptions", []),
        generated_artifact_paths=state.get("generated_artifact_paths", []),
        required_checks=state.get("required_checks", []),
        verification=state.get("verification", []),
        tags=state.get("tags", []),
        **digest_kwargs,
    )


def _decision_revision_history(
    config: AgentMeshConfig,
    snapshot: ReadModelSnapshot,
    *,
    dec_ulid: str,
    current_revision_sha: str,
    current_status: str,
    deadline_monotonic: float,
) -> dict[str, Any]:
    """Derive bounded authored revisions and completed approval versions."""

    try:
        state: dict[str, Any] | None = None
        revisions: list[dict[str, Any]] = []

        def targets_decision(record: dict[str, Any]) -> bool:
            payload = record.get("payload", {})
            payload_id = payload.get("decision_id") if isinstance(payload, dict) else ""
            return str(record.get("entity_id") or payload_id or "") == dec_ulid

        def add_revision(record: dict[str, Any], *, change_kind: str, reason: str) -> None:
            assert state is not None
            if revisions and revisions[-1]["outcome"] == "draft":
                revisions[-1]["outcome"] = "updated"
            field_omissions: list[str] = []
            omitted_fields: list[str] = []
            human_fields: dict[str, str] = {}
            field_sha256: dict[str, str] = {}
            for key in ("title", "context", "decision"):
                value = state.get(key, "")
                field_sha256[key] = hashlib.sha256(
                    json.dumps(value, ensure_ascii=False, sort_keys=True).encode("utf-8")
                ).hexdigest()
                bounded = _bounded_decision_diff_value(
                    key,
                    value,
                    max_bytes=MAX_DECISION_HISTORY_FIELD_BYTES,
                )
                if isinstance(bounded, str):
                    human_fields[key] = bounded
                else:
                    human_fields[key] = ""
                    omitted_fields.append(key)
                    if isinstance(bounded, dict) and bounded.get("_display_omitted"):
                        field_omissions.append(str(bounded["_display_omitted"]))
            revisions.append(
                {
                    "revision": len(revisions) + 1,
                    "approved_version": None,
                    "revision_sha": _decision_history_revision_sha(state),
                    "body_sha": str(state.get("body_sha") or ""),
                    "body_path": str(state.get("body_path") or ""),
                    "body_bytes": int(state.get("body_bytes") or 0),
                    "authored_utc": str(record.get("occurred_utc") or ""),
                    "author": str(record.get("actor") or ""),
                    "reason": reason,
                    "change_kind": change_kind,
                    "title": human_fields["title"],
                    "context": human_fields["context"],
                    "decision": human_fields["decision"],
                    "field_sha256": field_sha256,
                    "omitted_fields": omitted_fields,
                    "field_omissions": field_omissions,
                    "approvals": [],
                    "approval_complete": False,
                    "outcome": "draft",
                }
            )

        def complete_current_approval() -> None:
            if not revisions or revisions[-1]["approval_complete"]:
                return
            revisions[-1]["approval_complete"] = True
            revisions[-1]["outcome"] = "approved"

        for record in snapshot.records:
            if time.monotonic() >= deadline_monotonic:
                raise ReadModelUnavailable("decision version-history scan exceeded its time budget")
            if not targets_decision(record):
                continue
            kind = str(record.get("kind") or "")
            payload = record.get("payload", {})
            if not isinstance(payload, dict):
                continue
            if kind == "decision_proposed":
                state = _decision_history_state_from_proposal(payload, dec_ulid=dec_ulid)
                add_revision(record, change_kind="initial_proposal", reason="Initial proposal")
                continue
            if state is None:
                continue
            if kind == "decision_metadata_updated":
                fields = payload.get("fields_changed", {})
                if not isinstance(fields, dict):
                    continue
                changed_authority = DECISION_REVISION_AUTHORITY_FIELDS.intersection(fields)
                if not changed_authority:
                    continue
                status_change = fields.get("status")
                if (
                    isinstance(status_change, list)
                    and len(status_change) == 2
                    and status_change[0] in {"accepted", "in_force"}
                    and status_change[1] == "proposed"
                ):
                    complete_current_approval()
                state["contract_version"] = max(
                    int(state.get("contract_version") or 0),
                    _history_contract_version(payload.get("decision_contract_version")),
                )
                for field_name, old_new in fields.items():
                    if field_name == "status":
                        continue
                    if isinstance(old_new, list) and len(old_new) == 2:
                        state[field_name] = old_new[1]
                    else:
                        state[field_name] = old_new
                add_revision(
                    record,
                    change_kind=str(payload.get("change_kind") or "content_update"),
                    reason=str(payload.get("reason") or "Updated decision"),
                )
                continue
            if kind == "decision_accepted" and revisions:
                accepted_by = str(payload.get("accepted_by") or record.get("actor") or "")
                revisions[-1]["approvals"].append(
                    {
                        "accepted_by": accepted_by,
                        "approved_utc": str(
                            payload.get("approved_utc") or record.get("occurred_utc") or ""
                        ),
                        "notes": str(payload.get("notes") or ""),
                    }
                )
                policy = normalize_decision_review_policy(
                    state.get("review_policy", {}), allow_extensions=True
                )
                required_reviewers = set(policy.get("required_reviewers", []))
                if not required_reviewers:
                    complete_current_approval()
                else:
                    approved_reviewers = {
                        str(item.get("accepted_by") or "")
                        for item in revisions[-1]["approvals"]
                        if str(item.get("accepted_by") or "") in required_reviewers
                    }
                    if len(approved_reviewers) >= int(policy["approval_quorum"]):
                        complete_current_approval()
                continue
            if kind == "decision_rejected" and revisions:
                revisions[-1]["outcome"] = "rejected"

        if not revisions:
            raise ReadModelUnavailable("canonical proposal event is unavailable")
        if revisions[-1]["revision_sha"] != current_revision_sha:
            raise ReadModelUnavailable(
                "derived current version does not match the verified projection revision"
            )
        if current_status in DECISION_APPROVED_STATUSES:
            complete_current_approval()
        revisions[-1]["outcome"] = current_status
        approved_versions = 0
        for revision in revisions:
            if revision["approval_complete"]:
                approved_versions += 1
                revision["approved_version"] = approved_versions
        total_revisions = len(revisions)
        omitted_revisions = max(0, total_revisions - MAX_DECISION_HISTORY_VERSIONS)
        visible_revisions = revisions[-MAX_DECISION_HISTORY_VERSIONS:]
        body_cache: dict[tuple[str, str, int], tuple[str, str]] = {}
        total_body_bytes = 0
        for revision in reversed(visible_revisions):
            if time.monotonic() >= deadline_monotonic:
                raise ReadModelUnavailable(
                    "decision version-history body reads exceeded their time budget"
                )
            body_path = str(revision.pop("body_path") or "")
            body_sha = str(revision["body_sha"] or "")
            body_bytes = int(revision.pop("body_bytes") or 0)
            body_key = (body_path, body_sha, body_bytes)
            body = ""
            omission = ""
            if body_key in body_cache:
                body, omission = body_cache[body_key]
            elif not body_path and body_bytes == 0:
                body_cache[body_key] = ("", "")
            elif body_bytes > MAX_DECISION_HISTORY_BODY_BYTES:
                omission = (
                    f"Canonical body is {body_bytes} bytes; version comparison limit is "
                    f"{MAX_DECISION_HISTORY_BODY_BYTES} bytes."
                )
                body_cache[body_key] = ("", omission)
            elif total_body_bytes + body_bytes > MAX_DECISION_HISTORY_TOTAL_BODY_BYTES:
                omission = "Canonical body omitted because the bounded history budget is full."
                body_cache[body_key] = ("", omission)
            else:
                body = _decision_body_from_row(
                    config,
                    {
                        "body_path": body_path,
                        "body_sha": body_sha,
                        "body_bytes": body_bytes,
                    },
                )
                total_body_bytes += body_bytes
                body_cache[body_key] = (body, "")
            revision["body"] = body
            revision["body_available"] = not omission
            revision["body_omission"] = omission
        return {
            "available": True,
            "total_revisions": total_revisions,
            "approved_versions": approved_versions,
            "omitted_revisions": omitted_revisions,
            "revisions": visible_revisions,
        }
    except (ReadModelUnavailable, ValueError, TypeError, KeyError, RecursionError) as exc:
        return {
            "available": False,
            "total_revisions": 1,
            "approved_versions": 0,
            "omitted_revisions": 0,
            "revisions": [],
            "diagnostic": f"Revision history is unavailable: {exc}",
        }


def _pending_decision_revision_diff(
    config: AgentMeshConfig,
    snapshot: ReadModelSnapshot,
    *,
    row: Any,
    meta: dict[str, Any],
    body: str,
    collections: dict[str, Any],
    revision_sha: str,
    deadline_monotonic: float,
) -> dict[str, Any]:
    """Compare a Proposed decision with its last fully approved revision."""

    if str(row["status"]) != "proposed":
        return {"pending": False, "available": True}

    acquired = _DECISION_REVISION_DIFF_CAPACITY.acquire(blocking=False)
    try:
        if not acquired:
            raise ReadModelUnavailable(
                "revision-comparison capacity is busy; wait for the other comparison to finish"
            )
        baseline = _last_approved_decision_revision(
            config,
            snapshot,
            dec_ulid=str(row["dec_ulid"]),
            deadline_monotonic=deadline_monotonic,
        )
        raw_current_authority = _decision_authority_snapshot(row, meta, collections)
        raw_baseline_authority = baseline["authority"] if baseline is not None else {}
        metadata_changed = raw_baseline_authority != raw_current_authority
        current_authority = _bounded_decision_authority_snapshot(raw_current_authority)
        baseline_authority = _bounded_decision_authority_snapshot(raw_baseline_authority)
        body_before = str(baseline["body"]) if baseline is not None else ""
        body_changed = body_before != body
        baseline_label = (
            f"Last fully approved revision at event {baseline['event_seq']}"
            if baseline is not None
            else "No prior human-approved revision (empty baseline)"
        )
        metadata_diff = (
            "Authority metadata changed, but the bounded display values are identical "
            "because one or more fields exceeded display limits. Compare the labeled "
            "revision hashes.\n"
            if metadata_changed and baseline_authority == current_authority
            else _bounded_unified_diff(
                json.dumps(
                    baseline_authority,
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                ).splitlines(),
                json.dumps(
                    current_authority,
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                ).splitlines(),
                fromfile="approved authority" if baseline is not None else "empty authority",
                tofile="pending authority",
                max_output_bytes=MAX_DECISION_DIFF_METADATA_OUTPUT_BYTES,
                unchanged_message="No authority-bearing metadata changes.",
            )
        )
        body_diff = _bounded_decision_body_diff(
            body_before,
            body,
            fromfile="approved body" if baseline is not None else "empty body",
            tofile="pending body",
        )
        high_level_before = {
            key: baseline_authority.get(key, "") for key in ("title", "context", "decision")
        }
        high_level_after = {
            key: current_authority.get(key, "") for key in ("title", "context", "decision")
        }
        return {
            "pending": True,
            "available": True,
            "baseline_kind": "approved_revision" if baseline is not None else "empty",
            "baseline_label": baseline_label,
            "baseline_event_seq": baseline["event_seq"] if baseline is not None else None,
            "baseline_approved_utc": (baseline["approved_utc"] if baseline is not None else ""),
            "baseline_accepted_by": (baseline["accepted_by"] if baseline is not None else ""),
            "baseline_revision_sha": (baseline["revision_sha"] if baseline is not None else ""),
            "baseline_body_sha": baseline["body_sha"] if baseline is not None else "",
            "pending_revision_sha": revision_sha,
            "pending_body_sha": str(row["body_sha"] or ""),
            "metadata_changed": metadata_changed,
            "body_changed": body_changed,
            "has_changes": metadata_changed or body_changed,
            "metadata_diff": metadata_diff,
            "body_diff": body_diff,
            "high_level_before": high_level_before,
            "high_level_after": high_level_after,
        }
    except (ReadModelUnavailable, ValueError, TypeError, RecursionError) as exc:
        return {
            "pending": True,
            "available": False,
            "diagnostic": f"Revision comparison is unavailable: {exc}",
            "pending_revision_sha": revision_sha,
            "pending_body_sha": str(row["body_sha"] or ""),
        }
    finally:
        if acquired:
            _DECISION_REVISION_DIFF_CAPACITY.release()


def _last_approved_decision_revision(
    config: AgentMeshConfig,
    snapshot: ReadModelSnapshot,
    *,
    dec_ulid: str,
    deadline_monotonic: float,
) -> dict[str, Any] | None:
    def is_acceptance_record(record: dict[str, Any]) -> bool:
        if record.get("kind") != "decision_accepted":
            return False
        payload = record.get("payload", {})
        return (
            isinstance(payload, dict)
            and str(record.get("entity_id") or payload.get("decision_id") or "") == dec_ulid
        )

    # A first proposal has no approved baseline.  Establishing that fact from
    # the already verified immutable records avoids a second full replay.
    has_approved_baseline = False
    for record in snapshot.records:
        if time.monotonic() >= deadline_monotonic:
            raise ReadModelUnavailable("approved-baseline scan exceeded its time budget")
        if is_acceptance_record(record):
            has_approved_baseline = True
            break
    if time.monotonic() >= deadline_monotonic:
        raise ReadModelUnavailable("approved-baseline scan exceeded its time budget")
    if not has_approved_baseline:
        return None

    def fully_approved_at_boundary(conn: Any, record: dict[str, Any]) -> bool:
        if not is_acceptance_record(record):
            return False
        projected = conn.execute(
            "SELECT status FROM decisions WHERE dec_ulid=?",
            (dec_ulid,),
        ).fetchone()
        return projected is not None and str(projected["status"]) in {
            "accepted",
            "in_force",
        }

    def capture_approved_state(conn: Any, record: dict[str, Any]) -> dict[str, Any]:
        historical_row = conn.execute(
            "SELECT * FROM decisions WHERE dec_ulid=?",
            (dec_ulid,),
        ).fetchone()
        if historical_row is None or str(historical_row["status"]) not in {
            "accepted",
            "in_force",
        }:
            raise ReadModelUnavailable(
                "approved baseline is not fully approved at its canonical replay boundary"
            )
        historical_meta = json_loads(historical_row["meta_json"], {})
        if not isinstance(historical_meta, dict):
            historical_meta = {}
        historical_collections = _decision_collections(conn, dec_ulid)
        historical_revision_sha = _decision_revision_sha(
            historical_row,
            historical_meta,
            historical_collections,
        )
        payload = record.get("payload", {})
        if not isinstance(payload, dict):
            raise ReadModelUnavailable("approved baseline payload is not an object")
        return {
            "record": dict(record),
            "payload": dict(payload),
            "row": dict(historical_row),
            "meta": historical_meta,
            "collections": historical_collections,
            "historical_revision_sha": historical_revision_sha,
        }

    match = latest_read_model_match(
        config,
        snapshot,
        predicate=fully_approved_at_boundary,
        capture=capture_approved_state,
        max_events=MAX_DECISION_REVISION_REPLAY_EVENTS,
        max_source_bytes=MAX_DECISION_REVISION_REPLAY_SOURCE_BYTES,
        deadline_monotonic=deadline_monotonic,
    )
    if match is None:
        return None
    event_seq, captured = match
    record = captured["record"]
    payload = captured["payload"]
    historical_row = captured["row"]
    historical_meta = captured["meta"]
    historical_collections = captured["collections"]
    historical_revision_sha = captured["historical_revision_sha"]
    historical_body = _decision_body_from_row(config, historical_row)
    approved_revision_sha = str(payload.get("approved_revision_sha") or "")
    if approved_revision_sha and approved_revision_sha != historical_revision_sha:
        raise ReadModelUnavailable(
            "approved revision digest does not match its canonical replay boundary"
        )
    approved_body_sha = str(payload.get("approved_body_sha") or "")
    historical_body_sha = str(historical_row["body_sha"] or "")
    if approved_body_sha and approved_body_sha != historical_body_sha:
        raise ReadModelUnavailable(
            "approved body digest does not match its canonical replay boundary"
        )
    return {
        "event_seq": event_seq,
        "approved_utc": str(payload.get("approved_utc") or record.get("occurred_utc") or ""),
        "accepted_by": str(payload.get("accepted_by") or record.get("actor") or ""),
        "revision_sha": approved_revision_sha or historical_revision_sha,
        "body_sha": approved_body_sha or historical_body_sha,
        "authority": _decision_authority_snapshot(
            historical_row,
            historical_meta,
            historical_collections,
        ),
        "body": historical_body,
    }


def _decision_authority_snapshot(
    row: Any,
    meta: dict[str, Any],
    collections: dict[str, Any],
) -> dict[str, Any]:
    verification: list[dict[str, Any]] = []
    for item in collections.get("verification", []):
        if not isinstance(item, dict):
            continue
        verification.append(
            {
                key: item.get(key)
                for key in (
                    "command",
                    "execution_mode",
                    "argv",
                    "expected_signal",
                    "runtime_cost",
                    "drift_risk",
                )
                if item.get(key) is not None
            }
        )
    authority: dict[str, Any] = {
        "human_id": str(row["human_id"]),
        "title": str(row["title"]),
        "tier": str(row["tier"]),
        "applicability_scope": str(row["applicability_scope"]),
        "owner": str(row["owner"] or ""),
        "context": str(meta.get("context") or ""),
        "decision": str(meta.get("decision") or ""),
        "body_sha": str(row["body_sha"] or ""),
        "affected_code_globs": list(collections.get("affected_code_globs", [])),
        "exemptions": list(collections.get("exemptions", [])),
        "generated_artifact_paths": list(collections.get("generated_artifact_paths", [])),
        "required_checks": list(collections.get("required_checks", [])),
        "verification": verification,
        "tags": list(collections.get("tags", [])),
    }
    try:
        contract_version = int(row["contract_version"] or 0)
    except (IndexError, KeyError, TypeError, ValueError, OverflowError):
        contract_version = 0
    if contract_version >= 5:
        authority.update(
            {
                "assumptions": [
                    {
                        "id": str(item.get("id") or ""),
                        "text": str(item.get("text") or ""),
                        "references": list(item.get("references") or []),
                    }
                    for item in collections.get("assumptions", [])
                    if isinstance(item, dict)
                ],
                "evidence": collections.get("evidence", {}),
                "review_policy": meta.get("review_policy", {}),
            }
        )
    return authority


def _bounded_decision_authority_snapshot(value: dict[str, Any]) -> dict[str, Any]:
    return {key: _bounded_decision_diff_value(key, item) for key, item in value.items()}


def _bounded_decision_diff_value(
    label: str,
    value: Any,
    *,
    max_bytes: int = MAX_DECISION_DIFF_VALUE_BYTES,
) -> Any:
    if isinstance(value, (list, tuple)) and len(value) > MAX_DECISION_DIFF_COLLECTION_ITEMS:
        return {
            "_display_omitted": (
                f"{label} has {len(value)} items; display limit is "
                f"{MAX_DECISION_DIFF_COLLECTION_ITEMS}"
            )
        }
    if isinstance(value, dict) and len(value) > MAX_DECISION_DIFF_COLLECTION_ITEMS:
        return {
            "_display_omitted": (
                f"{label} has {len(value)} keys; display limit is "
                f"{MAX_DECISION_DIFF_COLLECTION_ITEMS}"
            )
        }
    try:
        rendered = json.dumps(value, ensure_ascii=False, sort_keys=True)
    except (TypeError, ValueError, RecursionError) as exc:
        return {"_display_omitted": f"{label} is not safely renderable: {exc}"}
    rendered_bytes = len(rendered.encode("utf-8"))
    if rendered_bytes > max_bytes:
        return {
            "_display_omitted": (
                f"{label} is {rendered_bytes} bytes; display limit is {max_bytes} bytes"
            )
        }
    return value


def _bounded_decision_body_diff(
    before: str,
    after: str,
    *,
    fromfile: str,
    tofile: str,
) -> str:
    before_bytes = len(before.encode("utf-8"))
    after_bytes = len(after.encode("utf-8"))
    if max(before_bytes, after_bytes) > MAX_DECISION_DIFF_BODY_INPUT_BYTES:
        return (
            "Body diff omitted: the larger revision is "
            f"{max(before_bytes, after_bytes)} bytes; display input limit is "
            f"{MAX_DECISION_DIFF_BODY_INPUT_BYTES} bytes. Compare the labeled body hashes.\n"
        )
    if before == after:
        return "No canonical body changes.\n"
    before_lines = _decision_body_diff_lines(before)
    after_lines = _decision_body_diff_lines(after)
    if max(len(before_lines), len(after_lines)) > MAX_DECISION_DIFF_BODY_LINES:
        return (
            "Body diff omitted: the larger revision has "
            f"{max(len(before_lines), len(after_lines))} lines; display limit is "
            f"{MAX_DECISION_DIFF_BODY_LINES} lines. Compare the labeled body hashes.\n"
        )
    return _bounded_unified_diff(
        before_lines,
        after_lines,
        fromfile=fromfile,
        tofile=tofile,
        max_output_bytes=MAX_DECISION_DIFF_BODY_OUTPUT_BYTES,
        unchanged_message="No canonical body changes.",
    )


def _decision_body_diff_lines(value: str) -> list[str]:
    """Render logical lines without losing their exact CR/LF termination."""

    lines: list[str] = []
    start = 0
    index = 0
    while index < len(value):
        character = value[index]
        if character == "\r":
            if index + 1 < len(value) and value[index + 1] == "\n":
                lines.append(value[start:index] + r"\r\n")
                index += 2
            else:
                lines.append(value[start:index] + r"\r")
                index += 1
            start = index
            continue
        if character == "\n":
            lines.append(value[start:index] + r"\n")
            index += 1
            start = index
            continue
        index += 1
    if start < len(value):
        lines.append(value[start:] + " [no line ending]")
    return lines


def _bounded_unified_diff(
    before_lines: list[str],
    after_lines: list[str],
    *,
    fromfile: str,
    tofile: str,
    max_output_bytes: int,
    unchanged_message: str,
) -> str:
    if before_lines == after_lines:
        return unchanged_message + "\n"
    parts: list[str] = []
    output_bytes = 0
    truncated = False
    for line in difflib.unified_diff(
        before_lines,
        after_lines,
        fromfile=fromfile,
        tofile=tofile,
        lineterm="",
    ):
        rendered = line + "\n"
        rendered_bytes = len(rendered.encode("utf-8"))
        if output_bytes + rendered_bytes > max_output_bytes:
            truncated = True
            break
        parts.append(rendered)
        output_bytes += rendered_bytes
    if truncated:
        marker = f"@@ diff output truncated; display limit is {max_output_bytes} bytes @@\n"
        marker_bytes = len(marker.encode("utf-8"))
        if marker_bytes > max_output_bytes:
            marker = "@@ diff truncated @@\n"
            marker_bytes = len(marker.encode("utf-8"))
        while parts and output_bytes + marker_bytes > max_output_bytes:
            removed = parts.pop()
            output_bytes -= len(removed.encode("utf-8"))
        if marker_bytes <= max_output_bytes:
            parts.append(marker)
    return "".join(parts)


def _append_meta_section(lines: list[str], title: str, value: Any) -> None:
    text = _meta_section_text(value)
    if text:
        lines.extend(["", f"## {title}", text])


def _meta_section_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        rendered_items = []
        for item in value:
            text = _meta_item_text(item)
            if text:
                rendered_items.append(f"- {_indent_subsequent_lines(text)}")
        return "\n".join(rendered_items)
    if isinstance(value, dict):
        if not value:
            return ""
        return json.dumps(value, indent=2, sort_keys=True)
    return str(value).strip()


def _meta_item_text(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, indent=2, sort_keys=True)
    return str(value).strip()


def _indent_subsequent_lines(text: str) -> str:
    return "\n  ".join(text.splitlines())


def workbench_bookmark_path(config: AgentMeshConfig) -> Path:
    return config.agent_dir / "workbench.html"


def managed_workbench_bookmark_path(config_home: Path | None = None) -> Path:
    """Return the stable machine-local bookmark for the per-user service."""

    root = (config_home or registry_dir()).expanduser().absolute()
    return root / "workbench.html"


def workbench_start_command(config: AgentMeshConfig, *, host: str, port: int) -> str:
    return (
        f"agent-mesh workbench --repo {shlex.quote(str(config.project_root))} "
        f"--host {shlex.quote(host)} --port {port}"
    )


def workbench_service_restart_command(
    *,
    python_executable: Path | None = None,
    platform_name: str | None = None,
) -> str:
    """Return a PATH-independent command for restarting the managed service."""

    command = [
        str(python_executable or Path(sys.executable)),
        "-m",
        "agent_mesh.cli.mail",
        "workbench",
        "service",
        "restart",
    ]
    if (platform_name or sys.platform) == "win32":
        # The Workbench documents this as a PowerShell command. Single-quoted
        # arguments keep %, !, spaces, and trailing backslashes literal; doubled
        # apostrophes are PowerShell's single-quote escape.
        quoted = ["'" + item.replace("'", "''") + "'" for item in command]
        return "& " + " ".join(quoted)
    return shlex.join(command)


def workbench_launch_url(context: WorkbenchContext) -> str:
    """Return an HTTP launch URL without sending the token to the server."""
    return f"{context.server_url}/#token={quote(context.access_token, safe='')}"


def workbench_console_url(context: WorkbenchContext) -> str:
    """Return a log-safe URL; credentials live only in the private bookmark."""

    return context.server_url


def _workbench_server_identity(
    context: WorkbenchContext,
    *,
    anchor_repository: Path,
) -> dict[str, str | int]:
    from agent_mesh.workbench_service import installed_package_identity

    parsed = urlparse(context.server_url)
    package_root, source_checkout = installed_package_identity()
    identity: dict[str, str | int] = {
        "mode": "managed" if context.managed_service else "manual",
        "package_root": str(package_root),
        "anchor_repository": str(anchor_repository.resolve()),
        "endpoint": context.server_url,
        "port": parsed.port or 0,
        "ownership_generation": context.ownership_generation,
        "ownership_revision": context.ownership_revision,
    }
    if source_checkout is not None:
        identity["source_checkout"] = str(source_checkout)
    return identity


def _validate_workbench_host(host: str) -> None:
    normalized = host.strip().casefold()
    if normalized == "localhost":
        return
    try:
        address = ipaddress.ip_address(normalized)
    except ValueError as exc:
        raise WorkbenchError(
            "Workbench is loopback-only; use --host 127.0.0.1 or localhost"
        ) from exc
    if not address.is_loopback:
        raise WorkbenchError("Workbench is loopback-only; use --host 127.0.0.1 or localhost")


def write_bookmark_file(config: AgentMeshConfig, context: WorkbenchContext) -> Path:
    payload = render_workbench_html(
        api_base=context.server_url,
        start_command=context.start_command,
        bookmark_path=context.bookmark_path,
        default_repo_id=context.default_repo_id or project_id(config.project_root),
        access_token=context.access_token,
        managed_service=context.managed_service,
    )
    _write_private_html(context.bookmark_path, payload)
    return context.bookmark_path


def write_managed_bookmark_pointer(
    config: AgentMeshConfig,
    managed_bookmark_path: Path,
    *,
    _validated: bool = False,
) -> Path:
    """Replace a project-local bookmark with a token-free managed-service pointer."""

    from agent_mesh.workbench_service import _managed_bookmark_target

    target = managed_bookmark_path.expanduser().absolute()
    if not _validated and _managed_bookmark_target(target) is None:
        raise WorkbenchError(
            "managed Workbench bookmark is unavailable or invalid; project pointer was unchanged"
        )
    target_url = target.as_uri()
    payload = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Open Agent Mesh Workbench</title>
</head>
<body data-agent-mesh-managed-bookmark-pointer="true">
  <main style="max-width: 680px; margin: 4rem auto; padding: 0 1.5rem; font: 16px/1.5 system-ui, sans-serif;">
    <h1>Agent Mesh Workbench moved</h1>
    <p>This project-local file is the manual fallback, not the automatic Workbench.</p>
    <p><a id="managed-workbench-link" href="{escape(target_url)}">Open the managed Workbench</a></p>
    <p>The stable private bookmark is <code>{escape(str(target))}</code>.</p>
    <p>If it does not open, run <code>agent-mesh workbench service open</code>.</p>
  </main>
</body>
</html>
"""
    pointer_path = workbench_bookmark_path(config)
    _write_private_html(pointer_path, payload)
    return pointer_path


def _write_private_html(path: Path, payload: str) -> None:
    from agent_mesh.workbench_service import _atomic_write

    _atomic_write(path, payload.encode("utf-8"), mode=0o600)


def render_server_workbench_html(context: WorkbenchContext) -> str:
    """Render the HTTP page without embedding the bearer token in its body."""
    return render_workbench_html(
        api_base="",
        start_command=context.start_command,
        bookmark_path=context.bookmark_path,
        default_repo_id=context.default_repo_id,
        access_token="",
        managed_service=context.managed_service,
    )


def _request_host_allowed(value: str, context: WorkbenchContext) -> bool:
    return value.strip().casefold() == urlparse(context.server_url).netloc.casefold()


def _request_origin_allowed(value: str, context: WorkbenchContext) -> bool:
    origin = value.strip()
    return not origin or origin in {"null", context.server_url}


def _request_header_values(headers: Any, name: str) -> list[str]:
    """Return every value so security-sensitive routes can reject duplicates."""
    get_all = getattr(headers, "get_all", None)
    if callable(get_all):
        return [str(value) for value in (get_all(name) or [])]
    return [
        str(value)
        for key, value in getattr(headers, "items", lambda: [])()
        if str(key).casefold() == name.casefold()
    ]


def render_workbench_html(
    *,
    api_base: str = "",
    start_command: str = "",
    bookmark_path: Path | None = None,
    default_repo_id: str = "",
    access_token: str = "",
    managed_service: bool = False,
) -> str:
    bookmark_url = bookmark_path.resolve().as_uri() if bookmark_path else ""
    decision_tier_options = "\n".join(
        f'<option value="{escape(tier)}">{escape(tier.replace("_", " ").title())}</option>'
        for tier in DECISION_TIERS
    )
    workflow_origin_options = "\n".join(
        [
            '<option value="">Any / unspecified</option>',
            *(
                f'<option value="{escape(origin)}">'
                f"{escape(origin.replace('-', ' ').title())}</option>"
                for origin in WORKFLOW_ORIGINS
            ),
        ]
    )
    return (
        WORKBENCH_HTML.replace("__AGENT_MESH_BOOKMARK_URL_JSON__", json.dumps(bookmark_url))
        .replace("__AGENT_MESH_API_BASE__", json.dumps(api_base))
        .replace("__AGENT_MESH_START_COMMAND__", escape(start_command))
        .replace("__AGENT_MESH_BOOKMARK_URL__", escape(bookmark_url))
        .replace("__AGENT_MESH_BOOKMARK_PATH__", escape(str(bookmark_path or "")))
        .replace("__AGENT_MESH_MANAGED_BOOKMARK_URL__", "")
        .replace(
            "__AGENT_MESH_MANAGED_BOOKMARK_PATH__",
            "Unavailable until current ownership provides a valid private bookmark",
        )
        .replace("__AGENT_MESH_DEFAULT_REPO_ID__", json.dumps(default_repo_id))
        .replace("__AGENT_MESH_ACCESS_TOKEN__", json.dumps(access_token))
        .replace("__AGENT_MESH_MANAGED_SERVICE__", json.dumps(managed_service))
        .replace("__AGENT_MESH_DECISION_TIER_OPTIONS__", decision_tier_options)
        .replace("__AGENT_MESH_WORKFLOW_ORIGIN_OPTIONS__", workflow_origin_options)
        .replace("__AGENT_MESH_MAX_ATTACHMENT_BYTES__", str(MAX_ATTACHMENT_BYTES))
        .replace(
            "__AGENT_MESH_MAX_ATTACHMENT_TOTAL_BYTES__",
            str(MAX_ATTACHMENT_TOTAL_BYTES),
        )
    )


def _load_and_rebuild(repo: Path) -> AgentMeshConfig:
    config = load_config(repo)
    lock_handle = acquire(config.agent_dir / ".mail-lock")
    try:
        _rebuild_if_stale(config)
    finally:
        lock_handle.release()
    return config


def _rebuild_if_stale(config: AgentMeshConfig) -> None:
    if not projection_is_current(config):
        rebuild_all(config)


def _handler_for(
    repo: Path,
    context: WorkbenchContext,
    *,
    _startup_code_fingerprint: str | None = None,
    _code_fingerprint_provider: Callable[[], str] | None = None,
    _restart_coordinator: WorkbenchRestartCoordinator | None = None,
    _serve_loop_signal: Callable[[], None] | None = None,
    _managed_authority_provider: Callable[[], Any] | None = None,
) -> type[BaseHTTPRequestHandler]:
    default_repo = repo.resolve()
    fingerprint_provider = _code_fingerprint_provider or _workbench_code_fingerprint
    startup_code_fingerprint = _startup_code_fingerprint or fingerprint_provider()
    fingerprint_lock = threading.Lock()
    stale_code = threading.Event()
    restart_coordinator = _restart_coordinator or WorkbenchRestartCoordinator()
    server_identity = _workbench_server_identity(context, anchor_repository=default_repo)
    from agent_mesh.workbench_service import (
        WorkbenchOwnershipBinding,
        acquire_workbench_request_lease,
        managed_workbench_authority,
    )

    ownership_binding = (
        WorkbenchOwnershipBinding(
            server_mode="managed" if context.managed_service else "manual",
            generation=context.ownership_generation,
            ownership_revision=context.ownership_revision,
        )
        if context.ownership_generation
        else None
    )

    if _managed_authority_provider is None:

        def authority_provider() -> Any:
            return managed_workbench_authority(
                authority_root=context.ownership_authority_root,
                include_health=not context.managed_service,
            )
    else:
        authority_provider = _managed_authority_provider

    def current_authority() -> Any:
        return authority_provider()

    def manual_superseded_payload(authority: Any) -> dict[str, Any]:
        verified = bool(authority.verified)
        state = str(authority.state)
        detail = (
            "The authenticated managed Workbench owns this OS-user control plane. Open its "
            "private bookmark; use the direct-human service relinquish action for manual access."
            if verified
            else "Managed ownership is configured but its endpoint is unavailable. Run "
            "'agent-mesh workbench service status', repair or restart it, or use the "
            "direct-human service relinquish action. Health failure does not restore manual access."
        )
        return {
            "ok": False,
            "code": WORKBENCH_MANUAL_SUPERSEDED,
            "error": "Manual Workbench live access is unavailable while managed authority exists.",
            "detail": detail,
            "server": "manual-superseded",
            "managed_service": False,
            "writes_allowed": False,
            "identity": server_identity,
            "managed_authority": authority.as_public_dict(),
            "authority_state": state,
        }

    def ownership_unavailable_payload(authority: Any) -> dict[str, Any]:
        return {
            "ok": False,
            "code": "WORKBENCH_OWNERSHIP_UNAVAILABLE",
            "error": "Workbench project access is unavailable until ownership is repaired.",
            "detail": (
                "Run 'agent-mesh workbench service status' and complete the exact install, "
                "repair, or relinquish recovery action."
            ),
            "managed_service": context.managed_service,
            "writes_allowed": False,
            "identity": server_identity,
            "managed_authority": authority.as_public_dict(),
            "authority_state": str(authority.state),
        }

    def code_freshness() -> str:
        if stale_code.is_set():
            return "stale"
        with fingerprint_lock:
            if stale_code.is_set():
                return "stale"
            try:
                fingerprint = fingerprint_provider()
            except (OSError, WorkbenchCodeError):
                return "unavailable"
            if fingerprint != startup_code_fingerprint:
                stale_code.set()
                return "stale"
            return "current"

    def code_is_current() -> bool:
        return code_freshness() == "current"

    def stale_code_payload() -> dict[str, Any]:
        return {
            "ok": False,
            "code": WORKBENCH_RESTART_REQUIRED,
            "error": (
                "Agent Mesh code changed after this Workbench process started; "
                "restart Workbench before writing canonical state."
            ),
            "restart_command": context.start_command,
            "identity": server_identity,
        }

    class Handler(BaseHTTPRequestHandler):
        _ownership_lease: Any = None

        def _begin_ownership_lease(self, operation: str) -> Any:
            from agent_mesh.workbench_service import (
                WorkbenchOwnershipBusy,
                WorkbenchOwnershipError,
            )

            if ownership_binding is None:
                authority = current_authority()
                self._json(
                    HTTPStatus.CONFLICT,
                    (
                        manual_superseded_payload(authority)
                        if not context.managed_service and not authority.manual_allowed
                        else ownership_unavailable_payload(authority)
                    ),
                )
                return None
            try:
                lease = acquire_workbench_request_lease(
                    ownership_binding,
                    operation=operation,
                    authority_root=context.ownership_authority_root,
                )
            except WorkbenchOwnershipBusy as exc:
                self._json(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {
                        "ok": False,
                        "code": "WORKBENCH_OWNERSHIP_BUSY",
                        "error": str(exc),
                    },
                )
                return None
            except WorkbenchOwnershipError:
                authority = current_authority()
                self._json(
                    HTTPStatus.CONFLICT,
                    (
                        manual_superseded_payload(authority)
                        if not context.managed_service and not authority.manual_allowed
                        else ownership_unavailable_payload(authority)
                    ),
                )
                return None
            self._ownership_lease = lease
            return lease

        def _release_ownership_lease(self) -> None:
            lease = self._ownership_lease
            self._ownership_lease = None
            if lease is not None:
                lease.release()

        def _owned_subprocess_result(
            self,
            *,
            repo: Path,
            operation: str,
            ttl_seconds: int,
            invoke: Callable[[str], dict[str, Any]],
            dispatch_run_id: str = "",
        ) -> dict[str, Any] | None:
            from agent_mesh.workbench_service import begin_workbench_invocation

            lease = self._ownership_lease
            if lease is None:
                raise WorkbenchError("Workbench subprocess ownership admission is missing")
            config = load_config(repo)
            invocation = begin_workbench_invocation(
                lease,
                repo_store_id=config.store_id,
                repo_path=config.project_root,
                operation=operation,
                ttl_seconds=ttl_seconds,
            )
            try:
                if dispatch_run_id:
                    invocation.bind_dispatch_run(lease.binding, dispatch_run_id)
            except Exception:
                invocation.release(child_completed=False)
                self._release_ownership_lease()
                raise
            self._release_ownership_lease()
            result: dict[str, Any] | None = None
            error: Exception | None = None
            try:
                result = invoke(invocation.envelope)
            except Exception as exc:  # preserve typed route handling after readmission
                error = exc
            finally:
                invocation.release(child_completed=error is None)
            if self._begin_ownership_lease(f"response {operation}") is None:
                return None
            if error is not None:
                raise error
            return result

        def _allowed_origin(self) -> str:
            origin = self.headers.get("Origin", "").strip()
            if origin and _request_origin_allowed(origin, context):
                return origin
            return ""

        def _request_headers_allowed(self) -> bool:
            if not _request_host_allowed(self.headers.get("Host", ""), context):
                self._json(
                    HTTPStatus.FORBIDDEN,
                    {"ok": False, "error": "Invalid Workbench Host header"},
                )
                return False
            if not _request_origin_allowed(self.headers.get("Origin", ""), context):
                self._json(
                    HTTPStatus.FORBIDDEN,
                    {"ok": False, "error": "Workbench origin is not allowed"},
                )
                return False
            return True

        def _send(self, status: HTTPStatus, body: bytes, content_type: str) -> None:
            lease = self._ownership_lease
            if lease is not None and not lease.is_current():
                status = HTTPStatus.CONFLICT
                body = json.dumps(
                    {
                        "ok": False,
                        "code": "WORKBENCH_OWNERSHIP_CHANGED",
                        "error": (
                            "Workbench ownership changed before the response completed; "
                            "no stale project result is available"
                        ),
                    },
                    indent=2,
                    sort_keys=True,
                ).encode("utf-8")
                content_type = "application/json; charset=utf-8"
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            allowed_origin = self._allowed_origin()
            if allowed_origin:
                self.send_header("Access-Control-Allow-Origin", allowed_origin)
                self.send_header("Vary", "Origin")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header(
                "Access-Control-Allow-Headers",
                "Content-Type, X-Agent-Mesh-Token",
            )
            self.end_headers()
            self.wfile.write(body)
            self.wfile.flush()

        def _json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
            self._send(
                status,
                json.dumps(payload, indent=2, sort_keys=True).encode("utf-8"),
                "application/json; charset=utf-8",
            )

        def _decision_operation_json(self, operation: Callable[[], dict[str, Any]]) -> None:
            with _decision_detail_request_slot() as admitted:
                if not admitted:
                    self._json(
                        HTTPStatus.SERVICE_UNAVAILABLE,
                        _decision_detail_busy_payload(),
                    )
                    return
                self._json(HTTPStatus.OK, operation())

        def do_OPTIONS(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path == WORKBENCH_RESTART_PATH:
                if parsed.query or not self._strict_restart_origin_and_host_allowed():
                    self._restart_rejected()
                    return
                self.send_response(HTTPStatus.NO_CONTENT)
                allowed_origin = self._allowed_origin()
                if allowed_origin:
                    self.send_header("Access-Control-Allow-Origin", allowed_origin)
                    self.send_header("Vary", "Origin")
                self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
                self.send_header("Access-Control-Allow-Headers", "X-Agent-Mesh-Token")
                self.send_header("Content-Length", "0")
                self.end_headers()
                self.wfile.flush()
                return
            if not self._request_headers_allowed():
                return
            self.send_response(HTTPStatus.NO_CONTENT)
            allowed_origin = self._allowed_origin()
            if allowed_origin:
                self.send_header("Access-Control-Allow-Origin", allowed_origin)
                self.send_header("Vary", "Origin")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header(
                "Access-Control-Allow-Headers",
                "Content-Type, X-Agent-Mesh-Token",
            )
            self.end_headers()
            self.wfile.flush()

        def _authorized(self) -> bool:
            if not context.access_token:
                return True
            provided = self.headers.get("X-Agent-Mesh-Token", "")
            if secrets.compare_digest(provided, context.access_token):
                return True
            self._json(
                HTTPStatus.FORBIDDEN,
                {"ok": False, "error": "Missing or invalid Workbench access token"},
            )
            return False

        def _strict_restart_origin_and_host_allowed(self) -> bool:
            hosts = _request_header_values(self.headers, "Host")
            origins = _request_header_values(self.headers, "Origin")
            return (
                len(hosts) == 1
                and _request_host_allowed(hosts[0], context)
                and len(origins) == 1
                and bool(origins[0].strip())
                and _request_origin_allowed(origins[0], context)
            )

        def _restart_rejected(self, status: HTTPStatus = HTTPStatus.FORBIDDEN) -> None:
            self._json(
                status,
                {
                    "ok": False,
                    "code": WORKBENCH_RESTART_REJECTED,
                    "error": "Restart request rejected",
                },
            )

        def _handle_restart_post(self, parsed: Any) -> None:
            if not self._strict_restart_origin_and_host_allowed():
                self._restart_rejected()
                return
            tokens = _request_header_values(self.headers, "X-Agent-Mesh-Token")
            if (
                len(tokens) != 1
                or not context.access_token
                or not secrets.compare_digest(tokens[0], context.access_token)
            ):
                self._restart_rejected()
                return
            if not context.managed_service:
                self._restart_rejected()
                return
            content_lengths = _request_header_values(self.headers, "Content-Length")
            if (
                parsed.query
                or _request_header_values(self.headers, "Transfer-Encoding")
                or len(content_lengths) > 1
                or (content_lengths and content_lengths[0] != "0")
            ):
                self._restart_rejected(HTTPStatus.BAD_REQUEST)
                return
            freshness = code_freshness()
            if freshness == "current":
                self._json(
                    HTTPStatus.CONFLICT,
                    {
                        "ok": False,
                        "code": WORKBENCH_RESTART_NOT_REQUIRED,
                        "error": "Workbench restart is not required",
                    },
                )
                return
            if freshness != "stale":
                self._json(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {
                        "ok": False,
                        "code": WORKBENCH_RESTART_UNVERIFIED,
                        "error": "Workbench restart eligibility could not be verified",
                    },
                )
                return
            result = restart_coordinator.request_restart()
            if result == WORKBENCH_RESTART_DRAIN_TIMEOUT:
                self._json(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {
                        "ok": False,
                        "code": WORKBENCH_RESTART_DRAIN_TIMEOUT,
                        "error": "Workbench restart drain timed out",
                    },
                )
                return
            self._json(
                HTTPStatus.ACCEPTED,
                {
                    "ok": True,
                    "code": WORKBENCH_RESTART_ACCEPTED,
                    "message": "Workbench restart accepted",
                },
            )
            if restart_coordinator.claim_serve_loop_signal():
                if _serve_loop_signal is not None:
                    _serve_loop_signal()
                else:
                    self.server.shutdown()

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            try:
                if not self._request_headers_allowed():
                    return
                if parsed.path == "/":
                    self._send(
                        HTTPStatus.OK,
                        render_server_workbench_html(context).encode("utf-8"),
                        "text/html; charset=utf-8",
                    )
                    return
                if parsed.path.startswith("/api/") and not self._authorized():
                    return
                if parsed.path.startswith("/api/") and not code_is_current():
                    self._json(
                        HTTPStatus.SERVICE_UNAVAILABLE,
                        {
                            **stale_code_payload(),
                            "server": "restart-required",
                            "managed_service": context.managed_service,
                        },
                    )
                    return
                if parsed.path == "/api/health":
                    authority = current_authority()
                    if not context.managed_service and not authority.manual_allowed:
                        payload = manual_superseded_payload(authority)
                        payload["ok"] = True
                        self._json(HTTPStatus.OK, payload)
                        return
                    if (
                        ownership_binding is None
                        or not authority.generation
                        or authority.generation != ownership_binding.generation
                        or authority.ownership_revision != ownership_binding.ownership_revision
                        or (context.managed_service and authority.mode != "configured")
                    ):
                        self._json(
                            HTTPStatus.SERVICE_UNAVAILABLE,
                            ownership_unavailable_payload(authority),
                        )
                        return
                    identity = dict(server_identity)
                    identity["ownership_generation"] = authority.generation
                    identity["ownership_revision"] = authority.ownership_revision
                    self._json(
                        HTTPStatus.OK,
                        {
                            "ok": True,
                            "server": "online",
                            "managed_service": context.managed_service,
                            "writes_allowed": True,
                            "identity": identity,
                        },
                    )
                    return
                authority = current_authority()
                if not context.managed_service and not authority.manual_allowed:
                    self._json(HTTPStatus.CONFLICT, manual_superseded_payload(authority))
                    return
                if parsed.path not in WORKBENCH_OWNERSHIP_GET_ROUTES:
                    self._json(
                        HTTPStatus.NOT_FOUND,
                        {"ok": False, "error": f"Unknown path: {parsed.path}"},
                    )
                    return
                if self._begin_ownership_lease(f"GET {parsed.path}") is None:
                    return
                if parsed.path == "/api/projects":
                    projects = list_registered_projects()
                    self._json(
                        HTTPStatus.OK,
                        {
                            "ok": True,
                            "default_repo_id": context.default_repo_id,
                            "projects": [project.as_dict() for project in projects],
                        },
                    )
                    return
                selected_repo = _registered_repo_from_request(parsed, default_repo)
                if parsed.path == "/api/snapshot":
                    params = parse_qs(parsed.query)
                    if context.managed_service:
                        _ensure_decision_read_model_warm(selected_repo)
                    self._json(
                        HTTPStatus.OK,
                        workbench_snapshot(
                            selected_repo,
                            message_status=params.get("message_status", [""])[0].strip(),
                            message_kind=params.get("message_kind", [""])[0].strip(),
                            message_feature=params.get("message_feature", [""])[0].strip(),
                            message_workflow_origin=params.get("message_origin", [""])[0].strip(),
                            message_query=params.get("message_q", [""])[0].strip(),
                            backlog_status=params.get("backlog_status", [""])[0].strip(),
                            backlog_lane=params.get("backlog_lane", [""])[0].strip(),
                            backlog_priority=params.get("backlog_priority", [""])[0].strip(),
                            backlog_owner=params.get("backlog_owner", [""])[0].strip(),
                            backlog_item_type=params.get("backlog_type", [""])[0].strip(),
                            backlog_launch_scope=params.get("backlog_scope", [""])[0].strip(),
                            backlog_wave=params.get("backlog_wave", [""])[0].strip(),
                            backlog_workflow_origin=params.get("backlog_origin", [""])[0].strip(),
                            backlog_query=params.get("backlog_q", [""])[0].strip(),
                            backlog_quick_filter=params.get("backlog_filter", [""])[0].strip(),
                            decision_query=params.get("decision_q", [""])[0].strip(),
                            decision_status=params.get("decision_status", [""])[0].strip(),
                            decision_tier=params.get("decision_tier", [""])[0].strip(),
                        ),
                    )
                    return
                if parsed.path == "/api/status":
                    self._json(HTTPStatus.OK, workbench_status(selected_repo))
                    return
                if parsed.path == "/api/feedback/receipt":
                    params = parse_qs(parsed.query)
                    submission_id = params.get("id", [""])[0].strip()
                    if not submission_id:
                        self._json(
                            HTTPStatus.BAD_REQUEST,
                            {"ok": False, "error": "Missing submission id"},
                        )
                        return
                    self._json(
                        HTTPStatus.OK,
                        feedback_submission_receipt(selected_repo, submission_id),
                    )
                    return
                if parsed.path == "/api/message":
                    params = parse_qs(parsed.query)
                    message_id = params.get("id", [""])[0].strip()
                    if not message_id:
                        self._json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "Missing id"})
                        return
                    result = lookup_message(selected_repo, message_id)
                    if result is None:
                        self._json(
                            HTTPStatus.NOT_FOUND,
                            {"ok": False, "error": f"No message found for {message_id}"},
                        )
                        return
                    self._json(HTTPStatus.OK, result)
                    return
                if parsed.path == "/api/messages":
                    params = parse_qs(parsed.query)
                    self._json(
                        HTTPStatus.OK,
                        {
                            "ok": True,
                            "messages": list_messages(
                                selected_repo,
                                status=params.get("status", [""])[0].strip(),
                                kind=params.get("kind", [""])[0].strip(),
                                feature=params.get("feature", [""])[0].strip(),
                                workflow_origin=params.get("origin", [""])[0].strip(),
                                query=params.get("q", [""])[0].strip(),
                            ),
                        },
                    )
                    return
                if parsed.path == "/api/backlog/items":
                    params = parse_qs(parsed.query)
                    self._json(
                        HTTPStatus.OK,
                        {
                            "ok": True,
                            "items": list_backlog_items(
                                selected_repo,
                                status=params.get("status", [""])[0].strip(),
                                lane=params.get("lane", [""])[0].strip(),
                                priority=params.get("priority", [""])[0].strip(),
                                owner=params.get("owner", [""])[0].strip(),
                                item_type=params.get("type", [""])[0].strip(),
                                launch_scope=params.get("scope", [""])[0].strip(),
                                wave=params.get("wave", [""])[0].strip(),
                                workflow_origin=params.get("origin", [""])[0].strip(),
                                query=params.get("q", [""])[0].strip(),
                                quick_filter=params.get("filter", [""])[0].strip(),
                            ),
                        },
                    )
                    return
                if parsed.path == "/api/backlog/item":
                    params = parse_qs(parsed.query)
                    item_id = params.get("id", [""])[0].strip()
                    if not item_id:
                        self._json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "Missing id"})
                        return
                    result = lookup_backlog_item(selected_repo, item_id)
                    if result is None:
                        self._json(
                            HTTPStatus.NOT_FOUND,
                            {"ok": False, "error": f"No backlog item found for {item_id}"},
                        )
                        return
                    self._json(HTTPStatus.OK, result)
                    return
                if parsed.path == "/api/backlog/kanban":
                    self._json(HTTPStatus.OK, backlog_kanban(selected_repo))
                    return
                if parsed.path == "/api/decisions/applicability":
                    params = parse_qs(parsed.query)
                    requested_mode = params.get("mode", ["worktree"])[0].strip() or "worktree"
                    if requested_mode not in {"pr", "staged", "worktree", "full"}:
                        raise GitChangeRequestError(
                            f"unsupported Git decision-check mode: {requested_mode}"
                        )
                    requested_base = params.get("base", [""])[0].strip() or None
                    self._json(
                        HTTPStatus.OK,
                        decision_applicability_for_changes(
                            selected_repo,
                            mode=cast(GitChangeMode, requested_mode),
                            base=requested_base,
                        ),
                    )
                    return
                if parsed.path == "/api/decisions":
                    params = parse_qs(parsed.query)
                    self._json(
                        HTTPStatus.OK,
                        {
                            "ok": True,
                            "next_id": next_decision_human_id(selected_repo),
                            "decisions": list_decisions(
                                selected_repo,
                                query=params.get("q", [""])[0].strip(),
                                status=params.get("status", [""])[0].strip(),
                                tier=params.get("tier", [""])[0].strip(),
                            ),
                        },
                    )
                    return
                if parsed.path == "/api/dispatches":
                    self._json(
                        HTTPStatus.OK,
                        {"ok": True, "dispatches": list_dispatches(selected_repo)},
                    )
                    return
                if parsed.path == "/api/dispatch":
                    params = parse_qs(parsed.query)
                    policy_id = params.get("id", [""])[0].strip()
                    if not policy_id:
                        self._json(
                            HTTPStatus.BAD_REQUEST,
                            {"ok": False, "error": "Missing policy id"},
                        )
                        return
                    result = lookup_dispatch(selected_repo, policy_id)
                    if result is None:
                        self._json(
                            HTTPStatus.NOT_FOUND,
                            {"ok": False, "error": f"No dispatch found for {policy_id}"},
                        )
                        return
                    self._json(HTTPStatus.OK, result)
                    return
                if parsed.path == "/api/decision":
                    params = parse_qs(parsed.query)
                    identifier = params.get("id", [""])[0].strip()
                    if not identifier:
                        self._json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "Missing id"})
                        return
                    with _decision_detail_request_slot() as admitted:
                        if not admitted:
                            self._json(
                                HTTPStatus.SERVICE_UNAVAILABLE,
                                _decision_detail_busy_payload(),
                            )
                            return
                        result = lookup_decision(
                            selected_repo,
                            identifier,
                            include_review_data=False,
                        )
                        if result is None:
                            self._json(
                                HTTPStatus.NOT_FOUND,
                                {"ok": False, "error": f"No decision found for {identifier}"},
                            )
                            return
                        self._json(HTTPStatus.OK, result)
                    return
                if parsed.path == "/api/decision/review":
                    params = parse_qs(parsed.query)
                    identifier = params.get("id", [""])[0].strip()
                    expected_revision_sha = params.get("revision_sha", [""])[0].strip()
                    if not identifier:
                        self._json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "Missing id"})
                        return
                    if not expected_revision_sha:
                        self._json(
                            HTTPStatus.BAD_REQUEST,
                            {"ok": False, "error": "Missing revision_sha"},
                        )
                        return
                    with _decision_detail_request_slot() as admitted:
                        if not admitted:
                            self._json(
                                HTTPStatus.SERVICE_UNAVAILABLE,
                                _decision_detail_busy_payload(),
                            )
                            return
                        result = lookup_decision_review(
                            selected_repo,
                            identifier,
                            expected_revision_sha=expected_revision_sha,
                        )
                        if result is None:
                            self._json(
                                HTTPStatus.NOT_FOUND,
                                {"ok": False, "error": f"No decision found for {identifier}"},
                            )
                            return
                        self._json(HTTPStatus.OK, result)
                    return
                self._json(
                    HTTPStatus.NOT_FOUND, {"ok": False, "error": f"Unknown path: {parsed.path}"}
                )
            except (DecisionPathError, GitChangeRequestError, ProjectRegistryError) as exc:
                self._json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc)})
            except (GitChangeUnavailable, ReadModelUnavailable) as exc:
                if parsed.path == "/api/decisions/applicability":
                    params = parse_qs(parsed.query)
                    requested_mode = params.get("mode", ["worktree"])[0].strip() or "worktree"
                    requested_base = params.get("base", [""])[0].strip() or None
                    if requested_mode in {"pr", "full"} and requested_base is None:
                        requested_base = "main"
                    unavailable_context = build_unavailable_decision_context(
                        mode=requested_mode,
                        base=requested_base,
                        diagnostic=str(exc),
                    )
                    self._json(
                        HTTPStatus.SERVICE_UNAVAILABLE,
                        {
                            "ok": False,
                            "context": unavailable_context,
                            "error": unavailable_context["diagnostics"][0],
                        },
                    )
                else:
                    self._json(
                        HTTPStatus.SERVICE_UNAVAILABLE,
                        {"ok": False, "error": str(exc)},
                    )
            except Exception as exc:  # pragma: no cover - local server safeguard
                self._json(HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": str(exc)})
            finally:
                self._release_ownership_lease()

        def do_POST(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path == WORKBENCH_RESTART_PATH:
                self._handle_restart_post(parsed)
                return
            with restart_coordinator.post_lease() as admitted:
                if not admitted:
                    self._json(
                        HTTPStatus.SERVICE_UNAVAILABLE,
                        {
                            "ok": False,
                            "code": WORKBENCH_RESTARTING,
                            "error": "Workbench restart is in progress",
                        },
                    )
                    return
                if not self._request_headers_allowed():
                    return
                if not self._authorized():
                    return
                if not code_is_current():
                    self._json(HTTPStatus.CONFLICT, stale_code_payload())
                    return
                authority = current_authority()
                if not context.managed_service and not authority.manual_allowed:
                    self._json(HTTPStatus.CONFLICT, manual_superseded_payload(authority))
                    return
                if parsed.path not in WORKBENCH_OWNERSHIP_POST_ROUTES:
                    self._json(
                        HTTPStatus.NOT_FOUND,
                        {"ok": False, "error": f"Unknown path: {parsed.path}"},
                    )
                    return
                if self._begin_ownership_lease(f"POST {parsed.path}") is None:
                    return
                try:
                    self._handle_non_restart_post(parsed)
                finally:
                    self._release_ownership_lease()

        def _handle_non_restart_post(self, parsed: Any) -> None:
            try:
                content_length = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                self._json(
                    HTTPStatus.BAD_REQUEST,
                    {"ok": False, "error": "Invalid Content-Length"},
                )
                return
            if content_length < 0 or content_length > MAX_REQUEST_BYTES:
                self._json(
                    HTTPStatus.REQUEST_ENTITY_TOO_LARGE,
                    {
                        "ok": False,
                        "error": f"Request body exceeds {MAX_REQUEST_BYTES // (1024 * 1024)} MB",
                    },
                )
                return
            raw = self.rfile.read(content_length) if content_length else b"{}"
            try:
                payload = json.loads(raw.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                self._json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": "Invalid JSON body"})
                return
            if not isinstance(payload, dict):
                self._json(
                    HTTPStatus.BAD_REQUEST,
                    {"ok": False, "error": "JSON body must be an object"},
                )
                return
            if not code_is_current():
                self._json(HTTPStatus.CONFLICT, stale_code_payload())
                return
            try:
                selected_repo = _registered_repo_from_request(parsed, default_repo)
                if parsed.path == "/api/feedback/draft":
                    self._json(HTTPStatus.OK, build_feedback_markdown(payload))
                    return
                if parsed.path == "/api/feedback/submit":
                    self._json(HTTPStatus.OK, submit_feedback_request(selected_repo, payload))
                    return
                if parsed.path == "/api/attachments/upload":
                    self._json(
                        HTTPStatus.OK,
                        save_attachment_uploads(selected_repo, payload.get("files", [])),
                    )
                    return
                if parsed.path == "/api/backlog/update":
                    result = update_backlog_item(
                        selected_repo,
                        item_id=_clean(payload.get("id")),
                        status=_optional_clean(payload.get("status")),
                        lane=_optional_clean(payload.get("lane")),
                        priority=_optional_clean(payload.get("priority")),
                        actor=_optional_clean(payload.get("actor")),
                    )
                    self._json(HTTPStatus.OK, result)
                    return
                if parsed.path == "/api/message/status":
                    result = update_request_status(
                        selected_repo,
                        request_id=_clean(payload.get("id")),
                        to_status=_clean(payload.get("status")),
                        reason=_clean(payload.get("reason")),
                        actor=_optional_clean(payload.get("actor")),
                    )
                    self._json(HTTPStatus.OK, result)
                    return
                if parsed.path == "/api/dispatch/run":
                    raw_timeout = payload.get("timeout_seconds", 3600)
                    ttl_seconds = (
                        min(raw_timeout + 120, 21_720)
                        if isinstance(raw_timeout, int) and not isinstance(raw_timeout, bool)
                        else 120
                    )
                    dispatch_result = self._owned_subprocess_result(
                        repo=selected_repo,
                        operation=WORKBENCH_SUBPROCESS_ROUTE_OPERATIONS[parsed.path],
                        ttl_seconds=ttl_seconds,
                        invoke=lambda envelope: run_dispatch_from_workbench(
                            selected_repo,
                            payload,
                            ownership_envelope=envelope,
                        ),
                    )
                    if dispatch_result is None:
                        return
                    self._json(
                        HTTPStatus.OK,
                        dispatch_result,
                    )
                    return
                if parsed.path == "/api/dispatch/assure":
                    assurance_result = self._owned_subprocess_result(
                        repo=selected_repo,
                        operation=WORKBENCH_SUBPROCESS_ROUTE_OPERATIONS[parsed.path],
                        ttl_seconds=120,
                        invoke=lambda envelope: record_dispatch_assurance_from_workbench(
                            selected_repo,
                            payload,
                            ownership_envelope=envelope,
                        ),
                    )
                    if assurance_result is None:
                        return
                    self._json(
                        HTTPStatus.OK,
                        assurance_result,
                    )
                    return
                if parsed.path in {
                    "/api/dispatch/assurance/flag",
                    "/api/dispatch/assurance/retire",
                }:
                    action = "flag" if parsed.path.endswith("/flag") else "retire"
                    lifecycle_result = self._owned_subprocess_result(
                        repo=selected_repo,
                        operation=WORKBENCH_SUBPROCESS_ROUTE_OPERATIONS[parsed.path],
                        ttl_seconds=120,
                        invoke=lambda envelope: transition_dispatch_assurance_from_workbench(
                            selected_repo,
                            payload,
                            action=action,
                            ownership_envelope=envelope,
                        ),
                    )
                    if lifecycle_result is None:
                        return
                    self._json(HTTPStatus.OK, lifecycle_result)
                    return
                if parsed.path == "/api/dispatch/cancel":
                    cancel_run_id, _cancel_actor = _validated_dispatch_cancel_request(payload)
                    cancellation_result = self._owned_subprocess_result(
                        repo=selected_repo,
                        operation=WORKBENCH_SUBPROCESS_ROUTE_OPERATIONS[parsed.path],
                        ttl_seconds=120,
                        dispatch_run_id=cancel_run_id,
                        invoke=lambda envelope: cancel_dispatch_from_workbench(
                            selected_repo,
                            payload,
                            ownership_envelope=envelope,
                        ),
                    )
                    if cancellation_result is None:
                        return
                    self._json(
                        HTTPStatus.OK,
                        cancellation_result,
                    )
                    return
                if parsed.path == "/api/decision/create":
                    self._decision_operation_json(
                        lambda: create_decision(
                            selected_repo,
                            human_id=_clean(payload.get("id")),
                            title=_clean(payload.get("title")),
                            tier=_clean(payload.get("tier")),
                            owner=_clean(payload.get("owner")),
                            context=_clean(payload.get("context")),
                            decision=_clean(payload.get("decision")),
                            body=(
                                str(payload.get("body"))
                                if payload.get("body") is not None
                                else None
                            ),
                            applicability_scope=_optional_clean(payload.get("applicability_scope")),
                            affected_code_globs=_string_list(payload.get("affected_code_globs")),
                            exemptions=_string_list(payload.get("exemptions")),
                            generated_artifact_paths=_string_list(
                                payload.get("generated_artifact_paths")
                            ),
                            required_checks=_string_list(payload.get("required_checks")),
                            verification=_string_list(payload.get("verification_commands")),
                            assumptions=_assumption_list(payload.get("assumptions")),
                            evidence=_evidence_map(payload.get("evidence")),
                            review_policy=_review_policy_from_payload(payload),
                            tags=_string_list(payload.get("tags")),
                            actor=_optional_clean(payload.get("actor")),
                        )
                    )
                    return
                if parsed.path == "/api/decision/update":
                    self._decision_operation_json(
                        lambda: update_decision(
                            selected_repo,
                            _clean(payload.get("id")),
                            title=_optional_clean(payload.get("title")),
                            tier=_optional_clean(payload.get("tier")),
                            owner=_optional_clean(payload.get("owner")),
                            context=_optional_clean(payload.get("context")),
                            decision=_optional_clean(payload.get("decision")),
                            body=(
                                str(payload.get("body"))
                                if payload.get("body") is not None
                                else None
                            ),
                            applicability_scope=(
                                _optional_clean(payload.get("applicability_scope"))
                                if "applicability_scope" in payload
                                else None
                            ),
                            affected_code_globs=(
                                _string_list(payload.get("affected_code_globs"))
                                if "affected_code_globs" in payload
                                else None
                            ),
                            exemptions=(
                                _string_list(payload.get("exemptions"))
                                if "exemptions" in payload
                                else None
                            ),
                            generated_artifact_paths=(
                                _string_list(payload.get("generated_artifact_paths"))
                                if "generated_artifact_paths" in payload
                                else None
                            ),
                            required_checks=(
                                _string_list(payload.get("required_checks"))
                                if "required_checks" in payload
                                else None
                            ),
                            verification=(
                                _string_list(payload.get("verification_commands"))
                                if "verification_commands" in payload
                                else None
                            ),
                            assumptions=(
                                _assumption_list(payload.get("assumptions"))
                                if "assumptions" in payload
                                else None
                            ),
                            evidence=(
                                _evidence_map(payload.get("evidence"))
                                if "evidence" in payload
                                else None
                            ),
                            review_policy=(
                                _review_policy_from_payload(payload)
                                if "required_reviewers" in payload or "approval_quorum" in payload
                                else None
                            ),
                            tags=(_string_list(payload.get("tags")) if "tags" in payload else None),
                            revision_reason=_clean(payload.get("revision_reason")),
                            actor=_optional_clean(payload.get("actor")),
                        )
                    )
                    return
                if parsed.path == "/api/decision/accept":
                    self._decision_operation_json(
                        lambda: accept_decision(
                            selected_repo,
                            _clean(payload.get("id")),
                            expected_body_sha=_clean(payload.get("expected_body_sha")),
                            expected_revision_sha=_clean(payload.get("expected_revision_sha")),
                            notes=_clean(payload.get("notes")),
                            actor=_clean(payload.get("actor")),
                        )
                    )
                    return
                if parsed.path == "/api/decision/reject":
                    self._decision_operation_json(
                        lambda: reject_decision(
                            selected_repo,
                            _clean(payload.get("id")),
                            expected_body_sha=_clean(payload.get("expected_body_sha")),
                            expected_revision_sha=_clean(payload.get("expected_revision_sha")),
                            reason=_clean(payload.get("reason")),
                            actor=_clean(payload.get("actor")),
                        )
                    )
                    return
                self._json(
                    HTTPStatus.NOT_FOUND, {"ok": False, "error": f"Unknown path: {parsed.path}"}
                )
            except WorkbenchBusyError as exc:
                self._json(
                    HTTPStatus.SERVICE_UNAVAILABLE,
                    {"ok": False, "error": str(exc)},
                )
            except (ProjectRegistryError, ValueError, WorkbenchError) as exc:
                self._json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc)})
            except Exception as exc:  # pragma: no cover - local server safeguard
                self._json(HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": str(exc)})

        def log_request(self, code: int | str = "-", size: int | str = "-") -> None:
            if urlparse(self.path).path == WORKBENCH_RESTART_PATH:
                print(
                    f"{self.address_string()} - Workbench restart request status={code} size={size}",
                    file=sys.stderr,
                )
                return
            super().log_request(code, size)

        def log_message(self, format: str, *args: Any) -> None:
            print(f"{self.address_string()} - {format % args}", file=sys.stderr)

    return Handler


def _registered_repo_from_request(parsed: Any, default_repo: Path) -> Path:
    params = parse_qs(parsed.query)
    identifier = params.get("repo", [""])[0].strip()
    if not identifier:
        identifier = project_id(default_repo)
    return resolve_registered_project(identifier).root


def _rel(config: AgentMeshConfig, path: Path) -> str:
    try:
        return str(path.relative_to(config.project_root))
    except ValueError:
        return str(path)


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _optional_clean(value: Any) -> str | None:
    if value is None:
        return None
    return str(value).strip()


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        raw = value
    else:
        raw = str(value).splitlines()
    cleaned: list[str] = []
    for item in raw:
        text = str(item).strip()
        if text.startswith(("- ", "* ")):
            text = text[2:].strip()
        if text:
            cleaned.append(text)
    return cleaned


def _assumption_list(value: Any) -> list[Any]:
    if not isinstance(value, list):
        return _string_list(value)
    assumptions: list[Any] = []
    for item in value:
        if isinstance(item, dict):
            assumptions.append(item)
            continue
        if not isinstance(item, str):
            raise WorkbenchError("Assumptions must be strings or objects")
        text = item.strip()
        if text:
            assumptions.append(text)
    return assumptions


def _evidence_map(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return {str(kind): references for kind, references in value.items()}
    try:
        return parse_decision_evidence_entries(_string_list(value))
    except ValueError as exc:
        raise WorkbenchError(str(exc)) from exc


def _review_policy_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    reviewers = _string_list(payload.get("required_reviewers"))
    raw_quorum = payload.get("approval_quorum")
    if not reviewers and raw_quorum in (None, ""):
        return {}
    if raw_quorum in (None, ""):
        quorum: int | None = None
    else:
        try:
            quorum = int(str(raw_quorum).strip())
        except ValueError as exc:
            raise WorkbenchError("Approval quorum must be an integer") from exc
    return {"required_reviewers": reviewers, "approval_quorum": quorum}


def _feedback_refs(payload: dict[str, Any]) -> list[Any]:
    refs: list[str] = []
    related_id = _clean(payload.get("related_id"))
    if related_id:
        refs.append(related_id)
    refs.extend(_string_list(payload.get("refs")))
    refs.extend(_string_list(payload.get("screenshots")))
    seen: set[str] = set()
    unique: list[str] = []
    for ref in refs:
        if ref not in seen:
            seen.add(ref)
            unique.append(ref)
    return unique


def _sanitize_filename(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "-", Path(name).name).strip("-.")
    return cleaned or "upload.bin"


def _backlog_payload_from_row(row) -> dict[str, Any]:
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
    return {key: value for key, value in payload.items() if value is not None}


def _public_backlog_owner(conn, row) -> str:
    return public_instance_handle_for_id(conn, str(row["owner_instance_id"] or "")) or str(
        row["owner_hint"] or ""
    )


def _backlog_item_from_row(conn, row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "title": row["title"],
        "item_type": row["item_type"],
        "summary": row["summary"],
        "status": row["status"],
        "priority": row["priority"],
        "lane": row["lane"] or "unassigned",
        "launch_scope": row["launch_scope"],
        "release_phase": row["release_phase"],
        "wave": row["release_phase"] or "",
        "owner_hint": _public_backlog_owner(conn, row),
        "workflow_origin": row["workflow_origin"] or "",
        "workflow_origin_valid": bool(row["workflow_origin_valid"]),
        "workflow_origin_source": row["workflow_origin_source"],
        "updated_utc": row["updated_utc"],
        "refs": json_loads(row["refs_json"], []),
    }


def _backlog_detail_from_row(conn, row) -> dict[str, Any]:
    links = conn.execute(
        "SELECT ref_type, ref_value FROM backlog_item_links WHERE item_id=? ORDER BY ref_type, ref_value",
        (row["id"],),
    ).fetchall()
    return {
        "id": row["id"],
        "title": row["title"],
        "item_type": row["item_type"],
        "summary": row["summary"] or "",
        "root_cause_summary": row["root_cause_summary"] or "",
        "architectural_category": row["architectural_category"] or "",
        "status": row["status"],
        "priority": row["priority"] or "",
        "launch_scope": row["launch_scope"] or "",
        "release_phase": row["release_phase"] or "",
        "production_state": row["production_state"] or "",
        "disposition": row["disposition"] or "",
        "owner_hint": _public_backlog_owner(conn, row),
        "lane": row["lane"] or "unassigned",
        "notes": row["notes"] or "",
        "workflow_origin": row["workflow_origin"] or "",
        "workflow_origin_valid": bool(row["workflow_origin_valid"]),
        "workflow_origin_source": row["workflow_origin_source"],
        "refs": json_loads(row["refs_json"], []),
        "links": [{"type": link["ref_type"], "value": link["ref_value"]} for link in links],
        "created_utc": row["created_utc"],
        "updated_utc": row["updated_utc"],
        "event_seq": row["event_seq"],
    }


def backlog_detail_block(item: dict[str, Any]) -> str:
    lines = [
        f"## {item.get('id', '')}",
        f"- status: {item.get('status', '')}",
        f"- lane: {item.get('lane', '')}",
        f"- priority: {item.get('priority', '')}",
        f"- type: {item.get('item_type', '')}",
        f"- owner: {item.get('owner_hint', '')}",
        f"- workflow_origin: {item.get('workflow_origin', '')}",
        f"- workflow_origin_source: {item.get('workflow_origin_source', '')}",
        f"- scope: {item.get('launch_scope', '')}",
        f"- wave: {item.get('release_phase') or item.get('wave') or ''}",
        f"- updated_utc: {item.get('updated_utc', '')}",
        "",
        str(item.get("title") or ""),
    ]
    sections = [
        ("Summary", item.get("summary")),
        ("Root Cause Summary", item.get("root_cause_summary")),
        ("Notes", item.get("notes")),
    ]
    for title, value in sections:
        text = str(value or "").strip()
        if text:
            lines.extend(["", f"## {title}", text])
    refs = item.get("refs") or []
    if refs:
        lines.extend(["", "## Refs"])
        for ref in refs:
            if isinstance(ref, dict):
                lines.append(f"- {ref.get('type', 'unknown')}:{ref.get('value', '')}")
            else:
                lines.append(f"- {ref}")
    links = item.get("links") or []
    if links:
        lines.extend(["", "## Links"])
        lines.extend(f"- {link.get('type', 'unknown')}:{link.get('value', '')}" for link in links)
    referral_events = [
        event
        for event in item.get("events") or []
        if str(event.get("event_type") or "").startswith("backlog_referral_")
    ]
    if referral_events:
        latest = referral_events[-1]
        details = latest.get("details") if isinstance(latest.get("details"), dict) else {}
        lines.extend(
            [
                "",
                "## Cross-repository referral",
                f"- status: {str(latest.get('event_type') or '').removeprefix('backlog_referral_')}",
                f"- referral_id: {details.get('referral_id', '')}",
                f"- target_project: {details.get('target_project', '')}",
            ]
        )
        if details.get("target_backlog"):
            lines.append(f"- target_backlog: {details['target_backlog']}")
        reason_code = str(details.get("reason_code") or "")
        if reason_code == "target_write_scope_absent":
            lines.append(
                "- requested_action: grant an agent write scope for the target repo, then rerun "
                "the referral with --apply"
            )
        elif reason_code == "duplicate_review_required":
            candidates = ", ".join(str(value) for value in details.get("duplicate_candidates", []))
            lines.append(
                "- requested_action: review possible target duplicates "
                f"({candidates}), then rerun with --use-existing TARGET-BKL-ID --apply"
            )
        elif reason_code == "target_write_failed":
            lines.append(
                "- requested_action: repair target write scope, then rerun the same referral "
                "with --apply"
            )
    return "\n".join(lines).rstrip() + "\n"


def _message_item_from_row(conn, row) -> dict[str, Any]:
    return {
        "id": row["id"],
        "kind": row["kind"],
        "thread_id": row["thread_id"],
        "request_id": row["request_id"] or "",
        "sender": public_message_sender(
            conn, str(row["sender"]), str(row["sender_instance_id"] or "")
        ),
        "recipients": public_message_recipients(
            conn,
            json_loads(row["recipients_json"], []),
            json_loads(row["recipient_instance_ids_json"], []),
        ),
        "feature": row["feature_id"] or "",
        "workflow_origin": row["workflow_origin"] or "",
        "workflow_origin_valid": bool(row["workflow_origin_valid"]),
        "workflow_origin_source": row["workflow_origin_source"],
        "title": row["title"] or row["summary"] or "",
        "status": row["status"],
        "resolution": row["resolution"] or "",
        "created_utc": row["created_utc"],
        "updated_utc": row["updated_utc"],
        "resolved_utc": row["resolved_utc"] or "",
        "event_seq": row["event_seq"],
    }


def _decision_item_from_row(
    row,
    collections: dict[str, list[Any]] | None = None,
    *,
    superseded_by: str = "",
) -> dict[str, Any]:
    meta = json_loads(row["meta_json"], {})
    if not isinstance(meta, dict):
        meta = {}
    collections = collections or {
        "affected_code_globs": [],
        "exemptions": [],
        "generated_artifact_paths": [],
        "verification": [],
    }
    completeness_issues = decision_completeness_issues(
        tier=str(row["tier"]),
        owner=str(row["owner"] or ""),
        affected_code_globs=collections["affected_code_globs"],
        verification=collections["verification"],
        applicability_scope=str(row["applicability_scope"]),
        exemptions=collections["exemptions"],
        generated_artifact_paths=collections["generated_artifact_paths"],
    )
    revision_count = _decision_revision_count(meta)
    approved_version_count = _decision_approved_version_count(
        meta, current_status=str(row["status"])
    )
    return {
        "id": row["human_id"],
        "title": row["title"],
        "tier": row["tier"],
        "tier_valid": bool(row["tier_valid"]),
        "status": row["status"],
        "applicability_scope": row["applicability_scope"],
        "owner": row["owner"] or "",
        "drift_risk": row["drift_risk"] or "",
        "display_utc": _decision_display_utc(row, meta),
        "proposed_utc": _decision_proposed_utc(row, meta),
        "accepted_utc": row["accepted_utc"],
        "in_force_utc": row["in_force_utc"],
        "retired_utc": row["retired_utc"],
        "last_verified_utc": row["last_verified_utc"],
        "superseded_by": superseded_by,
        "status_utc": _decision_status_utc(row, meta),
        "revision": revision_count,
        "revision_count": revision_count,
        "approved_version": approved_version_count or None,
        "approved_version_count": approved_version_count,
        "version": approved_version_count,
        "version_count": approved_version_count,
        "complete_for_acceptance": not completeness_issues,
        "completeness_issues": list(completeness_issues),
    }


def _public_decision_id(conn, value: Any) -> str:
    identifier = str(value or "")
    if not identifier:
        return ""
    row = conn.execute(
        "SELECT human_id FROM decisions WHERE dec_ulid=?",
        (identifier,),
    ).fetchone()
    return str(row["human_id"]) if row is not None else ""


def _public_decision_value(conn, value: Any) -> Any:
    """Remove storage identifiers from default Workbench decision JSON."""

    if isinstance(value, dict):
        return {
            str(key): _public_decision_value(conn, item)
            for key, item in value.items()
            if key not in {"dec_ulid", "internal_id"}
        }
    if isinstance(value, list):
        return [_public_decision_value(conn, item) for item in value]
    if isinstance(value, str) and value.startswith("dec_"):
        return _public_decision_id(conn, value)
    return value


def _decision_completeness_collections(
    conn,
    dec_ulids: set[str],
) -> dict[str, dict[str, list[Any]]]:
    collections: dict[str, dict[str, list[Any]]] = {
        dec_ulid: {
            "affected_code_globs": [],
            "exemptions": [],
            "generated_artifact_paths": [],
            "verification": [],
        }
        for dec_ulid in dec_ulids
    }
    for row in conn.execute("SELECT dec_ulid, pattern, kind FROM decision_globs ORDER BY rowid"):
        dec_ulid = str(row["dec_ulid"])
        if dec_ulid in collections:
            field = {
                "affected": "affected_code_globs",
                "exempt": "exemptions",
                "generated": "generated_artifact_paths",
            }.get(str(row["kind"]))
            if field is not None:
                collections[dec_ulid][field].append(str(row["pattern"]))
    for row in conn.execute(
        "SELECT dec_ulid, command, expected_signal FROM decision_verifications ORDER BY rowid"
    ):
        dec_ulid = str(row["dec_ulid"])
        if dec_ulid in collections:
            collections[dec_ulid]["verification"].append(
                {
                    "command": str(row["command"]),
                    "expected_signal": str(row["expected_signal"]),
                }
            )
    return collections


def _decision_collections(conn, dec_ulid: str) -> dict[str, Any]:
    affected_code_globs = [
        str(row["pattern"])
        for row in conn.execute(
            "SELECT pattern FROM decision_globs "
            "WHERE dec_ulid=? AND kind='affected' ORDER BY rowid",
            (dec_ulid,),
        )
    ]
    exemptions = [
        str(row["pattern"])
        for row in conn.execute(
            "SELECT pattern FROM decision_globs WHERE dec_ulid=? AND kind='exempt' ORDER BY rowid",
            (dec_ulid,),
        )
    ]
    generated_artifact_paths = [
        str(row["pattern"])
        for row in conn.execute(
            "SELECT pattern FROM decision_globs "
            "WHERE dec_ulid=? AND kind='generated' ORDER BY rowid",
            (dec_ulid,),
        )
    ]
    required_checks = [
        str(row["check_name"])
        for row in conn.execute(
            "SELECT check_name FROM decision_checks WHERE dec_ulid=? ORDER BY rowid",
            (dec_ulid,),
        )
    ]
    verification = [
        {
            "command": str(row["command"]),
            "execution_mode": str(row["execution_mode"]),
            "argv": json_loads(row["argv_json"], None),
            "expected_signal": str(row["expected_signal"]),
            "runtime_cost": row["runtime_cost"],
            "drift_risk": row["drift_risk"],
            "last_verified_utc": row["last_verified_utc"],
            "last_outcome": row["last_outcome"],
        }
        for row in conn.execute(
            "SELECT command, execution_mode, argv_json, expected_signal, "
            "runtime_cost, drift_risk, "
            "last_verified_utc, last_outcome FROM decision_verifications "
            "WHERE dec_ulid=? ORDER BY rowid",
            (dec_ulid,),
        )
    ]
    tags = [
        str(row["tag"])
        for row in conn.execute(
            "SELECT tag FROM decision_tags WHERE dec_ulid=? ORDER BY rowid",
            (dec_ulid,),
        )
    ]
    assumptions = [
        {
            "id": str(row["assumption_id"]),
            "text": str(row["text"]),
            "references": json_loads(row["references_json"], []),
            "status": str(row["status"]),
            "invalidated_event_id": row["invalidated_event_id"],
        }
        for row in conn.execute(
            "SELECT assumption_id, text, references_json, status, invalidated_event_id "
            "FROM decision_assumptions WHERE dec_ulid=? ORDER BY rowid",
            (dec_ulid,),
        )
    ]
    evidence: dict[str, list[str]] = {}
    for row in conn.execute(
        "SELECT evidence_kind, ref_value FROM decision_evidence "
        "WHERE dec_ulid=? ORDER BY evidence_kind, ref_value",
        (dec_ulid,),
    ):
        evidence.setdefault(str(row["evidence_kind"]), []).append(str(row["ref_value"]))
    return {
        "affected_code_globs": affected_code_globs,
        "exemptions": exemptions,
        "generated_artifact_paths": generated_artifact_paths,
        "required_checks": required_checks,
        "verification": verification,
        "assumptions": assumptions,
        "evidence": evidence,
        "tags": tags,
    }


def _decision_revision_sha(
    row: Any,
    meta: dict[str, Any],
    collections: dict[str, Any],
) -> str:
    digest_kwargs: dict[str, Any] = {}
    try:
        contract_version = int(row["contract_version"] or 0)
    except (IndexError, KeyError):
        contract_version = 0
    if contract_version >= 5:
        digest_kwargs = {
            "assumptions": collections.get("assumptions", []),
            "evidence": collections.get("evidence", {}),
            "review_policy": meta.get("review_policy", {}),
        }
    return decision_revision_digest(
        decision_id=str(row["dec_ulid"]),
        human_id=str(row["human_id"]),
        title=str(row["title"]),
        tier=str(row["tier"]),
        applicability_scope=str(row["applicability_scope"]),
        owner=str(row["owner"] or ""),
        context=str(meta.get("context") or ""),
        decision=str(meta.get("decision") or ""),
        body_sha=str(row["body_sha"]),
        affected_code_globs=collections.get("affected_code_globs", []),
        exemptions=collections.get("exemptions", []),
        generated_artifact_paths=collections.get("generated_artifact_paths", []),
        required_checks=collections.get("required_checks", []),
        verification=collections.get("verification", []),
        tags=collections.get("tags", []),
        **digest_kwargs,
    )


def _verification_definitions(values: list[Any]) -> list[dict[str, Any]]:
    definitions: list[dict[str, Any]] = []
    for item in values:
        if not isinstance(item, dict):
            continue
        definition = {
            "command": str(item.get("command") or ""),
            "execution_mode": str(item.get("execution_mode") or ""),
            "argv": item.get("argv"),
            "expected_signal": str(item.get("expected_signal") or "exit 0"),
        }
        for key in ("runtime_cost", "drift_risk"):
            if item.get(key) is not None:
                definition[key] = item[key]
        definitions.append(definition)
    return definitions


def _contains(value: Any, expected: str) -> bool:
    return expected.lower() in str(value or "").lower()


def _filter_backlog_items(
    items: list[dict[str, Any]],
    *,
    status: str = "",
    lane: str = "",
    priority: str = "",
    owner: str = "",
    item_type: str = "",
    launch_scope: str = "",
    wave: str = "",
    workflow_origin: str = "",
    query: str = "",
    quick_filter: str = "",
) -> list[dict[str, Any]]:
    filtered = list(items)
    filters = {
        "status": status,
        "lane": lane,
        "priority": priority,
        "owner_hint": owner,
        "item_type": item_type,
        "launch_scope": launch_scope,
        "wave": wave,
        "workflow_origin": workflow_origin,
    }
    for key, expected in filters.items():
        if expected:
            filtered = [item for item in filtered if _contains(item.get(key), expected)]
    if query:
        filtered = [item for item in filtered if _backlog_item_matches_query(item, query)]
    if quick_filter:
        filtered = [
            item for item in filtered if _backlog_item_matches_quick_filter(item, quick_filter)
        ]
    return filtered


def _backlog_item_matches_query(item: dict[str, Any], query: str) -> bool:
    haystack = "\n".join(str(item.get(field) or "") for field in BACKLOG_SEARCH_FIELDS)
    refs = item.get("refs") or []
    if refs:
        haystack += "\n" + "\n".join(str(ref) for ref in refs)
    return query.lower() in haystack.lower()


def _backlog_item_matches_quick_filter(item: dict[str, Any], quick_filter: str) -> bool:
    normalized = quick_filter.strip().lower().replace("-", "_")
    if normalized == "urgent":
        return _is_urgent(item)
    if normalized == "pending_user":
        return _is_pending_user(item)
    if normalized == "done":
        return _is_done(item)
    if normalized == "in_progress":
        return _is_in_progress(item)
    if normalized == "ahead":
        return not _is_done(item) and not _is_in_progress(item)
    return True


def _is_done(item: dict[str, Any]) -> bool:
    return str(item.get("status") or "").lower() in DONE_STATUSES


def _is_in_progress(item: dict[str, Any]) -> bool:
    status = str(item.get("status") or "").lower()
    lane = str(item.get("lane") or "").lower()
    return any(marker in status or marker in lane for marker in IN_PROGRESS_MARKERS)


def _is_pending_user(item: dict[str, Any]) -> bool:
    if _is_done(item):
        return False
    status = str(item.get("status") or "").lower()
    lane = str(item.get("lane") or "").lower()
    owner_hint = str(item.get("owner_hint") or "").lower()
    return any(marker in status or marker in lane for marker in PENDING_MARKERS) or owner_hint in {
        "operator",
        "user",
        "human",
    }


def _is_urgent(item: dict[str, Any]) -> bool:
    if _is_done(item):
        return False
    priority = str(item.get("priority") or "").upper()
    launch_scope = str(item.get("launch_scope") or "").lower()
    status = str(item.get("status") or "").lower()
    return priority == "P0" or "blocking" in launch_scope or "blocked" in status


def _decision_status_utc(row, meta: dict[str, Any]) -> str:
    status = str(row["status"] or "")
    if status == "retired" and row["retired_utc"]:
        return str(row["retired_utc"] or "")
    if status == "in_force" and row["in_force_utc"]:
        return str(row["in_force_utc"] or "")
    if status == "accepted" and row["accepted_utc"]:
        return str(row["accepted_utc"] or "")
    event_kind_by_status = {
        "accepted": "decision_accepted",
        "superseded": "decision_superseded",
        "retired": "decision_retired",
        "rejected": "decision_rejected",
    }
    expected_kind = event_kind_by_status.get(status)
    if expected_kind:
        for event in reversed(meta.get("event_log", [])):
            if isinstance(event, dict) and event.get("kind") == expected_kind:
                return str(event.get("occurred_utc") or row["proposed_utc"] or "")
    return _decision_proposed_utc(row, meta)


def _decision_proposed_utc(row, meta: dict[str, Any]) -> str:
    _ = meta
    return str(row["proposed_utc"] or "")


def _decision_display_utc(row, meta: dict[str, Any]) -> str:
    return _decision_status_utc(row, meta) or _decision_proposed_utc(row, meta)


def _feedback_request_counts(conn) -> dict[str, int]:
    return {
        "feedback_requests": conn.execute(
            f"SELECT COUNT(*) FROM messages WHERE kind='request' AND {FEEDBACK_REQUEST_PREDICATE_SQL}"
        ).fetchone()[0],
        "open_feedback_requests": conn.execute(
            f"""
            SELECT COUNT(*) FROM messages
            WHERE kind='request' AND status='open' AND {FEEDBACK_REQUEST_PREDICATE_SQL}
            """
        ).fetchone()[0],
        "closed_feedback_requests": conn.execute(
            f"""
            SELECT COUNT(*) FROM messages
            WHERE kind='request' AND status='closed' AND {FEEDBACK_REQUEST_PREDICATE_SQL}
            """
        ).fetchone()[0],
    }


def _snapshot_metrics(conn) -> dict[str, Any]:
    rows = conn.execute("SELECT * FROM backlog_items").fetchall()
    by_status = _count_by(rows, "status")
    by_lane = _count_by(rows, "lane", default="unassigned")
    by_priority = _count_by(rows, "priority", default="unprioritized")
    items = [_backlog_item_from_row(conn, row) for row in rows]
    done = 0
    in_progress = 0
    pending_user = 0
    urgent = 0
    for item in items:
        if _is_done(item):
            done += 1
        if _is_in_progress(item):
            in_progress += 1
        if _is_pending_user(item):
            pending_user += 1
        if _is_urgent(item):
            urgent += 1
    ahead = max(len(items) - done - in_progress, 0)
    recent = [
        {
            "id": row["id"],
            "title": row["title"],
            "status": row["status"],
            "lane": row["lane"],
            "priority": row["priority"],
            "updated_utc": row["updated_utc"],
        }
        for row in conn.execute(
            """
            SELECT * FROM backlog_items
            ORDER BY updated_utc DESC, event_seq DESC
            LIMIT 6
            """
        ).fetchall()
    ]
    return {
        "done": done,
        "in_progress": in_progress,
        "ahead": ahead,
        "urgent": urgent,
        "pending_user": pending_user,
        "by_status": by_status,
        "by_lane": by_lane,
        "by_priority": by_priority,
        "recent_backlog": recent,
    }


def _count_by(rows, key: str, *, default: str = "unknown") -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row[key] or default)
        counts[value] = counts.get(value, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: item[0]))


WORKBENCH_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Agent Mesh Workbench</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f7f9;
      --panel: #ffffff;
      --ink: #17202a;
      --muted: #667085;
      --line: #d8dee8;
      --accent: #176b87;
      --accent-2: #6b5b95;
      --ok: #176b3a;
      --warn: #9a5b13;
      --error: #a33a45;
      --soft: #eef4f8;
      --radius: 8px;
      --mono: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      --sans: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font-family: var(--sans);
    }
    main {
      width: min(1500px, calc(100vw - 28px));
      margin: 18px auto 42px;
      display: grid;
      gap: 14px;
    }
    header, section {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      min-width: 0;
      max-width: 100%;
    }
    header {
      padding: 16px;
      display: grid;
      gap: 10px;
    }
    h1, h2, h3, p { margin: 0; }
    h1 { font-size: 1.45rem; }
    h2 { font-size: 1rem; }
    h3 { font-size: 0.92rem; }
    p, .muted { color: var(--muted); line-height: 1.45; }
    code, pre { font-family: var(--mono); }
    .workbench-title {
      display: flex;
      flex-wrap: wrap;
      gap: 12px;
      align-items: end;
      justify-content: space-between;
    }
    .repo-picker {
      display: grid;
      gap: 4px;
      min-width: min(480px, 100%);
    }
    .repo-picker span { color: var(--muted); font-size: 0.78rem; }
    .command-box {
      display: grid;
      gap: 6px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: #f9fbfc;
      padding: 10px;
    }
    .command-line {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
      min-width: 0;
    }
    .command-line code {
      flex: 1;
      min-width: 260px;
      overflow-x: auto;
      white-space: nowrap;
      background: #eef2f6;
      border-radius: 6px;
      padding: 7px 8px;
      font-size: 0.82rem;
    }
    .copy-status {
      min-width: 112px;
      color: var(--ok);
      font-size: 0.82rem;
      font-weight: 600;
    }
    .copy-status.error { color: var(--error); }
    .status {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
      font-family: var(--mono);
      font-size: 0.84rem;
    }
    .badge {
      display: inline-flex;
      gap: 6px;
      align-items: center;
      border: 1px solid var(--line);
      border-radius: 999px;
      background: var(--soft);
      padding: 5px 9px;
      white-space: nowrap;
    }
    .connection {
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 9px 11px;
      font-size: 0.88rem;
      font-weight: 650;
    }
    .connection.checking { background: var(--soft); color: var(--muted); }
    .connection.online { background: #e9f7ef; border-color: #9bc9ab; color: var(--ok); }
    .connection.offline { background: #fff2f3; border-color: #e3abb1; color: var(--error); }
    .connection.restart-required {
      background: #fff2f3;
      border-color: #e3abb1;
      color: var(--error);
    }
    .metrics {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(130px, 1fr));
      gap: 8px;
    }
    .metric {
      border: 1px solid var(--line);
      border-radius: 6px;
      background: #f9fbfc;
      padding: 10px;
      display: grid;
      gap: 4px;
    }
	    .metric strong { font-size: 1.35rem; line-height: 1; }
	    .metric span { color: var(--muted); font-size: 0.82rem; }
	    .metric.urgent strong { color: var(--error); }
	    .metric[data-backlog-filter],
	    .metric[data-message-kind],
	    .metric[data-message-feature] {
	      cursor: pointer;
	    }
	    .metric[data-backlog-filter]:hover,
	    .metric[data-message-kind]:hover,
	    .metric[data-message-feature]:hover {
	      border-color: var(--accent);
	      background: #edf8fb;
	    }
	    .tabs {
	      display: flex;
	      flex-wrap: wrap;
	      gap: 8px;
	    }
	    .tab-button {
	      background: #e8edf2;
	      color: var(--ink);
	      border: 1px solid var(--line);
	    }
	    .tab-button.active {
	      background: var(--accent);
	      color: white;
	      border-color: var(--accent);
	    }
	    .tab-panel { display: none; min-width: 0; max-width: 100%; }
	    .tab-panel.active { display: grid; }
	    .dashboard-grid {
	      display: grid;
	      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
	      gap: 10px;
	    }
	    .mini-list {
	      border: 1px solid var(--line);
	      border-radius: 6px;
	      background: #f9fbfc;
	      padding: 10px;
	      display: grid;
	      gap: 6px;
	      align-content: start;
	    }
	    .mini-list div {
	      display: flex;
	      justify-content: space-between;
	      gap: 10px;
	      border-bottom: 1px solid #edf1f5;
	      padding-bottom: 5px;
	      font-size: 0.86rem;
	    }
	    .mini-list div:last-child { border-bottom: 0; padding-bottom: 0; }
	    .mini-list div[data-backlog-field] { cursor: pointer; }
	    .mini-list div[data-backlog-field]:hover span {
	      color: var(--accent);
	      text-decoration: underline;
	    }
	    [data-requires-server-gesture][aria-disabled="true"] {
	      cursor: not-allowed !important;
	      opacity: 0.62;
	    }
	    .field-note {
	      color: var(--muted);
	      font-size: 0.72rem;
	      font-weight: 600;
	      margin-left: 4px;
	      text-transform: uppercase;
	    }
	    .required-mark {
	      color: var(--error);
	      font-size: 0.95rem;
	      font-weight: 700;
	      margin-left: 3px;
	    }
	    .grid {
      display: grid;
      grid-template-columns: minmax(320px, 0.9fr) minmax(420px, 1.1fr);
      gap: 14px;
    }
    section {
      padding: 14px;
      display: grid;
      gap: 12px;
      min-width: 0;
    }
    .stack { display: grid; gap: 10px; min-width: 0; max-width: 100%; }
    .compact-stack { gap: 4px; align-items: start; }
    .row {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      align-items: center;
      min-width: 0;
      max-width: 100%;
    }
    .row > * { min-width: 0; }
    label {
      display: grid;
      gap: 5px;
      font-size: 0.88rem;
      color: var(--muted);
    }
    input, textarea, select {
      width: 100%;
      min-width: 0;
      max-width: 100%;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 9px 10px;
      font: inherit;
      color: var(--ink);
      background: #fff;
    }
    textarea { min-height: 120px; resize: vertical; }
    button {
      border: 0;
      border-radius: 6px;
      padding: 9px 11px;
      background: var(--accent);
      color: white;
      font: inherit;
      cursor: pointer;
      box-shadow: 0 1px 2px rgb(15 23 42 / 0.18);
      transition: background-color 140ms ease, box-shadow 140ms ease,
        filter 140ms ease, transform 90ms ease;
    }
    button.secondary { background: var(--accent-2); }
    button.ghost { background: #e8edf2; color: var(--ink); }
    button.danger { background: #9f3a38; color: white; }
    button:hover:not(:disabled) {
      filter: brightness(1.08);
      box-shadow: 0 3px 8px rgb(15 23 42 / 0.22);
      transform: translateY(-1px);
    }
    button.ghost:hover:not(:disabled) { background: #dbe5ed; }
    button:active:not(:disabled) {
      filter: brightness(0.94);
      box-shadow: 0 1px 2px rgb(15 23 42 / 0.16);
      transform: translateY(1px) scale(0.98);
    }
    button:focus-visible {
      outline: 3px solid rgb(10 115 141 / 0.28);
      outline-offset: 2px;
    }
    button:disabled { cursor: not-allowed; opacity: 0.52; }
    button[aria-busy="true"] { cursor: progress; }
    button.button-success,
    button.button-success:hover:not(:disabled) { background: #177245; color: white; }
    button.button-error,
    button.button-error:hover:not(:disabled) { background: var(--error); color: white; }
    button.button-click-feedback { animation: button-click-feedback 220ms ease-out; }
    button.table-action {
      padding: 4px 8px;
      border-radius: 6px;
      font-family: var(--mono);
      font-size: inherit;
    }
    @keyframes button-click-feedback {
      0% { box-shadow: 0 0 0 0 rgb(10 115 141 / 0.34); }
      100% { box-shadow: 0 0 0 7px rgb(10 115 141 / 0); }
    }
    .output {
      min-height: 120px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: #111827;
      color: #f9fafb;
      padding: 12px;
      white-space: pre-wrap;
      overflow: auto;
      overflow-wrap: anywhere;
      word-break: break-word;
      min-width: 0;
      max-width: 100%;
      font: 0.84rem/1.45 var(--mono);
    }
    .table-wrap {
      border: 1px solid var(--line);
      border-radius: 6px;
      overflow: auto;
      max-height: 420px;
      min-width: 0;
      max-width: 100%;
    }
    table { width: 100%; border-collapse: collapse; font-size: 0.88rem; }
    th, td {
      border-bottom: 1px solid var(--line);
      padding: 8px;
      text-align: left;
      vertical-align: top;
    }
    th {
      position: sticky;
      top: 0;
      background: #eef2f6;
      z-index: 1;
    }
    th[data-sort-key] {
      cursor: pointer;
      user-select: none;
    }
    th[data-sort-key]:hover {
      color: var(--accent);
      background: #dfeaf2;
    }
    .sort-mark {
      display: inline-block;
      min-width: 10px;
      margin-left: 4px;
      color: var(--accent);
      font-size: 0.75rem;
    }
    tr[data-message-id],
    tr[data-backlog-view-id],
    tr[data-decision-id] {
      cursor: pointer;
    }
    tr[data-message-id]:hover,
    tr[data-backlog-view-id]:hover,
    tr[data-decision-id]:hover {
      background: #edf8fb;
    }
    tr[data-message-id]:hover code,
    tr[data-backlog-view-id]:hover code,
    tr[data-decision-id]:hover code {
      color: var(--accent);
      text-decoration: underline;
    }
    .kanban {
      display: flex;
      gap: 10px;
      overflow-x: auto;
      padding-bottom: 4px;
    }
    .lane {
      flex: 0 0 260px;
      border: 1px solid var(--line);
      border-radius: 6px;
      background: #f9fbfc;
      padding: 10px;
      display: grid;
      gap: 8px;
      align-content: start;
    }
    .lane.dragover {
      border-color: var(--accent);
      background: #edf8fb;
    }
    .card {
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 9px;
      background: white;
      display: grid;
      gap: 6px;
    }
    .card strong { line-height: 1.3; }
    .card small { color: var(--muted); font-family: var(--mono); }
    .card[draggable="true"] { cursor: grab; }
    .card[draggable="true"]:active { cursor: grabbing; }
    .mini-field {
      min-width: 92px;
      padding: 5px 6px;
      font-size: 0.82rem;
    }
	    .edit-status {
	      min-height: 18px;
	      color: var(--muted);
	      font-size: 0.84rem;
	    }
	    .dropzone {
	      border: 1px dashed var(--line);
	      border-radius: 6px;
	      background: #f9fbfc;
	      padding: 14px;
	      display: grid;
	      gap: 9px;
	      text-align: center;
	    }
	    .dropzone.dragover {
	      border-color: var(--accent);
	      background: #edf8fb;
	    }
	    .file-list {
	      white-space: pre-wrap;
	      text-align: left;
	      font: 0.82rem/1.4 var(--mono);
	      color: var(--muted);
	    }
	    .search-controls {
	      display: grid;
	      grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
	      gap: 8px;
	      align-items: end;
	      min-width: 0;
	      max-width: 100%;
	    }
	    .search-controls > * { min-width: 0; }
	    .search-controls label:first-child {
	      grid-column: span 2;
	    }
	    details.disclosure,
	    details.system-details {
	      border: 1px solid var(--line);
	      border-radius: 6px;
	      background: #f9fbfc;
	      min-width: 0;
	      max-width: 100%;
	    }
	    details.disclosure > summary,
	    details.system-details > summary {
	      cursor: pointer;
	      padding: 10px 12px;
	      font-weight: 700;
	      color: var(--ink);
	    }
	    details.disclosure > :not(summary),
	    details.system-details > :not(summary) { margin: 0 12px 12px; }
	    .markdown-view {
	      color: var(--ink);
	      font: 0.95rem/1.55 system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
	      overflow-wrap: anywhere;
	    }
	    .markdown-view h1, .markdown-view h2, .markdown-view h3,
	    .markdown-view h4, .markdown-view h5, .markdown-view h6 { margin: 0.85em 0 0.35em; }
	    .markdown-view > :first-child { margin-top: 0; }
	    .markdown-view p { margin: 0.45em 0; }
	    .markdown-view ul, .markdown-view ol { margin: 0.45em 0; padding-left: 1.5em; }
	    .markdown-view pre {
	      overflow: auto;
	      max-width: 100%;
	      padding: 10px;
	      border-radius: 6px;
	      background: #111827;
	      color: #f9fafb;
	    }
	    .markdown-view code { font-family: var(--mono); }
	    .decision-browser {
	      display: grid;
	      grid-template-columns: minmax(0, 1fr);
	      gap: 12px;
	      align-items: start;
	    }
	    .decision-list-pane { display: grid; gap: 10px; min-width: 0; }
	    .decision-list-pane .table-wrap { max-height: 480px; }
	    tr.decision-row-selected td { background: #e8f6f9; }
	    .dashboard-nav-guide { display: grid; gap: 8px; }
	    .dashboard-nav-guide > div {
	      display: grid;
	      grid-template-columns: minmax(130px, 170px) 1fr;
	      gap: 10px;
	      align-items: center;
	    }
	    .dashboard-nav-guide button { text-align: left; }
	    .decision-review {
	      min-height: 300px;
	      padding: 16px;
	      border: 1px solid var(--line);
	      border-radius: 6px;
	      background: #fff;
	      display: grid;
	      gap: 12px;
	    }
	    .decision-review[hidden] { display: none; }
	    .decision-review h3, .decision-review h4 { margin: 0; }
	    .decision-review-heading { display: flex; gap: 8px; align-items: flex-start; }
	    .decision-review-heading > div { flex: 1; }
	    .decision-review-badges { display: flex; flex-wrap: wrap; gap: 6px; }
	    .decision-review-meta { color: var(--muted); font-size: 0.84rem; }
	    .decision-high-level { display: grid; gap: 12px; }
	    .decision-high-level section { padding: 0; gap: 5px; }
	    .decision-actions {
	      padding-top: 12px;
	      border-top: 1px solid var(--line);
	      display: grid;
	      gap: 8px;
	    }
	    .decision-actions-grid {
	      display: grid;
	      grid-template-columns: minmax(130px, 0.4fr) minmax(240px, 1fr);
	      gap: 8px;
	    }
	    .human-diff { display: grid; gap: 10px; }
	    .human-diff-field {
	      border: 1px solid var(--line);
	      border-radius: 6px;
	      padding: 10px;
	      background: #fff;
	    }
	    .human-diff-field h4 { margin: 0 0 7px; }
	    .human-diff-field del {
	      background: #fde2e1;
	      color: #7f1d1d;
	      text-decoration-thickness: 1px;
	      box-decoration-break: clone;
	      -webkit-box-decoration-break: clone;
	    }
	    .human-diff-field ins {
	      background: #dcfce7;
	      color: #14532d;
	      text-decoration: none;
	      box-decoration-break: clone;
	      -webkit-box-decoration-break: clone;
	    }
	    .human-diff-field .markdown-view > :first-child { margin-top: 0; }
	    .human-diff-field .markdown-view > :last-child { margin-bottom: 0; }
	    .markdown-diff-fallback {
	      display: grid;
	      grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
	      gap: 10px;
	    }
	    .markdown-diff-fallback section {
	      min-width: 0;
	      padding: 9px;
	      border: 1px solid var(--line);
	      border-radius: 6px;
	      background: #f9fbfc;
	    }
	    .markdown-diff-fallback h5 { margin: 0 0 7px; }
	    .human-diff-empty { color: var(--muted); font-style: italic; }
	    .decision-version-controls {
	      display: grid;
	      grid-template-columns: repeat(2, minmax(180px, 1fr));
	      gap: 8px;
	    }
	    .decision-version-body-diff { overflow-wrap: anywhere; }
	    .dispatch-audit { font-size: 0.78rem; }
	    .dispatch-audit summary { cursor: pointer; color: var(--muted); }
	    .dispatch-audit dl { margin: 6px 0 0; display: grid; grid-template-columns: auto 1fr; gap: 3px 7px; }
	    .dispatch-audit dd { margin: 0; overflow-wrap: anywhere; }
	    tr.dispatch-row-selected td { background: #e8f6f9; }
	    tr.dispatch-followup-row td:first-child {
	      border-left: 3px solid #9cc8d2;
	      padding-left: 22px;
	    }
	    .dispatch-chain-controls { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 5px; }
	    .dispatch-chain-tag {
	      display: inline-block;
	      margin-bottom: 3px;
	      color: #397282;
	      font-size: 0.78rem;
	      font-weight: 700;
	    }
	    .dispatch-result-card { margin-top: 10px; }
	    .dispatch-result-card .row { align-items: flex-start; }
	    .lifecycle-guide { display: grid; gap: 7px; font-size: 0.86rem; }
	    .lifecycle-guide div { display: grid; grid-template-columns: 100px 1fr; gap: 8px; }
	    .decision-editor {
	      margin: 12px 0;
	      padding: 14px;
	      border: 1px solid var(--line);
	      border-radius: 6px;
	      background: #f9fbfc;
	      display: grid;
	      gap: 10px;
	    }
	    .decision-applicability-panel {
	      margin: 12px 0;
	      padding: 14px;
	      border: 1px solid var(--line);
	      border-radius: 6px;
	      background: #f9fbfc;
	      display: grid;
	      gap: 10px;
	    }
	    .decision-editor-grid {
	      display: grid;
	      grid-template-columns: minmax(120px, 0.6fr) minmax(180px, 1fr) minmax(180px, 1fr);
	      gap: 8px;
	    }
	    .dispatch-editor-grid {
	      display: grid;
	      grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
	      gap: 8px;
	    }
	    .dispatch-editor-grid .wide { grid-column: 1 / -1; }
	    .dispatch-purpose-guide {
	      padding: 10px 12px;
	      border-left: 4px solid var(--accent);
	      border-radius: 4px;
	      background: #edf8fb;
	    }
	    .dispatch-purpose-guide p { margin: 0; }
	    .dispatch-use-cases {
	      display: grid;
	      grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
	      gap: 8px;
	    }
	    .dispatch-use-cases div {
	      padding: 9px 10px;
	      border: 1px solid var(--line);
	      border-radius: 6px;
	      background: #fff;
	    }
	    .dispatch-use-cases strong { display: block; margin-bottom: 3px; }
	    .dispatch-source-picker {
	      min-width: 0;
	      margin: 0;
	      padding: 10px;
	      border: 1px solid var(--line);
	      border-radius: 6px;
	      background: #f9fbfc;
	    }
	    .dispatch-source-picker legend {
	      padding: 0 5px;
	      color: var(--ink);
	      font-size: 0.9rem;
	      font-weight: 700;
	    }
	    .dispatch-source-options {
	      display: grid;
	      grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
	      gap: 8px;
	    }
	    .dispatch-source-option {
	      grid-template-columns: auto 1fr;
	      align-items: start;
	      gap: 8px;
	      padding: 9px;
	      border: 1px solid var(--line);
	      border-radius: 6px;
	      background: #fff;
	      color: var(--ink);
	      cursor: pointer;
	    }
	    .dispatch-source-option:focus-within {
	      outline: 3px solid rgb(10 115 141 / 0.2);
	      outline-offset: 1px;
	    }
	    .dispatch-source-option input { width: auto; margin-top: 3px; }
	    .dispatch-source-option span { display: grid; gap: 2px; }
	    .dispatch-source-option small { color: var(--muted); line-height: 1.35; }
	    .dispatch-source-panel {
	      padding: 10px;
	      border: 1px solid var(--line);
	      border-radius: 6px;
	      background: #fff;
	    }
	    .dispatch-source-panel[hidden] { display: none; }
	    .dispatch-source-panel label + label { margin-top: 8px; }
	    .dispatch-selection {
	      display: grid;
	      gap: 5px;
	      align-content: start;
	      padding: 8px 10px;
	      border: 1px solid var(--line);
	      border-radius: 6px;
	      background: #fff;
	    }
	    .dispatch-request-validation {
	      min-height: 18px;
	      margin: 7px 0 0;
	      color: var(--muted);
	      font-size: 0.84rem;
	    }
	    .dispatch-request-validation.valid { color: var(--ok); font-weight: 650; }
	    .dispatch-request-validation.invalid { color: var(--error); font-weight: 650; }
	    .dispatch-request-matches {
	      display: grid;
	      gap: 5px;
	      max-height: 250px;
	      margin-top: 7px;
	      overflow: auto;
	    }
	    .dispatch-request-matches[hidden] { display: none; }
	    button.dispatch-request-match {
	      display: grid;
	      grid-template-columns: minmax(210px, 0.8fr) minmax(0, 1.2fr);
	      gap: 3px 10px;
	      align-items: start;
	      width: 100%;
	      border: 1px solid var(--line);
	      background: #f9fbfc;
	      color: var(--ink);
	      text-align: left;
	    }
	    button.dispatch-request-match:hover:not(:disabled),
	    button.dispatch-request-match:focus-visible {
	      border-color: var(--accent);
	      background: #edf8fb;
	    }
	    button.dispatch-request-match:disabled {
	      cursor: not-allowed;
	      opacity: 1;
	      border-color: #e7c89d;
	      background: #fff7ed;
	    }
	    .dispatch-request-match code { overflow-wrap: anywhere; }
	    .dispatch-request-match small {
	      grid-column: 1 / -1;
	      color: var(--muted);
	    }
	    .dispatch-public-reference {
	      display: flex;
	      flex-wrap: wrap;
	      gap: 5px;
	      align-items: baseline;
	      margin-top: 4px;
	      color: #397282;
	      font-size: 0.78rem;
	      font-weight: 700;
	    }
	    .dispatch-public-reference code {
	      color: var(--ink);
	      font-weight: 600;
	      overflow-wrap: anywhere;
	    }
	    @media (max-width: 620px) {
	      button.dispatch-request-match { grid-template-columns: 1fr; }
	      .dispatch-request-match small { grid-column: 1; }
	    }
	    .dispatch-assurance-panel {
	      padding: 12px;
	      border: 1px solid var(--line);
	      border-radius: 6px;
	      background: #fff;
	      display: grid;
	      gap: 10px;
	    }
	    .dispatch-assurance-panel h3 { margin: 0; }
	    .dispatch-assurance-card {
	      padding: 12px;
	      border: 1px solid var(--line);
	      border-radius: 6px;
	      background: #f9fbfc;
	      display: grid;
	      gap: 8px;
	    }
	    .dispatch-assurance-card p { margin: 0; }
	    .dispatch-assurance-action {
	      display: grid;
	      grid-template-columns: minmax(170px, 0.7fr) minmax(220px, 1.4fr) auto;
	      gap: 8px;
	      align-items: end;
	    }
	    @media (max-width: 820px) {
	      .dispatch-assurance-action { grid-template-columns: 1fr; }
	    }
	    .decision-editor .wide { grid-column: 1 / -1; }
	    .decision-editor textarea { min-height: 96px; }
	    .decision-editor textarea.body { min-height: 220px; font-family: var(--mono); }
	    .decision-evidence-field { display: grid; gap: 8px; }
	    .decision-evidence-links {
	      display: flex;
	      flex-wrap: wrap;
	      gap: 7px;
	      align-items: center;
	    }
	    .decision-evidence-links:empty { display: none; }
	    button.decision-evidence-link {
	      display: inline-flex;
	      flex-wrap: wrap;
	      gap: 6px;
	      align-items: center;
	      padding: 6px 9px;
	      font-size: 0.8rem;
	      text-align: left;
	    }
	    button.decision-evidence-link code { overflow-wrap: anywhere; }
	    .decision-evidence-kind { font-weight: 700; }
	    .decision-evidence-destination { color: var(--muted); }
	    .decision-revision-diff {
	      padding: 12px;
	      border: 1px solid #c9d6df;
	      border-radius: 6px;
	      background: #fff;
	      display: grid;
	      gap: 9px;
	    }
	    .decision-revision-diff h3,
	    .decision-revision-diff h4 { margin: 0; }
	    .decision-revision-diff dl {
	      margin: 0;
	      display: grid;
	      grid-template-columns: minmax(150px, 0.55fr) minmax(240px, 1.45fr);
	      gap: 5px 10px;
	      font-size: 0.82rem;
	    }
	    .decision-revision-diff dt { color: var(--muted); font-weight: 700; }
	    .decision-revision-diff dd { margin: 0; overflow-wrap: anywhere; font-family: var(--mono); }
	    .decision-diff-output {
	      max-height: 320px;
	      min-height: 72px;
	      overflow: auto;
	      margin: 0;
	    }
	    .destructive-confirmation {
	      width: min(620px, calc(100vw - 32px));
	      border: 1px solid #9f3a38;
	      border-radius: 8px;
	      padding: 0;
	      color: var(--text);
	      background: white;
	    }
	    .destructive-confirmation::backdrop { background: rgb(15 23 42 / 0.55); }
	    .destructive-confirmation form { display: grid; gap: 12px; padding: 20px; }
	    .destructive-confirmation h3 { margin: 0; color: #8a2d2b; }
	    .destructive-confirmation ul { margin: 0; padding-left: 22px; }
	    .destructive-confirmation .acknowledgement {
	      display: flex;
	      gap: 8px;
	      align-items: flex-start;
	    }
	    .destructive-confirmation .acknowledgement input { margin-top: 3px; }
	    @media (max-width: 900px) {
	      .grid { grid-template-columns: 1fr; }
	      .search-controls { grid-template-columns: 1fr; }
	      .search-controls label:first-child { grid-column: span 1; }
	      .decision-editor-grid { grid-template-columns: 1fr; }
	      .decision-editor .wide { grid-column: span 1; }
	      .decision-revision-diff dl { grid-template-columns: 1fr; }
	      .decision-actions-grid { grid-template-columns: 1fr; }
	      .decision-version-controls { grid-template-columns: 1fr; }
	      .dashboard-nav-guide > div { grid-template-columns: 1fr; }
	    }
	    @media (prefers-reduced-motion: reduce) {
	      button { transition: none; }
	      button:hover:not(:disabled),
	      button:active:not(:disabled) { transform: none; }
	      button.button-click-feedback { animation: none; }
	    }
  </style>
</head>
<body>
<main>
	  <header>
	    <div class="workbench-title">
	      <h1>Agent Mesh Workbench</h1>
	      <label class="repo-picker">Active repository
	        <select id="repo-selector" data-requires-server disabled>
	          <option value="">Loading registered repos...</option>
	        </select>
	        <span>All feedback, status changes, backlog updates, and decision reads are scoped to this repository.</span>
	      </label>
	    </div>
	    <details class="system-details">
	      <summary>Server details and recovery</summary>
	    <div class="command-box" id="workbench-identity-panel" aria-label="Workbench server identity">
      <div class="command-line">
        <span class="muted">Mode</span>
        <strong id="workbench-mode">Checking…</strong>
        <span class="muted">Endpoint</span>
        <code id="workbench-endpoint">Checking…</code>
      </div>
      <div class="command-line">
        <span class="muted">Package root</span>
        <code id="workbench-package-root">Available after authenticated health check</code>
      </div>
      <div class="command-line">
        <span class="muted">Source checkout</span>
        <code id="workbench-source">Available after authenticated health check</code>
      </div>
      <div class="command-line">
        <span class="muted">Anchor repository</span>
        <code id="workbench-anchor-repo">Available after authenticated health check</code>
      </div>
      <div class="command-line" id="manual-superseded-panel" hidden>
        <strong>Manual server superseded</strong>
        <span id="manual-superseded-detail">Open the managed Workbench before using live data or actions.</span>
        <a id="manual-superseded-link" href="__AGENT_MESH_MANAGED_BOOKMARK_URL__" hidden>Open managed Workbench</a>
      </div>
    </div>
	    <div class="command-box" id="manual-launch-panel">
      <div class="command-line">
        <span class="muted">Automatic Workbench</span>
        <strong>This is the manual project bookmark.</strong>
        <code id="managed-bookmark-guidance-path">__AGENT_MESH_MANAGED_BOOKMARK_PATH__</code>
        <a id="open-managed-bookmark" href="__AGENT_MESH_MANAGED_BOOKMARK_URL__" hidden>Open managed Workbench</a>
      </div>
      <div class="command-line">
        <span class="muted">Start / restart</span>
        <code id="start-command">__AGENT_MESH_START_COMMAND__</code>
        <button type="button" class="ghost" id="copy-start-command">Copy</button>
        <span id="copy-start-status" class="copy-status" role="status" aria-live="polite"></span>
      </div>
      <div class="command-line">
        <span class="muted">Bookmark</span>
        <code id="bookmark-path">__AGENT_MESH_BOOKMARK_PATH__</code>
        <a id="bookmark-link" href="__AGENT_MESH_BOOKMARK_URL__">Open bookmark file</a>
	      </div>
	    </div>
	    <div class="command-box" id="managed-launch-panel" hidden>
      <div class="command-line">
        <span class="muted">Automatic startup</span>
        <strong>Enabled for this user</strong>
        <button type="button" class="ghost" id="recheck-server">Reconnect</button>
        <span id="recheck-server-status" class="copy-status" role="status" aria-live="polite"></span>
      </div>
      <div class="command-line">
        <span class="muted">Bookmark</span>
        <code id="managed-bookmark-path">__AGENT_MESH_BOOKMARK_PATH__</code>
        <a id="managed-bookmark-link" href="__AGENT_MESH_BOOKMARK_URL__">Open bookmark file</a>
      </div>
	      <div class="command-line">
	        <span class="muted">Service status</span>
	        <code id="managed-status-command">agent-mesh workbench service status</code>
	      </div>
	      <div class="command-line">
	        <span class="muted">Service start</span>
	        <code id="managed-start-command">agent-mesh workbench service start</code>
	      </div>
	      <div class="command-line">
	        <span class="muted">Service restart</span>
	        <code id="managed-restart-command">__AGENT_MESH_START_COMMAND__</code>
	      </div>
	      <div class="command-line">
	        <span class="muted">Install / repair</span>
	        <code id="managed-install-command">agent-mesh workbench service install</code>
	      </div>
	    </div>
	    </details>
	    <div id="server-connection" class="connection checking" role="status" aria-live="polite">Checking Workbench server...</div>
	    <details class="system-details">
	      <summary id="project-health-summary">Project and agent details</summary>
	      <div id="agent-contract-health" class="connection checking" role="status" aria-live="polite">Checking agent contract...</div>
	      <div id="decision-approval-health" class="connection checking" role="status" aria-live="polite">Checking decision approval authority...</div>
	      <div id="status" class="status"><span class="badge">loading</span></div>
	      <div id="instance-history" class="status"></div>
	    </details>
	  </header>

	  <nav class="tabs" aria-label="Workbench views">
	    <button class="tab-button active" data-tab="dashboard">Dashboard</button>
	    <button class="tab-button" data-tab="backlog">Backlog</button>
	    <button class="tab-button" data-tab="decisions">Decisions</button>
	    <button class="tab-button" data-tab="feedback">Verify / Feedback</button>
	    <button class="tab-button" data-tab="messages">Messages</button>
	    <button class="tab-button" data-tab="dispatches">Dispatch</button>
	    <button class="tab-button" data-tab="kanban">Kanban</button>
	  </nav>

	  <section id="tab-dashboard" class="tab-panel active">
	    <h2>Dashboard</h2>
	    <details class="disclosure dashboard-guide">
	      <summary>How to use this Workbench</summary>
	      <div class="dashboard-nav-guide">
	        <div><button type="button" class="ghost" data-dashboard-tab="backlog">Backlog</button><span>Review and prioritize durable work; select an item for its human-readable summary and update its workflow fields.</span></div>
	        <div><button type="button" class="ghost" data-dashboard-tab="decisions">Decisions</button><span>Review durable choices, compare saved revisions, and directly approve or reject an exact proposal revision.</span></div>
	        <div><button type="button" class="ghost" data-dashboard-tab="feedback">Verify / Feedback</button><span>Record an observation, screenshot, or verification result as durable feedback for follow-up.</span></div>
	        <div><button type="button" class="ghost" data-dashboard-tab="messages">Messages</button><span>Inspect durable requests and outcomes, then close or reopen requests with a reason.</span></div>
	        <div><button type="button" class="ghost" data-dashboard-tab="dispatches">Dispatch</button><span>Start or inspect managed AI work when a durable request needs an execution policy and outcome.</span></div>
	        <div><button type="button" class="ghost" data-dashboard-tab="kanban">Kanban</button><span>Scan backlog items by lane and move work between workflow stages.</span></div>
	      </div>
	    </details>
	    <div id="snapshot" class="metrics"></div>
	    <div class="dashboard-grid">
	      <div>
	        <h3>By Status</h3>
	        <div id="dashboard-status" class="mini-list"></div>
	      </div>
	      <div>
	        <h3>By Lane</h3>
	        <div id="dashboard-lane" class="mini-list"></div>
	      </div>
	      <div>
	        <h3>By Priority</h3>
	        <div id="dashboard-priority" class="mini-list"></div>
	      </div>
	      <div>
	        <h3>Recent Backlog</h3>
	        <div id="dashboard-recent" class="mini-list"></div>
	      </div>
	    </div>
	  </section>

	  <section id="tab-feedback" class="tab-panel">
	    <h2>Verify / Feedback</h2>
	    <div class="stack">
	      <label>Title <span class="required-mark">*</span><input id="fb-title" placeholder="Feature review feedback" required aria-required="true"></label>
	      <div class="row">
	        <label style="flex:1">Related REQ/RES <span class="field-note">Optional</span><input id="fb-related" placeholder="REQ-... or RES-..."></label>
	        <label style="width:150px">Severity <span class="field-note">Optional</span>
	          <select id="fb-severity">
	            <option>normal</option>
	            <option>launch-important</option>
	            <option>launch-blocking</option>
	            <option>nit</option>
	          </select>
	        </label>
	        <label style="width:210px">Workflow origin <span class="field-note">Optional</span>
	          <select id="fb-origin">__AGENT_MESH_WORKFLOW_ORIGIN_OPTIONS__</select>
	        </label>
	      </div>
	      <label>Target or area <span class="field-note">Optional</span><input id="fb-target" placeholder="feature, route, component, or backlog id"></label>
	      <label>Notes <span class="required-mark">*</span><textarea id="fb-notes" placeholder="What you observed, expected behavior, and acceptance criteria." required aria-required="true"></textarea></label>
	      <div id="attachment-dropzone" class="dropzone" data-requires-server-gesture>
	        <div><strong>Drop screenshots or a short MOV here</strong> or use the picker.</div>
	        <div class="row" style="justify-content:center;">
	          <button type="button" class="ghost" id="pick-attachments" data-requires-server>Choose files</button>
	          <button type="button" class="ghost" id="clear-attachments">Clear attachment paths</button>
	        </div>
	        <input id="attachment-picker" type="file" accept="image/*" data-requires-server multiple hidden>
	        <div id="attachment-upload-status" class="muted">PNG/JPEG screenshots and MOV clips are supported up to 40 MB per file and 40 MB per batch. Existing references survive a failed upload.</div>
	        <div id="attachment-upload-list" class="file-list"></div>
	      </div>
	      <label>Attachment paths <span class="field-note">Optional</span><textarea id="fb-screenshots" placeholder="- .agent-mesh/attachments/screenshots/screenshot.png"></textarea></label>
	      <label>Refs <span class="field-note">Optional</span><textarea id="fb-refs" placeholder="One path, URL, REQ, RES, BKL, or decision id per line."></textarea></label>
	      <div class="row">
	        <button id="submit-feedback" data-requires-server>Submit REQ</button>
	        <button type="button" class="ghost" id="clear-feedback">Clear</button>
	        <button id="draft-feedback" data-requires-server>Draft feedback</button>
	        <button class="ghost" id="copy-feedback" data-requires-server>Copy draft</button>
	        <button class="ghost" id="copy-request" data-requires-server>Copy agent request</button>
	      </div>
	      <div class="row" id="feedback-recovery-actions" hidden>
	        <button type="button" id="retry-pending-feedback" data-requires-server>Retry earlier submission</button>
	        <button type="button" class="ghost" id="abandon-pending-feedback">Abandon earlier retry state</button>
	      </div>
	      <div id="feedback-submit-status" class="edit-status"></div>
	      <pre id="feedback-output" class="output"></pre>
	    </div>
	  </section>

	  <section id="tab-messages" class="tab-panel">
	    <div class="row">
	      <h2 style="flex:1">Messages</h2>
	      <button class="ghost" id="reload-messages" data-requires-server>Refresh</button>
	    </div>
	    <div class="dispatch-purpose-guide">
	      <p><strong>Messages is the durable record of requests and results.</strong> Use it to find and read REQ requests and RES outcomes, inspect their thread, and close or reopen a request. Use Dispatch when you want a configured AI agent to perform new work.</p>
	    </div>
	    <details class="disclosure">
	      <summary>How Messages and Dispatch work together</summary>
	      <div class="dispatch-use-cases">
	        <div><strong>Read the record</strong><span class="muted">Messages shows what was requested, what result was recorded, who participated, and when it happened.</span></div>
	        <div><strong>Continue from a result</strong><span class="muted">Open a managed RES and choose Continue in Dispatch. Agent Mesh creates a new linked REQ and preserves the prior managed context.</span></div>
	        <div><strong>Start AI work</strong><span class="muted">Dispatch freezes the new request, verifies the selected runtime, runs the agent, and records its next RES here.</span></div>
	      </div>
	    </details>
	    <div class="search-controls">
	      <label>Search messages <span class="field-note">Optional</span><input id="message-search" placeholder="REQ, RES, title, sender, text..."></label>
	      <label>Status <span class="field-note">Optional</span>
	        <select id="message-status-filter">
	          <option value="">Any</option>
	          <option value="open">Open</option>
	          <option value="closed">Closed</option>
	        </select>
	      </label>
	      <label>Kind <span class="field-note">Optional</span>
	        <select id="message-kind-filter">
	          <option value="">Any</option>
	          <option value="request">Request</option>
	          <option value="response">Response</option>
	        </select>
	      </label>
	      <label>Feature <span class="field-note">Optional</span>
	        <select id="message-feature-filter">
	          <option value="">Any</option>
	          <option value="feedback">Feedback</option>
	        </select>
	      </label>
	      <label>Workflow origin <span class="field-note">Optional</span>
	        <select id="message-origin-filter">__AGENT_MESH_WORKFLOW_ORIGIN_OPTIONS__</select>
	      </label>
	      <button id="search-messages" data-requires-server>Apply filters</button>
	      <button class="ghost" id="reset-message-filters" data-requires-server>Reset</button>
	    </div>
	    <details class="disclosure" id="message-exact-search">
	      <summary>Find by exact message ID</summary>
	      <div class="row">
	        <label style="flex:1">Message ID <span class="required-mark">*</span><input id="message-id" placeholder="REQ-... or RES-..." required aria-required="true"></label>
	        <button id="lookup-message" data-requires-server>Lookup</button>
	      </div>
	      <p class="muted">Enter any message identifier exactly as stored.</p>
	    </details>
	    <div id="message-status-panel" class="row" style="display:none">
	      <label style="width:150px">Request status
	        <select id="message-new-status">
	          <option value="closed">Closed</option>
	          <option value="open">Open</option>
	        </select>
	      </label>
	      <label style="flex:1">Reason <input id="message-status-reason" placeholder="Triaged into backlog, test request, reopened for follow-up..."></label>
	      <button id="save-message-status" data-requires-server>Save status</button>
	    </div>
	    <div id="message-edit-status" class="edit-status" role="status" aria-live="polite"></div>
	    <div class="table-wrap">
	      <table>
	        <thead><tr>
	          <th data-sort-table="messages" data-sort-key="id" aria-sort="none">Reference <span class="sort-mark"></span></th>
	          <th data-sort-table="messages" data-sort-key="created_utc" aria-sort="none">Date <span class="sort-mark"></span></th>
	          <th data-sort-table="messages" data-sort-key="status" aria-sort="none">Status <span class="sort-mark"></span></th>
	          <th data-sort-table="messages" data-sort-key="kind" aria-sort="none">Kind <span class="sort-mark"></span></th>
	          <th data-sort-table="messages" data-sort-key="feature" aria-sort="none">Feature <span class="sort-mark"></span></th>
	          <th data-sort-table="messages" data-sort-key="workflow_origin" aria-sort="none">Origin <span class="sort-mark"></span></th>
	          <th data-sort-table="messages" data-sort-key="sender" aria-sort="none">From <span class="sort-mark"></span></th>
	          <th data-sort-table="messages" data-sort-key="recipients" aria-sort="none">To <span class="sort-mark"></span></th>
	          <th data-sort-table="messages" data-sort-key="title" aria-sort="none">Title <span class="sort-mark"></span></th>
	        </tr></thead>
	        <tbody id="message-body"></tbody>
	      </table>
	    </div>
	    <div id="message-markdown" class="markdown-view" hidden></div>
	    <details class="disclosure" id="message-technical-record" hidden>
	      <summary>Technical message record</summary>
	      <pre id="message-output" class="output"></pre>
	    </details>
	  </section>

	  <section id="tab-backlog" class="tab-panel">
	    <div class="row">
	      <h2 style="flex:1">Backlog</h2>
	      <button class="ghost" id="reload-backlog" data-requires-server>Refresh</button>
	    </div>
	    <div class="search-controls">
	      <label>Search all columns <span class="field-note">Optional</span><input id="backlog-search" placeholder="BKL, title, owner, scope, ref..."></label>
	      <label>Quick filter <span class="field-note">Optional</span>
	        <select id="backlog-quick-filter">
	          <option value="">Any</option>
	          <option value="pending_user">Pending user</option>
	          <option value="urgent">Urgent</option>
	          <option value="done">Done</option>
	          <option value="in_progress">In progress</option>
	          <option value="ahead">Ahead</option>
	        </select>
	      </label>
	      <label>Status <span class="field-note">Optional</span><input id="backlog-status-filter" placeholder="open"></label>
	      <label>Lane <span class="field-note">Optional</span><input id="backlog-lane-filter" placeholder="verify"></label>
	      <label>Priority <span class="field-note">Optional</span><input id="backlog-priority-filter" placeholder="P0"></label>
	      <label>Owner <span class="field-note">Optional</span><input id="backlog-owner-filter" placeholder="human"></label>
	      <label>Type <span class="field-note">Optional</span><input id="backlog-type-filter" placeholder="bug"></label>
	      <label>Scope <span class="field-note">Optional</span><input id="backlog-scope-filter" placeholder="launch"></label>
	      <label>Wave <span class="field-note">Optional</span><input id="backlog-wave-filter" placeholder="pre-launch"></label>
	      <label>Workflow origin <span class="field-note">Optional</span>
	        <select id="backlog-origin-filter">__AGENT_MESH_WORKFLOW_ORIGIN_OPTIONS__</select>
	      </label>
	      <button id="search-backlog" data-requires-server>Apply filters</button>
	      <button class="ghost" id="clear-backlog-filters" data-requires-server>Reset</button>
	    </div>
	    <div id="backlog-edit-status" class="edit-status" role="status" aria-live="polite"></div>
		    <div class="table-wrap">
		      <table>
        <thead><tr>
          <th data-sort-table="backlog" data-sort-key="id" aria-sort="none">ID <span class="sort-mark"></span></th>
          <th data-sort-table="backlog" data-sort-key="updated_utc" aria-sort="none">Updated <span class="sort-mark"></span></th>
          <th data-sort-table="backlog" data-sort-key="status" aria-sort="none">Status <span class="sort-mark"></span></th>
          <th data-sort-table="backlog" data-sort-key="lane" aria-sort="none">Lane <span class="sort-mark"></span></th>
          <th data-sort-table="backlog" data-sort-key="priority" aria-sort="none">Priority <span class="sort-mark"></span></th>
          <th data-sort-table="backlog" data-sort-key="item_type" aria-sort="none">Type <span class="sort-mark"></span></th>
	          <th data-sort-table="backlog" data-sort-key="owner_hint" aria-sort="none">Owner <span class="sort-mark"></span></th>
	          <th data-sort-table="backlog" data-sort-key="workflow_origin" aria-sort="none">Origin <span class="sort-mark"></span></th>
          <th data-sort-table="backlog" data-sort-key="launch_scope" aria-sort="none">Scope <span class="sort-mark"></span></th>
          <th data-sort-table="backlog" data-sort-key="wave" aria-sort="none">Wave <span class="sort-mark"></span></th>
          <th data-sort-table="backlog" data-sort-key="title" aria-sort="none">Title <span class="sort-mark"></span></th>
        </tr></thead>
	        <tbody id="backlog-body"></tbody>
	      </table>
	    </div>
	    <div id="backlog-markdown" class="markdown-view" hidden></div>
	    <details class="disclosure" id="backlog-technical-record" hidden>
	      <summary>Technical backlog record</summary>
	      <pre id="backlog-output" class="output"></pre>
	    </details>
	  </section>

	  <section id="tab-dispatches" class="tab-panel">
	    <div class="row">
	      <h2 style="flex:1">Delegate work to an AI agent</h2>
	      <button class="ghost" id="reload-dispatches" data-requires-server>Refresh</button>
	    </div>
	    <div class="dispatch-purpose-guide">
	      <p><strong>Dispatch starts and tracks independent AI work.</strong> Use it when a task should run through a configured agent and leave a durable request, attempt history, and result. To follow up on a completed result, choose Continue this work; Agent Mesh creates a new linked request and preserves the prior managed context. For an ordinary question or conversation, use your agent's chat.</p>
	    </div>
	    <details class="disclosure">
	      <summary>What can I use Dispatch for?</summary>
	      <div class="dispatch-use-cases">
	        <div><strong>Implementation</strong><span class="muted">Ask an agent with edit access to make a bounded code or documentation change.</span></div>
	        <div><strong>Research</strong><span class="muted">Run a durable investigation whose result should remain linked to its request.</span></div>
	        <div><strong>Independent review</strong><span class="muted">Bind a reviewer to an exact decision, repository file, or Git change set.</span></div>
	        <div><strong>Agent-to-agent work</strong><span class="muted">Agents use the same managed path to delegate work with stable history and limits.</span></div>
	      </div>
	    </details>
	    <div class="search-controls">
	      <label>Search dispatches <span class="field-note">Optional</span><input id="dispatch-search" placeholder="policy, REQ, purpose, target, response..."></label>
	      <button id="search-dispatches">Apply filter</button>
	      <button class="ghost" id="reset-dispatch-filters">Reset</button>
	    </div>
	    <div class="table-wrap">
	      <table>
	        <thead><tr>
	          <th>Work</th><th>Status</th><th>Agent</th><th>Attempt</th><th>Outcome</th><th>Assurance</th><th>Details</th>
	        </tr></thead>
	        <tbody id="dispatch-body"></tbody>
	      </table>
	    </div>
	    <p class="muted">Select a row to inspect its details. Inspecting prior work does not retry it.</p>
	    <details class="disclosure">
	      <summary>Start or retry AI work</summary>
	    <form id="dispatch-editor" class="stack">
	      <div class="row">
	        <strong style="flex:1">Start managed work</strong>
	        <span class="badge">Durable and tracked</span>
	      </div>
	      <div class="dispatch-editor-grid">
	        <label><span>AI agent <span class="required-mark">*</span></span><select id="dispatch-agent" required aria-required="true"></select></label>
	        <label><span>Run setup <span class="required-mark">*</span></span><select id="dispatch-profile" required aria-required="true"></select></label>
	        <label><span>Started by <span class="required-mark">*</span></span><select id="dispatch-actor" required aria-required="true"></select></label>
	        <label>Type of work
	          <select id="dispatch-purpose">
	            <option value="general" selected>General task</option>
	            <option value="implementation">Implementation</option>
	            <option value="research">Research</option>
	            <option value="review">Independent review</option>
	          </select>
	        </label>
	        <label>Agent role <span class="field-note">Suggested from the work type; editable</span><input id="dispatch-role" value="worker" data-auto-role="worker" placeholder="worker"></label>
	        <fieldset class="dispatch-source-picker wide">
	          <legend>What should the agent work from?</legend>
	          <div class="dispatch-source-options">
	            <label class="dispatch-source-option"><input type="radio" name="dispatch-source-mode" value="new" checked><span><strong>New request</strong><small>Write a new title and set of instructions.</small></span></label>
	            <label class="dispatch-source-option"><input type="radio" name="dispatch-source-mode" value="existing"><span><strong>Existing request</strong><small>Run a durable request that already exists in Messages.</small></span></label>
	            <label class="dispatch-source-option"><input type="radio" name="dispatch-source-mode" value="retry"><span><strong>Retry selected work</strong><small>Reuse the exact frozen instructions and limits from a prior dispatch.</small></span></label>
	          </div>
	        </fieldset>
	        <div id="dispatch-source-new" class="dispatch-source-panel wide">
	          <label>Request title <span class="required-mark">*</span><input id="dispatch-title" placeholder="Describe the outcome you need"></label>
	          <label>Instructions <span class="required-mark">*</span><textarea id="dispatch-request-body" placeholder="Describe the result you need. Put lengthy specifications in a project-owned file and reference its path."></textarea></label>
	          <label>Workstream <span class="field-note">Optional</span><input id="dispatch-workstream" list="dispatch-workstream-options" placeholder="For example: Website accessibility refresh"></label>
	          <datalist id="dispatch-workstream-options"></datalist>
	          <p class="muted">Reuse a workstream name to continue the same supported agent context across new requests. Leave it blank to start an isolated context for this request.</p>
	        </div>
	        <div id="dispatch-source-existing" class="dispatch-source-panel wide" hidden>
	          <label>Existing request <span class="required-mark">*</span><span class="field-note">Search canonical requests</span><input id="dispatch-message-id" placeholder="Type a REQ ID or request title" autocomplete="off" required aria-required="true" aria-describedby="dispatch-request-validation"></label>
	          <p id="dispatch-request-validation" class="dispatch-request-validation" role="status" aria-live="polite">Choose Existing request to load recent requests for the selected AI agent.</p>
	          <div id="dispatch-request-matches" class="dispatch-request-matches" aria-label="Matching existing requests" hidden></div>
	        </div>
	        <div id="dispatch-source-retry" class="dispatch-selection dispatch-source-panel wide" hidden>
	          <strong>Selected prior work</strong>
	          <span id="dispatch-retry-context" class="muted">Select a prior dispatch from the table, then choose this option to reuse its frozen setup.</span>
	          <button id="clear-dispatch-retry" class="ghost table-action" type="button" hidden>Clear selected work</button>
	          <input id="dispatch-policy-id" type="hidden">
	        </div>
	        <div id="dispatch-followup-context" class="dispatch-selection wide" hidden>
	          <strong>Continuing from a prior result</strong>
	          <span id="dispatch-followup-label" class="muted"></span>
	          <button id="clear-dispatch-followup" class="ghost table-action" type="button">Clear follow-up context</button>
	          <input id="dispatch-continue-response-id" type="hidden">
	        </div>
	        <label id="dispatch-subject-type-field">Exact work target <span id="dispatch-subject-required" class="required-mark" hidden>*</span><span class="field-note">What the agent inspects, not the agent identity; required for independent review</span>
	          <select id="dispatch-subject-type">
	            <option id="dispatch-subject-none" value="">No exact target</option>
	            <option value="decision">Decision revision</option>
	            <option value="artifact">Repository file</option>
	            <option value="worktree">Current unstaged and untracked changes</option>
	            <option value="staged">Staged changes</option>
	            <option value="full">Branch and all local changes</option>
	            <option value="pr">Committed branch changes</option>
	          </select>
	        </label>
	        <label id="dispatch-subject-value-field" hidden><span id="dispatch-subject-value-label">Target</span><span id="dispatch-subject-value-help" class="field-note"></span><input id="dispatch-subject-value"></label>
	        <label id="dispatch-subject-base-field" hidden>Compare with branch <span class="field-note">Local Git reference</span><input id="dispatch-subject-base" value="main"></label>
	        <p id="dispatch-subject-explanation" class="muted wide">This work will be linked to its request and result without an exact repository target.</p>
	        <label>Attempt limit <input id="dispatch-max-attempts" type="number" min="1" max="8" value="1"></label>
	        <label>Time limit (seconds) <input id="dispatch-timeout" type="number" min="1" max="21600" value="3600"></label>
	      </div>
	      <div class="row">
	        <button id="start-dispatch" type="submit" data-requires-server>Start AI work</button>
	        <button id="cancel-dispatch" class="ghost" type="button" data-requires-server data-action-unavailable="true" disabled>Stop active run</button>
	        <span class="field-note">A managed model run can take several minutes. This page reports the bounded result when it finishes.</span>
	      </div>
	      <div id="dispatch-status" class="edit-status" role="status" aria-live="polite"></div>
	      <div id="dispatch-result-card" class="markdown-view dispatch-result-card" hidden></div>
	      <div class="dispatch-assurance-panel">
	        <div class="row">
	          <h3 style="flex:1">Derive review assurance</h3>
	          <span class="badge">review.v1</span>
	        </div>
	        <p class="muted">Agent Mesh reads the closed review envelope from the exact dispatcher-bound RES. The operator cannot restate its disposition, finding counts, role, or artifact paths.</p>
	        <div class="row">
	          <button id="record-dispatch-assurance" type="button" data-requires-server>Derive from current RES</button>
	          <span class="field-note">Select a frozen policy above first. Managed review runs derive this automatically; this action is an idempotent recovery surface.</span>
	        </div>
	        <div id="dispatch-assurance-current" class="stack" hidden></div>
	      </div>
	      <details class="disclosure">
	        <summary>Technical dispatch output</summary>
	        <pre id="dispatch-output" class="output"></pre>
	      </details>
	    </form>
	    </details>
	  </section>

	  <section id="tab-decisions" class="tab-panel">
	    <div class="row">
	      <h2 style="flex:1">Decisions</h2>
	      <button id="new-decision" data-requires-server>New decision</button>
	      <button class="ghost" id="reload-decisions" data-requires-server>Refresh</button>
	    </div>
	    <div class="decision-browser">
	    <div class="decision-list-pane">
	    <div class="search-controls">
	      <label>Search by ID or text <span class="field-note">Optional</span><input id="decision-search" placeholder="D010 or architecture"></label>
	      <label>Status <span class="field-note">Optional</span>
	        <select id="decision-status-filter">
	          <option value="">Any</option>
	          <option value="proposed">Proposed</option>
	          <option value="accepted">Accepted</option>
	          <option value="in_force">In force</option>
	          <option value="superseded">Superseded</option>
	          <option value="retired">Retired</option>
	          <option value="rejected">Rejected</option>
	        </select>
	      </label>
	      <label>Tier <span class="field-note">Optional</span><input id="decision-tier-filter" placeholder="architecture_contract"></label>
	      <button id="search-decisions" data-requires-server>Apply filters</button>
	      <button class="ghost" id="reset-decision-filters" data-requires-server>Reset</button>
	    </div>
	    <div class="row">
	      <div id="decision-list-status" class="edit-status" style="flex:1" role="status" aria-live="polite"></div>
	      <button type="button" class="ghost" id="show-all-decisions" hidden>Show all decisions</button>
	    </div>
	    <details class="disclosure">
	      <summary>Status guide</summary>
	      <div class="lifecycle-guide">
	        <div><strong>Proposed</strong><span>Awaiting a direct human approve or reject action.</span></div>
	        <div><strong>Accepted</strong><span>Human-approved note or implementation plan. It records the choice but does not participate in code enforcement.</span></div>
	        <div><strong>In force</strong><span>Human-approved architecture, production, or compliance rule whose configured advisory or required enforcement is active.</span></div>
	        <div><strong>Rejected</strong><span>Declined proposal. This record is terminal; create a successor to reconsider it.</span></div>
	        <div><strong>Superseded</strong><span>Replaced by a named successor decision.</span></div>
	        <div><strong>Retired</strong><span>Previously approved, then intentionally withdrawn.</span></div>
	      </div>
	    </details>
	    <div class="table-wrap">
	      <table>
	        <thead><tr>
	          <th data-sort-table="decisions" data-sort-key="id" aria-sort="none">ID <span class="sort-mark"></span></th>
	          <th data-sort-table="decisions" data-sort-key="decision_date" aria-sort="none">Date <span class="sort-mark"></span></th>
	          <th data-sort-table="decisions" data-sort-key="status" aria-sort="none">Status <span class="sort-mark"></span></th>
	          <th data-sort-table="decisions" data-sort-key="version" aria-sort="none">Approved version <span class="sort-mark"></span></th>
	          <th data-sort-table="decisions" data-sort-key="tier" aria-sort="none">Tier <span class="sort-mark"></span></th>
	          <th data-sort-table="decisions" data-sort-key="owner" aria-sort="none">Owner <span class="sort-mark"></span></th>
	          <th data-sort-table="decisions" data-sort-key="title" aria-sort="none">Title <span class="sort-mark"></span></th>
	        </tr></thead>
	        <tbody id="decision-body"></tbody>
	      </table>
	    </div>
	    </div>
	    <article id="decision-review" class="decision-review" hidden>
	      <div class="decision-review-heading">
	        <div>
	          <h3 id="decision-review-title">Select a decision</h3>
	          <div id="decision-review-meta" class="decision-review-meta"></div>
	        </div>
	        <div class="decision-review-badges">
	          <span id="decision-review-version" class="badge"></span>
	          <span id="decision-review-status" class="badge"></span>
	        </div>
	      </div>
	      <div id="decision-review-loading" class="edit-status" role="status" aria-live="polite"></div>
	      <div id="decision-review-summary" class="decision-high-level"></div>
	      <div id="decision-revision-diff" class="decision-revision-diff" hidden>
	        <div class="row">
	          <h3 style="flex:1">Review changes for approval</h3>
	          <button type="button" class="ghost" id="retry-decision-review" data-requires-server hidden>Retry comparison</button>
	          <span id="decision-revision-diff-badge" class="badge">comparison</span>
	        </div>
	        <div id="decision-revision-diff-status" class="edit-status" role="status" aria-live="polite"></div>
	        <div id="decision-human-diff" class="human-diff"></div>
	        <details class="disclosure">
	          <summary>Technical comparison and verification hashes</summary>
	          <dl>
	            <dt>Approved baseline</dt><dd id="decision-revision-baseline-label"></dd>
	            <dt>Approved revision hash</dt><dd id="decision-revision-baseline-sha"></dd>
	            <dt>Approved body hash</dt><dd id="decision-body-baseline-sha"></dd>
	            <dt>Pending revision hash</dt><dd id="decision-revision-pending-sha"></dd>
	            <dt>Pending body hash</dt><dd id="decision-body-pending-sha"></dd>
	          </dl>
	          <h4>Authority-bearing metadata</h4>
	          <pre id="decision-metadata-diff" class="output decision-diff-output"></pre>
	          <h4>Canonical body</h4>
	          <pre id="decision-body-diff" class="output decision-diff-output"></pre>
	        </details>
	      </div>
	      <details id="decision-version-history" class="disclosure" hidden>
	        <summary id="decision-version-history-summary">Compare saved revisions</summary>
	        <div>
	          <p id="decision-version-history-status" class="edit-status" role="status" aria-live="polite"></p>
	          <div class="decision-version-controls">
	            <label>Earlier revision<select id="decision-version-from"></select></label>
	            <label>Later revision<select id="decision-version-to"></select></label>
	          </div>
	          <div id="decision-version-comparison" class="human-diff"></div>
	        </div>
	      </details>
	      <details id="decision-canonical-details" class="disclosure">
	        <summary>Full canonical decision</summary>
	        <div id="decision-canonical-markdown" class="markdown-view"></div>
	      </details>
	      <div class="decision-actions">
	        <div class="row">
	          <button type="button" class="ghost" id="edit-decision" data-requires-server>Edit proposal</button>
	        </div>
	        <div id="decision-human-actions" hidden>
	          <div class="decision-actions-grid">
	            <label>Human identity <span class="required-mark">*</span><select id="decision-review-actor" required aria-required="true"></select></label>
	            <label>Decision note <span class="required-mark">*</span><input id="decision-approval-notes" placeholder="Why you approve or reject this exact revision"></label>
	          </div>
	          <div class="row">
	            <button type="button" id="accept-decision" data-requires-server>Approve decision</button>
	            <button type="button" class="danger" id="reject-decision" data-requires-server>Reject decision</button>
	          </div>
	        </div>
	        <div id="decision-approval-status" class="edit-status" role="status" aria-live="polite"></div>
	      </div>
	    </article>
	    </div>
	    <details class="disclosure decision-applicability-panel">
	      <summary>Technical applicability: decisions affecting local Git changes</summary>
	      <div>
	      <div class="row">
	        <h3 style="flex:1;margin:0">Decisions affecting this change</h3>
	        <span class="badge">advisory dry run</span>
	      </div>
	      <div class="search-controls">
	        <label>Git comparison
	          <select id="decision-change-mode">
	            <option value="worktree">Worktree</option>
	            <option value="staged">Staged</option>
	            <option value="full">Full branch + local</option>
	            <option value="pr">PR / branch</option>
	          </select>
	        </label>
	        <label id="decision-change-base-field" hidden>Base ref <span class="field-note">Local only</span><input id="decision-change-base" value="main"></label>
	        <button class="ghost" id="reload-decision-applicability" data-requires-server>Evaluate changed paths</button>
	      </div>
	      <div id="decision-applicability-status" class="edit-status" role="status" aria-live="polite">Changed-path context has not been evaluated.</div>
	      <div id="decision-applicability-results" class="output"></div>
	      </div>
	    </details>
	    <form id="decision-editor" class="decision-editor" hidden>
	      <div class="row">
	        <strong id="decision-editor-heading" style="flex:1">New decision</strong>
	        <span id="decision-editor-lifecycle" class="badge">proposed</span>
	      </div>
	      <div class="decision-editor-grid">
	        <label>ID <span class="field-note">Auto if blank</span><input id="decision-edit-id" placeholder="D079"></label>
	        <label>Tier <span class="required-mark">*</span>
	          <select id="decision-edit-tier" required aria-required="true">
__AGENT_MESH_DECISION_TIER_OPTIONS__
	          </select>
	        </label>
	        <label>Owner <span class="field-note">Optional</span><input id="decision-edit-owner" placeholder="human or agent"></label>
	        <label>Applicability scope <span class="required-mark">*</span>
	          <select id="decision-edit-scope" required aria-required="true">
	            <option value="manual">Manual lookup only</option>
	            <option value="paths">Matching paths</option>
	            <option value="repository">Whole repository</option>
	          </select>
	        </label>
	        <label>Acting identity <span class="required-mark">*</span><select id="decision-edit-actor" required aria-required="true"></select></label>
	        <label class="wide">Title <span class="required-mark">*</span><input id="decision-edit-title" required aria-required="true" placeholder="One durable choice"></label>
	        <label class="wide">Context <span class="field-note">Optional</span><textarea id="decision-edit-context" placeholder="Why this choice is needed"></textarea></label>
	        <label class="wide">Decision <span class="field-note">Optional</span><textarea id="decision-edit-summary" placeholder="What has been decided"></textarea></label>
	        <label class="wide">Affected code globs <span class="field-note">One per line; required for path scope</span><textarea id="decision-edit-globs" placeholder="src/package/**"></textarea></label>
	        <label class="wide">Exemption globs <span class="field-note">One per line; subtracts matching paths</span><textarea id="decision-edit-exemptions" placeholder="src/generated/**"></textarea></label>
	        <label class="wide">Generated-artifact globs <span class="field-note">One per line; subtracts generated paths</span><textarea id="decision-edit-generated" placeholder="dist/**"></textarea></label>
	        <label class="wide">Required checks <span class="field-note">One repo-owned check per line</span><textarea id="decision-edit-checks" placeholder="pytest::tests/test_contract.py"></textarea></label>
	        <label class="wide">Verification commands <span class="field-note">One argv-only command per line; no pipes, redirection, expansion, or chaining; required for architecture contracts and above</span><textarea id="decision-edit-verification" placeholder="pytest tests/test_contract.py"></textarea></label>
	        <label class="wide">Assumptions <span class="field-note">One statement per line; saved as stable A1, A2, ... records</span><textarea id="decision-edit-assumptions" placeholder="The provider preserves the reviewed protocol contract"></textarea></label>
	        <div class="wide decision-evidence-field">
	          <label>Evidence <span class="field-note">One KIND=REFERENCE entry per line</span><textarea id="decision-edit-evidence" placeholder="test_artifact=tests/test_contract.py"></textarea></label>
	          <div id="decision-evidence-links" class="decision-evidence-links" aria-label="Open decision evidence"></div>
	          <span class="field-note">REQ, RES, and BKL values are durable record IDs. They are searchable in Workbench; use the evidence buttons to open the exact record.</span>
	        </div>
	        <label class="wide">Required reviewers <span class="field-note">One configured participant per line; the set is replaced together</span><textarea id="decision-edit-reviewers" placeholder="human-reviewer"></textarea></label>
	        <label>Approval quorum <span class="field-note">Defaults to all reviewers</span><input id="decision-edit-quorum" type="number" min="1" step="1"></label>
	        <label class="wide">Tags <span class="field-note">One per line</span><textarea id="decision-edit-tags" placeholder="architecture"></textarea></label>
	        <label class="wide">Canonical Markdown body <span class="field-note">Optional; generated from the fields above when blank</span><textarea id="decision-edit-body" class="body" placeholder="# D079 — Decision title"></textarea></label>
	        <label id="decision-revision-reason-field" class="wide" hidden>Revision reason <span class="required-mark">*</span><input id="decision-revision-reason" placeholder="Why an accepted decision is being reopened"></label>
	      </div>
	      <div class="row">
	        <button type="submit" id="save-decision" data-requires-server>Save decision</button>
	        <button type="button" class="ghost" id="cancel-decision-edit">Cancel</button>
	      </div>
	      <div id="decision-edit-status" class="edit-status" role="status" aria-live="polite"></div>
	      <p class="muted" style="margin:0">Edits are appended to the event history. Editing an accepted or in-force decision requires a reason and returns it to Proposed until it is accepted again.</p>
	    </form>
	    <details class="disclosure">
	      <summary>Raw canonical record</summary>
	      <pre id="decision-output" class="output" role="status" aria-live="polite"></pre>
	    </details>
	  </section>

	  <section id="tab-kanban" class="tab-panel">
	    <div class="row">
	      <h2 style="flex:1">Kanban</h2>
	      <button class="ghost" id="reload-kanban" data-requires-server>Reload</button>
    </div>
    <div id="kanban-status" class="edit-status" role="status" aria-live="polite"></div>
    <div id="kanban" class="kanban"></div>
	  </section>
</main>
<dialog id="destructive-confirmation" class="destructive-confirmation" aria-labelledby="destructive-confirmation-title" aria-describedby="destructive-confirmation-scope destructive-confirmation-consequences">
  <form method="dialog">
    <h3 id="destructive-confirmation-title">Second confirmation required</h3>
    <p id="destructive-confirmation-scope"></p>
    <ul id="destructive-confirmation-consequences"></ul>
    <label class="acknowledgement">
      <input id="destructive-confirmation-acknowledgement" type="checkbox">
      <span>I understand the named information and recovery state will no longer be available from this Workbench copy.</span>
    </label>
    <div class="row">
      <button type="submit" class="ghost" value="cancel" id="destructive-confirmation-cancel" autofocus>Cancel</button>
      <button type="submit" value="confirm" id="destructive-confirmation-apply" disabled>Remove named information</button>
    </div>
  </form>
</dialog>
<script>
const API_BASE = __AGENT_MESH_API_BASE__;
const DEFAULT_REPO_ID = __AGENT_MESH_DEFAULT_REPO_ID__;
const EMBEDDED_API_TOKEN = __AGENT_MESH_ACCESS_TOKEN__;
const MANAGED_SERVICE = __AGENT_MESH_MANAGED_SERVICE__;
const BOOKMARK_URL = __AGENT_MESH_BOOKMARK_URL_JSON__;
const FRAGMENT_TOKEN = new URLSearchParams(window.location.hash.slice(1)).get('token') || '';
const API_TOKEN = EMBEDDED_API_TOKEN || FRAGMENT_TOKEN;
if (FRAGMENT_TOKEN) history.replaceState(null, '', window.location.pathname + window.location.search);
const $ = (id) => document.getElementById(id);
const ATTACHMENT_STATUS_DEFAULT = 'PNG/JPEG screenshots and MOV clips are supported up to 40 MB per file and 40 MB per batch. Existing references survive a failed upload.';
const FEEDBACK_PENDING_KEY = 'agent-mesh.feedback.pending.v2';
const FEEDBACK_RECEIPT_KEY = 'agent-mesh.feedback.receipt.v2';
const FEEDBACK_DRAFT_KEY = 'agent-mesh.feedback.draft.v1';
const ACTIVE_REPO_KEY = 'agent-mesh.workbench.active-repo.v1';
const STALE_BOOKMARK_REFRESH_PARAM = 'agent-mesh-stale-token-refresh';
const MAX_STALE_BOOKMARK_REFRESHES = 1;
const RESTART_ATTEMPT_PARAM = 'agent-mesh-restart-attempt';
const RESTART_HEALTH_DEADLINE_MS = 90000;
const RESTART_HEALTH_MAX_REQUESTS = 45;
const RESTART_HEALTH_REQUEST_TIMEOUT_MS = 2000;
const RESTART_REQUEST_TIMEOUT_MS = 12000;
const MAX_ATTACHMENT_BYTES = __AGENT_MESH_MAX_ATTACHMENT_BYTES__;
const MAX_ATTACHMENT_TOTAL_BYTES = __AGENT_MESH_MAX_ATTACHMENT_TOTAL_BYTES__;
const FEEDBACK_INPUT_IDS = [
  'fb-title',
  'fb-related',
  'fb-severity',
  'fb-origin',
  'fb-target',
  'fb-notes',
  'fb-refs',
  'fb-screenshots',
];
let lastDraft = null;
let activeRepoId = DEFAULT_REPO_ID;
let projectRegistryLoaded = false;
let feedbackSubmissionId = '';
const recoveringFeedbackRepositories = new Set();
let lastMessage = null;
let draggedBacklogId = null;
let messageRows = [];
let backlogRows = [];
let decisionRows = [];
let dispatchRows = [];
const expandedDispatchChains = new Set();
let dispatchFocusedPolicyId = '';
let dispatchRequestSearchTimer = null;
let dispatchRequestSearchSequence = 0;
let dispatchRequestSearchResults = [];
let confirmedDispatchRequestId = '';
let confirmedDispatchRequestTarget = '';
let activeDecision = null;
let decisionFocusedId = '';
let activeDecisionEditorSnapshot = '';
let decisionNextId = '';
let decisionDefaultActor = '';
let repoViewGeneration = 0;
let decisionApplicabilityGeneration = 0;
let decisionLookupInFlight = null;
let decisionReviewLookupInFlight = null;
let runtimeProfiles = [];
let runtimeProfileInfo = new Map();
let staleBookmarkRefreshStarted = false;
let restartRecoveryPromise = null;
let workbenchRestartRequired = false;
let workbenchRestartDetail = '';
let workbenchServerOnline = false;
let workbenchManualSuperseded = false;
const tableSort = {
  messages: { key: 'created_utc', direction: 'desc' },
  backlog: { key: 'updated_utc', direction: 'desc' },
  decisions: { key: 'decision_date', direction: 'desc' },
};

function setServerControlDisabled(target, blocked = false) {
  const element = typeof target === 'string' ? $(target) : target;
  element.disabled = workbenchRestartRequired
    || workbenchManualSuperseded
    || !workbenchServerOnline
    || blocked;
}

function serverActionAvailable() {
  return !workbenchRestartRequired && !workbenchManualSuperseded && workbenchServerOnline;
}

function setServerGestureAvailability(root = document) {
  const disabled = !serverActionAvailable();
  root.querySelectorAll('[data-requires-server-gesture]').forEach((element) => {
    element.setAttribute('aria-disabled', disabled ? 'true' : 'false');
    if (element.hasAttribute('draggable')) {
      element.setAttribute('draggable', disabled ? 'false' : 'true');
    }
  });
}

function serverGestureBlocked(element = null) {
  return !serverActionAvailable()
    || Boolean(element && element.getAttribute('aria-disabled') === 'true');
}

function setServerConnection(state, detail = '') {
  if (workbenchRestartRequired) {
    state = 'restart-required';
    detail = workbenchRestartDetail || detail;
  }
  const online = state === 'online';
  workbenchServerOnline = online;
  const indicator = $('server-connection');
  indicator.className = `connection ${state}`;
  indicator.textContent = online
    ? 'Server online - submit and live data are available.'
    : state === 'restart-required'
      ? `Workbench restart required - ${detail}`.trim()
    : state === 'offline'
      ? MANAGED_SERVICE
        ? `Server reconnecting - the automatic service will restart it. ${detail}`.trim()
        : `Server offline - start or restart the command above. ${detail}`.trim()
      : state === 'superseded'
        ? `Manual server superseded - ${detail}`.trim()
      : 'Checking Workbench server...';
  document.querySelectorAll('[data-requires-server]').forEach((button) => {
    setServerControlDisabled(button, button.id === 'repo-selector' && !projectRegistryLoaded);
  });
  setServerGestureAvailability();
}

function configureServerIdentity(payload = {}) {
  const identity = payload.identity || {};
  const mode = identity.mode || (MANAGED_SERVICE ? 'managed' : 'manual');
  $('workbench-mode').textContent = mode === 'managed'
    ? 'Managed service'
    : mode === 'manual'
      ? 'Manual fallback'
      : text(mode);
  $('workbench-endpoint').textContent = identity.endpoint || API_BASE || 'Current server page';
  $('workbench-package-root').textContent = identity.package_root || 'Unavailable';
  $('workbench-source').textContent = identity.source_checkout || 'Unavailable';
  $('workbench-anchor-repo').textContent = identity.anchor_repository || 'Unavailable';
  const superseded = payload.code === 'WORKBENCH_MANUAL_SUPERSEDED'
    || payload.server === 'manual-superseded';
  workbenchManualSuperseded = superseded;
  $('manual-superseded-panel').hidden = !superseded;
  if (superseded) {
    const authority = payload.managed_authority || {};
    const bookmarkUrl = authority.bookmark_url || '';
    const bookmarkPath = authority.bookmark_path || '';
    ['manual-superseded-link', 'open-managed-bookmark'].forEach((id) => {
      const link = $(id);
      link.hidden = !bookmarkUrl;
      if (bookmarkUrl) link.href = bookmarkUrl;
    });
    $('managed-bookmark-guidance-path').textContent = bookmarkPath
      || 'Unavailable until current ownership provides a valid private bookmark';
    const verified = authority.verified === true;
    const authorityIdentity = authority.identity || {};
    $('manual-superseded-detail').textContent = verified
      ? `Authenticated managed service at ${authorityIdentity.endpoint || authority.api_base || 'the configured endpoint'} owns this control plane. Open its private bookmark; use the direct-human \\`agent-mesh workbench service relinquish\\` action for manual access.`
      : 'Managed ownership is configured but unavailable. Run `agent-mesh workbench service status`, repair or restart it, or use the direct-human `agent-mesh workbench service relinquish` action. Health failure does not restore manual access.';
    setServerConnection('superseded', $('manual-superseded-detail').textContent);
  }
}

function staleBookmarkRefreshAttempt() {
  try {
    const raw = new URL(window.location.href).searchParams.get(STALE_BOOKMARK_REFRESH_PARAM) || '';
    const attempt = Number.parseInt(raw, 10);
    return Number.isFinite(attempt) && attempt > 0 ? attempt : 0;
  } catch (error) {
    return MAX_STALE_BOOKMARK_REFRESHES;
  }
}

function refreshManagedBookmarkAfterUnauthorized(response, payload) {
  const tokenMismatch = response.status === 403
    && payload.error === 'Missing or invalid Workbench access token';
  if (!tokenMismatch || !MANAGED_SERVICE || !BOOKMARK_URL || staleBookmarkRefreshStarted) {
    return false;
  }
  const attempt = staleBookmarkRefreshAttempt();
  if (attempt >= MAX_STALE_BOOKMARK_REFRESHES) return false;
  try {
    const refreshUrl = new URL(BOOKMARK_URL);
    refreshUrl.searchParams.set(STALE_BOOKMARK_REFRESH_PARAM, String(attempt + 1));
    if (restartAttemptRecorded()) refreshUrl.searchParams.set(RESTART_ATTEMPT_PARAM, '1');
    staleBookmarkRefreshStarted = true;
    setServerConnection('offline', 'Access changed; loading the latest private bookmark.');
    window.location.replace(refreshUrl.href);
    return true;
  } catch (error) {
    return false;
  }
}

function clearStaleBookmarkRefreshAttempt() {
  if (!MANAGED_SERVICE || staleBookmarkRefreshAttempt() === 0) return;
  try {
    const cleanUrl = new URL(window.location.href);
    cleanUrl.searchParams.delete(STALE_BOOKMARK_REFRESH_PARAM);
    history.replaceState(null, '', cleanUrl.href);
  } catch (error) {
    // Automatic recovery remains bounded even when file-page history is unavailable.
  }
}

function restartAttemptRecorded() {
  try {
    return new URL(window.location.href).searchParams.get(RESTART_ATTEMPT_PARAM) === '1';
  } catch (error) {
    return true;
  }
}

function recordRestartAttempt() {
  const restartUrl = new URL(window.location.href);
  restartUrl.searchParams.set(RESTART_ATTEMPT_PARAM, '1');
  history.replaceState(null, '', restartUrl.href);
}

function clearRestartRecoveryMarkers() {
  try {
    const cleanUrl = new URL(window.location.href);
    cleanUrl.searchParams.delete(RESTART_ATTEMPT_PARAM);
    cleanUrl.searchParams.delete(STALE_BOOKMARK_REFRESH_PARAM);
    history.replaceState(null, '', cleanUrl.href);
  } catch (error) {
    // Successful authentication is authoritative even if file-page history is unavailable.
  }
}

function restartRecoveryUnavailable(detail) {
  workbenchRestartRequired = true;
  workbenchRestartDetail = `${detail} Relaunch unverified or unavailable. Run `
    + '`agent-mesh workbench service status`; use `service restart` if installed, '
    + 'or `service install` / the displayed terminal command when needed.';
  setServerConnection('restart-required', workbenchRestartDetail);
}

function delay(milliseconds) {
  return new Promise((resolve) => window.setTimeout(resolve, milliseconds));
}

async function fetchJsonWithTimeout(url, options, timeoutMilliseconds) {
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), timeoutMilliseconds);
  try {
    const response = await fetch(url, { ...options, signal: controller.signal });
    let payload = {};
    let payloadComplete = false;
    try {
      payload = await response.json();
      payloadComplete = true;
    } catch (error) {
      payload = {};
    }
    return { response, payload, payloadComplete };
  } finally {
    window.clearTimeout(timer);
  }
}

async function pollForReplacementWorkbench() {
  const started = performance.now();
  const deadline = started + RESTART_HEALTH_DEADLINE_MS;
  for (let attempt = 0; attempt < RESTART_HEALTH_MAX_REQUESTS; attempt += 1) {
    const scheduled = started + (attempt * RESTART_HEALTH_REQUEST_TIMEOUT_MS);
    const waitMilliseconds = Math.min(scheduled, deadline) - performance.now();
    if (waitMilliseconds > 0) await delay(waitMilliseconds);
    const remaining = deadline - performance.now();
    if (remaining <= 0) break;
    try {
      const { response, payload } = await fetchJsonWithTimeout(
        `${API_BASE}/api/health`,
        { headers: { 'X-Agent-Mesh-Token': API_TOKEN } },
        Math.min(RESTART_HEALTH_REQUEST_TIMEOUT_MS, remaining),
      );
      if (refreshManagedBookmarkAfterUnauthorized(response, payload)) {
        return { reloading: true };
      }
      if (response.ok && payload.ok === true && payload.server === 'online') {
        workbenchRestartRequired = false;
        workbenchRestartDetail = '';
        clearRestartRecoveryMarkers();
        setServerConnection('online');
        return { recovered: true };
      }
      if (payload.code === 'WORKBENCH_RESTART_REQUIRED') {
        if (staleBookmarkRefreshAttempt() > 0) {
          restartRecoveryUnavailable('The replacement Workbench still reports stale code.');
          return { recovered: false };
        }
      }
    } catch (error) {
      // A missing listener is expected while the native supervisor relaunches the process.
    }
  }
  restartRecoveryUnavailable('The bounded 90-second relaunch window expired.');
  return { recovered: false };
}

async function runManagedRestartRecovery() {
  workbenchRestartRequired = true;
  workbenchRestartDetail = 'Requesting a supervised Workbench restart.';
  setServerConnection('restart-required', workbenchRestartDetail);
  if (!restartAttemptRecorded()) {
    recordRestartAttempt();
    try {
      const { response, payload, payloadComplete } = await fetchJsonWithTimeout(
        `${API_BASE}/api/service/restart`,
        {
          method: 'POST',
          headers: { 'X-Agent-Mesh-Token': API_TOKEN },
        },
        RESTART_REQUEST_TIMEOUT_MS,
      );
      if (payloadComplete && refreshManagedBookmarkAfterUnauthorized(response, payload)) {
        return { reloading: true };
      }
      if (payloadComplete) {
        const fixedFailure = {
          'WORKBENCH_RESTART_DRAIN_TIMEOUT': 'An active write did not drain within ten seconds.',
          'WORKBENCH_RESTART_NOT_REQUIRED': payload.error || 'The restart request was not required.',
          'WORKBENCH_RESTART_UNVERIFIED': payload.error || 'Restart eligibility could not be verified.',
        }[payload.code];
        if (fixedFailure) {
          restartRecoveryUnavailable(fixedFailure);
          return { recovered: false };
        }
        if (response.status === 202 && payload.code === 'WORKBENCH_RESTART_ACCEPTED') {
          workbenchRestartDetail = 'Restart accepted; waiting for the replacement.';
          setServerConnection('restart-required', workbenchRestartDetail);
          return pollForReplacementWorkbench();
        }
        if (payload.code === 'WORKBENCH_RESTART_REJECTED' && response.status !== 403) {
          restartRecoveryUnavailable(payload.error || 'The restart request was rejected.');
          return { recovered: false };
        }
      }
    } catch (error) {
      // Timeout or network failure is indeterminate after the request may have been sent.
    }
  }
  workbenchRestartDetail = 'Restart accepted or indeterminate; waiting for the replacement.';
  setServerConnection('restart-required', workbenchRestartDetail);
  return pollForReplacementWorkbench();
}

function beginManagedRestartRecovery() {
  if (!MANAGED_SERVICE) return null;
  if (!restartRecoveryPromise) {
    const recovery = runManagedRestartRecovery().catch((error) => {
      restartRecoveryUnavailable(error.message || String(error));
      return { recovered: false };
    });
    restartRecoveryPromise = recovery;
    recovery.then((result) => {
      if (result && result.recovered === true && restartRecoveryPromise === recovery) {
        restartRecoveryPromise = null;
      }
    });
  }
  return restartRecoveryPromise;
}

async function api(path, options = {}, requestRepoId = activeRepoId) {
  let requestPath = path;
  if (
    requestRepoId
    && path !== '/api/health'
    && path !== '/api/projects'
    && path !== '/api/service/restart'
  ) {
    requestPath += `${path.includes('?') ? '&' : '?'}repo=${encodeURIComponent(requestRepoId)}`;
  }
  let response;
  try {
    response = await fetch(`${API_BASE}${requestPath}`, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        'X-Agent-Mesh-Token': API_TOKEN,
        ...(options.headers || {}),
      },
    });
  } catch (cause) {
    setServerConnection('offline', 'Clear remains available; writes require the server.');
    const error = new Error(`Workbench server is offline at ${API_BASE || 'this page'}.`);
    error.networkFailure = true;
    error.cause = cause;
    throw error;
  }
  const payload = await response.json();
  if (!response.ok || payload.ok === false) {
    if (payload.code === 'WORKBENCH_MANUAL_SUPERSEDED') {
      configureServerIdentity(payload);
      const error = new Error(payload.detail || payload.error || 'Manual Workbench superseded.');
      error.payload = payload;
      error.manualSuperseded = true;
      throw error;
    }
    if (refreshManagedBookmarkAfterUnauthorized(response, payload)) {
      const error = new Error('Workbench access changed; loading the latest private bookmark.');
      error.bookmarkRecovery = true;
      throw error;
    }
    let errorMessage = payload.error || response.statusText;
    if (payload.code === 'WORKBENCH_RESTART_REQUIRED') {
      const command = payload.restart_command ? ` Run: ${payload.restart_command}` : '';
      errorMessage = `${errorMessage || 'Workbench restart required.'}${command}`;
      workbenchRestartRequired = true;
      workbenchRestartDetail = errorMessage;
      setServerConnection('restart-required', errorMessage);
      beginManagedRestartRecovery();
    }
    const error = new Error(errorMessage);
    error.payload = payload;
    throw error;
  }
  if (path === '/api/health') {
    configureServerIdentity(payload);
    if (workbenchManualSuperseded) return payload;
  }
  setServerConnection('online');
  if (path === '/api/health') clearStaleBookmarkRefreshAttempt();
  if (path === '/api/health' && restartAttemptRecorded()) clearRestartRecoveryMarkers();
  return payload;
}

function repoStorageKey(base, repoId = activeRepoId) {
  return `${base}.${repoId || 'default'}`;
}

function captureRepoRequestContext() {
  const repoId = activeRepoId;
  const generation = repoViewGeneration;
  return {
    repoId,
    generation,
    isCurrent: () => repoId === activeRepoId && generation === repoViewGeneration,
  };
}

function stampRepoElement(element) {
  element.dataset.repoId = activeRepoId;
  element.dataset.repoGeneration = String(repoViewGeneration);
}

function clearRepoElementStamp(element) {
  delete element.dataset.repoId;
  delete element.dataset.repoGeneration;
}

function repoElementIsCurrent(element) {
  return Boolean(
    element
    && element.dataset.repoId === activeRepoId
    && Number(element.dataset.repoGeneration) === repoViewGeneration
  );
}

async function loadProjects() {
  const result = await api('/api/projects');
  const selector = $('repo-selector');
  selector.innerHTML = result.projects.map((project) => (
    `<option value="${escapeHtml(project.id)}">${escapeHtml(project.name)} - ${escapeHtml(project.root)}</option>`
  )).join('');
  let stored = '';
  try {
    stored = localStorage.getItem(ACTIVE_REPO_KEY) || '';
  } catch (error) {
    stored = '';
  }
  const available = new Set(result.projects.map((project) => project.id));
  activeRepoId = available.has(stored)
    ? stored
    : available.has(DEFAULT_REPO_ID)
      ? DEFAULT_REPO_ID
      : result.default_repo_id || result.projects[0]?.id || '';
  selector.value = activeRepoId;
  projectRegistryLoaded = true;
  setServerControlDisabled(selector, !activeRepoId);
  return result.projects;
}

function resetRepoView() {
  repoViewGeneration += 1;
  decisionApplicabilityGeneration += 1;
  decisionLookupInFlight = null;
  decisionReviewLookupInFlight = null;
  lastDraft = null;
  feedbackSubmissionId = '';
  lastMessage = null;
  draggedBacklogId = null;
  messageRows = [];
  backlogRows = [];
  decisionRows = [];
  dispatchRows = [];
  dispatchFocusedPolicyId = '';
  activeDecision = null;
  decisionFocusedId = '';
  $('decision-review').hidden = true;
  $('decision-review-loading').textContent = '';
  delete $('decision-review').dataset.decisionId;
  decisionNextId = '';
  renderMessageRows([]);
  renderBacklogRows([]);
  renderDecisionRows([]);
  if (typeof renderDispatchRows === 'function') renderDispatchRows([]);
  $('kanban').innerHTML = '';
  $('kanban-status').textContent = 'Repository changed; loading the active repository.';
  $('message-id').value = '';
  $('message-new-status').value = 'closed';
  $('message-status-reason').value = '';
  $('message-status-panel').style.display = 'none';
  clearRepoElementStamp($('message-status-panel'));
  FEEDBACK_INPUT_IDS.filter((id) => id !== 'fb-severity').forEach((id) => {
    $(id).value = '';
  });
  $('fb-severity').value = 'normal';
  $('attachment-picker').value = '';
  $('attachment-upload-list').textContent = '';
  $('attachment-upload-status').textContent = ATTACHMENT_STATUS_DEFAULT;
  $('feedback-output').textContent = '';
  $('feedback-submit-status').textContent = 'Repository changed; feedback will be submitted to the active repository.';
  setFeedbackRecoveryActions(false);
  $('message-output').textContent = '';
  $('message-markdown').innerHTML = '';
  $('message-markdown').hidden = true;
  $('message-technical-record').hidden = true;
	$('dispatch-policy-id').value = '';
	$('dispatch-retry-context').textContent = 'Select a prior dispatch from the table to reuse its frozen setup.';
	$('clear-dispatch-retry').hidden = true;
	clearDispatchRequestLookup({ clearInput: true });
	$('dispatch-continue-response-id').value = '';
	$('dispatch-followup-label').textContent = '';
	$('dispatch-followup-context').hidden = true;
	setDispatchSourceMode('new');
  $('backlog-output').textContent = '';
  $('backlog-markdown').innerHTML = '';
  $('backlog-markdown').hidden = true;
  $('backlog-technical-record').hidden = true;
  $('decision-output').textContent = '';
  $('dispatch-output').textContent = '';
  $('dispatch-workstream').value = '';
  $('dispatch-result-card').innerHTML = '';
  $('dispatch-result-card').hidden = true;
  $('dispatch-status').textContent = 'Repository changed; dispatch profiles are loading.';
  $('decision-applicability-results').textContent = '';
  $('decision-applicability-status').textContent = 'Repository changed; evaluate the new Git context.';
  setServerControlDisabled('reload-decision-applicability');
  resetDecisionEditor(true);
}

function snapshotParams() {
  const params = new URLSearchParams();
  [
    ['message_q', 'message-search'],
    ['message_status', 'message-status-filter'],
    ['message_kind', 'message-kind-filter'],
    ['message_feature', 'message-feature-filter'],
    ['message_origin', 'message-origin-filter'],
    ['backlog_q', 'backlog-search'],
    ['backlog_filter', 'backlog-quick-filter'],
    ['backlog_status', 'backlog-status-filter'],
    ['backlog_lane', 'backlog-lane-filter'],
    ['backlog_priority', 'backlog-priority-filter'],
    ['backlog_owner', 'backlog-owner-filter'],
    ['backlog_type', 'backlog-type-filter'],
    ['backlog_scope', 'backlog-scope-filter'],
    ['backlog_wave', 'backlog-wave-filter'],
    ['backlog_origin', 'backlog-origin-filter'],
    ['decision_q', 'decision-search'],
    ['decision_status', 'decision-status-filter'],
    ['decision_tier', 'decision-tier-filter'],
  ].forEach(([key, id]) => {
    const value = $(id).value.trim();
    if (value) params.set(key, value);
  });
  return params;
}

async function loadSnapshot() {
  const requestContext = captureRepoRequestContext();
  const params = snapshotParams();
  const suffix = params.toString() ? `?${params.toString()}` : '';
  const result = await api(`/api/snapshot${suffix}`);
  if (!requestContext.isCurrent()) return { stale: true };
  await Promise.all([
    loadStatus(result.status),
    loadMessages({ ok: true, messages: result.messages }),
    loadBacklog({ ok: true, items: result.backlog_items }),
    loadDecisions({ ok: true, decisions: result.decisions }),
    loadDispatches({ ok: true, dispatches: result.dispatches || [] }),
    loadKanban(result.kanban),
  ]);
  return result;
}

async function switchRepo(identifier) {
  if (!identifier || identifier === activeRepoId) return;
  activeRepoId = identifier;
  try {
    localStorage.setItem(ACTIVE_REPO_KEY, identifier);
  } catch (error) {
    // The selector still works when file-page storage is unavailable.
  }
  resetRepoView();
  const requestContext = captureRepoRequestContext();
  restoreFeedbackDraft();
  await loadSnapshot();
  if (!requestContext.isCurrent()) return { stale: true };
  await recoverPendingFeedbackSubmission();
  return { stale: false };
}

async function checkServerConnection() {
  if (restartRecoveryPromise) return;
  try {
    await api('/api/health');
    if (!workbenchManualSuperseded && projectRegistryLoaded) {
      await recoverPendingFeedbackSubmission();
    }
  } catch (error) {
    if (!error.networkFailure && !error.bookmarkRecovery && !error.manualSuperseded) {
      setServerConnection('offline', error.message);
    }
  }
}

function text(value) {
  return value == null ? '' : String(value);
}

function escapeHtml(value) {
  return text(value).replace(/[&<>"']/g, (ch) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
  }[ch]));
}

function renderMarkdownInline(value) {
  let rendered = escapeHtml(value);
  rendered = rendered.replace(/`([^`]+)`/g, '<code>$1</code>');
  rendered = rendered.replace(/\\*\\*([^*]+)\\*\\*/g, '<strong>$1</strong>');
  rendered = rendered.replace(/__([^_]+)__/g, '<strong>$1</strong>');
  rendered = rendered.replace(/(^|[^*])\\*([^*]+)\\*/g, '$1<em>$2</em>');
  return rendered;
}

function renderSafeMarkdown(value) {
  const lines = text(value).replace(/\\r\\n?/g, '\\n').split('\\n');
  const output = [];
  let paragraph = [];
  let listKind = '';
  let inFence = false;
  let fenceLines = [];
  const flushParagraph = () => {
    if (!paragraph.length) return;
    output.push(`<p>${renderMarkdownInline(paragraph.join(' '))}</p>`);
    paragraph = [];
  };
  const closeList = () => {
    if (!listKind) return;
    output.push(`</${listKind}>`);
    listKind = '';
  };
  lines.forEach((line) => {
    if (/^\\s*```/.test(line)) {
      flushParagraph();
      closeList();
      if (inFence) {
        output.push(`<pre><code>${escapeHtml(fenceLines.join('\\n'))}</code></pre>`);
        fenceLines = [];
      }
      inFence = !inFence;
      return;
    }
    if (inFence) {
      fenceLines.push(line);
      return;
    }
    const heading = line.match(/^(#{1,6})\\s+(.+)$/);
    const bullet = line.match(/^\\s*[-*+]\\s+(.+)$/);
    const ordered = line.match(/^\\s*\\d+[.)]\\s+(.+)$/);
    if (heading) {
      flushParagraph();
      closeList();
      const level = heading[1].length;
      output.push(`<h${level}>${renderMarkdownInline(heading[2])}</h${level}>`);
    } else if (bullet || ordered) {
      flushParagraph();
      const nextKind = bullet ? 'ul' : 'ol';
      if (listKind !== nextKind) {
        closeList();
        listKind = nextKind;
        output.push(`<${listKind}>`);
      }
      output.push(`<li>${renderMarkdownInline((bullet || ordered)[1])}</li>`);
    } else if (!line.trim()) {
      flushParagraph();
      closeList();
    } else {
      closeList();
      paragraph.push(line.trim());
    }
  });
  if (inFence) output.push(`<pre><code>${escapeHtml(fenceLines.join('\\n'))}</code></pre>`);
  flushParagraph();
  closeList();
  return output.join('');
}

function humanDiffValue(value) {
  if (value && typeof value === 'object') return '';
  return text(value);
}

function diffSequence(left, right, maxCells) {
  if (left.length * right.length > maxCells) return null;
  const table = Array.from({ length: left.length + 1 }, () => new Uint16Array(right.length + 1));
  for (let i = left.length - 1; i >= 0; i -= 1) {
    for (let j = right.length - 1; j >= 0; j -= 1) {
      table[i][j] = left[i] === right[j]
        ? table[i + 1][j + 1] + 1
        : Math.max(table[i + 1][j], table[i][j + 1]);
    }
  }
  const parts = [];
  let i = 0;
  let j = 0;
  while (i < left.length || j < right.length) {
    if (i < left.length && j < right.length && left[i] === right[j]) {
      parts.push({ type: 'equal', text: left[i] });
      i += 1;
      j += 1;
    } else if (j < right.length && (i >= left.length || table[i][j + 1] >= table[i + 1][j])) {
      parts.push({ type: 'insert', text: right[j] });
      j += 1;
    } else {
      parts.push({ type: 'delete', text: left[i] });
      i += 1;
    }
  }
  return parts;
}

function coalesceDiffOperations(operations) {
  return operations.reduce((parts, operation) => {
    const last = parts[parts.length - 1];
    if (last && last.type === operation.type) {
      last.text += operation.text;
    } else {
      parts.push({ ...operation });
    }
    return parts;
  }, []);
}

function boundaryWhitespaceReplacementOperations(before, after) {
  const left = Array.from(before);
  const right = Array.from(after);
  let prefix = 0;
  while (
    prefix < left.length
    && prefix < right.length
    && left[prefix] === right[prefix]
    && /\\s/u.test(left[prefix])
  ) {
    prefix += 1;
  }
  let suffix = 0;
  while (
    suffix < left.length - prefix
    && suffix < right.length - prefix
    && left[left.length - suffix - 1] === right[right.length - suffix - 1]
    && /\\s/u.test(left[left.length - suffix - 1])
  ) {
    suffix += 1;
  }
  const beforeMiddle = left.slice(prefix, left.length - suffix).join('');
  const afterMiddle = right.slice(prefix, right.length - suffix).join('');
  return [
    ...(prefix ? [{ type: 'equal', text: left.slice(0, prefix).join('') }] : []),
    ...(beforeMiddle ? [{ type: 'delete', text: beforeMiddle }] : []),
    ...(afterMiddle ? [{ type: 'insert', text: afterMiddle }] : []),
    ...(suffix ? [{ type: 'equal', text: left.slice(left.length - suffix).join('') }] : []),
  ];
}

function characterReplacementOperations(before, after) {
  const left = Array.from(before);
  const right = Array.from(after);
  const operations = diffSequence(left, right, 20000);
  if (operations) {
    const sharedCharacters = operations
      .filter(operation => operation.type === 'equal')
      .reduce((total, operation) => total + Array.from(operation.text).length, 0);
    if (sharedCharacters / Math.max(left.length, right.length, 1) < 0.5) {
      return boundaryWhitespaceReplacementOperations(before, after);
    }
    return coalesceDiffOperations(operations);
  }
  let prefix = 0;
  while (prefix < left.length && prefix < right.length && left[prefix] === right[prefix]) {
    prefix += 1;
  }
  let suffix = 0;
  while (
    suffix < left.length - prefix
    && suffix < right.length - prefix
    && left[left.length - suffix - 1] === right[right.length - suffix - 1]
  ) {
    suffix += 1;
  }
  const beforeMiddle = left.slice(prefix, left.length - suffix).join('');
  const afterMiddle = right.slice(prefix, right.length - suffix).join('');
  return [
    ...(prefix ? [{ type: 'equal', text: left.slice(0, prefix).join('') }] : []),
    ...(beforeMiddle ? [{ type: 'delete', text: beforeMiddle }] : []),
    ...(afterMiddle ? [{ type: 'insert', text: afterMiddle }] : []),
    ...(suffix ? [{ type: 'equal', text: left.slice(left.length - suffix).join('') }] : []),
  ];
}

function renderFlatDiffOperations(operations) {
  return coalesceDiffOperations(operations).map((operation) => {
    if (operation.type === 'equal') return escapeHtml(operation.text);
    if (operation.type === 'delete') return `<del>${escapeHtml(operation.text)}</del>`;
    return `<ins>${escapeHtml(operation.text)}</ins>`;
  }).join('');
}

function renderCharacterReplacement(before, after) {
  return renderFlatDiffOperations(characterReplacementOperations(before, after));
}

function shouldRenderCharacterReplacement(before, after) {
  const left = before.trim();
  const right = after.trim();
  if (!left || !right) return false;
  if (/\\s/u.test(left) || /\\s/u.test(right)) return false;
  return Math.max(Array.from(left).length, Array.from(right).length) <= 64;
}

function renderReplacement(before, after) {
  if (before && after && shouldRenderCharacterReplacement(before, after)) {
    return renderCharacterReplacement(before, after);
  }
  return (before ? `<del>${escapeHtml(before)}</del>` : '')
    + (after ? `<ins>${escapeHtml(after)}</ins>` : '');
}

function renderDiffOperations(operations) {
  const chunks = coalesceDiffOperations(operations);
  const output = [];
  let removed = '';
  let inserted = '';
  const flushChange = () => {
    if (removed && inserted) {
      output.push(renderReplacement(removed, inserted));
    } else if (removed) {
      output.push(`<del>${escapeHtml(removed)}</del>`);
    } else if (inserted) {
      output.push(`<ins>${escapeHtml(inserted)}</ins>`);
    }
    removed = '';
    inserted = '';
  };
  chunks.forEach((chunk) => {
    if (chunk.type === 'equal') {
      flushChange();
      output.push(escapeHtml(chunk.text));
    } else if (chunk.type === 'delete') {
      removed += chunk.text;
    } else {
      inserted += chunk.text;
    }
  });
  flushChange();
  return output.join('');
}

function diffWordTokens(value) {
  const raw = value.match(/[\\p{L}\\p{N}_]+|[^\\s\\p{L}\\p{N}_]+|\\s+/gu) || [];
  const tokens = [];
  let leadingWhitespace = '';
  raw.forEach((token) => {
    if (/^\\s+$/u.test(token)) {
      if (tokens.length) tokens[tokens.length - 1] += token;
      else leadingWhitespace += token;
      return;
    }
    tokens.push(leadingWhitespace + token);
    leadingWhitespace = '';
  });
  if (leadingWhitespace) {
    if (tokens.length) tokens[tokens.length - 1] += leadingWhitespace;
    else tokens.push(leadingWhitespace);
  }
  return tokens;
}

function renderChangedDiffChunk(before, after) {
  const wordOperations = diffSequence(diffWordTokens(before), diffWordTokens(after), 500000);
  return wordOperations
    ? renderDiffOperations(wordOperations)
    : renderReplacement(before, after);
}

function diffTextSegments(value) {
  const segments = [];
  let start = 0;
  for (let index = 0; index < value.length; index += 1) {
    const character = value[index];
    const sentenceBoundary = '.!?;:'.includes(character)
      && (index + 1 === value.length || /\\s/u.test(value[index + 1]));
    const lineBoundary = character === '\\n';
    if (!sentenceBoundary && !lineBoundary) continue;
    let end = index + 1;
    while (end < value.length && /\\s/u.test(value[end])) end += 1;
    segments.push(value.slice(start, end));
    start = end;
    index = end - 1;
  }
  if (start < value.length) segments.push(value.slice(start));
  return segments;
}

function renderHierarchicalTextDiff(before, after) {
  const leftSegments = diffTextSegments(before);
  const rightSegments = diffTextSegments(after);
  const segmentOperations = diffSequence(leftSegments, rightSegments, 1500000);
  if (!segmentOperations) return renderReplacement(before, after);
  const output = [];
  let removed = '';
  let inserted = '';
  const flushChange = () => {
    if (removed || inserted) output.push(renderChangedDiffChunk(removed, inserted));
    removed = '';
    inserted = '';
  };
  segmentOperations.forEach((operation) => {
    if (operation.type === 'equal') {
      flushChange();
      output.push(escapeHtml(operation.text));
    } else if (operation.type === 'delete') {
      removed += operation.text;
    } else {
      inserted += operation.text;
    }
  });
  flushChange();
  return output.join('');
}

function renderInlineWordDiff(beforeValue, afterValue) {
  const before = humanDiffValue(beforeValue);
  const after = humanDiffValue(afterValue);
  if (before === after) return `<span>${renderMarkdownInline(after || 'No text')}</span>`;
  const operations = diffSequence(diffWordTokens(before), diffWordTokens(after), 4000000);
  return operations
    ? renderDiffOperations(operations)
    : renderHierarchicalTextDiff(before, after);
}

function markdownDiffMarkers(before, after) {
  let nonce = 0;
  let markers;
  do {
    const prefix = `AGENTMESHDIFFMARKER${nonce}`;
    markers = {
      deleteStart: `${prefix}DELETESTART`,
      deleteEnd: `${prefix}DELETEEND`,
      insertStart: `${prefix}INSERTSTART`,
      insertEnd: `${prefix}INSERTEND`,
    };
    nonce += 1;
  } while (
    Object.values(markers).some(marker => before.includes(marker) || after.includes(marker))
  );
  return markers;
}

function markDiffOperations(operations, markers) {
  return coalesceDiffOperations(operations).map((operation) => {
    if (operation.type === 'equal') return operation.text;
    if (operation.type === 'delete') {
      return markers.deleteStart + operation.text + markers.deleteEnd;
    }
    return markers.insertStart + operation.text + markers.insertEnd;
  }).join('');
}

function renderMarkedDiffOperations(operations, markers) {
  const chunks = coalesceDiffOperations(operations);
  const output = [];
  let removed = '';
  let inserted = '';
  const flushChange = () => {
    if (removed && inserted && shouldRenderCharacterReplacement(removed, inserted)) {
      output.push(markDiffOperations(
        characterReplacementOperations(removed, inserted),
        markers,
      ));
    } else {
      if (removed) output.push(markers.deleteStart + removed + markers.deleteEnd);
      if (inserted) output.push(markers.insertStart + inserted + markers.insertEnd);
    }
    removed = '';
    inserted = '';
  };
  chunks.forEach((chunk) => {
    if (chunk.type === 'equal') {
      flushChange();
      output.push(chunk.text);
    } else if (chunk.type === 'delete') {
      removed += chunk.text;
    } else {
      inserted += chunk.text;
    }
  });
  flushChange();
  return output.join('');
}

function renderInlineMarkdownDiff(beforeValue, afterValue) {
  const before = humanDiffValue(beforeValue);
  const after = humanDiffValue(afterValue);
  if (before === after) return renderMarkdownInline(after || 'No text');
  const markers = markdownDiffMarkers(before, after);
  const operations = diffSequence(diffWordTokens(before), diffWordTokens(after), 4000000);
  const marked = operations
    ? renderMarkedDiffOperations(operations, markers)
    : markers.deleteStart + before + markers.deleteEnd
      + markers.insertStart + after + markers.insertEnd;
  return renderMarkdownInline(marked)
    .replaceAll(markers.deleteStart, '<del>')
    .replaceAll(markers.deleteEnd, '</del>')
    .replaceAll(markers.insertStart, '<ins>')
    .replaceAll(markers.insertEnd, '</ins>');
}

function markdownDiffBlocks(value) {
  const lines = text(value).replace(/\\r\\n?/g, '\\n').split('\\n');
  const blocks = [];
  let paragraph = [];
  let inFence = false;
  let fenceLines = [];
  const flushParagraph = () => {
    if (!paragraph.length) return;
    blocks.push({ kind: 'paragraph', text: paragraph.join(' ') });
    paragraph = [];
  };
  const flushFence = () => {
    blocks.push({ kind: 'code', text: fenceLines.join('\\n') });
    fenceLines = [];
  };
  lines.forEach((line) => {
    if (/^\\s*\\x60{3}/.test(line)) {
      flushParagraph();
      if (inFence) flushFence();
      inFence = !inFence;
      return;
    }
    if (inFence) {
      fenceLines.push(line);
      return;
    }
    const heading = line.match(/^(#{1,6})\\s+(.+)$/);
    const bullet = line.match(/^\\s*[-*+]\\s+(.+)$/);
    const ordered = line.match(/^\\s*\\d+[.)]\\s+(.+)$/);
    if (heading) {
      flushParagraph();
      blocks.push({ kind: 'heading', level: heading[1].length, text: heading[2] });
    } else if (bullet || ordered) {
      flushParagraph();
      blocks.push({
        kind: 'list-item',
        listKind: bullet ? 'ul' : 'ol',
        text: (bullet || ordered)[1],
      });
    } else if (!line.trim()) {
      flushParagraph();
    } else {
      paragraph.push(line.trim());
    }
  });
  if (inFence || fenceLines.length) flushFence();
  flushParagraph();
  return blocks;
}

function markdownDiffBlockKey(block) {
  return JSON.stringify([
    block.kind,
    Number(block.level || 0),
    block.listKind || '',
    block.text,
  ]);
}

function markdownDiffBlocksCompatible(before, after) {
  return before.kind === after.kind
    && Number(before.level || 0) === Number(after.level || 0)
    && (before.listKind || '') === (after.listKind || '');
}

function markdownDiffUnit(block, changeKind = 'equal', beforeBlock = null) {
  const source = beforeBlock || block;
  let html;
  if (changeKind === 'changed') {
    html = block.kind === 'code'
      ? renderInlineWordDiff(source.text, block.text)
      : renderInlineMarkdownDiff(source.text, block.text);
  } else {
    html = block.kind === 'code'
      ? escapeHtml(block.text)
      : renderMarkdownInline(block.text);
    if (changeKind === 'delete') html = `<del>${html}</del>`;
    if (changeKind === 'insert') html = `<ins>${html}</ins>`;
  }
  return { ...block, html };
}

function renderMarkdownDiffUnits(units) {
  const output = [];
  let listKind = '';
  const closeList = () => {
    if (!listKind) return;
    output.push(`</${listKind}>`);
    listKind = '';
  };
  units.forEach((unit) => {
    if (unit.kind === 'list-item') {
      if (listKind !== unit.listKind) {
        closeList();
        listKind = unit.listKind;
        output.push(`<${listKind}>`);
      }
      output.push(`<li>${unit.html}</li>`);
      return;
    }
    closeList();
    if (unit.kind === 'heading') {
      const level = Math.max(1, Math.min(6, Number(unit.level || 1)));
      output.push(`<h${level}>${unit.html}</h${level}>`);
    } else if (unit.kind === 'code') {
      output.push(`<pre><code>${unit.html}</code></pre>`);
    } else {
      output.push(`<p>${unit.html}</p>`);
    }
  });
  closeList();
  return output.join('');
}

function renderSafeMarkdownDiff(beforeValue, afterValue) {
  const before = humanDiffValue(beforeValue);
  const after = humanDiffValue(afterValue);
  if (before === after) return renderSafeMarkdown(after || 'No text');
  const left = markdownDiffBlocks(before);
  const right = markdownDiffBlocks(after);
  const operations = diffSequence(
    left.map(markdownDiffBlockKey),
    right.map(markdownDiffBlockKey),
    250000,
  );
  if (!operations) {
    return `
      <div class="markdown-diff-fallback">
        <section><h5>Before</h5><div class="markdown-view">${renderSafeMarkdown(before || 'No text')}</div></section>
        <section><h5>After</h5><div class="markdown-view">${renderSafeMarkdown(after || 'No text')}</div></section>
      </div>
    `;
  }
  const units = [];
  let leftIndex = 0;
  let rightIndex = 0;
  let removed = [];
  let inserted = [];
  const flushChanges = () => {
    while (
      removed.length
      && inserted.length
      && markdownDiffBlocksCompatible(removed[0], inserted[0])
    ) {
      const beforeBlock = removed.shift();
      const afterBlock = inserted.shift();
      units.push(markdownDiffUnit(afterBlock, 'changed', beforeBlock));
    }
    removed.forEach(block => units.push(markdownDiffUnit(block, 'delete')));
    inserted.forEach(block => units.push(markdownDiffUnit(block, 'insert')));
    removed = [];
    inserted = [];
  };
  operations.forEach((operation) => {
    if (operation.type === 'equal') {
      flushChanges();
      units.push(markdownDiffUnit(right[rightIndex]));
      leftIndex += 1;
      rightIndex += 1;
    } else if (operation.type === 'delete') {
      removed.push(left[leftIndex]);
      leftIndex += 1;
    } else {
      inserted.push(right[rightIndex]);
      rightIndex += 1;
    }
  });
  flushChanges();
  return renderMarkdownDiffUnits(units);
}

async function withButtonFeedback(button, action, {
  busyLabel = 'Working…',
  successLabel = 'Done ✓',
  failureLabel = 'Failed',
} = {}) {
  if (!button || button.getAttribute('aria-busy') === 'true') return null;
  const idleLabel = button.dataset.feedbackIdleLabel || button.textContent.trim();
  button.dataset.feedbackIdleLabel = idleLabel;
  button.classList.remove('button-success', 'button-error');
  button.setAttribute('aria-busy', 'true');
  button.disabled = true;
  button.textContent = busyLabel;
  try {
    const result = await action();
    if (result && result.stale === true) {
      button.textContent = idleLabel;
      return result;
    }
    if (result && result.buttonFeedback === 'error') {
      button.classList.add('button-error');
      button.textContent = failureLabel;
      return result;
    }
    button.classList.add('button-success');
    button.textContent = successLabel;
    return result;
  } catch (error) {
    button.classList.add('button-error');
    button.textContent = failureLabel;
    throw error;
  } finally {
    button.removeAttribute('aria-busy');
    setServerControlDisabled(button, button.dataset.actionUnavailable === 'true');
    window.setTimeout(() => {
      if (button.getAttribute('aria-busy') === 'true') return;
      button.classList.remove('button-success', 'button-error');
      button.textContent = button.dataset.feedbackIdleLabel || idleLabel;
    }, 1000);
  }
}

function formatLocalDateTime(value) {
  const raw = text(value).trim();
  if (!raw) return '';
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return raw;
  return parsed.toLocaleString(undefined, {
    year: 'numeric',
    month: 'short',
    day: '2-digit',
    hour: 'numeric',
    minute: '2-digit',
  });
}

function sortValue(row, key) {
  if (key === 'recipients') return (row.recipients || []).join(', ').toLowerCase();
  if (key === 'wave') return text(row.wave || row.release_phase).toLowerCase();
  if (key === 'decision_date') {
    return Date.parse(row.display_utc || row.status_utc || row.proposed_utc || '') || 0;
  }
  const value = row[key];
  if (key.endsWith('_utc')) return Date.parse(value || '') || 0;
  return text(value).toLowerCase();
}

function sortedRows(table, rows) {
  const state = tableSort[table];
  const direction = state.direction === 'desc' ? -1 : 1;
  return [...rows].sort((a, b) => {
    const left = sortValue(a, state.key);
    const right = sortValue(b, state.key);
    if (left < right) return -1 * direction;
    if (left > right) return 1 * direction;
    return text(a.id).localeCompare(text(b.id));
  });
}

function renderSortIndicators(table) {
  const state = tableSort[table];
  document.querySelectorAll(`[data-sort-table="${table}"][data-sort-key]`).forEach((header) => {
    const active = header.getAttribute('data-sort-key') === state.key;
    header.setAttribute(
      'aria-sort',
      active ? (state.direction === 'desc' ? 'descending' : 'ascending') : 'none',
    );
    const marker = header.querySelector('.sort-mark');
    if (marker) marker.textContent = active ? (state.direction === 'desc' ? 'v' : '^') : '';
  });
}

function setTableSort(table, key) {
  const state = tableSort[table];
  if (!state || !key) return;
  if (state.key === key) {
    state.direction = state.direction === 'desc' ? 'asc' : 'desc';
  } else {
    state.key = key;
    state.direction = key.endsWith('_utc') || key === 'decision_date' ? 'desc' : 'asc';
  }
  if (table === 'messages') renderMessageRows(messageRows);
  if (table === 'backlog') renderBacklogRows(backlogRows);
  if (table === 'decisions') renderDecisionRows(decisionRows);
}

function titleCaseWords(value) {
  return text(value).replace(/[_-]+/g, ' ').replace(/\\b\\w/g, (letter) => letter.toUpperCase());
}

function runtimeProviderLabel(profile) {
  return ({
    openai: 'OpenAI',
    anthropic: 'Anthropic',
  }[text(profile.provider).toLowerCase()] || titleCaseWords(profile.provider || 'Configured provider'));
}

function runtimeModelLabel(profile) {
  const rawModel = text(profile.model).trim();
  return rawModel.toLowerCase() === 'opus'
    ? 'Opus'
    : (rawModel || 'configured model').replace(/^gpt(?=-)/i, 'GPT');
}

function runtimeAgentKey(profile) {
  return JSON.stringify([
    text(profile.participant).trim(),
    text(profile.provider).trim().toLowerCase(),
    text(profile.model).trim().toLowerCase(),
  ]);
}

function runtimeAgentLabel(profile) {
  const participant = titleCaseWords(profile.participant || 'Configured agent');
  return `${runtimeProviderLabel(profile)} ${runtimeModelLabel(profile)} — ${participant}`;
}

function runtimeSetupLabel(profile) {
  const role = titleCaseWords(profile.durable_role || profile.role || 'Agent');
  const access = profile.permission_mode === 'read-only'
    ? 'read-only'
    : profile.permission_mode === 'workspace-write'
      ? 'can edit'
      : 'configured access';
  const network = (profile.required_capabilities || []).includes('network')
    ? 'with network'
    : 'local only';
  const continuity = profile.resumable ? 'resumes context' : 'fresh process';
  return `${role} · ${access} · ${network} · ${continuity}`;
}

function runtimeProfileLabel(profile) {
  const profileName = text(profile.name).trim();
  return `${runtimeAgentLabel(profile)} · ${runtimeSetupLabel(profile)}${profileName ? ` [${profileName}]` : ''}`;
}

function preferredDispatchProfile(profiles, priorProfile, defaultRecipient) {
  return profiles.find(profile => profile.name === priorProfile)
    || profiles.find(profile => (
      profile.participant === defaultRecipient
      && profile.durable_role === 'primary'
      && profile.resumable
    ))
    || profiles.find(profile => profile.participant === defaultRecipient)
    || profiles[0]
    || null;
}

function syncDispatchAgent({ preferredProfile = '', announce = false } = {}) {
  const agent = $('dispatch-agent');
  const profile = $('dispatch-profile');
  const compatible = runtimeProfiles.filter(item => runtimeAgentKey(item) === agent.value);
  const priorProfile = preferredProfile || profile.value;
  profile.innerHTML = compatible.map(item => `
    <option value="${escapeHtml(item.name)}" data-participant="${escapeHtml(item.participant)}">
      ${escapeHtml(runtimeSetupLabel(item))}
    </option>
  `).join('');
  profile.value = compatible.some(item => item.name === priorProfile)
    ? priorProfile
    : (compatible[0] ? compatible[0].name : '');
  if (announce) {
    $('dispatch-status').textContent = compatible.length
      ? `${runtimeAgentLabel(compatible[0])} selected. Choose one of its compatible run setups.`
      : 'No compatible run setup is enabled for this agent.';
  }
}

async function loadStatus(statusOverride = null) {
  const requestContext = captureRepoRequestContext();
  try {
    const status = statusOverride || await api('/api/status');
    if (!requestContext.isCurrent()) return { stale: true };
    const c = status.counts;
	    const actorSelect = $('decision-edit-actor');
	    const priorActor = actorSelect.value;
	    const approvalIdentities = status.project.decision_approval_identities || [];
	    decisionDefaultActor = approvalIdentities.includes(status.project.default_sender)
	      ? status.project.default_sender
	      : approvalIdentities[0] || '';
	    actorSelect.innerHTML = approvalIdentities.map((participant) =>
	      `<option value="${escapeHtml(participant)}">${escapeHtml(participant)}</option>`
	    ).join('');
	    actorSelect.value = approvalIdentities.includes(priorActor)
	      ? priorActor
	      : decisionDefaultActor;
	    const reviewActorSelect = $('decision-review-actor');
	    const priorReviewActor = reviewActorSelect.value;
	    reviewActorSelect.innerHTML = approvalIdentities.map((participant) =>
	      `<option value="${escapeHtml(participant)}">${escapeHtml(participant)}</option>`
	    ).join('');
	    reviewActorSelect.value = approvalIdentities.includes(priorReviewActor)
	      ? priorReviewActor
	      : decisionDefaultActor;
	    const approvalIndicator = $('decision-approval-health');
	    const approvalConfigured = status.project.decision_approval_diagnosis === 'configured';
	    approvalIndicator.className = `connection ${approvalConfigured ? 'online' : 'checking'}`;
	    approvalIndicator.textContent = approvalConfigured
	      ? `Decision approvals are restricted to configured human identities: ${approvalIdentities.join(', ')}.`
	      : 'Decision approval authority is using legacy participant compatibility. Configure [decision_approval].human_approvers to separate humans from agent participants; existing approvals remain unchanged.';
	    const dispatchActor = $('dispatch-actor');
	    const priorDispatchActor = dispatchActor.value;
	    const dispatchDefaultActor = status.project.default_sender || status.project.participants[0] || '';
	    dispatchActor.innerHTML = (status.project.participants || []).map((participant) =>
	      `<option value="${escapeHtml(participant)}">${escapeHtml(participant)}</option>`
	    ).join('');
	    dispatchActor.value = (status.project.participants || []).includes(priorDispatchActor)
	      ? priorDispatchActor
	      : dispatchDefaultActor;
	    const dispatchAgent = $('dispatch-agent');
	    const dispatchProfile = $('dispatch-profile');
	    const priorProfile = dispatchProfile.value;
	    runtimeProfiles = (status.runtime_profiles || []).filter(profile => profile.enabled);
	    runtimeProfileInfo = new Map(runtimeProfiles.map(profile => [profile.name, profile]));
	    const agentProfiles = [];
	    const seenAgents = new Set();
	    runtimeProfiles.forEach((profile) => {
	      const key = runtimeAgentKey(profile);
	      if (seenAgents.has(key)) return;
	      seenAgents.add(key);
	      agentProfiles.push(profile);
	    });
	    dispatchAgent.innerHTML = agentProfiles.map((profile) => `
	      <option value="${escapeHtml(runtimeAgentKey(profile))}">
	        ${escapeHtml(runtimeAgentLabel(profile))}
	      </option>
	    `).join('');
	    const selectedProfile = preferredDispatchProfile(
	      runtimeProfiles,
	      priorProfile,
	      status.project.default_recipient,
	    );
	    dispatchAgent.value = selectedProfile ? runtimeAgentKey(selectedProfile) : '';
	    syncDispatchAgent({ preferredProfile: selectedProfile ? selectedProfile.name : '' });
	    setServerControlDisabled('start-dispatch', !runtimeProfiles.length);
	    const contract = status.agent_contract || { healthy: false, files: [], conflicts: [] };
	    const contractIndicator = $('agent-contract-health');
	    contractIndicator.className = `connection ${contract.healthy ? 'online' : 'offline'}`;
	    contractIndicator.textContent = contract.health_message || 'Agent contract status is unavailable.';
	    const terminalInstanceStatuses = new Set([
	      'completed', 'failed', 'cancelled', 'closed', 'terminal', 'aborted',
	    ]);
	    const activeInstances = (status.instances || []).filter(
	      instance => !terminalInstanceStatuses.has(text(instance.status).toLowerCase()),
	    );
	    const historicalInstances = (status.instances || []).filter(
	      instance => terminalInstanceStatuses.has(text(instance.status).toLowerCase()),
	    );
	    $('status').innerHTML = [
	      `events ${status.agent_mesh.last_event_seq}`,
      `open ${c.open_requests}`,
      `open feedback ${c.open_feedback_requests}`,
      `backlog ${c.backlog_items}`,
      `decisions ${c.decisions}`,
    ].map((item) => `<span class="badge">${escapeHtml(item)}</span>`).join('');
	    $('instance-history').innerHTML = [
	      `<strong>Agent runs</strong> ${activeInstances.length} active · ${historicalInstances.length} historical`,
	      ...activeInstances.map(instance =>
	        `${instance.handle} · ${titleCaseWords(instance.status)} · ${instance.resumable ? 'resumable' : 'fresh process'}`
	      ),
	      historicalInstances.length
	        ? `${historicalInstances.length} completed or terminal runs are hidden from the header summary.`
	        : '',
	      `Repository: ${status.project.root}`,
	      `Default sender: ${status.project.default_sender} · recipient: ${status.project.default_recipient}`,
	      `Read model: ${status.agent_mesh.db_file}`,
	    ].filter(Boolean).map(item => `<span class="badge">${escapeHtml(item)}</span>`).join('');
	    $('project-health-summary').textContent = `Project and agent details · ${activeInstances.length} active run${activeInstances.length === 1 ? '' : 's'}`;
    const s = status.snapshot;
    $('snapshot').innerHTML = [
      ['Done', s.done, 'backlog items closed or complete', '', 'done'],
      ['In progress', s.in_progress, 'active or doing lanes/statuses', '', 'in_progress'],
      ['Ahead', s.ahead, 'not done or in progress yet', '', 'ahead'],
      ['Pending user', s.pending_user, 'review, verify, pending, or user-owned', '', 'pending_user'],
      ['Urgent', s.urgent, 'P0, blocked, or launch-blocking', 'urgent', 'urgent'],
      ['Open REQs', c.open_requests, 'request threads still open', '', '', 'request', '', 'open'],
      ['Open Feedback', c.open_feedback_requests, 'feedback REQs still open', 'urgent', '', 'request', 'feedback', 'open'],
      ['Closed Feedback', c.closed_feedback_requests, 'feedback REQs already closed', '', '', 'request', 'feedback', 'closed'],
    ].map(([label, value, hint, cls, filter, messageKind, messageFeature, messageStatus]) => `
      <div class="metric ${cls}"
        ${filter || messageKind ? 'data-requires-server-gesture' : ''}
        ${filter ? `data-backlog-filter="${escapeHtml(filter)}"` : ''}
        ${messageKind ? `data-message-kind="${escapeHtml(messageKind)}" data-message-feature="${escapeHtml(messageFeature || '')}" data-message-status="${escapeHtml(messageStatus || '')}"` : ''}
        ${filter || messageKind ? 'role="button" tabindex="0"' : ''}>
        <strong>${escapeHtml(value)}</strong>
        <span>${escapeHtml(label)}</span>
        <span>${escapeHtml(hint)}</span>
      </div>
    `).join('');
    renderBreakdown('dashboard-status', s.by_status, 'status');
    renderBreakdown('dashboard-lane', s.by_lane, 'lane');
    renderBreakdown('dashboard-priority', s.by_priority, 'priority');
    renderRecentBacklog(s.recent_backlog || []);
    setServerGestureAvailability($('tab-dashboard'));
    return status;
  } catch (error) {
    if (!requestContext.isCurrent()) return { stale: true };
    $('status').innerHTML = `<span class="badge">error ${escapeHtml(error.message)}</span>`;
    $('snapshot').innerHTML = '';
    return { error };
  }
}

function renderBreakdown(targetId, values, field) {
  const entries = Object.entries(values || {});
  $(targetId).innerHTML = entries.map(([label, count]) => `
    <div data-requires-server-gesture data-backlog-field="${escapeHtml(field)}" data-backlog-value="${escapeHtml(label)}" role="button" tabindex="0">
      <span>${escapeHtml(label)}</span><strong>${escapeHtml(count)}</strong>
    </div>
  `).join('') || '<p class="muted">No items.</p>';
}

function renderRecentBacklog(items) {
  $('dashboard-recent').innerHTML = items.map((item) => `
    <div>
      <span><code>${escapeHtml(item.id)}</code> ${escapeHtml(item.title)}</span>
      <strong>${escapeHtml(item.status || item.lane || '')}</strong>
    </div>
  `).join('') || '<p class="muted">No recent backlog.</p>';
}

function feedbackPayload() {
  return {
    title: $('fb-title').value,
    related_id: $('fb-related').value,
    severity: $('fb-severity').value,
    workflow_origin: $('fb-origin').value,
    target: $('fb-target').value,
    notes: $('fb-notes').value,
    refs: $('fb-refs').value,
    screenshots: $('fb-screenshots').value,
  };
}

function newFeedbackSubmissionId() {
  if (globalThis.crypto && typeof globalThis.crypto.randomUUID === 'function') {
    return `fb-${globalThis.crypto.randomUUID()}`;
  }
  return `fb-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 14)}`;
}

function storeJson(key, value) {
  try {
    localStorage.setItem(key, JSON.stringify(value));
  } catch (error) {
    // The form remains usable when file-page storage is unavailable.
  }
}

function readJson(key) {
  try {
    const raw = localStorage.getItem(key);
    return raw ? JSON.parse(raw) : null;
  } catch (error) {
    return null;
  }
}

function removeStored(key) {
  try {
    localStorage.removeItem(key);
  } catch (error) {
    // Ignore unavailable file-page storage.
  }
}

function readStoredRaw(key) {
  return localStorage.getItem(key);
}

function parseStoredJson(raw) {
  if (raw === null) return null;
  try {
    return JSON.parse(raw);
  } catch (error) {
    return null;
  }
}

function applyStoredSnapshot(changes) {
  try {
    changes.forEach((change) => {
      if (readStoredRaw(change.key) !== change.expected) {
        throw new Error('Workbench state changed after it was shown for confirmation.');
      }
    });
  } catch (error) {
    throw new Error(`Browser storage could not be verified; nothing was cleared. ${error.message || error}`);
  }
  try {
    changes.forEach((change) => {
      if (change.replacement === null) localStorage.removeItem(change.key);
      else localStorage.setItem(change.key, change.replacement);
      if (readStoredRaw(change.key) !== change.replacement) {
        throw new Error('browser storage did not retain the requested result');
      }
    });
  } catch (error) {
    let rollbackComplete = true;
    changes.forEach((change) => {
      try {
        if (change.expected === null) localStorage.removeItem(change.key);
        else localStorage.setItem(change.key, change.expected);
        if (readStoredRaw(change.key) !== change.expected) rollbackComplete = false;
      } catch (rollbackError) {
        rollbackComplete = false;
      }
    });
    if (rollbackComplete) {
      throw new Error(`Browser storage rejected the change and was restored; nothing was cleared. ${error.message || error}`);
    }
    throw new Error('Browser storage failed during removal and could not be fully restored. The form remains visible; reload and inspect recovery state before continuing.');
  }
}

function confirmDestructiveOperation({ title, scope, consequences, confirmLabel }) {
  const dialog = $('destructive-confirmation');
  if (dialog.open) return Promise.resolve(false);
  $('destructive-confirmation-title').textContent = title;
  $('destructive-confirmation-scope').textContent = scope;
  $('destructive-confirmation-consequences').replaceChildren(...consequences.map((text) => {
    const item = document.createElement('li');
    item.textContent = text;
    return item;
  }));
  const acknowledgement = $('destructive-confirmation-acknowledgement');
  const applyButton = $('destructive-confirmation-apply');
  acknowledgement.checked = false;
  applyButton.disabled = true;
  applyButton.textContent = confirmLabel;
  dialog.returnValue = 'cancel';
  const outcome = new Promise((resolve) => {
    dialog.addEventListener('close', () => {
      resolve(dialog.returnValue === 'confirm' && acknowledgement.checked);
    }, { once: true });
  });
  dialog.showModal();
  requestAnimationFrame(() => $('destructive-confirmation-cancel').focus());
  return outcome;
}

function feedbackAttachmentPaths() {
  return $('fb-screenshots').value.split(/\\r?\\n/).map((value) => (
    value.trim().replace(/^[-*]\\s+/, '')
  )).filter(Boolean);
}

function renderAttachmentList() {
  $('attachment-upload-list').textContent = feedbackAttachmentPaths().join('\\n');
}

function rememberFeedbackDraft() {
  storeJson(repoStorageKey(FEEDBACK_DRAFT_KEY), feedbackPayload());
}

function restoreFeedbackDraft() {
  const draft = readJson(repoStorageKey(FEEDBACK_DRAFT_KEY));
  if (draft) restoreFeedbackInputs(draft);
  renderAttachmentList();
  return draft;
}

function rememberPendingFeedback(payload) {
  storeJson(repoStorageKey(FEEDBACK_DRAFT_KEY), payload);
  storeJson(repoStorageKey(FEEDBACK_PENDING_KEY), {
    submission_id: payload.submission_id,
    payload,
    started_utc: new Date().toISOString(),
  });
}

function rememberFeedbackReceipt(receipt) {
  storeJson(repoStorageKey(FEEDBACK_RECEIPT_KEY), {
    submission_id: receipt.submission_id,
    request_id: receipt.request_id,
    event_seq: receipt.event_seq,
    recorded_utc: new Date().toISOString(),
  });
}

function restoreFeedbackInputs(payload) {
  $('fb-title').value = payload.title || '';
  $('fb-related').value = payload.related_id || '';
  $('fb-severity').value = payload.severity || 'normal';
  $('fb-origin').value = payload.workflow_origin || '';
  $('fb-target').value = payload.target || '';
  $('fb-notes').value = payload.notes || '';
  $('fb-refs').value = payload.refs || '';
  $('fb-screenshots').value = payload.screenshots || '';
  renderAttachmentList();
}

function feedbackPayloadComparable(payload) {
  const source = payload || {};
  return {
    title: source.title || '',
    related_id: source.related_id || '',
    severity: source.severity || 'normal',
    workflow_origin: source.workflow_origin || '',
    target: source.target || '',
    notes: source.notes || '',
    refs: source.refs || '',
    screenshots: source.screenshots || '',
  };
}

function feedbackPayloadsMatch(left, right) {
  return JSON.stringify(feedbackPayloadComparable(left))
    === JSON.stringify(feedbackPayloadComparable(right));
}

function setFeedbackRecoveryActions(visible, { retryVisible = true } = {}) {
  $('feedback-recovery-actions').hidden = !visible;
  $('retry-pending-feedback').hidden = !retryVisible;
}

function capturePendingFeedbackRecovery() {
  const pendingKey = repoStorageKey(FEEDBACK_PENDING_KEY);
  const draftKey = repoStorageKey(FEEDBACK_DRAFT_KEY);
  let pendingRaw;
  try {
    pendingRaw = readStoredRaw(pendingKey);
  } catch (error) {
    return {
      state: 'unreadable',
      repoId: activeRepoId,
      pendingKey,
      draftKey,
      canAbandon: false,
    };
  }
  if (pendingRaw === null) {
    return {
      state: 'absent',
      repoId: activeRepoId,
      pendingKey,
      draftKey,
      pendingRaw,
      canAbandon: false,
    };
  }
  const pending = parseStoredJson(pendingRaw);
  if (
    !pending
    || typeof pending !== 'object'
    || Array.isArray(pending)
    || typeof pending.submission_id !== 'string'
    || !pending.submission_id
    || !pending.payload
    || typeof pending.payload !== 'object'
    || Array.isArray(pending.payload)
    || pending.payload.submission_id !== pending.submission_id
  ) {
    return {
      state: 'invalid',
      repoId: activeRepoId,
      pendingKey,
      draftKey,
      pendingRaw,
      pending,
      canAbandon: true,
    };
  }
  let draftRaw;
  try {
    draftRaw = readStoredRaw(draftKey);
  } catch (error) {
    return {
      state: 'unreadable',
      repoId: activeRepoId,
      pendingKey,
      draftKey,
      pendingRaw,
      pending,
      canAbandon: true,
    };
  }
  return {
    state: 'valid',
    repoId: activeRepoId,
    pendingKey,
    pendingRaw,
    pending,
    draftKey,
    draftRaw,
    canAbandon: true,
  };
}

function pendingFeedbackRecoveryIsCurrent(snapshot) {
  if (!snapshot.canAbandon || activeRepoId !== snapshot.repoId) return false;
  try {
    return readStoredRaw(snapshot.pendingKey) === snapshot.pendingRaw;
  } catch (error) {
    return false;
  }
}

function currentFeedbackRepresentsPending(snapshot) {
  if (snapshot.state !== 'valid') return false;
  try {
    const draft = parseStoredJson(readStoredRaw(snapshot.draftKey));
    return Boolean(
      draft
      && draft.submission_id === snapshot.pending.submission_id
      && feedbackPayloadsMatch(draft, snapshot.pending.payload)
      && feedbackPayloadsMatch(feedbackPayload(), snapshot.pending.payload)
    );
  } catch (error) {
    return false;
  }
}

function showPendingFeedbackRecoveryProblem(snapshot) {
  feedbackSubmissionId = '';
  if (snapshot.state === 'invalid') {
    setFeedbackRecoveryActions(true, { retryVisible: false });
    $('feedback-submit-status').textContent = 'Stored pending-submission recovery is invalid and was preserved. Retry and new submission are blocked; use the separately confirmed Abandon action or Clear to discard the exact stored marker.';
    return;
  }
  setFeedbackRecoveryActions(snapshot.canAbandon, { retryVisible: false });
  $('feedback-submit-status').textContent = snapshot.canAbandon
    ? 'Pending-submission recovery could not be fully read and was preserved. Retry and new submission are blocked; use the separately confirmed Abandon action or Clear to discard the exact pending marker.'
    : 'Browser storage could not be read. Pending-submission recovery was left untouched, and retry, abandon, and new submission are blocked until storage can be verified.';
}

function detachFeedbackSubmission() {
  feedbackSubmissionId = '';
}

function clearFeedbackInputs({ clearStored = true } = {}) {
  FEEDBACK_INPUT_IDS.filter((id) => id !== 'fb-severity').forEach((id) => {
    $(id).value = '';
  });
  $('fb-severity').value = 'normal';
  $('attachment-picker').value = '';
  renderAttachmentList();
  $('attachment-upload-status').textContent = ATTACHMENT_STATUS_DEFAULT;
  lastDraft = null;
  feedbackSubmissionId = '';
  setFeedbackRecoveryActions(false);
  if (clearStored) {
    removeStored(repoStorageKey(FEEDBACK_PENDING_KEY));
    removeStored(repoStorageKey(FEEDBACK_DRAFT_KEY));
  }
}

function captureFeedbackDestructiveState() {
  const draftKey = repoStorageKey(FEEDBACK_DRAFT_KEY);
  const pendingKey = repoStorageKey(FEEDBACK_PENDING_KEY);
  return {
    repoId: activeRepoId,
    payload: JSON.stringify(feedbackPayload()),
    submissionId: feedbackSubmissionId,
    lastDraft,
    output: $('feedback-output').textContent,
    draftKey,
    pendingKey,
    draftRaw: readStoredRaw(draftKey),
    pendingRaw: readStoredRaw(pendingKey),
  };
}

function feedbackDestructiveStateIsCurrent(snapshot) {
  return activeRepoId === snapshot.repoId
    && JSON.stringify(feedbackPayload()) === snapshot.payload
    && feedbackSubmissionId === snapshot.submissionId
    && lastDraft === snapshot.lastDraft
    && $('feedback-output').textContent === snapshot.output
    && readStoredRaw(snapshot.draftKey) === snapshot.draftRaw
    && readStoredRaw(snapshot.pendingKey) === snapshot.pendingRaw;
}

async function clearFeedbackWithConfirmation() {
  const snapshot = captureFeedbackDestructiveState();
  const hasEnteredContent = FEEDBACK_INPUT_IDS.some((id) => $(id).value.trim());
  const hasStoredRecovery = snapshot.draftRaw !== null || snapshot.pendingRaw !== null;
  const hasRenderedDraft = snapshot.lastDraft !== null || Boolean(snapshot.output);
  if (hasEnteredContent || hasStoredRecovery || hasRenderedDraft) {
    const confirmed = await confirmDestructiveOperation({
      title: 'Discard this feedback draft?',
      scope: `Active repository ${snapshot.repoId || '(unknown)'}: current form values, the browser-local draft, and any pending-submission recovery marker.`,
      consequences: [
        'Unsubmitted text and browser-local retry state will no longer be recoverable from this Workbench copy.',
        'No submitted canonical REQ or uploaded attachment file will be deleted.',
        'Browser backups, extensions, screenshots, terminal output, and other external copies are not removed.',
      ],
      confirmLabel: 'Discard draft and recovery state',
    });
    if (!confirmed) {
      $('feedback-submit-status').textContent = 'Clear cancelled; feedback draft preserved.';
      return false;
    }
  }
  if (activeRepoId !== snapshot.repoId) return false;
  if (!feedbackDestructiveStateIsCurrent(snapshot)) {
    $('feedback-submit-status').textContent = 'Clear cancelled because the repository or feedback state changed; review the current draft and retry.';
    return false;
  }
  applyStoredSnapshot([
    { key: snapshot.draftKey, expected: snapshot.draftRaw, replacement: null },
    { key: snapshot.pendingKey, expected: snapshot.pendingRaw, replacement: null },
  ]);
  clearFeedbackInputs({ clearStored: false });
  $('feedback-output').textContent = '';
  $('feedback-submit-status').textContent = 'Feedback form cleared';
  return true;
}

async function clearAttachmentPathsWithConfirmation() {
  const attachmentPaths = feedbackAttachmentPaths();
  if (!attachmentPaths.length) {
    $('attachment-picker').value = '';
    $('attachment-upload-status').textContent = ATTACHMENT_STATUS_DEFAULT;
    return true;
  }
  const snapshot = captureFeedbackDestructiveState();
  const confirmed = await confirmDestructiveOperation({
    title: 'Discard attachment paths from this draft?',
    scope: `Active repository ${snapshot.repoId || '(unknown)'}: ${attachmentPaths.length} attachment path reference(s), the browser-local draft, and any pending-submission recovery marker.`,
    consequences: [
      'The named paths will be removed from this draft and any pending-submission retry identity will be discarded.',
      'No uploaded attachment file or submitted canonical REQ will be deleted.',
      'Filesystem copies, browser backups, screenshots, and other external copies are not removed.',
    ],
    confirmLabel: 'Discard paths and recovery state',
  });
  if (!confirmed) {
    $('attachment-upload-status').textContent = 'Clear cancelled; attachment paths and recovery state preserved.';
    return false;
  }
  if (activeRepoId !== snapshot.repoId) return false;
  if (!feedbackDestructiveStateIsCurrent(snapshot)) {
    $('attachment-upload-status').textContent = 'Clear cancelled because the repository or feedback state changed; review the current draft and retry.';
    return false;
  }
  const replacementPayload = { ...feedbackPayload(), screenshots: '' };
  applyStoredSnapshot([
    {
      key: snapshot.draftKey,
      expected: snapshot.draftRaw,
      replacement: JSON.stringify(replacementPayload),
    },
    { key: snapshot.pendingKey, expected: snapshot.pendingRaw, replacement: null },
  ]);
  feedbackSubmissionId = '';
  lastDraft = null;
  setFeedbackRecoveryActions(false);
  $('fb-screenshots').value = '';
  $('attachment-picker').value = '';
  renderAttachmentList();
  $('attachment-upload-status').textContent = ATTACHMENT_STATUS_DEFAULT;
  return true;
}

async function recoverPendingFeedbackSubmission() {
  const requestContext = captureRepoRequestContext();
  if (recoveringFeedbackRepositories.has(requestContext.repoId)) return null;
  const snapshot = capturePendingFeedbackRecovery();
  if (snapshot.state === 'absent') {
    setFeedbackRecoveryActions(false);
    return null;
  }
  if (snapshot.state !== 'valid') {
    showPendingFeedbackRecoveryProblem(snapshot);
    return null;
  }
  const pending = snapshot.pending;
  recoveringFeedbackRepositories.add(requestContext.repoId);
  try {
    const receipt = await api(
      `/api/feedback/receipt?id=${encodeURIComponent(pending.submission_id)}`,
      {},
      requestContext.repoId,
    );
    if (!requestContext.isCurrent()) return { stale: true };
    if (!pendingFeedbackRecoveryIsCurrent(snapshot)) return null;
    if (!receipt.found) {
      if (currentFeedbackRepresentsPending(snapshot)) {
        feedbackSubmissionId = pending.submission_id;
        setFeedbackRecoveryActions(false);
        $('feedback-submit-status').textContent = 'Previous submission was not found; the matching draft is preserved and is safe to retry.';
      } else {
        feedbackSubmissionId = '';
        setFeedbackRecoveryActions(true);
        $('feedback-submit-status').textContent = 'An earlier submission was not found. Its retry state and your newer draft are both preserved; retry or explicitly abandon the earlier submission before submitting this draft.';
      }
      return receipt;
    }
    rememberFeedbackReceipt(receipt);
    if (currentFeedbackRepresentsPending(snapshot)) {
      applyStoredSnapshot([
        { key: snapshot.draftKey, expected: snapshot.draftRaw, replacement: null },
        { key: snapshot.pendingKey, expected: snapshot.pendingRaw, replacement: null },
      ]);
      lastDraft = receipt;
      $('feedback-output').textContent = receipt.markdown;
      clearFeedbackInputs({ clearStored: false });
      $('feedback-submit-status').textContent = `Recovered submitted ${receipt.request_id}; no duplicate was created.`;
    } else {
      applyStoredSnapshot([
        { key: snapshot.pendingKey, expected: snapshot.pendingRaw, replacement: null },
      ]);
      feedbackSubmissionId = '';
      setFeedbackRecoveryActions(false);
      $('feedback-submit-status').textContent = `Recovered submitted ${receipt.request_id}; your newer draft was preserved and no duplicate was created.`;
    }
    return receipt;
  } catch (error) {
    if (pendingFeedbackRecoveryIsCurrent(snapshot)) {
      setFeedbackRecoveryActions(!currentFeedbackRepresentsPending(snapshot));
      $('feedback-submit-status').textContent = 'Submission outcome unknown; the pending retry state and current draft are preserved and will be checked when the server reconnects.';
    }
    return null;
  } finally {
    recoveringFeedbackRepositories.delete(requestContext.repoId);
  }
}

async function retryPendingFeedbackSubmission() {
  const requestContext = captureRepoRequestContext();
  const snapshot = capturePendingFeedbackRecovery();
  if (snapshot.state === 'absent') {
    setFeedbackRecoveryActions(false);
    $('feedback-submit-status').textContent = 'No earlier pending submission remains to retry.';
    return null;
  }
  if (snapshot.state !== 'valid') {
    showPendingFeedbackRecoveryProblem(snapshot);
    return null;
  }
  const pending = snapshot.pending;
  $('feedback-submit-status').textContent = 'Retrying earlier submission while preserving the current draft...';
  let result;
  try {
    result = await api('/api/feedback/submit', {
      method: 'POST',
      body: JSON.stringify({ ...pending.payload, submission_id: pending.submission_id }),
    }, requestContext.repoId);
  } catch (error) {
    if (!requestContext.isCurrent()) return { stale: true };
    throw error;
  }
  if (!requestContext.isCurrent()) return { stale: true };
  if (!pendingFeedbackRecoveryIsCurrent(snapshot)) return result;
  applyStoredSnapshot([
    { key: snapshot.pendingKey, expected: snapshot.pendingRaw, replacement: null },
  ]);
  rememberFeedbackReceipt(result);
  feedbackSubmissionId = '';
  setFeedbackRecoveryActions(false);
  $('feedback-submit-status').textContent = `Submitted earlier draft as ${result.request_id}; your current edited draft was preserved.`;
  await loadStatus();
  return result;
}

async function abandonPendingFeedbackWithConfirmation() {
  const requestContext = captureRepoRequestContext();
  const snapshot = capturePendingFeedbackRecovery();
  if (snapshot.state === 'absent') {
    setFeedbackRecoveryActions(false);
    $('feedback-submit-status').textContent = 'No earlier pending submission remains to abandon.';
    return false;
  }
  if (!snapshot.canAbandon) {
    showPendingFeedbackRecoveryProblem(snapshot);
    return false;
  }
  const pendingLabel = snapshot.state === 'valid'
    ? `pending submission ${snapshot.pending.submission_id}`
    : 'the invalid pending-submission recovery marker';
  const confirmed = await confirmDestructiveOperation({
    title: 'Abandon this earlier submission retry state?',
    scope: `Active repository ${snapshot.repoId || '(unknown)'}: ${pendingLabel}.`,
    consequences: [
      'This Workbench copy will no longer be able to retry or automatically recover that pending submission identity.',
      'Your current edited feedback draft will be preserved.',
      'A canonical REQ may already exist if the earlier request reached the server; this action does not delete it or any external copy.',
    ],
    confirmLabel: 'Abandon earlier retry state',
  });
  if (!confirmed) {
    if (!requestContext.isCurrent()) return false;
    $('feedback-submit-status').textContent = 'Abandon cancelled; earlier retry state and current draft preserved.';
    return false;
  }
  if (!requestContext.isCurrent()) return false;
  if (!pendingFeedbackRecoveryIsCurrent(snapshot)) {
    $('feedback-submit-status').textContent = 'Abandon cancelled because the repository or pending state changed; current feedback was preserved.';
    return false;
  }
  applyStoredSnapshot([
    { key: snapshot.pendingKey, expected: snapshot.pendingRaw, replacement: null },
  ]);
  if (
    snapshot.state === 'valid'
    && feedbackSubmissionId === snapshot.pending.submission_id
  ) feedbackSubmissionId = '';
  setFeedbackRecoveryActions(false);
  $('feedback-submit-status').textContent = 'Earlier retry state abandoned; current edited draft preserved.';
  return true;
}

async function draftFeedback() {
  const requestContext = captureRepoRequestContext();
  const payload = feedbackPayload();
  let result;
  try {
    result = await api(
      '/api/feedback/draft',
      { method: 'POST', body: JSON.stringify(payload) },
      requestContext.repoId,
    );
  } catch (error) {
    if (!requestContext.isCurrent()) return { stale: true };
    throw error;
  }
  if (!requestContext.isCurrent()) return { stale: true };
  lastDraft = result;
  $('feedback-output').textContent = lastDraft.markdown;
  $('feedback-submit-status').textContent = 'Draft updated';
  return lastDraft;
}

async function ensureDraft() {
  return await draftFeedback();
}

async function submitFeedback() {
  const requestContext = captureRepoRequestContext();
  const unresolvedPending = capturePendingFeedbackRecovery();
  if (unresolvedPending.state === 'invalid' || unresolvedPending.state === 'unreadable') {
    showPendingFeedbackRecoveryProblem(unresolvedPending);
    return null;
  }
  const missing = [];
  if (!$('fb-title').value.trim()) missing.push('Title');
  if (!$('fb-notes').value.trim()) missing.push('Notes');
  if (missing.length) {
    const requirement = missing.length === 1 ? `${missing[0]} is required` : `${missing.join(' and ')} are required`;
    $('feedback-submit-status').textContent = `${requirement} to submit feedback.`;
    return null;
  }
  if (
    unresolvedPending.state === 'valid'
    && unresolvedPending.pending.submission_id !== feedbackSubmissionId
  ) {
    $('feedback-submit-status').textContent = 'A previous submission outcome is unresolved. Reconnect or reload to recover it before submitting this edited draft; no recovery state was removed.';
    return null;
  }
  $('feedback-submit-status').textContent = 'Submitting REQ...';
  if (!feedbackSubmissionId) feedbackSubmissionId = newFeedbackSubmissionId();
  const payload = { ...feedbackPayload(), submission_id: feedbackSubmissionId };
  rememberPendingFeedback(payload);
  let result;
  try {
    result = await api('/api/feedback/submit', {
      method: 'POST',
      body: JSON.stringify(payload),
    }, requestContext.repoId);
  } catch (error) {
    if (!requestContext.isCurrent()) return { stale: true };
    throw error;
  }
  if (!requestContext.isCurrent()) return { stale: true };
  lastDraft = result;
  $('feedback-output').textContent = result.markdown;
  rememberFeedbackReceipt(result);
  clearFeedbackInputs();
  $('feedback-submit-status').textContent = result.reused
    ? `Recovered submitted ${result.request_id}; no duplicate was created.`
    : `Submitted ${result.request_id}; relay this ID to the agent.`;
  await loadStatus();
  return result;
}

async function lookupMessage() {
  const id = $('message-id').value.trim();
  if (!id) return;
  const requestContext = captureRepoRequestContext();
  const result = await api(`/api/message?id=${encodeURIComponent(id)}`);
  if (!requestContext.isCurrent()) return { stale: true };
  renderMessageDetail(result);
  return result;
}

async function loadMessages(resultOverride = null) {
  const requestContext = captureRepoRequestContext();
  const params = new URLSearchParams();
  const query = $('message-search').value.trim();
  const status = $('message-status-filter').value.trim();
  const kind = $('message-kind-filter').value.trim();
  const feature = $('message-feature-filter').value.trim();
  const origin = $('message-origin-filter').value.trim();
  if (query) params.set('q', query);
  if (status) params.set('status', status);
  if (kind) params.set('kind', kind);
  if (feature) params.set('feature', feature);
  if (origin) params.set('origin', origin);
  const suffix = params.toString() ? `?${params.toString()}` : '';
  const result = resultOverride || await api(`/api/messages${suffix}`);
  if (!requestContext.isCurrent()) return { stale: true };
  messageRows = result.messages;
  renderMessageRows(messageRows);
  $('message-edit-status').textContent = `Showing ${result.messages.length} message(s)${describeMessageFilters(params)}`;
  return result;
}

function renderMessageRows(messages) {
  const rows = sortedRows('messages', messages);
  renderSortIndicators('messages');
  $('message-body').innerHTML = rows.map((message) => `
    <tr data-requires-server-gesture data-message-id="${escapeHtml(message.id)}" data-repo-id="${escapeHtml(activeRepoId)}" data-repo-generation="${repoViewGeneration}">
      <td><span class="message-reference">${escapeHtml(messageDisplayReference(message))}</span></td>
      <td>${escapeHtml(formatLocalDateTime(message.created_utc))}</td>
      <td>${escapeHtml(message.status)}</td>
      <td>${escapeHtml(message.kind)}</td>
      <td>${escapeHtml(message.feature)}</td>
      <td>${escapeHtml(message.workflow_origin || '')}</td>
      <td>${escapeHtml(message.sender)}</td>
      <td>${escapeHtml((message.recipients || []).join(', '))}</td>
      <td>${escapeHtml(message.title)}</td>
    </tr>
  `).join('') || '<tr><td colspan="9" class="muted">No messages.</td></tr>';
  setServerGestureAvailability($('message-body'));
}

function renderMessageDetail(result) {
  lastMessage = result;
  const message = (result.packet || {}).message || {};
  const originatingDispatch = dispatchRows.find(
    item => item.output_message_id === message.id && item.status === 'completed',
  );
  $('message-id').value = result.id || message.id || $('message-id').value.trim();
  if (message.id) {
    messageRows = [messageRowFromPacket(message)];
    renderMessageRows(messageRows);
    $('message-edit-status').textContent = `Exact match: ${messageDisplayReference(message)}`;
  }
  $('message-output').textContent = result.block;
  $('message-markdown').hidden = false;
  $('message-markdown').innerHTML = [
    `<h3>${escapeHtml(message.title || message.summary || messageDisplayReference(message))}</h3>`,
    `<p class="decision-review-meta">${escapeHtml([
      messageDisplayReference(message),
      titleCaseWords(message.kind),
      message.sender ? `From ${message.sender}` : '',
      formatLocalDateTime(message.created_utc),
    ].filter(Boolean).join(' · '))}</p>`,
    originatingDispatch ? `<div class="row"><button type="button" class="ghost" data-requires-server data-message-continue-response="${escapeHtml(message.id)}">Continue in Dispatch</button><span class="field-note">Creates a new linked request and preserves this managed context.</span></div>` : '',
    renderSafeMarkdown(message.body || ''),
  ].join('');
  $('message-markdown').querySelectorAll('[data-requires-server]').forEach((control) => {
    setServerControlDisabled(control);
  });
  setServerGestureAvailability($('message-markdown'));
  $('message-technical-record').hidden = false;
  if (message.kind === 'request') {
    stampRepoElement($('message-status-panel'));
    $('message-status-panel').style.display = 'flex';
    $('message-new-status').value = message.status === 'closed' ? 'open' : 'closed';
    $('message-status-reason').value = '';
  } else {
    $('message-status-panel').style.display = 'none';
    clearRepoElementStamp($('message-status-panel'));
  }
}

function messageRowFromPacket(message) {
  return {
    id: message.id || '',
    request_id: message.request_id || '',
    kind: message.kind || '',
    created_utc: message.created_utc || '',
    status: message.status || '',
    feature: message.feature || '',
    workflow_origin: message.workflow_origin || '',
    sender: message.sender || '',
    recipients: message.recipients || [],
    title: message.title || message.summary || '',
  };
}

function messageDisplayReference(message = {}) {
  return text(message.id).trim();
}

function describeMessageFilters(params) {
  const entries = Array.from(params.entries());
  if (!entries.length) return '';
  return ` for ${entries.map(([key, value]) => `${key}=${value}`).join(', ')}`;
}

async function loadDispatches(resultOverride = null) {
  const requestContext = captureRepoRequestContext();
  const result = resultOverride || await api('/api/dispatches');
  if (!requestContext.isCurrent()) return { stale: true };
  dispatchRows = result.dispatches || [];
  const summary = renderDispatchRows(dispatchRows);
  $('dispatch-status').textContent = `Showing ${summary.chains} work chain${summary.chains === 1 ? '' : 's'} containing ${summary.records} dispatch record${summary.records === 1 ? '' : 's'}.`;
  return result;
}

function dispatchMatchesQuery(item, query) {
  if (!query) return true;
  return [
    item.policy_id,
    item.request_id,
    item.request_title,
    item.purpose,
    item.role,
    item.participant,
    item.runtime_profile,
    item.workstream,
    item.status,
    item.output_message_id,
    item.continuation_response_id,
  ].some(value => String(value || '').toLowerCase().includes(query));
}

function dispatchChainGroups(dispatches, query) {
  const rows = dispatches || [];
  const byPolicy = new Map(rows.map(item => [item.policy_id, item]));
  const membersByRoot = new Map();
  rows.forEach((item) => {
    const rootId = byPolicy.has(item.root_policy_id) ? item.root_policy_id : item.policy_id;
    if (!membersByRoot.has(rootId)) membersByRoot.set(rootId, []);
    membersByRoot.get(rootId).push(item);
  });
  return Array.from(membersByRoot.entries()).map(([rootId, members]) => {
    const root = byPolicy.get(rootId) || members[0];
    const followups = members
      .filter(item => item.policy_id !== root.policy_id)
      .sort((left, right) => String(left.frozen_utc || '').localeCompare(String(right.frozen_utc || '')));
    return { root, followups, members };
  }).filter(group => group.members.some(item => dispatchMatchesQuery(item, query)))
    .sort((left, right) => String(right.root.chain_latest_utc || right.root.frozen_utc || '')
      .localeCompare(String(left.root.chain_latest_utc || left.root.frozen_utc || '')));
}

function renderDispatchRecordRow(item, { isFollowUp = false, followupCount = 0, expanded = false } = {}) {
  const chainId = item.root_policy_id || item.policy_id;
  const chainControl = !isFollowUp && followupCount ? `
    <button type="button" class="ghost table-action" data-dispatch-chain-toggle="${escapeHtml(chainId)}"
      aria-expanded="${expanded ? 'true' : 'false'}">${expanded ? 'Hide' : 'Show'} ${followupCount} follow-up${followupCount === 1 ? '' : 's'}</button>` : '';
  const relationship = isFollowUp
    ? `<span class="dispatch-chain-tag">↳ Follow-up ${escapeHtml(item.continuation_depth || 1)}</span><br>`
    : followupCount
      ? `<span class="dispatch-chain-tag">Originating work · ${followupCount} follow-up${followupCount === 1 ? '' : 's'}</span><br>`
      : '';
  const latest = !isFollowUp && followupCount && item.chain_latest_utc
    ? `<br><span class="muted">Latest activity ${escapeHtml(formatLocalDateTime(item.chain_latest_utc))}</span>`
    : '';
  const requestReference = `<div class="dispatch-public-reference"><span>${isFollowUp ? 'REQ' : 'Originating REQ'}</span><code>${escapeHtml(item.request_id)}</code></div>`;
  const responseReference = item.output_message_id
    ? `<div class="dispatch-public-reference"><span>RES</span><code>${escapeHtml(item.output_message_id)}</code></div>`
    : '<div class="dispatch-public-reference muted"><span>No RES recorded</span></div>';
  const responseActions = item.status === 'completed' && item.output_message_id ? `
        <div class="stack compact-stack"><button type="button" class="ghost table-action" data-requires-server
          data-dispatch-message="${escapeHtml(item.output_message_id)}">Open response</button>
        <button type="button" class="ghost table-action" data-requires-server
          data-dispatch-continue-response="${escapeHtml(item.output_message_id)}">Continue this work</button></div>` : `<span class="muted">${escapeHtml(item.terminal_code || 'Pending')}</span>`;
  const rowClasses = [
    item.policy_id === dispatchFocusedPolicyId ? 'dispatch-row-selected' : '',
    isFollowUp ? 'dispatch-followup-row' : '',
  ].filter(Boolean).join(' ');
  return `
    <tr class="${rowClasses}" data-requires-server-gesture data-dispatch-policy="${escapeHtml(item.policy_id)}"
      data-dispatch-chain="${escapeHtml(chainId)}" data-repo-id="${escapeHtml(activeRepoId)}" data-repo-generation="${repoViewGeneration}">
	      <td>${relationship}<strong>${escapeHtml(item.request_title || `${titleCaseWords(item.purpose)} work`)}</strong>
	        ${requestReference}<span class="muted">Submitted ${escapeHtml(formatLocalDateTime(item.frozen_utc))}</span>${latest}<br>
	        <div class="dispatch-chain-controls"><button type="button" class="ghost table-action" data-requires-server
	          data-dispatch-message="${escapeHtml(item.request_id)}">Open request</button>${chainControl}</div></td>
      <td>${escapeHtml(titleCaseWords(item.status))}</td>
      <td>${escapeHtml(item.participant)}<br><span class="muted">${escapeHtml(runtimeProfileLabel(runtimeProfileInfo.get(item.runtime_profile) || {
        durable_role: item.role,
        permission_mode: '',
        required_capabilities: [],
      }))}</span></td>
      <td>${escapeHtml(item.attempt_number || 0)} / ${escapeHtml(item.maximum_attempts || 0)}
        ${item.retry_exhausted && item.status !== 'completed' ? '<br><span class="badge">retry exhausted</span>' : ''}
        </td>
	      <td>${responseReference}${responseActions}</td>
      <td>${renderDispatchAssuranceSummary(item)}</td>
      <td><details class="dispatch-audit">
	        <summary>Audit IDs</summary>
	        <dl>
	          <dt>Policy</dt><dd><code>${escapeHtml(item.policy_id)}</code></dd>
	          <dt>Attempt</dt><dd><code>${escapeHtml(item.attempt_id || 'not started')}</code></dd>
          ${item.continuation_response_id ? `<dt>Continues</dt><dd><code>${escapeHtml(item.continuation_response_id)}</code></dd>` : ''}
          <dt>Profile</dt><dd><code>${escapeHtml(item.runtime_profile)}</code></dd>
          <dt>Workstream</dt><dd>${escapeHtml(item.workstream || 'isolated request context')}</dd>
        </dl>
      </details></td>
    </tr>`;
}

function renderDispatchRows(dispatches) {
  const query = $('dispatch-search').value.trim().toLowerCase();
  const groups = dispatchChainGroups(dispatches, query);
  const rendered = [];
  let visibleRecordCount = 0;
  const totalRecordCount = groups.reduce((total, group) => total + group.members.length, 0);
  groups.forEach((group) => {
    const focused = group.members.some(item => item.policy_id === dispatchFocusedPolicyId);
    const expanded = Boolean(query) || focused || expandedDispatchChains.has(group.root.policy_id);
    rendered.push(renderDispatchRecordRow(group.root, {
      followupCount: group.followups.length,
      expanded,
    }));
    visibleRecordCount += 1;
    if (expanded) {
      group.followups.forEach((item) => {
        rendered.push(renderDispatchRecordRow(item, { isFollowUp: true }));
        visibleRecordCount += 1;
      });
    }
  });
  $('dispatch-body').innerHTML = rendered.join('') || '<tr><td colspan="7" class="muted">No dispatch policies.</td></tr>';
  $('dispatch-body').querySelectorAll('[data-requires-server]').forEach((control) => {
    setServerControlDisabled(control);
  });
  setServerGestureAvailability($('dispatch-body'));
  const workstreams = Array.from(new Set(
    (dispatches || []).map(item => text(item.workstream).trim()).filter(Boolean),
  )).sort((a, b) => a.localeCompare(b));
  $('dispatch-workstream-options').innerHTML = workstreams
    .map(value => `<option value="${escapeHtml(value)}"></option>`).join('');
  return {
    chains: groups.length,
    records: totalRecordCount,
    visibleRecords: visibleRecordCount,
  };
}

function renderDispatchResult(response, dispatch) {
  const panel = $('dispatch-result-card');
  const message = ((response || {}).packet || {}).message || {};
  if (!message.id) {
    panel.hidden = true;
    panel.innerHTML = '';
    return;
  }
  panel.hidden = false;
  panel.innerHTML = [
    '<div class="row">',
    '<div style="flex:1">',
    '<h3>Response received</h3>',
    `<p class="decision-review-meta">${escapeHtml([
      messageDisplayReference(message),
      message.sender ? `From ${message.sender}` : '',
      formatLocalDateTime(message.created_utc),
      dispatch && dispatch.workstream ? `Workstream ${dispatch.workstream}` : '',
    ].filter(Boolean).join(' · '))}</p>`,
    '</div>',
    '<div class="row">',
    `<button type="button" class="ghost" data-requires-server data-dispatch-continue-response="${escapeHtml(message.id)}">Continue this work</button>`,
    `<button type="button" class="ghost" data-requires-server data-dispatch-result-message="${escapeHtml(message.id)}">Open in Messages</button>`,
    '</div>',
    '</div>',
    renderSafeMarkdown(message.body || ''),
  ].join('');
  panel.querySelectorAll('[data-requires-server]').forEach((control) => {
    setServerControlDisabled(control);
  });
  setServerGestureAvailability(panel);
  panel.scrollIntoView({ block: 'nearest' });
}

function renderDispatchAssuranceSummary(item) {
  const states = item.assurance_states || {};
  const labels = ['active', 'flagged', 'superseded', 'retired']
    .filter(state => Number(states[state] || 0) > 0)
    .map(state => `${titleCaseWords(state)} ${states[state]}`);
  return labels.length
    ? labels.map(label => `<span class="badge">${escapeHtml(label)}</span>`).join(' ')
    : (item.assurance_count ? `${escapeHtml(item.assurance_count)} recorded` : 'Not recorded');
}

function renderDispatchAssurances(result) {
  const panel = $('dispatch-assurance-current');
  const assurances = (result && result.assurances) || [];
  if (!assurances.length) {
    panel.hidden = true;
    panel.innerHTML = '';
    return;
  }
  panel.hidden = false;
  panel.innerHTML = assurances.map((item) => {
    const lifecycle = item.lifecycle || {
      response_id: '', state: 'active', version: 1, reason_code: '', note: '',
      actor: '', occurred_utc: '', replacement_response_id: '',
    };
    const responseId = lifecycle.response_id || '';
    const canFlag = lifecycle.state === 'active';
    const canRetire = lifecycle.state === 'flagged';
    const replacement = lifecycle.replacement_response_id
      ? `<p><strong>Replacement:</strong> ${escapeHtml(lifecycle.replacement_response_id)}</p>`
      : '';
    const reason = lifecycle.reason_code
      ? `<p><strong>Reason:</strong> ${escapeHtml(titleCaseWords(lifecycle.reason_code))}${lifecycle.note ? ` — ${escapeHtml(lifecycle.note)}` : ''}</p>`
      : '';
    const actions = (canFlag || canRetire) ? `
      <div class="dispatch-assurance-action" data-assurance-response="${escapeHtml(responseId)}"
        data-assurance-version="${escapeHtml(lifecycle.version)}">
        <label>Reason
          <select data-assurance-reason>
            <option value="materially_inaccurate_claim">Materially inaccurate claim</option>
            <option value="stale_decision_citation">Stale decision citation</option>
            <option value="artifact_invalid">Artifact invalid</option>
            <option value="independence_contested">Independence contested</option>
            <option value="subject_binding_contested">Subject binding contested</option>
            <option value="human_retirement">Human retirement</option>
            <option value="other">Other</option>
          </select>
        </label>
        <label style="flex:1">Explanation <input data-assurance-note placeholder="Why this evidence must no longer satisfy a gate"></label>
        ${canFlag ? '<button type="button" data-requires-server data-assurance-action="flag">Flag evidence</button>' : ''}
        ${canRetire ? '<button type="button" class="danger" data-requires-server data-assurance-action="retire">Retire without replacement</button>' : ''}
      </div>` : '';
    return `<article class="dispatch-assurance-card">
      <div class="row"><strong style="flex:1">${escapeHtml(responseId || 'Recorded review response')}</strong>
        ${responseId ? `<button type="button" class="ghost table-action" data-assurance-message="${escapeHtml(responseId)}">Open response</button>` : ''}
        <span class="badge">${escapeHtml(titleCaseWords(lifecycle.state))} · v${escapeHtml(lifecycle.version)}</span></div>
      <p>${escapeHtml(titleCaseWords(item.disposition))} review evidence. ${escapeHtml(
        lifecycle.state === 'active'
          ? 'This evidence may satisfy its configured gate.'
          : 'This evidence remains in history but cannot satisfy a gate.'
      )}</p>
      ${reason}${replacement}
      ${lifecycle.state === 'flagged' ? '<p class="muted">Retry this selected review to obtain a reviewer-authored replacement; the replacement response is bound to this lineage automatically.</p>' : ''}
      ${actions}
    </article>`;
  }).join('');
  panel.querySelectorAll('[data-requires-server]').forEach(control => setServerControlDisabled(control));
}

async function lookupDispatch(policyId) {
  const requestContext = captureRepoRequestContext();
  const result = await api(`/api/dispatch?id=${encodeURIComponent(policyId)}`);
  if (!requestContext.isCurrent()) return { stale: true };
  dispatchFocusedPolicyId = policyId;
  renderDispatchRows(dispatchRows);
  $('dispatch-output').textContent = JSON.stringify(result, null, 2);
  renderDispatchAssurances(result);
  const gate = result.gate || {};
  const gateSummary = gate.configured
    ? `Review gate ${gate.satisfied ? 'satisfied' : 'unsatisfied'} (${(gate.reason_codes || []).join(', ') || 'no reason'}).`
    : 'No review assurance gate is configured for this policy.';
	$('dispatch-status').textContent = `${gateSummary} Details loaded; no retry was started.`;
  $('dispatch-policy-id').value = policyId;
  const selectedDispatch = dispatchRows.find((item) => item.policy_id === policyId);
	const retryUnavailable = selectedDispatch && (
	  selectedDispatch.retry_exhausted
	  || Number(selectedDispatch.attempt_number || 0) >= Number(selectedDispatch.maximum_attempts || 0)
	);
	$('dispatch-retry-context').textContent = selectedDispatch
	  ? retryUnavailable
	    ? `${selectedDispatch.request_title || titleCaseWords(selectedDispatch.purpose || 'Prior work')} · no retry attempts remain; start a new request for additional work.`
	    : `${selectedDispatch.request_title || titleCaseWords(selectedDispatch.purpose || 'Prior work')} · retry reuses its exact target, instructions, and limits.`
	  : 'Selected prior work · retry reuses its exact frozen setup.';
  $('clear-dispatch-retry').hidden = false;
  const attempts = result.attempts || [];
  const latestAttempt = attempts.length ? attempts[attempts.length - 1] : null;
  const cancelButton = $('cancel-dispatch');
  cancelButton.dataset.runId = latestAttempt ? String(latestAttempt.attempt_id || '') : '';
  cancelButton.dataset.actionUnavailable = String(
    !latestAttempt || !['planned', 'started'].includes(latestAttempt.status),
  );
  setServerControlDisabled(cancelButton, cancelButton.dataset.actionUnavailable === 'true');
  return result;
}

function dispatchSourceMode() {
	const selected = document.querySelector('input[name="dispatch-source-mode"]:checked');
	return selected ? selected.value : 'new';
}

function selectedDispatchTarget() {
  const profile = $('dispatch-profile');
  const option = profile.options[profile.selectedIndex];
  return option ? text(option.dataset.participant).trim() : '';
}

function dispatchRequestCandidates(messages, query, target) {
  const normalizedQuery = text(query).trim().toLowerCase();
  const normalizedTarget = text(target).trim();
  return (messages || []).filter((message) => {
    if (text(message.kind).toLowerCase() !== 'request') return false;
    const recipients = (message.recipients || []).map(value => text(value).trim());
    if (normalizedTarget && !recipients.includes(normalizedTarget)) return false;
    if (!normalizedQuery) return true;
    return [message.id, message.title, message.sender, message.status]
      .some(value => text(value).toLowerCase().includes(normalizedQuery));
  });
}

function dispatchRequestExistingPolicy(requestId, dispatches = dispatchRows) {
  const normalizedRequestId = text(requestId).trim().toLowerCase();
  return (dispatches || []).find(
    item => text(item.request_id).trim().toLowerCase() === normalizedRequestId,
  ) || null;
}

function dispatchRequestIneligibleGuidance(message, dispatches = dispatchRows) {
  const existing = dispatchRequestExistingPolicy(message && message.id, dispatches);
  if (!existing) return '';
  const status = text(existing.status).trim().toLowerCase();
  if (status === 'completed' && text(existing.output_message_id).trim()) {
    return `Already dispatched. Open ${existing.output_message_id} and choose Continue this work to create a linked REQ.`;
  }
  if (['planned', 'started'].includes(status)) {
    return 'This Dispatch is already in progress. Select it in the Dispatch list to inspect or cancel it.';
  }
  const attemptNumber = Number(existing.attempt_number || 0);
  const maximumAttempts = Number(existing.maximum_attempts || 0);
  if (!existing.retry_exhausted && maximumAttempts > attemptNumber) {
    return 'This request already has a frozen Dispatch policy. Select it in the Dispatch list and choose Retry selected work.';
  }
  return 'This request was already dispatched and has no retry remaining. Start a new request for additional work.';
}

function setDispatchRequestValidation(state, message) {
  const status = $('dispatch-request-validation');
  status.className = `dispatch-request-validation ${state || ''}`.trim();
  status.textContent = message;
  if (state === 'invalid') {
    $('dispatch-message-id').setAttribute('aria-invalid', 'true');
  } else {
    $('dispatch-message-id').removeAttribute('aria-invalid');
  }
}

function invalidateDispatchRequestSelection() {
  confirmedDispatchRequestId = '';
  confirmedDispatchRequestTarget = '';
}

function clearDispatchRequestLookup({ clearInput = false } = {}) {
  if (dispatchRequestSearchTimer !== null) {
    window.clearTimeout(dispatchRequestSearchTimer);
    dispatchRequestSearchTimer = null;
  }
  dispatchRequestSearchSequence += 1;
  dispatchRequestSearchResults = [];
  invalidateDispatchRequestSelection();
  if (clearInput) $('dispatch-message-id').value = '';
  $('dispatch-request-matches').innerHTML = '';
  $('dispatch-request-matches').hidden = true;
  setDispatchRequestValidation(
    '',
    'Choose Existing request to load recent requests for the selected AI agent.',
  );
}

function renderDispatchRequestMatches(messages, query, target) {
  const candidates = dispatchRequestCandidates(messages, query, target).slice(0, 8);
  dispatchRequestSearchResults = candidates;
  const container = $('dispatch-request-matches');
  const candidateRows = candidates.map(message => ({
    message,
    guidance: dispatchRequestIneligibleGuidance(message),
  }));
  container.innerHTML = candidateRows.map(({ message, guidance }) => `
    <button type="button" class="dispatch-request-match" data-dispatch-request-id="${escapeHtml(message.id)}"${guidance ? ' disabled aria-disabled="true"' : ''}>
      <code>${escapeHtml(message.id)}</code>
      <strong>${escapeHtml(message.title || 'Untitled request')}</strong>
      <small>${escapeHtml([
        titleCaseWords(message.status || 'open'),
        `To ${(message.recipients || []).join(', ')}`,
        formatLocalDateTime(message.created_utc),
        guidance,
      ].filter(Boolean).join(' · '))}</small>
    </button>`).join('');
  container.hidden = candidates.length === 0;
  const normalizedQuery = text(query).trim().toLowerCase();
  const exactRow = candidateRows.find(
    ({ message }) => text(message.id).toLowerCase() === normalizedQuery,
  ) || null;
  invalidateDispatchRequestSelection();
  if (exactRow && exactRow.guidance) {
    setDispatchRequestValidation('invalid', exactRow.guidance);
  } else if (exactRow) {
    const exact = exactRow.message;
    confirmedDispatchRequestId = text(exact.id).trim();
    confirmedDispatchRequestTarget = text(target).trim();
    $('dispatch-message-id').value = confirmedDispatchRequestId;
    setDispatchRequestValidation(
      'valid',
      `Valid request for ${confirmedDispatchRequestTarget || 'the selected AI agent'}: ${confirmedDispatchRequestId}`,
    );
  } else if (candidateRows.length) {
    const eligibleCount = candidateRows.filter(({ guidance }) => !guidance).length;
    setDispatchRequestValidation(
      eligibleCount ? '' : 'invalid',
      eligibleCount
        ? text(query).trim()
          ? `Choose one of ${eligibleCount} available matching request${eligibleCount === 1 ? '' : 's'} addressed to ${target || 'the selected AI agent'}.`
          : `Showing ${eligibleCount} available recent request${eligibleCount === 1 ? '' : 's'} addressed to ${target || 'the selected AI agent'}. Type to narrow the list.`
        : 'These requests already have Dispatch records. Use the next step shown on each request.',
    );
  } else {
    setDispatchRequestValidation(
      text(query).trim() ? 'invalid' : '',
      text(query).trim()
        ? `No matching request addressed to ${target || 'the selected AI agent'}.`
        : `No recent request is addressed to ${target || 'the selected AI agent'}.`,
    );
  }
  return exactRow && !exactRow.guidance ? exactRow.message : null;
}

async function searchDispatchRequests({ queryOverride = null, targetOverride = '', requireExact = false } = {}) {
  const query = queryOverride === null
    ? $('dispatch-message-id').value.trim()
    : text(queryOverride).trim();
  const target = text(targetOverride).trim() || selectedDispatchTarget();
  const requestContext = captureRepoRequestContext();
  const searchSequence = ++dispatchRequestSearchSequence;
  setDispatchRequestValidation('', query ? 'Checking canonical requests…' : 'Loading recent requests…');
  const params = new URLSearchParams({ kind: 'request' });
  if (query) params.set('q', query);
  let result;
  let dispatchResult = null;
  try {
    [result, dispatchResult] = await Promise.all([
      api(`/api/messages?${params.toString()}`, {}, requestContext.repoId),
      requireExact
        ? api('/api/dispatches', {}, requestContext.repoId)
        : Promise.resolve(null),
    ]);
  } catch (error) {
    if (requestContext.isCurrent() && searchSequence === dispatchRequestSearchSequence) {
      $('dispatch-request-matches').innerHTML = '';
      $('dispatch-request-matches').hidden = true;
      invalidateDispatchRequestSelection();
      setDispatchRequestValidation('invalid', error.message || 'Request lookup failed.');
    }
    throw error;
  }
  if (!requestContext.isCurrent() || searchSequence !== dispatchRequestSearchSequence) {
    return { stale: true, request: null };
  }
  if (dispatchResult) dispatchRows = dispatchResult.dispatches || [];
  const exactCandidate = dispatchRequestCandidates(result.messages || [], query, target)
    .find(message => text(message.id).toLowerCase() === text(query).toLowerCase()) || null;
  const ineligibleGuidance = exactCandidate
    ? dispatchRequestIneligibleGuidance(exactCandidate)
    : '';
  const exact = renderDispatchRequestMatches(result.messages || [], query, target);
  if (requireExact && !exact) {
    throw new Error(ineligibleGuidance || `Choose a valid request addressed to ${target || 'the selected AI agent'} from the matching results.`);
  }
  return { stale: false, request: exact };
}

function scheduleDispatchRequestSearch({ immediate = false } = {}) {
  if (dispatchRequestSearchTimer !== null) window.clearTimeout(dispatchRequestSearchTimer);
  invalidateDispatchRequestSelection();
  setDispatchRequestValidation('', 'Checking canonical requests…');
  dispatchRequestSearchTimer = window.setTimeout(() => {
    dispatchRequestSearchTimer = null;
    searchDispatchRequests().catch(() => {});
  }, immediate ? 0 : 180);
}

function selectDispatchRequest(requestId) {
  const selected = dispatchRequestSearchResults.find(message => message.id === requestId);
  if (!selected) return;
  const ineligibleGuidance = dispatchRequestIneligibleGuidance(selected);
  if (ineligibleGuidance) {
    invalidateDispatchRequestSelection();
    setDispatchRequestValidation('invalid', ineligibleGuidance);
    return;
  }
  const target = selectedDispatchTarget();
  $('dispatch-message-id').value = selected.id;
  confirmedDispatchRequestId = selected.id;
  confirmedDispatchRequestTarget = target;
  $('dispatch-request-matches').innerHTML = '';
  $('dispatch-request-matches').hidden = true;
  setDispatchRequestValidation(
    'valid',
    `Valid request for ${target || 'the selected AI agent'}: ${selected.id} — ${selected.title || 'Untitled request'}`,
  );
}

function syncDispatchReviewTarget() {
	const retrying = dispatchSourceMode() === 'retry';
	const reviewing = $('dispatch-purpose').value.trim().toLowerCase() === 'review';
	const subjectType = $('dispatch-subject-type').value;
	const needsValue = ['decision', 'artifact'].includes(subjectType);
	const needsBase = ['pr', 'full'].includes(subjectType);
	$('dispatch-subject-type-field').hidden = retrying;
	$('dispatch-subject-required').hidden = retrying || !reviewing;
	$('dispatch-subject-none').textContent = reviewing
	  ? 'Choose an exact review target…'
	  : 'No exact target';
	$('dispatch-subject-value-field').hidden = retrying || !needsValue;
	$('dispatch-subject-base-field').hidden = retrying || !needsBase;
	$('dispatch-subject-explanation').hidden = retrying;
	if (retrying) return;
	if (subjectType === 'decision') {
	  $('dispatch-subject-value-label').textContent = 'Decision ID';
	  $('dispatch-subject-value-help').textContent = 'Exact current revision';
	  $('dispatch-subject-value').placeholder = 'D014';
	  $('dispatch-subject-explanation').textContent = 'The review is bound to the selected decision revision. A later revision makes the evidence stale.';
	} else if (subjectType === 'artifact') {
	  $('dispatch-subject-value-label').textContent = 'Repository file path';
	  $('dispatch-subject-value-help').textContent = 'Existing file, relative to this repository';
	  $('dispatch-subject-value').placeholder = 'docs/reviews/D014.md';
	  $('dispatch-subject-explanation').textContent = 'The repository file is the review target, not an upload or a requested output. Its current contents are securely fingerprinted so later changes make the evidence stale.';
	} else if (subjectType === 'worktree') {
	  $('dispatch-subject-explanation').textContent = 'Review the current unstaged and untracked local changes. The exact change set is frozen when work starts.';
	} else if (subjectType === 'staged') {
	  $('dispatch-subject-explanation').textContent = 'Review only the changes currently staged in Git. The exact change set is frozen when work starts.';
	} else if (subjectType === 'full') {
	  $('dispatch-subject-explanation').textContent = 'Review committed branch changes plus staged, unstaged, and untracked local changes compared with the selected branch.';
	} else if (subjectType === 'pr') {
	  $('dispatch-subject-explanation').textContent = 'Review committed branch changes compared with the selected branch. Local uncommitted changes are excluded.';
	} else {
	  $('dispatch-subject-explanation').textContent = reviewing
	    ? 'Independent review requires an exact decision revision, repository file, or Git change set.'
	    : 'This work will be linked to its request and result without an exact repository target.';
	}
}

function syncDispatchPurpose({ announce = false } = {}) {
	const purpose = $('dispatch-purpose').value.trim().toLowerCase() || 'general';
	const role = $('dispatch-role');
	const suggestedRoles = {
	  general: 'worker',
	  implementation: 'developer',
	  research: 'researcher',
	  review: 'reviewer',
	};
	const previousSuggestion = role.dataset.autoRole || '';
	const nextSuggestion = suggestedRoles[purpose] || 'worker';
	if (!role.value.trim() || role.value.trim() === previousSuggestion) {
	  role.value = nextSuggestion;
	}
	role.dataset.autoRole = nextSuggestion;
	syncDispatchReviewTarget();
	if (announce) {
	  $('dispatch-status').textContent = purpose === 'review'
	    ? 'Independent review selected. Choose the exact revision, file, or Git change set to review.'
	    : `${titleCaseWords(purpose)} selected. An exact repository target is optional.`;
	}
}

function setDispatchSourceMode(mode, { announce = false } = {}) {
	const normalized = ['new', 'existing', 'retry'].includes(mode) ? mode : 'new';
	if (normalized !== 'new') clearDispatchFollowUp();
	document.querySelectorAll('input[name="dispatch-source-mode"]').forEach((control) => {
	  control.checked = control.value === normalized;
	});
	$('dispatch-source-new').hidden = normalized !== 'new';
	$('dispatch-source-existing').hidden = normalized !== 'existing';
	$('dispatch-source-retry').hidden = normalized !== 'retry';
	$('dispatch-title').disabled = normalized !== 'new';
	$('dispatch-request-body').disabled = normalized !== 'new';
	const continuing = Boolean($('dispatch-continue-response-id').value.trim());
	$('dispatch-workstream').disabled = normalized !== 'new' || continuing;
	$('dispatch-message-id').disabled = normalized !== 'existing';
	const retrying = normalized === 'retry';
	[
	  'dispatch-agent', 'dispatch-profile', 'dispatch-purpose', 'dispatch-role', 'dispatch-subject-type',
	  'dispatch-subject-value', 'dispatch-subject-base', 'dispatch-max-attempts',
	].forEach((id) => { $(id).disabled = retrying; });
	$('dispatch-agent').disabled = retrying || continuing;
	$('dispatch-profile').disabled = retrying || continuing;
	syncDispatchReviewTarget();
	if (announce) {
	  const labels = {
	    new: 'New request selected. Add a title and instructions.',
	    existing: 'Existing request selected. Enter its public request reference from Messages.',
	    retry: 'Retry selected. Choose prior work from the table if none is selected.',
	  };
	  $('dispatch-status').textContent = labels[normalized];
	}
}

function clearDispatchRetrySelection() {
  $('dispatch-policy-id').value = '';
	$('dispatch-retry-context').textContent = 'Select a prior dispatch from the table, then choose this option to reuse its frozen setup.';
  $('clear-dispatch-retry').hidden = true;
	setDispatchSourceMode('new');
	$('dispatch-status').textContent = 'Selected prior work cleared. New request is ready.';
}

function clearDispatchFollowUp({ announce = false } = {}) {
  $('dispatch-continue-response-id').value = '';
  $('dispatch-followup-label').textContent = '';
  $('dispatch-followup-context').hidden = true;
  const sourceMode = dispatchSourceMode();
  $('dispatch-agent').disabled = sourceMode === 'retry';
  $('dispatch-profile').disabled = sourceMode === 'retry';
  $('dispatch-workstream').disabled = sourceMode !== 'new';
  if (announce) {
    $('dispatch-status').textContent = 'Follow-up context cleared. New independent request is ready.';
  }
}

function prepareDispatchFollowUp(responseId, dispatch = null) {
  const selected = (
    dispatch
    && dispatch.status === 'completed'
    && dispatch.output_message_id === responseId
  ) ? dispatch : dispatchRows.find(
    item => item.output_message_id === responseId && item.status === 'completed',
  );
  if (!selected) {
    throw new Error('The completed managed dispatch for this response is not available. Refresh Dispatch and try again.');
  }
  clearDispatchFollowUp();
  $('dispatch-policy-id').value = '';
  $('dispatch-retry-context').textContent = 'Select a prior dispatch from the table, then choose this option to reuse its frozen setup.';
  $('clear-dispatch-retry').hidden = true;
  setDispatchSourceMode('new');
  switchTab('dispatches');
  $('dispatch-continue-response-id').value = responseId;
  $('dispatch-followup-label').textContent = `${responseId} · ${selected.request_title || 'Prior managed result'}`;
  $('dispatch-followup-context').hidden = false;
  const selectedProfile = runtimeProfileInfo.get(selected.runtime_profile);
  if (!selectedProfile) {
    clearDispatchFollowUp();
    throw new Error('The prior runtime profile is no longer available, so its managed context cannot be resumed.');
  }
  $('dispatch-agent').value = runtimeAgentKey(selectedProfile);
  syncDispatchAgent({ preferredProfile: selected.runtime_profile });
  if ($('dispatch-profile').value !== selected.runtime_profile) {
    clearDispatchFollowUp();
    throw new Error('The prior runtime profile is no longer available, so its managed context cannot be resumed.');
  }
  $('dispatch-agent').disabled = true;
  $('dispatch-profile').disabled = true;
  $('dispatch-purpose').value = 'general';
  syncDispatchPurpose();
  $('dispatch-subject-type').value = '';
  syncDispatchReviewTarget();
  $('dispatch-title').value = `Follow up: ${selected.request_title || 'Prior result'}`;
  $('dispatch-request-body').value = '';
  $('dispatch-workstream').value = selected.workstream || '';
  $('dispatch-workstream').disabled = true;
  const editorDisclosure = $('dispatch-editor').closest('details');
  if (editorDisclosure) editorDisclosure.open = true;
  $('dispatch-status').textContent = 'Prior result linked. Add your follow-up instructions, then start AI work.';
  $('dispatch-request-body').focus();
  $('dispatch-editor').scrollIntoView({ block: 'nearest' });
}

function dispatchPayload() {
  const profile = $('dispatch-profile');
  const option = profile.options[profile.selectedIndex];
	const sourceMode = dispatchSourceMode();
	const selectedPolicyId = $('dispatch-policy-id').value.trim();
	const selectedDispatch = dispatchRows.find((item) => item.policy_id === selectedPolicyId);
	const policyId = sourceMode === 'retry' ? selectedPolicyId : '';
  const continueResponseId = sourceMode === 'new'
    ? $('dispatch-continue-response-id').value.trim()
    : '';
  const subjectType = $('dispatch-subject-type').value;
  const subjectValue = $('dispatch-subject-value').value.trim();
  const payload = {
	  target: sourceMode === 'retry' && selectedDispatch
	    ? selectedDispatch.participant
	    : (option ? option.dataset.participant : ''),
	  profile: sourceMode === 'retry' && selectedDispatch
	    ? selectedDispatch.runtime_profile
	    : profile.value,
	  actor: $('dispatch-actor').value,
	  policy_id: policyId,
	  message_id: sourceMode === 'existing' ? $('dispatch-message-id').value.trim() : '',
	  title: sourceMode === 'new' ? $('dispatch-title').value.trim() : '',
	  body: sourceMode === 'new' ? $('dispatch-request-body').value : '',
	  workstream: sourceMode === 'new' && !continueResponseId
	    ? $('dispatch-workstream').value.trim()
	    : '',
	  continue_response_id: continueResponseId,
	    purpose: $('dispatch-purpose').value.trim() || 'general',
	    role: $('dispatch-role').value.trim() || 'worker',
    max_attempts: Number($('dispatch-max-attempts').value || 1),
    timeout_seconds: Number($('dispatch-timeout').value || 3600),
    subject_decision: '',
    subject_artifact: '',
    subject_change_mode: '',
    subject_change_base: '',
	};
  if (!policyId) {
	  if (subjectType === 'decision') payload.subject_decision = subjectValue;
	  else if (subjectType === 'artifact') payload.subject_artifact = subjectValue;
	  else if (subjectType) {
	    payload.subject_change_mode = subjectType;
      if (['pr', 'full'].includes(subjectType)) {
        payload.subject_change_base = $('dispatch-subject-base').value.trim() || 'main';
      }
    }
  }
  return payload;
}

function validateDispatchPayload(payload) {
	const sourceMode = dispatchSourceMode();
	if (sourceMode === 'retry') {
	  if (!payload.policy_id) {
	    throw new Error('Select prior work from the Dispatch table before choosing Retry selected work.');
	  }
	  const selectedDispatch = dispatchRows.find((item) => item.policy_id === payload.policy_id);
	  if (selectedDispatch && (
	    selectedDispatch.retry_exhausted
	    || Number(selectedDispatch.attempt_number || 0) >= Number(selectedDispatch.maximum_attempts || 0)
	  )) {
	    throw new Error('This work has used all allowed attempts. Start a new request for additional work.');
	  }
	  return;
	}
	if (sourceMode === 'existing' && !payload.message_id) {
	  throw new Error('Search for and choose an existing request.');
	}
	if (sourceMode === 'existing' && (
	  confirmedDispatchRequestId.toLowerCase() !== payload.message_id.toLowerCase()
	  || confirmedDispatchRequestTarget !== payload.target
	)) {
	  throw new Error('Choose a confirmed request from the matching results.');
	}
	if (sourceMode === 'new' && !payload.title) {
	  throw new Error('Add a title for the new request.');
	}
	if (sourceMode === 'new' && !payload.body.trim()) {
	  throw new Error('Add instructions for the new request.');
	}
	const subjectType = $('dispatch-subject-type').value;
	if (['decision', 'artifact'].includes(subjectType) && !$('dispatch-subject-value').value.trim()) {
	  throw new Error(subjectType === 'decision'
	    ? 'Enter the decision ID to review.'
	    : 'Enter the repository file path to review.');
	}
	if (payload.purpose.trim().toLowerCase() === 'review' && !subjectType) {
	  throw new Error('Choose an exact review target: a decision, repository file, or Git change set.');
	}
}

async function startDispatch() {
  const requestContext = captureRepoRequestContext();
  const payload = dispatchPayload();
	if (dispatchSourceMode() === 'existing') {
	  const confirmation = await searchDispatchRequests({
	    queryOverride: payload.message_id,
	    targetOverride: payload.target,
	    requireExact: true,
	  });
	  if (confirmation.stale) return confirmation;
	  payload.message_id = confirmation.request.id;
	}
	validateDispatchPayload(payload);
  renderDispatchResult(null, null);
  $('dispatch-status').textContent = 'Preflighting and dispatching… This may take several minutes.';
  const result = await api('/api/dispatch/run', {
    method: 'POST',
    body: JSON.stringify(payload),
  });
  if (!requestContext.isCurrent()) return { stale: true };
  dispatchRows = result.dispatches || [];
  dispatchFocusedPolicyId = (result.dispatch || {}).policy_id || '';
  clearDispatchFollowUp();
  renderDispatchRows(dispatchRows);
  $('dispatch-output').textContent = result.result || 'Dispatch completed.';
  renderDispatchResult(result.response, result.dispatch);
  $('dispatch-status').textContent = result.response
    ? 'Dispatch completed. Its response is displayed below and recorded in Messages.'
    : 'Dispatch completed, but no response record was returned. Inspect the technical output.';
  await Promise.all([loadStatus(), loadMessages()]);
  return result;
}

async function recordDispatchAssurance() {
  const requestContext = captureRepoRequestContext();
  const policyId = $('dispatch-policy-id').value.trim();
  if (!policyId) throw new Error('Select or enter a frozen dispatch policy first.');
	  $('dispatch-status').textContent = 'Reading the current RES and deriving review assurance…';
  const result = await api('/api/dispatch/assure', {
    method: 'POST',
    body: JSON.stringify({
      policy_id: policyId,
      actor: $('dispatch-actor').value,
    }),
  });
  if (!requestContext.isCurrent()) return { stale: true };
  dispatchRows = result.dispatches || [];
  renderDispatchRows(dispatchRows);
  if (result.dispatch) {
    $('dispatch-output').textContent = JSON.stringify(result.dispatch, null, 2);
    const gate = result.dispatch.gate || {};
    renderDispatchAssurances(result.dispatch);
    $('dispatch-status').textContent = gate.satisfied
	      ? 'Review assurance derived from the current RES; the gate is satisfied.'
	      : `Review assurance derived; gate remains unsatisfied (${(gate.reason_codes || []).join(', ') || 'no reason'}).`;
  } else {
    $('dispatch-output').textContent = result.result || 'Review assurance derived.';
	    $('dispatch-status').textContent = 'Review assurance derived.';
  }
  await loadStatus();
  return result;
}

async function transitionDispatchAssurance(card, action) {
  const requestContext = captureRepoRequestContext();
  const responseId = card.dataset.assuranceResponse || '';
  const expectedVersion = Number(card.dataset.assuranceVersion || 0);
  const reasonCode = card.querySelector('[data-assurance-reason]').value;
  const note = card.querySelector('[data-assurance-note]').value.trim();
  if (!note) throw new Error('Add a short explanation before changing reliance state.');
  const policyId = $('dispatch-policy-id').value.trim();
  const result = await api(`/api/dispatch/assurance/${action}`, {
    method: 'POST',
    body: JSON.stringify({
      response_id: responseId,
      expected_version: expectedVersion,
      reason_code: reasonCode,
      note,
      actor: $('dispatch-actor').value,
      policy_id: policyId,
    }),
  });
  if (!requestContext.isCurrent()) return { stale: true };
  dispatchRows = result.dispatches || [];
  renderDispatchRows(dispatchRows);
  if (result.dispatch) {
    renderDispatchAssurances(result.dispatch);
    $('dispatch-output').textContent = JSON.stringify(result.dispatch, null, 2);
  }
  $('dispatch-status').textContent = action === 'flag'
    ? `Flagged ${responseId}; it no longer satisfies review gates.`
    : `Retired ${responseId}; it remains visible in history and cannot satisfy gates.`;
  await loadStatus();
  return result;
}

async function cancelDispatch() {
  const requestContext = captureRepoRequestContext();
  const runId = $('cancel-dispatch').dataset.runId || '';
  if (!runId) throw new Error('Select a dispatch with an active attempt first.');
  $('dispatch-status').textContent = 'Cancelling the canonical outcome…';
  const result = await api('/api/dispatch/cancel', {
    method: 'POST',
    body: JSON.stringify({
      run_id: runId,
      actor: $('dispatch-actor').value,
    }),
  });
  if (!requestContext.isCurrent()) return { stale: true };
  dispatchRows = result.dispatches || [];
  renderDispatchRows(dispatchRows);
  $('dispatch-output').textContent = result.result || 'Dispatch outcome cancelled.';
  $('dispatch-status').textContent = 'Canonical outcome cancelled; any late model result will be discarded.';
  const policyId = $('dispatch-policy-id').value.trim();
  if (policyId) await lookupDispatch(policyId);
  await loadStatus();
  return result;
}

async function updateMessageStatus() {
  const statusPanel = $('message-status-panel');
  if (!repoElementIsCurrent(statusPanel)) {
    $('message-edit-status').textContent = 'This message control belongs to an earlier repository view; reload the active repository.';
    return { stale: true };
  }
  const requestContext = captureRepoRequestContext();
  const id = $('message-id').value.trim() || (((lastMessage || {}).packet || {}).message || {}).id || '';
  const status = $('message-new-status').value;
  const reason = $('message-status-reason').value.trim();
  if (!id) return;
  if (!reason) {
    $('message-edit-status').textContent = 'Reason is required to change request status.';
    return;
  }
  $('message-edit-status').textContent = `Saving ${id} status...`;
  const result = await api('/api/message/status', {
    method: 'POST',
    body: JSON.stringify({ id, status, reason }),
  });
  if (!requestContext.isCurrent()) return { stale: true };
  if (result.message) renderMessageDetail(result.message);
  $('message-edit-status').textContent = `Saved ${id} as ${result.status}`;
  await Promise.all([loadStatus(), loadMessages()]);
}

async function loadBacklog(resultOverride = null) {
  const requestContext = captureRepoRequestContext();
  const params = new URLSearchParams();
  const query = $('backlog-search').value.trim();
  const quickFilter = $('backlog-quick-filter').value;
  const status = $('backlog-status-filter').value.trim();
  const lane = $('backlog-lane-filter').value.trim();
  const priority = $('backlog-priority-filter').value.trim();
  const owner = $('backlog-owner-filter').value.trim();
  const itemType = $('backlog-type-filter').value.trim();
  const scope = $('backlog-scope-filter').value.trim();
  const wave = $('backlog-wave-filter').value.trim();
  const origin = $('backlog-origin-filter').value.trim();
  if (query) params.set('q', query);
  if (quickFilter) params.set('filter', quickFilter);
  if (status) params.set('status', status);
  if (lane) params.set('lane', lane);
  if (priority) params.set('priority', priority);
  if (owner) params.set('owner', owner);
  if (itemType) params.set('type', itemType);
  if (scope) params.set('scope', scope);
  if (wave) params.set('wave', wave);
  if (origin) params.set('origin', origin);
  const suffix = params.toString() ? `?${params.toString()}` : '';
  const result = resultOverride || await api(`/api/backlog/items${suffix}`);
  if (!requestContext.isCurrent()) return { stale: true };
  backlogRows = result.items;
  renderBacklogRows(backlogRows);
  $('backlog-edit-status').textContent = `Showing ${result.items.length} backlog item(s)${describeBacklogFilters(params)}`;
  return result;
}

function renderBacklogRows(items) {
  const rows = sortedRows('backlog', items);
  renderSortIndicators('backlog');
  $('backlog-body').innerHTML = rows.map((item) => `
    <tr data-requires-server-gesture data-backlog-view-id="${escapeHtml(item.id)}" data-repo-id="${escapeHtml(activeRepoId)}" data-repo-generation="${repoViewGeneration}">
      <td><code>${escapeHtml(item.id)}</code></td>
      <td>${escapeHtml(formatLocalDateTime(item.updated_utc))}</td>
      <td><input class="mini-field" data-requires-server data-backlog-id="${escapeHtml(item.id)}" data-backlog-field="status" value="${escapeHtml(item.status)}"></td>
      <td><input class="mini-field" data-requires-server data-backlog-id="${escapeHtml(item.id)}" data-backlog-field="lane" value="${escapeHtml(item.lane)}"></td>
      <td><select class="mini-field" data-requires-server data-backlog-id="${escapeHtml(item.id)}" data-backlog-field="priority">
        ${priorityOptions(item.priority)}
      </select></td>
      <td>${escapeHtml(item.item_type)}</td>
      <td>${escapeHtml(item.owner_hint)}</td>
      <td>${escapeHtml(item.workflow_origin || '')}</td>
      <td>${escapeHtml(item.launch_scope)}</td>
      <td>${escapeHtml(item.wave || item.release_phase || '')}</td>
      <td>${escapeHtml(item.title)}</td>
    </tr>
  `).join('') || '<tr><td colspan="11" class="muted">No backlog items.</td></tr>';
  $('backlog-body').querySelectorAll('[data-requires-server]').forEach((control) => {
    setServerControlDisabled(control);
  });
  setServerGestureAvailability($('backlog-body'));
}

async function lookupBacklogItem(id) {
  const requestContext = captureRepoRequestContext();
  const result = await api(`/api/backlog/item?id=${encodeURIComponent(id)}`);
  if (!requestContext.isCurrent()) return { stale: true };
  $('backlog-output').textContent = result.block;
  const item = result.item || {};
  $('backlog-markdown').hidden = false;
  $('backlog-markdown').innerHTML = [
    `<h3>${escapeHtml(`${item.id || id} · ${item.title || 'Backlog item'}`)}</h3>`,
    `<p class="decision-review-meta">${escapeHtml([
      titleCaseWords(item.status), titleCaseWords(item.lane), item.priority,
      item.owner_hint ? `Owner: ${item.owner_hint}` : '',
    ].filter(Boolean).join(' · '))}</p>`,
    renderSafeMarkdown(item.summary || item.notes || 'No human-facing summary is available.'),
  ].join('');
  $('backlog-technical-record').hidden = false;
  $('backlog-edit-status').textContent = `Loaded ${id}`;
  return result;
}

async function loadDecisions(resultOverride = null) {
  const requestContext = captureRepoRequestContext();
  const params = new URLSearchParams();
  const query = $('decision-search').value.trim();
  const status = $('decision-status-filter').value.trim();
  const tier = $('decision-tier-filter').value.trim();
  if (query) params.set('q', query);
  if (status) params.set('status', status);
  if (tier) params.set('tier', tier);
  const suffix = params.toString() ? `?${params.toString()}` : '';
  const result = resultOverride || await api(`/api/decisions${suffix}`);
  if (!requestContext.isCurrent()) return { stale: true };
  decisionRows = result.decisions;
  if (result.next_id) decisionNextId = result.next_id;
  renderDecisionRows(decisionRows);
  const focusedDecision = decisionFocusedId
    && decisionRows.find(item => item.id === decisionFocusedId);
  $('decision-list-status').textContent = focusedDecision
    ? `Showing selected decision ${decisionFocusedId}. Its details are ready below.`
    : `Showing ${decisionRows.length} decision(s)${params.toString() ? ` for ${Array.from(params.entries()).map(([key, value]) => `${key}=${value}`).join(', ')}` : ''}.`;
  return result;
}

function renderDecisionRows(decisions) {
  const selectedRows = decisionFocusedId
    ? decisions.filter(decision => decision.id === decisionFocusedId)
    : decisions;
  const rows = sortedRows('decisions', selectedRows.length ? selectedRows : decisions);
  renderSortIndicators('decisions');
  $('decision-body').innerHTML = rows.map((decision) => `
    <tr class="${decision.id === decisionFocusedId ? 'decision-row-selected' : ''}" data-requires-server-gesture data-decision-id="${escapeHtml(decision.id)}" data-repo-id="${escapeHtml(activeRepoId)}" data-repo-generation="${repoViewGeneration}">
      <td><button type="button" class="ghost table-action" aria-label="Open decision ${escapeHtml(decision.id)}" data-requires-server>${escapeHtml(decision.id)}</button></td>
      <td>${escapeHtml(formatLocalDateTime(decision.display_utc || decision.status_utc || decision.proposed_utc))}</td>
      <td>${escapeHtml(decision.status)}</td>
      <td>${Number(decision.approved_version_count || decision.approved_version || 0)
        ? `v${Number(decision.approved_version_count || decision.approved_version)}`
        : '—'}</td>
      <td>${escapeHtml(decision.tier)}</td>
      <td>${escapeHtml(decision.owner)}</td>
      <td>${escapeHtml(decision.title)}</td>
    </tr>
  `).join('') || '<tr><td colspan="7" class="muted">No decisions.</td></tr>';
  $('show-all-decisions').hidden = !decisionFocusedId;
  $('decision-body').querySelectorAll('[data-requires-server]').forEach((control) => {
    setServerControlDisabled(control);
  });
  setServerGestureAvailability($('decision-body'));
}

function focusDecisionList(id, message) {
  decisionFocusedId = id;
  renderDecisionRows(decisionRows);
  $('decision-list-status').textContent = message;
}

function showAllDecisions() {
  const previous = decisionFocusedId;
  decisionFocusedId = '';
  renderDecisionRows(decisionRows);
  $('decision-list-status').textContent = previous
    ? `Showing all ${decisionRows.length} decisions. ${previous} remains open below.`
    : `Showing all ${decisionRows.length} decisions.`;
}

function syncDecisionChangeBase() {
  const mode = $('decision-change-mode').value;
  $('decision-change-base-field').hidden = !['pr', 'full'].includes(mode);
}

function renderDecisionApplicability(context) {
  const request = context.request || {};
  const repository = context.repository || {};
  const decisions = context.decisions || [];
  const changes = request.changes || [];
  const pathCount = Number.isInteger(request.path_count)
    ? request.path_count
    : (request.paths || []).length;
  if (context.complete !== true || context.context_status !== 'complete') {
    const status = context.context_status || 'incomplete';
    const diagnostics = context.diagnostics || [];
    const lines = [
      `Mode: ${request.mode || 'worktree'} · changed paths represented: ${pathCount}`,
      `Context status: ${status}`,
      ...diagnostics.map((diagnostic) => `Diagnostic: ${diagnostic}`),
    ];
    $('decision-applicability-results').textContent = lines.join('\\n');
    $('decision-applicability-status').textContent = status === 'incomplete'
      ? `Changed-path context is incomplete for ${pathCount} paths; no empty-result claim is available. Narrow the comparison and retry.`
      : 'Changed-path context is unavailable; no empty-result claim is available. Resolve the diagnostic and retry.';
    return;
  }
  const lines = [
    `Mode: ${request.mode || 'worktree'} · changed paths: ${pathCount} · applicable decisions: ${decisions.length}`,
    `Boundary: ${request.boundary || 'change_review'} · policy: advisory · read model: ${repository.read_model || 'unknown'} · event: ${repository.event_seq ?? 'unknown'}`,
  ];
  if (request.merge_base) lines.push(`Merge base: ${request.merge_base}`);
  if (!changes.length) lines.push('', 'No changed paths in this comparison.');
  if (changes.length && !decisions.length) {
    lines.push('', 'No applicable accepted or in-force decisions for these changed paths.');
  }
  decisions.forEach((decision) => {
    lines.push(
      '',
      `${decision.id} · ${decision.status} · ${decision.configured_enforcement} → ${decision.effective_enforcement}`,
      decision.title || '',
    );
    (decision.matches || []).forEach((match) => {
      const reason = match.path === null
        ? '<repository>'
        : `${match.comparison ? `${match.comparison}: ` : ''}${match.change_kind || 'selected'} ${match.path}`;
      const pattern = match.pattern ? ` via ${match.pattern}` : '';
      const pathType = match.path_type ? ` [${match.path_type}]` : '';
      lines.push(`  - ${reason}${pattern}${pathType}`);
    });
    if (decision.required_checks && decision.required_checks.length) {
      lines.push(`  checks (not run): ${decision.required_checks.join(', ')}`);
    }
    if (decision.warnings && decision.warnings.length) {
      lines.push(`  warnings: ${decision.warnings.join('; ')}`);
    }
  });
  const lineLimit = 400;
  const visibleLines = lines.slice(0, lineLimit);
  const omittedLineCount = Math.max(0, lines.length - lineLimit);
  if (omittedLineCount) {
    visibleLines.push('', `… ${omittedLineCount} additional lines omitted`);
  }
  $('decision-applicability-results').textContent = visibleLines.join('\\n');
  $('decision-applicability-status').textContent = omittedLineCount
    ? `Changed-path decision context is complete, but this display omits ${omittedLineCount} lines. Use agent-mesh check decisions --json for the complete bounded envelope. No stored verification was executed.`
    : 'Changed-path decision context is complete. No stored verification was executed.';
}

async function loadDecisionApplicability() {
  syncDecisionChangeBase();
  const requestedRepoId = activeRepoId;
  const requestGeneration = ++decisionApplicabilityGeneration;
  const requestIsCurrent = () => (
    requestGeneration === decisionApplicabilityGeneration && requestedRepoId === activeRepoId
  );
  const mode = $('decision-change-mode').value;
  const params = new URLSearchParams({ mode });
  if (['pr', 'full'].includes(mode)) {
    const base = $('decision-change-base').value.trim() || 'main';
    params.set('base', base);
  }
  const evaluateButton = $('reload-decision-applicability');
  evaluateButton.disabled = true;
  $('decision-applicability-status').textContent = 'Evaluating local Git changes...';
  let result;
  try {
    result = await api(`/api/decisions/applicability?${params.toString()}`);
  } catch (error) {
    if (!requestIsCurrent()) return { stale: true };
    if (error.payload && error.payload.context) {
      renderDecisionApplicability(error.payload.context);
      return { ...error.payload, buttonFeedback: 'error' };
    }
    throw error;
  } finally {
    if (requestIsCurrent()) setServerControlDisabled(evaluateButton);
  }
  if (!requestIsCurrent()) return { stale: true };
  renderDecisionApplicability(result.context);
  return result;
}

async function lookupDecision(id) {
  if (decisionLookupInFlight) {
    $('decision-output').textContent = `${decisionLookupInFlight.id} is still loading. Wait for it to finish before opening ${id}.`;
    return { busy: true, loading: decisionLookupInFlight.id, requested: id };
  }
  const requestContext = captureRepoRequestContext();
  const lookupState = {
    id,
    repoId: requestContext.repoId,
    generation: requestContext.generation,
  };
  decisionLookupInFlight = lookupState;
  let loaded = false;
  const loadingMessage = `Loading ${id}…`;
  focusDecisionList(id, `Loading ${id}… The list is focused to keep its review in view.`);
  $('decision-output').textContent = loadingMessage;
  beginDecisionLookup(id);
  try {
    const result = await api(`/api/decision?id=${encodeURIComponent(id)}`);
    if (!requestContext.isCurrent()) return { stale: true };
    const d = result.decision;
    $('decision-output').textContent = result.block || d.block || [
      `## ${d.id}`,
      `- status: ${d.status || ''}`,
      `- tier: ${d.tier || ''}`,
      `- owner: ${d.owner || ''}`,
      `- drift_risk: ${d.drift_risk || ''}`,
      '',
      d.title || '',
    ].join('\\n').trim() + '\\n';
    populateDecisionEditor(d);
    loaded = true;
    $('decision-list-status').textContent = `Loaded ${id}. Showing the selected decision; its details are ready below.`;
    if ($('decision-review-loading')) {
      $('decision-review-loading').textContent = d.review_data_required
        ? `${id} is ready. Loading verified revision comparisons separately…`
        : `${id} loaded from verified canonical history.`;
    }
    if (d.review_data_required) {
      void loadDecisionReviewData(id, d.revision_sha, requestContext);
    }
    if ($('decision-review') && typeof $('decision-review').scrollIntoView === 'function') {
      window.setTimeout(() => {
        $('decision-review').scrollIntoView({ behavior: 'smooth', block: 'start' });
      }, 0);
    }
    return result;
  } catch (error) {
    if (requestContext.isCurrent()) {
      renderDecisionLoadFailure(id, error.message);
    }
    throw error;
  } finally {
    if (!requestContext.isCurrent() && $('decision-output').textContent === loadingMessage) {
      $('decision-output').textContent = '';
    }
    if (requestContext.isCurrent() && !loaded && $('decision-review-loading')) {
      $('decision-review-loading').textContent = '';
    }
    if (decisionLookupInFlight === lookupState) decisionLookupInFlight = null;
  }
}

function beginDecisionLookup(id) {
  activeDecision = null;
  activeDecisionEditorSnapshot = '';
  $('decision-editor').hidden = true;
  clearRepoElementStamp($('decision-editor'));
  $('decision-human-actions').hidden = true;
  $('decision-revision-diff').hidden = true;
  $('decision-version-history').hidden = true;
  $('retry-decision-review').hidden = true;
  $('accept-decision').disabled = true;
  $('reject-decision').disabled = true;
  const review = $('decision-review');
  review.hidden = false;
  stampRepoElement(review);
  review.dataset.decisionId = id;
  $('decision-review-title').textContent = `${id} · Loading decision`;
  $('decision-review-meta').textContent = '';
  $('decision-review-version').textContent = '';
  $('decision-review-status').textContent = 'Loading';
  $('decision-review-loading').textContent = `Loading ${id} and verifying its current canonical state…`;
  $('decision-review-summary').innerHTML = '<p class="human-diff-empty">The prior decision has been cleared while this selection loads.</p>';
  $('decision-canonical-markdown').innerHTML = '';
  $('decision-canonical-details').hidden = true;
  $('edit-decision').hidden = true;
}

function renderDecisionLoadFailure(id, diagnostic) {
  beginDecisionLookup(id);
  $('decision-review-title').textContent = `${id} · Decision unavailable`;
  $('decision-review-meta').textContent = '';
  $('decision-review-version').textContent = '';
  $('decision-review-status').textContent = 'Unavailable';
  $('decision-review-loading').textContent = '';
  $('decision-review-summary').innerHTML = `
    <section>
      <h4>Could not load this decision</h4>
      <p>${escapeHtml(diagnostic || 'The verified decision detail is unavailable.')}</p>
      <button type="button" class="ghost" data-retry-decision="${escapeHtml(id)}" data-requires-server>Retry decision</button>
    </section>
  `;
  $('decision-canonical-markdown').innerHTML = '';
  $('decision-canonical-details').hidden = true;
  $('edit-decision').hidden = true;
  $('decision-output').textContent = diagnostic || 'The verified decision detail is unavailable.';
  $('decision-list-status').textContent = `Could not load ${id}: ${diagnostic}`;
  $('decision-review-summary').querySelectorAll('[data-requires-server]').forEach((control) => {
    setServerControlDisabled(control);
  });
}

async function loadDecisionReviewData(id, revisionSha, requestContext = captureRepoRequestContext()) {
  if (!id || !revisionSha || !requestContext.isCurrent()) return { stale: true };
  if (decisionReviewLookupInFlight) {
    return {
      busy: true,
      loading: decisionReviewLookupInFlight.id,
      requested: id,
    };
  }
  const reviewState = {
    id,
    revisionSha,
    repoId: requestContext.repoId,
    generation: requestContext.generation,
  };
  decisionReviewLookupInFlight = reviewState;
  $('retry-decision-review').hidden = true;
  try {
    const params = new URLSearchParams({ id, revision_sha: revisionSha });
    const result = await api(`/api/decision/review?${params.toString()}`);
    if (!requestContext.isCurrent()) return { stale: true };
    if (
      !activeDecision
      || activeDecision.id !== id
      || activeDecision.revision_sha !== revisionSha
      || $('decision-review').dataset.decisionId !== id
    ) return { stale: true };
    if (result.stale === true || result.revision_sha !== revisionSha) {
      renderDecisionLoadFailure(id, 'This decision changed while its revision comparison was loading. Retry to review the current revision.');
      return { stale: true, changed: true };
    }
    activeDecision = {
      ...activeDecision,
      revision: result.revision,
      revision_count: result.revision_count,
      approved_version: result.approved_version,
      approved_version_count: result.approved_version_count,
      revision_history: result.revision_history,
      revision_diff: result.revision_diff,
      review_data_required: false,
    };
    renderDecisionVersionHistory(activeDecision);
    renderDecisionRevisionDiff(activeDecision.revision_diff);
    refreshDecisionApprovalState();
    $('decision-review-loading').textContent = `${id} is ready with verified revision comparisons.`;
    return result;
  } catch (error) {
    if (
      requestContext.isCurrent()
      && activeDecision
      && activeDecision.id === id
      && activeDecision.revision_sha === revisionSha
    ) {
      activeDecision = {
        ...activeDecision,
        revision_history: {
          ...(activeDecision.revision_history || {}),
          loading: false,
          available: false,
          diagnostic: `Saved revisions are unavailable: ${error.message}`,
        },
        revision_diff: activeDecision.status === 'proposed'
          ? {
              pending: true,
              available: false,
              loading: false,
              diagnostic: `Revision comparison is unavailable: ${error.message}`,
              pending_revision_sha: revisionSha,
              pending_body_sha: activeDecision.body_sha || '',
            }
          : activeDecision.revision_diff,
      };
      renderDecisionVersionHistory(activeDecision);
      renderDecisionRevisionDiff(activeDecision.revision_diff);
      refreshDecisionApprovalState();
      $('decision-review-loading').textContent = `${id} is ready, but its revision comparisons could not be loaded.`;
    }
    return { unavailable: true, error: error.message, buttonFeedback: 'error' };
  } finally {
    if (decisionReviewLookupInFlight === reviewState) decisionReviewLookupInFlight = null;
  }
}

function showDecisionMutationReceipt(result) {
  const id = (result.decision && result.decision.id) || 'Decision';
  const diagnostic = result.diagnostic
    || `${id} was recorded canonically, but detail refresh is temporarily unavailable.`;
  resetDecisionEditor(true);
  $('decision-output').textContent = diagnostic;
  return diagnostic;
}

function resetDecisionEditor(hide = false) {
  activeDecision = null;
  activeDecisionEditorSnapshot = '';
  $('decision-editor').hidden = hide;
  clearRepoElementStamp($('decision-editor'));
  $('decision-revision-diff').hidden = true;
  $('decision-revision-diff-status').textContent = '';
  $('decision-metadata-diff').textContent = '';
  $('decision-body-diff').textContent = '';
  $('decision-editor-heading').textContent = 'New decision';
  $('decision-editor-lifecycle').textContent = 'proposed';
  $('decision-edit-id').disabled = false;
  $('decision-edit-id').value = decisionNextId;
  $('decision-edit-tier').value = 'note';
  $('decision-edit-scope').value = 'manual';
  $('decision-edit-owner').value = '';
	$('decision-edit-actor').value = decisionDefaultActor;
  $('decision-edit-title').value = '';
  $('decision-edit-context').value = '';
  $('decision-edit-summary').value = '';
  $('decision-edit-globs').value = '';
  $('decision-edit-exemptions').value = '';
  $('decision-edit-generated').value = '';
  $('decision-edit-checks').value = '';
  $('decision-edit-verification').value = '';
  $('decision-edit-assumptions').value = '';
  $('decision-edit-evidence').value = '';
  $('decision-evidence-links').innerHTML = '';
  $('decision-edit-reviewers').value = '';
  $('decision-edit-quorum').value = '';
  $('decision-edit-tags').value = '';
  $('decision-edit-body').value = '';
  $('decision-revision-reason').value = '';
  $('decision-revision-reason-field').hidden = true;
	$('decision-approval-notes').value = '';
	$('decision-approval-notes').required = true;
  $('decision-human-actions').hidden = true;
  $('retry-decision-review').hidden = true;
  setServerControlDisabled('accept-decision');
  setServerControlDisabled('reject-decision');
  $('save-decision').textContent = 'Create decision';
  $('decision-edit-status').textContent = '';
  $('decision-approval-status').textContent = '';
  if (hide) {
    $('decision-review').hidden = true;
    clearRepoElementStamp($('decision-review'));
    delete $('decision-review').dataset.decisionId;
  }
}

async function showNewDecisionEditor() {
  const requestContext = captureRepoRequestContext();
  if (!decisionNextId) {
    const result = await api('/api/decisions');
    if (!requestContext.isCurrent()) return { stale: true };
    decisionNextId = result.next_id || '';
  }
  if (!requestContext.isCurrent()) return { stale: true };
  decisionFocusedId = '';
  renderDecisionRows(decisionRows);
  resetDecisionEditor(false);
  $('decision-review').hidden = true;
  stampRepoElement($('decision-editor'));
  $('decision-edit-title').focus();
  return { stale: false };
}

function renderDecisionRevisionDiff(diff) {
  const panel = $('decision-revision-diff');
  $('retry-decision-review').hidden = true;
  if (!diff || diff.pending !== true) {
    panel.hidden = true;
    $('decision-human-diff').innerHTML = '';
    return;
  }
  panel.hidden = false;
  $('decision-revision-pending-sha').textContent = diff.pending_revision_sha || 'unavailable';
  $('decision-body-pending-sha').textContent = diff.pending_body_sha || 'empty body';
  if (diff.loading === true) {
    $('decision-revision-diff-badge').textContent = 'loading comparison';
    $('decision-revision-diff-status').textContent = 'Loading the verified comparison with the last approved version…';
    $('decision-revision-baseline-label').textContent = 'Loading';
    $('decision-revision-baseline-sha').textContent = 'Loading';
    $('decision-body-baseline-sha').textContent = 'Loading';
    $('decision-metadata-diff').textContent = '';
    $('decision-body-diff').textContent = '';
    $('decision-human-diff').innerHTML = '<p class="human-diff-empty">The current decision is ready. Approval remains disabled until its comparison finishes loading.</p>';
    return;
  }
  if (diff.available !== true) {
    $('retry-decision-review').hidden = false;
    $('decision-revision-diff-badge').textContent = 'comparison unavailable';
    $('decision-revision-diff-status').textContent = diff.diagnostic
      || 'The exact approval comparison is unavailable. Approval is blocked.';
    $('decision-revision-baseline-label').textContent = 'Unavailable';
    $('decision-revision-baseline-sha').textContent = 'Unavailable';
    $('decision-body-baseline-sha').textContent = 'Unavailable';
    $('decision-metadata-diff').textContent = 'No safe metadata comparison is available.';
    $('decision-body-diff').textContent = 'No safe canonical body comparison is available.';
    $('decision-human-diff').innerHTML = '<p class="human-diff-empty">The human-facing comparison could not be verified. Approval remains blocked; rejection of the displayed exact revision remains available.</p>';
    return;
  }
  $('decision-revision-diff-badge').textContent = diff.baseline_kind === 'empty'
    ? 'no approved version'
    : diff.has_changes
      ? 'changes detected'
      : 'exact-content revert';
  $('decision-revision-baseline-label').textContent = [
    diff.baseline_label || '',
    diff.baseline_approved_utc ? `approved ${formatLocalDateTime(diff.baseline_approved_utc)}` : '',
    diff.baseline_accepted_by ? `by ${diff.baseline_accepted_by}` : '',
  ].filter(Boolean).join(' · ');
  $('decision-revision-baseline-sha').textContent = diff.baseline_revision_sha || 'empty baseline';
  $('decision-body-baseline-sha').textContent = diff.baseline_body_sha || 'empty baseline';
  $('decision-metadata-diff').textContent = diff.metadata_diff || 'No metadata comparison output.';
  $('decision-body-diff').textContent = diff.body_diff || 'No body comparison output.';
  renderDecisionHumanDiff(diff);
  $('decision-revision-diff-status').textContent = diff.baseline_kind === 'empty'
    ? 'No approved version exists yet. Review the current draft above, or compare saved revisions below to inspect feedback iterations.'
    : diff.has_changes
      ? 'Review the exact changes from the last approved version before approving this draft.'
      : 'This draft matches the last approved authority and body after a revert. Fresh human approval is still required because the lifecycle returned to Proposed.';
}

function renderDecisionHumanDiff(diff) {
  if (diff.baseline_kind === 'empty') {
    $('decision-human-diff').innerHTML = '<p class="human-diff-empty">There is no approved baseline to diff. The current draft is shown above; saved draft-to-draft comparisons are available below.</p>';
    return;
  }
  const before = diff.high_level_before || {};
  const after = diff.high_level_after || {};
  const labels = { title: 'Title', context: 'Context', decision: 'Decision' };
  const changed = Object.keys(labels).filter(
    key => humanDiffValue(before[key]) !== humanDiffValue(after[key]),
  );
  if (!changed.length) {
    $('decision-human-diff').innerHTML = diff.body_changed
      ? '<p class="human-diff-empty">The high-level fields are unchanged. Expand the full canonical decision or technical comparison to review body-only changes.</p>'
      : '<p class="human-diff-empty">No high-level wording changed.</p>';
    return;
  }
  $('decision-human-diff').innerHTML = changed.map((key) => `
    <div class="human-diff-field">
      <h4>${labels[key]}</h4>
      <div class="markdown-view">${key === 'title'
        ? renderInlineMarkdownDiff(before[key], after[key])
        : renderSafeMarkdownDiff(before[key], after[key])}</div>
    </div>
  `).join('');
}

function decisionEvidenceDestination(reference) {
  if (/^(?:req_|res_|REQ-|RES-)/.test(reference)) return 'messages';
  if (/^BKL-/.test(reference)) return 'backlog';
  return '';
}

function decisionEvidenceKindLabel(kind) {
  return String(kind || 'evidence')
    .replace(/[_-]+/g, ' ')
    .split(' ')
    .filter(Boolean)
    .map(word => word.charAt(0).toUpperCase() + word.slice(1))
    .join(' ');
}

function renderDecisionEvidenceLinks(evidence) {
  const entries = Object.entries(evidence || {})
    .flatMap(([kind, references]) => (references || []).map(reference => ({
      kind,
      reference: String(reference || ''),
      destination: decisionEvidenceDestination(String(reference || '')),
    })))
    .filter(item => item.reference && item.destination);
  $('decision-evidence-links').innerHTML = entries.map(item => `
    <button type="button" class="ghost decision-evidence-link" data-requires-server
      data-evidence-reference="${escapeHtml(item.reference)}"
      data-evidence-destination="${escapeHtml(item.destination)}"
      title="Open ${escapeHtml(item.reference)} in ${escapeHtml(item.destination === 'messages' ? 'Messages' : 'Backlog')}">
      <span class="decision-evidence-kind">${escapeHtml(decisionEvidenceKindLabel(item.kind))}</span>
      <code>${escapeHtml(item.reference)}</code>
      <span class="decision-evidence-destination">Open in ${item.destination === 'messages' ? 'Messages' : 'Backlog'} →</span>
    </button>
  `).join('');
  $('decision-evidence-links').querySelectorAll('[data-requires-server]').forEach((control) => {
    setServerControlDisabled(control);
  });
}

function decisionVersionOptionLabel(version) {
  const outcome = titleCaseWords(version.outcome || 'draft');
  const date = formatLocalDateTime(version.authored_utc);
  const approved = Number(version.approved_version || 0);
  return [
    `Revision ${version.revision}`,
    approved ? `Approved v${approved}` : outcome,
    approved && !['Approved', 'Accepted'].includes(outcome) ? outcome : '',
    date,
  ].filter(Boolean).join(' · ');
}

function decisionVersionBadge(decision, revisionCount, approvedCount) {
  const status = text(decision.status);
  if (status === 'proposed') {
    return approvedCount
      ? `Draft revision ${revisionCount} · Based on approved v${approvedCount}`
      : `Draft revision ${revisionCount} · No approved version`;
  }
  if (status === 'rejected') {
    return approvedCount
      ? `Rejected revision ${revisionCount} · Approved v${approvedCount} remains`
      : `Rejected revision ${revisionCount} · No approved version`;
  }
  if (status === 'in_force') return `Effective v${approvedCount} · Revision ${revisionCount}`;
  if (status === 'accepted') return `Approved v${approvedCount} · Revision ${revisionCount}`;
  return approvedCount
    ? `Approved v${approvedCount} · Revision ${revisionCount}`
    : `Revision ${revisionCount} · No approved version`;
}

function decisionVersionFieldChanged(beforeVersion, afterVersion, key) {
  const beforeSha = text((beforeVersion.field_sha256 || {})[key]);
  const afterSha = text((afterVersion.field_sha256 || {})[key]);
  if (beforeSha && afterSha) return beforeSha !== afterSha;
  return humanDiffValue(beforeVersion[key]) !== humanDiffValue(afterVersion[key]);
}

function decisionVersionFieldOmitted(version, key) {
  return (version.omitted_fields || []).includes(key);
}

function renderDecisionVersionComparison() {
  const history = (activeDecision && activeDecision.revision_history) || {};
  const versions = history.revisions || [];
  const fromVersion = versions.find(
    item => String(item.revision) === $('decision-version-from').value,
  );
  const toVersion = versions.find(
    item => String(item.revision) === $('decision-version-to').value,
  );
  const output = $('decision-version-comparison');
  if (!fromVersion || !toVersion) {
    output.innerHTML = '<p class="human-diff-empty">Choose two available revisions to compare.</p>';
    return;
  }
  if (Number(fromVersion.revision) >= Number(toVersion.revision)) {
    output.innerHTML = '<p class="human-diff-empty">Choose an earlier revision on the left and a later revision on the right.</p>';
    return;
  }
  const labels = { title: 'Title', context: 'Context', decision: 'Decision' };
  const changed = Object.keys(labels).filter(
    key => decisionVersionFieldChanged(fromVersion, toVersion, key),
  );
  const pieces = changed.map((key) => {
    if (
      decisionVersionFieldOmitted(fromVersion, key)
      || decisionVersionFieldOmitted(toVersion, key)
    ) {
      return `
        <div class="human-diff-field">
          <h4>${labels[key]}</h4>
          <p class="human-diff-empty">${labels[key]} changed, but the wording comparison is not displayed because one or both saved values exceed the bounded display limit.</p>
        </div>
      `;
    }
    return `
      <div class="human-diff-field">
        <h4>${labels[key]}</h4>
        <div class="markdown-view">${key === 'title'
          ? renderInlineMarkdownDiff(fromVersion[key], toVersion[key])
          : renderSafeMarkdownDiff(fromVersion[key], toVersion[key])}</div>
      </div>
    `;
  });
  const bodyChanged = text(fromVersion.body_sha) !== text(toVersion.body_sha);
  if (bodyChanged && fromVersion.body_available && toVersion.body_available) {
    pieces.push(`
      <details class="disclosure">
        <summary>Full canonical Markdown changes</summary>
        <div class="human-diff-field markdown-view decision-version-body-diff">${renderSafeMarkdownDiff(fromVersion.body, toVersion.body)}</div>
      </details>
    `);
  } else if (bodyChanged) {
    pieces.push(`<p class="human-diff-empty">The canonical body changed, but this body comparison is outside the bounded display limit. ${escapeHtml(fromVersion.body_omission || toVersion.body_omission || '')}</p>`);
  }
  if (!pieces.length) {
    pieces.push('<p class="human-diff-empty">No authority-bearing wording or canonical body changed between these revisions.</p>');
  }
  output.innerHTML = pieces.join('');
  const omitted = [...(fromVersion.field_omissions || []), ...(toVersion.field_omissions || [])];
  $('decision-version-history-status').textContent = [
    `Comparing Revision ${fromVersion.revision} with Revision ${toVersion.revision}.`,
    toVersion.reason ? `Later-revision reason: ${toVersion.reason}.` : '',
    omitted.length ? `Some large fields are omitted: ${omitted.join('; ')}` : '',
  ].filter(Boolean).join(' ');
}

function renderDecisionVersionHistory(decision) {
  const history = decision.revision_history || {};
  const revisionCount = Number(
    decision.revision_count || decision.revision || history.total_revisions || 1,
  );
  const approvedCount = Number(
    decision.approved_version_count || decision.approved_version || history.approved_versions || 0,
  );
  $('decision-review-version').textContent = decisionVersionBadge(
    decision, revisionCount, approvedCount,
  );
  const panel = $('decision-version-history');
  if (revisionCount <= 1) {
    panel.hidden = true;
    $('decision-version-comparison').innerHTML = '';
    return;
  }
  panel.hidden = false;
  $('decision-version-history-summary').textContent = `Compare saved revisions · ${revisionCount} revisions`;
  if (history.loading === true) {
    $('decision-version-history-status').textContent = 'Loading verified saved revisions…';
    $('decision-version-from').innerHTML = '';
    $('decision-version-to').innerHTML = '';
    $('decision-version-comparison').innerHTML = '';
    return;
  }
  if (history.available !== true || !(history.revisions || []).length) {
    $('decision-version-history-status').textContent = history.diagnostic
      || 'Canonical revision history is temporarily unavailable.';
    $('decision-version-from').innerHTML = '';
    $('decision-version-to').innerHTML = '';
    $('decision-version-comparison').innerHTML = '';
    return;
  }
  const versions = history.revisions;
  const options = versions.map(version => (
    `<option value="${version.revision}">${escapeHtml(decisionVersionOptionLabel(version))}</option>`
  )).join('');
  $('decision-version-from').innerHTML = options;
  $('decision-version-to').innerHTML = options;
  $('decision-version-from').value = String(versions[Math.max(0, versions.length - 2)].revision);
  $('decision-version-to').value = String(versions[versions.length - 1].revision);
  $('decision-version-history-status').textContent = history.omitted_revisions
    ? `${history.omitted_revisions} older revision(s) are outside the bounded display window.`
    : '';
  renderDecisionVersionComparison();
}

async function openDecisionEvidenceReference(reference, destination) {
  if (destination === 'messages') {
    switchTab('messages');
    $('message-search').value = reference;
    $('message-id').value = reference;
    return lookupMessage();
  }
  if (destination === 'backlog') {
    switchTab('backlog');
    clearBacklogFilterInputs();
    $('backlog-search').value = reference;
    return loadBacklog();
  }
  return null;
}

function renderDecisionReview(decision) {
  const meta = decision.meta || {};
  const status = decision.status || 'proposed';
  const context = text(meta.context).trim();
  const summary = text(meta.decision).trim();
  const body = text(decision.body).trim();
  $('decision-review').hidden = false;
  stampRepoElement($('decision-review'));
  $('decision-review').dataset.decisionId = decision.id;
  $('decision-review-title').textContent = `${decision.id} · ${decision.title || 'Untitled decision'}`;
  $('decision-review-status').textContent = titleCaseWords(status);
  $('decision-review-meta').textContent = [
    titleCaseWords(decision.tier),
    decision.owner ? `Owner: ${decision.owner}` : '',
    decision.applicability_scope ? `Scope: ${titleCaseWords(decision.applicability_scope)}` : '',
    formatLocalDateTime(decision.display_utc || decision.status_utc || decision.proposed_utc),
  ].filter(Boolean).join(' · ');
  const highLevel = [];
  if (context) {
    highLevel.push(`<section><h4>Context</h4><div class="markdown-view">${renderSafeMarkdown(context)}</div></section>`);
  }
  if (summary) {
    highLevel.push(`<section><h4>Decision</h4><div class="markdown-view">${renderSafeMarkdown(summary)}</div></section>`);
  }
  if (!highLevel.length) {
    highLevel.push(`<section><h4>Decision</h4><div class="markdown-view">${renderSafeMarkdown(body || decision.title || 'No human-facing summary is available.')}</div></section>`);
  }
  $('decision-review-summary').innerHTML = highLevel.join('');
  $('decision-canonical-details').hidden = false;
  $('decision-canonical-markdown').innerHTML = renderSafeMarkdown(body || [
    `# ${decision.id} — ${decision.title || ''}`,
    context ? `## Context\\n${context}` : '',
    summary ? `## Decision\\n${summary}` : '',
  ].filter(Boolean).join('\\n\\n'));
  $('decision-canonical-details').open = !context && !summary;
  renderDecisionVersionHistory(decision);
  renderDecisionRevisionDiff(decision.revision_diff);
  const terminal = ['superseded', 'retired', 'rejected'].includes(status);
  $('edit-decision').hidden = terminal;
  $('decision-human-actions').hidden = status !== 'proposed';
}

function populateDecisionEditor(decision, showEditor = false) {
  activeDecision = decision;
  const meta = decision.meta || {};
  const status = decision.status || 'proposed';
  $('decision-edit-status').textContent = '';
  $('decision-editor').hidden = !showEditor;
  stampRepoElement($('decision-editor'));
  if (typeof renderDecisionReview === 'function') renderDecisionReview(decision);
  $('decision-editor-heading').textContent = `Edit ${decision.id}`;
  $('decision-editor-lifecycle').textContent = status.replace('_', ' ');
  $('decision-edit-id').value = decision.id || '';
  $('decision-edit-id').disabled = true;
  $('decision-edit-tier').value = decision.tier || 'note';
  $('decision-edit-scope').value = decision.applicability_scope || 'manual';
  $('decision-edit-owner').value = decision.owner || '';
	$('decision-edit-actor').value = decisionDefaultActor;
  $('decision-edit-title').value = decision.title || '';
  $('decision-edit-context').value = meta.context || '';
  $('decision-edit-summary').value = meta.decision || '';
  $('decision-edit-globs').value = (decision.affected_code_globs || []).join('\\n');
  $('decision-edit-exemptions').value = (decision.exemptions || []).join('\\n');
  $('decision-edit-generated').value = (decision.generated_artifact_paths || []).join('\\n');
  $('decision-edit-checks').value = (decision.required_checks || []).join('\\n');
  $('decision-edit-verification').value = (decision.verification || [])
    .map(item => item.command || '')
    .filter(Boolean)
    .join('\\n');
  $('decision-edit-assumptions').value = (decision.assumptions || [])
    .map(item => item.text || '')
    .filter(Boolean)
    .join('\\n');
  $('decision-edit-evidence').value = Object.entries(decision.evidence || {})
    .flatMap(([kind, references]) => (references || []).map(reference => `${kind}=${reference}`))
    .join('\\n');
  if (typeof renderDecisionEvidenceLinks === 'function') {
    renderDecisionEvidenceLinks(decision.evidence || {});
  }
  const reviewPolicy = decision.review_policy || {};
  $('decision-edit-reviewers').value = (reviewPolicy.required_reviewers || []).join('\\n');
  $('decision-edit-quorum').value = reviewPolicy.approval_quorum || '';
  $('decision-edit-tags').value = (decision.tags || []).join('\\n');
  $('decision-edit-body').value = decision.body || '';
  if (typeof renderDecisionRevisionDiff === 'function') {
    renderDecisionRevisionDiff(decision.revision_diff);
  }
  $('decision-revision-reason').value = '';
  $('decision-revision-reason-field').hidden = !['accepted', 'in_force'].includes(status);
	$('decision-approval-notes').value = '';
	$('decision-approval-notes').required = status === 'proposed';
  $('save-decision').textContent = 'Save revision';
  activeDecisionEditorSnapshot = decisionRevisionEditorSnapshot();
  refreshDecisionApprovalState();
}

function decisionAssumptionsPayload() {
  const statements = $('decision-edit-assumptions').value
    .split('\\n')
    .map(item => item.trim())
    .filter(Boolean);
  const current = (activeDecision && activeDecision.assumptions) || [];
  const existingByText = new Map(current.map(item => [item.text || '', item]));
  const usedIds = new Set(
    statements
      .map(statement => existingByText.get(statement))
      .filter(Boolean)
      .map(item => item.id),
  );
  let nextId = 1;
  return statements.map(statement => {
    const existing = existingByText.get(statement);
    if (!existing) {
      while (usedIds.has(`A${nextId}`)) nextId += 1;
      const id = `A${nextId}`;
      usedIds.add(id);
      nextId += 1;
      return { id, text: statement, references: [] };
    }
    return {
      id: existing.id,
      text: statement,
      references: existing.references || [],
    };
  });
}

function decisionEvidencePayload() {
  const authored = $('decision-edit-evidence').value;
  if (!activeDecision || !activeDecision.id) return authored;
  const current = Object.entries(activeDecision.evidence || {})
    .flatMap(([kind, references]) => (references || []).map(reference => `${kind}=${reference}`))
    .join('\\n');
  return authored === current ? undefined : authored;
}

function decisionEditorPayload() {
  return {
    id: $('decision-edit-id').value.trim(),
    title: $('decision-edit-title').value,
    tier: $('decision-edit-tier').value,
    applicability_scope: $('decision-edit-scope').value,
    owner: $('decision-edit-owner').value,
	actor: $('decision-edit-actor').value,
    context: $('decision-edit-context').value,
    decision: $('decision-edit-summary').value,
    affected_code_globs: $('decision-edit-globs').value,
    exemptions: $('decision-edit-exemptions').value,
    generated_artifact_paths: $('decision-edit-generated').value,
    required_checks: $('decision-edit-checks').value,
    verification_commands: $('decision-edit-verification').value,
    assumptions: decisionAssumptionsPayload(),
    evidence: decisionEvidencePayload(),
    required_reviewers: $('decision-edit-reviewers').value,
    approval_quorum: $('decision-edit-quorum').value,
    tags: $('decision-edit-tags').value,
    body: $('decision-edit-body').value,
    revision_reason: $('decision-revision-reason').value,
  };
}

function decisionRevisionEditorSnapshot() {
  const payload = decisionEditorPayload();
  const { actor, revision_reason: revisionReason, ...revisionFields } = payload;
  return JSON.stringify(revisionFields);
}

function decisionEditorHasUnsavedChanges() {
  return Boolean(
    activeDecision
    && activeDecisionEditorSnapshot
    && decisionRevisionEditorSnapshot() !== activeDecisionEditorSnapshot
  );
}

function activeDecisionReviewIsCurrent() {
  return Boolean(
    activeDecision
    && activeDecision.id
    && repoElementIsCurrent($('decision-review'))
    && $('decision-review').dataset.decisionId === activeDecision.id
  );
}

function refreshDecisionApprovalState() {
  const decision = activeDecision;
  const reviewCurrent = activeDecisionReviewIsCurrent();
  const status = (decision && decision.status) || '';
  const proposed = reviewCurrent && status === 'proposed';
  const completenessIssues = (decision && decision.completeness_issues) || [];
  const approvalBlockers = (decision && decision.approval_blockers) || [];
  const revisionDiffUnavailable = proposed
    && (!decision.revision_diff || decision.revision_diff.available !== true);
  const unsavedChanges = proposed && decisionEditorHasUnsavedChanges();
  $('decision-human-actions').hidden = !proposed;
  setServerControlDisabled(
    'accept-decision',
    !proposed || completenessIssues.length > 0 || approvalBlockers.length > 0
      || revisionDiffUnavailable || unsavedChanges,
  );
  setServerControlDisabled(
    'reject-decision',
    !proposed || !String((decision && decision.revision_sha) || '').trim() || unsavedChanges,
  );
  $('decision-approval-status').textContent = ['superseded', 'retired', 'rejected'].includes(status)
    ? `This decision is ${status}; create a successor instead of editing it.`
    : proposed && completenessIssues.length
      ? `Approval unavailable: ${completenessIssues.join('; ')}. Add the missing metadata, then save the proposal.`
    : proposed && approvalBlockers.length
        ? `Approval unavailable: ${approvalBlockers.join('; ')}.`
        : revisionDiffUnavailable
          ? 'Approval unavailable until the exact approved-to-pending revision comparison is available. You may still reject the displayed exact revision.'
          : unsavedChanges
            ? 'Approval unavailable because the editor has unsaved proposal changes. Save or discard them, then review the refreshed comparison.'
            : '';
}

function openActiveDecisionEditor() {
  if (
    !activeDecisionReviewIsCurrent()
    || ['superseded', 'retired', 'rejected'].includes(activeDecision.status)
  ) {
    return;
  }
  populateDecisionEditor(activeDecision, true);
  $('decision-editor').scrollIntoView({ behavior: 'smooth', block: 'start' });
  $('decision-edit-title').focus();
}

function cancelDecisionEdit() {
  if (activeDecision && activeDecision.id) {
    populateDecisionEditor(activeDecision, false);
    $('decision-approval-status').textContent = 'Unsaved edits discarded.';
    return;
  }
  resetDecisionEditor(true);
}

async function saveDecision() {
  if (!repoElementIsCurrent($('decision-editor'))) {
    $('decision-edit-status').textContent = 'This decision editor belongs to an earlier repository view; reload the active repository.';
    return { stale: true };
  }
  const requestContext = captureRepoRequestContext();
  const payload = decisionEditorPayload();
  if (!payload.title.trim()) {
    $('decision-edit-status').textContent = 'Title is required.';
    return null;
  }
  const existingId = activeDecision && activeDecision.id;
  $('decision-edit-status').textContent = existingId
    ? `Saving ${existingId}...`
    : 'Creating decision...';
  const result = await api(
    existingId ? '/api/decision/update' : '/api/decision/create',
    { method: 'POST', body: JSON.stringify(existingId ? { ...payload, id: existingId } : payload) },
  );
  if (!requestContext.isCurrent()) return { stale: true };
  if (result.detail_available === false) {
    showDecisionMutationReceipt(result);
    decisionNextId = '';
    await Promise.all([loadStatus(), loadDecisions()]);
    return result;
  }
  populateDecisionEditor(result.decision);
  $('decision-output').textContent = result.block || result.decision.block || '';
  $('decision-edit-status').textContent = existingId
    ? `${existingId} saved as ${result.decision.status}.`
    : `${result.decision.id} created as Proposed.`;
  const readinessStatus = $('decision-approval-status').textContent;
  const savedStatus = existingId
    ? `${existingId} saved as Proposed. Review the displayed revision before approving or rejecting it.`
    : `${result.decision.id} created as Proposed. Review it before approving or rejecting it.`;
  $('decision-approval-status').textContent = [savedStatus, readinessStatus].filter(Boolean).join(' ');
  decisionNextId = '';
  await Promise.all([loadStatus(), loadDecisions()]);
  return result;
}

async function acceptActiveDecision() {
  if (!activeDecisionReviewIsCurrent()) {
    $('decision-approval-status').textContent = 'This decision review belongs to an earlier repository view; reload the active repository.';
    return { stale: true };
  }
  const requestContext = captureRepoRequestContext();
  const id = activeDecision && activeDecision.id;
  if (!id) return null;
	if (decisionEditorHasUnsavedChanges()) {
	  refreshDecisionApprovalState();
	  return null;
	}
	if ((activeDecision.completeness_issues || []).length) {
	  refreshDecisionApprovalState();
	  return null;
	}
	if ((activeDecision.approval_blockers || []).length) {
	  refreshDecisionApprovalState();
	  return null;
	}
	if (!activeDecision.revision_diff || activeDecision.revision_diff.available !== true) {
	  refreshDecisionApprovalState();
	  return null;
	}
	const actor = $('decision-review-actor').value.trim();
	if (!actor) {
	  $('decision-approval-status').textContent = 'Choose the approving human identity.';
	  return null;
	}
	const notes = $('decision-approval-notes').value.trim();
	if (!notes) {
	  $('decision-approval-status').textContent = 'Enter a human approval note.';
	  return null;
	}
	const bodySha = (activeDecision.body_sha || '').trim();
	if (!bodySha) {
	  $('decision-approval-status').textContent = 'Reload this decision before approving it.';
	  return null;
	}
	const revisionSha = (activeDecision.revision_sha || '').trim();
	if (!revisionSha) {
	  $('decision-approval-status').textContent = 'Reload this decision before approving it.';
	  return null;
	}
	if (!window.confirm(`Approve ${id} as ${actor}? This binds your approval to the displayed revision. Notes and implementation plans become Accepted; enforceable tiers become In force.`)) {
	  return null;
	}
  $('decision-approval-status').textContent = `Accepting ${id}...`;
  const result = await api('/api/decision/accept', {
    method: 'POST',
	body: JSON.stringify({
	  id,
	  actor,
	  notes,
	  expected_body_sha: bodySha,
	  expected_revision_sha: revisionSha,
	}),
  });
  if (!requestContext.isCurrent()) return { stale: true };
  if (result.detail_available === false) {
    showDecisionMutationReceipt(result);
    await Promise.all([loadStatus(), loadDecisions()]);
    return result;
  }
  populateDecisionEditor(result.decision, false);
  $('decision-output').textContent = result.block || result.decision.block || '';
  $('decision-approval-status').textContent = `${id} is now ${titleCaseWords(result.decision.status)}. ${result.decision.status === 'in_force' ? 'Its configured advisory or required enforcement is active.' : 'It records an approved non-enforcing choice.'}`;
  await Promise.all([loadStatus(), loadDecisions()]);
  return result;
}

async function rejectActiveDecision() {
  if (!activeDecisionReviewIsCurrent()) {
    $('decision-approval-status').textContent = 'This decision review belongs to an earlier repository view; reload the active repository.';
    return { stale: true };
  }
  const requestContext = captureRepoRequestContext();
  const id = activeDecision && activeDecision.id;
  if (!id || activeDecision.status !== 'proposed') return null;
  if (decisionEditorHasUnsavedChanges()) {
    refreshDecisionApprovalState();
    return null;
  }
  const actor = $('decision-review-actor').value.trim();
  const reason = $('decision-approval-notes').value.trim();
  const bodySha = text(activeDecision.body_sha).trim();
  const revisionSha = text(activeDecision.revision_sha).trim();
  if (!actor) {
    $('decision-approval-status').textContent = 'Choose the rejecting human identity.';
    return null;
  }
  if (!reason) {
    $('decision-approval-status').textContent = 'Enter why you are rejecting this proposal.';
    return null;
  }
  if (!revisionSha) {
    $('decision-approval-status').textContent = 'Reload this decision before rejecting it.';
    return null;
  }
  if (!window.confirm(`Reject ${id} as ${actor}? The rejected proposal becomes terminal; reconsideration requires a successor decision.`)) {
    return null;
  }
  $('decision-approval-status').textContent = `Rejecting ${id}…`;
  const result = await api('/api/decision/reject', {
    method: 'POST',
    body: JSON.stringify({
      id,
      actor,
      reason,
      expected_body_sha: bodySha,
      expected_revision_sha: revisionSha,
    }),
  });
  if (!requestContext.isCurrent()) return { stale: true };
  if (result.detail_available === false) {
    showDecisionMutationReceipt(result);
    await Promise.all([loadStatus(), loadDecisions()]);
    return result;
  }
  populateDecisionEditor(result.decision, false);
  $('decision-output').textContent = result.block || result.decision.block || '';
  $('decision-approval-status').textContent = `${id} was rejected. Create a successor decision if the choice is reconsidered.`;
  await Promise.all([loadStatus(), loadDecisions()]);
  return result;
}

async function loadKanban(resultOverride = null) {
  const requestContext = captureRepoRequestContext();
  const result = resultOverride || await api('/api/backlog/kanban');
  if (!requestContext.isCurrent()) return { stale: true };
  const lanes = Object.entries(result.lanes).sort(([a], [b]) => a.localeCompare(b));
  $('kanban').innerHTML = lanes.map(([lane, items]) => `
    <div class="lane" data-requires-server-gesture data-lane="${escapeHtml(lane)}" data-repo-id="${escapeHtml(activeRepoId)}" data-repo-generation="${repoViewGeneration}">
      <h3>${escapeHtml(lane)} <span class="muted">${items.length}</span></h3>
      ${items.map((item) => `
        <div class="card" draggable="true" data-requires-server-gesture data-backlog-id="${escapeHtml(item.id)}" data-repo-id="${escapeHtml(activeRepoId)}" data-repo-generation="${repoViewGeneration}">
          <small>${escapeHtml(item.id)} ${escapeHtml(item.priority || '')}</small>
          <strong>${escapeHtml(item.title)}</strong>
          ${item.wave || item.release_phase ? `<small>Wave ${escapeHtml(item.wave || item.release_phase)}</small>` : ''}
          <small>${escapeHtml(item.status)}</small>
        </div>
      `).join('')}
    </div>
  `).join('') || '<p class="muted">No backlog lanes.</p>';
  setServerGestureAvailability($('kanban'));
  $('kanban-status').textContent = `Showing ${lanes.length} lane(s) for the active repository.`;
  return result;
}

function priorityOptions(value) {
  const current = text(value);
  const options = ['P0', 'P1', 'P2', 'P3', ''];
  if (current && !options.includes(current)) options.unshift(current);
  return options.map((option) => `
    <option value="${escapeHtml(option)}" ${option === current ? 'selected' : ''}>${escapeHtml(option || '-')}</option>
  `).join('');
}

function describeBacklogFilters(params) {
  const entries = Array.from(params.entries());
  if (!entries.length) return '';
  return ` for ${entries.map(([key, value]) => `${key}=${value}`).join(', ')}`;
}

async function updateBacklogItem(id, field, value, sourceElement = null) {
  if (sourceElement && !repoElementIsCurrent(sourceElement)) {
    $('backlog-edit-status').textContent = 'This backlog control belongs to an earlier repository view; reload the active repository.';
    return { stale: true };
  }
  const requestContext = captureRepoRequestContext();
  $('backlog-edit-status').textContent = `Saving ${id} ${field}...`;
  await api('/api/backlog/update', {
    method: 'POST',
    body: JSON.stringify({ id, [field]: value }),
  });
  if (!requestContext.isCurrent()) return { stale: true };
  $('backlog-edit-status').textContent = `Saved ${id} ${field}`;
  await loadSnapshot();
  return { stale: false };
}

async function copyText(value) {
  await navigator.clipboard.writeText(value || '');
}

let copyStartStatusTimer = null;

async function copyStartCommand() {
  const button = $('copy-start-command');
  const status = $('copy-start-status');
  window.clearTimeout(copyStartStatusTimer);
  try {
    await copyText($('start-command').textContent.trim());
    button.textContent = 'Copied \u2713';
    status.classList.remove('error');
    status.textContent = 'Command copied';
  } catch (error) {
    button.textContent = 'Copy failed';
    status.classList.add('error');
    status.textContent = `Clipboard error: ${error.message || error}`;
  }
  copyStartStatusTimer = window.setTimeout(() => {
    button.textContent = 'Copy';
    status.classList.remove('error');
    status.textContent = '';
  }, 2200);
}

async function recheckServer() {
  const status = $('recheck-server-status');
  status.classList.remove('error');
  status.textContent = 'Checking...';
  await checkServerConnection();
  const online = $('server-connection').classList.contains('online');
  if (!online && MANAGED_SERVICE) {
    status.textContent = 'Loading the latest private bookmark...';
    window.location.replace($('managed-bookmark-link').href);
    return;
  }
  status.classList.toggle('error', !online);
  status.textContent = online ? 'Connected' : 'Still reconnecting';
}

function configureLaunchPanel() {
  $('manual-launch-panel').hidden = MANAGED_SERVICE;
  $('managed-launch-panel').hidden = !MANAGED_SERVICE;
  $('workbench-mode').textContent = MANAGED_SERVICE ? 'Managed service' : 'Manual fallback';
  $('workbench-endpoint').textContent = API_BASE || 'Current server page';
}

function switchTab(tab) {
  document.querySelectorAll('.tab-button').forEach((button) => {
    button.classList.toggle('active', button.getAttribute('data-tab') === tab);
  });
  document.querySelectorAll('.tab-panel').forEach((panel) => {
    panel.classList.toggle('active', panel.id === `tab-${tab}`);
  });
}

function clearBacklogFilterInputs() {
  [
    'backlog-search',
    'backlog-status-filter',
    'backlog-lane-filter',
    'backlog-priority-filter',
    'backlog-owner-filter',
    'backlog-type-filter',
    'backlog-scope-filter',
    'backlog-wave-filter',
    'backlog-origin-filter',
  ].forEach((id) => { $(id).value = ''; });
  $('backlog-quick-filter').value = '';
}

function applyBacklogQuickFilter(filter) {
  if (!serverActionAvailable()) return;
  clearBacklogFilterInputs();
  $('backlog-quick-filter').value = filter;
  switchTab('backlog');
  loadBacklog().catch((error) => {
    $('backlog-edit-status').textContent = error.message;
  });
}

function applyBacklogColumnFilter(field, value) {
  if (!serverActionAvailable()) return;
  clearBacklogFilterInputs();
  const target = {
    status: 'backlog-status-filter',
    lane: 'backlog-lane-filter',
    priority: 'backlog-priority-filter',
  }[field];
  if (!target) return;
  $(target).value = value;
  switchTab('backlog');
  loadBacklog().catch((error) => {
    $('backlog-edit-status').textContent = error.message;
  });
}

function clearMessageFilterInputs() {
  $('message-search').value = '';
  $('message-status-filter').value = '';
  $('message-kind-filter').value = '';
  $('message-feature-filter').value = '';
  $('message-origin-filter').value = '';
  $('message-id').value = '';
}

function clearMessageSearchResult() {
  lastMessage = null;
  $('message-exact-search').open = false;
  $('message-edit-status').textContent = '';
  $('message-output').textContent = '';
  $('message-markdown').innerHTML = '';
  $('message-markdown').hidden = true;
  $('message-technical-record').hidden = true;
  $('message-status-panel').style.display = 'none';
  clearRepoElementStamp($('message-status-panel'));
}

function clearDecisionFilterInputs() {
  $('decision-search').value = '';
  $('decision-status-filter').value = '';
  $('decision-tier-filter').value = '';
}

function clearDispatchFilterInputs() {
  $('dispatch-search').value = '';
}

function applyMessageFilter({ kind = '', feature = '', status = '' } = {}) {
  if (!serverActionAvailable()) return;
  clearMessageFilterInputs();
  $('message-feature-filter').value = feature || '';
  $('message-status-filter').value = status || '';
  $('message-kind-filter').value = kind || '';
  switchTab('messages');
  loadMessages().catch((error) => {
    $('message-edit-status').textContent = error.message;
  });
}

function addScreenshotPaths(paths) {
  detachFeedbackSubmission();
  const current = $('fb-screenshots').value.trim();
  const next = paths.map((path) => `- ${path}`).join('\\n');
  $('fb-screenshots').value = [current, next].filter(Boolean).join('\\n');
  renderAttachmentList();
  rememberFeedbackDraft();
}

function readFileAsDataUrl(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result);
    reader.onerror = () => reject(reader.error || new Error(`Unable to read ${file.name}`));
    reader.readAsDataURL(file);
  });
}

async function uploadAttachmentFiles(fileList) {
  const requestContext = captureRepoRequestContext();
  const files = Array.from(fileList || []);
  if (!files.length) return;
  const oversized = files.find((file) => file.size > MAX_ATTACHMENT_BYTES);
  if (oversized) {
    throw new Error(
      `${oversized.name} is ${(oversized.size / (1024 * 1024)).toFixed(1)} MB; `
      + `the per-file limit is ${MAX_ATTACHMENT_BYTES / (1024 * 1024)} MB. `
      + 'Existing attachment references were preserved.',
    );
  }
  const totalBytes = files.reduce((total, file) => total + file.size, 0);
  if (totalBytes > MAX_ATTACHMENT_TOTAL_BYTES) {
    throw new Error(
      `This batch is ${(totalBytes / (1024 * 1024)).toFixed(1)} MB; `
      + `the total limit is ${MAX_ATTACHMENT_TOTAL_BYTES / (1024 * 1024)} MB. `
      + 'Existing attachment references were preserved.',
    );
  }
  $('attachment-upload-status').textContent = `Uploading ${files.length} file(s)...`;
  let payloadFiles;
  try {
    payloadFiles = await Promise.all(files.map(async (file) => ({
      name: file.name,
      data_url: await readFileAsDataUrl(file),
    })));
  } catch (error) {
    if (!requestContext.isCurrent()) return { stale: true };
    throw error;
  }
  if (!requestContext.isCurrent()) return { stale: true };
  let result;
  try {
    result = await api('/api/attachments/upload', {
      method: 'POST',
      body: JSON.stringify({ files: payloadFiles }),
    }, requestContext.repoId);
  } catch (error) {
    if (!requestContext.isCurrent()) return { stale: true };
    throw error;
  }
  if (!requestContext.isCurrent()) return { stale: true };
  addScreenshotPaths(result.saved || []);
  $('attachment-upload-status').textContent = `Saved ${result.saved.length} file(s) to ${result.directory}`;
  return result;
}

document.addEventListener('click', (event) => {
  const button = event.target.closest('button');
  if (!button || button.disabled) return;
  button.classList.remove('button-click-feedback');
  void button.offsetWidth;
  button.classList.add('button-click-feedback');
  window.setTimeout(() => button.classList.remove('button-click-feedback'), 240);
});

document.querySelectorAll('.tab-button').forEach((button) => {
  button.addEventListener('click', () => switchTab(button.getAttribute('data-tab')));
});
document.querySelectorAll('[data-dashboard-tab]').forEach((button) => {
  button.addEventListener('click', () => switchTab(button.getAttribute('data-dashboard-tab')));
});
FEEDBACK_INPUT_IDS.forEach((id) => {
  $(id).addEventListener('input', () => {
    detachFeedbackSubmission();
    rememberFeedbackDraft();
  });
  $(id).addEventListener('change', () => {
    detachFeedbackSubmission();
    rememberFeedbackDraft();
  });
});
document.querySelectorAll('[data-sort-table][data-sort-key]').forEach((header) => {
  header.addEventListener('click', () => {
    setTableSort(header.getAttribute('data-sort-table'), header.getAttribute('data-sort-key'));
  });
  header.addEventListener('keydown', (event) => {
    if (event.key !== 'Enter' && event.key !== ' ') return;
    event.preventDefault();
    setTableSort(header.getAttribute('data-sort-table'), header.getAttribute('data-sort-key'));
  });
  header.setAttribute('tabindex', '0');
  header.setAttribute('role', 'button');
});
$('tab-dashboard').addEventListener('click', (event) => {
  const metric = event.target.closest('[data-backlog-filter]');
  if (metric) {
    if (serverGestureBlocked(metric)) return;
    applyBacklogQuickFilter(metric.getAttribute('data-backlog-filter'));
    return;
  }
  const messageMetric = event.target.closest('[data-message-feature]');
  if (messageMetric) {
    if (serverGestureBlocked(messageMetric)) return;
    applyMessageFilter({
      kind: messageMetric.getAttribute('data-message-kind') || '',
      feature: messageMetric.getAttribute('data-message-feature') || '',
      status: messageMetric.getAttribute('data-message-status') || '',
    });
    return;
  }
  const breakdown = event.target.closest('[data-backlog-field][data-backlog-value]');
  if (breakdown) {
    if (serverGestureBlocked(breakdown)) return;
    applyBacklogColumnFilter(
      breakdown.getAttribute('data-backlog-field'),
      breakdown.getAttribute('data-backlog-value'),
    );
  }
});
$('submit-feedback').addEventListener('click', () => submitFeedback().catch((error) => {
  $('feedback-output').textContent = error.message;
  $('feedback-submit-status').textContent = error.networkFailure
    ? 'Connection lost during submit. Retry when online; the submission receipt prevents duplicates.'
    : 'Submit failed';
}));
$('clear-feedback').addEventListener('click', () => {
  clearFeedbackWithConfirmation().catch((error) => {
    $('feedback-submit-status').textContent = error.message || String(error);
  });
});
$('destructive-confirmation-acknowledgement').addEventListener('change', (event) => {
  $('destructive-confirmation-apply').disabled = !event.target.checked;
});
$('draft-feedback').addEventListener('click', () => draftFeedback().catch((error) => {
  $('feedback-output').textContent = error.message;
  $('feedback-submit-status').textContent = 'Draft failed';
}));
$('copy-feedback').addEventListener('click', () => ensureDraft().then((draft) => {
  if (draft && draft.stale === true) return draft;
  return copyText(draft.markdown).then(() => {
    $('feedback-submit-status').textContent = 'Copied draft';
    return draft;
  });
}).catch((error) => {
  $('feedback-output').textContent = error.message;
  $('feedback-submit-status').textContent = 'Copy failed';
}));
$('copy-request').addEventListener('click', () => ensureDraft().then((draft) => {
  if (draft && draft.stale === true) return draft;
  return copyText(draft.request.body).then(() => {
    $('feedback-submit-status').textContent = 'Copied agent request';
    return draft;
  });
}).catch((error) => {
  $('feedback-output').textContent = error.message;
  $('feedback-submit-status').textContent = 'Copy failed';
}));
$('lookup-message').addEventListener('click', () => lookupMessage().catch((error) => {
  $('message-output').textContent = error.message;
}));
$('search-messages').addEventListener('click', () => {
  withButtonFeedback($('search-messages'), () => loadMessages(), {
    busyLabel: 'Applying…', successLabel: 'Applied ✓',
  }).catch((error) => { $('message-edit-status').textContent = error.message; });
});
$('reset-message-filters').addEventListener('click', () => {
  clearMessageFilterInputs();
  clearMessageSearchResult();
  withButtonFeedback($('reset-message-filters'), () => loadMessages(), {
    busyLabel: 'Resetting…', successLabel: 'Reset ✓',
  }).catch((error) => { $('message-edit-status').textContent = error.message; });
});
$('reload-messages').addEventListener('click', () => {
  withButtonFeedback(
    $('reload-messages'),
    () => loadMessages(),
    { busyLabel: 'Refreshing…', successLabel: 'Refreshed ✓' },
  ).catch((error) => {
    $('message-edit-status').textContent = error.message;
  });
});
$('message-body').addEventListener('click', (event) => {
  const row = event.target.closest('[data-message-id]');
  if (!row || serverGestureBlocked(row) || !repoElementIsCurrent(row)) return;
  $('message-id').value = row.getAttribute('data-message-id');
  lookupMessage().catch((error) => {
    $('message-output').textContent = error.message;
  });
});
$('save-message-status').addEventListener('click', () => updateMessageStatus().catch((error) => {
  $('message-edit-status').textContent = error.message;
}));
$('search-backlog').addEventListener('click', () => {
  withButtonFeedback($('search-backlog'), () => loadBacklog(), {
    busyLabel: 'Applying…', successLabel: 'Applied ✓',
  }).catch((error) => { $('backlog-edit-status').textContent = error.message; });
});
$('clear-backlog-filters').addEventListener('click', () => {
  clearBacklogFilterInputs();
  withButtonFeedback($('clear-backlog-filters'), () => loadBacklog(), {
    busyLabel: 'Resetting…', successLabel: 'Reset ✓',
  }).catch((error) => { $('backlog-edit-status').textContent = error.message; });
});
$('reload-backlog').addEventListener('click', () => {
  withButtonFeedback(
    $('reload-backlog'),
    () => loadBacklog(),
    { busyLabel: 'Refreshing…', successLabel: 'Refreshed ✓' },
  ).catch((error) => {
    $('backlog-edit-status').textContent = error.message;
  });
});
$('search-dispatches').addEventListener('click', () => {
  const summary = renderDispatchRows(dispatchRows);
  $('dispatch-status').textContent = `Showing ${summary.chains} matching work chain${summary.chains === 1 ? '' : 's'} containing ${summary.records} dispatch record${summary.records === 1 ? '' : 's'}.`;
});
$('reset-dispatch-filters').addEventListener('click', () => {
  clearDispatchFilterInputs();
  const summary = renderDispatchRows(dispatchRows);
  $('dispatch-status').textContent = `Showing all ${summary.chains} work chain${summary.chains === 1 ? '' : 's'} containing ${dispatchRows.length} dispatch record${dispatchRows.length === 1 ? '' : 's'}.`;
});
$('reload-dispatches').addEventListener('click', () => {
  withButtonFeedback(
    $('reload-dispatches'),
    () => loadDispatches(),
    { busyLabel: 'Refreshing…', successLabel: 'Refreshed ✓' },
  ).catch((error) => {
    $('dispatch-status').textContent = error.message;
  });
});
	document.querySelectorAll('input[name="dispatch-source-mode"]').forEach((control) => {
	  control.addEventListener('change', () => {
	    setDispatchSourceMode(control.value, { announce: true });
	    if (control.value === 'existing') {
	      scheduleDispatchRequestSearch({ immediate: true });
	    } else {
	      clearDispatchRequestLookup();
	    }
	  });
	});
	$('dispatch-agent').addEventListener('change', () => {
	  syncDispatchAgent({ announce: true });
	  if (dispatchSourceMode() === 'existing') scheduleDispatchRequestSearch({ immediate: true });
	});
	$('dispatch-profile').addEventListener('change', () => {
	  syncDispatchAgent();
	  if (dispatchSourceMode() === 'existing') scheduleDispatchRequestSearch({ immediate: true });
	});
	$('dispatch-message-id').addEventListener('input', () => scheduleDispatchRequestSearch());
	$('dispatch-message-id').addEventListener('focus', () => {
	  if (dispatchSourceMode() === 'existing' && $('dispatch-request-matches').hidden) {
	    scheduleDispatchRequestSearch({ immediate: true });
	  }
	});
	$('dispatch-request-matches').addEventListener('click', (event) => {
	  const match = event.target.closest('[data-dispatch-request-id]');
	  if (match) selectDispatchRequest(match.getAttribute('data-dispatch-request-id'));
	});
	$('dispatch-purpose').addEventListener('change', () => syncDispatchPurpose({ announce: true }));
	$('dispatch-subject-type').addEventListener('change', syncDispatchReviewTarget);
	$('clear-dispatch-retry').addEventListener('click', clearDispatchRetrySelection);
	$('clear-dispatch-followup').addEventListener('click', () => {
	  clearDispatchFollowUp({ announce: true });
	});
	syncDispatchPurpose();
	setDispatchSourceMode('new');
$('dispatch-editor').addEventListener('submit', (event) => {
  event.preventDefault();
  if (!serverActionAvailable()) return;
  withButtonFeedback(
    $('start-dispatch'),
    () => startDispatch(),
    { busyLabel: 'Dispatching…', successLabel: 'Completed ✓' },
  ).catch((error) => {
    $('dispatch-status').textContent = error.message;
    $('dispatch-output').textContent = error.message;
  });
});
$('record-dispatch-assurance').addEventListener('click', () => {
  if (!serverActionAvailable()) return;
  withButtonFeedback(
    $('record-dispatch-assurance'),
    () => recordDispatchAssurance(),
    { busyLabel: 'Recording…', successLabel: 'Recorded ✓' },
  ).catch((error) => {
    $('dispatch-status').textContent = error.message;
    $('dispatch-output').textContent = error.message;
  });
});
$('cancel-dispatch').addEventListener('click', () => {
  if (!serverActionAvailable()) return;
  withButtonFeedback(
    $('cancel-dispatch'),
    () => cancelDispatch(),
    { busyLabel: 'Cancelling…', successLabel: 'Cancelled ✓' },
  ).catch((error) => {
    $('dispatch-status').textContent = error.message;
    $('dispatch-output').textContent = error.message;
  });
});
$('dispatch-body').addEventListener('click', (event) => {
  const chainButton = event.target.closest('[data-dispatch-chain-toggle]');
  if (chainButton) {
    event.stopPropagation();
    const chainId = chainButton.getAttribute('data-dispatch-chain-toggle');
    if ($('dispatch-search').value.trim()) {
      $('dispatch-status').textContent = 'Follow-ups stay expanded while Dispatch search is active. Reset the filter to collapse this chain.';
      return;
    }
    if (expandedDispatchChains.has(chainId)) {
      expandedDispatchChains.delete(chainId);
    } else {
      expandedDispatchChains.add(chainId);
    }
    const summary = renderDispatchRows(dispatchRows);
    $('dispatch-status').textContent = `${expandedDispatchChains.has(chainId) ? 'Expanded' : 'Collapsed'} the selected work chain. Showing ${summary.visibleRecords} visible dispatch record${summary.visibleRecords === 1 ? '' : 's'}.`;
    return;
  }
  const continueButton = event.target.closest('[data-dispatch-continue-response]');
  if (continueButton && !serverGestureBlocked(continueButton)) {
    event.stopPropagation();
    const row = continueButton.closest('[data-dispatch-policy]');
    if (!row || !repoElementIsCurrent(row)) return;
    try {
      prepareDispatchFollowUp(
        continueButton.getAttribute('data-dispatch-continue-response'),
        dispatchRows.find(item => item.policy_id === row.getAttribute('data-dispatch-policy')),
      );
    } catch (error) {
      $('dispatch-status').textContent = error.message || String(error);
    }
    return;
  }
  const messageButton = event.target.closest('[data-dispatch-message]');
  if (messageButton && !messageButton.disabled) {
    event.stopPropagation();
    const messageId = messageButton.getAttribute('data-dispatch-message');
    switchTab('messages');
    $('message-search').value = messageId;
    $('message-id').value = messageId;
    lookupMessage().catch((error) => { $('message-output').textContent = error.message; });
    return;
  }
  if (event.target.closest('details')) return;
  const row = event.target.closest('[data-dispatch-policy]');
  if (!row || serverGestureBlocked(row) || !repoElementIsCurrent(row)) return;
  lookupDispatch(row.getAttribute('data-dispatch-policy')).catch((error) => {
    $('dispatch-status').textContent = error.message;
  });
});
$('dispatch-result-card').addEventListener('click', (event) => {
  const continueButton = event.target.closest('[data-dispatch-continue-response]');
  if (continueButton && !serverGestureBlocked(continueButton)) {
    try {
      prepareDispatchFollowUp(continueButton.getAttribute('data-dispatch-continue-response'));
    } catch (error) {
      $('dispatch-status').textContent = error.message || String(error);
    }
    return;
  }
  const messageButton = event.target.closest('[data-dispatch-result-message]');
  if (!messageButton || serverGestureBlocked(messageButton)) return;
  const messageId = messageButton.getAttribute('data-dispatch-result-message');
  switchTab('messages');
  $('message-search').value = messageId;
  $('message-id').value = messageId;
  lookupMessage().catch((error) => { $('message-output').textContent = error.message; });
});
$('message-markdown').addEventListener('click', (event) => {
  const continueButton = event.target.closest('[data-message-continue-response]');
  if (!continueButton || serverGestureBlocked(continueButton)) return;
  try {
    prepareDispatchFollowUp(continueButton.getAttribute('data-message-continue-response'));
  } catch (error) {
    $('message-edit-status').textContent = error.message || String(error);
  }
});
$('dispatch-assurance-current').addEventListener('click', (event) => {
  const messageButton = event.target.closest('[data-assurance-message]');
  if (messageButton) {
    const messageId = messageButton.dataset.assuranceMessage || '';
    switchTab('messages');
    $('message-search').value = messageId;
    $('message-id').value = messageId;
    lookupMessage().catch((error) => { $('message-output').textContent = error.message; });
    return;
  }
  const actionButton = event.target.closest('[data-assurance-action]');
  if (!actionButton || !serverActionAvailable() || actionButton.disabled) return;
  const action = actionButton.dataset.assuranceAction;
  if (!['flag', 'retire'].includes(action)) return;
  const card = actionButton.closest('[data-assurance-response]');
  if (!card) return;
  if (action === 'retire' && !window.confirm(
    'Retire this flagged review evidence without a replacement? This does not create a passing review and cannot be undone.'
  )) return;
  withButtonFeedback(
    actionButton,
    () => transitionDispatchAssurance(card, action),
    {
      busyLabel: action === 'flag' ? 'Flagging…' : 'Retiring…',
      successLabel: action === 'flag' ? 'Flagged ✓' : 'Retired ✓',
    },
  ).catch((error) => {
    $('dispatch-status').textContent = error.message;
    $('dispatch-output').textContent = error.message;
  });
});
$('search-decisions').addEventListener('click', () => {
  decisionFocusedId = '';
  withButtonFeedback($('search-decisions'), () => loadDecisions(), {
    busyLabel: 'Applying…', successLabel: 'Applied ✓',
  }).catch((error) => { $('decision-list-status').textContent = error.message; });
});
$('reset-decision-filters').addEventListener('click', () => {
  clearDecisionFilterInputs();
  decisionFocusedId = '';
  withButtonFeedback($('reset-decision-filters'), () => loadDecisions(), {
    busyLabel: 'Resetting…', successLabel: 'Reset ✓',
  }).catch((error) => { $('decision-list-status').textContent = error.message; });
});
$('reload-decisions').addEventListener('click', () => {
  withButtonFeedback(
    $('reload-decisions'),
    () => loadDecisions(),
    { busyLabel: 'Refreshing…', successLabel: 'Refreshed ✓' },
  ).catch((error) => {
    $('decision-output').textContent = error.message;
  });
});
$('show-all-decisions').addEventListener('click', showAllDecisions);
$('decision-review').addEventListener('click', (event) => {
  const retry = event.target.closest('[data-retry-decision]');
  if (!retry || retry.disabled) return;
  const id = retry.getAttribute('data-retry-decision');
  withButtonFeedback(retry, () => lookupDecision(id), {
    busyLabel: 'Retrying…', successLabel: 'Loaded ✓', failureLabel: 'Retry failed',
  }).catch((error) => {
    $('decision-list-status').textContent = `Could not load ${id}: ${error.message}`;
  });
});
$('retry-decision-review').addEventListener('click', () => {
  if (!activeDecisionReviewIsCurrent()) return;
  const id = activeDecision.id;
  const revisionSha = activeDecision.revision_sha;
  withButtonFeedback(
    $('retry-decision-review'),
    () => loadDecisionReviewData(id, revisionSha),
    { busyLabel: 'Retrying…', successLabel: 'Loaded ✓', failureLabel: 'Retry failed' },
  ).catch((error) => {
    $('decision-review-loading').textContent = `${id} is ready, but its revision comparisons could not be loaded: ${error.message}`;
  });
});
$('decision-version-from').addEventListener('change', renderDecisionVersionComparison);
$('decision-version-to').addEventListener('change', renderDecisionVersionComparison);
$('decision-change-mode').addEventListener('change', syncDecisionChangeBase);
$('reload-decision-applicability').addEventListener('click', () => {
  withButtonFeedback(
    $('reload-decision-applicability'),
    () => loadDecisionApplicability(),
    { busyLabel: 'Evaluating…', successLabel: 'Evaluated ✓' },
  ).catch((error) => {
      $('decision-applicability-status').textContent = error.message;
      $('decision-applicability-results').textContent = '';
    });
});
$('new-decision').addEventListener('click', () => showNewDecisionEditor().catch((error) => {
  $('decision-output').textContent = error.message;
}));
$('decision-editor').addEventListener('submit', (event) => {
  event.preventDefault();
  if (!serverActionAvailable()) return;
  saveDecision().catch((error) => {
    $('decision-edit-status').textContent = error.message;
  });
});
['input', 'change'].forEach((eventName) => {
  $('decision-editor').addEventListener(eventName, (event) => {
    if (['decision-edit-actor', 'decision-approval-notes', 'decision-revision-reason']
      .includes(event.target.id)) return;
    refreshDecisionApprovalState();
  });
});
$('accept-decision').addEventListener('click', () => acceptActiveDecision().catch((error) => {
  $('decision-approval-status').textContent = error.message;
}));
$('reject-decision').addEventListener('click', () => rejectActiveDecision().catch((error) => {
  $('decision-approval-status').textContent = error.message;
}));
$('edit-decision').addEventListener('click', openActiveDecisionEditor);
$('cancel-decision-edit').addEventListener('click', cancelDecisionEdit);
$('decision-body').addEventListener('click', (event) => {
  const row = event.target.closest('[data-decision-id]');
  if (!row || serverGestureBlocked(row) || !repoElementIsCurrent(row)) return;
  lookupDecision(row.getAttribute('data-decision-id')).catch((error) => {
    $('decision-output').textContent = error.message;
    $('decision-list-status').textContent = `Could not load the selected decision: ${error.message}`;
  });
});
$('decision-evidence-links').addEventListener('click', (event) => {
  const button = event.target.closest('[data-evidence-reference]');
  if (!button || button.disabled || !repoElementIsCurrent($('decision-editor'))) return;
  const reference = button.getAttribute('data-evidence-reference');
  const destination = button.getAttribute('data-evidence-destination');
  openDecisionEvidenceReference(reference, destination).catch((error) => {
    $('decision-output').textContent = error.message;
  });
});
$('reload-kanban').addEventListener('click', () => {
  withButtonFeedback(
    $('reload-kanban'),
    () => loadKanban(),
    { busyLabel: 'Reloading…', successLabel: 'Reloaded ✓' },
  ).catch((error) => {
    $('kanban-status').textContent = `Reload failed: ${error.message}`;
  });
});
$('copy-start-command').addEventListener('click', copyStartCommand);
$('recheck-server').addEventListener('click', () => {
  recheckServer().catch((error) => {
    $('recheck-server-status').classList.add('error');
    $('recheck-server-status').textContent = error.message || String(error);
  });
});
$('repo-selector').addEventListener('change', (event) => {
  switchRepo(event.target.value).catch((error) => {
    $('status').innerHTML = `<span class="badge">repo switch error ${escapeHtml(error.message)}</span>`;
  });
});
$('pick-attachments').addEventListener('click', () => $('attachment-picker').click());
$('attachment-picker').addEventListener('change', (event) => {
  uploadAttachmentFiles(event.target.files).catch((error) => {
    $('attachment-upload-status').textContent = error.message;
  });
  event.target.value = '';
});
$('clear-attachments').addEventListener('click', () => {
  clearAttachmentPathsWithConfirmation().catch((error) => {
    $('attachment-upload-status').textContent = error.message || String(error);
  });
});
$('retry-pending-feedback').addEventListener('click', () => {
  retryPendingFeedbackSubmission().catch((error) => {
    $('feedback-submit-status').textContent = error.message || String(error);
  });
});
$('abandon-pending-feedback').addEventListener('click', () => {
  abandonPendingFeedbackWithConfirmation().catch((error) => {
    $('feedback-submit-status').textContent = error.message || String(error);
  });
});
$('attachment-dropzone').addEventListener('dragover', (event) => {
  event.preventDefault();
  if (serverGestureBlocked(event.currentTarget)) {
    event.dataTransfer.dropEffect = 'none';
    $('attachment-dropzone').classList.remove('dragover');
    return;
  }
  $('attachment-dropzone').classList.add('dragover');
});
$('attachment-dropzone').addEventListener('dragleave', () => {
  $('attachment-dropzone').classList.remove('dragover');
});
$('attachment-dropzone').addEventListener('drop', (event) => {
  event.preventDefault();
  if (serverGestureBlocked(event.currentTarget)) {
    event.dataTransfer.dropEffect = 'none';
    $('attachment-dropzone').classList.remove('dragover');
    return;
  }
  $('attachment-dropzone').classList.remove('dragover');
  uploadAttachmentFiles(event.dataTransfer.files).catch((error) => {
    $('attachment-upload-status').textContent = error.message;
  });
});
$('backlog-body').addEventListener('change', (event) => {
  const target = event.target.closest('[data-backlog-id][data-backlog-field]');
  if (!target || !serverActionAvailable()) return;
  const row = target.closest('[data-backlog-view-id]');
  if (!repoElementIsCurrent(row)) {
    $('backlog-edit-status').textContent = 'This backlog control belongs to an earlier repository view; reload the active repository.';
    return;
  }
  updateBacklogItem(
    target.getAttribute('data-backlog-id'),
    target.getAttribute('data-backlog-field'),
    target.value,
    row,
  ).catch((error) => {
    $('backlog-edit-status').textContent = error.message;
  });
});
$('backlog-body').addEventListener('click', (event) => {
  if (event.target.closest('[data-backlog-id][data-backlog-field]')) return;
  const row = event.target.closest('[data-backlog-view-id]');
  if (!row || serverGestureBlocked(row) || !repoElementIsCurrent(row)) return;
  lookupBacklogItem(row.getAttribute('data-backlog-view-id')).catch((error) => {
    $('backlog-output').textContent = error.message;
    $('backlog-edit-status').textContent = 'Lookup failed';
  });
});
$('kanban').addEventListener('dragstart', (event) => {
  const card = event.target.closest('[data-backlog-id]');
  if (!card || serverGestureBlocked(card) || !repoElementIsCurrent(card)) return;
  draggedBacklogId = card.getAttribute('data-backlog-id');
  event.dataTransfer.effectAllowed = 'move';
  event.dataTransfer.setData('text/plain', draggedBacklogId);
});
$('kanban').addEventListener('dragend', () => {
  draggedBacklogId = null;
  document.querySelectorAll('.lane.dragover').forEach((lane) => lane.classList.remove('dragover'));
});
$('kanban').addEventListener('dragover', (event) => {
  const lane = event.target.closest('[data-lane]');
  if (!lane || serverGestureBlocked(lane)) return;
  event.preventDefault();
  lane.classList.add('dragover');
  event.dataTransfer.dropEffect = 'move';
});
$('kanban').addEventListener('dragleave', (event) => {
  const lane = event.target.closest('[data-lane]');
  if (lane && !lane.contains(event.relatedTarget)) lane.classList.remove('dragover');
});
$('kanban').addEventListener('drop', (event) => {
  const lane = event.target.closest('[data-lane]');
  if (!lane || serverGestureBlocked(lane) || !repoElementIsCurrent(lane)) return;
  event.preventDefault();
  lane.classList.remove('dragover');
  const itemId = draggedBacklogId || event.dataTransfer.getData('text/plain');
  const newLane = lane.getAttribute('data-lane');
  if (!itemId || !newLane) return;
  updateBacklogItem(itemId, 'lane', newLane, lane).catch((error) => {
    $('backlog-edit-status').textContent = error.message;
  });
});

configureLaunchPanel();
setServerConnection('checking');
restoreFeedbackDraft();
if (restartAttemptRecorded()) {
  beginManagedRestartRecovery();
} else {
  checkServerConnection();
}
setInterval(checkServerConnection, 5000);
window.addEventListener('focus', checkServerConnection);
loadProjects().then(() => {
  resetRepoView();
  restoreFeedbackDraft();
  return loadSnapshot();
}).then(() => recoverPendingFeedbackSubmission()).catch((error) => {
  if (error.manualSuperseded) return;
  $('status').innerHTML = `<span class="badge">startup error ${escapeHtml(error.message)}</span>`;
});
</script>
</body>
</html>
"""
