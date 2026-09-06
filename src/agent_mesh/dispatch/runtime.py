"""Provider-neutral runtime launch adapters for dispatch.

Runtime adapters are the boundary between canonical dispatch state and a local agent process. Most
build sanitized launch specs for the generic launcher; a continuity adapter may instead own a
private stdio protocol through the managed launch hook. Adapters do not acquire leases, append
lifecycle events, post RES messages, or persist raw prompts. That keeps agent-mesh core
platform-agnostic while allowing host controllers to launch Codex or future runtimes through the
same contract.
"""

from __future__ import annotations

import hashlib
import json
import os
import pty
import queue
import re
import select
import stat
import subprocess
import tempfile
import termios
import threading
import time
import tomllib
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Callable

from agent_mesh.config import config_from_agent_dir
from agent_mesh.project_registry import registry_dir
from agent_mesh.core.agent_instances import (
    INSTANCE_ENV_VAR,
    INSTANCE_ID_RE,
    RESERVED_INSTANCE_ENV_VARS,
    AgentInstanceError,
    RuntimeIdentityHandshake,
    allocate_instance_handle,
    normalize_handle_component,
    normalize_instance_label,
    reduce_agent_instances,
    scoped_reference_digest,
)
from agent_mesh.store.read_model import open_read_model

from .adapters import AgentRuntimeAdapter
from .process_io import run_bounded_process
from .types import AgentLaunchResult, AgentLaunchSpec, AgentRunRequest


DEFAULT_CODEX_BINARY = "/Applications/Codex.app/Contents/Resources/codex"
CODEX_APP_SERVER_PROTOCOL = "codex-app-server-v2"
CODEX_APP_SERVER_CLIENT_NAME = "agent-mesh"
CODEX_MANAGED_HOME_VERSION = "v1"
SUPPORTED_CODEX_APP_SERVER_VERSIONS = frozenset({"0.147.0", "0.153.4"})
MAX_APP_SERVER_LINE_CHARS = 4 * 1024 * 1024
MAX_APP_SERVER_STDERR_CHARS = 64 * 1024
MAX_APP_SERVER_PENDING_MESSAGES = 2048
MAX_APP_SERVER_THREAD_PAGES = 100
MAX_CODEX_MANAGED_CONFIG_BYTES = 64 * 1024
MAX_AGENT_PROCESS_STDOUT_BYTES = 4 * 1024 * 1024
MAX_AGENT_PROCESS_STDERR_BYTES = 64 * 1024
APP_SERVER_HANDSHAKE_TIMEOUT_SECONDS = 30
MAX_RUNTIME_WORKSTREAM_CHARS = 32
APP_SERVER_RECOVERY_OBSERVATION_TIMEOUT_SECONDS = 5
CODEX_APP_SERVER_TURN_STATUSES = frozenset(
    {"completed", "failed", "interrupted", "inProgress"}
)
CODEX_APP_SERVER_THREAD_SOURCE_KINDS = (
    "cli",
    "vscode",
    "exec",
    "appServer",
    "subAgent",
    "subAgentReview",
    "subAgentCompact",
    "subAgentThreadSpawn",
    "subAgentOther",
    "unknown",
)
INSTANCE_ID_SEARCH_RE = re.compile(r"AI-\d{8}-\d{2,}")
SUBSCRIPTION_API_CREDENTIALS = (
    "ANTHROPIC_API_KEY",
    "CURSOR_API_KEY",
    "GEMINI_API_KEY",
    "GOOGLE_API_KEY",
    "GOOGLE_APPLICATION_CREDENTIALS",
    "GOOGLE_CLOUD_PROJECT",
    "GOOGLE_CLOUD_LOCATION",
    "GOOGLE_GENAI_USE_VERTEXAI",
    "OPENAI_BASE_URL",
    "OPENAI_API_KEY",
    "OPENAI_ORGANIZATION",
    "OPENAI_PROJECT",
)
PROVIDER_SESSION_ENVIRONMENT_VARIABLES = (
    "ANTIGRAVITY_SESSION_ID",
    "CLAUDE_CODE_SESSION_ID",
    "CODEX_SESSION_ID",
    "CODEX_THREAD_ID",
    "CURSOR_AGENT_SESSION_ID",
    "CURSOR_SESSION_ID",
    "GEMINI_CLI_SESSION_ID",
)
CODEX_APP_SERVER_DISABLED_FEATURES = (
    "apps",
    "auth_elicitation",
    "browser_use",
    "browser_use_external",
    "browser_use_full_cdp_access",
    "computer_use",
    "external_agent_memory_import",
    "hooks",
    "image_generation",
    "in_app_browser",
    "memories",
    "multi_agent",
    "multi_agent_v2",
    "network_proxy",
    "plugin_sharing",
    "plugins",
    "realtime_conversation",
    "recommended_plugins",
    "remote_plugin",
    "skill_mcp_dependency_install",
    "skill_search",
    "standalone_web_search",
    "tool_call_mcp_elicitation",
    "tool_suggest",
)
CODEX_APP_SERVER_CONFIG_OVERRIDES = (
    "analytics.enabled=false",
    'otel.exporter="none"',
    'otel.trace_exporter="none"',
    'otel.metrics_exporter="none"',
    "otel.log_user_prompt=false",
    "mcp_servers={}",
    "model_providers={}",
    'model_provider="openai"',
    'forced_login_method="chatgpt"',
    'chatgpt_base_url="https://chatgpt.com/backend-api/"',
    'shell_environment_policy.inherit="none"',
    "allow_login_shell=false",
    "notify=[]",
    "hooks={}",
    'web_search="disabled"',
    "sandbox_workspace_write.network_access=false",
    "sandbox_workspace_write.writable_roots=[]",
    "tools={}",
    "features={}",
    *(f"features.{feature}=false" for feature in CODEX_APP_SERVER_DISABLED_FEATURES),
    "skills={}",
    "plugins={}",
)


class CodexAppServerError(RuntimeError):
    """A privacy-safe failure at the resumable Codex protocol boundary."""


class CodexAppServerTimeout(CodexAppServerError):
    """The resumable Codex protocol exceeded its bounded deadline."""


def _codex_app_server_argv(binary: str) -> list[str]:
    argv = [binary, "app-server", "--stdio", "--strict-config"]
    for override in CODEX_APP_SERVER_CONFIG_OVERRIDES:
        argv.extend(["--config", override])
    return argv


@dataclass(frozen=True)
class _ProtocolReadFailure:
    code: str


_PROTOCOL_EOF = object()


@dataclass(frozen=True)
class _PreparedCodexSession:
    thread_id: str = field(repr=False, compare=False)
    lifecycle_disposition: str
    newly_created: bool
    child_instance_handle: str = ""
    client: _CodexAppServerClient | None = field(
        default=None, repr=False, compare=False
    )


@dataclass(frozen=True)
class _CodexRuntimeConfig:
    binary: str
    version: str
    model: str
    participant: str
    provider: str
    durable_role: str
    role: str
    sandbox: str
    capabilities: tuple[str, ...]
    authentication_mode: str
    billing_mode: str
    credential_denylist: tuple[str, ...]
    adapter_trust: str
    session_identity_mode: str
    resumable: bool
    concurrent_attachment: bool
    terminal_observation: str
    events_path: Path | None
    project_scope: str


class _CodexAppServerClient:
    """Small bounded JSON-RPC client for Codex app-server stdio.

    Provider thread identifiers only cross this boundary in JSON-RPC stdin/stdout. They never enter
    process arguments, environment variables, result metadata, or exception text.
    """

    def __init__(
        self,
        *,
        binary: str,
        cwd: Path,
        environment: dict[str, str],
        child_instance_handle: str = "",
    ) -> None:
        launch_spec = _finalize_identity_boundary(
            AgentLaunchSpec(
                argv=_codex_app_server_argv(binary),
                cwd=cwd,
                requires_pty=False,
                timeout_seconds=APP_SERVER_HANDSHAKE_TIMEOUT_SECONDS,
                prompt_sha="",
                stdin_text="",
                metadata={"runtime_protocol": CODEX_APP_SERVER_PROTOCOL},
                environment=environment,
                child_instance_handle=child_instance_handle,
            )
        )
        self.argv = tuple(launch_spec.argv)
        self.cwd = launch_spec.cwd
        self.environment = launch_spec.environment
        self._process: subprocess.Popen[str] | None = None
        self._messages: queue.Queue[object] = queue.Queue(
            maxsize=MAX_APP_SERVER_PENDING_MESSAGES
        )
        self._notifications: list[dict[str, object]] = []
        self._protocol_read_failure: _ProtocolReadFailure | None = None
        self._stderr_chunks: list[str] = []
        self._stderr_chars = 0
        self._request_id = 0
        self._threads: list[threading.Thread] = []

    def __enter__(self) -> _CodexAppServerClient:
        try:
            process = subprocess.Popen(
                list(self.argv),
                cwd=self.cwd,
                env=self.environment,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )
        except OSError as exc:
            raise CodexAppServerError("CODEX_APP_SERVER_LAUNCH_FAILED") from exc
        self._process = process
        stdout_thread = threading.Thread(
            target=self._read_stdout,
            name="agent-mesh-codex-app-server-stdout",
            daemon=True,
        )
        stderr_thread = threading.Thread(
            target=self._read_stderr,
            name="agent-mesh-codex-app-server-stderr",
            daemon=True,
        )
        self._threads = [stdout_thread, stderr_thread]
        for thread in self._threads:
            thread.start()
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        del exc_type, exc, traceback
        self.close()

    def initialize(self, *, deadline: float) -> None:
        self.request(
            "initialize",
            {
                "clientInfo": {
                    "name": CODEX_APP_SERVER_CLIENT_NAME,
                    "version": "0.4",
                },
                # Supported Codex app-server versions gate the isolation-bearing
                # ``dynamicTools`` and ``environments`` thread fields behind this
                # negotiated API.
                # Opting in permits those explicit empty values; it does not
                # enable any experimental feature or invoke another method.
                "capabilities": {"experimentalApi": True},
            },
            deadline=deadline,
        )
        self.notify("initialized", {})

    def list_threads(self, *, cwd: Path, deadline: float) -> list[dict[str, object]]:
        threads: list[dict[str, object]] = []
        cursor: str | None = None
        seen_cursors: set[str] = set()
        for _page in range(MAX_APP_SERVER_THREAD_PAGES):
            params: dict[str, object] = {
                "cwd": str(cwd),
                "limit": 100,
                "sortKey": "created_at",
                "sortDirection": "desc",
                # Codex 0.147 may persist a thread started through app-server
                # under another schema-defined runtime source (observed live:
                # ``vscode``). Source is not an identity boundary; exact cwd
                # plus the project-scoped canonical session digest are.
                "sourceKinds": list(CODEX_APP_SERVER_THREAD_SOURCE_KINDS),
                "useStateDbOnly": True,
            }
            if cursor:
                params["cursor"] = cursor
            result = self.request("thread/list", params, deadline=deadline)
            data = result.get("data")
            if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
                raise CodexAppServerError("CODEX_APP_SERVER_THREAD_LIST_INVALID")
            threads.extend(data)
            next_cursor = result.get("nextCursor")
            if next_cursor is None:
                return threads
            if not isinstance(next_cursor, str) or not next_cursor or next_cursor in seen_cursors:
                raise CodexAppServerError("CODEX_APP_SERVER_THREAD_LIST_INVALID")
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        raise CodexAppServerError("CODEX_APP_SERVER_THREAD_LIST_INCOMPLETE")

    def start_thread(
        self, *, params: dict[str, object], deadline: float
    ) -> tuple[str, dict[str, object]]:
        result = self.request("thread/start", params, deadline=deadline)
        thread = result.get("thread")
        if not isinstance(thread, dict):
            raise CodexAppServerError("CODEX_APP_SERVER_THREAD_START_INVALID")
        thread_id = thread.get("id")
        if not isinstance(thread_id, str) or not thread_id:
            raise CodexAppServerError("CODEX_APP_SERVER_THREAD_START_INVALID")
        return thread_id, result

    def resume_thread(
        self,
        thread_id: str,
        *,
        params: dict[str, object],
        deadline: float,
    ) -> dict[str, object]:
        result = self.request(
            "thread/resume",
            {**params, "threadId": thread_id},
            deadline=deadline,
            sensitive_reference=thread_id,
        )
        thread = result.get("thread")
        resumed_id = thread.get("id") if isinstance(thread, dict) else None
        if resumed_id != thread_id:
            raise CodexAppServerError("CODEX_APP_SERVER_RESUME_ID_MISMATCH")
        return result

    def delete_thread(self, thread_id: str, *, deadline: float) -> None:
        self.request(
            "thread/delete",
            {"threadId": thread_id},
            deadline=deadline,
            sensitive_reference=thread_id,
        )

    def list_turns(
        self, thread_id: str, *, deadline: float
    ) -> list[dict[str, object]]:
        turns: list[dict[str, object]] = []
        cursor: str | None = None
        seen_cursors: set[str] = set()
        for _page in range(MAX_APP_SERVER_THREAD_PAGES):
            params: dict[str, object] = {
                "threadId": thread_id,
                "limit": 100,
                "sortDirection": "desc",
                "itemsView": "full",
            }
            if cursor:
                params["cursor"] = cursor
            result = self.request(
                "thread/turns/list",
                params,
                deadline=deadline,
                sensitive_reference=thread_id,
            )
            data = result.get("data")
            if not isinstance(data, list) or not all(
                isinstance(item, dict) for item in data
            ):
                raise CodexAppServerError("CODEX_APP_SERVER_TURN_LIST_INVALID")
            turns.extend(data)
            next_cursor = result.get("nextCursor")
            if next_cursor is None:
                return turns
            if (
                not isinstance(next_cursor, str)
                or not next_cursor
                or next_cursor in seen_cursors
            ):
                raise CodexAppServerError("CODEX_APP_SERVER_TURN_LIST_INVALID")
            seen_cursors.add(next_cursor)
            cursor = next_cursor
        raise CodexAppServerError("CODEX_APP_SERVER_TURN_LIST_INCOMPLETE")

    def run_turn(
        self,
        thread_id: str,
        *,
        prompt: str,
        client_user_message_id: str,
        deadline: float,
    ) -> tuple[str, str, str]:
        result = self.request(
            "turn/start",
            {
                "threadId": thread_id,
                "input": [{"type": "text", "text": prompt}],
                "clientUserMessageId": client_user_message_id,
            },
            deadline=deadline,
            sensitive_reference=thread_id,
        )
        turn = result.get("turn")
        turn_id = turn.get("id") if isinstance(turn, dict) else None
        if not isinstance(turn_id, str) or not turn_id:
            raise CodexAppServerError("CODEX_APP_SERVER_TURN_START_INVALID")

        last_agent_message = ""
        while True:
            message = self._next_message(deadline=deadline)
            if "id" in message and "method" in message:
                raise CodexAppServerError("CODEX_APP_SERVER_CLIENT_REQUEST_UNSUPPORTED")
            method = message.get("method")
            params = message.get("params")
            if not isinstance(params, dict):
                continue
            if params.get("threadId") != thread_id:
                continue
            if method == "item/completed" and params.get("turnId") == turn_id:
                item = params.get("item")
                if isinstance(item, dict) and item.get("type") == "agentMessage":
                    text = item.get("text")
                    if isinstance(text, str):
                        last_agent_message = text
                continue
            if method != "turn/completed":
                continue
            completed_turn = params.get("turn")
            if not isinstance(completed_turn, dict) or completed_turn.get("id") != turn_id:
                continue
            items = completed_turn.get("items")
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, dict) and item.get("type") == "agentMessage":
                        text = item.get("text")
                        if isinstance(text, str):
                            last_agent_message = text
            status = completed_turn.get("status")
            if (
                not isinstance(status, str)
                or status not in CODEX_APP_SERVER_TURN_STATUSES
            ):
                raise CodexAppServerError("CODEX_APP_SERVER_TURN_COMPLETION_INVALID")
            error = completed_turn.get("error")
            return status, last_agent_message, (
                "CODEX_APP_SERVER_TURN_ERROR" if error is not None else ""
            )

    def request(
        self,
        method: str,
        params: dict[str, object],
        *,
        deadline: float,
        sensitive_reference: str = "",
    ) -> dict[str, object]:
        self._request_id += 1
        request_id = self._request_id
        deferred: list[dict[str, object]] = []
        self._write(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": method,
                "params": params,
            }
        )
        while True:
            message = self._next_wire_message(deadline=deadline)
            if "id" in message and "method" in message:
                raise CodexAppServerError("CODEX_APP_SERVER_CLIENT_REQUEST_UNSUPPORTED")
            if message.get("id") != request_id:
                if "id" in message:
                    raise CodexAppServerError("CODEX_APP_SERVER_UNEXPECTED_RESPONSE")
                if (
                    len(self._notifications) + len(deferred)
                    >= MAX_APP_SERVER_PENDING_MESSAGES
                ):
                    raise CodexAppServerError("CODEX_APP_SERVER_NOTIFICATION_OVERFLOW")
                deferred.append(message)
                continue
            if (
                len(self._notifications) + len(deferred)
                > MAX_APP_SERVER_PENDING_MESSAGES
            ):
                raise CodexAppServerError("CODEX_APP_SERVER_NOTIFICATION_OVERFLOW")
            self._notifications.extend(deferred)
            error = message.get("error")
            if error is not None:
                del sensitive_reference
                method_code = re.sub(r"[^A-Z0-9]+", "_", method.upper()).strip("_")
                raise CodexAppServerError(
                    f"CODEX_APP_SERVER_{method_code or 'REQUEST'}_FAILED"
                )
            result = message.get("result")
            if not isinstance(result, dict):
                raise CodexAppServerError("CODEX_APP_SERVER_RESPONSE_INVALID")
            return result

    def notify(self, method: str, params: dict[str, object]) -> None:
        self._write({"jsonrpc": "2.0", "method": method, "params": params})

    def stderr_text(self, *, sensitive_reference: str = "") -> str:
        del sensitive_reference
        return "CODEX_APP_SERVER_PROVIDER_STDERR" if self._stderr_chunks else ""

    def close(self) -> None:
        process = self._process
        if process is None:
            return
        if process.stdin is not None:
            try:
                process.stdin.close()
            except OSError:
                pass
        if process.poll() is None:
            try:
                process.wait(timeout=1)
            except subprocess.TimeoutExpired:
                process.terminate()
                try:
                    process.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=1)
        for thread in self._threads:
            thread.join(timeout=1)
        self._process = None

    def _write(self, payload: dict[str, object]) -> None:
        process = self._process
        if process is None or process.stdin is None or process.poll() is not None:
            raise CodexAppServerError("CODEX_APP_SERVER_NOT_RUNNING")
        try:
            process.stdin.write(json.dumps(payload, separators=(",", ":")) + "\n")
            process.stdin.flush()
        except (BrokenPipeError, OSError) as exc:
            raise CodexAppServerError("CODEX_APP_SERVER_WRITE_FAILED") from exc

    def _next_message(self, *, deadline: float) -> dict[str, object]:
        if self._notifications:
            return self._notifications.pop(0)
        return self._next_wire_message(deadline=deadline)

    def _next_wire_message(self, *, deadline: float) -> dict[str, object]:
        if self._protocol_read_failure is not None:
            raise CodexAppServerError(self._protocol_read_failure.code)
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise CodexAppServerTimeout("CODEX_APP_SERVER_TIMEOUT")
        try:
            item = self._messages.get(timeout=remaining)
        except queue.Empty as exc:
            raise CodexAppServerTimeout("CODEX_APP_SERVER_TIMEOUT") from exc
        if self._protocol_read_failure is not None:
            raise CodexAppServerError(self._protocol_read_failure.code)
        if item is _PROTOCOL_EOF:
            raise CodexAppServerError("CODEX_APP_SERVER_EOF")
        if isinstance(item, _ProtocolReadFailure):
            raise CodexAppServerError(item.code)
        if not isinstance(item, str):
            raise CodexAppServerError("CODEX_APP_SERVER_RESPONSE_INVALID")
        try:
            payload = json.loads(item)
        except json.JSONDecodeError as exc:
            raise CodexAppServerError("CODEX_APP_SERVER_JSON_INVALID") from exc
        if not isinstance(payload, dict):
            raise CodexAppServerError("CODEX_APP_SERVER_RESPONSE_INVALID")
        # Codex 0.147 omits ``jsonrpc`` from successful response envelopes even
        # though requests and notifications use JSON-RPC 2.0 framing. Reject an
        # explicit incompatible version while accepting the reviewed native
        # response shape.
        jsonrpc_version = payload.get("jsonrpc")
        if jsonrpc_version is not None and jsonrpc_version != "2.0":
            raise CodexAppServerError("CODEX_APP_SERVER_JSONRPC_VERSION_INVALID")
        return payload

    def _enqueue_wire_item(self, item: object) -> bool:
        try:
            self._messages.put_nowait(item)
        except queue.Full:
            self._protocol_read_failure = _ProtocolReadFailure(
                "CODEX_APP_SERVER_MESSAGE_QUEUE_OVERFLOW"
            )
            return False
        return True

    def _read_stdout(self) -> None:
        process = self._process
        if process is None or process.stdout is None:
            self._enqueue_wire_item(_PROTOCOL_EOF)
            return
        while True:
            line = process.stdout.readline(MAX_APP_SERVER_LINE_CHARS + 1)
            if not line:
                self._enqueue_wire_item(_PROTOCOL_EOF)
                return
            if len(line) > MAX_APP_SERVER_LINE_CHARS or not line.endswith("\n"):
                self._enqueue_wire_item(
                    _ProtocolReadFailure("CODEX_APP_SERVER_PROTOCOL_LINE_TOO_LARGE")
                )
                return
            if not self._enqueue_wire_item(line):
                return

    def _read_stderr(self) -> None:
        process = self._process
        if process is None or process.stderr is None:
            return
        while self._stderr_chars < MAX_APP_SERVER_STDERR_CHARS:
            chunk = process.stderr.read(
                min(4096, MAX_APP_SERVER_STDERR_CHARS - self._stderr_chars)
            )
            if not chunk:
                return
            self._stderr_chunks.append(chunk)
            self._stderr_chars += len(chunk)


CodexAppServerClientFactory = Callable[..., _CodexAppServerClient]


def sanitized_child_environment(
    credential_denylist: tuple[str, ...] | list[str],
    *,
    source: dict[str, str] | None = None,
) -> dict[str, str]:
    """Copy the process environment without API credentials denied to subscription runtimes."""

    environment = dict(os.environ if source is None else source)
    for variable in {
        *PROVIDER_SESSION_ENVIRONMENT_VARIABLES,
        *SUBSCRIPTION_API_CREDENTIALS,
        *credential_denylist,
        *RESERVED_INSTANCE_ENV_VARS,
    }:
        environment.pop(variable, None)
    return environment


def codex_child_environment(
    credential_denylist: tuple[str, ...] | list[str],
    *,
    binary: str,
    source: dict[str, str] | None = None,
    isolate_user_config: bool = False,
) -> dict[str, str]:
    """Return a sanitized Codex environment with a self-contained launcher path.

    Resumable app-server launches receive a private Agent Mesh-owned ``CODEX_HOME``.
    It contains no user configuration and references the existing subscription
    authentication file without copying credential bytes.
    """

    source_environment = dict(os.environ if source is None else source)
    environment = sanitized_child_environment(
        credential_denylist,
        source=source_environment,
    )
    configured_binary = Path(binary).expanduser()
    if configured_binary.is_absolute():
        binary_parent = str(configured_binary.parent)
        path_entries = [
            entry for entry in environment.get("PATH", "").split(os.pathsep) if entry
        ]
        environment["PATH"] = os.pathsep.join(
            dict.fromkeys([binary_parent, *path_entries])
        )
    if isolate_user_config:
        environment["CODEX_HOME"] = str(
            _prepare_managed_codex_home(source_environment)
        )
    return environment


def _prepare_managed_codex_home(source_environment: dict[str, str]) -> Path:
    raw_source_home = source_environment.get("CODEX_HOME", "").strip()
    if raw_source_home:
        source_home = Path(raw_source_home).expanduser().absolute()
    else:
        raw_os_home = source_environment.get("HOME", "").strip()
        os_home = Path(raw_os_home).expanduser() if raw_os_home else Path.home()
        source_home = (os_home / ".codex").absolute()

    machine_root = registry_dir().expanduser().absolute()
    runtime_root = machine_root / "runtimes"
    managed_home = runtime_root / "codex" / CODEX_MANAGED_HOME_VERSION
    if source_home == managed_home:
        raise CodexAppServerError("CODEX_MANAGED_HOME_SOURCE_COLLISION")
    for path in (machine_root, runtime_root, managed_home.parent, managed_home):
        _ensure_private_runtime_directory(path)

    _validate_managed_codex_config(managed_home / "config.toml")

    source_auth = source_home / "auth.json"
    try:
        source_stat = source_auth.lstat()
    except OSError as exc:
        raise CodexAppServerError("CODEX_MANAGED_AUTH_UNAVAILABLE") from exc
    current_uid = getattr(os, "getuid", lambda: source_stat.st_uid)()
    if (
        not stat.S_ISREG(source_stat.st_mode)
        or source_stat.st_uid != current_uid
        or stat.S_IMODE(source_stat.st_mode) & 0o077
    ):
        raise CodexAppServerError("CODEX_MANAGED_AUTH_UNSAFE")

    managed_auth = managed_home / "auth.json"
    try:
        managed_stat = managed_auth.lstat()
    except FileNotFoundError:
        try:
            managed_auth.symlink_to(source_auth)
        except OSError as exc:
            raise CodexAppServerError("CODEX_MANAGED_AUTH_LINK_FAILED") from exc
        managed_stat = managed_auth.lstat()
    if not stat.S_ISLNK(managed_stat.st_mode):
        raise CodexAppServerError("CODEX_MANAGED_AUTH_LINK_INVALID")
    try:
        linked_source = Path(os.readlink(managed_auth))
    except OSError as exc:
        raise CodexAppServerError("CODEX_MANAGED_AUTH_LINK_INVALID") from exc
    if linked_source != source_auth:
        raise CodexAppServerError("CODEX_MANAGED_AUTH_LINK_INVALID")
    try:
        resolved_stat = managed_auth.stat()
    except OSError as exc:
        raise CodexAppServerError("CODEX_MANAGED_AUTH_UNAVAILABLE") from exc
    if (resolved_stat.st_dev, resolved_stat.st_ino) != (
        source_stat.st_dev,
        source_stat.st_ino,
    ):
        raise CodexAppServerError("CODEX_MANAGED_AUTH_CHANGED")
    return managed_home


def _validate_managed_codex_config(config_path: Path) -> None:
    """Accept only Codex-owned project trust metadata in the private runtime home."""

    try:
        initial = config_path.lstat()
    except FileNotFoundError:
        return
    except OSError as exc:
        raise CodexAppServerError("CODEX_MANAGED_HOME_CONFIG_UNSAFE") from exc

    current_uid = getattr(os, "getuid", lambda: initial.st_uid)()
    if (
        not stat.S_ISREG(initial.st_mode)
        or initial.st_uid != current_uid
        or stat.S_IMODE(initial.st_mode) & 0o077
        or initial.st_size > MAX_CODEX_MANAGED_CONFIG_BYTES
    ):
        raise CodexAppServerError("CODEX_MANAGED_HOME_CONFIG_UNSAFE")

    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(config_path, flags)
    except OSError as exc:
        raise CodexAppServerError("CODEX_MANAGED_HOME_CONFIG_UNSAFE") from exc
    try:
        observed = os.fstat(descriptor)
        if (
            not stat.S_ISREG(observed.st_mode)
            or observed.st_uid != current_uid
            or stat.S_IMODE(observed.st_mode) & 0o077
            or observed.st_size > MAX_CODEX_MANAGED_CONFIG_BYTES
            or (observed.st_dev, observed.st_ino)
            != (initial.st_dev, initial.st_ino)
        ):
            raise CodexAppServerError("CODEX_MANAGED_HOME_CONFIG_UNSAFE")
        chunks: list[bytes] = []
        remaining = MAX_CODEX_MANAGED_CONFIG_BYTES + 1
        while remaining:
            chunk = os.read(descriptor, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        payload = b"".join(chunks)
        if len(payload) > MAX_CODEX_MANAGED_CONFIG_BYTES:
            raise CodexAppServerError("CODEX_MANAGED_HOME_CONFIG_UNSAFE")
    except OSError as exc:
        raise CodexAppServerError("CODEX_MANAGED_HOME_CONFIG_UNSAFE") from exc
    finally:
        os.close(descriptor)

    try:
        final = config_path.lstat()
    except OSError as exc:
        raise CodexAppServerError("CODEX_MANAGED_HOME_CONFIG_UNSAFE") from exc
    if (final.st_dev, final.st_ino) != (observed.st_dev, observed.st_ino):
        raise CodexAppServerError("CODEX_MANAGED_HOME_CONFIG_UNSAFE")

    try:
        parsed = tomllib.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise CodexAppServerError("CODEX_MANAGED_HOME_CONFIG_UNSAFE") from exc
    if not parsed:
        return
    if set(parsed) != {"projects"} or not isinstance(parsed["projects"], dict):
        raise CodexAppServerError("CODEX_MANAGED_HOME_CONFIG_UNSAFE")
    for project_path, project_config in parsed["projects"].items():
        if (
            not isinstance(project_path, str)
            or not project_path
            or len(project_path) > 4096
            or not Path(project_path).is_absolute()
            or not isinstance(project_config, dict)
            or project_config != {"trust_level": "trusted"}
        ):
            raise CodexAppServerError("CODEX_MANAGED_HOME_CONFIG_UNSAFE")


def _ensure_private_runtime_directory(path: Path) -> None:
    try:
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
        observed = path.lstat()
    except OSError as exc:
        raise CodexAppServerError("CODEX_MANAGED_HOME_UNAVAILABLE") from exc
    current_uid = getattr(os, "getuid", lambda: observed.st_uid)()
    if not stat.S_ISDIR(observed.st_mode) or observed.st_uid != current_uid:
        raise CodexAppServerError("CODEX_MANAGED_HOME_UNSAFE")
    if stat.S_IMODE(observed.st_mode) & 0o077:
        try:
            path.chmod(0o700)
            observed = path.lstat()
        except OSError as exc:
            raise CodexAppServerError("CODEX_MANAGED_HOME_UNSAFE") from exc
        if stat.S_IMODE(observed.st_mode) & 0o077:
            raise CodexAppServerError("CODEX_MANAGED_HOME_UNSAFE")


class AgentProcessLauncher:
    """Run sanitized agent launch specs and return privacy-safe execution results."""

    def launch(self, spec: AgentLaunchSpec) -> AgentLaunchResult:
        spec = _finalize_identity_boundary(spec)
        started = time.monotonic()
        metadata = {
            **spec.metadata,
            "argv": list(spec.argv),
            "cwd": str(spec.cwd),
            "requires_pty": spec.requires_pty,
            "timeout_seconds": spec.timeout_seconds,
            "prompt_sha": spec.prompt_sha,
        }
        if spec.requires_pty:
            result = self._launch_pty(spec, metadata=metadata, started=started)
        else:
            result = self._launch_subprocess(spec, metadata=metadata, started=started)
        return _prefer_stdout_file(result, spec.stdout_file)

    def _launch_subprocess(
        self,
        spec: AgentLaunchSpec,
        *,
        metadata: dict[str, object],
        started: float,
    ) -> AgentLaunchResult:
        result = run_bounded_process(
            spec.argv,
            cwd=spec.cwd,
            environment=spec.environment or {},
            stdin=spec.stdin_text.encode("utf-8"),
            timeout_seconds=spec.timeout_seconds,
            stdout_limit=MAX_AGENT_PROCESS_STDOUT_BYTES,
            stderr_limit=MAX_AGENT_PROCESS_STDERR_BYTES,
        )
        output_exceeded = result.status == "output_too_large"
        return AgentLaunchResult(
            status=(
                "timeout"
                if result.status == "timeout"
                else (
                    "launch_error"
                    if result.status == "launch_error"
                    else ("completed" if result.returncode == 0 else "failed")
                )
            ),
            exit_code=result.returncode,
            stdout=result.stdout.decode("utf-8", errors="replace"),
            stderr=result.stderr.decode("utf-8", errors="replace"),
            duration_seconds=time.monotonic() - started,
            metadata={**metadata, **({"output_limit_exceeded": True} if output_exceeded else {})},
        )

    def _launch_pty(
        self,
        spec: AgentLaunchSpec,
        *,
        metadata: dict[str, object],
        started: float,
    ) -> AgentLaunchResult:
        master_fd, slave_fd = pty.openpty()
        try:
            attrs = termios.tcgetattr(slave_fd)
            attrs[3] = attrs[3] & ~termios.ECHO
            termios.tcsetattr(slave_fd, termios.TCSANOW, attrs)
            try:
                proc = subprocess.Popen(
                    spec.argv,
                    cwd=spec.cwd,
                    env=spec.environment,
                    stdin=subprocess.PIPE,
                    stdout=slave_fd,
                    stderr=slave_fd,
                    close_fds=True,
                )
            except OSError as exc:
                os.close(master_fd)
                return AgentLaunchResult(
                    status="launch_error",
                    exit_code=None,
                    stdout="",
                    stderr=str(exc),
                    duration_seconds=time.monotonic() - started,
                    metadata=metadata,
                )
        finally:
            os.close(slave_fd)

        chunks: list[bytes] = []
        timed_out = False
        deadline = started + spec.timeout_seconds
        stdin_bytes = spec.stdin_text.encode("utf-8")
        stdin_offset = 0
        stdin_fd: int | None = None
        if proc.stdin is not None:
            stdin_fd = proc.stdin.fileno()
            os.set_blocking(stdin_fd, False)
        try:
            while True:
                now = time.monotonic()
                if now >= deadline and proc.poll() is None:
                    timed_out = True
                    proc.kill()
                    proc.wait(timeout=1)
                if stdin_fd is not None and stdin_offset >= len(stdin_bytes):
                    proc.stdin.close()  # type: ignore[union-attr]
                    stdin_fd = None
                timeout = max(0.0, min(0.05, deadline - now))
                write_fds = [stdin_fd] if stdin_fd is not None and proc.poll() is None else []
                readable, writable, _ = select.select([master_fd], write_fds, [], timeout)
                if stdin_fd is not None and stdin_fd in writable:
                    try:
                        written = os.write(
                            stdin_fd, stdin_bytes[stdin_offset : stdin_offset + 65536]
                        )
                    except OSError:
                        proc.stdin.close()  # type: ignore[union-attr]
                        stdin_fd = None
                    else:
                        stdin_offset += written
                if readable:
                    try:
                        chunk = os.read(master_fd, 4096)
                    except OSError:
                        chunk = b""
                    if chunk:
                        chunks.append(chunk)
                if proc.poll() is not None:
                    while True:
                        readable, _, _ = select.select([master_fd], [], [], 0)
                        if not readable:
                            break
                        try:
                            chunk = os.read(master_fd, 4096)
                        except OSError:
                            break
                        if not chunk:
                            break
                        chunks.append(chunk)
                    break
        finally:
            if stdin_fd is not None and proc.stdin is not None:
                proc.stdin.close()
            os.close(master_fd)
            if proc.poll() is None:
                proc.kill()
                proc.wait(timeout=1)

        return AgentLaunchResult(
            status="timeout" if timed_out else ("completed" if proc.returncode == 0 else "failed"),
            exit_code=None if timed_out else proc.returncode,
            stdout=b"".join(chunks).decode("utf-8", errors="replace"),
            stderr="",
            duration_seconds=time.monotonic() - started,
            metadata=metadata,
        )


def _prefer_stdout_file(result: AgentLaunchResult, stdout_file: Path | None) -> AgentLaunchResult:
    if stdout_file is None:
        return result
    try:
        file_stdout = stdout_file.read_text(encoding="utf-8")
    except OSError:
        return result
    finally:
        try:
            stdout_file.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            pass
    if result.status != "completed" or result.exit_code != 0:
        return result
    return AgentLaunchResult(
        status=result.status,
        exit_code=result.exit_code,
        stdout=file_stdout,
        stderr=result.stderr,
        duration_seconds=result.duration_seconds,
        metadata=result.metadata,
    )


def _runtime_workstream(request: AgentRunRequest) -> str:
    """Return a stable, human-readable handle component for one dispatch wave."""

    raw = request.workstream.strip().lower()
    if not raw:
        return ""
    if (
        re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", raw)
        and len(raw) <= MAX_RUNTIME_WORKSTREAM_CHARS
    ):
        return normalize_handle_component(raw, field_name="runtime workstream")
    slug = re.sub(r"[^a-z0-9]+", "-", raw).strip("-") or "wave"
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:8]
    prefix = slug[: MAX_RUNTIME_WORKSTREAM_CHARS - len(digest) - 1].rstrip("-") or "wave"
    return normalize_handle_component(
        f"{prefix}-{digest}", field_name="runtime workstream"
    )


class CodexCliRuntimeAdapter(AgentRuntimeAdapter):
    """Launch either one-shot Codex exec or exact resumable app-server sessions.

    One-shot profiles retain the established ephemeral PTY path. Exact resumable profiles discover
    or create the provider thread during the trusted handshake and retain its raw identifier only
    in private adapter memory. Supported Codex versions materialize a new persisted thread at its
    first turn, so that turn uses the same child-bound app-server that created it; later turns
    resume it through a fresh child-bound app-server over stdio.
    """

    def __init__(
        self,
        spec,
        *,
        app_server_client_factory: CodexAppServerClientFactory | None = None,
    ) -> None:
        super().__init__(spec)
        self._app_server_client_factory = (
            app_server_client_factory or _CodexAppServerClient
        )
        self._prepared_sessions: dict[str, _PreparedCodexSession] = {}

    def response_candidate(self, result: AgentLaunchResult, *, max_body_chars: int = 20_000):
        """Use Codex's trusted final-message channel as the response boundary."""

        from .output_policy import extract_final_message_candidate

        return extract_final_message_candidate(result, max_body_chars=max_body_chars)

    def prepare_identity_handshake(self, request: AgentRunRequest) -> str:
        config = self._runtime_config(require_identity=True)
        if not config.resumable:
            return ""
        self._validate_resumable_contract(config)
        self._canonical_resume_candidates(config, request=request)
        deadline = time.monotonic() + min(
            APP_SERVER_HANDSHAKE_TIMEOUT_SECONDS,
            max(1, int(request.timeout_seconds)),
        )
        with self._new_app_server_client(config, request) as client:
            client.initialize(deadline=deadline)
            thread_ids = self._listed_thread_ids(
                client.list_threads(
                    cwd=Path(request.project_root).resolve(), deadline=deadline
                )
            )
        return _provider_inventory_digest(
            thread_ids, project_scope=config.project_scope
        )

    def identity_handshake(
        self, request: AgentRunRequest
    ) -> RuntimeIdentityHandshake:
        config = self._runtime_config(require_identity=True)
        if config.participant != request.target_agent.strip().lower():
            raise ValueError("AGENT_INSTANCE_RUNTIME_TARGET_MISMATCH")
        lifecycle_disposition = "new"
        external_session_ref = ""
        if config.resumable:
            attempt_key = self._attempt_key(request)
            if attempt_key in self._prepared_sessions:
                raise CodexAppServerError("CODEX_APP_SERVER_ATTEMPT_ALREADY_PREPARED")
            prepared = self._prepare_resumable_session(config, request)
            self._prepared_sessions[attempt_key] = prepared
            lifecycle_disposition = prepared.lifecycle_disposition
            external_session_ref = prepared.thread_id
        return RuntimeIdentityHandshake(
            participant=config.participant,
            provider=config.provider,
            runtime_profile=self.spec.name,
            durable_role=config.durable_role,
            role=config.role,
            capabilities=config.capabilities,
            permission_mode=config.sandbox,
            authentication_mode=config.authentication_mode,
            billing_mode=config.billing_mode,
            adapter_trust=config.adapter_trust,
            session_identity_mode=config.session_identity_mode,
            resumable=config.resumable,
            concurrent_attachment=config.concurrent_attachment,
            terminal_observation=config.terminal_observation,
            lifecycle_disposition=lifecycle_disposition,
            workstream=_runtime_workstream(request),
            external_session_ref=external_session_ref,
            launch_attempt_key=self._attempt_key(request),
        )

    def build_launch(self, request: AgentRunRequest) -> AgentLaunchSpec:
        config = self._runtime_config(require_identity=False)
        if config.resumable:
            raise ValueError("CODEX_RESUMABLE_REQUIRES_MANAGED_LAUNCH")
        actual_prompt = self._actual_prompt(config, request)
        prompt_sha = hashlib.sha256(actual_prompt.encode("utf-8")).hexdigest()
        fd, stdout_file_name = tempfile.mkstemp(prefix="agent-mesh-codex-", suffix=".txt")
        os.close(fd)
        stdout_file = Path(stdout_file_name)
        stdout_file.unlink(missing_ok=True)
        argv = [config.binary]
        if "network" in config.capabilities:
            argv.append("--search")
        argv.extend(
            [
                "exec",
                "--ignore-user-config",
                "--strict-config",
                "--model",
                config.model,
                "--sandbox",
                config.sandbox,
                "--skip-git-repo-check",
                "--ephemeral",
                "--output-last-message",
                str(stdout_file),
            ]
        )
        return AgentLaunchSpec(
            argv=argv,
            cwd=Path(request.project_root),
            requires_pty=True,
            timeout_seconds=int(request.timeout_seconds),
            prompt_sha=prompt_sha,
            stdin_text=actual_prompt,
            metadata=self._safe_metadata(config, request),
            stdout_file=stdout_file,
            environment=codex_child_environment(
                config.credential_denylist,
                binary=config.binary,
            ),
        )

    def launch(
        self,
        request: AgentRunRequest,
        *,
        launch_process: Callable[[AgentLaunchSpec], AgentLaunchResult],
        child_instance_handle: str,
    ) -> AgentLaunchResult:
        config = self._runtime_config(require_identity=False)
        if not config.resumable:
            return super().launch(
                request,
                launch_process=launch_process,
                child_instance_handle=child_instance_handle,
            )
        del launch_process
        started = time.monotonic()
        attempt_key = self._attempt_key(request)
        try:
            metadata = {
                **self._safe_metadata(config, request),
                "argv": _codex_app_server_argv(config.binary),
                "cwd": str(request.project_root),
                "requires_pty": False,
                "timeout_seconds": request.timeout_seconds,
                "prompt_sha": hashlib.sha256(
                    self._actual_prompt(config, request).encode("utf-8")
                ).hexdigest(),
                "runtime_protocol": CODEX_APP_SERVER_PROTOCOL,
            }
            _reject_identity_metadata(metadata)
            metadata_text = json.dumps(metadata, sort_keys=True, default=str)
        except Exception:
            self.abort_identity_handshake(request)
            raise
        prepared = self._prepared_sessions.pop(attempt_key, None)
        if prepared is None:
            return AgentLaunchResult(
                status="launch_error",
                exit_code=None,
                stderr="CODEX_APP_SERVER_HANDSHAKE_REQUIRED",
                duration_seconds=time.monotonic() - started,
                metadata=metadata,
            )
        held_client = prepared.client

        def abort_prepared() -> None:
            self._prepared_sessions[attempt_key] = prepared
            self.abort_identity_handshake(request)

        if prepared.thread_id in metadata_text:
            abort_prepared()
            raise CodexAppServerError("CODEX_APP_SERVER_PRIVATE_METADATA_FORBIDDEN")

        def observation_pending() -> AgentLaunchResult:
            return AgentLaunchResult(
                status="observation_pending",
                exit_code=None,
                stderr="CODEX_APP_SERVER_RECOVERY_OBSERVATION_REQUIRED",
                duration_seconds=time.monotonic() - started,
                metadata=metadata,
            )

        if held_client is not None:
            if prepared.child_instance_handle != child_instance_handle:
                abort_prepared()
                raise CodexAppServerError("CODEX_APP_SERVER_CHILD_BINDING_MISMATCH")
            client = held_client
        else:
            client = self._new_app_server_client(
                config, request, child_instance_handle=child_instance_handle
            )
        deadline = started + max(1, int(request.timeout_seconds))
        turn_submission_attempted = False
        client_entered = held_client is not None
        try:
            if held_client is None:
                client.__enter__()
                client_entered = True
                client.initialize(deadline=deadline)
                resume_result = client.resume_thread(
                    prepared.thread_id,
                    params=self._resume_params(config, request),
                    deadline=deadline,
                )
                self._validate_thread_result(
                    resume_result,
                    prepared.thread_id,
                    config=config,
                    request=request,
                )
            if held_client is None:
                recovery_deadline = min(
                    deadline,
                    time.monotonic()
                    + APP_SERVER_RECOVERY_OBSERVATION_TIMEOUT_SECONDS,
                )
                first_observation_poll = True
                while True:
                    if (
                        not first_observation_poll
                        and time.monotonic() >= recovery_deadline
                    ):
                        return observation_pending()
                    first_observation_poll = False
                    try:
                        prior_turn = _provider_turn_for_run(
                            client.list_turns(
                                prepared.thread_id, deadline=recovery_deadline
                            ),
                            run_id=request.run_id,
                        )
                    except CodexAppServerTimeout:
                        return observation_pending()
                    if prior_turn is None or prior_turn[0] != "inProgress":
                        break
                    if time.monotonic() >= recovery_deadline:
                        return observation_pending()
                    time.sleep(min(0.1, recovery_deadline - time.monotonic()))
            else:
                prior_turn = None
            if prior_turn is None:
                # From this point until a terminal provider observation, a
                # transport failure is ambiguous: the provider may have
                # accepted and continued the turn after this process lost
                # the wire. Preserve the canonical lease for same-run
                # observation instead of authorizing a replacement turn.
                turn_submission_attempted = True
                status, stdout, error_message = client.run_turn(
                    prepared.thread_id,
                    prompt=self._actual_prompt(config, request),
                    client_user_message_id=request.run_id,
                    deadline=deadline,
                )
            else:
                status, stdout, error_message = prior_turn
            if status == "inProgress":
                return observation_pending()
            stdout = _redact_provider_reference(stdout, prepared.thread_id)
            error_message = _redact_provider_reference(
                error_message, prepared.thread_id
            )
            if status == "completed":
                return AgentLaunchResult(
                    status="completed",
                    exit_code=0,
                    stdout=stdout,
                    duration_seconds=time.monotonic() - started,
                    metadata=metadata,
                )
            stderr = f"CODEX_APP_SERVER_TURN_{status.upper()}"
            if error_message:
                stderr += f": {error_message}"
            return AgentLaunchResult(
                status="failed",
                exit_code=1,
                stdout=stdout,
                stderr=stderr,
                duration_seconds=time.monotonic() - started,
                metadata=metadata,
            )
        except CodexAppServerTimeout:
            if turn_submission_attempted:
                return observation_pending()
            return AgentLaunchResult(
                status="timeout",
                exit_code=None,
                stderr="CODEX_APP_SERVER_TIMEOUT",
                duration_seconds=time.monotonic() - started,
                metadata=metadata,
            )
        except (CodexAppServerError, OSError) as exc:
            if turn_submission_attempted:
                return observation_pending()
            detail = _redact_provider_reference(str(exc), prepared.thread_id)
            provider_stderr = client.stderr_text(
                sensitive_reference=prepared.thread_id
            )
            stderr = detail or "CODEX_APP_SERVER_LAUNCH_FAILED"
            if provider_stderr:
                stderr = f"{stderr}\n{provider_stderr}"
            return AgentLaunchResult(
                status="launch_error",
                exit_code=None,
                stderr=stderr,
                duration_seconds=time.monotonic() - started,
                metadata=metadata,
            )
        finally:
            if client_entered:
                client.close()

    def abort_identity_handshake(self, request: AgentRunRequest) -> None:
        config = self._runtime_config(require_identity=False)
        prepared = self._prepared_sessions.pop(self._attempt_key(request), None)
        if prepared is None:
            return
        held_client = prepared.client
        try:
            if not prepared.newly_created:
                return
            try:
                session_digest = scoped_reference_digest(
                    prepared.thread_id,
                    project_scope=config.project_scope,
                    purpose="provider-session",
                )
                if any(
                    candidate.external_session_ref_digest == session_digest
                    for candidate in self._canonical_resume_candidates(
                        config, request=request
                    )
                ):
                    return
            except Exception:
                # Canonical uncertainty must preserve the provider thread for recovery.
                return
            deadline = time.monotonic() + min(
                APP_SERVER_HANDSHAKE_TIMEOUT_SECONDS,
                max(1, int(request.timeout_seconds)),
            )
            try:
                if held_client is not None:
                    held_client.delete_thread(prepared.thread_id, deadline=deadline)
                else:
                    with self._new_app_server_client(config, request) as client:
                        client.initialize(deadline=deadline)
                        client.delete_thread(prepared.thread_id, deadline=deadline)
            except (CodexAppServerError, OSError):
                pass
        finally:
            if held_client is not None:
                held_client.close()

    def _prepare_resumable_session(
        self, config: _CodexRuntimeConfig, request: AgentRunRequest
    ) -> _PreparedCodexSession:
        self._validate_resumable_contract(config)

        candidates = self._canonical_resume_candidates(config, request=request)
        if len(candidates) > 1:
            raise ValueError("CODEX_RESUME_CONTINUITY_AMBIGUOUS")
        if candidates and any(
            run_id != request.run_id
            for run_id in candidates[0].active_launch_run_ids
        ):
            raise ValueError("CODEX_RESUME_ALREADY_ATTACHED")

        deadline = time.monotonic() + min(
            APP_SERVER_HANDSHAKE_TIMEOUT_SECONDS,
            max(1, int(request.timeout_seconds)),
        )
        child_instance_handle = (
            self._new_session_child_handle(config, request=request)
            if not candidates
            else ""
        )
        client = self._new_app_server_client(
            config,
            request,
            child_instance_handle=child_instance_handle,
        )
        keep_client = False
        client_entered = False
        try:
            client.__enter__()
            client_entered = True
            client.initialize(deadline=deadline)
            baseline_digest = self._planned_inventory_digest(config, request)
            listed_thread_ids = self._listed_thread_ids(
                client.list_threads(
                    cwd=Path(request.project_root).resolve(), deadline=deadline
                )
            )
            current_digest = _provider_inventory_digest(
                listed_thread_ids, project_scope=config.project_scope
            )
            if not candidates:
                if current_digest != baseline_digest:
                    # A set delta is not proof that this run created the new
                    # provider thread; another app-server process is outside
                    # the canonical lock. Never adopt an unregistered thread.
                    raise ValueError("CODEX_RESUME_PROVIDER_INVENTORY_AMBIGUOUS")
                thread_id, result = client.start_thread(
                    params=self._start_params(config, request),
                    deadline=deadline,
                )
                try:
                    self._validate_thread_result(
                        result, thread_id, config=config, request=request
                    )
                except Exception:
                    try:
                        client.delete_thread(thread_id, deadline=deadline)
                    except Exception:
                        pass
                    raise
                keep_client = True
                return _PreparedCodexSession(
                    thread_id=thread_id,
                    lifecycle_disposition="new",
                    newly_created=True,
                    child_instance_handle=child_instance_handle,
                    client=client,
                )

            candidate = candidates[0]
            matches: list[str] = []
            for thread_id in listed_thread_ids:
                digest = scoped_reference_digest(
                    thread_id,
                    project_scope=config.project_scope,
                    purpose="provider-session",
                )
                if digest == candidate.external_session_ref_digest:
                    matches.append(thread_id)
            if len(matches) != 1:
                code = (
                    "CODEX_RESUME_THREAD_NOT_FOUND"
                    if not matches
                    else "CODEX_RESUME_PROVIDER_CONTINUITY_AMBIGUOUS"
                )
                raise ValueError(code)
            thread_id = matches[0]
            current_without_candidate = _provider_inventory_digest(
                [item for item in listed_thread_ids if item != thread_id],
                project_scope=config.project_scope,
            )
            candidate_is_current_attempt = (
                candidate.originating_run_id == request.run_id
            )
            if current_digest != baseline_digest and not (
                candidate_is_current_attempt
                and current_without_candidate == baseline_digest
            ):
                raise ValueError("CODEX_RESUME_PROVIDER_INVENTORY_AMBIGUOUS")
            result = client.resume_thread(
                thread_id,
                params=self._resume_params(config, request),
                deadline=deadline,
            )
            self._validate_thread_result(
                result, thread_id, config=config, request=request
            )
            launch_attempt_digest = scoped_reference_digest(
                self._attempt_key(request),
                project_scope=config.project_scope,
                purpose="launch-attempt",
            )
            lifecycle_disposition = dict(
                candidate.launch_attempt_dispositions
            ).get(launch_attempt_digest, "resumed")
            return _PreparedCodexSession(
                thread_id=thread_id,
                lifecycle_disposition=lifecycle_disposition,
                newly_created=False,
            )
        finally:
            if not keep_client and client_entered:
                client.close()

    @staticmethod
    def _validate_resumable_contract(config: _CodexRuntimeConfig) -> None:
        if config.version not in SUPPORTED_CODEX_APP_SERVER_VERSIONS:
            raise ValueError("CODEX_RESUME_PROTOCOL_VERSION_UNSUPPORTED")
        if config.session_identity_mode != "exact":
            raise ValueError("CODEX_RESUME_EXACT_SESSION_REQUIRED")
        if config.concurrent_attachment:
            raise ValueError("CODEX_RESUME_CONCURRENT_ATTACHMENT_UNSUPPORTED")
        if config.terminal_observation != "provider":
            raise ValueError("CODEX_RESUME_PROVIDER_OBSERVATION_REQUIRED")
        if config.events_path is None or not config.project_scope:
            raise ValueError("CODEX_RESUME_PROOF_UNAVAILABLE")

    @staticmethod
    def _listed_thread_ids(threads: list[dict[str, object]]) -> list[str]:
        thread_ids: list[str] = []
        for thread in threads:
            thread_id = thread.get("id")
            if not isinstance(thread_id, str) or not thread_id:
                raise CodexAppServerError("CODEX_APP_SERVER_THREAD_LIST_INVALID")
            if thread_id in thread_ids:
                raise CodexAppServerError("CODEX_APP_SERVER_THREAD_LIST_INVALID")
            thread_ids.append(thread_id)
        return thread_ids

    @staticmethod
    def _planned_inventory_digest(
        config: _CodexRuntimeConfig, request: AgentRunRequest
    ) -> str:
        if config.events_path is None:
            raise ValueError("CODEX_RESUME_PROOF_UNAVAILABLE")
        mesh_config = config_from_agent_dir(config.events_path.parent)
        with open_read_model(mesh_config) as snapshot:
            matches = [
                record
                for record in snapshot.records
                if record.get("kind") == "dispatch_run_planned"
                and record.get("entity_id") == request.run_id
            ]
        if len(matches) != 1:
            raise ValueError("CODEX_RESUME_CANONICAL_PREPARATION_REQUIRED")
        payload = matches[0].get("payload")
        digest = (
            payload.get("provider_inventory_digest")
            if isinstance(payload, dict)
            else None
        )
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ValueError("CODEX_RESUME_CANONICAL_PREPARATION_REQUIRED")
        return digest

    @staticmethod
    def _new_session_child_handle(
        config: _CodexRuntimeConfig, *, request: AgentRunRequest
    ) -> str:
        if config.events_path is None:
            raise ValueError("CODEX_RESUME_PROOF_UNAVAILABLE")
        mesh_config = config_from_agent_dir(config.events_path.parent)
        with open_read_model(mesh_config) as snapshot:
            instances = reduce_agent_instances(snapshot.records)
        return allocate_instance_handle(
            instances,
            participant=config.participant,
            durable_role=config.durable_role,
            workstream=_runtime_workstream(request),
        )

    def _canonical_resume_candidates(
        self, config: _CodexRuntimeConfig, *, request: AgentRunRequest
    ):
        if config.events_path is None:
            return []
        mesh_config = config_from_agent_dir(config.events_path.parent)
        if mesh_config.events_path.resolve() != config.events_path.resolve():
            raise ValueError("CODEX_RESUME_CANONICAL_PATH_MISMATCH")
        with open_read_model(mesh_config) as snapshot:
            instances = reduce_agent_instances(snapshot.records)
        workstream = _runtime_workstream(request)
        relevant = [
            instance
            for instance in instances.values()
            if instance.status == "active"
            and instance.participant == config.participant
            and instance.provider == config.provider
            and instance.runtime_profile == self.spec.name
            and instance.durable_role == config.durable_role
            and instance.workstream == workstream
        ]
        expected = (
            config.role,
            config.capabilities,
            config.sandbox,
            config.authentication_mode,
            config.billing_mode,
            config.adapter_trust,
            config.session_identity_mode,
            config.resumable,
            config.concurrent_attachment,
            config.terminal_observation,
            workstream,
        )
        for instance in relevant:
            actual = (
                instance.role,
                instance.capabilities,
                instance.permission_mode,
                instance.authentication_mode,
                instance.billing_mode,
                instance.adapter_trust,
                instance.session_identity_mode,
                instance.resumable,
                instance.concurrent_attachment,
                instance.terminal_observation,
                instance.workstream,
            )
            if actual != expected or not instance.external_session_ref_digest:
                raise ValueError("CODEX_RESUME_CANONICAL_PROFILE_MISMATCH")
        return relevant
    def _runtime_config(self, *, require_identity: bool) -> _CodexRuntimeConfig:
        options = self.spec.options
        binary = (
            str(options["binary"]).strip()
            if "binary" in options
            else DEFAULT_CODEX_BINARY
        )
        config = _CodexRuntimeConfig(
            binary=binary,
            version=str(options.get("version") or "").strip(),
            model=str(options.get("model") or "").strip(),
            participant=str(options.get("participant") or "").strip().lower(),
            provider=str(options.get("provider") or "").strip().lower(),
            durable_role=str(options.get("durable_role") or "").strip(),
            role=str(options.get("role") or "").strip(),
            sandbox=str(options.get("permission_mode") or "").strip(),
            capabilities=tuple(str(item) for item in options.get("capabilities", ())),
            authentication_mode=str(options.get("authentication_mode") or "").strip(),
            billing_mode=str(options.get("billing_mode") or "").strip(),
            credential_denylist=tuple(
                str(item) for item in options.get("credential_denylist", ())
            ),
            adapter_trust=str(options.get("adapter_trust") or "").strip(),
            session_identity_mode=str(
                options.get("session_identity_mode") or "none"
            ).strip(),
            resumable=bool(options.get("resumable", False)),
            concurrent_attachment=bool(options.get("concurrent_attachment", False)),
            terminal_observation=str(
                options.get("terminal_observation") or "process"
            ).strip(),
            events_path=(
                Path(str(options["events_path"]))
                if options.get("events_path")
                else None
            ),
            project_scope=str(options.get("project_scope") or "").strip(),
        )
        required = {
            "binary": config.binary,
            "version": config.version,
            "model": config.model,
            "role": config.role,
            "permission_mode": config.sandbox,
            "authentication_mode": config.authentication_mode,
            "billing_mode": config.billing_mode,
        }
        if require_identity:
            required.update(
                {
                    "participant": config.participant,
                    "provider": config.provider,
                    "durable_role": config.durable_role,
                    "adapter_trust": config.adapter_trust,
                    "terminal_observation": config.terminal_observation,
                }
            )
        for field_name, value in required.items():
            if not value:
                raise ValueError(f"codex runtime {field_name} must be configured")
        if config.sandbox not in {"read-only", "workspace-write"}:
            raise ValueError("codex runtime permission_mode must be read-only or workspace-write")
        if "repository" not in config.capabilities:
            raise ValueError("codex runtime capabilities must include repository")
        if not config.credential_denylist:
            raise ValueError("codex runtime credential_denylist must be configured")
        return config

    def _new_app_server_client(
        self,
        config: _CodexRuntimeConfig,
        request: AgentRunRequest,
        *,
        child_instance_handle: str = "",
    ):
        environment = codex_child_environment(
            config.credential_denylist,
            binary=config.binary,
            isolate_user_config=(
                self._app_server_client_factory is _CodexAppServerClient
            ),
        )
        return self._app_server_client_factory(
            binary=config.binary,
            cwd=Path(request.project_root),
            environment=environment,
            child_instance_handle=child_instance_handle,
        )

    @staticmethod
    def _attempt_key(request: AgentRunRequest) -> str:
        return f"{request.session_uuid}:{request.run_id}"

    @staticmethod
    def _actual_prompt(config: _CodexRuntimeConfig, request: AgentRunRequest) -> str:
        return f"Agent Mesh role: {config.role}\n\n{request.prompt}"

    @staticmethod
    def _start_params(
        config: _CodexRuntimeConfig, request: AgentRunRequest
    ) -> dict[str, object]:
        params = CodexCliRuntimeAdapter._common_thread_params(config, request)
        params["ephemeral"] = False
        return params

    @staticmethod
    def _resume_params(
        config: _CodexRuntimeConfig, request: AgentRunRequest
    ) -> dict[str, object]:
        params = CodexCliRuntimeAdapter._common_thread_params(config, request)
        params["excludeTurns"] = True
        return params

    @staticmethod
    def _common_thread_params(
        config: _CodexRuntimeConfig, request: AgentRunRequest
    ) -> dict[str, object]:
        params: dict[str, object] = {
            "cwd": str(Path(request.project_root).resolve()),
            "model": config.model,
            "modelProvider": config.provider,
            "sandbox": config.sandbox,
            "approvalPolicy": "never",
            "dynamicTools": [],
            "environments": [],
            "config": {
                "analytics": {"enabled": False},
                "allow_login_shell": False,
                "chatgpt_base_url": "https://chatgpt.com/backend-api/",
                "features": {
                    feature: False
                    for feature in CODEX_APP_SERVER_DISABLED_FEATURES
                },
                "forced_login_method": "chatgpt",
                "hooks": {},
                "mcp_servers": {},
                "model_provider": "openai",
                "model_providers": {},
                "notify": [],
                "otel": {
                    "exporter": "none",
                    "log_user_prompt": False,
                    "metrics_exporter": "none",
                    "trace_exporter": "none",
                },
                "plugins": {},
                "sandbox_workspace_write": {
                    "network_access": False,
                    "writable_roots": [],
                },
                "shell_environment_policy": {"inherit": "none"},
                "skills": {},
                "tools": {},
                "web_search": (
                    "live" if "network" in config.capabilities else "disabled"
                ),
            },
        }
        return params

    @staticmethod
    def _validate_thread_result(
        result: dict[str, object],
        expected_thread_id: str,
        *,
        config: _CodexRuntimeConfig,
        request: AgentRunRequest,
    ) -> None:
        thread = result.get("thread")
        if not isinstance(thread, dict) or thread.get("id") != expected_thread_id:
            raise CodexAppServerError("CODEX_APP_SERVER_THREAD_ID_MISMATCH")
        if thread.get("ephemeral") is not False:
            raise CodexAppServerError("CODEX_APP_SERVER_THREAD_NOT_PERSISTED")
        if thread.get("canAcceptDirectInput") is not True:
            raise CodexAppServerError("CODEX_APP_SERVER_THREAD_NOT_ATTACHABLE")
        expected_cwd = str(Path(request.project_root).resolve())
        if str(thread.get("cwd") or result.get("cwd") or "") != expected_cwd:
            raise CodexAppServerError("CODEX_APP_SERVER_THREAD_CWD_MISMATCH")
        observed_model = result.get("model")
        if isinstance(observed_model, str) and observed_model != config.model:
            raise CodexAppServerError("CODEX_APP_SERVER_THREAD_MODEL_MISMATCH")
        observed_provider = thread.get("modelProvider") or result.get("modelProvider")
        if isinstance(observed_provider, str) and observed_provider != config.provider:
            raise CodexAppServerError("CODEX_APP_SERVER_THREAD_PROVIDER_MISMATCH")

    @staticmethod
    def _safe_metadata(
        config: _CodexRuntimeConfig, request: AgentRunRequest
    ) -> dict[str, object]:
        return {
            "runtime": "codex-cli",
            "runtime_version": config.version,
            "target_agent": request.target_agent,
            "run_id": request.run_id,
            "session_uuid": request.session_uuid,
            "gen_ai.system": "openai",
            "model": config.model,
            "role": config.role,
            "sandbox": config.sandbox,
            "capabilities": list(config.capabilities),
            "authentication_mode": config.authentication_mode,
            "billing_mode": config.billing_mode,
        }


def _redact_provider_reference(value: str, reference: str) -> str:
    if not reference:
        return value
    return value.replace(reference, "[provider-session-redacted]")


def _provider_inventory_digest(
    thread_ids: list[str], *, project_scope: str
) -> str:
    digests = sorted(
        scoped_reference_digest(
            thread_id,
            project_scope=project_scope,
            purpose="provider-session-inventory",
        )
        for thread_id in thread_ids
    )
    framed = "\n".join(digests).encode("ascii")
    return hashlib.sha256(framed).hexdigest()


def _provider_turn_for_run(
    turns: list[dict[str, object]], *, run_id: str
) -> tuple[str, str, str] | None:
    matches: list[dict[str, object]] = []
    for turn in turns:
        items = turn.get("items")
        if not isinstance(items, list):
            raise CodexAppServerError("CODEX_APP_SERVER_TURN_LIST_INVALID")
        if any(
            isinstance(item, dict)
            and item.get("type") == "userMessage"
            and item.get("clientId") == run_id
            for item in items
        ):
            matches.append(turn)
    if len(matches) > 1:
        raise CodexAppServerError("CODEX_APP_SERVER_RUN_TURN_AMBIGUOUS")
    if not matches:
        return None
    turn = matches[0]
    status = turn.get("status")
    items = turn.get("items")
    if (
        not isinstance(status, str)
        or status not in CODEX_APP_SERVER_TURN_STATUSES
        or not isinstance(items, list)
    ):
        raise CodexAppServerError("CODEX_APP_SERVER_TURN_LIST_INVALID")
    stdout = ""
    for item in items:
        if isinstance(item, dict) and item.get("type") == "agentMessage":
            text = item.get("text")
            if isinstance(text, str):
                stdout = text
    return (
        status,
        stdout,
        "CODEX_APP_SERVER_TURN_ERROR" if turn.get("error") is not None else "",
    )


def _finalize_identity_boundary(spec: AgentLaunchSpec) -> AgentLaunchSpec:
    """Apply identity isolation immediately before both subprocess and PTY launch."""

    for argument in spec.argv:
        if INSTANCE_ID_SEARCH_RE.search(str(argument)):
            raise ValueError("AGENT_INSTANCE_PARENT_ID_IN_ARGV")
    _reject_identity_metadata(spec.metadata)
    environment = sanitized_child_environment((), source=spec.environment)
    child_instance_handle = spec.child_instance_handle.strip()
    if child_instance_handle:
        if INSTANCE_ID_RE.fullmatch(child_instance_handle):
            raise ValueError("AGENT_INSTANCE_CHILD_BINDING_INVALID")
        try:
            child_instance_handle = normalize_instance_label(child_instance_handle)
        except AgentInstanceError as exc:
            raise ValueError("AGENT_INSTANCE_CHILD_BINDING_INVALID") from exc
        environment[INSTANCE_ENV_VAR] = child_instance_handle
    return replace(spec, environment=environment)


def _reject_identity_metadata(value: object, *, key_path: str = "metadata") -> None:
    forbidden_key_tokens = (
        "provider_session",
        "session_ref",
        "binding_credential",
        "registrar_credential",
        "launch_attempt_key",
        "instance_id",
    )
    if isinstance(value, dict):
        for key, item in value.items():
            key_text = str(key).lower()
            if any(token in key_text for token in forbidden_key_tokens):
                raise ValueError("AGENT_INSTANCE_PRIVATE_METADATA_FORBIDDEN")
            _reject_identity_metadata(item, key_path=f"{key_path}.{key}")
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _reject_identity_metadata(item, key_path=f"{key_path}[{index}]")
        return
    if isinstance(value, str) and INSTANCE_ID_SEARCH_RE.search(value):
        raise ValueError("AGENT_INSTANCE_PRIVATE_METADATA_FORBIDDEN")
