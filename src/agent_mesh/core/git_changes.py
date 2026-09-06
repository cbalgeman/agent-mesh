"""Deterministic, NUL-safe Git change-set collection for decision checks."""

from __future__ import annotations

import os
import hashlib
import signal
import stat
import subprocess
import threading
import time
from dataclasses import asdict, dataclass, replace
from pathlib import Path, PureWindowsPath
from typing import BinaryIO, Literal

from agent_mesh.core.decision_applicability import normalize_candidate_path

GitChangeMode = Literal["pr", "staged", "worktree", "full"]

MAX_GIT_OUTPUT_BYTES = 16 * 1024 * 1024
MAX_GIT_STDERR_BYTES = 64 * 1024
GIT_TIMEOUT_SECONDS = 30


class GitChangeRequestError(ValueError):
    """Raised when a Git change-set request is invalid."""


class GitChangeUnavailable(RuntimeError):
    """Raised when Git cannot establish one complete local change set."""


def read_bounded_git_tracked_paths(repo_root: Path) -> bytes:
    """Return the local tracked-file list through the shared safety envelope."""

    root = repo_root.resolve()
    return _run_git(
        root,
        ["git", "--no-optional-locks", "ls-files", "-z", "--"],
        deadline=time.monotonic() + GIT_TIMEOUT_SECONDS,
    )


def read_bounded_git_diff_paths(repo_root: Path, *, base_ref: str) -> bytes:
    """Return local base-to-HEAD paths without accepting caller-controlled Git options."""

    base = _validate_base(base_ref)
    if base.startswith("-") or any(
        ord(character) < 32 or ord(character) == 127 for character in base
    ):
        raise GitChangeRequestError(
            "Git reference-scan base must not be option-like or control text"
        )
    root = repo_root.resolve()
    return _run_git(
        root,
        [
            "git",
            "--no-optional-locks",
            "diff",
            "--no-ext-diff",
            "--no-textconv",
            "--name-only",
            "-z",
            f"{base}..HEAD",
            "--",
        ],
        deadline=time.monotonic() + GIT_TIMEOUT_SECONDS,
    )


@dataclass(frozen=True)
class GitChange:
    """One repository-relative path and its role in a Git change."""

    path: str
    comparison: str
    change_kind: str
    status: str
    old_path: str | None = None
    new_path: str | None = None
    path_type: str = "unknown"
    old_path_type: str = "unknown"
    new_path_type: str = "unknown"
    old_mode: str = ""
    new_mode: str = ""
    old_oid: str = ""
    new_oid: str = ""

    def as_dict(self) -> dict[str, str | None]:
        return asdict(self)


@dataclass(frozen=True)
class GitChangeSet:
    """A fully resolved local comparison and its deterministic path entries."""

    mode: GitChangeMode
    changes: tuple[GitChange, ...]
    base: str | None = None
    base_oid: str | None = None
    head_oid: str | None = None
    merge_base: str | None = None

    @property
    def paths(self) -> tuple[str, ...]:
        return tuple(sorted({change.path for change in self.changes}))

    def request_fields(self) -> dict[str, object]:
        return {
            "mode": self.mode,
            "base": self.base,
            "base_oid": self.base_oid,
            "head_oid": self.head_oid,
            "merge_base": self.merge_base,
            "changes": [change.as_dict() for change in self.changes],
        }


@dataclass(frozen=True)
class _RawChange:
    status: str
    paths: tuple[str, ...]
    old_path_type: str
    new_path_type: str
    old_mode: str
    new_mode: str
    old_oid: str
    new_oid: str


@dataclass
class _StreamCapture:
    """One bounded subprocess stream drained by a platform-neutral reader thread."""

    name: str
    limit: int
    data: bytearray
    error: str | None = None


def collect_git_changes(
    repo_root: Path,
    *,
    mode: GitChangeMode,
    base: str | None = None,
    deadline_monotonic: float | None = None,
) -> GitChangeSet:
    """Collect the selected Git comparison without fetching or invoking a shell."""

    if mode not in {"pr", "staged", "worktree", "full"}:
        raise GitChangeRequestError(f"unsupported Git decision-check mode: {mode}")
    if mode in {"staged", "worktree"} and base is not None:
        raise GitChangeRequestError(f"--base is not valid with --mode {mode}")

    requested_base = _validate_base(base or "main") if mode in {"pr", "full"} else None
    root = repo_root.resolve()
    deadline = (
        deadline_monotonic
        if deadline_monotonic is not None
        else time.monotonic() + GIT_TIMEOUT_SECONDS
    )
    _require_git_root(root, deadline=deadline)

    base_oid: str | None = None
    head_oid: str | None = None
    merge_base: str | None = None
    comparisons: list[tuple[str, list[str]]]
    if mode in {"pr", "full"}:
        assert requested_base is not None
        base_oid = _resolve_commit(
            root,
            requested_base,
            label=f"base {requested_base!r}",
            deadline=deadline,
        )
        head_oid = _resolve_commit(root, "HEAD", label="HEAD", deadline=deadline)
        merge_base = _merge_base(root, base_oid, head_oid, deadline=deadline)
        comparisons = (
            [("pr", [merge_base, head_oid])]
            if mode == "pr"
            else [
                ("merge_base_to_head", [merge_base, head_oid]),
                ("head_to_index", ["--cached", head_oid]),
                ("index_to_worktree", []),
            ]
        )
    elif mode == "staged":
        head_oid = _resolve_commit(root, "HEAD", label="HEAD", deadline=deadline)
        comparisons = [("staged", ["--cached", head_oid])]
    else:
        comparisons = [("worktree", [])]

    common = [
        "git",
        "--no-optional-locks",
        "diff",
        "--no-ext-diff",
        "--no-textconv",
        "--find-renames",
        "--find-copies",
        "--find-copies-harder",
    ]
    changes: list[GitChange] = []
    for comparison_name, comparison_args in comparisons:
        name_status = _run_git(
            root,
            [*common, "--name-status", "-z", *comparison_args, "--"],
            deadline=deadline,
        )
        raw = _run_git(
            root,
            [*common, "--raw", "--no-abbrev", "-z", *comparison_args, "--"],
            deadline=deadline,
        )
        named_records = _parse_name_status(name_status)
        raw_records = _parse_raw_changes(raw)
        changes.extend(
            _combine_git_records(
                named_records,
                raw_records,
                comparison=comparison_name,
            )
        )

    if mode in {"worktree", "full"}:
        untracked = _run_git(
            root,
            [
                "git",
                "--no-optional-locks",
                "ls-files",
                "--others",
                "--exclude-standard",
                "-z",
                "--",
            ],
            deadline=deadline,
        )
        changes.extend(_untracked_changes(root, untracked, comparison="untracked"))
    if mode != "pr":
        unmerged = _run_git(
            root,
            ["git", "--no-optional-locks", "ls-files", "--unmerged", "-z", "--"],
            deadline=deadline,
        )
        changes = _label_unmerged_paths(changes, _parse_unmerged_paths(unmerged))

    ordered = tuple(
        sorted(
            changes,
            key=lambda item: (
                item.path,
                item.comparison,
                item.change_kind,
                item.status,
                item.old_path or "",
                item.new_path or "",
            ),
        )
    )
    if len({item.path for item in ordered}) > 10_000:
        raise GitChangeUnavailable("Git decision check exceeds 10000 distinct paths")
    return GitChangeSet(
        mode=mode,
        changes=ordered,
        base=requested_base,
        base_oid=base_oid,
        head_oid=head_oid,
        merge_base=merge_base,
    )


def capture_git_snapshot_identity(
    repo_root: Path,
    *,
    deadline_monotonic: float,
) -> str:
    """Bind the resolved HEAD and complete index content within one shared deadline."""

    root = repo_root.resolve()
    _require_git_root(root, deadline=deadline_monotonic)
    head = _run_git(
        root,
        ["git", "--no-optional-locks", "rev-parse", "--verify", "HEAD"],
        deadline=deadline_monotonic,
    ).strip()
    index = _run_git(
        root,
        ["git", "--no-optional-locks", "ls-files", "--stage", "-z", "--"],
        deadline=deadline_monotonic,
    )
    flags = _run_git(
        root,
        ["git", "--no-optional-locks", "ls-files", "-v", "-z", "--"],
        deadline=deadline_monotonic,
    )
    visible_index_diff = _run_git(
        root,
        [
            "git",
            "--no-optional-locks",
            "diff",
            "--cached",
            "--raw",
            "-z",
            "--full-index",
            "--no-ext-diff",
            "--no-textconv",
            "--no-renames",
            "--ita-visible-in-index",
            "--",
        ],
        deadline=deadline_monotonic,
    )
    invisible_index_diff = _run_git(
        root,
        [
            "git",
            "--no-optional-locks",
            "diff",
            "--cached",
            "--raw",
            "-z",
            "--full-index",
            "--no-ext-diff",
            "--no-textconv",
            "--no-renames",
            "--ita-invisible-in-index",
            "--",
        ],
        deadline=deadline_monotonic,
    )
    _reject_unsupported_index_flags(
        index,
        flags,
        visible_index_diff=visible_index_diff,
        invisible_index_diff=invisible_index_diff,
    )
    return hashlib.sha256(
        head + b"\0" + index + b"\0" + flags + b"\0" + visible_index_diff
    ).hexdigest()


def _reject_unsupported_index_flags(
    index: bytes,
    flags: bytes,
    *,
    visible_index_diff: bytes,
    invisible_index_diff: bytes,
) -> None:
    """Reject index states whose worktree changes Git intentionally hides."""

    for record in flags.split(b"\0"):
        if not record:
            continue
        if len(record) < 3 or record[1:2] != b" ":
            raise GitChangeUnavailable("Git index flag output is malformed")
        tag = chr(record[0])
        if tag == "S" or tag.islower():
            raise GitChangeUnavailable(
                "Git change subject rejects assume-unchanged or skip-worktree index entries"
            )
    if visible_index_diff != invisible_index_diff:
        raise GitChangeUnavailable("Git change subject rejects intent-to-add index entries")
    for record in index.split(b"\0"):
        if not record:
            continue
        header, separator, _path = record.partition(b"\t")
        fields = header.split(b" ")
        if not separator or len(fields) != 3:
            raise GitChangeUnavailable("Git staged index output is malformed")
        oid = fields[1]
        stage = fields[2]
        if stage == b"0" and oid and set(oid) == {ord("0")}:
            raise GitChangeUnavailable("Git change subject rejects intent-to-add index entries")


def _git_environment() -> dict[str, str]:
    env = dict(os.environ)
    for key in tuple(env):
        if (
            key.startswith("GIT_CONFIG_KEY_")
            or key.startswith("GIT_CONFIG_VALUE_")
            or key.startswith("GIT_TRACE")
        ):
            env.pop(key, None)
    for key in (
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_ALLOW_PROTOCOL",
        "GIT_ASKPASS",
        "GIT_CEILING_DIRECTORIES",
        "GIT_COMMON_DIR",
        "GIT_CONFIG_COUNT",
        "GIT_CONFIG_GLOBAL",
        "GIT_CONFIG_PARAMETERS",
        "GIT_CONFIG_SYSTEM",
        "GIT_CURL_VERBOSE",
        "GIT_DIFF_OPTS",
        "GIT_DIR",
        "GIT_DISCOVERY_ACROSS_FILESYSTEM",
        "GIT_EXEC_PATH",
        "GIT_EXTERNAL_DIFF",
        "GIT_FLUSH",
        "GIT_GRAFT_FILE",
        "GIT_HTTP_USER_AGENT",
        "GIT_INDEX_FILE",
        "GIT_NAMESPACE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_PROTOCOL_FROM_USER",
        "GIT_PROXY_COMMAND",
        "GIT_REDIRECT_STDERR",
        "GIT_REPLACE_REF_BASE",
        "GIT_SHALLOW_FILE",
        "GIT_SSH",
        "GIT_SSH_COMMAND",
        "GIT_SSL_NO_VERIFY",
        "GIT_WORK_TREE",
        "SSH_ASKPASS",
    ):
        env.pop(key, None)
    env.update(
        {
            "GIT_ATTR_NOSYSTEM": "1",
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_NO_LAZY_FETCH": "1",
            "GIT_OPTIONAL_LOCKS": "0",
            "GIT_PAGER": "cat",
            "GIT_TERMINAL_PROMPT": "0",
            "LANG": "C",
            "LC_ALL": "C",
        }
    )
    return env


def _run_git(
    root: Path,
    argv: list[str],
    *,
    deadline: float | None = None,
) -> bytes:
    command = argv
    if argv and argv[0] == "git":
        command = [
            "git",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "protocol.allow=never",
            *argv[1:],
        ]
    if deadline is None:
        deadline = time.monotonic() + GIT_TIMEOUT_SECONDS
    if deadline <= time.monotonic():
        raise GitChangeUnavailable("Git decision check timed out")
    try:
        process = _start_git_process(command, root)
    except FileNotFoundError as exc:
        raise GitChangeUnavailable("Git executable is unavailable") from exc
    except OSError as exc:
        raise GitChangeUnavailable(f"Git decision check could not start: {exc}") from exc

    assert process.stdout is not None
    assert process.stderr is not None
    stop_event = threading.Event()
    captures = {
        "stdout": _StreamCapture("stdout", MAX_GIT_OUTPUT_BYTES, bytearray()),
        "stderr": _StreamCapture("stderr", MAX_GIT_STDERR_BYTES, bytearray()),
    }
    readers = [
        threading.Thread(
            target=_drain_git_stream,
            args=(process.stdout, captures["stdout"], stop_event),
            name="agent-mesh-git-stdout",
            daemon=True,
        ),
        threading.Thread(
            target=_drain_git_stream,
            args=(process.stderr, captures["stderr"], stop_event),
            name="agent-mesh-git-stderr",
            daemon=True,
        ),
    ]
    started: list[threading.Thread] = []
    try:
        try:
            for reader in readers:
                reader.start()
                started.append(reader)
        except RuntimeError as exc:
            _terminate_git_process(process)
            _close_git_streams(process)
            cleanup_deadline = time.monotonic() + 1.0
            for reader in started:
                reader.join(timeout=max(0.0, cleanup_deadline - time.monotonic()))
            raise GitChangeUnavailable("Git decision-check stream reader could not start") from exc

        timed_out = False
        while process.poll() is None:
            if stop_event.is_set():
                _terminate_git_process(process)
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                timed_out = True
                _terminate_git_process(process)
                break
            stop_event.wait(min(remaining, 0.05))

        cleanup_deadline = time.monotonic() + 1.0
        for reader in started:
            reader.join(timeout=max(0.0, cleanup_deadline - time.monotonic()))
        if any(reader.is_alive() for reader in started):
            _terminate_git_process(process)
            _close_git_streams(process)
            cleanup_deadline = time.monotonic() + 1.0
            for reader in started:
                reader.join(timeout=max(0.0, cleanup_deadline - time.monotonic()))
        if any(reader.is_alive() for reader in started):
            raise GitChangeUnavailable("Git decision-check stream cleanup timed out")
        if timed_out:
            raise GitChangeUnavailable("Git decision check timed out")
        for capture in captures.values():
            if capture.error is not None:
                raise GitChangeUnavailable(capture.error)
        returncode = process.poll()
        if returncode is None:
            if not _terminate_git_process(process):
                raise GitChangeUnavailable("Git decision-check process cleanup timed out")
            returncode = process.returncode
        assert returncode is not None
    finally:
        if process.poll() is None:
            _terminate_git_process(process)
        _close_git_streams(process)
    if returncode != 0:
        detail = _sanitize_git_diagnostic(bytes(captures["stderr"].data))
        raise GitChangeUnavailable(detail or f"Git exited {returncode}")
    return bytes(captures["stdout"].data)


def _start_git_process(command: list[str], root: Path) -> subprocess.Popen[bytes]:
    if os.name == "nt":
        return subprocess.Popen(
            command,
            cwd=root,
            env=_git_environment(),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            creationflags=int(getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)),
        )
    return subprocess.Popen(
        command,
        cwd=root,
        env=_git_environment(),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )


def _drain_git_stream(
    stream: BinaryIO,
    capture: _StreamCapture,
    stop_event: threading.Event,
) -> None:
    try:
        while True:
            chunk = stream.read(64 * 1024)
            if not chunk:
                return
            remaining = capture.limit - len(capture.data)
            if len(chunk) > remaining:
                if remaining > 0:
                    capture.data.extend(chunk[:remaining])
                label = "output" if capture.name == "stdout" else "error output"
                capture.error = f"Git decision-check {label} exceeds its byte bound"
                stop_event.set()
                return
            capture.data.extend(chunk)
    except (OSError, ValueError) as exc:
        capture.error = f"Git decision-check output could not be read: {exc}"
        stop_event.set()


def _close_git_streams(process: subprocess.Popen[bytes]) -> None:
    for stream in (process.stdout, process.stderr):
        if stream is not None and not stream.closed:
            stream.close()


def _terminate_git_process(process: subprocess.Popen[bytes]) -> bool:
    if process.poll() is not None:
        return True
    if os.name == "nt":
        break_signal = getattr(signal, "CTRL_BREAK_EVENT", None)
        if break_signal is not None:
            try:
                process.send_signal(break_signal)
                process.wait(timeout=0.2)
                return True
            except (OSError, subprocess.TimeoutExpired):
                pass
        if process.poll() is None:
            try:
                subprocess.run(
                    [
                        str(
                            PureWindowsPath(os.environ.get("SystemRoot", r"C:\Windows"))
                            / "System32"
                            / "taskkill.exe"
                        ),
                        "/PID",
                        str(process.pid),
                        "/T",
                        "/F",
                    ],
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    check=False,
                    timeout=1,
                )
            except (OSError, subprocess.TimeoutExpired):
                pass
    else:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except OSError:
            try:
                process.kill()
            except OSError:
                pass
    if process.poll() is None:
        try:
            process.kill()
        except OSError:
            pass
    try:
        process.wait(timeout=1)
    except subprocess.TimeoutExpired:
        return False
    return True


def _sanitize_git_diagnostic(value: bytes) -> str:
    decoded = value.decode("utf-8", errors="replace")
    return " ".join("".join(char if char.isprintable() else " " for char in decoded).split())[:1000]


def _require_git_root(root: Path, *, deadline: float) -> None:
    raw = _run_git(
        root,
        ["git", "--no-optional-locks", "rev-parse", "--show-toplevel"],
        deadline=deadline,
    )
    try:
        reported = Path(raw.rstrip(b"\n").decode("utf-8")).resolve()
    except UnicodeDecodeError as exc:
        raise GitChangeUnavailable("Git repository root is not valid UTF-8") from exc
    if reported != root:
        raise GitChangeUnavailable(
            f"Agent Mesh project root {root} does not match Git root {reported}"
        )


def _validate_base(value: str) -> str:
    if not value or "\x00" in value:
        raise GitChangeRequestError("Git base must be non-empty text without NUL")
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise GitChangeRequestError("Git base must be valid UTF-8 text") from exc
    if len(encoded) > 4096:
        raise GitChangeRequestError("Git base exceeds 4096 UTF-8 bytes")
    return value


def _resolve_commit(root: Path, revision: str, *, label: str, deadline: float) -> str:
    try:
        raw = _run_git(
            root,
            [
                "git",
                "--no-optional-locks",
                "rev-parse",
                "--verify",
                "--end-of-options",
                f"{revision}^{{commit}}",
            ],
            deadline=deadline,
        )
    except GitChangeUnavailable as exc:
        raise GitChangeUnavailable(f"cannot resolve Git {label}: {exc}") from exc
    return _decode_object_id(raw, label=label)


def _merge_base(root: Path, base_oid: str, head_oid: str, *, deadline: float) -> str:
    raw = _run_git(
        root,
        ["git", "--no-optional-locks", "merge-base", base_oid, head_oid],
        deadline=deadline,
    )
    return _decode_object_id(raw, label="merge base")


def _decode_object_id(raw: bytes, *, label: str) -> str:
    try:
        value = raw.strip().decode("ascii")
    except UnicodeDecodeError as exc:
        raise GitChangeUnavailable(f"Git returned a non-ASCII {label}") from exc
    if not value or any(character not in "0123456789abcdefABCDEF" for character in value):
        raise GitChangeUnavailable(f"Git returned an invalid {label}")
    return value.lower()


def _nul_tokens(data: bytes) -> list[bytes]:
    if not data:
        return []
    if not data.endswith(b"\x00"):
        raise GitChangeUnavailable("Git returned a non-NUL-terminated path stream")
    return data[:-1].split(b"\x00")


def _decode_path(raw: bytes) -> str:
    try:
        return normalize_candidate_path(raw.decode("utf-8"))
    except UnicodeDecodeError as exc:
        raise GitChangeUnavailable("Git path cannot be represented as UTF-8") from exc
    except ValueError as exc:
        raise GitChangeUnavailable(f"Git returned an invalid repository path: {exc}") from exc


def _parse_name_status(data: bytes) -> list[tuple[str, tuple[str, ...]]]:
    tokens = _nul_tokens(data)
    records: list[tuple[str, tuple[str, ...]]] = []
    index = 0
    while index < len(tokens):
        try:
            status_code = tokens[index].decode("ascii")
        except UnicodeDecodeError as exc:
            raise GitChangeUnavailable("Git returned a non-ASCII change status") from exc
        index += 1
        if not status_code:
            raise GitChangeUnavailable("Git returned an empty change status")
        path_count = 2 if status_code[0] in {"R", "C"} else 1
        if index + path_count > len(tokens):
            raise GitChangeUnavailable("Git returned a truncated name-status record")
        paths = tuple(_decode_path(token) for token in tokens[index : index + path_count])
        records.append((status_code, paths))
        index += path_count
    return records


def _parse_raw_changes(data: bytes) -> list[_RawChange]:
    tokens = _nul_tokens(data)
    records: list[_RawChange] = []
    index = 0
    while index < len(tokens):
        header = tokens[index]
        index += 1
        if not header.startswith(b":"):
            raise GitChangeUnavailable("Git returned an invalid raw change header")
        fields = header[1:].split()
        if len(fields) != 5:
            raise GitChangeUnavailable("Git returned an incomplete raw change header")
        try:
            old_mode = fields[0].decode("ascii")
            new_mode = fields[1].decode("ascii")
            old_oid = fields[2].decode("ascii").lower()
            new_oid = fields[3].decode("ascii").lower()
            status_code = fields[4].decode("ascii")
        except UnicodeDecodeError as exc:
            raise GitChangeUnavailable("Git returned a non-ASCII raw change header") from exc
        path_count = 2 if status_code and status_code[0] in {"R", "C"} else 1
        if index + path_count > len(tokens):
            raise GitChangeUnavailable("Git returned a truncated raw change record")
        paths = tuple(_decode_path(token) for token in tokens[index : index + path_count])
        records.append(
            _RawChange(
                status=status_code,
                paths=paths,
                old_path_type=_git_mode_type(old_mode),
                new_path_type=_git_mode_type(new_mode),
                old_mode=old_mode,
                new_mode=new_mode,
                old_oid=old_oid,
                new_oid=new_oid,
            )
        )
        index += path_count
    return records


def _git_mode_type(mode: str) -> str:
    if mode == "000000":
        return "missing"
    if mode == "120000":
        return "symlink"
    if mode == "160000":
        return "submodule"
    if mode.startswith("100"):
        return "file"
    if mode.startswith("040"):
        return "tree"
    return f"mode_{mode}"


def _combine_git_records(
    named_records: list[tuple[str, tuple[str, ...]]],
    raw_records: list[_RawChange],
    *,
    comparison: str,
) -> list[GitChange]:
    raw_by_key = {(record.status, record.paths): record for record in raw_records}
    if len(raw_by_key) != len(raw_records):
        raise GitChangeUnavailable("Git returned duplicate raw change records")
    changes: list[GitChange] = []
    for status_code, paths in named_records:
        raw = raw_by_key.pop((status_code, paths), None)
        if raw is None:
            raise GitChangeUnavailable("Git name-status and raw change streams disagree")
        code = status_code[0]
        if code in {"R", "C"}:
            old_path, new_path = paths
            prefix = "renamed" if code == "R" else "copied"
            changes.extend(
                [
                    GitChange(
                        path=old_path,
                        comparison=comparison,
                        change_kind=f"{prefix}_from",
                        status=status_code,
                        old_path=old_path,
                        new_path=new_path,
                        path_type=raw.old_path_type,
                        old_path_type=raw.old_path_type,
                        new_path_type=raw.new_path_type,
                        old_mode=raw.old_mode,
                        new_mode=raw.new_mode,
                        old_oid=raw.old_oid,
                        new_oid=raw.new_oid,
                    ),
                    GitChange(
                        path=new_path,
                        comparison=comparison,
                        change_kind=f"{prefix}_to",
                        status=status_code,
                        old_path=old_path,
                        new_path=new_path,
                        path_type=raw.new_path_type,
                        old_path_type=raw.old_path_type,
                        new_path_type=raw.new_path_type,
                        old_mode=raw.old_mode,
                        new_mode=raw.new_mode,
                        old_oid=raw.old_oid,
                        new_oid=raw.new_oid,
                    ),
                ]
            )
            continue
        path = paths[0]
        change_kind = {
            "A": "added",
            "M": "modified",
            "D": "deleted",
            "T": "type_changed",
            "U": "conflicted",
            "X": "unknown",
            "B": "pairing_broken",
        }.get(code, f"status_{code}")
        path_type = raw.new_path_type if raw.new_path_type != "missing" else raw.old_path_type
        changes.append(
            GitChange(
                path=path,
                comparison=comparison,
                change_kind=change_kind,
                status=status_code,
                path_type=path_type,
                old_path_type=raw.old_path_type,
                new_path_type=raw.new_path_type,
                old_mode=raw.old_mode,
                new_mode=raw.new_mode,
                old_oid=raw.old_oid,
                new_oid=raw.new_oid,
            )
        )
    if raw_by_key:
        raise GitChangeUnavailable("Git raw change stream contains unmatched records")
    return changes


def _untracked_changes(root: Path, data: bytes, *, comparison: str) -> list[GitChange]:
    changes: list[GitChange] = []
    for raw_path in _nul_tokens(data):
        path = _decode_path(raw_path)
        path_type = _filesystem_path_type(root / path)
        changes.append(
            GitChange(
                path=path,
                comparison=comparison,
                change_kind="untracked",
                status="?",
                path_type=path_type,
                old_path_type="missing",
                new_path_type=path_type,
            )
        )
    return changes


def _parse_unmerged_paths(data: bytes) -> set[str]:
    paths: set[str] = set()
    for token in _nul_tokens(data):
        header, separator, raw_path = token.partition(b"\t")
        if not separator or len(header.split()) != 3:
            raise GitChangeUnavailable("Git returned an invalid unmerged-index record")
        paths.add(_decode_path(raw_path))
    return paths


def _label_unmerged_paths(changes: list[GitChange], unmerged_paths: set[str]) -> list[GitChange]:
    if not unmerged_paths:
        return changes
    labeled: list[GitChange] = []
    seen: set[str] = set()
    for change in changes:
        if change.path in unmerged_paths:
            labeled.append(replace(change, change_kind="conflicted", status="U"))
            seen.add(change.path)
        else:
            labeled.append(change)
    for path in sorted(unmerged_paths - seen):
        labeled.append(
            GitChange(
                path=path,
                comparison="unmerged_index",
                change_kind="conflicted",
                status="U",
            )
        )
    return labeled


def _filesystem_path_type(path: Path) -> str:
    try:
        mode = path.lstat().st_mode
    except OSError as exc:
        raise GitChangeUnavailable(f"cannot inspect untracked path {path.name!r}: {exc}") from exc
    if stat.S_ISLNK(mode):
        return "symlink"
    if stat.S_ISREG(mode):
        return "file"
    if stat.S_ISDIR(mode):
        return "directory"
    return "special"
