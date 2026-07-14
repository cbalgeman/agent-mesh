"""Provider-neutral runtime launch adapters for dispatch.

Runtime adapters are the boundary between canonical dispatch state and a local agent process. They
build sanitized launch specs only; they do not acquire leases, append lifecycle events, post RES
messages, or persist raw prompts. That keeps agent-mesh core platform-agnostic while allowing host
controllers to launch Claude Code, Codex, OpenCode, or future runtimes through the same contract.
"""
from __future__ import annotations

import hashlib
import os
import pty
import select
import subprocess
import tempfile
import termios
import time
from pathlib import Path

from .adapters import AgentRuntimeAdapter
from .types import AgentLaunchResult, AgentLaunchSpec, AgentRunRequest


DEFAULT_CODEX_BINARY = "/Applications/Codex.app/Contents/Resources/codex"


class AgentProcessLauncher:
    """Run sanitized agent launch specs and return privacy-safe execution results."""

    def launch(self, spec: AgentLaunchSpec) -> AgentLaunchResult:
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
        try:
            completed = subprocess.run(
                spec.argv,
                cwd=spec.cwd,
                input=spec.stdin_text,
                capture_output=True,
                text=True,
                timeout=spec.timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            return AgentLaunchResult(
                status="timeout",
                exit_code=None,
                stdout=_decode_timeout_stream(exc.stdout),
                stderr=_decode_timeout_stream(exc.stderr),
                duration_seconds=time.monotonic() - started,
                metadata=metadata,
            )
        except OSError as exc:
            return AgentLaunchResult(
                status="launch_error",
                exit_code=None,
                stdout="",
                stderr=str(exc),
                duration_seconds=time.monotonic() - started,
                metadata=metadata,
            )
        return AgentLaunchResult(
            status="completed" if completed.returncode == 0 else "failed",
            exit_code=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
            duration_seconds=time.monotonic() - started,
            metadata=metadata,
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
                        written = os.write(stdin_fd, stdin_bytes[stdin_offset : stdin_offset + 65536])
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


def _decode_timeout_stream(value: str | bytes | None) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value


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


class CodexCliRuntimeAdapter(AgentRuntimeAdapter):
    """Build launch specs for the local Codex CLI/Desktop runtime.

    The prompt is carried in-memory as stdin text for the future launcher. It is intentionally absent
    from argv and metadata so dispatch run events can record the launch metadata without leaking
    request bodies or generated prompt text.
    """

    def build_launch(self, request: AgentRunRequest) -> AgentLaunchSpec:
        if "binary" in self.spec.options:
            binary = str(self.spec.options["binary"]).strip()
        else:
            binary = DEFAULT_CODEX_BINARY
        if not binary:
            raise ValueError("codex runtime binary must be configured")
        sandbox = str(self.spec.options.get("sandbox") or "workspace-write").strip()
        if not sandbox:
            raise ValueError("codex runtime sandbox must be configured")
        prompt_sha = hashlib.sha256(request.prompt.encode("utf-8")).hexdigest()
        fd, stdout_file_name = tempfile.mkstemp(prefix="agent-mesh-codex-", suffix=".txt")
        os.close(fd)
        stdout_file = Path(stdout_file_name)
        stdout_file.unlink(missing_ok=True)
        argv = [
            binary,
            "exec",
            "--sandbox",
            sandbox,
            "--skip-git-repo-check",
            "--output-last-message",
            str(stdout_file),
        ]
        return AgentLaunchSpec(
            argv=argv,
            cwd=Path(request.project_root),
            requires_pty=True,
            timeout_seconds=int(request.timeout_seconds),
            prompt_sha=prompt_sha,
            stdin_text=request.prompt,
            metadata={
                "runtime": "codex-cli",
                "target_agent": request.target_agent,
                "run_id": request.run_id,
                "session_uuid": request.session_uuid,
                "gen_ai.system": "openai",
                "sandbox": sandbox,
            },
            stdout_file=stdout_file,
        )
