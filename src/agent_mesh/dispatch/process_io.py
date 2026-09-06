"""Bounded, shell-free subprocess transport for managed runtime processes."""

from __future__ import annotations

import os
import selectors
import signal
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import IO, cast


@dataclass(frozen=True)
class BoundedProcessResult:
    returncode: int | None
    stdout: bytes
    stderr: bytes
    status: str


def run_bounded_process(
    argv: list[str],
    *,
    cwd: Path,
    environment: dict[str, str],
    stdin: bytes,
    timeout_seconds: float,
    stdout_limit: int,
    stderr_limit: int,
) -> BoundedProcessResult:
    """Run one process while actively bounding both output streams and its process group."""

    try:
        process = subprocess.Popen(
            argv,
            cwd=cwd,
            env=environment,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            shell=False,
            close_fds=True,
            start_new_session=True,
        )
    except OSError:
        return BoundedProcessResult(None, b"", b"", "launch_error")

    stdin_stream = process.stdin
    stdout_stream = process.stdout
    stderr_stream = process.stderr
    selector: selectors.BaseSelector | None = None
    output = {"stdout": bytearray(), "stderr": bytearray()}
    limits = {"stdout": stdout_limit, "stderr": stderr_limit}
    stdin_offset = 0
    status = "completed"
    deadline = time.monotonic() + timeout_seconds
    termination_deadline: float | None = None
    try:
        if stdin_stream is None or stdout_stream is None or stderr_stream is None:
            raise OSError("subprocess pipe setup failed")
        selector = selectors.DefaultSelector()
        streams = {
            "stdin": stdin_stream,
            "stdout": stdout_stream,
            "stderr": stderr_stream,
        }
        for stream in streams.values():
            os.set_blocking(stream.fileno(), False)
        selector.register(stdout_stream, selectors.EVENT_READ, "stdout")
        selector.register(stderr_stream, selectors.EVENT_READ, "stderr")
        if stdin:
            selector.register(stdin_stream, selectors.EVENT_WRITE, "stdin")
        else:
            stdin_stream.close()

        while True:
            now = time.monotonic()
            if status == "completed" and now >= deadline:
                status = "timeout"
                _terminate_process_group(process)
                _close_registered(selector, stdin_stream)
                termination_deadline = now + 1.0
            if termination_deadline is not None and now >= termination_deadline:
                break

            wait_until = termination_deadline if termination_deadline is not None else deadline
            events = selector.select(max(0.0, min(0.05, wait_until - now)))
            for key, _mask in events:
                stream = cast(IO[bytes], key.fileobj)
                stream_name = key.data
                if stream_name == "stdin":
                    try:
                        written = os.write(
                            stdin_stream.fileno(), stdin[stdin_offset : stdin_offset + 65536]
                        )
                    except (BrokenPipeError, OSError):
                        _close_registered(selector, stdin_stream)
                    else:
                        stdin_offset += written
                        if stdin_offset >= len(stdin):
                            _close_registered(selector, stdin_stream)
                    continue

                try:
                    chunk = os.read(stream.fileno(), 65536)
                except BlockingIOError:
                    continue
                except OSError:
                    chunk = b""
                if not chunk:
                    _close_registered(selector, stream)
                    continue
                target = output[stream_name]
                if status == "completed" and len(target) + len(chunk) > limits[stream_name]:
                    status = "output_too_large"
                    output["stdout"].clear()
                    output["stderr"].clear()
                    _terminate_process_group(process)
                    _close_registered(selector, stdin_stream)
                    termination_deadline = time.monotonic() + 1.0
                elif status == "completed":
                    target.extend(chunk)

            read_streams_open = any(
                key.data in {"stdout", "stderr"} for key in selector.get_map().values()
            )
            stdin_open = any(key.data == "stdin" for key in selector.get_map().values())
            if process.poll() is not None and not read_streams_open:
                if stdin_open:
                    _close_registered(selector, stdin_stream)
                break
    except Exception:
        status = "launch_error"
        output["stdout"].clear()
        output["stderr"].clear()
        _terminate_process_group(process)
    finally:
        if selector is not None:
            try:
                registered = list(selector.get_map().values())
            except Exception:
                registered = []
            for key in registered:
                _close_registered(selector, cast(IO[bytes], key.fileobj))
            try:
                selector.close()
            except Exception:
                pass
        for cleanup_stream in (stdin_stream, stdout_stream, stderr_stream):
            if cleanup_stream is not None and not cleanup_stream.closed:
                try:
                    cleanup_stream.close()
                except Exception:
                    pass
        if process.poll() is None:
            _terminate_process_group(process)
        try:
            process.wait(timeout=1)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=1)

    if status != "completed":
        return BoundedProcessResult(
            None if status in {"timeout", "launch_error"} else process.returncode,
            bytes(output["stdout"]) if status == "timeout" else b"",
            bytes(output["stderr"]) if status == "timeout" else b"",
            status,
        )
    return BoundedProcessResult(
        process.returncode,
        bytes(output["stdout"]),
        bytes(output["stderr"]),
        status,
    )


def _close_registered(selector: selectors.BaseSelector, stream: IO[bytes]) -> None:
    if stream.closed:
        return
    try:
        selector.unregister(stream)
    except Exception:
        pass
    try:
        stream.close()
    except Exception:
        pass


def _terminate_process_group(process: subprocess.Popen[bytes]) -> None:
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except OSError:
        try:
            process.kill()
        except OSError:
            pass
