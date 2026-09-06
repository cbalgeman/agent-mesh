#!/usr/bin/env python3
"""Thin advisory Claude PreToolUse adapter for Agent Mesh decision digests."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any


MAX_INPUT_BYTES = 256 * 1024
MAX_CONTEXT_CHARS = 10_000
MAX_DIAGNOSTIC_CHARS = 1_000


def _output(additional_context: str) -> int:
    print(
        json.dumps(
            {
                "hookSpecificOutput": {
                    "hookEventName": "PreToolUse",
                    "additionalContext": additional_context[:MAX_CONTEXT_CHARS],
                }
            },
            separators=(",", ":"),
        )
    )
    return 0


def _warning(detail: object) -> int:
    text = " ".join(str(detail).split())[:MAX_DIAGNOSTIC_CHARS]
    return _output(
        "Agent Mesh advisory: path-scoped decision context was unavailable. "
        f"The edit is not blocked by the 0.4.0 advisory policy. Detail: {text}"
    )


def _request() -> dict[str, Any]:
    raw = sys.stdin.buffer.read(MAX_INPUT_BYTES + 1)
    if len(raw) > MAX_INPUT_BYTES:
        raise ValueError(f"hook input exceeds {MAX_INPUT_BYTES} bytes")
    value = json.loads(raw.decode("utf-8"))
    if not isinstance(value, dict):
        raise ValueError("hook input must be a JSON object")
    return value


def main() -> int:
    try:
        request = _request()
        tool_name = request.get("tool_name")
        tool_input = request.get("tool_input")
        if tool_name not in {"Edit", "Write"} or not isinstance(tool_input, dict):
            return _warning("expected Claude PreToolUse input for Edit or Write")
        file_path = tool_input.get("file_path")
        cwd = request.get("cwd")
        if not isinstance(file_path, str) or not file_path:
            return _warning("tool_input.file_path is missing")
        if not isinstance(cwd, str) or not cwd:
            return _warning("hook cwd is missing")

        root_text = os.environ.get("CLAUDE_PROJECT_DIR", cwd)
        root = Path(root_text).resolve(strict=True)
        candidate = Path(file_path)
        if not candidate.is_absolute():
            candidate = Path(cwd) / candidate
        resolved = candidate.resolve(strict=False)
        try:
            relative = resolved.relative_to(root).as_posix()
        except ValueError:
            return _warning("edit path is outside CLAUDE_PROJECT_DIR")

        executable = shutil.which("agent-q")
        if executable is None:
            return _warning("agent-q is not available on PATH")
        completed = subprocess.run(
            [executable, "decisions", "preflight", "--path", relative, "--digest"],
            cwd=root,
            text=True,
            encoding="utf-8",
            errors="strict",
            capture_output=True,
            check=False,
            timeout=10,
        )
        if completed.returncode != 0:
            diagnostic = completed.stderr or completed.stdout or f"agent-q exit {completed.returncode}"
            return _warning(diagnostic)
        if not completed.stdout.strip():
            return _warning("agent-q returned an empty decision digest")
        return _output(completed.stdout.strip())
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError, subprocess.SubprocessError) as exc:
        return _warning(exc)


if __name__ == "__main__":
    raise SystemExit(main())
