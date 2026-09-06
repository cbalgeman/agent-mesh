#!/usr/bin/env python3
"""Provider-neutral example for retrieval on the first edit/write to a path."""

from __future__ import annotations

import json
import math
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from agent_mesh.core.decision_applicability import normalize_candidate_path


REQUEST_SCHEMA = "agent-mesh.decision-hook-request.v1"
CONTEXT_SCHEMA = "agent-mesh.decision-context.v1"
MAX_CONTEXT_BYTES = 256 * 1024
MAX_DIAGNOSTIC_BYTES = 1024
DEFAULT_TIMEOUT_SECONDS = 10.0


@dataclass(frozen=True)
class HookResult:
    returncode: int
    context: dict[str, Any] | None
    stderr: str


def _bounded_utf8(value: str, limit: int) -> str:
    encoded = value.encode("utf-8")
    if len(encoded) <= limit:
        return value
    return encoded[:limit].decode("utf-8", errors="ignore")


def _validate_context(
    stdout: str,
    *,
    boundary: str,
    expected_path: str,
    expected_store_id: str,
    returncode: int,
) -> dict[str, Any] | None:
    if len(stdout.encode("utf-8")) > MAX_CONTEXT_BYTES:
        raise ValueError("agent-q decisions hook output exceeds the context bound")
    if returncode == 2:
        if stdout.strip():
            raise ValueError("agent-q decisions hook returned stdout with invalid-input exit 2")
        return None
    if not stdout.strip():
        raise ValueError("agent-q decisions hook returned no JSON context")
    try:
        parsed = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise ValueError("agent-q decisions hook returned malformed JSON") from exc
    if not isinstance(parsed, dict):
        raise ValueError("agent-q decisions hook returned non-object JSON")
    if parsed.get("schema") != CONTEXT_SCHEMA:
        raise ValueError("agent-q decisions hook returned an unsupported context schema")
    if parsed.get("privacy_class") != "project_private":
        raise ValueError("agent-q decisions hook returned an invalid privacy class")
    repository = parsed.get("repository")
    if repository is not None and (
        not isinstance(repository, dict) or repository.get("store_id") != expected_store_id
    ):
        raise ValueError("agent-q decisions hook returned context for a different repository")
    request = parsed.get("request")
    if not isinstance(request, dict) or request.get("boundary") != boundary:
        raise ValueError("agent-q decisions hook did not echo the request boundary")
    if not isinstance(parsed.get("decisions"), list):
        raise ValueError("agent-q decisions hook returned an invalid decision list")
    if returncode == 0:
        if parsed.get("complete") is not True or parsed.get("context_status") != "complete":
            raise ValueError("agent-q decisions hook returned incomplete context with exit 0")
        if repository is None:
            raise ValueError("agent-q decisions hook returned context without repository identity")
        if request.get("paths") != [expected_path]:
            raise ValueError("agent-q decisions hook did not echo the normalized request path")
    elif returncode == 3:
        if parsed.get("complete") is not False or parsed.get("context_status") not in {
            "incomplete",
            "unavailable",
        }:
            raise ValueError("agent-q decisions hook returned complete context with exit 3")
        if request.get("paths") != [] or request.get("path_count") != 1:
            raise ValueError("agent-q decisions hook returned invalid incomplete request metadata")
    return parsed


def context_for_new_path(
    path: str,
    *,
    boundary: str,
    seen_paths: set[str],
    repo_root: str | Path,
    expected_store_id: str,
    agent_q: Sequence[str] = ("agent-q",),
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS,
) -> HookResult | None:
    """Return advisory context once per completed path lookup in the current task."""

    if boundary not in {"edit", "write"}:
        raise ValueError("boundary must be 'edit' or 'write'")
    root = Path(repo_root)
    if not root.is_absolute():
        raise ValueError("repo_root must be an absolute path")
    try:
        root = root.resolve(strict=True)
    except OSError as exc:
        raise ValueError("repo_root must identify an existing repository directory") from exc
    if not root.is_dir():
        raise ValueError("repo_root must identify an existing repository directory")
    if not isinstance(expected_store_id, str) or not expected_store_id.strip():
        raise ValueError("expected_store_id must come from the initial task preflight")
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be a positive finite number")
    normalized_path = normalize_candidate_path(path)
    if normalized_path in seen_paths:
        return None
    request = {
        "schema": REQUEST_SCHEMA,
        "boundary": boundary,
        "paths": [normalized_path],
    }
    completed = subprocess.run(
        [*agent_q, "decisions", "hook"],
        input=json.dumps(request, separators=(",", ":")),
        text=True,
        encoding="utf-8",
        errors="strict",
        capture_output=True,
        check=False,
        cwd=root,
        timeout=timeout_seconds,
    )
    returncode = int(completed.returncode)
    if returncode not in {0, 2, 3}:
        raise ValueError(f"agent-q decisions hook returned unsupported exit {returncode}")
    context = _validate_context(
        completed.stdout,
        boundary=boundary,
        expected_path=normalized_path,
        expected_store_id=expected_store_id,
        returncode=returncode,
    )
    if completed.returncode == 0:
        seen_paths.add(normalized_path)
    return HookResult(
        returncode=returncode,
        context=context,
        stderr=_bounded_utf8(completed.stderr, MAX_DIAGNOSTIC_BYTES),
    )
