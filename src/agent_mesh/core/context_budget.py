"""Bounded, local-only reporting for candidate resident agent context."""

from __future__ import annotations

import hashlib
import os
import stat
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from agent_mesh.config import AgentMeshConfig
from agent_mesh.core.decision_context import build_decision_context
from agent_mesh.core.decision_digest import DecisionDigestOverflow, render_decision_digest
from agent_mesh.core.decision_applicability import DecisionPathError, normalize_candidate_paths
from agent_mesh.store.read_model import ReadModelUnavailable, open_read_model


CONTEXT_BUDGET_SCHEMA = "agent-mesh.context-budget.v1"
MAX_CONTEXT_SOURCE_BYTES = 2 * 1024 * 1024
MAX_CONTEXT_REPORT_BYTES = 16 * 1024 * 1024
MAX_CONTEXT_INSPECTION_BYTES = 16 * 1024 * 1024
MAX_CONTEXT_INSPECTION_EVENTS = 100_000
CONTEXT_BUDGET_TIMEOUT_SECONDS = 10.0
_INCOMPLETE_SOURCE_STATUSES = frozenset(
    {"unsafe", "unreadable", "changing", "oversize"}
)


class _SourceReadFailure(RuntimeError):
    def __init__(self, status: str, detail: str) -> None:
        super().__init__(detail)
        self.status = status
        self.detail = detail


@dataclass
class _InspectionBudget:
    deadline_monotonic: float
    inspection_byte_limit: int
    candidate_byte_limit: int
    event_limit: int
    inspection_bytes: int = 0
    candidate_bytes: int = 0
    events: int = 0

    @property
    def remaining_inspection_bytes(self) -> int:
        return max(0, self.inspection_byte_limit - self.inspection_bytes)

    @property
    def remaining_candidate_bytes(self) -> int:
        return max(0, self.candidate_byte_limit - self.candidate_bytes)

    @property
    def remaining_events(self) -> int:
        return max(0, self.event_limit - self.events)

    def require_time(self) -> None:
        if time.monotonic() >= self.deadline_monotonic:
            raise _SourceReadFailure(
                "unavailable", "context-budget inspection exceeded its time bound"
            )

    def consume_source(self, size: int) -> None:
        if (
            size > self.remaining_inspection_bytes
            or size > self.remaining_candidate_bytes
        ):
            raise _SourceReadFailure(
                "oversize", "aggregate context-budget byte bound exhausted"
            )
        self.inspection_bytes += size
        self.candidate_bytes += size

    def consume_replay(self, *, source_bytes: int, events: int) -> None:
        if source_bytes > self.remaining_inspection_bytes or events > self.remaining_events:
            raise _SourceReadFailure(
                "unavailable", "aggregate canonical replay bound exhausted"
            )
        self.inspection_bytes += source_bytes
        self.events += events

    def consume_digest(self, size: int) -> None:
        if size > self.remaining_candidate_bytes:
            raise _SourceReadFailure(
                "oversize", "aggregate candidate-context byte bound exhausted"
            )
        self.candidate_bytes += size


def _stable_root_file_bytes(
    root: Path,
    relative: str,
    *,
    maximum_bytes: int,
    deadline_monotonic: float,
) -> bytes:
    if time.monotonic() >= deadline_monotonic:
        raise _SourceReadFailure(
            "unavailable", "context-budget inspection exceeded its time bound"
        )
    if maximum_bytes < 1:
        raise _SourceReadFailure("oversize", "aggregate context-budget byte bound exhausted")
    path = root / relative
    try:
        before_path = path.lstat()
    except FileNotFoundError:
        raise
    except OSError as exc:
        raise _SourceReadFailure("unreadable", str(exc)) from exc
    if stat.S_ISLNK(before_path.st_mode) or not stat.S_ISREG(before_path.st_mode):
        raise _SourceReadFailure("unsafe", "source is not a regular no-follow file")
    no_follow = getattr(os, "O_NOFOLLOW", 0)
    if not no_follow:
        raise _SourceReadFailure("unsafe", "platform has no no-follow file-open primitive")
    flags = os.O_RDONLY | no_follow | getattr(os, "O_CLOEXEC", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise _SourceReadFailure("unreadable", str(exc)) from exc
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise _SourceReadFailure("unsafe", "opened source is not a regular file")
        if before.st_size > MAX_CONTEXT_SOURCE_BYTES:
            raise _SourceReadFailure(
                "oversize", f"source exceeds {MAX_CONTEXT_SOURCE_BYTES} bytes"
            )
        if before.st_size > maximum_bytes:
            raise _SourceReadFailure(
                "oversize", "source exceeds the remaining aggregate context-budget byte bound"
            )
        chunks: list[bytes] = []
        remaining = min(MAX_CONTEXT_SOURCE_BYTES, maximum_bytes) + 1
        while remaining:
            if time.monotonic() >= deadline_monotonic:
                raise _SourceReadFailure(
                    "unavailable", "context-budget inspection exceeded its time bound"
                )
            chunk = os.read(descriptor, min(64 * 1024, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        data = b"".join(chunks)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    try:
        after_path = path.lstat()
    except OSError as exc:
        raise _SourceReadFailure("changing", "source path changed during inspection") from exc
    before_identity = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    after_identity = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    after_path_identity = (
        after_path.st_dev,
        after_path.st_ino,
        after_path.st_size,
        after_path.st_mtime_ns,
    )
    if before_identity != after_identity or before_identity[:2] != (
        before_path.st_dev,
        before_path.st_ino,
    ) or after_path_identity != after_identity:
        raise _SourceReadFailure("changing", "source changed during inspection")
    if len(data) > MAX_CONTEXT_SOURCE_BYTES:
        raise _SourceReadFailure("oversize", f"source exceeds {MAX_CONTEXT_SOURCE_BYTES} bytes")
    try:
        data.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise _SourceReadFailure("unreadable", "source is not valid UTF-8") from exc
    return data


def _source_entry(
    config: AgentMeshConfig,
    relative: str,
    budget: _InspectionBudget,
) -> dict[str, Any]:
    entry: dict[str, Any] = {
        "kind": "instruction_file",
        "path": relative,
        "residency_status": "unknown",
        "status": "missing",
        "bytes": 0,
        "estimated_tokens": 0,
        "sha256": None,
    }
    try:
        budget.require_time()
        data = _stable_root_file_bytes(
            config.project_root.resolve(),
            relative,
            maximum_bytes=min(
                budget.remaining_inspection_bytes,
                budget.remaining_candidate_bytes,
            ),
            deadline_monotonic=budget.deadline_monotonic,
        )
        budget.consume_source(len(data))
    except FileNotFoundError:
        return entry
    except _SourceReadFailure as exc:
        entry["status"] = exc.status
        entry["diagnostic"] = exc.detail[:1000]
        return entry
    entry.update(
        {
            "status": "measured",
            "bytes": len(data),
            "estimated_tokens": (len(data) + 3) // 4,
            "sha256": hashlib.sha256(data).hexdigest(),
        }
    )
    return entry


def _hook_entries(
    config: AgentMeshConfig,
    paths: Iterable[str],
    budget: _InspectionBudget,
) -> list[dict[str, Any]]:
    selected = normalize_candidate_paths(paths)
    if not selected:
        return []
    try:
        budget.require_time()
        with open_read_model(
            config,
            max_bytes=budget.remaining_inspection_bytes,
            max_events=budget.remaining_events,
            deadline_monotonic=budget.deadline_monotonic,
        ) as snapshot:
            budget.consume_replay(
                source_bytes=snapshot.source_log_bytes,
                events=len(snapshot.records),
            )
            entries: list[dict[str, Any]] = []
            for path in selected:
                budget.require_time()
                context = build_decision_context(
                    config,
                    snapshot,
                    [path],
                    boundary="context_budget_sample",
                )
                try:
                    digest = render_decision_digest(context)
                except DecisionDigestOverflow as exc:
                    entries.append(
                        {
                            "kind": "decision_digest_sample",
                            "path": path,
                            "residency_status": "potential",
                            "status": "oversize",
                            "bytes": 0,
                            "estimated_tokens": 0,
                            "sha256": None,
                            "diagnostic": str(exc),
                        }
                    )
                    continue
                encoded = digest.encode("utf-8")
                try:
                    budget.consume_digest(len(encoded))
                except _SourceReadFailure as exc:
                    entries.append(
                        {
                            "kind": "decision_digest_sample",
                            "path": path,
                            "residency_status": "potential",
                            "status": exc.status,
                            "bytes": 0,
                            "estimated_tokens": 0,
                            "sha256": None,
                            "diagnostic": exc.detail,
                        }
                    )
                    continue
                entries.append(
                    {
                        "kind": "decision_digest_sample",
                        "path": path,
                        "residency_status": "potential",
                        "status": "measured" if context.get("complete") is True else "unavailable",
                        "bytes": len(encoded),
                        "estimated_tokens": (len(encoded) + 3) // 4,
                        "sha256": hashlib.sha256(encoded).hexdigest(),
                    }
                )
            return entries
    except (ReadModelUnavailable, OSError, _SourceReadFailure) as exc:
        detail = exc.detail if isinstance(exc, _SourceReadFailure) else str(exc)
        return [
            {
                "kind": "decision_digest_sample",
                "path": path,
                "residency_status": "potential",
                "status": "unavailable",
                "bytes": 0,
                "estimated_tokens": 0,
                "sha256": None,
                "diagnostic": detail[:1000],
            }
            for path in selected
        ]


def build_context_budget_report(
    configs: Iterable[AgentMeshConfig],
    *,
    scope: str,
    sample_paths: Iterable[str] | None = None,
    deadline_monotonic: float | None = None,
    inspection_byte_limit: int = MAX_CONTEXT_INSPECTION_BYTES,
    candidate_byte_limit: int = MAX_CONTEXT_REPORT_BYTES,
    inspection_event_limit: int = MAX_CONTEXT_INSPECTION_EVENTS,
) -> dict[str, Any]:
    """Measure declared root instructions and representative digest output without writing."""

    if min(inspection_byte_limit, candidate_byte_limit, inspection_event_limit) < 1:
        raise ValueError("context-budget inspection limits must be positive")
    budget = _InspectionBudget(
        deadline_monotonic=(
            deadline_monotonic
            if deadline_monotonic is not None
            else time.monotonic() + CONTEXT_BUDGET_TIMEOUT_SECONDS
        ),
        inspection_byte_limit=inspection_byte_limit,
        candidate_byte_limit=candidate_byte_limit,
        event_limit=inspection_event_limit,
    )
    projects: list[dict[str, Any]] = []
    fingerprints: dict[str, list[dict[str, str]]] = {}
    complete = True
    total_bytes = 0
    total_ceiling = 0
    for config in configs:
        sources = [
            _source_entry(config, relative, budget)
            for relative in config.context_budget.instruction_paths
        ]
        try:
            hooks = _hook_entries(
                config,
                config.context_budget.hook_sample_paths
                if sample_paths is None
                else sample_paths,
                budget,
            )
        except DecisionPathError as exc:
            hooks = [
                {
                    "kind": "decision_digest_sample",
                    "path": "<invalid>",
                    "residency_status": "potential",
                    "status": "unavailable",
                    "bytes": 0,
                    "estimated_tokens": 0,
                    "sha256": None,
                    "diagnostic": str(exc)[:1000],
                }
            ]
        entries = [*sources, *hooks]
        project_bytes = sum(int(item["bytes"]) for item in entries)
        project_complete = all(
            item["status"] not in _INCOMPLETE_SOURCE_STATUSES
            and item["status"] != "unavailable"
            for item in entries
        )
        complete = complete and project_complete
        total_bytes += project_bytes
        total_ceiling += config.context_budget.ceiling_bytes
        for item in entries:
            digest = item.get("sha256")
            if not digest:
                continue
            fingerprints.setdefault(str(digest), []).append(
                {
                    "project_key": config.project_key,
                    "kind": str(item["kind"]),
                    "path": str(item["path"]),
                }
            )
        projects.append(
            {
                "project_key": config.project_key,
                "project_root": str(config.project_root),
                "complete": project_complete,
                "candidate_resident_bytes": project_bytes,
                "estimated_tokens": (project_bytes + 3) // 4,
                "ceiling_bytes": config.context_budget.ceiling_bytes,
                "ceiling_status": (
                    "over" if project_bytes > config.context_budget.ceiling_bytes else "within"
                ),
                "sources": sources,
                "hook_outputs": hooks,
            }
        )
    duplicates = [
        {"sha256": digest, "occurrences": occurrences}
        for digest, occurrences in sorted(fingerprints.items())
        if len(occurrences) > 1
    ]
    return {
        "schema": CONTEXT_BUDGET_SCHEMA,
        "privacy_class": "project_private",
        "complete": complete,
        "scope": scope,
        "measurement": "declared files plus representative digest output; actual harness residency is unknown",
        "candidate_resident_bytes": total_bytes,
        "estimated_tokens": (total_bytes + 3) // 4,
        "ceiling_bytes": total_ceiling,
        "ceiling_status": "over" if total_bytes > total_ceiling else "within",
        "inspection": {
            "bytes_read": budget.inspection_bytes,
            "byte_limit": budget.inspection_byte_limit,
            "events_replayed": budget.events,
            "event_limit": budget.event_limit,
            "time_limit_seconds": CONTEXT_BUDGET_TIMEOUT_SECONDS,
        },
        "projects": projects,
        "exact_duplicates": duplicates,
    }


def render_context_budget_text(report: dict[str, Any]) -> str:
    lines = [
        f"Context budget: {str(report['ceiling_status']).upper()}",
        (
            f"candidate resident context: {report['candidate_resident_bytes']} bytes "
            f"(~{report['estimated_tokens']} tokens) / {report['ceiling_bytes']} byte ceiling"
        ),
        f"scope: {report['scope']} | complete: {str(bool(report['complete'])).lower()}",
        (
            f"inspection: {report['inspection']['bytes_read']} / "
            f"{report['inspection']['byte_limit']} bytes; "
            f"{report['inspection']['events_replayed']} / "
            f"{report['inspection']['event_limit']} events; "
            f"{report['inspection']['time_limit_seconds']} second wall-clock bound"
        ),
    ]
    for project in report["projects"]:
        lines.append(
            f"\n{project['project_key']}: {project['candidate_resident_bytes']} bytes "
            f"(~{project['estimated_tokens']} tokens), ceiling={project['ceiling_status']}"
        )
        for item in [*project["sources"], *project["hook_outputs"]]:
            lines.append(
                f"  {item['kind']}\t{item['status']}\t{item['bytes']}\t{item['path']}\t"
                f"residency={item['residency_status']}"
            )
            if item.get("diagnostic"):
                lines.append(f"    warning: {item['diagnostic']}")
    lines.append(
        f"\nexact duplicate groups: {len(report['exact_duplicates'])}"
    )
    lines.append(
        "note: totals are candidate context, not proof that a harness loaded every source"
    )
    return "\n".join(lines)
