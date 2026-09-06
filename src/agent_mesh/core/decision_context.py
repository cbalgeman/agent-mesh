"""Shared decision-context envelope construction for CLI and Workbench surfaces."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Iterable

from agent_mesh.config import AgentMeshConfig
from agent_mesh.core.decision_applicability import (
    DecisionApplicabilityBudgetExceeded,
    evaluate_decision_applicability,
)
from agent_mesh.core.git_changes import GitChange, GitChangeSet
from agent_mesh.store.read_model import ReadModelSnapshot

DECISION_CONTEXT_SCHEMA = "agent-mesh.decision-context.v1"
MAX_DECISION_CONTEXT_JSON_BYTES = 256 * 1024
MAX_DECISION_CONTEXT_OPERATIONS = 250_000
MAX_DECISION_CONTEXT_RESULT_ITEMS = 20_000
MAX_DECISION_CONTEXT_WORK_BYTES = 2 * 1024 * 1024


class _DecisionContextBudgetExceeded(RuntimeError):
    pass


@dataclass
class _DecisionContextBudget:
    items: int = 0
    estimated_bytes: int = 0

    def consume(self, value: object) -> None:
        encoded_bytes = len(json.dumps(value, sort_keys=True, ensure_ascii=False).encode("utf-8"))
        if self.items + 1 > MAX_DECISION_CONTEXT_RESULT_ITEMS:
            raise _DecisionContextBudgetExceeded(
                "decision context exceeds its result-item work budget"
            )
        if self.estimated_bytes + encoded_bytes > MAX_DECISION_CONTEXT_WORK_BYTES:
            raise _DecisionContextBudgetExceeded(
                "decision context exceeds its materialized-byte work budget"
            )
        self.items += 1
        self.estimated_bytes += encoded_bytes


def build_decision_context(
    config: AgentMeshConfig,
    snapshot: ReadModelSnapshot,
    paths: Iterable[str],
    *,
    boundary: str,
    include_proposed: bool = False,
    include_diagnostics: bool = False,
    change_set: GitChangeSet | None = None,
) -> dict[str, Any]:
    """Build one complete envelope from a verified read model and optional Git change set."""

    try:
        selected_paths, decisions = evaluate_decision_applicability(
            snapshot.conn,
            paths,
            include_proposed=include_proposed,
            include_diagnostics=include_diagnostics,
            max_operations=MAX_DECISION_CONTEXT_OPERATIONS,
            max_result_items=MAX_DECISION_CONTEXT_RESULT_ITEMS,
            max_estimated_bytes=MAX_DECISION_CONTEXT_WORK_BYTES,
        )
    except DecisionApplicabilityBudgetExceeded as exc:
        return _incomplete_decision_context(
            repository=_repository_fields(config, snapshot),
            request=_request_metadata(boundary=boundary, change_set=change_set),
            paths=exc.paths,
            diagnostic=str(exc),
        )
    request: dict[str, Any] = _request_metadata(boundary=boundary, change_set=change_set)
    request["paths"] = list(selected_paths)
    if change_set is not None:
        context_budget = _DecisionContextBudget()
        try:
            request["changes"] = _bounded_change_payload(change_set.changes, context_budget)
            _annotate_change_matches(
                decisions,
                change_set.changes,
                budget=context_budget,
            )
        except _DecisionContextBudgetExceeded as exc:
            return _incomplete_decision_context(
                repository=_repository_fields(config, snapshot),
                request=_request_metadata(boundary=boundary, change_set=change_set),
                paths=selected_paths,
                diagnostic=str(exc),
            )
    return {
        "schema": DECISION_CONTEXT_SCHEMA,
        "privacy_class": "project_private",
        "context_status": "complete",
        "complete": True,
        "repository": _repository_fields(config, snapshot),
        "request": request,
        "decisions": decisions,
        "diagnostics": list(snapshot.warnings),
    }


def render_bounded_decision_context_json(
    result: dict[str, Any],
    *,
    max_bytes: int = MAX_DECISION_CONTEXT_JSON_BYTES,
) -> tuple[str, bool]:
    """Render a context envelope or a bounded, explicitly incomplete replacement."""

    rendered = json.dumps(result, sort_keys=True)
    if len(rendered.encode("utf-8")) <= max_bytes:
        complete = result.get("complete") is True and result.get("context_status") == "complete"
        return rendered, complete

    raw_request = result.get("request")
    request: dict[str, Any] = raw_request if isinstance(raw_request, dict) else {}
    raw_paths = request.get("paths", [])
    paths = tuple(str(path) for path in raw_paths) if isinstance(raw_paths, list) else ()
    compact = _incomplete_decision_context(
        repository=result.get("repository"),
        request=request,
        paths=paths,
        diagnostic="applicable decision context exceeds the JSON output bound; narrow the path set",
    )
    compact_rendered = json.dumps(compact, sort_keys=True)
    if len(compact_rendered.encode("utf-8")) <= max_bytes:
        return compact_rendered, False
    return (
        json.dumps(
            {
                "complete": False,
                "context_status": "incomplete",
                "privacy_class": "project_private",
            },
            sort_keys=True,
        ),
        False,
    )


def build_unavailable_decision_context(
    *,
    mode: str,
    base: str | None,
    diagnostic: str,
    boundary: str = "change_review",
) -> dict[str, Any]:
    """Return the shared versioned envelope for unavailable changed-path context."""

    return {
        "schema": DECISION_CONTEXT_SCHEMA,
        "privacy_class": "project_private",
        "context_status": "unavailable",
        "complete": False,
        "repository": None,
        "request": {
            "boundary": boundary,
            "mode": mode,
            "base": base,
            "path_count": 0,
            "paths": [],
        },
        "decisions": [],
        "diagnostics": [_bounded_diagnostic(diagnostic)],
    }


def _repository_fields(
    config: AgentMeshConfig,
    snapshot: ReadModelSnapshot,
) -> dict[str, Any]:
    return {
        "project_key": config.project_key,
        "store_id": config.store_id,
        "projection_version": snapshot.projection_version,
        "source_log_sha256": snapshot.source_log_sha256,
        "event_seq": snapshot.event_seq,
        "read_model": snapshot.read_model,
    }


def _request_metadata(
    *,
    boundary: str,
    change_set: GitChangeSet | None,
) -> dict[str, Any]:
    request: dict[str, Any] = {"boundary": boundary}
    if change_set is not None:
        request.update(
            {
                "mode": change_set.mode,
                "base": change_set.base,
                "base_oid": change_set.base_oid,
                "head_oid": change_set.head_oid,
                "merge_base": change_set.merge_base,
            }
        )
    return request


def _incomplete_decision_context(
    *,
    repository: object,
    request: dict[str, Any],
    paths: Iterable[str],
    diagnostic: str,
) -> dict[str, Any]:
    selected_paths = tuple(paths)
    compact_request = {
        key: value for key, value in request.items() if key not in {"paths", "changes"}
    }
    compact_request.update(
        {
            "path_count": len(selected_paths),
            "paths": [],
            "paths_sha256": _paths_sha256(selected_paths),
        }
    )
    return {
        "schema": DECISION_CONTEXT_SCHEMA,
        "privacy_class": "project_private",
        "context_status": "incomplete",
        "complete": False,
        "repository": repository,
        "request": compact_request,
        "decisions": [],
        "diagnostics": [_bounded_diagnostic(diagnostic)],
    }


def _paths_sha256(paths: tuple[str, ...]) -> str:
    digest = hashlib.sha256()
    encoder = json.JSONEncoder(ensure_ascii=False, separators=(",", ":"))
    for chunk in encoder.iterencode(paths):
        digest.update(chunk.encode("utf-8"))
    return digest.hexdigest()


def _bounded_diagnostic(value: object) -> str:
    sanitized = " ".join(
        "".join(character if character.isprintable() else " " for character in str(value)).split()
    )
    return sanitized[:1000]


def _annotate_change_matches(
    decisions: list[dict[str, Any]],
    changes: tuple[GitChange, ...],
    *,
    budget: _DecisionContextBudget,
) -> None:
    changes_by_path: dict[str, list[GitChange]] = {}
    for change in changes:
        changes_by_path.setdefault(change.path, []).append(change)
    for decision in decisions:
        for collection_name in ("matches", "exclusions"):
            collection = decision.get(collection_name)
            if not isinstance(collection, list):
                continue
            annotated: list[dict[str, Any]] = []
            for item in collection:
                if not isinstance(item, dict):
                    continue
                path = item.get("path")
                path_changes = changes_by_path.get(str(path), []) if path is not None else []
                if not path_changes:
                    annotated.append(item)
                    continue
                for change in path_changes:
                    enriched = dict(item)
                    enriched.update(change.as_dict())
                    budget.consume(enriched)
                    annotated.append(enriched)
            decision[collection_name] = annotated


def _bounded_change_payload(
    changes: tuple[GitChange, ...],
    budget: _DecisionContextBudget,
) -> list[dict[str, str | None]]:
    payload: list[dict[str, str | None]] = []
    for change in changes:
        item = change.as_dict()
        budget.consume(item)
        payload.append(item)
    return payload
