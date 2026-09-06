"""Shared, deterministic decision applicability evaluation."""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any, Iterable

from agent_mesh.core.decision_glob import (
    MAX_DECISION_PATTERNS,
    DecisionGlobError,
    compile_meshglob,
)
from agent_mesh.store.sqlite import json_loads


class DecisionPathError(ValueError):
    """Raised when a candidate path is not safe repository-relative POSIX text."""


class DecisionApplicabilityBudgetExceeded(RuntimeError):
    """Raised before applicability evaluation can exceed a caller-provided work budget."""

    def __init__(self, detail: str, paths: tuple[str, ...]) -> None:
        super().__init__(detail)
        self.paths = paths


@dataclass
class _ApplicabilityBudget:
    paths: tuple[str, ...]
    max_operations: int | None
    max_result_items: int | None
    max_estimated_bytes: int | None
    operations: int = 0
    result_items: int = 0
    estimated_bytes: int = 0

    def consume_operation(self, count: int = 1) -> None:
        self.operations += count
        if self.max_operations is not None and self.operations > self.max_operations:
            self._raise("operation")

    def consume_bytes(self, *values: object) -> None:
        self.estimated_bytes += sum(len(str(value).encode("utf-8")) + 8 for value in values)
        if self.max_estimated_bytes is not None and self.estimated_bytes > self.max_estimated_bytes:
            self._raise("materialized-byte")

    def consume_result(self, *values: object) -> None:
        self.result_items += 1
        if self.max_result_items is not None and self.result_items > self.max_result_items:
            self._raise("result-item")
        self.consume_bytes(*values)

    def _raise(self, budget_name: str) -> None:
        raise DecisionApplicabilityBudgetExceeded(
            f"decision applicability exceeds its {budget_name} work budget",
            self.paths,
        )


def normalize_candidate_path(value: str) -> str:
    """Normalize one lexical repository-relative path without touching the filesystem."""

    raw = str(value)
    if not raw or "\x00" in raw:
        raise DecisionPathError("decision path must be non-empty text without NUL")
    try:
        raw_bytes = raw.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise DecisionPathError("decision path must be valid UTF-8 text") from exc
    if len(raw_bytes) > 4096:
        raise DecisionPathError("decision path exceeds 4096 UTF-8 bytes")
    if raw.startswith("/") or re.match(r"^[A-Za-z]:", raw):
        raise DecisionPathError("decision path must be repository-relative")
    parts = raw.split("/")
    if ".." in parts:
        raise DecisionPathError("decision path must not contain parent traversal")
    normalized_parts = [part for part in parts if part not in {"", "."}]
    if not normalized_parts:
        raise DecisionPathError("decision path must identify a repository entry")
    normalized = PurePosixPath(*normalized_parts).as_posix()
    if normalized.startswith("../") or normalized == "..":
        raise DecisionPathError("decision path escapes the repository")
    return normalized


def normalize_candidate_paths(values: Iterable[str]) -> tuple[str, ...]:
    normalized = tuple(sorted({normalize_candidate_path(value) for value in values}))
    if len(normalized) > 10_000:
        raise DecisionPathError("decision preflight exceeds 10000 distinct paths")
    return normalized


def _decision_patterns(
    conn: sqlite3.Connection,
    dec_ulid: str,
    *,
    budget: _ApplicabilityBudget | None = None,
) -> dict[str, list[str]]:
    patterns: dict[str, list[str]] = {"affected": [], "exempt": [], "generated": []}
    for row in conn.execute(
        "SELECT kind, pattern FROM decision_globs WHERE dec_ulid=? ORDER BY kind, pattern",
        (dec_ulid,),
    ):
        kind = str(row["kind"])
        pattern = str(row["pattern"])
        if budget is not None:
            budget.consume_operation()
            budget.consume_bytes(kind, pattern)
        if kind in patterns:
            patterns[kind].append(pattern)
    return patterns


def _compile_patterns(
    patterns: dict[str, list[str]],
    *,
    budget: _ApplicabilityBudget | None = None,
) -> tuple[dict[str, list[tuple[str, Any]]], list[str]]:
    compiled: dict[str, list[tuple[str, Any]]] = {
        "affected": [],
        "exempt": [],
        "generated": [],
    }
    warnings: list[str] = []
    for kind in ("affected", "exempt", "generated"):
        if len(patterns[kind]) > MAX_DECISION_PATTERNS:
            warnings.append(
                f"historical {kind} pattern list exceeds {MAX_DECISION_PATTERNS} entries"
            )
            continue
        for pattern in patterns[kind]:
            if budget is not None:
                budget.consume_operation()
            try:
                compiled[kind].append((pattern, compile_meshglob(pattern)))
            except DecisionGlobError as exc:
                warnings.append(f"invalid historical {kind} pattern {pattern!r}: {exc}")
    return compiled, warnings


def _first_match(
    path: str,
    patterns: list[tuple[str, Any]],
    *,
    budget: _ApplicabilityBudget | None = None,
) -> str | None:
    for source, matcher in patterns:
        if budget is not None:
            budget.consume_operation()
        if matcher.matches(path):
            return source
    return None


def _checks(
    conn: sqlite3.Connection,
    dec_ulid: str,
    *,
    budget: _ApplicabilityBudget | None = None,
) -> list[str]:
    checks: list[str] = []
    for row in conn.execute(
        "SELECT check_name FROM decision_checks WHERE dec_ulid=? ORDER BY check_name",
        (dec_ulid,),
    ):
        check_name = str(row["check_name"])
        if budget is not None:
            budget.consume_operation()
            budget.consume_result(check_name)
        checks.append(check_name)
    return checks


def _verification_commands(
    conn: sqlite3.Connection,
    dec_ulid: str,
    *,
    budget: _ApplicabilityBudget | None = None,
) -> list[str]:
    commands: list[str] = []
    for row in conn.execute(
        "SELECT command FROM decision_verifications WHERE dec_ulid=? ORDER BY command",
        (dec_ulid,),
    ):
        command = str(row["command"])
        if budget is not None:
            budget.consume_operation()
            budget.consume_result(command)
        commands.append(command)
    return commands


def evaluate_decision_applicability(
    conn: sqlite3.Connection,
    paths: Iterable[str],
    *,
    include_proposed: bool = False,
    include_diagnostics: bool = False,
    lifecycle_statuses: Iterable[str] | None = None,
    max_operations: int | None = None,
    max_result_items: int | None = None,
    max_estimated_bytes: int | None = None,
) -> tuple[tuple[str, ...], list[dict[str, Any]]]:
    """Return normalized paths and applicable decisions from one read transaction."""

    selected_paths = normalize_candidate_paths(paths)
    budget = _ApplicabilityBudget(
        paths=selected_paths,
        max_operations=max_operations,
        max_result_items=max_result_items,
        max_estimated_bytes=max_estimated_bytes,
    )
    budget.consume_bytes(*selected_paths)
    statuses = (
        {str(status) for status in lifecycle_statuses}
        if lifecycle_statuses is not None
        else {"accepted", "in_force"}
    )
    if include_proposed and lifecycle_statuses is None:
        statuses.add("proposed")
    decisions: list[dict[str, Any]] = []
    for row in conn.execute("SELECT * FROM decisions ORDER BY human_id"):
        budget.consume_operation()
        status = str(row["status"])
        if status not in statuses:
            continue
        scope = str(row["applicability_scope"] or "manual")
        patterns = _decision_patterns(conn, str(row["dec_ulid"]), budget=budget)
        compiled, warnings = _compile_patterns(patterns, budget=budget)
        matches: list[dict[str, Any]] = []
        exclusions: list[dict[str, Any]] = []

        if scope == "paths":
            for path in selected_paths:
                affected = _first_match(path, compiled["affected"], budget=budget)
                if affected is None:
                    continue
                exempt = _first_match(path, compiled["exempt"], budget=budget)
                generated = _first_match(path, compiled["generated"], budget=budget)
                excluding = exempt or generated
                if excluding is not None:
                    budget.consume_result(path, excluding)
                    exclusions.append(
                        {
                            "path": path,
                            "pattern": excluding,
                            "kind": "exempt" if exempt is not None else "generated",
                        }
                    )
                    continue
                budget.consume_result(path, affected)
                matches.append({"path": path, "pattern": affected})
        elif scope == "repository":
            if not selected_paths:
                budget.consume_result("<repository>")
                matches.append({"path": None, "pattern": None})
            for path in selected_paths:
                exempt = _first_match(path, compiled["exempt"], budget=budget)
                generated = _first_match(path, compiled["generated"], budget=budget)
                excluding = exempt or generated
                if excluding is None:
                    budget.consume_result(path)
                    matches.append({"path": path, "pattern": None})
                else:
                    budget.consume_result(path, excluding)
                    exclusions.append(
                        {
                            "path": path,
                            "pattern": excluding,
                            "kind": "exempt" if exempt is not None else "generated",
                        }
                    )
        elif scope != "manual":
            warnings.append(f"invalid historical applicability_scope {scope!r}")

        invalid_context = bool(warnings) or not bool(row["tier_valid"])
        if not matches and not invalid_context:
            continue
        configured = str(row["enforcement_mode"] or "none")
        effective = configured
        if configured == "required":
            effective = "advisory"
        if invalid_context or status == "proposed":
            effective = "none"
        if not row["tier_valid"]:
            warnings.append(f"historical tier {row['tier']!r} is invalid")

        meta = json_loads(row["meta_json"], {})
        if not isinstance(meta, dict):
            meta = {}
        summary = str(meta.get("decision") or meta.get("context") or "").strip()
        summary_truncated = len(summary) > 500
        item: dict[str, Any] = {
            "id": str(row["human_id"]),
            "title": str(row["title"]),
            "summary": summary[:500],
            "summary_truncated": summary_truncated,
            "status": status,
            "tier": str(row["tier"]),
            "tier_valid": bool(row["tier_valid"]),
            "applicability_scope": scope,
            "configured_enforcement": configured,
            "effective_enforcement": effective,
            "evaluation_status": "not_run",
            "would_block": None,
            "body_sha": str(row["body_sha"]),
            "matches": matches,
            "exclusions": exclusions,
            "required_checks": _checks(conn, str(row["dec_ulid"]), budget=budget),
            "verification_commands": _verification_commands(
                conn, str(row["dec_ulid"]), budget=budget
            ),
            "warnings": sorted(set(warnings)),
        }
        if include_diagnostics:
            item["internal_id"] = str(row["dec_ulid"])
        budget.consume_result(json.dumps(item, sort_keys=True, ensure_ascii=False))
        decisions.append(item)
    return selected_paths, decisions
