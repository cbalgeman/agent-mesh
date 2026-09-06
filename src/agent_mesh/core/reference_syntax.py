"""Current built-in Agent Mesh reference grammar and occurrence extraction."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable

REFERENCE_TOKEN_PATTERN = (
    r"(?:"
    r"D\d+(?:-(?:[SB]\d+|[A-Z]))?(?:-§[A-Za-z0-9._-]+)?|"
    r"REQ-\d{8}T\d{6}Z-[A-Z0-9_-]+-\d{5}|"
    r"RES-\d{8}T\d{6}Z-[A-Z0-9_-]+-\d{5}|"
    r"AI-\d{8}-\d{2,}|"
    r"FBK-[A-Za-z0-9][\w-]*|DI-[A-Za-z0-9][\w-]*|J-[A-Za-z0-9][\w-]*|"
    r"BKL-[A-Za-z0-9][\w-]*|IMP-[A-Za-z0-9][\w-]*"
    r")"
)
REFERENCE_RE = re.compile(
    rf"(?<![A-Za-z0-9_])(?P<reference>{REFERENCE_TOKEN_PATTERN})(?![A-Za-z0-9_])"
)
REFERENCE_TOKEN_RE = re.compile(rf"^{REFERENCE_TOKEN_PATTERN}$")


@dataclass(frozen=True)
class ReferenceOccurrence:
    """One reference and the source location that caused it to be resolved."""

    reference: str
    source: str
    source_kind: str
    line: int | None = None
    column: int | None = None

    def as_dict(self, *, resolution_index: int) -> dict[str, Any]:
        return {
            "resolution_index": resolution_index,
            "source": self.source,
            "source_kind": self.source_kind,
            "line": self.line,
            "column": self.column,
        }


def reference_kind(reference: str) -> str:
    if reference.startswith("D"):
        return "decision"
    if reference.startswith("REQ-"):
        return "request"
    if reference.startswith("RES-"):
        return "response"
    if reference.startswith("BKL-"):
        return "backlog"
    if reference.startswith("AI-"):
        return "agent_instance"
    if reference.startswith("FBK-"):
        return "feedback"
    if reference.startswith("DI-"):
        return "design_intent"
    if reference.startswith("J-"):
        return "journey"
    if reference.startswith("IMP-"):
        return "improvement"
    return "unknown"


def explicit_reference_occurrences(
    references: Iterable[str],
    *,
    max_occurrences: int | None = None,
) -> list[ReferenceOccurrence]:
    occurrences: list[ReferenceOccurrence] = []
    for reference in references:
        token = reference.strip()
        if REFERENCE_TOKEN_RE.fullmatch(token) is None:
            raise ValueError(f"unsupported reference token: {reference!r}")
        if max_occurrences is None or len(occurrences) < max_occurrences:
            occurrences.append(
                ReferenceOccurrence(reference=token, source="argv", source_kind="explicit")
            )
    return occurrences


def extract_reference_occurrences(
    text: str,
    *,
    source: str,
    source_kind: str,
    max_occurrences: int | None = None,
) -> list[ReferenceOccurrence]:
    occurrences: list[ReferenceOccurrence] = []
    for line_number, line in _reference_lines(text):
        for match in REFERENCE_RE.finditer(line):
            if max_occurrences is not None and len(occurrences) >= max_occurrences:
                return occurrences
            occurrences.append(
                ReferenceOccurrence(
                    reference=match.group("reference"),
                    source=source,
                    source_kind=source_kind,
                    line=line_number,
                    column=match.start("reference") + 1,
                )
            )
    return occurrences


def _reference_lines(text: str) -> Iterable[tuple[int, str]]:
    """Yield common newline-delimited lines without materializing a line list."""

    start = 0
    cursor = 0
    line_number = 1
    while cursor < len(text):
        character = text[cursor]
        if character != "\r" and character != "\n":
            cursor += 1
            continue
        yield line_number, text[start:cursor]
        if character == "\r" and text[cursor : cursor + 2] == "\r\n":
            cursor += 1
        cursor += 1
        start = cursor
        line_number += 1
    if start < len(text):
        yield line_number, text[start:]
