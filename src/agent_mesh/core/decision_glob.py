"""Deterministic, bounded path matching for decision applicability."""

from __future__ import annotations

import re
from dataclasses import dataclass
from functools import lru_cache

MAX_DECISION_PATTERNS = 128
MAX_DECISION_PATTERN_BYTES = 512


class DecisionGlobError(ValueError):
    """Raised when a decision glob is outside the meshglob-v1 grammar."""


@dataclass(frozen=True)
class _CharacterClass:
    negated: bool
    literals: frozenset[str]
    ranges: tuple[tuple[str, str], ...]

    def matches(self, value: str) -> bool:
        contained = value in self.literals or any(
            ord(start) <= ord(value) <= ord(end) for start, end in self.ranges
        )
        return not contained if self.negated else contained


@dataclass(frozen=True)
class _Token:
    kind: str
    value: str | _CharacterClass | None = None


@dataclass(frozen=True)
class MeshGlob:
    source: str
    segments: tuple[tuple[_Token, ...] | None, ...]
    literal_prefix: str

    def matches(self, path: str) -> bool:
        path_segments = tuple(path.split("/"))
        previous = [False] * (len(path_segments) + 1)
        previous[0] = True
        for segment in self.segments:
            current = [False] * (len(path_segments) + 1)
            if segment is None:
                current[0] = previous[0]
                for path_index in range(1, len(path_segments) + 1):
                    current[path_index] = previous[path_index] or current[path_index - 1]
            else:
                for path_index in range(1, len(path_segments) + 1):
                    current[path_index] = previous[path_index - 1] and _segment_matches(
                        segment, path_segments[path_index - 1]
                    )
            previous = current
        return previous[-1]


def normalize_decision_pattern(pattern: str) -> str:
    value = str(pattern)
    if not value:
        raise DecisionGlobError("decision glob must not be empty")
    if "\x00" in value:
        raise DecisionGlobError("decision glob must not contain NUL")
    if len(value.encode("utf-8")) > MAX_DECISION_PATTERN_BYTES:
        raise DecisionGlobError(f"decision glob exceeds {MAX_DECISION_PATTERN_BYTES} UTF-8 bytes")
    if value.startswith("/") or re.match(r"^[A-Za-z]:", value):
        raise DecisionGlobError("decision glob must be repository-relative POSIX text")
    segments = value.split("/")
    if any(segment in {"", ".", ".."} for segment in segments):
        raise DecisionGlobError("decision glob contains an empty, dot, or parent segment")
    if "{" in value or "}" in value:
        raise DecisionGlobError("brace expansion is not supported by meshglob-v1")
    if any(marker in value for marker in ("@(", "+(", "?(", "*(", "!(")):
        raise DecisionGlobError("extglobs are not supported by meshglob-v1")
    return value


@lru_cache(maxsize=4096)
def compile_meshglob(pattern: str) -> MeshGlob:
    source = normalize_decision_pattern(pattern)
    compiled: list[tuple[_Token, ...] | None] = []
    prefix: list[str] = []
    prefix_open = True
    for raw_segment in source.split("/"):
        if raw_segment == "**":
            compiled.append(None)
            prefix_open = False
            continue
        if "**" in raw_segment:
            raise DecisionGlobError("** is valid only as a complete path segment")
        tokens = _parse_segment(raw_segment)
        compiled.append(tokens)
        if prefix_open and all(token.kind == "literal" for token in tokens):
            prefix.append("".join(str(token.value) for token in tokens))
        else:
            prefix_open = False
    return MeshGlob(
        source=source,
        segments=tuple(compiled),
        literal_prefix="/".join(prefix),
    )


def validate_decision_patterns(patterns: list[str] | tuple[str, ...]) -> tuple[str, ...]:
    if len(patterns) > MAX_DECISION_PATTERNS:
        raise DecisionGlobError(f"decision filter list exceeds {MAX_DECISION_PATTERNS} patterns")
    normalized: list[str] = []
    for pattern in patterns:
        normalized.append(compile_meshglob(str(pattern)).source)
    return tuple(normalized)


def meshglob_match(path: str, pattern: str) -> bool:
    return compile_meshglob(pattern).matches(path)


def _parse_segment(segment: str) -> tuple[_Token, ...]:
    tokens: list[_Token] = []
    index = 0
    while index < len(segment):
        value = segment[index]
        if value == "*":
            tokens.append(_Token("star"))
            index += 1
            continue
        if value == "?":
            tokens.append(_Token("any"))
            index += 1
            continue
        if value == "[":
            character_class, index = _parse_character_class(segment, index)
            tokens.append(_Token("class", character_class))
            continue
        tokens.append(_Token("literal", value))
        index += 1
    return tuple(tokens)


def _parse_character_class(segment: str, start: int) -> tuple[_CharacterClass, int]:
    index = start + 1
    negated = False
    if index < len(segment) and segment[index] == "!":
        negated = True
        index += 1
    elif index < len(segment) and segment[index] == "^":
        raise DecisionGlobError("[^...] classes are not supported; use [!...] instead")

    members: list[str] = []
    if index < len(segment) and segment[index] == "]":
        members.append("]")
        index += 1
    while index < len(segment) and segment[index] != "]":
        members.append(segment[index])
        index += 1
    if index >= len(segment):
        raise DecisionGlobError("unclosed character class")
    if not members:
        raise DecisionGlobError("empty character class")

    literals: set[str] = set()
    ranges: list[tuple[str, str]] = []
    member_index = 0
    while member_index < len(members):
        value = members[member_index]
        if (
            member_index + 2 < len(members)
            and value != "-"
            and members[member_index + 1] == "-"
            and members[member_index + 2] != "-"
        ):
            end = members[member_index + 2]
            if ord(value) > ord(end):
                raise DecisionGlobError(f"descending character range: {value}-{end}")
            ranges.append((value, end))
            member_index += 3
            continue
        if value == "-" and member_index not in {0, len(members) - 1}:
            raise DecisionGlobError("- is literal only first or last in a character class")
        literals.add(value)
        member_index += 1

    return (
        _CharacterClass(
            negated=negated,
            literals=frozenset(literals),
            ranges=tuple(ranges),
        ),
        index + 1,
    )


def _segment_matches(tokens: tuple[_Token, ...], value: str) -> bool:
    previous = [False] * (len(value) + 1)
    previous[0] = True
    for token in tokens:
        current = [False] * (len(value) + 1)
        if token.kind == "star":
            current[0] = previous[0]
            for value_index in range(1, len(value) + 1):
                current[value_index] = previous[value_index] or current[value_index - 1]
        else:
            for value_index in range(1, len(value) + 1):
                character = value[value_index - 1]
                if token.kind == "any":
                    token_matches = True
                elif token.kind == "literal":
                    token_matches = token.value == character
                else:
                    character_class = token.value
                    token_matches = isinstance(
                        character_class, _CharacterClass
                    ) and character_class.matches(character)
                current[value_index] = previous[value_index - 1] and token_matches
        previous = current
    return previous[-1]
