"""Domain-neutral adapter contracts for project-specific integrations."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class AdapterSpec:
    name: str
    domain: str
    privacy_class: str
    options: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MessageBlock:
    message_id: str
    kind: str
    thread_id: str
    body: str
    body_sha: str
    source_uri: str
    source_path: str | None = None
    line_start: int | None = None
    line_end: int | None = None


@dataclass(frozen=True)
class ExtractedRef:
    ref_type: str
    ref_value: str
    source_field: str
    privacy_class: str


class Adapter(ABC):
    """Base adapter shape shared by all current and future domains."""

    def __init__(self, spec: AdapterSpec) -> None:
        self.spec = spec

    @property
    def name(self) -> str:
        return self.spec.name

    @property
    def domain(self) -> str:
        return self.spec.domain

    @property
    def privacy_class(self) -> str:
        return self.spec.privacy_class

    def healthcheck(self) -> bool:
        return True


class LookupAdapter(Adapter):
    """Lookup adapter contract for addressable entities in any domain."""

    @abstractmethod
    def lookup(self, identifier: str) -> Any | None:
        raise NotImplementedError


class RefExtractionAdapter(Adapter):
    """Reference extraction contract for domain-owned text or payload fields."""

    @abstractmethod
    def extract(self, text: str, *, source_field: str = "body") -> list[ExtractedRef]:
        raise NotImplementedError
