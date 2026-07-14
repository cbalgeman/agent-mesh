"""Adapter registry and config-driven loading."""
from __future__ import annotations

import importlib
from dataclasses import replace
from typing import Iterator

from agent_mesh.adapters.base import Adapter, AdapterSpec
from agent_mesh.config import AdapterDeclaration, AgentMeshConfig

__all__ = [
    "Adapter",
    "AdapterRegistry",
    "AdapterSpec",
]


class AdapterRegistry:
    """In-memory adapter registry keyed by config adapter name."""

    def __init__(self) -> None:
        self._adapters: dict[str, Adapter] = {}

    @classmethod
    def from_config(cls, config: AgentMeshConfig) -> "AdapterRegistry":
        registry = cls()
        for declaration in config.adapters.values():
            if declaration.enabled:
                registry.register(load_adapter(declaration))
        return registry

    def register(self, adapter: Adapter) -> None:
        if adapter.name in self._adapters:
            raise ValueError(f"adapter already registered: {adapter.name}")
        self._adapters[adapter.name] = adapter

    def get(self, name: str) -> Adapter:
        try:
            return self._adapters[name]
        except KeyError as exc:
            raise KeyError(f"adapter not registered: {name}") from exc

    def by_domain(self, domain: str) -> list[Adapter]:
        return [adapter for adapter in self._adapters.values() if adapter.domain == domain]

    def __iter__(self) -> Iterator[Adapter]:
        return iter(self._adapters.values())


def load_adapter(declaration: AdapterDeclaration) -> Adapter:
    module_name, _, class_name = declaration.class_path.rpartition(".")
    if not module_name or not class_name:
        raise ValueError(f"invalid adapter class path: {declaration.class_path}")
    module = importlib.import_module(module_name)
    cls = getattr(module, class_name)
    spec = AdapterSpec(
        name=declaration.name,
        domain=declaration.domain,
        privacy_class=declaration.privacy_class,
        options=dict(declaration.options),
    )
    adapter = cls(spec)
    if not isinstance(adapter, Adapter):
        raise TypeError(f"{declaration.class_path} did not produce an Adapter")
    return adapter


def with_options(declaration: AdapterDeclaration, **options: object) -> AdapterDeclaration:
    merged = dict(declaration.options)
    merged.update(options)
    return replace(declaration, options=merged)
