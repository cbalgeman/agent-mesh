"""Runtime-family driver registry for managed dispatch.

Agent Mesh ships and maintains the official driver set.  The registry keeps
provider-specific preflight and construction outside the CLI and dispatch core.
An explicit project-local V1 driver may enter through its reserved namespace;
it remains visibly separate from the official built-in set.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Protocol

from agent_mesh.config import LOCAL_RUNTIME_DRIVER_SOURCE, RuntimeProfile

from .adapters import AgentRuntimeAdapter

if TYPE_CHECKING:
    from agent_mesh.core.events import _CapabilityAppendReceipt


MANDATORY_RUNTIME_DRIVER_CHECKS = frozenset(
    {
        "version",
        "model",
        "authentication_billing",
        "capabilities",
        "permission_mode",
    }
)
GENERIC_RUNTIME_PREFLIGHT_CHECKS = frozenset(
    {
        "enabled",
        "adapter",
        "driver_contract",
        "executable",
        "credential_isolation",
        "repository_scope",
        "driver_evidence",
    }
)
_HOST_PREFLIGHT_PROOF = object()


@dataclass(frozen=True)
class RuntimePreflightCheck:
    name: str
    passed: bool
    detail: str
    evidence_class: str = "unverified"
    trust_source: str = "unknown"
    probe_version: str = "agent-mesh.effective-capability.v1"

    @property
    def blocking_eligible(self) -> bool:
        return self.passed and self.evidence_class in {
            "generic_host",
            "built_in_driver",
        }


@dataclass(frozen=True)
class RuntimePreflightResult:
    profile_name: str
    target: str
    binary_path: Path | None
    checks: tuple[RuntimePreflightCheck, ...]
    observed_utc: str = ""
    valid_until_utc: str = ""
    drift_inputs_digest: str = ""
    profile_digest: str = ""
    _host_preflight_proof: object | None = field(default=None, repr=False, compare=False)

    @property
    def passed(self) -> bool:
        return bool(self.checks) and all(check.passed for check in self.checks)

    def failure_summary(self) -> str:
        failures = [check.name for check in self.checks if not check.passed]
        return ", ".join(failures) if failures else "none"

    @property
    def blocking_eligible(self) -> bool:
        """Whether a blocking dispatch/assurance gate may rely on this proof."""

        if not self.passed:
            return False
        blocking_checks = set(MANDATORY_RUNTIME_DRIVER_CHECKS) | {"effective_capabilities"}
        required = {check.name: check for check in self.checks if check.name in blocking_checks}
        return set(required) == blocking_checks and all(
            check.blocking_eligible for check in required.values()
        )

    def capability_receipt(self, required_capabilities: tuple[str, ...]) -> dict[str, object]:
        """Return a privacy-safe, versioned receipt for the effective capability layer."""

        declared = next((item for item in self.checks if item.name == "capabilities"), None)
        effective = next(
            (item for item in self.checks if item.name == "effective_capabilities"), None
        )
        results = [
            _capability_result(self.checks, declared, effective, capability)
            for capability in required_capabilities
        ]
        receipt: dict[str, object] = {
            "schema": "agent-mesh.capability-receipt.v1",
            "observed_utc": self.observed_utc,
            "valid_until_utc": self.valid_until_utc,
            "drift_inputs_digest": self.drift_inputs_digest,
            "profile_digest": self.profile_digest,
            "results": results,
        }
        encoded = json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode("utf-8")
        receipt["receipt_digest"] = hashlib.sha256(encoded).hexdigest()
        return receipt

    def mint_capability_append_authority(
        self,
        *,
        events_path: str | Path,
        required_capabilities: tuple[str, ...],
    ) -> tuple[dict[str, object], "_CapabilityAppendReceipt"]:
        """Mint one append authority only for a result produced by host preflight."""

        if self._host_preflight_proof is not _HOST_PREFLIGHT_PROOF or not self.passed:
            raise RuntimeError("host-verified runtime preflight is required")
        receipt = self.capability_receipt(required_capabilities)
        from agent_mesh.core.events import _prepare_capability_append

        authority = _prepare_capability_append(
            events_path=events_path,
            receipt=receipt,
        )
        return receipt, authority

    def discard_capability_append_authority(self, authority: "_CapabilityAppendReceipt") -> None:
        """Revoke an unused one-shot append authority; consumed authorities are a no-op."""

        from agent_mesh.core.events import _discard_capability_append_receipt

        _discard_capability_append_receipt(authority)


def _capability_result(
    checks: tuple[RuntimePreflightCheck, ...],
    declared: RuntimePreflightCheck | None,
    aggregate_effective: RuntimePreflightCheck | None,
    capability: str,
) -> dict[str, object]:
    effective = next(
        (item for item in checks if item.name == f"effective_capability:{capability}"),
        aggregate_effective,
    )
    return {
        "capability": capability,
        "declared": bool(declared and declared.passed),
        "effective": bool(effective and effective.passed),
        "evidence_class": (effective.evidence_class if effective is not None else "unverified"),
        "trust_source": effective.trust_source if effective is not None else "unknown",
        "probe_version": (
            effective.probe_version
            if effective is not None
            else "agent-mesh.effective-capability.v1"
        ),
    }


CommandRunner = Callable[..., subprocess.CompletedProcess[str]]
RuntimeLifecycleMode = tuple[str, bool, bool, str]


@dataclass(frozen=True)
class RuntimeDriverContext:
    """Project facts an owned driver may place in its provider-neutral spec."""

    events_path: Path
    project_scope: str


class RuntimePreflightHook(Protocol):
    def __call__(
        self,
        profile: RuntimeProfile,
        *,
        binary_path: Path,
        project_root: Path,
        environment: dict[str, str],
        runner: CommandRunner,
    ) -> list[RuntimePreflightCheck]: ...


class RuntimeFactoryHook(Protocol):
    def __call__(
        self,
        profile: RuntimeProfile,
        *,
        binary_path: Path,
        context: RuntimeDriverContext,
    ) -> AgentRuntimeAdapter: ...


@dataclass(frozen=True)
class RuntimeAdapterDriver:
    """One Agent Mesh-owned runtime-family integration.

    ``adapter_id`` names a runtime protocol family, not a model.  Models remain
    opaque profile values proved by the driver's runtime-catalog preflight.
    """

    adapter_id: str
    providers: frozenset[str]
    preflight: RuntimePreflightHook
    factory: RuntimeFactoryHook
    ownership: str = "agent-mesh-built-in"
    model_policy: str = "runtime-catalog"
    lifecycle_modes: frozenset[RuntimeLifecycleMode] = frozenset(
        {("none", False, False, "process")}
    )
    additional_required_checks: frozenset[str] = frozenset()
    resumable_check: str | None = None

    def supports(self, profile: RuntimeProfile) -> bool:
        return profile.adapter == self.adapter_id and profile.provider in self.providers

    def supports_lifecycle(self, profile: RuntimeProfile) -> bool:
        return (
            profile.session_identity_mode,
            profile.resumable,
            profile.concurrent_attachment,
            profile.terminal_observation,
        ) in self.lifecycle_modes


class RuntimeAdapterNotFound(LookupError):
    """The configured provider/adapter pair has no selected live driver."""


class RuntimeAdapterRegistry:
    """Explicit registry of built-in drivers plus one validated local selection."""

    def __init__(self) -> None:
        self._drivers: dict[str, RuntimeAdapterDriver] = {}

    def register(self, driver: RuntimeAdapterDriver) -> None:
        if driver.ownership != "agent-mesh-built-in":
            raise ValueError("runtime drivers must be owned and shipped by Agent Mesh")
        if driver.adapter_id.startswith("local:"):
            raise ValueError("built-in runtime drivers cannot use the local: namespace")
        self._register_validated(driver)

    def register_project_local(self, driver: RuntimeAdapterDriver) -> None:
        """Register one manifest-backed local driver built by the trusted V1 host."""

        if driver.ownership != LOCAL_RUNTIME_DRIVER_SOURCE:
            raise ValueError("project-local runtime driver ownership is invalid")
        if not driver.adapter_id.startswith("local:"):
            raise ValueError("project-local runtime drivers require the local: namespace")
        if driver.lifecycle_modes != frozenset({("none", False, False, "process")}):
            raise ValueError("project-local V1 drivers are one-shot process-observed only")
        if driver.resumable_check or driver.additional_required_checks:
            raise ValueError("project-local V1 drivers cannot extend continuity or check names")
        self._register_validated(driver)

    def _register_validated(self, driver: RuntimeAdapterDriver) -> None:
        if not driver.adapter_id or not driver.providers:
            raise ValueError("runtime driver requires an adapter id and provider set")
        if driver.model_policy != "runtime-catalog":
            raise ValueError("runtime drivers must prove models from the runtime catalog")
        if not driver.lifecycle_modes:
            raise ValueError("runtime driver requires at least one complete lifecycle mode")
        for mode in driver.lifecycle_modes:
            if len(mode) != 4 or mode[0] not in {"none", "exact"}:
                raise ValueError("runtime driver lifecycle mode is invalid")
            if not isinstance(mode[1], bool) or not isinstance(mode[2], bool):
                raise ValueError("runtime driver lifecycle flags must be booleans")
            if mode[3] not in {"process", "provider", "none"}:
                raise ValueError("runtime driver terminal observation is invalid")
            if mode[1] and mode[0] != "exact":
                raise ValueError("resumable runtime lifecycle requires exact identity")
        reserved = MANDATORY_RUNTIME_DRIVER_CHECKS | GENERIC_RUNTIME_PREFLIGHT_CHECKS
        if (
            not all(driver.additional_required_checks)
            or driver.additional_required_checks & reserved
        ):
            raise ValueError("additional runtime checks must be distinct provider-specific names")
        has_resumable_mode = any(mode[1] for mode in driver.lifecycle_modes)
        if has_resumable_mode and not driver.resumable_check:
            raise ValueError("resumable runtime driver requires a distinct continuity check")
        if driver.resumable_check and (
            driver.resumable_check in reserved
            or driver.resumable_check in driver.additional_required_checks
        ):
            raise ValueError("resumable runtime check must be distinct from all other checks")
        if driver.adapter_id in self._drivers:
            raise ValueError(f"runtime driver already registered: {driver.adapter_id}")
        self._drivers[driver.adapter_id] = driver

    def resolve(self, profile: RuntimeProfile) -> RuntimeAdapterDriver:
        driver = self._drivers.get(profile.adapter)
        if driver is None or not driver.supports(profile):
            raise RuntimeAdapterNotFound(
                f"{profile.provider}/{profile.adapter} has no configured live driver"
            )
        return driver

    def adapter_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self._drivers))


def built_in_runtime_registry() -> RuntimeAdapterRegistry:
    """Return the complete driver set shipped in this Agent Mesh release."""

    from .codex_driver import CODEX_CLI_DRIVER

    registry = RuntimeAdapterRegistry()
    registry.register(CODEX_CLI_DRIVER)
    return registry


def runtime_registry_for_profile(
    profile: RuntimeProfile,
    *,
    project_root: Path,
    registry: RuntimeAdapterRegistry | None = None,
) -> RuntimeAdapterRegistry:
    """Return built-ins plus the profile's one explicitly selected local driver."""

    drivers = registry or built_in_runtime_registry()
    if profile.driver_source == LOCAL_RUNTIME_DRIVER_SOURCE:
        try:
            drivers.resolve(profile)
        except RuntimeAdapterNotFound:
            pass
        else:
            return drivers
        from .local_driver import project_local_runtime_driver

        drivers.register_project_local(
            project_local_runtime_driver(profile, project_root=project_root)
        )
    return drivers
