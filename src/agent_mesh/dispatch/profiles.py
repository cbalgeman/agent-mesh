"""Fail-closed preflight for project-local executable runtime profiles."""

from __future__ import annotations

import os
import hashlib
import json
import shutil
import subprocess
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

from agent_mesh.config import RuntimeProfile

from .runtime import (
    PROVIDER_SESSION_ENVIRONMENT_VARIABLES,
    SUBSCRIPTION_API_CREDENTIALS,
    codex_child_environment,
)
from .runtime_registry import (
    CommandRunner,
    GENERIC_RUNTIME_PREFLIGHT_CHECKS,
    MANDATORY_RUNTIME_DRIVER_CHECKS,
    RuntimeAdapterNotFound,
    RuntimeAdapterRegistry,
    RuntimePreflightCheck,
    RuntimePreflightResult,
    _HOST_PREFLIGHT_PROOF,
    runtime_registry_for_profile,
)
from .process_io import run_bounded_process


RUNTIME_PREFLIGHT_TIMEOUT_SECONDS = 15.0
MAX_RUNTIME_PREFLIGHT_STDOUT_BYTES = 1024 * 1024
MAX_RUNTIME_PREFLIGHT_STDERR_BYTES = 64 * 1024
RUNTIME_CAPABILITY_RECEIPT_VALIDITY_SECONDS = 300


def runtime_profile_revision(profile: RuntimeProfile) -> dict[str, object]:
    """Return the complete privacy-safe routing authority frozen by dispatch.v1."""

    authority: dict[str, object] = {
        "name": profile.name,
        "target": profile.target,
        "provider": profile.provider,
        "adapter": profile.adapter,
        "version": profile.version,
        "model": profile.model,
        "durable_role": profile.durable_role,
        "role": profile.role,
        "permission_mode": profile.permission_mode,
        "repository_scope": profile.repository_scope,
        "required_capabilities": list(profile.required_capabilities),
        "authentication_mode": profile.authentication_mode,
        "billing_mode": profile.billing_mode,
        "adapter_trust": profile.adapter_trust,
        "session_identity_mode": profile.session_identity_mode,
        "resumable": profile.resumable,
        "concurrent_attachment": profile.concurrent_attachment,
        "terminal_observation": profile.terminal_observation,
        "driver_source": profile.driver_source,
        "driver_protocol": profile.driver_protocol,
        "driver_manifest_sha256": profile.driver_manifest_sha256,
    }
    encoded = json.dumps(authority, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return {**authority, "digest": hashlib.sha256(encoded).hexdigest()}


def preflight_runtime_profile(
    profile: RuntimeProfile,
    *,
    project_root: str | Path,
    environ: dict[str, str] | None = None,
    runner: CommandRunner | None = None,
    registry: RuntimeAdapterRegistry | None = None,
) -> RuntimePreflightResult:
    """Verify a profile without issuing a model prompt or persisting provider output."""

    deadline = time.monotonic() + RUNTIME_PREFLIGHT_TIMEOUT_SECONDS
    run = runner or _bounded_preflight_runner(deadline)
    root = Path(project_root).resolve()
    observed = datetime.now(UTC).replace(microsecond=0)
    source_environment = dict(os.environ if environ is None else environ)
    child_environment = codex_child_environment(
        profile.credential_denylist,
        binary=profile.binary,
        source=source_environment,
    )
    drivers = runtime_registry_for_profile(
        profile,
        project_root=root,
        registry=registry,
    )
    driver = None
    try:
        driver = drivers.resolve(profile)
    except RuntimeAdapterNotFound:
        pass

    checks: list[RuntimePreflightCheck] = [
        RuntimePreflightCheck(
            "enabled",
            profile.enabled,
            "profile is explicitly enabled" if profile.enabled else "profile is disabled",
        )
    ]

    adapter_supported = driver is not None
    if driver is None:
        adapter_detail = f"{profile.provider}/{profile.adapter} has no configured live driver"
    elif driver.ownership == "project-local":
        adapter_detail = (
            f"{profile.provider}/{profile.adapter} uses the explicitly selected "
            "project-local runtime driver"
        )
    else:
        adapter_detail = (
            f"{profile.provider}/{profile.adapter} uses the Agent Mesh-owned runtime-family driver"
        )
    checks.append(
        RuntimePreflightCheck(
            "adapter",
            adapter_supported,
            adapter_detail,
        )
    )
    driver_contract_ok = False
    if driver is not None:
        driver_contract_ok = driver.supports_lifecycle(profile)
        checks.append(
            RuntimePreflightCheck(
                "driver_contract",
                driver_contract_ok,
                (
                    "configured lifecycle combination is supported"
                    if driver_contract_ok
                    else f"configured lifecycle combination is not supported by {driver.adapter_id}"
                ),
            )
        )

    binary_path = _resolve_binary(profile.binary, path=source_environment.get("PATH"))
    executable_ok = (
        binary_path is not None and binary_path.is_file() and os.access(binary_path, os.X_OK)
    )
    checks.append(
        RuntimePreflightCheck(
            "executable",
            executable_ok,
            (
                f"resolved executable {binary_path}"
                if executable_ok
                else f"could not resolve executable {profile.binary!r}"
            ),
        )
    )

    denied = (
        set(PROVIDER_SESSION_ENVIRONMENT_VARIABLES)
        | set(SUBSCRIPTION_API_CREDENTIALS)
        | set(profile.credential_denylist)
    )
    leaked = sorted(variable for variable in denied if variable in child_environment)
    checks.append(
        RuntimePreflightCheck(
            "credential_isolation",
            not leaked,
            (
                f"stripped {len(denied)} denied credential variables"
                if not leaked
                else "denied credential variables remain in the child environment"
            ),
        )
    )

    repository_ok = (
        profile.repository_scope == "project" and root.is_dir() and (root / ".agent-mesh").is_dir()
    )
    checks.append(
        RuntimePreflightCheck(
            "repository_scope",
            repository_ok,
            (
                f"launch root is the Agent Mesh project {root}"
                if repository_ok
                else "repository scope is not a valid Agent Mesh project root"
            ),
        )
    )

    if (
        not adapter_supported
        or driver is None
        or not driver_contract_ok
        or not executable_ok
        or binary_path is None
    ):
        checks.extend(
            [
                RuntimePreflightCheck("version", False, "adapter or executable unavailable"),
                RuntimePreflightCheck("model", False, "adapter or executable unavailable"),
                RuntimePreflightCheck(
                    "authentication_billing", False, "adapter or executable unavailable"
                ),
                RuntimePreflightCheck("capabilities", False, "adapter or executable unavailable"),
                RuntimePreflightCheck(
                    "effective_capabilities", False, "adapter or executable unavailable"
                ),
                RuntimePreflightCheck(
                    "permission_mode", False, "adapter or executable unavailable"
                ),
            ]
        )
        return RuntimePreflightResult(
            profile_name=profile.name,
            target=profile.target,
            binary_path=binary_path,
            checks=tuple(checks),
            _host_preflight_proof=_HOST_PREFLIGHT_PROOF,
            **_receipt_metadata(profile, binary_path, observed),
        )

    assert driver is not None
    hook_failure = ""
    try:
        driver_checks = driver.preflight(
            profile,
            binary_path=binary_path,
            project_root=root,
            environment=child_environment,
            runner=run,
        )
    except Exception as exc:
        # A provider/runtime failure is a failed proof, not an authorization to
        # bypass preflight or surface provider-controlled exception text.
        driver_checks = []
        hook_failure = type(exc).__name__
    if driver.ownership == "agent-mesh-built-in" and not any(
        check.name == "effective_capabilities" for check in driver_checks
    ):
        declared = next(
            (check for check in driver_checks if check.name == "capabilities"),
            None,
        )
        if declared is not None:
            # Built-in drivers own their probe semantics.  Drivers written for
            # the pre-D014 interface may return one positive capability proof;
            # adapt that proof into the explicitly named effective layer.  A
            # project-local/self-attested driver never receives this upgrade.
            driver_checks.append(
                RuntimePreflightCheck(
                    "effective_capabilities",
                    declared.passed,
                    "built-in driver effective proof: " + declared.detail,
                )
            )
    driver_names = [check.name for check in driver_checks]
    required_driver_checks = set(MANDATORY_RUNTIME_DRIVER_CHECKS) | set(
        driver.additional_required_checks
    )
    if profile.resumable:
        if driver.resumable_check:
            required_driver_checks.add(driver.resumable_check)
        else:
            required_driver_checks.add("continuity")
    missing_driver_checks = sorted(required_driver_checks - set(driver_names))
    duplicate_driver_checks = sorted(
        name for name in set(driver_names) if driver_names.count(name) != 1
    )
    reserved_driver_checks = sorted(set(driver_names) & set(GENERIC_RUNTIME_PREFLIGHT_CHECKS))
    evidence_ok = not (
        hook_failure or missing_driver_checks or duplicate_driver_checks or reserved_driver_checks
    )
    if evidence_ok:
        evidence_detail = "driver supplied each mandatory preflight proof exactly once"
    else:
        issues = []
        if hook_failure:
            issues.append(f"hook_failed={hook_failure}")
        if missing_driver_checks:
            issues.append(f"missing={','.join(missing_driver_checks)}")
        if duplicate_driver_checks:
            issues.append(f"duplicate={','.join(duplicate_driver_checks)}")
        if reserved_driver_checks:
            issues.append(f"reserved={','.join(reserved_driver_checks)}")
        evidence_detail = "invalid driver preflight evidence: " + "; ".join(issues)
    checks.append(RuntimePreflightCheck("driver_evidence", evidence_ok, evidence_detail))
    generic_checks = [
        replace(
            check,
            evidence_class="generic_host",
            trust_source="agent-mesh-host",
        )
        for check in checks
    ]
    driver_evidence_class = (
        "built_in_driver" if driver.ownership == "agent-mesh-built-in" else "self_attested"
    )
    classified_driver_checks = [
        replace(
            check,
            evidence_class=driver_evidence_class,
            trust_source=driver.ownership,
        )
        for check in driver_checks
    ]
    return RuntimePreflightResult(
        profile_name=profile.name,
        target=profile.target,
        binary_path=binary_path,
        checks=tuple([*generic_checks, *classified_driver_checks]),
        _host_preflight_proof=_HOST_PREFLIGHT_PROOF,
        **_receipt_metadata(profile, binary_path, observed),
    )


def _receipt_metadata(
    profile: RuntimeProfile,
    binary_path: Path | None,
    observed: datetime,
) -> dict[str, str]:
    profile_digest = str(runtime_profile_revision(profile)["digest"])
    binary_identity: dict[str, int | str] = {"state": "unavailable"}
    if binary_path is not None:
        try:
            stat_result = binary_path.stat()
        except OSError:
            pass
        else:
            binary_identity = {
                "state": "available",
                "device": int(stat_result.st_dev),
                "inode": int(stat_result.st_ino),
                "size": int(stat_result.st_size),
                "mtime_ns": int(stat_result.st_mtime_ns),
            }
    drift_payload = {
        "profile_digest": profile_digest,
        "binary_identity": binary_identity,
    }
    drift_digest = hashlib.sha256(
        json.dumps(drift_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return {
        "observed_utc": observed.isoformat().replace("+00:00", "Z"),
        "valid_until_utc": (
            observed + timedelta(seconds=RUNTIME_CAPABILITY_RECEIPT_VALIDITY_SECONDS)
        )
        .isoformat()
        .replace("+00:00", "Z"),
        "drift_inputs_digest": drift_digest,
        "profile_digest": profile_digest,
    }


def _bounded_preflight_runner(deadline: float):
    """Adapt the shared bounded subprocess transport to the driver runner protocol."""

    def run(argv, **kwargs):
        remaining = max(0.0, deadline - time.monotonic())
        if remaining <= 0:
            return subprocess.CompletedProcess(argv, 124, stdout="", stderr="")
        configured_timeout = float(kwargs.get("timeout", remaining))
        result = run_bounded_process(
            list(argv),
            cwd=Path(kwargs["cwd"]),
            environment=dict(kwargs["env"]),
            stdin=b"",
            timeout_seconds=min(remaining, configured_timeout),
            stdout_limit=MAX_RUNTIME_PREFLIGHT_STDOUT_BYTES,
            stderr_limit=MAX_RUNTIME_PREFLIGHT_STDERR_BYTES,
        )
        try:
            stdout = result.stdout.decode("utf-8", errors="strict")
            stderr = result.stderr.decode("utf-8", errors="strict")
        except UnicodeDecodeError:
            return subprocess.CompletedProcess(argv, 125, stdout="", stderr="")
        return subprocess.CompletedProcess(
            argv,
            result.returncode if result.status == "completed" else 124,
            stdout=stdout,
            stderr=stderr,
        )

    return run


def _resolve_binary(binary: str, *, path: str | None) -> Path | None:
    candidate = Path(binary).expanduser()
    if candidate.is_absolute():
        return candidate.resolve()
    resolved = shutil.which(binary, path=path)
    return Path(resolved).resolve() if resolved else None
