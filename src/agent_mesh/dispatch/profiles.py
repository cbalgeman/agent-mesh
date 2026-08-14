"""Fail-closed preflight for project-local executable runtime profiles."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from agent_mesh.config import RuntimeProfile

from .runtime import SUBSCRIPTION_API_CREDENTIALS, sanitized_child_environment


@dataclass(frozen=True)
class RuntimePreflightCheck:
    name: str
    passed: bool
    detail: str


@dataclass(frozen=True)
class RuntimePreflightResult:
    profile_name: str
    target: str
    binary_path: Path | None
    checks: tuple[RuntimePreflightCheck, ...]

    @property
    def passed(self) -> bool:
        return bool(self.checks) and all(check.passed for check in self.checks)

    def failure_summary(self) -> str:
        failures = [check.name for check in self.checks if not check.passed]
        return ", ".join(failures) if failures else "none"


CommandRunner = Callable[..., subprocess.CompletedProcess[str]]


def preflight_runtime_profile(
    profile: RuntimeProfile,
    *,
    project_root: str | Path,
    environ: dict[str, str] | None = None,
    runner: CommandRunner | None = None,
) -> RuntimePreflightResult:
    """Verify a profile without issuing a model prompt or persisting provider output."""

    run = runner or subprocess.run
    root = Path(project_root).resolve()
    source_environment = dict(os.environ if environ is None else environ)
    child_environment = sanitized_child_environment(
        profile.credential_denylist,
        source=source_environment,
    )
    checks: list[RuntimePreflightCheck] = [
        RuntimePreflightCheck(
            "enabled",
            profile.enabled,
            "profile is explicitly enabled" if profile.enabled else "profile is disabled",
        )
    ]

    adapter_supported = profile.adapter == "codex-cli" and profile.provider == "openai"
    checks.append(
        RuntimePreflightCheck(
            "adapter",
            adapter_supported,
            (
                "openai/codex-cli has a machine-verifiable live adapter"
                if adapter_supported
                else (
                    f"{profile.provider}/{profile.adapter} has no machine-verifiable live adapter"
                )
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

    denied = set(SUBSCRIPTION_API_CREDENTIALS) | set(profile.credential_denylist)
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

    if not adapter_supported or not executable_ok or binary_path is None:
        checks.extend(
            [
                RuntimePreflightCheck("version", False, "adapter or executable unavailable"),
                RuntimePreflightCheck("model", False, "adapter or executable unavailable"),
                RuntimePreflightCheck(
                    "authentication_billing", False, "adapter or executable unavailable"
                ),
                RuntimePreflightCheck("capabilities", False, "adapter or executable unavailable"),
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
        )

    checks.extend(
        _codex_preflight_checks(
            profile,
            binary_path=binary_path,
            project_root=root,
            environment=child_environment,
            runner=run,
        )
    )
    return RuntimePreflightResult(
        profile_name=profile.name,
        target=profile.target,
        binary_path=binary_path,
        checks=tuple(checks),
    )


def _codex_preflight_checks(
    profile: RuntimeProfile,
    *,
    binary_path: Path,
    project_root: Path,
    environment: dict[str, str],
    runner: CommandRunner,
) -> list[RuntimePreflightCheck]:
    version = _run(
        runner,
        [str(binary_path), "--version"],
        cwd=project_root,
        environment=environment,
    )
    observed_version = _codex_version(version)
    version_ok = observed_version == profile.version

    models = _run(
        runner,
        [str(binary_path), "debug", "models"],
        cwd=project_root,
        environment=environment,
    )
    model_slugs = _codex_model_slugs(models)
    model_ok = profile.model in model_slugs

    auth = _run(
        runner,
        [str(binary_path), "login", "status"],
        cwd=project_root,
        environment=environment,
    )
    auth_lines = set()
    if auth is not None:
        auth_lines = {line.strip() for line in (auth.stdout + "\n" + auth.stderr).splitlines()}
    auth_ok = (
        profile.authentication_mode == "chatgpt"
        and profile.billing_mode == "subscription"
        and auth is not None
        and auth.returncode == 0
        and "Logged in using ChatGPT" in auth_lines
    )

    general_help = _run(
        runner,
        [str(binary_path), "--help"],
        cwd=project_root,
        environment=environment,
    )
    exec_help = _run(
        runner,
        [str(binary_path), "exec", "--help"],
        cwd=project_root,
        environment=environment,
    )
    general_text = (
        general_help.stdout if general_help is not None and general_help.returncode == 0 else ""
    )
    exec_text = exec_help.stdout if exec_help is not None and exec_help.returncode == 0 else ""
    supported_capabilities = set()
    if "--cd <DIR>" in general_text and "--skip-git-repo-check" in exec_text:
        supported_capabilities.add("repository")
    if "exec" in general_text and "--sandbox <SANDBOX_MODE>" in exec_text:
        supported_capabilities.add("tools")
    if "--search" in general_text:
        supported_capabilities.add("network")
    missing_capabilities = sorted(set(profile.required_capabilities) - supported_capabilities)
    capabilities_ok = not missing_capabilities

    permission_ok = (
        profile.permission_mode in {"read-only", "workspace-write"}
        and profile.permission_mode in exec_text
    )
    return [
        RuntimePreflightCheck(
            "version",
            version_ok,
            (
                f"verified codex-cli {profile.version}"
                if version_ok
                else f"expected {profile.version}; observed {observed_version or 'unavailable'}"
            ),
        ),
        RuntimePreflightCheck(
            "model",
            model_ok,
            (
                f"model catalog contains {profile.model}"
                if model_ok
                else f"model catalog does not contain {profile.model}"
            ),
        ),
        RuntimePreflightCheck(
            "authentication_billing",
            auth_ok,
            (
                "verified ChatGPT subscription login with API credentials stripped"
                if auth_ok
                else "could not verify the configured ChatGPT subscription boundary"
            ),
        ),
        RuntimePreflightCheck(
            "capabilities",
            capabilities_ok,
            (
                f"verified {', '.join(profile.required_capabilities)}"
                if capabilities_ok
                else f"missing capability proof: {', '.join(missing_capabilities)}"
            ),
        ),
        RuntimePreflightCheck(
            "permission_mode",
            permission_ok,
            (
                f"verified {profile.permission_mode} sandbox"
                if permission_ok
                else f"permission mode {profile.permission_mode!r} is not supported"
            ),
        ),
    ]


def _resolve_binary(binary: str, *, path: str | None) -> Path | None:
    candidate = Path(binary).expanduser()
    if candidate.is_absolute():
        return candidate.resolve()
    resolved = shutil.which(binary, path=path)
    return Path(resolved).resolve() if resolved else None


def _run(
    runner: CommandRunner,
    argv: list[str],
    *,
    cwd: Path,
    environment: dict[str, str],
) -> subprocess.CompletedProcess[str] | None:
    try:
        return runner(
            argv,
            cwd=cwd,
            env=environment,
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None


def _codex_version(result: subprocess.CompletedProcess[str] | None) -> str:
    if result is None or result.returncode != 0:
        return ""
    prefix = "codex-cli "
    output = result.stdout.strip()
    return output[len(prefix) :] if output.startswith(prefix) else ""


def _codex_model_slugs(result: subprocess.CompletedProcess[str] | None) -> set[str]:
    if result is None or result.returncode != 0:
        return set()
    try:
        payload = json.loads(result.stdout)
        models = payload.get("models", [])
    except (AttributeError, json.JSONDecodeError, TypeError):
        return set()
    if not isinstance(models, list):
        return set()
    return {
        str(model["slug"])
        for model in models
        if isinstance(model, dict) and isinstance(model.get("slug"), str)
    }
