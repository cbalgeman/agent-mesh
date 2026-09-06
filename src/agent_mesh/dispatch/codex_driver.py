"""Agent Mesh-owned Codex runtime-family driver registration."""

from __future__ import annotations

import json
import secrets
import stat
import subprocess
from pathlib import Path

from agent_mesh.adapters.base import AdapterSpec
from agent_mesh.config import RuntimeProfile

from .adapters import AgentRuntimeAdapter
from .runtime import SUPPORTED_CODEX_APP_SERVER_VERSIONS, CodexCliRuntimeAdapter
from .runtime_registry import (
    CommandRunner,
    RuntimeAdapterDriver,
    RuntimeDriverContext,
    RuntimePreflightCheck,
)


def _codex_preflight(
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
    configuration_invalid = _codex_configuration_invalid(models, auth)

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
    # Run a real command through Codex's own sandbox without a model turn.  A
    # successful read/write probe positively observes both repository access
    # and subprocess execution at the child boundary; help text remains only
    # declaration evidence.
    sandbox_probe_ok = _codex_sandbox_capability_probe(
        profile,
        binary_path=binary_path,
        project_root=project_root,
        environment=environment,
        runner=runner,
    )
    effective_capabilities: set[str] = set()
    if sandbox_probe_ok:
        effective_capabilities.update({"repository", "tools"})
    network_probe_ok = _codex_network_capability_probe(
        profile,
        project_root=project_root,
        environment=environment,
        runner=runner,
    )
    if network_probe_ok:
        effective_capabilities.add("network")
    missing_effective = sorted(
        set(profile.required_capabilities) - effective_capabilities
    )
    effective_capabilities_ok = not missing_effective

    permission_ok = (
        profile.permission_mode in {"read-only", "workspace-write"}
        and profile.permission_mode in exec_text
    )
    checks = [
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
                f"runtime catalog contains {profile.model}"
                if model_ok
                else (
                    _codex_configuration_guidance()
                    if configuration_invalid
                    else f"runtime catalog does not contain {profile.model}"
                )
            ),
        ),
        RuntimePreflightCheck(
            "authentication_billing",
            auth_ok,
            (
                "verified ChatGPT subscription login with API credentials stripped"
                if auth_ok
                else (
                    _codex_configuration_guidance()
                    if configuration_invalid
                    else "could not verify the configured ChatGPT subscription boundary"
                )
            ),
        ),
        RuntimePreflightCheck(
            "capabilities",
            capabilities_ok,
            (
                f"declared runtime support for {', '.join(profile.required_capabilities)}"
                if capabilities_ok
                else f"missing declared capability: {', '.join(missing_capabilities)}"
            ),
        ),
        RuntimePreflightCheck(
            "effective_capabilities",
            effective_capabilities_ok,
            (
                "bounded no-model driver probe positively observed "
                + ", ".join(profile.required_capabilities)
                if effective_capabilities_ok
                else "effective capability not positively observed: "
                + ", ".join(missing_effective)
            ),
        ),
        *[
            RuntimePreflightCheck(
                f"effective_capability:{capability}",
                capability in effective_capabilities,
                _effective_capability_detail(
                    capability,
                    observed=capability in effective_capabilities,
                    configuration_invalid=configuration_invalid,
                ),
            )
            for capability in profile.required_capabilities
        ],
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
    if profile.resumable:
        app_server_help = _run(
            runner,
            [str(binary_path), "app-server", "--help"],
            cwd=project_root,
            environment=environment,
        )
        app_server_text = (
            app_server_help.stdout
            if app_server_help is not None and app_server_help.returncode == 0
            else ""
        )
        resume_ok = (
            profile.version in SUPPORTED_CODEX_APP_SERVER_VERSIONS
            and profile.session_identity_mode == "exact"
            and profile.terminal_observation == "provider"
            and not profile.concurrent_attachment
            and "--stdio" in app_server_text
            and "--strict-config" in app_server_text
            and "--config <key=value>" in app_server_text
        )
        checks.append(
            RuntimePreflightCheck(
                "provider_session_resume",
                resume_ok,
                (
                    "verified exact provider-session transport over bounded app-server stdio"
                    if resume_ok
                    else "could not verify exact resumable app-server transport"
                ),
            )
        )
    return checks


def _codex_sandbox_capability_probe(
    profile: RuntimeProfile,
    *,
    binary_path: Path,
    project_root: Path,
    environment: dict[str, str],
    runner: CommandRunner,
) -> bool:
    if profile.permission_mode == "read-only":
        sentinel = project_root / ".agent-mesh" / "config.toml"
        result = _run(
            runner,
            [
                str(binary_path),
                "sandbox",
                "--permission-profile",
                ":read-only",
                "--cd",
                str(project_root),
                "--",
                "/bin/test",
                "-r",
                str(sentinel),
            ],
            cwd=project_root,
            environment=environment,
        )
        return result is not None and result.returncode == 0

    if profile.permission_mode != "workspace-write":
        return False
    probe_path = (
        project_root
        / ".agent-mesh"
        / f".runtime-capability-probe-{secrets.token_hex(12)}"
    )
    try:
        result = _run(
            runner,
            [
                str(binary_path),
                "sandbox",
                "--permission-profile",
                ":workspace",
                "--cd",
                str(project_root),
                "--",
                "/usr/bin/touch",
                str(probe_path),
            ],
            cwd=project_root,
            environment=environment,
        )
        try:
            observed = probe_path.lstat()
        except OSError:
            return False
        return bool(
            result is not None
            and result.returncode == 0
            and stat.S_ISREG(observed.st_mode)
            and observed.st_size == 0
        )
    finally:
        try:
            probe_path.unlink()
        except FileNotFoundError:
            pass


def _codex_network_capability_probe(
    profile: RuntimeProfile,
    *,
    project_root: Path,
    environment: dict[str, str],
    runner: CommandRunner,
) -> bool:
    if "network" not in profile.required_capabilities:
        return True
    result = _run(
        runner,
        [
            "/usr/bin/curl",
            "--silent",
            "--show-error",
            "--head",
            "--connect-timeout",
            "3",
            "--max-time",
            "5",
            "--output",
            "/dev/null",
            "https://chatgpt.com/",
        ],
        cwd=project_root,
        environment=environment,
    )
    return result is not None and result.returncode == 0


def _codex_configuration_invalid(
    *results: subprocess.CompletedProcess[str] | None,
) -> bool:
    for result in results:
        if result is None:
            continue
        output = result.stdout + "\n" + result.stderr
        if "Error loading configuration:" in output or "invalid configuration" in output.lower():
            return True
    return False


def _codex_configuration_guidance() -> str:
    return (
        "Codex user configuration is invalid; run `codex login status` in Terminal "
        "and repair the reported ~/.codex/config.toml entry"
    )


def _effective_capability_detail(
    capability: str,
    *,
    observed: bool,
    configuration_invalid: bool = False,
) -> str:
    if not observed and configuration_invalid and capability in {"repository", "tools"}:
        return _codex_configuration_guidance()
    if capability == "network":
        return (
            "bounded no-model host network probe reached the configured provider boundary"
            if observed
            else "host network probe did not reach the configured provider boundary; verify "
            "ChatGPT connectivity and the selected profile"
        )
    return (
        f"bounded no-model Codex sandbox probe observed {capability}"
        if observed
        else f"Codex sandbox probe did not establish {capability}"
    )


def _build_codex_adapter(
    profile: RuntimeProfile,
    *,
    binary_path: Path,
    context: RuntimeDriverContext,
) -> AgentRuntimeAdapter:
    configured_binary = Path(profile.binary).expanduser()
    runtime_binary = configured_binary if configured_binary.is_absolute() else binary_path
    return CodexCliRuntimeAdapter(
        AdapterSpec(
            name=profile.name,
            domain="agent_runtime",
            privacy_class="project_private",
            options={
                "binary": str(runtime_binary),
                "version": profile.version,
                "model": profile.model,
                "participant": profile.target,
                "provider": profile.provider,
                "durable_role": profile.durable_role,
                "role": profile.role,
                "permission_mode": profile.permission_mode,
                "capabilities": profile.required_capabilities,
                "authentication_mode": profile.authentication_mode,
                "billing_mode": profile.billing_mode,
                "credential_denylist": profile.credential_denylist,
                "adapter_trust": profile.adapter_trust,
                "session_identity_mode": profile.session_identity_mode,
                "resumable": profile.resumable,
                "concurrent_attachment": profile.concurrent_attachment,
                "terminal_observation": profile.terminal_observation,
                "events_path": str(context.events_path),
                "project_scope": context.project_scope,
            },
        )
    )


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


CODEX_CLI_DRIVER = RuntimeAdapterDriver(
    adapter_id="codex-cli",
    providers=frozenset({"openai"}),
    preflight=_codex_preflight,
    factory=_build_codex_adapter,
    lifecycle_modes=frozenset(
        {
            ("none", False, False, "process"),
            ("exact", True, False, "provider"),
        }
    ),
    resumable_check="provider_session_resume",
)
