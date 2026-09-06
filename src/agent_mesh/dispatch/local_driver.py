"""Bounded host for digest-pinned project-local one-shot runtime drivers."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import tempfile
import tomllib
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Callable, Iterator

from agent_mesh.adapters.base import AdapterSpec
from agent_mesh.config import (
    LOCAL_RUNTIME_ADAPTER_RE,
    LOCAL_RUNTIME_DRIVER_PROTOCOL,
    LOCAL_RUNTIME_DRIVER_SOURCE,
    SHA256_RE,
    ConfigError,
    RuntimeProfile,
)
from agent_mesh.core.agent_instances import RuntimeIdentityHandshake

from .adapters import AgentRuntimeAdapter
from .process_io import run_bounded_process
from .runtime import sanitized_child_environment
from .runtime_registry import (
    CommandRunner,
    RuntimeAdapterDriver,
    RuntimeDriverContext,
    RuntimePreflightCheck,
)
from .types import AgentLaunchResult, AgentLaunchSpec, AgentRunRequest


LOCAL_DRIVER_MANIFEST_SCHEMA = "agent-mesh.runtime-driver-manifest.v1"
LOCAL_DRIVER_PROBE_REQUEST_SCHEMA = "agent-mesh.runtime-driver-probe-request.v1"
LOCAL_DRIVER_PROBE_RESPONSE_SCHEMA = "agent-mesh.runtime-driver-probe-response.v1"
LOCAL_DRIVER_LAUNCH_REQUEST_SCHEMA = "agent-mesh.runtime-driver-launch-request.v1"
MAX_LOCAL_DRIVER_MANIFEST_BYTES = 32 * 1024
MAX_LOCAL_DRIVER_ENTRYPOINT_BYTES = 16 * 1024 * 1024
MAX_LOCAL_DRIVER_PROBE_REQUEST_BYTES = 32 * 1024
MAX_LOCAL_DRIVER_PROBE_STDOUT_BYTES = 64 * 1024
MAX_LOCAL_DRIVER_PROBE_STDERR_BYTES = 16 * 1024
MAX_LOCAL_DRIVER_LAUNCH_REQUEST_BYTES = 1024 * 1024
LOCAL_DRIVER_PROBE_TIMEOUT_SECONDS = 15
MAX_LOCAL_DRIVER_LIST_ITEMS = 256
MAX_LOCAL_DRIVER_TEXT_CHARS = 512
_PROVIDER_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,62}$")
_MANIFEST_FIELDS = frozenset(
    {"schema", "id", "provider", "entrypoint", "entrypoint_sha256"}
)
_PROBE_RESPONSE_FIELDS = frozenset(
    {
        "schema",
        "version",
        "models",
        "authentication_mode",
        "billing_mode",
        "capabilities",
        "permission_modes",
    }
)


class LocalRuntimeDriverError(ConfigError):
    """A project-local driver failed a stable trust or protocol boundary."""


@dataclass(frozen=True)
class LocalRuntimeDriverManifest:
    driver_id: str
    provider: str
    protocol: str
    manifest_path: Path
    manifest_sha256: str
    entrypoint_path: Path
    entrypoint_sha256: str
    entrypoint_bytes: bytes = field(repr=False)


@dataclass(frozen=True)
class LocalDriverScaffoldResult:
    directory: Path
    manifest_path: Path
    entrypoint_path: Path
    manifest_sha256: str
    entrypoint_sha256: str


def project_local_runtime_driver(
    profile: RuntimeProfile,
    *,
    project_root: Path,
) -> RuntimeAdapterDriver:
    """Load the one local driver explicitly selected and pinned by ``profile``."""

    manifest = load_project_local_manifest(profile, project_root=project_root)

    def preflight(
        profile: RuntimeProfile,
        *,
        binary_path: Path,
        project_root: Path,
        environment: dict[str, str],
        runner: CommandRunner,
    ) -> list[RuntimePreflightCheck]:
        del runner
        return _local_driver_preflight(
            profile,
            manifest=manifest,
            binary_path=binary_path,
            project_root=project_root,
            environment=environment,
        )

    def factory(
        profile: RuntimeProfile,
        *,
        binary_path: Path,
        context: RuntimeDriverContext,
    ) -> AgentRuntimeAdapter:
        return _build_local_runtime_adapter(
            profile,
            manifest=manifest,
            binary_path=binary_path,
            context=context,
        )

    return RuntimeAdapterDriver(
        adapter_id=manifest.driver_id,
        providers=frozenset({manifest.provider}),
        preflight=preflight,
        factory=factory,
        ownership=LOCAL_RUNTIME_DRIVER_SOURCE,
        lifecycle_modes=frozenset({("none", False, False, "process")}),
    )


def load_project_local_manifest(
    profile: RuntimeProfile,
    *,
    project_root: Path,
) -> LocalRuntimeDriverManifest:
    if profile.driver_source != LOCAL_RUNTIME_DRIVER_SOURCE:
        raise LocalRuntimeDriverError("LOCAL_DRIVER_SOURCE_INVALID")
    root = project_root.resolve(strict=True)
    manifest_path, manifest_bytes = _read_project_file(
        root,
        profile.driver_manifest,
        max_bytes=MAX_LOCAL_DRIVER_MANIFEST_BYTES,
        code="LOCAL_DRIVER_MANIFEST",
    )
    manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()
    if manifest_sha != profile.driver_manifest_sha256:
        raise LocalRuntimeDriverError("LOCAL_DRIVER_MANIFEST_DIGEST_MISMATCH")
    try:
        payload = tomllib.loads(manifest_bytes.decode("utf-8"))
    except (UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise LocalRuntimeDriverError("LOCAL_DRIVER_MANIFEST_INVALID") from exc
    if not isinstance(payload, dict) or set(payload) != set(_MANIFEST_FIELDS):
        raise LocalRuntimeDriverError("LOCAL_DRIVER_MANIFEST_FIELDS_INVALID")

    schema = _bounded_text(payload.get("schema"), code="LOCAL_DRIVER_MANIFEST_SCHEMA_INVALID")
    driver_id = _bounded_text(payload.get("id"), code="LOCAL_DRIVER_ID_INVALID")
    provider = _bounded_text(payload.get("provider"), code="LOCAL_DRIVER_PROVIDER_INVALID")
    entrypoint = _bounded_text(
        payload.get("entrypoint"), code="LOCAL_DRIVER_ENTRYPOINT_INVALID"
    )
    entrypoint_sha = _bounded_text(
        payload.get("entrypoint_sha256"), code="LOCAL_DRIVER_ENTRYPOINT_DIGEST_INVALID"
    )
    if schema != LOCAL_DRIVER_MANIFEST_SCHEMA:
        raise LocalRuntimeDriverError("LOCAL_DRIVER_MANIFEST_SCHEMA_UNSUPPORTED")
    if not LOCAL_RUNTIME_ADAPTER_RE.fullmatch(driver_id) or driver_id != profile.adapter:
        raise LocalRuntimeDriverError("LOCAL_DRIVER_ID_MISMATCH")
    if not _PROVIDER_RE.fullmatch(provider) or provider != profile.provider:
        raise LocalRuntimeDriverError("LOCAL_DRIVER_PROVIDER_MISMATCH")
    if not SHA256_RE.fullmatch(entrypoint_sha):
        raise LocalRuntimeDriverError("LOCAL_DRIVER_ENTRYPOINT_DIGEST_INVALID")

    manifest_relative = Path(profile.driver_manifest)
    entrypoint_relative = manifest_relative.parent / entrypoint
    entrypoint_path, entrypoint_bytes = _read_project_file(
        root,
        entrypoint_relative,
        max_bytes=MAX_LOCAL_DRIVER_ENTRYPOINT_BYTES,
        code="LOCAL_DRIVER_ENTRYPOINT",
        require_executable=True,
    )
    actual_entrypoint_sha = hashlib.sha256(entrypoint_bytes).hexdigest()
    if actual_entrypoint_sha != entrypoint_sha:
        raise LocalRuntimeDriverError("LOCAL_DRIVER_ENTRYPOINT_DIGEST_MISMATCH")
    return LocalRuntimeDriverManifest(
        driver_id=driver_id,
        provider=provider,
        protocol=LOCAL_RUNTIME_DRIVER_PROTOCOL,
        manifest_path=manifest_path,
        manifest_sha256=manifest_sha,
        entrypoint_path=entrypoint_path,
        entrypoint_sha256=entrypoint_sha,
        entrypoint_bytes=entrypoint_bytes,
    )


def scaffold_project_local_driver(
    *,
    project_root: Path,
    output: str,
    driver_id: str,
    provider: str,
) -> LocalDriverScaffoldResult:
    """Create a fail-closed V1 driver skeleton without editing project config."""

    if not LOCAL_RUNTIME_ADAPTER_RE.fullmatch(driver_id):
        raise LocalRuntimeDriverError("LOCAL_DRIVER_ID_INVALID")
    if not _PROVIDER_RE.fullmatch(provider):
        raise LocalRuntimeDriverError("LOCAL_DRIVER_PROVIDER_INVALID")
    root = project_root.resolve(strict=True)
    relative = _relative_path(output, code="LOCAL_DRIVER_SCAFFOLD_PATH_INVALID")
    directory = root / relative
    entrypoint_path = directory / "driver.py"
    entrypoint_text = _scaffold_entrypoint(driver_id=driver_id, provider=provider)
    entrypoint_bytes = entrypoint_text.encode("utf-8")
    entrypoint_sha = hashlib.sha256(entrypoint_bytes).hexdigest()
    manifest_path = directory / "driver.toml"
    manifest_text = (
        f'schema = "{LOCAL_DRIVER_MANIFEST_SCHEMA}"\n'
        f'id = "{driver_id}"\n'
        f'provider = "{provider}"\n'
        'entrypoint = "driver.py"\n'
        f'entrypoint_sha256 = "{entrypoint_sha}"\n'
    )
    manifest_bytes = manifest_text.encode("utf-8")
    _create_scaffold_files(
        root,
        relative,
        files=(("driver.py", entrypoint_bytes, 0o700), ("driver.toml", manifest_bytes, 0o600)),
    )
    manifest_sha = hashlib.sha256(manifest_text.encode("utf-8")).hexdigest()
    return LocalDriverScaffoldResult(
        directory=directory,
        manifest_path=manifest_path,
        entrypoint_path=entrypoint_path,
        manifest_sha256=manifest_sha,
        entrypoint_sha256=entrypoint_sha,
    )


class LocalOneShotRuntimeAdapter(AgentRuntimeAdapter):
    """Generic D006 one-shot host for one pinned external V1 entrypoint."""

    def __init__(
        self,
        spec: AdapterSpec,
        *,
        manifest: LocalRuntimeDriverManifest,
    ) -> None:
        super().__init__(spec)
        self._manifest = manifest

    def prepare_identity_handshake(self, request: AgentRunRequest) -> str:
        self._launch_request(request)
        return ""

    def identity_handshake(self, request: AgentRunRequest) -> RuntimeIdentityHandshake:
        options = self.spec.options
        return RuntimeIdentityHandshake(
            participant=str(options["participant"]),
            provider=str(options["provider"]),
            runtime_profile=self.spec.name,
            durable_role=str(options["durable_role"]),
            role=str(options["role"]),
            capabilities=tuple(options["capabilities"]),
            permission_mode=str(options["permission_mode"]),
            authentication_mode=str(options["authentication_mode"]),
            billing_mode=str(options["billing_mode"]),
            adapter_trust=LOCAL_RUNTIME_DRIVER_SOURCE,
            session_identity_mode="none",
            resumable=False,
            concurrent_attachment=False,
            terminal_observation="process",
            lifecycle_disposition="new",
            launch_attempt_key=f"local-{request.session_uuid}-{request.run_id}",
        )

    def build_launch(self, request: AgentRunRequest) -> AgentLaunchSpec:
        del request
        raise LocalRuntimeDriverError("LOCAL_DRIVER_MANAGED_LAUNCH_REQUIRED")

    def _build_managed_launch(
        self,
        request: AgentRunRequest,
        *,
        entrypoint: Path,
    ) -> AgentLaunchSpec:
        options = self.spec.options
        prompt_sha = hashlib.sha256(request.prompt.encode("utf-8")).hexdigest()
        return AgentLaunchSpec(
            argv=[str(entrypoint), "launch"],
            cwd=request.project_root,
            requires_pty=False,
            timeout_seconds=request.timeout_seconds,
            prompt_sha=prompt_sha,
            stdin_text=self._launch_request(request),
            metadata={
                "runtime_protocol": LOCAL_RUNTIME_DRIVER_PROTOCOL,
                "driver_id": options["driver_id"],
                "driver_manifest_sha256": options["driver_manifest_sha256"],
                "driver_entrypoint_sha256": options["entrypoint_sha256"],
            },
            environment=sanitized_child_environment(tuple(options["credential_denylist"])),
        )

    def launch(
        self,
        request: AgentRunRequest,
        *,
        launch_process: Callable[[AgentLaunchSpec], AgentLaunchResult],
        child_instance_handle: str,
    ) -> AgentLaunchResult:
        with _materialized_entrypoint(self._manifest) as entrypoint:
            launch_spec = replace(
                self._build_managed_launch(request, entrypoint=entrypoint),
                child_instance_handle=child_instance_handle,
            )
            result = launch_process(launch_spec)
            if result.status != "completed" or result.exit_code != 0:
                return replace(result, stdout="", stderr="")
            return replace(result, stderr="")

    def _launch_request(self, request: AgentRunRequest) -> str:
        options = self.spec.options
        payload = {
            "schema": LOCAL_DRIVER_LAUNCH_REQUEST_SCHEMA,
            "provider_binary": options["binary"],
            "version": options["version"],
            "model": options["model"],
            "permission_mode": options["permission_mode"],
            "capabilities": list(options["capabilities"]),
            "authentication_mode": options["authentication_mode"],
            "billing_mode": options["billing_mode"],
            "prompt": request.prompt,
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n"
        if len(encoded.encode("utf-8")) > MAX_LOCAL_DRIVER_LAUNCH_REQUEST_BYTES:
            raise LocalRuntimeDriverError("LOCAL_DRIVER_LAUNCH_REQUEST_TOO_LARGE")
        return encoded


def _build_local_runtime_adapter(
    profile: RuntimeProfile,
    *,
    manifest: LocalRuntimeDriverManifest,
    binary_path: Path,
    context: RuntimeDriverContext,
) -> AgentRuntimeAdapter:
    return LocalOneShotRuntimeAdapter(
        AdapterSpec(
            name=profile.name,
            domain="agent_runtime",
            privacy_class="project_private",
            options={
                "binary": str(binary_path),
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
                "adapter_trust": LOCAL_RUNTIME_DRIVER_SOURCE,
                "session_identity_mode": "none",
                "resumable": False,
                "concurrent_attachment": False,
                "terminal_observation": "process",
                "entrypoint": str(manifest.entrypoint_path),
                "driver_id": manifest.driver_id,
                "driver_manifest_sha256": manifest.manifest_sha256,
                "entrypoint_sha256": manifest.entrypoint_sha256,
                "events_path": str(context.events_path),
                "project_scope": context.project_scope,
            },
        ),
        manifest=manifest,
    )


def _local_driver_preflight(
    profile: RuntimeProfile,
    *,
    manifest: LocalRuntimeDriverManifest,
    binary_path: Path,
    project_root: Path,
    environment: dict[str, str],
) -> list[RuntimePreflightCheck]:
    request = {
        "schema": LOCAL_DRIVER_PROBE_REQUEST_SCHEMA,
        "provider_binary": str(binary_path),
        "configured_version": profile.version,
        "configured_model": profile.model,
        "authentication_mode": profile.authentication_mode,
        "billing_mode": profile.billing_mode,
        "required_capabilities": list(profile.required_capabilities),
        "permission_mode": profile.permission_mode,
    }
    request_text = json.dumps(request, sort_keys=True, separators=(",", ":")) + "\n"
    if len(request_text.encode("utf-8")) > MAX_LOCAL_DRIVER_PROBE_REQUEST_BYTES:
        return _failed_probe_checks("request_too_large")
    with _materialized_entrypoint(manifest) as entrypoint:
        result = run_bounded_process(
            [str(entrypoint), "probe"],
            cwd=project_root,
            environment=environment,
            stdin=request_text.encode("utf-8"),
            timeout_seconds=LOCAL_DRIVER_PROBE_TIMEOUT_SECONDS,
            stdout_limit=MAX_LOCAL_DRIVER_PROBE_STDOUT_BYTES,
            stderr_limit=MAX_LOCAL_DRIVER_PROBE_STDERR_BYTES,
        )
    if result.status != "completed" or result.returncode != 0:
        return _failed_probe_checks(result.status)
    try:
        response = json.loads(result.stdout.decode("utf-8"), object_pairs_hook=_unique_object)
        _validate_probe_response(response)
    except (UnicodeDecodeError, json.JSONDecodeError, LocalRuntimeDriverError):
        return _failed_probe_checks("response_invalid")
    assert isinstance(response, dict)
    version_ok = response["version"] == profile.version
    model_ok = profile.model in response["models"]
    authentication_ok = (
        response["authentication_mode"] == profile.authentication_mode
        and response["billing_mode"] == profile.billing_mode
    )
    capabilities_ok = set(profile.required_capabilities).issubset(response["capabilities"])
    permission_ok = profile.permission_mode in response["permission_modes"]
    return [
        RuntimePreflightCheck("version", version_ok, _proof_detail("version", version_ok)),
        RuntimePreflightCheck("model", model_ok, _proof_detail("model", model_ok)),
        RuntimePreflightCheck(
            "authentication_billing",
            authentication_ok,
            _proof_detail("authentication and billing", authentication_ok),
        ),
        RuntimePreflightCheck(
            "capabilities",
            capabilities_ok,
            _proof_detail("capabilities", capabilities_ok),
        ),
        RuntimePreflightCheck(
            "effective_capabilities",
            capabilities_ok,
            _proof_detail("effective_capabilities", capabilities_ok),
        ),
        RuntimePreflightCheck(
            "permission_mode",
            permission_ok,
            _proof_detail("permission mode", permission_ok),
        ),
    ]


def _validate_probe_response(value: object) -> None:
    if not isinstance(value, dict) or set(value) != set(_PROBE_RESPONSE_FIELDS):
        raise LocalRuntimeDriverError("LOCAL_DRIVER_PROBE_FIELDS_INVALID")
    if value.get("schema") != LOCAL_DRIVER_PROBE_RESPONSE_SCHEMA:
        raise LocalRuntimeDriverError("LOCAL_DRIVER_PROBE_SCHEMA_INVALID")
    for field_name in ("version", "authentication_mode", "billing_mode"):
        _bounded_text(value.get(field_name), code="LOCAL_DRIVER_PROBE_TEXT_INVALID")
    for field_name in ("models", "capabilities", "permission_modes"):
        values = value.get(field_name)
        if not isinstance(values, list) or len(values) > MAX_LOCAL_DRIVER_LIST_ITEMS:
            raise LocalRuntimeDriverError("LOCAL_DRIVER_PROBE_LIST_INVALID")
        normalized = [
            _bounded_text(item, code="LOCAL_DRIVER_PROBE_LIST_INVALID") for item in values
        ]
        if len(set(normalized)) != len(normalized):
            raise LocalRuntimeDriverError("LOCAL_DRIVER_PROBE_LIST_DUPLICATE")


def _failed_probe_checks(reason: str) -> list[RuntimePreflightCheck]:
    detail = f"project-local driver probe failed ({reason})"
    return [
        RuntimePreflightCheck(name, False, detail)
        for name in (
            "version",
            "model",
            "authentication_billing",
            "capabilities",
            "effective_capabilities",
            "permission_mode",
        )
    ]


def _proof_detail(label: str, passed: bool) -> str:
    outcome = "reported matching" if passed else "did not report matching"
    return f"project-local probe {outcome} {label}"


@contextmanager
def _materialized_entrypoint(
    manifest: LocalRuntimeDriverManifest,
) -> Iterator[Path]:
    """Execute the verified bytes, not a path that project code can swap after validation."""

    with tempfile.TemporaryDirectory(prefix="agent-mesh-driver-") as raw_directory:
        directory = Path(raw_directory)
        directory.chmod(0o700)
        entrypoint = directory / "entrypoint"
        with entrypoint.open("xb") as handle:
            handle.write(manifest.entrypoint_bytes)
        entrypoint.chmod(0o700)
        yield entrypoint


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise LocalRuntimeDriverError("LOCAL_DRIVER_JSON_DUPLICATE_KEY")
        result[key] = value
    return result


def _bounded_text(value: object, *, code: str) -> str:
    if not isinstance(value, str) or not value or len(value) > MAX_LOCAL_DRIVER_TEXT_CHARS:
        raise LocalRuntimeDriverError(code)
    if any(ord(character) < 32 for character in value):
        raise LocalRuntimeDriverError(code)
    return value


def _read_project_file(
    root: Path,
    relative: str | Path,
    *,
    max_bytes: int,
    code: str,
    require_executable: bool = False,
) -> tuple[Path, bytes]:
    path = _relative_path(relative, code=f"{code}_PATH_INVALID")
    descriptors: list[int] = []
    try:
        descriptor = os.open(root, _directory_open_flags())
    except OSError as exc:
        raise LocalRuntimeDriverError(f"{code}_UNAVAILABLE") from exc
    descriptors.append(descriptor)
    try:
        for component in path.parts[:-1]:
            _reject_symlink(descriptor, component, code=code)
            try:
                descriptor = os.open(
                    component,
                    _directory_open_flags(),
                    dir_fd=descriptor,
                )
            except OSError as exc:
                raise LocalRuntimeDriverError(f"{code}_UNAVAILABLE") from exc
            descriptors.append(descriptor)
        final_name = path.parts[-1]
        _reject_symlink(descriptor, final_name, code=code)
        try:
            file_descriptor = os.open(
                final_name,
                os.O_RDONLY
                | getattr(os, "O_CLOEXEC", 0)
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_NONBLOCK", 0),
                dir_fd=descriptor,
            )
        except OSError as exc:
            raise LocalRuntimeDriverError(f"{code}_UNAVAILABLE") from exc
        descriptors.append(file_descriptor)
        info = os.fstat(file_descriptor)
        if not stat.S_ISREG(info.st_mode):
            raise LocalRuntimeDriverError(f"{code}_NOT_REGULAR")
        if require_executable and not info.st_mode & 0o111:
            raise LocalRuntimeDriverError(f"{code}_NOT_EXECUTABLE")
        if info.st_size > max_bytes:
            raise LocalRuntimeDriverError(f"{code}_TOO_LARGE")
        chunks: list[bytes] = []
        remaining = max_bytes + 1
        while remaining:
            chunk = os.read(file_descriptor, min(65536, remaining))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        value = b"".join(chunks)
        if len(value) > max_bytes:
            raise LocalRuntimeDriverError(f"{code}_TOO_LARGE")
        return root / path, value
    finally:
        for open_descriptor in reversed(descriptors):
            os.close(open_descriptor)


def _relative_path(value: str | Path, *, code: str) -> Path:
    path = Path(value)
    if (
        not path.parts
        or path.is_absolute()
        or ".." in path.parts
        or any(any(ord(character) < 32 or ord(character) == 127 for character in part) for part in path.parts)
    ):
        raise LocalRuntimeDriverError(code)
    return path


def _create_scaffold_files(
    root: Path,
    relative: Path,
    *,
    files: tuple[tuple[str, bytes, int], ...],
) -> None:
    descriptors: list[int] = []
    created_directories: list[tuple[int, str]] = []
    created_files: list[tuple[int, str]] = []
    try:
        descriptor = os.open(root, _directory_open_flags())
        descriptors.append(descriptor)
        for index, component in enumerate(relative.parts):
            is_target = index == len(relative.parts) - 1
            try:
                info = os.stat(component, dir_fd=descriptor, follow_symlinks=False)
            except FileNotFoundError:
                try:
                    os.mkdir(component, mode=0o700, dir_fd=descriptor)
                except OSError as exc:
                    raise LocalRuntimeDriverError(
                        "LOCAL_DRIVER_SCAFFOLD_PATH_UNSAFE"
                    ) from exc
                created_directories.append((descriptor, component))
            else:
                if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
                    raise LocalRuntimeDriverError("LOCAL_DRIVER_SCAFFOLD_PATH_UNSAFE")
                if is_target:
                    raise LocalRuntimeDriverError("LOCAL_DRIVER_SCAFFOLD_TARGET_EXISTS")
            try:
                descriptor = os.open(
                    component,
                    _directory_open_flags(),
                    dir_fd=descriptor,
                )
            except OSError as exc:
                raise LocalRuntimeDriverError("LOCAL_DRIVER_SCAFFOLD_PATH_UNSAFE") from exc
            descriptors.append(descriptor)

        for name, content, mode in files:
            try:
                file_descriptor = os.open(
                    name,
                    os.O_WRONLY
                    | os.O_CREAT
                    | os.O_EXCL
                    | getattr(os, "O_CLOEXEC", 0)
                    | getattr(os, "O_NOFOLLOW", 0),
                    mode,
                    dir_fd=descriptor,
                )
            except OSError as exc:
                raise LocalRuntimeDriverError("LOCAL_DRIVER_SCAFFOLD_TARGET_EXISTS") from exc
            created_files.append((descriptor, name))
            try:
                os.fchmod(file_descriptor, mode)
                _write_all(file_descriptor, content)
                os.fsync(file_descriptor)
            finally:
                os.close(file_descriptor)
    except Exception:
        for parent_descriptor, name in reversed(created_files):
            try:
                os.unlink(name, dir_fd=parent_descriptor)
            except OSError:
                pass
        for parent_descriptor, name in reversed(created_directories):
            try:
                os.rmdir(name, dir_fd=parent_descriptor)
            except OSError:
                pass
        raise
    finally:
        for open_descriptor in reversed(descriptors):
            os.close(open_descriptor)


def _directory_open_flags() -> int:
    return (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_DIRECTORY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )


def _reject_symlink(parent_descriptor: int, component: str, *, code: str) -> None:
    try:
        info = os.stat(component, dir_fd=parent_descriptor, follow_symlinks=False)
    except OSError as exc:
        raise LocalRuntimeDriverError(f"{code}_UNAVAILABLE") from exc
    if stat.S_ISLNK(info.st_mode):
        raise LocalRuntimeDriverError(f"{code}_SYMLINK_FORBIDDEN")


def _write_all(descriptor: int, content: bytes) -> None:
    offset = 0
    while offset < len(content):
        written = os.write(descriptor, content[offset:])
        if not written:
            raise OSError("short write")
        offset += written


def _scaffold_entrypoint(*, driver_id: str, provider: str) -> str:
    return f'''#!/usr/bin/env python3
"""Fail-closed Agent Mesh local driver scaffold for {driver_id}."""

import json
import sys

PROTOCOL = "{LOCAL_RUNTIME_DRIVER_PROTOCOL}"
PROVIDER = "{provider}"


def probe(request):
    # TODO: query no-prompt machine-readable provider surfaces and return
    # agent-mesh.runtime-driver-probe-response.v1. Never echo configured facts.
    raise RuntimeError("probe is not implemented")


def launch(request):
    # TODO: invoke the provider once, passing request["prompt"] outside argv,
    # and write only the response candidate to stdout.
    raise RuntimeError("launch is not implemented")


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in {{"probe", "launch"}}:
        return 2
    try:
        request = json.load(sys.stdin)
        response = probe(request) if sys.argv[1] == "probe" else launch(request)
    except Exception:
        return 2
    if sys.argv[1] == "probe":
        json.dump(response, sys.stdout, sort_keys=True, separators=(",", ":"))
        sys.stdout.write("\\n")
    elif response is not None:
        sys.stdout.write(str(response))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
'''
