"""Install and manage the Workbench as a native per-user service."""
from __future__ import annotations

import json
import ipaddress
import os
import plistlib
import re
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence
from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen
from xml.sax.saxutils import escape as xml_escape

from agent_mesh.project_registry import registry_dir


SERVICE_LABEL = "dev.agent-mesh.workbench"
SYSTEMD_UNIT = "agent-mesh-workbench.service"
WINDOWS_TASK = "Agent Mesh Workbench"
SERVICE_SCHEMA_VERSION = 1
SUPPORTED_PLATFORMS = {"darwin", "linux", "win32"}


class WorkbenchServiceError(RuntimeError):
    """Raised when the native Workbench service cannot be managed safely."""


@dataclass(frozen=True)
class WorkbenchServiceSpec:
    """The stable launch contract persisted in the native service definition."""

    repo: Path
    host: str
    port: int
    python_executable: Path
    config_home: Path

    @property
    def command(self) -> tuple[str, ...]:
        return (
            str(self.python_executable),
            "-m",
            "agent_mesh.cli.mail",
            "workbench",
            "--repo",
            str(self.repo),
            "--host",
            self.host,
            "--port",
            str(self.port),
            "--managed-service",
            "--config-home",
            str(self.config_home),
        )

    def as_dict(self, *, platform_name: str, definition: Path) -> dict[str, Any]:
        return {
            "schema_version": SERVICE_SCHEMA_VERSION,
            "platform": platform_name,
            "repo": str(self.repo),
            "host": self.host,
            "port": self.port,
            "python_executable": str(self.python_executable),
            "config_home": str(self.config_home),
            "definition": str(definition),
        }


@dataclass(frozen=True)
class WorkbenchServiceStatus:
    platform: str
    installed: bool
    running: bool | None
    state: str
    definition: Path
    metadata: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "platform": self.platform,
            "installed": self.installed,
            "running": self.running,
            "state": self.state,
            "definition": str(self.definition),
            "metadata": self.metadata,
        }


def current_platform(platform_name: str | None = None) -> str:
    value = platform_name or sys.platform
    if value not in SUPPORTED_PLATFORMS:
        raise WorkbenchServiceError(
            f"automatic Workbench service is not supported on platform {value!r}; "
            "supported platforms are macOS, Linux with systemd, and Windows"
        )
    return value


def make_service_spec(
    *,
    repo: Path,
    host: str,
    port: int,
    python_executable: Path | None = None,
    config_home: Path | None = None,
    platform_name: str | None = None,
) -> WorkbenchServiceSpec:
    platform_value = current_platform(platform_name)
    host_value = host.strip()
    _validate_loopback_host(host_value)
    executable = (python_executable or Path(sys.executable)).expanduser().resolve()
    if platform_value == "win32" and executable.name.casefold() == "python.exe":
        pythonw = executable.with_name("pythonw.exe")
        if pythonw.exists():
            executable = pythonw
    if not (1 <= int(port) <= 65535):
        raise WorkbenchServiceError("Workbench service port must be between 1 and 65535")
    return WorkbenchServiceSpec(
        repo=repo.expanduser().resolve(),
        host=host_value,
        port=int(port),
        python_executable=executable,
        config_home=(config_home or registry_dir()).expanduser().resolve(),
    )


def service_definition_path(
    platform_name: str | None = None,
    *,
    home: Path | None = None,
) -> Path:
    platform_value = current_platform(platform_name)
    user_home = (home or Path.home()).expanduser().resolve()
    if platform_value == "darwin":
        return user_home / "Library" / "LaunchAgents" / f"{SERVICE_LABEL}.plist"
    if platform_value == "linux":
        xdg_config = os.environ.get("XDG_CONFIG_HOME", "").strip()
        config_root = Path(xdg_config).expanduser().resolve() if xdg_config else user_home / ".config"
        return config_root / "systemd" / "user" / SYSTEMD_UNIT
    return registry_dir() / "workbench-task.xml"


def service_metadata_path() -> Path:
    return registry_dir() / "workbench-service.json"


def render_launch_agent(spec: WorkbenchServiceSpec) -> bytes:
    log_dir = spec.config_home / "logs"
    payload = {
        "Label": SERVICE_LABEL,
        "ProgramArguments": list(spec.command),
        "RunAtLoad": True,
        "KeepAlive": True,
        "ProcessType": "Background",
        "ThrottleInterval": 10,
        "WorkingDirectory": str(spec.repo),
        "StandardOutPath": str(log_dir / "workbench.log"),
        "StandardErrorPath": str(log_dir / "workbench-error.log"),
        "EnvironmentVariables": {"PYTHONUNBUFFERED": "1"},
    }
    return plistlib.dumps(payload, fmt=plistlib.FMT_XML, sort_keys=True)


def _systemd_quote(value: str) -> str:
    escaped = (
        value.replace("%", "%%")
        .replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )
    return f'"{escaped}"'


def render_systemd_unit(spec: WorkbenchServiceSpec) -> str:
    command = " ".join(_systemd_quote(argument) for argument in spec.command)
    return "\n".join(
        [
            "[Unit]",
            "Description=Agent Mesh Workbench",
            "After=default.target",
            "",
            "[Service]",
            "Type=simple",
            f"ExecStart={command}",
            f"WorkingDirectory={_systemd_quote(str(spec.repo))}",
            'Environment="PYTHONUNBUFFERED=1"',
            "Restart=on-failure",
            "RestartSec=2s",
            "TimeoutStopSec=15s",
            "",
            "[Install]",
            "WantedBy=default.target",
            "",
        ]
    )


def _windows_arguments(arguments: Sequence[str]) -> str:
    return subprocess.list2cmdline(list(arguments))


def render_windows_task(spec: WorkbenchServiceSpec, *, user_id: str) -> str:
    command = xml_escape(str(spec.python_executable))
    arguments = xml_escape(_windows_arguments(spec.command[1:]))
    working_directory = xml_escape(str(spec.repo))
    identity = xml_escape(user_id)
    return f'''<?xml version="1.0" encoding="UTF-16"?>
<Task version="1.4" xmlns="http://schemas.microsoft.com/windows/2004/02/mit/task">
  <RegistrationInfo>
    <Description>Agent Mesh Workbench per-user service</Description>
  </RegistrationInfo>
  <Triggers>
    <LogonTrigger>
      <Enabled>true</Enabled>
      <UserId>{identity}</UserId>
    </LogonTrigger>
  </Triggers>
  <Principals>
    <Principal id="Author">
      <UserId>{identity}</UserId>
      <LogonType>InteractiveToken</LogonType>
      <RunLevel>LeastPrivilege</RunLevel>
    </Principal>
  </Principals>
  <Settings>
    <MultipleInstancesPolicy>IgnoreNew</MultipleInstancesPolicy>
    <DisallowStartIfOnBatteries>false</DisallowStartIfOnBatteries>
    <StopIfGoingOnBatteries>false</StopIfGoingOnBatteries>
    <AllowHardTerminate>true</AllowHardTerminate>
    <StartWhenAvailable>true</StartWhenAvailable>
    <RunOnlyIfNetworkAvailable>false</RunOnlyIfNetworkAvailable>
    <AllowStartOnDemand>true</AllowStartOnDemand>
    <Enabled>true</Enabled>
    <Hidden>false</Hidden>
    <RunOnlyIfIdle>false</RunOnlyIfIdle>
    <WakeToRun>false</WakeToRun>
    <ExecutionTimeLimit>PT0S</ExecutionTimeLimit>
    <Priority>7</Priority>
    <RestartOnFailure>
      <Interval>PT1M</Interval>
      <Count>999</Count>
    </RestartOnFailure>
  </Settings>
  <Actions Context="Author">
    <Exec>
      <Command>{command}</Command>
      <Arguments>{arguments}</Arguments>
      <WorkingDirectory>{working_directory}</WorkingDirectory>
    </Exec>
  </Actions>
</Task>
'''


def install_workbench_service(
    spec: WorkbenchServiceSpec,
    *,
    platform_name: str | None = None,
) -> WorkbenchServiceStatus:
    platform_value = current_platform(platform_name)
    definition = service_definition_path(platform_value)
    if platform_value == "darwin":
        _ensure_private_dir(spec.config_home / "logs")
        _atomic_write(definition, render_launch_agent(spec), mode=0o600)
        domain = _launchd_domain()
        _run(("launchctl", "bootout", domain, str(definition)), check=False)
        _run(("launchctl", "bootstrap", domain, str(definition)))
        _run(("launchctl", "enable", f"{domain}/{SERVICE_LABEL}"))
        _run(("launchctl", "kickstart", "-k", f"{domain}/{SERVICE_LABEL}"))
    elif platform_value == "linux":
        _require_systemd_user()
        _atomic_write(definition, render_systemd_unit(spec).encode("utf-8"), mode=0o600)
        _run(("systemctl", "--user", "daemon-reload"))
        _run(("systemctl", "--user", "enable", SYSTEMD_UNIT))
        # `enable --now` does not restart an already-running unit after its
        # definition changes. `restart` both starts an inactive unit and makes
        # idempotent reinstalls apply the current executable, repo, and port.
        _run(("systemctl", "--user", "restart", SYSTEMD_UNIT))
    else:
        user_id = _windows_user_id()
        _atomic_write(
            definition,
            render_windows_task(spec, user_id=user_id).encode("utf-16"),
            mode=0o600,
        )
        _run(("schtasks.exe", "/End", "/TN", WINDOWS_TASK), check=False)
        _run(("schtasks.exe", "/Create", "/TN", WINDOWS_TASK, "/XML", str(definition), "/F"))
        _run(("schtasks.exe", "/Run", "/TN", WINDOWS_TASK))

    _write_metadata(spec.as_dict(platform_name=platform_value, definition=definition))
    return workbench_service_status(platform_name=platform_value)


def start_workbench_service(*, platform_name: str | None = None) -> WorkbenchServiceStatus:
    platform_value = current_platform(platform_name)
    definition = service_definition_path(platform_value)
    if not definition.exists() and platform_value != "win32":
        raise WorkbenchServiceError("Workbench service is not installed")
    if platform_value == "darwin":
        domain = _launchd_domain()
        loaded = _run(
            ("launchctl", "print", f"{domain}/{SERVICE_LABEL}"),
            check=False,
        ).returncode == 0
        if not loaded:
            _run(("launchctl", "bootstrap", domain, str(definition)))
        _run(("launchctl", "kickstart", f"{domain}/{SERVICE_LABEL}"))
    elif platform_value == "linux":
        _require_systemd_user()
        _run(("systemctl", "--user", "start", SYSTEMD_UNIT))
    else:
        _run(("schtasks.exe", "/Run", "/TN", WINDOWS_TASK))
    return workbench_service_status(platform_name=platform_value)


def restart_workbench_service(*, platform_name: str | None = None) -> WorkbenchServiceStatus:
    platform_value = current_platform(platform_name)
    if platform_value == "darwin":
        domain = _launchd_domain()
        result = _run(
            ("launchctl", "kickstart", "-k", f"{domain}/{SERVICE_LABEL}"),
            check=False,
        )
        if result.returncode != 0:
            return start_workbench_service(platform_name=platform_value)
    elif platform_value == "linux":
        _require_systemd_user()
        _run(("systemctl", "--user", "restart", SYSTEMD_UNIT))
    else:
        _run(("schtasks.exe", "/End", "/TN", WINDOWS_TASK), check=False)
        _run(("schtasks.exe", "/Run", "/TN", WINDOWS_TASK))
    return workbench_service_status(platform_name=platform_value)


def uninstall_workbench_service(*, platform_name: str | None = None) -> WorkbenchServiceStatus:
    platform_value = current_platform(platform_name)
    definition = service_definition_path(platform_value)
    metadata = _read_metadata()
    if platform_value == "darwin":
        _run(("launchctl", "bootout", _launchd_domain(), str(definition)), check=False)
    elif platform_value == "linux":
        _require_systemd_user()
        _run(("systemctl", "--user", "disable", "--now", SYSTEMD_UNIT), check=False)
    else:
        _run(("schtasks.exe", "/End", "/TN", WINDOWS_TASK), check=False)
        _run(("schtasks.exe", "/Delete", "/TN", WINDOWS_TASK, "/F"), check=False)

    definition.unlink(missing_ok=True)
    _managed_bookmark_path(metadata).unlink(missing_ok=True)
    service_metadata_path().unlink(missing_ok=True)
    if platform_value == "linux":
        _run(("systemctl", "--user", "daemon-reload"))
        _run(("systemctl", "--user", "reset-failed", SYSTEMD_UNIT), check=False)
    return workbench_service_status(platform_name=platform_value)


def workbench_service_status(
    *,
    platform_name: str | None = None,
) -> WorkbenchServiceStatus:
    platform_value = current_platform(platform_name)
    definition = service_definition_path(platform_value)
    metadata = _read_metadata()
    running: bool | None
    if platform_value == "darwin":
        result = _run(
            ("launchctl", "print", f"{_launchd_domain()}/{SERVICE_LABEL}"),
            check=False,
        )
        loaded = result.returncode == 0
        running = loaded and bool(re.search(r"(?:state\s*=\s*running|pid\s*=\s*\d+)", result.stdout))
        state = "running" if running else "loaded" if loaded else "not loaded"
        installed = definition.exists()
    elif platform_value == "linux":
        if not definition.exists():
            return WorkbenchServiceStatus(
                platform=platform_value,
                installed=False,
                running=False,
                state="not installed",
                definition=definition,
                metadata=metadata,
            )
        _require_systemd_user()
        result = _run(
            ("systemctl", "--user", "is-active", SYSTEMD_UNIT),
            check=False,
        )
        running = result.returncode == 0 and result.stdout.strip() == "active"
        state = result.stdout.strip() or "inactive"
        installed = True
    else:
        result = _run(("schtasks.exe", "/Query", "/TN", WINDOWS_TASK), check=False)
        installed = result.returncode == 0
        running = None if installed else False
        state = "registered with Task Scheduler" if installed else "not installed"
    return WorkbenchServiceStatus(
        platform=platform_value,
        installed=installed,
        running=running,
        state=state,
        definition=definition,
        metadata=metadata,
    )


def wait_for_managed_workbench(
    bookmark_path: Path,
    *,
    timeout_seconds: float = 10.0,
    poll_interval: float = 0.1,
) -> bool:
    """Wait until a managed bookmark can authenticate to its loopback server."""

    deadline = time.monotonic() + max(timeout_seconds, 0.0)
    while True:
        target = _managed_bookmark_target(bookmark_path)
        if target is not None and _managed_health_ready(*target):
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(max(poll_interval, 0.01))


def _managed_bookmark_target(bookmark_path: Path) -> tuple[str, str] | None:
    try:
        payload = bookmark_path.read_text(encoding="utf-8")
    except OSError:
        return None
    if "const MANAGED_SERVICE = true;" not in payload:
        return None
    api_match = re.search(r"^const API_BASE = (.+);$", payload, flags=re.MULTILINE)
    token_match = re.search(
        r"^const EMBEDDED_API_TOKEN = (.+);$",
        payload,
        flags=re.MULTILINE,
    )
    if api_match is None or token_match is None:
        return None
    try:
        api_base = json.loads(api_match.group(1))
        token = json.loads(token_match.group(1))
    except json.JSONDecodeError:
        return None
    if not isinstance(api_base, str) or not isinstance(token, str) or not token:
        return None
    parsed = urlparse(api_base)
    if parsed.scheme != "http" or parsed.username or parsed.password or not parsed.hostname:
        return None
    try:
        _validate_loopback_host(parsed.hostname)
        _ = parsed.port
    except (ValueError, WorkbenchServiceError):
        return None
    return api_base.rstrip("/"), token


def _managed_health_ready(api_base: str, token: str) -> bool:
    request = Request(
        f"{api_base}/api/health",
        headers={"X-Agent-Mesh-Token": token},
    )
    try:
        with urlopen(request, timeout=0.5) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (URLError, OSError, TimeoutError, ValueError, json.JSONDecodeError):
        return False
    return bool(
        isinstance(payload, dict)
        and payload.get("ok") is True
        and payload.get("managed_service") is True
    )


def _validate_loopback_host(host: str) -> None:
    normalized = host.strip().casefold()
    if normalized == "localhost":
        return
    try:
        address = ipaddress.ip_address(normalized)
    except ValueError as exc:
        raise WorkbenchServiceError(
            "Workbench services are loopback-only; use 127.0.0.1 or localhost"
        ) from exc
    if address.version != 4 or not address.is_loopback:
        raise WorkbenchServiceError(
            "Workbench services are loopback-only; use 127.0.0.1 or localhost"
        )


def _launchd_domain() -> str:
    if not hasattr(os, "getuid"):
        raise WorkbenchServiceError("launchd user services require a Unix user ID")
    return f"gui/{os.getuid()}"


def _require_systemd_user() -> None:
    result = _run(("systemctl", "--user", "show-environment"), check=False)
    if result.returncode != 0:
        detail = _result_detail(result)
        raise WorkbenchServiceError(
            "systemd user services are unavailable for this login session"
            + (f": {detail}" if detail else "")
        )


def _windows_user_id() -> str:
    result = _run(("whoami.exe",), check=False)
    value = result.stdout.strip()
    if result.returncode == 0 and value:
        return value
    domain = os.environ.get("USERDOMAIN", "").strip()
    username = os.environ.get("USERNAME", "").strip()
    if username:
        return f"{domain}\\{username}" if domain else username
    raise WorkbenchServiceError("could not determine the current Windows user for Task Scheduler")


def _run(command: Sequence[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            list(command),
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError as exc:
        raise WorkbenchServiceError(f"could not run {command[0]}: {exc}") from exc
    if check and result.returncode != 0:
        detail = _result_detail(result)
        raise WorkbenchServiceError(
            f"{command[0]} failed with exit code {result.returncode}"
            + (f": {detail}" if detail else "")
        )
    return result


def _result_detail(result: subprocess.CompletedProcess[str]) -> str:
    return (result.stderr.strip() or result.stdout.strip()).splitlines()[-1][:500] if (
        result.stderr.strip() or result.stdout.strip()
    ) else ""


def _atomic_write(path: Path, payload: bytes, *, mode: int) -> None:
    _ensure_private_dir(path.parent)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary_path = Path(temporary_name)
    try:
        if os.name != "nt" and hasattr(os, "fchmod"):
            os.fchmod(fd, mode)
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
        if os.name != "nt":
            path.chmod(mode)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def _ensure_private_dir(path: Path) -> None:
    existed = path.exists()
    path.mkdir(parents=True, exist_ok=True)
    if os.name != "nt" and not existed:
        path.chmod(0o700)


def _write_metadata(payload: dict[str, Any]) -> None:
    serialized = (json.dumps(payload, indent=2, sort_keys=True) + "\n").encode("utf-8")
    _atomic_write(service_metadata_path(), serialized, mode=0o600)


def _read_metadata() -> dict[str, Any] | None:
    path = service_metadata_path()
    if not path.exists():
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(value, dict) or value.get("schema_version") != SERVICE_SCHEMA_VERSION:
        return None
    return value


def _managed_bookmark_path(metadata: dict[str, Any] | None) -> Path:
    raw_config_home = metadata.get("config_home") if metadata else None
    if isinstance(raw_config_home, str) and raw_config_home.strip():
        return Path(raw_config_home).expanduser().resolve() / "workbench.html"
    return registry_dir() / "workbench.html"
