"""Minimal Workbench bootstrap that fingerprints code before server modules import."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Sequence

from agent_mesh.workbench_freshness import WorkbenchCodeError, workbench_code_fingerprint


SUPERVISED_RESTART_EXIT_CODE = 75


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="agent-mesh-workbench-runner")
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--open", action="store_true")
    parser.add_argument("--managed-service", action="store_true")
    parser.add_argument("--config-home", type=Path)
    parser.add_argument("--ownership-generation", default="")
    parser.add_argument("--ownership-revision", type=int, default=0)
    parser.add_argument("--launchd-socket-name", default="")
    args = parser.parse_args(argv)

    if args.managed_service:
        os.environ["AGENT_MESH_WORKBENCH_SERVICE"] = "1"
        if args.config_home is not None:
            os.environ["AGENT_MESH_CONFIG_HOME"] = str(args.config_home.expanduser().resolve())
    try:
        expected_code_fingerprint = workbench_code_fingerprint()
    except WorkbenchCodeError as exc:
        print(f"agent-mesh: {exc}", file=sys.stderr)
        return 2

    from agent_mesh.config import ConfigError, load_config
    from agent_mesh.project_registry import ProjectRegistryError, list_registered_projects
    from agent_mesh.workbench import (
        WorkbenchError,
        WorkbenchServeOutcome,
        _validate_workbench_host,
        serve_workbench,
    )

    try:
        repo = args.repo
        if args.managed_service:
            try:
                load_config(repo)
            except ConfigError:
                projects = list_registered_projects()
                if not projects:
                    raise
                repo = projects[0].root
        _validate_workbench_host(args.host)
        outcome = serve_workbench(
            repo=repo,
            host=args.host,
            port=args.port,
            open_browser=args.open,
            expected_code_fingerprint=expected_code_fingerprint,
            ownership_generation=args.ownership_generation,
            ownership_revision=args.ownership_revision,
            launchd_socket_name=args.launchd_socket_name,
        )
    except (ConfigError, ProjectRegistryError, WorkbenchError) as exc:
        print(f"agent-mesh: {exc}", file=sys.stderr)
        return 2
    if outcome is WorkbenchServeOutcome.RESTART_REQUESTED:
        return SUPERVISED_RESTART_EXIT_CODE
    return 0


if __name__ == "__main__":  # pragma: no cover - module execution boundary
    raise SystemExit(main())
