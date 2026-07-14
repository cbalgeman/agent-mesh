#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
WORKDIR="${WORKDIR:-$(mktemp -d "${TMPDIR:-/tmp}/agent-mesh-n-agent.XXXXXX")}"
PYTHON="${PYTHON:-$(command -v python3 || command -v python)}"
N="${1:-${N:-1}}"

export PYTHONPATH="$ROOT/src${PYTHONPATH:+:$PYTHONPATH}"
export AGENT_MESH_EXAMPLE_WORKDIR="$WORKDIR"

"$PYTHON" "$(dirname "$0")/scenario.py" "$N"
