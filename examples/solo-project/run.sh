#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
WORKDIR="${WORKDIR:-$(mktemp -d "${TMPDIR:-/tmp}/agent-mesh-solo.XXXXXX")}"
PYTHON="${PYTHON:-$(command -v python3 || command -v python)}"

export PYTHONPATH="$ROOT/src${PYTHONPATH:+:$PYTHONPATH}"

mkdir -p "$WORKDIR"
cd "$WORKDIR"

"$PYTHON" -m agent_mesh.cli.mail init --participants agent-a --default-sender agent-a --default-recipient agent-a --no-register >/dev/null
REQ="$("$PYTHON" -m agent_mesh.cli.mail request --from agent-a --to agent-a "Solo project review" "Self-addressed request in a one-agent project.")"
"$PYTHON" -m agent_mesh.cli.mail respond --from agent-a "$REQ" "Solo response" "The one-agent flow works." >/dev/null
"$PYTHON" -m agent_mesh.cli.q render --all >/dev/null
"$PYTHON" -m agent_mesh.cli.q verify-chain .agent-mesh/events.jsonl >/dev/null

test -f .agent-mesh/views/inbox.md
test -f .agent-mesh/views/outbox-agent-a.md
test "$(find .agent-mesh/views -maxdepth 1 -name 'outbox-*.md' | wc -l | tr -d ' ')" = "1"
"$PYTHON" -m agent_mesh.cli.q list --status open | grep "$REQ" >/dev/null

echo "solo-project ok: $WORKDIR"
