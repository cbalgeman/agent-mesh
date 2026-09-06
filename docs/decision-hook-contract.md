# Decision Edit/Write Hook Contract

Agent Mesh exposes an optional provider-neutral hook boundary for agent harnesses
that can observe edits or writes. The package does not install hooks, launch a
provider, or block edits. A harness invokes the CLI when a task first touches a
path that was not included in its earlier task preflight.

For compact human or prompt context, task preflight also supports:

```bash
agent-q decisions preflight --path src/example.py --digest
```

The digest is a bounded projection of the machine-readable JSON. It includes the
decision title, one-line rule when authored, canonical tier and validity,
effective advisory state, path match, body digest, and verification commands.
The JSON envelope remains the interoperable contract and source for automation.

## Invocation

Run `agent-q decisions hook` from the adopted repository and send exactly one
UTF-8 JSON object on standard input. The request is bounded to 262,144 bytes.
The command writes one JSON object to standard output when decision context is
complete, incomplete, or unavailable.

```json
{
  "schema": "agent-mesh.decision-hook-request.v1",
  "boundary": "edit",
  "paths": ["src/agent_mesh/workbench.py"]
}
```

The request fields are intentionally small and closed:

| Field | Required | Contract |
|---|---:|---|
| `schema` | yes | Exactly `agent-mesh.decision-hook-request.v1` |
| `boundary` | yes | `edit` or `write` |
| `paths` | yes | Non-empty array of repository-relative path strings |

Unknown or duplicate fields, malformed JSON, escaped lone surrogates, absolute
paths, parent traversal, NUL bytes, and over-limit input are invalid. Paths use
the same lexical normalization, deduplication, sorting, UTF-8 validation, and
bounds as task preflight. The hook does not resolve symlinks or touch the
filesystem to normalize a candidate path.

## Output and exit codes

A valid request returns the same `agent-mesh.decision-context.v1` object as
`agent-q decisions preflight --json`. The only intentional difference is
`request.boundary`, which echoes `edit` or `write` instead of `task`.

| Exit | Meaning | Standard output |
|---:|---|---|
| `0` | Complete advisory context, including a valid empty decision list | One complete decision-context JSON object |
| `2` | Invalid invocation or request | Empty; bounded diagnostic on standard error |
| `3` | Context unavailable or incomplete | One decision-context JSON object with `complete=false` |

Exit `3` never means that no decision applies. In the 0.4.0 advisory policy, a
harness surfaces exit `2` or `3` to the agent or human but does not block the
edit merely because the hook could not establish complete context. Required-mode
blocking needs a separate human-approved policy.

Hook output is `project_private`. A harness must not copy it outside the local
process boundary unless that destination is authorized for project-private
context. Default output uses human-readable decision IDs and omits raw prompts,
attachments, evidence bodies, credentials, and internal `dec_` identifiers.

## Harness responsibilities

The harness, not Agent Mesh, owns provider integration and task-local state:

1. Run task preflight for the initial planned path set.
2. Keep the explicit repository root, the returned `repository.store_id`, and a
   task-local set of paths already preflighted.
3. On the first edit or write to a new path, call `agent-q decisions hook` with
   that path from the explicit repository root before performing the change when
   the harness supports a pre-edit boundary.
4. Add the path to the seen set only after a complete exit `0`; retry or surface
   incomplete context explicitly after exit `3`.
5. Reject a complete response whose `repository.store_id` is absent or differs
   from the initial task preflight. Never mark that path seen.
6. Inject only complete, bounded context into the agent session.
7. Never translate `effective_enforcement=advisory` into a blocking decision.

The reference adapter in `examples/decision-hook/harness.py` demonstrates this
task-local seen-path behavior without depending on a particular provider or
agent runtime. It uses strict UTF-8 subprocess I/O, accepts only exits `0`, `2`,
and `3`, normalizes paths through the shared lexical normalizer, and requires a
successful response to echo the exact normalized path before marking it seen.
It binds subprocess execution to an explicit repository root, verifies every
complete response against the initial preflight's store ID, rejects context
output above 262,144 bytes, truncates retained
diagnostics to 1,024 bytes, and applies a configurable timeout that defaults to
10 seconds. Spawn, decode, timeout, and contract-validation exceptions remain
advisory failures for the integrating harness to surface.

## Claude Code PreToolUse recipe

`examples/decision-hook/claude_pretooluse.py` is a thin Claude-specific recipe
over the provider-neutral task preflight. Copy it to
`.claude/hooks/agent-mesh-pretooluse.py`, make it executable, and register it in
`.claude/settings.json`:

```json
{
  "hooks": {
    "PreToolUse": [
      {
        "matcher": "Edit|Write",
        "hooks": [
          {
            "type": "command",
            "command": "python3 \"$CLAUDE_PROJECT_DIR\"/.claude/hooks/agent-mesh-pretooluse.py"
          }
        ]
      }
    ]
  }
}
```

The adapter reads Claude's `tool_input.file_path`, requires it to remain under
`CLAUDE_PROJECT_DIR`, invokes `agent-q` as explicit argv from that repository,
and returns the digest through `hookSpecificOutput.additionalContext`. It never
returns a permission decision. Invalid or incomplete context becomes a visible
advisory reminder while the 0.4.0 policy leaves the edit unblocked. This follows
Claude Code's current documented `PreToolUse` JSON contract; verify the external
hook contract when upgrading Claude Code:
<https://code.claude.com/docs/en/hooks#pretooluse>.

## Shell example

```bash
printf '%s\n' \
  '{"schema":"agent-mesh.decision-hook-request.v1","boundary":"write","paths":["src/example.py"]}' \
  | agent-q decisions hook
```

The command is read-only: it uses one hash-chain-verified snapshot and does not
write the event log, SQLite projection, views, locks, journals, or Git state. It
does not execute stored verification definitions, invoke a shell, access the
network, install a hook, accept a decision, or create canonical read events.
