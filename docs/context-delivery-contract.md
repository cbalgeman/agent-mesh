# Canonical context delivery contract

Agent Mesh exposes a zero-hook bootstrap boundary for harnesses that need to
discover canonical repository state without copying decision meanings into
prompts, hooks, or model memory:

```bash
agent-q context bootstrap --pretty
```

The command is bounded, mutation-free, provider-neutral, and project-private.
It does not install integration files, launch a model, restore a provider
context window, accept a decision, or claim that an installed harness invokes
any lifecycle event.

## Exit contract

| Exit | Meaning |
|---|---|
| `0` | Bootstrap retrieval is complete. Delivery capabilities may still all be `unreported`. |
| `2` | An explicit prior cursor, self-report, or mapping fixture is invalid. Standard output is empty. |
| `3` | Canonical retrieval, fixed-target inspection, or bounded output is unavailable or incomplete. The JSON envelope says why. |

`complete` describes bounded retrieval only. It is independent of managed
contract health, protocol availability, and harness delivery state. Missing
optional lifecycle integration does not change `agent-mesh adopt --check`
health or its exit code.

## Bootstrap envelope

`agent-mesh.context-bootstrap.v1` contains:

- canonical repository provenance from one stable, fully hash-chain-verified
  `events.jsonl` snapshot;
- bounded inspection of only `AGENTS.md` and `CLAUDE.md`;
- the full managed-contract body SHA-256 plus its 12-character display digest;
- a structured freshness cursor and optional comparison;
- protocol vocabulary and explicit non-claims;
- a delivery self-report and, only when a validator ran, a separate
  point-in-time verification receipt;
- bounded warnings and diagnostics.

It never includes decision prose, copied decision glosses, provider prompts,
provider session identifiers, credentials, or hand-maintained project-domain
values. Retrieve decision meaning with `agent-q decisions preflight` and
resolve cited records with `agent-q refs resolve`.

The envelope is `project_private`. Its hashes are comparison metadata, not
secrets, signatures, credentials, nonces, or authorization. They can
fingerprint a repository and should not be copied into a public prompt or log.

## Fixed-target inspection

The bootstrap inspector never calls the recursive legacy-conflict scan. It
performs fixed, no-follow reads of `AGENTS.md` and `CLAUDE.md`, with before and
after file-identity checks. An `lstat` existence check on `.claude` determines
whether `CLAUDE.md` is required; the directory is never traversed.

Each target records:

```json
{
  "target": "agents",
  "path": "AGENTS.md",
  "required": true,
  "status": "current",
  "content_sha256": "<64 lowercase hex or null>"
}
```

Statuses are `current`, `missing`, `stale`, `malformed`, `unsafe`,
`unreadable`, `oversize`, or `changing`. Unsafe, unreadable, oversize, or
changing required targets make the bootstrap incomplete. `missing` and `stale`
are successfully observed states; managed-contract health reports them
separately.

An incomplete fixed-target inspection emits `freshness.cursor=null` and
`freshness.comparison=null`; it never reports `unchanged`. Platforms without a
safe no-follow open primitive fail closed as `unsafe` instead of falling back
to a race-prone ordinary open.

Ceilings are 1 MiB per target, 2 MiB total, three stability attempts, and two
seconds of fixed-target inspection within the overall command deadline.

## Freshness cursor

The cursor schema is `agent-mesh.context-bootstrap-cursor.v1`; its domain is
`agent-mesh.context-bootstrap.freshness`. It contains only closed fields:

- `store_id`;
- `event_seq`;
- `source_log_sha256`;
- `tail_event_sha256` computed from the exact stable event bytes verified by
  the full hash-chain walk;
- managed-contract body SHA-256 and fixed target requiredness, status, and
  content SHA-256;
- `cursor_sha256`.

The cursor hash is:

```text
SHA256(
  UTF8("agent-mesh.context-bootstrap.freshness")
  + NUL
  + canonical_json(cursor_without_cursor_sha256)
)
```

Canonical JSON uses UTF-8, sorted keys, fixed compact separators, and rejects
non-finite numbers. A prior cursor is limited to 16 KiB and rejects duplicate
or unknown fields, invalid types or lowercase digests, excessive JSON nesting,
and a mismatched self-hash. At `event_seq=0`, the source hash must be SHA-256 of
the empty log and the tail must be the canonical sentinel hash.

Compare with a saved cursor:

```bash
agent-q context bootstrap --prior-cursor .private/agent-mesh-cursor.json --pretty
```

Comparison produces `refresh_required`, ordered `reasons`, and
`prefix_proven`. Reasons are:

- `unchanged`;
- `store_mismatch`;
- `canonical_state_advanced`, only when the prior tail matches the current
  verified successor record's `prev_event_hash`;
- `canonical_state_rolled_back`;
- `canonical_history_changed` for same-height or non-prefix divergence;
- `contract_changed`, independently added when managed-contract or fixed-target
  evidence changes.

Every canonical event conservatively changes freshness. The comparison proves
only what follows from the current verified snapshot and forgeable prior
metadata.

## Delivery self-report

A harness may supply a bounded closed self-report:

```bash
agent-q context bootstrap --report .private/context-delivery-report.json
```

The `agent-mesh.context-delivery-report.v1` input contains an adapter ID and
version plus zero or more capability entries. Self-reported states are only
`unsupported` or `reported`; omitted capabilities normalize to `unreported`.
Input that claims `verified`, includes fingerprints or scopes, attaches a
receipt, or adds unknown fields is invalid.

The fixed capability vocabulary is:

```text
session_bootstrap
task_preflight
pre_edit
pre_write
resume_refresh
compaction_refresh
reference_resolution
change_review
```

`reported` is not evidence that an installed lifecycle event ran. `verified`
is reserved for validator-produced installed-vertical evidence; this first
slice makes no installed-provider claim.

The envelope's `protocols` array is the portable command-discovery seam. Every
capability has a closed `protocol_id`, argv prefix, complete-versus-prefix
classification, input transport and schema, required dynamic arguments, and
output schema. These records prevent each harness from rediscovering the CLI
mapping, but do not prove that the harness invokes it.

## Static mapping fixtures and receipts

The installed package includes closed built-in schema fixtures for Claude,
Codex through Hermes, and a generic local harness:

```bash
agent-q context bootstrap --builtin-mapping claude --pretty
```

The GitHub repository also publishes matching reviewable JSON examples under
`examples/context-delivery/`; those paths are not required by wheel-only
installations. `--mapping <path>` validates one of those exact closed fixture
definitions.

Each `agent-mesh.context-delivery-mapping.v1` fixture lists every capability
exactly once and uses argv from a fixed provider-neutral allowlist. It cannot
contain a shell command, environment assignment, path, decision ID or meaning,
mutable project value, credential, prompt, or provider session reference.

Agent Mesh hashes the exact stable fixture bytes, independently fingerprints
the installed context-delivery validator artifact, and emits a separate
`agent-mesh.context-delivery-verification.v1` schema-fixture receipt with a UTC
time, separate fixture and adapter fingerprints, named closed-schema,
capability-command-binding, fixture-fingerprint, and adapter-fingerprint
checks, and an evidence hash. Callers cannot supply either receipt fingerprint.
The receipt verifies mapping schema only. Its verified-capability list is
empty, its runtime is null, and it is not accepted later as continuing proof.

Installed-vertical verification, persistent receipts, runtime-driver coupling,
automatic installation, and mandatory enforcement are deliberately deferred.

## Global bounds and limitations

- canonical snapshot: 64 MiB, 50,000 events, 10 seconds;
- prior cursor: 16 KiB;
- report or mapping fixture: 64 KiB;
- rendered JSON: 256 KiB;
- all explicit files use stable, no-follow reads.

The command detects stale context only when invoked. It cannot erase stale
provider context, interpret surrounding prose, guarantee a model read the
result, or compensate for lifecycle events a harness does not expose. The
supported baseline remains the managed contract plus explicit CLI retrieval;
hooks and adapters are optional delivery conveniences.
