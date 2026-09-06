# Durable AI-Agent Instance Handles

Agent Mesh distinguishes parallel AI-agent work contexts without turning personas,
display names, providers, or runtime profiles into extra identities. The instance
contract has exactly two identity layers:

| Layer | Example | Visibility |
|---|---|---|
| Public instance handle | `claude-design` | Normal CLI, Workbench, packets, views, routing, and ownership |
| Internal instance ID | `AI-20260820-01` | Canonical attribution and explicit diagnostics only |

The public handle is `<participant>-<durable-role>`. `claude` is the participant
and `design` is the durable role in `claude-design`. Participant, provider,
runtime profile, model, permissions, authentication, and billing remain separate
registration facts; none is a parallel public identity. Agent Mesh does not add a
persona or display-name layer.

Within one project, normal surfaces show only the bare handle. An aggregated
cross-project presentation qualifies it as `<project-key>/<handle>`. Internal
`AI-...` IDs are project-local and must not be used with normal `--instance`,
`--to-instance`, or `--owner-instance` options.

## Automatic identity handshake

A trusted runtime integration performs the normal startup handshake. It reports
the participant, provider, durable role, runtime profile, runtime security facts,
whether provider continuity is resumable, and one lifecycle disposition:

- `new`: authoritative evidence that this is a new work context;
- `resumed`: authoritative evidence that provider context was resumed;
- `unknown`: no authoritative continuity signal.

Agent Mesh then resolves continuity deterministically:

- authoritative `new` allocates a new internal instance and public handle;
- authoritative `resumed` must match exactly one stored, resumable binding;
- `unknown` continues without a prompt when zero or one viable candidate exists;
- multiple viable candidates stop startup and report public handles for a human
  choice;
- contradictions stop before canonical mutation.

For exact-resumable managed Dispatch, the resolved wave is the durable
workstream. Repeated dispatches in the same wave resume the same active instance
and provider context. A different wave cannot become a resume candidate and is
allocated a distinct workstream-qualified handle when the base handle is already
reserved. This prevents both suffix churn within one development wave and
context leakage between unrelated waves.

A launch-attempt key is optional. When supplied, retrying the same handshake is
idempotent, including a reference-less one-shot launch. Reusing that key with
changed session evidence or a changed lifecycle disposition is rejected. Without
a key, Agent Mesh makes no retry-idempotency claim.

The runtime integration is also the registrar. Agent Mesh records the actual
configured adapter and trust source; it does not invent a provider or runtime
identity from process names.

Managed integrations resolve through the runtime-family driver registry
described in `docs/runtime-adapter-contract.md`. The registry keys runtime
families rather than models, and CLI/core dispatch contain no provider-specific
factory branch. Agent Mesh currently ships only the Codex driver. Other providers
can participate when started externally, or a project can explicitly select a
digest-pinned, one-shot V1 local driver whose canonical trust source remains
`project-local`. Exact/resumable continuity remains unavailable until Agent Mesh
ships and reviews the corresponding built-in driver.

For `openai` + `codex-cli`, an exact resumable profile uses Codex app-server over
stdio. The trusted handshake starts a provider thread when no compatible canonical
instance exists. Codex 0.147 materializes that persisted thread at its first turn,
so the same child-bound app-server remains alive through canonical registration and
the first managed turn. On a later process invocation, Agent Mesh enumerates
bounded Codex thread metadata through app-server for the exact project root,
hashes each provider thread ID in the project scope, and resumes only the single
thread whose digest matches the
single compatible active instance. A missing provider thread, multiple canonical
candidates, an already attached non-concurrent instance, or any returned
thread/model/root mismatch stops before identity registration, lease acquisition,
or process launch.

Before creating a provider thread, Agent Mesh records one SHA-256 digest of the
bounded project-root thread inventory in `dispatch_run_planned`. If the dispatcher
dies after `thread/start` but before registration and the provider still lists an
inventory addition, the retry cannot prove that addition belongs to this run; it
fails closed without binding or creating another thread. Any concurrent or
ambiguous visible inventory change has the same result. If the provider no longer
lists the unmaterialized, no-turn thread and the complete inventory still matches
the planned digest, retry may create a fresh new session; it does not claim that
the inaccessible entry was resumed or deleted. Provider-side retention of such an
unlisted entry remains outside Agent Mesh's proof. A new thread's handshake
app-server is pre-bound to the exact
child handle and remains alive for the first turn; an existing persisted thread is
resumed in a fresh app-server bound to the resolved child public handle. The exact
provider thread ID travels only in JSON-RPC stdin. The raw ID is held only
in private adapter memory, is redacted from provider output and errors, and never
appears in argv, environment, canonical state, launch metadata, or diagnostics.
The turn is tagged provider-side with the public dispatch run ID. A retry recovers
that completed turn. An in-progress turn or transport loss after submission returns
an observation-pending result while preserving the active lease, instead of
submitting the same provider turn twice.

## Bind and address work by handle

Managed launchers inject the child binding automatically. For an already-running
unmanaged process, bind the public handle before issuing Agent Mesh commands:

```bash
export AGENT_MESH_INSTANCE_ID=claude-case-study
agent-mesh backlog create --actor claude --title "Audit heading tokens"
```

Alternatively, put the global handle before each command:

```bash
agent-mesh --instance claude-case-study backlog create \
  --actor claude --title "Audit heading tokens"
```

Once a participant has an active instance, an unbound canonical write from that
participant fails closed. A bound handle must belong to the event's `--from` or
`--actor` participant. This prevents provider-only attribution and accidental
fallback to the human default sender.

Hand a request or backlog item to another active instance using its handle:

```bash
agent-mesh --instance claude-case-study request \
  --from claude \
  --to-instance claude-design \
  "Fix inconsistent card spacing" \
  "The case-study layout exposes a design-system token mismatch."

agent-mesh --instance claude-case-study backlog create \
  --actor claude \
  --owner-instance claude-design \
  --title "Normalize card spacing tokens"

agent-q list --to-instance claude-design --status open
agent-q backlog list --owner-instance claude-design
```

Packets and views render the sender and targeted recipients as handles, not as a
participant plus a second instance field. Generic runtime dispatch intentionally
does not launch work addressed to a specific existing chat; the named chat
retrieves the canonical packet with `agent-q packet --id <REQ-id>`.

### Explicit cross-project mapping

Instance IDs and provider-session digests are project-local. A write that crosses
repository boundaries therefore requires an explicit mapping in the target
project's `.agent-mesh/config.toml`:

```toml
[identity.cross_project_instance_mappings]
"source-project/claude-design" = "claude-design-target"
```

The key is the source `<project-key>/<handle>` and the value is the target-local
handle. Both instances must be active and belong to the same participant. This
target-side opt-in prevents matching labels, internal IDs, or session digests from
silently linking identities across projects.

## Managed child lifecycle

Each managed child launch receives a new child instance binding. The final
launcher boundary strips inherited Agent Mesh identity and reserved registrar or
provider credentials, then injects only the child's public handle. The child's
canonical events are attributed to that child, not to the dispatcher parent.

One-shot children become terminal after `completed`, `failed`, `timeout`, or
`launch_error`. Release and terminal suffixes are ordered, retry-safe after a
crash, and bound to the dispatch run's exact terminal result: `TimeoutError`
cannot replay as `launch_error` or generic `failed`, and `LaunchError` cannot
replay as `timeout`. Recovery preserves the observed run outcome. Resumable
instances remain available only when the configured adapter can prove provider
continuity. Retiring an instance prevents new attribution and addressing while
preserving history.

If a new resumable handshake creates a provider thread but execution fails before
the canonical identity is registered, the adapter makes a bounded best-effort
`thread/delete` cleanup and always discards the raw reference from memory. A hard
process death after thread creation but before its first turn may leave an
unmaterialized provider entry. Before registration, an ambiguous visible inventory
delta requires manual reconciliation because ownership cannot be proven. If no
entry is listed and the inventory is unchanged, a retry can create a fresh no-turn
session, while making no deletion or continuity claim about an inaccessible
provider entry. After registration, a missing or unresumable exact bound thread
fails closed for manual reconciliation. No replacement provider turn is submitted
in an ambiguous canonical state. Once a first turn has materialized the canonical
binding, launch recovery uses the exact provider session and run marker rather than
silently replacing its context.

Unmanaged descendants are outside this guarantee. A process started outside the
managed launcher does not become a distinct child merely because it inherited a
shell or provider context. The dispatch execution boundary will not start an
agent adapter that lacks the automatic identity handshake, even for a launch-only
run that will not post a response.

## Inspect and manage public state

```bash
agent-q instances list --participant claude
agent-q instances show claude-design

agent-mesh instance update claude-design \
  --handle claude-design-system \
  --reason "Clarify the durable role"

agent-mesh instance retire claude-design-system \
  --reason "The work context was closed"
```

Normal inspection exposes the handle, lifecycle status, provider, runtime
profile, resumability, adapter trust, binding source, and terminal outcome.
`agent-q instances show <AI-id> --diagnostic` is the explicit diagnostic escape
hatch for the hidden internal ID.

## Manual compatibility fallback

Automatic handshake is required for dispatch launch. If a trusted integration is
unavailable, a human may register an already-running unmanaged instance
explicitly for attribution and addressing:

```bash
agent-mesh instance register \
  --participant claude \
  --provider anthropic \
  --handle claude-design \
  --workstream design-system \
  --actor human \
  --external-session-ref-digest <project-scoped-sha256>
```

`instance register` is the sole manual unbound bootstrap: it records
`registration_origin = manual` and the named registrar even when that actor
already has active instances. It does not attribute the new handle to another
chat and it does not weaken later writes. After registration, commands authored
by a participant with active instances still require that command's correct
public handle through `--instance` or `AGENT_MESH_INSTANCE_ID`.

Compute the digest in a private provider-side integration. Never place a raw
provider session reference in command arguments, environment variables, logs,
canonical state, Workbench diagnostics, or launch metadata.

## Security and context boundary

Instance attribution is workflow enforcement, not cryptographic authentication.
Repository permissions, provider authentication, executable/runtime profiles,
and human review remain separate controls. An internal ID does not preserve a
provider context window, copy ordinary chat, or authenticate a process.

Only canonical Agent Mesh records receive internal instance attribution.
Ordinary chat is not logged automatically; promote concise durable requests and
material results instead of mirroring transcripts for continuity.
