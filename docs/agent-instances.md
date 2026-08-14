# Long-Lived AI-Agent Instances

Agent Mesh can give each long-running AI chat or work session a durable,
project-local identity. This is useful when several instances use the same
participant and provider but own different workstreams—for example,
`claude-case-study` and `claude-design` in one repository.

An instance is distinct from the other identity layers:

| Layer | Example | Meaning |
|---|---|---|
| Participant | `claude` | Durable sender/recipient identity in project coordination |
| Provider | `anthropic` | Model service associated with the instance |
| Runtime profile | `claude_review` | Optional executable, model, permissions, and billing contract |
| Agent instance | `AI-20260813-01`, label `claude-design` | One addressable, long-lived chat or work instance |

The generated `AI-YYYYMMDD-NN` ID is stable until explicitly retired. Labels
are human-friendly aliases and can change without invalidating an earlier
label. The registry is event-backed, so it rebuilds from the canonical event
log rather than depending on a process, terminal, or SQLite database surviving.

## Register and bind an instance

Register an instance once from the adopted repository:

```bash
agent-mesh instance register \
  --participant claude \
  --provider anthropic \
  --label claude-design \
  --workstream design-system \
  --external-session-ref "$CLAUDE_SESSION_ID"
```

The provider session reference is optional. Agent Mesh stores only its SHA-256
digest so the raw provider identifier does not enter canonical state. The
command prints the allocated Agent Mesh ID.

Bind every later Agent Mesh write from that chat by setting an environment
variable in the shell **before launching or resuming the agent process**:

```bash
export AGENT_MESH_INSTANCE_ID=AI-20260813-01
# launch or resume the intended agent from this shell
```

If the chat cannot retain an environment variable, put the global option before
the subcommand on every invocation:

```bash
agent-mesh --instance claude-design backlog create \
  --actor claude --title "Audit heading tokens"
```

An agent command cannot retroactively modify its parent process environment. If
the chat is already running without the binding, use global `--instance` on
every Agent Mesh command or restart it from a bound shell. The stable ID belongs
to Agent Mesh, not to the provider. A multi-day or resumed chat keeps the same
identity only when its launch environment, wrapper, or instructions continue
supplying that same ID or label. Agent Mesh cannot infer that two provider chat
windows are the same instance, and it does not preserve or extend the provider's
context window.

Once a participant has at least one active registered instance, Agent Mesh
fails closed on new canonical events from that participant when no instance is
bound. It also rejects a bound instance when the event's `--from` or `--actor`
participant does not own it. This prevents silent fallback to provider-only
attribution or accidental use of the human default sender. Where an event also
carries a compatibility authorship field such as `payload.from`, that field
must match the canonical event-envelope actor; projections and dispatch trust
the envelope actor.

The same human-readable label may be registered in multiple adopted projects.
A cross-repository command resolves that label independently in each project,
so each target log records its own project-local instance ID. The label must map
to the same participant, provider, and external-session digest in both projects.
Because project-local `AI-...` IDs can collide, ID-form bindings are rejected
for cross-project writes; bind the shared label instead.

## Hand work to another instance

A case-study instance can send a durable request directly to the design-system
instance even though both use the `claude` participant:

```bash
agent-mesh --instance claude-case-study request \
  --from claude \
  --to-instance claude-design \
  "Fix inconsistent card spacing" \
  "The case-study layout exposes a design-system token mismatch."
```

The request retains `claude` as its participant recipient and adds the stable
target instance ID. The named instance can retrieve its queue and reply:

```bash
agent-q list --to-instance claude-design --status open
agent-q packet --id <REQ-id>
agent-mesh --instance claude-design reply --from claude \
  <REQ-id> "Accepted" "The design-system work is now tracked."
```

When the finding is already suitable for durable backlog work, assign it
directly:

```bash
agent-mesh --instance claude-case-study backlog create \
  --actor claude \
  --owner-instance claude-design \
  --title "Normalize card spacing tokens" \
  --status open --lane next-up --priority P1

agent-q backlog list --owner-instance claude-design
```

Requests addressed to a specific existing instance are intentionally excluded
from generic runtime dispatch. A dispatcher cannot safely recreate or select a
particular provider chat. The human or provider-side workflow relays the REQ ID
to that chat, which then retrieves the canonical packet.

## Inspect, rename, and retire

```bash
agent-q instances list --participant claude
agent-q instances show claude-design

agent-mesh instance update claude-design \
  --label claude-design-system \
  --reason "Clarify the workstream name"

agent-mesh instance retire claude-design-system \
  --reason "The chat was closed"
```

Retirement prevents new attribution or addressing but preserves historical
events and aliases. Create a new instance for a replacement chat instead of
reusing the retired identity.

## Security and context boundary

Instance attribution is workflow enforcement, not cryptographic authentication.
Anyone who can write as a participant and knows an active instance ID may claim
it. Repository permissions, provider authentication, executable/runtime
profiles, and human review remain separate controls.

Only canonical Agent Mesh records receive `actor_instance_id`. Ordinary chat is
not logged automatically. Promote concise durable requests and material results;
do not mirror full transcripts just to maintain instance continuity.
