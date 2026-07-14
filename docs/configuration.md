# Configuration

This page documents project-local `.agent-mesh/config.toml` changes. `agent-mesh`
configuration is intentionally repo-scoped: package defaults stay generic, and each
consumer project decides which humans/agents, aliases, and compatibility views it wants.

## Choose the project identity used in IDs

`[project].default_sender` is the identity used when `agent-mesh request`,
`agent-mesh reply`, feedback submission, or the workbench omit an explicit `--from`
or sender. New public IDs include this identity in uppercase:

```text
REQ-20260708T210852Z-HUMAN-21697
RES-20260708T211402Z-CODEX-03842
```

Pick this value intentionally during onboarding. A personal project can use a real
name or handle such as `david`; a reusable package example should usually use a
generic alias such as `human` or `user`. The value must be listed in
`[agents].participants` so writes can be validated. Changing `default_sender`
affects only future writes; existing REQ/RES IDs and the event hash chain are not
rewritten.

## Add an agent to an existing project

Adding a participant is a config edit plus a projection rebuild. It must not rewrite
`.agent-mesh/events.jsonl`, migrate historical events, or copy project-specific wrapper
logic into the package.

1. Edit `.agent-mesh/config.toml`.
2. Add the new agent name to `[agents].participants`.
3. Optionally update `[project].default_sender` / `[project].default_recipient`;
   remember that `default_sender` becomes the uppercase sender segment in new
   REQ/RES IDs.
4. Optionally add or update `[routing.aliases]` for multi-recipient groups.
5. Optionally add compatibility-view paths if a legacy project needs generated markdown
   views for that agent.
6. Rebuild/render derived state and verify the chain.

Example project-local configuration:

```toml
schema_version = 1

[project]
name = "example-project"
default_sender = "human"
default_recipient = "builder"

[agents]
participants = ["human", "builder", "reviewer", "observer"]

[features]
hash_chain = true
body_externalization = false

[paths]
events_log = ".agent-mesh/events.jsonl"
db = ".agent-mesh/messages.db"
views_dir = ".agent-mesh/views"
archive_dir = ".agent-mesh/archive"
bodies_dir = ".agent-mesh/bodies"

[routing]
preserve_raw_to = true

[routing.aliases]
reviewers = ["reviewer", "observer"]
all = ["builder", "reviewer", "observer"]

[checks]
exempt_paths = [".agent-mesh/**", ".git/**", "**/__pycache__/**", "build/**", "dist/**"]

[compatibility_views]
inbox = "docs/coordination-shadow/inbox.md"
message_log = "docs/coordination-shadow/message-log.md"
archive_dir = "docs/coordination-shadow/archive"

[compatibility_views.outbox]
builder = "docs/coordination-shadow/outbox-builder.md"
reviewer = "docs/coordination-shadow/outbox-reviewer.md"
observer = "docs/coordination-shadow/outbox-observer.md"
```

Compatibility views are projections, not source of truth. Use them only when a project
needs a legacy markdown surface. The canonical state remains `.agent-mesh/events.jsonl`
plus rebuildable SQLite/views under `.agent-mesh/`.

After editing config, run from the project root:

```bash
agent-q rebuild --all
agent-q render --all
agent-q verify-chain .agent-mesh/events.jsonl
```

Smoke test the new participant without relying on legacy files:

```bash
agent-mesh request --from human --to observer "Smoke test" "Please reply APPROVED."
agent-mesh reply --from observer <REQ-id> "APPROVED" "Visible."
agent-q list --status open
agent-q locate <REQ-id>
```

Routing aliases expand at write time. This example sends one request to both reviewers:

```bash
agent-mesh request --from human --to reviewers "Review the config" "Confirm aliases expand."
```

Invariants:

- adding participant #N is config-only;
- existing events/hash chain remain valid;
- the new participant has an empty outbox until it replies or receives a message;
- removed participants can remain visible historically, but should be removed from
  `participants` and aliases so they cannot send/receive new messages;
- generated compatibility views may be deleted and regenerated; do not hand-edit them;
- package code stays project-neutral. Consumer repos can wrap `agent-mesh`, but wrappers
  must not become package defaults.

When another repo is the consumer, treat that repo's agent as a tester/user of the package:
make reusable fixes in `agent-mesh`, then have the consumer verify through its wrapper or
local `PYTHONPATH` without vendoring package code.

## Machine-Local Workbench Registry

Project data and policy remain in each repo's `.agent-mesh/config.toml`. The list
of repos available to the shared Workbench is machine-local operational metadata
stored at `${AGENT_MESH_CONFIG_HOME}/projects.toml`,
`${XDG_CONFIG_HOME}/agent-mesh/projects.toml`, or
`~/.config/agent-mesh/projects.toml`, in that precedence order.

`agent-mesh init` registers the initialized repo automatically. An adoption agent
handling an existing initialized repo should run:

```bash
agent-mesh projects register --repo .
agent-mesh projects list
```

Use `agent-mesh init --no-register` only when a repo intentionally must not appear
in the shared Workbench. Do not commit the machine-local registry, copy it between
users, or ask the human to maintain it manually. Registry entries are canonical
resolved paths; the Workbench ignores stale entries and rejects unknown repo IDs.
Registration also rejects symlinked or external Agent Mesh state paths so a
selected repo cannot route Workbench reads or writes into another checkout.
