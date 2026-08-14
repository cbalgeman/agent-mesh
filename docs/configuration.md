# Configuration

This page documents project-local `.agent-mesh/config.toml` changes. `agent-mesh`
configuration is intentionally repo-scoped: package defaults stay generic, and each
consumer project decides which humans/agents, aliases, and compatibility views it wants.

## Choose whether Agent Mesh state is shared through Git

New projects record this privacy-first default:

```toml
[version_control]
state_sharing = "local-only"
```

`local-only` keeps every `.agent-mesh/` path out of a normal Git add. The
alternative, `git-shared`, must be selected explicitly and exposes only the
canonical config, event log, and externalized bodies through a deny-by-default
allowlist. It never allows attachments, databases, Workbench bookmarks, or
unknown future files.

Treat this as a durable onboarding decision. Git-shared state is appropriate only
when everyone with repository access is allowed to read the coordination history.
See `docs/privacy.md` for upgrade behavior, removal of already tracked state, and
the pre-publication checklist.

## Choose the project identity used in IDs

Each initialized project persists three identity fields in addition to its
display name and participant defaults:

```toml
[project]
name = "example-project"
timezone = "America/Los_Angeles"
key = "example-project"
store_id = "store_01KZPR603PBSJ1VKR9KA9PB6M4"
default_sender = "human"
default_recipient = "builder"
```

- `timezone` is the human user's IANA timezone. It determines the date portion
  of newly allocated `BKL-YYYYMMDD-NN` IDs. Canonical event timestamps remain
  UTC.
- `key` is the stable, human-readable cross-repository project reference.
- `store_id` is the stable machine identity. Agent Mesh generates it once; do
  not edit, copy to a simultaneously live checkout, or derive it from the path.

`agent-mesh adopt` fills missing identity fields under a repository lock and
replaces an explicit legacy path-derived `repo-…` value atomically. It preserves
an existing current value and never rewrites `timezone` or `key` implicitly.
Machine-registry registration rejects a `store_id` already assigned to another
live root. After an actual repository move, registration may replace the old row
only when that recorded root no longer exists.

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

## Add an agent to an existing project: identity vs runtime

“Add an agent” is ambiguous. This section adds a **participant identity**: a
durable sender/recipient address in Agent Mesh. It does not install an executable
runtime, authenticate a provider, select or pin a model, grant repository/tool/
internet access, or assign a workflow role. Specify and verify those runtime
integration layers separately. In particular, verify the executable, selected
model, repository and tool access, authentication method, and billing mode as
independent facts. A participant identity alone never enables execution.

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
timezone = "America/Los_Angeles"
key = "example-project"
store_id = "store_01KZPR603PBSJ1VKR9KA9PB6M4"
default_sender = "human"
default_recipient = "builder"

[agents]
participants = ["human", "builder", "reviewer", "observer"]

[features]
hash_chain = true
body_externalization = false

[version_control]
state_sharing = "local-only"

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

## Distinguish parallel instances of one participant

A participant such as `claude` is the durable coordination identity; it is not
one particular chat window. When several long-running chats use that participant,
register each as an AI-agent instance rather than inventing provider names or
duplicating participants:

```bash
agent-mesh instance register --participant claude --provider anthropic \
  --label claude-case-study --workstream case-study
agent-mesh instance register --participant claude --provider anthropic \
  --label claude-design --workstream design-system
agent-q instances list --participant claude
```

The instance registry is append-only project state, not TOML configuration.
Each stable ID can have human-friendly label aliases and an optional runtime
profile or hashed external session reference. Once a participant has an active
instance, every new canonical event authored by that participant must carry a
valid active instance binding via global `--instance` or
`AGENT_MESH_INSTANCE_ID`.

Use request `--to-instance` or backlog `--owner-instance` to hand work to a
specific existing chat. Do not use a runtime profile to stand in for an
instance: the profile describes how a process may be launched, while the
instance identifies the long-lived work context expected to receive the
handoff. See `docs/agent-instances.md` for commands and boundaries.

## Enable a pinned executable runtime

A participant identity is inert until exactly one enabled runtime profile binds
it to an executable. Profiles are project-local and name every execution fact
that must not be inherited accidentally: provider, adapter, executable version,
model, role, permission mode, repository scope, capabilities, authentication and
billing boundary, and credential denylist.
Participant identity alone never enables execution.

This example enables a subscription-backed Codex research/review profile. Pin
the version actually installed on the machine; an upgrade deliberately makes
preflight fail until the profile is reviewed and updated.

```toml
[dispatch.runtime_profiles.sol_research]
target = "codex"
provider = "openai"
adapter = "codex-cli"
binary = "/opt/homebrew/bin/codex"
version = "0.146.1"
model = "gpt-5.6-sol"
role = "research-reviewer"
permission_mode = "read-only"
repository_scope = "project"
required_capabilities = ["repository", "tools", "network"]
authentication_mode = "chatgpt"
billing_mode = "subscription"
credential_denylist = [
  "OPENAI_API_KEY",
  "ANTHROPIC_API_KEY",
  "GEMINI_API_KEY",
  "GOOGLE_API_KEY",
  "GOOGLE_APPLICATION_CREDENTIALS",
]
enabled = true
```

The denylist contains variable names, never credential values. Subscription
profiles also receive a package-level denylist for common OpenAI, Anthropic,
Google/Gemini, Vertex, and Cursor API credentials. The child process receives a
copied environment with all of those variables removed; the parent environment
is unchanged.

Run the read-only preflight before creating work for the participant:

```bash
agent-q dispatches preflight --target codex
```

For `codex-cli`, preflight uses non-prompt CLI surfaces to verify the resolved
binary and exact version, requested model in the model catalog, ChatGPT login,
subscription billing policy, configured sandbox, repository root, declared
tool/network flags, and credential isolation. `dispatches once` and
`dispatches worker` repeat that preflight before writing a plan, lease, or start
event. The bounded worker repeats preflight before every possible live launch,
so version, authentication, or capability drift between long-running iterations
fails before the next lifecycle write. A failure writes no dispatch events for
that attempted launch.

Only `openai` + `codex-cli` currently has a machine-verifiable live adapter.
Profiles for Antigravity, Claude, Cursor, or another runtime can be represented
by the same schema, but preflight fails the `adapter` check until that adapter
can prove its version, model, authentication/billing boundary, and capabilities
without a billed probe or brittle UI scraping. Do not weaken the check to make a
profile appear live.

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

Current registry rows use the repository's stable `[project].store_id`. When a
valid same-root row still uses the older path-derived `repo-…` ID, the registry
reader upgrades that row under the machine-registry lock and replaces the file
atomically. It does not reassign a `store_id` already claimed by another root;
resolve that conflict explicitly with `agent-mesh projects register` after
checking the affected paths.
