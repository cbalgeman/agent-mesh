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
store_id = "store_01JABCDEF0123456789ABCDEFG"
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

Older installations may keep `name`, `default_sender`, and `default_recipient`
at the top level and have no `[project]` table. That format remains readable.
`agent-mesh adopt --repo <path> --check` reports it as an identity migration,
separately from stale or conflicting managed instructions. Running the command
without `--check` preserves the existing config text and mode, appends the
canonical `[project]` identity, and is safe to repeat or run concurrently. The
same collision-safe registry rules continue to apply.

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

Adding a participant is a config edit plus a projection rebuild. It must not
rewrite `.agent-mesh/events.jsonl` or migrate historical events.

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
store_id = "store_01JABCDEF0123456789ABCDEFG"
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
- generated compatibility views may be deleted and regenerated; do not hand-edit
  them.

## Distinguish parallel instances of one participant

A participant such as `claude` is the durable coordination identity; it is not
one particular chat window. When several long-running chats use that participant,
give each a public `<participant>-<durable-role>` handle rather than inventing
provider names, personas, or duplicate participants. A trusted runtime
integration normally registers or resumes the handle through its automatic
startup handshake. Manual registration is the compatibility fallback:

```bash
agent-mesh instance register --participant claude --provider anthropic \
  --handle claude-case-study --workstream case-study
agent-mesh instance register --participant claude --provider anthropic \
  --handle claude-design --workstream design-system
agent-q instances list --participant claude
```

The instance registry is append-only project state, not TOML configuration.
Normal surfaces expose one stable public handle; the project-local `AI-...` ID is
hidden canonical attribution used only by internals and explicit diagnostics.
Historical aliases remain input-compatible but are not a display-name layer.
An optional provider reference is accepted only as a precomputed project-scoped
digest. Once a participant has an active instance, every new canonical event
authored by that participant must carry a valid active handle binding via global
`--instance` or `AGENT_MESH_INSTANCE_ID`.

Use request `--to-instance` or backlog `--owner-instance` to hand work to a
specific existing chat. Do not use a runtime profile to stand in for an
instance: the profile describes how a process may be launched, while the
instance identifies the long-lived work context expected to receive the
handoff. See `docs/agent-instances.md` for commands and boundaries.

## Separate direct-human decision approval from agent participants

New approval events can bind an explicit human authority that is independent of
dispatch participants, runtime profiles, and reviewer roles:

```toml
[decision_approval]
human_approvers = ["human"]
```

Every listed identity must already be a project participant. This table does
not grant agents or dispatched reviewers human approval authority. Agent Mesh
never infers an approver from provider, model, runtime role, instance handle, or
a successful review assurance.

An existing project without `[decision_approval]` remains in the explicit
`legacy_participants` compatibility mode. Existing acceptances stay valid and
no decision changes status. Workbench reports the unmigrated diagnosis; review
the actual human identities and add the table deliberately. New acceptances
then record the explicit authority mode and revision that were in force for
that event. Later configuration changes do not rewrite historical acceptance.

## Configure subject-bound review assurance

`review.v1` is optional and advisory by default:

```toml
[review_assurance]
enforcement = "advisory"
independence = "distinct_instance"
quorum = 1
validity_seconds = 604800
reviewer_roles = []
covered_decisions = []
path_globs = []
backlog_transitions = []
release_gates = []
artifact_roots = ["docs", ".agent-mesh/reviews"]
authorized_uri_schemes = []
```

- `enforcement` is `advisory` or `blocking`.
- `independence` is `distinct_instance`, `distinct_context`, or
  `distinct_instance_and_context`.
- `quorum` is the number of distinct qualifying reviewers.
- `validity_seconds` is between 60 seconds and 365 days.
- `reviewer_roles` limits qualifying role labels. Blocking mode requires a
  non-empty allowlist, and the selected dispatch role must be in it.
- `artifact_roots` are non-escaping repository-relative roots allowed for
  detailed review references. Blocking mode requires at least one root.
- `authorized_uri_schemes` is empty by default. URI is reserved in the schema,
  but this release performs no URI fetch and cannot treat one as authoritative.
- coverage lists tell an adopting workflow where it requires a gate. Decision
  acceptance and backlog status changes enforce configured coverage directly;
  path and release workflows invoke `agent-q dispatches gate --boundary-kind …`
  at their explicit boundary.
- `path_globs` are compiled as meshglob-v1 during configuration loading; an
  invalid pattern is a configuration error, not an uncovered path.

Independence never falls back to the current command's actor, instance, or a
synthetic context hash. A decision subject may carry nested `author_provenance`
only when it is rederived from the canonical revision event ID, sequence, and
event hash. Artifact and change-set subjects do not accept caller-authored author
fields. When the exact subject has no canonical author instance or context, a
blocking policy that requires that fact remains unsatisfied.

Use `agent-q dispatches gate --policy dpol_... --json` at an advisory or
blocking transition boundary. See [Durable Dispatch](dispatch-contract.md) and
[Review Assurance](review-assurance-contract.md) for subject, freshness, and
typed-artifact rules.

## Enable a pinned executable runtime

A participant identity is inert until an enabled runtime profile binds it to an
executable. A participant may have multiple profiles; a caller must select the
profile explicitly whenever routing would otherwise be ambiguous. Profiles are
project-local and name every execution fact
that must not be inherited accidentally: provider, adapter, executable version,
model, role, permission mode, repository scope, capabilities, authentication and
billing boundary, and credential denylist.
Participant identity alone never enables execution.

Workbench groups enabled profiles first by AI agent (participant, provider, and
model), then limits **Run setup** to compatible atomic profiles for that agent.
It does not synthesize arbitrary combinations of role, access, network, and
continuity. Add or revise the complete profile in configuration when a new
combination is required; the UI displays the resulting validated choice.

Agent Mesh owns and ships its official built-in runtime-family drivers; adopter
support does not assume that a provider or community maintainer will supply one.
The integration unit is a runtime family, not a model. Model IDs are opaque
project configuration and must be discovered from the installed runtime catalog,
so a new model under a compatible runtime does not require a new Agent Mesh
driver or core release. An adopter can also select a digest-pinned project-local
V1 driver for basic one-shot launch without waiting for an Agent Mesh release.
That escape hatch is explicitly project-trusted and does not claim official
provider support or exact continuity. See `docs/runtime-adapter-contract.md` and
`docs/local-runtime-drivers.md` for the ownership, compatibility, and trust
contracts.

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
durable_role = "independent-review"
role = "research-reviewer"
permission_mode = "read-only"
repository_scope = "project"
required_capabilities = ["repository", "tools", "network"]
authentication_mode = "chatgpt"
billing_mode = "subscription"
adapter_trust = "configured"
session_identity_mode = "none"
resumable = false
concurrent_attachment = false
terminal_observation = "process"
credential_denylist = [
  "OPENAI_API_KEY",
  "ANTHROPIC_API_KEY",
  "GEMINI_API_KEY",
  "GOOGLE_API_KEY",
  "GOOGLE_APPLICATION_CREDENTIALS",
]
enabled = true
```

That profile is intentionally one-shot: it uses ephemeral `codex exec`, receives
a fresh `codex-independent-review` child handle for the launch, and becomes
terminal when the process ends. One-shot mode remains valid and does not claim
provider-context continuity.

Use exact resumability only for a durable role whose provider context should
survive separate dispatcher process invocations:

```toml
[dispatch.runtime_profiles.codex_builder]
target = "codex"
provider = "openai"
adapter = "codex-cli"
binary = "/opt/homebrew/bin/codex"
version = "0.153.4"
model = "gpt-5.6-sol"
durable_role = "builder"
role = "implementation"
permission_mode = "workspace-write"
repository_scope = "project"
required_capabilities = ["repository", "tools"]
authentication_mode = "chatgpt"
billing_mode = "subscription"
adapter_trust = "configured"
session_identity_mode = "exact"
resumable = true
concurrent_attachment = false
terminal_observation = "provider"
credential_denylist = ["OPENAI_API_KEY"]
enabled = true
```

The resumable Codex adapter requires all four continuity settings shown above.
Agent Mesh additionally binds each managed launch to the Dispatch plan's
resolved wave. The wave becomes the instance's durable workstream: later work in
that wave resumes the exact provider session, while another wave receives a
separate context and, when necessary, a workstream-qualified public handle.
For a new Workbench request, the optional **Workstream** field supplies this
stable wave; `agent-q dispatches run --workstream <name>` is the equivalent CLI
surface. Reuse the same human-facing name for related requests. Leaving it blank
deliberately gives that request an isolated context. Existing requests inherit
their stored feature or referenced backlog wave, and retries reuse the frozen
request context rather than accepting a replacement workstream.
Resumable protocol support is deliberately allowlisted to reviewed Codex CLI
versions; the current allowlist is `0.147.0` and `0.153.4`, while one-shot profiles
keep their ordinary exact executable-version pin.
It creates or proves the exact Codex app-server thread during the automatic
identity handshake. Supported Codex versions materialize a new persisted thread
at its first turn, so Agent Mesh keeps that same child-bound app-server alive through the
canonical identity, launch binding, lease, and started events, then submits the
first turn on that connection. Later turns discover and resume the persisted
thread over JSON-RPC stdio in a fresh child-bound process. It never puts the raw
provider thread ID in command arguments, environment variables, canonical events,
launch metadata, or diagnostics. It fails closed when the canonical digest cannot
be matched to exactly one project-root app-server thread. The adapter rejects
concurrent attachment, non-provider terminal observation, and exact-session
profiles without an available canonical/provider proof.

The app-server launch neutralizes configured MCP servers, plugins, skills, hooks,
analytics, and OTEL exporters; pins the OpenAI/ChatGPT provider and authentication
boundary; disables login shells; and prevents shell tools from inheriting the
app-server environment. Networked browser, app, plugin, computer-use, remote-memory,
and multi-agent features are explicitly disabled. A profile without the `network`
capability also sends `web_search = "disabled"` and disables sandbox network access.
The same overrides are sent in thread start/resume parameters while the configured
model, sandbox, approval policy, and project root remain explicit. Authentication
state is retained, but user configuration cannot add an unprofiled provider, MCP,
telemetry, shell-environment, tool, or network capability to this runtime boundary.
The built-in preflight proves a requested Codex network capability by combining
declared native `--search` support with a bounded, no-model connection to the
configured provider boundary from the sanitized child environment. It does not
treat a shell-sandbox `curl` denial as a web-search denial: native web search and
model-generated shell networking remain distinct, and shell network access stays
restricted by the explicit sandbox configuration.

For resumable profiles, Agent Mesh launches app-server with a private
machine-local `CODEX_HOME` under the Agent Mesh configuration directory. It does
not load the user's Codex `config.toml`; this keeps a pinned CLI compatible when
the desktop app writes settings for a newer CLI and prevents user-configured
plugins, MCP servers, hooks, or tools from entering managed dispatch. The
private home references the existing owner-only ChatGPT authentication file
without copying its credential bytes and stores only managed session state.
Codex may persist an owner-only `config.toml` containing project trust metadata
in that private home. Agent Mesh accepts only the bounded `projects` table with
absolute paths and `trust_level = "trusted"`; any provider, model, MCP, plugin,
hook, telemetry, tool, or other setting still fails closed before an attempt.

The denylist contains variable names, never credential values. Subscription
profiles also receive a package-level denylist for common OpenAI, Anthropic,
Google/Gemini, Vertex, and Cursor API credentials. The child process receives a
copied environment with all of those variables removed; the parent environment
is unchanged. Provider-native continuity variables, including `CODEX_THREAD_ID`,
are stripped at both the initial environment copy and the final process boundary
so a child cannot inherit the parent conversation.

Run the read-only preflight before creating work for the participant:

```bash
agent-q dispatches preflight --target codex
```

For a provider without an official built-in driver, an agent can scaffold a
project-local one-shot driver without changing Agent Mesh core:

```bash
agent-mesh drivers scaffold \
  --id local:example:claude-cli \
  --provider anthropic \
  --output tools/agent-mesh-drivers/claude-cli
```

After implementing its fail-closed `probe` and `launch` operations, pin the
printed manifest digest in a profile. The manifest separately pins the local
entrypoint digest:

```toml
[dispatch.runtime_profiles.local_claude]
target = "claude"
provider = "anthropic"
adapter = "local:example:claude-cli"
binary = "/usr/local/bin/claude"
version = "1.2.3"
model = "claude-model-from-runtime-catalog"
durable_role = "reviewer"
role = "research-reviewer"
permission_mode = "read-only"
repository_scope = "project"
required_capabilities = ["repository", "tools"]
authentication_mode = "subscription"
billing_mode = "subscription"
credential_denylist = ["ANTHROPIC_API_KEY"]
adapter_trust = "project-local"
session_identity_mode = "none"
resumable = false
concurrent_attachment = false
terminal_observation = "process"
driver_source = "project-local"
driver_protocol = "agent-mesh.runtime-driver.v1"
driver_manifest = "tools/agent-mesh-drivers/claude-cli/driver.toml"
driver_manifest_sha256 = "0000000000000000000000000000000000000000000000000000000000000000"
enabled = true
```

Replace the example digest with the scaffold's exact output, then run the same
no-model-prompt check used by live dispatch:

```bash
agent-q drivers check --target claude
```

Project-local V1 is fixed to `none`/non-resumable/non-concurrent/process-observed
one-shot execution. It cannot shadow a built-in adapter, is never auto-discovered,
and cannot claim provider continuity. Review local executable changes and update
both digests deliberately; a digest detects drift but does not sign or sandbox
the code. Agent Mesh appends no event during `drivers check`, but that command
executes unsandboxed project code; it cannot certify that the driver avoids file
mutation, prompt echo, or provider-reference output. See
`docs/local-runtime-drivers.md` for the exact JSON protocol, bounds, path rules,
and repair workflow.

For `codex-cli`, preflight uses non-prompt CLI surfaces to verify the resolved
binary and exact version, requested model in the model catalog, ChatGPT login,
subscription billing policy, configured sandbox, repository root, declared
tool/network flags, and credential isolation. It also runs a bounded no-model
probe through the actual Codex sandbox to prove effective repository/tool
access; help text alone is not effective evidence. `dispatches run`,
`dispatches once`, and `dispatches worker` repeat preflight before lifecycle
writes. The high-level flow rechecks drift-sensitive evidence immediately before
launch, and the bounded worker repeats preflight before every possible live launch,
so version, authentication, or capability drift between long-running iterations
fails before the next lifecycle write. A failure writes no dispatch events for
that attempted launch.

For a resumable profile, preflight additionally requires Codex app-server's
strict stdio/config-override transport and a reviewed protocol version. Exact
thread discovery and resume are then verified at the live handshake boundary
without issuing a model turn. Before a new provider thread is created, the planned
event records only a digest of the bounded provider inventory. That digest proves
whether the inventory remained unchanged without persisting raw IDs. The adapter
never adopts an unregistered set delta. A hard crash after provider thread creation
but before its first turn can leave an unmaterialized provider entry. Before
canonical registration, an ambiguous visible inventory delta blocks replacement.
If no entry is listed and the complete inventory still matches the planned digest,
retry may create a fresh no-turn session without claiming that an inaccessible
provider entry was resumed or deleted. After registration, inability to resume the
exact bound thread fails closed for manual provider reconciliation. A newly created
thread is deleted best-effort after a caught failure. Once the first turn has
materialized the bound thread, later invocations can prove and resume it. A provider
turn that remains in progress, or whose terminal status becomes uncertain after
submission, returns observation-pending and retains the active lease until a retry
observes a provider-terminal result.

Only `openai` + `codex-cli` currently has an Agent Mesh-owned,
machine-verifiable live driver. Official built-in profiles for Antigravity,
Claude, Cursor, or another runtime fail the `adapter` check until Agent Mesh
ships the corresponding reviewed driver. A project may instead opt into the
narrow project-local V1 one-shot boundary above, with its trust source preserved
as `project-local`. Do not relabel a local probe as official support or weaken an
exact-continuity check to make a profile appear live. External/manual participation remains available without a driver.

## Adoption targets

The first successful `agent-mesh adopt --repo .` persists the managed instruction
targets it selected:

```toml
[adoption]
contract_targets = ["agents"]
```

Supported values are `agents` (`AGENTS.md`) and `claude` (`CLAUDE.md`). Repeating
`--target` on a later adoption replaces this list deliberately. Without a
persisted value, Agent Mesh selects `agents` and adds `claude` when Claude project
configuration is present, except when a root `CLAUDE.md` already imports
`AGENTS.md` with a standalone `@AGENTS.md` or `@./AGENTS.md` line. Applying a
narrower target set removes only the marked Agent Mesh contract from unselected
files; all repository-authored content remains.

## Context budget

The report-only context inventory has conservative defaults:

```toml
[context_budget]
ceiling_bytes = 131072
instruction_paths = ["AGENTS.md", "CLAUDE.md", "MEMORY.md"]
hook_sample_paths = ["AGENTS.md"]
per_prompt_paths = []
```

Run `agent-mesh doctor --context-budget`; add `--json` for the stable
`agent-mesh.context-budget.v1` envelope. The inventory uses fixed, no-follow
reads of configured repository-root instruction files and generates bounded
decision-digest samples for the configured repository-relative paths. It reports
candidate bytes, a four-bytes-per-token estimate, ceiling status, residency
unknowns, exact whole-file hashes, and duplicate managed-contract blocks.
`per_prompt_paths` may name bounded repository-relative files containing
representative repeated-injection context. Agent Mesh measures those bytes with
`per_prompt_candidate` residency but never executes hook code. It never recursively scans a repository or
home directory, reads credentials, writes canonical state, or proves what a
harness actually loaded. Ceiling overruns do not block work in 0.4.x.
The command returns `0` for a complete report and `3` when a safety or inspection
bound makes the report incomplete.

`--scope registered-projects` opts into aggregating the explicit machine-local
Workbench registry. Every project keeps its own file list and ceiling; the
aggregate ceiling is their sum. Instruction paths are limited to root files so a
configuration cannot turn this command into a general file scanner. One shared
wall-clock, source-byte, and canonical-event replay budget covers enumeration,
instruction reads, and mutation-free in-memory decision replay across the scope.

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
