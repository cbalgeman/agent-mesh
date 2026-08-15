# agent-mesh

Agent Mesh gives a human one reliable place to coordinate the AI agents working
on a software project. It turns important requests, decisions, backlog items,
handoffs, and results into a durable, project-local history that survives chat
boundaries and changes of agent.

## Why adopt it?

AI-assisted work easily becomes scattered across chat windows, agents, and
private notes. Agent Mesh adds a shared source of truth, human-readable work
IDs, explicit human approval for durable decisions, and a local Workbench where
you can see what is open, decided, or complete. Agents can recover the relevant
context and hand work off without copying whole conversations. Ordinary chat
can remain chat, and project state stays private and local by default.

## What adoption looks like

Give your coding agent access to Agent Mesh and the repository you want to
adopt. The agent inspects the project, asks you only for the choices it cannot
safely infer, initializes the local Agent Mesh state, installs the project
contract, verifies it, and opens the Workbench. You remain the approval point
for durable decisions and for whether any Agent Mesh state is shared through
Git. Existing workflows can move over gradually instead of being replaced all
at once.

## Human User experience

The Workbench is the primary human control surface. It keeps review, priority,
and approval understandable without requiring the human to reconstruct project
state from agent chats or operate the CLI.

### What you see in the Workbench

| View | What you can see and do | Why it matters |
|---|---|---|
| Repository selector and health | Choose the active repository and see the local server and agent-contract status. | Keeps every action in the intended project and makes a broken or stale setup visible. |
| Dashboard | See open requests and feedback, urgent and pending-user work, progress totals, priority and lane breakdowns, and recent backlog activity. | Gives you an immediate answer to “What needs my attention?” without asking an agent to reconstruct it. |
| Verify / Feedback | Record an observation, severity, target, references, and screenshots, then submit it as a durable REQ addressed to one or more agents. | Creates attributable, trackable work instead of a comment that disappears in chat. Relay its ID to an agent so it can retrieve and process the feedback, respond on the same thread, and create or link backlog items when follow-up work is needed. |
| Messages | Search requests and responses by status, kind, origin, participant, or text; inspect a thread; and close or reopen a request with a reason. | Preserves assignments, handoffs, outcomes, and the context needed by the next human or agent. |
| Backlog | Search and filter work by status, lane, priority, owner, type, scope, or origin, and update its status, lane, or priority. | Provides one current, human-readable work queue across agents. |
| Decisions | Search proposed, accepted, in-force, rejected, superseded, and retired decisions; create or revise proposals; and approve an exact revision with a note. | Shows what the human actually authorized and prevents an edited decision from silently inheriting an earlier approval. |
| Kanban | See backlog items grouped by lane and move them between lanes. | Makes flow, ownership, and bottlenecks easy to scan. |

### Human control and privacy

- New projects are private by default: `local-only` keeps `.agent-mesh/` out of
  normal Git adds. `git-shared` is an explicit opt-in that exposes the canonical
  config, event log, and body files to everyone who can read the repository.
- Agent Mesh is not a secret store. Free-form messages and bodies can contain
  anything a participant writes, and there is currently no automatic redaction,
  deletion, or retention policy. Do not record credentials or unnecessary
  personal information.
- Uploaded attachments stay local and are not included by the `git-shared`
  allowlist, but their paths can be referenced by feedback records.
- Decisions require direct human approval. Agents may propose or revise them,
  but cannot approve them on the human's behalf.
- Memory is project-scoped and deliberately retrieved. Agent Mesh does not
  semantically inject every past record into every prompt or provide
  multi-tenant access control inside one project store.
- The Workbench is served only on the local loopback interface and uses a
  private access token. It is a local UI, not a hosted Agent Mesh service.

## What Agent Mesh records

Behind those views, the durable project record includes:

- **Requests and responses** with bodies, status history, participants,
  timestamps, and thread relationships.
- **Long-lived AI-agent instances** with stable IDs, human-friendly labels,
  provider and workstream metadata, per-event attribution, and direct request or
  backlog ownership. This distinguishes two Claude chats in the same project
  without pretending they are different providers.
- **Human feedback** with its severity, target, references, and paths to any
  locally stored attachments.
- **Backlog work** with descriptions, priorities, lanes, owners, relationships,
  notes, and activity history.
- **Decisions** with proposals, revisions, lifecycle status, human approval
  identity and notes, affected paths, required checks, and verification results.
- **Provenance and settings** with source and causal links, body fidelity,
  project identity, participant aliases, routing defaults, and sharing mode.

Ordinary conversation is not copied automatically. A human or agent promotes a
concise request, decision, or material result when it should survive the chat.

## How it works

The canonical source of truth is `.agent-mesh/events.jsonl`, an append-only,
hash-linked event log. Longer bodies may be stored as separate files addressed
by content hash. A SQLite database and optional Markdown views are derived from
the log and can be rebuilt. The `agent-mesh` CLI and local browser Workbench are
the supported write surfaces; `agent-q` provides bounded reads for humans and
agents. Repository locking and crash recovery make concurrent local writers
safe.

The hash chain is tamper-evident, not encryption or cryptographic identity
proof. Corrections are recorded as new events so history remains auditable.

For parallel agent chats, an instance ID can remain stable across days and
process restarts. A case-study agent can create a REQ or backlog item addressed
to a specific design-system agent, and the receiving chat can retrieve the
canonical packet by ID. The chat must keep supplying its assigned ID; Agent Mesh
cannot infer provider chat continuity or preserve the provider's context window.

## Status

Pre-1.0. The command and storage contracts may still change before a stable
release. Agent Mesh is implemented with the Python standard library and has no
required hosted service, model provider, database server, or third-party runtime
dependency.

## Installation and releases

Install a published PyPI release with:

```bash
python -m pip install agent-mesh
```

See the [changelog](https://github.com/cbalgeman/agent-mesh/blob/main/CHANGELOG.md)
for release notes. Maintainers use the documented
[release procedure](https://github.com/cbalgeman/agent-mesh/blob/main/docs/releasing.md).

## Verify the published source

The public repository includes a privacy-reviewed contract pack covering the
request/response smoke flow, hash-chain tamper detection, decision lifecycle
stop-lines, and local-only versus Git-shared privacy behavior. Run the same
checks used by public CI with:

```bash
python -m pip install -e ".[test]"
python -m ruff check src tests/public
python -m pytest -q
```

Public CI runs this pack on the supported Python 3.11 and 3.12 versions. The
larger development suite is deliberately not copied into the curated repository.

## Support

Use [GitHub Issues](https://github.com/cbalgeman/agent-mesh/issues) for bug
reports, feature requests, and usage questions. Do not include credentials,
private repository content, or local `.agent-mesh/` state in a public issue.

## License

MIT. See `LICENSE`.

## Human Users can stop reading here

You now have the information needed to decide whether Agent Mesh fits your
workflow. To adopt it, give your coding agent access to the Agent Mesh repository
and the repository you want to adopt, then send this instruction:

```text
Adopt Agent Mesh in the target repository. Read the AI Agent guide below, then
read docs/adoption.md and docs/privacy.md. Inspect the target repository, ask me
only for choices you cannot safely infer, complete the setup, and return the
Workbench bookmark and verification results. Do not accept decisions or enable
Git sharing on my behalf.
```

Everything after this line is written exclusively for the AI Agent performing
the adoption and ongoing project work.

---

# AI Agent guide

## AI Agent experience

Agent Mesh is model- and runtime-neutral. It gives you, the agent working in the
repository, a stable contract and bounded retrieval tools. A project may also
configure an optional, explicit runtime profile for Agent Mesh to dispatch work
to an external agent executable; a participant identity alone never selects or
launches a model.

- The managed repository contract tells you which records are canonical, when
  to create a REQ or material RES, how to read applicable decisions, and which
  actions remain human-only.
- `agent-q packet --id <REQ-or-RES-id>` retrieves bounded thread context;
  `agent-q backlog get <BKL-id>` retrieves a work item; and
  `agent-q decisions show <D-id>` retrieves canonical decision metadata,
  status, affected paths, checks, and verification state.
- Process a feedback REQ by retrieving it, recording findings on the same
  thread, and creating or linking backlog work when implementation is needed.
  Preserve the REQ as the provenance-bearing source for any derived backlog
  items.
- Write durable changes through the CLI or Workbench and verify the resulting
  record. You may propose or revise decisions but must never approve one for the
  human.

### Address long-lived agent instances

Use an agent-instance identity when multiple long-running chats share one
participant or provider but own different workstreams. Register each chat once,
then launch or resume it from an environment that binds all of its Agent Mesh
writes to the stable ID or label:

```bash
agent-mesh instance register --participant claude --provider anthropic \
  --label claude-case-study --workstream case-study
agent-mesh instance register --participant claude --provider anthropic \
  --label claude-design --workstream design-system

export AGENT_MESH_INSTANCE_ID=claude-case-study
# launch or resume the intended agent from this shell
```

If the environment cannot persist with the chat, put the global option before
every subcommand. The same binding makes a cross-instance handoff explicit:

```bash
agent-mesh --instance claude-case-study request --from claude \
  --to-instance claude-design \
  "Fix the design token" "The case study exposed a shared styling issue."

agent-mesh --instance claude-case-study backlog create --actor claude \
  --owner-instance claude-design --title "Normalize the design token"

agent-q list --to-instance claude-design --status open
agent-q backlog list --owner-instance claude-design
```

Once a participant has an active registered instance, unbound new events from
that participant fail instead of falling back to provider-only attribution.
Specific-instance REQs are not launched by generic runtime dispatch, because a
dispatcher cannot safely recreate an existing provider chat. Relay the REQ ID
to the named chat, which retrieves it with `agent-q packet --id <REQ-id>`.

The identity is durable Agent Mesh state, not cryptographic authentication or a
copy of chat context. A resumed multi-day chat keeps its identity only when its
environment, wrapper, or instructions continue supplying the same ID. See
`docs/agent-instances.md` for lifecycle, privacy, and retirement details.

### Keep durable context beside important code

Place a short Agent Mesh reference in a code comment when a non-obvious
constraint should remain beside the code it governs while pointing to the fuller
canonical record:

```text
# <D-id>: preserve direct human approval; do not automate this transition.
# <BKL-id>: remove this compatibility path after the linked migration closes.
```

Include the one-line rationale, not only a bare ID. This lets the next human or
agent understand why the comment matters before retrieving the decision,
request, or work item.

This is a reliable but bounded feature today. Human-readable decision IDs,
REQ/RES IDs, backlog IDs, and AI-agent instance IDs resolve through Agent Mesh
query commands, and
`agent-mesh check refs` can scan tracked code and fail on a dangling decision,
request, response, backlog, or instance reference. The managed repository contract also
requires you to consult canonical decisions before a related durable choice.

The references are validated pointers, not automatic enforcement. Agent Mesh
does not yet guarantee that every agent harness will inject a referenced record,
rerun retrieval when a file is edited, detect a semantic contradiction, or
enforce every affected decision before commit. Opt the repository into the
reference check in local hooks or CI, follow the managed contract, and retrieve
the cited record before changing the governed behavior.

## First-Time Adoption Instructions

Read `docs/adoption.md` before initializing a target repository. Inspect the
target yourself, summarize only the setup decisions that require human input,
and wait for the human's response. After the human responds, record the durable
choices as Proposed decisions. The human then directly approves each decision
in Workbench, or by running the interactive CLI acceptance command; never accept
a decision on the human's behalf. Continue setup after acceptance and verify the
records in Workbench's Decisions tab. Keep all `.agent-mesh/` state local by
default; sharing canonical state through Git is a separate explicit onboarding
choice.

## Manual Quickstart

```bash
pip install agent-mesh
cd ~/your-project
agent-mesh init --participants human,agent --default-sender human --default-recipient agent
agent-mesh adopt --repo .
agent-mesh adopt --repo . --check
agent-mesh request --to agent "Review the auth refactor"
agent-q list --status open
agent-q packet --id <REQ-id>
agent-mesh workbench --repo .
```

The quickstart uses the privacy-first `local-only` default. It causes a normal
`git add -A` to select no `.agent-mesh/` path. Use
`--state-sharing git-shared` only after approving Git access to the canonical
config, event log, and externalized bodies. See `docs/privacy.md` before sharing
a repository or changing this setting.

## Automatic Workbench

The recommended adoption flow installs one automatic Workbench service for the
current user:

```bash
agent-mesh workbench service install --repo . --open
```

The command is idempotent and uses `launchd` on macOS, `systemd --user` on Linux,
or Task Scheduler on Windows. The service starts at sign-in and restarts after a
failure. It serves every valid repo in the machine-local Workbench registry, so
adopting another project does not create another background process. After an
agent installs it, the human can use the stable machine-local bookmark printed
by the command without opening a terminal. Reinstalling from another project or
restarting the service refreshes that same bookmark. Use
`agent-mesh workbench service open` whenever the bookmark is not already saved;
`service status` prints both its exact path and the open command. Use `start`,
`restart`, or `uninstall` for lifecycle management. Installing or refreshing the
service also turns the anchor repo's old project-local bookmark into a token-free
pointer to the managed bookmark. The manual
`agent-mesh workbench --repo .` command remains the fallback when the native user
supervisor is unavailable.

`agent-q packet` returns bounded, thread-scoped JSON for grounding an agent on a
request or response. `agent-mesh workbench` starts a small local UI and writes a
bookmarkable `.agent-mesh/workbench.html` file for the project. Its Decisions tab
creates Proposed decisions, appends revisions, and records explicit human
acceptance. Editing an accepted or in-force decision requires a reason and
returns it to Proposed until it is accepted again. Repository Markdown decision
logs are optional generated compatibility views, never separate writable
tracking surfaces.

`agent-mesh init` automatically registers the repo in the machine-local
Workbench registry and reports when the managed agent contract is incomplete.
`agent-mesh adopt` installs a versioned contract in applicable agent instruction
files; `agent-mesh adopt --check` detects stale contracts and conflicting legacy
decision-write guidance. The Workbench shows the same contract health signal.

The repository selector can switch among registered repos, and the server
resolves its opaque repo ID before feedback, request-status, backlog, attachment,
or decision operations. The bookmark is static, while live reads and writes
require the loopback server. With the automatic service, native supervision and
the page's reconnect loop keep that server available. If a service restart leaves
an already-open page with the prior token, the page reloads the latest private
bookmark once; a successful health check resets that bounded recovery for the
next restart. The browser never executes a shell command. Feedback submits use retry-safe receipts so an uncertain retry
returns the original REQ instead of creating a duplicate. A per-server access
token and restricted browser origins protect the local mutation APIs
automatically. The server is loopback-only, the HTTP launch URL carries its token
only in a URL fragment, and the managed token-bearing bookmark is private (`0600`
on macOS/Linux) and stored outside project repositories. Manual project bookmarks
remain ignored by Git.

## Configuration

Project-local configuration lives in `.agent-mesh/config.toml`. See `docs/configuration.md` for the supported config surface, including how to add a new agent/participant to an existing project without rewriting historical events. Long-lived chat identities are documented in `docs/agent-instances.md`. Privacy and Git-tracking behavior are documented in `docs/privacy.md`.

## Agent-Driven Adoption

For first-time setup in a real repository, start with `docs/adoption.md`. It is
written for the coding agent: it tells the agent how to inspect the target repo,
ask only for missing project-local input, initialize `.agent-mesh/`, verify the
chain, start the Workbench, and give the human a bookmarkable Workbench path.

## Migrating Existing Workflows

If you already coordinate through scripts, markdown files, issue trackers, or
chat logs, see `docs/migration.md`. The recommended path is shadow-first:
inventory the current workflow, import into `.agent-mesh/`, preserve source
provenance, let agents review a dry-run, and keep old surfaces as projections
until the chain and compatibility views are verified.

Project-specific importers should live in the consumer repository. `agent-mesh`
ships the generic substrate, recovery reports, projections, and review commands.

## Examples

```bash
bash examples/solo-project/run.sh
N=3 bash examples/n-agent/run.sh
```

## Layout

```text
src/agent_mesh/
├── core/      # events.jsonl, lock, recovery, hashing
├── store/     # SQLite schema + queries
├── views/     # rendered inbox/outbox/archive
├── cli/       # agent-mesh + agent-q CLIs
└── config.py  # .agent-mesh/config.toml loader

examples/
├── solo-project/     # 1 agent, simple use
└── n-agent/          # parameterized N-agent flow
```
