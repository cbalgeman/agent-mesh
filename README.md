# agent-mesh

A durable, append-only message substrate for multi-agent collaboration. Designed for human + AI agent teams to communicate through structured requests/responses with full audit history.

## What it is

`agent-mesh` is a project-local message substrate built around a content-addressable, schema-versioned event log. It scales from one project to many, supports concurrent writers, and recovers cleanly from interrupted writes.

## What it gives you

- **`events.jsonl`** — durable, append-only, schema-versioned, tamper-evident source of truth
- **SQLite index** — derived, rebuildable, indexed for fast queries
- **Generated views** — optional human-readable inbox.md / outbox-*.md regenerated from the index
- **CLI tools** — `agent-mesh` for writes and `agent-q` for reads
- **Local workbench** — browser UI for message lookup, feedback drafting, backlog, kanban, and decisions
- **Multi-agent safe** — owner-aware locking and idempotent crash recovery
- **Project-agnostic** — agent identities, paths, and features are configurable per project

## Status

Pre-1.0. The public package surface is intentionally small: core library code, CLI entry points, configuration reference, and runnable examples.

## First-Time Adoption

If you are trying `agent-mesh` in a repository for the first time, the human
instruction should stay simple:

```text
Give your agent the Agent Mesh repo and your target repo. Ask your agent to read
the README, adoption, and privacy docs. Your agent will walk you through the rest.
```

The rest of the setup is agent-facing. Agents should read `docs/adoption.md`
before initializing a target repository, summarize the setup decisions that need
human input, and wait for the human's response. After the human responds, record
the durable approved choices in the Agent Mesh decision log, accept them on the
human's behalf, then implement and verify the setup. The human can review those
records in the Workbench's Decisions tab. New projects keep all `.agent-mesh/` state
local by default; sharing canonical state through Git is a separate explicit
onboarding choice.

## Manual Quickstart

```bash
pip install agent-mesh
cd ~/your-project
agent-mesh init --participants human,agent --default-sender human --default-recipient agent
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
`agent-mesh workbench service status`, `start`, `restart`, or `uninstall` for
lifecycle management. The manual
`agent-mesh workbench --repo .` command remains the fallback when the native user
supervisor is unavailable.

`agent-q packet` returns bounded, thread-scoped JSON for grounding an agent on a
request or response. `agent-mesh workbench` starts a small local UI and writes a
bookmarkable `.agent-mesh/workbench.html` file for the project. `agent-mesh init`
automatically registers the repo in the machine-local Workbench registry. The
repository selector can switch among registered repos, and the server resolves
its opaque repo ID before feedback, request-status, backlog, attachment, or
decision operations. The bookmark is static, while live reads and writes require
the loopback server. With the automatic service, native supervision and the
page's reconnect loop keep that server available; the browser never executes a
shell command. Feedback submits use retry-safe receipts so an uncertain retry
returns the original REQ instead of creating a duplicate. A per-server access
token and restricted browser origins protect the local mutation APIs
automatically. The server is loopback-only, the HTTP launch URL carries its token
only in a URL fragment, and the managed token-bearing bookmark is private (`0600`
on macOS/Linux) and stored outside project repositories. Manual project bookmarks
remain ignored by Git.

## Configuration

Project-local configuration lives in `.agent-mesh/config.toml`. See `docs/configuration.md` for the supported config surface, including how to add a new agent/participant to an existing project without rewriting historical events.
Privacy and Git-tracking behavior are documented in `docs/privacy.md`.

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

## License

MIT. See `LICENSE`.
