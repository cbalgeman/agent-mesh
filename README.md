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
| Dashboard | See open requests and feedback, urgent and pending-user work, progress totals, priority and lane breakdowns, recent backlog activity, and a collapsible guide to every Workbench view. | Gives you an immediate answer to “What needs my attention?” and a one-click path to the right workflow without asking an agent to reconstruct it. |
| Backlog | Search and filter work by status, lane, priority, owner, type, scope, or origin, and update its status, lane, or priority. | Provides one current, human-readable work queue across agents. |
| Decisions | Filter the full-width decision list, read rendered Context and Decision content below the selected row, compare any available canonical versions, and approve or reject the exact proposal revision with a note. | Shows what the human actually authorized and what they are deciding now, while preserving the hashes, code scope, checks, and canonical record needed for verification and enforcement. |
| Verify / Feedback | Record an observation, severity, target, references, and screenshots, then submit it as a durable REQ addressed to one or more agents. | Creates attributable, trackable work instead of a comment that disappears in chat. Relay its ID to an agent so it can retrieve and process the feedback, respond on the same thread, and create or link backlog items when follow-up work is needed. |
| Messages | Search requests and responses by status, kind, origin, participant, or text; inspect a thread; close or reopen a request with a reason; and continue a managed result in Dispatch. | Preserves assignments, handoffs, outcomes, and the context needed by the next human or agent. |
| Dispatch | Search managed work by human-facing request title, launch or retry it with a plainly described runtime profile, continue from a completed result as a linked request, and inspect audit IDs or assurance only when needed. | Keeps durable delegation and review evidence portable across agents, models, and harnesses without making policy, request, run, or profile IDs the primary navigation. |
| Kanban | See backlog items grouped by lane and move them between lanes. | Makes flow, ownership, and bottlenecks easy to scan. |

### Human control and privacy

- New projects are private by default: `local-only` keeps `.agent-mesh/` out of
  normal Git adds. `git-shared` is an explicit opt-in that exposes the canonical
  config, event log, and body files to everyone who can read the repository.
- Agent Mesh is not a secret store. Free-form messages and bodies can contain
  anything a participant writes, and there is currently no automatic redaction,
  deletion, retention enforcement, or privacy-reviewed export command. Do not
  record credentials or unnecessary personal information. The proposed
  [privacy lifecycle contract](docs/privacy-lifecycle.md) distinguishes
  append-only correction, retrieval tombstones, emergency byte removal, and
  privacy-reviewed export without claiming that any of those future mechanisms
  already exists. Development provenance is tracked by decision `D008` in the
  full development checkout. Public packages do not include that canonical
  decision state, and no corresponding command is currently advertised by the
  CLI.
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
- **Durable AI-agent instance handles** such as `claude-design`, backed by hidden
  project-local IDs for canonical attribution. Handles support direct requests
  and backlog ownership without turning provider, persona, or runtime profile
  into a second public identity.
- **Human feedback** with its severity, target, references, and paths to any
  locally stored attachments.
- **Backlog work** with descriptions, priorities, lanes, owners, relationships,
  notes, and activity history.
- **Decisions** with proposals, revisions, lifecycle status, human approval
  identity and notes, affected paths, required checks, and verification results.
- **Dispatch and review assurance** with frozen policies, bounded attempts,
  capability receipts, exact response slots and RES outcomes, subject-bound
  review evidence, and typed references to detailed project-owned artifacts.
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

For parallel agent chats, a public handle such as `claude-design` can remain
stable across days and process restarts. A case-study agent can create a REQ or
backlog item addressed to a specific design-system agent, and the receiving chat
can retrieve the canonical packet by ID. A trusted runtime integration performs
the automatic identity handshake; an unmanaged chat keeps supplying its assigned
public handle. Agent Mesh preserves coordination identity but cannot itself
preserve the provider's context window.

## Status

Pre-1.0. The command and storage contracts may still change before a stable
release. Agent Mesh is implemented with the Python standard library and has no
required hosted service, model provider, database server, or third-party runtime
dependency.

## Install and verify

Install or upgrade the published package from PyPI:

```bash
python -m pip install --upgrade my-agent-mesh
```

Confirm that the installed package and both command-line tools are available:

```bash
python -m pip show my-agent-mesh
python -c "import agent_mesh; print(agent_mesh.__version__)"
agent-mesh --help
agent-q --help
```

These commands inspect the installed PyPI distribution without modifying a
project repository.

See the [changelog](https://github.com/cbalgeman/agent-mesh/blob/main/CHANGELOG.md)
for release notes and upgrade instructions.

## Support

Use [GitHub Issues](https://github.com/cbalgeman/agent-mesh/issues) for bug
reports, feature requests, and usage questions. Do not include credentials,
private repository content, or local `.agent-mesh/` state in a public issue.

## License

MIT. See `LICENSE`.

## Human Users can stop reading here

You now have the information needed to decide whether Agent Mesh fits your
workflow. Give your coding agent access to the repository you want to adopt,
then send this instruction:

```text
Adopt Agent Mesh in the target repository. Install or upgrade my-agent-mesh from
PyPI. Read the AI Agent guide below and the official adoption, configuration,
privacy, and migration docs at
https://github.com/cbalgeman/agent-mesh/tree/main before changing the target
repository; ask me only for choices you cannot safely infer, complete the setup,
and return the Workbench bookmark and verification results. Do not accept
decisions or enable Git sharing on my behalf.
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
- `agent-q refs resolve <ID>... --json` batch-resolves decision, request,
  response, backlog, and agent-instance references from one verified snapshot.
  It returns canonical titles or handles, lifecycle status, revision/body
  hashes where available, bounded canonical fields, explicit record/authority
  semantics, and repository provenance. Terminal backlog items preserve the
  original filing but lead with bounded outcome notes instead of presenting a
  stale summary as governing meaning.
  `--file` scans an explicit project-local file, including ignored private
  integration files, while `--stdin` supports provider-neutral hooks. See the
  [reference context contract](docs/reference-context-contract.md) for the
  versioned envelope and exit semantics.
- `agent-q context bootstrap --pretty` emits one bounded, mutation-free
  `agent-mesh.context-bootstrap.v1` envelope with canonical provenance,
  fixed-target managed-contract inspection, a structured freshness cursor,
  and truthful lifecycle-delivery states. `--prior-cursor` distinguishes
  append, rollback, divergence, store, and contract changes. Optional
  `--report` inputs cannot self-assert installed verification; wheel-available
  `--builtin-mapping claude|codex-hermes|generic-local` fixtures prove schema
  conformance only.
  See the [context delivery contract](docs/context-delivery-contract.md).
- `agent-q decisions preflight --path <repo-relative-path> --json` returns all
  applicable current decisions from one verified, mutation-free snapshot. It
  labels the bounded context `project_private`, uses human-readable decision IDs
  by default, and explains each path match or exclusion. Add `--digest` instead
  of `--json` for a compact prompt-ready projection with the rule, tier validity,
  body pin, path match, and verification command.
- `agent-q decisions hook` accepts a closed edit/write JSON request on stdin and
  returns the same bounded context for optional retrieval-on-write integrations.
  The [hook contract](docs/decision-hook-contract.md) is provider-neutral,
  advisory, and never installs or launches an agent harness.
- `agent-q dispatches run --live` creates or selects a REQ, freezes one
  `dispatch.v1` policy and response slot, proves declared and effective runtime
  capabilities, launches a bounded attempt, and records its material RES.
  `agent-q dispatches assure` derives current `review.v1` evidence from the
  exact dispatcher-bound RES, while
  `agent-q dispatches gate --policy ... --json` evaluates freshness and
  advisory/blocking enforcement without writing state. Keep long review text in
  project-owned files and attach typed references instead of copying it into a
  RES. See the [dispatch](docs/dispatch-contract.md) and
  [review assurance](docs/review-assurance-contract.md) contracts.
- `agent-mesh check decisions --mode pr|staged|worktree|full --json` derives the
  actual local Git path set and evaluates it through the same mutation-free
  matcher. It keeps rename/copy and deleted paths, adds untracked paths in local
  modes, never fetches or runs stored checks, and remains advisory in 0.4.0.
- `agent-mesh doctor --context-budget` reports candidate resident bytes and
  estimated tokens for configured root instruction files and representative
  decision-digest hook output. `--scope registered-projects` measures only the
  explicit machine-local project registry; it never searches the home directory.
- Process a feedback REQ by retrieving it, recording findings on the same
  thread, and creating or linking backlog work when implementation is needed.
  Preserve the REQ as the provenance-bearing source for any derived backlog
  items.
- Write durable changes through the CLI or Workbench and verify the resulting
  record. You may propose or revise decisions but must never approve one for the
  human.

### Address durable agent instances

Use a public `<participant>-<durable-role>` handle when parallel work contexts
share one participant. A trusted runtime integration normally registers or
resumes the correct instance through an automatic handshake. It resolves exact
continuity without prompting, stops on contradictions, and presents handles
when genuine ambiguity requires a human choice.

Exact-resumable Dispatch profiles scope provider context to the resolved
development wave. Repeated work in that wave resumes one stable instance;
another wave receives a separate durable-workstream-qualified handle and
provider context. One-shot profiles still allocate a truthful terminal instance
per run and never claim continuity.

For an unmanaged compatibility fallback, a human can register a handle using a
precomputed project-scoped digest. Never put a raw provider session reference in
the command:

```bash
agent-mesh instance register --participant claude --provider anthropic \
  --handle claude-case-study --workstream case-study
agent-mesh instance register --participant claude --provider anthropic \
  --handle claude-design --workstream design-system

export AGENT_MESH_INSTANCE_ID=claude-case-study
```

Managed child launchers inject their binding automatically and strip inherited
parent identity. For an already-running unmanaged process, put the global public
handle before every subcommand. The same handle makes a handoff explicit:

```bash
agent-mesh --instance claude-case-study request --from claude \
  --to-instance claude-design \
  "Fix the design token" "The case study exposed a shared styling issue."

agent-mesh --instance claude-case-study backlog create --actor claude \
  --owner-instance claude-design --title "Normalize the design token"

agent-q list --to-instance claude-design --status open
agent-q backlog list --owner-instance claude-design
```

Once a participant has an active instance, unbound new events from that
participant fail instead of falling back to provider-only attribution. Normal
CLI, Workbench, packet, and view surfaces use the handle only; hidden `AI-...`
IDs are reserved for canonical internals and explicit diagnostics.

Specific-instance REQs are not launched by generic runtime dispatch. Relay the
REQ ID to the named chat, which retrieves it with
`agent-q packet --id <REQ-id>`.

The identity is durable Agent Mesh state, not cryptographic authentication or a
guarantee that a runtime integration can prove provider continuity. See
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
REQ/RES IDs, and backlog IDs resolve through Agent Mesh query commands, and
`agent-mesh check refs` can scan tracked code and fail on a dangling decision,
request, response, backlog, or instance reference. Every resolved scan result
includes its canonical title or public handle and current status; decision
results also include the full revision hash in JSON and a 12-character revision
hash prefix in plain text. Use repeated `--file` options or explicit `--stdin`
when private/ignored integration files are outside Git's tracked set. The
managed repository contract also requires you to consult canonical decisions
before a related durable choice.

The references are validated pointers, not automatic enforcement. Task
preflight, optional edit/write hooks, the Git change-set check, and the
Workbench display use the same applicability result, but Agent Mesh does not
guarantee that every agent harness installs those boundaries, detects a semantic
contradiction, or blocks a commit. Version 0.4.0 is deliberately advisory; use
the managed contract and explicit local/CI checks without treating their
presence as mandatory enforcement.

The context bootstrap does not make optional hooks part of correctness. An
all-`unreported` delivery report remains a complete, healthy zero-hook
baseline. Static Claude, Codex/Hermes, and generic-local mappings prove schema
conformance only; they do not prove that an installed model or harness invokes
the mapped lifecycle events.

Decision tier IDs are protocol vocabulary, not project-defined categories. New
writes accept exactly `note`, `implementation_plan`, `architecture_contract`,
`production_invariant`, and `compliance_security`. Historical unknown tiers stay
visible with `tier_valid=false` and effective enforcement `none`; find them with
`agent-q decisions list --invalid-tier`, then append an explicit correction with
`agent-mesh decision amend <D-id> --tier <canonical-tier> --reason <reason>`.
Correcting an accepted or in-force decision returns it to Proposed for fresh
direct-human approval. Use decision tags for project-specific categorization.

Backlog references are work-item history, not normative authority. Their JSON
resolution keeps summary, root-cause, disposition, and notes separate; consumers
must not promote an original filing summary over its later corrective outcome.

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

The managed adoption contract also installs a selective chat-to-mesh policy.
Ordinary conversation stays in chat. Clear durable work becomes a concise REQ,
and only a material outcome or evidence becomes a RES. If promotion is
ambiguous, the agent suggests the record and waits for explicit human
confirmation. Complete chat transcripts are never mirrored by default.

## Manual Quickstart (after install)

```
pip install my-agent-mesh
cd ~/your-project
agent-mesh init --participants human,agent --default-sender human --default-recipient agent
agent-mesh adopt --repo .
agent-mesh adopt --repo . --check
agent-mesh request --to agent "Review the auth refactor"
agent-q list --status open
agent-q packet --id <REQ-id>
agent-mesh workbench --repo .
```

For chat-sourced work, use the provenance-preserving promotion surface. It is
safe by default: an unspecified classification is a `promotion-candidate` and
writes nothing until `--confirmed-by` names the confirming human participant.

```bash
agent-mesh promote chat-only
agent-mesh promote request --confirmed-by human --to agent \
  --source-channel codex-chat --source-uri codex://current-thread \
  --ref backlog:BKL-123 "Implement the approved change" \
  "Concise durable work contract"
agent-mesh promote response --classification durable-event --from agent \
  --source-channel codex-chat --source-uri codex://current-thread \
  --ref backlog:BKL-123 <REQ-id> "Implementation verified" \
  "Material outcome and evidence"
```

The quickstart uses the privacy-first `local-only` default. It causes a normal
`git add -A` to select no `.agent-mesh/` path. Use
`--state-sharing git-shared` only after approving Git access to the canonical
config, event log, and externalized bodies. See `docs/privacy.md` before sharing
a repository or changing this setting.

Choose the sender identity intentionally during init. `default_sender` is the actor
used when you omit `--from`, and it is stamped into new public IDs such as
`REQ-20260708T210852Z-HUMAN-21697`. Use a personal alias like `david` if you want
named IDs, or keep `human`/`user` for a generic project-local identity.

## Automatic Workbench

The recommended adoption flow installs one automatic Workbench service for the
current user:

```bash
agent-mesh workbench service install --repo . --open
```

The command is idempotent and initially supports `launchd` on macOS,
`systemd --user` on Linux. D015 ownership activation fails closed on Windows;
Task Scheduler definition rendering remains conformance-only until a later
human-approved revision supplies reparse-safe handle traversal, durable
replacement, and an installed recovery vertical. On supported platforms the
service starts at sign-in and restarts after a failure. It serves every valid repo in the machine-local Workbench registry, so
adopting another project does not create another background process. After an
agent installs it, the human can use the stable machine-local bookmark printed
by the command without opening a terminal. Reinstalling from another project or
restarting the service refreshes that same bookmark. Use
`agent-mesh workbench service open` whenever the bookmark is not already saved;
`service status` prints both its exact path and the open command. Use `start`,
`restart`, or `repair` for managed lifecycle recovery. `service relinquish` and
`service uninstall` are separate direct-human actions that require typing
`RELINQUISH`; both disable supervisor relaunch before manual server access is
restored. Installing or refreshing the
service also turns the anchor repo's old project-local bookmark into a token-free
pointer to the managed bookmark. A manual `agent-mesh workbench --repo .` server
may access project data only when the per-user ownership record is positively
`absent` or explicitly `relinquished`. A configured service that is offline or
broken still owns Workbench-server admission; health loss never silently enables
a competing manual server. Direct `agent-mesh` and `agent-q` CLI writers remain
outside this Workbench-server ownership boundary.

Every project-backed Workbench request is bound to the current per-user ownership
generation and monotonic revision through response flush. Workbench-launched CLI
children carry the same bounded binding and revalidate it before every canonical
append. The child consumes the envelope at process bootstrap so capability
probes, runtime drivers, and provider/model grandchildren cannot inherit it; a
direct CLI process without that Workbench envelope keeps its existing authority.
The authority root comes from the OS account rather than inherited `HOME`, XDG,
or Agent Mesh configuration variables. Install, repair, start, restart,
relinquish, and uninstall are serialized through one stable OS-account lock and
a nonce-bound quiesce claim. A drain releases the kernel lock so already-admitted
requests and marker-bound child appends can finish, then reacquires it and
revalidates the same claim, generation, revision, and empty inventories before
any supervisor or ownership effect. A replacement generation quiesces and drains
the current one first. The
managed runner starts only after the final configured generation and revision
are durable, and both values must match for health and request admission.
`service status` reports persisted ownership separately from endpoint
liveness. The first upgrade from a pre-ownership Workbench requires explicitly
stopping the old process, then running `service install` (or `service repair` for
an incomplete activation). Failure before configuration remains `activating`;
a supervisor-start failure after configuration is `configured_unavailable`.
Both remain fail-closed until explicit recovery. After relinquishment,
only explicit `service start` or `service install` reactivates managed ownership,
and the human must type `ACTIVATE`; `open` and `repair` cannot reclaim it.

If a Workbench dispatch parent is interrupted, its generation-bound marker is
retained and relinquishment reports the exact direct command
`agent-q recover --resolve-dispatch=<run-id>`. That command idempotently appends
any missing D014 terminal, lease-release, and instance-terminal suffixes,
rebuilds the read model, verifies the closed canonical lifecycle, and only then
retires the marker. Retry relinquishment after it succeeds; repeating the
recovery command is a verified no-write success.

The Workbench provides feedback drafting, REQ/RES lookup, backlog/kanban review,
lightweight backlog updates, frozen dispatch launch/retry and review-assurance
status, changed-path decision applicability with exact Git match explanations,
and append-only decision creation, revision, direct-human approval, and
exact-revision rejection. A completed approval becomes `accepted` for the
non-enforcing note and implementation-plan tiers, or `in_force` for tiers with
advisory or required enforcement; these are alternative results, not successive
approval steps. Feedback, ordinary CLI messages, and managed Dispatch all mint
the same public `REQ-...` and `RES-...` identifiers. Frozen policy, attempt,
and lease IDs remain audit details rather than parallel user-facing identities.
Typed REQ,
RES, and backlog evidence in decisions opens the exact corresponding Workbench
record instead of leaving an opaque internal ID. `agent-mesh init` automatically adds the repo to
the machine-local Workbench registry, and the repository selector can switch
among every valid registered repo. The browser identifies the selected repo by
opaque repo ID rather than filesystem path; all feedback, attachments,
request-status changes, backlog updates, and decision reads/writes are resolved
against that repo on the server. A repository Markdown decision log is not a
second write surface: when one is needed for compatibility, it must be generated
from Agent Mesh and treated as read-only. The bookmark is static, while live reads and writes
require the loopback server. With the automatic service, native supervision and
the page's reconnect loop keep that server available. When an endpoint-capable
managed process detects package drift, its authenticated bookmark records one
attempt, requests a fixed zero-input self-retirement, and waits at most 90
seconds for the native supervisor. The server admits no new writes while it
drains already-admitted POSTs for at most ten seconds, flushes the result, and
then returns runner exit code 75. The browser never executes a shell command,
invokes a service manager, or accepts a browser-supplied command. If relaunch
rotates the token, the page preserves the attempt marker while reloading the
latest private bookmark once;
a successful authenticated health check clears both markers. A running Workbench fingerprints
the installed Agent Mesh Python package. If drift is present when a request is
admitted, health reports `WORKBENCH_RESTART_REQUIRED` and POST fails before its
body is read. The server rechecks after parsing and before dispatch, then latches
restart-required state. These checks detect drift; they do not synchronize
package replacement with an already-running request. A pre-endpoint, offline,
broken, uninstalled, manual, or unsupervised process still requires the explicit
service lifecycle command; configured ownership requires repair or direct-human
relinquishment before a manual server can access project state. `agent-mesh
workbench service status` reports supervisor, ownership, and authenticated API
states separately. Feedback
submits use retry-safe receipts, so reconnecting and
retrying an uncertain submission returns the original REQ instead of creating a
duplicate. Message lookup reads through the SQLite index and thread-scoped
packets; generated inbox/outbox projections are not required for grounding. A
per-server access token and restricted browser origins protect the local mutation
APIs automatically. The server is loopback-only, the HTTP launch URL carries its
token only in a URL fragment, and the managed token-bearing bookmark is private
(`0600` on macOS/Linux) and stored outside project repositories. Manual project
bookmarks remain ignored by Git.

The shipped launchd and systemd definitions are configured to relaunch a
nonzero exit. On macOS, launchd owns the exact IPv4 loopback listener and the
runner adopts only the single named socket matching its configured host and
port. That lets the bookmark's bounded health polling act as an on-demand
relaunch signal after exit `75`; a wrong socket name, count, type, or endpoint
fails closed. Existing launchd installations need `service repair` or
`service install` to receive the socket-activated definition. The Windows Task
Scheduler renderer emits a retry count within
its valid `1..255` schema range, but D015 lifecycle mutations reject Windows
before writing a definition or invoking the supervisor. Rendering definitions
does not prove an installed supervisor works. Current evidence is deliberately
platform-specific:

| Platform | Endpoint and runner evidence | Installed-supervisor relaunch |
|---|---|---|
| macOS | Two controlled live source-drift requests returned `202` and each old runner exited `75`. | Verified on 2026-09-01: launchd retained the named listener, started a replacement, rotated the private bookmark token, and authenticated health returned ready. |
| Linux | Not exercised in the release environment. | Unverified. |
| Windows | Definition rendering uses schema-valid retry count `3`; D015 activation is intentionally unsupported and fails closed. | Deferred by D015. |

Supervisor policy, rate limits, or a broken installation can still prevent a
relaunch. Use the visible status, start, restart, and install commands when the
bounded browser recovery reports unavailable. Windows relaunch remains unverified,
and activation remains unavailable until its deferred native safety and
installed-service vertical is approved and succeeds.

Agents may inspect and add machine-local registrations without human
intervention:

```bash
agent-mesh projects list
agent-mesh projects register --repo /path/to/existing/repo
```

Unregistration removes machine-local Workbench information. It requires a human
in an interactive terminal to pass a default-negative prompt and type the exact
displayed registry scope. The `agent-mesh projects unregister` CLI has no
non-interactive bypass; agents must not invoke lower-level registry writers on
the human's behalf. The OS account remains the security boundary:

```bash
agent-mesh projects unregister --repo /path/to/repo
```

Agents can also route a backlog finding to the registered repository that owns
the implementation. The command previews the boundary and requested action by
default; `--apply` is retry-safe when the caller already has explicit write
scope for both repos, while `--record-only` preserves a source-side pending
receipt when it does not:

```bash
agent-mesh backlog refer <SOURCE-BKL-ID> --to target-project \
  --reason "The implementation defect is in the Agent Mesh package"
```

The preview explains the source and target boundary, duplicate handling, and
what the human must do when the caller lacks target-repository write scope.

## Configuration

Project-local configuration lives in `.agent-mesh/config.toml`. See
`docs/configuration.md` for the supported config surface, including how to add a new
agent/participant to an existing project without rewriting historical events.
Durable instance handles are documented in `docs/agent-instances.md`. Privacy
and Git-tracking behavior are documented in `docs/privacy.md`. Managed runtime
ownership and compatibility are documented in `docs/runtime-adapter-contract.md`;
adopters who need a provider Agent Mesh does not yet ship can use the bounded,
digest-pinned one-shot escape hatch in `docs/local-runtime-drivers.md` without
claiming official support or exact provider continuity. The selected local
executable is unsandboxed project code and remains the privacy and response-content
trust boundary; the digest detects drift but does not certify behavior.

## Agent-Driven Adoption

For first-time setup in a real repository, start with `docs/adoption.md`. It is
written for the coding agent: it tells the agent how to inspect the target repo,
ask only for missing project-local input, initialize `.agent-mesh/`, verify the
chain, start the Workbench, and give the human a bookmarkable Workbench path.
