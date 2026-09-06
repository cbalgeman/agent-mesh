# Agent Adoption Instructions

This file is for the coding agent that is adding `agent-mesh` to a target
repository. Do not require the human to read these docs. Use this repository as
the source of truth, inspect the target repository yourself, and ask the human
only for project-local choices that cannot be inferred.
In short: ask the human only for project-local choices that cannot be inferred.

The human should be able to use either flow. The PyPI wheel installs the runtime
and command-line tools; the official GitHub repository supplies the adoption
documents:

1. Install `my-agent-mesh` from PyPI, give the agent the target repository, and
   require it to read the official sources at
   <https://github.com/cbalgeman/agent-mesh/blob/main/README.md> and
   <https://github.com/cbalgeman/agent-mesh/tree/main/docs> before setup.
2. Point an agent at the GitHub repository URL and let the agent read these
   adoption instructions before installing the package.

If you cannot access the URL because network access is unavailable, ask the
human to provide a local clone, download, or archive. Continue from the local
copy without changing the process below.

## Human-Facing Relay

This is the complete message a human should need to send to another human:

```text
Give your agent the Agent Mesh repo and your target repo. Ask your agent to read
the README, adoption, and privacy docs. Your agent will walk you through the rest.
```

Do not push CLI details, config tables, migration mechanics, or workflow policy
onto the human unless they ask. Those are the agent's job to inspect, summarize,
and implement.

## Agent Entry Prompt

If the human gives you only the repository path or URL, treat this as the task:

```text
Add Agent Mesh to my target repository. Install or upgrade `my-agent-mesh` from
PyPI. Read the official README and adoption, configuration, privacy, and
migration docs from https://github.com/cbalgeman/agent-mesh/tree/main; those
documents are not bundled in the installed wheel. Inspect my target repo;
summarize the setup decisions I need to make; and ask me only for missing
choices. Wait for my response, record my durable choices as Proposed Agent Mesh
decisions, then implement the approved setup, run verification, create a
smoke-test request, start the Workbench as an automatic per-user service, and
give me the Workbench bookmark. Show me where to review and approve my recorded
choices in the Workbench's Decisions tab.
```

## Required Source Reading

Read these files from the `agent-mesh` repository—the official GitHub source—
before modifying the target repo:

- [`README.md`](https://github.com/cbalgeman/agent-mesh/blob/main/README.md)
- [`docs/adoption.md`](https://github.com/cbalgeman/agent-mesh/blob/main/docs/adoption.md)
- [`docs/configuration.md`](https://github.com/cbalgeman/agent-mesh/blob/main/docs/configuration.md)
- [`docs/dispatch-contract.md`](https://github.com/cbalgeman/agent-mesh/blob/main/docs/dispatch-contract.md)
- [`docs/review-assurance-contract.md`](https://github.com/cbalgeman/agent-mesh/blob/main/docs/review-assurance-contract.md)
- [`docs/privacy.md`](https://github.com/cbalgeman/agent-mesh/blob/main/docs/privacy.md)
- [`docs/migration.md`](https://github.com/cbalgeman/agent-mesh/blob/main/docs/migration.md)

Use `docs/migration.md` when the target repository already has coordination
scripts, markdown queues, issue labels, chat exports, or task boards. Use
`docs/configuration.md` for participant names, aliases, identity defaults, and
generated compatibility views.

## Adoption Boundaries

Runtime integration is separate from adoption: a participant name
is a routing identity, not proof that a CLI/model is installed, authenticated,
correctly billed, or able to access the repo, tools, and internet. External
relays are separate again: preserve them as read-only evidence and explicitly
triage and reproduce their findings before promoting them into the target repo.

Requests, responses, and backlog items can preserve their source in the
first-class `workflow_origin` field exposed by the CLI as `--origin`. Use
`runtime-integration` for findings about an executable agent integration and
`external-input` for relayed findings from another repository. Do not overload
backlog scheduling fields to represent provenance.

Explicit durable delegation should use the high-level Dispatch flow when the
selected runtime integration supports it. This remains portable across models
and harnesses because enforcement depends on the frozen canonical policy,
current RES, and assurance evidence—not on an agent remembering a particular
hook. Harness-native and external/manual delegation remain valid when labelled
truthfully; they are not managed launches.

For detailed reviews, specifications, test reports, and design documents, use
project-owned files rather than stretching REQ/RES into document storage. Keep
the REQ as the concise contract and the RES as the concise material outcome,
then record typed, content-bound artifact references through `review.v1`.
Agent Mesh does not publish, copy, or grant access to a referenced file.

## Default Selective Chat-to-Mesh Policy

Adoption installs this policy in the managed `AGENTS.md`/`CLAUDE.md` contract.
Classify chat turns by their coordination value, not by whether they happened
near a coding session:

- `chat-only`: ordinary conversation, explanations, status questions, tentative
  brainstorming, and assistant narration that creates no durable obligation or
  result. Do not write an Agent Mesh event.
- `promotion-candidate`: a possible task, commitment, decision, blocker, or
  material result whose durable intent is ambiguous. Suggest one concise record
  and wait for explicit human confirmation. Do not write while waiting.
- `durable-event`: an explicit work contract, accepted coordination change,
  material outcome, evidence, blocker, or handoff. Promote it through the
  canonical command for its domain.

A REQ is the durable work contract, not a copy of the human's full message. A
RES is a material outcome or evidence for that contract, not every assistant
reply. Preserve enough detail to act and verify, while leaving small talk,
reasoning narration, repeated context, and full transcripts in chat. Use backlog
commands for durable tasks and decision commands for proposed or approved
choices instead of representing every durable fact as mail.

For chat-sourced REQ/RES records, use `agent-mesh promote`. The command requires
an explicit source channel and URI, writes `source_context_refs`,
`body_authority`, `body_fidelity`, a causal edge, and manual source-selection
metadata, then delegates to the existing canonical request/response writers.
Its default `promotion-candidate` classification refuses to append until
`--confirmed-by` names the human participant who approved promotion.

```bash
# Conversational turn: explicit no-op; no event is appended.
agent-mesh promote chat-only

# Ambiguous candidate after the human user explicitly confirms promotion.
agent-mesh promote request --confirmed-by human --to codex \
  --source-channel codex-chat --source-uri codex://current-thread \
  --ref backlog:BKL-123 "Implement the approved change" \
  "Concise durable work contract"

# Unambiguously material outcome for the REQ.
agent-mesh promote response --classification durable-event --from codex \
  --source-channel codex-chat --source-uri codex://current-thread \
  --ref backlog:BKL-123 <REQ-id> "Implementation verified" \
  "Material outcome and evidence"
```

After a promotion, verify the canonical record and source chain with
`agent-q packet --id <REQ-or-RES-id>` and
`agent-q trace <REQ-or-RES-id> --show-source`. Never scrape or mirror an entire
chat session as an adoption default.

## Backlog ID Allocation

Create a normal project backlog item without manually coordinating a
date/sequence number. `backlog create` atomically allocates
`BKL-YYYYMMDD-NN` in the project's persisted human-user IANA timezone and
prints the new human-facing ID:

```bash
agent-mesh backlog create --title "Investigate the reproduced issue" \
  --status open --lane next-up --priority P1
```

New items require `--title` or JSON field `title`. Use `backlog update BKL-ID`
when updating an existing item. `backlog upsert --id` is only for preserving a
missing externally allocated/imported ID and fails with `BACKLOG_ID_COLLISION`
if that ID already exists. The compatibility `upsert` surface can also allocate
an ID when omitted, but normal creation should use `backlog create`. Existing
arbitrary or temporary ULID-form backlog IDs remain valid without format-specific
compatibility code.

## Legacy Project Identity Migration

Repositories created before the `[project]` table remain readable. `agent-mesh
adopt --repo <path> --check` identifies that shape as a project-identity
migration, separately from stale or conflicting managed instructions. Running
`agent-mesh adopt --repo <path>` holds the project-identity lock, preserves the
existing config text and file mode, and appends one canonical `[project]` table
with the existing effective name and sender/recipient identities plus a stable
timezone, project key, and `store_...` ID. Repeated or concurrent runs reuse the
same result. Same-root legacy Workbench registry rows are upgraded through the
normal collision-safe registry path; a store ID claimed by another live root is
never reassigned.

## Target-Repo Procedure

1. Confirm the target repository root.
2. Check whether `.agent-mesh/` already exists.
3. Inspect existing coordination docs, scripts, queues, issue templates, and
   generated files.
4. Decide whether this is a fresh setup, a migration, or an existing
   `agent-mesh` project that only needs configuration changes.
5. Summarize the setup decisions for the human before implementation:
   - participant names and roles;
   - whether parallel work contexts need distinct public instance handles and
     durable roles under the same participant;
   - default human/user sender identity;
   - default recipient agent;
   - optional aliases such as `reviewers` or `all`;
   - whether this is fresh setup or a migration from an existing workflow;
   - whether compatibility views are needed;
   - whether Agent Mesh state stays `local-only` or is explicitly `git-shared`;
   - whether to add `CLAUDE.md` / `AGENTS.md` workflow instructions;
   - whether to suggest hooks or install agent skills;
   - whether managed dispatch is needed, which runtime profiles are eligible,
     and whether subject-bound review is advisory or blocking;
   - which human identities—not agent participants—may directly approve
     decisions.
6. Ask the human only for choices that cannot be inferred; then wait for the human's response before implementing the setup.
7. Install or upgrade the published package with
   `python -m pip install --upgrade my-agent-mesh`, then run
   `agent-mesh --help` and `agent-q --help`.
8. Initialize or update the target repo using the approved participants and
   defaults so the decision log is available.
9. If parallel work contexts need direct attribution or handoffs, configure a
   trusted runtime integration to perform the automatic identity handshake. Use
   `agent-mesh instance register --handle <participant>-<durable-role>` only as
   an unmanaged compatibility fallback. Bind and address normal commands by
   public handle, never by the hidden project-local ID. Do not infer provider
   continuity from a process name or window; if a provider reference is needed,
   compute its project-scoped digest privately and supply only the digest.
10. Run `agent-mesh adopt --repo .` to install the versioned managed instruction
   contract, then run `agent-mesh adopt --repo . --check`. Remove or rewrite any
   conflicting legacy instruction that still tells an agent to write a Markdown
   decision log. Tell the human that ordinary chat remains chat-only, durable
   coordination is promoted selectively, and ambiguous promotion waits for
   their explicit confirmation. If a supported runtime will perform durable
   delegation, direct agents to the Workbench Dispatch flow or
   `agent-q dispatches run`; do not present an optional hook as the delegation
   transport or as assurance evidence.
11. Ensure the repo is in the machine-local Workbench registry. `agent-mesh init`
   does this automatically; for an existing initialized repo, run
   `agent-mesh projects register --repo .`. Registration is inferred operational
   metadata, not a project choice, so do not ask the human to edit or approve an
   allowlist.
12. Record each durable setup choice as a Proposed decision. Do not record
    secrets or transient troubleshooting answers as decisions. Never accept a
    decision on the human's behalf.
13. Direct the human to Workbench's Approve and accept control, or give them the
    interactive CLI command. Wait for their direct approval action, then verify
    the accepted decision with `agent-q decisions show <decision-id>`.
14. Implement the rest of the approved integration, rebuild derived state, and
    verify the event chain.
15. Send a smoke-test request and confirm it is queryable. For a named-instance
    setup, send one request with `--to-instance`, verify it appears under
    `agent-q list --to-instance`, and confirm an unbound write by that
    participant fails closed.
16. Install or refresh the automatic per-user Workbench service, verify that its
    health check passes, and give the human the stable machine-local bookmark
    printed by the command. Use the manual server only when the native user
    supervisor is unavailable.
17. Confirm the new repo appears in the Workbench repository selector. Show the
    human the Workbench's Decisions tab and report exactly what changed,
    which user decisions were recorded, what was verified, and what input is
    still needed.

## Record the Human's Setup Decisions

The setup summary is a real input gate, not just an informational preview.
After the human responds, preserve each durable choice as a Proposed decision in
the canonical Agent Mesh decision log. Decision acceptance is a separate,
direct human action even when the human already endorsed the choice in chat. If
`.agent-mesh/` does not exist yet, initialize it with the approved identities
first; then record the proposals before making the remaining workflow changes.

Choose the next unused project-local decision ID after checking
`agent-q decisions list`. For a compact group of onboarding choices, a `note`
tier decision is usually sufficient:

```bash
agent-mesh decision propose \
  --id D001 \
  --title "Agent Mesh project setup" \
  --tier note \
  --context "The human reviewed the onboarding choices." \
  --decision "Use human and agent as participants; use agent as the default recipient; keep Agent Mesh state local-only."
```

Do not run the acceptance command as an agent. Ask the human to use Workbench's
Approve and accept control. As an optional terminal path, give the human this
command to run themselves; it displays the decision hash and requires them to
type a decision-specific confirmation:

```bash
agent-mesh decision accept D001 \
  --by human \
  --notes "I reviewed and approve this Agent Mesh setup decision."
agent-q decisions show D001
```

Replace the example ID, identities, and decision text with the approved
project-local values. Use separate decisions when choices have different owners,
lifecycles, or consequences. The event log is canonical; the Workbench reads its
SQLite projection and displays the records in the Decisions tab. Confirm that
the newly accepted decisions appear there when handing the Workbench to the
human.

## Suggested Setup Patterns

These are examples, not package defaults. Pick identities that match the target
repo and confirm them with the human before writing config.

Basic human plus one agent:

```bash
agent-mesh init \
  --participants human,agent \
  --default-sender human \
  --default-recipient agent \
  --state-sharing local-only
```

Claude primary plus Codex reviewer:

```bash
agent-mesh init \
  --participants human,claude,codex \
  --default-sender human \
  --default-recipient claude \
  --state-sharing local-only
```

For a personal setup, replace `human` with the user's preferred local identity.
If the human wants Claude to orchestrate and Codex to review, keep Claude as
`default_recipient` and add Codex as a participant or alias target.

State sharing is a separate approval choice. Recommend `local-only` unless the
human explicitly wants the canonical coordination history in Git and confirms
that every repository reader may see it. For that case only, initialize with
`--state-sharing git-shared`. Git-shared mode allowlists config, events, and
externalized bodies; attachments and generated runtime state remain local.

Append-only corrections do not remove prior bytes, and current Agent Mesh has no
automatic redaction, retention, or privacy-export command. Before recording
sensitive material or sharing a repository, read `docs/privacy.md` and the
`docs/privacy-lifecycle.md` contract. Development provenance is tracked by
decision `D008` in the full development checkout. Public packages do not include
that canonical decision state. Current CLI help and release notes must
independently advertise any capability that later lands.
Emergency canonical or Git-history rewriting, attachment deletion, remote
updates, and backup destruction always require a separate reviewed plan and
direct human authorization.

## Fresh Setup Commands

Run from the target repository. Replace the source path and identities with the
actual values for the project.

```bash
python3 -m pip install -e /path/to/agent-mesh
agent-mesh init \
  --participants human,agent \
  --default-sender human \
  --default-recipient agent \
  --state-sharing local-only
agent-mesh adopt --repo .
agent-mesh adopt --repo . --check
agent-q status
agent-q verify-chain .agent-mesh/events.jsonl
agent-mesh projects list
```

Then create and inspect a smoke-test request:

```bash
agent-mesh request --to agent "Smoke test" "Confirm agent-mesh is installed."
agent-q list --status open
agent-q packet --id <REQ-id>
```

Install or refresh the automatic Workbench service after verification unless the
target environment cannot run a local server or the human declines:

```bash
agent-mesh workbench service install --repo . --host 127.0.0.1 --port 8767 --open
agent-mesh workbench service status
```

Agent Mesh defines native supervisor formats for macOS (`launchd`), Linux
(`systemd --user`), or Windows (Task Scheduler). D015 activates the install-once,
per-user service only on macOS and Linux, where it starts at sign-in and restarts
after a process failure. Activation fails closed on Windows before filesystem or
Task Scheduler mutation. Windows support is deferred until reparse-safe relative
handles, durable replacement, and an installed recovery vertical are separately
approved and verified. The supported desktop setup does not require an
administrator account. Re-running `service install` is safe and updates the
existing definition, so each adopting agent should run it instead of asking
whether another repo already installed the service.

There is one service and one multi-repo Workbench per user, not one background
process per project. Registered repositories appear in the repository selector.
After the adopting agent installs the service, the human can open the bookmark
without opening a terminal. The managed page replaces the command strip with
automatic-startup status and a `Reconnect` button. When a restart leaves an
already-open page with the prior access token, the page automatically reloads
the latest private bookmark once. A successful health check clears that bounded
attempt so a later restart can recover too; `Reconnect` remains the explicit
fallback. When an endpoint-capable managed process reports exact
`WORKBENCH_RESTART_REQUIRED`, the bookmark first records one attempt in its URL,
then sends one authenticated zero-body request to the fixed restart endpoint.
The server closes new POST admission, waits at most ten seconds for admitted
writes to finish and flush their responses, flushes a fixed acceptance, and
returns runner exit code 75. The bookmark does not retry an indeterminate POST;
it polls health at most 45 times over 90 seconds, with a two-second bound on each
request. A replacement-token 403 carries the attempt marker through the existing
single bookmark reload, and only a successful authenticated health response
clears both markers. It does not execute a shell command. Browser pages cannot
safely and portably launch arbitrary native processes, so native supervision is
the cross-platform startup boundary.

The server fingerprints the installed Agent Mesh Python package when it starts.
If drift is present when a request is admitted, the health endpoint reports
`WORKBENCH_RESTART_REQUIRED` and POST fails before its request body is read. The
server checks again after parsing and before dispatch, then latches the
restart-required state so an older in-flight response cannot re-enable controls.
These checks are fail-closed drift detection, not filesystem-update/request
synchronization. A process from before the restart endpoint needs one final
explicit stop followed by service installation to establish D015 ownership.
Endpoint-capable reachable managed processes can retire through the bounded
protocol; offline or broken configured services still require restart, repair,
or direct-human relinquishment/uninstall. Stop/start an authorized manual server
around an upgrade. `agent-mesh workbench service status` reports native
supervisor, ownership, and authenticated API state separately.

launchd `KeepAlive` and systemd `Restart=on-failure` are rendered supervisor
intent. The macOS definition also makes launchd own the exact IPv4 loopback
listener; the runner adopts only the single named listening socket matching its
configured host and port. A bounded bookmark health request can therefore
provide on-demand relaunch pressure after exit `75`, even when the GUI domain
defers unconditional `KeepAlive` work. Existing launchd installations need
`agent-mesh workbench service repair --repo <path>` or a fresh install to receive
that definition. Task Scheduler output is conformance-only while D015 Windows
activation is deferred. Rendered intent is not evidence that an installed
supervisor actually relaunched exit 75. The release evidence matrix is:

| Platform | Endpoint and runner evidence | Installed-supervisor relaunch |
|---|---|---|
| macOS | Two controlled live source-drift requests returned `202` and each old runner exited `75`. | Verified on 2026-09-01: launchd retained the named listener, started a replacement, rotated the private bookmark token, and authenticated health returned ready. |
| Linux | Not exercised in the release environment. | Unverified. |
| Windows | Rendered task uses schema-valid retry count `3`; D015 activation rejects lifecycle mutation. | Deferred by D015. |

The managed panel keeps exact status, start, restart, and install commands
visible and copyable when recovery is unavailable. A platform becomes verified
only after a named installed vertical proves exactly one relaunch.

Use these lifecycle commands for diagnosis or removal:

```bash
agent-mesh workbench service status
agent-mesh workbench service open
agent-mesh workbench service start --open
agent-mesh workbench service restart --open
agent-mesh workbench service repair --repo . --open
agent-mesh workbench service relinquish
agent-mesh workbench service uninstall
```

`relinquish` and `uninstall` require the direct human to type `RELINQUISH`.
Relinquishment closes new Workbench admission, waits at most ten seconds for
admitted requests, disables native supervisor relaunch, and only then records
manual authority. A timeout or supervisor-removal failure changes no ownership
state. If an interrupted Workbench dispatch has a canonical run ID, the command
returns the exact direct-CLI `agent-q recover --resolve-dispatch=...` action;
dispatch recovery remains a separate D014 operation.
After relinquishment, only explicit `service start` or `service install` can
reclaim managed ownership, and the human must type `ACTIVATE`. `service open`
does not implicitly reactivate, and `repair` cannot replace that human action.

Use the manual server only after ownership is positively `absent` or the human
has explicitly relinquished it:

```bash
agent-mesh workbench --repo . --host 127.0.0.1 --port 8767
```

The Workbench reads the machine-local registry and lets the user switch among
registered repos. The browser sends an opaque repo ID; the server resolves that
ID against the registry before every read or write. Feedback requests,
attachments, request-status changes, and backlog updates must therefore be
recorded only in the active repo's `.agent-mesh/` state. Decision reads use the
same boundary. Workbench decision creation, revision, and acceptance routes do
as well. Never add
an API that accepts a browser-supplied filesystem path. The generated page also
uses an automatic per-server access token and restricted CORS origins; do not
replace either control with wildcard browser access. The server rejects
non-loopback hosts, the HTTP launch page receives its token only through the URL
fragment, and the generated token-bearing bookmark is written as private
runtime state outside project repositories. Manual project bookmarks remain
ignored by Git. Registered project state and attachment paths must remain
physically inside the selected repo and must not traverse symlinks.
The native service definition stores the Python executable, anchor repo,
loopback host, port, machine-local config path, and non-secret ownership
generation; it does not store the Workbench access token. A fresh token is
generated when the process starts and is written only to the private bookmark.
The ownership record and lock live in one deterministic OS-user authority root
independent of repository, virtual environment, and `AGENT_MESH_CONFIG_HOME`.
Its persisted modes are `absent`, `activating`, `configured`, and `relinquished`;
`verified`, `configured_unavailable`, `invalid`, and `legacy_uninitialized` are
observations, not persisted modes. `service status` prints the current generation
and monotonic ownership revision without exposing the token.

For the automatic service, report these outputs to the human:

- the stable machine-local bookmark path printed by `service install`;
- the same exact bookmark path and `service open` action printed by `service status`;
- the local browser URL, usually `http://127.0.0.1:8767`;
- confirmation that `agent-mesh workbench service status` reports the native
  definition and the managed health check passed.

Installing, starting, restarting, or opening the managed service rewrites the
old `.agent-mesh/workbench.html` for every valid registered repository as a
token-free pointer page. It identifies itself as the manual project bookmark and
links to the stable private managed bookmark. A later manual-server start must
preserve that pointer while current ownership supplies the valid managed target.
Invalid ownership renders recovery guidance without inventing a link. This prevents an
old repo-local bookmark from silently steering the human back to a stale manual
port or copied restart command.

Every authenticated health response identifies the server as managed or manual
and reports its installed package root, optional positively detected source
checkout, anchor repository, loopback endpoint, and port without returning the
access token. The page shows those facts above the workflow tabs. A manual server
reads the current ownership record for every project API without a negative
authorization cache. When a managed service is `activating` or `configured`, the
manual server refuses all live reads and writes before reading project state;
an authenticated generation-bound health response is shown as verified
authority, while missing or unreachable managed health is shown only as
configured-unavailable. The latter is not represented as a live listener and
does not restore manual access.

The recovery surface does not scan for or kill operating-system PIDs. Open the
private managed bookmark and stop a verified manual server from the terminal
that launched it. If managed authority is configured but unavailable, run
`agent-mesh workbench service status`, use `restart` or `repair`, or have the
human explicitly run `service relinquish`/`uninstall` before starting the manual
server. Missing or malformed ownership state is fail-closed. On the first D015
upgrade, explicitly stop every pre-D015 Workbench process, then run `service
install`; a manual launch initializes `absent` only after bounded proof that no
managed definition, metadata, bookmark, or prior activation marker exists.
The production authority root is derived from the OS account and ignores
inherited `HOME`, XDG, and Agent Mesh configuration overrides. The account-home
lock anchor is accepted only when its complete parent ancestry is neither owned
nor writable by that account, so the anchor inode cannot be replaced by a second
compliant process. Native-service
transitions use a stable OS-account lock plus a nonce-bound quiesce claim. The
kernel lock is released while already-admitted requests and marker-bound child
appends drain, then reacquired before the claim, generation, revision, descriptor
chain, and empty inventories are revalidated for definition, metadata,
supervisor, or final-record work. Reinstall and repair quiesce and drain an
existing managed generation before replacing it.

Every project-backed Workbench read or in-process mutation holds a generation
and ownership-revision lease through project access and response flush. A
Workbench-launched CLI subprocess carries a closed, bounded, expiring envelope
with that binding, repository identity, operation, and parent identity; it
consumes the envelope before launching probes or provider/model grandchildren
and revalidates before every canonical append. Direct terminal invocations of
`agent-mesh` and `agent-q` do not carry this envelope and retain their existing
canonical authority. The repository mail lock, semantic replay, D014 assurance,
and direct-human approval remain separate controls.

Managed admission requires an exact match on both the final configured
generation and `ownership_revision`; a same-generation stale runner is rejected
by health, reads, mutations, and child append admission. Incomplete dispatch
markers survive subprocess failure or parent loss. When relinquishment reports
`dispatch_recovery_required`, run its exact
`agent-q recover --resolve-dispatch=<run-id>` command from the named repository.
It performs bounded, idempotent D014 terminal/suffix reconciliation, rebuilds,
verifies the canonical lifecycle, and retires only the matching marker. Then
retry relinquishment; a repeated recovery command performs no canonical write.
Registered bookmark routing uses a non-mutating byte-, entry-, and
deadline-bounded registry read. A managed bookmark path is shown or copied into
a token-free project pointer only after a bounded no-follow read proves its
loopback endpoint matches the ownership record.

For the manual fallback, also report the restart command, shell-quoted if the
repository path contains spaces.

Also direct the human to the Decisions tab, where the choices recorded after the
onboarding approval gate should now be visible. The Decisions tab is the normal
human authoring surface: New decision creates a Proposed record, edits append a
revision, and Approve and accept records the human's direct approval action. An
accepted or in-force decision that is edited must include a reason and returns
to Proposed until the human accepts it again. Repository Markdown decision logs
are optional generated compatibility views, never writable tracking surfaces.

Tell the human to bookmark the Workbench file path. With the automatic service,
the native supervisor starts the server at sign-in and the page retries its
connection when opened or focused. An exact managed-token mismatch triggers one
automatic reload of that private bookmark; other authorization failures remain
visible and do not auto-navigate. Exact stale-code output may additionally
trigger the single bounded supervised-restart attempt described above. A drain
timeout, a stale replacement, or an expired relaunch window stops automatic
recovery and displays status, restart, repair, and direct-human relinquishment
surfaces. Reopening the unmarked bookmark is a new direct user action. With an
authorized manual server, the human or an agent must run the restart command
before using live actions.

Explain that the bookmark is a static launcher and viewer shell: live queries,
uploads, drafts, and submissions require the local server. The connection banner
must say `Server online` before the human submits feedback. When it is offline,
server-dependent actions are disabled while the client-side Clear action remains
available. Feedback submissions carry retry-safe receipts; after a lost response,
reconnecting or retrying the preserved form returns the original REQ rather than
creating a duplicate.

## Advanced Migration Procedure

If the project already has request queues, markdown handoff files, issue labels,
chat exports, or task boards, do not overwrite them first.

Follow the shadow-first process in `docs/migration.md`:

1. Inventory existing coordination sources.
2. Classify each source as source of truth, projection, wrapper, or archive.
3. Initialize `.agent-mesh/` in a branch or temporary copy.
4. Preserve source provenance on imported events.
5. Let agents review a dry-run import before appending events.
6. Keep compatibility views pointed at shadow paths until the project owner
   approves cutover.

When migrating, map the old workflow into Agent Mesh concepts explicitly:

- requests and review asks become `req_created` messages;
- replies, approvals, and blockers become response threads;
- durable tasks become backlog items;
- accepted policy or architecture choices become decisions;
- ad hoc human notes become feedback requests or backlog evidence.

If there is ambiguity, stop after the inventory and give the human a migration
brief with source-of-truth candidates, recommended mapping, risks, and the
smallest reversible first step.

## Identity Defaults

Choose `default_sender` intentionally. It is used when commands omit `--from`,
and it appears in public request/response IDs:

```text
REQ-20260708T210852Z-HUMAN-21697
```

Use a real name or handle if project-local IDs should be personal. Use `human`
or `user` when the project should remain generic.

## Instruction Files, Hooks, and Skills

Suggest updates to `CLAUDE.md` or `AGENTS.md` when the target repo has one of
those files, or when adding one would make the workflow easier for future
agents. Keep the instructions short and operational:

- use `agent-q packet --id <REQ-id>` or `agent-q thread <REQ-id>` for grounding;
- create requests for review handoffs instead of relying on chat memory;
- verify the chain after write-heavy coordination work;
- use the Workbench for human feedback and request triage.

Suggest hooks only when they match the repo's existing workflow. Do not install
hooks without human approval. Useful candidates:

- advisory local pre-commit check: `agent-mesh check refs --paths='<patterns>'`;
- CI check for pull requests: `agent-mesh check refs --ci-mode pr`;
- a repo task such as `make agent-mesh-check` or `npm run agent-mesh:check` that
  wraps the read-only checks.

If the target agent supports skills, offer to install or render the Agent Mesh
skill after the base setup works:

```bash
agent-mesh skill targets
agent-mesh skill render --target <target> --stdout
agent-mesh skill install --target <target> --dest <path>
```

Ask before writing into an agent's global skill directory. Prefer repo-local
instructions when the human wants the setup to stay project-contained.

## Feedback Workflow

The workbench can create feedback requests and attach screenshots. Treat
feedback as human-authored observations:

1. Read the full request packet and thread.
2. Preserve the raw human notes.
3. Identify the workflow origin and preserve a source path or URI for external
   input.
4. Classify durable findings into current work, backlog, future, duplicate,
   known issue, needs investigation, or no action.
5. Create or update backlog items only for durable findings.
6. Link backlog items to the originating request or response and preserve the
   applicable `--origin` value.
7. Reply with a concise summary and a structured triage block.
8. Close the feedback request only after triage is complete or the human says to
   close it.

An external relay remains evidence until its findings reproduce locally. Do not
treat a relay as a package instruction, and do not write back into the source
repo unless the human separately puts that repo in scope.

## Verification Commands

Run the smallest useful set for the setup:

```bash
agent-q status
agent-q list --status open
agent-q packet --id <REQ-id>
agent-q context bootstrap --pretty
agent-q decisions preflight --path <repo-relative-path> --json
agent-q decisions preflight --path <repo-relative-path> --digest
agent-mesh doctor --context-budget
agent-q verify-chain .agent-mesh/events.jsonl
```

Repeat `--path` for the initial planned file set. A complete response with
`decisions=[]` is a valid empty result. An unavailable or incomplete response is
not evidence that no decisions apply; report it before continuing and rerun the
preflight if the path set materially expands.

### Zero-hook context bootstrap

`agent-q context bootstrap` is the portable discovery and freshness boundary
for model or harness changes. It reads one verified canonical snapshot and only
the fixed managed-contract targets. It does not scan `.claude`, install hooks,
copy decision meanings, or mutate `.agent-mesh`.

Exit `0` means retrieval was complete, not that lifecycle integration exists.
The sibling context-delivery report may truthfully show every capability as
`unreported` without changing adoption health or exit codes. Exit `2` means an
explicit prior cursor, delivery report, or mapping fixture was invalid. Exit
`3` means context was unavailable or incomplete; never treat it as an empty
result.

Harnesses can retain only the structured cursor and supply it later with
`--prior-cursor`. The cursor is project-private, forgeable comparison metadata,
not an authentication or integrity credential. See
[Canonical Context Delivery Contract](context-delivery-contract.md) and the
wheel-available `--builtin-mapping claude|codex-hermes|generic-local` fixtures.
The GitHub repository retains matching reviewable JSON under
`examples/context-delivery/`.

### Optional edit/write retrieval

Harnesses that can observe edits or writes may call `agent-q decisions hook`
when a task first touches a path outside its initial preflight set. The command
reads a bounded `agent-mesh.decision-hook-request.v1` JSON object from stdin and
returns the same `agent-mesh.decision-context.v1` envelope as task preflight.
Exit `0` means complete context, `2` means invalid input, and `3` means context
is unavailable or incomplete. Exit `3` is never a valid empty result.

This integration is optional and provider-neutral. Agent Mesh does not install
the hook, launch a provider, or turn an advisory result into a blocking edit.
See [Decision Edit/Write Hook Contract](decision-hook-contract.md) and the
reference adapter in `examples/decision-hook/harness.py`. That contract also
ships a Claude Code `PreToolUse` recipe which injects the compact digest for the
actual `Edit` or `Write` path without making a permission decision.

### Context-budget inventory

`agent-mesh doctor --context-budget` measures the configured repository-root
instruction files plus representative `preflight --digest` output. The default
files are `AGENTS.md`, `CLAUDE.md`, and `MEMORY.md`; missing files contribute
zero bytes and unsafe or changing files make the report incomplete. Token counts
use a transparent four-bytes-per-token estimate rather than claiming a provider
tokenizer result. `residency_status` remains `unknown` for files and `potential`
for hook samples because Agent Mesh cannot prove what a harness actually loaded.

Use `--scope registered-projects` only when you deliberately want a machine-wide
comparison across the explicit Agent Mesh project registry. The command does not
walk project trees, `.claude`, user home directories, transcripts, or credentials,
and it does not append canonical events. Exact duplicate hashes identify review
candidates without claiming that differently worded instructions are equivalent.
The command exits `0` for a complete report and `3` when a safety or inspection
bound makes the report incomplete. Registry enumeration, instruction reads, and
in-memory canonical replay share one wall-clock, byte, and event budget.

Configure each repository independently:

```toml
[context_budget]
ceiling_bytes = 131072
instruction_paths = ["AGENTS.md", "CLAUDE.md", "MEMORY.md"]
hook_sample_paths = ["AGENTS.md"]
```

Instruction paths are intentionally limited to repository-root files. Hook sample
paths are repository-relative lexical paths used only for decision applicability;
they are not opened as files. Exceeding the ceiling is report-only in 0.4.0.

### Canonical decision tiers

Tier IDs are fixed protocol semantics in 0.4.0. Projects may use decision tags for
their own categories but cannot rename, remove, or weaken the five canonical tier
IDs. Preserved historical values remain readable with `tier_valid=false` and no
effective enforcement. Audit and normalize them explicitly:

```bash
agent-q decisions list --invalid-tier
agent-mesh decision amend D123 --tier architecture_contract \
  --reason "Normalize the imported historical tier"
```

An accepted or in-force record returns to Proposed after this revision and needs
fresh direct-human approval. Agent Mesh never silently rewrites the historical
event that supplied the invalid value.

If backlog or decision domains are used:

```bash
agent-q backlog list
agent-q decisions list
```

## Stop Lines

Stop and ask the human before proceeding when:

- participant names or default sender identity are unclear;
- Git-shared state is requested without confirmation that repository readers may
  see the coordination history;
- an existing coordination system has more than one plausible source of truth;
- a migration would overwrite live files;
- chain verification fails;
- a request references unknown participants;
- generated files appear hand-edited and the project owner has not approved
  regenerating them.
