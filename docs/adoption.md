# Agent Adoption Instructions

This file is for the coding agent that is adding `agent-mesh` to a target
repository. Do not require the human to read these docs. Use this repository as
the source of truth, inspect the target repository yourself, and ask the human
only for project-local choices that cannot be inferred.
In short: ask the human only for project-local choices that cannot be inferred.

The human should be able to use either flow:

1. Pull or download this repository, then point an agent at the local checkout.
2. Point an agent at the GitHub repository URL and let the agent fetch or read it.

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
Add agent-mesh to my target repository. Use the agent-mesh repo I gave you as
the implementation source. Read its README, adoption, configuration, privacy,
and migration docs, inspect my target repo, summarize the setup decisions I need to
make, and ask me only for missing choices. Wait for my response, record my
durable approved choices in the Agent Mesh decision log, then implement the
approved setup, run verification, create a smoke-test request, start the
Workbench, and give me the Workbench file path and restart command. Show me where
to review my recorded choices in the Workbench's Decisions tab.
```

## Required Source Reading

Read these files from the `agent-mesh` repository before modifying the target
repo:

- `README.md`
- `docs/adoption.md`
- `docs/configuration.md`
- `docs/privacy.md`
- `docs/migration.md`

Use `docs/migration.md` when the target repository already has coordination
scripts, markdown queues, issue labels, chat exports, or task boards. Use
`docs/configuration.md` for participant names, aliases, identity defaults, and
generated compatibility views.

## Target-Repo Procedure

1. Confirm the target repository root.
2. Check whether `.agent-mesh/` already exists.
3. Inspect existing coordination docs, scripts, queues, issue templates, and
   generated files.
4. Decide whether this is a fresh setup, a migration, or an existing
   `agent-mesh` project that only needs configuration changes.
5. Summarize the setup decisions for the human before implementation:
   - participant names and roles;
   - default human/user sender identity;
   - default recipient agent;
   - optional aliases such as `reviewers` or `all`;
   - whether this is fresh setup or a migration from an existing workflow;
   - whether compatibility views are needed;
   - whether Agent Mesh state stays `local-only` or is explicitly `git-shared`;
   - whether to add `CLAUDE.md` / `AGENTS.md` workflow instructions;
   - whether to suggest hooks or install agent skills.
6. Ask the human only for choices that cannot be inferred; then wait for the human's response before implementing the setup.
7. Install or run `agent-mesh` from the source checkout.
8. Initialize or update the target repo using the approved participants and
   defaults so the decision log is available.
9. Ensure the repo is in the machine-local Workbench registry. `agent-mesh init`
   does this automatically; for an existing initialized repo, run
   `agent-mesh projects register --repo .`. Registration is inferred operational
   metadata, not a project choice, so do not ask the human to edit or approve an
   allowlist.
10. Record each durable approved setup choice in the Agent Mesh decision log and
   accept it on behalf of the human who made the choice. Do not record secrets or
   transient troubleshooting answers as decisions.
11. Verify the recorded decisions with `agent-q decisions show <decision-id>`.
12. Implement the rest of the approved integration, rebuild derived state, and
    verify the event chain.
13. Send a smoke-test request and confirm it is queryable.
14. Start the Workbench once so `.agent-mesh/workbench.html` exists, then give
    the human the file path, URL, and restart command.
15. Confirm the new repo appears in the Workbench repository selector. Show the
    human the Workbench's Decisions tab and report exactly what changed,
    which user decisions were recorded, what was verified, and what input is
    still needed.

## Record the Human's Setup Decisions

The setup summary is a real approval gate, not just an informational preview.
After the human responds, preserve each durable choice in the canonical Agent
Mesh decision log before completing the integration. If `.agent-mesh/` does not
exist yet, initialize it with the approved identities first; then record the
decisions before making the remaining workflow changes.

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
agent-mesh decision accept D001 \
  --by human \
  --notes "Confirmed by the human during Agent Mesh onboarding."
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

Start the Workbench after verification unless the target environment cannot run
a local server or the human declines:

```bash
agent-mesh workbench --repo . --host 127.0.0.1 --port 8767
```

The Workbench reads the machine-local registry and lets the user switch among
registered repos. The browser sends an opaque repo ID; the server resolves that
ID against the registry before every read or write. Feedback requests,
attachments, request-status changes, and backlog updates must therefore be
recorded only in the active repo's `.agent-mesh/` state. Decision reads use the
same boundary, and future decision mutation routes must do so as well. Never add
an API that accepts a browser-supplied filesystem path. The generated page also
uses an automatic per-server access token and restricted CORS origins; do not
replace either control with wildcard browser access. The server rejects
non-loopback hosts, the HTTP launch page receives its token only through the URL
fragment, and the generated token-bearing bookmark is written as private
runtime state and ignored by Git. Registered project state and attachment paths
must remain physically inside the selected repo and must not traverse symlinks.

Report all three outputs to the human:

- the bookmarkable file path, usually `.agent-mesh/workbench.html`;
- the local browser URL, usually `http://127.0.0.1:8767`;
- the restart command, shell-quoted if the repository path contains spaces.

Also direct the human to the Decisions tab, where the choices recorded after the
onboarding approval gate should now be visible.

Tell the human to bookmark the Workbench file path, use the browser URL while
the server is running, and rerun the restart command when they want the local UI
again.

Explain that the bookmark is a static launcher and viewer shell: live queries,
uploads, drafts, and submissions require the local server. The connection banner must say `Server online` before the human submits feedback. When it is offline,
server-dependent actions are disabled while the client-side Clear action remains
available. Feedback submissions carry retry-safe receipts; after a lost response, reconnecting or retrying the preserved form returns the original REQ rather than
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
3. Classify durable findings into current work, backlog, future, duplicate,
   known issue, needs investigation, or no action.
4. Create or update backlog items only for durable findings.
5. Link backlog items to the originating request or response.
6. Reply with a concise summary and a structured triage block.
7. Close the feedback request only after triage is complete or the human says to
   close it.

## Verification Commands

Run the smallest useful set for the setup:

```bash
agent-q status
agent-q list --status open
agent-q packet --id <REQ-id>
agent-q verify-chain .agent-mesh/events.jsonl
```

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
