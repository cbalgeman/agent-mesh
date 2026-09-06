# Changelog

All notable public changes to Agent Mesh are recorded here. Agent Mesh is
pre-1.0, so minor releases may change command or storage contracts.

## [0.4.1] - 2026-09-06

Agent Mesh 0.4.1 removes upgrade traps found while adopting 0.4.0 in a mature,
multi-agent repository. The fixes apply to any repository with existing agent
instances, historical decisions, imported instruction files, ignored working
files, or more than one package installation.

### What is fixed for people using Agent Mesh

- Register another durable agent handle without borrowing an existing chat's
  identity. Registration is now an explicit manual bootstrap event; ordinary
  writes by a participant with active instances still require the correct
  public handle and remain fail-closed.
- Run `agent-mesh doctor` for a real setup check. It reports the package version,
  whether the running code is an editable or installed distribution, its module
  and Python locations, managed instruction targets, and adoption migrations.
- Keep one managed instruction contract when `CLAUDE.md` imports `AGENTS.md`.
  Adoption recognizes the root import, persists the chosen target set in
  `.agent-mesh/config.toml`, and removes only an Agent Mesh-owned block from a
  target that is no longer selected. Repository-authored text is preserved.
- See accepted or in-force decisions that no longer satisfy the current tier,
  scope, path-pattern, owner, or verification contract during `adopt --check`
  and `doctor`. Agent Mesh reports the repair and never rewrites the decision or
  bypasses fresh human approval.
- Detect identical managed contract blocks even when the surrounding instruction
  files differ. Projects may also declare representative per-prompt context files
  so the context budget distinguishes persistent instructions from repeated
  injection candidates without executing hooks.
- Include ignored or private working files explicitly with repeated
  `agent-mesh check decisions --path <repo-relative-path>`. The Git-derived
  comparison remains bounded and unchanged; explicit paths close only the blind
  spots the caller names.
- When a reference scan exceeds its budget, the diagnostic now points to
  `--paths`, `--file`, and `--ci-mode pr` instead of only saying to narrow the
  input.
- Use `agent-mesh --version` and `agent-q --version` directly.

### Upgrade steps

1. Back up the target repository's `.agent-mesh` directory.
2. Install `my-agent-mesh==0.4.1`, then run `agent-mesh --version` and
   `agent-mesh doctor` to confirm which installation is active.
3. Run `agent-mesh adopt --repo .`. Review any managed-block removal and the
   persisted `[adoption].contract_targets` value, then run
   `agent-mesh adopt --repo . --check`.
4. If a decision migration is reported, inspect it with
   `agent-q decisions show <D-id>`. Amend the invalid metadata only after review;
   the decision returns to Proposed and requires direct human re-approval.
5. Run `agent-q verify-chain` before new durable writes. If Git ignores governed
   files, include each changed path explicitly in `check decisions --path` or use
   the path-scoped preflight/hook.
6. Refresh an installed managed Workbench with
   `agent-mesh workbench service repair`, then verify its authenticated health.

Normal upgrade and diagnostic commands do not rewrite canonical events. This
patch does not map historical tier names automatically, infer replacement paths,
approve decisions, execute configured hooks, or make ignored files visible to
Git.

## [0.4.0] - 2026-09-05

Agent Mesh 0.4 makes durable work easier to delegate, review, find, and continue
across agent sessions. It also gives adopting agents smaller, path-specific
project guidance while keeping direct human approval as the final authority for
durable decisions.

### What is new for people using Agent Mesh

- Continue completed work from Messages or Dispatch. Follow-up requests stay
  connected to the original work and appear as an expandable chain, including
  failed or not-yet-started branches that still matter for audit.
- Choose the **AI agent** separately from its compatible **Run setup**. Agent
  Mesh still freezes the exact runtime configuration behind the scenes, while
  the Workbench presents the choice in human terms.
- Find requests and results by visible REQ and RES references. Existing requests
  can be searched by title or ID, and unavailable requests explain whether to
  continue, retry, or start new work.
- Read completed Markdown responses directly in Dispatch and open the exact
  durable Messages record when more history is needed.
- Attach independent review evidence to the exact decision, artifact, or change
  set that was reviewed. Review evidence can be corrected or superseded, but it
  never replaces direct human approval.
- Give agents compact guidance for the files they are changing instead of
  repeatedly loading a full standards document. A Claude Code hook recipe is
  included, and other harnesses can use the same path-scoped contract.
- See how much persistent project guidance may consume an agent's context. The
  context-budget report stays local, changes nothing, and can optionally compare
  registered projects.
- Recover a stale managed Workbench more safely after an upgrade or interrupted
  dispatch without silently starting a competing server.
- Rely on a more defensive public release pipeline with continuous secret
  scanning, pinned automation dependencies, weekly dependency updates, and a
  private vulnerability-reporting path. These controls protect the Agent Mesh
  public repository and release process; they do not automatically scan an
  adopter's private repositories.

### What adopting agents need to know

- Before editing, request compact decision guidance for every planned path:
  `agent-q decisions preflight --path <repo-relative-path> --digest`. An empty,
  complete result is valid; unavailable or incomplete context is not.
- Use `agent-mesh doctor --context-budget` to report persistent instruction and
  digest size. Treat the result as measurement, not permission to rewrite or
  delete project guidance.
- Route explicit durable delegation through Workbench Dispatch or
  `agent-q dispatches run` when the configured runtime supports it. Continue
  resumable work within its resolved Dispatch wave; do not represent unrelated
  harness-native work as a managed launch.
- Use public REQ and RES references for human navigation. Keep policy, run,
  lease, and hidden AI-instance identifiers in audit details.
- Treat review assurance as evidence, never as human approval. Agents must not
  accept a decision for the human.
- Use `agent-q refs resolve --json` when an integration needs canonical titles,
  statuses, revisions, or reference types instead of maintaining handwritten
  identifier summaries.
- During upgrade, run `agent-mesh adopt --repo .`, review the managed-instruction
  changes, then verify them with `agent-mesh adopt --repo . --check`.

### Technical reference

The details below preserve the exact public command, storage, runtime, and
assurance contracts for integrators and adopting agents. The linked contract
documents remain the normative implementation guides.

See the [decision-hook contract](docs/decision-hook-contract.md),
[Dispatch contract](docs/dispatch-contract.md),
[review-assurance contract](docs/review-assurance-contract.md),
[runtime-adapter contract](docs/runtime-adapter-contract.md), and
[reference-context contract](docs/reference-context-contract.md).

<details>
<summary>Expand technical contract and implementation details</summary>

#### Added

- `agent-q decisions preflight --digest` emits bounded prompt-ready decision
  guidance with canonical tier validity, rule text, body pins, exact path matches,
  and recorded verification commands. A shipped Claude Code `PreToolUse` recipe
  injects that digest for the actual Edit/Write path without deciding permissions
  or weakening the healthy zero-hook baseline.
- `agent-mesh doctor --context-budget` provides a report-only, project-private
  inventory of configured root instruction files and representative digest output,
  with byte/token estimates, per-project ceilings, exact duplicate hashes, and an
  opt-in registered-project aggregation scope.
- The public repository adds Gitleaks checks for pull requests, public-main
  pushes, weekly schedules, and manual runs; full-SHA GitHub Actions pins;
  Dependabot coverage for Python and GitHub Actions; and a private security
  reporting policy. Finding comments, summaries, and uploaded Gitleaks artifacts
  are disabled to reduce disclosure risk.

- `dispatch.v1` adds an immutable per-REQ/per-response-slot delegation policy,
  bounded same-policy retries, truthful management provenance, runtime-profile
  selection, and a high-level CLI and Workbench flow that records the exact
  dispatcher-bound RES outcome.
- `review.v1` adds exact decision/artifact/change-set subjects, configurable
  reviewer independence, quorum, validity and advisory/blocking evaluation,
  typed content-bound references to detailed project-owned review files, and
  `agent-q dispatches gate` for mutation-free transition checks.
- Review assurances now have an append-only correction lifecycle. Humans and
  operators can flag a public RES, direct-human authority can retire it, and a
  new dispatcher-bound reviewer RES can supersede it without rewriting the
  original verdict. Flagged, superseded, and retired assurances cannot satisfy
  a gate; normal CLI and Workbench views remain keyed by public RES IDs.
- Runtime preflight now records expiring capability receipts and separates
  declared interface support from effective no-model/no-billing probe evidence.
  Self-attested project-local drivers remain ineligible for blocking assurance.
- Decision evidence IDs for REQ, RES, and backlog records are now navigable to
  the exact Workbench record instead of remaining opaque text.
- A reachable stale managed Workbench can now make one authenticated,
  zero-input self-retirement request. The server stops new writes, drains
  admitted writes for at most ten seconds, flushes a fixed response, and lets
  the native per-user supervisor relaunch it through runner exit code 75. The
  bookmark preserves one bounded recovery attempt across token rotation and
  never invokes a shell or service manager.
- Workbench service ownership now has one deterministic per-user authority
  record with random generations, monotonic revisions, request leases, and
  Workbench-child append envelopes. Managed health failure no longer enables a
  competing manual server. Direct-human `service relinquish` and `uninstall`
  actions disable supervisor relaunch before restoring manual access; explicit
  `service repair` recovers invalid or incomplete activation. The authority root
  is OS-account-derived rather than `HOME`-derived, lifecycle transitions use a
  stable lock plus a nonce-bound drain claim, managed admission matches both
  generation and revision, and `agent-q recover --resolve-dispatch=<run-id>` provides the real
  idempotent interrupted-dispatch recovery path required before relinquishment.
- `agent-q refs resolve` provides bounded batch resolution of canonical
  decisions, requests, responses, backlog items, and agent-instance handles
  from one verified, mutation-free snapshot. Explicit files and stdin are
  supported for private or ignored integrations. Results distinguish proposals,
  human-approved decisions, non-normative work-item history, coordination
  records, and identities. Backlog fields remain separate and terminal items
  select notes, root cause, or disposition before a potentially stale filing
  summary.

#### Changed

- The five decision-tier IDs are fixed at every supported writer. Historical and
  out-of-band unknown tiers remain append-only and non-authoritative, can be listed with
  `agent-q decisions list --invalid-tier`, and receive exact guided amendment and
  fresh-human-approval instructions in compact digests.

- Every new request and response now uses the same public `REQ-...` or `RES-...`
  identifier whether it was created through the ordinary CLI, Feedback, or
  managed Dispatch. Workbench keeps policy, run, and lease IDs under audit
  details instead of presenting them as parallel user-facing identities.
- Exact-resumable managed runtimes now bind continuity to the resolved Dispatch
  wave. Repeated work in one wave reuses its stable public instance and provider
  context; different waves remain separate. One-shot runtimes continue to use a
  new terminal instance per run. The reviewed resumable Codex app-server
  allowlist now includes CLI `0.153.4` while retaining `0.147.0` compatibility;
  repository-local profiles still fail closed on any unreviewed executable drift.
  The private Codex home now accepts only owner-only, bounded project trust
  metadata that Codex `0.153.4` persists there; every unprofiled config key and
  link substitution remains a fail-closed provider-preparation error.
- Workbench Dispatch now accepts a human-facing workstream for continuing a
  supported provider context across new requests, shows each item's submitted
  timestamp, and displays the completed Markdown response directly with a link
  to its durable Messages record. A completed managed RES can start a new linked
  follow-up REQ from either Messages or Dispatch; the follow-up inherits the
  prior Workstream, runtime profile, stable managed session identity, and
  bounded durable thread context. Dispatch presents the originating work and
  its independently durable follow-ups as an expandable chain while preserving
  failed and not-started branches for audit. Public REQ and RES references are
  visible on the chain rows, and the Existing request control now searches
  canonical REQs by ID or title, filters them to the selected AI agent, and
  confirms the exact selection against freshly loaded Dispatch policies before
  launch. Already-dispatched REQs are visibly unavailable and point to the
  correct next action: continue from a completed RES, retry the frozen policy,
  or start a new request after retries are exhausted. The built-in Codex driver
  derives the bounded response from its structured final-message protocol;
  generic stdout and project-local drivers retain exact textual framing. A
  managed Workbench review run now retains narrowly scoped append authority for
  automatic assurance derivation from its exact completed run; flag and retire
  remain separate lifecycle operations.
- Workbench Dispatch now separates **AI agent** from its compatible **Run
  setup** choices while continuing to freeze one atomic runtime profile. Full
  Git change-set review capture has a dedicated bounded deadline, and capture
  failures return an actionable CLI/Workbench error without a Python traceback
  or partial Dispatch writes.
- Direct-human decision approval authority is now separately configurable from
  agent participants and dispatch roles. Existing projects remain in diagnosed
  legacy compatibility mode until explicit human approvers are configured;
  historical acceptances and decision status are unchanged.
- Managed adoption contract v7 directs supported durable delegation through
  Dispatch, keeps detailed reviews/specifications in project-owned files, and
  states that hooks do not prove dispatch or assurance.
- Windows Task Scheduler definitions now use a schema-valid restart count of
  three instead of the invalid value 999. Rendered definitions remain
  conformance evidence; D015 managed-ownership activation now fails closed on
  Windows pending reparse-safe traversal, durable replacement, and an installed
  recovery vertical. New macOS definitions give launchd the exact IPv4 loopback
  listener, and the runner adopts only the single named socket matching its
  configured endpoint. Bounded bookmark health polling can therefore provide
  the demand signal after runner exit 75 instead of relying only on `KeepAlive`
  in an `on-demand-only` GUI domain. A 2026-09-01 installed macOS vertical
  verified two controlled source-drift recoveries: each protected restart
  returned `202`, launchd recorded runner exit `75`, the replacement retained
  the launchd listener, rotated the private bookmark token, and returned
  authenticated ready health. Installed relaunch remains unverified on Linux
  and Windows.
- `agent-mesh check refs` now shares the verified resolver, renders canonical
  titles/statuses and decision revision hashes, uses NUL-safe Git filename
  handling with bounded local Git reads, and no longer rebuilds the SQLite
  projection during ordinary scans. Unvalidated decision fragments are partial
  results and return nonzero instead of claiming the full citation resolved.

</details>

### Compatibility, limits, and upgrade notes

- Normal upgrades do not rewrite canonical project data or change existing
  decision lifecycle status.
- Context-budget reporting is project-private, local, and report-only. It does
  not upload or modify instruction files.
- Managed Workbench restart recovery has been validated on macOS. Installed
  relaunch remains unverified on Linux and Windows, and Windows activation fails
  closed where its required safe traversal and recovery boundary is unavailable.

- Existing projects do not receive inferred human approvers. Review the actual
  human identities before adding `[decision_approval].human_approvers`. Review
  assurance remains advisory by default; configure blocking only after the
  intended transition is wired to its exact boundary. Decision acceptance and
  backlog status changes enforce configured coverage directly; path and release
  workflows call `agent-q dispatches gate --boundary-kind ...` explicitly.
- Refresh managed instructions with `agent-mesh adopt --repo .`, then verify
  contract v7 using `agent-mesh adopt --repo . --check`.
- Custom hooks or prompts that embed hand-written decision summaries should
  switch to `agent-q refs resolve --json`; Agent Mesh cannot safely rewrite
  arbitrary ignored integration files.
- A process installed before the ownership contract needs one explicit stop
  followed by `agent-mesh workbench service install` so the first ownership
  generation is established. Endpoint-capable managed bookmarks recover
  reachable package drift automatically. Offline or broken configured services
  retain ownership until direct-human repair, relinquishment, or uninstall;
  health failure alone is not manual fallback authority.

## [0.3.0] - 2026-08-15

This is the first Agent Mesh release distributed through PyPI.

### Added

- A local Workbench for human review of requests, feedback, backlog work, and
  decisions, including direct human approval of exact decision revisions.
- Stable, human-readable project, request, response, backlog, decision, and
  AI-agent instance identifiers.
- Long-lived AI-agent instance registration, attribution, direct routing, and
  backlog ownership for parallel agent chats.
- Provider-neutral runtime profiles and bounded dispatch preflight that keep
  participant identity separate from executable, model, authentication, and
  billing configuration.
- Cross-repository backlog referrals with explicit source and target ownership
  boundaries.
- Selective chat-to-mesh promotion with source provenance and explicit handling
  for ambiguous durable records.

### Changed

- Decision records now support revision history, tier requirements, affected
  paths, required checks, and durable verification outcomes.
- Decision verification executes reviewed argument vectors without shell
  interpolation, and invalid decision lifecycle events fail before append and
  during replay.
- Project identity is rename-stable, and Workbench registration uses opaque
  repository identifiers rather than exposing paths to the browser.
- Adoption installs a versioned repository contract and keeps Agent Mesh state
  local-only unless Git sharing is explicitly selected.

### Upgrade notes

1. Back up the target repository's `.agent-mesh` directory.
2. Install the release with
   `python -m pip install --upgrade my-agent-mesh==0.3.0`.
3. Run `agent-mesh adopt --repo . --check`. If the managed contract is stale,
   review and run `agent-mesh adopt --repo .`, then repeat the check.
4. Run `agent-q verify-chain` before writing new durable records.
5. Refresh the automatic Workbench with
   `agent-mesh workbench service restart` when it is installed.

Projects initialized by the public 0.2.0 code already use the structured
`[project]` configuration table and do not need a package-data migration for
0.3.0. Older or custom configurations that keep project fields only at the
top level are not automatically rewritten by the adoption command; review
those configurations separately before relying on adoption automation. The
compatibility reader can still load their supported top-level values, but
adoption cannot safely synthesize the missing project identity table without a
reviewed migration.

[0.4.1]: https://github.com/cbalgeman/agent-mesh/releases/tag/v0.4.1
[0.4.0]: https://github.com/cbalgeman/agent-mesh/releases/tag/v0.4.0
[0.3.0]: https://github.com/cbalgeman/agent-mesh/releases/tag/v0.3.0
