# Durable Dispatch Contract

Agent Mesh dispatch is a versioned delegation contract built on the existing
REQ and RES registers. A REQ remains the durable work request. A RES remains the
material outcome. Dispatch adds a frozen policy, bounded attempt lifecycle, and
truthful provenance; it does not mirror a chat transcript or rename every
delegation as a review.

The null path remains supported: a human or harness may delegate work outside
Agent Mesh and later record a provenance-labelled material result. Agent Mesh
does not claim to intercept every subagent or require a hook in every harness.
Such work cannot satisfy a formal managed-dispatch or subject-bound review gate
unless it is recorded through the applicable contract with current evidence.

## `dispatch.v1`

One frozen policy binds exactly one REQ and one response slot. It records:

- purpose and role;
- participant, durable role, and selected runtime profile;
- required capabilities and permission ceiling;
- response and optional artifact contracts;
- provenance requirements and management level;
- retry limit;
- optional exact review subject and `review.v1` policy.

The policy is immutable. A retry creates a new attempt against the same policy,
REQ, subject, slot, and policy digest. It never creates a replacement REQ.
There may be at most one active attempt for a policy. A newer active attempt
suspends any older successful outcome for gate purposes.

A `single` REQ has one slot keyed by the request. A `multi` REQ has one slot per
participant recipient and, when directly addressed, per instance. Each policy
binds one slot; version 1 does not infer a request-wide result across slots.
The current outcome is the completed terminal attempt with the greatest
canonical terminal event sequence. It satisfies a gate only when it has a valid
dispatcher-bound RES.

Cancellation, timeout, parent loss, and retry-budget exhaustion are explicit
canonical facts. `agent-q dispatches cancel --run drun_...` (or **Cancel active
outcome** in Workbench) terminalizes an active attempt as `cancelled`, releases
its lease, and makes any later process result ineligible for RES posting. The
external process may take time to exit; cancellation is an outcome revocation,
not a claim that the operating system process stopped synchronously. A bounded
lease expiry lets the next retry reconcile an abandoned attempt as
`parent_lost`. Reaching the frozen maximum attempt count records one idempotent
`dispatch_retry_exhausted` fact; it never silently creates another attempt.
Cancellation and parent-loss reconciliation are suffix-idempotent: if the
dispatcher stops after writing the terminal event, the next invocation repairs
only the missing lease release and instance terminal facts before returning.

When a `review.v1` policy declares reviewer quorum, qualifying assurances may
come from distinct current response slots of the same REQ only when their exact
subject and assurance-policy revision match. This makes multi-reviewer quorum
explicit without allowing an unrelated successful review to satisfy the gate.

## High-level flow

Use the Dispatch tab in Workbench or the high-level CLI. Both create or select a
REQ, freeze policy, resolve the target, run preflight, launch one managed
attempt, and record the bounded RES outcome:

```bash
agent-q dispatches run --live \
  --target codex --profile sol_review --actor human \
  --message REQ-... --purpose review --role reviewer \
  --subject-decision D014 --max-attempts 2
```

After a completed review attempt, the same high-level run derives its structured
`review.v1` assurance from the exact RES. A managed Workbench invocation permits
that append only when the assurance names the invocation's bound run. Flagging
or retiring assurance remains a separate, explicitly scoped operation.

Create a concise REQ inline when one does not already exist:

```bash
agent-q dispatches run --live \
  --target codex --profile sol_review --actor human \
  --title "Review the exact pending revision" \
  --body "Return a bounded verdict; put detail in docs/reviews/D014.md." \
  --purpose review --subject-decision D014
```

Continue from a completed managed result by using its public RES reference:

```bash
agent-q dispatches run --live \
  --target codex --profile sol_review --actor human \
  --title "Follow up on the accessibility review" \
  --body "Address the remaining keyboard-navigation finding." \
  --continue-response RES-...
```

A follow-up is a new durable REQ whose `refs` include the prior RES. It is not
another RES in the old request and it does not turn Dispatch into a chat
transcript. Agent Mesh inherits the prior Workstream, runtime profile, stable
managed session identity, and a bounded copy of the prior request/result thread.
An exact-resumable runtime also reuses its provider session; a one-shot runtime
receives the durable bounded context in a new process. Do not pass a separate
`--workstream` for a follow-up. To change the Workstream, profile, or agent,
start an independent request instead.

Workbench presents these independently durable records as one expandable work
chain beneath the originating Dispatch row. Each follow-up remains separately
addressable and auditable, including failed or not-started sibling branches;
grouping does not rewrite or collapse the canonical REQ, RES, or policy records.
The originating row displays its public REQ reference, and every completed row
displays its public RES reference; follow-up rows display both so they can be
found directly in Messages. When selecting an existing request, Workbench
searches canonical REQs by public ID or title, limits choices to requests
addressed to the selected AI agent, and confirms the exact REQ again before the
dispatch starts. It refreshes Dispatch policies during that confirmation so a
REQ that already has a frozen policy is visibly unavailable instead of failing
after launch: completed work points to **Continue this work** on its RES,
retryable work points to **Retry selected work**, and exhausted work points to a
new request. Policy and attempt identifiers remain in the row's audit details.

Retry without changing the contract:

```bash
agent-q dispatches run --live \
  --target codex --profile sol_review --actor human \
  --policy dpol_...
```

Cancel an active canonical outcome:

```bash
agent-q dispatches cancel --run drun_... --actor human
```

Lower-level `once` and `worker` commands remain available for compatibility and
operations. They do not retroactively turn their legacy lifecycle records into
`dispatch.v1` policy evidence.

## Routing and runtime profiles

A participant is a durable coordination identity. A runtime profile selects an
execution boundary. Provider, model, harness, durable role, public instance,
and provider session are separate facts. One participant may have multiple
enabled profiles; callers must choose `--profile` when target selection would
otherwise be ambiguous. Switching profile, model, or harness does not mint a
new participant or claim provider-context continuity.

Workbench makes that atomic choice in two stages: **AI agent** selects the
provider, model, and participant; **Run setup** then shows only compatible
profiles for that agent, summarized by role, repository access, network access,
and continuity. The selected profile is still frozen as one validated execution
contract. Work type and Workstream remain separate request facts; Workstream is
the context lane, not another runtime-profile category.

Managed execution currently requires a compatible built-in or explicitly
configured project-local runtime-family driver. Manual, harness-native, and
addressed-running-instance work remain valid provenance classes for migration
and diagnosis, but this release does not let those classes satisfy an
authoritative formal review gate. They must not be reported as a managed
launch.

## Capability receipts

Preflight distinguishes declared interface support from effective capability
evidence. Every required capability must pass both layers before the first
attempt lifecycle write. Built-in probes are bounded, no-model/no-billing tests
with one deadline. Help text alone is declaration evidence, not proof that the
child can actually read the repository, run tools, or use network access.

For the built-in Codex driver, native web search and model-generated shell
networking are separate boundaries. Network-capability evidence combines the
runtime's declared native `--search` support with a bounded, no-model connection
to the configured provider boundary from the sanitized child environment. It
does not run `curl` inside the shell sandbox, which may correctly deny direct
network access. A later provider or search failure still produces a bounded
runtime failure and is not concealed by the preflight receipt.

Resumable Codex launches use a private machine-local `CODEX_HOME` without the
user's `config.toml`, so settings written by a newer Codex app cannot invalidate
the pinned managed protocol or add tools and integrations. Codex may write
owner-only project trust metadata into that private home; Agent Mesh accepts
only a bounded `projects` table of absolute paths with
`trust_level = "trusted"`. Every provider, model, MCP, plugin, hook, telemetry,
tool, or other config key fails closed. The private home references the existing
owner-only ChatGPT authentication file without copying credential bytes and
retains Agent Mesh-managed provider session state. Unsafe directories, an
unavailable authentication file, a substituted link, or unprofiled config fails
closed before an attempt is appended. Provider-preparation failures return a
bounded code and leave the frozen policy available for retry; they never emit a
Python traceback to Workbench users.

The built-in Codex driver reads the provider protocol's structurally identified
final assistant message, so it does not depend on the model reproducing textual
response markers. Exact markers remain accepted for compatibility. Generic
process output and project-local drivers still require one exact marker pair:
their stdout can mix logs, tool chatter, and candidate responses and therefore
has no trusted structural final-message boundary. Both paths enforce the same
control-character stripping, non-empty body, and 20,000-character limit before
the accepted body can enter a RES; rejected raw output is never persisted.

The receipt records evidence class, trust source, probe version, observation and
expiry times, drift inputs, profile digest, and a privacy-safe receipt digest.
A project-local driver's result remains self-attested and cannot satisfy a
blocking assurance gate. After real host preflight, Agent Mesh mints a
process-, thread-, profile-, repository-, and lock-bound one-use append authority
and consumes it under the active canonical lock when the first attempt event is
written. This authority exists only inside the supported high-level runner; it
is never serialized, cannot cross a process restart, and is discarded if the
runner fails before the planned event. Persisted receipt JSON is evidence, not
authority, and raw event/emitter callers cannot self-assert a built-in proof.
Receipt expiry authorizes only this planning boundary; later review gates prove
that the receipt was valid when planning occurred rather than requiring it to
remain live for the duration of the review. Drift-sensitive facts are checked
again immediately before launch; a denial produces a categorical outcome rather
than raw provider output.

If the canonical lock cannot be reacquired after the child process returns, the
runner writes no terminal, response, assurance, or lease-release suffix. The
active attempt remains available for bounded reconciliation under a later valid
lock instead of allowing unlocked canonical writes.

## Detailed artifacts

Keep coordination and material outcomes concise in REQ/RES. Put long reviews,
specifications, test reports, and design documents in project-owned files.
`review.v1` can reference repository files with typed, content-bound metadata:
repository path, SHA-256, byte size, media type, revision, visibility,
provenance, and subject digest. URI is a reserved schema location type, but this
release never fetches a URI and therefore never treats one as authoritative.

Referencing a file does not copy, publish, export, or grant access to it. A
missing, unauthorized, changed, or oversized artifact cannot satisfy a current
gate. See [Review Assurance Contract](review-assurance-contract.md).

## Hooks and managed instructions

Managed instructions should direct explicit durable delegation through this
flow when the integration supports it. Optional decision hooks can improve
context delivery, but they are neither a dispatch transport nor proof that a
review occurred. Formal enforcement depends on canonical policy, outcome, and
assurance evidence, so it remains portable when users switch models or
harnesses.
