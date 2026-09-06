# Review Assurance Contract

`review.v1` is the standard subject-bound review specialization of generic
dispatch. It is not the name of all dispatches and it never substitutes for the
human authority that approves a decision.

## Exact subject binding

Before launch, Agent Mesh resolves and freezes exactly one subject:

- a decision revision and its canonical revision hash;
- an artifact and its content digest; or
- an exact Git change set including comparison mode/base, commit identities,
  paths, statuses, modes, content object IDs, and stable no-follow hashes for
  worktree or untracked content.

Incomplete, changing, conflicted, unreadable, escaping, or oversized input
makes the subject unavailable. Request prose and a digest written by the
reviewer are not binding authority. The assurance copies the dispatcher-resolved
subject from the frozen policy. Worktree subjects are collected and
content-resolved twice under one absolute deadline and must match exactly.
Agent Mesh also rejects `assume-unchanged`, `skip-worktree`, and intent-to-add
index entries because those states can hide content from an ordinary Git change
enumeration.

## What an assurance records

A canonical assurance binds:

- frozen policy and current attempt;
- originating dispatcher-bound RES;
- exact subject and policy revisions;
- reviewer participant, instance, context, role, and independence class;
- `GO`, `PASS`, or another explicit disposition;
- bounded fatal, material, and minor finding counts;
- typed references to detailed artifacts;
- management provenance, recording time, and expiry.

Only `GO` and `PASS` are passing dispositions. Configured independence and
distinct-reviewer quorum are evaluated across policies for the same REQ, exact
subject, and assurance-policy revision. Each contributing response slot must
have its own current dispatcher-bound RES and assurance. An active retry
suspends the older assurance in that slot without erasing another participant's
current review.

## Correction and retirement

The original RES and assurance are immutable. Each recorded assurance begins at
reliance state `active`, version 1. A version-bound `flag` appends a new state
when a cited claim, artifact, independence fact, or subject binding is
materially contested. Flagging preserves the reviewer's original disposition
but immediately excludes that response from gate quorum and freshness results.
It does not manufacture a `NO-GO` verdict.

Normal CLI and Workbench surfaces address assurance evidence by its public RES
reference:

```bash
agent-q dispatches assurance --response RES-... --json
agent-q dispatches flag-assurance --response RES-... --expected-version 1 \
  --reason-code stale_decision_citation \
  --note "The response cites a superseded decision revision."
```

The expected version prevents competing flags, replacement reviews, or
retirements from silently winning a race. An exact retry of the same action is
idempotent. An agent may flag suspected bad evidence, but only an identity in
the configured direct-human authority may retire a flagged assurance without a
replacement. Retirement remains non-passing and irreversible.

A replacement must be a new dispatcher-bound reviewer RES against the exact
same subject. Its closed envelope carries the public `replaces_response_id`
commitment supplied by Agent Mesh. Derivation revalidates the new review's
current attempt, subject, policy, reviewer identity, independence, capability,
artifacts, and freshness; one canonical append then activates the replacement
and marks the flagged predecessor `superseded`. Operator-authored replacement
verdicts remain forbidden.

Independence is evidence-based. For a decision revision, Agent Mesh resolves a
nested `author_provenance` commitment from the canonical revision event ID,
sequence, event hash, author participant, and any canonically bound instance or
context facts. Artifact and change-set subjects have no author identity unless a
future canonical provenance contract supplies one. Agent Mesh never accepts
caller-written author fields or synthesizes them from the dispatching CLI
session. A blocking policy that asks for an unavailable author fact fails closed
before recording assurance. An advisory policy may record the reviewer as
`external_unverified`, but that record does not satisfy independence or quorum;
the transition remains allowed only because the policy is advisory.

## Detailed review files

REQ/RES should carry coordination and the concise material outcome. Store a
detailed review, specification, or report in a project-owned Markdown or other
authorized file. The reviewer lists its repository-relative path in the closed
envelope inside the dispatcher-bound RES:

```text
AGENT_MESH_REVIEW_V1_BEGIN
{"schema":"agent-mesh.review-response.v1","policy_id":"dpol_...","policy_digest":"...","subject_digest":"...","disposition":"GO","finding_counts":{"fatal":0,"material":0,"minor":2},"artifact_paths":["docs/reviews/D014-independent-review.md"]}
AGENT_MESH_REVIEW_V1_END
```

Managed review dispatch supplies the frozen policy and subject commitments in
the prompt and derives the assurance automatically after accepting the RES.
The recovery command is idempotent and accepts no operator-authored verdict,
counts, reviewer role, or artifact list:

```bash
agent-q dispatches assure --policy dpol_... --actor human
```

Agent Mesh resolves the file without following symlinks outside the authorized
roots and stores a typed content commitment. It does not ingest the detailed
document into a RES, make the file public, or grant another runtime access.
URI references remain non-authoritative in this release because resolution
never performs network fetching.

## Freshness and gate evaluation

An assurance becomes stale when its decision revision, change set, artifact,
policy revision, current attempt, referenced detail file, or assurance validity
window changes. Capability receipt expiry is checked when the attempt is
planned: once canonical replay proves the receipt was current at that launch
boundary, a long-running legitimate review does not become invalid merely
because the short-lived launch receipt later expires. Evaluate the current gate
without writing state:

```bash
agent-q dispatches gate --policy dpol_... --json
```

The result reports whether assurance is configured, whether the evidence is
currently satisfied, whether the configured advisory/blocking policy allows the
caller to proceed, the current attempt, qualifying public RES references, and
stable reason codes. Internal assurance identifiers are diagnostic detail.
Blocking callers must require `allows_transition=true`; advisory callers may
proceed while still displaying the unsatisfied reasons.

Workbench exposes the same policy, attempts, exact RES link, human-facing
reliance state and reason, typed artifact references, and gate result. Flag and
eligible direct-human retirement actions are shown next to the affected RES;
internal assurance IDs remain in technical detail. Repository artifacts remain project-owned
and are opened with repository tooling; Workbench does not serve or follow
their paths. Its recovery action derives the exact current RES envelope without
requiring users to copy internal IDs or restate a verdict between tools.

## Project policy

The default is advisory and requires one distinct-instance reviewer. Projects
may configure the frozen policy defaults:

```toml
[review_assurance]
enforcement = "blocking"
reviewer_roles = ["architecture_reviewer", "security_reviewer"]
independence = "distinct_instance_and_context"
quorum = 2
validity_seconds = 604800
artifact_roots = ["docs/reviews", ".agent-mesh/reviews"]
authorized_uri_schemes = []
covered_decisions = ["D014"]
path_globs = ["src/agent_mesh/**"]
backlog_transitions = ["in-progress->done"]
release_gates = ["publish"]
```

The coverage lists declare where an adopting workflow intends to require the
gate. Decision acceptance and configured backlog status transitions enforce the
boundary automatically. Path and release workflows call the read-only boundary
gate explicitly, for example:

```bash
agent-q dispatches gate \
  --boundary-kind path --boundary-key src/agent_mesh/core/events.py --json
agent-q dispatches gate \
  --boundary-kind release --boundary-key publish --json
```

Exactly one frozen policy cohort must cover the boundary. A cohort is the set
of response-slot policies with the same REQ, exact subject, and assurance-policy
revision; quorum may be satisfied across that cohort. Missing or ambiguous
cohort resolution is unsatisfied. A covered decision acceptance or backlog
transition commits its qualifying canonical assurance IDs, a host-derived
`gate_evaluated_utc`, and the gate digest into the transition event. The trusted
gate time is minted while the canonical append lock and projection transaction
are held; caller-authored event time is not freshness authority. Semantic replay
recomputes the historical gate from the canonical prefix at that bound time.
Legacy acceptance events without this binding remain replayable but do not gain
retroactive assurance evidence.

A project-local self-attested capability result may appear on a nonblocking
host-preflighted attempt for diagnosis, but it cannot satisfy a blocking review
policy. Caller-authored receipt JSON is never append authority. Unbound external
review can remain useful human evidence, but it is not authoritative for a
formal gate.
