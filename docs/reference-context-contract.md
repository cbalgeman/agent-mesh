# Canonical Reference Context Contract

`agent-q refs resolve` is the provider-neutral read boundary for resolving
Agent Mesh IDs at citation or rendering time. Integrations should consume its
versioned JSON rather than copy decision meanings into hooks, prompts, memory
indexes, or generated documentation.

```bash
agent-q refs resolve D019 BKL-20260828-02 --json
agent-q refs resolve --file .claude/hooks/context.py --json
printf '%s\n' 'Review D019 before changing economics.' | agent-q refs resolve --stdin --json
```

All commands run from the adopted repository. Files must resolve inside that
repository; symlink and `..` escapes are rejected. `--stdin` is explicit and
refuses an interactive terminal so an integration cannot hang accidentally.

## Envelope

Successful JSON uses `agent-mesh.reference-context.v1` and is labelled
`project_private`. The repository block binds the result to `project_key`,
stable `store_id`, projection version, verified source-log hash, event sequence,
and `read_model=in_memory`.

Each unique token has one entry in `resolutions`; `occurrences` retain every
source location and point to its resolution by index. Common resolution fields
include:

- `requested_id`, `base_id`, `fragment`, and `fragment_validation`;
- `kind`, `resolution_status`, `canonical_id`, and `alias_used`;
- `record_semantics` and `authority_class`, so consumers can distinguish a
  human-approved decision from a proposal, work-item history, coordination
  context, or an identity record;
- `title`, `status`, and `state_event_seq`;
- `revision_sha256` and `body_sha256` when the domain provides them;
- bounded `canonical_text`, its canonical source field, and truncation state;
- explicit warnings, including a visible warning for Proposed decisions.

Backlog records are not normative decisions. They use
`record_semantics=work_item_history` and `authority_class=non_normative`, and
expose independently bounded `summary`, `root_cause_summary`, `disposition`,
and `notes` values under `work_item_fields`. For a terminal item, the convenience
`canonical_text` excerpt selects notes, root cause, or disposition before the
original summary. The summary remains present because it is part of the work
item's history, but a warning tells consumers that it may describe the refuted
or completed filing rather than the outcome.

Decision section fragments are preserved but reported as unvalidated until a
canonical section-identity contract exists. Such a result is `partial`, exits
nonzero, and proves only that the base decision exists; it does not prove the
named section exists.

## Completeness and exit codes

Snapshot status and per-reference status are separate:

- `complete=true`, `context_status=complete`, and `resolution_status=resolved`
  means the reference was found in one verified snapshot.
- `resolution_status=partial` means the base decision resolved but its requested
  fragment could not be validated.
- A missing ID is a complete snapshot with `resolution_status=not_found`.
- `FBK-`, `DI-`, `J-`, and `IMP-` currently return `unsupported`, not a false
  `not_found` claim.
- A corrupt, changing, or unavailable canonical log returns
  `complete=false`, `context_status=unavailable`, and no resolutions.
- Input or output budget exhaustion returns `context_status=incomplete`, never
  a successful empty result.

JSON and plain-text output share a 256 KiB total output ceiling. Exit `0` means
every requested reference was fully resolved. Exit `1` means at least one
reference was partial, missing, or unsupported. Exit `2` is an invalid request,
and exit `3` is unavailable or incomplete canonical context.

Both commands open canonical state under the same 64 MiB source-log, 50,000
event, and 10-second snapshot-and-replay ceilings. Exhausting any ceiling fails
closed with exit `3` and no partial resolution set. When `check refs` runs
outside Git, its fallback walk does not follow symlinks and is limited to 50,000
visited filesystem entries, 10,000 files, and 5 seconds; use explicit `--file`
inputs when a larger non-Git tree needs a targeted scan.

`agent-mesh check refs` uses the same resolver. Missing supported references and
unvalidated decision fragments exit `1`, while unsupported legacy domains are
warnings. A requested `--file` that cannot be read completely is an invalid
request rather than a successful skipped scan. Ordinary scans are mutation-free;
only explicit `--record-scan` appends a scanner event.

## Authority boundary

Resolution proves identity, current canonical fields, lifecycle status, and
snapshot provenance. It does not prove that prose surrounding a citation is a
correct semantic interpretation. Proposed decisions remain visibly Proposed
with `authority_class=proposal` and must not be elevated to accepted authority
by a consumer. Backlog summaries are filed work history, not decisions; consume
the domain fields and warnings instead of treating one summary as governing
meaning.

No package upgrade changes decision lifecycle state. Existing custom
integrations that store hand-written summaries must opt into this resolver;
Agent Mesh cannot safely rewrite arbitrary ignored or private files.
