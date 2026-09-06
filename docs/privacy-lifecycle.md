# Privacy Lifecycle Contract

Development provenance: this contract is tracked by decision `D008` in the full
Agent Mesh development checkout. Published bundles do not include that
repository's canonical decision state, so public users must rely on the current
capability statement in this document, actual CLI help, and release notes. This
document defines the intended correction, redaction, retention, tombstone, and
export semantics. It does not add a delete, history-rewrite, retention, or
privacy-export command.

Agent Mesh is an append-only coordination store, not a secret store. Ordinary
mistakes and sensitive-data incidents require different remedies. A corrective
event can make a prior statement non-current, but it does not remove the prior
bytes. A view filter can hide a record, but it does not erase canonical state.
Only an exceptional, explicitly authorized rewrite of every affected copy can
remove bytes, and even that cannot prove deletion from unknown clones, caches,
or backups.

## Design premise and null alternative

The safer integrity default is strict append-only history plus access
restriction, quarantine, credential rotation, and—when necessary—a clean-store
rebootstrap. That is the current behavior and preserves the original audit
chain, but it leaves the sensitive bytes in every retained old copy. Emergency
rewrite is justified only when the authenticated storage owner determines that
continued byte presence creates more harm than losing public cross-epoch audit
continuity. It intentionally sacrifices that continuity and can invalidate prior
approvals, verification results, and publication authority. It is never an
automatic fallback for ordinary correction.

## Terms

| Term | Meaning | Removes prior canonical bytes? |
|---|---|---|
| Correction | A domain-native append that revises, supersedes, resolves, or annotates a record. | No. |
| Tombstone | An append-only instruction that excludes named records from ordinary current-state retrieval while preserving diagnostic lineage. | No. |
| Redaction | Replacement of sensitive content with a typed placeholder in a newly generated canonical-history epoch. | Yes, from the named rewritten surfaces. |
| Deletion | Removal of a non-canonical file or an entire explicitly named copy. | Only from that copy. |
| Retention | A declared duration and disposition for a class of data. | Not by itself. |
| Export | A new, non-authoritative copy produced from a verified canonical snapshot under an explicit disclosure policy. | No. |
| History epoch | An opaque identity namespace for one canonical chain. The legacy chain may use a fixed non-secret marker; every rewritten epoch uses a fresh high-entropy random identifier independent of event or log content, never a content digest. | No. |

These terms must not be used interchangeably in CLI output, Workbench, docs, or
incident records.

## Non-negotiable invariants

1. Ordinary correction never mutates or deletes `events.jsonl` history.
2. A tombstone changes ordinary retrieval eligibility, not historical truth and
   not byte presence.
3. Emergency byte removal is an offline maintenance operation invoked under the
   authenticated authority of the storage or operating-system owner, outside
   Workbench and agent dispatch. It requires a separately controlled,
   out-of-band approval record. A canonical participant or `actor` string is
   attribution, not authentication or authorization. An agent may identify the
   incident and prepare a bounded plan, but may not apply a canonical rewrite,
   delete attachments, rewrite Git history, force update a remote, or destroy
   backups on its own.
4. No removed secret, personal value, raw provider session identifier, or
   reversible encoding of it may appear in the redaction event, tombstone,
   filename, command line, log, error, or export manifest. A plain digest is not
   safe for a low-entropy value. Provider-session, launch-attempt,
   provider-inventory, grounding, scoped/keyed, and other opaque correlator
   digests are part of the redaction dependency closure when their inputs are
   removed.
5. A rewritten canonical history is a new history epoch. The new epoch is added
   to every rewritten event envelope, and every surviving line is rechained from
   line 1 so every event receives a new event hash. A public event ID may be
   preserved only after a schema-aware reference analysis establishes that the
   ID is not itself sensitive; sensitive IDs are remapped and every canonical
   reference is rewritten. Consumers compare `(history_epoch, event_id)`, not
   `event_id` alone. This requires a store/envelope format bump and a declared
   minimum reader version: pre-epoch binaries must fail closed before replay,
   rather than ignore the new envelope field. Canonical, query, export, and
   reference APIs all carry the epoch.
6. The rewrite follows the dependency closure of every content commitment,
   including body and attachment digests, decision body/revision approvals,
   verification results, publication approvals, and export approvals. It never
   changes a historical approval digest to the replacement digest: that would
   falsely claim approval of content the reviewer never saw. Authority bound to
   changed content becomes explicitly non-authoritative under a non-sensitive
   redaction reason. Affected decisions return to Proposed, affected verification
   becomes stale, and fresh direct human review is required.
7. Privacy classification controls export eligibility, not content safety.
   `public_project` text can still contain a secret or personal detail. No
   structural exporter may claim that it detects all sensitive content.
8. No export command mutates canonical events, projections, decisions, or
   approval state. Preparing an export never sends or publishes it.
9. Retention or redaction of one surface must never be reported as universal
   erasure. Reports name every inspected surface and every unresolved copy.

## Human confirmation for destructive actions

A human-triggered action is destructive when it removes the only persisted copy
of user-authored or project-registration information, deletes a retained local or
export copy, rewrites prior canonical bytes into a new history epoch, or
invalidates prior authority as part of that privacy rewrite. Starting the action
is the first confirmation. Before mutation, Workbench must present a distinct
second modal that:

- names the exact records, files, registry entries, copies, or history epoch in
  scope without echoing a sensitive value;
- states whether canonical bytes will be preserved or rewritten, which approval
  and verification authority will become non-authoritative, and that fresh human
  review is required when authority changes;
- explains that deleting or rewriting a named copy does not remove unknown
  clones, backups, caches, terminal output, or other unresolved copies;
- defaults to **Cancel**, requires a new affirmative action, and cannot reuse the
  click that opened the modal as the final confirmation.

A future server-side destructive operation uses a mutation-free prepare step.
The server returns an immutable, bounded plan and a short-lived, single-use
confirmation capability bound to the verified source snapshot, exact operation,
scope, and replacement artifact digests. The second modal renders that server
plan. Apply reacquires the relevant lock and revalidates every bound precondition
before consuming the capability or changing bytes. A stale page, changed source
snapshot, expired or reused capability, expanded scope, or changed replacement
artifact fails closed and requires a new plan plus both confirmations. A retry
uses an idempotency identity and a read-only outcome lookup; it never repeats an
ambiguous destructive mutation merely because the response was lost.

The emergency canonical rewrite defined below remains an offline maintenance
operation outside Workbench. Its CLI or offline tool must provide an equally
strong direct-human flow: interactive terminal only, mutation-free plan review,
a default-negative first prompt, and an operation-specific typed second
confirmation bound to that exact plan. Automation flags such as `--yes` cannot
substitute for either confirmation. External Git, hosting, backup, or recipient
copy deletion remains a separately authorized action for each storage owner.

Cancellation, dismissal, invalid input, stale state, timeout, process failure,
and validation failure before apply must leave canonical and external target
bytes unchanged. Tests must cover every such path plus duplicate apply, lost
response, concurrent mutation, and service restart. Ordinary append-only
correction, decision revision, retirement, or supersession is not deletion: it
preserves prior canonical bytes and continues to use its domain-native human
authority rules. Cleanup of an uncommitted temporary provider resource, or of a
transient duplicate only after its durable replacement is verified, is likewise
transactional cleanup rather than a human destructive action.

## Ordinary correction

Use the native event for the affected domain:

- revise or supersede a decision;
- post a correcting response and resolve or reopen the request as appropriate;
- update a backlog item with the reason for the correction;
- append a provenance or status correction through its supported writer.

The current projection should prefer the latest valid domain state while
retaining lineage. Diagnostic and canonical-history reads continue to expose
the earlier event. Corrections must identify the target record and explain what
changed without repeating sensitive bytes unnecessarily.

Ordinary correction is the default for wrong status, title, ownership,
classification, reasoning, or other non-secret content. It is not a remedy for
credentials, private keys, regulated personal data, or content whose continued
byte presence is itself the incident.

## Tombstones

Two future tombstone meanings are reserved and must remain distinct:

- A **retrieval tombstone** names canonical record IDs and prevents those records
  from appearing in ordinary current-state, grounding, Workbench, generated-view,
  and default-export surfaces. Explicit diagnostic history may show the record
  only to an authorized local operator. The source bytes remain canonical.
- A **rejected-value tombstone** prevents an automatic extraction system from
  silently reasserting a rejected value. Agent Mesh does not currently perform
  automatic fact extraction. If this mechanism is later added, it must use a
  scoped keyed digest for sensitive or low-entropy values rather than store the
  value or a plain digest.

A future `record_tombstoned` event must contain an opaque tombstone ID, target
record IDs, reason category, actor, timestamp, and retrieval scope. It must not
contain the rejected or sensitive value. There is no writer or projection for
this event today.

## Emergency sensitive-data removal

Emergency removal follows one bounded incident plan:

1. **Contain.** Stop Agent Mesh writers and sharing. Revoke or rotate exposed
   credentials first; history rewriting does not make an active secret safe.
2. **Inventory.** Name the affected event fields, externalized bodies,
   attachments, SQLite projections, generated views, archives, recovery
   journals, logs, exports, Git refs, stashes, notes, worktrees, reflogs,
   unreachable objects, alternates, LFS or other object storage, pull-request
   refs, remotes, CI artifacts and caches, host retention, backups, and known
   clones. Do not place the sensitive value in the inventory.
3. **Plan.** Produce a restricted manifest containing opaque incident and target
   IDs, safe replacement types, the expected history epoch, affected Git refs,
   exact repository/storage roots, the separately controlled out-of-band
   approval reference, and explicit unresolved copies. A participant or actor
   name is never sufficient approval. The manifest must not contain a digest of the
   pre-rewrite log or any old event hash: either can become an offline guessing
   oracle for a low-entropy removed value. If incident correlation genuinely
   requires a pre-state commitment, use an incident-specific secret-keyed
   commitment whose high-entropy key is stored separately. Record and enforce
   the access, retention, rotation, and destruction lifecycle of both manifest
   and key. The plan is reviewed by a human before mutation.
4. **Rewrite canonically.** A future purpose-built offline tool, running outside
   Workbench and agent dispatch, must create a new history epoch, add that epoch
   to every event envelope, replace targeted values with schema-valid typed
   placeholders, remap sensitive identifiers and references, update body hashes
   and sizes, and rechain every surviving line from line 1. It preserves event
   sequence and reference topology; omission of individual events is not an
   allowed shortcut. If a required field, identifier, reference, or authority
   record cannot be safely represented, the rewrite fails closed and the
   operator quarantines the old store and reboots a clean store instead. The tool
   follows every derived content commitment, explicitly invalidates authority
   bound to changed content, validates append-time and replay invariants,
   rebuilds projections, and proves referential integrity before atomic
   activation. Hand-editing `events.jsonl` or body files is not a supported
   substitute.
5. **Record the boundary.** The sanitized history appends a future
   `privacy_redaction_applied` event containing only the opaque incident ID,
   sanitized target record IDs, reason category, operator, approval reference,
   prior and replacement history epochs, digests of sanitized replacement
   artifacts, and unresolved-copy count. It contains no pre-rewrite digest, old
   event hash, or old-to-new hash map.
6. **Clean derived and local files.** Rebuild disposable projections and views,
   then remove only explicitly approved, exact-target attachments, bodies,
   journals, logs, and export bundles. Cleanup is repository-contained,
   no-follow and symlink-safe. A content-addressed body or attachment is eligible
   only after a full reference count over the rewritten canonical log is zero;
   one event's replacement cannot delete bytes still shared by another event.
   Hard links, snapshots, and copies outside the bounded root remain unresolved.
   Any retained original, quarantine copy, incident workspace, manifest, or
   commitment key remains sensitive and must stay access-controlled and listed
   as unresolved until its authorized retention or destruction is complete.
7. **Reconcile Git and external copies.** For Git-shared state, rewrite every
   affected branch and tag or publish from a reviewed clean history. Commit IDs,
   signatures, attestations, and approvals bound to the prior Git objects do not
   carry forward automatically. Remote replacement, force updates, fork
   coordination, and clone cleanup require separate human authorization.
   Rewriting refs does not remove blobs from object databases, reflogs,
   hosting-only refs, LFS/object storage, or host retention. Every old repository
   and object store remains an unresolved copy until its storage owner purges it.
   Local-only mode does not cover backups or copies outside `.agent-mesh/`.
8. **Attest narrowly.** Report which surfaces were replaced or deleted, which
   checks passed, which copies remain unresolved, and whether quarantine copies
   still exist. Never claim global erasure unless every relevant storage owner
   can establish it. A sanitized epoch proves only its own schema, replay, and
   hash-chain consistency. After old hashes and originals are destroyed, it
   cannot publicly prove what was removed or faithful correspondence with the
   prior epoch. Retained originals or secret-keyed evidence can support incident
   review only while remaining unresolved sensitive copies.

Until the purpose-built rewrite and verification tool exists, Agent Mesh has no
supported emergency canonical-redaction path. Operators should preserve the
store for incident analysis, restrict access, rotate credentials, and obtain a
reviewed repository/storage remediation plan rather than improvising a partial
rewrite.

## Retention classes

| Surface | Default | Permitted disposition | Important limit |
|---|---|---|---|
| `events.jsonl` and referenced canonical bodies | Indefinite | Domain correction, tombstone, or approved emergency history rewrite. | Automatic expiry would break audit and references. |
| SQLite projections and generated views | Disposable | Rebuild or remove at any time from a verified snapshot. | Removal does not affect canonical bytes. |
| Local attachments | Indefinite and excluded from Git sharing | Exact-target human-approved deletion only when the rewritten-log reference count is zero. | Paths may remain in canonical events; links and external copies require separate accounting. |
| Recovery journals and partial files | Until successful recovery is verified | Remove through recovery maintenance. | Retain while needed to prove or finish an interrupted append. |
| Diagnostic logs and Workbench bookmarks | Local operational policy | Rotate/remove after the owning process no longer needs them. | They may contain tokens, paths, or errors outside canonical policy. |
| Export bundles | Declared per export | Delete at the recorded expiry and notify recipients when required. | Agent Mesh cannot enforce deletion from copies it does not control. |
| Git history, forks, clones, caches, and backups | External owner policy | Rewrite/delete only with storage-owner authorization. | Current-tree cleanup is not historical erasure. |

Future configurable retention must be opt-in, class-specific, dry-run capable,
bounded to a verified snapshot, and auditable. It may automatically clean only
derived or explicitly ephemeral data. It must not silently expire canonical
events, referenced bodies, or attachments.

## Privacy-reviewed exports

Export is a positive-allowlist operation over one hash-chain-verified snapshot:

1. The manifest records schema version, project key, an opaque source-snapshot
   reference and sequence boundary, selection predicate, included privacy
   classes, body/attachment policy, redaction rules, omitted counts by reason,
   generated export-artifact digests, intended recipient/purpose, creator, and
   expiry. It does not disclose a raw digest of undisclosed canonical source
   bytes.
2. Default export includes only fields declared `public_project`; undeclared
   fields fail closed and are omitted with a diagnostic.
3. `project_private` fields require explicit selection and human review for the
   named recipient and purpose.
4. `sensitive_private` fields are never copied raw. Only a separately reviewed
   sanitized artifact or aggregate bound by an explicit publication approval may
   be included.
5. Bodies, attachments, internal `AI-*` IDs, raw provider references,
   provider-session digests, launch-attempt digests, provider-inventory or
   grounding digests, other opaque correlators, local paths, access tokens, and
   diagnostic output are excluded by default. An artifact digest may be exported
   only when it identifies an artifact included in the same reviewed bundle.
   Each selected body or attachment is enumerated in the manifest.
6. Structural redaction removes or replaces declared fields. It does not claim
   semantic PII or secret detection. A human reviews the rendered candidate and
   manifest before any external transfer.
7. A future publication approval binds the exact manifest digest, recipient,
   purpose, privacy classes, and expiry. Changing the bundle invalidates that
   approval. Agent Mesh never uploads or sends the bundle automatically.
8. File output is exclusive-create only at an explicitly selected, bounded path,
   with restrictive mode `0600` or the platform equivalent, no symlink or
   reparse-point traversal, atomic completion, and cleanup of partial output.
   `project_private` and sensitive-derived artifacts do not go to stdout unless
   the human explicitly selects an interactive-safe mode and acknowledges that
   terminals, pipes, scrollback, and logs can create additional copies. Every
   stdout or file copy enters unresolved-copy accounting. The candidate bundle
   and manifest remain sensitive until human review and authorized deletion.

`agent-q export ...` remains reserved as a canonical-read-only producer: public
output may go to stdout, while any private or sensitive-derived output follows
the explicit interactive-safe or bounded-file rules above. It must not append an
event or refresh a projection. A future write-side approval command belongs
under `agent-mesh privacy ...` and requires direct human operation. Neither
command is implemented today.

## Required implementation gates

The future privacy domain is not mergeable until it has:

- a domain contract with field-level privacy and authority tables;
- closed schemas for history epochs, tombstones, redaction records, retention
  policies, export manifests, and publication approvals;
- append-time and replay validation with stable stop-line codes;
- a dry-run redaction plan that never echoes target values;
- fault-injection tests proving atomic rewrite and recovery;
- authority-dependency tests proving changed decisions return to Proposed and
  stale verification/publication approvals cannot carry forward;
- topology tests proving every surviving line receives the new history epoch,
  no event is silently omitted, and unsanitizable required fields fail closed;
- backward-reader refusal plus epoch-qualified lookup, query, export, and
  cross-epoch reference tests;
- negative retrieval tests for tombstoned records across Agent Q, grounding,
  Workbench, generated views, and default exports;
- deterministic export fixtures and must-not-export secret sentinels;
- exclusive-create, restrictive-permission, symlink/reparse, partial-output,
  stdout-acknowledgement, and export-copy-accounting tests;
- shared-body reference counting plus exact-target, no-follow, symlink, hard-link,
  attachment, Git-history, backup, and unresolved-copy accounting;
- independent security/privacy review and a direct human approval gate.

No partial implementation may advertise deletion, redaction, retention
enforcement, privacy-safe export, or erasure before the corresponding gates
land.
