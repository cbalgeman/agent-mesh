# Migrating an Existing Workflow

This guide is for teams that already coordinate through scripts, markdown files,
issue trackers, chat logs, or custom queues and want to move to `agent-mesh`
without losing history or breaking their current workflow.

The migration model is intentionally project-owned. `agent-mesh` provides the
event log, schema, CLI, projections, and recovery tools; your project owns the
one-time importer and any compatibility wrapper that understands your current
files or APIs.

## Migration Principles

- Keep the old workflow readable until the new event log is verified.
- Import into a shadow `.agent-mesh/` first; do not overwrite live files on the
  first pass.
- Preserve provenance. Every imported message should say where it came from and
  how much authority that source has.
- Let agents review the migration output. They are good at finding missing
  roots, duplicated work items, weak source links, and stale compatibility docs.
- Move new writes to `agent-mesh` before removing old read surfaces.

## 1. Inventory the Current System

Have an agent produce a short migration inventory before writing code:

```text
List every current coordination source:
- request/response queues or markdown files
- issue tracker labels or project boards
- decision records
- backlog/task documents
- chat or agent-session history
- scripts that create, update, archive, or summarize work
- IDs and status values that users already recognize
```

For each source, classify it:

- source of truth: canonical state today
- projection: derived from another source
- convenience wrapper: script or UI around the state
- stale/archive: useful for search but not authoritative

Only one imported path should become canonical in `agent-mesh/events.jsonl`.
Everything else should become provenance, a compatibility view, or remain outside
the package.

## 2. Initialize a Shadow Mesh

Work in a temporary copy or a feature branch first:

```bash
cd ~/your-project
agent-mesh init --participants human,builder,reviewer \
  --default-sender human \
  --default-recipient builder \
  --state-sharing local-only
agent-q verify-chain .agent-mesh/events.jsonl
```

Keep the shadow mesh `local-only` unless the project owner explicitly approves
sharing canonical coordination history through Git. Older Agent Mesh configs
without `[version_control].state_sharing` retain the former Git-shared behavior;
review and migrate that setting before assuming an existing mesh is private. See
`docs/privacy.md` before committing migration output.

If existing users expect markdown files, configure compatibility views to shadow
paths rather than live paths:

```toml
[compatibility_views]
inbox = "docs/coordination-shadow/inbox.md"
message_log = "docs/coordination-shadow/message-log.md"
archive_dir = "docs/coordination-shadow/archive"

[compatibility_views.outbox]
builder = "docs/coordination-shadow/outbox-builder.md"
reviewer = "docs/coordination-shadow/outbox-reviewer.md"
```

Render and inspect the shadow files:

```bash
agent-q rebuild --all
agent-q render --all
agent-q verify-chain .agent-mesh/events.jsonl
```

## 3. Write a Project-Owned Importer

Put importer code in your repository, not inside `agent-mesh`. The importer
should translate your legacy records into canonical event payloads and preserve
the original source in metadata.

Recommended import metadata:

- `import_batch_id`: stable ID for one migration run
- `legacy_id`: original ID in the old system
- `legacy_source_path` or `source_uri`: where the original record lived
- `source_context_refs`: file, chat, issue, or tool evidence for the import
- `body_authority`: `human_chat`, `tool_payload`, `agent_mail`, `unknown`, etc.
- `body_fidelity`: `full`, `metadata_only`, `inferred`

Prefer a dry-run mode that writes a JSON plan first. Then have an agent review
the plan before appending events.

## 4. Recover Source Context From Agent History

If your old workflow was driven by chat or agent sessions, use recovery reports
to decide which chat/tool turn should be attached to each imported message.

Create a file containing the IDs you want to audit:

```text
REQ-20260701T120000Z-BUILDER-00001
RES-20260701T121500Z-REVIEWER-00002
```

Scan one or more JSONL histories:

```bash
agent-q recover-sources \
  --ids-file migration/ids.txt \
  --source ~/history/codex.jsonl \
  --source ~/history/claude.jsonl \
  --output migration/source-ledger.json \
  --pretty
```

Reduce that ledger into a review manifest:

```bash
agent-q audit-recovered-sources \
  --ledger migration/source-ledger.json \
  --output migration/source-review.json \
  --pretty
```

For reviewed source matches, create a promotion plan:

```bash
agent-q plan-source-promotions \
  --promotion-review migration/source-review.json \
  --output migration/source-promotion-plan.json \
  --pretty
```

Treat these reports as evidence. Do not blindly promote weak candidates; keep
`requires_review=true` records in a manual queue.

## 5. Agent-Driven Review Loop

Ask one agent to import and another to review. A useful review prompt:

```text
Review this migration dry-run.

Check:
- every imported request has an original source or an explicit metadata-only caveat
- every response points to an existing request
- statuses and timestamps are copied, not guessed
- legacy IDs remain searchable
- no live compatibility file is overwritten
- `agent-q verify-chain` passes
- `agent-q list`, `agent-q thread`, and `agent-q packet` show useful context

Return blockers first, then safe-to-run commands.
```

The review should run narrow commands against the shadow mesh:

```bash
agent-q status
agent-q list --status open
agent-q thread <REQ-id>
agent-q packet --id <REQ-id>
agent-q events --kind req_created --json
agent-q verify-chain .agent-mesh/events.jsonl
```

## 6. Cut Over Gradually

Use this order:

1. Import historical records into a shadow mesh.
2. Rebuild, render, and verify the chain.
3. Compare shadow compatibility views against the legacy surface.
4. Move new writes to `agent-mesh`.
5. Keep old files as read-only projections or archives.
6. Only point compatibility views at live legacy paths after a backup, hash
   manifest, restore test, and explicit approval.

If a legacy date, status, or owner is wrong, correct it in the importer or append
a migration correction event with provenance. Do not hide project-specific
corrections in package display code.

## 7. Keep the Package Boundary Clean

Reusable fixes belong in `agent-mesh` when they apply to many projects: schema
validation, chain verification, rendering, generic CLI commands, recovery
reports, and workbench affordances.

Project-specific behavior belongs in your repo: old filename conventions,
custom date repairs, issue-tracker adapters, domain-specific backlog rules, and
one-time import parsers.
