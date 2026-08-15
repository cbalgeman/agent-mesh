# Changelog

All notable public changes to Agent Mesh are recorded here. Agent Mesh is
pre-1.0, so minor releases may change command or storage contracts.

## [0.3.0] - Unreleased

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
- A curated public verification pack and CI checks for supported Python
  versions.

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
   `python -m pip install --upgrade agent-mesh==0.3.0`.
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

[0.3.0]: https://github.com/cbalgeman/agent-mesh/releases/tag/v0.3.0
