# Context-delivery schema fixtures

These mappings demonstrate the closed
`agent-mesh.context-delivery-mapping.v1`
schema for Claude, Codex through Hermes, and a generic local harness. Validate
one while producing a bootstrap envelope:

```bash
agent-q context bootstrap --mapping examples/context-delivery/claude.json --pretty
```

These paths are reviewable source-checkout examples. Wheel-only installations
use the equivalent embedded selector:

```bash
agent-q context bootstrap --builtin-mapping claude --pretty
```

The mappings are static schema fixtures. They do not install hooks, prove that
an installed provider exposes an event, or make any capability `verified`.
`reported` means only that the mapping declares a bounded argv invocation;
`unsupported` makes a known gap visible. Agent Mesh computes the fixture's
artifact SHA-256 while validating it, independently fingerprints the installed
validator artifact, and emits a separate schema-fixture receipt. The receipt
verifies only mapping schema; it promotes no lifecycle
capability to `verified`. Installed-vertical verification is not part of this
first slice.

Mapping commands come from a fixed provider-neutral allowlist. The fixtures do
not contain decision IDs or meanings, mutable project values, provider session
references, credentials, or prompts.
