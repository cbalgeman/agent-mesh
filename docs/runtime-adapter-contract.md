# Runtime-Family Driver Contract

Development provenance: this contract is tracked by Proposed decision `D009` in
the full Agent Mesh development checkout. It is not governing policy unless and
until a human accepts that exact revision. Published packages do not contain the
development repository's canonical decision state; this document therefore
states both current capability and intended ownership directly.

Agent Mesh does not depend on a runtime adapter for ordinary coordination. Any
externally started agent can use messages, decisions, backlog, instance routing,
and verification through `agent-mesh` and `agent-q`. A runtime-family driver is
required only when Agent Mesh itself launches and manages that agent process.

## Ownership premise

Agent Mesh has one development team. Its adoption strategy must not assume that
providers, community maintainers, or adopters will publish missing adapters.
Agent Mesh owns, ships, tests, documents, and supports its official built-in
runtime-family drivers. A bounded project-local one-shot protocol gives adopters
a repair and integration escape hatch, but official multi-provider support does
not depend on adopters or third parties maintaining it.

Null alternative: retain direct Codex-only managed launch and defer a registry
until a second runtime-family vertical exists. That minimizes premature
abstraction risk, but preserves the central Codex branch and makes the second
integration a larger coupled change. Agent Mesh accepts the small registry seam
now only with mandatory fail-closed evidence, complete lifecycle-mode contracts,
a synthetic second-family managed vertical, and Codex behavior-equivalence
tests. The synthetic vertical validates the shared boundary; it does not claim
that a second real provider is currently supported.

The unit of integration is a **runtime protocol family**, not a model. One
`codex-cli` driver covers models that the installed Codex runtime exposes and
that pass the configured capability checks. Future official Claude, Gemini, or
other drivers follow their runtime families rather than multiplying by model.

## Current capability

| Operation | Current support |
|---|---|
| External/manual agent participation | Provider-neutral; no runtime driver required. |
| Managed one-shot launch | Built-in `openai` + `codex-cli` driver. |
| Project-local managed one-shot launch | Supported through explicit, digest-pinned `agent-mesh.runtime-driver.v1`; project-trusted, not official provider support. |
| Managed exact provider continuity | Built-in `openai` + `codex-cli` driver for explicitly reviewed app-server protocol versions. |
| Official built-in Claude, Gemini, Cursor, or other launch | Not implemented; built-in selection fails closed before canonical launch events. |

The runtime registry is the sole managed-dispatch selection seam. CLI and core
dispatch code resolve a profile through that registry and contain no
provider-specific construction branch. The in-tree registry has one real driver;
a synthetic second-family managed vertical proves that another owned driver can
probe a runtime catalog, preflight, construct, bind a durable child identity, and
execute through the same dispatch path without editing the CLI. It is a contract
test, not a substitute for an installed-provider vertical.

An explicitly selected project-local driver enters through the same registry
interface under the reserved `local:` namespace and `project-local` trust source.
It cannot replace or shadow an in-tree driver. The generic host, not arbitrary
project code, owns config validation, path containment, digest checks, subprocess
bounds, environment isolation, one-shot identity, and lifecycle events. The full
V1 contract and authoring workflow are in [Project-local one-shot runtime
drivers](local-runtime-drivers.md).

The local executable remains unsandboxed project code and is the privacy and
canonical-response-content trust boundary. The host cannot certify that it avoids
project mutation, prompt echo, fabricated observations, or provider-reference
output; digest pinning makes the reviewed bytes explicit, not safe.

## Compatibility policy

The profile keeps these facts separate:

- participant and durable role;
- provider and runtime-family driver;
- provider executable and observed version;
- opaque model identifier;
- repository, tool, and network capabilities;
- permission mode;
- authentication and billing boundary;
- credential denylist;
- continuity and terminal-observation requirements.

Compatibility follows the narrowest changed layer:

| Upstream change | Required action |
|---|---|
| A new model appears in an otherwise compatible runtime catalog | Select the opaque model ID in project config and rerun preflight. No registry or core code change. |
| An installed runtime version changes without changing the exercised protocol | Update the exact project profile pin only after the built-in driver proves version, catalog, auth/billing, capabilities, permissions, and isolation. |
| A provider changes CLI flags, configuration, authentication, output, or process behavior | Agent Mesh updates and releases that built-in driver and its vertical tests. |
| A project-local one-shot driver breaks | The adopter can repair its local executable, rerun conformance preflight, and deliberately update both digest pins without modifying Agent Mesh core. |
| A provider changes exact session/continuity protocol | The affected continuity mode remains disabled until Agent Mesh reviews and allowlists the new protocol behavior. One-shot or external/manual participation remains separate. |
| A new runtime family is prioritized | Agent Mesh implements and ships a new built-in driver against the shared registry contract. Core dispatch and canonical schemas do not gain a provider branch. |

This policy reduces model-release coupling; it cannot make an undocumented,
breaking provider protocol safe automatically. Unknown or unproved automation
fails closed. That failure disables only managed launch for the affected profile,
not the Agent Mesh coordination substrate.

Project-local V1 narrows release-cadence dependency for one-shot launch. It does
not make local code an official driver and does not generalize exact continuity:
provider-session discovery, replay, crash recovery, and terminal observation
remain built-in and version-reviewed capabilities.

## Built-in driver interface

Every official driver declares:

- one stable adapter ID and its supported provider identifiers;
- `ownership = "agent-mesh-built-in"`;
- `model_policy = "runtime-catalog"`;
- an allowlist of complete `(session_identity_mode, resumable,
  concurrent_attachment, terminal_observation)` lifecycle modes;
- one distinct continuity-proof check name whenever a resumable lifecycle mode
  is advertised;
- a no-prompt preflight hook;
- a factory that returns the provider-neutral `AgentRuntimeAdapter` interface.

The generic preflight owns enabled-state, executable resolution, repository
scope, and package-level credential/session-variable isolation. The driver owns
provider-specific proof of executable version, model availability,
authentication/billing, capabilities, permission mode, and any exact continuity
transport. A driver does not weaken or skip the generic checks.

The generic boundary requires exactly one driver result for version, model,
authentication/billing, capabilities, and permission mode. Missing, duplicate,
or generic-name-shadowing results fail `driver_evidence`. A resumable profile
also requires the driver's separate declared continuity proof; an ordinary
version or capability result cannot double as continuity evidence.

The factory receives only the validated profile, resolved executable, canonical
events path, and opaque project scope. It returns an adapter whose launch and
identity handshake obey the pinned runtime-profile contract above and the
durable identity-handshake contract in [Agent instances](agent-instances.md)
(development decisions D002 and D006). Provider-specific code may not enter the
canonical dispatch schema, CLI routing logic, or participant identity model.

## Required driver gates

An official driver is not enabled for live dispatch until tests prove:

- exact executable-version observation and runtime-catalog model discovery;
- authentication and billing boundary without a billed model probe;
- repository, tool, network, and permission capabilities from machine-verifiable
  surfaces rather than model-name inference;
- child environment isolation, including provider-native session variables and
  common API credentials;
- raw prompt transport outside argv and persistent launch metadata;
- automatic child instance registration and correct participant attribution;
- bounded process I/O, timeout, error, and cleanup behavior;
- no raw provider session references in canonical state, environment, argv,
  output, errors, or diagnostics;
- crash, retry, ambiguity, and duplicate-turn behavior for every claimed
  continuity mode;
- a real installed-runtime preflight and launch vertical before the support
  matrix calls the driver available.

A synthetic second-family managed vertical remains in core tests so a future
refactor cannot silently restore Codex-specific branches or weaken the shared
preflight, environment-isolation, identity-handshake, launch-spec, and lifecycle
boundaries. Each real built-in driver adds its own installed-provider tests;
passing the synthetic vertical or one real driver never certifies another.

## Deliberate non-solutions

- Do not add one adapter per model.
- Do not infer capabilities from model names.
- Do not accept arbitrary shell command templates as trusted managed drivers.
- Do not load project-local drivers as in-process Python plugins or discover them
  by scanning directories.
- Do not auto-downgrade exact continuity to a fresh session. A profile may opt
  into one-shot behavior separately, but a failed resumable proof stays failed.
- Do not advertise a provider merely because its profile parses.
- Do not make ordinary coordination depend on managed-runtime availability.

## Delivery sequence

1. Keep the owned registry, generic preflight delegation, and Codex driver
   behavior-equivalent under existing tests.
2. Retain the synthetic second-family test as the provider-neutral core gate.
3. Keep the project-local one-shot protocol stable, bounded, explicitly selected,
   and visibly distinct from official support.
4. Add the next official one-shot driver in tree, with Agent Mesh maintaining
   its provider-specific preflight and vertical acceptance.
5. Add exact continuity only when that provider exposes a machine-verifiable,
   recoverable protocol; otherwise keep the driver explicitly one-shot.
6. Publish a support matrix by runtime family, executable versions, auth/billing
   boundary, permissions, and continuity mode. Model IDs remain runtime-catalog
   data rather than a hand-maintained Agent Mesh list.

This sequence makes Agent Mesh self-sufficient without pretending that one
generic launcher can safely normalize every provider or that unknown upstream
protocol changes are compatible.
