# Project-Local One-Shot Runtime Drivers

Development provenance: this contract is tracked by Proposed decision `D010`
in the full Agent Mesh development checkout. It is not governing policy unless
and until a human accepts that exact revision. Published packages do not contain
the development repository's canonical decision state, so this document states
the complete intended boundary directly.

Agent Mesh supports external or manually started agents without a runtime
driver. It also owns and supports a finite set of built-in runtime-family
drivers. Project-local drivers are a third, deliberately narrower option: an
adopter can build or repair basic managed one-shot launch without waiting for an
Agent Mesh release, but that local code does not become an officially supported
driver.

## V1 scope

The protocol identifier is `agent-mesh.runtime-driver.v1`. V1 permits only this
complete lifecycle mode:

```text
(session_identity_mode="none", resumable=false,
 concurrent_attachment=false, terminal_observation="process")
```

V1 does not permit exact provider continuity, provider-terminal recovery,
concurrent attachment, in-process Python plugins, arbitrary shell templates,
auto-discovery, or silent replacement of a built-in driver. Those exclusions
are protocol constraints, not missing configuration switches.

## Selection, namespaces, and trust

Local IDs use `local:<owner>:<runtime>`, with lowercase ASCII letters, digits,
dots, underscores, and hyphens in each component. Agent Mesh reserves the
`local:` prefix for project-local drivers and forbids built-ins from using it.
A local driver cannot use or shadow a built-in ID.

The runtime profile selects one exact adapter ID and declares
`driver_source = "project-local"`. There is no directory scan or precedence
rule. An Agent Mesh update cannot silently change a profile from local to
built-in or vice versa; the project must edit the profile deliberately.

The profile pins a project-contained manifest with SHA-256 and declares the
protocol. The manifest pins one project-contained executable with a second
SHA-256 and declares the exact driver ID and provider. Manifest and executable paths must be
relative, regular, non-symlink files reached without symlink traversal. Absolute
paths, `..`, path escape, unknown fields, and oversized files fail closed.

Digest matching is drift detection. It is not code signing, malware analysis,
authorship proof, sandboxing, or human authentication. A local executable is
project-controlled code with the same practical authority as any explicitly
run project tool. An agent can propose or implement a repair, but the project is
responsible for reviewing the changed executable and updating both pins.
Canonical identity records mark this boundary as `adapter_trust =
"project-local"`; they never relabel it as an Agent Mesh built-in.

The entrypoint digest is not a transitive dependency lock: interpreters, dynamic
libraries, imported modules, provider binaries, and operating-system loaders
remain separately trusted. Prefer a self-contained executable or independently
pinned dependencies. A malicious executable can also persist data or deliberately
detach descendants beyond ordinary process-group cleanup; V1 bounds Agent Mesh's
transport but is not a sandbox for project-controlled code.

The selected executable is therefore the privacy and canonical-content trust
boundary. It can read or change project files, echo its input, fabricate probe
observations, and place arbitrary text in successful stdout. Agent Mesh cannot
generically recognize every provider's session identifiers or prove that local
code queried the claimed provider surface. Digest review and normal project-code
review carry those guarantees; `agent-q drivers check` cannot certify them.

## Manifest

A V1 manifest contains exactly:

```toml
schema = "agent-mesh.runtime-driver-manifest.v1"
id = "local:example:claude-cli"
provider = "anthropic"
entrypoint = "driver.py"
entrypoint_sha256 = "<64 lowercase hex characters>"
```

The entrypoint is resolved relative to the manifest directory and invoked
from a private temporary snapshot of the digest-verified bytes with
`shell=false`; a project edit cannot swap the path between verification and
execution. The manifest cannot supply arguments, environment variables, hooks,
imports, or shell fragments.

The adapter's direct `build_launch()` surface fails closed. Only Agent Mesh's
managed `launch()` hook can materialize the verified snapshot and pass it to the
bounded process launcher; library callers cannot obtain a launch spec pointing
at the mutable project source path.

## Probe operation

Agent Mesh invokes the fixed argv `[entrypoint, "probe"]` in the project root.
It writes one bounded JSON request to stdin:

```json
{
  "schema": "agent-mesh.runtime-driver-probe-request.v1",
  "provider_binary": "/resolved/provider/binary",
  "configured_version": "1.2.3",
  "configured_model": "model-slug",
  "authentication_mode": "subscription",
  "billing_mode": "subscription",
  "required_capabilities": ["repository", "tools"],
  "permission_mode": "read-only"
}
```

The driver queries machine-verifiable provider surfaces without issuing a model
prompt and returns one bounded JSON object:

```json
{
  "schema": "agent-mesh.runtime-driver-probe-response.v1",
  "version": "1.2.3",
  "models": ["model-slug"],
  "authentication_mode": "subscription",
  "billing_mode": "subscription",
  "capabilities": ["repository", "tools"],
  "permission_modes": ["read-only"]
}
```

Agent Mesh, not the local driver, compares these observations with the profile.
Missing, duplicate, unknown, malformed, oversized, timed-out, or nonzero
responses fail every mandatory driver proof. Provider-controlled stderr and
exception messages do not become preflight diagnostics.

## Launch operation

After the same preflight passes, Agent Mesh invokes fixed argv
`[entrypoint, "launch"]` and supplies one bounded JSON request on stdin. It
contains the resolved provider binary, validated profile facts, and raw prompt.
The local executable translates that request into one provider invocation and
writes only the provider's response candidate to stdout.

The Agent Mesh host never puts the prompt in argv, environment, manifest, digest
metadata, or host-authored lifecycle fields. It does send the prompt to the
selected executable. Agent Mesh owns credential and provider-session environment
stripping, child-instance binding, the one-shot identity handshake, canonical
dispatch lifecycle, timeout enforcement, bounded stdout/stderr, and stable
failure classification. Agent Mesh discards local-driver stderr and all stdout
on a failed launch. On a successful zero-exit launch, fenced stdout can become a
canonical response when the caller selects `--post-response`; local code can echo
the prompt or include a raw provider reference there. The project must review the
driver not to persist or emit those values. The generic host cannot enforce that
provider-specific content rule.

## Authoring workflow

```bash
agent-mesh drivers scaffold \
  --id local:example:claude-cli \
  --provider anthropic \
  --output tools/agent-mesh-drivers/claude-cli
```

Scaffolding creates a new directory, a fail-closed executable skeleton, and its
manifest. It does not edit `.agent-mesh/config.toml`, enable live dispatch, run
the provider, or overwrite an existing path. The skeleton's probe and launch
operations remain disabled until implemented.

After implementation, update the manifest's entrypoint digest, compute the new
manifest digest, configure the explicit project-local runtime fields, and run:

```bash
agent-q drivers check --target claude
```

That command performs the same no-model-prompt preflight used by live dispatch.
Agent Mesh itself appends no event during the check, but it executes the selected
project-local code, which can mutate project files. A successful check means the
pinned executable reported observations matching the profile; it is not an
independent certification, official support, or a billed-turn test. Normal
`agent-q dispatches once --live ...` remains the explicit managed launch surface.

## Compatibility and repair

Local code remains selected until the profile changes. If a provider update
breaks it, a project agent may patch the local entrypoint and tests under normal
repository review, then deliberately update the two digests. If a later Agent
Mesh release repairs or adds an official driver, it does not replace the local
selection; the project can switch back explicitly after its own preflight.

The protocol version is independent of provider and model versions. A new model
under a compatible provider runtime is catalog data. A breaking Agent Mesh
protocol change requires a new protocol identifier; V1 behavior is never
silently reinterpreted.

## Required gates

The implementation and public conformance surface must prove:

- namespace collision refusal and explicit profile selection;
- manifest and entrypoint digest mismatch refusal;
- absolute path, escape, symlink, non-file, permission, and size refusal;
- unknown protocol/manifest keys and invalid identifiers fail closed;
- malformed, duplicate, oversized, timed-out, and nonzero probe responses fail
  with stable, non-provider-controlled diagnostics;
- API credentials, provider-native session variables, and parent Agent Mesh
  bindings do not reach probe or launch children;
- the host passes prompts only through bounded launch stdin and never places them
  in argv, environment, metadata, or host-authored lifecycle fields;
- failed loading or preflight causes Agent Mesh to append no canonical launch
  event, without claiming that unsandboxed local code cannot mutate the project;
- the generic one-shot handshake records `project-local` trust and the exact
  configured participant/provider/capability boundary;
- one fixture executable completes a real canonical managed-launch vertical;
- a hostile fixture demonstrates that local code can mutate project files and
  that conformance cannot certify local-code privacy or integrity;
- built-in Codex behavior and registry selection remain unchanged.

## Alternatives

The null alternative is official built-ins plus external/manual participation.
It is safer and remains fully supported, but basic managed launch then depends
on the Agent Mesh release cadence.

An in-process Python plugin system is more flexible, but loads project code into
the Agent Mesh process and creates import, override, dependency, and crash
coupling. Arbitrary command templates are smaller but cannot prove semantics and
reintroduce shell and prompt-leak risks. V1 selects a bounded external process
because it contains the extension boundary while preserving an adopter repair
path.
