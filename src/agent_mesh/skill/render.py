"""Canonical agent-mesh skill content + per-target rendering.

Design contract (RES-2-phase5-impl.md §3, §2):
- Single canonical SKILL.md body owned by this package.
- Per-target adapters wrap the canonical body with target-specific
  frontmatter / preamble. They never edit the body.
- Rendered output is provenance-stamped: package version + content
  digest + best-effort source commit. The digest covers the canonical
  body only, so the same body across targets produces the same digest.
- Destination I/O lives in callers (CLI / install). Git commit lookup is
  best-effort and degrades to `unknown` outside a git checkout.
"""
from __future__ import annotations

import hashlib
import subprocess
from dataclasses import dataclass
from pathlib import Path

from agent_mesh import __version__ as PACKAGE_VERSION

# ---------------------------------------------------------------------------
# Canonical skill body (RES-2 §3). Edit this to change the skill semantics.
# ---------------------------------------------------------------------------

CANONICAL_SKILL_BODY = """\
# agent-mesh

Use when the current repo contains `.agent-mesh/` or when the user asks to
use the agent-mesh handoff substrate.

## Detect
- Walk upward from cwd for `.agent-mesh/`.
- If absent, do not invent a project. Use a legacy handoff tool only when the
  target repository explicitly documents it.

## Read Before Write
- Run `agent-q status` and targeted `agent-q list/locate/body` before
  responding.
- Read `.agent-mesh/config.toml` to learn participants, routing defaults,
  response_mode, and compatibility view paths before writing.
- For code changes, run decision/quality preflight once those event domains
  are available.

## Write
- Use `agent-mesh request/respond/resolve/reopen`.
- Do not hand-edit generated views or `events.jsonl`.
- Respect `response_mode = single|multi`.

## Compatibility
- Legacy views are projections, not source of truth, once `.agent-mesh/`
  exists.

## Quality Discipline
- For T1/T2 decisions, consult the project quality bar when available.
- For bugs, prefer root-cause investigation before closure.
- Until quality/investigation events exist, treat this as advisory
  procedure. Future event names (do not invent today):
  `quality_bar_declared`, `quality_bar_updated`, `quality_gate_evaluated`,
  `investigation_opened`, `investigation_artifact_added`,
  `investigation_finding_recorded`, `investigation_closed`.

## Feedback Triage
- Treat feedback REQs as human-authored observations. Preserve the raw
  observation text; do not overwrite it with structured guesses.
- Read the full packet/thread first: `agent-q packet --id <REQ-id>` or
  `agent-q body <REQ-id>` plus relevant linked refs.
- Parse durable findings into explicit dispositions:
  current_wave, backlog, future, duplicate, known_issue, needs_investigation,
  or no_action.
- Create or update backlog items only for durable findings. Link each item
  back to the feedback REQ/RES or source ref, and avoid duplicating existing
  BKL items.
- In the reply, include a concise human-readable summary plus a structured
  triage block listing each finding, disposition, priority/severity when known,
  and created/updated backlog IDs.
- Resolve/close the feedback REQ only when the triage is complete or the
  project owner explicitly marks it closed. Reopen if follow-up work or
  corrected triage is needed.

## Stop Lines
- Unknown participant, duplicate response to single-mode request,
  hash-chain failure, recovery stop-line, or missing request — stop and
  report, do not proceed.

## Command Card

Read side (safe, no writes):
  agent-q status
  agent-q list [--from X] [--to Y] [--status open|resolved]
  agent-q locate <message_id>
  agent-q body <message_id>

Write side (append-only events):
  agent-mesh request --to <agent> "<title>" "<body>"
  agent-mesh respond <REQ-id> "<summary>" "<details>"
  agent-mesh resolve <REQ-id> "<reason>"
  agent-mesh reopen <REQ-id> "<reason>"

References (read these for full contract, do not paraphrase from memory):
  README.md
  docs/configuration.md
"""


# ---------------------------------------------------------------------------
# Targets
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Target:
    name: str
    description: str
    # Optional preamble inserted above the canonical body. Empty string
    # means "render canonical body verbatim".
    preamble: str = ""
    # YAML-frontmatter consumers require frontmatter at byte 0; for those
    # targets, place the provenance comment after the preamble.
    provenance_after_preamble: bool = False


_GENERIC = Target(
    name="generic",
    description="Plain markdown SKILL.md, no runtime-specific frontmatter.",
)

_CLAUDE = Target(
    name="claude",
    description="Claude Code skill format (YAML frontmatter + markdown).",
    preamble=(
        "---\n"
        "name: agent-mesh\n"
        "description: Use the agent-mesh handoff substrate when `.agent-mesh/` exists in the repo.\n"
        "---\n\n"
    ),
    provenance_after_preamble=True,
)

_CODEX = Target(
    name="codex",
    description="Codex CLI skill (markdown with codex header comment).",
    preamble=(
        "<!-- codex-skill: agent-mesh -->\n"
        "<!-- Trigger: repo contains `.agent-mesh/` or user asks for the agent-mesh substrate. -->\n\n"
    ),
)

_HERMES = Target(
    name="hermes",
    description="Hermes Agent skill (YAML frontmatter compatible with skill_view).",
    preamble=(
        "---\n"
        "name: agent-mesh\n"
        "description: Multi-agent durable handoff substrate. Triggered by `.agent-mesh/` in repo.\n"
        "category: autonomous-ai-agents\n"
        "---\n\n"
    ),
    provenance_after_preamble=True,
)


SUPPORTED_TARGETS: dict[str, Target] = {
    t.name: t for t in (_GENERIC, _CLAUDE, _CODEX, _HERMES)
}


class UnknownTargetError(ValueError):
    """Raised when an unsupported skill target is requested."""


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def skill_source_digest() -> str:
    """SHA-256 digest (12 hex chars) of the canonical skill body.

    The digest is over the canonical body only; preambles do not affect
    it. This makes it safe to verify "which skill version is installed"
    independent of target.
    """
    full = hashlib.sha256(CANONICAL_SKILL_BODY.encode("utf-8")).hexdigest()
    return full[:12]


def source_commit_digest() -> str:
    """Best-effort short git commit for the package source tree."""
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / ".git").exists():
            try:
                return subprocess.check_output(
                    ["git", "-C", str(parent), "rev-parse", "--short=12", "HEAD"],
                    text=True,
                    stderr=subprocess.DEVNULL,
                ).strip()
            except (OSError, subprocess.CalledProcessError):
                return "unknown"
    return "unknown"


def _provenance_block(target: Target) -> str:
    return (
        f"<!-- agent-mesh skill | target={target.name} | "
        f"package=agent-mesh@{PACKAGE_VERSION} | "
        f"source-digest={skill_source_digest()} | "
        f"source-commit={source_commit_digest()} -->\n\n"
    )


def render_skill(target_name: str) -> str:
    """Render the agent-mesh skill for the given target.

    Output shape:

        <provenance comment>
        <target preamble (optional)>
        <canonical body>

    Raises UnknownTargetError for unknown targets.
    """
    target = SUPPORTED_TARGETS.get(target_name)
    if target is None:
        known = ", ".join(sorted(SUPPORTED_TARGETS))
        raise UnknownTargetError(
            f"unknown skill target {target_name!r}; known targets: {known}"
        )
    provenance = _provenance_block(target)
    if target.provenance_after_preamble:
        return target.preamble + provenance + CANONICAL_SKILL_BODY
    return provenance + target.preamble + CANONICAL_SKILL_BODY
