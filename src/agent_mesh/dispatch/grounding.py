"""Cold-start grounding assembler for the optional dispatch layer (host-agnostic).

Assembles everything a fresh wave-scoped agent needs to re-ground for one mesh message, and FAILS
LOUDLY (``SubstrateIncomplete``) if a required input is missing instead of letting an under-grounded
run start. It understands the agent-mesh canonical event schema (kinds, thread_id, and the
``body_fidelity`` / ``body_authority`` provenance fields); all project CONTENT -- the agent-standards
text, referenced rows, the wave-doc acceptance section, and git state -- comes through the host's
``GroundingSources`` + ``ProjectStateProvider``.

Privacy tiers:
  * ``telemetry()``  -- sizes + per-section sha256[:16] only, NO previews. The dispatcher records this.
  * ``summary()``    -- adds 120-char previews; for human inspection only, never into telemetry.
  * ``render()``     -- full assembled prompt, LIVE dispatch only.

Stable-prefix-first ordering: the agent-standards, wave-doc/acceptance, and referenced rows are the
cacheable stable prefix; git state, prior decisions, the thread, and the target message are the
volatile tail after the cache breakpoint.
"""
from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field

from .adapters import GroundingSources, ProjectStateProvider

PREVIEW = 120
VERDICT_RE = re.compile(r"\b(APPROVE_WITH_CHANGES|APPROVE|REQUEST_CHANGES|REJECT|NO-GO|GO)\b")


class SubstrateIncomplete(Exception):
    """Raised by build_prompt when a required grounding input is missing (live mode never runs
    under-grounded)."""

    def __init__(self, entity_id: str, missing: list[str]) -> None:
        self.entity_id = entity_id
        self.missing = missing
        super().__init__(f"substrate incomplete for {entity_id}: {', '.join(missing)}")


def _sha(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _recovery_hint(payload: dict) -> str | None:
    """Where an operator could recover a missing canonical body (never read as canonical here)."""
    refs = payload.get("source_context_refs") or []
    if refs:
        r = refs[0]
        return f"{r.get('source_path', '?')}:{r.get('line_start', '?')}-{r.get('line_end', '?')}"
    legacy = payload.get("legacy_source_path")
    if legacy:
        return f"{legacy}:{payload.get('source_line_start', '?')}-{payload.get('source_line_end', '?')}"
    return None


def prior_decisions_text(thread_events: list[dict]) -> str | None:
    out = []
    for event in thread_events:
        if event.get("kind") != "res_posted":
            continue
        payload = event.get("payload", {}) or {}
        match = VERDICT_RE.search(f"{payload.get('summary', '')} {payload.get('body', '')[:200]}")
        if match:
            out.append(f"{event.get('entity_id')} ({payload.get('from', '?')}): {match.group(1)} "
                       f"-- {payload.get('summary', '')[:160]}")
    return "\n".join(out) if out else None


@dataclass
class Section:
    name: str
    role: str   # "stable" | "volatile"
    text: str   # full text; kept internal (summary() exposes a preview; telemetry() does not)

    def hashes(self) -> dict:
        return {"name": self.name, "role": self.role, "chars": len(self.text), "sha256": _sha(self.text)}

    def summary(self) -> dict:
        out = self.hashes()
        out["preview"] = self.text[:PREVIEW].replace("\n", " ").strip()
        return out


@dataclass
class GroundingResult:
    entity_id: str
    complete: bool
    missing: list[str]
    sections: list[Section]
    notes: list[str] = field(default_factory=list)

    @property
    def stable_chars(self) -> int:
        return sum(len(s.text) for s in self.sections if s.role == "stable")

    @property
    def volatile_chars(self) -> int:
        return sum(len(s.text) for s in self.sections if s.role == "volatile")

    @property
    def total_chars(self) -> int:
        return sum(len(s.text) for s in self.sections)

    @property
    def est_tokens(self) -> int:
        return self.total_chars // 4  # rough; a live host can substitute a real tokenizer

    @property
    def stable_prefix_sha(self) -> str:
        return _sha("".join(s.text for s in self.sections if s.role == "stable"))

    def telemetry(self) -> dict:
        """Privacy-safe record for telemetry: sizes + section hashes + SANITIZED completeness only.
        NO previews and NO verbatim 'missing' strings (those can carry refs, feature names, or
        message ids). The full missing list stays on the result object, SubstrateIncomplete, and
        summary() for operator use."""
        return {
            "complete": self.complete,
            "missing_count": len(self.missing),
            "missing_digest": _sha("\n".join(sorted(self.missing))) if self.missing else "",
            "est_tokens": self.est_tokens,
            "stable_chars": self.stable_chars,
            "volatile_chars": self.volatile_chars,
            "sections": len(self.sections),
            "stable_prefix_sha": self.stable_prefix_sha,
            "section_hashes": [s.hashes() for s in self.sections],
        }

    def summary(self) -> dict:
        """Human inspection only: adds the full missing list, notes, and 120-char previews. Not for
        telemetry."""
        out = self.telemetry()
        out["missing"] = self.missing       # full operator detail; deliberately absent from telemetry()
        out["notes"] = self.notes
        out["sections"] = [s.summary() for s in self.sections]
        return out

    def render(self) -> str:
        """Full assembled prompt (LIVE only): stable prefix, cache breakpoint, volatile tail."""
        parts, broke = [], False
        for section in self.sections:
            if section.role == "volatile" and not broke:
                parts.append("<<CACHE_BREAKPOINT (cache_control on the stable prefix above)>>")
                broke = True
            parts.append(f"# === {section.name} ===\n{section.text}")
        return "\n\n".join(parts)


def assemble_grounding(
    entity_id: str,
    sources: GroundingSources,
    project: ProjectStateProvider,
    *,
    events: list[dict] | None = None,
    require_acceptance: bool = False,
) -> GroundingResult:
    """Assemble the grounding for one message. Pulls canonical events + project content through the
    injected host adapters and FAILS CLOSED: any missing required input lands in ``missing`` and
    marks the result incomplete (build_prompt raises on that)."""
    if events is None:
        events = sources.events()
    by_id = {e.get("entity_id"): e for e in events if e.get("kind") in ("req_created", "res_posted")}
    event = by_id.get(entity_id)
    missing: list[str] = []
    notes: list[str] = []
    sections: list[Section] = []

    # (2) agent standards -- REQUIRED stable
    agents = sources.agents_md_text()
    if agents:
        sections.append(Section("agent-standards", "stable", agents))
    else:
        missing.append("agent-standards (AGENTS.md) missing")

    if event is None:
        missing.append(f"message {entity_id} not found in canonical events")
        return GroundingResult(entity_id, False, missing, sections, notes)

    payload = event.get("payload", {}) or {}
    thread_id = event.get("thread_id") or entity_id
    thread_events = [e for e in events if e.get("thread_id") == thread_id]
    feature = (payload.get("feature") or "").strip()
    refs = list(payload.get("refs", []) or [])

    # (3a) wave doc / acceptance -- recorded, REQUIRED when require_acceptance + a real feature
    wave_doc = sources.wave_doc_text(feature)
    if wave_doc:
        sections.append(Section("wave-doc/acceptance", "stable", wave_doc))
    elif require_acceptance and feature and feature.lower() not in ("", "(none)", "none"):
        missing.append(f"acceptance source required for a planned/risky apply on feature {feature}")
    else:
        notes.append("no wave doc / acceptance section"
                     + (f" for feature {feature}" if feature else " (no feature)"))

    # (3b) referenced rows -- conditionally REQUIRED stable (host-supplied snapshot)
    rows_text, unresolved = sources.backlog_rows_text(refs)
    if rows_text:
        sections.append(Section("referenced-rows", "stable", rows_text))
    if unresolved:
        missing.append("unresolved refs: " + ",".join(unresolved))
    if not refs:
        notes.append("no refs on message")

    # (4) git state -- REQUIRED volatile
    git_state = project.git_state_text()
    if git_state:
        sections.append(Section("git-state", "volatile", git_state))
    else:
        missing.append("git state unavailable")

    # (3c) prior decisions in the thread -- recorded volatile
    prior = prior_decisions_text(thread_events)
    if prior:
        sections.append(Section("prior-decisions", "volatile", prior))

    # (1) full thread -- REQUIRED: every thread event must carry a full-fidelity body, else the
    # decision it holds is omitted and the grounding is incomplete.
    thread_lines, thread_bad = [], []
    for e in sorted(thread_events, key=lambda x: int(x.get("event_seq", 0))):
        ep = e.get("payload", {}) or {}
        fidelity = ep.get("body_fidelity", "?")
        body = ep.get("body", "") or ""
        if fidelity != "full" or not body.strip():
            thread_bad.append((e.get("entity_id"), fidelity, _recovery_hint(ep)))
            body = f"[{fidelity}; body not in canonical event]"
        thread_lines.append(
            f"-- {e.get('kind')} {e.get('entity_id')} from={ep.get('from', '?')} "
            f"fidelity={fidelity} authority={ep.get('body_authority', '?')} "
            f"ctx={ep.get('source_context_status', '?')} --\n{body[:1500]}"
        )
    sections.append(Section("thread", "volatile", "\n\n".join(thread_lines)))
    if thread_bad:
        missing.append("thread events missing full body: " + ",".join(str(b[0]) for b in thread_bad))
        for eid, fidelity, hint in thread_bad:
            notes.append(f"recover {eid} (fidelity={fidelity})" + (f" from {hint}" if hint else ""))

    # (1) the TARGET message body specifically must be full-fidelity + non-empty
    tgt_fidelity = payload.get("body_fidelity", "?")
    tgt_body = payload.get("body", "") or ""
    if tgt_fidelity != "full" or not tgt_body.strip():
        missing.append(f"target message body unavailable (fidelity={tgt_fidelity})")
        hint = _recovery_hint(payload)
        if hint:
            notes.append(f"recover target body from {hint} (validate hash, reimport as full)")
    else:
        sections.append(Section(
            "target-message", "volatile",
            f"id={entity_id} to={payload.get('to')} feature={feature or '(none)'} "
            f"authority={payload.get('body_authority', '?')} "
            f"ctx={payload.get('source_context_status', '?')} title={payload.get('title', '')}\n\n{tgt_body}",
        ))

    return GroundingResult(entity_id, len(missing) == 0, missing, sections, notes)


def build_prompt(
    entity_id: str,
    sources: GroundingSources,
    project: ProjectStateProvider,
    *,
    events: list[dict] | None = None,
    require_acceptance: bool = False,
) -> str:
    """LIVE-mode entry: assemble and RAISE ``SubstrateIncomplete`` if the substrate is incomplete."""
    result = assemble_grounding(entity_id, sources, project, events=events,
                                require_acceptance=require_acceptance)
    if not result.complete:
        raise SubstrateIncomplete(entity_id, result.missing)
    return result.render()
