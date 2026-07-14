"""agent-mesh universal skill rendering.

Phase 5 (Universal agent skill) — canonical skill content lives in this
package and is rendered per target runtime. Installer policy: render to
stdout or an explicit destination by default. No blind writes to global
agent home directories. No `--all` target.
"""
from agent_mesh.skill.render import (
    SUPPORTED_TARGETS,
    UnknownTargetError,
    render_skill,
    skill_source_digest,
    source_commit_digest,
)

__all__ = [
    "SUPPORTED_TARGETS",
    "UnknownTargetError",
    "render_skill",
    "skill_source_digest",
    "source_commit_digest",
]
