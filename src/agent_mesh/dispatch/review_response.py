"""Closed, bounded ``review.v1`` outcome envelopes carried by a RES body.

The human-readable RES remains the material outcome.  One machine-readable
envelope inside that same body lets the dispatcher derive assurance facts
without allowing a later operator to restate or reverse the reviewer's verdict.
"""

from __future__ import annotations

import json
from typing import Any

from agent_mesh.core.assurance import REVIEW_BEGIN, REVIEW_END, REVIEW_SCHEMA


def review_response_instructions(
    *,
    policy_id: str,
    policy_digest: str,
    subject_digest: str,
    replaces_response_id: str = "",
) -> str:
    """Return a compact exact envelope contract for the launched reviewer."""

    example: dict[str, Any] = {
        "schema": REVIEW_SCHEMA,
        "policy_id": policy_id,
        "policy_digest": policy_digest,
        "subject_digest": subject_digest,
        "disposition": "GO",
        "finding_counts": {"fatal": 0, "material": 0, "minor": 0},
        "artifact_paths": [],
    }
    if replaces_response_id:
        example["replaces_response_id"] = replaces_response_id
    encoded = json.dumps(example, sort_keys=True, separators=(",", ":"))
    return (
        "Your AGENT_MESH_RESPONSE_BEGIN/END body must contain exactly one review "
        "envelope. Keep your concise verdict outside it; put detail in a permitted "
        "project file and list that repository-relative path in artifact_paths.\n"
        f"{REVIEW_BEGIN}\n{encoded}\n{REVIEW_END}\n"
        "Replace only disposition, finding_counts, and artifact_paths. Do not change "
        "the schema, policy, subject, or replacement commitments."
    )
