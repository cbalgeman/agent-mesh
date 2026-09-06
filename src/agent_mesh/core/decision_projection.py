"""Reusable read-side queries over the deterministic decision projection."""

from __future__ import annotations

import sqlite3
from typing import Any

from agent_mesh.core.decision_schema import decision_revision_digest
from agent_mesh.store.sqlite import json_loads


def decision_revision_sha_from_projection(
    conn: sqlite3.Connection,
    row: sqlite3.Row,
    *,
    include_authored_review_metadata: bool = True,
) -> str:
    """Reconstruct the canonical authored revision digest from one projected row."""

    dec_ulid = str(row["dec_ulid"])
    globs = {
        kind: [
            str(item["pattern"])
            for item in conn.execute(
                "SELECT pattern FROM decision_globs WHERE dec_ulid=? AND kind=? ORDER BY rowid",
                (dec_ulid, kind),
            )
        ]
        for kind in ("affected", "exempt", "generated")
    }
    required_checks = [
        str(item["check_name"])
        for item in conn.execute(
            "SELECT check_name FROM decision_checks WHERE dec_ulid=? ORDER BY rowid",
            (dec_ulid,),
        )
    ]
    verification = [
        {
            "command": str(item["command"]),
            "execution_mode": str(item["execution_mode"]),
            "argv": json_loads(item["argv_json"], None),
            "expected_signal": str(item["expected_signal"]),
            "runtime_cost": item["runtime_cost"],
            "drift_risk": item["drift_risk"],
        }
        for item in conn.execute(
            "SELECT command, execution_mode, argv_json, expected_signal, "
            "runtime_cost, drift_risk FROM decision_verifications "
            "WHERE dec_ulid=? ORDER BY rowid",
            (dec_ulid,),
        )
    ]
    tags = [
        str(item["tag"])
        for item in conn.execute(
            "SELECT tag FROM decision_tags WHERE dec_ulid=? ORDER BY rowid",
            (dec_ulid,),
        )
    ]
    meta = json_loads(row["meta_json"], {})
    if not isinstance(meta, dict):
        meta = {}
    assumptions = [
        {
            "id": str(item["assumption_id"]),
            "text": str(item["text"]),
            "references": json_loads(item["references_json"], []),
        }
        for item in conn.execute(
            "SELECT assumption_id, text, references_json FROM decision_assumptions "
            "WHERE dec_ulid=? ORDER BY rowid",
            (dec_ulid,),
        )
    ]
    evidence: dict[str, list[str]] = {}
    for item in conn.execute(
        "SELECT evidence_kind, ref_value FROM decision_evidence "
        "WHERE dec_ulid=? ORDER BY evidence_kind, ref_value",
        (dec_ulid,),
    ):
        evidence.setdefault(str(item["evidence_kind"]), []).append(str(item["ref_value"]))
    digest_kwargs: dict[str, Any] = {}
    if include_authored_review_metadata and int(row["contract_version"] or 0) >= 5:
        digest_kwargs = {
            "assumptions": assumptions,
            "evidence": evidence,
            "review_policy": meta.get("review_policy", {}),
        }
    return decision_revision_digest(
        decision_id=dec_ulid,
        human_id=str(row["human_id"]),
        title=str(row["title"]),
        tier=str(row["tier"]),
        applicability_scope=str(row["applicability_scope"]),
        owner=str(row["owner"] or ""),
        context=str(meta.get("context") or ""),
        decision=str(meta.get("decision") or ""),
        body_sha=str(row["body_sha"]),
        affected_code_globs=globs["affected"],
        exemptions=globs["exempt"],
        generated_artifact_paths=globs["generated"],
        required_checks=required_checks,
        verification=verification,
        tags=tags,
        **digest_kwargs,
    )
