"""Reviewed migrations for narrowly identified legacy decision events.

This module is deliberately not part of normal append or replay.  It exists for
operator-authorized recovery when stricter replay validation identifies an
already-canonical legacy event and no valid log backup is available.
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from agent_mesh.config import AgentMeshConfig
from agent_mesh.core.chain import verify_chain_bytes
from agent_mesh.core.decision_projection import decision_revision_sha_from_projection
from agent_mesh.core.decision_schema import DecisionBodyIntegrityError, read_verified_decision_body
from agent_mesh.core.hashing import canonical_json
from agent_mesh.core.lock import acquire
from agent_mesh.core.recovery import recover
from agent_mesh.store.rebuild import (
    DECISION_METADATA_INVALID,
    DecisionStopLine,
    apply_record,
    rebuild_all,
)
from agent_mesh.store.sqlite import initialize_schema, resolve_decision
from agent_mesh.views import render_all


class DecisionRecoveryError(RuntimeError):
    """Raised before mutation when a reviewed migration precondition fails."""


@dataclass(frozen=True)
class DecisionAcceptanceMigrationResult:
    """Evidence returned by a legacy decision-acceptance migration."""

    event_id: str
    event_seq: int
    decision_id: str
    approved_revision_sha: str
    before_sha256: str
    after_sha256: str
    applied: bool
    backup_path: Path | None = None
    receipt_path: Path | None = None


@dataclass(frozen=True)
class _PreparedMigration:
    result: DecisionAcceptanceMigrationResult
    original: bytes
    candidate: bytes


def migrate_legacy_decision_acceptance(
    config: AgentMeshConfig,
    *,
    event_id: str,
    expected_log_sha256: str,
    expected_approved_revision_sha: str,
    authorization_note: str = "",
    apply: bool = False,
) -> DecisionAcceptanceMigrationResult:
    """Upgrade one malformed final ``decision_accepted`` event to v5.

    Dry-run is the default.  Applying requires a non-empty authorization note,
    acquires the canonical lock, retains the exact original log, atomically
    installs an in-memory-validated candidate, rebuilds projections, and writes
    a recovery receipt outside canonical history.
    """

    if apply and not authorization_note.strip():
        raise DecisionRecoveryError("applying a migration requires an authorization note")

    lock_handle = acquire(config.agent_dir / ".mail-lock")
    try:
        recover(config.events_path, config.agent_dir)
        current_sha = hashlib.sha256(config.events_path.read_bytes()).hexdigest()
        if current_sha != expected_log_sha256:
            if not apply:
                raise DecisionRecoveryError(
                    f"events log SHA-256 changed: expected {expected_log_sha256}, "
                    f"got {current_sha}"
                )
            return _resume_applied_migration(
                config,
                event_id=event_id,
                expected_log_sha256=expected_log_sha256,
                expected_approved_revision_sha=expected_approved_revision_sha,
                authorization_note=authorization_note,
            )
        prepared = _prepare_migration(
            config,
            event_id=event_id,
            expected_log_sha256=expected_log_sha256,
            expected_approved_revision_sha=expected_approved_revision_sha,
        )
        if not apply:
            return prepared.result

        recovery_dir = config.agent_dir / "recovery"
        recovery_dir.mkdir(parents=True, exist_ok=True)
        backup_path = recovery_dir / (
            f"events-before-{event_id}-{prepared.result.before_sha256[:12]}.jsonl"
        )
        intent_path = recovery_dir / (
            f"decision-acceptance-{event_id}-{prepared.result.before_sha256[:12]}.intent.json"
        )
        receipt_path = recovery_dir / (
            f"decision-acceptance-{event_id}-{prepared.result.after_sha256[:12]}.json"
        )
        _write_exclusive_or_verify(backup_path, prepared.original)
        migration_evidence = _migration_evidence(
            prepared.result,
            authorization_note=authorization_note,
            backup_path=backup_path,
            config=config,
        )
        _write_exclusive_or_verify(
            intent_path,
            canonical_json({**migration_evidence, "state": "prepared"}) + b"\n",
        )
        _atomic_replace(config.events_path, prepared.candidate)
        _complete_applied_migration(
            config,
            result=prepared.result,
            evidence=migration_evidence,
            receipt_path=receipt_path,
        )
        return DecisionAcceptanceMigrationResult(
            **{
                **prepared.result.__dict__,
                "applied": True,
                "backup_path": backup_path,
                "receipt_path": receipt_path,
            }
        )
    finally:
        lock_handle.release()


def _resume_applied_migration(
    config: AgentMeshConfig,
    *,
    event_id: str,
    expected_log_sha256: str,
    expected_approved_revision_sha: str,
    authorization_note: str,
) -> DecisionAcceptanceMigrationResult:
    recovery_dir = config.agent_dir / "recovery"
    intent_path = recovery_dir / (
        f"decision-acceptance-{event_id}-{expected_log_sha256[:12]}.intent.json"
    )
    if not intent_path.exists():
        current_sha = hashlib.sha256(config.events_path.read_bytes()).hexdigest()
        raise DecisionRecoveryError(
            f"events log SHA-256 changed without a matching migration intent: "
            f"expected {expected_log_sha256}, got {current_sha}"
        )
    try:
        evidence = json.loads(intent_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DecisionRecoveryError(f"cannot read migration intent: {intent_path}") from exc
    if not isinstance(evidence, dict) or evidence.get("state") != "prepared":
        raise DecisionRecoveryError("migration intent is not a prepared recovery record")
    if evidence.get("authorization_note") != authorization_note.strip():
        raise DecisionRecoveryError("authorization note does not match the prepared migration")
    if evidence.get("event_id") != event_id:
        raise DecisionRecoveryError("migration intent event ID does not match")
    if evidence.get("before_sha256") != expected_log_sha256:
        raise DecisionRecoveryError("migration intent before SHA-256 does not match")
    if evidence.get("approved_revision_sha") != expected_approved_revision_sha:
        raise DecisionRecoveryError("migration intent approved revision SHA-256 does not match")

    backup_path = recovery_dir / (
        f"events-before-{event_id}-{expected_log_sha256[:12]}.jsonl"
    )
    if evidence.get("backup_path") != str(backup_path.relative_to(config.agent_dir)):
        raise DecisionRecoveryError("migration intent backup path does not match")
    if not backup_path.exists() or hashlib.sha256(backup_path.read_bytes()).hexdigest() != expected_log_sha256:
        raise DecisionRecoveryError("migration backup is absent or does not match the original log")

    result = DecisionAcceptanceMigrationResult(
        event_id=event_id,
        event_seq=int(evidence["event_seq"]),
        decision_id=str(evidence["decision_id"]),
        approved_revision_sha=str(evidence["approved_revision_sha"]),
        before_sha256=expected_log_sha256,
        after_sha256=str(evidence["after_sha256"]),
        applied=False,
    )
    receipt_path = recovery_dir / (
        f"decision-acceptance-{event_id}-{result.after_sha256[:12]}.json"
    )
    _complete_applied_migration(
        config,
        result=result,
        evidence={key: value for key, value in evidence.items() if key != "state"},
        receipt_path=receipt_path,
    )
    return DecisionAcceptanceMigrationResult(
        **{
            **result.__dict__,
            "applied": True,
            "backup_path": backup_path,
            "receipt_path": receipt_path,
        }
    )


def _complete_applied_migration(
    config: AgentMeshConfig,
    *,
    result: DecisionAcceptanceMigrationResult,
    evidence: dict[str, Any],
    receipt_path: Path,
) -> None:
    current = config.events_path.read_bytes()
    current_sha = hashlib.sha256(current).hexdigest()
    if current_sha != result.after_sha256:
        raise DecisionRecoveryError(
            f"applied migration SHA-256 mismatch: expected {result.after_sha256}, got {current_sha}"
        )
    chain = verify_chain_bytes(current)
    if not chain.ok:
        raise DecisionRecoveryError(f"applied migration chain is invalid: {chain.error}")
    try:
        records = [json.loads(line) for line in current.splitlines(keepends=True)]
    except json.JSONDecodeError as exc:
        raise DecisionRecoveryError(f"applied migration contains invalid JSON: {exc}") from exc
    if not records or records[-1].get("event_id") != result.event_id:
        raise DecisionRecoveryError("applied migration event is not the canonical tail")
    verification_conn = _replay_prefix(config, records)
    verification_conn.close()

    rebuild_all(config)
    render_all(config)
    _write_exclusive_or_verify(
        receipt_path,
        canonical_json({**evidence, "state": "completed"}) + b"\n",
    )


def _migration_evidence(
    result: DecisionAcceptanceMigrationResult,
    *,
    authorization_note: str,
    backup_path: Path,
    config: AgentMeshConfig,
) -> dict[str, Any]:
    return {
        "authorization_note": authorization_note.strip(),
        "event_id": result.event_id,
        "event_seq": result.event_seq,
        "decision_id": result.decision_id,
        "migration": "legacy-decision-acceptance-to-v5",
        "preserved_human_approval": True,
        "approved_revision_sha": result.approved_revision_sha,
        "before_sha256": result.before_sha256,
        "after_sha256": result.after_sha256,
        "backup_path": str(backup_path.relative_to(config.agent_dir)),
    }


def _prepare_migration(
    config: AgentMeshConfig,
    *,
    event_id: str,
    expected_log_sha256: str,
    expected_approved_revision_sha: str,
) -> _PreparedMigration:
    original = config.events_path.read_bytes()
    before_sha = hashlib.sha256(original).hexdigest()
    if before_sha != expected_log_sha256:
        raise DecisionRecoveryError(
            f"events log SHA-256 changed: expected {expected_log_sha256}, got {before_sha}"
        )
    chain = verify_chain_bytes(original)
    if not chain.ok:
        raise DecisionRecoveryError(f"canonical chain is invalid: {chain.error}")
    if not original or not original.endswith(b"\n"):
        raise DecisionRecoveryError("events log must be non-empty and newline-terminated")

    lines = original.splitlines(keepends=True)
    try:
        records = [json.loads(line) for line in lines]
    except json.JSONDecodeError as exc:
        raise DecisionRecoveryError(f"events log contains invalid JSON: {exc}") from exc
    if not all(isinstance(record, dict) for record in records):
        raise DecisionRecoveryError("every canonical event must be a JSON object")

    target = records[-1]
    if lines[-1] != canonical_json(target) + b"\n":
        raise DecisionRecoveryError("authorized tail is not already canonical JSON")
    if str(target.get("event_id") or "") != event_id:
        raise DecisionRecoveryError("the authorized event is not the canonical tail")
    if target.get("kind") != "decision_accepted":
        raise DecisionRecoveryError("the authorized tail is not decision_accepted")

    payload = target.get("payload")
    if not isinstance(payload, dict):
        raise DecisionRecoveryError("decision acceptance payload must be an object")
    expected_legacy_fields = {
        "accepted_by",
        "approval_source",
        "approved_body_sha",
        "approved_utc",
        "decision_id",
        "notes",
    }
    if set(payload) != expected_legacy_fields:
        raise DecisionRecoveryError(
            "legacy acceptance payload has fields outside the reviewed migration shape"
        )
    if payload.get("accepted_by") != target.get("actor"):
        raise DecisionRecoveryError("accepted_by does not match the preserved human actor")
    if payload.get("approval_source") not in {"workbench", "interactive_cli"}:
        raise DecisionRecoveryError("approval_source is not a human approval surface")
    if payload.get("approved_utc") != target.get("occurred_utc"):
        raise DecisionRecoveryError("approved_utc does not match the event timestamp")
    if not str(payload.get("notes") or "").strip():
        raise DecisionRecoveryError("approval note is empty")

    conn = _replay_prefix(config, records[:-1])
    try:
        try:
            apply_record(target, config, conn=conn, require_next=True)
        except DecisionStopLine as exc:
            if (
                exc.code != DECISION_METADATA_INVALID
                or exc.detail
                != "decision_contract_version=5 is required to accept this decision revision"
            ):
                raise DecisionRecoveryError(
                    f"tail fails for an unreviewed reason: {exc.code}: {exc.detail}"
                ) from exc
        else:
            raise DecisionRecoveryError("tail does not reproduce the reviewed replay stop line")

        decision_id = str(payload.get("decision_id") or "")
        dec_ulid = resolve_decision(conn, decision_id)
        if dec_ulid is None:
            raise DecisionRecoveryError("accepted decision is absent from the valid prefix")
        if (
            target.get("entity_id") != dec_ulid
            or target.get("thread_id") != dec_ulid
            or decision_id != dec_ulid
        ):
            raise DecisionRecoveryError("decision identities do not agree")
        row = conn.execute(
            "SELECT dec_ulid, human_id, title, status, tier, contract_version, "
            "applicability_scope, owner, body_sha, body_path, body_bytes, meta_json "
            "FROM decisions WHERE dec_ulid=?",
            (dec_ulid,),
        ).fetchone()
        if row is None or str(row["status"]) != "proposed":
            raise DecisionRecoveryError("accepted decision is not Proposed in the valid prefix")
        if int(row["contract_version"] or 0) < 5:
            raise DecisionRecoveryError("accepted decision revision is not a v5 decision")
        if payload.get("approved_body_sha") != row["body_sha"]:
            raise DecisionRecoveryError("approved body SHA does not match the valid prefix")
        try:
            read_verified_decision_body(
                config.agent_dir,
                body_path=str(row["body_path"] or ""),
                body_sha=str(row["body_sha"]),
                body_bytes=int(row["body_bytes"] or 0),
            )
        except DecisionBodyIntegrityError as exc:
            raise DecisionRecoveryError(f"approved decision body is invalid: {exc}") from exc
        revision_sha = decision_revision_sha_from_projection(conn, row)
        if revision_sha != expected_approved_revision_sha:
            raise DecisionRecoveryError(
                "derived approved revision SHA-256 does not match the reviewed value"
            )
    finally:
        conn.close()

    migrated = json.loads(json.dumps(target))
    migrated["payload"]["decision_contract_version"] = 5
    migrated["payload"]["approved_revision_sha"] = revision_sha
    candidate_line = canonical_json(migrated) + b"\n"
    candidate = b"".join(lines[:-1]) + candidate_line
    candidate_chain = verify_chain_bytes(candidate)
    if not candidate_chain.ok:
        raise DecisionRecoveryError(f"migrated candidate chain is invalid: {candidate_chain.error}")

    verification_conn = _replay_prefix(config, [*records[:-1], migrated])
    verification_conn.close()
    after_sha = hashlib.sha256(candidate).hexdigest()
    result = DecisionAcceptanceMigrationResult(
        event_id=event_id,
        event_seq=int(target["event_seq"]),
        decision_id=str(target["entity_id"]),
        approved_revision_sha=revision_sha,
        before_sha256=before_sha,
        after_sha256=after_sha,
        applied=False,
    )
    return _PreparedMigration(result=result, original=original, candidate=candidate)


def _replay_prefix(config: AgentMeshConfig, records: list[dict[str, Any]]) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    try:
        initialize_schema(conn)
        for record in records:
            apply_record(record, config, conn=conn, require_next=True)
    except Exception:
        conn.close()
        raise
    return conn


def _write_exclusive_or_verify(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        if path.read_bytes() != data:
            raise DecisionRecoveryError(f"existing recovery artifact differs: {path}")
        return
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
    finally:
        _fsync_dir(path.parent)


def _atomic_replace(path: Path, data: bytes) -> None:
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}-migration-", dir=path.parent)
    temporary_path = Path(temporary_name)
    try:
        os.fchmod(fd, path.stat().st_mode & 0o777)
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
        _fsync_dir(path.parent)
    finally:
        if temporary_path.exists():
            temporary_path.unlink()


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
