"""Deterministic projection from events.jsonl into SQLite."""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

from agent_mesh.config import (
    AgentMeshConfig,
    config_from_agent_dir,
    ensure_project_dirs,
    load_config,
)
from agent_mesh.core.agent_instances import (
    INSTANCE_ADAPTER_TRUST_SOURCES,
    INSTANCE_CONTRACT_VERSION,
    INSTANCE_EVENT_KINDS,
    INSTANCE_ID_RE,
    INSTANCE_LIFECYCLE_DISPOSITIONS,
    INSTANCE_REGISTRATION_ORIGINS,
    INSTANCE_SESSION_IDENTITY_MODES,
    INSTANCE_TERMINAL_OBSERVATION_MODES,
    INSTANCE_TERMINAL_OUTCOMES,
    MUTABLE_INSTANCE_FIELDS,
    AgentInstanceError,
    normalize_instance_label,
    normalize_new_instance_handle,
    validate_actor_shadow,
    validate_external_session_ref_digest,
)
from agent_mesh.core.decision_schema import (
    DECISION_IN_FORCE_TIERS,
    DECISION_BODY_FORMAT_GENERATED_V1,
    DECISION_BODY_FORMAT_UNKNOWN,
    DecisionBodyIntegrityError,
    applicability_scope_for,
    decision_applicability_issues,
    decision_current_revision_events,
    decision_body_format_for_proposal,
    enforcement_for_tier,
    is_valid_decision_tier,
    normalize_decision_assumptions,
    normalize_decision_evidence,
    normalize_decision_review_policy,
    normalize_decision_verification,
    parse_generated_decision_body,
    read_verified_decision_body,
)
from agent_mesh.core.decision_projection import decision_revision_sha_from_projection
from agent_mesh.core.dispatch_schema import (
    DISPATCH_EVENT_KINDS,
    DispatchSchemaError,
    validate_dispatch_payload,
)
from agent_mesh.core.events import Event, utc_now
from agent_mesh.core.hashing import canonical_json, hash_event_line
from agent_mesh.core.human_authority import (
    has_persisted_direct_human_authority,
    is_direct_human_control_event,
)
from agent_mesh.core.provenance import (
    body_authority_for_payload,
    body_fidelity_for_payload,
    confidence_value,
    validate_event_provenance,
)
from agent_mesh.core.workflow_origin import (
    ProjectedWorkflowOrigin,
    project_workflow_origin,
)
from agent_mesh.store.sqlite import (
    ALL_TABLES,
    connect,
    get_meta,
    get_last_event_seq,
    initialize_schema,
    json_dumps,
    json_loads,
    reset_schema,
    resolve_decision,
    set_last_event_seq,
    set_meta,
)

# Bump this whenever replay semantics change in a way that requires existing
# SQLite projections to be regenerated. Event-log equality alone cannot detect
# a package upgrade that changes how historical events are interpreted.
PROJECTION_VERSION = "16"

DECISION_PARENT_MISSING = "DECISION_PARENT_MISSING"
DECISION_SUPERSEDE_TARGET_INVALID = "DECISION_SUPERSEDE_TARGET_INVALID"
DECISION_SUPERSEDE_CYCLE = "DECISION_SUPERSEDE_CYCLE"
DECISION_HUMAN_ID_COLLISION = "DECISION_HUMAN_ID_COLLISION"
DECISION_ALIAS_FORK = "DECISION_ALIAS_FORK"
DECISION_IDENTITY_MISMATCH = "DECISION_IDENTITY_MISMATCH"
DECISION_TRANSITION_INVALID = "DECISION_TRANSITION_INVALID"
DECISION_METADATA_INVALID = "DECISION_METADATA_INVALID"
DECISION_BODY_PROJECTION_DIVERGENCE = "DECISION_BODY_PROJECTION_DIVERGENCE"

BACKLOG_ID_COLLISION = "BACKLOG_ID_COLLISION"
BACKLOG_ITEM_MISSING = "BACKLOG_ITEM_MISSING"

DECISION_STOP_LINE_CODES = {
    DECISION_PARENT_MISSING,
    DECISION_SUPERSEDE_TARGET_INVALID,
    DECISION_SUPERSEDE_CYCLE,
    DECISION_HUMAN_ID_COLLISION,
    DECISION_ALIAS_FORK,
    DECISION_IDENTITY_MISMATCH,
    DECISION_TRANSITION_INVALID,
    DECISION_METADATA_INVALID,
}

DECISION_TERMINAL_STATUSES = frozenset({"superseded", "retired", "rejected"})
DECISION_MUTATING_EVENT_KINDS = frozenset(
    {
        "decision_accepted",
        "decision_superseded",
        "decision_retired",
        "decision_rejected",
        "decision_metadata_updated",
    }
)

DISPATCH_BODY_LEAK = "DISPATCH_BODY_LEAK"
DISPATCH_LEASE_DUPLICATE = "DISPATCH_LEASE_DUPLICATE"
DISPATCH_LEASE_UNKNOWN_RUN = "DISPATCH_LEASE_UNKNOWN_RUN"
DISPATCH_LEASE_RELEASE_INVALID = "DISPATCH_LEASE_RELEASE_INVALID"
DISPATCH_RUN_UNKNOWN_MESSAGE = "DISPATCH_RUN_UNKNOWN_MESSAGE"
DISPATCH_RUN_WITHOUT_LEASE = "DISPATCH_RUN_WITHOUT_LEASE"
DISPATCH_RUN_TERMINAL_WITHOUT_STARTED_OR_LEASE = "DISPATCH_RUN_TERMINAL_WITHOUT_STARTED_OR_LEASE"
DISPATCH_SUPERSEDE_INVALID = "DISPATCH_SUPERSEDE_INVALID"
DISPATCH_OUTPUT_MESSAGE_INVALID = "DISPATCH_OUTPUT_MESSAGE_INVALID"
DISPATCH_OUTPUT_THREAD_MISMATCH = "DISPATCH_OUTPUT_THREAD_MISMATCH"
DISPATCH_OUTPUT_INSTANCE_MISMATCH = "DISPATCH_OUTPUT_INSTANCE_MISMATCH"
DISPATCH_OUTPUT_ORDER_INVALID = "DISPATCH_OUTPUT_ORDER_INVALID"
DISPATCH_RESPONSE_RUN_INVALID = "DISPATCH_RESPONSE_RUN_INVALID"
DISPATCH_RESPONSE_INSTANCE_MISMATCH = "DISPATCH_RESPONSE_INSTANCE_MISMATCH"
DISPATCH_RESPONSE_ORDER_INVALID = "DISPATCH_RESPONSE_ORDER_INVALID"

DISPATCH_STOP_LINE_CODES = {
    DISPATCH_BODY_LEAK,
    DISPATCH_LEASE_DUPLICATE,
    DISPATCH_LEASE_UNKNOWN_RUN,
    DISPATCH_LEASE_RELEASE_INVALID,
    DISPATCH_RUN_UNKNOWN_MESSAGE,
    DISPATCH_RUN_WITHOUT_LEASE,
    DISPATCH_RUN_TERMINAL_WITHOUT_STARTED_OR_LEASE,
    DISPATCH_SUPERSEDE_INVALID,
    DISPATCH_OUTPUT_MESSAGE_INVALID,
    DISPATCH_OUTPUT_THREAD_MISMATCH,
    DISPATCH_OUTPUT_INSTANCE_MISMATCH,
    DISPATCH_OUTPUT_ORDER_INVALID,
    DISPATCH_RESPONSE_RUN_INVALID,
    DISPATCH_RESPONSE_INSTANCE_MISMATCH,
    DISPATCH_RESPONSE_ORDER_INVALID,
}

# Hierarchical decisions use numbered S/B slices (D038-S1, D067-B2).
# Existing projects also use single-letter variant gates (D076-B, D076-E).
DECISION_SUFFIX_PATTERN = r"(?:[SB]\d+|[A-Z])"
DECISION_ID_RE = re.compile(rf"^D(\d+)(?:-({DECISION_SUFFIX_PATTERN}))?$")
SECTION_REF_RE = re.compile(rf"^D(\d+)(?:-{DECISION_SUFFIX_PATTERN})?-§(.+)$")

DECISION_EVENT_KINDS = {
    "decision_proposed",
    "decision_accepted",
    "decision_revisited",
    "decision_superseded",
    "decision_retired",
    "decision_rejected",
    "decision_metadata_updated",
    "decision_assumption_violated",
    "decision_check_failed",
    "decision_drift_detected",
    "decision_verification_recorded",
}


class DecisionStopLine(RuntimeError):
    """Raised when a decision event violates a v1.1 STOP-LINE."""

    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}")


class BacklogStopLine(RuntimeError):
    """Raised when a create-only or update-only backlog write is invalid."""

    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}")


class AgentInstanceStopLine(RuntimeError):
    """Raised when replay encounters invalid instance lifecycle or attribution."""

    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}")


def validate_backlog_write(conn: sqlite3.Connection, item_id: str, intent: str) -> None:
    """Validate explicit backlog create/update intent against the projection."""
    if intent not in {"create", "update", "upsert"}:
        raise ValueError(f"invalid backlog write_intent: {intent}")
    if intent == "upsert":
        return
    existing = conn.execute(
        "SELECT 1 FROM backlog_items WHERE id=?",
        (item_id,),
    ).fetchone()
    if intent == "create" and existing is not None:
        raise BacklogStopLine(BACKLOG_ID_COLLISION, item_id)
    if intent == "update" and existing is None:
        raise BacklogStopLine(BACKLOG_ITEM_MISSING, item_id)


def validate_decision_proposal_identity(
    conn: sqlite3.Connection,
    human_id: str,
    *,
    dec_ulid: str | None = None,
    aliases: Iterable[str] = (),
) -> None:
    """Validate a proposed decision ID against the current projection.

    Semantic decision writers call this while holding the mesh write lock and
    before writing a body or appending an event. Replay uses the same check so
    live writes and historical projection enforce one collision rule.
    """
    if not DECISION_ID_RE.fullmatch(human_id):
        raise ValueError(f"invalid decision human_id: {human_id}")
    existing = resolve_decision(conn, human_id)
    if existing is not None:
        raise DecisionStopLine(DECISION_HUMAN_ID_COLLISION, human_id)
    if dec_ulid is not None:
        existing_entity = conn.execute(
            "SELECT human_id FROM decisions WHERE dec_ulid=?",
            (dec_ulid,),
        ).fetchone()
        if existing_entity is not None:
            raise DecisionStopLine(
                DECISION_HUMAN_ID_COLLISION,
                f"{human_id} reuses {dec_ulid}",
            )
    for alias in aliases:
        existing_alias = conn.execute(
            "SELECT dec_ulid FROM decision_aliases WHERE human_id=?",
            (alias,),
        ).fetchone()
        if existing_alias is not None:
            raise DecisionStopLine(DECISION_ALIAS_FORK, alias)


def _decision_contract_version(payload: dict[str, Any]) -> int:
    raw = payload.get("decision_contract_version")
    if raw is None or raw == "":
        return 0
    if isinstance(raw, bool) or not isinstance(raw, (int, str)):
        raise DecisionStopLine(
            DECISION_METADATA_INVALID, "decision_contract_version must be an integer"
        )
    if isinstance(raw, str) and not raw.isdigit():
        raise DecisionStopLine(
            DECISION_METADATA_INVALID, "decision_contract_version must be an integer"
        )
    version = int(raw)
    if version < 0:
        raise DecisionStopLine(
            DECISION_METADATA_INVALID, "decision_contract_version must not be negative"
        )
    return version


def _validate_v5_string_arrays(
    values: dict[str, Any],
    *,
    required: bool,
) -> None:
    for field_name in (
        "affected_code_globs",
        "exemptions",
        "generated_artifact_paths",
        "required_checks",
        "tags",
    ):
        if field_name not in values:
            if required:
                raise DecisionStopLine(
                    DECISION_METADATA_INVALID, f"{field_name} must be a string array"
                )
            continue
        value = values[field_name]
        if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
            raise DecisionStopLine(
                DECISION_METADATA_INVALID, f"{field_name} must be a string array"
            )
    if "verification" not in values:
        if required:
            raise DecisionStopLine(
                DECISION_METADATA_INVALID, "verification must be an object array"
            )
    else:
        verification = values["verification"]
        if not isinstance(verification, list) or not all(
            isinstance(item, dict) for item in verification
        ):
            raise DecisionStopLine(
                DECISION_METADATA_INVALID, "verification must be an object array"
            )


def validate_decision_event(
    conn: sqlite3.Connection,
    *,
    kind: str,
    entity_id: str,
    thread_id: str,
    actor: str,
    occurred_utc: str,
    payload: dict[str, Any],
    participants: Iterable[str] | None = None,
    approval_authority_mode: str | None = None,
    approval_authority_revision: str | None = None,
) -> str:
    """Validate one decision event against the current projection.

    Durable append calls this before journaling; replay calls the same validator
    before projection so the store remains a defense-in-depth boundary.
    """

    if kind not in DECISION_EVENT_KINDS:
        raise ValueError(f"not a decision lifecycle event: {kind}")
    if kind == "decision_proposed":
        human_id = str(payload.get("human_id") or "")
        if entity_id != thread_id or not entity_id.startswith("dec_"):
            raise DecisionStopLine(
                DECISION_IDENTITY_MISMATCH,
                f"{kind} requires matching canonical dec_ entity_id and thread_id",
            )
        raw_aliases = payload.get("aliases", [])
        if not isinstance(raw_aliases, list) or not all(
            isinstance(alias, str) for alias in raw_aliases
        ):
            raise DecisionStopLine(
                DECISION_METADATA_INVALID, "decision aliases must be a string array"
            )
        aliases = tuple(raw_aliases)
        invalid_alias = next(
            (alias for alias in aliases if not DECISION_ID_RE.fullmatch(alias)),
            None,
        )
        if invalid_alias is not None:
            raise DecisionStopLine(DECISION_METADATA_INVALID, f"invalid alias: {invalid_alias}")
        if human_id in aliases or len(set(aliases)) != len(aliases):
            raise DecisionStopLine(
                DECISION_ALIAS_FORK, f"aliases for {human_id} must be unique and non-primary"
            )
        try:
            validate_decision_proposal_identity(
                conn,
                human_id,
                dec_ulid=entity_id,
                aliases=aliases,
            )
        except ValueError as exc:
            raise DecisionStopLine(DECISION_METADATA_INVALID, str(exc)) from exc
        supersedes = payload.get("supersedes")
        if supersedes:
            predecessor = resolve_decision(conn, str(supersedes))
            if predecessor is None:
                raise DecisionStopLine(DECISION_SUPERSEDE_TARGET_INVALID, str(supersedes))
            _ensure_supersede_target_valid(conn, predecessor)
            _ensure_no_supersede_cycle(conn, predecessor, entity_id)
        if _decision_contract_version(payload) >= 5:
            _validate_v5_string_arrays(payload, required=True)
            for field_name in ("assumptions", "evidence", "review_policy"):
                if field_name not in payload:
                    raise DecisionStopLine(
                        DECISION_METADATA_INVALID,
                        f"{field_name} is required for decision_contract_version=5",
                    )
            try:
                normalize_decision_assumptions(payload.get("assumptions", []))
                normalize_decision_evidence(
                    payload.get("evidence", {}),
                    allow_extensions=participants is None,
                )
                normalize_decision_review_policy(
                    payload.get("review_policy", {}),
                    participants=participants,
                    allow_extensions=participants is None,
                )
            except ValueError as exc:
                raise DecisionStopLine(DECISION_METADATA_INVALID, str(exc)) from exc
            issues = decision_applicability_issues(
                applicability_scope=payload.get("applicability_scope"),
                tier=str(payload.get("tier") or ""),
                affected_code_globs=payload.get("affected_code_globs", []),
                exemptions=payload.get("exemptions", []),
                generated_artifact_paths=payload.get("generated_artifact_paths", []),
            )
            if issues:
                raise DecisionStopLine(DECISION_METADATA_INVALID, "; ".join(issues))
        return entity_id

    dec_ulid = resolve_decision(conn, entity_id)
    payload_identifier = str(payload.get("decision_id") or "")
    payload_dec_ulid = resolve_decision(conn, payload_identifier)
    if dec_ulid is None or payload_dec_ulid is None:
        raise DecisionStopLine(DECISION_PARENT_MISSING, entity_id or payload_identifier)
    if entity_id != dec_ulid or thread_id != dec_ulid or payload_dec_ulid != dec_ulid:
        raise DecisionStopLine(
            DECISION_IDENTITY_MISMATCH,
            f"{kind} must use {dec_ulid} for entity_id, thread_id, and decision_id",
        )

    row = conn.execute(
        "SELECT dec_ulid, human_id, title, status, tier, contract_version, "
        "applicability_scope, owner, body_sha, meta_json "
        "FROM decisions WHERE dec_ulid=?",
        (dec_ulid,),
    ).fetchone()
    if row is None:
        raise DecisionStopLine(DECISION_PARENT_MISSING, dec_ulid)
    status = str(row["status"])
    if status in DECISION_TERMINAL_STATUSES and kind in DECISION_MUTATING_EVENT_KINDS:
        raise DecisionStopLine(
            DECISION_TRANSITION_INVALID, f"{kind} cannot mutate {dec_ulid} while {status}"
        )

    if kind == "decision_accepted":
        contract_version = _decision_contract_version(payload)
        if int(row["contract_version"] or 0) >= 5 and contract_version < 5:
            raise DecisionStopLine(
                DECISION_METADATA_INVALID,
                "decision_contract_version=5 is required to accept this decision revision",
            )
        if contract_version >= 5:
            accepted_by = str(payload.get("accepted_by") or "").strip()
            notes = str(payload.get("notes") or "").strip()
            approval_source = str(payload.get("approval_source") or "")
            approved_utc = str(payload.get("approved_utc") or "")
            approved_body_sha = str(payload.get("approved_body_sha") or "")
            approved_revision_sha = str(payload.get("approved_revision_sha") or "")
            recorded_authority_mode = str(payload.get("approval_authority_mode") or "")
            recorded_authority_revision = str(payload.get("approval_authority_revision") or "")
            current_revision_sha = decision_revision_sha_from_projection(conn, row)
            if not accepted_by or accepted_by != actor:
                raise DecisionStopLine(
                    DECISION_METADATA_INVALID,
                    "accepted_by must match the decision_accepted event actor",
                )
            if not notes:
                raise DecisionStopLine(
                    DECISION_METADATA_INVALID, "decision acceptance notes must not be empty"
                )
            if approval_source not in {"workbench", "interactive_cli"}:
                raise DecisionStopLine(
                    DECISION_METADATA_INVALID,
                    "approval_source must be workbench or interactive_cli",
                )
            if not approved_utc or approved_utc != occurred_utc:
                raise DecisionStopLine(
                    DECISION_METADATA_INVALID,
                    "approved_utc must match the decision_accepted event timestamp",
                )
            if approved_body_sha != str(row["body_sha"]):
                raise DecisionStopLine(
                    DECISION_METADATA_INVALID,
                    "approved_body_sha does not match the current decision body",
                )
            if bool(recorded_authority_mode) != bool(recorded_authority_revision):
                raise DecisionStopLine(
                    DECISION_METADATA_INVALID,
                    "approval authority mode and revision must be recorded together",
                )
            if recorded_authority_mode and recorded_authority_mode not in {
                "legacy_participants",
                "explicit_human_approvers",
            }:
                raise DecisionStopLine(
                    DECISION_METADATA_INVALID,
                    "approval_authority_mode is invalid",
                )
            if recorded_authority_revision and not re.fullmatch(
                r"[0-9a-f]{64}", recorded_authority_revision
            ):
                raise DecisionStopLine(
                    DECISION_METADATA_INVALID,
                    "approval_authority_revision must be a SHA-256 digest",
                )
            if approval_authority_mode is not None:
                if not recorded_authority_mode or not recorded_authority_revision:
                    raise DecisionStopLine(
                        DECISION_METADATA_INVALID,
                        "new acceptance must bind its approval authority",
                    )
                if (
                    recorded_authority_mode != approval_authority_mode
                    or recorded_authority_revision != approval_authority_revision
                ):
                    raise DecisionStopLine(
                        DECISION_METADATA_INVALID,
                        "approval authority binding does not match current configuration",
                    )
            legacy_revision_sha = ""
            if participants is None:
                legacy_revision_sha = decision_revision_sha_from_projection(
                    conn,
                    row,
                    include_authored_review_metadata=False,
                )
            legacy_matches = participants is None and approved_revision_sha == legacy_revision_sha
            if approved_revision_sha != current_revision_sha and not legacy_matches:
                raise DecisionStopLine(
                    DECISION_METADATA_INVALID,
                    "approved_revision_sha does not match the current decision revision",
                )
            raw_meta = json_loads(row["meta_json"], {})
            if not isinstance(raw_meta, dict):
                raw_meta = {}
            try:
                review_policy = normalize_decision_review_policy(
                    raw_meta.get("review_policy", {}),
                    participants=participants,
                    allow_extensions=participants is None,
                )
            except ValueError as exc:
                raise DecisionStopLine(DECISION_METADATA_INVALID, str(exc)) from exc
            required_reviewers = review_policy.get("required_reviewers", [])
            if not legacy_matches and required_reviewers and accepted_by not in required_reviewers:
                raise DecisionStopLine(
                    DECISION_METADATA_INVALID,
                    f"accepted_by {accepted_by!r} is not a required reviewer",
                )
            if not legacy_matches and any(
                isinstance(item, dict)
                and item.get("kind") == "decision_accepted"
                and isinstance(item.get("payload"), dict)
                and item["payload"].get("accepted_by") == accepted_by
                and item["payload"].get("approved_revision_sha") == approved_revision_sha
                for item in decision_current_revision_events(raw_meta)
            ):
                raise DecisionStopLine(
                    DECISION_METADATA_INVALID,
                    f"reviewer {accepted_by!r} already approved this revision",
                )
        if status != "proposed":
            raise DecisionStopLine(
                DECISION_TRANSITION_INVALID, f"cannot accept {dec_ulid} while {status}"
            )
    if kind == "decision_rejected" and _decision_contract_version(payload) >= 5:
        rejected_by = str(payload.get("rejected_by") or "").strip()
        reason = str(payload.get("reason") or "").strip()
        rejection_source = str(payload.get("rejection_source") or "")
        rejected_utc = str(payload.get("rejected_utc") or "")
        rejected_body_sha = str(payload.get("rejected_body_sha") or "")
        rejected_revision_sha = str(payload.get("rejected_revision_sha") or "")
        recorded_authority_mode = str(payload.get("rejection_authority_mode") or "")
        recorded_authority_revision = str(payload.get("rejection_authority_revision") or "")
        current_revision_sha = decision_revision_sha_from_projection(conn, row)
        if not rejected_by or rejected_by != actor:
            raise DecisionStopLine(
                DECISION_METADATA_INVALID,
                "rejected_by must match the decision_rejected event actor",
            )
        if participants is not None and rejected_by not in set(participants):
            raise DecisionStopLine(
                DECISION_METADATA_INVALID,
                f"rejected_by {rejected_by!r} is not in the direct-human authority",
            )
        if not reason:
            raise DecisionStopLine(
                DECISION_METADATA_INVALID, "decision rejection reason must not be empty"
            )
        if rejection_source != "workbench":
            raise DecisionStopLine(DECISION_METADATA_INVALID, "rejection_source must be workbench")
        if not rejected_utc or rejected_utc != occurred_utc:
            raise DecisionStopLine(
                DECISION_METADATA_INVALID,
                "rejected_utc must match the decision_rejected event timestamp",
            )
        if rejected_body_sha != str(row["body_sha"]):
            raise DecisionStopLine(
                DECISION_METADATA_INVALID,
                "rejected_body_sha does not match the current decision body",
            )
        if rejected_revision_sha != current_revision_sha:
            raise DecisionStopLine(
                DECISION_METADATA_INVALID,
                "rejected_revision_sha does not match the current decision revision",
            )
        if recorded_authority_mode not in {
            "legacy_participants",
            "explicit_human_approvers",
        }:
            raise DecisionStopLine(DECISION_METADATA_INVALID, "rejection authority mode is invalid")
        if not re.fullmatch(r"[0-9a-f]{64}", recorded_authority_revision):
            raise DecisionStopLine(
                DECISION_METADATA_INVALID,
                "rejection authority revision must be a SHA-256 digest",
            )
        if approval_authority_mode is not None and (
            recorded_authority_mode != approval_authority_mode
            or recorded_authority_revision != approval_authority_revision
        ):
            raise DecisionStopLine(
                DECISION_METADATA_INVALID,
                "rejection authority binding does not match current configuration",
            )
    if kind == "decision_superseded":
        successor = resolve_decision(conn, str(payload.get("superseded_by") or ""))
        if successor is None:
            raise DecisionStopLine(
                DECISION_SUPERSEDE_TARGET_INVALID, str(payload.get("superseded_by") or "")
            )
        _ensure_supersede_target_valid(conn, successor)
        _ensure_no_supersede_cycle(conn, dec_ulid, successor)
        if status not in {"accepted", "in_force"}:
            raise DecisionStopLine(
                DECISION_TRANSITION_INVALID, f"cannot supersede {dec_ulid} while {status}"
            )
    elif kind == "decision_retired" and status not in {"accepted", "in_force"}:
        raise DecisionStopLine(
            DECISION_TRANSITION_INVALID, f"cannot retire {dec_ulid} while {status}"
        )
    elif kind == "decision_rejected" and status != "proposed":
        raise DecisionStopLine(
            DECISION_TRANSITION_INVALID, f"cannot reject {dec_ulid} while {status}"
        )
    elif kind == "decision_revisited":
        new_id = payload.get("new_decision_id")
        if new_id and resolve_decision(conn, str(new_id)) is None:
            raise DecisionStopLine(DECISION_PARENT_MISSING, str(new_id))
    elif kind == "decision_metadata_updated":
        _validate_decision_metadata_update(conn, dec_ulid, row, payload, participants=participants)
    elif kind == "decision_verification_recorded":
        outcome = str(payload.get("outcome") or "")
        if outcome not in {"pass", "fail"}:
            raise DecisionStopLine(
                DECISION_METADATA_INVALID,
                f"invalid decision verification outcome: {outcome}",
            )
    return dec_ulid


def _validate_decision_metadata_update(
    conn: sqlite3.Connection,
    dec_ulid: str,
    row: sqlite3.Row,
    payload: dict[str, Any],
    *,
    participants: Iterable[str] | None = None,
) -> None:
    fields = payload.get("fields_changed")
    if not isinstance(fields, dict) or not fields:
        raise DecisionStopLine(
            DECISION_METADATA_INVALID, "fields_changed must be a non-empty object"
        )
    malformed = next(
        (
            field_name
            for field_name, old_new in fields.items()
            if not isinstance(old_new, list) or len(old_new) != 2
        ),
        None,
    )
    if malformed is not None:
        raise DecisionStopLine(
            DECISION_METADATA_INVALID, f"{malformed} must contain [old, new] values"
        )

    status = str(row["status"])
    status_change = fields.get("status")
    if status_change is not None:
        old_status, new_status = (str(status_change[0]), str(status_change[1]))
        allowed = {
            ("accepted", "in_force"),
            ("accepted", "proposed"),
            ("in_force", "proposed"),
        }
        if old_status != status or (old_status, new_status) not in allowed:
            raise DecisionStopLine(
                DECISION_TRANSITION_INVALID,
                f"invalid metadata status transition {old_status}->{new_status} from {status}",
            )
    if status in {"accepted", "in_force"} and set(fields) != {"status"}:
        if not isinstance(status_change, list) or str(status_change[1]) != "proposed":
            raise DecisionStopLine(
                DECISION_TRANSITION_INVALID,
                f"editing {dec_ulid} while {status} must return it to proposed",
            )
    if status == "proposed" and status_change is not None:
        raise DecisionStopLine(
            DECISION_TRANSITION_INVALID,
            "proposed decision metadata cannot set lifecycle status directly",
        )

    human_id_change = fields.get("human_id")
    if human_id_change is not None:
        old_human_id, new_human_id = map(str, human_id_change)
        if old_human_id != str(row["human_id"]) or not DECISION_ID_RE.fullmatch(new_human_id):
            raise DecisionStopLine(
                DECISION_METADATA_INVALID,
                f"invalid human_id change {old_human_id}->{new_human_id}",
            )
        existing = resolve_decision(conn, new_human_id)
        if existing is not None and existing != dec_ulid:
            raise DecisionStopLine(DECISION_ALIAS_FORK, new_human_id)

    tier_change = fields.get("tier")
    if tier_change is not None and not str(tier_change[1]).strip():
        raise DecisionStopLine(DECISION_METADATA_INVALID, "tier must not be empty")

    if _decision_contract_version(payload) >= 5:
        changed_values = {
            field_name: old_new[1]
            for field_name, old_new in fields.items()
            if isinstance(old_new, list) and len(old_new) == 2
        }
        _validate_v5_string_arrays(changed_values, required=False)
        try:
            if "assumptions" in changed_values:
                normalize_decision_assumptions(changed_values["assumptions"])
            if "evidence" in changed_values:
                normalize_decision_evidence(
                    changed_values["evidence"],
                    allow_extensions=participants is None,
                )
            if "review_policy" in changed_values:
                normalize_decision_review_policy(
                    changed_values["review_policy"],
                    participants=participants,
                    allow_extensions=participants is None,
                )
        except ValueError as exc:
            raise DecisionStopLine(DECISION_METADATA_INVALID, str(exc)) from exc
        affected = [
            str(item["pattern"])
            for item in conn.execute(
                "SELECT pattern FROM decision_globs "
                "WHERE dec_ulid=? AND kind='affected' ORDER BY pattern",
                (dec_ulid,),
            )
        ]
        if "affected_code_globs" in fields:
            raw_affected = fields["affected_code_globs"][1]
            affected = list(raw_affected) if isinstance(raw_affected, list) else []
        meta = json_loads(row["meta_json"], {})
        if not isinstance(meta, dict):
            meta = {}
        exemptions = meta.get("exemptions", [])
        generated = meta.get("generated_artifact_paths", [])
        if "exemptions" in fields:
            exemptions = fields["exemptions"][1]
        if "generated_artifact_paths" in fields:
            generated = fields["generated_artifact_paths"][1]
        scope = (
            fields["applicability_scope"][1]
            if "applicability_scope" in fields
            else row["applicability_scope"]
        )
        tier = fields["tier"][1] if "tier" in fields else row["tier"]
        issues = decision_applicability_issues(
            applicability_scope=scope,
            tier=str(tier),
            affected_code_globs=affected,
            exemptions=exemptions if isinstance(exemptions, list) else [],
            generated_artifact_paths=generated if isinstance(generated, list) else [],
        )
        if issues:
            raise DecisionStopLine(DECISION_METADATA_INVALID, "; ".join(issues))


class DispatchStopLine(RuntimeError):
    """Raised when a dispatch event violates a dispatch-domain STOP-LINE at replay time."""

    def __init__(self, code: str, detail: str) -> None:
        self.code = code
        self.detail = detail
        super().__init__(f"{code}: {detail}")


@dataclass(frozen=True)
class RebuildResult:
    event_count: int
    last_event_seq: int
    table_hashes: dict[str, str]
    schema_sha256: str


@dataclass(frozen=True)
class DecisionReplayDiagnostic:
    valid_events: int
    total_events: int
    event_seq: int | None = None
    event_id: str = ""
    kind: str = ""
    code: str = ""
    detail: str = ""

    @property
    def ok(self) -> bool:
        return self.event_seq is None


def diagnose_decision_replay(
    config: AgentMeshConfig,
    *,
    records: Iterable[dict[str, Any]],
) -> DecisionReplayDiagnostic:
    """Replay in memory and identify the first decision stop-line without mutations."""

    replay_records = list(records)
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    try:
        initialize_schema(conn)
        for index, record in enumerate(replay_records):
            try:
                apply_record(record, config, conn=conn, require_next=True)
            except DecisionStopLine as exc:
                return DecisionReplayDiagnostic(
                    valid_events=index,
                    total_events=len(replay_records),
                    event_seq=int(record["event_seq"]),
                    event_id=str(record["event_id"]),
                    kind=str(record["kind"]),
                    code=exc.code,
                    detail=exc.detail,
                )
        records_by_seq = {int(record["event_seq"]): record for record in replay_records}
        for row in conn.execute("SELECT * FROM decisions ORDER BY human_id"):
            body_path = str(row["body_path"] or "")
            body_bytes = int(row["body_bytes"] or 0)
            # Historical/imported decisions may preserve a byte count without
            # a repository body object. Existing approval completeness checks
            # already surface that legacy condition; this diagnostic targets
            # divergence in bodies that can actually be verified and parsed.
            if not body_path or Path(body_path).parts[:1] != ("bodies",):
                continue
            try:
                body = read_verified_decision_body(
                    config.agent_dir,
                    body_path=body_path,
                    body_sha=str(row["body_sha"] or ""),
                    body_bytes=body_bytes,
                ).decode("utf-8", errors="strict")
            except (DecisionBodyIntegrityError, UnicodeDecodeError) as exc:
                source = records_by_seq.get(int(row["event_seq"]), {})
                return DecisionReplayDiagnostic(
                    valid_events=len(replay_records),
                    total_events=len(replay_records),
                    event_seq=int(row["event_seq"]),
                    event_id=str(source.get("event_id") or ""),
                    kind=str(source.get("kind") or "decision_projection"),
                    code=DECISION_BODY_PROJECTION_DIVERGENCE,
                    detail=f"{row['human_id']} canonical body integrity failed: {exc}",
                )
            meta = json_loads(row["meta_json"], {})
            if not isinstance(meta, dict):
                meta = {}
            if meta.get("body_format") != DECISION_BODY_FORMAT_GENERATED_V1:
                continue
            parsed = parse_generated_decision_body(body, human_id=str(row["human_id"]))
            if parsed is None:
                source = records_by_seq.get(int(row["event_seq"]), {})
                return DecisionReplayDiagnostic(
                    valid_events=len(replay_records),
                    total_events=len(replay_records),
                    event_seq=int(row["event_seq"]),
                    event_id=str(source.get("event_id") or ""),
                    kind=str(source.get("kind") or "decision_projection"),
                    code=DECISION_BODY_PROJECTION_DIVERGENCE,
                    detail=f"{row['human_id']} generated canonical body has invalid structure",
                )
            projected = {
                "title": str(row["title"] or ""),
                "context": str(meta.get("context") or ""),
                "decision": str(meta.get("decision") or ""),
            }
            mismatched = sorted(
                field_name for field_name, value in parsed.items() if value != projected[field_name]
            )
            if mismatched:
                source = records_by_seq.get(int(row["event_seq"]), {})
                return DecisionReplayDiagnostic(
                    valid_events=len(replay_records),
                    total_events=len(replay_records),
                    event_seq=int(row["event_seq"]),
                    event_id=str(source.get("event_id") or ""),
                    kind=str(source.get("kind") or "decision_projection"),
                    code=DECISION_BODY_PROJECTION_DIVERGENCE,
                    detail=(
                        f"{row['human_id']} generated canonical body disagrees with projected "
                        f"metadata fields: {', '.join(mismatched)}"
                    ),
                )
        return DecisionReplayDiagnostic(
            valid_events=len(replay_records), total_events=len(replay_records)
        )
    finally:
        conn.close()


def rebuild_all(
    config: AgentMeshConfig | None = None,
    *,
    start: str | Path | None = None,
) -> RebuildResult:
    cfg = config or load_config(start)
    ensure_project_dirs(cfg)
    conn = connect(cfg.db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        event_count = 0
        last_seq = 0
        try:
            reset_schema(conn, preserve_transaction=True)
            for record in read_event_records(cfg.events_path):
                apply_record(
                    record,
                    cfg,
                    conn=conn,
                    require_next=True,
                    manage_transaction=False,
                )
                event_count += 1
                last_seq = int(record["event_seq"])
            source_log_sha256 = file_sha256(cfg.events_path)
            set_meta(conn, "events_jsonl_sha", source_log_sha256)
            set_meta(conn, "projection_version", PROJECTION_VERSION)
            table_hashes = table_hashes_for_connection(conn)
            schema_sha256 = schema_sha256_for_connection(conn)
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        return RebuildResult(
            event_count=event_count,
            last_event_seq=last_seq,
            table_hashes=table_hashes,
            schema_sha256=schema_sha256,
        )
    finally:
        conn.close()


def apply_event(event: Event | dict[str, Any], agent_dir: str | Path | None = None) -> None:
    """Project a single event after it has been durably appended."""
    record = event.to_dict() if isinstance(event, Event) else event
    cfg = _config_from_agent_dir(agent_dir)
    ensure_project_dirs(cfg)
    conn = connect(cfg.db_path)
    try:
        apply_record(record, cfg, conn=conn, require_next=True)
        with conn:
            set_meta(conn, "events_jsonl_sha", file_sha256(cfg.events_path))
    finally:
        conn.close()


def apply_record(
    record: dict[str, Any],
    config: AgentMeshConfig,
    *,
    conn,
    require_next: bool,
    manage_transaction: bool = True,
) -> None:
    if manage_transaction:
        initialize_schema(conn)
    event_seq = int(record["event_seq"])
    if manage_transaction:
        last_seq = get_last_event_seq(conn)
    else:
        row = conn.execute("SELECT last_event_seq FROM events_seen LIMIT 1").fetchone()
        last_seq = int(row["last_event_seq"]) if row else 0
    if event_seq <= last_seq:
        return
    if require_next and event_seq != last_seq + 1:
        raise RuntimeError(
            f"projection gap: next event_seq must be {last_seq + 1}, got {event_seq}"
        )

    if not manage_transaction:
        _project_record(conn, record, config)
        set_last_event_seq(conn, event_seq, str(record.get("occurred_utc", utc_now())))
        return

    with conn:
        _project_record(conn, record, config)
        set_last_event_seq(conn, event_seq, str(record.get("occurred_utc", utc_now())))


def read_event_records(events_path: str | Path) -> list[dict[str, Any]]:
    path = Path(events_path)
    if not path.exists():
        return []
    records = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                records.append(json.loads(line))
    return sorted(records, key=lambda item: int(item["event_seq"]))


def table_hashes_for(config: AgentMeshConfig) -> dict[str, str]:
    conn = connect(config.db_path)
    try:
        initialize_schema(conn)
        return table_hashes_for_connection(conn)
    finally:
        conn.close()


def table_hashes_for_connection(conn: sqlite3.Connection) -> dict[str, str]:
    """Fingerprint every projection table through the caller's SQLite snapshot."""

    from agent_mesh.store.sqlite import canonical_table_dump

    hashes: dict[str, str] = {}
    for table in ALL_TABLES:
        payload = json_dumps(canonical_table_dump(conn, table)).encode("utf-8")
        hashes[table] = hashlib.sha256(payload).hexdigest()
    return hashes


def schema_sha256_for_connection(conn: sqlite3.Connection) -> str:
    """Fingerprint every non-internal object in the disposable projection schema."""

    rows = conn.execute(
        "SELECT type, name, tbl_name, sql FROM sqlite_schema "
        "WHERE name NOT LIKE 'sqlite_%' ORDER BY type, name"
    ).fetchall()
    payload = json_dumps([dict(row) for row in rows]).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def file_sha256(path: str | Path) -> str:
    file_path = Path(path)
    if not file_path.exists():
        return hashlib.sha256(b"").hexdigest()
    digest = hashlib.sha256()
    with file_path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def projection_is_current(config: AgentMeshConfig) -> bool:
    """Return whether SQLite exactly projects the current event log and code contract.

    Callers that use this result to skip a rebuild must hold the project mail
    lock while checking it, so an append cannot race between the SHA comparison
    and the subsequent read.
    """
    if not config.db_path.exists():
        return False
    expected_sha = file_sha256(config.events_path)
    try:
        conn = connect(config.db_path)
        try:
            return (
                get_meta(conn, "projection_version") == PROJECTION_VERSION
                and get_meta(conn, "events_jsonl_sha") == expected_sha
            )
        finally:
            conn.close()
    except sqlite3.DatabaseError:
        return False


def _project_record(conn, record: dict[str, Any], config: AgentMeshConfig) -> None:
    kind = str(record["kind"])
    validate_event_provenance(kind, record.get("payload", {}), str(record.get("entity_id", "")))
    try:
        validate_actor_shadow(kind, str(record.get("actor", "")), record.get("payload", {}))
    except AgentInstanceError as exc:
        code, _, detail = str(exc).partition(": ")
        raise AgentInstanceStopLine(code, detail or str(exc)) from exc
    _validate_projected_instance_attribution(conn, record, config)
    if kind in INSTANCE_EVENT_KINDS:
        _project_agent_instance_event(conn, record, config)
    elif kind == "req_created":
        _project_req_created(conn, record, config)
    elif kind == "res_posted":
        _project_res_posted(conn, record)
    elif kind == "req_status_changed":
        _project_req_status_changed(conn, record)
    elif kind == "req_claimed":
        conn.execute(
            "UPDATE messages SET claimed_by=?, claimed_utc=?, updated_utc=?, event_seq=? "
            "WHERE id=? AND kind='request'",
            (
                record["payload"].get("claimed_by"),
                record["occurred_utc"],
                record["occurred_utc"],
                record["event_seq"],
                record["entity_id"],
            ),
        )
    elif kind == "req_unclaimed":
        conn.execute(
            "UPDATE messages SET claimed_by=NULL, claimed_utc=NULL, updated_utc=?, event_seq=? "
            "WHERE id=? AND kind='request'",
            (record["occurred_utc"], record["event_seq"], record["entity_id"]),
        )
    elif kind == "message_ref_added":
        payload = record["payload"]
        message_id = payload.get("source_message_id") or record["entity_id"]
        conn.execute(
            "INSERT OR IGNORE INTO message_refs(message_id, ref_type, ref_value) VALUES (?, ?, ?)",
            (
                message_id,
                payload["ref_type"],
                payload["ref_value"],
            ),
        )
        if payload.get("ref_type") == "origin":
            row = conn.execute(
                "SELECT workflow_origin, workflow_origin_source FROM messages WHERE id=?",
                (message_id,),
            ).fetchone()
            value = str(payload.get("ref_value") or "").strip()
            if row is not None and not row["workflow_origin"]:
                conn.execute(
                    "UPDATE messages SET workflow_origin=?, workflow_origin_valid=1, "
                    "workflow_origin_source='legacy_ref' WHERE id=?",
                    (value, message_id),
                )
            elif row is not None and row["workflow_origin"] != value:
                conn.execute(
                    "UPDATE messages SET workflow_origin_valid=0, "
                    "workflow_origin_source='conflict' WHERE id=?",
                    (message_id,),
                )
    elif kind in DECISION_EVENT_KINDS:
        _project_decision_event(conn, record, config)
    elif kind in DISPATCH_EVENT_KINDS:
        _project_dispatch_event(conn, record, config)
    elif kind == "backlog_item_upserted":
        _project_backlog_item_upserted(conn, record, config)
    elif kind == "backlog_link_added":
        _project_backlog_link_added(conn, record)
    elif kind == "backlog_event_recorded":
        _project_backlog_event_recorded(conn, record)
    elif kind in {
        "decision_reference_observed",
        "decision_reference_resolved",
        "decision_scanner_run_completed",
        "projection_regenerated",
        "index_rebuilt",
        "projection_stale_detected",
        "projection_size_exceeded",
        "message_backfilled",
        "backfill_batch_completed",
        "message_body_stored",
    }:
        _project_ops_event(conn, record)
    _project_instance_last_seen(conn, record)


def _validate_projected_instance_attribution(
    conn, record: dict[str, Any], config: AgentMeshConfig
) -> None:
    actor = str(record.get("actor", ""))
    instance_id = str(record.get("actor_instance_id", "")).strip()
    kind = str(record.get("kind", ""))
    payload = record.get("payload", {})
    if not isinstance(payload, dict):
        payload = {}
    direct_human_control = is_direct_human_control_event(kind, payload)
    direct_human_authorized = has_persisted_direct_human_authority(
        kind=kind,
        actor=actor,
        payload=payload,
    )
    if instance_id:
        if direct_human_control:
            raise AgentInstanceStopLine(
                "AGENT_INSTANCE_FORBIDDEN_FOR_HUMAN_EVENT",
                f"{kind} must record direct human authority without an AI-agent instance",
            )
        row = conn.execute(
            "SELECT participant, status FROM agent_instances WHERE id=?", (instance_id,)
        ).fetchone()
        if row is None:
            raise AgentInstanceStopLine("AGENT_INSTANCE_UNKNOWN", instance_id)
        if row["status"] == "terminal":
            raise AgentInstanceStopLine("AGENT_INSTANCE_TERMINAL", instance_id)
        if row["status"] != "active":
            raise AgentInstanceStopLine("AGENT_INSTANCE_RETIRED", instance_id)
        if row["participant"] != actor:
            raise AgentInstanceStopLine(
                "AGENT_INSTANCE_ACTOR_MISMATCH",
                f"{instance_id} belongs to {row['participant']!r}, not {actor!r}",
            )
        return
    manual_registration_bootstrap = (
        record.get("kind") == "agent_instance_registered"
        and isinstance(payload, dict)
        and str(payload.get("registration_origin", "")).strip().lower() == "manual"
        and str(payload.get("registrar", "")).strip() == actor
    )
    active = conn.execute(
        "SELECT id FROM agent_instances WHERE participant=? AND status='active' LIMIT 1",
        (actor,),
    ).fetchone()
    if active is not None and not direct_human_authorized and not manual_registration_bootstrap:
        raise AgentInstanceStopLine(
            "AGENT_INSTANCE_REQUIRED", f"participant {actor!r} has active registered instances"
        )


def validate_event_projection(config: AgentMeshConfig, records: Iterable[dict[str, Any]]) -> None:
    """Run the exact full projector against an isolated database before canonical append."""

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    try:
        initialize_schema(conn)
        for record in sorted(records, key=lambda item: int(item.get("event_seq", 0))):
            apply_record(record, config, conn=conn, require_next=True)
    finally:
        conn.close()


def validate_agent_instance_projection(
    config: AgentMeshConfig, records: Iterable[dict[str, Any]]
) -> None:
    """Backward-compatible name for full candidate projection of instance events."""

    validate_event_projection(config, records)


def _project_agent_instance_event(conn, record: dict[str, Any], config: AgentMeshConfig) -> None:
    kind = str(record["kind"])
    payload = record.get("payload", {})
    identifier = str(payload.get("id") or record["entity_id"])
    event_seq = int(record["event_seq"])
    if kind == "agent_instance_registered":
        if not INSTANCE_ID_RE.fullmatch(identifier):
            raise AgentInstanceStopLine("AGENT_INSTANCE_ID_INVALID", identifier)
        participant = str(payload.get("participant", "")).strip()
        provider = str(payload.get("provider", "")).strip().lower()
        contract_version = payload.get("instance_contract_version", 1)
        if (
            isinstance(contract_version, bool)
            or not isinstance(contract_version, int)
            or contract_version < 1
        ):
            raise AgentInstanceStopLine(
                "AGENT_INSTANCE_CONTRACT_VERSION_INVALID", str(contract_version)
            )
        try:
            label = (
                normalize_new_instance_handle(
                    str(payload.get("label", "")), participant=participant
                )
                if contract_version >= INSTANCE_CONTRACT_VERSION
                else normalize_instance_label(str(payload.get("label", "")))
            )
            session_digest = validate_external_session_ref_digest(
                str(payload.get("external_session_ref_digest", ""))
            )
            launch_digest = validate_external_session_ref_digest(
                str(payload.get("launch_attempt_digest", ""))
            )
        except ValueError as exc:
            raise AgentInstanceStopLine("AGENT_INSTANCE_METADATA_INVALID", str(exc)) from exc
        if not participant or not provider:
            raise AgentInstanceStopLine(
                "AGENT_INSTANCE_METADATA_INVALID",
                "participant and provider must be non-empty",
            )
        if conn.execute("SELECT 1 FROM agent_instances WHERE id=?", (identifier,)).fetchone():
            raise AgentInstanceStopLine("AGENT_INSTANCE_ID_COLLISION", identifier)
        if conn.execute("SELECT 1 FROM agent_instance_aliases WHERE label=?", (label,)).fetchone():
            raise AgentInstanceStopLine("AGENT_INSTANCE_LABEL_COLLISION", label)
        capabilities = payload.get("capabilities", [])
        if not isinstance(capabilities, list) or not all(
            isinstance(item, str) and item.strip() for item in capabilities
        ):
            raise AgentInstanceStopLine(
                "AGENT_INSTANCE_METADATA_INVALID", "capabilities must be strings"
            )
        if len(set(capabilities)) != len(capabilities):
            raise AgentInstanceStopLine(
                "AGENT_INSTANCE_METADATA_INVALID", "capabilities must be unique"
            )
        resumable = payload.get("resumable", False)
        concurrent_attachment = payload.get("concurrent_attachment", False)
        if not isinstance(resumable, bool) or not isinstance(concurrent_attachment, bool):
            raise AgentInstanceStopLine(
                "AGENT_INSTANCE_METADATA_INVALID", "lifecycle flags must be booleans"
            )
        adapter_trust = str(payload.get("adapter_trust", "legacy")).strip().lower()
        session_identity_mode = str(payload.get("session_identity_mode", "legacy")).strip().lower()
        terminal_observation = str(payload.get("terminal_observation", "legacy")).strip().lower()
        registration_origin = str(payload.get("registration_origin", "legacy")).strip().lower()
        registrar = str(payload.get("registrar", record.get("actor", ""))).strip()
        parent_instance_id = str(payload.get("parent_instance_id", "")).strip()
        originating_run_id = str(payload.get("originating_run_id", "")).strip()
        if contract_version >= INSTANCE_CONTRACT_VERSION:
            runtime_profile = str(payload.get("runtime_profile", "")).strip()
            profile = config.runtime_profiles.get(runtime_profile)
            # Disabled profiles remain authoritative for immutable historical
            # instance events. Launch-time resolution still rejects them.
            if profile is None:
                raise AgentInstanceStopLine("RUNTIME_PROFILE_UNKNOWN", runtime_profile)
            expected_profile = {
                "participant": profile.target,
                "provider": profile.provider,
                "durable_role": profile.durable_role,
                "role": profile.role,
                "capabilities": list(profile.required_capabilities),
                "permission_mode": profile.permission_mode,
                "authentication_mode": profile.authentication_mode,
                "billing_mode": profile.billing_mode,
                "adapter_trust": profile.adapter_trust,
                "session_identity_mode": profile.session_identity_mode,
                "resumable": profile.resumable,
                "concurrent_attachment": profile.concurrent_attachment,
                "terminal_observation": profile.terminal_observation,
            }
            actual_profile = {
                "participant": participant,
                **{name: payload.get(name) for name in expected_profile if name != "participant"},
            }
            mismatches = [
                name for name, value in expected_profile.items() if actual_profile[name] != value
            ]
            if mismatches:
                raise AgentInstanceStopLine(
                    "AGENT_INSTANCE_PROFILE_MISMATCH",
                    ", ".join(sorted(mismatches)),
                )
            if adapter_trust not in INSTANCE_ADAPTER_TRUST_SOURCES - {"legacy", "unmanaged"}:
                raise AgentInstanceStopLine("AGENT_INSTANCE_ADAPTER_UNTRUSTED", adapter_trust)
            if session_identity_mode not in INSTANCE_SESSION_IDENTITY_MODES - {"legacy"}:
                raise AgentInstanceStopLine(
                    "AGENT_INSTANCE_SESSION_MODE_INVALID", session_identity_mode
                )
            if terminal_observation not in INSTANCE_TERMINAL_OBSERVATION_MODES - {"legacy"}:
                raise AgentInstanceStopLine(
                    "AGENT_INSTANCE_TERMINAL_OBSERVATION_INVALID", terminal_observation
                )
            if registration_origin not in INSTANCE_REGISTRATION_ORIGINS - {"legacy", "manual"}:
                raise AgentInstanceStopLine(
                    "AGENT_INSTANCE_REGISTRATION_ORIGIN_INVALID", registration_origin
                )
            if registrar != str(record.get("actor", "")).strip():
                raise AgentInstanceStopLine("AGENT_INSTANCE_REGISTRAR_MISMATCH", registrar)
            if registration_origin == "integration":
                if registrar not in config.identity.integration_registrars:
                    raise AgentInstanceStopLine("AGENT_INSTANCE_REGISTRAR_UNAUTHORIZED", registrar)
                if parent_instance_id or originating_run_id or record.get("actor_instance_id"):
                    raise AgentInstanceStopLine(
                        "AGENT_INSTANCE_INTEGRATION_LINEAGE_INVALID", identifier
                    )
            else:
                if not parent_instance_id:
                    if (
                        registrar not in config.decision_approval_identities
                        or record.get("actor_instance_id")
                        or not originating_run_id
                    ):
                        raise AgentInstanceStopLine(
                            "AGENT_INSTANCE_HUMAN_DISPATCH_AUTHORITY_INVALID", registrar
                        )
                else:
                    parent = conn.execute(
                        "SELECT participant, status FROM agent_instances WHERE id=?",
                        (parent_instance_id,),
                    ).fetchone()
                    if parent is None:
                        raise AgentInstanceStopLine(
                            "AGENT_INSTANCE_PARENT_UNKNOWN", parent_instance_id
                        )
                    if parent["status"] != "active":
                        raise AgentInstanceStopLine(
                            "AGENT_INSTANCE_PARENT_NOT_ACTIVE", parent_instance_id
                        )
                    if (
                        record.get("actor_instance_id") != parent_instance_id
                        or parent["participant"] != registrar
                        or not originating_run_id
                    ):
                        raise AgentInstanceStopLine("AGENT_INSTANCE_REGISTRAR_MISMATCH", registrar)
                dispatch_run = conn.execute(
                    "SELECT target_agent FROM dispatch_runs WHERE run_id=?",
                    (originating_run_id,),
                ).fetchone()
                if dispatch_run is None or dispatch_run["target_agent"] != participant:
                    raise AgentInstanceStopLine(
                        "AGENT_INSTANCE_ORIGINATING_RUN_INVALID",
                        originating_run_id,
                    )
            required_metadata = (
                "durable_role",
                "role",
                "permission_mode",
                "authentication_mode",
                "billing_mode",
            )
            if any(not str(payload.get(name, "")).strip() for name in required_metadata):
                raise AgentInstanceStopLine(
                    "AGENT_INSTANCE_METADATA_INVALID", "required runtime metadata is missing"
                )
            if session_identity_mode == "exact" and not session_digest:
                raise AgentInstanceStopLine("AGENT_INSTANCE_EXACT_SESSION_REQUIRED", identifier)
            if session_identity_mode == "none" and session_digest:
                raise AgentInstanceStopLine("AGENT_INSTANCE_SESSION_MODE_CONFLICT", identifier)
            if resumable and session_identity_mode != "exact":
                raise AgentInstanceStopLine("AGENT_INSTANCE_RESUME_UNSUPPORTED", identifier)
            lifecycle_disposition = str(payload.get("lifecycle_disposition", "")).strip()
            if lifecycle_disposition not in INSTANCE_LIFECYCLE_DISPOSITIONS:
                raise AgentInstanceStopLine(
                    "AGENT_INSTANCE_DISPOSITION_INVALID", lifecycle_disposition
                )
        try:
            conn.execute(
                """
            INSERT INTO agent_instances(
              id, participant, provider, label, workstream, runtime_profile,
              external_session_ref_digest, instance_contract_version, durable_role,
              role, capabilities_json, permission_mode, authentication_mode, billing_mode,
              adapter_trust, session_identity_mode, resumable, concurrent_attachment,
              terminal_observation, registrar, registration_origin, parent_instance_id,
              originating_run_id, launch_attempt_digest, last_binding_source,
              active_launch_run_ids_json, terminal_outcome, terminal_utc,
              status, created_utc, updated_utc,
              created_event_seq, event_seq
            ) VALUES (
              :id, :participant, :provider, :label, :workstream, :runtime_profile,
              :session_digest, :contract_version, :durable_role, :role, :capabilities,
              :permission_mode, :authentication_mode, :billing_mode, :adapter_trust,
              :session_identity_mode, :resumable, :concurrent_attachment,
              :terminal_observation, :registrar, :registration_origin,
              :parent_instance_id, :originating_run_id, :launch_attempt_digest, '',
              '[]', '', NULL, 'active', :occurred_utc, :occurred_utc,
              :event_seq, :event_seq
            )
            """,
                {
                    "id": identifier,
                    "participant": participant,
                    "provider": provider,
                    "label": label,
                    "workstream": str(payload.get("workstream", "")).strip(),
                    "runtime_profile": str(payload.get("runtime_profile", "")).strip(),
                    "session_digest": session_digest,
                    "contract_version": contract_version,
                    "durable_role": str(payload.get("durable_role", "")).strip(),
                    "role": str(payload.get("role", "")).strip(),
                    "capabilities": json_dumps(capabilities),
                    "permission_mode": str(payload.get("permission_mode", "")).strip(),
                    "authentication_mode": str(payload.get("authentication_mode", "")).strip(),
                    "billing_mode": str(payload.get("billing_mode", "")).strip(),
                    "adapter_trust": adapter_trust,
                    "session_identity_mode": session_identity_mode,
                    "resumable": int(resumable),
                    "concurrent_attachment": int(concurrent_attachment),
                    "terminal_observation": terminal_observation,
                    "registrar": registrar,
                    "registration_origin": registration_origin,
                    "parent_instance_id": parent_instance_id,
                    "originating_run_id": originating_run_id,
                    "launch_attempt_digest": launch_digest,
                    "occurred_utc": record["occurred_utc"],
                    "event_seq": event_seq,
                },
            )
        except sqlite3.IntegrityError as exc:
            raise AgentInstanceStopLine("AGENT_INSTANCE_IDENTITY_COLLISION", identifier) from exc
        conn.execute(
            "INSERT INTO agent_instance_aliases(label, instance_id, is_primary, event_seq) "
            "VALUES (?, ?, 1, ?)",
            (label, identifier, event_seq),
        )
        if launch_digest:
            conn.execute(
                "INSERT INTO agent_instance_launch_attempts("
                "digest, instance_id, lifecycle_disposition, event_seq) "
                "VALUES (?, ?, ?, ?)",
                (
                    launch_digest,
                    identifier,
                    str(payload.get("lifecycle_disposition", "")).strip(),
                    event_seq,
                ),
            )
        return

    current = conn.execute("SELECT * FROM agent_instances WHERE id=?", (identifier,)).fetchone()
    if current is None:
        raise AgentInstanceStopLine("AGENT_INSTANCE_UNKNOWN", identifier)
    if current["status"] == "terminal":
        raise AgentInstanceStopLine("AGENT_INSTANCE_TERMINAL", identifier)
    if current["status"] != "active":
        raise AgentInstanceStopLine("AGENT_INSTANCE_RETIRED", identifier)
    if kind == "agent_instance_bound":
        _validate_projected_lifecycle_actor(current, record, identifier)
        binding_source = str(payload.get("binding_source", "")).strip()
        if not binding_source:
            raise AgentInstanceStopLine(
                "AGENT_INSTANCE_BINDING_INVALID", "binding_source must be non-empty"
            )
        expected = {
            "runtime_profile": str(current["runtime_profile"] or ""),
            "external_session_ref_digest": str(current["external_session_ref_digest"] or ""),
        }
        if any(str(payload.get(name, "")).strip() != value for name, value in expected.items()):
            raise AgentInstanceStopLine("AGENT_INSTANCE_BINDING_MISMATCH", identifier)
        launch_digest = str(payload.get("launch_attempt_digest", "")).strip()
        try:
            launch_digest = validate_external_session_ref_digest(launch_digest)
        except ValueError as exc:
            raise AgentInstanceStopLine("AGENT_INSTANCE_BINDING_INVALID", str(exc)) from exc
        launch_owner = (
            conn.execute(
                "SELECT instance_id, lifecycle_disposition "
                "FROM agent_instance_launch_attempts WHERE digest=?",
                (launch_digest,),
            ).fetchone()
            if launch_digest
            else None
        )
        if launch_owner is not None and launch_owner["instance_id"] != identifier:
            raise AgentInstanceStopLine("AGENT_INSTANCE_LAUNCH_KEY_COLLISION", identifier)
        if binding_source == "launch-attempt-retry" and launch_owner is None:
            raise AgentInstanceStopLine("AGENT_INSTANCE_BINDING_LAUNCH_MISMATCH", identifier)
        lifecycle_disposition = str(payload.get("lifecycle_disposition", "")).strip()
        if lifecycle_disposition not in INSTANCE_LIFECYCLE_DISPOSITIONS:
            raise AgentInstanceStopLine("AGENT_INSTANCE_DISPOSITION_INVALID", lifecycle_disposition)
        if (
            launch_owner is not None
            and launch_owner["lifecycle_disposition"]
            and launch_owner["lifecycle_disposition"] != lifecycle_disposition
        ):
            raise AgentInstanceStopLine("AGENT_INSTANCE_LAUNCH_KEY_METADATA_CONFLICT", identifier)
        resumed = payload.get("provider_context_resumed")
        if not isinstance(resumed, bool):
            raise AgentInstanceStopLine(
                "AGENT_INSTANCE_BINDING_INVALID", "provider_context_resumed must be boolean"
            )
        expected_resumed = (
            binding_source in {"session-digest", "launch-attempt-retry"}
            and bool(current["resumable"])
            and current["session_identity_mode"] == "exact"
            and expected["external_session_ref_digest"]
            == str(payload.get("external_session_ref_digest", "")).strip()
        )
        if resumed != expected_resumed:
            raise AgentInstanceStopLine("AGENT_INSTANCE_BINDING_RESUME_MISMATCH", identifier)
        if launch_digest and launch_owner is None:
            conn.execute(
                "INSERT INTO agent_instance_launch_attempts("
                "digest, instance_id, lifecycle_disposition, event_seq) "
                "VALUES (?, ?, ?, ?)",
                (launch_digest, identifier, lifecycle_disposition, event_seq),
            )
        conn.execute(
            "UPDATE agent_instances SET last_binding_source=?, updated_utc=?, event_seq=? "
            "WHERE id=?",
            (binding_source, record["occurred_utc"], event_seq, identifier),
        )
        return
    if kind == "agent_instance_launch_bound":
        _validate_projected_lifecycle_actor(current, record, identifier)
        run_id = str(payload.get("run_id", "")).strip()
        if not run_id:
            raise AgentInstanceStopLine("AGENT_INSTANCE_LAUNCH_INVALID", "run_id must be non-empty")
        prior_launch = conn.execute(
            "SELECT 1 FROM agent_instance_launch_runs WHERE instance_id=? LIMIT 1",
            (identifier,),
        ).fetchone()
        if (
            current["registration_origin"] == "dispatch"
            and prior_launch is None
            and run_id != current["originating_run_id"]
        ):
            raise AgentInstanceStopLine("AGENT_INSTANCE_ORIGINATING_RUN_MISMATCH", run_id)
        active_runs = json_loads(current["active_launch_run_ids_json"], [])
        if not isinstance(active_runs, list):
            raise AgentInstanceStopLine("AGENT_INSTANCE_LAUNCH_STATE_INVALID", identifier)
        if active_runs and run_id not in active_runs and not current["concurrent_attachment"]:
            raise AgentInstanceStopLine("AGENT_INSTANCE_LAUNCH_CONFLICT", identifier)
        run = conn.execute(
            "SELECT instance_id, status FROM agent_instance_launch_runs WHERE run_id=?",
            (run_id,),
        ).fetchone()
        if run is not None and run["instance_id"] != identifier:
            raise AgentInstanceStopLine("AGENT_INSTANCE_RUN_BINDING_COLLISION", run_id)
        if run is not None and run["status"] != "active":
            raise AgentInstanceStopLine("AGENT_INSTANCE_RUN_BINDING_TERMINAL", run_id)
        if run_id not in active_runs:
            active_runs.append(run_id)
        if run is None:
            conn.execute(
                "INSERT INTO agent_instance_launch_runs("
                "run_id, instance_id, status, outcome, bound_event_seq, released_event_seq"
                ") VALUES (?, ?, 'active', NULL, ?, NULL)",
                (run_id, identifier, event_seq),
            )
        conn.execute(
            "UPDATE agent_instances SET active_launch_run_ids_json=?, updated_utc=?, "
            "event_seq=? WHERE id=?",
            (json_dumps(active_runs), record["occurred_utc"], event_seq, identifier),
        )
        return
    if kind == "agent_instance_launch_released":
        _validate_projected_lifecycle_actor(current, record, identifier)
        run_id = str(payload.get("run_id", "")).strip()
        outcome = str(payload.get("outcome", "")).strip()
        if outcome not in INSTANCE_TERMINAL_OUTCOMES:
            raise AgentInstanceStopLine("AGENT_INSTANCE_RELEASE_OUTCOME_INVALID", outcome)
        active_runs = json_loads(current["active_launch_run_ids_json"], [])
        if not isinstance(active_runs, list) or run_id not in active_runs:
            raise AgentInstanceStopLine("AGENT_INSTANCE_LAUNCH_RELEASE_INVALID", identifier)
        launch = conn.execute(
            "SELECT instance_id, status FROM agent_instance_launch_runs WHERE run_id=?",
            (run_id,),
        ).fetchone()
        if launch is None or launch["instance_id"] != identifier or launch["status"] != "active":
            raise AgentInstanceStopLine("AGENT_INSTANCE_LAUNCH_RELEASE_INVALID", identifier)
        _validate_projected_dispatch_suffix(
            conn,
            run_id=run_id,
            outcome=outcome,
            event_seq=event_seq,
        )
        active_runs = [active for active in active_runs if active != run_id]
        conn.execute(
            "UPDATE agent_instance_launch_runs SET status='released', outcome=?, "
            "released_event_seq=? WHERE run_id=?",
            (outcome, event_seq, run_id),
        )
        conn.execute(
            "UPDATE agent_instances SET active_launch_run_ids_json=?, updated_utc=?, "
            "event_seq=? WHERE id=?",
            (json_dumps(active_runs), record["occurred_utc"], event_seq, identifier),
        )
        return
    if kind == "agent_instance_terminal":
        _validate_projected_lifecycle_actor(current, record, identifier)
        active_runs = json_loads(current["active_launch_run_ids_json"], [])
        if active_runs:
            raise AgentInstanceStopLine("AGENT_INSTANCE_TERMINAL_WITH_ACTIVE_LAUNCH", identifier)
        outcome = str(payload.get("outcome", "")).strip()
        if outcome not in INSTANCE_TERMINAL_OUTCOMES:
            raise AgentInstanceStopLine("AGENT_INSTANCE_TERMINAL_INVALID", outcome)
        run_id = str(payload.get("run_id", "")).strip()
        launch = conn.execute(
            "SELECT instance_id, status, outcome FROM agent_instance_launch_runs WHERE run_id=?",
            (run_id,),
        ).fetchone()
        if (
            launch is None
            or launch["instance_id"] != identifier
            or launch["status"] != "released"
            or launch["outcome"] != outcome
        ):
            raise AgentInstanceStopLine("AGENT_INSTANCE_TERMINAL_SUFFIX_INVALID", identifier)
        _validate_projected_dispatch_suffix(
            conn,
            run_id=run_id,
            outcome=outcome,
            event_seq=event_seq,
        )
        conn.execute(
            "UPDATE agent_instances SET status='terminal', terminal_outcome=?, "
            "terminal_utc=?, updated_utc=?, event_seq=? WHERE id=?",
            (
                outcome,
                record["occurred_utc"],
                record["occurred_utc"],
                event_seq,
                identifier,
            ),
        )
        return
    if kind == "agent_instance_retired":
        if not str(payload.get("reason", "")).strip():
            raise AgentInstanceStopLine("AGENT_INSTANCE_RETIRE_INVALID", "reason must be non-empty")
        conn.execute(
            "UPDATE agent_instances SET status='retired', retired_utc=?, updated_utc=?, "
            "event_seq=? WHERE id=?",
            (record["occurred_utc"], record["occurred_utc"], event_seq, identifier),
        )
        return

    if not str(payload.get("reason", "")).strip():
        raise AgentInstanceStopLine("AGENT_INSTANCE_UPDATE_INVALID", "reason must be non-empty")
    fields = payload.get("fields_changed", {})
    if not isinstance(fields, dict) or not fields:
        raise AgentInstanceStopLine(
            "AGENT_INSTANCE_UPDATE_INVALID", "fields_changed must be non-empty"
        )
    unsupported = set(fields) - set(MUTABLE_INSTANCE_FIELDS)
    if unsupported:
        raise AgentInstanceStopLine(
            "AGENT_INSTANCE_UPDATE_INVALID",
            "immutable or unknown fields: " + ", ".join(sorted(unsupported)),
        )
    updates: dict[str, str] = {}
    for field_name in MUTABLE_INSTANCE_FIELDS:
        if field_name not in fields:
            continue
        old_new = fields[field_name]
        if not isinstance(old_new, list) or len(old_new) != 2:
            raise AgentInstanceStopLine("AGENT_INSTANCE_UPDATE_INVALID", field_name)
        old_value = str(old_new[0]).strip()
        current_value = str(current[field_name] or "").strip()
        if old_value != current_value:
            raise AgentInstanceStopLine(
                "AGENT_INSTANCE_UPDATE_INVALID",
                f"{field_name} old value does not match current state",
            )
        value = str(old_new[1]).strip()
        if field_name == "label":
            try:
                value = normalize_new_instance_handle(
                    value, participant=str(current["participant"])
                )
            except ValueError as exc:
                raise AgentInstanceStopLine("AGENT_INSTANCE_LABEL_INVALID", str(exc)) from exc
            collision = conn.execute(
                "SELECT instance_id FROM agent_instance_aliases WHERE label=?", (value,)
            ).fetchone()
            if collision is not None and collision["instance_id"] != identifier:
                raise AgentInstanceStopLine("AGENT_INSTANCE_LABEL_COLLISION", value)
            conn.execute(
                "UPDATE agent_instance_aliases SET is_primary=0 WHERE instance_id=?",
                (identifier,),
            )
            conn.execute(
                "INSERT INTO agent_instance_aliases(label, instance_id, is_primary, event_seq) "
                "VALUES (?, ?, 1, ?) ON CONFLICT(label) DO UPDATE SET "
                "is_primary=1, event_seq=excluded.event_seq",
                (value, identifier, event_seq),
            )
        elif field_name == "external_session_ref_digest":
            try:
                value = validate_external_session_ref_digest(value)
            except ValueError as exc:
                raise AgentInstanceStopLine("AGENT_INSTANCE_METADATA_INVALID", str(exc)) from exc
        if value == current_value:
            raise AgentInstanceStopLine(
                "AGENT_INSTANCE_UPDATE_INVALID", f"{field_name} does not change"
            )
        updates[field_name] = value
    if updates:
        assignments = ", ".join(f"{name}=?" for name in updates)
        conn.execute(
            f"UPDATE agent_instances SET {assignments}, updated_utc=?, event_seq=? WHERE id=?",
            (*updates.values(), record["occurred_utc"], event_seq, identifier),
        )


def _validate_projected_lifecycle_actor(current, record: dict[str, Any], identifier: str) -> None:
    actor = str(record.get("actor", "")).strip()
    actor_instance_id = str(record.get("actor_instance_id", "")).strip()
    if current["registration_origin"] == "dispatch":
        if actor != current["registrar"] or actor_instance_id != current["parent_instance_id"]:
            raise AgentInstanceStopLine("AGENT_INSTANCE_LAUNCH_PARENT_MISMATCH", identifier)
        return
    if current["registration_origin"] == "integration" and (
        actor != current["registrar"] or actor_instance_id
    ):
        raise AgentInstanceStopLine("AGENT_INSTANCE_INTEGRATION_LIFECYCLE_MISMATCH", identifier)


def _validate_projected_dispatch_suffix(
    conn,
    *,
    run_id: str,
    outcome: str,
    event_seq: int,
) -> None:
    run = conn.execute(
        "SELECT status, error_class, event_seq FROM dispatch_runs WHERE run_id=?",
        (run_id,),
    ).fetchone()
    run_status = str(run["status"]) if run is not None else ""
    error_class = str(run["error_class"] or "") if run is not None else ""
    if outcome == "completed":
        outcome_matches_run = run_status == "completed" and not error_class
        expected_release_reason = "completed"
    elif outcome == "timeout":
        outcome_matches_run = run_status == "timed_out" and error_class == "Timeout"
        expected_release_reason = "timeout"
    elif outcome == "cancelled":
        outcome_matches_run = run_status == "cancelled" and error_class == "Cancelled"
        expected_release_reason = "cancelled"
    elif outcome == "parent_loss":
        outcome_matches_run = run_status == "parent_lost" and error_class == "ParentLoss"
        expected_release_reason = "parent_loss"
    elif outcome == "launch_error":
        outcome_matches_run = run_status == "failed" and error_class == "LaunchError"
        expected_release_reason = "failed"
    elif outcome == "failed":
        outcome_matches_run = (
            run_status == "failed"
            and bool(error_class)
            and error_class not in {"Timeout", "LaunchError", "Cancelled", "ParentLoss"}
        )
        expected_release_reason = "failed"
    else:
        # Dispatch recovery preserves the observed run outcome. ``recovered`` remains a valid
        # integration lifecycle value, but it cannot replace a dispatch run's terminal fact.
        outcome_matches_run = False
        expected_release_reason = "failed"
    lease = conn.execute(
        "SELECT status, reason, event_seq FROM dispatch_leases WHERE run_id=? "
        "ORDER BY event_seq DESC LIMIT 1",
        (run_id,),
    ).fetchone()
    if (
        run is None
        or not outcome_matches_run
        or int(run["event_seq"]) >= event_seq
        or lease is None
        or str(lease["status"]) != "released"
        or str(lease["reason"]) != expected_release_reason
        or int(lease["event_seq"]) <= int(run["event_seq"])
        or int(lease["event_seq"]) >= event_seq
    ):
        raise AgentInstanceStopLine(
            "AGENT_INSTANCE_DISPATCH_SUFFIX_INVALID",
            run_id,
        )


def _project_instance_last_seen(conn, record: dict[str, Any]) -> None:
    instance_id = str(record.get("actor_instance_id", "")).strip()
    if not instance_id:
        return
    conn.execute(
        "UPDATE agent_instances SET last_seen_utc=?, last_seen_event_seq=? WHERE id=?",
        (record["occurred_utc"], record["event_seq"], instance_id),
    )


def _validate_instance_recipients(conn, recipients: list[str], instance_ids: list[Any]) -> None:
    recipient_set = {str(value) for value in recipients}
    seen: set[str] = set()
    for raw_id in instance_ids:
        instance_id = str(raw_id)
        if instance_id in seen:
            raise AgentInstanceStopLine("AGENT_INSTANCE_ADDRESS_DUPLICATE", instance_id)
        seen.add(instance_id)
        row = conn.execute(
            "SELECT participant, status FROM agent_instances WHERE id=?", (instance_id,)
        ).fetchone()
        if row is None:
            raise AgentInstanceStopLine("AGENT_INSTANCE_UNKNOWN", instance_id)
        if row["status"] == "terminal":
            raise AgentInstanceStopLine("AGENT_INSTANCE_TERMINAL", instance_id)
        if row["status"] != "active":
            raise AgentInstanceStopLine("AGENT_INSTANCE_RETIRED", instance_id)
        if row["participant"] not in recipient_set:
            raise AgentInstanceStopLine(
                "AGENT_INSTANCE_RECIPIENT_MISMATCH",
                f"{instance_id} belongs to {row['participant']!r}",
            )


def _validate_instance_response(conn, record: dict[str, Any], request_id: str) -> None:
    request = conn.execute(
        "SELECT recipient_instance_ids_json FROM messages WHERE id=? AND kind='request'",
        (request_id,),
    ).fetchone()
    if request is None:
        return
    target_ids = json_loads(request["recipient_instance_ids_json"], [])
    if not isinstance(target_ids, list) or not target_ids:
        return
    placeholders = ",".join("?" for _ in target_ids)
    rows = conn.execute(
        f"SELECT id, participant FROM agent_instances WHERE id IN ({placeholders})",
        tuple(str(value) for value in target_ids),
    ).fetchall()
    actor = str(record.get("actor", ""))
    addressed_participants = {str(row["participant"]) for row in rows}
    if actor not in addressed_participants:
        return
    actor_instance_id = str(record.get("actor_instance_id", ""))
    if actor_instance_id not in {str(value) for value in target_ids}:
        raise AgentInstanceStopLine(
            "AGENT_INSTANCE_NOT_ADDRESSED",
            f"{actor_instance_id or actor} is not addressed by {request_id}",
        )


def _project_req_created(conn, record: dict[str, Any], config: AgentMeshConfig) -> None:
    payload = record["payload"]
    body = str(payload.get("body", ""))
    recipients = payload.get("to", [])
    if isinstance(recipients, str):
        recipients = config.canonical_recipients(recipients)
    recipient_instance_ids = payload.get("to_instances", [])
    if not isinstance(recipient_instance_ids, list):
        raise AgentInstanceStopLine("AGENT_INSTANCE_ADDRESS_INVALID", "to_instances must be a list")
    _validate_instance_recipients(conn, recipients, recipient_instance_ids)
    meta = {"body": body}
    if "original_to" in payload:
        meta["original_to"] = payload["original_to"]
    meta["response_mode"] = str(payload.get("response_mode") or "single")
    _add_provenance_to_meta(meta, payload)
    workflow_origin = project_workflow_origin(payload)
    _insert_message(
        conn,
        record=record,
        kind="request",
        request_id=None,
        parent_id=None,
        sender=str(record.get("actor", "")),
        sender_instance_id=str(record.get("actor_instance_id", "")),
        recipients=recipients,
        recipient_instance_ids=[str(value) for value in recipient_instance_ids],
        feature_id=str(payload.get("feature", "")),
        workflow_origin=workflow_origin,
        title=str(payload.get("title", "")),
        summary=None,
        body=body,
        body_authority=body_authority_for_payload(payload),
        body_fidelity=body_fidelity_for_payload(payload),
        status="open",
        meta=meta,
    )
    _insert_refs(conn, record["entity_id"], payload.get("refs", []))
    _project_message_provenance(conn, record, synthetic_edges=[])


def _project_res_posted(conn, record: dict[str, Any]) -> None:
    payload = record["payload"]
    body = str(payload.get("body", ""))
    request_id = str(payload.get("request_id") or record["thread_id"])
    parent_id = str(payload.get("parent_id") or request_id)
    _validate_response_parent(conn, record, request_id, parent_id)
    _validate_instance_response(conn, record, request_id)
    _validate_dispatch_response(conn, record, request_id)
    meta = {"body": body}
    if parent_id:
        meta["parent_id"] = parent_id
    if payload.get("parent_kind"):
        meta["parent_kind"] = payload.get("parent_kind")
    if isinstance(payload.get("authorship_policy"), dict):
        meta["authorship_policy"] = payload["authorship_policy"]
    _add_provenance_to_meta(meta, payload)
    request = conn.execute(
        "SELECT workflow_origin, workflow_origin_valid FROM messages WHERE id=?",
        (request_id,),
    ).fetchone()
    inherited_origin = None
    if request is not None and request["workflow_origin"]:
        inherited_origin = ProjectedWorkflowOrigin(
            str(request["workflow_origin"]),
            bool(request["workflow_origin_valid"]),
            "inherited",
        )
    workflow_origin = project_workflow_origin(payload, inherited=inherited_origin)
    _insert_message(
        conn,
        record=record,
        kind="response",
        request_id=request_id,
        parent_id=parent_id,
        sender=str(record.get("actor", "")),
        sender_instance_id=str(record.get("actor_instance_id", "")),
        recipients=[],
        recipient_instance_ids=[],
        feature_id="",
        workflow_origin=workflow_origin,
        title=None,
        summary=str(payload.get("summary", "")),
        body=body,
        body_authority=body_authority_for_payload(payload),
        body_fidelity=body_fidelity_for_payload(payload),
        status="posted",
        meta=meta,
    )
    _insert_refs(conn, record["entity_id"], payload.get("refs", []))
    _project_message_provenance(
        conn,
        record,
        synthetic_edges=[
            {
                "relation": "replied_to",
                "from_ref": record["entity_id"],
                "to_ref": parent_id,
                "synthetic": True,
                "source": "res_posted.parent_id",
            }
        ],
    )


def _validate_dispatch_response(
    conn,
    record: dict[str, Any],
    request_id: str,
) -> None:
    payload = record.get("payload", {})
    refs = payload.get("source_context_refs", []) if isinstance(payload, dict) else []
    run_ids = {
        str(ref.get("source_event_id", "")).strip()
        for ref in refs
        if isinstance(ref, dict) and str(ref.get("channel", "")).strip() == "agent-mesh-dispatch"
    }
    if not run_ids:
        return
    if len(run_ids) != 1 or not next(iter(run_ids)):
        raise DispatchStopLine(
            DISPATCH_RESPONSE_RUN_INVALID,
            f"{record['entity_id']} must reference exactly one dispatch run",
        )
    run_id = next(iter(run_ids))
    run = conn.execute(
        "SELECT status, input_message_id FROM dispatch_runs WHERE run_id=?",
        (run_id,),
    ).fetchone()
    launch = conn.execute(
        "SELECT instance_id, status, bound_event_seq, released_event_seq "
        "FROM agent_instance_launch_runs WHERE run_id=?",
        (run_id,),
    ).fetchone()
    lease = _open_lease_for_run(conn, run_id)
    if (
        run is None
        or str(run["status"]) != "started"
        or str(run["input_message_id"]) != request_id
        or lease is None
    ):
        raise DispatchStopLine(
            DISPATCH_RESPONSE_RUN_INVALID,
            f"{record['entity_id']} does not reference an active dispatch for its request",
        )
    if launch is None:
        if str(record.get("actor_instance_id", "")):
            raise DispatchStopLine(
                DISPATCH_RESPONSE_INSTANCE_MISMATCH,
                f"{run_id} has no bound child instance",
            )
        return
    if str(record.get("actor_instance_id", "")) != str(launch["instance_id"]):
        raise DispatchStopLine(
            DISPATCH_RESPONSE_INSTANCE_MISMATCH,
            f"{run_id} response was not authored by its bound child instance",
        )
    if (
        str(launch["status"]) != "active"
        or launch["released_event_seq"] is not None
        or int(launch["bound_event_seq"]) >= int(record["event_seq"])
    ):
        raise DispatchStopLine(
            DISPATCH_RESPONSE_ORDER_INVALID,
            f"{run_id} requires bound child < response < completion",
        )


def _validate_response_parent(
    conn, record: dict[str, Any], request_id: str, parent_id: str
) -> None:
    request = conn.execute(
        "SELECT kind, thread_id FROM messages WHERE id=?", (request_id,)
    ).fetchone()
    if request is None or request["kind"] != "request":
        raise RuntimeError(f"RES_REQUEST_MISSING: {record['entity_id']} request_id={request_id}")
    if str(record["thread_id"]) != request_id:
        raise RuntimeError(
            f"RES_THREAD_MISMATCH: {record['entity_id']} thread_id={record['thread_id']} request_id={request_id}"
        )
    parent = conn.execute(
        "SELECT kind, thread_id, request_id FROM messages WHERE id=?", (parent_id,)
    ).fetchone()
    if parent is None:
        raise RuntimeError(f"RES_PARENT_MISSING: {record['entity_id']} parent_id={parent_id}")
    declared_parent_kind = record.get("payload", {}).get("parent_kind")
    if declared_parent_kind is not None and str(declared_parent_kind) != str(parent["kind"]):
        raise RuntimeError(
            f"RES_PARENT_KIND_MISMATCH: {record['entity_id']} parent_id={parent_id} "
            f"declared={declared_parent_kind} actual={parent['kind']}"
        )
    if parent["kind"] == "request":
        if parent_id != request_id:
            raise RuntimeError(
                f"RES_PARENT_REQUEST_MISMATCH: {record['entity_id']} parent_id={parent_id} request_id={request_id}"
            )
        return
    if parent["kind"] == "response":
        if str(parent["thread_id"]) != request_id or str(parent["request_id"]) != request_id:
            raise RuntimeError(
                f"RES_PARENT_THREAD_MISMATCH: {record['entity_id']} parent_id={parent_id} request_id={request_id}"
            )
        return
    raise RuntimeError(f"RES_PARENT_INVALID_KIND: {record['entity_id']} parent_id={parent_id}")


def _project_req_status_changed(conn, record: dict[str, Any]) -> None:
    payload = record["payload"]
    to_status = str(payload.get("to_status", "open"))
    conn.execute(
        "UPDATE messages SET status=?, resolution=?, resolved_utc=?, updated_utc=?, event_seq=? "
        "WHERE id=? AND kind='request'",
        (
            to_status,
            payload.get("reason"),
            record["occurred_utc"] if to_status == "closed" else None,
            record["occurred_utc"],
            record["event_seq"],
            record["entity_id"],
        ),
    )


def _insert_message(
    conn,
    *,
    record: dict[str, Any],
    kind: str,
    request_id: str | None,
    parent_id: str | None,
    sender: str,
    sender_instance_id: str,
    recipients: list[str],
    recipient_instance_ids: list[str],
    feature_id: str,
    workflow_origin: ProjectedWorkflowOrigin,
    title: str | None,
    summary: str | None,
    body: str,
    body_authority: str,
    body_fidelity: str | None,
    status: str,
    meta: dict[str, Any],
) -> None:
    from agent_mesh.core.assurance import (
        REVIEW_BEGIN,
        ReviewResponseError,
        parse_review_response,
    )

    body_bytes = body.encode("utf-8")
    body_sha = hashlib.sha256(body_bytes).hexdigest()
    review_envelope_json = None
    if REVIEW_BEGIN in body:
        try:
            envelope = parse_review_response(body)
        except ReviewResponseError:
            pass
        else:
            projected_envelope = {
                "policy_id": envelope.policy_id,
                "policy_digest": envelope.policy_digest,
                "subject_digest": envelope.subject_digest,
                "disposition": envelope.disposition,
                "finding_counts": envelope.finding_counts,
                "artifact_paths": list(envelope.artifact_paths),
            }
            if envelope.replaces_response_id:
                projected_envelope["replaces_response_id"] = envelope.replaces_response_id
            review_envelope_json = json_dumps(projected_envelope)
    conn.execute(
        """
        INSERT INTO messages(
          id, kind, schema_version, thread_id, request_id, parent_id, sender,
          sender_instance_id, recipients_json, recipient_instance_ids_json, feature_id,
          workflow_origin, workflow_origin_valid, workflow_origin_source, title, summary,
          body_preview, body_sha, body_path,
          body_bytes, body_media_type, body_authority, body_fidelity,
          review_envelope_json, status, resolution,
          resolved_utc, claimed_by,
          claimed_utc, has_fenced_json, json_packet_type, created_utc, updated_utc,
          event_seq, source_file, source_line_start, source_line_end, import_batch_id, meta_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?,
          'text/markdown', ?, ?, ?, ?, NULL, NULL, NULL, NULL, ?, ?, ?, ?, ?, NULL, NULL,
          NULL, NULL, ?)
        ON CONFLICT(id) DO UPDATE SET
          updated_utc=excluded.updated_utc,
          event_seq=excluded.event_seq,
          sender_instance_id=excluded.sender_instance_id,
          recipient_instance_ids_json=excluded.recipient_instance_ids_json,
          body_authority=excluded.body_authority,
          body_fidelity=excluded.body_fidelity,
          workflow_origin=excluded.workflow_origin,
          workflow_origin_valid=excluded.workflow_origin_valid,
          workflow_origin_source=excluded.workflow_origin_source,
          meta_json=excluded.meta_json
        """,
        (
            record["entity_id"],
            kind,
            int(record.get("schema_version", 1)),
            record["thread_id"],
            request_id,
            parent_id,
            sender,
            sender_instance_id or None,
            json_dumps(recipients),
            json_dumps(recipient_instance_ids),
            feature_id,
            workflow_origin.value,
            workflow_origin.valid,
            workflow_origin.source,
            title,
            summary,
            body[:500],
            body_sha,
            len(body_bytes),
            body_authority,
            body_fidelity,
            review_envelope_json,
            status,
            1 if "```json" in body else 0,
            _packet_type(body),
            record["occurred_utc"],
            record["occurred_utc"],
            record["event_seq"],
            json_dumps(meta),
        ),
    )


def _insert_refs(conn, message_id: str, refs: list[Any]) -> None:
    for ref in refs:
        if isinstance(ref, dict):
            ref_type = str(ref.get("type", "unknown"))
            ref_value = str(ref.get("value", ""))
        else:
            ref_value = str(ref)
            ref_type = _infer_ref_type(ref_value)
            if ref_type == "origin":
                ref_value = ref_value.partition(":")[2].strip()
        if ref_value:
            conn.execute(
                "INSERT OR IGNORE INTO message_refs(message_id, ref_type, ref_value) VALUES (?, ?, ?)",
                (message_id, ref_type, ref_value),
            )


def _add_provenance_to_meta(meta: dict[str, Any], payload: dict[str, Any]) -> None:
    for key in (
        "source_context_refs",
        "causal_edges",
        "body_authority",
        "body_fidelity",
        "source_context_status",
        "source_selection",
    ):
        if key in payload:
            meta[key] = payload[key]


def _project_message_provenance(
    conn,
    record: dict[str, Any],
    *,
    synthetic_edges: list[dict[str, Any]],
) -> None:
    payload = record["payload"]
    message_id = str(record["entity_id"])
    event_seq = int(record["event_seq"])
    conn.execute("DELETE FROM message_source_context_refs WHERE message_id=?", (message_id,))
    conn.execute("DELETE FROM message_causal_edges WHERE message_id=?", (message_id,))
    conn.execute("DELETE FROM message_source_selection WHERE message_id=?", (message_id,))

    for index, item in enumerate(payload.get("source_context_refs") or []):
        conn.execute(
            """
            INSERT INTO message_source_context_refs(
              message_id, ref_index, channel, source_kind, source_id, source_event_id,
              source_uri, role, observed_utc, confidence, event_seq, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                message_id,
                index,
                _string_or_none(item.get("channel")),
                _string_or_none(item.get("source_kind") or item.get("source_type")),
                _first_string(item, "source_id", "turn_id", "message_id", "id"),
                _first_string(item, "source_event_id", "event_id"),
                _string_or_none(item.get("source_uri")),
                _string_or_none(item.get("role")),
                _string_or_none(item.get("observed_utc")),
                confidence_value(item.get("confidence")),
                event_seq,
                json_dumps(
                    _extra_fields(
                        item,
                        {
                            "channel",
                            "source_kind",
                            "source_type",
                            "source_id",
                            "turn_id",
                            "message_id",
                            "id",
                            "source_event_id",
                            "event_id",
                            "source_uri",
                            "role",
                            "observed_utc",
                            "confidence",
                        },
                    )
                ),
            ),
        )

    explicit_edges = [_normalized_edge(item, record) for item in payload.get("causal_edges") or []]
    seen = {(edge["relation"], edge.get("from_ref"), edge.get("to_ref")) for edge in explicit_edges}
    edges = list(explicit_edges)
    for item in synthetic_edges:
        edge = _normalized_edge(item, record)
        key = (edge["relation"], edge.get("from_ref"), edge.get("to_ref"))
        if key not in seen:
            edges.append(edge)
            seen.add(key)

    for index, edge in enumerate(edges):
        conn.execute(
            """
            INSERT INTO message_causal_edges(
              message_id, edge_index, relation, from_ref, to_ref, confidence, event_seq, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                message_id,
                index,
                str(edge["relation"]),
                _string_or_none(edge.get("from_ref")),
                _string_or_none(edge.get("to_ref")),
                confidence_value(edge.get("confidence")),
                event_seq,
                json_dumps(
                    _extra_fields(
                        edge,
                        {"relation", "from_ref", "from_id", "to_ref", "to_id", "confidence"},
                    )
                ),
            ),
        )

    source_selection = payload.get("source_selection")
    if isinstance(source_selection, dict):
        conn.execute(
            """
            INSERT INTO message_source_selection(
              message_id, mode, confidence, selected_by, requires_review, event_seq, meta_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                message_id,
                str(source_selection["mode"]),
                float(source_selection["confidence"]),
                str(source_selection["selected_by"]),
                1 if source_selection["requires_review"] else 0,
                event_seq,
                json_dumps(
                    _extra_fields(
                        source_selection,
                        {"mode", "confidence", "selected_by", "requires_review"},
                    )
                ),
            ),
        )


def _normalized_edge(item: dict[str, Any], record: dict[str, Any]) -> dict[str, Any]:
    edge = dict(item)
    if "from_ref" not in edge:
        edge["from_ref"] = edge.get("from_id") or edge.get("source_ref") or edge.get("source_id")
    if "to_ref" not in edge:
        edge["to_ref"] = edge.get("to_id") or edge.get("target_ref") or record["entity_id"]
    return edge


def _first_string(item: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = _string_or_none(item.get(key))
        if value:
            return value
    return None


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    return str(value)


def _extra_fields(item: dict[str, Any], known: set[str]) -> dict[str, Any]:
    return {key: value for key, value in item.items() if key not in known}


def _project_backlog_item_upserted(conn, record: dict[str, Any], config: AgentMeshConfig) -> None:
    payload = record["payload"]
    item_id = str(payload.get("id") or record["entity_id"])
    write_intent = str(payload.get("write_intent") or "upsert")
    validate_backlog_write(conn, item_id, write_intent)
    current = conn.execute("SELECT status FROM backlog_items WHERE id=?", (item_id,)).fetchone()
    previous_status = str(current["status"] or "") if current is not None else "none"
    next_status = str(payload.get("status") or previous_status)
    binding = payload.get("review_gate")
    if binding is not None:
        from agent_mesh.core.assurance import (
            AssuranceResolutionError,
            validate_assurance_gate_binding,
        )

        try:
            validate_assurance_gate_binding(
                config,
                conn,
                binding,
                boundary_kind="backlog_transition",
                boundary_key=f"{previous_status}->{next_status}",
            )
        except AssuranceResolutionError as exc:
            raise DispatchStopLine(str(exc), item_id) from exc
    refs = payload.get("refs", [])
    if not isinstance(refs, list):
        refs = []
    workflow_origin = project_workflow_origin(payload)
    raw_owner_instance_id = payload.get("owner_instance_id")
    owner_instance_id = (
        str(raw_owner_instance_id).strip() if raw_owner_instance_id is not None else ""
    )
    if owner_instance_id:
        owner_instance = conn.execute(
            "SELECT status FROM agent_instances WHERE id=?", (owner_instance_id,)
        ).fetchone()
        if owner_instance is None:
            raise AgentInstanceStopLine("AGENT_INSTANCE_UNKNOWN", owner_instance_id)
        if owner_instance["status"] == "terminal":
            raise AgentInstanceStopLine("AGENT_INSTANCE_TERMINAL", owner_instance_id)
        if owner_instance["status"] != "active":
            raise AgentInstanceStopLine("AGENT_INSTANCE_RETIRED", owner_instance_id)
    meta = {
        key: value
        for key, value in payload.items()
        if key
        not in {
            "id",
            "title",
            "item_type",
            "summary",
            "root_cause_summary",
            "architectural_category",
            "status",
            "priority",
            "launch_scope",
            "release_phase",
            "production_state",
            "disposition",
            "owner_hint",
            "owner_instance_id",
            "lane",
            "notes",
            "workflow_origin",
            "refs",
            "review_gate",
        }
    }
    conn.execute(
        """
        INSERT INTO backlog_items(
          id, title, item_type, summary, root_cause_summary, architectural_category,
          status, priority, launch_scope, release_phase, production_state, disposition,
          owner_hint, owner_instance_id, lane, notes, workflow_origin, workflow_origin_valid,
          workflow_origin_source, refs_json, created_utc, updated_utc, event_seq, meta_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
          title=excluded.title,
          item_type=excluded.item_type,
          summary=excluded.summary,
          root_cause_summary=excluded.root_cause_summary,
          architectural_category=excluded.architectural_category,
          status=excluded.status,
          priority=excluded.priority,
          launch_scope=excluded.launch_scope,
          release_phase=excluded.release_phase,
          production_state=excluded.production_state,
          disposition=excluded.disposition,
          owner_hint=excluded.owner_hint,
          owner_instance_id=excluded.owner_instance_id,
          lane=excluded.lane,
          notes=excluded.notes,
          workflow_origin=excluded.workflow_origin,
          workflow_origin_valid=excluded.workflow_origin_valid,
          workflow_origin_source=excluded.workflow_origin_source,
          refs_json=excluded.refs_json,
          updated_utc=excluded.updated_utc,
          event_seq=excluded.event_seq,
          meta_json=excluded.meta_json
        """,
        (
            item_id,
            str(payload.get("title", "")),
            payload.get("item_type"),
            payload.get("summary"),
            payload.get("root_cause_summary"),
            payload.get("architectural_category"),
            str(payload.get("status", "open")),
            payload.get("priority"),
            payload.get("launch_scope"),
            payload.get("release_phase"),
            payload.get("production_state"),
            payload.get("disposition"),
            payload.get("owner_hint"),
            owner_instance_id or None,
            payload.get("lane"),
            payload.get("notes"),
            workflow_origin.value,
            workflow_origin.valid,
            workflow_origin.source,
            json_dumps(refs),
            record["occurred_utc"],
            record["occurred_utc"],
            record["event_seq"],
            json_dumps(meta),
        ),
    )


def _project_backlog_link_added(conn, record: dict[str, Any]) -> None:
    payload = record["payload"]
    item_id = str(payload.get("item_id") or record["entity_id"])
    conn.execute(
        """
        INSERT OR REPLACE INTO backlog_item_links(link_event_id, item_id, ref_type, ref_value, created_utc, event_seq)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            record["event_id"],
            item_id,
            str(payload.get("ref_type", "unknown")),
            str(payload.get("ref_value", "")),
            record["occurred_utc"],
            record["event_seq"],
        ),
    )


def _project_backlog_event_recorded(conn, record: dict[str, Any]) -> None:
    payload = record["payload"]
    item_id = str(payload.get("item_id") or record["entity_id"])
    details = payload.get("details", {})
    if not isinstance(details, dict):
        details = {"value": details}
    conn.execute(
        """
        INSERT OR REPLACE INTO backlog_events(
          event_id, item_id, event_type, actor, actor_instance_id, created_utc,
          details_json, event_seq
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            record["event_id"],
            item_id,
            str(payload.get("event_type", record["kind"])),
            str(record.get("actor", "")),
            str(record.get("actor_instance_id", "")) or None,
            record["occurred_utc"],
            json_dumps(details),
            record["event_seq"],
        ),
    )


def _project_dispatch_event(conn, record: dict[str, Any], config: AgentMeshConfig) -> None:
    """Project one dispatch-domain event, folding lifecycle state into dispatch_runs/dispatch_leases.

    Re-enforces the strict allow-list at replay (a hand-edited or imported log must not smuggle a
    body in), then applies the per-kind projection + replay-time STOP-LINE checks (see
    ``docs/domains/dispatch.md`` §6). Raises ``DispatchStopLine`` on any violation; the surrounding
    ``apply_record`` transaction rolls back so no partial state is committed.
    """
    kind = str(record["kind"])
    payload = record.get("payload", {}) or {}
    try:
        validate_dispatch_payload(kind, payload)
    except DispatchSchemaError as exc:
        raise DispatchStopLine(exc.code, exc.detail) from exc
    event_seq = int(record["event_seq"])
    if kind == "dispatch_policy_frozen":
        _project_dispatch_policy_frozen(conn, record, payload, event_seq)
    elif kind == "review_assurance_recorded":
        _project_review_assurance_recorded(conn, record, payload, event_seq, config)
    elif kind == "review_assurance_flagged":
        _project_review_assurance_lifecycle(conn, record, payload, event_seq, state="flagged")
    elif kind == "review_assurance_retired":
        _project_review_assurance_lifecycle(conn, record, payload, event_seq, state="retired")
    elif kind == "dispatch_run_planned":
        _project_dispatch_run_planned(conn, record, payload, event_seq)
    elif kind == "dispatch_run_blocked":
        _project_dispatch_run_blocked(conn, record, payload, event_seq)
    elif kind == "dispatch_lease_acquired":
        _project_dispatch_lease_acquired(conn, payload, event_seq)
    elif kind == "dispatch_lease_released":
        _project_dispatch_lease_released(conn, payload, event_seq)
    elif kind == "dispatch_run_started":
        _project_dispatch_run_started(conn, payload, event_seq)
    elif kind in ("dispatch_run_completed", "dispatch_run_failed"):
        _project_dispatch_run_terminal(conn, kind, payload, event_seq)
    elif kind == "dispatch_run_terminated":
        _project_dispatch_run_terminated(conn, payload, event_seq)
    elif kind == "dispatch_retry_exhausted":
        _project_dispatch_retry_exhausted(conn, record, payload, event_seq)


def _require_known_request(conn, input_message_id: str, run_id: str) -> None:
    row = conn.execute(
        "SELECT 1 FROM messages WHERE id=? AND kind='request'", (input_message_id,)
    ).fetchone()
    if row is None:
        raise DispatchStopLine(
            DISPATCH_RUN_UNKNOWN_MESSAGE, f"{run_id} input_message_id={input_message_id}"
        )


def _project_dispatch_policy_frozen(conn, record, payload, event_seq: int) -> None:
    """Project an immutable dispatch.v1 policy after validating its response slot."""

    policy_id = str(payload["policy_id"])
    request_id = str(payload["request_id"])
    if str(record.get("entity_id") or "") != policy_id:
        raise DispatchStopLine(
            "DISPATCH_POLICY_IDENTITY_MISMATCH",
            f"event entity does not match policy {policy_id}",
        )
    request = conn.execute(
        "SELECT thread_id, recipients_json, recipient_instance_ids_json, meta_json "
        "FROM messages WHERE id=? AND kind='request'",
        (request_id,),
    ).fetchone()
    if request is None:
        raise DispatchStopLine("DISPATCH_POLICY_UNKNOWN_REQUEST", request_id)
    if str(record.get("thread_id") or "") != str(request["thread_id"]):
        raise DispatchStopLine(
            "DISPATCH_POLICY_THREAD_MISMATCH", f"policy={policy_id} request={request_id}"
        )

    slot = payload["response_slot"]
    slot_kind = str(slot["kind"])
    slot_key = str(slot["key"])
    meta = json_loads(request["meta_json"], {})
    response_mode = (
        str(meta.get("response_mode") or "single") if isinstance(meta, dict) else "single"
    )
    recipients = {str(item) for item in json_loads(request["recipients_json"], [])}
    recipient_instances = {
        str(item) for item in json_loads(request["recipient_instance_ids_json"], [])
    }
    target = payload["target"]
    valid_slot = False
    if response_mode == "single":
        valid_slot = slot_kind == "single" and slot_key == request_id
    elif response_mode == "multi" and slot_kind == "participant":
        valid_slot = slot_key in recipients and slot_key == str(target["participant"])
    elif response_mode == "multi" and slot_kind == "instance":
        valid_slot = slot_key in recipient_instances and slot_key == str(
            target.get("instance_id") or ""
        )
    if not valid_slot:
        raise DispatchStopLine(
            "DISPATCH_POLICY_RESPONSE_SLOT_INVALID",
            f"request={request_id} mode={response_mode} slot={slot_kind}",
        )

    try:
        conn.execute(
            """
            INSERT INTO dispatch_policies(
              policy_id, request_id, contract_version, policy_digest, purpose, role,
              required_capabilities_json, permission_ceiling, response_contract_json,
              artifact_contract_json, provenance_requirements_json, retry_json,
              response_slot_kind, response_slot_key, target_json,
              runtime_profile_revision_json, management_level,
              subject_json, assurance_policy_json, frozen_utc, event_seq
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                policy_id,
                request_id,
                payload["contract_version"],
                payload["policy_digest"],
                payload["purpose"],
                payload["role"],
                json_dumps(payload["required_capabilities"]),
                payload["permission_ceiling"],
                json_dumps(payload["response_contract"]),
                json_dumps(payload["artifact_contract"]),
                json_dumps(payload["provenance_requirements"]),
                json_dumps(payload["retry"]),
                slot_kind,
                slot_key,
                json_dumps(target),
                json_dumps(payload["runtime_profile_revision"]),
                payload["management_level"],
                json_dumps(payload["subject"]) if "subject" in payload else None,
                (
                    json_dumps(payload["assurance_policy"])
                    if "assurance_policy" in payload
                    else None
                ),
                payload["frozen_utc"],
                event_seq,
            ),
        )
    except sqlite3.IntegrityError as exc:
        raise DispatchStopLine(
            "DISPATCH_POLICY_DUPLICATE",
            f"request={request_id} slot={slot_kind}:{slot_key}",
        ) from exc


def _project_review_assurance_recorded(
    conn,
    record,
    payload,
    event_seq: int,
    config: AgentMeshConfig,
) -> None:
    """Project review.v1 evidence only when its frozen subject and outcome bind."""

    assurance_id = str(payload["assurance_id"])
    if str(record.get("entity_id") or "") != assurance_id:
        raise DispatchStopLine("REVIEW_ASSURANCE_IDENTITY_MISMATCH", assurance_id)
    policy_id = str(payload.get("policy_id") or "")
    policy = (
        conn.execute("SELECT * FROM dispatch_policies WHERE policy_id=?", (policy_id,)).fetchone()
        if policy_id
        else None
    )
    bound = bool(payload["bound"])
    authoritative = bool(payload["authoritative"])
    supersedes_assurance_id = str(payload.get("supersedes_assurance_id") or "")
    supersedes_expected_version = payload.get("supersedes_expected_version")
    superseded_lifecycle = None
    if bound and policy is None:
        raise DispatchStopLine("REVIEW_ASSURANCE_POLICY_MISSING", policy_id)
    if bound:
        assert policy is not None
        if str(policy["request_id"]) != str(payload["request_id"]):
            raise DispatchStopLine("REVIEW_ASSURANCE_REQUEST_MISMATCH", assurance_id)
        if str(policy["policy_digest"]) != str(payload.get("policy_digest") or ""):
            raise DispatchStopLine("REVIEW_ASSURANCE_POLICY_MISMATCH", assurance_id)
        expected_subject = json_loads(policy["subject_json"], None)
        if expected_subject is None or expected_subject != payload["subject"]:
            raise DispatchStopLine("REVIEW_ASSURANCE_SUBJECT_MISMATCH", assurance_id)
        expected_assurance = json_loads(policy["assurance_policy_json"], None)
        if (
            not isinstance(expected_assurance, dict)
            or expected_assurance.get("revision") != payload["policy_revision"]
        ):
            raise DispatchStopLine("REVIEW_ASSURANCE_POLICY_REVISION_MISMATCH", assurance_id)
        reviewer = payload["reviewer"]
        reviewer_roles = expected_assurance.get("reviewer_roles", [])
        if reviewer.get("role") != policy["role"] or (
            reviewer_roles and reviewer.get("role") not in reviewer_roles
        ):
            raise DispatchStopLine("REVIEW_ASSURANCE_ROLE_MISMATCH", assurance_id)
        subject = payload["subject"]
        author_provenance = subject.get("author_provenance")
        if not isinstance(author_provenance, dict):
            author_provenance = {}
        author_instance = str(author_provenance.get("instance_id") or "")
        author_context = str(author_provenance.get("context_digest") or "")
        reviewer_instance = str(reviewer.get("instance_id") or "")
        reviewer_context = str(reviewer.get("context_digest") or "")
        required_independence = str(expected_assurance.get("independence") or "")
        missing_instance_authority = (
            required_independence
            in {
                "distinct_instance",
                "distinct_instance_and_context",
            }
            and not author_instance
        )
        missing_context_authority = (
            required_independence
            in {
                "distinct_context",
                "distinct_instance_and_context",
            }
            and not author_context
        )
        distinct_instance = (
            bool(author_instance)
            and bool(reviewer_instance)
            and reviewer_instance != author_instance
        )
        distinct_context = bool(author_context) and reviewer_context != author_context
        actual_independence = (
            "external_unverified"
            if missing_instance_authority or missing_context_authority
            else (
                "distinct_instance_and_context"
                if distinct_instance and distinct_context
                else "distinct_instance"
                if distinct_instance
                else "distinct_context"
                if distinct_context
                else "same_context"
            )
        )
        if str(reviewer.get("independence_class") or "") != actual_independence:
            raise DispatchStopLine("REVIEW_ASSURANCE_INDEPENDENCE_CLASS_MISMATCH", assurance_id)
        independence_ok = {
            "distinct_instance": distinct_instance,
            "distinct_context": distinct_context,
            "distinct_instance_and_context": distinct_instance and distinct_context,
        }.get(required_independence, False)
        if not independence_ok and str(expected_assurance.get("enforcement") or "") == "blocking":
            raise DispatchStopLine("REVIEW_ASSURANCE_INDEPENDENCE_MISMATCH", assurance_id)
        run = conn.execute(
            "SELECT policy_id, input_message_id, status, output_message_id, "
            "management_level, capability_receipt_json "
            "FROM dispatch_runs WHERE run_id=?",
            (payload["attempt_id"],),
        ).fetchone()
        if (
            run is None
            or str(run["policy_id"] or "") != policy_id
            or str(run["input_message_id"]) != str(payload["request_id"])
        ):
            raise DispatchStopLine("REVIEW_ASSURANCE_ATTEMPT_MISMATCH", assurance_id)
        response_id = str(payload.get("originating_response_id") or "")
        if (
            str(run["status"]) != "completed"
            or not response_id
            or response_id != str(run["output_message_id"] or "")
        ):
            raise DispatchStopLine("REVIEW_ASSURANCE_OUTCOME_NOT_CURRENT", assurance_id)
        response = conn.execute(
            "SELECT sender, sender_instance_id, review_envelope_json "
            "FROM messages WHERE id=? AND kind='response'",
            (response_id,),
        ).fetchone()
        if (
            response is None
            or str(response["sender"]) != str(reviewer.get("participant") or "")
            or str(response["sender_instance_id"] or "") != reviewer_instance
        ):
            raise DispatchStopLine("REVIEW_ASSURANCE_REVIEWER_MISMATCH", assurance_id)
        envelope = json_loads(response["review_envelope_json"], None)
        expected_envelope = {
            "policy_id": policy_id,
            "policy_digest": str(policy["policy_digest"]),
            "subject_digest": str(payload["subject"].get("digest") or ""),
            "disposition": payload["disposition"],
            "finding_counts": payload["finding_counts"],
            "artifact_paths": [
                str(item.get("location") or "") for item in payload["artifact_refs"]
            ],
        }
        if supersedes_assurance_id:
            superseded = conn.execute(
                "SELECT * FROM review_assurances WHERE assurance_id=?",
                (supersedes_assurance_id,),
            ).fetchone()
            superseded_lifecycle = _current_review_assurance_lifecycle(
                conn, supersedes_assurance_id
            )
            if superseded is None or superseded_lifecycle is None:
                raise DispatchStopLine(
                    "REVIEW_ASSURANCE_SUPERSESSION_TARGET_MISSING", supersedes_assurance_id
                )
            if supersedes_assurance_id == assurance_id:
                raise DispatchStopLine(
                    "REVIEW_ASSURANCE_SUPERSESSION_SELF", supersedes_assurance_id
                )
            if str(superseded["subject_json"]) != json_dumps(payload["subject"]):
                raise DispatchStopLine(
                    "REVIEW_ASSURANCE_SUPERSESSION_SUBJECT_MISMATCH", supersedes_assurance_id
                )
            if str(superseded_lifecycle["state"]) != "flagged":
                raise DispatchStopLine(
                    "REVIEW_ASSURANCE_SUPERSESSION_TARGET_NOT_FLAGGED",
                    supersedes_assurance_id,
                )
            if isinstance(supersedes_expected_version, bool) or int(
                supersedes_expected_version or 0
            ) != int(superseded_lifecycle["lifecycle_version"]):
                raise DispatchStopLine(
                    "REVIEW_ASSURANCE_LIFECYCLE_VERSION_STALE", supersedes_assurance_id
                )
            replaced_response_id = str(superseded["originating_response_id"] or "")
            if not replaced_response_id:
                raise DispatchStopLine(
                    "REVIEW_ASSURANCE_SUPERSESSION_RESPONSE_MISSING", supersedes_assurance_id
                )
            expected_envelope["replaces_response_id"] = replaced_response_id
        if envelope != expected_envelope:
            raise DispatchStopLine("REVIEW_ASSURANCE_RESPONSE_ENVELOPE_MISMATCH", assurance_id)
        instance = conn.execute(
            "SELECT external_session_ref_digest, launch_attempt_digest "
            "FROM agent_instances WHERE id=?",
            (reviewer_instance,),
        ).fetchone()
        observed_context = (
            str(instance["external_session_ref_digest"] or "")
            or str(instance["launch_attempt_digest"] or "")
            if instance is not None
            else ""
        )
        if not observed_context or reviewer_context != observed_context:
            raise DispatchStopLine("REVIEW_ASSURANCE_CONTEXT_UNPROVEN", assurance_id)
        if (
            payload["management_level"] != run["management_level"]
            or payload["management_level"] != policy["management_level"]
        ):
            raise DispatchStopLine("REVIEW_ASSURANCE_MANAGEMENT_MISMATCH", assurance_id)
        recorded = _parse_dispatch_utc(
            payload["recorded_utc"],
            code="REVIEW_ASSURANCE_TIME_INVALID",
            run_id=assurance_id,
        )
        valid_until = _parse_dispatch_utc(
            payload["valid_until_utc"],
            code="REVIEW_ASSURANCE_TIME_INVALID",
            run_id=assurance_id,
        )
        occurred = _parse_dispatch_utc(
            record.get("occurred_utc"),
            code="REVIEW_ASSURANCE_TIME_INVALID",
            run_id=assurance_id,
        )
        if recorded != occurred or (valid_until - recorded).total_seconds() != int(
            expected_assurance.get("validity_seconds") or 0
        ):
            raise DispatchStopLine("REVIEW_ASSURANCE_TIME_MISMATCH", assurance_id)
        if not authoritative:
            raise DispatchStopLine("REVIEW_ASSURANCE_AUTHORITY_MISMATCH", assurance_id)
    elif authoritative:
        raise DispatchStopLine("REVIEW_ASSURANCE_UNBOUND_AUTHORITY", assurance_id)

    conn.execute(
        """
        INSERT INTO review_assurances(
          assurance_id, request_id, attempt_id, bound, policy_id, policy_digest,
          subject_json, reviewer_json, policy_revision, disposition,
          finding_counts_json, management_level, originating_response_id,
          recorded_utc, valid_until_utc, authoritative, event_seq
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            assurance_id,
            payload["request_id"],
            payload["attempt_id"],
            1 if bound else 0,
            policy_id or None,
            payload.get("policy_digest"),
            json_dumps(payload["subject"]),
            json_dumps(payload["reviewer"]),
            payload["policy_revision"],
            payload["disposition"],
            json_dumps(payload["finding_counts"]),
            payload["management_level"],
            payload.get("originating_response_id"),
            payload["recorded_utc"],
            payload["valid_until_utc"],
            1 if authoritative else 0,
            event_seq,
        ),
    )
    initial_operation_key = hashlib.sha256(f"recorded:{assurance_id}".encode("utf-8")).hexdigest()
    conn.execute(
        """
        INSERT INTO review_assurance_lifecycle(
          lifecycle_event_id, assurance_id, lifecycle_version, state, reason_code,
          note, actor, occurred_utc, replacement_assurance_id, operation_key, event_seq
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            f"{record['event_id']}:active",
            assurance_id,
            1,
            "active",
            None,
            None,
            str(record.get("actor") or ""),
            str(record.get("occurred_utc") or ""),
            None,
            initial_operation_key,
            event_seq,
        ),
    )
    if supersedes_assurance_id:
        assert superseded_lifecycle is not None
        prior_version = int(superseded_lifecycle["lifecycle_version"])
        supersession_operation_key = hashlib.sha256(
            (f"superseded:{supersedes_assurance_id}:{prior_version}:{assurance_id}").encode("utf-8")
        ).hexdigest()
        conn.execute(
            """
            INSERT INTO review_assurance_lifecycle(
              lifecycle_event_id, assurance_id, lifecycle_version, state, reason_code,
              note, actor, occurred_utc, replacement_assurance_id, operation_key, event_seq
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                f"{record['event_id']}:superseded",
                supersedes_assurance_id,
                prior_version + 1,
                "superseded",
                "replacement_accepted",
                None,
                str(record.get("actor") or ""),
                str(record.get("occurred_utc") or ""),
                assurance_id,
                supersession_operation_key,
                event_seq,
            ),
        )
    for index, item in enumerate(payload["artifact_refs"]):
        conn.execute(
            """
            INSERT INTO assurance_artifact_refs(
              assurance_id, ref_index, location_type, location, sha256, byte_size,
              media_type, revision, visibility, provenance, subject_digest,
              field_privacy_json
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                assurance_id,
                index,
                item["location_type"],
                item["location"],
                item["sha256"],
                item["byte_size"],
                item["media_type"],
                item["revision"],
                item["visibility"],
                item["provenance"],
                item["subject_digest"],
                json_dumps(item["field_privacy"]),
            ),
        )
    if supersedes_assurance_id:
        from agent_mesh.core.assurance import evaluate_review_assurance

        replacement_gate = evaluate_review_assurance(
            config,
            conn,
            policy_id,
            now_utc=str(record.get("occurred_utc") or ""),
            validate_live_subject=False,
            validate_live_artifacts=False,
        )
        if (
            not replacement_gate.satisfied
            or assurance_id not in replacement_gate.qualifying_assurance_ids
        ):
            raise DispatchStopLine(
                "REVIEW_ASSURANCE_SUPERSESSION_NOT_QUALIFYING",
                ",".join(replacement_gate.reason_codes),
            )


def _current_review_assurance_lifecycle(conn, assurance_id: str):
    return conn.execute(
        "SELECT * FROM review_assurance_lifecycle WHERE assurance_id=? "
        "ORDER BY lifecycle_version DESC LIMIT 1",
        (assurance_id,),
    ).fetchone()


def _project_review_assurance_lifecycle(
    conn,
    record: dict[str, Any],
    payload: dict[str, Any],
    event_seq: int,
    *,
    state: str,
) -> None:
    assurance_id = str(payload["assurance_id"])
    if str(record.get("entity_id") or "") != assurance_id:
        raise DispatchStopLine("REVIEW_ASSURANCE_IDENTITY_MISMATCH", assurance_id)
    assurance = conn.execute(
        "SELECT request_id, originating_response_id FROM review_assurances WHERE assurance_id=?",
        (assurance_id,),
    ).fetchone()
    if assurance is None:
        raise DispatchStopLine("REVIEW_ASSURANCE_LIFECYCLE_TARGET_MISSING", assurance_id)
    if str(record.get("thread_id") or "") != str(assurance["request_id"]):
        raise DispatchStopLine("REVIEW_ASSURANCE_THREAD_MISMATCH", assurance_id)
    if str(payload["response_id"]) != str(assurance["originating_response_id"] or ""):
        raise DispatchStopLine("REVIEW_ASSURANCE_RESPONSE_MISMATCH", assurance_id)
    duplicate = conn.execute(
        "SELECT assurance_id FROM review_assurance_lifecycle WHERE operation_key=?",
        (str(payload["operation_key"]),),
    ).fetchone()
    if duplicate is not None:
        raise DispatchStopLine("REVIEW_ASSURANCE_OPERATION_DUPLICATE", assurance_id)
    current = _current_review_assurance_lifecycle(conn, assurance_id)
    if current is None:
        raise DispatchStopLine("REVIEW_ASSURANCE_LIFECYCLE_STATE_MISSING", assurance_id)
    expected_version = int(payload["expected_version"])
    next_version = int(payload["next_version"])
    if (
        expected_version != int(current["lifecycle_version"])
        or next_version != expected_version + 1
    ):
        raise DispatchStopLine("REVIEW_ASSURANCE_LIFECYCLE_VERSION_STALE", assurance_id)
    expected_current_state = "active" if state == "flagged" else "flagged"
    if str(current["state"]) != expected_current_state:
        raise DispatchStopLine(
            "REVIEW_ASSURANCE_LIFECYCLE_TRANSITION_INVALID",
            f"{assurance_id}:{current['state']}->{state}",
        )
    conn.execute(
        """
        INSERT INTO review_assurance_lifecycle(
          lifecycle_event_id, assurance_id, lifecycle_version, state, reason_code,
          note, actor, occurred_utc, replacement_assurance_id, operation_key, event_seq
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            str(record["event_id"]),
            assurance_id,
            next_version,
            state,
            str(payload["reason_code"]),
            str(payload["note"]),
            str(record.get("actor") or ""),
            str(record.get("occurred_utc") or ""),
            None,
            str(payload["operation_key"]),
            event_seq,
        ),
    )


def _open_lease_for_run(conn, run_id: str):
    return conn.execute(
        "SELECT lease_id FROM dispatch_leases WHERE run_id=? AND status='open'", (run_id,)
    ).fetchone()


def _request_thread_for_run(conn, input_message_id) -> str | None:
    """The canonical thread of a run's input request (the authority for the run's thread, §1)."""
    if not input_message_id:
        return None
    row = conn.execute(
        "SELECT thread_id FROM messages WHERE id=? AND kind='request'", (input_message_id,)
    ).fetchone()
    return str(row["thread_id"]) if row is not None and row["thread_id"] else None


def _project_dispatch_run_planned(conn, record, payload, event_seq: int) -> None:
    run_id = str(payload["run_id"])
    input_message_id = str(payload["input_message_id"])
    _require_known_request(conn, input_message_id, run_id)
    policy_id = str(payload.get("policy_id") or "")
    if policy_id:
        policy = conn.execute(
            "SELECT * FROM dispatch_policies WHERE policy_id=?", (policy_id,)
        ).fetchone()
        if policy is None:
            raise DispatchStopLine("DISPATCH_ATTEMPT_POLICY_MISSING", policy_id)
        if str(policy["request_id"]) != input_message_id or str(policy["policy_digest"]) != str(
            payload["policy_digest"]
        ):
            raise DispatchStopLine("DISPATCH_ATTEMPT_POLICY_MISMATCH", run_id)
        target = json_loads(policy["target_json"], {})
        if (
            not isinstance(target, dict)
            or str(target.get("participant") or "") != str(payload["target_agent"])
            or str(target.get("runtime_profile") or "") != str(payload["runtime_profile"])
            or str(policy["management_level"]) != str(payload["management_level"])
        ):
            raise DispatchStopLine("DISPATCH_ATTEMPT_ROUTE_MISMATCH", run_id)
        _validate_dispatch_attempt_authority(policy, payload, run_id)
        if (
            conn.execute("SELECT 1 FROM dispatch_runs WHERE run_id=?", (run_id,)).fetchone()
            is not None
        ):
            raise DispatchStopLine("DISPATCH_ATTEMPT_DUPLICATE", run_id)
        attempt_number = int(payload["attempt_number"])
        retry = json_loads(policy["retry_json"], {})
        max_attempts = int(retry.get("max_attempts") or 0) if isinstance(retry, dict) else 0
        if attempt_number > max_attempts:
            raise DispatchStopLine("DISPATCH_ATTEMPT_RETRY_EXHAUSTED", run_id)
        prior = conn.execute(
            "SELECT run_id, status, attempt_number FROM dispatch_runs "
            "WHERE policy_id=? ORDER BY attempt_number DESC LIMIT 1",
            (policy_id,),
        ).fetchone()
        if attempt_number == 1:
            if prior is not None:
                raise DispatchStopLine("DISPATCH_ATTEMPT_SEQUENCE_INVALID", run_id)
        elif (
            prior is None
            or int(prior["attempt_number"] or 0) != attempt_number - 1
            or str(prior["run_id"]) != str(payload.get("previous_attempt_id") or "")
            or str(prior["status"])
            not in {"completed", "failed", "cancelled", "timed_out", "parent_lost"}
        ):
            raise DispatchStopLine("DISPATCH_ATTEMPT_PREDECESSOR_INVALID", run_id)
    grounding = payload.get("grounding", {}) or {}
    try:
        conn.execute(
            """
            INSERT INTO dispatch_runs(
              run_id, run_mode, input_message_id, target_agent, gen_ai_system, model,
              session_key, session_key_source, session_uuid, wave, classification, gate,
              gate_reason_code, requires_gate_json, grounding_complete, grounding_digest,
              plan_artifact_hash, adapter_capabilities_json, target_event_seq, response_mode,
              status, planned, thread_id, planned_utc, policy_id, policy_digest,
              attempt_number, previous_attempt_id, management_level, runtime_profile,
              effective_capability_evidence_digest, capability_receipt_json, event_seq
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(run_id) DO UPDATE SET
              run_mode=excluded.run_mode, input_message_id=excluded.input_message_id,
              target_agent=excluded.target_agent, gen_ai_system=excluded.gen_ai_system,
              model=excluded.model, session_key=excluded.session_key,
              session_key_source=excluded.session_key_source, session_uuid=excluded.session_uuid,
              wave=excluded.wave, classification=excluded.classification, gate=excluded.gate,
              gate_reason_code=excluded.gate_reason_code,
              requires_gate_json=excluded.requires_gate_json,
              grounding_complete=excluded.grounding_complete,
              grounding_digest=excluded.grounding_digest,
              plan_artifact_hash=excluded.plan_artifact_hash,
              adapter_capabilities_json=excluded.adapter_capabilities_json,
              target_event_seq=excluded.target_event_seq, response_mode=excluded.response_mode,
              status=excluded.status, planned=1, thread_id=excluded.thread_id,
              planned_utc=excluded.planned_utc, policy_id=excluded.policy_id,
              policy_digest=excluded.policy_digest, attempt_number=excluded.attempt_number,
              previous_attempt_id=excluded.previous_attempt_id,
              management_level=excluded.management_level,
              runtime_profile=excluded.runtime_profile,
              effective_capability_evidence_digest=
                excluded.effective_capability_evidence_digest,
              capability_receipt_json=excluded.capability_receipt_json,
              event_seq=excluded.event_seq
            """,
            (
                run_id,
                payload["run_mode"],
                input_message_id,
                payload["target_agent"],
                payload.get("gen_ai_system"),
                payload.get("model"),
                payload.get("session_key"),
                payload.get("session_key_source"),
                payload.get("session_uuid"),
                payload.get("wave"),
                payload.get("classification"),
                payload.get("gate"),
                payload.get("gate_reason_code"),
                json_dumps(payload.get("requires_gate", [])),
                1 if grounding.get("complete") else 0,
                grounding.get("digest"),
                payload.get("plan_artifact_hash"),
                json_dumps(payload.get("adapter_capabilities", {})),
                payload.get("target_event_seq"),
                payload.get("response_mode"),
                payload.get("status"),
                str(record.get("thread_id", "")),
                payload.get("planned_utc"),
                payload.get("policy_id"),
                payload.get("policy_digest"),
                payload.get("attempt_number"),
                payload.get("previous_attempt_id"),
                payload.get("management_level"),
                payload.get("runtime_profile"),
                payload.get("effective_capability_evidence_digest"),
                json_dumps(payload.get("capability_receipt")),
                event_seq,
            ),
        )
    except sqlite3.IntegrityError as exc:
        code = "DISPATCH_ATTEMPT_ACTIVE" if policy_id else "DISPATCH_RUN_DUPLICATE"
        raise DispatchStopLine(code, run_id) from exc


def _parse_dispatch_utc(value: Any, *, code: str, run_id: str) -> datetime:
    raw = str(value or "")
    if not raw.endswith("Z"):
        raise DispatchStopLine(code, run_id)
    try:
        parsed = datetime.fromisoformat(raw[:-1] + "+00:00")
    except ValueError as exc:
        raise DispatchStopLine(code, run_id) from exc
    if parsed.tzinfo is None:
        raise DispatchStopLine(code, run_id)
    return parsed.astimezone(UTC)


def _validate_dispatch_attempt_authority(policy, payload, run_id: str) -> None:
    """Replay the immutable profile and capability authority for dispatch.v1."""

    frozen = json_loads(policy["runtime_profile_revision_json"], None)
    receipt = payload.get("capability_receipt")
    if not isinstance(frozen, dict) or not isinstance(receipt, dict):
        raise DispatchStopLine("DISPATCH_ATTEMPT_PROFILE_REVISION_INVALID", run_id)
    target = json_loads(policy["target_json"], {})
    if (
        receipt.get("profile_digest") != frozen.get("digest")
        or payload.get("runtime_profile") != frozen.get("name")
        or payload.get("target_agent") != frozen.get("target")
        or payload.get("model") != frozen.get("model")
        or payload.get("gen_ai_system") != frozen.get("provider")
        or not isinstance(target, dict)
        or target.get("durable_role") != frozen.get("durable_role")
        or policy["permission_ceiling"] != frozen.get("permission_mode")
    ):
        raise DispatchStopLine("DISPATCH_ATTEMPT_PROFILE_REVISION_MISMATCH", run_id)
    required = json_loads(policy["required_capabilities_json"], [])
    results = receipt.get("results")
    if not isinstance(required, list) or not isinstance(results, list):
        raise DispatchStopLine("DISPATCH_ATTEMPT_CAPABILITY_RECEIPT_INVALID", run_id)
    by_capability = {
        str(item.get("capability") or ""): item for item in results if isinstance(item, dict)
    }
    if set(by_capability) != {str(item) for item in required}:
        raise DispatchStopLine("DISPATCH_ATTEMPT_CAPABILITY_SET_MISMATCH", run_id)
    assurance_policy = json_loads(policy["assurance_policy_json"], None)
    blocking = (
        isinstance(assurance_policy, dict) and assurance_policy.get("enforcement") == "blocking"
    )
    for item in by_capability.values():
        if item.get("declared") is not True or item.get("effective") is not True:
            raise DispatchStopLine("DISPATCH_ATTEMPT_CAPABILITY_UNPROVEN", run_id)
        if blocking and item.get("evidence_class") not in {
            "generic_host",
            "built_in_driver",
        }:
            raise DispatchStopLine("DISPATCH_ATTEMPT_CAPABILITY_UNTRUSTED", run_id)
    observed = _parse_dispatch_utc(
        receipt.get("observed_utc"),
        code="DISPATCH_ATTEMPT_CAPABILITY_TIME_INVALID",
        run_id=run_id,
    )
    valid_until = _parse_dispatch_utc(
        receipt.get("valid_until_utc"),
        code="DISPATCH_ATTEMPT_CAPABILITY_TIME_INVALID",
        run_id=run_id,
    )
    planned = _parse_dispatch_utc(
        payload.get("planned_utc"),
        code="DISPATCH_ATTEMPT_CAPABILITY_TIME_INVALID",
        run_id=run_id,
    )
    if not (observed <= planned < valid_until) or (valid_until - observed).total_seconds() > 300:
        raise DispatchStopLine("DISPATCH_ATTEMPT_CAPABILITY_EXPIRED", run_id)


def _project_dispatch_run_blocked(conn, record, payload, event_seq: int) -> None:
    run_id = str(payload["run_id"])
    input_message_id = str(payload["input_message_id"])
    _require_known_request(conn, input_message_id, run_id)
    conn.execute(
        """
        INSERT INTO dispatch_runs(
          run_id, run_mode, input_message_id, target_agent, gate,
          block_reason_codes_json, missing_count, status, thread_id, planned_utc, event_seq
        ) VALUES (?,?,?,?,?,?,?,'blocked',?,?,?)
        ON CONFLICT(run_id) DO UPDATE SET
          gate=excluded.gate, block_reason_codes_json=excluded.block_reason_codes_json,
          missing_count=excluded.missing_count, status='blocked', event_seq=excluded.event_seq
        """,
        (
            run_id,
            payload["run_mode"],
            input_message_id,
            payload["target_agent"],
            payload.get("gate"),
            json_dumps(payload.get("block_reason_codes", [])),
            payload.get("missing_count"),
            str(record.get("thread_id", "")),
            payload.get("planned_utc"),
            event_seq,
        ),
    )


def _project_dispatch_lease_acquired(conn, payload, event_seq: int) -> None:
    run_id = str(payload["run_id"])
    known_run = conn.execute("SELECT 1 FROM dispatch_runs WHERE run_id=?", (run_id,)).fetchone()
    if known_run is None:
        # The contract §4 FK dispatch_leases.run_id -> dispatch_runs.run_id, enforced at replay
        # (the codebase enforces FKs via stop-lines, not SQL FK clauses on log-rebuilt tables).
        raise DispatchStopLine(
            DISPATCH_LEASE_UNKNOWN_RUN, f"lease={payload.get('lease_id')} run_id={run_id}"
        )
    try:
        conn.execute(
            """
            INSERT INTO dispatch_leases(
              lease_id, run_id, input_message_id, target_agent, session_uuid,
              ttl_seconds, status, created_utc, event_seq
            ) VALUES (?,?,?,?,?,?,'open',?,?)
            """,
            (
                payload["lease_id"],
                payload["run_id"],
                payload["input_message_id"],
                payload["target_agent"],
                payload.get("session_uuid"),
                payload.get("ttl_seconds"),
                payload.get("created_utc"),
                event_seq,
            ),
        )
    except sqlite3.IntegrityError as exc:
        # The lease_id PK or the uq_dispatch_leases_open partial-unique index rejected the row:
        # a second open lease for the same (input_message_id, target_agent), or a duplicate id.
        raise DispatchStopLine(
            DISPATCH_LEASE_DUPLICATE,
            f"{payload.get('input_message_id')}/{payload.get('target_agent')}",
        ) from exc


def _project_dispatch_lease_released(conn, payload, event_seq: int) -> None:
    lease_id = str(payload["lease_id"])
    run_id = str(payload["run_id"])
    reason = payload.get("reason")
    superseded_by = payload.get("superseded_by_run_id")
    # A release must target an EXISTING, OPEN lease that belongs to the named run; otherwise the
    # UPDATE would silently no-op and the release fact would live only in the log, invisible to the
    # projection and to `agent-q dispatches verify`. Fail closed instead.
    lease = conn.execute(
        "SELECT run_id, status FROM dispatch_leases WHERE lease_id=?", (lease_id,)
    ).fetchone()
    if lease is None:
        raise DispatchStopLine(DISPATCH_LEASE_RELEASE_INVALID, f"unknown lease={lease_id}")
    if lease["status"] != "open":
        raise DispatchStopLine(
            DISPATCH_LEASE_RELEASE_INVALID, f"lease not open: {lease_id} status={lease['status']}"
        )
    if str(lease["run_id"]) != run_id:
        raise DispatchStopLine(
            DISPATCH_LEASE_RELEASE_INVALID,
            f"lease={lease_id} run mismatch event_run={run_id} lease_run={lease['run_id']}",
        )
    if reason == "superseded":
        if not superseded_by or superseded_by == run_id:
            raise DispatchStopLine(
                DISPATCH_SUPERSEDE_INVALID, f"lease={lease_id} superseded_by={superseded_by}"
            )
        # The contract requires the successor to be a run with a dispatch_run_planned (§6); a
        # blocked-only run also creates a row, so check the durable `planned` flag, not mere
        # existence (status mutates as the run advances, so it cannot be the discriminator).
        target = conn.execute(
            "SELECT planned FROM dispatch_runs WHERE run_id=?", (superseded_by,)
        ).fetchone()
        if target is None or not target["planned"]:
            raise DispatchStopLine(
                DISPATCH_SUPERSEDE_INVALID,
                f"superseded_by_run_id is not a planned run: {superseded_by}",
            )
    cur = conn.execute(
        """
        UPDATE dispatch_leases SET status='released', reason=?, superseded_by_run_id=?,
          released_utc=?, event_seq=? WHERE lease_id=? AND status='open'
        """,
        (reason, superseded_by, payload.get("released_utc"), event_seq, lease_id),
    )
    if cur.rowcount != 1:
        raise DispatchStopLine(
            DISPATCH_LEASE_RELEASE_INVALID, f"release affected {cur.rowcount} rows: {lease_id}"
        )


def _project_dispatch_run_started(conn, payload, event_seq: int) -> None:
    run_id = str(payload["run_id"])
    if _open_lease_for_run(conn, run_id) is None:
        raise DispatchStopLine(DISPATCH_RUN_WITHOUT_LEASE, run_id)
    conn.execute(
        "UPDATE dispatch_runs SET status='started', started_utc=?, event_seq=? WHERE run_id=?",
        (payload.get("started_utc"), event_seq, run_id),
    )


def _project_dispatch_run_terminal(conn, kind: str, payload, event_seq: int) -> None:
    run_id = str(payload["run_id"])
    row = conn.execute(
        "SELECT started_utc, input_message_id, policy_id FROM dispatch_runs WHERE run_id=?",
        (run_id,),
    ).fetchone()
    started = row is not None and row["started_utc"] is not None
    open_lease = _open_lease_for_run(conn, run_id) is not None
    if not (started and open_lease):
        raise DispatchStopLine(
            DISPATCH_RUN_TERMINAL_WITHOUT_STARTED_OR_LEASE,
            f"{run_id} started={started} open_lease={open_lease}",
        )
    if kind == "dispatch_run_completed":
        output_message_id = str(payload["output_message_id"])
        out = conn.execute(
            "SELECT thread_id, sender_instance_id, event_seq FROM messages "
            "WHERE id=? AND kind='response'",
            (output_message_id,),
        ).fetchone()
        if out is None:
            raise DispatchStopLine(
                DISPATCH_OUTPUT_MESSAGE_INVALID, f"{run_id} output_message_id={output_message_id}"
            )
        if row is not None and row["policy_id"]:
            binding = conn.execute(
                "SELECT 1 FROM message_source_context_refs "
                "WHERE message_id=? AND channel='agent-mesh-dispatch' "
                "AND source_event_id=? AND role='authoritative_body'",
                (output_message_id, run_id),
            ).fetchone()
            if binding is None:
                raise DispatchStopLine(
                    DISPATCH_OUTPUT_MESSAGE_INVALID,
                    f"{run_id} output is not dispatcher-bound",
                )
        # Bind against the input request's CANONICAL thread (resolved from messages), never the run's
        # self-declared/possibly-empty stored thread. Fail closed if it cannot be established.
        req_thread = _request_thread_for_run(
            conn, row["input_message_id"] if row is not None else None
        )
        if not req_thread or str(out["thread_id"]) != str(req_thread):
            raise DispatchStopLine(
                DISPATCH_OUTPUT_THREAD_MISMATCH,
                f"{run_id} output_thread={out['thread_id']} req_thread={req_thread!r}",
            )
        launch = conn.execute(
            "SELECT instance_id, status, bound_event_seq, released_event_seq "
            "FROM agent_instance_launch_runs WHERE run_id=?",
            (run_id,),
        ).fetchone()
        if launch is not None:
            if str(out["sender_instance_id"] or "") != str(launch["instance_id"]):
                raise DispatchStopLine(
                    DISPATCH_OUTPUT_INSTANCE_MISMATCH,
                    f"{run_id} output was not authored by its bound child instance",
                )
            output_event_seq = int(out["event_seq"])
            bound_event_seq = int(launch["bound_event_seq"])
            if (
                str(launch["status"]) != "active"
                or launch["released_event_seq"] is not None
                or output_event_seq <= bound_event_seq
                or output_event_seq >= event_seq
            ):
                raise DispatchStopLine(
                    DISPATCH_OUTPUT_ORDER_INVALID,
                    f"{run_id} requires bound child < response < completion < release/terminal",
                )
        conn.execute(
            """
            UPDATE dispatch_runs SET status='completed', output_message_id=?, input_tokens=?,
              cache_read_input_tokens=?, cache_creation_input_tokens=?, total_input_tokens=?,
              completed_utc=?, terminal_code='completed_with_bound_response', event_seq=?
              WHERE run_id=?
            """,
            (
                output_message_id,
                payload.get("input_tokens"),
                payload.get("cache_read_input_tokens"),
                payload.get("cache_creation_input_tokens"),
                payload.get("total_input_tokens"),
                payload.get("completed_utc"),
                event_seq,
                run_id,
            ),
        )
    else:  # dispatch_run_failed
        conn.execute(
            "UPDATE dispatch_runs SET status='failed', error_class=?, failed_utc=?, "
            "terminal_code=?, event_seq=? "
            "WHERE run_id=?",
            (
                payload.get("error_class"),
                payload.get("failed_utc"),
                payload.get("error_class"),
                event_seq,
                run_id,
            ),
        )


def _project_dispatch_run_terminated(conn, payload, event_seq: int) -> None:
    run_id = str(payload["run_id"])
    terminal_state = str(payload["terminal_state"])
    row = conn.execute(
        "SELECT status, started_utc FROM dispatch_runs WHERE run_id=?",
        (run_id,),
    ).fetchone()
    if row is None or str(row["status"]) not in {"planned", "started"}:
        raise DispatchStopLine("DISPATCH_RUN_TERMINATION_INVALID", run_id)
    started = row["started_utc"] is not None
    open_lease = _open_lease_for_run(conn, run_id) is not None
    if terminal_state == "timed_out" and not (started and open_lease):
        raise DispatchStopLine(
            "DISPATCH_RUN_TERMINAL_WITHOUT_STARTED_OR_LEASE",
            f"{run_id} started={started} open_lease={open_lease}",
        )
    if started and not open_lease:
        raise DispatchStopLine(
            "DISPATCH_RUN_TERMINAL_WITHOUT_STARTED_OR_LEASE",
            f"{run_id} started={started} open_lease={open_lease}",
        )
    error_class = {
        "cancelled": "Cancelled",
        "timed_out": "Timeout",
        "parent_lost": "ParentLoss",
    }[terminal_state]
    conn.execute(
        "UPDATE dispatch_runs SET status=?, error_class=?, failed_utc=?, "
        "terminal_code=?, event_seq=? WHERE run_id=?",
        (
            terminal_state,
            error_class,
            payload["terminated_utc"],
            terminal_state,
            event_seq,
            run_id,
        ),
    )


def _project_dispatch_retry_exhausted(conn, record, payload, event_seq: int) -> None:
    policy_id = str(payload["policy_id"])
    run_id = str(payload["run_id"])
    policy = conn.execute(
        "SELECT request_id, retry_json, retry_exhausted_run_id FROM dispatch_policies "
        "WHERE policy_id=?",
        (policy_id,),
    ).fetchone()
    run = conn.execute(
        "SELECT policy_id, attempt_number, status FROM dispatch_runs WHERE run_id=?",
        (run_id,),
    ).fetchone()
    retry = json_loads(policy["retry_json"], {}) if policy is not None else {}
    maximum = int(retry.get("max_attempts") or 0) if isinstance(retry, dict) else 0
    terminal_statuses = {
        "completed",
        "failed",
        "cancelled",
        "timed_out",
        "parent_lost",
    }
    if (
        policy is None
        or run is None
        or str(record.get("entity_id") or "") != policy_id
        or str(record.get("thread_id") or "") != str(policy["request_id"])
        or str(run["policy_id"] or "") != policy_id
        or int(run["attempt_number"] or 0) != int(payload["attempt_number"])
        or int(payload["max_attempts"]) != maximum
        or int(payload["attempt_number"]) != maximum
        or str(run["status"]) not in terminal_statuses
    ):
        raise DispatchStopLine("DISPATCH_RETRY_EXHAUSTION_INVALID", policy_id)
    latest = conn.execute(
        "SELECT run_id FROM dispatch_runs WHERE policy_id=? "
        "ORDER BY attempt_number DESC, event_seq DESC LIMIT 1",
        (policy_id,),
    ).fetchone()
    if latest is None or str(latest["run_id"]) != run_id:
        raise DispatchStopLine("DISPATCH_RETRY_EXHAUSTION_NOT_CURRENT", policy_id)
    if policy["retry_exhausted_run_id"] is not None:
        raise DispatchStopLine("DISPATCH_RETRY_EXHAUSTION_DUPLICATE", policy_id)
    conn.execute(
        "UPDATE dispatch_policies SET retry_exhausted_run_id=?, retry_exhausted_utc=?, "
        "event_seq=? WHERE policy_id=?",
        (run_id, payload["exhausted_utc"], event_seq, policy_id),
    )


def _project_decision_event(
    conn: sqlite3.Connection,
    record: dict[str, Any],
    config: AgentMeshConfig,
) -> None:
    kind = record["kind"]
    payload = record["payload"]
    dec_ulid = validate_decision_event(
        conn,
        kind=str(kind),
        entity_id=str(record["entity_id"]),
        thread_id=str(record["thread_id"]),
        actor=str(record["actor"]),
        occurred_utc=str(record["occurred_utc"]),
        payload=payload,
    )
    if kind == "decision_proposed":
        _project_decision_proposed(conn, record)
        return

    if kind == "decision_accepted":
        row = conn.execute(
            "SELECT human_id, tier, meta_json FROM decisions WHERE dec_ulid=?", (dec_ulid,)
        ).fetchone()
        binding = payload.get("review_gate")
        if binding is not None:
            from agent_mesh.core.assurance import (
                AssuranceResolutionError,
                validate_assurance_gate_binding,
            )

            try:
                validate_assurance_gate_binding(
                    config,
                    conn,
                    binding,
                    boundary_kind="decision",
                    boundary_key=str(row["human_id"]),
                )
            except AssuranceResolutionError as exc:
                raise DispatchStopLine(str(exc), dec_ulid) from exc
        tier = row["tier"]
        meta = json_loads(row["meta_json"], {})
        _append_decision_log(conn, dec_ulid, record)
        if not _decision_quorum_reached(meta, record):
            return
        status = "in_force" if tier in DECISION_IN_FORCE_TIERS else "accepted"
        conn.execute(
            "UPDATE decisions SET status=?, accepted_utc=?, in_force_utc=?, event_seq=? "
            "WHERE dec_ulid=?",
            (
                status,
                record["occurred_utc"],
                record["occurred_utc"] if status == "in_force" else None,
                record["event_seq"],
                dec_ulid,
            ),
        )
    elif kind == "decision_revisited":
        _append_decision_log(conn, dec_ulid, record)
        new_id = payload.get("new_decision_id")
        if new_id and resolve_decision(conn, str(new_id)) is None:
            raise DecisionStopLine(DECISION_PARENT_MISSING, str(new_id))
    elif kind == "decision_superseded":
        successor = resolve_decision(conn, str(payload.get("superseded_by", "")))
        if successor is None:
            raise DecisionStopLine(
                DECISION_SUPERSEDE_TARGET_INVALID, str(payload.get("superseded_by"))
            )
        _ensure_supersede_target_valid(conn, successor)
        _ensure_no_supersede_cycle(conn, dec_ulid, successor)
        conn.execute(
            "UPDATE decisions SET status='superseded', superseded_by=?, event_seq=? WHERE dec_ulid=?",
            (successor, record["event_seq"], dec_ulid),
        )
        conn.execute(
            "UPDATE decisions SET supersedes=?, event_seq=? WHERE dec_ulid=?",
            (dec_ulid, record["event_seq"], successor),
        )
    elif kind == "decision_retired":
        conn.execute(
            "UPDATE decisions SET status='retired', retired_utc=?, event_seq=? WHERE dec_ulid=?",
            (record["occurred_utc"], record["event_seq"], dec_ulid),
        )
    elif kind == "decision_rejected":
        conn.execute(
            "UPDATE decisions SET status='rejected', event_seq=? WHERE dec_ulid=?",
            (record["event_seq"], dec_ulid),
        )
    elif kind == "decision_metadata_updated":
        _project_decision_metadata_updated(conn, record, dec_ulid)
    elif kind == "decision_assumption_violated":
        conn.execute(
            "UPDATE decision_assumptions SET status='violated', invalidated_event_id=? "
            "WHERE dec_ulid=? AND assumption_id=?",
            (record["event_id"], dec_ulid, payload.get("assumption_id")),
        )
    elif kind == "decision_check_failed":
        _append_decision_log(conn, dec_ulid, record)
    elif kind == "decision_drift_detected":
        conn.execute(
            "UPDATE decision_verifications SET last_verified_utc=?, last_outcome='fail', "
            "last_event_id=? WHERE dec_ulid=? AND command=?",
            (record["occurred_utc"], record["event_id"], dec_ulid, payload.get("command")),
        )
        conn.execute(
            "UPDATE decisions SET last_verified_utc=?, event_seq=? WHERE dec_ulid=?",
            (record["occurred_utc"], record["event_seq"], dec_ulid),
        )
        _append_decision_log(conn, dec_ulid, record)
    elif kind == "decision_verification_recorded":
        outcome = str(payload.get("outcome") or "")
        if outcome not in {"pass", "fail"}:
            raise ValueError(f"invalid decision verification outcome: {outcome}")
        conn.execute(
            "UPDATE decision_verifications SET last_verified_utc=?, last_outcome=?, "
            "last_event_id=? WHERE dec_ulid=? AND command=?",
            (
                record["occurred_utc"],
                outcome,
                record["event_id"],
                dec_ulid,
                payload.get("command"),
            ),
        )
        conn.execute(
            "UPDATE decisions SET last_verified_utc=?, event_seq=? WHERE dec_ulid=?",
            (record["occurred_utc"], record["event_seq"], dec_ulid),
        )
        _append_decision_log(conn, dec_ulid, record)


def _legacy_decision_assumptions(value: Any) -> list[dict[str, Any]]:
    assumptions: list[dict[str, Any]] = []
    raw_values = value if isinstance(value, list) else []
    for index, item in enumerate(raw_values, start=1):
        if isinstance(item, dict):
            assumption_id = str(item.get("id") or item.get("assumption_id") or f"A{index}")
            text = str(item.get("text", ""))
            references = item.get("references", [])
        else:
            assumption_id = f"A{index}"
            text = str(item)
            references = []
        assumptions.append({"id": assumption_id, "text": text, "references": references})
    return assumptions


def _legacy_decision_evidence(value: Any) -> dict[str, list[str]]:
    if not isinstance(value, dict):
        return {}
    evidence: dict[str, list[str]] = {}
    for raw_kind, raw_values in value.items():
        values = raw_values if isinstance(raw_values, list) else [raw_values]
        evidence[str(raw_kind)] = [str(item) for item in values]
    return evidence


def _project_decision_proposed(conn, record: dict[str, Any]) -> None:
    payload = record["payload"]
    dec_ulid = record["entity_id"]
    human_id = str(payload["human_id"])
    applicability_scope = applicability_scope_for(
        payload.get("applicability_scope"), payload.get("affected_code_globs", [])
    )
    parent = parent_human_id(human_id)
    validate_decision_proposal_identity(
        conn,
        human_id,
        dec_ulid=dec_ulid,
        aliases=(str(alias) for alias in payload.get("aliases", [])),
    )

    supersedes = payload.get("supersedes")
    supersedes_ulid = None
    if supersedes:
        supersedes_ulid = resolve_decision(conn, str(supersedes))
        if supersedes_ulid is None:
            raise DecisionStopLine(DECISION_SUPERSEDE_TARGET_INVALID, str(supersedes))
        _ensure_supersede_target_valid(conn, supersedes_ulid)
        _ensure_no_supersede_cycle(conn, supersedes_ulid, dec_ulid)

    contract_version = _decision_contract_version(payload)
    raw_review_policy = payload.get("review_policy", {})
    if contract_version >= 5:
        normalized_review_policy = normalize_decision_review_policy(
            raw_review_policy, allow_extensions=True
        )
        review_policy = (
            {**raw_review_policy, **normalized_review_policy}
            if isinstance(raw_review_policy, dict)
            else normalized_review_policy
        )
    else:
        review_policy = raw_review_policy
    meta = {
        "context": payload.get("context", ""),
        "decision": payload.get("decision", ""),
        "body_format": payload.get("body_format") or decision_body_format_for_proposal(payload),
        "rejected_alternatives": payload.get("rejected_alternatives", []),
        "consequences": payload.get("consequences", []),
        "review_policy": review_policy,
        "exemptions": payload.get("exemptions", []),
        "generated_artifact_paths": payload.get("generated_artifact_paths", []),
        "author_provenance": _decision_revision_author_provenance(conn, record),
    }
    if isinstance(payload.get("legacy_markdown_body"), str):
        meta["legacy_markdown_body"] = payload["legacy_markdown_body"]
        meta["legacy_body_source"] = str(payload.get("legacy_body_source") or "")
    verification = normalize_decision_verification(payload.get("verification", []))
    drift_risk = _max_drift_risk(
        item.get("drift_risk") for item in verification if isinstance(item, dict)
    )
    conn.execute(
        """
        INSERT INTO decisions(
          dec_ulid, human_id, parent_human_id, title, tier, tier_valid, status,
          contract_version, applicability_scope, enforcement_mode, owner,
          body_sha, body_path, body_bytes, body_media_type, superseded_by, supersedes,
          proposed_utc, accepted_utc, in_force_utc, retired_utc, last_verified_utc, drift_risk,
          event_seq, meta_json
        ) VALUES (?, ?, ?, ?, ?, ?, 'proposed', ?, ?, ?, ?, ?, ?, ?, 'text/markdown', NULL, ?,
          ?, NULL, NULL, NULL, NULL, ?, ?, ?)
        """,
        (
            dec_ulid,
            human_id,
            parent,
            payload["title"],
            payload["tier"],
            is_valid_decision_tier(str(payload["tier"])),
            contract_version,
            applicability_scope,
            payload.get("enforcement_mode") or enforcement_for_tier(str(payload["tier"])),
            payload.get("owner"),
            payload["body_sha"],
            payload.get("body_path"),
            int(payload["body_bytes"]),
            supersedes_ulid,
            record["occurred_utc"],
            drift_risk,
            record["event_seq"],
            json_dumps(meta),
        ),
    )
    aliases = [human_id, *payload.get("aliases", [])]
    for alias in aliases:
        _insert_alias(conn, str(alias), dec_ulid, is_primary=(alias == human_id))
    for pattern in payload.get("affected_code_globs", []):
        _insert_glob(conn, dec_ulid, str(pattern), "affected")
    for pattern in payload.get("exemptions", []):
        _insert_glob(conn, dec_ulid, str(pattern), "exempt")
    for pattern in payload.get("generated_artifact_paths", []):
        _insert_glob(conn, dec_ulid, str(pattern), "generated")
    for check in payload.get("required_checks", []):
        conn.execute(
            "INSERT OR IGNORE INTO decision_checks(dec_ulid, check_name) VALUES (?, ?)",
            (dec_ulid, str(check)),
        )
    for item in verification:
        if isinstance(item, dict):
            conn.execute(
                "INSERT OR IGNORE INTO decision_verifications("
                "dec_ulid, command, execution_mode, argv_json, expected_signal, "
                "runtime_cost, drift_risk, "
                "last_verified_utc, last_outcome, last_event_id"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)",
                (
                    dec_ulid,
                    str(item.get("command", "")),
                    str(item.get("execution_mode") or "legacy_shell"),
                    json_dumps(item.get("argv")) if item.get("argv") else None,
                    str(item.get("expected_signal", "")),
                    item.get("runtime_cost"),
                    item.get("drift_risk"),
                    item.get("last_verified_utc"),
                    item.get("last_outcome"),
                ),
            )
    raw_assumptions = payload.get("assumptions", [])
    assumptions = (
        normalize_decision_assumptions(raw_assumptions)
        if contract_version >= 5
        else _legacy_decision_assumptions(raw_assumptions)
    )
    for item in assumptions:
        conn.execute(
            "INSERT OR IGNORE INTO decision_assumptions("
            "dec_ulid, assumption_id, text, references_json, status, invalidated_event_id"
            ") VALUES (?, ?, ?, ?, 'active', NULL)",
            (dec_ulid, item["id"], item["text"], json_dumps(item["references"])),
        )
    raw_evidence = payload.get("evidence", {})
    evidence = (
        normalize_decision_evidence(raw_evidence, allow_extensions=True)
        if contract_version >= 5
        else _legacy_decision_evidence(raw_evidence)
    )
    for kind, values in evidence.items():
        for value in values:
            conn.execute(
                "INSERT OR IGNORE INTO decision_evidence("
                "dec_ulid, evidence_kind, ref_value"
                ") VALUES (?, ?, ?)",
                (dec_ulid, kind, value),
            )
    for tag in payload.get("tags", []):
        conn.execute(
            "INSERT OR IGNORE INTO decision_tags(dec_ulid, tag) VALUES (?, ?)",
            (dec_ulid, str(tag)),
        )


def _project_decision_metadata_updated(conn, record: dict[str, Any], dec_ulid: str) -> None:
    fields = record["payload"].get("fields_changed", {})
    if not isinstance(fields, dict):
        return
    contract_version = _decision_contract_version(record["payload"])
    if contract_version:
        conn.execute(
            "UPDATE decisions SET contract_version=MAX(contract_version, ?) WHERE dec_ulid=?",
            (contract_version, dec_ulid),
        )
    if "human_id" in fields:
        old_new = fields["human_id"]
        if isinstance(old_new, list) and len(old_new) == 2:
            old_id, new_id = str(old_new[0]), str(old_new[1])
            existing = resolve_decision(conn, new_id)
            if existing is not None and existing != dec_ulid:
                raise DecisionStopLine(DECISION_ALIAS_FORK, new_id)
            conn.execute("UPDATE decision_aliases SET is_primary=0 WHERE dec_ulid=?", (dec_ulid,))
            _insert_alias(conn, old_id, dec_ulid, is_primary=False)
            _insert_alias(conn, new_id, dec_ulid, is_primary=True)
            conn.execute(
                "UPDATE decisions SET human_id=?, parent_human_id=?, event_seq=? WHERE dec_ulid=?",
                (new_id, parent_human_id(new_id), record["event_seq"], dec_ulid),
            )
    simple_columns = {
        "title": "title",
        "owner": "owner",
        "applicability_scope": "applicability_scope",
        "body_sha": "body_sha",
        "body_path": "body_path",
        "body_bytes": "body_bytes",
    }
    for field_name, column_name in simple_columns.items():
        if field_name not in fields:
            continue
        new_value = _decision_changed_value(fields[field_name])
        conn.execute(
            f"UPDATE decisions SET {column_name}=?, event_seq=? WHERE dec_ulid=?",
            (new_value, record["event_seq"], dec_ulid),
        )
    if "tier" in fields:
        new_tier = str(_decision_changed_value(fields["tier"]) or "").strip()
        if not new_tier:
            raise ValueError("decision tier must not be empty")
        conn.execute(
            "UPDATE decisions SET tier=?, tier_valid=?, enforcement_mode=?, event_seq=? "
            "WHERE dec_ulid=?",
            (
                new_tier,
                is_valid_decision_tier(new_tier),
                enforcement_for_tier(new_tier),
                record["event_seq"],
                dec_ulid,
            ),
        )
    meta_fields = {
        "context",
        "decision",
        "body_format",
        "rejected_alternatives",
        "consequences",
        "review_policy",
        "exemptions",
        "generated_artifact_paths",
    }
    if meta_fields.intersection(fields) or ("body_sha" in fields and "body_format" not in fields):
        row = conn.execute(
            "SELECT meta_json FROM decisions WHERE dec_ulid=?", (dec_ulid,)
        ).fetchone()
        meta = json_loads(row["meta_json"] if row else None, {})
        if not isinstance(meta, dict):
            meta = {}
        if "body_sha" in fields and "body_format" not in fields:
            meta["body_format"] = DECISION_BODY_FORMAT_UNKNOWN
        for field_name in meta_fields.intersection(fields):
            changed_value = _decision_changed_value(fields[field_name])
            if field_name == "review_policy" and contract_version >= 5:
                normalized_review_policy = normalize_decision_review_policy(
                    changed_value, allow_extensions=True
                )
                meta[field_name] = (
                    {**changed_value, **normalized_review_policy}
                    if isinstance(changed_value, dict)
                    else normalized_review_policy
                )
            else:
                meta[field_name] = changed_value
        conn.execute(
            "UPDATE decisions SET meta_json=?, event_seq=? WHERE dec_ulid=?",
            (json_dumps(meta), record["event_seq"], dec_ulid),
        )

    collections_changed = False
    if "affected_code_globs" in fields:
        values = _decision_changed_value(fields["affected_code_globs"])
        conn.execute("DELETE FROM decision_globs WHERE dec_ulid=? AND kind='affected'", (dec_ulid,))
        for pattern in values if isinstance(values, list) else []:
            _insert_glob(conn, dec_ulid, str(pattern), "affected")
        if (
            "applicability_scope" not in fields
            and _decision_contract_version(record["payload"]) < 5
        ):
            inferred_scope = "paths" if isinstance(values, list) and values else "manual"
            conn.execute(
                "UPDATE decisions SET applicability_scope=?, event_seq=? WHERE dec_ulid=?",
                (inferred_scope, record["event_seq"], dec_ulid),
            )
        collections_changed = True
    for field_name, kind in (
        ("exemptions", "exempt"),
        ("generated_artifact_paths", "generated"),
    ):
        if field_name not in fields:
            continue
        values = _decision_changed_value(fields[field_name])
        conn.execute(
            "DELETE FROM decision_globs WHERE dec_ulid=? AND kind=?",
            (dec_ulid, kind),
        )
        for pattern in values if isinstance(values, list) else []:
            _insert_glob(conn, dec_ulid, str(pattern), kind)
        collections_changed = True
    if "required_checks" in fields:
        values = _decision_changed_value(fields["required_checks"])
        conn.execute("DELETE FROM decision_checks WHERE dec_ulid=?", (dec_ulid,))
        for check in values if isinstance(values, list) else []:
            conn.execute(
                "INSERT OR IGNORE INTO decision_checks(dec_ulid, check_name) VALUES (?, ?)",
                (dec_ulid, str(check)),
            )
        collections_changed = True
    if "verification" in fields:
        raw_values = _decision_changed_value(fields["verification"])
        values = normalize_decision_verification(raw_values if isinstance(raw_values, list) else [])
        conn.execute("DELETE FROM decision_verifications WHERE dec_ulid=?", (dec_ulid,))
        for item in values if isinstance(values, list) else []:
            if not isinstance(item, dict):
                continue
            conn.execute(
                "INSERT OR IGNORE INTO decision_verifications("
                "dec_ulid, command, execution_mode, argv_json, expected_signal, "
                "runtime_cost, drift_risk, "
                "last_verified_utc, last_outcome, last_event_id"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, NULL, NULL, NULL)",
                (
                    dec_ulid,
                    str(item.get("command", "")),
                    str(item.get("execution_mode") or "legacy_shell"),
                    json_dumps(item.get("argv")) if item.get("argv") else None,
                    str(item.get("expected_signal", "")),
                    item.get("runtime_cost"),
                    item.get("drift_risk"),
                ),
            )
        conn.execute(
            "UPDATE decisions SET last_verified_utc=NULL, drift_risk=NULL WHERE dec_ulid=?",
            (dec_ulid,),
        )
        collections_changed = True
    if "assumptions" in fields:
        raw_values = _decision_changed_value(fields["assumptions"])
        values = (
            normalize_decision_assumptions(raw_values)
            if contract_version >= 5
            else _legacy_decision_assumptions(raw_values)
        )
        conn.execute("DELETE FROM decision_assumptions WHERE dec_ulid=?", (dec_ulid,))
        for item in values:
            conn.execute(
                "INSERT INTO decision_assumptions("
                "dec_ulid, assumption_id, text, references_json, status, invalidated_event_id"
                ") VALUES (?, ?, ?, ?, 'active', NULL)",
                (
                    dec_ulid,
                    item["id"],
                    item["text"],
                    json_dumps(item["references"]),
                ),
            )
        collections_changed = True
    if "evidence" in fields:
        raw_values = _decision_changed_value(fields["evidence"])
        values = (
            normalize_decision_evidence(raw_values, allow_extensions=True)
            if contract_version >= 5
            else _legacy_decision_evidence(raw_values)
        )
        conn.execute("DELETE FROM decision_evidence WHERE dec_ulid=?", (dec_ulid,))
        for kind, references in values.items():
            for reference in references:
                conn.execute(
                    "INSERT INTO decision_evidence(dec_ulid, evidence_kind, ref_value) "
                    "VALUES (?, ?, ?)",
                    (dec_ulid, kind, reference),
                )
        collections_changed = True
    if "tags" in fields:
        values = _decision_changed_value(fields["tags"])
        conn.execute("DELETE FROM decision_tags WHERE dec_ulid=?", (dec_ulid,))
        for tag in values if isinstance(values, list) else []:
            conn.execute(
                "INSERT OR IGNORE INTO decision_tags(dec_ulid, tag) VALUES (?, ?)",
                (dec_ulid, str(tag)),
            )
        collections_changed = True
    if collections_changed:
        conn.execute(
            "UPDATE decisions SET event_seq=? WHERE dec_ulid=?",
            (record["event_seq"], dec_ulid),
        )
    if "status" in fields:
        old_new = fields["status"]
        new_status = _decision_changed_value(old_new)
        reset_approval = str(new_status) == "proposed"
        conn.execute(
            "UPDATE decisions SET status=?, "
            "accepted_utc=CASE WHEN ? THEN NULL ELSE accepted_utc END, "
            "in_force_utc=CASE WHEN ? THEN NULL ELSE COALESCE(in_force_utc, ?) END, "
            "event_seq=? "
            "WHERE dec_ulid=?",
            (
                str(new_status),
                reset_approval,
                reset_approval,
                record["occurred_utc"] if str(new_status) == "in_force" else None,
                record["event_seq"],
                dec_ulid,
            ),
        )
    revision_fields = {
        "human_id",
        "title",
        "tier",
        "applicability_scope",
        "owner",
        "context",
        "decision",
        "body_sha",
        "affected_code_globs",
        "exemptions",
        "generated_artifact_paths",
        "required_checks",
        "verification",
        "tags",
        "assumptions",
        "evidence",
        "review_policy",
    }
    if revision_fields.intersection(fields):
        row = conn.execute(
            "SELECT meta_json FROM decisions WHERE dec_ulid=?", (dec_ulid,)
        ).fetchone()
        meta = json_loads(row["meta_json"] if row else None, {})
        if not isinstance(meta, dict):
            meta = {}
        meta["author_provenance"] = _decision_revision_author_provenance(conn, record)
        conn.execute(
            "UPDATE decisions SET meta_json=?, event_seq=? WHERE dec_ulid=?",
            (json_dumps(meta), record["event_seq"], dec_ulid),
        )
    _append_decision_log(conn, dec_ulid, record)


def _decision_changed_value(value: Any) -> Any:
    if isinstance(value, list) and len(value) == 2:
        return value[1]
    return value


def _decision_revision_author_provenance(
    conn: sqlite3.Connection, record: dict[str, Any]
) -> dict[str, Any]:
    """Derive closed authorship facts from the canonical event and instance prefix."""

    actor = str(record.get("actor") or "")
    provenance: dict[str, Any] = {
        "source": "canonical_event",
        "source_event_id": str(record.get("event_id") or ""),
        "source_event_seq": int(record.get("event_seq") or 0),
        "source_event_hash": hash_event_line(canonical_json(record) + b"\n"),
        "participant": actor,
    }
    instance_id = str(record.get("actor_instance_id") or "")
    if not instance_id:
        return provenance
    instance = conn.execute(
        "SELECT participant, external_session_ref_digest, launch_attempt_digest "
        "FROM agent_instances WHERE id=?",
        (instance_id,),
    ).fetchone()
    if instance is None or str(instance["participant"] or "") != actor:
        return provenance
    provenance["instance_id"] = instance_id
    context_digest = str(instance["external_session_ref_digest"] or "") or str(
        instance["launch_attempt_digest"] or ""
    )
    if context_digest:
        provenance["context_digest"] = context_digest
    return provenance


def _project_ops_event(conn, record: dict[str, Any]) -> None:
    if record["kind"] == "decision_reference_resolved":
        payload = record["payload"]
        dec_ulid = resolve_decision(
            conn, str(payload.get("dec_ulid") or payload.get("decision_id"))
        )
        if dec_ulid:
            conn.execute(
                "INSERT OR REPLACE INTO decision_references_in_code("
                "dec_ulid, file_path, line_start, line_end, reference_form, commit_sha, scanner_run_id"
                ") VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    dec_ulid,
                    payload["file_path"],
                    int(payload["line_start"]),
                    int(payload["line_end"]),
                    payload["reference_form"],
                    payload.get("commit_sha"),
                    payload["scanner_run_id"],
                ),
            )
    _ = conn


def _insert_alias(conn, human_id: str, dec_ulid: str, *, is_primary: bool) -> None:
    existing_rows = conn.execute(
        "SELECT dec_ulid FROM decision_aliases WHERE human_id=?", (human_id,)
    ).fetchall()
    existing = {row["dec_ulid"] for row in existing_rows}
    if existing and existing != {dec_ulid}:
        raise DecisionStopLine(DECISION_ALIAS_FORK, human_id)
    conn.execute(
        "INSERT OR REPLACE INTO decision_aliases(human_id, dec_ulid, is_primary) VALUES (?, ?, ?)",
        (human_id, dec_ulid, 1 if is_primary else 0),
    )


def _insert_glob(conn, dec_ulid: str, pattern: str, kind: str) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO decision_globs(dec_ulid, pattern, kind) VALUES (?, ?, ?)",
        (dec_ulid, pattern, kind),
    )


def _ensure_supersede_target_valid(conn, dec_ulid: str) -> None:
    row = conn.execute("SELECT status FROM decisions WHERE dec_ulid=?", (dec_ulid,)).fetchone()
    if row is None or row["status"] not in {"accepted", "in_force"}:
        raise DecisionStopLine(DECISION_SUPERSEDE_TARGET_INVALID, dec_ulid)


def _ensure_no_supersede_cycle(conn, old_dec: str, new_dec: str) -> None:
    current: str | None = new_dec
    seen = {old_dec}
    while current:
        if current in seen:
            raise DecisionStopLine(DECISION_SUPERSEDE_CYCLE, f"{old_dec}->{new_dec}")
        seen.add(current)
        row = conn.execute(
            "SELECT superseded_by FROM decisions WHERE dec_ulid=?", (current,)
        ).fetchone()
        current = row["superseded_by"] if row else None


def _append_decision_log(conn, dec_ulid: str, record: dict[str, Any]) -> None:
    row = conn.execute("SELECT meta_json FROM decisions WHERE dec_ulid=?", (dec_ulid,)).fetchone()
    meta = json_loads(row["meta_json"] if row else None, {})
    log = list(meta.get("event_log", []))
    log.append(
        {
            "event_id": record["event_id"],
            "kind": record["kind"],
            "occurred_utc": record["occurred_utc"],
            "payload": record["payload"],
        }
    )
    meta["event_log"] = log
    conn.execute(
        "UPDATE decisions SET meta_json=?, event_seq=? WHERE dec_ulid=?",
        (json_dumps(meta), record["event_seq"], dec_ulid),
    )


def _decision_quorum_reached(meta: dict[str, Any], current_record: dict[str, Any]) -> bool:
    try:
        review_policy = normalize_decision_review_policy(
            meta.get("review_policy", {}), allow_extensions=True
        )
    except ValueError:
        return False
    required = review_policy.get("required_reviewers", [])
    if not required:
        return True
    quorum = int(review_policy["approval_quorum"])
    current_payload = current_record.get("payload", {})
    current_revision_sha = (
        str(current_payload.get("approved_revision_sha") or "")
        if isinstance(current_payload, dict)
        else ""
    )
    accepted_by: set[str] = set()
    for item in decision_current_revision_events(meta):
        if item.get("kind") != "decision_accepted":
            continue
        payload = item.get("payload", {})
        if not isinstance(payload, dict):
            continue
        if current_revision_sha and payload.get("approved_revision_sha") != current_revision_sha:
            continue
        accepted_by.add(str(payload.get("accepted_by") or item.get("actor", "")))
    accepted_by.add(
        str(current_record.get("payload", {}).get("accepted_by", current_record.get("actor", "")))
    )
    return len(accepted_by.intersection({str(item) for item in required})) >= quorum


def parent_human_id(human_id: str) -> str | None:
    if SECTION_REF_RE.match(human_id):
        return None
    match = DECISION_ID_RE.match(human_id)
    if not match or not match.group(2):
        return None
    return f"D{match.group(1)}"


def _max_drift_risk(values: Iterable[Any]) -> str | None:
    order = {"low": 1, "medium": 2, "high": 3}
    best: str | None = None
    for value in values:
        text = str(value) if value is not None else ""
        if text in order and (best is None or order[text] > order[best]):
            best = text
    return best


def _packet_type(body: str) -> str | None:
    if "```json" not in body:
        return None
    if "triage" in body.lower():
        return "triage"
    if "plan-ready" in body.lower():
        return "plan-ready"
    return "json"


def _infer_ref_type(ref_value: str) -> str:
    if ref_value.startswith("origin:"):
        return "origin"
    if ref_value.startswith("REQ-"):
        return "req"
    if ref_value.startswith("RES-"):
        return "res"
    if ref_value.startswith("FBK-"):
        return "feedback"
    if ref_value.startswith("BKL-"):
        return "backlog"
    if ref_value.startswith("AI-"):
        return "agent_instance"
    if re.fullmatch(r"[0-9a-f]{7,40}", ref_value):
        return "commit"
    return "unknown"


def _config_from_agent_dir(agent_dir: str | Path | None) -> AgentMeshConfig:
    if agent_dir is None:
        return load_config()
    return config_from_agent_dir(agent_dir)
