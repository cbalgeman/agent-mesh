"""Bounded subject and typed-artifact resolution for review assurance."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import stat
import time
import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, cast
from urllib.parse import urlsplit

from agent_mesh.config import AgentMeshConfig
from agent_mesh.core.decision_glob import DecisionGlobError, compile_meshglob
from agent_mesh.core.dispatch_schema import ASSURANCE_LIFECYCLE_REASON_CODES
from agent_mesh.core.git_changes import (
    GitChange,
    GitChangeMode,
    GitChangeSet,
    capture_git_snapshot_identity,
    collect_git_changes,
)
from agent_mesh.store.rebuild import decision_revision_sha_from_projection, resolve_decision
from agent_mesh.store.sqlite import json_loads


MAX_ASSURANCE_ARTIFACT_BYTES = 16 * 1024 * 1024
MAX_CHANGE_SUBJECT_FILE_BYTES = 16 * 1024 * 1024
MAX_CHANGE_SUBJECT_TOTAL_BYTES = 64 * 1024 * 1024
MAX_CHANGE_SUBJECT_ENTRIES = 512
MAX_ASSURANCE_RESOLUTION_SECONDS = 5.0
MAX_CHANGE_SUBJECT_RESOLUTION_SECONDS = 30.0
REVIEW_BEGIN = "AGENT_MESH_REVIEW_V1_BEGIN"
REVIEW_END = "AGENT_MESH_REVIEW_V1_END"
REVIEW_SCHEMA = "agent-mesh.review-response.v1"
MAX_REVIEW_ARTIFACTS = 16
_REVIEW_RE = re.compile(
    rf"(?:^|\n){re.escape(REVIEW_BEGIN)}[ \t]*\n"
    rf"(?P<payload>[^\n]+)\n{re.escape(REVIEW_END)}[ \t]*(?:\n|$)"
)
_REVIEW_DISPOSITIONS = frozenset({"GO", "CONDITIONAL", "NO-GO", "PASS", "NOT_SATISFIED"})


class AssuranceResolutionError(ValueError):
    """The requested subject or artifact could not be bound completely."""


class ReviewResponseError(ValueError):
    """The accepted RES does not contain one valid closed review envelope."""


@dataclass(frozen=True)
class ReviewResponseEnvelope:
    policy_id: str
    policy_digest: str
    subject_digest: str
    disposition: str
    finding_counts: dict[str, int]
    artifact_paths: tuple[str, ...]
    replaces_response_id: str | None = None


@dataclass(frozen=True)
class DispatchOutcome:
    """Current outcome of one frozen dispatch response slot."""

    policy_id: str
    attempt_id: str | None
    attempt_number: int
    status: str
    output_message_id: str | None
    terminal_event_seq: int | None
    gate_satisfying: bool
    reason: str


@dataclass(frozen=True)
class AssuranceGateResult:
    """One bounded explanation of whether a review assurance gate is current."""

    policy_id: str
    configured: bool
    enforcement: str
    satisfied: bool
    current_attempt_id: str | None
    qualifying_assurance_ids: tuple[str, ...]
    reason_codes: tuple[str, ...]

    @property
    def allows_transition(self) -> bool:
        return self.satisfied or self.enforcement != "blocking"


def assurance_lifecycle_for_response(
    conn: sqlite3.Connection, response_id: str
) -> dict[str, Any] | None:
    """Resolve one assurance by its public RES reference and return current reliance state."""

    row = conn.execute(
        "SELECT * FROM review_assurances WHERE originating_response_id=? "
        "AND authoritative=1 ORDER BY event_seq DESC LIMIT 1",
        (response_id,),
    ).fetchone()
    if row is None:
        return None
    return assurance_lifecycle_for_id(conn, str(row["assurance_id"]))


def assurance_lifecycle_for_id(
    conn: sqlite3.Connection, assurance_id: str
) -> dict[str, Any] | None:
    """Return the current append-only lifecycle row with public response links."""

    row = conn.execute(
        "SELECT a.*, l.lifecycle_version, l.state, l.reason_code, l.note, "
        "l.actor AS lifecycle_actor, l.occurred_utc AS lifecycle_utc, "
        "l.replacement_assurance_id, l.operation_key "
        "FROM review_assurances a JOIN review_assurance_lifecycle l "
        "ON l.assurance_id=a.assurance_id "
        "WHERE a.assurance_id=? ORDER BY l.lifecycle_version DESC LIMIT 1",
        (assurance_id,),
    ).fetchone()
    if row is None:
        return None
    replacement_response_id = ""
    if row["replacement_assurance_id"]:
        replacement = conn.execute(
            "SELECT originating_response_id FROM review_assurances WHERE assurance_id=?",
            (row["replacement_assurance_id"],),
        ).fetchone()
        if replacement is not None:
            replacement_response_id = str(replacement["originating_response_id"] or "")
    return {
        "assurance_id": str(row["assurance_id"]),
        "response_id": str(row["originating_response_id"] or ""),
        "request_id": str(row["request_id"]),
        "policy_id": str(row["policy_id"] or ""),
        "subject": json_loads(row["subject_json"], None),
        "disposition": str(row["disposition"]),
        "authoritative": bool(row["authoritative"]),
        "state": str(row["state"]),
        "version": int(row["lifecycle_version"]),
        "reason_code": str(row["reason_code"] or ""),
        "note": str(row["note"] or ""),
        "actor": str(row["lifecycle_actor"] or ""),
        "occurred_utc": str(row["lifecycle_utc"] or ""),
        "replacement_response_id": replacement_response_id,
        "operation_key": str(row["operation_key"]),
    }


def append_assurance_lifecycle_transition(
    config: AgentMeshConfig,
    *,
    response_id: str,
    actor: str,
    action: str,
    expected_version: int,
    reason_code: str,
    note: str,
    lock_acquired: bool,
) -> dict[str, Any]:
    """Append one version-bound flag or direct-human retirement under the mesh lock."""

    if action not in {"flag", "retire"}:
        raise AssuranceResolutionError("REVIEW_ASSURANCE_LIFECYCLE_ACTION_INVALID")
    if reason_code not in ASSURANCE_LIFECYCLE_REASON_CODES:
        raise AssuranceResolutionError("REVIEW_ASSURANCE_LIFECYCLE_REASON_INVALID")
    normalized_note = note.strip()
    if (
        not normalized_note
        or len(normalized_note) > 512
        or any(unicodedata.category(ch) in {"Cc", "Zl", "Zp"} for ch in normalized_note)
    ):
        raise AssuranceResolutionError("REVIEW_ASSURANCE_LIFECYCLE_NOTE_INVALID")
    if isinstance(expected_version, bool) or expected_version < 1:
        raise AssuranceResolutionError("REVIEW_ASSURANCE_LIFECYCLE_VERSION_INVALID")
    if not lock_acquired:
        raise AssuranceResolutionError("REVIEW_ASSURANCE_LIFECYCLE_LOCK_REQUIRED")
    authority = {
        "action": action,
        "response_id": response_id,
        "actor": actor,
        "expected_version": expected_version,
        "reason_code": reason_code,
        "note": normalized_note,
    }
    operation_key = hashlib.sha256(
        json.dumps(authority, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    conn = sqlite3.connect(config.db_path)
    conn.row_factory = sqlite3.Row
    try:
        existing = conn.execute(
            "SELECT assurance_id FROM review_assurance_lifecycle WHERE operation_key=?",
            (operation_key,),
        ).fetchone()
        if existing is not None:
            result = assurance_lifecycle_for_id(conn, str(existing["assurance_id"]))
            if result is None:  # pragma: no cover - projection invariant.
                raise AssuranceResolutionError("REVIEW_ASSURANCE_LIFECYCLE_STATE_MISSING")
            return {**result, "reused": True}
        current = assurance_lifecycle_for_response(conn, response_id)
        if current is None:
            raise AssuranceResolutionError(f"REVIEW_ASSURANCE_RESPONSE_UNKNOWN: {response_id}")
        if current["version"] != expected_version:
            raise AssuranceResolutionError("REVIEW_ASSURANCE_LIFECYCLE_VERSION_STALE")
        required_state = "active" if action == "flag" else "flagged"
        if current["state"] != required_state:
            raise AssuranceResolutionError(
                f"REVIEW_ASSURANCE_LIFECYCLE_TRANSITION_INVALID: {current['state']}->{action}"
            )
        if action == "retire" and actor not in config.decision_approval_identities:
            raise AssuranceResolutionError("REVIEW_ASSURANCE_HUMAN_AUTHORITY_REQUIRED")
        assurance_id = str(current["assurance_id"])
        request_id = str(current["request_id"])
    finally:
        conn.close()
    from agent_mesh.core.events import Event, append_event, generate_event_id, utc_now

    payload: dict[str, Any] = {
        "contract_version": "review-assurance-lifecycle.v1",
        "assurance_id": assurance_id,
        "response_id": response_id,
        "expected_version": expected_version,
        "next_version": expected_version + 1,
        "reason_code": reason_code,
        "note": normalized_note,
        "operation_key": operation_key,
    }
    kind = "review_assurance_flagged"
    if action == "retire":
        kind = "review_assurance_retired"
        payload.update(
            {
                "approval_authority_mode": config.decision_approval_authority_mode,
                "approval_authority_revision": config.decision_approval_authority_revision,
            }
        )
    occurred_utc = utc_now()
    append_event(
        config.events_path,
        Event(
            event_id=generate_event_id(),
            occurred_utc=occurred_utc,
            actor=actor,
            kind=kind,
            entity_id=assurance_id,
            thread_id=request_id,
            payload=payload,
        ),
        lock_acquired=True,
    )
    return {
        **current,
        "state": "flagged" if action == "flag" else "retired",
        "version": expected_version + 1,
        "reason_code": reason_code,
        "note": normalized_note,
        "actor": actor,
        "occurred_utc": occurred_utc,
        "operation_key": operation_key,
        "reused": False,
    }


def parse_review_response(body: str) -> ReviewResponseEnvelope:
    """Parse exactly one single-line JSON envelope from a bounded RES body."""

    matches = list(_REVIEW_RE.finditer(body))
    if len(matches) != 1 or body.count(REVIEW_BEGIN) != 1 or body.count(REVIEW_END) != 1:
        raise ReviewResponseError("REVIEW_RESPONSE_ENVELOPE_MISSING_OR_AMBIGUOUS")
    raw = matches[0].group("payload")
    if len(raw.encode("utf-8")) > 8 * 1024:
        raise ReviewResponseError("REVIEW_RESPONSE_ENVELOPE_TOO_LARGE")
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, UnicodeError) as exc:
        raise ReviewResponseError("REVIEW_RESPONSE_ENVELOPE_INVALID_JSON") from exc
    required = {
        "schema",
        "policy_id",
        "policy_digest",
        "subject_digest",
        "disposition",
        "finding_counts",
        "artifact_paths",
    }
    optional = {"replaces_response_id"}
    if (
        not isinstance(value, dict)
        or not required.issubset(value)
        or set(value) - required - optional
    ):
        raise ReviewResponseError("REVIEW_RESPONSE_ENVELOPE_FIELDS_INVALID")
    if value["schema"] != REVIEW_SCHEMA:
        raise ReviewResponseError("REVIEW_RESPONSE_SCHEMA_INVALID")
    for field in ("policy_digest", "subject_digest"):
        digest = value[field]
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise ReviewResponseError(f"REVIEW_RESPONSE_{field.upper()}_INVALID")
    policy_id = value["policy_id"]
    if not isinstance(policy_id, str) or not policy_id or len(policy_id) > 128:
        raise ReviewResponseError("REVIEW_RESPONSE_POLICY_ID_INVALID")
    disposition = value["disposition"]
    if disposition not in _REVIEW_DISPOSITIONS:
        raise ReviewResponseError("REVIEW_RESPONSE_DISPOSITION_INVALID")
    counts = value["finding_counts"]
    if not isinstance(counts, dict) or set(counts) != {"fatal", "material", "minor"}:
        raise ReviewResponseError("REVIEW_RESPONSE_FINDING_COUNTS_INVALID")
    normalized_counts: dict[str, int] = {}
    for field in ("fatal", "material", "minor"):
        count = counts[field]
        if isinstance(count, bool) or not isinstance(count, int) or count < 0 or count > 1_000:
            raise ReviewResponseError("REVIEW_RESPONSE_FINDING_COUNTS_INVALID")
        normalized_counts[field] = count
    artifact_paths = value["artifact_paths"]
    if not isinstance(artifact_paths, list) or len(artifact_paths) > MAX_REVIEW_ARTIFACTS:
        raise ReviewResponseError("REVIEW_RESPONSE_ARTIFACT_PATHS_INVALID")
    normalized_paths: list[str] = []
    for path in artifact_paths:
        if (
            not isinstance(path, str)
            or not path
            or len(path.encode("utf-8")) > 512
            or "\n" in path
            or "\r" in path
        ):
            raise ReviewResponseError("REVIEW_RESPONSE_ARTIFACT_PATHS_INVALID")
        normalized_paths.append(path)
    if len(set(normalized_paths)) != len(normalized_paths):
        raise ReviewResponseError("REVIEW_RESPONSE_ARTIFACT_PATHS_DUPLICATE")
    replaces_response_id = value.get("replaces_response_id")
    if replaces_response_id is not None and (
        not isinstance(replaces_response_id, str)
        or not replaces_response_id
        or len(replaces_response_id) > 128
        or any(unicodedata.category(ch) in {"Cc", "Zl", "Zp"} for ch in replaces_response_id)
    ):
        raise ReviewResponseError("REVIEW_RESPONSE_REPLACEMENT_INVALID")
    return ReviewResponseEnvelope(
        policy_id=policy_id,
        policy_digest=value["policy_digest"],
        subject_digest=value["subject_digest"],
        disposition=disposition,
        finding_counts=normalized_counts,
        artifact_paths=tuple(normalized_paths),
        replaces_response_id=replaces_response_id,
    )


@dataclass(frozen=True)
class StableFile:
    sha256: str
    byte_size: int
    media_type: str


def resolve_decision_subject(
    conn,
    identifier: str,
) -> dict[str, Any]:
    dec_ulid = resolve_decision(conn, identifier)
    if dec_ulid is None:
        raise AssuranceResolutionError(f"ASSURANCE_DECISION_UNKNOWN: {identifier}")
    row = conn.execute("SELECT * FROM decisions WHERE dec_ulid=?", (dec_ulid,)).fetchone()
    if row is None:
        raise AssuranceResolutionError(f"ASSURANCE_DECISION_UNKNOWN: {identifier}")
    revision = decision_revision_sha_from_projection(conn, row)
    binding: dict[str, Any] = {
        "type": "decision_revision",
        "stable_id": str(row["human_id"]),
        "digest": revision,
        "revision": revision,
        "privacy_class": "project_private",
    }
    meta = json_loads(row["meta_json"], {})
    author_provenance = meta.get("author_provenance") if isinstance(meta, dict) else None
    if isinstance(author_provenance, dict):
        binding["author_provenance"] = author_provenance
    return binding


def resolve_artifact_subject(
    config: AgentMeshConfig,
    location: str,
    *,
    media_type: str = "text/markdown",
) -> dict[str, Any]:
    relative, candidate = _authorized_repository_path(config, location)
    stable = _stable_file_at(
        config.project_root,
        candidate,
        max_bytes=MAX_ASSURANCE_ARTIFACT_BYTES,
        deadline_monotonic=time.monotonic() + MAX_ASSURANCE_RESOLUTION_SECONDS,
    )
    binding = {
        "type": "artifact",
        "stable_id": relative,
        "digest": stable.sha256,
        "revision": "",
        "privacy_class": "project_private",
    }
    return binding


def resolve_change_set_subject(
    repo_root: Path,
    change_set: GitChangeSet,
    *,
    deadline_monotonic: float | None = None,
) -> dict[str, Any]:
    """Bind Git object commitments plus stable no-follow worktree content."""

    root = repo_root.resolve()
    deadline = (
        deadline_monotonic
        if deadline_monotonic is not None
        else time.monotonic() + MAX_ASSURANCE_RESOLUTION_SECONDS
    )
    if len(change_set.changes) > MAX_CHANGE_SUBJECT_ENTRIES:
        raise AssuranceResolutionError("ASSURANCE_CHANGE_SET_TOO_MANY_ENTRIES")
    total_bytes = 0
    entries: list[dict[str, Any]] = []
    worktree_cache: dict[str, StableFile] = {}
    for change in change_set.changes:
        _check_deadline(deadline)
        if change.status[:1] in {"U", "X", "B"} or change.change_kind == "conflicted":
            raise AssuranceResolutionError(f"ASSURANCE_CHANGE_SET_CONFLICTED: {change.path}")
        old_commitment = _object_commitment(change.old_oid, change.old_path_type)
        new_commitment = _object_commitment(change.new_oid, change.new_path_type)
        if _needs_worktree_commitment(change):
            candidate_path = change.new_path or change.path
            stable = worktree_cache.get(candidate_path)
            if stable is None:
                stable = _stable_file_at(
                    root,
                    Path(candidate_path),
                    max_bytes=MAX_CHANGE_SUBJECT_FILE_BYTES,
                    allow_symlink=True,
                    deadline_monotonic=deadline,
                )
                total_bytes += stable.byte_size
                if total_bytes > MAX_CHANGE_SUBJECT_TOTAL_BYTES:
                    raise AssuranceResolutionError("ASSURANCE_CHANGE_SET_TOTAL_BYTES_EXCEEDED")
                worktree_cache[candidate_path] = stable
            new_commitment = f"sha256:{stable.sha256}"
        entries.append(
            {
                "path": change.path,
                "comparison": change.comparison,
                "change_kind": change.change_kind,
                "status": change.status,
                "old_path": change.old_path,
                "new_path": change.new_path,
                "old_mode": change.old_mode,
                "new_mode": change.new_mode,
                "old_content": old_commitment,
                "new_content": new_commitment,
            }
        )
    envelope = {
        "schema": "agent-mesh.change-subject.v1",
        "mode": change_set.mode,
        "base": change_set.base,
        "base_oid": change_set.base_oid,
        "head_oid": change_set.head_oid,
        "merge_base": change_set.merge_base,
        "changes": entries,
    }
    digest = hashlib.sha256(
        json.dumps(envelope, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    binding = {
        "type": "change_set",
        "stable_id": f"git:{change_set.mode}",
        "digest": digest,
        "revision": change_set.head_oid or "",
        "privacy_class": "project_private",
        "resolution": {
            "kind": "git_changes_v1",
            "mode": change_set.mode,
            "base": change_set.base or "",
            "base_oid": change_set.base_oid or "",
            "head_oid": change_set.head_oid or "",
            "merge_base": change_set.merge_base or "",
            "entries": entries,
        },
    }
    return binding


def resolve_current_change_set_subject(
    repo_root: Path,
    *,
    mode: GitChangeMode,
    base: str | None = None,
) -> dict[str, Any]:
    """Resolve one change subject while proving HEAD/index stability end to end."""

    root = repo_root.resolve()
    deadline = time.monotonic() + MAX_CHANGE_SUBJECT_RESOLUTION_SECONDS
    before = capture_git_snapshot_identity(root, deadline_monotonic=deadline)
    changes = collect_git_changes(
        root,
        mode=mode,
        base=base,
        deadline_monotonic=deadline,
    )
    subject = resolve_change_set_subject(
        root,
        changes,
        deadline_monotonic=deadline,
    )
    second_changes = collect_git_changes(
        root,
        mode=mode,
        base=base,
        deadline_monotonic=deadline,
    )
    second_subject = resolve_change_set_subject(
        root,
        second_changes,
        deadline_monotonic=deadline,
    )
    after = capture_git_snapshot_identity(root, deadline_monotonic=deadline)
    if before != after or subject != second_subject:
        raise AssuranceResolutionError("ASSURANCE_CHANGE_SET_SNAPSHOT_CHANGED")
    return subject


def resolve_typed_artifact_ref(
    config: AgentMeshConfig,
    *,
    location_type: str,
    location: str,
    media_type: str,
    revision: str,
    visibility: str,
    provenance: str,
    subject_digest: str,
    deadline_monotonic: float | None = None,
) -> dict[str, Any]:
    """Resolve one reference without fetching or granting access to its target."""

    if location_type == "repository_path":
        relative, candidate = _authorized_repository_path(config, location)
        stable = _stable_file_at(
            config.project_root,
            candidate,
            max_bytes=MAX_ASSURANCE_ARTIFACT_BYTES,
            deadline_monotonic=(
                deadline_monotonic
                if deadline_monotonic is not None
                else time.monotonic() + MAX_ASSURANCE_RESOLUTION_SECONDS
            ),
        )
        resolved_location = relative
        sha256 = stable.sha256
        byte_size = stable.byte_size
    elif location_type == "uri":
        parsed = urlsplit(location)
        if (
            not parsed.scheme
            or parsed.scheme.lower() not in config.review_assurance.authorized_uri_schemes
        ):
            raise AssuranceResolutionError("ASSURANCE_ARTIFACT_URI_SCHEME_UNAUTHORIZED")
        if parsed.username is not None or parsed.password is not None:
            raise AssuranceResolutionError("ASSURANCE_ARTIFACT_URI_CREDENTIALS_FORBIDDEN")
        raise AssuranceResolutionError(
            "ASSURANCE_ARTIFACT_URI_UNVERIFIED: URI presence never triggers fetching"
        )
    else:
        raise AssuranceResolutionError("ASSURANCE_ARTIFACT_LOCATION_TYPE_INVALID")
    return {
        "location_type": location_type,
        "location": resolved_location,
        "sha256": sha256,
        "byte_size": byte_size,
        "media_type": media_type,
        "revision": revision,
        "visibility": visibility,
        "provenance": provenance,
        "subject_digest": subject_digest,
        "field_privacy": {
            "location": "project_private",
            "sha256": "project_private",
            "revision": "project_private",
            "subject_digest": "project_private",
        },
    }


def validate_typed_artifact_refs(
    config: AgentMeshConfig,
    references: Iterable[dict[str, Any]],
    *,
    artifact_contract: dict[str, Any] | None = None,
    expected_subject_digest: str = "",
    expected_provenance: str = "",
    deadline_monotonic: float | None = None,
) -> None:
    items = list(references)
    if len(items) > 16:
        raise AssuranceResolutionError("ASSURANCE_ARTIFACT_COUNT_EXCEEDED")
    deadline = (
        deadline_monotonic
        if deadline_monotonic is not None
        else time.monotonic() + MAX_ASSURANCE_RESOLUTION_SECONDS
    )
    total_bytes = 0
    for item in items:
        _check_deadline(deadline)
        resolved = resolve_typed_artifact_ref(
            config,
            location_type=str(item.get("location_type") or ""),
            location=str(item.get("location") or ""),
            media_type=str(item.get("media_type") or ""),
            revision=str(item.get("revision") or ""),
            visibility=str(item.get("visibility") or ""),
            provenance=str(item.get("provenance") or ""),
            subject_digest=str(item.get("subject_digest") or ""),
            deadline_monotonic=deadline,
        )
        for field in (
            "location",
            "sha256",
            "byte_size",
            "media_type",
            "revision",
            "visibility",
            "provenance",
            "subject_digest",
            "field_privacy",
        ):
            if item.get(field) != resolved[field]:
                raise AssuranceResolutionError(f"ASSURANCE_ARTIFACT_DIGEST_MISMATCH: {field}")
        if expected_subject_digest and item.get("subject_digest") != expected_subject_digest:
            raise AssuranceResolutionError("ASSURANCE_ARTIFACT_SUBJECT_MISMATCH")
        if expected_provenance and item.get("provenance") != expected_provenance:
            raise AssuranceResolutionError("ASSURANCE_ARTIFACT_PROVENANCE_MISMATCH")
        total_bytes += int(resolved["byte_size"])
        if artifact_contract is not None:
            root = str(artifact_contract.get("root") or "").rstrip("/")
            location = str(resolved["location"])
            if not root or not (location == root or location.startswith(root + "/")):
                raise AssuranceResolutionError("ASSURANCE_ARTIFACT_CONTRACT_ROOT_MISMATCH")
            media_types = artifact_contract.get("media_types")
            if not isinstance(media_types, list) or resolved["media_type"] not in media_types:
                raise AssuranceResolutionError("ASSURANCE_ARTIFACT_MEDIA_TYPE_MISMATCH")
            if resolved["visibility"] != artifact_contract.get("visibility"):
                raise AssuranceResolutionError("ASSURANCE_ARTIFACT_VISIBILITY_MISMATCH")
            if total_bytes > int(artifact_contract.get("max_bytes") or 0):
                raise AssuranceResolutionError("ASSURANCE_ARTIFACT_TOTAL_BYTES_EXCEEDED")


def _authorized_repository_path(config: AgentMeshConfig, location: str) -> tuple[str, Path]:
    raw = Path(location)
    if raw.is_absolute() or not raw.parts or ".." in raw.parts:
        raise AssuranceResolutionError("ASSURANCE_ARTIFACT_PATH_INVALID")
    relative = raw.as_posix()
    allowed = False
    for root_value in config.review_assurance.artifact_roots:
        root = Path(root_value).as_posix().rstrip("/")
        if relative == root or relative.startswith(root + "/"):
            allowed = True
            break
    if not allowed:
        raise AssuranceResolutionError("ASSURANCE_ARTIFACT_PATH_UNAUTHORIZED")
    return relative, raw


def _stable_file_at(
    root: Path,
    relative: Path,
    *,
    max_bytes: int,
    allow_symlink: bool = False,
    deadline_monotonic: float,
) -> StableFile:
    """Hash one repository-relative file without following any path component."""

    if relative.is_absolute() or not relative.parts or ".." in relative.parts:
        raise AssuranceResolutionError("ASSURANCE_ARTIFACT_PATH_INVALID")
    directory_flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        directory_flags |= os.O_DIRECTORY
    if hasattr(os, "O_NOFOLLOW"):
        directory_flags |= os.O_NOFOLLOW
    try:
        directory_fd = os.open(root, directory_flags)
    except OSError as exc:
        raise AssuranceResolutionError("ASSURANCE_ARTIFACT_ROOT_UNREADABLE") from exc
    try:
        for component in relative.parts[:-1]:
            _check_deadline(deadline_monotonic)
            try:
                child_fd = os.open(component, directory_flags, dir_fd=directory_fd)
            except OSError as exc:
                raise AssuranceResolutionError(
                    "ASSURANCE_ARTIFACT_PARENT_SYMLINK_OR_UNREADABLE"
                ) from exc
            os.close(directory_fd)
            directory_fd = child_fd
        name = relative.parts[-1]
        try:
            before = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        except OSError as exc:
            raise AssuranceResolutionError("ASSURANCE_ARTIFACT_UNREADABLE") from exc
        if stat.S_ISLNK(before.st_mode):
            if not allow_symlink:
                raise AssuranceResolutionError("ASSURANCE_ARTIFACT_SYMLINK_FORBIDDEN")
            try:
                raw = os.readlink(name, dir_fd=directory_fd).encode("utf-8")
                after = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            except (OSError, UnicodeEncodeError) as exc:
                raise AssuranceResolutionError("ASSURANCE_ARTIFACT_SYMLINK_UNREADABLE") from exc
            if _stat_identity(before) != _stat_identity(after):
                raise AssuranceResolutionError("ASSURANCE_ARTIFACT_CHANGED")
            if len(raw) > max_bytes:
                raise AssuranceResolutionError("ASSURANCE_ARTIFACT_TOO_LARGE")
            return StableFile(hashlib.sha256(raw).hexdigest(), len(raw), "inode/symlink")
        if not stat.S_ISREG(before.st_mode):
            raise AssuranceResolutionError("ASSURANCE_ARTIFACT_NOT_REGULAR")
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(name, flags, dir_fd=directory_fd)
        except OSError as exc:
            raise AssuranceResolutionError("ASSURANCE_ARTIFACT_UNREADABLE") from exc
        try:
            return _stable_open_descriptor(
                descriptor,
                before=before,
                max_bytes=max_bytes,
                deadline_monotonic=deadline_monotonic,
            )
        finally:
            os.close(descriptor)
    finally:
        os.close(directory_fd)


def _stable_open_descriptor(
    descriptor: int,
    *,
    before: os.stat_result,
    max_bytes: int,
    deadline_monotonic: float,
) -> StableFile:
    opened = os.fstat(descriptor)
    if _stat_identity(before) != _stat_identity(opened):
        raise AssuranceResolutionError("ASSURANCE_ARTIFACT_CHANGED")
    if opened.st_size > max_bytes:
        raise AssuranceResolutionError("ASSURANCE_ARTIFACT_TOO_LARGE")
    digest = hashlib.sha256()
    byte_size = 0
    while True:
        _check_deadline(deadline_monotonic)
        chunk = os.read(descriptor, min(1024 * 1024, max_bytes + 1 - byte_size))
        if not chunk:
            break
        byte_size += len(chunk)
        if byte_size > max_bytes:
            raise AssuranceResolutionError("ASSURANCE_ARTIFACT_TOO_LARGE")
        digest.update(chunk)
    after = os.fstat(descriptor)
    if _stat_identity(before) != _stat_identity(after):
        raise AssuranceResolutionError("ASSURANCE_ARTIFACT_CHANGED")
    return StableFile(digest.hexdigest(), byte_size, "application/octet-stream")


def _check_deadline(deadline_monotonic: float) -> None:
    if time.monotonic() >= deadline_monotonic:
        raise AssuranceResolutionError("ASSURANCE_ARTIFACT_DEADLINE_EXCEEDED")


def _stat_identity(value: os.stat_result) -> tuple[int, int, int, int, int, int]:
    return (
        int(value.st_dev),
        int(value.st_ino),
        int(value.st_mode),
        int(value.st_size),
        int(value.st_mtime_ns),
        int(value.st_ctime_ns),
    )


def _object_commitment(oid: str, path_type: str) -> str:
    if oid and set(oid) != {"0"}:
        return f"git:{oid}"
    if path_type == "missing":
        return "missing"
    return "unavailable"


def _needs_worktree_commitment(change: GitChange) -> bool:
    if change.new_path_type == "missing" or change.change_kind in {"deleted", "renamed_from"}:
        return False
    return change.comparison in {"worktree", "index_to_worktree", "untracked"} and (
        not change.new_oid or set(change.new_oid) == {"0"}
    )


def current_dispatch_outcome(conn: sqlite3.Connection, policy_id: str) -> DispatchOutcome:
    """Return the current per-slot outcome; an active retry suspends older success."""

    row = conn.execute(
        "SELECT run_id, attempt_number, status, output_message_id, event_seq "
        "FROM dispatch_runs WHERE policy_id=? "
        "ORDER BY attempt_number DESC, event_seq DESC LIMIT 1",
        (policy_id,),
    ).fetchone()
    if row is None:
        return DispatchOutcome(policy_id, None, 0, "missing", None, None, False, "no_attempt")
    status = str(row["status"])
    output_message_id = str(row["output_message_id"] or "") or None
    satisfying = status == "completed" and output_message_id is not None
    reason = (
        "completed_with_bound_response"
        if satisfying
        else "active_attempt"
        if status in {"planned", "started"}
        else "terminal_without_bound_response"
    )
    return DispatchOutcome(
        policy_id=policy_id,
        attempt_id=str(row["run_id"]),
        attempt_number=int(row["attempt_number"] or 0),
        status=status,
        output_message_id=output_message_id,
        terminal_event_seq=(
            int(row["event_seq"])
            if status in {"completed", "failed", "cancelled", "timed_out", "parent_lost"}
            else None
        ),
        gate_satisfying=satisfying,
        reason=reason,
    )


def evaluate_transition_assurance(
    config: AgentMeshConfig,
    conn: sqlite3.Connection,
    *,
    boundary_kind: str,
    boundary_key: str,
    policy_id: str = "",
    now_utc: str | None = None,
) -> AssuranceGateResult:
    """Resolve exactly one covered policy for a concrete transition boundary."""

    if not _boundary_is_covered(config, boundary_kind, boundary_key):
        return AssuranceGateResult(policy_id, False, "advisory", True, None, (), ("not_covered",))
    cohorts: dict[tuple[str, str, str], list[str]] = {}
    rows = conn.execute(
        "SELECT policy_id, request_id, subject_json, assurance_policy_json "
        "FROM dispatch_policies "
        "ORDER BY event_seq"
    ).fetchall()
    for row in rows:
        candidate_id = str(row["policy_id"])
        assurance = json_loads(row["assurance_policy_json"], None)
        subject = json_loads(row["subject_json"], None)
        if not isinstance(assurance, dict) or not isinstance(subject, dict):
            continue
        coverage = assurance.get("coverage")
        if not isinstance(coverage, dict) or coverage.get("kind") != boundary_kind:
            continue
        coverage_key = str(coverage.get("key") or "")
        if boundary_kind == "path":
            try:
                if not compile_meshglob(coverage_key).matches(boundary_key):
                    continue
            except DecisionGlobError:
                continue
        elif coverage_key != boundary_key:
            continue
        if boundary_kind == "decision" and (
            subject.get("type") != "decision_revision" or subject.get("stable_id") != boundary_key
        ):
            continue
        if boundary_kind == "path":
            resolution = subject.get("resolution")
            entries = resolution.get("entries") if isinstance(resolution, dict) else None
            if subject.get("type") != "change_set" or not isinstance(entries, list):
                continue
            paths = {str(entry.get("path") or "") for entry in entries if isinstance(entry, dict)}
            if boundary_key not in paths:
                continue
        if boundary_kind in {"backlog_transition", "release"} and subject.get("type") not in {
            "decision_revision",
            "change_set",
            "artifact",
        }:
            continue
        cohort_key = (
            str(row["request_id"]),
            str(row["subject_json"]),
            str(row["assurance_policy_json"]),
        )
        cohorts.setdefault(cohort_key, []).append(candidate_id)
    if policy_id:
        cohorts = {
            key: candidate_ids
            for key, candidate_ids in cohorts.items()
            if policy_id in candidate_ids
        }
    if len(cohorts) != 1:
        reason = "coverage_policy_missing" if not cohorts else "coverage_policy_ambiguous"
        return AssuranceGateResult(
            policy_id,
            True,
            config.review_assurance.enforcement,
            False,
            None,
            (),
            (reason,),
        )
    candidate_ids = next(iter(cohorts.values()))
    anchor_policy_id = policy_id or candidate_ids[0]
    return evaluate_review_assurance(
        config,
        conn,
        anchor_policy_id,
        now_utc=now_utc,
    )


def _boundary_is_covered(config: AgentMeshConfig, boundary_kind: str, boundary_key: str) -> bool:
    review = config.review_assurance
    if boundary_kind == "decision":
        return boundary_key in review.covered_decisions
    if boundary_kind == "backlog_transition":
        return boundary_key in review.backlog_transitions
    if boundary_kind == "release":
        return boundary_key in review.release_gates
    if boundary_kind == "path":
        try:
            return any(
                compile_meshglob(pattern).matches(boundary_key) for pattern in review.path_globs
            )
        except DecisionGlobError:
            return False
    return False


def evaluate_review_assurance(
    config: AgentMeshConfig,
    conn: sqlite3.Connection,
    policy_id: str,
    *,
    now_utc: str | None = None,
    validate_live_subject: bool = True,
    validate_live_artifacts: bool = True,
) -> AssuranceGateResult:
    """Evaluate current response, subject, artifact, disposition, and quorum evidence."""

    policy = conn.execute(
        "SELECT subject_json, assurance_policy_json, artifact_contract_json, "
        "runtime_profile_revision_json FROM dispatch_policies WHERE policy_id=?",
        (policy_id,),
    ).fetchone()
    if policy is None:
        return AssuranceGateResult(
            policy_id, False, "blocking", False, None, (), ("policy_missing",)
        )
    assurance_policy = json_loads(policy["assurance_policy_json"], None)
    if not isinstance(assurance_policy, dict):
        return AssuranceGateResult(
            policy_id, False, "advisory", True, None, (), ("not_configured",)
        )
    enforcement = str(assurance_policy.get("enforcement") or "advisory")
    outcome = current_dispatch_outcome(conn, policy_id)
    subject = json_loads(policy["subject_json"], None)
    try:
        subject_current = (
            _subject_is_current(config, conn, subject)
            if validate_live_subject
            else isinstance(subject, dict)
        )
    except (AssuranceResolutionError, ValueError, OSError):
        subject_current = False
    if not subject_current:
        return AssuranceGateResult(
            policy_id,
            True,
            enforcement,
            False,
            outcome.attempt_id,
            (),
            ("subject_stale",),
        )

    now = _parse_assurance_utc(now_utc) if now_utc else datetime.now(UTC)
    sibling_policies = conn.execute(
        "SELECT policy_id FROM dispatch_policies WHERE request_id=("
        "SELECT request_id FROM dispatch_policies WHERE policy_id=?"
        ") AND subject_json=? AND assurance_policy_json=? ORDER BY event_seq",
        (
            policy_id,
            str(policy["subject_json"]),
            str(policy["assurance_policy_json"]),
        ),
    ).fetchall()
    rows: list[sqlite3.Row] = []
    reasons: set[str] = set()
    for sibling in sibling_policies:
        sibling_id = str(sibling["policy_id"])
        sibling_outcome = current_dispatch_outcome(conn, sibling_id)
        if not sibling_outcome.gate_satisfying or not sibling_outcome.attempt_id:
            reasons.add("response_slot_unsatisfied")
            continue
        capability_reason = _current_capability_reason(conn, sibling_id, sibling_outcome.attempt_id)
        if capability_reason:
            reasons.add(capability_reason)
            continue
        rows.extend(
            conn.execute(
                "SELECT * FROM review_assurances WHERE policy_id=? AND attempt_id=? "
                "AND authoritative=1 ORDER BY event_seq",
                (sibling_id, sibling_outcome.attempt_id),
            ).fetchall()
        )
    rows.sort(key=lambda row: int(row["event_seq"]))
    qualifying: list[str] = []
    reviewers: set[tuple[str, str, str]] = set()
    for row in rows:
        lifecycle = assurance_lifecycle_for_id(conn, str(row["assurance_id"]))
        if lifecycle is None:
            reasons.add("assurance_lifecycle_missing")
            continue
        if lifecycle["state"] != "active":
            reasons.add(f"assurance_{lifecycle['state']}")
            continue
        if str(row["disposition"]) not in {"GO", "PASS"}:
            reasons.add("non_passing_disposition")
            continue
        try:
            if _parse_assurance_utc(str(row["valid_until_utc"])) <= now:
                reasons.add("assurance_expired")
                continue
        except ValueError:
            reasons.add("assurance_time_invalid")
            continue
        refs = _artifact_rows(conn, str(row["assurance_id"]))
        row_policy = conn.execute(
            "SELECT artifact_contract_json FROM dispatch_policies WHERE policy_id=?",
            (str(row["policy_id"]),),
        ).fetchone()
        artifact_contract = (
            json_loads(row_policy["artifact_contract_json"], None)
            if row_policy is not None
            else None
        )
        if validate_live_artifacts:
            try:
                validate_typed_artifact_refs(
                    config,
                    refs,
                    artifact_contract=(
                        artifact_contract if isinstance(artifact_contract, dict) else None
                    ),
                    expected_subject_digest=str(subject.get("digest") or ""),
                    expected_provenance=str(row["management_level"]),
                )
            except AssuranceResolutionError:
                reasons.add("artifact_stale")
                continue
        reviewer = json_loads(row["reviewer_json"], {})
        independence_class = str(reviewer.get("independence_class") or "")
        required_independence = str(assurance_policy.get("independence") or "")
        independence_ok = {
            "distinct_instance": independence_class
            in {"distinct_instance", "distinct_instance_and_context"},
            "distinct_context": independence_class
            in {"distinct_context", "distinct_instance_and_context"},
            "distinct_instance_and_context": independence_class == "distinct_instance_and_context",
        }.get(required_independence, False)
        if not independence_ok:
            reasons.add("independence_unproven")
            continue
        key = (
            str(reviewer.get("participant") or ""),
            str(reviewer.get("instance_id") or ""),
            str(reviewer.get("context_digest") or ""),
        )
        if key in reviewers:
            reasons.add("duplicate_reviewer")
            continue
        reviewers.add(key)
        qualifying.append(str(row["assurance_id"]))

    quorum = int(assurance_policy.get("quorum") or 1)
    if len(qualifying) < quorum:
        reasons.add("quorum_unsatisfied")
    satisfied = len(qualifying) >= quorum
    if satisfied:
        reasons = {"satisfied"}
    elif not rows:
        reasons.add("assurance_missing")
    return AssuranceGateResult(
        policy_id,
        True,
        enforcement,
        satisfied,
        outcome.attempt_id,
        tuple(qualifying),
        tuple(sorted(reasons)),
    )


def assurance_gate_binding(
    gate: AssuranceGateResult,
    *,
    boundary_kind: str,
    boundary_key: str,
    gate_evaluated_utc: str,
) -> dict[str, Any]:
    """Return the exact canonical evidence commitment for one transition gate."""

    authority: dict[str, Any] = {
        "schema": "agent-mesh.review-gate.v1",
        "boundary_kind": boundary_kind,
        "boundary_key": boundary_key,
        "gate_evaluated_utc": gate_evaluated_utc,
        "policy_id": gate.policy_id,
        "enforcement": gate.enforcement,
        "satisfied": gate.satisfied,
        "current_attempt_id": gate.current_attempt_id or "",
        "qualifying_assurance_ids": list(gate.qualifying_assurance_ids),
        "reason_codes": list(gate.reason_codes),
    }
    digest = hashlib.sha256(
        json.dumps(authority, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    return {**authority, "evidence_digest": digest}


def validate_assurance_gate_binding(
    config: AgentMeshConfig,
    conn: sqlite3.Connection,
    binding: Any,
    *,
    boundary_kind: str,
    boundary_key: str,
) -> None:
    """Recompute a historical gate from its canonical prefix and exact event time."""

    if not isinstance(binding, dict) or binding.get("schema") != "agent-mesh.review-gate.v1":
        raise AssuranceResolutionError("REVIEW_ASSURANCE_GATE_BINDING_INVALID")
    if binding.get("boundary_kind") != boundary_kind or binding.get("boundary_key") != boundary_key:
        raise AssuranceResolutionError("REVIEW_ASSURANCE_GATE_BOUNDARY_MISMATCH")
    gate_evaluated_utc = str(binding.get("gate_evaluated_utc") or "")
    try:
        _parse_assurance_utc(gate_evaluated_utc)
    except ValueError as exc:
        raise AssuranceResolutionError("REVIEW_ASSURANCE_GATE_TIME_INVALID") from exc
    policy_id = str(binding.get("policy_id") or "")
    policy = conn.execute(
        "SELECT assurance_policy_json FROM dispatch_policies WHERE policy_id=?",
        (policy_id,),
    ).fetchone()
    assurance = json_loads(policy["assurance_policy_json"], None) if policy is not None else None
    coverage = assurance.get("coverage") if isinstance(assurance, dict) else None
    if not isinstance(coverage, dict) or coverage != {
        "kind": boundary_kind,
        "key": boundary_key,
    }:
        raise AssuranceResolutionError("REVIEW_ASSURANCE_GATE_POLICY_MISMATCH")
    gate = evaluate_review_assurance(
        config,
        conn,
        policy_id,
        now_utc=gate_evaluated_utc,
        validate_live_subject=False,
        validate_live_artifacts=False,
    )
    expected = assurance_gate_binding(
        gate,
        boundary_kind=boundary_kind,
        boundary_key=boundary_key,
        gate_evaluated_utc=gate_evaluated_utc,
    )
    if expected != binding:
        raise AssuranceResolutionError("REVIEW_ASSURANCE_GATE_EVIDENCE_MISMATCH")


def _current_capability_reason(
    conn: sqlite3.Connection,
    policy_id: str,
    attempt_id: str,
) -> str:
    row = conn.execute(
        "SELECT r.capability_receipt_json, p.required_capabilities_json, "
        "p.assurance_policy_json, p.runtime_profile_revision_json "
        "FROM dispatch_runs r JOIN dispatch_policies p ON p.policy_id=r.policy_id "
        "WHERE r.run_id=? AND r.policy_id=?",
        (attempt_id, policy_id),
    ).fetchone()
    if row is None:
        return "capability_receipt_missing"
    receipt = json_loads(row["capability_receipt_json"], None)
    required = json_loads(row["required_capabilities_json"], [])
    profile = json_loads(row["runtime_profile_revision_json"], None)
    assurance = json_loads(row["assurance_policy_json"], None)
    if not isinstance(receipt, dict) or not isinstance(profile, dict):
        return "capability_receipt_invalid"
    if receipt.get("profile_digest") != profile.get("digest"):
        return "capability_profile_stale"
    results = receipt.get("results")
    if not isinstance(results, list) or not isinstance(required, list):
        return "capability_receipt_invalid"
    by_capability = {
        str(item.get("capability") or ""): item for item in results if isinstance(item, dict)
    }
    if set(by_capability) != {str(item) for item in required}:
        return "capability_set_mismatch"
    blocking = isinstance(assurance, dict) and assurance.get("enforcement") == "blocking"
    for item in by_capability.values():
        if item.get("declared") is not True or item.get("effective") is not True:
            return "capability_unproven"
        if blocking and item.get("evidence_class") not in {
            "generic_host",
            "built_in_driver",
        }:
            return "capability_untrusted"
    return ""


def _subject_is_current(
    config: AgentMeshConfig,
    conn: sqlite3.Connection,
    subject: Any,
) -> bool:
    if not isinstance(subject, dict) or subject.get("type") == "unbound":
        return False
    subject_type = str(subject.get("type") or "")
    if subject_type == "decision_revision":
        current = resolve_decision_subject(conn, str(subject.get("stable_id") or ""))
    elif subject_type == "artifact":
        current = resolve_artifact_subject(config, str(subject.get("stable_id") or ""))
    elif subject_type == "change_set":
        resolution = subject.get("resolution")
        if not isinstance(resolution, dict) or resolution.get("kind") != "git_changes_v1":
            return False
        mode = str(resolution.get("mode") or "")
        base = str(resolution.get("base") or "") or None
        current = resolve_current_change_set_subject(
            config.project_root,
            mode=cast(GitChangeMode, mode),
            base=base,
        )
    else:
        return False
    return current == subject


def _artifact_rows(conn: sqlite3.Connection, assurance_id: str) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM assurance_artifact_refs WHERE assurance_id=? ORDER BY ref_index",
        (assurance_id,),
    ).fetchall()
    return [
        {
            "location_type": str(row["location_type"]),
            "location": str(row["location"]),
            "sha256": str(row["sha256"]),
            "byte_size": int(row["byte_size"]),
            "media_type": str(row["media_type"]),
            "revision": str(row["revision"]),
            "visibility": str(row["visibility"]),
            "provenance": str(row["provenance"]),
            "subject_digest": str(row["subject_digest"]),
            "field_privacy": json.loads(str(row["field_privacy_json"])),
        }
        for row in rows
    ]


def _parse_assurance_utc(value: str | None) -> datetime:
    raw = str(value or "")
    if not raw.endswith("Z"):
        raise ValueError("timestamp must be UTC")
    parsed = datetime.fromisoformat(raw[:-1] + "+00:00")
    if parsed.tzinfo is None:
        raise ValueError("timestamp must be timezone-aware")
    return parsed.astimezone(UTC)
