"""Bounded, provider-neutral canonical context bootstrap and freshness validation."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable

from agent_mesh.adoption import (
    CONTRACT_BODY,
    CONTRACT_TARGETS,
    CONTRACT_VERSION,
    contract_digest,
    contract_text_status,
)
from agent_mesh.config import LEGACY_STORE_ID_RE, STORE_ID_RE, AgentMeshConfig
from agent_mesh.core.hashing import SENTINEL_PREV_HASH
from agent_mesh.store.read_model import CanonicalEventSnapshot


CONTEXT_BOOTSTRAP_SCHEMA = "agent-mesh.context-bootstrap.v1"
FRESHNESS_CURSOR_SCHEMA = "agent-mesh.context-bootstrap-cursor.v1"
FRESHNESS_CURSOR_DOMAIN = "agent-mesh.context-bootstrap.freshness"
DELIVERY_REPORT_SCHEMA = "agent-mesh.context-delivery-report.v1"
DELIVERY_VERIFICATION_SCHEMA = "agent-mesh.context-delivery-verification.v1"
LIFECYCLE_MAPPING_SCHEMA = "agent-mesh.context-delivery-mapping.v1"

DELIVERY_CAPABILITIES = (
    "session_bootstrap",
    "task_preflight",
    "pre_edit",
    "pre_write",
    "resume_refresh",
    "compaction_refresh",
    "reference_resolution",
    "change_review",
)
DELIVERY_STATES = ("unreported", "unsupported", "reported", "verified")
VERIFICATION_SCOPES = ("schema_fixture", "installed_vertical")

MAX_CONTEXT_BOOTSTRAP_JSON_BYTES = 256 * 1024
MAX_CONTEXT_SNAPSHOT_BYTES = 64 * 1024 * 1024
MAX_CONTEXT_SNAPSHOT_EVENTS = 50_000
MAX_CONTEXT_BOOTSTRAP_SECONDS = 10.0
MAX_CONTRACT_TARGET_BYTES = 1024 * 1024
MAX_CONTRACT_TOTAL_BYTES = 2 * 1024 * 1024
MAX_PRIOR_CURSOR_BYTES = 16 * 1024
MAX_DELIVERY_REPORT_BYTES = 64 * 1024
MAX_LIFECYCLE_MAPPING_BYTES = 64 * 1024
MAX_DIAGNOSTIC_CHARS = 1_000

_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")
_TARGET_STATUSES = frozenset(
    {
        "current",
        "stale",
        "missing",
        "malformed",
        "unsafe",
        "unreadable",
        "changing",
        "oversize",
    }
)
_INCOMPLETE_REQUIRED_TARGET_STATUSES = frozenset(
    {"unsafe", "unreadable", "changing", "oversize"}
)
_SELF_REPORTED_STATES = frozenset({"unsupported", "reported"})
_PROTOCOL_SPECS: dict[str, dict[str, Any]] = {
    "session_bootstrap": {
        "protocol_id": "session_bootstrap",
        "argv_prefix": ["agent-q", "context", "bootstrap"],
        "invocation": "complete",
        "input_transport": "none",
        "input_schema": None,
        "required_arguments": [],
        "output_schema": CONTEXT_BOOTSTRAP_SCHEMA,
    },
    "task_preflight": {
        "protocol_id": "task_preflight",
        "argv_prefix": ["agent-q", "decisions", "preflight", "--json"],
        "invocation": "prefix",
        "input_transport": "argv",
        "input_schema": None,
        "required_arguments": ["--path <repo-relative-path>"],
        "output_schema": "agent-mesh.decision-context.v1",
    },
    "pre_edit": {
        "protocol_id": "pre_edit",
        "argv_prefix": ["agent-q", "decisions", "hook"],
        "invocation": "complete",
        "input_transport": "stdin_json",
        "input_schema": "agent-mesh.decision-hook-request.v1",
        "required_arguments": [],
        "output_schema": "agent-mesh.decision-context.v1",
    },
    "pre_write": {
        "protocol_id": "pre_write",
        "argv_prefix": ["agent-q", "decisions", "hook"],
        "invocation": "complete",
        "input_transport": "stdin_json",
        "input_schema": "agent-mesh.decision-hook-request.v1",
        "required_arguments": [],
        "output_schema": "agent-mesh.decision-context.v1",
    },
    "resume_refresh": {
        "protocol_id": "resume_refresh",
        "argv_prefix": ["agent-q", "context", "bootstrap"],
        "invocation": "complete",
        "input_transport": "none",
        "input_schema": None,
        "required_arguments": [],
        "output_schema": CONTEXT_BOOTSTRAP_SCHEMA,
    },
    "compaction_refresh": {
        "protocol_id": "compaction_refresh",
        "argv_prefix": ["agent-q", "context", "bootstrap"],
        "invocation": "complete",
        "input_transport": "none",
        "input_schema": None,
        "required_arguments": [],
        "output_schema": CONTEXT_BOOTSTRAP_SCHEMA,
    },
    "reference_resolution": {
        "protocol_id": "reference_resolution",
        "argv_prefix": ["agent-q", "refs", "resolve", "--json"],
        "invocation": "prefix",
        "input_transport": "argv_or_stdin",
        "input_schema": None,
        "required_arguments": ["<ID>... or --stdin/--file"],
        "output_schema": "agent-mesh.reference-context.v1",
    },
    "change_review": {
        "protocol_id": "change_review",
        "argv_prefix": ["agent-mesh", "check", "decisions", "--json"],
        "invocation": "prefix",
        "input_transport": "argv",
        "input_schema": None,
        "required_arguments": [],
        "output_schema": "agent-mesh.decision-context.v1",
    },
}

_BUILTIN_LIFECYCLE_EVENTS: dict[str, dict[str, str | None]] = {
    "claude": {
        "session_bootstrap": "SessionStart",
        "task_preflight": "UserPromptSubmit",
        "pre_edit": "PreToolUse.Edit",
        "pre_write": "PreToolUse.Write",
        "resume_refresh": "SessionStart.resume",
        "compaction_refresh": "PreCompact",
        "reference_resolution": "reference_scan",
        "change_review": "change_review",
    },
    "codex-hermes": {
        "session_bootstrap": "hermes.session.start",
        "task_preflight": "hermes.task.start",
        "pre_edit": "hermes.pre_edit",
        "pre_write": "hermes.pre_write",
        "resume_refresh": "hermes.session.resume",
        "compaction_refresh": "hermes.compaction.complete",
        "reference_resolution": "hermes.reference_scan",
        "change_review": "hermes.change_review",
    },
    "generic-local": {
        "session_bootstrap": "session_start",
        "task_preflight": "task_start",
        "pre_edit": None,
        "pre_write": None,
        "resume_refresh": None,
        "compaction_refresh": None,
        "reference_resolution": "reference_scan",
        "change_review": "change_review",
    },
}


class ContextDeliveryRequestError(ValueError):
    """Raised when a prior cursor or lifecycle mapping is not a closed valid input."""


@dataclass(frozen=True)
class _TargetReadFailure(Exception):
    status: str
    detail: str


def contract_body_sha256() -> str:
    """Return the full managed-contract body digest used by freshness cursors."""

    return hashlib.sha256(CONTRACT_BODY.encode("utf-8")).hexdigest()


def default_delivery_report() -> dict[str, Any]:
    """Return the truthful zero-hook delivery report.

    Protocol availability and harness delivery are intentionally separate.  An
    adopter without a lifecycle adapter remains healthy while every delivery
    boundary is visibly unreported.
    """

    return {
        "protocol_available": True,
        "health_independent": True,
        "report": {
            "schema": DELIVERY_REPORT_SCHEMA,
            "adapter": None,
            "capabilities": [
                {"capability": capability, "state": "unreported"}
                for capability in DELIVERY_CAPABILITIES
            ],
        },
        "verification": None,
    }


def inspect_contract_targets(
    config: AgentMeshConfig,
    *,
    deadline_monotonic: float,
) -> dict[str, Any]:
    """Inspect only the two fixed managed targets under explicit read bounds."""

    required = {"agents"}
    for marker in (config.project_root / "CLAUDE.md", config.project_root / ".claude"):
        try:
            marker.lstat()
        except FileNotFoundError:
            continue
        except OSError:
            required.add("claude")
        else:
            required.add("claude")
    remaining = MAX_CONTRACT_TOTAL_BYTES
    targets: list[dict[str, Any]] = []
    diagnostics: list[str] = []
    complete = True
    for target, relative in CONTRACT_TARGETS.items():
        is_required = target in required
        status_value = "missing"
        content_sha256: str | None = None
        size_bytes: int | None = None
        path = config.project_root / relative
        try:
            data = _stable_fixed_file_bytes(
                path,
                max_bytes=min(MAX_CONTRACT_TARGET_BYTES, max(remaining, 0)),
                deadline_monotonic=min(deadline_monotonic, time.monotonic() + 2.0),
            )
        except FileNotFoundError:
            pass
        except _TargetReadFailure as exc:
            status_value = exc.status
            diagnostics.append(_bounded_diagnostic(f"{relative.as_posix()}: {exc.detail}"))
        else:
            remaining -= len(data)
            size_bytes = len(data)
            content_sha256 = hashlib.sha256(data).hexdigest()
            try:
                status_value = contract_text_status(data.decode("utf-8"))
            except UnicodeDecodeError:
                status_value = "unreadable"
                diagnostics.append(
                    _bounded_diagnostic(f"{relative.as_posix()}: target is not valid UTF-8")
                )
        if is_required and status_value in _INCOMPLETE_REQUIRED_TARGET_STATUSES:
            complete = False
        targets.append(
            {
                "target": target,
                "path": relative.as_posix(),
                "required": is_required,
                "status": status_value,
                "content_sha256": content_sha256,
                "size_bytes": size_bytes,
            }
        )
    return {
        "inspection_status": "complete" if complete else "incomplete",
        "version": CONTRACT_VERSION,
        "body_sha256": contract_body_sha256(),
        "display_digest": contract_digest(),
        "targets": targets,
        "diagnostics": diagnostics,
    }


def build_freshness_cursor(
    config: AgentMeshConfig,
    snapshot: CanonicalEventSnapshot,
    contract: dict[str, Any],
) -> dict[str, Any]:
    """Build a self-hashed cursor from one verified canonical snapshot."""

    cursor_targets = [
        {
            "target": str(item["target"]),
            "path": str(item["path"]),
            "required": bool(item["required"]),
            "status": str(item["status"]),
            "content_sha256": item.get("content_sha256"),
        }
        for item in contract["targets"]
    ]
    cursor: dict[str, Any] = {
        "schema": FRESHNESS_CURSOR_SCHEMA,
        "domain": FRESHNESS_CURSOR_DOMAIN,
        "store_id": config.store_id,
        "event_seq": snapshot.event_seq,
        "source_log_sha256": snapshot.source_log_sha256,
        "tail_event_sha256": snapshot.tail_event_sha256,
        "managed_contract": {
            "body_sha256": str(contract["body_sha256"]),
            "targets": cursor_targets,
        },
    }
    cursor["cursor_sha256"] = _cursor_sha256(cursor)
    return cursor


def validate_prior_cursor(value: Any) -> dict[str, Any]:
    """Validate one closed, bounded-depth freshness cursor and its self-hash."""

    if not isinstance(value, dict):
        raise ContextDeliveryRequestError("prior cursor must be a JSON object")
    expected = {
        "schema",
        "domain",
        "store_id",
        "event_seq",
        "source_log_sha256",
        "tail_event_sha256",
        "managed_contract",
        "cursor_sha256",
    }
    _require_exact_keys(value, expected, label="prior cursor")
    if value["schema"] != FRESHNESS_CURSOR_SCHEMA or value["domain"] != FRESHNESS_CURSOR_DOMAIN:
        raise ContextDeliveryRequestError("prior cursor has an unsupported schema or domain")
    store_id = _bounded_string(value["store_id"], "prior cursor store_id", 128)
    if not (STORE_ID_RE.fullmatch(store_id) or LEGACY_STORE_ID_RE.fullmatch(store_id)):
        raise ContextDeliveryRequestError("prior cursor store_id is not a supported store ID")
    event_seq = value["event_seq"]
    if (
        isinstance(event_seq, bool)
        or not isinstance(event_seq, int)
        or not 0 <= event_seq <= MAX_CONTEXT_SNAPSHOT_EVENTS
    ):
        raise ContextDeliveryRequestError(
            f"prior cursor event_seq must be an integer from 0 to {MAX_CONTEXT_SNAPSHOT_EVENTS}"
        )
    for field in (
        "source_log_sha256",
        "tail_event_sha256",
        "cursor_sha256",
    ):
        if not isinstance(value[field], str) or not _SHA256_RE.fullmatch(value[field]):
            raise ContextDeliveryRequestError(f"prior cursor {field} must be lowercase SHA-256")
    managed_contract = value["managed_contract"]
    if not isinstance(managed_contract, dict):
        raise ContextDeliveryRequestError("prior cursor managed_contract must be an object")
    _require_exact_keys(
        managed_contract,
        {"body_sha256", "targets"},
        label="prior cursor managed_contract",
    )
    if not isinstance(managed_contract["body_sha256"], str) or not _SHA256_RE.fullmatch(
        managed_contract["body_sha256"]
    ):
        raise ContextDeliveryRequestError(
            "prior cursor managed_contract body_sha256 must be lowercase SHA-256"
        )
    raw_targets = managed_contract["targets"]
    if not isinstance(raw_targets, list) or len(raw_targets) != len(CONTRACT_TARGETS):
        raise ContextDeliveryRequestError("prior cursor targets must contain every fixed target")
    targets: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in raw_targets:
        if not isinstance(raw, dict):
            raise ContextDeliveryRequestError("prior cursor target entries must be objects")
        _require_exact_keys(
            raw,
            {"target", "path", "required", "status", "content_sha256"},
            label="prior cursor target",
        )
        target = _bounded_string(raw["target"], "prior cursor target", 32)
        if target not in CONTRACT_TARGETS or target in seen:
            raise ContextDeliveryRequestError("prior cursor has an unknown or duplicate target")
        seen.add(target)
        if raw["path"] != CONTRACT_TARGETS[target].as_posix():
            raise ContextDeliveryRequestError("prior cursor target path does not match its identity")
        if not isinstance(raw["required"], bool):
            raise ContextDeliveryRequestError("prior cursor target required must be boolean")
        if raw["status"] not in _TARGET_STATUSES:
            raise ContextDeliveryRequestError("prior cursor target has an unsupported status")
        content_hash = raw["content_sha256"]
        if content_hash is not None and (
            not isinstance(content_hash, str) or not _SHA256_RE.fullmatch(content_hash)
        ):
            raise ContextDeliveryRequestError(
                "prior cursor target content_sha256 must be null or lowercase SHA-256"
            )
        targets.append(
            {
                "target": target,
                "path": raw["path"],
                "required": raw["required"],
                "status": raw["status"],
                "content_sha256": content_hash,
            }
        )
    if tuple(item["target"] for item in targets) != tuple(CONTRACT_TARGETS):
        raise ContextDeliveryRequestError("prior cursor targets are not in canonical order")
    normalized = {
        "schema": FRESHNESS_CURSOR_SCHEMA,
        "domain": FRESHNESS_CURSOR_DOMAIN,
        "store_id": store_id,
        "event_seq": event_seq,
        "source_log_sha256": value["source_log_sha256"],
        "tail_event_sha256": value["tail_event_sha256"],
        "managed_contract": {
            "body_sha256": managed_contract["body_sha256"],
            "targets": targets,
        },
    }
    if event_seq == 0 and (
        normalized["source_log_sha256"] != hashlib.sha256(b"").hexdigest()
        or normalized["tail_event_sha256"] != SENTINEL_PREV_HASH
    ):
        raise ContextDeliveryRequestError(
            "an empty prior cursor must use the empty-log SHA-256 and sentinel tail"
        )
    if _cursor_sha256(normalized) != value["cursor_sha256"]:
        raise ContextDeliveryRequestError("prior cursor cursor_sha256 does not match its fields")
    normalized["cursor_sha256"] = value["cursor_sha256"]
    return normalized


def compare_freshness_cursors(
    prior: dict[str, Any],
    current: dict[str, Any],
    snapshot: CanonicalEventSnapshot,
) -> dict[str, Any]:
    """Classify freshness without treating a forgeable cursor as authority."""

    if prior["store_id"] != current["store_id"]:
        return {
            "refresh_required": True,
            "reasons": ["store_mismatch"],
            "prefix_proven": None,
            "prior_event_seq": prior["event_seq"],
            "current_event_seq": current["event_seq"],
        }
    reasons: list[str] = []
    contract_changed = prior["managed_contract"] != current["managed_contract"]
    if prior["event_seq"] > current["event_seq"]:
        reasons.append("canonical_state_rolled_back")
        prefix_proven: bool | None = False
    elif prior["event_seq"] == current["event_seq"]:
        prefix_proven = (
            prior["source_log_sha256"] == current["source_log_sha256"]
            and prior["tail_event_sha256"] == current["tail_event_sha256"]
        )
        reasons.append("unchanged" if prefix_proven else "canonical_history_changed")
    else:
        successor = snapshot.records[prior["event_seq"]]
        prefix_proven = successor.get("prev_event_hash") == prior["tail_event_sha256"]
        reasons.append(
            "canonical_state_advanced" if prefix_proven else "canonical_history_changed"
        )
    if contract_changed:
        reasons.append("contract_changed")
    if reasons == ["unchanged", "contract_changed"]:
        reasons = ["contract_changed"]
    return {
        "refresh_required": reasons != ["unchanged"],
        "reasons": reasons,
        "prefix_proven": prefix_proven,
        "prior_event_seq": prior["event_seq"],
        "current_event_seq": current["event_seq"],
    }


def validate_lifecycle_mapping(
    value: Any,
) -> dict[str, Any]:
    """Validate a static mapping without converting self-report into verification."""

    if not isinstance(value, dict):
        raise ContextDeliveryRequestError("lifecycle mapping must be a JSON object")
    _require_exact_keys(
        value,
        {
            "schema",
            "fixture_id",
            "runtime_family",
            "claim_scope",
            "adapter_id",
            "capabilities",
        },
        label="lifecycle mapping",
    )
    if value["schema"] != LIFECYCLE_MAPPING_SCHEMA:
        raise ContextDeliveryRequestError("lifecycle mapping has an unsupported schema")
    runtime_family = value["runtime_family"]
    if runtime_family not in _BUILTIN_LIFECYCLE_EVENTS:
        raise ContextDeliveryRequestError("lifecycle mapping runtime_family is unsupported")
    expected = _builtin_mapping_value(runtime_family)
    if value != expected:
        raise ContextDeliveryRequestError(
            "lifecycle mapping must match the closed built-in capability, event, and action table"
        )
    report_capabilities = []
    normalized_mapping = []
    for item in expected["capabilities"]:
        capability = str(item["capability"])
        state_value = "reported" if item["mapping_status"] == "mapped" else "unsupported"
        report_capabilities.append({"capability": capability, "state": state_value})
        normalized = {
            "capability": capability,
            "mapping_status": item["mapping_status"],
            "protocol_id": capability,
        }
        if item["mapping_status"] == "mapped":
            normalized["lifecycle_event"] = item["lifecycle_event"]
        normalized_mapping.append(normalized)
    report = {
        "schema": DELIVERY_REPORT_SCHEMA,
        "adapter": {
            "id": "agent-mesh.context-delivery",
            "version": "schema-fixture",
        },
        "capabilities": report_capabilities,
    }
    return {
        "protocol_available": True,
        "health_independent": True,
        "fixture": {
            "fixture_id": expected["fixture_id"],
            "runtime_family": runtime_family,
            "fixture_sha256": None,
        },
        "report": report,
        "mapping": normalized_mapping,
        "verification": None,
    }


def validate_delivery_report(value: Any) -> dict[str, Any]:
    """Normalize harness self-report while rejecting all verification claims."""

    if not isinstance(value, dict):
        raise ContextDeliveryRequestError("delivery report must be a JSON object")
    _require_exact_keys(
        value,
        {"schema", "adapter", "capabilities"},
        label="delivery report",
    )
    if value["schema"] != DELIVERY_REPORT_SCHEMA:
        raise ContextDeliveryRequestError("delivery report has an unsupported schema")
    adapter = value["adapter"]
    if not isinstance(adapter, dict):
        raise ContextDeliveryRequestError("delivery report adapter must be an object")
    _require_exact_keys(adapter, {"id", "version"}, label="delivery report adapter")
    adapter_id = _bounded_identifier(adapter["id"], "delivery report adapter id")
    adapter_version = _bounded_string(adapter["version"], "delivery report adapter version", 64)
    raw_capabilities = value["capabilities"]
    if not isinstance(raw_capabilities, list) or len(raw_capabilities) > len(
        DELIVERY_CAPABILITIES
    ):
        raise ContextDeliveryRequestError("delivery report capabilities exceed the vocabulary")
    reported: dict[str, str] = {}
    for raw in raw_capabilities:
        if not isinstance(raw, dict):
            raise ContextDeliveryRequestError("delivery report capability must be an object")
        _require_exact_keys(raw, {"capability", "state"}, label="delivery report capability")
        capability = _bounded_string(raw["capability"], "delivery report capability", 64)
        if capability not in DELIVERY_CAPABILITIES or capability in reported:
            raise ContextDeliveryRequestError("delivery report has an unknown or duplicate capability")
        state_value = raw["state"]
        if state_value not in _SELF_REPORTED_STATES:
            raise ContextDeliveryRequestError(
                "delivery report state must be unsupported or reported; verified is validator-only"
            )
        reported[capability] = state_value
    normalized = {
        "schema": DELIVERY_REPORT_SCHEMA,
        "adapter": {"id": adapter_id, "version": adapter_version},
        "capabilities": [
            {"capability": capability, "state": reported.get(capability, "unreported")}
            for capability in DELIVERY_CAPABILITIES
        ],
    }
    return {
        "protocol_available": True,
        "health_independent": True,
        "report": normalized,
        "verification": None,
    }


def _builtin_mapping_value(runtime_family: str) -> dict[str, Any]:
    events = _BUILTIN_LIFECYCLE_EVENTS[runtime_family]
    capabilities: list[dict[str, Any]] = []
    for capability in DELIVERY_CAPABILITIES:
        lifecycle_event = events[capability]
        if lifecycle_event is None:
            capabilities.append(
                {"capability": capability, "mapping_status": "unsupported"}
            )
            continue
        protocol = _PROTOCOL_SPECS[capability]
        capabilities.append(
            {
                "capability": capability,
                "mapping_status": "mapped",
                "lifecycle_event": lifecycle_event,
                "action": {
                    "argv": list(protocol["argv_prefix"]),
                    "input_schema": protocol["input_schema"],
                    "output_schema": protocol["output_schema"],
                },
            }
        )
    return {
        "schema": LIFECYCLE_MAPPING_SCHEMA,
        "fixture_id": f"{runtime_family}-static",
        "runtime_family": runtime_family,
        "claim_scope": "schema_fixture",
        "adapter_id": "agent-mesh.context-delivery",
        "capabilities": capabilities,
    }


def _protocol_records() -> list[dict[str, Any]]:
    return [
        {
            "protocol_id": capability,
            "argv_prefix": list(_PROTOCOL_SPECS[capability]["argv_prefix"]),
            "invocation": _PROTOCOL_SPECS[capability]["invocation"],
            "input_transport": _PROTOCOL_SPECS[capability]["input_transport"],
            "input_schema": _PROTOCOL_SPECS[capability]["input_schema"],
            "required_arguments": list(
                _PROTOCOL_SPECS[capability]["required_arguments"]
            ),
            "output_schema": _PROTOCOL_SPECS[capability]["output_schema"],
        }
        for capability in DELIVERY_CAPABILITIES
    ]


def _with_schema_fixture_receipt(
    delivery: dict[str, Any],
    *,
    fixture_sha256: str,
) -> dict[str, Any]:
    normalized = json.loads(json.dumps(delivery, allow_nan=False))
    normalized["fixture"]["fixture_sha256"] = fixture_sha256
    adapter_artifact_sha256 = _builtin_adapter_artifact_sha256()
    receipt: dict[str, Any] = {
        "schema": DELIVERY_VERIFICATION_SCHEMA,
        "verification_scope": "schema_fixture",
        "verified_at_utc": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "fixture": {
            "fixture_id": normalized["fixture"]["fixture_id"],
            "fixture_sha256": fixture_sha256,
        },
        "adapter": {
            "id": "agent-mesh.context-delivery",
            "artifact_sha256": adapter_artifact_sha256,
            "distribution_fingerprint": None,
        },
        "runtime": None,
        "capabilities": [],
        "checks": [
            {"name": "closed_schema", "outcome": "pass"},
            {"name": "capability_command_binding", "outcome": "pass"},
            {"name": "fixture_fingerprint", "outcome": "pass"},
            {"name": "adapter_fingerprint", "outcome": "pass"},
        ],
    }
    receipt["evidence_sha256"] = _canonical_sha256(receipt)
    normalized["verification"] = receipt
    return normalized


def _builtin_adapter_artifact_sha256() -> str:
    try:
        data = _stable_fixed_file_bytes(
            Path(__file__),
            max_bytes=2 * 1024 * 1024,
            deadline_monotonic=time.monotonic() + 2.0,
        )
    except (FileNotFoundError, _TargetReadFailure) as exc:
        raise ContextDeliveryRequestError(
            "cannot fingerprint the installed context-delivery adapter"
        ) from exc
    return hashlib.sha256(data).hexdigest()


def build_context_bootstrap(
    config: AgentMeshConfig,
    snapshot: CanonicalEventSnapshot,
    *,
    deadline_monotonic: float,
    prior_cursor: dict[str, Any] | None = None,
    context_delivery: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one bounded bootstrap envelope from one verified canonical snapshot."""

    contract = inspect_contract_targets(config, deadline_monotonic=deadline_monotonic)
    complete = contract["inspection_status"] == "complete"
    cursor = build_freshness_cursor(config, snapshot, contract) if complete else None
    comparison = (
        compare_freshness_cursors(prior_cursor, cursor, snapshot)
        if prior_cursor is not None and cursor is not None
        else None
    )
    diagnostics = list(contract["diagnostics"])
    return {
        "schema": CONTEXT_BOOTSTRAP_SCHEMA,
        "privacy_class": "project_private",
        "context_status": "complete" if complete else "incomplete",
        "complete": complete,
        "repository": {
            "project_key": config.project_key,
            "store_id": config.store_id,
            "event_seq": snapshot.event_seq,
            "source_log_sha256": snapshot.source_log_sha256,
            "source_log_bytes": snapshot.source_log_bytes,
            "tail_event_sha256": snapshot.tail_event_sha256,
        },
        "managed_contract": contract,
        "protocols": _protocol_records(),
        "delivery_vocabulary": {
            "capabilities": list(DELIVERY_CAPABILITIES),
            "states": list(DELIVERY_STATES),
            "verification_scopes": list(VERIFICATION_SCOPES),
        },
        "non_claims": {
            "semantic_validation": False,
            "automatic_enforcement": False,
            "provider_context_restoration": False,
            "installed_provider_support": False,
        },
        "context_delivery": context_delivery or default_delivery_report(),
        "freshness": {"cursor": cursor, "comparison": comparison},
        "warnings": [],
        "diagnostics": diagnostics,
    }


def build_unavailable_context_bootstrap(diagnostic: str) -> dict[str, Any]:
    return {
        "schema": CONTEXT_BOOTSTRAP_SCHEMA,
        "privacy_class": "project_private",
        "context_status": "unavailable",
        "complete": False,
        "repository": None,
        "managed_contract": None,
        "protocols": _protocol_records(),
        "delivery_vocabulary": {
            "capabilities": list(DELIVERY_CAPABILITIES),
            "states": list(DELIVERY_STATES),
            "verification_scopes": list(VERIFICATION_SCOPES),
        },
        "non_claims": {
            "semantic_validation": False,
            "automatic_enforcement": False,
            "provider_context_restoration": False,
            "installed_provider_support": False,
        },
        "context_delivery": default_delivery_report(),
        "freshness": {"cursor": None, "comparison": None},
        "warnings": [],
        "diagnostics": [_bounded_diagnostic(diagnostic)],
    }


def render_bounded_context_bootstrap_json(
    result: dict[str, Any],
    *,
    pretty: bool = False,
    max_bytes: int = MAX_CONTEXT_BOOTSTRAP_JSON_BYTES,
) -> tuple[str, bool]:
    indent = 2 if pretty else None
    rendered = json.dumps(
        result,
        allow_nan=False,
        indent=indent,
        sort_keys=True,
        separators=None if pretty else (",", ":"),
    )
    if len(rendered.encode("utf-8")) <= max_bytes:
        return rendered, result.get("complete") is True
    compact = build_unavailable_context_bootstrap(
        "context bootstrap exceeds its JSON output bound"
    )
    compact["context_status"] = "incomplete"
    compact_rendered = json.dumps(
        compact,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return compact_rendered, False


def load_prior_cursor(path: Path) -> dict[str, Any]:
    value, _ = _load_bounded_json(path, max_bytes=MAX_PRIOR_CURSOR_BYTES, label="prior cursor")
    return validate_prior_cursor(value)


def load_lifecycle_mapping(path: Path) -> dict[str, Any]:
    value, fixture_sha256 = _load_bounded_json(
        path,
        max_bytes=MAX_LIFECYCLE_MAPPING_BYTES,
        label="lifecycle mapping",
    )
    delivery = validate_lifecycle_mapping(value)
    return _with_schema_fixture_receipt(delivery, fixture_sha256=fixture_sha256)


def load_builtin_lifecycle_mapping(runtime_family: str) -> dict[str, Any]:
    if runtime_family not in _BUILTIN_LIFECYCLE_EVENTS:
        raise ContextDeliveryRequestError("built-in lifecycle mapping is unsupported")
    value = _builtin_mapping_value(runtime_family)
    delivery = validate_lifecycle_mapping(value)
    return _with_schema_fixture_receipt(
        delivery,
        fixture_sha256=_canonical_sha256(value),
    )


def load_delivery_report(path: Path) -> dict[str, Any]:
    value, _ = _load_bounded_json(
        path,
        max_bytes=MAX_DELIVERY_REPORT_BYTES,
        label="delivery report",
    )
    return validate_delivery_report(value)


def _load_bounded_json(path: Path, *, max_bytes: int, label: str) -> tuple[Any, str]:
    try:
        data = _stable_fixed_file_bytes(
            path.expanduser(),
            max_bytes=max_bytes,
            deadline_monotonic=time.monotonic() + 2.0,
        )
    except FileNotFoundError as exc:
        raise ContextDeliveryRequestError(f"{label} file does not exist: {path}") from exc
    except _TargetReadFailure as exc:
        raise ContextDeliveryRequestError(f"cannot read {label}: {exc.detail}") from exc
    try:
        value = json.loads(data.decode("utf-8"), object_pairs_hook=_unique_json_object)
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise ContextDeliveryRequestError(f"{label} is not valid bounded JSON") from exc
    return value, hashlib.sha256(data).hexdigest()


def _stable_fixed_file_bytes(
    path: Path,
    *,
    max_bytes: int,
    deadline_monotonic: float,
) -> bytes:
    nofollow = getattr(os, "O_NOFOLLOW", None)
    if not isinstance(nofollow, int) or nofollow == 0:
        raise _TargetReadFailure(
            "unsafe", "this platform does not expose a safe no-follow file-open primitive"
        )
    if max_bytes < 0:
        raise _TargetReadFailure("oversize", "target exceeds the total byte budget")
    for _ in range(3):
        if time.monotonic() >= deadline_monotonic:
            raise _TargetReadFailure("changing", "target read exceeded its time budget")
        try:
            before = path.lstat()
        except FileNotFoundError:
            raise
        except OSError as exc:
            raise _TargetReadFailure("unreadable", f"cannot inspect target: {exc}") from exc
        if stat.S_ISLNK(before.st_mode) or not stat.S_ISREG(before.st_mode):
            raise _TargetReadFailure("unsafe", "target is a symlink or is not a regular file")
        if before.st_size > max_bytes:
            raise _TargetReadFailure("oversize", f"target exceeds the {max_bytes}-byte budget")
        flags = os.O_RDONLY | nofollow
        try:
            descriptor = os.open(path, flags)
        except OSError as exc:
            raise _TargetReadFailure("unreadable", f"cannot open target: {exc}") from exc
        try:
            opened = os.fstat(descriptor)
            if not stat.S_ISREG(opened.st_mode):
                raise _TargetReadFailure("unsafe", "opened target is not a regular file")
            if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
                continue
            chunks: list[bytes] = []
            used = 0
            while True:
                if time.monotonic() >= deadline_monotonic:
                    raise _TargetReadFailure("changing", "target read exceeded its time budget")
                chunk = os.read(descriptor, min(64 * 1024, max_bytes - used + 1))
                if not chunk:
                    break
                used += len(chunk)
                if used > max_bytes:
                    raise _TargetReadFailure(
                        "oversize", f"target exceeds the {max_bytes}-byte budget"
                    )
                chunks.append(chunk)
            after_open = os.fstat(descriptor)
        finally:
            os.close(descriptor)
        try:
            after = path.lstat()
        except OSError:
            continue
        identity_before = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
        identity_after_open = (
            after_open.st_dev,
            after_open.st_ino,
            after_open.st_size,
            after_open.st_mtime_ns,
        )
        identity_after = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
        data = b"".join(chunks)
        if identity_before == identity_after_open == identity_after and len(data) == before.st_size:
            return data
    raise _TargetReadFailure("changing", "target changed during bounded stable read")


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ContextDeliveryRequestError(f"duplicate JSON field: {key}")
        value[key] = item
    return value


def _require_exact_keys(value: dict[str, Any], expected: set[str], *, label: str) -> None:
    actual = set(value)
    if actual == expected:
        return
    unknown = sorted(actual - expected)
    missing = sorted(expected - actual)
    detail = []
    if unknown:
        detail.append(f"unknown={','.join(unknown)}")
    if missing:
        detail.append(f"missing={','.join(missing)}")
    raise ContextDeliveryRequestError(f"{label} fields are not closed ({'; '.join(detail)})")


def _bounded_string(value: Any, label: str, max_chars: int) -> str:
    if not isinstance(value, str) or not value or len(value) > max_chars:
        raise ContextDeliveryRequestError(f"{label} must be a non-empty string <= {max_chars} chars")
    return value


def _bounded_identifier(value: Any, label: str) -> str:
    text = _bounded_string(value, label, 64)
    if not _IDENTIFIER_RE.fullmatch(text):
        raise ContextDeliveryRequestError(f"{label} has an invalid identifier")
    return text


def _canonical_sha256(value: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(value)).hexdigest()


def _canonical_json(value: dict[str, Any]) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _cursor_sha256(value: dict[str, Any]) -> str:
    return hashlib.sha256(
        FRESHNESS_CURSOR_DOMAIN.encode("utf-8") + b"\x00" + _canonical_json(value)
    ).hexdigest()


def _bounded_diagnostic(value: str) -> str:
    return value[:MAX_DIAGNOSTIC_CHARS]


def capability_state_counts(report: dict[str, Any]) -> dict[str, int]:
    """Return stable sibling-report counts for human-facing adoption output."""

    counts = {state: 0 for state in DELIVERY_STATES}
    raw_report = report.get("report")
    raw = raw_report.get("capabilities", []) if isinstance(raw_report, dict) else []
    states = {
        str(item.get("capability")): str(item.get("state"))
        for item in raw
        if isinstance(item, dict)
    }
    for capability in DELIVERY_CAPABILITIES:
        state_value = states.get(capability, "unreported")
        if state_value in counts:
            counts[state_value] += 1
    return counts


def iter_fixture_capabilities(report: dict[str, Any]) -> Iterable[tuple[str, str]]:
    """Expose deterministic capability/state pairs for docs and conformance tests."""

    raw_report = report.get("report")
    raw = raw_report.get("capabilities", []) if isinstance(raw_report, dict) else []
    states = {
        str(item.get("capability")): str(item.get("state"))
        for item in raw
        if isinstance(item, dict)
    }
    for capability in DELIVERY_CAPABILITIES:
        yield capability, states.get(capability, "unreported")
