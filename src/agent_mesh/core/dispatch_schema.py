"""Strict allowlist schema for the dispatch event domain (the ``DISPATCH_BODY_LEAK`` guard).

The dispatch domain's core invariant is that a payload NEVER carries prompts, command strings, reply
bodies, raw error text, or substrate text: only hashes, digests, counts, enum codes, and ids. This
module is the single enforcement of that invariant. It lives in ``core`` (substrate), not in the
optional ``agent_mesh.dispatch`` orchestration layer, so it can be applied at BOTH write time
(``append_event``) and replay time (``store.rebuild``) without the substrate importing dispatch.

Enforcement is by SCHEMA, not by string length: every payload key must be in the per-kind allowed
set; every enum field must hold an allowed value; every digest field must match ``^[0-9a-f]{16}$``;
every string is length-bounded and newline-free (a one-line id/code/hash never spans lines, a leaked
prompt or stack trace would); and a defense-in-depth scan rejects any key whose name belongs to a
forbidden family (``body``, ``prompt``, ``command`` ...). See ``docs/domains/dispatch.md`` §1, §6.
"""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from typing import Any, Callable

DISPATCH_BODY_LEAK = "DISPATCH_BODY_LEAK"

DISPATCH_EVENT_KINDS = frozenset(
    {
        "dispatch_run_planned",
        "dispatch_run_blocked",
        "dispatch_lease_acquired",
        "dispatch_lease_released",
        "dispatch_run_started",
        "dispatch_run_completed",
        "dispatch_run_failed",
        "dispatch_run_terminated",
        "dispatch_retry_exhausted",
        "dispatch_policy_frozen",
        "review_assurance_recorded",
        "review_assurance_flagged",
        "review_assurance_retired",
    }
)

# --- enum value sets (the closed vocabularies the payloads may carry) ---------------------------
RUN_MODES = frozenset({"dry_run", "live"})
SESSION_KEY_SOURCES = frozenset({"feature", "wave_ref", "thread_id"})
CLASSIFICATIONS = frozenset({"routine", "risky"})
PLANNED_GATES = frozenset({"auto-dispatch", "hold-for-approval", "blocked-substrate-incomplete"})
BLOCKED_GATES = frozenset({"blocked-substrate-incomplete", "hold-for-approval"})
GATE_REASON_CODES = frozenset(
    {"routine", "non-routine-default-deny", "single-mode-multi-recipient", "substrate-incomplete"}
)
RESPONSE_MODES = frozenset({"single", "multi"})
PLANNED_STATUSES = frozenset({"dry_run", "planned"})
RELEASE_REASONS = frozenset(
    {"completed", "failed", "cancelled", "timeout", "parent_loss", "expired", "superseded"}
)
RUN_TERMINAL_STATES = frozenset({"cancelled", "timed_out", "parent_lost"})
MANAGEMENT_LEVELS = frozenset(
    {"managed_launch", "addressed_running_instance", "harness_native", "external_manual"}
)
PERMISSION_CEILINGS = frozenset({"read-only", "workspace-write", "danger-full-access"})
PRIVACY_CLASSES = frozenset({"public_project", "project_private", "sensitive_private"})
SUBJECT_TYPES = frozenset({"decision_revision", "change_set", "artifact", "unbound"})
ASSURANCE_DISPOSITIONS = frozenset(
    {"GO", "CONDITIONAL", "NO-GO", "PASS", "NOT_SATISFIED", "INFORMATIONAL"}
)
ASSURANCE_LIFECYCLE_REASON_CODES = frozenset(
    {
        "materially_inaccurate_claim",
        "stale_decision_citation",
        "artifact_invalid",
        "independence_contested",
        "subject_binding_contested",
        "human_retirement",
        "other",
    }
)
INDEPENDENCE_CLASSES = frozenset(
    {
        "same_context",
        "distinct_context",
        "distinct_instance",
        "distinct_instance_and_context",
        "external_unverified",
    }
)
PROVENANCE_REQUIREMENTS = frozenset(
    {"runtime_profile", "instance", "context", "subject_binding", "artifact_digest"}
)
RESPONSE_CONTRACTS = frozenset({"bounded_res", "review_v1"})
RESPONSE_SLOT_KINDS = frozenset({"single", "participant", "instance"})
ARTIFACT_LOCATION_TYPES = frozenset({"repository_path", "uri"})
ARTIFACT_PROVENANCE = frozenset(
    {"managed_launch", "addressed_running_instance", "harness_native", "external_manual"}
)
CAPABILITY_EVIDENCE_CLASSES = frozenset(
    {"generic_host", "built_in_driver", "self_attested", "unverified"}
)

RESPONSE_CANDIDATE_STATUSES = frozenset({"ready", "rejected"})
RESPONSE_CANDIDATE_REASONS = frozenset(
    {
        "ok",
        "not_successful",
        "missing_response_markers",
        "ambiguous_response_markers",
        "malformed_response_markers",
        "empty_response_body",
        "response_body_too_large",
        "unterminated_terminal_control",
    }
)

# The apply-gate halt categories. This MUST stay equal to dispatch.eval.GATE_HALTS; the substrate
# cannot import the optional dispatch layer, so the value is duplicated here and covered by the
# package's internal validation suite.
DISPATCH_GATE_HALT_CODES = frozenset(
    {
        "missing-or-failing-eval-results",
        "eval-fingerprint-mismatch",
        "stale-artifact-hash",
        "approval-identity-missing",
        "self-approval",
        "changed-git-sha",
        "changed-touched-row-version",
        "invariant-failure",
    }
)
# A blocked run's reasons are the apply-gate halts (a held risky gate) plus the substrate-incomplete
# code (a hard block); never free text.
BLOCK_REASON_CODES = frozenset(DISPATCH_GATE_HALT_CODES | {"substrate-incomplete"})

# Defense in depth on top of the per-kind allow-list: no key name may belong to a leak-prone family,
# even if a future allow-list edit mistakenly admitted it. "missing" is deliberately absent so the
# legitimate ``missing_count`` key is not flagged; a bare ``missing`` key is rejected by the
# allow-list regardless.
_FORBIDDEN_KEY_SUBSTRINGS = (
    "body",
    "prompt",
    "command",
    "would_run",
    "post_reply",
    "substrate",
    "preview",
    "traceback",
    "stacktrace",
)

_DIGEST_RE = re.compile(r"^[0-9a-f]{16}$")
_SHA256_DIGEST_RE = re.compile(r"^[0-9a-f]{64}$")
# error_class is a category code (a bare class/identifier), NEVER raw error text: no spaces, colons,
# paths, or message content can pass. Anchored so it cannot carry a single-line raw error string.
_ERROR_CLASS_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,63}$")
_MAX_STR = 256
_MAX_ARRAY = 64


class DispatchSchemaError(ValueError):
    """A dispatch payload violated the strict allow-list schema (a ``DISPATCH_BODY_LEAK``).

    Carries ``code`` (always ``DISPATCH_BODY_LEAK``) so write-time callers can convert it to an
    ``EventProtocolError`` and replay-time callers to a ``DispatchStopLine`` with the same code.
    """

    code = DISPATCH_BODY_LEAK

    def __init__(self, detail: str) -> None:
        self.detail = detail
        super().__init__(f"{DISPATCH_BODY_LEAK}: {detail}")


# --- field validators -------------------------------------------------------------------------
Validator = Callable[[Any, str], None]


def _check_key_family(key: str) -> None:
    lowered = key.lower()
    for token in _FORBIDDEN_KEY_SUBSTRINGS:
        if token in lowered:
            raise DispatchSchemaError(f"forbidden key family {token!r} in key {key!r}")


def _s(*, maxlen: int = _MAX_STR, allow_empty: bool = False) -> Validator:
    def check(value: Any, path: str) -> None:
        if not isinstance(value, str):
            raise DispatchSchemaError(f"{path} must be a string, got {type(value).__name__}")
        # Reject EVERY control character and Unicode line/paragraph separator, not just LF/CR:
        # NEL (U+0085), VT, FF, U+2028, U+2029 would otherwise split a "single-line" field. A
        # one-line id/code/hash never spans lines; a leaked prompt or stack trace would.
        if any(unicodedata.category(ch) in ("Cc", "Zl", "Zp") for ch in value):
            raise DispatchSchemaError(
                f"{path} must not contain a control or line-separator character"
            )
        if not allow_empty and not value:
            raise DispatchSchemaError(f"{path} must be a non-empty string")
        if len(value) > maxlen:
            raise DispatchSchemaError(f"{path} exceeds max length {maxlen}")

    return check


def _enum(allowed: frozenset[str]) -> Validator:
    def check(value: Any, path: str) -> None:
        if not isinstance(value, str) or value not in allowed:
            raise DispatchSchemaError(f"{path} must be one of {sorted(allowed)}, got {value!r}")

    return check


def _digest(value: Any, path: str) -> None:
    # fullmatch, not match: Python's `$` also matches just before a trailing newline, so `.match`
    # would accept "0123456789abcdef\n". fullmatch anchors at the true end of the string.
    if not isinstance(value, str) or not _DIGEST_RE.fullmatch(value):
        raise DispatchSchemaError(f"{path} must be a 16-char lowercase hex digest, got {value!r}")


def _sha256_digest(value: Any, path: str) -> None:
    if not isinstance(value, str) or not _SHA256_DIGEST_RE.fullmatch(value):
        raise DispatchSchemaError(f"{path} must be a 64-char lowercase hex digest, got {value!r}")


def _error_class(value: Any, path: str) -> None:
    # A bare class/category identifier only (e.g. TimeoutError, provider_error): never raw error
    # text, a message, a path, or a token fragment. Enforced by schema, not by length.
    if not isinstance(value, str) or not _ERROR_CLASS_RE.fullmatch(value):
        raise DispatchSchemaError(
            f"{path} must be a bare class/category identifier (^[A-Za-z][A-Za-z0-9_]{{0,63}}$), "
            f"got {value!r}"
        )


def _int(value: Any, path: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int):
        raise DispatchSchemaError(f"{path} must be an integer, got {value!r}")


def _int_or_null(value: Any, path: str) -> None:
    if value is None:
        return
    _int(value, path)


def _bool(value: Any, path: str) -> None:
    if not isinstance(value, bool):
        raise DispatchSchemaError(f"{path} must be a boolean")


def _positive_int(value: Any, path: str) -> None:
    _int(value, path)
    if value < 1:
        raise DispatchSchemaError(f"{path} must be >= 1")


def _purpose(value: Any, path: str) -> None:
    _s(maxlen=64)(value, path)
    if not re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", value):
        raise DispatchSchemaError(f"{path} must be a lowercase purpose slug")


def _str_array_free(*, max_items: int = 32, maxlen: int = 128) -> Validator:
    def check(value: Any, path: str) -> None:
        if not isinstance(value, list) or len(value) > max_items:
            raise DispatchSchemaError(f"{path} must be an array of at most {max_items} strings")
        seen: set[str] = set()
        for index, item in enumerate(value):
            _s(maxlen=maxlen)(item, f"{path}[{index}]")
            if item in seen:
                raise DispatchSchemaError(f"{path} must not contain duplicates")
            seen.add(item)

    return check


def _str_array(allowed: frozenset[str]) -> Validator:
    def check(value: Any, path: str) -> None:
        if not isinstance(value, list):
            raise DispatchSchemaError(f"{path} must be an array")
        if len(value) > _MAX_ARRAY:
            raise DispatchSchemaError(f"{path} exceeds max array length {_MAX_ARRAY}")
        for index, item in enumerate(value):
            if not isinstance(item, str) or item not in allowed:
                raise DispatchSchemaError(
                    f"{path}[{index}] must be one of {sorted(allowed)}, got {item!r}"
                )

    return check


def _grounding(value: Any, path: str) -> None:
    if not isinstance(value, dict):
        raise DispatchSchemaError(f"{path} must be an object")
    allowed = {"complete", "digest"}
    for key in value:
        _check_key_family(key)
        if key not in allowed:
            raise DispatchSchemaError(f"{path}.{key} is not an allowed grounding field")
    if not isinstance(value.get("complete"), bool):
        raise DispatchSchemaError(f"{path}.complete must be a boolean")
    _digest(value.get("digest"), f"{path}.digest")


def _adapter_capabilities(value: Any, path: str) -> None:
    if not isinstance(value, dict):
        raise DispatchSchemaError(f"{path} must be an object")
    allowed = {"cache_ttl_control", "cache_prewarm"}
    for key in value:
        _check_key_family(key)
        if key not in allowed:
            raise DispatchSchemaError(f"{path}.{key} is not an allowed adapter_capabilities field")
    for field in ("cache_ttl_control", "cache_prewarm"):
        if not isinstance(value.get(field), bool):
            raise DispatchSchemaError(f"{path}.{field} must be a boolean")


def _response_contract(value: Any, path: str) -> None:
    if not isinstance(value, dict) or set(value) != {"kind", "max_chars", "requires_fence"}:
        raise DispatchSchemaError(
            f"{path} must contain exactly kind, max_chars, and requires_fence"
        )
    _enum(RESPONSE_CONTRACTS)(value["kind"], f"{path}.kind")
    _positive_int(value["max_chars"], f"{path}.max_chars")
    if value["max_chars"] > 20_000:
        raise DispatchSchemaError(f"{path}.max_chars must not exceed 20000")
    _bool(value["requires_fence"], f"{path}.requires_fence")


def _artifact_contract(value: Any, path: str) -> None:
    required = {"root", "media_types", "max_bytes", "visibility", "creation_allowed"}
    if not isinstance(value, dict) or set(value) != required:
        raise DispatchSchemaError(f"{path} must contain exactly {sorted(required)}")
    _s(maxlen=256, allow_empty=True)(value["root"], f"{path}.root")
    _str_array_free(max_items=16, maxlen=128)(value["media_types"], f"{path}.media_types")
    _int(value["max_bytes"], f"{path}.max_bytes")
    if value["max_bytes"] < 0 or value["max_bytes"] > 16 * 1024 * 1024:
        raise DispatchSchemaError(f"{path}.max_bytes must not exceed 16777216")
    _enum(PRIVACY_CLASSES)(value["visibility"], f"{path}.visibility")
    _bool(value["creation_allowed"], f"{path}.creation_allowed")
    if not value["root"] and (
        value["media_types"] or value["max_bytes"] != 0 or value["creation_allowed"]
    ):
        raise DispatchSchemaError(
            f"{path} with an empty root must be a disabled zero-byte contract"
        )


def _retry_contract(value: Any, path: str) -> None:
    if not isinstance(value, dict) or set(value) != {"max_attempts"}:
        raise DispatchSchemaError(f"{path} must contain exactly max_attempts")
    _positive_int(value["max_attempts"], f"{path}.max_attempts")
    if value["max_attempts"] > 8:
        raise DispatchSchemaError(f"{path}.max_attempts must not exceed 8")


def _dispatch_target(value: Any, path: str) -> None:
    required = {"participant", "durable_role", "runtime_profile"}
    optional = {"instance_id"}
    if (
        not isinstance(value, dict)
        or not required.issubset(value)
        or set(value) - required - optional
    ):
        raise DispatchSchemaError(
            f"{path} must contain participant, durable_role, runtime_profile, "
            "and optional instance_id"
        )
    for field in required:
        _s(maxlen=128)(value[field], f"{path}.{field}")
    if "instance_id" in value:
        _s(maxlen=128)(value["instance_id"], f"{path}.instance_id")


def _runtime_profile_revision(value: Any, path: str) -> None:
    required = {
        "name",
        "target",
        "provider",
        "adapter",
        "version",
        "model",
        "durable_role",
        "role",
        "permission_mode",
        "repository_scope",
        "required_capabilities",
        "authentication_mode",
        "billing_mode",
        "adapter_trust",
        "session_identity_mode",
        "resumable",
        "concurrent_attachment",
        "terminal_observation",
        "driver_source",
        "driver_protocol",
        "driver_manifest_sha256",
        "digest",
    }
    if not isinstance(value, dict) or set(value) != required:
        raise DispatchSchemaError(f"{path} has invalid runtime-profile revision fields")
    for field in required - {
        "required_capabilities",
        "resumable",
        "concurrent_attachment",
        "digest",
    }:
        _s(maxlen=256, allow_empty=field in {"driver_protocol", "driver_manifest_sha256"})(
            value[field], f"{path}.{field}"
        )
    _str_array_free(max_items=32, maxlen=64)(
        value["required_capabilities"], f"{path}.required_capabilities"
    )
    _bool(value["resumable"], f"{path}.resumable")
    _bool(value["concurrent_attachment"], f"{path}.concurrent_attachment")
    _sha256_digest(value["digest"], f"{path}.digest")
    authority = {key: item for key, item in value.items() if key != "digest"}
    expected = hashlib.sha256(
        json.dumps(authority, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    if value["digest"] != expected:
        raise DispatchSchemaError(f"{path}.digest does not bind the profile revision")


def _response_slot(value: Any, path: str) -> None:
    if not isinstance(value, dict) or set(value) != {"kind", "key"}:
        raise DispatchSchemaError(f"{path} must contain exactly kind and key")
    _enum(RESPONSE_SLOT_KINDS)(value["kind"], f"{path}.kind")
    _s(maxlen=128)(value["key"], f"{path}.key")


def _author_provenance(value: Any, path: str) -> None:
    required = {
        "source",
        "source_event_id",
        "source_event_seq",
        "source_event_hash",
        "participant",
    }
    optional = {"instance_id", "context_digest"}
    if (
        not isinstance(value, dict)
        or not required.issubset(value)
        or set(value) - required - optional
    ):
        raise DispatchSchemaError(f"{path} has invalid canonical author-provenance fields")
    if value["source"] != "canonical_event":
        raise DispatchSchemaError(f"{path}.source must be canonical_event")
    _s(maxlen=128)(value["source_event_id"], f"{path}.source_event_id")
    if not isinstance(value["source_event_seq"], int) or value["source_event_seq"] < 1:
        raise DispatchSchemaError(f"{path}.source_event_seq must be a positive integer")
    _sha256_digest(value["source_event_hash"], f"{path}.source_event_hash")
    _s(maxlen=128)(value["participant"], f"{path}.participant")
    if "instance_id" in value:
        _s(maxlen=128)(value["instance_id"], f"{path}.instance_id")
    if "context_digest" in value:
        _sha256_digest(value["context_digest"], f"{path}.context_digest")


def _subject_binding(value: Any, path: str) -> None:
    required = {"type", "stable_id", "digest", "revision", "privacy_class"}
    optional = {
        "author_provenance",
        "resolution",
    }
    if (
        not isinstance(value, dict)
        or not required.issubset(value)
        or set(value) - required - optional
    ):
        raise DispatchSchemaError(f"{path} has invalid subject-binding fields")
    _enum(SUBJECT_TYPES)(value["type"], f"{path}.type")
    _s(maxlen=256, allow_empty=value["type"] == "unbound")(value["stable_id"], f"{path}.stable_id")
    if value["type"] == "unbound":
        _s(maxlen=64, allow_empty=True)(value["digest"], f"{path}.digest")
    else:
        _sha256_digest(value["digest"], f"{path}.digest")
    _s(maxlen=128, allow_empty=True)(value["revision"], f"{path}.revision")
    _enum(PRIVACY_CLASSES)(value["privacy_class"], f"{path}.privacy_class")
    if "author_provenance" in value:
        _author_provenance(value["author_provenance"], f"{path}.author_provenance")
    resolution = value.get("resolution")
    if value["type"] == "change_set":
        required_resolution = {
            "kind",
            "mode",
            "base",
            "base_oid",
            "head_oid",
            "merge_base",
            "entries",
        }
        if not isinstance(resolution, dict) or set(resolution) != required_resolution:
            raise DispatchSchemaError(
                f"{path}.resolution must bind the complete change-set envelope"
            )
        _enum(frozenset({"git_changes_v1"}))(resolution["kind"], f"{path}.resolution.kind")
        _enum(frozenset({"pr", "staged", "worktree", "full"}))(
            resolution["mode"], f"{path}.resolution.mode"
        )
        _s(maxlen=256, allow_empty=True)(resolution["base"], f"{path}.resolution.base")
        for field in ("base_oid", "head_oid", "merge_base"):
            value_field = resolution[field]
            if value_field:
                _s(maxlen=64)(value_field, f"{path}.resolution.{field}")
            else:
                _s(maxlen=64, allow_empty=True)(value_field, f"{path}.resolution.{field}")
        entries = resolution["entries"]
        if not isinstance(entries, list) or len(entries) > 512:
            raise DispatchSchemaError(f"{path}.resolution.entries exceeds 512 items")
        entry_fields = {
            "path",
            "comparison",
            "change_kind",
            "status",
            "old_path",
            "new_path",
            "old_mode",
            "new_mode",
            "old_content",
            "new_content",
        }
        for index, entry in enumerate(entries):
            entry_path = f"{path}.resolution.entries[{index}]"
            if not isinstance(entry, dict) or set(entry) != entry_fields:
                raise DispatchSchemaError(f"{entry_path} has invalid fields")
            for field in entry_fields:
                item = entry[field]
                if item is None and field in {"old_path", "new_path"}:
                    continue
                _s(maxlen=1024, allow_empty=field in {"old_mode", "new_mode"})(
                    item, f"{entry_path}.{field}"
                )
    elif resolution is not None:
        raise DispatchSchemaError(f"{path}.resolution is only valid for a change_set")


def _assurance_policy(value: Any, path: str) -> None:
    required = {
        "revision",
        "reviewer_roles",
        "independence",
        "quorum",
        "validity_seconds",
        "enforcement",
        "coverage",
    }
    if not isinstance(value, dict) or set(value) != required:
        raise DispatchSchemaError(f"{path} must contain exactly {sorted(required)}")
    _sha256_digest(value["revision"], f"{path}.revision")
    _str_array_free(max_items=32, maxlen=128)(value["reviewer_roles"], f"{path}.reviewer_roles")
    _enum(INDEPENDENCE_CLASSES - {"same_context", "external_unverified"})(
        value["independence"], f"{path}.independence"
    )
    _positive_int(value["quorum"], f"{path}.quorum")
    _positive_int(value["validity_seconds"], f"{path}.validity_seconds")
    _enum(frozenset({"advisory", "blocking"}))(value["enforcement"], f"{path}.enforcement")
    coverage = value["coverage"]
    if not isinstance(coverage, dict) or set(coverage) != {"kind", "key"}:
        raise DispatchSchemaError(f"{path}.coverage must contain exactly kind and key")
    _enum(frozenset({"none", "decision", "path", "backlog_transition", "release"}))(
        coverage["kind"], f"{path}.coverage.kind"
    )
    _s(maxlen=256, allow_empty=coverage["kind"] == "none")(coverage["key"], f"{path}.coverage.key")
    if coverage["kind"] == "none" and coverage["key"]:
        raise DispatchSchemaError(f"{path}.coverage.key must be empty when kind=none")
    expected = assurance_policy_revision(value)
    if value["revision"] != expected:
        raise DispatchSchemaError(f"{path}.revision does not bind the assurance policy")


def assurance_policy_revision(value: dict[str, Any]) -> str:
    """Return the SHA-256 revision for a frozen review-assurance policy."""

    authority = {key: item for key, item in value.items() if key != "revision"}
    encoded = json.dumps(authority, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _reviewer(value: Any, path: str) -> None:
    required = {"participant", "instance_id", "context_digest", "role", "independence_class"}
    if not isinstance(value, dict) or set(value) != required:
        raise DispatchSchemaError(f"{path} must contain exactly {sorted(required)}")
    for field in ("participant", "instance_id", "role"):
        _s(maxlen=128, allow_empty=field == "instance_id")(value[field], f"{path}.{field}")
    _sha256_digest(value["context_digest"], f"{path}.context_digest")
    _enum(INDEPENDENCE_CLASSES)(value["independence_class"], f"{path}.independence_class")


def _finding_counts(value: Any, path: str) -> None:
    required = {"fatal", "material", "minor"}
    if not isinstance(value, dict) or set(value) != required:
        raise DispatchSchemaError(f"{path} must contain exactly fatal, material, and minor")
    for field in required:
        _int(value[field], f"{path}.{field}")
        if value[field] < 0 or value[field] > 1_000:
            raise DispatchSchemaError(f"{path}.{field} must be between 0 and 1000")


def _typed_artifact_refs(value: Any, path: str) -> None:
    if not isinstance(value, list) or len(value) > 16:
        raise DispatchSchemaError(f"{path} must be an array of at most 16 references")
    required = {
        "location_type",
        "location",
        "sha256",
        "byte_size",
        "media_type",
        "revision",
        "visibility",
        "provenance",
        "subject_digest",
        "field_privacy",
    }
    privacy_fields = {
        "location",
        "sha256",
        "revision",
        "subject_digest",
    }
    for index, item in enumerate(value):
        item_path = f"{path}[{index}]"
        if not isinstance(item, dict) or set(item) != required:
            raise DispatchSchemaError(f"{item_path} has invalid typed-reference fields")
        _enum(ARTIFACT_LOCATION_TYPES)(item["location_type"], f"{item_path}.location_type")
        _s(maxlen=512)(item["location"], f"{item_path}.location")
        _sha256_digest(item["sha256"], f"{item_path}.sha256")
        _int(item["byte_size"], f"{item_path}.byte_size")
        if item["byte_size"] < 0:
            raise DispatchSchemaError(f"{item_path}.byte_size must be non-negative")
        _s(maxlen=128)(item["media_type"], f"{item_path}.media_type")
        _s(maxlen=128, allow_empty=True)(item["revision"], f"{item_path}.revision")
        _enum(PRIVACY_CLASSES)(item["visibility"], f"{item_path}.visibility")
        _enum(ARTIFACT_PROVENANCE)(item["provenance"], f"{item_path}.provenance")
        _sha256_digest(item["subject_digest"], f"{item_path}.subject_digest")
        field_privacy = item["field_privacy"]
        if not isinstance(field_privacy, dict) or set(field_privacy) != privacy_fields:
            raise DispatchSchemaError(
                f"{item_path}.field_privacy must classify {sorted(privacy_fields)}"
            )
        for field, privacy_class in field_privacy.items():
            _enum(PRIVACY_CLASSES)(privacy_class, f"{item_path}.field_privacy.{field}")
        for private_field in ("location", "revision", "subject_digest"):
            if field_privacy[private_field] == "public_project":
                raise DispatchSchemaError(
                    f"{item_path}.field_privacy.{private_field} defaults to project_private "
                    "and cannot be public in canonical assurance"
                )


def _capability_receipt(value: Any, path: str) -> None:
    required = {
        "schema",
        "observed_utc",
        "valid_until_utc",
        "drift_inputs_digest",
        "profile_digest",
        "results",
        "receipt_digest",
    }
    if not isinstance(value, dict) or set(value) != required:
        raise DispatchSchemaError(f"{path} has invalid capability-receipt fields")
    if value["schema"] != "agent-mesh.capability-receipt.v1":
        raise DispatchSchemaError(f"{path}.schema is invalid")
    _UTC(value["observed_utc"], f"{path}.observed_utc")
    _UTC(value["valid_until_utc"], f"{path}.valid_until_utc")
    _sha256_digest(value["drift_inputs_digest"], f"{path}.drift_inputs_digest")
    _sha256_digest(value["profile_digest"], f"{path}.profile_digest")
    _sha256_digest(value["receipt_digest"], f"{path}.receipt_digest")
    results = value["results"]
    if not isinstance(results, list) or len(results) > 32:
        raise DispatchSchemaError(f"{path}.results must contain at most 32 items")
    seen: set[str] = set()
    for index, item in enumerate(results):
        item_path = f"{path}.results[{index}]"
        fields = {
            "capability",
            "declared",
            "effective",
            "evidence_class",
            "trust_source",
            "probe_version",
        }
        if not isinstance(item, dict) or set(item) != fields:
            raise DispatchSchemaError(f"{item_path} has invalid fields")
        _s(maxlen=64)(item["capability"], f"{item_path}.capability")
        if item["capability"] in seen:
            raise DispatchSchemaError(f"{path}.results contains a duplicate capability")
        seen.add(item["capability"])
        _bool(item["declared"], f"{item_path}.declared")
        _bool(item["effective"], f"{item_path}.effective")
        _enum(CAPABILITY_EVIDENCE_CLASSES)(item["evidence_class"], f"{item_path}.evidence_class")
        _s(maxlen=128)(item["trust_source"], f"{item_path}.trust_source")
        _s(maxlen=128)(item["probe_version"], f"{item_path}.probe_version")
    authority = {key: item for key, item in value.items() if key != "receipt_digest"}
    expected = hashlib.sha256(
        json.dumps(authority, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    if value["receipt_digest"] != expected:
        raise DispatchSchemaError(f"{path}.receipt_digest does not bind the receipt")


# --- per-kind schema: (required fields, optional fields) -> validators -------------------------
_FieldMap = dict[str, Validator]


def _spec(required: _FieldMap, optional: _FieldMap | None = None) -> tuple[_FieldMap, _FieldMap]:
    return required, optional or {}


_UTC = _s(maxlen=40)
_ID = _s(maxlen=128)

_SCHEMA: dict[str, tuple[_FieldMap, _FieldMap]] = {
    "dispatch_policy_frozen": _spec(
        {
            "contract_version": _enum(frozenset({"dispatch.v1"})),
            "policy_id": _ID,
            "request_id": _ID,
            "policy_digest": _sha256_digest,
            "purpose": _purpose,
            "role": _s(maxlen=128),
            "required_capabilities": _str_array_free(max_items=32, maxlen=64),
            "permission_ceiling": _enum(PERMISSION_CEILINGS),
            "response_contract": _response_contract,
            "artifact_contract": _artifact_contract,
            "provenance_requirements": _str_array(PROVENANCE_REQUIREMENTS),
            "retry": _retry_contract,
            "response_slot": _response_slot,
            "target": _dispatch_target,
            "runtime_profile_revision": _runtime_profile_revision,
            "management_level": _enum(MANAGEMENT_LEVELS),
            "frozen_utc": _UTC,
        },
        {
            "subject": _subject_binding,
            "assurance_policy": _assurance_policy,
        },
    ),
    "review_assurance_recorded": _spec(
        {
            "contract_version": _enum(frozenset({"review.v1"})),
            "assurance_id": _ID,
            "request_id": _ID,
            "attempt_id": _ID,
            "bound": _bool,
            "subject": _subject_binding,
            "reviewer": _reviewer,
            "policy_revision": _sha256_digest,
            "disposition": _enum(ASSURANCE_DISPOSITIONS),
            "finding_counts": _finding_counts,
            "artifact_refs": _typed_artifact_refs,
            "management_level": _enum(MANAGEMENT_LEVELS),
            "recorded_utc": _UTC,
            "valid_until_utc": _UTC,
            "authoritative": _bool,
        },
        {
            "policy_id": _ID,
            "policy_digest": _sha256_digest,
            "originating_response_id": _ID,
            "supersedes_assurance_id": _ID,
            "supersedes_expected_version": _positive_int,
        },
    ),
    "review_assurance_flagged": _spec(
        {
            "contract_version": _enum(frozenset({"review-assurance-lifecycle.v1"})),
            "assurance_id": _ID,
            "response_id": _ID,
            "expected_version": _positive_int,
            "next_version": _positive_int,
            "reason_code": _enum(ASSURANCE_LIFECYCLE_REASON_CODES),
            "note": _s(maxlen=512),
            "operation_key": _sha256_digest,
        }
    ),
    "review_assurance_retired": _spec(
        {
            "contract_version": _enum(frozenset({"review-assurance-lifecycle.v1"})),
            "assurance_id": _ID,
            "response_id": _ID,
            "expected_version": _positive_int,
            "next_version": _positive_int,
            "reason_code": _enum(ASSURANCE_LIFECYCLE_REASON_CODES),
            "note": _s(maxlen=512),
            "operation_key": _sha256_digest,
            "approval_authority_mode": _enum(
                frozenset({"explicit_human_approvers", "legacy_participants"})
            ),
            "approval_authority_revision": _sha256_digest,
        }
    ),
    "dispatch_run_planned": _spec(
        {
            "run_id": _ID,
            "run_mode": _enum(RUN_MODES),
            "input_message_id": _ID,
            "target_agent": _s(maxlen=64),
            "gen_ai_system": _s(maxlen=64),
            "model": _s(maxlen=128),
            "session_key": _s(maxlen=_MAX_STR),
            "session_key_source": _enum(SESSION_KEY_SOURCES),
            "session_uuid": _ID,
            "wave": _s(maxlen=_MAX_STR),
            "classification": _enum(CLASSIFICATIONS),
            "gate": _enum(PLANNED_GATES),
            "gate_reason_code": _enum(GATE_REASON_CODES),
            "requires_gate": _str_array(DISPATCH_GATE_HALT_CODES),
            "grounding": _grounding,
            "plan_artifact_hash": _digest,
            "adapter_capabilities": _adapter_capabilities,
            "target_event_seq": _int,
            "response_mode": _enum(RESPONSE_MODES),
            "planned_utc": _UTC,
            "status": _enum(PLANNED_STATUSES),
        },
        {
            "provider_inventory_digest": _sha256_digest,
            "policy_id": _ID,
            "policy_digest": _sha256_digest,
            "attempt_number": _positive_int,
            "previous_attempt_id": _ID,
            "management_level": _enum(MANAGEMENT_LEVELS),
            "runtime_profile": _s(maxlen=128),
            "effective_capability_evidence_digest": _sha256_digest,
            "capability_receipt": _capability_receipt,
        },
    ),
    "dispatch_run_blocked": _spec(
        {
            "run_id": _ID,
            "run_mode": _enum(RUN_MODES),
            "input_message_id": _ID,
            "target_agent": _s(maxlen=64),
            "gate": _enum(BLOCKED_GATES),
            "block_reason_codes": _str_array(BLOCK_REASON_CODES),
            "missing_count": _int,
            "planned_utc": _UTC,
        }
    ),
    "dispatch_lease_acquired": _spec(
        {
            "lease_id": _ID,
            "run_id": _ID,
            "input_message_id": _ID,
            "target_agent": _s(maxlen=64),
            "session_uuid": _ID,
            "ttl_seconds": _int,
            "created_utc": _UTC,
        }
    ),
    "dispatch_lease_released": _spec(
        {
            "lease_id": _ID,
            "run_id": _ID,
            "reason": _enum(RELEASE_REASONS),
            "released_utc": _UTC,
        },
        {"superseded_by_run_id": _ID},
    ),
    "dispatch_run_started": _spec(
        {
            "run_id": _ID,
            "session_uuid": _ID,
            "started_utc": _UTC,
        }
    ),
    "dispatch_run_completed": _spec(
        {
            "run_id": _ID,
            "output_message_id": _ID,
            "input_tokens": _int_or_null,
            "cache_read_input_tokens": _int_or_null,
            "cache_creation_input_tokens": _int_or_null,
            "total_input_tokens": _int_or_null,
            "completed_utc": _UTC,
        }
    ),
    "dispatch_run_failed": _spec(
        {
            "run_id": _ID,
            "error_class": _error_class,
            "failed_utc": _UTC,
        },
        {
            "response_candidate_status": _enum(RESPONSE_CANDIDATE_STATUSES),
            "response_candidate_reason": _enum(RESPONSE_CANDIDATE_REASONS),
        },
    ),
    "dispatch_run_terminated": _spec(
        {
            "run_id": _ID,
            "terminal_state": _enum(RUN_TERMINAL_STATES),
            "terminated_utc": _UTC,
        }
    ),
    "dispatch_retry_exhausted": _spec(
        {
            "policy_id": _ID,
            "run_id": _ID,
            "attempt_number": _positive_int,
            "max_attempts": _positive_int,
            "exhausted_utc": _UTC,
        }
    ),
}


def validate_dispatch_payload(kind: str, payload: Any) -> None:
    """Raise ``DispatchSchemaError`` if a dispatch event payload violates the allow-list schema.

    Non-dispatch kinds return immediately (this is composed with the mail/decisions validators).

    Args:
        kind: The event ``kind``.
        payload: The event ``payload`` (must be a dict for dispatch kinds).

    Raises:
        DispatchSchemaError: On any allow-list, enum, digest, length, newline, forbidden-family, or
            required-field violation. ``.code`` is ``DISPATCH_BODY_LEAK``.
    """
    if kind not in DISPATCH_EVENT_KINDS:
        return
    if not isinstance(payload, dict):
        raise DispatchSchemaError(f"{kind}.payload must be an object")
    required, optional = _SCHEMA[kind]
    allowed = set(required) | set(optional)
    for key in payload:
        _check_key_family(key)
        if key not in allowed:
            raise DispatchSchemaError(f"{kind}.payload has key {key!r} outside the allow-list")
    for field in required:
        if field not in payload:
            raise DispatchSchemaError(f"{kind}.payload is missing required field {field!r}")
    validators = {**required, **optional}
    for key, value in payload.items():
        validators[key](value, f"{kind}.payload.{key}")
    # Cross-field: a superseded release MUST name the replacing run; other reasons must not.
    if kind == "dispatch_lease_released":
        reason = payload.get("reason")
        has_successor = "superseded_by_run_id" in payload
        if reason == "superseded" and not has_successor:
            raise DispatchSchemaError(
                "dispatch_lease_released.payload.superseded_by_run_id is required when reason=superseded"
            )
        if reason != "superseded" and has_successor:
            raise DispatchSchemaError(
                "dispatch_lease_released.payload.superseded_by_run_id is only allowed when reason=superseded"
            )
    if kind == "dispatch_run_failed":
        _validate_failed_response_candidate_fields(payload)
    if kind == "dispatch_retry_exhausted" and (
        payload["attempt_number"] != payload["max_attempts"]
    ):
        raise DispatchSchemaError("dispatch_retry_exhausted attempt_number must equal max_attempts")
    if kind == "dispatch_policy_frozen":
        expected_digest = dispatch_policy_digest(payload)
        if payload["policy_digest"] != expected_digest:
            raise DispatchSchemaError(
                "dispatch_policy_frozen.payload.policy_digest does not bind the policy snapshot"
            )
    if kind == "dispatch_run_planned":
        _validate_v1_attempt_fields(payload)
    if kind == "review_assurance_recorded":
        _validate_assurance_binding(payload)


def dispatch_policy_digest(payload: dict[str, Any]) -> str:
    """Return the SHA-256 commitment for one frozen policy payload."""

    authority = {key: value for key, value in payload.items() if key != "policy_digest"}
    encoded = json.dumps(
        authority, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _validate_v1_attempt_fields(payload: dict[str, Any]) -> None:
    fields = {
        "policy_id",
        "policy_digest",
        "attempt_number",
        "management_level",
        "runtime_profile",
        "effective_capability_evidence_digest",
        "capability_receipt",
    }
    present = fields.intersection(payload)
    if present and present != fields:
        raise DispatchSchemaError(
            "dispatch_run_planned dispatch.v1 attempt fields must be present together"
        )
    if not present and "previous_attempt_id" in payload:
        raise DispatchSchemaError(
            "dispatch_run_planned previous_attempt_id requires dispatch.v1 attempt fields"
        )
    if present:
        attempt_number = payload["attempt_number"]
        if attempt_number == 1 and "previous_attempt_id" in payload:
            raise DispatchSchemaError(
                "dispatch_run_planned first attempt must not name a predecessor"
            )
        if attempt_number > 1 and "previous_attempt_id" not in payload:
            raise DispatchSchemaError("dispatch_run_planned retry requires previous_attempt_id")
        if (
            payload["effective_capability_evidence_digest"]
            != payload["capability_receipt"]["receipt_digest"]
        ):
            raise DispatchSchemaError("dispatch_run_planned capability receipt digest mismatch")


def _validate_assurance_binding(payload: dict[str, Any]) -> None:
    bound = payload["bound"]
    policy_fields = {"policy_id", "policy_digest", "originating_response_id"}
    present = {field for field in policy_fields if field in payload}
    if bound and present != policy_fields:
        raise DispatchSchemaError(
            "review_assurance_recorded bound assurance requires policy_id, policy_digest, "
            "and originating_response_id"
        )
    if not bound and present:
        raise DispatchSchemaError(
            "review_assurance_recorded unbound assurance must not claim policy or response binding"
        )
    if bound and payload["subject"]["type"] == "unbound":
        raise DispatchSchemaError("bound assurance requires a concrete subject")
    if not bound and payload["authoritative"]:
        raise DispatchSchemaError("unbound assurance cannot be authoritative")
    supersession_fields = {"supersedes_assurance_id", "supersedes_expected_version"}
    supplied_supersession = supersession_fields.intersection(payload)
    if supplied_supersession and supplied_supersession != supersession_fields:
        raise DispatchSchemaError(
            "review assurance supersession requires assurance id and expected version together"
        )
    if supplied_supersession and not bound:
        raise DispatchSchemaError("unbound assurance cannot supersede authoritative evidence")


def _validate_failed_response_candidate_fields(payload: dict[str, Any]) -> None:
    has_status = "response_candidate_status" in payload
    has_reason = "response_candidate_reason" in payload
    if has_status != has_reason:
        raise DispatchSchemaError(
            "dispatch_run_failed.payload.response_candidate_status and response_candidate_reason must appear together"
        )
    if not has_status:
        return
    error_class = payload.get("error_class")
    if error_class not in {"OutputNotPosted", "OutputRejected"}:
        raise DispatchSchemaError(
            "dispatch_run_failed response candidate fields are only allowed with output candidate error classes"
        )
    status = payload["response_candidate_status"]
    reason = payload["response_candidate_reason"]
    if error_class == "OutputRejected" and status != "rejected":
        raise DispatchSchemaError("dispatch_run_failed OutputRejected candidates must be rejected")
    if status == "ready" and reason != "ok":
        raise DispatchSchemaError(
            "dispatch_run_failed ready response candidates must use reason=ok"
        )
    if status == "rejected" and reason in {"ok", "not_successful"}:
        raise DispatchSchemaError(
            "dispatch_run_failed rejected response candidates must use an output-policy rejection reason"
        )
