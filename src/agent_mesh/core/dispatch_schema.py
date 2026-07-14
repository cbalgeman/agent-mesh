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
RELEASE_REASONS = frozenset({"completed", "failed", "expired", "superseded"})

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
            raise DispatchSchemaError(f"{path} must not contain a control or line-separator character")
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


# --- per-kind schema: (required fields, optional fields) -> validators -------------------------
_FieldMap = dict[str, Validator]


def _spec(required: _FieldMap, optional: _FieldMap | None = None) -> tuple[_FieldMap, _FieldMap]:
    return required, optional or {}


_UTC = _s(maxlen=40)
_ID = _s(maxlen=128)

_SCHEMA: dict[str, tuple[_FieldMap, _FieldMap]] = {
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
        }
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
        raise DispatchSchemaError("dispatch_run_failed ready response candidates must use reason=ok")
    if status == "rejected" and reason in {"ok", "not_successful"}:
        raise DispatchSchemaError(
            "dispatch_run_failed rejected response candidates must use an output-policy rejection reason"
        )
