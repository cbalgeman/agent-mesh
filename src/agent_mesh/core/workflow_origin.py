"""Validated workflow-origin provenance shared across Agent Mesh domains."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

WORKFLOW_ORIGINS = (
    "self-hosting",
    "runtime-integration",
    "external-input",
)

ORIGIN_EVENT_KINDS = frozenset(
    {"req_created", "res_posted", "backlog_item_upserted", "message_ref_added"}
)


class WorkflowOriginValidationError(ValueError):
    """Raised when a new event carries invalid or conflicting origin data."""


@dataclass(frozen=True)
class ProjectedWorkflowOrigin:
    value: str | None
    valid: bool
    source: str


def validate_event_workflow_origin(
    kind: str,
    payload: dict[str, Any],
    entity_id: str,
) -> None:
    """Validate new origin-bearing writes while leaving historical replay compatible."""

    if kind not in ORIGIN_EVENT_KINDS:
        return
    explicit = payload.get("workflow_origin")
    if explicit is not None:
        _validate_value(explicit, f"{kind}.payload.workflow_origin", entity_id)
    refs = _origin_ref_values(_event_refs(kind, payload))
    for value in refs:
        _validate_value(value, f"{kind}.payload origin ref", entity_id)
    unique_refs = set(refs)
    if len(unique_refs) > 1:
        raise WorkflowOriginValidationError(
            f"{kind}.payload has conflicting origin refs for {entity_id}: "
            + ", ".join(sorted(unique_refs))
        )
    if explicit is not None and unique_refs and str(explicit).strip() not in unique_refs:
        raise WorkflowOriginValidationError(
            f"{kind}.payload.workflow_origin conflicts with origin ref for {entity_id}"
        )


def project_workflow_origin(
    payload: dict[str, Any],
    *,
    inherited: ProjectedWorkflowOrigin | None = None,
) -> ProjectedWorkflowOrigin:
    """Project explicit or legacy-ref origin without rejecting historical records."""

    explicit = payload.get("workflow_origin")
    refs = sorted(set(_origin_ref_values(payload.get("refs", []))))
    if explicit is not None:
        value = str(explicit).strip()
        valid = value in WORKFLOW_ORIGINS
        if refs and (len(refs) > 1 or refs[0] != value):
            valid = False
        return ProjectedWorkflowOrigin(value or None, valid, "explicit")
    if refs:
        value = refs[0]
        valid = len(refs) == 1 and value in WORKFLOW_ORIGINS
        return ProjectedWorkflowOrigin(value, valid, "legacy_ref")
    if inherited is not None and inherited.value:
        return ProjectedWorkflowOrigin(inherited.value, inherited.valid, "inherited")
    return ProjectedWorkflowOrigin(None, True, "none")


def refs_with_workflow_origin(
    refs: Iterable[Any],
    workflow_origin: str | None,
    *,
    replace: bool = False,
) -> list[Any]:
    """Add the compatibility origin ref while keeping the first-class field authoritative."""

    items = list(refs)
    if not workflow_origin:
        return items
    if replace:
        items = [item for item in items if not _is_origin_ref(item)]
    existing = _origin_ref_values(items)
    if workflow_origin not in existing:
        items.append({"type": "origin", "value": workflow_origin})
    return items


def _event_refs(kind: str, payload: dict[str, Any]) -> list[Any]:
    if kind == "message_ref_added" and payload.get("ref_type") == "origin":
        return [{"type": "origin", "value": payload.get("ref_value", "")}]
    refs = payload.get("refs", [])
    return refs if isinstance(refs, list) else []


def _origin_ref_values(refs: Iterable[Any]) -> list[str]:
    values: list[str] = []
    for item in refs:
        if isinstance(item, dict) and str(item.get("type", "")).strip() == "origin":
            values.append(str(item.get("value", "")).strip())
        elif isinstance(item, str) and item.startswith("origin:"):
            values.append(item.partition(":")[2].strip())
    return values


def _is_origin_ref(item: Any) -> bool:
    if isinstance(item, dict):
        return str(item.get("type", "")).strip() == "origin"
    return isinstance(item, str) and item.startswith("origin:")


def _validate_value(value: Any, field: str, entity_id: str) -> None:
    normalized = str(value).strip()
    if normalized not in WORKFLOW_ORIGINS:
        raise WorkflowOriginValidationError(
            f"{field} invalid for {entity_id}: {normalized!r}; "
            f"expected one of {', '.join(WORKFLOW_ORIGINS)}"
        )
