"""Generic transactional risky-apply guard for the optional dispatch layer (host-agnostic).

The guard supplies the reusable machinery; a host's ``StoreAdapter`` supplies all domain meaning.
For one packet it: normalizes -> plans the would-touch rows (read-only) -> binds an approval
artifact over EVERY would-touch row plus the git SHA, policy/guard versions, and the eval
fingerprint -> snapshots the projection unit -> applies -> rebuilds projections -> checks
invariants -> and rolls back (exact restore over the FULL candidate set, deleting any file the
apply created) on either an apply error OR a NEWLY introduced invariant (delta = post minus pre).

It carries no host knowledge, assumes no host paths, and persists nothing: ``artifact_hash`` takes
every input as an explicit VALUE (git SHA, policy version, eval fingerprint), so nothing is read
from a host global. Pre-existing invariant violations are surfaced but never block an unrelated
apply.
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import tempfile
from pathlib import Path

from agent_mesh.core.chain import ChainAnchor, capture_anchor, verify_chain

from .adapters import StoreAdapter

GUARD_VERSION = "dispatch-guard-v1"


def _capture_chain_anchor(store: StoreAdapter) -> ChainAnchor | None:
    """The pre-apply tail of the store's canonical log (the known-good precondition), or ``None``
    when the store is not canonical-event-backed."""
    events_path = store.events_path()
    if events_path is None:
        return None
    return capture_anchor(events_path)


def _chain_invariant(store: StoreAdapter, anchor: ChainAnchor | None) -> list[str]:
    """A post-apply invariant: verify the events THIS apply appended chain onto the pre-apply tail.

    This checks the APPENDED SUFFIX only -- the anchor is the known-good precondition, so it does NOT
    re-validate the pre-existing log (a malformed pre-anchor tail is not this check's concern; the
    CLI / periodic full walk is the audit backstop). No-op when the store exposes no canonical log.
    """
    if anchor is None:
        return []
    events_path = store.events_path()
    if events_path is None:
        return []
    result = verify_chain(events_path, anchor=anchor)
    return [] if result.ok else [f"CHAIN_BROKEN: {result.error}"]


def _snapshot(paths: list[Path], tmpdir: str) -> dict:
    """Manifest over the FULL candidate set: ``{path: snapshot_copy or None}``. ``None`` records a
    file absent at snapshot time, so rollback deletes it if the apply created it."""
    manifest: dict = {}
    for index, raw in enumerate(paths):
        path = Path(raw)
        if path.exists():
            dst = Path(tmpdir) / f"{index:02d}-{path.name}"
            shutil.copy2(path, dst)
            manifest[str(path)] = dst
        else:
            manifest[str(path)] = None
    return manifest


def _restore(manifest: dict) -> None:
    """Roll the candidate set back to its exact pre-apply state: files that existed are atomically
    replaced from their snapshot (same-dir temp + ``os.replace``, temp cleaned up on failure);
    files created during the apply are removed."""
    for original, snap in manifest.items():
        dest = Path(original)
        if snap is not None:
            tmp = dest.with_name(dest.name + ".restore.tmp")
            try:
                shutil.copy2(snap, tmp)
                os.replace(tmp, dest)
            finally:
                if tmp.exists():
                    tmp.unlink()
        elif dest.exists():
            dest.unlink()


def artifact_hash(
    *,
    normalized: dict,
    plan: list[dict],
    touched_rows: dict,
    git_sha: str,
    policy_version: str,
    eval_fingerprint: str,
    guard_version: str = GUARD_VERSION,
    target_event_seq: str = "",
    input_message_id: str = "",
) -> tuple[str, dict]:
    """The approval artifact: a hash over everything whose change must expire the approval. Every
    input is an explicit value (no host globals). ``plan``/``touched_rows`` come from the host's
    ``StoreAdapter``, ``git_sha`` from a ``ProjectStateProvider``, ``eval_fingerprint`` from the
    host's ``EvalSuite``."""
    plan_digest = hashlib.sha256(
        json.dumps(plan, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()[:16]
    binding = {
        "packet": normalized,
        "plan_digest": plan_digest,
        "touched_rows": touched_rows,
        "git_sha": git_sha,
        "policy_version": policy_version,
        "guard_version": guard_version,
        "eval_fingerprint": eval_fingerprint,
        "target_event_seq": str(target_event_seq),
        "input_message_id": input_message_id,
    }
    blob = json.dumps(binding, sort_keys=True, default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16], binding


def guarded_apply(
    packet: dict,
    store: StoreAdapter,
    *,
    git_sha: str,
    eval_fingerprint: str,
    policy_version: str,
    apply: bool = False,
    atomic_promote: bool = False,
    actor: str = "agent",
    response_ref: str = "",
    target_event_seq: str = "",
    input_message_id: str = "",
) -> dict:
    """Transactionally apply ``packet`` through ``store`` (dry-run by default). On ``apply=True`` it
    snapshots, applies, rebuilds, checks invariants, and rolls back on any apply error or a newly
    introduced invariant. With ``atomic_promote=True`` it instead applies to a SAME-FS COPY of the
    store and atomically promotes it only after invariants pass -- the live store is never touched
    on failure (the live-safe path). Returns a host-neutral result dict; persists nothing."""
    normalized = store.normalize(packet, response_ref=response_ref)
    plan = store.plan_transitions(normalized)
    touched = store.touched_row_versions(plan)
    artifact, binding = artifact_hash(
        normalized=normalized,
        plan=plan,
        touched_rows=touched,
        git_sha=git_sha,
        policy_version=policy_version,
        eval_fingerprint=eval_fingerprint,
        target_event_seq=target_event_seq,
        input_message_id=input_message_id,
    )
    summary = {
        "artifact_hash": artifact,
        "binding_keys": sorted(binding.keys()),
        "git_sha": git_sha[:12],
        "policy_version": policy_version,
        "guard_version": GUARD_VERSION,
        "eval_fingerprint": eval_fingerprint,
        "transitions": len(plan),
        "touched_rows": sorted(touched.keys()),
    }
    pre = store.check_invariants()
    if not apply:
        return {"ok": True, "mode": "dry-run", "pre_apply_invariants": pre, **summary}
    if atomic_promote:
        return _apply_atomic(store, packet, actor, response_ref, set(pre), pre, summary)
    return _apply_snapshot(store, packet, actor, response_ref, set(pre), pre, summary)


def _apply_snapshot(store, packet, actor, response_ref, pre_set, pre, summary) -> dict:
    """Bridge path: snapshot the projection set, apply in place, restore on any error/new invariant."""
    paths = [Path(p) for p in store.snapshot_paths()]
    events_path = store.events_path()
    if events_path is not None and events_path not in paths:
        # An event-backed store appends in place on this path; include it so a chain failure (or any
        # rollback) reverts the events log with the rest of the unit.
        paths.append(events_path)
    with tempfile.TemporaryDirectory() as tmpdir:
        manifest = _snapshot(paths, tmpdir)
        anchor = _capture_chain_anchor(store)
        try:
            result = store.apply(packet, actor=actor, response_ref=response_ref)
            store.rebuild_projections()
        except Exception as exc:  # noqa: BLE001 - any apply/rebuild failure must roll back
            _restore(manifest)
            return {"ok": False, "mode": "apply-error-rolled-back", "restored": True,
                    "error": str(exc), **summary}
        new_violations = [
            v for v in store.check_invariants() if v not in pre_set
        ] + _chain_invariant(store, anchor)
        if new_violations:
            _restore(manifest)
            return {"ok": False, "mode": "applied-rolled-back", "restored": True,
                    "post_apply_invariant_failures": new_violations,
                    "preexisting_invariants": pre, **summary}
        return {"ok": True, "mode": "applied", "result": result,
                "preexisting_invariants": pre, **summary}


def _apply_atomic(store, packet, actor, response_ref, pre_set, pre, summary) -> dict:
    """Live-safe path: apply + check on a same-fs COPY; promote atomically only if invariants pass.
    The live store is mutated by ONE atomic promote or not at all -- never on failure."""
    with store.working_copy() as promote:
        # Capture inside working_copy so events_path() yields the COPY's log; the apply appends to the
        # copy, the chain check runs on the copy, and a failure discards the copy (live untouched).
        anchor = _capture_chain_anchor(store)
        try:
            result = store.apply(packet, actor=actor, response_ref=response_ref)
            store.rebuild_projections()
        except Exception as exc:  # noqa: BLE001 - error on the copy; live store untouched (no promote)
            return {"ok": False, "mode": "apply-error-discarded", "restored": True,
                    "error": str(exc), **summary}
        new_violations = [
            v for v in store.check_invariants() if v not in pre_set
        ] + _chain_invariant(store, anchor)
        if new_violations:
            return {"ok": False, "mode": "applied-discarded", "restored": True,
                    "post_apply_invariant_failures": new_violations,
                    "preexisting_invariants": pre, **summary}
        promote()  # atomically replace the live files with the validated copy
        return {"ok": True, "mode": "applied-atomic", "result": result,
                "preexisting_invariants": pre, **summary}
