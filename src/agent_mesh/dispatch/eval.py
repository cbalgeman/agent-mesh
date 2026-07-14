"""Generic deterministic eval runner + risky-path gate for the dispatch layer (host-agnostic).

The host supplies the ``EvalSuite`` (its cases + scorer/suite versions); the engine validates it
(failing closed on an empty or blank contract), runs each case under ``StoreAdapter.isolated()``
(never the live store), computes the contract fingerprint, and returns a report VALUE. The engine
persists nothing -- a host ``RunRecorder`` decides where a report goes. ``gate()`` is the
default-deny go/no-go for a risky apply: GO only when a passing eval scored against the approved
contract exists, that contract still matches, and nothing else the approval bound has drifted.
"""
from __future__ import annotations

import hashlib
import json

from .adapters import StoreAdapter
from .types import EvalSuite

GATE_HALTS = (
    "missing-or-failing-eval-results",
    "eval-fingerprint-mismatch",
    "stale-artifact-hash",
    "approval-identity-missing",
    "self-approval",
    "changed-git-sha",
    "changed-touched-row-version",
    "invariant-failure",
)


def _validate_suite(suite: EvalSuite) -> None:
    """Fail closed: the suite must anchor an approval, so it needs cases, nonblank scorer/suite
    versions, and case IDs that are nonblank and unique (the case-set digest and report rows are
    keyed on those IDs)."""
    if not suite.cases:
        raise ValueError("eval suite has no cases (fails closed)")
    if not str(suite.scorer_version).strip():
        raise ValueError("eval suite scorer_version is blank (fails closed)")
    if not str(suite.suite_version).strip():
        raise ValueError("eval suite suite_version is blank (fails closed)")
    names = [str(case.name).strip() for case in suite.cases]
    if any(not name for name in names):
        raise ValueError("eval case name is blank (fails closed)")
    if len(set(names)) != len(names):
        raise ValueError("eval case names must be unique (fails closed)")


def case_set_digest(suite: EvalSuite) -> str:
    """Order-independent digest of the SET of cases plus the scorer/suite versions. Changing the
    case set or the scorer moves it even when the suite version is not bumped."""
    _validate_suite(suite)
    blob = json.dumps(
        {"cases": sorted(case.name for case in suite.cases),
         "scorer": suite.scorer_version, "suite": suite.suite_version},
        sort_keys=True,
    )
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


def eval_fingerprint(suite: EvalSuite) -> str:
    """The eval CONTRACT fingerprint bound into the approval artifact."""
    return f"{suite.suite_version}:{suite.scorer_version}:{case_set_digest(suite)}"


def run_evals(suite: EvalSuite, store: StoreAdapter, *, judge=None) -> dict:
    """Run every case under ``store.isolated()`` (the live store is never touched) and return a
    report value. ``judge``, if given, is an ADVISORY callable ``judge(name, passed, detail) ->
    str`` recorded per case; it can never flip a pass/fail. Persists nothing."""
    _validate_suite(suite)
    results = []
    for case in suite.cases:
        with store.isolated():
            try:
                passed, detail = case.fn()
            except Exception as exc:  # noqa: BLE001 - a crashing case is a failed case
                passed, detail = False, f"case raised: {exc}"
        entry = {"name": case.name, "passed": bool(passed), "detail": detail}
        if judge is not None:
            try:
                entry["advisory_verdict"] = str(judge(case.name, passed, detail))
            except Exception as exc:  # noqa: BLE001 - advisory only, never blocks
                entry["advisory_verdict"] = f"<judge-error: {exc}>"
        results.append(entry)
    return {
        "suite_version": suite.suite_version,
        "scorer_version": suite.scorer_version,
        "case_set_digest": case_set_digest(suite),
        "eval_fingerprint": eval_fingerprint(suite),
        "passed": all(r["passed"] for r in results),
        "counts": {"total": len(results), "passed": sum(r["passed"] for r in results),
                   "failed": sum(not r["passed"] for r in results)},
        "results": results,
        "judge_mode": "advisory" if judge is not None else "off",
    }


def gate(eval_report: dict | None, approval: dict, observed: dict) -> dict:
    """Go/no-go for a risky apply. ``approval`` is the artifact captured at approval time;
    ``observed`` is the world now. Default-deny: any uncertainty is a halt."""
    halts = []
    if not eval_report or not eval_report.get("passed"):
        halts.append("missing-or-failing-eval-results")
    else:
        approved_fp = approval.get("eval_fingerprint")
        if eval_report.get("eval_fingerprint") != approved_fp or observed.get("eval_fingerprint") != approved_fp:
            halts.append("eval-fingerprint-mismatch")
    if observed.get("artifact_hash") != approval.get("artifact_hash"):
        halts.append("stale-artifact-hash")
    # default-deny on identity: a missing/blank approver or requester cannot authorize anything,
    # and an approver that equals the requester is a self-approval.
    approver = str(approval.get("approver") or "").strip()
    requester = str(approval.get("requester") or "").strip()
    if not approver or not requester:
        halts.append("approval-identity-missing")
    elif approver == requester:
        halts.append("self-approval")
    if observed.get("git_sha") != approval.get("git_sha"):
        halts.append("changed-git-sha")
    if observed.get("touched_rows") != approval.get("touched_rows"):
        halts.append("changed-touched-row-version")
    if observed.get("new_invariants"):
        halts.append("invariant-failure")
    return {"go": not halts, "halts": halts}
