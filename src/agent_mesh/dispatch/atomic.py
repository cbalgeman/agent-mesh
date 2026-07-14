"""Roll-forward-safe, exact multi-file commit for a host's projection unit (StoreAdapter.working_copy).

Replacing several files with per-file ``os.replace`` is atomic per FILE but not for the GROUP, and
it cannot make the live unit EXACTLY equal a validated candidate set when a file should disappear. So
this module commits the unit as a journaled set of REPLACE and DELETE operations over the FULL
candidate set:

  * for each candidate, if the validated copy has it -> replace the live file with the copy;
  * if the validated copy does NOT have it -> delete the live file (no stale file survives).

``promote_unit`` durably journals the ops, performs them, then clears the journal. If a crash
interrupts the sequence, ``recover_unit`` re-applies the (idempotent) ops from the surviving copies,
reaching the fully-validated new unit -- a reader never observes a mixed or stale-file state.

``recover_unit`` must run before any read of the unit; recovery FAILURE while a journal is pending
must be loud and blocking (the host must not proceed to read a possibly-mixed unit).
"""
from __future__ import annotations

import json
import os
from pathlib import Path

JOURNAL_NAME = ".promote-journal.json"


def _fsync(path: Path) -> None:
    """Best-effort durability: fsync a file or directory so the journal survives a crash."""
    try:
        flags = os.O_RDONLY
        if hasattr(os, "O_DIRECTORY") and path.is_dir():
            flags |= os.O_DIRECTORY
        fd = os.open(str(path), flags)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError:
        pass


def _plan_ops(pairs: list[tuple[Path, Path]]) -> list[list[str]]:
    """Per candidate (live, copy): replace if the copy exists, else delete the live file."""
    ops: list[list[str]] = []
    for live, copy in pairs:
        if Path(copy).exists():
            ops.append(["replace", str(live), str(copy)])
        else:
            ops.append(["delete", str(live), str(copy)])
    return ops


def _apply_ops(ops: list) -> None:
    """Idempotent: a replace whose copy is already consumed, or a delete whose target is already
    gone, is a no-op -- so re-applying after an interruption rolls forward to the same final unit."""
    for op, live, copy in ops:
        if op == "replace":
            if Path(copy).exists():
                os.replace(Path(copy), Path(live))
        elif op == "delete":
            try:
                os.remove(Path(live))
            except FileNotFoundError:
                pass


def promote_unit(pairs: list[tuple[Path, Path]], journal: Path) -> None:
    """Commit the FULL candidate set so the live unit exactly equals the validated copies: replace
    present copies, delete absent ones. Journaled for roll-forward recovery on interruption."""
    ops = _plan_ops(pairs)
    journal.write_text(json.dumps({"status": "committing", "ops": ops}), encoding="utf-8")
    _fsync(journal)
    _fsync(journal.parent)
    _apply_ops(ops)
    journal.unlink()
    _fsync(journal.parent)


def recover_unit(journal: Path) -> bool:
    """If a promote was interrupted (journal present + committing), roll forward: re-apply the ops
    (idempotent) from the surviving copies, then clear the journal. Returns True if it recovered.
    Raises if an op cannot be completed (the caller must fail closed). Call before any read."""
    if not journal.exists():
        return False
    try:
        data = json.loads(journal.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        journal.unlink()   # a partial/corrupt journal means the promote never began; the live unit
        return False       # is the consistent pre-promote state -> safe to proceed after clearing
    if data.get("status") != "committing":
        journal.unlink()
        return False
    _apply_ops(data.get("ops", []))
    journal.unlink()
    _fsync(journal.parent)
    return True
