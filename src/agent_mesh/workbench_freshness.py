"""Bounded code-generation fingerprinting for long-running Workbench servers."""

from __future__ import annotations

import hashlib
import os
import stat
from pathlib import Path


MAX_WORKBENCH_CODE_FILES = 512
MAX_WORKBENCH_CODE_ENTRIES = 4096
MAX_WORKBENCH_CODE_BYTES = 32 * 1024 * 1024
_READ_CHUNK_BYTES = 64 * 1024


class WorkbenchCodeError(RuntimeError):
    """Raised when the installed Agent Mesh source inventory is unsafe or unavailable."""


def workbench_code_fingerprint(package_root: Path | None = None) -> str:
    """Digest Python sources with hard inventory and read bounds."""

    root = (package_root or Path(__file__).resolve().parent).resolve()
    if not root.is_dir():
        raise WorkbenchCodeError("Agent Mesh package source directory is unavailable")

    directories = [root]
    records: list[tuple[str, int, bytes]] = []
    entry_count = 0
    total_bytes = 0
    while directories:
        directory = directories.pop()
        try:
            iterator = os.scandir(directory)
        except OSError as exc:
            raise WorkbenchCodeError("Agent Mesh package sources could not be inventoried") from exc
        try:
            with iterator:
                for entry in iterator:
                    entry_count += 1
                    if entry_count > MAX_WORKBENCH_CODE_ENTRIES:
                        raise WorkbenchCodeError(
                            "Agent Mesh package source inventory exceeds its entry bound"
                        )
                    try:
                        if entry.is_symlink():
                            raise WorkbenchCodeError(
                                "Agent Mesh package source inventory contains a symlink"
                            )
                        if entry.is_dir(follow_symlinks=False):
                            directories.append(Path(entry.path))
                            continue
                        if not entry.name.endswith(".py"):
                            continue
                        metadata = entry.stat(follow_symlinks=False)
                    except OSError as exc:
                        raise WorkbenchCodeError(
                            "Agent Mesh package source inventory could not be inspected"
                        ) from exc
                    if not stat.S_ISREG(metadata.st_mode):
                        raise WorkbenchCodeError(
                            "Agent Mesh package source inventory is not regular"
                        )
                    if len(records) >= MAX_WORKBENCH_CODE_FILES:
                        raise WorkbenchCodeError(
                            "Agent Mesh package source inventory exceeds its file bound"
                        )
                    if (
                        metadata.st_size < 0
                        or total_bytes + metadata.st_size > MAX_WORKBENCH_CODE_BYTES
                    ):
                        raise WorkbenchCodeError(
                            "Agent Mesh package source inventory exceeds its byte bound"
                        )
                    file_digest, actual_bytes = _bounded_file_digest(
                        Path(entry.path),
                        expected=metadata,
                        remaining_bytes=MAX_WORKBENCH_CODE_BYTES - total_bytes,
                    )
                    try:
                        # DirEntry.stat() may return metadata cached by the first call.
                        # Use a fresh pathname lookup to detect atomic replacement after
                        # the opened descriptor was hashed.
                        after = os.stat(entry.path, follow_symlinks=False)
                    except OSError as exc:
                        raise WorkbenchCodeError(
                            "Agent Mesh package source changed during fingerprinting"
                        ) from exc
                    before_identity = (
                        metadata.st_dev,
                        metadata.st_ino,
                        metadata.st_mode,
                        metadata.st_size,
                        metadata.st_mtime_ns,
                    )
                    after_identity = (
                        after.st_dev,
                        after.st_ino,
                        after.st_mode,
                        after.st_size,
                        after.st_mtime_ns,
                    )
                    if before_identity != after_identity or actual_bytes != metadata.st_size:
                        raise WorkbenchCodeError(
                            "Agent Mesh package source changed during fingerprinting"
                        )
                    total_bytes += actual_bytes
                    relative = Path(entry.path).relative_to(root).as_posix()
                    records.append((relative, actual_bytes, file_digest))
        except OSError as exc:
            raise WorkbenchCodeError("Agent Mesh package sources could not be inventoried") from exc

    if not records:
        raise WorkbenchCodeError("Agent Mesh package source inventory is empty")
    records.sort(key=lambda item: item[0])
    digest = hashlib.sha256(b"agent-mesh.workbench-code.v2\0")
    for relative, size, file_digest in records:
        relative_bytes = relative.encode("utf-8")
        digest.update(len(relative_bytes).to_bytes(4, "big"))
        digest.update(relative_bytes)
        digest.update(size.to_bytes(8, "big"))
        digest.update(file_digest)
    return digest.hexdigest()


def _bounded_file_digest(
    path: Path,
    *,
    expected: os.stat_result,
    remaining_bytes: int,
) -> tuple[bytes, int]:
    digest = hashlib.sha256()
    size = 0
    flags = (
        os.O_RDONLY
        | getattr(os, "O_CLOEXEC", 0)
        | getattr(os, "O_NOFOLLOW", 0)
        | getattr(os, "O_NONBLOCK", 0)
    )
    if os.name == "nt":  # pragma: no cover - Windows-specific binary mode
        flags |= getattr(os, "O_BINARY", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise WorkbenchCodeError("Agent Mesh package source could not be opened") from exc
    try:
        opened = os.fstat(descriptor)
        opened_identity = (
            opened.st_dev,
            opened.st_ino,
            opened.st_mode,
            opened.st_size,
            opened.st_mtime_ns,
        )
        expected_identity = (
            expected.st_dev,
            expected.st_ino,
            expected.st_mode,
            expected.st_size,
            expected.st_mtime_ns,
        )
        if opened_identity != expected_identity or not stat.S_ISREG(opened.st_mode):
            raise WorkbenchCodeError("Agent Mesh package source changed before fingerprinting")
        while True:
            allowed = remaining_bytes - size
            chunk = os.read(descriptor, min(_READ_CHUNK_BYTES, allowed + 1))
            if not chunk:
                break
            size += len(chunk)
            if size > remaining_bytes:
                raise WorkbenchCodeError(
                    "Agent Mesh package source inventory exceeds its byte bound"
                )
            digest.update(chunk)
        after = os.fstat(descriptor)
        after_identity = (
            after.st_dev,
            after.st_ino,
            after.st_mode,
            after.st_size,
            after.st_mtime_ns,
        )
        if after_identity != opened_identity or size != opened.st_size:
            raise WorkbenchCodeError("Agent Mesh package source changed during fingerprinting")
    except OSError as exc:
        raise WorkbenchCodeError("Agent Mesh package source could not be read") from exc
    finally:
        try:
            os.close(descriptor)
        except OSError:
            pass
    return digest.digest(), size
