"""Prefixed ULID minting, shared by every domain that needs a sortable opaque id.

A ULID body is a 26-char Crockford base-32 encoding of a 128-bit value: a 48-bit millisecond
timestamp in the high bits (so ids sort by creation time) plus 80 bits of randomness. The mesh uses
``ev_<ulid>`` for events, ``run_<ulid>`` for dispatch runs, and ``lease_<ulid>`` for dispatch leases;
they all share this one implementation so the format can never drift between domains.
"""
from __future__ import annotations

import secrets
import time

_CROCKFORD32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def new_ulid(prefix: str) -> str:
    """Return a ``<prefix>_<ulid>`` identifier with a ULID-compatible 26-char body.

    Args:
        prefix: Short domain tag placed before the underscore (e.g. ``"ev"``, ``"run"``, ``"lease"``).

    Returns:
        A string like ``run_01J9Z3KQ8VN7XME4WB0CF2RTAH`` whose 26-char body sorts by creation time.

    Example:
        >>> new_ulid("run").startswith("run_")
        True
    """
    if not prefix:
        raise ValueError("new_ulid prefix must be a non-empty string")
    timestamp_ms = int(time.time() * 1000) & ((1 << 48) - 1)
    randomness = secrets.randbits(80)
    value = (timestamp_ms << 80) | randomness
    chars = [_CROCKFORD32[(value >> shift) & 0x1F] for shift in range(125, -1, -5)]
    return f"{prefix}_" + "".join(chars)
