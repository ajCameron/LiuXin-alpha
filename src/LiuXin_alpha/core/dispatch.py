"""Shared dispatch heuristics for core method routing."""

from __future__ import annotations


WRITE_PREFIXES = (
    "add",
    "create",
    "delete",
    "dirty",
    "dupe",
    "ensure",
    "interlink",
    "link",
    "lock",
    "persist",
    "publish",
    "refresh",
    "register",
    "remove",
    "set",
    "shutdown",
    "sync",
    "unlink",
    "update",
)

WRITE_EXACT = {
    "backup",
    "bootstrap_storage_manager",
    "close",
}


def looks_like_write_method(method_name: str) -> bool:
    """Best-effort write-path classifier for proxy auto-dispatch."""
    token = str(method_name).strip().lower()
    if token in WRITE_EXACT:
        return True
    return token.startswith(WRITE_PREFIXES)


__all__ = [
    "WRITE_EXACT",
    "WRITE_PREFIXES",
    "looks_like_write_method",
]
