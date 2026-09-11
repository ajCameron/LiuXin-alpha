"""
Classify method names for automatic Core proxy command/query routing.

The shared exact-name/prefix lists are a naming heuristic, not authorization,
effect analysis, or proof that a method classified as a query cannot write.
"""

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
    """
    Match a stripped lowercase method name against known write names or prefixes.

    Prefixes need no separator or word boundary, so names such as ``address``
    match ``add``. An unrecognized name is classified as read-like without
    inspecting its implementation.

    Example:
        >>> looks_like_write_method(" Update_title "), looks_like_write_method("get_title")
        (True, False)
        >>> looks_like_write_method("address")
        True


    :param method_name: Name stringified, stripped, and lowercased before classification.
    :return: ``True`` for an exact write name or any recognized prefix, otherwise ``False``.
    """
    token = str(method_name).strip().lower()
    if token in WRITE_EXACT:
        return True
    return token.startswith(WRITE_PREFIXES)


__all__ = [
    "WRITE_EXACT",
    "WRITE_PREFIXES",
    "looks_like_write_method",
]
