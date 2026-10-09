"""Provide small dependency-free validators shared by storage value layers."""

from __future__ import annotations


def require_unique_metadata(metadata: tuple[tuple[str, str], ...]) -> None:
    """
    Reject duplicate native metadata keys while preserving the supplied sequence.

    Empty keys and declared string types are intentionally not validated here. Pair-unpacking and
    key-hashing errors propagate to expose malformed producer values.

    Example:
        >>> require_unique_metadata((("kind", "file"),))


    :param metadata: Ordered native key/value pairs whose keys must be unique.
    :return: None when keys are unique; duplicate keys raise ValueError.
    """

    keys = tuple(key for key, _value in metadata)
    if len(keys) != len(set(keys)):
        raise ValueError("driver metadata keys must be unique.")


__all__ = ["require_unique_metadata"]
