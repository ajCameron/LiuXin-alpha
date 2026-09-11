"""
Normalize lexical member paths for storage workflow artifacts.

This helper strips leading separators and rejects parent components. It is a
portable spelling normalization, not a full backend filename policy or proof of
filesystem containment during extraction.

Example:
    >>> normalize_archive_path("/books//novel.epub")
    'books/novel.epub'
"""

from __future__ import annotations


def normalize_archive_path(value: str) -> str:
    """
    Normalize separators and dot components while rejecting empty or parent-traversing member paths.

    Stringify the value, replace backslashes, and strip leading slashes. Do not trim whitespace or
    reject drive prefixes, NULs, reserved names, or overlong components here. Filesystem containment
    and backend-specific validation belong to the caller.

    Example:
        >>> normalize_archive_path("/books//novel.epub")
        'books/novel.epub'


    :param value: Path-like text interpreted lexically as artifact member components.
    :return: Nonempty slash-joined components; empty results or any .. component raise ValueError.
    """
    text = str(value).replace("\\", "/").lstrip("/")
    parts = [part for part in text.split("/") if part not in {"", "."}]
    if not parts:
        raise ValueError("backup archive path must not be empty.")
    if any(part == ".." for part in parts):
        raise ValueError("backup archive path must not contain '..'.")
    return "/".join(parts)


__all__ = ["normalize_archive_path"]
