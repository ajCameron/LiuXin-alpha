"""Small helpers shared by storage workflow models and implementations."""

from __future__ import annotations


def normalize_archive_path(value: str) -> str:
    """Return a safe relative POSIX-style path within a storage artifact.

    Example:
        >>> normalize_archive_path("/books//novel.epub")
        'books/novel.epub'
    """
    text = str(value).replace("\\", "/").lstrip("/")
    parts = [part for part in text.split("/") if part not in {"", "."}]
    if not parts:
        raise ValueError("backup archive path must not be empty.")
    if any(part == ".." for part in parts):
        raise ValueError("backup archive path must not contain '..'.")
    return "/".join(parts)


__all__ = ["normalize_archive_path"]
