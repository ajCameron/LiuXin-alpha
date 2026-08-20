"""Internal helpers for safe, actionable storage-driver failures."""

from __future__ import annotations

import errno
import os
import re
import sqlite3

from urllib.parse import urlsplit, urlunsplit

from LiuXin_alpha.storage.api import (
    StorageAlreadyExists,
    StorageError,
    StorageIntegrityError,
    StorageNoSpace,
    StorageNotFound,
    StoragePermissionDenied,
    StorageReadOnly,
    StorageTimeout,
    StorageUnavailable,
)


_SECRET_ASSIGNMENT = re.compile(
    r"(?i)\b(password|passwd|token|secret|authorization|credential|"
    + r"access[_-]?key|api[_-]?key)\s*([=:])\s*([^\s,;]+)"
)


def driver_failure_message(
    backend: str,
    operation: str,
    *,
    target: str | os.PathLike[str] | None = None,
    reason: str | None = None,
) -> str:
    """Build one-line context without reproducing credentials or huge stderr."""

    message = f"{str(backend).strip()} {str(operation).strip()} failed"
    if target is not None:
        message += f" for {_safe_target(target)!r}"
    detail = _safe_detail(reason)
    if detail:
        message += f": {detail}"
    return message + "."


def translate_os_error(
    error: OSError,
    *,
    backend: str,
    operation: str,
    target: str | os.PathLike[str] | None = None,
) -> StorageError:
    """Translate common OS failures while retaining operation-level context."""

    error_number = getattr(error, "errno", None)
    reason = getattr(error, "strerror", None) or type(error).__name__
    message = driver_failure_message(
        backend,
        operation,
        target=target,
        reason=reason,
    )
    if isinstance(error, FileNotFoundError) or error_number == errno.ENOENT:
        return StorageNotFound(message)
    if isinstance(error, FileExistsError) or error_number == errno.EEXIST:
        return StorageAlreadyExists(message)
    if isinstance(error, PermissionError) or error_number in {errno.EACCES, errno.EPERM}:
        return StoragePermissionDenied(message)
    if error_number in {errno.ENOSPC, getattr(errno, "EDQUOT", -1)}:
        return StorageNoSpace(message)
    if error_number == errno.EROFS:
        return StorageReadOnly(message)
    if isinstance(error, TimeoutError) or error_number == errno.ETIMEDOUT:
        return StorageTimeout(message)
    return StorageUnavailable(message)


def translate_sqlite_error(
    error: sqlite3.Error,
    *,
    operation: str,
    target: str | os.PathLike[str],
) -> StorageError:
    """Classify useful SQLite failures without exposing SQL or parameters."""

    error_name = str(getattr(error, "sqlite_errorname", "") or "").upper()
    normalized = str(error).lower()
    reason = _sqlite_reason(error_name, normalized)
    message = driver_failure_message(
        "SQLite",
        operation,
        target=target,
        reason=reason,
    )
    if "FULL" in error_name or "database or disk is full" in normalized:
        return StorageNoSpace(message)
    if "READONLY" in error_name or "readonly database" in normalized:
        return StorageReadOnly(message)
    if any(marker in error_name for marker in ("CORRUPT", "NOTADB")) or any(
        marker in normalized
        for marker in ("malformed", "not a database", "database disk image is malformed")
    ):
        return StorageIntegrityError(message)
    if any(marker in error_name for marker in ("AUTH", "PERM")) or "not authorized" in normalized:
        return StoragePermissionDenied(message)
    if any(marker in error_name for marker in ("BUSY", "LOCKED", "CANTOPEN", "IOERR")) or any(
        marker in normalized
        for marker in ("locked", "unable to open database file", "disk i/o error")
    ):
        return StorageUnavailable(message)
    return StorageError(message)


def _sqlite_reason(error_name: str, normalized: str) -> str:
    if "FULL" in error_name or "database or disk is full" in normalized:
        return "the database or containing disk is full"
    if "READONLY" in error_name or "readonly database" in normalized:
        return "the database is read-only"
    if any(marker in error_name for marker in ("CORRUPT", "NOTADB")) or any(
        marker in normalized for marker in ("malformed", "not a database")
    ):
        return "the database is corrupt or is not a SQLite database"
    if "BUSY" in error_name or "LOCKED" in error_name or "locked" in normalized:
        return "the database is busy or locked"
    if any(marker in error_name for marker in ("AUTH", "PERM")):
        return "SQLite denied the operation"
    return str(normalized or error_name or "SQLite backend error")


def _safe_target(value: str | os.PathLike[str]) -> str:
    text = os.fspath(value)
    try:
        parsed = urlsplit(text)
    except ValueError:
        return _safe_detail(text, limit=300) or "<unknown>"
    if not parsed.scheme or not parsed.netloc:
        return _safe_detail(text, limit=300) or "<unknown>"
    hostname = parsed.hostname or "<unknown-host>"
    try:
        port = parsed.port
    except ValueError:
        port = None
    authority = hostname if port is None else f"{hostname}:{port}"
    query = "<redacted>" if parsed.query else ""
    return urlunsplit((parsed.scheme, authority, parsed.path, query, ""))


def _safe_detail(value: str | None, *, limit: int = 500) -> str:
    if value is None:
        return ""
    text = " ".join(str(value).replace("\x00", "").split())
    text = _SECRET_ASSIGNMENT.sub(r"\1\2<redacted>", text)
    if len(text) > limit:
        text = text[: max(0, limit - 3)].rstrip() + "..."
    return text.rstrip(".")


__all__ = [
    "driver_failure_message",
    "translate_os_error",
    "translate_sqlite_error",
]
