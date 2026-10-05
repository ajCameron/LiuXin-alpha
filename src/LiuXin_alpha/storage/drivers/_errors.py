"""
Translate selected OS and SQLite failures into contextual storage exceptions.

Message helpers remove URL user information, replace URL queries, and redact a
fixed set of assignment-shaped secret labels. They are selective transformations,
not general secret or SQL filtering. Local/fallback detail is flattened and
length-limited; recognized URL paths and caller-supplied operation labels retain
their own content. Translators return exceptions for callers to raise and chain.
"""

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
    """
    Combine backend, operation, optional target, and a filtered reason into an error message.

    Backend and operation are stringified and stripped, without additional redaction or newline
    filtering. Target handling follows _safe_target; reason handling follows _safe_detail. A final
    period is always appended. Callers must supply suitable labels and should not treat the result
    as exhaustive secret filtering.

    Example:
        >>> driver_failure_message("HTTP", "read", target="https://user:secret@example.test/a")
        "HTTP read failed for 'https://example.test/a'."


    :param backend: Caller-supplied backend label, stripped at its edges.
    :param operation: Caller-supplied operation label, stripped at its edges.
    :param target: Optional path or URL rendered through the target filter and repr.
    :param reason: Optional backend detail, flattened and selectively redacted before inclusion.
    :return: Contextual message ending in a period, omitting target or detail when absent.
    """

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
    """
    Classify selected OS errors and return a storage exception with operation context.

    Match missing/existing objects, permissions, space/quota exhaustion, read-only filesystems, and
    timeouts in that order. Unmatched OS failures become StorageUnavailable. Detail comes from
    strerror or the exception class name; this function does not raise or attach an exception cause.

    Example:
        >>> type(translate_os_error(FileNotFoundError(), backend="filesystem", operation="read"))
        <class 'LiuXin_alpha.storage.api.errors.StorageNotFound'>


    :param error: OS exception classified by selected subclasses and errno values.
    :param backend: Backend label passed to the shared contextual message builder.
    :param operation: Operation label describing the failed filesystem or transport action.
    :param target: Optional path or endpoint rendered through the shared target filter.
    :return: New StorageError subclass instance for the caller to raise, usually chained from error.
    """

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
    """
    Classify SQLite error names and message fragments into storage failure categories.

    Match full, read-only, corrupt/not-a-database, denied, and busy/open/I/O failures in order;
    unknown cases use StorageError. A mapped human reason is preferred, but fallback exception text
    can survive selective detail filtering, including SQL or values not matched by the
    assignment-redaction pattern.

    Example:
        >>> error = sqlite3.OperationalError("database is locked")
        >>> type(translate_sqlite_error(error, operation="write", target="catalog.sqlite"))
        <class 'LiuXin_alpha.storage.api.errors.StorageUnavailable'>


    :param error: SQLite exception providing optional sqlite_errorname plus message text.
    :param operation: Failed database operation label, independent of its SQL statement.
    :param target: Database path or diagnostic URL supplied to the shared target filter.
    :return: New storage exception with SQLite context; raising and cause chaining remain with the caller.
    """

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
    """
    Map recognized SQLite name/text patterns to short explanations, otherwise retain fallback text.

    Inputs are expected to be uppercase error-name and lowercase message text. Mapping order is
    full, read-only, corruption, busy/locked, then authorization. This helper performs no redaction;
    the message builder filters its result later.

    Example:
        >>> _sqlite_reason("SQLITE_FULL", "database or disk is full")
        'the database or containing disk is full'


    :param error_name: Uppercase sqlite_errorname, or empty text when the attribute is absent.
    :param normalized: Lowercase exception message used for matching and as the first fallback.
    :return: Mapped explanation, normalized message, error name, or the generic SQLite backend error text.
    """

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
    """
    Remove selected URL credential components or filter fallback path/detail text.

    For a parsed scheme and authority, reconstruct using hostname and valid port, keep the path,
    replace a nonempty query with <redacted>, and drop the fragment. Invalid ports are omitted. URL
    paths are not assignment-redacted or truncated, and reconstructed targets are diagnostic text
    rather than round-trip URI contracts.

    Parse failures and values without both scheme and authority use _safe_detail with a
    300-character budget, falling back to <unknown> when it returns empty.

    Example:
        >>> _safe_target("https://user:pass@example.test/a?token=secret#part")
        'https://example.test/a?<redacted>'


    :param value: Path-like or URL target converted with os.fspath before inspection.
    :return: Diagnostic target with the stated filtering; it is not a general secret scrubber.
    """

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
    """
    Flatten text, redact selected assignment-shaped labels, and trim its length and final periods.

    Remove NULs and collapse whitespace. The regex recognizes specific secret labels followed by =
    or : and a nonempty token ending at whitespace, comma, or semicolon. It does not discover
    unlabeled secrets or all authorization formats.

    Overlong text is sliced to limit minus three and given an ellipsis, but the final rstrip removes
    those periods again. No truncation marker survives; sufficiently small limits can produce empty
    text. Limits are not independently validated.

    Example:
        >>> _safe_detail("token=secret" + chr(10) + "request failed.")
        'token=<redacted> request failed'
        >>> _safe_detail("abcdefgh", limit=6)
        'abc'


    :param value: Optional detail converted to str; None returns empty text immediately.
    :param limit: Character budget used before stripping final periods; defaults to 500.
    :return: Flattened, selectively redacted text with trailing periods removed, possibly empty.
    """

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
