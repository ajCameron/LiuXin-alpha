"""
Route storage requests through Core and provide shared argument/input helpers.

Queries and commands request storage-enabled composition, leave the session,
then publish JSON. These helpers neither preflight report destinations before
execution nor interpret result health flags. Publication failure does not undo
completed Core operations; lower layers own validation and authorization.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from LiuXin_alpha.surfaces.cli.common import (
    add_connection_arguments,
    add_json_output,
    emit_json,
    open_cli_core,
)


def _storage_query(
    args: argparse.Namespace, operation: str, payload: dict[str, Any]
) -> int:
    """
    Query storage-enabled Core, exit its session, and publish the receipt as JSON.

    No receipt shape or ok/healthy flag is checked here. Core/session/output
    failures propagate; construction-only stdout redirection follows open_cli_core.

    Example:
        >>> _storage_query(args, "storage.default.get", {})  # doctest: +SKIP


    :param args: Shared Core selectors and JSON publication options.
    :param operation: Exact named query passed to the client.
    :param payload: Request dictionary forwarded without copying or local validation.
    :return: Zero after publication, independently of receipt health flags.
    """
    with open_cli_core(args, enable_storage_manager=True) as core:
        result = core.query(operation, payload)
    emit_json(result, args)
    return 0


def _storage_command(
    args: argparse.Namespace, operation: str, payload: dict[str, Any]
) -> int:
    """
    Execute a storage command and publish its receipt after session cleanup.

    No report-destination preflight or cross-operation rollback is added. A Core
    mutation may survive a later cleanup, serialization, or publication failure.

    Example:
        >>> _storage_command(args, "storage.refresh", {"strict": False})  # doctest: +SKIP


    :param args: Shared Core selectors and JSON publication options.
    :param operation: Exact named mutation passed to the client.
    :param payload: Request dictionary forwarded unchanged; Core owns its contract.
    :return: Zero after publication without interpreting receipt ok/healthy flags.
    """
    with open_cli_core(args, enable_storage_manager=True) as core:
        result = core.command(operation, payload)
    emit_json(result, args)
    return 0


def _store_reference(value: str) -> str | int:
    """
    Interpret a stripped integer-like Store selector as an ID, otherwise as text.

    Negative IDs, leading signs/zeroes, and an empty text selector are retained
    according to int conversion; existence and valid ID ranges are not checked.

    Example:
        >>> [_store_reference(value) for value in (" 007 ", " books ", " ")]
        [7, 'books', '']


    :param value: Store selector stringified and stripped before integer conversion.
    :return: Parsed integer or the stripped token when conversion raises ValueError.
    """
    token = str(value).strip()
    try:
        return int(token)
    except ValueError:
        return token


def _bounded_file_bytes(path: str, max_mib: float) -> bytes:
    """
    Read at most a binary-MiB limit plus one byte and reject oversized input.

    Expand '~' but do not resolve the path, reject symlinks, require a regular
    file, or compare source state before/after reading. Opening a special file
    can block. Fractional positive limits truncate to bytes, possibly zero;
    nonfinite values are left to integer conversion errors.

    Example:
        >>> content = _bounded_file_bytes("book.epub", 16.0)  # doctest: +SKIP


    :param path: CLI-host input path opened in binary mode.
    :param max_mib: Positive transfer ceiling in units of 1,048,576 bytes.
    :return: Entire content when its observed length does not exceed the byte ceiling.
    :raises ValueError: The limit is nonpositive/invalid or the read exceeds it.
    :raises OverflowError: Converting an infinite limit to an integer fails.
    """
    if max_mib <= 0:
        raise ValueError("--max-transfer-mib must be greater than zero.")
    source = Path(path).expanduser()
    limit = int(max_mib * 1024 * 1024)
    with source.open("rb") as stream:
        content = stream.read(limit + 1)
    if len(content) > limit:
        raise ValueError(
            f"Input exceeds the configured {limit} byte transfer limit: {source!s}"
        )
    return content


def _core_json(parser: argparse.ArgumentParser) -> None:
    """
    Add shared Core selection and JSON publication options to a storage leaf parser.

    Example:
        >>> parser = argparse.ArgumentParser()
        >>> _core_json(parser)
        >>> args = parser.parse_args([])
        >>> args.db_type, args.output, args.replace_output
        ('SQLite', '-', False)


    :param parser: Mutable leaf parser receiving connection and output declarations.
    :return: None; register options without opening Core or validating a destination.
    """
    add_connection_arguments(parser)
    add_json_output(parser)
