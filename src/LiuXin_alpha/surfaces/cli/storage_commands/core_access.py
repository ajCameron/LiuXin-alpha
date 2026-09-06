"""Storage CLI core access ownership."""

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
    with open_cli_core(args, enable_storage_manager=True) as core:
        result = core.query(operation, payload)
    emit_json(result, args)
    return 0


def _storage_command(
    args: argparse.Namespace, operation: str, payload: dict[str, Any]
) -> int:
    with open_cli_core(args, enable_storage_manager=True) as core:
        result = core.command(operation, payload)
    emit_json(result, args)
    return 0


def _store_reference(value: str) -> str | int:
    token = str(value).strip()
    try:
        return int(token)
    except ValueError:
        return token


def _bounded_file_bytes(path: str, max_mib: float) -> bytes:
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
    add_connection_arguments(parser)
    add_json_output(parser)
