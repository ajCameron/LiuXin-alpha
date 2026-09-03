"""Consolidated plugin and optional-capability inspection."""

from __future__ import annotations

import argparse

from typing import Any

from LiuXin_alpha.surfaces.cli.common import (
    add_connection_arguments,
    add_json_output,
    emit_json,
    open_cli_core,
)


_PROBES = (
    ("program", "capabilities.list", {}),
    ("storage_backends", "storage.backends.list", {}),
    ("storage_sources", "storage.sources.supported", {}),
    ("storage_resources", "storage.resources.describe", {}),
    ("metadata_files", "metadata.file.formats", {}),
    ("metadata_online", "metadata.online.sources", {}),
    ("conversion", "conversion.formats", {}),
    ("ingest", "ingest.formats", {}),
)


def cmd_plugins_inspect(args: argparse.Namespace) -> int:
    sections: dict[str, Any] = {}
    errors: list[dict[str, str]] = []
    with open_cli_core(args, enable_storage_manager=True) as core:
        for section, operation, payload in _PROBES:
            try:
                sections[section] = core.query(operation, payload)
            except Exception as error:
                errors.append(
                    {
                        "section": section,
                        "operation": operation,
                        "error": str(error),
                        "error_type": type(error).__name__,
                    }
                )
    result = {"ok": not errors, "sections": sections, "errors": errors}
    emit_json(result, args)
    return 0 if not errors else 1


def build_plugins_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """
    Build the `plugins` command-line parser.


    :param subparsers:
    :return:
    """
    parser = subparsers.add_parser(
        "plugins",
        aliases=["capabilities"],
        help="Inspect installed adapters and optional operational capabilities.",
    )
    commands = parser.add_subparsers(dest="plugins_command", required=True)
    inspect = commands.add_parser(
        "inspect", aliases=["list"], help="Probe all public capability families."
    )
    add_connection_arguments(inspect)
    add_json_output(inspect)
    inspect.set_defaults(handler=cmd_plugins_inspect)


__all__ = ["build_plugins_parser"]
