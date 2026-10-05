"""
Aggregate Core capability discovery across eight optional operational families.

One storage-enabled Core session supplies program, storage, metadata, conversion,
and ingest receipts. Query exceptions become individual error records so later
families still run; receipt contents are not themselves validated as successful.
Unlike support diagnostics, this output is not credential-redacted here.
"""

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
    """
    Query every configured capability probe and publish successes plus errors.

    Per-query Exception failures record section, operation, exception type, and
    the unredacted exception text, then continue. A returned ``ok=False`` is
    still a successful call. Probe payload dictionaries and successful receipts
    are passed/stored without copying. Session entry/exit and output failures
    propagate; the report is emitted only after the session exits.

    Example:
        >>> cmd_plugins_inspect(parsed_plugins_inspect_args)  # doctest: +SKIP


    :param args: Parsed connection selectors and JSON output controls.
    :return: Zero if no probe raised an Exception, otherwise one, after output.
    """
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
    Register plugins/capabilities inspect/list aliases without running probes.

    Each accepted spelling selects the same handler and connection/JSON flags.

    Example:
        >>> root = argparse.ArgumentParser()
        >>> build_plugins_parser(root.add_subparsers())
        >>> args = root.parse_args(["capabilities", "list", "--database", "db.sqlite"])
        >>> args.handler is cmd_plugins_inspect
        True


    :param subparsers: Root CLI subparser collection receiving the plugin family.
    :return: None; the family, aliases, and handler defaults are added in place.
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
