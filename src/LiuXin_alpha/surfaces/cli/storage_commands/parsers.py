"""Storage CLI parsers ownership."""

from __future__ import annotations

import argparse

from LiuXin_alpha.surfaces.cli.storage_commands.ingest import cmd_storage_ingest
from LiuXin_alpha.surfaces.cli.storage_commands.ingest_options import (
    add_storage_ingest_arguments,
)
from LiuXin_alpha.surfaces.cli.storage_commands.parser_files import (
    _add_files_parser,
    _add_location_parser,
    _add_sources_parser,
)
from LiuXin_alpha.surfaces.cli.storage_commands.parser_integrity import (
    _add_asset_parser,
    _add_audit_parser,
    _add_policies_parser,
    _add_reconcile_parser,
    _add_recovery_parser,
    _add_repair_parser,
    _add_replica_parser,
    _add_resources_parser,
    _add_status_parser,
)
from LiuXin_alpha.surfaces.cli.storage_commands.parser_stores import (
    _add_add_store_parser,
    _add_backends_parser,
    _add_default_parser,
    _add_refresh_parser,
    _add_store_parser,
    _add_stores_parser,
)


def _build_storage_admin_parsers(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """Install command families in their stable help/discovery order."""
    _add_backends_parser(commands)
    _add_add_store_parser(commands)
    _add_stores_parser(commands)
    _add_store_parser(commands)
    _add_default_parser(commands)
    _add_refresh_parser(commands)
    _add_files_parser(commands)
    _add_location_parser(commands)
    _add_sources_parser(commands)
    _add_asset_parser(commands)
    _add_replica_parser(commands)
    _add_status_parser(commands)
    _add_audit_parser(commands)
    _add_reconcile_parser(commands)
    _add_repair_parser(commands)
    _add_recovery_parser(commands)
    _add_policies_parser(commands)
    _add_resources_parser(commands)


def build_storage_parser(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    """Register the top-level ``storage`` command family."""

    parser = subparsers.add_parser(
        "storage",
        help="Ingest files and administer Stores, Replicas, sources, and policies.",
    )
    storage_subparsers = parser.add_subparsers(
        dest="storage_command",
        required=True,
    )
    ingest = storage_subparsers.add_parser(
        "ingest",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        help="Catalogue a mixed local tree with bounded recursive containers.",
        description=(
            "Catalogue loose files and nested SquashFS, ISO/UDF, ZIP, TAR, RAR, "
            "and 7z containers without extracting into the source tree.\n\n"
            "Exit codes: 0 complete/ready; 1 completed with issues or failed; "
            "2 command configuration; 130 SIGINT; 143 SIGTERM."
        ),
    )
    add_storage_ingest_arguments(ingest)
    ingest.set_defaults(handler=cmd_storage_ingest)
    _build_storage_admin_parsers(storage_subparsers)
