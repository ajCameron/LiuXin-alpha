"""
Construct the complete CLI grammar independently of application execution.

Completion needs to inspect this grammar, so its registrar is supplied by the
caller instead of importing the completion command back into this owner.
"""

from __future__ import annotations

import argparse

from LiuXin_alpha.constants import __version__
from LiuXin_alpha.surfaces.cli.capabilities import build_plugins_parser
from LiuXin_alpha.surfaces.cli.catalogue import (
    build_acquisition_parser,
    build_catalog_parser,
)
from LiuXin_alpha.surfaces.cli.config_cli import (
    build_config_parser,
    build_connection_parsers,
)
from LiuXin_alpha.surfaces.cli.core_cli import build_core_parser
from LiuXin_alpha.surfaces.cli.diagnostics import build_diagnostics_parsers
from LiuXin_alpha.surfaces.cli.initialize import build_init_parser
from LiuXin_alpha.surfaces.cli.jobs import build_jobs_parser
from LiuXin_alpha.surfaces.cli.metadata import build_metadata_parser
from LiuXin_alpha.surfaces.cli.parser_types import CompletionRegistrar
from LiuXin_alpha.surfaces.cli.postgres import build_postgres_parser
from LiuXin_alpha.surfaces.cli.serve import build_serve_parser
from LiuXin_alpha.surfaces.cli.squashfs_parsers import build_squashfs_parser
from LiuXin_alpha.surfaces.cli.storage import build_storage_parser
from LiuXin_alpha.surfaces.cli.workflows import (
    build_backup_parser,
    build_conversion_parser,
    build_database_parser,
    build_ingest_parser,
    build_maintenance_parser,
)


def create_parser(
    *, register_completion: CompletionRegistrar
) -> argparse.ArgumentParser:
    """
    Register required command families in stable help order on a fresh parser.

    Invoke the supplied completion registrar exactly once between diagnostics
    and Core registration. Global selectors use separate namespace destinations;
    position normalization and mutual-exclusion policy belong to the application.
    Imported builders may fail during construction, but no handler is run here.

    Example:
        >>> from LiuXin_alpha.surfaces.cli.completion import build_completion_parser
        >>> parser = create_parser(register_completion=build_completion_parser)
        >>> parser.parse_args(['completion', 'zsh']).shell
        'zsh'


    :param register_completion: Callback adding completion to the root subparser collection.
    :return: Fresh liuxin parser with version, global selectors, and required surface choice.
    """
    parser = argparse.ArgumentParser(
        prog="liuxin",
        description="LiuXin operational command-line surfaces",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"LiuXin {__version__}",
    )
    parser.add_argument(
        "--system-root",
        dest="global_system_root",
        help="Use SYSTEM_ROOT/liuxin-system.json for every supported command.",
    )
    parser.add_argument(
        "--profile",
        dest="global_profile",
        help="Use a named or path-based LiuXin deployment profile.",
    )
    subparsers = parser.add_subparsers(dest="surface", required=True)
    build_init_parser(subparsers)
    build_connection_parsers(subparsers)
    build_config_parser(subparsers)
    build_diagnostics_parsers(subparsers)
    register_completion(subparsers)
    build_core_parser(subparsers)
    build_jobs_parser(subparsers)
    build_catalog_parser(subparsers)
    build_acquisition_parser(subparsers)
    build_metadata_parser(subparsers)
    build_storage_parser(subparsers)
    build_ingest_parser(subparsers)
    build_conversion_parser(subparsers)
    build_backup_parser(subparsers)
    build_database_parser(subparsers)
    build_maintenance_parser(subparsers)
    build_serve_parser(subparsers)
    build_squashfs_parser(subparsers)
    build_postgres_parser(subparsers)
    build_plugins_parser(subparsers)
    return parser


__all__ = ["create_parser"]
