"""Top-level packaged LiuXin command-line application."""

from __future__ import annotations

import argparse
import sys

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
from LiuXin_alpha.surfaces.cli.completion import build_completion_parser
from LiuXin_alpha.surfaces.cli.core_cli import build_core_parser
from LiuXin_alpha.surfaces.cli.diagnostics import build_diagnostics_parsers
from LiuXin_alpha.surfaces.cli.initialize import build_init_parser
from LiuXin_alpha.surfaces.cli.jobs import build_jobs_parser
from LiuXin_alpha.surfaces.cli.metadata import build_metadata_parser
from LiuXin_alpha.surfaces.cli.postgres import build_postgres_parser
from LiuXin_alpha.surfaces.cli.serve import build_serve_parser
from LiuXin_alpha.surfaces.cli.squashfs import build_squashfs_parser
from LiuXin_alpha.surfaces.cli.storage import build_storage_parser
from LiuXin_alpha.surfaces.cli.workflows import (
    build_backup_parser,
    build_conversion_parser,
    build_database_parser,
    build_ingest_parser,
    build_maintenance_parser,
)


def build_parser() -> argparse.ArgumentParser:
    """
    Build the top-level ``liuxin`` command-line parser.


    :return:
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
    build_completion_parser(subparsers)
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


def _normalise_shortcuts(argv: list[str]) -> list[str]:
    """Expand concise operator forms before argparse sees subcommands."""

    if len(argv) < 2 or argv[0] != "ingest":
        return argv
    if argv[1] in {"disk", "formats", "remote-html", "runs", "-h", "--help"}:
        return argv
    if "--source" in argv:
        index = argv.index("--source")
        if index + 1 >= len(argv):
            return argv
        source = argv[index + 1]
        remainder = [*argv[1:index], *argv[index + 2 :]]
        return [
            "storage",
            "ingest",
            "--source-root",
            source,
            *remainder,
        ]
    if not argv[1].startswith("-"):
        return [
            "storage",
            "ingest",
            "--source-root",
            argv[1],
            *argv[2:],
        ]
    return argv


def _hoist_global_selectors(argv: list[str]) -> list[str]:
    """Allow global profile selectors before or after a subcommand."""

    selected: list[str] = []
    remainder: list[str] = []
    index = 0
    while index < len(argv):
        value = argv[index]
        matched = next(
            (
                option
                for option in ("--system-root", "--profile")
                if value == option or value.startswith(option + "=")
            ),
            None,
        )
        if matched is None:
            remainder.append(value)
            index += 1
            continue
        if value == matched:
            if index + 1 >= len(argv):
                return argv
            selected.extend((matched, argv[index + 1]))
            index += 2
        else:
            selected.append(value)
            index += 1
    return [*selected, *_normalise_shortcuts(remainder)]


def main(argv: list[str] | None = None) -> int:
    """
    Run the cli command-line entry point.


    :param argv:
    :return:
    """
    parser = build_parser()
    selected = sys.argv[1:] if argv is None else argv
    args = parser.parse_args(_hoist_global_selectors(list(selected)))
    global_root = getattr(args, "global_system_root", None)
    global_profile = getattr(args, "global_profile", None)
    if global_root and global_profile:
        parser.error("--system-root and --profile are mutually exclusive")
    if global_root:
        if getattr(args, "system_root", None):
            parser.error("--system-root was provided more than once")
        args.system_root = global_root
    if global_profile:
        if getattr(args, "profile", None):
            parser.error("--profile was provided more than once")
        args.profile = global_profile
    handler = getattr(args, "handler", None)
    if handler is None:
        parser.print_help()
        return 2
    try:
        return int(handler(args))
    except Exception as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 2


__all__ = ["build_parser", "main"]
