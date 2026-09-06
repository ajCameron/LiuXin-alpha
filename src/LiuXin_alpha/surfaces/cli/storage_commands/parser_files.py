"""Storage CLI parser files ownership."""

from __future__ import annotations

import argparse

from LiuXin_alpha.surfaces.cli.common import add_connection_arguments
from LiuXin_alpha.surfaces.cli.storage_commands.administration import (
    cmd_storage_file_copy,
    cmd_storage_file_delete,
    cmd_storage_file_get,
    cmd_storage_file_locate,
    cmd_storage_file_put,
    cmd_storage_files_list,
    cmd_storage_location_stat,
    cmd_storage_source_add,
    cmd_storage_source_register,
    cmd_storage_sources_list,
)
from LiuXin_alpha.surfaces.cli.storage_commands.core_access import _core_json


def _add_files_parser(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    files = commands.add_parser(
        "files", help="List, transfer, locate, copy, or delete stored files."
    )
    file_commands = files.add_subparsers(dest="files_action", required=True)
    files_list = file_commands.add_parser("list")
    _core_json(files_list)
    files_list.add_argument("--limit", type=int, default=100)
    files_list.add_argument("--offset", type=int, default=0)
    files_list.set_defaults(handler=cmd_storage_files_list)
    locate = file_commands.add_parser("locate")
    _core_json(locate)
    locate.add_argument("asset_id", type=int)
    locate.add_argument("--store", help="Preferred Store UUID or unique name.")
    locate.set_defaults(handler=cmd_storage_file_locate)
    get = file_commands.add_parser(
        "get", aliases=["read"], help="Read an asset to a CLI-host file."
    )
    add_connection_arguments(get)
    get.add_argument("asset_id", type=int)
    get.add_argument("file_output", help="CLI-host output path, or - for stdout.")
    get.add_argument("--store", help="Preferred Store UUID or unique name.")
    get.add_argument("--replace-file-output", action="store_true")
    get.set_defaults(handler=cmd_storage_file_get)
    put = file_commands.add_parser(
        "put", help="Transfer a CLI-host file into managed storage."
    )
    _core_json(put)
    put.add_argument("input", help="Path on the CLI host.")
    put.add_argument("--store", help="Target Store UUID or unique name.")
    put.add_argument("--metadata-file", help="Optional rich storage-hint JSON object.")
    put.add_argument("--name")
    put.add_argument("--original-name")
    put.add_argument("--media-type")
    put.add_argument("--max-transfer-mib", type=float, default=512.0)
    put.set_defaults(handler=cmd_storage_file_put)
    copy = file_commands.add_parser(
        "copy", help="Create another managed replica of an asset."
    )
    _core_json(copy)
    copy.add_argument("asset_id", type=int)
    copy.add_argument("--store", help="Target Store UUID or unique name.")
    copy.add_argument("--metadata-file", help="Optional rich storage-hint JSON object.")
    copy.set_defaults(handler=cmd_storage_file_copy)
    file_delete = file_commands.add_parser(
        "delete", help="Commit deletion of one replica."
    )
    _core_json(file_delete)
    file_delete.add_argument("replica_id", type=int)
    file_delete.add_argument("--yes", action="store_true")
    file_delete.set_defaults(handler=cmd_storage_file_delete)


def _add_location_parser(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    location = commands.add_parser("location", help="Inspect an exact Store key.")
    location_commands = location.add_subparsers(dest="location_action", required=True)
    location_stat = location_commands.add_parser("stat")
    _core_json(location_stat)
    location_stat.add_argument("store_uuid")
    location_stat.add_argument("key")
    location_stat.set_defaults(handler=cmd_storage_location_stat)


def _add_sources_parser(
    commands: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    sources = commands.add_parser("sources", help="Inspect or register ingest sources.")
    source_commands = sources.add_subparsers(dest="sources_action", required=True)
    source_list = source_commands.add_parser("list", aliases=["supported"])
    _core_json(source_list)
    source_list.set_defaults(handler=cmd_storage_sources_list)
    register = source_commands.add_parser("register")
    _core_json(register)
    register.add_argument("kind")
    register.add_argument("options_file", help="CLI-host source-options JSON object.")
    register.set_defaults(handler=cmd_storage_source_register)

    source_add = source_commands.add_parser(
        "add", help="Register a common source without constructing a JSON object."
    )
    _core_json(source_add)
    source_add.add_argument(
        "kind",
        choices=(
            "unmanaged-disk",
            "rclone-http",
            "wget-html",
            "native-html",
            "squashfs-open",
        ),
    )
    source_add.add_argument(
        "location", help="Disk/archive path or remote URL as seen by Core."
    )
    source_add.add_argument("--name")
    source_add.add_argument("--source-label")
    source_add.add_argument("--extension", action="append")
    source_add.add_argument("--no-hash", action="store_true")
    source_add.add_argument("--follow-symlinks", action="store_true")
    source_add.add_argument("--no-store-links", action="store_true")
    source_add.add_argument("--no-refresh", action="store_true")
    source_add.add_argument("--timeout", type=float)
    source_add.add_argument("--requests-per-hour", type=float)
    source_add.add_argument(
        "--options-file",
        help="Optional overrides for uncommon source-specific options.",
    )
    source_add.set_defaults(handler=cmd_storage_source_add)
