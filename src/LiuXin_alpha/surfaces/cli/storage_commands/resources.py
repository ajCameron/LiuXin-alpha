"""Storage CLI resources ownership."""

from __future__ import annotations

import argparse
from typing import Any

from LiuXin_alpha.surfaces.cli.common import load_json_object
from LiuXin_alpha.surfaces.cli.storage_commands.core_access import (
    _storage_command,
    _storage_query,
)


def cmd_storage_resources_describe(args: argparse.Namespace) -> int:
    return _storage_query(args, "storage.resources.describe", {})


def cmd_storage_resource_list(args: argparse.Namespace) -> int:
    payload: dict[str, Any] = {
        "resource": args.resource,
        "limit": int(args.limit),
        "offset": int(args.offset),
    }
    if args.where_file:
        payload["where"] = load_json_object(args.where_file)
    return _storage_query(args, "storage.resource.list", payload)


def cmd_storage_resource_get(args: argparse.Namespace) -> int:
    return _storage_query(
        args,
        "storage.resource.get",
        {"resource": args.resource, "id": int(args.resource_id)},
    )


def cmd_storage_resource_write(args: argparse.Namespace) -> int:
    payload: dict[str, Any] = {
        "resource": args.resource,
        "values": load_json_object(args.values_file),
    }
    operation = "storage.resource.create"
    if args.resource_action == "update":
        payload["id"] = int(args.resource_id)
        operation = "storage.resource.update"
    return _storage_command(args, operation, payload)


def cmd_storage_resource_delete(args: argparse.Namespace) -> int:
    if not args.yes:
        raise ValueError("Storage resource deletion requires --yes.")
    return _storage_command(
        args,
        "storage.resource.delete",
        {"resource": args.resource, "id": int(args.resource_id)},
    )
