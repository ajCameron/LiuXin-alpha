"""
Route generic storage resource discovery, reads, and writes through named Core operations.

CLI-host control files are bounded JSON objects, not locally checked resource
schemas. Only deletion requires --yes here. Shared helpers publish after Core
session exit and return zero without interpreting result flags; output failure
can follow an already-completed mutation.
"""

from __future__ import annotations

import argparse
from typing import Any

from LiuXin_alpha.surfaces.cli.common import load_json_object
from LiuXin_alpha.surfaces.cli.storage_commands.core_access import (
    _storage_command,
    _storage_query,
)


def cmd_storage_resources_describe(args: argparse.Namespace) -> int:
    """
    Publish Core's resource descriptions using an empty request payload.

    Example:
        >>> cmd_storage_resources_describe(parsed_resources_describe_args)  # doctest: +SKIP


    :param args: Core connection selectors and JSON publication settings.
    :return: Zero after publishing storage.resources.describe without local schema validation.
    """
    return _storage_query(args, "storage.resources.describe", {})


def cmd_storage_resource_list(args: argparse.Namespace) -> int:
    """
    List a resource page with an optional JSON-object filter loaded before Core opens.

    Example:
        >>> cmd_storage_resource_list(parsed_resource_list_args)  # doctest: +SKIP


    :param args: resource name, limit/offset, optional where_file, and Core/output controls.
    :return: Zero after publishing storage.resource.list; Core owns filter and paging validation.
    """
    payload: dict[str, Any] = {
        "resource": args.resource,
        "limit": int(args.limit),
        "offset": int(args.offset),
    }
    if args.where_file:
        payload["where"] = load_json_object(args.where_file)
    return _storage_query(args, "storage.resource.list", payload)


def cmd_storage_resource_get(args: argparse.Namespace) -> int:
    """
    Fetch a named resource's record using an integer-converted resource ID.

    Example:
        >>> cmd_storage_resource_get(parsed_resource_get_args)  # doctest: +SKIP


    :param args: resource/resource_id and shared connection/JSON output options.
    :return: Zero after storage.resource.get publication, without requiring a found record.
    """
    return _storage_query(
        args,
        "storage.resource.get",
        {"resource": args.resource, "id": int(args.resource_id)},
    )


def cmd_storage_resource_write(args: argparse.Namespace) -> int:
    """
    Load a values object, then create or update the selected storage resource.

    Exact resource_action='update' adds an integer ID and selects update; every
    other direct action value selects create. Values load before Core composition,
    and neither path requires a confirmation flag in this handler.

    Example:
        >>> cmd_storage_resource_write(parsed_resource_write_args)  # doctest: +SKIP


    :param args: resource, values_file, resource_action, update resource_id,
        and shared connection/output options.
    :return: Zero after publishing the create/update receipt without checking result flags.
    """
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
    """
    Require confirmation before deleting a storage resource record by integer ID.

    Example:
        >>> cmd_storage_resource_delete(parsed_resource_delete_args)  # doctest: +SKIP


    :param args: resource/resource_id, yes flag, and connection/JSON publication settings.
    :return: Zero after publishing storage.resource.delete; later output failure does not undo it.
    :raises ValueError: The yes confirmation flag is falsey.
    """
    if not args.yes:
        raise ValueError("Storage resource deletion requires --yes.")
    return _storage_command(
        args,
        "storage.resource.delete",
        {"resource": args.resource, "id": int(args.resource_id)},
    )
