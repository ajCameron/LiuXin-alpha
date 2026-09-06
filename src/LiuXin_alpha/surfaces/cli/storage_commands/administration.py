"""Storage CLI administration ownership."""

from __future__ import annotations

import argparse
import base64
import json
import sys
from pathlib import Path
from typing import Any

from LiuXin_alpha.surfaces.cli.common import (
    decode_wire_bytes,
    emit_bytes,
    emit_json,
    load_json_object,
    open_cli_core,
)
from LiuXin_alpha.surfaces.cli.storage_commands.core_access import (
    _bounded_file_bytes,
    _storage_command,
    _storage_query,
    _store_reference,
)


def cmd_storage_stores_list(args: argparse.Namespace) -> int:
    return _storage_query(args, "storage.stores.list", {"refresh": args.refresh})


def cmd_storage_store_show(args: argparse.Namespace) -> int:
    return _storage_query(
        args, "storage.store.get", {"store": _store_reference(args.store)}
    )


def cmd_storage_store_save(args: argparse.Namespace) -> int:
    return _storage_command(
        args, "storage.store.save", {"store": load_json_object(args.store_file)}
    )


def cmd_storage_backends_list(args: argparse.Namespace) -> int:
    return _storage_query(
        args,
        "storage.backends.list",
        {"include_internal": bool(args.include_internal)},
    )


def cmd_storage_store_update(args: argparse.Namespace) -> int:
    changes: dict[str, Any] = {}
    for argument, field in (
        (args.name, "name"),
        (args.root, "root"),
        (args.url, "url"),
        (args.protocol, "protocol"),
        (args.role, "operational_role"),
        (args.replication_policy_id, "replication_policy_id"),
        (args.backup_policy_id, "backup_policy_id"),
    ):
        if argument is not None:
            changes[field] = argument
    if args.failure_domain is not None or args.clear_failure_domain:
        changes["failure_domain"] = (
            None if args.clear_failure_domain else args.failure_domain
        )
    if args.region is not None or args.clear_region:
        changes["region"] = None if args.clear_region else args.region
    if args.read_only is not None:
        changes["read_only"] = bool(args.read_only)
    if args.add_tag:
        changes["add_tags"] = list(args.add_tag)
    if args.remove_tag:
        changes["remove_tags"] = list(args.remove_tag)
    return _storage_command(
        args,
        "storage.store.update",
        {"store": _store_reference(args.store), "changes": changes},
    )


def cmd_storage_store_probe(args: argparse.Namespace) -> int:
    return _storage_command(
        args, "storage.store.probe", {"store": _store_reference(args.store)}
    )


def cmd_storage_store_delete(args: argparse.Namespace) -> int:
    if not args.yes:
        raise ValueError("Store removal requires --yes.")
    return _storage_command(
        args,
        "storage.store.delete",
        {
            "store": _store_reference(args.store),
            "delete_from_database": bool(args.delete_from_database),
        },
    )


def cmd_storage_store_evacuate(args: argparse.Namespace) -> int:
    payload: dict[str, Any] = {
        "store": _store_reference(args.store),
        "max_assets": int(args.max_assets),
    }
    if args.destination_store:
        payload["destination_store"] = _store_reference(args.destination_store)
    if not args.yes:
        return _storage_query(args, "storage.store.evacuate.plan", payload)
    payload.update(
        {
            "max_actions": int(args.max_actions),
            "max_transfer_bytes": int(
                float(args.max_transfer_gib) * 1024 * 1024 * 1024
            ),
            "keep_source_bytes": bool(args.keep_source_bytes),
        }
    )
    with open_cli_core(args, enable_storage_manager=True) as core:
        result = core.command("storage.store.evacuate.apply", payload)
    emit_json(result, args)
    return 0 if bool(result.get("ok", False)) else 1


def cmd_storage_default_show(args: argparse.Namespace) -> int:
    return _storage_query(args, "storage.default.get", {})


def cmd_storage_default_set(args: argparse.Namespace) -> int:
    return _storage_command(
        args, "storage.default.set", {"store": _store_reference(args.store)}
    )


def cmd_storage_refresh(args: argparse.Namespace) -> int:
    return _storage_command(
        args,
        "storage.refresh",
        {
            "startup_on_add": bool(args.startup_on_add),
            "include_offline": bool(args.include_offline),
            "clear_existing": not bool(args.keep_existing),
            "strict": bool(args.strict),
        },
    )


def cmd_storage_files_list(args: argparse.Namespace) -> int:
    return _storage_query(
        args,
        "storage.files.list",
        {"limit": int(args.limit), "offset": int(args.offset)},
    )


def cmd_storage_file_locate(args: argparse.Namespace) -> int:
    payload: dict[str, Any] = {"asset_id": int(args.asset_id)}
    if args.store:
        payload["store_uuid"] = args.store
    return _storage_query(args, "storage.file.locate", payload)


def cmd_storage_file_get(args: argparse.Namespace) -> int:
    payload: dict[str, Any] = {"asset_id": int(args.asset_id)}
    if args.store:
        payload["store_uuid"] = args.store
    with open_cli_core(args, enable_storage_manager=True) as core:
        result = core.query("storage.file.read", payload)
    content = decode_wire_bytes(result.get("content"), label="storage file content")
    emit_bytes(
        content,
        output=args.file_output,
        replace=bool(args.replace_file_output),
    )
    if args.file_output != "-":
        print(
            json.dumps(
                {
                    "asset_id": args.asset_id,
                    "size": len(content),
                    "location": result.get("location"),
                },
                ensure_ascii=True,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
    return 0


def cmd_storage_file_put(args: argparse.Namespace) -> int:
    content = _bounded_file_bytes(args.input, args.max_transfer_mib)
    source = Path(args.input).expanduser()
    payload: dict[str, Any] = {
        "content_base64": base64.b64encode(content).decode("ascii"),
        "original_name": args.original_name or source.name,
    }
    if args.store:
        payload["store_uuid"] = args.store
    if args.name:
        payload["name"] = args.name
    if args.media_type:
        payload["media_type"] = args.media_type
    if args.metadata_file:
        payload["metadata"] = load_json_object(args.metadata_file)
    return _storage_command(args, "storage.file.put", payload)


def cmd_storage_file_copy(args: argparse.Namespace) -> int:
    payload: dict[str, Any] = {"asset_id": int(args.asset_id)}
    if args.store:
        payload["store"] = args.store
    if args.metadata_file:
        payload["metadata"] = load_json_object(args.metadata_file)
    return _storage_command(args, "storage.file.copy", payload)


def cmd_storage_file_delete(args: argparse.Namespace) -> int:
    if not args.yes:
        raise ValueError("Replica deletion requires --yes.")
    return _storage_command(
        args, "storage.file.delete", {"replica_id": int(args.replica_id)}
    )


def cmd_storage_location_stat(args: argparse.Namespace) -> int:
    return _storage_query(
        args,
        "storage.location.stat",
        {"store_uuid": args.store_uuid, "key": args.key},
    )


def cmd_storage_sources_list(args: argparse.Namespace) -> int:
    return _storage_query(args, "storage.sources.supported", {})


def cmd_storage_source_register(args: argparse.Namespace) -> int:
    return _storage_command(
        args,
        "storage.source.register",
        {"kind": args.kind, "options": load_json_object(args.options_file)},
    )


def cmd_storage_source_add(args: argparse.Namespace) -> int:
    kind = str(args.kind).lower().replace("-", "_")
    endpoint_fields = {
        "unmanaged_disk": "disk_path",
        "rclone_http": "remote_url",
        "wget_html": "remote_url",
        "native_html": "remote_url",
        "squashfs_open": "archive_path",
    }
    field = endpoint_fields.get(kind)
    if field is None:
        raise ValueError(
            "Typed source setup supports {}. Use `storage sources register` "
            "for another advertised kind.".format(", ".join(sorted(endpoint_fields)))
        )
    options: dict[str, Any] = {field: args.location}
    if args.name:
        options["store_name"] = args.name
    if args.extension:
        options["ebook_extensions"] = list(args.extension)
    if args.source_label:
        options["source_label"] = args.source_label
    if kind == "unmanaged_disk":
        options.update(
            {
                "compute_hash": not bool(args.no_hash),
                "follow_symlinks": bool(args.follow_symlinks),
                "attach_store_links": not bool(args.no_store_links),
                "refresh_storage_manager": not bool(args.no_refresh),
            }
        )
    elif kind in {"rclone_http", "wget_html", "native_html"}:
        if args.timeout is not None:
            options["timeout_s"] = float(args.timeout)
        if args.requests_per_hour is not None:
            options["max_http_requests_per_hour"] = float(args.requests_per_hour)
        options["attach_store_links"] = not bool(args.no_store_links)
        options["refresh_storage_manager"] = not bool(args.no_refresh)
    if args.options_file:
        options.update(load_json_object(args.options_file))
    return _storage_command(
        args,
        "storage.source.register",
        {"kind": kind, "options": options},
    )


def cmd_storage_asset_show(args: argparse.Namespace) -> int:
    return _storage_query(args, "storage.asset.get", {"asset_id": int(args.asset_id)})


def cmd_storage_replica_verify(args: argparse.Namespace) -> int:
    with open_cli_core(args, enable_storage_manager=True) as core:
        result = core.command(
            "storage.replica.verify",
            {
                "replica_id": int(args.replica_id),
                "calculate_digests": not bool(args.no_digests),
            },
        )
    emit_json(result, args)
    return 0 if bool(result.get("healthy", False)) else 1


def cmd_storage_asset_verify(args: argparse.Namespace) -> int:
    payload: dict[str, Any] = {"asset_id": int(args.asset_id)}
    if args.replica_id:
        payload["replica_ids"] = [int(value) for value in args.replica_id]
    if args.all_replicas:
        payload["all_replicas"] = True
    with open_cli_core(args, enable_storage_manager=True) as core:
        result = core.command("storage.asset.verify", payload)
    emit_json(result, args)
    return 0 if bool(result.get("healthy", False)) else 1
