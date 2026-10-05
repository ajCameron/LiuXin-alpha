"""
Translate Store, asset, replica, source, and file administration into Core requests.

Most commands use shared request/publication helpers and return zero after JSON
output regardless of receipt flags. Evacuation apply checks ok; asset/replica
verification checks healthy. File download emits bytes instead of a JSON report.
Local file/control reads precede the relevant requests, but report destinations
are not preflighted before mutation. Later output failures do not roll back Core
changes. Store selectors have deliberately different payload keys/coercion across
operations; preserve each operation's contract rather than normalizing globally.
"""

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
    """
    List configured Stores, forwarding the refresh selector unchanged.

    Example:
        >>> cmd_storage_stores_list(parsed_stores_args)  # doctest: +SKIP


    :param args: Core/output controls and refresh flag passed to storage.stores.list.
    :return: Zero after publishing the query receipt without checking its status flags.
    """
    return _storage_query(args, "storage.stores.list", {"refresh": args.refresh})


def cmd_storage_store_show(args: argparse.Namespace) -> int:
    """
    Fetch one Store using a stripped numeric-ID-or-name selector.

    Example:
        >>> cmd_storage_store_show(parsed_store_show_args)  # doctest: +SKIP


    :param args: Core/output controls and store selector converted by _store_reference.
    :return: Zero after publishing storage.store.get, without requiring a found record.
    """
    return _storage_query(
        args, "storage.store.get", {"store": _store_reference(args.store)}
    )


def cmd_storage_store_save(args: argparse.Namespace) -> int:
    """
    Load a CLI-host Store object and submit it to Core persistence without a local schema.

    The bounded JSON object is loaded before opening Core. This raw save path does
    not apply typed Store-add capability checks or its refresh/probe sequence.

    Example:
        >>> cmd_storage_store_save(parsed_store_save_args)  # doctest: +SKIP


    :param args: store_file path plus connection and receipt-publication options.
    :return: Zero after publishing storage.store.save, independent of nested success flags.
    """
    return _storage_command(
        args, "storage.store.save", {"store": load_json_object(args.store_file)}
    )


def cmd_storage_backends_list(args: argparse.Namespace) -> int:
    """
    Request backend descriptors with optional internal-provider visibility.

    Example:
        >>> cmd_storage_backends_list(parsed_backends_args)  # doctest: +SKIP


    :param args: Connection/output selectors and truth-tested include_internal option.
    :return: Zero after publishing storage.backends.list without probing providers here.
    """
    return _storage_query(
        args,
        "storage.backends.list",
        {"include_internal": bool(args.include_internal)},
    )


def cmd_storage_store_update(args: argparse.Namespace) -> int:
    """
    Build a partial Store update, preserving unspecified values and explicit clearing.

    Non-None scalar options are copied unchanged. Clear flags override supplied
    failure-domain/region values; read_only is boolean-coerced, and truthy tag
    lists are copied without deduplication. An empty changes dictionary is allowed.

    Example:
        >>> cmd_storage_store_update(parsed_store_update_args)  # doctest: +SKIP


    :param args: Store selector, scalar/policy fields, clear/read-only/tag controls,
        and shared connection/output settings.
    :return: Zero after publishing storage.store.update; Core validates update semantics.
    """
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
    """
    Ask Core to probe one Store and publish the probe receipt without interpreting health.

    Example:
        >>> cmd_storage_store_probe(parsed_store_probe_args)  # doctest: +SKIP


    :param args: Numeric-ID-or-name Store selector plus connection/output controls.
    :return: Zero after publication, even when the receipt reports unavailable status.
    """
    return _storage_command(
        args, "storage.store.probe", {"store": _store_reference(args.store)}
    )


def cmd_storage_store_delete(args: argparse.Namespace) -> int:
    """
    Require --yes before asking Core to remove a Store with the selected database policy.

    This handler delegates deletion semantics; it does not independently delete
    local bytes or add rollback for later receipt-publication failure.

    Example:
        >>> cmd_storage_store_delete(parsed_store_delete_args)  # doctest: +SKIP


    :param args: store, yes, delete_from_database, and shared Core/output settings.
    :return: Zero after publishing storage.store.delete without inspecting its receipt flags.
    :raises ValueError: The explicit confirmation flag is falsey.
    """
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
    """
    Preview Store evacuation unless confirmed, then apply with action/byte budgets.

    Both selectors use numeric-ID-or-name conversion. Preview forwards max_assets;
    apply additionally truncates binary GiB to bytes and forwards keep_source_bytes.
    No local positivity/range checks or saved-plan equality checks are performed.

    Example:
        >>> cmd_storage_store_evacuate(parsed_evacuation_args)  # doctest: +SKIP


    :param args: Source/optional destination, asset/action/transfer limits, yes,
        source-retention policy, and shared Core/output controls.
    :return: Zero after preview publication; after apply, zero for truthy receipt.ok,
        otherwise one. Output is published before the apply status is interpreted.
    """
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
    """
    Publish Core's current default-Store selection without requiring one to exist.

    Example:
        >>> cmd_storage_default_show(parsed_default_show_args)  # doctest: +SKIP


    :param args: Core connection selectors and JSON publication options.
    :return: Zero after publishing storage.default.get with an empty request payload.
    """
    return _storage_query(args, "storage.default.get", {})


def cmd_storage_default_set(args: argparse.Namespace) -> int:
    """
    Request default-Store selection by numeric ID or name, leaving eligibility to Core.

    Example:
        >>> cmd_storage_default_set(parsed_default_set_args)  # doctest: +SKIP


    :param args: store selector plus Core connection and JSON output settings.
    :return: Zero after publishing storage.default.set without a local health check.
    """
    return _storage_command(
        args, "storage.default.set", {"store": _store_reference(args.store)}
    )


def cmd_storage_refresh(args: argparse.Namespace) -> int:
    """
    Refresh storage-manager composition using the selected startup/offline/strict policy.

    keep_existing is inverted into clear_existing; all four policy values use
    truthiness. A returned failure count does not change this handler's exit code.

    Example:
        >>> cmd_storage_refresh(parsed_refresh_args)  # doctest: +SKIP


    :param args: startup_on_add, include_offline, keep_existing, strict, and Core/output controls.
    :return: Zero after publishing storage.refresh without interpreting configuration failures.
    """
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
    """
    Request a page of storage files with integer-converted pagination values.

    Example:
        >>> cmd_storage_files_list(parsed_files_list_args)  # doctest: +SKIP


    :param args: limit/offset plus shared Core/output settings; ranges are not checked here.
    :return: Zero after publishing storage.files.list, including empty pages.
    """
    return _storage_query(
        args,
        "storage.files.list",
        {"limit": int(args.limit), "offset": int(args.offset)},
    )


def cmd_storage_file_locate(args: argparse.Namespace) -> int:
    """
    Locate an asset, optionally restricting lookup to an unchanged Store UUID selector.

    Example:
        >>> cmd_storage_file_locate(parsed_file_locate_args)  # doctest: +SKIP


    :param args: Integer-convertible asset_id, optional truthy store sent as store_uuid,
        and shared connection/output controls.
    :return: Zero after publishing storage.file.locate without requiring a usable location.
    """
    payload: dict[str, Any] = {"asset_id": int(args.asset_id)}
    if args.store:
        payload["store_uuid"] = args.store
    return _storage_query(args, "storage.file.locate", payload)


def cmd_storage_file_get(args: argparse.Namespace) -> int:
    """
    Read asset bytes through Core and publish them on the CLI host or stdout.

    Query and session cleanup precede strict wire decoding and destination checks;
    there is no local download-size cap or metadata verification. For a filesystem
    destination print an ASCII-escaped size/location summary on stderr after the
    bytes are published. Summary failure does not retract the output file.

    Example:
        >>> cmd_storage_file_get(parsed_file_get_args)  # doctest: +SKIP


    :param args: asset_id, optional store_uuid selector via store, file_output,
        replace_file_output, and connection options; JSON-output controls are unused.
    :return: Zero after byte publication and any stderr summary, without a receipt health check.
    """
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
    """
    Read a size-bounded CLI-host file and submit its base64 contents for managed storage.

    Read the file and optional metadata object before opening Core. original_name
    falls back to the expanded path's basename; other falsey optional fields are
    omitted. The request contains bytes, not a server-side path, and does not check
    whether the input changed during reading.

    Example:
        >>> cmd_storage_file_put(parsed_file_put_args)  # doctest: +SKIP


    :param args: input/max_transfer_mib, naming/media/metadata controls, optional store
        forwarded as store_uuid, and shared connection/output settings.
    :return: Zero after publishing storage.file.put; receipt publication may fail after storage.
    """
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
    """
    Request a managed asset copy with optional destination and JSON metadata overrides.

    Load metadata before opening Core. Unlike file put/locate, this request uses
    the key store and does not locally convert its value to a numeric reference.

    Example:
        >>> cmd_storage_file_copy(parsed_file_copy_args)  # doctest: +SKIP


    :param args: asset_id, optional truthy store/metadata_file, and Core/output settings.
    :return: Zero after publishing storage.file.copy without interpreting success flags.
    """
    payload: dict[str, Any] = {"asset_id": int(args.asset_id)}
    if args.store:
        payload["store"] = args.store
    if args.metadata_file:
        payload["metadata"] = load_json_object(args.metadata_file)
    return _storage_command(args, "storage.file.copy", payload)


def cmd_storage_file_delete(args: argparse.Namespace) -> int:
    """
    Require explicit confirmation before requesting deletion of one replica.

    Example:
        >>> cmd_storage_file_delete(parsed_file_delete_args)  # doctest: +SKIP


    :param args: replica_id, yes confirmation, and shared connection/output controls.
    :return: Zero after publishing storage.file.delete; later output failure cannot undo deletion.
    :raises ValueError: The yes flag is falsey, before Core is opened.
    """
    if not args.yes:
        raise ValueError("Replica deletion requires --yes.")
    return _storage_command(
        args, "storage.file.delete", {"replica_id": int(args.replica_id)}
    )


def cmd_storage_location_stat(args: argparse.Namespace) -> int:
    """
    Inspect a storage location through Core using its unchanged Store UUID and key.

    Example:
        >>> cmd_storage_location_stat(parsed_location_stat_args)  # doctest: +SKIP


    :param args: store_uuid/key plus Core/output controls; path semantics belong to Core.
    :return: Zero after publishing storage.location.stat without requiring existence.
    """
    return _storage_query(
        args,
        "storage.location.stat",
        {"store_uuid": args.store_uuid, "key": args.key},
    )


def cmd_storage_sources_list(args: argparse.Namespace) -> int:
    """
    Publish Core's supported source-registration kinds without registering a source.

    Example:
        >>> cmd_storage_sources_list(parsed_sources_list_args)  # doctest: +SKIP


    :param args: Connection selectors and JSON output options.
    :return: Zero after the storage.sources.supported query receipt is published.
    """
    return _storage_query(args, "storage.sources.supported", {})


def cmd_storage_source_register(args: argparse.Namespace) -> int:
    """
    Register a source using its unchanged kind and a CLI-host JSON options object.

    This generic path does not apply typed source-add normalization or defaults.
    Options are loaded before opening Core and validated by the Core operation.

    Example:
        >>> cmd_storage_source_register(parsed_source_register_args)  # doctest: +SKIP


    :param args: kind/options_file plus connection and JSON publication settings.
    :return: Zero after publishing storage.source.register, irrespective of receipt flags.
    """
    return _storage_command(
        args,
        "storage.source.register",
        {"kind": args.kind, "options": load_json_object(args.options_file)},
    )


def cmd_storage_source_add(args: argparse.Namespace) -> int:
    """
    Build typed local/HTTP/archive source options, then apply JSON overrides last.

    Lowercase kind and replace hyphens without stripping surrounding whitespace.
    Support unmanaged_disk, rclone_http, wget_html, native_html, and squashfs_open.
    Disk options include hash/symlink/link/refresh policies; HTTP kinds add optional
    float timing/rate controls plus link/refresh policies. SquashFS receives neither
    group. The optional JSON object can override even the endpoint and typed defaults;
    no local secret filtering, positive-range validation, or source probing is added.

    Example:
        >>> cmd_storage_source_add(parsed_source_add_args)  # doctest: +SKIP


    :param args: kind/location, optional source name/extensions/label, backend policy
        flags, options_file overrides, and connection/output controls.
    :return: Zero after publishing storage.source.register with normalized kind and merged options.
    :raises ValueError: The normalized kind is outside the supported typed setup set.
    """
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
    """
    Publish one asset's storage record by integer-converted ID.

    Example:
        >>> cmd_storage_asset_show(parsed_asset_show_args)  # doctest: +SKIP


    :param args: asset_id and shared Core connection/JSON publication controls.
    :return: Zero after storage.asset.get publication without requiring a found asset.
    """
    return _storage_query(args, "storage.asset.get", {"asset_id": int(args.asset_id)})


def cmd_storage_replica_verify(args: argparse.Namespace) -> int:
    """
    Verify one replica through Core and map the reported healthy flag to exit status.

    Example:
        >>> cmd_storage_replica_verify(parsed_replica_verify_args)  # doctest: +SKIP


    :param args: replica_id, no_digests inverted into calculate_digests, and Core/output settings.
    :return: Zero for truthy receipt.healthy, otherwise one, after publishing the full receipt.
    """
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
    """
    Verify an asset using optional replica IDs and/or the all-replicas selector.

    Preserve repeated IDs and forward both selectors if supplied; this handler
    does not enforce exclusivity or validate positive ID ranges.

    Example:
        >>> cmd_storage_asset_verify(parsed_asset_verify_args)  # doctest: +SKIP


    :param args: asset_id, optional repeated replica_id, all_replicas, and Core/output options.
    :return: Zero for truthy receipt.healthy, otherwise one, after receipt publication.
    """
    payload: dict[str, Any] = {"asset_id": int(args.asset_id)}
    if args.replica_id:
        payload["replica_ids"] = [int(value) for value in args.replica_id]
    if args.all_replicas:
        payload["all_replicas"] = True
    with open_cli_core(args, enable_storage_manager=True) as core:
        result = core.command("storage.asset.verify", payload)
    emit_json(result, args)
    return 0 if bool(result.get("healthy", False)) else 1
