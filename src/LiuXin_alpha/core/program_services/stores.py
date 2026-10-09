"""
Expose Store configuration, lifecycle, source/backend discovery, and copy operations through Core.

Configuration edits are normalized here; endpoint topology cannot change
while live Replica claims remain. Store lookup belongs to store_resolution.
Storage/library methods own persistence and backend policy. These adapters add
no enclosing transaction across mutations and subsequent status/readback, and
plain projections still rely on the outer Core wire encoder for opaque values.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any
from uuid import UUID

from LiuXin_alpha.core.errors import CoreDispatchError
from LiuXin_alpha.core.program_services.payloads import (
    _callable,
    _mapping,
    _payload,
    _required_int,
    _required_text,
    _text_list,
    plain,
)
from LiuXin_alpha.core.program_services.store_resolution import (
    _store,
    _store_configuration,
)
from LiuXin_alpha.storage.api import Location

if TYPE_CHECKING:
    from LiuXin_alpha.core.commands import CoreCommand
    from LiuXin_alpha.core.queries import CoreQuery
    from LiuXin_alpha.core.runtime import CoreRuntime


def storage_store_get(
    runtime: CoreRuntime,
    query: CoreQuery,
) -> dict[str, Any]:
    """
    Describe a live Store, falling back to durable configuration only on CoreDispatchError during lookup.

    The fallback reports unloaded/unavailable with unknown capacity; it is not a
    physical probe. Live configuration/status errors propagate without falling back.
    loaded=True means lookup succeeded, not that the Store's returned status is healthy.

    Example:
        >>> description = storage_store_get(runtime, query)  # doctest: +SKIP


    :param runtime: Runtime supplying live Store and durable configuration resolution.
    :param query: Query with required store reference accepted by the shared Store resolvers.
    :return: Projected configuration/status, UUID/name/root labels, and loaded flag.
    :raises CoreDispatchError: If the selector is absent/empty or both resolution paths fail.
    """
    payload = _payload(query)
    reference = payload.get("store")
    if reference in (None, ""):
        raise CoreDispatchError("`store` is required.")
    try:
        store = _store(runtime, reference)
    except CoreDispatchError:
        configuration = _store_configuration(runtime, reference)
        status: Any = {
            "available": False,
            "writable": False,
            "total_bytes": None,
            "free_bytes": None,
            "object_count": None,
            "checked_at": None,
            "message": "Store configuration is not currently loaded.",
            "warnings": [],
            "details": [],
        }
        loaded = False
    else:
        configuration = store.configuration
        status = plain(store.status())
        loaded = True
    return {
        "configuration": plain(configuration),
        "store_uuid": configuration.store_uuid,
        "store_name": configuration.store_name,
        "store_root_uri": configuration.store_root_uri,
        "loaded": loaded,
        "status": status,
    }


def _update_field_value(public_name: str, value: Any) -> Any:
    """
    Normalize a Store edit value according to its public field category, without validating field membership.

    Name/root require nonblank stripped text; read_only uses truthiness. Policy
    IDs preserve None or use plain int, accepting bool and negative values here.
    Other fields preserve None or unstripped stringified text.

    Example:
        >>> _update_field_value("name", " archive "), _update_field_value("read_only", "false")
        ('archive', True)


    :param public_name: Public edit key selecting the normalization rule.
    :param value: Supplied replacement value; downstream configuration validation still applies.
    :return: Normalized text, bool, integer, or None for dataclass replacement.
    :raises CoreDispatchError: If name/root becomes blank; conversion errors otherwise propagate normally.
    """
    if public_name in {"name", "root"}:
        text = str(value or "").strip()
        if not text:
            raise CoreDispatchError(f"`{public_name}` must not be empty.")
        return text
    if public_name == "read_only":
        return bool(value)
    if public_name in {"replication_policy_id", "backup_policy_id"}:
        return None if value is None else int(value)
    return None if value is None else str(value)


def storage_store_update(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    """
    Normalize allowlisted durable Store edits, reject unsafe endpoint changes, and report the updated live status.

    Editable fields are name, root, url, protocol, failure_domain, region,
    operational_role, read_only, both default policy IDs, and add_tags/remove_tags.
    Tags are exact-case sets sorted for storage, with removal winning conflicts.
    Empty replacement sets are rejected, but supplied values need not differ.

    Changed root/URL/protocol is forbidden while any claim's state is not the
    DELETED enum member. This check does not reserve topology against concurrent
    changes. Dataclass replacement and manager policy remain authoritative; live
    lookup/status can fail after update_store has persisted its change.

    Example:
        >>> receipt = storage_store_update(runtime, command)  # doctest: +SKIP


    :param runtime: Runtime providing configuration lookup, replica enumeration, Store update, and live status.
    :param command: Command with required store reference and Mapping changes using the public keys above.
    :return: updated=True, string Store UUID, projected manager result, and live status after successful readback.
    :raises CoreDispatchError: For invalid fields/values, no replacements, or endpoint changes with remaining live claims.
    """
    from LiuXin_alpha.storage import api as storage_api

    payload = _payload(command)
    reference = payload.get("store")
    if reference in (None, ""):
        raise CoreDispatchError("`store` is required.")
    changes = _mapping(payload, "changes")
    configuration = _store_configuration(runtime, reference)
    allowed = {
        "name": "store_name",
        "root": "store_root_uri",
        "url": "store_url",
        "protocol": "store_access_protocol",
        "failure_domain": "store_failure_domain",
        "region": "store_region",
        "operational_role": "operational_role",
        "read_only": "read_only",
        "replication_policy_id": "store_default_replication_policy_id",
        "backup_policy_id": "store_default_backup_policy_id",
    }
    unknown = set(changes) - set(allowed) - {"add_tags", "remove_tags"}
    if unknown:
        raise CoreDispatchError(
            "Unknown Store update fields: {}.".format(", ".join(sorted(unknown)))
        )
    replacements: dict[str, Any] = {}
    for public_name, attribute in allowed.items():
        if public_name not in changes:
            continue
        replacements[attribute] = _update_field_value(public_name, changes[public_name])
    tags = set(configuration.store_tags)
    tags.update(_text_list(changes, "add_tags"))
    tags.difference_update(_text_list(changes, "remove_tags"))
    if "add_tags" in changes or "remove_tags" in changes:
        replacements["store_tags"] = tuple(sorted(tags))
    if not replacements:
        raise CoreDispatchError("No Store configuration changes were supplied.")
    topology_changes = {
        attribute
        for attribute in (
            "store_root_uri",
            "store_url",
            "store_access_protocol",
        )
        if attribute in replacements
        and replacements[attribute] != getattr(configuration, attribute)
    }
    if topology_changes and any(
        record.state is not storage_api.ReplicaState.DELETED
        for record in runtime.library.storage.iter_replica_records(
            store_ref=configuration.store_uuid
        )
    ):
        raise CoreDispatchError(
            "Store endpoint fields cannot change while live Replica claims "
            "remain; evacuate the Store first."
        )
    updated = dataclasses.replace(configuration, **replacements)
    result = runtime.library.storage.update_store(
        configuration.store_uuid,
        updated,
    )
    live = runtime.library.storage.get_store(configuration.store_uuid)
    return {
        "updated": True,
        "store_uuid": str(configuration.store_uuid),
        "configuration": plain(result),
        "status": plain(live.status()),
    }


def storage_default_get(
    runtime: CoreRuntime,
    query: CoreQuery,
) -> dict[str, Any]:
    """
    Resolve the manager's default Store, treating ordinary default/Store lookup errors as unconfigured.

    Configuration access and projection occur after that error handler and may
    still raise. The result describes selection, without probing availability.

    Example:
        >>> selected = storage_default_get(runtime, query)  # doctest: +SKIP


    :param runtime: Runtime whose storage manager supplies the default reference and Store.
    :param query: Ignored query envelope; no payload fields are consumed.
    :return: configured=False/store=None on lookup failure, otherwise UUID/name/root and projected configuration.
    """
    del query
    storage = runtime.library.storage
    try:
        store = storage.get_store(storage.get_default_store_ref())
    except Exception:
        return {"configured": False, "store": None}
    configuration = store.configuration
    return {
        "configured": True,
        "store": {
            "store_uuid": configuration.store_uuid,
            "store_name": configuration.store_name,
            "store_root_uri": configuration.store_root_uri,
            "configuration": plain(configuration),
        },
    }


def storage_location_stat(
    runtime: CoreRuntime,
    query: CoreQuery,
) -> dict[str, Any]:
    """
    Construct a UUID/key Location and delegate stat without converting absence or errors into a negative result.

    exists=True is unconditional after stat returns, even if an alternate manager
    returns None. UUID errors and storage exceptions propagate. No Store-name
    fallback or adapter path-confinement check is performed here.

    Example:
        >>> observation = storage_location_stat(runtime, query)  # doctest: +SKIP


    :param runtime: Runtime supplying storage.stat for the constructed Location.
    :param query: Query with required stripped store_uuid and key text.
    :return: Projected location, exists=True, and projected stat result after successful delegation.
    """
    payload = _payload(query)
    location = Location(
        UUID(_required_text(payload, "store_uuid")),
        _required_text(payload, "key"),
    )
    info = runtime.library.storage.stat(location)
    return {
        "location": plain(location),
        "exists": True,
        "stat": plain(info),
    }


def storage_sources_supported(
    runtime: CoreRuntime,
    query: CoreQuery,
) -> dict[str, Any]:
    """
    Advertise the five fixed source-registration families without probing runtime capabilities or installed tools.

    Example:
        >>> [entry["kind"] for entry in storage_sources_supported(None, None)["kinds"]][:2]
        ['unmanaged_disk', 'rclone_http']


    :param runtime: Unused runtime; returned families are static registration metadata.
    :param query: Unused query envelope; no filtering is supported.
    :return: Ordered kind/method/remote declarations for disk, rclone HTTP, wget HTML, native HTML, and open SquashFS.
    """
    del runtime, query
    return {
        "kinds": [
            {
                "kind": "unmanaged_disk",
                "method": "register_unmanaged_disk",
                "remote": False,
            },
            {
                "kind": "rclone_http",
                "method": "register_rclone_http_store",
                "remote": True,
            },
            {
                "kind": "wget_html",
                "method": "register_wget_html_store",
                "remote": True,
            },
            {
                "kind": "native_html",
                "method": "register_native_html_store",
                "remote": True,
            },
            {
                "kind": "squashfs_open",
                "method": "ensure_open_squashfs_store",
                "remote": False,
            },
        ]
    }


def storage_backends_list(
    runtime: CoreRuntime,
    query: CoreQuery,
) -> dict[str, Any]:
    """
    Describe default-registry backend kinds, declared capabilities, characteristics, and limitations.

    Only user-selectable descriptors are requested unless include_internal is
    truthy. Registry order is retained and no Store instances are opened or probed.
    The credentials message is guidance, not a secret-validation operation here.

    Example:
        >>> backends = storage_backends_list(runtime, query)  # doctest: +SKIP


    :param runtime: Unused runtime; discovery uses DEFAULT_BACKEND_REGISTRY.
    :param query: Query with optional truth-tested include_internal, defaulting to False.
    :return: Backend descriptor list, count, and non-secret configuration guidance.
    """
    from LiuXin_alpha.storage.utils.backend_registry import (
        DEFAULT_BACKEND_REGISTRY,
    )

    del runtime
    payload = _payload(query)
    include_internal = bool(payload.get("include_internal", False))
    backends = []
    for descriptor in DEFAULT_BACKEND_REGISTRY.iter_descriptors(
        user_selectable_only=not include_internal,
    ):
        characteristics = descriptor.characteristics
        backends.append(
            {
                "kind": descriptor.kind,
                "label": descriptor.label,
                "aliases": list(descriptor.aliases),
                "location_type": descriptor.location_type,
                "access_protocol": descriptor.access_protocol,
                "access_protocol_aliases": list(descriptor.access_protocol_aliases),
                "read_only_default": descriptor.read_only_default,
                "user_selectable": descriptor.user_selectable,
                "policy_section": descriptor.policy_section,
                "capabilities": {
                    "folders": descriptor.supports_folders,
                    "hierarchical_list": (descriptor.supports_hierarchical_list),
                    "random_read": descriptor.supports_random_read,
                    "random_write": descriptor.supports_random_write,
                    "delete": descriptor.supports_delete,
                    "checksums": descriptor.supports_checksums,
                    "immutable_objects": (descriptor.supports_immutable_objects),
                },
                "characteristics": plain(characteristics),
                "limitations": [
                    {
                        "code": limitation.code,
                        "message": limitation.message,
                    }
                    for limitation in characteristics.limitations
                ],
            }
        )
    return {
        "backends": backends,
        "count": len(backends),
        "credentials": (
            "Store configuration persists non-secret options only. Use "
            "backend-native credential files, profiles, environment "
            "injection, or an external secret provider."
        ),
    }


def storage_store_probe(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    """
    Probe one live Store and then separately read its current status.

    The probe may perform backend I/O or update observations. The subsequent status
    read is not an atomic snapshot of the probe and can fail after it completes.

    Example:
        >>> receipt = storage_store_probe(runtime, command)  # doctest: +SKIP


    :param runtime: Runtime supplying live Store resolution.
    :param command: Command with a required nonempty store reference.
    :return: Original selector, projected probe result as status, and projected later live_status.
    """
    payload = _payload(command)
    reference = payload.get("store")
    if reference in (None, ""):
        raise CoreDispatchError("`store` is required.")
    store = _store(runtime, reference)
    result = store.probe()
    return {
        "store": reference,
        "status": plain(result),
        "live_status": plain(store.status()),
    }


def storage_store_delete(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    """
    Unregister a Store and optionally forget configuration and delete matching canonical Store rows.

    delete_from_database is truth-tested and defaults False. Canonical search and
    sequential row deletion run only when remove_store returns truthy and forgetting
    was requested. Those later operations can fail after unregistration; this
    adapter adds no transaction, confirmation field, or byte-deletion guarantee.

    Example:
        >>> receipt = storage_store_delete(runtime, command)  # doctest: +SKIP


    :param runtime: Runtime providing Store configuration lookup, manager removal, and database search/delete.
    :param command: Command with required store and optional delete_from_database switch.
    :return: Original selector, raw removal outcome as unregistered, and requested-and-removed database-deletion flag.
    """
    payload = _payload(command)
    reference = payload.get("store")
    if reference in (None, ""):
        raise CoreDispatchError("`store` is required.")
    storage = runtime.library.storage
    configuration = _store_configuration(runtime, reference)
    deleted_from_database = bool(payload.get("delete_from_database", False))
    removed = storage.remove_store(
        configuration.store_uuid,
        forget_configuration=deleted_from_database,
    )
    if removed and deleted_from_database:
        rows = runtime.database.search(
            "stores",
            "store_uuid",
            str(configuration.store_uuid),
        )
        for row in rows:
            runtime.database.delete(row)
    return {
        "store": reference,
        "unregistered": removed,
        "deleted_from_database": deleted_from_database and removed,
    }


def storage_default_set(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    """
    Resolve a live Store and ask the manager to select it as default, then read its name for the receipt.

    Manager eligibility/persistence policy applies. selected=True means its setter
    returned, not that a separate default lookup verified the selection; later
    configuration access may fail after the change.

    Example:
        >>> receipt = storage_default_set(runtime, command)  # doctest: +SKIP


    :param runtime: Runtime providing live Store resolution and default-store selection.
    :param command: Command containing a required nonempty store reference.
    :return: selected=True and the selected Store's reference and configuration name.
    """
    payload = _payload(command)
    reference = payload.get("store")
    if reference in (None, ""):
        raise CoreDispatchError("`store` is required.")
    storage = runtime.library.storage
    store = _store(runtime, reference)
    storage.set_default_store(store.store_ref)
    return {
        "selected": True,
        "store_uuid": store.store_ref,
        "store_name": store.configuration.store_name,
    }


def storage_file_copy(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    """
    Read an entire managed Asset, add its bytes through library policy, and locate the returned Asset.

    Reading occurs before metadata or target-Store validation, with no byte cap.
    Metadata must be None/Mapping; no source metadata is automatically copied here.
    Missing/empty store delegates default placement to add_file. The library may
    deduplicate rather than allocate a new Asset; this is not a backend-native copy.
    Location/projection failure after addition does not trigger compensating removal.

    Example:
        >>> receipt = storage_file_copy(runtime, command)  # doctest: +SKIP


    :param runtime: Runtime supplying library read/add/locate operations and optional Store selection.
    :param command: Command with required asset_id, optional Mapping metadata, and optional target store.
    :return: Source ID, projected returned Asset and Location, and source content length in size.
    """
    payload = _payload(command)
    asset_id = _required_int(payload, "asset_id")
    content = runtime.library.read_file(asset_id)
    metadata = payload.get("metadata")
    if metadata is not None and not isinstance(metadata, Mapping):
        raise CoreDispatchError("`metadata` must be an object or null.")
    target_store = (
        None
        if payload.get("store") in (None, "")
        else _store(runtime, payload["store"])
    )
    asset = runtime.library.add_file(
        bytes(content),
        metadata=None if metadata is None else dict(metadata),
        store=target_store,
    )
    location = runtime.library.locate_file(asset, store=target_store)
    return {
        "source_asset_id": asset_id,
        "asset": plain(asset),
        "location": plain(location),
        "size": len(content),
    }


def storage_source_register(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    """
    Invoke an allowlisted library source-registration method with caller options and reconcile afterwards.

    Kind is stripped, lowercased, and hyphen-to-underscore normalized. Options must
    be a Mapping and are expanded as keyword arguments without a per-provider
    adapter schema; the library owns validation, scanning, and persistence. Returned
    reports are projected before reconciliation, which can fail after registration.

    Example:
        >>> receipt = storage_source_register(runtime, command)  # doctest: +SKIP


    :param runtime: Runtime whose library provides the selected registration method and reconciliation.
    :param command: Command with kind and required Mapping options for one advertised source family.
    :return: Reconciled normalized kind, registered=True, and projected library report; not an independent registration probe.
    :raises CoreDispatchError: If kind/options validation fails or the required library callable is unavailable.
    """
    payload = _payload(command)
    kind = _required_text(payload, "kind").lower().replace("-", "_")
    options = _mapping(payload, "options")
    method_names = {
        "unmanaged_disk": "register_unmanaged_disk",
        "rclone_http": "register_rclone_http_store",
        "wget_html": "register_wget_html_store",
        "native_html": "register_native_html_store",
        "squashfs_open": "ensure_open_squashfs_store",
    }
    method_name = method_names.get(kind)
    if method_name is None:
        raise CoreDispatchError(f"Unsupported storage source kind: {kind!r}.")
    method = _callable(
        runtime.library,
        method_name,
        area="library storage registration",
    )
    result = method(**options)
    return runtime.services.reconcile(
        {
            "kind": kind,
            "registered": True,
            "report": plain(result),
        }
    )
