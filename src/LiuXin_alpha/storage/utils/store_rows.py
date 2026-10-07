"""
Read, backfill, and dependency-order durable Store catalogue rows.

These helpers isolate compatibility row access and bootstrap ordering from the
application-facing StorageManager.  They do not own transactions or construct
Stores; callers retain those orchestration boundaries.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import urlparse
from uuid import UUID

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.utils.backend_registry import DEFAULT_BACKEND_REGISTRY
from LiuXin_alpha.storage.utils.store_configuration import store_configuration_from_row


def _row_value(row: Any, key: str):
    """
    Return a subscribed row value, falling back to an attribute or None.

    Example:
        >>> _row_value({"store_id": 7}, "store_id")
        7


    :param row: Mapping-like or attribute-based row object.
    :param key: Requested column or attribute name.
    :return: The subscribed or fallback value, or None when both are unavailable.
    """

    try:
        return row[key]
    except Exception:
        return getattr(row, key, None)


def _row_int(row: Any, key: str) -> int | None:
    """
    Return an optional row value converted to ``int``.

    Example:
        >>> _row_int({"store_id": "7"}, "store_id")
        7


    :param row: Mapping-like or attribute-based row object.
    :param key: Requested column or attribute name.
    :return: The converted integer, or None for absent and invalid values.
    """

    try:
        value = _row_value(row, key)
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _row_text(row: Any, key: str) -> str | None:
    """
    Return a stripped, nonempty text representation of a row value.

    Example:
        >>> _row_text({"name": " books "}, "name")
        'books'


    :param row: Mapping-like or attribute-based row object.
    :param key: Requested column or attribute name.
    :return: Stripped nonempty text, or None when the value is absent or blank.
    """

    value = _row_value(row, key)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _row_uuid(row: Any, key: str) -> UUID | None:
    """
    Return a row UUID, or None for absent and ValueError-invalid values.

    Example:
        >>> _row_uuid({"id": "not-a-uuid"}, "id") is None
        True


    :param row: Mapping-like or attribute-based row object.
    :param key: Requested column or attribute name.
    :return: The existing or parsed UUID, or None for absent and invalid values.
    """

    value = _row_value(row, key)
    if isinstance(value, UUID):
        return value
    if value is None or value == "":
        return None
    try:
        return UUID(str(value))
    except ValueError:
        return None


def _persist_derived_store_uuid(
    database: Any,
    *,
    row: Any,
    store_id: int | None,
    store_ref: api.StoreUUID,
) -> None:
    """
    Backfill a missing Store UUID when the row and database permit it.

    No write occurs without a row ID, when a UUID is already present, when an
    explicit allowed-column set excludes ``store_uuid``, or when the database
    has no callable update macro.

    Example:
        >>> _persist_derived_store_uuid(database, row=row, store_id=7, store_ref=store_ref)  # doctest: +SKIP


    :param database: Database whose update-row macro owns the optional backfill.
    :param row: Store row inspected for UUID and allowed-column compatibility.
    :param store_id: Durable row identity, or None when the row cannot be updated.
    :param store_ref: Derived Store UUID persisted when all preconditions hold.
    :return: None after skipping or completing the backfill.
    """

    if store_id is None or _row_text(row, "store_uuid") is not None:
        return
    allowed_columns = getattr(row, "allowed_columns", None)
    if allowed_columns is not None and "store_uuid" not in set(allowed_columns):
        return
    macros = getattr(database, "macros", None)
    update_row = getattr(macros, "update_row", None)
    if not callable(update_row):
        return
    update_row(
        "stores",
        store_id,
        {"store_uuid": str(store_ref)},
        id_column="store_id",
    )


def _row_kind(row: Any) -> str:
    """
    Return the row's locally normalized backend-kind text.

    Example:
        >>> _row_kind({"store_kind": " Encrypted-Store "})
        'encrypted_store'


    :param row: Store row carrying an optional store_kind value.
    :return: Lowercase, underscore-normalized kind text, possibly empty.
    """

    return (_row_text(row, "store_kind") or "").lower().replace("-", "_")


def _is_encrypted_row(row: Any) -> bool:
    """
    Return whether a row uses a recognized encrypted backend alias.

    Example:
        >>> _is_encrypted_row({"store_kind": "aes-gcm"})
        True


    :param row: Store row carrying an optional backend kind.
    :return: True only when the default registry resolves the kind to encrypted.
    """

    kind = _row_kind(row)
    try:
        return DEFAULT_BACKEND_REGISTRY.canonical_kind(kind) == "encrypted"
    except (ValueError, api.StoreUnsupportedOperation):
        return False


def _configuration_dependencies(
    manager: Any,
    configuration: api.StoreConfiguration,
) -> frozenset[api.StoreUUID]:
    """
    Collect Store dependencies used to order database bootstrap rows.

    Backing materialization and preferred-Replica Stores are included when
    available.  Encrypted Stores also depend on their configured or URI-derived
    inner Store UUID.  The Store's own UUID is removed.

    Example:
        >>> dependencies = _configuration_dependencies(manager, configuration)  # doctest: +SKIP


    :param manager: Manager used for an optional preferred-Replica lookup.
    :param configuration: Store configuration whose backing and wrapper references are inspected.
    :return: Discovered non-self Store UUID dependencies.
    """

    dependencies: set[api.StoreUUID] = set()
    backing = configuration.backing
    if backing is not None:
        if backing.materialization_store_ref is not None:
            dependencies.add(backing.materialization_store_ref)
        if backing.preferred_replica_id is not None:
            try:
                replica = manager.get_replica_record(backing.preferred_replica_id)
            except api.ReplicaNotFound:
                pass
            else:
                if replica.digital_asset_id == backing.digital_asset_id:
                    dependencies.add(replica.location.store_ref)

    try:
        kind = DEFAULT_BACKEND_REGISTRY.canonical_kind(configuration.store_kind)
    except (ValueError, api.StoreUnsupportedOperation):
        kind = configuration.store_kind
    if kind == "encrypted":
        raw_inner_ref = dict(configuration.backend_options).get("inner_store_uuid")
        if raw_inner_ref is None:
            parsed = urlparse(configuration.store_root_uri)
            raw_inner_ref = parsed.netloc or parsed.path.strip("/") or None
        try:
            if raw_inner_ref is not None:
                dependencies.add(UUID(str(raw_inner_ref)))
        except ValueError:
            pass
    dependencies.discard(configuration.store_uuid)
    return frozenset(dependencies)


def _order_store_rows(
    manager: Any,
    rows: tuple[Any, ...],
) -> tuple[Any, ...]:
    """
    Order Store rows by discoverable in-snapshot dependencies.

    Malformed rows remain available for later error reporting, and cycles use a
    deterministic fallback ordering instead of being silently discarded.

    Example:
        >>> _order_store_rows(None, ())
        ()


    :param manager: Manager used to discover preferred-Replica dependencies.
    :param rows: Snapshot of original database row objects.
    :return: The original row objects in deterministic bootstrap order.
    """

    translated: list[tuple[Any, api.StoreConfiguration | None]] = []
    for row in rows:
        try:
            configuration = store_configuration_from_row(
                row,
                fallback_store_id=_row_int(row, "store_id"),
            )
        except Exception:
            configuration = None
        translated.append((row, configuration))

    configured_refs = {
        configuration.store_uuid
        for _, configuration in translated
        if configuration is not None
    }
    remaining = list(translated)
    ordered: list[Any] = []
    while remaining:
        remaining_refs = {
            configuration.store_uuid
            for _, configuration in remaining
            if configuration is not None
        }
        ready = [
            item
            for item in remaining
            if item[1] is None
            or not (
                _configuration_dependencies(manager, item[1])
                & remaining_refs
                & configured_refs
            )
        ]
        if not ready:
            # Keep deterministic reporting when malformed rows declare a
            # dependency cycle; construction will provide the useful error.
            ready = list(remaining)
        ready.sort(
            key=lambda item: (
                item[1] is not None and item[1].backing is not None,
                _is_encrypted_row(item[0]),
                _row_int(item[0], "store_id") or 0,
            )
        )
        for item in ready:
            ordered.append(item[0])
            remaining.remove(item)
    return tuple(ordered)


__all__ = []
