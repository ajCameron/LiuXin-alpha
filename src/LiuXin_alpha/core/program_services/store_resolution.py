"""
Resolve live Store facades and, on selected dispatch failures, durable Store configurations.

UUID, numeric database identity, and exact name follow different fallback paths.
Not every live-manager error means absence: durable fallback catches only
CoreDispatchError, so StoreUnavailable and StoreConfigurationNotFound can remain
visible before a durable search is attempted. No backend is opened by row conversion.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import UUID

from LiuXin_alpha.core.errors import CoreDispatchError
from LiuXin_alpha.storage.store_spec_utils import store_configuration_from_row

if TYPE_CHECKING:
    from LiuXin_alpha.core.runtime import CoreRuntime


def _store(runtime: CoreRuntime, reference: Any) -> Any:
    """
    Resolve a live Store by UUID first, then numeric database row, then a unique exact live name.

    Only TypeError/ValueError from UUID lookup trigger numeric fallback. Numeric
    conversion/row lookup similarly suppress those two exceptions, but subsequent
    row conversion and live Store lookup errors propagate. Booleans skip the numeric
    path; other int-convertible references may be coerced. Names are not stripped.

    Example:
        >>> from types import SimpleNamespace
        >>> from unittest.mock import Mock
        >>> store = SimpleNamespace(configuration=SimpleNamespace(store_name="archive"))
        >>> storage = Mock()
        >>> storage.iter_stores.return_value = [store]
        >>> runtime = SimpleNamespace(library=SimpleNamespace(storage=storage), database=Mock())
        >>> _store(runtime, "archive") is store
        True


    :param runtime: Runtime providing library.storage and database row lookup for numeric references.
    :param reference: UUID-like value, int-convertible row identity other than bool, or exact Store name.
    :return: Live Store facade found by the first successful resolution path, not a durable-only configuration.
    :raises CoreDispatchError: If name fallback finds zero or multiple matches, both reported as Unknown Store.
    """
    storage = runtime.library.storage
    try:
        return storage.get_store(UUID(str(reference)))
    except (TypeError, ValueError):
        pass
    if not isinstance(reference, bool):
        try:
            row = runtime.database.get_row_from_id("stores", int(reference))
        except (TypeError, ValueError):
            row = None
        if row is not None:
            configuration = store_configuration_from_row(
                row,
                fallback_store_id=int(reference),
            )
            return storage.get_store(configuration.store_uuid)
    matches = [
        store
        for store in storage.iter_stores()
        if store.configuration.store_name == str(reference)
    ]
    if len(matches) == 1:
        return matches[0]
    raise CoreDispatchError(f"Unknown Store: {reference!r}.")


def _durable_store_rows(
    runtime: CoreRuntime,
    reference: Any,
) -> list[Any]:
    """
    Read durable rows by UUID, numeric identity, then exact name until one path produces rows.

    UUID searches may return multiple rows and stop fallback. Boolean references
    skip numeric lookup; numeric conversion errors are ignored, but database lookup
    errors propagate. Rows are neither converted nor deduplicated in this helper.

    Example:
        >>> from types import SimpleNamespace
        >>> from unittest.mock import Mock
        >>> database = Mock()
        >>> database.search.return_value = [{"store_name": "archive"}]
        >>> _durable_store_rows(SimpleNamespace(database=database), "archive")
        [{'store_name': 'archive'}]


    :param runtime: Runtime whose database supports search and get_row_from_id on stores.
    :param reference: UUID-like value, non-boolean int-convertible row identity, or exact name after text conversion.
    :return: List of raw matching rows in database result order, possibly empty or ambiguous.
    """
    rows: list[Any] = []
    try:
        store_uuid = UUID(str(reference))
    except (TypeError, ValueError):
        store_uuid = None
    if store_uuid is not None:
        rows = list(
            runtime.database.search(
                "stores",
                "store_uuid",
                str(store_uuid),
            )
            or ()
        )
    if not rows and not isinstance(reference, bool):
        try:
            store_id = int(reference)
        except (TypeError, ValueError):
            store_id = None
        if store_id is not None:
            row = runtime.database.get_row_from_id("stores", store_id)
            if row is not None:
                rows = [row]
    if not rows:
        rows = list(
            runtime.database.search(
                "stores",
                "store_name",
                str(reference),
            )
            or ()
        )
    return rows


def _store_configuration(runtime: CoreRuntime, reference: Any) -> Any:
    """
    Return a live Store's configuration, falling back to exactly one durable row only after CoreDispatchError.

    Other live-resolution failures propagate instead of triggering durable lookup.
    The fallback derives an optional integer row-ID hint, then wraps row-conversion
    Exceptions with a chained invalid-configuration dispatch error. Successful live
    configurations are returned directly without this extra conversion/validation.

    Example:
        >>> configuration = _store_configuration(runtime, "archive")  # doctest: +SKIP


    :param runtime: Runtime exposing live Store lookup and durable stores-table search.
    :param reference: Store reference accepted by the live/durable resolution paths.
    :return: Existing live configuration or newly converted durable row configuration; no backend is constructed.
    :raises CoreDispatchError: If durable lookup has no unique row or conversion of that row fails.
    """
    try:
        return _store(runtime, reference).configuration
    except CoreDispatchError:
        pass
    rows = _durable_store_rows(runtime, reference)
    if len(rows) > 1:
        raise CoreDispatchError(f"Ambiguous durable Store reference: {reference!r}.")
    if not rows:
        raise CoreDispatchError(f"Unknown Store: {reference!r}.")
    row = rows[0]
    try:
        fallback_store_id = int(row["store_id"])
    except (KeyError, TypeError, ValueError):
        fallback_store_id = None
    try:
        return store_configuration_from_row(
            row,
            fallback_store_id=fallback_store_id,
        )
    except Exception as exc:
        raise CoreDispatchError(
            f"Durable Store {reference!r} has an invalid configuration: {str(exc) or type(exc).__name__}"
        ) from exc
