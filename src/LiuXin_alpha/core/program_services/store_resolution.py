"""Core-owned store resolution operations and wire translation."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from uuid import UUID

from LiuXin_alpha.core.errors import CoreDispatchError
from LiuXin_alpha.storage.store_spec_utils import store_configuration_from_row

if TYPE_CHECKING:
    from LiuXin_alpha.core.runtime import CoreRuntime


def _store(runtime: CoreRuntime, reference: Any) -> Any:
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
    """Search UUID, numeric identity, then name without constructing a Store."""
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
    """Resolve live Stores first, then validate durable-only configurations."""
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
