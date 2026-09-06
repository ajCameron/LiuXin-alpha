"""Core-owned maintenance operations and wire translation."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any, cast

from LiuXin_alpha.core.errors import CoreDispatchError
from LiuXin_alpha.core.program_services.payloads import (
    _callable,
    _optional_int,
    _payload,
    _required_int,
    _required_text,
    plain,
)

if TYPE_CHECKING:
    from LiuXin_alpha.core.commands import CoreCommand
    from LiuXin_alpha.core.queries import CoreQuery
    from LiuXin_alpha.core.runtime import CoreRuntime


def maintenance_status(
    runtime: CoreRuntime,
    query: CoreQuery,
) -> dict[str, Any]:
    del query
    maintenance = runtime.services.maintenance
    iter_plugins = getattr(maintenance, "iter_plugins", None)
    plugins = (
        list(cast(Iterable[Any], iter_plugins())) if callable(iter_plugins) else []
    )
    db = runtime.database
    dirty = getattr(db, "get_dirtied_count", None)
    return {
        "service": type(maintenance).__name__,
        "plugins": [
            {
                "name": str(getattr(plugin, "name", None) or type(plugin).__name__),
                "type": type(plugin).__name__,
            }
            for plugin in plugins
        ],
        "dirty_count": (int(cast(Any, dirty())) if callable(dirty) else None),
        "main_queue_size": (
            maintenance.main_table_dirtied_queue.qsize()
            if hasattr(maintenance, "main_table_dirtied_queue")
            else None
        ),
        "interlink_queue_size": (
            maintenance.interlink_dirtied_queue.qsize()
            if hasattr(maintenance, "interlink_dirtied_queue")
            else None
        ),
    }


def maintenance_duplicates_find(
    runtime: CoreRuntime,
    query: CoreQuery,
) -> dict[str, Any]:
    payload = _payload(query)
    from LiuXin_alpha.databases.maintenance.legacy import find_duplicates

    table = _required_text(payload, "table")
    column = _required_text(payload, "column")
    comparison = str(payload.get("comparison") or "nocase")
    result = find_duplicates(
        runtime.database,
        table,
        column,
        comparison=comparison,
    )
    return {
        "table": table,
        "column": column,
        "comparison": comparison,
        "duplicates": plain(result),
    }


def maintenance_run(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    payload = _payload(command)
    max_events = _optional_int(
        payload,
        "max_events",
        default=128,
        minimum=1,
    )
    assert max_events is not None
    result = _callable(
        runtime.services.maintenance,
        "run_once",
        area="maintenance",
    )(max_events=max_events)
    return {"max_events": max_events, "plugins": plain(result)}


def maintenance_clean(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    payload = _payload(command)
    table = _required_text(payload, "table")
    raw_ids = payload.get("row_ids")
    if not isinstance(raw_ids, Sequence) or isinstance(raw_ids, (str, bytes)):
        raise CoreDispatchError("`row_ids` must be an array.")
    row_ids = [int(cast(Any, item)) for item in raw_ids]
    _callable(
        runtime.services.maintenance,
        "clean",
        area="maintenance",
    )(table, row_ids)
    return runtime.services.reconcile(
        {"cleaned": True, "table": table, "row_ids": row_ids}
    )


def maintenance_merge(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    payload = _payload(command)
    table = _required_text(payload, "table")
    retained_id = _required_int(payload, "retained_id")
    merged_id = _required_int(payload, "merged_id")
    if retained_id == merged_id:
        raise CoreDispatchError("`retained_id` and `merged_id` must differ.")
    _callable(
        runtime.services.maintenance,
        "merge",
        area="maintenance",
    )(table, retained_id, merged_id)
    return runtime.services.reconcile(
        {
            "merged": True,
            "table": table,
            "retained_id": retained_id,
            "merged_id": merged_id,
        }
    )
