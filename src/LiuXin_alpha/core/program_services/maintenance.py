"""
Adapt Core maintenance inspection, event processing, cleanup, and merge requests to the composed maintenance service.

Missing callable operations use capability_unavailable errors. Subsystems own
cleanup/merge semantics; post-mutation read-side reconciliation does not wrap
those operations in an adapter transaction. Status snapshots and duplicate
discovery do not assert that a maintenance run has completed.
"""

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
    """
    Describe the maintenance service's optional plugins, database dirty count, and queue sizes.

    Missing callable plugin enumeration yields no plugins; a missing callable dirty
    counter or absent queue attribute yields None. Errors from present methods,
    attributes, integer conversion, and qsize propagate. Queue counts are separate
    observations, not a synchronized snapshot. Resolving the service may initialize it.

    Example:
        >>> status = maintenance_status(runtime, query)  # doctest: +SKIP


    :param runtime: Runtime supplying the maintenance service and database dirty-count capability.
    :param query: Ignored query envelope; status accepts no payload options here.
    :return: Service type, plugin name/type records, dirty_count, and optional queue sizes.
    """
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
    """
    Delegate duplicate-value discovery for one table/column to the legacy maintenance helper.

    Falsey comparison values select nocase; other values are stringified without
    stripping or validation here. The helper owns comparison semantics and result
    grouping. This handler neither merges duplicates nor adds paging.

    Example:
        >>> duplicates = maintenance_duplicates_find(runtime, query)  # doctest: +SKIP


    :param runtime: Runtime whose database is inspected by find_duplicates.
    :param query: Query with required table/column text and optional comparison mode.
    :return: Selected table, column, comparison, and plain-projected duplicate groups.
    """
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
    """
    Run one maintenance-service pass with a positive event budget and project its plugin results.

    max_events defaults to 128 with no adapter upper cap; explicit None reaches
    an assertion rather than choosing the default. The service owns event-budget
    interpretation and plugin side effects; no additional reconciliation runs here.

    Example:
        >>> receipt = maintenance_run(runtime, command)  # doctest: +SKIP


    :param runtime: Runtime whose maintenance service provides run_once.
    :param command: Command with optional positive integer-convertible max_events, excluding bool.
    :return: Requested event budget and plain-projected run_once result under plugins.
    """
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
    """
    Clean explicitly selected rows through the maintenance service, then reconcile read-side state.

    row_ids must be a non-str/non-bytes Sequence. Plain int conversion accepts
    bool and truncatable numeric values, preserving duplicates and order. The
    clean result is ignored: cleaned=True indicates returned delegation, not a
    removed-row count. Reconciliation can fail after cleanup has run.

    Example:
        >>> receipt = maintenance_clean(runtime, command)  # doctest: +SKIP


    :param runtime: Runtime supplying maintenance.clean and service reconciliation.
    :param command: Command with required table and row_ids array; no confirmation field is checked here.
    :return: Reconciled cleaned=True receipt with table and converted row_ids.
    :raises CoreDispatchError: If the request shape or clean capability is unavailable.
    """
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
    """
    Merge two distinct row IDs through the maintenance service and reconcile after delegation.

    Both IDs use the non-boolean required-integer helper. Equality after conversion
    is rejected before capability lookup. The service owns retention, conflicts,
    and transactional behavior; merged=True is set after its return without readback.

    Example:
        >>> receipt = maintenance_merge(runtime, command)  # doctest: +SKIP


    :param runtime: Runtime providing maintenance.merge and post-write reconciliation.
    :param command: Command with table, retained_id, and a distinct merged_id.
    :return: Reconciled table/ID receipt with merged=True; reconciliation may fail after the merge.
    :raises CoreDispatchError: If request validation fails, IDs are equal, or merge is unavailable.
    """
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
