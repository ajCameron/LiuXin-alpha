"""
Adapt Core maintenance inspection, event processing, cleanup, and merge requests to the composed maintenance service.

Missing callable operations use capability_unavailable errors. Subsystems own
cleanup/merge semantics; post-mutation read-side reconciliation does not wrap
those operations in an adapter transaction. Status snapshots and duplicate
discovery do not assert that a maintenance run has completed.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
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

    An attached durable runtime instead reports schema/configuration and pending
    obligations, age, attempts, errors and job references. This branch neither
    creates a jobs ledger nor resolves/starts the legacy maintenance service.

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
    driver = getattr(runtime.database, "driver", None)
    durable = vars(driver).get("_maintenance_runtime") if driver is not None else None
    if durable is not None:
        return {"service": type(durable).__name__, **durable.status()}
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
    Inspect duplicate candidates through Catalog's scoped, bounded reader.

    Comparison defaults to identity; nocase and exact are also supported.
    max_rows bounds the complete scan, and optional where equality filters
    restrict its scope. Oversized scans fail without returning partial groups.
    No repair, maintenance service initialization or worker startup occurs.

    Example:
        >>> duplicates = maintenance_duplicates_find(runtime, query)  # doctest: +SKIP


    :param runtime: Runtime whose database is inspected by find_duplicates.
    :param query: Required table/column and optional comparison, max_rows and where.
    :return: Comparison policy, scanned count and scoped duplicate candidate groups.
    """
    payload = _payload(query)
    from LiuXin_alpha.catalog.duplicates import (
        DEFAULT_MAX_ROWS,
        DuplicateScanLimit,
        find_duplicates,
    )
    from LiuXin_alpha.errors import InputIntegrityError

    table = _required_text(payload, "table")
    column = _required_text(payload, "column")
    try:
        return find_duplicates(
            runtime.database,
            table,
            column,
            comparison=payload.get("comparison", "identity"),
            max_rows=payload.get("max_rows", DEFAULT_MAX_ROWS),
            where=payload.get("where"),
        )
    except DuplicateScanLimit as error:
        raise CoreDispatchError(str(error), code="scan_limit_exceeded") from error
    except (ValueError, TypeError, KeyError, InputIntegrityError) as error:
        raise CoreDispatchError(str(error), code="invalid_payload") from error


def maintenance_run(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    """
    Run one maintenance-service pass with a positive event budget and project its plugin results.

    An attached durable runtime accepts action=install/drain/retry/pause/resume/
    prune/prune-jobs/start/rebuild/upgrade. Upgrade defaults to an inspected migration preview.
    Rebuild checkpoints bounded Work projection pages without
    starting a worker; restart explicitly requests a new scan. Install is explicit
    schema/configuration work; drain uses bounded
    jobs passes, while start polls only for the lifetime of a long-lived Core.
    Retry and enable controls require plugin_id. Legacy injected services retain
    their existing run_once route when no durable runtime is attached.

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
    driver = getattr(runtime.database, "driver", None)
    durable = vars(driver).get("_maintenance_runtime") if driver is not None else None
    if durable is not None:
        action = str(payload.get("action", "drain"))
        if action == "install":
            return {"action": action, **durable.install()}
        if action == "upgrade":
            return {"action": action, **_upgrade_plugin(durable, payload)}
        if action == "rebuild":
            return {
                "action": action,
                **_rebuild_work_pages(runtime, payload, max_events),
            }
        if action == "retry":
            return {
                "action": action,
                "reset": durable.retry(_required_text(payload, "plugin_id")),
            }
        if action in {"pause", "resume"}:
            durable.set_enabled(
                _required_text(payload, "plugin_id"), action == "resume"
            )
            return {"action": action, "plugin_id": payload["plugin_id"]}
        if action in {"prune", "prune-jobs"}:
            return {
                "action": action,
                **_prune_history(runtime, durable, action, payload, max_events),
            }
        if action == "start":
            return {
                "action": action,
                **runtime.durable_jobs.start_maintenance(limit=max_events),
            }
        if action == "drain":
            return {"action": action, **_drain_jobs(runtime, payload, max_events)}
        raise CoreDispatchError("Unknown maintenance action")
    result = _callable(
        runtime.services.maintenance,
        "run_once",
        area="maintenance",
    )(max_events=max_events)
    return {"max_events": max_events, "plugins": plain(result)}


def _prune_history(
    runtime: CoreRuntime,
    durable: Any,
    action: str,
    payload: Mapping[str, Any],
    limit: int,
) -> dict[str, Any]:
    """Keep journal reclamation and age-based jobs retirement with their owners."""
    if action == "prune":
        return {"removed": durable.prune_completed(limit=limit)}
    return runtime.durable_jobs.prune_maintenance_history(
        older_than_days=payload.get("older_than_days", 7),
        limit=limit,
        before_run_id=payload.get("before_run_id"),
    )


def _upgrade_plugin(durable: Any, payload: Mapping[str, Any]) -> dict[str, Any]:
    """Adapt an explicit upgrade request and report malformed fields consistently."""
    try:
        return durable.upgrade(
            _required_text(payload, "plugin_id"),
            expected_version=_required_text(payload, "expected_version"),
            expected_definition=payload.get("expected_definition"),
            dry_run=payload.get("dry_run", True),
        )
    except (ValueError, TypeError) as error:
        raise CoreDispatchError(str(error), code="invalid_payload") from error


def _drain_jobs(
    runtime: CoreRuntime, payload: Mapping[str, Any], max_events: int
) -> dict[str, Any]:
    """Validate the concrete job-pass budget before resolving a worker or ledger."""
    batches = _optional_int(payload, "max_batches", default=128, minimum=1)
    if batches is None:
        raise CoreDispatchError("max_batches cannot be null", code="invalid_payload")
    return runtime.durable_jobs.drain_maintenance(max_batches=batches, limit=max_events)


def _rebuild_work_pages(
    runtime: CoreRuntime, payload: Mapping[str, Any], max_events: int
) -> dict[str, Any]:
    """Schedule bounded Catalog rebuild pages, restarting only the first page."""
    from LiuXin_alpha.catalog.maintenance import rebuild_work_projections

    batches = payload.get("max_batches", 1)
    if type(batches) is not int or not 1 <= batches <= 128:
        raise CoreDispatchError(
            "Rebuild max_batches must be between 1 and 128",
            code="invalid_payload",
        )
    restart = payload.get("restart", False)
    try:
        result = rebuild_work_projections(
            runtime.database, limit=max_events, restart=restart
        )
        scheduled = result["scheduled_this_page"]
        for _ in range(1, batches):
            if result["scheduling_complete"]:
                break
            result = rebuild_work_projections(
                runtime.database,
                limit=max_events,
                restart=False,
            )
            scheduled += result["scheduled_this_page"]
    except (ValueError, TypeError) as error:
        raise CoreDispatchError(str(error), code="invalid_payload") from error
    return {**result, "scheduled_this_call": scheduled}


def maintenance_clean(
    runtime: CoreRuntime,
    command: CoreCommand,
) -> dict[str, Any]:
    """
    Clean explicitly selected rows through the maintenance service, then reconcile read-side state.

    The built-in selective cleanup is unavailable; its NotImplementedError is
    exposed as capability_unavailable before any reconciliation. Injected services
    can supply an actual implementation.

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
    try:
        _callable(
            runtime.services.maintenance,
            "clean",
            area="maintenance",
        )(table, row_ids)
    except NotImplementedError as error:
        raise CoreDispatchError(
            str(error),
            code="capability_unavailable",
            details={"area": "maintenance", "operation": "clean"},
        ) from error
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
