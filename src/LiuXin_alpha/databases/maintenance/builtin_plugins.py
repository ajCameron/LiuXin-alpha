"""
Register creator rename, creator-sort repair and counting plugins.

Selection is creator-specific for the two real jobs. Registration returns fresh
objects; no plugin starts a thread on construction.
"""

from __future__ import annotations

from typing import Iterable

from LiuXin_alpha.databases.maintenance.events import DirtyRowEvent, MaintenanceEvent, RenameRequestEvent
from LiuXin_alpha.databases.maintenance.legacy import ensure_creators_sort
from LiuXin_alpha.databases.maintenance.plugins import (
    MaintenancePluginBase,
    MaintenancePluginContext,
    MaintenancePluginResult,
)


class CreatorSortMaintenancePlugin(MaintenancePluginBase):
    """
    Repair missing sort values for selected creator dirty events.

    The engine controls selection and coalescing before calling the handler.

    Example:
        >>> plugin = CreatorSortMaintenancePlugin()
        >>> plugin.name
        'creator-sort'
    """

    name = "creator-sort"
    priority = 50

    # Todo: re-write for the WEMI stack
    def wants_event(self, event: MaintenanceEvent) -> bool:
        """
        Select DirtyRowEvent instances whose table is exactly creators.

        The kind field is not inspected, so new_dirty_row events also qualify.

        Example:
            >>> CreatorSortMaintenancePlugin().wants_event(DirtyRowEvent("creators", 1))
            True


        :param event: Maintenance event to inspect or enqueue.
        :return: Whether this plugin is interested.
        """
        return isinstance(event, DirtyRowEvent) and event.table == "creators"

    # Todo: Type this better?
    def coalesce_key(self, event: MaintenanceEvent) -> object | None:
        """
        Group dirty events by kind, table and row ID.

        Example:
            >>> worker.coalesce_key(event)  # doctest: +SKIP


        :param event: Maintenance event to inspect or enqueue.
        :return: Three-part key for DirtyRowEvent, or None for other classes.
        """
        if isinstance(event, DirtyRowEvent):
            return event.kind, event.table, event.row_id
        return None

    def handle_events(
        self,
        context: MaintenancePluginContext,
        events: Iterable[MaintenanceEvent],
    ) -> "MaintenancePluginResult":
        """
        Load creator rows for dirty events and repair their missing sort values.

        Ignores non-dirty classes and suppresses row-fetch exceptions. Counts successful
        fetches, even if a host returns a missing-row sentinel. Calls ensure_creators_sort
        once for the collected rows; repair errors propagate to the engine. Table filtering
        is expected from wants_event(), not repeated here.

        Example:
            >>> worker.handle_events(context, events)  # doctest: +SKIP


        :param context: Database and logger context for this invocation.
        :param events: Iterable of events to process.
        :return: Result whose handled count is successful row fetches; deferred/errors stay
            zero.
        """
        rows = []
        handled = 0
        for event in events:
            if not isinstance(event, DirtyRowEvent):
                continue
            try:
                rows.append(context.db.get_row_from_id("creators", event.row_id))
                handled += 1
            except Exception:
                continue
        if rows:
            ensure_creators_sort(rows)
        return MaintenancePluginResult(handled=handled)


class CreatorRenameMaintenancePlugin(MaintenancePluginBase):
    """
    Apply creator rename requests and then repair missing sort values.

    The engine controls selection and coalescing before calling the handler.

    Example:
        >>> plugin = CreatorRenameMaintenancePlugin()
        >>> plugin.name
        'creator-rename'
    """

    name = "creator-rename"
    priority = 60

    def wants_event(self, event: MaintenanceEvent) -> bool:
        """
        Select RenameRequestEvent instances targeting exactly creators.

        Example:
            >>> worker.wants_event(event)  # doctest: +SKIP


        :param event: Maintenance event to inspect or enqueue.
        :return: Whether this plugin is interested.
        """
        return isinstance(event, RenameRequestEvent) and event.table == "creators"

    def coalesce_key(self, event: MaintenanceEvent) -> object | None:
        """
        Group rename requests by table and item ID so the engine keeps the last.

        Example:
            >>> worker.coalesce_key(event)  # doctest: +SKIP


        :param event: Maintenance event to inspect or enqueue.
        :return: Two-part key for RenameRequestEvent, otherwise None.
        """
        if isinstance(event, RenameRequestEvent):
            return event.table, event.item_id
        return None

    def handle_events(
        self,
        context: "MaintenancePluginContext",
        events: Iterable["MaintenanceEvent"],
    ) -> "MaintenancePluginResult":
        """
        Rename fetched creator rows, sync them and ensure sort values.

        Ignores other event classes. Each row fetch, mutation, sync and sort repair is
        inside a broad exception guard; failures are silently skipped and earlier changes
        need not roll back. Counts only requests completing every step.

        Example:
            >>> worker.handle_events(context, events)  # doctest: +SKIP


        :param context: Database and logger context for this invocation.
        :param events: Iterable of events to process.
        :return: Result with the completed rename count and zero other counters.
        """
        handled = 0
        for event in events:
            if not isinstance(event, RenameRequestEvent):
                continue
            try:
                creator_row = context.db.get_row_from_id("creators", row_id=event.item_id)
                creator_row["creator"] = event.value
                creator_row.sync()
                ensure_creators_sort([creator_row])
                handled += 1
            except Exception:
                continue
        return MaintenancePluginResult(handled=handled)


class NullMaintenancePlugin(MaintenancePluginBase):
    """
    Count delivered events without touching the database.

    The engine controls selection and coalescing before calling the handler.

    Example:
        >>> plugin = NullMaintenancePlugin()
        >>> plugin.name
        'null-maintenance'
    """

    name = "null-maintenance"
    priority = -100

    def handle_events(
        self,
        context: "MaintenancePluginContext",
        events: Iterable["MaintenanceEvent"],
    ) -> "MaintenancePluginResult":
        """
        Consume the event iterable and count its elements without database work.

        Example:
            >>> plugin = NullMaintenancePlugin()
            >>> plugin.handle_events(None, [DirtyRowEvent("works", 1)]).handled
            1


        :param context: Database and logger context for this invocation.
        :param events: Iterable of events to process.
        :return: Result with handled equal to the number of consumed events.
        """
        count = sum(1 for _ in events)
        return MaintenancePluginResult(handled=count)


# Explicit builtin registration first. This is safer than trying to hook the
# heavier calibre-style customize plugin machinery into an internal DB service.
def get_builtin_maintenance_plugins() -> list["MaintenancePluginBase"]:
    """
    Construct the standard creator-rename, creator-sort and null plugins.

    Returns fresh instances in that order; the engine subsequently sorts by priority.

    Example:
        >>> [p.name for p in get_builtin_maintenance_plugins()]
        ['creator-rename', 'creator-sort', 'null-maintenance']


    :return: List of three plugin instances.
    """
    return [
        CreatorRenameMaintenancePlugin(),
        CreatorSortMaintenancePlugin(),
        NullMaintenancePlugin(),
    ]
