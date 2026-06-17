"""
Builtin maintenance plugins.

These are intentionally small and conservative.
The aim is to give the new maintenance engine one clearly useful builtin plugin without canonising all the legacy
``maintenance_bot.py`` into the new contract immediately.
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
    name = "creator-sort"
    priority = 50

    # Todo: re-write for the WEMI stack
    def wants_event(self, event: MaintenanceEvent) -> bool:
        """
        Check all incoming maintenance events.

        :param event:
        :return:
        """
        return isinstance(event, DirtyRowEvent) and event.table == "creators"

    # Todo: Type this better?
    def coalesce_key(self, event: MaintenanceEvent) -> object | None:
        if isinstance(event, DirtyRowEvent):
            return event.kind, event.table, event.row_id
        return None

    def handle_events(
        self,
        context: MaintenancePluginContext,
        events: Iterable[MaintenanceEvent],
    ) -> "MaintenancePluginResult":
        """
        Handle maintenance events.

        :param context:
        :param events:
        :return:
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
    Compatibility plugin for the old ``rename_item()`` API.

    This keeps the old creators-only behaviour, but routes it through the new
    plugin engine so the engine owns the unit of work rather than the thread
    wrapper owning bespoke methods forever.
    """

    name = "creator-rename"
    priority = 60

    def wants_event(self, event: MaintenanceEvent) -> bool:
        """
        Check all incoming maintenance events to see if we're going to respond to them.

        :param event:
        :return:
        """
        return isinstance(event, RenameRequestEvent) and event.table == "creators"

    def coalesce_key(self, event: MaintenanceEvent) -> object | None:
        if isinstance(event, RenameRequestEvent):
            return event.table, event.item_id
        return None

    def handle_events(
        self,
        context: "MaintenancePluginContext",
        events: Iterable["MaintenanceEvent"],
    ) -> "MaintenancePluginResult":
        """
        Allow the plugin to process events.

        :param context:
        :param events:
        :return:
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
    Safe default so the engine can be introduced before every job is ported.
    """

    name = "null-maintenance"
    priority = -100

    def handle_events(
        self,
        context: "MaintenancePluginContext",
        events: Iterable["MaintenanceEvent"],
    ) -> "MaintenancePluginResult":
        """
        Handle maintenance events.

        :param context:
        :param events:
        :return:
        """
        count = sum(1 for _ in events)
        return MaintenancePluginResult(handled=count)


# Explicit builtin registration first. This is safer than trying to hook the
# heavier calibre-style customize plugin machinery into an internal DB service.
def get_builtin_maintenance_plugins() -> list["MaintenancePluginBase"]:
    return [
        CreatorRenameMaintenancePlugin(),
        CreatorSortMaintenancePlugin(),
        NullMaintenancePlugin(),
    ]
