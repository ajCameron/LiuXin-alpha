"""
Provide an explicit counting plugin for the transient callback adapter.

Domain maintenance plugins belong to Catalog. There are no implicit database
plugins, and constructing the counting plugin starts no worker.
"""

from __future__ import annotations

from collections.abc import Iterable

from LiuXin_alpha.databases.maintenance.events import MaintenanceEvent
from LiuXin_alpha.databases.maintenance.plugins import (
    MaintenancePluginBase,
    MaintenancePluginContext,
    MaintenancePluginResult,
)

# Todo: Thinking more about this system, it makes sense that there are a number of classes of plugin -
# prompt - plugins which MUST run after any update - not sure what these could possibly be
# eventual - the plugin will run - in time. It's not that important.
# This should be implemented, but could be made cleaer


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
        context: MaintenancePluginContext,
        events: Iterable[MaintenanceEvent],
    ) -> MaintenancePluginResult:
        """
        Consume the event iterable and count its elements without database work.

        Example:
            >>> from LiuXin_alpha.databases.maintenance.events import DirtyRowEvent
            >>> plugin = NullMaintenancePlugin()
            >>> plugin.handle_events(None, [DirtyRowEvent("works", 1)]).handled
            1


        :param context: Database and logger context for this invocation.
        :param events: Iterable of events to process.
        :return: Result with handled equal to the number of consumed events.
        """
        count = sum(1 for _ in events)
        return MaintenancePluginResult(handled=count)


# Catalog owns live WEMI plugins; the queue adapter has no implicit handlers.
def get_builtin_maintenance_plugins() -> list[MaintenancePluginBase]:
    """
    Return no implicit legacy plugins for WEMI catalogues.

    Catalog owns the transactional Work revision and durable summary
    implementations. The obsolete creator adapters have been removed.

    Example:
        >>> [p.name for p in get_builtin_maintenance_plugins()]
        []


    :return: An empty list; transient handlers must be supplied explicitly.
    """
    return []
