"""
Define maintenance plugin context, telemetry and lifecycle hooks.

Plugins share a database and optional logger. The base supplies inert lifecycle
hooks and accepts every event; subclasses must implement handle_events().
"""

from __future__ import annotations

import abc
import dataclasses

from typing import TYPE_CHECKING, Iterable

from LiuXin_alpha.databases.maintenance.events import MaintenanceEvent

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api import DatabaseAPI


@dataclasses.dataclass(slots=True)
class MaintenancePluginContext:
    """
    Carry the database and optional logger passed to a plugin.

    This mutable slotted dataclass does not open or own either resource.

    Example:
        >>> context = MaintenancePluginContext(db=None)
        >>> context.logger is None
        True
    """
    db: "DatabaseAPI"
    logger: object | None = None


@dataclasses.dataclass(slots=True)
class MaintenancePluginResult:
    """
    Report handled, deferred and error counts for a plugin batch.

    Counts default to zero and are not range-validated or aggregated by this record.

    Example:
        >>> result = MaintenancePluginResult(handled=2)
        >>> result.handled, result.errors
        (2, 0)
    """
    handled: int = 0
    deferred: int = 0
    errors: int = 0


class MaintenancePluginBase(abc.ABC):
    """
    Supply default lifecycle, selection and coalescing behavior for plugins.

    name, priority and enabled_by_default are conventional attributes. The engine sorts
    priority but does not filter enabled_by_default. handle_events() remains abstract.

    Example:
        >>> import inspect
        >>> inspect.isabstract(MaintenancePluginBase)
        True
    """

    name = "maintenance-plugin"
    priority = 0
    enabled_by_default = True

    def startup(self, context: MaintenancePluginContext) -> None:
        """
        Provide a lifecycle hook that ignores the context and performs no work.

        Example:
            >>> worker.startup(context)  # doctest: +SKIP


        :param context: Database and logger context for this invocation.
        :return: None.
        """
        return None

    def shutdown(self, context: MaintenancePluginContext) -> None:
        """
        Provide a lifecycle hook that ignores the context and performs no work.

        Example:
            >>> worker.shutdown(context)  # doctest: +SKIP


        :param context: Database and logger context for this invocation.
        :return: None.
        """
        return None

    def wants_event(self, event: MaintenanceEvent) -> bool:
        """
        Accept every event unless a subclass narrows selection.

        Example:
            >>> worker.wants_event(event)  # doctest: +SKIP


        :param event: Maintenance event to inspect or enqueue.
        :return: True for every supplied event.
        """
        return True

    def coalesce_key(self, event: MaintenanceEvent) -> object | None:
        """
        Keep an event independent of every other event in the batch.

        Example:
            >>> worker.coalesce_key(event)  # doctest: +SKIP


        :param event: Maintenance event to inspect or enqueue.
        :return: None, requesting passthrough rather than coalescing.
        """
        return None

    @abc.abstractmethod
    def handle_events(
        self,
        context: MaintenancePluginContext,
        events: Iterable[MaintenanceEvent],
    ) -> MaintenancePluginResult:
        """
        Require subclasses to implement batch processing.

        The abstract base body raises NotImplementedError if called directly.

        Example:
            >>> worker.handle_events(context, events)  # doctest: +SKIP


        :param context: Database and logger context for this invocation.
        :param events: Iterable of events to process.
        :return: No normal return from the base; implementations should return
            MaintenancePluginResult.
        """
        raise NotImplementedError
