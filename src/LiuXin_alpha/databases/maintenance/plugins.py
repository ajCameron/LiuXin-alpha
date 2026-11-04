"""
Define maintenance plugin context, telemetry and lifecycle hooks.

Plugins share a database and optional logger. The base supplies inert lifecycle
hooks and accepts every event; subclasses must implement handle_events().
Mode declarations distinguish lazy work from required transactional prompt work.
MaintenanceRuntime supplies transaction-bound prompt and durable lazy execution.
The historical queue engine remains an explicit compatibility route only.
"""

from __future__ import annotations

import abc
import dataclasses
from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING

from LiuXin_alpha.databases.db_types.maintenance_types import MaintenanceMode
from LiuXin_alpha.databases.maintenance.events import MaintenanceEvent

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api import DatabaseAPI


@dataclasses.dataclass(slots=True)
class MaintenancePluginContext:
    """
    Carry the database and optional logger passed to a plugin.

    Durable handlers receive connection, plugin_id and mode; db is None to prevent
    accidental use of a facade that opens another write connection. The borrowed
    connection permits row SQL only and expires when the handler returns. The old
    queue adapter still supplies db/logger without a transaction guarantee.

    Example:
        >>> context = MaintenancePluginContext(db=None)
        >>> context.logger is None
        True
    """

    db: DatabaseAPI | None
    logger: object | None = None
    connection: object | None = None
    plugin_id: str | None = None
    mode: MaintenanceMode = MaintenanceMode.LAZY


@dataclasses.dataclass(slots=True)
class MaintenancePluginResult:
    """
    Report handled, deferred and error counts for a plugin batch.

    Counts default to zero and are not range-validated or aggregated by this record.
    The durable runtime requires nonnegative integers, no deferred/errors, and
    handled equal to the selected/coalesced event count. Anything else rolls back
    the batch; lazy failures retain their obligation and retry evidence.

    Example:
        >>> result = MaintenancePluginResult(handled=2)
        >>> result.handled, result.errors
        (2, 0)
    """

    handled: int = 0
    deferred: int = 0
    errors: int = 0


@dataclasses.dataclass(frozen=True, slots=True)
class MaintenancePluginUpgrade:
    """Authorize one old version's explicit migration into this implementation.

    replay_pending asserts that the new handler understands the captured old
    events. Otherwise the old implementation must drain them first. A migration
    callback receives a borrowed database-only context and the old declaration.
    It must execute synchronously and return None; deferred work is rejected.
    Its writes share the declaration/receipt transaction; external effects are
    outside this contract. Projections of this plugin and its dependents are
    invalidated before the callback and must be rebuilt under the new policy.
    """

    from_version: str
    replay_pending: bool = False
    migrate: Callable | None = None


class MaintenancePluginBase(abc.ABC):
    """
    Supply default lifecycle, selection and coalescing behavior for plugins.

    Declare a stable name/version, physical tables and optional dependencies.
    Installation persists the declaration and enabled_by_default. Dependencies
    precede priority ordering. mode defaults to LAZY; PROMPT runs in the source
    write unit. Implement database-only effects through context.connection.

    Example:
        >>> import inspect
        >>> inspect.isabstract(MaintenancePluginBase)
        True
    """

    name = "maintenance-plugin"
    version = "1"
    tables: tuple[str, ...] = ()
    dependencies: tuple[str, ...] = ()
    mode: MaintenanceMode = MaintenanceMode.LAZY
    priority = 0
    enabled_by_default = True
    retry_limit = 5
    retry_backoff_s = 1.0
    upgrades: tuple[MaintenancePluginUpgrade, ...] = ()

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

        Durable execution requires an actual bool; False deliberately skips an
        event, while None and other truthy/falsey values fail the whole invocation.

        Example:
            >>> worker.wants_event(event)  # doctest: +SKIP


        :param event: Maintenance event to inspect or enqueue.
        :return: True for every supplied event.
        """
        return True

    def coalesce_key(self, event: MaintenanceEvent) -> object | None:
        """
        Keep an event independent of every other event in the batch.

        Overrides must return a hashable key or None synchronously. Deferred
        coroutine/generator results fail rather than silently acknowledging work.

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
