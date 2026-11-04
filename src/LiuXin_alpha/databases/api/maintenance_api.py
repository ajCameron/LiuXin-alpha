"""
Separate maintenance facade, callback, plugin and service contracts.

The facade preserves legacy cleanup/merge entry points, callbacks accept driver
notifications, and services dispatch plugin work. Most methods are abstract.
DatabaseMaintainerAPI stores its database; MaintenancePluginAPI supplies no-op lifecycle
hooks, an all-events filter and a no-coalescing default. Concrete engines and plugins
also satisfy these surfaces by duck typing rather than inheriting every ABC.
"""

from __future__ import annotations

import abc
from collections.abc import Hashable, Iterable
from queue import Queue
from typing import TYPE_CHECKING, Protocol

from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI
from LiuXin_alpha.databases.db_types.maintenance_types import MaintenanceMode

if TYPE_CHECKING:
    from LiuXin_alpha.databases.maintenance.events import MaintenanceEvent
    from LiuXin_alpha.databases.maintenance.plugins import (
        MaintenancePluginContext,
        MaintenancePluginResult,
    )


class DatabaseMaintainerAPI(abc.ABC):
    """
    Define the database-bound compatibility facade for notifications and legacy
    maintenance.

    The base constructor only stores db. The concrete Maintainer forwards events to
    MaintenanceEngine and retains synchronous cleanup/merge helpers; background service
    lifecycle is exposed through separate contracts below. Selective cleanup is
    unavailable. Physical interlink merges share a transaction and reject conflicting
    duplicate payloads; they do not combine scalar metadata or intralinks.

    Example:
        maintainer: DatabaseMaintainerAPI = db.maintenance
        maintainer.dirty_record("works", work_id)
    """

    def __init__(self, db: DatabaseAPI) -> None:
        """
        Store the database used by a concrete maintenance facade.

        Example:
            super().__init__(db)  # In a concrete facade constructor.


        :param db: Database object retained as self.db without copying or validation.
        :return: None; does not create queues or start a thread in this base
            implementation.
        """

        self.db = db

    @abc.abstractmethod
    def _do_merge_one_table(
        self,
        src_table: str,
        dst_table: str,
        link_table: str,
        item_1_id: int,
        item_2_id: int,
    ) -> None:
        """
        Repoint one relation’s links from the removed item to the surviving item.

        Concrete Maintainer uses a physical link specification and shared macro
        transaction. Only proven identical duplicate payloads are deleted; other
        conflicts or write errors propagate and roll back the unit. This helper
        does not delete item_2 itself.

        Example:
            maintainer._do_merge_one_table("works", "agents", link_table, surviving_id,
            removed_id)


        :param src_table: Other endpoint table whose links are being examined.
        :param dst_table: Table containing both items being merged.
        :param link_table: Interlink table connecting the source and destination tables.
        :param item_1_id: Destination-table item ID that survives.
        :param item_2_id: Destination-table item ID whose links are moved.
        :return: None; the abstract method supplies no operational default.
        """

        ...

    @abc.abstractmethod
    def clean(self, table: str, item_ids: Iterable[int]) -> None:
        """
        Expose the unfinished legacy unused-row cleanup entry point.

        Concrete Maintainer forwards to legacy.clean. Supplying item_ids raises
        NotImplementedError before reading or writing.
        Passing None, despite the annotation, only returns quietly for books/titles;
        other recognized main tables reach NotImplementedError after relation
        inspection. No general cleanup operation is implemented.

        Example:
            maintainer.clean("tags", candidate_ids)  # Currently fails: legacy selective
            cleanup is unfinished.


        :param table: Main table proposed for cleanup.
        :param item_ids: Candidate item IDs; the current legacy implementation cannot
            handle a supplied iterable.
        :return: None; the abstract method supplies no operational default.
        :raises NotImplementedError: Selective or unrestricted cleanup is unavailable.
        :raises KeyError: Unrestricted cleanup names an unknown main table.
        """

        ...

    @abc.abstractmethod
    def dirty_interlink_record(
        self, update_type: str, table1: str, table2: str, table1_id: int, table2_id: int
    ) -> None:
        """
        Enqueue a notification describing a changed relation between two rows.

        The concrete sink queues a five-item tuple for later DirtyInterlinkEvent
        conversion. It neither writes the relation nor verifies that either row exists.

        Example:
            sink.dirty_interlink_record("update", "works", "agents", work_id, agent_id)


        :param update_type: Update label preserved as text without validating a fixed
            vocabulary.
        :param table1: First endpoint table name.
        :param table2: Second endpoint table name.
        :param table1_id: First endpoint ID converted to int.
        :param table2_id: Second endpoint ID converted to int.
        :return: None; the abstract method supplies no operational default.
        """

        ...

    @abc.abstractmethod
    def dirty_record(self, table: str, row_id: int) -> None:
        """
        Enqueue a notification that an existing row needs maintenance.

        Concrete dispatch places a (table, row_id) tuple on the engine’s main-row queue
        without checking table eligibility or changing the database. This queue is
        separate from Database.dirty_records_queue.

        Example:
            sink.dirty_record("works", work_id)


        :param table: Table name converted to text by the concrete callback sink.
        :param row_id: Row ID converted to int by the concrete callback sink.
        :return: None; the abstract method supplies no operational default.
        """

        ...

    @abc.abstractmethod
    def merge(self, table: str, item_1_id: int, item_2_id: int) -> None:
        """
        Move a removed item’s interlinks to a survivor, then delete the removed row.

        Concrete Maintainer processes available interlinks with every other main table,
        then deletes item_2. It does not merge the two rows’ metadata or explicitly
        process same-table intralinks. One serialized transaction encloses every
        transfer and deletion. IDs must be distinct, positive and present. Only
        proven identical duplicate payloads are removed; conflicting payloads or
        any write failure roll back all transfers and preserve the source row.

        Example:
            maintainer.merge("tags", surviving_tag_id, removed_tag_id)


        :param table: Table containing both item IDs.
        :param item_1_id: Surviving row ID.
        :param item_2_id: Row ID to remove after relation processing.
        :return: None; the abstract method supplies no operational default.
        """

        ...

    @abc.abstractmethod
    def new_dirty_record(self, table: str, row_id: int) -> None:
        """
        Enqueue a typed new-row notification for maintenance plugins.

        The concrete sink adds a DirtyRowEvent with kind=new_dirty_row to the manual
        event queue, not the legacy main-row queue. No row is created by this
        notification.

        Example:
            sink.new_dirty_record("works", work_id)


        :param table: Table name converted to text.
        :param row_id: New row ID converted to int.
        :return: None; the abstract method supplies no operational default.
        """

        ...


class MaintenanceCallbackSinkAPI(Protocol):
    """
    Specify the lightweight dirty-event callbacks invoked by driver hooks.

    Callbacks enqueue maintenance notifications; they do not synchronously repair rows.
    The concrete sink uses legacy row/interlink queues plus a manual event queue for new
    rows. This contract does not register SQL functions or guarantee their arity;
    registration belongs to the driver.

    Example:
        sink: MaintenanceCallbackSinkAPI = callback_sink
        sink.dirty_record("works", work_id)
    """

    @abc.abstractmethod
    def dirty_record(self, table: str, row_id: int) -> None:
        """
        Enqueue a notification that an existing row needs maintenance.

        Concrete dispatch places a (table, row_id) tuple on the engine’s main-row queue
        without checking table eligibility or changing the database. This queue is
        separate from Database.dirty_records_queue.

        Example:
            sink.dirty_record("works", work_id)


        :param table: Table name converted to text by the concrete callback sink.
        :param row_id: Row ID converted to int by the concrete callback sink.
        :return: None; the abstract method supplies no operational default.
        """

        ...

    @abc.abstractmethod
    def new_dirty_record(self, table: str, row_id: int) -> None:
        """
        Enqueue a typed new-row notification for maintenance plugins.

        The concrete sink adds a DirtyRowEvent with kind=new_dirty_row to the manual
        event queue, not the legacy main-row queue. No row is created by this
        notification.

        Example:
            sink.new_dirty_record("works", work_id)


        :param table: Table name converted to text.
        :param row_id: New row ID converted to int.
        :return: None; the abstract method supplies no operational default.
        """

        ...

    @abc.abstractmethod
    def dirty_interlink_record(
        self, update_type: str, table1: str, table2: str, table1_id: int, table2_id: int
    ) -> None:
        """
        Enqueue a notification describing a changed relation between two rows.

        The concrete sink queues a five-item tuple for later DirtyInterlinkEvent
        conversion. It neither writes the relation nor verifies that either row exists.

        Example:
            sink.dirty_interlink_record("update", "works", "agents", work_id, agent_id)


        :param update_type: Update label preserved as text without validating a fixed
            vocabulary.
        :param table1: First endpoint table name.
        :param table2: Second endpoint table name.
        :param table1_id: First endpoint ID converted to int.
        :param table2_id: Second endpoint ID converted to int.
        :return: None; the abstract method supplies no operational default.
        """

        ...


class MaintenancePluginAPI(Protocol):
    """
    Define optional lifecycle/filter hooks and the required maintenance batch handler.

    name identifies the installed implementation, dependencies precede priority,
    and enabled_by_default supplies its initial persisted setting. The legacy
    queue adapter treats that setting as advisory. Plugins may implement this shape
    without subclassing this protocol. Default hooks do nothing, accept every event and
    preserve every event separately.

    mode declares LAZY or PROMPT consistency independently of priority. Install
    physical-table subscriptions with MaintenanceRuntime for transactional prompt
    effects and durable lazy obligations. The old queue engine is a transient
    compatibility adapter; now=True there is not a consistency guarantee.

    Example:
        class MyPlugin(MaintenancePluginAPI):
            name = "my-maintenance"
            def handle_events(self, context, events):
                return process_events(context.db, events)
    """

    name: str = "maintenance-plugin"
    version: str = "1"
    tables: tuple[str, ...] = ()
    dependencies: tuple[str, ...] = ()
    mode: MaintenanceMode = MaintenanceMode.LAZY
    priority: int = 0
    enabled_by_default: bool = True
    retry_limit: int = 5
    retry_backoff_s: float = 1.0

    def startup(self, context: MaintenancePluginContext) -> None:
        """
        Provide a no-op default for plugin initialization.

        The engine calls startup when its background loop begins, and for a plugin
        registered while the thread is alive. Manual run_once does not automatically
        invoke startup.

        Example:
            plugin.startup(context)


        :param context: Maintenance context containing the database and optional logger.
        :return: None; this default creates no resources.
        """

        return None

    def shutdown(self, context: MaintenancePluginContext) -> None:
        """
        Provide a no-op default for plugin resource cleanup.

        The background engine calls shutdown in reverse plugin order when leaving its
        loop. Manual run_once does not supply a shutdown lifecycle.

        Example:
            plugin.shutdown(context)


        :param context: Maintenance context containing the database and optional logger.
        :return: None; this default releases no resources.
        """

        return None

    def wants_event(self, event: MaintenanceEvent) -> bool:
        """
        Accept every maintenance event unless a plugin overrides this filter.

        The engine evaluates interest before coalescing. Override to limit kinds/tables
        without consuming or mutating the shared event list.

        Example:
            if plugin.wants_event(event):
                selected.append(event)


        :param event: Event considered for this plugin’s batch.
        :return: True in this default implementation.
        """

        return True

    def coalesce_key(self, event: MaintenanceEvent) -> Hashable | None:
        """
        Disable event coalescing by returning None by default.

        The engine keeps only the last interested event per non-None key within one
        dispatch batch, independently for each plugin. Unkeyed events precede collapsed
        groups in the dispatched list. A key must be hashable; returning an unhashable
        object fails during grouping outside the hook’s exception handler.

        Example:
            key = plugin.coalesce_key(event)  # None preserves this event independently.


        :param event: Interested event being considered for duplicate collapse.
        :return: None to preserve this event, or an override’s hashable grouping key.
        """

        return None

    @abc.abstractmethod
    def handle_events(
        self, context: MaintenancePluginContext, events: Iterable[MaintenanceEvent]
    ) -> MaintenancePluginResult:
        """
        Require a plugin to process its selected, coalesced event batch.

        The base body raises NotImplementedError. The engine records results by plugin
        name. MaintenanceRuntime rolls back a failed/incomplete batch and retains
        lazy retry evidence. Use context.connection for all database effects;
        commit, rollback, close and a second connection are forbidden.

        Example:
            result = plugin.handle_events(context, selected_events)


        :param context: Maintenance context containing the database and optional logger.
        :param events: Iterable of events accepted by this plugin; the engine currently
            supplies a list.
        :return: MaintenancePluginResult with
            handled/deferred/errors counts.
        :raises NotImplementedError: The abstract base implementation is called
            directly.
        """

        raise NotImplementedError


class MaintenanceServiceAPI(abc.ABC):
    """
    Specify plugin registration, bounded dispatch, stopping and rename requests.

    This service surface does not require callers to manipulate thread internals. The
    concrete MaintenanceEngine processes priority-ordered plugins and exposes a separate
    callback sink. The Maintainer facade forwards equivalent operations, while the older
    bot contract adds historical constructor/run expectations.

    Example:
        service: MaintenanceServiceAPI = db.maintenance.maintainer
        results = service.run_once(max_events=32)
    """

    @abc.abstractmethod
    def register_plugin(self, plugin: MaintenancePluginAPI) -> None:
        """
        Add a plugin and restore descending priority order.

        The current engine allows duplicate registrations and duplicate names; same-name
        results overwrite earlier entries. If its thread is alive, call the new plugin’s
        startup and log startup failures. Registration itself is not synchronized
        against dispatch.

        Only LAZY mode (or an omitted mode on an older duck-typed plugin) is supported
        by the current engine. Unknown modes raise ValueError; PROMPT raises
        NotImplementedError before registration or startup.

        Example:
            service.register_plugin(plugin)


        :param plugin: Plugin implementing lifecycle, event filtering, coalescing and
            batch handling.
        :return: None; the abstract method supplies no operational default.
        """

        ...

    @abc.abstractmethod
    def iter_plugins(self) -> tuple[MaintenancePluginAPI, ...]:
        """
        Return a snapshot of plugins in their current dispatch order.

        The tuple is a snapshot of registration order, not a copy of the plugin objects.

        Example:
            plugin_names = [plugin.name for plugin in service.iter_plugins()]


        :return: Tuple of registered plugin objects in the current engine, highest
            priority first.
        """

        ...

    @abc.abstractmethod
    def run_once(self, *, max_events: int = 128) -> dict[str, MaintenancePluginResult]:
        """
        Drain one bounded batch and dispatch it synchronously to interested plugins.

        Drain manual events first, then dirty-row events, then interlink events. If all
        queues are empty, dispatch one TickEvent. Coalescing may reduce each plugin’s
        batch. This does not start a thread or run startup/shutdown hooks, and is not
        serialized against a concurrently running engine loop.

        Example:
            results = service.run_once(max_events=32)


        :param max_events: Requested event limit; the current engine uses max(1,
            int(max_events)).
        :return: Mapping from participating plugin names to their returned or generated
            error results.
        """

        ...

    @abc.abstractmethod
    def stop(self) -> None:
        """
        Request that the maintenance background loop exit.

        The concrete engine clears its run flag and enqueues a shutdown event. It does
        not join the thread, finish every pending event or guarantee delivery of that
        shutdown event before exit. Thread owners must join separately when they require
        completion.

        Example:
            service.stop()
            engine.join(timeout=1)


        :return: None; the abstract method supplies no operational default.
        """

        ...

    @abc.abstractmethod
    def rename_item(
        self, item_id: int, table: str, value: str, now: bool = True
    ) -> None:
        """
        Dispatch or enqueue a rename request for maintenance plugins.

        The engine creates a RenameRequestEvent for explicitly supplied handlers.
        There is no built-in rename plugin; the default service refuses this route
        and directs callers to Catalog. Custom handlers retain their own error
        policy and the queue does not supply durable completion guarantees.

        Example:
            service.rename_item(work_id, "works", "New name", now=False)


        :param item_id: ID of the row named by the request.
        :param table: Table identifying the target for interested plugins.
        :param value: Requested replacement text.
        :param now: True to dispatch immediately; False to queue for a later batch.
        :return: None; the abstract method supplies no operational default.
        """

        ...


class MaintenanceBotAPI(MaintenanceServiceAPI):
    """
    Preserve the historical thread-shaped maintenance service contract.

    Inherit service operations and declare legacy construction/run entry points. The
    current MaintenanceEngine constructor instead accepts plugins and creates its own
    compatibility queues; it is not a signature-compatible implementation of this
    historical constructor. The Maintainer facade is the normal owner that creates and
    starts the engine.

    Example:
        engine = db.maintenance.maintainer
        engine.stop()
        engine.join(timeout=1)
    """

    @abc.abstractmethod
    def __init__(
        self,
        db: DatabaseAPI,
        dirtied_main_queue: Queue[tuple[str, int]] | None = None,
        dirtied_interlink_queue: Queue[tuple[str, str, str, int, int]] | None = None,
        interval: int = 2,
        scheduling_interval: float = 0.5,
    ) -> None:
        """
        Describe the legacy database, queue and timing inputs for a maintenance bot.

        These parameters belong to the historical abstract shape. Current
        MaintenanceEngine requires plugins instead of the two queue arguments and
        defaults scheduling_interval to 0.25; use its concrete constructor when
        constructing that engine.

        Example:
            bot = LegacyBot(db, dirtied_main_queue=row_queue,
            dirtied_interlink_queue=link_queue)


        :param db: Database supplying maintenance work.
        :param dirtied_main_queue: Historical main-row dirty queue, or None for
            implementation defaults.
        :param dirtied_interlink_queue: Historical interlink dirty queue, or None for
            implementation defaults.
        :param interval: Legacy interval between maintenance cycles, in seconds.
        :param scheduling_interval: Legacy polling/sleep granularity, in seconds.
        :return: None; the abstract method supplies no operational default.
        """

        ...

    @abc.abstractmethod
    def run(self) -> None:
        """
        Run plugin lifecycle hooks and repeated maintenance cycles until stopped.

        MaintenanceEngine starts plugins in priority order, calls run_once on its
        interval and shuts plugins down in reverse order. Calling run directly blocks
        the caller; Thread.start launches the background execution. stop requests exit
        without joining or draining every queued event.

        Example:
            engine.start()  # For a newly constructed, not-yet-started engine.


        :return: None when the concrete background loop exits.
        """

        ...
