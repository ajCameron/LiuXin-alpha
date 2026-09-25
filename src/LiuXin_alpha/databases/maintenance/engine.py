"""
Dispatch queued maintenance events to plugins on demand or in a daemon thread.

Manual requests are drained before row callbacks and interlink callbacks. Each
plugin receives its own filtered/coalesced view of the batch; events are broadcast,
not consumed by the first handler.
"""

from __future__ import annotations

import queue
import threading
import time
import weakref

from collections import defaultdict
from typing import TYPE_CHECKING, Iterable

from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.databases.maintenance.events import (
    DirtyInterlinkEvent,
    DirtyRowEvent,
    MaintenanceEvent,
    RenameRequestEvent,
    ShutdownEvent,
    TickEvent,
)
from LiuXin_alpha.databases.maintenance.plugins import (
    MaintenancePluginContext,
    MaintenancePluginResult,
)

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api import DatabaseAPI
    from LiuXin_alpha.databases.api.maintenance_api import MaintenancePluginAPI


class MaintenanceCallbackSink:
    """
    Adapt legacy callbacks to the engine's compatibility queues.

    The sink retains the engine strongly. Queue insertion and integer conversion
    failures propagate to callers.

    Example:
        >>> from types import SimpleNamespace
        >>> engine = MaintenanceEngine(SimpleNamespace(), [])
        >>> engine.callback_sink.dirty_record("creators", 1)
        >>> engine.main_table_dirtied_queue.get_nowait()
        ('creators', 1)
    """

    def __init__(self, engine: "MaintenanceEngine") -> None:
        """
        Retain the engine receiving subsequent callbacks.

        Example:
            >>> MaintenanceCallbackSink(engine)  # doctest: +SKIP


        :param engine: Maintenance engine whose queues receive callback notifications.
        :return: None.
        """
        self._engine = engine

    def dirty_record(self, table: str, row_id: int) -> None:  # noqa: ANN001 - callback compatibility
        """
        Enqueue a normalized (table, row_id) tuple without blocking.

        False table values become empty text; row_id is converted to int. Does not construct
        the event until the engine drains this queue.

        Example:
            >>> worker.dirty_record("creators", 1)  # doctest: +SKIP


        :param table: Table associated with the row or operation.
        :param row_id: Row identifier converted to int by event/callback construction.
        :return: None.
        """
        self._engine.main_table_dirtied_queue.put((str(table or ""), int(row_id)), block=False)

    def new_dirty_record(self, table: str, row_id: int) -> None:  # noqa: ANN001 - callback compatibility
        """
        Enqueue a DirtyRowEvent tagged new_dirty_row on the manual queue.

        Converts table and ID before insertion; this queue is drained ahead of compatibility
        queues.

        Example:
            >>> worker.new_dirty_record("creators", 1)  # doctest: +SKIP


        :param table: Table associated with the row or operation.
        :param row_id: Row identifier converted to int by event/callback construction.
        :return: None.
        """
        self._engine._manual_events.put(DirtyRowEvent(str(table or ""), int(row_id), kind="new_dirty_row"))

    def dirty_interlink_record(
            self,
            update_type: str,
            table1: str,
            table2: str,
            table1_id: int,
            table2_id: int) -> None:  # noqa: ANN001
        """
        Enqueue a normalized five-field relationship tuple without blocking.

        Text labels use str(value or "") and IDs use int(); failures propagate.

        Example:
            >>> worker.dirty_interlink_record("UPDATE", "creators", "titles", 1, 1)  # doctest: +SKIP


        :param update_type: Relationship change label; false values become empty text.
        :param table1: First relationship endpoint table.
        :param table2: Second relationship endpoint table.
        :param table1_id: First endpoint identifier, converted to int.
        :param table2_id: Second endpoint identifier, converted to int.
        :return: None.
        """
        self._engine.interlink_dirtied_queue.put(
            (str(update_type or ""), str(table1 or ""), str(table2 or ""), int(table1_id), int(table2_id)),
            block=False,
        )


class MaintenanceEngine(threading.Thread):
    """
    Own maintenance queues and dispatch plugin batches in priority order.

    Construction does not start the daemon thread. start() enables periodic dispatch;
    run_once() dispatches in the caller. No lock serializes manual and background
    dispatch. stop() requests exit without joining or promising a queue drain.

    Example:
        >>> from types import SimpleNamespace
        >>> engine = MaintenanceEngine(SimpleNamespace(), [])
        >>> engine.is_alive(), engine.run_once()
        (False, {})
    """

    def __init__(
        self,
        db: "DatabaseAPI",
        plugins: Iterable["MaintenancePluginAPI"],
        *,
        interval: float = 2.0,
        scheduling_interval: float = 0.25,
    ) -> None:
        """
        Initialize an unstarted daemon thread, sorted plugins and unbounded queues.

        Retains db weakly when possible, otherwise through a closure. Sorts descending
        integer priorities and converts timing options to float; no positivity validation or
        enabled_by_default filtering occurs.

        Example:
            >>> MaintenanceEngine(database, [])  # doctest: +SKIP


        :param db: Database whose rows, driver and schema helpers are used.
        :param plugins: Iterable of plugins, sorted by descending integer priority.
        :param interval: Seconds between dispatch passes; converted to float without range
            validation.
        :param scheduling_interval: Maximum sleep between scheduling checks, in seconds; not
            range-validated.
        :return: None.
        """
        super().__init__(name="liuxin-maintenance-engine", daemon=True)
        try:
            self._db_ref = weakref.ref(db)
        except TypeError:
            self._db_ref = lambda: db
        self._plugins = list(sorted(plugins, key=lambda plugin: int(getattr(plugin, "priority", 0)), reverse=True))
        self._manual_events: queue.Queue[MaintenanceEvent] = queue.Queue()
        self._keep_running = True
        self._interval = float(interval)
        self._scheduling_interval = float(scheduling_interval)
        self.callback_sink = MaintenanceCallbackSink(self)

        # Compatibility queues: existing code and tests may inspect these names.
        self.main_table_dirtied_queue: queue.Queue[tuple[str, int]] = queue.Queue()
        self.interlink_dirtied_queue: queue.Queue[tuple[str, str, str, int, int]] = queue.Queue()

    @property
    def db(self) -> "DatabaseAPI":
        """
        Resolve the live database object from the retained reference.

        Raises RuntimeError if a weakly referenced database has been collected.

        Example:
            >>> engine.db  # doctest: +SKIP


        :return: Database object itself, not a weakref.
        """
        db = self._db_ref()
        if db is None:
            raise RuntimeError("Database reference is gone.")
        return db

    @property
    def context(self) -> "MaintenancePluginContext":
        """
        Construct a fresh plugin context using the live database and default logger.

        Example:
            >>> engine.context  # doctest: +SKIP


        :return: New MaintenancePluginContext; an expired database reference raises
            RuntimeError.
        """
        return MaintenancePluginContext(db=self.db, logger=default_log)

    def register_plugin(self, plugin: "MaintenancePluginAPI") -> None:#
        """
        Append a plugin and restore descending priority order.

        If the thread is alive, immediately calls startup in the registering thread and logs
        ordinary startup exceptions. Registration has no duplicate check or synchronization
        with dispatch; integer-priority conversion errors propagate.

        Example:
            >>> engine.register_plugin(plugin)  # doctest: +SKIP


        :param plugin: Plugin instance to register without duplicate or enabled-flag
            filtering.
        :return: None.
        """
        self._plugins.append(plugin)
        self._plugins.sort(key=lambda item: int(getattr(item, "priority", 0)), reverse=True)
        if self.is_alive():
            try:
                plugin.startup(self.context)
            except Exception as exc:
                default_log.log_exception("Maintenance plugin startup failed.", exc, "WARNING")

    def iter_plugins(self) -> Iterable["MaintenancePluginAPI"]:
        """
        Snapshot the registered plugins in their current priority order.

        Example:
            >>> from types import SimpleNamespace
            >>> MaintenanceEngine(SimpleNamespace(), []).iter_plugins()
            ()


        :return: Tuple of plugin instances.
        """
        return tuple(self._plugins)

    def enqueue(self, event: "MaintenanceEvent") -> None:
        """
        Place an event on the manual queue without validating its class or kind.

        Example:
            >>> engine.enqueue(event)  # doctest: +SKIP


        :param event: Maintenance event to inspect or enqueue.
        :return: None.
        """
        self._manual_events.put(event)

    def stop(self) -> None:
        """
        Clear the run flag and enqueue a shutdown-context event.

        Does not join the worker or guarantee that queued events, including shutdown, will
        be dispatched. A ShutdownEvent enqueued through enqueue() alone does not clear the
        flag.

        Example:
            >>> engine.stop()  # doctest: +SKIP


        :return: None.
        """
        self._keep_running = False
        self._manual_events.put(ShutdownEvent("explicit-stop"))

    def rename_item(self, item_id: int, table: str, value: str, now: bool = True) -> None:
        """
        Create a rename request and dispatch it immediately or enqueue it.

        Synchronous dispatch does not run startup/shutdown hooks and can overlap the worker
        thread. Plugin result telemetry is discarded.

        Example:
            >>> engine.rename_item(1, "creators", "New name")  # doctest: +SKIP


        :param item_id: Identifier of the row to rename.
        :param table: Table associated with the row or operation.
        :param value: Requested new name; event construction converts false values to empty
            text.
        :param now: Whether to dispatch synchronously; False enqueues work.
        :return: None.
        """
        event = RenameRequestEvent(item_id=item_id, table=table, value=value)
        if now:
            self._dispatch([event])
        else:
            self.enqueue(event)

    def _drain_pending_events(self, *, max_events: int = 128) -> list[MaintenanceEvent]:
        """
        Drain a bounded batch in manual, row, then interlink queue order.

        The shared limit is at least one. Converts compatibility tuples to typed events;
        malformed tuples or ID conversions propagate after removal. Does not call
        task_done(). If no events are available, returns one TickEvent.

        Example:
            >>> from types import SimpleNamespace
            >>> engine = MaintenanceEngine(SimpleNamespace(), [])
            >>> engine._drain_pending_events()[0].kind
            'tick'


        :param max_events: Combined drain limit, coerced to int and clamped to at least one.
        :return: List of drained typed events, or a single idle tick.
        """
        batch: list[MaintenanceEvent] = []

        while len(batch) < max(1, int(max_events)):
            try:
                event = self._manual_events.get_nowait()
            except queue.Empty:
                break
            batch.append(event)

        while len(batch) < max(1, int(max_events)):
            try:
                table, row_id = self.main_table_dirtied_queue.get_nowait()
            except queue.Empty:
                break
            batch.append(DirtyRowEvent(table=table, row_id=row_id))

        while len(batch) < max(1, int(max_events)):
            try:
                update_type, table1, table2, table1_id, table2_id = self.interlink_dirtied_queue.get_nowait()
            except queue.Empty:
                break
            batch.append(
                DirtyInterlinkEvent(
                    update_type=update_type,
                    table1=table1,
                    table2=table2,
                    table1_id=table1_id,
                    table2_id=table2_id,
                )
            )

        if not batch:
            batch.append(TickEvent())
        return batch

    def run_once(self, *, max_events: int = 128) -> dict[str, MaintenancePluginResult]:
        """
        Drain pending work and dispatch one batch in the calling thread.

        Does not check the stop flag or run lifecycle hooks; idle queues produce a tick.

        Example:
            >>> engine.run_once()  # doctest: +SKIP


        :param max_events: Combined drain limit, coerced to int and clamped to at least one.
        :return: Mapping from participating plugin names to their results.
        """
        return self._dispatch(self._drain_pending_events(max_events=max_events))

    def _dispatch(self, batch: Iterable["MaintenanceEvent"]) -> dict[str, "MaintenancePluginResult"]:
        """
        Filter, coalesce and broadcast a materialized batch to every registered plugin.

        Selection or handler exceptions are logged and recorded as errors=1. A coalesce_key
        exception makes that event passthrough; an unhashable returned key is not caught.
        Passthrough events come first, then the last event for each key in first-key order.
        Plugins with no interest are omitted; duplicate names overwrite earlier results.

        Example:
            >>> engine._dispatch(events)  # doctest: +SKIP


        :param batch: Iterable of events materialized once for every plugin to inspect.
        :return: Result mapping keyed by stringified plugin name.
        """
        context = self.context
        events = list(batch)
        plugin_results: dict[str, MaintenancePluginResult] = {}

        for plugin in self._plugins:
            try:
                interested = [event for event in events if plugin.wants_event(event)]
            except Exception as exc:
                default_log.log_exception(
                    f"Maintenance plugin {getattr(plugin, 'name', plugin)!r} failed during wants_event().",
                    exc,
                    "WARNING",
                )
                plugin_results[str(getattr(plugin, "name", plugin))] = MaintenancePluginResult(errors=1)
                continue

            if not interested:
                continue

            grouped: dict[object, list[MaintenanceEvent]] = defaultdict(list)
            passthrough: list[MaintenanceEvent] = []
            for event in interested:
                try:
                    key = plugin.coalesce_key(event)
                except Exception as exc:
                    default_log.log_exception(
                        f"Maintenance plugin {getattr(plugin, 'name', plugin)!r} failed during coalesce_key().",
                        exc,
                        "WARNING",
                    )
                    key = None
                if key is None:
                    passthrough.append(event)
                else:
                    grouped[key].append(event)

            collapsed = passthrough + [group[-1] for group in grouped.values() if group]
            try:
                plugin_results[str(getattr(plugin, "name", plugin))] = plugin.handle_events(context, collapsed)
            except Exception as exc:
                default_log.log_exception(
                    f"Maintenance plugin {getattr(plugin, 'name', plugin)!r} failed during handle_events().",
                    exc,
                    "WARNING",
                )
                plugin_results[str(getattr(plugin, "name", plugin))] = MaintenancePluginResult(errors=1)

        return plugin_results

    def run(self) -> None:
        """
        Run plugin startup hooks, periodic dispatch and reverse-order shutdown hooks.

        start() invokes this in the daemon thread; direct calls block the caller. Ordinary
        lifecycle exceptions are logged. Dispatch-loop exceptions still run shutdown hooks
        but terminate the worker; stop() is observed between scheduling checks. No final
        queue drain is performed.

        Example:
            >>> engine.run()  # doctest: +SKIP


        :return: None.
        """
        context = self.context
        for plugin in self._plugins:
            try:
                plugin.startup(context)
            except Exception as exc:
                default_log.log_exception(
                    f"Maintenance plugin {getattr(plugin, 'name', plugin)!r} failed during startup().",
                    exc,
                    "WARNING",
                )

        try:
            next_tick = time.monotonic()
            while self._keep_running:
                now = time.monotonic()
                if now < next_tick:
                    time.sleep(min(self._scheduling_interval, next_tick - now))
                    continue
                next_tick = now + self._interval
                self.run_once()
        finally:
            for plugin in reversed(self._plugins):
                try:
                    plugin.shutdown(context)
                except Exception as exc:
                    default_log.log_exception(
                        f"Maintenance plugin {getattr(plugin, 'name', plugin)!r} failed during shutdown().",
                        exc,
                        "WARNING",
                    )
