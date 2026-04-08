"""Plugin-driven background maintenance engine."""

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
    from LiuXin_alpha.databases.api.maintenance import MaintenancePluginAPI


class MaintenanceCallbackSink:
    """Compatibility adapter exposing the callback methods drivers already call."""

    def __init__(self, engine: "MaintenanceEngine") -> None:
        self._engine = engine

    def dirty_record(self, table, row_id):  # noqa: ANN001 - callback compatibility
        self._engine.main_table_dirtied_queue.put((str(table or ""), int(row_id)), block=False)

    def new_dirty_record(self, table, row_id):  # noqa: ANN001 - callback compatibility
        self._engine._manual_events.put(DirtyRowEvent(str(table or ""), int(row_id), kind="new_dirty_row"))

    def dirty_interlink_record(self, update_type, table1, table2, table1_id, table2_id):  # noqa: ANN001
        self._engine.interlink_dirtied_queue.put(
            (str(update_type or ""), str(table1 or ""), str(table2 or ""), int(table1_id), int(table2_id)),
            block=False,
        )


class MaintenanceEngine(threading.Thread):
    """Background dispatcher that routes maintenance events to registered plugins."""

    def __init__(
        self,
        db: "DatabaseAPI",
        plugins: Iterable["MaintenancePluginAPI"],
        *,
        interval: float = 2.0,
        scheduling_interval: float = 0.25,
    ) -> None:
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
        db = self._db_ref()
        if db is None:
            raise RuntimeError("Database reference is gone.")
        return db

    @property
    def context(self) -> MaintenancePluginContext:
        return MaintenancePluginContext(db=self.db, logger=default_log)

    def register_plugin(self, plugin: "MaintenancePluginAPI") -> None:
        self._plugins.append(plugin)
        self._plugins.sort(key=lambda item: int(getattr(item, "priority", 0)), reverse=True)
        if self.is_alive():
            try:
                plugin.startup(self.context)
            except Exception as exc:
                default_log.log_exception("Maintenance plugin startup failed.", exc, "WARNING")

    def iter_plugins(self):
        return tuple(self._plugins)

    def enqueue(self, event: MaintenanceEvent) -> None:
        self._manual_events.put(event)

    def stop(self) -> None:
        self._keep_running = False
        self._manual_events.put(ShutdownEvent("explicit-stop"))

    def rename_item(self, item_id: int, table: str, value: str, now: bool = True) -> None:
        event = RenameRequestEvent(item_id=item_id, table=table, value=value)
        if now:
            self._dispatch([event])
        else:
            self.enqueue(event)

    def _drain_pending_events(self, *, max_events: int = 128) -> list[MaintenanceEvent]:
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
        return self._dispatch(self._drain_pending_events(max_events=max_events))

    def _dispatch(self, batch: Iterable[MaintenanceEvent]) -> dict[str, MaintenancePluginResult]:
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
