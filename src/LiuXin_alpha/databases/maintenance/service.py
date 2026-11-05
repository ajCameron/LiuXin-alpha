"""
Expose the legacy maintainer interface over the plugin engine.

The facade retains a database reference and starts background work unless constructed
with autostart=False for staged runtime initialization.
Dirty callbacks delegate to the engine. Selective cleanup is unavailable; physical
interlink merging uses one transaction and refuses conflicting duplicate payloads.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING

from LiuXin_alpha.databases.api import DatabaseMaintainerAPI
from LiuXin_alpha.databases.maintenance.builtin_plugins import (
    get_builtin_maintenance_plugins,
)
from LiuXin_alpha.databases.maintenance.engine import MaintenanceEngine

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api import DatabaseAPI, MaintenancePluginAPI
    from LiuXin_alpha.databases.maintenance.plugins import MaintenancePluginResult


class Maintainer(DatabaseMaintainerAPI):
    """
    Start and expose a database maintenance engine with compatibility helpers.

    Use stop() to request shutdown or close() to stop and join the worker.
    Manual dispatch can overlap background work.

    Example:
        >>> maintenance = Maintainer(database, plugins=[])  # doctest: +SKIP
        >>> maintenance.stop()  # doctest: +SKIP
    """

    db: DatabaseAPI

    def __init__(
        self,
        db: DatabaseAPI,
        *,
        plugins: Iterable[MaintenancePluginAPI] | None = None,
        interval: float = 2.0,
        scheduling_interval: float = 0.25,
        autostart: bool = True,
    ) -> None:
        """
        Bind the database, build the engine, alias its queues and start its thread.

        None plugins selects fresh builtins; an empty iterable explicitly selects no
        plugins. By default construction starts background work immediately; runtime
        wiring can defer it with autostart=False.

        Example:
            >>> Maintainer(database)  # doctest: +SKIP


        :param db: Database whose rows, driver and schema helpers are used.
        :param plugins: Optional plugin iterable; None selects builtins, while an empty
            iterable selects none.
        :param interval: Seconds between dispatch passes; converted to float without
            range validation.
        :param scheduling_interval: Maximum sleep between scheduling checks, in seconds;
            not range-validated.
        :param autostart: Start immediately; False prepares an unstarted engine.
        :return: None.
        """
        super().__init__(db=db)

        if plugins is None:
            plugins = get_builtin_maintenance_plugins()

        self.maintainer = MaintenanceEngine(
            db=self.db,
            plugins=plugins,
            interval=interval,
            scheduling_interval=scheduling_interval,
        )
        self.main_table_dirtied_queue = self.maintainer.main_table_dirtied_queue
        self.interlink_dirtied_queue = self.maintainer.interlink_dirtied_queue
        if autostart:
            self.start()

    def start(self) -> None:
        """Start the prepared worker once, after its owner has finished wiring."""
        self.maintainer.start()

    def close(self, *, timeout: float = 1.0) -> None:
        """Stop and join a started worker; refuse to discard one still running.

        Closing an unstarted or already stopped worker is harmless. A timeout
        leaves ownership with the caller, which may retry after work completes.
        """
        self.stop()
        if self.maintainer.ident is not None:
            self.maintainer.join(timeout=timeout)
        if self.maintainer.is_alive():
            raise RuntimeError("Maintenance worker is still running")

    def register_plugin(self, plugin: MaintenancePluginAPI) -> None:
        """
        Delegate registration and possible immediate startup to the engine.

        Example:
            >>> worker.register_plugin(plugin)  # doctest: +SKIP


        :param plugin: Plugin instance to register without duplicate or enabled-flag
            filtering.
        :return: None.
        """
        self.maintainer.register_plugin(plugin)

    def iter_plugins(self) -> tuple[MaintenancePluginAPI, ...]:
        """
        Return the engine's priority-ordered plugin snapshot.

        Example:
            >>> worker.iter_plugins()  # doctest: +SKIP


        :return: Tuple of plugin instances in dispatch order.
        """
        return self.maintainer.iter_plugins()

    def run_once(self, *, max_events: int = 128) -> dict[str, MaintenancePluginResult]:
        """
        Dispatch one bounded event batch synchronously through the engine.

        The background worker remains active; this method adds no synchronization.

        Example:
            >>> worker.run_once()  # doctest: +SKIP


        :param max_events: Combined drain limit, coerced to int and clamped to at least
            one.
        :return: Mapping of participating plugin names to MaintenancePluginResult
            objects.
        """
        return self.maintainer.run_once(max_events=max_events)

    def stop(self) -> None:
        """
        Request engine shutdown without joining or draining its queues.

        Example:
            >>> worker.stop()  # doctest: +SKIP


        :return: None.
        """
        self.maintainer.stop()

    def rename_item(
        self,
        item_id: int,
        table: str,
        value: str,
        now: bool = True,
        db: DatabaseAPI | None = None,
    ) -> None:
        """
        Dispatch or queue a rename through the bound database's engine.

        A different db creates a temporary Maintainer, starts it, forwards the request
        and stops it in finally without joining. With now=False that temporary engine
        may stop before processing the queued rename.

        Example:
            >>> worker.rename_item(1, "works", "New name")  # doctest: +SKIP


        :param item_id: Identifier of the row to rename.
        :param table: Table associated with the row or operation.
        :param value: Requested new name; event construction converts false values to
            empty text.
        :param now: Whether to dispatch synchronously; False enqueues work.
        :param db: Optional target database; None uses the bound database.
        :return: None.
        """
        target_db = db if db is not None else self.db
        if target_db is not self.db:
            # Compatibility behaviour only. The new engine is bound to one DB.
            maintenance = Maintainer(target_db)
            try:
                maintenance.rename_item(
                    item_id=item_id, table=table, value=value, now=now
                )
            finally:
                maintenance.stop()
            return
        self.maintainer.rename_item(item_id=item_id, table=table, value=value, now=now)

    def dirty_record(self, table: str, row_id: int) -> None:
        """
        Forward a changed-row notification to the compatibility queue.

        The callback sink converts IDs to integers and false text fields to empty
        strings; errors propagate.

        Example:
            >>> worker.dirty_record("works", 1)  # doctest: +SKIP


        :param table: Table associated with the row or operation.
        :param row_id: Row identifier converted to int by event/callback construction.
        :return: None.
        """
        self.maintainer.callback_sink.dirty_record(table, row_id)

    def new_dirty_record(self, table: str, row_id: int) -> None:
        """
        Forward a new-row notification to the manual event queue.

        The callback sink converts IDs to integers and false text fields to empty
        strings; errors propagate.

        Example:
            >>> worker.new_dirty_record("works", 1)  # doctest: +SKIP


        :param table: Table associated with the row or operation.
        :param row_id: Row identifier converted to int by event/callback construction.
        :return: None.
        """
        self.maintainer.callback_sink.new_dirty_record(table, row_id)

    def dirty_interlink_record(
        self,
        update_type: str,
        table1: str,
        table2: str,
        table1_id: int,
        table2_id: int,
    ) -> None:
        """
        Forward a relationship notification to the compatibility queue.

        The callback sink converts IDs to integers and false text fields to empty
        strings; errors propagate.

        Example:
            >>> worker.dirty_interlink_record(
            ...     "UPDATE", "works", "expressions", 1, 1
            ... )  # doctest: +SKIP


        :param update_type: Relationship change label; false values become empty text.
        :param table1: First relationship endpoint table.
        :param table2: Second relationship endpoint table.
        :param table1_id: First endpoint identifier, converted to int.
        :param table2_id: Second endpoint identifier, converted to int.
        :return: None.
        """
        self.maintainer.callback_sink.dirty_interlink_record(
            update_type, table1, table2, table1_id, table2_id
        )

    def clean(self, table: str, item_ids: Iterable[int]) -> None:
        """Report unavailable cleanup explicitly, without deleting candidate rows."""
        raise NotImplementedError(
            "Selective maintenance cleanup is unavailable; no rows were changed"
        )

    def merge(self, table: str, item_1_id: int, item_2_id: int) -> None:
        """
        Repoint registered interlinks from the second row to the first, then delete it.

        One serialized macro transaction owns every transfer and the final deletion.
        Only proven identical duplicate link payloads are removed. Other conflicts
        and write/maintenance failures roll back the entire operation. This physical
        repair does not combine scalar metadata, intralinks or tree parents.

        Example:
            >>> worker.merge("tags", 1, 2)  # doctest: +SKIP


        :param table: Table associated with the row or operation.
        :param item_1_id: Surviving row identifier.
        :param item_2_id: Row identifier to delete after attempting to repoint links.
        :return: None.
        """
        if table not in self.db.main_tables:
            raise ValueError("Merge requires a registered main table")
        if any(type(value) is not int or value < 1 for value in (item_1_id, item_2_id)):
            raise ValueError("Merge requires positive integer row IDs")
        if item_1_id == item_2_id:
            raise ValueError("Merge requires distinct retained and removed rows")
        macros = self.db.macros
        with macros.transaction(serialized=True):
            if not macros.get_row(table, item_1_id) or not macros.get_row(
                table, item_2_id
            ):
                raise ValueError("Both merge rows must exist")
            for main_table in sorted(self.db.main_tables):
                if main_table == table:
                    continue
                link_table = self.db.driver_wrapper.get_link_table_name(
                    table1=table, table2=main_table
                )
                if link_table is None or link_table not in self.db.interlink_tables:
                    continue
                self._do_merge_one_table(
                    src_table=main_table,
                    dst_table=table,
                    link_table=link_table,
                    item_1_id=item_1_id,
                    item_2_id=item_2_id,
                )
            macros.delete_row(table, item_2_id)

    def _do_merge_one_table(
        self,
        src_table: str,
        dst_table: str,
        link_table: str,
        item_1_id: int,
        item_2_id: int,
    ) -> None:
        """
        Repoint links in the shared transaction, preserving typed identities.

        Check duplicate identity and payload before deleting a redundant row; no
        write exception is treated as proof of duplication. Timestamps alone may
        differ. Conflicting roles, priorities or other payloads require an explicit
        domain decision and fail the whole merge.

        Example:
            >>> worker._do_merge_one_table(
            ...     "expressions", "works", "expression_work_links", 1, 2
            ... )  # doctest: +SKIP


        :param src_table: Other endpoint table.
        :param dst_table: Table containing the surviving and removed IDs.
        :param link_table: Physical interlink table whose rows are repointed.
        :param item_1_id: Surviving row identifier.
        :param item_2_id: Row identifier to delete after attempting to repoint links.
        :return: None.
        """
        if item_1_id == item_2_id:
            raise ValueError("Merge requires distinct retained and removed rows")
        wrapper, macros = self.db.driver_wrapper, self.db.macros
        spec = wrapper.get_link_spec(dst_table, src_table)
        if spec is None or spec.link_table != link_table:
            raise ValueError("Merge requires a complete physical link specification")
        link_id_col = wrapper.get_id_column(link_table)
        with macros.transaction():
            for row in macros.get_rows(
                link_table, where={spec.primary_link_col: item_2_id}
            ):
                identity = {
                    spec.primary_link_col: item_1_id,
                    spec.secondary_link_col: row[spec.secondary_link_col],
                }
                if spec.type_part_of_identity:
                    type_column = spec.type_link_col
                    if type_column is None:
                        raise ValueError(
                            "Typed link identity requires a physical type column"
                        )
                    identity[type_column] = row[type_column]
                duplicates = macros.get_rows(link_table, where=identity)
                if duplicates:
                    ignored = {
                        link_id_col,
                        spec.primary_link_col,
                        wrapper.get_table_spec(link_table).datestamp_column,
                    }
                    payload = {
                        key: value
                        for key, value in row.items()
                        if key not in ignored
                        and not key.endswith(
                            ("_created_timestamp_ep_k", "_modified_timestamp_ep_k")
                        )
                    }
                    if len(duplicates) != 1 or any(
                        duplicates[0][key] != value for key, value in payload.items()
                    ):
                        raise ValueError(
                            f"Conflicting duplicate link payload in {link_table}"
                        )
                    macros.delete_row(link_table, row[link_id_col])
                else:
                    macros.update_row(
                        link_table, row[link_id_col], {spec.primary_link_col: item_1_id}
                    )
