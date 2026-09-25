"""
Expose the legacy maintainer interface over the plugin engine.

The facade starts background work immediately and retains a database reference.
Dirty callbacks delegate to the engine; cleanup and merge helpers retain their
legacy limitations.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Iterable, Optional, TYPE_CHECKING

from LiuXin_alpha.databases.api import DatabaseMaintainerAPI
from LiuXin_alpha.databases.maintenance.builtin_plugins import get_builtin_maintenance_plugins
from LiuXin_alpha.databases.maintenance.engine import MaintenanceEngine
from LiuXin_alpha.databases.maintenance.legacy import clean as legacy_clean

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api import DatabaseAPI
    from LiuXin_alpha.databases.maintenance.engine import MaintenancePluginResult


class Maintainer(DatabaseMaintainerAPI):
    """
    Start and expose a database maintenance engine with compatibility helpers.

    Use stop() to request shutdown; joining requires the underlying maintainer thread.
    Manual dispatch can overlap background work.

    Example:
        >>> maintenance = Maintainer(database, plugins=[])  # doctest: +SKIP
        >>> maintenance.stop()  # doctest: +SKIP
    """

    db: "DatabaseAPI"

    def __init__(
        self,
        db: "DatabaseAPI",
        *,
        plugins=None,
        interval: float = 2.0,
        scheduling_interval: float = 0.25,
    ) -> None:
        """
        Bind the database, build the engine, alias its queues and start its thread.

        None plugins selects fresh builtins; an empty iterable explicitly selects no
        plugins. Construction starts background work immediately.

        Example:
            >>> Maintainer(database)  # doctest: +SKIP


        :param db: Database whose rows, driver and schema helpers are used.
        :param plugins: Optional plugin iterable; None selects builtins, while an empty
            iterable selects none.
        :param interval: Seconds between dispatch passes; converted to float without range
            validation.
        :param scheduling_interval: Maximum sleep between scheduling checks, in seconds; not
            range-validated.
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
        self.maintainer.start()

    def register_plugin(self, plugin) -> None:  # noqa: ANN001 - plugin base is intentionally duck-typed
        """
        Delegate registration and possible immediate startup to the engine.

        Example:
            >>> worker.register_plugin(plugin)  # doctest: +SKIP


        :param plugin: Plugin instance to register without duplicate or enabled-flag
            filtering.
        :return: None.
        """
        self.maintainer.register_plugin(plugin)

    def iter_plugins(self) -> None:
        """
        Return the engine's priority-ordered plugin snapshot.

        Example:
            >>> worker.iter_plugins()  # doctest: +SKIP


        :return: Tuple of plugin instances, despite the None annotation.
        """
        return self.maintainer.iter_plugins()

    def run_once(self, *, max_events: int = 128) -> dict[str, "MaintenancePluginResult"]:
        """
        Dispatch one bounded event batch synchronously through the engine.

        The background worker remains active; this method adds no synchronization.

        Example:
            >>> worker.run_once()  # doctest: +SKIP


        :param max_events: Combined drain limit, coerced to int and clamped to at least one.
        :return: Mapping of participating plugin names to MaintenancePluginResult objects.
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
        db: Optional["DatabaseAPI"] = None,
    ) -> None:
        """
        Dispatch or queue a rename through the bound database's engine.

        A different db creates a temporary Maintainer, starts it, forwards the request and
        stops it in finally without joining. With now=False that temporary engine may stop
        before processing the queued rename.

        Example:
            >>> worker.rename_item(1, "creators", "New name")  # doctest: +SKIP


        :param item_id: Identifier of the row to rename.
        :param table: Table associated with the row or operation.
        :param value: Requested new name; event construction converts false values to empty
            text.
        :param now: Whether to dispatch synchronously; False enqueues work.
        :param db: Optional target database; None uses the bound database.
        :return: None.
        """
        target_db = db if db is not None else self.db
        if target_db is not self.db:
            # Compatibility behaviour only. The new engine is bound to one DB.
            maintenance = Maintainer(target_db)
            try:
                maintenance.rename_item(item_id=item_id, table=table, value=value, now=now)
            finally:
                maintenance.stop()
            return
        self.maintainer.rename_item(item_id=item_id, table=table, value=value, now=now)

    def dirty_record(self, table: str, row_id: int) -> None:
        """
        Forward a changed-row notification to the compatibility queue.

        The callback sink converts IDs to integers and false text fields to empty strings;
        errors propagate.

        Example:
            >>> worker.dirty_record("creators", 1)  # doctest: +SKIP


        :param table: Table associated with the row or operation.
        :param row_id: Row identifier converted to int by event/callback construction.
        :return: None.
        """
        self.maintainer.callback_sink.dirty_record(table, row_id)

    def new_dirty_record(self, table: str, row_id: int) -> None:
        """
        Forward a new-row notification to the manual event queue.

        The callback sink converts IDs to integers and false text fields to empty strings;
        errors propagate.

        Example:
            >>> worker.new_dirty_record("creators", 1)  # doctest: +SKIP


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

        The callback sink converts IDs to integers and false text fields to empty strings;
        errors propagate.

        Example:
            >>> worker.dirty_interlink_record("UPDATE", "creators", "titles", 1, 1)  # doctest: +SKIP


        :param update_type: Relationship change label; false values become empty text.
        :param table1: First relationship endpoint table.
        :param table2: Second relationship endpoint table.
        :param table1_id: First endpoint identifier, converted to int.
        :param table2_id: Second endpoint identifier, converted to int.
        :return: None.
        """
        self.maintainer.callback_sink.dirty_interlink_record(update_type, table1, table2, table1_id, table2_id)

    def clean(self, table: str, item_ids: Iterable[int]) -> None:
        """
        Call the unfinished legacy cleanup helper with a required ID iterable.

        Any non-None item_ids currently reaches raise NotImplemented, which produces
        TypeError rather than NotImplementedError. This path performs no deletion.

        Example:
            >>> worker.clean("creators", [1])  # doctest: +SKIP


        :param table: Table associated with the row or operation.
        :param item_ids: Optional restricted ID iterable; this legacy mode is unfinished.
        :return: None only when the legacy helper returns; ordinary ID iterables raise
            TypeError.
        """
        legacy_clean(self.db, table, item_ids=item_ids)

    def merge(self, table: str, item_1_id: int, item_2_id: int) -> None:
        """
        Repoint registered interlinks from the second row to the first, then delete it.

        Iterates other main tables, skipping absent or unregistered link tables. Does not
        merge scalar row values, intralinks or tree parents. Earlier updates are not rolled
        back here if a later operation fails.

        Example:
            >>> worker.merge("creators", 1, 1)  # doctest: +SKIP


        :param table: Table associated with the row or operation.
        :param item_1_id: Surviving row identifier.
        :param item_2_id: Row identifier to delete after attempting to repoint links.
        :return: None.
        """
        for main_table in self.db.main_tables:
            if main_table == table:
                continue

            link_table = self.db.driver_wrapper.get_link_table_name(table1=table, table2=main_table)
            if link_table is None:
                continue
            if link_table not in self.db.interlink_tables:
                continue

            self._do_merge_one_table(
                src_table=main_table,
                dst_table=table,
                link_table=link_table,
                item_1_id=item_1_id,
                item_2_id=item_2_id,
            )

        item_2_row = self.db.get_row_from_id(table, row_id=item_2_id)
        self.db.delete(item_2_row)

    def _do_merge_one_table(
        self,
        src_table: str,
        dst_table: str,
        link_table: str,
        item_1_id: int,
        item_2_id: int,
    ) -> None:
        """
        Replace one endpoint ID in each matching link row and sync it.

        On any sync exception, deletes that link row, assuming a duplicate; unrelated write
        failures can therefore also trigger deletion. No transaction boundary is added.

        Example:
            >>> worker._do_merge_one_table("creators", "titles", "creator_title_links", 1, 1)  # doctest: +SKIP


        :param src_table: Other endpoint table.
        :param dst_table: Table containing the surviving and removed IDs.
        :param link_table: Physical interlink table whose rows are repointed.
        :param item_1_id: Surviving row identifier.
        :param item_2_id: Row identifier to delete after attempting to repoint links.
        :return: None.
        """
        dst_table_id_col = self.db.driver_wrapper.get_id_column(dst_table)
        link_table_tag_id_col = self.db.driver_wrapper.get_link_column(
            table1=dst_table,
            table2=src_table,
            column_type=dst_table_id_col,
        )
        link_table_id_col = self.db.driver_wrapper.get_id_column(link_table)

        affect_link_ids = self.db.macros.get_values_one_condition(
            table=link_table,
            rtn_column=link_table_id_col,
            cond_column=link_table_tag_id_col,
            value=item_2_id,
            default_value=(),
        )

        for link_table_id in affect_link_ids:
            link_table_row = self.db.get_row_from_id(link_table, link_table_id)
            link_table_row[link_table_tag_id_col] = item_1_id
            try:
                link_table_row.sync()
            except Exception:
                # Link already exists - no repoint is needed - just remove the extraneous link.
                self.db.delete(link_table_row)
