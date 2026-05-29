"""
Compatibility maintainer façade backed by the new maintenance engine.
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
    Compatibility façade between the database and the maintenance engine.

    The old maintainer combined callback sink, service lifecycle, and merge
    helpers. That public shape still exists for compatibility, but the actual
    background work now goes through :class:`MaintenanceEngine` and plugins.
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
        Constructor.

        :param db:
        :param plugins:
        :param interval:
        :param scheduling_interval:
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
        Register a plugin to do work through the maintenance engine.

        :param plugin:
        :return:
        """
        self.maintainer.register_plugin(plugin)

    def iter_plugins(self) -> None:
        """
        Iterate over the available plugins.

        :return:
        """
        return self.maintainer.iter_plugins()

    def run_once(self, *, max_events: int = 128) -> dict[str, "MaintenancePluginResult"]:
        """
        Preform a single run of the maintenance plugin.

        :param max_events:
        :return:
        """
        return self.maintainer.run_once(max_events=max_events)

    def stop(self) -> None:
        """
        Call stop to shut down the maintenance engine.

        :return:
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
        Indicate a rename has occurred in a particular table.

        :param item_id:
        :param table:
        :param value:
        :param now:
        :param db:
        :return:
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
        Note that a record in a table has been dirtied - so metadata can be updated.

        :param table:
        :param row_id:
        :return:
        """
        self.maintainer.callback_sink.dirty_record(table, row_id)

    def new_dirty_record(self, table: str, row_id: int) -> None:
        """
        Proxy which calls new_dirty_record directly on the maintainer.

        :param table:
        :param row_id:
        :return:
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
        Indicate that an interlink record has been dirtied in a particular table.

        :param update_type:
        :param table1:
        :param table2:
        :param table1_id:
        :param table2_id:
        :return:
        """
        self.maintainer.callback_sink.dirty_interlink_record(update_type, table1, table2, table1_id, table2_id)

    def clean(self, table: str, item_ids: Iterable[int]) -> None:
        """
        Note that the given item ids should be cleaned from the given table.

        :param table:
        :param item_ids:
        :return:
        """
        legacy_clean(self.db, table, item_ids=item_ids)

    def merge(self, table: str, item_1_id: int, item_2_id: int) -> None:
        """
        Combined the given two items into one, repointing links as required.

        :param table:
        :param item_1_id:
        :param item_2_id:
        :return:
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
