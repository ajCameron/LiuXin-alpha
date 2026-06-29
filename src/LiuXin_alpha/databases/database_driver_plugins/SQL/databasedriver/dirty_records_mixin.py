
"""
Methods to interact directly with the dirtied records table.
"""

from __future__ import annotations

from LiuXin_alpha.utils.logging import default_log



class DirtyRecordsMixin:
    """
    Mixin to deal with dirty records.
    """

    def direct_dirty_record(self, table: str, table_id: int, reason: str) -> None:
        """
        Add a record to the dirtied queue.

        :param table:
        :param table_id:
        :param reason:
        :return:
        """
        if table not in self.tables:
            wrn_str = "Unable to dirtied record - table not found.\n"
            default_log.log_variables(
                wrn_str,
                "WARNING",
                ("table", table),
                ("table_id", table_id),
                ("reason", reason),
            )
        self.dirtied_records_queue.put((table, table_id, reason))