
"""
Queue dirty-record notifications for later processing by the host driver.
"""

from __future__ import annotations

from LiuXin_alpha.utils.logging import default_log



class DirtyRecordsMixin:
    """
    Submit table/ID/reason tuples to a host-provided dirtied-record queue.

    Example:
        The host supplies ``tables`` and ``dirtied_records_queue`` before using this mixin.
    """

    def direct_dirty_record(self, table: str, table_id: int, reason: str) -> None:
        """
        Enqueue a dirty-record notification, warning if the table is unrecognized.

        An unknown table is still queued. This method performs no database write.

        Example:
            ``driver.direct_dirty_record("books", 3, "metadata changed")`` queues a notification.


        :param table: Table name checked against the host's current table collection.
        :param table_id: ID of the affected record.
        :param reason: Reason string retained in the queued tuple.
        :return: ``None``.
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