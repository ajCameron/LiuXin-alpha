

from LiuXin_alpha.utils.logging import default_log

class DirtyRecordsMixin:
    """
    Mixin to deal with dirty records.
    """

    def dirty_record(self, table, table_id, reason):
        """
        Add a record to the dirtied dictionary.

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