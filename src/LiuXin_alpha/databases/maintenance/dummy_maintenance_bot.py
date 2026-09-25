"""
Provide inert callbacks for maintenance wiring.

Every method ignores its arguments and returns None; no thread, queue or database is
created.
"""

from __future__ import print_function, annotations


class DummyMaintenanceBot:
    """
    Accept legacy dirty callbacks without scheduling work.

    Example:
        >>> bot = DummyMaintenanceBot()
        >>> bot.dirty_record("works", 1) is None
        True
    """

    def __init__(self):
        """
        Ignore this compatibility call without creating or changing state.

        Example:
            >>> bot = DummyMaintenanceBot()
            >>> bot.new_dirty_record("works", 1) is None
            True


        :return: None.
        """
        pass

    def dirty_record(self, table, row_id):
        """
        Ignore this compatibility call without creating or changing state.

        Example:
            >>> bot = DummyMaintenanceBot()
            >>> bot.new_dirty_record("works", 1) is None
            True


        :param table: Table associated with the row or operation.
        :param row_id: Row identifier converted to int by event/callback construction.
        :return: None.
        """
        pass

    def new_dirty_record(self, table, row_id):
        """
        Ignore this compatibility call without creating or changing state.

        Example:
            >>> bot = DummyMaintenanceBot()
            >>> bot.new_dirty_record("works", 1) is None
            True


        :param table: Table associated with the row or operation.
        :param row_id: Row identifier converted to int by event/callback construction.
        :return: None.
        """
        pass

    def dirty_interlink_record(self, update_type, table1, table2, table1_id, table2_id):
        """
        Ignore this compatibility call without creating or changing state.

        Example:
            >>> bot = DummyMaintenanceBot()
            >>> bot.new_dirty_record("works", 1) is None
            True


        :param update_type: Relationship change label; false values become empty text.
        :param table1: First relationship endpoint table.
        :param table2: Second relationship endpoint table.
        :param table1_id: First endpoint identifier, converted to int.
        :param table2_id: Second endpoint identifier, converted to int.
        :return: None.
        """
        pass
