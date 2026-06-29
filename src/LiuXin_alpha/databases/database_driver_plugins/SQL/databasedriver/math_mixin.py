
"""
Get the max and min values of a column.
"""

from __future__ import annotations

from typing import Optional


class MathFunctionsMixin:
    """
    Math functions which can be run on columns.
    """

    def direct_get_max(self, column: str) -> Optional[int]:
        """
        Get the maximum value from a column and return it.
        """
        col_table = self.direct_identify_table_from_column(column)
        stmt = "SELECT MAX({}) FROM {};".format(column, col_table)

        conn = self.get_connection()
        max_val = next(conn.execute(stmt))[0]
        try:
            return int(max_val)
        except ValueError:
            return None

    def direct_get_min(self, column: str) -> Optional[int]:
        """
        Get the maximum value from a column and return it.
        """
        col_table = self.dirct_identify_table_from_column(column)
        stmt = "SELECT MIN({}) FROM {};".format(column, col_table)

        conn = self.get_connection()
        min_val = next(conn.execute(stmt))[0]
        try:
            return int(min_val)
        except TypeError:
            return None
