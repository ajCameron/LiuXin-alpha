
"""
Query column extrema and coerce successful aggregate results to integers.
"""

from __future__ import annotations

from typing import Optional


class MathFunctionsMixin:
    """
    Provide integer-valued SQL MIN/MAX queries using host table inference.

    Example:
        ``driver.direct_get_max("book_id")`` queries the inferred books table.
    """

    def direct_get_max(self, column: str) -> Optional[int]:
        """
        Query MAX and convert it with int, returning None for null or non-convertible values.

        Float conversion truncates. SQL/inference errors propagate; the acquired connection is not explicitly closed here.

        Example:
            ``driver.direct_get_max("book_id")`` returns ``None`` for an empty table.


        :param column: Trusted column name whose table is inferred by the driver.
        :return: The integer maximum, or ``None`` on TypeError/ValueError during conversion.
        """
        col_table = self.direct_identify_table_from_column(column)
        stmt = "SELECT MAX({}) FROM {};".format(column, col_table)

        conn = self.get_connection()
        max_val = next(conn.execute(stmt))[0]
        try:
            return int(max_val)
        except (TypeError, ValueError):
            return None

    def direct_get_min(self, column: str) -> Optional[int]:
        """
        Query MIN and convert it with int, returning None for null or non-convertible values.

        Float conversion truncates. SQL/inference errors propagate; the acquired connection is not explicitly closed here.

        Example:
            ``driver.direct_get_min("book_id")`` returns the smallest ID when present.


        :param column: Trusted column name whose table is inferred by the driver.
        :return: The integer minimum, or ``None`` on TypeError/ValueError during conversion.
        """
        col_table = self.direct_identify_table_from_column(column)
        stmt = "SELECT MIN({}) FROM {};".format(column, col_table)

        conn = self.get_connection()
        min_val = next(conn.execute(stmt))[0]
        try:
            return int(min_val)
        except (TypeError, ValueError):
            return None
