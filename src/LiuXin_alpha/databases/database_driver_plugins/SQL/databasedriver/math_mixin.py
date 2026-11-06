"""
Query column extrema and coerce successful aggregate results to integers.
"""

from __future__ import annotations

from typing import Optional
from contextlib import closing


from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver._state import (
    SQLDriverMixinState,
    driver_operations,
)


class MathFunctionsMixin(SQLDriverMixinState):
    """
    Provide integer-valued SQL MIN/MAX queries using host table inference.

    Example:
        ``driver.direct_get_max("book_id")`` queries the inferred books table.
    """

    def direct_get_max(self, column: str) -> Optional[int]:
        """
        Query MAX and convert it with int, returning None for null or non-convertible
        values.

        Float conversion truncates. SQL/inference errors propagate; the acquired
        connection and cursor close on every path.

        Example:
            ``driver.direct_get_max("book_id")`` returns ``None`` for an empty table.


        :param column: Trusted column name whose table is inferred by the driver.
        :return: The integer maximum, or ``None`` on TypeError/ValueError during
            conversion.
        """
        col_table = driver_operations(self).direct_identify_table_from_column(column)
        stmt = "SELECT MAX({}) FROM {};".format(column, col_table)

        with closing(driver_operations(self).get_connection()) as conn:
            with closing(conn.execute(stmt)) as cursor:
                row = cursor.fetchone()
                max_val = row[0] if row is not None else None
        if max_val is None:
            return None
        try:
            return int(max_val)
        except (TypeError, ValueError):
            return None

    def direct_get_min(self, column: str) -> Optional[int]:
        """
        Query MIN and convert it with int, returning None for null or non-convertible
        values.

        Float conversion truncates. SQL/inference errors propagate; the acquired
        connection and cursor close on every path.

        Example:
            ``driver.direct_get_min("book_id")`` returns the smallest ID when present.


        :param column: Trusted column name whose table is inferred by the driver.
        :return: The integer minimum, or ``None`` on TypeError/ValueError during
            conversion.
        """
        col_table = driver_operations(self).direct_identify_table_from_column(column)
        stmt = "SELECT MIN({}) FROM {};".format(column, col_table)

        with closing(driver_operations(self).get_connection()) as conn:
            with closing(conn.execute(stmt)) as cursor:
                row = cursor.fetchone()
                min_val = row[0] if row is not None else None
        if min_val is None:
            return None
        try:
            return int(min_val)
        except (TypeError, ValueError):
            return None
