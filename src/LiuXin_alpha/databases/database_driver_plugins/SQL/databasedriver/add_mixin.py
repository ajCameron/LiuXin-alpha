"""
Insert inferred-table rows with normalized identity derivation and custom-column text
sanitization.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from typing import Any, Literal
from collections.abc import Iterable

from LiuXin_alpha.databases.normalized_identities import (
    NormalizedValuePreparer,
    prepare_normalized_values,
)
from LiuXin_alpha.databases.transactions import transaction_scope
from LiuXin_alpha.errors import (
    DatabaseDriverError,
    DatabaseIntegrityError,
    InputIntegrityError,
)


from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver._state import (
    SQLDriverMixinState,
    driver_operations,
)


class AddingMixin(SQLDriverMixinState):
    """
    Add individual or homogeneous batches of row dictionaries through host driver
    helpers.

    Example:
        A driver combines ``AddingMixin`` with table inference and connection
        management.
    """

    @staticmethod
    def _sanitize_embedded_nul_text(
        *, target_table: str, row_dict: dict[str, Any]
    ) -> None:
        """
        Replace embedded NUL characters in string values with visible ``<NUL>`` text.

        Mutate the mapping in place; non-string values are untouched. The table argument
        is currently unused.

        Example:
            >>> row = {"value": "a" + chr(0) + "b"}
            >>> AddingMixin._sanitize_embedded_nul_text(
            ...     target_table="custom_column_1", row_dict=row
            ... )
            >>> row
            {'value': 'a<NUL>b'}


        :param target_table: Table context accepted by the helper but not consulted.
        :param row_dict: Mutable column/value mapping whose string payloads are
            sanitized.
        :return: ``None``.
        """

        for col, val in list(row_dict.items()):
            if isinstance(val, str) and "\x00" in val:
                row_dict[col] = val.replace("\x00", "<NUL>")

    def direct_add_simple_row_dict(self, row_dict: dict[str, Any]) -> int:
        """Insert one row, committing only success and always closing its connection.

        Custom-column text retains the existing embedded-NUL sanitization. Table
        inference and normalization operate on a copy of the caller's mapping.

        Example:
            ``driver.direct_add_simple_row_dict({"work_title": "None"})`` preserves
            the literal text and returns the generated identity.

        :param row_dict: Column/value mapping with an optional table hint.
        :return: The cursor's inserted row ID.
        """
        row_dict = dict(row_dict)
        target_table = driver_operations(self).direct_identify_table_from_row(row_dict)
        if target_table is False:
            raise InputIntegrityError("Cannot insert a row without an identified table")
        row_dict = prepare_normalized_values(
            driver_operations(self), target_table, row_dict
        )
        if target_table.startswith("custom_column_"):
            self._sanitize_embedded_nul_text(
                target_table=target_table, row_dict=row_dict
            )
        columns = tuple(row_dict)
        if columns:
            stmt = "INSERT INTO `{}` ({}) VALUES ({})".format(
                target_table, ",".join(columns), ",".join("?" for _ in columns)
            )
        else:
            stmt = f"INSERT INTO `{target_table}` DEFAULT VALUES"
        try:
            with closing(driver_operations(self).get_connection()) as conn:
                with transaction_scope(conn, owns_transaction=True):
                    cursor = conn.execute(
                        stmt, tuple(row_dict[column] for column in columns)
                    )
                    new_id = cursor.lastrowid
                    if new_id is None:
                        raise DatabaseDriverError(
                            "Insert did not return a row identity"
                        )
        except sqlite3.IntegrityError as exc:
            raise DatabaseIntegrityError(
                f"Insert into {target_table!r} failed: {exc}"
            ) from exc
        except sqlite3.Error as exc:
            raise DatabaseDriverError(
                f"Insert into {target_table!r} failed: {exc}"
            ) from exc
        return new_id

    def direct_add_multiple_simple_row_dicts(
        self, row_dict_list: Iterable[dict[str, Any]]
    ) -> Literal[True] | None:
        """Insert homogeneous row mappings as one atomic batch.

        Validate table and column sets before writing. Every parameter tuple follows
        the same column order, regardless of each input mapping's construction order.
        Resolve normalization declarations/headings once for this batch and reload
        them for the next call.
        A failure rolls back the whole batch and closes its owned connection.

        Example:
            Mappings with keys ``a, b`` and ``b, a`` bind their values to the same
            columns. A later uniqueness failure leaves neither row inserted.

        :param row_dict_list: Iterable of same-table mappings; never mutated.
        :return: True for an empty batch; otherwise None.
        """
        rows = [dict(row) for row in row_dict_list]
        if not rows:
            return True
        target_table = driver_operations(self).direct_identify_table_from_row(rows[0])
        if target_table is False:
            raise InputIntegrityError("Cannot insert rows without an identified table")
        for row in rows[1:]:
            if (
                driver_operations(self).direct_identify_table_from_row(row)
                != target_table
            ):
                raise InputIntegrityError("Rows from different tables.")
        normalizer = NormalizedValuePreparer.from_source(
            driver_operations(self), target_table
        )
        rows = [normalizer.prepare(row) for row in rows]
        columns = tuple(rows[0])
        id_column = driver_operations(self).direct_get_id_column(target_table)
        for row in rows:
            if set(row) != set(columns):
                raise InputIntegrityError("Rows with different column names.")
            if row.get(id_column) is not None:
                raise InputIntegrityError("Cannot update a row using this method!")
            if target_table.startswith("custom_column_"):
                self._sanitize_embedded_nul_text(
                    target_table=target_table, row_dict=row
                )
        stmt = "INSERT INTO `{}` ({}) VALUES ({})".format(
            target_table, ",".join(columns), ",".join("?" for _ in columns)
        )
        values = [tuple(row[column] for column in columns) for row in rows]
        try:
            with closing(driver_operations(self).get_connection()) as conn:
                with transaction_scope(conn, owns_transaction=True):
                    conn.executemany(stmt, values)
        except sqlite3.IntegrityError as exc:
            raise DatabaseIntegrityError(
                f"Batch insert into {target_table!r} failed: {exc}"
            ) from exc
        except sqlite3.Error as exc:
            raise DatabaseDriverError(
                f"Batch insert into {target_table!r} failed: {exc}"
            ) from exc

        return None
