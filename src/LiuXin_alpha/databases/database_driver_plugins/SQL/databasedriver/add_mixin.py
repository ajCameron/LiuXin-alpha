"""
Insert inferred-table rows with normalized identity derivation and custom-column text
sanitization.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from typing import Any

from LiuXin_alpha.databases.normalized_identities import (
    prepare_normalized_values,
)
from LiuXin_alpha.databases.transactions import transaction_scope
from LiuXin_alpha.errors import (
    DatabaseDriverError,
    DatabaseIntegrityError,
    InputIntegrityError,
)


class AddingMixin:
    """
    Add individual or homogeneous batches of row dictionaries through host driver
    helpers.

    Example:
        A driver combines ``AddingMixin`` with table inference and connection
        management.
    """

    @staticmethod
    def _sanitize_embedded_nul_text(*, target_table: str, row_dict: dict) -> None:
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
        target_table = self.direct_identify_table_from_row(row_dict)
        row_dict = prepare_normalized_values(self, target_table, row_dict)
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
            with closing(self.get_connection()) as conn:
                with transaction_scope(conn, owns_transaction=True):
                    cursor = conn.execute(
                        stmt, tuple(row_dict[column] for column in columns)
                    )
                    new_id = cursor.lastrowid
        except sqlite3.IntegrityError as exc:
            raise DatabaseIntegrityError(
                f"Insert into {target_table!r} failed: {exc}"
            ) from exc
        except sqlite3.Error as exc:
            raise DatabaseDriverError(
                f"Insert into {target_table!r} failed: {exc}"
            ) from exc
        return new_id

    def direct_add_multiple_simple_row_dicts(self, row_dict_list):
        """Insert homogeneous row mappings as one atomic batch.

        Validate table and column sets before writing. Every parameter tuple follows
        the same column order, regardless of each input mapping's construction order.
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
        target_table = self.direct_identify_table_from_row(rows[0])
        for row in rows[1:]:
            if self.direct_identify_table_from_row(row) != target_table:
                raise InputIntegrityError("Rows from different tables.")
        rows = [prepare_normalized_values(self, target_table, row) for row in rows]
        columns = tuple(rows[0])
        id_column = self.direct_get_id_column(target_table)
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
            with closing(self.get_connection()) as conn:
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
