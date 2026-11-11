"""
Update bound row values and maintain configured normalized identity columns.
"""

from __future__ import annotations

import sqlite3
from contextlib import closing
from copy import deepcopy
from typing import Any, Literal

from LiuXin_alpha.databases.db_types.driver_types import RowSQLConnection

from LiuXin_alpha.databases.normalized_identities import (
    prepare_normalized_values,
)
from LiuXin_alpha.databases.transactions import transaction_scope
from LiuXin_alpha.errors import (
    DatabaseDriverError,
    DatabaseIntegrityError,
    RowIntegrityError,
    InputIntegrityError,
)
from LiuXin_alpha.utils.logging import default_log


from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver._state import (
    SQLDriverMixinState,
    driver_operations,
)


class UpdateMixin(SQLDriverMixinState):
    """
    Provide scalar-column batches and dictionary-based row updates through host schema
    helpers.

    Example:
        ``driver.direct_update_columns({3: "Example"}, field="book_title")`` updates one
        inferred column.
    """

    # Todo: Check for field degeneracy
    # Todo: I think this may have been superseded by the writers....
    def direct_update_columns(
        self,
        id_values_map: dict[int, Any],
        field: str | None = None,
        table: str | None = None,
    ) -> None:
        """Update one field for multiple IDs atomically, preserving normalized keys.

        Infer the field's table; a conflicting table hint retains the legacy warning
        and inferred-table behavior. Dictionary-valued updates remain unsupported.

        Example:
            Updating only ``tag_phash`` rederives each key from its stored tag text.

        :param id_values_map: Mapping from row IDs to replacement scalar values.
        :param field: Column to update.
        :param table: Optional expected table name.
        :return: None, including for an empty mapping.
        """
        if not id_values_map:
            return
        if any(isinstance(value, dict) for value in id_values_map.values()):
            raise NotImplementedError("Multi-column batch updates are unsupported")
        if field is None:
            raise InputIntegrityError("A column is required for nonempty updates")
        target_table = driver_operations(self).direct_identify_table_from_column(field)
        if table is not None and table != target_table:
            default_log.warning(
                f"Column {field!r} belongs to {target_table!r}, not {table!r}"
            )
        id_column = driver_operations(self).direct_get_id_column(target_table)
        try:
            with closing(driver_operations(self).get_connection()) as conn:
                with transaction_scope(conn, owns_transaction=True):
                    for row_id, value in id_values_map.items():
                        self._update_row_values(
                            conn, target_table, id_column, row_id, {field: value}
                        )
        except sqlite3.IntegrityError as exc:
            raise DatabaseIntegrityError(
                f"Batch update of {target_table!r} failed: {exc}"
            ) from exc
        except sqlite3.Error as exc:
            raise DatabaseDriverError(
                f"Batch update of {target_table!r} failed: {exc}"
            ) from exc

    def direct_update_row_dict(self, row_dict: dict[str, Any]) -> Literal[True] | None:
        """Update a copied payload without coercing literal text to SQL NULL.

        Key-only normalized updates read the missing display value on this write
        connection. The operation commits on success and closes on every exit.

        Example:
            Updating ``work_title`` to ``"None"`` preserves that string; Python
            ``None`` still requests SQL NULL.

        :param row_dict: Column/value mapping including the target row's ID.
        :return: True for an ID-only no-op; otherwise None.
        """
        row_dict = deepcopy(dict(row_dict))
        target_table = driver_operations(self).direct_identify_table_from_row(row_dict)
        if target_table is False:
            raise InputIntegrityError("Cannot update a row without an identified table")
        id_column = driver_operations(self).direct_get_id_column(target_table)
        if id_column not in row_dict:
            raise RowIntegrityError(
                f"Update of {target_table!r} requires {id_column!r}"
            )
        row_id = row_dict.pop(id_column)
        if not row_dict:
            return True
        try:
            with closing(driver_operations(self).get_connection()) as conn:
                with transaction_scope(conn, owns_transaction=True):
                    self._update_row_values(
                        conn, target_table, id_column, row_id, row_dict
                    )
        except sqlite3.IntegrityError as exc:
            raise DatabaseIntegrityError(
                f"Update of {target_table!r} failed: {exc}"
            ) from exc
        except sqlite3.Error as exc:
            raise DatabaseDriverError(
                f"Update of {target_table!r} failed: {exc}"
            ) from exc

        return None

    def _update_row_values(
        self,
        conn: RowSQLConnection,
        table: str,
        id_column: str,
        row_id: Any,
        values: dict[str, Any],
    ) -> None:
        """Derive and update supplied columns inside the caller's transaction.

        :param conn: Connection whose transaction is owned by the calling writer.
        :param table: Inferred table name.
        :param id_column: Table's identity column.
        :param row_id: Bound ID to update.
        :param values: Supplied changes, copied by normalization.
        :return: None; affected-row counts retain the legacy writer contract.
        """
        payload = prepare_normalized_values(
            driver_operations(self),
            table,
            values,
            read_existing=lambda: driver_operations(self)._read_row_for_update(
                conn, table, id_column, row_id
            ),
        )
        assignments = ", ".join(f'"{column}" = ?' for column in payload)
        conn.execute(
            f'UPDATE "{table}" SET {assignments} WHERE "{id_column}" = ?',
            (*payload.values(), row_id),
        )
