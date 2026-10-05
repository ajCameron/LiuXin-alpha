
"""
Provide the legacy insert-then-search helper for custom-column values.

Column names come from the driver wrapper. The helper attempts an insertion even for an
existing value, suppresses only DatabaseDriverError from that insertion, and returns the
lowest ID matching raw SQL equality. Schema constraints, wrapper behavior and caller
transaction ownership determine whether this acts as deduplication.
"""

from __future__ import annotations

import json

from typing import Any, TYPE_CHECKING

from LiuXin_alpha.utils.libraries.liuxin_six import iteritems

from LiuXin_alpha.utils.language_tools import plural_singular_mapper

from LiuXin_alpha.errors import DatabaseDriverError

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.database_api import DatabaseAPI




class CustomColumnsEnsureValueMacrosMixin:
    """
    Supply custom-value insertion and ID lookup to a host with a driver wrapper.

    The host must provide db.driver_wrapper with display/ID-column lookup, execute and
    get operations. Construction adds no database binding, schema or transaction. This
    mixin handles values in existing tables; custom-column definition management lives
    in a separate mixin.

    Example:
        A host with db.driver_wrapper can call
        ``host.ensure_custom_column_value("custom_column_1", "A")``
        to attempt insertion and return the first matching ID.
    """

    db: "DatabaseAPI"

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - CUSTOM COLUMN MACROS
    def ensure_custom_column_value(self, cc_table: str, value: Any) -> Any:
        """
        Attempt to insert a custom value, then return the lowest ID matched by raw
        equality.

        Ask the wrapper for display and ID columns before writing. Insert the value and
        suppress only DatabaseDriverError from that execution; other failures propagate.
        Always query matching IDs in ascending order and return result[0][0]. No match
        raises IndexError for a normal row-list result. In particular, value=None is
        compared with = and cannot match SQL NULL.

        Existing values still trigger an insert attempt. Without a uniqueness constraint
        this can create another row while returning an older ID. No normalization,
        casefolding, explicit commit, rollback or cache invalidation is added. Table and
        wrapper-derived column names are trusted SQL identifiers.

        Example:
            >>> import sqlite3
            >>> from types import SimpleNamespace
            >>> conn = sqlite3.connect(":memory:")
            >>> _ = conn.execute("CREATE TABLE entries (id INTEGER PRIMARY KEY, value TEXT)")
            >>> wrapper = SimpleNamespace(get_display_column=lambda table: "value", get_id_column=lambda table: "id", execute=conn.execute, get=lambda sql, params: conn.execute(sql, params).fetchall())
            >>> macros = CustomColumnsEnsureValueMacrosMixin()
            >>> macros.db = SimpleNamespace(driver_wrapper=wrapper)
            >>> macros.ensure_custom_column_value("entries", "A")
            1
            >>> macros.ensure_custom_column_value("entries", "A")
            1
            >>> conn.execute("SELECT COUNT(*) FROM entries").fetchone()[0]
            2
            >>> conn.close()


        :param cc_table: Trusted existing custom-value table passed to wrapper column
            lookups and interpolated into SQL.
        :param value: Raw value used for both insertion and SQL equality lookup.
        :return: The first cell of the first ID-ordered matching row; it need not
            identify a newly inserted row.
        """
        # Todo: Hopefully? Making a value column method would be good here.
        cc_val_column = self.db.driver_wrapper.get_display_column(cc_table)
        cc_id_column = self.db.driver_wrapper.get_id_column(cc_table)

        # Todo: The custom columns table have a different structure - account for it
        try:
            insert_stmt = "INSERT INTO {0} ({1}) VALUES (?);".format(cc_table, cc_val_column)
            self.db.driver_wrapper.execute(insert_stmt, (value,))
        except DatabaseDriverError:
            pass

        search_stmt = "SELECT {0} FROM {1} WHERE {2} = ? ORDER BY {0};".format(cc_id_column, cc_table, cc_val_column)
        return self.db.driver_wrapper.get(search_stmt, (value,))[0][0]
