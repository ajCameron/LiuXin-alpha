"""
Read SQLite-compatible view headings and retrieve a typed row by its id column.

Concrete drivers supply connections and row conversion. Both helpers quote
relation names and preserve borrowed connections. Neither creates or alters views. Row lookup requires a column literally named id and
checks that the query produces at most one row; PRAGMA heading lookup can also
inspect a table and does not verify that its target is a view.
"""

from typing import Any

from LiuXin_alpha.databases.db_types.driver_types import QueryConnection


from LiuXin_alpha.databases.database_driver_plugins.SQL.databasedriver._state import (
    SQLDriverMixinState,
    driver_operations,
)


# Todo: The ability to create custom views?
# Todo: Rename to DatabaseDriverViewMixin
class ViewMixin(SQLDriverMixinState):
    """
    Add view introspection and id-based row lookup to a composed SQL driver.

    The host provides connection access, native schema inspection and value
    conversion. Reads borrow the supplied or primary handle, falling back to a
    temporary owned connection. Cursors and owned connections close on every path,
    including query/conversion failures, missing rows and duplicate IDs.

    Example:
        With a concrete driver and a view exposing an ``id`` column,
        ``driver.direct_get_view_row_dict_from_id("book_names", 1)`` retrieves
        the corresponding converted row or returns None if no row matches.
    """

    # Todo: Add the ability to enumerate all views
    def direct_get_view_row_dict_from_id(
        self, view: str, row_id: int, *, connection: QueryConnection | None = None
    ) -> dict[str, Any] | None:
        """Read at most one view row by its literal id column, on one connection.

        Quote the relation name and inspect/convert it on the same schema snapshot.
        Fetch at most two rows; duplicates raise DatabaseIntegrityError, missing
        matches return None. Always close cursors and only owned connections.
        Borrowed writes and transaction ownership remain with the caller.
        """
        from LiuXin_alpha.databases.view_reads import read_view_row

        return read_view_row(
            driver_operations(self), view, row_id, connection=connection
        )

    # Todo: We, nominally, know all the views in the schema ... so we can type this
    def direct_get_view_column_headings(self, view: str) -> list[str]:
        """
        Read relation column names in the order returned by PRAGMA TABLE_INFO.

        Quote the supplied name as a SQLite identifier. Tables also work; an
        unknown relation normally yields an empty list. Inspect the primary
        connection's pending schema without committing or closing it. When no
        primary exists, open and close a temporary connection. Always close the
        cursor, including on failure; SQL errors propagate.

        Example:
            >>> import sqlite3
            >>> from types import SimpleNamespace
            >>> conn = sqlite3.connect(":memory:")
            >>> _ = conn.execute(
            ...     "CREATE VIEW book_names AS SELECT 1 AS id, 'A' AS title"
            ... )
            >>> host = SimpleNamespace(conn=conn)
            >>> ViewMixin.direct_get_view_column_headings(host, "book_names")
            ['id', 'title']
            >>> ViewMixin.direct_get_view_column_headings(host, "missing_view")
            []
            >>> conn.execute("SELECT 1").fetchone()
            (1,)
            >>> conn.close()


        :param view: Relation name; no check distinguishes tables from views.
        :return: A fresh list of column names, possibly empty.
        """
        # Table and view discovery must inspect the same pending schema snapshot.
        conn = getattr(self, "conn", None)
        owned = conn is None
        if conn is None:
            conn = driver_operations(self).get_connection()
        quoted = '"' + str(view).replace('"', '""') + '"'
        try:
            cursor = conn.execute(f"PRAGMA table_info({quoted})")
            try:
                return [row[1] for row in cursor]
            finally:
                cursor.close()
        finally:
            if owned:
                conn.close()
