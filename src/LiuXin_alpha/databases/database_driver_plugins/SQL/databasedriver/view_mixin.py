
"""
Read SQLite-compatible view headings and retrieve a typed row by its id column.

Concrete drivers supply connections and row conversion. These helpers interpolate
trusted view names directly into SQL, without quoting or validating them. They do
not create or alter views. Row lookup requires a column literally named id and
checks that the query produces at most one row; PRAGMA heading lookup can also
inspect a table and does not verify that its target is a view.
"""

from typing import Any, Optional

from LiuXin_alpha.utils.libraries.liuxin_six import force_cmp, user_input, force_unicode

from LiuXin_alpha.utils.logging import default_log

from LiuXin_alpha.errors import DatabaseIntegrityError


class ViewMixin:
    """
    Add view introspection and id-based row lookup to a composed SQL driver.

    The host provides get_connection and _row_to_dict; row conversion may itself
    require declared-type introspection. The headings helper does not explicitly
    close its connection. Row lookup closes its own connection on completed searches,
    including missing or duplicate matches, but has no finally cleanup on errors.

    Example:
        With a concrete driver and a view exposing an ``id`` column,
        ``driver.direct_get_view_row_dict_from_id("book_names", 1)`` retrieves
        the corresponding converted row or returns None if no row matches.
    """
    def direct_get_view_row_dict_from_id(self, view: str, row_id: int) -> Optional[dict[str, Any]]:
        """
        Retrieve at most one view row by a bound, text-coerced id value.

        Convert both arguments with force_unicode, currently str. Interpolate the
        view name and query a column literally named id, binding the id as data.
        Fetch headings through direct_get_view_column_headings and convert every
        match through _row_to_dict with the view as its declared-type lookup key.
        Conversion may therefore turn numeric cells into ints or floats.

        Consume all matches before deciding the result. Log and return None for
        zero rows; log and raise DatabaseIntegrityError for multiple rows. Close
        this query's connection on these paths and on success. Query, heading,
        conversion or logging errors propagate without guaranteed cleanup; the
        headings helper's separate connection is not explicitly closed here.

        Example:
            A view defined as ``SELECT book_id AS id, title FROM books`` supports
            ``driver.direct_get_view_row_dict_from_id("book_names", 1)``.
            A view exposing only ``book_id`` needs the ``id`` alias first.


        :param view: Trusted view spelling suitable for direct SQL interpolation;
            converted to text without identifier validation or quoting.
        :param row_id: Identifier converted to text and bound to the id predicate;
            the annotated int type is not enforced at runtime.
        :return: A new heading-to-converted-value dictionary, or None if no row
            matches. Duplicate matches raise DatabaseIntegrityError.
        """
        view = force_unicode(view)
        row_id = force_unicode(row_id)

        conn = self.get_connection()
        c = conn.cursor()

        headings = self.direct_get_view_column_headings(view)
        table_id_name = "id"

        stmt = "SELECT * FROM {} WHERE {} = ?".format(view, table_id_name)
        rows = []
        result = dict()
        for row in c.execute(stmt, (row_id,)):
            # Use the same typed coercion as table-row helpers (INTEGER stays int, etc.)
            result = self._row_to_dict(table=view, headings=headings, row=row)
            rows.append(result)

        if len(rows) > 1:
            err_str = "Error - search yielded multiple rows. Aborting.\n"
            err_str += repr(rows)
            default_log.error(err_str)
            conn.close()
            raise DatabaseIntegrityError(err_str)

        elif len(rows) == 0:
            info_str = "Warning - search yielded no results. Consider sources of logical error."
            default_log.log_variables(info_str, "INFO", ("table", view), ("row_id", row_id))
            conn.close()
            return None

        else:
            conn.close()
            return result

    # Todo: We, nominally, know all the views in the schema ... so we can type this
    def direct_get_view_column_headings(self, view: str) -> list[str]:
        """
        Read relation column names in the order returned by PRAGMA TABLE_INFO.

        Interpolate the supplied name directly, without canonicalization, quoting
        or identifier validation. Tables also work, and an unknown relation
        normally produces an empty list. Only column names are retained from
        the PRAGMA rows. SQL errors propagate. This helper does not explicitly
        close its cursor or connection, even after successful iteration.

        Example:
            >>> import sqlite3
            >>> from types import SimpleNamespace
            >>> conn = sqlite3.connect(":memory:")
            >>> _ = conn.execute("CREATE VIEW book_names AS SELECT 1 AS id, 'A' AS title")
            >>> host = SimpleNamespace(get_connection=lambda: conn)
            >>> ViewMixin.direct_get_view_column_headings(host, "book_names")
            ['id', 'title']
            >>> ViewMixin.direct_get_view_column_headings(host, "missing_view")
            []
            >>> conn.execute("SELECT 1").fetchone()
            (1,)
            >>> conn.close()


        :param view: Trusted relation spelling suitable for direct PRAGMA
            interpolation; no check distinguishes tables from views.
        :return: A fresh list of column names, possibly empty.
        """
        # Todo: Add checking against injection attacks
        stmt = "PRAGMA TABLE_INFO({})".format(view)

        conn = self.get_connection()
        c = conn.cursor()

        view_columns = []
        for i in c.execute(stmt):
            view_columns.append(i[1])

        return view_columns
