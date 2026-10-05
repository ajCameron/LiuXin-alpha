
"""
Provide legacy custom-column value, link and cleanup SQL macros.

These helpers expect trusted schema names and inflected column prefixes, commonly with
_id, _value, _book and _extra suffixes. Names are interpolated without identifier
validation while data is bound. Reads often require the project connection.get
extension; raw sqlite3.Connection does not provide it. Writes have method-specific
commit behavior and do not share an automatic transaction or cache-invalidation
boundary. Inherited mixins supply column management and insert-then-search value lookup.
"""

import datetime
import json
import sqlite3
import types
from typing import Optional, Union, Any, Iterable

from LiuXin_alpha.utils.libraries.liuxin_six import iteritems

from LiuXin_alpha.utils.language_tools import plural_singular_mapper

from LiuXin_alpha.databases.database_driver_plugins.SQL.macros.cc_macros_mixin.cc_management_macros import (
    CustomColumnsManagementMacrosMixin)
from LiuXin_alpha.databases.database_driver_plugins.SQL.macros.cc_macros_mixin.cc_ensure_values_mixin import (
    CustomColumnsEnsureValueMacrosMixin,
)



class SQLiteDatabaseCustomColumnMacros(
    CustomColumnsEnsureValueMacrosMixin,
    CustomColumnsManagementMacrosMixin,
):
    """
    Compose legacy custom-column reads, writes and cleanup with management and value-
    lookup mixins.

    The surrounding macro facade supplies db and, for some methods, execute; this class
    has no initializer that attaches a database. Read results preserve the chosen
    connection.get semantics: project SQLite returns row lists by default and the first
    cell or None for all=False. The methods do not generally validate schema,
    cardinality or value types, and return annotations may describe a narrower shape
    than the actual result.

    Example:
        >>> macros = SQLiteDatabaseCustomColumnMacros()
        >>> macros._get_cc_id_val("custom_column_1")
        ('custom_column_1_id', 'custom_column_1_value')
        >>> from types import SimpleNamespace
        >>> from LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver import SQLite_Connection
        >>> conn = sqlite3.connect(":memory:", factory=SQLite_Connection)
        >>> _ = conn.execute("CREATE TABLE custom_column_1 (custom_column_1_id INTEGER PRIMARY KEY, custom_column_1_value TEXT)")
        >>> macros.db = SimpleNamespace(driver=SimpleNamespace(conn=conn))
        >>> macros.insert_multiple_values_into_cc_table("custom_column_1", ("A", "B"), conn=conn)
        >>> macros.get_all_cc_id_val_pairs("custom_column_1", conn=conn)
        [(1, 'A'), (2, 'B')]
        >>> macros.get_cc_id_from_value("custom_column_1", "B", conn=conn)
        2
        >>> macros.get_cc_id_and_value_from_id("custom_column_1", 1, conn=conn)
        (1, 'A')
        >>> macros.direct_get_custom_tables(conn=conn)
        {'custom_column_1'}
        >>> conn.close()
    """
    @staticmethod
    def _get_cc_id_val(custom_column: str) -> tuple[str, str]:
        """
        Derive the legacy ID and value column names from an inflected table name.

        Delegate singularization to plural_singular_mapper and append _id and _value.
        This performs no database lookup or identifier validation; exceptional names
        follow the inflector's rules.

        Example:
            >>> SQLiteDatabaseCustomColumnMacros._get_cc_id_val("tags")
            ('tag_id', 'tag_value')


        :param custom_column: Trusted custom-table name used to derive its ID/value
            column prefixes.
        :return: A pair of generated (id_column, value_column) strings.
        """
        cc_col = plural_singular_mapper(custom_column)
        return "{}_id".format(cc_col), "{}_value".format(cc_col)

    @staticmethod
    def _cc_table_col_mapper(table: str) -> str:
        """
        Return the shared inflector's singular form used as a custom-column prefix.

        No schema lookup, suffix removal or separate identifier validation is performed.

        Example:
            >>> SQLiteDatabaseCustomColumnMacros._cc_table_col_mapper("custom_column_1")
            'custom_column_1'


        :param table: Trusted custom-table name, interpolated into SQL; column prefixes
            come from plural_singular_mapper.
        :return: The prefix returned by plural_singular_mapper.
        """
        return plural_singular_mapper(table)

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - READ

    def get_dirtied_cache(self):
        """
        Map dirty book IDs to enumeration positions while accommodating two queue
        schemas.

        Inspect metadata_dirtied_books using PRAGMA. Prefer the legacy
        metadata_dirtied_book column when present; otherwise read non-null
        metadata_dirtied_table_id values, filtering table=books when the table
        discriminator exists. Failed introspection or an unknown layout returns an empty
        dictionary; errors during the selected query propagate. Query order is
        unspecified, duplicate IDs keep their last enumeration position, and legacy null
        IDs are retained.

        Example:
            >>> from types import SimpleNamespace
            >>> conn = sqlite3.connect(":memory:")
            >>> _ = conn.execute("CREATE TABLE metadata_dirtied_books (metadata_dirtied_table TEXT, metadata_dirtied_table_id INTEGER)")
            >>> _ = conn.executemany("INSERT INTO metadata_dirtied_books VALUES (?, ?)", [("books", 7), ("tags", 8), ("books", None)])
            >>> macros = SQLiteDatabaseCustomColumnMacros()
            >>> macros.db = SimpleNamespace(driver_wrapper=SimpleNamespace(execute=conn.execute))
            >>> macros.get_dirtied_cache()
            {7: 0}
            >>> conn.close()


        :return: A dictionary of dirty IDs to zero-based positions, possibly empty.
        """
        # Schema drift handling:
        #   legacy: metadata_dirtied_books(metadata_dirtied_book)
        #   current: metadata_dirtied_books(metadata_dirtied_table, metadata_dirtied_table_id)

        try:
            cols = [row[1] for row in self.db.driver_wrapper.execute("PRAGMA table_info(metadata_dirtied_books)")]
        except Exception:
            cols = []

        if "metadata_dirtied_book" in cols:
            stmt = "SELECT metadata_dirtied_book FROM metadata_dirtied_books"
        elif "metadata_dirtied_table_id" in cols:
            # Prefer book dirties if the table distinguishes them, otherwise take all.
            if "metadata_dirtied_table" in cols:
                stmt = (
                    "SELECT metadata_dirtied_table_id FROM metadata_dirtied_books "
                    "WHERE metadata_dirtied_table='books' AND metadata_dirtied_table_id IS NOT NULL"
                )
            else:
                stmt = "SELECT metadata_dirtied_table_id FROM metadata_dirtied_books WHERE metadata_dirtied_table_id IS NOT NULL"
        else:
            # Best effort: empty cache when table is absent or unknown.
            return {}

        dirtied_cache = {x: i for i, (x,) in enumerate(self.db.driver_wrapper.execute(stmt))}
        return dirtied_cache

    def get_cc_id_and_value_from_id(
            self, custom_column: str,
            target_id: int,
            conn: Optional[sqlite3.Connection] = None) -> tuple[int, str]:
        """
        Read the first ID/value row matching one custom-value ID.

        Use the supplied connection or db.driver.conn and index the default get result
        at zero. On project SQLite this returns a two-cell row and raises IndexError
        when no row matches. Additional matches are silently ignored and no ordering or
        transaction is added.

        Example:
            With a compatible connection, ``macros.get_cc_id_and_value_from_id(
            "custom_column_1", 7, conn=conn)`` returns its ID and stored value.


        :param custom_column: Trusted custom-table name used to derive its ID/value
            column prefixes.
        :param target_id: ID bound to the selected row or link predicate.
        :param conn: Optional compatible connection; None uses the default path
            described above. Connections are not closed here.
        :return: The first row from connection.get, normally an (id, value) tuple.
        """
        if conn is None:
            cc_id_col, cc_val_col = self._get_cc_id_val(custom_column)
            return self.db.driver.conn.get(
                "SELECT {0}, {1} FROM {2} WHERE {0}=?" "".format(cc_id_col, cc_val_col, custom_column),
                (target_id,),
            )[0]
        else:
            cc_id_col, cc_val_col = self._get_cc_id_val(custom_column)
            return conn.get(
                "SELECT {0}, {1} FROM {2} WHERE {0}=?" "".format(cc_id_col, cc_val_col, custom_column),
                (target_id,),
            )[0]

    # Todo: Basically the same as the above method - merge
    def get_cc_id_value_from_cc_id(
            self, table: str, old_id: int
    ) -> tuple[int, str]:
        """
        Read the first ID/value row for an ID through the stored default connection.

        Derive both column names and index the default get result at zero. Missing rows
        raise IndexError with the project SQLite adapter; duplicate matches are not
        checked. This is a legacy counterpart to get_cc_id_and_value_from_id without a
        connection override.

        Example:
            ``macros.get_cc_id_value_from_cc_id("custom_column_1", 7)``
            returns the first matching row from db.driver.conn.


        :param table: Trusted custom-table name, interpolated into SQL; column prefixes
            come from plural_singular_mapper.
        :param old_id: Existing custom-value ID bound to the ID-column predicate.
        :return: The first matching two-cell row.
        """
        cc_id_col, cc_val_col = self._get_cc_id_val(table)
        return self.db.driver.conn.get(
            "SELECT {id_col}, {val_col} FROM {table} WHERE {id_col}=?".format(
                id_col=cc_id_col, val_col=cc_val_col, table=table
            ),
            (old_id,),
        )[0]

    def get_cc_id_from_value(
            self,
            target_table: str,
            cc_value: Union[str, int, datetime.datetime],
            all: bool = False,
            conn: Optional[sqlite3.Connection] = None
    ) -> int:
        """
        Look up custom-value IDs using SQL equality and the requested get result mode.

        Bind the raw value without normalization, explicit collation or ordering. Pass
        all through unchanged: project SQLite returns the first ID or None for false,
        and a list of one-cell rows for true. None is bound to equality and does not
        become IS NULL; duplicate matches are not rejected.

        Example:
            >>> from types import SimpleNamespace
            >>> conn = SimpleNamespace(get=lambda sql, params, **kw: [(4,)] if kw["all"] else 4)
            >>> macros = SQLiteDatabaseCustomColumnMacros()
            >>> macros.get_cc_id_from_value("custom_column_1", "A", conn=conn)
            4
            >>> macros.get_cc_id_from_value("custom_column_1", "A", all=True, conn=conn)
            [(4,)]


        :param target_table: Trusted custom-table name whose inflected prefix supplies
            ID/value columns.
        :param cc_value: Value bound without normalization or case conversion.
        :param all: Forwarded get mode: false selects the first cell; true selects all
            rows on project SQLite.
        :param conn: Optional compatible connection; None uses the default path
            described above. Connections are not closed here.
        :return: The connection.get result, whose shape follows all rather than the int
            annotation.
        """
        cc_id_col, cc_val_col = self._get_cc_id_val(target_table)
        if conn is None:
            return self.db.driver.conn.get(
                "SELECT {id_col} FROM {table} WHERE {val_col}=?".format(
                    id_col=cc_id_col, table=target_table, val_col=cc_val_col
                ),
                (cc_value,),
                all=all,
            )
        else:
            return conn.get(
                "SELECT {id_col} FROM {table} WHERE {val_col}=?".format(
                    id_col=cc_id_col, table=target_table, val_col=cc_val_col
                ),
                (cc_value,),
                all=all,
            )

    # Todo: Needs a new name in line with the extensions of custom columns to all table
    def get_cc_lt_books_from_lt_value(
            self,
            lt: str,
            value: Union[str, int, datetime.datetime],
            conn: Optional[sqlite3.Connection] = None
    ) -> Iterable[int]:
        """
        Read owner IDs linked to one custom-value ID using legacy _book/_value columns.

        The value argument denotes the referenced row ID, not its display text. Return
        the connection's default get result without flattening, sorting or
        deduplication; project SQLite returns one-cell rows. No normalization or schema
        discovery occurs.

        Example:
            ``macros.get_cc_lt_books_from_lt_value("books_custom_column_1_link",
            7, conn=conn)`` reads owners linked to value row 7.


        :param lt: Trusted legacy link-table name with inflected _book and _value column
            names.
        :param value: Referenced custom-value ID bound to the link _value column.
        :param conn: Optional compatible connection; None uses the default path
            described above. Connections are not closed here.
        :return: The get result, normally a list of (book_id,) rows.
        """
        lt_col = plural_singular_mapper(lt)

        if conn is None:
            # return self.db.driver.conn.get('SELECT book from %s WHERE value=?;' % lt, (value,))
            return self.db.driver.conn.get(
                "SELECT {lt_col}_book from {lt} WHERE {lt_col}_value=?;" "".format(lt=lt, lt_col=lt_col),
                (value,),
            )
        else:
            return conn.get(
                "SELECT {lt_col}_book from {lt} WHERE {lt_col}_value=?;" "".format(lt=lt, lt_col=lt_col),
                (value,),
            )

    def get_all_cc_custom_values(
            self,
            cc_table: str,
            distinct: bool = False,
            conn: Optional[sqlite3.Connection] = None
    ) -> Iterable[Union[int, str, float]]:
        """
        Read every legacy _value cell, optionally applying SQL DISTINCT.

        With distinct false, pass all=True explicitly. With distinct true, omit the all
        argument and rely on the connection default. Project SQLite therefore returns
        row lists in both cases; values are not flattened. No ordering, normalization or
        conversion is requested, and link tables return referenced IDs rather than
        display text.

        Example:
            ``macros.get_all_cc_custom_values("custom_column_1", distinct=True,
            conn=conn)`` returns distinct stored values as one-cell rows on project SQLite.


        :param cc_table: Trusted table with an inflected _value column.
        :param distinct: Use SELECT DISTINCT when truthy; otherwise retain duplicate
            rows.
        :param conn: Optional compatible connection; None uses the default path
            described above. Connections are not closed here.
        :return: The connection.get row container.
        """
        cc_col = plural_singular_mapper(cc_table)

        if not distinct:
            if conn is None:
                return self.db.driver.conn.get(
                    "SELECT {cc_col}_value FROM {table}" "".format(table=cc_table, cc_col=cc_col),
                    all=True,
                )
            else:
                return conn.get(
                    "SELECT {cc_col}_value FROM {table}" "".format(table=cc_table, cc_col=cc_col),
                    all=True,
                )
        else:
            if conn is None:
                return self.db.driver.conn.get(
                    "SELECT DISTINCT {cc_col}_value FROM {table}" "".format(table=cc_table, cc_col=cc_col)
                )
            else:
                return conn.get("SELECT DISTINCT {cc_col}_value FROM {table}" "".format(table=cc_table, cc_col=cc_col))

    def get_cc_series_index_indices(
            self, cc_series_link_table: str, series_id: int, conn: Optional[sqlite3.Connection] = None
    ) -> tuple[Union[float, int], ...]:
        """
        Read ordered extra values for every owner linked to the requested series ID.

        First select owners having the requested _value, then select all their _extra
        rows in ascending order. The outer query does not restrict _value to series_id,
        so an owner linked to several series contributes extras from those other rows
        too. Return the connection's default row container with duplicates and nulls
        retained.

        Example:
            If one book links to series 7 at index 2 and series 8 at index 4,
            requesting series 7 can include both extras because the outer filter is by book.


        :param cc_series_link_table: Trusted legacy series link table with _book, _value
            and _extra columns.
        :param series_id: Series row ID used only in the owner-selecting subquery.
        :param conn: Optional compatible connection; None uses the default path
            described above. Connections are not closed here.
        :return: The get result, normally one-cell extra rows in ascending SQL order.
        """
        lt_col = plural_singular_mapper(cc_series_link_table)

        if conn is None:
            return self.db.driver.conn.get(
                "SELECT {lt}.{lt_col}_extra "
                "FROM {lt} "
                "WHERE {lt}.{lt_col}_book IN "
                "(SELECT {lt_col}_book FROM {lt} where {lt_col}_value=?) "
                "ORDER BY {lt}.{lt_col}_extra".format(lt=cc_series_link_table, lt_col=lt_col),
                (series_id,),
            )
        else:
            return conn.get(
                "SELECT {lt}.{lt_col}_extra "
                "FROM {lt} "
                "WHERE {lt}.{lt_col}_book IN "
                "(SELECT {lt_col}_book FROM {lt} where {lt_col}_value=?) "
                "ORDER BY {lt}.{lt_col}_extra".format(lt=cc_series_link_table, lt_col=lt_col),
                (series_id,),
            )

    # Todo: Will sometimes yield unexpected reuslts - so checking to make sure it's being used as expected would be appropriate
    # Todo: This doesn't work on non-normalized tables - might want to update?
    def check_for_cc_link(
            self,
            link_table: str,
            book_id: int,
            value_id: int,
            conn: Optional[sqlite3.Connection] = None) -> bool:
        """
        Return the first owner cell matching an owner/value link pair.

        Use get(all=False) without converting its result to bool. Project SQLite returns
        the matching book ID or None; a matching ID of zero is therefore falsey.
        Multiple matches are not counted, and the caller must supply a table with legacy
        link columns.

        Example:
            >>> from types import SimpleNamespace
            >>> conn = SimpleNamespace(get=lambda *args, **kwargs: 0)
            >>> result = SQLiteDatabaseCustomColumnMacros().check_for_cc_link("links", 0, 7, conn=conn)
            >>> result, bool(result)
            (0, False)


        :param link_table: Trusted legacy link-table name with inflected column
            prefixes.
        :param book_id: Book or legacy owner ID bound as a query value.
        :param value_id: Custom-value ID bound to the link value column.
        :param conn: Optional compatible connection; None uses the default path
            described above. Connections are not closed here.
        :return: The selected owner cell or the adapter's missing-value result, not
            necessarily bool.
        """
        lt_col = plural_singular_mapper(link_table)

        if conn is None:
            return self.db.driver.conn.get(
                "SELECT {lt_col}_book FROM {link_table} WHERE {lt_col}_book=? AND {lt_col}_value=?"
                "".format(link_table=link_table, lt_col=lt_col),
                (book_id, value_id),
                all=False,
            )
        else:
            return conn.get(
                "SELECT {lt_col}_book FROM {link_table} WHERE {lt_col}_book=? AND {lt_col}_value=?"
                "".format(link_table=link_table, lt_col=lt_col),
                (book_id, value_id),
                all=False,
            )

    def read_cc_value_from_meta_2(
            self,
            num: int,
            book_id: int,
            conn: Optional[sqlite3.Connection] = None
    ) -> Iterable[Union[int, str, float]]:
        """
        Read the first custom_N cell from the legacy meta2 relation for one book.

        Interpolate num into the selected column name and bind book_id. Forward
        all=False, returning the scalar or None with project SQLite. This neither
        discovers available custom fields nor decodes strings or containers stored in
        the cell.

        Example:
            ``macros.read_cc_value_from_meta_2(3, 7, conn=conn)`` selects
            custom_3 from meta2 for ID 7.


        :param num: Trusted custom-column suffix interpolated into custom_<num>; no
            integer coercion occurs.
        :param book_id: Book or legacy owner ID bound as a query value.
        :param conn: Optional compatible connection; None uses the default path
            described above. Connections are not closed here.
        :return: The first selected cell according to the connection.get contract.
        """
        if conn is None:
            return self.db.driver.conn.get("SELECT custom_%s FROM meta2 WHERE id=?" % num, (book_id,), all=False)
        else:
            return conn.get("SELECT custom_%s FROM meta2 WHERE id=?" % num, (book_id,), all=False)

    def get_all_cc_id_val_pairs(self, table, conn: Optional[sqlite3.Connection] = None):
        """
        Read ID/value pairs from a custom table using the default get result mode.

        Resolve the optional connection, derive columns through the inflector, and
        select every row without ordering or transformation. Project SQLite returns a
        list of two-cell tuples; empty tables return an empty list.

        Example:
            ``macros.get_all_cc_id_val_pairs("custom_column_1", conn=conn)``
            returns stored (id, value) rows without flattening.


        :param table: Trusted custom-table name, interpolated into SQL; column prefixes
            come from plural_singular_mapper.
        :param conn: Optional compatible connection; None uses the default path
            described above. Connections are not closed here.
        :return: The connection.get result containing ID/value rows.
        """
        cc_id_col, cc_val_col = self._get_cc_id_val(table)

        conn = conn if conn is not None else self.db.driver.conn

        return conn.get(
            "SELECT {id_col}, {val_col} FROM {table}" "".format(id_col=cc_id_col, val_col=cc_val_col, table=table)
        )

    def get_cc_books_from_link_table(self, lt: str, lt_value: Any) -> Iterable[int]:
        """
        Read owners linked to a custom-value ID on the stored default connection.

        Bind the value ID and return the unmodified default get result, normally one-
        cell rows on project SQLite. No explicit ordering, duplicate removal or
        connection override is provided.

        Example:
            ``macros.get_cc_books_from_link_table("books_custom_column_1_link", 7)``
            returns rows naming the owners of custom-value row 7.


        :param lt: Trusted legacy link-table name with inflected _book and _value column
            names.
        :param lt_value: Referenced custom-value ID bound to the link _value column.
        :return: The default connection's owner-row container.
        """
        lt_col = plural_singular_mapper(lt)

        books = self.db.driver.conn.get(
            "SELECT {lt_col}_book from {lt} WHERE {lt_col}_value=?;" "".format(lt=lt, lt_col=lt_col),
            (lt_value,),
        )
        return books

    # Todo: The link is probably not a needed - can work it out from the link table
    # Todo: This probably doesn't work well for generalized custom columns
    # Todo: Deprecate conn - use the stored connections
    def get_cc_books_for_dirtying(
            self, table: str, link: str, id: int, conn: Optional[Any] = None
    ) -> Iterable[str]:
        """
        Read legacy book IDs from the derived books_<table>_link relation.

        Derive the link-table name from table, then append link as the predicate-column
        suffix to its inflected prefix. The link argument is a suffix such as value, not
        an independently supplied table name. The table singularization is computed but
        does not affect the relation name. Return rows without flattening, sorting or
        marking them dirty.

        Example:
            ``macros.get_cc_books_for_dirtying("custom_column_1", "value", 7,
            conn=conn)`` reads owners from books_custom_column_1_link.


        :param table: Trusted custom-table name, interpolated into SQL; column prefixes
            come from plural_singular_mapper.
        :param link: Trusted link-column suffix appended to the derived link prefix.
        :param id: Custom-value ID bound to the derived predicate column.
        :param conn: Optional compatible connection; None uses the default path
            described above. Connections are not closed here.
        :return: The connection.get result, normally (book_id,) rows.
        """
        conn = conn if conn is not None else self.db.driver.conn

        table_col = plural_singular_mapper(table)
        lt = "books_{table}_link".format(table=table, table_col=table_col, link=link)
        lt_col = self._cc_table_col_mapper(lt)

        return conn.get(
            "SELECT {lt_col}_book from books_{table}_link WHERE {lt_col}_{link}=?"
            "".format(table=table, lt_col=lt_col, link=link),
            (id,),
        )

    def direct_get_custom_and_extra(self, link_table, index, conn=None):
        """
        Read only the first extra cell for one owner from a legacy link table.

        Despite the method name, the query selects no custom value or ID. Use
        get(all=False), with project SQLite returning the first extra or None. There is
        no ordering, so multiple matching links do not have a defined winner.

        Example:
            ``macros.direct_get_custom_and_extra("books_custom_column_1_link",
            7, conn=conn)`` returns one extra cell for book 7.


        :param link_table: Trusted legacy link-table name with inflected column
            prefixes.
        :param index: Owner ID matched against the link _book column.
        :param conn: Optional compatible connection; None uses the default path
            described above. Connections are not closed here.
        :return: The selected extra scalar or the adapter's missing-value result.
        """
        lt_col = plural_singular_mapper(link_table)

        conn = conn if conn is not None else self.db.driver.conn

        return conn.get(
            "SELECT {lt_col}_extra FROM {lt} WHERE {lt_col}_book=?" "".format(lt=link_table, lt_col=lt_col),
            (index,),
            all=False,
        )

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - WRITE

    def add_cc_table_value(self, table, value, conn=None):
        """
        Insert one custom value and return the selected execution path's new-row result.

        With an explicit connection, execute the INSERT and return cursor.lastrowid
        without committing. With conn=None, delegate to db.driver.direct_execute_sql;
        the shared SQLite driver uses a fresh connection and commits, returning
        lastrowid. The macro does not normalize values, deduplicate rows or invalidate
        caches.

        Example:
            >>> conn = sqlite3.connect(":memory:")
            >>> _ = conn.execute("CREATE TABLE custom_column_1 (custom_column_1_id INTEGER PRIMARY KEY, custom_column_1_value TEXT)")
            >>> SQLiteDatabaseCustomColumnMacros().add_cc_table_value("custom_column_1", "A", conn=conn)
            1
            >>> conn.in_transaction
            True
            >>> conn.close()


        :param table: Trusted custom-table name, interpolated into SQL; column prefixes
            come from plural_singular_mapper.
        :param value: Value bound as supplied, subject to backend conversion and
            constraints.
        :param conn: Optional compatible connection; None uses the default path
            described above. Connections are not closed here.
        :return: The driver helper result for the default path, or cursor.lastrowid for
            an explicit connection.
        """
        cc_table_col = self._cc_table_col_mapper(table)
        if conn is None:
            # This solution was leaving the database locked, but this might be breaking lastrowid
            # return self.db.driver.conn.execute('INSERT INTO %s(value) VALUES(?)'%table, (value,)).lastrowid
            # Todo: not sure lastrowid is entirely thread safe?
            return self.db.driver.direct_execute_sql(
                "INSERT INTO {table}({table_col}_value) VALUES(?)" "".format(table=table, table_col=cc_table_col),
                (value,),
            )
        else:
            # Todo: not sure lastrowid is entirely thread safe?
            conn_rtn = conn.execute(
                "INSERT INTO {table}({table_col}_value) VALUES(?)" "".format(table=table, table_col=cc_table_col),
                (value,),
            ).lastrowid
            return conn_rtn

    # Todo: As extra is an optional argument, might want to change the name here
    def add_cc_link_with_extra(self, lt, book_id, value_id, extra=None, conn=None, target_column="value"):
        """
        Insert one legacy owner/value link, including the extra column only when extra
        is non-None.

        Append target_column to the inflected link prefix. None extra omits the column;
        zero and other falsey non-None values are inserted. Commit the stored connection
        when conn is omitted, but leave an explicit connection uncommitted. Existing
        duplicates and invalid endpoints are handled only by database constraints; no
        transaction rollback or cache invalidation is added.

        Example:
            ``macros.add_cc_link_with_extra("books_custom_column_1_link", 7, 3,
            extra=0, conn=conn)`` inserts zero and leaves commit ownership with the caller.


        :param lt: Trusted legacy link-table name with inflected _book and _value column
            names.
        :param book_id: Book or legacy owner ID bound as a query value.
        :param value_id: Custom-value ID bound to the link value column.
        :param extra: Optional extra value; None omits that column so its database
            default applies.
        :param conn: Optional compatible connection; None uses the default path
            described above. Connections are not closed here.
        :param target_column: Trusted suffix appended to the link prefix, normally
            value; interpolated without validation.
        :return: None; the execute result is discarded.
        """
        lt_col = self._cc_table_col_mapper(lt)

        local_conn = conn if conn is not None else self.db.driver.conn

        if extra is not None:

            extra_stmt = (
                "INSERT INTO {lt}({lt_col}_book, {lt_col}_{target_column}, {lt_col}_extra) VALUES (?,?,?)".format(
                    lt=lt, target_column=target_column, lt_col=lt_col
                )
            )
            local_conn.execute(extra_stmt, (book_id, value_id, extra))
        else:
            # target column should always be value
            stmt = "INSERT INTO {lt} ({lt_col}_book, {lt_col}_{target_column}) VALUES (?,?)".format(
                lt=lt, target_column=target_column, lt_col=lt_col
            )
            local_conn.execute(stmt, (book_id, value_id))

        # If the conn passed in is None, then assume we're in autocommit mode and commit the changes
        # Todo: This is a crude solution - do need to create those semi-private methods which take a conn and give you
        #       the option of auto-commit or not
        if conn is None:
            local_conn.commit()

    # Todo: As extra is an optional argument, might want to change the name here
    # Todo: Should be able to detect the extra or not automatically
    def add_cc_link_with_extra_multi(self, lt, sequence, extra=False, conn=None, target_column="value"):
        """
        Insert legacy links from two- or three-item parameter sequences.

        Truthiness of extra chooses a three-column INSERT; it is a shape flag, not a
        shared extra value. Forward sequence directly to executemany without
        prevalidation or materialization. Each row must provide owner/value or
        owner/value/extra in that order. Commit only when conn is omitted. A later
        binding/constraint failure can leave earlier writes pending and skips the final
        commit.

        Example:
            >>> conn = sqlite3.connect(":memory:")
            >>> _ = conn.execute("CREATE TABLE links (link_book INTEGER, link_value INTEGER, link_extra REAL)")
            >>> macros = SQLiteDatabaseCustomColumnMacros()
            >>> macros.add_cc_link_with_extra_multi("links", [(7, 3, 2.5)], extra=True, conn=conn)
            >>> macros.add_cc_link_with_extra("links", 8, 3, extra=0, conn=conn)
            >>> conn.execute("SELECT * FROM links ORDER BY link_book").fetchall()
            [(7, 3, 2.5), (8, 3, 0.0)]
            >>> conn.in_transaction
            True
            >>> conn.close()


        :param lt: Trusted legacy link-table name with inflected _book and _value column
            names.
        :param sequence: Iterable of two- or three-item binding sequences as selected by
            extra.
        :param extra: Truthy to include a per-row extra binding; falsey to insert only
            owner/value.
        :param conn: Optional compatible connection; None uses the default path
            described above. Connections are not closed here.
        :param target_column: Trusted suffix appended to the link prefix, normally
            value; interpolated without validation.
        :return: None; the executemany result is discarded.
        """
        lt_col = self._cc_table_col_mapper(lt)
        local_conn = conn if conn is not None else self.db.driver.conn

        if extra:

            extra_stmt = (
                "INSERT INTO {lt}({lt_col}_book, {lt_col}_{target_column}, {lt_col}_extra) VALUES (?,?,?)"
                "".format(lt=lt, lt_col=lt_col, target_column=target_column)
            )
            local_conn.executemany(extra_stmt, sequence)
        else:

            stmt = "INSERT INTO {lt} ({lt_col}_book, {lt_col}_{target_column}) VALUES (?,?)" "".format(
                lt=lt, lt_col=lt_col, target_column=target_column
            )
            local_conn.executemany(stmt, sequence)

        # If the conn passed in is None, then assume we're in autocommit mode and commit the changes
        # Todo: This is a crude solution - do need to create those semi-private methods which take a conn and give you
        #       the option of auto-commit or not
        if conn is None:
            local_conn.commit()

    # Todo: Merge into add_cc_table_value - with the different of the value being an iterable
    def insert_multiple_values_into_cc_table(self, table, values, conn=None):
        """
        Materialize scalar values into one-item bindings and append them to a custom
        table.

        Derive the _value column and call executemany on the explicit or stored
        connection. Input iteration completes before execution; later binding/constraint
        errors may leave earlier rows written. No deduplication, normalization, commit
        or rollback is performed here.

        Example:
            ``macros.insert_multiple_values_into_cc_table("custom_column_1",
            ("A", "B"), conn=conn)`` appends both values without an explicit commit.


        :param table: Trusted custom-table name, interpolated into SQL; column prefixes
            come from plural_singular_mapper.
        :param values: Iterable of scalar values fully materialized before the database
            call.
        :param conn: Optional compatible connection; None uses the default path
            described above. Connections are not closed here.
        :return: None; the executemany result is discarded.
        """
        conn = conn if conn is not None else self.db.driver.conn

        table_col = self._cc_table_col_mapper(table)

        conn.executemany(
            "INSERT INTO {table}({table_col}_value) VALUES (?)" "".format(table=table, table_col=table_col),
            [(x,) for x in values],
        )

    # Todo: Can rename this - remove db
    def do_cc_db_bulk_addition(self, temp_tables, custom_table, link_table, add, remove, conn=None):
        """
        Populate prepared add/remove ID tables, then change links for their selected
        owners.

        For each truthy add/remove collection, look up the first existing custom-value
        ID using PYNOCASE and LIMIT 1, inserting it into the corresponding temporary
        table. Missing display values contribute no ID; no custom-value rows are created
        here and temporary tables are not cleared. Duplicate resolved IDs can violate
        their constraints.

        Delete matching owner/removal pairs first, then INSERT OR REPLACE the Cartesian
        product of selected owner and addition IDs. Replacement can discard previous
        extras or other row fields according to SQLite replacement semantics. Names and
        the three-table layout are trusted; no commit, rollback or cleanup is added.

        Example:
            With temp_tables ordered as selected owners, additions and removals,
            ``macros.do_cc_db_bulk_addition(temp_tables, custom_table, link_table,
            ("A",), ("B",), conn=conn)`` removes B links before adding existing A values.


        :param temp_tables: Indexable sequence of three existing tables with id columns:
            owners, additions, removals.
        :param custom_table: Trusted existing custom-value table with inflected _id and
            _value columns.
        :param link_table: Trusted legacy link-table name with inflected column
            prefixes.
        :param add: Display values to resolve with PYNOCASE and add for every selected
            owner.
        :param remove: Display values to resolve with PYNOCASE and remove for every
            selected owner.
        :param conn: Optional compatible connection; None uses the default path
            described above. Connections are not closed here.
        :return: None; temporary-table population and link changes execute on the chosen
            connection.
        """
        conn = conn if conn is not None else self.db.driver.conn
        ct_col = self._cc_table_col_mapper(custom_table)
        lt_col = self._cc_table_col_mapper(link_table)

        for table, tags in enumerate([add, remove]):
            if not tags:
                continue
            table = temp_tables[table + 1]
            insert = (
                "INSERT INTO {tt}(id) SELECT {ct}.{ct_col}_id FROM {ct} WHERE {ct_col}_value=?"
                " COLLATE PYNOCASE LIMIT 1"
            ).format(tt=table, ct=custom_table, ct_col=ct_col)
            conn.executemany(insert, [(x,) for x in tags])

        # now do the real work -- removing and adding the tags
        if remove:
            cc_rmv_stmt = """DELETE FROM {lt} WHERE
                             {lt_col}_book IN (SELECT id FROM {tt1}) AND
                             {lt_col}_value IN (SELECT id FROM {tt2})
                             """.format(
                lt=link_table, lt_col=lt_col, tt1=temp_tables[0], tt2=temp_tables[2]
            )
            conn.execute(cc_rmv_stmt)

        if add:
            conn.execute(
                """
            INSERT OR REPLACE INTO {lt}({lt_col}_book, {lt_col}_value) SELECT {tt1}.id, {tt2}.id FROM {tt1}, {tt2}
            """.format(
                    lt=link_table, lt_col=lt_col, tt1=temp_tables[0], tt2=temp_tables[1]
                )
            )

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - UPDATE

    def update_cc_value(self, cc_column, cc_id, cc_value, conn=None):
        """
        Replace a custom-value cell selected by its legacy ID column.

        With conn=None, call the host's self.execute and discard its result; transaction
        behavior belongs to that host operation. With an explicit connection, return its
        execute result without committing. No row-count check, normalization or identity
        merge occurs.

        Example:
            ``macros.update_cc_value("custom_column_1", 7, "Revised", conn=conn)``
            returns the explicit connection's cursor or execution result.


        :param cc_column: Trusted custom-table name, despite the parameter being called
            a column.
        :param cc_id: ID bound to the custom table's inflected _id predicate.
        :param cc_value: Value bound without normalization or case conversion.
        :param conn: Optional compatible connection; None uses the default path
            described above. Connections are not closed here.
        :return: None on the host-execute path, otherwise the explicit
            connection.execute result.
        """
        cc_col = self._cc_table_col_mapper(cc_column)

        update_stmt = "UPDATE {cc_column} SET {cc_col}_value=? WHERE {cc_col}_id=?".format(
            cc_column=cc_column, cc_col=cc_col
        )

        if conn is None:
            self.execute(update_stmt, (cc_value, cc_id))
        else:
            return conn.execute(update_stmt, (cc_value, cc_id))

    def repoint_cc_lt_values(self, lt, new_id, old_id):
        """
        Replace every matching legacy link value ID through the host execute operation.

        Bind new_id then old_id and update all matching _value cells. The helper neither
        merges duplicate links nor checks row counts; backend constraints may reject the
        change. Commit/rollback behavior is delegated to self.execute.

        Example:
            ``macros.repoint_cc_lt_values("books_custom_column_1_link", 3, 7)``
            repoints all references from value row 7 to row 3.


        :param lt: Trusted legacy link-table name with inflected _book and _value column
            names.
        :param new_id: Replacement custom-value ID.
        :param old_id: Existing custom-value ID to replace.
        :return: None; the host execution result is discarded.
        """
        lt_col = self._cc_table_col_mapper(lt)
        self.execute(
            "UPDATE {lt} SET {lt_col}_value=? WHERE {lt_col}_value=?".format(lt=lt, lt_col=lt_col),
            (
                new_id,
                old_id,
            ),
        )

    # Todo: Rename these two methods to be consistent - decide a pithy name for the src and dst table
    def update_cc_lt_value_by_value(self, lt, new_value_id, old_value_id, conn=None):
        """
        Repoint matching legacy link values on an explicit or stored connection.

        Execute one UPDATE binding the new and old IDs, discard its result and perform
        no explicit commit. All matching rows are updated; constraints determine whether
        duplicate or invalid links are allowed.

        Example:
            ``macros.update_cc_lt_value_by_value("links", 3, 7, conn=conn)``
            changes link_value from 7 to 3 wherever it matches.


        :param lt: Trusted legacy link-table name with inflected _book and _value column
            names.
        :param new_value_id: Replacement link _value ID.
        :param old_value_id: Link _value ID to match.
        :param conn: Optional compatible connection; None uses the default path
            described above. Connections are not closed here.
        :return: None; no affected-row count is returned.
        """
        lt_col = self._cc_table_col_mapper(lt)
        update_stmt = "UPDATE {lt} SET {lt_col}_value=? WHERE {lt_col}_value=?".format(lt=lt, lt_col=lt_col)

        if conn is None:
            self.db.driver.conn.execute(
                update_stmt,
                (
                    new_value_id,
                    old_value_id,
                ),
            )
        else:
            conn.execute(
                update_stmt,
                (
                    new_value_id,
                    old_value_id,
                ),
            )

    def update_custom_column_additional_column_many(self, table, column, sequence):
        """
        Update one suffixed link column for each owner/value pair through
        db.executemany.

        Build the target column by appending column to the inflected table prefix. Pass
        sequence unchanged, expecting (new_cell, owner_id, value_id) for each execution.
        No independent commit, validation or result collection occurs; the host owns
        execution semantics.

        Example:
            ``macros.update_custom_column_additional_column_many("links",
            "extra", [(2.5, 7, 3)])`` updates link_extra for owner 7/value 3.


        :param table: Trusted custom-table name, interpolated into SQL; column prefixes
            come from plural_singular_mapper.
        :param column: Trusted suffix such as extra, appended to the link prefix.
        :param sequence: Iterable of (new_cell, owner_id, value_id) parameter sequences.
        :return: None; the host executemany result is discarded.
        """
        table_col = self._cc_table_col_mapper(table)
        stmt = "UPDATE {table} SET {table_col}_{column}=? WHERE {table_col}_book=? AND {table_col}_value=?".format(
            table=table, table_col=table_col, column=column
        )
        self.db.executemany(stmt, sequence)

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - DELETE

    # Todo: This might want to be replaced with a trigger - probably better
    # Todo: Also needs to be renamed
    # Todo: Rename target_id to custom_id
    def delete_cc_item(self, table, lt, target_id, conn=None):
        """
        Delete all links to a custom value, delete its row, then commit the chosen
        connection.

        Both table and lt are required and have inflected ID/value columns. Delete links
        first and the value second; even an explicit connection is committed, including
        its other pending work. No savepoint or rollback is established, so an
        intermediate failure can leave earlier writes pending. Missing rows are silently
        tolerated.

        Example:
            >>> from types import SimpleNamespace
            >>> calls = []
            >>> conn = SimpleNamespace(execute=lambda sql, params: calls.append(params), commit=lambda: calls.append("commit"))
            >>> SQLiteDatabaseCustomColumnMacros().delete_cc_item("custom_column_1", "links", 7, conn=conn)
            >>> calls
            [(7,), (7,), 'commit']


        :param table: Trusted custom-table name, interpolated into SQL; column prefixes
            come from plural_singular_mapper.
        :param lt: Trusted legacy link-table name with inflected _book and _value column
            names.
        :param target_id: ID bound to the selected row or link predicate.
        :param conn: Optional compatible connection; None uses the default path
            described above. Connections are not closed here.
        :return: None; commit is attempted after both DELETE statements succeed.
        """
        lt_col = self._cc_table_col_mapper(lt)
        table_col = self._cc_table_col_mapper(table)

        lt_stmt = "DELETE FROM {lt} WHERE {lt_col}_value=?".format(lt=lt, lt_col=lt_col)
        table_stmt = "DELETE FROM {table} WHERE {table_col}_id=?".format(table=table, table_col=table_col)

        if conn is None:
            self.db.driver.conn.execute(lt_stmt, (target_id,))
            self.db.driver.conn.execute(table_stmt, (target_id,))
            self.db.driver.conn.commit()
        else:
            conn.execute(lt_stmt, (target_id,))
            conn.execute(table_stmt, (target_id,))
            conn.commit()

    # Todo: Make this consistent with the use of conn - might want to make new versions of all these functions, semi-prviate
    #       which actually include conn
    # Todo: Check that the comment is accurate - might be making malformed custom column tables
    # Todo: rename book to target_id and value to cc_id?
    def break_cc_lt_link(self, lt, book, value=None):
        """
        Delete one owner/value pair for a truthy value, or every link for an owner
        otherwise.

        The branch tests truthiness, not an explicit None check: zero, empty text and
        other falsey values select the all-links deletion. Use self.execute and discard
        its result; transaction behavior belongs to the host. This does not delete the
        referenced custom-value rows.

        Example:
            >>> calls = []
            >>> macros = SQLiteDatabaseCustomColumnMacros()
            >>> macros.execute = lambda sql, params: calls.append(params)
            >>> macros.break_cc_lt_link("links", 7, value=0)
            >>> calls
            [(7,)]


        :param lt: Trusted legacy link-table name with inflected _book and _value column
            names.
        :param book: Owner ID whose links are selected.
        :param value: Truthy custom-value ID to restrict deletion, or any falsey value
            to delete all links for the owner.
        :return: None; all matching links are deleted through the host execute
            operation.
        """
        lt_col = self._cc_table_col_mapper(lt)
        if value:
            del_stmt = "DELETE FROM {lt} WHERE {lt_col}_book=? and {lt_col}_value=?".format(lt=lt, lt_col=lt_col)
            self.execute(del_stmt, (book, value))
        else:
            del_stmt = "DELETE FROM {lt} WHERE {lt_col}_book=?".format(lt=lt, lt_col=lt_col)
            self.execute(del_stmt, (book,))

    def delete_from_cc_table_by_id(self, table, target_id, conn=None):
        """
        Delete matching custom-table ID rows using the selected execution path.

        With conn=None, delegate to self.execute; otherwise call conn.execute without
        committing. No link cleanup, row-count check or cache invalidation occurs here;
        backend triggers and constraints determine related effects.

        Example:
            ``macros.delete_from_cc_table_by_id("custom_column_1", 7, conn=conn)``
            deletes ID 7 without an explicit commit.


        :param table: Trusted custom-table name, interpolated into SQL; column prefixes
            come from plural_singular_mapper.
        :param target_id: ID bound to the selected row or link predicate.
        :param conn: Optional compatible connection; None uses the default path
            described above. Connections are not closed here.
        :return: None; the execution result is discarded.
        """
        table_col = self._cc_table_col_mapper(table)
        del_stmt = "DELETE FROM {table} WHERE {table_col}_id=?".format(table=table, table_col=table_col)

        if conn is None:
            self.execute(del_stmt, (target_id,))
        else:
            conn.execute(del_stmt, (target_id,))

    def delete_from_cc_table_by_value(self, table, target_id):
        """
        Delete every custom-table row whose _value equals the supplied value.

        Despite the parameter name, target_id is matched against _value, not _id. None
        is bound to equality and does not select SQL nulls. Delegate to self.execute
        without explicit link cleanup or transaction handling.

        Example:
            ``macros.delete_from_cc_table_by_value("custom_column_1", "A")``
            deletes every row whose stored value equals A.


        :param table: Trusted custom-table name, interpolated into SQL; column prefixes
            come from plural_singular_mapper.
        :param target_id: Stored custom value to match, despite the ID-oriented
            parameter name.
        :return: None; the host execution result is discarded.
        """
        table_col = self._cc_table_col_mapper(table)
        del_stmt = "DELETE FROM {table} WHERE {table_col}_value=?".format(table=table, table_col=table_col)

        self.execute(del_stmt, (target_id,))

    def break_cc_links_by_book_id(self, lt, book_id, conn=None):
        """
        Delete links for one owner or a supported collection of owners.

        Strings and integers, including bool, use one bound DELETE. Tuples, lists, sets
        and generator objects select the bulk branch; other forms raise
        NotImplementedError. An explicit connection receives one-item binding tuples.
        Without it, materialize raw elements and pass them unchanged to db.executemany,
        falling back to driver.direct_executemany on AttributeError. Scalar elements in
        that default bulk path may be invalid parameter sequences. No explicit commit or
        rollback is added.

        Example:
            >>> conn = sqlite3.connect(":memory:")
            >>> _ = conn.execute("CREATE TABLE links (link_book INTEGER, link_value INTEGER)")
            >>> _ = conn.executemany("INSERT INTO links VALUES (?, ?)", [(1, 7), (2, 7), (3, 7)])
            >>> macros = SQLiteDatabaseCustomColumnMacros()
            >>> macros.break_cc_links_by_book_id("links", [1, 2], conn=conn)
            >>> conn.execute("SELECT link_book FROM links").fetchall()
            [(3,)]
            >>> conn.close()


        :param lt: Trusted legacy link-table name with inflected _book and _value column
            names.
        :param book_id: A str/int owner ID or tuple/list/set/generator of owners; other
            iterables are unsupported.
        :param conn: Optional compatible connection; None uses the default path
            described above. Connections are not closed here.
        :return: None; supported IDs drive single or repeated deletes.
        """
        lt_col = self._cc_table_col_mapper(lt)
        stmt = "DELETE FROM {lt} WHERE {lt_col}_book=?".format(lt=lt, lt_col=lt_col)

        if isinstance(book_id, (str, int)):
            if conn is None:
                self.db.driver.conn.execute(stmt, (book_id,))

            else:
                conn.execute(stmt, (book_id,))

        elif isinstance(book_id, (tuple, list, set, types.GeneratorType)):

            if conn is None:
                target_ids = tuple([k for k in book_id])
                try:
                    self.db.executemany(stmt, target_ids)
                except AttributeError:
                    self.db.driver.direct_executemany(stmt, target_ids)

            else:
                conn.executemany(stmt, ((k,) for k in book_id))

        else:
            raise NotImplementedError("book_id had unexpected form {} - type {}".format(book_id, type(book_id)))

    def break_cc_links_by_book_id_and_value(self, lt, book_id, value_id, conn=None):
        """
        Delete a specific owner/value pair using only the stored default connection.

        Build a legacy _book/_value predicate and bind both IDs. Any non-None conn
        argument raises NotImplementedError before execution. No commit, duplicate check
        or value-row deletion occurs.

        Example:
            >>> try:
            ...     SQLiteDatabaseCustomColumnMacros().break_cc_links_by_book_id_and_value("links", 7, 3, conn=object())
            ... except NotImplementedError:
            ...     print("override unsupported")
            override unsupported


        :param lt: Trusted legacy link-table name with inflected _book and _value column
            names.
        :param book_id: Book or legacy owner ID bound as a query value.
        :param value_id: Custom-value ID bound to the link value column.
        :param conn: Must be None; every explicit connection is unsupported.
        :return: None after default-connection execution.
        """
        lt_col = self._cc_table_col_mapper(lt)
        break_stmt = "DELETE FROM {lt} WHERE {lt_col}_book=? and {lt_col}_value=?".format(lt=lt, lt_col=lt_col)

        if conn is None:
            self.db.driver.conn.execute(break_stmt, (book_id, value_id))
        else:
            raise NotImplementedError

    # Todo: Rename as "clear cc by book"
    def clear_cc_entries_from_table(self, table, book_id, conn=None):
        """
        Delete every row whose legacy _book column matches one owner ID.

        Execute on the explicit or stored connection without an explicit commit. This
        applies to tables carrying that owner column; it neither deletes shared value
        rows elsewhere nor checks how many records were removed.

        Example:
            ``macros.clear_cc_entries_from_table("books_custom_column_1_link",
            7, conn=conn)`` clears all links for owner 7.


        :param table: Trusted custom-table name, interpolated into SQL; column prefixes
            come from plural_singular_mapper.
        :param book_id: Book or legacy owner ID bound as a query value.
        :param conn: Optional compatible connection; None uses the default path
            described above. Connections are not closed here.
        :return: None; the execution result is discarded.
        """
        table_col = self._cc_table_col_mapper(table)
        clear_stmt = "DELETE FROM {table} WHERE {table_col}_book=?".format(table=table, table_col=table_col)

        if conn is None:
            self.db.driver.conn.execute(clear_stmt, (book_id,))
        else:
            conn.execute(clear_stmt, (book_id,))

    # Todo: Check that this clear is actually happening
    #       Make an entry
    #       Clear it.
    #       Add some more. Then add it back. Test it;'x
    def clear_cc_unused_table_entries(self, table, lt, conn=None):
        """
        Delete value rows for which the generated link-count subquery finds no
        references.

        The count column uses the inflected link prefix, but the value predicate uses
        the literal link-table name plus _value, without a table qualifier. This works
        only when that spelling resolves appropriately; when the table name and
        inflected prefix differ, normal schemas can raise a missing-column error. No
        schema validation, commit or rollback is added, and only the supplied link table
        is checked.

        Example:
            >>> conn = sqlite3.connect(":memory:")
            >>> _ = conn.execute("CREATE TABLE entries (entry_id INTEGER PRIMARY KEY)")
            >>> _ = conn.execute("CREATE TABLE links (link_id INTEGER, link_value INTEGER)")
            >>> try:
            ...     SQLiteDatabaseCustomColumnMacros().clear_cc_unused_table_entries("entries", "links", conn=conn)
            ... except sqlite3.OperationalError:
            ...     print("generated links_value is absent")
            generated links_value is absent
            >>> conn.close()


        :param table: Trusted custom-table name, interpolated into SQL; column prefixes
            come from plural_singular_mapper.
        :param lt: Trusted legacy link-table name with inflected _book and _value column
            names.
        :param conn: Optional compatible connection; None uses the default path
            described above. Connections are not closed here.
        :return: None on success; backend errors from the generated DELETE propagate.
        """
        table_col = self._cc_table_col_mapper(table)
        lt_col = self._cc_table_col_mapper(lt)

        clear_stmt = (
            "DELETE FROM {table} WHERE (SELECT COUNT({lt_col}_id) "
            "FROM {lt} "
            "WHERE {lt}_value={table}.{table_col}_id) < 1"
            "".format(table=table, table_col=table_col, lt=lt, lt_col=lt_col)
        )

        if conn is None:
            self.db.driver.conn.execute(clear_stmt)
        else:
            conn.execute(clear_stmt)

    def clean_custom(self, cc_num_map, cc_table_name_factory=None, conn=None):
        """
        Prune unreferenced values for normalized custom-column definitions, then commit.

        Iterate metadata values and process entries whose normalized flag is truthy.
        Call the supplied factory with each num to obtain (value_table, link_table),
        build correlated count-based DELETE statements and execute the complete script.
        No transaction or rollback is added. Both default and explicit connections are
        committed after successful script execution, including unrelated pending work.
        No eligible entries means no connection access or SQL. A factory is required
        when any entry is normalized despite its None default.

        Example:
            >>> SQLiteDatabaseCustomColumnMacros().clean_custom({})
            >>> SQLiteDatabaseCustomColumnMacros().clean_custom({1: {"normalized": False}})


        :param cc_num_map: Mapping whose values contain normalized and, for eligible
            entries, num.
        :param cc_table_name_factory: Callable taking num and returning (value_table,
            link_table); required for normalized entries.
        :param conn: Optional compatible connection; None uses the default path
            described above. Connections are not closed here.
        :return: None; generated cleanup statements and the final commit are executed
            only when needed.
        """

        st = (
            "DELETE FROM {table} WHERE (SELECT COUNT({lt_col}_id) "
            "FROM {lt} "
            "WHERE {lt}.{lt_col}_value={table}.{table_col}_id) < 1;"
        )

        statements = []
        for data in cc_num_map.values():

            if data["normalized"]:
                table, lt = cc_table_name_factory(data["num"])
                table_col = self._cc_table_col_mapper(table)
                lt_col = self._cc_table_col_mapper(lt)

                statements.append(st.format(lt=lt, table=table, table_col=table_col, lt_col=lt_col))
        if statements:
            if conn is None:
                self.db.driver.conn.executescript(" \n".join(statements))
                self.db.driver.conn.commit()
            else:
                conn.executescript(" \n".join(statements))
                conn.commit()

    #
    # ------------------------------------------------------------------------------------------------------------------
    def direct_get_custom_tables(self, conn=None):
        """
        Return main-schema physical table names matching the legacy custom-table
        patterns.

        Query sqlite_master for tables whose names match custom_column_* or
        *_custom_column_*_link using GLOB. Build a set from the first cell of each
        default get row. Views and temporary tables are excluded, but similarly named
        unrelated tables can match; custom-column metadata is not consulted.

        Example:
            ``macros.direct_get_custom_tables(conn=conn)`` returns matching
            physical names rather than their contents or custom-column definitions.


        :param conn: Optional compatible connection; None uses the default path
            described above. Connections are not closed here.
        :return: A set of matching table-name cells from the query result.
        """
        conn = conn if conn is not None else self.db.driver.conn

        return set(
            [
                x[0]
                for x in conn.get(
                    'SELECT name FROM sqlite_master WHERE type="table" AND '
                    '(name GLOB "custom_column_*" OR name GLOB "*_custom_column_*_link")'
                )
            ]
        )

    #
    # ------------------------------------------------------------------------------------------------------------------
