
"""
Macros to handle custom column values.
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
    Macros affecting only custom columns.

    """
    @staticmethod
    def _get_cc_id_val(custom_column: str) -> tuple[str, str]:
        """
        Return the id and val columns for a given custom column

        :param custom_column: Returns the id col and the val col for the given custom  column
        :return:
        """
        cc_col = plural_singular_mapper(custom_column)
        return "{}_id".format(cc_col), "{}_value".format(cc_col)

    @staticmethod
    def _cc_table_col_mapper(table: str) -> str:
        """
        Returns the basic column name from the table name.

        :param table:
        :return:
        """
        return plural_singular_mapper(table)

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - READ

    def get_dirtied_cache(self):
        """
        Return the dirtied cache from the database.

        :return:
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
        Return the id and values for a given target_id.

        :param custom_column:
        :param target_id:
        :param conn:
        :return:
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
        Return the old id and the old value from a table id.

        :param table:
        :param old_id:
        :return:
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
        Return the id of a custom column belonging to the particular given value.

        :param target_table:
        :param cc_value:
        :param all:
        :param conn:
        :return:
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
        Takes a value and returns the books corresponding to it from the cc link table.

        Note - values should be ids - values of the table being linked to it.
        :param lt: Link table
        :param value: The value to search for
        :param conn: Connection to use for
        :return:
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
        Return all the values for a custom column - should work both on link tables and on the maijn tables

        :param cc_table:
        :param distinct:
        :param conn:
        :return:
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
        Returns all the indices for a given series

        Used to offer completion by providing the next index in the sequence.
        :param cc_series_link_table:
        :param series_id:
        :param conn:
        :return:
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
        Check to see if there is a link between a given book_id and a value_id

        :param link_table:
        :param book_id:
        :param value_id:
        :param conn:
        :return:
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
        Read and return the custom column value from the meta_2 table.

        :param num:
        :param book_id:
        :param conn:
        :return:
        """
        if conn is None:
            return self.db.driver.conn.get("SELECT custom_%s FROM meta2 WHERE id=?" % num, (book_id,), all=False)
        else:
            return conn.get("SELECT custom_%s FROM meta2 WHERE id=?" % num, (book_id,), all=False)

    def get_all_cc_id_val_pairs(self, table, conn: Optional[sqlite3.Connection] = None):
        """
        Return id and value pair for every entry on a cc table.

        :param table:
        :param conn: Database connection
        :return:
        """
        cc_id_col, cc_val_col = self._get_cc_id_val(table)

        conn = conn if conn is not None else self.db.driver.conn

        return conn.get(
            "SELECT {id_col}, {val_col} FROM {table}" "".format(id_col=cc_id_col, val_col=cc_val_col, table=table)
        )

    def get_cc_books_from_link_table(self, lt: str, lt_value: Any) -> Iterable[int]:
        """
        Return books with that value from a link table.

        Takes a lt_value - which should be an id in the actual values custom column table, and return all the books
        associated with those values.
        :param lt: The custom column link table
        :param lt_value: The value to search for in the link table.
        :return:
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
        Get the books which are referenced by the custom table.

        :param table:
        :param link:
        :param id:
        :return:
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
        Return the custom and extra values from the database.

        :param link_table:
        :param index:
        :param conn:
        :return:
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
        Returns the id of the newly created row.

        :param table:
        :param value:
        :param conn:
        :return:
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
        Add a custom columns link with an extra element as well.
        :param lt:
        :param book_id:
        :param value_id:
        :param extra:
        :param conn:
        :param target_column: The cc column - either a reference to a cc value in another table or the value itself
        :return:
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
        Add a custom columns link with an extra element as well.
        :param lt:
        :param book_id:
        :param value_id:
        :param extra:
        :param conn:
        :param target_column: The cc column - either a reference to a cc value in another table or the value itself
        :return:
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
        Inserts multiple values into a custom column table.
        :param table:
        :param values:
        :param conn:
        :return:
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
        With some created temp tables, add the values in, normalize, and move to the actual custom column table
        :param conn:
        :return:
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
        Update the custom column to a new value.
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
        Repoint the values column of a link table.
        :param lt:
        :param new_id:
        :param old_id:
        :return:
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
        Update cc value by changing the value.
        :param lt:
        :param new_value_id:
        :param old_value_id:
        :param conn: The connection to the database.
        :return:
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
        Update, using the sequence, the additional column in the custom column.
        :param table:
        :param column:
        :param sequence:
        :return:
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
        Remove an item - and all the links which refer to it if there's a link table.
        :param table:
        :param lt:
        :param target_id: The target_id to try and remove from the system.
        :param conn: A connection to the database.
        :return:
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
        Break a link in a custom columns link table (or a regular custom columns table - not sure it currently makes
        a difference).
        :param lt:
        :param book:
        :param value: If None, then all the entries for the given book will be removed
        :return:
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
        Remove a target entry from a custom table.
        :param table:
        :param target_id:
        :param conn:
        :return:
        """
        table_col = self._cc_table_col_mapper(table)
        del_stmt = "DELETE FROM {table} WHERE {table_col}_id=?".format(table=table, table_col=table_col)

        if conn is None:
            self.execute(del_stmt, (target_id,))
        else:
            conn.execute(del_stmt, (target_id,))

    def delete_from_cc_table_by_value(self, table, target_id):
        """
        Remove a target entry from a custom table.
        :param table:
        :param target_id:
        :return:
        """
        table_col = self._cc_table_col_mapper(table)
        del_stmt = "DELETE FROM {table} WHERE {table_col}_value=?".format(table=table, table_col=table_col)

        self.execute(del_stmt, (target_id,))

    def break_cc_links_by_book_id(self, lt, book_id, conn=None):
        """
        Break all cc links to the given book_id.
        :param lt:
        :param book_id:
        :param conn:
        :return:
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
        Break all links in a cc link table with a particular book_id and value_id.
        :param lt:
        :param book_id:
        :param value_id:
        :return:
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
        Remove all the entries corresponding to a given book
        :return:
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
        Clear entries from the cc table which are no longer in use.
        :param table:
        :param lt:
        :param conn:
        :return:
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
        Takes a cc_num_map (keyed with the cc num and valued with

        :param cc_num_map:
        :param cc_table_name_factory: Function which produces the tables names from the cc table num.
        :param conn:
        :return:
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
        Directly query the database for the current custom tables content.
        :param conn:
        :return:
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
