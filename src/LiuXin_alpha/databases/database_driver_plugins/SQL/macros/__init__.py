
"""
Macros are pre-defined operation on the database - intended for speedup.
"""


# Macros which provide pre-defined operations on the database.
# This allows you the option of replacing the generic macros which will use the methods provided by the database with
# more efficient macros tailored to the underlying database
# Macros should be shortcuts to preform useful operatios on the tables - but ones which can be all replicated using
# objects from the database
# If it's a fundamental operation, then it should be done in the driver
# Todo: In line with this, move the create custom columns logic down into the driver

import sqlite3 as sqlite

from collections import defaultdict
from typing import TYPE_CHECKING

from LiuXin_alpha.databases.database_driver_plugins.SQL.macros.cc_macros_mixin import \
    SQLiteDatabaseCustomColumnMacros
from LiuXin_alpha.errors import DatabaseDriverError, DatabaseIntegrityError

from LiuXin_alpha.databases.api import MacrosAPI

from LiuXin_alpha.utils.logging import default_log

# Todo: This needs to be replaced with a column name factory
from LiuXin_alpha.databases.database_driver_plugins.macros_base import MacrosBase
from LiuXin_alpha.databases.database_driver_plugins.SQL.macros.temp_tables_macros_mixin import TempTablesMacrosMixin
from LiuXin_alpha.databases.database_driver_plugins.SQL.macros.hash_tables_macros_mixin import HashTablesMacrosMixin
from LiuXin_alpha.databases.database_driver_plugins.SQL.macros.titles_macros_mixin import TitlesMacroMethodsMixin
from LiuXin_alpha.databases.database_driver_plugins.SQL.macros.books_mecros_mixin import BooksMacrosMixin
from LiuXin_alpha.databases.database_driver_plugins.SQL.macros.series_title_link_macros import SeriesTitleLinkMacros

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database import DatabaseAPI


# Todo: Probably should have it's own API


class SQLiteDatabaseMacros(
    MacrosBase,
    SQLiteDatabaseCustomColumnMacros,
    TempTablesMacrosMixin,
    HashTablesMacrosMixin,
    TitlesMacroMethodsMixin, BooksMacrosMixin, SeriesTitleLinkMacros,
    MacrosAPI
):
    """
    Provides pre-defined operations on an SQLite database.
    """

    def __init__(self, db: "DatabaseAPI") -> None:
        """
        Attaches to the underlying database to provide additional services.
        :param db:
        """
        super(SQLiteDatabaseMacros, self).__init__(db=db)

    # Todo - These should probably be semi private
    @property
    def get(self):
        """
        For compatibility.
        :param args:
        :param kwargs:
        :return:
        """
        return self.db.get

    @property
    def execute(self):
        """
        For compatibility.
        :return:
        """
        return self.db.driver_wrapper.execute

    @property
    def executemany(self):
        """
        For compatibility.
        :return:
        """
        return self.db.driver_wrapper.executemany

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO REPLACE IN PHYSICAL ASSET PATHS

    def replace_in_folder_store_path(self, target_str, replacement):
        """
        Run a replacement in the folder_store_path column of the folder_stores table.
        :param target_str:
        :param replacement:
        :return:
        """
        replace_sql = "UPDATE folder_stores SET folder_store_path = replace(folder_store_path, ?, ?);"
        self.execute(replace_sql, (target_str, replacement))

    def replace_in_folder_store_marker_path(self, target_str, replacement):
        """
        Run a replacement in the folder_store_marker_path column of the folder stores table.
        :param target_str:
        :param replacement:
        :return:
        """
        replace_sql = "UPDATE folder_stores SET folder_store_marker_path = replace(folder_store_marker_path, ?, ?);"
        self.execute(replace_sql, (target_str, replacement))

    def replace_in_folder_path(self, target_str, replacement):
        """
        Run a replacement in the folder_path column of the folders table.
        :param target_str:
        :param replacement:
        :return:
        """
        replace_sql = "UPDATE folders SET folder_path = replace(folder_path, ?, ?);"
        self.execute(replace_sql, (target_str, replacement))

    def replace_in_cover_path(self, target_str, replacement):
        """
        Run a replacement in the cover_path column of the covers table.
        :param target_str:
        :param replacement:
        :return:
        """
        replace_sql = "UPDATE covers SET cover_path = replace(cover_path, ?, ?);"
        self.execute(replace_sql, (target_str, replacement))

    def replace_in_file_path(self, target_str, replacement):
        """
        Run a replacement in the cover_path column of the covers table.
        :param target_str:
        :param replacement:
        :return:
        """
        replace_sql = "UPDATE files SET file_path = replace(file_path, ?, ?);"
        self.execute(replace_sql, (target_str, replacement))

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - LINK BREAKING METHODS

    def break_lang_title_links(self, title_id, link_type=None):
        """
        Break links bwteen the given title and any relevant languages
        :param title_id:
        :param link_type: Defaults to None - which will remove all links between the given title row and any languages
        :return:
        """
        if link_type is not None:
            stmt = (
                "DELETE FROM language_title_links WHERE language_title_link_title_id = ? "
                "AND language_title_link_type = '{}';".format(link_type)
            )
            self.execute(stmt, (title_id,))
        else:
            stmt = "DELETE FROM language_title_links WHERE language_title_link_title_id = ?;".format(link_type)
            self.execute(stmt, (title_id,))

    # Todo: This is a bad name - it breaks generic LINKS
    def break_generic_link(self, link_table, link_col, remove_id, link_type=None):
        """
        Break a generic link - all links matching the given remove_id will be deleted.
        :param link_table:
        :param link_col:
        :param remove_id: If remove_id is an int, only that row will be removed. If it's an iterable then all the ids
                          in that iterable will be removed.
        :param link_type: If provided
        :return:
        """
        if link_type is None:
            stmt = "DELETE FROM {0} WHERE {1} = ?;".format(link_table, link_col)
            if isinstance(remove_id, int):
                self.execute(stmt, (remove_id,))
            else:
                self.executemany(stmt, remove_id)
        else:
            link_table_col = self.db.driver_wrapper.get_column_base(link_table)
            link_table_type_col = "{}_type".format(link_table_col)
            stmt = "DELETE FROM {0} WHERE {1} = ? AND {2} = ?;".format(link_table, link_col, link_table_type_col)
            if isinstance(remove_id, int):
                self.execute(stmt, (remove_id, link_type))
            else:
                # self.executemany(stmt, remove_id)
                raise NotImplementedError

    def break_generic_single_link(self, link_table, left_link_col, right_link_col, left_id, right_id):
        """
        Break a specified link between two entities.
        :param link_table:
        :param left_link_col:
        :param right_link_col:
        :param left_id:
        :param right_id:
        :return:
        """
        del_stmt = "DELETE FROM {0} WHERE {1} = ? AND {2} = ?;".format(link_table, left_link_col, right_link_col)
        self.execute(del_stmt, (left_id, right_id))

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - LINK MAKING METHODS

    def make_generic_link(self, link_table, left_link_col, right_link_col, priority_col, left_id, right_id):
        """
        Make a generic link between two entities without a priority column.
        :param link_table:
        :param left_link_col:
        :param right_link_col:
        :param priority_col:
        :param left_id:
        :param right_id:
        :return:
        """
        stmt = (
            "INSERT INTO {0}({1}, {2}, {3}) "
            "SELECT ?, ?, MAX({3}) "
            "FROM {0}".format(link_table, left_link_col, right_link_col, priority_col)
        )
        self.execute(stmt, (left_id, right_id))

    # Todo: The interface for this macro is terrible and you should feel bad. Fix it.
    def make_generic_link_no_priority(
        self,
        link_table,
        left_link_col,
        right_link_col,
        left_id=None,
        right_id=None,
        id_pairs=None,
    ):
        """
        Write a generic link without priority or anything else. Just forming the link.
        :param link_table:
        :param left_link_col:
        :param right_link_col:
        :param left_id:
        :param right_id:
        :return:
        """
        ins_stmt = "INSERT INTO {0}({2}, {1}) VALUES(?, ?)".format(link_table, left_link_col, right_link_col)
        if id_pairs is None:
            self.execute(ins_stmt, (left_id, right_id))
        else:
            self.executemany(ins_stmt, id_pairs)

    def break_creator_title_links(self, title_id, creator_type=("author", "authors")):
        """
        Remove links of a certain type between titles and creators
        :param title_id: The title to remove all the creators for
        :param creator_type:
        :return:
        """
        del_stmt = (
            "DELETE FROM creator_title_links "
            "WHERE creator_title_link_title_id=? AND creator_title_link_type IN {};".format(creator_type)
        )

        if isinstance(title_id, int):
            self.execute(del_stmt, (title_id,))
        else:
            try:
                self.executemany(del_stmt, ((k,) for k in title_id))
            except Exception as e:
                err_str = "db.executemany failed"
                err_str = default_log.log_exception(err_str, e, "ERROR")
                raise DatabaseDriverError(err_str)

    def make_creator_title_links(self, title_id=None, creator_id=None, id_pairs=None, creator_type="authors"):
        """
        Construct a link between a title and a creator.
        :param title_id:
        :param creator_id:
        :param creator_type:
        :return:
        """
        insert_stmt = (
            "INSERT INTO creator_title_links "
            "(creator_title_link_title_id, creator_title_link_creator_id, "
            "creator_title_link_type, creator_title_link_priority) "
            "SELECT ?, ?, 'authors', MIN(creator_title_link_priority) - 1 FROM creator_title_links;"
        )

        if id_pairs is not None:
            self.executemany(insert_stmt, id_pairs)
        else:
            self.execute(insert_stmt, (title_id, creator_id))

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - CLEAN METHOD

    def publisher_clear_unused(self):
        """
        Clear publishers which don't have any active entries in publisher_title_links.
        :return:
        """
        del_stmt = (
            "DELETE FROM publishers WHERE publisher_id NOT IN "
            "(SELECT publisher_title_link_publisher_id FROM publisher_title_links);"
        )
        self.execute(del_stmt)

    def creator_clear_unused(self):
        """
        Clear creators which don't have any active entries in publisher_title_links.
        :return:
        """
        del_stmt = (
            "DELETE FROM creators WHERE creator_id NOT IN "
            "(SELECT creator_title_link_creator_id FROM creator_title_links);"
        )
        self.execute(del_stmt)

    # Todo: This is not actually about cleaning - it's about breaking a generic link - need to rename and
    # merge
    def generic_clean_update(self, link_table, link_col, value_for_clear):
        """
        Preforms a generic clean.
        :param link_table:
        :param link_col:
        :param value_for_clear:
        :return:
        """
        del_stmt = "DELETE FROM {0} WHERE {1}=?".format(link_table, link_col)
        if isinstance(value_for_clear, int):
            self.execute(del_stmt, (value_for_clear,))
        else:
            self.executemany(del_stmt, value_for_clear)

    def break_lang_title_primary_link(self, title_id):
        """
        Remove links of primary type between titles and languages.
        :param title_id:
        :return:
        """
        del_stmt = (
            "DELETE FROM language_title_links "
            "WHERE language_title_link_title_id = ? AND language_title_link_type = 'primary';"
        )
        if isinstance(title_id, int):
            self.execute(del_stmt, title_id)
        else:
            self.executemany(del_stmt, title_id)

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - FEED MANAGEMENT

    def add_feed(self, title, script):
        """
        Add a feed to the feeds table.
        :param title:
        :param script:
        :return:
        """
        insert_stmt = "INSERT INTO feeds(feed_title, feed_script) VALUES (?, ?);"
        self.execute(insert_stmt, (title, script))

    def delete_feed(self, feed_id):
        """
        Remove a feed from the feeds table.
        :param feed_id:
        :return:
        """
        del_stmt = "DELETE FROM feeds WHERE feed_id=?;"
        if isinstance(feed_id, int):
            self.execute(del_stmt, (feed_id,))
        else:
            self.executemany(del_stmt, feed_id)

    #
    # ------------------------------------------------------------------------------------------------------------------

    def get_foreign_key_replacement_trigger(self, target_table, search_column="book", target_id="book_id", old=True):
        """
        Used when, for whatever reason, we don't want to use a full foreign key.
        """
        return "DELETE FROM {} WHERE {}=OLD.{};".format(target_table, search_column, target_id)

    #
    # ------------------------------------------------------------------------------------------------------------------

    def delete_item_by_id(self, item_table, item_id_col, item_id):
        """
        Delete an item from a given table.
        :param item_table:
        :param item_id_col:
        :param item_id:
        :return:
        """
        # Todo: Really needs some kind of checking
        del_stmt = "DELETE FROM {} WHERE {}=?".format(item_table, item_id_col)
        if isinstance(item_id, int):
            self.execute(del_stmt, (item_id,))
        else:
            self.executemany(del_stmt, item_id)

    def delete_title(self, title_id):
        """
        Delete a title - and a book if it exists.
        :param title_id:
        :return:
        """
        title_del_stmt = "DELETE FROM titles WHERE title_id = ?;"
        self.execute(title_del_stmt, (title_id,))
        book_del_stmt = "DELETE FROM books WHERE book_id = ?;"
        self.execute(book_del_stmt, (title_id,))

    def delete_book(self, book_id):
        """
        Just delete a book from the system python
        :param book_id:
        :return:
        """
        book_del_stmt = "DELETE FROM books WHERE book_id = ?;"
        self.execute(book_del_stmt, (book_id,))

    def set_override_book_path(self, book_id, path):
        """
        A column called book_path is provided so that the user can set an override path for that book.
        :param book_id:
        :param path:
        :return:
        """
        self.execute("UPDATE books SET book_paths=? WHERE book_id=?", (path, book_id))

    # Todo: Merge with the below
    def direct_update_column_in_table(self, table, column, table_id_col, item_id, new_value):
        """
        Preform an update of a column in a specified table.
        :param table:
        :param column:
        :param item_id:
        :param new_value:
        :param table_id_col:
        :return:
        """
        stmt = "UPDATE {0} SET {1} = ? WHERE {2} = ?;".format(table, column, table_id_col)
        self.execute(stmt, (item_id, new_value))

    def update_column_in_table(self, table, column, table_id_col, item_id, new_value):
        """
        Preform an update of a column in a specified table.
        :param table:
        :param column:
        :param item_id:
        :param new_value:
        :param table_id_col:
        :return:
        """
        # Todo: Why isn't this working?
        # stmt = "UPDATE {0} SET {1} = ? WHERE {2} = ?;".format(table, column, table_id_col)
        # self.execute(stmt, (item_id, new_value))
        target_row = self.db.get_row_from_id(table, item_id)
        target_row[column] = new_value
        target_row.sync()

    # Todo: Merge with the driver method - which does the same thing - dry the code base out
    def get_unique_values(self, table, column):
        """
        Returns a set of all the values of the given column of the table.
        :param table:
        :param column:
        :return:
        """
        current_values = set()
        stmt = "SELECT {} FROM {};".format(column, table)
        for row in self.execute(stmt):
            current_values.add(row[0])
        return current_values

    def get_values_one_condition(self, table, rtn_column, cond_column, value, default_value=None):
        """
        Return all the values in a table which satisfy one condition.
        :param table:
        :param rtn_column:
        :param cond_column:
        :param value:
        :param default_value: Return this if the book_id doesn't exist in the table
        :return:
        """
        current_values = set()
        stmt = "SELECT {0} FROM {1} WHERE {2} = ?;".format(rtn_column, table, cond_column)
        try:
            for row in self.execute(stmt, (value,)):
                current_values.add(row[0])
        except TypeError:
            return default_value
        return current_values

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - CREATOR VALUES METHODS

    def get_creator_sort(self, creator_id):
        """
        Returns the creator sort value for a given id from the creators table
        :param creator_id:
        :return:
        """
        db_result = self.get("SELECT creator_sort FROM creators WHERE creator_id=?", (creator_id,))
        try:
            return db_result[0][0]
        except IndexError:
            return None

    def get_creator_link(self, creator_id):
        """
        Returns the creator link value for a given id.
        :param creator_id:
        :return:
        """
        db_result = self.get("SELECT creator_link FROM creators WHERE creator_id=?", (creator_id,))
        try:
            return db_result[0][0]
        except IndexError:
            return None

    # Todo: Add the single option - bring into line with the naming scheme
    def update_creator_links(self, values):
        """
        Update the creator links for multiple creators.
        :param values: Iterable of tuples - the first element being the id of the creator and the second element being
                       the new creator links
        :return:
        """
        stmt = "UPDATE creators SET creator_link=? WHERE creator_id=?"
        self.executemany(stmt, values)

    # Todo: Bring into line with the rest by offering a singular and multiple update options
    def update_creator_sorts(self, values):
        """
        Update the creator sorts for multiple individual creators.
        :param values: Iterable of tuples - the first element being the new creator sort and the second element being
                       the creator id to set it for
        :return:
        """
        stmt = "UPDATE creators SET creator_sort=? WHERE creator_id=?"
        self.executemany(stmt, values)

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - TITLES VALUES METHODS

    def update_title(self, title_id, title):
        """
        Preform an update of a title. title row will not be created.
        :param title_id:
        :param title:
        :return:
        """
        if not title:
            self.execute("UPDATE titles SET title=Null WHERE title_id=?;", (title_id,))
        else:
            self.execute("UPDATE titles SET title=? WHERE title_id=?;", (title, title_id))

    # Todo: Add the setting null option
    def update_title_creator_sort(self, title_id, creator_val):
        """
        Update the creator sort for an individual book stored in the title.
        :param title_id:
        :param creator_val:
        :return:
        """
        stmt = "UPDATE titles SET title_creator_sort = ? WHERE title_id = ?;"
        self.execute(stmt, (creator_val, title_id))

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - FILE VALUES METHODS

    def set_file_name(self, file_id, new_fname):
        """
        Set the file name for a specific file stored in the files table.
        :param file_id:
        :param new_fname:
        :return:
        """
        stmt = "UPDATE files SET file_name = ? WHERE file_id = ?;"
        self.execute(stmt, (new_fname, file_id))

    def set_file_size(self, file_id, size):
        """
        Sets the size of a file.
        :param file_id:
        :param size:
        :return:
        """
        stmt = "UPDATE files SET file_size = ? WHERE file_id = ?;"
        self.execute(stmt, (size, file_id))

    def set_file_size_and_name(self, file_id, size, fname):
        """
        Sets both the size and the name at the same time.
        :param file_id:
        :param size:
        :param fname:
        :return:
        """
        stmt = "UPDATE files SET file_name = ?, file_size = ? WHERE file_id = ?;"
        self.execute(stmt, (fname, size, file_id))

    # Todo: Rename to make this clear it takes out a file row, not a physical file - and the one below it
    def delete_file_by_id(self, file_id):
        """
        Deletes the file given by the specified file_id.
        :param file_id:
        :return:
        """
        stmt = """
        DELETE FROM files WHERE file_id = ?;
        """
        self.execute(stmt, (file_id,))

    def delete_files_by_id(self, file_ids):
        """
        Delete all the files given by the specified ids.
        :param file_ids:
        :return:
        """
        stmt = """
        DELETE FROM files WHERE file_id = ?;
        """
        self.executemany(stmt, file_ids)

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - BULK DELETE METHODS

    # Todo: Rename bulk delete by values
    def bulk_delete_in_table(self, table, column, column_values):
        """
        Preform a bulk delete in a specified table.
        :param table: Table to remove the values from
        :param column: Column in the table
        :param column_values: All entries in the column with a value in this object will be removed
        :return:
        """
        self.executemany("DELETE FROM {0} WHERE {1}=?".format(table, column), column_values)

    def bulk_delete_items_in_table_two_matching_cols(self, table, col_1, col_2, column_values):
        """
        Preform a bulk delete in a specified table - of entries matching two conditions.
        :param table:
        :param col_1:
        :param col_2:
        :param column_values:
        :return:
        """
        stmt = "DELETE FROM {0} WHERE {1}=? AND {2}=?;".format(table, col_1, col_2)
        self.executemany(stmt, column_values)

    def delete_in_table(self, table, column, value):
        """
        Delete from the table whenever the given value shows up.
        :param table:
        :param column:
        :param value:
        :return:
        """
        del_stmt = "DELETE FROM {0} WHERE {1}=?;".format(table, column)
        self.execute(del_stmt, (value,))

    def delete_title_identifiers(self, title_id, id_type=None):
        """
        Delete all the identifiers associated with a given title.
        :param title_id:
        :param id_type: If id_type is not None, then all the identifiers of this type for the title will be removed
        :return:
        """
        if id_type is None:
            del_stmt = """
            DELETE FROM identifiers 
            WHERE identifier_id IN (
            SELECT identifier_id
            FROM identifiers INNER JOIN identifier_title_links
            ON identifiers.identifier_id = identifier_title_links.identifier_title_link_identifier_id
            WHERE identifier_title_link_title_id = ?
            );
            """
            self.execute(del_stmt, title_id)
        else:
            del_stmt = """
            DELETE FROM identifiers 
            WHERE identifier_id IN (
            SELECT identifier_id
            FROM identifiers INNER JOIN identifier_title_links
            ON identifiers.identifier_id = identifier_title_links.identifier_title_link_identifier_id
            WHERE identifier_title_link_title_id = ? AND identifier_type = ?
            );
            """
            self.execute(del_stmt, (title_id, id_type))

    def add_title_identifier(self, title_id, id_type, id_val):
        """
        Add an new identifier to a title specified by the title id
        :param title_id: The id of the book to add the identifier to
        :param id_type: The type of the identifier to add
        :param id_val: The value of the identifier to add
        :return:
        """
        title_row = self.db.get_row_from_id("titles", row_id=title_id)

        new_id_row = self.db.get_blank_row("identifiers")
        new_id_row["identifier_type"] = id_type
        new_id_row["identifier"] = id_val
        new_id_row.sync()

        self.db.interlink_rows(primary_row=title_row, secondary_row=new_id_row, type=id_type)

    def bulk_update_link_table(self, link_table, update_column, other_column, values):
        """
        Preform a bulk update on a link table - used for repoiting links - i.e. moving an identifier between titles
        :param link_table: The name of the link table
        :param update_column: The column to update
        :param other_column: The other link column
        :param values: iterable of tuples with the first element being for the column to update and the second element
                       being the other column to update
        :return:
        """
        stmt = "UPDATE {0} SET {1} = ? WHERE {1} = ? AND {2} = ?".format(link_table, update_column, other_column)
        self.executemany(stmt, values)

    def bulk_add_links(self, link_table, src_col, dst_col, values):
        """
        Bulk add links to a link table.
        Type e.t.c setting is not supported.
        :param link_table:
        :param src_col:
        :param dst_col:
        :param values:
        :return:
        """
        stmt = "INSERT INTO {0}({1}, {2}) VALUES (?,?);".format(link_table, src_col, dst_col)
        try:
            self.executemany(stmt, values)
        except DatabaseDriverError:
            with self.db.lock:
                # There is probably a priority column - which we need to deal with
                for val_pair in values:
                    link_row = self.db.get_blank_row(link_table)
                    link_row[src_col] = val_pair[0]
                    link_row[dst_col] = val_pair[1]

                    priority_col = "{}_priority".format(link_table[:-1])

                    link_row[priority_col] = self.db.get_max(priority_col) + 1

                    link_row.sync()

    def reprioritize_link(
        self,
        link_table,
        left_link_col,
        right_link_col,
        left_id,
        right_id,
        new_type=None,
        new_priority="MAX",
    ):
        """
        Change the priority of a link.
        :param link_table:
        :param left_link_col:
        :param right_link_col:
        :param left_id:
        :param right_id:
        :param new_type: If this is specified, then the type will be changed
        :return:
        """
        assert new_priority == "MAX", "Only max mode is supported at the moment"

        link_base_col = self.db.driver_wrapper.get_column_base(link_table)
        link_priority_col = "{0}_priority".format(link_base_col)

        if new_type is None:
            stmt = "UPDATE {0} SET {1} = (SELECT MAX({1}) + 1 FROM {0})" " WHERE {2} = ? AND {3} = ?;".format(
                link_table, link_priority_col, left_link_col, right_link_col
            )
            self.execute(stmt, (left_id, right_id))
        else:
            # First change the priority
            self.reprioritize_link(
                link_table=link_table,
                left_link_col=left_link_col,
                right_link_col=right_link_col,
                left_id=left_id,
                right_id=right_id,
                new_type=None,
                new_priority=new_priority,
            )
            # Then change the link type
            link_type_col = "{0}_type".format(link_base_col)
            stmt = "UPDATE {0} SET {3} = ? WHERE {1} = ? AND {2} = ?;".format(
                link_table, left_link_col, right_link_col, link_type_col
            )
            self.execute(stmt, (new_type, left_id, right_id))

    # Todo: primary_language table
    # Todo: Tests how this responds when you set the land_id to None - should be fine, but check
    def set_title_primary_language(self, title_id, lang_id):
        """
        Set the primary title of a work. The primary language of the title is scrubbed and replaced with the given
        language id.
        :param title_id: Id of the title to set the primary language for
        :param lang_id: The id of the language to set primary for the given title
        :return:
        """
        # There can only be one primary language link between the title and the languages table
        del_stmt = (
            "DELETE FROM language_title_links "
            "WHERE language_title_link_title_id = ? AND language_title_link_type = 'primary';"
        )
        self.execute(del_stmt, (title_id,))

        title_row = self.db.get_row_from_id("titles", row_id=title_id)
        lang_row = self.db.get_row_from_id("languages", row_id=lang_id)

        try:
            self.db.interlink_rows(
                primary_row=title_row,
                secondary_row=lang_row,
                type="primary",
                priority="highest",
            )
        except DatabaseIntegrityError:
            # If there are language title links which are not primary
            del_stmt = (
                "DELETE FROM language_title_links "
                "WHERE language_title_link_title_id = ? AND language_title_link_language_id = ?;"
            )
            self.execute(del_stmt, (title_id, lang_id))

            self.db.interlink_rows(
                primary_row=title_row,
                secondary_row=lang_row,
                type="primary",
                priority="highest",
            )

        # # Add back a link between the title and the new entry
        # insert_stmt = "INSERT INTO language_title_links " \
        #               "(language_title_link_title_id, language_title_link_language_id, language_title_link_type)" \
        #               "VALUES (?, ?, 'primary');"
        # self.execute(insert_stmt, (title_id, lang_id))

    def library_unset_series(self, title_id, series_id):
        """
        Used to remove a specific link between a title and a series.
        :param db:
        :param title_id:
        :param series_id:
        :return:
        """
        del_stmt = (
            "DELETE FROM series_title_links "
            "WHERE series_title_link_title_id = ? AND series_title_link_series_id= ? ;"
        )
        self.execute(del_stmt, (title_id, series_id))

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - READ METHODS - FOR READING DATA FROM THE BACKEND
    def read_link_property_trios(self, link_table, link_property_col, first_id, second_id):
        """
        Used when caching additional properties associated with a link. Returns a trio of values of the form
        (link_property_col, first_id, second_id).
        E.g. if you want the index values of all the series_title_links then call this method with
        (series_title_links, series_title_link_index, series_title_link_series_id, series_title_link_title_id)
        and you will receive back an iterable of trios of those three values.
        :param link_table: The name of the link table to read from
        :param link_property_col:
        :param first_id:
        :param second_id:
        :return:
        """
        stmt = "SELECT {0}, {1}, {2} FROM {3};".format(link_property_col, first_id, second_id, link_table)
        return self.execute(stmt)

    def get_all_table_link_data(self, table1, table2, typed=False, priority=False):
        """
        Method to return all the link data in a given table.

        if the link is neither typed or priority, then the return will be a dictionary of dictionaries, keyed with the
        id of the first table, then the id of the second table, then a set of all the ids

        if the link is priority, but not types, the sets becomes lists of all the ids in table2, in priority order

        if the link is typed, but not priority, the return becomes a dictionary of dicts of dicts.
        First level keyed with the ids from table1, then the types, then sets of all the ids fro table2

        If the link is both types and priority the dict of dicts of dicts structure remains, but the last level is
        valued with lists of the ids in priority order, not sets.

        :param table1: The primary table
        :param table2: The secondary table
        :param typed:
        :param priority:
        :return:
        """

        all_table_link_data = dict()

        for target_row in self.db.get_all_rows(table1):

            all_table_link_data[target_row.row_id] = dict(
                self.get_link_data(
                    table1,
                    table2,
                    table1_id=target_row.row_id,
                    typed=typed,
                    priority=priority,
                )
            )

        return all_table_link_data

    def get_link_data(self, table1, table2, table1_id, typed=False, priority=False):
        """
        Return an object containing the data for all the items in table2 linked to table1.
        :param table1:
        :param table2:
        :param table1_id:
        :param typed:
        :param priority:
        :return:
        """
        table2_id_col = self.db.driver_wrapper.get_id_column(table2)

        if not typed and not priority:
            table1_row = self.db.get_row_from_id(table1, table1_id)
            linked_rows = self.db.get_interlinked_rows(table1_row, table2)
            return set([lr[table2_id_col] for lr in linked_rows])

        elif not typed and priority:
            table1_row = self.db.get_row_from_id(table1, table1_id)
            linked_rows = self.db.get_interlinked_rows(table1_row, table2)
            return [lr[table2_id_col] for lr in linked_rows]

        elif typed and not priority:
            table1_id_col = self.db.driver_wrapper.get_id_column(table1)

            link_table = self.db.driver_wrapper.get_link_table_name(table1, table2)

            link_table_type_col = self.db.driver_wrapper.get_link_column(table1, table2, "type")
            link_table_t1_id_col = self.db.driver_wrapper.get_link_column(table1, table2, table1_id_col)
            link_table_t2_id_col = self.db.driver_wrapper.get_link_column(table1, table2, table2_id_col)

            stmt = "SELECT {1}, {2} FROM {3} WHERE {0} = ?;".format(
                link_table_t1_id_col,
                link_table_t2_id_col,
                link_table_type_col,
                link_table,
            )

            link_container = defaultdict(set)
            for other_id, link_type in self.db.driver_wrapper.execute(stmt, table1_id):
                link_container[link_type].add(other_id)

            return link_container

        elif typed and priority:
            table1_id_col = self.db.driver_wrapper.get_id_column(table1)

            link_table = self.db.driver_wrapper.get_link_table_name(table1, table2)

            link_table_type_col = self.db.driver_wrapper.get_link_column(table1, table2, "type")
            link_table_priority_col = self.db.driver_wrapper.get_link_column(table1, table2, "priority")
            link_table_t1_id_col = self.db.driver_wrapper.get_link_column(table1, table2, table1_id_col)
            link_table_t2_id_col = self.db.driver_wrapper.get_link_column(table1, table2, table2_id_col)

            stmt = "SELECT {1}, {2} FROM {3} WHERE {0} = ? ORDER BY {4} DESC ;".format(
                link_table_t1_id_col,
                link_table_t2_id_col,
                link_table_type_col,
                link_table,
                link_table_priority_col,
            )

            link_container = defaultdict(list)
            for other_id, link_type in self.db.driver_wrapper.execute(stmt, table1_id):
                link_container[link_type].append(other_id)

            return link_container

        else:
            raise NotImplementedError

    def get_title_series_ids_set(self, title_id):
        """
        Returns as set of all the series ids associated with a title id
        :param title_id:
        :return:
        """
        stmt = "SELECT series_title_link_series_id FROM series_title_links WHERE series_title_link_title_id = ?;"
        return set(row[0] for row in self.execute(stmt, (title_id,)))

    def get_linked_ids(self, link_table, left_id_col, right_id_col, left_id, type_filter=None):
        """
        Return the ids linked to the given left_id in the specified link table.
        :param link_table:
        :param left_id_col:
        :param left_id:
        :param type_filter: If it's specified (not None) then only entries with the given type will be returned
        :return:
        """
        if type_filter is None:
            stmt = "SELECT {0} FROM {1} WHERE {2} = ?;".format(right_id_col, link_table, left_id_col)
            return set(row[0] for row in self.execute(stmt, (left_id,)))
        else:
            link_type_col = "{0}_type".format(self.db.driver_wrapper.get_column_base(link_table))
            stmt = "SELECT {0} FROM {1} WHERE {2} = ? AND {3} = ?;".format(
                right_id_col, link_table, left_id_col, link_type_col
            )
            return set(row[0] for row in self.execute(stmt, (left_id, type_filter)))

    # Todo: Not actually file macros..
    # - FILE MACROS
    def read_creator_with_sort_and_link(self):
        """
        Returns an iterable of types of the form (creator_id, creator, creator_sort, creator_link) from the creators
        table.
        :return:
        """
        stmt = "SELECT creator_id, creator, creator_sort, creator_link FROM creators;"
        return self.execute(stmt)

    def read_book_id_with_cover_id_and_cover_nmame(self):
        """
        Designed for the initial read of the covers table - returns a tuple of the form (book_id, cover_id, cover_fname)
        in priority order for the books (so if a book_id appears twice in the sequence the second time it appears
        will correspond to the second cover in the priority order for that book)
        :return:
        """
        stmt = """
                SELECT books.book_id, covers.cover_id, covers.cover_name
                  FROM books
                  JOIN book_cover_links
                    ON books.book_id = book_cover_links.book_cover_link_book_id
                  JOIN covers
                    ON book_cover_links.book_cover_link_cover_id = covers.cover_id
              ORDER BY book_cover_links.book_cover_link_priority DESC;"""
        return self.execute(stmt)

    def read_book_id_with_file_id_file_ext_file_name_and_file_size(self):
        """
        For the initial read of the formats table - returns a tuple of the form
        (book_id, file_id, fmt, file_name, file_size)
        in priority order for the format in the book.
        So, if a book_id appears twice in the sequence the second time it appears will be for the second format in the
        book.
        :return:
        """
        stmt = """
                SELECT books.book_id, files.file_id, files.file_extension, files.file_name, files.file_size
                  FROM books
                  JOIN book_file_links
                    ON books.book_id = book_file_links.book_file_link_book_id
                  JOIN files
                    ON book_file_links.book_file_link_file_id = files.file_id
              ORDER BY book_file_links.book_file_link_priority DESC;"""
        return self.execute(stmt)

    def read_file_backups_for_book(self, book_id):
        """
        One of the options available to the user is to back up a format before making changes to it.
        These backups are noted as such on the database with title-title links.
        Reads and returns the backup title-title links for the given book_id.
        :param book_id:
        :return:
        """
        backup_stmt = """
                SELECT file_file_intralinks.file_file_intralink_primary_id, file_file_intralinks.file_file_intralink_secondary_id
                  FROM books
                  JOIN book_file_links
                    ON books.book_id = book_file_links.book_file_link_book_id
                  JOIN files
                    ON book_file_links.book_file_link_file_id = files.file_id
                  JOIN file_file_intralinks
                    ON files.file_id = file_file_intralinks.file_file_intralink_primary_id
                 WHERE books.book_id = ?
                 ORDER BY book_file_links.book_file_link_priority DESC;"""
        return self.execute(backup_stmt, book_id)

    def read_file_properties_for_book(self, book_id):
        """
        Reads the file properties for a single database book.
        Returns an iterable of tuples - file_id, fmt, file_name, file_size in priority order.
        :return:
        """
        stmt = """
                SELECT files.file_id, files.file_extension, files.file_name, files.file_size
                  FROM books
                  JOIN book_file_links
                    ON books.book_id = book_file_links.book_file_link_book_id
                  JOIN files
                    ON book_file_links.book_file_link_file_id = files.file_id
                 WHERE books.book_id = ?
                 ORDER BY book_file_links.book_file_link_priority DESC;"""
        return self.execute(stmt, book_id)

    def read_all_identifiers(self):
        """
        Reads all the identifiers from the identifiers table into memory.
        Returns a tuple of the form book, typ, val - the book id for the identifier - the type of the identifier and the
        value of the identifier.
        :return:
        """
        stmt = """
                SELECT identifier_title_links.identifier_title_link_title_id, 
                identifier_title_links.identifier_title_link_type,
                identifiers.identifier
                FROM identifier_title_links JOIN identifiers
                ON identifier_title_links.identifier_title_link_identifier_id = identifiers.identifier_id
                ORDER BY identifier_title_links.identifier_title_link_priority DESC;
                """
        return self.execute(stmt)

    def read_book_sizes_sum_mode(self):
        """
        Reads the tuple book_id, file_size (where size is computed as the sum of all the individual file sizes) from the
        files table.
        :return:
        """
        stmt = """
                    SELECT books.book_id,(SELECT SUM(files.file_size) FROM files WHERE files.file_id IN
                    (SELECT file_folder_links.file_folder_link_file_id FROM file_folder_links
                    WHERE file_folder_links.file_folder_link_folder_id IN
                    (SELECT book_folder_links.book_folder_link_folder_id FROM book_folder_links
                    WHERE book_folder_links.book_folder_link_book_id = books.book_id))) FROM books;
                    """
        return self.execute(stmt)

    def read_book_sizes_max_mode(self):
        """
        Reads the tuple book_id, file_size (where size is computed as the max of all the individual file sizes) from the
        files table.
        :return:
        """
        stmt = """
                    SELECT books.book_id,(SELECT MAX(files.file_size) FROM files WHERE files.file_id IN
                    (SELECT file_folder_links.file_folder_link_file_id FROM file_folder_links
                    WHERE file_folder_links.file_folder_link_folder_id IN
                    (SELECT book_folder_links.book_folder_link_folder_id FROM book_folder_links
                    WHERE book_folder_links.book_folder_link_book_id = books.book_id))) FROM books;
                    """
        return self.execute(stmt)

    def read_book_sizes_min_mode(self):
        """
        Reads the tuple book_id, file_size (where size is computed as the min of all the individual file sizes) from the
        files table.
        :return:
        """
        stmt = """
                    SELECT books.book_id,(SELECT MIN(files.file_size) FROM files WHERE files.file_id IN
                    (SELECT file_folder_links.file_folder_link_file_id FROM file_folder_links
                    WHERE file_folder_links.file_folder_link_folder_id IN
                    (SELECT book_folder_links.book_folder_link_folder_id FROM book_folder_links
                    WHERE book_folder_links.book_folder_link_book_id = books.book_id))) FROM books;
                    """
        return self.execute(stmt)

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - CUSTOM COLUMN MACROS
    def ensure_custom_column_value(self, cc_table, value):
        """
        Add values to a custom column - values are assumed to not already exist.
        :return:
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

    # Todo: Do you need two different unique ids stores in two different places?
    def set_library_id(self, new_val):
        """
        Preform a set of the library id.
        :param new_val:
        :return:
        """
        if self.db.driver_wrapper.get_record_count("library_id"):
            self.execute("UPDATE library_id SET library_id_uuid = ?", (new_val,))

        else:
            self.execute("INSERT INTO library_id (library_id_uuid) VALUES (?);", (new_val,))

    def set_database_version(self, new_val):
        """
        Preform a set of the database_version.
        This should be possible, but rarely used. I'm leaving the code in her in anticipation that, when multiple
        versions of LiuXin are in the wild, we might want to allow for database upgrades.
        So you might, then, want to be able to change the version.
        :param new_val:
        :return:
        """
        if self.db.driver_wrapper.get_record_count("library_id"):
            self.execute("UPDATE database_version SET database_version_version = ?", (new_val,))

        else:
            self.execute(
                "INSERT INTO database_version (database_version_version) VALUES (?);",
                (new_val,),
            )

    def set_title_identifier(self, title_id, id_type, id_val):
        """
        Set an identifier linked to a title - the identifier will be set as the primary of that type linked to the
        title.
        :param db: The database to write the changes into
        :param title_id: The id of the title to link the identifier to
        :param id_type: The identifier type (e.g. isbn, e.t.c)
        :param id_val: The identifier will be set to this.
                       No normalization is done - this is supposed to be the name of a valid identifier type which is
                       known to the database.
        :return:
        """
        if id_val:
            # The database has to be updated
            # Todo: Should be using the ensure.identifier method instead of add
            try:
                ident_row = self.db.add.identifier(identifier=id_val, identifier_type=id_type)
            except DatabaseIntegrityError:
                # Identifier already exists - retrieve the row and promote it to the highest priority
                ident_row = self.db.ensure.identifier(identifier=id_val, identifier_type=id_type, error=False)
                ident_id = ident_row["identifier_id"]

                # Check to see if there is already a link between the identifier and the title
                stmt = (
                    "SELECT identifier_title_link_id "
                    "FROM identifier_title_links "
                    "WHERE identifier_title_link_title_id = ? AND identifier_title_link_identifier_id = ?;"
                )
                it_status = self.db.driver.conn.get(stmt, (title_id, ident_id), all=False)

                # The link exists - it just needs to be promoted to the top of the stack
                if it_status:
                    # Retrieve the row
                    it_link = self.db.get_row_from_id("identifier_title_links", it_status)
                    # Maximize the priority
                    it_link["identifier_title_link_priority"] = self.db.get_max("identifier_title_link_priority") + 1
                    it_link.sync()
                else:
                    raise DatabaseIntegrityError("Cannot link to this identifier - it's linked to another title")

            else:
                title_row = self.db.get_row_from_id(table="titles", row_id=title_id)
                self.db.apply.identifier(resource_row=title_row, identifier=ident_row, identifier_type="isbn")

        else:
            # isbn has been passed in as none - wipe all the identifiers of that type linked to the title
            # Foreign keys should also take out the entries on the identifiers table itself
            stmt = (
                "DELETE FROM identifier_title_links "
                "WHERE identifier_title_link_title_id = ? AND identifier_title_link_type = ?;"
            )
            self.db.driver.conn.execute(stmt, (title_id, id_type))
            self.db.driver.conn.commit()

    def set_title_isbn(self, title_id, isbn):
        """
        Set a isbn in the identifiers table for a particular title.
        :param title_id: The id of the book to update.
        :param isbn: The isbn of the book to update.
        :return:
        """
        self.set_title_identifier(title_id=title_id, id_type="isbn", id_val=isbn)

    def set_title_rating(self, title_id, rating):
        """
        Sets the user_rating for the given id - which is the one used in the meta view.
        The rating table should have already been set up by this point - just calculating the appropriate row_id and
        writing it into the ratings table.
        Updates the database - does not update the cache.
        :param db: The database to do the update on
        :param title_id: The id of the book to set the rating for
        :param rating: An integer in the range 0-10.
                       If the integer is 0 - or if the rating evaluates to 0, the rating will be set Null.
        :type rating: int
        :return:
        """
        # Clear the ratings table of any current user_ratings for the title
        self.db.driver.conn.execute(
            "DELETE FROM rating_title_links "
            "WHERE rating_title_link_type = 'user'"
            "AND rating_title_link_title_id = ?;",
            (title_id,),
        )

        if not rating:
            return
        rating = int(rating) + 1

        rat_row_id = rating
        self.db.driver.conn.execute(
            "INSERT INTO rating_title_links "
            "(rating_title_link_title_id, rating_title_link_rating_id, rating_title_link_type) "
            "VALUES (?,?,?);",
            (title_id, rat_row_id, "user"),
        )
        self.db.driver.conn.commit()

    # Todo: THe below probably needs to be tested

    def unapply_series_tags(self, series_id, tags):
        """
        Remove all the tags in the given itterator from the given series.
        :param series_id:
        :param tags:
        :return:
        """
        for tag in tags:
            tag_id = self.db.driver.conn.get("SELECT tag_id FROM tags WHERE tag=?", (tag,), all=False)
            if tag_id:
                self.db.driver.conn.execute(
                    "DELETE FROM series_tag_links " "WHERE series_tag_link_tag_id=? " "AND series_tag_link_series_id=?",
                    (tag_id, series_id),
                )
        self.db.driver.conn.commit()
        self.db.driver.conn.commit()

    def update_feed(self, feed_id, script, title):
        """
        Update a feed stored in the feeds table.
        :param feed_id:
        :param script:
        :param title:
        :return:
        """
        self.db.driver.conn.execute("UPDATE feeds set feed_title=? WHERE feed_id=?", (title, feed_id))
        self.db.driver.conn.execute("UPDATE feeds set feed_script=? WHERE feed_id=?", (script, feed_id))
        self.db.driver.conn.commit()

    def set_feeds(self, feeds):
        """
        Clears an entire feed table and populate the table anew with an iterator.
        :param feeds:
        :return:
        """
        self.db.driver.conn.execute("DELETE FROM feeds")
        for title, script in feeds:
            self.db.driver.conn.execute(
                "INSERT INTO feeds(feed_title, feed_script) VALUES (?, ?)",
                (title, script),
            )
        self.db.driver.conn.commit()

    def set_author_sort(self, title_id, sort):
        """
        Set the author sort for a given title id.
        :param title_id:
        :param sort:
        :return:
        """
        self.db.driver.conn.execute("UPDATE titles SET title_creator_sort=? WHERE title_id=?;", (sort, title_id))
        self.db.driver.conn.commit()

    def set_has_cover(self, book_id, value):
        """
        Set the has_cover field for the specified book.
        :param book_id:
        :param value:
        :return:
        """
        self.db.driver.conn.execute("UPDATE books SET book_has_cover=? WHERE book_id=?;", (value, book_id))
        self.db.driver.conn.commit()

    def remove_unused_series(self):
        """
        Remove series which are not currently in use - i.e. linked to the titles table.
        :return:
        """
        for (series_id,) in self.db.driver.conn.get("SELECT series_id FROM series"):
            if not self.db.driver.conn.get(
                "SELECT series_title_link_id " "FROM series_title_links " "WHERE series_title_link_series_id=?",
                (series_id,),
            ):
                self.db.driver.conn.execute("DELETE FROM series WHERE series_id=?", (series_id,))
        self.db.driver.conn.commit()

    def set_conversion_options(self, book_id, fmt, options):
        """
        Set a conversion option for a book.
        :param book_id:
        :param fmt:
        :param options:
        :return:
        """
        data = sqlite.Binary(cPickle.dumps(options, -1))
        oid = self.db.driver.conn.get(
            "SELECT conversion_option_id FROM conversion_options "
            "WHERE conversion_option_book=? AND conversion_option_format=?",
            (book_id, fmt.upper()),
            all=False,
        )

        if oid:
            self.db.driver.conn.execute(
                "UPDATE conversion_options " "SET conversion_option_data=? " "WHERE conversion_option_id=?",
                (data, oid),
            )
        else:
            self.db.driver.conn.execute(
                "INSERT INTO conversion_options"
                "(conversion_option_book,"
                "conversion_option_format,"
                "conversion_option_data) VALUES (?,?,?)",
                (book_id, fmt.upper(), data),
            )
        self.db.driver.conn.commit()

    def delete_conversion_options(self, book_id, fmt, commit=True):
        """
        Delete a conversion option for a format from a given id.
        :param book_id:
        :param fmt:
        :param commit:
        :return:
        """
        stmt = "DELETE FROM conversion_options WHERE conversion_option_book=? AND conversion_option_format=?"
        self.db.driver.conn.execute(stmt, (book_id, fmt.upper()))
        if commit:
            self.db.driver.conn.commit()

    def clear_publisher_title_links_by_title_id(self, title_id):
        """
        Remove all publisher title links with the publisher being linked to the given title_id.
        :param title_id:
        :return:
        """
        del_stmt = "DELETE FROM publisher_title_links " "WHERE publisher_title_link_title_id = ?;"
        self.db.driver_wrapper.execute(del_stmt, (title_id,))

    def check_for_title_id_publisher_id_link(self, pub_id, title_id):
        """
        Check to see if there is an existing link between a given publisher id and a given title id.
        :param pub_id:
        :param title_id:
        :return:
        """
        stmt = (
            "SELECT publisher_title_link_id "
            "FROM publisher_title_links "
            "WHERE publisher_title_link_publisher_id = ? AND publisher_title_link_title_id = ? "
            "ORDER BY publisher_title_link_priority DESC;"
        )
        pt_id = self.db.driver.conn.get(stmt, (pub_id, title_id), all=False)
        return pt_id

    def clear_null_publisher_links_from_title(self, title_id):
        """
        Remove all the links to publisher 0, linked to the specified title_id, are removed.
        :param title_id:
        :return:
        """
        del_stmt = (
            "DELETE FROM publisher_title_links "
            "WHERE publisher_title_link_publisher_id = 0 AND publisher_title_link_title_id = ?;"
        )
        self.db.driver_wrapper.execute(del_stmt, (title_id,))

    def link_publisher_to_null_publisher_row(self, title_id):
        """
        Link the null publisher row to a title with maximum priorityt.
        :param title_id:
        :return:
        """
        # Nullify the publisher - by linking it to the null pub row
        stmt = (
            "INSERT INTO publisher_title_links "
            "(publisher_title_link_title_id, publisher_title_link_publisher_id, "
            "publisher_title_link_priority) "
            "SELECT ?, 0, MAX(publisher_title_link_priority) + 1 FROM publisher_title_links;"
        )

        try:
            self.db.driver_wrapper.execute(stmt, (title_id,))
        except DatabaseDriverError:
            # Link has already been set null
            pass

    def clear_title_comments_from_title_id(self, title_id):
        """
        Remove all the comments linked to a title with the given id.
        :param title_id:
        :return:
        """
        stmt = "DELETE FROM comment_title_links WHERE comment_title_link_title_id = ?;"
        self.db.driver_wrapper.execute(stmt, (title_id,))

    def delete_tag_by_value(self, tag):
        """
        Delete a tag from the tags table using the value of the tag in that table.
        :param tag:
        :return:
        """
        self.db.driver.conn.execute("DELETE FROM tags WHERE tag=?;", (tag,))
        self.db.driver.conn.commit()

    def get_tag_id_from_value(self, tag):
        """
        Retrieve the tag id corresponding to the given tag value.
        :param tag:
        :return:
        """
        return self.db.driver.conn.get("SELECT tag_id FROM tags WHERE tag=?", (tag,), all=False)

    def break_tag_title_link(self, tag_id, title_id):
        """
        Break a link, if one exists, between the given title and tag
        :param tag_id:
        :param title_id:
        :return:
        """
        self.db.driver.conn.execute(
            "DELETE FROM tag_title_links " "WHERE tag_title_link_tag_id=? " "AND tag_title_link_title_id=?",
            (tag_id, title_id),
        )

    def break_creator_tag_link(self, tag_id, creator_id):
        """
        Break a list - if one exists - between the given creator and given tag.
        :param tag_id:
        :param creator_id:
        :return:
        """
        self.db.driver.conn.execute(
            "DELETE FROM creator_tag_links " "WHERE creator_tag_link_tag_id=? " "AND creator_tag_link_creator_id=?",
            (tag_id, creator_id),
        )

    def add_tag(self, tag_value):
        """
        Add a tag to the database and return the lastrowid - hopefully corresponding to that tag.
        :param tag_value:
        :return:
        """
        return self.db.driver.conn.execute("INSERT INTO tags(tag) VALUES(?);", (tag_value,)).lastrowid

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - CREATOR_TITLE_MACROS

    def clear_tag_title_links_for_title(self, title_id):
        """
        Clear the tags linked to a given title.
        :param title_id:
        :return:
        """
        self.db.driver.conn.execute("DELETE FROM tag_title_links WHERE tag_title_link_title_id=?;", (title_id,))

    def check_for_tag_title_link(self, title_id, tag_id):
        """
        Check to see if there's a link between the given title and tag.
        :param title_id:
        :param tag_id:
        :return:
        """
        return self.db.driver.conn.get(
            "SELECT tag_title_link_title_id "
            "FROM tag_title_links "
            "WHERE tag_title_link_title_id=? AND tag_title_link_tag_id=?;",
            (title_id, tag_id),
            all=False,
        )

    def add_tag_title_link(self, title_id, tag_id):
        """
        Add a link betwekn the given tag and title
        :param title_id:
        :param tag_id:
        :return:
        """
        self.db.driver.conn.execute(
            "INSERT INTO tag_title_links" "(tag_title_link_title_id, tag_title_link_tag_id) VALUES (?,?)",
            (title_id, tag_id),
        )

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - CREATOR_TAG_MACROS

    def clear_creator_tag_links_for_creator(self, creator_id):
        """
        Clear the tags linked to a given creator.
        :param creator_id:
        :return:
        """
        self.db.driver.conn.execute(
            "DELETE FROM creator_tag_links WHERE creator_tag_link_creator_id=?;",
            (creator_id,),
        )

    def check_for_creator_tag_link(self, creator_id, tag_id):
        """
        Check to see if there's a link between the given title and tag.
        :param creator_id:
        :param tag_id:
        :return:
        """
        return self.db.driver.conn.get(
            "SELECT creator_tag_link_creator_id "
            "FROM creator_tag_links "
            "WHERE creator_tag_link_creator_id=? AND creator_tag_link_tag_id=?;",
            (creator_id, tag_id),
            all=False,
        )

    def add_creator_tag_link(self, creator_id, tag_id):
        """
        Add a link between a given creator and tag.
        :param title_id:
        :param tag_id:
        :return:
        """
        self.db.driver.conn.execute(
            "INSERT INTO creator_tag_links" "(creator_tag_link_creator_id, creator_tag_link_tag_id) VALUES (?,?)",
            (creator_id, tag_id),
        )

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - SERIES_TAG_MACROS

    def clear_series_tag_links_for_series(self, series_id):
        """
        Clear the tags linked to a given creator.
        :param series_id:
        :return:
        """
        self.db.driver.conn.execute(
            "DELETE FROM series_tag_links WHERE series_tag_link_series_id=?;",
            (series_id,),
        )

    def check_for_series_tag_link(self, series_id, tag_id):
        """
        Check to see if there's a link between the given title and tag.
        :param series_id:
        :param tag_id:
        :return:
        """
        return self.db.driver.conn.get(
            "SELECT series_tag_link_series_id "
            "FROM series_tag_links "
            "WHERE series_tag_link_series_id=? AND series_tag_link_tag_id=?;",
            (series_id, tag_id),
            all=False,
        )

    def add_series_tag_link(self, series_id, tag_id):
        """
        Add a link between a given creator and tag.
        :param series_id:
        :param tag_id:
        :return:
        """
        self.db.driver.conn.execute(
            "INSERT INTO series_tag_links" "(series_tag_link_series_id, series_tag_link_tag_id) VALUES (?,?)",
            (series_id, tag_id),
        )

    #
    # ------------------------------------------------------------------------------------------------------------------
