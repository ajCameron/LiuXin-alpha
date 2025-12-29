import uuid

from LiuXin.databases.drivers.SQLite.macros import SQLiteDatabaseMacros

# - one_to_one
from LiuXin.databases.caches.base.tables import BaseTable
from LiuXin.databases.caches.base.tables import BaseOneToOneTable
from LiuXin.databases.caches.base.tables import BasePathTable
from LiuXin.databases.caches.base.tables import BaseSizeTable
from LiuXin.databases.caches.base.tables import BaseUUIDTable
from LiuXin.databases.caches.base.tables import BaseCompositeTable

# - many_to_one
from LiuXin.databases.caches.base.tables import BaseManyToOneTable

# - many_to_many
from LiuXin.databases.caches.base.tables import BaseManyToManyTable
from LiuXin.databases.caches.base.tables import BaseTypedManyToManyTable
from LiuXin.databases.caches.base.tables import BaseCreatorsTable
from LiuXin.databases.caches.base.tables import BaseFormatsTable

from LiuXin.utils.calibre import isbytestring, force_unicode
from LiuXin.utils.logger import default_log

# Py2/Py3 compatibility layer
from LiuXin.utils.lx_libraries.liuxin_six import iteritems


# Todo: Need to use this error instead of the standard db errors
class MemorySQLiteError(Exception):
    pass


# ----------------------------------------------------------------------------------------------------------------------
#
# - ONE TO ONE TABLES


class SQLiteTable(BaseTable):
    """
    Represents a table in the cache.
    Stores the information needed to preform, process and return queries on the in memory database.
    """

    def get_value(self, rid):
        """
        Return the value for the given id.
        :param rid:
        :return:
        """
        raise NotImplementedError


class SQLiteOneToOneTable(SQLiteTable, BaseOneToOneTable):
    def __init__(self, name, metadata, link_table=None, custom=False):
        """
        Setup for a OneToOne table.
        Used for elements with a one to one relation with books and titles.
        :param name:
        :param metadata:
        :param link_table: If applicable, the table linking this table to the titles or books table
        :param custom:
        """
        super(SQLiteOneToOneTable, self).__init__(name, metadata, link_table, custom)

        self.memory_db = None
        self.macros = None

        # Properties of the table - later used to more easily retrieve values
        if self.metadata["table"] is None:
            self.meta = True
            try:
                self.table = self.metadata["in_table"]
            except KeyError:
                self.table = "meta"
        else:
            self.meta = False
            self.table = self.metadata["table"]

        self.column = self.metadata["column"]
        if self.meta:
            self.table_id_col = "id"
        else:
            self.table_id_col = "book_id" if self.table == "books" else "title_id"

    # Todo - A call to read must trigger a re-read and refresh onm all the tables
    def startup(self, memory_db, db):
        self.memory_db = memory_db
        self.macros = self.memory_db.macros
        self.set_link_tables(db)

    def remove_books(self, book_ids, db):
        """
        Remove books from the cache.
        Books are not removed from the actual database - just deleted from the cache.
        :param book_ids:
        :param db:
        :return:
        """
        for book_id in book_ids:
            self.macros.delete_title(title_id=book_id)

    def get_value(self, rid, default_value=None):
        """
        Return the value for the given id.
        :param rid:
        :param default_value: Return this if the book_id cannot be found in the table
        :return:
        """
        if self.meta:
            return self.memory_db.macros.get_values_one_condition(
                table="meta",
                rtn_column=self.column,
                cond_column=self.table_id_col,
                value=rid,
                default_value=default_value,
            )
        else:
            return self.memory_db.macros.get_values_one_condition(
                table=self.table,
                rtn_column=self.column,
                cond_column=self.table_id_col,
                value=rid,
                default_value=default_value,
            )


class SQLitePathTable(SQLiteOneToOneTable, BasePathTable):
    def set_path(self, book_id, path, db):
        """
        Update the override path in the books table - both the one stored in the cache and the one in the database.
        :param book_id:
        :param path:
        :param db:
        :return:
        """
        self.macros.set_override_book_path(book_id=book_id, path=path)
        self.set_db_path(book_id, path, db)


class SQLiteSizeTable(SQLiteOneToOneTable, BaseSizeTable):
    """
    Represents the size of the book.
    """

    def __init__(self, name, metadata, link_table=None):

        SQLiteOneToOneTable.__init__(self, name, metadata, link_table)

        self.size_mode = None
        self._parse_size_mode()


class SQLiteUUIDTable(SQLiteOneToOneTable, BaseUUIDTable):
    """
    UUID table - one to one as every title is assigned one UUID.
    """

    def lookup_by_uuid(self, uuid):
        db_result = self.memory_db.driver_wrapper.driver.conn.get(
            "SELECT book_id FROM books WHERE book_uuid=?", (uuid,)
        )
        try:
            return db_result[0][0]
        except IndexError:
            return None


class SQLiteCompositeTable(SQLiteOneToOneTable, BaseCompositeTable):
    def __init__(self, name, metadata, link_table=None, custom=False):

        SQLiteOneToOneTable.__init__(self, name, metadata, link_table, custom=custom)

        self.composite_template = None
        self.contains_html = False
        self.make_category = False
        self.composite_sort = False
        self.use_decorations = False

    def read(self, db):
        """
        Because the values for composite caches tend to be generated on the fly minimal actual reading is needed.
        :param db:
        :return:
        """

        d = self.metadata["display"]
        self.composite_template = ["composite_template"]
        self.contains_html = d.get("contains_html", False)
        self.make_category = d.get("make_category", False)
        self.composite_sort = d.get("composite_sort", False)
        self.use_decorations = d.get("use_decorations", False)

    def remove_books(self, book_ids, db):
        return set()


class SQLiteVirtualOneToOneTable(SQLiteOneToOneTable):
    """
    Temporary table created in the cache to
    """

    def __init__(self, name, metadata, link_table=None, custom=False):

        SQLiteOneToOneTable.__init__(self, name, metadata, link_table, custom=custom)

        self.temp_table_name = str(uuid.uuid4())


#
# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
#
# - MANY TO ONE TABLES


class SQLiteManyToOneTable(SQLiteOneToOneTable, BaseManyToOneTable):
    def __init__(self, name, metadata, link_table=None, custom=False):
        """
        Preform startup tasks - additionally read the properties of the link table into the class.
        :param args:
        :param kwargs:
        """
        super(SQLiteManyToOneTable, self).__init__(name=name, metadata=metadata, link_table=link_table, custom=custom)

        # These are true both for the main database and for the in memory copy
        self.link_to = None
        self.link_table = None

        # The column holding the id of the book or title in the link table
        self.link_table_bt_id_column = None

        # The column holding the id of the object which the book or title is linked to in the link table
        self.link_table_table_id_column = None

    def remove_books(self, book_ids, db):
        """
        Remove all the given books from this table in the cache.
        :param book_ids:
        :param db:
        :return:
        """
        for book_id in book_ids:
            self.macros.delete_title(book_id)

        # Todo: Retrieve the item ids and mark them for cleaning

    def remove_items(self, item_ids, db):
        """
        Remove all the given items from the cache.
        Items are removed from the link table - the underlying asset tables are not touched (as they might be linked
        to other objects).
        :param item_ids:
        :param db:
        :return:
        """
        del_stmt = "DELETE FROM {0} WHERE {1} = ?;".format(self.link_table, self.link_table_table_id_column)
        for item_id in item_ids:
            self.memory_db.driver_wrapper.execute(del_stmt, item_id)

        # Todo: Mark the item ids for cleaning

    def rename_item(self, item_id, new_name, db):
        """
        Preform a rename on an item in the cache.
        :param item_id:
        :param new_name:
        :param db:
        :return:
        """
        # Todo: Need to check for and deal with name clash - should currently simply fail
        table, column = self.metadata["table"], self.metadata["column"]
        id_col = self.table_id_col
        self.macros.direct_update_column_in_table(table, column, id_col, item_id, new_name)


#
# ----------------------------------------------------------------------------------------------------------------------
# ----------------------------------------------------------------------------------------------------------------------
#
# - MANY TO MANY TABLES


class SQLiteManyToManyTable(SQLiteManyToOneTable, BaseManyToManyTable):
    def remove_items(self, item_ids, db, restrict_to_book_ids=None):
        """
        Remove items from the table - updating the database and the cache.
        :param item_ids: Remove the items with the given ids
        :param db: From this database
        :param restrict_to_book_ids: Only remove items from the ids list which are linked to the given books
        :return affected_books: Books whose properties have changed
        """
        link_table = self.link_table
        item_link_col = self.link_table_table_id_column
        bt_link_col = self.link_table_bt_id_column

        affected_books = set()
        if restrict_to_book_ids is None:

            # Iterate through the item ids - removing the links to every item and recording the book ids
            for item_id in item_ids:

                # Record the affected books
                affected_books = affected_books.union(
                    self.memory_db.macros.get_values_one_condition(
                        table=link_table,
                        rtn_column=bt_link_col,
                        cond_column=item_link_col,
                        value=item_id,
                        default_value=set(),
                    )
                )

            # Purge all links to the item from the links table
            self.memory_db.macros.bulk_delete_in_table(
                table=link_table,
                column=item_link_col,
                column_values=tuple((tid,) for tid in item_ids),
            )

            # Actual database
            db.macros.bulk_delete_in_table(table=link_table, column=item_link_col, column_values=item_ids)

            # Mark the items that where just removed for potential cleaning
            db.maintainer.clean(table=self.metadata["table"], item_ids=item_ids)

            return affected_books

        else:

            restrict_to_book_ids = set(restrict_to_book_ids)

            # Generate all possible id combinations - try and remove all from the link - and
            id_pairs = []
            for book_id in restrict_to_book_ids:
                for item_id in item_ids:
                    id_pairs.append((book_id, item_id))

                    # Calculate the affected books - intersection of all the books linked to the id with the restriction
                    item_book_ids = self.memory_db.macros.get_values_one_condition(
                        table=link_table,
                        rtn_column=bt_link_col,
                        cond_column=item_link_col,
                        value=item_id,
                        default_value=set(),
                    )

                    affected_books = affected_books.union(item_book_ids.intersection(restrict_to_book_ids))

            # Remove all links - first from the cache and then from the database
            self.memory_db.macros.bulk_delete_items_in_table_two_matching_cols(
                table=link_table,
                col_1=bt_link_col,
                col_2=item_link_col,
                column_values=id_pairs,
            )

            db.macros.bulk_delete_items_in_table_two_matching_cols(
                table=link_table,
                col_1=bt_link_col,
                col_2=item_link_col,
                column_values=id_pairs,
            )

            return affected_books


class SQLiteTypedManyToManyTable(SQLiteManyToManyTable, BaseTypedManyToManyTable):
    """
    many_to_many table - for many to many relations where typing is important.
    """

    @property
    def seen_types(self):
        return self.macros.get_unique_values(table=self.link_table, column=self.link_table_type)


class SQLiteCreatorsTable(SQLiteTypedManyToManyTable, BaseCreatorsTable):
    """
    Basis for either a subtype of the creators table (e.g. Authors).
    """

    def __init__(self, name, metadata, link_table=None):
        super(SQLiteTypedManyToManyTable, self).__init__(name=name, metadata=metadata, link_table=link_table)

        # Set the link table for the authors - which is a subset for the creators table
        self.link_table = "creator_title_links"
        self.link_table_bt_id_column = "creator_title_link_title_id"
        self.link_table_table_id_column = "creator_title_link_creator_id"
        self.link_table_type = "creator_title_link_type"

    def get_creator_sort(self, cid):
        """
        Returns the creator sort from the cache.
        :param cid:
        :return:
        """
        return self.memory_db.macros.get_creator_sort(cid)

    def get_creator_link(self, cid):
        """
        Return the creator link from the cache.
        :param cid:
        :return:
        """
        return self.memory_db.macros.get_creator_link(cid)

    def set_sort_names(self, aus_map, db):
        """
        Update the database with the given author_sort map
        :param aus_map: An author_sort map
        :param db:
        :return aus_map: A processed author sort map - as it will actually be written into the database
        """
        # Prepare the sort maps for update
        aus_map = {aid: (a or "").strip() for aid, a in iteritems(aus_map)}
        aus_map = {aid: a for aid, a in iteritems(aus_map) if a != self.get_creator_sort(aid)}

        # Update the cache
        self.memory_db.macros.update_creator_sorts([(v, k) for k, v in iteritems(aus_map)])

        # Update the backend
        db.macros.update_creator_sorts([(v, k) for k, v in iteritems(aus_map)])
        return aus_map

    def set_links(self, link_map, db):
        """
        NOTE: THIS DOES NOT UPDATE THE LINKS BETWEEN CREATOR AND BOOKS, DESPITE THE CONFUSING NAME.
        This uses the link_map (keyed with the creator_id, valued with the value that the creator_link will have) to
        update the creators table with new links.
        :param link_map:
        :param db:
        :return link_map: With the standard transforms done on the values
        """
        link_map = {author_id: (l or "").strip() for author_id, l in iteritems(link_map)}
        link_map = {aid: l for aid, l in iteritems(link_map) if l != self.get_creator_link(aid)}

        self.memory_db.macros.update_creator_links([(v, k) for k, v in iteritems(link_map)])

        db.macros.update_creator_links([(v, k) for k, v in iteritems(link_map)])
        return link_map


class SQLiteFormatsTable(SQLiteManyToManyTable, BaseFormatsTable):
    """
    Class for the formats table - provides methods for dealing with formats.
    """

    def __init__(self, name, metadata, link_table=None):
        super(SQLiteFormatsTable, self).__init__(name=name, metadata=metadata, link_table=link_table)

        # Set the link table for the authors - which is a subset for the creators table
        self.link_table = "book_file_links"
        self.link_table_bt_id_column = "book_file_link_book_id"
        self.link_table_table_id_column = "book_file_link_file_id"

    def format_id_map_for_book(self, book_id):
        """
        Produce a dictionary keyed with the numbered format and valued with the id of the file.
        :param book_id:
        :return:
        """
        numbered_fmts = {}
        fmt_counts = {}
        for (
            file_id,
            file_ext,
            file_name,
            file_size,
        ) in self.memory_db.macros.read_file_properties_for_book(book_id):
            if file_ext in fmt_counts:
                fmt_counts[file_ext] += 1
                numbered_fmt = "{}_{}".format(file_ext.upper(), fmt_counts[file_ext])
            else:
                fmt_counts[file_ext] = 1
                numbered_fmt = "{}_{}".format(file_ext.upper(), fmt_counts[file_ext])
            numbered_fmts[numbered_fmt] = file_id
        return numbered_fmts

    def set_fname(self, book_id, fmt, fname, db):
        """
        Update the file name of a format in the database.
        :param book_id:
        :param fmt:
        :param fname:
        :param db:
        :return:
        """
        # We need to know what file the user wants to update
        fmt_file_map = self.format_id_map_for_book(book_id)
        file_id = fmt_file_map[fmt]

        # Preform the update in the cache
        self.memory_db.macros.set_file_name(file_id, fname)

        # Preform the update in the main database
        db.macros.set_file_name(file_id, fname)

        # Todo: Notify the maintainer that the file name has changed and this needs to be updated on disk


class SQLiteSeriesTable(SQLiteManyToManyTable):
    """
    Class for series and series link tables - provides index information as well as series info.
    """

    pass
