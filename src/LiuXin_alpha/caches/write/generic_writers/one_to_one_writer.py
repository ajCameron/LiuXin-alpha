
"""
One to one writers are responsible for writing to a single value item.

This may, or many not, be in another table.
"""

from __future__ import division, absolute_import, print_function, unicode_literals

from LiuXin_alpha.databases.adaptors import sqlite_datetime
from LiuXin_alpha.caches.write.base_writer import BaseWriter
from LiuXin_alpha.catalog import Catalog

from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems
from LiuXin_alpha.utils.logging import default_log


class OneToOneWriter(BaseWriter):
    """Coordinate database and cache changes for legacy scalar fields."""

    def __init__(self, field):
        super(OneToOneWriter, self).__init__(field)
        self.set_books_func = self.one_one_in_books if field.metadata["table"] == "books" else self.one_one_in_other

        if self.name in {"timestamp", "uuid", "sort"}:
            self.accept_vals = bool

    # Todo: Cache updates should be handled by a seperate process (with reference to the docstring)
    def one_one_in_books(self, book_id_val_map, db, field, *args):
        """
        Set fields for a one-one field in the books/title table.
        Preform an update of the database and cache for a generic database field.
        :param book_id_val_map: Keyed with the id of the book and valued with the value to set for in the database.
        :param db: db object to preform the update on
        :param field: Object representing the field to update (must have a column item - which describes the column to
                      update)
                      Typically an in memory store of the item.
        :param args:
        :return affects_ids: The book ids which have been changed by this operation
        """
        if args:
            info_str = "unexpected args passed to one_one_in_books"
            default_log.log_variables(info_str, "INFO", ("args", args))

        db_updater = {
            "series_index": self.series_index_one_one_db_updater,
            "last_modified": self.last_modified_one_one_db_updater,
        }.get(self.name, self.generic_one_one_db_updater)

        # Check that the given field is allowed in the books table and error if it isn't
        col = (
            field.metadata["column"]
            if "liuxin_table_name" not in field.metadata
            else field.metadata["liuxin_table_name"]
        )

        if "in_table" in field.metadata.keys():
            dst_table = field.metadata["in_table"]
        else:
            if col.startswith("book") or col.startswith("title"):
                dst_table = "books" if col.startswith("book") else "titles"
            else:
                dst_table = "titles"

        if dst_table == "titles" and not col.startswith("title"):
            table_col = "title_{}".format(col)
        elif dst_table == "books" and not col.startswith("book"):
            table_col = "book_{}".format(col)
        else:
            table_col = col

        # Todo: This is a stupid patch - fix it by renaming the column
        if table_col == "title_pubdate":
            table_col = "book_pubdate"

        if book_id_val_map:

            # Writing the changes out the database
            book_val_map = {k: sqlite_datetime(v) for k, v in iteritems(book_id_val_map)}

            db_updater(db=db, values_map=book_val_map, field=table_col, table=dst_table)

            # Updating the cache - if one is present in the field
            try:
                field.table.book_col_map.update(book_id_val_map)
            except AttributeError:
                pass

        # Return a set of the touched ids
        return set(book_id_val_map)

    # Todo: Comments should really be "one_many" - and need to test that this works properly with
    #       actualy one_one in other
    def one_one_in_other(self, book_id_val_map, db, field, *args):
        """
        Set a one-one field in a non-books table.
        If a field is not one-one, then the new value is guaranteed to be the highest priority of the item type linked
        to that book record - but old max value won't be deleted by default.
        This should provide calibre emulation - while retaining data for later use.
        :param book_id_val_map:
        :param db:
        :param field:
        :param args:
        :return:
        """
        field.table.update_precheck(
            book_id_item_id_map=book_id_val_map,
            id_map_update=dict(),
            acceptance_functions=[self.accept_vals, self.adapter],
        )

        if args:
            info_str = "Unexpected arguments passed to LiuXin.databases.write:one_one_in_other.\n"
            default_log.log_variables(info_str, "INFO", ("args", args))

        if not field.table.custom:
            db_updater = {"comments": self.comments_one_one_in_other_updater}.get(
                field.table.name, self.generic_one_one_in_other_updater
            )

        else:
            db_updater = self.cc_one_one_updater

        id_map = None

        # Process the book_id_val_map - if the value is set to None then all the entries in the other table should be
        # deleted
        deleted = tuple((k, None) for k, v in iteritems(book_id_val_map) if v is None)
        if deleted:

            if not field.table.custom:
                self.delete_one_to_one_in_other(db, field, deleted)
            else:
                self.custom_delete_one_to_one_in_other(db, field, deleted)

            # Todo: See below AND DO NOT DO THIS HERE - SEPERATION OF CONCERNS. THIS IS THE WRITER! IT WRITES TO THE DB!

            # Remove the deleted values form the cache - if the passed in field is a cache like object
            if hasattr(field, "table") and hasattr(field, "complex_update") and not field.complex_update:
                for book_id in deleted:
                    field.table.book_col_map.pop(book_id[0], None)

        # Make the text which will be written to the database - the cases where the comment are to be set None have
        # already been acted on
        updated = {k: v for k, v in iteritems(book_id_val_map) if v is not None}
        book_col_map = None
        if updated:

            id_map, book_col_map = db_updater(db, field, updated)

            # Todo: This is REALLY stupid - there is a call to a cache update method in the set_field function in the
            #       cache
            # which probably triggered all these calls - UPDATE THE DATABASE. THEN UPDATE THE CACHE. DO EACH with the
            # FUNCTIONS WHICH CLAIM TO DO THAT!

            # Update the cache - if the passed in field has a cache like structure
            if field.table.name != "comments":
                if hasattr(field, "table") and hasattr(field, "complex_update") and not field.complex_update:
                    field.table.book_col_map.update(updated)

        if id_map is None and not deleted:
            return set(book_id_val_map)

        elif id_map is None and deleted:
            rtn_info = dict()
            rtn_info["dirtied"] = set(book_id_val_map)
            rtn_info["id_map"] = None
            rtn_info["book_col_map"] = dict(did for did in deleted)
            return rtn_info

        else:
            # Todo: Need to rename id_map
            rtn_info = dict()
            rtn_info["dirtied"] = set(book_id_val_map)
            rtn_info["id_map"] = id_map
            rtn_info["book_col_map"] = book_col_map
            return rtn_info

    @staticmethod
    def generic_one_one_db_updater(db, values_map, field, table):
        """
        Generic update method - applies the book_id_val_map to the database.
        :param db:
        :param values_map:
        :param field:
        :param table:
        :return:
        """
        db.update_columns(values_map=values_map, field=field, table=table)

    @staticmethod
    def series_index_one_one_db_updater(db, values_map, field, table):
        """
        Do an update on the series_index - this should update the series index for the primary index of all entries in
        the values_map - creating a link to the null series if required.
        :param db: The database to do the update on
        :param values_map: Keyed with the id of the book and valued with the new series index
        :param field:
        :param table:
        :return:
        """
        for book_id in values_map:
            series_index_val = values_map[book_id]
            library_set_series_index(db=db, title_id=book_id, idx=series_index_val)

    @staticmethod
    def last_modified_one_one_db_updater(db, values_map, field, table):
        """
        Do an update on the last_modified field in the books table.
        :param db: The database to do the update on.
        :param values_map: Keyed with the book id and valued with the new last_modified value.
        :param field:
        :param table:
        :return:
        """
        for book_id in values_map:
            # Last-modified is a legacy ``books`` projection field, so the
            # Calibre compatibility adapter remains its storage owner.
            db.metadata_sql.update_book_last_modified(
                book_id=book_id,
                last_modified=values_map[book_id],
            )

    @staticmethod
    def comments_one_one_in_other_updater(db, field, updated):
        """
        Updater for the comments table
        :param db:
        :param field:
        :param updated:
        :return:
        """
        id_map = dict()
        book_col_map = dict()

        for book_id in updated:
            comment_val = updated[book_id]
            book_comment_id = Catalog(db).comments.replace_for_wemi(
                level="work",
                entity_id=book_id,
                data={"text": comment_val},
            )

            id_map[book_comment_id] = comment_val
            book_col_map[book_id] = book_comment_id

        return id_map, book_col_map

    @staticmethod
    def generic_one_one_in_other_updater(db, field, updated):
        """
        Generic one-one in other table updater.
        :return:
        """
        # Update the database - unlinking the records in the other database from the books - they should be fielded by
        # the maintenance bot
        # Todo: What? Probably shouldn't be comments
        for book_id, val in iteritems(updated):
            comment_row = db.get_blank_row("comments")
            comment_row["comment"] = val
            comment_row.sync()
            db.macros.make_generic_link(
                field.table.link_table,
                field.table.link_table_bt_id_column,
                field.table.link_table_table_id_column,
                field.table.link_table_priority_col,
                book_id,
                comment_row["comment_id"],
            )

        return None, None

    @staticmethod
    def cc_one_one_updater(db, field, updated):
        """
        Updater for the comments table
        :param db:
        :param field:
        :param updated:
        :return:
        """
        for book_id, val in iteritems(updated):

            # break the old link - if one exists
            db.macros.break_cc_lt_link(lt=field.metadata["table"], book=book_id)

            # write the new value to the custom column table
            db.macros.add_cc_link_with_extra(lt=field.metadata["table"], book_id=book_id, value_id=val)

        return None, None
