
"""
Write scalar legacy fields in owner columns or linked value tables.
"""

from __future__ import division, absolute_import, print_function, unicode_literals

from LiuXin_alpha.databases.adaptors import sqlite_datetime
from LiuXin_alpha.caches.write.base_writer import BaseWriter
from LiuXin_alpha.catalog import Catalog

from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems
from LiuXin_alpha.utils.logging import default_log


class OneToOneWriter(BaseWriter):
    """
    Adapt scalar values and coordinate legacy database and cache updates.

    The metadata table selects the owner-column path only when it equals "books"; other tables use linked-row handling. Public writes filter false values for fields named timestamp, uuid or sort before adaptation.

    Example:
        With metadata table="books", ``OneToOneWriter(field).set_books({7: value}, db)`` updates the resolved owner column.
    """

    def __init__(self, field):
        """
        Initialize scalar adaptation, choose the storage hook and set special-value filtering.

        Example:
            A field named "uuid" gets ``accept_vals = bool``; a field with metadata table="books" binds one_one_in_books.


        :param field: Legacy field supplying metadata and the required table/cache hooks.
        :return: None; stores field state and the selected update hook.
        """

        super(OneToOneWriter, self).__init__(field)
        self.set_books_func = self.one_one_in_books if field.metadata["table"] == "books" else self.one_one_in_other

        if self.name in {"timestamp", "uuid", "sort"}:
            self.accept_vals = bool

    # Todo: Cache updates should be handled by a seperate process (with reference to the docstring)
    def one_one_in_books(self, book_id_val_map, db, field, *args):
        """
        Resolve the legacy destination column, persist values and then update a simple cache.

        Choose the updater by writer name, then derive the column from liuxin_table_name when present, otherwise column. metadata.in_table overrides destination inference; otherwise book/title prefixes choose books/titles, defaulting to titles. Add the matching prefix when absent and rewrite title_pubdate to book_pubdate without changing the selected table. Nonempty updates pass sqlite_datetime-converted values to persistence, then merge original values into book_col_map; AttributeError during that merge is suppressed. Metadata is still required for empty updates, and other errors propagate without undoing persistence.

        Example:
            With metadata {"column": "sort", "in_table": "titles"}, a value for owner 7 is written to title_sort and its original value is cached afterward.


        :param book_id_val_map: Owner IDs mapped to scalar values, normally already accepted/adapted by set_books.
        :param db: Database adapter used by the row/link helpers; collaborator errors propagate.
        :param field: Field with required metadata and an optional table.book_col_map cache.
        :param args: Additional arguments, logged but otherwise ignored.
        :return: Set of all supplied owner IDs, including values equal to existing storage.
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
        Precheck linked scalar updates, process deletions, then write non-null values.

        Pass the original update mapping, an empty ID map and acceptance/adapter functions to table.update_precheck. Select custom-column, comments or generic linked-row persistence. Deletions run first and remove simple cache entries when complex_update exists and is false. Non-comment successful updates merge raw supplied values into that simple cache. If the updater returns an ID map, return its book projection directly; that branch does not merge deletion markers into the projection. Without an ID map, deletions produce a projection containing only deleted IDs mapped to None. No batch rollback covers these steps.

        Example:
            A comments replacement returns its new comment-ID/value maps; a deletion-only update ``{7: None}`` returns dirtied={7} and book_col_map={7: None}.


        :param book_id_val_map: Owner IDs mapped to scalar values; None requests link removal.
        :param db: Database adapter used by the row/link helpers; collaborator errors propagate.
        :param field: Legacy field supplying metadata and the required table/cache hooks.
        :param args: Additional compatibility arguments, logged but otherwise ignored.
        :return: An affected-ID set when no ID map or deletions exist; otherwise a dictionary with dirtied, id_map and book_col_map.
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
        Forward a scalar values map to the database column updater.

        Example:
            ``generic_one_one_db_updater(db, {7: "Example"}, "title_title", "titles")`` forwards those keyword arguments to db.update_columns.


        :param db: Database adapter used by the row/link helpers; collaborator errors propagate.
        :param values_map: Owner IDs mapped to persistence-ready scalar values.
        :param field: Resolved physical column name.
        :param table: Resolved destination table name.
        :return: None; described persistence side effects happen through the supplied adapter.
        """
        db.update_columns(values_map=values_map, field=field, table=table)

    @staticmethod
    def series_index_one_one_db_updater(db, values_map, field, table):
        """
        Attempt the legacy series-index helper call for each supplied owner.

        This module currently neither defines nor imports library_set_series_index. A nonempty update therefore raises NameError at the first call unless an external caller has injected that global. Empty mappings return normally; no working series-index persistence is implied by this compatibility hook.

        Example:
            >>> OneToOneWriter.series_index_one_one_db_updater(None, {}, None, None) is None
            True


        :param db: Database adapter used by the row/link helpers; collaborator errors propagate.
        :param values_map: Owner IDs mapped to new series-index values.
        :param field: Compatibility argument not used by this helper.
        :param table: Compatibility argument not used by this helper.
        :return: None; described persistence side effects happen through the supplied adapter.
        :raises NameError: A nonempty mapping reaches the unresolved library_set_series_index global.
        """
        for book_id in values_map:
            series_index_val = values_map[book_id]
            library_set_series_index(db=db, title_id=book_id, idx=series_index_val)

    @staticmethod
    def last_modified_one_one_db_updater(db, values_map, field, table):
        """
        Forward each last-modified value to the legacy books projection adapter.

        Call metadata_sql.update_book_last_modified sequentially. No direct cache update or whole-batch rollback is supplied.

        Example:
            >>> OneToOneWriter.last_modified_one_one_db_updater(None, {}, None, None) is None
            True


        :param db: Database adapter used by the row/link helpers; collaborator errors propagate.
        :param values_map: Book IDs mapped to new last-modified values.
        :param field: Compatibility argument not used by this helper.
        :param table: Compatibility argument not used by this helper.
        :return: None; described persistence side effects happen through the supplied adapter.
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
        Replace each Work comment through Catalog and collect the resulting cache maps.

        Create a Catalog wrapper per Work and call comments.replace_for_wemi with level="work" and data={"text": value}. This helper does not mutate the field cache; successful earlier replacements are not rolled back if a later one fails.

        Example:
            >>> OneToOneWriter.comments_one_one_in_other_updater(None, None, {})
            ({}, {})


        :param db: Database adapter used by the row/link helpers; collaborator errors propagate.
        :param field: Compatibility argument not used by this helper.
        :param updated: Work IDs mapped to comment text.
        :return: Pair (id_map, book_col_map), mapping returned comment IDs to text and Work IDs to comment IDs.
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
        Create a comments row per supplied value and link it with legacy table metadata.

        Despite the generic name, each row is created in "comments" and assigned through the "comment" key, then synced and linked. Existing links are not explicitly cleared here. Errors can leave an already-synced row or earlier link changes behind.

        Example:
            For ``updated={7: "note"}``, create and sync a comments row before calling make_generic_link with owner 7 and the new comment_id.


        :param db: Database adapter used by the row/link helpers; collaborator errors propagate.
        :param field: Field whose table supplies the link name, endpoint columns and priority column.
        :param updated: Owner IDs mapped to values written into new comment rows.
        :return: (None, None); no replacement cache maps are returned.
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
        Break each owner custom-column link, then insert its supplied value ID.

        Use break_cc_lt_link followed by add_cc_link_with_extra for each owner. No value-row creation, explicit extra index, cache refresh or transaction rollback is provided here.

        Example:
            For ``updated={7: 4}``, clear owner 7 links in the configured custom table and add a link to value ID 4.


        :param db: Database adapter used by the row/link helpers; collaborator errors propagate.
        :param field: Field metadata providing the custom link table under "table".
        :param updated: Owner IDs mapped to custom-column value IDs.
        :return: (None, None); no replacement cache maps are returned.
        """
        for book_id, val in iteritems(updated):

            # break the old link - if one exists
            db.macros.break_cc_lt_link(lt=field.metadata["table"], book=book_id)

            # write the new value to the custom column table
            db.macros.add_cc_link_with_extra(lt=field.metadata["table"], book_id=book_id, value_id=val)

        return None, None
