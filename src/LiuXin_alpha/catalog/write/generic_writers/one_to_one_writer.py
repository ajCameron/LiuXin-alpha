
"""
The one to one writer is intended for writing one to one fields.

There are two types
 - OneToOneSingleTableWriter
 - OneToOneJoinedWriter

 The first is for writing into a single table.
 The second is for writing when the one to one is in a different table.

"""

from __future__ import division, absolute_import, print_function, unicode_literals

from typing import Callable, Any, Optional, TYPE_CHECKING

from LiuXin_alpha.databases.adaptors import sqlite_datetime
from LiuXin_alpha.catalog.write.base_writer import BaseCatalogWriter
from LiuXin_alpha.catalog.catalog_macros import library_set_last_modified, library_set_comment

from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.catalog.catalog_macros import library_set_series_index

if TYPE_CHECKING:

    from LiuXin_alpha.catalog.api import CatalogAPI


class OneToOneCatalogWriterBase(BaseCatalogWriter):
    """
    Base class for the two one-to-one database writers.
    """
    def __init__(
            self,
            catalog: "CatalogAPI",
            table: str,
            column: str,
            adapter: Callable[[Any, ], str] = lambda x: str(x),
            accept_vals: Callable[[Any, ], bool] = lambda x: True,
            name: Optional[str] = None,
            link_table: Optional[str] = None,
            link_table_bt_id_column: Optional[str] = None,
            link_table_item_id_column: Optional[str] = None,
            datatype: Optional[str] = None,
    ) -> None:
        """
        Constructor for the one-to-one database writer.

        :param catalog:
        :param table:
        :param column:
        :param adapter:
        :param accept_vals:
        :param name:
        :param link_table:
        :param link_table_bt_id_column:
        :param link_table_item_id_column:
        """

        super(OneToOneCatalogWriterBase, self).__init__(
            catalog=catalog,
            table=table,
            column=column,
            adapter=adapter,
            accept_vals=accept_vals,
            name=name,
            link_table=link_table,
            link_table_bt_id_column=link_table_bt_id_column,
            link_table_item_id_column=link_table_item_id_column,
            datatype=datatype,)

        self.set_values_func = self.one_one_in_one_table if self.is_same_table() else self.one_one_in_other

        if self.name in {"timestamp", "uuid", "sort"}:
            self.accept_vals = bool

    def is_same_table(self) -> bool:
        """
        Checks to see if the 1-1 relation is in one table or two.

        :return:
        """
        return self.table == self.catalog.get_table_from_column(self.column)

    # def set_values_func(
    #         self,
    #         item_id_val_map: dict[int, str],
    #         allow_case_change: bool = False) -> set[int]:

    def one_one_in_one_table(
            self,
            item_id_val_map: dict[int, Any],
            allow_case_change: bool = False) -> set[int]:
        """
        Set fields for a one-one field in the books/title table.

        Preform an update of the database and cache for a generic database field.
        :param item_id_val_map: Keyed with the id of the item and valued with the value to set for in the database.
        :param allow_case_change:

        :return affects_ids: The book ids which have been changed by this operation
        """

        db_updater = {
            "series_index": self.series_index_one_one_db_updater,
            "last_modified": self.last_modified_one_one_db_updater,
        }.get(self.name, self.generic_one_one_db_updater)

        if item_id_val_map:

            # Writing the changes out the database
            item_id_val_map = {k: self.adapter(v) for k, v in iteritems(item_id_val_map)}

            db_updater(db=self.catalog, values_map=item_id_val_map, allow_case_change=allow_case_change)

        # Return a set of the touched ids
        return set(item_id_val_map)

    def update_precheck(self,
            book_id_item_id_map,
            id_map_update,
            acceptance_functions,
        ) -> None:
        """
        Preform a precheck of the update before writing to the database.

        :param book_id_item_id_map:
        :param id_map_update:
        :param acceptance_functions:
        :return:
        """
        # field.table.update_precheck(
        #     book_id_item_id_map=book_id_val_map,
        #     id_map_update=dict(),
        #     acceptance_functions=[self.accept_vals, self.adapter],
        # )
        raise NotImplementedError()

    # Todo: Comments should really be "one_many" - and need to test that this works properly with
    #       actualy one_one in other
    def one_one_in_other(
            self,
            item_id_val_map: dict[int, Any],
            allow_case_change: bool = False) -> set[int]:
        """
        Set a one-one field in a non-books table.

        If a field is not one-one, then the new value is guaranteed to be the highest priority of the item type linked
        to that book record - but old max value won't be deleted by default.
        This should provide calibre emulation - while retaining data for later use.

        :param item_id_val_map:
        :param allow_case_change:

        :return:
        """
        db_updater = {
        }.get(self.name, self.generic_one_one_db_updater)

        # Todo: Need to go and get this logic and implement it here
        self.update_precheck(
            book_id_item_id_map=item_id_val_map,
            id_map_update=dict(),
            acceptance_functions=[self.accept_vals, self.adapter],
        )

        id_map = None

        # Process the book_id_val_map - if the value is set to None then all the entries in the other table should be
        # deleted
        deleted = tuple((k, None) for k, v in iteritems(item_id_val_map) if v is None)
        if deleted:

            if not self.custom:
                self.delete_one_to_one_in_other(deleted)
            else:
                self.custom_delete_one_to_one_in_other(deleted)

        # Make the text which will be written to the database - the cases where the comment are to be set None have
        # already been acted on
        updated = {k: v for k, v in iteritems(item_id_val_map) if v is not None}
        book_col_map = None
        if updated:

            id_map, book_col_map = db_updater(item_id_val_map, allow_case_change=allow_case_change)

        if id_map is None and not deleted:
            return set(item_id_val_map)

        raise NotImplementedError("Need to do some more work to figure out what actually updated.")

    def generic_one_one_db_updater(self, values_map, allow_case_change=False) -> set[int]:
        """
        Generic update method - applies the book_id_val_map to the database.

        :param values_map: Map to write out for the given 1-1 link.
        :param allow_case_change:

        :return:
        """
        # Todo: Perhaps we can do better on the return?
        self.catalog.update_columns(values_map=values_map, field=self.column, table=self.table)
        return set(values_map)

    def series_index_one_one_db_updater(self, values_map, allow_case_change: bool = False) -> set[int]:
        """
        Do an update on the series_index.

        This should update the series index for the primary index of all entries in the values_map - creating a link to
        the null series if required.

        :param values_map: Keyed with the id of the book and valued with the new series index
        :param allow_case_change:

        :return:
        """
        for book_id in values_map:
            series_index_val = values_map[book_id]
            library_set_series_index(db=self.catalog, title_id=book_id, idx=series_index_val)

        return set(values_map)

    def last_modified_one_one_db_updater(self, values_map, allow_case_change: bool = False) -> set[int]:
        """
        Do an update on the last_modified field in the books table.

        :param values_map: Keyed with the book id and valued with the new last_modified value.
        :param allow_case_change:

        :return:
        """
        for book_id in values_map:
            library_set_last_modified(self.catalog, book_id, values_map[book_id])

        return set(values_map)

    def comments_one_one_in_other_updater(
            self,
            values_map: dict[int, str],
            allow_case_change: bool = False) -> set[int]:
        """
        Updater for the comments table

        :param values_map:
        :param allow_case_change: May actually mean something in this context ...

        :return:
        """
        id_map = dict()
        book_col_map = dict()

        for book_id in values_map:
            comment_val = values_map[book_id]
            book_comment_id = library_set_comment(self.catalog, book_id, comment_val)

            id_map[book_comment_id] = comment_val
            book_col_map[book_id] = book_comment_id

        return set(values_map)

    def generic_one_one_in_other_updater(
            self,
            values_map: dict[int, Any],
            allow_case_change: bool = False) -> set[int]:
        """
        Generic one-one in other table updater.

        :return:
        """
        # Update the database - unlinking the records in the other database from the books - they should be garbage
        # collected by the maintenance bot
        # Todo: What? Probably shouldn't be comments
        for book_id, val in iteritems(values_map):
            comment_row = self.catalog.get_blank_row("comments")
            comment_row["comment"] = val
            comment_row.sync()
            self.catalog.macros.make_generic_link(
                self.link_table,
                self.link_table_bt_id_column,
                self.link_table_table_id_column,
                self.link_table_priority_col,
                book_id,
                comment_row["comment_id"],
            )

        return set(values_map)

    def cc_one_one_updater(self, values_map: dict[int, Any], allow_case_change: bool = False) -> set[int]:
        """
        Updater for the comments table

        :param values_map:
        :param allow_case_change:
        :return:
        """
        for book_id, val in iteritems(values_map):

            # break the old link - if one exists
            self.catalog.macros.break_cc_lt_link(lt=self.link_table, book=book_id)

            # write the new value to the custom column table
            self.catalog.macros.add_cc_link_with_extra(lt=self.link_table, book_id=book_id, value_id=val)

        return set(values_map)
