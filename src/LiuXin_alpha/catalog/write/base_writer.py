
"""
Base class for the writers - responsible for writing structured data to the database.

Writers are convenience methods to streamline getting data into the database.
These include functions such as
 - write author_sort_name
 - write covers
 - write identifiers

and so on.

They are NOT responsible for cache updates - those happen in the cache.

A note on teminology.

An ITEM is the thing on the left of the join.
A VALUE is the thing on the right of the join.

You set a VALUE for an ITEM.
If this is not reflected in the naming, it should be.

"""

from __future__ import division, absolute_import, print_function, unicode_literals, annotations

import pprint
from copy import deepcopy

from typing import TYPE_CHECKING, Any, Callable, Mapping, Optional, Iterable, Union

from LiuXin_alpha.utils.libraries.liuxin_six import string_types

from LiuXin_alpha.databases.adaptors import get_adapter_from_name_and_dt
from LiuXin_alpha.errors import DatabaseIntegrityError
from LiuXin_alpha.metadata.ebook_metadata_tools import author_to_author_sort
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems, basestring, \
    dict_itervalues as itervalues
from LiuXin_alpha.utils.logging import default_log

if TYPE_CHECKING:

    from LiuXin_alpha.catalog.api import CatalogAPI
    from LiuXin_alpha.catalog.api.field_metadata_api import FieldMetadataAPI


class BaseCatalogWriter:
    """
    Base class for a catalog writer.

    Exists to write metadata out to the database.
    Database, and metadata aware. Thus, a catalog object.
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
        Specify the table, or the combination of tables, to write to.

        :param catalog: The catalog to write out to

        :param table: String Name of the table.
                      This is the table containing the ITEMS.

                      If we're writing into a single table, then this SHOULD be a table that contains the COLUMN to
                      update.

                      E.g. If we're writing "series" to the "series" table, both the table and the column should be
                      "series"

                      E.g. if we're writing "tags" to the "tags" table, then the table should be "tags" and the column
                      should be "tag".

                      If we're writing into multiple tables, then the "column" probably isn't in the "table".

                      E.g. if we're writing "tags" to "works", then the table is "works" and the column is "tag".

                      E.g. if we're writing "series" to "works", then the table is "works" and the column is "series".

        :param column: Column the VALUE(s) are being written to.

                       Updates are going to be of the form dict[int, Any] - the int is an id in the table we're planning
                       to write to.
                       Any is some form of update instruction for the column values.

        :param adapter: Takes the update values and renders them as strings for writing

        :param name: The name for the table - if there is one.
                     If None, it'll default to "{table}--{column}"

        :param link_table: If provided, the table linking the table and the column-table - can be derived.
                           None if no link

        :param datatype: The datatype of the values we're writing to - used for validation.

        """
        self.catalog = catalog

        # The table the writer is targeting
        self.table = table

        # The table the column is in
        self.column_table = table
        # The column itself to update
        self.column = column

        self.name = name if name is not None else f"{table}--{column}"

        self.link_table = link_table
        self.link_table_bt_id_column = link_table_bt_id_column
        self.link_table_table_id_column = link_table_item_id_column
        self.link_table_priority_col = None

        self.adapter = adapter

        self.accept_vals = accept_vals

        self._sanity_check_connection()

        self.custom = self.is_custom()

        # We can mostly infer this from knowing the dst column
        self.datatype = datatype

    def is_custom(self) -> bool:
        """
        We should be able to work this out.

        :return:
        """
        raise NotImplementedError()

    def _sanity_check_connection(self) -> None:
        """
        Check all the variables we need are set.

        :return:
        """
        raise NotImplementedError(
            "We need to sanity check and set vars - does the link we're trying to write to exist?"
        )

    def update_precheck(self, src_id_dst_val_map, id_map_update: Optional[dict[str, Any]]) -> bool:
        """

        :param src_id_dst_val_map:
        :param id_map_update:
        :return:
        """
        raise NotImplementedError(
            "Has to be swapped out for the right one as required."
        )

    def set_values_func(
            self,
            item_id_val_map: dict[int, str],
            allow_case_change: bool = False) -> set[int]:
        """
        Does the work of writing the final, adapted values out to the database.

        The name is for legacy compatibility reasons.
        :param item_id_val_map:
        :param db:
        :param field:
        :param allow_case_change:
        :return:
        """
        raise NotImplementedError("Needs to be overridden.")

    def no_adapter_set_values(
            self,
            book_id_val_map: dict[int, str],
            allow_case_change: bool = True) -> set[int]:
        """
        Used when the values in question should not be run through an adapter before being written out to the database.

        :param book_id_val_map:
        :param db:
        :param allow_case_change:
        :return:
        """
        if not book_id_val_map:
            return set()

        try:
            dirtied = self.set_values_func(book_id_val_map, self.catalog, allow_case_change)
        except Exception as e:
            err_str = "error while calling self.set_books_func"
            default_log.log_exception(err_str, e, "ERROR", ("self.set_books_func", self.set_values_func))
            raise

        return dirtied

    def set_values(
            self,
            item_id_val_map: dict[int, Any],
            allow_case_change: bool = True) -> set[int]:
        """
        Preform the write for the given metadata into the books in accordance with the book_id_val_mpa.

        :param item_id_val_map:
        :param db:
        :param allow_case_change:
        :return:
        """
        item_id_val_map = {k: self.adapter(v) for k, v in iteritems(item_id_val_map) if self.accept_vals(v)}

        if not item_id_val_map:
            return set()

        try:
            dirtied = self.set_values_func(item_id_val_map, self.catalog, allow_case_change)
        except Exception as e:
            err_str = "error while calling self.set_books_func"
            default_log.log_exception(err_str, e, "ERROR", ("self.set_books_func", self.set_values_func))
            raise

        return dirtied

    # Todo: We need to be able to type table
    def add_and_get_db_id(
        self,
        val: Any,
        is_authors: bool = False,
        id_map_update = None,
    ) -> int:
        """
        Add a value to the db and return its value.

        If the val does not exist in the db it will be created.

        :param val: The value to search for
        :param db: The database to do the search in.
        :param is_authors: Is the value from the authors table?
        :param id_map_update:

        :return None: All changes happen internally to the value passed into the function
        """
        # Process m to extract the table and column the value will be added into - adding flexibility
        # Todo: Account for is_authors - use the author phash search system here
        m_table = self.column_table
        m_col = self.column

        # Todo: This should, tbh, be a separate method
        if is_authors:

            # Todo: Use this in the add.creator method, by default
            aus = author_to_author_sort(val)

            # Todo: Why does this happen? Make sure that it happens everywhere it should. Should add to add.creator
            val_row = self.catalog.add.creator(creator=val.replace(",", "|"), creator_sort=aus).row_dict

            item_id = val_row["creator_id"]

        elif m_table in self.catalog.custom_tables:

            item_id = self.catalog.macros.ensure_custom_column_value(m_table, val)

        else:

            # Deal with the generic case
            val_row = self.catalog.get_blank_row(m_table)
            val_row[m_col] = val
            val_row.sync()
            item_id = val_row.row_id

        return item_id

    # Todo: This should not be here
    # Generic one to one methods in other tables
    def delete_one_to_one_in_other(
            self,
            deleted: Union[tuple[Union[str, int], ...], list[Union[str, int]], tuple[tuple[int, Any], ...]]) -> None:
        """
        Remove one to one entries in a table not of books type.

        :param db:
        :param field:
        :param deleted:
        :return:
        """
        # Todo: Why is this hack necessary? Does it do what you think it does?
        deleted_ids = tuple(de[0] for de in deleted)

        # Delete all references to the book from the link table - foreign keys should take out the value from the
        # one_to_one table as well
        self.catalog.metadata_sql.break_generic_link(self.link_table, self.link_table_bt_id_column, deleted_ids)

    # Todo: This should also not be here
    def custom_delete_one_to_one_in_other(
            self,
            deleted: Union[tuple[Union[str, int], ...], list[Union[str, int]], tuple[tuple[int, Any], ...]]) -> None:
        """
        Remove one to one entries in a custom table attached to books.

        :param db:
        :param field:
        :param deleted:
        :return:
        """
        deleted_ids = tuple(de[0] for de in deleted)

        self.catalog.macros.break_cc_links_by_book_id(lt=self.link_table, book_id=deleted_ids)

    # Todo: Check that dirtied has an update method
    def change_case(self,
                    case_changes: dict[int, Any],
                    dirtied: set[int],
                    is_authors: bool = False) -> set[int]:
        """
        Write case changes into the database.

        :param case_changes: A list of case changes to be applied to the database
        :param dirtied: A set of values which may have been dirtied
        :param is_authors: Should we use the authors metrics?

        :return:
        """
        # Process the field to get the table and the column the update should happen in
        # Todo: Account for the authors-creators change

        # Processing the author strings to ensure safety when written into the database
        if is_authors:
            vals = {item_id: val.replace(",", "|") for item_id, val in iteritems(case_changes)}
        else:
            vals = {item_id: val for item_id, val in iteritems(case_changes)}

        # Update the database with the case change
        self.catalog.update_columns(values_map=vals, field=self.column, table=self.table)

        return set(vals).union(dirtied)

    # Todo: This should not be here
    def do_generic_one_to_many_db_update(
        self,
        db: "CatalogAPI",
        is_custom_series: bool,
        updated: Union[dict[int, Any], dict[int, str]],
        deleted: Union[tuple[str], list[str]],
        clean_before_write: bool = False,
        link_type: Optional[str] = None,
    ):
        """
        Generic handler for applying changes to the db.

        Should be fairly general.
        Give it some values to update, and some values to delete and it'll do that.

        :param db:
        :param table:
        :param field:
        :param is_custom_series:
        :param updated:
        :param deleted:
        :param clean_before_write: If True, then all links to any given book_id in update will be broken before
                                   proceeding to write the new values out to the database.
        :param link_type: If provided, then all the links will be set to this type
        :return:
        """
        # Update the db link table - remove all the links to the book
        if deleted:
            # Todo: This also doesn't seem to work - at all - needs to be fixed
            # db.metadata_sql.break_generic_link(table.link_table, table.link_table_bt_id_column, ((k,) for k in deleted))
            # db.metadata_sql.break_generic_link(table.link_table, table.link_table_bt_id_column, (k for k in deleted))
            for del_id in deleted:
                db.metadata_sql.break_generic_link(self.link_table, self.link_table_bt_id_column, del_id)

        if updated:
            if is_custom_series:

                # Todo: Should trip this mess
                raise NotImplementedError
                # del_stmt = 'DELETE FROM {0} WHERE book=?; '.format(table.link_table)
                # ins_stmt = 'INSERT INTO {0}(book, {1}, extra) VALUES(?, ?, 1.0);'
                # .format(table.link_table, m['link_column'])
            else:
                pass

            # Lock the database to stop anything else from writing to it while doing the update
            with db.lock:
                # Todo: This macro just won't work in this form
                # db.metadata_sql.break_generic_link(table.link_table, table.link_table_bt_id_column,
                #                              (book_id for book_id in iterkeys(updated)))

                for book_id, item_id in iteritems(updated):

                    title_row = db.get_row_from_id("titles", row_id=book_id)

                    if isinstance(item_id, int):

                        # Done here to allow the recursive call for the dict process
                        db.metadata_sql.break_generic_link(
                            link_table=self.link_table,
                            link_col=self.link_table_bt_id_column,
                            remove_id=book_id,
                            link_type=link_type,
                        )
                        # Break any existing links to the item - they need to be repointed
                        db.metadata_sql.break_generic_link(
                            link_table=self.link_table,
                            link_col=self.link_table_table_id_column,
                            remove_id=item_id,
                            link_type=link_type,
                        )

                        item_row = db.get_row_from_id(table.name, row_id=item_id)
                        db.interlink_rows(
                            primary_row=title_row,
                            secondary_row=item_row,
                            type=link_type,
                        )

                        # Todo: Ideally do this in a MACRO
                        # if not priority:
                        #     db.macros.make_generic_link_no_priority(table.link_table, table.link_table_table_id_column,
                        #                                             table.link_table_bt_id_column,
                        #                                             book_id, item_id)
                        # else:
                        #     db.macros.make_generic_link(link_table=table.link_table,
                        #                                 left_link_col=table.link_table_table_id_column,
                        #                                 right_link_col=table.link_table_bt_id_column,
                        #                                 priority_col=table.priority_column,
                        #                                 left_id=book_id, right_id=item_id)

                    elif isinstance(item_id, (set, list, tuple)):

                        # Done here to allow the recursive call for the dict process
                        db.metadata_sql.break_generic_link(
                            link_table=self.link_table,
                            link_col=self.link_table_bt_id_column,
                            remove_id=book_id,
                            link_type=link_type,
                        )

                        item_id = deepcopy([iid for iid in item_id])
                        item_id.reverse()

                        for true_item_id in item_id:
                            # Break any existing links to the item - with any type - they need to be repointed
                            db.metadata_sql.break_generic_link(
                                link_table=self.link_table,
                                link_col=self.link_table_table_id_column,
                                remove_id=true_item_id,
                            )

                            item_row = db.get_row_from_id(self.column_table, row_id=true_item_id)
                            db.interlink_rows(
                                primary_row=title_row,
                                secondary_row=item_row,
                                type=link_type,
                            )

                            # Todo: Think the problem is this doesn't preserve the other properties of links
                            # if not priority:
                            #     # Todo: Think I've confused left and right here
                            #     db.macros.make_generic_link_no_priority(link_table=table.link_table,
                            #                                             left_link_col=table.link_table_table_id_column,
                            #                                             right_link_col=table.link_table_bt_id_column,
                            #                                             left_id=book_id, right_id=true_item_id)
                            # else:
                            #     db.macros.make_generic_link(link_table=table.link_table,
                            #                                 left_link_col=table.link_table_table_id_column,
                            #                                 right_link_col=table.link_table_bt_id_column,
                            #                                 priority_col=table.priority_column,
                            #                                 left_id=book_id, right_id=true_item_id)

                    # We've been passed a type dict - call recursively to handle it
                    elif isinstance(item_id, dict):

                        for local_link_type, link_vals in iteritems(item_id):
                            if link_vals is not None:
                                self.do_generic_one_to_many_db_update(
                                    db,
                                    is_custom_series=is_custom_series,
                                    updated={book_id: link_vals},
                                    deleted=set(),
                                    clean_before_write=clean_before_write,
                                    link_type=local_link_type,
                                )
                            else:
                                db.metadata_sql.break_generic_link(
                                    link_table=self.link_table,
                                    link_col=self.link_table_bt_id_column,
                                    remove_id=book_id,
                                    link_type=local_link_type,
                                )

                    else:
                        err_str = "Attempt to do_generic_one_to_many_db_update encountered an unexpected case"
                        err_str = default_log.log_variables(err_str, "ERROR", ("item_id", item_id))
                        raise NotImplementedError(err_str)

        return None, None

    # Todo: This should, also, not be here
    def do_generic_many_to_many_db_update(
        self,
        is_custom_series: bool,
        updated,
        deleted,
        clean_before_write: bool = False,
        link_type: Optional[str] = None,
    ):
        """
        Generic handler for applying changes to the db.

        Should be fairly general. Even generic.
        :param db:
        :param table:
        :param is_custom_series:
        :param updated:
        :param deleted:
        :param clean_before_write: If True, then all links to any given book_id in update will be broken before
                                   proceeding to write the new values out to the database.
        :param link_type: If provided, then all the links will be set to this type
        :return:
        """
        # Update the db link table - remove all the links to the book
        if deleted:
            # Todo: This also doesn't seem to work - at all
            # db.metadata_sql.break_generic_link(table.link_table, table.link_table_bt_id_column, ((k,) for k in deleted))
            # db.metadata_sql.break_generic_link(table.link_table, table.link_table_bt_id_column, (k for k in deleted))
            for del_id in deleted:
                db.metadata_sql.break_generic_link(self.link_table, self.link_table_bt_id_column, del_id)

        if updated:
            if is_custom_series:
                # Todo: Should trip this mess
                raise NotImplementedError
                # del_stmt = 'DELETE FROM {0} WHERE book=?; '.format(table.link_table)
                # ins_stmt = 'INSERT INTO {0}(book, {1}, extra) VALUES(?, ?, 1.0);'.format(table.link_table, m['link_column'])
            else:
                pass

            # Lock the database to stop anything else from writing to it while doing the update
            with self.catalog.lock:
                # Todo: This macro just won't work in this form
                # db.metadata_sql.break_generic_link(table.link_table, table.link_table_bt_id_column,
                #                              (book_id for book_id in iterkeys(updated)))

                for book_id, item_id in iteritems(updated):

                    title_row = self.catalog.get_row_from_id("titles", row_id=book_id)

                    # Todo: With how the data is currently being used, this should never be triggered
                    if isinstance(item_id, int):

                        item_row = self.catalog.get_row_from_id(self.column_table, row_id=item_id)
                        try:
                            self.catalog.interlink_rows(
                                primary_row=title_row,
                                secondary_row=item_row,
                                type=link_type,
                            )
                        except DatabaseIntegrityError:
                            # The link exists - but it needs to be repointed - and, potentially, retyped
                            self.catalog.macros.reprioritize_link(
                                link_table=self.link_table,
                                left_link_col=self.link_table_bt_id_column,
                                right_link_col=self.link_table_table_id_column,
                                left_id=book_id,
                                right_id=item_id,
                                new_type=link_type,
                            )

                        # Todo: Ideally do this in a MACRO
                        # if not priority:
                        #     db.macros.make_generic_link_no_priority(table.link_table, table.link_table_table_id_column,
                        #                                             table.link_table_bt_id_column,
                        #                                             book_id, item_id)
                        # else:
                        #     db.macros.make_generic_link(link_table=table.link_table,
                        #                                 left_link_col=table.link_table_table_id_column,
                        #                                 right_link_col=table.link_table_bt_id_column,
                        #                                 priority_col=table.priority_column,
                        #                                 left_id=book_id, right_id=item_id)

                    elif isinstance(item_id, (set, list, tuple)):

                        # Need to know the links before and after - the valid links will be repointed
                        existing_item_ids = db.macros.get_linked_ids(
                            link_table=self.link_table,
                            left_id_col=self.link_table_bt_id_column,
                            right_id_col=self.link_table_table_id_column,
                            left_id=book_id,
                            type_filter=link_type,
                        )

                        item_id = deepcopy([iid for iid in item_id])
                        item_id.reverse()

                        for true_item_id in item_id:

                            # If the item is already linked to the book, then repoint it
                            # This preserves any additional data which might be associated with the link
                            if true_item_id in existing_item_ids:
                                self.catalog.macros.reprioritize_link(
                                    link_table=self.link_table,
                                    left_link_col=self.link_table_bt_id_column,
                                    right_link_col=self.link_table_table_id_column,
                                    left_id=book_id,
                                    right_id=true_item_id,
                                    new_type=link_type,
                                )
                                continue

                            # If the item is not linked to the book - then it has to be - retrieve and link
                            item_row = self.catalog.get_row_from_id(self.column_table, row_id=true_item_id)
                            try:
                                self.catalog.interlink_rows(
                                    primary_row=title_row,
                                    secondary_row=item_row,
                                    type=link_type,
                                )
                            except DatabaseIntegrityError:
                                # Item may already be linked to the book - but with a different type - repointing
                                # anyway
                                self.catalog.macros.reprioritize_link(
                                    link_table=self.link_table,
                                    left_link_col=self.link_table_bt_id_column,
                                    right_link_col=self.link_table_table_id_column,
                                    left_id=book_id,
                                    right_id=true_item_id,
                                    new_type=link_type,
                                )

                        # Remove the links which once existed but are no longer needed
                        for excess_item_id in set(existing_item_ids) - set(item_id):

                            self.catalog.metadata_sql.break_generic_single_link(
                                link_table=self.link_table,
                                left_link_col=self.link_table_bt_id_column,
                                right_link_col=self.link_table_table_id_column,
                                left_id=book_id,
                                right_id=excess_item_id,
                            )

                            # if not priority:
                            #     # Todo: Think I've confused left and right here
                            #     db.macros.make_generic_link_no_priority(link_table=table.link_table,
                            #                                             left_link_col=table.link_table_table_id_column,
                            #                                             right_link_col=table.link_table_bt_id_column,
                            #                                             left_id=book_id, right_id=true_item_id)
                            # else:
                            #     db.macros.make_generic_link(link_table=table.link_table,
                            #                                 left_link_col=table.link_table_table_id_column,
                            #                                 right_link_col=table.link_table_bt_id_column,
                            #                                 priority_col=table.priority_column,
                            #                                 left_id=book_id, right_id=true_item_id)

                    # We've been passed a type dict - call recursively to handle it
                    elif isinstance(item_id, dict):

                        for local_link_type, link_vals in iteritems(item_id):
                            if link_vals is not None:
                                self.do_generic_many_to_many_db_update(
                                    is_custom_series=is_custom_series,
                                    updated={book_id: link_vals},
                                    deleted=set(),
                                    clean_before_write=clean_before_write,
                                    link_type=local_link_type,
                                )
                            else:
                                self.catalog.metadata_sql.break_generic_link(
                                    link_table=self.link_table,
                                    link_col=self.link_table_bt_id_column,
                                    remove_id=book_id,
                                    link_type=local_link_type,
                                )

                    else:
                        err_str = "Cannot parse item_id to update"
                        err_str = default_log.log_variables(err_str, "ERROR", ("item_id", item_id))
                        raise NotImplementedError(err_str)

        return None, None

    @staticmethod
    def _unexpected_val_in_item_id_val_map(item_id_val_map, val):
        """
        Err msg.

        :param item_id_val_map:
        :param val:
        :return:
        """
        err_msg = [
            "Unexpected value found in book_id_val_map",
            "book_id_val_map: \n{}\n".format(pprint.pformat(item_id_val_map)),
            "val: {}".format(val),
            "type(val): {}".format(type(val)),
        ]
        return "\n".join(err_msg)
