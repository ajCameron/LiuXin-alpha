
"""
Base class for the writers  -responsible for writing structured data to the database.

Writers are convenience methods to streamline getting data into the database.
These include functions such as
 - write author_sort_name
 - write covers
 - write identifiers

and so on.
"""

from __future__ import division, absolute_import, print_function, unicode_literals, annotations

import pprint
from copy import deepcopy

from typing import TYPE_CHECKING, Any, Callable, Mapping, Optional, Iterable, Union

from LiuXin_alpha.utils.libraries.liuxin_six import string_types

from LiuXin_alpha.databases.adaptors import get_adapter
from LiuXin_alpha.errors import DatabaseIntegrityError
from LiuXin_alpha.metadata.ebook_metadata_tools import author_to_author_sort
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems, basestring, \
    dict_itervalues as itervalues
from LiuXin_alpha.utils.logging import default_log

if TYPE_CHECKING:

    from LiuXin_alpha.catalog.api import CatalogAPI
    from LiuXin_alpha.caches.api.storage_cache_api import FieldBasicInterfaceAPI
    from LiuXin_alpha.caches.api.storage_cache_api import StorageCacheBaseTableAPI
    from LiuXin_alpha.catalog.api.field_metadata_api import FieldMetadataAPI


# Todo: Writer is, explicitely, doing two jobs. Updating the cache and updatiung the db. These should be different.
class BaseWriter:
    """
    Base clas for a calibre-style database writer.

    This assumes, at present, that we're writing into a table linked to books.
    """

    def __init__(self, field: "FieldBasicInterfaceAPI") -> None:
        """
        Writer base which should be used for every writer.

        :param field:
        """
        self.adapter = get_adapter(field.name, field.metadata)
        self.name = field.name
        self.field = field
        self.dt = field.metadata["datatype"]
        self.accept_vals = lambda x: True

    def set_books_func(
            self,
            book_id_val_map: dict[int, bool],
            db: "CatalogAPI",
            field,
            allow_case_change: bool = False) -> set[int]:
        """
        Should be over-ridden by the specified writer.

        :param book_id_val_map:
        :param db:
        :param field:
        :param allow_case_change:
        :return:
        """
        raise NotImplementedError("Needs to be overridden.")

    def no_adapter_set_books(
            self,
            book_id_val_map,
            db: "CatalogAPI",
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
            dirtied = self.set_books_func(book_id_val_map, db, self.field, allow_case_change)
        except Exception as e:
            err_str = "error while calling self.set_books_func"
            default_log.log_exception(err_str, e, "ERROR", ("self.set_books_func", self.set_books_func))
            raise

        return dirtied

    def set_books(
            self,
            book_id_val_map: dict[int, Any],
            db: "CatalogAPI",
            allow_case_change: bool = True) -> set[int]:
        """
        Preform the write for the given metadata into the books in accordance with the book_id_val_mpa.

        :param book_id_val_map:
        :param db:
        :param allow_case_change:
        :return:
        """
        book_id_val_map = {k: self.adapter(v) for k, v in iteritems(book_id_val_map) if self.accept_vals(v)}

        if not book_id_val_map:
            return set()

        try:
            dirtied = self.set_books_func(book_id_val_map, db, self.field, allow_case_change)
        except Exception as e:
            err_str = "error while calling self.set_books_func"
            default_log.log_exception(err_str, e, "ERROR", ("self.set_books_func", self.set_books_func))
            raise

        return dirtied

    # Todo: We need to be able to type table - also - why can't this just be field like everything else? Or as well?
    @staticmethod
    def get_db_id(
        val: Any,
        db: "CatalogAPI",
        m: "FieldMetadataAPI",
        table: "StorageCacheBaseTableAPI",
        kmap: Callable[[str, ], str],
        rid_map: Mapping[str, int],
        allow_case_change: bool,
        case_changes: dict[int, str],
        val_map: dict[Any, int],
        is_authors: bool = False,
        id_map_update = None,
    ):
        """
        Get the db id for the value val - creating if necessary.

        If the val does not exist in the db it is inserted into it.
        :param val: The value to search for
        :param db: The database to do the search in.
        :param m: field.metadata for the field being searched
        :param table:
        :param kmap: Case mapper - usually either icu_lower or the identity function
        :param rid_map: Keyed with values from the database and valued with the id corresponding to that value
                        Used to try and map the given value to values on the database.

        :param allow_case_change:
        :param case_changes: A dictionary recording the required case changes to get a match
        :param val_map: A map keyed with the value and valued with its id
        :param is_authors: Is the value from the authors table?
        :param id_map_update:
        :return None: All changes happen internally to the value passed into the function
        """
        id_map_update = id_map_update if id_map_update is not None else dict()

        # Process m to extract the table and column the value will be added into - adding flexibility
        # Todo: Account for is_authors - use the author phash search system here
        if isinstance(m, string_types):
            m_table = m
            # Todo: This... should be in the DatabaseAPI
            m_col = db.get_display_column(m_table)
        else:
            m_table = m["table"]
            m_col = m["column"]

        # Tries looking the value up in the cache - if it fails starts checking the database
        kval = kmap(val)
        item_id = rid_map.get(kval, None)

        # If the item can't be found in the cache then it needs to be added to the database
        if item_id is None:

            # Todo: This should, tbh, be a seperate method
            if is_authors:

                # Todo: Use this in the add.creator method, by default
                aus = author_to_author_sort(val)

                # Todo: Why does this happen? Make sure that it happens everywhere it should. Should add to add.creator
                val_row = db.add.creator(creator=val.replace(",", "|"), creator_sort=aus).row_dict

                item_id = val_row["creator_id"]
                try:
                    table.seen_item_ids.add(item_id)
                except:
                    pass

                # Writing the values which are unique to authors into the cache
                table.asort_map[item_id] = aus
                table.alink_map[item_id] = ""

            elif m_table in db.custom_tables:

                item_id = db.macros.ensure_custom_column_value(m_table, val)

            else:

                # Deal with the generic case
                val_row = db.get_blank_row(m_table)
                val_row[m_col] = val
                val_row.sync()
                item_id = val_row.row_id
                try:
                    table.seen_item_ids.add(item_id)
                except:
                    pass

            # Store the new values for later write out into the cache
            rid_map[kval] = item_id

        # If the value is already in the cache/ the table check to see if it has the same case as the given value
        # If it doesn't register the cahnge - if it does no further action need be taken
        elif allow_case_change and val != table.id_map[item_id]:
            case_changes[item_id] = val

        # Finally writing the full analyzed value, id pair into the cache update
        id_map_update[item_id] = val
        val_map[val] = item_id

        return id_map_update

    # Generic one to one methods in other tables
    @staticmethod
    def delete_one_to_one_in_other(
            db: "CatalogAPI",
            field: "FieldBasicInterfaceAPI",
            deleted: Union[tuple[str], list[str]]) -> None:
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
        db.metadata_sql.break_generic_link(field.table.link_table, field.table.link_table_bt_id_column, deleted_ids)

    @staticmethod
    def custom_delete_one_to_one_in_other(
            db: "CatalogAPI",
            field: "FieldBasicInterfaceAPI",
            deleted: Union[tuple[str], list[str]]) -> None:
        """
        Remove one to one entries in a custom table attached to books.

        :param db:
        :param field:
        :param deleted:
        :return:
        """
        deleted_ids = tuple(de[0] for de in deleted)

        db.macros.break_cc_links_by_book_id(lt=field.metadata["table"], book_id=deleted_ids)

    # Todo: Check that dirtied has an update method
    @staticmethod
    def change_case(case_changes, dirtied, db, table, m, is_authors=False):
        """
        Write case changes into the database.

        :param case_changes: A list of case changes to be applied to the database
        :param dirtied: An object containing the dirtied books
        :param db: A database to write the changes to
        :param table: A Table object to cache the changes
        :param m:
        :param is_authors: Should
        :return:
        """
        # Process the field to get the table and the column the update should happen in
        # Todo: Account for the authors-creators change
        if isinstance(m, string_types):
            m_table = m
            m_col = db.direct_get_display_column(m)
        else:
            m_table = m["table"]
            m_col = m["column"]

        # Processing the author strings to ensure safety when written into the database
        if is_authors:
            vals = {item_id: val.replace(",", "|") for item_id, val in iteritems(case_changes)}
        else:
            vals = {item_id: val for item_id, val in iteritems(case_changes)}

        # Update the database with the case change
        db.update_columns(values_map=vals, field=m_col, table=m_table)

        # Write the case changes into the cache and dirty the appropriate books
        for item_id, val in iteritems(case_changes):
            table.id_map[item_id] = val
            dirtied.update(table.col_book_map[item_id])
            if is_authors:
                table.asort_map[item_id] = author_to_author_sort(val)

    def do_generic_one_to_many_db_update(
        self,
        db: "CatalogAPI",
        table: "StorageCacheBaseTableAPI",
        field: "FieldBasicInterfaceAPI",
        is_custom_series: bool,
        updated,
        deleted: Union[tuple[str], list[str]],
        clean_before_write: bool = False,
        link_type: Optional[str] = None,
    ):
        """
        Generic handler for applying changes to the db.

        Should be fairly general.
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
                db.metadata_sql.break_generic_link(table.link_table, table.link_table_bt_id_column, del_id)

        if updated:
            if is_custom_series:
                m = field.metadata
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
                            link_table=table.link_table,
                            link_col=table.link_table_bt_id_column,
                            remove_id=book_id,
                            link_type=link_type,
                        )
                        # Break any existing links to the item - they need to be repointed
                        db.metadata_sql.break_generic_link(
                            link_table=table.link_table,
                            link_col=table.link_table_table_id_column,
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
                            link_table=table.link_table,
                            link_col=table.link_table_bt_id_column,
                            remove_id=book_id,
                            link_type=link_type,
                        )

                        item_id = deepcopy([iid for iid in item_id])
                        item_id.reverse()

                        for true_item_id in item_id:
                            # Break any existing links to the item - with any type- they need to be repointed
                            db.metadata_sql.break_generic_link(
                                link_table=table.link_table,
                                link_col=table.link_table_table_id_column,
                                remove_id=true_item_id,
                            )

                            item_row = db.get_row_from_id(table.name, row_id=true_item_id)
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
                                    table=table,
                                    field=field,
                                    is_custom_series=is_custom_series,
                                    updated={book_id: link_vals},
                                    deleted=set(),
                                    clean_before_write=clean_before_write,
                                    link_type=local_link_type,
                                )
                            else:
                                db.metadata_sql.break_generic_link(
                                    link_table=table.link_table,
                                    link_col=table.link_table_bt_id_column,
                                    remove_id=book_id,
                                    link_type=local_link_type,
                                )

                    else:
                        err_str = "Attempt to do_generic_one_to_many_db_update encountered an unexpected case"
                        err_str = default_log.log_variables(err_str, "ERROR", ("item_id", item_id))
                        raise NotImplementedError(err_str)

        return None, None

    def do_generic_many_to_many_db_update(
        self,
        db: "CatalogAPI",
        table,
        field,
        is_custom_series,
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
                db.metadata_sql.break_generic_link(table.link_table, table.link_table_bt_id_column, del_id)

        if updated:
            if is_custom_series:
                m = field.metadata
                # Todo: Should trip this mess
                raise NotImplementedError
                # del_stmt = 'DELETE FROM {0} WHERE book=?; '.format(table.link_table)
                # ins_stmt = 'INSERT INTO {0}(book, {1}, extra) VALUES(?, ?, 1.0);'.format(table.link_table, m['link_column'])
            else:
                pass

            # Lock the database to stop anything else from writing to it while doing the update
            with db.lock:
                # Todo: This macro just won't work in this form
                # db.metadata_sql.break_generic_link(table.link_table, table.link_table_bt_id_column,
                #                              (book_id for book_id in iterkeys(updated)))

                for book_id, item_id in iteritems(updated):

                    title_row = db.get_row_from_id("titles", row_id=book_id)

                    # Todo: With how the data is currently being used, this should never be triggered
                    if isinstance(item_id, int):

                        item_row = db.get_row_from_id(table.name, row_id=item_id)
                        try:
                            db.interlink_rows(
                                primary_row=title_row,
                                secondary_row=item_row,
                                type=link_type,
                            )
                        except DatabaseIntegrityError:
                            # The link exists - but it needs to be repointed - and, potentially, retyped
                            db.macros.reprioritize_link(
                                link_table=table.link_table,
                                left_link_col=table.link_table_bt_id_column,
                                right_link_col=table.link_table_table_id_column,
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
                            link_table=table.link_table,
                            left_id_col=table.link_table_bt_id_column,
                            right_id_col=table.link_table_table_id_column,
                            left_id=book_id,
                            type_filter=link_type,
                        )

                        item_id = deepcopy([iid for iid in item_id])
                        item_id.reverse()

                        for true_item_id in item_id:

                            # If the item is already linked to the book, then repoint it
                            # This preserves any additional data which might be associated with the link
                            if true_item_id in existing_item_ids:
                                db.macros.reprioritize_link(
                                    link_table=table.link_table,
                                    left_link_col=table.link_table_bt_id_column,
                                    right_link_col=table.link_table_table_id_column,
                                    left_id=book_id,
                                    right_id=true_item_id,
                                    new_type=link_type,
                                )
                                continue

                            # If the item is not linked to the book - then it has to be - retrieve and link
                            item_row = db.get_row_from_id(table.name, row_id=true_item_id)
                            try:
                                db.interlink_rows(
                                    primary_row=title_row,
                                    secondary_row=item_row,
                                    type=link_type,
                                )
                            except DatabaseIntegrityError:
                                # Item may already be linked to the book - but with a different type - repointing
                                # anyway
                                db.macros.reprioritize_link(
                                    link_table=table.link_table,
                                    left_link_col=table.link_table_bt_id_column,
                                    right_link_col=table.link_table_table_id_column,
                                    left_id=book_id,
                                    right_id=true_item_id,
                                    new_type=link_type,
                                )

                        # Remove the links which once existed but are no longer needed
                        for excess_item_id in set(existing_item_ids) - set(item_id):

                            db.metadata_sql.break_generic_single_link(
                                link_table=table.link_table,
                                left_link_col=table.link_table_bt_id_column,
                                right_link_col=table.link_table_table_id_column,
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
                                    db,
                                    table=table,
                                    field=field,
                                    is_custom_series=is_custom_series,
                                    updated={book_id: link_vals},
                                    deleted=set(),
                                    clean_before_write=clean_before_write,
                                    link_type=local_link_type,
                                )
                            else:
                                db.metadata_sql.break_generic_link(
                                    link_table=table.link_table,
                                    link_col=table.link_table_bt_id_column,
                                    remove_id=book_id,
                                    link_type=local_link_type,
                                )

                    else:
                        err_str = "Cannot parse item_id to update"
                        err_str = default_log.log_variables(err_str, "ERROR", ("item_id", item_id))
                        raise NotImplementedError(err_str)

        return None, None

    def _do_vals_to_ids(
        self,
        book_id_val_map,
        db_id_matcher,
        db,
        m,
        table,
        kmap,
        rid_map,
        allow_case_change,
        case_changes,
        val_map,
        id_map_update,
    ):
        """
        Attempt to map values to ids.

        :param book_id_val_map:
        :param db_id_matcher:
        :param db:
        :param m:
        :param table:
        :param kmap:
        :param rid_map:
        :param allow_case_change:
        :param case_changes:
        :param val_map:
        :param id_map_update:
        :return:
        """
        def _process_list_set_str_val(val) -> None:
            """

            :param val:
            :return:
            """

            # We have a list or set of values
            if isinstance(val, (set, list)):
                # To keep compatibility with other methods
                if isinstance(val, list):
                    true_vals = deepcopy(val)
                    true_vals.reverse()
                else:
                    true_vals = val

                for true_val in true_vals:
                    if isinstance(true_val, int):
                        pass
                    else:
                        db_id_matcher(
                            true_val,
                            db,
                            m,
                            table,
                            kmap,
                            rid_map,
                            allow_case_change,
                            case_changes,
                            val_map,
                            id_map_update=id_map_update,
                        )

            elif isinstance(val, basestring):

                db_id_matcher(
                    val,
                    db,
                    m,
                    table,
                    kmap,
                    rid_map,
                    allow_case_change,
                    case_changes,
                    val_map,
                    id_map_update=id_map_update,
                )

            elif isinstance(val, int):
                pass

            else:
                raise NotImplementedError

        for val in itervalues(book_id_val_map):
            if val is not None:
                if isinstance(val, (basestring, set, list)):
                    _process_list_set_str_val(val)

                # Presumably match has occurred already. Or something has gone terribly wrong.
                elif isinstance(val, int):
                    pass

                elif isinstance(val, dict):
                    for nested_vals in itervalues(val):
                        if nested_vals:
                            _process_list_set_str_val(nested_vals)
                else:
                    raise NotImplementedError(self._unexpected_val_in_book_id_val_map(book_id_val_map, val))

    @staticmethod
    def _unexpected_val_in_book_id_val_map(book_id_val_map, val):
        """
        Err msg.

        :param book_id_val_map:
        :param val:
        :return:
        """
        err_msg = [
            "Unexpected value found in book_id_val_map",
            "book_id_val_map: \n{}\n".format(pprint.pformat(book_id_val_map)),
            "val: {}".format(val),
            "type(val): {}".format(type(val)),
        ]
        return "\n".join(err_msg)
