
"""
Writer for two entities which have a one-to-many relationship.
"""

from __future__ import division, absolute_import, print_function, unicode_literals

from collections import defaultdict

from typing import Callable, Any, Optional, TYPE_CHECKING

from LiuXin_alpha.catalog.write.generic_writers.many_to_one_writer import ManyToOneWriter
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems, six_string_types, basestring, \
    dict_iterkeys as iterkeys
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.text.icu import safe_lower

if TYPE_CHECKING:

    from LiuXin_alpha.catalog.api import CatalogAPI


class OneToManyWriter(ManyToOneWriter):
    """
    Writer for objects with a One-To-Many relationship with each other.
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
    ) -> None:



        super(OneToManyWriter, self).__init__(
            catalog=catalog,
            table=table,
            column=column,
            adapter=adapter,
            accept_vals=accept_vals,
            name=name,
            link_table=link_table,
            link_table_bt_id_column=link_table_bt_id_column,
            link_table_item_id_column=link_table_item_id_column)

        self.m_table = self.field.metadata["table"]
        self.m_column = self.field.metadata["column"]

        # Is the value being linked to unique? Default assumption is no - as in the case of notes - where many different
        # notes may be linked to a single title e.t.c
        try:
            self.val_unique = bool(self.field.metadata["val_unique"])
        except KeyError:
            self.val_unique = False

        if self.val_unique:
            self.set_books_func = (
                self.set_books_for_enum if field.metadata["datatype"] == "enumeration" else self.set_books_func_one_many
            )
        else:
            self.set_books_func = (
                self.set_books_for_enum
                if field.metadata["datatype"] == "enumeration"
                else self.set_books_function_one_many_not_unique
            )

    def set_books_for_enum(self, book_id_val_map, db, field, allow_case_change):
        allowed = set(field.metadata["display"]["enum_values"])
        book_id_val_map = {k: v for k, v in iteritems(book_id_val_map) if v is None or v in allowed}
        if not book_id_val_map:
            return set()
        return self.set_books_func_one_many(book_id_val_map, db, field, False)

    def set_books_function_one_many_not_unique(self, book_id_val_map, db, field, allow_case_change, *args):
        """
        Responsible for returning enough information to preform a cache update.
        :param book_id_val_map:
        :param db:
        :param field:
        :param allow_case_change: Irrelevant here
        :param args:
        :return:
        """
        if args:
            info_str = "set_books_func_one_many had unexpected arguments passed into it"
            default_log.log_variables(info_str, "INFO", ("args", args))

        # Transform the book_id_val_map into a form which can be written out to the database
        new_book_id_val_map = dict()

        # Todo: This information HAS to be availble elsewhere
        link_table = db.driver_wrapper.get_link_table_name(table1="titles", table2=self.m_table)
        link_col = db.driver_wrapper.get_link_column(
            table1="titles",
            table2=self.m_table,
            column_type=db.driver_wrapper.get_id_column("titles"),
        )
        right_link_col = db.driver_wrapper.get_link_column(
            table1="titles",
            table2=self.m_table,
            column_type=db.driver_wrapper.get_id_column(self.m_table),
        )
        left_link_col = db.driver_wrapper.get_link_column(
            table1="titles",
            table2=self.m_table,
            column_type=db.driver_wrapper.get_id_column("titles"),
        )

        book_id_val_map, id_map_update = field.update_preflight(
            book_id_item_id_map=book_id_val_map, id_map_update=dict()
        )

        field.table.update_precheck(book_id_val_map, id_map_update)

        if field.table.priority is False and field.table.typed is False:
            return self._do_not_unique_not_priority_and_not_typed_db_update(
                db, book_id_val_map, link_table, link_col, right_link_col
            )

        elif field.table.priority is True and field.table.typed is False:
            return self._do_not_unique_priority_and_not_typed_db_update(
                db, book_id_val_map, link_table, link_col, right_link_col
            )

        elif field.table.priority is False and field.table.typed is True:
            return self._do_not_unique_not_priority_and_typed_db_update(
                db,
                book_id_val_map,
                link_table,
                link_col,
                right_link_col,
                left_link_col,
                field,
            )

        elif field.table.priority is True and field.table.typed is True:
            return self._do_not_unique_priority_and_typed_db_update(
                db, book_id_val_map, link_table, link_col, right_link_col, field
            )

        else:
            raise NotImplementedError

    def _do_not_unique_not_priority_and_not_typed_db_update(
        self, db, book_id_val_map, link_table, link_col, right_link_col
    ):
        """
        Do db update in the case where the link does not have priority or type information.
        :param db:
        :param book_id_val_map:
        :param link_table:
        :param link_col:
        :param right_link_col:
        :return:
        """
        id_map = dict()

        final_book_id_val_map = defaultdict(set)

        # Todo: Does not deal with ids being passed in as integers
        # Todo: Note this WILL NOT WORK on typed tables - though the modification is easy
        # Nothing fancy is needed - just need to preformm the write out to the table
        # Assume we have a valid update dict - if we've got this far
        for book_id, book_vals in iteritems(book_id_val_map):

            # Todo: Need to generalize this - and rationalize the metadata
            bt_row = db.get_row_from_id("titles", book_id)

            # Todo: Write out the algorithm for what happens when an update dict of a certain form is passed to an update method
            # If we're being passed a string, then add it as the only value
            db.metadata_sql.break_generic_link(link_table=link_table, link_col=link_col, remove_id=book_id)

            # If the link has the concept of priority this should set it correctly - if not it doesn't matter
            book_vals = list(bv for bv in book_vals)
            book_vals.reverse()

            # Add the links back in
            for book_val in book_vals:

                # If we're being passed an iterable of strings, then we just need to add, link and return
                if isinstance(book_val, six_string_types):
                    new_val_row = db.get_blank_row(self.m_table)
                    new_val_row[self.m_column] = book_val
                    new_val_row.sync()

                    db.interlink_rows(primary_row=bt_row, secondary_row=new_val_row)
                    id_map[new_val_row.row_id] = book_val
                    final_book_id_val_map[book_id].add(new_val_row.row_id)

                elif isinstance(book_val, int):
                    # We're being passed an integer - assume this is a note_id - move the note association to the
                    # specified title

                    # Break an existing link to the item
                    db.metadata_sql.break_generic_link(
                        link_table=link_table,
                        link_col=right_link_col,
                        remove_id=book_val,
                    )

                    # Link the note back to the title
                    book_val_row = db.get_row_from_id(self.m_table, book_val)
                    db.interlink_rows(primary_row=bt_row, secondary_row=book_val_row)

                    final_book_id_val_map[book_id].add(book_val)
                else:
                    raise NotImplementedError

            if not book_vals:
                final_book_id_val_map[book_id] = None

        return {
            "dirtied": set(book_id_val_map),
            "book_col_map": final_book_id_val_map,
            "id_map": id_map,
        }

    def _do_not_unique_priority_and_not_typed_db_update(
        self, db, book_id_val_map, link_table, link_col, right_link_col
    ):
        """
        Do db update in the case where the link does not have priority or type information.
        :param db:
        :param book_id_val_map:
        :param link_table:
        :param link_col:
        :param right_link_col:
        :return:
        """
        id_map = dict()

        final_book_id_val_map = defaultdict(list)

        # Todo: Does not deal with ids being passed in as integers
        # Todo: Note this WILL NOT WORK on typed tables - though the modification is easy
        # Nothing fancy is needed - just need to preformm the write out to the table
        # Assume we have a valid update dict - if we've got this far
        for book_id, book_vals in iteritems(book_id_val_map):

            # Todo: Need to generalize this - and rationalize the metadata
            bt_row = db.get_row_from_id("titles", book_id)

            # Todo: Write out the algorithm for what happens when an update dict of a certain form is passed to an update method
            # If we're being passed a string, then add it as the only value
            db.metadata_sql.break_generic_link(link_table=link_table, link_col=link_col, remove_id=book_id)

            # If the link has the concept of priority this should set it correctly - if not it doesn't matter
            book_vals = list(bv for bv in book_vals) if book_vals is not None else []
            book_vals.reverse()

            # Add the links back in
            for book_val in book_vals:

                # If we're being passed an iterable of strings, then we just need to add, link and return
                if isinstance(book_val, six_string_types):
                    new_val_row = db.get_blank_row(self.m_table)
                    new_val_row[self.m_column] = book_val
                    new_val_row.sync()

                    db.interlink_rows(primary_row=bt_row, secondary_row=new_val_row)
                    id_map[new_val_row.row_id] = book_val
                    final_book_id_val_map[book_id] = [
                        new_val_row.row_id,
                    ] + final_book_id_val_map[book_id]

                elif isinstance(book_val, int):
                    # We're being passed an integer - assume this is a note_id - move the note association to the
                    # specified title

                    # Break an existing link to the item
                    db.metadata_sql.break_generic_link(
                        link_table=link_table,
                        link_col=right_link_col,
                        remove_id=book_val,
                    )

                    # Link the note back to the title
                    book_val_row = db.get_row_from_id(self.m_table, book_val)
                    db.interlink_rows(primary_row=bt_row, secondary_row=book_val_row)

                    final_book_id_val_map[book_id] = [
                        book_val,
                    ] + final_book_id_val_map[book_id]

                else:
                    raise NotImplementedError

            if not book_vals:
                final_book_id_val_map[book_id] = None

        return {
            "dirtied": set(book_id_val_map),
            "book_col_map": final_book_id_val_map,
            "id_map": id_map,
        }

    def _do_not_unique_not_priority_and_typed_db_update(
        self,
        db,
        book_id_val_map,
        link_table,
        link_col,
        right_link_col,
        left_link_col,
        field,
    ):
        """
        Do db update in the case where the link does not have priority but does have type information.
        :param db:
        :param book_id_val_map:
        :param link_table:
        :param link_col:

        :param right_link_col:
        :param left_link_col:

        :param field:
        :return:
        """
        id_map = dict()
        new_ids = set()

        final_book_id_val_map = defaultdict(self._default_dict_list_factory)

        # Nothing fancy is needed - just need to preform the write out to the table
        # Assume we have a valid update dict - if we've got this far
        for book_id, type_dict in iteritems(book_id_val_map):

            if type_dict is None:
                final_book_id_val_map[book_id] = None
                continue

            for link_type, book_vals in iteritems(type_dict):

                # Todo: This destroys information which has been added to the link
                # After this, there should be no links of any kind to the book - they all need to be re-added
                db.metadata_sql.break_generic_link(
                    link_table=link_table,
                    link_col=link_col,
                    remove_id=book_id,
                    link_type=link_type,
                )

                # Todo: Write out the algorithm for what happens when an update dict of a certain form is passed to an update method
                # If we're being passed a string, then add it as the only value

                # Check to see if fields actually need to be nullified - and note if they do
                if book_vals is None:
                    final_book_id_val_map[book_id][link_type] = None
                    continue

                # If the link has the concept of priority this should set it correctly - if not it doesn't matter
                book_vals = list(bv for bv in book_vals) if book_vals is not None else []
                book_vals.reverse()

                # Add the links back in
                for book_val in book_vals:

                    # If we're being passed an iterable of strings, then we just need to add, link and return
                    if isinstance(book_val, six_string_types):
                        new_val_row = db.get_blank_row(self.m_table)
                        new_val_row[self.m_column] = book_val
                        new_val_row.sync()

                        id_map[new_val_row.row_id] = book_val
                        final_book_id_val_map[book_id][link_type] = [new_val_row.row_id,] + final_book_id_val_map[
                            book_id
                        ][link_type]
                        new_ids.add(new_val_row.row_id)

                    elif isinstance(book_val, int):
                        # We're being passed an integer - assume this is a note_id - move the note association to the
                        # specified title

                        final_book_id_val_map[book_id][link_type] = [book_val,] + final_book_id_val_map[
                            book_id
                        ][link_type]
                    else:
                        raise NotImplementedError

        try:
            field.table.cache_update_precheck(final_book_id_val_map, id_map)
        except Exception as e:
            for item_id in new_ids:
                db.driver_wrapper.delete_by_id(target_table=self.m_table, row_id=item_id)
            raise

        for book_id, type_dict in iteritems(final_book_id_val_map):

            if type_dict is None:
                db.metadata_sql.break_generic_link(link_table=link_table, link_col=link_col, remove_id=book_id)
                continue

            for link_type, book_vals in iteritems(type_dict):

                # Break any links which exist between the book and the item with that type
                if book_vals is None:
                    db.metadata_sql.break_generic_link(
                        link_table=link_table,
                        link_col=left_link_col,
                        remove_id=book_id,
                        link_type=link_type,
                    )
                    continue

                # Todo: Need to generalize this - and rationalize the metadata
                bt_row = db.get_row_from_id("titles", book_id)

                book_vals = list(book_vals)
                book_vals.reverse()
                for item_id in book_vals:
                    # Break any existing link to the item
                    db.metadata_sql.break_generic_link(
                        link_table=link_table,
                        link_col=right_link_col,
                        remove_id=item_id,
                    )

                    item_row = db.get_row_from_id(self.m_table, item_id)
                    db.interlink_rows(primary_row=bt_row, secondary_row=item_row, type=link_type)

        return {
            "dirtied": set(book_id_val_map),
            "book_col_map": final_book_id_val_map,
            "id_map": id_map,
        }

    def _do_not_unique_priority_and_typed_db_update(
        self, db, book_id_val_map, link_table, link_col, right_link_col, field
    ):
        """
        Do db update in the case where the link does not have priority or type information.
        :param db:
        :param book_id_val_map:
        :param link_table:
        :param link_col:
        :param right_link_col:
        :return:
        """
        id_map = dict()
        new_ids = set()

        final_book_id_val_map = defaultdict(self._default_dict_list_factory)

        # Nothing fancy is needed - just need to preform the write out to the table
        # Assume we have a valid update dict - if we've got this far
        for book_id, type_dict in iteritems(book_id_val_map):

            if type_dict is None:
                final_book_id_val_map[book_id] = None
                continue

            for link_type, book_vals in iteritems(type_dict):

                # Todo: This destroys information which has been added to the link
                # After this, there should be no links of any kind to the book - they all need to be re-added
                db.metadata_sql.break_generic_link(
                    link_table=link_table,
                    link_col=link_col,
                    remove_id=book_id,
                    link_type=link_type,
                )

                # Todo: Write out the algorithm for what happens when an update dict of a certain form is passed to an update method
                # If we're being passed a string, then add it as the only value

                # Check to see if fields actually need to be nullified - and note if they do
                if book_vals is None:
                    final_book_id_val_map[book_id][link_type] = None
                    continue

                # If the link has the concept of priority this should set it correctly - if not it doesn't matter
                book_vals = list(bv for bv in book_vals) if book_vals is not None else []
                book_vals.reverse()

                # Add the links back in
                for book_val in book_vals:

                    # If we're being passed an iterable of strings, then we just need to add, link and return
                    if isinstance(book_val, six_string_types):
                        new_val_row = db.get_blank_row(self.m_table)
                        new_val_row[self.m_column] = book_val
                        new_val_row.sync()

                        id_map[new_val_row.row_id] = book_val
                        final_book_id_val_map[book_id][link_type] = [new_val_row.row_id,] + final_book_id_val_map[
                            book_id
                        ][link_type]
                        new_ids.add(new_val_row.row_id)

                    elif isinstance(book_val, int):
                        # We're being passed an integer - assume this is a note_id - move the note association to the
                        # specified title

                        final_book_id_val_map[book_id][link_type] = [book_val,] + final_book_id_val_map[
                            book_id
                        ][link_type]
                    else:
                        raise NotImplementedError

        try:
            field.table.cache_update_precheck(final_book_id_val_map, id_map)
        except Exception as e:
            for item_id in new_ids:
                db.driver_wrapper.delete_by_id(target_table=self.m_table, row_id=item_id)
            raise

        for book_id, type_dict in iteritems(final_book_id_val_map):

            if type_dict is None:
                db.metadata_sql.break_generic_link(link_table=link_table, link_col=link_col, remove_id=book_id)
                continue

            for link_type, book_vals in iteritems(type_dict):

                if book_vals is None:
                    continue

                # Todo: Need to generalize this - and rationalize the metadata
                bt_row = db.get_row_from_id("titles", book_id)

                book_vals = list(book_vals)
                book_vals.reverse()
                for item_id in book_vals:

                    # Break any existing link to the item
                    db.metadata_sql.break_generic_link(
                        link_table=link_table,
                        link_col=right_link_col,
                        remove_id=item_id,
                    )

                    item_row = db.get_row_from_id(self.m_table, item_id)
                    db.interlink_rows(primary_row=bt_row, secondary_row=item_row, type=link_type)

        return {
            "dirtied": set(book_id_val_map),
            "book_col_map": final_book_id_val_map,
            "id_map": id_map,
        }

    def _default_dict_list_factory(self):
        return defaultdict(list)

    def _default_dict_set_factory(self):
        return defaultdict(set)

    def set_books_func_one_many(self, book_id_val_map, db, field, allow_case_change, *args):
        """
        Responsible for returning enough information to preform a cache update.
        :param book_id_val_map: A map from the book ids to the update values
        :param db: The database to run the update on
        :param field: The field being updated
        :param allow_case_change: If True allows case changes when trying to match the updated value to existing values
                                  on the database.
        :param args:
        :return:
        """
        if args:
            info_str = "set_books_func_one_many had unexpected arguments passed into it"
            default_log.log_variables(info_str, "INFO", ("args", args))

        book_id_val_map, id_map_update = field.table.update_preflight_unique(
            book_id_item_id_map=book_id_val_map, id_map_update=dict()
        )

        # Todo: Check that this is also being done first for all the  other update method
        # Want to do a gross check to make sure the update isn't totally invalid before going any further
        field.table.update_precheck_unique(book_id_val_map, id_map_update)

        available_db_matchers = {None: None}
        db_id_matcher = available_db_matchers.get(self.name, self.get_db_id)

        m = field.metadata
        table = field.table
        dt = m["datatype"]

        # Map values to db ids - including any new values
        # Creating a map which will, in turn, be used to actually find all the ids in the table - by turning the id:item
        # relation around and applying a normalization function to every element in the table
        kmap = safe_lower if dt in {"text", "series"} else lambda x: x
        rid_map = {kmap(item): item_id for item_id, item in iteritems(table.id_map)}

        # table has some entries which differ only in case, fix that
        if len(rid_map) != len(table.id_map):
            table.fix_case_duplicates(db)
            rid_map = {kmap(item): item_id for item_id, item in iteritems(table.id_map)}

        # Clean the val map and make a note of the case changes - then match the given string to an entry on the
        # database
        val_map = {None: None}
        case_changes = {}

        id_map_update = dict()

        self._do_vals_to_ids(
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
        )

        if field.table.priority is False and field.table.typed is False:
            return self._do_unique_not_priority_and_not_typed_db_update(
                db, book_id_val_map, field, val_map, id_map_update
            )

        elif field.table.priority is True and field.table.typed is False:
            return self._do_unique_priority_and_not_typed_db_update(db, book_id_val_map, field, val_map, id_map_update)

        elif field.table.priority is False and field.table.typed is True:
            return self._do_unique_not_priority_and_typed_db_update(db, book_id_val_map, field, val_map, id_map_update)

        elif field.table.priority is True and field.table.typed is True:
            # The only difference from above is the dictionary is finally valued with a list not a set - should all
            # still work
            return self._do_unique_priority_and_typed_db_update(db, book_id_val_map, field, val_map, id_map_update)
        else:
            raise NotImplementedError

    def _do_unique_not_priority_and_not_typed_db_update(self, db, book_id_val_map, field, val_map, id_map_update):

        m = field.metadata
        table = field.table
        dt = m["datatype"]

        # custom series are new fields with a series like structure (in that they have indices - not the full
        # series-tree-index structure) - if the table is a custom column then it's name will start with the custom
        # columns prefix - #
        is_custom_series = dt == "series" and table.name.startswith("#")

        dirtied = set()
        case_changes = {}

        if not self.field.table.custom:
            db_update_links = {None: None}.get(self.name, self.do_generic_one_to_many_db_update)
        else:
            db_update_links = self.do_custom_one_many_db_update

        db_clean_unused_items = {None: None}.get(self.name, self.generic_many_one_clear_unused)

        # Preform case changes - if allowed
        if case_changes:
            self.change_case(case_changes, dirtied, db, table, m)

        # creating a map between the book ids and the item ids
        clean_book_id_item_id_map = defaultdict(set)
        for b_id, item_ids_set in iteritems(book_id_val_map):
            if not item_ids_set:
                clean_book_id_item_id_map[b_id] = set()
                continue
            for item_id in item_ids_set:
                if isinstance(item_id, int):
                    clean_book_id_item_id_map[b_id].add(item_id)
                elif isinstance(item_id, basestring):
                    clean_book_id_item_id_map[b_id].add(val_map[item_id])
                else:
                    raise NotImplementedError
        book_id_item_id_map = clean_book_id_item_id_map

        # Todo: Need to implement this sort of checking for the other update
        # Ignore those items whose value is the same as the current value
        book_id_item_id_map = {k: v for k, v in iteritems(book_id_item_id_map) if v != table.book_col_map.get(k, None)}
        dirtied |= set(book_id_item_id_map)

        # Todo: This should be done in the cache - where the storage details can be taken into account
        # Update the book -> col and col -> book maps

        deleted = set()
        updated = {}
        for book_id, item_ids_set in iteritems(book_id_item_id_map):
            if item_ids_set:
                updated[book_id] = item_ids_set
            else:
                deleted.add(book_id)

        db_update_links(
            db,
            table,
            field,
            is_custom_series,
            updated,
            deleted,
            clean_before_write=True,
        )

        rtn_info = dict()
        rtn_info["dirtied"] = dirtied
        rtn_info["book_col_map"] = book_id_item_id_map
        rtn_info["id_map"] = id_map_update

        # Remove no longer used items
        try:
            clear_unused = m["clear_unused"]
        except KeyError:
            clear_unused = True

        if clear_unused:
            db_clean_unused_items(db, table, field)

        return rtn_info

    def _do_unique_priority_and_not_typed_db_update(self, db, book_id_val_map, field, val_map, id_map_update):

        m = field.metadata
        table = field.table
        dt = m["datatype"]

        # custom series are new fields with a series like structure (in that they have indices - not the full
        # series-tree-index structure) - if the table is a custom column then it's name will start with the custom
        # columns prefix - #
        is_custom_series = dt == "series" and table.name.startswith("#")

        dirtied = set()
        case_changes = {}

        if not self.field.table.custom:
            db_update_links = {None: None}.get(self.name, self.do_generic_one_to_many_db_update)
        else:
            db_update_links = self.do_custom_one_many_db_update

        db_clean_unused_items = {None: None}.get(self.name, self.generic_many_one_clear_unused)

        # Preform case changes - if allowed
        if case_changes:
            self.change_case(case_changes, dirtied, db, table, m)

        # creating a map between the book ids and the item ids
        clean_book_id_item_id_map = defaultdict(list)

        def _to_id(item, val_map):
            if isinstance(item, int):
                return item
            else:
                return val_map[item]

        for b_id, item_ids_list in iteritems(book_id_val_map):
            if not item_ids_list:
                clean_book_id_item_id_map[b_id] = []
                continue
            clean_book_id_item_id_map[b_id] = [_to_id(item, val_map) for item in item_ids_list]

        book_id_item_id_map = clean_book_id_item_id_map

        # Todo: Need to implement this sort of checking for the other update
        # Ignore those items whose value is the same as the current value
        book_id_item_id_map = {k: v for k, v in iteritems(book_id_item_id_map) if v != table.book_col_map.get(k, None)}
        dirtied |= set(book_id_item_id_map)

        # Todo: This should be done in the cache - where the storage details can be taken into account
        # Update the book -> col and col -> book maps
        deleted = set()
        updated = {}
        for book_id, item_ids_set in iteritems(book_id_item_id_map):
            if item_ids_set:
                updated[book_id] = item_ids_set
            else:
                deleted.add(book_id)

        db_update_links(
            db,
            table,
            field,
            is_custom_series,
            updated,
            deleted,
            clean_before_write=True,
        )

        rtn_info = dict()
        rtn_info["dirtied"] = dirtied
        rtn_info["book_col_map"] = book_id_item_id_map
        rtn_info["id_map"] = id_map_update

        # Remove no longer used items
        try:
            clear_unused = m["clear_unused"]
        except KeyError:
            clear_unused = True

        # Todo: Is producing unexpected results - needs a re-write
        # if clear_unused:
        #     db_clean_unused_items(db, table, field)

        return rtn_info

    def _do_unique_not_priority_and_typed_db_update(self, db, book_id_val_map, field, val_map, id_map_update):

        m = field.metadata
        table = field.table
        dt = m["datatype"]

        # custom series are new fields with a series like structure (in that they have indices - not the full
        # series-tree-index structure) - if the table is a custom column then it's name will start with the custom
        # columns prefix - #
        is_custom_series = dt == "series" and table.name.startswith("#")

        dirtied = set()
        case_changes = {}

        if not self.field.table.custom:
            db_update_links = {None: None}.get(self.name, self.do_generic_one_to_many_db_update)
        else:
            db_update_links = self.do_custom_one_many_db_update

        db_clean_unused_items = {None: None}.get(self.name, self.generic_many_one_clear_unused)

        # Preform case changes - if allowed
        if case_changes:
            self.change_case(case_changes, dirtied, db, table, m)

        # creating a map between the book ids and the item ids
        clean_book_id_item_id_map = dict()

        def _to_id(item, val_map):
            if isinstance(item, int):
                return item
            else:
                return val_map[item]

        for b_id, link_dict in iteritems(book_id_val_map):
            if not link_dict:
                clean_book_id_item_id_map[b_id] = None
                continue
            clean_b_link_dict = dict()
            for link_type, link_set in iteritems(link_dict):
                try:
                    clean_b_link_dict[link_type] = set([_to_id(item, val_map) for item in link_set])
                except TypeError:
                    clean_b_link_dict[link_type] = None
            clean_book_id_item_id_map[b_id] = clean_b_link_dict

        book_id_item_id_map = clean_book_id_item_id_map

        # Todo: Need to implement this sort of checking for the other update
        # Ignore those items whose value is the same as the current value
        book_id_item_id_map = {k: v for k, v in iteritems(book_id_item_id_map) if v != field.ids_for_book(k)}
        dirtied |= set(book_id_item_id_map)

        # Todo: This should be done in the cache - where the storage details can be taken into account
        # Update the book -> col and col -> book maps

        deleted = set()
        updated = {}
        for book_id, item_ids_set in iteritems(book_id_item_id_map):
            if item_ids_set:
                updated[book_id] = item_ids_set
            else:
                deleted.add(book_id)

        db_update_links(
            db,
            table,
            field,
            is_custom_series,
            updated,
            deleted,
            clean_before_write=True,
        )

        rtn_info = dict()
        rtn_info["dirtied"] = dirtied
        rtn_info["book_col_map"] = book_id_item_id_map
        rtn_info["id_map"] = id_map_update

        # Remove no longer used items
        try:
            clear_unused = m["clear_unused"]
        except KeyError:
            clear_unused = True

        # Todo: Is producing unexpected results - needs a re-write
        # if clear_unused:
        #     db_clean_unused_items(db, table, field)

        return rtn_info

    def _do_unique_priority_and_typed_db_update(self, db, book_id_val_map, field, val_map, id_map_update):

        m = field.metadata
        table = field.table
        dt = m["datatype"]

        # custom series are new fields with a series like structure (in that they have indices - not the full
        # series-tree-index structure) - if the table is a custom column then it's name will start with the custom
        # columns prefix - #
        is_custom_series = dt == "series" and table.name.startswith("#")

        dirtied = set()
        case_changes = {}

        if not self.field.table.custom:
            db_update_links = {None: None}.get(self.name, self.do_generic_one_to_many_db_update)
        else:
            db_update_links = self.do_custom_one_many_db_update

        db_clean_unused_items = {None: None}.get(self.name, self.generic_many_one_clear_unused)

        # Preform case changes - if allowed
        if case_changes:
            self.change_case(case_changes, dirtied, db, table, m)

        # creating a map between the book ids and the item ids
        clean_book_id_item_id_map = dict()

        def _to_id(item, val_map):
            if isinstance(item, int):
                return item
            else:
                return val_map[item]

        for b_id, link_dict in iteritems(book_id_val_map):
            if not link_dict:
                clean_book_id_item_id_map[b_id] = None
                continue
            clean_b_link_dict = dict()
            for link_type, link_list in iteritems(link_dict):
                try:
                    clean_b_link_dict[link_type] = [_to_id(item, val_map) for item in link_list]
                except TypeError:
                    clean_b_link_dict[link_type] = None
            clean_book_id_item_id_map[b_id] = clean_b_link_dict

        book_id_item_id_map = clean_book_id_item_id_map

        # Todo: Need to implement this sort of checking for the other update
        # Ignore those items whose value is the same as the current value
        book_id_item_id_map = {k: v for k, v in iteritems(book_id_item_id_map) if v != field.ids_for_book(k)}
        dirtied |= set(book_id_item_id_map)

        # Todo: This should be done in the cache - where the storage details can be taken into account
        # Update the book -> col and col -> book maps

        # Todo: Make sure this is consistent with the other methods like this
        field.table.cache_update_precheck(book_id_item_id_map, val_map)
        deleted = set()
        updated = {}
        for book_id, item_ids_set in iteritems(book_id_item_id_map):
            if item_ids_set:
                updated[book_id] = item_ids_set
            else:
                deleted.add(book_id)

        db_update_links(
            db,
            table,
            field,
            is_custom_series,
            updated,
            deleted,
            clean_before_write=True,
        )

        rtn_info = dict()
        rtn_info["dirtied"] = dirtied
        rtn_info["book_col_map"] = book_id_item_id_map
        rtn_info["id_map"] = id_map_update

        # Remove no longer used items
        try:
            clear_unused = m["clear_unused"]
        except KeyError:
            clear_unused = True

        # Todo: Is producing unexpected results - needs a re-write
        # if clear_unused:
        #     db_clean_unused_items(db, table, field)

        return rtn_info

    @staticmethod
    def do_custom_one_many_db_update(
        db,
        table,
        field,
        is_custom_series,
        updated,
        deleted,
        clean_before_write=False,
        priority=False,
    ):
        """
        Update a many to one entry in a custom table.
        :param db:
        :param table:
        :param field:
        :param is_custom_series:
        :param updated:
        :param deleted:
        :return:
        """
        # Update the db link table - remove all the links to the book
        if deleted:
            try:
                cc_table = table.link_table
            except AttributeError:
                cc_table = table.metadata["table"]
            db.macros.break_cc_links_by_book_id(lt=cc_table, book_id=((k,) for k in deleted))

        if updated:

            if is_custom_series:
                m = field.metadata
                try:
                    cc_table = table.link_table
                except AttributeError:
                    cc_table = table.metadata["table"]

                # Lock the database to stop anything else from writing to it while doing the update
                with db.lock:

                    db.macros.break_cc_links_by_book_id(
                        lt=cc_table,
                        book_id=((book_id,) for book_id in iterkeys(updated)),
                    )
                    db.macros.add_cc_link_with_extra_multi(
                        lt=cc_table,
                        sequence=((book_id, item_id, 1.0) for book_id, item_id in iteritems(updated)),
                        extra=True,
                        target_column=m["link_column"],
                    )

            else:
                try:
                    cc_table = table.link_table
                except AttributeError:
                    cc_table = table.metadata["table"]

                # Lock the database to stop anything else from writing to it while doing the update
                with db.lock:

                    db.macros.break_cc_links_by_book_id(
                        lt=cc_table,
                        book_id=((book_id,) for book_id in iterkeys(updated)),
                    )
                    db.macros.add_cc_link_with_extra_multi(
                        lt=cc_table,
                        sequence=(
                            (
                                book_id,
                                item_id,
                            )
                            for book_id, item_id in iteritems(updated)
                        ),
                        extra=False,
                    )

        return None, None
