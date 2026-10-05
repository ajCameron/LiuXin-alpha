"""
Write legacy one-to-many fields across unique/non-unique, typed and priority shapes.
"""

from __future__ import division, absolute_import, print_function, unicode_literals

from collections import defaultdict

from LiuXin_alpha.caches.write.generic_writers.many_to_one_writer import ManyToOneWriter
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems, six_string_types, basestring, \
    dict_iterkeys as iterkeys
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.text.icu import safe_lower


class OneToManyWriter(ManyToOneWriter):
    """
    Select legacy one-to-many persistence by value uniqueness and link capabilities.

    Inherit unadapted public updates from ManyToOneWriter. Enumeration metadata uses a filtering hook; other fields choose unique resolution or fresh-row creation. Most hooks return cache-update dictionaries, while an empty filtered enumeration returns a set.

    Example:
        For non-unique notes metadata, ``OneToManyWriter(field).set_books({7: ["note"]}, db)`` selects the appropriate typed/priority row writer.
    """

    def __init__(self, field):
        """
        Store destination metadata and bind the unique, non-unique or enumeration hook.

        metadata.val_unique is converted with bool and defaults to False when absent. Both uniqueness branches route datatype="enumeration" to set_books_for_enum.

        Example:
            With val_unique=True and datatype="text", the writer binds set_books_func_one_many; without val_unique it binds the non-unique dispatcher.


        :param field: Legacy field supplying metadata and the required table/cache hooks.
        :return: None; initializes inherited state plus m_table, m_column, val_unique and set_books_func.
        """

        super(OneToManyWriter, self).__init__(field)

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
        """
        Drop disallowed enumeration values and delegate accepted updates with case changes disabled.

        Allowed values are tested exactly, without normalization. None is always retained. Unhashable values or unhashable configured choices raise TypeError before delegation.

        Example:
            >>> from types import SimpleNamespace
            >>> field = SimpleNamespace(metadata={"display": {"enum_values": ["A"]}})
            >>> OneToManyWriter.set_books_for_enum(None, {7: "B"}, None, field, True)
            set()


        :param book_id_val_map: Owner IDs mapped to hashable enumeration values or None.
        :param db: Database adapter used by the row/link helpers; collaborator errors propagate.
        :param field: Field metadata supplying display.enum_values.
        :param allow_case_change: Ignored requested case-change flag; delegation always uses False.
        :return: An empty set if no entries survive, otherwise the unique writer result.
        :raises TypeError: An enumeration choice or tested update value is unhashable.
        """

        allowed = set(field.metadata["display"]["enum_values"])
        book_id_val_map = {k: v for k, v in iteritems(book_id_val_map) if v is None or v in allowed}
        if not book_id_val_map:
            return set()
        return self.set_books_func_one_many(book_id_val_map, db, field, False)

    def set_books_function_one_many_not_unique(self, book_id_val_map, db, field, allow_case_change, *args):
        """
        Preflight a non-unique update and dispatch by literal typed/priority flags.

        Derive link-table and endpoint columns through driver_wrapper using "titles" and m_table, then run field.update_preflight and table.update_precheck. All four boolean capability combinations have dedicated writers; values other than literal True/False are rejected. The downstream helper persists rows/links and returns maps rather than directly refreshing the field cache.

        Example:
            A table with priority=True and typed=False routes a normalized list of notes to _do_not_unique_priority_and_not_typed_db_update.


        :param book_id_val_map: Owner-to-value update mapping normalized by field.update_preflight.
        :param db: Database adapter used by the row/link helpers; collaborator errors propagate.
        :param field: Legacy field supplying metadata and the required table/cache hooks.
        :param allow_case_change: Compatibility argument not used by this helper.
        :param args: Additional arguments, logged and otherwise ignored.
        :return: Dictionary with dirtied owner IDs, book_col_map projections and id_map values for cache consumers.
        :raises NotImplementedError: A table capability flag is not a literal boolean.
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
        Replace untyped owner links by creating string rows or moving existing IDs.

        Read each owner from "titles" and clear its links before processing a reversed copy of the supplied iterable. Strings create and sync new m_table rows; integers detach existing target links before relinking. Accumulate result IDs in sets; an empty iterable produces a None projection. None is not accepted and raises TypeError after owner links have already been cleared. id_map contains only newly created string rows. No transaction or cache refresh is supplied; prior removals/creations survive a later error.

        Example:
            With configured note metadata, an update ``{7: ["new note", 4]}`` creates one row and moves existing note 4 to owner 7.


        :param db: Database adapter used by the row/link helpers; collaborator errors propagate.
        :param book_id_val_map: Owner IDs mapped to iterables of strings/integers; use an empty iterable to clear links.
        :param link_table: Physical link-table name.
        :param link_col: Owner-ID column used to clear links.
        :param right_link_col: Target-ID column used to detach an existing item from any owner.
        :return: Dictionary with dirtied owner IDs, book_col_map projections and id_map values for cache consumers.
        :raises NotImplementedError: An iterable element is neither a string nor an integer.
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
        Replace untyped owner links by creating string rows or moving existing IDs.

        Read each owner from "titles" and clear its links before processing a reversed copy of the supplied iterable. Strings create and sync new m_table rows; integers detach existing target links before relinking. Prepend each result ID so the returned list retains input order; None and empty iterables produce a None projection. id_map contains only newly created string rows. No transaction or cache refresh is supplied; prior removals/creations survive a later error.

        Example:
            With configured note metadata, an update ``{7: ["new note", 4]}`` creates one row and moves existing note 4 to owner 7.


        :param db: Database adapter used by the row/link helpers; collaborator errors propagate.
        :param book_id_val_map: Owner IDs mapped to iterables of strings/integers; None clears links.
        :param link_table: Physical link-table name.
        :param link_col: Owner-ID column used to clear links.
        :param right_link_col: Target-ID column used to detach an existing item from any owner.
        :return: Dictionary with dirtied owner IDs, book_col_map projections and id_map values for cache consumers.
        :raises NotImplementedError: An iterable element is neither a string nor an integer.
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
        Prepare typed row replacements, validate their cache maps, then relink targets.

        For each type, clear its existing owner links before preparing new rows. Reverse each iterable, create/sync string rows and retain integer IDs; result lists retain input order even when priority is disabled. A typed None records a null projection, and an empty typed iterable may leave that type absent from the result. On cache_update_precheck failure, attempt to delete newly created rows, then re-raise; earlier link removals are not restored and cleanup can itself fail. After validation, owner-level None clears all links; other result IDs are detached from every owner and linked with the requested type. Typed None also issues a second type-filtered deletion through left_link_col. Other failures can leave partial row/link changes. No direct cache refresh occurs.

        Example:
            For ``{7: {"note": ["new note", 4]}}``, prepare IDs and validate the resulting typed projection before linking each target to owner 7.


        :param db: Database adapter used by the row/link helpers; collaborator errors propagate.
        :param book_id_val_map: Owner IDs mapped to typed iterables, or None to clear every owner link.
        :param link_table: Physical link-table name.
        :param link_col: Owner-ID column used to clear links.
        :param right_link_col: Target-ID column used to detach existing items.
        :param left_link_col: Owner-ID column used for the second typed-None deletion pass.
        :param field: Field whose table.cache_update_precheck validates the prepared ID maps.
        :return: Dictionary with dirtied owner IDs, book_col_map projections and id_map values for cache consumers.
        :raises NotImplementedError: A typed iterable element is neither string nor integer.
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
        Prepare typed row replacements, validate their cache maps, then relink targets.

        For each type, clear its existing owner links before preparing new rows. Reverse each iterable, create/sync string rows and retain integer IDs; result lists retain input order even when priority is disabled. A typed None records a null projection, and an empty typed iterable may leave that type absent from the result. On cache_update_precheck failure, attempt to delete newly created rows, then re-raise; earlier link removals are not restored and cleanup can itself fail. After validation, owner-level None clears all links; other result IDs are detached from every owner and linked with the requested type. Typed None needs no second deletion because the first pass already cleared it. Other failures can leave partial row/link changes. No direct cache refresh occurs.

        Example:
            For ``{7: {"note": ["new note", 4]}}``, prepare IDs and validate the resulting typed projection before linking each target to owner 7.


        :param db: Database adapter used by the row/link helpers; collaborator errors propagate.
        :param book_id_val_map: Owner IDs mapped to typed iterables, or None to clear every owner link.
        :param link_table: Physical link-table name.
        :param link_col: Owner-ID column used to clear links.
        :param right_link_col: Target-ID column used to detach existing items.
        :param field: Field whose table.cache_update_precheck validates the prepared ID maps.
        :return: Dictionary with dirtied owner IDs, book_col_map projections and id_map values for cache consumers.
        :raises NotImplementedError: A typed iterable element is neither string nor integer.
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
        """
        Create a fresh dictionary whose missing type keys receive independent lists.

        Example:
            >>> groups = OneToManyWriter._default_dict_list_factory(None)
            >>> groups["a"].append(4)
            >>> groups["b"]
            []


        :return: A new defaultdict(list).
        """

        return defaultdict(list)

    def _default_dict_set_factory(self):
        """
        Create a fresh dictionary whose missing type keys receive independent sets.

        Example:
            >>> groups = OneToManyWriter._default_dict_set_factory(None)
            >>> groups["a"].add(4)
            >>> groups["b"]
            set()


        :return: A new defaultdict(set).
        """

        return defaultdict(set)

    def set_books_func_one_many(self, book_id_val_map, db, field, allow_case_change, *args):
        """
        Resolve unique values after table preflight and dispatch by link capabilities.

        Run update_preflight_unique and update_precheck_unique, build a case-normalized reverse value lookup, repair duplicates and resolve/create values with the shared helper. The preflight ID map is replaced with a fresh map before resolution. Collected case_changes are not passed to the downstream helpers, which each initialize their own empty case-change map. Choose among four literal typed/priority combinations. Lookup and repair may persist changes before later validation or link writes fail.

        Example:
            For a unique untyped unordered field, ``{7: {"Shelf A"}}`` resolves the value before the set-based link updater runs.


        :param book_id_val_map: Owner updates accepted by the table unique preflight.
        :param db: Database adapter used by the row/link helpers; collaborator errors propagate.
        :param field: Legacy field supplying metadata and the required table/cache hooks.
        :param allow_case_change: Case-change permission forwarded to value resolution.
        :param args: Additional arguments, logged and otherwise ignored.
        :return: Dictionary with dirtied owner IDs, book_col_map projections and id_map values for cache consumers.
        :raises NotImplementedError: The capability flags or a value-resolution shape are unsupported.
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

        """
        Convert unique sets to changed ID projections and persist their links.

        Build sets by retaining integer IDs and resolving other values through val_map. False payloads become empty sets; only integer and string elements are accepted. Compare against table.book_col_map.get(owner, None) to discard unchanged owners. Split changed projections by truthiness into updated and deleted, then dispatch generic or custom link persistence with clean_before_write=True. No internal_update_cache call occurs here. Unused-item cleanup defaults to enabled via metadata.clear_unused and runs after persistence. The locally created case-change map is empty; this helper does not apply case changes collected by the caller. Custom persistence forwards each collection as one macro item rather than flattening it.

        Example:
            With a compatible table, ``{7: ["Shelf", 4]}`` resolves "Shelf" through val_map, discards unchanged owner projections and dispatches the changed links.


        :param db: Database adapter used by the row/link helpers; collaborator errors propagate.
        :param book_id_val_map: Normalized owner updates in the corresponding typed/priority shape.
        :param field: Legacy field supplying metadata and the required table/cache hooks.
        :param val_map: Resolved value-to-ID lookup.
        :param id_map_update: ID-to-value map returned unchanged in the result.
        :return: Dictionary with dirtied owner IDs, book_col_map projections and id_map values for cache consumers.
        :raises KeyError: A non-integer value has no entry in val_map.
        """

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

        """
        Convert unique lists to changed ID projections and persist their links.

        Build lists by retaining integer IDs and resolving other values through val_map. False payloads become empty lists; non-integer elements are looked up without a separate type check. Compare against table.book_col_map.get(owner, None) to discard unchanged owners. Split changed projections by truthiness into updated and deleted, then dispatch generic or custom link persistence with clean_before_write=True. No internal_update_cache call occurs here. Unused-item cleanup is currently inactive even when metadata.clear_unused is true. The locally created case-change map is empty; this helper does not apply case changes collected by the caller. Custom persistence forwards each collection as one macro item rather than flattening it.

        Example:
            With a compatible table, ``{7: ["Shelf", 4]}`` resolves "Shelf" through val_map, discards unchanged owner projections and dispatches the changed links.


        :param db: Database adapter used by the row/link helpers; collaborator errors propagate.
        :param book_id_val_map: Normalized owner updates in the corresponding typed/priority shape.
        :param field: Legacy field supplying metadata and the required table/cache hooks.
        :param val_map: Resolved value-to-ID lookup.
        :param id_map_update: ID-to-value map returned unchanged in the result.
        :return: Dictionary with dirtied owner IDs, book_col_map projections and id_map values for cache consumers.
        :raises KeyError: A non-integer value has no entry in val_map.
        """

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
            """
            Preserve an integer target ID or look up the resolved value.

            Example:
                Inside the enclosing conversion, integer 4 passes through and "Shelf" maps to 4 when val_map contains that entry.


            :param item: One target value from the surrounding update container.
            :param val_map: Mapping from non-integer values to target IDs.
            :return: The original integer, including bool, or val_map[item].
            :raises KeyError: A non-integer value is unresolved.
            :raises TypeError: A non-integer lookup key is unhashable.
            """

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

        """
        Convert unique typed dictionaries of sets to changed ID projections and persist their links.

        Build typed dictionaries of sets by retaining integer IDs and resolving other values through val_map. False owner payloads become None; any TypeError while converting a type value makes that type None, including iteration or lookup/hash errors. Compare against field.ids_for_book to discard unchanged owners. Split changed projections by truthiness into updated and deleted, then dispatch generic or custom link persistence with clean_before_write=True. No internal_update_cache call occurs here. Unused-item cleanup is currently inactive even when metadata.clear_unused is true. The locally created case-change map is empty; this helper does not apply case changes collected by the caller. Custom persistence forwards each collection as one macro item rather than flattening it.

        Example:
            With a compatible table, ``{7: {"note": ["Shelf", 4]}}`` resolves "Shelf" through val_map, discards unchanged owner projections and dispatches the changed links.


        :param db: Database adapter used by the row/link helpers; collaborator errors propagate.
        :param book_id_val_map: Normalized owner updates in the corresponding typed/priority shape.
        :param field: Legacy field supplying metadata and the required table/cache hooks.
        :param val_map: Resolved value-to-ID lookup.
        :param id_map_update: ID-to-value map returned unchanged in the result.
        :return: Dictionary with dirtied owner IDs, book_col_map projections and id_map values for cache consumers.
        :raises KeyError: A non-integer value has no entry in val_map.
        """

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
            """
            Preserve an integer target ID or look up the resolved value.

            Example:
                Inside the enclosing conversion, integer 4 passes through and "Shelf" maps to 4 when val_map contains that entry.


            :param item: One target value from the surrounding update container.
            :param val_map: Mapping from non-integer values to target IDs.
            :return: The original integer, including bool, or val_map[item].
            :raises KeyError: A non-integer value is unresolved.
            :raises TypeError: A non-integer lookup key is unhashable.
            """

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

        """
        Convert unique typed dictionaries of lists to changed ID projections and persist their links.

        Build typed dictionaries of lists by retaining integer IDs and resolving other values through val_map. False owner payloads become None; any TypeError while converting a type value makes that type None, including iteration or lookup/hash errors. Compare against field.ids_for_book to discard unchanged owners. Before persistence call table.cache_update_precheck with the converted updates and val_map. Split changed projections by truthiness into updated and deleted, then dispatch generic or custom link persistence with clean_before_write=True. No internal_update_cache call occurs here. Unused-item cleanup is currently inactive even when metadata.clear_unused is true. The locally created case-change map is empty; this helper does not apply case changes collected by the caller. Custom persistence forwards each collection as one macro item rather than flattening it.

        Example:
            With a compatible table, ``{7: {"note": ["Shelf", 4]}}`` resolves "Shelf" through val_map, discards unchanged owner projections and dispatches the changed links.


        :param db: Database adapter used by the row/link helpers; collaborator errors propagate.
        :param book_id_val_map: Normalized owner updates in the corresponding typed/priority shape.
        :param field: Legacy field supplying metadata and the required table/cache hooks.
        :param val_map: Resolved value-to-ID lookup.
        :param id_map_update: ID-to-value map returned unchanged in the result.
        :return: Dictionary with dirtied owner IDs, book_col_map projections and id_map values for cache consumers.
        :raises KeyError: A non-integer value has no entry in val_map.
        """

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
            """
            Preserve an integer target ID or look up the resolved value.

            Example:
                Inside the enclosing conversion, integer 4 passes through and "Shelf" maps to 4 when val_map contains that entry.


            :param item: One target value from the surrounding update container.
            :param val_map: Mapping from non-integer values to target IDs.
            :return: The original integer, including bool, or val_map[item].
            :raises KeyError: A non-integer value is unresolved.
            :raises TypeError: A non-integer lookup key is unhashable.
            """

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
        Replace legacy custom links using one insertion record per update-map entry.

        Deletions pass singleton owner-ID tuples outside the lock. Nonempty updates acquire db.lock, clear updated owners and call add_cc_link_with_extra_multi with pairs or series triples. clean_before_write and priority do not alter behavior. In particular, collections from higher-level one-to-many converters are not expanded here; acceptance of such values depends on the macro. No cache update or whole-operation rollback is provided.

        Example:
            For scalar ``updated={7: 4}``, the ordinary branch sends (7, 4); the series branch sends (7, 4, 1.0).


        :param db: Database adapter used by the row/link helpers; collaborator errors propagate.
        :param table: Table with link_table, falling back to metadata["table"] on AttributeError.
        :param field: Field metadata with link_column for custom-series inserts.
        :param is_custom_series: Whether insertion records carry index 1.0.
        :param updated: Owner-to-item mapping; each value is forwarded as one item, without flattening collections.
        :param deleted: Owner IDs whose custom links are deleted before locked updates.
        :param clean_before_write: Compatibility argument not used by this helper.
        :param priority: Compatibility argument not used by this helper.
        :return: (None, None); no replacement cache maps are returned.
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
