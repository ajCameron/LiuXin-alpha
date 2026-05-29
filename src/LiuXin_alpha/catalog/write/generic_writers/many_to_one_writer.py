from __future__ import division, absolute_import, print_function, unicode_literals

import pprint
from collections import defaultdict
from copy import deepcopy

from LiuXin_alpha.catalog.write.base_writer import BaseWriter
from LiuXin_alpha.errors import InputIntegrityError
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems, basestring, dict_iterkeys as iterkeys
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.text.icu import safe_lower


class ManyToOneWriter(BaseWriter):
    """
    Write in to a many to one table.
    """

    def __init__(self, field):
        super(ManyToOneWriter, self).__init__(field)
        self.set_books_func = self.many_one
        self.set_books = self.no_adapter_set_books

        if field.table.typed:
            self._make_book_id_item_id_map = self._typed_make_book_id_item_id_map

    # Todo: Normalize names inside this function
    def many_one(self, book_id_val_map, db, field, allow_case_change, *args):
        """
        Update fields where many books are linked to one item.
        No examples of this exist in the canonical database. Custom examples might include "character_introductions"
        (characters can be introduced, at most, once) or shelf locations in a physical library (a book can be on, at
        most, one shelf).
        Retrieves the appropriate handler for the database upate for the particular field. Passes that into the update
        handler which is also responsible for updating the cache and running clean operations on the table.
        :param book_id_val_map: A map from the book ids to the update values
        :param db: The database to run the update on
        :param field: The field being updated
        :param allow_case_change: If True allows case changes when trying to match the updated value to existing values
                                  on the database.
        :param args:
        :return:
        """
        if args:
            info_str = "many_one had unexpected arguments passed into it"
            default_log.log_variables(info_str, "INFO", ("args", args))

        if not field.table.custom:
            db_update_links = {"rating": self.do_rating_many_one_db_update}.get(
                self.name, self.do_generic_many_one_db_update
            )
        else:
            db_update_links = self.do_custom_many_one_db_update

        db_clean_unused_items = {"rating": self.dummy_many_one_clear_unused}.get(
            self.name, self.generic_many_one_clear_unused
        )

        db_id_matcher = {"rating": self.get_rating_id}.get(self.name, self.get_db_id)

        dirtied = set()
        m = field.metadata
        table = field.table
        dt = m["datatype"]

        table.update_precheck(book_id_val_map, {})

        # custom series are new fields with a series like structure (in that they have indices - not the full
        # series-tree-index structure) - if the table is a custom column then it's name will start with the custom
        # columns prefix - #
        is_custom_series = dt == "series" and table.name.startswith("#")

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
        case_changes = {}
        id_map_update = dict()
        val_map = {None: None}

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

        # Preform case changes - if allowed
        if case_changes:
            self.change_case(case_changes, dirtied, db, table, m)

        # creating an in-memory map between the book ids and the item ids
        book_id_item_id_map = self._make_book_id_item_id_map(book_id_val_map, val_map)

        # Todo: Need to implement a per-table method to whether update is even required
        # Ignore those items whose value is the same as the current value
        book_id_item_id_map = {k: v for k, v in iteritems(book_id_item_id_map) if v != table.book_col_map.get(k, None)}
        dirtied |= set(book_id_item_id_map)

        # Todo: This should be done in the cache - where the storage details can be taken into account
        updated, deleted = table.internal_update_cache(book_id_item_id_map, id_map_update)

        book_col_map, id_map = db_update_links(db, table, field, is_custom_series, updated, deleted)

        rtn_info = dict()
        rtn_info["dirtied"] = set(updated.keys()).union(deleted)
        rtn_info["book_col_map"] = book_id_item_id_map
        rtn_info["id_map"] = id_map_update
        # Todo: Not being respected by the fields update method - contradictory methods used - c.f. internal_update_used
        rtn_info["cache_update_needed"] = False

        # Remove no longer used items
        try:
            clear_unused = m["clear_unused"]
        except KeyError:
            clear_unused = False

        if clear_unused:
            db_clean_unused_items(db, table, field)

        return rtn_info

    def _make_book_id_item_id_map(self, book_id_val_map, val_map):
        """
        Transform the book_id_val_map to a book_id_item_id_map
        :param book_id_val_map:
        :param val_map:
        :return:
        """
        book_id_item_id_map = dict()
        for book_id, item_val in iteritems(book_id_val_map):
            if item_val in val_map:
                book_id_item_id_map[book_id] = val_map[item_val]
            else:
                book_id_item_id_map[book_id] = item_val
        return book_id_item_id_map

    def _typed_make_book_id_item_id_map(self, book_id_val_map, val_map):
        """
        Transform the book_id_val_map to a book_id_item_id_map - in the case where the map contains type information as
        well.
        :param book_id_val_map:
        :param val_map:
        :return:
        """
        book_id_item_id_map = defaultdict(dict)
        for book_id, item_val in iteritems(book_id_val_map):
            if item_val is None:
                book_id_item_id_map[book_id] = None

            elif isinstance(item_val, dict):
                for link_type, link_val in iteritems(item_val):
                    if link_val is None:
                        book_id_item_id_map[book_id][link_type] = None
                    elif isinstance(link_val, basestring):
                        book_id_item_id_map[book_id][link_type] = val_map[link_val]
                    elif isinstance(link_val, int):
                        book_id_item_id_map[book_id][link_type] = link_val
                    else:
                        raise NotImplementedError

            else:
                err_str = "Unexpected form of book_id_val_map"
                err_str = default_log.log_variables(
                    err_str,
                    "ERROR",
                    ("book_id", book_id),
                    ("item_val", item_val),
                    ("book_id_val_map", book_id_val_map),
                )
                raise NotImplementedError(err_str)

        return book_id_item_id_map

    @staticmethod
    def dummy_many_one_clear_unused(db, table, field):
        """
        Remove unused elements from the ratings table.
        Currently not used - as that table should be preserved.
        :param db:
        :param table:
        :param field:
        :return:
        """
        pass

    @staticmethod
    def do_rating_many_one_db_update(db, table, field, is_custom_series, updated, deleted):
        """
        Preform updates of the title-ratings link table.
        :param db:
        :param table:
        :param field:
        :param is_custom_series:
        :param updated:
        :param deleted:
        :return:
        """
        # Preform updates on all the links which haven't been broken
        for book_id in updated:
            book_val = updated[book_id]
            db.macros.set_title_rating(book_id, book_val)

        # Break any links which have been marked to be deleted
        for book_id in deleted:
            db.macros.set_title_rating(db, book_id, 0)

        # Todo: This is a problem that needs to be fixed - by returning the maps - later
        return None, None

    def do_generic_many_one_db_update(self, db, table, field, is_custom_series, updated, deleted, link_type=None):
        """
        Use the generic database update handler to apply the changes to the database.
        :param db:
        :param table:
        :param is_custom_series:
        :param updated:
        :param deleted:
        :return:
        """
        # Update the db link table - remove all the links to the book
        if deleted:
            # Todo: Neither of these forms seem to actually work - fix this
            # db.macros.break_generic_link(table.link_table, table.link_table_bt_id_column, ((k,) for k in deleted))
            # db.macros.break_generic_link(table.link_table, table.link_table_bt_id_column, (k for k in deleted))
            for del_id in deleted:
                db.macros.break_generic_link(table.link_table, table.link_table_bt_id_column, del_id)

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

                for book_id, book_val in iteritems(updated):

                    if isinstance(book_val, int):

                        # About to write a new link - so all old links - regardless of type - must be broken
                        db.macros.break_generic_link(table.link_table, table.link_table_bt_id_column, book_id)

                        title_row = db.get_row_from_id("titles", row_id=book_id)
                        book_row = db.get_row_from_id(table.name, row_id=book_val)
                        db.interlink_rows(
                            primary_row=title_row,
                            secondary_row=book_row,
                            type=link_type,
                        )

                        # db.macros.make_generic_link_no_priority(table.link_table, table.link_table_table_id_column,
                        #                                         table.link_table_bt_id_column,
                        #                                         book_id, item_id)
                    elif isinstance(book_val, dict):

                        for book_link_type, book_link_val in iteritems(book_val):
                            # Recurse to deal with the case where
                            self.do_generic_many_one_db_update(
                                db,
                                table,
                                field,
                                is_custom_series,
                                updated={book_id: book_link_val},
                                deleted=dict(),
                                link_type=book_link_type,
                            )

                    elif book_val is None:

                        # Nullify the link for the specified type - or the whole thing
                        db.macros.break_generic_link(
                            table.link_table,
                            table.link_table_bt_id_column,
                            book_id,
                            link_type=link_type,
                        )

                    else:
                        raise NotImplementedError(self._book_val_has_unexpected_form(updated, book_val))

        return None, None

    def _book_val_has_unexpected_form(self, updated, book_val):
        """
        Err msg
        :param updated:
        :param book_val:
        :param book_val:
        :return:
        """
        err_msg = [
            "book_val was found to have unexpected form",
            "update: \n{}\n".format(pprint.pformat(updated)),
            "book_val: {}".format(book_val),
            "type(book_val): {}".format(type(book_val)),
        ]
        return "\n".join(err_msg)

    # Todo: This is probably going to lead to unpredictable results - especially with the cache rewrite - fix it
    @staticmethod
    def generic_many_one_clear_unused(db, table, field):
        """
        Clear now unused items from a many-one table on the database.
        :return:
        """
        remove = {item_id for item_id in table.id_map if not table.col_book_map.get(item_id, False)}
        if remove:
            m = field.metadata
            table_id = m["table_id"] if "table_id" in m.keys() else "id"
            db.macros.break_generic_link(m["table"], table_id, ((item_id,) for item_id in remove))

            # Todo: This needs to be in the table rather than in write - seperation of concerns
            for item_id in remove:
                del table.id_map[item_id]
                table.col_book_map.pop(item_id, None)

    @staticmethod
    def do_custom_many_one_db_update(db, table, field, is_custom_series, updated, deleted):
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
        # Update the db link table

        # delete all links to the books which have references cleared for them
        if deleted:
            try:
                cc_table = table.link_table
            except AttributeError:
                cc_table = table.metadata["table"]
            target_ids = set([k for k in deleted])
            db.macros.break_cc_links_by_book_id(lt=cc_table, book_id=target_ids)

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

    # Todo: Probably needs to be in the ensure method
    @staticmethod
    def get_rating_id(
        val,
        db,
        m,
        table,
        kmap,
        rid_map,
        allow_case_change,
        case_changes,
        val_map,
        is_authors=False,
        id_map_update=None,
    ):
        """
        Attempts to match the given rating value to an entry in the ratings table.
        :param val: The value to match - will fail if it's not an integer in the range 1-10.
        :param db:
        :param m:
        :param table:
        :param kmap:
        :param rid_map:
        :param allow_case_change:
        :param case_changes:
        :param val_map:
        :param is_authors:
        :return:
        """
        # Todo: Needs to do cache update - doesn't currently
        # Todo: These should really be methods in the cache - it'd be a whole lot more elegant

        old_val = deepcopy(val)

        # Pass False to set the rating for the title null
        if not val:
            val_map[val] = None
            return

        val = int(val)
        if val not in range(1, 11):
            err_str = "Cannot set rating - rating must be an integer in the range 1-10"
            err_str = default_log.log_variables(err_str, "ERROR", ("val", val))
            raise InputIntegrityError(err_str)

        val_map[old_val] = int(val)
