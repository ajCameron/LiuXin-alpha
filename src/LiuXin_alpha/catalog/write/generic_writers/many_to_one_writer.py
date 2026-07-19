
"""
Many entries are linked to one item.

E.g. comments - many comments can be linked to one item.

This is the dual of one-to-many.

"""


from __future__ import division, absolute_import, print_function, unicode_literals

import pprint
from collections import defaultdict
from copy import deepcopy

from typing import Callable, Any, Optional, TYPE_CHECKING

from LiuXin_alpha.catalog.write.base_writer import BaseCatalogWriter
from LiuXin_alpha.errors import InputIntegrityError
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems, basestring, dict_iterkeys as iterkeys
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.text.icu import safe_lower

if TYPE_CHECKING:

    from LiuXin_alpha.catalog.api import CatalogAPI


class ManyToOneWriter(BaseCatalogWriter):
    """
    Write in to a many-to-one table.
    """
    # Todo: Have we circled back to the point where a link dataclass - as an alternative constructor - should be included?
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
            link_typed: bool = False,
            datatype: Optional[str] = None,
    ) -> None:
        """
        Startup a Many-to-One writer - many entries linked to one in another table.

        (or, I guess, in the same table... but eh. Just use a link. It's easier to reason about).
        :param catalog:
        :param table:
        :param column:
        :param adapter:
        :param accept_vals:
        :param name:
        :param link_table:
        :param link_table_bt_id_column:
        :param link_table_item_id_column:
        :param link_typed:
        :param datatype:
        """

        super(ManyToOneWriter, self).__init__(
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

        self.set_values_func = self.many_one
        self.set_values = self.no_adapter_set_values

        if link_typed:
            self._make_book_id_item_id_map = self._typed_make_book_id_item_id_map

    def many_one(
            self,
            src_id_dst_val_map: dict[int, Any],
            allow_case_change: bool = False) -> set[int]:
        """
        Update fields where many books are linked to one item.

        No examples of this exist in the canonical database. Custom examples might include "character_introductions"
        (characters can be introduced, at most, once) or shelf locations in a physical library (a book can be on, at
        most, one shelf).
        Retrieves the appropriate handler for the database upate for the particular field. Passes that into the update
        handler which is also responsible for updating the cache and running clean operations on the table.

        :param src_id_dst_val_map: A map from the src ids to the update values
        :param allow_case_change: If True allows case changes when trying to match the updated value to existing values
                                  on the database.

        :return:
        """

        if not self.custom:
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

        # Todo: Need to isolate this from the field code and include here
        self.update_precheck(src_id_dst_val_map, {})

        # custom series are new fields with a series like structure
        # (in that they have indices - not the full series-tree-index structure) -
        # if the table is a custom column then it's name will start with the custom columns prefix - #)
        is_custom_series = self.datatype == "series" and self.name.startswith("#")

        # Clean the val map and make a note of the case changes - then match the given string to an entry on the
        # database
        case_changes = {}
        id_map_update = dict()
        val_map = {None: None}

        self._do_vals_to_ids(
            src_id_dst_val_map,
            db_id_matcher,
            allow_case_change,
            case_changes,
            val_map,
            id_map_update,
        )

        # Preform case changes - if allowed
        if case_changes:
            self.change_case(case_changes, dirtied)

        book_col_map, id_map = db_update_links(db, table, field, is_custom_series, updated, deleted)

        rtn_info = dict()
        rtn_info["dirtied"] = set(updated.keys()).union(deleted)
        rtn_info["book_col_map"] = book_id_item_id_map
        rtn_info["id_map"] = id_map_update
        # Todo: Not being respected by the fields update method - contradictory methods used - c.f. internal_update_used
        rtn_info["cache_update_needed"] = False

        # Remove no longer used items
        # Todo: We should store this and read it off the database
        try:
            clear_unused = m["clear_unused"]
        except KeyError:
            clear_unused = False

        if clear_unused:
            db_clean_unused_items(db, table, field)

        # return rtn_info
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
        Transform the book_id_val_map to a book_id_item_id_map

        In the case where the map contains type information as well.
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

    def do_rating_many_one_db_update(
            self,
            *,
            updated: dict[int, Any],
            deleted: set[int],
            is_custom_series: bool = False,
            link_type: Optional[str] = None) -> tuple[Optional[dict[int, int]], Optional[set[int]]]:
        """
        Preform updates of the title-ratings link table.

        :param updated:
        :param deleted:
        :param is_custom_series: Nope.
        :param link_type: Not relevant here

        :return:
        """
        assert not is_custom_series, "Not relevant to the this update."
        assert link_type is None, "Not relevant to the this update."

        # Preform updates on all the links which haven't been broken
        for book_id in updated:
            book_val = updated[book_id]
            self.catalog.metadata_sql.set_title_rating(book_id, book_val)

        # Break any links which have been marked to be deleted
        for book_id in deleted:
            self.catalog.metadata_sql.set_title_rating(book_id, 0)

        # Todo: This is a problem that needs to be fixed - by returning the maps - later
        return None, None

    def do_generic_many_one_db_update(
            self,
            *,
            updated: dict[int, Any],
            deleted: set[int],
            is_custom_series: bool = False,
            link_type: Optional[str] = None) -> tuple[Optional[dict[int, int]],  Optional[set[int]]]:
        """
        Use the generic database update handler to apply the changes to the database.

        :param updated:
        :param deleted:
        :param is_custom_series:
        :param link_type: Used to set the link type for the update - if one is present

        :return:
        """
        # Update the db link table - remove all the links to the book
        if deleted:
            # Todo: Neither of these forms seem to actually work - fix this
            # db.metadata_sql.break_generic_link(table.link_table, table.link_table_bt_id_column, ((k,) for k in deleted))
            # db.metadata_sql.break_generic_link(table.link_table, table.link_table_bt_id_column, (k for k in deleted))
            for del_id in deleted:
                self.catalog.metadata_sql.break_generic_link(self.link_table, self.link_table_bt_id_column, del_id)

        if updated:
            if is_custom_series:
                # Todo: Should trip this mess
                raise NotImplementedError("Not ready to try this for a custom series yet.")
                # del_stmt = 'DELETE FROM {0} WHERE book=?; '.format(table.link_table)
                # ins_stmt = 'INSERT INTO {0}(book, {1}, extra) VALUES(?, ?, 1.0);'.format(table.link_table, m['link_column'])
            else:
                pass

            # Lock the database to stop anything else from writing to it while doing the update
            with self.catalog.lock:

                for src_item_id, dst_item_val in iteritems(updated):

                    # We're writing based on an ID
                    if isinstance(dst_item_val, int):

                        # About to write a new link - so all old links - regardless of type - must be broken
                        self.catalog.metadata_sql.break_generic_link(self.link_table, self.link_table_bt_id_column, src_item_id)

                        src_row = self.catalog.get_row_from_id(self.table, row_id=src_item_id)
                        val_row = self.catalog.get_row_from_id(self.column_table, row_id=dst_item_val)
                        self.catalog.interlink_rows(
                            primary_row=src_row,
                            secondary_row=val_row,
                            type=link_type,
                        )

                        # db.macros.make_generic_link_no_priority(table.link_table, table.link_table_table_id_column,
                        #                                         table.link_table_bt_id_column,
                        #                                         book_id, item_id)

                    elif isinstance(dst_item_val, dict):

                        for book_link_type, book_link_val in iteritems(dst_item_val):
                            # Recurse to deal with the case where we have nested values
                            self.do_generic_many_one_db_update(
                                updated = {src_item_id: book_link_val},
                                deleted = set(),
                                link_type = book_link_type,
                            )

                    # Nullify the link for the specified type - or the whole thing
                    elif dst_item_val is None:

                        self.catalog.metadata_sql.break_generic_link(
                            self.link_table,
                            self.link_table_bt_id_column,
                            src_item_id,
                            link_type=link_type,
                        )

                    else:
                        raise NotImplementedError(self._book_val_has_unexpected_form(updated, dst_item_val))

        return None, None

    @staticmethod
    def _book_val_has_unexpected_form(updated, book_val):
        """
        Err msg.

        :param updated:
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
    def generic_many_one_clear_unused():
        """
        Clear now unused items from a many-one table on the database.

        :return:
        """
        raise NotImplementedError("This to be re-written into pure sql.")

    def do_custom_many_one_db_update(
            self,
            *,
            updated: dict[int, Any],
            deleted: set[int],
            is_custom_series: bool = False,
            link_type: Optional[str] = None) -> tuple[Optional[dict[int, int]],  Optional[set[int]]]:
        """
        Update a many-to-one entry in a custom table.

        :param updated:
        :param deleted:
        :param is_custom_series:
        :param link_type:

        :return:
        """
        # Update the db link table

        # delete all links to the books which have references cleared for them
        if deleted:
            try:
                cc_table = self.link_table
            except AttributeError:
                cc_table = self.table
            target_ids = set([k for k in deleted])
            self.catalog.macros.break_cc_links_by_book_id(lt=cc_table, book_id=target_ids)

        # Preformaing an update to the custom columns
        if updated:

            if is_custom_series:
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
