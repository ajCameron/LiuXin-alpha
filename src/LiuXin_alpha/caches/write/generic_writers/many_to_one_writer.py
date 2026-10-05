"""
Coordinate legacy many-to-one value resolution, cache updates and link writes.
"""

from __future__ import division, absolute_import, print_function, unicode_literals

import pprint
from collections import defaultdict
from copy import deepcopy

from LiuXin_alpha.caches.write.base_writer import BaseWriter
from LiuXin_alpha.errors import InputIntegrityError
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems, basestring, dict_iterkeys as iterkeys
from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.text.icu import safe_lower


class ManyToOneWriter(BaseWriter):
    """
    Write one target per owner, with optional typed legacy update mappings.

    Public writes bypass scalar adaptation, resolve value IDs, update the table cache and then persist links. The hook returns a result dictionary rather than only an affected-ID set.

    Example:
        With a configured many-to-one field, ``ManyToOneWriter(field).set_books({7: "Shelf A"}, db)`` resolves the value and returns update metadata.
    """

    def __init__(self, field):
        """
        Initialize shared state and bind unadapted many-to-one updates.

        Example:
            A typed table makes ``writer._make_book_id_item_id_map`` use the converter for ``{book_id: {link_type: value}}`` mappings.


        :param field: Legacy field supplying metadata and the relation cache/update hooks.
        :return: None; binds many_one and selects the typed converter when table.typed is truthy.
        """

        super(ManyToOneWriter, self).__init__(field)
        self.set_books_func = self.many_one
        self.set_books = self.no_adapter_set_books

        if field.table.typed:
            self._make_book_id_item_id_map = self._typed_make_book_id_item_id_map

    # Todo: Normalize names inside this function
    def many_one(self, book_id_val_map, db, field, allow_case_change, *args):
        """
        Resolve target values, update the table cache, and persist changed links.

        Select rating/custom/generic persistence and value matchers, precheck input, build a normalized reverse map and repair case duplicates. Resolve values through the shared helper and apply case changes, then convert IDs and discard values equal to the cached projection. internal_update_cache runs before database persistence. The returned dirtied set is computed from updated keys and deleted entries, replacing the earlier locally accumulated set. Optional metadata.clear_unused triggers cleanup after writes. No whole-operation rollback restores cache or database changes after failure.

        Example:
            With compatible cache hooks, ``writer.many_one({7: "Shelf A"}, db, field, True)`` resolves the shelf and returns the maps used for changed links.


        :param book_id_val_map: Book-to-value mapping accepted by table precheck and the selected typed/untyped converter.
        :param db: Database adapter used by the selected persistence helpers; errors propagate.
        :param field: Legacy field supplying metadata and the relation cache/update hooks.
        :param allow_case_change: Whether value resolution may schedule case changes.
        :param args: Additional compatibility arguments, logged and otherwise ignored.
        :return: Dictionary with dirtied, book_col_map, id_map and cache_update_needed=False.
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

        # Todo: We need some way to do this independent of the cache
        # Todo: We also need

        # Todo: SOD ME. No wonder calibre is a barely functional mess! This is happening whenever there's a write
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
        Replace known hashable values with IDs and preserve unmatched values.

        Every value is tested for membership in val_map, including integers. Unknown values pass through unchanged; lists, sets and dictionaries cannot be used as membership keys.

        Example:
            >>> ManyToOneWriter._make_book_id_item_id_map(None, {7: "Shelf", 8: 9}, {"Shelf": 4})
            {7: 4, 8: 9}


        :param book_id_val_map: Book IDs mapped to hashable scalar values.
        :param val_map: Value-to-ID lookup, commonly including None mapped to None.
        :return: A new book-to-ID/value dictionary.
        :raises TypeError: An input value is unhashable.
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
        Resolve typed scalar values while retaining None and integer IDs.

        Strings require an exact lookup; integer values including booleans pass through unchanged. Link-type keys are retained without validation. Unsupported outer shapes are logged before raising.

        Example:
            >>> result = ManyToOneWriter._typed_make_book_id_item_id_map(None, {7: {"role": "Shelf"}, 8: None}, {"Shelf": 4})
            >>> dict(result)
            {7: {'role': 4}, 8: None}


        :param book_id_val_map: Book IDs mapped to None or dictionaries of link type to None/string/integer.
        :param val_map: String-value-to-ID lookup for typed values.
        :return: A new defaultdict(dict) with converted typed values or None per book.
        :raises KeyError: A typed string is missing from val_map.
        :raises NotImplementedError: An outer or typed value has an unsupported shape.
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
        Preserve rating entries by performing no unused-item cleanup.

        Example:
            >>> ManyToOneWriter.dummy_many_one_clear_unused(None, None, None) is None
            True


        :param db: Compatibility argument not used by this helper.
        :param table: Compatibility argument not used by this helper.
        :param field: Compatibility argument not used by this helper.
        :return: None; no database or cache access occurs.
        """
        pass

    @staticmethod
    def do_rating_many_one_db_update(db, table, field, is_custom_series, updated, deleted):
        """
        Set requested title ratings, then clear deleted ratings with zero.

        Updates run before deletions, so an ID present in both ends at zero. This helper performs no independent range validation or cache refresh.

        Example:
            ``do_rating_many_one_db_update(db, table, field, False, {7: 8}, {9})`` sets rating 8 for book 7 and zero for book 9.


        :param db: Database adapter used by the selected persistence helpers; errors propagate.
        :param table: Compatibility argument not used by this helper.
        :param field: Compatibility argument not used by this helper.
        :param is_custom_series: Compatibility argument not used by this helper.
        :param updated: Book IDs mapped to rating values passed unchanged to metadata_sql.
        :param deleted: Book IDs whose rating is subsequently set to zero.
        :return: (None, None); this helper returns no replacement cache maps.
        """
        # Preform updates on all the links which haven't been broken
        for book_id in updated:
            book_val = updated[book_id]
            db.metadata_sql.set_title_rating(book_id, book_val)

        # Break any links which have been marked to be deleted
        for book_id in deleted:
            db.metadata_sql.set_title_rating(book_id, 0)

        # Todo: This is a problem that needs to be fixed - by returning the maps - later
        return None, None

    def do_generic_many_one_db_update(self, db, table, field, is_custom_series, updated, deleted, link_type=None):
        """
        Apply legacy integer, typed-dictionary or null link updates under the database lock.

        An integer update clears all links for that owner without a type filter, reads the owner from "titles" and links the target row with the requested type. Typed dictionaries recurse while the outer lock remains held; each integer branch again clears all owner types, so successive typed assignments can remove earlier ones. None clears links with the supplied type filter. Deletions happen before custom-series rejection and outside the lock. No cache update or transaction rollback is supplied.

        Example:
            ``writer.do_generic_many_one_db_update(db, table, field, False, {7: 4}, {})`` clears owner 7 links and inserts target 4.


        :param db: Database adapter used by the selected persistence helpers; errors propagate.
        :param table: Table providing link and destination table names.
        :param field: Field metadata accessed only when rejecting updated custom series.
        :param is_custom_series: Reject nonempty custom-series updates after processing deletions.
        :param updated: Book IDs mapped to integer targets, nested type dictionaries or None.
        :param deleted: Book IDs whose owner links are cleared before acquiring the update lock.
        :param link_type: Link type passed to insertion and nullification calls.
        :return: (None, None); this helper returns no replacement cache maps.
        :raises NotImplementedError: A nonempty custom-series update or unsupported value shape is encountered.
        """
        # Update the db link table - remove all the links to the book
        if deleted:
            # Todo: Neither of these forms seem to actually work - fix this
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

                for book_id, book_val in iteritems(updated):

                    if isinstance(book_val, int):

                        # About to write a new link - so all old links - regardless of type - must be broken
                        db.metadata_sql.break_generic_link(table.link_table, table.link_table_bt_id_column, book_id)

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
                        db.metadata_sql.break_generic_link(
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
        Format the rejected value and its enclosing update for diagnostics.

        Example:
            >>> "type(book_val): <class 'float'>" in ManyToOneWriter._book_val_has_unexpected_form(None, {7: 1.5}, 1.5)
            True


        :param updated: Complete update mapping included via pprint.
        :param book_val: Rejected value included with its Python type.
        :return: A four-part newline-joined diagnostic string; nothing is logged here.
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
        Remove cached unused target IDs after delegating database cleanup.

        Select IDs whose reverse membership is absent or false. Pass singleton ID tuples to break_generic_link using metadata.table directly, then delete those IDs from id_map and col_book_map. This does not independently verify storage references; a database failure prevents the subsequent cache removals.

        Example:
            If cached ID 4 has no reverse membership, ``generic_many_one_clear_unused(db, table, field)`` submits it for cleanup and then removes its cache entries.


        :param db: Database adapter used by the selected persistence helpers; errors propagate.
        :param table: Table with id_map and reverse col_book_map used to identify unused targets.
        :param field: Field metadata providing table and optional table_id, defaulting to "id".
        :return: None; any described database/cache mutations happen in place.
        """
        remove = {item_id for item_id in table.id_map if not table.col_book_map.get(item_id, False)}
        if remove:
            m = field.metadata
            table_id = m["table_id"] if "table_id" in m.keys() else "id"
            db.metadata_sql.break_generic_link(m["table"], table_id, ((item_id,) for item_id in remove))

            # Todo: This needs to be in the table rather than in write - seperation of concerns
            for item_id in remove:
                del table.id_map[item_id]
                table.col_book_map.pop(item_id, None)

    @staticmethod
    def do_custom_many_one_db_update(db, table, field, is_custom_series, updated, deleted):
        """
        Replace custom-column links, optionally writing a default series index.

        Deletions use a set of IDs outside the lock. Nonempty updates acquire db.lock, break links using singleton book-ID tuples, then call add_cc_link_with_extra_multi. Series sequences contain (book_id, item_id, 1.0) with target_column; ordinary sequences contain pairs with extra=False. No cache refresh or rollback is performed here.

        Example:
            A custom-series update ``{7: 4}`` inserts the sequence (7, 4, 1.0) after existing custom links for book 7 are cleared.


        :param db: Database adapter used by the selected persistence helpers; errors propagate.
        :param table: Table supplying link_table, or metadata["table"] when that attribute is absent.
        :param field: Field metadata supplying link_column for custom series.
        :param is_custom_series: Whether inserted links carry extra index 1.0.
        :param updated: Book IDs mapped to target IDs.
        :param deleted: Book IDs whose custom links are cleared before locked replacements.
        :return: (None, None); this helper returns no replacement cache maps.
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
        Map a false value to None or an integer-convertible rating to the range 1–10.

        False values map to None without int conversion. Other values pass through int, so fractional numbers are truncated and True becomes 1. The converted result must be 1–10. No database lookup, case update or id_map_update change occurs; the original value must be a usable dictionary key.

        Example:
            >>> values = {}
            >>> ManyToOneWriter.get_rating_id("8", None, None, None, None, {}, False, {}, values)
            >>> values
            {'8': 8}


        :param val: Original rating value, deep-copied before conversion.
        :param db: Compatibility argument not used by this helper.
        :param m: Compatibility argument not used by this helper.
        :param table: Compatibility argument not used by this helper.
        :param kmap: Compatibility argument not used by this helper.
        :param rid_map: Compatibility argument not used by this helper.
        :param allow_case_change: Compatibility argument not used by this helper.
        :param case_changes: Compatibility argument not used by this helper.
        :param val_map: Mutable map receiving the result under the original value.
        :param is_authors: Compatibility argument not used by this helper.
        :param id_map_update: Compatibility argument not used by this helper.
        :return: None; any described database/cache mutations happen in place.
        :raises InputIntegrityError: The converted integer is outside 1–10.
        :raises ValueError: int cannot parse the supplied value.
        :raises TypeError: Conversion or insertion of the original map key is unsupported.
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
