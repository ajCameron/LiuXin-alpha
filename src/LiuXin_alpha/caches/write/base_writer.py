
"""
Adapt legacy field writes and share Calibre-style value/link mutation helpers.

Writers retain a field, choose an input adapter and invoke a specialized
set_books_func. Shared helpers resolve/create related values, record case
changes and mutate title-linked associations. These compatibility paths
rely on legacy table maps and database macros; they do not provide the
modern Cache facade's lifecycle or transaction/reconciliation boundary.
"""

from __future__ import division, absolute_import, print_function, unicode_literals, annotations

import pprint
from copy import deepcopy

from typing import TYPE_CHECKING, Any, Callable, Mapping, Optional, Iterable, Union

from LiuXin_alpha.catalog import Catalog
from LiuXin_alpha.catalog.api.common import MetadataCandidate
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
    Adapt accepted field values and delegate legacy database writes.

    Construction records the field and its datatype and selects an adapter.
    Subclasses replace set_books_func or implement the method. Specialized
    helpers can mutate database rows, links and cache maps in several stages;
    failures can follow earlier writes without rollback. The book-ID spelling
    in this layer often selects rows from the titles table.

    Example:
        A title writer uses the same input adaptation wrapper with its own
        set_books_func implementation.
    """

    def __init__(self, field: "FieldBasicInterfaceAPI") -> None:
        """
        Select the field adapter and retain legacy writer metadata.

        Adapter selection happens first and can fail before remaining attributes
        are assigned. No database write or cache initialization is performed.

        Example:
            A subclass can replace accept_vals after construction to filter values before adaptation.


        :param field: Field providing name and metadata, including datatype.
        :return: None; stores adapter/name/field/datatype and an accept-all value predicate.
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
        Require a subclass to implement the actual field write hook.

        Example:
            Subclasses bind set_books_func to a specialized setter during construction.


        :param book_id_val_map: Owner-ID-to-value mapping supplied by the wrapper.
        :param db: Database/catalog adapter used by the concrete writer.
        :param field: Field whose storage is being changed.
        :param allow_case_change: Case-change policy offered to the concrete hook.
        :return: No result; the base hook always raises.
        :raises NotImplementedError: The base hook is called without an implementation.
        """
        raise NotImplementedError("Needs to be overridden.")

    def no_adapter_set_books(
            self,
            book_id_val_map,
            db: "CatalogAPI",
            allow_case_change: bool = True) -> set[int]:
        """
        Pass a nonempty value mapping directly to the selected write hook.

        Catch Exception from the hook, log it with the selected function, then
        reraise. Logging can itself fail; no rollback or post-write repair is added.

        Example:
            An empty mapping returns set() without accessing the hook or database.


        :param book_id_val_map: Caller mapping forwarded unchanged without acceptance filtering or adaptation.
        :param db: Database/catalog adapter passed to the hook.
        :param allow_case_change: Case-change flag passed positionally to the hook.
        :return: Hook-provided dirty-ID set, or a new empty set for false-valued input.
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
        Filter original values, adapt accepted values and invoke the write hook.

        Evaluate accept_vals on each original value before self.adapter. Build
        the whole new mapping before writing. Filtering/adaptation failures occur
        outside the logging try block; hook Exceptions are logged and reraised.
        No transaction or cache reconciliation is added here.

        Example:
            >>> from types import SimpleNamespace
            >>> writer = SimpleNamespace(accept_vals=lambda v: v > 0, adapter=str, field=None,
            ...     set_books_func=lambda values, db, field, case: set(values))
            >>> BaseWriter.set_books(writer, {1: -1, 2: 3}, None)
            {2}


        :param book_id_val_map: Owner-ID-to-value mapping; keys are retained while accepted values are adapted.
        :param db: Database/catalog adapter passed to the selected hook.
        :param allow_case_change: Case-change flag passed positionally to the hook.
        :return: Hook-provided dirty-ID set, or a new empty set when no values survive.
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
        Resolve or create a related value and update caller-owned lookup maps.

        Resolve target table/column before reverse lookup. On a missing key,
        create an author through Catalog.agents.match_or_create_person, ensure a
        custom-column value through macros, or fill/sync a blank ordinary row.
        Author creation replaces commas with pipes in its stored name, computes
        author sort and updates asort_map/alink_map. Failures while adding to
        seen_item_ids are suppressed in author and ordinary-row paths.

        Store the resolved ID in rid_map for a new value. Existing values can record
        case_changes without writing that case here. Always update id_map_update
        and val_map afterward. These mutations and row creation are not atomic;
        a later map/key error can follow persistence.

        Example:
            Two inputs sharing the same kmap key can reuse the first resolved ID
            through the mutated rid_map.


        :param val: Value keyed through kmap and retained in the output maps.
        :param db: Database/catalog adapter used for metadata lookup and creation.
        :param m: Table-name string or metadata mapping with table and column entries.
        :param table: Legacy table object holding ID, seen-ID and optional author maps.
        :param kmap: Function mapping the value to its reverse-index lookup key.
        :param rid_map: Reverse-value lookup mapping mutated when a missing value is created.
        :param allow_case_change: True records a differing existing display value as a case change.
        :param case_changes: Mutable ID-to-value mapping receiving requested case changes.
        :param val_map: Mutable original-value-to-ID output mapping.
        :param is_authors: True creates people through Catalog and updates author-specific cache maps.
        :param id_map_update: Optional mutable ID-to-value output map; None creates a new dictionary.
        :return: The supplied or newly created id_map_update dictionary.
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
                catalog = Catalog(db)
                item_id = catalog.agents.match_or_create_person(
                    MetadataCandidate(
                        {
                            "name": val.replace(",", "|"),
                            "sort_name": aus,
                            "type": "person",
                        }
                    )
                )
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
        Break owner links using the first element of each supplied deletion entry.

        Do not normalize entries or delete destination rows explicitly. Strings
        are also subscripted, so a plain string entry contributes its first character.
        Any foreign-key cleanup is controlled by the database, not this helper.

        Example:
            Entries [(7,), (8,)] select IDs (7, 8) for link removal.


        :param db: Database adapter exposing metadata_sql.break_generic_link.
        :param field: Field whose table supplies link-table and owner-link-column names.
        :param deleted: Deletion entries indexed at position zero; normally one-element records.
        :return: None; invokes the generic link-breaking macro with a tuple of extracted IDs.
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
        Break custom-column owner links using first elements of deletion entries.

        The helper does not validate entry shape or explicitly delete owner rows.

        Example:
            Entries [(7,), (8,)] are forwarded as book_id=(7, 8).


        :param db: Database adapter exposing macros.break_cc_links_by_book_id.
        :param field: Field whose metadata table identifies the custom-column link target.
        :param deleted: Deletion entries indexed at position zero.
        :return: None; calls the custom-column macro with the extracted ID tuple.
        """
        deleted_ids = tuple(de[0] for de in deleted)

        db.macros.break_cc_links_by_book_id(lt=field.metadata["table"], book_id=deleted_ids)

    # Todo: Check that dirtied has an update method
    @staticmethod
    def change_case(case_changes, dirtied, db, table, m, is_authors=False):
        """
        Write related-value case changes and dirty every cached referring owner.

        For a string metadata selector use direct_get_display_column; otherwise
        read table/column entries. Database author values replace commas with pipes,
        while id_map retains the caller value. Invoke update_columns even for empty
        changes. Later cache/map failures do not roll back the earlier database call.

        Example:
            Changing one shared author display value dirties all owners in that
            author's col_book_map entry.


        :param case_changes: ID-to-display-value changes, read without copying nested values.
        :param dirtied: Mutable dirty-owner collection supporting update.
        :param db: Database adapter exposing column updates and display-column lookup.
        :param table: Legacy table holding id_map, col_book_map and optional asort_map.
        :param m: Table name or metadata mapping identifying the target column.
        :param is_authors: True replaces commas for stored author names and recomputes cached author sort.
        :return: None; writes the database values before changing cache maps and dirty IDs.
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
        Replace title-linked associations using legacy exclusive-destination rules.

        Remove deleted-owner links before acquiring the update lock. Nonempty
        custom-series updates raise after those deletions. For other updates acquire
        db.lock and fetch each owner from titles. Integer targets break existing
        source and destination links filtered by link_type, then create the link.

        Sequence/set targets break the source links for that type, deep-copy and
        reverse their iteration order, then remove every destination's existing
        links without a type restriction before creating replacements. Type dicts
        recurse for non-None values; None values only clear that owner/type.
        Recursion enters db.lock again, so the lock must support that usage.
        clean_before_write has no effect. No enclosing transaction, row deletion
        or cache-map repair is added; failures can follow earlier unlinking/writes.

        Example:
            A sequence replacement can transfer a destination away from another
            owner by breaking its old destination-side links first.


        :param db: Database adapter with metadata_sql/macros, row lookup and a context-managed lock.
        :param table: Legacy target table describing link names and endpoint columns.
        :param field: Field metadata used when checking the unsupported custom-series branch.
        :param is_custom_series: True rejects nonempty updates through the unimplemented custom-series branch.
        :param updated: Owner-ID mapping to integer targets, sequences/sets or per-type dictionaries.
        :param deleted: Owner IDs whose links are removed before update processing.
        :param clean_before_write: Compatibility flag forwarded recursively but not used to control cleanup.
        :param link_type: Optional link type passed to creation and selected deletion/reprioritization calls.
        :return: (None, None) after processing; no dirty-ID or result payload is calculated.
        :raises NotImplementedError: A nonempty custom-series update or unsupported target shape is encountered.
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
        Reconcile title-linked association sets while retaining reusable link metadata.

        Apply deleted-owner link removals before the update lock and before
        rejecting nonempty custom-series updates. Under db.lock, integer targets
        create a link and reprioritize on DatabaseIntegrityError. Sequence/set
        targets read existing IDs for link_type, reverse a deep-copied input list,
        reprioritize existing links and create/reprioritize missing ones.

        Remove previously accepted targets absent from the replacement set using
        break_generic_single_link without a type argument. Type dictionaries recurse
        for non-None values, while None clears that owner/type. Recursive calls
        reenter the lock. clean_before_write is unused; integer updates do not
        replace the entire source set. No enclosing transaction or rollback is added.

        Example:
            An existing association retained in a sequence is reprioritized so its
            additional stored link metadata can survive.


        :param db: Database adapter with metadata_sql/macros, row lookup and a context-managed lock.
        :param table: Legacy target table describing link names and endpoint columns.
        :param field: Field metadata used when checking the unsupported custom-series branch.
        :param is_custom_series: True rejects nonempty updates through the unimplemented custom-series branch.
        :param updated: Owner-ID mapping to integer targets, sequences/sets or per-type dictionaries.
        :param deleted: Owner IDs whose links are removed before update processing.
        :param clean_before_write: Compatibility flag forwarded recursively but not used to control cleanup.
        :param link_type: Optional link type passed to creation and selected deletion/reprioritization calls.
        :return: (None, None) after processing; cache maps are not repaired here.
        :raises NotImplementedError: A nonempty custom-series update or unsupported target shape is encountered.
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
        Resolve non-integer values into shared lookup maps through a caller matcher.

        Skip top-level None and integers, including bool. Reverse a deep copy
        of lists before resolving non-integer elements; iterate sets in their
        existing order. Strings are single values. Type dictionaries send each
        truthy value through the same helper, skipping false-valued entries.
        Tuples and unsupported outer/nested shapes raise; earlier matcher calls
        may already have created rows or updated maps.

        Example:
            For a list ["A", 7, "B"], matching visits "B" then "A" and leaves
            integer ID 7 unresolved because it is already an identity.


        :param book_id_val_map: Owner mapping containing strings, lists, sets, integer IDs, type dictionaries or None.
        :param db_id_matcher: Matcher invoked for each unresolved value with shared map arguments.
        :param db: Database/catalog adapter passed to the matcher.
        :param m: Table-name or field metadata selector passed to the matcher.
        :param table: Legacy related-value table passed to the matcher.
        :param kmap: Value-key normalization callable passed to the matcher.
        :param rid_map: Reverse-key-to-ID mapping that the matcher can extend.
        :param allow_case_change: Case-change permission passed unchanged.
        :param case_changes: Mutable ID-to-display-value case-change output.
        :param val_map: Mutable original-value-to-ID output.
        :param id_map_update: Mutable ID-to-value output passed by keyword to the matcher.
        :return: None; matching effects occur through callbacks and shared dictionaries.
        :raises NotImplementedError: An outer or nested value has an unsupported shape.
        """
        def _process_list_set_str_val(val) -> None:
            """
            Resolve one supported scalar or collection using the enclosing matcher state.

            Lists are deep-copied and reversed; sets are iterated directly. Integer
            elements and scalar integers are skipped. Strings are matched as a whole;
            other shapes raise without rolling back earlier callback effects.

            Example:
                A list containing [3, "tag"] invokes the matcher only for "tag".


            :param val: List, set, string or integer value to interpret.
            :return: None; calls the matcher for unresolved elements using enclosing shared maps.
            :raises NotImplementedError: The supplied scalar/container type is unsupported.
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
        Format the full update mapping and unsupported value for a diagnostic.

        Example:
            >>> "type(val): <class 'float'>" in BaseWriter._unexpected_val_in_book_id_val_map({1: 1.5}, 1.5)
            True


        :param book_id_val_map: Complete input mapping rendered with pprint.pformat.
        :param val: Unsupported value whose string form and runtime type are included.
        :return: Multiline diagnostic string; no logging or raising is performed here.
        """
        err_msg = [
            "Unexpected value found in book_id_val_map",
            "book_id_val_map: \n{}\n".format(pprint.pformat(book_id_val_map)),
            "val: {}".format(val),
            "type(val): {}".format(type(val)),
        ]
        return "\n".join(err_msg)
