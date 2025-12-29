"""
This is a basic ManyToManyTable - no priority or type information is stored.
"""

from collections import defaultdict
from copy import deepcopy

from typing import TypeVar, Optional, Type, Iterable, Any
from typing_extensions import Self

from LiuXin.customize.cache.base_tables import (
    BaseManyToManyTable,
    MANY_MANY,
)
from LiuXin.databases.caches.calibre.tables.base import null
from LiuXin.databases.caches.calibre.tables.many_one_tables.many_to_one_table import CalibreManyToOneTable
from LiuXin.databases.db_types import (
    TableTypes,
    MetadataDict,
    InterLinkTableName,
    SrcTableID,
    DstTableID,
)
from LiuXin.databases.write import uniq

from LiuXin.exceptions import (
    DatabaseIntegrityError,
    InputIntegrityError,
    InvalidCacheUpdate,
)
from LiuXin.exceptions import InvalidUpdate

from LiuXin.utils.icu import lower as icu_lower
from LiuXin.utils.lx_libraries.liuxin_six import (
    dict_iteritems as iteritems,
    dict_itervalues as itervalues,
)
from LiuXin.utils.logger import default_log
from LiuXin.utils.general_ops.language_tools import plural_singular_mapper

# Py2/Py3 compatibility layer
from past.builtins import basestring

T = TypeVar("T")


class CalibreManyToManyTable(CalibreManyToOneTable, BaseManyToManyTable[T]):
    """
    Represents data that has a many-to-many mapping with books. i.e. each book can have more than one value and each
    value can be mapped to more than one book. For example: tags or authors.
    """

    # Todo: Should not be able to change these properties
    table_type: TableTypes = MANY_MANY

    # Characterize the table
    # Todo: This should not be changeable
    _priority: bool = False
    _typed: bool = False

    def __init__(
        self, name: str, metadata: MetadataDict, link_table: Optional[InterLinkTableName] = None, custom: bool = False
    ) -> None:
        """
        Startup the ManyToMany table.

        Noting if it is custom - which affects what the link table is called.
        :param name: The name of the table
        :param metadata: The metadata associated with the table - how it should display and other properties
        :param link_table: The table linking this table to either the books or titles field
        :param custom: Is this table a custom table
        """
        super(CalibreManyToManyTable, self).__init__(name=name, metadata=metadata, link_table=link_table, custom=custom)

        # Properties of the table
        # - Has the type gone through?
        self.data_loaded: bool = False

        # - A subtable is a table composed of a selection from the main table
        #   i.e. you could just select elements of a certain type from the main table and form a sutable from them
        self.is_subtable: bool = False

        # Todo: This should be called all_book_ids or something like it
        self.seen_book_ids: set[SrcTableID] = set()

        self.seen_books: set[T] = set()
        self.known_link_types: set[str] = set()

    @classmethod
    def from_typed_table(cls, db, original_table: Type[Self], type_filter: str) -> None:
        """
        Build a ManyToManyTable from existing maps.

        :param db: The database currently being cached
        :param original_table: The original table to build this table from
        :param type_filter: Create the new table restricted to these types from the old
        :return:
        """
        assert original_table.typed

        sub_table = cls(
            name=original_table.name,
            metadata=original_table.metadata,
            link_table=original_table.link_table,
        )
        sub_table.is_subtable = True
        sub_table.set_link_table(db=db)

        sub_table.id_map = original_table.id_map
        sub_table.book_col_map = original_table.book_col_map[type_filter]
        sub_table.col_book_map = original_table.col_book_map[type_filter]

    @staticmethod
    def _book_col_map_factory() -> dict[SrcTableID, set[DstTableID]]:
        """
        Produces the container for the book_col_map.

        This varies depending on the sort of information to be stored.
        E.g. if a ManyToMany table is not priority or typed then it's just a dict keyed with the book ids and valued
        with a set
        E.g. if a ManyToMany table is priority, but not typed, then it's a dict keyed with the book ids and valued with
        a list.
        E.g. if a ManyToMany table is typed, but not priority, then it's a dict keyed with the book ids and then valued
        with another dict, which is keyed with the name of the type, then valued with a set
        E.g. if a ManyToMany table is typed, and priority, then it's a dict keyed with the book ids and then valued
        with another dict, which is keyed with the name of the type, then valued with a list
        :return:
        """

        return defaultdict(set)

    def book_data(self, book_id: SrcTableID) -> dict[SrcTableID, set[DstTableID]]:
        """
        Return the book data for a given book id

        :param book_id:
        :return:
        """
        return deepcopy(self.book_col_map[book_id])

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - READ METHODS

    def read_maps(self, db, type_filter: Optional[str] = None) -> bool:
        """
        Reads the database and produces two maps - bool_col_map and col_book_map.

        book_col_map is keyed with the book_id and valued with a list of the ids of the items in the target table
        col_book_map is keyed with the item ids and valued with a set of the ids of the books
        :param db: The database to read off
        :param type_filter:
        :return status: Was data read into the cache successfully?
        """
        # Todo: These have already been loaded in the class - use from there instead of another call
        if self.custom:
            return self.read_maps_custom(db)

        # Todo: Shouldn't be trying to do this here - use the inherited table types
        # Todo: Likewise these should be split down into more classes
        try:
            link_table_priority = db.driver_wrapper.get_link_column(
                table1="titles", table2=self.metadata["table"], column_type="priority"
            )
        except DatabaseIntegrityError:
            link_table_priority = None

        if type_filter is None:
            if link_table_priority is None:
                return self.read_maps_no_type_filter_no_type_no_priority(db)

            else:
                return self.read_maps_no_type_filter_no_type_priority(db)
        else:

            if link_table_priority is not None:
                return self.read_maps_type_filter_no_type_no_priority(db, type_filter)
            else:
                return self.read_maps_type_filter_no_type_priority(db, type_filter)

    # Todo: possibly merge this over into macros
    # Todo: This does not, currently, respect priority - possible it should?
    def read_maps_type_filter_no_type_priority(self, db, type_filter: str) -> bool:
        """
        Preform a read in the case where a type filter has been set, but no type is recorded, likewise no priority.

        :param db:
        :param type_filter:
        :return:
        """
        book_col_map = self._book_col_map_factory()
        col_book_map = self._col_book_map_factory()
        seen_books = self.seen_books

        # Get the name of the link table and the relevant columns
        link_table_name = self.link_table
        link_table_book = self.link_table_bt_id_column
        link_table_other = self.link_table_table_id_column

        link_table_type = db.driver_wrapper.get_link_column(
            table1="titles", table2=self.metadata["table"], column_type="type"
        )

        try:
            link_table_priority = db.driver_wrapper.get_link_column(
                table1="titles", table2=self.metadata["table"], column_type="priority"
            )
        except DatabaseIntegrityError:
            link_table_priority = None

        stmt = self.selectq_filter_no_priority.format(
            link_table_book,
            link_table_other,
            link_table_name,
            link_table_type,
            type_filter,
        )

        # Preform the search and cache the results
        for book_id, item_id in db.driver_wrapper.execute(stmt):
            col_book_map[item_id].add(book_id)
            book_col_map[book_id].append(item_id)

            seen_books.add(book_id)

        # Convert everything into a tuple
        self.book_col_map = {k: tuple(v) for k, v in iteritems(book_col_map)}
        self.col_book_map = col_book_map

        return True

    def read_maps_type_filter_no_type_no_priority(self, db, type_filter: str) -> bool:
        """
        Read maps in the case where there is no type filter but no type or priority.

        :return:
        """
        book_col_map = self._book_col_map_factory()
        col_book_map = self._col_book_map_factory()

        seen_books = self.seen_books

        # Get the name of the link table and the relevant columns
        link_table_name = self.link_table
        link_table_book = self.link_table_bt_id_column
        link_table_other = self.link_table_table_id_column

        link_table_type = db.driver_wrapper.get_link_column(
            table1="titles", table2=self.metadata["table"], column_type="type"
        )

        try:
            link_table_priority = db.driver_wrapper.get_link_column(
                table1="titles", table2=self.metadata["table"], column_type="priority"
            )
        except DatabaseIntegrityError:
            link_table_priority = None

        stmt = self.selectq_filter_desc.format(
            link_table_book,
            link_table_other,
            link_table_name,
            link_table_priority,
            link_table_type,
            type_filter,
        )

        # Preform the search and cache the results
        for book_id, item_id in db.driver_wrapper.execute(stmt):
            col_book_map[item_id].add(book_id)
            book_col_map[book_id].append(item_id)

            seen_books.add(book_id)

        # Convert everything into a tuple
        self.book_col_map = {k: tuple(v) for k, v in iteritems(book_col_map)}
        self.col_book_map = col_book_map

        return True

    def read_maps_no_type_filter_no_type_priority(self, db) -> bool:
        """
        Read maps in the case where there is no type filter and no type - but a priority is set.

        :return:
        """
        book_col_map = self._book_col_map_factory()
        col_book_map = self._col_book_map_factory()

        seen_books = self.seen_books

        # Get the name of the link table and the relevant columns
        link_table_name = self.link_table
        link_table_book = self.link_table_bt_id_column
        link_table_other = self.link_table_table_id_column
        # Todo: Shouldn't be trying to do this here - use the inherited table types
        try:
            link_table_priority = db.driver_wrapper.get_link_column(
                table1="titles", table2=self.metadata["table"], column_type="priority"
            )
        except DatabaseIntegrityError:
            link_table_priority = None

        stmt = self.selectq_desc.format(link_table_book, link_table_other, link_table_name, link_table_priority)

        # Preform the search and cache the results
        for book_id, item_id in db.driver_wrapper.execute(stmt):
            col_book_map[item_id].add(book_id)
            book_col_map[book_id].append(item_id)

            seen_books.add(book_id)

        # Convert everything into a tuple
        self.book_col_map = {k: tuple(v) for k, v in iteritems(book_col_map)}
        self.col_book_map = col_book_map

        return True

    def read_maps_no_type_filter_no_type_no_priority(self, db) -> bool:
        """
        Read maps in the case where there is no type filter, no type is set and no priority is set.

        :return:
        """
        link_table_name = self.link_table
        link_table_book = self.link_table_bt_id_column
        link_table_other = self.link_table_table_id_column

        book_col_map = self._book_col_map_factory()
        col_book_map = self._col_book_map_factory()

        seen_books = self.seen_books

        stmt = self.selectq_no_priority.format(link_table_book, link_table_other, link_table_name)

        # Preform the search and cache the results
        for book_id, item_id in db.driver_wrapper.execute(stmt):
            col_book_map[item_id].add(book_id)
            book_col_map[book_id].append(item_id)

            seen_books.add(book_id)

        # Convert everything into a tuple
        self.book_col_map = {k: tuple(v) for k, v in iteritems(book_col_map)}
        self.col_book_map = col_book_map

        return True

    def read_maps_custom(self, db) -> bool:
        """
        Preform read from a custom table from the database.

        :param db:
        :return:
        """
        book_col_map = self._book_col_map_factory()
        col_book_map = self._col_book_map_factory()

        seen_books = self.seen_books

        try:
            self_lt = self.link_table
        except AttributeError:
            self_lt = None

        if self_lt:
            custom_link_table = self_lt
        else:
            custom_link_table = "books_{}_link".format(self.metadata["table"])
        self.link_table = custom_link_table
        self.link_table_bt_id_column = "book"
        self.link_table_table_id_column = "value"

        if not self.custom:
            # Todo: Switch over to using named formatting for this
            # Todo: This should be deprecated - should never be getting into custom with a table which is not marked
            #       as custom
            stmt = self.custom_selectq_old.format(self.metadata["link_column"], custom_link_table)
        else:
            cl_table_col = plural_singular_mapper(custom_link_table)
            stmt = self.custom_selectq.format(
                lt_col=cl_table_col,
                lt_link_col=self.metadata["link_column"],
                lt=custom_link_table,
            )

        # Preform the search and cache the results
        for book_id, item_id in db.driver_wrapper.execute(stmt):
            col_book_map[item_id].add(book_id)
            book_col_map[book_id].append(item_id)

            seen_books.add(seen_books)

        # Convert everything into a tuple
        self.book_col_map = {k: tuple(v) for k, v in iteritems(book_col_map)}
        self.col_book_map = col_book_map

        return True

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - UPDATE METHODS
    def update_preflight(
        self,
        book_id_item_id_map: dict[SrcTableID, Iterable[T]],
        id_map_update: dict[DstTableID, T],
        dirtied: Optional[set[SrcTableID]] = None,
    ) -> tuple[dict[SrcTableID, set[T]], Optional[set[SrcTableID]]]:
        """
        Gives the table a chance to bring the :param book_id_item_id_map:

        :param book_id_item_id_map: The update map to preform the preflight on
        :param id_map_update: Also needed to fully define the update
        :param dirtied: Note the ids of books where changes had to be made - done here because the "has update occurred"
                        logic might be complicated.
        :return:
        """
        dirtied = set() if dirtied is None else dirtied

        # Standardize the book_id_item_id_map
        clean_book_id_item_id_map: dict[SrcTableID, set[T]] = dict()
        for book_id, book_vals in iteritems(book_id_item_id_map):

            if book_vals is None:
                clean_book_id_item_id_map[book_id] = set()

            elif isinstance(book_vals, (int, basestring)):
                try:
                    clean_book_id_item_id_map[book_id] = {
                        book_vals,
                    }.union(self.book_col_map[book_id])
                except KeyError:
                    raise InvalidUpdate
                dirtied.add(book_id)
                continue

            elif isinstance(book_vals, (set, frozenset)):
                clean_book_id_item_id_map[book_id] = book_vals

            elif isinstance(book_vals, (tuple, list)):
                clean_book_id_item_id_map[book_id] = set(book_vals)

            elif isinstance(book_vals, (dict,)):
                if not book_vals:
                    clean_book_id_item_id_map[book_id] = set()
                else:
                    err_str = "Couldn't parse book_id_item_id_map - dictionary fallback failed"
                    err_str = default_log.log_variables(
                        err_str,
                        "ERROR",
                        ("book_id_item_id_map", book_id_item_id_map),
                        ("type(book_vals)", type(book_vals)),
                    )
                    raise NotImplementedError(err_str)
            else:
                err_str = "Couldn't parse book_id_item_id_map"
                err_str = default_log.log_variables(
                    err_str,
                    "ERROR",
                    ("book_id_item_id_map", book_id_item_id_map),
                    ("type(book_vals)", type(book_vals)),
                )
                raise NotImplementedError(err_str)

        return clean_book_id_item_id_map, id_map_update

    def update_precheck(
        self, book_id_item_id_map: dict[SrcTableID, set[DstTableID]], id_map_update: dict[DstTableID, T]
    ) -> bool:
        """
        Check that an update is of a valid form before writing it out to the cache and the database.

        Called when you know the ids you want to assign to the book after the update. Checks those ids are valid.
        No changes will be made to the :param book_id_item_id_map: (e.g. if the map is valued with tuples - not lists
        as expected - this will not be corrected.
        Raised InvalidCacheUpdate if the cache update is invalid in some way.

        :param book_id_item_id_map: Keyed with the ids of the books to update and valued with the
        :param id_map_update:
        :return status: Should always return - an exception is raised if anything fails.
        """
        # Precheck can be run more than once sometimes
        if hasattr(book_id_item_id_map, "checked") and book_id_item_id_map.checked:
            return True

        for book_id, book_vals in iteritems(book_id_item_id_map):

            if book_id not in self.seen_book_ids:
                raise InvalidCacheUpdate("Cannot match book_id - cannot preform update as cannot link")

            # Entries will be nullified
            if book_vals is None:
                continue

            if isinstance(book_vals, int):
                if not (book_vals in self.id_map or book_vals in id_map_update):
                    err_str = "Cannot match update id - cannot preform update as cannot link"
                    err_str = default_log.log_variables(
                        err_str,
                        "ERROR",
                        ("book_vals", book_vals),
                        ("book_id_item_id_map", book_id_item_id_map),
                    )
                    raise InvalidCacheUpdate(err_str)
                continue

            # A single, perhaps new entry will be set
            if isinstance(book_vals, basestring):
                continue

            # Check that the new values are not ordered - explicitly this table does not store ordered information
            # and should reject any attempt to feed it such
            if not isinstance(book_vals, (set, frozenset, list)):
                raise InvalidCacheUpdate(
                    "Map needs to be valued with a set, frozenset or list - {}" "".format(type(book_vals))
                )

            # Check that all the ids are valid
            for item_id in book_vals:
                if isinstance(item_id, basestring):
                    continue

                if not (item_id in self.id_map or item_id in id_map_update):
                    err_str = "Cannot match update id - cannot preform update as cannot link"
                    err_str = default_log.log_variables(
                        err_str,
                        "ERROR",
                        ("item_id", item_id),
                        ("book_id_item_id_map", book_id_item_id_map),
                    )
                    raise InvalidCacheUpdate(err_str)

        return True

    def update_cache(
        self, book_id_val_map: dict[SrcTableID, Iterable[DstTableID]], id_map: dict[SrcTableID, T] = None
    ) -> bool:
        """
        Preforms a cache update on the internal maps stored in the database.

        :param book_id_val_map:
        :param id_map:
        :return status: Did the update to the cache go through?
        """
        # book_id_val_map should be keyed with the id of the book in question and valued with the new id of the target
        # - if the target is already linked to the book then promote it
        # - if the target is not linked to the book then link it
        for book_id, book_val in iteritems(book_id_val_map):

            if not isinstance(book_val, set):
                raise InvalidCacheUpdate
            for item_id in book_val:
                if not isinstance(item_id, int):
                    raise InvalidCacheUpdate

            # Item ids currently linked to the book
            old_book_col_ids = deepcopy(self.book_col_map[book_id])

            # Add the item to the front of the linked book ids
            try:
                self.book_col_map[book_id].add(book_val)
            except AttributeError:
                if isinstance(book_val, int):
                    book_col_map_ids = set(self.book_col_map[book_id])
                    book_col_map_ids.add(book_val)
                    self.book_col_map[book_id] = tuple(book_col_map_ids)
                elif isinstance(book_val, set):
                    self.book_col_map[book_id] = book_val
                else:
                    raise NotImplementedError("Cannot parse book_id_val_map")

            # We're adding a value
            if isinstance(book_val, int):
                self.col_book_map[book_val].add(book_id)

            # We're doing a full replace of the values of a book
            elif isinstance(book_val, set):
                # Need to remove reference to the book form all the old item ids
                for old_item_id in old_book_col_ids:
                    self._remove_book_from_item(old_item_id, book_id)

                # And now add the new values back in
                for new_item_id in book_val:
                    self._add_book_to_item(new_item_id, book_id)

            else:

                raise NotImplementedError(f"Cannot parse book_id_val_map - {book_id=} - {book_val=}")

        # id_map will just have the updates applied to it - no need to do more
        if id_map is not None:
            self.id_map.update(id_map)

    def _remove_item_from_book(self, book_id: SrcTableID, item_id: DstTableID) -> None:
        """
        Remove a given item from a book.

        :param book_id:
        :param item_id:
        :return:
        """
        old_item_ids = set(deepcopy(self.book_col_map[book_id]))
        old_item_ids.remove(item_id)
        self.book_col_map[book_id] = tuple(old_item_ids)

    def _add_item_to_book(self, book_id: SrcTableID, item_id: DstTableID) -> None:
        """
        Add a given item to a book.

        :param book_id:
        :param item_id:
        :return:
        """
        old_item_ids = set(deepcopy(self.book_col_map[book_id]))
        old_item_ids.add(item_id)
        self.book_col_map[book_id] = tuple(old_item_ids)

    def _remove_book_from_item(self, item_id: DstTableID, book_id: SrcTableID) -> None:
        """
        Remove a given book from an item.

        :param item_id:
        :param book_id:
        :return:
        """
        old_book_ids = set(deepcopy(self.col_book_map[item_id]))
        old_book_ids.remove(book_id)
        self.col_book_map[item_id] = tuple(old_book_ids)

    def _add_book_to_item(self, item_id: DstTableID, book_id: SrcTableID) -> None:
        """
        Add a given book to an item.

        :param item_id:
        :param book_id:
        :return:
        """
        old_book_ids = set(deepcopy(self.col_book_map[item_id]))
        old_book_ids.add(book_id)
        self.col_book_map[item_id] = tuple(old_book_ids)

    def internal_update_cache(
        self, book_id_item_id_map: dict[SrcTableID, set[DstTableID]], id_map_update: dict[SrcTableID, T]
    ) -> tuple[dict[SrcTableID, Iterable[DstTableID]], set[SrcTableID]]:
        """
        Internal method to actually update the cache.

        :param book_id_item_id_map:
        :param id_map_update: Dictionary used to directly update the id_map
        :return:
        """
        self.id_map.update(id_map_update)

        # Update the book -> col and col -> book maps that form the cache
        deleted = set()
        updated = {}
        for book_id, item_ids in iteritems(book_id_item_id_map):
            old_item_ids = self.book_col_map.get(book_id, None)
            if old_item_ids:
                for old_item_id in old_item_ids:
                    self.col_book_map[old_item_id].discard(book_id)

            if item_ids:
                self.book_col_map[book_id] = item_ids
                for item_id in item_ids:
                    self.col_book_map[item_id].add(book_id)
                updated[book_id] = item_ids
            else:
                self.book_col_map.pop(book_id, None)
                deleted.add(book_id)

        return updated, deleted

    #
    # ------------------------------------------------------------------------------------------------------------------

    def fix_link_table(self, db) -> None:
        """
        Remove everything from the link table not connected to a point - and any unused entries.

        Occurs when a change has been made to the id maps - an item removed there - but which hasn't been propagated to
        the database.
        :param db:
        :return:
        """
        # Build a set of the ids of everything linked to a book - iterates over the dict of sets and saves them all
        linked_item_ids = {item_id for item_ids in itervalues(self.book_col_map) for item_id in item_ids}

        # Identify things which are linked to, but no longer exist
        extra_item_ids = linked_item_ids - set(self.id_map)

        # Make the link table names from the Metadata
        idcol = db.driver_wrapper.get_id_column(self.metadata["table"])
        link_table = db.driver_wrapper.get_link_table_name(table1="titles", table2=self.metadata["table"])
        link_table_item_col = db.driver_wrapper.get_link_column(
            table1="titles", table2=self.metadata["table"], column_type=idcol
        )

        # If extra items exist, they must be removed
        if extra_item_ids:

            # Update the cache to remove the extra ite,s
            for item_id in extra_item_ids:
                book_ids = self.col_book_map.pop(item_id, ())
                for book_id in book_ids:
                    self.book_col_map[book_id] = tuple(
                        iid for iid in self.book_col_map.pop(book_id, ()) if iid not in extra_item_ids
                    )

            db.macros.bulk_delete_in_table(link_table, link_table_item_col, tuple((x,) for x in extra_item_ids))

    def remove_books(self, book_ids: Iterable[SrcTableID], db) -> set[DstTableID]:
        """
        Remove any references to the given book_ids from this table.

        Called after a book has been deleted - or to clear all assets of this type from the book.
        :param book_ids: The book_ids to remove
        :param db: The database to remove them from
        :return clean: The set of ids that are now unused
        """
        # Ids to be removed from the linked to table - because they are no longer in use
        clean = set()

        for book_id in book_ids:
            item_ids = self.book_col_map.pop(book_id, ())
            for item_id in item_ids:
                try:
                    self.col_book_map[item_id].remove(book_id)
                except KeyError:
                    if self.id_map.pop(item_id, null) is not null:
                        clean.add(item_id)
                else:
                    # If the item is now not referred to by any books add it to the set for cleaning
                    if not self.col_book_map[item_id]:
                        del self.col_book_map[item_id]
                        if self.id_map.pop(item_id, null) is not null:
                            clean.add(item_id)

        # Update the table and remove any unused entries - not currently in use
        # Todo: Needs to actually delete the objects
        if clean and self.do_clean_on_remove:
            try:
                db.maintainer.clean(table=self.metadata["table"], item_ids=clean)
            except AttributeError:
                raise AttributeError("type(db): {}".format(type(db)))

        return clean

    def remove_items(
        self, item_ids: set[DstTableID], db, restrict_to_book_ids: Optional[Iterable[SrcTableID]] = None
    ) -> set[SrcTableID]:
        """
        Remove items from the table - updating the database and the cache.

        :param item_ids: Remove the items with the given ids
        :param db: From this database
        :param restrict_to_book_ids: Only remove items from the ids list which are linked to the given books
        :return affected_books: Books whose properties have changed
        """
        affected_books = set()
        item_ids = set(item_ids)
        restrict_to_book_ids = set(restrict_to_book_ids) if restrict_to_book_ids is not None else None

        # Todo: These should already have been set
        # Making the names of the link column to target
        idcol = self.table_id_col
        link_table = self.link_table
        item_link_col = self.link_table_table_id_column

        if restrict_to_book_ids is None:

            # Iterate through the items to remove
            for item_id in item_ids:

                # Check that the item actually has an entry in the id_map - thus is valid to remove
                val = self.id_map.pop(item_id, null)
                if val is null:
                    continue

                # Retrieve the book ids linked to the tag which is being removed
                book_ids = self.col_book_map.pop(item_id, set())

                # Update the book to show that the tag has been removed
                for book_id in book_ids:
                    self.book_col_map[book_id] = tuple(x for x in self.book_col_map.get(book_id, ()) if x != item_id)
                affected_books.update(book_ids)

            item_ids = tuple((x,) for x in item_ids)

            # Remove the links from the book to the item
            db.macros.bulk_delete_in_table(table=link_table, column=item_link_col, column_values=item_ids)

            # Mark the items that where just removed for potential cleaning
            db.maintainer.clean(table=self.metadata["table"], item_ids=item_ids)

            return affected_books

        else:

            items_to_process_normally = set()

            # Check if all books with the item are in the restriction - If so, process them normally
            for item_id in item_ids:

                books_to_process = self.col_book_map.get(item_id, set())
                books_not_to_delete = books_to_process - restrict_to_book_ids

                # Some books are not in the restriction - they must be processed specially
                if books_not_to_delete:
                    books_to_delete = books_to_process & restrict_to_book_ids

                    # Remove only these books from the id maps - update them where needed
                    self.col_book_map[item_id] = books_not_to_delete
                    for book_id in books_to_delete:
                        self.book_col_map[book_id] = tuple(
                            x for x in self.book_col_map.get(book_id, ()) if x != item_id
                        )
                        affected_books |= books_to_delete

                else:

                    items_to_process_normally.add(item_id)

            # Delete book/item pairs from the link table - the main table is untouched, as there might still be books (
            # and other things) linking to it
            book_link_col = db.driver_wrapper.get_link_column(
                table1="titles", table2=self.metadata["table"], column_type="title_id"
            )
            db.macros.bulk_delete_items_in_table_two_matching_cols(
                link_table,
                book_link_col,
                item_link_col,
                [(b, i) for b in affected_books for i in item_ids],
            )

            # Remove any items which can be processed in the normal way - and use this method to deal with them
            if items_to_process_normally:
                affected_books |= self.remove_items(items_to_process_normally, db)
            return affected_books

    def rename_item(self, item_id: DstTableID, new_name: str, db) -> tuple[set[SrcTableID], DstTableID]:
        """
        Rename an item in the table and update the cache.
        If the new name does not match an existing item already in the database then a new item will be created.
        If the new name for the item matches an existing name on the database THAT ITEM WILL CEASE TO EXIST.
        Being replaced with :param item_id:.
        This is so that the item your currently working with doesn't spontaneously cease to exist when you happen
        to rename it to something degenerate.
        :param item_id:
        :param new_name:
        :param db:
        :return:
        """
        # Create a map to try and match the item to existing items - fallback to using icu_lower if nothing better
        # presents itself
        # Todo: Bug seems to be caused by having series display strings set to None - THIS SHOULD NOT HAPPEN
        try:
            rmap = {icu_lower(v): k for k, v in iteritems(self.id_map)}
        except TypeError:
            rmap = dict()
            for k, v in iteritems(self.id_map):
                try:
                    rmap[icu_lower(v)] = k
                except TypeError:
                    pass

        existing_item_id = rmap.get(icu_lower(new_name), None)

        table, column = self.metadata["table"], self.metadata["column"]
        try:
            id_col = db.driver_wrapper.get_id_column(table)
        except (KeyError, InputIntegrityError):
            if table == "authors":
                id_col = "creator_id"
                table = "creators"
                column = "creator"
            else:
                raise

        link_table_name = db.driver_wrapper.get_link_table_name(table1="titles", table2=table)
        link_table_book_col = db.driver_wrapper.get_link_column(table1="titles", table2=table, column_type="title_id")
        link_table_item_col = db.driver_wrapper.get_link_column(table1="titles", table2=table, column_type=id_col)

        affected_book_ids = self.col_book_map.get(item_id, set())
        new_item_id = item_id

        # Just rename - no merge or anything fancy required
        if existing_item_id is None or existing_item_id == item_id:

            self.id_map[item_id] = new_name
            db.macros.update_column_in_table(
                table=table,
                column=column,
                table_id_col=id_col,
                item_id=item_id,
                new_value=new_name,
            )

        # Replacement has to happen - jumping to another entry and marking the old entry for cleanup/auto-merge
        else:

            # Todo: Needs to error out if used on a table with a tree structure

            # All the titles linked to the old item need to be pointed to the new item - while making sure we're not
            # pointing to the same book twice in any one case

            # Remove the old item from the maps - it's no longer needed
            self.id_map.pop(existing_item_id, None)

            old_item_book_ids = self.col_book_map.pop(existing_item_id, set())
            new_item_book_ids = self.col_book_map.get(item_id, set())
            dual_book_ids = old_item_book_ids.intersection(new_item_book_ids)

            # Replacing item_id with existing_item could cause the same id to appear twice in the book list. Handle that
            # by removing existing item from the book list before replacing.
            for book_id in old_item_book_ids:
                self.book_col_map[book_id] = tuple(
                    (item_id if x == existing_item_id else x)
                    for x in self.book_col_map.get(book_id, ())
                    if x != item_id
                )
            self.col_book_map[item_id].update(old_item_book_ids)

            db.macros.bulk_delete_items_in_table_two_matching_cols(
                table=link_table_name,
                col_1=link_table_book_col,
                col_2=link_table_item_col,
                column_values=((dbi, item_id) for dbi in dual_book_ids),
            )

            # Update the link table to point to the replaced item and delete the old item
            # db.macros.update_column_in_table(
            #     table=link_table_name, column=link_table_item_col, table_id_col=link_table_item_col,
            #     item_id=existing_item_id, new_value=item_id)

            # Pass the item id to the database for potential cleaning
            db.maintenance.merge(
                table=self.metadata["table"],
                item_1_id=item_id,
                item_2_id=existing_item_id,
            )

            # Need to, potentially, preform a rename on the existing object
            row = db.get_row_from_id(table, row_id=item_id)
            row[column] = new_name
            row.sync()

        return affected_book_ids, item_id

    def fix_case_duplicates(self, db) -> None:
        """
        Merge any entries which are the same up to case.

        Left in place, and not replaced with a function depending on the quantities to compare, because if entries
        differ only by their case then they probably should be merged anyway.
        :param db:
        :return:
        """
        # Build a case map - keyed with the lower case of the item and valued with all the items which correspond to
        # that item
        case_map = defaultdict(set)
        for item_id, val in iteritems(self.id_map):
            try:
                case_map[icu_lower(val)].add(item_id)
            except TypeError:
                # Probably just tried to convert a val that was not a string - ignore and move on
                pass

        # Building the names of the statements needed for the SQL
        idcol = db.driver_wrapper.get_id_column(self.metadata["table"])
        link_table = db.driver_wrapper.get_link_table_name(table1="titles", table2=self.metadata["table"])
        link_table_item_col = db.driver_wrapper.get_link_column(
            table1="titles", table2=self.metadata["table"], column_type=idcol
        )
        link_table_book_col = db.driver_wrapper.get_link_column(
            table1="titles", table2=self.metadata["table"], column_type="title_id"
        )

        # Working through applying the merges
        for v in itervalues(case_map):

            if len(v) > 1:

                # All books will end up linked to the chosen main_id
                done_books = set()
                main_id = min(v)
                v.discard(main_id)

                for item_id in v:
                    self.id_map.pop(item_id, None)
                    books = self.col_book_map.pop(item_id, set())

                    for book_id in books:
                        if book_id in done_books:
                            continue
                        done_books.add(book_id)
                        orig = self.book_col_map.get(book_id, ())
                        if not orig:
                            continue

                        vals = uniq(tuple(main_id if x in v else x for x in orig))
                        self.book_col_map[book_id] = vals

                        if len(orig) == len(vals):
                            db.macros.bulk_update_link_table(
                                link_table=link_table,
                                update_column=link_table_item_col,
                                other_column=link_table_book_col,
                                values=tuple((main_id, x, book_id) for x in v),
                            )

                        else:
                            # duplicates - if there are duplicate links fix that by removing then re-adding the link
                            # Remove first to not conflict with the UNIQ condition that should be (and will be) on
                            # these tables
                            db.macros.delete_in_table(
                                table=link_table,
                                column=link_table_book_col,
                                value=book_id,
                            )

                            db.macros.bulk_add_links(
                                link_table,
                                link_table_book_col,
                                link_table_item_col,
                                values=[(book_id, x) for x in vals],
                            )

                    # Tag for updating all the other link tables
                    db.maintenance.merge(
                        table=self.metadata["table"],
                        item_1_id=main_id,
                        item_2_id=item_id,
                    )
