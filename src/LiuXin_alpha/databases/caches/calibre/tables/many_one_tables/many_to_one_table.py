"""
ManyToOne tables are for instances where many titles could be linked to one - and only one - object.

These are slightly rare in LiuXin - mostly it's ManyToMany.
However it's still used for calibre ratings and covers.
"""


import uuid
from collections import defaultdict

from typing import Any, Union, TypeVar, Optional, Iterable

from LiuXin.exceptions import InvalidCacheUpdate

from LiuXin.customize.cache.base_tables import BaseManyToOneTable, MANY_ONE

from LiuXin.databases.caches.calibre.tables.base import null, MultiTableMixin
from LiuXin.databases.db_types import (
    SrcTableID,
    DstTableID,
    MainTableName,
    MetadataDict,
    InterLinkTableName,
    TableTypes,
)

from LiuXin.library.standardization import (
    standardize_publisher,
    standardize_series,
    standardize_tag,
)

from LiuXin.utils.icu import lower as icu_lower
from LiuXin.utils.lx_libraries.liuxin_six import iteritems
from LiuXin.utils.lx_libraries.liuxin_six import six_unicode
from LiuXin.utils.general_ops.language_tools import plural_singular_mapper

from past.builtins import basestring

T = TypeVar("T")


class CalibreManyToOneTable(BaseManyToOneTable[T], MultiTableMixin):
    """
    Represents data where one data item can map to many books, for example: ratings.

    Each book can be assumed to have only one value for data of this type - an unambiguous result is required.
    """

    _table_type: TableTypes = MANY_ONE

    # Characterize the table
    _priority: bool = False
    _typed: bool = False

    def __init__(
        self, name: str, metadata: MetadataDict, link_table: Optional[InterLinkTableName] = None, custom: bool = False
    ) -> None:
        """
        Startup a ManyToOneTable - includes the link_table and if the Table is custom.

        (which may affect how the table behaves in some circumstances).
        :param name: The name of the table
        :param metadata: The metadata associated with the table - how it should display and other properties
        :param link_table: The table linking this table to either the books or titles field
        :param custom: Is this table a custom table
        """
        super(BaseManyToOneTable, self).__init__(name=name, metadata=metadata, link_table=link_table, custom=custom)

        # Todo: This should be called all_book_ids or something like it - confusing with seen_books
        self.seen_book_ids = set()
        self.seen_item_ids = set()

        # MAPS
        # - Keyed with the item id and valued with the item value
        self.id_map = self._id_map_factory()
        # - Keyed with the book id and valued with the value for that book from the other table
        self.book_col_map = self._book_col_map_factory()
        # - Keyed with the book id and valued with a set of the ids linked to the book
        self.col_book_map = self._col_book_map_factory()

    @property
    def table_type(self) -> bool:
        """
        Is this a table which respects the table_type of links or not?

        :return:
        """
        return self._table_type

    @table_type.setter
    def table_type(self, value: Any) -> None:
        """
        You CANNOT change this variable - it's a function of the class you chose for this link.

        :return:
        """
        raise ValueError(f"You CANNOT set the table_type property directly. Use a different class.")

    @property
    def priority(self) -> bool:
        """
        Is this a table which respects the priority of links or not?

        :return:
        """
        return self._priority

    @priority.setter
    def priority(self, value: Any) -> None:
        """
        You CANNOT change this variable - it's a function of the class you chose for this link.

        :return:
        """
        raise ValueError(f"You CANNOT set the priority property directly. Use a different class.")

    @property
    def typed(self) -> bool:
        """
        Does this table had the concept of the type of the links or not?

        :return:
        """
        return self._typed

    @typed.setter
    def typed(self, value: Any) -> None:
        """
        You CANNOT change this variable - it's a function of the class you chose for this link.

        :return:
        """
        raise ValueError(f"You CANNOT set the typed property directly. Use a different class.")

    @classmethod
    def from_typed_table(
        cls, original_table: Union["CalibreTypedManyToOneTable", "CalibrePriorityTypedManyToOneTable"], type_filter: str
    ) -> "CalibreManyToOneTable":
        """
        Build a ManyToManyTable from existing maps.

        :param original_table: The original table to build this table from
        :param type_filter: Create the new table restricted to these types from the old
        :return:
        """
        assert isinstance(original_table, CalibreManyToOneTable)
        assert original_table.typed, f"{original_table=} must be typed to use this method."

        sub_table = cls(
            name=original_table.name,
            metadata=original_table.metadata,
            link_table=original_table.link_table,
        )
        sub_table.is_subtable = True

        # Todo: Need to filter for the link types - so we need to store them
        sub_table.id_map = original_table.id_map
        sub_table.book_col_map = original_table.book_col_map
        sub_table.col_book_map = original_table.col_book_map[type_filter]

        return sub_table

    @staticmethod
    def _id_map_factory() -> dict[SrcTableID, T]:
        """
        Returns the container to be used to hold the id maps.

        :return:
        """
        return dict()

    @staticmethod
    def _book_col_map_factory() -> dict[SrcTableID, Optional[DstTableID]]:
        """
        Used to store the book-col relations.

        The book-item relation is one-to-one - so this will be a dict keyed with the id of the book and valued with the
        id of the other side.
        :return:
        """
        return dict()

    def _col_book_map_factory(self) -> dict[DstTableID, set[SrcTableID]]:
        """
        Used to store the col-book relation.

        The item-book relation is one-many - one item can be linked to many books.
        :return:
        """
        return defaultdict(set)

    def read(self, db) -> None:
        """
        Load the table with data from the database.

        :param db:
        :return:
        """
        # Record the specific link table that was used to read this data
        self.set_link_table(db)

        # Clearing the cache before the read
        self._clear_internal_caches()

        # Reading off the database
        self.read_id_maps(db)
        self.read_maps(db)

    def _clear_internal_caches(self) -> None:
        """
        Clear the internal caches - restoring everything to its default value.

        :return:
        """
        self.id_map = self._id_map_factory()
        self.col_book_map = self._col_book_map_factory()
        self.book_col_map = self._book_col_map_factory()

    def update_preflight(
        self,
        book_id_item_id_map: dict[SrcTableID, set[DstTableID]],
        id_map_update: Optional[dict[DstTableID, T]] = None,
        dirtied: Optional[set[SrcTableID]] = None,
    ) -> tuple[dict[SrcTableID, set[DstTableID]], Optional[dict[DstTableID, T]]]:
        """
        Bring the update into a form where it can be more easily written out to the database.

        :param book_id_item_id_map:
        :param id_map_update:
        :return:
        """
        return book_id_item_id_map, id_map_update

    # Todo: This definitely needs bulking up - adding some additional checks
    def update_precheck(
        self,
        book_id_item_id_map: dict[SrcTableID, Optional[Union[str, DstTableID]]],
        id_map_update: dict[DstTableID, T],
    ) -> None:
        """
        Check that the given update is of a correct form to be applied to the cache and the database.

        :param book_id_item_id_map:
        :param id_map_update:
        """
        for book_id, book_val in iteritems(book_id_item_id_map):

            if book_val is None:
                continue

            if not isinstance(book_val, (int, basestring)):
                raise InvalidCacheUpdate("update_dict malformed")

            if isinstance(book_val, int):
                if book_val not in self.seen_item_ids:
                    raise InvalidCacheUpdate("update_dict malformed")

    def read_id_maps(self, db) -> None:
        """
        Read the entire table and create a dictionary keyed with the id and valued with the relevant column value.

        Loads any elements from the table connected to the book.
        :param db:
        :return None: Changes are purely internal so there is no return.
        """
        # Read the values from the id table
        if not self.custom:
            id_col = db.driver_wrapper.get_id_column(self.metadata["table"])
            stmt = "SELECT {0},{1} FROM {2} WHERE {0} > 0;".format(
                id_col, self.metadata["column"], self.metadata["table"]
            )
            query = db.driver_wrapper.execute(stmt)

        else:
            cc_table = self.metadata["table"]
            cc_col = plural_singular_mapper(cc_table)
            cc_col_target = self.metadata["column"]

            query = db.driver_wrapper.execute(
                "SELECT {cc_col}_id, {cc_col}_{cc_col_target} FROM {cc_table}"
                "".format(cc_col=cc_col, cc_col_target=cc_col_target, cc_table=cc_table)
            )

        if self.unserialize is None:
            self.id_map = dict(query)
        else:
            us = self.unserialize
            self.id_map = {book_id: us(val) for book_id, val in query}

        # Read the ids from the books table - so that we know what ids are valid for the table
        # Todo: Need to generalize to other things linked to
        title_query = db.driver_wrapper.execute("SELECT title_id FROM titles")
        self.seen_book_ids = set([tr[0] for tr in title_query])

        self.seen_item_ids = set(self.id_map.keys())

    def read_maps(self, db, type_filter: Optional[str] = None) -> None:
        """
        Read the map between the book and the link table - use this to build the column.

        :param db:
        :param type_filter:
        :return:
        """
        self.set_link_table(db)

        if not self.custom:

            # Read off the link table
            if type_filter is None:
                return self.read_maps_typed_and_or_priority(db)
            else:
                return self.read_maps_type_filter(db, type_filter)
        else:

            return self.read_maps_custom(db)

    def read_maps_type_filter(self, db, type_filter: str) -> None:
        """
        Read maps with a filter to only register elements of a certain type.

        :return:
        """
        link_table_book_id = self.link_table_bt_id_column
        link_table_other_id = self.link_table_table_id_column

        link_type_col = db.driver_wrapper.get_link_column(
            table1="titles", table2=self.metadata["table"], column_type="type"
        )
        stmt = 'SELECT {0}, {1} FROM {2} WHERE {3} = "{4}";'.format(
            link_table_book_id,
            link_table_other_id,
            self.link_table,
            link_type_col,
            six_unicode(type_filter),
        )

        col_book_map = self.col_book_map
        book_col_map = self.book_col_map
        for book_id, item_id in db.driver_wrapper.execute(stmt):
            col_book_map[item_id].add(book_id)
            book_col_map[book_id] = item_id

    # Todo: This needs to be renamed - for it is a lie - also probably moved to the table it should be used in
    def read_maps_typed_and_or_priority(self, db) -> None:
        """
        Preform a read in the case that there is a type column in the link table.

        :param db:
        :return:
        """
        link_table_book_id = self.link_table_bt_id_column
        link_table_other_id = self.link_table_table_id_column

        if self.link_table_priority_col is None:
            stmt = "SELECT {0}, {1} FROM {2};".format(link_table_book_id, link_table_other_id, self.link_table)
        else:
            stmt = self.selectq.format(
                link_table_book_id,
                link_table_other_id,
                self.link_table,
                self.link_table_priority_col,
            )

        col_book_map = self.col_book_map
        book_col_map = self.book_col_map
        for book_id, item_id in db.driver_wrapper.execute(stmt):
            col_book_map[item_id].add(book_id)
            book_col_map[book_id] = item_id

        for book_id in self.seen_book_ids:
            if book_id not in self.book_col_map:
                self.book_col_map[book_id] = None

        for item_id in self.seen_item_ids:
            if item_id not in self.col_book_map:
                self.col_book_map[item_id] = set()

    def read_maps_custom(self, db) -> None:
        """
        Preform a read from a custom table.

        :param db:
        :return:
        """
        try:
            self_lt = self.link_table
        except AttributeError:
            self_lt = None

        lt = self_lt if self_lt is not None else "books_{}_link".format(self.metadata["table"])
        lt_col = plural_singular_mapper(lt)
        target_col = self.metadata["link_column"]

        stmt = "SELECT {lt_col}_book, {lt_col}_{target_col} FROM {lt}" "".format(
            lt=lt, lt_col=lt_col, target_col=target_col
        )

        col_book_map = self.col_book_map
        book_col_map = self.book_col_map
        for book_id, item_id in db.driver_wrapper.execute(stmt):
            col_book_map[item_id].add(book_id)
            book_col_map[book_id] = item_id

    def remove_books(self, book_ids: Iterable[SrcTableID], db) -> set[DstTableID]:
        """
        Unlink the given books from this table - updating the cache and changing the database.

        Originally removed any now-unused items - not currently implemented due to problems clearing items which might
        be referenced in multiple different tables.
        :param book_ids:
        :param db:
        :return:
        """
        clean = set()
        for book_id in book_ids:

            # Remove the book_id from the cache - keep the id of the item linked to it
            item_id = self.book_col_map.pop(book_id, None)

            # Drop the item from the relevant col_book_map set
            if item_id is not None:
                try:
                    self.col_book_map[item_id].discard(book_id)
                except KeyError:
                    # If the item is no longer in the col_book_map, it is linked to no books
                    if self.id_map.pop(item_id, null) is not null:
                        clean.add(item_id)
                else:
                    # If the item is not being used add it to the cleaning set
                    if not self.col_book_map[item_id]:
                        del self.col_book_map[item_id]
                        if self.id_map.pop(item_id, null) is not null:
                            clean.add(item_id)

        # Delete unused or unwanted entries from the database - currently does nothing
        if clean:
            db.clean(table=self.metadata["table"], item_ids=clean)
        return clean

    def remove_items(self, item_ids: Iterable[DstTableID], db) -> set[SrcTableID]:
        """
        Remove items from the table, updating the cache and then the link row.

        :param item_ids:
        :param db:
        :return affected_books: Books that where affected by removing the items
        """
        link_table = self.link_table
        link_table_item_col = self.link_table_table_id_column

        # Build a set of the affected books
        affected_books = set()
        for item_id in item_ids:
            # Remove from the id_map - if the item isn't linked to anything then continue
            val = self.id_map.pop(item_id, null)
            if val is null:
                continue

            # Remove the item from the col_book_map - retain the ids of all the books it's linked to - use these to
            # update the other caches
            book_ids = self.col_book_map.pop(item_id, set())
            for book_id in book_ids:
                self.book_col_map.pop(book_id, None)
            affected_books.update(book_ids)

        # Scrub the links from the link table then scrub the unused elements from the target table.
        item_ids = tuple((x,) for x in item_ids)
        db.driver_wrapper.executemany(
            "DELETE FROM {0} WHERE {1} = ?;".format(link_table, link_table_item_col),
            item_ids,
        )
        db.maintainer.clean(table=self.metadata["table"], item_ids=item_ids)

        return affected_books

    # 1) For tags we don't care about subtle degeneracy - so using the unmodified calibre function
    # 2) For everything else, tag the items for potential merge and send it over to the database maintainer
    def rename_item(self, item_id: DstTableID, new_name: str, db) -> tuple[set[SrcTableID], DstTableID]:
        """
        Change the column value for the item_id to the value given by new_name

        :param item_id: The item to update
        :param new_name: The value to change the column to
        :param db: The database to preform the change in
        :return:
        """
        table, column = self.metadata["table"], self.metadata["column"]
        affected_books = self.col_book_map.get(item_id, set())

        # Using the appropriate function to build an existing values dictionary
        if table == "publishers" and column == "publisher":
            rmap = {standardize_publisher(v): k for k, v in iteritems(self.id_map)}
        elif table == "series" and column == "series":
            rmap = {standardize_series(v): k for k, v in iteritems(self.id_map)}
        else:
            rmap = {icu_lower(v): k for k, v in iteritems(self.id_map)}

        existing_item = rmap.get(standardize_tag(new_name), None)

        # If possible, rename - if the new name clashes with an existing name, jump to that instead

        # A simple rename will be enough
        if existing_item is None or existing_item == item_id:
            self.id_map[item_id] = new_name
            id_col = db.driver_wrapper.get_id_column(table)
            db.driver_wrapper.execute(
                "UPDATE {0} SET {1}=? WHERE {2}=?".format(table, column, id_col),
                (new_name, item_id),
            )
            new_id = item_id

        # We have to replace - unlike calibre replacement is not followed by a deletion of the old entry - we might
        # still need it
        else:
            new_id = existing_item
            self.id_map.pop(item_id, None)
            books = self.col_book_map.pop(item_id, set())
            for book_id in books:
                self.book_col_map[book_id] = existing_item
            self.col_book_map[existing_item].update(books)

            idcol = db.driver_wrapper.get_id_column(self.metadata["table"])
            link_table = db.driver_wrapper.get_link_table_name(table1="titles", table2=self.metadata["table"])
            link_col = db.driver_wrapper.get_link_column(
                table1="titles", table2=self.metadata["table"], column_type=idcol
            )

            # For custom series this means that the series index can potentially have duplicates/be incorrect, but
            # there is no way to handle that in this context.
            db.driver_wrapper.execue(
                "UPDATE {0} SET {1}=? WHERE {1}=?;".format(link_table, link_col),
                (existing_item, item_id),
            )

        return affected_books, new_id

    def update_db(
        self,
        book_id_to_val_map: dict[SrcTableID, Optional[Union[DstTableID, str]]],
        db,
        allow_case_change: bool = False,
    ) -> bool:
        """
        Write data out to the database.

        :param book_id_to_val_map:
        :param db:
        :param allow_case_change:
        :return:
        """
        return super(CalibreManyToOneTable, self).update_db(
            book_id_to_val_map=book_id_to_val_map,
            db=db,
            allow_case_change=allow_case_change,
        )

    def update_cache(
        self, book_id_val_map: dict[SrcTableID, Optional[DstTableID]], id_map: Optional[dict[DstTableID, T]] = None
    ) -> None:
        """
        Update the object maps stored in the cache.

        :param book_id_val_map:
        :return:
        """
        # Sometimes called - means this function is being used as a dummy and the actual update (hopefully) happened
        # elsewhere in the code
        if book_id_val_map is None and id_map is None:
            return

        id_map = id_map if id_map is not None else dict()

        for book_id, book_val in iteritems(book_id_val_map):

            if book_id not in self.seen_book_ids:
                raise InvalidCacheUpdate("Cannot find book_id - cannot preform update")

            try:
                current_vals = self.book_col_map[book_id]
            except KeyError:
                continue

            if isinstance(book_val, int):
                if isinstance(current_vals, set):
                    current_vals.add(book_val)

                elif isinstance(current_vals, tuple):
                    self.book_col_map[book_id] = tuple(
                        [
                            book_val,
                        ]
                        + list(current_vals)
                    )

                elif isinstance(current_vals, int):
                    self.book_col_map[book_id] = book_val

                else:
                    raise NotImplementedError("book_val has unexpected form")

            # Todo: This is not great - should never be getting to here with a string value
            # Todo: Take this out and replace it with an error - soon as
            elif isinstance(book_val, basestring):

                unique_code = str(uuid.uuid4())
                if isinstance(current_vals, set):
                    current_vals.add(unique_code)

                elif isinstance(current_vals, tuple):
                    self.book_col_map[book_id] = tuple(
                        [
                            unique_code,
                        ]
                        + list(current_vals)
                    )

                else:
                    raise NotImplementedError("book_val has unexpected form")

                id_map[unique_code] = book_val

            else:
                raise NotImplementedError("Unexpected form for update - {}".format(book_val))

        if id_map is not None:
            self.id_map.update(id_map)

    def internal_update_cache(
        self, book_id_item_id_map: dict[SrcTableID, Optional[DstTableID]], id_map_update: dict[DstTableID, T]
    ) -> tuple[dict[SrcTableID, Optional[T]], set[SrcTableID]]:
        """
        Update cache with some additional information provided.

        Used in write when it needs to know some info about the cache before writing out to the database.
        :param book_id_item_id_map:
        :param id_map_update: Dictionary used to directly update the id_map
        :return:
        """
        self.id_map.update(id_map_update)

        # Update the book -> col and col -> book maps
        deleted = set()
        updated = {}

        for book_id, link_val in iteritems(book_id_item_id_map):

            old_item_id = self.book_col_map.get(book_id, None)

            # We are nullifying - remove the link and continie
            if link_val is None:
                self.book_col_map[book_id] = None
                if old_item_id is not None:
                    self.col_book_map[old_item_id].discard(book_id)

                deleted.add(book_id)
                continue

            # We are updating - break the old link and write a new one back out
            self.book_col_map[book_id] = link_val

            if old_item_id is not None:
                self.col_book_map[old_item_id].discard(book_id)
            self.col_book_map[link_val].add(book_id)

            updated[book_id] = link_val

        return updated, deleted
