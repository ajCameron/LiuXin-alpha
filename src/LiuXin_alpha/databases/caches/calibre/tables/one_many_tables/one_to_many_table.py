"""
Basic CalibreOneToMany table - one "book" is mapped to many items.
"""

import pprint

from collections import defaultdict
from copy import deepcopy

from typing import TypeVar, Optional, Any, Union

from LiuXin.customize.cache.base_tables import ONE_MANY

from LiuXin.databases.caches.calibre.tables.base import CalibreBaseTable, MultiTableMixin
from LiuXin.databases.db_types import TableTypes, MetadataDict, SrcTableID, DstTableID, InterLinkTableName

from LiuXin.exceptions import InvalidCacheUpdate

from LiuXin.utils.lx_libraries.liuxin_six import iteritems

from past.builtins import basestring


T = TypeVar("T")


# ----------------------------------------------------------------------------------------------------------------------
#
# - ONE TO MANY TABLES


# Todo: Need to be able to deal with being fed a custom column
from LiuXin.utils.lx_libraries.liuxin_six import dict_iteritems as iteritems


class CalibreOneToManyTable(CalibreBaseTable[T], MultiTableMixin):
    """
    For the case where one, and only one, book is linked to many items and the items are linked to no other books.

    E.g. "comments" - one book can be linked to many of them, but comments are never linked to more than one book
    """

    _table_type: TableTypes = ONE_MANY

    # Characterize the class
    _priority: bool = False
    _typed: bool = False

    def __init__(
        self, name: str, metadata: MetadataDict, link_table: InterLinkTableName = None, custom: bool = False
    ) -> None:
        """
        Preform startup for a vanilla OneToMany table.

        No data is loaded at this point - that's done in the read method.
        :param name:
        :param metadata:
        :param link_table:
        :param custom:
        """
        super(CalibreOneToManyTable, self).__init__(name=name, metadata=metadata, link_table=link_table, custom=custom)

        self.seen_book_ids: set[SrcTableID] = set()
        self.seen_item_ids: set[DstTableID] = set()

        try:
            self.val_unique = self.metadata["val_unique"]
        except KeyError:
            self.val_unique = True

        # Todo: Manually change the names of these and see what breaks - eventually should never be directly accessed
        # MAPS
        self.id_map = self._id_map_factory()
        # Keyed with the item id and valued with it's value from the other table
        # Unchanged for the rest of the table variants

        # Todo: Rename these for each of the sub tables - for typing reasons

        self.book_col_map = self._book_col_map_factory()
        # Keyed with the book id and valued with the result from the other table - which should be a set (as there are
        # many unordered items for each book in the other table)

        self.col_book_map = self._col_book_map_factory()
        # Keyed with the item id and valued with the result from the books table - which should be a single item
        # (as there is only one book linked to each item)

    def _clear_internal_caches(self) -> None:
        """
        Clear any data stored in this table.

        :return:
        """
        self.id_map = self._id_map_factory()
        self.book_col_map = self._book_col_map_factory()
        self.col_book_map = self._col_book_map_factory()

    @staticmethod
    def _id_map_factory() -> dict[DstTableID, Optional[T]]:
        """
        Returns the container to be used to hold the id maps

        Keyed with the item id and valued with its value.
        :return:
        """
        return dict()

    def _book_col_map_factory(self) -> dict[SrcTableID, set[DstTableID]]:
        """
        Returns an empty book col map - ready for data load.

        This is a OneToMany table - so the maps will be keyed with the book_id and valued with a set of item ids.
        (A set because the table is not priority - so there is no concept of order - which would call for a list).
        :return:
        """
        return defaultdict(set)

    @staticmethod
    def _col_book_map_factory() -> dict[DstTableID, Optional[SrcTableID]]:
        """
        Returns an empty col book map - ready for data load.

        This is a OneToMany table - so it'll be a dictionary keyed with the item id and - optionally - valued with a
        book.
        No type data needs to be stored, and priority could not be expressed in this format anyways.
        :return:
        """
        return dict()

    # Todo: Rejig custom columns to behave sanely
    # Todo: These should probably be internal - only ever should be called internally by the read method
    # Todo: Merge with the other read id maps from elsewhere in the tables
    def read_id_maps(self, db) -> None:
        """
        Read the entire table and create a dictionary keyed with the id and valued with the relevant column value.

        Loads any elements from the table connected to the book.
        :param db:
        :return None: All changes are made internally
        """
        # Todo: Should be able to just remove this if and still have everything work
        if not self.custom:
            id_col = db.driver_wrapper.get_id_column(self.metadata["table"])
            stmt = "SELECT {0},{1} FROM {2} WHERE {0} > 0;".format(
                id_col, self.metadata["column"], self.metadata["table"]
            )
            query = db.driver_wrapper.execute(stmt)
        else:
            query = db.execute("SELECT id, {0} FROM {1}".format(self.metadata["column"], self.metadata["table"]))

        if self.unserialize is None:
            self.id_map = dict(query)
        else:
            us = self.unserialize
            self.id_map = {book_id: us(val) for book_id, val in query}

        # Read the ids from the books table - so that we know what ids are valid for the table
        title_query = db.driver_wrapper.execute("SELECT title_id FROM titles")
        self.seen_book_ids = set([tr[0] for tr in title_query])

        self.seen_item_ids = set(self.id_map.keys())

    def read_maps(self, db) -> None:
        """
        Preform a read from the database into the table.

        :param db:
        :return None: All changes are made internally
        """
        stmt = "SELECT {}, {} FROM {};".format(
            self.link_table_bt_id_column,
            self.link_table_table_id_column,
            self.link_table,
        )

        for bt_id, other_id in db.driver_wrapper.execute(stmt):
            self.book_col_map[bt_id].add(other_id)
            self.col_book_map[other_id] = bt_id

        for book_id in self.seen_book_ids:
            if book_id not in self.book_col_map:
                self.book_col_map[book_id] = set()

        for item_id in self.id_map.keys():
            if item_id not in self.col_book_map:
                self.col_book_map[item_id] = None

    def read(self, db) -> None:
        """
        Load the table with data from the database.

        Actually does the job of manning the data into the cache read for manipulation.
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

    def update_precheck(
        self, book_id_item_id_map: dict[SrcTableID, Optional[DstTableID]], id_map_update: dict[DstTableID, Optional[T]]
    ) -> None:
        """
        Check that an update is of a valid form before writing it out to the cache and the database.

        Called when you know the ids you want to assign to the book after the update. Checks those ids are valid.
        No changes will be made to the :param book_id_item_id_map: (e.g. if the map is valued with tuples - not lists
        as expected - this will not be corrected.
        Raised InvalidCacheUpdate if the cache update is invalid in some way.

        :param book_id_item_id_map: Keyed with the ids of the books to update and valued with the
        :param id_map_update:
        :return None: Just throws an error if something isn't right
        """
        seen_ids = set()
        for book_id, book_vals in iteritems(book_id_item_id_map):

            if book_vals is None:
                continue

            if not isinstance(book_vals, set):
                raise InvalidCacheUpdate("Update must be valued with a set")

            # Strings can overlap - but ids cannot - so filter out the strings
            new_book_vals = set([bv for bv in book_vals if not isinstance(bv, basestring)])

            # Check to ensure that all the item entries for a specific book are different - this is supposed to be a
            # ONE to MANY field, after all
            seen_ids_after = seen_ids.union(set(new_book_vals))
            if len(seen_ids_after) - len(seen_ids) != len(set(new_book_vals)):
                raise InvalidCacheUpdate("overlap between book values - {}".format(book_id_item_id_map))
            seen_ids = seen_ids_after

    def update_precheck_unique(
        self, book_id_item_id_map: dict[SrcTableID, Optional[DstTableID]], id_map_update: dict[DstTableID, Optional[T]]
    ) -> None:
        """
        Check that an update is of a valid form before writing it out to the cache and the database.

        Called when you know the ids you want to assign to the book after the update. Checks those ids are valid.
        No changes will be made to the :param book_id_item_id_map: (e.g. if the map is valued with tuples - not lists
        as expected - this will not be corrected.
        Raised InvalidCacheUpdate if the cache update is invalid in some way.
        :param book_id_item_id_map: Keyed with the ids of the books to update and valued with the
        :param id_map_update:
        :return:
        """
        seen_ids = set()
        seen_strs = set()
        for book_id, book_vals in iteritems(book_id_item_id_map):

            if book_vals is None:
                continue

            if isinstance(book_vals, (int, basestring)):
                continue

            if not isinstance(book_vals, set):
                raise InvalidCacheUpdate(self._malformed_update_error(book_id_item_id_map, book_id, book_vals))

            # Strings cannot overlap - neither can ids so filter out the strings
            # (at the point where this is called an attempt should have already been made to match the strs)
            new_book_id_vals = set([bv for bv in book_vals if not isinstance(bv, basestring)])

            # Check to ensure that all the item entries for a specific book are different - this is a unique one to
            # many field after all
            seen_ids_after = seen_ids.union(set(new_book_id_vals))
            if len(seen_ids_after) - len(seen_ids) != len(set(new_book_id_vals)):
                raise InvalidCacheUpdate("overlap between book values on ids- {}".format(book_id_item_id_map))
            seen_ids = seen_ids_after

            # Strings cannot overlap - neither can ids so filter out the strings
            # (at the point where this is called an attempt should have already been made to match the strs)
            new_book_str_vals = set([bv for bv in book_vals if isinstance(bv, basestring)])

            # Check to ensure that all the item entries for a specific book are different - this is a unique one to
            # many field after all
            seen_strs_after = seen_strs.union(set(new_book_str_vals))
            if len(seen_strs_after) - len(seen_strs) != len(set(new_book_str_vals)):
                raise InvalidCacheUpdate("overlap between book values on strs - {}".format(book_id_item_id_map))
            seen_strs = seen_strs_after

    def _malformed_update_error(self, book_id_item_id_map: Any, book_id: Any, book_vals: Any) -> str:
        """
        Err msg - raised when there' something wring with the update before it's written out.
        """
        err_msg = [
            "If this point is reached update must be valued with a set",
            "book_id_item_id_map: \n{}\n".format(pprint.pformat(book_id_item_id_map)),
            "book_id: {}".format(book_id),
            "book_vals: {}".format(book_vals),
        ]
        return "\n".join(err_msg)

    def update_preflight(
        self,
        book_id_item_id_map: dict[SrcTableID, Optional[set[Union[DstTableID, str]]]],
        id_map_update: dict[DstTableID, Optional[T]] = None,
        dirtied: Optional[set[SrcTableID]] = None,
    ) -> tuple[dict[SrcTableID, set[DstTableID]], set[SrcTableID]]:
        """
        Bring the update into a form where it can be more easily written out to the database.

        :param book_id_item_id_map:
        :param id_map_update:
        :return:
        """
        id_map_update = dict() if id_map_update is None else id_map_update
        clean_book_id_item_id_map = defaultdict(set)

        for book_id, book_vals in iteritems(book_id_item_id_map):
            if book_vals is None:
                clean_book_id_item_id_map[book_id] = set()
                continue

            # Unmatched string - just add it into the set
            if isinstance(book_vals, (basestring, int)):
                clean_book_id_item_id_map[book_id] = {
                    book_vals,
                }.union(self.book_col_map[book_id])
            elif isinstance(book_vals, (set,)):
                clean_book_id_item_id_map[book_id] = set(book_vals)
            else:
                raise InvalidCacheUpdate("book_vals invalid")

        return clean_book_id_item_id_map, id_map_update

    def update_preflight_unique(
        self,
        book_id_item_id_map: dict[SrcTableID, Optional[set[Union[DstTableID, str]]]],
        id_map_update: dict[DstTableID, Optional[T]] = None,
        dirtied: Optional[set[SrcTableID]] = None,
    ) -> tuple[dict[SrcTableID, set[DstTableID]], set[SrcTableID]]:
        """
        Bring the update into a form where it can be more easily written out to the database.

        :param book_id_item_id_map:
        :param id_map_update:
        :return:
        """
        id_map_update = dict() if id_map_update is None else id_map_update
        clean_book_id_item_id_map = defaultdict(set)

        for book_id, book_vals in iteritems(book_id_item_id_map):
            if book_vals is None:
                clean_book_id_item_id_map[book_id] = set()
                continue

            # Unmatched string - just add it into the set
            if isinstance(book_vals, (basestring, int)):
                clean_book_id_item_id_map[book_id] = {
                    self._match_item_val(book_vals),
                }.union(self.book_col_map[book_id])

            elif isinstance(book_vals, (set,)):
                # Process the set - if any of the strings in it can be matched to an existing id, then they should be
                # If not, then they'll be created later
                clean_id_set = set()
                for cand_item_id in book_vals:
                    if isinstance(cand_item_id, int):
                        clean_id_set.add(cand_item_id)
                    elif isinstance(cand_item_id, basestring):
                        # Todo: This is a heinous hack - which will also be quite slow
                        clean_id_set.add(self._match_item_val(cand_item_id))
                    else:
                        raise NotImplementedError

                clean_book_id_item_id_map[book_id] = clean_id_set

            else:
                raise InvalidCacheUpdate("book_vals invalid - expected set, basestring or int")

        # Check the final product of the matching is valid
        self.update_precheck_unique(clean_book_id_item_id_map, id_map_update)

        return clean_book_id_item_id_map, id_map_update

    # Todo: This is a bad way to handle this problem, it's gotta be said
    def _match_item_val(self, item_str: T) -> Union[DstTableID, T]:
        """
        Attempt to match a string, which might be the value of an item, to it's corresponding item.

        If the value corresponds to an item, return its id.
        If not, just return the original object it was called with.

        :param item_str: Object to try matching via a search of the id_map table.
        :return matched_id/original_object:
        """
        for true_item_id in self.id_map:
            if self.id_map[true_item_id] == item_str:
                return true_item_id
        return item_str

    def update_cache(
        self,
        book_id_val_map: dict[SrcTableID, Optional[set[Union[DstTableID, int]]]],
        id_map: dict[DstTableID, Optional[T]] = None,
    ):
        """
        Actually preform a cache update - writing the values out to the cache.

        :param book_id_val_map:
        :param id_map:
        :return:
        """
        self.internal_update_cache(book_id_val_map, id_map)

    def internal_update_cache(
        self, book_id_item_id_map: dict[SrcTableID, Optional[set[DstTableID]]], id_map_update: dict[DstTableID, T]
    ) -> tuple[dict[SrcTableID, Optional[T]], set[SrcTableID]]:
        """
        Update cache with some additional information provided in the return.

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

            # Acquire a list of all the items originally linked to the book
            old_item_ids = self.book_col_map.get(book_id, set())

            # We are nullifying - remove the links
            if link_val is None:

                self.book_col_map[book_id] = set()
                for item_id in old_item_ids:
                    self.col_book_map[item_id] = None

                deleted.add(book_id)
                continue

            # Nullify the existing links
            for old_item_id in deepcopy(old_item_ids):
                # Remove the item from the col_book_map
                try:
                    old_item_book_id = self.col_book_map[old_item_id]
                except KeyError:
                    continue
                if old_item_book_id is None:
                    continue

                # Note that the item is gone from the col book map as well
                self.book_col_map[old_item_book_id].discard(old_item_id)

                # Nullify after the value has been used above
                self.col_book_map[old_item_id] = None

            for new_item_id in link_val:
                # Remove the item from the col_book_map
                try:
                    new_item_book_id = self.col_book_map[new_item_id]
                except KeyError:
                    continue
                if new_item_book_id is None:
                    continue

                # Note that the item is gone from the col book map as well
                self.book_col_map[new_item_book_id].discard(new_item_id)

                # Nullify after the value has been used above
                self.col_book_map[new_item_id] = None

            # We are updating - break the old link and write a new one back out
            self.book_col_map[book_id] = link_val
            for item_id in link_val:
                self.col_book_map[item_id] = book_id

            updated[book_id] = link_val

        return updated, deleted
