"""
ManyToOne tables are for instances where many titles could be linked to one - and only one - object.

These are slightly rare in LiuXin - mostly it's ManyToMany.
"""


import uuid
from collections import defaultdict
from copy import deepcopy

from typing import Any, Union, TypeVar, Optional

from LiuXin.databases.caches.calibre.tables.many_one_tables import CalibreManyToOneTable
from LiuXin.databases.db_types import MetadataDict, SrcTableID, DstTableID, InterLinkTableName

from LiuXin.exceptions import InvalidCacheUpdate

from LiuXin.utils.lx_libraries.liuxin_six import iteritems
from LiuXin.utils.lx_libraries.liuxin_six import iterkeys
from LiuXin.utils.lx_libraries.liuxin_six import itervalues

from past.builtins import basestring

T = TypeVar("T")


class CalibreTypedManyToOneTable(CalibreManyToOneTable[T]):
    """
    many-to-one table with additional type information - however the entries linked to the many-to-one table are not
    ordered. Thus they will be returned as a dictionary of sets of elements.
    Many books are linked to the same entry.
    The typing comes into play when asking the question - "what books are linked to this entry?" - you can now ask
    the question "what books are linked to this entry with what type?"
    """

    _priority = False
    _typed = True

    def __init__(
        self, name: str, metadata: MetadataDict, link_table: Optional[InterLinkTableName] = None, custom: bool = False
    ):
        """
        Initialise a TypedManyToOneTable - a many to one link without a concept of priority but with a type.

        This table has a concept of type - but not priority.
        Each link has a type in addition to existing - but the links are not ordered.
        (In the prototypical example, the links would not be ordered in the books).

        Loading data off the database happens later - in read.
        :param name:
        :param metadata:
        :param link_table:
        :param custom:
        """
        super(CalibreTypedManyToOneTable, self).__init__(
            name=name, metadata=metadata, link_table=link_table, custom=custom
        )

        self.seen_link_types: set[str] = set()

        self.book_type_map: dict[SrcTableID, Optional[str]] = dict()

    def get_subtable(self, type_filter: str) -> CalibreManyToOneTable:
        """
        Returns a sub-table of the current table with data restricted to a specific type filter.

        Table returned would be a ManyToManyTable - with no type information stored (as that has already been filtered
        out).
        It can't be Priority - that data is not stored in this table (and, presumably, is not present in the db
        representation of this table).
        :param type_filter: String which should be in the existing type filters.
                            A table will be created using only the values for this filer.
        :return new_table: Loaded with data from this table.
        """
        return CalibreManyToOneTable.from_typed_table(original_table=self, type_filter=type_filter)

    @staticmethod
    def _type_container() -> dict[str, set[SrcTableID]]:
        """
        Part of the hack needed to make defaultdict return nested dicts.

        :return dict_container: Keyed with the type of the link and valued with a set - which will eventually contain
                                book values.
        """
        return defaultdict(set)

    def _col_book_map_factory(self) -> dict[DstTableID, dict[str, set[SrcTableID]]]:
        """
        Type has to be preserved when mapping the column to the book.

        As a result, the data storage structure for this col_book_map is
        dict keyed with item ids, then valued with types, then valued with sets of book ids.
        :return:
        """
        return defaultdict(self._type_container)

    def read_maps(self, db, type_filter: Optional[str] = None) -> None:
        """
        Preform a read out of the database and into the internal caches

        :param db:
        :param type_filter: If present, all links _except_ links with this type will be ignored.
        :return:
        """
        link_table_book_id = self.link_table_bt_id_column
        link_table_other_id = self.link_table_table_id_column

        link_type_col = db.driver_wrapper.get_link_column(
            table1="titles", table2=self.metadata["table"], column_type="type"
        )
        stmt = "SELECT {0}, {1}, {2} FROM {3};".format(
            link_table_book_id, link_table_other_id, link_type_col, self.link_table
        )

        col_book_map = self.col_book_map
        book_col_map = self.book_col_map
        seen_link_types = self.seen_link_types

        book_type_map = self.book_type_map

        for book_id, item_id, link_type in db.driver_wrapper.execute(stmt):
            col_book_map[link_type][item_id].add(book_id)
            book_col_map[book_id] = item_id

            book_type_map[book_id] = link_type

            seen_link_types.add(link_type)

        for item_id in self.seen_item_ids:
            for lt in seen_link_types:
                if item_id not in col_book_map[lt]:
                    col_book_map[lt][item_id] = set()

        for book_id in self.seen_book_ids:
            if book_id not in book_col_map:
                book_col_map[book_id] = None

    def internal_update_cache(
        self, book_id_item_id_map: dict[SrcTableID, dict[str, DstTableID]], id_map_update: dict[DstTableID, Optional[T]]
    ) -> tuple[dict[SrcTableID, Optional[dict[str, DstTableID]]], set[SrcTableID]]:
        """
        Update cache with some additional information provided.

        Used in write when it needs to know some info about the cache before writing out to the database.
        :param book_id_item_id_map:
        :param id_map_update: Dictionary used to directly update the id_map
        :return:
        """
        self.id_map.update(id_map_update)

        # If additions have been made, they need to be noted
        for new_item_id in id_map_update:
            for lt in self.seen_link_types:
                if new_item_id not in self.col_book_map:
                    self.col_book_map[lt][new_item_id] = set()

        # Update the book -> col and col -> book maps
        deleted = set()
        updated = {}
        for book_id, item_val in iteritems(book_id_item_id_map):

            # We're nullifying - just remove the book from the item map
            if item_val is None:
                # Remove the book from the map
                old_item_id = self.book_col_map.get(book_id, None)
                # Clear any mention from the col_book_map
                if old_item_id is not None:
                    self.book_col_map[book_id] = None
                    for link_type in iterkeys(self.col_book_map):
                        try:
                            self.col_book_map[link_type][old_item_id].remove(book_id)
                        except KeyError:
                            pass

                deleted.add(book_id)
                continue

            assert isinstance(item_val, dict)

            for link_type, link_val in iteritems(item_val):

                old_item_id = self.book_col_map.get(book_id, None)

                if link_val is None:

                    # A link type is given and the link is being nullified - check if the link matches and then clear
                    # if it does - if it does not then ignore and continue
                    try:
                        old_link_type = self.book_type_map[book_id]
                    except KeyError:
                        # No link is set - so no need to nullify - just continue
                        continue
                    if old_link_type != link_type:
                        continue

                    # Nullifying
                    self.book_col_map[book_id] = None
                    if old_link_type is not None and old_item_id is not None:
                        try:
                            self.col_book_map[old_link_type][old_item_id].remove(book_id)
                        except KeyError:
                            pass
                    self.book_type_map[book_id] = None

                elif isinstance(link_val, int):

                    # Completely remove the old link
                    try:
                        old_link_type = self.book_type_map[book_id]
                    except KeyError:
                        self.book_type_map[book_id] = None
                        old_link_type = None

                    # Nullifying
                    self.book_col_map[book_id] = None
                    if old_link_type is not None:
                        try:
                            self.col_book_map[old_link_type][old_item_id].remove(book_id)
                        except KeyError:
                            pass
                    self.book_type_map[book_id] = None

                    # Write the new link back out
                    self.book_col_map[book_id] = link_val
                    self.col_book_map[link_type][link_val].add(book_id)
                    self.book_type_map[book_id] = link_type

                else:

                    raise NotImplementedError

            updated[book_id] = item_val

        for lt in self.seen_link_types:
            if self._is_type_dict_null(self.col_book_map[lt]):
                del self.col_book_map[lt]

        return updated, deleted

    def _is_type_dict_null(self, type_dict: dict[Any, Any]) -> bool:
        """
        Checks to see if a type dict has any content.

        A type dict is keyed with strings - the types and valued with sets of book ids.
        This checks over all the entries to see if any of those sets have content.
        :param type_dict:
        :return status: True if there is content and False otherwise.
        """
        for item_id, item_vals in iteritems(type_dict):
            if item_vals:
                return False
        return True

    def update_precheck(
        self, book_id_item_id_map: dict[SrcTableID, dict[str, DstTableID]], id_map_update: dict[DstTableID, Optional[T]]
    ):
        """
        Check that an update is of a valid form before writing it out to the cache and the database.

        Called when you know the ids you want to assign to the book after the update. Checks those ids are valid.
        No changes will be made to the :param book_id_item_id_map: (e.g. if the map is valued with tuples - not lists
        as expected - this will not be corrected.
        Raised InvalidCacheUpdate if the cache update is invalid in some way.

        :param book_id_item_id_map: Keyed with the ids of the books to update and valued with the
        :param id_map_update:
        :return None: No return - just errors if there's a problem.
        """
        for book_id, book_val in iteritems(book_id_item_id_map):

            if book_val is None:
                continue

            if isinstance(book_val, (set, list)):
                raise InvalidCacheUpdate("book_val cannot be set or list valued - we need type information to update")

            if not isinstance(book_val, dict):
                raise InvalidCacheUpdate(
                    "book_val has to be in the form of a dictionary - " "type information is needed to update"
                )

            if not len(book_val) == 1:
                for lt, lv in iteritems(book_val):
                    if lv is not None:
                        raise InvalidCacheUpdate("book_val is malformed")

            for book_val_val in itervalues(book_val):

                if book_val_val is None:
                    continue

                if not isinstance(book_val_val, (basestring, int)):
                    raise InvalidCacheUpdate("Book update dict is malformed!")

                if isinstance(book_val_val, int):
                    if book_val_val not in self.seen_item_ids and book_val_val not in id_map_update:
                        raise InvalidCacheUpdate("Book update dict is malformed")

    # Todo: This might be being hit twice during the update operations - as the update is not being marked as being done
    #       by the internal cache - check that this is the case
    def update_cache(
        self,
        book_id_val_map: dict[SrcTableID, dict[str, DstTableID]],
        id_map: Optional[dict[DstTableID, Optional[T]]] = None,
    ) -> None:
        """
        Actually preform the update of the cache.

        :param book_id_val_map: dict of dicts
                                Keyed with the
        :param id_map:
        :return None: All changes are made internally
        """

        self.update_precheck(book_id_val_map, id_map)

        self.internal_update_cache(book_id_val_map, id_map_update=id_map)

    def item_data(
        self, book_id: SrcTableID, type_filter: str = None
    ) -> Union[dict[str, set[SrcTableID]], set[SrcTableID]]:
        """
        Returns the book data for the given item - as a dictionary keyed with link_types and valued with book ids sets.

        :param book_id:
        :param type_filter:
        :return:
        """
        if type_filter is None:
            item_data_dict = dict()
            for known_type in self.col_book_map.keys():
                item_data_dict[known_type] = self.col_book_map[known_type][book_id]
            return deepcopy(item_data_dict)
        else:
            assert type_filter in self.col_book_map.keys()
            return deepcopy(self.col_book_map[type_filter][book_id])
