"""
ManyToOne tables are for instances where many titles could be linked to one - and only one - object.

These are slightly rare in LiuXin - mostly it's ManyToMany.
"""

from collections import defaultdict

from typing import TypeVar, Optional
from typing_extensions import Self

from LiuXin.databases.caches.calibre.tables.many_one_tables.many_to_one_table import CalibreManyToOneTable
from LiuXin.databases.db_types import MetadataDict, InterLinkTableName, SrcTableID, DstTableID

from LiuXin.exceptions import InvalidCacheUpdate

from LiuXin.utils.lx_libraries.liuxin_six import iteritems
from LiuXin.utils.general_ops.python_tools import element_to_front

T = TypeVar("T")


class CalibrePriorityManyToOneTable(CalibreManyToOneTable[T]):
    """
    many-to-one table with additional priority information -

    the entries linked to the many-to-one table are ordered
    but do not have type information - thus they will be returned as lists of information.
    The priority comes into play when asking "what books are associated with this item?" - the priority gives you an
    ordering (though not the other way round - as a maximum of one item can be associated with each book)
    """

    _priority: bool = True
    _typed: bool = False

    def __init__(
        self, name: str, metadata: MetadataDict, link_table: Optional[InterLinkTableName] = None, custom: bool = False
    ) -> None:
        """
        Init a PriorityManyToOne table.

        Data is not loaded at this point - that will happen later.
        :param name:
        :param metadata:
        :param link_table:
        :param custom:
        """
        super(CalibrePriorityManyToOneTable, self).__init__(
            name=name, metadata=metadata, link_table=link_table, custom=custom
        )

    @classmethod
    def from_priority_typed_table(cls, original_table: CalibreManyToOneTable, type_filter: str) -> Self:
        """
        Makes a PriorityManyToOneTable from a PriorityTypedManyToOneTable - by filtering out all but one of the

        :param original_table: A CalibrePriorityManyToOneTable table - a filter will be applied.
        :param type_filter:
        :return:
        """
        raise NotImplementedError

    def _col_book_map_factory(self) -> dict[DstTableID, list[SrcTableID]]:
        """
        Used to store the column-book links.

        This is a priority many to one link - so
         - the link from the book to the column is one-to-one
         - the link from the column to the book is many to one and ordered
         so the col-book link is modelled as a dictionary keyed with the id of the dst column and valued with a list of
         book ids
        :return:
        """
        return defaultdict(list)

    @staticmethod
    def _type_list_container() -> dict[str, list[SrcTableID]]:
        """
        Should not ... actually be here.

        :return:
        """
        return defaultdict(list)

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - READ METHODS

    def read_maps(self, db, type_filter: Optional[str] = None) -> None:
        """
        Preform a read out of the database and into the internal caches.

        :param db: The database to read off
        :param type_filter: Apply this type filter during the read
        :return None: All changes are made internally
        """
        link_table_book_id = self.link_table_bt_id_column
        link_table_other_id = self.link_table_table_id_column

        link_priority_col = db.driver_wrapper.get_link_column(
            table1="titles", table2=self.metadata["table"], column_type="priority"
        )
        stmt = "SELECT {0}, {1} FROM {2} ORDER BY {3} DESC;".format(
            link_table_book_id, link_table_other_id, self.link_table, link_priority_col
        )

        col_book_map = self.col_book_map
        book_col_map = self.book_col_map
        for book_id, item_id in db.driver_wrapper.execute(stmt):
            col_book_map[item_id].append(book_id)
            book_col_map[book_id] = item_id

        for book_id in self.seen_book_ids:
            if book_id not in self.book_col_map:
                self.book_col_map[book_id] = None

        for item_id in self.seen_item_ids:
            if item_id not in self.col_book_map:
                self.col_book_map[item_id] = []

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - UPDATE METHODS
    # Todo: Think this is not used - and so can be safely removed - though it should probably be used
    def cache_update_precheck(
        self, book_id_val_map: dict[SrcTableID, DstTableID], id_map: Optional[dict[DstTableID, T]]
    ) -> None:
        """
        Preforms checks that the given update is valid before applying it to the cache.

        :param book_id_val_map: Keyed with the
        :param id_map:
        :return None: An error will be raised if the update is malformed
        """
        for book_id, book_val in iteritems(book_id_val_map):
            if book_id not in self.seen_book_ids:
                raise InvalidCacheUpdate("book_id not found in the known book_ids")

        if not id_map:
            return

        # Todo: Check this against the table data type - some kinda validate function?
        for item_id in id_map:
            assert isinstance(item_id, (str, int))

    def update_cache(
        self, book_id_val_map: dict[SrcTableID, DstTableID], id_map: Optional[dict[DstTableID, T]]
    ) -> bool:
        """
        Preform an internal update of the data cached in this table.

        :param book_id_val_map: Keyed with the
        :param id_map:
        :return status: Did the cache update go through?
        """
        self.cache_update_precheck(book_id_val_map, id_map)

        # book_id_val_map should be keyed with the id of the book in question and valued with the new id of the target
        # - if the target is already linked to the book then the book_col_map (being single valued) need not be changed
        #   but the book_id needs to be promoted to the top of the col_book_map for that value
        # - if the target is not linked, then it needs to be in the book_col_map and added to the front of the
        #   col_book_map
        for book_id, book_val in iteritems(book_id_val_map):

            # Check to see if the item is already linked to the book
            try:
                is_book_already_set = book_val == self.book_col_map[book_id]
            except KeyError:
                is_book_already_set = False

            if is_book_already_set:
                # Just need to promote the book_id to the front of the col_book_map list
                self.col_book_map[book_val] = element_to_front(
                    target_list=self.col_book_map[book_val], list_element=book_id
                )
            else:
                # Need to promote the element and update the book_col_map
                self.book_col_map[book_id] = book_val
                try:
                    self.col_book_map[book_val] = element_to_front(
                        target_list=self.col_book_map[book_val], list_element=book_id
                    )
                except ValueError:
                    # The item is not in the list - so just insert it at the front
                    self.col_book_map[book_val] = [
                        book_id,
                    ] + self.col_book_map[book_val]

        # id_map will just have the updates applied to it - no need to do more
        if id_map is not None:
            self.id_map.update(id_map)

        return True

    def internal_update_cache(
        self, book_id_item_id_map: dict[SrcTableID, DstTableID], id_map_update: dict[DstTableID, T]
    ) -> tuple[dict[SrcTableID, T], set[SrcTableID]]:
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
                    self.col_book_map[old_item_id].remove(book_id)

                deleted.add(book_id)
                continue

            # We are updating - break the old link and write a new one back out
            self.book_col_map[book_id] = link_val

            if old_item_id is not None:
                self.col_book_map[old_item_id].remove(book_id)
            self.col_book_map[link_val] = [
                book_id,
            ] + self.col_book_map[link_val]

            updated[book_id] = link_val

        return updated, deleted

    #
    # ------------------------------------------------------------------------------------------------------------------
