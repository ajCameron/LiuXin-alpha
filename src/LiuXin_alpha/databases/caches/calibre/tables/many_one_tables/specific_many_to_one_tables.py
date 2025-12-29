"""
ManyToOne tables specialized for various purposes.
"""

from collections import defaultdict

from typing import Any, Union, TypeVar, Optional

from LiuXin.databases.caches.calibre.tables.many_one_tables.typed_many_to_one_table import CalibreTypedManyToOneTable
from LiuXin.databases.caches.calibre.tables.many_one_tables.typed_many_to_one_table import CalibreManyToOneTable
from LiuXin.databases.db_types import (
    MetadataDict,
    InterLinkTableName,
    SrcTableID,
    DstTableID,
    TableColumnName,
    RatingInt,
)

from LiuXin.exceptions import InvalidCacheUpdate

from LiuXin.utils.lx_libraries.liuxin_six import iteritems
from LiuXin.utils.general_ops.language_tools import plural_singular_mapper

from past.builtins import basestring

T = TypeVar("T")


class CalibreRatingTable(CalibreTypedManyToOneTable[int]):

    _priority = False
    _typed = True

    def read_id_maps(self, db) -> None:
        """
        Read the database to create the id maps.

        :param db:
        :return None: All changes take place internally
        """
        self.link_table_bt_id_column: TableColumnName = "rating_title_link_title_id"
        self.link_table_table_id_column: TableColumnName = "rating_title_link_rating_id"
        self.link_table: InterLinkTableName = "rating_title_links"

        super(CalibreRatingTable, self).read_id_maps(db)

        idcol: TableColumnName = db.driver_wrapper.get_id_column(self.metadata["table"])
        link_table: TableColumnName = db.driver_wrapper.get_link_table_name(
            table1="titles", table2=self.metadata["table"]
        )
        link_table_book_col = db.driver_wrapper.get_link_column(
            table1="titles", table2=self.metadata["table"], column_type=idcol
        )

        # Ensure there are no records with rating=0 in the table. These should be represented as rating:None instead.
        bad_ids: set[DstTableID] = {item_id for item_id, rating in iteritems(self.id_map) if rating == 0}
        if bad_ids:

            # Filter the 0 ids from the id_map
            self.id_map = {item_id: rating for item_id, rating in iteritems(self.id_map) if rating != 0}

            # Remove any links between the bad ids and books
            db.driver_wrapper.executemany(
                "DELETE FROM {0} WHERE {1}=?".format(link_table, link_table_book_col),
                tuple((x,) for x in bad_ids),
            )

            # Delete the bad entries from the table itself
            db.driver_wrapper.execute(
                "DELETE FROM {0} WHERE {1}=0".format(self.metadata["table"], self.metadata["column"])
            )

    def read_maps(self, db, type_filter: str = "calibre") -> None:
        """
        Read the maps - filtering the link table to only return the results which are of link type calibre.

        Not the best fix in the world, but it should work.
        :param db:
        :param type_filter: defaults to "calibre"
        :return:
        """
        assert type_filter == "calibre"

        link_table_book_id: TableColumnName = self.link_table_bt_id_column
        link_table_other_id: TableColumnName = self.link_table_table_id_column

        link_type_col: TableColumnName = db.driver_wrapper.get_link_column(
            table1="titles", table2=self.metadata["table"], column_type="type"
        )
        stmt: str = "SELECT {0}, {1}, {2} FROM {3};".format(
            link_table_book_id, link_table_other_id, link_type_col, self.link_table
        )

        col_book_map = self.col_book_map
        book_col_map = self.book_col_map
        seen_link_types = self.seen_link_types

        book_type_map = self.book_type_map

        for book_id, item_id, link_type in db.driver_wrapper.execute(stmt):
            col_book_map[link_type][item_id].add(book_id)
            if link_type == "calibre":
                book_col_map[book_id] = item_id

            book_type_map[book_id] = link_type

            seen_link_types.add(link_type)

    def update_db(
        self, book_id_to_val_map: dict[SrcTableID, Optional[RatingInt]], db, allow_case_change: bool = False
    ) -> bool:
        """
        Write an update of calibre ratings out to the database.

        :param book_id_to_val_map:
        :param db:
        :param allow_case_change:
        :return:
        """

        # This table is presenting a table WITHOUT a type - while actually being a typed table
        # Making changes to the underlying table requires changes to the form of the update dict
        new_update_dict = defaultdict(dict)
        for book_id, book_val in iteritems(book_id_to_val_map):
            new_update_dict[book_id]["calibre"] = book_val

        return super(CalibreRatingTable, self).update_db(
            book_id_to_val_map=new_update_dict,
            db=db,
            allow_case_change=allow_case_change,
        )

    # Todo: I think this is being run twice - note in write that the update has been done
    def internal_update_cache(
        self, book_id_item_id_map: dict[SrcTableID, Optional[DstTableID]], id_map_update: dict[DstTableID, Optional[T]]
    ) -> tuple[dict[SrcTableID, Optional[DstTableID]], set[SrcTableID]]:
        """
        Write out to the caches ratings values

        :param book_id_item_id_map:
        :param id_map_update:
        :return:
        """
        # Need this for writing out to the database
        type_stripped_dict = dict()

        # Update the book_col_map and col_book_map
        for book_id, book_val in iteritems(book_id_item_id_map):

            try:
                self.book_col_map[book_id] = book_val["calibre"]
            except TypeError:
                self.book_col_map[book_id] = book_val

            try:
                self.col_book_map["calibre"][book_val["calibre"]].add(book_id)
            except TypeError:
                self.col_book_map["calibre"][book_val].add(book_id)

            try:
                type_stripped_dict[book_id] = book_val["calibre"]
            except TypeError:
                type_stripped_dict[book_id] = book_val

        return type_stripped_dict, set()


# ----------------------------------------------------------------------------------------------------------------------
#
# - CUSTOM COLUMN RATING TABLE


class CalibreCustomColumnsManyOneTable(CalibreManyToOneTable[T]):

    _priority = False
    _typed = False

    def __init__(
        self, name: str, metadata: MetadataDict, link_table: Optional[InterLinkTableName] = None, custom: bool = False
    ) -> None:
        """
        Startup a custom ManyToOne table - which has slightly different logic re. link tables.

        :param name: The name of the table
        :param metadata: The metadata associated with the table - how it should display and other properties
        :param link_table: The table linking this table to either the books or titles field
        :param custom: Is this table a custom table
        """
        super(CalibreCustomColumnsManyOneTable, self).__init__(
            name=name, metadata=metadata, link_table=link_table, custom=custom
        )

    def update_precheck(
        self, book_id_item_id_map: dict[SrcTableID, Optional[DstTableID]], id_map_update: dict[DstTableID, Optional[T]]
    ) -> None:
        """
        Check the given book update is valid.

        :param book_id_item_id_map:
        :param id_map_update:
        :return None: Will throw an error if there's a problem.
        """

        for book_id, book_val in iteritems(book_id_item_id_map):

            if book_id not in self.seen_book_ids:
                raise InvalidCacheUpdate("update_dict malformed")

            if book_val is None:
                continue

            if not isinstance(book_val, (int, basestring)):
                raise InvalidCacheUpdate("update_dict malformed")

            if isinstance(book_val, int):
                if book_val not in self.seen_item_ids:
                    raise InvalidCacheUpdate("update_dict malformed")

    def _read_id_maps(self, db) -> None:
        """
        Read the entire table and create a dictionary keyed with the id and valued with the relevant column value.

        Loads any elements from the table connected to the book.
        :param db:
        :return None: All changes are made internally
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
            target_col = self.metadata["column"]

            query = db.driver_wrapper.execute(
                "SELECT {cc_col}_id, {cc_col}_{target_col} FROM {cc_table}"
                "".format(cc_table=cc_table, cc_col=cc_col, target_col=target_col)
            )

        if self.unserialize is None:
            self.id_map = dict(query)
        else:
            us = self.unserialize
            self.id_map = {book_id: us(val) for book_id, val in query}

        # Read the ids from the books table - so that we know what ids are valid for the table
        # Todo: Need to generalize to other things that the custom column could be linked to
        title_query = db.driver_wrapper.execute("SELECT title_id FROM titles")
        self.seen_book_ids = set([tr[0] for tr in title_query])

        self.seen_item_ids = set(self.id_map.keys())

    # Todo: Need to move the sql which does the reads into the macros
    def read_id_maps(self, db) -> None:
        """
        Read the database to create the id maps.

        :param db:
        :return None: All changes are made internally
        """
        # Todo: Move the functions that make the names for the custom columns over into utils
        # Link table properties
        self.link_table_bt_id_column = "book"
        # self.link_table_bt_id_column = "{}_custom_column_{}_link_book".format(
        #     self.metadata["in_table"], self.metadata["colnum"]
        # )
        self.link_table_table_id_column = "value"
        # self.link_table_table_id_column = "{}_custom_column_{}_link_value".format(
        #     self.metadata["in_table"], self.metadata["colnum"]
        # )
        self.link_table = "{}_custom_column_{}_link".format(self.metadata["in_table"], self.metadata["colnum"])

        # Read the actual ratings table
        self._read_id_maps(db)

        # ratings table properties
        # idcol = db.driver_wrapper.get_id_column(self.metadata['table']) # Todo: After the name change for custom columns this should just work
        idcol = "id"
        link_table = self.link_table
        link_table_book_col = self.link_table_bt_id_column

        # Ensure there are no records with rating=0 in the table. These should be represented as rating:None instead.
        bad_ids = {item_id for item_id, rating in iteritems(self.id_map) if rating == 0}
        if bad_ids:

            # Filter the 0 ids from the id_map
            self.id_map = {item_id: rating for item_id, rating in iteritems(self.id_map) if rating != 0}

            # Remove any links between the bad ids and books
            db.driver_wrapper.executemany(
                "DELETE FROM {0} WHERE {1}=?".format(link_table, link_table_book_col),
                tuple((x,) for x in bad_ids),
            )

            # Delete the bad entries from the table itself
            db.driver_wrapper.execute(
                "DELETE FROM {0} WHERE {1}=0".format(self.metadata["table"], self.metadata["column"])
            )

    def read_maps(self, db, type_filter: str = "calibre") -> None:
        """
        Read the maps - filtering the link table to only return the results which are of link type calibre.

        Not the best fix in the world, but it should work.
        :param db:
        :param type_filter: Should be calibre, unless there is a very good reason for it not to be.
        :return None:
        """
        if not self.custom:
            lt_book_id = self.link_table_bt_id_column
            lt_value_id = self.link_table_table_id_column

            stmt = "SELECT {0}, {1} FROM {2};".format(lt_book_id, lt_value_id, self.link_table)

        else:

            lt = self.link_table
            lt_col = plural_singular_mapper(lt)

            lt_book_id = self.link_table_bt_id_column
            lt_value_id = self.link_table_table_id_column

            stmt = "SELECT {lt_col}_{lt_book_id}, {lt_col}_{lt_value_id} FROM {lt};" "".format(
                lt_book_id=lt_book_id, lt_value_id=lt_value_id, lt=lt, lt_col=lt_col
            )
        col_book_map = self.col_book_map
        book_col_map = self.book_col_map

        for book_id, item_id in db.driver_wrapper.execute(stmt):
            col_book_map[item_id].add(book_id)
            book_col_map[book_id] = item_id

    def update_db(self, book_id_to_val_map: dict[SrcTableID, Optional[T]], db, allow_case_change: bool = False) -> bool:
        """
        Write an update out to a custom ManyToOneTable on the database.

        :param book_id_to_val_map:
        :param db:
        :param allow_case_change:
        :return:
        """

        # Todo: This can probably be removed
        new_update_dict = dict()
        for book_id, book_val in iteritems(book_id_to_val_map):
            new_update_dict[book_id] = book_val

        return super(CalibreCustomColumnsManyOneTable, self).update_db(
            book_id_to_val_map=book_id_to_val_map,
            db=db,
            allow_case_change=allow_case_change,
        )

    # Todo: I think this is being run twice - note in write that this has already been run
    # Todo: There is definitely some cache update logic being run before this - as such, this is incomplete
    #          we're only adding to the cache - not removing or repointing the values
    def internal_update_cache(
        self, book_id_item_id_map: dict[SrcTableID, Optional[DstTableID]], id_map_update: dict[SrcTableID, Optional[T]]
    ) -> tuple[dict[SrcTableID, DstTableID], set[SrcTableID]]:
        """
        Preform a write out to the caches ratings values
        :param book_id_item_id_map:
        :param id_map_update:
        :return updated, deleted:
                updated - dict- keyed with the value of the book id and valued with the new value
                          Note - the new value can never be None
                deleted - set of book ids to be set null
        """
        self.id_map.update(id_map_update)

        updated = dict()
        deleted = set()

        # Update the book_col_map and col_book_map
        for book_id, book_val in iteritems(book_id_item_id_map):

            if book_val is None:

                try:
                    old_book_val = self.book_col_map[book_id]
                except KeyError:
                    pass
                else:
                    self.col_book_map[old_book_val].remove(book_id)

                deleted.add(book_id)

                del self.book_col_map[book_id]

                continue

            # Remove old mentions of the value to be repointed
            try:
                old_book_val = self.book_col_map[book_id]
            except KeyError:
                pass
            else:
                self.col_book_map[old_book_val].remove(book_id)

            # Update the cache with the new values
            self.book_col_map[book_id] = book_val
            self.col_book_map[book_val].add(book_id)

            updated[book_id] = book_val

        # Purge the empty sets
        for col_id in self.col_book_map.keys():
            if not self.col_book_map[col_id]:
                del self.col_book_map[col_id]

        return updated, deleted
