"""
ManyToMany tables are items which are linked to many other items and visa versa.

E.g. "tags" and "titles" - many "tags" can be assigned to a "title" and visa versa.
"""

import pprint
from collections import defaultdict, OrderedDict
from copy import deepcopy

from typing import TypeVar, Optional, Iterable, Union, Any

from LiuXin.databases.caches.calibre.tables.many_many_tables.many_to_many_table import CalibreManyToManyTable
from LiuXin.databases.db_types import (
    MetadataDict,
    InterLinkTableName,
    SrcTableID,
    DstTableID,
)
from LiuXin.databases.write import uniq

from LiuXin.exceptions import (
    InvalidCacheUpdate,
)

from LiuXin.utils.icu import lower as icu_lower
from LiuXin.utils.lx_libraries.liuxin_six import (
    dict_iteritems as iteritems,
    dict_itervalues as itervalues,
)

# Py2/Py3 compatibility layer
from past.builtins import basestring

T = TypeVar("T")


class CalibreTypedManyToManyTable(CalibreManyToManyTable[T]):
    """
    Represents a many to many field with a type - so the items the book are linked to are broken down into categories.
    """

    _priority: bool = False
    _typed: bool = True

    def __init__(
        self, name: str, metadata: MetadataDict, link_table: Optional[InterLinkTableName] = None, custom: bool = False
    ) -> None:
        """
        Startup a typed many to many table.

        :param name:
        :param metadata:
        :param link_table:
        :param custom:
        """
        super(CalibreTypedManyToManyTable, self).__init__(
            name=name, metadata=metadata, link_table=link_table, custom=custom
        )

        # Todo: Change this back to book_col_map and col_book_map - to be consistent with everything else
        self.col_book_map = defaultdict(self._type_container)
        self.book_col_map = defaultdict(self._type_container)

    @classmethod
    def from_priority_typed_table(cls, db, original_table: CalibreManyToManyTable, type_filter: str) -> None:
        """
        Factor method to build a ManyToManyTable from existing maps.

        :param db: The database currently being cached
        :param original_table: The original table to build this table from
        :param type_filter: Create the new table restricted to these types from the old
        :return:
        """

        # assert isinstance(
        #     original_table, CalibrePriorityTypedManyToManyTable
        # ), "only PriorityTypedManyToManyTables are supported by this method"

        assert original_table.priority and original_table.typed

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

    # Todo: type_filter should be one of the permitted types in the main table - implement that
    def get_subtable(self, db, type_filter: str) -> CalibreManyToManyTable:
        """
        Returns a subtable of the current table with data restricted to a specific type filter.

        Table returned would be a ManyToManyTable - with no type information stored (as that has already been filtered
        out).
        :param db: The database we're operating on
        :param type_filter: Filter to apply to the
        :return:
        """
        return CalibreManyToManyTable.from_typed_table(db=db, original_table=self, type_filter=type_filter)

    @staticmethod
    def _type_container() -> dict[Union[SrcTableID, DstTableID], Iterable[Union[SrcTableID, DstTableID]]]:
        """
        Type container for the sub class.

        :return:
        """
        return defaultdict(set)

    def book_data(
        self, book_id: SrcTableID, type_filter: str = None
    ) -> Union[Iterable[DstTableID], dict[DstTableID, dict[str, Any]]]:
        """
        Returns the book data for the given book.

        :param book_id:
        :param type_filter:
        :return:
        """
        if type_filter is None:
            book_data_dict = dict()
            for known_type in self.book_col_map.keys():
                book_data_dict[known_type] = self.book_col_map[known_type][book_id]
            return deepcopy(book_data_dict)
        else:
            assert type_filter in self.book_col_map.keys()
            return deepcopy(self.book_col_map[type_filter][book_id])

    def item_data(
        self, item_id: DstTableID, type_filter: str = None
    ) -> Union[Iterable[SrcTableID], dict[SrcTableID, dict[str, Any]]]:
        """
        Returns the book data for the given item.

        :param item_id:
        :param type_filter:
        :return:
        """
        if type_filter is None:
            item_data_dict = dict()
            for known_type in self.col_book_map.keys():
                item_data_dict[known_type] = self.col_book_map[known_type][item_id]
            return deepcopy(item_data_dict)
        else:
            assert type_filter in self.col_book_map.keys()
            return deepcopy(self.col_book_map[type_filter][item_id])

    def vals_book_data(self, book_id: SrcTableID, type_filter: Optional[str] = None) -> Union[dict[Any, Any], set[T]]:
        """
        Returns the book data for the given record.

        :param book_id:
        :param type_filter:
        :return:
        """
        if type_filter is None:
            book_data_dict = dict()
            for known_type in self.book_col_map.keys():
                book_data_dict[known_type] = self._ids_to_vals(self.book_col_map[known_type][book_id])
            return deepcopy(book_data_dict)
        else:
            assert type_filter in self.book_col_map.keys()
            return deepcopy(self._ids_to_vals(self.book_col_map[type_filter][book_id]))

    def _ids_to_vals(self, ids_container: Iterable[DstTableID]) -> set[T]:
        """
        Takes a set of ids and produces a container of values.

        :param ids_container:
        :return:
        """
        return set([self.id_map[id_] for id_ in ids_container])

    def read(self, db) -> None:
        """
        Load the table with data from the database.

        :param db:
        :return:
        """
        # Clear the cache before the read
        self.id_map = {}

        self.col_book_map = defaultdict(self._type_container)
        self.book_col_map = defaultdict(self._type_container)

        # Reading off the database
        self.read_id_maps(db)
        self.read_maps(db)

        self.data_loaded = True

    def read_maps(self, db, type_filter: Optional[str] = None) -> None:
        """
        Preform a read off the database - recording the retrieved data in the cache.

        :param db: DatabasePing to preform read off
        :param type_filter: Not sure what this should do
        :return:
        """
        self.set_link_table(db, set_type=True)

        typed_col_book_map = self.col_book_map
        typed_book_col_map = self.book_col_map

        # Todo: The name for this is TERRIBLE - you have seen_books and seen_book_ids
        # By the time that we get here, seen_books_ids should already contain ALL the book ids
        seen_books = self.seen_books
        known_link_types = self.known_link_types

        # Read the book_id, table_id and the link type out of the link table - then build the maps
        stmt = "SELECT {0}, {1}, {2} FROM {3};".format(
            self.link_table_bt_id_column,
            self.link_table_table_id_column,
            self.link_table_type,
            self.link_table,
        )

        for book_id, item_id, link_type in db.driver_wrapper.execute(stmt):
            typed_col_book_map[link_type][item_id].add(book_id)
            typed_book_col_map[link_type][book_id].add(item_id)

            seen_books.add(book_id)
            known_link_types.add(link_type)

        # Make sure that every book has an entry for every category - even if it's empty
        for book_id in self.seen_book_ids:
            for link_type in known_link_types:
                if book_id not in typed_book_col_map[link_type]:
                    typed_book_col_map[link_type][book_id] = set()

        for item_id in self.seen_item_ids:
            for link_type in known_link_types:
                if item_id not in typed_col_book_map[link_type]:
                    typed_col_book_map[link_type][item_id] = set()

    # Todo: This is a really good idea - use it everywhere - instead of known_link_types
    @property
    def seen_types(self) -> set[str]:
        """
        Returns a set of all the types which have been read into the cache.

        :return:
        """
        return set(self.col_book_map.keys())

    @property
    def seen_link_types(self) -> set[str]:
        """
        Returns a set of all the types which have been read into the cache.

        :return:
        """
        return self.seen_types

    def remove_books(self, book_ids: Iterable[SrcTableID], db) -> set[DstTableID]:
        """
        Remove any references to the given book_ids from this table.

        Called after a book has been deleted.
        :param book_ids: The book_ids to remove
        :param db: The database to remove them from
        :return clean: The set of ids that are now unused
        """
        # 1) Remove the books from the map
        # 2) Then use those ids to remove the book ids from the cache corresponding to the creator
        clean_candidates = set()

        for known_type in self.seen_types:
            for book_id in book_ids:

                item_ids = self.book_col_map[known_type].pop(book_id, ())
                for item_id in item_ids:

                    try:
                        self.col_book_map[known_type][item_id].remove(book_id)
                    except KeyError:
                        pass
                    else:
                        # If the item is now not referenced by any books it may be suitable for cleaning.
                        if not self.col_book_map[known_type][item_id]:
                            clean_candidates.add(item_id)

        # 3) Check to see if any of the candidates for cleaning are now linked to no titles
        clean = set()
        for item_id in clean_candidates:

            for known_type in self.seen_types:
                if self.col_book_map[known_type][item_id]:
                    # If the item_id appears anywhere then break out of the loop - item is still in ise
                    break
            else:
                clean.add(item_id)

        # Preform the actual clear
        if clean and self.do_clean_on_remove:

            for known_type in self.seen_types:
                for item_id in clean:
                    del self.col_book_map[known_type][item_id]

            for item_id in clean:
                del self.id_map[item_id]

            try:
                db.maintainer.clean(table=self.metadata["table"], item_ids=clean)
            except AttributeError:
                raise AttributeError("type(db): {}".format(type(db)))

        return clean

    def remove_items(
        self, item_ids: Iterable[DstTableID], db, restrict_to_book_ids: Optional[Iterable[SrcTableID]] = None
    ) -> set[SrcTableID]:
        """
        Remove the items from the table - updating both the database and the cache.

        :param item_ids:
        :param db:
        :param restrict_to_book_ids:
        :return affected_books:
        """
        # Todo: This is not preforming properly
        affected_books = set()

        # Todo: These should already have been set
        # Making the names of the link column to target
        idcol = db.driver_wrapper.get_id_column(self.metadata["table"])
        link_table = db.driver_wrapper.get_link_table_name(table1="titles", table2=self.metadata["table"])
        item_link_col = db.driver_wrapper.get_link_column(
            table1="titles", table2=self.metadata["table"], column_type=idcol
        )

        if restrict_to_book_ids is None:

            for known_type in self.seen_types:
                for item_id in item_ids:

                    # update the typed_col_book_map and typed_book_col_map
                    try:
                        book_ids = self.col_book_map[known_type].pop(item_id)
                    except KeyError:
                        continue
                    for book_id in book_ids:
                        self.book_col_map[known_type][book_id].remove(item_id)
                        affected_books.add(book_id)

            for item_id in item_ids:
                del self.id_map[item_id]

            item_ids = tuple((x,) for x in item_ids)

            # Remove the links from the book to the item
            db.driver_wrapper.executemany(
                "DELETE FROM {0} WHERE {1}=?".format(link_table, item_link_col),
                item_ids,
            )

            # Mark the items that where just removed for potential cleaning
            db.maintainer.clean(table=self.metadata["table"], item_ids=item_ids)

            return affected_books

        else:

            for known_type in self.seen_types:

                for item_id in item_ids:

                    book_ids = self.col_book_map[known_type][item_id].intersection(item_ids)
                    for book_id in book_ids:
                        self.book_col_map[known_type][book_id].remove(item_id)
                        affected_books.add(book_id)

            ids_for_clean = set()
            for item_id in item_ids:
                for known_type in self.seen_types:
                    if self.col_book_map[known_type][item_id]:
                        # If the item_id appears anywhere then break out of the loop - item is still in ise
                        break
                else:
                    ids_for_clean.add(item_id)

            for item_id in ids_for_clean:
                del self.id_map[item_id]

            # Delete book/item pairs from the link table - the main table is untouched, as there might still be books (
            # and other things) linking to it
            book_link_col = db.driver_wrapper.get_link_column(
                table1="titles", table2=self.metadata["table"], column_type="title_id"
            )
            stmt = "DELETE FROM {0} WHERE {1}=? AND {2}=?;".format(link_table, book_link_col, item_link_col)
            db.driver_wrapper.executemany(stmt, [(b, i) for b in affected_books for i in item_ids])

            return affected_books

    def get_affected_books(self, item_id: DstTableID) -> set[SrcTableID]:
        """
        Returns the books that would be affected by a change to the given item.

        :param item_id:
        :return:
        """
        affected_book_ids = set()
        for creator_type in self.col_book_map:
            typed_affected_ids = self.col_book_map[creator_type].get(item_id, set())
            for book_id in typed_affected_ids:
                affected_book_ids.add(book_id)
        return affected_book_ids

    def rename_item(self, item_id: DstTableID, new_name: T, db) -> tuple[set[SrcTableID], DstTableID]:
        """
        Preform a rename of an item in the table and update the cache.

        If the new name doesn't match an existing item then a new entry will be created.
        If the item already exists then everything pointed to the old item will be repointed to the new structure.
        :param item_id:
        :param new_name:
        :param db:
        :return:
        """
        # Create a map to try and match the item to existing items - fallback to using icu_lower if nothing better
        # presents itself
        rmap = {icu_lower(v): k for k, v in iteritems(self.id_map)}
        existing_item_id = rmap.get(icu_lower(new_name), None)

        table, column = self.metadata["table"], self.metadata["column"]
        try:
            id_col = db.driver_wrapper.get_id_column(table)
        except KeyError:
            if table == "authors":
                id_col = "creator_id"
                table = "creators"
                column = "creator"
            elif table == "cover":
                id_col = "cover_id"
                table = "covers"
                column = "cover"
            else:
                raise

        link_table_name = db.driver_wrapper.get_link_table_name(table1="titles", table2=table)
        link_table_book_col = db.driver_wrapper.get_link_column(table1="titles", table2=table, column_type="title_id")
        link_table_item_col = db.driver_wrapper.get_link_column(table1="titles", table2=table, column_type=id_col)

        affected_book_ids = self.get_affected_books(item_id)
        new_item_id = item_id

        # Just rename - no merge or anything fancy required
        if existing_item_id is None or existing_item_id == item_id:

            self.id_map[item_id] = new_name
            stmt = "UPDATE {0} SET {1}=? WHERE {2}=?;".format(table, column, id_col)
            db.driver_wrapper.execute(stmt, (new_name, item_id))

        # Replacement has to happen - jumping to another entry and marking the old entry for cleanup/auto-merge
        else:

            # All the titles linked to the old item need to be pointed to the new item
            # Need to make sure that we're not trying to link to the same book twice
            new_item_id = existing_item_id
            dual_book_ids = set()

            # Remove the old item from the maps - it's no longer needed
            self.id_map.pop(item_id, None)
            for link_type in self.seen_types:
                old_item_book_ids = self.col_book_map[link_type].pop(item_id, set())
                new_item_book_ids = self.col_book_map[link_type].get(new_item_id, set())
                dual_book_ids += old_item_book_ids.intersection(new_item_book_ids)

                # Replacing item_id with existing_item could cause the same id to appear twice in the book list. Handle that
                # by removing existing item from the book list before replacing.
                for book_id in old_item_book_ids:
                    self.book_col_map[link_type][book_id] = set(
                        (existing_item_id if x == item_id else x)
                        for x in self.book_col_map.get(book_id, ())
                        if x != existing_item_id
                    )
                self.col_book_map[existing_item_id].update(old_item_book_ids)

            # Find and remove any books which are linked to both tags
            del_stmt = "DELETE FROM {0} WHERE {1}=? AND {2}=?;".format(
                link_table_name, link_table_book_col, link_table_item_col
            )
            for dual_book_id in dual_book_ids:
                db.driver_wrapper.execute(del_stmt, (dual_book_id, existing_item_id))

            # Update the link table to point to the replaced item and delete the old item
            update_stmt = "UPDATE {0} SET {1}=? WHERE {1}=?;".format(link_table_name, link_table_item_col)
            db.driver_wrapper.execute(update_stmt, (existing_item_id, item_id))

            # Pass the item id to the database for potential cleaning
            db.maintenance.merge(table=self.metadata["table"], item_1_id=new_item_id, item_2_id=item_id)

        return affected_book_ids, new_item_id

    def fix_case_duplicates(self, db) -> None:
        """
        Merge any entries which are the same up to case.

        :param db:
        :return:
        """
        # Build a case map - keyed with the lower case of the item and valued with all the items which correspond to
        # that item
        case_map = defaultdict(set)
        for item_id, val in iteritems(self.id_map):
            case_map[icu_lower(val)].add(item_id)

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

                for link_type in self.seen_types:

                    for item_id in v:
                        try:
                            self.id_map.pop(item_id, None)
                        except KeyError:
                            pass

                        books = self.col_book_map[link_type].pop(item_id, set())

                        for book_id in books:
                            if book_id in done_books:
                                continue
                            done_books.add(book_id)
                            orig = self.book_col_map[link_type].get(book_id, ())
                            if not orig:
                                continue

                            vals = uniq(tuple(main_id if x in v else x for x in orig))
                            self.book_col_map[book_id] = vals

                            if len(orig) == len(vals):
                                # We have a simple replacement - update in the link table
                                stmt = "UPDATE {0} SET {1} = ? WHERE {1} = ? AND {2} = ?".format(
                                    link_table, link_table_item_col, link_table_book_col
                                )
                                db.executemany(stmt, tuple((main_id, x, book_id) for x in v))
                            else:
                                # duplicates - if there are duplicate links fix that by removing then re-adding the link
                                # Remove first to not conflict with the UNIQ condition that should be (and will be) on
                                # these tables
                                del_stmt = "DELETE FROM {0} WHERE {1}=?;".format(link_table, link_table_book_col)
                                db.driver_wrapper.execute(del_stmt, (book_id,))

                                inst_stmt = (
                                    "INSERT INTO {0} ({1},{2}) VALUES (?,?)".format(
                                        link_table,
                                        link_table_book_col,
                                        link_table_item_col,
                                    ),
                                )
                                db.driver_wrapper.executemany(inst_stmt, tuple((book_id, x) for x in vals))

                # Tag for updating all the other link tables
                db.maintenance.merge(table=self.metadata["table"], item_1_id=main_id, item_2_id=v)

    def update_preflight(
        self,
        book_id_item_id_map: dict[SrcTableID, Optional[Iterable[DstTableID]]],
        id_map_update: Optional[dict[DstTableID, T]] = None,
        dirtied: Optional[set[SrcTableID]] = None,
    ):
        """
        Processes the book_id_item_id_map to bring it into a form to write an update out to the db and the cache.

        :param book_id_item_id_map:
        :param id_map_update:
        :return:
        """
        dirtied = set() if dirtied is None else dirtied

        clean_book_id_item_id_map = defaultdict(dict)
        for book_id, book_update_dir in iteritems(book_id_item_id_map):

            if book_update_dir is None:
                clean_book_id_item_id_map[book_id] = None
                continue

            for link_type, link_vals in iteritems(book_update_dir):

                # Todo: Implement checking that the link type is allowed - after we have a table of allowed values

                # Use the original values from the update
                if link_vals is None:
                    clean_book_id_item_id_map[book_id][link_type] = None

                elif isinstance(link_vals, set):
                    clean_book_id_item_id_map[book_id][link_type] = book_update_dir[link_type]

                elif isinstance(link_vals, (basestring, int)):
                    clean_book_id_item_id_map[book_id][link_type] = {
                        book_update_dir[link_type],
                    }.union(self.book_col_map[link_type][book_id])

                else:
                    # Todo: Should be BadUpdate of some form
                    raise NotImplementedError(
                        self._update_preflight_unexpected_case(book_id_item_id_map, link_type, link_vals)
                    )

        return clean_book_id_item_id_map, id_map_update

    def _update_preflight_unexpected_case(self, book_id_item_id_map: Any, link_type: Any, link_vals: Any) -> str:
        """
        Err msg generated when the update cannot be normalised.

        :param book_id_item_id_map:
        :param link_type:
        :param link_vals:
        :return:
        """
        err_msg = [
            "Update preflight encountered a case it did not expect.",
            "book_id_item_id_map: \n{}\n".format(pprint.pformat(book_id_item_id_map)),
            "link_type: {}".format(link_type),
            "link_vals: {}".format(pprint.pformat(link_vals)),
        ]
        if not isinstance(link_vals, set):
            err_msg.append("link vals are supposed to be a set and aren't")

        return "\n".join(err_msg)

    def update_precheck(
        self, book_id_item_id_map: dict[SrcTableID, dict[str, Iterable[DstTableID]]], id_map_update: dict[DstTableID, T]
    ) -> None:
        """
        Check that an update is of a valid form before writing it out to the cache and the database.

        Called when you know the ids you want to assign to the book after the update.
        Checks those ids are valid.
        No changes will be made to the :param book_id_item_id_map: (e.g. if the map is valued with tuples - not lists
        as expected - this will not be corrected.
        Raised InvalidCacheUpdate if the cache update is invalid in some way.
        :param book_id_item_id_map: Keyed with the ids of the books to update and valued with the
        :param id_map_update:
        :return status:
        """
        for book_id, book_vals in iteritems(book_id_item_id_map):

            # We're clearing all entries for the given field
            if book_vals is None:
                continue

            # Check that the new valued for the book_id are ordered - in some way
            if not isinstance(book_vals, dict):
                raise InvalidCacheUpdate("Map needs to be keyed with a dict")

            # Check that all the ids are valid
            for link_type, link_vals in iteritems(book_vals):
                if link_vals is None:
                    continue

                # If the map is valued with a single integer, check to see if it is valid and then continue if it is
                if isinstance(link_vals, int):
                    if not (link_vals in self.id_map or link_vals in id_map_update):
                        raise InvalidCacheUpdate("Cannot match update id - cannot preform update as cannot link")
                    continue

                # Todo: Should be checking the type of the vals as well - in this case and in PriorityTyped
                for item_id in link_vals:
                    # Does not claim to be an id on the system - so doesn't current need to match - entry will be
                    # created
                    if isinstance(item_id, basestring):
                        continue

                    # Claims to be a valid id - so there must be a match
                    if not (item_id in self.id_map or item_id in id_map_update):
                        raise InvalidCacheUpdate("Cannot match update id - cannot preform update as cannot link")

    def update_cache(
        self, book_id_val_map: dict[SrcTableID, dict[str, Iterable[DstTableID]]], id_map: dict[DstTableID, T] = None
    ) -> bool:
        """
        Preforms a cache update on the internal maps stored in the database.

        :param book_id_val_map:
        :param id_map:
        :return status: Did the update go through to the cache succesfully?
        """
        self.update_precheck(book_id_item_id_map=book_id_val_map, id_map_update=id_map)
        # Todo: Need to do col_book_map updates

        # book_id_val_map should be keyed with the id of the book in question and valued with the new id of the target
        # - if the target is already linked to the book then promote it
        # - if the target is not linked to the book then link it
        for book_id, book_val in iteritems(book_id_val_map):

            if book_val is None:
                for lt in self.known_link_types:
                    self.book_col_map[lt][book_id] = set()
                continue

            if isinstance(book_val, str):
                raise NotImplementedError("Bad update - cannot determine type to update from update dict")

            elif isinstance(book_val, (dict, OrderedDict)):
                for val_type, val_val in iteritems(book_val):
                    if isinstance(val_val, str):
                        self.book_col_map[val_type][book_id].add(val_val)
                    elif isinstance(val_val, (set, frozenset)):
                        self.book_col_map[val_type][book_id] = set(val_val)
                    else:
                        raise NotImplementedError("Couldn't pass individual book value")

            else:
                raise NotImplementedError("Cannot update - update dict badly formed")

            # Add the item to the front of the linked book ids
            # self.book_col_map[book_id].add(book_val)
            # self.col_book_map[book_val].add(book_id)

        # id_map will just have the updates applied to it - no need to do more
        if id_map is not None:
            self.id_map.update(id_map)

    def internal_update_cache(
        self, book_id_item_id_map: dict[SrcTableID, dict[str, Iterable[DstTableID]]], id_map_update: dict[DstTableID, T]
    ) -> bool:
        """
        It sometimes makes sense to do an update to the cache to gather information before updating the database.

        :param book_id_item_id_map:
        :param id_map_update:
        :return:
        """
        self.id_map.update(id_map_update)

        # Update the book -> col and col -> book maps that form the cache
        deleted = set()
        updated = defaultdict(dict)

        for book_id, link_dict in iteritems(book_id_item_id_map):

            if link_dict is None:

                old_book_data = self.book_data(book_id=book_id)

                # We're nullifying the entries for the given book - first nullifying the book_col_links
                for known_link_type in self.known_link_types:
                    self.book_col_map[known_link_type][book_id] = set()

                for old_link_type, old_link_values in iteritems(old_book_data):
                    for item_id in old_link_values:
                        self.col_book_map[old_link_type][item_id].remove(book_id)

                deleted.add(book_id)
                continue

            # If the link_dict has no content, then this position should never have been reached in the first place
            assert link_dict, "Cannot preform update - link_dict malformed: {}".format(link_dict)

            old_book_data = self.book_data(book_id=book_id)

            for link_type, item_ids in iteritems(link_dict):

                # Remove the col_book_map links
                try:
                    old_book_item_ids = old_book_data[link_type]
                except KeyError:
                    old_book_item_ids = set()
                for item_id in old_book_item_ids:
                    self.col_book_map[link_type][item_id].discard(book_id)

                if item_ids is None:

                    # Todo: Check seen_link_types is being properly updated when new link types are introduced by update
                    # If the link type is not known to the system, there is no need to make changes
                    if link_type not in self.seen_link_types:
                        continue

                    # Clear the book_col_map links
                    self.book_col_map[link_type][book_id] = set()

                    # Note the change in the updates which will be written out to the database
                    updated[book_id][link_type] = None

                    continue

                # In the case where there's an element in the newly set ids which is already linked to the book with a
                # different role, that element needs to be removed
                for lt in self.known_link_types:
                    for niid in item_ids:
                        self.book_col_map[lt][book_id].discard(niid)
                        self.col_book_map[lt][niid].discard(book_id)

                # Write the new links back out to the cache
                self.book_col_map[link_type][book_id] = item_ids
                for niid in item_ids:
                    self.col_book_map[link_type][niid].add(book_id)

                # Note the change in the updates which will be written out to the database
                updated[book_id][link_type] = item_ids

        return updated, deleted


# Todo: Tests with two sets with the same ids and different types - should always fail
