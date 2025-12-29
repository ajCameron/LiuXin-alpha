"""
A TypedManyToOne table which stores type information for the link with the link itself.
"""

from collections import defaultdict
from copy import deepcopy

from typing import TypeVar, Optional, Union, Iterable, Any

from LiuXin.databases.caches.calibre.tables.one_many_tables.one_to_many_table import CalibreOneToManyTable
from LiuXin.databases.db_types import SrcTableID, DstTableID, MetadataDict, InterLinkTableName

from LiuXin.exceptions import InvalidCacheUpdate

from LiuXin.utils.lx_libraries.liuxin_six import iteritems

from past.builtins import basestring


T = TypeVar("T")


# ----------------------------------------------------------------------------------------------------------------------
#
# - ONE TO MANY TABLES


# Todo: Need to be able to deal with being fed a custom column
from LiuXin.utils.lx_libraries.liuxin_six import dict_iteritems as iteritems


# Todo: Should detected and fail if the link this is being asked to represent is not of the right type
class CalibreTypedOneToManyTable(CalibreOneToManyTable[T]):
    """
    For the case where one, and only one, book is linked to many items and the items are linked to no other books.

    Type information is recorded so the items can be partitioned down based on their type.
    """

    _priority: bool = False
    _typed: bool = True

    def __init__(
        self, name: str, metadata: MetadataDict, link_table: Optional[InterLinkTableName] = None, custom: bool = False
    ) -> None:
        """
        Preform startup for typed OneToMany table.

        :param name:
        :param metadata:
        :param link_table:
        :param custom:
        """
        super(CalibreTypedOneToManyTable, self).__init__(
            name=name, metadata=metadata, link_table=link_table, custom=custom
        )

        # MAPS
        # book_col_map - Keyed with the book id, then valued with a dictionary keyed with the type and finally value
        #                with a set of item values
        # col_book_map - Keyed with the item id and valued with the result from the books table - which should be a
        #                single item (as there is only one book linked to each item)

        self.seen_link_types = set()

    def _book_col_map_factory(self) -> dict[SrcTableID, dict[str, set[DstTableID]]]:
        """
        Used to store the typed info for the table.

        :return:
        """
        return defaultdict(self._type_container)

    @staticmethod
    def _col_book_map_factory() -> dict[DstTableID, SrcTableID]:
        """
        Does not store type info this way round.

        :return:
        """
        return dict()

    # Todo: Move to the utility mixin
    @staticmethod
    def _type_container() -> dict[str, set[DstTableID]]:
        """
        Used to store the types and the ids they're linked to

        :return:
        """
        return defaultdict(set)

    def read_maps(self, db, type_filter: Optional[str] = None) -> None:
        """
        Preform a read out of the database and into the internal caches.

        :param db:
        :param type_filter:
        :return None: All changes are made internally.
        """
        link_table_book_id = self.link_table_bt_id_column
        link_table_other_id = self.link_table_table_id_column

        link_type_col = db.driver_wrapper.get_link_column(
            table1="titles", table2=self.metadata["table"], column_type="type"
        )
        stmt = "SELECT {0}, {1}, {2} FROM {3};".format(
            link_table_book_id, link_table_other_id, link_type_col, self.link_table
        )

        seen_link_types = self.seen_link_types
        col_book_map = self.col_book_map
        book_col_map = self.book_col_map

        for book_id, item_id, link_type in db.driver_wrapper.execute(stmt):
            # Construct the maps which link the entries and the books
            book_col_map[link_type][book_id].add(item_id)
            col_book_map[item_id] = book_id

            # Note any additional types of link which have been seen
            seen_link_types.add(link_type)

        for book_id in self.seen_book_ids:
            for link_type in self.seen_link_types:
                if book_id not in book_col_map[link_type]:
                    book_col_map[link_type][book_id] = set()

        for item_id in self.seen_item_ids:
            if item_id not in col_book_map:
                col_book_map[item_id] = None

    def book_data(
        self, book_id: SrcTableID, type_filter: Optional[str] = None
    ) -> tuple[dict[str, set[DstTableID, ...]], set[DstTableID, ...]]:
        """
        Returns the book data for the given record - in ids form. If you want the actual values then use vals_book_data

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

    def vals_book_data(
        self, book_id: SrcTableID, type_filter: Optional[str] = None
    ) -> tuple[dict[str, set[DstTableID, ...]], set[DstTableID, ...]]:
        """
        Returns the book data for the given record - in the form of vals. If you want ids then use book_data

        :param book_id:
        :param type_filter:
        :return:
        """
        if type_filter is None:
            book_ids_dict = self.book_data(book_id=book_id, type_filter=None)
            return {lt: self._ids_to_vals(lv) for lt, lv in iteritems(book_ids_dict)}
        else:
            return self._ids_to_vals(self.book_data(book_id=book_id, type_filter=type_filter))

    def cache_update_precheck(
        self, book_id_val_map: dict[SrcTableID, DstTableID], id_map: Optional[dict[DstTableID, T]] = None
    ) -> None:
        """
        Check the update for a cache before applying it to the stored data.

        :param book_id_val_map:
        :param id_map:
        :return None: An error is raised if the update is not valid.
        """
        # Used to check that the vals being assigned to the books have no overlap - this is meant to be a one to many
        # link
        vals_set = set()

        for book_id, book_vals in iteritems(book_id_val_map):

            if not isinstance(book_id, int):
                raise InvalidCacheUpdate("book_id_val_map form is not valid")

            if book_vals is None:
                continue

            if not isinstance(book_vals, dict):
                raise InvalidCacheUpdate("book_id_val_map form is not valid")

            for link_type, link_vals in iteritems(book_vals):
                # Nullification for the individual link type
                if link_vals is None:
                    continue

                # Todo: This really should fail as well
                if not isinstance(link_type, basestring):
                    raise InvalidCacheUpdate("book_id_val_map form is not valid")

                if not isinstance(link_vals, list):
                    raise InvalidCacheUpdate(self._link_vals_bad_form(link_vals))
                for link_val in link_vals:
                    assert isinstance(link_val, int), "link_vals contained a non int"

                old_vals_set = deepcopy(vals_set)
                vals_set = vals_set.union(set(link_vals))
                if len(vals_set) - len(old_vals_set) != len(link_vals):
                    raise InvalidCacheUpdate("book_id_val_map contains repeated elements in boook {}".format(book_id))

    def update_precheck_unique(
        self,
        book_id_item_id_map: dict[SrcTableID, Optional[Union[DstTableID, str, Iterable[DstTableID]]]],
        id_map_update: dict[DstTableID, Optional[T]],
    ) -> None:
        """
        Check that an update is of a valid form before writing it out to the cache and the database.

        Called when you know the ids you want to assign to the book after the update. Checks those ids are valid.
        No changes will be made to the :param book_id_item_id_map: (e.g. if the map is valued with tuples - not lists
        as expected - this will not be corrected.
        Raised InvalidCacheUpdate if the cache update is invalid in some way.

        :param book_id_item_id_map: Keyed with the ids of the books to update and valued with the
        :param id_map_update:
        :return None: An error is raised if the update is malformed.
        """
        seen_ids = set()
        seen_strs = set()
        for book_id, book_vals_dict in iteritems(book_id_item_id_map):

            if book_vals_dict is None:
                continue

            for link_type, book_vals in iteritems(book_vals_dict):

                if isinstance(book_vals, (int, basestring)) or book_vals is None:
                    continue

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

    def update_precheck(
        self,
        book_id_item_id_map: dict[SrcTableID, Optional[Union[DstTableID, str, Iterable[DstTableID]]]],
        id_map_update: dict[DstTableID, Optional[T]],
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
        pass

    def update_preflight(
        self, book_id_item_id_map, id_map_update=None, dirtied=None
    ) -> tuple[dict[SrcTableID, dict[str, set[DstTableID, ...]]], dict[DstTableID, Optional[T]]]:
        """
        Bring the update maps into standard form before attempting to write them out to the database.

        The items being linked to need not be unique (or, rather, it doesn't matter if they're unique).
        :param book_id_item_id_map:
        :param id_map_update:
        :param dirtied:
        :return:
        """

        id_map_update = dict() if id_map_update is None else id_map_update
        clean_book_id_item_id_map = defaultdict(dict)

        for book_id, type_dict in iteritems(book_id_item_id_map):
            # Unmatched string - just add it into the sets
            if type_dict is None:
                clean_book_id_item_id_map[book_id] = None

            elif isinstance(type_dict, dict):
                for link_type, link_vals in iteritems(type_dict):

                    if link_vals is None:
                        clean_book_id_item_id_map[book_id][link_type] = None

                    elif isinstance(link_vals, (int, basestring)):
                        clean_book_id_item_id_map[book_id][link_type] = {
                            link_vals,
                        }.union(self.book_col_map[link_type][book_id])

                    elif isinstance(link_vals, set):
                        clean_book_id_item_id_map[book_id][link_type] = link_vals

                    else:
                        raise NotImplementedError(self._type_dict_form_not_good(type_dict))
            else:
                raise NotImplementedError

        return clean_book_id_item_id_map, id_map_update

    def update_preflight_unique(self, book_id_item_id_map, id_map_update=None, dirtied=None):
        """
        Bring the update into a form where it can be more easily written out to the database.

        In the case where the values being linked to must be unique.
        :param book_id_item_id_map:
        :param id_map_update:
        :return:
        """
        clean_book_id_item_id_map = defaultdict(dict)

        for book_id, book_update_dir in iteritems(book_id_item_id_map):

            if book_update_dir is None:
                clean_book_id_item_id_map[book_id] = None
                continue

            if not isinstance(book_update_dir, dict):
                raise NotImplementedError

            for link_type, link_vals in iteritems(book_update_dir):

                if link_type in self.seen_link_types:

                    # Use the original values from the update
                    if link_vals is None:
                        clean_book_id_item_id_map[book_id][link_type] = None
                        continue

                    elif isinstance(link_vals, set):
                        clean_book_id_item_id_map[book_id][link_type] = book_update_dir[link_type]

                    elif isinstance(link_vals, (basestring, int)):
                        old_item_ids = self.book_col_map[link_type][book_id]
                        if link_vals not in old_item_ids:
                            clean_book_id_item_id_map[book_id][link_type] = {
                                book_update_dir[link_type],
                            }.union(self.book_col_map[link_type][book_id])
                        else:
                            old_item_ids.remove(book_update_dir[link_type])
                            clean_book_id_item_id_map[book_id][link_type] = {
                                book_update_dir[link_type],
                            }.union(old_item_ids)
                    else:
                        raise NotImplementedError

                else:

                    if link_vals is None:
                        # Trying to nullify something without an entry - so just skip it
                        continue
                    elif isinstance(link_vals, set):
                        clean_book_id_item_id_map[book_id][link_type] = link_vals
                    elif isinstance(link_vals, (basestring, int)):
                        clean_book_id_item_id_map[book_id][link_type] = {
                            link_vals,
                        }
                    else:
                        raise NotImplementedError

        return clean_book_id_item_id_map, id_map_update

    @staticmethod
    def _type_dict_form_not_good(type_dict: Any) -> str:
        """
        Error message.

        :return:
        """
        err_msg = [
            "The form of the type dictionary couldn't be parsed",
            "type_dict: {}".format(type_dict),
        ]
        return "\n".join(err_msg)

    def _ids_to_vals(self, ids_container: set[DstTableID, ...]) -> set[T, ...]:
        """
        Takes a set of ids and turns it into a set of values from the table.

        :param ids_container:
        :return:
        """
        return set(self.id_map[id_] for id_ in ids_container)

    def update_cache(self, book_id_val_map: dict[SrcTableID, dict[str, DstTableID]], id_map: dict[DstTableID, T]=None) -> None:
        """
        Write an update to the cache out to the internal data store.

        :param book_id_val_map:
        :param id_map:
        :return:
        """

        self.internal_update_cache(book_id_val_map, id_map)

    # Todo: All these updates should probably produce a dataclass
    def internal_update_cache(
        self, book_id_item_id_map: dict[SrcTableID, dict[str, DstTableID]], id_map_update: dict[DstTableID, T]
    ) -> tuple[dict[SrcTableID, dict[str, DstTableID]], set[SrcTableID]]:
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
                for known_link_type in self.seen_link_types:
                    self.book_col_map[known_link_type][book_id] = set()

                for old_link_type, old_link_values in iteritems(old_book_data):
                    for item_id in old_link_values:
                        self.col_book_map[item_id] = None

                deleted.add(book_id)
                continue

            # If the link_dict has no content, then this position should never have been reached in the first place
            assert link_dict, "Cannot preform update - link_dict malformed: {}".format(link_dict)

            old_book_data = deepcopy(self.book_data(book_id=book_id))

            # Need to preform an update
            for link_type, item_ids in iteritems(link_dict):

                if item_ids is None:

                    # Todo: Check seen_link_types is being properly updated when new link types are introduced by update
                    # If the link type is not known to the system, there is no need to make changes
                    if link_type not in self.seen_link_types:
                        continue

                    # Clear the book_col_map links
                    self.book_col_map[link_type][book_id] = set()

                    # Remove the col_book_map links
                    try:
                        old_book_item_ids = old_book_data[link_type]
                    except KeyError:
                        old_book_item_ids = set()
                    for item_id in old_book_item_ids:
                        try:
                            self.col_book_map[item_id] = None
                        except ValueError:
                            pass

                    # Note the change in the updates which will be written out to the database
                    updated[book_id][link_type] = None

                    continue

                # Getting tuples passed into the system - need to standardize on lists
                item_ids = set(item_ids)

                # Remove links between the new item ids and any existing books
                # This is because links may have been repointed (in type or to different books) by the update
                # E.g. changing the type of an item associated with a book or moving an item from one book to another
                # (possibly also with a change in link type)
                # Todo: Should be able to use the remove_item method for this - which can then be optimized
                # Todo: IN ALL PLACES - this should be affecting the dirtied!
                for item_id in item_ids:
                    try:
                        existing_book_id = self.col_book_map[item_id]
                    except KeyError:
                        continue
                    if existing_book_id is None:
                        continue
                    for lt in self.book_col_map:
                        try:
                            self.book_col_map[lt][existing_book_id].discard(item_id)
                        except ValueError:
                            continue

                # Remove the col_book_map links
                try:
                    old_book_item_ids = old_book_data[link_type]
                except KeyError:
                    old_book_item_ids = []
                for item_id in old_book_item_ids:
                    self.col_book_map[item_id] = None

                # In the case where there's an element in the newly set ids which is already linked to the book with a
                # different role, that element needs to be removed
                for lt in self.seen_link_types:
                    for niid in item_ids:
                        try:
                            self.book_col_map[lt][book_id].discard(niid)
                        except ValueError:
                            pass
                        try:
                            self.col_book_map[niid] = None
                        except ValueError:
                            pass

                # Write the new links back out to the cache
                self.book_col_map[link_type][book_id] = item_ids
                for niid in item_ids:
                    self.col_book_map[niid] = book_id

                # Note the change in the updates which will be written out to the database
                updated[book_id][link_type] = item_ids

        return updated, deleted
