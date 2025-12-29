import pprint

from collections import defaultdict
from copy import deepcopy

from typing import TypeVar, Optional, Union, Any, Iterable, Mapping

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


class CalibrePriorityTypedOneToManyTable(CalibreOneToManyTable[T]):
    """
    For the case where one, and only one, book is linked to many items and the items are linked to no other books.

    Priority and type information is recorded - so items can be split down into types and ordered within them (and
    overall) using the priority column.
    """

    _priority = True
    _typed = True

    def __init__(
        self, name: str, metadata: MetadataDict, link_table: Optional[InterLinkTableName] = None, custom: bool = False
    ) -> None:
        """
        Preform startup for a PriorityTypedOneToMany table.

        This is a table where one book is linked to many items, but with the concept of type and priority.
        :param name:
        :param metadata:
        :param link_table:
        :param custom:
        """
        super(CalibrePriorityTypedOneToManyTable, self).__init__(
            name=name, metadata=metadata, link_table=link_table, custom=custom
        )

        # MAPS
        # Keyed with the book id, then valued with a dictionary keyed with the type and finally valued with a list
        # which holds the items linked to the given book in priority order
        self.book_col_map = self._book_col_map_factory()
        # Keyed with the item id and valued with the result from the books table - which should be a single item
        # (as there is only one book linked to each item)
        self.col_book_map = self._col_book_map_factory()

        self.seen_link_types = set()

    def book_data(
        self, book_id: SrcTableID, type_filter: Optional[str] = None
    ) -> Union[dict[SrcTableID, dict[str, list[DstTableID, ...]]], list[DstTableID, ...]]:
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
            assert type_filter is not None and type_filter in self.book_col_map.keys()
            return deepcopy(self.book_col_map[type_filter][book_id])

    def vals_book_data(
        self, book_id: SrcTableID, type_filter: Optional[str] = None
    ) -> tuple[dict[str, list[T, ...]], list[T, ...]]:
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

    def _ids_to_vals(self, ids_container: Iterable[DstTableID]) -> list[T, ...]:
        """
        Takes an id container and returns all the values for that container.

        :param ids_container:
        :return:
        """
        return [self.id_map[id_] for id_ in ids_container]

    def _book_col_map_factory(self) -> dict[SrcTableID, dict[str, list[DstTableID]]]:
        """
        Data structure to hold the book-col relations.

        This is a PriorityTypedOneToMany table - so it has the concept of type and order.
        Thus this structure is a dict of dicts.
        Keyed with the book_id, valued with a dict of types, which is, in turn, valued with a list of item ids.
        :return:
        """
        return defaultdict(self._type_list_container)

    @staticmethod
    def _col_book_map_factory() -> dict[DstTableID, SrcTableID]:
        """
        Stores the column-book maps.

        This is just a one to one relation - item to book.
        :return:
        """
        return dict()

    @staticmethod
    def _type_list_container() -> dict[str, list[DstTableID]]:
        """
        Part of the nested default dict infrastructure.

        :return:
        """
        return defaultdict(list)

    @staticmethod
    def _type_container() -> dict[str, set[DstTableID]]:
        """
        Part of the default dict infrastructure.

        :return:
        """
        return defaultdict(set)

    def read_maps(self, db, type_filter: Optional[str] = None) -> None:
        """
        Preform a read out of the database and into the internal caches

        :param db:
        :param type_filter:
        :return:
        """
        link_table_book_id = self.link_table_bt_id_column
        link_table_other_id = self.link_table_table_id_column

        link_type_col = db.driver_wrapper.get_link_column(
            table1="titles", table2=self.metadata["table"], column_type="type"
        )
        link_priority_col = db.driver_wrapper.get_link_column(
            table1="titles", table2=self.metadata["table"], column_type="priority"
        )

        stmt = "SELECT {0}, {1}, {2} FROM {3} ORDER BY {4} DESC;".format(
            link_table_book_id,
            link_table_other_id,
            link_type_col,
            self.link_table,
            link_priority_col,
        )

        seen_link_types = self.seen_link_types
        col_book_map = self.col_book_map
        book_col_map = self.book_col_map

        for book_id, item_id, link_type in db.driver_wrapper.execute(stmt):

            # Construct the maps linking the entries to the books and visa versa
            book_col_map[link_type][book_id].append(item_id)
            col_book_map[item_id] = book_id

            # Note the link type - as it might not have been seen before
            seen_link_types.add(link_type)

        for book_id in self.seen_book_ids:
            for link_type in self.seen_link_types:
                if book_id not in book_col_map[link_type]:
                    book_col_map[link_type][book_id] = []

        for item_id in self.seen_item_ids:
            if item_id not in col_book_map:
                col_book_map[item_id] = None

    def update_preflight_unique(
        self, book_id_item_id_map: dict[SrcTableID, DstTableID], id_map_update=None, dirtied=None
    ):
        """
        Bring the update into a form where it can be more easily written out to the database.

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
                    elif isinstance(link_vals, list):
                        clean_book_id_item_id_map[book_id][link_type] = book_update_dir[link_type]
                    elif isinstance(link_vals, (basestring, int)):
                        old_item_ids = self.book_col_map[link_type][book_id]
                        if link_vals not in old_item_ids:
                            clean_book_id_item_id_map[book_id][link_type] = [
                                book_update_dir[link_type],
                            ] + self.book_col_map[link_type][book_id]
                        else:
                            old_item_ids.remove(book_update_dir[link_type])
                            clean_book_id_item_id_map[book_id][link_type] = [
                                book_update_dir[link_type],
                            ] + old_item_ids
                    else:
                        raise NotImplementedError

                else:

                    if link_vals is None:
                        # Trying to nullify something without an entry - so just skip it
                        continue
                    elif isinstance(link_vals, list):
                        clean_book_id_item_id_map[book_id][link_type] = link_vals
                    elif isinstance(link_vals, (basestring, int)):
                        clean_book_id_item_id_map[book_id][link_type] = [
                            link_vals,
                        ]
                    else:
                        raise NotImplementedError

        return clean_book_id_item_id_map, id_map_update

    def update(
        self,
        book_id_val_map: dict[SrcTableID, T],
        db,
        id_map: Optional[dict[DstTableID, T]] = None,
        allow_case_change: bool = False,
    ) -> None:
        """
        Preform an update to both the cache and the db.

        book_id_val_map - Update should be of the form of a dictionary - keyed with the id of the book.
        valued - None -removes all the entries, of all types, for the given book
               - type dict - Keyed with the link_type - as a string -
                             valued with None - then removes all links OF THIS TYPE
                             valued with a list - updates all the ids of that type linked to the book to be that list
                             type does not appear - no changes will be made for that type

        :param book_id_val_map:
        :param db:
        :param id_map:
        :param allow_case_change:
        :return None: All changes are made internally
        """
        return super(CalibrePriorityTypedOneToManyTable, self).update(
            book_id_val_map, db, id_map=id_map, allow_case_change=allow_case_change
        )

    # Todo: Needs to be merged with update_precheck and generally cleaned up
    def update_precheck(
        self, book_id_item_id_map: dict[SrcTableID, DstTableID], id_map_update: dict[DstTableID, T]
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
        self,
        book_id_item_id_map: dict[SrcTableID, DstTableID],
        id_map_update: Optional[dict[DstTableID, T]] = None,
        dirtied: set[SrcTableID] = None,
    ) -> tuple[dict[SrcTableID, DstTableID], dict[DstTableID, T]]:
        """
        Processes the book_id_item_id_map to standardize it before updating the db and the cache.

        :param book_id_item_id_map:
        :param id_map_update:
        :return:
        """
        clean_book_id_item_id_map = self._book_col_map_factory()

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
                    elif isinstance(link_vals, list):
                        clean_book_id_item_id_map[book_id][link_type] = book_update_dir[link_type]
                    elif isinstance(link_vals, (basestring, int)):
                        old_item_ids = self.book_col_map[link_type][book_id]
                        if link_vals not in old_item_ids:
                            clean_book_id_item_id_map[book_id][link_type] = [
                                book_update_dir[link_type],
                            ] + self.book_col_map[link_type][book_id]
                        else:
                            old_item_ids.remove(book_update_dir[link_type])
                            clean_book_id_item_id_map[book_id][link_type] = [
                                book_update_dir[link_type],
                            ] + old_item_ids
                    else:
                        raise NotImplementedError

                else:

                    if link_vals is None:
                        # Trying to nullify something without an entry - so just skip it
                        continue
                    elif isinstance(link_vals, list):
                        clean_book_id_item_id_map[book_id][link_type] = link_vals
                    elif isinstance(link_vals, (basestring, int)):
                        clean_book_id_item_id_map[book_id][link_type] = [
                            link_vals,
                        ]
                    else:
                        raise NotImplementedError

        return clean_book_id_item_id_map, id_map_update

    def update_db(
        self, book_id_to_val_map: Mapping[SrcTableID, Union[T, Iterable[T]]], db, allow_case_change: bool = False
    ) -> bool:
        """
        Actually write an update out to the database.

        :param book_id_to_val_map:
        :param db:
        :param allow_case_change:
        :return:
        """

        return super(CalibrePriorityTypedOneToManyTable, self).update_db(book_id_to_val_map, db, allow_case_change)

    def cache_update_precheck(
        self, book_id_val_map: dict[SrcTableID, DstTableID], id_map: Optional[dict[DstTableID:T]] = None
    ) -> None:
        """
        Check the update for a cache before applying it to the stored data.

        :param book_id_val_map:
        :param id_map:
        :return None: An error is raised if the update is not valid
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
                    raise InvalidCacheUpdate("book_id_val_map contains repeated elements")

    def strict_cache_update_precheck(
        self, book_id_val_map: dict[SrcTableID, DstTableID], id_map: Optional[dict[DstTableID, T]] = None
    ) -> None:
        """
        Check the update for a cache before applying it to the stored data.

        :param book_id_val_map:
        :param id_map:
        :return:
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

                if not isinstance(link_vals, list):
                    raise InvalidCacheUpdate(self._link_vals_bad_form(link_vals))
                for link_val in link_vals:
                    assert isinstance(link_val, int), "link_vals contained a non int"

                old_vals_set = deepcopy(vals_set)
                vals_set = vals_set.union(set(link_vals))
                if len(vals_set) - len(old_vals_set) != len(link_vals):
                    raise InvalidCacheUpdate("book_id_val_map contains repeated elements")

    def update_cache(
        self,
        book_id_val_map: dict[SrcTableID, dict[str, list[DstTableID, ...]]],
        id_map: Optional[dict[DstTableID, T]] = None,
    ) -> None:
        """
        Check the cache update and then preform it.

        By this point in the update process the update dicts should be pretty much just read to write out.
        :param book_id_val_map: Keyed with a book id, valued with a dict, keyed with the type and valued with a book_id
        :param id_map:
        :return None:
        """

        self.strict_cache_update_precheck(book_id_val_map, id_map)

        self.internal_update_cache(book_id_val_map, id_map)

    def internal_update_cache(
        self,
        book_id_item_id_map: dict[SrcTableID, dict[str, list[DstTableID, ...]]],
        id_map_update: dict[SrcTableID, T],
    ) -> tuple[dict[SrcTableID, dict[str, list[DstTableID, ...]]], set[SrcTableID]]:
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
                    self.book_col_map[known_link_type][book_id] = []

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
                    self.book_col_map[link_type][book_id] = []

                    # Remove the col_book_map links
                    try:
                        old_book_item_ids = old_book_data[link_type]
                    except KeyError:
                        old_book_item_ids = []
                    for item_id in old_book_item_ids:
                        try:
                            self.col_book_map[item_id] = None
                        except ValueError:
                            pass

                    # Note the change in the updates which will be written out to the database
                    updated[book_id][link_type] = None

                    continue

                # Getting tuples passed into the system - need to standardize on lists
                item_ids = list(item_ids)

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
                            self.book_col_map[lt][existing_book_id].remove(item_id)
                        except ValueError:
                            continue

                # Remove the col_book_map links from the old data
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
                            self.book_col_map[lt][book_id].remove(niid)
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

    def update_precheck_unique(
        self,
        book_id_item_id_map: dict[SrcTableID, dict[str, list[DstTableID, ...]]],
        id_map_update: dict[SrcTableID, T],
    ) -> tuple[dict[SrcTableID, dict[str, list[DstTableID, ...]]], set[SrcTableID]]:
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

    @staticmethod
    def _pt_malformed_update_error(book_id_item_id_map: Any, book_id: Any, book_vals: Any) -> str:
        """
        Err msg.

        The update dict about to be applied to the cache and the database is malformed.
        """
        err_msg = [
            "If this point is reached update must be valued with a list",
            "book_id_item_id_map: \n{}\n".format(pprint.pformat(book_id_item_id_map)),
            "book_id: {}".format(book_id),
            "book_vals: {}".format(book_vals),
        ]
        return "\n".join(err_msg)

    @staticmethod
    def _link_vals_bad_form(link_vals: Any) -> str:
        """
        Err msg.

        :param link_vals:
        :return:
        """
        err_msg = [
            "book_id_val_map form is not valid",
            "link_vlas: {}".format(link_vals),
        ]
        return "\n".join(err_msg)


#
# ----------------------------------------------------------------------------------------------------------------------
