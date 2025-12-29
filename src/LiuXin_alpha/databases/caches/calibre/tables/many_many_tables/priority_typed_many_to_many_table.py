"""
PriorityTypedManyToManyTables have both a priority and a type - so entries are in a certain order and with a type.

E.g. "titles" linked to "creators"
Two pieces of information need to be recorded
 - what type of creator is linked to the title
 - what order should the creator appear in the title
"""

from collections import defaultdict, OrderedDict
from copy import deepcopy

from typing import TypeVar, Optional, Union, Any

from LiuXin.databases.db_types import (
    MetadataDict,
    InterLinkTableName,
    SrcTableID,
    DstTableID,
)
from LiuXin.databases.caches.calibre.tables.many_many_tables.many_to_many_table import CalibreManyToManyTable
from LiuXin.databases.caches.calibre.tables.many_many_tables.priority_many_to_many_table import (
    CalibrePriorityManyToManyTable,
)

from LiuXin.exceptions import (
    InvalidCacheUpdate,
)
from LiuXin.utils.lx_libraries.liuxin_six import (
    dict_iteritems as iteritems,
)
from LiuXin.utils.logger import default_log

# Py2/Py3 compatibility layer
from LiuXin.utils.lx_libraries.liuxin_six import iterkeys

from past.builtins import basestring

T = TypeVar("T")


class CalibrePriorityTypedManyToManyTable(CalibreManyToManyTable):
    """
    ManyToMany table with a priority for ordering the items the books are linked to and a type to that link.

    This partitions the results down into categories.
    """

    _priority: bool = True
    _typed: bool = False

    def __init__(
        self, name: str, metadata: MetadataDict, link_table: InterLinkTableName = None, custom: bool = False
    ) -> None:
        """
        Initialize this table.

        Load is not preformed at this point.
        The table is also not associated with a database.
        :param name:
        :param metadata:
        :param link_table:
        :param custom:
        """
        super(CalibrePriorityTypedManyToManyTable, self).__init__(
            name=name, metadata=metadata, link_table=link_table, custom=custom
        )

    # Todo: This is a really good idea - use it everywhere
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

    # Todo: type_filter should be one of the permitted types in the main table - implement that
    def get_priority_subtable(self, db, type_filter) -> CalibrePriorityManyToManyTable:
        """
        Returns a subtable of the current table with data restricted to a specific type filter.
        Table returned would be a ManyToManyTable - with no type information stored (as that has already been filtered
        out).
        :param db: The database which is currently being operated on
        :param type_filter: Used to construct the PriorityManyToMany by applying a filter for this type.
        :return:
        """
        return CalibrePriorityManyToManyTable.from_typed_priority_table(
            db=db, original_table=self, type_filter=type_filter
        )

    def _book_col_map_factory(self) -> dict[SrcTableID, dict[str, list[DstTableID]]]:
        """
        Needed to trick default dicts into producing the right container.

        :return:
        """
        return defaultdict(self._default_dict_list_container)

    def _col_book_map_factory(self) -> dict[DstTableID, dict[str, list[SrcTableID, ...]]]:
        """
        Needed to trick default dicts into producing the right container.

        :return:
        """
        return defaultdict(self._default_dict_list_container)

    @staticmethod
    def _default_dict_list_container() -> dict[str, list[Union[SrcTableID, DstTableID], ...]]:
        """
        Trick needed to construct nested defualtdicts.

        :return:
        """
        return defaultdict(list)

    def book_data(self, book_id: SrcTableID, type_filter: Optional[str] = None) -> Any:
        """
        Returns the book data for the given record.

        :param book_id: The id of the book to retrieve the data for
        :param type_filter: A string type filter - must be in the known types for the table
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

    def vals_book_data(self, book_id: SrcTableID, type_filter: Optional[str] = None) -> Any:
        """
        Returns the book data for the given record - in the form of vals. If you want ids then use book_data

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

    def item_data(self, item_id: DstTableID, type_filter: Optional[str] = None) -> Any:
        """
        Returns the book data for the given record.

        :param item_id:
        :param type_filter:
        :return:
        """
        if type_filter is None:
            book_data_dict = dict()
            for known_type in self.col_book_map.keys():
                book_data_dict[known_type] = self.col_book_map[known_type][item_id]
            return deepcopy(book_data_dict)
        else:
            assert type_filter in self.col_book_map.keys()
            return deepcopy(self.col_book_map[type_filter][item_id])

    # Todo: This should be in the base methods somewhere
    # Todo: Rename to tell the user the type of return you get from this table
    def _ids_to_vals(self, ids_container: list[DstTableID, ...]) -> list[T, ...]:
        """
        Takes a container of val ids and turns it into a container of vals.

        :param ids_container: In this case a list
        :return:
        """
        return [self.id_map[id_] for id_ in ids_container]

    # Todo: read_maps is ambiguous - read_link_maps?
    def read_maps(self, db, type_filter: Optional[str] = None) -> None:
        """
        Preform a read into the internal caches.

        :param db:
        :param type_filter: Optional string to serve as a type filter when preforming the read.
        :return:
        """
        assert type_filter is None, "type_filter is no longer in use"

        # Todo: Rename this to something more useful - characterize_link_table
        self.set_link_table(db=db, set_type=True)

        book_col_map = self.book_col_map
        col_book_map = self.col_book_map

        seen_books = self.seen_books
        known_link_types = self.known_link_types

        stmt = "SELECT {}, {}, {} FROM {} ORDER BY {} DESC;".format(
            self.link_table_bt_id_column,
            self.link_table_table_id_column,
            self.link_table_type,
            self.link_table,
            self.link_table_priority_col,
        )

        for bt_id, other_id, link_type in db.driver_wrapper.execute(stmt):
            book_col_map[link_type][bt_id].append(other_id)
            col_book_map[link_type][other_id].append(bt_id)

            seen_books.add(bt_id)
            known_link_types.add(link_type)

        for book_id in self.seen_book_ids:
            for link_type in known_link_types:
                if book_id not in book_col_map[link_type]:
                    book_col_map[link_type][book_id] = []

        for item_id in self.seen_item_ids:
            for link_type in known_link_types:
                if item_id not in col_book_map[link_type]:
                    col_book_map[link_type][item_id] = []

    def update_preflight(
        self,
        book_id_item_id_map: dict[SrcTableID, dict[str, list[DstTableID]]],
        id_map_update: Optional[dict[DstTableID, T]] = None,
        dirtied: Optional[set[SrcTableID]] = None,
    ) -> tuple[dict[SrcTableID, dict[str, Optional[list[DstTableID]]]], dict[DstTableID, T]]:
        """
        Processes the book_id_item_id_map to bring it into a useful form to write an update out to the db and the cache.

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

                elif isinstance(link_vals, list):
                    clean_book_id_item_id_map[book_id][link_type] = book_update_dir[link_type]

                elif isinstance(link_vals, (basestring, int)):
                    clean_book_id_item_id_map[book_id][link_type] = [book_update_dir[link_type],] + self.book_col_map[
                        link_type
                    ][book_id]

                else:
                    raise NotImplementedError

        return clean_book_id_item_id_map, id_map_update

    def internal_update_cache(
        self,
        book_id_item_id_map: dict[SrcTableID, dict[str, Optional[list[DstTableID]]]],
        id_map_update: dict[DstTableID, T],
    ):
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
                    self.book_col_map[known_link_type][book_id] = []

                for old_link_type, old_link_values in iteritems(old_book_data):
                    for item_id in old_link_values:
                        self.col_book_map[old_link_type][item_id].remove(book_id)

                deleted.add(book_id)
                continue

            # If the link_dict has no content, then this position should never have been reached in the first place
            assert link_dict, "Cannot preform update - link_dict malformed: {}".format(link_dict)

            old_book_data = self.book_data(book_id=book_id)

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
                            self.col_book_map[link_type][item_id].remove(book_id)
                        except ValueError:
                            pass

                    # Note the change in the updates which will be written out to the database
                    updated[book_id][link_type] = None

                    continue

                # Getting tuples passed into the system - need to standardize on lists
                item_ids = list(item_ids)

                # Remove the col_book_map links
                try:
                    old_book_item_ids = old_book_data[link_type]
                except KeyError:
                    old_book_item_ids = []
                for item_id in old_book_item_ids:
                    self.col_book_map[link_type][item_id].remove(book_id)

                # In the case where there's an element in the newly set ids which is already linked to the book with a
                # different role, that element needs to be removed
                for lt in self.known_link_types:
                    for niid in item_ids:
                        try:
                            self.book_col_map[lt][book_id].remove(niid)
                        except ValueError:
                            pass
                        try:
                            self.col_book_map[lt][niid].remove(book_id)
                        except ValueError:
                            pass

                # Write the new links back out to the cache
                self.book_col_map[link_type][book_id] = item_ids
                for niid in item_ids:
                    self.col_book_map[link_type][niid] = [book_id,] + self.col_book_map[
                        link_type
                    ][niid]

                # Note the change in the updates which will be written out to the database
                updated[book_id][link_type] = item_ids

        return updated, deleted

    def update_precheck(self, book_id_item_id_map, id_map_update):
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

                # If this is a int, then check it is a valid id and pass it if so
                # Note - this has to be overridden if the method is being used to write integers
                if isinstance(link_vals, int):
                    if not (link_vals in self.id_map or link_vals in id_map_update):
                        err_str = "Cannot match update id - cannot preform update as cannot link"
                        err_str = default_log.log_variables(
                            err_str,
                            "ERROR",
                            ("link_vals", link_vals),
                            ("id_map_update", id_map_update),
                        )
                        raise InvalidCacheUpdate(err_str)
                    continue

                for item_id in link_vals:

                    # Todo: Need to restructure so this is not necessary - the check should be run after the full update
                    #       has been built
                    if isinstance(item_id, basestring):
                        continue

                    if not (item_id in self.id_map or item_id in id_map_update):
                        err_str = "Cannot match update id - cannot preform update as cannot link"
                        err_str = default_log.log_variables(
                            err_str,
                            "ERROR",
                            ("item_id", item_id),
                            ("id_map_update", id_map_update),
                        )
                        raise InvalidCacheUpdate(err_str)

    def update_cache(self, book_id_val_map, id_map=None):
        """
        Preforms a cache update on the internal maps stored in the database.
        :param book_id_val_map:
        :param id_map:
        :return:
        """
        self.update_precheck(book_id_val_map, id_map_update=id_map)

        # book_id_val_map should be keyed with the id of the book in question and valued with the new id of the target
        # - if the target is already linked to the book then promote it
        # - if the target is not linked to the book then link it
        for book_id, link_dict in iteritems(book_id_val_map):

            # Item ids currently linked to the book
            old_book_col_ids = deepcopy(self.book_data(book_id=book_id))

            # We're nullifying all the entries from the old link dict
            if link_dict is None:
                for old_link_type in iterkeys(old_book_col_ids):
                    self.book_col_map[old_link_type][book_id] = []

                for old_link_type, old_link_vals in iteritems(old_book_col_ids):
                    pass

                continue

            for link_type, link_vals in iteritems(link_dict):
                if isinstance(link_vals, list):
                    self.book_col_map[link_type][book_id] = link_vals
                elif link_vals is None:
                    self.book_col_map[link_type][book_id] = list()
                else:
                    raise NotImplementedError()

            # Iterate through the update - adding the element to the front of the list
            for link_type, link_vals in iteritems(link_dict):
                if isinstance(link_vals, list):
                    pass

            # # Add the item to the front of the linked book ids
            # try:
            #     self.book_col_map[book_id].add(book_val)
            # except AttributeError:
            #     if isinstance(book_val, int):
            #         book_col_map_ids = set(self.book_col_map[book_id])
            #         book_col_map_ids.add(book_val)
            #         self.book_col_map[book_id] = tuple(book_col_map_ids)
            #     elif isinstance(book_val, set):
            #         self.book_col_map[book_id] = book_val
            #     else:
            #         raise NotImplementedError("Cannot parse book_id_val_map")
            #
            # # We're adding a value
            # if isinstance(book_val, int):
            #     self.col_book_map[book_val].add(book_id)
            # # We're doing a full replace of the values of a book
            # elif isinstance(book_val, set):
            #     # Need to remove reference to the book form all the old item ids
            #     for old_item_id in old_book_col_ids:
            #         self._remove_book_from_item(old_item_id, book_id)
            #     # And now add the new values back in
            #     for new_item_id in book_val:
            #         self._add_book_to_item(new_item_id, book_id)
            # else:
            #     raise NotImplementedError("Cannot parse book_id_val_map")

        # id_map will just have the updates applied to it - no need to do more
        if id_map is not None:
            self.id_map.update(id_map)
