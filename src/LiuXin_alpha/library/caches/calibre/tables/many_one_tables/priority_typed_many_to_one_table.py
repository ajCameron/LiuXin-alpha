"""
Model typed priority many-to-one relations.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise priority typed many to one table through a consuming regression::

        python -m pytest -q tests/library/test_unified_library.py
"""


from collections import defaultdict
from copy import deepcopy

from typing import TypeVar, Optional, Union, Any

from LiuXin_alpha.library.caches.calibre.tables.many_one_tables.many_to_one_table import CalibreManyToOneTable
from LiuXin_alpha.databases.db_types import MetadataDict, InterLinkTableName, SrcTableID, DstTableID

from LiuXin_alpha.errors import InvalidCacheUpdate

from LiuXin_alpha.utils.libraries.liuxin_six import iteritems, iterkeys, itervalues, basestring


T = TypeVar("T")


class CalibrePriorityTypedManyToOneTable(CalibreManyToOneTable[T]):
    """
    many-to-one table with both priority and type information

    Example:
        Exercise CalibrePriorityTypedManyToOneTable through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    _priority: bool = True
    _typed: bool = True

    def __init__(
        self, name: str, metadata: MetadataDict, link_table: Optional[InterLinkTableName] = None, custom: bool = False
    ) -> None:
        """
        Initialise a PriorityTypedManyToMany table.

        Example:
            Exercise CalibrePriorityTypedManyToOneTable.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param metadata: Value supplied for metadata under the utility contract.
        :param link_table: Value supplied for link table under the utility contract.
        :param custom: Value supplied for custom under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        super(CalibrePriorityTypedManyToOneTable, self).__init__(
            name=name, metadata=metadata, link_table=link_table, custom=custom
        )

        self.seen_link_types: set[str] = set()

        self.book_type_map: dict[str, list[DstTableID]] = dict()

    def _col_book_map_factory(self) -> dict[SrcTableID, dict[str, list[DstTableID]]]:
        """
        Part of a hack to produce a nested default dict.

        Example:
            Exercise CalibrePriorityTypedManyToOneTable. col book map factory through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return defaultdict(self._type_list_container)

    @staticmethod
    def _book_col_map_factory() -> dict[SrcTableID, dict[str, list[DstTableID]]]:
        """
        Part of a hack to produce a nested default dict.

        Example:
            Exercise CalibrePriorityTypedManyToOneTable. book col map factory through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return dict()

    @staticmethod
    def _type_list_container() -> dict[str, list[DstTableID]]:
        """
        Part of a hack to produce a nested default dict.

        Example:
            Exercise CalibrePriorityTypedManyToOneTable. type list container through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return defaultdict(list)

    def read_maps(self, db, type_filter: Optional[str] = None) -> bool:
        """
        Preform a read out of the database and into the internal cache.

        Example:
            Exercise CalibrePriorityTypedManyToOneTable.read maps through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db: Value supplied for db under the utility contract.
        :param type_filter: Value supplied for type filter under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        assert type_filter is None, "type_filter is no longer in use"

        self.set_link_table(db=db, set_type=True)

        stmt = "SELECT {}, {}, {} FROM {} ORDER BY {} DESC;".format(
            self.link_table_bt_id_column,
            self.link_table_table_id_column,
            self.link_table_type,
            self.link_table,
            self.link_table_priority_col,
        )

        col_book_map = self.col_book_map
        book_col_map = self.book_col_map
        seen_link_types = self.seen_link_types

        for bt_id, other_id, link_type in db.driver_wrapper.execute(stmt):
            book_col_map[bt_id] = other_id
            col_book_map[link_type][other_id].append(bt_id)

            self.book_type_map[bt_id] = link_type

            seen_link_types.add(link_type)

        for item_id in self.seen_item_ids:
            for lt in seen_link_types:
                if item_id not in col_book_map[lt]:
                    col_book_map[lt][item_id] = []

        for book_id in self.seen_book_ids:
            if book_id not in book_col_map:
                book_col_map[book_id] = None

        return True

    def cache_update_precheck(
        self, book_id_val_map: dict[SrcTableID, Optional[Union[DstTableID, T]]], id_map: dict[DstTableID, T]
    ) -> None:
        """
        Preforms checks that the given update is valid before applying it to the cache.

        Example:
            Exercise CalibrePriorityTypedManyToOneTable.cache update precheck through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_val_map: Value supplied for book id val map under the utility
            contract.
        :param id_map: Value supplied for id map under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for book_id, book_val in iteritems(book_id_val_map):
            if book_id not in self.seen_book_ids:
                raise InvalidCacheUpdate(self._book_id_not_known(book_id))

            if isinstance(book_val, (list, set, tuple)):
                raise InvalidCacheUpdate("book_val not of valid form")

            if book_val is None:
                continue

            if not isinstance(book_val, dict):
                raise InvalidCacheUpdate("book_val not of valid form - supposed to be a dict, and isn't")

    def _book_id_not_known(self, book_id: SrcTableID) -> str:
        """
        Err msg

        Example:
            Exercise CalibrePriorityTypedManyToOneTable. book id not known through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        err_msg = [
            "book_id was not known to the system",
            "book_id: {}".format(book_id),
            "self.seen_book_ids: {}".format(self.seen_book_ids),
        ]
        return "\n".join(err_msg)

    def update_cache(
        self,
            book_id_val_map: dict[SrcTableID, Optional[DstTableID]],
            id_map: Optional[dict[DstTableID, Optional[T]]] = None
    ) -> Any:
        """
        Actually preform an update of the cache.

        Example:
            Exercise CalibrePriorityTypedManyToOneTable.update cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_val_map: Value supplied for book id val map under the utility
            contract.
        :param id_map: Value supplied for id map under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        self.cache_update_precheck(book_id_val_map, id_map)

        return self.internal_update_cache(book_id_val_map, id_map)

    def update_precheck(
        self,
        book_id_item_id_map: dict[SrcTableID, Optional[DstTableID]],
        id_map_update: Optional[dict[DstTableID, Optional[T]]],
    ) -> None:
        """
        Check that an update is of a valid form before writing it out to the cache and the database.

        Example:
            Exercise CalibrePriorityTypedManyToOneTable.update precheck through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_item_id_map: Value supplied for book id item id map under the utility
            contract.
        :param id_map_update: Value supplied for id map update under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
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
                    if book_val_val not in self.seen_item_ids:
                        raise InvalidCacheUpdate("Book update dict is malformed")

    def internal_update_cache(
        self, book_id_item_id_map: dict[SrcTableID, Optional[DstTableID]], id_map_update: dict[DstTableID, Optional[T]]
    ) -> tuple[dict[SrcTableID, Optional[DstTableID]], set[SrcTableID]]:
        """
        Update cache with additional info - used in write when we need some info about the cache before db write.

        Example:
            Exercise CalibrePriorityTypedManyToOneTable.internal update cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_item_id_map: Value supplied for book id item id map under the utility
            contract.
        :param id_map_update: Value supplied for id map update under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self.id_map.update(id_map_update)

        # If additions have been made, they need to be noted
        for new_item_id in id_map_update:
            for lt in self.seen_link_types:
                if new_item_id not in self.col_book_map:
                    self.col_book_map[lt][new_item_id] = []

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
                        except ValueError:
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
                        except ValueError:
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
                        except ValueError:
                            pass
                    self.book_type_map[book_id] = None

                    # Write the new link back out
                    self.book_col_map[book_id] = link_val
                    self.col_book_map[link_type][link_val] = [book_id,] + self.col_book_map[
                        link_type
                    ][link_val]
                    self.book_type_map[book_id] = link_type

                else:

                    raise NotImplementedError

            updated[book_id] = item_val

        for lt in self.seen_link_types:
            if self._is_type_dict_null(self.col_book_map[lt]):
                del self.col_book_map[lt]

        return updated, deleted

    @staticmethod
    def _is_type_dict_null(type_dict: dict[Any, Any]) -> bool:
        """
        Checks to see if a type dict has any content

        Example:
            Exercise CalibrePriorityTypedManyToOneTable. is type dict null through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param type_dict: Value supplied for type dict under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        for item_id, item_vals in iteritems(type_dict):
            if item_vals:
                return False
        return True

    def item_data(
        self, book_id: SrcTableID, type_filter: Optional[str] = None
    ) -> Union[dict[str, list[DstTableID]], list[DstTableID]]:
        """
        Returns the item data for the given book - returns dict keyed with link_types and valued with sets of book_ids

        Example:
            Exercise CalibrePriorityTypedManyToOneTable.item data through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param type_filter: Value supplied for type filter under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if type_filter is None:
            item_data_dict = dict()
            for known_type in self.col_book_map.keys():
                item_data_dict[known_type] = self.col_book_map[known_type][book_id]
            return deepcopy(item_data_dict)
        else:
            assert type_filter in self.col_book_map.keys()
            return deepcopy(self.col_book_map[type_filter][book_id])
