# Todo: Add checking that doc strings are not entirely identicle
"""
Model priority-ordered many-to-many relations.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise priority many to many table through a consuming regression::

        python -m pytest -q tests/library/test_unified_library.py
"""

from collections import defaultdict
from copy import deepcopy

from typing import TypeVar, Optional, Type, Any
try:
    from typing_extensions import Self
except ImportError:
    from typing import Self

from LiuXin_alpha.customize.cache.base_tables import (
    MANY_MANY,
)


from LiuXin_alpha.databases.db_types import (
    MetadataDict,
    InterLinkTableName,
    SrcTableID,
    DstTableID,
    MainTableName,
)
from LiuXin_alpha.library.caches.calibre.tables.many_many_tables.many_to_many_table import CalibreManyToManyTable

from LiuXin_alpha.errors import (
    InvalidCacheUpdate,
)
from LiuXin_alpha.utils.libraries.liuxin_six import (
    dict_iteritems as iteritems, basestring
)
from LiuXin_alpha.utils.logging import default_log


T = TypeVar("T")


class CalibrePriorityManyToManyTable(CalibreManyToManyTable[T]):
    """
    Many to many links with a priority for ordering.

    Example:
        Exercise CalibrePriorityManyToManyTable through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    table_type = MANY_MANY

    _priority: bool = True
    _typed: bool = False

    def __init__(
        self, name: str, metadata: MetadataDict, link_table: Optional[InterLinkTableName] = None, custom: bool = False
    ) -> None:
        """
        Initialize a PriorityManyToMany table.

        Example:
            Exercise CalibrePriorityManyToManyTable.  init   through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param metadata: Value supplied for metadata under the utility contract.
        :param link_table: Value supplied for link table under the utility contract.
        :param custom: Value supplied for custom under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        super(CalibrePriorityManyToManyTable, self).__init__(
            name=name, metadata=metadata, link_table=link_table, custom=custom
        )

        # Todo: Normalize to is_subtable
        self.sub_table: bool = False
        # Todo: Normalize to subtable_type_filter
        self.sub_table_type_filter: Optional[str] = None

        self.main_table_name: Optional[MainTableName] = None
        self.auxiliary_table_name: Optional[MainTableName] = None

        self.known_book_ids: set[SrcTableID] = set()

    @classmethod
    def from_typed_table(cls, db, original_table: CalibreManyToManyTable, type_filter: str) -> None:
        """
        Class method which _will refuse to_ produce one of these tables from a typed table.

        Example:
            Exercise CalibrePriorityManyToManyTable.from typed table through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db: Value supplied for db under the utility contract.
        :param original_table: Value supplied for original table under the utility contract.
        :param type_filter: Value supplied for type filter under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError("Cannot generate a typed table from a priority one")

    @classmethod
    def from_typed_priority_table(cls, db, original_table: CalibreManyToManyTable, type_filter: str) -> Type[Self]:
        """
        Construct a PriorityManyToManyTable from a PriorityTypedManyToManyTable by filtering out one of the types.

        Example:
            Exercise CalibrePriorityManyToManyTable.from typed priority table through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db: Value supplied for db under the utility contract.
        :param original_table: Value supplied for original table under the utility contract.
        :param type_filter: Value supplied for type filter under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        # assert isinstance(
        #     original_table, CalibrePriorityTypedManyToManyTable
        # ), "only PriorityTypedManyToManyTables are supported by this method"
        assert original_table.typed and original_table.priority

        # Preform startup on the new subtable
        sub_table = cls(
            name=original_table.name,
            metadata=original_table.metadata,
            link_table=original_table.link_table,
        )

        # Set properties for the new subtable
        sub_table.is_subtable = True
        sub_table.subtable_type_filter = type_filter

        sub_table.set_link_table(db=db)

        sub_table.id_map = original_table.id_map
        sub_table.book_col_map = original_table.book_col_map[type_filter]
        sub_table.col_book_map = original_table.col_book_map[type_filter]

        sub_table.seen_books = original_table.seen_books

        return sub_table

    def _book_col_map_factory(self) -> dict[SrcTableID, list[DstTableID]]:
        """
        This is a PriorityManyToMany table - the concept of order is important, so the data is stored in lists.

        Example:
            Exercise CalibrePriorityManyToManyTable. book col map factory through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return defaultdict(list)

    def _col_book_map_factory(self) -> dict[DstTableID, list[SrcTableID]]:
        """
        This is a PriorityManyToMany table - the concept of order is important, so the data is stored in lists.

        Example:
            Exercise CalibrePriorityManyToManyTable. col book map factory through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return defaultdict(list)

    def book_data(self, book_id: SrcTableID, type_filter: str = None) -> list[DstTableID]:
        """
        Returns the book data for the given record.

        Example:
            Exercise CalibrePriorityManyToManyTable.book data through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param type_filter: Value supplied for type filter under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return deepcopy(self.book_col_map[book_id])

    def vals_book_data(self, book_id: SrcTableID, type_filter: str = None) -> list[T]:
        """
        Returns the book data for the given record - in the form of vals. If you want ids then use book_data

        Example:
            Exercise CalibrePriorityManyToManyTable.vals book data through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param type_filter: Value supplied for type filter under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return deepcopy(self._ids_to_vals(self.book_col_map[book_id]))

    def _ids_to_vals(self, ids_container: list[DstTableID]) -> list[T]:
        """
        Takes a list of vals from the other table and turns them into their corresponding values.

        Example:
            Exercise CalibrePriorityManyToManyTable. ids to vals through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param ids_container: Value supplied for ids container under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return list(self.id_map[id_] for id_ in ids_container)

    def read(self, db) -> None:
        """
        Read data into the database.

        Example:
            Exercise CalibrePriorityManyToManyTable.read through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        super(CalibrePriorityManyToManyTable, self).read(db)

        self.read_known_book_ids(db)

    # Todo: read_maps is ambiguous - read_link_maps?
    def read_maps(self, db, type_filter: Optional[str] = None) -> None:
        """
        Preform a read into the internal caches.

        Example:
            Exercise CalibrePriorityManyToManyTable.read maps through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db: Value supplied for db under the utility contract.
        :param type_filter: Value supplied for type filter under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        assert type_filter is None, "type_filter is no longer in use"

        # Todo: Rename this to something more useful - characterize_link_table
        self.set_link_table(db=db, set_type=False)

        book_col_map = self.book_col_map
        col_book_map = self.col_book_map

        seen_books = self.seen_books

        stmt = "SELECT {}, {} FROM {} ORDER BY {} DESC;".format(
            self.link_table_bt_id_column,
            self.link_table_table_id_column,
            self.link_table,
            self.link_table_priority_col,
        )

        for bt_id, other_id in db.driver_wrapper.execute(stmt):
            book_col_map[bt_id].append(other_id)
            col_book_map[other_id].append(bt_id)

            seen_books.add(bt_id)

        for book_id in self.seen_book_ids:
            if book_id not in book_col_map:
                book_col_map[book_id] = []

        for item_id in self.seen_item_ids:
            if item_id not in col_book_map:
                col_book_map[item_id] = []

    def read_known_book_ids(self, db) -> None:
        """
        Reads all the books which are known to exist.

        Example:
            Exercise CalibrePriorityManyToManyTable.read known book ids through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.known_book_ids = set(db.macros.get_unique_values("titles", "title_id"))

    def internal_update_cache(
        self, book_id_item_id_map: dict[SrcTableID, list[DstTableID, ...]], id_map_update: dict[DstTableID, T]
    ) -> tuple[dict[SrcTableID, list[DstTableID, ...]], set[SrcTableID]]:
        """
        It sometimes makes sense to do an update to the cache to gather information before updating the database.

        Example:
            Exercise CalibrePriorityManyToManyTable.internal update cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_item_id_map: Value supplied for book id item id map under the utility
            contract.
        :param id_map_update: Value supplied for id map update under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        id_map_update = dict() if id_map_update is None else id_map_update

        self.id_map.update(id_map_update)

        # Update the book -> col and col -> book maps that form the cache
        deleted = set()
        updated = {}

        for book_id, link_ids in iteritems(book_id_item_id_map):

            if link_ids is None:

                old_item_ids = self.book_col_map.get(book_id, None)

                # We're nullifying the entries for the given book - first nullifying the book_col_links
                self.book_col_map[book_id] = []

                for item_id in old_item_ids:
                    self.col_book_map[item_id].remove(book_id)

                deleted.add(book_id)
                continue

            # If the link_dict has no content, then this position should never have been reached in the first place
            # assert link_ids, "Cannot preform update - link_dict malformed: {}".format(link_ids)

            old_item_ids = self.book_col_map.get(book_id, None)

            # Getting tuples passed into the system - need to standardize on lists
            item_ids = list(link_ids)

            for item_id in old_item_ids:
                self.col_book_map[item_id].remove(book_id)

            # Write the new links back out to the cache
            self.book_col_map[book_id] = item_ids
            for niid in item_ids:
                self.col_book_map[niid] = [
                    book_id,
                ] + self.col_book_map[niid]

            # Note the change in the updates which will be written out to the database
            updated[book_id] = item_ids

        return updated, deleted

    # Todo: Might not actually be used - check later
    def cache_update_preflight(
        self, book_id_item_id_map: dict[SrcTableID, list[DstTableID]], id_map_update: dict[DstTableID, T]
    ) -> tuple[dict[SrcTableID, list[DstTableID]], dict[DstTableID, T]]:
        """
        Gives the table a chance to bring the book_id_item_id_map into standard form before preforming an update.

        Example:
            Exercise CalibrePriorityManyToManyTable.cache update preflight through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_item_id_map: Value supplied for book id item id map under the utility
            contract.
        :param id_map_update: Value supplied for id map update under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        # Standardize the book_id_item_id_map
        clean_book_id_item_id_map = dict()
        for book_id, book_vals in iteritems(book_id_item_id_map):
            if isinstance(book_vals, int):
                clean_book_id_item_id_map[book_id] = [
                    book_vals,
                ] + self.book_col_map[book_id]

            elif isinstance(book_vals, (list, tuple)):
                clean_book_id_item_id_map[book_id] = book_vals

            elif isinstance(book_vals, basestring):
                # If the value for the book_id_item_id_map is a string, then check the id_map_update for it
                # Then look up the id and treat it as if we've been given an id
                if book_vals in id_map_update.values():
                    book_val_id = None
                    for item_id, str_val in iteritems(id_map_update):
                        if str_val == book_vals:
                            book_val_id = item_id
                            break
                    assert book_val_id is not None
                    clean_book_id_item_id_map[book_id] = [
                        book_val_id,
                    ] + self.book_col_map[book_id]

                else:
                    err_msg = [
                        "Unexpected form of the value of the update preflight",
                        "book_id_item_id_map: {}".format(book_id_item_id_map),
                        "id_map_update: {}".format(id_map_update),
                        "book_id: {}".format(book_id),
                        "book_vals: {}".format(book_vals),
                    ]
                    raise NotImplementedError("\n".join(err_msg))
            elif book_vals is None:
                clean_book_id_item_id_map[book_id] = []
            else:
                err_msg = [
                    "Unexpected form of the value of the update preflight",
                    "book_id_item_id_map: {}".format(book_id_item_id_map),
                    "id_map_update: {}".format(id_map_update),
                    "book_id: {}".format(book_id),
                    "book_vals: {}".format(book_vals),
                ]
                raise NotImplementedError("\n".join(err_msg))

        return clean_book_id_item_id_map, id_map_update

    def update_preflight(
        self,
        book_id_item_id_map: dict[SrcTableID, list[DstTableID]],
        id_map_update: dict[DstTableID, T],
        dirtied: Optional[set[DstTableID]] = None,
    ):
        """
        Gives the table a chance to bring the book_id_item_id_map into standard form before preforming an update.

        Example:
            Exercise CalibrePriorityManyToManyTable.update preflight through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_item_id_map: Value supplied for book id item id map under the utility
            contract.
        :param id_map_update: Value supplied for id map update under the utility contract.
        :param dirtied: Value supplied for dirtied under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """

        dirtied = set() if dirtied is None else dirtied

        # Standardize the book_id_item_id_map
        clean_book_id_item_id_map = dict()
        for book_id, book_vals in iteritems(book_id_item_id_map):

            if isinstance(book_vals, (int, basestring)):
                if book_vals not in self.book_col_map[book_id]:
                    clean_book_id_item_id_map[book_id] = [
                        book_vals,
                    ] + list(self.book_col_map[book_id])
                else:
                    old_book_ids = deepcopy(list(self.book_col_map[book_id]))
                    old_book_ids.remove(book_vals)
                    clean_book_id_item_id_map[book_id] = [
                        book_vals,
                    ] + old_book_ids

            elif isinstance(book_vals, (list, tuple)):
                clean_book_id_item_id_map[book_id] = book_vals

            elif book_vals is None:
                clean_book_id_item_id_map[book_id] = []

            else:
                err_msg = [
                    "Unexpected form of the value of the update preflight",
                    "book_id_item_id_map: {}".format(book_id_item_id_map),
                    "id_map_update: {}".format(id_map_update),
                    "book_id: {}".format(book_id),
                    "book_vals: {}".format(book_vals),
                ]
                raise NotImplementedError("\n".join(err_msg))

        return clean_book_id_item_id_map, id_map_update

    def update_precheck(
        self, book_id_item_id_map: dict[SrcTableID, list[DstTableID]], id_map_update: dict[DstTableID, T]
    ) -> None:
        """
        Check that an update is of a valid form before writing it out to the cache and the database.

        Example:
            Exercise CalibrePriorityManyToManyTable.update precheck through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_item_id_map: Value supplied for book id item id map under the utility
            contract.
        :param id_map_update: Value supplied for id map update under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for book_id, book_vals in iteritems(book_id_item_id_map):

            # Check to see if the book_id is known to the system - if it isn't then the update cannot proceed
            if book_id not in self.known_book_ids:
                raise InvalidCacheUpdate("title id needs to correspond to a title known to the system")

            # Will get sorted out by the writer during the update
            if book_vals is None:
                continue

            # Check that the new valued for the book_id are ordered - in some way
            if not isinstance(book_vals, (tuple, list, int, basestring)):
                raise InvalidCacheUpdate(self._book_vals_unacceptable_type(book_vals))

            # Will get sorted out by the writer during the update
            if isinstance(book_vals, (int, basestring)):
                continue

            # Check that we have no repeated elements in the list
            assert len(book_vals) == len(set(book_vals)), self._repeated_elements_found_in_update_list(
                book_id, book_vals, book_id_item_id_map
            )

            # Check that all the ids are valid
            for item_id in book_vals:
                if isinstance(item_id, basestring):
                    continue

                if not (item_id in self.id_map or item_id in id_map_update):
                    err_str = "Cannot match update id - cannot preform update as cannot link"
                    err_str = default_log.log_variables(err_str, "ERROR", ("item_id", item_id))
                    raise InvalidCacheUpdate(err_str)

    @staticmethod
    def _repeated_elements_found_in_update_list(
        book_id: SrcTableID, book_vals: list[DstTableID, ...], book_id_item_id_map: Any
    ) -> str:
        """
        Err msg - triggered when repeated elements are found in a book update list.

        Example:
            Exercise CalibrePriorityManyToManyTable. repeated elements found in update list through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param book_vals: Value supplied for book vals under the utility contract.
        :param book_id_item_id_map: Value supplied for book id item id map under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        err_msg = [
            "Update has been rejected - values contained repeated elements",
            "book_id: {}".format(book_id),
            "book_vals: {}".format(book_vals),
            "book_id_item_id_map: {}".format(book_id_item_id_map),
        ]
        return "\n".join(err_msg)

    @staticmethod
    def _book_vals_unacceptable_type(book_vals: list[DstTableID]) -> str:
        """
        Err msg - an unexpected and unacceptable type was found in the book vals list.

        Example:
            Exercise CalibrePriorityManyToManyTable. book vals unacceptable type through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_vals: Value supplied for book vals under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        err_msg = [
            "Map needs to be valued with a tuple, list, int or basestring",
            "book_vals: {}".format(book_vals),
        ]
        return "\n".join(err_msg)

    # Todo: Merge with the internal_update_cache method
    def update_cache(
        self, book_id_val_map: dict[SrcTableID, list[DstTableID]], id_map: dict[DstTableID, T] = None
    ) -> bool:
        """
        Preform an actual update of the cache - data will not be written out to the database.

        Example:
            Exercise CalibrePriorityManyToManyTable.update cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_val_map: Value supplied for book id val map under the utility
            contract.
        :param id_map: Value supplied for id map under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        book_id_val_map, id_map = self.update_preflight(book_id_val_map, id_map)

        self.update_precheck(book_id_val_map, id_map)

        self.internal_update_cache(book_id_item_id_map=book_id_val_map, id_map_update=id_map)

        return True

    def update_db(
        self, book_id_to_val_map: dict[SrcTableID, list[DstTableID]], db, allow_case_change: bool = False
    ) -> bool:
        """
        Write information contained in the :param book_id_to_val_map: out to the database.

        Example:
            Exercise CalibrePriorityManyToManyTable.update db through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_to_val_map: Value supplied for book id to val map under the utility
            contract.
        :param db: Value supplied for db under the utility contract.
        :param allow_case_change: Value supplied for allow case change under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return super(CalibrePriorityManyToManyTable, self).update_db(
            book_id_to_val_map=book_id_to_val_map,
            db=db,
            allow_case_change=allow_case_change,
        )
