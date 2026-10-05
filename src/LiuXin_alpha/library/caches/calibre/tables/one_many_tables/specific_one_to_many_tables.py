"""
Provide specialized one-to-many relation tables.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise specific one to many tables through a consuming regression::

        python -m pytest -q tests/library/test_unified_library.py
"""


from collections import defaultdict
from collections import OrderedDict

from typing import TypeVar, Optional, Union, Iterable

from LiuXin_alpha.library.caches.calibre.tables.one_many_tables.priority_typed_one_to_many_table import (
    CalibrePriorityTypedOneToManyTable,
)
from LiuXin_alpha.databases.db_types import SrcTableID, DstTableID, IdentifiersStr


from LiuXin_alpha.errors import InvalidCacheUpdate

from LiuXin_alpha.utils.libraries.liuxin_six import iteritems, dict_iteritems as iteritems


T = TypeVar("T")


# ----------------------------------------------------------------------------------------------------------------------
#
# - ONE TO MANY TABLES


# Todo: Need to be able to deal with being fed a custom column
from LiuXin_alpha.customize.cache.base_tables import BaseIdentifiersTable



class CalibreIdentifiersTable(CalibrePriorityTypedOneToManyTable[T], BaseIdentifiersTable):
    """
    Represents the identifiers of a book.

    Example:
        Exercise CalibreIdentifiersTable through a consuming regression::

            python -m pytest -q tests/library/test_unified_library.py
    """

    _priority = True
    _typed = True

    def read_id_maps(self, db) -> None:
        """
        Not used in this context - doesn't much matter what the specific ids of the identifiers are.

        Example:
            Exercise CalibreIdentifiersTable.read id maps through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    def fix_case_duplicates(self, db) -> None:
        """
        All identifiers are stored in lower case anyways.

        Example:
            Exercise CalibreIdentifiersTable.fix case duplicates through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    # Todo: Wwould be cool if the type filter could take a list, a string or None
    def read_maps(self, db, type_filter: Optional[str] = None) -> None:
        """
        Read the maps between the identifiers book and the identifier table.

        Example:
            Exercise CalibreIdentifiersTable.read maps through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param db: Value supplied for db under the utility contract.
        :param type_filter: Value supplied for type filter under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if type_filter is not None:
            raise NotImplementedError(f"{type_filter=} not supported - pass None.")

        self.book_col_map = defaultdict(self._container_for_book)
        self.col_book_map = defaultdict(set)

        for book, typ, val in db.metadata_sql.read_all_identifiers():
            # Ignore malformed identifier entries
            if typ is not None and val is not None:
                self.col_book_map[typ].add(book)
                self.book_col_map[book][typ].add(val)

    def write_to_db(self, book_id: SrcTableID, db) -> None:
        """
        Write the identifiers stored in the book_col_map out to the database.

        Example:
            Exercise CalibreIdentifiersTable.write to db through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id: Value supplied for book id under the utility contract.
        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        db.metadata_sql.delete_title_identifiers(book_id)
        db.macros.delete_in_table("identifier_title_links", "identifier_title_link_title_id", book_id)

        title_row = db.get_row_from_id("titles", row_id=book_id)

        for id_type in self.book_col_map[book_id]:

            for ids_val in self.book_col_map[book_id][id_type]:

                # Construct the id row
                id_row = db.get_blank_row("identifiers")
                id_row["identifier"] = ids_val
                id_row["identifier_type"] = id_type
                id_row.sync()

                # Link the id row to the title row
                db.interlink_rows(primary_row=title_row, secondary_row=id_row, type=id_type)

    def update_precheck(
        self, book_id_item_id_map: dict[SrcTableID, Iterable[IdentifiersStr]], id_map_update: dict[DstTableID, T]
    ) -> None:
        """
        Check that an update is of a valid form before writing it out to the cache and the database.

        Example:
            Exercise CalibreIdentifiersTable.update precheck through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_item_id_map: Value supplied for book id item id map under the utility
            contract.
        :param id_map_update: Value supplied for id map update under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        seen_ids = set()
        for book_id, book_vals in iteritems(book_id_item_id_map):

            if book_vals is None:
                continue

            # Check to ensure that all the item entries for a specific book are different - this is supposed to be a
            # ONE to MANY field, after all
            # Todo: This allows entries which produce dupes with other books in the table.
            seen_ids_after = seen_ids.union(set(book_vals))
            if len(seen_ids_after) - len(seen_ids) == len(book_vals):
                raise InvalidCacheUpdate("overlap between book values")
            seen_ids = seen_ids_after

    def update_cache(
        self,
        book_id_val_map: dict[SrcTableID, Union[str, Iterable[str], dict[str, Iterable[str]]]],
        id_map: Optional[dict[DstTableID, T]] = None,
    ) -> None:
        """
        Preform updates on the cache.

        Example:
            Exercise CalibreIdentifiersTable.update cache through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_val_map: Value supplied for book id val map under the utility
            contract.
        :param id_map: Value supplied for id map under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        # Todo: Need to handle col_book_map as well
        for book_id, book_val in iteritems(book_id_val_map):

            # If the book_id is just valued by a str - then assume it's an isbn and add it in
            if isinstance(book_val, str):
                self.book_col_map[book_id]["isbn"].add(book_val)
            # Assume we're doing a wholesale replace on available isbns
            elif isinstance(book_val, (tuple, list, set, frozenset)):
                self.book_col_map[book_id]["isbn"] = set(book_val)
            elif isinstance(book_val, (dict, OrderedDict)):
                for id_type, id_vals in iteritems(book_val):
                    self.book_col_map[book_id][id_type] = set(id_vals)
            else:
                raise NotImplementedError("book_id_val_map value not recognized")

    def update_db(
        self,
        book_id_to_val_map: dict[SrcTableID, Union[dict[str, Iterable[str]], Iterable[str], None, str]],
        db,
        allow_case_change: bool = False,
    ) -> set[SrcTableID]:
        """
        Preform updates on the database.

        Example:
            Exercise CalibreIdentifiersTable.update db through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_id_to_val_map: Value supplied for book id to val map under the utility
            contract.
        :param db: Value supplied for db under the utility contract.
        :param allow_case_change: Value supplied for allow case change under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        # Todo: This needs to be merged with the identifiers update method over in write
        for book_id, book_val in iteritems(book_id_to_val_map):

            # If the book_id is just valued by a str - try and parse it to see if it's an identifier string - then add
            # it in
            if isinstance(book_val, str):
                # Assume we're dealing with an isbn and add it as such
                if ":" not in book_val:
                    db.metadata_sql.add_title_identifier(title_id=book_id, id_type="isbn", id_val=book_val)
                else:
                    # Todo: Add checking that the id_type is one of the allowable values
                    id_type, id_val = book_val.split(":")
                    db.metadata_sql.add_title_identifier(title_id=book_id, id_type=id_type, id_val=id_val)

            # Assume we're doing a wholesale replace on available isbns
            elif isinstance(book_val, (tuple, list, set, frozenset)):
                db.metadata_sql.delete_title_identifiers(book_id, id_type="isbn")
                for new_id in book_val:
                    db.metadata_sql.add_title_identifier(title_id=book_id, id_type="isbn", id_val=new_id)

            elif isinstance(book_val, (dict, OrderedDict)):
                db.metadata_sql.delete_title_identifiers(book_id)
                for id_type, id_vals in iteritems(book_val):
                    for new_id in id_vals:
                        db.metadata_sql.add_title_identifier(title_id=book_id, id_type=id_type, id_val=new_id)

            else:
                raise NotImplementedError("book_id_val_map value not recognized")

        return set(book_id_to_val_map)

    @staticmethod
    def _container_for_book() -> dict[str, set[str]]:
        """
        Part of allowing the production of nested default dicts.

        Example:
            Exercise CalibreIdentifiersTable. container for book through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return defaultdict(set)

    def remove_books(self, book_ids: Iterable[SrcTableID], db) -> set[DstTableID]:
        """
        Remove books from the cache.

        Example:
            Exercise CalibreIdentifiersTable.remove books through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param book_ids: Book identities included in the batched read operation.
        :param db: Value supplied for db under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        clean = set()
        for book_id in book_ids:
            item_map = self.book_col_map.pop(book_id, {})
            for item_id in item_map:
                try:
                    self.col_book_map[item_id].discard(book_id)
                except KeyError:
                    clean.add(item_id)
                else:
                    if not self.col_book_map[item_id]:
                        del self.col_book_map[item_id]
                        clean.add(item_id)
        return clean

    def remove_items(
        self, item_ids: Iterable[DstTableID], db, restrict_to_book_ids: Optional[Iterable[SrcTableID]] = None
    ) -> None:
        """
        NOT SUPPORTED - Directly remove identifiers from the system.

        Example:
            Exercise CalibreIdentifiersTable.remove items through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_ids: Value supplied for item ids under the utility contract.
        :param db: Value supplied for db under the utility contract.
        :param restrict_to_book_ids: Value supplied for restrict to book ids under the
            utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError("Direct deletion of identifiers is not implemented")

    def rename_item(self, item_id: DstTableID, new_name: IdentifiersStr, db) -> None:
        """
        Rename an item stored in the cache - NOT USED FOR IDENTIFIERS.

        Example:
            Exercise CalibreIdentifiersTable.rename item through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :param item_id: Value supplied for item id under the utility contract.
        :param new_name: Value supplied for new name under the utility contract.
        :param db: Value supplied for db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise NotImplementedError("Cannot rename identifiers")

    def all_identifier_types(self) -> Iterable[str]:
        """
        Return all the identifier types known to the system.

        Example:
            Exercise CalibreIdentifiersTable.all identifier types through a consuming regression::

                python -m pytest -q tests/library/test_unified_library.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return frozenset(k for k, v in iteritems(self.col_book_map) if v)
