"""
Specific OneToMany tables are OneToMany tables built for a specific purpose.

E.g. identifiers - which emulates the calibre identifier table for compatibility.
"""


from collections import defaultdict
from collections import OrderedDict

from typing import TypeVar, Optional, Union, Iterable

from LiuXin.databases.caches.calibre.tables.one_many_tables.priority_typed_one_to_many_table import (
    CalibrePriorityTypedOneToManyTable,
)
from LiuXin.databases.db_types import SrcTableID, DstTableID, MetadataDict, InterLinkTableName, IdentifiersStr


from LiuXin.exceptions import InvalidCacheUpdate

from LiuXin.utils.lx_libraries.liuxin_six import iteritems


T = TypeVar("T")


# ----------------------------------------------------------------------------------------------------------------------
#
# - ONE TO MANY TABLES


# Todo: Need to be able to deal with being fed a custom column
from LiuXin.customize.cache.base_tables import BaseIdentifiersTable
from LiuXin.utils.lx_libraries.liuxin_six import dict_iteritems as iteritems


class CalibreIdentifiersTable(CalibrePriorityTypedOneToManyTable[T], BaseIdentifiersTable):
    """
    Represents the identifiers of a book.

    book_col_map - a dictionary of dictionaries - valued with the id of the book, then valued with the type of
    identifier and finally with a set of those identifier types.
    """

    _priority = True
    _typed = True

    def read_id_maps(self, db) -> None:
        """
        Not used in this context - doesn't much matter what the specific ids of the identifiers are.

        :param db:
        :return:
        """
        pass

    def fix_case_duplicates(self, db) -> None:
        """
        All identifiers are stored in lower case anyways.

        :param db:
        :return:
        """
        pass

    # Todo: Wwould be cool if the type filter could take a list, a string or None
    def read_maps(self, db, type_filter: Optional[str] = None) -> None:
        """
        Read the maps between the identifiers book and the identifier table.

        book_col_map - A dictionary of dictionaries of sets. Keyed with the book_id, then keyed with the type of
                       identifier and valued with a set of all the identifiers of that type associated with the title.
        col_book_map - A dictionary of sets, keyed with the type of the identifier and valued with a set of the books
                       that have that identifier.
        :param db:
        :param type_filter: Not currently in use
        :return None: All changes are made internally
        """
        if type_filter is not None:
            raise NotImplementedError(f"{type_filter=} not supported - pass None.")

        self.book_col_map = defaultdict(self._container_for_book)
        self.col_book_map = defaultdict(set)

        for book, typ, val in db.macros.read_all_identifiers():
            # Ignore malformed identifier entries
            if typ is not None and val is not None:
                self.col_book_map[typ].add(book)
                self.book_col_map[book][typ].add(val)

    def write_to_db(self, book_id: SrcTableID, db) -> None:
        """
        Write the identifiers stored in the book_col_map out to the database.

        All identifiers for the title will be scrubbed and the currently cached identifiers will be written out.
        :param book_id:
        :param db: The database to preform the edit in
        :return None: Data is written blind
        """
        db.macros.delete_title_identifiers(book_id)
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

        Called when you know the ids you want to assign to the book after the update. Checks those ids are valid.
        No changes will be made to the :param book_id_item_id_map: (e.g. if the map is valued with tuples - not lists
        as expected - this will not be corrected.
        Raised InvalidCacheUpdate if the cache update is invalid in some way.

        :param book_id_item_id_map: Keyed with the ids of the books to update and valued with the
        :param id_map_update:
        :return None: Raises an error if the update is malformed.
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

        Update should take the form of a dictionary of dictionaries of sets. Keyed with the id of the book to update,
        then keyed with the type of identifier to preform the update on, and finally valued with the identifiers to
        add in or replace.
        :param book_id_val_map: Keyed with the book id - then the identifier type - and then a container of the actual
                                Identifiers
        :param id_map: Should always be None (the identifiers act as their own ids - each being unique)
        :return None: All changes are handled internally
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

        Update should take the form of a dictionary of dictionaries of sets. Keyed with the id of the book to update,
        then keyed with the type of identifier to preform the update on, and finally valued with the identifiers to
        add in or replace.
        :param book_id_to_val_map:
        :param db:
        :param allow_case_change:
        :return:
        """
        # Todo: This needs to be merged with the identifiers update method over in write
        for book_id, book_val in iteritems(book_id_to_val_map):

            # If the book_id is just valued by a str - try and parse it to see if it's an identifier string - then add
            # it in
            if isinstance(book_val, str):
                # Assume we're dealing with an isbn and add it as such
                if ":" not in book_val:
                    db.macros.add_title_identifier(title_id=book_id, id_type="isbn", id_val=book_val)
                else:
                    # Todo: Add checking that the id_type is one of the allowable values
                    id_type, id_val = book_val.split(":")
                    db.macros.add_title_identifier(title_id=book_id, id_type=id_type, id_val=id_val)

            # Assume we're doing a wholesale replace on available isbns
            elif isinstance(book_val, (tuple, list, set, frozenset)):
                db.macros.delete_title_identifiers(book_id, id_type="isbn")
                for new_id in book_val:
                    db.macros.add_title_identifier(title_id=book_id, id_type="isbn", id_val=new_id)

            elif isinstance(book_val, (dict, OrderedDict)):
                db.macros.delete_title_identifiers(book_id)
                for id_type, id_vals in iteritems(book_val):
                    for new_id in id_vals:
                        db.macros.add_title_identifier(title_id=book_id, id_type=id_type, id_val=new_id)

            else:
                raise NotImplementedError("book_id_val_map value not recognized")

        return set(book_id_to_val_map)

    @staticmethod
    def _container_for_book() -> dict[str, set[str]]:
        """
        Part of allowing the production of nested default dicts.

        :return:
        """
        return defaultdict(set)

    def remove_books(self, book_ids: Iterable[SrcTableID], db) -> set[DstTableID]:
        """
        Remove books from the cache.

        Should be called after any delete activity in the cache - for each table which might be affected.
        :param book_ids:
        :param db:
        :return clean: Set of ids from the linked table to be removed
                       In this case, all the ids linked to each of the books for removal.
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

        Instead, please update the books with new identifiers.
        :param item_ids:
        :param db:
        :param restrict_to_book_ids:
        :return:
        """
        raise NotImplementedError("Direct deletion of identifiers is not implemented")

    def rename_item(self, item_id: DstTableID, new_name: IdentifiersStr, db) -> None:
        """
        Rename an item stored in the cache - NOT USED FOR IDENTIFIERS.

        :param item_id:
        :param new_name:
        :param db:
        :return:
        """
        raise NotImplementedError("Cannot rename identifiers")

    def all_identifier_types(self) -> Iterable[str]:
        """
        Return all the identifier types known to the system.

        :return:
        """
        return frozenset(k for k, v in iteritems(self.col_book_map) if v)
