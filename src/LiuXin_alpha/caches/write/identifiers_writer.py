
"""
Replace Work identifiers through Catalog and then update legacy identifier maps.
"""

from __future__ import division, absolute_import, print_function, unicode_literals, annotations

from typing import TYPE_CHECKING

from LiuXin_alpha.caches.write.base_writer import BaseWriter
from LiuXin_alpha.catalog import Catalog
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems

if TYPE_CHECKING:

    from LiuXin_alpha.catalog.api import CatalogAPI
    from LiuXin_alpha.caches.api.storage_cache_api import FieldBasicInterfaceAPI


class IdentifiersWrite(BaseWriter):
    """
    Write identifier mappings without the scalar BaseWriter adapter.

    Each Work replacement must succeed in storage before its forward and reverse cache maps are changed. Multiple requested Works are processed sequentially.

    Example:
        With a valid database and identifier field, ``IdentifiersWrite(field).set_books({7: {"doi": "10.1000/example"}}, db)`` replaces Work 7 identifiers.
    """

    def __init__(self, field) -> None:
        """
        Initialize shared writer state and bypass scalar adaptation for identifier maps.

        Example:
            After ``writer = IdentifiersWrite(field)``, ``writer.set_books`` accepts complete identifier mappings per Work.


        :param field: Legacy field whose name and metadata select the inherited value adapter.
        :return: None; binds identifiers and replaces set_books with no_adapter_set_books.
        """
        super(IdentifiersWrite, self).__init__(field=field)

        self.set_books_func = self.identifiers
        self.set_books = self.no_adapter_set_books

    # Todo: Tbh, the fact that we need to keep giving these methods the field is stupid.
    @staticmethod
    def identifiers(
            book_id_val_map,
            db: "CatalogAPI",
            field: "FieldBasicInterfaceAPI",
            *args) -> set[int]:
        """
        Replace each Work identifier set, then mirror the supplied mapping in cache.

        Delegate validation and storage transactionality to ``Catalog(db).identifiers.replace_for_wemi`` at level "work". After success, remove missing schemes from the existing forward map and discard reverse membership, retaining empty reverse sets; merge supplied values and add reverse membership. Cache values come from the input, not a readback of normalized storage. There is no transaction around the complete batch or the subsequent cache edits.

        Example:
            Starting with a DOI-only cache entry, ``identifiers({7: {"uuid": valid_uuid}}, db, field)`` replaces storage, removes its DOI membership and adds UUID membership. A rejected ISBN leaves that Work cache unchanged.


        :param book_id_val_map: Work IDs mapped to complete scheme-to-value dictionaries; an empty dictionary clears that Work.
        :param db: Database adapter wrapped or called by the writer; collaborator failures propagate.
        :param field: Field with mutable table.book_col_map and table.col_book_map caches.
        :param args: Additional compatibility arguments, ignored by this hook.
        :return: Set of all supplied Work IDs after every replacement and cache update succeeds.
        :raises ValueError: The Catalog identifier repository rejects a supplied identifier, before this Work cache changes.
        """
        table = field.table
        catalog = Catalog(db)
        for book_id, ids in iteritems(book_id_val_map):
            # Storage owns validation and transactionality. Do not advance the
            # in-memory cache until the authoritative replacement succeeds.
            catalog.identifiers.replace_for_wemi(
                level="work",
                entity_id=book_id,
                identifiers=ids,
            )

            # If the book does not currently have an entry in the ids cache, add it
            if book_id not in table.book_col_map:
                table.book_col_map[book_id] = {}

            current_ids = table.book_col_map[book_id]
            remove_keys = set(current_ids) - set(ids)

            for key in remove_keys:
                table.col_book_map.get(key, set()).discard(book_id)
                current_ids.pop(key, None)
            current_ids.update(ids)

            for key, val in iteritems(ids):
                if key not in table.col_book_map:
                    table.col_book_map[key] = set()
                table.col_book_map[key].add(book_id)

        return set(book_id_val_map)
