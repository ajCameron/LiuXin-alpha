
"""
Responsible for writing identifiers out to the database.
"""

from __future__ import division, absolute_import, print_function, unicode_literals, annotations

from typing import TYPE_CHECKING

from LiuXin_alpha.caches.write import BaseWriter
from LiuXin_alpha.catalog import Catalog
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems

if TYPE_CHECKING:

    from LiuXin_alpha.catalog.api import CatalogAPI
    from LiuXin_alpha.caches.api.storage_cache_api import FieldBasicInterfaceAPI


class IdentifiersWrite(BaseWriter):
    """
    Class for writing identifier information out to the table
    """

    def __init__(self, field) -> None:
        """
        Startup the identifiers writer.

        :param field:
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
        Write identifiers out to the table.

        Unless this is called with append this will overwrite all the identifiers currently associated with the book.
        :param book_id_val_map: Keyed with the id of the book and valued with the identifiers to update that book with
        :param db: The database to do the update on
        :param field: A field with identifier like structure (theoretically - only currently working for the identifiers
                      table).
        :param args: Ignored
        :return:
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
