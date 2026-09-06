
"""
Writer responsible for writing UUID changes out to the database.
"""

from __future__ import division, absolute_import, print_function, unicode_literals, annotations

from typing import TYPE_CHECKING

from LiuXin_alpha.caches.write.generic_writers.one_to_one_writer import OneToOneWriter

if TYPE_CHECKING:
    from LiuXin_alpha.catalog.api import CatalogAPI
    from LiuXin_alpha.caches.api.storage_cache_api import FieldBasicInterfaceAPI


class UUIDWriter(OneToOneWriter):
    """
    Responsible for writing UUID data out to the database.
    """
    def __init__(self, field: "FieldBasicInterfaceAPI") -> None:
        """
        Constructor.

        :param field:
        """
        super(UUIDWriter, self).__init__(field)
        self.set_books_func = self.set_uuid

    def set_uuid(
            self,
            book_id_val_map: dict[int, str],
            db: "CatalogAPI",
            field: "FieldBasicInterfaceAPI",
            *args) -> set[int]:
        """
        Update the uuid for the book.

        :param book_id_val_map: Keyed with the id of the book and valued with the new uuid value
        :param db: The database/catalog to preform the update on
        :param field: In memory field representing data from the database
        :param args:

        :return:
        """
        # Todo: This should not have to happen here
        # Update the cache
        if field is not None:
            field.table.update_uuid_cache(book_id_val_map)

        # Update the database through the uuid field
        return self.one_one_in_books(book_id_val_map, db, field, *args)
