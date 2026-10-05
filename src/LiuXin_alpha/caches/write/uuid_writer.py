
"""
Update the legacy UUID cache before delegating a scalar UUID database write.
"""

from __future__ import division, absolute_import, print_function, unicode_literals, annotations

from typing import TYPE_CHECKING

from LiuXin_alpha.caches.write.generic_writers.one_to_one_writer import OneToOneWriter

if TYPE_CHECKING:
    from LiuXin_alpha.catalog.api import CatalogAPI
    from LiuXin_alpha.caches.api.storage_cache_api import FieldBasicInterfaceAPI


class UUIDWriter(OneToOneWriter):
    """
    Write UUID fields with a cache update preceding scalar persistence.

    Inherited UUID set_books filtering rejects false values before adaptation. The hook itself first updates the UUID cache and does not undo that update if database persistence later fails.

    Example:
        With a UUID field, ``UUIDWriter(field).set_books({7: uuid_text}, db)`` updates the legacy UUID cache before the scalar write.
    """
    def __init__(self, field: "FieldBasicInterfaceAPI") -> None:
        """
        Initialize scalar UUID adaptation/filtering and bind set_uuid.

        Example:
            After ``writer = UUIDWriter(uuid_field)``, a field named "uuid" uses the inherited bool acceptance predicate.


        :param field: Legacy field whose name and metadata select the inherited value adapter.
        :return: None; binds the UUID hook after OneToOneWriter initialization.
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
        Update the UUID cache, then invoke the scalar books-table write helper.

        This hook performs no UUID parsing. When field is supplied, its UUID cache update happens even for an empty mapping and before database persistence. Cache errors prevent the write; later persistence errors propagate without restoring the earlier UUID cache state.

        Example:
            For a configured UUID field, ``writer.set_uuid({7: uuid_text}, db, field)`` calls ``field.table.update_uuid_cache`` before updating the database column.


        :param book_id_val_map: Book IDs mapped to UUID strings, normally already accepted and adapted by set_books.
        :param db: Database adapter wrapped or called by the writer; collaborator failures propagate.
        :param field: UUID field supplying table.update_uuid_cache and scalar column metadata; None skips the first step but is still invalid for the inherited helper.
        :param args: Additional arguments forwarded to one_one_in_books, where they are logged.
        :return: The affected-ID set returned by one_one_in_books.
        """
        # Todo: This should not have to happen here
        # Update the cache
        if field is not None:
            field.table.update_uuid_cache(book_id_val_map)

        # Update the database through the uuid field
        return self.one_one_in_books(book_id_val_map, db, field, *args)
