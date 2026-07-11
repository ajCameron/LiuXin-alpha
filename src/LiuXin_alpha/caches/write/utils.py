
"""
Generic writer utils - supporting all other writers.
"""

from __future__ import division, absolute_import, print_function, unicode_literals

from typing import TYPE_CHECKING

if TYPE_CHECKING:

    from LiuXin_alpha.catalog.api import CatalogAPI
    from LiuXin_alpha.caches.api.storage_cache_api import FieldBasicInterfaceAPI


class DummyWriter:
    """
    Dummy for when you don't want have to set up an actual functional writer.
    """
    def __init__(self, field) -> None:
        self.field = field
        self.set_books_func = self.dummy

    @staticmethod
    def dummy(book_id_val_map, *args):
        """
        Dummy for the writer which changes no books.

        :param book_id_val_map:
        :param args:
        :return:
        """
        return set()

    def set_books(
            self,
            book_id_val_map,
            db: "CatalogAPI",
            allow_case_change: bool = True,
            error: bool = False) -> set[int]:
        """
        Set books by writing their values out to the database.

        :param book_id_val_map:
        :param db:
        :param allow_case_change:
        :param error:
        :return:
        """
        raise NotImplementedError("writer is not available for this field")

    def set_books_for_enum(
            self,
            book_id_val_map,
            db: "CatalogAPI",
            field,
            allow_case_change: bool = True) -> None:
        """
        Set books for an enumeration type field.

        :param book_id_val_map:
        :param db:
        :param field:
        :param allow_case_change:
        :return:
        """
        raise NotImplementedError("writer is not available for this field")


class UpdateDict(dict):
    """
    Designed to hold updates to the database in dictionary form

    A dict with some additional attributes (such as have they been checked before writing).
    """

    def __init__(self, *args, **kwargs):
        """
        Startup the dict.

        :param args:
        :param kwargs:
        """
        super(UpdateDict, self).__init__(*args, **kwargs)

        self.checked = False
