
"""
Methods to write infomation concerning covers to the database.
"""

from __future__ import division, absolute_import, print_function, unicode_literals, annotations

from typing import TYPE_CHECKING

from LiuXin_alpha.caches.write.base_writer import BaseWriter
from LiuXin_alpha.catalog.catalog_macros import library_set_cover

from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems

if TYPE_CHECKING:

    from LiuXin_alpha.catalog.api import CatalogAPI
    from LiuXin_alpha.caches.api.storage_cache_api import FieldBasicInterfaceAPI#


class CoversWrite(BaseWriter):
    """
    Class for writing covers information out to the table.
    """

    def __init__(self, field: "FieldBasicInterfaceAPI") -> None:
        """
        Constructor.

        :param field:
        """
        super(CoversWrite, self).__init__(field=field)
        self.set_books_func = self.set_cover_exists

    @staticmethod
    def set_cover_exists(
            book_id_val_map: dict[int, bool],
            db: "CatalogAPI",
            field: "FieldBasicInterfaceAPI",
            *args):
        """
        Set a flag to indicate if the works have a cover or not

        :param book_id_val_map:
        :param db:
        :param field:
        :param args:
        :return:
        """
        for book_id, cover_status in iteritems(book_id_val_map):
            library_set_cover(db, book_id, cover_status)

        return set(book_id_val_map)
