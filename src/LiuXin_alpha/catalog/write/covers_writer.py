
"""
Methods to write infomation concerning covers to the database.
"""

from __future__ import division, absolute_import, print_function, unicode_literals, annotations

from typing import TYPE_CHECKING

from LiuXin_alpha.catalog.write.base_writer import BaseWriter
from LiuXin_alpha.catalog.write.library_macros import library_set_cover

from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


class CoversWrite(BaseWriter):
    """
    Class for writing covers information out to the table.
    """

    def __init__(self, field):
        """
        Constructor.

        :param field:
        """
        super(CoversWrite, self).__init__(field=field)
        self.set_books_func = self.set_cover_exists

    @staticmethod
    def set_cover_exists(book_id_val_map, db: "DatabaseAPI", field, *args):
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
