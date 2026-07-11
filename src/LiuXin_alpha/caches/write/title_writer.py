
"""
Responsible for writing title changes out to the title field.
"""

from __future__ import division, absolute_import, print_function, unicode_literals

from typing import TYPE_CHECKING, Optional

from LiuXin_alpha.caches.write import OneToOneWriter
from LiuXin_alpha.metadata.ebook_metadata_tools import title_sort
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems


if TYPE_CHECKING:

    from LiuXin_alpha.catalog.api import CatalogAPI
    from LiuXin_alpha.caches.api.storage_cache_api import FieldBasicInterfaceAPI



class TitleWriter(OneToOneWriter):
    """
    Writer for compatibility with the calibre style title writer.

    In line with the calibre compatibility policy, defaults to writing manifestation titles.
    """
    def __init__(self, field) -> None:
        """
        Constructor.

        :param field:
        """
        super(TitleWriter, self).__init__(field)

        self.set_books_func = self.set_title

    def set_title(
            self,
            book_id_val_map, db: "CatalogAPI",
            field: Optional["FieldBasicInterfaceAPI"] = None, *args) -> set[int]:
        """
        Set the title and update the title_sort field.

        As per the calibre compatibility policy,
        :param book_id_val_map:
        :param db:
        :param field:
        :param args:
        :return:
        """
        # Update the titles in the database
        ans = self.one_one_in_books(book_id_val_map, db, field, *args)

        # Update the title sort field - if required
        if field is not None:
            field.title_sort_field.writer.set_books({k: title_sort(v) for k, v in iteritems(book_id_val_map)}, db)
        return ans
