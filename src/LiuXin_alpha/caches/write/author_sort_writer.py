
"""
Specialized writer to write an author sort map onto a "book" table.

Of course, author sort, per-say, is somewhat complicated by WEMI.
"""

# Todo: We really, really need a good thing to map to the calibre concept of a book.
#       Probably the best thing to map to it is "items" - need to formalize this.


from __future__ import division, absolute_import, print_function, unicode_literals

from typing import TYPE_CHECKING, Optional

from LiuXin_alpha.caches.write.base_writer import BaseWriter
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


class AuthorSortWriter(BaseWriter):
    """
    Class for writing information out to the AuthorSort table.
    """

    def __init__(self, field):
        """
        Startup the writer for the author sort fields.

        :param field:
        """
        super(AuthorSortWriter, self).__init__(field)
        self.set_books_func = self.set_author_sort

    # Todo: Alias this method into macros?
    # Todo: Remove args - where-ever possible
    # Todo: We should know the field from startup - why do we need it again?
    @staticmethod
    def set_author_sort(book_id_val_map: dict[int, Optional[str]], db: "DatabaseAPI", field, *args):
        """
        Set the author sort for the given books.

        :param book_id_val_map:
        :param db:
        :param field:
        :param args:
        :return:
        """
        for book_id, creator_val in iteritems(book_id_val_map):
            db.metadata_sql.update_title_creator_sort(title_id=book_id, creator_val=creator_val)

        return set(book_id_val_map)
