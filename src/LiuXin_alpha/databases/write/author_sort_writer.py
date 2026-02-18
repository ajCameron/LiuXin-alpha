from __future__ import division, absolute_import, print_function, unicode_literals

from LiuXin_alpha.databases.write import BaseWriter
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems


class AuthorSortWriter(BaseWriter):
    """
    Class for writing information out to the AuthorSort table.
    """

    def __init__(self, field):
        super(AuthorSortWriter, self).__init__(field)
        self.set_books_func = self.set_author_sort

    @staticmethod
    def set_author_sort(book_id_val_map, db, field, *args):
        """
        Set the author sort for the given books.
        :param book_id_val_map:
        :param db:
        :param field:
        :param args:
        :return:
        """
        for book_id, creator_val in iteritems(book_id_val_map):
            db.macros.update_title_creator_sort(title_id=book_id, creator_val=creator_val)
        return set(book_id_val_map)
