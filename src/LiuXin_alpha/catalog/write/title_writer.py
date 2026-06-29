from __future__ import division, absolute_import, print_function, unicode_literals

from LiuXin_alpha.catalog.write import OneToOneWriter
from LiuXin_alpha.metadata.ebook_metadata_tools import title_sort
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems


class TitleWriter(OneToOneWriter):
    def __init__(self, field):
        super(TitleWriter, self).__init__(field)
        self.set_books_func = self.set_title

    def set_title(self, book_id_val_map, db, field, *args):
        """
        Set the title and update the title_sort field
        :param book_id_val_map:
        :param db:
        :param field:
        :param args:
        :return:
        """
        # Update the titles in the database
        ans = self.one_one_in_books(book_id_val_map, db, field, *args)

        # Update the title sort field
        field.title_sort_field.writer.set_books({k: title_sort(v) for k, v in iteritems(book_id_val_map)}, db)
        return ans
