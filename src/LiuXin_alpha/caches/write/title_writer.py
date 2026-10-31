
"""
Write Expression title overrides through the versioned Calibre policy.
"""

from __future__ import division, absolute_import, print_function, unicode_literals

from LiuXin_alpha.caches.write.generic_writers.one_to_one_writer import OneToOneWriter


class TitleWriter(OneToOneWriter):
    """Write Expression titles while leaving shared Work titles and sort intact.

    Calibre title_sort is derived by the compatibility projection. This writer
    does not invoke a second legacy sort writer after committing the title.
    """
    def __init__(self, field) -> None:
        """
        Initialize scalar writer behavior and select the title hook.

        Example:
            Construct ``TitleWriter(title_field)``; title_sort is derived when
            reading the compatibility projection, without a companion write.


        :param field: Legacy field whose name and metadata select the inherited value adapter.
        :return: None; binds set_title as the inherited set_books hook.
        """
        super(TitleWriter, self).__init__(field)

        self.set_books_func = self.set_title

    def set_title(self, book_id_val_map, db, field=None, *args):
        """Set Expression title overrides through the shared compatibility policy.

        Explicit Expression references and validated SQL rays are accepted.
        The projection derives title_sort from the displayed title; no second
        write changes shared Work sort metadata. The persistence batch is
        atomic. Callers must refresh other rays and companion sort caches.

        :param book_id_val_map: Compatibility references mapped to title text/None.
        :param db: Database borrowed by Catalog.
        :param field: Optional legacy field whose direct cache receives input values.
        :param args: Retained compatibility arguments.
        :return: Supplied compatibility keys after persistence succeeds.
        """
        return self.one_one_in_books(book_id_val_map, db, field, *args)
