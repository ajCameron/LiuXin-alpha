
"""
Write author-sort text through the explicit Calibre-to-WEMI policy.

The persistence batch is atomic. Work fields are shared by its Expressions;
callers remain responsible for refreshing every affected cache projection.
"""

# Calibre compatibility uses the versioned Expression policy in Catalog.


from __future__ import division, absolute_import, print_function, unicode_literals

from typing import TYPE_CHECKING

from LiuXin_alpha.caches.write.base_writer import BaseWriter
from LiuXin_alpha.catalog.catalog import Catalog

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


class AuthorSortWriter(BaseWriter):
    """
    Bind legacy author-sort text writes to the shared adaptation wrapper.

    BaseWriter selects the field adapter. The specialized setter writes
    through Catalog.calibre and returns dirty IDs, leaving surrounding cache
    reconciliation to its caller.

    Example:
        >>> issubclass(AuthorSortWriter, BaseWriter)
        True
    """

    def __init__(self, field):
        """
        Initialize the base field adapter and bind the specialized write hook.

        Example:
            Calling set_books on this writer adapts accepted values before the specialized setter.


        :param field: Field whose name/metadata configure adaptation and writer identity.
        :return: None; sets set_books_func to set_author_sort after base initialization.
        """
        super(AuthorSortWriter, self).__init__(field)
        self.set_books_func = self.set_author_sort

    # Todo: Alias this method into macros?
    # Todo: Remove args - where-ever possible
    # Todo: We should know the field from startup - why do we need it again?
    @staticmethod
    def set_author_sort(book_id_val_map, db: "DatabaseAPI", field, *args):
        """Write shared Work author-sort values through the Calibre policy.

        Keys must be explicit Expression references or validated W:E:M ray
        strings. Bare source/book/Work integers are rejected. Several Work
        parents require explicit Work context. The batch is atomic and alias
        conflicts fail before writes. The caller owns cache reconciliation.

        Example:
            >>> AuthorSortWriter.set_author_sort({}, None, None)
            set()

        :param book_id_val_map: CalibreBookRef/ray keys mapped to text or None.
        :param db: Database borrowed by Catalog.
        :param field: Retained compatibility argument.
        :param args: Retained compatibility arguments.
        :return: Supplied keys after the entire batch succeeds.
        """
        if book_id_val_map:
            Catalog(db).calibre.write_field("author_sort", book_id_val_map)
        return set(book_id_val_map)
