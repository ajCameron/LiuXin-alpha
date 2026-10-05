
"""
Write legacy author-sort text through the metadata SQL compatibility adapter.

These specialized setters invoke one adapter operation per supplied owner
and report the supplied IDs as dirty. They do not rebuild cache projections
or add a transaction across the writes.
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
    Bind legacy author-sort text writes to the shared adaptation wrapper.

    BaseWriter selects the field adapter. The specialized setter writes
    through metadata_sql and returns dirty IDs, leaving surrounding cache
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
    def set_author_sort(book_id_val_map: dict[int, Optional[str]], db: "DatabaseAPI", field, *args):
        """
        Write author-sort text for every supplied owner in mapping order.

        Call update_title_creator_sort(title_id=..., creator_val=...) for each
        entry. The legacy book ID is used as the title ID; no cache map is updated.
        No local validation, transaction or rollback is added; a later failure can
        follow earlier writes. Empty mappings return an empty set.

        Example:
            >>> AuthorSortWriter.set_author_sort({}, None, None)
            set()


        :param book_id_val_map: Owner-ID-to-value mapping forwarded without local coercion.
        :param db: Database/catalog adapter exposing metadata_sql.
        :param field: Unused field compatibility argument.
        :param args: Unused positional compatibility arguments, including the case-change flag.
        :return: Set of all supplied mapping keys after every adapter call succeeds.
        """
        for book_id, creator_val in iteritems(book_id_val_map):
            db.metadata_sql.update_title_creator_sort(title_id=book_id, creator_val=creator_val)

        return set(book_id_val_map)
