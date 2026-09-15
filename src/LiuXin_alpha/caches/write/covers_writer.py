
"""
Write legacy cover-presence flags through the metadata SQL compatibility adapter.

These specialized setters invoke one adapter operation per supplied owner
and report the supplied IDs as dirty. They do not rebuild cache projections
or add a transaction across the writes.
"""

from __future__ import division, absolute_import, print_function, unicode_literals, annotations

from typing import TYPE_CHECKING

from LiuXin_alpha.caches.write.base_writer import BaseWriter

from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems

if TYPE_CHECKING:

    from LiuXin_alpha.catalog.api import CatalogAPI
    from LiuXin_alpha.caches.api.storage_cache_api import FieldBasicInterfaceAPI#


class CoversWrite(BaseWriter):
    """
    Bind legacy cover-presence flags writes to the shared adaptation wrapper.

    BaseWriter selects the field adapter. The specialized setter writes
    through metadata_sql and returns dirty IDs, leaving surrounding cache
    reconciliation to its caller.

    Example:
        >>> issubclass(CoversWrite, BaseWriter)
        True
    """

    def __init__(self, field: "FieldBasicInterfaceAPI") -> None:
        """
        Initialize the base field adapter and bind the specialized write hook.

        Example:
            Calling set_books on this writer adapts accepted values before the specialized setter.


        :param field: Field whose name/metadata configure adaptation and writer identity.
        :return: None; sets set_books_func to set_cover_exists after base initialization.
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
        Write cover-presence flags for every supplied owner in mapping order.

        Call metadata_sql.set_has_cover for each entry. This is a Calibre
        compatibility projection; the setter does not store cover bytes or route
        through metadata-aware Catalog operations.
        No local validation, transaction or rollback is added; a later failure can
        follow earlier writes. Empty mappings return an empty set.

        Example:
            >>> CoversWrite.set_cover_exists({}, None, None)
            set()


        :param book_id_val_map: Owner-ID-to-value mapping forwarded without local coercion.
        :param db: Database/catalog adapter exposing metadata_sql.
        :param field: Unused field compatibility argument.
        :param args: Unused positional compatibility arguments, including the case-change flag.
        :return: Set of all supplied mapping keys after every adapter call succeeds.
        """
        for book_id, cover_status in iteritems(book_id_val_map):
            # ``book_has_cover`` is a Calibre compatibility projection. Its
            # storage semantics remain owned by the cache/database adapter,
            # not by the metadata-aware Catalog.
            db.metadata_sql.set_has_cover(book_id, cover_status)

        return set(book_id_val_map)
