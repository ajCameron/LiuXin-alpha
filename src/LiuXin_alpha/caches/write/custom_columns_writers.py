
"""
Write legacy custom-series indexes to link columns and cached book values.
"""

from __future__ import division, absolute_import, print_function, unicode_literals, annotations

from typing import TYPE_CHECKING

from LiuXin_alpha.caches.write.base_writer import BaseWriter
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems

if TYPE_CHECKING:

    from LiuXin_alpha.catalog.api import CatalogAPI
    from LiuXin_alpha.caches.api.storage_cache_api import FieldBasicInterfaceAPI


class CustomSeriesIndexWriter(BaseWriter):
    """
    Adapt custom-series index values and bind the link-column update hook.

    The hook updates the first linked series only; cache entries are assigned even when a book has no series link.

    Example:
        With a configured index field, ``CustomSeriesIndexWriter(field).set_books({7: 2.0}, db)`` updates its first series position.
    """

    def __init__(self, field: "FieldBasicInterfaceAPI") -> None:
        """
        Initialize the shared adapter and bind ``custom_series_index``.

        Example:
            Construct ``CustomSeriesIndexWriter(index_field)`` before passing book-to-index updates to ``set_books``.


        :param field: Legacy field whose name and metadata select the inherited value adapter.
        :return: None; stores the field and bound update hook.
        """
        super(CustomSeriesIndexWriter, self).__init__(field)
        self.set_books_func = self.custom_series_index

    @staticmethod
    def custom_series_index(book_id_val_map, db: "CatalogAPI", field, *args) -> set[int]:
        """
        Assign cached indexes and persist those with a linked series.

        For each book, inspect ``series_field.ids_for_book`` and use its first ID, wrapping a scalar integer in a tuple. Assign every requested cache value before issuing one batch macro call. Books without a truthy linked-ID result affect the cache but are absent from the returned set. A later lookup or write failure does not roll back earlier cache assignments.

        Example:
            For a book linked to series 4, ``custom_series_index({7: None}, db, field)`` writes index 1.0 for link (7, 4) and returns {7}. With no link it still caches 1.0 but returns an empty set.


        :param book_id_val_map: Book IDs mapped to new index values; None becomes 1.0.
        :param db: Database adapter wrapped or called by the writer; collaborator failures propagate.
        :param field: Index field exposing series_field, table.book_col_map and table/column metadata.
        :param args: Additional compatibility arguments, ignored by this hook.
        :return: Set of book IDs included in the database update sequence.
        """
        series_field = field.series_field
        sequence = []
        for book_id, sidx in iteritems(book_id_val_map):
            if sidx is None:
                sidx = 1.0
            ids = series_field.ids_for_book(book_id)
            if ids:
                if isinstance(ids, int):
                    ids = (ids,)
                sequence.append((sidx, book_id, ids[0]))
            field.table.book_col_map[book_id] = sidx

        if sequence:
            db.macros.update_custom_column_additional_column_many(
                table=field.metadata["table"],
                column=field.metadata["column"],
                sequence=sequence,
            )

        return {s[1] for s in sequence}
