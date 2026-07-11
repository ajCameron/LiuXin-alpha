
"""
Custom column writers - responsible for writing information out to custom columns.
"""

from __future__ import division, absolute_import, print_function, unicode_literals, annotations

from typing import TYPE_CHECKING

from LiuXin_alpha.caches.write import BaseWriter
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems

if TYPE_CHECKING:

    from LiuXin_alpha.catalog.api import CatalogAPI
    from LiuXin_alpha.caches.api.storage_cache_api import FieldBasicInterfaceAPI


class CustomSeriesIndexWriter(BaseWriter):
    """
    Class for writing data out to custom series index tables.
    """

    def __init__(self, field: "FieldBasicInterfaceAPI") -> None:
        """
        Startup the custom series index writer.

        :param field:
        """
        super(CustomSeriesIndexWriter, self).__init__(field)
        self.set_books_func = self.custom_series_index

    @staticmethod
    def custom_series_index(book_id_val_map, db: "CatalogAPI", field, *args) -> set[int]:
        """
        Table of type series have an extra column in their link table - which is the index of that custom series.

        This method writes new values for the custom index out to the database.
        :param book_id_val_map: Keyed with the id of the book and valued with the new index value for that book.
        :param db: The database to preform the update in
        :param field: The base field - the name of the index field will be constructed from that
        :param args: Any additional arguments are ignored
        :return:
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
