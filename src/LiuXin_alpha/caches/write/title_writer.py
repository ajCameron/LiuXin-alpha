
"""
Write legacy title columns and then request the companion title-sort update.
"""

from __future__ import division, absolute_import, print_function, unicode_literals

from typing import TYPE_CHECKING, Optional

from LiuXin_alpha.caches.write.generic_writers.one_to_one_writer import OneToOneWriter
from LiuXin_alpha.metadata.ebook_metadata_tools import title_sort
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems


if TYPE_CHECKING:

    from LiuXin_alpha.catalog.api import CatalogAPI
    from LiuXin_alpha.caches.api.storage_cache_api import FieldBasicInterfaceAPI



class TitleWriter(OneToOneWriter):
    """
    Extend scalar field writes with a follow-up title-sort calculation.

    The inherited books-table path derives the actual destination from field metadata. A later sort update failure does not undo the title write.

    Example:
        With title metadata and a companion sort field, ``TitleWriter(field).set_books({7: "The Example"}, db)`` writes the title and then its calculated sort value.
    """
    def __init__(self, field) -> None:
        """
        Initialize scalar writer behavior and select the title hook.

        Example:
            Construct ``TitleWriter(title_field)`` where ``title_field.title_sort_field.writer`` can accept the follow-up mapping.


        :param field: Legacy field whose name and metadata select the inherited value adapter.
        :return: None; binds set_title as the inherited set_books hook.
        """
        super(TitleWriter, self).__init__(field)

        self.set_books_func = self.set_title

    def set_title(
            self,
            book_id_val_map, db: "CatalogAPI",
            field: Optional["FieldBasicInterfaceAPI"] = None, *args) -> set[int]:
        """
        Write titles through the scalar helper, then update companion sort values.

        The primary scalar update runs first. If field is not None, calculate ``title_sort`` for each input title and call its companion writer with that mapping. Exceptions from sorting or the second write propagate after the primary change. Passing None does not provide a metadata-free write: the inherited scalar helper requires a field.

        Example:
            For a configured title field, ``writer.set_title({7: "The Example"}, db, field)`` first writes the title, then forwards ``{7: title_sort("The Example")}`` to its sort writer.


        :param book_id_val_map: Book IDs mapped to title values, normally adapted by set_books.
        :param db: Database adapter wrapped or called by the writer; collaborator failures propagate.
        :param field: Title field metadata and optional companion title_sort_field; despite the default, the inherited helper dereferences this argument.
        :param args: Additional arguments forwarded to one_one_in_books, where they are logged.
        :return: The affected-ID set returned by one_one_in_books; the companion writer result is discarded.
        """
        # Update the titles in the database
        ans = self.one_one_in_books(book_id_val_map, db, field, *args)

        # Update the title sort field - if required
        if field is not None:
            field.title_sort_field.writer.set_books({k: title_sort(v) for k, v in iteritems(book_id_val_map)}, db)
        return ans
