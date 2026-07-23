
"""
Write language information into the database.
"""

from __future__ import division, absolute_import, print_function, unicode_literals, annotations

from typing import TYPE_CHECKING

from LiuXin_alpha.caches.write import BaseWriter
from LiuXin_alpha.catalog import Catalog
from LiuXin_alpha.databases.macro_types import LinkValue
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems, six_string_types

if TYPE_CHECKING:

    from LiuXin_alpha.catalog.api import CatalogAPI
    from LiuXin_alpha.caches.api.storage_cache_api.storage_fields_api.base_field import (
        FieldBasicInterfaceAPI,
    )


class LanguagesWriter(BaseWriter):
    """
    Class for writing languages information out to the table.
    """

    def __init__(self, field: "FieldBasicInterfaceAPI") -> None:
        """
        Constructor.

        :param field:
        """
        super(LanguagesWriter, self).__init__(field=field)

        self.set_books = self.no_adapter_set_books
        self.set_books_func = self.set_languages

    @staticmethod
    def set_languages(
            book_id_val_map,
            db: "CatalogAPI",
            field: "FieldBasicInterfaceAPI",
            *args) -> set[int]:
        """
        Preforms a set into the languages table.

        Parses the :param book_id_val_map: and uses the information it provides to update the links between the titles
        table and the languages table.
        :param book_id_val_map: Assume that we receive a directory keyed with the book id and valued with the language CODE.
        :param db:
        :param field:
        :param args:
        :return:
        """
        catalog = Catalog(db)
        writer = catalog.create_writer("works", "language")
        for book_id, lang_code in iteritems(book_id_val_map):

            if isinstance(lang_code, six_string_types):
                language_match = catalog.languages.exact(lang_code)
                if not language_match.is_match or language_match.entity_id is None:
                    raise ValueError("Language could not be resolved: {!r}".format(lang_code))
                writer.write(
                    {book_id: LinkValue(language_match.entity_id)},
                    link_type="primary",
                )

                continue

            elif isinstance(lang_code, dict):
                # Todo: Spin primary language off into a different table
                # Check that we're not trying to try and set multiple primary languages
                if "primary" in lang_code and len(lang_code["primary"]) not in [0, 1]:
                    raise AssertionError("Trying to set multiple languages primary - stop it!")

                # Todo: Now should be done in the languages table
                links = []
                for link_type, language_ids in iteritems(lang_code):
                    # Check the status of the link dict before trying to write it out onto the database
                    assert isinstance(
                        link_type, six_string_types
                    ), "link type not a basestring - link update probably malformed"
                    assert isinstance(language_ids, list), "link_ids are not a list - link update probably malformed"

                    for language_id in language_ids:

                        if isinstance(language_id, int):
                            catalog.languages.require(language_id)
                            resolved_id = language_id
                        elif isinstance(language_id, six_string_types):
                            language_match = catalog.languages.exact(language_id)
                            if not language_match.is_match or language_match.entity_id is None:
                                raise ValueError(
                                    "Language could not be resolved: {!r}".format(language_id)
                                )
                            resolved_id = language_match.entity_id
                        else:
                            raise NotImplementedError
                        links.append(LinkValue(resolved_id, link_type=link_type))
                writer.write({book_id: tuple(links)})

                continue

            else:

                raise NotImplementedError("Cannot preformed update - book_id_val_map is not well formed")

        # Just assume that every indicated book has been touched
        return set(book_id_val_map)
