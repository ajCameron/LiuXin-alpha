
"""
Resolve legacy language updates and forward typed Work links to Catalog writers.
"""

from __future__ import division, absolute_import, print_function, unicode_literals, annotations

from typing import TYPE_CHECKING

from LiuXin_alpha.caches.write.base_writer import BaseWriter
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
    Accept language codes or typed language lists without scalar adaptation.

    Writes resolve Catalog language identities and target Work-language links. This class does not directly refresh the supplied legacy field cache.

    Example:
        For a configured field, ``LanguagesWriter(field).set_books({7: "eng"}, db)`` resolves English and writes a primary Work-language link.
    """

    def __init__(self, field: "FieldBasicInterfaceAPI") -> None:
        """
        Initialize shared writer state and bind the unadapted language hook.

        Example:
            Use ``LanguagesWriter(field)`` for updates such as ``{7: {"primary": ["eng"]}}``.


        :param field: Legacy field whose name and metadata select the inherited value adapter.
        :return: None; stores the field and binds no_adapter_set_books and set_languages.
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
        Resolve each language payload and delegate a typed link write for its Work.

        A string must resolve by ``catalog.languages.exact`` and is written as one LinkValue with link_type="primary". A dictionary requires at most one entry in its "primary" list, string type keys and list values; integer IDs pass ``languages.require``, strings use exact matching, and all resolved links are passed as one tuple. Empty dictionaries produce empty link tuples. Resolution finishes per Work before that Work is written; failures do not undo earlier Work writes. No legacy field cache is refreshed here.

        Example:
            With existing language IDs, ``set_languages({7: {"primary": [1], "secondary": [2]}}, db, field)`` forwards two typed LinkValue objects for Work 7.


        :param book_id_val_map: Work IDs mapped to a language-code string or a dictionary of link-type strings to lists of language IDs/codes.
        :param db: Database adapter wrapped or called by the writer; collaborator failures propagate.
        :param field: Compatibility field argument; this hook does not inspect or update it.
        :param args: Additional compatibility arguments, ignored by this hook.
        :return: Set of all supplied Work IDs after all delegated writes succeed.
        :raises ValueError: A language code does not resolve to a matched entity ID.
        :raises AssertionError: The primary list has more than one value, or dictionary key/value types violate the asserted shape.
        :raises NotImplementedError: A top-level payload or nested language value has an unsupported type.
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
