
"""
Write language information into the database.
"""


from __future__ import division, absolute_import, print_function, unicode_literals, annotations

from typing import TYPE_CHECKING

from LiuXin_alpha.catalog.write import BaseWriter
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems, six_string_types
from LiuXin_alpha.utils.logging import default_log

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


class LanguagesWriter(BaseWriter):
    """
    Class for writing languages information out to the table.
    """

    def __init__(self, field) -> None:
        """
        Constructor.

        :param field:
        """
        super(LanguagesWriter, self).__init__(field=field)

        self.set_books = self.no_adapter_set_books
        self.set_books_func = self.set_languages

    @staticmethod
    def set_languages(book_id_val_map, db: "DatabaseAPI", field, *args) -> set[int]:
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
        for book_id, lang_code in iteritems(book_id_val_map):

            if isinstance(lang_code, six_string_types):

                # Scrub any primary languages from the languages table - if they exist
                db.metadata_sql.break_lang_title_links(book_id, link_type="primary")

                title_row = db.get_row_from_id("titles", row_id=book_id)
                # Todo: ensure.language is being called at least three times in this module - does it need to be?
                lang_row = db.ensure.language(lang_code, lang_code="either")
                db.interlink_rows(primary_row=title_row, secondary_row=lang_row, type="primary")
                continue

            elif isinstance(lang_code, dict):

                # Scrub all language_title links for the given id from the database - the ones in use will be recreated
                db.metadata_sql.break_lang_title_links(book_id)

                # Going to need the title row to link language rows to it
                title_row = db.get_row_from_id("titles", row_id=book_id)

                # Todo: Spin primary language off into a different table
                # Check that we're not trying to try and set multiple primary languages
                if "primary" in lang_code and len(lang_code["primary"]) not in [0, 1]:
                    raise AssertionError("Trying to set multiple languages primary - stop it!")

                # Todo: Now should be done in the languages table
                for link_type, language_ids in iteritems(lang_code):
                    # Check the status of the link dict before trying to write it out onto the database
                    assert isinstance(
                        link_type, six_string_types
                    ), "link type not a basestring - link update probably malformed"
                    assert isinstance(language_ids, list), "link_ids are not a list - link update probably malformed"

                    for language_id in language_ids:

                        if isinstance(language_id, int):
                            lang_row = db.get_row_from_id("languages", row_id=language_id)
                        elif isinstance(language_id, six_string_types):
                            lang_row = db.ensure.language(language_id, lang_code="either")
                        else:
                            raise NotImplementedError

                        try:
                            db.interlink_rows(
                                primary_row=title_row,
                                secondary_row=lang_row,
                                type=link_type,
                                priority="lowest",
                            )
                        except AttributeError:
                            err_str = "AttributeError while trying to link rows"
                            err_str = default_log.log_variables(
                                err_str,
                                "ERROR",
                                ("language_id", language_id),
                                ("lang_row", lang_row),
                            )
                            raise NotImplementedError(err_str)

                continue

            else:

                raise NotImplementedError("Cannot preformed update - book_id_val_map is not well formed")

        # Just assume that every indicated book has been touched
        return set(book_id_val_map)
