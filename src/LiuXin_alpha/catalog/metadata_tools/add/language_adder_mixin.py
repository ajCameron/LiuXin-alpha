"""Language creation and linking workflows for metadata tools."""

from __future__ import unicode_literals

from LiuXin_alpha.databases.row import Row


class LanguageAdderMixin:
    """
    Add methods for rows in the ``languages`` table.
    """

    def language(self, language_name, language_code):
        """
        Create a language row.
        """
        language_row = Row(database=self.db)
        language_row["language"] = language_name
        language_row["language_code"] = language_code
        language_row.sync()
        return language_row
