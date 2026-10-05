"""
Insert a language name/code pair without normalization.
"""

from __future__ import unicode_literals

from LiuXin_alpha.databases.row import Row


class LanguageAdderMixin:
    """
    Supply language creation to a legacy Add host.

    The host provides the database and any peers required by the method.
    Validation and synchronization failures propagate to the caller.

    Example:
        The helper stores exactly the supplied code; use Ensure for compatibility lookup.
    """

    def language(self, language_name, language_code):
        """
        Insert a language name/code pair without normalization.

        Example:
            The helper stores exactly the supplied code; use Ensure for compatibility lookup.


        :param language_name: Human-readable name assigned to language.
        :param language_code: Code assigned to language_code without validation.
        :return: Created database Row; synchronization and schema errors propagate.
        """
        language_row = Row(database=self.db)
        language_row["language"] = language_name
        language_row["language_code"] = language_code
        language_row.sync()
        return language_row
