"""Note creation and linking workflows for metadata tools."""

from __future__ import unicode_literals

from LiuXin_alpha.databases.row import Row


class NoteAdderMixin:
    """
    Add methods for rows in the ``notes`` table.
    """

    def note(self, note):
        """
        Add a note row and return it.
        """
        note_row = Row(database=self.db)
        note_row["note"] = note
        note_row.sync()
        return note_row
