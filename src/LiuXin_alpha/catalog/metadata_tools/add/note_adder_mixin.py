"""
Insert a legacy note row from the supplied value.
"""

from __future__ import unicode_literals

from LiuXin_alpha.databases.row import Row


class NoteAdderMixin:
    """
    Supply note creation to a legacy Add host.

    The host provides the database and any peers required by the method.
    Validation and synchronization failures propagate to the caller.

    Example:
        An empty value reaches Row validation unchanged; this helper does not reject it first.
    """

    def note(self, note):
        """
        Insert a legacy note row from the supplied value.

        Assign the identifying column on a new Row and sync once. No resource link
        or reuse lookup is performed.

        Example:
            An empty value reaches Row validation unchanged; this helper does not reject it first.


        :param note: Text assigned unchanged; no local nonempty or type validation.
        :return: Created database Row; synchronization and schema errors propagate.
        """
        note_row = Row(database=self.db)
        note_row["note"] = note
        note_row.sync()
        return note_row
