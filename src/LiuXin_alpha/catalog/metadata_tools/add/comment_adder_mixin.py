"""
Insert a legacy comment row from the supplied value.
"""

from __future__ import unicode_literals

from LiuXin_alpha.databases.row import Row


class CommentAdderMixin:
    """
    Supply comment creation to a legacy Add host.

    The host provides the database and any peers required by the method.
    Validation and synchronization failures propagate to the caller.

    Example:
        An empty value reaches Row validation unchanged; this helper does not reject it first.
    """

    def comment(self, comment):
        """
        Insert a legacy comment row from the supplied value.

        Assign the identifying column on a new Row and sync once. No resource link
        or reuse lookup is performed.

        Example:
            An empty value reaches Row validation unchanged; this helper does not reject it first.


        :param comment: Text assigned unchanged; no local nonempty or type validation.
        :return: Created database Row; synchronization and schema errors propagate.
        """
        comment_row = Row(database=self.db)
        comment_row["comment"] = comment
        comment_row.sync()
        return comment_row
