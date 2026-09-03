"""Comment creation and linking workflows for metadata tools."""

from __future__ import unicode_literals

from LiuXin_alpha.databases.row import Row


class CommentAdderMixin:
    """
    Add methods for rows in the ``comments`` table.
    """

    def comment(self, comment):
        """
        Add a comment row and return it.
        """
        comment_row = Row(database=self.db)
        comment_row["comment"] = comment
        comment_row.sync()
        return comment_row
