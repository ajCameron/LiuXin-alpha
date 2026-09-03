"""Synopsis creation and linking workflows for metadata tools."""

from __future__ import unicode_literals

from LiuXin_alpha.databases.row import Row


class SynopsisAdderMixin:
    """
    Add methods for rows in the ``synopses`` table.
    """

    def synopsis(self, synopsis):
        """
        Add a synopsis row and return it.
        """
        synopsis_row = Row(database=self.db)
        synopsis_row["synopsis"] = synopsis
        synopsis_row.sync()
        return synopsis_row
