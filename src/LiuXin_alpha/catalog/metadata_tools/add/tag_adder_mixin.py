"""Tag creation and linking workflows for metadata tools."""

from __future__ import unicode_literals

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.metadata.standardization import make_tag_search_term


class TagAdderMixin:
    """
    Add methods for rows in the ``tags`` table.
    """

    def tag(self, tag, tag_phash=None):
        """
        Create a tag row.
        """
        tag_row = Row(database=self.db)
        tag_row["tag"] = tag
        tag_row["tag_phash"] = tag_phash if tag_phash is not None else make_tag_search_term(tag)
        tag_row.sync()
        return tag_row
