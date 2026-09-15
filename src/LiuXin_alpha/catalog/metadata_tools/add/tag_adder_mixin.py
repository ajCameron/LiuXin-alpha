"""
Insert a Tag with a supplied or generated search hash.
"""

from __future__ import unicode_literals

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.metadata.standardization import make_tag_search_term


class TagAdderMixin:
    """
    Supply tag creation to a legacy Add host.

    The host provides the database and any peers required by the method.
    Validation and synchronization failures propagate to the caller.

    Example:
        Supplying an empty hash preserves it because only None requests generation.
    """

    def tag(self, tag, tag_phash=None):
        """
        Insert a Tag with a supplied or generated search hash.

        Example:
            Supplying an empty hash preserves it because only None requests generation.


        :param tag: Tag text preserved unchanged.
        :param tag_phash: Search hash; None calls make_tag_search_term(tag).
        :return: Created database Row; synchronization and schema errors propagate.
        """
        tag_row = Row(database=self.db)
        tag_row["tag"] = tag
        tag_row["tag_phash"] = tag_phash if tag_phash is not None else make_tag_search_term(tag)
        tag_row.sync()
        return tag_row
