"""
Insert a Tag through the Catalog identity policy.
"""

from __future__ import unicode_literals

from LiuXin_alpha.catalog.row_mutations import atomic_write, persist_row
from LiuXin_alpha.databases.row import Row


class TagAdderMixin:
    """
    Supply tag creation to a legacy Add host.

    The host provides the database and any peers required by the method.
    Validation and synchronization failures propagate to the caller.

    Example:
        The catalogue derives the key from tag text, including when a hash is supplied.
    """

    @atomic_write
    def tag(self, tag, tag_phash=None):
        """
        Insert a Tag through the Catalog identity policy.

        Example:
            The catalogue derives the key from tag text, including when a hash is supplied.


        :param tag: Tag text preserved unchanged.
        :param tag_phash: Accepted input key; Catalog rederives it from tag text.
        :return: Created database Row; synchronization and schema errors propagate.
        """
        tag_row = Row(database=self.db)
        tag_row["tag"] = tag
        tag_row["tag_phash"] = tag_phash
        persist_row(self, tag_row)
        return tag_row
