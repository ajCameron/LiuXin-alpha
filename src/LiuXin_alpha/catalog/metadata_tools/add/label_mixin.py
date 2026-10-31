
"""
Retain the standalone legacy Label insertion mixin and tag-named alias.
"""


from __future__ import unicode_literals

from LiuXin_alpha.catalog.row_mutations import atomic_write, persist_row
from LiuXin_alpha.databases.row import Row

from LiuXin_alpha.metadata.standardization import make_tag_search_term

from LiuXin_alpha.databases.api import RowAPI

from typing import Optional


class LabelMixin:
    """
    Supply Label insertion outside the current Add composition.

    This retained mixin is not inherited by Add, whose tag method creates Tags.

    Example:
        A custom host inheriting LabelMixin can call label(text) or its tag alias.
    """
    @atomic_write
    def label(self, tag: str, tag_phash: Optional[str] = None) -> RowAPI:
        """
        Insert a Label with a supplied or generated tag-style search hash.

        Example:
            A label is persisted in label columns even when invoked through this mixin's tag alias.


        :param tag: Label text stored unchanged; parameter retains the legacy tag spelling.
        :param tag_phash: Hash override; None calls make_tag_search_term.
        :return: Created database Row; synchronization and schema errors propagate.
        """
        tag_row = Row(database=self.db)

        tag_row["label"] = tag
        tag_row["label_phash"] = tag_phash if tag_phash is not None else make_tag_search_term(tag)
        persist_row(self, tag_row)

        return tag_row

    @atomic_write
    def tag(self, tag: str, tag_phash: Optional[str] = None) -> RowAPI:
        """
        Forward the historical tag method to Label insertion.

        Example:
            A label is persisted in label columns even when invoked through this mixin's tag alias.


        :param tag: Label text stored unchanged; parameter retains the legacy tag spelling.
        :param tag_phash: Hash override; None calls make_tag_search_term.
        :return: Created database Row; synchronization and schema errors propagate.
        """
        return self.label(tag, tag_phash=tag_phash)
