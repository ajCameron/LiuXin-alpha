
"""
Retain the standalone legacy Label insertion mixin and tag-named alias.
"""


from __future__ import unicode_literals

import queue as Queue
import re

from collections import defaultdict, OrderedDict
from copy import deepcopy

import LiuXin_alpha.utils.libraries.liuxin_six as six
from LiuXin_alpha.utils.libraries.liuxin_six import string_types

from LiuXin_alpha.metadata.constants import CREATOR_TYPES, EXTERNAL_EBOOK_ID_SCHEMA, INTERNAL_EBOOK_ID_SCHEMA

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.databases.hashes import generate_title_fingerprint

from LiuXin_alpha.errors import InputIntegrityError, DatabaseIntegrityError

from LiuXin_alpha.metadata.standardization import standardize_creator_name, make_creator_phash, gen_title_author_phash
from LiuXin_alpha.metadata.standardization import standardize_genre
from LiuXin_alpha.metadata.standardization import standardize_language
from LiuXin_alpha.metadata.standardization import make_tag_search_term
from LiuXin_alpha.metadata.standardization import standardize_tag
from LiuXin_alpha.metadata.standardization import make_title_search_term
from LiuXin_alpha.metadata.standardization import standardize_title
from LiuXin_alpha.metadata.standardization import standardize_identifier
from LiuXin_alpha.metadata.standardization import standardize_publisher
from LiuXin_alpha.metadata.standardization import standardize_series
from LiuXin_alpha.metadata.standardization import make_series_phash

from LiuXin_alpha.metadata.utils import authors_to_sort_string
from LiuXin_alpha.metadata.utils import author_to_author_sort
from LiuXin_alpha.metadata.utils import title_sort as generate_title_sort
from LiuXin_alpha.metadata.utils import check_isbn
from LiuXin_alpha.metadata.utils import check_issn
from LiuXin_alpha.metadata.utils import check_doi
from LiuXin_alpha.metadata.standardize import standardize_id_name

from LiuXin_alpha.utils.date import isoformat_timestamp, utcnow
from LiuXin_alpha.utils.identifiers import get_unique_group_id
from LiuXin_alpha.utils.logging import default_log

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

from LiuXin_alpha.databases.api import RowAPI

from typing import Optional


class LabelMixin:
    """
    Supply Label insertion outside the current Add composition.

    This retained mixin is not inherited by Add, whose tag method creates Tags.

    Example:
        A custom host inheriting LabelMixin can call label(text) or its tag alias.
    """
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
        tag_row.sync()

        return tag_row

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

