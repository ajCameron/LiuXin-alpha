
"""
Enables adding label rows to the database.
"""


from __future__ import unicode_literals

import queue as Queue
import re

from collections import defaultdict, OrderedDict
from copy import deepcopy

import six
from six import string_types

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
    Enables adding labels to the system.
    """
    def label(self, tag: str, tag_phash: Optional[str] = None) -> RowAPI:
        """
        Make a tag and return the row of the new tag.

        This method includes no checking to see if collisions are going to occur. Use ensure_tag to run collision
        checking.
        In almost all circumstances you should be using ensure_tag, not this method.
        :param tag:
        :param tag_phash:

        :return:
        """
        tag_row = Row(database=self.db)

        tag_row["label"] = tag
        tag_row["label_phash"] = tag_phash if tag_phash is not None else make_tag_search_term(tag)
        tag_row.sync()

        return tag_row

    def tag(self, tag: str, tag_phash: Optional[str] = None) -> RowAPI:
        """
        In the future, will return a specialized tag row.

        :param tag:
        :param tag_phash:
        :return:
        """
        return self.label(tag, tag_phash=tag_phash)

