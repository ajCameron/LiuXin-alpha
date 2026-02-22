

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



class TitleAddMixin:
    """
    Add a "title" to the database
    """

    # Todo: Enable adding title interlink data in one call
    # So would like to able to note that this is an alt-title for another work with just one call to this method.
    def title(
        self,
        title,
        title_sort=None,
        title_phash=None,
        title_creator_sort=None,
        title_pub_date=None,
        title_copyright_date=None,
        title_wikipedia=None,
        title_fiction_length_category=None,
        title_type=None,
        title_wordcount=None,
        title_source=None,
        title_source_path=None,
        title_source_name=None,
        title_created_datestamp=None,
        title_datestamp=None,
        override_title_row=None,
    ):
        """
        Populate a title row, add it to the database, and return.
        :param title: The title of the work
        :param title_sort: The title_sort for the work - will be set automatically if nothing is provided
        :param title_phash: Phash generated from the title and the creators - which is used to fuzzily match books when
                            adding.
        :param title_creator_sort:  Sort string for the creators of a work
        :param title_pub_date: The publication date for the work
        :param title_copyright_date: The copyright date for the work
        :param title_wikipedia: A wikipedia link to the work
        :param title_fiction_length_category:
        :param title_type: What type of resource is the title?
        :param title_wordcount: What is the title's wordcount?
        :param title_source: Where did the title come from?
        :param title_source_path: The original paths of the files in the book (for debugging).
        :param title_source_name: The original names of all the files
        :param title_created_datestamp: Defaults to now
        :param title_datestamp: When was the title created?
        :param override_title_row: If this is passed in then it's used in place of a generated blank row - useful if
                                   you just want to update the information in a title row.
        :return:
        """
        if override_title_row is None:
            title_row = Row(database=self.db)
        else:
            title_row = override_title_row

        title_row["title"] = title
        title_row["title_sort"] = title_sort if title_sort is not None else generate_title_sort(title)
        title_row["title_phash"] = title_phash if title_phash is not None else make_title_search_term(title)

        title_row["title_creator_sort"] = title_creator_sort

        title_row["title_pub_date"] = title_pub_date
        if title_copyright_date is not None:
            title_row["title_copyright_date"] = title_copyright_date
        else:
            title_row["title_copyright_date"] = title_pub_date
        title_row["title_wikipedia"] = title_wikipedia
        title_row["title_fiction_length_category"] = title_fiction_length_category
        title_row["title_type"] = title_type
        title_row["title_wordcount"] = title_wordcount

        title_row["title_source"] = title_source
        title_row["title_source_path"] = title_source_path
        title_row["title_source_name"] = title_source_name
        title_row["title_created_datestamp"] = (
            title_created_datestamp if title_created_datestamp is not None else utcnow()
        )

        title_row.sync()

        return title_row
