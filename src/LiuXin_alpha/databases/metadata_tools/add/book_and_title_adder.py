

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

    # Todo: Rationalize the columns - quite a few of them need to go - or become views
    def book(
        self,
        title_row,
        book_sort=None,
        book_flags=None,
        book_pubdate=None,
        book_copyright_date=None,
        book_uuid=None,
        book_has_cover=False,
        book_has_local_cover=None,
        book_last_modified=None,
        book_fingerprint=None,
        book_paths=None,
        book_size=None,
        book_rating=None,
        book_created_datestamp=None,
        book_datestamp=None,
    ):
        """
        Creates an entry in the books table linked to the given title. Needs to be linked to an existing title row.

        Generates everything off that.

        One and only one book is allowed per title row. This is enforced by a foreign key constraint.
        If you try and add
        a book from the same title row twice, you will get an error. Delete that row specifically, using the delete
        methods in library.db, then try to add the book again.

        :param title_row: Every book must be associated with a title - this is the title row that the book will be
                          associated with.
        :param book_sort:
        :param book_flags:
        :param book_pubdate: Date that the book was published on
        :param book_copyright_date: Copyright date of the book in question - latest known.
                               Here instead of over in title because the title date and the copyright date of the work
                               could well differ (if the work has come back into copyright for example - or it could be
                               that the work has been sufficiently re-worked to be copyrighted again.
        :param book_uuid: A unique identifier for the book - of None one will be auto-generated
        :param book_has_cover:
        :param book_has_local_cover:
        :param book_last_modified:
        :param book_fingerprint:
        :param book_paths:
        :param book_size:
        :param book_rating:
        :param book_created_datestamp:
        :param book_datestamp:
        :return:
        """
        # For additional explanations of what these fields are and do, see LiuXin.docs.table_explanations

        # Ensure that the book_id is the same as the title_id and that title doesn't already have a book associated with
        # it
        new_book_id = title_row["title_id"]
        clash_book_rows = self.db.driver_wrapper.search("books", "book_id", new_book_id)
        if clash_book_rows:
            err_str = (
                "Title already has a book - you cannot generate another - if you want to recreate the book "
                "first delete it. Then re-add it."
            )
            default_log.error(err_str)
            raise DatabaseIntegrityError(err_str)

        # Add the book and register it on the database
        book_row_dict = {"book_id": new_book_id}
        self.db.driver_wrapper.add_row(book_row_dict)
        book_row = Row(database=self.db, row_dict=book_row_dict)

        book_creation_time = isoformat_timestamp()
        book_row["book_created_datestamp"] = book_creation_time

        # Add the book row to the database.
        book_row.sync()

        book_row["book_sort"] = book_sort
        book_row["book_flags"] = book_flags

        # Assume, in the absence of an override, that the book_pubdate is the same as the title_pubdate
        book_row["book_pubdate"] = book_pubdate if book_pubdate is None else title_row["title_pub_date"]

        # If the copyright date is not set assume it was the pubdate. If given, If not assume it was the date the title
        # was published.
        if book_copyright_date is not None:
            book_row["book_copyright_date"] = book_copyright_date
        elif book_pubdate is not None:
            book_row["book_copyright_date"] = book_pubdate
        else:
            book_row["book_copyright_date"] = title_row["title_pub_date"]

        book_row["book_uuid"] = book_uuid if book_uuid is not None else get_unique_group_id()

        book_row["book_has_cover"] = book_has_cover
        book_row["book_has_local_cover"] = book_has_local_cover
        book_row["book_last_modified"] = book_last_modified if book_last_modified is not None else book_creation_time

        book_row["book_fingerprint"] = (
            book_fingerprint if book_fingerprint is not None else generate_title_fingerprint(self.db, title_row)
        )

        book_row["book_paths"] = book_paths
        book_row["book_size"] = book_size

        book_row["book_rating"] = book_rating
        book_row["book_created_datestamp"] = book_created_datestamp
        book_row["book_datestamp"] = book_datestamp

        book_row.sync()

        return book_row
