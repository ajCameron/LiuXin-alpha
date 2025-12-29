from __future__ import unicode_literals

import queue as Queue
import re

from collections import defaultdict, OrderedDict
from copy import deepcopy

import six
from six import string_types

from LiuXin.metadata.constants import CREATOR_TYPES
from LiuXin.metadata.constants import EXTERNAL_EBOOK_ID_SCHEMA
from LiuXin.metadata.constants import INTERNAL_EBOOK_ID_SCHEMA

from LiuXin.databases.row import Row
from LiuXin.databases.fingerprints import generate_title_fingerprint

from LiuXin.exceptions import InputIntegrityError
from LiuXin.exceptions import DatabaseIntegrityError

from LiuXin.library.standardization import standardize_creator_name
from LiuXin.library.standardization import make_creator_phash
from LiuXin.library.standardization import gen_title_author_phash
from LiuXin.library.standardization import standardize_genre
from LiuXin.library.standardization import standardize_language
from LiuXin.library.standardization import make_tag_search_term
from LiuXin.library.standardization import standardize_tag
from LiuXin.library.standardization import make_title_search_term
from LiuXin.library.standardization import standardize_title
from LiuXin.library.standardization import standardize_identifier
from LiuXin.library.standardization import standardize_publisher
from LiuXin.library.standardization import standardize_series
from LiuXin.library.standardization import make_series_phash

from LiuXin.metadata import authors_to_sort_string
from LiuXin.metadata import author_to_author_sort
from LiuXin.metadata import title_sort as generate_title_sort
from LiuXin.metadata import check_isbn
from LiuXin.metadata import check_issn
from LiuXin.metadata import check_doi
from LiuXin.metadata.metadata_standardize import standardize_id_name

from LiuXin.utils.date import isoformat_timestamp, utcnow
from LiuXin.utils.general_ops.id_creation_tools import get_unique_group_id
from LiuXin.utils.logger import default_log

from LiuXin.utils.lx_libraries.liuxin_six import six_unicode

# Todo: Make sure that all the tables with tree like structure have parent, depth, tree_id and full
# Todo: Make sure that all tables have a phash and sort field


# ----------------------------------------------------------------------------------------------------------------------
#
# - CLASS FOR ALL THE METHODS TO ADD METADATA TO THE LIBRARY
#
# ----------------------------------------------------------------------------------------------------------------------
# Every table which can be added to directly has a method here
# So to add some metadata to the library call library.add.table_name() - with the required parameters
# This also allows you to swap other add classes in more easily, if they've been rewritten
class Add(object):
    """
    Class to add rows to the library.
    There is no rating method here - because all ratings should already have been added.
    This method is for metadata - to find the methods used to add objects physically look in the folder_stores,adder
    method.
    """

    def __init__(self, database):
        self.db = database
        self.ensure = None
        self.apply = None

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
        One and only one book is allowed per title row. This is enforced by a foreign key constraint. If you try and add
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

    def comment(self, comment):
        """
        Add a comment to the database.
        :param note: The text of the note.
        :return note_row: The row for the new note
        """
        comment_row = Row(database=self.db)
        comment_row["comment"] = comment
        comment_row.sync()
        return comment_row

    def creator(
        self,
        creator,
        creator_sort=None,
        creator_short_name=None,
        creator_last_name=None,
        creator_phash=None,
        creator_legal_name=None,
        creator_birth_date=None,
        creator_death_date=None,
        creator_type="authors",
        creator_seminal_work=None,
        creator_one_person=True,
        creator_wikipedia=None,
        creator_imdb=None,
        creator_link=None,
        creator_created_datestamp=None,
        creator_datestamp=None,
        creator_language=None,
        creator_bio=None,
        creator_image=None,
    ):
        """
        Add to the creator table - no collision checking will be preformed.

        :param creator: The name of the creator used in all their works (J. R. R. Tolkien)
        :param creator_sort: Sort of the creators table - e.g. Tolkien, J. R. R.
        :param creator_short_name: A shortened form of the creators name (ideally unique) - e.g. Tolkien
        :param creator_last_name: The creators surname (Tolkien)
        :param creator_phash: Used to fuzzily match the creator
        :param creator_legal_name: The legal name of the creator (probably just the full name)
                                   (i.e. John Ronald Reuel Tolkien)
        :param creator_birth_date: When was the creator born?
        :param creator_death_date: When did the creator die?
        :param creator_type: What role does this creator serve by default? (authors, editors, e.t.c)
        :param creator_seminal_work: What work is the creator most famous for?
        :param creator_one_person:
        :param creator_wikipedia: A link to the wikipedia article on the creator
        :param creator_imdb: A link to the IMDB page for the creator
        :param creator_link: A link to the creators website.

        :param creator_language: What language did the creator work in? Creates an associate between the creator and a
                                 language in the languages table.
        :param creator_bio: A biography for the creator - will be added to the notes table and the id will be entered
                            here.
        :param creator_image: Not implemented at present
        :return new_creator_row: The row once all the information has been added to it and it's been synced to the
                                 database.
        """
        # Make the actual creator row
        creator_row = Row(database=self.db)

        # Set creator name information
        creator_row["creator"] = creator
        creator_row["creator_sort"] = author_to_author_sort(creator) if creator_sort is None else creator_sort
        creator_row["creator_short_name"] = creator if creator_short_name is None else creator_short_name
        creator_row["creator_last_name"] = creator.split(" ")[-1] if creator_last_name is None else creator_last_name
        creator_row["creator_phash"] = make_creator_phash(creator) if creator_phash is None else creator_phash
        creator_row["creator_legal_name"] = creator_legal_name if creator_legal_name is not None else creator

        # Creator dates
        creator_row["creator_birth_date"] = creator_birth_date
        creator_row["creator_death_date"] = creator_death_date

        # Creator works metadata
        creator_type = creator_type.lower().strip()
        if creator_type not in CREATOR_TYPES:
            err_str = "Unable to create_creator - creator type was not recognized."
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("creator_type", creator_type),
                ("CREATOR_TYPES", CREATOR_TYPES),
            )
            raise InputIntegrityError(err_str)
        creator_row["creator_type"] = creator_type
        creator_row["creator_seminal_work"] = creator_seminal_work
        creator_row["creator_one_person"] = creator_one_person

        # Creator's online prescnece
        creator_row["creator_wikipedia"] = creator_wikipedia
        creator_row["creator_imdb"] = creator_imdb
        creator_row["creator_link"] = creator_link

        # Row creation dates
        creator_row["creator_created_datestamp"] = utcnow() if creator_created_datestamp is None else creator_datestamp
        creator_row["creator_datestamp"] = creator_datestamp

        creator_row.sync()

        # Set the assets associated with the creator
        if creator_image is not None:
            info_str = "Cannot set creator_image at present - not implemented"
            default_log.info(info_str)

        # Now that the row exists and has been added to the database associate the other rows with it
        if creator_language is None:
            pass
        elif creator_language is not None and isinstance(creator_language, Row):
            self.apply.language(language=creator_language, resource_row=creator_row)
        else:
            err_str = "Unable to parse creator_language - creator_language must be a row"
            err_str = default_log.log_variables(err_str, "ERROR", ("creator_language", creator_language))
            raise NotImplementedError(err_str)

        # Add the bio (if any)
        if creator_bio is None:
            pass
        elif creator_bio is not None and isinstance(creator_bio, Row):
            self.apply.note(note=creator_bio, resource=creator_row)
        elif creator_bio is not None and isinstance(creator_bio, string_types):
            self.apply.note(note=creator_bio, resource=creator_row)
        else:
            err_str = "Unable to parse creator_language - creator_language must be a row or a string"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("creator_language", creator_language),
                ("creator_lagnuage_type", type(creator_language)),
            )
            raise NotImplementedError(err_str)

        return creator_row

    def genre(
        self,
        genre,
        genre_sort=None,
        genre_phash=None,
        genre_parent=None,
        genre_position=None,
        genre_full=None,
        genre_datestamp=None,
    ):
        """
        Create an entry in the genre table.

        :param genre: The string name of the genre
        :param genre_full:
        :param genre_parent: The parent row of the genre - can only be another genre row
        :type genre_parent: LiuXin row object
        :param genre_position: The position of the sub-genre underneath that parent - this allows you to change the
                               view priority for sub-genres - sub-genres with this set will always rank above those
                               that don't.
        :param genre_datestamp:
        :return:
        """
        genre_row = Row(database=self.db)

        # Set the genre name strings
        genre_row["genre"] = genre
        genre_row["genre_sort"] = genre_sort
        genre_row["genre_phash"] = genre_phash

        # Set the genre tree positions
        genre_row["genre_parent"] = six_unicode(genre_parent.row_id) if genre_parent is not None else genre_parent
        genre_row["genre_position"] = genre_position
        genre_row["genre_full"] = genre_full

        genre_row["genre_datestamp"] = genre_datestamp if genre_datestamp is not None else utcnow()

        genre_row.sync()

        return genre_row

    def identifier(self, identifier, identifier_type):
        """
        Create an entry in the identifiers table.
        :param identifier:
        :param identifier_type:
        :return:
        """
        return self.ensure.identifier(identifier, identifier_type)

    def language(self, language_name, language_code):
        """
        Create an entry in the languages table of the database.
        :param language_name: The name of the language
        :param language_code: It's code
        :return:
        """
        language_row = Row(database=self.db)

        language_row["language"] = language_name
        language_row["language_code"] = language_code

        language_row.sync()
        return language_row

    def note(self, note):
        """
        Add a note to the database
        :param note: The text of the note.
        :return note_row: The row for the new note
        """
        note_row = Row(database=self.db)

        note_row["note"] = note

        note_row.sync()

        return note_row

    def publisher(
        self,
        publisher,
        publisher_sort=None,
        publisher_phash=None,
        publisher_description=None,
        publisher_wikipedia=None,
        publisher_website=None,
        publisher_parent=None,
        publishr_position=None,
        publisher_full=None,
    ):
        """
        Create an entry in the publisher table.
        :param publisher: The name of the publisher
        :param publisher_description: A row from the notes table - has to already have an id
        :param publisher_wikipedia: A link to the wikipedia page for the publisher
        :param publisher_website: A link to the website for the publisher
        :param publisher_parent: A row which will be set as the parent row for the publisher
        :return:
        """
        # publisher_tree_id: Publishers have a tree structure. This is a unique_id for each tree in the publishers
        #                    table
        publisher_row = Row(database=self.db)

        publisher_row["publisher"] = publisher
        publisher_row["publisher_sort"] = publisher_sort
        publisher_row["publisher_phash"] = publisher_phash

        if publisher_description is None:
            pass
        elif isinstance(publisher_description, Row):
            pass
        elif isinstance(publisher_description, string_types):
            publisher_description = self.note(note=publisher_description)
        else:
            raise NotImplementedError

        publisher_row["publisher_wikipedia"] = publisher_wikipedia
        publisher_row["publisher_website"] = publisher_website

        if publisher_parent is not None:
            publisher_row["publisher_parent"] = publisher_parent["publisher_id"]
        publisher_row["publisher_position"] = publishr_position
        publisher_row["publisher_full"] = publisher_full

        publisher_row.sync()

        # Interlink the description
        if publisher_description is not None:
            self.db.interlink_rows(primary_row=publisher_row, secondary_row=publisher_description)

        return publisher_row

    def series(
        self,
        series,
        series_sort=None,
        series_phash=None,
        series_parent=None,
        series_parent_position=None,
        series_full=None,
        series_creator=None,
        series_note=None,
    ):
        """
        Create a series record in the series table of the database.
        Ideally this method would be provided with a series_creator, which will be linked to the series row as the
        archetype creator - but it's not required and will probably work fine without it.
        No checks are run to see if the series exists already.
        :param series: The name of the series
        :param series_creator: Every series should be linked to
        :param series_sort: The sort name of the series
        :param series_parent: Does the series have a parent series
        :param series_parent_position: If the series has a parent what position should it have in the parent sort
        :param series_note: A note to be attached to the series
        :return:
        """
        # series_full: A full string representation of the entire series tree
        # series_tree_id: Each series should have one - a unique tree to identify which series' it's associated with
        # series_phash: A combination of the series and main creator (if there's one) - used for searching - will be
        #               generated by this method

        series_row = Row(database=self.db)
        series_row["series"] = series
        series_row["series_sort"] = series_sort if series_sort is not None else generate_title_sort(series)
        if series_phash is None:
            if series_creator is not None:
                series_row["series_phash"] = make_series_phash(series_creator["creator"], series)
            else:
                series_row["series_phash"] = make_series_phash("", series)
        else:
            series_row["series_phash"] = series_phash

        if series_parent is None:
            series_row["series_parent"] = None
            series_row["series_parent_position"] = None
        elif isinstance(series_parent, Row):
            series_row["series_parent"] = series_parent["series_id"]
            series_row["series_parent_position"] = series_parent_position
        else:
            err_str = "Can only set the series parent with another series row"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("series_parent", series_parent),
                ("series_parent_type", type(series_parent)),
            )
            raise InputIntegrityError(err_str)
        series_row["series_full"] = series_full

        # Create the series row - link to the creator row, if one is present
        series_row.sync()

        # Link the creator row to the series row - if one is present
        if series_creator is None:
            pass
        elif isinstance(series_creator, Row):
            self.apply.creator(resource_row=series_row, creator_row=series_creator)
        else:
            err_str = "Unable to parse series_creator value - was not a string or row"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("series_creator", series_creator),
                ("series_creator_type", type(series_creator)),
            )
            raise InputIntegrityError(err_str)

        # Link the note row to the series row - if applicable
        if series_note is None:
            pass
        elif isinstance(series_note, Row):
            self.apply.note(note=series_note, resource=series_row)
        elif isinstance(series_note, string_types):
            note_row = self.note(series_note)
            self.apply.note(note=note_row, resource=series_row)
        else:
            err_str = "Unable to parse series_note value - was not a string or row"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("series_note", series_note),
                ("series_note_type", type(series_note)),
            )
            raise InputIntegrityError(err_str)

        return series_row

    def subject(self, subject, subject_sort=None, subject_parent=None):
        """
        Makes an entry in the subjects table. Subject is what a work is about. Genre is what type of work it is.
        Effectively another form of tag - except this one provides a tree hierarchy.
        :param subject:
        :param subject_parent:
        :return:
        """
        subject_row = Row(database=self.db)

        subject_row["subject"] = subject
        subject_row["subject_sort"] = subject_sort if subject_sort is not None else make_title_search_term(subject)

        if subject_parent is None:
            subject_row["subject_parent"] = None
        elif subject_parent is not None and isinstance(subject_parent, Row):
            subject_row["subject_parent"] = subject_parent.row_id
        else:
            err_str = "Unable to parse subject_parent - expected a Row"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("subject_parent", subject_parent),
                ("subject_parent_type", type(subject_parent)),
            )
            raise NotImplementedError(err_str)

        subject_row.sync()

        return subject_row

    def synopsis(self, synopsis):
        """
        Makes an entry in the NOTES table and returns the row.
        When the row is interlinked with the title the type has to be recorded as synopsis - this is the only difference
        between a synopsis and a note.
        :param synopsis:
        :return:
        """
        synopsis_row = Row(database=self.db)
        synopsis_row["synopsis"] = synopsis
        synopsis_row.sync()
        return synopsis_row

    def tag(self, tag, tag_phash=None):
        """
        Make a tag and return the row of the new tag.
        This method includes no checking to see if collisions are going to occur. Use ensure_tag to run collision
        checking.
        In almost all circumstances you should be using ensure_tag, not this method.
        :param tag:
        :return:
        """
        tag_row = Row(database=self.db)
        tag_row["tag"] = tag
        tag_row["tag_phash"] = tag_phash if tag_phash is not None else make_tag_search_term(tag)
        tag_row.sync()
        return tag_row

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
