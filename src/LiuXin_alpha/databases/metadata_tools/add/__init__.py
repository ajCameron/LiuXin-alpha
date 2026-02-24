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

# Todo: Make sure that all the tables with tree like structure have parent, depth, tree_id and full
# Todo: Make sure that all tables have a phash and sort field

from LiuXin_alpha.databases.metadata_tools.add.agent_creator_org_adder_mixin import AgentCreatorOrgMixin


# ----------------------------------------------------------------------------------------------------------------------
#
# - CLASS FOR ALL THE METHODS TO ADD METADATA TO THE LIBRARY
#
# ----------------------------------------------------------------------------------------------------------------------
# Every table which can be added to directly has a method here
# So to add some metadata to the library call library.add.table_name() - with the required parameters
# This also allows you to swap other add classes in more easily, if they've been rewritten
class Add(AgentCreatorOrgMixin):
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

