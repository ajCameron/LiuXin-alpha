


from __future__ import with_statement, unicode_literals

import datetime
import os
from collections import OrderedDict
from collections import defaultdict

from LiuXin_alpha.catalog import Catalog
from LiuXin_alpha.catalog.api.common import IdentifierCandidate, MetadataCandidate
from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.databases.macro_types import LinkValue
from LiuXin_alpha.errors import DatabaseIntegrityError

from LiuXin_alpha.metadata.constants import (
    EXTERNAL_EBOOK_ID_SCHEMA,
    INTERNAL_EBOOK_ID_SCHEMA,
    METADATA_NULL_VALUES,
    creator_to_marc,
)
from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import (
    CalibreLikeLiuXinBookMetaData as LiuXinMetaData,
)
from LiuXin_alpha.metadata.ebook_metadata_tools import title_sort, to_epoch_ms
from LiuXin_alpha.utils.logging import default_log

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode


def _epoch_ms(value):
    if value is None:
        return None
    if isinstance(value, int):
        return value
    try:
        return int(to_epoch_ms(value))
    except Exception:
        return None


def _iso_date(value):
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value.date().isoformat()
    if isinstance(value, datetime.date):
        return value.isoformat()
    return str(value)


def _year(value):
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.year
    if value is not None:
        text = str(value).strip()
        if len(text) >= 4 and text[:4].isdigit():
            return int(text[:4])
    return None


def _split_break_joined(value):
    if value is None:
        return ()
    if isinstance(value, (list, tuple)):
        return tuple(str(item) for item in value if item)
    return tuple(part for part in str(value).split("(#BREAK#)") if part)


def _guess_format_detail(*values):
    extensions = {
        extension.lstrip(".").lower()
        for value in values
        for _, extension in (os.path.splitext(str(value).strip()),)
        if extension
    }
    return next(iter(extensions)).upper() if len(extensions) == 1 else None


def _guess_carrier_type(format_detail):
    if format_detail is None:
        return None
    value = format_detail.lower()
    if value in {"epub", "pdf", "mobi", "azw3", "cbz", "cbr", "djvu", "fb2", "txt", "rtf", "docx"}:
        return "ebook"
    if value in {"mp3", "m4b", "flac", "ogg", "aac", "wav"}:
        return "audiobook"
    if value in {"mp4", "mkv", "avi"}:
        return "video"
    return None


# The metadata class - which adds metadata handling capability to the library class


class Metadata:
    """
    Class to add the capability to work with metadata objects to the library.

    Provides read/write to and from metadata objects.
    """

    def __init__(self, database, library, override_fsm=None):
        """
        Startup the database class.
        :param database:
        :param library:
        :param override_fsm:
        """
        self.db = database
        self.lib = library
        try:
            self.fsm = library.fsm
        except AttributeError:
            pass
        if override_fsm is not None:
            self.fsm = override_fsm
        self.semantic_catalog = Catalog(self.db)
        self._semantic_work_link_writers = None

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - FROM BOOK METHODS

    # Todo: Actually needs testing - first need to write the comprehensive test library
    # Todo: Merge with the function in the metadata object itself
    def from_book(self, book_id):
        """
        Take an id of a book on the database - reads all the metadata about that book and returns it in a metadata
        object.
        :param book_id:
        :return:
        """
        md_reader = MetadataFromBookRow(self.lib, book_id)
        md = md_reader.run()

        return md

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - TO METHOD

    # Todo: Things stored in the book row need to be actually stored in the book row
    def to_book(self, md, force_book_id=None, preserve_uuid=None):
        """
        Takes a prepared set of metadata - returns a book row.
        :param md:
        :param force_book_id: If an int then the returned title and book row are guaranteed to have this id.
                              If the row doesn't exist then it'll be created. If the row does exist it'll be
                              overwritten.
        :param preserve_uuid:
        :return:
        """
        title_row, book_row = self.to_title(md, force_book_id=force_book_id, preserve_uuid=preserve_uuid)
        return book_row

    def to_title(self, md, force_book_id=None, preserve_uuid=None):
        """
        Adds the title represented by a metadata object to the table.
        The title is assumed not to exist - it will simply be created.
        :param md: The metadata object
        :param force_book_id: If an int then the returned title and book row are guaranteed to have this id.
                              If the row doesn't exist then it'll be created. If the row does exist it'll be
                              overwritten.
        :param preserve_uuid: If not None, then this value will be set as the books UUID.
        :return:
        """
        # Todo: check application id - if it doesn't match, then discard all the ids
        # Todo: Most of these metadata standardization methods should be moved over into the metadata finalize method

        # Check that this method has been passed the right type of metadata
        assert isinstance(md, LiuXinMetaData), "Can only process LiuXin metadata objects"

        metadata_adder = MeatdataToBookRow(md=md, library=self)

        title_row, book_row = metadata_adder.run(force_book_id=force_book_id, preserve_uuid=preserve_uuid)

        return title_row, book_row


#
# ----------------------------------------------------------------------------------------------------------------------


class MetadataFromBookRow(object):
    """
    Read all the metadata associated with an entry on the books table.
    Return it as a metadata object.
    """

    def __init__(self, library, book_id, cover_form="path", file_form="path"):
        """
        Setup the class - set the global behavior for the read methods.
        :param library: The library object to read the data from
        :param book_id: The id of the book to read the metadata from
        :param cover_form: What form the cover data be included in
        :type cover_form: str - choices are 'path', 'scratch_path', 'raw_data'
        :param file_form: What form the file data should be included in
        :type file_form: str - choices are 'path', 'scratch_path', 'raw_data'
        """
        self.lib = library
        self.db = library.catalog

        self.book_id = book_id
        self.title_row = self.db.get_row_from_id("titles", self.book_id)
        self.book_row = self.db.get_row_from_id("books", self.book_id)

        self.rtn_md = LiuXinMetaData()

        # Constants to determine how the cover and file data is embedded in the metadata object
        assert cover_form in [
            "path",
            "scratch_path",
            "raw_data",
        ], "cover_form not recognized"
        self.cover_form = cover_form
        assert file_form in [
            "path",
            "scratch_path",
            "raw_data",
        ], "file_form not recognized"
        self.file_form = file_form

    def run(self):
        """
        Read data out of the database and load it into a metadata object.
        :return:
        """
        self.read_creators()
        self.read_covers()
        self.read_files()
        self.read_genres()
        self.read_identifiers()
        self.read_notes()
        self.read_comments()
        self.read_synopsis()
        self.read_publishers()
        ts_rows = self.read_series()
        self.read_tags(ts_rows)
        self.read_title_data()
        self.read_language()
        self.read_languages()
        self.read_languages_available()
        self.read_subjects()

        return self.rtn_md

    def read_creators(self):
        """
        Add creators to the metadata object.
        :return:
        """
        # Construct the creators dict, then write it out to the metadata object
        book_creators_dict = self.lib.get_creators_dict(title_row=self.title_row, row_ids=True)
        self.rtn_md.update_creators(creators_dict=book_creators_dict)

    def read_covers(self):
        """
        Load all the covers associated with the book.
        Note the id of the cover on the database if requested.
        :return:
        """
        title_cover_rows = self.db.get_interlinked_rows(primary_row=self.book_row, secondary_table="covers")
        for cover_row in title_cover_rows:

            # Acquire path and add it to the md object
            if self.cover_form == "path":
                cover_loc = self.lib.fsm.get_loc(asset_row=cover_row)
                self.rtn_md.add_cover(data=cover_loc.path, typ="path", cover_id=cover_row["cover_id"])
            else:
                raise NotImplementedError("this form of cover add is not currently supported")

    def read_files(self):
        """
        Load all the files associated with the book.
        Note the id of the file on the database (if requested).
        :return:
        """
        title_file_rows = self.db.get_interlinked_rows(primary_row=self.book_row, secondary_table="files")
        for file_row in title_file_rows:

            # Acquire the path to the asset and add it to the md object
            if self.file_form == "path":
                file_loc = self.lib.fsm.get_loc(asset_row=file_row)
                self.rtn_md.add_file(data=file_loc.path, typ="path", file_id=file_row["file_id"])
            else:
                raise NotImplementedError("this form of file add is not currently supported")

    def read_genres(self):
        """
        Record the genres associated with the given title.
        :return:
        """
        genre_rows = self.db.get_interlinked_rows(primary_row=self.title_row, secondary_table="genres")
        genre_rows.reverse()

        new_genres = OrderedDict()
        for genre_row in genre_rows:
            new_genres[genre_row["genre"]] = genre_row["genre_id"]
        self.rtn_md.direct_add("genre", new_genres)

    def read_identifiers(self):
        """
        Record the identifiers associated with a given title.
        :return:
        """
        md = self.rtn_md

        # Gather the required information
        id_interlink_rows = self.db.get_interlink_rows(primary_row=self.title_row, secondary_table="identifiers")
        ext_identifiers = defaultdict(OrderedDict)
        int_identifiers = defaultdict(OrderedDict)
        for link_row in id_interlink_rows:

            link_type = link_row["identifier_title_link_type"]
            link_ident_id = link_row["identifier_title_link_identifier_id"]
            ident_row = self.db.get_row_from_id("identifiers", link_ident_id)

            if link_type in EXTERNAL_EBOOK_ID_SCHEMA:
                ext_identifiers[link_type][ident_row["identifier"]] = ident_row["identifier_id"]
            elif link_type in INTERNAL_EBOOK_ID_SCHEMA:
                int_identifiers[link_type][ident_row["identifier"]] = ident_row["identifier_id"]
            else:
                err_str = "Identifier type not recognized by the system"
                err_str = default_log.log_variables(
                    err_str,
                    "ERROR",
                    ("link_type", link_type),
                    ("link_row_id", link_row["identifier_title_link_id"]),
                )
                raise KeyError(err_str)

        # Write it out to the metadata object
        for id_type in ext_identifiers:
            md.direct_add(id_type, ext_identifiers[id_type], key_check=True)
        for id_type in int_identifiers:
            md.direct_add(id_type, int_identifiers[id_type])

    def read_notes(self):
        """
        Load everything off the notes table.
        At the moment the notes table includes comments and synopsis - these will be loaded in a separate table.
        :return:
        """
        md = self.rtn_md
        book_title_row = self.title_row

        # Load the actual notes
        note_rows = self.db.get_interlinked_rows(primary_row=book_title_row, secondary_table="notes", type_filter="note")
        notes = OrderedDict()
        for note_row in note_rows:
            notes[note_row["note"]] = note_row
        md.direct_add("notes", notes, key_check=True)

    def read_comments(self):
        """
        Read the comments off the notes table and add it.
        :return:
        """
        comment_rows = self.db.get_interlinked_rows(
            primary_row=self.title_row, secondary_table="notes", type_filter="comment"
        )
        comments = OrderedDict()
        for comment_row in comment_rows:
            comments[comment_row["note"]] = comment_row
        self.rtn_md.direct_add("comments", comments, key_check=True)

    def read_synopsis(self):
        """
        Read the synopsis off the notes table and add them.
        :return:
        """
        synopsis_rows = self.db.get_interlinked_rows(
            primary_row=self.title_row, secondary_table="notes", type_filter="synopses"
        )
        synopses = OrderedDict()
        for synopsis_row in synopsis_rows:
            synopses[synopsis_row["note"]] = synopsis_row
        self.rtn_md.direct_add("synopses", synopses, key_check=True)

    def read_publishers(self):
        """
        Add publishers to the return md object.
        :return:
        """
        title_publisher_rows = self.db.get_interlinked_rows(primary_row=self.title_row, secondary_table="publishers")
        publishers = OrderedDict()
        for publisher_row in title_publisher_rows:
            publishers[publisher_row["publisher"]] = publisher_row
        self.rtn_md.direct_add("publisher", publishers, key_check=True)

    def read_series(self):
        """
        Add the series related fields to the metadata object - this includes the series and the series_index
        OrderedDicts.
        The series field is keyed with the name of the series and valued with it's id. The series index field is keyed
        with the name of the series and valued with the index of the current title in that series.
        :return title_series_rows: The series the title is in - needed later when recording the title series tags.
        """
        # Make appropriate containers for the series and series_index data - fill them with data and then load them into
        # the metadata object
        md_series = METADATA_NULL_VALUES["series"]
        md_series_index = METADATA_NULL_VALUES["series_index"]
        title_series_rows = self.db.get_interlinked_rows(primary_row=self.title_row, secondary_table="series")
        for series_row in title_series_rows:
            # Add the series
            series_name = series_row["series"]
            md_series[series_name] = series_row

            # Add the series index
            st_link = self.db.get_interlink_row(primary_row=self.title_row, secondary_row=series_row)
            md_series_index[series_name] = st_link["series_title_link_index"]
        # Copy them into the database
        self.rtn_md.direct_add(key="series", value=md_series, key_check=True)
        self.rtn_md.direct_add(key="series_index", value=md_series_index, key_check=True)

        return title_series_rows

    def read_tags(self, title_series_rows):
        """
        Transfer all the tags - for the moment ignoring if they're title, series or creator tags.
        :return:
        """
        # Transfer all the tags - for the moment ignoring if they're title, series, or creator tags
        md_tags = OrderedDict()
        title_tag_rows = self.db.get_interlinked_rows(primary_row=self.title_row, secondary_table="tags")
        for tag_row in title_tag_rows:
            md_tags[tag_row["tag"]] = tag_row

        # Copy over all the creators tags
        creator_tag_rows = []
        title_creator_rows = self.db.get_interlinked_rows(primary_row=self.title_row, secondary_table="creators")
        for creator_row in title_creator_rows:
            creator_row_tags = self.db.get_interlinked_rows(primary_row=creator_row, secondary_table="tags")
            creator_tag_rows += creator_row_tags
        for creator_tag_row in creator_tag_rows:
            md_tags[creator_tag_row["tag"]] = creator_tag_row

        # Copy over the series tags
        series_tag_rows = []
        for series_row in title_series_rows:
            series_tags = self.db.get_interlinked_rows(primary_row=series_row, secondary_table="tags")
            series_tag_rows += series_tags
        for series_tag_row in series_tag_rows:
            md_tags[series_tag_row["tag"]] = series_tag_row

    def read_title_data(self):
        """
        Add the data from the title row.
        :return:
        """
        md = self.rtn_md
        title_row = self.title_row

        # Copy the title over
        md.title = title_row["title"]
        # Add the title row
        md.title_row = title_row

        # Add the title sort
        md.title_sort = title_row["title_sort"]
        # -----

        # WORDCOUNT
        md.wordcount = title_row["title_wordcount"]
        # ---------

    def read_language(self):
        """
        Read data for the language field (the primary language of the work) from the languages table and add it to the
        metadata object.
        :return:
        """
        lang_rows = self.db.get_interlinked_rows(
            primary_row=self.title_row,
            secondary_table="languages",
            type_filter="primary",
        )
        if len(lang_rows) == 0:
            return
        self.rtn_md.language = lang_rows[0]["language"]

    def read_languages(self):
        """
        Read data for the languages field (the languages contained or mentioned in the work).
        :return:
        """
        lang_rows = self.db.get_interlinked_rows(
            primary_row=self.title_row,
            secondary_table="languages",
            type_filter="contained_in",
        )
        if len(lang_rows) == 0:
            return

        lang_rows.reverse()
        langs = [l["language"] for l in lang_rows]
        self.rtn_md.direct_add(key="languages", value=langs, key_check=True)

    def read_languages_available(self):
        """
        Read data for the languages_available field - the languages that the work is currently available in.
        :return:
        """
        lang_rows = self.db.get_interlinked_rows(
            primary_row=self.title_row,
            secondary_table="languages",
            type_filter="available_language",
        )
        if len(lang_rows) == 0:
            return

        lang_rows.reverse()
        langs_available = OrderedDict()
        for lang_row in lang_rows:
            langs_available[lang_row["language"]] = lang_row["language_id"]
        self.rtn_md.direct_add(key="languages_available", value=langs_available, key_check=True)

    def read_ratings(self):
        """
        Read values from the ratings table and add them to the metadata object.
        :return:
        """
        rating_link_rows = self.db.get_interlink_rows(primary_row=self.title_row, secondary_table="ratings")

        # Process all the ratings link rows
        for rating_link_row in rating_link_rows:

            rating_row = self.db.get_row_from_id("ratings", rating_link_row["rating_title_link_rating_id"])
            rating_tuple = (
                rating_link_row["rating_title_link_type"],
                rating_row["rating"],
            )
            self.rtn_md.rating = rating_tuple

    def read_subjects(self):
        """
        Read values from the subjects table and add them to the metadata object.
        :return:
        """
        title_series_rows = self.db.get_interlinked_rows(primary_row=self.title_row, secondary_table="subjects")
        title_series_rows.reverse()

        subject_dict = OrderedDict()
        for series_row in title_series_rows:
            subject_dict[series_row["series"]] = series_row["series_id"]

        self.rtn_md.direct_add(key="subject", value=subject_dict, key_check=True)


class MeatdataToBookRow(object):
    """
    Adder to add a metadata object to the table - conveniently encapsulated as a class.
    Some of the objects needed to add metadata to the database - such as the working title row - are common between all
    methods - passing them by keyword is laborious - so using a class.
    """

    # Todo: Check that the application_id matches - if it doesn't then ignore all the ids given in the metadata object
    def __init__(self, md, library):
        """
        Setup the class - set the global behavior for the add methods.
        :param md: md object to add to the database
        :param db: THe database to add the metadata to
        """
        self.md = md
        self.library = library
        self.db = self.library.db

        self.catalog = self.library.semantic_catalog
        writers = self.library._semantic_work_link_writers
        if writers is None:
            writers = {
                column: self.catalog.create_writer("works", column)
                for column in (
                    "agent_canonical_name",
                    "genre",
                    "language",
                    "note",
                    "rating",
                    "series",
                    "subject",
                    "synopsis",
                    "tag",
                )
            }
            self.library._semantic_work_link_writers = writers
        self._work_link_writers = writers
        self.fsm = library.fsm

        self.app_id_match = False

    def run(self, force_book_id=None, preserve_uuid=None):
        """
        Add the metadata object to the database.
        :param force_book_id: If provided then this will be used instead of an automatically generated book row
        :param preserve_uuid: If not None, then this value will be set as the books uuid
        :return:
        """
        # Add the title - this needs to be done first as the majority of things will link to it
        title_row = self.make_title_row(self.md, force_book_id=force_book_id)
        if force_book_id is not None:
            self.__break_connections(title_row)

        # Check to see if the book row already exists - if it doesn't then create it
        cand_book_row = self.db.get_row_from_id("books", title_row["title_id"])
        if cand_book_row is None:
            projected = self.db.search(
                table="books",
                column="book_work_id",
                search_term=title_row["title_id"],
            )
            if not projected:
                raise DatabaseIntegrityError(
                    "Unable to project a book row from the created WEMI stack"
                )
            book_row = projected[0]
        else:
            book_row = cand_book_row
        if preserve_uuid:
            identifier_id = self.catalog.identifiers.match_or_create(
                IdentifierCandidate("calibre_uuid", str(preserve_uuid))
            )
            self.catalog.identifiers.link_to_wemi(
                identifier_id=identifier_id,
                level="work",
                entity_id=int(title_row["title_id"]),
                priority=0,
            )

        self.add_creators(title_row)
        self.add_identifiers(title_row)
        self.add_comments(title_row)
        self.add_notes(title_row)
        self.add_covers(book_row)
        self.add_files(book_row)
        self.add_genres(title_row)
        self.add_language(title_row)
        self.add_languages(title_row)
        self.add_languages_available(title_row)
        self.add_ratings(title_row)
        self.add_series(title_row)
        self.add_synopsis(title_row)
        self.add_tags(title_row)
        self.add_publishers(title_row)
        self.add_subjects(title_row)

        return title_row, book_row

    def __break_connections(self, title_row):
        """Clear replaceable Work metadata through normalized contracts.

        The newly created WEMI path remains attached. Reusable linked values
        are retained as historical rows, while their relationships and owned
        identifiers are removed from the Work before replacement metadata is
        applied.

        :param title_row: Compatibility title projection for the Work.
        :return: None.
        """

        work_id = self._work_id(title_row)
        with self.db.macros.transaction():
            for writer in self._work_link_writers.values():
                writer.write({work_id: ()})
            self.catalog.identifiers.replace_for_wemi(
                level="work",
                entity_id=work_id,
                identifiers={},
            )
            self.catalog.comments.replace_for_wemi(
                level="work",
                entity_id=work_id,
                data=None,
            )

    def make_title_row(self, md, force_book_id=None):
        """
        Read all the title row related metadata out of the md object - add it to a title row and return the row.
        :param md:
        :param force_book_id:
        :return:
        """
        title = md.title
        if not md.is_null("title_sort"):
            title_sort_string = md.title_sort
        else:
            title_sort_string = None

        if not md.is_null("creator_sort"):
            title_creator_sort = md.creator_sort
        else:
            title_creator_sort = None

        if not md.is_null("pubdate"):
            title_pubdate = md.pubdate
        else:
            title_pubdate = None

        if not md.is_null("copyright_date"):
            title_copyright_date = md.copyright_date
        else:
            title_copyright_date = None
        if title_copyright_date is None and not md.is_null("copyright"):
            title_copyright_date = md.copyright

        if not md.is_null("wikipedia"):
            title_wikipedia = md.wikipedia
        else:
            title_wikipedia = None
        if title_wikipedia is None and not md.is_null("title_wikipedia"):
            title_wikipedia = md.title_wikipedia

        if not md.is_null("title_fiction_length_category"):
            title_fiction_length_category = md.title_fiction_length_category
        else:
            title_fiction_length_category = None
        if title_fiction_length_category is None and not md.is_null("length_category"):
            title_fiction_length_category = md.length_category
        if title_fiction_length_category is None and not md.is_null("length"):
            title_fiction_length_category = md.length

        if not md.is_null("title_type"):
            title_type = md.title_type
        else:
            title_type = None
        if title_type is None and not md.is_null("type"):
            title_type = md.type
        if title_type is None and not md.is_null("doc_type"):
            title_type = md.doc_type

        if not md.is_null("title_source"):
            title_source = md.title_source
        else:
            title_source = None
        if title_source is None and not md.is_null("source"):
            title_source = md.source

        # Add in the file data, if any is provided
        # Todo: Check this is also done when adding a format to the book
        title_source_path = None
        if not md.is_null("filepath"):
            file_paths = md.filepath
            # Filter out any bad entries which might have made their way into the list
            file_paths = [six_unicode(p) for p in file_paths if p]
            file_path_str = "(#BREAK#)".join([path for path in file_paths])
            title_source_path = file_path_str

        title_source_name = None
        if not md.is_null("filename"):
            file_names = md.filename
            # Filter out any bad entries which might have made their way into the list
            file_names = [six_unicode(n) for n in file_names if n]
            file_name_str = "(#BREAK#)".join(n for n in file_names)
            title_source_name = file_name_str

        if not md.is_null("title_wordcount"):
            title_wordcount = md.title_wordcount
        else:
            title_wordcount = None
        if title_wordcount is None and not md.is_null("wordcount"):
            title_wordcount = md.wordcount

        source_paths = _split_break_joined(title_source_path)
        source_names = _split_break_joined(title_source_name)
        format_detail = _guess_format_detail(*source_names, *source_paths)
        item_count = max(len(source_paths), len(source_names))
        if item_count == 0:
            item_count = 1
        items = []
        for index in range(item_count):
            source_path = source_paths[index] if index < len(source_paths) else None
            source_name = source_names[index] if index < len(source_names) else None
            if source_name is None and source_path is not None:
                source_name = os.path.basename(source_path)
            items.append(
                {
                    "item_type": "digital" if source_name or source_path else None,
                    "item_source": title_source,
                    "item_source_path": source_path,
                    "item_source_name": source_name,
                }
            )

        publication_year = _year(title_pubdate)
        copyright_date = _iso_date(title_copyright_date or title_pubdate)
        created = self.catalog.mutations.writer.create_wemi_stack(
            work={
                "work_title": title,
                "work_canonical_title": title,
                "work_sort_title": title_sort_string or title_sort(title),
                "work_creator_sort": title_creator_sort,
                "work_type": title_type,
                "work_original_date": _epoch_ms(title_pubdate),
                "work_original_year": publication_year or _year(title_copyright_date),
                "work_original_copyright_date": copyright_date,
                "work_wikipedia_link": title_wikipedia,
                "work_discovery_note": title_source,
            },
            expression={
                "expression_year": publication_year,
                "expression_is_preferred": 1,
                "expression_original_date": _epoch_ms(title_pubdate),
                "expression_original_copyright_date": copyright_date,
                "expression_wordcount": title_wordcount,
                "expression_fiction_length_category": title_fiction_length_category,
            },
            manifestation={
                "manifestation_carrier_type": _guess_carrier_type(format_detail),
                "manifestation_format_detail": format_detail,
                "manifestation_pub_year": publication_year,
                "manifestation_pub_date": _iso_date(title_pubdate),
            },
            items=items,
            origin=title_source,
            work_id=force_book_id,
        )
        title_row = self.db.get_row_from_id("titles", created.work_id)
        if title_row is None:
            raise DatabaseIntegrityError("Created Work has no title projection")
        return title_row

    def add_creators(self, title_row):
        """
        Add creators to the title row.
        Each type of creator has an ordered_dict keyed with the name of the creators and valued with their id on the
        database, or None if they have not yet been added to the database.
        If a row_id is present (and the database id matches) then it is used and the actual name is ignored.
        :return:
        """
        creators_dict = self.md.get_creators_dump()

        # Filter out the empty creator types
        creators_dict = dict((c, creators_dict[c]) for c in creators_dict if creators_dict[c] is not None)
        creator_rows_dict = defaultdict(list)
        for creator_role in creators_dict:
            self.__add_one_creator_role(creators_dict, creator_role, creator_rows_dict, title_row)

    def add_identifiers(self, title_row):
        """
        Process the identifiers - add the internal and external identifiers - no checks are applied to make sure
        that identifiers clash - that should have been done earlier, if it was going to be done at all
        Also the identifiers should have been standardized when they where added to the metadata object
        :return:
        """
        md = self.md

        # External identifiers
        identifiers_dict = md.get_identifiers()
        # Filter out the identifier types which have no useful content
        identifiers_dict = dict((i, identifiers_dict[i]) for i in identifiers_dict if identifiers_dict[i] is not None)
        for id_type in identifiers_dict:
            id_set = identifiers_dict[id_type]
            self.__apply_identifiers_set(title_row, id_type, id_set)

        # Internal identifiers
        internal_ids_dict = md.get_internal_identifiers()
        internal_ids_dict = dict(
            (i, internal_ids_dict[i]) for i in internal_ids_dict if internal_ids_dict[i] is not None
        )
        for int_id_type in internal_ids_dict:
            int_id_set = internal_ids_dict[int_id_type]
            for int_id_val in int_id_set:
                identifier_id = self.catalog.identifiers.match_or_create(
                    IdentifierCandidate(int_id_type, int_id_val)
                )
                self.catalog.identifiers.link_to_wemi(
                    identifier_id=identifier_id,
                    level="work",
                    entity_id=self._work_id(title_row),
                    priority=0,
                )

    def add_comments(self, title_row):
        """
        Pulling any comments out and adding them as notes.
        :param title_row:
        :return:
        """
        comments = self.md.comments
        comments = [c for c in comments if c is not None and c.strip()]
        for comment in comments:
            self.catalog.comments.add_for_wemi(
                level="work",
                entity_id=self._work_id(title_row),
                data={"text": comment},
            )

    def add_notes(self, title_row):
        """
        Store the notes on the notes table as actual notes.
        :param title_row:
        :return:
        """
        notes = self.md.notes
        notes = [n for n in notes if n is not None and n.strip()]
        notes.reverse()
        for note in notes:
            self.catalog.notes.add_for_wemi(
                level="work",
                entity_id=self._work_id(title_row),
                data={"note": note},
            )

    def add_covers(self, book_row):
        """
        Read title cover data and add it to the database.
        :param book_row:
        :return:
        """
        covers_data = self.md.cover_data
        self.__ensure_title_covers(covers_data=covers_data, book_row=book_row, cache_first=True)

    def add_files(self, book_row):
        """
        Add the files from the metadata object to the given book row.
        :param book_row:
        :return:
        """
        file_data = self.md.files
        self.__ensure_title_files(file_data, book_row)

    def add_genres(self, title_row):
        """
        Load the genres.
        Genre and subject seems to be something that people often confuse - but it should have been sorted out in the
        metadata stage.
        :param title_row:
        :return:
        """
        # Todo: Add a standardized genres table -  in fact, ship with a genres table
        md_genres = self.md.genre
        md_genre_names = [g for g in md_genres.keys()]
        md_genre_names.reverse()
        self.__link_genres_to_title(title_row=title_row, genre_names=md_genre_names, genres=md_genres)
        # ------

    def add_language(self, title_row):
        """
        The language field describes the primary language of the work
        :return:
        """
        language = self.md.language
        if language:
            self.__ensure_primary_language(title_row=title_row, lang_str=language)

    # Todo: Switch over to using the methods in library apply
    def add_languages(self, title_row):
        """
        The languages field demotes the languages included in the work.
        :param title_row:
        :return:
        """
        languages = self.md.languages
        if languages:
            self.__ensure_languages(title_row, languages, lang_type="contained_in")

    def add_languages_available(self, title_row):
        """
        The languages_available field - the language options included in the work
        :param title_row:
        :return:
        """
        langs_available = self.md.languages_available
        if langs_available:
            langs_strs = langs_available.keys()
            self.__ensure_languages(title_row, langs_strs, "available_language")

    def add_ratings(self, title_row):
        """
        Process the ratings - these should come in the form of a dictionary keyed with the rating type and valued
        with the rating value.
        :param title_row:
        :return:
        """
        ratings = self.md.ratings
        links = []
        for rating_type in ratings:
            rating_id = self.catalog.ratings.match_or_create(
                MetadataCandidate({"value": ratings[rating_type]})
            )
            links.append(LinkValue(rating_id, link_type=rating_type))
        if links:
            self._work_link_writers["rating"].write(
                {self._work_id(title_row): tuple(links)}
            )

    def add_series(self, title_row):
        """
        Link the title to any given series in the order that they appear in the OrderedDict
        :param title_row:
        :return:
        """
        series = self.md.series
        series_index = self.md.series_index
        self.__link_series_to_title(
            title_row,
            list(series.keys()),
            series,
            series_index,
        )

    def add_synopsis(self, title_row):
        """
        Add the synopses to the title row.
        Synopsis will be added in the form of notes of synopsis type.
        :param title_row:
        :return:
        """
        synopses = self.md.synopses
        self.__ensure_note_rows(title_row=title_row, notes=synopses, note_type="synopsis")

    def add_tags(self, title_row):
        """
        Ensure appropriate tag rows and associate them with the title
        :param title_row:
        :return:
        """
        if self.app_id_match:
            tags_vals = [(t, v) for t, v in self.md.tags.items()]
            tag_ids = []
            for tag_str, tag_val in tags_vals:
                # Check to see if the tag already has an id on the database and add it if it doesn't
                tag_row = self.get_row_from_value(tag_val, table="tags")
                if tag_row is None:
                    tag_id = self.catalog.tags.match_or_create(
                        MetadataCandidate({"text": tag_str})
                    )
                else:
                    tag_id = int(tag_row["tag_id"])
                tag_ids.append(tag_id)

        else:
            tags = self.md.tags
            tag_ids = []
            for tag_string in tags.keys():
                tag_id = self.catalog.tags.match_or_create(
                    MetadataCandidate({"text": tag_string})
                )
                tag_ids.append(tag_id)
        if tag_ids:
            self._work_link_writers["tag"].write(
                {
                    self._work_id(title_row): tuple(
                        LinkValue(tag_id) for tag_id in tag_ids
                    )
                }
            )

    def add_publishers(self, title_row):
        """
        Ensure appropriate publisher rows and associate them with the title
        :param title_row:
        :return:
        """
        # Handle the publishers themselves
        publishers = OrderedDict()
        publishers.update(self.md.publisher)
        # Imprints should always be a lower priority than publishers.
        publishers.update(self.md.imprint)
        self.__ensure_title_publishers(publishers=publishers, title_row=title_row)

    def add_subjects(self, title_row):
        """
        Describe the subjects of a work.
        :param title_row:
        :return:
        """
        subjects = self.md.subject
        unique_subjects_ids, subject_rows = self.__ensure_subject_rows(subjects=subjects, standardize=True)
        if len(unique_subjects_ids) != len(subject_rows):
            # Log that something has gone wrong
            err_str = "Ensuring subjects with standardization has failed - attempting to ensure them blind"
            default_log.log_variables(
                err_str,
                "INFO",
                ("subjects", subjects),
                ("unique_subjects_ids", unique_subjects_ids),
                ("subject_rows", subject_rows),
            )

            # Try the fallback
            unique_subjects_ids, subject_rows = self.__ensure_subject_rows(subjects=subjects, standardize=False)
            assert len(unique_subjects_ids) == len(subject_rows), "Fallback has failed - cannot add series"
        # Do the actual linking
        self.__interlink_subject_rows(title_row=title_row, subject_rows=subject_rows)

    def get_row_from_value(self, row_id, table):
        """
        A number of variables in metadata are stored in the form of OrderedDicts - keyed with the name of the object and
        (optionally) valued with either the id or the row of that object in the database.
        This processes that value and returns the row correspond to it.
        If the class variable app_id_match is not set to True then this method will always return None.
        :param row_id:
        :param table:
        :return:
        """
        if not self.app_id_match:
            return None

        if isinstance(row_id, Row):
            return row_id
        elif isinstance(row_id, dict):
            return Row(database=self.db, row_dict=row_id)
        elif row_id is None:
            return None
        elif str(row_id).lower() == "none":
            return None
        else:
            try:
                int_id = int(row_id)
            except ValueError as e:
                err_str = "Cannot process creator dict - unrecognized value type"
                default_log.log_exception(err_str, e, "ERROR", ("row_id", row_id))
                raise
            return self.db.get_row_from_id(table=table, row_id=int_id)

    @staticmethod
    def _work_id(title_row):
        for key in ("work_id", "title_id"):
            try:
                value = title_row[key]
            except Exception:
                continue
            if value is not None:
                return int(value)
        raise DatabaseIntegrityError("Could not resolve Work ID from title row")

    def __add_one_creator_role(self, creators_dict, creator_role, creator_rows_dict, title_row):
        """
        Add one creator type to a title.
        It's assumed that, by this point, the metadata has been cleaned and all the creators present are going to be
        added to the database.
        :return:
        """
        role_ordered_dict = creators_dict[creator_role]

        unique_creator_ids, creator_rows = self.__generate_creator_list(
            title_row, creator_role, role_ordered_dict, creator_rows_dict
        )

        # At this point there should be a one to one mapping between the creators in the creators dict and a number of
        # creators rows - if this is not the case something hs gone wrong - probably two creators in the creators dict
        # have been mapped to the same entry in the creators table.
        # Fallback is to not use standardization when ensuring the creator rows on the database - which means that the
        # rows will be created with EXACTLY those names on the creators table.
        if len(unique_creator_ids) != len(creator_rows):

            default_log.info("ensure creators with standardization has failed - trying the fallback")
            unique_creator_ids, creator_rows = self.__generate_creator_list(
                title_row,
                creator_role,
                role_ordered_dict,
                creator_rows_dict,
                standardize=False,
            )

            assert len(unique_creator_ids) == len(
                creator_rows
            ), "The number of generated rows and the expected number of rows did not match - falling back has failed"

        self.__interlink_creators_rows_with_title(creator_rows, title_row, creator_role)

    def __generate_creator_list(
        self,
        title_row,
        creator_role,
        role_ordered_dict,
        creator_rows_dict,
        standardize=True,
    ):
        """
        Generate the creator_priority_list from a role_ordered_dict - this is a list of tuples - first element being the
        creator row and the second element being the priority with which that row should be linked to the title row.
        :param title_row: The title row that the creators will be eventually linked to
        :param creator_role: The role that the creator is playing in the creation of the work
        :param role_ordered_dict: An OrderedDict keyed with the name of the author and valued with the row id
                                  corresponding to that creator
        :param creator_rows_dict:
        :param standardize: Use standardize when ensure the new creator rows
        :return:
        """
        unique_creator_ids = set()
        creator_priority_rows = []

        creator_names = [k for k in role_ordered_dict.keys()]
        creator_names.reverse()

        for creator_name in creator_names:

            # Check to see if the row is already specified by the metadata object - if it is then use that one instead
            creator_row = self.get_row_from_value(row_id=role_ordered_dict[creator_name], table="creators")
            if creator_row is None:
                agent_id = self.catalog.agents.match_or_create_person(
                    MetadataCandidate(
                        {
                            "name": creator_name,
                            "type": "person",
                            "note": "creator_seminal_work={}".format(title_row["title"]),
                        }
                    )
                )
                creator_row = self.db.get_row_from_id("agents", agent_id)

            unique_creator_ids.add(self._agent_id(creator_row))
            creator_rows_dict[creator_role].append(creator_row)
            creator_priority_rows.append(creator_row)

        return unique_creator_ids, creator_priority_rows

    def __interlink_creators_rows_with_title(self, creator_priority_rows, title_row, creator_role):
        """
        Associated a number of creators in an iterable with a title_row
        :param creator_priority_rows: A list of tuples - first entry being the creator row and the second entry being
                                      the priority that should be used when associating them with the title row
        :param title_row:
        :type title_row: LiuXin Row object
        :param creator_role: The link will be created with this role
        :return:
        """
        for priority, creator_row in enumerate(creator_priority_rows):

            # Todo: Make it so that the interlink_rows method can also take priority = "max"
            try:
                self.catalog.agents.link_to_wemi(
                    agent_id=self._agent_id(creator_row),
                    level="work",
                    entity_id=self._work_id(title_row),
                    role=creator_to_marc(creator_role),
                    priority=priority,
                )
            except Exception as e:
                err_str = "Error while trying to associate the creator row with a title"
                err_str = default_log.log_exception(
                    err_str,
                    e,
                    "ERROR",
                    ("title_row", title_row),
                    ("creator_row", creator_row),
                    ("creator_priority_rows", creator_priority_rows),
                    ("creator_role", creator_role),
                    ("dir(e)", dir(e)),
                )
                raise NotImplementedError(err_str)

    @staticmethod
    def _agent_id(agent_row):
        for key in ("agent_id", "creator_id", "publisher_id"):
            try:
                value = agent_row[key]
            except Exception:
                continue
            if value is not None:
                return int(value)
        raise DatabaseIntegrityError("Could not resolve Agent ID from row")

    # Todo: Why do identifiers have priority again?
    def __apply_identifiers_set(self, title_row, id_type, id_set):
        """
        Apply all the identifiers in a given set to the given title.
        :param title_row: The title to add the identifiers to
        :param ids_set: The ids to try and apply to the title
        :return (bad_ids, good_ids): a tuple of sets - the bad ids are the ones that cannot be applied, for whatever
                                     reason - the good_ids are the ones that can be.
        """
        for id_val in id_set:
            try:
                identifier_id = self.catalog.identifiers.match_or_create(
                    IdentifierCandidate(id_type, id_val)
                )
            except Exception as e:
                err_str = "Unable to add identifier - something went wrong - ignoring"
                default_log.log_exception(err_str, e, "INFO", ("id_val", id_val))
                continue
            self.catalog.identifiers.link_to_wemi(
                identifier_id=identifier_id,
                level="work",
                entity_id=self._work_id(title_row),
                priority=0,
            )

    def __ensure_title_covers(self, covers_data, book_row, cache_first=True):
        """
        Link a given selection of covers to the given book row.
        Covers will be linked in the same order as the covers_data OrderedDict.
        :param covers_data: An OrderedDict keyed with the cover tuple and valued with the id of that cover on the
                            database - if any.
                            NOTE: Not currently supported. All covers will just be added to the database.
        :type covers_data: OrderedDict
        :param book_row:
        :param cache_first: If True then the first cover in the covers_data OrderedDict will be added to the covers
                            cache.
        :return cover_locs: A list of the locations of all the covers added to the system.
        """
        cover_tuples = covers_data.keys()
        cover_values = covers_data.values()
        if not cover_tuples:
            return []
        cover_tuples.reverse()
        cover_values.reverse()
        cover_tup_vals = zip(cover_tuples, cover_values)

        # Todo: The location that the covers are loaded into should really be specifiable in the add method
        # Add the covers to the folder store - need to make a folder for the book, and then load the covers into it
        book_folder_loc = self.fsm.ensure_book_folder(
            book_row,
            allowed_fs_ids="all",
            check=True,
            strict_db_position=False,
            stop_before_book=False,
        )

        cover_locs = []
        for cover_tuple, cover_value in cover_tup_vals:
            if self.app_id_match:
                # Check to see if the entry is a cover which is already present on the system - if it is then don't try
                # and load it and just link to the existing resource
                cover_row = self.get_row_from_value(row_id=cover_value, table="covers")
                if cover_row is None:
                    cover_loc = self.fsm.add.cover_tuple(
                        book_folder_loc,
                        cover_tuple,
                        cover_name=None,
                        cover_original_path=None,
                        cover_local=False,
                        cover_linked_row=book_row,
                        override_cover_row=None,
                    )
                else:
                    self.db.interlink_rows(primary_row=book_row, secondary_row=cover_row)
                    cover_loc = self.fsm.get_loc(asset_row=cover_row)

            else:
                cover_loc = self.fsm.add.cover_tuple(
                    book_folder_loc,
                    cover_tuple,
                    cover_name=None,
                    cover_original_path=None,
                    cover_local=False,
                    cover_linked_row=book_row,
                    override_cover_row=None,
                )
            cover_locs.append(cover_loc)
        cover_locs.reverse()

        if cache_first:
            main_cover_loc = cover_locs[0]

            self.fsm.cover_cache.download_from_fs(main_cover_loc["cover_row"]["cover_id"])

        return cover_locs

    # Todo: Actually implement hash checking. That would be nice.
    # Todo: Actually implement fingerprinting. That too would be nice.
    def __ensure_title_files(self, files, book_row):
        """
        Link the given files to the book row.
        Files are stored in an OrderedDict - in the order that they should appear on the database.
        :param files: An OrderedDict keyed with a file tuple and valued with the id of that file on the database - if
                      any.
                      NOTE - Not currently supported - all files will just be added to the database.
        :type files: OrderedDict
        :return file_locs: An iterable of the locations of the files - in the order that they should appear.
        """
        file_tuples = files.keys()
        if not file_tuples:
            return []
        file_tuples.reverse()

        # Add the files as formats in a book
        format_locs = []
        for fmt_tuple in file_tuples:
            file_loc = self.fsm.add.format_tuple(book_id=book_row["book_id"], fmt_tuple=fmt_tuple)

            format_locs.append(file_loc)

    def __link_genres_to_title(self, title_row, genre_names, genres):
        """
        Link a list of series names to a title row.
        :param title_row: The title to link all the series to
        :param genre_names: The names of the series to link to - in the order that they should be linked to. Should be
                             a subset of the series keys, ordered the way that you want them to appear linked to the
                             title.
        :param genres: Keyed with the names of the series and valued with either a pointer to the row representing the
                       series (in the form of an id or a row). If None then tries to ensure the series and linked to the
                       new series.
        :type genres: OrderedDict
        :param series_index: Keyed with the name of the series (as it appears in :param series:) and valued with the
                             index that the series should have.
        :type series_index: OrderedDict
        :return:
        """
        try:
            genre_rows = self.__ensure_genres_rows(genre_names, genres)
        except AssertionError:
            # Standardization has caused two series names to be mapped to the same series - this won't do, so ensuring
            # the series rows without an attempt to standardize the series name before searching for it
            genre_rows = self.__ensure_genres_rows(genre_names, genres, standardize=False)

        if genre_rows:
            self._work_link_writers["genre"].write(
                {
                    self._work_id(title_row): tuple(
                        LinkValue(int(genre_row["genre_id"]))
                        for genre_row in genre_rows
                    )
                }
            )

    def __ensure_genres_rows(self, genre_names, genres, standardize=True):
        """
        Ensure that a given set of genre rows exists - also ensure that there is a 1-1 correspondence between the
        given genre names and the genre rows.
        :param genre_names:
        :param genres:
        :return:
        """
        genre_rows = []
        genre_ids = set()

        for genre_name in genre_names:

            # Check to see if the genre is already specified and generate it if it isn't
            genre_row = self.get_row_from_value(genres[genre_name], "genres")
            if genre_row is None:
                genre_id = self.catalog.genres.match_or_create(
                    MetadataCandidate({"name": genre_name})
                )
                genre_row = self.db.get_row_from_id("genres", genre_id)

            genre_rows.append(genre_row)
            genre_ids.add(six_unicode(genre_row["genre_id"]))

        # Check that there is a one to one match between the series rows and and the series names
        assert len(genre_ids) == len(genre_rows), "Cannot ensure genres - wrong number of series was ensured"

        return genre_rows

    # Todo: Doesn't need to be a separate method
    def __ensure_primary_language(self, title_row, lang_str):
        """
        Ensure that the primary language of a work is set.
        :param title_row:
        :param lang_str:
        :return:
        """
        # Retrieve the language row and link it to the title row
        language_match = self.catalog.languages.exact(lang_str)
        if not language_match.is_match or language_match.entity_id is None:
            raise DatabaseIntegrityError("Language could not be resolved: {!r}".format(lang_str))
        self._work_link_writers["language"].write(
            {self._work_id(title_row): LinkValue(language_match.entity_id)},
            link_type="primary",
        )

    def __ensure_languages(self, title_row, lang_strs, lang_type="contained_in"):
        """
        Note that the following languages are contained in the work - looked up from their language strings.
        :param title_row:
        :param lang_strs:
        :return:
        """
        lang_strs = list(lang_strs)
        lang_strs.reverse()
        language_ids = []
        for lang_str in lang_strs:
            language_match = self.catalog.languages.exact(lang_str)
            if not language_match.is_match or language_match.entity_id is None:
                raise DatabaseIntegrityError("Language could not be resolved: {!r}".format(lang_str))
            language_ids.append(language_match.entity_id)
        self._work_link_writers["language"].write(
            {
                self._work_id(title_row): tuple(
                    LinkValue(language_id) for language_id in language_ids
                )
            },
            link_type=lang_type,
        )

    def __link_series_to_title(self, title_row, series_names, series, series_index):
        """
        Link a list of series names to a title row.
        :param title_row: The title to link all the series to
        :param series_names: The names of the series to link to - in the order that they should be linked to. Should be
                             a subset of the series keys, ordered the way that you want them to appear linked to the
                             title.
        :param series: Keyed with the names of the series and valued with either a pointer to the row representing the
                       series (in the form of an id or a row). If None then tries to ensure the series and linked to the
                       new series.
        :type series: OrderedDict
        :param series_index: Keyed with the name of the series (as it appears in :param series:) and valued with the
                             index that the series should have.
        :type series_index: OrderedDict
        :return:
        """
        series_names = list(series_names)
        series_names.reverse()
        try:
            series_rows = self.__ensure_series_rows(title_row, series_names, series)
        except AssertionError:
            # Standardization has caused two series names to be mapped to the same series - this won't do, so ensuring
            # the series rows without an attempt to standardize the series name before searching for it
            series_rows = self.__ensure_series_rows(title_row, series_names, series, standardize=False)

        series = zip(series_names, series_rows)
        writer = self._work_link_writers["series"]
        index_column = next(
            (
                column.name
                for column in writer.link_spec.extra_link_columns
                if column.name.endswith("_index")
            ),
            None,
        )
        links = []

        for series_name, series_row in series:
            # Determine the series index - if present
            link_series_index = None
            if series_name in series_index:
                link_series_index = series_index[series_name]

            extra = (
                {index_column: link_series_index}
                if index_column is not None and link_series_index is not None
                else {}
            )
            links.append(
                LinkValue(int(series_row["series_id"]), extra=extra)
            )
        if links:
            writer.write({self._work_id(title_row): tuple(links)})

    def __ensure_series_rows(self, title_row, series_names, series, standardize=True):
        """
        Ensure that a given set of series rows exists - also ensure that there is a 1-1 correspondence between the
        given series names and the series rows.
        :param title_row:
        :param series_names:
        :param series:
        :param series_index:
        :return:
        """
        series_rows = []
        series_ids = set()

        for series_name in series_names:

            # If the series is provided with an id, then use that, otherwise just use a series which matches on name
            series_row = self.get_row_from_value(row_id=series[series_name], table="series")
            if series_row is None:
                series_id = self.catalog.series.match_or_create(
                    MetadataCandidate({"name": series_name})
                )
                series_row = self.db.get_row_from_id("series", series_id)

            series_rows.append(series_row)
            series_ids.add(six_unicode(series_row["series_id"]))

        # Check that there is a one to one match between the series rows and and the series names
        assert len(series_ids) == len(series_rows), "Cannot ensure series - wrong number of series was ensured"

        return series_rows

        # Todo: Finally remove the synopses and comments tables - then add them back in consistently

    def __ensure_note_rows(self, title_row, notes, note_type="note"):
        """
        Ensure the synopsis rows - linked to the notes table under type synopsis.
        :param title_row:
        :param notes: Keyed with the note value, and valued with either the row, row_id, or None corresponding to the
                      row of that note on the database.
        :type notes: OrderedDict
        :param note_type:
        :return:
        """
        note_strings = list(notes.keys())
        note_strings.reverse()

        for note in note_strings:

            if note_type == "synopsis":
                self.catalog.synopses.add_for_wemi(
                    level="work",
                    entity_id=self._work_id(title_row),
                    data={"synopsis": note},
                )
            else:
                self.catalog.notes.add_for_wemi(
                    level="work",
                    entity_id=self._work_id(title_row),
                    data={"note": note},
                )

    def __ensure_title_publishers(self, publishers, title_row):
        """
        Ensure the publishers linked to the given title row.
        :param publishers: The publishers ordered dict from the metadata object
        :param title_row: The title row to link all the publishers to
        :param publisher_type: The type of publisher (options are "imprint" or "publisher")
        :return:
        """
        for priority, pub_name in enumerate(publishers):

            pub_row = self.get_row_from_value(publishers[pub_name], "publishers")
            # Is the creator already on the system, or does it have to be created?
            if pub_row is None:
                publisher_id = self.catalog.agents.match_or_create_organisation(
                    MetadataCandidate({"name": pub_name, "type": "organisation"})
                )
                pub_row = self.db.get_row_from_id("agents", publisher_id)

            self.catalog.agents.link_to_wemi(
                agent_id=self._agent_id(pub_row),
                level="work",
                entity_id=self._work_id(title_row),
                role="pbl",
                priority=priority,
            )

    def __ensure_subject_rows(self, subjects, standardize=True):
        """
        Takes a subject OrderedDict object and tries to ensure every entry in it has a corresponding series.
        :param subjects:
        :return:
        """
        unique_subject_ids = set()
        subject_rows = []

        subject_names = [k for k in subjects.keys()]
        subject_names.reverse()

        for subject_name in subject_names:

            subject_row = self.get_row_from_value(subjects[subject_name], "subject")
            # If the subject is not already in the system then it has to be created
            if subject_row is None:
                subject_id = self.catalog.subjects.match_or_create(
                    MetadataCandidate({"name": subject_name})
                )
                subject_row = self.db.get_row_from_id("subjects", subject_id)

            unique_subject_ids.add(subject_row["subject_id"])
            subject_rows.append(subject_row)

        return unique_subject_ids, subject_rows

    def __interlink_subject_rows(self, title_row, subject_rows):
        """
        Interlink all the given rows to the title row.
        Rows are assumed to be in the order that they should be interlinked in (thus the last row in the list will end
        up with the highest priority, because it will be linked last).
        :param title_row: The title row to link all the series to
        :param subject_rows: The rows to link to the title row.
        :return:
        """
        if subject_rows:
            self._work_link_writers["subject"].write(
                {
                    self._work_id(title_row): tuple(
                        LinkValue(int(subject_row["subject_id"]))
                        for subject_row in subject_rows
                    )
                }
            )
