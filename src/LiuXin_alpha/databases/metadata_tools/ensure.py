import queue as Queue
from copy import deepcopy

from LiuXin.databases.metadata_tools.add import Add
from LiuXin.databases.row import Row

from LiuXin.exceptions import DatabaseIntegrityError
from LiuXin.exceptions import InputIntegrityError

from LiuXin.library.standardization import make_creator_phash
from LiuXin.library.standardization import make_series_phash
from LiuXin.library.standardization import make_tag_search_term
from LiuXin.library.standardization import standardize_creator_name
from LiuXin.library.standardization import standardize_genre
from LiuXin.library.standardization import standardize_language
from LiuXin.library.standardization import standardize_publisher
from LiuXin.library.standardization import standardize_series

from LiuXin.metadata import check_doi
from LiuXin.metadata import check_isbn
from LiuXin.metadata import check_issn
from LiuXin.metadata.constants import EXTERNAL_EBOOK_ID_SCHEMA
from LiuXin.metadata.constants import INTERNAL_EBOOK_ID_SCHEMA
from LiuXin.metadata.metadata_standardize import standardize_id_name

from LiuXin.utils.logger import default_log

from LiuXin.utils.lx_libraries.liuxin_six import six_unicode


class Ensure(object):
    """
    Class for the methods which ensure that a resource exists on the database, creating it as needed.
    """

    def __init__(self, database):
        self.db = database
        self.add = Add(self.db)

    # Todo: Add author collision checking with user verification
    def creator(self, creator_name, match_queue):
        """
        Takes a creator name - returns the row corresponding to that creator - making it if required.
        Currently puts all matching creators in the match_queue for returning.
        :param creator_name: The name of a creator
        :param match_queue: A place to put the matches - the creators it might be
        :return:
        """
        creator_name = deepcopy(creator_name)
        found_creators = set()

        # If the name is a string separated by & assume the method has been passed multiple creators by mistake
        if "&" in creator_name:
            err_str = "ensure_creator was passed a string which appeared to be composed of multiple names"
            err_str = default_log.log_variables(err_str, "ERROR", ("creator_name", creator_name))
            raise NotImplementedError(err_str)

        # Working through the various types of creator row, looking for a good match
        creator_name = standardize_creator_name(creator_name)
        candidate_rows = self.db.search(table="creators", column="creator", search_term=creator_name)
        if len(candidate_rows) > 0:
            for row in candidate_rows:
                if row.row_id not in found_creators:
                    match_queue.put(row)
                    found_creators.add(row.row_id)

        # If that fails, try the creator phash - which is not preferred, as it may introduce degeneration and is less
        # efficient
        creator_phash = make_creator_phash(creator_name)
        candidate_rows = self.db.search(table="creators", column="creator_phash", search_term=creator_phash)
        if len(candidate_rows) > 0:
            for row in candidate_rows:
                if row.row_id not in found_creators:
                    match_queue.put(row)

        # If this point is reached then the creator will have to be created - use the creation method
        if len(found_creators) == 0:
            creator_row = self.add.creator(creator=creator_name)
            match_queue.put(creator_row)

    # Todo: Should be using the creator function instead of this one
    def creator_blind(self, creator_name, seminal_work=None, standardize=True):
        """
        Ensure a creator row without having to
        :param creator_name:
        :param seminal_work: If provided, and the creator has to be created, will set the seminal work to be this
        :return:
        """
        # Working through the various types of creator row, looking for a good match
        if standardize:
            creator_name = standardize_creator_name(creator_name)
        candidate_rows = self.db.search(table="creators", column="creator", search_term=creator_name)
        if len(candidate_rows) > 0:
            return candidate_rows[0]

        # If that fails, try the creator phash - which is not preferred, as it may introduce degeneration and is less
        # efficient
        creator_phash = make_creator_phash(creator_name)
        candidate_rows = self.db.search(table="creators", column="creator_phash", search_term=creator_phash)
        if len(candidate_rows) > 0:
            return candidate_rows[0]

        return self.add.creator(creator=creator_name, creator_seminal_work=seminal_work)

    def genre(self, genre_string, standardize=True):
        """
        Ensure that the given genre exists - genres must be unique - so always returns a single row.
        :param genre_string: Try and ensure a genre with that name
        :param standardize: If True, then try and standardize the name before searching for it in the genres table.
        :return:
        """
        if genre_string is None:
            err_str = "Library.ensure_genre called with None"
            default_log.error(err_str)
            raise InputIntegrityError(err_str)
        genre_string = deepcopy(six_unicode(genre_string))
        if standardize:
            genre = standardize_genre(genre_string)
        else:
            genre = genre_string

        candidate_rows = self.db.search(table="genres", column="genre", search_term=genre)
        if len(candidate_rows) > 1:
            err_str = (
                "searching the database for a specific genre returned multiple rows - "
                "trying to use the maintenance bot to fix the problem"
            )
            default_log.log_variables(
                err_str,
                "ERROR",
                ("genre_string", genre_string),
                ("genre", genre),
                ("candidate_rows", candidate_rows),
            )
            # Try and fix the problem using the maintenance bot
            from LiuXin.databases.maintenance_bot import fix_duplicates

            fix_duplicates(self.db, table="genres", column="genre", comparison=standardize_genre)
            candidate_rows = self.db.search(table="genres", column="genre", search_term=genre)
            if len(candidate_rows) > 1:
                raise DatabaseIntegrityError("Maintenance bot didn't fix the problem")
            elif len(candidate_rows) == 1:
                return candidate_rows[0]
        elif len(candidate_rows) == 1:
            return candidate_rows[0]

        # If this point is reached, then the genre row has to be created
        genre_row = Row(database=self.db)
        genre_row["genre"] = genre
        genre_row.sync()
        return genre_row

    def identifier(self, identifier, identifier_type, error=True):
        """
        Create an entry in the identifiers table.
        :param identifier:
        :param identifier_type:
        :return:
        """
        old_id_type = deepcopy(identifier_type)
        identifier_type = standardize_id_name(identifier_type)

        if identifier_type is None:
            err_str = "Cannot add identifier - identifier type not recognized"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("old_id_type", old_id_type),
                ("EXTERNAL_EBOOK_ID_SCHEMA", EXTERNAL_EBOOK_ID_SCHEMA),
                ("INTERNAL_EBOOK_ID_SCHEMA", INTERNAL_EBOOK_ID_SCHEMA),
            )
            raise InputIntegrityError(err_str)

        # Checks to see if the identifier is valid - for the types that have check functions
        if identifier_type in ["isbn10", "isbn13", "isbn"]:
            old_id = deepcopy(identifier)
            identifier = check_isbn(identifier)
            if identifier is None:
                err_str = "Passed isbn not valid"
                err_str = default_log.log_variables(
                    err_str,
                    "ERROR",
                    ("old_id", old_id),
                    ("identifier_type", identifier_type),
                )
                raise InputIntegrityError(err_str)

        if identifier_type == "issn":
            identifier = check_issn(identifier)
            if identifier is None:
                err_str = "Passed issn string was not valid."
                err_str = default_log.log_variables(
                    err_str,
                    "ERROR",
                    ("identifier", identifier),
                    ("identifier_type", identifier_type),
                )
                raise InputIntegrityError(err_str)

        if identifier_type == "doi":
            identifier = check_doi(identifier)
            if identifier is None:
                err_str = "Passed doi string was not valid."
                err_str = default_log.log_variables(
                    err_str,
                    "ERROR",
                    ("identifier", identifier),
                    ("identifier_type", identifier_type),
                )
                raise InputIntegrityError(err_str)

        id_row = Row(database=self.db)
        id_row["identifier"] = identifier
        id_row["identifier_type"] = identifier_type

        if error:
            id_row.sync()
        else:
            try:
                id_row.sync()
            except DatabaseIntegrityError as e:
                # Probably a violation of the unique constraint - search for the identifier row which caused it
                id_rows = self.db.search(table="identifiers", column="identifier", search_term=identifier)
                if len(id_rows) > 1:
                    err_str = "Recovery search for identifier row produced multiple results"
                    err_str = default_log.log_variables(
                        err_str,
                        "ERROR",
                        ("id_rows", id_rows),
                        ("identifier", identifier),
                        ("identifier_type", identifier_type),
                        ("id_row", id_row),
                    )
                    raise NotImplementedError(err_str)
                elif len(id_rows) == 1:
                    return id_rows[0]
                else:
                    err_str = (
                        "Recovery search for a row yielded no results - not sure why the error was thrown - "
                        "logging original error"
                    )
                    err_str = default_log.log_exception(
                        err_str,
                        e,
                        "ERROR",
                        ("id_rows", id_rows),
                        ("id_row", id_row),
                        ("identifier", identifier),
                        ("identifier_type", identifier_type),
                    )
                    raise DatabaseIntegrityError(err_str)

        return id_row

    # Todo: Re-write and implement - cba right now
    def language(self, language_string, lang_code=False):
        """
        Ensures that a given language is in the language database.
        :param language_string:
        :param lang_code: If True, then assumes that the given language_string is actually a language code string.
                          Language will be created with that code and returned if it doesn't exist.
                          If False, searches on the name of the language - the "language" column in the table
                          If "either" will search both code and name for a match and return one if found
        :return:
        """
        if language_string is None:
            err_str = "Library.ensure_language called with None"
            default_log.error(err_str)
            raise InputIntegrityError(err_str)

        language_string = deepcopy(six_unicode(language_string))
        # Todo: Needs to be a similar method for language code - not implemented at the moment
        # Todo: Needs to be searching off a custom search column - not the actual search terms
        stand_language = standardize_language(language_string)

        # Todo: Both the language and language_code column should be unique
        # Try with the standardized language
        if lang_code is True:

            # Try with the language code
            candidate_rows = self.db.search(table="languages", column="language_code", search_term=stand_language)
            if len(candidate_rows) > 1:
                err_str = "searching the database for a specific, standardized language returned multiple rows"
                err_str = default_log.log_variables(
                    err_str,
                    "ERROR",
                    ("language_string", language_string),
                    ("language", stand_language),
                    ("candidate_rows", candidate_rows),
                )
                raise DatabaseIntegrityError(err_str)
            elif len(candidate_rows) == 1:
                return candidate_rows[0]

            # Search has failed with the standardized language - try with the regular language string instead

            candidate_rows = self.db.search(table="languages", column="language_code", search_term=language_string)
            if len(candidate_rows) > 1:
                err_str = "searching the database for a specific, unstandardized language returned multiple rows"
                err_str = default_log.log_variables(
                    err_str,
                    "ERROR",
                    ("language_string", language_string),
                    ("language", stand_language),
                    ("candidate_rows", candidate_rows),
                )
                raise DatabaseIntegrityError(err_str)
            elif len(candidate_rows) == 1:
                return candidate_rows[0]

        elif lang_code is False:

            candidate_rows = self.db.search(table="languages", column="language", search_term=stand_language)
            if len(candidate_rows) > 1:
                err_str = "searching the database for a specific, standardized language returned multiple rows"
                err_str = default_log.log_variables(
                    err_str,
                    "ERROR",
                    ("language_string", language_string),
                    ("language", stand_language),
                    ("candidate_rows", candidate_rows),
                )
                raise DatabaseIntegrityError(err_str)
            elif len(candidate_rows) == 1:
                return candidate_rows[0]

            # Try the unstandardized language string
            candidate_rows = self.db.search(table="languages", column="language", search_term=language_string)
            if len(candidate_rows) > 1:
                err_str = "searching the database for a specific, unstandardized language returned multiple rows"
                err_str = default_log.log_variables(
                    err_str,
                    "ERROR",
                    ("language_string", language_string),
                    ("language", stand_language),
                    ("candidate_rows", candidate_rows),
                )
                raise DatabaseIntegrityError(err_str)
            elif len(candidate_rows) == 1:
                return candidate_rows[0]

        elif lang_code == "either":

            candidate_rows = self.db.search(table="languages", column="language", search_term=stand_language)
            if len(candidate_rows) > 1:
                err_str = "searching the database for a specific, standardized language returned multiple rows"
                err_str = default_log.log_variables(
                    err_str,
                    "ERROR",
                    ("language_string", language_string),
                    ("language", stand_language),
                    ("candidate_rows", candidate_rows),
                )
                raise DatabaseIntegrityError(err_str)
            elif len(candidate_rows) == 1:
                return candidate_rows[0]

            # Try the unstandardized language string
            candidate_rows = self.db.search(table="languages", column="language", search_term=language_string)
            if len(candidate_rows) > 1:
                err_str = "searching the database for a specific, unstandardized language returned multiple rows"
                err_str = default_log.log_variables(
                    err_str,
                    "ERROR",
                    ("language_string", language_string),
                    ("language", stand_language),
                    ("candidate_rows", candidate_rows),
                )
                raise DatabaseIntegrityError(err_str)
            elif len(candidate_rows) == 1:
                return candidate_rows[0]

            # Try with the language code
            candidate_rows = self.db.search(table="languages", column="language_code", search_term=stand_language)
            if len(candidate_rows) > 1:
                err_str = "searching the database for a specific, standardized language returned multiple rows"
                err_str = default_log.log_variables(
                    err_str,
                    "ERROR",
                    ("language_string", language_string),
                    ("language", stand_language),
                    ("candidate_rows", candidate_rows),
                )
                raise DatabaseIntegrityError(err_str)
            elif len(candidate_rows) == 1:
                return candidate_rows[0]

            # Search has failed with the standardized language - try with the regular language string instead

            candidate_rows = self.db.search(table="languages", column="language_code", search_term=language_string)
            if len(candidate_rows) > 1:
                err_str = "searching the database for a specific, unstandardized language returned multiple rows"
                err_str = default_log.log_variables(
                    err_str,
                    "ERROR",
                    ("language_string", language_string),
                    ("language", stand_language),
                    ("candidate_rows", candidate_rows),
                )
                raise DatabaseIntegrityError(err_str)
            elif len(candidate_rows) == 1:
                return candidate_rows[0]

        else:

            raise NotImplementedError("Couldn't parse lang_code")

        # If this point is reached, then the language row has to be created
        # Almost certainly something has gone wrong
        # Todo: Ship with a complete languages table - always either match to it or come up with a clever solution
        # Todo: This is a terrible hack for the language code -
        language_row = Row(database=self.db)
        if not lang_code:
            language_row["language"] = stand_language
            language_row["language_code"] = stand_language
        else:
            language_row["language"] = stand_language
            language_row["language_code"] = stand_language
        language_row.sync()
        return language_row

    def publisher(self, publisher, standardize=True):
        """
        Ensures that a given publisher is in the publishers table of the database.
        :param publisher:
        :return:
        """
        if publisher is None:
            err_str = "Library.ensure_publisher called with None"
            default_log.error(err_str)
            raise InputIntegrityError(err_str)
        if standardize:
            publisher = standardize_publisher(publisher)

        candidate_rows = self.db.search(table="publishers", column="publisher", search_term=publisher)
        if len(candidate_rows) > 1:
            err_str = "searching the database for a specific publisher returned multiple rows"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("publisher_string", publisher),
                ("publisher", publisher),
                ("candidate_rows", candidate_rows),
            )
            raise DatabaseIntegrityError(err_str)
        elif len(candidate_rows) == 1:
            return candidate_rows[0]

        # If this point is reached, then the genre row has to be created
        pub_row = self.add.publisher(publisher=publisher)
        return pub_row

    def rating(self, rating):
        """
        Ensure a rating. Ratings are on a scale of 0-10 (integers). These will be displayed down to 0-5 - which gives
        an available resolution of half a star.
        :param rating: On a scale of 0-10 - if the rating is between 0-5 double it and pass the doubled value into this
                       method - it will return the row appropriate to that rating
        :return:
        """
        rating = int(rating)
        rating_id = rating + 1
        return self.db.get_row_from_id("ratings", rating_id)

    # Todo: Rethink how series and creators are managed
    # Todo: Upgrade and rethink - there has to be a better way of fuzzily seeking series
    # Weak fuzzy matching of series - generally the author of a series will appear in every book of the series - usually
    # there are exceptions (might want to write a multi-author series exception). But, generally, anything in the
    # Honorverse will have David Weber on the cover somewhere (for example)
    # More problematic is the example of Star Wars - which will probably not have George Lucas on the cover
    def series(
        self,
        creator_rows,
        series_name,
        series_queue=None,
        confidence=False,
        stand=True,
        use_phash=True,
    ):
        """
        Takes a set of creators and a series name - checks for a matching series linked to any subset of those creators.
        If none exists, then it has to be created.
        :param creator_rows:
        :param series_name:
        :param series_queue: A queue object which potentially matching series are placed on.
        :param confidence: How confident is the process about the given data? If the creators rows are auto-generated
                           (i.e. from metadata) then the process isn't very confident about the series to be generated
                           and it probably shouldn't be linked to a bunch of other stuff, which might cause additional
                           confusion later.
        :param stand: Should the series string be standardized before writing
        :param use_phash: Use series phash when searching for a series
        :return:
        """
        if creator_rows is None:
            creator_rows = []

        creator_names = [r["creator"] for r in creator_rows]
        series_name = deepcopy(six_unicode(series_name))
        if stand:
            series = standardize_series(series_name)
        else:
            series = series_name
        found_series = []

        # Try for an exact match of the series string - if we do then that's the one we prefer
        candidate_rows = self.db.search(table="series", column="series", search_term=series_name)
        if len(candidate_rows) > 0 and series_queue is None:
            return candidate_rows[0]
        for row in candidate_rows:
            series_queue.put(row)
            found_series.append(row)

        if use_phash:
            # Starts by trying to generate phashes for the creator-series combos. Runs through until it finds a match
            # returns the first sensible match
            creator_series_phashs = [make_series_phash(creator_string, series) for creator_string in creator_names]
            for phash in creator_series_phashs:
                candidate_rows = self.db.search(table="series", column="series_phash", search_term=phash)
                if len(candidate_rows) > 0:
                    if series_queue is None:
                        return candidate_rows[0]
                    for row in candidate_rows:
                        series_queue.put(row)
                        found_series.append(row)

            # Now search for the series phash and add any that match that the series name
            series_phash = make_series_phash("", series)
            series_name_matches = self.db.search(table="series", column="series_phash", search_term=series_phash)
            if len(series_name_matches) > 0 and series_queue is None:
                return series_name_matches[0]
            for row in series_name_matches:
                series_queue.put(row)
                found_series.append(row)

        if len(found_series) > 0:
            if series_queue is None:
                return found_series[0]
        else:
            # If this point had been reached, then the series will have to be created -
            if creator_rows:
                if not confidence:
                    series_row = self.add.series(series=series)
                else:
                    series_row = self.add.series(series=series, series_creator=creator_rows[0])
            else:
                series_row = self.add.series(series=series)
            if series_queue is None:
                return series_row
            series_queue.put(series_row)

    def series_blind(self, creator_rows, series_name, stand=True, use_phash=True):
        """
        Doesn't dump the results to a queue - just
        :param creator_rows:
        :param series_name:
        :param stand: Attempt to use
        :return:
        """
        series_queue = Queue.Queue(0)
        try:
            self.series(
                creator_rows=creator_rows,
                series_name=series_name,
                series_queue=series_queue,
                stand=stand,
                use_phash=use_phash,
            )
        except DatabaseIntegrityError:
            # Row already exists - retrieve it and return
            return self.db.search(table="series", column="series", search_term=series_name)[0]

        return series_queue.get_nowait()

    def subject(self, subject, standardize=True):
        """
        Ensures that a subject exists - returns the row for it.
        :param subject:
        :return:
        """
        subject = six_unicode(subject)
        subject_rows = self.db.search(table="subjects", column="subject", search_term=subject)
        if len(subject_rows) > 0:
            return subject_rows[0]
        return self.add.subject(subject=subject, subject_parent=None)

    def tag(self, tag_text):
        """
        Ensure that the the tag exists in the database - return the corresponding row, creating it if required.
        :param tag_text:
        :return:
        """
        # Makes a tag search term - searches the database for that tag search term - if it exists returns the
        # appropriate row - if not, makes the row and then returns it.
        try:
            tag_phash = make_tag_search_term(tag_text)
        except TypeError as e:
            err_str = "Type error while trying to ensure tag - tag was probably not a string"
            err_str = default_log.log_exception(
                err_str,
                e,
                "ERROR",
                ("tag_text", tag_text),
                ("type(tag)", type(tag_text)),
            )
            raise InputIntegrityError(err_str)

        # Todo: Need a database with tag_phash, creator_phash e.t.c.
        # Try matching on the phash
        tag_phash_rows = self.db.search(table="tags", column="tag_phash", search_term=tag_phash)
        if len(tag_phash_rows) == 1:
            return tag_phash_rows[0]
        elif len(tag_phash_rows) > 1:
            err_str = "tag_phash matched to multiple tags - which shouldn't happen. Check the database"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("tag_text", tag_text),
                ("tag_phash", tag_phash),
                ("tag_phash_rows", tag_phash_rows),
            )
            raise DatabaseIntegrityError(err_str)

        # Try matching on the actual tag text - just in case
        tag_rows = self.db.search(table="tags", column="tag", search_term=tag_text)
        if len(tag_rows) == 1:
            return tag_rows[0]
        elif len(tag_rows) > 1:
            err_str = "tag text matched to multiple tags - which shouldn't happen. Check the database"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("tag_text", tag_text),
                ("tag_phash", tag_phash),
                ("tag_rows", tag_rows),
            )
            raise DatabaseIntegrityError(err_str)

        # If this point has been reached the row cannot be matched - create a new one
        tag_row = Row(database=self.db)
        tag_row["tag"] = tag_text
        tag_row["tag_phash"] = tag_phash
        tag_row.sync()
        return tag_row
