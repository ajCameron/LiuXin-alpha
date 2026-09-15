
"""
Resolve legacy metadata values with entity-specific matching and creation policies.
"""

from __future__ import annotations

import queue as Queue

from typing import Any, Dict, List, Optional, Tuple, Union, TYPE_CHECKING

from copy import deepcopy

from LiuXin_alpha.catalog.metadata_tools.add import Add
from LiuXin_alpha.databases.row import Row

from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError

from LiuXin_alpha.metadata.standardization import make_creator_phash
from LiuXin_alpha.metadata.standardization import make_series_phash
from LiuXin_alpha.metadata.standardization import make_tag_search_term
from LiuXin_alpha.metadata.standardization import standardize_creator_name
from LiuXin_alpha.metadata.standardization import standardize_genre
from LiuXin_alpha.metadata.standardization import standardize_language
from LiuXin_alpha.metadata.standardization import standardize_publisher
from LiuXin_alpha.metadata.standardization import standardize_series
from LiuXin_alpha.metadata.standardize import standardize_id_name


from LiuXin_alpha.metadata.utils import check_doi, check_isbn, check_issn
from LiuXin_alpha.metadata.constants import EXTERNAL_EBOOK_ID_SCHEMA, INTERNAL_EBOOK_ID_SCHEMA

from LiuXin_alpha.utils.logging import default_log

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.database_api import DatabaseAPI
    from LiuXin_alpha.databases.api.row_api import RowAPI


class Ensure:
    """
    Group compatibility lookup/create helpers with a borrowed database.

    Construction creates an Add helper, which Catalog composition later replaces
    with its shared, peer-wired Add. Methods differ in duplicate handling,
    normalization and queue behavior; these operations have no common transaction
    or uniform guarantee of reuse without insertion.

    Example:
        Use creator_blind for a first-match Row, or creator with a queue to inspect
        its legacy candidate sequence and possible creation.
    """

    def __init__(self, database: "DatabaseAPI") -> None:
        """
        Retain the database and construct a local Add helper.

        Example:
            Catalog composition replaces this local Add with its shared Add instance.


        :param database: Borrowed database handle; no query or capability validation.
        :return: None; sets db and a fresh Add whose peer attributes remain unset.
        """
        self.db = database
        self.add = Add(self.db)

    # Todo: Add author collision checking with user verification
    def creator(self, creator_name: str, match_queue: Queue.Queue["RowAPI"]):
        """
        Queue exact and phonetic Creator candidates, creating if no exact IDs were recorded.

        Exact candidates are deduplicated by row_id. Phonetic candidates are checked
        against exact IDs but never added to that set, so duplicates can recur.
        Creation is based on the exact-ID set, not whether the queue has matches.

        Example:
            Phonetic-only matches can be queued and still followed by a newly created Creator.


        :param creator_name: Single name, standardized after rejecting any ampersand.
        :param match_queue: Queue receiving Rows; put may block according to queue capacity.
        :return: None; results are delivered through the queue.
        :raises InputIntegrityError: The input contains an ampersand.
        """
        creator_name = deepcopy(creator_name)
        found_creators = set()

        # If the name is a string separated by & assume the method has been passed multiple creators by mistake
        if "&" in creator_name:
            err_str = "ensure_creator was passed a string which appeared to be composed of multiple names"
            err_str = default_log.log_variables(err_str, "ERROR", ("creator_name", creator_name))
            raise InputIntegrityError(err_str)

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
    def creator_blind(
            self,
            creator_name: str,
            seminal_work: Optional[str] = None,
            standardize: bool = True) -> "RowAPI":
        """
        Select the first exact or phonetic Creator match, otherwise create one.

        No ambiguity resolution or transaction protects lookup/create.

        Example:
            An existing phonetic match wins without comparing the seminal-work hint.


        :param creator_name: Creator name to search.
        :param seminal_work: Seminal-work hint used only when creating.
        :param standardize: Standardize the name before both lookup forms.
        :return: First candidate in database order or newly created person Agent.
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

    def genre(self, genre_string: str, standardize: bool = True) -> "RowAPI":
        """
        Resolve a Genre, attempting duplicate maintenance before creating.

        After maintenance, search again and fail if multiple rows remain. If no
        match remains, create a Row with only the genre column populated.

        Example:
            Multiple matches invoke fix_duplicates even when the caller disabled standardization.


        :param genre_string: Genre input; None is rejected, other values are converted to text.
        :param standardize: Apply standardize_genre before lookup.
        :return: Unique matching Genre Row or newly inserted Genre Row.
        :raises InputIntegrityError: Genre input is None.
        :raises DatabaseIntegrityError: Duplicate maintenance leaves multiple matches.
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
            from LiuXin_alpha.databases.maintenance import fix_duplicates

            fix_duplicates(self.db, table="genres", column="genre", comparison=standardize_genre)
            candidate_rows = self.db.search(table="genres", column="genre", search_term=genre)
            if len(candidate_rows) > 1:
                raise DatabaseIntegrityError("Maintenance bot didn't fix the problem")
            elif len(candidate_rows) == 1:
                return candidate_rows[0]
        elif len(candidate_rows) == 1:
            return candidate_rows[0]

        # Lookup has failed. Create.
        genre_row = Row(database=self.db)
        genre_row["genre"] = genre
        genre_row.sync()
        return genre_row

    def identifier(self, identifier: str, identifier_type: str, error: bool = True) -> "RowAPI":
        """
        Normalize a scheme, validate its value and attempt Identifier insertion.

        There is no pre-insertion reuse lookup. Recovery searches identifier text
        without a scheme filter; zero/multiple matches raise DatabaseIntegrityError.
        No encompassing transaction is opened.

        Example:
            error=False still validates the scheme and value before attempting insertion.


        :param identifier: Identifier text; ISBN/ISSN/DOI checkers may normalize it.
        :param identifier_type: Scheme standardized with standardize_id_name.
        :param error: True propagates sync errors; false attempts recovery after DatabaseIntegrityError.
        :return: Inserted Row, or the sole value-only recovery match.
        :raises InputIntegrityError: Scheme or validated identifier value is invalid.
        :raises DatabaseIntegrityError: Insertion fails under strict policy or recovery is not unique.
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
                    raise DatabaseIntegrityError(err_str)
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
    def language(self, language_string: str, lang_code: bool = False) -> "RowAPI":
        """
        Resolve a language by normalized then original name/code, or insert it.

        Insertion writes the standardized input to both language and language_code;
        it does not independently derive a human name and canonical code.

        Example:
            With either mode, name lookups precede code lookups; multiple matches in
            any attempted lookup fail immediately.


        :param language_string: Language input; None rejected and other values converted to text.
        :param lang_code: Exactly True for code, False for name, or "either" for name then code.
        :return: First unique match or inserted language Row.
        :raises InputIntegrityError: Input is None or the mode is unsupported.
        :raises DatabaseIntegrityError: Any attempted lookup finds multiple rows.
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

            raise InputIntegrityError("lang_code must be True, False, or 'either'")

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

    def publisher(self, publisher: str, standardize: bool = True) -> "RowAPI":
        """
        Reuse an exact Publisher value or create an organisation Agent.

        The current multiple-match error-reporting branch references an undefined
        publisher_string name and can raise NameError before its intended integrity
        error. This compatibility defect is retained.

        Example:
            No exact match delegates to add.publisher with the selected spelling.


        :param publisher: Publisher input; None is rejected.
        :param standardize: Apply standardize_publisher before lookup.
        :return: Sole matching Publisher Row or newly created organisation Agent.
        :raises InputIntegrityError: Publisher is None.
        :raises NameError: Multiple matches reach the malformed error-reporting branch.
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

    def rating(self, rating: Union[int, float]) -> "RowAPI":
        """
        Look up the prepopulated Rating Row at int(value)+1.

        No range validation or repair of missing Rating rows occurs.

        Example:
            A rating of 3.9 looks up row ID 4.


        :param rating: Numeric/coercible value; int() truncation and conversion errors apply.
        :return: Database lookup result, possibly None; no row is inserted.
        """
        rating = int(rating)
        rating_id = rating + 1
        return self.db.get_row_from_id("ratings", rating_id)

    # Todo: Gotta work on the qeue situation here
    # Todo: Rethink how series and creators are managed
    # Todo: Upgrade and rethink - there has to be a better way of fuzzily seeking series
    # Weak fuzzy matching of series - generally the author of a series will appear in every book of the series - usually
    # there are exceptions (might want to write a multi-author series exception). But, generally, anything in the
    # Honorverse will have David Weber on the cover somewhere (for example)
    # More problematic is the example of Star Wars - which will probably not have George Lucas on the cover
    def series(
        self,
        creator_rows: Union[tuple["RowAPI"], list["RowAPI"]],
        series_name: str,
        series_queue: Optional[Queue.Queue["RowAPI"]] = None,
        confidence: bool = False,
        stand: bool = True,
        use_phash: bool = True,
    ) -> "RowAPI":
        """
        Find Series candidates, optionally queueing them before creation.

        Queued candidates are not deduplicated. Without a queue, the first exact or
        phonetic match wins. Creation is not transactionally coupled to lookup;
        queue insertion may block and failures can follow earlier queued results.

        Example:
            With a queue, existing candidates do not prevent a subsequent creation attempt.


        :param creator_rows: Optional Creator Rows whose creator columns seed phonetic hashes.
        :param series_name: Name converted to text; exact lookup uses this pre-standardized spelling.
        :param series_queue: Optional queue; when supplied, found candidates are queued without early return.
        :param confidence: When creating with Creator Rows, attach only the first if this flag is true.
        :param stand: Standardize the name for hashes and creation, not the initial exact lookup.
        :param use_phash: Search Creator-specific hashes and then the name-only hash.
        :return: First matching Row without a queue, otherwise a newly created Row also queued.
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

        return series_row

    def series_blind(self, creator_rows, series_name, stand=True, use_phash=True):
        """
        Run queued Series resolution and return its first queued candidate.

        On DatabaseIntegrityError, search the original exact name and take index zero.
        An empty recovery result raises IndexError; an unexpectedly empty queue raises
        queue.Empty. Other failures propagate.

        Example:
            An existing queued match may be returned after series() has also inserted a new row.


        :param creator_rows: Optional Creator Rows passed to series.
        :param series_name: Series name passed unchanged.
        :param stand: Standardization preference passed to series.
        :param use_phash: Phonetic lookup preference passed to series.
        :return: First queued Row, or first exact-name recovery Row after DatabaseIntegrityError.
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

    # Subjects still exists under WEMI. This is fine.
    def subject(self, subject: str, standardize: bool = True) -> "RowAPI":
        """
        Reuse the first exact Subject string or create one without a parent.

        Example:
            standardize=False and standardize=True currently follow the same lookup path.


        :param subject: Value converted with six_unicode.
        :param standardize: Retained compatibility argument; currently ignored.
        :return: First matching Row or newly inserted Subject Row.
        """
        subject = six_unicode(subject)
        subject_rows = self.db.search(table="subjects", column="subject", search_term=subject)
        if len(subject_rows) > 0:
            return subject_rows[0]
        return self.add.subject(subject=subject, subject_parent=None)

    def tag(self, tag_text: str) -> "RowAPI":
        """
        Resolve by search hash, then exact text, otherwise insert a Tag.

        TypeError from hash generation becomes InputIntegrityError. Multiple
        matches at either lookup raise; no lookup/create transaction is opened.

        Example:
            A unique phonetic/search-hash match wins before exact text is checked.


        :param tag_text: Text preserved for insertion; make_tag_search_term derives the matching hash.
        :return: Unique hash/text match or newly created Tag Row.
        :raises InputIntegrityError: Hash generation rejects the input type.
        :raises DatabaseIntegrityError: A hash or exact-text lookup has multiple matches.
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
