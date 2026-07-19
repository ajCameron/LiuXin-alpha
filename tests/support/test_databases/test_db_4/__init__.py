import LiuXin_alpha.utils.libraries.liuxin_tqdm as tqdm
from copy import deepcopy
from itertools import cycle


from LiuXin_alpha.utils.libraries.liuxin_clint import puts, colored

from LiuXin_alpha.library.library import Library

from LiuXin_alpha.metadata.constants import CREATOR_CATEGORIES
from LiuXin_alpha.metadata.constants import RATING_TYPES
from LiuXin_alpha.metadata.constants import ALL_ID_TYPES

from .. import TestDatabaseBuilder
from .._legacy.tools import BasicMetadataFramework

from utils.lx_libraries.liuxin_random import LiuXinBadPseudoRandomGenerator

# Todo: There has to be code around here somewhere to write a database out to csv - find it and use it
# Todo: Perhaps should be an option that you can call from the database class


class TestDB4Builder(TestDatabaseBuilder):
    """
    Execute build for a database with comprehensive test metadata - every field has an automatically generated data set.
    """

    def __init__(
        self,
        dst_file_path,
        csv_folder_path=None,
        dump=False,
        plugin_name=None,
        new_db_uuid=None,
        test_asset_version=None,
        comment_count=100,
        creator_count=400,
        genre_count=100,
        language_count=50,
        publisher_count=100,
        series_count=100,
        subject_count=100,
        tag_count=200,
        title_count=100,
        generate_trees=True,
        comment_creator_max=5,
        comments_for_all_creators=False,
        comment_series_max=5,
        comments_for_all_series=False,
        comment_title_max=5,
        comments_for_all_titles=False,
        creator_note_max=5,
        notes_for_all_creators=False,
        creator_series_max=10,
        creators_for_all_series=False,
        creator_synopsis_max=5,
        synopses_for_all_creators=False,
        creator_tag_max=10,
        tags_for_all_creators=False,
        creator_title_max=5,
        creators_for_all_titles=False,
        genre_series_max=4,
        genres_for_all_series=False,
        genre_title_max=3,
        genres_for_all_titles=False,
        identifier_title_max=5,
        identifiers_for_all_titles=False,
        language_title_contained_max=3,
        language_title_available_max=3,
        languages_for_all_titles=False,
        note_publisher_max=4,
        notes_for_all_publishers=False,
        note_series_max=4,
        notes_for_all_series=False,
        note_title_max=3,
        notes_for_all_titles=False,
        publisher_title_max=3,
        publishers_for_all_titles=False,
        rating_title_max=5,
        ratings_for_all_titles=False,
        series_synopsis_max=3,
        synopses_for_all_series=False,
        series_tag_max=5,
        tags_for_all_series=False,
        series_title_max=3,
        series_for_all_titles=False,
        subject_title_max=5,
        subjects_for_all_titles=False,
        tag_title_max=5,
        tags_for_all_titles=False,
        synopsis_title_max=5,
        synopses_for_all_titles=False,
        folder_store_count=10,
        max_folders_for_book=1,
        max_files_for_book=10,
        max_folders_for_series=1,
    ):

        # Everything is as before - but also need to store a large number of variables to control the data generated
        super(TestDB4Builder, self).__init__(
            dst_file_path=dst_file_path,
            csv_folder_path=csv_folder_path,
            dump=dump,
            plugin_name=plugin_name,
            new_db_uuid=new_db_uuid,
            test_asset_version=test_asset_version,
        )

        # Parameters to control building the test main tables
        self.comment_count = comment_count
        self.creator_count = creator_count
        self.genre_count = genre_count
        self.language_count = language_count
        self.publisher_count = publisher_count
        self.series_count = series_count
        self.subject_count = subject_count
        self.tag_count = tag_count
        self.title_count = title_count

        self.generate_trees = generate_trees

        # Parameters to control the building of the interlink tables
        self.comment_creator_max = comment_creator_max
        self.comments_for_all_creators = comments_for_all_creators
        self.comment_series_max = comment_series_max
        self.comments_for_all_series = comments_for_all_series
        self.comment_title_max = comment_title_max
        self.comments_for_all_titles = comments_for_all_titles

        self.creator_note_max = creator_note_max
        self.notes_for_all_creators = notes_for_all_creators
        self.creator_series_max = creator_series_max
        self.creators_for_all_series = creators_for_all_series
        self.creator_synopsis_max = creator_synopsis_max
        self.synopses_for_all_creators = synopses_for_all_creators
        self.creator_tag_max = creator_tag_max
        self.tags_for_all_creators = tags_for_all_creators
        self.creator_title_max = creator_title_max
        self.creators_for_all_titles = creators_for_all_titles

        self.genre_series_max = genre_series_max
        self.genres_for_all_series = genres_for_all_series
        self.genre_title_max = genre_title_max
        self.genres_for_all_titles = genres_for_all_titles

        self.identifier_title_max = identifier_title_max
        self.identifiers_for_all_titles = identifiers_for_all_titles

        self.language_title_contained_max = language_title_contained_max
        self.language_title_available_max = language_title_available_max
        self.languages_for_all_titles = languages_for_all_titles

        self.note_publisher_max = note_publisher_max
        self.notes_for_all_publishers = notes_for_all_publishers
        self.note_series_max = note_series_max
        self.notes_for_all_series = notes_for_all_series
        self.note_title_max = note_title_max
        self.notes_for_all_titles = notes_for_all_titles

        self.publisher_title_max = publisher_title_max
        self.publishers_for_all_titles = publishers_for_all_titles

        self.rating_title_max = rating_title_max
        self.ratings_for_all_titles = ratings_for_all_titles

        self.series_synopsis_max = series_synopsis_max
        self.synopses_for_all_series = synopses_for_all_series
        self.series_tag_max = series_tag_max
        self.tags_for_all_series = tags_for_all_series
        self.series_title_max = series_title_max
        self.series_for_all_titles = series_for_all_titles

        self.subject_title_max = subject_title_max
        self.subjects_for_all_titles = subjects_for_all_titles

        self.tag_title_max = tag_title_max
        self.tags_for_all_titles = tags_for_all_titles

        self.synopsis_title_max = synopsis_title_max
        self.synopses_for_all_titles = synopses_for_all_titles

        # Random object sources (will be filled in later
        self.test_uuids = None
        self.test_rand_ints = None
        self.test_extend_rand_ints = None
        self.test_rand_size_ints = None
        self.test_rand_cent_ints = None
        self.test_rand_decade_ints = None
        self.test_rand_quad_ints = None
        self.rand_size_ints = None
        self.rand_names_list = None

        # Properties which need to be set as lists
        # Sorted because we need to be sure that the order of the ID_TYPES and RATING_TYPES is consistent between
        # builds - so using sorted to ensure that
        self.all_id_types = sorted(list([t for t in ALL_ID_TYPES]))
        self.rating_types = sorted(list([t for t in RATING_TYPES]))

        self.basic_md_framework = None

        # Display parameters during the build
        self.verbose = False

        # Parameters to control the build of file assets
        self.book_extensions = [
            "lrf",
            "rar",
            "zip",
            "rtf",
            "lit",
            "txt",
            "txtz",
            "text",
            "htm",
            "xhtm",
            "html",
            "htmlz",
            "xhtml",
            "pdf",
            "pdb",
            "updb",
            "pdr",
            "prc",
            "mobi",
            "azw",
            "doc",
            "epub",
            "fb2",
            "djv",
            "djvu",
            "lrx",
            "cbr",
            "cbz",
            "cbc",
            "oebzip",
            "rb",
            "imp",
            "odt",
            "chm",
            "tpz",
            "azw1",
            "pml",
            "pmlz",
            "mbp",
            "tan",
            "tif",
            "snb",
            "xps",
            "oxps",
            "azw4",
            "book",
            "zbf",
            "pobi",
            "docx",
            "docm",
            "md",
            "textile",
            "markdown",
            "ibook",
            "ibooks",
            "iba",
            "azw3",
            "ps",
            "kepub",
        ]

    def info(self, *args, **kwargs):
        """
        Prints the given text in green to the terminal.
        :param args:
        :param kwargs:
        :return:
        """
        puts(colored.green(*args, **kwargs))

    def initialize_random_assets(self, rng=None):
        """
        Initialize the random number generators/random uuid generators which produce the data that will be used to
        generate the records.
        :param rng: If provided, then uses it to advance the initial state of the cycles. If not, then all the cycles
                    are left at zero
        :return:
        """
        from .._legacy.constants import test_uuids

        self.test_uuids = cycle(iter(test_uuids))
        self.test_uuids_list = deepcopy(test_uuids)

        if rng:
            advance_number = rng.randint(0, len(test_uuids))
            for _ in range(advance_number):
                self.test_uuids.next()

        from .._legacy.constants import rand_ints

        self.test_rand_ints = cycle(iter(rand_ints))

        if rng:
            advance_number = rng.randint(0, len(rand_ints))
            for _ in range(advance_number):
                self.test_rand_ints.next()

        from .._legacy.constants import extended_rand_ints

        self.test_extend_rand_ints = cycle(iter(extended_rand_ints))

        if rng:
            advance_number = rng.randint(0, len(extended_rand_ints))
            for _ in range(advance_number):
                self.test_extend_rand_ints.next()

        from .._legacy.constants import rand_size_ints

        self.test_rand_size_ints = cycle(iter(rand_size_ints))

        if rng:
            advance_number = rng.randint(0, len(rand_size_ints))
            for _ in range(advance_number):
                self.test_rand_size_ints.next()

        from .._legacy.constants import rand_cent_ints

        self.test_rand_cent_ints = cycle(iter(rand_cent_ints))

        if rng:
            advance_number = rng.randint(0, len(rand_cent_ints))
            for _ in range(advance_number):
                self.test_rand_cent_ints.next()

        from .._legacy.constants import rand_decade_ints

        self.test_rand_decade_ints = cycle(iter(rand_decade_ints))

        if rng:
            advance_number = rng.randint(0, len(rand_decade_ints))
            for _ in range(advance_number):
                self.test_rand_decade_ints.next()

        from .._legacy.constants import rand_quad_ints

        self.test_rand_quad_ints = cycle(iter(rand_quad_ints))

        if rng:
            advance_number = rng.randint(0, len(rand_quad_ints))
            for _ in range(advance_number):
                self.test_rand_quad_ints.next()

        # ASSET DATA RANDOM FIELDS
        from .._legacy.constants import rand_size_ints

        self.rand_size_ints = cycle(iter(rand_size_ints))

        if rng:
            advance_number = rng.randint(0, len(rand_size_ints))
            for _ in range(advance_number):
                self.rand_size_ints.next()

        from .._legacy.constants import rand_names_list

        self.rand_names_list = cycle(iter(rand_names_list))

        if rng:
            advance_number = rng.randint(0, len(rand_names_list))
            for _ in range(advance_number):
                self.rand_names_list.next()

    def get_random_uuid(self, current_rng, length=None):
        """
        Return a uuid randomly selected from the list
        :param length: The length of the uuid string to return - defaults to None (the entire string is returned)
        :param current_rng: You must pass in an rng to use
        :return:
        """
        if length is None:
            return current_rng.choice(self.test_uuids_list)
        else:
            return current_rng.choice(self.test_uuids_list)[:length]

    @staticmethod
    def get_random_date(internal_rng, start_year=1950):
        """
        Return a date randomly chosen from the list.
        :param internal_rng: We are moving away from random due to repeatability issues.
        :param start_year:
        :return:
        """
        with internal_rng as int_rng:

            import datetime as dt

            start_date = dt.date(start_year, 1, 1)

            # DO NOT CHANGE THIS - as it will invalidate all the dates generated for the dummy databases
            nbdays = (dt.date(2022, 1, 1) - start_date).days
            d = int_rng.randint(0, nbdays)

            birth_date = start_date + dt.timedelta(days=d)
            return birth_date

    @staticmethod
    def get_random_datestamp(internal_rng, start_year=2010):
        """
        Return a date randomly chosen from the list.
        :param internal_rng: We are moving away from random due to repeatability issues.
        :param start_year:
        :return:
        """
        with internal_rng as int_rng:

            import datetime as dt

            start_date = dt.date(start_year, 1, 1)

            # DO NOT CHANGE THIS - as it will invalidate all the dates generated for the dummy databases
            nbdays = (dt.date(2022, 1, 1) - start_date).days
            d = int_rng.randint(0, nbdays)

            birth_date = start_date + dt.timedelta(days=d)
            return birth_date

    def get_limited_rows(self, db, table, table_count, for_all_rows=False):
        """
        Trees built in tables produce a great deal of rows - which is sometimes overkill. This method produces either
        a limited subset of the rows (the test ones deliberately created).
        :param db: The scratch db
        :param table:
        :param table_count:
        :param for_all_rows:
        :return:
        """
        if for_all_rows:
            return db.get_all_rows(table)
        else:
            table_rows = []
            for i in range(1, table_count + 1):
                table_rows.append(db.get_row_from_id(table, row_id=i))
            return table_rows

    def generation_preflight(self, rng, welcome):
        """
        Called before any work of generation is done. Puts the random assets into a predictable form.
        :param rng: The currently in use rng
        :param welcome: Welcome string to tell the user what this long running method is actually doing
        :return:
        """
        self.okay_print(welcome)

        self.initialize_random_assets(rng=rng)

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - GENERATE THE BASIC ROWS

    def generate_comment_rows(self, scratch_db):
        """
        Build the specified number of comment rows.
        These won't be linked to anything in particular. Additional comment rows may be generated as part of making
        the interlinks.
        :param scratch_db:
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(4321654)

        self.generation_preflight(rng=lx_random, welcome="Generating test comments")

        if self.verbose:
            comment_range = range(1, self.comment_count + 1)
        else:
            comment_range = tqdm.tqdm(range(1, self.comment_count + 1))

        for comment_num in comment_range:
            test_comment_row = scratch_db.get_blank_row("comments")
            test_comment_row["comment"] = "TEST COMMENT - {} - DELETE ME - {}" "".format(
                comment_num, self.get_random_uuid(current_rng=lx_random)
            )
            test_comment_row.sync()

            if self.verbose:
                puts(colored.green("Comment {} generated".format(comment_num)))

    def generate_creator_rows(self, scratch_db):
        """
        Build the specified number of creator rows.
        :param scratch_db:
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(432423424242)

        self.generation_preflight(rng=lx_random, welcome="Generating test creators")

        if self.verbose:
            creator_range = range(1, self.creator_count + 1)
        else:
            creator_range = tqdm.tqdm(range(1, self.creator_count + 1))

        for creator_num in creator_range:
            test_creator_row = scratch_db.get_blank_row("creators")

            test_creator_row["creator"] = "c-{}-{}".format(
                creator_num, self.get_random_uuid(length=6, current_rng=lx_random)
            )
            test_creator_row["creator_sort"] = "cs-{}-{}" "".format(
                creator_num, self.get_random_uuid(length=6, current_rng=lx_random)
            )
            test_creator_row["creator_short_name"] = "TEST CREATOR SHORT NAME - {} - DELETE ME - {}" "".format(
                creator_num, self.get_random_uuid(current_rng=lx_random)
            )
            test_creator_row["creator_last_name"] = "TEST CREATOR SURNAME - HONEST - {} - DELETE ME - {}" "".format(
                creator_num, self.get_random_uuid(current_rng=lx_random)
            )
            test_creator_row["creator_phash"] = "TEST CREATOR PHASH - {} - DELETE ME - {}" "".format(
                creator_num, self.get_random_uuid(current_rng=lx_random)
            )
            test_creator_row["creator_legal_name"] = "TEST CREATOR LEGAL NAME - {} - DELETE ME - {}" "".format(
                creator_num, self.get_random_uuid(current_rng=lx_random)
            )

            test_creator_row["creator_birth_date"] = "TEST CREATOR BIRTHDATE - {} - DELETE ME - {}" "".format(
                creator_num, self.get_random_uuid(current_rng=lx_random)
            )
            test_creator_row["creator_death_date"] = "TEST CREATOR DEATHDATE - {} - DELETE ME - {}" "".format(
                creator_num, self.get_random_uuid(current_rng=lx_random)
            )

            test_creator_row["creator_type"] = "TEST CREATOR TYPE - {} - DELETE ME - {}" "".format(
                creator_num, self.get_random_uuid(current_rng=lx_random)
            )
            test_creator_row["creator_seminal_work"] = "TEST CREATOR SEMINAL WORK - {} - DELETE ME - {}" "".format(
                creator_num, self.get_random_uuid(current_rng=lx_random)
            )

            test_creator_row["creator_wikipedia"] = "TEST CREATOR WIKIPEDIA - {} - DELETE ME - {}" "".format(
                creator_num, self.get_random_uuid(current_rng=lx_random)
            )
            test_creator_row["creator_imdb"] = "TEST CREATOR IMDB - {} - DELETE ME - {}" "".format(
                creator_num, self.get_random_uuid(current_rng=lx_random)
            )
            test_creator_row["creator_link"] = "TEST CREATOR LINK - {} - DELETE ME - {}" "".format(
                creator_num, self.get_random_uuid(current_rng=lx_random)
            )

            test_creator_row.sync()

            if self.verbose:
                puts(colored.green("creator {} generated".format(test_creator_row["creator_id"])))

        err_message = [
            "Assertion statement on the number of creators has failed",
            'scratch_db.driver_wrapper.get_record_count("creators"): {}'
            "".format(scratch_db.driver_wrapper.get_record_count("creators")),
        ]
        assert scratch_db.driver_wrapper.get_record_count("creators") >= self.creator_count, "\n".join(err_message)

    def generate_genre_rows(self, scratch_db):
        """
        Generate genre rows and add them to the database.
        :param scratch_db:
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(1274)

        self.generation_preflight(rng=lx_random, welcome="Generating test genres")

        if self.verbose:
            genre_range = range(1, self.genre_count + 1)
        else:
            genre_range = tqdm.tqdm(range(1, self.genre_count + 1))

        for genre_num in genre_range:
            test_genre_row = scratch_db.get_blank_row("genres")

            test_genre_row["genre"] = "TEST GENRE - {} - DELETE ME - {}" "".format(
                genre_num, self.get_random_uuid(current_rng=lx_random)
            )
            test_genre_row["genre_sort"] = "TEST genre_sort - {} - DELETE ME - {}" "".format(
                genre_num, self.get_random_uuid(current_rng=lx_random)
            )
            test_genre_row["genre_phash"] = "TEST genre_phash - {} - DELETE ME - {}" "".format(
                genre_num, self.get_random_uuid(current_rng=lx_random)
            )

            test_genre_row.sync()

            if self.verbose:
                puts(colored.green("genre {} generated".format(test_genre_row["genre_id"])))

        if self.generate_trees:
            self.hang_trees(scratch_db, "genres", parent_position=False)

    # Todo: Trim the "rating_source" method out of the database
    def generate_language_rows(self, scratch_db):
        """
        Generate false language rows for the database.
        :param scratch_db:
        :return:
        """
        from datetime import datetime
        from datetime import timedelta

        # title_created_datestamp
        language_datestamp_start_time = "2022-01-10 23:01:44"
        language_datestamp_current_time = datetime.strptime(language_datestamp_start_time, "%Y-%m-%d %H:%M:%S")
        language_datestamp_delta = timedelta(seconds=1)

        lx_random = LiuXinBadPseudoRandomGenerator(657893324)

        self.generation_preflight(rng=lx_random, welcome="Generating test languages")

        if self.verbose:
            lang_range = range(1, self.language_count + 1)
        else:
            lang_range = tqdm.tqdm(range(1, self.language_count + 1))

        scratch_db.driver_wrapper.clear("languages")
        for lang_num in lang_range:
            test_lang_row = scratch_db.get_blank_row("languages")

            test_lang_row["language"] = "TEST LANGUAGE - {} - DELETE ME - {}" "".format(
                lang_num, self.get_random_uuid(current_rng=lx_random)
            )
            test_lang_row["language_code"] = "TEST LANGUAGE CODE - {} - DELETE ME - {}" "".format(
                lang_num, self.get_random_uuid(current_rng=lx_random)
            )

            test_lang_row["language_datestamp"] = str(language_datestamp_current_time)
            language_datestamp_current_time += language_datestamp_delta

            test_lang_row.sync()

            if self.verbose:
                puts(colored.green("lang {} generated".format(lang_num)))

    def generate_publisher_rows(self, scratch_db):
        """
        Generate false publisher rows for the database.
        :param scratch_db:
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(1275)

        self.generation_preflight(rng=lx_random, welcome="Generating test publishers")

        if self.verbose:
            pub_range = range(1, self.publisher_count + 1)
        else:
            pub_range = tqdm.tqdm(range(1, self.publisher_count + 1))

        for pub_num in pub_range:
            test_pub_row = scratch_db.get_blank_row("publishers")

            test_pub_row["publisher"] = "TEST PUBLISHER - {} - DELETE ME - {}".format(
                pub_num, self.get_random_uuid(current_rng=lx_random)
            )
            test_pub_row["publisher_sort"] = "TEST publisher_sort - {} - DELETE ME - {}" "".format(
                pub_num, self.get_random_uuid(current_rng=lx_random)
            )
            test_pub_row["publisher_phash"] = "TEST publisher_phash - {} - DELETE ME - {}" "".format(
                pub_num, self.get_random_uuid(current_rng=lx_random)
            )

            test_pub_row["publisher_wikipedia"] = "TEST publisher_wikipedia - {} - DELETE ME - {}" "".format(
                pub_num, self.get_random_uuid(current_rng=lx_random)
            )
            test_pub_row["publisher_website"] = "TEST publisher_website - {} - DELETE ME - {}" "".format(
                pub_num, self.get_random_uuid(current_rng=lx_random)
            )

            test_pub_row.sync()

            if self.verbose:
                puts(colored.green("publisher row {} generate".format(pub_num)))

        if self.generate_trees:
            self.hang_trees(scratch_db, "publishers", parent_position=False)

    def generate_series_rows(self, scratch_db):
        """
        Generate series rows for the database.
        :param scratch_db:
        :return:
        """
        from datetime import datetime
        from datetime import timedelta

        # deterministically set the series datestamp
        series_datestamp_start_time = "2023-01-10 23:01:44"
        series_datestamp_current_time = datetime.strptime(series_datestamp_start_time, "%Y-%m-%d %H:%M:%S")
        series_datestamp_delta = timedelta(seconds=1)

        lx_random = LiuXinBadPseudoRandomGenerator(1296)

        self.generation_preflight(rng=lx_random, welcome="Generating test series")

        if self.verbose:
            series_range = range(1, self.series_count + 1)
        else:
            series_range = tqdm.tqdm(range(1, self.series_count + 1))

        # Make the specified non tree series rows
        for series_num in series_range:
            test_series_row = scratch_db.get_blank_row("series")

            test_series_row["series"] = "s-{}-{}" "".format(
                series_num, self.get_random_uuid(length=8, current_rng=lx_random)
            )
            test_series_row["series_sort"] = "TEST series_sort - {} - DELETE ME - {}" "".format(
                series_num, self.get_random_uuid(current_rng=lx_random)
            )
            test_series_row["series_phash"] = "TEST series_phash - {} - DELETE ME - {}" "".format(
                series_num, self.get_random_uuid(current_rng=lx_random)
            )

            test_series_row["series_over_author"] = lx_random.choice([0, 1])

            test_series_row["series_datestamp"] = str(series_datestamp_current_time)
            series_datestamp_current_time += series_datestamp_delta

            test_series_row.sync()

            if self.verbose:
                puts(colored.green("series {} generated".format(test_series_row["series_id"])))

        if self.generate_trees:
            self.hang_trees(
                scratch_db,
                "series",
                parent_position=True,
                start_datestamp=series_datestamp_current_time,
            )

    def generate_subject_rows(self, scratch_db):
        """
        Generate subject rows for the database - including tree data
        :param scratch_db:
        :return:
        """
        from datetime import datetime
        from datetime import timedelta

        lx_random = LiuXinBadPseudoRandomGenerator(1297)

        self.generation_preflight(rng=lx_random, welcome="Generating test subjects")

        if self.verbose:
            subject_range = range(1, self.subject_count + 1)
        else:
            subject_range = tqdm.tqdm(range(1, self.subject_count + 1))

        subject_datestamp_start_time = "2017-04-24 23:59:11"
        subject_datestamp_start_time = datetime.strptime(subject_datestamp_start_time, "%Y-%m-%d %H:%M:%S")
        subject_datestamp_delta = timedelta(seconds=1)

        # Make test subjects not in trees
        for subject_num in subject_range:
            test_subject_row = scratch_db.get_blank_row("subjects")

            test_subject_row["subject"] = "TEST SUBJECT - {} - DELETE ME - {}" "".format(
                subject_num, self.get_random_uuid(current_rng=lx_random)
            )
            test_subject_row["subject_phash"] = "TEST subject_phash - {} - DELETE ME {}" "".format(
                subject_num, self.get_random_uuid(current_rng=lx_random)
            )
            test_subject_row["subject_sort"] = "TEST subject_sort - {} - DELETE ME - {}" "".format(
                subject_num, self.get_random_uuid(current_rng=lx_random)
            )

            # Todo: Switch over to linux epoch time
            test_subject_row["subject_datestamp"] = str(subject_datestamp_start_time)
            subject_datestamp_start_time += subject_datestamp_delta

            test_subject_row.sync()

            if self.verbose:
                puts(colored.green("Generating subject row {}".format(test_subject_row["subject_id"])))

        if self.generate_trees:
            self.hang_trees(scratch_db=scratch_db, table="subjects", parent_position=True)

    def generate_tag_rows(self, scratch_db):
        """
        Generate tag rows for the database.
        :param scratch_db:
        :return:
        """
        from datetime import datetime
        from datetime import timedelta

        lx_random = LiuXinBadPseudoRandomGenerator(1302)

        self.generation_preflight(rng=lx_random, welcome="Generating test tags")

        if self.verbose:
            tag_range = range(1, self.tag_count + 1)
        else:
            tag_range = tqdm.tqdm(range(1, self.tag_count + 1))

        tag_datestamp_start_time = "2022-04-24 23:59:11"
        tag_datestamp_start_time = datetime.strptime(tag_datestamp_start_time, "%Y-%m-%d %H:%M:%S")
        tag_datestamp_delta = timedelta(seconds=1)

        for tag_num in tag_range:
            test_tag_row = scratch_db.get_blank_row("tags")

            test_tag_row["tag"] = "TEST TAG - {} - {}" "".format(tag_num, self.get_random_uuid(current_rng=lx_random))
            test_tag_row["tag_phash"] = "TEST tag_phash - {} - DELETE ME - {}" "".format(
                tag_num, self.get_random_uuid(current_rng=lx_random)
            )

            test_tag_row["tag_datestamp"] = str(tag_datestamp_start_time)
            tag_datestamp_start_time += tag_datestamp_delta

            test_tag_row.sync()

            if self.verbose:
                puts(colored.green("tag {} generated".format(tag_num)))

    def generate_title_rows(self, scratch_db, scratch_lib):
        """
        Generate randomly populated tag rows.
        :param scratch_db:
        :return:
        """
        from datetime import datetime
        from datetime import timedelta

        lx_random = LiuXinBadPseudoRandomGenerator(1302)

        self.generation_preflight(rng=lx_random, welcome="Generating test titles")

        if self.verbose:
            title_range = range(1, self.title_count + 1)
        else:
            title_range = tqdm.tqdm(range(1, self.title_count + 1))

        # title_created_datestamp
        title_created_datestamp_start_time = "2022-04-24 23:59:11"
        title_created_datestamp_start_time = datetime.strptime(title_created_datestamp_start_time, "%Y-%m-%d %H:%M:%S")
        title_created_datestamp_delta = timedelta(seconds=1)

        # title_datestamp
        title_datestamp = 1650844751

        for title_num in title_range:

            # Generate the title row - will then generate a book row based off it
            test_title_row = scratch_db.get_blank_row("titles")

            test_title_row["title"] = "t-{}-{}".format(title_num, self.get_random_uuid(length=8, current_rng=lx_random))
            test_title_row["title_sort"] = "TEST TITLE SORT - {} - DELETE ME - {}" "".format(
                title_num, self.get_random_uuid(current_rng=lx_random)
            )
            test_title_row["title_phash"] = "TEST TITLE PHASH - {} - DELETE ME - {}" "".format(
                title_num, self.get_random_uuid(current_rng=lx_random)
            )

            test_title_row["title_creator_sort"] = "TEST TITLE CREATOR SORT - {} - DELETE ME - {}" "".format(
                title_num, self.get_random_uuid(current_rng=lx_random)
            )

            test_title_row["title_pub_date"] = self.get_random_date(lx_random)
            test_title_row["title_copyright_date"] = "TEST TITLE COPYRIGHT DATE - {} - DELETE ME - {}" "".format(
                title_num, self.get_random_uuid(current_rng=lx_random)
            )
            test_title_row["title_wikipedia"] = "TEST TITLE WIKIPEDIA - {} - DELETE ME - {}" "".format(
                title_num, self.get_random_uuid(current_rng=lx_random)
            )
            test_title_row[
                "title_fiction_length_category"
            ] = "TEST title_fiction_length_category - {} - " "DELETE ME - {}".format(
                title_num, self.get_random_uuid(current_rng=lx_random)
            )
            test_title_row["title_type"] = "TEST title_type - {} - DELETE ME - {}" "".format(
                title_num, self.get_random_uuid(current_rng=lx_random)
            )
            test_title_row["title_wordcount"] = lx_random.randint(1, 1000000)

            test_title_row["title_source"] = "TEST title_source - {} - DELETE ME - {}" "".format(
                title_num, self.get_random_uuid(current_rng=lx_random)
            )
            test_title_row["title_source_path"] = "TEST title_source_path - {} - DELETE ME - {}" "".format(
                title_num, self.get_random_uuid(current_rng=lx_random)
            )
            test_title_row["title_source_name"] = "TEST title_source_name - {} - DELETE ME - {}" "".format(
                title_num, self.get_random_uuid(current_rng=lx_random)
            )

            # Write some timestamp info out
            test_title_row["title_last_modified"] = str(title_created_datestamp_start_time)
            test_title_row["title_created_datestamp"] = str(title_created_datestamp_start_time)
            title_created_datestamp_start_time += title_created_datestamp_delta

            test_title_row["title_datestamp"] = title_datestamp
            title_datestamp += 1

            test_title_row.sync()

            # Generate the parameters for the new book and prepare to write
            test_book_sort = "TEST BOOK_SORT - {} - Probably Not In Use - DELETE ME - {}" "".format(
                title_num, self.get_random_uuid(current_rng=lx_random)
            )
            test_book_flags = "TEST BOOK_FLAGS - {} - Delete Me - {}".format(
                title_num, self.get_random_uuid(current_rng=lx_random)
            )
            test_book_pubdate = self.get_random_date(lx_random)
            test_book_copyright_date = self.get_random_date(lx_random)
            test_book_uuid = self.get_random_uuid(current_rng=lx_random)

            # Write the variables out to a books rows corresponding to the title
            scratch_lib.add.book(
                title_row=test_title_row,
                book_sort=test_book_sort,
                book_flags=test_book_flags,
                book_pubdate=test_book_pubdate,
                book_copyright_date=test_book_copyright_date,
                book_uuid=test_book_uuid,
            )

            if self.verbose:
                puts(colored.green("test title {} generated with book".format(test_title_row["title_id"])))

    def make_comment_creator_links(self, scratch_db):
        """
        Comments on creators - reviewing them (this table needs to be changed to reviews at some point).
        :param scratch_db:
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(14098)

        self.generation_preflight(
            rng=lx_random,
            welcome="Generating test comments and linking them to creators",
        )

        creator_rows = self.get_limited_rows(scratch_db, "creators", self.creator_count, self.comments_for_all_creators)

        for creator_row in creator_rows:

            total_comment_num = lx_random.randint(0, self.comment_creator_max)
            for comment_num in range(1, total_comment_num + 1):
                test_note_str = "TEST COMMENT - CREATOR {} - COMMENT NUM - {} - {}" "".format(
                    creator_row["creator_id"],
                    comment_num,
                    self.get_random_uuid(current_rng=lx_random),
                )
                scratch_db.apply.comments(comment=test_note_str, resource_row=creator_row)

            puts(
                colored.green(
                    "{} comments generated and linked to creator {}"
                    "".format(total_comment_num, creator_row["creator_id"])
                )
            )

    def make_comment_series_links(self, scratch_db):
        """
        Comments on series - reviewing them (this table needs to be changed to reviews at some point).
        :param scratch_db:
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(14098)

        self.generation_preflight(rng=lx_random, welcome="Generating test comments and linking them to series")

        series_rows = self.get_limited_rows(scratch_db, "series", self.series_count, self.comments_for_all_series)

        for series_row in series_rows:

            total_comment_num = lx_random.randint(0, self.comment_creator_max)
            for comment_num in range(1, total_comment_num + 1):
                test_note_str = "TEST COMMENT - SERIES {} - COMMENT NUM - {} - {}" "".format(
                    series_row["series_id"],
                    comment_num,
                    self.get_random_uuid(current_rng=lx_random),
                )
                scratch_db.apply.comments(comment=test_note_str, resource_row=series_row)

            puts(
                colored.green(
                    "{} comments generated and linked to creator {}"
                    "".format(total_comment_num, series_row["series_id"])
                )
            )

    def make_comment_title_links(self, scratch_db):
        """
        Comments on titles - reviewing them (this table needs to be changed to reviews at some point).
        :param scratch_db:
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(14099)

        self.generation_preflight(
            rng=lx_random,
            welcome="Generating test comments and linking them to creators",
        )

        title_rows = self.get_limited_rows(scratch_db, "titles", self.title_count, self.comments_for_all_titles)

        for title_row in title_rows:

            total_comment_num = lx_random.randint(0, self.comment_creator_max)
            for comment_num in range(1, total_comment_num + 1):
                test_note_str = "TEST COMMENT - TITLE {} - NOTE NUM - {} - {}" "".format(
                    title_row["title_id"],
                    comment_num,
                    self.get_random_uuid(current_rng=lx_random),
                )
                scratch_db.apply.comments(comment=test_note_str, resource_row=title_row)

            puts(
                colored.green(
                    "{} comments generated and linked to title {}" "".format(total_comment_num, title_row["title_id"])
                )
            )

    # ----

    def make_creator_note_links(self, scratch_db):
        """
        Link creators to notes describing them.
        Notes are generated for the creators as they are needed. This will result in a large number of notes being
        :param scratch_db:
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(5484631351)

        self.generation_preflight(rng=lx_random, welcome="Generating notes for creators")

        creator_rows = self.get_limited_rows(scratch_db, "creators", self.creator_count, self.notes_for_all_creators)

        for creator_row in creator_rows:

            total_note_num = lx_random.randint(0, self.creator_note_max)
            for note_num in range(1, total_note_num + 1):
                test_note_str = "TEST NOTE - CREATOR {} - NOTE NUM - {} - {}" "".format(
                    creator_row["creator_id"],
                    note_num,
                    self.get_random_uuid(current_rng=lx_random),
                )
                scratch_db.apply.note(note=test_note_str, resource=creator_row)

            puts(
                colored.green(
                    "{} notes generated and linked to creator {}" "".format(total_note_num, creator_row["creator_id"])
                )
            )

    def make_creator_series_links(self, scratch_db):
        """
        Link existing creators to existing series.
        Number of links to be generated is (by default) a random number up to the number of series available.
        Series to be linked will also be drawn randomly - with repeated results discarded. So the total number will
        always be less that the drawn number.
        :param scratch_db:
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(9804814141)

        self.generation_preflight(rng=lx_random, welcome="About to generate creator-series links")

        if self.creators_for_all_series:
            total_series_count = scratch_db.driver_wrapper.get_record_count("series")
        else:
            total_series_count = self.creator_count
        if not total_series_count:
            return

        creator_rows = self.get_limited_rows(scratch_db, "creators", self.creator_count, self.creators_for_all_series)

        # Each series can have one, and only one, creator. Check that this is the case - before trying for an update
        # Which will hopefully save time
        used_creator_series_ids = set()

        # Link creator to a randomly chosen selection if series
        for creator_row in creator_rows:

            creator_series_total = lx_random.randint(0, self.creator_series_max)
            creator_series_num = 0
            for creator_series_num in range(creator_series_total):

                # To account for the fact that the 0 series row may exist
                try:
                    cand_series_id = lx_random.randint(1, total_series_count - 1)
                except ValueError:
                    return

                # Check to see if the generated candidate pair already exists - if it does, then ignore and continue
                # Otherwise it'd hit the interlink row function - which would fail - so saving all that
                if cand_series_id in used_creator_series_ids:
                    continue
                used_creator_series_ids.add(cand_series_id)

                try:
                    creator_series_row = scratch_db.get_row_from_id("series", cand_series_id)
                except ValueError:
                    # Will hit a value error if there aren't enough series to support the generation - so abort before
                    # things get more badly screwed up
                    return

                # try:
                scratch_db.interlink_rows(primary_row=creator_row, secondary_row=creator_series_row)
                # except DatabaseIntegrityError:
                #     # The series is, presumably, already linked to the series - ignore
                #     continue
                # else:
                creator_series_num += 1

            puts(
                colored.green(
                    "{} of {} series added to creators {}".format(
                        creator_series_num,
                        creator_series_total,
                        creator_row["creator_id"],
                    )
                )
            )

    def make_creator_synopsis_links(self, scratch_db, scratch_lib):
        """
        Generating and linking synopsis to the creators
        :param scratch_db:
        :param scratch_lib:
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(12343525)

        self.generation_preflight(rng=lx_random, welcome="Generating test synopses for creators")

        creator_rows = self.get_limited_rows(scratch_db, "creators", self.creator_count, self.synopses_for_all_creators)

        for creator_row in creator_rows:

            total_synop_num = lx_random.randint(0, self.creator_synopsis_max)
            for synop_num in range(1, total_synop_num + 1):
                test_synop_str = "TEST SYNOPSIS - CREATOR {} - SYNOPSIS NUM {} - {}" "".format(
                    creator_row["creator_id"],
                    synop_num,
                    self.get_random_uuid(current_rng=lx_random),
                )
                scratch_lib.apply.synopsis(synopsis=test_synop_str, resource=creator_row)

            puts(
                colored.green(
                    "{} synopses generated and linked to creator {}"
                    "".format(total_synop_num, creator_row["creator_id"])
                )
            )

    def make_creator_tag_links(self, scratch_db):
        """
        Applies randomly chosen tags to the creator. Maximum will be set by the creator_tag_max parameter.
        :param scratch_db:
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(657)

        self.generation_preflight(rng=lx_random, welcome="About to generate creator-tag links")

        db_tag_count = scratch_db.driver_wrapper.get_record_count("tags")
        # Link each series to a randomly chosen number of tags

        creator_rows = self.get_limited_rows(scratch_db, "creators", self.creator_count, self.tags_for_all_creators)

        creator_tag_id_pairs = set()

        for creator_row in creator_rows:

            creator_row_id = creator_row.row_id

            creator_tag_total = lx_random.randint(0, self.creator_tag_max)
            creator_tag_count = 0
            for creator_tag_num in range(creator_tag_total):

                creator_tag_cand_id = lx_random.randint(1, db_tag_count)

                # Filter for the pairs which have already been written - and ignore them
                creator_tag_pair = (creator_row_id, creator_tag_cand_id)
                if creator_tag_pair in creator_tag_id_pairs:
                    continue
                creator_tag_id_pairs.add(creator_tag_pair)

                creator_tag_row = scratch_db.get_row_from_id("tags", creator_tag_cand_id)

                scratch_db.interlink_rows(primary_row=creator_row, secondary_row=creator_tag_row)
                # except DatabaseIntegrityError:
                #     # The tag is, presumably, already linked to the series - ignore
                #     continue
                # else:
                creator_tag_count += 1

            puts(
                colored.green(
                    "{} of {} tags added to series {}".format(
                        creator_tag_count, creator_tag_total, creator_row["creator_id"]
                    )
                )
            )

    def make_creator_title_links(self, scratch_db):
        """
        Construct creator_title links.
        The maximum number of creators that can be associated with a title is given by the creator_title_max parameter.
        This is maximum for each CATEGORY of creators - so a title could, theoretically, have
        len(CREATOR_CATEGORIES) * creator_title_max categories
        :param scratch_db:
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(89)

        self.generation_preflight(rng=lx_random, welcome="Generating title-creator links")

        title_rows = self.get_limited_rows(scratch_db, "titles", self.title_count, self.creators_for_all_titles)

        creator_id_title_id_pairs = set()

        for title_row in title_rows:

            title_row_id = title_row.row_id

            title_creators = 0
            total_title_creators = 0
            for creator_role in CREATOR_CATEGORIES:

                creator_count = lx_random.randint(0, self.creator_title_max)

                # This was originally a mistake - now actually created
                total_title_creators += creator_count
                total_title_creators += 1

                creator_category_count = 0
                for creator_num in range(0, creator_count + 1):

                    # Select a repeatably random row from the creators table
                    creator_id = lx_random.randint(1, self.creator_count)

                    # Check to see if it's a combination which has been added before - if it has, no reason to do the
                    # work of trying - and failing - to add it again
                    cand_cid_tid_cr_pair = (creator_id, title_row_id)
                    if cand_cid_tid_cr_pair in creator_id_title_id_pairs:
                        continue
                    creator_id_title_id_pairs.add(cand_cid_tid_cr_pair)

                    creator_row = scratch_db.get_row_from_id("creators", creator_id)
                    if creator_row is None:
                        puts(colored.red("creator {} skipped".format(creator_id)))

                    # Title may already be linked to the creator
                    # try:
                    scratch_db.interlink_rows(
                        primary_row=title_row,
                        secondary_row=creator_row,
                        type=creator_role,
                    )
                    # except DatabaseIntegrityError:
                    #     pass
                    # else:
                    creator_category_count += 1

                title_creators += creator_category_count

            puts(
                colored.green(
                    "{} of {} creators linked to title {}".format(
                        title_creators, total_title_creators, title_row["title_id"]
                    )
                )
            )

    # ----

    def make_genre_series_links(self, scratch_db):
        """
        Construct the genre title links.
        Titles will be linked to a randomly chosen number of rnadomly chosen genres
        :param scratch_db:
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(494309)

        self.generation_preflight(rng=lx_random, welcome="Generating genre-series links")

        # Todo - Go through and check that we're using the TRUE current count - might be radically different due to trees
        db_genre_count = scratch_db.driver_wrapper.get_record_count("genres")

        series_rows = self.get_limited_rows(scratch_db, "series", self.series_count, self.genres_for_all_series)

        genre_id_series_id_pairs = set()

        for series_row in series_rows:

            series_row_id = series_row.row_id

            series_genre_count = lx_random.randint(0, self.genre_series_max)

            series_genres = 0
            for genre_link_num in range(series_genre_count):

                genre_row_id = lx_random.randint(1, db_genre_count)

                gid_sid_pair = (genre_row_id, series_row_id)
                if gid_sid_pair in genre_id_series_id_pairs:
                    continue
                genre_id_series_id_pairs.add(gid_sid_pair)

                genre_row = scratch_db.get_row_from_id("genres", genre_row_id)

                scratch_db.interlink_rows(primary_row=series_row, secondary_row=genre_row)

                series_genres += 1

            puts(
                colored.green(
                    "{} of {} genres linked to series {}".format(
                        series_genres, series_genre_count, series_row["series_id"]
                    )
                )
            )

    def make_genre_title_links(self, scratch_db):
        """
        Link genres to titles. The maximum number of genres that can be linked to a title is given by genre_title_mac
        :param scratch_db:
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(46)

        self.generation_preflight(rng=lx_random, welcome="Generating genre-title links")

        db_genre_count = scratch_db.driver_wrapper.get_record_count("genres")

        title_rows = self.get_limited_rows(scratch_db, "titles", self.title_count, self.genres_for_all_titles)

        genre_id_title_id_pairs = set()

        for title_row in title_rows:

            title_row_id = title_row.row_id

            title_genre_count = lx_random.randint(0, self.genre_title_max)

            title_genres = 0
            for genre_link_num in range(title_genre_count):

                genre_row_id = lx_random.randint(1, db_genre_count)

                cand_gid_tid_pair = (genre_row_id, title_row_id)
                if cand_gid_tid_pair in genre_id_title_id_pairs:
                    continue
                genre_id_title_id_pairs.add(cand_gid_tid_pair)

                genre_row = scratch_db.get_row_from_id("genres", genre_row_id)

                scratch_db.interlink_rows(primary_row=title_row, secondary_row=genre_row)

                title_genres += 1

            puts(
                colored.green(
                    "{} of {} genres linked to title {}".format(title_genres, title_genre_count, title_row["title_id"])
                )
            )

    def make_identifier_title_links(self, scratch_db):
        """
        Construct identifier-title links.
        identifiers are generated on the fly.
        Control the maximum number of identifiers for any one title using the identifier_title_max parameter.
        Generates both internal and external identifiers.
        :param scratch_db:
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(50)

        self.generation_preflight(rng=lx_random, welcome="Generating identifier-title links")

        title_rows = self.get_limited_rows(scratch_db, "titles", self.title_count, self.identifiers_for_all_titles)

        for title_row in title_rows:

            id_count = lx_random.randint(0, self.identifier_title_max)

            seen_id_types = set()

            for i in range(1, id_count + 1):

                id_type = lx_random.choice(self.all_id_types)

                id_row = scratch_db.get_blank_row("identifiers")
                id_row["identifier"] = "TEST EXTERNAL IDENTIFIER - TYPE {} - TITLE {} - ID NUM {} - {}" "".format(
                    id_type,
                    title_row["title_id"],
                    i,
                    self.get_random_uuid(current_rng=lx_random),
                )
                id_row["identifier_type"] = id_type
                id_row.sync()

                scratch_db.interlink_rows(primary_row=title_row, secondary_row=id_row, type=id_type)

                seen_id_types.add(id_type)

            puts(
                colored.green(
                    "{} identifiers of types {} generated and added to title {}".format(
                        id_count, seen_id_types, title_row["title_id"]
                    )
                )
            )

    def make_language_title_links(self, scratch_db, scratch_lib):
        """
        There are three different types - primary_language, available_language and contained_language - set one for the
        primary and a random number for the others.
        :param scratch_db:
        :param scratch_lib:
        :return:
        """
        db_language_count = scratch_db.driver_wrapper.get_record_count("languages")

        lx_random = LiuXinBadPseudoRandomGenerator(41984149141)

        self.generation_preflight(rng=lx_random, welcome="Generating a primary language for every title")

        title_rows = self.get_limited_rows(scratch_db, "titles", self.title_count, self.languages_for_all_titles)

        # - primary - language
        for title_row in title_rows:
            lang_id = lx_random.randint(1, db_language_count)
            lang_row = scratch_db.get_row_from_id("languages", lang_id)
            scratch_lib.apply.primary_language(language=lang_row, title_row=title_row)

            puts(colored.green("primary language set for title {}".format(title_row["title_id"])))

        self.generation_preflight(
            rng=lx_random,
            welcome="About to add a number of contained in languages to the title",
        )

        # - contained_in - language

        ci_lang_id_title_id_pair = set()

        for title_row in title_rows:

            title_row_id = title_row.row_id

            contained_count = lx_random.randint(0, self.language_title_contained_max)
            title_lang_count = 0
            for i in range(contained_count):

                lang_id = lx_random.randint(1, db_language_count)

                cand_lid_tid_pair = (lang_id, title_row_id)
                if cand_lid_tid_pair in ci_lang_id_title_id_pair:
                    continue
                ci_lang_id_title_id_pair.add(cand_lid_tid_pair)

                lang_row = scratch_db.get_row_from_id("languages", lang_id)

                scratch_db.apply.contained_language(language=lang_row, title_row=title_row)

                title_lang_count += 1

            puts(
                colored.green(
                    "{} of {} contained languages added to title {}".format(
                        title_lang_count, contained_count, title_row["title_id"]
                    )
                )
            )

        lx_random = LiuXinBadPseudoRandomGenerator(524234234)

        self.generation_preflight(
            rng=lx_random,
            welcome="About to add a number of available languages to the title",
        )

        # - available_language - language
        available_lang_id_title_id_pair = set()

        for title_row in title_rows:

            title_row_id = title_row.row_id

            available_count = lx_random.randint(0, self.language_title_available_max)
            title_lang_count = 0
            for i in range(available_count):

                lang_id = lx_random.randint(1, db_language_count)

                available_lid_tid_pair = (lang_id, title_row_id)
                if available_lid_tid_pair in available_lang_id_title_id_pair:
                    continue
                available_lang_id_title_id_pair.add(available_lid_tid_pair)

                lang_row = scratch_db.get_row_from_id("languages", lang_id)

                scratch_lib.apply.available_language(language=lang_row, title_row=title_row)

                title_lang_count += 1

            puts(
                colored.green(
                    "{} of {} available languages added to title {}".format(
                        title_lang_count, available_count, title_row["title_id"]
                    )
                )
            )

    def make_note_publisher_links(self, scratch_db, scratch_lib):
        """
        Generate notes and associate them with the given publisher.
        The total number of notes that will be associated with any one publisher is controlled with note_publisher_max.
        :param scratch_db:
        :param scratch_lib:
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(524234234)

        self.generation_preflight(rng=lx_random, welcome="Generating notes for publishers")

        publisher_rows = self.get_limited_rows(
            scratch_db,
            "publishers",
            self.publisher_count,
            self.notes_for_all_publishers,
        )

        for pub_row in publisher_rows:

            total_note_num = lx_random.randint(0, self.note_publisher_max)

            for note_num in range(1, total_note_num + 1):
                test_note_str = "TEST NOTE - PUBLISHER {} - NOTE NUM {} - {}" "".format(
                    pub_row["publisher"],
                    note_num,
                    self.get_random_uuid(current_rng=lx_random),
                )
                scratch_lib.apply.note(note=test_note_str, resource=pub_row)

            puts(colored.green("{} notes generated for publisher {}".format(total_note_num, pub_row["publisher_id"])))

    def make_note_series_links(self, scratch_db, scratch_lib):
        """
        Generates notes and associates them with all series.
        The maximum number of notes that will be associated with any series is given by note_series_max.
        :param scratch_db:
        :param scratch_lib:
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(41984149141)

        self.generation_preflight(rng=lx_random, welcome="Generating notes for series")

        series_rows = self.get_limited_rows(scratch_db, "series", self.series_count, self.notes_for_all_series)

        for series_row in series_rows:

            total_note_num = lx_random.randint(0, self.note_series_max)
            for note_num in range(1, total_note_num + 1):
                test_note_str = "TEST NOTE - SERIES {} - NOTE NUM {} - {}" "".format(
                    series_row["series"],
                    note_num,
                    self.get_random_uuid(current_rng=lx_random),
                )
                scratch_lib.apply.note(note=test_note_str, resource=series_row)

            puts(colored.green("{} notes generated for series {}".format(total_note_num, series_row["series_id"])))

    def make_note_title_links(self, scratch_db, scratch_lib):
        """
        Generate notes and associate them with titles.
        The maximum number of notes that will be associated with a title is controlled with the note_title_max
        parameter.
        :param scratch_db:
        :param scratch_lib:
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(552520502525)

        self.generation_preflight(rng=lx_random, welcome="Generating notes for titles")

        title_rows = self.get_limited_rows(scratch_db, "titles", self.title_count, self.notes_for_all_titles)

        # The number of notes associated with the title and their text are both randomly generated
        for title_row in title_rows:

            total_note_num = lx_random.randint(0, self.note_title_max)

            for note_num in range(1, total_note_num + 1):
                test_note_str = "TEST NOTE - TITLE {} - NOTE NUM {} - {}" "".format(
                    title_row["title_id"],
                    note_num,
                    self.get_random_uuid(current_rng=lx_random),
                )
                scratch_lib.apply.note(note=test_note_str, resource=title_row)

            puts(colored.green("{} notes generated for title {}".format(total_note_num, title_row["title_id"])))

    def make_publisher_title_links(self, scratch_db):
        """
        Publishers are selected from the publishers table and applied to titles from the titles table.
        The maximum number of titles to be set is controlled with publisher_title_max.
        :param scratch_db:
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(49)

        self.generation_preflight(rng=lx_random, welcome="About to associate publishers with titles")

        db_publisher_count = scratch_db.driver_wrapper.get_record_count("publishers")

        title_rows = self.get_limited_rows(scratch_db, "titles", self.title_count, self.publishers_for_all_titles)

        pub_id_title_id_pairs = set()

        for title_row in title_rows:

            title_row_id = title_row.row_id

            title_pub_count = lx_random.randint(0, self.publisher_title_max)
            total_title_pubs = 0
            for i in range(0, title_pub_count + 1):

                pub_id = lx_random.randint(1, db_publisher_count)

                pub_id_title_id_cand_pair = (pub_id, title_row_id)
                if pub_id_title_id_cand_pair in pub_id_title_id_pairs:
                    continue
                pub_id_title_id_pairs.add(pub_id_title_id_cand_pair)

                pub_row = scratch_db.get_row_from_id("publishers", pub_id)

                scratch_db.interlink_rows(primary_row=title_row, secondary_row=pub_row)

                total_title_pubs += 1

            puts(
                colored.green(
                    "{} of {} publishers have been added to title {}".format(
                        total_title_pubs, title_pub_count, title_row["title_id"]
                    )
                )
            )

    def make_rating_title_links(self, scratch_db):
        """
        Construct links forming an number of ratings for each of the titles.
        The maximum number of ratings that will be assigned to a title is determined by rating_title_max. Number will
        vary between 0 and that value.
        :param scratch_db:
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(81)

        self.generation_preflight(rng=lx_random, welcome="About to apply test ratings to titles")

        title_rows = self.get_limited_rows(scratch_db, "titles", self.title_count, self.ratings_for_all_titles)

        title_id_rating_type_triples = set()

        # Associate either 0 or 1 rating of each type to each of the titles
        for title_row in title_rows:

            title_row_id = title_row.row_id

            total_title_ratings = lx_random.randint(0, self.rating_title_max)
            applied_title_ratings = 0
            for i in range(total_title_ratings):
                rating_type = lx_random.choice(self.rating_types)

                rating_int = lx_random.randint(1, 10)

                cand_rid_tid_type_triple = (title_row_id, rating_type)
                if cand_rid_tid_type_triple in title_id_rating_type_triples:
                    continue
                title_id_rating_type_triples.add(cand_rid_tid_type_triple)

                self.basic_md_framework.apply.rating(rating=rating_int, rating_type=rating_type, resource_row=title_row)

                applied_title_ratings += 1

            # Todo: This form is quite useful - apply it everywhere
            puts(
                colored.green(
                    "{} of {} ratings applied to title {}".format(
                        applied_title_ratings,
                        total_title_ratings,
                        title_row["title_id"],
                    )
                )
            )

    def make_series_synopsis_links(self, scratch_db, scratch_lib):
        """
        Generating test series synopsis for the series.
        Synopsis will be linked. The number generated will be controled by the parameter series_synopsis_max. Number
        added will be a random choice in the range 0-series_synopsis_max.
        :param scratch_db:
        :param scratch_lib:
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(57324085)

        self.generation_preflight(
            rng=lx_random,
            welcome="Generating test series synopsis and linking them to each of the series",
        )

        series_rows = self.get_limited_rows(scratch_db, "series", self.series_count, self.synopses_for_all_series)

        for series_row in series_rows:

            total_synop_num = lx_random.randint(0, self.series_synopsis_max)

            for synop_num in range(1, total_synop_num + 1):
                test_synop_str = "TEST SYNOPSIS - SERIES {} - SYNOPSIS NUM {} - {}" "".format(
                    series_row["series"],
                    synop_num,
                    self.get_random_uuid(current_rng=lx_random),
                )
                scratch_lib.apply.synopsis(synopsis=test_synop_str, resource=series_row)

            puts(
                colored.green(
                    "{} synopsis have been linked to series {}" "".format(total_synop_num, series_row["series_id"])
                )
            )

    def make_series_tag_links(self, scratch_db):
        """
        Link some of the pre-existing random tags to the series.
        Tags will be randomly selected from the existing tags table and applied to each of the series rows.
        :param scratch_db:
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(657)

        self.generation_preflight(rng=lx_random, welcome="About to generate series-tag links")

        total_tag_count = scratch_db.driver_wrapper.get_record_count("tags")

        series_rows = self.get_limited_rows(scratch_db, "series", self.series_count, self.tags_for_all_series)

        series_id_tag_id_pairs = set()

        # Link each series to a randomly chosen number of tags
        for series_row in series_rows:

            series_row_id = series_row.row_id

            series_tag_total = lx_random.randint(0, self.series_tag_max)
            series_tag_added = 0
            for series_tag_num in range(series_tag_total):

                series_tag_row_id = lx_random.randint(1, total_tag_count)

                cand_sid_tid_pair = (series_row_id, series_tag_row_id)
                if cand_sid_tid_pair in series_id_tag_id_pairs:
                    continue
                series_id_tag_id_pairs.add(cand_sid_tid_pair)

                series_tag_row = scratch_db.get_row_from_id("tags", series_tag_row_id)
                scratch_db.interlink_rows(primary_row=series_row, secondary_row=series_tag_row)

                series_tag_added += 1

            puts(
                colored.green(
                    "{} of {} tags added to series {}".format(
                        series_tag_added, series_tag_total, series_row["series_id"]
                    )
                )
            )

    def make_series_title_links(self, scratch_db):
        """
        Link each of the titles to a randomly determined number of series.
        Maximum number of series each title will be linked to is controlled with the series_title_max parameter
        :param scratch_db:
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(58959386)

        self.generation_preflight(rng=lx_random, welcome="Generating series title links")

        db_series_count = scratch_db.driver_wrapper.get_record_count("series")

        title_rows = self.get_limited_rows(scratch_db, "titles", self.title_count, self.series_for_all_titles)

        series_id_title_id_pairs = set()

        # Link each of the titles with a randomly selected number of randomly selected series
        for title_row in title_rows:

            title_row_id = title_row.row_id

            series_count = lx_random.randint(0, self.series_title_max)
            series_added_to_title = 0
            for series_num in range(1, series_count + 1):

                series_id = lx_random.randint(1, db_series_count - 1)

                cand_sid_tid_pair = (series_id, title_row_id)
                if cand_sid_tid_pair in series_id_title_id_pairs:
                    continue
                series_id_title_id_pairs.add(cand_sid_tid_pair)

                series_row = scratch_db.get_row_from_id("series", series_id)

                scratch_db.interlink_rows(
                    primary_row=title_row,
                    secondary_row=series_row,
                    index=lx_random.randint(0, 20),
                )

                series_added_to_title += 1

            puts(
                colored.green(
                    "{} of {} series added to title {}".format(
                        series_added_to_title, series_count, title_row["title_id"]
                    )
                )
            )

    def make_subject_title_links(self, scratch_db):
        """
        Link existing subjects to the existing titles - titles can be linked to multiple subjects - if they will or not
        depends on the density.
        :param scratch_db:
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(13223)

        self.generation_preflight(rng=lx_random, welcome="Linking titles to subjects")

        # Todo: Better way to do this might be to get unique ids and drop the zero row
        # Todo: Make this a get_valid_target_ids methods
        db_subject_count = scratch_db.driver_wrapper.get_record_count("subjects")

        title_rows = self.get_limited_rows(scratch_db, "titles", self.title_count, self.subjects_for_all_titles)

        subject_id_title_id_pairs = set()

        for title_row in title_rows:

            title_row_id = title_row.row_id

            subject_count = lx_random.randint(0, self.subject_title_max)
            subjects_added_to_title = 0
            for subject_num in range(1, subject_count + 1):

                subject_id = lx_random.randint(1, db_subject_count - 1)

                cand_sid_tid_pair = (subject_id, title_row_id)
                if cand_sid_tid_pair in subject_id_title_id_pairs:
                    continue
                subject_id_title_id_pairs.add(cand_sid_tid_pair)

                subject_row = scratch_db.get_row_from_id("subjects", subject_id)

                scratch_db.interlink_rows(primary_row=title_row, secondary_row=subject_row)

                subjects_added_to_title += 1

            puts(
                colored.green(
                    "{} of {} series added to title {}".format(
                        subjects_added_to_title, subject_count, title_row["title_id"]
                    )
                )
            )

    def make_tag_title_links(self, scratch_db):
        """
        Makes the tag_title links.
        Iterates over the titles table adding tags to each of the title rows.
        Maximum number of tags which will be applied to a title are given by tag_title_max. A randomly selected number
        up to that (and including 0) will be selected.
        :param scratch_db:
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(53253255322142142421)

        self.generation_preflight(rng=lx_random, welcome="About to generate tag-title links")

        total_tag_count = scratch_db.driver_wrapper.get_record_count("tags")

        title_rows = self.get_limited_rows(scratch_db, "titles", self.title_count, self.tags_for_all_titles)

        tag_id_title_id_pairs = set()

        # Link each title to a randomly selected number of tags up to tag_title_max
        for title_row in title_rows:

            title_row_id = title_row.row_id

            tag_title_count = lx_random.randint(0, self.tag_title_max)
            actual_title_tag_count = 0
            for i in range(tag_title_count):

                target_tag_id = lx_random.randint(1, total_tag_count)

                cand_tagid_tid_pair = (target_tag_id, title_row_id)
                if cand_tagid_tid_pair in tag_id_title_id_pairs:
                    continue
                tag_id_title_id_pairs.add(cand_tagid_tid_pair)

                target_tag_row = scratch_db.get_row_from_id("tags", target_tag_id)

                scratch_db.interlink_rows(primary_row=title_row, secondary_row=target_tag_row)

                actual_title_tag_count += 1

            puts(
                colored.green(
                    "{} title tag links have been generated for title {}"
                    "".format(actual_title_tag_count, tag_title_count, title_row["title_id"])
                )
            )

    def make_synopsis_title_links(self, scratch_db, scratch_lib):
        """
        Generate synopsis rows and link them to every title row.
        :param scratch_db:
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(41984149141)

        self.generation_preflight(
            rng=lx_random,
            welcome="Generating test title synopses and linking them to the title",
        )

        title_rows = self.get_limited_rows(scratch_db, "titles", self.title_count, self.synopses_for_all_titles)

        for title_row in title_rows:

            total_synop_num = lx_random.randint(0, self.synopsis_title_max)

            for synop_num in range(1, total_synop_num + 1):

                test_synop_str = "TEST SYNOPSIS - TITLE {} - SYNOPSIS NUM {} - {}" "".format(
                    title_row["title"],
                    synop_num,
                    self.get_random_uuid(current_rng=lx_random),
                )
                scratch_lib.apply.synopsis(synopsis=test_synop_str, resource=title_row)

            puts(colored.green("{} synopses generated for title {}".format(total_synop_num, title_row["title_id"])))

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - CONSTRUCT INTRALINKS
    def generic_build_intralink_table(self, scratch_db, target_table, link_types, seed=14567):
        """
        Replacement for the table specific methods.
        :param scratch_db:
        :param target_table:
        :param link_types:
        :param seed:
        :return:
        """
        # Needed to maintain compatibility with the previous implementation
        assert link_types, "some link types need to be provided"
        if len(link_types) == 1:
            monotype = link_types[0]
        else:
            monotype = False

        puts(colored.green("About to intralink the {} table".format(target_table)))
        lx_random = LiuXinBadPseudoRandomGenerator(seed)
        tt_creator_count = scratch_db.driver_wrapper.get_record_count(target_table)

        done_id_pairs = set()

        target_table_id = scratch_db.driver_wrapper.get_id_column(target_table)

        for target_table_row in scratch_db.get_all_rows(target_table):

            tt_row_id = target_table_row[target_table_id]
            target_tt_id = lx_random.randint(1, tt_creator_count)

            if tt_row_id == target_tt_id:
                continue

            # Check to see if we've generated this pair before
            cand_tt_id_pair_a = (tt_row_id, target_tt_id)
            if cand_tt_id_pair_a in done_id_pairs:
                continue
            done_id_pairs.add(cand_tt_id_pair_a)

            cand_tt_id_pair_b = (target_tt_id, tt_row_id)
            if cand_tt_id_pair_b in done_id_pairs:
                continue
            done_id_pairs.add(cand_tt_id_pair_b)

            if monotype:
                target_creator_row = scratch_db.get_row_from_id(target_table, row_id=target_tt_id)
                scratch_db.intralink_rows(
                    primary_row=target_table_row,
                    secondary_row=target_creator_row,
                    link_type=monotype,
                )
            else:
                target_creator_row = scratch_db.get_row_from_id(target_table, row_id=target_tt_id)
                scratch_db.intralink_rows(
                    primary_row=target_table_row,
                    secondary_row=target_creator_row,
                    link_type=lx_random.choice(link_types),
                )

            puts(colored.green("Intralink built for {} {}".format(target_table, tt_row_id)))

    def build_publisher_publisher_intralinks(self, scratch_db):
        """
        Constructs the title-title intralinks
        :param scratch_db:
        :return:
        """
        puts(colored.green("About to intralink the creators table"))

        lx_random = LiuXinBadPseudoRandomGenerator(14567357)

        table_id_col = scratch_db.driver_wrapper.get_id_column("publishers")
        all_valid_ids = scratch_db.get_values_set(table_id_col, iterator_return=False)
        try:
            all_valid_ids.remove(0)
        except KeyError:
            pass
        if not all_valid_ids:
            return
        all_valid_ids_list = list([i for i in all_valid_ids])

        publisher_id_pairs = set()

        for publisher_row in scratch_db.get_all_rows("publishers"):

            publisher_id = publisher_row["publisher_id"]
            target_publisher_id = lx_random.choice(all_valid_ids_list)

            if publisher_id == target_publisher_id:
                continue

            # Check to see if we've generated this pair before
            cand_pub_id_pair_a = (publisher_id, target_publisher_id)
            if cand_pub_id_pair_a in publisher_id_pairs:
                continue
            publisher_id_pairs.add(cand_pub_id_pair_a)

            cand_pub_id_pair_b = (target_publisher_id, publisher_id)
            if cand_pub_id_pair_b in publisher_id_pairs:
                continue
            publisher_id_pairs.add(cand_pub_id_pair_b)

            target_title_row = scratch_db.get_row_from_id("publishers", row_id=target_publisher_id)
            scratch_db.intralink_rows(
                primary_row=publisher_row,
                secondary_row=target_title_row,
                link_type="user_marked_different",
            )

        assert (
            scratch_db.driver_wrapper.get_record_count("publisher_publisher_intralinks") > 0
        ), "No publisher_publisher_intralinks have been written"

    #
    # ------------------------------------------------------------------------------------------------------------------
    # ------------------------------------------------------------------------------------------------------------------
    #
    # - UTILS

    def hang_trees(
        self,
        scratch_db,
        table,
        tree_count=10,
        seed=1234,
        parent_position=True,
        start_datestamp=None,
    ):
        """
        Hang trees of the existing rows for the given table.
        :param scratch_db:
        :param table:
        :param tree_count: Total number of trees to be generated and hung off existing rows
        :param seed: Seed for the rng to be used to determine the characteristics of the trees to be built
        :param parent_position: If True then will attempt to populate the parent_position column of the database with
                                a position for the newly created row under the parent.
        :return:
        """
        if start_datestamp is None:
            return self._hang_trees_no_datestamp(
                scratch_db=scratch_db,
                table=table,
                tree_count=tree_count,
                seed=seed,
                parent_position=parent_position,
            )

        from datetime import timedelta

        datestamp_delta = timedelta(seconds=1)
        table_datestamp_col = scratch_db.driver_wrapper.get_datestamp_column(table)

        # Todo: Check that the trees do not intersect or cycle - only want that in deliberately malformed databases
        puts(colored.green("Generating test {} trees. {} will be generated.".format(table, tree_count)))
        lx_random = LiuXinBadPseudoRandomGenerator(seed)

        # Select ten genre rows and then link ten sub genres to ech of them
        # Todo: Warning - it's highly possible this will generated loops - especially for small genre counts
        # Avoid this by using the mechanism used to crete subject trees
        # Todo: add_child method in the database driver - with loop detected
        # Todo: Problem with multiple draws of the same object
        # Make tree subjects - choose twenty at random and add subjects to them
        chosen_ids = set()

        title_0_row = scratch_db.driver_wrapper.get_row_from_id(table, row_id=0)
        table_id_col = scratch_db.driver_wrapper.get_id_column(table)
        all_valid_ids = scratch_db.get_values_set(table_id_col, iterator_return=False)
        try:
            all_valid_ids.remove(0)
        except KeyError:
            pass

        all_valid_ids_list = list([i for i in all_valid_ids])

        if len(all_valid_ids_list) == 0:
            return

        if self.verbose:
            tree_range = range(0, tree_count)
        else:
            tree_range = tqdm.tqdm(range(0, tree_count))

        for i in tree_range:

            # Choose a random row to hang the tree off
            tree_root_row_id = lx_random.choice(all_valid_ids_list)
            tree_root_row = scratch_db.get_row_from_id(table, tree_root_row_id)

            generate_test_tree_with_datestamps(
                root_row=tree_root_row,
                row_name_str="TEST {0} - IN TREE - {0} ID {1} - {1}".format(table, "{}"),
                datestamp_col=table_datestamp_col,
                datestamp_start=start_datestamp,
                datestamp_delta=datestamp_delta,
                uuid_stream=self.test_uuids,
                seed=tree_root_row_id,
                parent_position=parent_position,
            )

            if self.verbose:
                puts(colored.green("Generated {} tree {} with datestamps".format(table, i)))

    def _hang_trees_no_datestamp(self, scratch_db, table, tree_count=10, seed=1234, parent_position=True):
        """
        Hang trees of the existing rows for the given table.
        :param scratch_db:
        :param table:
        :param tree_count: Total number of trees to be generated and hung off existing rows
        :param seed: Seed for the rng to be used to determine the characteristics of the trees to be built
        :param parent_position: If True then will attempt to populate the parent_position column of the database with
                                a position for the newly created row under the parent.
        :return:
        """
        # Todo: Check that the trees do not intersect or cycle - only want that in deliberately malformed databases
        puts(colored.green("Generating test {} trees. {} will be generated.".format(table, tree_count)))
        lx_random = LiuXinBadPseudoRandomGenerator(seed)

        # Select ten genre rows and then link ten sub genres to ech of them
        # Todo: Warning - it's highly possible this will generated loops - especially for small genre counts
        # Avoid this by using the mechanism used to crete subject trees
        # Todo: add_child method in the database driver - with loop detected
        # Todo: Problem with multiple draws of the same object
        # Make tree subjects - choose twenty at random and add subjects to them
        chosen_ids = set()

        title_0_row = scratch_db.driver_wrapper.get_row_from_id(table, row_id=0)
        table_id_col = scratch_db.driver_wrapper.get_id_column(table)
        all_valid_ids = scratch_db.get_values_set(table_id_col, iterator_return=False)
        try:
            all_valid_ids.remove(0)
        except KeyError:
            pass

        all_valid_ids_list = list([i for i in all_valid_ids])

        if len(all_valid_ids_list) == 0:
            return

        if self.verbose:
            tree_range = range(0, tree_count)
        else:
            tree_range = tqdm.tqdm(range(0, tree_count))

        for i in tree_range:

            # Choose a random row to hang the tree off
            tree_root_row_id = lx_random.choice(all_valid_ids_list)
            tree_root_row = scratch_db.get_row_from_id(table, tree_root_row_id)

            generate_test_tree(
                root_row=tree_root_row,
                row_name_str="TEST {0} - IN TREE - {0} ID {1} - {1}".format(table, "{}"),
                uuid_stream=self.test_uuids,
                seed=tree_root_row_id,
                parent_position=parent_position,
            )

            if self.verbose:
                puts(colored.green("Generated {} tree {}".format(table, i)))

    #
    # ------------------------------------------------------------------------------------------------------------------

    def detail_databases(self, scratch_db):
        """
        Make a database with comprehensive test metadata.
        Dummy values are generated for books, files and folders. These are randomly interlinked to try and generate as
        good a possible an approximating of a thoroughly filled out database.
        Options are provided to change the number of books and other assets created. This allows generating very large
        data sets for performance testing.
        :param scratch_db: The fake data will be added to this database (database should start empty or results will be
                           unpredictable)
        :return:
        """
        self.initialize_random_assets()

        test_db = scratch_db
        self.basic_md_framework = BasicMetadataFramework(db=scratch_db)

        test_lib = Library(db=test_db, fsm="")

        assert 0 == test_db.driver_wrapper.get_record_count("titles")

        # --------------------------------------------------------------------------------------------------------------
        #
        # - CONSTRUCT DUMMY MAIN TABLES

        # CONSTRUCT THE METADATA ELEMENTS FIRST - THE ASSET ELEMENTS WILL BE LINKED TO THESE AFTER THEY'RE CREATED
        self.generate_comment_rows(scratch_db)
        self.generate_creator_rows(scratch_db)
        self.generate_genre_rows(scratch_db)

        # IDENTIFIER ROWS - generated as required while linking them to the titles
        # NOTE ROWS - generated as required ehile linking them to the various assets that they're about

        self.generate_language_rows(scratch_db)
        self.generate_publisher_rows(scratch_db)

        # RATINGS ROWS - Should already exist

        self.generate_series_rows(scratch_db)
        self.generate_subject_rows(scratch_db)

        # SYNOPSES ROWS - will be generated as they're applied to other resources
        self.generate_tag_rows(scratch_db)

        self.generate_title_rows(scratch_db, test_lib)

        #
        # --------------------------------------------------------------------------------------------------------------
        # --------------------------------------------------------------------------------------------------------------
        #
        # - CONSTRUCT DUMMY INTERLINK TABLES
        self.populate_interlink_tables(scratch_db, test_lib)
        #
        # --------------------------------------------------------------------------------------------------------------
        # --------------------------------------------------------------------------------------------------------------
        #
        # - WRITE DUMMY ENTRIES INTO THE INTRALINK TABLES
        self.populate_intralink_tables(scratch_db)
        #
        # --------------------------------------------------------------------------------------------------------------
        # --------------------------------------------------------------------------------------------------------------
        #
        # - GIVE THE USER A CHANCE TO ADD ANY CUSTOM DATA THEY WANT

        self.add_new_main_tables(scratch_db)
        self.add_custom_columns(scratch_db)
        self.populate_custom_columns(scratch_db)

        #
        # ------------------------------------------------------------------------------------------------------------------
        # ------------------------------------------------------------------------------------------------------------------
        #
        # - BUILD FAKE ASSET DATA
        self.generate_fake_asset_data(scratch_db)

        return scratch_db

    def populate_interlink_tables(self, scratch_db, test_lib):
        """
        Populate the interlink tables.
        :param scratch_db:
        :return:
        """
        # ----------------------
        # - MAKE COMMENT-X LINKS
        self.make_comment_creator_links(scratch_db)
        self.make_comment_series_links(scratch_db)
        self.make_comment_title_links(scratch_db)
        # ----------------------

        # ----------------------
        # - MAKE CREATOR-X LINKS
        self.make_creator_note_links(scratch_db)
        self.make_creator_series_links(scratch_db)
        self.make_creator_synopsis_links(scratch_db, test_lib)
        self.make_creator_tag_links(scratch_db)
        self.make_creator_title_links(scratch_db)
        # ---------------------------------------
        # ---------------------------------------
        # - MAKE GENRE-X LINKS
        self.make_genre_series_links(scratch_db)
        self.make_genre_title_links(scratch_db)
        # -------------------------------------
        # -------------------------
        # - MAKE IDENTIFIER-X LINKS
        self.make_identifier_title_links(scratch_db)
        # -------------------------
        # -----------------------
        # - MAKE LANGUAGE-X LINKS
        self.make_language_title_links(scratch_db, test_lib)
        # -----------------------
        # -------------------
        # - MAKE NOTE-X LINKS
        self.make_note_publisher_links(scratch_db, test_lib)
        self.make_note_series_links(scratch_db, test_lib)
        self.make_note_title_links(scratch_db, test_lib)
        # -------------------
        # ------------------------
        # - MAKE PUBLISHER-X LINKS
        self.make_publisher_title_links(scratch_db)
        # ------------------------
        # ---------------------
        # - MAKE RATING-X LINKS
        self.make_rating_title_links(scratch_db)
        # ---------------------
        # ---------------------
        # - MAKE SERIES-X LINKS
        # Todo: Validate that links are being actually written to all these tables
        self.make_series_synopsis_links(scratch_db, test_lib)
        self.make_series_tag_links(scratch_db)
        self.make_series_title_links(scratch_db)
        # ---------------------
        # ----------------------
        # - MAKE SUBJECT-X LINKS
        self.make_subject_title_links(scratch_db)
        # ----------------------
        # ------------------
        # - MAKE TAG-X LINKS
        self.make_tag_title_links(scratch_db)
        # ------------------
        # -----------------------
        # - MAKE SYNOPSIS-X LINKS
        self.make_synopsis_title_links(scratch_db, test_lib)
        # -----------------------

    def populate_intralink_tables(self, scratch_db):
        """
        Populate the intralink tables
        :param scratch_db:
        :return:
        """
        # --------------------------------------------------------------------------------------------------------------
        #
        # - CONSTRUCT INTRALINK TABLES
        self.generic_build_intralink_table(scratch_db, "creators", ("user_marked_different",), 14567)
        self.generic_build_intralink_table(scratch_db, "titles", ("user_marked_different",), 14567357)
        self.build_publisher_publisher_intralinks(scratch_db)
        #
        # --------------------------------------------------------------------------------------------------------------

    def generate_fake_asset_data(self, scratch_db):
        """
        Legacy folder_stores-backed asset generation has been retired.

        Current support DB provisioning uses the FRBR-native synthetic builders
        in tests.support.test_resources_manager, and the provisioned DBs no
        longer materialize a folder_stores table.
        """
        return scratch_db

    def add_new_main_tables(self, scatch_db):
        """
        Called before asset generation - gives the user a chance to add any custom tables they want - to populate them,
        link them to other tables e.t.c.
        :param scatch_db:
        :return:
        """
        pass

    def add_custom_columns(self, scratch_db):
        """
        Called before asset generation - gives the user a chance to add any custom columns they want and to populate
        them.
        :param scratch_db:
        :return:
        """
        pass

    def populate_custom_columns(self, scratch_db):
        """
        Gives a user the change to populate the custom columns when they have been created.
        :param scratch_db:
        :return:
        """
        pass

    def write_timestamps(self, scratch_db):

        # Because of the way that books are being created for titles we don't get direct book row access
        # So we need to write this out now, after the fact.
        self.write_timestamps_books_table(scratch_db)

    @staticmethod
    def write_timestamps_books_table(scratch_db):
        """
        Update the timestamp columns of the books field to static values, freezing them after database rebuilds.
        :param scratch_db:
        :return:
        """
        from datetime import datetime
        from datetime import timedelta

        # Update the books table
        book_lm_datestamp_start_time = "2022-04-24 23:59:11"
        book_lm_datestamp_start_time = datetime.strptime(book_lm_datestamp_start_time, "%Y-%m-%d %H:%M:%S")
        book_delta = timedelta(seconds=1)

        book_created_datestamp = 1650844348
        for book_row in scratch_db.get_all_rows("books"):

            book_row["book_created_datestamp"] = book_created_datestamp
            book_created_datestamp += 1

            book_row["book_last_modified"] = str(book_lm_datestamp_start_time)
            book_lm_datestamp_start_time += book_delta

            book_row.sync()

def build_test_db(
    dst_file_path,
    dump=False,
    plugin_name=None,
    new_db_uuid="auto",
    test_asset_version=None,
):
    """
    Construct the test database specified by this module.
    In this case a blank database is constructed and filled with data - before being copied into the test_databases
    folder.
    :param dst_file_path: The file to write the database to after it's been built.
    :param dump: HERE IGNORED
    :return:
    """
    test_db_builder = TestDB4Builder(
        dst_file_path=dst_file_path,
        csv_folder_path=None,
        dump=dump,
        new_db_uuid=new_db_uuid,
        plugin_name=plugin_name,
        test_asset_version=test_asset_version,
    )
    test_db_builder.run()


def generate_test_tree(
    root_row,
    row_name_str=None,
    uuid_stream=None,
    parent_position=True,
    seed=100,
    max_layers=5,
):
    """
    Used to build trees of rows rooted at the root row.
    Trees should not intersect (though additional trees can be hung off other trees and some entries might have more
    than one tree)
    :param root_row: The tree will be rooted in this row
    :param row_name_str: Will be used to generate a name for each of the new rows
    :param uuid_stream: A source of uuids to insert into the raw name string
    :param parent_position: Some tables with a tree like structure have a parent_position column. This is used to
                            sort the sub rows under the main row.
                            If this parameter is True then the parent_position column will be populated.
    :param seed:
    :param max_layers: The maximum number of layers which will be build.
    :return:
    """
    lx_random = LiuXinBadPseudoRandomGenerator(seed)

    table = root_row.table
    db = root_row.catalog
    table_col_base = db.driver_wrapper.get_column_base(table)
    parent_col = "{}_parent".format(table_col_base)
    parent_pos = "{}_parent_position".format(table_col_base)

    # used to generate the parent positions for the child rows of the main row
    parent_positions = {1, 2, 3, 4, 5, 6, 7, 8, 9}

    # The number of layers that the tree will be built with
    layers = lx_random.randint(1, max_layers)

    previous_layer_rows = [root_row]
    for i in range(0, layers):

        # Used to record the elements at the current level of the tree - later will be used to hang other rows on it
        current_layer_rows = []
        for previous_layer_row in previous_layer_rows:

            # The number of rows to become the children of the current row - has to be limited - as the size grows
            # exponentially
            layer_count = lx_random.randint(1, 4)
            current_parent_positions = deepcopy(parent_positions)

            for i in range(1, layer_count + 1):

                # The new row to be added at this level
                current_row = db.get_blank_row(table)
                current_row[parent_col] = previous_layer_row.row_id

                # Set the position - this should make sure that the same position is not set twice
                if parent_position:
                    this_row_parent_pos = lx_random.choice([n for n in current_parent_positions])
                    current_parent_positions.remove(this_row_parent_pos)
                    current_row[parent_pos] = this_row_parent_pos

                if row_name_str is not None:
                    current_row[table_col_base] = row_name_str.format(
                        current_row.row_id, _advance_legacy_iterator(uuid_stream)
                    )

                current_row.sync()

                current_layer_rows.append(current_row)

        previous_layer_rows = current_layer_rows


def generate_test_tree_with_datestamps(
    root_row,
    datestamp_col,
    datestamp_start,
    datestamp_delta,
    row_name_str=None,
    uuid_stream=None,
    parent_position=True,
    seed=100,
    max_layers=5,
):
    """
    Used to build trees of rows rooted at the root row.
    Trees should not intersect (though additional trees can be hung off other trees and some entries might have more
    than one tree)
    :param root_row: The tree will be rooted in this row
    :param row_name_str: Will be used to generate a name for each of the new rows
    :param uuid_stream: A source of uuids to insert into the raw name string
    :param parent_position: Some tables with a tree like structure have a parent_position column. This is used to
                            sort the sub rows under the main row.
                            If this parameter is True then the parent_position column will be populated.
    :param seed:
    :param max_layers: The maximum number of layers which will be build.
    :return:
    """
    lx_random = LiuXinBadPseudoRandomGenerator(seed)

    table = root_row.table
    db = root_row.catalog
    table_col_base = db.driver_wrapper.get_column_base(table)
    parent_col = "{}_parent".format(table_col_base)
    parent_pos = "{}_parent_position".format(table_col_base)

    # used to generate the parent positions for the child rows of the main row
    parent_positions = {1, 2, 3, 4, 5, 6, 7, 8, 9}

    # The number of layers that the tree will be built with
    layers = lx_random.randint(1, max_layers)

    current_datestamp = deepcopy(datestamp_start)

    previous_layer_rows = [root_row]
    for i in range(0, layers):

        # Used to record the elements at the current level of the tree - later will be used to hang other rows on it
        current_layer_rows = []
        for previous_layer_row in previous_layer_rows:

            # The number of rows to become the children of the current row - has to be limited - as the size grows
            # exponentially
            layer_count = lx_random.randint(1, 4)
            current_parent_positions = deepcopy(parent_positions)

            for i in range(1, layer_count + 1):

                # The new row to be added at this level
                current_row = db.get_blank_row(table)
                current_row[parent_col] = previous_layer_row.row_id

                current_row[datestamp_col] = str(current_datestamp)
                current_datestamp += datestamp_delta

                # Set the position - this should make sure that the same position is not set twice
                if parent_position:
                    this_row_parent_pos = lx_random.choice([n for n in current_parent_positions])
                    current_parent_positions.remove(this_row_parent_pos)
                    current_row[parent_pos] = this_row_parent_pos

                if row_name_str is not None:
                    current_row[table_col_base] = row_name_str.format(
                        current_row.row_id, _advance_legacy_iterator(uuid_stream)
                    )

                current_row.sync()

                current_layer_rows.append(current_row)

        previous_layer_rows = current_layer_rows


def _advance_legacy_iterator(stream):
    """Accept both Py2-style `.next()` objects and normal Python 3 iterators."""
    if stream is None:
        raise TypeError("uuid_stream is required when row_name_str is provided")
    next_method = getattr(stream, "next", None)
    if callable(next_method):
        return next_method()
    return next(stream)


from .._tree_generators import (  # noqa: E402
    generate_test_tree as generate_test_tree,
    generate_test_tree_with_datestamps as generate_test_tree_with_datestamps,
)
