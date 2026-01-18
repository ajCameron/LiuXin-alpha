# Make data that will be used to test the FormatsTable

# Books are linked to folders. Folders can be basically arbitary resources somewhere out in the real world (provided
# there is a driver for them present in the FolderStores section so LiuXin can talk to them).
# The files which are present inside each of these folders are linked to the folder in the links table
# So, effectively, files are linked to books through the folders table.
# The book-folder link does not have a priority that matters
# The folder-file lin priority serves as the priority column for the book-file link - it affects the order the files
# appear and the extension they are assigned

from itertools import cycle

from clint.textui import puts, colored

from LiuXin.databases.row import Row

from LiuXin_tests.test_databases import TestDatabaseBuilder
from LiuXin_tests.test_databases.test_db_1 import test_db_1_folder

__folder__ = test_db_1_folder


class TestDB3Builder(TestDatabaseBuilder):
    """
    Constructs test_db_3 - which has a LOT of (largely invalid) fake file and folder data
    """

    def detail_databases(self, scratch_db):
        make_file_test_data(scratch_db)

        return scratch_db

    def write_timestamps(self, scratch_db):

        self.write_timestamps_books_table(scratch_db)
        self.write_timestamps_titles_table(scratch_db)

    @staticmethod
    def write_timestamps_books_table(scratch_db):
        """
        Update the timestamp columns of the books field to static values, freezing them after database rebuilds.
        :param scratch_db:
        :return:
        """
        # Update the books table

        theo_book_created_datestamp_dict = {
            1: 1650844348,
            2: 1650844348,
            3: 1650844348,
            4: 1650844348,
            5: 1650844348,
            6: 1650844348,
            7: 1650844348,
            8: 1650844348,
            9: 1650844348,
            10: 1650844348,
            11: 1650844348,
            12: 1650844348,
            13: 1650844348,
            14: 1650844348,
            15: 1650844348,
            16: 1650844348,
            17: 1650844348,
            18: 1650844348,
            19: 1650844348,
            20: 1650844348,
            21: 1650844348,
            22: 1650844348,
            23: 1650844348,
            24: 1650844348,
            25: 1650844348,
            26: 1650844348,
            27: 1650844348,
            28: 1650844348,
            29: 1650844348,
            30: 1650844348,
            31: 1650844348,
            32: 1650844348,
            33: 1650844348,
            34: 1650844348,
            35: 1650844348,
            36: 1650844348,
            37: 1650844348,
            38: 1650844348,
            39: 1650844348,
            40: 1650844348,
            41: 1650844348,
            42: 1650844348,
            43: 1650844348,
            44: 1650844348,
            45: 1650844348,
            46: 1650844348,
            47: 1650844348,
            48: 1650844348,
            49: 1650844348,
            50: 1650844348,
            51: 1650844348,
            52: 1650844348,
            53: 1650844348,
            54: 1650844348,
            55: 1650844348,
            56: 1650844348,
            57: 1650844348,
            58: 1650844348,
            59: 1650844348,
            60: 1650844348,
            61: 1650844348,
            62: 1650844348,
            63: 1650844348,
            64: 1650844348,
            65: 1650844348,
            66: 1650844348,
            67: 1650844348,
            68: 1650844349,
            69: 1650844349,
            70: 1650844349,
            71: 1650844349,
            72: 1650844349,
            73: 1650844349,
            74: 1650844349,
            75: 1650844349,
            76: 1650844349,
            77: 1650844349,
            78: 1650844349,
            79: 1650844349,
            80: 1650844349,
            81: 1650844349,
            82: 1650844349,
            83: 1650844349,
            84: 1650844349,
            85: 1650844349,
            86: 1650844349,
            87: 1650844349,
            88: 1650844349,
            89: 1650844349,
            90: 1650844349,
            91: 1650844349,
            92: 1650844349,
            93: 1650844349,
            94: 1650844349,
            95: 1650844349,
            96: 1650844349,
            97: 1650844349,
        }
        for book_row in scratch_db.get_all_rows("books"):
            book_row["book_created_datestamp"] = theo_book_created_datestamp_dict[book_row.row_id]
            book_row.sync()

    @staticmethod
    def write_timestamps_titles_table(scratch_db):
        """
        Update the timestamp columns of the books field to static values, freezing them after database rebuilds.
        :param scratch_db:
        :return:
        """
        theo_title_created_datestamp_dict = {
            1: "2022-05-09 18:28:52",
            2: "2022-05-09 18:28:52",
            3: "2022-05-09 18:28:52",
            4: "2022-05-09 18:28:52",
            5: "2022-05-09 18:28:52",
            6: "2022-05-09 18:28:52",
            7: "2022-05-09 18:28:52",
            8: "2022-05-09 18:28:52",
            9: "2022-05-09 18:28:52",
            10: "2022-05-09 18:28:52",
            11: "2022-05-09 18:28:52",
            12: "2022-05-09 18:28:52",
            13: "2022-05-09 18:28:52",
            14: "2022-05-09 18:28:52",
            15: "2022-05-09 18:28:52",
            16: "2022-05-09 18:28:52",
            17: "2022-05-09 18:28:52",
            18: "2022-05-09 18:28:52",
            19: "2022-05-09 18:28:52",
            20: "2022-05-09 18:28:52",
            21: "2022-05-09 18:28:52",
            22: "2022-05-09 18:28:52",
            23: "2022-05-09 18:28:52",
            24: "2022-05-09 18:28:52",
            25: "2022-05-09 18:28:52",
            26: "2022-05-09 18:28:52",
            27: "2022-05-09 18:28:52",
            28: "2022-05-09 18:28:52",
            29: "2022-05-09 18:28:52",
            30: "2022-05-09 18:28:52",
            31: "2022-05-09 18:28:52",
            32: "2022-05-09 18:28:52",
            33: "2022-05-09 18:28:52",
            34: "2022-05-09 18:28:52",
            35: "2022-05-09 18:28:52",
            36: "2022-05-09 18:28:52",
            37: "2022-05-09 18:28:52",
            38: "2022-05-09 18:28:52",
            39: "2022-05-09 18:28:52",
            40: "2022-05-09 18:28:52",
            41: "2022-05-09 18:28:52",
            42: "2022-05-09 18:28:52",
            43: "2022-05-09 18:28:52",
            44: "2022-05-09 18:28:52",
            45: "2022-05-09 18:28:52",
            46: "2022-05-09 18:28:52",
            47: "2022-05-09 18:28:52",
            48: "2022-05-09 18:28:52",
            49: "2022-05-09 18:28:52",
            50: "2022-05-09 18:28:52",
            51: "2022-05-09 18:28:52",
            52: "2022-05-09 18:28:52",
            53: "2022-05-09 18:28:52",
            54: "2022-05-09 18:28:52",
            55: "2022-05-09 18:28:52",
            56: "2022-05-09 18:28:52",
            57: "2022-05-09 18:28:52",
            58: "2022-05-09 18:28:52",
            59: "2022-05-09 18:28:52",
            60: "2022-05-09 18:28:52",
            61: "2022-05-09 18:28:52",
            62: "2022-05-09 18:28:52",
            63: "2022-05-09 18:28:52",
            64: "2022-05-09 18:28:52",
            65: "2022-05-09 18:28:52",
            66: "2022-05-09 18:28:52",
            67: "2022-05-09 18:28:52",
            68: "2022-05-09 18:28:52",
            69: "2022-05-09 18:28:52",
            70: "2022-05-09 18:28:52",
            71: "2022-05-09 18:28:52",
            72: "2022-05-09 18:28:52",
            73: "2022-05-09 18:28:52",
            74: "2022-05-09 18:28:52",
            75: "2022-05-09 18:28:52",
            76: "2022-05-09 18:28:52",
            77: "2022-05-09 18:28:52",
            78: "2022-05-09 18:28:52",
            79: "2022-05-09 18:28:52",
            80: "2022-05-09 18:28:52",
            81: "2022-05-09 18:28:52",
            82: "2022-05-09 18:28:52",
            83: "2022-05-09 18:28:52",
            84: "2022-05-09 18:28:52",
            85: "2022-05-09 18:28:52",
            86: "2022-05-09 18:28:52",
            87: "2022-05-09 18:28:52",
            88: "2022-05-09 18:28:52",
            89: "2022-05-09 18:28:52",
            90: "2022-05-09 18:28:52",
            91: "2022-05-09 18:28:52",
            92: "2022-05-09 18:28:52",
            93: "2022-05-09 18:28:52",
            94: "2022-05-09 18:28:52",
            95: "2022-05-09 18:28:52",
            96: "2022-05-09 18:28:52",
            97: "2022-05-09 18:28:52",
        }

        theo_title_last_modified_datestamp_dict = {
            1: "2022-05-09 18:28:52",
            2: "2022-05-09 18:28:52",
            3: "2022-05-09 18:28:52",
            4: "2022-05-09 18:28:52",
            5: "2022-05-09 18:28:52",
            6: "2022-05-09 18:28:52",
            7: "2022-05-09 18:28:52",
            8: "2022-05-09 18:28:52",
            9: "2022-05-09 18:28:52",
            10: "2022-05-09 18:28:52",
            11: "2022-05-09 18:28:52",
            12: "2022-05-09 18:28:52",
            13: "2022-05-09 18:28:52",
            14: "2022-05-09 18:28:52",
            15: "2022-05-09 18:28:52",
            16: "2022-05-09 18:28:52",
            17: "2022-05-09 18:28:52",
            18: "2022-05-09 18:28:52",
            19: "2022-05-09 18:28:52",
            20: "2022-05-09 18:28:52",
            21: "2022-05-09 18:28:52",
            22: "2022-05-09 18:28:52",
            23: "2022-05-09 18:28:52",
            24: "2022-05-09 18:28:52",
            25: "2022-05-09 18:28:52",
            26: "2022-05-09 18:28:52",
            27: "2022-05-09 18:28:52",
            28: "2022-05-09 18:28:52",
            29: "2022-05-09 18:28:52",
            30: "2022-05-09 18:28:52",
            31: "2022-05-09 18:28:52",
            32: "2022-05-09 18:28:52",
            33: "2022-05-09 18:28:52",
            34: "2022-05-09 18:28:52",
            35: "2022-05-09 18:28:52",
            36: "2022-05-09 18:28:52",
            37: "2022-05-09 18:28:52",
            38: "2022-05-09 18:28:52",
            39: "2022-05-09 18:28:52",
            40: "2022-05-09 18:28:52",
            41: "2022-05-09 18:28:52",
            42: "2022-05-09 18:28:52",
            43: "2022-05-09 18:28:52",
            44: "2022-05-09 18:28:52",
            45: "2022-05-09 18:28:52",
            46: "2022-05-09 18:28:52",
            47: "2022-05-09 18:28:52",
            48: "2022-05-09 18:28:52",
            49: "2022-05-09 18:28:52",
            50: "2022-05-09 18:28:52",
            51: "2022-05-09 18:28:52",
            52: "2022-05-09 18:28:52",
            53: "2022-05-09 18:28:52",
            54: "2022-05-09 18:28:52",
            55: "2022-05-09 18:28:52",
            56: "2022-05-09 18:28:52",
            57: "2022-05-09 18:28:52",
            58: "2022-05-09 18:28:52",
            59: "2022-05-09 18:28:52",
            60: "2022-05-09 18:28:52",
            61: "2022-05-09 18:28:52",
            62: "2022-05-09 18:28:52",
            63: "2022-05-09 18:28:52",
            64: "2022-05-09 18:28:52",
            65: "2022-05-09 18:28:52",
            66: "2022-05-09 18:28:52",
            67: "2022-05-09 18:28:52",
            68: "2022-05-09 18:28:52",
            69: "2022-05-09 18:28:52",
            70: "2022-05-09 18:28:52",
            71: "2022-05-09 18:28:52",
            72: "2022-05-09 18:28:52",
            73: "2022-05-09 18:28:52",
            74: "2022-05-09 18:28:52",
            75: "2022-05-09 18:28:52",
            76: "2022-05-09 18:28:52",
            77: "2022-05-09 18:28:52",
            78: "2022-05-09 18:28:52",
            79: "2022-05-09 18:28:52",
            80: "2022-05-09 18:28:52",
            81: "2022-05-09 18:28:52",
            82: "2022-05-09 18:28:52",
            83: "2022-05-09 18:28:52",
            84: "2022-05-09 18:28:52",
            85: "2022-05-09 18:28:52",
            86: "2022-05-09 18:28:52",
            87: "2022-05-09 18:28:52",
            88: "2022-05-09 18:28:52",
            89: "2022-05-09 18:28:52",
            90: "2022-05-09 18:28:52",
            91: "2022-05-09 18:28:52",
            92: "2022-05-09 18:28:52",
            93: "2022-05-09 18:28:52",
            94: "2022-05-09 18:28:52",
            95: "2022-05-09 18:28:52",
            96: "2022-05-09 18:28:52",
            97: "2022-05-09 18:28:52",
        }

        for title_row in scratch_db.get_all_rows("titles"):
            title_row["title_created_datestamp"] = theo_title_created_datestamp_dict[title_row.row_id]
            title_row["title_last_modified"] = theo_title_last_modified_datestamp_dict[title_row.row_id]
            title_row.sync()


# Todo: Add the capacity to dump the resulting files back to here
def build_test_db(
    dst_file_path,
    dump=False,
    plugin_name=None,
    new_db_uuid="auto",
    test_asset_version=None,
):
    """
    Constructs test db 1 - then adds the randomly generated file data to it.
    Should build in a repeatable, platform agnostic way.
    :param dst_file_path:
    :param dump:
    :return:
    """
    test_db_builder = TestDB3Builder(
        dst_file_path=dst_file_path,
        csv_folder_path=__folder__,
        dump=dump,
        new_db_uuid=new_db_uuid,
        plugin_name=plugin_name,
        test_asset_version=test_asset_version,
    )
    test_db_builder.run()


def make_file_test_data(test_db, clear=True, extensions=None):
    """
    Write file test data to the target db - in a repeatable manner - if the database has the same number of books each
    time it should end up with the test data each time.
    :param test_db:
    :param clear: The relevant tables will be cleared before anything is written to them
    :param extensions: Overriding iterable of extensions to assign to the files. If None is passed will default to
                       cycling over ["epub", "mobi", "pdf"]
    :return:
    """
    if clear:
        # Clear the decks - erase book_folder_links, folders, file_folder_links, files
        test_db.driver_wrapper.clear("book_folder_links")
        test_db.driver_wrapper.clear("folders")
        test_db.driver_wrapper.clear("file_folder_links")
        test_db.driver_wrapper.clear("files")

    rand_ints = [
        5,
        8,
        5,
        2,
        6,
        1,
        6,
        7,
        5,
        2,
        4,
        4,
        5,
        4,
        6,
        9,
        9,
        1,
        3,
        1,
        9,
        3,
        9,
        6,
        1,
        4,
        7,
        4,
        9,
        1,
        5,
        7,
        8,
        1,
        8,
        8,
        1,
        5,
        6,
        4,
        7,
        4,
        9,
        3,
        6,
        2,
        5,
        2,
        3,
        8,
        8,
        1,
        9,
        3,
        6,
        7,
        3,
        1,
        8,
        7,
        9,
        9,
        7,
        4,
        8,
        6,
        2,
        1,
        3,
        1,
        2,
        1,
        9,
        7,
        5,
        4,
        4,
        8,
        4,
        1,
        5,
        7,
        1,
        2,
        6,
        1,
        5,
        1,
        9,
        6,
        9,
        6,
        4,
        6,
        7,
        7,
        3,
        1,
        7,
        8,
        9,
        7,
        3,
        7,
        5,
        5,
        7,
        7,
        1,
        1,
        3,
        3,
        9,
        8,
        7,
        4,
        3,
        9,
        3,
        2,
        2,
        6,
        9,
        5,
        6,
        5,
        8,
        3,
        8,
        6,
        4,
        6,
        7,
        3,
        2,
        6,
        1,
        7,
        6,
        2,
        5,
        1,
        7,
        6,
        4,
        8,
        5,
        5,
        1,
        6,
        3,
        9,
        2,
        1,
        7,
        4,
        7,
        6,
        6,
        2,
        2,
        1,
        4,
        2,
        3,
        5,
        3,
        3,
        4,
        5,
        7,
        3,
        3,
        9,
        8,
        9,
        2,
        5,
        6,
        9,
        2,
        4,
        8,
        5,
        8,
        4,
        9,
        9,
        4,
        3,
        3,
        5,
        2,
        5,
        6,
        4,
        4,
        2,
        3,
        8,
    ]

    rand_iter = cycle(iter(rand_ints))

    if extensions is None:
        extensions = ["epub", "mobi", "pdf"]
    ext_iter = cycle(iter(extensions))

    # Add a random number of books to each title
    for book_row in test_db.get_all_rows("books"):

        # Create and associate a number of folders with every book
        folder_rows = []

        folder_count = rand_iter.next()
        for i in range(folder_count):
            folder_row = Row(database=test_db)
            folder_row["folder_scratch"] = "DELETE ME"
            folder_rows.append(folder_row)

        puts(colored.green("{} folders created for book {}".format(folder_count, book_row["book_id"])))

        for fr in folder_rows:

            fr.sync()
            test_db.interlink_rows(primary_row=book_row, secondary_row=fr, priority="highest")

            # Create an associate a number of files with the
            file_count = rand_iter.next()
            for i in range(file_count):

                file_row = Row(database=test_db)
                file_row["file_base_folder"] = fr["folder_id"]
                file_row["file_size"] = 1234
                file_row["file_extension"] = ext_iter.next()
                file_row.sync()
                test_db.interlink_rows(primary_row=fr, secondary_row=file_row, priority="highest")

            puts(colored.green("{} - files created".format(file_count)))
