# Make a test data set with additional files test data

import os
import shutil
import sys
from itertools import cycle

from LiuXin_alpha.utils.libraries.liuxin_clint import puts, colored

from LiuXin_alpha.paths import LiuXin_data_folder

from LiuXin_alpha.databases.database import Database
from LiuXin_alpha.databases.row import Row

from LiuXin_tests.test_databases import file_load_test_database_backup
from LiuXin_tests.test_databases import TestDatabaseBuilder


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
    test_db_builder = TestDBFileAndFolderBuilder(
        dst_file_path=dst_file_path,
        csv_folder_path=None,
        dump=dump,
        new_db_uuid=new_db_uuid,
        plugin_name=plugin_name,
        test_asset_version=test_asset_version,
    )
    test_db_builder.run()


class TestDBFileAndFolderBuilder(TestDatabaseBuilder):
    """
    Builds a test database with file and folder data baked in.
    """

    def load_base_database(self):
        # Load a full copy of the database with all the current test data into a scratch folder for additional work
        full_data_backup_path = file_load_test_database_backup(base=False, scratch=True)

        # Open the test database - write some test data
        scratch_db = Database(metadata={"database_path": full_data_backup_path})
        return scratch_db

    @staticmethod
    def detail_databases(scratch_db):

        # Write a test set of files and folder

        ################################################################################################################

        # Clear the tables that will have test data inserted into them
        scratch_db.driver_wrapper.clear("files")
        scratch_db.driver_wrapper.clear("folders")
        scratch_db.driver_wrapper.clear("file_folder_links")
        scratch_db.driver_wrapper.clear("book_folder_links")
        scratch_db.driver_wrapper.clear("book_file_links")

        # Test sizes for the file_size field
        from LiuXin_tests.test_constants import rand_size_ints

        rand_size_ints = cycle(iter(rand_size_ints))

        # Write some test data into the files and folders table
        from LiuXin_tests.test_constants import rand_ints

        rand_iter = cycle(iter(rand_ints))

        # The name cycling
        from LiuXin_tests.test_constants import rand_names_list

        rand_names_list = cycle(iter(rand_names_list))

        extensions = ["MOBI", "EPUB", "PDF"]
        extensions = cycle(iter(extensions))

        for book_row in scratch_db.get_all_rows("books"):

            # Create and associate a number of folders with every book
            folder_rows = []

            folder_count = rand_iter.next()
            for i in range(folder_count):
                folder_row = Row(database=scratch_db)
                folder_row["folder_scratch"] = "DELETE ME"
                folder_rows.append(folder_row)

            puts(colored.green("{} folders created for book {}".format(folder_count, book_row["book_id"])))

            for fr in folder_rows:

                fr.sync()
                scratch_db.interlink_rows(primary_row=book_row, secondary_row=fr)

                file_count = rand_iter.next()
                for i in range(file_count):

                    # Make a dummy file row
                    file_row = Row(database=scratch_db)
                    file_row["file_base_folder"] = fr["folder_id"]
                    file_row["file_size"] = rand_size_ints.next()
                    file_row["file_extension"] = extensions.next()
                    file_row["file_name"] = rand_names_list.next()
                    file_row.sync()

                    # Link the file row to the folder it should be in
                    scratch_db.interlink_rows(primary_row=fr, secondary_row=file_row, priority="not_set")

                    # Link the file row to the book that it should be in
                    scratch_db.interlink_rows(primary_row=file_row, secondary_row=book_row, priority="highest")

        ################################################################################################################
