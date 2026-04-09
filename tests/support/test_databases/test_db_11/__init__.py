# Generates test_db_11 - a database with complex series and some apparently valid asset data

import os

from LiuXin_alpha.utils.libraries.liuxin_clint import puts, colored

from .. import load_data
from ..test_db_10 import add_complex_series_to_db
from .. import TestDatabaseBuilder

__folder__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))


class TestDB11Builder(TestDatabaseBuilder):
    """
    Generates test_db_11 with complex series metadata.

    Legacy folder_stores-backed asset generation has been retired. Current
    support DB provisioning supplies asset-heavy fixtures via synthetic
    FRBR-native builders in tests.support.test_resources_manager instead.
    """

    def load_base_database(self):
        return load_data(folder_path=None, overwrite_db=False, base_data=False, load_from=None)

    @staticmethod
    def purge_tables(scratch_db):
        # There is some redundancy here
        # Clear the link tables as well - should be cleared when the tables are cleared - but might not be (faulty db?)
        # Want to make sure as to the final state.
        puts(colored.green("Purging asset rows - all will be removed. Also removing links from the assets to md rows"))
        scratch_db.driver_wrapper.clear("files")
        scratch_db.driver_wrapper.clear("file_folder_links")
        scratch_db.driver_wrapper.clear("book_file_links")

        scratch_db.driver_wrapper.clear("folders")
        scratch_db.driver_wrapper.clear("book_folder_links")

        scratch_db.driver_wrapper.clear("covers")
        scratch_db.driver_wrapper.clear("book_cover_links")
        scratch_db.driver_wrapper.clear("cover_creator_links")
        scratch_db.driver_wrapper.clear("cover_series_links")

    def detail_databases(self, scratch_db):
        # Include the complex series data
        puts(colored.green("Adding complex series data"))
        scratch_db = add_complex_series_to_db(scratch_db)
        return scratch_db

    def build_valid_asset_data(
        self,
        scratch_db,
        fs_count=10,
        book_folder_count=100,
        format_count=400,
        book_cover_count=150,
        creator_cover_count=150,
        series_cover_count=150,
    ):
        """
        Legacy folder_stores-backed asset generation has been retired.
        """
        return scratch_db


# Todo: Add capacity to dump the data here after it's been built
def build_test_db(
    dst_file_path,
    dump=False,
    plugin_name=None,
    new_db_uuid="auto",
    test_asset_version=None,
):
    """
    test_db_2 is intended for applications where the whole database had to be read into the cache - as such size is at a
    premium in order to speed up the tests.
    The test database has one title - it's got all the metadata associated with that title - but it only has one title.
    This method constructs the test database - starting with a regular test database and removing everything except
    title 1 (and the unknown title - if it exists).
    :param dst_file_path: Place to copy the database file to after it's been built
    :param dump: If True then the csv files compromising this database will be written into the folder where this
                 script is running.
    :return:
    """
    test_db_builder = TestDB11Builder(
        dst_file_path=dst_file_path,
        csv_folder_path=None,
        dump=dump,
        new_db_uuid=new_db_uuid,
        plugin_name=plugin_name,
        test_asset_version=test_asset_version,
    )
    test_db_builder.run()
