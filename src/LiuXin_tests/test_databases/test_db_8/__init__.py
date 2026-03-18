# Generates test_db_8 - as with 7 - just contains a different (though equally limited) set of titles

import os

from LiuXin_alpha.utils.libraries.liuxin_clint import puts, colored

from LiuXin_tests.test_databases import load_data
from LiuXin_tests.test_databases import TestDatabaseBuilder

from LiuXin_alpha.utils.file_ops.file_ops import checked_copy

__folder__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))


class TestDB8Builder(TestDatabaseBuilder):
    """
    Constructs test_db_8 - which is the regular test database - but without most of the titles (means it's faster to
    load when running basic tests on the cache).
    """

    def load_base_database(self):
        return load_data(folder_path=None, overwrite_db=False, base_data=False, load_from=None)

    @staticmethod
    def purge_tables(scratch_db):
        puts(colored.green("Purging asset rows - all will be removed"))
        scratch_db.driver_wrapper.clear("files")
        scratch_db.driver_wrapper.clear("folders")
        scratch_db.driver_wrapper.clear("covers")

    @staticmethod
    def detail_databases(scratch_db):
        title_count = scratch_db.driver_wrapper.get_record_count("titles")

        # Purge all the titles not in the approved titles set
        approved_title_set = {0, 86, 5, 6, 7, 18}

        puts(colored.green("Purging titles - titles in {} will be retained".format(approved_title_set)))
        for title_id in range(0, title_count + 1):
            if int(title_id) not in approved_title_set:
                title_row = scratch_db.get_row_from_id("titles", title_id)
                scratch_db.delete(row=title_row)


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
    test_db_builder = TestDB8Builder(
        dst_file_path=dst_file_path,
        csv_folder_path=None,
        dump=dump,
        new_db_uuid=new_db_uuid,
        plugin_name=plugin_name,
        test_asset_version=test_asset_version,
    )
    test_db_builder.run()
