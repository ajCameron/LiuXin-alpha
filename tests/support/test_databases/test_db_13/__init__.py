# test_db_13 - a completely blank database
# At least it should be easy to remember (lucky 13). Wish it was the case that this was intended.

import os

from clint.textui import puts, colored

from LiuXin_tests.test_databases import load_data
from LiuXin_tests.test_databases import TestDatabaseBuilder
from LiuXin_tests.test_utils.test_utils import BasicMetadataFramework

__folder__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))


class TestDB13Builder(TestDatabaseBuilder):
    """
    Build test_db_13 - which is completely empty - because I forgot to make one earlier
    """

    def load_base_database(self):
        return load_data(folder_path=None, overwrite_db=False, base_data=False, load_from=None)

    @staticmethod
    def purge_tables(scratch_db):
        for table in scratch_db.main_tables:
            scratch_db.driver_wrapper.clear(table)

    @staticmethod
    def detail_databases(scratch_db):
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
    test_db_13 - completely empty database.
    :param dst_file_path: Place to copy the database file to after it's been built
    :param dump: If True then the csv files compromising this database will be written into the folder where this
                 script is running.
    :return:
    """
    test_db_builder = TestDB13Builder(
        dst_file_path=dst_file_path,
        csv_folder_path=None,
        dump=dump,
        new_db_uuid=new_db_uuid,
        plugin_name=plugin_name,
        test_asset_version=test_asset_version,
    )
    test_db_builder.run()
