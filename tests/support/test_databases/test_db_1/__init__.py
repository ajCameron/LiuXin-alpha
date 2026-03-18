import os

from .. import TestDatabaseBuilder

__folder__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
test_db_1_folder = __folder__


def build_test_db(
    dst_file_path,
    dump=False,
    plugin_name=None,
    new_db_uuid="auto",
    test_asset_version=None,
):
    """
    Construct the test database specified by this module.
    In this case an augmented base data set with slightly riched metadata.
    :param dst_file_path: The file to write the database to after it's been built.
    :param dump: HERE IGNORED
    :return:
    """
    test_db_builder = TestDatabaseBuilder(
        dst_file_path=dst_file_path,
        csv_folder_path=__folder__,
        dump=dump,
        plugin_name=plugin_name,
        new_db_uuid=new_db_uuid,
        test_asset_version=test_asset_version,
    )
    test_db_builder.run()
