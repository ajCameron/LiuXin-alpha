# test_db_13 - a completely blank database
# At least it should be easy to remember (lucky 13). Wish it was the case that this was intended.

"""
Expose the supported test db 13 compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/support/test_databases/test_legacy_objects_smoke.py
"""
import os

from LiuXin_alpha.utils.libraries.liuxin_clint import puts, colored

from LiuXin_tests.test_databases import load_data
from LiuXin_tests.test_databases import TestDatabaseBuilder
from LiuXin_tests.test_utils.test_utils import BasicMetadataFramework

__folder__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))


class TestDB13Builder(TestDatabaseBuilder):
    """
    Build test_db_13 - which is completely empty - because I forgot to make one earlier

    Example:
        Exercise TestDB13Builder through a consuming regression::

            python -m pytest -q tests/support/test_databases/test_legacy_objects_smoke.py
    """

    def load_base_database(self):
        """
        Perform the load base database operation under explicit file-format and conversion rules.

        Example:
            Exercise TestDB13Builder.load base database through a consuming regression::

                python -m pytest -q tests/support/test_databases/test_legacy_objects_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return load_data(folder_path=None, overwrite_db=False, base_data=False, load_from=None)

    @staticmethod
    def purge_tables(scratch_db):
        """
        Perform the purge tables operation under explicit file-format and conversion rules.

        Example:
            Exercise TestDB13Builder.purge tables through a consuming regression::

                python -m pytest -q tests/support/test_databases/test_legacy_objects_smoke.py


        :param scratch_db: Value supplied for scratch db under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for table in scratch_db.main_tables:
            scratch_db.driver_wrapper.clear(table)

    @staticmethod
    def detail_databases(scratch_db):
        """
        Perform the detail databases operation under explicit file-format and conversion rules.

        Example:
            Exercise TestDB13Builder.detail databases through a consuming regression::

                python -m pytest -q tests/support/test_databases/test_legacy_objects_smoke.py


        :param scratch_db: Value supplied for scratch db under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
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
    test_db_13 - completely empty database.

    Example:
        Exercise build test db through a consuming regression::

            python -m pytest -q tests/support/test_databases/test_legacy_objects_smoke.py


    :param dst_file_path: Value supplied for dst file path under the utility contract.
    :param dump: Value supplied for dump under the utility contract.
    :param plugin_name: Value supplied for plugin name under the utility contract.
    :param new_db_uuid: Value supplied for new db uuid under the utility contract.
    :param test_asset_version: Value supplied for test asset version under the utility
        contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
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
