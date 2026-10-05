# test_db_13 - a completely blank database
# At least it should be easy to remember (lucky 13). Wish it was the case that this was intended.

"""
Build the deterministic test_db_13 database fixture and its declared content profile.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/databases/test_test_resources_manager.py
"""
import os

from LiuXin_alpha.utils.libraries.liuxin_clint import puts, colored

from .. import load_data
from .. import TestDatabaseBuilder
from .._legacy.tools import BasicMetadataFramework

__folder__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))


class TestDB13Builder(TestDatabaseBuilder):
    """
    Build test_db_13 - which is completely empty - because I forgot to make one earlier

    Example:
        Exercise TestDB13Builder through a consuming regression::

            python -m pytest -q tests/databases/test_test_resources_manager.py
    """

    def load_base_database(self):
        """
        Load the shared base schema and rows before profile-specific mutations.

        Example:
            Exercise TestDB13Builder.load base database through a consuming regression::

                python -m pytest -q tests/databases/test_test_resources_manager.py


        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        return load_data(folder_path=None, overwrite_db=False, base_data=False, load_from=None)

    @staticmethod
    def purge_tables(scratch_db):
        """
        Remove rows not required by the selected database profile.

        Example:
            Exercise TestDB13Builder.purge tables through a consuming regression::

                python -m pytest -q tests/databases/test_test_resources_manager.py


        :param scratch_db: Scratch database receiving deterministic schema and rows.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        for table in scratch_db.main_tables:
            scratch_db.driver_wrapper.clear(table)

    @staticmethod
    def detail_databases(scratch_db):
        """
        Return or record the database profiles supplied by this fixture module.

        Example:
            Exercise TestDB13Builder.detail databases through a consuming regression::

                python -m pytest -q tests/databases/test_test_resources_manager.py


        :param scratch_db: Scratch database receiving deterministic schema and rows.
        :return: The deterministic fixture value, path, bytes, record or collection
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

            python -m pytest -q tests/databases/test_test_resources_manager.py


    :param dst_file_path: Destination file written with the generated database or asset.
    :param dump: Value supplied for dump under the deterministic fixture contract.
    :param plugin_name: Value supplied for plugin name under the deterministic fixture
        contract.
    :param new_db_uuid: Value supplied for new db uuid under the deterministic fixture
        contract.
    :param test_asset_version: Value supplied for test asset version under the
        deterministic fixture contract.
    :return: None; fixture state or the supplied destination is updated in place.
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
