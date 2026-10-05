# Generates test_db_11 - a database with complex series and some apparently valid asset data

"""
Build the deterministic test_db_11 database fixture and its declared content profile.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/databases/test_test_resources_manager.py
"""
import os

from LiuXin_alpha.utils.libraries.liuxin_clint import puts, colored

from .. import load_data
from ..test_db_10 import add_complex_series_to_db
from .. import TestDatabaseBuilder

__folder__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))


class TestDB11Builder(TestDatabaseBuilder):
    """
    Generates test_db_11 with complex series metadata.

    Example:
        Exercise TestDB11Builder through a consuming regression::

            python -m pytest -q tests/databases/test_test_resources_manager.py
    """

    def load_base_database(self):
        """
        Load the shared base schema and rows before profile-specific mutations.

        Example:
            Exercise TestDB11Builder.load base database through a consuming regression::

                python -m pytest -q tests/databases/test_test_resources_manager.py


        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        return load_data(folder_path=None, overwrite_db=False, base_data=False, load_from=None)

    @staticmethod
    def purge_tables(scratch_db):
        # There is some redundancy here
        # Clear the link tables as well - should be cleared when the tables are cleared - but might not be (faulty db?)
        # Want to make sure as to the final state.
        """
        Remove rows not required by the selected database profile.

        Example:
            Exercise TestDB11Builder.purge tables through a consuming regression::

                python -m pytest -q tests/databases/test_test_resources_manager.py


        :param scratch_db: Scratch database receiving deterministic schema and rows.
        :return: None; fixture state or the supplied destination is updated in place.
        """
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
        """
        Return or record the database profiles supplied by this fixture module.

        Example:
            Exercise TestDB11Builder.detail databases through a consuming regression::

                python -m pytest -q tests/databases/test_test_resources_manager.py


        :param scratch_db: Scratch database receiving deterministic schema and rows.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
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

        Example:
            Exercise TestDB11Builder.build valid asset data through a consuming regression::

                python -m pytest -q tests/databases/test_test_resources_manager.py


        :param scratch_db: Scratch database receiving deterministic schema and rows.
        :param fs_count: Value supplied for fs count under the deterministic fixture
            contract.
        :param book_folder_count: Value supplied for book folder count under the
            deterministic fixture contract.
        :param format_count: Value supplied for format count under the deterministic fixture
            contract.
        :param book_cover_count: Value supplied for book cover count under the deterministic
            fixture contract.
        :param creator_cover_count: Value supplied for creator cover count under the
            deterministic fixture contract.
        :param series_cover_count: Value supplied for series cover count under the
            deterministic fixture contract.
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
    test_db_2 is intended for applications where the whole database had to be read into the cache - as such size is at a premium in order to speed up the tests. The test database has one title - it's got all the metadata associated with that title - but it only has one title. This method constructs the test database - starting with a regular test database and removing everything except title 1 (and the unknown title - if it exists).

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
    test_db_builder = TestDB11Builder(
        dst_file_path=dst_file_path,
        csv_folder_path=None,
        dump=dump,
        new_db_uuid=new_db_uuid,
        plugin_name=plugin_name,
        test_asset_version=test_asset_version,
    )
    test_db_builder.run()
