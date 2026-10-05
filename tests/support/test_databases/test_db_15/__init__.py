# Generates test_db_15 - restricted title count - only three titles

"""
Build the deterministic test_db_15 database fixture and its declared content profile.

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

__folder__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))


class TestDB9Builder(TestDatabaseBuilder):
    """
    Constructs test_db_8 - which is the regular test database - but without most of the titles (means it's faster to load when running basic tests on the cache).

    Example:
        Exercise TestDB9Builder through a consuming regression::

            python -m pytest -q tests/databases/test_test_resources_manager.py
    """

    def load_base_database(self):
        """
        Load the shared base schema and rows before profile-specific mutations.

        Example:
            Exercise TestDB9Builder.load base database through a consuming regression::

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
            Exercise TestDB9Builder.purge tables through a consuming regression::

                python -m pytest -q tests/databases/test_test_resources_manager.py


        :param scratch_db: Scratch database receiving deterministic schema and rows.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        puts(colored.green("Purging asset rows - all will be removed"))
        scratch_db.driver_wrapper.clear("files")
        scratch_db.driver_wrapper.clear("folders")
        scratch_db.driver_wrapper.clear("covers")

    @staticmethod
    def detail_databases(scratch_db):
        """
        Return or record the database profiles supplied by this fixture module.

        Example:
            Exercise TestDB9Builder.detail databases through a consuming regression::

                python -m pytest -q tests/databases/test_test_resources_manager.py


        :param scratch_db: Scratch database receiving deterministic schema and rows.
        :return: None; fixture state or the supplied destination is updated in place.
        """
        title_count = scratch_db.driver_wrapper.get_record_count("titles")

        # Purge all the titles not in the approved titles set
        approved_title_set = {0, 1, 26, 36}

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
    test_db_builder = TestDB9Builder(
        dst_file_path=dst_file_path,
        csv_folder_path=None,
        dump=dump,
        new_db_uuid=new_db_uuid,
        plugin_name=plugin_name,
        test_asset_version=test_asset_version,
    )
    test_db_builder.run()
