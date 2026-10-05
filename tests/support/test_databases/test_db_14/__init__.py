# Generates test_db_14 - Only has the first 10 title rows (possibly including 0)

"""
Build the deterministic test_db_14 database fixture and its declared content profile.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/databases/test_test_resources_manager.py
"""
from LiuXin_alpha.utils.libraries.liuxin_clint import puts, colored

from ..test_db_1 import test_db_1_folder as __folder__
from .. import TestDatabaseBuilder


class TestDB2Builder(TestDatabaseBuilder):
    """
    Executes build for the test database described here.

    Example:
        Exercise TestDB2Builder through a consuming regression::

            python -m pytest -q tests/databases/test_test_resources_manager.py
    """

    @staticmethod
    def detail_databases(scratch_db):
        """
        Delete all but the first title (and title 0 - if it exists).

        Example:
            Exercise TestDB2Builder.detail databases through a consuming regression::

                python -m pytest -q tests/databases/test_test_resources_manager.py


        :param scratch_db: Scratch database receiving deterministic schema and rows.
        :return: The deterministic fixture value, path, bytes, record or collection
            described above.
        """
        title_count = scratch_db.driver_wrapper.get_record_count("titles")

        # Purge the titles of all but title 0 and titles 1-10
        puts(colored.green("Purging titles - all but the first 10 (and, if present, 0) will be removed"))
        for title_id in range(10, title_count + 1):
            title_row = scratch_db.get_row_from_id("titles", title_id)
            scratch_db.delete(row=title_row)

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
    test_db_builder = TestDB2Builder(
        dst_file_path=dst_file_path,
        csv_folder_path=__folder__,
        dump=dump,
        new_db_uuid=new_db_uuid,
        plugin_name=plugin_name,
        test_asset_version=test_asset_version,
    )
    test_db_builder.run()
