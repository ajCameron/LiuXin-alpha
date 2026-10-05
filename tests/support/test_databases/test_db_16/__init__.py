"""
Build the deterministic test_db_16 database fixture and its declared content profile.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/databases/test_test_resources_manager.py
"""
from ..test_db_4 import TestDB4Builder


def build_test_db(
    dst_file_path,
    dump=False,
    plugin_name=None,
    new_db_uuid="auto",
    test_asset_version=None,
):
    """
    Construct the test database specified by this module. In this case a blank database is constructed and filled with data - before being copied into the test_databases folder.

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
    test_db_builder = TestDB4Builder(
        dst_file_path=dst_file_path,
        csv_folder_path=None,
        dump=dump,
        plugin_name=plugin_name,
        comment_count=1,
        creator_count=1,
        genre_count=1,
        language_count=1,
        publisher_count=1,
        series_count=1,
        subject_count=1,
        tag_count=1,
        title_count=1,
        folder_store_count=1,
        creator_note_max=1,
        creator_series_max=1,
        creator_synopsis_max=1,
        creator_tag_max=1,
        creator_title_max=1,
        genre_series_max=1,
        genre_title_max=1,
        identifier_title_max=1,
        language_title_contained_max=1,
        language_title_available_max=1,
        note_publisher_max=1,
        note_series_max=1,
        note_title_max=1,
        publisher_title_max=1,
        rating_title_max=1,
        series_synopsis_max=1,
        series_tag_max=1,
        series_title_max=1,
        subject_title_max=1,
        tag_title_max=1,
        synopsis_title_max=1,
    )
    test_db_builder.run()
