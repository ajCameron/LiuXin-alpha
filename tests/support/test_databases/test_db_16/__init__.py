from ..test_db_4 import TestDB4Builder


def build_test_db(
    dst_file_path,
    dump=False,
    plugin_name=None,
    new_db_uuid="auto",
    test_asset_version=None,
):
    """
    Construct the test database specified by this module.
    In this case a blank database is constructed and filled with data - before being copied into the test_databases
    folder.
    :param dst_file_path: The file to write the database to after it's been built.
    :param dump: HERE IGNORED
    :return:
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
