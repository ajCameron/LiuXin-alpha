from __future__ import print_function

from LiuXin_alpha.exceptions import DatabaseIntegrityError

from ..test_db_21 import TestDB21Builer


class TestDB22Builer(TestDB21Builer):
    """
    Preforms build for test db 21 - which has a table almost identical to series - but with a different name.
    """

    def add_new_main_tables(self, scatch_db):

        super(TestDB22Builer, self).add_new_main_tables(scatch_db)

    # Todo: Actually custom TABLES
    def add_custom_columns(self, scratch_db):

        test_titles_cc_1_name = "do_not_read"
        scratch_db.driver_wrapper.create_custom_column(in_table="titles", name=test_titles_cc_1_name)

        # Books custom columns
        # - is_multiple - False - all data
        book_cc_count = 2  # Started on 2, as we have already created a custom column above
        for cc_datatype in (
            "rating",
            "int",
            "text",
            "comments",
            "series",
            "composite",
            "enumeration",
            "float",
            "datetime",
            "bool",
        ):
            print(
                "About to create {} cc for books table - books cc {} - not multiple".format(cc_datatype, book_cc_count)
            )
            test_books_cc_name = "books_cc_{}_do_not_read_{}_not_multiple".format(book_cc_count, cc_datatype)
            scratch_db.driver_wrapper.create_custom_column(
                name=test_books_cc_name,
                datatype=cc_datatype,
                is_multiple=False,
                label=None,
                editable=True,
                display=None,
                in_table="books",
            )

            book_cc_count += 1

        # Books custom columns
        # - is_multiple - True - all data types
        for cc_datatype in (
            "rating",
            "int",
            "text",
            "comments",
            "series",
            "composite",
            "enumeration",
            "float",
            "datetime",
            "bool",
        ):
            print(
                "About to create {} cc for books table - books cc {} - multiple" "".format(book_cc_count, cc_datatype)
            )
            test_books_cc_name = "books_cc_{}_do_not_read_{}_multiple".format(book_cc_count, cc_datatype)
            try:
                scratch_db.driver_wrapper.create_custom_column(
                    name=test_books_cc_name,
                    datatype=cc_datatype,
                    is_multiple=True,
                    label=None,
                    editable=True,
                    display=None,
                    in_table="books",
                )
            except NotImplementedError:  # Done for reasons of legacy compatibility
                scratch_db.driver_wrapper.create_custom_column(
                    name=test_books_cc_name,
                    datatype=cc_datatype,
                    is_multiple=False,
                    label=None,
                    editable=True,
                    display=None,
                    in_table="books",
                )

            book_cc_count += 1

        test_tags_cc_1_name = "really_do_not_read"
        scratch_db.driver_wrapper.create_custom_column(in_table="tags", name=test_tags_cc_1_name)


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
    test_db_builder = TestDB22Builer(
        dst_file_path=dst_file_path,
        csv_folder_path=None,
        dump=dump,
        plugin_name=plugin_name,
        comment_count=10,
        creator_count=100,
        genre_count=10,
        language_count=10,
        publisher_count=10,
        series_count=10,
        subject_count=10,
        tag_count=10,
        title_count=10,
        folder_store_count=10,
        comment_creator_max=5,
        comment_series_max=5,
        comment_title_max=5,
        creator_note_max=5,
        creator_series_max=5,
        creator_synopsis_max=5,
        creator_tag_max=5,
        creator_title_max=5,
        genre_series_max=5,
        genre_title_max=5,
        identifier_title_max=5,
        language_title_contained_max=5,
        language_title_available_max=5,
        note_publisher_max=5,
        note_series_max=5,
        note_title_max=5,
        publisher_title_max=5,
        rating_title_max=5,
        series_synopsis_max=5,
        series_tag_max=5,
        series_title_max=5,
        subject_title_max=5,
        tag_title_max=5,
        synopsis_title_max=5,
    )
    test_db_builder.run()
