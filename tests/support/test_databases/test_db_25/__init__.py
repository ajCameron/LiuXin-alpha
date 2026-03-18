from __future__ import print_function


from ..test_db_23 import TestDB23Builer

from utils.lx_libraries.liuxin_random import LiuXinBadPseudoRandomGenerator


class TestDB25Builder(TestDB23Builer):
    def add_new_main_tables(self, scatch_db):
        pass

    def _populate_custom_column_6(self, scratch_db):
        """
        Populate an int, non-multiple custom column.
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(36435626)

        # Custom column 4

        # This is a non-multiple text (tag_ type column - so create a hundred entries and either link the book to or
        # not
        target_id_range = []
        for cc_val_i in range(1, 101):

            scratch_db.macros.add_cc_table_value(
                table="custom_column_6",
                value="test cc6 {} - {}" "".format(cc_val_i, self.get_random_uuid(current_rng=lx_random)),
            )
            target_id_range.append(cc_val_i)
            print("Generating cc6 value for {}".format(cc_val_i))

        for book_row in scratch_db.get_all_rows("books"):

            book_cc_i_status = lx_random.randint(0, 1)
            if book_row.row_id in [1, 4]:
                book_cc_i_status = 1

            if book_cc_i_status:
                # Create the new custom column entry

                cc_i_id = lx_random.choice(target_id_range)

                # Link it back to the given title
                scratch_db.macros.add_cc_link_with_extra(
                    lt="books_custom_column_6_link",
                    book_id=book_row.row_id,
                    value_id=cc_i_id,
                )

                print("custom_column_6 entry added to book {}".format(book_row.row_id))

    def _populate_custom_column_8(self, scratch_db):
        """
        Populate an int, non-multiple custom column.
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(36435626)

        # Custom column 4

        # This is a non-multiple text (tag_ type column - so create a hundred entries and either link the book to or
        # not
        target_id_range = []
        for cc_val_i in range(1, 101):

            scratch_db.macros.add_cc_table_value(
                table="custom_column_8",
                value="test cc8 {} - {}" "".format(cc_val_i, self.get_random_uuid(current_rng=lx_random)),
            )
            target_id_range.append(cc_val_i)
            print("Generating cc8 value for {}".format(cc_val_i))

        for book_row in scratch_db.get_all_rows("books"):

            book_cc_i_status = lx_random.randint(0, 1)
            if book_row.row_id in [1, 4]:
                book_cc_i_status = 1

            if book_cc_i_status:
                # Create the new custom column entry

                cc_i_id = lx_random.choice(target_id_range)

                # Link it back to the given title
                scratch_db.macros.add_cc_link_with_extra(
                    lt="books_custom_column_8_link",
                    book_id=book_row.row_id,
                    value_id=cc_i_id,
                )

                print("custom_column_8 entry added to book {}".format(book_row.row_id))

    # ------------------------------------------------------------------------------------------------------------------
    #
    # - KILL THE FEATURES THAT WE'RE NOT USING
    def populate_interlink_tables(self, scratch_db, test_lib):
        """
        Populate the interlink tables.
        :param scratch_db:
        :return:
        """
        pass

    def populate_intralink_tables(self, scratch_db):
        """
        Populate the intralink tables
        :param scratch_db:
        :return:
        """
        pass

    def generate_fake_asset_data(self, test_db):
        """
        Populate the database with fake asset data.
        :param test_db:
        :return:
        """
        pass

    #
    # ------------------------------------------------------------------------------------------------------------------


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
    test_db_builder = TestDB25Builder(
        dst_file_path=dst_file_path,
        csv_folder_path=None,
        dump=dump,
        plugin_name=plugin_name,
        comment_count=2,
        creator_count=10,
        genre_count=2,
        language_count=2,
        publisher_count=2,
        series_count=2,
        subject_count=2,
        tag_count=2,
        title_count=300,
        folder_store_count=10,
        generate_trees=False,
        comment_creator_max=0,
        comment_series_max=0,
        comment_title_max=0,
        creator_note_max=0,
        creator_series_max=0,
        creator_synopsis_max=0,
        creator_tag_max=0,
        creator_title_max=0,
        genre_series_max=0,
        genre_title_max=0,
        identifier_title_max=0,
        language_title_contained_max=0,
        language_title_available_max=0,
        note_publisher_max=0,
        note_series_max=5,
        note_title_max=0,
        publisher_title_max=0,
        rating_title_max=0,
        series_synopsis_max=0,
        series_tag_max=0,
        series_title_max=0,
        subject_title_max=0,
        tag_title_max=0,
        synopsis_title_max=0,
    )
    test_db_builder.run()
