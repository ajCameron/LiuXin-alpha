from __future__ import print_function

from LiuXin_tests.test_databases.test_db_22 import TestDB22Builer

from utils.lx_libraries.liuxin_random import LiuXinBadPseudoRandomGenerator


class TestDB23Builer(TestDB22Builer):
    """
    Preforms build for test db 21 - which has a table almost identical to series - but with a different name.
    """

    def populate_custom_columns(self, scratch_db):
        """
        Gives a user the change to populate the custom columns when they have been created.
        :param scratch_db:
        :return:
        """
        self._populate_custom_column_1(scratch_db)
        self._populate_custom_column_2(scratch_db)
        self._populate_custom_column_3(scratch_db)
        self._populate_custom_column_4(scratch_db)
        self._populate_custom_column_5(scratch_db)
        self._populate_custom_column_6(scratch_db)

        self._populate_custom_column_8(scratch_db)
        self._populate_custom_column_9(scratch_db)
        self._populate_custom_column_10(scratch_db)
        self._populate_custom_column_11(scratch_db)
        self._populate_custom_column_12(scratch_db)
        self._populate_custom_column_13(scratch_db)
        self._populate_custom_column_14(scratch_db)

    def _populate_custom_column_1(self, scratch_db):
        """
        Populate the first custom column - a one to many - probably - of text type in the titles table.
        :param scratch_db:
        :return:
        """
        # Custom column 1
        lx_random = LiuXinBadPseudoRandomGenerator(36435626)
        cc_1_id = 0
        for title_row in scratch_db.get_all_rows("titles"):

            title_cc_1_count = lx_random.randint(1, 5)
            for current_cc_entry in range(1, title_cc_1_count + 1):

                # Create the new custom column entry
                scratch_db.macros.add_cc_table_value(
                    table="custom_column_1",
                    value="test titles cc {} - {}".format(title_row.row_id, current_cc_entry),
                )
                cc_1_id += 1

                # Link it back to the given title
                scratch_db.macros.add_cc_link_with_extra(
                    lt="titles_custom_column_1_link",
                    book_id=title_row.row_id,
                    value_id=cc_1_id,
                )

                print("{} custom_column_1 entries added to title {}".format(title_cc_1_count, title_row.row_id))

    def _populate_custom_column_2(self, scratch_db):
        """
        Populate a ratings, non-multiple custom column
        :param scratch_db:
        :return:
        """
        # cc 2 is a rating table linked to books - so populate the ratings table and then link all the books to it
        # - maybe once?
        lx_random = LiuXinBadPseudoRandomGenerator(5789235709857)

        # Populate the ratings table
        for i in range(1, 11):
            new_rating_str = "cc2_rating_{}".format(i)
            scratch_db.macros.add_cc_table_value(table="custom_column_2", value=new_rating_str)

        # For every book either give it a rating or not
        for book_row in scratch_db.get_all_rows("books"):

            row_status = lx_random.randint(0, 1)

            if row_status:

                target_book_id = book_row.row_id
                target_cc_rating = lx_random.randint(1, 10)

                try:
                    scratch_db.macros.add_cc_link_with_extra(
                        lt="books_custom_column_2_link",
                        book_id=target_book_id,
                        value_id=target_cc_rating,
                    )
                except:
                    err_msg = [
                        "Error while trying to add cc_2 values to a cc",
                        "target_book_id: {}".format(target_book_id),
                        "target_cc_rating: {}".format(target_cc_rating),
                    ]
                    raise Exception("\n".join(err_msg))

                print(
                    "rating {} was added to books custom_column_2 for book {}"
                    "".format(target_cc_rating, book_row.row_id)
                )

            else:
                print("No rating was added to book {}".format(book_row.row_id))

    def _populate_custom_column_3(self, scratch_db):
        """
        Populate an int, non-multiple custom column.
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(57892357)

        # For every book either give it an int or not
        for book_row in scratch_db.get_all_rows("books"):

            row_status = lx_random.randint(0, 1)
            row_value = lx_random.randint(-3468, 5000)

            if row_status:
                scratch_db.macros.add_cc_link_with_extra(
                    lt="custom_column_3", book_id=book_row.row_id, value_id=row_value
                )
                print("cc3 value was added to books custom_column_2 for book {}" "".format(book_row.row_id))
            else:
                print("No rating was added to book {}".format(book_row.row_id))

    def _populate_custom_column_4(self, scratch_db):
        """
        Populate an int, non-multiple custom column.
        :return:
        """
        # Custom column 4
        lx_random = LiuXinBadPseudoRandomGenerator(36435626)

        cc_1_id = 0
        for book_row in scratch_db.get_all_rows("books"):

            book_cc_1_status = lx_random.randint(0, 1)
            if book_cc_1_status:

                # Create the new custom column entry
                scratch_db.macros.add_cc_table_value(
                    table="custom_column_4",
                    value="test books cc4 {} - {}".format(book_row.row_id, self.get_random_uuid(current_rng=lx_random)),
                )
                cc_1_id += 1

                # Link it back to the given title
                scratch_db.macros.add_cc_link_with_extra(
                    lt="books_custom_column_4_link",
                    book_id=book_row.row_id,
                    value_id=cc_1_id,
                )

                print("custom_column_4 entry added to book {}".format(book_row.row_id))

    def _populate_custom_column_5(self, scratch_db):
        """
        Populate an int, non-multiple custom column.
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(57892357)

        # For every book either give it an int or not
        for book_row in scratch_db.get_all_rows("books"):

            row_status = lx_random.randint(0, 1)
            row_value = "cc5 value for book {} - {}" "".format(
                book_row.row_id, self.get_random_uuid(current_rng=lx_random)
            )

            if row_status:
                scratch_db.macros.add_cc_link_with_extra(
                    lt="custom_column_5", book_id=book_row.row_id, value_id=row_value
                )
                print("cc3 value was added to books custom_column_5 for book {}" "".format(book_row.row_id))
            else:
                print("No rating was added to book {}".format(book_row.row_id))

    def _populate_custom_column_6(self, scratch_db):
        """
        Populate a ratings, non-multiple custom column
        :param scratch_db:
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(5789235709857)

        # cc 6 is a series table linked to books - so populate the series table and then link all the books to it
        # - maybe once?

        # Populate the ratings table
        for i in range(1, 11):
            new_rating_str = "cc6_series_{}".format(i)
            scratch_db.macros.add_cc_table_value(table="custom_column_6", value=new_rating_str)

        # For every book either give it a rating or not
        for book_row in scratch_db.get_all_rows("books"):

            row_status = lx_random.randint(0, 1)

            if row_status:
                target_cc_rating = lx_random.randint(1, 10)  # randint and range have different ends...
                scratch_db.macros.add_cc_link_with_extra(
                    lt="books_custom_column_6_link",
                    book_id=book_row.row_id,
                    value_id=target_cc_rating,
                    extra="some extra for book {} - {}"
                    "".format(book_row.row_id, self.get_random_uuid(current_rng=lx_random)),
                )
                print(
                    "series {} was added to books custom_column_6 for book {}"
                    "".format(target_cc_rating, book_row.row_id)
                )
            else:
                print("No series was added to book {}".format(book_row.row_id))

    def _populate_custom_column_8(self, scratch_db):
        """
        Populate a float based custom column.
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(578923570985754674764)

        # cc 8 is a non-multiple float based table. So either give every book a float or don't

        # Populate the enumeration table
        for i in range(1, 11):
            new_rating_str = "cc8_enumeration_{}".format(i)
            scratch_db.macros.add_cc_table_value(table="custom_column_8", value=new_rating_str)

        # For every book either give it a rating or not
        for book_row in scratch_db.get_all_rows("books"):

            row_status = lx_random.randint(0, 1)

            if row_status:
                target_cc_rating = lx_random.randint(1, 10)
                scratch_db.macros.add_cc_link_with_extra(
                    lt="books_custom_column_8_link",
                    book_id=book_row.row_id,
                    value_id=target_cc_rating,
                    extra=None,
                )
                print(
                    "enumeration {} was added to books custom_column_8 for book {}"
                    "".format(target_cc_rating, book_row.row_id)
                )
            else:
                print("No enumeration was added to book {}".format(book_row.row_id))

    def _populate_custom_column_9(self, scratch_db):
        """
        Populate an int, non-multiple custom column.
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(57892357)

        # For every book either give it an int or not
        for book_row in scratch_db.get_all_rows("books"):

            row_status = lx_random.randint(0, 1)
            row_value = lx_random.random()

            if row_status:
                scratch_db.macros.add_cc_link_with_extra(
                    lt="custom_column_9", book_id=book_row.row_id, value_id=row_value
                )
                print("cc3 value was added to books custom_column_5 for book {}" "".format(book_row.row_id))
            else:
                print("No rating was added to book {}".format(book_row.row_id))

    def _populate_custom_column_10(self, scratch_db):
        """
        Populate an int, non-multiple custom column.
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(57892357)

        # For every book either give it an int or not
        for book_row in scratch_db.get_all_rows("books"):

            row_status = lx_random.randint(0, 1)

            # Need to build out the date string
            year_val = lx_random.randint(0, 4000)
            month_val = lx_random.randint(1, 12)
            day_val = lx_random.randint(1, 28)

            hour_val = lx_random.randint(0, 23)
            minute_val = lx_random.randint(0, 59)
            second_val = lx_random.randint(0, 59)

            row_value = "{}-{}-{} {}:{}:{}".format(year_val, month_val, day_val, hour_val, minute_val, second_val)

            if row_status:
                scratch_db.macros.add_cc_link_with_extra(
                    lt="custom_column_10", book_id=book_row.row_id, value_id=row_value
                )
                print("cc5 value was added to books custom_column_10 for book {}" "".format(book_row.row_id))
            else:
                print("No rating was added to book {}".format(book_row.row_id))

    def _populate_custom_column_11(self, scratch_db):
        """
        Populate an int, non-multiple custom column.
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(57892357)

        # For every book either give it an int or not
        for book_row in scratch_db.get_all_rows("books"):

            row_status = lx_random.randint(0, 1)
            row_value = bool(lx_random.randint(0, 1))

            if row_status:
                scratch_db.macros.add_cc_link_with_extra(
                    lt="custom_column_11", book_id=book_row.row_id, value_id=row_value
                )
                print("cc3 value was added to books custom_column_11 for book {}" "".format(book_row.row_id))
            else:
                print("No rating was added to book {}".format(book_row.row_id))

    def _populate_custom_column_12(self, scratch_db):
        """
        Populate a float based custom column.
        :return:
        """
        # cc 8 is a non-multiple float based table. So either give every book a float or don't
        lx_random = LiuXinBadPseudoRandomGenerator(578923570985754674764)

        # Populate the enumeration table
        for i in range(1, 11):
            new_rating_str = "cc12_rating_{}".format(i)
            scratch_db.macros.add_cc_table_value(table="custom_column_12", value=new_rating_str)

        # For every book either give it a rating or not
        for book_row in scratch_db.get_all_rows("books"):

            row_status = lx_random.randint(0, 1)

            if row_status:
                target_cc_rating = lx_random.randint(1, 10)
                scratch_db.macros.add_cc_link_with_extra(
                    lt="books_custom_column_12_link",
                    book_id=book_row.row_id,
                    value_id=target_cc_rating,
                    extra=None,
                )
                print(
                    "enumeration {} was added to books custom_column_12 for book {}"
                    "".format(target_cc_rating, book_row.row_id)
                )
            else:
                print("No enumeration was added to book {}".format(book_row.row_id))

    def _populate_custom_column_13(self, scratch_db):
        """
        Populate an int, non-multiple custom column.
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(57892357)

        # For every book either give it an int or not
        for book_row in scratch_db.get_all_rows("books"):

            row_status = lx_random.randint(0, 1)
            row_value = lx_random.randint(-50, 50)

            if row_status:
                scratch_db.macros.add_cc_link_with_extra(
                    lt="custom_column_13", book_id=book_row.row_id, value_id=row_value
                )
                print("int value was added to books custom_column_13 for book {}" "".format(book_row.row_id))
            else:
                print("No rating was added to book {}".format(book_row.row_id))

    def _populate_custom_column_14(self, scratch_db):
        """
        Populate a float based custom column.
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(578923570985754674764)

        # cc 8 is a non-multiple float based table. So either give every book a float or don't

        # Populate the enumeration table
        for i in range(1, 11):
            new_rating_str = "cc14_text_{}".format(i)
            scratch_db.macros.add_cc_table_value(table="custom_column_14", value=new_rating_str)

        # For every book either give it a rating or not
        for book_row in scratch_db.get_all_rows("books"):

            row_status = lx_random.randint(0, 1)

            if row_status:
                target_cc_rating = lx_random.randint(1, 10)
                scratch_db.macros.add_cc_link_with_extra(
                    lt="books_custom_column_14_link",
                    book_id=book_row.row_id,
                    value_id=target_cc_rating,
                    extra=None,
                )
                print(
                    "cc14 value {} was added to books custom_column_14 for book {}"
                    "".format(target_cc_rating, book_row.row_id)
                )
            else:
                print("No enumeration was added to book {}".format(book_row.row_id))

    def _populate_custom_column_15(self, scratch_db):
        """
        Populate an int, non-multiple custom column.
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(57892357)

        # For every book either give it an int or not
        for book_row in scratch_db.get_all_rows("books"):

            row_status = lx_random.randint(0, 1)
            row_value = "this is a test {}".format(self.get_random_uuid(current_rng=lx_random))

            if row_status:
                scratch_db.macros.add_cc_link_with_extra(
                    lt="custom_column_15", book_id=book_row.row_id, value_id=row_value
                )
                print("int value was added to books custom_column_15 for book {}" "".format(book_row.row_id))
            else:
                print("No rating was added to book {}".format(book_row.row_id))

    def _populate_custom_column_16(self, scratch_db):
        """
        Populate a float based custom column.
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(578923570985754674764)

        # cc 8 is a non-multiple float based table. So either give every book a float or don't

        # Populate the enumeration table
        for i in range(1, 11):
            new_rating_str = "cc16_series_{}".format(i)
            scratch_db.macros.add_cc_table_value(table="custom_column_16", value=new_rating_str)

        # For every book either give it a rating or not
        for book_row in scratch_db.get_all_rows("books"):

            row_status = lx_random.randint(0, 1)

            if row_status:
                target_cc_rating = lx_random.randint(1, 10)
                scratch_db.macros.add_cc_link_with_extra(
                    lt="books_custom_column_16_link",
                    book_id=book_row.row_id,
                    value_id=target_cc_rating,
                    extra=None,
                )
                print(
                    "cc16 value {} was added to books custom_column_14 for book {}"
                    "".format(target_cc_rating, book_row.row_id)
                )
            else:
                print("No enumeration was added to book {}".format(book_row.row_id))

    # cc 17 is skilled, because it's a composite custom column

    def _populate_custom_column_18(self, scratch_db):
        """
        Populate a float based custom column.
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(578923570985754674764)

        # cc 8 is a non-multiple float based table. So either give every book a float or don't

        # Populate the enumeration table
        for i in range(1, 11):
            new_rating_str = "cc18_enumeration_{}".format(i)
            scratch_db.macros.add_cc_table_value(table="custom_column_18", value=new_rating_str)

        # For every book either give it a rating or not
        for book_row in scratch_db.get_all_rows("books"):

            row_status = lx_random.randint(0, 1)

            if row_status:
                target_cc_rating = lx_random.randint(1, 10)
                scratch_db.macros.add_cc_link_with_extra(
                    lt="books_custom_column_18_link",
                    book_id=book_row.row_id,
                    value_id=target_cc_rating,
                    extra=None,
                )
                print(
                    "cc18 value {} was added to books custom_column_14 for book {}"
                    "".format(target_cc_rating, book_row.row_id)
                )
            else:
                print("No enumeration was added to book {}".format(book_row.row_id))

    def _populate_custom_column_19(self, scratch_db):
        """
        Populate an int, non-multiple custom column.
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(57892357)

        # For every book either give it an int or not
        for book_row in scratch_db.get_all_rows("books"):

            row_status = lx_random.randint(0, 1)
            row_value = lx_random.random() * lx_random.randint(-100, 100)

            if row_status:
                scratch_db.macros.add_cc_link_with_extra(
                    lt="custom_column_19", book_id=book_row.row_id, value_id=row_value
                )
                print("int value was added to books custom_column_19 for book {}" "".format(book_row.row_id))
            else:
                print("No rating was added to book {}".format(book_row.row_id))

    def _populate_custom_column_20(self, scratch_db):
        """
        Populate an int, non-multiple custom column.
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(57892357)

        # For every book either give it an int or not
        for book_row in scratch_db.get_all_rows("books"):

            row_status = lx_random.randint(0, 1)
            row_value = "fake datetime value - {}".format(self.get_random_uuid(current_rng=lx_random))

            if row_status:
                scratch_db.macros.add_cc_link_with_extra(
                    lt="custom_column_20", book_id=book_row.row_id, value_id=row_value
                )
                print("int value was added to books custom_column_20 for book {}" "".format(book_row.row_id))
            else:
                print("No rating was added to book {}".format(book_row.row_id))

    def _populate_custom_column_21(self, scratch_db):
        """
        Populate an int, non-multiple custom column.
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(5789232)

        # For every book either give it an int or not
        for book_row in scratch_db.get_all_rows("books"):

            row_status = lx_random.randint(0, 1)
            row_value = lx_random.randint(0, 1)

            if row_status:
                scratch_db.macros.add_cc_link_with_extra(
                    lt="custom_column_21", book_id=book_row.row_id, value_id=row_value
                )
                print("float value was added to books custom_column_21 for book {}" "".format(book_row.row_id))
            else:
                print("No rating was added to book {}".format(book_row.row_id))

    def _populate_custom_column_22(self, scratch_db):
        """
        Populate an int, non-multiple custom column.
        :return:
        """
        lx_random = LiuXinBadPseudoRandomGenerator(57892110)

        # For every book either give it an int or not
        for tag_row in scratch_db.get_all_rows("tags"):

            row_status = lx_random.randint(0, 1)
            row_value = lx_random.randint(0, 1)

            if row_status:
                scratch_db.macros.add_cc_link_with_extra(
                    lt="custom_column_22", book_id=tag_row.row_id, value_id=row_value
                )
                print("float value was added to books custom_column_22 for book {}" "".format(tag_row.row_id))
            else:
                print("No rating was added to book {}".format(tag_row.row_id))


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
    test_db_builder = TestDB23Builer(
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
