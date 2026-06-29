from ..test_db_19 import TestDB19Builder


class TestDB21Builer(TestDB19Builder):
    """
    Preforms build for test db 21 - which has a table almost identical to series - but with a different name.
    """

    def add_new_main_tables(self, scatch_db):

        self.build_not_series_table(scatch_db)

        super(TestDB21Builer, self).add_new_main_tables(scatch_db)

    # Todo: Merge this over into the generic methods - and give them the option to generate indices as well
    def build_not_series_table(
        self,
        scratch_db,
        linked_to="titles",
        add_mode="all",
        add_limit=5,
        count_multiplier=10,
        left_crosslink=True,
        right_crosslink=True,
    ):
        """
        Build the "not_series" table - which has all the properties of the series table, but is not (intended to bypass
        the custom stuff existing for the series field).
        Many elements in :param linked_to: can be linked to up to one element in the new table that will be generated.
        Elements from :param linked_to: will be linked to elements in the newly created table with the mode
        :param add_mode: - default is "all" where every element in the new table (control element count in the new table
        with :param other_table_count:) will be linked to between 0 and :param add_limit: elements from the
        :param linked_to: table.

        :param scratch_db: The database to operate on
        :param linked_to: The table which the new table will be linked to
        :param add_mode: Mode to add links between the two table
        :param other_table_count: How many elements should be generated in the other table
        :param add_limit: Maximum number of links between the old table and the new one
        :param count_multiplier: Entries equal to this times the total number of entries on the linked to table will be
                                 produced (e.g. if this is linked to the titles table - and this is set to 10 - 10 * the
                                 number of entries on the titles table will be produced
        :param left_crosslink:
        :param right_crosslink:
        :return:
        """
        from utils.lx_libraries.liuxin_random import LiuXinBadPseudoRandomGenerator

        lx_random = LiuXinBadPseudoRandomGenerator(seed=19471948194646)
        self.generation_preflight(
            rng=lx_random,
            welcome="Building not_series table linked to {}".format(linked_to),
        )

        new_table_name = "not_series"
        new_table_column_name = scratch_db.driver_wrapper.driver.direct_get_column_name(new_table_name)

        # Construct the new main table
        scratch_db.driver_wrapper.driver.direct_create_main_table(table_name=new_table_name)

        # Link the new main table back to the linked_to table - with a one to one relation
        scratch_db.driver_wrapper.driver.direct_link_main_tables(
            primary_table=linked_to,
            secondary_table=new_table_name,
            link_type="many_many",
            requested_cols={"index", "priority"},
        )

        # Building one element in the new table for every element in the linked_to table
        custom_table_row_count = scratch_db.driver_wrapper.get_record_count(linked_to) * count_multiplier
        for i in range(1, custom_table_row_count + 1):

            new_table_blank_row = scratch_db.get_blank_row(new_table_name)
            new_table_blank_row[new_table_column_name] = "{} - ROW {} - {}" "".format(
                new_table_name, i, self.get_random_uuid(current_rng=lx_random)
            )
            new_table_blank_row.sync()

        not_series_title_link_pairs = set()

        if right_crosslink:

            new_table_count = scratch_db.driver_wrapper.get_record_count(new_table_name)

            # Linking elements from the new table to the old one according to the link mode
            if add_mode == "all":

                # For all the rows in the old table - link them to rows in the new table with a number of links chosen to
                # be between 0 and add_limit
                for linked_to_row in scratch_db.get_all_rows(table=linked_to, iterator_return=False):

                    other_row_count = lx_random.randint(0, add_limit)

                    for i in range(other_row_count):

                        # Needs to be done here as, to maintain legacy compatibility, we need to preserve the order
                        # random calls are made in
                        proposed_index = lx_random.choice(range(-100, 100))

                        random_new_row_id = lx_random.randint(1, new_table_count)
                        random_new_row = scratch_db.get_row_from_id(new_table_name, random_new_row_id)

                        ns_t_pair = (random_new_row_id, linked_to_row.row_id)
                        if ns_t_pair in not_series_title_link_pairs:
                            continue
                        not_series_title_link_pairs.add(ns_t_pair)

                        scratch_db.interlink_rows(
                            primary_row=random_new_row,
                            secondary_row=linked_to_row,
                            index=proposed_index,
                        )

            else:

                raise NotImplementedError("Add mode not recognized")

        if left_crosslink:

            # Linking tables from the old table to the new one - according to the link mode
            if add_mode == "all":

                linked_to_row_count = scratch_db.driver_wrapper.get_record_count(linked_to)

                # For all the rows in the new table - link them to rows in the old table with a number of links chosen
                # to be between 0 and add_limit
                for new_table_row in scratch_db.get_all_rows(table=new_table_name, iterator_return=False):

                    other_row_count = lx_random.randint(0, add_limit)
                    for i in range(other_row_count):
                        random_old_row_id = lx_random.randint(1, linked_to_row_count)
                        random_old_row = scratch_db.get_row_from_id(linked_to, random_old_row_id)

                        ns_t_pair = (new_table_row.row_id, random_old_row_id)
                        if ns_t_pair in not_series_title_link_pairs:
                            continue
                        not_series_title_link_pairs.add(ns_t_pair)

                        scratch_db.interlink_rows(primary_row=random_old_row, secondary_row=new_table_row)

            else:

                raise NotImplementedError("Add mode not recognized")


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
    test_db_builder = TestDB21Builer(
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
