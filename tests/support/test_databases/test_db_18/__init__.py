from LiuXin.exceptions import DatabaseIntegrityError

from LiuXin_tests.test_databases.test_db_4 import TestDB4Builder


class TestDB17Builder(TestDB4Builder):
    def add_new_main_tables(self, scatch_db):
        """
        Add custom columns to the database - linking a number of them to titles
        :param scatch_db: DatabasePing which is being detailed
        :return:
        """
        # ONE TO ONE
        # Build secondary_uuid custom table linked to titles
        self.build_new_main_table_with_one_one_links(
            scratch_db=scatch_db,
            linked_to="titles",
            new_table_name="secondary_uuids",
            add_mode="all",
        )

        #  Build books_secondary_uuid custom table linked to books
        self.build_new_main_table_with_one_one_links(
            scratch_db=scatch_db,
            linked_to="books",
            new_table_name="books_secondary_uuid",
            add_mode="all",
        )

        # MANY-ONE
        # Build loc_shelf_number custom table linked to books
        # (library of congress shelf number. Many books should be on a single shelf - so this should be MANY-ONE)
        self.build_new_main_table_with_many_one_links(
            scratch_db=scatch_db,
            linked_to="titles",
            new_table_name="loc_shelf_numbers",
            add_mode="all",
        )

        # Build a many to one for publishers - owned by (one company can own many publishers)
        self.build_new_main_table_with_many_one_links(
            scratch_db=scatch_db,
            linked_to="publishers",
            new_table_name="publisher_owners",
            add_mode="all",
        )

        # ONE-MANY
        # Build a one to many table for titles - one book can be linked to many of this other thing, but there cannot
        # be any overlap (the thing cannot be linked to more than one book)
        self.build_new_main_table_with_one_many_links(
            scratch_db=scatch_db,
            linked_to="titles",
            new_table_name="character_introductions",
            add_mode="all",
            restricted_types=["primary", "secondary", "incidental"],
        )
        #
        self.build_new_main_table_with_one_many_links(
            scratch_db=scatch_db,
            linked_to="series",
            new_table_name="series_character_introductions",
            add_mode="all",
        )

        # MANY-MANY
        # Build a years reprinted table - one book can be reprinted in many different years
        self.build_new_main_table_with_many_many_links(
            scratch_db=scatch_db,
            linked_to="books",
            new_table_name="year_first_published",
            add_mode="all",
        )

    def build_new_main_table_with_one_one_links(
        self, scratch_db, linked_to="titles", new_table_name=None, add_mode="all"
    ):
        """
        Build a new main table with a one to one link to the given table.
        Add entries to the new table with the mode specified by :param add_mode: - default is "all" for generating a
        new entry for every element of the table being linked to
        :param scratch_db: DatabasePing to operate on
        :param linked_to: The other main table to link the new one to
        :param new_table_name: An override name to specify the name of the new table manually - one will be
                               automatically generated if None is passed
        :param add_mode: How should the new table be populated in relation to the old
                         "all" - just adds entries for all the entries in the original table
        :return:
        """
        internal_rng = self.get_internal_rng(45682214)

        self.initialize_random_assets(rng=internal_rng)

        self.info("About to build {} linked to {}".format(new_table_name, linked_to))

        if new_table_name is None:
            new_table_name = "test_one_one_table_{}_{}".format(
                linked_to, self.get_random_uuid(current_rng=internal_rng, length=5)
            )
        new_table_column_name = scratch_db.driver_wrapper.driver.direct_get_column_name(new_table_name)
        linked_to_id_col = scratch_db.driver_wrapper.driver.direct_get_id_column(linked_to)

        # Construct the new main table
        scratch_db.driver_wrapper.driver.direct_create_new_main_table(table_name=new_table_name)

        # Link the new main table back to the linked_to table - with a one to one relation
        scratch_db.driver_wrapper.driver.direct_link_main_tables(
            primary_table=linked_to, secondary_table=new_table_name, link_type="one_one"
        )

        # Populate the new table with data in the specified mode
        if add_mode == "all":

            for linked_to_row in scratch_db.get_all_rows(table=linked_to, iterator_return=False):

                # Create a new row in the given table - we'lll be linking it to the row from the linked_to table
                new_table_row = scratch_db.get_blank_row(new_table_name)
                new_table_row[new_table_column_name] = "{} - {} - LINKED TO {} {}".format(
                    new_table_name,
                    self.get_random_uuid(current_rng=internal_rng),
                    linked_to,
                    linked_to_row[linked_to_id_col],
                )
                new_table_row.sync()

                scratch_db.interlink_rows(primary_row=new_table_row, secondary_row=linked_to_row)

        else:

            raise NotImplementedError("Add mode not currently supported")

    def build_new_main_table_with_one_many_links(
        self,
        scratch_db,
        linked_to="titles",
        new_table_name=None,
        add_mode="all",
        add_limit=5,
        restricted_types=None,
    ):
        """
        Build a new main table with a one to many link to the given table.
        One element in the linked_to table can be linked to many elements in the other table - or none.
        Add entries to the new table with the mode specified by :param add_mode: - default is "all" for generating a
        random number of elements (between 0 and :param add_limit:) for every element in the :param linked_to: table.
        :param scratch_db: DatabasePing to operate on
        :param linked_to: The other main table to link the new one to
        :param new_table_name: An override name to specify the name of the new table manually - one will be
                               automatically generated if None is passed
        :param add_mode: How should the new table be populated in relation to the old
                         "all" - just adds entries for all the entries in the original table
        :param add_limit: Specify the maximum number of elements in the new table which will be
        :param restricted_types: A restriction to the types which will be set for the links
        :return:
        """
        self.info("About to build {} linked to {}".format(new_table_name, linked_to))

        internal_rng = self.get_internal_rng(456823424)

        self.initialize_random_assets(rng=internal_rng)

        # Just generate and use a random table name
        if new_table_name is None:
            new_table_name = "test_one_many_table_{}_{}" "".format(
                linked_to, self.get_random_uuid(current_rng=internal_rng, length=5)
            )

        new_table_column_name = scratch_db.driver_wrapper.driver.direct_get_column_name(new_table_name)
        linked_to_id_col = scratch_db.driver_wrapper.driver.direct_get_id_column(linked_to)

        # Construct the new main table
        scratch_db.driver_wrapper.driver.direct_create_new_main_table(table_name=new_table_name)

        # Link the new main table back to the linked_to table - with a one to one relation
        scratch_db.driver_wrapper.driver.direct_link_main_tables(
            primary_table=linked_to,
            secondary_table=new_table_name,
            link_type="one_many",
        )

        # Populate the new table with data in the specified mode
        if add_mode == "all":

            for linked_to_row in scratch_db.get_all_rows(table=linked_to, iterator_return=False):

                # Determine the number of elements which will be linked to the single element
                element_count = self.get_randint(internal_rng, 0, add_limit)

                self.info("Linking {} elements to row {}".format(element_count, linked_to_row.row_id))

                for i in range(element_count):

                    # Create a new row in the given table - we'll be linking it to the row from the linked_to table
                    new_table_row = scratch_db.get_blank_row(new_table_name)

                    new_table_row[new_table_column_name] = "{} - {} - LINKED TO {} {} - ROW {}".format(
                        new_table_name,
                        self.get_random_uuid(current_rng=internal_rng),
                        linked_to,
                        linked_to_row[linked_to_id_col],
                        i,
                    )

                    new_table_row.sync()

                    if restricted_types is None:
                        scratch_db.interlink_rows(primary_row=new_table_row, secondary_row=linked_to_row)
                    else:
                        this_link_type = internal_rng.choice(restricted_types)
                        scratch_db.interlink_rows(
                            primary_row=new_table_row,
                            secondary_row=linked_to_row,
                            type=this_link_type,
                        )

        else:

            raise NotImplementedError

    def build_new_main_table_with_many_one_links(
        self,
        scratch_db,
        linked_to="titles",
        new_table_name=None,
        add_mode="all",
        other_table_count="same_as_linked_to",
        add_limit=5,
    ):
        """
        Build a new main table with a many to one link to the given table.

        e.g. Many items in the new table should be linked to a single element in the linked_to table.
        However any given item in the new table should only be linked to a single title

        Many elements in :param linked_to: can be linked to up to one element in the new table that will be generated.
        Elements from :param linked_to: will be linked to elements in the newly created table with the mode
        :param add_mode: - default is "all" where every element in the new table (control element count in the new table
        with :param other_table_count:) will be linked to between 0 and :param add_limit: elements from the
        :param linked_to: table.

        :param scratch_db: The database to operate on
        :param linked_to: The table which the new table will be linked to
        :param new_table_name: Override name for the new table that will be generated
        :param add_mode: Mode to add links between the two table
        :param other_table_count: How many elements should be generated in the other table
        :param add_limit: Maximum number of links between the old table and the new one
        :return:
        """
        self.info("About to build {} linked to {}".format(new_table_name, linked_to))

        internal_rng = self.get_internal_rng(456823424)

        self.initialize_random_assets(rng=internal_rng)

        if new_table_name is None:
            new_table_name = "test_many_one_table_{}_{}" "".format(
                linked_to, self.get_random_uuid(current_rng=internal_rng, length=5)
            )
        new_table_column_name = scratch_db.driver_wrapper.driver.direct_get_column_name(new_table_name)
        linked_to_id_col = scratch_db.driver_wrapper.driver.direct_get_id_column(linked_to)

        # Construct the new main table
        scratch_db.driver_wrapper.driver.direct_create_new_main_table(table_name=new_table_name)

        # Link the new main table back to the linked_to table - with a many to one relation between the new table and
        # the linked to table
        scratch_db.driver_wrapper.driver.direct_link_main_tables(
            primary_table=linked_to,
            secondary_table=new_table_name,
            link_type="one_many",
        )

        # Generating elements in the new table
        if other_table_count == "same_as_linked_to":

            # Building one element in the new table for every element in the linked_to table
            for i in range(1, scratch_db.driver_wrapper.get_record_count(linked_to) + 1):

                new_table_blank_row = scratch_db.get_blank_row(new_table_name)
                new_table_blank_row[new_table_column_name] = "{} - ROW {} - {}".format(
                    new_table_name, i, self.get_random_uuid(current_rng=internal_rng)
                )

        else:

            raise NotImplementedError("Given mode of generating elements in the new table is not supported")

        self.info("We got past this point without a massive memory leak - probably")

        new_table_row_count = scratch_db.driver_wrapper.get_record_count(new_table_name)

        used_new_table_ids = set()

        # Linking elements from the new table to the old one according to the link mode
        if add_mode == "all":

            # For all the rows in the old table - link them to rows in the new table with a number of links chosen to
            # be between 0 and add_limit
            for linked_to_row in scratch_db.get_all_rows(table=linked_to, iterator_return=False):

                other_row_count = internal_rng.randint(0, add_limit)
                for i in range(other_row_count):

                    random_new_row_id = internal_rng.randint(1, new_table_row_count)

                    if random_new_row_id in used_new_table_ids:
                        continue
                    used_new_table_ids.add(random_new_row_id)

                    random_new_row = scratch_db.get_row_from_id(new_table_name, random_new_row_id)
                    scratch_db.interlink_rows(primary_row=random_new_row, secondary_row=linked_to_row)

        else:

            raise NotImplementedError("Add mode not recognized")

    def build_new_main_table_with_many_many_links(
        self,
        scratch_db,
        linked_to="titles",
        new_table_name=None,
        add_mode="all",
        other_table_count="same_as_linked_to",
        add_limit=5,
    ):
        """
        Build a new main table with a many to one link to the given table.
        Many elements in :param linked_to: can be linked to up to one element in the new table that will be generated.
        Elements from :param linked_to: will be linked to elements in the newly created table with the mode
        :param add_mode: - default is "all" where every element in the new table (control element count in the new table
        with :param other_table_count:) will be linked to between 0 and :param add_limit: elements from the
        :param linked_to: table.

        :param scratch_db: The database to operate on
        :param linked_to: The table which the new table will be linked to
        :param new_table_name: Override name for the new table that will be generated
        :param add_mode: Mode to add links between the two table
        :param other_table_count: How many elements should be generated in the other table
        :param add_limit: Maximum number of links between the old table and the new one
        :return:
        """
        self.info("About to build {} linked to {}".format(new_table_name, linked_to))

        internal_rng = self.get_internal_rng(456823424)

        self.initialize_random_assets(rng=internal_rng)

        if new_table_name is None:
            new_table_name = "test_one_one_table_{}_{}" "".format(
                linked_to, self.get_random_uuid(current_rng=internal_rng, length=5)
            )
        new_table_column_name = scratch_db.driver_wrapper.driver.direct_get_column_name(new_table_name)
        linked_to_id_col = scratch_db.driver_wrapper.driver.direct_get_id_column(linked_to)

        # Construct the new main table
        scratch_db.driver_wrapper.driver.direct_create_new_main_table(table_name=new_table_name)

        # Link the new main table back to the linked_to table - with a one to one relation
        scratch_db.driver_wrapper.driver.direct_link_main_tables(
            primary_table=linked_to,
            secondary_table=new_table_name,
            link_type="one_many",
        )

        # Generating elements in the new table
        if other_table_count == "same_as_linked_to":

            # Building one element in the new table for every element in the linked_to table
            for i in range(1, scratch_db.driver_wrapper.get_record_count(linked_to) + 1):

                new_table_blank_row = scratch_db.get_blank_row(new_table_name)
                new_table_blank_row[new_table_column_name] = "{} - ROW {} - {}".format(
                    new_table_name, i, self.get_random_uuid(current_rng=internal_rng)
                )

        else:

            raise NotImplementedError("Given mode of generating elements in the new table is not supported")

        new_table_row_count = scratch_db.driver_wrapper.get_record_count(new_table_name)

        used_new_table_ids = set()

        # Linking elements from the new table to the old one according to the link mode
        if add_mode == "all":

            # For all the rows in the old table - link them to rows in the new table with a number of links chosen to
            # be between 0 and add_limit
            for linked_to_row in scratch_db.get_all_rows(table=linked_to, iterator_return=False):

                other_row_count = self.get_randint(internal_rng, 0, add_limit)
                for i in range(other_row_count):

                    random_new_row_id = internal_rng.randint(1, new_table_row_count)

                    if random_new_row_id in used_new_table_ids:
                        continue
                    used_new_table_ids.add(random_new_row_id)

                    random_new_row = scratch_db.get_row_from_id(table=new_table_name, row_id=random_new_row_id)

                    scratch_db.interlink_rows(primary_row=random_new_row, secondary_row=linked_to_row)

        else:

            raise NotImplementedError("Add mode not recognized")

        linked_to_row_count = scratch_db.driver_wrapper.get_record_count(linked_to)

        # Linking tables from the old table to the new one - according to the link mode
        if add_mode == "all":

            # For all the rows in the new table - link them to rows in the old table with a number of links chosen to
            # be between 0 and add_limit
            for new_table_row in scratch_db.get_all_rows(table=new_table_name, iterator_return=False):

                other_row_count = self.get_randint(internal_rng, 0, add_limit)
                for i in range(other_row_count):

                    linked_to_row_id = internal_rng.randint(1, linked_to_row_count)

                    random_old_row = scratch_db.get_row_from_id(table=linked_to, row_id=linked_to_row_id)

                    try:
                        scratch_db.interlink_rows(primary_row=random_old_row, secondary_row=new_table_row)
                    except DatabaseIntegrityError:
                        pass


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
    test_db_builder = TestDB17Builder(
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
