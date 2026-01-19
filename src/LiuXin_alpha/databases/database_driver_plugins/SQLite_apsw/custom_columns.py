class SQLiteCustomColumnsDriverMixin(object):
    """
    Mixin which contains all the logic for the custom columns in one place.
    """

    # Todo: The code for this is currently over in macros. It should probably be here
    # Todo: When this method is called it needs to be noted in the custom columns table with details of the column created
    # Todo: Need to write a standardized way of doing custom columns - with sane naming
    # Then test how all these tables behave when confronted with a custom column
    # custom column tables "{original_table}_custom_column_{name}"
    # When deleting an entry in the main table should also take out all entries in the custom columns
    # (do this by modifying the link creation syntax so it supports the option of delete triggers)
    # Support all relation types
    def direct_create_custom_column(self, in_table, column_name, data_type="TEXT", multi=False):
        """
        Direct create a custom column in a given table.
        This column can have one or many values and will be included in the row return under the custom_fields
        attribute.
        Custom columns are stored in tables linked to the main table. You can tell the tables store custom columns
        because their names will start with "custom_column__"
        :param in_table: Table to create the custom column in. Can be "main", "interlink", "intralink" or "helper".
        :param column_name: Name for the custom column which will be created in the given table.
        :param data_type: Datatype for the new custom column

        :param multi: If False a one to one table will be created

                      If True multiple values will be allowed for each value in the original table.
                      The form that the multi parameters takes will determine the type of multiple relation formed
                      between the table and it's new column.

                      If the multi value is simply True then the relation will take it's default form - many_to_many

                      If multi = "one_many" then the relation will take a one_many form (one book can be linked to many
                      items - but no other book can be linked to that item.
                      Items are considered to be unique.

                      If multi = "many_one" then the relation will take a many_one form (many books can be linked to
                      one, and only one, item.
                      Items are considered to be unique.

        :return:
        """
        assert in_table != "custom_columns", "Cannot create custom column in custom_columns table"

        # Todo: In one_many type links - make sure that items are exclusive to books
        if not multi:
            return self.direct_create_one_to_one_custom_column(
                target_table=in_table,
                custom_column_name=column_name,
                datatype=data_type,
            )
        else:
            if multi == "many_many":
                # Need to create a many_many relation between the book and the values for it in the custom column
                self.direct_create_many_many_custom_column(target_table=in_table, custom_column_name=column_name)
            elif multi == "one_many":
                # Need to create a one_many relation between the book and it's items
                return self.direct_create_one_to_many_custom_column(
                    target_table=in_table,
                    custom_column_name=column_name,
                    datatype=data_type,
                )
            elif multi == "many_one":
                return self.direct_create_many_to_one_custom_column(
                    target_table=in_table, custom_column_name=column_name
                )
            else:
                raise NotImplementedError("form of multi not recognized")

    # Todo: Also need to mark all the link tables between the new custom columns and the tables they are rooted in as custom_column_links
    # Todo: Need a way of writing out to these - custom columns need to be actually integrated into the database
    # Todo: Needs to error out if the datatype is not a known SQLite one
    # Todo: Validate that the target table is a known table.
    # Todo: Front end to database class
    # Todo: Need to register all the custom columns created - make sure this isn't being confused with a main table on restart
    def direct_create_one_to_one_custom_column(
        self, target_table, custom_column_name, datatype="TEXT", normalized=False
    ):
        """
        Create a custom column with a one-one relation between entries in the custom column and the given target table.
        There can be, at most, one value for every value in the :param target_table:
        Removing a value from the :param target_table: will also remove a value from this table.

        :param target_table: The table which the custom column will be attached to
        :param custom_column_name: The name of the custom column which will be generated
        :param datatype: Datatype for the custom column
        :param normalized: If True then the one-one table will be through a link table (useful for something like
                           series - where you want multiple entries in the cc but only one should be linked to the
                           main entry at any one time
                           If False, there is no link table.

        :return custom_col_table: The name of the table holding the new custom column
        """
        # VALIDATE

        if normalized:

            # Create a table to hold the values

            raise NotImplementedError

        else:

            assert target_table != "custom_columns", "Cannot create custom column in custom_columns"

            # Check the components which are going to go into the table name
            assert self._validate_table_name(table_name=custom_column_name), "custom column name not valid"

            # MAIN TABLE

            target_table_id_col = self._get_id_column(target_table)
            target_table_col_name = self._get_table_col_base(target_table)

            # The table name - includes the name of the table linked to and the name of the custom column
            custom_col_table = self._get_custom_column_table_name(target_table, custom_column_name)
            assert custom_col_table not in self.tables, "cannot create custom column - it already exists"

            cc_sqlite_template = """
            CREATE TABLE IF NOT EXISTS `{custom_col_table}` (
                `{custom_col_table}_id` INTEGER PRIMARY KEY,
    
                `{custom_col_table}_{target_table_col_name}_id` TEXT NULL ,
                `{custom_col_table}_{target_table_col_name}_value` {datatype} NULL,
    
                `{custom_col_table}_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
    
                `{custom_col_table}_scratch` TEXT NULL,
    
            CONSTRAINT `{custom_col_table}_{target_table_col_name}_unique` UNIQUE (`{custom_col_table}_{target_table_col_name}_id`),
    
            CONSTRAINT `{custom_col_table}_value_in_{target_table}`
              FOREIGN KEY (`{custom_col_table}_{target_table_col_name}_id`)
              REFERENCES `{target_table}` (`{target_table_id_col}`)
              ON DELETE CASCADE
              ON UPDATE CASCADE);
                  """.format(
                custom_col_table=custom_col_table,
                target_table=target_table,
                target_table_id_col=target_table_id_col,
                datatype=datatype,
                target_table_col_name=target_table_col_name,
            )

            # INDEXES
            # - Indexing the reference to the table this is a custom column for - needed every time a lookup is done for
            #   the custom column value for a particular book
            table_id_ref_index = "CREATE INDEX {0}_idx ON {0} ({0}_{4}_id);".format(
                custom_col_table,
                target_table,
                target_table_id_col,
                datatype,
                target_table_col_name,
            )

            # - Indexing the values stored in the custom column - as we might want to search on the custom column values
            #   at some point
            col_value_ref = "CREATE INDEX {0}_value ON {0} ({0}_{4}_value);".format(
                custom_col_table,
                target_table,
                target_table_id_col,
                datatype,
                target_table_col_name,
            )

            # BUILD
            sql_scripts = [cc_sqlite_template, table_id_ref_index, col_value_ref]

            self.executescript("\n".join(sql_scripts))

            # Todo: Merge these two methods
            self.call_after_table_changes()
            self._zero_prop_cache()

            return custom_col_table

    # Todo: Removing values from books should also delete from this table
    # Todo: The values of the custom column should be unique
    # Todo: We should know the permissable data types - they should not be changeable
    # Todo: This should update the custom columns table with new information
    # Todo: Check that the direct_link_main_tables method has a properly autoincrementing primary key and priority
    def direct_create_one_to_many_custom_column(self, target_table, custom_column_name, datatype="TEXT"):
        """
        Create a custom column with a one-many relation between entries in the custom column and the given target table.
        There can be many values for every value in the :param target_table:, but these values must not intersect (they
        must be unique to the given book, title, series e.t.c)

        Removing a value from the :param target_table: will also remove all the linked

        :param target_table: The table which the custom column will be attached to
        :param custom_column_name: The name of the custom column which will be generated
        :param datatype: Datatype for the custom column
        :return custom_col_table: The name of the table holding the new custom column
        """
        assert self._validate_table_name(table_name=custom_column_name)
        assert target_table != "custom_columns", "Cannot Create a custom column on the custom_columns table"

        # Create a custom table to hold the custom column data - then link it over to the main table which it's supposed
        # to be a custom column in
        custom_col_table = self._get_custom_column_table_name(target_table, custom_column_name)

        # Make the new main table which will be used to hold the custom column information
        self.direct_create_new_main_table(table_name=custom_col_table, column_headings=None)

        # Link the new, storage main table over to the table which it's supposed to represent a custom column in
        link_table_name = self.direct_link_main_tables(
            primary_table=target_table,
            secondary_table=custom_col_table,
            link_type="one_many",
            requested_cols=None,
        )

        # Add a trigger to remove unused items from the table containing the custom column data when links to them
        # are removed
        # - Gather some properties of the lik we'll need to set up the trigger
        cc_col = self.direct_get_column_name(custom_col_table)
        cc_id_col = "{}_id".format(cc_col)

        link_table, link_table_col = self._get_link_table_name_col_name(
            primary_table=target_table, secondary_table=custom_col_table
        )

        link_table_cc_id_col = "{0}_{1}".format(link_table_col, cc_id_col)

        # - Define the actual SQLite for the trigger logic
        cc_cleanup_trigger = """
        CREATE TRIGGER {0}_cleanup_after_{1}_delete
            AFTER DELETE
            ON {2}
        BEGIN
            DELETE FROM {0}
            WHERE {3} = OLD.{4};
        END
        """.format(
            custom_col_table, target_table, link_table, cc_id_col, link_table_cc_id_col
        )
        self.execute_sql(cc_cleanup_trigger)

        self.call_after_table_changes()

        return custom_col_table

    # Todo: datatype
    def direct_create_many_to_one_custom_column(self, target_table, custom_column_name):
        """
        Create a custom column with a many-one relation between the entries in the custom column and the given target
        table.
        There can be many :param target_table: rows linked to the entries in this column -  but each  can only be linked
        to one of the entries in the custom table.

        Removing a value from :param target_table: will also remove all the elements linked to that title will be
        removed.

        :param target_table:
        :param custom_column_name:

        :return:
        """
        assert target_table != "custom_columns", "Cannot create custom column in custom_columns"
        assert self._validate_table_name(table_name=custom_column_name)

        # Create a custom table to hold the custom column data - then link it over to the main table which it's supposed
        # to be a custom column in
        custom_col_table = self._get_custom_column_table_name(target_table, custom_column_name)

        # Make the new main table which will be used to hold the custom column information
        self.direct_create_new_main_table(table_name=custom_col_table, column_headings=None)

        # Link the new, storage main table over to the table which it's supposed to represent a custom column in
        self.direct_link_main_tables(
            primary_table=target_table,
            secondary_table=custom_col_table,
            link_type="many_one",
            requested_cols=None,
        )

        self.call_after_table_changes()

        return custom_col_table

    def direct_create_many_many_custom_column(self, target_table, custom_column_name):
        """
        Create a custom column with a many-many relation between the entries in the custom column and the given target
        table.
        There can be many :param target_table: rows linked to the entries in the columns - and many entries can be
        linked to many entries in the custom column.

        :param target_table:
        :param custom_column_name:
        :return:
        """
        assert target_table != "custom_columns", "Cannot create custom column in custom_columns"
        assert self._validate_table_name(table_name=custom_column_name)

        # Create a custom table to hold the custom column data - then link it over to the main table which it's supposed
        # to be a custom column in
        custom_col_table = self._get_custom_column_table_name(target_table, custom_column_name)

        # Make the new main table which will be used to hold the custom column information
        self.direct_create_new_main_table(table_name=custom_col_table, column_headings=None)

        # Link the new, storage main table over to the table which it's supposed to represent a custom column in
        self.direct_link_main_tables(
            primary_table=target_table,
            secondary_table=custom_col_table,
            link_type="many_many",
            requested_cols=None,
        )

        self.call_after_table_changes()

        return custom_col_table

    def _get_custom_column_table_name(self, table, column_name):
        """
        Returns the name of a table to be used to store the given custom column.
        The name of the table contains information as to the name of the custom column and the table it's being applied
        to.
        :param table: The table the custom column is in
        :param column_name: The name of the custom column to be created
        :return:
        """
        return "custom_column${0}${1}".format(table, column_name)
