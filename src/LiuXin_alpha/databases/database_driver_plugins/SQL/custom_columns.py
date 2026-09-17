
"""
Create SQLite custom-value tables and their one/many relationships.

These low-level helpers build physical tables/links without registering a complete
Calibre-style custom_columns metadata record. Concrete hosts supply execution,
naming, cached table information and schema-change callbacks.
"""

from __future__ import annotations


class SQLiteCustomColumnsDriverMixin:
    """
    Build dedicated custom-value storage tables through concrete driver hooks.

    Example:
        ``driver.direct_create_custom_column("works", "note")`` creates the
        inline one-to-one storage table for that label.
    """

    # Todo: The code for this is currently over in macros. It should probably be here
    # Todo: When this method is called it needs to be noted in the custom columns table with details of the column created
    # Todo: Need to write a standardized way of doing custom columns - with sane naming
    # Then test how all these tables behave when confronted with a custom column
    # custom column tables "{original_table}_custom_column_{name}"
    # When deleting an entry in the main table should also take out all entries in the custom columns
    # (do this by modifying the link creation syntax so it supports the option of delete triggers)
    # Support all relation types
    def direct_create_custom_column(
            self,
            # Todo: This should be highly typable
            in_table: str,
            column_name: str,
            # Todo: This should be highly typable
            data_type: str = "TEXT",
            multi: bool = False) -> None:
        """
        Dispatch physical custom-column creation by relationship type.

        Falsy multi selects inline one-to-one; True selects many_many. Other supported
        values are one_many and many_one. Reject custom_columns as a target by assertion
        and unknown modes with NotImplementedError. Despite the annotation, return the
        created table name from the selected helper.

        Example:
            ``direct_create_custom_column("works", "labels", multi=True)`` selects
            the many-to-many builder.


        :param in_table: Existing target table, not a table-category label.
        :param column_name: Custom-column label used to derive the storage name.
        :param data_type: SQL type forwarded to one-to-one/one-to-many; only one-to-one currently uses it.
        :param multi: Falsy for one-to-one, True for many_many, or a supported relationship string.
        :return: Created custom-value table name.
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
            # Historically, callers used ``multi=True`` to mean "default" multiple
            # relation type. Treat this as many_many for backwards compatibility.
            if multi is True:
                multi = "many_many"
            if multi == "many_many":
                # Need to create a many_many relation between the book and the values for it in the custom column
                return self.direct_create_many_many_custom_column(
                    target_table=in_table, custom_column_name=column_name
                )
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
            self,
            target_table: str,
            custom_column_name: str,
            datatype: str = "TEXT",
            normalized: bool = False
    ) -> str:
        """
        Create an inline custom-value table with a unique parent reference and cascade FK.

        Assert label/target validity and cache nonexistence, create lookup/value indexes,
        then invoke schema-change/cache hooks. The parent-reference column has TEXT affinity.
        Normalized mode is unsupported and raises NotImplementedError.

        Example:
            For each work, at most one linked note row can exist; deleting the work
            cascades to that row when SQLite foreign keys are enabled.


        :param target_table: Existing main table to which the custom values belong.
        :param custom_column_name: Custom-column label used in its generated storage-table name.
        :param datatype: Requested SQL type; only the inline one-to-one builder uses it.
        :param normalized: Must be False; the normalized link-table variant is not implemented.
        :return: Created storage-table name.
        """
        # VALIDATE

        if normalized:

            # Create a table to hold the values

            raise NotImplementedError

        else:

            assert target_table != "custom_columns", "Cannot create custom column in custom_columns"

            # Check the components which are going to go into the table name
            assert self.direct_validate_table_name(table_name=custom_column_name), "custom column name not valid"

            # MAIN TABLE

            target_table_id_col = self.direct_get_id_column(target_table)
            target_table_col_name = self.direct_get_table_col_base(target_table)

            # The table name - includes the name of the table linked to and the name of the custom column
            custom_col_table = self.direct_get_custom_column_table_name(target_table, custom_column_name)
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

            self.direct_executescript("\n".join(sql_scripts))

            # Todo: Merge these two methods
            self.call_after_table_changes()
            self._zero_prop_cache()

            return custom_col_table

    # Todo: Removing values from books should also delete from this table
    # Todo: The values of the custom column should be unique
    # Todo: We should know the permissable data types - they should not be changeable
    # Todo: This should update the custom columns table with new information
    # Todo: Check that the direct_link_main_tables method has a properly autoincrementing primary key and priority
    def direct_create_one_to_many_custom_column(
            self,
            target_table: str,
            custom_column_name: str,
            datatype: str = "TEXT") -> str:
        """
        Create a value table and exclusive one-to-many links plus an unlink cleanup trigger.

        The trigger deletes the custom value whenever its link is removed. The datatype
        argument is currently ignored: main-table creation uses its own default type.

        Example:
            One work can own several custom values, and unlinking one deletes that
            owned value through the generated trigger.


        :param target_table: Existing main table to which the custom values belong.
        :param custom_column_name: Custom-column label used in its generated storage-table name.
        :param datatype: Requested SQL type; only the inline one-to-one builder uses it.
        :return: Created custom-value table name.
        """
        assert self.direct_validate_table_name(table_name=custom_column_name)
        assert target_table != "custom_columns", "Cannot Create a custom column on the custom_columns table"

        # Create a custom table to hold the custom column data - then link it over to the main table which it's supposed
        # to be a custom column in
        custom_col_table = self.direct_get_custom_column_table_name(target_table, custom_column_name)

        # Make the new main table which will be used to hold the custom column information
        self.direct_create_main_table(table_name=custom_col_table, column_headings=None)

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
        self.direct_execute_sql(cc_cleanup_trigger)

        self.call_after_table_changes()

        return custom_col_table

    # Todo: datatype
    def direct_create_many_to_one_custom_column(
            self,
            target_table: str,
            custom_column_name: str) -> str:
        """
        Create reusable custom values with at most one linked value per parent.

        Build the main table with default settings, then many_one links and refresh schema
        state. No unused-value cleanup trigger is installed here.

        Example:
            Several works may share one custom value while each work has at most one.


        :param target_table: Existing main table to which the custom values belong.
        :param custom_column_name: Custom-column label used in its generated storage-table name.
        :return: Created custom-value table name.
        """
        assert target_table != "custom_columns", "Cannot create custom column in custom_columns"
        assert self.direct_validate_table_name(table_name=custom_column_name)

        # Create a custom table to hold the custom column data - then link it over to the main table which it's supposed
        # to be a custom column in
        custom_col_table = self.direct_get_custom_column_table_name(target_table, custom_column_name)

        # Make the new main table which will be used to hold the custom column information
        self.direct_create_main_table(table_name=custom_col_table, column_headings=None)

        # Link the new, storage main table over to the table which it's supposed to represent a custom column in
        self.direct_link_main_tables(
            primary_table=target_table,
            secondary_table=custom_col_table,
            link_type="many_one",
            requested_cols=None,
        )

        self.call_after_table_changes()

        return custom_col_table

    def direct_create_many_many_custom_column(
            self,
            target_table: str,
            custom_column_name: str) -> str:
        """
        Create shared custom values and strict many-to-many links without optional columns.

        Build the main table with default settings and notify the host of schema changes.

        Example:
            Multiple works can share several custom labels through the generated link table.


        :param target_table: Existing main table to which the custom values belong.
        :param custom_column_name: Custom-column label used in its generated storage-table name.
        :return: Created custom-value table name.
        """
        assert target_table != "custom_columns", "Cannot create custom column in custom_columns"
        assert self.direct_validate_table_name(table_name=custom_column_name)

        # Create a custom table to hold the custom column data - then link it over to the main table which it's supposed
        # to be a custom column in
        custom_col_table = self.direct_get_custom_column_table_name(target_table, custom_column_name)

        # Make the new main table which will be used to hold the custom column information
        self.direct_create_main_table(table_name=custom_col_table, column_headings=None)

        # Link the new, storage main table over to the table which it's supposed to represent a custom column in
        self.direct_link_main_tables(
            primary_table=target_table,
            secondary_table=custom_col_table,
            link_type="many_many",
            requested_cols=None,
        )

        self.call_after_table_changes()

        return custom_col_table

    @staticmethod
    def direct_get_custom_column_table_name(table: str, column_name: str) -> str:
        """
        Combine table and label into the custom_column$ naming convention without validation.

        Example:
            >>> SQLiteCustomColumnsDriverMixin.direct_get_custom_column_table_name("works", "note")
            'custom_column$works$note'


        :param table: Target table spelling inserted into the name.
        :param column_name: Custom label inserted into the name.
        :return: Storage-table name containing both supplied components.
        """
        return "custom_column${0}${1}".format(table, column_name)
