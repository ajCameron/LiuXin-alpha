
"""
Mixin method to allow the driver to create tables.

Does not include custom column table creation logic.
"""

from LiuXin_alpha.utils.language_tools import plural_singular_mapper




class TableCreationMixin:
    """
    Mixin to permit the creation of new main tables.
    """

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - TABLE CREATION METHODS
    # Todo: Need a way to change the data type of the default column - also the data type of any additional columns created
    # Todo: Pull the "new" out of the name - that's implcit
    # Todo: Need a way to designate this new table "custom"
    def direct_create_new_main_table(
            self,
            table_name,
            column_headings=None,
            index_on="all",
            default_datatype="TEXT",
            default_unique=False,
    ):
        """
        Create a new main table on the database.

        :param table_name: Name for the new main table (please obey the naming scheme). Trying to create a table with a
                           name the same as that of another in the database)

        :param column_headings: Columns names (in the final table the name of the table _ column name.
                                The final table with have additional datestamp and scratch columns.
                                Columns headings should be provided in the form of a dictionary (optionally ordered)
                                Keyed with the name of the column and valued with the datatype for that column.

        :param index_on: The columns to also create indexes for - defaults to 'all' - which will generate an index for
                         all the requested custom columns

        :param default_datatype: The default datatype what will be used if no other is provided. Defaults to txt.



        :return:
        """
        table_col = plural_singular_mapper(table_name)

        indices = []

        # TABLE PREAMBLE

        table_comment = """
    -- -----------------------------------------------------
    -- Table `{0}`
    -- -----------------------------------------------------
    """.format(
            table_name
        )

        table_head = """
            CREATE TABLE IF NOT EXISTS `{0}` (
        `{1}_id` INTEGER PRIMARY KEY,

            """.format(
            table_name, table_col
        )

        # COLUMN CONTENT
        if column_headings is None:

            # - In the case where the column headings are None, then generate the default column headings
            table_columns = """
            `{table_col}` {datatype} NULL,
                """.format(
                table_name=table_name, table_col=table_col, datatype=default_datatype
            )

            if index_on == "all":

                default_col_index = "CREATE INDEX {0}_default_col_index ON {0} ({1});".format(table_name, table_col)
                indices.append(default_col_index)

            else:

                raise NotImplementedError

        else:

            # - Process the columns headings object to produce the requested headings
            col_template = """
            `{0}_{1}` {2} NULL,            
                """.format(
                table_col, "{0}", "{1}"
            )

            additional_columns = []
            for col in column_headings:

                try:
                    additional_columns.append(col_template.format(col, column_headings[col]["datatype"]))
                except KeyError:
                    # If no datatype is present in the specifications dict, use the default
                    additional_columns.append(col_template.format(col, default_datatype))

            table_columns = "\n".join(additional_columns)

        # TABLE FINISHING
        table_tail = """

        `{1}_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,

        `{1}_scratch` TEXT NULL);
            """.format(
            table_name, table_col
        )

        table_sqlite = table_comment + table_head + table_columns + table_tail

        full_script = [
            table_sqlite,
        ]
        full_script.extend(indices)

        # # Index for the custom columns
        # assert index_on == "all", "Cannot but index on all custom columns"
        # default_col_index = "CREATE INDEX {0}_default_col_index ON {0} ({1});".format(table_name, table_col)
        # full_script.append(default_col_index)

        self.executescript("\n".join(full_script))

        self._zero_prop_cache()
