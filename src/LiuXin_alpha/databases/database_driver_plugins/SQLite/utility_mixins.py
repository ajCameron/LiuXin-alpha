
"""
Mixins for other classes to add functionality.
"""


# Moving some of the code here so it can be imported and used for common operations

import sqlite3

from copy import deepcopy

from typing import Optional, Iterable, Union, LiteralString

from LiuXin_alpha.utils.language_tools.pluralizers import plural_singular_mapper

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode
from LiuXin_alpha.utils.libraries.liuxin_six import string_types as basestring


class ColumnNameMixin:
    """
    Contains the methods used to generate the column and table names.
    """

    @staticmethod
    def get_allowed_types_table_name(for_table: str) -> str:
        """
        Returns the allowed types table name for a given table.

        :param for_table:
        :return:
        """
        return "allowed_types__{}".format(for_table)

    def get_allowed_types_table_name_intralinks(self, for_table: str) -> str:
        """
        Sometimes, intralink tables need types as well.

        :param for_table:
        :return:
        """
        return self.get_allowed_types_table_name("{}_{}_intralinks".format(for_table, for_table))

    @staticmethod
    def direct_get_column_base(table_name: str) -> str:
        """
        Returning the prefix for the column names for each column.

        :param table_name:
        :return:
        """
        return plural_singular_mapper(table_name)

    @staticmethod
    def _get_link_table_name_col_name(primary_table: str, secondary_table: str) -> tuple[str, str]:
        """
        Return the standardized name for a link table between the given primary and secondary tables.

        :param primary_table:
        :param secondary_table:
        :return table_name, col_name:
        """
        original_tables = [primary_table, secondary_table]
        tables = deepcopy(original_tables)
        tables.sort()

        table1_l_p = six_unicode(deepcopy(tables[0]))

        table2_l_p = six_unicode(deepcopy(tables[1]))

        # the singular form of the actual table - used in
        table1_l_s = plural_singular_mapper(table1_l_p)

        table2_l_s = plural_singular_mapper(table2_l_p)

        column_name = table1_l_s + "_" + table2_l_s + "_link"
        table_name = "{}s".format(column_name)

        return table_name, column_name

    @staticmethod
    def get_interlink_table_name(table1: str, table2: str) -> tuple[str, str]:
        """
        Return the name for an interlink table, from the two tables it should join.

        :param table1:
        :param table2:
        :return table_name, col_name:
        """
        tables = [table1, table2]
        tables.sort()

        # the plural form of the table names - the names of the actual table
        table1_l_p = deepcopy(tables[0])
        table1_l_p = six_unicode(table1_l_p)

        table2_l_p = deepcopy(tables[1])
        table2_l_p = six_unicode(table2_l_p)

        # the singular form of the actual table - used in making the column names
        table1_l_s = plural_singular_mapper(table1_l_p)

        table2_l_s = plural_singular_mapper(table2_l_p)

        column_name = table1_l_s + "_" + table2_l_s + "_link"
        table_name = "{}s".format(column_name)

        return table_name, column_name


class SQLiteTableLinkingMixin(ColumnNameMixin):
    """
    Class for generating link tables.

    Centralising the link table logic - to ensure consistency.
    """

    allowed_link_types: frozenset[str] = frozenset(
        [
            "many_many",
            "many_many_non_exclusive",
            "one_many",
            "many_one",
            "one_one",
            "one_one_normalized",
            "rating",
        ]
    )

    # Todo: Standardize table names for this type of method to primary and secondary
    def direct_link_main_tables(
        self,
        primary_table,
        secondary_table,
        link_type="many_many",
        requested_cols="all",
        index_both=True,
        allowed_types=None,
        override_restriction_sql=None,
    ):

        link_sql_list, table_name = self._get_direct_link_main_tables_sqlite(
            primary_table=primary_table,
            secondary_table=secondary_table,
            link_type=link_type,
            requested_cols=requested_cols,
            index_both=index_both,
            allowed_types=allowed_types,
            override_restriction_sql=override_restriction_sql,
        )

        try:
            self.executescript("\n".join(link_sql_list))
        except sqlite3.OperationalError as e:
            err_msg = ["Operational error when trying to link tables", "primary_table: {}".format(primary_table),
                       "secondary_table: {}".format(secondary_table), "link_type: {}".format(link_type),
                       "requested_cols: {}".format(requested_cols), "index_both: {}".format(index_both),
                       "\n" + "\n--------\n".join(link_sql_list) + "\n", "e: {}".format(e),
                       "e.message : {}".format(e.message)]

            print("\n".join(err_msg))

            raise

        # Changes have been made to the database - register this fact for later info
        self._zero_prop_cache()

        # Return the generated table name for additional work
        return table_name

    def _get_direct_link_main_tables_sqlite(
        self,
        primary_table: str,
        secondary_table: str,
        link_type: str = "many_many",
        requested_cols: str = "all",
        index_both: bool = True,
        allowed_types: Optional[Iterable[str]] = None,
        one_link_with_one_type: bool = True,
        override_restriction_sql: Optional[str] = None,
    ) -> tuple[list[str], Union[str, LiteralString]]:
        """
        Link the given main tables. The primary and secondary table designations indicate which table should be linked
        to the other with the given relationship.
        :param primary_table: This table will be linked to the secondary
        :param secondary_table:
        :param link_type: String indicating the type of link to be made between the two tables - primary will be linked
                          to secondary with the given link type.

        many_many - many of the primary table can be linked to many items in the secondary.
                    E.g. titles and tags - many tags can be linked to many titles
        many_one - many of the primary are linked to a single one of the secondary
                   E.g. files and folder stores - many files can be in a single foplder store, but they cannot be in
                   more than one folder store
        one_many - one of the primary can be linked to many of the secondary (just many_one seen the other way round)
                   E.g. one folder store can contain many files
                   E.g. one book can contain many files
        one_one - one of the primary can be linked to one of the secondary
                  E.g. uuids - every book has one and only one
        one_one_normalized - While the primary can only be linked to one of the secondary, the secondary can be linked
                             to many of the primary
                             E.g. the primary language of a title
                             This is one
        # Todo: Handle primary language through this mechanism, not the generic languages table

        :param requested_cols: Link table will be generated with the following properties - which will be applied to
                               each link. Default to "all"
        :param allowed_types: If provided, and there's a types column requested, will generate a allowed types table
                              and restrict the permitted types to these.
                              Should be None, or an itterable.

        :param index_both: Index both sides of the link to make searching lookup faster.

        :param one_link_with_one_type: If True, and there is a type column, then only one link between entities in the
                                       primary and secondayr table is allowed with each type

        E.g. language_title_links. You are allowed to link lang_1 and title_1 more than once provided the type is either
        null or different.

        # Todo: This should probably go away.
        :param override_restriction_sql: If provided, then this SQL will be used instead of the automatically generated
                                         one for the restrictions

        :return:
        """
        # Todo: Checking that primary and secondary are in main tables
        assert link_type in self.allowed_link_types, self._bad_link_type_error(link_type)

        # Generate the SQLite to link the two given tables with the given link type

        # 1) Need to make the name for the new link table - which might involve swapping the relation if the order of
        #    the primary and secondary tables has changed

        # Need to see if we need to do an inversion based on the COLUMN names, not the table name

        primary_table_row_name = self.direct_get_column_base(primary_table)
        secondary_table_row_name = self.direct_get_column_base(secondary_table)

        original_tables_after = [primary_table_row_name, secondary_table_row_name]

        original_tables_before = deepcopy(original_tables_after)

        original_tables_after.sort()

        if original_tables_before == original_tables_after:
            tables = [primary_table, secondary_table]
            flip = False
        else:
            tables = [secondary_table, primary_table]
            flip = True

        # 2) If the ordering of the tables has been changed then need to reflect this in the relation type
        if link_type in [
            "many_many",
            "many_many_non_exclusive",
            "one_one",
            "one_one_normalized",
            "rating",
        ]:
            pass

        elif link_type in ["many_one", "one_many"]:
            if not flip:
                pass
            else:
                if link_type == "many_one":
                    link_type = "one_many"
                elif link_type == "one_many":
                    link_type = "many_one"
                else:
                    raise NotImplementedError("This position should never be reached")

        else:
            raise NotImplementedError("This position should never be reached")

        # 2) Generate the main body of the table SQLite - the restrictions will be generated and applied later
        # the plural form of the table names - the names of the actual table
        table1_l_p = six_unicode(deepcopy(tables[0]))

        table2_l_p = six_unicode(deepcopy(tables[1]))

        # the singular form of the actual table - used in
        table1_l_s = plural_singular_mapper(table1_l_p)

        table2_l_s = plural_singular_mapper(table2_l_p)

        column_name = table1_l_s + "_" + table2_l_s + "_link"
        table_name = "{}s".format(column_name)

        comment_row = """
                -- -----------------------------------------------------
                -- Table `{}s`
                -- -----------------------------------------------------
                    """

        # - Not a great way of getting the table name - but should work as links is the plural of link
        comment_row = comment_row.format(column_name)

        table_sql_stmt_component_list = []

        if requested_cols is None:
            requested_cols = set()

        decrement_requested_cols = deepcopy(requested_cols)

        if decrement_requested_cols == "all":

            link_rows = """
                CREATE TABLE IF NOT EXISTS `{0}s`(
                  `{0}_id` INTEGER PRIMARY KEY ,
                  `{0}_{1}_id` INT UNSIGNED NULL,
                  `{0}_{2}_id` INT UNSIGNED NULL,
                  `{0}_priority` INT DEFAULT 0,
                  `{0}_primary` INT NULL DEFAULT 0,
                  `{0}_type` TEXT NULL,
                  `{0}_index` TEXT NULL,
                  `{0}_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),
                  `{0}_scratch` TEXT NULL 
                  """

        else:

            assert isinstance(decrement_requested_cols, set)

            link_rows_header = """
                CREATE TABLE IF NOT EXISTS `{0}s`(
                  `{0}_id` INTEGER PRIMARY KEY ,
                  `{0}_{1}_id` INT UNSIGNED NULL,
                  `{0}_{2}_id` INT UNSIGNED NULL,"""

            if "priority" in decrement_requested_cols:
                link_rows_header += "\n      `{0}_priority` INT DEFAULT 0,"
                decrement_requested_cols.remove("priority")

            if "primary" in decrement_requested_cols:
                link_rows_header += "\n      `{0}_primary` INT NULL DEFAULT 0,"
                decrement_requested_cols.remove("primary")

            if "type" in decrement_requested_cols:
                link_rows_header += "\n      `{0}_type` TEXT NULL,"
                decrement_requested_cols.remove("type")

            if "index" in decrement_requested_cols:
                link_rows_header += "\n      `{0}_index` TEXT NULL,"
                decrement_requested_cols.remove("index")

            link_table_footer = """
                  `{0}_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),
                  `{0}_scratch` TEXT NULL"""

            link_rows = link_rows_header + link_table_footer

        # The full statement will be constructed with a join later - do not want a comma between the comment and the
        # start of the actual table
        link_rows = comment_row + link_rows.format(column_name, table1_l_s, table2_l_s)
        table_sql_stmt_component_list.append(link_rows)

        # If the entry in either the left or the right table is deleted then it should remove this entry in the link
        # table as well
        left_foreign_key = """
                CONSTRAINT `{0}_{1}_id`
                  FOREIGN KEY (`{0}_{1}_id`)
                  REFERENCES `{2}` (`{1}_id`)
                  ON DELETE CASCADE
                  ON UPDATE CASCADE""".format(
            column_name, table1_l_s, table1_l_p, table2_l_s, table2_l_p
        )

        right_foreign_key = """
                CONSTRAINT `{0}_{3}_id`
                  FOREIGN KEY (`{0}_{3}_id`)
                  REFERENCES `{4}` (`{3}_id`)
                  ON DELETE CASCADE
                  ON UPDATE CASCADE""".format(
            column_name, table1_l_s, table1_l_p, table2_l_s, table2_l_p
        )
        table_sql_stmt_component_list.append(left_foreign_key)
        table_sql_stmt_component_list.append(right_foreign_key)

        # 3) Apply the restrictions to ensure the link is of the given type
        if override_restriction_sql is None:

            if link_type == "many_many":

                many_many_restrictions_list = []

                # One restriction must be applied - the same two objects cannot be linked twice
                many_many_restriction = (
                    "\n    CONSTRAINT `{0}_and_{1}_non_repeating_many_many_mapping`\n"
                    "      UNIQUE ({2}_{1}_id, {2}_{0}_id)".format(table1_l_s, table2_l_s, column_name)
                )

                many_many_restrictions_list.append(many_many_restriction)

                # If we have a priority column, then we need to generate a priority so that the primary is well ordered on
                # the secondary
                if requested_cols == "all" or "priority" in requested_cols:

                    m_t_m_ordering = (
                        "\n    CONSTRAINT `{0}_well_ordered_on_secondary_{1}`\n"
                        "UNIQUE ({2}_{3}_id, {2}_priority)".format(
                            primary_table,
                            secondary_table,
                            column_name,
                            primary_table_row_name,
                        )
                    )
                    many_many_restrictions_list.append(m_t_m_ordering)

                table_sql_stmt_component_list.append(",\n".join(many_many_restrictions_list))

            elif link_type == "many_many_non_exclusive":

                many_many_ne_restrictions_list = []

                if requested_cols == "all" or "priority" in requested_cols:

                    m_t_m_ordering = (
                        "\n    CONSTRAINT `{0}_well_ordered_on_secondary_{1}`\n"
                        "UNIQUE ({2}_{3}_id, {2}_priority)".format(
                            primary_table,
                            secondary_table,
                            column_name,
                            primary_table_row_name,
                        )
                    )
                    many_many_ne_restrictions_list.append(m_t_m_ordering)

                table_sql_stmt_component_list.append(",\n".join(many_many_ne_restrictions_list))

            elif link_type == "one_many":

                one_many_restrictions_list = []

                # table 1 is linked to table 2 with a relation of type one_many - thus restrict the number of entries from
                # table 1 to one
                one_many_restriction = (
                    "\n    CONSTRAINT `{0}_and_{1}_have_many_one_mapping`\n"
                    "      UNIQUE ({2}_{1}_id)".format(table1_l_s, table2_l_s, column_name)
                )
                one_many_restrictions_list.append(one_many_restriction)

                if requested_cols == "all" or "priority" in requested_cols:

                    o_t_m_ordering = (
                        "\n CONSTRAINT `{1}_well_ordered_on_{0}`\n"
                        "   UNIQUE ({2}_{0}_id, {2}_priority)".format(table1_l_s, table2_l_s, column_name)
                    )
                    one_many_restrictions_list.append(o_t_m_ordering)

                table_sql_stmt_component_list.append(",".join(one_many_restrictions_list))

            elif link_type == "many_one":

                many_one_restrictions_list = []

                # table 1 is linked to table 2 with a relation of type many_one - thus restricting the number of entries
                # from table 2 to one
                many_one_restriction = (
                    "\n    CONSTRAINT `{0}_and_{1}_have_one_many_mapping`\n"
                    "      UNIQUE ({2}_{0}_id)".format(table1_l_s, table2_l_s, column_name)
                )
                many_one_restrictions_list.append(many_one_restriction)

                if requested_cols == "all" or "priority" in requested_cols:
                    m_t_o_ordering = (
                        "\n CONSTRAINT `{0}_well_ordered_on_{1}`\n"
                        "   UNIQUE ({2}_{1}_id, {2}_priority)".format(table1_l_s, table2_l_s, column_name)
                    )
                    many_one_restrictions_list.append(m_t_o_ordering)

                table_sql_stmt_component_list.append(",".join(many_one_restrictions_list))

            elif link_type == "one_one":
                # table 1 is linked to table 2 with a relation of type one_one - thus restricting the number of entries
                # from both tables to one
                one_one_restriction = (
                    "\n    CONSTRAINT `{2}_{1}_id_appears_once`\n"
                    "      UNIQUE ({2}_{1}_id),"
                    "\n    CONSTRAINT `{2}_{0}_id_appears_once`\n"
                    "      UNIQUE ({2}_{0}_id)".format(table1_l_s, table2_l_s, column_name)
                )
                table_sql_stmt_component_list.append(one_one_restriction)

                # No priority logic really required...
                # Todo: The concept of a priority column for this type of table is meaningless. Remove it.

            elif link_type == "one_one_normalized":

                one_one_normalized_restriction = "\n CONSTRAINT `primary_appears_once`\n " "UNIQUE ({0}_{1}_id)".format(
                    column_name, primary_table_row_name
                )
                table_sql_stmt_component_list.append(one_one_normalized_restriction)

                # Todo: The concept of a priority column for this type of table is meaningless. Remove it.

            elif link_type == "rating":

                rating_title_linked_once = (
                    "\n    CONSTRAINT `one_type_of_{2}_per_{1}`\n"
                    "      UNIQUE({0}_{1}_id, {0}_type)\n".format(
                        column_name, primary_table_row_name, secondary_table_row_name
                    )
                )
                table_sql_stmt_component_list.append(rating_title_linked_once)

            else:

                raise NotImplementedError("link_type not recognized")

            # In the case where there are types specified, we must construct and populate a link type table for it
            if (requested_cols == "all" or "type" in requested_cols) and allowed_types is not None:

                # Add in the foreign key linking out to the allowed_types table
                att_name = self.get_allowed_types_table_name(table_name)
                att_col_name = att_name[:-1]  # Consistently just trimming the s off

                # Should not be necessary in the other cases
                # Todo: Check that this is true for all other cases
                if link_type == "many_many_non_exclusive" and one_link_with_one_type:

                    olot_constraint = """
                    CONSTRAINT `{1}_{2}_linked_max_once_for_each_type`
                    UNIQUE({0}_{1}_id, {0}_{2}_id, {0}_type)
                    """.format(
                        column_name, primary_table_row_name, secondary_table_row_name
                    )

                    table_sql_stmt_component_list.append(olot_constraint)

                at_foreign_key = """
                CONSTRAINT `{0}_type_is_allowed`
                  FOREIGN KEY (`{1}_type`)
                  REFERENCES `{2}` (`{3}_type`)
    
                """.format(
                    att_name, column_name, att_name, att_col_name
                )

                table_sql_stmt_component_list.append(at_foreign_key)

                table_sqlite = ",".join(table_sql_stmt_component_list) + ");"

                # Generate the allowed types table SQLite
                att_sqlite_list = self._build_allowed_types_table_interlink(
                    for_table=table_name, allowed_types=allowed_types
                )
                full_script = att_sqlite_list + [
                    table_sqlite,
                ]

            else:

                table_sqlite = ",".join(table_sql_stmt_component_list) + ");"
                full_script = [
                    table_sqlite,
                ]

        else:
            table_sql_stmt_component_list.append(override_restriction_sql)
            table_sqlite = ",".join(table_sql_stmt_component_list) + ");"
            full_script = [
                table_sqlite,
            ]

        if index_both:

            # Index on the left - the reference out to the custom column
            left_index_stmt = "CREATE INDEX IF NOT EXISTS {2}_{0}_id_index ON {2}s ({2}_{0}_id);".format(
                table1_l_s,
                table2_l_s,
                column_name,
            )
            full_script.append(left_index_stmt)

            # Index on the left - the reference to the original table that the custom column will appear in
            right_index_stmt = "CREATE INDEX IF NOT EXISTS  {2}_{1}_id_index ON {2}s ({2}_{1}_id);".format(
                table1_l_s,
                table2_l_s,
                column_name,
            )
            full_script.append(right_index_stmt)

        return full_script, table_name

    def _bad_link_type_error(self, link_type: str) -> str:
        """
        Error message for when the requested link type between two tables is nonsense

        :param self:
        :param link_type:
        :return:
        """
        err_msg = [
            "Requested link type between two main tables is not known - probable typo?",
            "link_type: {}".format(link_type),
            "allowed_link_types: {}".format(self.allowed_link_types),
        ]
        return "\n".join(err_msg)

    def build_interlink_table_sqlite(
            self,
            table1: str,
            table2: str,
            requested_cols: Optional[Union[str, Iterable[str]]] = None
    ) -> list[str]:
        """
        Build and return sqlite for the interlink table.

        :param table1:
        :param table2:
        :param requested_cols:
        :return:
        """

        table_name, _ = self.get_interlink_table_name(table1, table2)

        sql_override_restriction = None
        if table_name in self.INTERLINK_TABLE_CONSTRAINTS:
            sql_override_restriction = self.INTERLINK_TABLE_CONSTRAINTS[table_name]
        assert sql_override_restriction is not None, "This should not be none - {}".format(table_name)

        allowed_link_types = None
        if requested_cols == "all" or "type" in requested_cols:

            # A type column has been declared - if there is TYPE column, then there should also be an allowed_type
            # table
            assert (
                table_name in self.ALLOWED_INTERLINK_TYPE_VAL_DICT.keys()
            ), "type column requested for {} but not corresponding value in allowed_type_val_dict".format(table_name)

            allowed_link_types = self.ALLOWED_INTERLINK_TYPE_VAL_DICT[table_name]

        # We have a simple string to use instead of any generated restrictions - just use that
        # Todo: Eventually should be able to pull this out - it's becoming increasingly redundant
        if sql_override_restriction is None or isinstance(sql_override_restriction, basestring):
            full_script, table_name = self._get_direct_link_main_tables_sqlite(
                primary_table=table1,
                secondary_table=table2,
                requested_cols=requested_cols,
                allowed_types=allowed_link_types,
                override_restriction_sql=sql_override_restriction,
            )

        # We have a dictionary which characterises the link - use that
        elif isinstance(sql_override_restriction, dict):

            primary_table = sql_override_restriction["primary"]
            secondary_table = sql_override_restriction["secondary"]
            link_tyoe = sql_override_restriction["link_type"]

            full_script, table_name = self._get_direct_link_main_tables_sqlite(
                primary_table=primary_table,
                secondary_table=secondary_table,
                link_type=link_tyoe,
                requested_cols=requested_cols,
                allowed_types=allowed_link_types,
                one_link_with_one_type=True,
                override_restriction_sql=None,
            )

        else:
            raise NotImplementedError(
                "Unexpected case for sql_override_restriction - {} - {}".format(
                    sql_override_restriction, type(sql_override_restriction)
                )
            )

        return full_script

    # Todo: How is this different from the above?
    def _build_interlink_table_sqlite(
        self,
        table1: str,
        table2: str,
        requested_cols: Optional[Union[str, list[str]]] = None,
        allowed_types: Optional[Iterable[str]] = None,
        override_restriction_sql: Optional[str] = None,
    ) -> list[str]:
        """
        Takes the names of two tables. Builds the SQLite code and returns it.

        :param table1: The name of the first table (order doesn't matter - they will be alphabetized)
        :param table2: The name of the second table
        :param requested_cols: The cols which should be included in the

        :return link_sql: SQL for a table to link the two given tables. Also with the SQL needed to make an
                          Allowed types table for the link in question - if required.
        """
        tables = [table1, table2]
        tables.sort()

        table_name, column_name = self.get_interlink_table_name(table1, table2)

        # the plural form of the table names - the names of the actual table
        table1_l_p = deepcopy(tables[0])
        table1_l_p = six_unicode(table1_l_p)

        table2_l_p = deepcopy(tables[1])
        table2_l_p = six_unicode(table2_l_p)

        # the singular form of the actual table - used in making the column names
        table1_l_s = plural_singular_mapper(table1_l_p)

        table2_l_s = plural_singular_mapper(table2_l_p)

        # With the table name in hand we can go ahead and construct the allowed type table
        att_table_sqlite = self.build_allowed_types_table_interlink(table_name, allowed_types=allowed_types)

        comment_row = """
        -- -----------------------------------------------------
        -- Table `{}s`
        -- -----------------------------------------------------
            """

        comment_row = comment_row.format(column_name)

        sql_stmt_component_list = []

        # If we've got a type column we also need a allowed_types__{table_name} table
        if requested_cols == "all":

            link_rows = """
        CREATE TABLE IF NOT EXISTS `{0}s`(
          `{0}_id` INTEGER PRIMARY KEY ,
          `{0}_{1}_id` INT UNSIGNED NULL,
          `{0}_{2}_id` INT UNSIGNED NULL,
          `{0}_priority` INT DEFAULT 0,
          `{0}_primary` INT NULL DEFAULT 0,
          `{0}_type` TEXT NULL,
          `{0}_index` TEXT NULL,
          `{0}_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),
          `{0}_scratch` TEXT NULL"""

        else:

            assert isinstance(requested_cols, set)
            decremented_requested_cols = deepcopy(requested_cols)

            link_rows_header = """
        CREATE TABLE IF NOT EXISTS `{0}s`(
          `{0}_id` INTEGER PRIMARY KEY ,
          `{0}_{1}_id` INT UNSIGNED NULL,
          `{0}_{2}_id` INT UNSIGNED NULL,"""

            if "priority" in decremented_requested_cols:
                link_rows_header += "\n      `{0}_priority` INT DEFAULT 0,"
                decremented_requested_cols.remove("priority")

            if "primary" in decremented_requested_cols:
                link_rows_header += "\n      `{0}_primary` INT NULL DEFAULT 0,"
                decremented_requested_cols.remove("primary")

            if "type" in decremented_requested_cols:
                link_rows_header += "\n      `{0}_type` TEXT NULL,"
                decremented_requested_cols.remove("type")

            if "index" in decremented_requested_cols:
                link_rows_header += "\n      `{0}_index` TEXT NULL,"
                decremented_requested_cols.remove("index")

            link_table_footer = """
          `{0}_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),
          `{0}_scratch` TEXT NULL"""

            link_rows = link_rows_header + link_table_footer

        # The full statement will be constructed with a join later - do not want a comma between the comment and the
        # start of the actual table
        link_rows = comment_row + link_rows.format(column_name, table1_l_s, table2_l_s)
        sql_stmt_component_list.append(link_rows)

        if requested_cols == "all" or "type" in requested_cols:
            # Add in the foreign key linking out to the allowed_types table
            att_name = self.get_allowed_types_table_name(table_name)
            att_col_name = att_name[:-1]  # Consistently just trimming the s off

            at_foreign_key = """
            CONSTRAINT `{0}_type_is_allowed`
              FOREIGN KEY (`{1}_type`)
              REFERENCES `{2}` (`{3}_type`)

            """.format(
                att_name, column_name, att_name, att_col_name
            )

            sql_stmt_component_list.append(at_foreign_key)

        # If the entry in either the left or the right table is deleted then it should remove this entry in the link
        # table as well
        left_foreign_key = """
        CONSTRAINT `{0}_{1}_id`
          FOREIGN KEY (`{0}_{1}_id`)
          REFERENCES `{2}` (`{1}_id`)
          ON DELETE CASCADE
          ON UPDATE CASCADE""".format(
            column_name, table1_l_s, table1_l_p, table2_l_s, table2_l_p
        )

        right_foreign_key = """
        CONSTRAINT `{0}_{3}_id`
          FOREIGN KEY (`{0}_{3}_id`)
          REFERENCES `{4}` (`{3}_id`)
          ON DELETE CASCADE
          ON UPDATE CASCADE""".format(
            column_name, table1_l_s, table1_l_p, table2_l_s, table2_l_p
        )
        sql_stmt_component_list.append(left_foreign_key)
        sql_stmt_component_list.append(right_foreign_key)

        if override_restriction_sql is not None:
            sql_stmt_component_list.append(override_restriction_sql)

        sqlite = ",".join(sql_stmt_component_list) + ");"
        if att_table_sqlite is not None:
            att_table_sqlite.append(sqlite)
            return att_table_sqlite
        else:
            return [
                sqlite,
            ]

    def build_allowed_types_table_interlink(
            self,
            for_table: str,
            allowed_types: Optional[Iterable[str]] = None
    ) -> list[str]:
        """
        Construct an allowed types table - populated with the values from the allowed_type_val_dict.

        :param for_table:
        :param allowed_types:
        :return att_sql: A list of SQLite statements which both creates and populates the table
        """
        if allowed_types is None:
            return []

        return self._build_allowed_types_table_interlink(for_table, allowed_types)

    def _build_allowed_types_table_interlink(self, for_table, allowed_types):
        """
        Construct an allowed types table - populated with the values from the allowed_type_val_dict.
        :param for_table:
        :return att_sql: A list of SQLite statements which both creates and populates the table
        """

        allowed_table_name = self.get_allowed_types_table_name(for_table)
        allowed_table_col_name = allowed_table_name[:-1]

        att_table_sqlite = """
        CREATE TABLE IF NOT EXISTS `{table}` (
          `{column}_id` INTEGER PRIMARY KEY,
          `{column}_type` TEXT NULL,
          `{column}_datestamp` DATETIME DEFAULT CURRENT_TIMESTAMP,
          `{column}_scratch` TEXT NULL,          
          CONSTRAINT `{table}_type_unique`
          UNIQUE({column}_type)
          );

        """.format(
            table=allowed_table_name, column=allowed_table_col_name
        )

        # Add a statement for every element we want to add to the table
        att_add_sqlite = []
        for at in allowed_types:
            at_insert_stmt = 'INSERT INTO {table} ({column}_type) VALUES ("{at}");'.format(
                table=allowed_table_name, column=allowed_table_col_name, at=at
            )
            att_add_sqlite.append(at_insert_stmt)

        return [
            att_table_sqlite,
        ] + att_add_sqlite
