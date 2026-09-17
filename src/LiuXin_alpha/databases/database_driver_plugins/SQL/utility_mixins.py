
"""
Share SQL naming conventions and SQLite link-table DDL construction.

Builders emit controlled SQL fragments with cardinality, provenance and optional
type/symmetry guards. Most methods only return SQL; execution methods rely on a
concrete host connection, script executor and schema/cache hooks.
"""

# Moving some of the code here so it can be imported and used for common operations

from __future__ import absolute_import, annotations

import re
import sqlite3
from copy import deepcopy
from typing import TYPE_CHECKING, Iterable, Literal, LiteralString, Optional, Union

from LiuXin_alpha.utils.language_tools.pluralizers import plural_singular_mapper
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode
from LiuXin_alpha.utils.libraries.liuxin_six import string_types as basestring


class ColumnNameMixin:
    """
    Derive singular column prefixes and conventional link/allowed-type table names.

    Example:
        >>> ColumnNameMixin.get_interlink_table_name("works", "agents")
        ('agent_work_links', 'agent_work_link')
    """

    @staticmethod
    def get_allowed_types_table_name(for_table: str) -> str:
        """
        Prefix the supplied table spelling with allowed_types__.

        Example:
            >>> ColumnNameMixin.get_allowed_types_table_name("agent_work_links")
            'allowed_types__agent_work_links'


        :param for_table: Table whose allowed-type lookup name is derived.
        :return: Allowed-type table name; no validation or SQL escaping is performed.
        """
        return "allowed_types__{}".format(for_table)

    def get_allowed_types_table_name_intralinks(self, for_table: str) -> str:
        """
        Build the allowed-type name from a repeated, unmodified main-table spelling.

        Example:
            >>> ColumnNameMixin().get_allowed_types_table_name_intralinks("works")
            'allowed_types__works_works_intralinks'


        :param for_table: Table whose allowed-type lookup name is derived.
        :return: Allowed-type name for the repeated main-table intralink spelling.
        """
        return self.get_allowed_types_table_name("{}_{}_intralinks".format(for_table, for_table))

    @staticmethod
    def direct_get_column_base(table_name: str) -> str:
        """
        Use the shared plural/singular mapper to obtain a column prefix.

        Example:
            >>> ColumnNameMixin.direct_get_column_base("works")
            'work'


        :param table_name: Table name passed unchanged to the shared mapper.
        :return: Canonical singular prefix.
        """
        return plural_singular_mapper(table_name)

    @staticmethod
    def _get_link_table_name_col_name(primary_table: str, secondary_table: str) -> tuple[str, str]:
        """
        Sort supplied table names, singularize each and derive link table/prefix names.

        Sorting occurs before singularization; no table existence or identifier validation
        is performed.

        Example:
            For works and agents, the result is agent_work_links and agent_work_link.


        :param primary_table: First table sorted before singularization.
        :param secondary_table: Second table sorted before singularization.
        :return: Pair of plural link-table name and singular column prefix.
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
        Sort supplied table names, singularize each and derive link table/prefix names.

        Sorting occurs before singularization; no table existence or identifier validation
        is performed.

        Example:
            For works and agents, the result is agent_work_links and agent_work_link.


        :param table1: First table whose singular prefix participates in the link name.
        :param table2: Second table whose singular prefix participates in the link name.
        :return: Pair of plural link-table name and singular column prefix.
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
    Generate SQLite interlink/intralink schemas and execute selected setup operations.

    Hosts provide driver/builder hooks. Type lookup tables exist in both legacy FK
    form and extensible __types tables guarded by triggers.

    Example:
        A concrete builder calls direct_get_direct_link_main_tables_sql to inspect
        DDL before executing it.
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
    def set_database_version(self) -> None:
        """
        Upsert row 1 of database_version using the APSW driver master-version string.

        Commit, then assert that the value reads back. OperationalError on the write is
        translated to an assertion suggesting that metadata tables are missing.

        Example:
            Call after creating database_version; the method writes and commits its version row.


        :return: None; changes the host connection and checks the stored version.
        """
        # Todo: Just wrong. Very wrong.
        from LiuXin_alpha.databases.database_driver_plugins.SQLite_apsw import (
            get_SQLite_driver_master_version,
        )

        version_str = get_SQLite_driver_master_version()

        c = self.conn.cursor()
        try:
            c.execute(
                "INSERT OR REPLACE INTO database_version (database_version_id, database_version_version) VALUES (?, ?);",
                ("1", version_str),
            )
        except sqlite3.OperationalError as e:
            raise AssertionError(
                "database_version table is missing; ensure metadata tables are created before calling set_database_version()"
            ) from e
        self.conn.commit()

        row = c.execute(
            "SELECT database_version_version FROM database_version WHERE database_version_id = ?;",
            ("1",),
        ).fetchone()
        assert row is not None and row[0] == version_str

    # Todo: The aim is for this to be the single source of truth for the database linker/generator
    def direct_link_main_tables(
        self,
        primary_table: str,
        secondary_table: str,
        link_type: Literal["many_many", "one_many", "many_one", "one_one"] = "many_many",
        requested_cols: Union[str, Iterable[str]] = "all",
        index_both: bool = True,
        allowed_types: Optional[Iterable[str]] = None,
        override_restriction_sql: Optional[str] = None,
        nullable_fks: bool = True,
    ) -> str:
        """
        Generate a link script, execute it through the host and clear property caches.

        The legacy OperationalError diagnostic accesses e.message, which may itself raise
        AttributeError on modern sqlite3 exceptions and mask the original failure.

        Example:
            Linking agents and works returns agent_work_links after script execution.


        :param primary_table: First main table in the requested relationship.
        :param secondary_table: Second main table in the requested relationship.
        :param link_type: Supported cardinality name from allowed_link_types.
        :param requested_cols: all, None, or iterable of optional metadata suffixes.
        :param index_both: Whether to index both endpoint foreign-key columns.
        :param allowed_types: Optional iterable of permitted type labels.
        :param override_restriction_sql: Trusted raw constraint SQL replacing generated cardinality and type constraints.
        :param nullable_fks: Requested FK nullability; interlink generation currently always emits nullable FKs.
        :return: Generated link-table name on success.
        """

        link_sql_list, table_name = self.direct_get_direct_link_main_tables_sql(
            primary_table=primary_table,
            secondary_table=secondary_table,
            link_type=link_type,
            requested_cols=requested_cols,
            index_both=index_both,
            allowed_types=allowed_types,
            override_restriction_sql=override_restriction_sql,
            nullable_fks=nullable_fks,
        )

        try:
            self.direct_executescript("\n".join(link_sql_list))
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

    def direct_get_direct_link_main_tables_sql(
        self,
        primary_table: str,
        secondary_table: str,
        link_type: str = "many_many",
        requested_cols: str = "all",
        index_both: bool = True,
        allowed_types: Optional[Iterable[str]] = None,
        one_link_with_one_type: bool = True,
        override_restriction_sql: Optional[str] = None,
        nullable_fks: bool = True,
    ) -> tuple[list[str], Union[str, LiteralString]]:
        """
        Generate link DDL, optional type-table inserts and endpoint/sequence indexes.

        Sort singular bases and invert one/many orientation when needed. Strict many_many
        uniqueness ignores type; nonexclusive uniqueness includes type when requested.
        FKs remain nullable regardless of nullable_fks to allow blank-row construction.
        Raw overrides replace automatic cardinality/type handling. This does not execute SQL;
        identifiers and legacy type-seed literals require trusted inputs.

        Example:
            With requested_cols=None, the link still contains source, datestamp and scratch
            columns alongside its IDs and relationship constraints.


        :param primary_table: First main table in the requested relationship.
        :param secondary_table: Second main table in the requested relationship.
        :param link_type: Supported cardinality name from allowed_link_types.
        :param requested_cols: all, None, or iterable of optional metadata suffixes.
        :param index_both: Whether to index both endpoint foreign-key columns.
        :param allowed_types: Optional iterable of permitted type labels.
        :param one_link_with_one_type: Accepted but unused; uniqueness follows link_type and requested metadata.
        :param override_restriction_sql: Trusted raw constraint SQL replacing generated cardinality and type constraints.
        :param nullable_fks: Requested FK nullability; interlink generation currently always emits nullable FKs.
        :return: SQL statement list and generated link-table name.
        """
        # Todo: Checking that primary and secondary are in main tables
        if link_type not in self.allowed_link_types:
            raise NotImplementedError(self._bad_link_type_error(link_type))

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
            requested_cols_norm: Union[str, set[str]] = set()
        elif isinstance(requested_cols, str):
            if requested_cols.strip().lower() == "all":
                requested_cols_norm = "all"
            else:
                raise TypeError(f"requested_cols must be 'all' or an iterable; got: {requested_cols!r}")
        else:
            requested_cols_norm = {str(x).strip().lower() for x in requested_cols}

        def requested_col_enabled(name: str) -> bool:
            """
            Check the enclosing normalized selection for one optional metadata column.

            Example:
                A normalized all selection enables both type and priority.


            :param name: Optional column suffix to look up.
            :return: Whether all or the selected suffix set enables the name.
            """
            return requested_cols_norm == "all" or (
                isinstance(requested_cols_norm, set) and name in requested_cols_norm
            )

        decrement_requested_cols = deepcopy(requested_cols_norm)

        if decrement_requested_cols == "all":

            # "all" includes the full standard metadata surface for link tables.
            link_rows = """
                CREATE TABLE IF NOT EXISTS `{0}s`(
                  `{0}_id` INTEGER PRIMARY KEY ,
                  `{0}_{1}_id` INTEGER {3},
                  `{0}_{2}_id` INTEGER {3},
                  `{0}_priority` INTEGER DEFAULT 0,
                  `{0}_primary` INTEGER NULL DEFAULT 0,
                  `{0}_type` TEXT NULL,
                  `{0}_origin` TEXT NULL,
                  `{0}_source` TEXT NULL,
                  `{0}_policy` TEXT NULL,
                  `{0}_data` TEXT NULL,
                  `{0}_index` TEXT NULL,
                  `{0}_sequence_number` INTEGER NULL,
                  `{0}_is_required` INTEGER DEFAULT 1,
                  `{0}_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),
                  `{0}_scratch` TEXT NULL 
                  """

            # NOTE: Link tables are constructed via a "blank row then fill" workflow in DriverWrapper.get_blank_row.
            # Any NOT NULL constraints on optional metadata columns (like `type`) break that workflow.
            # We therefore keep `{0}_type` nullable even for role-style many_many_non_exclusive tables, and rely on
            # UNIQUE(..., type) semantics (SQLite treats NULL as distinct) to preserve non-exclusive behaviour.

        else:

            assert isinstance(decrement_requested_cols, set)

            # Support "nullable" sentinel (documented in TOML) but it does not create a physical column.
            if "nullable" in decrement_requested_cols:
                decrement_requested_cols.remove("nullable")

            link_rows_header = """
                CREATE TABLE IF NOT EXISTS `{0}s`(
                  `{0}_id` INTEGER PRIMARY KEY ,
                  `{0}_{1}_id` INTEGER {3},
                  `{0}_{2}_id` INTEGER {3},"""

            if "priority" in decrement_requested_cols:
                link_rows_header += "\n      `{0}_priority` INTEGER DEFAULT 0,"
                decrement_requested_cols.remove("priority")

            if "primary" in decrement_requested_cols:
                link_rows_header += "\n      `{0}_primary` INTEGER NULL DEFAULT 0,"
                decrement_requested_cols.remove("primary")

            if "type" in decrement_requested_cols:
                # Keep `type` nullable (see NOTE above).
                link_rows_header += "\n      `{0}_type` TEXT NULL,"
                decrement_requested_cols.remove("type")

            if "origin" in decrement_requested_cols:
                link_rows_header += "\n      `{0}_origin` TEXT NULL,"
                decrement_requested_cols.remove("origin")

            # `source` is a standard relation-link provenance column on every generated link table.
            if "source" in decrement_requested_cols:
                decrement_requested_cols.remove("source")

            if "policy" in decrement_requested_cols:
                link_rows_header += "\n      `{0}_policy` TEXT NULL,"
                decrement_requested_cols.remove("policy")

            if "data" in decrement_requested_cols:
                link_rows_header += "\n      `{0}_data` TEXT NULL,"
                decrement_requested_cols.remove("data")

            if "index" in decrement_requested_cols:
                link_rows_header += "\n      `{0}_index` TEXT NULL,"
                decrement_requested_cols.remove("index")

            if "sequence_number" in decrement_requested_cols:
                link_rows_header += "\n      `{0}_sequence_number` INTEGER NULL,"
                decrement_requested_cols.remove("sequence_number")

            if "is_required" in decrement_requested_cols:
                link_rows_header += "\n      `{0}_is_required` INTEGER DEFAULT 1,"
                decrement_requested_cols.remove("is_required")

            # Any remaining requested columns are treated as bespoke TEXT columns, provided they are safe SQL identifiers.
            for extra_col in sorted(decrement_requested_cols):
                assert re.match(
                    r"^[a-z][a-z0-9_]*$",
                    extra_col,
                ), f"Unsafe requested column name {extra_col!r} for link table {table_name!r}"
                link_rows_header += f"\n      `{{0}}_{extra_col}` TEXT NULL,"

            decrement_requested_cols.clear()

            link_table_footer = """
                  `{0}_source` TEXT NULL,
                  `{0}_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),
                  `{0}_scratch` TEXT NULL"""

            link_rows = link_rows_header + link_table_footer

        # The full statement will be constructed with a join later - do not want a comma between the comment and the
        # start of the actual table
        # NOTE: Interlink rows are often created as "blank" placeholders and then populated.
        # Enforcing NOT NULL on FK columns prevents this workflow, so we keep FKs nullable for now.
        # (We can revisit strict FK NOT NULL constraints once row construction is not placeholder-based.)
        fk_null_sql = "NULL"

        link_rows = comment_row + link_rows.format(column_name, table1_l_s, table2_l_s, fk_null_sql)
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

            # Some link tables carry a `type` column. For strict many-many mappings we still
            # enforce uniqueness on just the pair (A,B). For role-style mappings, use
            # many_many_non_exclusive which uses (A,B,type) to allow multiple roles.
            has_type_col = (
                requested_cols_norm == "all"
                or (isinstance(requested_cols_norm, set) and "type" in requested_cols_norm)
            )

            if link_type == "many_many":

                many_many_restrictions_list = []

                # Strict many-to-many: the same pair cannot be linked twice (even if a type column exists).
                many_many_restriction = (
                    "\n    CONSTRAINT `{0}_and_{1}_non_repeating_many_many_mapping`\n"
                    "      UNIQUE ({2}_{1}_id, {2}_{0}_id)".format(table1_l_s, table2_l_s, column_name)
                )
                many_many_restrictions_list.append(many_many_restriction)

                # If we have a priority column then ensure ordering on the secondary.
                if requested_col_enabled("priority"):
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

                # Role-style many-to-many: allow the same pair multiple times as long as `type` differs.
                # SQLite UNIQUE treats NULL as distinct, so multiple NULL types are permitted.
                if has_type_col:
                    many_many_ne_restriction = (
                        "\n    CONSTRAINT `{0}_and_{1}_non_repeating_many_many_non_exclusive_mapping`\n"
                        "      UNIQUE ({2}_{1}_id, {2}_{0}_id, {2}_type)".format(
                            table1_l_s,
                            table2_l_s,
                            column_name,
                        )
                    )
                else:
                    # Without a type column, fall back to strict uniqueness on the pair.
                    many_many_ne_restriction = (
                        "\n    CONSTRAINT `{0}_and_{1}_non_repeating_many_many_non_exclusive_mapping`\n"
                        "      UNIQUE ({2}_{1}_id, {2}_{0}_id)".format(
                            table1_l_s,
                            table2_l_s,
                            column_name,
                        )
                    )
                many_many_ne_restrictions_list.append(many_many_ne_restriction)

                if requested_col_enabled("priority"):

                    if has_type_col:
                        m_t_m_ordering = (
                            "\n    CONSTRAINT `{0}_well_ordered_on_secondary_{1}`\n"
                            "UNIQUE ({2}_{3}_id, {2}_type, {2}_priority)".format(
                                primary_table,
                                secondary_table,
                                column_name,
                                primary_table_row_name,
                            )
                        )
                    else:
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

                if requested_col_enabled("priority"):

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

                if requested_col_enabled("priority"):
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
            if requested_col_enabled("type") and allowed_types is not None:

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

        if requested_col_enabled("sequence_number"):
            sequence_index_stmt = (
                "CREATE UNIQUE INDEX IF NOT EXISTS {2}_{0}_sequence_idx ON {2}s ({2}_{0}_id, {2}_sequence_number);"
            ).format(
                table1_l_s,
                table2_l_s,
                column_name,
            )
            full_script.append(sequence_index_stmt)

        return full_script, table_name

    def _bad_link_type_error(self, link_type: str) -> str:
        """
        Describe an unsupported cardinality and the host allowed-link set.

        Example:
            An unknown link_type is included in the multiline NotImplementedError detail.


        :param link_type: Supported cardinality name from allowed_link_types.
        :return: Diagnostic text; set display order is not guaranteed.
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
            requested_cols: Optional[Union[str, Iterable[str]]] = None,
            allowed_types: Optional[Iterable[str]] = None,
            nullable_fks: bool = True,
    ) -> list[str]:
        """
        Resolve a required legacy constraint entry and generate the matching link script.

        A string supplies raw constraints; a mapping supplies primary/secondary/link_type.
        When type is requested, optional allowed labels can fall back to the legacy mapping.
        Missing constraints fail by assertion; unexpected constraint representations raise
        NotImplementedError.

        Example:
            An INTERLINK_TABLE_CONSTRAINTS mapping can select one_many for the generated link.


        :param table1: First table whose singular prefix participates in the link name.
        :param table2: Second table whose singular prefix participates in the link name.
        :param requested_cols: all, None, or iterable of optional metadata suffixes.
        :param allowed_types: Optional iterable of permitted type labels.
        :param nullable_fks: Requested FK nullability; interlink generation currently always emits nullable FKs.
        :return: Generated statement list without executing it.
        """

        table_name, _ = self.get_interlink_table_name(table1, table2)

        sql_override_restriction = None
        if table_name in self.INTERLINK_TABLE_CONSTRAINTS:
            sql_override_restriction = self.INTERLINK_TABLE_CONSTRAINTS[table_name]
        assert sql_override_restriction is not None, "This should not be none - {}".format(table_name)

        # If a type column is requested, callers may optionally provide an explicit allowed-types
        # set. If they don't, we fall back to any legacy mapping present; otherwise the type
        # column remains free-form text (no allowed-types table will be built).
        if allowed_types is None:
            try:
                if requested_cols == "all" or (requested_cols is not None and "type" in requested_cols):
                    allowed_types = self.ALLOWED_INTERLINK_TYPE_VAL_DICT.get(table_name)
            except TypeError:
                # requested_cols might be a non-iterable sentinel; ignore
                allowed_types = self.ALLOWED_INTERLINK_TYPE_VAL_DICT.get(table_name)

        allowed_link_types = allowed_types

        # We have a simple string to use instead of any generated restrictions - just use that
        # Todo: Eventually should be able to pull this out - it's becoming increasingly redundant
        if sql_override_restriction is None or isinstance(sql_override_restriction, basestring):
            full_script, table_name = self.direct_get_direct_link_main_tables_sql(
                primary_table=table1,
                secondary_table=table2,
                requested_cols=requested_cols,
                allowed_types=allowed_link_types,
                nullable_fks=nullable_fks,
                override_restriction_sql=sql_override_restriction,
            )

        # We have a dictionary which characterises the link - use that
        elif isinstance(sql_override_restriction, dict):

            primary_table = sql_override_restriction["primary"]
            secondary_table = sql_override_restriction["secondary"]
            link_tyoe = sql_override_restriction["link_type"]

            full_script, table_name = self.direct_get_direct_link_main_tables_sql(
                primary_table=primary_table,
                secondary_table=secondary_table,
                link_type=link_tyoe,
                requested_cols=requested_cols,
                allowed_types=allowed_link_types,
                nullable_fks=nullable_fks,
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


    def build_allowed_types_table_interlink(
            self,
            for_table: str,
            allowed_types: Optional[Iterable[str]] = None
    ) -> list[str]:
        """
        Return no statements for None, otherwise build a legacy allowed-type table.

        Example:
            ``build_allowed_types_table_interlink(name, None)`` returns an empty list.


        :param for_table: Table whose allowed-type lookup name is derived.
        :param allowed_types: Optional iterable of permitted type labels.
        :return: DDL/insert statement list, or an empty list.
        """
        if allowed_types is None:
            return []

        return self._build_allowed_types_table_interlink(for_table, allowed_types)

    def _build_allowed_types_table_interlink(self, for_table, allowed_types):
        """
        Generate a legacy allowed-type table and one unescaped insert per label.

        The type column is unique and nullable. Inputs are interpolated into SQL, including
        double-quoted seed values; use trusted labels and do not assume repeat seeding is safe.

        Example:
            Two labels produce one CREATE TABLE statement followed by two INSERT statements.


        :param for_table: Table whose allowed-type lookup name is derived.
        :param allowed_types: Optional iterable of permitted type labels.
        :return: Ordered DDL and seed statements.
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


    # ---------------------------------------------------------------------
    # FRBR generator helpers
    #
    # The FRBR database generator prefers a `{link_table}__types` reference
    # table (extensible) plus triggers, rather than a strict FK into an
    # `allowed_types__*` table. Centralise that logic here so that link-table
    # related SQL is produced in one place.
    # ---------------------------------------------------------------------

    def direct_create_interlink_types_reference_table(
        self,
        interlink_table_name: str,
        interlink_column_base: str,
        allowed_types: list[str],
        # Todo: This should probably be a protocol
        connection: sqlite3.Connection,
    ) -> None:
        """
        Create/seed a __types table, install nonnull-type guard triggers and commit.

        Seed labels are bound and inserted if absent. Insert/update guards reject nonnull
        values missing from the reference table; NULL remains allowed. Existing guards
        are retained by IF NOT EXISTS.

        Example:
            Adding a label to the reference table extends the accepted nonnull link types.


        :param interlink_table_name: Trusted existing link-table name.
        :param interlink_column_base: Trusted singular prefix of its type column.
        :param allowed_types: Optional iterable of permitted type labels.
        :param connection: Open SQLite connection; retained rather than closed here.
        :return: None; mutates and commits the caller connection.
        """

        conn = connection
        c = conn.cursor()

        types_table = f"{interlink_table_name}__types"

        c.execute(
            f"""
            CREATE TABLE IF NOT EXISTS `{types_table}` (
              `type` TEXT PRIMARY KEY
            );
            """
        )

        for t in allowed_types:
            c.execute(f"INSERT OR IGNORE INTO `{types_table}` (`type`) VALUES (?);", (t,))

        link_type_col = f"{interlink_column_base}_type"

        trig_insert = f"{interlink_table_name}__type_guard_insert"
        trig_update = f"{interlink_table_name}__type_guard_update"

        c.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS `{trig_insert}`
            BEFORE INSERT ON `{interlink_table_name}`
            FOR EACH ROW
            WHEN NEW.`{link_type_col}` IS NOT NULL
             AND NOT EXISTS (SELECT 1 FROM `{types_table}` WHERE `type` = NEW.`{link_type_col}`)
            BEGIN
              SELECT RAISE(ABORT, 'Invalid link type (not in {types_table}).');
            END;
            """
        )

        c.execute(
            f"""
            CREATE TRIGGER IF NOT EXISTS `{trig_update}`
            BEFORE UPDATE OF `{link_type_col}` ON `{interlink_table_name}`
            FOR EACH ROW
            WHEN NEW.`{link_type_col}` IS NOT NULL
             AND NOT EXISTS (SELECT 1 FROM `{types_table}` WHERE `type` = NEW.`{link_type_col}`)
            BEGIN
              SELECT RAISE(ABORT, 'Invalid link type (not in {types_table}).');
            END;
            """
        )

        conn.commit()

    def direct_build_allowed_types_table_intralink(
        self,
        for_table: str,
        allowed_types: Optional[Iterable[str]] = None,
    ) -> list[str]:
        """
        Build a legacy intralink type table using explicit labels or stored defaults.

        Assert membership when the host exposes intralink_tables. Falsy resolved labels
        produce no statements; interpolated seed labels are not escaped.

        Example:
            A main table without configured allowed types returns an empty statement list.


        :param for_table: Table whose allowed-type lookup name is derived.
        :param allowed_types: Optional iterable of permitted type labels.
        :return: DDL/seed statements or an empty list.
        """

        # Preserve historical generator behaviour: intralink allowed-types are only
        # meaningful for tables explicitly requested for intralinks.
        intralink_tables = getattr(self, "intralink_tables", None)
        if intralink_tables is not None:
            assert for_table in intralink_tables, f"Unknown intralink main table: {for_table!r}"

        if allowed_types is None:
            allowed_types = getattr(self, "intralink_allowed_types_by_table", {}).get(for_table)
        if not allowed_types:
            return []

        allowed_table_name = self.get_allowed_types_table_name_intralinks(for_table)
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

        att_add_sqlite: list[str] = []
        for at in allowed_types:
            at_insert_stmt = 'INSERT INTO {table} ({column}_type) VALUES ("{at}");'.format(
                table=allowed_table_name, column=allowed_table_col_name, at=at
            )
            att_add_sqlite.append(at_insert_stmt)

        return [att_table_sqlite] + att_add_sqlite

    def direct_build_intralink_table_sql(
        self,
        name: str,
        allowed_types: Optional[Iterable[str]] = None,
        requested_cols: Optional[Union[str, Iterable[str], set[str]]] = None,
        index_both: bool = True,
        nullable_fks: bool = True,
        symmetric: bool = False,
        symmetric_types: Optional[Iterable[str]] = None,
        use_reference_types_table: bool = False,
    ) -> list[str]:
        """
        Generate self-link DDL, indexes and optional canonical-order guard triggers.

        None selects type; allowed labels force a type column. Always include source and
        reject nonnull self-edges. Symmetric guards reject reversed endpoints rather than
        swapping them; symmetric_types restricts that guard even when symmetric is true.
        Reference-type mode omits the FK and requires later guard installation. Explicit
        sequence_number/is_required subsets currently emit those columns twice because
        the bespoke-column filter also includes them; all avoids that duplication.

        Example:
            A symmetric work self-link accepts primary_id below secondary_id and rejects
            the reverse ordering when both endpoints are nonnull.


        :param name: Main-table name, optionally resolved by the host match_to_table_name hook.
        :param allowed_types: Optional iterable of permitted type labels.
        :param requested_cols: all, None, or iterable of optional metadata suffixes.
        :param index_both: Whether to index both endpoint foreign-key columns.
        :param nullable_fks: Whether the two self-link foreign keys are nullable.
        :param symmetric: Whether to guard canonical ordering for all types when no subset is supplied.
        :param symmetric_types: Optional reusable type-label collection limiting canonical-order guards.
        :param use_reference_types_table: Whether a separately installed __types guard replaces legacy allowed-type FKs.
        :return: Ordered statements; no SQL is executed.
        """

        name_local = deepcopy(name)
        name_local = six_unicode(name_local)

        target_table_name = getattr(self, "match_to_table_name", lambda x: x)(name_local)
        if target_table_name is None:
            target_table_name = name_local

        target_row_name = plural_singular_mapper(target_table_name)
        row_name = f"{target_row_name}_{target_row_name}_intralink"
        table_name = f"{row_name}s"

        # Normalise requested columns.
        if requested_cols is None:
            # Historical behaviour: always include `type` for intralinks.
            req: Union[str, set[str]] = {"type"}
        elif isinstance(requested_cols, str):
            if requested_cols.strip().lower() == "all":
                req = "all"
            else:
                raise TypeError(f"requested_cols must be 'all' or an iterable; got: {requested_cols!r}")
        else:
            req = {str(x).strip().lower() for x in requested_cols}

        # If allowed types were requested, ensure we have a `type` column.
        if allowed_types and req != "all":
            req.add("type")

        # Support "nullable" sentinel (documented in TOML) but it does not create a physical column.
        if req != "all" and "nullable" in req:
            req.remove("nullable")

        # If symmetric_types is used we must have a type column.
        if symmetric_types:
            if req != "all" and "type" not in req:
                raise ValueError("symmetric_types requires a `type` requested column in the intralink table")

        # Decide FK nullability.
        fk_null_sql = "NULL" if nullable_fks else "NOT NULL"

        # Optional allowed-types table (legacy mode) OR types reference tables (FRBR mode).
        # In FRBR mode we create `{table}__types` via `direct_create_interlink_types_reference_table` at runtime,
        # so do not emit an FK here.
        allowed_type_table_sqlite: list[str] = []
        at_foreign_key = ""
        if (allowed_types is not None) and (not use_reference_types_table):
            allowed_type_table_sqlite = self.direct_build_allowed_types_table_intralink(
                target_table_name, allowed_types=allowed_types
            )
            if allowed_type_table_sqlite:
                att_name = self.get_allowed_types_table_name_intralinks(target_table_name)
                att_col_name = att_name[:-1]
                at_foreign_key = f"""
      CONSTRAINT `{att_name}_type_is_allowed`
        FOREIGN KEY (`{row_name}_type`)
        REFERENCES `{att_name}` (`{att_col_name}_type`),
"""

        # Column builder
        col_lines: list[str] = [
            f"  `{row_name}_id` INTEGER PRIMARY KEY ,",
            f"  `{row_name}_primary_id` INTEGER {fk_null_sql},",
            f"  `{row_name}_secondary_id` INTEGER {fk_null_sql},",
        ]

        # Interlink-style optional columns
        def _add_optional(col: str, ddl: str) -> None:
            """
            Append one comma-terminated optional column to the enclosing DDL list.

            Example:
                The priority suffix appends a prefixed INTEGER DEFAULT 0 column.


            :param col: Trusted column suffix appended to the enclosing row_name.
            :param ddl: Trusted SQL type/default fragment for the column.
            :return: None; mutates the enclosing col_lines list.
            """
            col_lines.append(f"  `{row_name}_{col}` {ddl},")

        if req == "all" or (req != "all" and "priority" in req):
            _add_optional("priority", "INTEGER DEFAULT 0")
        if req == "all" or (req != "all" and "primary" in req):
            _add_optional("primary", "INTEGER NULL DEFAULT 0")
        if req == "all" or (req != "all" and "type" in req):
            _add_optional("type", "TEXT NULL")
        if req == "all" or (req != "all" and "index" in req):
            _add_optional("index", "TEXT NULL")
        if req == "all" or (req != "all" and "sequence_number" in req):
            _add_optional("sequence_number", "INTEGER NULL")
        if req == "all" or (req != "all" and "is_required" in req):
            _add_optional("is_required", "INTEGER DEFAULT 1")
        if req == "all" or (req != "all" and "origin" in req):
            _add_optional("origin", "TEXT NULL")
        if req == "all" or (req != "all" and "policy" in req):
            _add_optional("policy", "TEXT NULL")
        if req == "all" or (req != "all" and "data" in req):
            _add_optional("data", "TEXT NULL")
        _add_optional("source", "TEXT NULL")

        # Bespoke safe columns (TEXT NULL)
        if req != "all":
            reserved = {
                "priority",
                "primary",
                "type",
                "index",
                "origin",
                "source",
                "policy",
                "data",
            }
            for extra in sorted(req - reserved):
                if not re.fullmatch(r"[a-z][a-z0-9_]*", extra):
                    raise TypeError(f"Unsafe intralink requested column name: {extra!r}")
                _add_optional(extra, "TEXT NULL")

        # Standard tail columns
        col_lines.append(f"  `{row_name}_datestamp` DATETIME DEFAULT (STRFTIME('%s', 'now')),")
        col_lines.append(f"  `{row_name}_scratch` TEXT NULL,")

        # Constraints
        constraint_lines: list[str] = []

        # Always disallow self-self edges when both ends are non-null.
        constraint_lines.append(
            f"  CONSTRAINT `{row_name}_no_self_link` CHECK (`{row_name}_primary_id` IS NULL OR `{row_name}_secondary_id` IS NULL OR `{row_name}_primary_id` != `{row_name}_secondary_id`),"
        )

        has_type_col = (req == "all") or (req != "all" and "type" in req)

        # Uniqueness on (primary,secondary[,type])
        if has_type_col:
            constraint_lines.append(
                f"  CONSTRAINT `{row_name}_pair_unique_with_type` UNIQUE (`{row_name}_primary_id`, `{row_name}_secondary_id`, `{row_name}_type`),"
            )
        else:
            constraint_lines.append(
                f"  CONSTRAINT `{row_name}_pair_unique` UNIQUE (`{row_name}_primary_id`, `{row_name}_secondary_id`),"
            )

        # If priority exists, enforce ordering uniqueness per primary (and per type if present).
        has_priority = (req == "all") or (req != "all" and "priority" in req)
        if has_priority:
            if has_type_col:
                constraint_lines.append(
                    f"  CONSTRAINT `{row_name}_well_ordered` UNIQUE (`{row_name}_primary_id`, `{row_name}_type`, `{row_name}_priority`),"
                )
            else:
                constraint_lines.append(
                    f"  CONSTRAINT `{row_name}_well_ordered` UNIQUE (`{row_name}_primary_id`, `{row_name}_priority`),"
                )

        # Foreign keys to the base table.
        constraint_lines.append(
            f"""
  CONSTRAINT `{row_name}_primary_id_fk`
    FOREIGN KEY (`{row_name}_primary_id`)
    REFERENCES `{target_table_name}` (`{target_row_name}_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,""".rstrip()
        )
        constraint_lines.append(
            f"""
  CONSTRAINT `{row_name}_secondary_id_fk`
    FOREIGN KEY (`{row_name}_secondary_id`)
    REFERENCES `{target_table_name}` (`{target_row_name}_id`)
    ON DELETE CASCADE
    ON UPDATE CASCADE""".rstrip()
        )

        # Assemble CREATE TABLE
        columns_sql = "\n".join(col_lines)
        constraints_sql = "\n".join(constraint_lines)

        create_stmt = f"""

-- -----------------------------------------------------
-- Table `{table_name}`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `{table_name}` (
{columns_sql}
{at_foreign_key}{constraints_sql}
);
""".strip("\n")

        stmts: list[str] = []
        stmts.extend(allowed_type_table_sqlite)
        stmts.append(create_stmt)

        # Indexing (optional, mirrors interlink behaviour).
        if index_both:
            stmts.append(
                f"CREATE INDEX IF NOT EXISTS `{table_name}__primary_idx` ON `{table_name}` (`{row_name}_primary_id`);"
            )
            stmts.append(
                f"CREATE INDEX IF NOT EXISTS `{table_name}__secondary_idx` ON `{table_name}` (`{row_name}_secondary_id`);"
            )

        # Symmetric ordering enforcement: cannot auto-swap in SQLite triggers, so reject out-of-order inserts/updates.
        if symmetric or symmetric_types:
            if symmetric_types:
                types_list = [str(t) for t in symmetric_types]
                in_list = ", ".join(["'" + t.replace("'", "''") + "'" for t in types_list])
                type_pred = f"NEW.`{row_name}_type` IN ({in_list})"
                label = " (symmetric_types)"
            else:
                type_pred = "1=1"
                label = ""

            trig_ins = f"{table_name}__symmetric_order_guard_insert"
            trig_upd = f"{table_name}__symmetric_order_guard_update"

            cond = (
                f"""WHEN {type_pred}
 AND NEW.`{row_name}_primary_id` IS NOT NULL
 AND NEW.`{row_name}_secondary_id` IS NOT NULL
 AND NEW.`{row_name}_primary_id` >= NEW.`{row_name}_secondary_id`"""
            )

            stmts.append(
                f"""CREATE TRIGGER IF NOT EXISTS `{trig_ins}`
BEFORE INSERT ON `{table_name}`
FOR EACH ROW
{cond}
BEGIN
  SELECT RAISE(ABORT, 'Symmetric intralink requires primary_id < secondary_id{label}.');
END;"""
            )

            stmts.append(
                f"""CREATE TRIGGER IF NOT EXISTS `{trig_upd}`
BEFORE UPDATE OF `{row_name}_primary_id`, `{row_name}_secondary_id`, `{row_name}_type` ON `{table_name}`
FOR EACH ROW
{cond}
BEGIN
  SELECT RAISE(ABORT, 'Symmetric intralink requires primary_id < secondary_id{label}.');
END;"""
            )

        return stmts
