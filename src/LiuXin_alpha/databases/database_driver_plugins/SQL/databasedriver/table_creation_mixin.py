
"""
Build conventionally named main tables through the driver's script execution helper.
"""

from __future__ import annotations

from typing import Optional, Iterable, Union

from LiuXin_alpha.utils.language_tools import plural_singular_mapper


class TableCreationMixin:
    """
    Generate main-table DDL with an ID, requested data columns, datestamp and scratch fields.

    Example:
        ``driver.direct_create_main_table("examples")`` creates the conventional default layout.
    """
    # Todo: Should be creation methods for all types of table
    # Todo: Central registyr on the database for table types
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - TABLE CREATION METHODS
    # Todo: Need a way to change the data type of the default column - also the data type of any additional columns created
    # Todo: Pull the "new" out of the name - that's implcit
    # Todo: Need a way to designate this new table "custom"
    def direct_create_main_table(
            self,
            table_name: str,
            column_headings: Optional[Iterable[str]] = None,
            index_on: Optional[Union[str, Iterable[str]]]="all",
            default_datatype: str = "TEXT",
            default_unique: bool = False,
    ) -> None:
        """
        Create a conventionally named main table and invalidate schema caches.

        With no headings, create one default data column and its index; only index_on="all" is implemented in that branch. Explicit headings must be a mapping to datatype specification mappings; that branch creates no indexes. default_unique is unused. Trusted names/types become SQL syntax; repeated default creation may fail on the existing index even though the table uses IF NOT EXISTS.

        Example:
            ``driver.direct_create_main_table("examples", {"name": {"datatype": "TEXT"}})`` creates an example_name column.


        :param table_name: Trusted plural table name; its singular form prefixes generated columns.
        :param column_headings: Optional suffix-to-specification mapping; each spec may contain a ``datatype`` key.
        :param index_on: Use ``all`` for the default layout; ignored for explicit column specifications.
        :param default_datatype: Trusted SQL type used for the default column or a spec missing datatype.
        :param default_unique: Accepted but currently unused; no uniqueness constraint is generated from it.
        :return: ``None``.
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

        self.direct_executescript("\n".join(full_script))

        self._zero_prop_cache()
