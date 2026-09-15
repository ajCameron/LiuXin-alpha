
"""
Create, query and delete directed relationships within one database table.

These facade helpers distinguish link Rows from endpoint Rows and depend on wrapper naming and schema rules. Mutations are incremental; historical return/deletion defects are documented where they occur rather than silently changing behavior.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional

from LiuXin_alpha.errors import InputIntegrityError, DatabaseIntegrityError

from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

from LiuXin_alpha.databases.row import Row

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.database_api import DatabaseAPI
    from LiuXin_alpha.databases.api.row_api import RowAPI, IntralinkRowAPI


class DatabaseIntralinkRowsMixin:
    """
    Add self-link operations to a facade with Row factories and schema helpers.

    Link types are normalized on creation and may be restricted by preferences as well as database constraints. Lookup direction is significant; methods do not automatically reverse symmetric relationships.

    Example:
        For two existing Rows from a self-link-capable table, db.intralink_rows(first, second, link_type="related") creates the directed relationship if the configured type is allowed.
    """

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO WRITE TO INTRALINK TABLES START HERE

    # Todo: Need to extend to account for the other interlink data types
    def intralink_rows(
            self: "DatabaseAPI",
            primary_row: "RowAPI",
            secondary_row: "RowAPI",
            link_type: str) -> "IntralinkRowAPI":
        """
        Create and synchronize a typed directed link between rows in the same table.

        Validate equal table names and IDs, then consult allowed_<table>_intralink_types preferences if present. Resolve endpoint/type columns, allocate an ID and sync. Later schema constraints still apply; failed synchronization has no explicit cleanup of an allocated placeholder.

        Example:
            With related permitted for the table, link = db.intralink_rows(first, second, " RELATED ") stores the normalized type related.


        :param primary_row: Primary endpoint with a non-None row ID.
        :param secondary_row: Secondary endpoint in the same table, also with an ID.
        :param link_type: Type converted to text, stripped and lowercased before validation/storage.
        :return: Persisted generic Row for the self-link.
        :raises InputIntegrityError: Tables differ, an endpoint ID is missing, or preferences reject the normalized type.
        """
        link_type = six_unicode(link_type).lower().strip()
        if not primary_row.table == secondary_row.table:
            err_str = "Cannot intralink rows from different table types"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("primary_row", primary_row),
                ("secondary_row", secondary_row),
                ("link_type", link_type),
            )
            raise InputIntegrityError(err_str)
        table = primary_row.table

        if primary_row.row_id is None or secondary_row.row_id is None:
            err_str = "Both rows must have ids set before they can be linked."
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("primary_row", primary_row),
                ("secondary_row", secondary_row),
                ("link_type", link_type),
            )
            raise InputIntegrityError(err_str)

        # Checks that the intralink type is one of those allowed for this table in preferences
        # Todo: Move this to init for performance
        allowed_types_name = "allowed_{0}_intralink_types".format(primary_row.table)
        try:
            allowed_link_types = self.preferences[allowed_types_name]
        except KeyError:
            info_str = "Allowed type name not found in preferences - no restrictions applied to intralink type"
            default_log.log_variables(info_str, "INFO", ("allowed_type_name", allowed_types_name))
        else:
            allowed_link_types = frozenset([six_unicode(lt).lower().strip() for lt in allowed_link_types])
            if link_type not in allowed_link_types:
                err_str = "Unable to intralink rows - link type not recognized"
                err_str = default_log.log_variables(
                    err_str,
                    "ERROR",
                    ("primary_row", primary_row),
                    ("secondary_row", secondary_row),
                    ("link_type", link_type),
                    ("allowed_link_types", allowed_link_types),
                )
                raise InputIntegrityError(err_str)

        intralink_row = dict()
        primary_col = self.driver_wrapper.get_intralink_column(table, "primary_id")
        secondary_col = self.driver_wrapper.get_intralink_column(table, "secondary_id")
        type_col = self.driver_wrapper.get_intralink_column(table, "type")
        intralink_row[primary_col] = primary_row.row_id
        intralink_row[secondary_col] = secondary_row.row_id
        intralink_row[type_col] = link_type

        intralink_row = Row(row_dict=intralink_row, database=self)
        intralink_row.ensure_row_has_id()
        intralink_row.sync()

        return intralink_row

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO READ INTRALINKED ROWS START HERE

    def get_intralink_row(
            self: "DatabaseAPI",
            primary_row: "RowAPI",
            secondary_row: "RowAPI") -> Optional["IntralinkRowAPI"]:
        """
        Find the unique directed self-link for an endpoint pair.

        Validate that a self-link table exists, search by primary ID, then compare secondary IDs as text. Type is not filtered, so differently typed matches can be ambiguous.

        Example:
            For same-table Rows, db.get_intralink_row(first, second) looks only in the first-to-second direction.


        :param primary_row: Primary endpoint.
        :param secondary_row: Secondary endpoint from the same table.
        :return: Matching link Row, or None when absent.
        :raises InputIntegrityError: Tables differ or the self-link table is unavailable.
        :raises DatabaseIntegrityError: More than one link matches the directed pair.
        """
        primary_table = primary_row.table
        secondary_table = secondary_row.table

        link_table_name = self.driver_wrapper.get_link_table_name(primary_table, secondary_table)
        if not link_table_name or (primary_table != secondary_table):
            err_str = "Given tables cannot be connected - or you have used an interlink method, not the intralink one"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("primary_row", primary_row),
                ("secondary_row", secondary_row),
                ("link_table_name", link_table_name),
            )
            raise InputIntegrityError(err_str)

        primary_id_col = self.driver_wrapper.get_link_column(primary_table, primary_table, "primary_id")
        secondary_id_col = self.driver_wrapper.get_link_column(primary_table, primary_table, "secondary_id")

        candidate_rows = []
        # Search the table using the primary_id - refine using the secondary to return the actually desired result
        primary_id = six_unicode(primary_row.row_id)
        secondary_id = six_unicode(secondary_row.row_id)
        for row in self.search(table=link_table_name, column=primary_id_col, search_term=primary_id):
            if secondary_id == six_unicode(row[secondary_id_col]):
                candidate_rows.append(row)

        if len(candidate_rows) == 0:
            return None

        elif len(candidate_rows) == 1:
            return candidate_rows[0]

        err_str = "Rows are joined by more than one intralink row - which shouldn't happen."
        err_str = default_log.log_variables(
            err_str,
            "ERROR",
            ("candidate_rows", candidate_rows),
            ("primary_row", primary_row),
            ("secondary_row", secondary_row),
        )
        raise DatabaseIntegrityError(err_str)

    def get_intralink_rows(
            self: "DatabaseAPI",
            row: "RowAPI",
            primary: bool = True,
            secondary: bool = True,
            link_type_filter: Optional[str] = None) -> list["RowAPI"]:
        """
        Collect self-link Rows mentioning a seed in either endpoint column.

        There is no deduplication or priority sort; a self-link can appear twice when both directions are requested. With both direction flags false the result is empty, although schema columns are still resolved.

        Example:
            For a seed Row, db.get_intralink_rows(seed, primary=False, secondary=True) returns incoming self-links.


        :param row: Seed Row.
        :param primary: Include relationships where the seed is primary.
        :param secondary: Include relationships where the seed is secondary.
        :param link_type_filter: Optional type compared as text without stripping or lowercasing.
        :return: Link Row list, primary-query results followed by secondary-query results.
        """
        table = row.table
        row_id = six_unicode(row.row_id)

        intralink_table = self.driver_wrapper.get_link_table_name(table, table)
        intralink_table_primary_row = self.driver_wrapper.get_link_column(table, table, "primary_id")
        intralink_table_secondary_row = self.driver_wrapper.get_link_column(table, table, "secondary_id")

        row_pool = []
        # Search the intralink table for mentions of the id in the primary column
        if primary:
            primary_intralink_rows = self.search(
                table=intralink_table,
                column=intralink_table_primary_row,
                search_term=row_id,
            )
            row_pool.extend([r for r in primary_intralink_rows])

        # Search the intralink table for mentions of the id in the secondary column
        if secondary:
            secondary_intralink_rows = self.search(
                table=intralink_table,
                column=intralink_table_secondary_row,
                search_term=row_id,
            )
            row_pool.extend([r for r in secondary_intralink_rows])

        if link_type_filter is None:
            return row_pool
        else:
            intralink_table_link_type = self.driver_wrapper.get_link_column(table, table, "type")
            filtered_row_pool = [
                r for r in row_pool if six_unicode(r[intralink_table_link_type]) == six_unicode(link_type_filter)
            ]
            return filtered_row_pool

    def get_intralinked_rows(
            self: "DatabaseAPI",
            primary_row: "RowAPI",
            secondary_row: "RowAPI") -> Optional[list["RowAPI"]]:
        """
        Search one self-link direction while retaining the legacy link-row return value.

        Exactly one seed must be non-None. The method still loads opposite endpoint Rows before returning link Rows, so endpoint-loading errors can propagate without contributing returned values.

        Example:
            links = db.get_intralinked_rows(primary_row=seed, secondary_row=None) currently returns outgoing relationship records, not their target records.


        :param primary_row: Outgoing seed, or None when using secondary_row.
        :param secondary_row: Incoming seed, or None when using primary_row.
        :return: Matching link Rows; despite the method name, the separately loaded endpoint Rows are discarded.
        :raises InputIntegrityError: Both seeds are supplied or both are None.
        """
        if primary_row is not None and secondary_row is not None:
            err_str = "You seem to have both the title rows that you could want - do you want the intralink row itself?"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("primary_row", primary_row),
                ("secondary_row", secondary_row),
            )
            raise InputIntegrityError(err_str)
        if primary_row is None and secondary_row is None:
            err_str = "Both primary and secondary rows supplied to get_intralinked_rows where null"
            default_log.error(err_str)
            raise InputIntegrityError(err_str)

        # Get every row with a the primary_row_id as it's primary - return that
        if primary_row is not None:
            table = primary_row.table
            primary_row_id = six_unicode(primary_row.row_id)

            intralink_table = self.driver_wrapper.get_link_table_name(table, table)
            intralink_table_primary_row = self.driver_wrapper.get_link_column(table, table, "primary_id")
            intralink_table_secondary_row = self.driver_wrapper.get_link_column(table, table, "secondary_id")

            intralink_rows = self.search(
                table=intralink_table,
                column=intralink_table_primary_row,
                search_term=primary_row_id,
            )

            intralinked_rows = []
            for link_row in intralink_rows:
                secondary_id = link_row[intralink_table_secondary_row]
                intralinked_rows.append(self.get_row_from_id(table=table, row_id=secondary_id))
            return intralink_rows

        # Get every row with a the secondary_row_id as it's primary - return that
        elif secondary_row is not None:
            table = secondary_row.table
            secondary_row_id = six_unicode(secondary_row.row_id)

            intralink_table = self.driver_wrapper.get_link_table_name(table, table)
            intralink_table_primary_row = self.driver_wrapper.get_link_column(table, table, "primary_id")
            intralink_table_secondary_row = self.driver_wrapper.get_link_column(table, table, "secondary_id")

            intralink_rows = self.search(
                table=intralink_table,
                column=intralink_table_secondary_row,
                search_term=secondary_row_id,
            )

            intralinked_rows = []
            for link_row in intralink_rows:
                primary_id = link_row[intralink_table_primary_row]
                intralinked_rows.append(self.get_row_from_id(table=table, row_id=primary_id))
            return intralink_rows

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO DELETE INTRALINK ROWS START HERE

    # Todo: Consider renaming - unlink_intralink
    def unlinked_intralink(
            self: "DatabaseAPI",
            primary_row: "RowAPI",
            secondary_row: "RowAPI") -> None:
        """
        Delete a unique directed pair or every outgoing link from a primary seed.

        Both seeds select a unique pair; only a primary seed selects all outgoing links. The secondary-only branch incorrectly reads primary_row.table while primary_row is None and raises AttributeError. There is no grouped rollback for multi-row deletion.

        Example:
            For a valid seed, db.unlinked_intralink(primary_row=seed, secondary_row=None) removes its outgoing links and retains endpoint records.


        :param primary_row: Primary seed, or None for the legacy incoming-only branch.
        :param secondary_row: Secondary seed, or None to delete all outgoing links.
        :return: None; a missing explicitly requested pair is a no-op.
        :raises InputIntegrityError: Both seeds are None.
        :raises AttributeError: Only secondary_row is supplied, due to the legacy branch defect.
        :raises DatabaseIntegrityError: A requested pair has more than one link.
        """
        if primary_row is not None and secondary_row is not None:

            link_row = self.get_intralink_row(primary_row=primary_row, secondary_row=secondary_row)
            # Deal with the case where there is no link to remove
            if link_row is None:
                return
            self.delete(link_row)

        elif primary_row is not None and secondary_row is None:

            table = primary_row.table
            primary_id = primary_row.row_id

            # Search the intralink table for any rows with the given primary_id - delete them
            intralink_table = self.driver_wrapper.get_link_table_name(table1=table, table2=table)
            intralink_table_primary = self.driver_wrapper.get_link_column(
                table1=table, table2=table, column_type="primary_id"
            )
            link_rows = self.search(
                table=intralink_table,
                column=intralink_table_primary,
                search_term=primary_id,
            )

            [self.delete(l_r) for l_r in link_rows]

        elif primary_row is None and secondary_row is not None:

            table = primary_row.table
            secondary_id = secondary_row.row_id

            # Search the intralink table for any rows with the given primary_id - delete them
            intralink_table = self.driver_wrapper.get_link_table_name(table1=table, table2=table)
            intralink_table_primary = self.driver_wrapper.get_link_column(
                table1=table, table2=table, column_type="secondary_id"
            )
            link_rows = self.search(
                table=intralink_table,
                column=intralink_table_primary,
                search_term=secondary_id,
            )

            [self.delete(l_r) for l_r in link_rows]

        elif primary_row is None and secondary_row is None:

            err_str = "unlink_intralink called without content"
            default_log.error(err_str)
            raise InputIntegrityError(err_str)

    #
    # ----------------------------------------------------------------------------------------------------------------------
