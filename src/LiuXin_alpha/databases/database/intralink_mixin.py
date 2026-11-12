"""
Create, query and delete directed relationships within one database table.

These facade helpers distinguish link Rows from endpoint Rows and depend on wrapper
naming and schema rules. Creation inserts a complete link atomically, and endpoint
queries return opposite endpoint Rows in either direction.
"""

from __future__ import annotations

from LiuXin_alpha.databases.database._state import DatabaseMixinState

from typing import TYPE_CHECKING, Optional, Any

from LiuXin_alpha.errors import InputIntegrityError, DatabaseIntegrityError

from LiuXin_alpha.utils.logging import default_log
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

from LiuXin_alpha.databases.row import Row

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.row_api import RowAPI


class DatabaseIntralinkRowsMixin(DatabaseMixinState):
    """
    Add self-link operations to a facade with Row factories and schema helpers.

    Link types are normalized on creation and may be restricted by preferences as well
    as database constraints. Lookup direction is significant; methods do not
    automatically reverse symmetric relationships.

    Example:
        For two existing Rows from a self-link-capable table, db.intralink_rows(first,
        second, link_type="related") creates the directed relationship if the configured
        type is allowed.
    """

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO WRITE TO INTRALINK TABLES START HERE

    # Todo: Need to extend to account for the other interlink data types
    def intralink_rows(
        self, primary_row: "RowAPI", secondary_row: "RowAPI", link_type: str
    ) -> "RowAPI":
        """Insert one complete directed relationship between rows in the same table.

        Normalize the type and enforce configured preference restrictions before
        inserting. The complete insert and readback share one macro transaction;
        constraint failures leave no placeholder. Nested calls use a savepoint.

        :param primary_row: Persisted outgoing endpoint.
        :param secondary_row: Persisted incoming endpoint in the same table.
        :param link_type: Type converted to text, stripped and lowercased.
        :return: Persisted relationship Row.
        :raises InputIntegrityError: Invalid endpoints or a preference-rejected type.
        :raises DatabaseIntegrityError: A database constraint rejects the relationship.
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
        table = self._row_table(primary_row)

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
            default_log.log_variables(
                info_str, "INFO", ("allowed_type_name", allowed_types_name)
            )
        else:
            allowed_link_types = frozenset(
                [six_unicode(lt).lower().strip() for lt in allowed_link_types]
            )
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

        payload: dict[str, Any] = {}
        primary_col = self._database.driver_wrapper.get_intralink_column(
            table, "primary_id"
        )
        secondary_col = self._database.driver_wrapper.get_intralink_column(
            table, "secondary_id"
        )
        type_col = self._database.driver_wrapper.get_intralink_column(table, "type")
        payload[primary_col] = primary_row.row_id
        payload[secondary_col] = secondary_row.row_id
        payload[type_col] = link_type

        intralink_table = self._database.driver_wrapper.get_link_table_name(
            table, table
        )
        if not intralink_table:
            raise InputIntegrityError(f"No intralink table for {table!r}")
        with self._write_transaction():
            row_id = self._database.macros.insert_row(intralink_table, payload)
            stored = self._database.macros.get_row(intralink_table, row_id)
            if stored is None:
                raise DatabaseIntegrityError(
                    "Inserted intralink could not be read back"
                )
            return Row(row_dict=dict(stored), database=self._database)

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO READ INTRALINKED ROWS START HERE

    def get_intralink_row(
        self, primary_row: "RowAPI", secondary_row: "RowAPI"
    ) -> Optional["RowAPI"]:
        """
        Find the unique directed self-link for an endpoint pair.

        Validate that a self-link table exists, search by primary ID, then compare
        secondary IDs as text. Type is not filtered, so differently typed matches can be
        ambiguous.

        Example:
            For same-table Rows, db.get_intralink_row(first, second) looks only in the
            first-to-second direction.


        :param primary_row: Primary endpoint.
        :param secondary_row: Secondary endpoint from the same table.
        :return: Matching link Row, or None when absent.
        :raises InputIntegrityError: Tables differ or the self-link table is
            unavailable.
        :raises DatabaseIntegrityError: More than one link matches the directed pair.
        """
        primary_table = self._row_table(primary_row)
        secondary_table = self._row_table(secondary_row)

        link_table_name = self._database.driver_wrapper.get_link_table_name(
            primary_table, secondary_table
        )
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

        primary_id_col = self._database.driver_wrapper.get_link_column(
            primary_table, primary_table, "primary_id"
        )
        secondary_id_col = self._database.driver_wrapper.get_link_column(
            primary_table, primary_table, "secondary_id"
        )

        candidate_rows = []
        # Search the table using the primary_id - refine using the secondary to return the actually desired result
        primary_id = six_unicode(primary_row.row_id)
        secondary_id = six_unicode(secondary_row.row_id)
        for row in self._database.search(
            table=link_table_name, column=primary_id_col, search_term=primary_id
        ):
            if secondary_id == six_unicode(row[secondary_id_col]):
                candidate_rows.append(row)

        if len(candidate_rows) == 0:
            return None

        elif len(candidate_rows) == 1:
            return candidate_rows[0]

        err_str = (
            "Rows are joined by more than one intralink row - which shouldn't happen."
        )
        err_str = default_log.log_variables(
            err_str,
            "ERROR",
            ("candidate_rows", candidate_rows),
            ("primary_row", primary_row),
            ("secondary_row", secondary_row),
        )
        raise DatabaseIntegrityError(err_str)

    def get_intralink_rows(
        self,
        row: "RowAPI",
        primary: bool = True,
        secondary: bool = True,
        link_type_filter: Optional[str] = None,
    ) -> list["RowAPI"]:
        """
        Collect self-link Rows mentioning a seed in either endpoint column.

        There is no deduplication or priority sort; a self-link can appear twice when
        both directions are requested. With both direction flags false the result is
        empty, although schema columns are still resolved.

        Example:
            For a seed Row, db.get_intralink_rows(seed, primary=False, secondary=True)
            returns incoming self-links.


        :param row: Seed Row.
        :param primary: Include relationships where the seed is primary.
        :param secondary: Include relationships where the seed is secondary.
        :param link_type_filter: Optional type compared as text without stripping or
            lowercasing.
        :return: Link Row list, primary-query results followed by secondary-query
            results.
        """
        table = self._row_table(row)
        row_id = six_unicode(row.row_id)

        intralink_table = self._database.driver_wrapper.get_link_table_name(
            table, table
        )
        if not intralink_table:
            raise InputIntegrityError(f"No intralink table for {table!r}")
        intralink_table_primary_row = self._database.driver_wrapper.get_link_column(
            table, table, "primary_id"
        )
        intralink_table_secondary_row = self._database.driver_wrapper.get_link_column(
            table, table, "secondary_id"
        )

        row_pool = []
        # Search the intralink table for mentions of the id in the primary column
        if primary:
            primary_intralink_rows = self._database.search(
                table=intralink_table,
                column=intralink_table_primary_row,
                search_term=row_id,
            )
            row_pool.extend([r for r in primary_intralink_rows])

        # Search the intralink table for mentions of the id in the secondary column
        if secondary:
            secondary_intralink_rows = self._database.search(
                table=intralink_table,
                column=intralink_table_secondary_row,
                search_term=row_id,
            )
            row_pool.extend([r for r in secondary_intralink_rows])

        if link_type_filter is None:
            return row_pool
        else:
            intralink_table_link_type = self._database.driver_wrapper.get_link_column(
                table, table, "type"
            )
            filtered_row_pool = [
                r
                for r in row_pool
                if six_unicode(r[intralink_table_link_type])
                == six_unicode(link_type_filter)
            ]
            return filtered_row_pool

    def get_intralinked_rows(
        self, primary_row: Optional["RowAPI"], secondary_row: Optional["RowAPI"]
    ) -> list["RowAPI"]:
        """Return the opposite endpoint Rows of directed self-links.

        Supply exactly one seed. A primary seed returns outgoing targets; a
        secondary seed returns incoming sources. Distinct relationships may yield
        the same endpoint more than once. Use get_intralink_rows for link records.

        :param primary_row: Persisted outgoing seed, or None.
        :param secondary_row: Persisted incoming seed, or None.
        :return: Opposite endpoint Rows in query order.
        :raises InputIntegrityError: Invalid seed combination or missing identity.
        :raises DatabaseIntegrityError: A relationship references a missing endpoint.
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

        seed = primary_row if primary_row is not None else secondary_row
        assert seed is not None
        table = self._row_table(seed)
        if seed.row_id is None:
            raise InputIntegrityError("Intralink lookup requires a persisted endpoint")
        wrapper = self._database.driver_wrapper
        link_table = wrapper.get_link_table_name(table, table)
        if not link_table:
            raise InputIntegrityError(f"No intralink table for {table!r}")
        seed_suffix, target_suffix = (
            ("primary_id", "secondary_id")
            if primary_row is not None
            else ("secondary_id", "primary_id")
        )
        seed_column = wrapper.get_link_column(table, table, seed_suffix)
        target_column = wrapper.get_link_column(table, table, target_suffix)
        links = self._database.macros.get_rows(
            link_table, where={seed_column: seed.row_id}
        )
        endpoints: list[RowAPI] = []
        for link in links:
            target_id = link[target_column]
            endpoint = (
                self._database.get_row_from_id(table=table, row_id=target_id)
                if target_id is not None
                else None
            )
            if endpoint is None:
                raise DatabaseIntegrityError("Intralink refers to a missing endpoint")
            endpoints.append(endpoint)
        return endpoints

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO DELETE INTRALINK ROWS START HERE

    # Todo: Consider renaming - unlink_intralink
    def unlinked_intralink(
        self, primary_row: Optional["RowAPI"], secondary_row: Optional["RowAPI"]
    ) -> None:
        """
        Delete a unique directed pair or every link in one endpoint direction.

        Both seeds select a unique pair; only a primary seed selects all outgoing links.
        Only a secondary seed selects all incoming links. The complete deletion uses
        one macro transaction and preserves any outer caller transaction.

        Example:
            For a valid seed, db.unlinked_intralink(primary_row=seed,
            secondary_row=None) removes its outgoing links and retains endpoint records.


        :param primary_row: Primary seed, or None for incoming-only deletion.
        :param secondary_row: Secondary seed, or None to delete all outgoing links.
        :return: None; a missing explicitly requested pair is a no-op.
        :raises InputIntegrityError: Both seeds are None.
        :raises DatabaseIntegrityError: A requested pair has more than one link.
        """
        seeds = [seed for seed in (primary_row, secondary_row) if seed is not None]
        if not seeds:
            raise InputIntegrityError("unlink_intralink called without content")
        table = self._row_table(seeds[0])
        if any(self._row_table(seed) != table or seed.row_id is None for seed in seeds):
            raise InputIntegrityError(
                "Intralink seeds must be persisted in the same table"
            )
        wrapper = self._database.driver_wrapper
        link_table = wrapper.get_link_table_name(table1=table, table2=table)
        if not link_table:
            raise InputIntegrityError(f"No intralink table for {table!r}")
        with self._write_transaction():
            if primary_row is not None and secondary_row is not None:
                link = self.get_intralink_row(primary_row, secondary_row)
                if link is not None:
                    self._database.macros.delete_row(link_table, link.row_id)
            else:
                column = wrapper.get_link_column(
                    table1=table,
                    table2=table,
                    column_type="primary_id"
                    if primary_row is not None
                    else "secondary_id",
                )
                self._database.macros.delete_rows_where(
                    link_table, where={column: seeds[0].row_id}
                )

    #
    # ----------------------------------------------------------------------------------------------------------------------
