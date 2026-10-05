
"""
Read and mutate relationships between rows in different database tables.

Queries distinguish relationship Rows from their endpoint Rows. Priority handling varies by operation; mutations use several backend calls without a surrounding transaction. Callers must account for schema constraints, duplicate relationships and partial failure.
"""

from __future__ import annotations

from copy import deepcopy
from numbers import Number

from typing import TYPE_CHECKING, Optional, Union, Any, Optional, Literal, Iterable

from LiuXin_alpha.databases.row import Row

from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError

from LiuXin_alpha.utils.logging import default_log

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

if TYPE_CHECKING:

    from LiuXin_alpha.databases.api.database_api import DatabaseAPI
    from LiuXin_alpha.databases.api.row_api import RowAPI, IntralinkRowAPI


class DatabaseInterlinkRowsMixin:
    """
    Provide relationship lookup, creation, ordering and deletion through a facade.

    Requires Row/search factories, schema capabilities and a driver wrapper. Methods retain legacy naming and some inconsistent ordering conventions; read each operation contract before composing mutations.

    Example:
        For two existing compatible Rows, link = db.interlink_rows(primary_row=agent, secondary_row=work, type="author") creates their relationship when allowed by the schema.
    """

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO READ INTERLINK TABLES START HERE
    # Todo: We can have multiple links with different types...
    # Todo: Givne the existence of onlink, we eprobably want two different methods ... for typing
    def get_interlink_row(
            self: "DatabaseAPI",
            primary_row: "RowAPI",
            secondary_row: "RowAPI",
            onelink: bool = True) -> Optional[Union["RowAPI", list["RowAPI"]]]:
        """
        Find relationship Rows connecting one ordered endpoint pair.

        Reject same-table or unlinked schemas. Search by the primary ID and compare secondary IDs as text; this does not filter relationship types.

        Example:
            For compatible rows, links = db.get_interlink_row(agent, work, onelink=False) returns every matching relationship or None.


        :param primary_row: Primary endpoint Row.
        :param secondary_row: Secondary endpoint Row in a different table.
        :param onelink: Require at most one match when True; otherwise return all matches.
        :return: None for no matches; one Row when onelink=True, otherwise a nonempty list.
        :raises InputIntegrityError: The tables are identical or have no relationship table.
        :raises DatabaseIntegrityError: Multiple matches exist while onelink=True.
        """
        primary_table = primary_row.table
        secondary_table = secondary_row.table

        link_table_name = self.driver_wrapper.get_link_table_name(primary_table, secondary_table)
        if not link_table_name or (primary_table == secondary_table):
            err_str = "Given tables cannot be connected - or you have used an interlink method, not the intralink one"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("primary_row", primary_row),
                ("secondary_row", secondary_row),
                ("link_table_name", link_table_name),
            )
            raise InputIntegrityError(err_str)

        # Search the interlink table for a row which matches the required criteria
        interlink_table = self.driver_wrapper.get_link_table_name(primary_table, secondary_table)
        primary_link_col = self.driver_wrapper.get_link_column(
            primary_table,
            secondary_table,
            self.driver_wrapper.get_id_column(primary_table),
        )
        secondary_link_col = self.driver_wrapper.get_link_column(
            primary_table,
            secondary_table,
            self.driver_wrapper.get_id_column(secondary_table),
        )

        # Search for links which reference the primary_row
        candidate_rows = []
        link_rows = self.search(
            table=interlink_table,
            column=primary_link_col,
            search_term=primary_row.row_id,
        )
        secondary_id = six_unicode(secondary_row.row_id)
        for row in link_rows:
            if secondary_id == six_unicode(row[secondary_link_col]):
                candidate_rows.append(row)

        if len(candidate_rows) == 0:
            return None
        elif len(candidate_rows) == 1:
            if onelink:
                return candidate_rows[0]
            else:
                return candidate_rows
        else:
            if onelink:
                err_str = "Only one link is permitted between each row pair"
                err_str = default_log.log_variables(
                    err_str,
                    "ERROR",
                    ("primary_row", primary_row),
                    ("secondary_row", secondary_row),
                    ("link_table_name", link_table_name),
                    ("candidate_rows", candidate_rows),
                )
                raise DatabaseIntegrityError(err_str)
            else:
                return candidate_rows

    def get_interlink_rows(
            self: "DatabaseAPI" ,
            primary_row: "RowAPI",
            secondary_table: str) -> list["RowAPI"]:
        """
        Return relationship Rows from one endpoint to a target table.

        A DatabaseIntegrityError resolving priority leaves search order intact; missing keys or incomparable sort values are not suppressed.

        Example:
            For a compatible schema, db.get_interlink_rows(agent, "works") returns link records rather than work records.


        :param primary_row: Primary endpoint Row.
        :param secondary_table: Different table whose relationship Rows are requested.
        :return: List of relationship Rows, sorted by ascending priority when that column resolves.
        :raises InputIntegrityError: Same-table or unavailable interlink schema.
        """
        primary_table = primary_row.table

        link_table_name = self.driver_wrapper.get_link_table_name(primary_table, secondary_table)
        if not link_table_name or (primary_table == secondary_table):
            err_str = "Given tables cannot be connected - or you have used an interlink method, not the intralink one"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("primary_row", primary_row),
                ("secondary_table", secondary_table),
                ("link_table_name", link_table_name),
            )
            raise InputIntegrityError(err_str)

        # Search the interlink table for a row which matches the required criteria
        interlink_table = self.driver_wrapper.get_link_table_name(primary_table, secondary_table)
        primary_link_col = self.driver_wrapper.get_link_column(
            primary_table,
            secondary_table,
            self.driver_wrapper.get_id_column(primary_table),
        )

        link_rows = self.search(
            table=interlink_table,
            column=primary_link_col,
            search_term=primary_row.row_id,
        )
        try:
            priority_col = self.driver_wrapper.get_link_column(primary_table, secondary_table, "priority")
        except DatabaseIntegrityError:
            pass
        else:
            link_rows = sorted(link_rows, key=lambda x: x[priority_col])
        return link_rows

    def get_interlinked_rows(
            self: "DatabaseAPI",
            target_row: Optional["RowAPI"] = None,
            secondary_table: Optional[str] = None,
            type_filter: Optional[str] = None,
            **kwargs: Any) -> list["RowAPI"]:
        """
        Resolve endpoint Rows reached through inter-table relationships.

        Validate keywords, target table argument, concrete Row type, target category and different endpoint tables in that order. Only a missing priority key suppresses sorting. Duplicate target IDs are preserved; type values are compared without normalization.

        Example:
            For an existing agent Row, works = db.get_interlinked_rows(primary_row=agent, secondary_table="works", type_filter="author") follows only author links.


        :param target_row: Concrete Row used as the primary endpoint.
        :param secondary_table: Required target main/helper table name.
        :param type_filter: Optional exact relationship-type filter.
        :param kwargs: Compatibility primary_row alias accepted only when target_row is None; all other keywords fail.
        :return: Endpoint Row list, descending by link priority when available; [] when no link table or matches exist.
        :raises TypeError: Keywords are unexpected or secondary_table is missing.
        :raises InputIntegrityError: The seed is not a concrete Row, the target category is invalid, or the tables match.
        """

        # Backwards/forwards compatibility: some callers (notably contract tests) use
        # primary_row=<Row> to mean the same as target_row=<Row>.
        if target_row is None and "primary_row" in kwargs:
            target_row = kwargs.pop("primary_row")

        # Defensive: if a caller passed unexpected keywords, fail loudly with a helpful message.
        if kwargs:
            unexpected = ", ".join(sorted(kwargs.keys()))
            raise TypeError(f"get_interlinked_rows() got unexpected keyword argument(s): {unexpected}")

        if secondary_table is None:
            raise TypeError("get_interlinked_rows() missing required argument: 'secondary_table'")

        if not isinstance(target_row, Row):
            err_str = "Input to the DatabasePing class has to be in the form of Rows"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("target_row", target_row),
                ("secondary_table", secondary_table),
            )
            raise InputIntegrityError(err_str)

        if secondary_table not in self.main_tables and secondary_table not in self.helper_tables:
            err_str = "Secondary table needs to be in either the main tables or the helper tables"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("target_row", target_row),
                ("secondary_table", secondary_table),
            )
            raise InputIntegrityError(err_str)

        if target_row.table == secondary_table:
            err_str = "This method is for interlink rows, not intralink rows."
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("target_row", target_row),
                ("secondary_table", secondary_table),
            )
            raise InputIntegrityError(err_str)

        primary_table = target_row.table
        primary_id = target_row.row_id
        primary_id_col = self.driver_wrapper.get_id_column(primary_table)

        secondary_id_col = self.driver_wrapper.get_id_column(secondary_table)

        # Get the name of the link table - check to see if it exists (if it doesn't, returns None) - signalling that no
        # link exists
        link_table = self.driver_wrapper.get_link_table_name(primary_table, secondary_table)
        if not link_table:
            return []

        link_table_col = self.driver_wrapper.get_column_base(link_table)
        primary_table_link_col = link_table_col + "_" + primary_id_col
        secondary_table_link_col = link_table_col + "_" + secondary_id_col
        link_priority_col = link_table_col + "_priority"

        link_rows = self.driver_wrapper.search(table=link_table, column=primary_table_link_col, search_term=primary_id)
        if not link_rows:
            return []

        # The highest priority rows will be the first in the list - if there is a priority row to order them
        try:
            link_rows = sorted(link_rows, key=lambda x: x[link_priority_col], reverse=True)
        except KeyError:
            pass

        if type_filter is None:
            secondary_ids = [r[secondary_table_link_col] for r in link_rows]
            secondary_rows = [self.get_row_from_id(table=secondary_table, row_id=r_id) for r_id in secondary_ids]
            return secondary_rows
        else:
            link_type_column = link_table_col + "_type"
            secondary_ids = [r[secondary_table_link_col] for r in link_rows if r[link_type_column] == type_filter]
            secondary_rows = [self.get_row_from_id(table=secondary_table, row_id=r_id) for r_id in secondary_ids]
            return secondary_rows

    def get_interlink_values(
            self: "DatabaseAPI",
            target_row: "RowAPI",
            secondary_column: str) -> set[Any]:
        """
        Collect distinct column values from linked endpoint Rows.

        Example:
            For an agent linked to works, db.get_interlink_values(agent, "work_title") collects linked title values.


        :param target_row: Seed Row.
        :param secondary_column: Column whose owning table is identified by the wrapper.
        :return: Set of values; relationship priority and duplicates are discarded.
        :raises TypeError: A retrieved column value is unhashable.
        """
        secondary_table = self.driver_wrapper.identify_table_from_column(secondary_column)
        linked_rows = self.get_interlinked_rows(target_row=target_row, secondary_table=secondary_table)
        return set([r[secondary_column] for r in linked_rows])

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO WRITE TO INTERLINK TABLES START HERE

    def check_for_link_table_priority(
            self: "DatabaseAPI",
            link_table_name: str,
            primary_link_table_name: str,
            secondary_link_table_name: str) -> bool:
        """
        Cache whether resolved relationship capabilities declare a priority column.

        A new result requires matching table identity and a true priority capability. This does not invalidate an earlier cached result after schema changes.

        Example:
            During relationship creation, db.check_for_link_table_priority(link_table, "agents", "works") determines whether priority should be populated.


        :param link_table_name: Relationship table used as the cache key.
        :param primary_link_table_name: First endpoint table.
        :param secondary_link_table_name: Second endpoint table.
        :return: Cached or newly determined boolean.
        """
        if link_table_name in self._link_has_priority:
            return self._link_has_priority[link_table_name]

        capabilities = self.get_link_capabilities(
            primary_link_table_name,
            secondary_link_table_name,
        )
        has_priority = bool(
            capabilities is not None
            and capabilities.link_table == link_table_name
            and capabilities.priority
        )
        self._link_has_priority[link_table_name] = has_priority
        return has_priority

    # Todo: Remain type to link type
    # Todo: Extend with the other permissable link attributes
    def interlink_rows(
            self: "DatabaseAPI",
            primary_row: "RowAPI",
            secondary_row: "RowAPI",
            priority: str = "highest",
            type: Optional[Union[Literal["not-set"], Literal["highest"], Literal["lowest"], Number]] = None,
            **col_value_pairs: Any) -> "IntralinkRowAPI":
        """
        Allocate a relationship record, populate endpoint fields and synchronize it.

        Check link-table existence and endpoint IDs before allocation. Highest/lowest use whole-column integer extrema plus/minus one, with an empty-table fallback of one. For not_set, a colliding non-null default may be replaced by the next priority for this primary/type. On sync DatabaseIntegrityError delete the allocated row and reraise; other failures can leave partial state. Reload failures are suppressed.

        Example:
            For a compatible writable schema, link = db.interlink_rows(agent, work, priority="highest", type="author") creates a relationship above the current global priority maximum.


        :param primary_row: Primary endpoint with a non-None ID.
        :param secondary_row: Secondary endpoint with a non-None ID.
        :param priority: Number, None for zero, highest/lowest, or the exact not_set sentinel.
        :param type: Optional type value stored unchanged when non-None.
        :param col_value_pairs: Additional relationship columns resolved by the wrapper.
        :return: Persisted generic Row for the relationship, with defaults reloaded when possible.
        :raises InputIntegrityError: No relationship table, a missing endpoint ID, or an unsupported active priority value.
        :raises DatabaseIntegrityError: Nonempty-table extrema cannot be converted, or synchronization violates a constraint.
        """
        # Check that the tables can be interlinked
        primary_row_table = primary_row.table
        secondary_row_table = secondary_row.table
        link_table = self.driver_wrapper.get_link_table_name(primary_row_table, secondary_row_table)
        if not link_table:
            err_str = "Tables cannot be linked - no such link table exists"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("primary_row", primary_row),
                ("secondary_row", secondary_row),
            )
            raise InputIntegrityError(err_str)

        # Check that both the rows have ids
        primary_id = primary_row.row_id
        secondary_id = secondary_row.row_id
        if primary_id is None or secondary_id is None:
            err_str = "Table cannot be linked - one of the rows doesn't have an id"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("primary_row", primary_row),
                ("secondary_row", secondary_row),
            )
            raise InputIntegrityError(err_str)

        link_row = dict()
        for col in col_value_pairs:
            link_row_col = self.driver_wrapper.get_link_column(primary_row_table, secondary_row_table, col)
            link_row[link_row_col] = col_value_pairs[col]

        # Make the link dict - do not add it as yet
        primary_row_id_col = self.driver_wrapper.get_id_column(primary_row_table)
        primary_link_col = self.driver_wrapper.get_link_column(
            primary_row_table, secondary_row_table, primary_row_id_col
        )

        secondary_row_id_col = self.driver_wrapper.get_id_column(secondary_row_table)
        secondary_link_col = self.driver_wrapper.get_link_column(
            primary_row_table, secondary_row_table, secondary_row_id_col
        )

        link_row[primary_link_col] = primary_id
        link_row[secondary_link_col] = secondary_id

        # Process the priority - only numbers can be written into the priority column
        if priority != "not_set":

            if self.check_for_link_table_priority(link_table, primary_row_table, secondary_row_table):
                priority_col = self.driver_wrapper.get_link_column(primary_row_table, secondary_row_table, "priority")

                # Set the priority of the link if the table has a priority column
                if priority_col is not None:
                    priority_key = six_unicode(priority).lower().strip()
                    if priority is None:
                        link_row[priority_col] = 0
                    elif priority_key == "highest" or priority_key == "lowest":
                        priority_num = (
                            self.get_max(priority_col) if priority_key == "highest" else self.get_min(priority_col)
                        )
                        try:
                            priority_val = int(priority_num) + 1 if priority_key == "highest" else int(priority_num) - 1
                        except (ValueError, TypeError) as e:
                            # Correct a bug which throws an error when t a link table is empty
                            link_row_count = self.driver_wrapper.get_record_count(target_table=link_table)
                            if link_row_count != 0:
                                err_str = (
                                    "get_max for a priority column appears to have returned something not a number"
                                )
                                err_str = default_log.log_exception(
                                    err_str,
                                    e,
                                    "ERROR",
                                    ("priority_num", priority_num),
                                    ("primary_row", primary_row),
                                    ("secondary_row", secondary_row),
                                    ("priority", priority),
                                )
                                raise DatabaseIntegrityError(err_str)
                            else:
                                info_str = "Link table appeared to be empty - setting piority_val to 1 and continuing"
                                default_log.log_variables(info_str, "INFO")
                                priority_val = 1
                        link_row[priority_col] = priority_val

                    elif isinstance(priority, Number):
                        link_row[priority_col] = priority

                    else:
                        err_str = "priority type not recognized and cannot be parsed"
                        err_str = default_log.log_variables(
                            err_str,
                            "ERROR",
                            ("primary_row", primary_row),
                            ("secondary_row", secondary_row),
                            ("priority", priority),
                        )
                        raise InputIntegrityError(err_str)

        # Process the type - Todo: Add checking that the type is valid for that combination
        if type is not None:
            type_col = self.driver_wrapper.get_link_column(primary_row_table, secondary_row_table, "type")
            link_row[type_col] = type

        # Acquire an id for the link row and add it
        link_table_id = self.driver_wrapper.get_id_column(link_table)
        blank_link_row = self.driver_wrapper.get_blank_row(link_table)
        link_row[link_table_id] = blank_link_row[link_table_id]


        # If priority wasn't explicitly set but this link table has a priority column with a non-NULL default,
        # multiple links for the same primary row may collide with UNIQUE(primary_id, priority).
        # In that case, auto-assign the next available priority for this primary (and type, if relevant).
        if priority == "not_set":
            if self.check_for_link_table_priority(link_table, primary_row_table, secondary_row_table):
                priority_col = self.driver_wrapper.get_link_column(primary_row_table, secondary_row_table, "priority")
                if priority_col is not None:
                    blank_default = blank_link_row.get(priority_col, None)
                    # If the DB default is NULL, UNIQUE constraints won't collide on it in SQLite (multiple NULLs allowed).
                    if blank_default is not None:
                        type_col = None
                        if type is not None:
                            try:
                                type_col = self.driver_wrapper.get_link_column(
                                    primary_row_table, secondary_row_table, "type"
                                )
                            except Exception:
                                type_col = None

                        blank_default_cmp = blank_default
                        if isinstance(blank_default_cmp, str):
                            try:
                                blank_default_cmp = float(blank_default_cmp) if "." in blank_default_cmp else int(blank_default_cmp)
                            except Exception:
                                blank_default_cmp = blank_default

                        where = "`{}` = ? AND `{}` = ?".format(primary_link_col, priority_col)
                        vals = [primary_id, blank_default_cmp]
                        if type is not None and type_col is not None:
                            where += " AND `{}` = ?".format(type_col)
                            vals.append(type)

                        exists = self.driver_wrapper.get(
                            "SELECT 1 FROM `{}` WHERE {} LIMIT 1;".format(link_table, where),
                            vals,
                            all=False,
                        )
                        if exists is not None:
                            where2 = "`{}` = ?".format(primary_link_col)
                            vals2 = [primary_id]
                            if type is not None and type_col is not None:
                                where2 += " AND `{}` = ?".format(type_col)
                                vals2.append(type)

                            max_row = self.driver_wrapper.get(
                                "SELECT MAX(`{}`) FROM `{}` WHERE {};".format(priority_col, link_table, where2),
                                vals2,
                                all=False,
                            )
                            max_val = None
                            if max_row is not None and len(max_row) > 0:
                                max_val = max_row[0]

                            try:
                                if max_val is None:
                                    next_val = 1
                                else:
                                    if isinstance(max_val, str):
                                        max_val = float(max_val) if "." in max_val else int(max_val)
                                    next_val = max_val + 1
                            except Exception:
                                next_val = 1

                            link_row[priority_col] = next_val

        # Todo: This is pretty inefficient - try and tidy it up
        # Sync the new data back to the database
        link_row = Row(row_dict=link_row, database=self)
        try:
            link_row.sync()
        except DatabaseIntegrityError:
            self.delete(link_row)
            raise

        # The Row instance created above may not include every link-table column (e.g. columns with DB defaults
        # like priority). Many callers expect those defaults to be visible immediately, so reload from the DB.
        try:
            link_row.load_row_from_id(row_id=link_row.row_id, table=link_table)
        except Exception:
            # Best-effort only: if reload fails, still return the successfully-created link.
            pass

        return link_row

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO UPDATE A LINK BETWEEN TWO ROWS START HERE

    def dupe_interlinks(
            self,
            src_row: "RowAPI",
            dst_row: "RowAPI",
            swap_priorities: bool = False,
            restrict_to_tables: Optional[Iterable[str]]=None,
            force_priority: Optional[str] = None,
    ):
        """
        Recreate a source row relationships on a destination using the normal link writer.

        Reverse each target list before creation. Original type and extra relationship attributes are not copied. Writes are incremental and not rolled back as a group; duplicate relationships and the legacy swap limitation still apply.

        Example:
            For compatible rows in the same table, db.dupe_interlinks(source, destination, restrict_to_tables=["works"]) recreates links to works using default relationship attributes.


        :param src_row: Row whose linked endpoints are read.
        :param dst_row: Row that will acquire new relationships.
        :param swap_priorities: Invoke the legacy priority-swap helper after each creation.
        :param restrict_to_tables: Target table iterable, or all main tables except the source table.
        :param force_priority: Priority argument for new links, or None for the normal highest default.
        :return: None.
        """
        # So this method only tries to handle interlinks
        if restrict_to_tables is None:
            other_main_tables = set(t for t in deepcopy(self.main_tables))
            other_main_tables.remove(src_row.table)
        else:
            other_main_tables = restrict_to_tables

        # Identify all the rows linked to the src_row - then link them to the dst row
        for main_table in other_main_tables:
            src_linked_rows = self.get_interlinked_rows(target_row=src_row, secondary_table=main_table)
            src_linked_rows.reverse()
            for src_linked_row in src_linked_rows:

                if force_priority is None:
                    self.interlink_rows(primary_row=dst_row, secondary_row=src_linked_row)
                else:
                    self.interlink_rows(
                        primary_row=dst_row,
                        secondary_row=src_linked_row,
                        priority=force_priority,
                    )

                if swap_priorities:
                    self.swap_priorities(src_row=src_linked_row, dst_row_1=src_row, dst_row_2=dst_row)

    def swap_priorities(
            self: "DatabaseAPI",
            src_row: "RowAPI",
            dst_row_1: "RowAPI",
            dst_row_2: "RowAPI") -> None:
        """
        Apply the legacy two-link priority update sequence.

        Read both priorities, then overwrite the first link priority with None and sync it twice. The second link receives the original first priority; the intended first replacement is never restored. This is not currently a correct two-way swap and has no grouped rollback.

        Example:
            Inspect and correct priorities explicitly when a true exchange is needed; db.swap_priorities(seed, first, second) retains the legacy first-priority-to-None behavior.


        :param src_row: Common endpoint.
        :param dst_row_1: First linked endpoint.
        :param dst_row_2: Second linked endpoint in the same target table.
        :return: None.
        """
        src_row_table = src_row.table
        dst_table = dst_row_1.table
        link_priority_column = self.driver_wrapper.get_link_column(src_row_table, dst_table, "priority")

        dst_row_1_link = self.get_interlink_row(primary_row=src_row, secondary_row=dst_row_1)
        dst_row_2_link = self.get_interlink_row(primary_row=src_row, secondary_row=dst_row_2)

        priority_hold = dst_row_1_link[link_priority_column]
        dst_row_1_link[link_priority_column] = dst_row_2_link[link_priority_column]
        dst_row_2_link[link_priority_column] = priority_hold

        # Need this to get around the unique constraint
        dst_row_1_link[link_priority_column] = None
        dst_row_1_link.sync()

        # Actually do the work of writing the change out
        dst_row_1_link.sync()
        dst_row_2_link.sync()

    # Todo: Need tests for the other col-value pairs
    def update_interlink(
            self: "DatabaseAPI",
            primary_row: "RowAPI",
            secondary_row: "RowAPI",
            priority: Union[Literal["unchanged"], Literal["highest"], Literal["lowest"]] = "unchanged",
            **col_value_pairs: Any) -> "IntralinkRowAPI":
        """
        Modify the unique relationship Row and synchronize requested fields.

        Resolve a priority column even when unchanged is requested. Highest/lowest use whole-column integer extrema; extra keyword columns can override earlier assignments. No rollback is added around sync. Missing links are not explicitly checked before subsequent Row use.

        Example:
            For an existing unique relationship, db.update_interlink(agent, work, priority=10) persists its new priority.


        :param primary_row: Primary endpoint.
        :param secondary_row: Secondary endpoint.
        :param priority: unchanged, highest, lowest, a Number, or None for zero.
        :param col_value_pairs: Other link-column values, resolved after priority processing.
        :return: Updated relationship Row.
        :raises InputIntegrityError: The priority value is unsupported.
        :raises DatabaseIntegrityError: The pair is ambiguous, extrema are invalid, or synchronization fails.
        """
        interlink_row = self.get_interlink_row(primary_row=primary_row, secondary_row=secondary_row)
        primary_row_table = primary_row.table
        secondary_row_table = secondary_row.table

        # Update the priority to the newly given quantity
        # Process the priority - only numbers can be written into the priority column
        priority_col = self.driver_wrapper.get_link_column(primary_row_table, secondary_row_table, "priority")
        priority_key = six_unicode(priority).lower().strip()
        if priority is None:
            interlink_row[priority_col] = 0
        elif priority_key == "unchanged":
            pass
        elif priority_key == "highest" or priority_key == "lowest":
            priority_num = self.get_max(priority_col) if priority_key == "highest" else self.get_min(priority_col)
            try:
                priority_val = int(priority_num) + 1 if priority_key == "highest" else int(priority_num) - 1
            except (ValueError, TypeError) as e:
                err_str = "get_max for a priority column appears to have returned something not a number"
                err_str = default_log.log_exception(
                    err_str,
                    e,
                    "ERROR",
                    ("priority_num", priority_num),
                    ("primary_row", primary_row),
                    ("secondary_row", secondary_row),
                    ("priority", priority),
                )
                raise DatabaseIntegrityError(err_str)
            else:
                interlink_row[priority_col] = priority_val
        elif isinstance(priority, Number):
            interlink_row[priority_col] = priority
        else:
            err_str = "priority type not recognized and cannot be parsed"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("primary_row", primary_row),
                ("secondary_row", secondary_row),
                ("priority", priority),
                ("priority_type", type(priority)),
            )
            raise InputIntegrityError(err_str)

        # Update everything else specified by the keyword pairs
        for col in col_value_pairs:
            link_row_col = self.driver_wrapper.get_link_column(primary_row_table, secondary_row_table, col)
            interlink_row[link_row_col] = col_value_pairs[col]

        interlink_row.sync()
        return interlink_row

    # Todo: Test this with both a tuple and list of ids
    def update_interlink_priority(
            self: "DatabaseAPI",
            primary_row: "RowAPI",
            secondary_table: str,
            ordered_ids: Union[tuple[int, ...], list[int]]) -> None:
        """
        Assign successive global highest priorities in reverse requested ID order.

        Require only equal list lengths, then map current endpoints by integer ID and process a reversed copy of ordered_ids. Duplicate or unknown IDs are not prevalidated; repeated updates can fail after earlier changes.

        Example:
            For three uniquely linked endpoints, db.update_interlink_priority(agent, "works", [third_id, first_id, second_id]) makes that the descending-priority order.


        :param primary_row: Seed whose linked endpoints will be reordered.
        :param secondary_table: Target endpoint table.
        :param ordered_ids: Desired endpoint IDs in descending result order.
        :return: None.
        :raises AssertionError: The number of linked endpoints differs from ordered_ids.
        :raises KeyError: A requested ID is absent from the linked endpoint map.
        """
        secondary_rows = self.get_interlinked_rows(target_row=primary_row, secondary_table=secondary_table)
        assert len(secondary_rows) == len(ordered_ids)

        secondary_row_map = dict((int(r.row_id), r) for r in secondary_rows)

        # Add the rows in the order specified by the ordered_ids
        ordered_ids = [_ for _ in deepcopy(ordered_ids)]
        ordered_ids.reverse()

        for row_id in ordered_ids:
            secondary_row = secondary_row_map[int(row_id)]
            self.update_interlink(primary_row, secondary_row, priority="highest")

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHOD TO UNLINK TWO ROWS STARTS HERE

    def unlink_interlink(self: "DatabaseAPI", primary_row: "RowAPI", secondary_row: "RowAPI") -> None:
        """
        Delete the unique relationship Row connecting an endpoint pair.

        Uses the single-link lookup; multiple matches raise and a missing match reaches delete(None). Endpoint records are retained.

        Example:
            For an existing unique relationship, db.unlink_interlink(agent, work) deletes its link Row.


        :param primary_row: Primary endpoint.
        :param secondary_row: Secondary endpoint.
        :return: None.
        :raises DatabaseIntegrityError: The endpoint pair has multiple relationship Rows.
        :raises AttributeError: No relationship exists and delete receives None.
        """
        link_row = self.get_interlink_row(primary_row=primary_row, secondary_row=secondary_row)
        self.delete(link_row)

    # Todo: Test on a table like ratings, where we can have multiple links between the same title and rating but with
    #       different types. That caused this method to error.
    # Todo: Test on multiple different type filters - including types filters which are lists
    def unlink_all(
            self: "DatabaseAPI",
            primary_row: "RowAPI",
            secondary_table: str,
            type_filter: Optional[str] = None) -> None:
        """
        Delete relationships from a seed to a target table, optionally by type.

        Initially fetch endpoints without filtering. The typed path retries ambiguous single-link lookups in multi-link mode; the unfiltered path does not. Repeated endpoints can revisit already deleted links, so missing-link failures and partial deletion remain possible. No group transaction or endpoint deletion is performed.

        Example:
            For a schema with suitable relationship multiplicity, db.unlink_all(agent, "works", type_filter="author") removes author links while retaining endpoint Rows.


        :param primary_row: Primary endpoint.
        :param secondary_table: Target endpoint table.
        :param type_filter: Exact type value to remove, or None for unfiltered deletion.
        :return: None.
        """
        linked_to_rows = self.get_interlinked_rows(target_row=primary_row, secondary_table=secondary_table)
        if type_filter is None:
            for linked_row in linked_to_rows:
                interlink_row = self.get_interlink_row(primary_row=primary_row, secondary_row=linked_row)
                self.delete(interlink_row)
        else:
            interlink_column = self.driver_wrapper.get_link_column(primary_row.table, secondary_table, "type")
            for linked_row in linked_to_rows:
                try:
                    interlink_row = self.get_interlink_row(primary_row=primary_row, secondary_row=linked_row)
                    interlink_rows = [
                        interlink_row,
                    ]
                except DatabaseIntegrityError:
                    # We might be dealing with a table like ratings
                    interlink_rows = self.get_interlink_row(
                        primary_row=primary_row, secondary_row=linked_row, onelink=False
                    )

                for ilr in interlink_rows:
                    if ilr[interlink_column] == type_filter:
                        self.delete(ilr)

    #
    # ----------------------------------------------------------------------------------------------------------------------
