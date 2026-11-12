"""
Read and mutate relationships between rows in different database tables.

Queries distinguish relationship Rows from their endpoint Rows. Priority handling varies
by operation. Creation, duplication, priority swaps and bulk unlinking share macro
transactions; individual relationship updates retain their legacy write boundary. Callers must account for
schema constraints and duplicate relationships.
"""

from __future__ import annotations

from LiuXin_alpha.databases.database._state import DatabaseMixinState

from numbers import Number

from typing import TYPE_CHECKING, Optional, Union, Any, Literal, Iterable, overload

from LiuXin_alpha.databases.row import Row

from LiuXin_alpha.errors import DatabaseIntegrityError, InputIntegrityError

from LiuXin_alpha.utils.logging import default_log

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.row_api import RowAPI


class DatabaseInterlinkRowsMixin(DatabaseMixinState):
    """
    Provide relationship lookup, creation, ordering and deletion through a facade.

    Requires Row/search factories, schema capabilities and a driver wrapper. Methods
    retain legacy naming and some inconsistent ordering conventions; read each operation
    contract before composing mutations.

    Example:
        For two existing compatible Rows, link = db.interlink_rows(primary_row=agent,
        secondary_row=work, type="author") creates their relationship when allowed by
        the schema.
    """

    _link_has_priority: dict[str, bool] | None

    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO READ INTERLINK TABLES START HERE
    # Todo: We can have multiple links with different types...
    # Todo: Givne the existence of onlink, we eprobably want two different methods ... for typing
    @overload
    def get_interlink_row(
        self,
        primary_row: "RowAPI",
        secondary_row: "RowAPI",
        onelink: Literal[True] = True,
    ) -> Optional["RowAPI"]: ...

    @overload
    def get_interlink_row(
        self, primary_row: "RowAPI", secondary_row: "RowAPI", onelink: Literal[False]
    ) -> Optional[list["RowAPI"]]: ...

    @overload
    def get_interlink_row(
        self, primary_row: "RowAPI", secondary_row: "RowAPI", onelink: bool
    ) -> Optional[Union["RowAPI", list["RowAPI"]]]: ...

    def get_interlink_row(
        self, primary_row: "RowAPI", secondary_row: "RowAPI", onelink: bool = True
    ) -> Optional[Union["RowAPI", list["RowAPI"]]]:
        """
        Find relationship Rows connecting one ordered endpoint pair.

        Reject same-table or unlinked schemas. Search by the primary ID and compare
        secondary IDs as text; this does not filter relationship types.

        Example:
            For compatible rows, links = db.get_interlink_row(agent, work,
            onelink=False) returns every matching relationship or None.


        :param primary_row: Primary endpoint Row.
        :param secondary_row: Secondary endpoint Row in a different table.
        :param onelink: Require at most one match when True; otherwise return all
            matches.
        :return: None for no matches; one Row when onelink=True, otherwise a nonempty
            list.
        :raises InputIntegrityError: The tables are identical or have no relationship
            table.
        :raises DatabaseIntegrityError: Multiple matches exist while onelink=True.
        """
        primary_table = self._row_table(primary_row)
        secondary_table = self._row_table(secondary_row)

        link_table_name = self._database.driver_wrapper.get_link_table_name(
            primary_table, secondary_table
        )
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
        interlink_table = link_table_name
        primary_link_col = self._database.driver_wrapper.get_link_column(
            primary_table,
            secondary_table,
            self._database.driver_wrapper.get_id_column(primary_table),
        )
        secondary_link_col = self._database.driver_wrapper.get_link_column(
            primary_table,
            secondary_table,
            self._database.driver_wrapper.get_id_column(secondary_table),
        )

        # Search for links which reference the primary_row
        candidate_rows = []
        link_rows = self._database.search(
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
        self, primary_row: "RowAPI", secondary_table: str
    ) -> list["RowAPI"]:
        """
        Return relationship Rows from one endpoint to a target table.

        A DatabaseIntegrityError resolving priority leaves search order intact; missing
        keys or incomparable sort values are not suppressed.

        Example:
            For a compatible schema, db.get_interlink_rows(agent, "works") returns link
            records rather than work records.


        :param primary_row: Primary endpoint Row.
        :param secondary_table: Different table whose relationship Rows are requested.
        :return: List of relationship Rows, sorted by ascending priority when that
            column resolves.
        :raises InputIntegrityError: Same-table or unavailable interlink schema.
        """
        primary_table = self._row_table(primary_row)

        link_table_name = self._database.driver_wrapper.get_link_table_name(
            primary_table, secondary_table
        )
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
        interlink_table = link_table_name
        primary_link_col = self._database.driver_wrapper.get_link_column(
            primary_table,
            secondary_table,
            self._database.driver_wrapper.get_id_column(primary_table),
        )

        link_rows = self._database.search(
            table=interlink_table,
            column=primary_link_col,
            search_term=primary_row.row_id,
        )
        try:
            priority_col = self._database.driver_wrapper.get_link_column(
                primary_table, secondary_table, "priority"
            )
        except DatabaseIntegrityError:
            pass
        else:
            link_rows = sorted(link_rows, key=lambda x: x[priority_col])
        return link_rows

    def get_interlinked_rows(
        self,
        target_row: Optional["RowAPI"] = None,
        secondary_table: Optional[str] = None,
        type_filter: Optional[str] = None,
        **kwargs: Any,
    ) -> list["RowAPI | None"]:
        """
        Resolve endpoint Rows reached through inter-table relationships.

        Validate keywords, target table argument, concrete Row type, target category and
        different endpoint tables in that order. Only a missing priority key suppresses
        sorting. Duplicate target IDs are preserved; type values are compared without
        normalization.

        Example:
            For an existing agent Row, works =
            db.get_interlinked_rows(primary_row=agent, secondary_table="works",
            type_filter="author") follows only author links.


        :param target_row: Concrete Row used as the primary endpoint.
        :param secondary_table: Required target main/helper table name.
        :param type_filter: Optional exact relationship-type filter.
        :param kwargs: Compatibility primary_row alias accepted only when target_row is
            None; all other keywords fail.
        :return: Endpoint Row list, descending by link priority when available; [] when
            no link table or matches exist. Missing endpoint records remain None.
        :raises TypeError: Keywords are unexpected or secondary_table is missing.
        :raises InputIntegrityError: The seed is not a concrete Row, the target category
            is invalid, or the tables match.
        """

        # Backwards/forwards compatibility: some callers (notably contract tests) use
        # primary_row=<Row> to mean the same as target_row=<Row>.
        if target_row is None and "primary_row" in kwargs:
            target_row = kwargs.pop("primary_row")

        # Defensive: if a caller passed unexpected keywords, fail loudly with a helpful message.
        if kwargs:
            unexpected = ", ".join(sorted(kwargs.keys()))
            raise TypeError(
                f"get_interlinked_rows() got unexpected keyword argument(s): {unexpected}"
            )

        if secondary_table is None:
            raise TypeError(
                "get_interlinked_rows() missing required argument: 'secondary_table'"
            )

        if not isinstance(target_row, Row):
            err_str = "Input to the DatabasePing class has to be in the form of Rows"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("target_row", target_row),
                ("secondary_table", secondary_table),
            )
            raise InputIntegrityError(err_str)

        if (
            secondary_table not in self._database.main_tables
            and secondary_table not in self.helper_tables
        ):
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

        primary_table = self._row_table(target_row)
        primary_id = target_row.row_id
        primary_id_col = self._database.driver_wrapper.get_id_column(primary_table)

        secondary_id_col = self._database.driver_wrapper.get_id_column(secondary_table)

        # Get the name of the link table - check to see if it exists (if it doesn't, returns None) - signalling that no
        # link exists
        link_table = self._database.driver_wrapper.get_link_table_name(
            primary_table, secondary_table
        )
        if not link_table:
            return []

        link_table_col = self._database.driver_wrapper.get_column_base(link_table)
        primary_table_link_col = link_table_col + "_" + primary_id_col
        secondary_table_link_col = link_table_col + "_" + secondary_id_col
        link_priority_col = link_table_col + "_priority"

        link_rows = self._database.driver_wrapper.search(
            table=link_table, column=primary_table_link_col, search_term=primary_id
        )
        if not link_rows:
            return []

        # The highest priority rows will be the first in the list - if there is a priority row to order them
        try:
            link_rows = sorted(
                link_rows, key=lambda x: x[link_priority_col], reverse=True
            )
        except KeyError:
            pass

        if type_filter is not None:
            link_type_column = link_table_col + "_type"
            link_rows = [r for r in link_rows if r[link_type_column] == type_filter]
        secondary_ids = [r[secondary_table_link_col] for r in link_rows]
        batch = getattr(self._database.macros, "get_rows_by_ids", None)
        relation_type = getattr(
            self._database.driver_wrapper, "get_relation_type", None
        )
        if (
            callable(batch)
            and callable(relation_type)
            and relation_type(secondary_table) == "table"
        ):
            rows = self._database.macros.get_rows_by_ids(
                secondary_table, (value for value in secondary_ids if value is not None)
            )
            # Preserve the legacy link ordering, duplicates and absent endpoints.
            return [
                Row(row_dict=dict(rows[value]), database=self._database)
                if value in rows
                else None
                for value in secondary_ids
            ]
        return [
            self._database.get_row_from_id(table=secondary_table, row_id=value)
            for value in secondary_ids
        ]

    def get_interlink_values(
        self, target_row: "RowAPI", secondary_column: str
    ) -> set[Any]:
        """
        Collect distinct column values from linked endpoint Rows.

        Example:
            For an agent linked to works, db.get_interlink_values(agent, "work_title")
            collects linked title values.


        :param target_row: Seed Row.
        :param secondary_column: Column whose owning table is identified by the wrapper.
        :return: Set of values; relationship priority and duplicates are discarded.
        :raises TypeError: A retrieved column value is unhashable.
        """
        secondary_table = self._database.driver_wrapper.identify_table_from_column(
            secondary_column
        )
        linked_rows = self.get_interlinked_rows(
            target_row=target_row, secondary_table=secondary_table
        )
        if any(row is None for row in linked_rows):
            raise DatabaseIntegrityError("Interlink refers to a missing endpoint")
        return {row[secondary_column] for row in linked_rows if row is not None}

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHODS TO WRITE TO INTERLINK TABLES START HERE

    def check_for_link_table_priority(
        self,
        link_table_name: str,
        primary_link_table_name: str,
        secondary_link_table_name: str,
    ) -> bool:
        """
        Cache whether resolved relationship capabilities declare a priority column.

        A new result requires matching table identity and a true priority capability.
        This does not invalidate an earlier cached result after schema changes.

        Example:
            During relationship creation, db.check_for_link_table_priority(link_table,
            "agents", "works") determines whether priority should be populated.


        :param link_table_name: Relationship table used as the cache key.
        :param primary_link_table_name: First endpoint table.
        :param secondary_link_table_name: Second endpoint table.
        :return: Cached or newly determined boolean.
        """
        cache = self._link_has_priority
        if cache is None:
            raise RuntimeError("Database link cache is unavailable after cleanup")
        if link_table_name in cache:
            return cache[link_table_name]

        capabilities = self._database.get_link_capabilities(
            primary_link_table_name,
            secondary_link_table_name,
        )
        has_priority = bool(
            capabilities is not None
            and capabilities.link_table == link_table_name
            and capabilities.priority
        )
        cache[link_table_name] = has_priority
        return has_priority

    # Todo: Remain type to link type
    # Todo: Extend with the other permissable link attributes
    def interlink_rows(
        self,
        primary_row: "RowAPI",
        secondary_row: "RowAPI",
        priority: Optional[Union[int, float, str]] = "highest",
        type: Optional[str] = None,
        **col_value_pairs: Any,
    ) -> "RowAPI":
        """
        Create and read back a relationship within one macro transaction.

        Check link-table existence and endpoint IDs before allocation. Highest/lowest
        use whole-column integer extrema plus/minus one, with an empty-table fallback of
        one. For not_set, a colliding non-null default may be replaced by the next
        priority for this primary/type. Default allocation, population and readback
        share the caller's transaction; any failure rolls back the whole operation.

        Example:
            For a compatible writable schema, link = db.interlink_rows(agent, work,
            priority="highest", type="author") creates a relationship above the current
            global priority maximum.


        :param primary_row: Primary endpoint with a non-None ID.
        :param secondary_row: Secondary endpoint with a non-None ID.
        :param priority: Number, None for zero, highest/lowest, or the exact not_set
            sentinel.
        :param type: Optional type value stored unchanged when non-None.
        :param col_value_pairs: Additional relationship columns resolved by the wrapper.
        :return: Persisted generic Row including database defaults.
        :raises InputIntegrityError: No relationship table, a missing endpoint ID, or an
            unsupported active priority value.
        :raises DatabaseIntegrityError: Nonempty-table extrema cannot be converted, or
            insertion or population violates a constraint.
        """
        # Check that the tables can be interlinked
        primary_row_table = self._row_table(primary_row)
        secondary_row_table = self._row_table(secondary_row)
        link_table = self._database.driver_wrapper.get_link_table_name(
            primary_row_table, secondary_row_table
        )
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

        with self._write_transaction():
            payload: dict[str, Any] = {}
            type_col: str | None
            for col in col_value_pairs:
                link_row_col = self._database.driver_wrapper.get_link_column(
                    primary_row_table, secondary_row_table, col
                )
                payload[link_row_col] = col_value_pairs[col]

            # Make the link dict - do not add it as yet
            primary_row_id_col = self._database.driver_wrapper.get_id_column(
                primary_row_table
            )
            primary_link_col = self._database.driver_wrapper.get_link_column(
                primary_row_table, secondary_row_table, primary_row_id_col
            )

            secondary_row_id_col = self._database.driver_wrapper.get_id_column(
                secondary_row_table
            )
            secondary_link_col = self._database.driver_wrapper.get_link_column(
                primary_row_table, secondary_row_table, secondary_row_id_col
            )

            payload[primary_link_col] = primary_id
            payload[secondary_link_col] = secondary_id

            # Process the priority - only numbers can be written into the priority column
            if priority != "not_set":
                if self.check_for_link_table_priority(
                    link_table, primary_row_table, secondary_row_table
                ):
                    priority_col = self._database.driver_wrapper.get_link_column(
                        primary_row_table, secondary_row_table, "priority"
                    )

                    # Set the priority of the link if the table has a priority column
                    if priority_col is not None:
                        priority_key = six_unicode(priority).lower().strip()
                        if priority is None:
                            payload[priority_col] = 0
                        elif priority_key == "highest" or priority_key == "lowest":
                            aggregate = "MAX" if priority_key == "highest" else "MIN"
                            priority_num = self._database.driver_wrapper.fetch_scalar(
                                f'SELECT {aggregate}("{priority_col}") FROM "{link_table}"'
                            )
                            try:
                                if priority_num is None:
                                    raise ValueError("Priority column is empty")
                                priority_val = (
                                    int(priority_num) + 1
                                    if priority_key == "highest"
                                    else int(priority_num) - 1
                                )
                            except (ValueError, TypeError) as e:
                                # Correct a bug which throws an error when t a link table is empty
                                link_row_count = (
                                    self._database.driver_wrapper.fetch_scalar(
                                        f'SELECT COUNT(*) FROM "{link_table}"'
                                    )
                                )
                                if link_row_count != 0:
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
                                    info_str = "Link table appeared to be empty - setting piority_val to 1 and continuing"
                                    default_log.log_variables(info_str, "INFO")
                                    priority_val = 1
                            payload[priority_col] = priority_val

                        elif isinstance(priority, Number):
                            payload[priority_col] = priority

                        else:
                            err_str = (
                                "priority type not recognized and cannot be parsed"
                            )
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
                type_col = self._database.driver_wrapper.get_link_column(
                    primary_row_table, secondary_row_table, "type"
                )
                payload[type_col] = type

            # Acquire an id for the link row and add it
            macros = self._database.macros
            # Preserve not_set's database-default behavior without publishing a blank row.
            # Other modes insert the complete payload directly below.
            link_id = None
            blank_link_row: dict[str, Any] = {}
            if priority == "not_set":
                link_id = macros.insert_row(link_table, {primary_link_col: None})
                blank = macros.get_row(link_table, link_id)
                if blank is None:
                    raise DatabaseIntegrityError(
                        "Allocated interlink could not be read back"
                    )
                blank_link_row = dict(blank)

            # If priority wasn't explicitly set but this link table has a priority column with a non-NULL default,
            # multiple links for the same primary row may collide with UNIQUE(primary_id, priority).
            # In that case, auto-assign the next available priority for this primary (and type, if relevant).
            if priority == "not_set":
                if self.check_for_link_table_priority(
                    link_table, primary_row_table, secondary_row_table
                ):
                    priority_col = self._database.driver_wrapper.get_link_column(
                        primary_row_table, secondary_row_table, "priority"
                    )
                    if priority_col is not None:
                        blank_default = blank_link_row.get(priority_col, None)
                        # If the DB default is NULL, UNIQUE constraints won't collide on it in SQLite (multiple NULLs allowed).
                        if blank_default is not None:
                            type_col = None
                            if type is not None:
                                try:
                                    type_col = (
                                        self._database.driver_wrapper.get_link_column(
                                            primary_row_table,
                                            secondary_row_table,
                                            "type",
                                        )
                                    )
                                except Exception:
                                    type_col = None

                            blank_default_cmp = blank_default
                            if isinstance(blank_default_cmp, str):
                                try:
                                    blank_default_cmp = (
                                        float(blank_default_cmp)
                                        if "." in blank_default_cmp
                                        else int(blank_default_cmp)
                                    )
                                except Exception:
                                    blank_default_cmp = blank_default

                            where = "`{}` = ? AND `{}` = ?".format(
                                primary_link_col, priority_col
                            )
                            vals: list[Any] = [primary_id, blank_default_cmp]
                            if type is not None and type_col is not None:
                                where += " AND `{}` = ?".format(type_col)
                                vals.append(type)

                            exists = self._database.driver_wrapper.fetch_scalar(
                                "SELECT 1 FROM `{}` WHERE {} LIMIT 1;".format(
                                    link_table, where
                                ),
                                vals,
                            )
                            if exists is not None:
                                where2 = "`{}` = ?".format(primary_link_col)
                                vals2: list[Any] = [primary_id]
                                if type is not None and type_col is not None:
                                    where2 += " AND `{}` = ?".format(type_col)
                                    vals2.append(type)

                                max_val = self._database.driver_wrapper.fetch_scalar(
                                    "SELECT MAX(`{}`) FROM `{}` WHERE {};".format(
                                        priority_col, link_table, where2
                                    ),
                                    vals2,
                                )

                                try:
                                    if max_val is None:
                                        next_val = 1
                                    else:
                                        if isinstance(max_val, str):
                                            max_val = (
                                                float(max_val)
                                                if "." in max_val
                                                else int(max_val)
                                            )
                                        next_val = max_val + 1
                                except Exception:
                                    next_val = 1

                                payload[priority_col] = next_val

            if link_id is None:
                link_id = macros.insert_row(link_table, payload)
            else:
                macros.update_row(link_table, link_id, payload)
            stored = macros.get_row(link_table, link_id)
            if stored is None:
                raise DatabaseIntegrityError(
                    "Inserted interlink could not be read back"
                )
            return Row(row_dict=dict(stored), database=self._database)

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
        restrict_to_tables: Optional[Iterable[str]] = None,
        force_priority: Optional[Union[int, float, str]] = None,
    ) -> None:
        """
        Recreate a source row relationships on a destination using the normal link
        writer.

        Reverse each target list before creation. Original type and extra relationship
        attributes are not copied. Creation and optional priority swaps share one
        macro transaction; failures roll back the complete operation.

        Example:
            For compatible rows in the same table, db.dupe_interlinks(source,
            destination, restrict_to_tables=["works"]) recreates links to works using
            default relationship attributes.


        :param src_row: Row whose linked endpoints are read.
        :param dst_row: Row that will acquire new relationships.
        :param swap_priorities: Exchange source/destination priorities after each
            creation.
        :param restrict_to_tables: Target table iterable, or all main tables except the
            source table.
        :param force_priority: Priority argument for new links, or None for the normal
            highest default.
        :return: None.
        """
        if self._row_table(src_row) != self._row_table(dst_row):
            raise InputIntegrityError(
                "Interlink duplication requires the same endpoint table"
            )
        if src_row.row_id is None or dst_row.row_id is None:
            raise InputIntegrityError(
                "Interlink duplication requires persisted endpoints"
            )
        with self._write_transaction():
            # So this method only tries to handle interlinks
            if restrict_to_tables is None:
                other_main_tables: Iterable[str] = self._database.main_tables - {
                    src_row.table
                }
            else:
                other_main_tables = restrict_to_tables

            # Identify all the rows linked to the src_row - then link them to the dst row
            for main_table in other_main_tables:
                src_linked_rows = self.get_interlinked_rows(
                    target_row=src_row, secondary_table=main_table
                )
                src_linked_rows.reverse()
                for src_linked_row in src_linked_rows:
                    if src_linked_row is None:
                        raise DatabaseIntegrityError(
                            "Interlink refers to a missing endpoint"
                        )

                    if force_priority is None:
                        self.interlink_rows(
                            primary_row=dst_row, secondary_row=src_linked_row
                        )
                    else:
                        self.interlink_rows(
                            primary_row=dst_row,
                            secondary_row=src_linked_row,
                            priority=force_priority,
                        )

                    if swap_priorities:
                        self.swap_priorities(
                            src_row=src_linked_row, dst_row_1=src_row, dst_row_2=dst_row
                        )

    def swap_priorities(
        self, src_row: "RowAPI", dst_row_1: "RowAPI", dst_row_2: "RowAPI"
    ) -> None:
        """Exchange two existing relationship priorities atomically.

        Both destination Rows must belong to the same table. Resolve each unique
        relationship, use a temporary NULL or unused integer priority, and restore both final
        values in one macro transaction. Failures roll back the entire exchange;
        nested calls join the caller's transaction through a savepoint.

        :param src_row: Common persisted endpoint.
        :param dst_row_1: First linked endpoint.
        :param dst_row_2: Second linked endpoint in the same target table.
        :return: None; swapping a link with itself is a no-op.
        """
        src_table = self._row_table(src_row)
        dst_table = self._row_table(dst_row_1)
        if self._row_table(dst_row_2) != dst_table:
            raise InputIntegrityError(
                "Priority swaps require the same destination table"
            )
        if any(row.row_id is None for row in (src_row, dst_row_1, dst_row_2)):
            raise InputIntegrityError("Priority swaps require persisted endpoints")
        wrapper = self._database.driver_wrapper
        priority_column = wrapper.get_link_column(src_table, dst_table, "priority")
        macros = self._database.macros
        with self._write_transaction():
            first = self.get_interlink_row(src_row, dst_row_1)
            second = self.get_interlink_row(src_row, dst_row_2)
            if first is None or second is None:
                raise InputIntegrityError(
                    "Both links must exist before swapping priorities"
                )
            if first.row_id == second.row_id:
                return
            table = self._row_table(first)
            first_priority = first[priority_column]
            second_priority = second[priority_column]
            if first_priority == second_priority:
                return
            # Nullable priorities need no table scan. A non-nullable schema needs
            # an unused integer slot instead. Neither intermediate value commits.
            column = next(
                column
                for column in wrapper.get_table_spec(table).columns
                if column.name == priority_column
            )
            temporary: int | None = None
            if column.nullable is not True:
                used = {row[priority_column] for row in macros.get_rows(table)}
                temporary = 0
                while temporary in used:
                    temporary += 1
            macros.update_row(table, first.row_id, {priority_column: temporary})
            macros.update_row(table, second.row_id, {priority_column: first_priority})
            macros.update_row(table, first.row_id, {priority_column: second_priority})

    # Todo: Need tests for the other col-value pairs
    def update_interlink(
        self,
        primary_row: "RowAPI",
        secondary_row: "RowAPI",
        priority: Optional[Union[int, float, str]] = "unchanged",
        **col_value_pairs: Any,
    ) -> "RowAPI":
        """
        Modify the unique relationship Row and synchronize requested fields.

        Resolve a priority column even when unchanged is requested. Highest/lowest use
        whole-column integer extrema; extra keyword columns can override earlier
        assignments. No rollback is added around sync. Missing links are rejected before
        column resolution or mutation.

        Example:
            For an existing unique relationship, db.update_interlink(agent, work,
            priority=10) persists its new priority.


        :param primary_row: Primary endpoint.
        :param secondary_row: Secondary endpoint.
        :param priority: unchanged, highest, lowest, a Number, or None for zero.
        :param col_value_pairs: Other link-column values, resolved after priority
            processing.
        :return: Updated relationship Row.
        :raises InputIntegrityError: The link is missing or the priority is unsupported.
        :raises DatabaseIntegrityError: The pair is ambiguous, extrema are invalid, or
            synchronization fails.
        """
        interlink_row = self.get_interlink_row(
            primary_row=primary_row, secondary_row=secondary_row
        )
        if interlink_row is None:
            raise InputIntegrityError("Cannot update a missing interlink")
        primary_row_table = self._row_table(primary_row)
        secondary_row_table = self._row_table(secondary_row)

        # Update the priority to the newly given quantity
        # Process the priority - only numbers can be written into the priority column
        priority_col = self._database.driver_wrapper.get_link_column(
            primary_row_table, secondary_row_table, "priority"
        )
        priority_key = six_unicode(priority).lower().strip()
        if priority is None:
            interlink_row[priority_col] = 0
        elif priority_key == "unchanged":
            pass
        elif priority_key == "highest" or priority_key == "lowest":
            priority_num = (
                self._database.get_max(priority_col)
                if priority_key == "highest"
                else self._database.get_min(priority_col)
            )
            try:
                if priority_num is None:
                    raise ValueError("Priority column is empty")
                priority_val = (
                    int(priority_num) + 1
                    if priority_key == "highest"
                    else int(priority_num) - 1
                )
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
            link_row_col = self._database.driver_wrapper.get_link_column(
                primary_row_table, secondary_row_table, col
            )
            interlink_row[link_row_col] = col_value_pairs[col]

        interlink_row.sync()
        return interlink_row

    # Todo: Test this with both a tuple and list of ids
    def update_interlink_priority(
        self, primary_row: "RowAPI", secondary_table: str, ordered_ids: Iterable[int]
    ) -> None:
        """
        Assign successive global highest priorities in reverse requested ID order.

        Require only equal list lengths, then map current endpoints by integer ID and
        process a reversed copy of ordered_ids. Duplicate or unknown IDs are not
        prevalidated; repeated updates can fail after earlier changes.

        Example:
            For three uniquely linked endpoints, db.update_interlink_priority(agent,
            "works", [third_id, first_id, second_id]) makes that the descending-priority
            order.


        :param primary_row: Seed whose linked endpoints will be reordered.
        :param secondary_table: Target endpoint table.
        :param ordered_ids: Desired endpoint IDs in descending result order.
        :return: None.
        :raises AssertionError: The number of linked endpoints differs from ordered_ids.
        :raises KeyError: A requested ID is absent from the linked endpoint map.
        """
        secondary_rows = self.get_interlinked_rows(
            target_row=primary_row, secondary_table=secondary_table
        )
        ordered = list(ordered_ids)
        assert len(secondary_rows) == len(ordered)
        secondary_row_map: dict[int, RowAPI] = {}
        for row in secondary_rows:
            if row is None or row.row_id is None:
                raise DatabaseIntegrityError("Interlink refers to a missing endpoint")
            secondary_row_map[int(row.row_id)] = row

        # Add the rows in the order specified by the ordered_ids
        for row_id in reversed(ordered):
            secondary_row = secondary_row_map[int(row_id)]
            self.update_interlink(primary_row, secondary_row, priority="highest")

    #
    # ----------------------------------------------------------------------------------------------------------------------
    # ----------------------------------------------------------------------------------------------------------------------
    #
    # - METHOD TO UNLINK TWO ROWS STARTS HERE

    def unlink_interlink(self, primary_row: "RowAPI", secondary_row: "RowAPI") -> None:
        """
        Delete the unique relationship Row connecting an endpoint pair.

        Uses the single-link lookup; multiple matches raise and a missing match reaches
        delete(None). Endpoint records are retained.

        Example:
            For an existing unique relationship, db.unlink_interlink(agent, work)
            deletes its link Row.


        :param primary_row: Primary endpoint.
        :param secondary_row: Secondary endpoint.
        :return: None.
        :raises DatabaseIntegrityError: The endpoint pair has multiple relationship
            Rows.
        :raises InputIntegrityError: No relationship exists for the endpoint pair.
        """
        link_row = self.get_interlink_row(
            primary_row=primary_row, secondary_row=secondary_row
        )
        if link_row is None:
            raise InputIntegrityError("Cannot unlink a missing interlink")
        self._database.delete(link_row)

    # Todo: Test on a table like ratings, where we can have multiple links between the same title and rating but with
    #       different types. That caused this method to error.
    # Todo: Test on multiple different type filters - including types filters which are lists
    def unlink_all(
        self,
        primary_row: "RowAPI",
        secondary_table: str,
        type_filter: Optional[str] = None,
    ) -> None:
        """Delete every matching relationship while retaining both endpoint tables.

        Delete link records directly, including multiple types between the same pair.
        A type filter selects an exact stored value. All deletions form one macro
        transaction and nested calls respect the caller's transaction ownership.

        :param primary_row: Persisted seed endpoint.
        :param secondary_table: Different endpoint table.
        :param type_filter: Exact type to remove, or None for every type.
        :return: None; an empty match is harmless.
        """
        primary_table = self._row_table(primary_row)
        if primary_row.row_id is None or primary_table == secondary_table:
            raise InputIntegrityError(
                "Unlinking requires a persisted interlink endpoint"
            )
        wrapper = self._database.driver_wrapper
        table = wrapper.get_link_table_name(primary_table, secondary_table)
        if not table:
            raise InputIntegrityError("No interlink table for these endpoints")
        endpoint_column = wrapper.get_link_column(
            primary_table, secondary_table, wrapper.get_id_column(primary_table)
        )
        where: dict[str, Any] = {endpoint_column: primary_row.row_id}
        if type_filter is not None:
            type_column = wrapper.get_link_column(
                primary_table, secondary_table, "type"
            )
            where[type_column] = type_filter
        with self._write_transaction():
            self._database.macros.delete_rows_where(table, where=where)

    #
    # ----------------------------------------------------------------------------------------------------------------------
