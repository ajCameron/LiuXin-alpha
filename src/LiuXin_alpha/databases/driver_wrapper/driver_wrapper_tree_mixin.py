"""
Traverse parent-linked rows and delegate persisted tree updates.

The host supplies table/column discovery, search and row lookup. Traversal rejects
cycles with InputIntegrityError. Sibling iteration order is unspecified.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from LiuXin_alpha.databases.driver_wrapper._base import DriverWrapperBase
from LiuXin_alpha.errors import InputIntegrityError
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode
from LiuXin_alpha.utils.logging import default_log


class DriverWrapperTreeMixin(DriverWrapperBase):
    """
    Traverse parent-linked rows and delegate persisted tree updates.

    The host supplies table/column discovery, search and row lookup. Traversal
    rejects cycles with InputIntegrityError; sibling order is unspecified.

    Example:
        >>> list(wrapper.walk(series_row))  # doctest: +SKIP
    """

    # ------------------------------------------------------------------------------------------------------------------
    # - METHODS TO DEAL WITH TREE STRUCTURES IN TABLES
    # ------------------------------------------------------------------------------------------------------------------
    # Todo: Needs to throw an error when used on a table without a tree structure
    def get_linear_row_list(self, start_row: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Follow parent references and return the chain from ancestor to starting row.

        Infers table and parent heading from the starting mapping. Missing parent keys,
        None and case-insensitive text none terminate the chain; the starting mapping is
        retained as-is. Parent rows are fetched through get_row_from_id(). Repeated
        parents, empty input and missing ancestors raise InputIntegrityError; a
        table without parents returns the start row. No siblings are traversed.

        Example:
            >>> from types import SimpleNamespace
            >>> row = {"series_id": 1, "series_parent": None}
            >>> host = SimpleNamespace(
            ...     identify_table_from_row_dict=lambda row: "series",
            ...     get_parent_column=lambda table: "series_parent",
            ... )
            >>> DriverWrapperTreeMixin.get_linear_row_list(host, row) == [row]
            True


        :param start_row: Starting row dictionary containing its identifier and any
            required parent value.
        :return: List of row dictionaries ordered from highest reached ancestor to
            start_row.
        """
        table = self.identify_table_from_row_dict(start_row)
        if table is False:
            raise InputIntegrityError("Cannot identify an empty tree row")
        table_parent_column = self.get_parent_column(table)
        if table_parent_column is False or table_parent_column is None:
            return [start_row]

        linear_rows = []
        visited_parents: set[str] = set()
        current_row = start_row
        try:
            current_parent_id = start_row[table_parent_column]
            if (
                six_unicode(current_parent_id).lower() == "none"
                or current_parent_id is None
            ):
                linear_rows.append(start_row)
                return linear_rows
        except KeyError:
            linear_rows.append(start_row)
            return linear_rows

        while (
            six_unicode(current_parent_id).upper() != "NONE"
            and current_parent_id is not None
        ):
            # extracting the current parent id
            try:
                current_parent_id = current_row[table_parent_column]
                if current_parent_id == "NONE":
                    linear_rows = [current_row] + linear_rows
                    return linear_rows
            except KeyError:
                linear_rows = [current_row] + linear_rows
                return linear_rows

            linear_rows = [current_row] + linear_rows

            if (
                six_unicode(current_parent_id).lower() != "none"
                and current_parent_id is not None
            ):
                parent_key = str(current_parent_id)
                if parent_key in visited_parents:
                    raise InputIntegrityError(
                        f"Cycle detected in {table!r} at parent {parent_key}"
                    )
                visited_parents.add(parent_key)
                parent_row = self.get_row_from_id(table=table, row_id=current_parent_id)
                if parent_row is False:
                    raise InputIntegrityError(
                        f"Missing ancestor {current_parent_id!r} in {table!r}"
                    )
                current_row = parent_row
            else:
                break

        return linear_rows

    # Todo: Again, should error when called on a table which does not have a tree structure
    def set_tree_ids(self, table: str) -> bool:
        """
        Write each positive-ID row's root ID and display value as a tree identifier.

        Delegates to the driver's direct_set_tree_ids hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Use ``<root_id>_<root_display>``; an existing ID-zero sentinel receives
        ``0_<display>`` separately. Commit each update, with no atomic batch or explicit
        connection close. Missing tree-ID columns raise InputIntegrityError.

        Example:
            >>> wrapper.set_tree_ids("series")  # doctest: +SKIP


        :param table: Existing table name used for schema lookup.
        :return: ``True`` after the updates complete.
        """
        return self.driver.direct_set_tree_ids(table)

    def set_full_column(self, table: str) -> bool:
        """
        Write ancestor display paths for positive-ID rows, committing each row
        separately.

        Delegates to the driver's direct_set_full_column hook. The following backend
        details describe the shared SQL/SQLite implementation; backend errors propagate.

        Require the host's ``get_full_column_name`` helper and the aggregation helper's
        dependencies. A missing full column raises InputIntegrityError; SQLite
        operational failures become DatabaseDriverError. Sentinel rows are skipped and
        the acquired write connection is not explicitly closed.

        Example:
            >>> wrapper.set_full_column("series")  # doctest: +SKIP


        :param table: Existing table whose derived column is required.
        :return: ``True`` after all selected rows have been processed.
        """
        return self.driver.direct_set_full_column(target_table=table)

    def walk(self, start_row: dict[str, Any]) -> Iterator[dict[str, Any]]:
        """
        Resolve a row's tree columns and return an iterator over its descendants.

        Infers the table and ID heading. A missing parent heading (None or False) raises
        InputIntegrityError before iteration. Returns _walk() without consuming it;
        traversal starts with the supplied row. Sibling order is unspecified.
        A cycle or repeated descendant raises InputIntegrityError during iteration.

        Example:
            >>> wrapper.walk(series_row)  # doctest: +SKIP


        :param start_row: Starting row dictionary containing its identifier and any
            required parent value.
        :return: Iterator of row dictionaries beginning with start_row.
        """
        table = self.identify_table_from_row_dict(start_row)
        if table is False:
            raise InputIntegrityError("Cannot identify an empty tree row")
        table_id_col = self.get_id_column(table)
        table_parent_col = self.get_parent_column(table)

        if table_parent_col is None or table_parent_col is False:
            err_str = "Given table does not have a tree structure - so can't be walked"
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("start_row", start_row),
                ("table", table),
                ("table_id_col", table_id_col),
                ("table_parent_col", table_parent_col),
            )
            raise InputIntegrityError(err_str)

        return self._walk(start_row, table, table_id_col, table_parent_col)

    def _walk(
        self,
        start_row: dict[str, Any],
        table: str,
        table_id_col: str,
        table_parent_col: str,
    ) -> Iterator[dict[str, Any]]:
        # Load the ids pool with the ids of parent rows - search for them in the parent column and yield those rows
        # If a row has no children (not referenced in any parent column) then it's a leaf row and we're done for that
        # branch
        """
        Yield a starting row and descendants selected through parent-column searches.

        Converts IDs to integers and keeps pending IDs in a set. Yields the original
        starting mapping first, then children returned by search(), scheduling each
        child ID. A separate visited set rejects cycles/repeated rows before yielding
        the duplicate. Sibling order is unspecified; memory grows with visited IDs.

        Example:
            >>> from types import SimpleNamespace
            >>> root = {"series_id": 1, "series_parent": None}
            >>> child = {"series_id": 2, "series_parent": 1}
            >>> host = SimpleNamespace(
            ...     search=lambda **kw: [child] if kw["search_term"] == 1 else []
            ... )
            >>> list(
            ...     DriverWrapperTreeMixin._walk(
            ...         host, root, "series", "series_id", "series_parent"
            ...     )
            ... ) == [root, child]
            True


        :param start_row: Starting row dictionary containing its identifier and any
            required parent value.
        :param table: Table name in the current schema.
        :param table_id_col: Identifier column in the traversed table.
        :param table_parent_col: Parent-reference column in the traversed table.
        :return: Iterator yielding the starting row and discovered descendants.
        """
        ids_pool = set()
        ids_pool.add(int(start_row[table_id_col]))
        visited = set(ids_pool)

        # Start the walk by yielding the start row - then working through the ids pool - take each id from it, find all
        # the children, yield them and add their ids for recursion on down. Continue until all rows have been yielded.
        yield start_row
        while ids_pool:
            working_id = ids_pool.pop()
            working_children = self.search(
                table=table, column=table_parent_col, search_term=working_id
            )

            for child_row in working_children:
                child_id = int(child_row[table_id_col])
                if child_id in visited:
                    raise InputIntegrityError(
                        f"Cycle or repeated row in {table!r}: {child_id}"
                    )
                visited.add(child_id)
                ids_pool.add(child_id)
                yield child_row
