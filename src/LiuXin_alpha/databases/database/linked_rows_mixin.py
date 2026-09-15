
"""
Offer uncached convenience queries for rows reached from a seed.

Same-table queries return the seed itself; cross-table queries use the existing interlink reader. These helpers do not traverse self-link edges. Dictionaries are converted to Rows, while existing Rows retain their original owner.
"""

from __future__ import annotations

from typing import Any, Iterable, Optional, TYPE_CHECKING

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.errors import InputIntegrityError
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode
from LiuXin_alpha.utils.logging import default_log

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api import RowAPI
    from LiuXin_alpha.databases.api import DatabaseAPI


class DatabaseLinkedRowsMixin:
    """
    Compose linked-row lists, first matches, ID sets and simple fingerprints.

    Uses the host facade schema categories, Row factory and interlink reader. It has no memoization, transitive traversal or transaction boundary; each target table can require a fresh query.

    Example:
        For an existing agent Row, db.get_first_linked_row(agent, "works") returns the first linked work in the interlink reader ordering.
    """

    def _coerce_link_seed_row(self: "DatabaseAPI", seed_row: "RowAPI | dict[str, Any]") -> "RowAPI":
        """
        Accept a concrete Row or wrap a dictionary using this facade.

        An existing Row is not rebound or checked for ownership by this facade. Row construction from a dictionary may consult schema metadata and applies the normal Row validation rules.

        Example:
            During linked lookup, an existing seed Row is reused; passing seed.row_dict constructs another Row through the receiving facade.


        :param seed_row: Concrete Row retained unchanged, or dictionary copied into a new Row.
        :return: Existing Row by identity, or newly constructed Row.
        :raises InputIntegrityError: The seed is neither a concrete Row nor a dict.
        """
        if isinstance(seed_row, Row):
            return seed_row

        if isinstance(seed_row, dict):
            return Row(database=self, row_dict=dict(seed_row))

        err_str = "Linked-row helper expected a Row or row_dict."
        err_str = default_log.log_variables(err_str, "ERROR", ("seed_row", seed_row))
        raise InputIntegrityError(err_str)

    def _validate_linked_target_table(self: "DatabaseAPI", target_table: str) -> None:
        """
        Check a target against the host main and helper table categories.

        An absent/false helper collection contributes no names. Main-table state must already be initialized.

        Example:
            For a facade whose main_tables contains works, _validate_linked_target_table("works") permits that target.


        :param target_table: Target table name already converted to text by the caller.
        :return: None on success.
        :raises InputIntegrityError: The target is absent from both permitted categories.
        """
        valid_tables = set(self.main_tables).union(set(getattr(self, "helper_tables", set()) or set()))
        if target_table not in valid_tables:
            err_str = "Linked-row helper given an invalid target_table."
            err_str = default_log.log_variables(
                err_str,
                "ERROR",
                ("target_table", target_table),
                ("valid_tables", sorted(valid_tables)),
            )
            raise InputIntegrityError(err_str)

    def get_linked_rows(
        self: "DatabaseAPI",
        seed_row: "RowAPI | dict[str, Any]",
        target_table: str,
        *,
        type_filter: Optional[str] = None,
    ) -> list["RowAPI"]:
        """
        Return the seed for its own table or follow cross-table interlinks.

        Same-table lookup ignores type_filter and does not query self-links. Cross-table results retain the interlink reader descending-priority order and duplicates. No cache or ownership check is introduced.

        Example:
            For an agent Row, db.get_linked_rows(agent, "agents") returns [agent]; db.get_linked_rows(agent, "works") follows its cross-table links.


        :param seed_row: Concrete seed Row or row dictionary.
        :param target_table: Main/helper table name converted to text and validated first.
        :param type_filter: Optional exact relationship-type filter for cross-table queries.
        :return: Single-element seed list for the same table, otherwise the interlink reader endpoint list.
        """
        target_table = six_unicode(target_table)
        self._validate_linked_target_table(target_table)
        normalized_seed = self._coerce_link_seed_row(seed_row)

        if normalized_seed.table == target_table:
            return [normalized_seed]

        return self.get_interlinked_rows(
            target_row=normalized_seed,
            secondary_table=target_table,
            type_filter=type_filter,
        )

    def get_first_linked_row(
        self,
        seed_row: "RowAPI | dict[str, Any]",
        target_table: str,
        *,
        type_filter: Optional[str] = None,
    ) -> Optional["RowAPI"]:
        """
        Return the first linked result using the normal linked-row ordering.

        The full linked list is retrieved before selecting its first element.

        Example:
            For an agent with linked works, first = db.get_first_linked_row(agent, "works") chooses the highest-priority result when priorities exist.


        :param seed_row: Concrete seed Row or row dictionary.
        :param target_table: Main/helper target table.
        :param type_filter: Optional cross-table relationship-type filter.
        :return: First Row, or None for an empty result.
        """
        rows = self.get_linked_rows(seed_row, target_table, type_filter=type_filter)
        if rows:
            return rows[0]
        return None

    def get_linked_ids_set(
        self,
        seed_row: "RowAPI | dict[str, Any]",
        target_table: str,
        *,
        type_filter: Optional[str] = None,
    ) -> set[int]:
        """
        Collect distinct target ID-column values from linked Rows.

        Resolve the target ID column even when the linked result list is empty. Ordering is discarded.

        Example:
            For an agent Row, db.get_linked_ids_set(agent, "works") returns the distinct IDs of its linked works.


        :param seed_row: Concrete seed Row or row dictionary.
        :param target_table: Main/helper target table converted to text.
        :param type_filter: Optional cross-table type filter.
        :return: Set of stored target ID values; no integer coercion is performed despite the annotation.
        """
        target_table = six_unicode(target_table)
        rows = self.get_linked_rows(seed_row, target_table, type_filter=type_filter)
        id_column = self.driver_wrapper.get_id_column(target_table)
        return {row[id_column] for row in rows}

    def get_linked_fingerprint(
        self,
        seed_row: "RowAPI | dict[str, Any]",
        *,
        target_tables: Optional[Iterable[str]] = None,
        type_filter: Optional[str] = None,
    ) -> set[str]:
        """
        Collect table-and-ID strings for requested direct linked targets.

        The seed itself is included when its table is selected. This is a simple membership representation, not a cryptographic digest or transitive graph fingerprint; queries are not atomic as a group.

        Example:
            For an agent Row, db.get_linked_fingerprint(agent, target_tables=["agents", "works"]) includes its own agent identifier and each directly linked work identifier.


        :param seed_row: Concrete seed Row or row dictionary reused for each target query.
        :param target_tables: Target table iterable, or all main tables when None.
        :param type_filter: Optional cross-table type filter applied by each query.
        :return: Set of strings formatted as table_id; duplicates and ordering are discarded.
        """
        if target_tables is None:
            tables = list(self.main_tables)
        else:
            tables = [six_unicode(t) for t in target_tables]

        fingerprint: set[str] = set()
        for table in tables:
            for row_id in self.get_linked_ids_set(seed_row, table, type_filter=type_filter):
                fingerprint.add(f"{table}_{six_unicode(row_id)}")
        return fingerprint
