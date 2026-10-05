"""
Relationships between different database tables.

Abstract declarations describe the concrete facade conventions; their bodies do not execute database operations. The file retains duplicate method declarations; later definitions replace earlier ones in the runtime class. Both lexical occurrences are documented. Cardinality, priority handling and cleanup remain implementation-specific.
"""

from __future__ import annotations

import abc
from typing import Optional, Union, Any, Iterable, TYPE_CHECKING

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api import RowAPI


class DatabaseInterlinkRowsMixinAPI(abc.ABC):
    """
    Declare relationships between different database tables.

    Implement all abstract members before instantiation. The file retains duplicate method declarations; later definitions replace earlier ones in the runtime class. Both lexical occurrences are documented. Cardinality, priority handling and cleanup remain implementation-specific.

    Example:
        >>> import inspect
        >>> inspect.isabstract(DatabaseInterlinkRowsMixinAPI)
        True
    """

    @abc.abstractmethod
    def get_interlink_row(
        self,
        primary_row: "RowAPI",
        secondary_row: "RowAPI",
        onelink: bool = True,
    ) -> Optional[Union["RowAPI", list["RowAPI"]]]:
        """
        Find relationship Rows connecting one ordered endpoint pair.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Reject same-table or unlinked schemas. Search by the primary ID and compare secondary IDs as text; this does not filter relationship types.

        Example:
            For compatible rows, links = db.get_interlink_row(agent, work, onelink=False) returns every matching relationship or None.


        :param primary_row: Primary endpoint Row.
        :param secondary_row: Secondary endpoint Row in a different table.
        :param onelink: Require at most one match when True; otherwise return all matches.
        :return: None for no matches; one Row when onelink=True, otherwise a nonempty list.
        :raises InputIntegrityError: The tables are identical or have no relationship table.
        :raises DatabaseIntegrityError: Multiple matches exist while onelink=True.
        """

    # Todo: get_interlink_rows and get_interlinked_rows seem similar
    @abc.abstractmethod
    def get_interlink_rows(self, primary_row: "RowAPI", secondary_table: str) -> list["RowAPI"]:
        """
        Return relationship Rows from one endpoint to a target table.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: A DatabaseIntegrityError resolving priority leaves search order intact; missing keys or incomparable sort values are not suppressed.

        Example:
            For a compatible schema, db.get_interlink_rows(agent, "works") returns link records rather than work records.


        :param primary_row: Primary endpoint Row.
        :param secondary_table: Different table whose relationship Rows are requested.
        :return: List of relationship Rows, sorted by ascending priority when that column resolves.
        :raises InputIntegrityError: Same-table or unavailable interlink schema.
        """

    @abc.abstractmethod
    def get_interlinked_rows(
        self,
        primary_row: Optional["RowAPI"] = None,
        secondary_table: Optional[str] = None,
        type_filter: Optional[str] = None,
        **kwargs: Any,
    ) -> list["RowAPI"]:
        """
        Resolve endpoint Rows reached through inter-table relationships.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Validate keywords, target table argument, concrete Row type, target category and different endpoint tables in that order. Only a missing priority key suppresses sorting. Duplicate target IDs are preserved; type values are compared without normalization.

        Example:
            For an existing agent Row, works = db.get_interlinked_rows(primary_row=agent, secondary_table="works", type_filter="author") follows only author links.


        :param primary_row: Primary endpoint; concrete Database accepts this as an alias for target_row.
        :param secondary_table: Required target main/helper table.
        :param type_filter: Optional exact relationship type.
        :param kwargs: Extra implementation arguments; concrete Database also exposes target_row and rejects unknown keywords.
        :return: Endpoint Row list, descending by link priority when available; [] when no link table or matches exist.
        :raises TypeError: Keywords are unexpected or secondary_table is missing.
        :raises InputIntegrityError: The seed is not a concrete Row, the target category is invalid, or the tables match.
        """

    @abc.abstractmethod
    def get_interlink_values(self, target_row: "RowAPI", secondary_column: str) -> set[Any]:
        """
        Collect distinct column values from linked endpoint Rows.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Collect distinct column values from linked endpoint Rows.

        Example:
            For an agent linked to works, db.get_interlink_values(agent, "work_title") collects linked title values.


        :param target_row: Seed Row.
        :param secondary_column: Column whose owning table is identified by the wrapper.
        :return: Set of values; relationship priority and duplicates are discarded.
        :raises TypeError: A retrieved column value is unhashable.
        """

    @abc.abstractmethod
    def interlink_rows(
        self,
        primary_row: "RowAPI",
        secondary_row: "RowAPI",
        priority: Optional[Union[int, float, str]] = "highest",
        type: Optional[str] = None,
        **col_value_pairs: Any,
    ) -> "RowAPI":
        """
        Allocate a relationship record, populate endpoint fields and synchronize it.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Check link-table existence and endpoint IDs before allocation. Highest/lowest use whole-column integer extrema plus/minus one, with an empty-table fallback of one. For not_set, a colliding non-null default may be replaced by the next priority for this primary/type. On sync DatabaseIntegrityError delete the allocated row and reraise; other failures can leave partial state. Reload failures are suppressed.

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

        ...

    @abc.abstractmethod
    def dupe_interlinks(
        self,
        src_row: "RowAPI",
        dst_row: "RowAPI",
        swap_priorities: bool = False,
        restrict_to_tables: Optional[Iterable[str]] = None,
        force_priority: Optional[Union[int, float, str]] = None,
    ) -> None:
        """
        Recreate a source row relationships on a destination using the normal link writer.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Reverse each target list before creation. Original type and extra relationship attributes are not copied. Writes are incremental and not rolled back as a group; duplicate relationships and the legacy swap limitation still apply.

        Example:
            For compatible rows in the same table, db.dupe_interlinks(source, destination, restrict_to_tables=["works"]) recreates links to works using default relationship attributes.


        :param src_row: Row whose linked endpoints are read.
        :param dst_row: Row that will acquire new relationships.
        :param swap_priorities: Invoke the legacy priority-swap helper after each creation.
        :param restrict_to_tables: Target table iterable, or all main tables except the source table.
        :param force_priority: Priority argument for new links, or None for the normal highest default.
        :return: None.
        """

    @abc.abstractmethod
    def swap_priorities(self, src_row: "RowAPI", dst_row_1: "RowAPI", dst_row_2: "RowAPI") -> None:
        """
        Apply the legacy two-link priority update sequence.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Read both priorities, then overwrite the first link priority with None and sync it twice. The second link receives the original first priority; the intended first replacement is never restored. This is not currently a correct two-way swap and has no grouped rollback.

        Example:
            Inspect and correct priorities explicitly when a true exchange is needed; db.swap_priorities(seed, first, second) retains the legacy first-priority-to-None behavior.


        :param src_row: Common endpoint.
        :param dst_row_1: First linked endpoint.
        :param dst_row_2: Second linked endpoint in the same target table.
        :return: None.
        """

    @abc.abstractmethod
    def update_interlink(
        self,
        primary_row: "RowAPI",
        secondary_row: "RowAPI",
        priority: Optional[Union[int, float, str]] = "unchanged",
        **col_value_pairs: Any,
    ) -> "RowAPI":
        """
        Modify the unique relationship Row and synchronize requested fields.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Resolve a priority column even when unchanged is requested. Highest/lowest use whole-column integer extrema; extra keyword columns can override earlier assignments. No rollback is added around sync. Missing links are not explicitly checked before subsequent Row use.

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

    @abc.abstractmethod
    def update_interlink_priority(
            self,
            primary_row: "RowAPI",
            secondary_table: str,
            ordered_ids: Iterable[int]) -> None:
        """
        Assign successive global highest priorities in reverse requested ID order.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Require only equal list lengths, then map current endpoints by integer ID and process a reversed copy of ordered_ids. Duplicate or unknown IDs are not prevalidated; repeated updates can fail after earlier changes.

        Example:
            For three uniquely linked endpoints, db.update_interlink_priority(agent, "works", [third_id, first_id, second_id]) makes that the descending-priority order.


        :param primary_row: Seed Row.
        :param secondary_table: Target endpoint table.
        :param ordered_ids: Desired target IDs; use a sized, copyable list or tuple with the concrete facade, despite this broader Iterable annotation.
        :return: None.
        :raises AssertionError: The number of linked endpoints differs from ordered_ids.
        :raises KeyError: A requested ID is absent from the linked endpoint map.
        """

    @abc.abstractmethod
    def unlink_interlink(self, primary_row: "RowAPI", secondary_row: "RowAPI") -> None:
        """
        Delete the unique relationship Row connecting an endpoint pair.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Uses the single-link lookup; multiple matches raise and a missing match reaches delete(None). Endpoint records are retained.

        Example:
            For an existing unique relationship, db.unlink_interlink(agent, work) deletes its link Row.


        :param primary_row: Primary endpoint.
        :param secondary_row: Secondary endpoint.
        :return: None.
        :raises DatabaseIntegrityError: The endpoint pair has multiple relationship Rows.
        :raises AttributeError: No relationship exists and delete receives None.
        """

    @abc.abstractmethod
    def unlink_all(self, primary_row: "RowAPI", secondary_table: str, type_filter: Optional[str] = None) -> None:
        """
        Delete relationships from a seed to a target table, optionally by type.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Initially fetch endpoints without filtering. The typed path retries ambiguous single-link lookups in multi-link mode; the unfiltered path does not. Repeated endpoints can revisit already deleted links, so missing-link failures and partial deletion remain possible. No group transaction or endpoint deletion is performed.

        Example:
            For a schema with suitable relationship multiplicity, db.unlink_all(agent, "works", type_filter="author") removes author links while retaining endpoint Rows.


        :param primary_row: Primary endpoint.
        :param secondary_table: Target endpoint table.
        :param type_filter: Exact type value to remove, or None for unfiltered deletion.
        :return: None.
        """


    # ---------------------------------------------------------------------------------------------
    # Interlink tables (many-to-many between two *different* tables)
    # ---------------------------------------------------------------------------------------------
    @abc.abstractmethod
    def get_interlink_row(
        self,
        primary_row: "RowAPI",
        secondary_row: "RowAPI",
        onelink: bool = True,
    ) -> Optional[Union["RowAPI", list["RowAPI"]]]:
        """
        Find relationship Rows connecting one ordered endpoint pair.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Reject same-table or unlinked schemas. Search by the primary ID and compare secondary IDs as text; this does not filter relationship types.

        Example:
            For compatible rows, links = db.get_interlink_row(agent, work, onelink=False) returns every matching relationship or None.


        :param primary_row: Primary endpoint Row.
        :param secondary_row: Secondary endpoint Row in a different table.
        :param onelink: Require at most one match when True; otherwise return all matches.
        :return: None for no matches; one Row when onelink=True, otherwise a nonempty list.
        :raises InputIntegrityError: The tables are identical or have no relationship table.
        :raises DatabaseIntegrityError: Multiple matches exist while onelink=True.
        """

    @abc.abstractmethod
    def get_interlink_rows(self, primary_row: "RowAPI", secondary_table: str) -> list["RowAPI"]:
        """
        Return relationship Rows from one endpoint to a target table.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: A DatabaseIntegrityError resolving priority leaves search order intact; missing keys or incomparable sort values are not suppressed.

        Example:
            For a compatible schema, db.get_interlink_rows(agent, "works") returns link records rather than work records.


        :param primary_row: Primary endpoint Row.
        :param secondary_table: Different table whose relationship Rows are requested.
        :return: List of relationship Rows, sorted by ascending priority when that column resolves.
        :raises InputIntegrityError: Same-table or unavailable interlink schema.
        """

    @abc.abstractmethod
    def get_interlinked_rows(
        self,
        primary_row: Optional["RowAPI"] = None,
        secondary_table: Optional[str] = None,
        type_filter: Optional[str] = None,
        **kwargs: Any,
    ) -> list["RowAPI"]:
        """
        Resolve endpoint Rows reached through inter-table relationships.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Validate keywords, target table argument, concrete Row type, target category and different endpoint tables in that order. Only a missing priority key suppresses sorting. Duplicate target IDs are preserved; type values are compared without normalization.

        Example:
            For an existing agent Row, works = db.get_interlinked_rows(primary_row=agent, secondary_table="works", type_filter="author") follows only author links.


        :param primary_row: Primary endpoint; concrete Database accepts this as an alias for target_row.
        :param secondary_table: Required target main/helper table.
        :param type_filter: Optional exact relationship type.
        :param kwargs: Extra implementation arguments; concrete Database also exposes target_row and rejects unknown keywords.
        :return: Endpoint Row list, descending by link priority when available; [] when no link table or matches exist.
        :raises TypeError: Keywords are unexpected or secondary_table is missing.
        :raises InputIntegrityError: The seed is not a concrete Row, the target category is invalid, or the tables match.
        """

    @abc.abstractmethod
    def get_interlink_values(self, target_row: "RowAPI", secondary_column: str) -> set[Any]:
        """
        Collect distinct column values from linked endpoint Rows.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Collect distinct column values from linked endpoint Rows.

        Example:
            For an agent linked to works, db.get_interlink_values(agent, "work_title") collects linked title values.


        :param target_row: Seed Row.
        :param secondary_column: Column whose owning table is identified by the wrapper.
        :return: Set of values; relationship priority and duplicates are discarded.
        :raises TypeError: A retrieved column value is unhashable.
        """

    @abc.abstractmethod
    def interlink_rows(
        self,
        primary_row: "RowAPI",
        secondary_row: "RowAPI",
        priority: Optional[Union[int, float, str]] = "highest",
        type: Optional[str] = None,
        **col_value_pairs: Any,
    ) -> "RowAPI":
        """
        Allocate a relationship record, populate endpoint fields and synchronize it.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Check link-table existence and endpoint IDs before allocation. Highest/lowest use whole-column integer extrema plus/minus one, with an empty-table fallback of one. For not_set, a colliding non-null default may be replaced by the next priority for this primary/type. On sync DatabaseIntegrityError delete the allocated row and reraise; other failures can leave partial state. Reload failures are suppressed.

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

    @abc.abstractmethod
    def dupe_interlinks(
        self,
        src_row: "RowAPI",
        dst_row: "RowAPI",
        swap_priorities: bool = False,
        restrict_to_tables: Optional[Iterable[str]] = None,
        force_priority: Optional[Union[int, float, str]] = None,
    ) -> None:
        """
        Recreate a source row relationships on a destination using the normal link writer.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Reverse each target list before creation. Original type and extra relationship attributes are not copied. Writes are incremental and not rolled back as a group; duplicate relationships and the legacy swap limitation still apply.

        Example:
            For compatible rows in the same table, db.dupe_interlinks(source, destination, restrict_to_tables=["works"]) recreates links to works using default relationship attributes.


        :param src_row: Row whose linked endpoints are read.
        :param dst_row: Row that will acquire new relationships.
        :param swap_priorities: Invoke the legacy priority-swap helper after each creation.
        :param restrict_to_tables: Target table iterable, or all main tables except the source table.
        :param force_priority: Priority argument for new links, or None for the normal highest default.
        :return: None.
        """

    @abc.abstractmethod
    def swap_priorities(self, src_row: "RowAPI", dst_row_1: "RowAPI", dst_row_2: "RowAPI") -> None:
        """
        Apply the legacy two-link priority update sequence.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Read both priorities, then overwrite the first link priority with None and sync it twice. The second link receives the original first priority; the intended first replacement is never restored. This is not currently a correct two-way swap and has no grouped rollback.

        Example:
            Inspect and correct priorities explicitly when a true exchange is needed; db.swap_priorities(seed, first, second) retains the legacy first-priority-to-None behavior.


        :param src_row: Common endpoint.
        :param dst_row_1: First linked endpoint.
        :param dst_row_2: Second linked endpoint in the same target table.
        :return: None.
        """

    @abc.abstractmethod
    def update_interlink(
        self,
        primary_row: "RowAPI",
        secondary_row: "RowAPI",
        priority: Optional[Union[int, float, str]] = "unchanged",
        **col_value_pairs: Any,
    ) -> "RowAPI":
        """
        Modify the unique relationship Row and synchronize requested fields.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Resolve a priority column even when unchanged is requested. Highest/lowest use whole-column integer extrema; extra keyword columns can override earlier assignments. No rollback is added around sync. Missing links are not explicitly checked before subsequent Row use.

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

    @abc.abstractmethod
    def update_interlink_priority(
        self,
        primary_row: "RowAPI",
        secondary_table: str,
        ordered_ids: Iterable[int],
    ) -> None:
        """
        Assign successive global highest priorities in reverse requested ID order.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Require only equal list lengths, then map current endpoints by integer ID and process a reversed copy of ordered_ids. Duplicate or unknown IDs are not prevalidated; repeated updates can fail after earlier changes.

        Example:
            For three uniquely linked endpoints, db.update_interlink_priority(agent, "works", [third_id, first_id, second_id]) makes that the descending-priority order.


        :param primary_row: Seed Row.
        :param secondary_table: Target endpoint table.
        :param ordered_ids: Desired target IDs; use a sized, copyable list or tuple with the concrete facade, despite this broader Iterable annotation.
        :return: None.
        :raises AssertionError: The number of linked endpoints differs from ordered_ids.
        :raises KeyError: A requested ID is absent from the linked endpoint map.
        """

    @abc.abstractmethod
    def unlink_interlink(self, primary_row: "RowAPI", secondary_row: "RowAPI") -> None:
        """
        Delete the unique relationship Row connecting an endpoint pair.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Uses the single-link lookup; multiple matches raise and a missing match reaches delete(None). Endpoint records are retained.

        Example:
            For an existing unique relationship, db.unlink_interlink(agent, work) deletes its link Row.


        :param primary_row: Primary endpoint.
        :param secondary_row: Secondary endpoint.
        :return: None.
        :raises DatabaseIntegrityError: The endpoint pair has multiple relationship Rows.
        :raises AttributeError: No relationship exists and delete receives None.
        """

    @abc.abstractmethod
    def unlink_all(self, primary_row: "RowAPI", secondary_table: str, type_filter: Optional[str] = None) -> None:
        """
        Delete relationships from a seed to a target table, optionally by type.

        Abstract hook; subclasses supply the operation. Concrete Database behavior: Initially fetch endpoints without filtering. The typed path retries ambiguous single-link lookups in multi-link mode; the unfiltered path does not. Repeated endpoints can revisit already deleted links, so missing-link failures and partial deletion remain possible. No group transaction or endpoint deletion is performed.

        Example:
            For a schema with suitable relationship multiplicity, db.unlink_all(agent, "works", type_filter="author") removes author links while retaining endpoint Rows.


        :param primary_row: Primary endpoint.
        :param secondary_table: Target endpoint table.
        :param type_filter: Exact type value to remove, or None for unfiltered deletion.
        :return: None.
        """
